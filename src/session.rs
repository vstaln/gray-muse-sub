//! Muse session pool + turn driver — mirrors acp_bridge::session's
//! shape (absorbed-prefix continuation matching, per-conversation
//! sessions) but drives `muse serve` MSP sessions instead of ACP
//! children.
//!
//! Turn model (mirrors the pi extension):
//! - The host resends full `input[]` each request (`store:false`); a
//!   pooled MSP session holds its own history, so only the fresh tail
//!   is sent as `turn/start` input.
//! - Muse's own tools run inside the session; `approval/requested` is
//!   auto-decided (approved choices only) — a refusal or an
//!   unanswerable `userInput/requested` fails the turn, matching pi's
//!   headless path.
//! - `item/delta` field `text` is assistant text; `summary.N` fields are
//!   visible reasoning (MSP v1 never streams raw thinking text).

use acp_bridge::chat::{self, OutItem, TurnResult, Usage};
use acp_bridge::guard::{self, TurnGuard};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use crate::msp::{self, MspHost};

const MAX_SESSIONS: usize = 8;
/// Records inside this window re-warm at startup (claude-sub's warm
/// window); older ones degrade to a plain cold start.
const WARM_WINDOW: Duration = Duration::from_secs(6 * 3600);
/// Record files idle past this are reaped — a lost record only ever
/// costs a cold start, so nothing of value outlives a day. Muse's own
/// session logs are never touched.
const RECORD_TTL: Duration = Duration::from_secs(24 * 3600);
/// Hard cap on a single MSP turn; the relay's own keepalives cover the
/// host read side, this only bounds a wedged session.
const TURN_TIMEOUT: Duration = Duration::from_secs(900);
const POLL: Duration = Duration::from_millis(250);
const MAX_TRANSCRIPT_CHARS: usize = 400_000;
const RECORDS: &[&str] = &[
    "User: ",
    "Assistant: ",
    "System: ",
    "Assistant called tool ",
    "Tool result for ",
    "## Tool result for ",
    "## System instructions",
];
const ONE_TURN_RULE: &str = "You write exactly ONE assistant turn. Never write `User:`, \
    `## Tool result for …` or any other transcript record yourself; tool results only come from \
    the harness. Never claim a command ran unless its `Tool result` is in the transcript.";

struct MuseSession {
    session_id: String,
    model: String,
    /// Verbatim Responses `input[]` already sent into this session.
    absorbed: Vec<Value>,
    reply_call_ids: Vec<String>,
    reply_text: String,
    last_used: Instant,
    prior_cut: bool,
    /// Re-pooled from a persisted record after a plugin restart — its
    /// first real turn is the resume's proof: a drain error there means
    /// a stale record, so the request falls back to a fresh session
    /// instead of failing.
    rewarmed: bool,
}

pub struct Pool {
    sessions: Mutex<HashMap<String, MuseSession>>,
}

impl Pool {
    pub fn new() -> Arc<Self> {
        Arc::new(Pool {
            sessions: Mutex::new(HashMap::new()),
        })
    }
}

fn uuid7() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// pi's sparkUsesMeta: spark-family ids route through provider "meta".
fn spark_uses_meta(model: &str) -> bool {
    let m = model.trim().to_lowercase();
    m == "spark" || m.starts_with("muse-spark-") || m == "muse-spark"
}

/// pi's reasoningEffort map: off→none, max→ultra, rest verbatim.
fn reasoning_effort(effort: Option<&str>) -> Option<String> {
    match effort? {
        "off" => Some("none".into()),
        "max" => Some("ultra".into()),
        other => Some(other.to_string()),
    }
}

/// Extract vision input from a Responses item's content parts
/// (`input_image`/`image_url` data URLs) → MSP image parts.
fn item_images(item: &Value, out: &mut Vec<Value>) {
    let Some(parts) = item.get("content").and_then(Value::as_array) else {
        return;
    };
    for part in parts {
        let ty = part.get("type").and_then(Value::as_str).unwrap_or("");
        if ty != "input_image" && ty != "image_url" {
            continue;
        }
        let url = part
            .get("image_url")
            .and_then(|u| u.as_str().map(str::to_string).or_else(|| {
                u.get("url").and_then(Value::as_str).map(str::to_string)
            }))
            .unwrap_or_default();
        let Some(rest) = url.strip_prefix("data:") else {
            continue;
        };
        let Some((mime, data)) = rest.split_once(";base64,") else {
            continue;
        };
        if mime.is_empty() || data.is_empty() {
            continue;
        }
        out.push(json!({"type": "image", "mediaType": mime, "base64Data": data}));
    }
}

/// Render one tail item to transcript text (mirrors pi's deltaParts:
/// toolResult → "## Tool result" block, never dropped).
fn tail_item_text(item: &Value) -> Option<String> {
    let ty = item.get("type").and_then(Value::as_str).unwrap_or("");
    if ty == "function_call_output" {
        let call_id = item
            .get("call_id")
            .and_then(Value::as_str)
            .unwrap_or("?");
        let output = item
            .get("output")
            .map(|o| match o {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            })
            .unwrap_or_default();
        return Some(format!("## Tool result for {call_id}\n{output}"));
    }
    chat::item_to_transcript_pub(item)
}

/// MSP `turn/start` input parts for a slice of Responses input items.
/// `system` is prepended as pi's "## System instructions" block.
fn input_parts(items: &[Value], system: &str) -> Result<Vec<Value>, String> {
    let mut texts: Vec<String> = Vec::new();
    let mut images: Vec<Value> = Vec::new();
    if !system.trim().is_empty() {
        texts.push(format!("## System instructions\n{system}\n\n{ONE_TURN_RULE}"));
    }
    for item in items {
        if let Some(mut t) = tail_item_text(item) {
            if t.len() > MAX_TRANSCRIPT_CHARS {
                let mut end = MAX_TRANSCRIPT_CHARS;
                while !t.is_char_boundary(end) {
                    end -= 1;
                }
                t.truncate(end);
                t.push_str("\n[truncated]");
            }
            texts.push(t);
        }
        item_images(item, &mut images);
    }
    let joined = texts.join("\n\n");
    if joined.trim().is_empty() && images.is_empty() {
        return Err("muse-sub received an empty conversation".into());
    }
    let mut parts = vec![json!({"type": "text", "text": joined.trim()})];
    if !images.is_empty() {
        // pi attachVisionImages hint: retained tool-read images are
        // rejected by the `muse` route, so vision input rides the turn.
        parts.push(json!({"type": "text", "text":
            "Image attachments above/below are vision input; analyze them directly."}));
        parts.extend(images);
    }
    Ok(parts)
}

/// The approval mode sessions are created (and resumed) under:
/// sandboxed hosts decide per-request (auto-decided on our side),
/// `GRAY_MUSE_SUB_UNSANDBOXED=1` gets the pi-muse-msp allowAll default.
fn approval_mode() -> &'static str {
    if crate::setup::sandboxed() {
        "onRequest"
    } else {
        "allowAll"
    }
}

fn start_session(host: &MspHost, model: &str) -> Result<String, String> {
    let cwd = std::env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "/".to_string());
    let mut params = json!({
        "commandId": uuid7(),
        "workspaceRoot": cwd,
        "approvalMode": approval_mode(),
    });
    if spark_uses_meta(model) {
        params["providerId"] = json!("meta");
    }
    // Bare "muse-spark"/"spark" = server default: providerId only.
    if !["muse-spark", "spark", ""].contains(&model.trim().to_lowercase().as_str()) {
        params["modelId"] = json!(model);
    }
    let result = host.request("session/start", params, msp::RPC_TIMEOUT)?;
    result
        .get("session")
        .and_then(|s| s.get("sessionId"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("session/start returned no sessionId: {}", short(&result)))
}

fn short(v: &Value) -> String {
    let s = v.to_string();
    s.chars().take(300).collect()
}

fn usage_from_msp(v: &Value) -> Option<Usage> {
    let g = |k: &str| v.get(k).and_then(Value::as_u64).unwrap_or(0);
    let input = g("inputTokens") + g("input_tokens");
    let output = g("outputTokens") + g("output_tokens");
    let cache_read = g("cachedTokens") + g("cacheReadTokens");
    let cache_write = g("cacheWriteTokens");
    if input + output + cache_read + cache_write == 0 {
        return None;
    }
    Some(Usage {
        input_tokens: input,
        output_tokens: output,
        total_tokens: input + output + cache_read + cache_write,
        extra: v.clone(),
    })
}

fn is_summary_field(field: &str) -> bool {
    field
        .strip_prefix("summary.")
        .map(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
        .unwrap_or(false)
}

/// Decide one approval: pick the first "approved"-family choice, else
/// fail. Returns Err(text) when the turn should be cancelled.
fn decide_approval(
    host: &MspHost,
    handled: &mut HashSet<String>,
    params: &Value,
    session_id: &str,
) -> Result<(), String> {
    if params.get("sessionId").and_then(Value::as_str) != Some(session_id) {
        return Ok(());
    }
    let approval_id = params
        .get("approvalId")
        .and_then(Value::as_str)
        .unwrap_or("");
    let requirement = params
        .get("currentRequirementId")
        .cloned()
        .unwrap_or(Value::Null);
    if approval_id.is_empty() || requirement.is_null() {
        return Err("Muse requested an invalid approval".into());
    }
    let key = format!("{approval_id}:{}", requirement);
    if !handled.insert(key) {
        return Ok(());
    }
    let choices = params
        .get("availableChoices")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let choice = choices.iter().find(|c| {
        matches!(
            c.get("decision").and_then(Value::as_str),
            Some("approved") | Some("approvedForSession")
        )
    });
    let Some(choice) = choice else {
        return Err(format!(
            "Muse approval had no automatic choice (tool {})",
            params.get("toolName").and_then(Value::as_str).unwrap_or("?")
        ));
    };
    host.request(
        "approval/decide",
        json!({
            "commandId": uuid7(),
            "sessionId": session_id,
            "approvalId": approval_id,
            "choiceId": choice.get("choiceId").and_then(Value::as_str).unwrap_or(""),
            "requirementId": requirement,
        }),
        msp::RPC_TIMEOUT,
    )
    .map(|_| ())
    .or_else(|e| {
        if e.to_lowercase().contains("already") && e.to_lowercase().contains("resolved") {
            Ok(())
        } else {
            Err(format!("approval/decide failed: {e}"))
        }
    })
}

/// Headless clarification: cancel the request and fail the turn (pi -p
/// parity — there is no UI to answer with).
fn refuse_user_input(host: &MspHost, params: &Value, session_id: &str) -> Result<(), String> {
    if params.get("sessionId").and_then(Value::as_str) != Some(session_id) {
        return Ok(());
    }
    let id = params
        .get("userInputId")
        .and_then(Value::as_str)
        .unwrap_or("");
    if id.is_empty() {
        return Err("Muse requested invalid user input".into());
    }
    let _ = host.request(
        "userInput/cancel",
        json!({
            "commandId": uuid7(),
            "sessionId": session_id,
            "userInputId": id,
            "reason": "gray-muse-sub has no interactive UI",
        }),
        msp::RPC_TIMEOUT,
    );
    Err("Muse asked a clarifying question; answer it in chat and retry".into())
}

/// Drive one MSP turn to its `turn/completed` notification.
fn drive(
    host: &MspHost,
    session_id: &str,
    parts: Vec<Value>,
    effort: Option<String>,
    cancel: &AtomicBool,
) -> Result<TurnResult, String> {
    let rx = host.subscribe();
    let command_id = uuid7();
    let mut params = json!({
        "commandId": command_id,
        "sessionId": session_id,
        "input": parts,
    });
    if let Some(e) = effort {
        params["reasoningEffort"] = json!(e);
    }
    let ack = host.request("turn/start", params, msp::TURN_ACK_TIMEOUT)?;
    let turn_id = ack
        .get("turnId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or(command_id.clone());
    if turn_id.is_empty() {
        return Err("Muse MSP turn/start returned no turnId".into());
    }

    let deadline = Instant::now() + TURN_TIMEOUT;
    let max_steps = crate::setup::max_steps();
    let mut steps = 0u64;
    let mut result = TurnResult {
        stop_reason: "stop".into(),
        ..Default::default()
    };
    let mut streamed: HashMap<String, usize> = HashMap::new();
    let mut approvals_handled: HashSet<String> = HashSet::new();
    let mut last_field: HashMap<String, String> = HashMap::new();
    // In-order (itemId → block) append: a fresh itemId opens a new block.
    let mut open_item = String::new();
    let mut open_is_text = true;
    let mut turn_guard = TurnGuard::new(RECORDS, Some(guard::LABEL));
    let mut push = |is_text: bool, item_id: &str, piece: &str| -> Option<&'static str> {
        if turn_guard.cut().is_some() {
            return None;
        }
        if item_id != open_item || is_text != open_is_text {
            open_item = item_id.to_string();
            open_is_text = is_text;
            turn_guard.new_segment();
            result.items.push(if is_text {
                OutItem::Message {
                    text: String::new(),
                }
            } else {
                OutItem::Reasoning {
                    text: String::new(),
                }
            });
        }
        match result.items.last_mut() {
            Some(OutItem::Message { text }) => {
                text.push_str(piece);
                if is_text {
                    return turn_guard.feed(text);
                }
            }
            Some(OutItem::Reasoning { text }) => text.push_str(piece),
            _ => {}
        }
        None
    };
    let mut cut: Option<&'static str> = None;

    loop {
        if cancel.load(Ordering::SeqCst) {
            let _ = host.request(
                "turn/interrupt",
                json!({"commandId": uuid7(), "sessionId": session_id,
                       "turnId": turn_id, "retract": false}),
                msp::RPC_TIMEOUT,
            );
            return Err("cancelled".into());
        }
        if Instant::now() >= deadline {
            let _ = host.request(
                "turn/interrupt",
                json!({"commandId": uuid7(), "sessionId": session_id,
                       "turnId": turn_id, "retract": false}),
                msp::RPC_TIMEOUT,
            );
            return Err("muse turn timed out".into());
        }
        let msg = match rx.recv_timeout(POLL) {
            Ok(m) => m,
            Err(mpsc_err) => match mpsc_err {
                std::sync::mpsc::RecvTimeoutError::Timeout => continue,
                std::sync::mpsc::RecvTimeoutError::Disconnected => {
                    return Err("muse serve exited".into())
                }
            },
        };
        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
        let params = msg.get("params").cloned().unwrap_or_else(|| json!({}));
        if method == msp::DIED_METHOD {
            return Err("muse serve exited mid-turn".into());
        }
        if params.get("sessionId").and_then(Value::as_str) != Some(session_id)
            && method != "turn/completed"
        {
            continue;
        }
        match method {
            "item/delta" => {
                let Some(delta) = params.get("delta").and_then(Value::as_str) else {
                    continue;
                };
                let field = params
                    .get("field")
                    .and_then(Value::as_str)
                    .unwrap_or("text");
                let item_id = params
                    .get("itemId")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if field == "text" {
                    cut = cut.or(push(true, item_id, delta));
                    *streamed.entry(item_id.to_string()).or_insert(0) += delta.len();
                } else if is_summary_field(field) {
                    // summary.N deltas chain across N: newline only when N advances.
                    let piece = match last_field.get(item_id) {
                        Some(prev) if prev != field => format!("\n{delta}"),
                        _ => delta.to_string(),
                    };
                    last_field.insert(item_id.to_string(), field.to_string());
                    push(false, item_id, &piece);
                    *streamed.entry(item_id.to_string()).or_insert(0) += piece.len();
                }
            }
            "item/completed" => {
                let Some(item) = params.get("item") else {
                    continue;
                };
                if item.get("turnId").and_then(Value::as_str) != Some(turn_id.as_str()) {
                    continue;
                }
                let kind = item.get("kind").and_then(Value::as_str).unwrap_or("");
                let item_id = item
                    .get("itemId")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let (is_text, snapshot) = match kind {
                    "agentMessage" => (
                        true,
                        item.get("text").and_then(Value::as_str).unwrap_or("").to_string(),
                    ),
                    "reasoning" => (false, {
                        item.get("summary")
                            .and_then(Value::as_array)
                            .map(|a| {
                                a.iter()
                                    .filter_map(Value::as_str)
                                    .collect::<Vec<_>>()
                                    .join("\n")
                            })
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| {
                                item.get("text")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_string()
                            })
                    }),
                    _ => continue,
                };
                let seen = streamed.get(item_id).copied().unwrap_or(0);
                if snapshot.len() > seen && snapshot.is_char_boundary(seen) {
                    cut = cut.or(push(is_text, item_id, &snapshot[seen..]));
                }
                streamed.remove(item_id);
            }
            "session/tokenUsage" => {
                if let Some(u) = params.get("usage").and_then(usage_from_msp) {
                    result.usage = Some(u);
                }
                steps += 1;
                if max_steps != 0 && steps >= max_steps {
                    let _ = host.request(
                        "turn/interrupt",
                        json!({"commandId": uuid7(), "sessionId": session_id,
                               "turnId": turn_id, "retract": false}),
                        msp::RPC_TIMEOUT,
                    );
                    push(
                        true,
                        "stepcap",
                        &format!(
                            "[muse-sub: stopped after {steps} steps — raise GRAY_MUSE_SUB_MAX_STEPS to allow more]"
                        ),
                    );
                    return Ok(result);
                }
            }
            "approval/requested" | "approval/updated" => {
                if let Err(e) =
                    decide_approval(host, &mut approvals_handled, &params, session_id)
                {
                    let _ = host.request(
                        "turn/cancel",
                        json!({"commandId": uuid7(), "sessionId": session_id,
                               "turnId": turn_id}),
                        msp::RPC_TIMEOUT,
                    );
                    return Err(e);
                }
            }
            "userInput/requested" => {
                if let Err(e) = refuse_user_input(host, &params, session_id) {
                    let _ = host.request(
                        "turn/cancel",
                        json!({"commandId": uuid7(), "sessionId": session_id,
                               "turnId": turn_id}),
                        msp::RPC_TIMEOUT,
                    );
                    return Err(e);
                }
            }
            "turn/retryScheduled" => {
                let attempt = params.get("attempt").and_then(Value::as_u64).unwrap_or(0);
                let max = params.get("maxAttempts").and_then(Value::as_u64).unwrap_or(0);
                push(
                    false,
                    "retry",
                    &format!(
                        "[muse: attempt {attempt}/{max} failed; retrying]\n"
                    ),
                );
            }
            "turn/completed" => {
                if params.get("sessionId").and_then(Value::as_str) != Some(session_id)
                    || params.get("turnId").and_then(Value::as_str) != Some(turn_id.as_str())
                {
                    continue;
                }
                // Terminal usage is authoritative (pi: tokenUsage can be lost).
                if let Some(u) = params.get("usage").and_then(usage_from_msp) {
                    result.usage = Some(u);
                }
                match params.get("terminal").and_then(Value::as_str).unwrap_or("completed") {
                    "completed" => return Ok(result),
                    "cancelled" => {
                        return Err(format!(
                            "Muse turn was cancelled{}",
                            params
                                .get("reason")
                                .and_then(Value::as_str)
                                .map(|r| format!(": {r}"))
                                .unwrap_or_default()
                        ))
                    }
                    terminal => {
                        let msg = params
                            .get("error")
                            .and_then(|e| e.get("message"))
                            .or_else(|| params.get("reason"))
                            .and_then(Value::as_str)
                            .unwrap_or("Muse turn failed");
                        return Err(format!("{msg} (terminal: {terminal})"));
                    }
                }
            }
            _ => {}
        }
        if let Some(m) = cut {
            eprintln!(
                "muse-sub: reply cut at fabricated {} record; turn interrupted",
                guard::marker_name(m)
            );
            let _ = host.request(
                "turn/interrupt",
                json!({"commandId": uuid7(), "sessionId": session_id,
                       "turnId": turn_id, "retract": false}),
                msp::RPC_TIMEOUT,
            );
            result.stop_reason = "cut".into();
            return Ok(result);
        }
    }
}

/// The relay TurnFn entry point: Responses request bytes → Responses SSE.
pub fn run_turn(pool: &Arc<Pool>, raw: &[u8], cancel: &AtomicBool) -> Result<Vec<u8>, String> {
    let body: Value = serde_json::from_slice(raw).map_err(|e| format!("bad request json: {e}"))?;
    let model_hint = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let prep = chat::prepare_turn(&body, &model_hint)?;
    let host = msp::ensure()?;

    let mut guard = pool.sessions.lock().map_err(|_| "pool lock".to_string())?;

    // Continuation: same model + verbatim absorbed prefix + matching echo.
    let mut chosen: Option<String> = None;
    let mut tail: &[Value] = &[];
    for (key, s) in guard.iter() {
        if s.model != prep.model {
            continue;
        }
        if let Ok((t, _undelivered)) = chat::continuation(
            &s.absorbed,
            &s.reply_call_ids,
            &s.reply_text,
            &prep.input,
        ) {
            tail = t;
            chosen = Some(key.clone());
            break;
        }
    }

    let fresh = chosen.is_none();
    let mut key = match chosen {
        Some(k) => k,
        None => pool_fresh(&mut guard, &host, &prep.model)?,
    };
    let mut session_id = guard
        .get(&key)
        .ok_or("session vanished")?
        .session_id
        .clone();

    // Fresh sessions take the whole transcript + system block; reused
    // sessions already hold both — only the new tail goes in.
    let (items, system) = if fresh {
        (&prep.input[..], prep.system.as_str())
    } else {
        (tail, "")
    };
    let mut parts = input_parts(items, system)?;
    if let Some(s) = guard.get_mut(&key) {
        if std::mem::take(&mut s.prior_cut) && !fresh {
            parts.insert(0, json!({"type": "text", "text": acp_bridge::guard::CUT_NOTE}));
        }
    }

    let effort = reasoning_effort(prep.effort.as_deref());
    let mut result = drive(&host, &session_id, parts, effort.clone(), cancel);
    if result.is_err() && guard.get(&key).is_some_and(|s| s.rewarmed) {
        // First drain on a re-pooled session proves the resume: a dead
        // wire here means a stale record, not a turn failure — the
        // request is still answerable fresh.
        eprintln!(
            "muse-sub: re-warmed session {session_id} failed its first turn; spawning fresh (fresh:rewarm_dead)"
        );
        guard.remove(&key);
        remove_record(&session_id);
        key = pool_fresh(&mut guard, &host, &prep.model)?;
        session_id = guard
            .get(&key)
            .ok_or("session vanished")?
            .session_id
            .clone();
        let parts = input_parts(&prep.input, &prep.system)?;
        result = drive(&host, &session_id, parts, effort, cancel);
    }

    match result {
        Ok(mut r) => {
            let was_cut = r.stop_reason == "cut";
            if was_cut {
                r.stop_reason = "stop".into();
            }
            guard::clean_items(&mut r.items);
            let (call_ids, reply_text) = chat::reply_signature(&r);
            if let Some(s) = guard.get_mut(&key) {
                s.prior_cut = was_cut;
                s.absorbed = prep.input.clone();
                s.reply_call_ids = call_ids;
                s.reply_text = reply_text;
                s.last_used = Instant::now();
                s.rewarmed = false;
                save_record(&Record {
                    sid: s.session_id.clone(),
                    system: prep.system.clone(),
                    model: s.model.clone(),
                    effort: prep.effort.clone(),
                    absorbed: s.absorbed.clone(),
                    reply_call_ids: s.reply_call_ids.clone(),
                    reply_text: s.reply_text.clone(),
                    prior_cut: s.prior_cut,
                    saved_at: now_secs(),
                });
            }
            let response_id = format!("resp_{}", chat::rand_hex(24));
            Ok(chat::fold_result(&r, &prep.model, &response_id))
        }
        Err(e) => {
            // A dead session can't absorb a retry's delta: drop it so the
            // next turn starts a fresh MSP session rather than replaying a
            // mismatched prefix — and drop its record, so a restart can't
            // re-warm a session with continuation state it no longer holds.
            if let Some(s) = guard.remove(&key) {
                remove_record(&s.session_id);
            }
            Err(e)
        }
    }
}

// ---------- restart-safe pool records ----------
//
// A plugin restart used to strand every pooled session: the pool died
// with the process and the next turn replayed the whole transcript into
// a fresh `session/start`. `muse serve` sessions persist on disk
// (`~/.local/share/muse/sessions/<y>/<m>/<d>/<sid>/session.jsonl`) and
// `session/resume` reattaches them, so each settled turn also writes a
// JSON `Record` under the cache dir; at startup [`rewarm`] re-pools the
// sessions inside [`WARM_WINDOW`] — the next turn then matches as a
// strict continuation instead of replaying history.
//
// Everything here is best-effort: a lost or unreadable record costs a
// cold start, never a failed turn.

/// The persisted pool entry: the session's resume identity plus the
/// strict-continuation state `run_turn` verifies on its next request.
/// A non-JSON file still parses — first line as a bare session id — but
/// carries no continuation state, so it can't re-warm.
#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct Record {
    /// The MSP session id — the `session/resume` target.
    pub sid: String,
    /// Request `instructions` at save time — informational: a resumed
    /// session already holds its original system block.
    #[serde(default)]
    pub system: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub effort: Option<String>,
    /// The request `input` array the session had absorbed at save time
    /// — the strict-continuation prefix the next turn must extend.
    #[serde(default)]
    pub absorbed: Vec<Value>,
    /// Call ids and text of the session's last answer — the echo zone
    /// `continuation` verifies.
    #[serde(default)]
    pub reply_call_ids: Vec<String>,
    #[serde(default)]
    pub reply_text: String,
    /// A pending `CUT_NOTE` for the session's next turn.
    #[serde(default)]
    pub prior_cut: bool,
    /// Unix seconds when written; restores the idle clock.
    #[serde(default)]
    pub saved_at: u64,
}

/// `$XDG_CACHE_HOME/muse-sub` (or `~/.cache/muse-sub`).
fn cache_root() -> PathBuf {
    std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| Path::new(&h).join(".cache")))
        .unwrap_or_else(std::env::temp_dir)
        .join("muse-sub")
}

/// The record file for session `sid` — named by the session id itself
/// (uuid-checked before it becomes a path component).
fn resume_file(sid: &str) -> PathBuf {
    cache_root().join("resume").join(sid)
}

/// Canonical 8-4-4-4-12 hex uuid. Checked before an id becomes a path
/// component or a `session/resume` param.
fn is_uuid(s: &str) -> bool {
    s.len() == 36
        && s.char_indices().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// Write `bytes` to `path` atomically (0600 temp file + rename), parents
/// created 0700 — the same durability rules as claude-sub's records.
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let dir = path.parent().unwrap_or(Path::new("."));
    let mut mkdir = std::fs::DirBuilder::new();
    mkdir.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        mkdir.mode(0o700);
    }
    mkdir.create(dir)?;
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let tmp = dir.join(format!(".{name}.{}.{}.tmp", std::process::id(), uuid7()));
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let res = opts
        .open(&tmp)
        .and_then(|mut f| f.write_all(bytes))
        .and_then(|()| std::fs::rename(&tmp, path));
    if res.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    res
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

fn record_of(text: &str) -> Option<Record> {
    let r = if let Ok(r) = serde_json::from_str::<Record>(text) {
        r
    } else {
        let sid = text.lines().next()?.trim().to_string();
        Record {
            sid,
            ..Default::default()
        }
    };
    is_uuid(&r.sid).then_some(r)
}

/// Every stored record with its file age, freshest first. Files idle
/// past [`RECORD_TTL`] are reaped in passing; the rest feed [`rewarm`] —
/// stale or unreadable ones are skipped, worst case a conversation
/// falls back to a cold start.
fn records() -> Vec<(Record, Duration)> {
    let dir = cache_root().join("resume");
    let now = SystemTime::now();
    let mut out: Vec<(Record, Duration)> = Vec::new();
    for p in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
        if p.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let path = p.path();
        let age = p
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| now.duration_since(t).ok())
            .unwrap_or(Duration::MAX);
        if age > RECORD_TTL {
            let _ = std::fs::remove_file(&path);
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Some(rec) = record_of(&text) else {
            continue;
        };
        out.push((rec, age));
    }
    out.sort_by_key(|(_, age)| *age);
    out
}

/// Write the pool record for a settled turn — resume identity +
/// continuation state — so a plugin restart can re-pool the session.
/// Atomic, 0600, best-effort.
fn save_record(rec: &Record) {
    if !is_uuid(&rec.sid) {
        return;
    }
    if let Ok(text) = serde_json::to_string(rec) {
        let _ = write_atomic(&resume_file(&rec.sid), text.as_bytes());
    }
}

/// Drop a session's record — a session removed over a failed turn must
/// not be re-warmed with continuation state it no longer holds.
fn remove_record(sid: &str) {
    if is_uuid(sid) {
        let _ = std::fs::remove_file(resume_file(sid));
    }
}

/// Muse's own session store: `$XDG_DATA_HOME/muse/sessions` or
/// `~/.local/share/muse/sessions` — the CLI logs each session to
/// `<y>/<m>/<d>/<sid>/session.jsonl`.
fn sessions_root() -> Option<PathBuf> {
    std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| Path::new(&h).join(".local/share")))
        .map(|d| d.join("muse").join("sessions"))
}

/// Whether the CLI's log for `sid` is still on disk — a record whose
/// transcript is gone can't resume, so rewarm skips it. The check also
/// keeps [`rewarm`] from spawning `muse serve` when nothing could resume.
fn session_exists(sid: &str) -> bool {
    if !is_uuid(sid) {
        return false;
    }
    let Some(root) = sessions_root() else {
        return false;
    };
    // <root>/<y>/<m>/<d>/<sid>/session.jsonl — walk the dated tiers.
    let mut dirs = vec![root];
    for _ in 0..3 {
        let mut next = Vec::new();
        for dir in &dirs {
            for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
                let p = e.path();
                if p.is_dir() {
                    next.push(p);
                }
            }
        }
        if next.is_empty() {
            return false;
        }
        dirs = next;
    }
    dirs.iter()
        .any(|d| d.join(sid).join("session.jsonl").is_file())
}

/// Insert a fresh `session/start` into the pool, evicting the oldest
/// entry at the cap. Returns the new pool key.
fn pool_fresh(
    guard: &mut HashMap<String, MuseSession>,
    host: &MspHost,
    model: &str,
) -> Result<String, String> {
    if guard.len() >= MAX_SESSIONS {
        if let Some(oldest) = guard
            .iter()
            .min_by_key(|(_, s)| s.last_used)
            .map(|(k, _)| k.clone())
        {
            guard.remove(&oldest);
        }
    }
    let key = format!("sess-{}", chat::rand_hex(8));
    let session_id = start_session(host, model)?;
    guard.insert(
        key.clone(),
        MuseSession {
            session_id,
            model: model.to_string(),
            absorbed: Vec::new(),
            reply_call_ids: Vec::new(),
            reply_text: String::new(),
            last_used: Instant::now(),
            prior_cut: false,
            rewarmed: false,
        },
    );
    Ok(key)
}

/// Reattach a persisted session on the running host: `session/resume`
/// without its history payload (we track continuation state ourselves),
/// then re-pin the approval mode — a resumed session otherwise keeps the
/// mode it was created with. The session id in the result must echo the
/// request; a different one is refused.
fn resume_session(host: &MspHost, sid: &str) -> Result<(), String> {
    let result = host.request(
        "session/resume",
        json!({"commandId": uuid7(), "sessionId": sid, "excludeItems": true}),
        msp::RPC_TIMEOUT,
    )?;
    match result
        .get("session")
        .and_then(|s| s.get("sessionId"))
        .and_then(Value::as_str)
    {
        Some(got) if got == sid => {}
        other => return Err(format!("session/resume returned {other:?}, expected {sid}")),
    }
    let _ = host.request(
        "session/setApprovalMode",
        json!({"commandId": uuid7(), "sessionId": sid, "mode": approval_mode()}),
        msp::RPC_TIMEOUT,
    );
    Ok(())
}

/// Re-pool sessions whose records are still inside [`WARM_WINDOW`], up
/// to [`MAX_SESSIONS`], freshest first. A plugin restart used to strand
/// them: the pool died with the process and the next turn replayed the
/// whole transcript into a fresh `session/start`. Resumed idle here —
/// the host already holds the conversation — the next turn on the same
/// conversation matches as a strict continuation and only the new tail
/// is sent. Runs once at startup on a background thread; a skipped or
/// refused record degrades to a cold start. A re-pooled session that
/// dies on its first real turn falls back to a fresh spawn
/// (`fresh:rewarm_dead` in `run_turn`).
pub fn rewarm(pool: &Arc<Pool>) {
    let mut guard = match pool.sessions.lock() {
        Ok(g) => g,
        Err(_) => return,
    };
    // Spawning the host only pays off once a resumable record is found —
    // `session_exists` filters cheaply first so an empty store never
    // spawns `muse serve` just to rewarm nothing.
    let mut host: Option<Arc<MspHost>> = None;
    for (rec, age) in records() {
        if guard.len() >= MAX_SESSIONS || age > WARM_WINDOW {
            break;
        }
        // A non-JSON or bare-id record carries no continuation state.
        if rec.absorbed.is_empty() || !session_exists(&rec.sid) {
            continue;
        }
        if host.is_none() {
            match msp::ensure() {
                Ok(h) => host = Some(h),
                Err(e) => {
                    eprintln!("muse-sub: rewarm skipped, host unavailable: {e}");
                    break;
                }
            }
        }
        let Some(h) = host.as_ref() else {
            break;
        };
        match resume_session(h, &rec.sid) {
            Ok(()) => {
                // Restore the idle clock from the record's age so pool
                // eviction keeps its true position across the restart.
                let ago = Instant::now()
                    .checked_sub(Duration::from_secs(now_secs().saturating_sub(rec.saved_at)));
                guard.insert(
                    format!("sess-{}", chat::rand_hex(8)),
                    MuseSession {
                        session_id: rec.sid.clone(),
                        model: rec.model.clone(),
                        absorbed: rec.absorbed.clone(),
                        reply_call_ids: rec.reply_call_ids.clone(),
                        reply_text: rec.reply_text.clone(),
                        last_used: ago.unwrap_or_else(Instant::now),
                        prior_cut: rec.prior_cut,
                        rewarmed: true,
                    },
                );
            }
            Err(e) => {
                eprintln!("muse-sub: rewarm of {} skipped: {e}", rec.sid);
            }
        }
    }
}
