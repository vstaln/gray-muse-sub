use gray_acp::chat::{self, OutItem, TurnResult, Usage};
use gray_acp::guard::{self, TurnGuard};
use muse_code_sdk::connection::ConnectionError;
use muse_code_sdk::fold::{FoldOutcome, SessionFold};
use muse_code_sdk::msp::{ErrorKind, SessionStartResult, TurnCompletedParams, TurnStartResult};
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime};
use tokio::sync::{broadcast, Mutex};
use tokio::time::{sleep_until, timeout, Instant};

use crate::msp::{self, MspHost};

const MAX_SESSIONS: usize = 8;
const WARM_WINDOW: Duration = Duration::from_secs(6 * 3600);
const RECORD_TTL: Duration = Duration::from_secs(24 * 3600);
const TURN_TIMEOUT: Duration = Duration::from_secs(900);
const STOP_GRACE: Duration = Duration::from_secs(5);
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

#[derive(serde::Serialize, serde::Deserialize)]
struct Record {
    sid: String,
    #[serde(default)]
    workspace: String,
    #[serde(default)]
    system: String,
    model: String,
    absorbed: Vec<Value>,
    reply_call_ids: Vec<String>,
    reply_text: String,
    #[serde(default)]
    prior_cut: bool,
}

struct Entry {
    record: Record,
    host: Weak<MspHost>,
}

pub struct Pool {
    sessions: Mutex<Option<Vec<Entry>>>,
}

impl Pool {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            sessions: Mutex::new(None),
        })
    }
}

fn command_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

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
            .and_then(|u| {
                u.as_str()
                    .map(str::to_string)
                    .or_else(|| u.get("url").and_then(Value::as_str).map(str::to_string))
            })
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
        let call_id = item.get("call_id").and_then(Value::as_str).unwrap_or("?");
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
        texts.push(format!(
            "## System instructions\n{system}\n\n{ONE_TURN_RULE}"
        ));
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
    let tmp = dir.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        command_id()
    ));
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

fn records() -> Vec<Entry> {
    let now = SystemTime::now();
    let mut records: Vec<(Duration, Entry)> = Vec::new();
    for entry in std::fs::read_dir(cache_root().join("resume"))
        .into_iter()
        .flatten()
        .flatten()
    {
        let name = entry.file_name();
        let Some(sid) = name.to_str().filter(|s| is_uuid(s)) else {
            continue;
        };
        let path = entry.path();
        let Some(age) = entry
            .metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| now.duration_since(t).ok())
        else {
            continue;
        };
        if age > RECORD_TTL {
            let _ = std::fs::remove_file(&path);
        } else if age <= WARM_WINDOW {
            let Some(record) = std::fs::read(&path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Record>(&bytes).ok())
            else {
                continue;
            };
            if record.sid == sid && !record.absorbed.is_empty() {
                records.push((
                    age,
                    Entry {
                        record,
                        host: Weak::new(),
                    },
                ));
            }
        }
    }
    records.sort_by_key(|(age, _)| *age);
    records
        .into_iter()
        .take(MAX_SESSIONS)
        .map(|(_, entry)| entry)
        .collect()
}

fn save_record(record: &Record) {
    if let Ok(bytes) = serde_json::to_vec(record) {
        let _ = write_atomic(&resume_file(&record.sid), &bytes);
    }
}

fn workspace() -> Result<String, String> {
    let cwd = std::env::current_dir().map_err(|e| format!("Muse workspace: {e}"))?;
    Ok(cwd
        .canonicalize()
        .unwrap_or(cwd)
        .to_string_lossy()
        .into_owned())
}

fn permissions() -> (&'static str, &'static str) {
    if crate::setup::sandboxed() {
        ("promptUnmatched", ":ask-me")
    } else {
        ("allowAll", ":unrestricted")
    }
}

async fn start_session(host: &MspHost, model: &str, workspace: &str) -> Result<String, String> {
    let sid = command_id();
    let (mode, profile) = permissions();
    let mut params = json!({
        "commandId": command_id(), "sessionId": sid, "workspaceRoot": workspace,
        "providerId": "meta", "approvalMode": mode, "permissionProfile": profile,
    });
    if !matches!(
        model.trim().to_ascii_lowercase().as_str(),
        "muse-spark" | "spark" | ""
    ) {
        params["modelId"] = json!(model);
    }
    let data = host.control("session/start", params).await?;
    let result: SessionStartResult =
        serde_json::from_value(data).map_err(|e| format!("Muse session/start: {e}"))?;
    if result.session.session_id != sid || result.session.active_turn_id.is_some() {
        return Err("Muse session/start did not return the requested idle session".into());
    }
    Ok(sid)
}

async fn resume_session(host: &MspHost, sid: &str, workspace: &str) -> Result<bool, String> {
    let (mode, profile) = permissions();
    let params = json!({"commandId": command_id(), "sessionId": sid, "excludeItems": true, "permissionProfile": profile});
    let result = match timeout(msp::RPC_TIMEOUT, host.call("session/resume", params)).await {
        Ok(Ok(result)) => result,
        Ok(Err(ConnectionError::Msp(e)))
            if matches!(e.kind(), ErrorKind::SessionNotFound | ErrorKind::NotFound) =>
        {
            return Ok(false)
        }
        Ok(Err(e)) => return Err(format!("Muse session/resume: {e}")),
        Err(_) => {
            host.close().await;
            return Err("Muse session/resume timed out; host closed".into());
        }
    };
    if result.pointer("/session/sessionId").and_then(Value::as_str) != Some(sid) {
        return Err("Muse session/resume returned a different session identity".into());
    }
    if result
        .pointer("/history/noneReason")
        .and_then(Value::as_str)
        == Some("projectionUnavailable")
    {
        return Ok(false);
    }
    if result
        .pointer("/session/activeTurnId")
        .is_some_and(|value| !value.is_null())
    {
        return Err("Muse resumed a session with an unfinished turn".into());
    }
    if result
        .pointer("/session/workspaceRoot")
        .and_then(Value::as_str)
        != Some(workspace)
    {
        return Ok(false);
    }
    let reply = host
        .control(
            "session/setApprovalMode",
            json!({"commandId": command_id(), "sessionId": sid, "mode": mode}),
        )
        .await?;
    if !reply
        .get("status")
        .is_some_and(|status| status == "accepted")
    {
        return Err("Muse did not accept the resumed session's approval mode".into());
    }
    Ok(true)
}

#[derive(Default)]
struct Tokens {
    input: u64,
    output: u64,
    cached: u64,
    steps: u64,
    seen: HashSet<String>,
}

impl Tokens {
    fn add(&mut self, params: &Map<String, Value>) {
        if let Some(cursor) = string(params, "viewCursor") {
            if !self.seen.insert(cursor.into()) {
                return;
            }
        }
        self.steps += 1;
        if let Some(usage) = params.get("usage") {
            let cached = usage
                .get("cacheReadTokens")
                .or_else(|| usage.get("cachedTokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            self.input = self.input.saturating_add(
                params
                    .get("promptTokens")
                    .or_else(|| usage.get("inputTokens"))
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            );
            self.output = self.output.saturating_add(
                usage
                    .get("outputTokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
            );
            self.cached = self.cached.saturating_add(cached);
        }
    }

    fn finish(self, terminal: &TurnCompletedParams) -> Option<Usage> {
        let (input, output, cached) = if self.steps != 0 {
            (self.input, self.output, self.cached)
        } else {
            let u = terminal.usage?;
            (
                u.input_tokens,
                u.output_tokens,
                u.cache_read_tokens.unwrap_or(u.cached_tokens),
            )
        };
        Some(Usage {
            input_tokens: input,
            output_tokens: output,
            total_tokens: input.saturating_add(output),
            extra: json!({"cached_tokens": cached.min(input)}),
        })
    }
}

fn string<'a>(params: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    params.get(key).and_then(Value::as_str)
}

#[derive(Clone, Copy)]
enum Stop {
    Disconnect,
    Timeout,
    StepLimit,
    Transcript,
    Approval,
    Question,
}

impl Stop {
    fn failure(self) -> Option<&'static str> {
        match self {
            Self::Disconnect => Some("cancelled"),
            Self::Timeout => Some("Muse turn timed out"),
            Self::Approval => Some("Muse requested an approval Gray cannot answer interactively"),
            Self::Question => Some("Muse asked a clarifying question; answer it in chat and retry"),
            Self::StepLimit | Self::Transcript => None,
        }
    }
}

fn interrupt(host: &Arc<MspHost>, sid: &str, tid: &str, stop: Stop) -> (Stop, Instant) {
    let host = Arc::clone(host);
    let params =
        json!({"commandId": command_id(), "sessionId": sid, "turnId": tid, "retract": false});
    tokio::spawn(async move {
        let _ = timeout(STOP_GRACE, host.call("turn/interrupt", params)).await;
    });
    (stop, Instant::now() + STOP_GRACE)
}

fn decline_approval(
    host: &Arc<MspHost>,
    sid: &str,
    params: &Map<String, Value>,
    handled: &mut HashSet<(String, String)>,
) -> bool {
    let (Some(id), Some(requirement)) = (
        string(params, "approvalId"),
        string(params, "currentRequirementId"),
    ) else {
        return false;
    };
    if !handled.insert((id.into(), requirement.into())) {
        return true;
    }
    let choice = params
        .get("availableChoices")
        .and_then(Value::as_array)
        .and_then(|choices| {
            ["denied", "abort"].into_iter().find_map(|decision| {
                choices.iter().find_map(|c| {
                    (c.get("decision").and_then(Value::as_str) == Some(decision))
                        .then(|| c.get("choiceId").and_then(Value::as_str))
                        .flatten()
                })
            })
        });
    let Some(choice) = choice else { return false };
    let params = json!({"commandId": command_id(), "sessionId": sid, "approvalId": id, "requirementId": requirement, "choiceId": choice});
    let host = Arc::clone(host);
    tokio::spawn(async move {
        let _ = host.control("approval/decide", params).await;
    });
    true
}

fn project(fold: &SessionFold, order: &[String], tid: &str, stopped: bool) -> (Vec<OutItem>, bool) {
    let mut items = Vec::new();
    let mut guard = TurnGuard::new(RECORDS, Some(guard::LABEL));
    for id in order {
        let Some(item) = fold.items().get(id).and_then(Value::as_object) else {
            continue;
        };
        if string(item, "turnId") != Some(tid)
            || (!stopped && string(item, "status") != Some("completed"))
        {
            continue;
        }
        match string(item, "kind") {
            Some("agentMessage") => {
                let mut text = string(item, "text").unwrap_or_default().to_owned();
                guard.new_segment();
                let cut = guard.feed(&mut text).is_some();
                items.push(OutItem::Message { text });
                if cut {
                    return (items, true);
                }
            }
            Some("reasoning") => {
                let text = item
                    .get("summary")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();
                if !text.is_empty() {
                    items.push(OutItem::Reasoning { text });
                }
            }
            _ => {}
        }
    }
    (items, false)
}

async fn drive(
    host: &Arc<MspHost>,
    sid: &str,
    parts: Vec<Value>,
    effort: Option<String>,
    cancel: &AtomicBool,
) -> Result<TurnResult, String> {
    if cancel.load(Ordering::Acquire) {
        return Err("cancelled".into());
    }
    let mut events = host.subscribe();
    let tid = command_id();
    let mut params = json!({"commandId": tid, "sessionId": sid, "input": parts, "ifBusy": "queue"});
    if let Some(effort) = effort {
        params["reasoningEffort"] = json!(effort);
    }
    let ack = tokio::select! {
        biased;
        ack = timeout(msp::RPC_TIMEOUT, host.call("turn/start", params)) => Some(ack),
        _ = msp::cancelled(cancel) => None,
    };
    let mut stopping = None;
    match ack {
        None => stopping = Some(interrupt(host, sid, &tid, Stop::Disconnect)),
        Some(Ok(Ok(value))) => {
            let valid = serde_json::from_value::<TurnStartResult>(value).is_ok_and(|r| {
                r.command_id == tid
                    && r.turn_id == tid
                    && r.started_new_turn
                    && r.status == muse_code_sdk::msp::CommandStatus::Accepted
            });
            if !valid {
                host.close().await;
                return Err("Muse did not acknowledge the requested fresh turn".into());
            }
        }
        Some(Ok(Err(ConnectionError::Msp(e)))) => return Err(format!("Muse turn/start: {e}")),
        other => {
            host.close().await;
            return Err(match other {
                Some(Ok(Err(e))) => format!("Muse turn/start: {e}"),
                _ => "Muse turn/start timed out; host closed".into(),
            });
        }
    }

    let deadline = Instant::now() + TURN_TIMEOUT;
    let max_steps = crate::setup::max_steps();
    let mut fold = SessionFold::new(sid);
    let mut tokens = Tokens::default();
    let mut order = Vec::new();
    let mut handled = HashSet::new();
    loop {
        let current_deadline = stopping.map_or(deadline, |(_, until)| until);
        let event = tokio::select! {
            biased;
            _ = msp::cancelled(cancel), if stopping.is_none() => {
                stopping = Some(interrupt(host, sid, &tid, Stop::Disconnect));
                continue;
            }
            _ = sleep_until(current_deadline) => {
                if let Some((stop, _)) = stopping {
                    host.close().await;
                    return Err(format!("{}; Muse did not confirm interruption and its host was closed", stop.failure().unwrap_or("Muse turn interrupted")));
                }
                stopping = Some(interrupt(host, sid, &tid, Stop::Timeout));
                continue;
            }
            event = events.recv() => match event {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    host.close().await;
                    return Err("Muse notification delivery gap; host closed".into());
                }
                Err(broadcast::error::RecvError::Closed) => return Err("Muse host exited mid-turn".into()),
            },
            _ = host.closed() => return Err("Muse host exited mid-turn".into()),
        };
        let Some(params) = event.params.as_ref() else {
            continue;
        };
        if string(params, "sessionId") != Some(sid)
            || string(params, "turnId").is_some_and(|id| id != tid)
        {
            continue;
        }
        let item = params.get("item").and_then(Value::as_object);
        if item.is_some_and(|item| string(item, "turnId") != Some(&tid)) {
            continue;
        }
        let new_item = item
            .and_then(|item| string(item, "itemId"))
            .filter(|id| fold.items().get(id).is_none());
        let applied = fold.apply(&event);
        if let Some(id) = new_item.filter(|id| fold.items().get(id).is_some()) {
            order.push(id.to_owned());
        }
        match event.method.as_str() {
            "view/gap" | "session/viewHealthChanged"
                if !fold.is_current()
                    || string(params, "health").is_some_and(|health| health != "healthy") =>
            {
                host.close().await;
                return Err("Muse lost its live session projection; host closed".into());
            }
            "turn/completed" if string(params, "turnId") == Some(&tid) => {
                let terminal: TurnCompletedParams =
                    serde_json::from_value(Value::Object(params.clone()))
                        .map_err(|e| format!("Muse turn/completed: {e}"))?;
                let stop = stopping.map(|(stop, _)| stop);
                if let Some(error) = stop.and_then(Stop::failure) {
                    return Err(error.into());
                }
                let status = &terminal.terminal;
                if *status != muse_code_sdk::msp::TurnTerminal::Completed
                    && !(stop.is_some() && *status == muse_code_sdk::msp::TurnTerminal::Cancelled)
                {
                    return Err(terminal
                        .error
                        .map(|e| e.message)
                        .or(terminal.reason)
                        .unwrap_or_else(|| format!("Muse turn ended as {status:?}")));
                }
                let (mut items, cut) = project(&fold, &order, &tid, stop.is_some());
                if matches!(stop, Some(Stop::StepLimit)) {
                    items.push(OutItem::Message { text: format!("[muse-sub: stopped after {} steps — raise GRAY_MUSE_SUB_MAX_STEPS to allow more]", tokens.steps) });
                }
                return Ok(TurnResult {
                    items,
                    usage: tokens.finish(&terminal),
                    stop_reason: if cut { "cut" } else { "stop" }.into(),
                    ..Default::default()
                });
            }
            "session/tokenUsage" if string(params, "turnId") == Some(&tid) => {
                tokens.add(params);
                if stopping.is_none() && max_steps != 0 && tokens.steps >= max_steps {
                    stopping = Some(interrupt(host, sid, &tid, Stop::StepLimit));
                }
            }
            "approval/requested" | "approval/updated" if stopping.is_none() => {
                if !decline_approval(host, sid, params, &mut handled) {
                    stopping = Some(interrupt(host, sid, &tid, Stop::Approval));
                }
            }
            "userInput/requested" if stopping.is_none() => {
                stopping = Some(interrupt(host, sid, &tid, Stop::Question))
            }
            "item/started" | "item/updated" | "item/completed" | "item/delta"
                if stopping.is_none() && !matches!(applied, FoldOutcome::Ignored) =>
            {
                let id = string(params, "itemId").or_else(|| {
                    params
                        .get("item")
                        .and_then(Value::as_object)
                        .and_then(|item| string(item, "itemId"))
                });
                if id
                    .and_then(|id| fold.items().get(id))
                    .and_then(Value::as_object)
                    .is_some_and(|item| {
                        string(item, "turnId") == Some(&tid)
                            && string(item, "kind") == Some("agentMessage")
                            && string(item, "text").is_some_and(|text| {
                                guard::cut_transcript(text, RECORDS, Some(guard::LABEL))
                                    .1
                                    .is_some()
                            })
                    })
                {
                    stopping = Some(interrupt(host, sid, &tid, Stop::Transcript));
                }
            }
            _ => {}
        }
    }
}

pub fn run_turn(pool: &Arc<Pool>, raw: &[u8], cancel: &AtomicBool) -> Result<Vec<u8>, String> {
    msp::block_on(async {
        let body: Value =
            serde_json::from_slice(raw).map_err(|e| format!("bad request json: {e}"))?;
        let prep = chat::prepare_turn(
            &body,
            body.get("model")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        )?;
        let mut guard = tokio::select! {
            _ = msp::cancelled(cancel) => return Err("cancelled".into()),
            guard = pool.sessions.lock() => guard,
        };
        let entries = guard.get_or_insert_with(records);
        let root = workspace()?;
        let host = msp::ensure_async().await?;
        if cancel.load(Ordering::Acquire) {
            return Err("cancelled".into());
        }
        let candidate = entries.iter().enumerate().find_map(|(index, e)| {
            let r = &e.record;
            if r.model != prep.model
                || r.system != prep.system
                || (!r.workspace.is_empty() && r.workspace != root)
            {
                return None;
            }
            let (tail, undelivered) =
                chat::continuation(&r.absorbed, &r.reply_call_ids, &r.reply_text, &prep.input)
                    .ok()?;
            (!undelivered).then_some((index, tail))
        });
        let mut resumed = None;
        if let Some((index, tail)) = candidate {
            let entry = entries.remove(index);
            if entry
                .host
                .upgrade()
                .is_some_and(|old| Arc::ptr_eq(&old, &host))
                || resume_session(&host, &entry.record.sid, &root).await?
            {
                resumed = Some((entry.record, tail));
            } else {
                let _ = std::fs::remove_file(resume_file(&entry.record.sid));
            }
        }
        let (sid, parts) = if let Some((record, tail)) = resumed {
            let mut parts = input_parts(tail, "")?;
            if record.prior_cut {
                parts.insert(0, json!({"type": "text", "text": guard::CUT_NOTE}));
            }
            (record.sid, parts)
        } else {
            let parts = input_parts(&prep.input, &prep.system)?;
            (start_session(&host, &prep.model, &root).await?, parts)
        };
        let _ = std::fs::remove_file(resume_file(&sid));
        let mut result = drive(
            &host,
            &sid,
            parts,
            reasoning_effort(prep.effort.as_deref()),
            cancel,
        )
        .await?;
        let prior_cut = result.stop_reason == "cut";
        result.stop_reason = "stop".into();
        guard::clean_items(&mut result.items);
        let (reply_call_ids, reply_text) = chat::reply_signature(&result);
        let record = Record {
            sid,
            workspace: root,
            system: prep.system,
            model: prep.model.clone(),
            absorbed: prep.input,
            reply_call_ids,
            reply_text,
            prior_cut,
        };
        save_record(&record);
        entries.insert(
            0,
            Entry {
                record,
                host: Arc::downgrade(&host),
            },
        );
        entries.truncate(MAX_SESSIONS);
        Ok(chat::fold_result(
            &result,
            &prep.model,
            &format!("resp_{}", chat::rand_hex(24)),
        ))
    })
}
