//! Mock-verified relay: a fake `muse serve` host (python, newline
//! JSON-RPC) is spawned via GRAY_MUSE_SUB_COMMAND and driven through the
//! real session/run_turn path — handshake, session/start, turn/start,
//! deltas, usage, terminal, approval auto-decide, userInput refusal and
//! cross-turn session continuation.

use muse_sub::{msp, session};
use serde_json::{json, Value};
use std::sync::atomic::AtomicBool;

const FAKE: &str = r#"#!/usr/bin/env python3
import sys, json, os
LOG = os.environ.get("FAKE_MUSE_LOG", "/dev/null")
def send(o):
    sys.stdout.write(json.dumps(o) + "\n"); sys.stdout.flush()
def log(o):
    with open(LOG, "a") as f: f.write(json.dumps(o) + "\n")
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try: msg = json.loads(line)
    except Exception: continue
    log(msg)
    rid = msg.get("id"); meth = msg.get("method"); p = msg.get("params") or {}
    if rid is None:
        continue  # notification (e.g. initialized)
    if meth == "initialize":
        send({"id": rid, "result": {"schema": {"fingerprint": "fake-1"}}})
    elif meth == "model/list":
        send({"id": rid, "result": {"models": [
            {"modelId": "muse-spark-1.3", "displayLabel": "muse-spark-1.3", "isDefault": True},
            {"modelId": "muse-spark-1.2", "displayLabel": "muse-spark-1.2"}]}})
    elif meth == "session/start":
        send({"id": rid, "result": {"session": {"sessionId": "sess-fake"}}})
    elif meth == "turn/start":
        tid = p.get("commandId", "t1"); sid = p.get("sessionId")
        send({"id": rid, "result": {"turnId": tid}})
        text = " ".join(str(x.get("text", "")) for x in p.get("input", []) if x.get("type") == "text")
        if "approve-me" in text:
            send({"method": "approval/requested", "params": {
                "sessionId": sid, "approvalId": "ap1", "currentRequirementId": "r1",
                "toolName": "bash",
                "availableChoices": [
                    {"choiceId": "c1", "decision": "approved", "label": "Allow"},
                    {"choiceId": "c2", "decision": "denied", "label": "Deny"}]}})
        if "ask-me" in text:
            send({"method": "userInput/requested", "params": {
                "sessionId": sid, "userInputId": "u1", "questions": []}})
        send({"method": "item/delta", "params": {"sessionId": sid, "itemId": "m1",
              "field": "text", "delta": "Hello "}})
        send({"method": "item/delta", "params": {"sessionId": sid, "itemId": "m1",
              "field": "text", "delta": "world"}})
        send({"method": "item/delta", "params": {"sessionId": sid, "itemId": "r1",
              "field": "summary.0", "delta": "musing"}})
        send({"method": "session/tokenUsage", "params": {"sessionId": sid,
              "usage": {"inputTokens": 10, "outputTokens": 5}}})
        if "ask-me" in text:
            send({"method": "turn/completed", "params": {"sessionId": sid, "turnId": tid,
                  "terminal": "cancelled", "reason": "user refused"}})
        else:
            send({"method": "turn/completed", "params": {"sessionId": sid, "turnId": tid,
                  "terminal": "completed",
                  "usage": {"inputTokens": 10, "outputTokens": 5, "cacheReadTokens": 2}}})
    else:
        # approval/decide, userInput/cancel, turn/interrupt, turn/cancel, ...
        send({"id": rid, "result": {}})
"#;

fn log_lines(log: &std::path::Path) -> Vec<Value> {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

fn count_method(log: &[Value], method: &str) -> usize {
    log.iter()
        .filter(|m| m.get("method").and_then(Value::as_str) == Some(method))
        .count()
}

fn user_msg(text: &str) -> Value {
    json!({"role": "user", "content": [{"type": "input_text", "text": text}]})
}

#[test]
fn mock_host_full_turn() {
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("fake-muse");
    std::fs::write(&script, FAKE).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perm = std::fs::metadata(&script).unwrap().permissions();
        perm.set_mode(0o755);
        std::fs::set_permissions(&script, perm).unwrap();
    }
    let log = dir.path().join("requests.jsonl");
    std::env::set_var("GRAY_MUSE_SUB_COMMAND", &script);
    std::env::set_var("FAKE_MUSE_LOG", &log);
    msp::shutdown(); // fresh host under our env

    // Live model/list through the real handshake.
    let host = msp::ensure().unwrap();
    let res = host
        .request("model/list", json!({}), msp::RPC_TIMEOUT)
        .unwrap();
    assert_eq!(res["models"].as_array().unwrap().len(), 2);
    assert_eq!(host.fingerprint().as_deref(), Some("fake-1"));

    // Turn 1: deltas + usage + terminal → folded Responses SSE.
    let pool = session::Pool::new();
    let cancel = AtomicBool::new(false);
    let body = json!({
        "model": "muse-spark-1.3",
        "instructions": "be terse",
        "input": [user_msg("hi")],
    });
    let sse = session::run_turn(&pool, body.to_string().as_bytes(), &cancel).unwrap();
    let s = String::from_utf8(sse).unwrap();
    assert!(s.contains("Hello world"), "sse missing text: {s}");
    assert!(s.contains("musing"), "sse missing reasoning: {s}");
    assert!(s.contains("response.completed"));
    // terminal usage: 10 in + 5 out + 2 cacheRead = 17 total.
    assert!(s.contains("\"total_tokens\":17"), "sse missing usage: {s}");

    // Turn 2: absorbed prefix + reply echo + new tail → session reuse
    // (no second session/start).
    let body2 = json!({
        "model": "muse-spark-1.3",
        "instructions": "be terse",
        "input": [
            {"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
            {"role": "assistant", "content": [{"type": "output_text", "text": "Hello world"}]},
            user_msg("again"),
        ],
    });
    session::run_turn(&pool, body2.to_string().as_bytes(), &cancel).unwrap();
    let entries = log_lines(&log);
    assert_eq!(count_method(&entries, "session/start"), 1);
    // The delta turn must NOT carry the system block again.
    let turn_starts: Vec<&Value> = entries
        .iter()
        .filter(|m| m.get("method").and_then(Value::as_str) == Some("turn/start"))
        .collect();
    assert_eq!(turn_starts.len(), 2);
    let second_input = turn_starts[1]["params"]["input"].to_string();
    assert!(!second_input.contains("be terse"), "system resent: {second_input}");
    assert!(second_input.contains("again"));

    // Turn 3: approval auto-decide (approved choice picked).
    let body3 = json!({"model": "muse-spark-1.3", "input": [user_msg("approve-me")]});
    session::run_turn(&pool, body3.to_string().as_bytes(), &cancel).unwrap();
    let entries = log_lines(&log);
    let decides: Vec<&Value> = entries
        .iter()
        .filter(|m| m.get("method").and_then(Value::as_str) == Some("approval/decide"))
        .collect();
    assert_eq!(decides.len(), 1);
    assert_eq!(decides[0]["params"]["choiceId"], "c1");

    // Turn 4: unanswerable userInput → cancel + failed turn.
    let body4 = json!({"model": "muse-spark-1.3", "input": [user_msg("ask-me")]});
    let err = session::run_turn(&pool, body4.to_string().as_bytes(), &cancel)
        .err()
        .expect("userInput turn should fail");
    assert!(err.contains("clarifying"), "unexpected error: {err}");
    let entries = log_lines(&log);
    assert_eq!(count_method(&entries, "userInput/cancel"), 1);

    msp::shutdown();
}
