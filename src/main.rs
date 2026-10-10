//! muse-sub — Gray sidecar that relays chat turns to the Muse Code CLI's
//! `muse serve` MSP host (stdio JSON-RPC; pi-muse-msp parity, model role).

use acp_bridge::relay::{self, Intents, TurnFn};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::Child;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use muse_sub::{msp, session, setup, usage};

const PROVIDER_ID: &str = "muse";
const AUTH_CLI: &str = "muse-cli";
const TOKEN_PREFIX: &str = "muse-sub-";

/// pi-muse-msp's FALLBACK_MODELS — the documented catalog when a live
/// `model/list` can't be read (no binary, cold host, auth pending).
fn fallback_models() -> Vec<Value> {
    [
        ("muse-spark", "Muse Spark (server default)"),
        ("muse-spark-1.3", "muse-spark-1.3"),
        ("muse-spark-1.3-contributor", "muse-spark-1.3-contributor"),
        ("muse-spark-1.2", "muse-spark-1.2"),
        ("muse-spark-1.2-contributor", "muse-spark-1.2-contributor"),
    ]
    .iter()
    .map(|(id, name)| json!({"id": id, "name": name}))
    .collect()
}

fn manifest() -> Value {
    json!({
        "name": "muse-sub",
        "display_name": "Muse",
        "version": env!("CARGO_PKG_VERSION"),
        "protocol": "1.2",
        "capabilities": ["provider.credentials", "host.say"],
        "providers": [{
            "id": PROVIDER_ID,
            "name": "Muse (muse serve MSP)",
            "auth_methods": [
                {"id": AUTH_CLI, "kind": "oauth",
                 "name": "Muse CLI login",
                 "operations": ["login", "refresh", "revoke", "models", "chat"]},
            ],
            "transport": {
                "kind": "openai-responses",
                "base_url": "https://127.0.0.1:1/",
                "authorization": {"kind": "bearer", "secret_name": "relay_token"},
            },
        }],
        "commands": ["/muse"],
    })
}

// ---------- errors ----------

#[derive(Debug)]
enum RpcError {
    Protocol(String),
    Unavailable(String),
    Rpc { code: &'static str, message: String },
}

fn unavailable(msg: impl Into<String>) -> RpcError {
    RpcError::Unavailable(msg.into())
}

fn error_value(e: &RpcError) -> Value {
    match e {
        RpcError::Rpc { code, message } => json!({
            "code": code, "message": message, "retryable": false, "terminal": true,
        }),
        RpcError::Unavailable(message) => json!({
            "code": "unavailable", "message": message, "retryable": true, "terminal": false,
        }),
        RpcError::Protocol(message) => json!({
            "code": "protocol", "message": message, "retryable": false, "terminal": true,
        }),
    }
}

// ---------- login ops (mirrors cursor-sub) ----------

struct LoginOp {
    child: Option<Child>,
    url: Arc<Mutex<Option<String>>>,
}
type Ops = Arc<Mutex<HashMap<String, LoginOp>>>;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

fn completed_now() -> Value {
    json!({
        "operation_id": "",
        "status": "completed",
        "verification_uri": "",
        "expires_at": now_secs() + 600,
        "retry_after_ms": 0,
    })
}

fn auth_start(ops: &Ops, params: &Value) -> Result<Value, RpcError> {
    let method = params.get("auth_method").and_then(Value::as_str).unwrap_or_default();
    if method != AUTH_CLI {
        return Err(RpcError::Protocol(format!("unknown auth method {method}")));
    }
    if setup::resolve_command().is_none() {
        return Err(unavailable(setup::INSTALL_HINT));
    }
    if msp::probe().is_ok() {
        return Ok(completed_now());
    }
    let mut child = setup::spawn_login().map_err(|e| unavailable(format!("muse login: {e}")))?;
    let url: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    if let Some(stdout) = child.stdout.take() {
        let url2 = url.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(start) = line.find("http") {
                    let end = line[start..]
                        .find(|c: char| c.is_whitespace() || c == '"' || c == '\'')
                        .map(|e| start + e)
                        .unwrap_or(line.len());
                    if let Ok(mut u) = url2.lock() {
                        if u.is_none() {
                            *u = Some(line[start..end].to_string());
                        }
                    }
                }
            }
        });
    }
    let op_id = uuid::Uuid::new_v4().to_string();
    ops.lock().unwrap().insert(
        op_id.clone(),
        LoginOp {
            child: Some(child),
            url,
        },
    );
    Ok(json!({
        "operation_id": op_id,
        "status": "pending",
        "verification_uri": "",
        "expires_at": now_secs() + 900,
        "retry_after_ms": 2000,
    }))
}

fn auth_poll(ops: &Ops, params: &Value) -> Result<Value, RpcError> {
    let op_id = params.get("operation_id").and_then(Value::as_str).unwrap_or_default();
    // Any working host settles the op, whatever login path got it there.
    if let Ok(fp) = msp::probe() {
        if !op_id.is_empty() {
            ops.lock().unwrap().remove(op_id);
        }
        return Ok(json!({"state": "completed",
            "credential": setup::login_material(fp.as_deref())}));
    }
    if op_id.is_empty() {
        return Ok(json!({"state": "failed", "error": {
            "code": "unavailable", "message": setup::LOGIN_HINT,
            "retryable": false, "terminal": true,
        }}));
    }
    let (child_done, url) = {
        let mut g = ops.lock().unwrap();
        let Some(op) = g.get_mut(op_id) else {
            return Ok(json!({"state": "operation_lost"}));
        };
        let done = match op.child.as_mut().map(|c| c.try_wait()) {
            Some(Ok(Some(status))) => Some(status.success()),
            Some(Err(_)) => Some(false),
            _ => None,
        };
        let url = op.url.lock().ok().and_then(|u| u.clone());
        (done, url)
    };
    match child_done {
        Some(false) => {
            ops.lock().unwrap().remove(op_id);
            Ok(json!({"state": "failed", "error": {
                "code": "login_failed",
                "message": setup::LOGIN_HINT,
                "retryable": false, "terminal": true,
            }}))
        }
        _ => Ok(json!({
            "state": "pending",
            "verification_uri": url.unwrap_or_default(),
            "retry_after_ms": 2000,
        })),
    }
}

fn auth_cancel(ops: &Ops, params: &Value) -> Result<Value, RpcError> {
    let op_id = params.get("operation_id").and_then(Value::as_str).unwrap_or_default();
    if let Some(mut op) = ops.lock().unwrap().remove(op_id) {
        if let Some(mut c) = op.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
    Ok(json!({}))
}

fn auth_refresh(params: &Value) -> Result<Value, RpcError> {
    let method = params.get("auth_method").and_then(Value::as_str).unwrap_or_default();
    if method != AUTH_CLI {
        return Err(RpcError::Protocol(format!("unknown auth method {method}")));
    }
    match msp::probe() {
        Ok(fp) => Ok(json!({"credential": setup::login_material(fp.as_deref())})),
        Err(e) => {
            if setup::resolve_command().is_none() {
                Err(unavailable(setup::INSTALL_HINT))
            } else {
                Err(RpcError::Rpc {
                    code: "invalid_grant",
                    message: format!("{} ({e})", setup::LOGIN_HINT),
                })
            }
        }
    }
}

fn auth_revoke(params: &Value) -> Result<Value, RpcError> {
    let method = params.get("auth_method").and_then(Value::as_str).unwrap_or_default();
    if method == AUTH_CLI {
        msp::shutdown();
        setup::logout();
    }
    Ok(json!({}))
}

// ---------- models + chat ----------

fn gate() -> Result<(), RpcError> {
    if setup::resolve_command().is_none() {
        return Err(unavailable(setup::INSTALL_HINT));
    }
    Ok(())
}

/// Live `model/list` rows → catalog; a default row brings back the
/// bare "muse-spark" server-default alias (pi parity).
fn live_models() -> Result<Vec<Value>, String> {
    let host = msp::ensure()?;
    let result = host.request("model/list", json!({}), msp::RPC_TIMEOUT)?;
    let rows = result
        .get("models")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out: Vec<Value> = Vec::new();
    let mut has_default = false;
    for row in rows {
        let id = row.get("modelId").and_then(Value::as_str).unwrap_or("");
        if id.is_empty() {
            continue;
        }
        if row.get("isDefault").and_then(Value::as_bool) == Some(true) {
            has_default = true;
        }
        out.push(json!({
            "id": id,
            "name": row
                .get("displayLabel")
                .and_then(Value::as_str)
                .unwrap_or(id),
        }));
    }
    if has_default {
        out.insert(0, json!({"id": "muse-spark", "name": "Muse Spark (server default)"}));
    }
    Ok(out)
}

fn provider_models(_params: &Value) -> Result<Value, RpcError> {
    // Documented fallback ships the catalog even when the live list
    // can't be read (task spec: "live or documented fallback").
    let rows = live_models().unwrap_or_else(|_| fallback_models());
    Ok(json!({
        "provider": PROVIDER_ID,
        "models": rows,
        "default_model": "muse-spark",
    }))
}

fn provider_chat(
    intents: &Intents,
    pool: &Arc<session::Pool>,
    params: &Value,
) -> Result<Value, RpcError> {
    let provider = params.get("provider").and_then(Value::as_str).unwrap_or_default();
    if provider != PROVIDER_ID {
        return Err(RpcError::Protocol("unknown provider".into()));
    }
    gate()?;
    let model = params.get("model").and_then(Value::as_str).unwrap_or_default();
    let pool2 = pool.clone();
    let turn: TurnFn = Arc::new(move |raw: &[u8], cancel: &AtomicBool| {
        session::run_turn(&pool2, raw, cancel)
    });
    let (relay_url, relay_token) =
        relay::mint_turn(intents, model, turn, TOKEN_PREFIX).map_err(unavailable)?;
    Ok(json!({
        "relay_url": relay_url,
        "relay_token": relay_token,
    }))
}

fn command_run(params: &Value) -> Result<Value, RpcError> {
    let name = params.get("name").and_then(Value::as_str).unwrap_or_default();
    if name != "/muse" {
        return Err(RpcError::Protocol(format!("unknown command {name}")));
    }
    let argv: Vec<String> = params
        .get("argv")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default();
    match argv.first().map(String::as_str).unwrap_or("") {
        "" => Ok(json!({})), // bare /muse → provider-login shortcut
        "status" => {
            let text = match setup::resolve_command() {
                None => setup::INSTALL_HINT.to_string(),
                Some(bin) => match setup::cli_version() {
                    Some(v) => match msp::probe() {
                        Ok(fp) => format!(
                            "Muse CLI: {bin} {v}; serve handshake ok{}",
                            fp.map(|f| format!(" (schema {f})")).unwrap_or_default()
                        ),
                        Err(e) => format!("Muse CLI: {bin} {v}; serve probe failed: {e}"),
                    },
                    None => format!("Muse CLI found at {bin} but `muse --version` failed"),
                },
            };
            Ok(json!({"text": text}))
        }
        _ => Err(RpcError::Protocol("usage: /muse [status]".into())),
    }
}

// ---------- sidecar loop ----------

fn dispatch(
    method: &str,
    params: &Value,
    ops: &Ops,
    intents: &Intents,
    pool: &Arc<session::Pool>,
) -> Result<Value, RpcError> {
    match method {
        "plugin/manifest" => Ok(manifest()),
        "provider/auth/start" => auth_start(ops, params),
        "provider/auth/poll" => auth_poll(ops, params),
        "provider/auth/cancel" => auth_cancel(ops, params),
        "provider/auth/refresh" => auth_refresh(params),
        "provider/auth/revoke" => auth_revoke(params),
        "provider/usage" => usage::handle(&params).map_err(unavailable),
        "provider/models" => provider_models(params),
        "provider/chat" => provider_chat(intents, pool, params),
        "command/run" => command_run(params),
        "plugin/tools" => Ok(json!({"tools": []})),
        "event/notify" => Ok(json!({})),
        "plugin/shutdown" => {
            msp::shutdown();
            Ok(json!({}))
        }
        _ => Err(RpcError::Protocol(format!("unknown method {method}"))),
    }
}

fn write_response(out: &mut impl Write, id: Value, outcome: Result<Value, RpcError>) -> bool {
    let response = match outcome {
        Ok(result) => json!({"id": id, "result": result}),
        Err(e) => json!({"id": id, "error": error_value(&e)}),
    };
    writeln!(out, "{response}")
        .and_then(|()| out.flush())
        .is_ok()
}

fn main() {
    if std::env::args().nth(1).as_deref() == Some("manifest") {
        println!("{}", manifest());
        return;
    }
    let ops: Ops = Arc::new(Mutex::new(HashMap::new()));
    let intents: Intents = Default::default();
    let pool = session::Pool::new();
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(req) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let id = req.get("id").cloned().unwrap_or(Value::Null);
        let method = req
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let params = req.get("params").cloned().unwrap_or_else(|| json!({}));
        let result = dispatch(&method, &params, &ops, &intents, &pool);
        if !id.is_null() && !write_response(&mut out, id, result) {
            return;
        }
        if method == "plugin/shutdown" {
            break;
        }
    }
}
