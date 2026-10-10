//! MSP client — newline-delimited JSON-RPC 2.0 over a `muse serve` stdio
//! child, mirroring pi-muse-msp's MspHost. One lazy singleton host per
//! sidecar process; turn drivers subscribe to raw notifications and
//! filter by sessionId.
//!
//! Wire shape (verified against pi-muse-msp 0.2.4):
//!   → {"jsonrpc":"2.0","id":N,"method":M,"params":{...}}
//!   ← {"id":N,"result":{...}} | {"id":N,"error":{"code","message","data"}}
//!   ← {"jsonrpc":"2.0","method":M,"params":{...}}        (notification)

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// pi's HOST_RPC_TIMEOUT_MS for control calls.
pub const RPC_TIMEOUT: Duration = Duration::from_secs(15);
/// pi's TURN_START_ACK_TIMEOUT_MS for the turn/start acknowledgement.
pub const TURN_ACK_TIMEOUT: Duration = Duration::from_secs(15);
/// Synthetic notification injected when the host's stdout closes so
/// waiting turn loops fail fast instead of riding out their deadline.
pub const DIED_METHOD: &str = "$host/died";

struct Inner {
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    next_id: AtomicU64,
    pending: Mutex<std::collections::HashMap<u64, Sender<Result<Value, String>>>>,
    subs: Mutex<Vec<Arc<Sender<Value>>>>,
    fingerprint: Mutex<Option<String>>,
}

pub struct MspHost {
    inner: Arc<Inner>,
}

impl MspHost {
    /// Spawn + handshake (`initialize`, then `initialized` notification).
    pub fn spawn(sandboxed: bool) -> Result<MspHost, String> {
        let bin = crate::setup::resolve_command().ok_or_else(|| crate::setup::INSTALL_HINT.to_string())?;
        let mut args = vec!["serve".to_string(), "--trust-workspace".to_string()];
        if !sandboxed {
            args.push("--disable-sandbox".to_string());
        }
        let mut child = Command::new(&bin)
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn `{bin} {}`: {e}", args.join(" ")))?;
        let stdin = child.stdin.take().ok_or("muse serve stdin pipe")?;
        let stdout = child.stdout.take().ok_or("muse serve stdout pipe")?;
        if let Some(stderr) = child.stderr.take() {
            std::thread::Builder::new()
                .name("muse-stderr".into())
                .spawn(move || {
                    let _ = std::io::copy(&mut BufReader::new(stderr), &mut std::io::sink());
                })
                .ok();
        }
        let inner = Arc::new(Inner {
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            next_id: AtomicU64::new(1),
            pending: Mutex::new(std::collections::HashMap::new()),
            subs: Mutex::new(Vec::new()),
            fingerprint: Mutex::new(None),
        });
        {
            let inner = inner.clone();
            std::thread::Builder::new()
                .name("muse-serve-rx".into())
                .spawn(move || reader(stdout, inner))
                .map_err(|e| format!("reader thread: {e}"))?;
        }
        let host = MspHost {
            inner: inner.clone(),
        };
        let result = host.request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": "gray_muse_sub",
                    "title": "Gray Muse Sub",
                    "version": env!("CARGO_PKG_VERSION"),
                }
            }),
            RPC_TIMEOUT,
        )?;
        let fp = result
            .get("schema")
            .and_then(|s| s.get("fingerprint"))
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Ok(mut slot) = inner.fingerprint.lock() {
            *slot = fp;
        }
        host.notify("initialized", json!({}));
        Ok(host)
    }

    pub fn fingerprint(&self) -> Option<String> {
        self.inner.fingerprint.lock().ok()?.clone()
    }

    pub fn alive(&self) -> bool {
        self.inner
            .child
            .lock()
            .map(|mut c| c.try_wait().ok().flatten().is_none())
            .unwrap_or(false)
    }

    /// JSON-RPC request; `Err` on transport failure, timeout, or an
    /// MSP-level error object (formatted like pi's `muse MSP error …`).
    pub fn request(&self, method: &str, params: Value, timeout: Duration) -> Result<Value, String> {
        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel::<Result<Value, String>>();
        self.inner
            .pending
            .lock()
            .map_err(|_| "pending lock".to_string())?
            .insert(id, tx);
        let line = format!(
            "{}\n",
            json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
        );
        let write = self
            .inner
            .stdin
            .lock()
            .map_err(|_| "stdin lock".to_string())
            .and_then(|mut w| {
                w.write_all(line.as_bytes())
                    .and_then(|()| w.flush())
                    .map_err(|e| format!("muse write: {e}"))
            });
        if let Err(e) = write {
            self.inner.pending.lock().ok().map(|mut p| p.remove(&id));
            return Err(e);
        }
        match rx.recv_timeout(timeout) {
            Ok(r) => r,
            Err(_) => {
                self.inner.pending.lock().ok().map(|mut p| p.remove(&id));
                Err(format!("{method} timed out"))
            }
        }
    }

    pub fn notify(&self, method: &str, params: Value) {
        let line = format!(
            "{}\n",
            json!({"jsonrpc": "2.0", "method": method, "params": params})
        );
        if let Ok(mut w) = self.inner.stdin.lock() {
            let _ = w.write_all(line.as_bytes());
            let _ = w.flush();
        }
    }

    /// Subscribe to every host message (requests' replies excluded).
    /// Filter on `method`/`params.sessionId` downstream.
    pub fn subscribe(&self) -> Receiver<Value> {
        let (tx, rx) = mpsc::channel::<Value>();
        if let Ok(mut subs) = self.inner.subs.lock() {
            subs.push(Arc::new(tx));
        }
        rx
    }
}

impl Drop for MspHost {
    fn drop(&mut self) {
        if let Ok(mut c) = self.inner.child.lock() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn reader(stdout: impl std::io::Read, inner: Arc<Inner>) {
    let mut lines = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        match lines.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(msg) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        // Numeric id = a response to one of our requests (pi's rule).
        if let Some(id) = msg.get("id").and_then(Value::as_u64) {
            let tx = inner
                .pending
                .lock()
                .ok()
                .and_then(|mut p| p.remove(&id));
            if let Some(tx) = tx {
                let out = if let Some(err) = msg.get("error") {
                    let kind = err
                        .get("data")
                        .and_then(|d| d.get("kind"))
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    Err(format!(
                        "muse MSP error {}: {} {}",
                        err.get("code").and_then(Value::as_i64).unwrap_or(0),
                        err.get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown error"),
                        kind
                    )
                    .trim_end()
                    .to_string())
                } else {
                    Ok(msg.get("result").cloned().unwrap_or_else(|| json!({})))
                };
                let _ = tx.send(out);
            }
            continue;
        }
        if msg.get("method").and_then(Value::as_str).is_none() {
            continue;
        }
        if let Ok(mut subs) = inner.subs.lock() {
            subs.retain(|tx| tx.send(msg.clone()).is_ok());
        }
    }
    // Host stdout closed: fail pending calls and wake every subscriber.
    if let Ok(mut p) = inner.pending.lock() {
        for (_, tx) in p.drain() {
            let _ = tx.send(Err("muse serve exited".to_string()));
        }
    }
    if let Ok(mut subs) = inner.subs.lock() {
        subs.retain(|tx| {
            tx.send(json!({"method": DIED_METHOD, "params": {}})).is_ok()
        });
    }
}

// ---------- process-wide singleton ----------

static HOST: Mutex<Option<Arc<MspHost>>> = Mutex::new(None);

/// The shared `muse serve` child, (re)spawned on demand — mirrors pi's
/// `host.ensure()`: a dead host is disposed and respawned fresh.
pub fn ensure() -> Result<Arc<MspHost>, String> {
    let mut slot = HOST.lock().map_err(|_| "host lock".to_string())?;
    if let Some(h) = slot.as_ref() {
        if h.alive() {
            return Ok(h.clone());
        }
        *slot = None;
    }
    let host = Arc::new(MspHost::spawn(crate::setup::sandboxed())?);
    *slot = Some(host.clone());
    Ok(host)
}

/// Drop the singleton (plugin/shutdown) so the child exits with us.
pub fn shutdown() {
    if let Ok(mut slot) = HOST.lock() {
        *slot = None;
    }
}

/// Cheap "is the CLI usable" probe for auth: handshake + `model/list`.
/// Returns the schema fingerprint on success.
pub fn probe() -> Result<Option<String>, String> {
    let host = ensure()?;
    let _ = host.request("model/list", json!({}), RPC_TIMEOUT)?;
    Ok(host.fingerprint())
}
