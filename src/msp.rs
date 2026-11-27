use muse_code_sdk::connection::{Connection, ConnectionError, StdioTransport};
use muse_code_sdk::handshake::Handshake;
use muse_code_sdk::msp::{ClientCapabilities, ClientInfo, InitializeParams, Notification};
use muse_code_sdk::spawn::{spawn_muse, MuseChild, SpawnOptions};
use serde_json::{json, Value};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{broadcast, watch, Mutex};
use tokio::time::{sleep, timeout};

pub const RPC_TIMEOUT: Duration = Duration::from_secs(15);
pub const POLL: Duration = Duration::from_millis(100);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);

static RUNTIME: LazyLock<std::io::Result<Runtime>> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("muse-sdk")
        .enable_all()
        .build()
});
static HOST: Mutex<Option<Arc<MspHost>>> = Mutex::const_new(None);

pub struct MspHost {
    connection: Connection,
    child: Mutex<Option<MuseChild>>,
    stopped: watch::Receiver<bool>,
    fingerprint: String,
    closing: AtomicBool,
}

impl MspHost {
    async fn spawn(read_only: bool) -> Result<Self, String> {
        let binary = crate::setup::resolve_command().ok_or(crate::setup::INSTALL_HINT)?;
        let mut options = SpawnOptions::default();
        options.muse_bin = Some(binary.into());
        options.cwd = Some(std::env::current_dir().map_err(|e| e.to_string())?);
        options.env = Some([("MUSE_NO_AUTO_UPDATE".into(), "1".into())].into());
        options.shutdown_timeout = Duration::from_secs(2);
        if read_only {
            options.args.extend([
                "--disable-shell".into(),
                "--disable-write".into(),
                "--no-session-log".into(),
            ]);
        } else {
            options.args.push("--trust-workspace".into());
            if !crate::setup::sandboxed() {
                options.args.push("--disable-sandbox".into());
            }
        }
        let mut child = spawn_muse(options).map_err(|e| e.to_string())?;
        let stdin = child.take_stdin().ok_or("Muse stdin pipe missing")?;
        let stdout = child.take_stdout().ok_or("Muse stdout pipe missing")?;
        let (connection, driver) = Connection::new(StdioTransport::new(stdin, stdout));
        let (done, stopped) = watch::channel(false);
        tokio::spawn(async move {
            driver.run().await;
            done.send_replace(true);
        });
        let init = timeout(
            STARTUP_TIMEOUT,
            Handshake::new(connection.clone()).initialize(InitializeParams {
                client_info: ClientInfo {
                    name: "gray_muse_sub".into(),
                    title: Some("Gray Muse Sub".into()),
                    version: env!("CARGO_PKG_VERSION").into(),
                },
                capabilities: Some(ClientCapabilities {
                    user_input_dialogs: Some(false),
                    ..Default::default()
                }),
            }),
        )
        .await;
        match init {
            Ok(Ok(ready)) if ready.initialize_result.schema.version == 1 => {
                let fingerprint = ready.initialize_result.schema.fingerprint.clone();
                Ok(Self {
                    connection: ready.into_connection(),
                    child: Mutex::new(Some(child)),
                    stopped,
                    fingerprint,
                    closing: AtomicBool::new(false),
                })
            }
            other => {
                connection.close().await;
                let _ = child.close().await;
                Err(match other {
                    Ok(Ok(_)) => "Muse returned an unsupported MSP envelope version".into(),
                    Ok(Err(e)) => format!("Muse initialize: {e}"),
                    Err(_) => "Muse initialize timed out".into(),
                })
            }
        }
    }

    pub fn fingerprint(&self) -> Option<String> {
        Some(self.fingerprint.clone())
    }

    fn alive(&self) -> bool {
        !self.closing.load(Ordering::Acquire) && !*self.stopped.borrow()
    }

    pub async fn closed(&self) {
        let mut stopped = self.stopped.clone();
        let _ = stopped.wait_for(|closed| *closed).await;
    }

    pub async fn close(&self) {
        self.closing.store(true, Ordering::Release);
        self.connection.close().await;
        if let Some(mut child) = self.child.lock().await.take() {
            let _ = child.close().await;
        }
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value, ConnectionError> {
        self.connection
            .request(method, params.as_object().cloned())
            .await
            .map(Value::Object)
    }

    pub async fn control(&self, method: &str, params: Value) -> Result<Value, String> {
        match timeout(RPC_TIMEOUT, self.call(method, params)).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => Err(format!("Muse {method}: {error}")),
            Err(_) => {
                self.close().await;
                Err(format!("Muse {method} timed out; host closed"))
            }
        }
    }

    pub fn request(&self, method: &str, params: Value, limit: Duration) -> Result<Value, String> {
        block_on(async {
            timeout(limit, self.call(method, params))
                .await
                .map_err(|_| format!("Muse {method} timed out"))?
                .map_err(|e| format!("Muse {method}: {e}"))
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Notification> {
        self.connection.subscribe_notifications()
    }
}

pub fn block_on<T>(f: impl Future<Output = Result<T, String>>) -> Result<T, String> {
    RUNTIME
        .as_ref()
        .map_err(|e| format!("Muse runtime: {e}"))?
        .block_on(f)
}

pub async fn cancelled(cancel: &AtomicBool) {
    while !cancel.load(Ordering::Acquire) {
        sleep(POLL).await;
    }
}

pub async fn ensure_async() -> Result<Arc<MspHost>, String> {
    let mut slot = HOST.lock().await;
    if let Some(host) = slot.as_ref().filter(|h| h.alive()) {
        return Ok(host.clone());
    }
    if let Some(host) = slot.take() {
        host.close().await;
    }
    let host = Arc::new(MspHost::spawn(false).await?);
    *slot = Some(host.clone());
    Ok(host)
}

pub fn ensure() -> Result<Arc<MspHost>, String> {
    block_on(ensure_async())
}

pub fn shutdown() {
    if LazyLock::get(&RUNTIME).is_some() {
        let _ = block_on(async {
            if let Some(host) = HOST.lock().await.take() {
                host.close().await;
            }
            Ok(())
        });
    }
}

pub fn catalog() -> Result<(Value, Option<String>), String> {
    block_on(async {
        let host = MspHost::spawn(true).await?;
        let result = host.control("model/list", json!({})).await;
        host.close().await;
        Ok((result?, host.fingerprint()))
    })
}

pub fn probe() -> Result<Option<String>, String> {
    catalog().map(|(_, fingerprint)| fingerprint)
}
