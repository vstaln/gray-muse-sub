//! Binary resolution + auth probing for the Muse Code CLI (`muse`).
//!
//! MSP itself carries no key material: `muse serve` authenticates with
//! whatever the CLI holds (Muse/Command Code account login). Like
//! cursor-sub, the sidecar treats a working `muse` binary as the
//! credential — `auth/refresh` proves it with a serve handshake.

use serde_json::{json, Value};
use std::process::{Command, Stdio};

pub const INSTALL_HINT: &str =
    "Muse CLI not found — install Muse Code from commandcode.ai and ensure `muse` is on PATH";
pub const LOGIN_HINT: &str =
    "Muse CLI could not serve sessions — run `muse login` (or `cmd login`) to authenticate";

/// Mirror of cursor-sub's GRAY_CURSOR_SUB_COMMAND / pi's PI_MUSE_BINARY.
const BINARY_ENV: &str = "GRAY_MUSE_SUB_COMMAND";
/// Sandboxed by default: `muse serve` runs with workspace trust only
/// (approvalMode onRequest, auto-decided). `GRAY_MUSE_SUB_UNSANDBOXED=1`
/// restores the pi-muse-msp default of --disable-sandbox + allowAll;
/// `GRAY_MUSE_SUB_SANDBOXED=1` is still accepted as a no-op.
const UNSANDBOX_ENV: &str = "GRAY_MUSE_SUB_UNSANDBOXED";
const MAX_STEPS_ENV: &str = "GRAY_MUSE_SUB_MAX_STEPS";

pub fn resolve_command() -> Option<String> {
    if let Ok(explicit) = std::env::var(BINARY_ENV) {
        let explicit = explicit.trim().to_string();
        if !explicit.is_empty() {
            return Some(explicit);
        }
    }
    which_on_path("muse")
}

pub fn sandboxed() -> bool {
    !matches!(
        std::env::var(UNSANDBOX_ENV).map(|v| v.trim().to_lowercase()),
        Ok(v) if v == "1" || v == "true" || v == "yes"
    )
}

/// Per-turn cap on billed model steps (one `session/tokenUsage` each);
/// 0 = unlimited.
pub fn max_steps() -> u64 {
    std::env::var(MAX_STEPS_ENV)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(30)
}

/// `muse --version` probe — install check only, never auth.
pub fn cli_version() -> Option<String> {
    let bin = resolve_command()?;
    let out = Command::new(&bin)
        .arg("--version")
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let v = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if v.is_empty() {
        None
    } else {
        Some(v)
    }
}

/// Best-effort interactive login (`muse login`; Command Code's CLI uses
/// `cmd login` — same product family). The child is returned for the
/// auth-op lifecycle; stdout may carry a verification URL.
pub fn spawn_login() -> std::io::Result<std::process::Child> {
    let bin = resolve_command().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, INSTALL_HINT)
    })?;
    Command::new(bin)
        .arg("login")
        .env("NO_OPEN_BROWSER", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
}

pub fn logout() {
    if let Some(bin) = resolve_command() {
        let _ = Command::new(bin)
            .arg("logout")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn which_on_path(name: &str) -> Option<String> {
    let path = std::env::var("PATH").ok()?;
    for dir in path.split(':') {
        let candidate = format!("{dir}/{name}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(m) = std::fs::metadata(&candidate) {
                if m.is_file() && m.permissions().mode() & 0o111 != 0 {
                    return Some(candidate);
                }
            }
        }
        #[cfg(not(unix))]
        if std::fs::metadata(&candidate).map(|m| m.is_file()).unwrap_or(false) {
            return Some(candidate);
        }
    }
    None
}

/// Credential material for a working muse host.
pub fn login_material(fingerprint: Option<&str>) -> Value {
    json!({
        "secrets": {"muse_cli": "1"},
        "metadata": {
            "binary": resolve_command().unwrap_or_default(),
            "cli_version": cli_version().unwrap_or_default(),
            "schema_fingerprint": fingerprint.unwrap_or(""),
            "sandboxed": sandboxed(),
        },
        "expires_at": null,
    })
}
