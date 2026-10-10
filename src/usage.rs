//! `provider/usage`: Muse subscription card.
//!
//! The `muse` CLI speaks MSP over stdio; its protocol carries no quota
//! or credit RPC, so there is no usage feed to read. The card reports
//! the connection honestly with no fabricated windows.

use serde_json::{Value, json};

use crate::setup;

/// `provider/usage` entry: CLI presence is the whole signal.
pub fn handle(_params: &Value) -> Result<Value, String> {
    if setup::resolve_command().is_none() {
        return Err("muse CLI not installed".to_string());
    }
    Ok(json!({
        "available": true,
        "title": "Muse",
        "windows": [],
        "note": "the MSP protocol exposes no quota endpoint",
    }))
}
