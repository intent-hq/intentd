//! `intentd git-credential` — daemon-backed git credential helper
//! (monorepo#884 Phase 2).
//!
//! Speaks the line-oriented git-credential protocol on stdin/stdout: git
//! writes `key=value` attribute lines terminated by a blank line (or EOF), and
//! a `get` operation answers with `username=`/`password=` lines. Only `get`
//! for `protocol=https` + `host=github.com` is answered — everything else
//! (including `store`/`erase`, other hosts, a daemon that is not running, the
//! gate being off, or no token) prints nothing and exits 0 so git falls
//! through to its remaining helpers/prompt rules. The credential comes from
//! the running daemon over the UDS `system.gitCredential` control RPC
//! (UDS-only by design; never exposed over WSS). The token value is never
//! logged and never appears on argv.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::Path;

use serde_json::{json, Value};

use crate::client::rpc_call;

/// Run the helper for `operation` against the daemon at `socket`. Always
/// returns `Ok(())`: every failure mode is silent by design (git treats a
/// zero-exit helper with no output as "no credential, keep going").
pub async fn run(operation: &str, socket: &Path) -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let attrs = parse_attributes(stdin.lock());
    if let Some((username, password)) = credential_for(operation, &attrs, socket).await {
        let mut stdout = std::io::stdout().lock();
        // A write failure (closed pipe) is as silent as every other miss.
        let _ = writeln!(stdout, "username={username}\npassword={password}");
        let _ = stdout.flush();
    }
    Ok(())
}

/// Decide whether to answer, and fetch the credential from the daemon if so.
async fn credential_for(
    operation: &str,
    attrs: &BTreeMap<String, String>,
    socket: &Path,
) -> Option<(String, String)> {
    if !should_answer(operation, attrs) {
        return None;
    }
    let params = json!({ "pid": std::process::id() });
    let response = rpc_call(socket, "system.gitCredential", params)
        .await
        .ok()?;
    extract_credential(&response)
}

/// Parse git-credential attribute lines (`key=value`, one per line) up to the
/// first blank line or EOF. Later occurrences of a key override earlier ones
/// (matching git's own behavior). Unreadable lines end the parse — the helper
/// then simply fails the gate and stays silent.
pub(crate) fn parse_attributes(reader: impl BufRead) -> BTreeMap<String, String> {
    let mut attrs = BTreeMap::new();
    for line in reader.lines() {
        let Ok(line) = line else { break };
        if line.is_empty() {
            break;
        }
        if let Some((key, value)) = line.split_once('=') {
            attrs.insert(key.to_string(), value.to_string());
        }
    }
    attrs
}

/// The answer gate: only a `get` operation for `protocol=https` on
/// `host=github.com` (case-insensitive, exact host — no subdomains, no
/// explicit port) is eligible. `store`/`erase` and anything else are no-ops.
pub(crate) fn should_answer(operation: &str, attrs: &BTreeMap<String, String>) -> bool {
    if operation != "get" {
        return false;
    }
    let protocol_ok = attrs
        .get("protocol")
        .is_some_and(|p| p.eq_ignore_ascii_case("https"));
    let host_ok = attrs
        .get("host")
        .is_some_and(|h| h.eq_ignore_ascii_case("github.com"));
    protocol_ok && host_ok
}

/// Pull `(username, password)` out of a `system.gitCredential` response
/// envelope; `None` for `credential: null`, errors, or malformed shapes.
/// Values containing control characters are rejected — they would corrupt the
/// line-oriented credential protocol.
pub(crate) fn extract_credential(response: &Value) -> Option<(String, String)> {
    let credential = response.get("result")?.get("credential")?;
    let username = credential.get("username")?.as_str()?;
    let password = credential.get("password")?.as_str()?;
    if username.is_empty() || password.is_empty() {
        return None;
    }
    if [username, password]
        .iter()
        .any(|s| s.chars().any(char::is_control))
    {
        return None;
    }
    Some((username.to_string(), password.to_string()))
}

#[cfg(test)]
mod tests;
