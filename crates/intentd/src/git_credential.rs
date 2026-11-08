//! `intentd git-credential` — daemon-backed git credential helper
//! (monorepo#884 Phase 2).
//!
//! Speaks the line-oriented git-credential protocol on stdin/stdout: git
//! writes `key=value` attribute lines terminated by a blank line (or EOF), and
//! a `get` operation answers with `username=`/`password=` lines. Only `get`
//! for GitHub or a path-qualified GitLab HTTPS request is forwarded; the daemon
//! validates its original full instance and current child policy. Everything else
//! (including `store`/`erase`, a daemon that is not running, the
//! gate being off, or no token) prints nothing and exits 0 so git falls
//! through to its remaining helpers/prompt rules. The credential comes from
//! the running daemon over the UDS `system.gitCredential` control RPC
//! (UDS-only by design; never exposed over WSS). The token value is never
//! logged and never appears on argv.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::path::Path;
use std::time::Duration;

use serde_json::{json, Value};

use crate::client::rpc_call;

/// Ceiling on the daemon round-trip for one `get`. A daemon that accepts the
/// connection but never answers (wedged event loop, stalled token resolution)
/// must not wedge the user's fetch/push with it — expiry is a silent miss so
/// git falls through to its remaining helpers.
const DAEMON_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Run the helper for `operation` against the daemon at `socket`. Always
/// returns `Ok(())`: every failure mode is silent by design (git treats a
/// zero-exit helper with no output as "no credential, keep going").
pub async fn run(operation: &str, socket: &Path) -> anyhow::Result<()> {
    let stdin = std::io::stdin();
    let attrs = parse_attributes(stdin.lock());
    if let Some((username, password)) =
        credential_for(operation, &attrs, socket, DAEMON_RPC_TIMEOUT).await
    {
        let mut stdout = std::io::stdout().lock();
        // A write failure (closed pipe) is as silent as every other miss.
        let _ = writeln!(stdout, "username={username}\npassword={password}");
        let _ = stdout.flush();
    }
    Ok(())
}

/// Decide whether to answer, and fetch the credential from the daemon if so.
/// `protocol`/`host` are forwarded so the daemon re-checks the scope gate
/// server-side; the round-trip is bounded by `rpc_timeout` (expiry is a
/// silent miss, like every other failure).
async fn credential_for(
    operation: &str,
    attrs: &BTreeMap<String, String>,
    socket: &Path,
    rpc_timeout: Duration,
) -> Option<(String, String)> {
    if !should_answer(operation, attrs) {
        return None;
    }
    let params = json!({
        "pid": std::process::id(),
        "protocol": attrs.get("protocol"),
        "host": attrs.get("host"),
        "path": attrs.get("path"),
        "username": attrs.get("username"),
    });
    let response = tokio::time::timeout(
        rpc_timeout,
        rpc_call(socket, "system.gitCredential", params),
    )
    .await
    .ok()?
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

/// The preliminary answer gate: GitHub retains its exact host rules; other
/// HTTPS requests require a repository path and daemon-side instance admission.
/// `store`/`erase` and other protocols remain no-ops.
/// A git-supplied `username` (an explicit identity in the remote URL, e.g.
/// `https://alice@github.com/...`) must match the daemon's fixed helper
/// identity — otherwise stay silent so the user's own credentials/prompt win.
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
    let username_ok = attrs
        .get("username")
        .is_none_or(|u| u == intent_git::auth::TOKEN_USERNAME);
    if protocol_ok && host_ok {
        return username_ok;
    }
    // GitLab's identity and grant are selected only by the daemon. The helper
    // forwards a path-bearing HTTPS request, never a token or alternate source.
    let Some(host) = attrs.get("host") else {
        return false;
    };
    let Some(path) = attrs.get("path") else {
        return false;
    };
    protocol_ok
        && !host.is_empty()
        && !path.is_empty()
        && !host.contains(['/', '@', '?', '#', '\\'])
        && !path.starts_with('/')
        && !path.contains(['?', '#', '\\'])
        && ![host, path]
            .iter()
            .any(|value| value.chars().any(char::is_control))
        && attrs.get("username").is_none_or(|user| user == "oauth2")
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

/// The longest token the machine hands out (node-link.md § Forge token on
/// the machine, rule 6), plus its status byte and newline.
const MAX_NODE_REPLY: u64 = 16 * 1024 + 2;

/// `intentd git-credential --node` (nodeProtocol 15), for a top-level remote
/// agent's git: answers `get` for HTTPS github.com with the backend's live
/// token from the machine's intentd at `socket`, else `$GH_TOKEN`. Silent
/// on everything else, like [`run`]. The token is never logged.
pub async fn run_node(operation: &str, socket: &Path) {
    let attrs = parse_attributes(std::io::stdin().lock());
    let Some(token) = node_credential(operation, &attrs, socket, DAEMON_RPC_TIMEOUT).await else {
        return;
    };
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(
        stdout,
        "username={}\npassword={}",
        intent_git::auth::TOKEN_USERNAME,
        token.as_str()
    );
    let _ = stdout.flush();
}

/// The token for a github.com `get`: the machine's live one, else
/// `$GH_TOKEN`.
async fn node_credential(
    operation: &str,
    attrs: &BTreeMap<String, String>,
    socket: &Path,
    wait: Duration,
) -> Option<zeroize::Zeroizing<String>> {
    let github = operation == "get"
        && attrs
            .get("protocol")
            .is_some_and(|p| p.eq_ignore_ascii_case("https"))
        && attrs
            .get("host")
            .is_some_and(|h| h.eq_ignore_ascii_case("github.com"))
        && attrs
            .get("username")
            .is_none_or(|u| u == intent_git::auth::TOKEN_USERNAME);
    if !github {
        return None;
    }
    let live = tokio::time::timeout(wait, machine_token(socket))
        .await
        .ok()
        .flatten();
    live.or_else(|| std::env::var("GH_TOKEN").ok().map(zeroize::Zeroizing::new))
        .filter(|token| usable(token))
}

/// Ask the machine's intentd for the held token.
#[cfg(not(unix))]
async fn machine_token(_socket: &Path) -> Option<zeroize::Zeroizing<String>> {
    None
}

/// Ask the machine's intentd for the held token.
#[cfg(unix)]
async fn machine_token(socket: &Path) -> Option<zeroize::Zeroizing<String>> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let mut stream = tokio::net::UnixStream::connect(socket).await.ok()?;
    let request = format!(
        "{} github.com\n",
        intent_transport::node_link::CREDENTIAL_REQUEST
    );
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut reply = zeroize::Zeroizing::new(Vec::new());
    (&mut stream)
        .take(MAX_NODE_REPLY + 1)
        .read_to_end(&mut reply)
        .await
        .ok()?;
    let token = reply.strip_prefix(&[0])?.strip_suffix(b"\n")?;
    let token = std::str::from_utf8(token).ok()?;
    Some(zeroize::Zeroizing::new(token.to_owned()))
}

/// Whether `token` can go on a git-credential line.
fn usable(token: &str) -> bool {
    !token.is_empty()
        && token.len() as u64 <= MAX_NODE_REPLY
        && !token.chars().any(|c| c.is_control() || c.is_whitespace())
}

#[cfg(test)]
mod tests;
