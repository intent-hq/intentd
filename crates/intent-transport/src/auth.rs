//! Bearer auth + origin allow-list (PROTOCOL §2).
//!
//! Ports `src/main/websocket-auth.ts` and `isAllowedWebSocketApiOrigin`
//! (`src/main/websocket-api-server.ts`). The bearer token is 32 random bytes
//! hex-encoded (64 chars) and persisted in the OS keychain via the `keyring`
//! crate under account `server.auth.token` (sensitive — never logged, never
//! returned in plaintext over the wire). [`validate_token`] is length-checked
//! first then compared in constant time. The HTTP-upgrade wiring that *uses*
//! these helpers (401 bad token, 403 when disabled, socket destroy) is M5.3.

use std::sync::Arc;
use std::time::Duration;

use subtle::ConstantTimeEq;
use tokio::task;
use tokio::time::timeout;

use intent_core::{Error, Result};

/// Keychain service name used for `intentd` secrets (matches `intent-sourcecontrol`).
const KEYRING_SERVICE: &str = "intentd";
/// Keychain account/key for the bearer token (`server.auth.token`).
const KEYRING_ACCOUNT: &str = "server.auth.token";
/// Token length in raw bytes; hex-encoded this yields a 64-char token.
const TOKEN_BYTES: usize = 32;

/// Per-call budget for keychain-backed token reads on the WS upgrade path.
/// [`KeyringTokenStore::load_token`] is synchronous FFI into the OS keychain
/// (`Security.framework` / `libsecret` / `wincred`) and can stall indefinitely
/// — locked, prompting, or otherwise unresponsive. [`validate_token_bounded`]
/// moves the load onto a blocking thread and caps it by this budget so the
/// accept task can never wedge a hung upgrade behind a Keychain prompt: on
/// timeout the connection is rejected cleanly and other accepts keep flowing.
pub const TOKEN_OP_TIMEOUT: Duration = Duration::from_secs(3);

/// Abstraction over secret token persistence. The production implementation is
/// [`KeyringTokenStore`]; tests use an in-memory store so they never touch the
/// real user keychain. This is a test seam over the *same* `keyring` crate, not
/// a second keychain abstraction.
pub trait TokenStore: Send + Sync {
    /// Return the stored token, or `None` if unset/unavailable.
    fn load_token(&self) -> Option<String>;
    /// Persist the token, replacing any existing value.
    fn store_token(&self, token: &str) -> Result<()>;
}

/// OS-keychain-backed [`TokenStore`] (the production default).
#[derive(Debug, Default, Clone, Copy)]
pub struct KeyringTokenStore;

impl TokenStore for KeyringTokenStore {
    fn load_token(&self) -> Option<String> {
        let entry = keyring::Entry::new(KEYRING_SERVICE, KEYRING_ACCOUNT).ok()?;
        entry.get_password().ok().filter(|t| !t.is_empty())
    }

    fn store_token(&self, token: &str) -> Result<()> {
        let entry = keyring::Entry::new(KEYRING_SERVICE, KEYRING_ACCOUNT)
            .map_err(|e| Error::Internal(format!("keychain unavailable: {e}")))?;
        entry
            .set_password(token)
            .map_err(|e| Error::Internal(format!("failed to persist auth token: {e}")))
    }
}

/// Generate a new random token (32 bytes hex = 64 chars), persist it, and
/// return it. Port of `generateToken`.
pub fn generate_token(store: &dyn TokenStore) -> Result<String> {
    let token = random_hex_token()?;
    store.store_token(&token)?;
    tracing::info!("generated new WebSocket API token");
    Ok(token)
}

/// Return the persisted token, creating and persisting one only when missing.
/// Port of `getOrCreateToken` / `ensureWebSocketApiToken`.
///
/// This runs synchronously — the underlying [`KeyringTokenStore`] can block on
/// the OS keychain. That is acceptable at startup because the daemon has not
/// begun serving yet: a hung keychain prompt fails startup loudly rather than
/// silently wedging in-flight WS upgrades. The async accept path uses
/// [`validate_token_bounded`], which offloads and bounds the read.
pub fn get_or_create_token(store: &dyn TokenStore) -> Result<String> {
    match store.load_token() {
        Some(existing) if !existing.is_empty() => Ok(existing),
        _ => generate_token(store),
    }
}

/// Validate a candidate token against the stored token using a length-checked,
/// constant-time comparison. Port of `validateToken`.
pub fn validate_token(store: &dyn TokenStore, candidate: &str) -> bool {
    let Some(stored) = store.load_token() else {
        return false;
    };
    token_matches(&stored, candidate)
}

/// Outcome of an async, bounded token validation. The WS upgrade path maps
/// `Invalid` and `Unavailable` to the same `401 Unauthorized` reject (never
/// leak whether the keychain stalled to an unauthenticated caller), while
/// logging distinguishes the two.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidateOutcome {
    /// The candidate matched the stored token.
    Ok,
    /// The candidate did not match, or nothing was stored / no candidate.
    Invalid,
    /// The token store did not answer within [`TOKEN_OP_TIMEOUT`] (or the
    /// blocking task panicked). Treated as a rejection at the upgrade gate.
    Unavailable,
}

/// Async, timeout-bounded variant of [`validate_token`] for the WS upgrade
/// path. The store's synchronous keychain FFI runs on a blocking thread via
/// [`tokio::task::spawn_blocking`], capped by [`TOKEN_OP_TIMEOUT`], so a
/// stalled/prompting OS keychain cannot wedge the accept loop. On timeout or
/// panic the outcome is [`ValidateOutcome::Unavailable`] — callers reject the
/// connection cleanly, other in-flight connections keep working.
pub async fn validate_token_bounded(
    store: Arc<dyn TokenStore>,
    candidate: String,
) -> ValidateOutcome {
    let handle = task::spawn_blocking(move || validate_token(store.as_ref(), &candidate));
    match timeout(TOKEN_OP_TIMEOUT, handle).await {
        Ok(Ok(true)) => ValidateOutcome::Ok,
        Ok(Ok(false)) => ValidateOutcome::Invalid,
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "ws token validation panicked; rejecting");
            ValidateOutcome::Unavailable
        }
        Err(_) => {
            tracing::warn!(
                timeout_ms = TOKEN_OP_TIMEOUT.as_millis() as u64,
                "ws token validation timed out; rejecting",
            );
            ValidateOutcome::Unavailable
        }
    }
}

/// 32 cryptographically-random bytes, lowercase hex-encoded (64 chars).
fn random_hex_token() -> Result<String> {
    let mut bytes = [0u8; TOKEN_BYTES];
    getrandom::getrandom(&mut bytes)
        .map_err(|e| Error::Internal(format!("failed to generate random token: {e}")))?;
    Ok(hex::encode(bytes))
}

/// Constant-time token comparison: reject empty/length-mismatch first, then
/// compare the bytes in constant time. Pure (no keychain) for testability.
fn token_matches(stored: &str, candidate: &str) -> bool {
    if candidate.is_empty() || stored.is_empty() {
        return false;
    }
    if candidate.len() != stored.len() {
        return false;
    }
    candidate.as_bytes().ct_eq(stored.as_bytes()).into()
}

/// Extract a bearer token from an `Authorization` header value, expecting the
/// format `Bearer <token>` (case-insensitive scheme, regex `^Bearer\s+(\S+)$`).
/// Port of `extractBearerToken`.
pub fn extract_bearer_token(authorization: Option<&str>) -> Option<String> {
    let header = authorization?;
    if !header.get(..6)?.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let rest = &header[6..];
    if !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let token = rest.trim_start();
    if token.is_empty() || token.contains(char::is_whitespace) {
        return None;
    }
    Some(token.to_string())
}

/// Extract a token from a request: the `Authorization: Bearer <t>` header first,
/// then the `?token=` query param of `request_target` (e.g. `/ws?token=abc`).
/// Port of the server's private `extractToken(req)`.
pub fn extract_token(authorization: Option<&str>, request_target: &str) -> Option<String> {
    extract_bearer_token(authorization).or_else(|| extract_query_token(request_target))
}

/// First `token` query param of a request target, percent-decoded; `None` when
/// absent or empty (matching `URLSearchParams.get('token')` truthiness).
fn extract_query_token(request_target: &str) -> Option<String> {
    let query = request_target.split_once('?')?.1;
    let query = query.split('#').next().unwrap_or(query);
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if percent_decode(key) == "token" {
            let value = percent_decode(value);
            return if value.is_empty() { None } else { Some(value) };
        }
    }
    None
}

/// Minimal `application/x-www-form-urlencoded` decode: `+` → space and `%XX`
/// → byte (invalid escapes are left verbatim). UTF-8 is decoded lossily.
fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi * 16 + lo) as u8);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Whether a WebSocket upgrade `Origin` is allowed. Native clients (iOS, CLI)
/// send no Origin and pass; cross-origin browser upgrades are rejected. Port of
/// `isAllowedWebSocketApiOrigin`.
pub fn is_allowed_origin(origin: Option<&str>) -> bool {
    is_allowed_origin_with_host(origin, &local_hostname())
}

/// Lowercased OS hostname (`os.hostname()` equivalent), or empty on failure.
fn local_hostname() -> String {
    whoami::fallible::hostname()
        .map(|h| h.to_lowercase())
        .unwrap_or_default()
}

/// Core origin matcher, parameterized on the local hostname for hermetic tests.
fn is_allowed_origin_with_host(origin: Option<&str>, local_host: &str) -> bool {
    let origin = match origin {
        None => return true,
        Some(o) => o,
    };
    if origin.is_empty() {
        return true;
    }
    if origin == "null" {
        return false;
    }
    if origin.starts_with("file://") {
        return true;
    }
    let Some(hostname) = origin_hostname(origin) else {
        return false;
    };
    let hostname = hostname.to_lowercase();
    if matches!(
        hostname.as_str(),
        "localhost" | "127.0.0.1" | "::1" | "[::1]"
    ) {
        return true;
    }
    let clean_host = hostname.trim_start_matches('[').trim_end_matches(']');
    if local_host.is_empty() {
        return false;
    }
    clean_host == local_host
        || clean_host == format!("{local_host}.local")
        || format!("{clean_host}.local") == local_host
}

/// Extract the hostname from an absolute origin (`scheme://host[:port]`),
/// preserving `[...]` for IPv6 literals to mirror WHATWG `URL.hostname`.
fn origin_hostname(origin: &str) -> Option<String> {
    let after_scheme = origin.split_once("://")?.1;
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    if authority.is_empty() {
        return None;
    }
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    if let Some(end) = host_port.find(']') {
        return Some(host_port[..=end].to_string());
    }
    let host = host_port.split(':').next().unwrap_or(host_port);
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

/// Whether bearer auth is required. Defaults per the config table: `true` on a
/// TCP listener, `false` otherwise (UDS). An explicit `server.auth.enabled`
/// setting overrides the default.
pub fn is_auth_enabled(configured: Option<bool>, tcp: bool) -> bool {
    configured.unwrap_or(tcp)
}

/// Whether mDNS network discovery is enabled. Defaults to `false`; an explicit
/// `server.discovery.enabled` setting overrides.
pub fn is_discovery_enabled(configured: Option<bool>) -> bool {
    configured.unwrap_or(false)
}

#[cfg(test)]
mod tests;
