//! Bearer auth + origin allow-list (PROTOCOL §2).
//!
//! Ports `src/main/websocket-auth.ts` and `isAllowedWebSocketApiOrigin`
//! (`src/main/websocket-api-server.ts`). The bearer token is 32 random bytes
//! hex-encoded (64 chars) and persisted in the shared file-backed secrets
//! store ([`intent_core::FileSecretStore`], `~/intent/secrets.json`) under
//! account `server.auth.token` (sensitive — never logged, never returned in
//! plaintext over the wire). [`validate_token`] is length-checked first then
//! compared in constant time. The HTTP-upgrade wiring that *uses* these
//! helpers (401 bad token, 403 when disabled, socket destroy) is M5.3.
//!
//! Every backing secret-store call runs on the tokio blocking pool with a
//! bounded timeout so a stalled backing store never blocks an async worker.
//! The in-memory TTL cache on [`AsyncTokenStore`] keeps repeat WSS upgrades
//! cheap and caps how many blocking-pool threads a stuck store can pile up.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use subtle::ConstantTimeEq;
use tokio::sync::watch;
use tokio::time::timeout;

use intent_core::{Error, Result};

/// Secrets-store account/key for the bearer token (`server.auth.token`).
const TOKEN_ACCOUNT: &str = "server.auth.token";
/// Token length in raw bytes; hex-encoded this yields a 64-char token.
const TOKEN_BYTES: usize = 32;

/// Default bounded wait for a token **read** before the caller gives up.
/// Mirrors `intent-services::AsyncSecretStore` so a stalled backing store
/// never wedges the WSS upgrade path.
const DEFAULT_LOAD_TIMEOUT: Duration = Duration::from_secs(3);
/// Default bounded wait for a token **write**. Longer than the read budget so
/// a slow disk never spuriously fails a persist; still must not block the
/// runtime forever.
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a load result (present or absent) is served from the in-process
/// cache before the next call re-consults the backing store. Keeps repeat WSS
/// upgrades cheap without turning token rotations into a long propagation
/// window.
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(60);
/// Rate-limit window for the `secret-store load/write timed out` warning so a
/// wedged backing store doesn't drown the daemon log.
const DEFAULT_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Abstraction over secret token persistence. The production implementation is
/// [`FileTokenStore`]; tests use an in-memory store so they never touch the
/// real secrets file.
pub trait TokenStore: Send + Sync {
    /// Return the stored token, or `None` if unset/unavailable.
    fn load_token(&self) -> Option<String>;
    /// Persist the token, replacing any existing value.
    ///
    /// # Errors
    ///
    /// Returns an error if the token cannot be persisted to the backing store.
    fn store_token(&self, token: &str) -> Result<()>;
}

/// File-backed [`TokenStore`] (the production default): delegates to the
/// shared [`intent_core::FileSecretStore`] (`~/intent/secrets.json`) under
/// account `server.auth.token` — the same entry `settings.*` redacts.
#[derive(Debug, Default, Clone)]
pub struct FileTokenStore {
    secrets: intent_core::FileSecretStore,
}

impl TokenStore for FileTokenStore {
    fn load_token(&self) -> Option<String> {
        self.secrets.load(TOKEN_ACCOUNT).ok().flatten()
    }

    fn store_token(&self, token: &str) -> Result<()> {
        self.secrets.store(TOKEN_ACCOUNT, token)
    }
}

/// Async, single-flight, TTL-cached wrapper around a synchronous [`TokenStore`]
/// so blocking secret-store calls never wedge the tokio runtime. Every backing
/// call runs on the blocking pool via [`tokio::task::spawn_blocking`]; reads
/// are bounded by a short timeout and coalesced via single-flight (mirroring
/// `AsyncSecretStore`'s tokio-watch pattern) so a hung backing store occupies
/// at most one blocking-pool thread total. Cache entries are invalidated on
/// successful writes and expire on TTL. Cheap to clone (state behind `Arc`).
#[derive(Clone)]
pub struct AsyncTokenStore {
    inner: Arc<dyn TokenStore>,
    state: Arc<Mutex<TokenState>>,
    load_timeout: Duration,
    write_timeout: Duration,
    cache_ttl: Duration,
    warn_interval: Duration,
}

/// Combined async state: the single cache slot, timeout-warn rate-limit
/// bookkeeping, and the monotonic counter used by the generation guard in
/// [`AsyncTokenStore::spawn_load`].
struct TokenState {
    entry: Option<Entry>,
    last_warn: Option<Instant>,
    /// Monotonic counter dispensing a unique `load_id` per in-flight load, so
    /// a delayed `spawn_blocking` result can tell whether it still owns the slot.
    next_load_id: u64,
}

/// The cache slot: either an in-flight load that later resolvers can wait on,
/// or a resolved value valid until `expires_at`.
enum Entry {
    /// A blocking load is in progress. `rx` receives `Some(value)` when the
    /// `spawn_blocking` task finishes; `started_at` lets late waiters shrink
    /// their remaining budget so the effective wait per caller stays bounded.
    /// `load_id` uniquely tags this in-flight load so a delayed completion can
    /// detect an intervening store / newer load and refuse to clobber the
    /// fresher slot.
    InFlight {
        rx: watch::Receiver<Option<Option<String>>>,
        started_at: Instant,
        load_id: u64,
    },
    /// A resolved value cached in-process; served without touching the
    /// backing store until `expires_at`.
    Cached {
        value: Option<String>,
        expires_at: Instant,
    },
}

impl AsyncTokenStore {
    /// Wrap `inner` with the production timeout / TTL defaults.
    pub fn new(inner: Arc<dyn TokenStore>) -> Self {
        Self::with_timings(
            inner,
            DEFAULT_LOAD_TIMEOUT,
            DEFAULT_WRITE_TIMEOUT,
            DEFAULT_CACHE_TTL,
            DEFAULT_WARN_INTERVAL,
        )
    }

    /// Wrap `inner` with explicit timings — used by tests to compress the
    /// timeout / TTL windows so a full round-trip fits in a test budget.
    pub fn with_timings(
        inner: Arc<dyn TokenStore>,
        load_timeout: Duration,
        write_timeout: Duration,
        cache_ttl: Duration,
        warn_interval: Duration,
    ) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(TokenState {
                entry: None,
                last_warn: None,
                next_load_id: 0,
            })),
            load_timeout,
            write_timeout,
            cache_ttl,
            warn_interval,
        }
    }

    /// Read the token, returning `None` on absent / timeout / backing-error.
    /// Concurrent callers are coalesced into a single `spawn_blocking`; a cached
    /// result is served without touching the backing store until it expires.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub async fn load_token(&self) -> Option<String> {
        let action = {
            let mut state = self.state.lock().unwrap();
            match &state.entry {
                Some(Entry::Cached { value, expires_at }) if *expires_at > Instant::now() => {
                    return value.clone();
                }
                Some(Entry::InFlight { rx, started_at, .. }) => LoadAction::Wait {
                    rx: rx.clone(),
                    started_at: *started_at,
                },
                _ => {
                    let (tx, rx) = watch::channel::<Option<Option<String>>>(None);
                    let started_at = Instant::now();
                    let load_id = state.next_load_id;
                    state.next_load_id = state.next_load_id.wrapping_add(1);
                    state.entry = Some(Entry::InFlight {
                        rx: rx.clone(),
                        started_at,
                        load_id,
                    });
                    LoadAction::Start { tx, rx, load_id }
                }
            }
        };
        match action {
            LoadAction::Wait { mut rx, started_at } => {
                let remaining = self.load_timeout.saturating_sub(started_at.elapsed());
                self.await_load(&mut rx, remaining).await
            }
            LoadAction::Start {
                tx,
                mut rx,
                load_id,
            } => {
                self.spawn_load(tx, load_id);
                self.await_load(&mut rx, self.load_timeout).await
            }
        }
    }

    /// Persist `token`, replacing any existing value. Runs the blocking write
    /// off the async runtime with a bounded timeout, then refreshes the cache
    /// so subsequent loads observe the new value without re-hitting the
    /// backing store. Timeouts / backing errors surface as [`Error::Internal`].
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the backing-store write fails or times out.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub async fn store_token(&self, token: &str) -> Result<()> {
        let inner = self.inner.clone();
        let value_owned = token.to_string();
        let handle = tokio::task::spawn_blocking(move || inner.store_token(&value_owned));
        match timeout(self.write_timeout, handle).await {
            Ok(Ok(Ok(()))) => {
                let mut guard = self.state.lock().unwrap();
                guard.entry = Some(Entry::Cached {
                    value: Some(token.to_string()),
                    expires_at: Instant::now() + self.cache_ttl,
                });
                Ok(())
            }
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(join_err)) => Err(Error::Internal(format!(
                "secret-store write task panicked: {join_err}"
            ))),
            Err(_) => {
                self.warn_timeout("secret-store write timed out");
                Err(Error::Internal("secret-store write timed out".to_string()))
            }
        }
    }

    /// Kick off the blocking load, publishing the result via `tx` and swapping
    /// the `InFlight` slot for a Cached one so subsequent callers short-circuit.
    /// Runs to completion even after every awaiting caller has timed out —
    /// that's the point: only ONE blocking-pool thread at a time. The
    /// `load_id` generation guard ensures a delayed completion does NOT
    /// overwrite a slot that an intervening `store_token` / newer load
    /// already refreshed: the write only happens if the slot is still the
    /// `InFlight` tagged with `load_id`.
    fn spawn_load(&self, tx: watch::Sender<Option<Option<String>>>, load_id: u64) {
        let inner = self.inner.clone();
        let state = self.state.clone();
        let ttl = self.cache_ttl;
        tokio::spawn(async move {
            let result: Option<String> =
                match tokio::task::spawn_blocking(move || inner.load_token()).await {
                    Ok(v) => v.filter(|t| !t.is_empty()),
                    Err(join_err) => {
                        tracing::warn!(error = %join_err, "secret-store load task panicked");
                        None
                    }
                };
            {
                let mut guard = state.lock().unwrap();
                let still_ours = matches!(
                    &guard.entry,
                    Some(Entry::InFlight { load_id: id, .. }) if *id == load_id,
                );
                if still_ours {
                    guard.entry = Some(Entry::Cached {
                        value: result.clone(),
                        expires_at: Instant::now() + ttl,
                    });
                }
            }
            let _ = tx.send(Some(result));
        });
    }

    /// Wait up to `remaining` for the in-flight load to publish a value; on
    /// timeout return `None` (the current caller gives up but the underlying
    /// blocking task keeps running and will populate the cache when it
    /// eventually completes, subject to the generation guard).
    async fn await_load(
        &self,
        rx: &mut watch::Receiver<Option<Option<String>>>,
        remaining: Duration,
    ) -> Option<String> {
        if let Some(v) = rx.borrow().clone() {
            return v;
        }
        if remaining.is_zero() {
            self.warn_timeout("secret-store load timed out");
            return None;
        }
        let start = Instant::now();
        loop {
            let left = remaining.saturating_sub(start.elapsed());
            if left.is_zero() {
                self.warn_timeout("secret-store load timed out");
                return None;
            }
            match timeout(left, rx.changed()).await {
                Ok(Ok(())) => {
                    if let Some(v) = rx.borrow().clone() {
                        return v;
                    }
                }
                Ok(Err(_)) => return None,
                Err(_) => {
                    self.warn_timeout("secret-store load timed out");
                    return None;
                }
            }
        }
    }

    /// Emit a rate-limited WARN when a secret-store call times out, so a wedged
    /// backing store surfaces in the daemon log without spamming.
    fn warn_timeout(&self, msg: &str) {
        let should = {
            let mut guard = self.state.lock().unwrap();
            let now = Instant::now();
            match guard.last_warn {
                Some(prev) if now.duration_since(prev) < self.warn_interval => false,
                _ => {
                    guard.last_warn = Some(now);
                    true
                }
            }
        };
        if should {
            tracing::warn!(account = %TOKEN_ACCOUNT, "{msg}");
        }
    }
}

/// Internal choice returned by the entry probe in [`AsyncTokenStore::load_token`].
enum LoadAction {
    /// A load is already in flight; wait on the existing receiver.
    Wait {
        rx: watch::Receiver<Option<Option<String>>>,
        started_at: Instant,
    },
    /// No load in flight; the current caller registered a new `InFlight` slot
    /// (tagged with `load_id`) and now owns the `spawn_blocking` / notify
    /// responsibility.
    Start {
        tx: watch::Sender<Option<Option<String>>>,
        rx: watch::Receiver<Option<Option<String>>>,
        load_id: u64,
    },
}

/// Generate a new random token (32 bytes hex = 64 chars), persist it, and
/// return it. Port of `generateToken`.
///
/// # Errors
///
/// Returns `Error::Internal` if generating random bytes or persisting the token fails.
pub async fn generate_token(store: &AsyncTokenStore) -> Result<String> {
    let token = random_hex_token()?;
    store.store_token(&token).await?;
    tracing::info!("generated new WebSocket API token");
    Ok(token)
}

/// Return the persisted token, creating and persisting one only when missing.
/// Port of `getOrCreateToken` / `ensureWebSocketApiToken`.
///
/// # Errors
///
/// Returns `Error::Internal` if a missing token cannot be generated and persisted.
pub async fn get_or_create_token(store: &AsyncTokenStore) -> Result<String> {
    match store.load_token().await {
        Some(existing) if !existing.is_empty() => Ok(existing),
        _ => generate_token(store).await,
    }
}

/// Validate a candidate token against the stored token using a length-checked,
/// constant-time comparison. Port of `validateToken`.
pub(crate) async fn validate_token(store: &AsyncTokenStore, candidate: &str) -> bool {
    let Some(stored) = store.load_token().await else {
        return false;
    };
    token_matches(&stored, candidate)
}

/// 32 cryptographically-random bytes, lowercase hex-encoded (64 chars).
fn random_hex_token() -> Result<String> {
    let mut bytes = [0u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes)
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
pub(crate) fn extract_bearer_token(authorization: Option<&str>) -> Option<String> {
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
pub(crate) fn extract_token(authorization: Option<&str>, request_target: &str) -> Option<String> {
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
                    out.push(u8::try_from(hi * 16 + lo).expect("hex byte fits in u8"));
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
pub(crate) fn is_allowed_origin(origin: Option<&str>) -> bool {
    is_allowed_origin_with_host(origin, &local_hostname())
}

/// Lowercased OS hostname (`os.hostname()` equivalent), or empty on failure.
fn local_hostname() -> String {
    whoami::hostname()
        .map(|h| h.to_lowercase())
        .unwrap_or_default()
}

/// Core origin matcher, parameterized on the local hostname for hermetic tests.
fn is_allowed_origin_with_host(origin: Option<&str>, local_host: &str) -> bool {
    let Some(origin) = origin else { return true };
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
#[cfg(test)]
pub(crate) fn is_auth_enabled(configured: Option<bool>, tcp: bool) -> bool {
    configured.unwrap_or(tcp)
}

#[cfg(test)]
mod tests;
