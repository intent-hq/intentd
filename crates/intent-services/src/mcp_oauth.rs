//! Per-MCP-server OAuth token bags (PROTOCOL §5.22 companion). The
//! `mcp.oauth.*` RPC family manages the opaque OAuth bag associated with each
//! external MCP server id; bags are secret material. Every wire response is
//! **presence-only** — the bag itself never leaves the daemon over the wire.
//! Storage lives in the `mcp_oauth_tokens` `SQLite` table (§9.4); internal
//! consumers (e.g. an outbound HTTP request built inside the daemon) read the
//! raw bag through [`Store::get_mcp_oauth_token`], never through the wire.
//!
//! [`McpOauthService::authorization_header`] is refresh-aware: a bag whose
//! `expires_at` sits within [`REFRESH_SKEW_SECS`] of now and that carries the
//! RFC 6749 §6 refresh metadata (`refresh_token` + `token_endpoint` +
//! `client_id`) is refreshed and re-persisted before the header is built.
//! Every refresh problem is fail-soft (WARN + fall back to the stored token);
//! a refresh failure never surfaces as an RPC error.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use intent_core::{now_iso, Error, Result};
use intent_store::Store;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use crate::settings::REDACTED_PLACEHOLDER;

/// Clock-skew guard: a bag whose `expires_at` is within this many seconds of
/// now (or past) is treated as expired and eligible for refresh.
const REFRESH_SKEW_SECS: u64 = 60;

/// After a failed refresh attempt for a server, no new attempt is made for
/// this long — a dead token endpoint must not add latency to every header
/// build.
const REFRESH_FAILURE_COOLDOWN: Duration = Duration::from_secs(60);

/// Bounded budget for the whole refresh POST (connect + response), so a
/// wedged token endpoint cannot hang a header build indefinitely.
const REFRESH_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// `expires_at` values above this threshold are epoch **milliseconds**; at or
/// below, epoch **seconds** (10^12 seconds is ~33,658 CE, 10^12 ms is 2001).
const EXPIRES_AT_MS_THRESHOLD: f64 = 1e12;

/// Per-server single-flight refresh locks. Process-wide statics are safe
/// here: one daemon owns one store, and the service itself is rebuilt per
/// call, so cross-call state must live outside it. Keyed by server id;
/// bounded by the number of configured MCP servers.
static REFRESH_LOCKS: LazyLock<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Instant of the last **failed** refresh attempt per server id (cooldown
/// source; entries are cleared on the next successful refresh).
static REFRESH_FAILED_AT: LazyLock<Mutex<HashMap<String, Instant>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Executor for the `mcp.oauth.*` namespace over the [`Store`]. Construct one
/// per call from the long-lived `Services`; refresh single-flight/cooldown
/// state lives in module statics because instances are per-call.
pub(crate) struct McpOauthService<'a> {
    store: &'a Store,
}

impl<'a> McpOauthService<'a> {
    pub(crate) fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Ensure `server_id` is a non-empty string; empty ids never round-trip
    /// because the FE keys bags by server id.
    fn require_server_id(server_id: &str) -> Result<&str> {
        if server_id.is_empty() {
            return Err(Error::InvalidParams("serverId is required".to_string()));
        }
        Ok(server_id)
    }

    /// `mcp.oauth.list` → `{ tokens: [{ serverId, value }] }` — one entry per
    /// stored bag, sorted by `serverId`. `value` is always the redaction
    /// placeholder (a bag is stored iff a plaintext existed).
    pub(crate) async fn list(&self) -> Result<Value> {
        let ids = self.store.list_mcp_oauth_server_ids().await?;
        let tokens: Vec<Value> = ids
            .into_iter()
            .map(|server_id| {
                json!({
                    "serverId": server_id,
                    "value": REDACTED_PLACEHOLDER,
                })
            })
            .collect();
        Ok(json!({ "tokens": tokens }))
    }

    /// `mcp.oauth.get` → `{ serverId, value }`. `value` is the redaction
    /// placeholder when a bag exists and `null` when it does not. Never
    /// echoes bag contents on the wire.
    pub(crate) async fn get(&self, server_id: &str) -> Result<Value> {
        let server_id = Self::require_server_id(server_id)?;
        let value = match self.store.get_mcp_oauth_token(server_id).await? {
            Some(_) => json!(REDACTED_PLACEHOLDER),
            None => Value::Null,
        };
        Ok(json!({ "serverId": server_id, "value": value }))
    }

    /// `mcp.oauth.set` → persist `token_bag` for `server_id` and return
    /// `{ serverId, value }` with the redaction placeholder as `value`. The
    /// bag itself is **never** echoed. Accepts any JSON body (object / array /
    /// scalar) so the FE's bag shape can evolve without a daemon change.
    pub(crate) async fn set(&self, server_id: &str, token_bag: Value) -> Result<Value> {
        let server_id = Self::require_server_id(server_id)?;
        let raw = serde_json::to_string(&token_bag)
            .map_err(|e| Error::Internal(format!("encode mcp oauth token failed: {e}")))?;
        self.store
            .set_mcp_oauth_token(server_id, &raw, &now_iso())
            .await?;
        Ok(json!({ "serverId": server_id, "value": REDACTED_PLACEHOLDER }))
    }

    /// `mcp.oauth.delete` → drop the persisted bag for `server_id`. Idempotent:
    /// missing bags succeed with `{ success: true }`.
    pub(crate) async fn delete(&self, server_id: &str) -> Result<Value> {
        let server_id = Self::require_server_id(server_id)?;
        self.store.delete_mcp_oauth_token(server_id).await?;
        Ok(json!({ "success": true }))
    }

    /// Build the `Authorization` header value from the stored bag for
    /// `server_id`, when one exists and carries an `access_token`. Internal
    /// consumer seam (§5.22.1): the raw bag is read from the store to build an
    /// outbound request and never crosses the wire. An expired bag carrying
    /// refresh metadata is refreshed (and re-persisted) first — see
    /// [`Self::refresh_if_expired`]; all other bags behave exactly as before.
    /// A lowercase `bearer` token type is capitalized (OAuth servers may
    /// return it lowercase); a missing `token_type` defaults to `Bearer`.
    pub(crate) async fn authorization_header(&self, server_id: &str) -> Result<Option<String>> {
        let Some(raw) = self.store.get_mcp_oauth_token(server_id).await? else {
            return Ok(None);
        };
        let bag: Value = serde_json::from_str(&raw).unwrap_or(Value::Null);
        let bag = self.refresh_if_expired(server_id, bag).await;
        Ok(header_from_bag(&bag))
    }

    /// Refresh the bag's access token when it is expired (within
    /// [`REFRESH_SKEW_SECS`] of `expires_at`) **and** the bag carries the RFC
    /// 6749 §6 refresh metadata. Fail-soft: any missing/unparseable metadata,
    /// network error, non-2xx status, or malformed response logs a WARN and
    /// returns the bag unchanged. Single-flight per server id: concurrent
    /// header builds serialize on a per-server lock and re-read the persisted
    /// bag under it, so one expiry triggers one POST; a failed attempt arms
    /// [`REFRESH_FAILURE_COOLDOWN`]. `mcp.oauth.set` / `mcp.oauth.delete` do
    /// not take the lock, so the post-refresh persist is guarded: the merged
    /// bag is written back only when the stored bag still matches the
    /// snapshot the refresh was computed from — an external replace/revoke
    /// that raced the POST always wins.
    async fn refresh_if_expired(&self, server_id: &str, bag: Value) -> Value {
        match parse_expires_at(&bag) {
            ExpiresAt::Absent => return bag,
            ExpiresAt::Unparseable => {
                tracing::warn!(
                    server = %server_id,
                    "mcp oauth bag has unparseable expires_at; using stored access token"
                );
                return bag;
            }
            ExpiresAt::EpochMs(at_ms) if at_ms > refresh_deadline_ms() => return bag,
            ExpiresAt::EpochMs(_) => {}
        }
        if RefreshParams::from_bag(&bag).is_none() {
            tracing::warn!(
                server = %server_id,
                "mcp oauth token expired but bag lacks refresh metadata \
                 (refresh_token/token_endpoint/client_id); using stored access token"
            );
            return bag;
        }
        let lock = refresh_lock(server_id);
        let _guard = lock.lock().await;
        // Re-read under the lock: a concurrent build may have refreshed and
        // persisted while this task waited, in which case the fresh bag is
        // used as-is (one POST per expiry, not one per caller). The raw
        // snapshot is kept so the post-refresh persist can detect an
        // external `mcp.oauth.set`/`delete` that raced the refresh POST.
        let snapshot = self
            .store
            .get_mcp_oauth_token(server_id)
            .await
            .unwrap_or(None);
        let bag = snapshot
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or(bag);
        let ExpiresAt::EpochMs(at_ms) = parse_expires_at(&bag) else {
            return bag;
        };
        if at_ms > refresh_deadline_ms() {
            return bag;
        }
        let Some(params) = RefreshParams::from_bag(&bag) else {
            return bag;
        };
        if in_failure_cooldown(server_id) {
            return bag;
        }
        match run_refresh(&params).await {
            Ok(resp) => {
                REFRESH_FAILED_AT.lock().unwrap().remove(server_id);
                let merged = merge_refresh_response(&bag, &resp);
                // The `mcp.oauth.set`/`delete` RPCs write without taking the
                // refresh lock, so the stored bag may have been replaced or
                // revoked while the POST was in flight. Persist the merged
                // bag only when the store still holds the exact snapshot
                // this refresh was computed from; otherwise the external
                // mutation wins and the refreshed token is dropped.
                match self.store.get_mcp_oauth_token(server_id).await {
                    Ok(current) if current.is_some() && current == snapshot => {
                        match serde_json::to_string(&merged) {
                            Ok(raw) => {
                                if let Err(e) = self
                                    .store
                                    .set_mcp_oauth_token(server_id, &raw, &now_iso())
                                    .await
                                {
                                    tracing::warn!(
                                        server = %server_id,
                                        error = %e,
                                        "failed to persist refreshed mcp oauth bag"
                                    );
                                }
                            }
                            Err(e) => tracing::warn!(
                                server = %server_id,
                                error = %e,
                                "failed to encode refreshed mcp oauth bag"
                            ),
                        }
                        merged
                    }
                    Ok(Some(current)) => {
                        // Replaced mid-refresh: the header is built from the
                        // replacement the user just stored, not the token
                        // refreshed from superseded credentials.
                        tracing::warn!(
                            server = %server_id,
                            "mcp oauth bag replaced during refresh; \
                             discarding refreshed token and using the replacement"
                        );
                        serde_json::from_str(&current).unwrap_or(Value::Null)
                    }
                    Ok(None) => {
                        // Deleted (revoked) mid-refresh: honor the
                        // revocation fully — no persist and no header for
                        // this request either, since a token minted from
                        // revoked credentials must not outlive the delete.
                        tracing::warn!(
                            server = %server_id,
                            "mcp oauth bag deleted during refresh; discarding refreshed token"
                        );
                        Value::Null
                    }
                    Err(e) => {
                        // Stored state unverifiable: fail-soft — use the
                        // fresh token for this one request but skip the
                        // persist so a concurrent mutation is never
                        // clobbered.
                        tracing::warn!(
                            server = %server_id,
                            error = %e,
                            "failed to re-read mcp oauth bag after refresh; \
                             using refreshed token without persisting"
                        );
                        merged
                    }
                }
            }
            Err(e) => {
                REFRESH_FAILED_AT
                    .lock()
                    .unwrap()
                    .insert(server_id.to_string(), Instant::now());
                tracing::warn!(
                    server = %server_id,
                    error = %e,
                    "mcp oauth token refresh failed; using stored access token"
                );
                bag
            }
        }
    }
}

/// Build `"<TokenType> <access_token>"` from a bag, or `None` when the bag
/// has no non-empty `access_token`. A lowercase `bearer` is capitalized; a
/// missing `token_type` defaults to `Bearer`.
fn header_from_bag(bag: &Value) -> Option<String> {
    let token = bag
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())?;
    let token_type = bag
        .get("token_type")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("Bearer");
    let token_type = if token_type.eq_ignore_ascii_case("bearer") {
        "Bearer"
    } else {
        token_type
    };
    Some(format!("{token_type} {token}"))
}

/// Classified `expires_at` disposition. `Absent` bags never refresh (today's
/// behavior, byte-identical); `Unparseable` is fail-soft (WARN + stored
/// token); `EpochMs` is normalized to epoch milliseconds regardless of
/// whether the bag recorded seconds or milliseconds.
enum ExpiresAt {
    Absent,
    Unparseable,
    EpochMs(u64),
}

/// Parse the bag's `expires_at` into [`ExpiresAt`]. Accepts JSON numbers and
/// numeric strings; values above [`EXPIRES_AT_MS_THRESHOLD`] are already
/// milliseconds, the rest are seconds.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn parse_expires_at(bag: &Value) -> ExpiresAt {
    let Some(v) = bag.get("expires_at") else {
        return ExpiresAt::Absent;
    };
    let n = match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    };
    let Some(n) = n.filter(|n| n.is_finite()) else {
        return ExpiresAt::Unparseable;
    };
    let ms = if n > EXPIRES_AT_MS_THRESHOLD {
        n
    } else {
        n * 1000.0
    };
    ExpiresAt::EpochMs(ms.max(0.0) as u64)
}

/// Current wall-clock time as epoch milliseconds.
fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The epoch-ms instant a bag's `expires_at` must exceed to count as fresh
/// (now plus the [`REFRESH_SKEW_SECS`] skew window).
fn refresh_deadline_ms() -> u64 {
    now_epoch_ms().saturating_add(REFRESH_SKEW_SECS * 1000)
}

/// The per-server single-flight lock, created on first use.
fn refresh_lock(server_id: &str) -> Arc<AsyncMutex<()>> {
    REFRESH_LOCKS
        .lock()
        .unwrap()
        .entry(server_id.to_string())
        .or_default()
        .clone()
}

/// Whether the server's last refresh attempt failed within
/// [`REFRESH_FAILURE_COOLDOWN`].
fn in_failure_cooldown(server_id: &str) -> bool {
    REFRESH_FAILED_AT
        .lock()
        .unwrap()
        .get(server_id)
        .is_some_and(|at| at.elapsed() < REFRESH_FAILURE_COOLDOWN)
}

/// The refresh-grant inputs read from a bag. All three required fields must
/// be non-empty strings for a refresh to be attempted; `client_secret` and
/// `scope` are forwarded when present. 🔒 Holds secret material — no
/// `Debug`/`Serialize`, and the fields never appear in logs or errors.
struct RefreshParams {
    refresh_token: String,
    token_endpoint: String,
    client_id: String,
    client_secret: Option<String>,
    scope: Option<String>,
}

impl RefreshParams {
    fn from_bag(bag: &Value) -> Option<Self> {
        let field = |key: &str| {
            bag.get(key)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(ToString::to_string)
        };
        Some(Self {
            refresh_token: field("refresh_token")?,
            token_endpoint: field("token_endpoint")?,
            client_id: field("client_id")?,
            client_secret: field("client_secret"),
            scope: field("scope"),
        })
    }
}

/// Run one RFC 6749 §6 `refresh_token` grant: form-encoded POST to the bag's
/// `token_endpoint`, bounded by [`REFRESH_HTTP_TIMEOUT`]. Returns the parsed
/// 2xx JSON body (guaranteed to carry a non-empty `access_token`) or an error
/// string that never contains credential material — only the endpoint
/// URL/status can appear in it.
async fn run_refresh(params: &RefreshParams) -> std::result::Result<Value, String> {
    let client = reqwest::Client::builder()
        .timeout(REFRESH_HTTP_TIMEOUT)
        .build()
        .map_err(|e| format!("http client build failed: {e}"))?;
    let mut form: Vec<(&str, &str)> = vec![
        ("grant_type", "refresh_token"),
        ("refresh_token", params.refresh_token.as_str()),
        ("client_id", params.client_id.as_str()),
    ];
    if let Some(secret) = &params.client_secret {
        form.push(("client_secret", secret.as_str()));
    }
    if let Some(scope) = &params.scope {
        form.push(("scope", scope.as_str()));
    }
    let resp = client
        .post(&params.token_endpoint)
        .form(&form)
        .send()
        .await
        .map_err(|e| format!("token endpoint request failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("token endpoint returned HTTP {status}"));
    }
    let body: Value = resp
        .json()
        .await
        .map_err(|_| "token endpoint response was not valid JSON".to_string())?;
    if body
        .get("access_token")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
    {
        return Err("token endpoint response missing access_token".to_string());
    }
    Ok(body)
}

/// Merge a successful refresh response into the existing bag: new
/// `access_token` (and `token_type` when returned); `expires_at` recomputed
/// from `expires_in` as epoch ms — or removed when the response carries no
/// `expires_in`, since the stale value described the replaced token and
/// keeping it would re-fire a refresh on every header build; `refresh_token`
/// replaced only when the server rotates it. All other bag fields
/// (`token_endpoint`, `client_id`, …) are preserved.
fn merge_refresh_response(bag: &Value, resp: &Value) -> Value {
    let mut merged = bag.as_object().cloned().unwrap_or_default();
    if let Some(t) = resp
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        merged.insert("access_token".to_string(), json!(t));
    }
    if let Some(t) = resp
        .get("token_type")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        merged.insert("token_type".to_string(), json!(t));
    }
    if let Some(r) = resp
        .get("refresh_token")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        merged.insert("refresh_token".to_string(), json!(r));
    }
    match resp.get("expires_in").and_then(Value::as_u64) {
        Some(secs) => {
            merged.insert(
                "expires_at".to_string(),
                json!(now_epoch_ms().saturating_add(secs.saturating_mul(1000))),
            );
        }
        None => {
            merged.remove("expires_at");
        }
    }
    Value::Object(merged)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use intent_store::Store;
    use uuid::Uuid;

    use super::*;

    /// Dummy bag literal used across the tests — asserted to be absent from
    /// every wire response so a real bag would be caught by the same guards.
    const DUMMY_BAG_LITERAL: &str = "dummy-oauth-payload-marker";

    struct TempDb {
        path: PathBuf,
    }
    impl TempDb {
        fn new() -> Self {
            Self {
                path: std::env::temp_dir().join(format!("intentd-oauth-{}.db", Uuid::new_v4())),
            }
        }
    }
    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    async fn open() -> (TempDb, Store) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        (tmp, store)
    }

    fn contains_dummy(v: &Value) -> bool {
        serde_json::to_string(v)
            .unwrap()
            .contains(DUMMY_BAG_LITERAL)
    }

    #[tokio::test]
    async fn empty_list_when_no_tokens_stored() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let out = svc.list().await.unwrap();
        assert_eq!(out["tokens"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn get_missing_server_id_is_null_value() {
        let (_tmp, store) = open().await;
        let out = McpOauthService::new(&store).get("ghost").await.unwrap();
        assert_eq!(out, json!({ "serverId": "ghost", "value": Value::Null }));
    }

    #[tokio::test]
    async fn set_persists_and_redacts_bag_on_the_wire() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let bag = json!({
            "access_token": DUMMY_BAG_LITERAL,
            "refresh_token": DUMMY_BAG_LITERAL,
            "expires_at": 1_700_000_000_u64,
            "token_type": "Bearer",
        });
        let out = svc.set("srv-linear", bag).await.unwrap();
        assert_eq!(out["serverId"], json!("srv-linear"));
        assert_eq!(out["value"], json!(REDACTED_PLACEHOLDER));
        assert!(
            !contains_dummy(&out),
            "set() response leaked dummy bag literal"
        );
        // The raw bag is retrievable through the store for internal use.
        let raw = store
            .get_mcp_oauth_token("srv-linear")
            .await
            .unwrap()
            .expect("bag persisted");
        assert!(raw.contains(DUMMY_BAG_LITERAL));
    }

    #[tokio::test]
    async fn get_after_set_stays_redacted() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        svc.set("srv-x", json!({ "access_token": DUMMY_BAG_LITERAL }))
            .await
            .unwrap();
        let got = svc.get("srv-x").await.unwrap();
        assert_eq!(got["value"], json!(REDACTED_PLACEHOLDER));
        assert!(!contains_dummy(&got), "get() leaked dummy bag literal");
    }

    #[tokio::test]
    async fn list_lists_stored_server_ids_redacted_sorted() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        svc.set("b", json!({ "t": DUMMY_BAG_LITERAL }))
            .await
            .unwrap();
        svc.set("a", json!({ "t": DUMMY_BAG_LITERAL }))
            .await
            .unwrap();
        let out = svc.list().await.unwrap();
        assert!(!contains_dummy(&out), "list() leaked dummy bag literal");
        let arr = out["tokens"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["serverId"], json!("a"));
        assert_eq!(arr[0]["value"], json!(REDACTED_PLACEHOLDER));
        assert_eq!(arr[1]["serverId"], json!("b"));
    }

    #[tokio::test]
    async fn delete_is_idempotent_and_removes_the_bag() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        // Absent: idempotent success.
        let out = svc.delete("nope").await.unwrap();
        assert_eq!(out, json!({ "success": true }));
        // Present: removed.
        svc.set("srv", json!({ "t": DUMMY_BAG_LITERAL }))
            .await
            .unwrap();
        let out = svc.delete("srv").await.unwrap();
        assert_eq!(out, json!({ "success": true }));
        assert!(store.get_mcp_oauth_token("srv").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn empty_server_id_rejected() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        for res in [
            svc.get("").await,
            svc.set("", json!({})).await,
            svc.delete("").await,
        ] {
            let err = res.unwrap_err();
            assert!(matches!(err, Error::InvalidParams(_)));
        }
    }

    #[tokio::test]
    async fn set_replaces_previous_bag() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        svc.set("srv", json!({ "v": 1 })).await.unwrap();
        svc.set("srv", json!({ "v": 2, "marker": DUMMY_BAG_LITERAL }))
            .await
            .unwrap();
        let raw = store
            .get_mcp_oauth_token("srv")
            .await
            .unwrap()
            .expect("bag persisted");
        assert!(raw.contains("\"v\":2"));
        assert!(raw.contains(DUMMY_BAG_LITERAL));
    }

    #[tokio::test]
    async fn authorization_header_builds_from_stored_bag() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        svc.set(
            "srv",
            json!({ "access_token": "tok123", "token_type": "Bearer" }),
        )
        .await
        .unwrap();
        let hdr = svc.authorization_header("srv").await.unwrap();
        assert_eq!(hdr.as_deref(), Some("Bearer tok123"));
    }

    #[tokio::test]
    async fn authorization_header_capitalizes_lowercase_bearer_and_defaults() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        svc.set(
            "lower",
            json!({ "access_token": "t1", "token_type": "bearer" }),
        )
        .await
        .unwrap();
        svc.set("none", json!({ "access_token": "t2" }))
            .await
            .unwrap();
        assert_eq!(
            svc.authorization_header("lower").await.unwrap().as_deref(),
            Some("Bearer t1")
        );
        assert_eq!(
            svc.authorization_header("none").await.unwrap().as_deref(),
            Some("Bearer t2")
        );
    }

    #[tokio::test]
    async fn authorization_header_absent_bag_or_token_is_none() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        assert!(svc.authorization_header("ghost").await.unwrap().is_none());
        svc.set("no-token", json!({ "refresh_token": "r" }))
            .await
            .unwrap();
        assert!(svc
            .authorization_header("no-token")
            .await
            .unwrap()
            .is_none());
    }

    // -- refresh-aware header builds ------------------------------------------
    //
    // Refresh single-flight/cooldown state lives in module statics keyed by
    // server id, so every refresh test uses a unique (uuid) server id to stay
    // independent of other tests in the same process.

    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Local mock token endpoint: answers every request with `response` (raw
    /// HTTP bytes) after `delay_ms`, counting hits and capturing raw request
    /// text so tests can assert the form fields.
    struct TokenEndpoint {
        url: String,
        hits: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<String>>>,
        handle: tokio::task::JoinHandle<()>,
    }

    impl Drop for TokenEndpoint {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    /// One HTTP request is complete once the headers have arrived and the
    /// body length matches `content-length` (reqwest may split them across
    /// writes).
    fn request_complete(buf: &[u8]) -> bool {
        let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            return false;
        };
        let headers = String::from_utf8_lossy(&buf[..pos]).to_ascii_lowercase();
        let content_length = headers
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        buf.len() >= pos + 4 + content_length
    }

    async fn token_endpoint(response: &'static str, delay_ms: u64) -> TokenEndpoint {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let handle = tokio::spawn({
            let hits = hits.clone();
            let requests = requests.clone();
            async move {
                loop {
                    let Ok((mut sock, _)) = listener.accept().await else {
                        return;
                    };
                    let hits = hits.clone();
                    let requests = requests.clone();
                    tokio::spawn(async move {
                        let mut buf = Vec::new();
                        let mut tmp = [0u8; 4096];
                        loop {
                            match sock.read(&mut tmp).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                            }
                            if request_complete(&buf) {
                                break;
                            }
                        }
                        hits.fetch_add(1, Ordering::SeqCst);
                        requests
                            .lock()
                            .unwrap()
                            .push(String::from_utf8_lossy(&buf).into_owned());
                        if delay_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
                        let _ = sock.write_all(response.as_bytes()).await;
                    });
                }
            }
        });
        TokenEndpoint {
            url: format!("http://{addr}"),
            hits,
            requests,
            handle,
        }
    }

    fn ok_token_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    const HTTP_500: &str =
        "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";

    fn now_secs() -> u64 {
        now_epoch_ms() / 1000
    }

    fn unique_id(prefix: &str) -> String {
        format!("{prefix}-{}", Uuid::new_v4())
    }

    /// A bag carrying the full refresh metadata pointing at `endpoint`.
    fn refreshable_bag(endpoint: &str, expires_at: Value) -> Value {
        let mut bag = json!({
            "access_token": "old-token",
            "token_type": "Bearer",
            "refresh_token": "refresh-1",
            "token_endpoint": endpoint,
            "client_id": "cid-1",
        });
        bag["expires_at"] = expires_at;
        bag
    }

    #[tokio::test]
    async fn not_expired_bag_passes_through_without_refresh() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let ep = token_endpoint("", 0).await;
        let id = unique_id("fresh");
        svc.set(&id, refreshable_bag(&ep.url, json!(now_secs() + 3600)))
            .await
            .unwrap();
        let hdr = svc.authorization_header(&id).await.unwrap();
        assert_eq!(hdr.as_deref(), Some("Bearer old-token"));
        assert_eq!(ep.hits.load(Ordering::SeqCst), 0, "no refresh POST fired");
    }

    #[tokio::test]
    async fn expired_bag_refreshes_persists_and_builds_new_header() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let body = r#"{"access_token":"new-token","token_type":"bearer","expires_in":3600,"refresh_token":"refresh-2"}"#;
        let resp: &'static str = Box::leak(ok_token_response(body).into_boxed_str());
        let ep = token_endpoint(resp, 0).await;
        let id = unique_id("expired");
        let mut bag = refreshable_bag(&ep.url, json!(now_secs() - 100));
        bag["client_secret"] = json!("csec-1");
        bag["scope"] = json!("read write");
        svc.set(&id, bag).await.unwrap();

        let before_ms = now_epoch_ms();
        let hdr = svc.authorization_header(&id).await.unwrap();
        assert_eq!(hdr.as_deref(), Some("Bearer new-token"));
        assert_eq!(ep.hits.load(Ordering::SeqCst), 1);

        // The grant is a form-encoded RFC 6749 §6 refresh, with the optional
        // client_secret/scope forwarded.
        let req = ep.requests.lock().unwrap().last().unwrap().clone();
        assert!(req.contains("grant_type=refresh_token"), "req: {req}");
        assert!(req.contains("refresh_token=refresh-1"), "req: {req}");
        assert!(req.contains("client_id=cid-1"), "req: {req}");
        assert!(req.contains("client_secret=csec-1"), "req: {req}");
        assert!(req.contains("scope=read+write"), "req: {req}");

        // The refreshed bag is persisted: rotated refresh_token, new
        // access_token, expires_at recomputed as epoch ms.
        let raw = store.get_mcp_oauth_token(&id).await.unwrap().unwrap();
        let persisted: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(persisted["access_token"], json!("new-token"));
        assert_eq!(persisted["refresh_token"], json!("refresh-2"));
        assert_eq!(persisted["token_endpoint"], json!(ep.url));
        assert_eq!(persisted["client_id"], json!("cid-1"));
        let expires_at = persisted["expires_at"].as_u64().unwrap();
        assert!(
            expires_at >= before_ms + 3_500_000,
            "expires_at should be ~now+3600s in ms, got {expires_at}"
        );
    }

    #[tokio::test]
    async fn refresh_failure_falls_back_to_stored_token() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let ep = token_endpoint(HTTP_500, 0).await;
        let id = unique_id("fail");
        svc.set(&id, refreshable_bag(&ep.url, json!(now_secs() - 100)))
            .await
            .unwrap();
        let hdr = svc.authorization_header(&id).await.unwrap();
        assert_eq!(hdr.as_deref(), Some("Bearer old-token"));
        assert_eq!(ep.hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn expired_without_metadata_falls_back_without_post() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let ep = token_endpoint("", 0).await;
        let id = unique_id("no-meta");
        let mut bag = refreshable_bag(&ep.url, json!(now_secs() - 100));
        bag.as_object_mut().unwrap().remove("refresh_token");
        svc.set(&id, bag).await.unwrap();
        let hdr = svc.authorization_header(&id).await.unwrap();
        assert_eq!(hdr.as_deref(), Some("Bearer old-token"));
        assert_eq!(ep.hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unparseable_expires_at_falls_back_without_post() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let ep = token_endpoint("", 0).await;
        let id = unique_id("bad-exp");
        svc.set(&id, refreshable_bag(&ep.url, json!("not-a-number")))
            .await
            .unwrap();
        let hdr = svc.authorization_header(&id).await.unwrap();
        assert_eq!(hdr.as_deref(), Some("Bearer old-token"));
        assert_eq!(ep.hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn expires_at_recognized_in_seconds_and_milliseconds() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let body = r#"{"access_token":"new-token","expires_in":3600}"#;
        let resp: &'static str = Box::leak(ok_token_response(body).into_boxed_str());
        let ep = token_endpoint(resp, 0).await;

        // Milliseconds in the future → fresh, no refresh.
        let fresh_ms = unique_id("fresh-ms");
        svc.set(
            &fresh_ms,
            refreshable_bag(&ep.url, json!(now_epoch_ms() + 3_600_000)),
        )
        .await
        .unwrap();
        assert_eq!(
            svc.authorization_header(&fresh_ms)
                .await
                .unwrap()
                .as_deref(),
            Some("Bearer old-token")
        );
        assert_eq!(ep.hits.load(Ordering::SeqCst), 0);

        // Milliseconds in the past → expired, refreshes.
        let stale_ms = unique_id("stale-ms");
        svc.set(
            &stale_ms,
            refreshable_bag(&ep.url, json!(now_epoch_ms() - 100_000)),
        )
        .await
        .unwrap();
        assert_eq!(
            svc.authorization_header(&stale_ms)
                .await
                .unwrap()
                .as_deref(),
            Some("Bearer new-token")
        );
        assert_eq!(ep.hits.load(Ordering::SeqCst), 1);

        // Seconds in the past → expired, refreshes.
        let stale_secs = unique_id("stale-secs");
        svc.set(
            &stale_secs,
            refreshable_bag(&ep.url, json!(now_secs() - 100)),
        )
        .await
        .unwrap();
        assert_eq!(
            svc.authorization_header(&stale_secs)
                .await
                .unwrap()
                .as_deref(),
            Some("Bearer new-token")
        );
        assert_eq!(ep.hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn refresh_token_kept_when_response_does_not_rotate_it() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let body = r#"{"access_token":"new-token","expires_in":3600}"#;
        let resp: &'static str = Box::leak(ok_token_response(body).into_boxed_str());
        let ep = token_endpoint(resp, 0).await;
        let id = unique_id("no-rotate");
        svc.set(&id, refreshable_bag(&ep.url, json!(now_secs() - 100)))
            .await
            .unwrap();
        let hdr = svc.authorization_header(&id).await.unwrap();
        assert_eq!(hdr.as_deref(), Some("Bearer new-token"));
        let raw = store.get_mcp_oauth_token(&id).await.unwrap().unwrap();
        let persisted: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(persisted["refresh_token"], json!("refresh-1"));
        // token_type not returned → previous value kept.
        assert_eq!(persisted["token_type"], json!("Bearer"));
    }

    #[tokio::test]
    async fn concurrent_header_builds_refresh_single_flight() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let body = r#"{"access_token":"new-token","expires_in":3600}"#;
        let resp: &'static str = Box::leak(ok_token_response(body).into_boxed_str());
        let ep = token_endpoint(resp, 200).await;
        let id = unique_id("flight");
        svc.set(&id, refreshable_bag(&ep.url, json!(now_secs() - 100)))
            .await
            .unwrap();
        let headers = tokio::join!(
            svc.authorization_header(&id),
            svc.authorization_header(&id),
            svc.authorization_header(&id),
            svc.authorization_header(&id),
            svc.authorization_header(&id),
        );
        for hdr in [headers.0, headers.1, headers.2, headers.3, headers.4] {
            assert_eq!(hdr.unwrap().as_deref(), Some("Bearer new-token"));
        }
        assert_eq!(
            ep.hits.load(Ordering::SeqCst),
            1,
            "concurrent builds must share one refresh POST"
        );
    }

    #[tokio::test]
    async fn failed_refresh_arms_cooldown_no_immediate_retry() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let ep = token_endpoint(HTTP_500, 0).await;
        let id = unique_id("cooldown");
        svc.set(&id, refreshable_bag(&ep.url, json!(now_secs() - 100)))
            .await
            .unwrap();
        for _ in 0..2 {
            let hdr = svc.authorization_header(&id).await.unwrap();
            assert_eq!(hdr.as_deref(), Some("Bearer old-token"));
        }
        assert_eq!(
            ep.hits.load(Ordering::SeqCst),
            1,
            "second build within the cooldown must not retry the refresh"
        );
    }

    /// Waits until the mock endpoint has received the refresh POST, i.e. the
    /// refresh is in flight (the delayed response has not been sent yet).
    async fn wait_for_hit(ep: &TokenEndpoint) {
        while ep.hits.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn set_during_inflight_refresh_is_not_clobbered_by_persist() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let body = r#"{"access_token":"new-token","expires_in":3600}"#;
        let resp: &'static str = Box::leak(ok_token_response(body).into_boxed_str());
        let ep = token_endpoint(resp, 500).await;
        let id = unique_id("race-set");
        svc.set(&id, refreshable_bag(&ep.url, json!(now_secs() - 100)))
            .await
            .unwrap();
        // Replace the bag through mcp.oauth.set while the refresh POST is
        // held open by the mock endpoint's response delay.
        let (hdr, ()) = tokio::join!(svc.authorization_header(&id), async {
            wait_for_hit(&ep).await;
            svc.set(
                &id,
                json!({ "access_token": "replacement-token", "token_type": "Bearer" }),
            )
            .await
            .unwrap();
        });
        // The externally-set bag wins; the refresh result is discarded.
        let raw = store.get_mcp_oauth_token(&id).await.unwrap().unwrap();
        let persisted: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            persisted["access_token"],
            json!("replacement-token"),
            "refresh persist must not clobber a bag replaced mid-refresh"
        );
        // The header is built from the current (replacement) stored bag.
        assert_eq!(hdr.unwrap().as_deref(), Some("Bearer replacement-token"));
    }

    #[tokio::test]
    async fn delete_during_inflight_refresh_is_not_resurrected_by_persist() {
        let (_tmp, store) = open().await;
        let svc = McpOauthService::new(&store);
        let body = r#"{"access_token":"new-token","expires_in":3600}"#;
        let resp: &'static str = Box::leak(ok_token_response(body).into_boxed_str());
        let ep = token_endpoint(resp, 500).await;
        let id = unique_id("race-delete");
        svc.set(&id, refreshable_bag(&ep.url, json!(now_secs() - 100)))
            .await
            .unwrap();
        // Revoke the bag through mcp.oauth.delete while the refresh POST is
        // held open by the mock endpoint's response delay.
        let (hdr, ()) = tokio::join!(svc.authorization_header(&id), async {
            wait_for_hit(&ep).await;
            svc.delete(&id).await.unwrap();
        });
        assert!(
            store.get_mcp_oauth_token(&id).await.unwrap().is_none(),
            "refresh persist must not resurrect a bag deleted mid-refresh"
        );
        // The revocation is honored for this request too: no header from a
        // token minted off revoked credentials.
        assert!(hdr.unwrap().is_none());
    }
}
