//! Unit tests for `transport::auth`: token generate/validate (timing-safe,
//! length-checked), `extract_token` (header + `?token=`), and the origin
//! allow-list matrix. The keychain is replaced by an in-memory [`MemoryStore`]
//! so tests never touch the real OS keychain.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use super::*;

/// In-memory [`TokenStore`] for hermetic tests.
#[derive(Default)]
struct MemoryStore {
    token: Mutex<Option<String>>,
}

impl MemoryStore {
    fn with(token: &str) -> Self {
        Self {
            token: Mutex::new(Some(token.to_string())),
        }
    }
}

impl TokenStore for MemoryStore {
    fn load_token(&self) -> Option<String> {
        self.token.lock().unwrap().clone().filter(|t| !t.is_empty())
    }
    fn store_token(&self, token: &str) -> Result<()> {
        *self.token.lock().unwrap() = Some(token.to_string());
        Ok(())
    }
}

/// A `TokenStore` whose `load_token` sleeps well past the compressed test
/// timeout, counting calls so tests can prove concurrent callers are
/// coalesced into a single `spawn_blocking`.
#[derive(Default)]
struct BlockingTokenStore {
    load_calls: AtomicUsize,
}

impl TokenStore for BlockingTokenStore {
    fn load_token(&self) -> Option<String> {
        self.load_calls.fetch_add(1, Ordering::SeqCst);
        // Long enough to outlive the wrapper's compressed test timeout but
        // short enough that the tokio runtime's blocking-pool shutdown at
        // end-of-test doesn't hold up the whole test binary.
        thread::sleep(Duration::from_millis(500));
        None
    }
    fn store_token(&self, _token: &str) -> Result<()> {
        Ok(())
    }
}

/// A `TokenStore` whose `load_token` waits on a barrier so tests can hold
/// ONE call in flight while probing generation-guard semantics.
struct BarrierTokenStore {
    load_calls: AtomicUsize,
    barrier: Arc<Barrier>,
}

impl TokenStore for BarrierTokenStore {
    fn load_token(&self) -> Option<String> {
        self.load_calls.fetch_add(1, Ordering::SeqCst);
        self.barrier.wait();
        Some("old-token".to_string())
    }
    fn store_token(&self, _token: &str) -> Result<()> {
        Ok(())
    }
}

/// Wrap a [`MemoryStore`] as an [`AsyncTokenStore`]. Tests share the same
/// `Arc` so the inner store survives the wrapper for load-after-store checks.
fn async_of(inner: Arc<MemoryStore>) -> AsyncTokenStore {
    AsyncTokenStore::new(inner)
}

#[tokio::test]
async fn generate_token_is_64_lowercase_hex_and_persisted() {
    let store = Arc::new(MemoryStore::default());
    let token = generate_token(&async_of(store.clone())).await.unwrap();
    assert_eq!(token.len(), 64, "32 random bytes hex-encode to 64 chars");
    assert!(
        token
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
        "token must be lowercase hex",
    );
    assert_eq!(store.load_token().as_deref(), Some(token.as_str()));
}

#[tokio::test]
async fn generated_tokens_are_unique() {
    let a = generate_token(&async_of(Arc::new(MemoryStore::default())))
        .await
        .unwrap();
    let b = generate_token(&async_of(Arc::new(MemoryStore::default())))
        .await
        .unwrap();
    assert_ne!(a, b);
}

#[tokio::test]
async fn get_or_create_returns_existing_then_stable() {
    let store = async_of(Arc::new(MemoryStore::default()));
    let first = get_or_create_token(&store).await.unwrap();
    let second = get_or_create_token(&store).await.unwrap();
    assert_eq!(first, second, "must not regenerate when one already exists");
}

#[tokio::test]
async fn validate_token_accepts_the_stored_value() {
    let token = generate_token(&async_of(Arc::new(MemoryStore::default())))
        .await
        .unwrap();
    let store = async_of(Arc::new(MemoryStore::with(&token)));
    assert!(validate_token(&store, &token).await);
}

#[tokio::test]
async fn validate_token_rejects_wrong_same_length_value() {
    // Same length, differs only in the last char — exercises the constant-time path.
    let stored = "a".repeat(64);
    let candidate = format!("{}b", "a".repeat(63));
    let store = async_of(Arc::new(MemoryStore::with(&stored)));
    assert_eq!(stored.len(), candidate.len());
    assert!(!validate_token(&store, &candidate).await);
}

#[tokio::test]
async fn validate_token_rejects_empty_and_length_mismatch() {
    let store = async_of(Arc::new(MemoryStore::with(&"a".repeat(64))));
    assert!(
        !validate_token(&store, "").await,
        "empty candidate rejected"
    );
    assert!(
        !validate_token(&store, "a").await,
        "length mismatch rejected"
    );
    assert!(
        !validate_token(&store, &"a".repeat(65)).await,
        "longer rejected"
    );
}

#[tokio::test]
async fn validate_token_rejects_when_nothing_stored() {
    let store = async_of(Arc::new(MemoryStore::default()));
    assert!(!validate_token(&store, &"a".repeat(64)).await);
}

#[test]
fn token_matches_is_length_checked_then_equal() {
    assert!(token_matches("abc123", "abc123"));
    assert!(!token_matches("abc123", "abc124"));
    assert!(!token_matches("abc123", "abc12"));
    assert!(!token_matches("", "abc"));
    assert!(!token_matches("abc", ""));
}

#[test]
fn extract_bearer_token_parses_header_case_insensitively() {
    assert_eq!(
        extract_bearer_token(Some("Bearer abc123")).as_deref(),
        Some("abc123")
    );
    assert_eq!(
        extract_bearer_token(Some("bearer abc123")).as_deref(),
        Some("abc123")
    );
    assert_eq!(
        extract_bearer_token(Some("BEARER\tabc123")).as_deref(),
        Some("abc123")
    );
    assert_eq!(
        extract_bearer_token(Some("Bearer   abc123")).as_deref(),
        Some("abc123")
    );
}

#[test]
fn extract_bearer_token_rejects_malformed_headers() {
    assert_eq!(extract_bearer_token(None), None);
    assert_eq!(extract_bearer_token(Some("")), None);
    assert_eq!(extract_bearer_token(Some("Bearer")), None);
    assert_eq!(extract_bearer_token(Some("Bearer ")), None);
    assert_eq!(extract_bearer_token(Some("Bearerabc")), None);
    assert_eq!(extract_bearer_token(Some("Bearer a b")), None);
    assert_eq!(extract_bearer_token(Some("Token abc")), None);
    assert_eq!(extract_bearer_token(Some(" Bearer abc")), None);
}

#[test]
fn extract_token_prefers_header_then_query() {
    assert_eq!(
        extract_token(Some("Bearer headertok"), "/ws?token=querytok").as_deref(),
        Some("headertok"),
    );
    assert_eq!(
        extract_token(None, "/ws?token=querytok").as_deref(),
        Some("querytok"),
    );
    assert_eq!(
        extract_token(None, "/ws?foo=1&token=qt&bar=2").as_deref(),
        Some("qt"),
    );
    assert_eq!(extract_token(None, "/ws"), None);
    assert_eq!(extract_token(None, "/ws?token="), None);
    assert_eq!(
        extract_token(None, "/ws?token=a%2Bb").as_deref(),
        Some("a+b"),
        "percent-encoded query value is decoded",
    );
}

const LOCAL: &str = "clement";

#[test]
fn origin_allows_native_and_local_contexts() {
    assert!(
        is_allowed_origin_with_host(None, LOCAL),
        "no Origin (native client)"
    );
    assert!(is_allowed_origin_with_host(Some(""), LOCAL), "empty Origin");
    assert!(is_allowed_origin_with_host(Some("file://"), LOCAL));
    assert!(is_allowed_origin_with_host(
        Some("file:///Users/x/index.html"),
        LOCAL
    ));
}

#[test]
fn origin_allows_loopback_hosts() {
    for o in [
        "http://localhost",
        "http://localhost:5180",
        "https://127.0.0.1:5180",
        "http://[::1]:5180",
    ] {
        assert!(
            is_allowed_origin_with_host(Some(o), LOCAL),
            "{o} must be allowed"
        );
    }
}

#[test]
fn origin_allows_own_hostname_and_local_form() {
    assert!(is_allowed_origin_with_host(
        Some("https://clement:5180"),
        LOCAL
    ));
    assert!(is_allowed_origin_with_host(
        Some("https://clement.local:5180"),
        LOCAL
    ));
    // local_host already carries the `.local` suffix.
    assert!(is_allowed_origin_with_host(
        Some("https://clement"),
        "clement.local"
    ));
    // case-insensitive host comparison.
    assert!(is_allowed_origin_with_host(
        Some("https://CLEMENT:5180"),
        LOCAL
    ));
}

#[test]
fn origin_rejects_null_and_cross_origin() {
    assert!(
        !is_allowed_origin_with_host(Some("null"), LOCAL),
        "sandboxed null rejected"
    );
    assert!(!is_allowed_origin_with_host(
        Some("https://evil.example.com"),
        LOCAL
    ));
    assert!(!is_allowed_origin_with_host(
        Some("https://attacker.local"),
        LOCAL
    ));
    assert!(
        !is_allowed_origin_with_host(Some("not a url"), LOCAL),
        "unparseable rejected"
    );
}

#[test]
fn origin_rejects_cross_origin_when_local_host_unknown() {
    assert!(!is_allowed_origin_with_host(Some("https://clement"), ""));
    // loopback + file:// + native still pass with no known local host.
    assert!(is_allowed_origin_with_host(Some("http://localhost"), ""));
    assert!(is_allowed_origin_with_host(None, ""));
}

#[test]
fn real_hostname_wrapper_allows_loopback() {
    assert!(is_allowed_origin(Some("http://localhost:5180")));
    assert!(is_allowed_origin(None));
    assert!(!is_allowed_origin(Some("null")));
}

#[test]
fn auth_enabled_defaults_true_on_tcp_only() {
    assert!(is_auth_enabled(None, true), "TCP default is enabled");
    assert!(!is_auth_enabled(None, false), "UDS default is disabled");
    assert!(
        !is_auth_enabled(Some(false), true),
        "explicit override wins"
    );
    assert!(is_auth_enabled(Some(true), false), "explicit override wins");
}

#[test]
fn extract_bearer_token_rejects_short_and_non_whitespace_separator() {
    // header.get(..6) returns None for anything shorter than 6 bytes.
    assert_eq!(extract_bearer_token(Some("abc")), None);
    assert_eq!(extract_bearer_token(Some("Beare")), None);
    // 6 chars but no whitespace after.
    assert_eq!(extract_bearer_token(Some("Bearer")), None);
    // 6 chars then non-whitespace.
    assert_eq!(extract_bearer_token(Some("Bearer-x")), None);
    // Multi-byte char at byte 6 boundary: `header.get(..6)` succeeds on ASCII
    // prefix, but the next byte is a `-`, not whitespace.
    assert_eq!(extract_bearer_token(Some("BEARER-abc")), None);
}

#[test]
fn extract_token_decodes_plus_to_space_and_handles_no_value() {
    // `+` → space in the query value.
    assert_eq!(extract_token(None, "/ws?token=a+b").as_deref(), Some("a b"),);
    // First `token` wins (matches URLSearchParams.get semantics in TS).
    assert_eq!(
        extract_token(None, "/ws?token=first&token=second").as_deref(),
        Some("first"),
    );
    // Bare `?token` (no `=`) splits as ("token", "") and yields None for empty value.
    assert_eq!(extract_token(None, "/ws?token"), None);
    // Header neither present nor query.
    assert_eq!(extract_token(None, ""), None);
}

#[test]
fn extract_query_token_strips_fragment() {
    // Fragment after the query is stripped before scanning.
    assert_eq!(
        extract_token(None, "/ws?token=abc#frag").as_deref(),
        Some("abc"),
    );
    // Fragment-only target: split_once('?') is None → no token.
    assert_eq!(extract_token(None, "/ws#frag"), None);
}

#[test]
fn extract_query_token_handles_percent_escape_edges() {
    // Invalid hex escape — left verbatim (the `%` is preserved, scan continues).
    assert_eq!(
        extract_token(None, "/ws?token=%ZZ%21").as_deref(),
        Some("%ZZ!"),
        "invalid escape stays verbatim; valid one (%21) decodes",
    );
    // Trailing `%` with no two following bytes — fails the i+2 check, kept verbatim.
    assert_eq!(
        extract_token(None, "/ws?token=abc%").as_deref(),
        Some("abc%"),
    );
    // Percent-encoded key still matches `token` after decode.
    assert_eq!(extract_token(None, "/ws?%74oken=x").as_deref(), Some("x"),);
}

#[test]
fn origin_hostname_handles_userinfo_and_ipv6_and_paths() {
    // Userinfo is stripped (rsplit_once('@') keeps the host:port).
    assert!(is_allowed_origin_with_host(
        Some("https://user:pass@localhost:5180"),
        LOCAL
    ));
    // IPv6 literal in brackets is preserved as `[::1]` and matches the loopback set.
    assert!(is_allowed_origin_with_host(
        Some("https://[::1]:5180/path?q=1#frag"),
        LOCAL
    ));
    // Path/query/hash after the authority are correctly trimmed.
    assert!(is_allowed_origin_with_host(
        Some("https://localhost:5180/some/path?x=1#h"),
        LOCAL
    ));
}

#[test]
fn origin_rejects_empty_authority_and_missing_scheme() {
    // Missing scheme entirely — split_once("://") returns None → reject.
    assert!(!is_allowed_origin_with_host(Some("localhost:5180"), LOCAL));
    // Scheme present but authority empty (`https:///path`) → None hostname → reject.
    assert!(!is_allowed_origin_with_host(Some("https:///path"), LOCAL));
    // Scheme with only `:` after authority (empty host before port).
    assert!(!is_allowed_origin_with_host(Some("https://:5180"), LOCAL));
}

/// A wedged keychain: `load_token` returns `None` within the timeout and the
/// caller sees an unset token instead of hanging forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_token_returns_none_on_timeout() {
    let inner: Arc<dyn TokenStore> = Arc::new(BlockingTokenStore::default());
    let store = AsyncTokenStore::with_timings(
        inner,
        Duration::from_millis(50),
        Duration::from_secs(1),
        Duration::from_secs(60),
        Duration::from_secs(60),
    );
    let start = Instant::now();
    let v = store.load_token().await;
    let elapsed = start.elapsed();
    assert!(v.is_none(), "wedged keychain must resolve to None");
    assert!(
        elapsed < Duration::from_millis(500),
        "load must return within its deadline, took {elapsed:?}"
    );
}

/// Concurrent callers share the single in-flight keychain call — a wedged
/// keychain occupies one blocking-pool thread total, not one per caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_load_tokens_are_single_flight() {
    let sync = Arc::new(BlockingTokenStore::default());
    let inner: Arc<dyn TokenStore> = sync.clone();
    let store = Arc::new(AsyncTokenStore::with_timings(
        inner,
        Duration::from_millis(50),
        Duration::from_secs(1),
        Duration::from_secs(60),
        Duration::from_secs(60),
    ));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let s = store.clone();
        handles.push(tokio::spawn(async move { s.load_token().await }));
    }
    for h in handles {
        assert!(h.await.unwrap().is_none());
    }
    assert_eq!(
        sync.load_calls.load(Ordering::SeqCst),
        1,
        "single-flight must coalesce concurrent callers into ONE keychain load"
    );
}

/// A `store_token` that lands while a slow load is still parked in the
/// blocking pool must win: the delayed load result must not clobber the
/// fresher cache entry. Guards against the pre-generation-counter race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn intervening_store_token_wins_over_slow_load() {
    let barrier = Arc::new(Barrier::new(2));
    let sync = Arc::new(BarrierTokenStore {
        load_calls: AtomicUsize::new(0),
        barrier: barrier.clone(),
    });
    let inner: Arc<dyn TokenStore> = sync.clone();
    let store = Arc::new(AsyncTokenStore::with_timings(
        inner,
        Duration::from_millis(50),
        Duration::from_secs(1),
        Duration::from_secs(60),
        Duration::from_secs(60),
    ));
    // Start a load; the caller times out but the sync `load_token` is still
    // parked on the barrier inside the blocking pool.
    assert!(store.load_token().await.is_none());
    // Fresh write lands while the slow load is still pending.
    store.store_token("new-token").await.unwrap();
    // Release the sync `load_token`; its completion task must refuse to
    // clobber the fresher Cached slot (the load_id no longer matches).
    barrier.wait();
    // Give the completion task time to run and observe the mismatch.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        store.load_token().await,
        Some("new-token".to_string()),
        "intervening store_token must win against a delayed load result"
    );
}
