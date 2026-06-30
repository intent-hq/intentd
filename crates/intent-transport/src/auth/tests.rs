//! Unit tests for `transport::auth`: token generate/validate (timing-safe,
//! length-checked), `extract_token` (header + `?token=`), and the origin
//! allow-list matrix. The keychain is replaced by an in-memory [`MemoryStore`]
//! so tests never touch the real OS keychain.

use std::sync::Mutex;

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

#[test]
fn generate_token_is_64_lowercase_hex_and_persisted() {
    let store = MemoryStore::default();
    let token = generate_token(&store).unwrap();
    assert_eq!(token.len(), 64, "32 random bytes hex-encode to 64 chars");
    assert!(
        token
            .chars()
            .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
        "token must be lowercase hex",
    );
    assert_eq!(store.load_token().as_deref(), Some(token.as_str()));
}

#[test]
fn generated_tokens_are_unique() {
    let a = generate_token(&MemoryStore::default()).unwrap();
    let b = generate_token(&MemoryStore::default()).unwrap();
    assert_ne!(a, b);
}

#[test]
fn get_or_create_returns_existing_then_stable() {
    let store = MemoryStore::default();
    let first = get_or_create_token(&store).unwrap();
    let second = get_or_create_token(&store).unwrap();
    assert_eq!(first, second, "must not regenerate when one already exists");
}

#[test]
fn validate_token_accepts_the_stored_value() {
    let token = generate_token(&MemoryStore::default()).unwrap();
    let store = MemoryStore::with(&token);
    assert!(validate_token(&store, &token));
}

#[test]
fn validate_token_rejects_wrong_same_length_value() {
    // Same length, differs only in the last char — exercises the constant-time path.
    let stored = "a".repeat(64);
    let candidate = format!("{}b", "a".repeat(63));
    let store = MemoryStore::with(&stored);
    assert_eq!(stored.len(), candidate.len());
    assert!(!validate_token(&store, &candidate));
}

#[test]
fn validate_token_rejects_empty_and_length_mismatch() {
    let store = MemoryStore::with(&"a".repeat(64));
    assert!(!validate_token(&store, ""), "empty candidate rejected");
    assert!(!validate_token(&store, "a"), "length mismatch rejected");
    assert!(!validate_token(&store, &"a".repeat(65)), "longer rejected");
}

#[test]
fn validate_token_rejects_when_nothing_stored() {
    assert!(!validate_token(&MemoryStore::default(), &"a".repeat(64)));
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
fn discovery_disabled_by_default() {
    assert!(!is_discovery_enabled(None));
    assert!(is_discovery_enabled(Some(true)));
    assert!(!is_discovery_enabled(Some(false)));
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
