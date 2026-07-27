//! Unit tests for the git-credential protocol parse + answer gate
//! (monorepo#884 Phase 2).

use serde_json::json;

use super::*;

fn attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn parse_attributes_reads_key_value_lines_until_blank() {
    let input = "protocol=https\nhost=github.com\npath=owner/repo.git\n\nignored=after-blank\n";
    let parsed = parse_attributes(input.as_bytes());
    assert_eq!(parsed.get("protocol").unwrap(), "https");
    assert_eq!(parsed.get("host").unwrap(), "github.com");
    assert_eq!(parsed.get("path").unwrap(), "owner/repo.git");
    assert!(!parsed.contains_key("ignored"));
}

#[test]
fn parse_attributes_last_value_wins_and_tolerates_junk() {
    let input = "host=first\nnot-an-attribute\nhost=second\nempty=\n";
    let parsed = parse_attributes(input.as_bytes());
    assert_eq!(parsed.get("host").unwrap(), "second");
    assert_eq!(parsed.get("empty").unwrap(), "");
    // Values may themselves contain `=` (split at the first one only).
    let parsed = parse_attributes("url=https://github.com/a?b=c\n".as_bytes());
    assert_eq!(parsed.get("url").unwrap(), "https://github.com/a?b=c");
}

#[test]
fn should_answer_requires_get_https_github() {
    let github = attrs(&[("protocol", "https"), ("host", "github.com")]);
    assert!(should_answer("get", &github));
    // Case-insensitive matching for both fields.
    let upper = attrs(&[("protocol", "HTTPS"), ("host", "GitHub.COM")]);
    assert!(should_answer("get", &upper));
    // store/erase are silent no-ops even for github.com.
    assert!(!should_answer("store", &github));
    assert!(!should_answer("erase", &github));
    assert!(!should_answer("", &github));
}

#[test]
fn should_answer_rejects_other_hosts_and_protocols() {
    for (protocol, host) in [
        ("https", "gitlab.com"),
        ("https", "api.github.com"),
        ("https", "github.com.evil.com"),
        ("https", "github.com:8443"),
        ("http", "github.com"),
        ("ssh", "github.com"),
    ] {
        let a = attrs(&[("protocol", protocol), ("host", host)]);
        assert!(
            !should_answer("get", &a),
            "{protocol}://{host} must not match"
        );
    }
    // Missing protocol or host never matches.
    assert!(!should_answer("get", &attrs(&[("host", "github.com")])));
    assert!(!should_answer("get", &attrs(&[("protocol", "https")])));
    assert!(!should_answer("get", &BTreeMap::new()));
}

#[test]
fn should_answer_defers_to_explicit_url_identity() {
    // A remote URL that pins a username (https://alice@github.com/...) makes
    // git send `username=alice`; the helper must stay silent so the user's
    // own credentials/prompt win.
    let pinned = attrs(&[
        ("protocol", "https"),
        ("host", "github.com"),
        ("username", "alice"),
    ]);
    assert!(!should_answer("get", &pinned));
    // The daemon's own fixed identity is fine.
    let matching = attrs(&[
        ("protocol", "https"),
        ("host", "github.com"),
        ("username", intent_git::auth::TOKEN_USERNAME),
    ]);
    assert!(should_answer("get", &matching));
}

#[cfg(unix)]
#[tokio::test]
async fn hung_daemon_is_a_silent_miss_after_timeout() {
    // A daemon that accepts the connection but never answers must not block
    // git forever: the RPC times out and the helper treats it as a miss.
    let sock = std::env::temp_dir().join(format!("itd-gitcred-hung-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);
    let listener = tokio::net::UnixListener::bind(&sock).expect("bind hung-daemon socket");
    let hold = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });

    let github = attrs(&[("protocol", "https"), ("host", "github.com")]);
    let result = credential_for("get", &github, &sock, Duration::from_millis(200)).await;
    assert_eq!(result, None);

    hold.abort();
    let _ = std::fs::remove_file(&sock);
}

#[test]
fn extract_credential_reads_result_and_rejects_bad_shapes() {
    let ok = json!({ "jsonrpc": "2.0", "id": 1, "result": {
        "credential": { "username": "x-access-token", "password": "gho_abc" }
    }});
    assert_eq!(
        extract_credential(&ok),
        Some(("x-access-token".to_string(), "gho_abc".to_string()))
    );

    // credential: null → no output.
    let null = json!({ "jsonrpc": "2.0", "id": 1, "result": { "credential": null } });
    assert_eq!(extract_credential(&null), None);
    // Error envelope → no output.
    let err = json!({ "jsonrpc": "2.0", "id": 1, "error": { "code": -32001, "message": "x" } });
    assert_eq!(extract_credential(&err), None);
    // Missing/empty/non-string fields → no output.
    let empty = json!({ "result": { "credential": { "username": "", "password": "p" } } });
    assert_eq!(extract_credential(&empty), None);
    let non_string = json!({ "result": { "credential": { "username": 1, "password": "p" } } });
    assert_eq!(extract_credential(&non_string), None);
}

#[test]
fn extract_credential_rejects_control_characters() {
    // A newline in the password would corrupt the line protocol (and could
    // smuggle extra attribute lines); reject rather than emit.
    let sneaky = json!({ "result": { "credential": {
        "username": "x-access-token", "password": "gho_a\nusername=evil"
    }}});
    assert_eq!(extract_credential(&sneaky), None);
}
