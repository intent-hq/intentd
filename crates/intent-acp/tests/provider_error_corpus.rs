//! Quota classifier against the errors the pinned ACP adapters really send
//! (intent-hq/intent#7615).
//!
//! Backup provider failover and the quota banner both key off
//! [`intent_acp::is_quota_exceeded`] / [`intent_acp::message_is_quota_exceeded`].
//! Their first tests used hand-written strings, so the real Claude Code
//! weekly-limit error matched nothing and failover never fired. This test
//! reads `fixtures/provider-error-corpus.json` instead: every structured error
//! kind the pinned adapters can attach, plus their real limit and non-limit
//! wordings. `scripts/check-provider-error-corpus.py` keeps the corpus in step
//! with the pinned adapter packages.
//!
//! Two limits on what this proves:
//! - claude-agent-acp files a result as a usage limit from its TEXT (the SDK's
//!   `USAGE_LIMIT_ERROR_PREFIXES`) whatever its errorKind, and attaches no
//!   `data` when the message carries no kind. So every Claude rendering is also
//!   checked text-only; wordings the classifier only catches through the
//!   errorKind marker are pinned as `textOnlyKnownGap`.
//! - codex-acp raises a JSON-RPC prompt error only for the kinds the corpus
//!   lists in `raisedAsRpcError` (usageLimitExceeded). The other codex kind
//!   cases, including rateLimitExceeded and HTTP 429, are the typed-failure
//!   shape intentd does not receive today; they stream as agent text, which
//!   no classifier sees.

use intent_acp::{
    is_quota_exceeded, is_transient_provider_fetch_failure, is_transient_upstream_disconnect,
    message_is_quota_exceeded, AcpError, JsonRpcError,
};
use serde_json::{json, Map, Value};

const CORPUS: &str = include_str!("fixtures/provider-error-corpus.json");

/// One adapter error as intentd receives it, with what the classifier should say.
struct Case {
    label: String,
    error: JsonRpcError,
    quota: bool,
    known_gap: bool,
}

fn corpus() -> Value {
    serde_json::from_str(CORPUS).expect("provider-error-corpus.json is valid JSON")
}

fn expect_quota(entry: &Value, what: &str) -> bool {
    match entry["expect"].as_str() {
        Some("quota") => true,
        Some("not-quota") => false,
        other => panic!("{what}: `expect` must be \"quota\" or \"not-quota\", got {other:?}"),
    }
}

fn object<'a>(value: &'a Value, what: &str) -> &'a Map<String, Value> {
    value
        .as_object()
        .unwrap_or_else(|| panic!("{what} must be an object"))
}

fn internal_error(data: Value) -> JsonRpcError {
    JsonRpcError {
        code: -32603,
        message: "Internal error".to_string(),
        data: Some(data),
    }
}

/// One rendering per structured kind: the kind alone, with no wording, so the
/// structured signal is tested on its own.
fn kind_cases(corpus: &Value) -> Vec<Case> {
    let mut cases = Vec::new();
    for (adapter, table) in object(&corpus["kinds"], "kinds") {
        let field = table["field"]
            .as_str()
            .unwrap_or_else(|| panic!("kinds.{adapter}.field must be a string"));
        for (kind, entry) in object(&table["values"], &format!("kinds.{adapter}.values")) {
            let what = format!("{adapter} {field}={kind}");
            let data = match adapter.as_str() {
                "codex" => json!({ "message": "Turn failed", field: kind }),
                _ => json!({ field: kind }),
            };
            cases.push(Case {
                quota: expect_quota(entry, &what),
                known_gap: entry.get("knownGap").is_some(),
                label: what,
                error: internal_error(data),
            });
        }
        let Some(structured) = table.get("structuredValues") else {
            continue;
        };
        for (kind, entry) in object(structured, &format!("kinds.{adapter}.structuredValues")) {
            let what = format!("{adapter} {field}={{{kind}}}");
            cases.push(Case {
                quota: expect_quota(entry, &what),
                known_gap: entry.get("knownGap").is_some(),
                label: what.clone(),
                error: internal_error(json!({
                    "message": "Turn failed",
                    field: { kind: { "httpStatusCode": null } },
                })),
            });
            // codex-acp reports any structured kind carrying HTTP 429 as rate_limited.
            cases.push(Case {
                quota: true,
                known_gap: false,
                label: format!("{what} with httpStatusCode 429"),
                error: internal_error(json!({
                    "message": "Too Many Requests",
                    field: { kind: { "httpStatusCode": 429 } },
                })),
            });
        }
    }
    cases
}

fn rendering_cases(corpus: &Value) -> Vec<Case> {
    let renderings = corpus["renderings"]
        .as_array()
        .expect("renderings must be an array");
    let mut cases = Vec::new();
    for (index, entry) in renderings.iter().enumerate() {
        let adapter = entry["adapter"]
            .as_str()
            .unwrap_or_else(|| panic!("renderings[{index}].adapter must be a string"));
        let source = entry["source"]
            .as_str()
            .unwrap_or_else(|| panic!("renderings[{index}].source must be a string"));
        let what = format!("{adapter} renderings[{index}] ({source})");
        let message = entry["message"]
            .as_str()
            .unwrap_or_else(|| panic!("{what}: message must be a string"));
        let quota = expect_quota(entry, &what);
        let error = JsonRpcError {
            code: entry.get("code").and_then(Value::as_i64).unwrap_or(-32603),
            message: message.to_string(),
            data: entry.get("data").filter(|d| !d.is_null()).cloned(),
        };
        let text_only_gap = entry.get("textOnlyKnownGap").is_some();
        if adapter == "claude-code" && error.data.is_some() {
            // The same result without its errorKind, as claude-agent-acp sends
            // a usage-limit message whose assistant error carries no kind.
            cases.push(Case {
                label: format!("{what} text only"),
                error: JsonRpcError {
                    data: None,
                    ..error.clone()
                },
                quota,
                known_gap: text_only_gap,
            });
        } else {
            assert!(
                !text_only_gap,
                "{what}: textOnlyKnownGap only applies to claude-code renderings with data"
            );
        }
        cases.push(Case {
            quota,
            known_gap: entry.get("knownGap").is_some(),
            error,
            label: what,
        });
    }
    cases
}

/// Every path a provider error reaches the classifier by: the typed error, its
/// rendered text, and the text after the service layer wraps it.
fn classifications(error: &JsonRpcError) -> [(&'static str, bool); 3] {
    let err = AcpError::Rpc(error.clone());
    let rendered = err.to_string();
    [
        ("is_quota_exceeded", is_quota_exceeded(&err)),
        (
            "message_is_quota_exceeded",
            message_is_quota_exceeded(&rendered),
        ),
        (
            "message_is_quota_exceeded (session/prompt failed wrap)",
            message_is_quota_exceeded(&format!("session/prompt failed: {rendered}")),
        ),
    ]
}

/// Checks every case and returns one line per problem, so a single red run
/// names every entry the classifier gets wrong.
fn problems(cases: &[Case]) -> Vec<String> {
    let mut problems = Vec::new();
    for case in cases {
        let rendered = AcpError::Rpc(case.error.clone()).to_string();
        for (path, got) in classifications(&case.error) {
            let correct = got == case.quota;
            if case.known_gap && correct {
                problems.push(format!(
                    "{}: listed as a knownGap but {path} now classifies it correctly; \
                     remove `knownGap` from the corpus entry. Rendered: {rendered}",
                    case.label
                ));
            } else if !case.known_gap && !correct {
                let expected = if case.quota { "quota" } else { "not quota" };
                problems.push(format!(
                    "{}: expected {expected}, {path} returned {got}. Rendered: {rendered}",
                    case.label
                ));
            }
        }
        if case.quota && !case.known_gap {
            let err = AcpError::Rpc(case.error.clone());
            if is_transient_upstream_disconnect(&err) || is_transient_provider_fetch_failure(&err) {
                problems.push(format!(
                    "{}: a quota rejection must stay terminal, but a transient classifier \
                     would retry it. Rendered: {rendered}",
                    case.label
                ));
            }
        }
    }
    problems
}

#[test]
fn every_pinned_adapter_error_kind_is_classified() {
    let cases = kind_cases(&corpus());
    assert!(!cases.is_empty(), "the corpus lists no error kinds");
    let problems = problems(&cases);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn every_real_adapter_rendering_is_classified() {
    let cases = rendering_cases(&corpus());
    assert!(!cases.is_empty(), "the corpus lists no renderings");
    let problems = problems(&cases);
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

/// The corpus must keep both halves: limit errors that have to fail over, and
/// non-limit errors that must not. Losing either half would let the classifier
/// drift toward matching everything or nothing.
#[test]
fn corpus_covers_both_adapters_and_both_outcomes() {
    let corpus = corpus();
    let cases: Vec<Case> = kind_cases(&corpus)
        .into_iter()
        .chain(rendering_cases(&corpus))
        .collect();
    for adapter in ["claude-code", "codex"] {
        for quota in [true, false] {
            assert!(
                cases
                    .iter()
                    .any(|c| c.label.starts_with(adapter) && c.quota == quota && !c.known_gap),
                "the corpus has no {adapter} kind with expect={}",
                if quota { "quota" } else { "not-quota" }
            );
        }
    }
    let renderings = corpus["renderings"].as_array().expect("renderings array");
    for adapter in ["claude-code", "codex"] {
        assert!(
            renderings
                .iter()
                .any(|r| r["adapter"] == adapter && r["expect"] == "quota"),
            "the corpus has no real {adapter} limit wording"
        );
    }
}
