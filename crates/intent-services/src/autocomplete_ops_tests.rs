use std::time::Duration;

use intent_core::settings_file::{AutocompleteModel, AutocompleteService};
use serde_json::json;

use super::*;
use crate::test_support::mock_chat_upstream::{MockChatUpstream, MockReply};

// --- model-id resolution ----------------------------------------------------

#[test]
fn model_id_table_matches_the_contract() {
    use AutocompleteModel::{Nemotron35Lightning as Nemo, Particle1 as Particle};
    use AutocompleteService::{Kilo, Openrouter};
    let cases = [
        (
            Kilo,
            Nemo,
            false,
            Some("nvidia/nemotron-3.5-lightning:free"),
        ),
        (Kilo, Nemo, true, Some("nvidia/nemotron-3.5-lightning")),
        (Kilo, Particle, false, Some("particle-ai/particle-1:free")),
        (Kilo, Particle, true, Some("particle-ai/particle-1")),
        (
            Openrouter,
            Nemo,
            true,
            Some("nvidia/nemotron-3.5-lightning"),
        ),
        (Openrouter, Particle, true, Some("particle-ai/particle-1")),
        (Openrouter, Nemo, false, None),
        (Openrouter, Particle, false, None),
    ];
    for (service, model, has_key, expected) in cases {
        assert_eq!(
            resolve_model_id(service, model, has_key).as_deref(),
            expected,
            "{service:?} {model:?} key={has_key}"
        );
    }
}

#[test]
fn stored_values_parse_with_defaults_for_unknown_values() {
    assert_eq!(
        parse_service(Some("openrouter")),
        AutocompleteService::Openrouter
    );
    assert_eq!(parse_service(Some("kilo")), AutocompleteService::Kilo);
    assert_eq!(parse_service(Some("groq")), AutocompleteService::Kilo);
    assert_eq!(parse_service(None), AutocompleteService::Kilo);
    assert_eq!(
        parse_model(Some("particle-ai/particle-1")),
        AutocompleteModel::Particle1
    );
    assert_eq!(
        parse_model(Some("nvidia/nemotron-3.5-lightning")),
        AutocompleteModel::Nemotron35Lightning
    );
    assert_eq!(
        parse_model(Some("gone/model")),
        AutocompleteModel::Nemotron35Lightning
    );
    assert_eq!(parse_model(None), AutocompleteModel::Nemotron35Lightning);
    assert_eq!(
        key_path(AutocompleteService::Kilo),
        "autocomplete.kilo.apiKey"
    );
    assert_eq!(
        key_path(AutocompleteService::Openrouter),
        "autocomplete.openrouter.apiKey"
    );
}

// --- param validation ---------------------------------------------------------

#[test]
fn params_accept_bounds() {
    let p = parse_params(&json!({ "text": "a", "context": "" })).unwrap();
    assert_eq!(p.text, "a");
    assert_eq!(p.context, "");
    let max = parse_params(&json!({
        "text": "é".repeat(MAX_TEXT_CHARS),
        "context": "界".repeat(MAX_CONTEXT_CHARS),
    }))
    .unwrap();
    assert_eq!(max.text.chars().count(), MAX_TEXT_CHARS);
}

#[test]
fn params_reject_bad_types_and_lengths() {
    let bad = [
        json!({}),
        json!({ "text": "hi" }),
        json!({ "context": "c" }),
        json!({ "text": 5, "context": "" }),
        json!({ "text": null, "context": "" }),
        json!({ "text": "hi", "context": null }),
        json!({ "text": "hi", "context": ["x"] }),
        json!({ "text": "", "context": "" }),
        json!({ "text": "a".repeat(MAX_TEXT_CHARS + 1), "context": "" }),
        json!({ "text": "hi", "context": "a".repeat(MAX_CONTEXT_CHARS + 1) }),
        json!("text"),
    ];
    for params in bad {
        let err = parse_params(&params).expect_err(&format!("{params} must be rejected"));
        assert!(matches!(err, Error::InvalidParams(_)), "{params}: {err:?}");
    }
}

// --- request body ---------------------------------------------------------------

#[test]
fn body_matches_the_contract() {
    let params = Params {
        text: "Fix the".to_string(),
        context: "assistant: done\nSuggested: Run tests".to_string(),
    };
    let body = build_body("particle-ai/particle-1:free", &params);
    assert_eq!(
        body,
        json!({
            "model": "particle-ai/particle-1:free",
            "max_tokens": 40,
            "temperature": 0.2,
            "stop": ["\n"],
            "reasoning": { "enabled": false },
            "messages": [
                { "role": "system", "content": SYSTEM_PROMPT },
                {
                    "role": "user",
                    "content": "<conversation>\nassistant: done\nSuggested: Run tests\n</conversation>\n<typed>Fix the</typed>",
                },
            ],
        })
    );
}

#[test]
fn content_extraction_tolerates_odd_shapes() {
    assert_eq!(
        extract_content(&json!({ "choices": [{ "message": { "content": "x" } }] })),
        Some("x")
    );
    assert_eq!(
        extract_content(&json!({ "choices": [{ "message": { "content": null } }] })),
        None
    );
    assert_eq!(extract_content(&json!({ "choices": [] })), None);
    assert_eq!(extract_content(&json!({ "error": "x" })), None);
}

// --- reply clean-up -------------------------------------------------------------

#[test]
fn clean_reply_cases() {
    let cases: &[(&str, &str, &str, &str)] = &[
        (
            "plain continuation",
            "Fix the ",
            "failing test",
            "failing test",
        ),
        (
            "leading space kept",
            "Fix the",
            " failing test",
            " failing test",
        ),
        (
            "first line only",
            "Run ",
            "the tests\nthen deploy",
            "the tests",
        ),
        (
            "carriage return line end",
            "Run ",
            "the tests\r\nmore",
            "the tests",
        ),
        (
            "echo dropped",
            "Fix the ",
            "Fix the failing test",
            "failing test",
        ),
        (
            "echo dropped case-insensitive",
            "fix THE ",
            "Fix the failing test",
            "failing test",
        ),
        (
            "echo keeps the model's space",
            "Fix the",
            "Fix the failing test",
            " failing test",
        ),
        (
            "echo of trimmed draft",
            "Fix the  ",
            "fix the failing test",
            "failing test",
        ),
        (
            "wrapping double quotes",
            "Fix ",
            "\"the build\"",
            "the build",
        ),
        ("wrapping backticks", "Run ", "`cargo test`", "cargo test"),
        (
            "wrapping curly quotes",
            "Fix ",
            "\u{201c}the build\u{201d}",
            "the build",
        ),
        ("quoted echo", "Fix the ", "\"Fix the build\"", "build"),
        ("unbalanced quote kept", "Say ", "\"hello", "\"hello"),
        (
            "separate backtick spans kept",
            "Run ",
            "`make test` then `make check`",
            "`make test` then `make check`",
        ),
        (
            "separate double-quoted spans kept",
            "Say ",
            "\"foo\" or \"bar\"",
            "\"foo\" or \"bar\"",
        ),
        (
            "sentence cut",
            "Fix ",
            "the build. Then deploy it.",
            "the build.",
        ),
        (
            "question cut",
            "Can you ",
            "check this? Thanks",
            "check this?",
        ),
        ("exclamation cut", "Ship ", "it! Now", "it!"),
        (
            "dotted names do not cut",
            "Open ",
            "file.rs and v1.2 notes",
            "file.rs and v1.2 notes",
        ),
        ("trailing period kept", "Fix ", "the build.", "the build."),
        ("empty reply", "Fix ", "", ""),
        ("whitespace reply", "Fix ", "   ", ""),
        ("echo only", "Fix the", "Fix the", ""),
        ("blank first line", "Fix ", "\nsecond line", ""),
    ];
    for (name, text, reply, expected) in cases {
        assert_eq!(clean_reply(text, "", reply), *expected, "{name}");
    }
}

#[test]
fn clean_reply_caps_length_on_char_boundaries() {
    let reply = "界".repeat(MAX_COMPLETION_CHARS + 50);
    let cleaned = clean_reply("Hi ", "", &reply);
    assert_eq!(cleaned.chars().count(), MAX_COMPLETION_CHARS);
}

#[test]
fn clean_reply_handles_multibyte_case_folding() {
    assert_eq!(clean_reply("ÉTÉ ", "", "été chaud"), "chaud");
    assert_eq!(clean_reply("İ", "", "i̇x"), "i̇x");
}

/// Spacing between the draft and the suggestion: models usually reply
/// without a leading space, which glued "Show me the" + "before/after" into
/// "Show me thebefore/after".
#[test]
fn clean_reply_spacing_rules() {
    let cases: &[(&str, &str, &str, &str)] = &[
        // a. Draft ends with whitespace: drop the reply's leading whitespace.
        (
            "draft space, reply space",
            "Fix the ",
            " failing test",
            "failing test",
        ),
        (
            "draft space, reply spaces",
            "Fix the ",
            "   failing test",
            "failing test",
        ),
        (
            "draft space, reply tab",
            "Fix the\t",
            "\tfailing test",
            "failing test",
        ),
        (
            "draft space, bare reply",
            "Fix the ",
            "failing test",
            "failing test",
        ),
        // b. Reply starts with whitespace: exactly one space.
        (
            "one space kept",
            "Show me the",
            " before/after view",
            " before/after view",
        ),
        (
            "spaces collapsed",
            "Show me the",
            "   before/after view",
            " before/after view",
        ),
        (
            "tab becomes a space",
            "Show me the",
            "\tbefore/after view",
            " before/after view",
        ),
        // c. Otherwise the model continues the current word.
        (
            "mid-word continuation",
            "Show me the scr",
            "eenshots",
            "eenshots",
        ),
        (
            "bare reply kept as is",
            "Show me the",
            "before/after",
            "before/after",
        ),
    ];
    for (name, text, reply, expected) in cases {
        assert_eq!(clean_reply(text, "", reply), *expected, "{name}");
    }
}

/// Junk guard: angle brackets the context never had, or control characters.
#[test]
fn clean_reply_drops_junk() {
    assert_eq!(
        clean_reply("Show me pe", "assistant: done", "re>n>n>e>r>s>o>n>s>hots>"),
        ""
    );
    assert_eq!(clean_reply("Show me ", "", "<b>the diff</b>"), "");
    assert_eq!(clean_reply("Fix ", "", "the\u{7}build"), "");
    assert_eq!(clean_reply("Fix ", "", "the\u{1b}[31mbuild"), "");
    // Angle brackets the conversation already uses are legitimate.
    assert_eq!(
        clean_reply(
            "Return a ",
            "fn parse() -> Result<Value>",
            "Result<Value> instead"
        ),
        "Result<Value> instead"
    );
    // So are ones the draft itself uses.
    assert_eq!(clean_reply("Make it Vec<", "", "String>"), "String>");
}

/// The prompt asks the model for the spacing rules the clean-up enforces.
#[test]
fn system_prompt_asks_for_word_boundaries() {
    assert!(SYSTEM_PROMPT.contains(
        "If the typed text ends in the middle of a word, start with the rest of that word. \
         If it ends with a complete word, start your reply with a single space."
    ));
}

// --- upstream call --------------------------------------------------------------

/// Budget for calls that must succeed (stretched on slow runners).
fn ok_budget() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(5))
}

fn request<'a>(
    url: &'a str,
    service: AutocompleteService,
    api_key: Option<&'a str>,
    timeout: Duration,
) -> UpstreamRequest<'a> {
    UpstreamRequest {
        url,
        service,
        api_key,
        body: json!({ "model": "m" }),
        timeout,
    }
}

#[tokio::test]
async fn kilo_request_without_key_has_no_auth_or_openrouter_headers() {
    let mock = MockChatUpstream::start(MockReply::content("the tests")).await;
    let url = mock.url();
    let reply = request_completion(
        http_client(),
        request(&url, AutocompleteService::Kilo, None, ok_budget()),
    )
    .await;
    assert_eq!(reply.as_deref(), Some("the tests"));
    let got = mock.requests();
    assert_eq!(got.len(), 1);
    assert!(got[0].request_line.starts_with("POST /chat/completions "));
    assert_eq!(got[0].header("authorization"), None);
    assert_eq!(got[0].header("http-referer"), None);
    assert_eq!(got[0].header("x-title"), None);
    assert_eq!(got[0].body, json!({ "model": "m" }));
}

#[tokio::test]
async fn openrouter_request_carries_key_and_attribution_headers() {
    let mock = MockChatUpstream::start(MockReply::content("x")).await;
    let url = mock.url();
    let reply = request_completion(
        http_client(),
        request(
            &url,
            AutocompleteService::Openrouter,
            Some("sk-or-test"),
            ok_budget(),
        ),
    )
    .await;
    assert_eq!(reply.as_deref(), Some("x"));
    let got = mock.requests();
    assert_eq!(got[0].header("authorization"), Some("Bearer sk-or-test"));
    assert_eq!(got[0].header("http-referer"), Some("https://intentapp.dev"));
    assert_eq!(got[0].header("x-title"), Some("Intent"));
}

#[tokio::test]
async fn kilo_request_with_key_sends_bearer() {
    let mock = MockChatUpstream::start(MockReply::content("x")).await;
    let url = mock.url();
    let _ = request_completion(
        http_client(),
        request(
            &url,
            AutocompleteService::Kilo,
            Some("kilo-key"),
            ok_budget(),
        ),
    )
    .await;
    assert_eq!(
        mock.requests()[0].header("authorization"),
        Some("Bearer kilo-key")
    );
}

#[tokio::test]
async fn soft_failures_map_to_none() {
    let failures = [
        (
            "404 model gone",
            MockReply::Status(404, r#"{"error":"model not found"}"#.to_string()),
        ),
        (
            "429 rate limit",
            MockReply::Status(429, r#"{"error":"rate limited"}"#.to_string()),
        ),
        ("500", MockReply::Status(500, "oops".to_string())),
        ("not json", MockReply::Status(200, "<html>".to_string())),
        (
            "null content",
            MockReply::Status(
                200,
                r#"{"choices":[{"message":{"content":null}}]}"#.to_string(),
            ),
        ),
        (
            "oversized",
            MockReply::Status(200, "x".repeat(MAX_RESPONSE_BYTES + 1)),
        ),
    ];
    for (name, reply) in failures {
        let mock = MockChatUpstream::start(reply).await;
        let url = mock.url();
        let got = request_completion(
            http_client(),
            request(&url, AutocompleteService::Kilo, None, ok_budget()),
        )
        .await;
        assert_eq!(got, None, "{name}");
    }
}

#[tokio::test]
async fn timeout_maps_to_none() {
    let mock = MockChatUpstream::start(MockReply::Hang).await;
    let url = mock.url();
    let got = request_completion(
        http_client(),
        request(
            &url,
            AutocompleteService::Kilo,
            None,
            Duration::from_millis(200),
        ),
    )
    .await;
    assert_eq!(got, None);
    assert_eq!(mock.requests().len(), 1, "the request reached the upstream");
}

#[tokio::test]
async fn connection_refused_maps_to_none() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let refused = format!("http://127.0.0.1:{port}/chat/completions");
    let got = request_completion(
        http_client(),
        request(&refused, AutocompleteService::Kilo, None, ok_budget()),
    )
    .await;
    assert_eq!(got, None);
}

#[test]
fn default_upstream_points_at_the_contract_endpoints() {
    let up = AutocompleteUpstream::default();
    assert_eq!(
        up.kilo_url,
        "https://api.kilo.ai/api/gateway/chat/completions"
    );
    assert_eq!(
        up.openrouter_url,
        "https://openrouter.ai/api/v1/chat/completions"
    );
    assert_eq!(up.timeout, Duration::from_secs(4));
}
