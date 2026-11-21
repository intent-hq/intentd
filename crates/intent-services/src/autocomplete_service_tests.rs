//! `input.autocomplete` through the service layer: settings and secrets
//! decide whether and how the upstream is asked, and every failure answers
//! `{ completion: "" }`. The upstream is a local mock; no real network.

use std::sync::Arc;
use std::time::Duration;

use intent_core::{Error, WorkspaceApi};
use intent_store::Store;
use serde_json::{json, Value};

use crate::autocomplete_ops::AutocompleteUpstream;
use crate::test_support::mock_chat_upstream::{MockChatUpstream, MockReply};
use crate::test_support::test_tempdir;
use crate::{Services, SettingsRegistry};

struct Fixture {
    _dir: tempfile::TempDir,
    svc: Services,
    kilo: MockChatUpstream,
    openrouter: MockChatUpstream,
}

async fn fixture(kilo: MockReply, openrouter: MockReply, timeout: Duration) -> Fixture {
    let dir = test_tempdir("intentd-autocomplete-");
    let store = Store::open(&dir.path().join("db.sqlite"))
        .await
        .expect("open store");
    let registry =
        Arc::new(SettingsRegistry::load(dir.path().join("config.toml")).expect("load registry"));
    let kilo = MockChatUpstream::start(kilo).await;
    let openrouter = MockChatUpstream::start(openrouter).await;
    let svc = Services::new_isolated(store)
        .with_settings_registry(registry)
        .with_autocomplete_upstream(AutocompleteUpstream {
            kilo_url: kilo.url(),
            openrouter_url: openrouter.url(),
            timeout,
        });
    Fixture {
        _dir: dir,
        svc,
        kilo,
        openrouter,
    }
}

fn ok_budget() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(5))
}

async fn set(svc: &Services, changes: Value) {
    svc.settings_update(changes).await.expect("settings.update");
}

async fn complete(svc: &Services, text: &str) -> Value {
    svc.input_autocomplete(json!({ "text": text, "context": "assistant: tests pass" }))
        .await
        .expect("input.autocomplete")
}

#[intent_test_macros::daemon_test]
async fn disabled_by_default_returns_empty_without_a_request() {
    let f = fixture(
        MockReply::content("x"),
        MockReply::content("x"),
        ok_budget(),
    )
    .await;
    assert_eq!(
        complete(&f.svc, "Fix the ").await,
        json!({ "completion": "" })
    );
    assert!(f.kilo.requests().is_empty());
    assert!(f.openrouter.requests().is_empty());
}

#[intent_test_macros::daemon_test]
async fn kilo_without_key_uses_the_free_model_and_cleans_the_reply() {
    let f = fixture(
        MockReply::content("Fix the failing test. Then ship."),
        MockReply::content("x"),
        ok_budget(),
    )
    .await;
    set(
        &f.svc,
        json!([{ "path": "autocomplete.enabled", "value": true }]),
    )
    .await;
    assert_eq!(
        complete(&f.svc, "Fix the ").await,
        json!({ "completion": "failing test." })
    );
    let got = f.kilo.requests();
    assert_eq!(got.len(), 1);
    assert_eq!(
        got[0].body["model"],
        json!("nvidia/nemotron-3.5-lightning:free")
    );
    assert_eq!(got[0].header("authorization"), None);
    assert_eq!(
        got[0].body["messages"][1]["content"],
        json!("<conversation>\nassistant: tests pass\n</conversation>\n<typed>Fix the </typed>")
    );
    assert!(f.openrouter.requests().is_empty());
}

#[intent_test_macros::daemon_test]
async fn kilo_with_key_uses_the_paid_model_and_bearer() {
    let f = fixture(
        MockReply::content("failing test"),
        MockReply::content("x"),
        ok_budget(),
    )
    .await;
    set(
        &f.svc,
        json!([
            { "path": "autocomplete.enabled", "value": true },
            { "path": "autocomplete.model", "value": "particle-ai/particle-1" },
            { "path": "autocomplete.kilo.apiKey", "value": "kilo-secret" },
        ]),
    )
    .await;
    assert_eq!(
        complete(&f.svc, "Fix the ").await,
        json!({ "completion": "failing test" })
    );
    let got = f.kilo.requests();
    assert_eq!(got[0].body["model"], json!("particle-ai/particle-1"));
    assert_eq!(got[0].header("authorization"), Some("Bearer kilo-secret"));
}

#[intent_test_macros::daemon_test]
async fn openrouter_without_key_makes_no_request() {
    let f = fixture(
        MockReply::content("x"),
        MockReply::content("x"),
        ok_budget(),
    )
    .await;
    set(
        &f.svc,
        json!([
            { "path": "autocomplete.enabled", "value": true },
            { "path": "autocomplete.service", "value": "openrouter" },
            // A Kilo key must not be used for OpenRouter.
            { "path": "autocomplete.kilo.apiKey", "value": "kilo-secret" },
        ]),
    )
    .await;
    assert_eq!(
        complete(&f.svc, "Fix the ").await,
        json!({ "completion": "" })
    );
    assert!(f.kilo.requests().is_empty());
    assert!(f.openrouter.requests().is_empty());
}

#[intent_test_macros::daemon_test]
async fn openrouter_with_key_uses_the_base_model_and_attribution() {
    let f = fixture(
        MockReply::content("x"),
        MockReply::content(" and rerun CI"),
        ok_budget(),
    )
    .await;
    set(
        &f.svc,
        json!([
            { "path": "autocomplete.enabled", "value": true },
            { "path": "autocomplete.service", "value": "openrouter" },
            { "path": "autocomplete.openrouter.apiKey", "value": "sk-or-secret" },
        ]),
    )
    .await;
    assert_eq!(
        complete(&f.svc, "Fix it").await,
        json!({ "completion": " and rerun CI" })
    );
    let got = f.openrouter.requests();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].body["model"], json!("nvidia/nemotron-3.5-lightning"));
    assert_eq!(got[0].header("authorization"), Some("Bearer sk-or-secret"));
    assert_eq!(got[0].header("http-referer"), Some("https://intentapp.dev"));
    assert_eq!(got[0].header("x-title"), Some("Intent"));
    assert!(f.kilo.requests().is_empty());
}

#[intent_test_macros::daemon_test]
async fn upstream_failures_answer_an_empty_completion() {
    for (name, reply, timeout) in [
        ("404", MockReply::Status(404, "{}".to_string()), ok_budget()),
        ("429", MockReply::Status(429, "{}".to_string()), ok_budget()),
        ("timeout", MockReply::Hang, Duration::from_millis(200)),
        (
            "garbage",
            MockReply::Status(200, "nope".to_string()),
            ok_budget(),
        ),
    ] {
        let f = fixture(reply, MockReply::content("x"), timeout).await;
        set(
            &f.svc,
            json!([{ "path": "autocomplete.enabled", "value": true }]),
        )
        .await;
        assert_eq!(
            complete(&f.svc, "Fix the ").await,
            json!({ "completion": "" }),
            "{name}"
        );
        assert_eq!(f.kilo.requests().len(), 1, "{name}");
    }
}

#[intent_test_macros::daemon_test]
async fn bad_params_reject_even_when_disabled() {
    let f = fixture(
        MockReply::content("x"),
        MockReply::content("x"),
        ok_budget(),
    )
    .await;
    for params in [
        json!({ "text": "", "context": "" }),
        json!({ "text": "hi" }),
        json!({ "text": "hi", "context": 3 }),
    ] {
        let err = f
            .svc
            .input_autocomplete(params.clone())
            .await
            .expect_err(&format!("{params} must be rejected"));
        assert!(matches!(err, Error::InvalidParams(_)), "{params}: {err:?}");
    }
}

#[intent_test_macros::daemon_test]
async fn api_keys_are_redacted_in_settings_reads() {
    let f = fixture(
        MockReply::content("x"),
        MockReply::content("x"),
        ok_budget(),
    )
    .await;
    set(
        &f.svc,
        json!([{ "path": "autocomplete.openrouter.apiKey", "value": "sk-or-secret" }]),
    )
    .await;
    let listed = f.svc.settings_list().await.expect("settings.list");
    let text = listed.to_string();
    assert!(!text.contains("sk-or-secret"), "{text}");
}
