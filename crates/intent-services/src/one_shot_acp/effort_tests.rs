use super::{run_one_shot_acp, OneShotCommand, OneShotEffort, OneShotError};
use serde_json::{json, Value};
use std::{path::PathBuf, time::Duration};

async fn run(
    behavior: Value,
    effort: OneShotEffort,
    model: Option<&str>,
) -> (Result<String, OneShotError>, Vec<Value>) {
    let dir = crate::test_support::test_tempdir("one-shot-effort-");
    let log = dir.path().join("requests.jsonl");
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../intentd/tests/fixtures/mock-quick-action-effort.mjs");
    let cmd = OneShotCommand::binary("node".into(), vec![fixture.to_string_lossy().into_owned()])
        .env("MOCK_EFFORT_BEHAVIOR", behavior.to_string())
        .env("MOCK_EFFORT_LOG", log.as_os_str());
    let result = run_one_shot_acp(
        None,
        cmd,
        "hello",
        model,
        None,
        Duration::from_secs(30),
        &effort,
    )
    .await;
    let requests = std::fs::read_to_string(log)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    (result, requests)
}

fn explicit(value: &str) -> OneShotEffort {
    OneShotEffort {
        explicit: Some(value.into()),
        saved: vec![],
    }
}
fn saved(values: &[&str]) -> OneShotEffort {
    OneShotEffort {
        explicit: None,
        saved: values.iter().map(|v| (*v).into()).collect(),
    }
}

#[tokio::test]
async fn effort_uses_final_live_selector_before_prompt() {
    for (behavior, effort, expected) in [
        (json!({}), explicit("HIGH"), "high"),
        (
            json!({"openingValues":["low"],"modelValues":["low","high"]}),
            explicit("high"),
            "high",
        ),
        (
            json!({"openingValues":["low"],"modelValues":["low","high"]}),
            saved(&["high", "low"]),
            "high",
        ),
        (
            json!({"modelValues":["low"]}),
            saved(&["high", "low"]),
            "low",
        ),
        (
            json!({"modelOptions":null}),
            saved(&["high", "low"]),
            "high",
        ),
        (
            json!({"modelOptions":[{"invalid":true}]}),
            explicit("high"),
            "high",
        ),
        (
            json!({"rejectModel":true,"openingValues":["low"]}),
            saved(&["high", "low"]),
            "low",
        ),
    ] {
        let (result, requests) = run(behavior.clone(), effort, Some("chosen")).await;
        let reply: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(reply["effort"], expected, "{behavior}");
        assert_eq!(
            requests
                .iter()
                .map(|r| r["method"].as_str().unwrap())
                .collect::<Vec<_>>(),
            [
                "initialize",
                "session/new",
                "session/set_config_option",
                "session/set_config_option",
                "session/prompt"
            ]
        );
        assert_eq!(requests[2]["params"]["configId"], "model");
        assert_eq!(requests[3]["params"]["configId"], "adapter-thinking");
        assert_eq!(requests[3]["params"]["value"], expected);
    }
}

#[tokio::test]
async fn explicit_effort_failure_never_sends_prompt() {
    for (behavior, invalid) in [
        (json!({"modelValues":["low"]}), true),
        (json!({"modelOptions":[]}), true),
        (json!({"rejectModel":true,"openingValues":["low"]}), true),
        (json!({"rejectEffort":true}), false),
        (json!({"echoEffort":"low"}), false),
    ] {
        let (result, requests) = run(behavior.clone(), explicit("high"), Some("chosen")).await;
        let err = result.unwrap_err();
        assert_eq!(
            matches!(err, OneShotError::InvalidEffort(_)),
            invalid,
            "{behavior}: {err}"
        );
        assert!(
            requests.iter().all(|r| r["method"] != "session/prompt"),
            "{behavior}: {requests:?}"
        );
    }
}

#[tokio::test]
async fn absent_or_unsupported_saved_effort_preserves_provider_default() {
    for (behavior, effort) in [
        (json!({}), OneShotEffort::default()),
        (json!({}), saved(&["stale", "also-stale"])),
        (json!({"modelOptions":[]}), saved(&["high", "low"])),
    ] {
        let (result, requests) = run(behavior, effort, Some("chosen")).await;
        let reply: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(reply["effort"], "medium");
        assert_eq!(requests.len(), 4, "no effort request: {requests:?}");
    }
    let (result, requests) = run(json!({"rejectEffort":true}), saved(&["high", "low"]), None).await;
    assert!(result.is_ok());
    assert_eq!(requests.last().unwrap()["method"], "session/prompt");
}

#[tokio::test]
async fn already_current_effort_needs_no_extra_request() {
    let (result, requests) = run(json!({}), explicit("medium"), None).await;
    assert!(result.is_ok());
    assert_eq!(requests.len(), 3);
}
