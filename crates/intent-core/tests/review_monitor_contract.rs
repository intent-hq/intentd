//! Planned contract tests: these declarations do not enable a provider or register a monitor.
use intent_core::review_monitor::*;
use intent_core::{AgentId, Error, WorkspaceApi, WorkspaceId};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OldWake {
    r#type: String,
    monitor_id: String,
    repo: String,
    pr_number: u64,
    reason: String,
}

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/review_monitor_v1.json")).unwrap()
}

#[test]
fn legacy_and_addressed_requests_round_trip_without_changing_identity() {
    for value in fixture()["requests"].as_array().unwrap() {
        let request: ReviewMonitorRequest = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(&request).unwrap(), *value);
        request.address().unwrap();
    }
    let legacy: ReviewMonitorRequest =
        serde_json::from_value(json!({"prNumber":17,"repo":"Owner/Repo"})).unwrap();
    assert_eq!(
        legacy.address().unwrap(),
        ReviewMonitorAddress::LegacyGithub {
            repo: "Owner/Repo".into()
        }
    );
    let primary: ReviewMonitorRequest =
        serde_json::from_value(json!({"prNumber":17,"repo":null})).unwrap();
    assert_eq!(
        primary.address().unwrap(),
        ReviewMonitorAddress::SelectedRoot { git_root_id: None }
    );
    let registered: ReviewMonitorRequest =
        serde_json::from_value(fixture()["requests"][2].clone()).unwrap();
    assert_eq!(
        registered.address().unwrap(),
        ReviewMonitorAddress::SelectedRoot {
            git_root_id: Some(intent_core::WorkspaceGitRootId::from_string(
                "registered-root"
            )),
        }
    );
    for value in &fixture()["requests"].as_array().unwrap()[3..] {
        let explicit: ReviewMonitorRequest = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(
            explicit.address().unwrap(),
            ReviewMonitorAddress::Explicit {
                target: serde_json::from_value(value["target"].clone()).unwrap(),
            }
        );
    }
    // Preserve the old i64 domain for legacy callers; new explicit addresses use JS-safe numbers.
    serde_json::from_value::<ReviewMonitorRequest>(
        json!({"prNumber":i64::MAX,"repo":"Owner/Repo"}),
    )
    .unwrap();
}

#[test]
fn invalid_or_competing_addresses_never_silently_fall_back() {
    let target = fixture()["requests"][3]["target"].clone();
    for value in [
        json!({"prNumber":0}),
        json!({"prNumber":-1}),
        json!({"prNumber":1.5}),
        json!({"prNumber":"17"}),
        json!({"prNumber":u64::MAX}),
        json!({"prNumber":17,"repo":false}),
        json!({"prNumber":17,"repo":"Owner/Repo","gitRootId":"root"}),
        json!({"prNumber":17,"repo":"Owner/Repo","target":target}),
        json!({"prNumber":17,"gitRootId":"root","target":target}),
        json!({"prNumber":17,"gitRootId":""}),
        json!({"prNumber":17,"gitRootId":null}),
        json!({"prNumber":17,"target":null}),
        json!({"prNumber":18,"target":target}),
        json!({"prNumber":17,"connectionId":"caller-chosen"}),
        json!({"prNumber":17,"agentId":"someone-else"}),
        json!({"prNumber":9_007_199_254_740_992_u64,"gitRootId":"root"}),
    ] {
        assert!(
            serde_json::from_value::<ReviewMonitorRequest>(value.clone()).is_err(),
            "{value}"
        );
    }
    for (provider, kind) in [
        ("gitlab", "pull-request"),
        ("github", "merge-request"),
        ("gitlab", "issue"),
    ] {
        let mut value = fixture()["requests"][3].clone();
        value["target"]["repository"]["provider"] = json!(provider);
        value["target"]["kind"] = json!(kind);
        assert!(serde_json::from_value::<ReviewMonitorRequest>(value).is_err());
    }
}

#[test]
fn same_number_does_not_collapse_forge_instance_project_account_or_host() {
    let original: ReviewMonitorBinding =
        serde_json::from_value(fixture()["binding"].clone()).unwrap();
    assert_eq!(
        serde_json::to_value(&original).unwrap(),
        fixture()["binding"]
    );
    for path in [
        vec!["resource", "repository", "provider"],
        vec!["resource", "repository", "instanceBaseUrl"],
        vec!["resource", "repository", "projectPath"],
        vec!["connection", "accountId"],
        vec!["connection", "connectionId"],
        vec!["scope", "daemonId"],
        vec!["scope", "authorityScopeId"],
    ] {
        let mut value = fixture()["binding"].clone();
        let replacement = if path.last() == Some(&"provider") {
            "github"
        } else {
            "different"
        };
        let mut leaf = &mut value;
        for key in path {
            leaf = &mut leaf[key];
        }
        *leaf = json!(replacement);
        let changed: ReviewMonitorBinding = serde_json::from_value(value).unwrap();
        assert_ne!(original, changed);
    }
    for field in ["authorityGeneration", "connectionGeneration"] {
        let mut value = fixture()["binding"].clone();
        let parent = if field == "authorityGeneration" {
            "scope"
        } else {
            "connection"
        };
        value[parent][field] = json!("18446744073709551614");
        assert_ne!(
            original,
            serde_json::from_value::<ReviewMonitorBinding>(value).unwrap()
        );
    }
}

#[test]
fn optional_extensions_preserve_old_rows_and_unknown_states() {
    assert_eq!(
        serde_json::to_value(ReviewMonitorExtension::default()).unwrap(),
        json!({})
    );
    assert_eq!(
        serde_json::from_value::<ReviewMonitorExtension>(
            json!({"repo":"Owner/Repo","prNumber":17})
        )
        .unwrap(),
        ReviewMonitorExtension::default()
    );
    for value in fixture()["extensions"].as_array().unwrap() {
        let extension: ReviewMonitorExtension = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(extension).unwrap(), *value);
    }
    assert!(serde_json::from_value::<ReviewMonitorExtension>(json!({
        "monitorAvailability":{"state":"backoff"}
    }))
    .is_err());
    let unresolved: ReviewMonitorExtension = serde_json::from_value(json!({
        "monitorAvailability":{"state":"unresolved","reason":"future-reason"}
    }))
    .unwrap();
    assert_eq!(
        unresolved.monitor_availability,
        Some(ReviewMonitorAvailability::Unresolved {
            reason: ReviewMonitorUnresolvedReason::Unknown,
        })
    );
    // Future state words stay inspectable, not coerced to available/readiness.
    let extension: ReviewMonitorExtension =
        serde_json::from_value(json!({"monitorAvailability":{"state":"future-state"}})).unwrap();
    assert_eq!(
        extension.monitor_availability,
        Some(ReviewMonitorAvailability::Unknown)
    );
}

#[test]
fn lifecycle_events_and_wakes_keep_old_names_and_optional_fields() {
    let value = fixture()["event"].clone();
    let event: ReviewMonitorEventData = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(event).unwrap(), value);
    let value = fixture()["wake"].clone();
    let wake: ReviewMonitorWakeMetadata = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(wake).unwrap(), value);
    let old: OldWake = serde_json::from_value(value).unwrap();
    assert_eq!(
        (
            old.r#type.as_str(),
            old.monitor_id.as_str(),
            old.repo.as_str(),
            old.pr_number,
            old.reason.as_str()
        ),
        (
            "pr_monitor_wake",
            "monitor-17",
            "Team/Sub/Project",
            17,
            "changed"
        )
    );
    for key in ["resource", "monitorAvailability", "url", "pausedUntil"] {
        let mut old = fixture()["wake"].clone();
        old.as_object_mut().unwrap().remove(key);
        serde_json::from_value::<ReviewMonitorWakeMetadata>(old).unwrap();
    }
    let legacy = json!({"type":"pr_monitor_wake","monitorId":"m","repo":"a/b","prNumber":17,"reason":"completed"});
    assert_eq!(
        serde_json::to_value(
            serde_json::from_value::<ReviewMonitorWakeMetadata>(legacy.clone()).unwrap()
        )
        .unwrap(),
        legacy
    );
}

#[test]
fn capability_absence_is_unsupported_and_declarations_do_not_advertise_it() {
    let absent: ReviewMonitoringCapabilities =
        serde_json::from_value(json!({"liveState":true})).unwrap();
    assert!(!absent.supports_review_monitoring());
    assert_eq!(serde_json::to_value(absent).unwrap(), json!({}));
    for (version, supported) in [(0, false), (1, true), (2, true)] {
        let capabilities: ReviewMonitoringCapabilities =
            serde_json::from_value(json!({"reviewMonitoring":version})).unwrap();
        assert_eq!(capabilities.supports_review_monitoring(), supported);
    }
    assert!(serde_json::from_value::<ReviewMonitoringCapabilities>(
        json!({"reviewMonitoring":true})
    )
    .is_err());
}

#[tokio::test]
async fn old_workspace_api_implementations_refuse_addressed_calls_without_legacy_fallback() {
    struct OldApi;
    impl WorkspaceApi for OldApi {
        #[track_caller]
        fn pr_state(
            &self,
            _: WorkspaceId,
            _: u64,
            _: Option<String>,
        ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
            panic!("must not fall back to legacy GitHub")
        }
        #[track_caller]
        fn pr_monitor_start(
            &self,
            _: WorkspaceId,
            _: AgentId,
            _: u64,
            _: Option<String>,
        ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
            panic!("must not create a legacy monitor")
        }
        #[track_caller]
        fn pr_monitor_stop(
            &self,
            _: WorkspaceId,
            _: AgentId,
            _: u64,
            _: Option<String>,
        ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
            panic!("must not cancel a colliding legacy monitor")
        }
    }
    let request: ReviewMonitorRequest =
        serde_json::from_value(fixture()["requests"][3].clone()).unwrap();
    let ws = WorkspaceId::from_string("workspace");
    let owner = AgentId::from_string("owner");
    for result in [
        OldApi.pr_state_addressed(ws.clone(), request.clone()).await,
        OldApi
            .pr_monitor_start_addressed(ws.clone(), owner.clone(), request.clone())
            .await,
        OldApi.pr_monitor_stop_addressed(ws, owner, request).await,
    ] {
        assert!(matches!(result, Err(Error::Unsupported(_))));
    }
}

#[test]
fn recipient_projection_blocks_unsafe_legacy_inputs_on_every_surface() {
    let golden: Value =
        serde_json::from_str(include_str!("fixtures/review_monitor_projection_v1.json")).unwrap();
    let enabled = ReviewMonitoringCapabilities {
        review_monitoring: Some(1),
    };
    let project = |host: &ReviewMonitoringCapabilities, capabilities: Value| {
        let recipient = ReviewMonitorRecipientSupport::from_capabilities(&capabilities);
        golden["input"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(surface, entries)| {
                let entries: Vec<Value> = entries
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|entry| {
                        let payload = entry["payload"].clone();
                        let output = match entry["origin"].as_str().unwrap() {
                            "legacy-github" => ReviewMonitorOutput::LegacyGithub(payload),
                            "qualified" => ReviewMonitorOutput::Qualified(payload),
                            other => panic!("invalid fixture origin {other}"),
                        };
                        output.project(host, recipient)
                    })
                    .collect();
                (surface.clone(), json!(entries))
            })
            .collect::<serde_json::Map<String, Value>>()
    };
    for capabilities in [
        json!({}),
        json!(null),
        json!({"reviewMonitoring": null}),
        json!({"reviewMonitoring": false}),
        json!({"reviewMonitoring": "1"}),
        json!({"reviewMonitoring": 0}),
        json!({"reviewMonitoring": -1}),
        json!({"reviewMonitoring": 1.5}),
    ] {
        assert_eq!(json!(project(&enabled, capabilities)), golden["legacy"]);
    }
    let modern = json!({"reviewMonitoring": 1});
    let actual = json!(project(&enabled, modern.clone()));
    assert_eq!(actual, golden["qualified"]);
    assert_eq!(
        json!(project(&ReviewMonitoringCapabilities::default(), modern)),
        golden["legacy"]
    );
    assert_eq!(
        json!(project(&enabled, json!({"reviewMonitoring": 2}))),
        actual
    );

    // Explicitly exercise the review regressions: colliding identity, unknown
    // approvals, and no URL must survive unchanged ONLY for supported recipients.
    assert_eq!(actual["list"][0]["prNumber"], actual["list"][1]["prNumber"]);
    assert_eq!(actual["list"][0]["repo"], actual["list"][1]["repo"]);
    assert_ne!(actual["list"][0]["url"], actual["list"][1]["url"]);
    assert!(actual["list"][1]["lastSnapshot"]["approvals"]["have"].is_null());
    assert!(actual["list"][2]["lastSnapshot"]["approvals"]["have"].is_null());
    assert!(actual["wake"][1]["metadata"].get("url").is_none());
    assert!(actual["missing-url"][0].get("url").is_none());
    assert_eq!(golden["legacy"]["wake"].as_array().unwrap().len(), 1);
    assert!(golden["legacy"]["unresolved"]
        .as_array()
        .unwrap()
        .is_empty());

    // Optional source-bound oracle consumes actual Rust-projected output, not a
    // handwritten JS filter. Normal core tests do not require Node or a FE tree.
    if let Some(path) = std::env::var_os("REVIEW_MONITOR_PROJECTION_OUT") {
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&json!({
                "legacy": project(&enabled, json!({})),
                "qualified": actual,
            }))
            .unwrap(),
        )
        .unwrap();
    }
}

#[test]
fn proxy_or_other_session_support_cannot_upgrade_a_legacy_recipient() {
    let host = ReviewMonitoringCapabilities {
        review_monitoring: Some(1),
    };
    let supported =
        ReviewMonitorRecipientSupport::from_capabilities(&json!({"reviewMonitoring":1}));
    let old_session = ReviewMonitorRecipientSupport::default();
    for recipient in [
        old_session,
        supported.intersect(old_session),
        old_session.intersect(supported),
    ] {
        assert_eq!(
            ReviewMonitorOutput::Qualified("private").project(&host, recipient),
            None
        );
        assert_eq!(
            ReviewMonitorOutput::LegacyGithub("unchanged").project(&host, recipient),
            Some("unchanged")
        );
    }
    assert_eq!(
        ReviewMonitorOutput::Qualified("private").project(&host, supported.intersect(supported)),
        Some("private")
    );
    // A fresh connection/session starts legacy; no previous receiver state is inherited.
    assert_eq!(
        ReviewMonitorOutput::Qualified("queued")
            .project(&host, ReviewMonitorRecipientSupport::default()),
        None
    );
}
