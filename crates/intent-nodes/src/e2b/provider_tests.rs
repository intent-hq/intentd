//! E2B behind [`CloudProvider`]: the neutral calls reach the same E2B
//! endpoints as the client, and E2B failures keep their meaning and text.

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::json;

use super::fake::{frame, FakeServer, Reply};
use super::*;
use crate::cloud::{
    CloudErrorKind, CloudInstance, CloudProvider, CreateInstance, InstanceState, ListInstances,
};

const KEY: &str = "e2b_test_key_0123456789abcdef";
const ENVD_TOKEN: &str = "envd-token-secret-value";

fn provider(server: &FakeServer) -> std::sync::Arc<dyn CloudProvider> {
    std::sync::Arc::new(
        E2bClient::new(
            SecretString::new(KEY),
            E2bConfig {
                api_url: server.url.clone(),
                envd_url: server.url.clone(),
                default_retry_wait: Duration::from_millis(10),
                max_rate_limit_retries: 0,
                create_spacing: Duration::ZERO,
                ..E2bConfig::default()
            },
        )
        .unwrap(),
    )
}

fn sandbox_body(id: &str) -> serde_json::Value {
    json!({
        "templateID": "intent/intent-node-2x4:v1",
        "sandboxID": id,
        "envdVersion": "0.2.0",
        "envdAccessToken": ENVD_TOKEN,
    })
}

fn process_stream(stdout: &str, exit_code: i32) -> Reply {
    use base64::Engine as _;
    let b64 = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);
    let mut body = frame(0, &json!({"event": {"start": {"pid": 7}}}));
    body.extend(frame(
        0,
        &json!({"event": {"data": {"stdout": b64(stdout)}}}),
    ));
    body.extend(frame(
        0,
        &json!({"event": {"end": {"exitCode": exit_code, "exited": true, "status": "exit status"}}}),
    ));
    body.extend(frame(2, &json!({})));
    Reply::Stream {
        parts: vec![(body, None)],
    }
}

#[test]
fn the_provider_is_e2b() {
    let client = E2bClient::new(SecretString::new(KEY), E2bConfig::default()).unwrap();
    assert_eq!(CloudProvider::kind(&client), "e2b");
}

#[tokio::test]
async fn create_sends_the_image_tags_and_key_and_keeps_the_access_token() {
    let server = FakeServer::start(vec![Reply::json(201, &sandbox_body("sbx1"))]).await;
    let instance = provider(&server)
        .create(&CreateInstance {
            image: "intent/intent-node-2x4:v1".into(),
            timeout_secs: Some(1800),
            pause_on_timeout: Some(true),
            tags: BTreeMap::from([("agentId".into(), "a1".into())]),
            idempotency_key: Some("intent-a1-x".into()),
            ..CreateInstance::default()
        })
        .await
        .unwrap();
    assert_eq!(instance.id, "sbx1");
    assert_eq!(instance.image, "intent/intent-node-2x4:v1");
    assert_eq!(instance.access_token.as_ref().unwrap().expose(), ENVD_TOKEN);
    assert!(!format!("{instance:?}").contains(ENVD_TOKEN));

    let [req] = server.requests().try_into().unwrap();
    assert_eq!(req.target, "/v2/sandboxes");
    assert_eq!(req.header("x-api-key"), Some(KEY));
    assert_eq!(req.header("idempotency-key"), Some("intent-a1-x"));
    assert_eq!(
        req.json(),
        json!({
            "templateID": "intent/intent-node-2x4:v1",
            "timeout": 1800,
            "autoPause": true,
            "metadata": {"agentId": "a1"}
        })
    );
}

#[tokio::test]
async fn run_command_reaches_envd_with_the_instance_token() {
    let server = FakeServer::start(vec![process_stream("hi\n", 3)]).await;
    let instance = CloudInstance {
        id: "sbx1".into(),
        image: "base".into(),
        access_token: Some(SecretString::new(ENVD_TOKEN)),
        volume_id: None,
    };
    let mut spec = ProcessSpec::new("/bin/sh");
    spec.args = vec!["-c".into(), "echo hi; exit 3".into()];
    let output = provider(&server)
        .run_command(&instance, &spec)
        .await
        .unwrap();
    assert_eq!(output.stdout, b"hi\n");
    assert_eq!(output.exit.exit_code, 3);
    assert!(!output.success());

    let [req] = server.requests().try_into().unwrap();
    assert_eq!(req.target, "/process.Process/Start");
    assert_eq!(req.header("e2b-sandbox-id"), Some("sbx1"));
    assert_eq!(req.header("x-access-token"), Some(ENVD_TOKEN));
}

#[tokio::test]
async fn connect_reports_a_resume_and_the_new_token() {
    let server = FakeServer::start(vec![
        Reply::json(201, &sandbox_body("sbx1")),
        Reply::json(200, &sandbox_body("sbx1")),
    ])
    .await;
    let provider = provider(&server);
    let resumed = provider.connect("sbx1", 1800).await.unwrap();
    assert!(resumed.resumed);
    assert_eq!(resumed.instance.id, "sbx1");
    assert_eq!(
        resumed.instance.access_token.as_ref().unwrap().expose(),
        ENVD_TOKEN
    );
    assert!(!provider.connect("sbx1", 1800).await.unwrap().resumed);
    let reqs = server.requests();
    assert_eq!(reqs[0].target, "/v2/sandboxes/sbx1/connect");
    assert_eq!(reqs[0].json(), json!({"timeout": 1800}));
}

#[tokio::test]
async fn pause_keep_alive_and_delete_reach_their_endpoints() {
    let server = FakeServer::start(vec![
        Reply::status(204),
        Reply::status(204),
        Reply::status(204),
        Reply::error(404, "", "gone"),
    ])
    .await;
    let provider = provider(&server);
    provider.pause("sbx1").await.unwrap();
    provider.keep_alive("sbx1", 1800).await.unwrap();
    assert!(provider.delete("sbx1").await.unwrap());
    assert!(!provider.delete("sbx1").await.unwrap());
    let targets: Vec<_> = server
        .requests()
        .into_iter()
        .map(|r| format!("{} {}", r.method, r.target))
        .collect();
    assert_eq!(
        targets,
        [
            "POST /sandboxes/sbx1/pause",
            "POST /sandboxes/sbx1/timeout",
            "DELETE /sandboxes/sbx1",
            "DELETE /sandboxes/sbx1",
        ]
    );
}

#[tokio::test]
async fn list_filters_by_tags_and_states() {
    let server = FakeServer::start(vec![Reply::json(
        200,
        &json!([
            {"sandboxID": "s1", "templateID": "t", "state": "running",
             "metadata": {"agentId": "a1"}, "startedAt": "2026-11-11T10:00:00Z"},
            {"sandboxID": "s2", "templateID": "t", "state": "paused"},
            {"sandboxID": "s3", "templateID": "t", "state": "snapshotting"}
        ]),
    )])
    .await;
    let listed = provider(&server)
        .list(&ListInstances {
            tags: BTreeMap::from([("intentBackendId".into(), "b1".into())]),
            states: vec![InstanceState::Running, InstanceState::Paused],
        })
        .await
        .unwrap();
    let summary: Vec<_> = listed
        .iter()
        .map(|i| (i.id.as_str(), i.state, i.started_at.as_str()))
        .collect();
    assert_eq!(
        summary,
        [
            ("s1", InstanceState::Running, "2026-11-11T10:00:00Z"),
            ("s2", InstanceState::Paused, ""),
            ("s3", InstanceState::Other, ""),
        ]
    );
    assert_eq!(
        listed[0].tags.get("agentId").map(String::as_str),
        Some("a1")
    );
    let [req] = server.requests().try_into().unwrap();
    assert!(req.target.starts_with("/v2/sandboxes?"));
    assert!(req.target.contains("metadata=intentBackendId%3Db1"));
    assert!(req.target.contains("state=running%2Cpaused"));
}

#[tokio::test]
async fn image_ready_reads_the_template_alias() {
    let server = FakeServer::start(vec![
        Reply::json(200, &json!({})),
        Reply::error(404, "", "no such alias"),
    ])
    .await;
    let provider = provider(&server);
    assert!(provider
        .image_ready("intent/intent-node-2x4:v1")
        .await
        .unwrap());
    assert!(!provider
        .image_ready("intent/intent-node-2x4:v2")
        .await
        .unwrap());
    let targets: Vec<_> = server.requests().into_iter().map(|r| r.target).collect();
    assert_eq!(
        targets,
        [
            "/templates/aliases/intent%2Fintent-node-2x4:v1",
            "/templates/aliases/intent%2Fintent-node-2x4:v2",
        ]
    );
    for bad in ["no-tag", "a:b:c", ":v1", "intent/x:"] {
        let error = provider.image_ready(bad).await.unwrap_err();
        assert_eq!(error.kind(), CloudErrorKind::InvalidArgument, "{bad}");
    }
}

#[tokio::test]
async fn check_key_reports_a_rejected_key() {
    let server = FakeServer::start(vec![Reply::status(200), Reply::error(401, "", "bad")]).await;
    let provider = provider(&server);
    provider.check_key().await.unwrap();
    let error = provider.check_key().await.unwrap_err();
    assert_eq!(error.kind(), CloudErrorKind::Unauthorized);
    assert_eq!(error.status(), Some(401));
}

#[test]
fn e2b_failures_keep_their_class_status_retry_and_text() {
    use crate::cloud::{CloudError, QuotaLimit};
    let cases: Vec<(E2bError, CloudErrorKind, Option<u16>, bool)> = vec![
        (
            E2bError::InvalidArgument("x".into()),
            CloudErrorKind::InvalidArgument,
            None,
            false,
        ),
        (
            E2bError::NotConfigured,
            CloudErrorKind::NotConfigured,
            None,
            false,
        ),
        (
            E2bError::RateLimited {
                retry_after: Some(Duration::from_secs(7)),
                error_code: None,
                message: "slow".into(),
            },
            CloudErrorKind::Quota(QuotaLimit::Rate),
            None,
            true,
        ),
        (
            E2bError::ConcurrencyLimit {
                error_code: None,
                message: "full".into(),
            },
            CloudErrorKind::Quota(QuotaLimit::Concurrency),
            None,
            false,
        ),
        (
            E2bError::PlanLimit {
                status: 400,
                error_code: None,
                message: "too big".into(),
            },
            CloudErrorKind::Quota(QuotaLimit::Plan),
            Some(400),
            false,
        ),
        (
            E2bError::NotFound("gone".into()),
            CloudErrorKind::NotFound,
            None,
            false,
        ),
        (
            E2bError::Conflict("pausing".into()),
            CloudErrorKind::Conflict,
            None,
            true,
        ),
        (
            E2bError::Unauthorized { status: 403 },
            CloudErrorKind::Unauthorized,
            Some(403),
            false,
        ),
        (
            E2bError::Api {
                status: 502,
                error_code: None,
                message: "bad gateway".into(),
                should_retry: None,
            },
            CloudErrorKind::Api,
            Some(502),
            true,
        ),
        (
            E2bError::Api {
                status: 500,
                error_code: None,
                message: "boom".into(),
                should_retry: Some(true),
            },
            CloudErrorKind::Api,
            Some(500),
            true,
        ),
        (
            E2bError::PauseRefused("busy".into()),
            CloudErrorKind::Other,
            None,
            true,
        ),
        (
            E2bError::Unreachable("dns".into()),
            CloudErrorKind::Other,
            None,
            true,
        ),
        (
            E2bError::Transport("reset".into()),
            CloudErrorKind::Other,
            None,
            false,
        ),
        (
            E2bError::IdempotencyOutcomeUnknown,
            CloudErrorKind::Other,
            None,
            false,
        ),
    ];
    for (e2b, kind, status, retryable) in cases {
        let text = e2b.to_string();
        let quota = e2b.quota_limit();
        let error = CloudError::from(e2b);
        assert_eq!(error.kind(), kind, "{text}");
        assert_eq!(error.status(), status, "{text}");
        assert_eq!(error.is_retryable(), retryable, "{text}");
        assert_eq!(error.quota_limit(), quota, "{text}");
        assert_eq!(error.to_string(), text);
    }
    let limited = CloudError::from(E2bError::RateLimited {
        retry_after: Some(Duration::from_secs(7)),
        error_code: None,
        message: "slow".into(),
    });
    assert_eq!(limited.retry_after(), Some(Duration::from_secs(7)));
    assert_eq!(
        CloudError::from(E2bError::NotFound("x".into())).retry_after(),
        None
    );
}
