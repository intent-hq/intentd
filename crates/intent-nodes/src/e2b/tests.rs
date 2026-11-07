use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use base64::Engine as _;
use serde_json::json;
use tokio::sync::oneshot;

use super::fake::{frame, FakeServer, Reply};
use super::*;

const KEY: &str = "e2b_test_key_0123456789abcdef";
const ENVD_TOKEN: &str = "envd-token-secret-value";
const TRAFFIC_TOKEN: &str = "traffic-token-secret-value";

fn hang_guard() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(60))
}

fn client(server: &FakeServer) -> E2bClient {
    E2bClient::new(
        SecretString::new(KEY),
        E2bConfig {
            api_url: server.url.clone(),
            envd_url: server.url.clone(),
            default_retry_wait: Duration::from_millis(10),
            create_spacing: Duration::ZERO,
            ..E2bConfig::default()
        },
    )
    .unwrap()
}

fn sandbox_body(id: &str) -> serde_json::Value {
    json!({
        "templateID": "intent-node",
        "sandboxID": id,
        "clientID": "c",
        "envdVersion": "0.2.0",
        "envdAccessToken": ENVD_TOKEN,
        "trafficAccessToken": TRAFFIC_TOKEN,
        "domain": "e2b.app"
    })
}

fn create_request() -> CreateSandbox {
    CreateSandbox {
        template_id: "intent-node".into(),
        timeout_secs: Some(600),
        auto_pause: Some(true),
        metadata: BTreeMap::from([
            ("intentBackendId".into(), "b1".into()),
            ("agentId".into(), "a1".into()),
        ]),
        idempotency_key: Some("intent-b1-p1".into()),
    }
}

fn assert_no_secrets(text: &str) {
    for secret in [KEY, ENVD_TOKEN, TRAFFIC_TOKEN] {
        assert!(!text.contains(secret), "secret leaked into {text}");
    }
}

#[tokio::test]
async fn create_sends_key_idempotency_and_body_and_redacts_tokens() {
    let server = FakeServer::start(vec![Reply::json(201, &sandbox_body("sbx1"))]).await;
    let client = client(&server);
    let sandbox = client.create(&create_request()).await.unwrap();

    assert_eq!(sandbox.sandbox_id, "sbx1");
    assert_eq!(sandbox.template_id, "intent-node");
    assert_eq!(sandbox.envd_version, "0.2.0");
    assert_eq!(sandbox.domain.as_deref(), Some("e2b.app"));
    assert_eq!(
        sandbox.envd_access_token.as_ref().unwrap().expose(),
        ENVD_TOKEN
    );
    assert_no_secrets(&format!("{sandbox:?} {client:?}"));

    let [req] = server.requests().try_into().unwrap();
    assert_eq!(req.method, "POST");
    assert_eq!(req.target, "/v2/sandboxes");
    assert_eq!(req.header("x-api-key"), Some(KEY));
    assert_eq!(req.header("idempotency-key"), Some("intent-b1-p1"));
    assert_eq!(
        req.json(),
        json!({
            "templateID": "intent-node",
            "timeout": 600,
            "autoPause": true,
            "metadata": {"agentId": "a1", "intentBackendId": "b1"}
        })
    );
}

#[tokio::test]
async fn create_omits_unset_options() {
    let server = FakeServer::start(vec![Reply::json(201, &sandbox_body("sbx1"))]).await;
    client(&server)
        .create(&CreateSandbox {
            template_id: "base".into(),
            ..CreateSandbox::default()
        })
        .await
        .unwrap();
    let [req] = server.requests().try_into().unwrap();
    assert_eq!(req.json(), json!({"templateID": "base"}));
    assert_eq!(req.header("idempotency-key"), None);
}

#[tokio::test]
async fn create_rejects_bad_input_before_any_request() {
    let server = FakeServer::start(vec![]).await;
    let client = client(&server);
    for key in ["", "Upper", "under_score", &"a".repeat(256)] {
        let mut req = create_request();
        req.idempotency_key = Some(key.to_string());
        assert!(matches!(
            client.create(&req).await,
            Err(E2bError::InvalidArgument(_))
        ));
    }
    let mut req = create_request();
    req.template_id = String::new();
    assert!(matches!(
        client.create(&req).await,
        Err(E2bError::InvalidArgument(_))
    ));
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn create_422_reports_idempotency_outcome_unknown() {
    let server = FakeServer::start(vec![Reply::error(
        422,
        "idempotency_outcome_unknown",
        "outcome unknown",
    )])
    .await;
    let err = client(&server).create(&create_request()).await.unwrap_err();
    assert!(
        matches!(err, E2bError::IdempotencyOutcomeUnknown),
        "{err:?}"
    );
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn create_409_mismatch_is_not_retried() {
    let server = FakeServer::start(vec![Reply::error(
        409,
        "idempotency_request_mismatch",
        "different request",
    )])
    .await;
    let err = client(&server).create(&create_request()).await.unwrap_err();
    assert!(matches!(err, E2bError::IdempotencyMismatch), "{err:?}");
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn create_409_in_progress_is_retried_with_the_same_key() {
    let server = FakeServer::start(vec![
        Reply::error(409, "idempotency_in_progress", "in progress"),
        Reply::json(201, &sandbox_body("sbx1")),
    ])
    .await;
    let sandbox = client(&server).create(&create_request()).await.unwrap();
    assert_eq!(sandbox.sandbox_id, "sbx1");
    let reqs = server.requests();
    assert_eq!(reqs.len(), 2);
    assert_eq!(reqs[0].body, reqs[1].body);
    assert!(reqs
        .iter()
        .all(|r| r.header("idempotency-key") == Some("intent-b1-p1")));
}

#[tokio::test]
async fn rate_limit_waits_for_retry_after_then_succeeds() {
    let server = FakeServer::start(vec![
        Reply::error(429, "", "slow down").with_header("retry-after", "1"),
        Reply::json(201, &sandbox_body("sbx1")),
    ])
    .await;
    let started = Instant::now();
    let sandbox = client(&server).create(&create_request()).await.unwrap();
    assert_eq!(sandbox.sandbox_id, "sbx1");
    let reqs = server.requests();
    assert_eq!(reqs.len(), 2);
    assert!(reqs[1].at.duration_since(reqs[0].at) >= Duration::from_secs(1));
    assert!(started.elapsed() >= Duration::from_secs(1));
}

#[tokio::test]
async fn rate_limit_gives_up_after_the_retry_budget() {
    let replies = (0..3)
        .map(|_| Reply::error(429, "", "slow down").with_header("retry-after", "0"))
        .collect();
    let server = FakeServer::start(replies).await;
    let mut client = client(&server);
    client.config.max_rate_limit_retries = 2;
    let err = client.set_timeout("sbx1", 60).await.unwrap_err();
    assert!(
        matches!(&err, E2bError::RateLimited { retry_after: Some(d), message, .. } if *d == Duration::ZERO && message == "slow down"),
        "{err:?}"
    );
    assert_eq!(server.requests().len(), 3);
    assert_eq!(err.quota_limit(), Some(QuotaLimit::Rate));
    assert_eq!(QuotaLimit::Rate.as_str(), "rate");
}

#[tokio::test]
async fn rate_limit_wait_is_capped() {
    let server = FakeServer::start(vec![
        Reply::error(429, "", "slow down").with_header("retry-after", "3600"),
        Reply::status(204),
    ])
    .await;
    let mut client = client(&server);
    client.config.max_retry_wait = Duration::from_millis(50);
    // Hang guard: an uncapped wait would sleep for the hour E2B asked for.
    tokio::time::timeout(hang_guard(), client.set_timeout("sbx1", 60))
        .await
        .expect("the retry wait is capped")
        .unwrap();
}

#[tokio::test]
async fn creates_are_serialized_and_spaced() {
    let server = FakeServer::start(vec![
        Reply::json(201, &sandbox_body("sbx1")).with_delay(Duration::from_millis(150)),
        Reply::json(201, &sandbox_body("sbx2")).with_delay(Duration::from_millis(150)),
    ])
    .await;
    let mut client = client(&server);
    client.config.create_spacing = Duration::from_millis(200);
    let mut first = create_request();
    first.idempotency_key = Some("k1".into());
    let mut second = create_request();
    second.idempotency_key = Some("k2".into());
    let (a, b) = tokio::join!(client.create(&first), client.create(&second));
    let mut ids = vec![a.unwrap().sandbox_id, b.unwrap().sandbox_id];
    ids.sort();
    assert_eq!(ids, ["sbx1", "sbx2"]);
    assert_eq!(server.max_in_flight(), 1);
    let reqs = server.requests();
    assert!(reqs[1].at.duration_since(reqs[0].at) >= Duration::from_millis(350));
}

#[tokio::test]
async fn connect_reports_resumed_or_running() {
    let server = FakeServer::start(vec![
        Reply::json(201, &sandbox_body("sbx1")),
        Reply::json(200, &sandbox_body("sbx1")),
    ])
    .await;
    let client = client(&server);
    let resumed = client.connect("sbx1", 900).await.unwrap();
    assert!(resumed.resumed);
    assert_eq!(resumed.sandbox.sandbox_id, "sbx1");
    let running = client.connect("sbx1", 900).await.unwrap();
    assert!(!running.resumed);
    let reqs = server.requests();
    assert_eq!(reqs[0].method, "POST");
    assert_eq!(reqs[0].target, "/v2/sandboxes/sbx1/connect");
    assert_eq!(reqs[0].json(), json!({"timeout": 900}));
    assert_eq!(reqs[0].header("x-api-key"), Some(KEY));
}

#[tokio::test]
async fn connect_maps_not_found_and_conflict() {
    let server = FakeServer::start(vec![
        Reply::error(404, "", "sandbox not found"),
        Reply::error(409, "", "sandbox is pausing"),
    ])
    .await;
    let client = client(&server);
    assert!(matches!(
        client.connect("sbx1", 60).await,
        Err(E2bError::NotFound(_))
    ));
    assert!(matches!(
        client.connect("sbx1", 60).await,
        Err(E2bError::Conflict(_))
    ));
}

#[tokio::test]
async fn pause_succeeds_on_204_and_reports_503_refusal() {
    let server = FakeServer::start(vec![
        Reply::status(204),
        Reply::error(503, "", "node busy with snapshot work"),
        Reply::error(409, "", "sandbox is pausing"),
    ])
    .await;
    let client = client(&server);
    client.pause("sbx1").await.unwrap();
    let err = client.pause("sbx1").await.unwrap_err();
    assert!(matches!(err, E2bError::PauseRefused(_)), "{err:?}");
    assert!(err.is_retryable());
    assert!(matches!(
        client.pause("sbx1").await,
        Err(E2bError::Conflict(_))
    ));
    let reqs = server.requests();
    assert_eq!(reqs[0].method, "POST");
    assert_eq!(reqs[0].target, "/sandboxes/sbx1/pause");
}

#[tokio::test]
async fn set_timeout_posts_seconds() {
    let server = FakeServer::start(vec![Reply::status(204)]).await;
    client(&server).set_timeout("sbx1", 1800).await.unwrap();
    let [req] = server.requests().try_into().unwrap();
    assert_eq!(req.method, "POST");
    assert_eq!(req.target, "/sandboxes/sbx1/timeout");
    assert_eq!(req.json(), json!({"timeout": 1800}));
}

#[tokio::test]
async fn delete_is_idempotent_on_404() {
    let server = FakeServer::start(vec![
        Reply::status(204),
        Reply::error(404, "", "not found"),
        Reply::error(500, "internal_server_error", "removal failed"),
    ])
    .await;
    let client = client(&server);
    assert!(client.delete("sbx1").await.unwrap());
    assert!(!client.delete("sbx1").await.unwrap());
    let err = client.delete("sbx1").await.unwrap_err();
    assert!(
        matches!(&err, E2bError::Api { status: 500, error_code: Some(c), .. } if c == "internal_server_error"),
        "{err:?}"
    );
    let reqs = server.requests();
    assert_eq!(reqs[0].method, "DELETE");
    assert_eq!(reqs[0].target, "/sandboxes/sbx1");
}

#[tokio::test]
async fn sandbox_ids_are_validated() {
    let server = FakeServer::start(vec![]).await;
    let client = client(&server);
    for id in ["", "../templates", "a/b", "a?b", "a b"] {
        assert!(matches!(
            client.pause(id).await,
            Err(E2bError::InvalidArgument(_))
        ));
        assert!(matches!(
            client.delete(id).await,
            Err(E2bError::InvalidArgument(_))
        ));
    }
    assert!(server.requests().is_empty());
}

fn listed(id: &str, state: &str) -> serde_json::Value {
    json!({
        "templateID": "intent-node",
        "sandboxID": id,
        "clientID": "c",
        "startedAt": "2026-11-06T10:00:00Z",
        "endAt": "2026-11-06T11:00:00Z",
        "cpuCount": 2,
        "memoryMB": 1024,
        "diskSizeMB": 10240,
        "metadata": {"intentBackendId": "b1"},
        "state": state,
        "envdVersion": "0.2.0"
    })
}

#[tokio::test]
async fn list_filters_by_metadata_and_follows_pages() {
    let server = FakeServer::start(vec![
        Reply::json(200, &json!([listed("sbx1", "running")])).with_header("x-next-token", "page 2"),
        Reply::json(
            200,
            &json!([listed("sbx2", "paused"), listed("sbx3", "snapshotting")]),
        ),
    ])
    .await;
    let found = client(&server)
        .list(&ListSandboxes {
            metadata: BTreeMap::from([
                ("intentBackendId".into(), "b1".into()),
                ("agent id".into(), "a&1".into()),
            ]),
            states: vec![SandboxState::Running, SandboxState::Paused],
        })
        .await
        .unwrap();
    let ids: Vec<_> = found.iter().map(|s| s.sandbox_id.as_str()).collect();
    assert_eq!(ids, ["sbx1", "sbx2", "sbx3"]);
    assert_eq!(found[0].state, SandboxState::Running);
    assert_eq!(found[1].state, SandboxState::Paused);
    assert_eq!(found[2].state, SandboxState::Other);
    assert_eq!(found[0].metadata["intentBackendId"], "b1");
    assert_eq!(found[0].cpu_count, 2);
    assert_eq!(found[0].memory_mb, 1024);

    let reqs = server.requests();
    assert_eq!(reqs.len(), 2);
    let first = query(&reqs[0].target);
    assert_eq!(first["metadata"], "agent%20id=a%261&intentBackendId=b1");
    assert_eq!(first["state"], "running,paused");
    assert!(!first.contains_key("nextToken"));
    assert_eq!(query(&reqs[1].target)["nextToken"], "page 2");
    assert!(reqs[0].target.starts_with("/v2/sandboxes?"));
}

#[tokio::test]
async fn list_without_filters_sends_no_query() {
    let server = FakeServer::start(vec![Reply::json(200, &json!([]))]).await;
    let found = client(&server)
        .list(&ListSandboxes::default())
        .await
        .unwrap();
    assert!(found.is_empty());
    assert_eq!(server.requests()[0].target, "/v2/sandboxes");
}

fn query(target: &str) -> BTreeMap<String, String> {
    let (_, q) = target.split_once('?').unwrap_or((target, ""));
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap();
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap()
}

fn b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn sandbox() -> Sandbox {
    serde_json::from_value(sandbox_body("sbx1")).unwrap()
}

fn spec() -> ProcessSpec {
    let mut spec = ProcessSpec::new("/usr/local/bin/intentd");
    spec.user = Some("user".into());
    spec.args = vec!["call".into(), "executor.mint".into()];
    spec.cwd = Some("/home/user".into());
    spec.envs.insert(
        "INTENTD_TOKEN".into(),
        SecretString::new("env-secret-value"),
    );
    spec.timeout = Some(Duration::from_secs(30));
    spec
}

#[tokio::test]
async fn process_start_frames_the_request_and_sends_envd_headers() {
    let server = FakeServer::start(vec![Reply::Stream {
        parts: vec![
            (frame(0, &json!({"event": {"start": {"pid": 7}}})), None),
            (
                frame(
                    0,
                    &json!({"event": {"end": {"exited": true, "status": "exit status 0"}}}),
                ),
                None,
            ),
            (frame(2, &json!({})), None),
        ],
    }])
    .await;
    let output = client(&server)
        .run_process(&sandbox(), &spec())
        .await
        .unwrap();
    assert_eq!(output.pid, Some(7));
    assert_eq!(output.exit.exit_code, 0);
    assert!(output.exit.exited);
    assert!(output.success());

    let [req] = server.requests().try_into().unwrap();
    assert_eq!(req.method, "POST");
    assert_eq!(req.target, "/process.Process/Start");
    assert_eq!(req.header("content-type"), Some("application/connect+json"));
    assert_eq!(req.header("connect-protocol-version"), Some("1"));
    assert_eq!(req.header("connect-timeout-ms"), Some("30000"));
    assert_eq!(req.header("e2b-sandbox-id"), Some("sbx1"));
    assert_eq!(req.header("e2b-sandbox-port"), Some("49983"));
    assert_eq!(req.header("x-access-token"), Some(ENVD_TOKEN));
    assert_eq!(
        req.header("authorization"),
        Some(format!("Basic {}", b64(b"user:")).as_str())
    );
    assert_eq!(req.header("x-api-key"), None);
    assert_eq!(req.body[0], 0);
    let len = u32::from_be_bytes(req.body[1..5].try_into().unwrap()) as usize;
    assert_eq!(len, req.body.len() - 5);
    let payload: serde_json::Value = serde_json::from_slice(&req.body[5..]).unwrap();
    assert_eq!(
        payload,
        json!({
            "process": {
                "cmd": "/usr/local/bin/intentd",
                "args": ["call", "executor.mint"],
                "envs": {"INTENTD_TOKEN": "env-secret-value"},
                "cwd": "/home/user"
            },
            "stdin": false
        })
    );
    assert!(!format!("{:?}", spec()).contains("env-secret-value"));
}

#[tokio::test]
async fn process_output_streams_before_the_process_ends() {
    let (release, gate) = oneshot::channel();
    let mut first = frame(0, &json!({"event": {"start": {"pid": 9}}}));
    first.extend(frame(0, &json!({"event": {"keepalive": {}}})));
    let stdout = frame(
        0,
        &json!({"event": {"data": {"stdout": b64(b"token-abc\n")}}}),
    );
    // Split one frame across writes to exercise reassembly.
    let (head, tail) = stdout.split_at(3);
    first.extend_from_slice(head);
    let mut rest = tail.to_vec();
    rest.extend(frame(
        0,
        &json!({"event": {"data": {"stderr": b64(b"warn")}}}),
    ));
    rest.extend(frame(
        0,
        &json!({"event": {"end": {"exitCode": 3, "exited": true, "status": "exit status 3"}}}),
    ));
    rest.extend(frame(2, &json!({})));
    let server = FakeServer::start(vec![Reply::Stream {
        parts: vec![(first, None), (rest, Some(gate))],
    }])
    .await;
    let client = client(&server);
    let mut stream = client.start_process(&sandbox(), &spec()).await.unwrap();

    assert!(matches!(
        stream.next_event().await.unwrap(),
        Some(ProcessEvent::Started { pid: 9 })
    ));
    release.send(()).unwrap();
    match stream.next_event().await.unwrap() {
        Some(ProcessEvent::Stdout(chunk)) => {
            assert_eq!(chunk.as_bytes(), b"token-abc\n");
            assert!(!format!("{chunk:?}").contains("token-abc"));
        }
        other => panic!("expected stdout, got {other:?}"),
    }
    assert!(
        matches!(stream.next_event().await.unwrap(), Some(ProcessEvent::Stderr(c)) if c.as_bytes() == b"warn")
    );
    match stream.next_event().await.unwrap() {
        Some(ProcessEvent::Exited(exit)) => {
            assert_eq!(exit.exit_code, 3);
            assert_eq!(exit.status, "exit status 3");
        }
        other => panic!("expected exit, got {other:?}"),
    }
    assert!(stream.next_event().await.unwrap().is_none());
    assert!(stream.next_event().await.unwrap().is_none());
}

#[tokio::test]
async fn run_process_collects_output_and_redacts_it_in_debug() {
    let server = FakeServer::start(vec![Reply::Stream {
        parts: vec![(
            [
                frame(0, &json!({"event": {"start": {"pid": 1}}})),
                frame(0, &json!({"event": {"data": {"stdout": b64(b"tok")}}})),
                frame(0, &json!({"event": {"data": {"stdout": b64(b"en-xyz")}}})),
                frame(0, &json!({"event": {"end": {"exitCode": 0, "exited": true, "status": "exit status 0"}}})),
                frame(2, &json!({})),
            ]
            .concat(),
            None,
        )],
    }])
    .await;
    let output = client(&server)
        .run_process(&sandbox(), &spec())
        .await
        .unwrap();
    assert_eq!(output.stdout, b"token-xyz");
    assert!(output.stderr.is_empty());
    assert!(!format!("{output:?}").contains("token-xyz"));
}

#[tokio::test]
async fn process_end_stream_error_is_reported() {
    let server = FakeServer::start(vec![Reply::Stream {
        parts: vec![(
            frame(
                2,
                &json!({"error": {"code": "invalid_argument", "message": "cwd '/tmp/missing' does not exist"}}),
            ),
            None,
        )],
    }])
    .await;
    let err = client(&server)
        .run_process(&sandbox(), &spec())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, E2bError::Process { code, message } if code == "invalid_argument" && message.contains("/tmp/missing")),
        "{err:?}"
    );
}

#[tokio::test]
async fn process_stream_cut_short_is_a_protocol_error() {
    let server = FakeServer::start(vec![Reply::Stream {
        parts: vec![(frame(0, &json!({"event": {"start": {"pid": 1}}})), None)],
    }])
    .await;
    let err = client(&server)
        .run_process(&sandbox(), &spec())
        .await
        .unwrap_err();
    assert!(matches!(err, E2bError::Protocol(_)), "{err:?}");
}

#[tokio::test]
async fn process_stream_without_end_event_is_a_protocol_error() {
    let server = FakeServer::start(vec![Reply::Stream {
        parts: vec![(frame(2, &json!({})), None)],
    }])
    .await;
    let err = client(&server)
        .run_process(&sandbox(), &spec())
        .await
        .unwrap_err();
    assert!(matches!(err, E2bError::Protocol(_)), "{err:?}");
}

#[tokio::test]
async fn process_rejects_compressed_and_oversized_frames() {
    let mut huge = vec![0u8];
    huge.extend_from_slice(&u32::MAX.to_be_bytes());
    let server = FakeServer::start(vec![
        Reply::Stream {
            parts: vec![(frame(1, &json!({})), None)],
        },
        Reply::Stream {
            parts: vec![(huge, None)],
        },
    ])
    .await;
    let client = client(&server);
    for _ in 0..2 {
        let err = client.run_process(&sandbox(), &spec()).await.unwrap_err();
        assert!(matches!(err, E2bError::Protocol(_)), "{err:?}");
    }
}

#[tokio::test]
async fn process_pre_stream_errors_are_mapped() {
    let server = FakeServer::start(vec![
        Reply::json(401, &json!({"code": 401, "message": "bad access token"})),
        Reply::json(
            502,
            &json!({"sandboxId": "sbx1", "message": "sandbox not found", "code": 502}),
        ),
        Reply::Full {
            status: 400,
            headers: vec![],
            body: b"missing header".to_vec(),
            delay: Duration::ZERO,
        },
    ])
    .await;
    let client = client(&server);
    assert!(matches!(
        client.run_process(&sandbox(), &spec()).await,
        Err(E2bError::Unauthorized { status: 401 })
    ));
    assert!(matches!(
        client.run_process(&sandbox(), &spec()).await,
        Err(E2bError::NotFound(_))
    ));
    let err = client.run_process(&sandbox(), &spec()).await.unwrap_err();
    assert!(
        matches!(&err, E2bError::Api { status: 400, message, .. } if message == "missing header"),
        "{err:?}"
    );
}

#[tokio::test]
async fn unauthorized_api_errors_never_echo_the_key() {
    let server = FakeServer::start(vec![Reply::error(401, "", &format!("bad key {KEY}"))]).await;
    let err = client(&server).delete("sbx1").await.unwrap_err();
    assert!(matches!(err, E2bError::Unauthorized { status: 401 }));
    assert_no_secrets(&format!("{err} {err:?}"));
}

#[tokio::test]
async fn error_messages_redact_an_echoed_key() {
    let server = FakeServer::start(vec![Reply::error(400, "", &format!("bad key {KEY}"))]).await;
    let err = client(&server).delete("sbx1").await.unwrap_err();
    assert!(matches!(err, E2bError::Api { status: 400, .. }));
    assert_no_secrets(&format!("{err} {err:?}"));
}

#[tokio::test]
async fn transport_errors_carry_no_secrets() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let client = E2bClient::new(
        SecretString::new(KEY),
        E2bConfig {
            api_url: url,
            ..E2bConfig::default()
        },
    )
    .unwrap();
    let err = client.pause("sbx1").await.unwrap_err();
    assert!(matches!(err, E2bError::Unreachable(_)), "{err:?}");
    assert!(err.is_retryable());
    assert_no_secrets(&format!("{err} {err:?}"));
}

#[tokio::test]
async fn api_key_loads_from_the_secret_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"));
    assert!(load_api_key(store.clone()).await.unwrap().is_none());
    store.store(API_KEY_ACCOUNT, "").unwrap();
    assert!(load_api_key(store.clone()).await.unwrap().is_none());
    store.store(API_KEY_ACCOUNT, KEY).unwrap();
    let key = load_api_key(store).await.unwrap().unwrap();
    assert_eq!(key.expose(), KEY);
    assert_no_secrets(&format!("{key:?}"));
}

#[tokio::test]
async fn corrupt_secret_store_is_an_error_without_contents() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("secrets.json");
    std::fs::write(&path, format!("{{not json {KEY}")).unwrap();
    let store = intent_core::FileSecretStore::with_path(path);
    let err = load_api_key(store).await.unwrap_err();
    assert!(matches!(err, E2bError::SecretStore(_)), "{err:?}");
    assert_no_secrets(&format!("{err} {err:?}"));
}

#[test]
fn empty_api_key_is_rejected() {
    assert!(matches!(
        E2bClient::new(SecretString::new(""), E2bConfig::default()),
        Err(E2bError::NotConfigured)
    ));
}

#[tokio::test]
async fn a_key_echoed_across_the_message_cut_is_still_redacted() {
    let message = format!("{}{KEY}{}", "x".repeat(500), "y".repeat(100));
    let server = FakeServer::start(vec![Reply::error(400, "", &message)]).await;
    let err = client(&server).delete("sbx1").await.unwrap_err();
    let text = format!("{err} {err:?}");
    assert_no_secrets(&text);
    assert!(!text.contains(&KEY[..12]), "key prefix leaked: {text}");
}

#[tokio::test]
async fn should_retry_header_decides_retryability() {
    let server = FakeServer::start(vec![
        Reply::error(503, "sandbox_capacity_unavailable", "busy")
            .with_header("should-retry", "false"),
        Reply::error(503, "sandbox_capacity_unavailable", "busy"),
        Reply::error(500, "sandbox_create_failed", "failed").with_header("should-retry", "true"),
    ])
    .await;
    let client = client(&server);
    let refused = client.create(&create_request()).await.unwrap_err();
    assert!(
        matches!(
            refused,
            E2bError::Api {
                status: 503,
                should_retry: Some(false),
                ..
            }
        ),
        "{refused:?}"
    );
    assert!(!refused.is_retryable());
    let unknown = client.create(&create_request()).await.unwrap_err();
    assert!(
        matches!(
            unknown,
            E2bError::Api {
                should_retry: None,
                ..
            }
        ),
        "{unknown:?}"
    );
    assert!(unknown.is_retryable());
    let retry = client.create(&create_request()).await.unwrap_err();
    assert!(retry.is_retryable());
}

#[tokio::test]
async fn rate_limit_without_retry_after_uses_the_default_wait() {
    let server =
        FakeServer::start(vec![Reply::error(429, "", "slow down"), Reply::status(204)]).await;
    let mut client = client(&server);
    client.config.default_retry_wait = Duration::from_millis(300);
    client.pause("sbx1").await.unwrap();
    let reqs = server.requests();
    assert!(reqs[1].at.duration_since(reqs[0].at) >= Duration::from_millis(300));
}

#[tokio::test]
async fn create_in_progress_retry_honors_retry_after() {
    let server = FakeServer::start(vec![
        Reply::error(409, "idempotency_in_progress", "in progress").with_header("retry-after", "1"),
        Reply::json(201, &sandbox_body("sbx1")),
    ])
    .await;
    client(&server).create(&create_request()).await.unwrap();
    let reqs = server.requests();
    assert!(reqs[1].at.duration_since(reqs[0].at) >= Duration::from_secs(1));
}

#[tokio::test]
async fn create_422_without_the_idempotency_code_keeps_the_message() {
    let server = FakeServer::start(vec![Reply::error(422, "", "bad template config")]).await;
    let err = client(&server).create(&create_request()).await.unwrap_err();
    assert!(
        matches!(&err, E2bError::Api { status: 422, message, .. } if message == "bad template config"),
        "{err:?}"
    );
}

#[tokio::test]
async fn envd_messages_redact_the_access_token() {
    let server = FakeServer::start(vec![
        Reply::json(400, &json!({"code": 400, "message": format!("bad token {ENVD_TOKEN}")})),
        Reply::Stream {
            parts: vec![(
                frame(2, &json!({"error": {"code": "unauthenticated", "message": format!("token {ENVD_TOKEN} {}", "z".repeat(2000))}})),
                None,
            )],
        },
    ])
    .await;
    let client = client(&server);
    for _ in 0..2 {
        let err = client.run_process(&sandbox(), &spec()).await.unwrap_err();
        let text = format!("{err} {err:?}");
        assert_no_secrets(&text);
        assert!(text.len() < 1500, "message is not bounded");
    }
}

#[tokio::test]
async fn pty_output_is_reported_as_stdout() {
    let server = FakeServer::start(vec![Reply::Stream {
        parts: vec![(
            [
                frame(0, &json!({"event": {"data": {"pty": b64(b"tty")}}})),
                frame(0, &json!({"event": {"end": {"exited": true}}})),
                frame(2, &json!({})),
            ]
            .concat(),
            None,
        )],
    }])
    .await;
    let output = client(&server)
        .run_process(&sandbox(), &spec())
        .await
        .unwrap();
    assert_eq!(output.stdout, b"tty");
}

#[tokio::test]
async fn default_spec_lets_envd_choose_the_user() {
    let server = FakeServer::start(vec![Reply::Stream {
        parts: vec![(
            [
                frame(0, &json!({"event": {"end": {"exited": true}}})),
                frame(2, &json!({})),
            ]
            .concat(),
            None,
        )],
    }])
    .await;
    client(&server)
        .run_process(&sandbox(), &ProcessSpec::new("/bin/true"))
        .await
        .unwrap();
    assert_eq!(server.requests()[0].header("authorization"), None);
}

#[tokio::test]
async fn process_start_waits_for_headers_only_up_to_the_request_timeout() {
    let server = FakeServer::start(vec![
        Reply::status(200).with_delay(Duration::from_secs(3600))
    ])
    .await;
    let mut client = client(&server);
    client.config.request_timeout = Duration::from_millis(100);
    // Hang guard: an unbounded header wait would wait for the hour-long delay.
    let err = tokio::time::timeout(
        hang_guard(),
        client.start_process(&sandbox(), &ProcessSpec::new("/bin/true")),
    )
    .await
    .expect("the header wait is bounded")
    .unwrap_err();
    assert!(matches!(err, E2bError::Transport(_)), "{err:?}");
    assert!(!err.is_retryable(), "the command may have started");
}

const CONCURRENCY_MESSAGE: &str = "You have reached the maximum number of concurrent E2B sandboxes (20). If you need more, please contact us at 'https://e2b.dev/docs/getting-help'";

#[tokio::test]
async fn concurrency_429_fails_at_once_and_releases_the_create_gate() {
    let server = FakeServer::start(vec![
        Reply::error(429, "", &format!("{CONCURRENCY_MESSAGE} key {KEY}"))
            .with_header("retry-after", "30"),
        Reply::json(201, &sandbox_body("sbx2")),
    ])
    .await;
    let mut client = client(&server);
    client.config.create_spacing = Duration::ZERO;
    let first = create_request();
    let mut second = create_request();
    second.idempotency_key = Some("intent-b1-p2".into());
    let (first, second) = tokio::time::timeout(hang_guard(), async {
        tokio::join!(client.create(&first), client.create(&second))
    })
    .await
    .expect("a concurrency refusal is not retried");
    let err = first.unwrap_err();
    assert!(
        matches!(&err, E2bError::ConcurrencyLimit { message, .. } if message.contains("maximum number of concurrent")),
        "{err:?}"
    );
    assert_eq!(err.quota_limit(), Some(QuotaLimit::Concurrency));
    assert_eq!(QuotaLimit::Concurrency.as_str(), "concurrency");
    assert!(!err.is_retryable());
    assert_no_secrets(&format!("{err} {err:?}"));
    assert_eq!(second.unwrap().sandbox_id, "sbx2");
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn concurrency_429_is_recognized_by_error_code() {
    let server = FakeServer::start(vec![Reply::error(
        429,
        "concurrent_sandbox_limit_reached",
        "limit",
    )])
    .await;
    let err = client(&server).connect("sbx1", 60).await.unwrap_err();
    assert!(
        matches!(&err, E2bError::ConcurrencyLimit { error_code: Some(c), .. } if c == "concurrent_sandbox_limit_reached"),
        "{err:?}"
    );
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn non_quota_errors_have_no_quota_limit() {
    let server = FakeServer::start(vec![Reply::error(404, "", "gone")]).await;
    let err = client(&server).pause("sbx1").await.unwrap_err();
    assert_eq!(err.quota_limit(), None);
}

#[tokio::test]
async fn a_rate_limit_that_mentions_concurrent_requests_is_retried() {
    let server = FakeServer::start(vec![
        Reply::error(429, "", "too many concurrent requests").with_header("retry-after", "0"),
        Reply::status(204),
    ])
    .await;
    client(&server).pause("sbx1").await.unwrap();
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn error_codes_are_scrubbed() {
    let server = FakeServer::start(vec![
        Reply::error(429, &format!("x{KEY}"), "slow down").with_header("retry-after", "0"),
        Reply::error(500, &format!("y{KEY}"), "failed"),
    ])
    .await;
    let mut client = client(&server);
    client.config.max_rate_limit_retries = 0;
    for _ in 0..2 {
        let err = client.delete("sbx1").await.unwrap_err();
        assert_no_secrets(&format!("{err} {err:?}"));
    }
}
