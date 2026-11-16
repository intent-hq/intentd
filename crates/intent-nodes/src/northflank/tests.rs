use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{json, Value};

use super::fake::{FakeServer, Reply, WsSession, WsStep};
use super::*;
use crate::cloud::{
    CloudErrorKind, CloudInstance, CloudProvider, CreateInstance, InstanceState, ListInstances,
    ProcessSpec, QuotaLimit,
};

const TOKEN: &str = "nf-token-secret-0123456789abcdef";
const BACKEND: &str = "b1";
const BACKEND_TAG: &str = "intent-backend-7dc96f776c84";

fn config(server: &FakeServer) -> NorthflankConfig {
    NorthflankConfig {
        api_url: server.url.clone(),
        exec_url: server.ws_url.clone(),
        project_id: "p1".into(),
        request_timeout: Duration::from_secs(10),
        connect_timeout: Duration::from_secs(5),
        ready_timeout: Duration::from_secs(5),
        poll_interval: Duration::from_millis(10),
        fast_poll_window: Duration::from_secs(60),
        slow_poll_interval: Duration::from_millis(10),
        crash_window: Duration::ZERO,
        max_rate_limit_retries: 2,
        max_retry_wait: Duration::from_secs(1),
        exec_drain_timeout: Duration::from_millis(300),
        registry_url: None,
    }
}

fn client(server: &FakeServer) -> NorthflankClient {
    NorthflankClient::new(SecretString::new(TOKEN), config(server)).unwrap()
}

fn assert_no_token(text: &str) {
    assert!(!text.contains(TOKEN), "token leaked into {text}");
}

fn tags() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("intentBackendId".to_string(), BACKEND.to_string()),
        ("agentId".to_string(), "a1".to_string()),
    ])
}

fn create_request() -> CreateInstance {
    CreateInstance {
        image: "ghcr.io/intent-hq/intent-node:0.11.184".into(),
        tags: tags(),
        plan: Some("nf-compute-400".into()),
        disk_gib: Some(5),
        ..CreateInstance::default()
    }
}

fn tag_list(names: &[&str]) -> Reply {
    let tags: Vec<Value> = names
        .iter()
        .map(|n| json!({"id": format!("id-{n}"), "name": n}))
        .collect();
    Reply::json(
        200,
        &json!({"data": {"tags": tags}, "pagination": {"hasNextPage": false, "count": tags.len()}}),
    )
}

/// A deployment service as `GET /projects/p1/services/{id}` returns it:
/// Northflank keeps no annotation, and returns the runtime environment.
fn service(id: &str, status: &str, instances: u32, tags: &BTreeMap<String, String>) -> Value {
    let mut value = legacy_service(id, status, instances);
    value["runtimeEnvironment"] =
        json!({"INTENT_TAGS": serde_json::to_string(tags).unwrap(), "OTHER": "x"});
    value
}

/// A service made before the tags moved to the runtime environment: they
/// went to an annotation, which Northflank drops, so none can be read.
fn legacy_service(id: &str, status: &str, instances: u32) -> Value {
    json!({
        "id": id,
        "name": id,
        "serviceType": "deployment",
        "tags": [NODE_TAG, BACKEND_TAG],
        "description": "Intent cloud machine",
        "createdAt": "2026-11-11T10:00:00.000Z",
        "deployment": {
            "instances": instances,
            "external": {"imagePath": "ghcr.io/intent-hq/intent-node:0.11.184"},
            "metadata": {"labels": {}, "annotations": {}}
        },
        "status": {"deployment": {"status": status, "reason": "DEPLOYING"}}
    })
}

fn containers(statuses: &[&str]) -> Reply {
    let list: Vec<Value> = statuses
        .iter()
        .enumerate()
        .map(|(i, s)| json!({"name": format!("c{i}"), "status": s, "createdAt": 1, "updatedAt": 1}))
        .collect();
    Reply::data(&json!({ "containers": list }))
}

fn instance(id: &str) -> CloudInstance {
    CloudInstance {
        id: id.into(),
        image: "ghcr.io/intent-hq/intent-node:0.11.184".into(),
        access_token: None,
        volume_id: Some(id.into()),
    }
}

// --- configuration and project checks ---

#[test]
fn backend_tag_is_the_hash_prefix() {
    assert_eq!(backend_tag(BACKEND), BACKEND_TAG);
    assert_eq!(backend_tag("live"), "intent-backend-247610f4dedd");
    let tag = backend_tag("anything");
    assert_eq!(tag.len(), "intent-backend-".len() + 12);
    assert!(tag[15..]
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
}

#[test]
fn new_refuses_an_empty_token_and_redacts_it() {
    let err =
        NorthflankClient::new(SecretString::new(""), NorthflankConfig::default()).unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::NotConfigured);
    let client =
        NorthflankClient::new(SecretString::new(TOKEN), NorthflankConfig::default()).unwrap();
    assert_no_token(&format!("{client:?}"));
    assert_eq!(client.kind(), CLOUD_PROVIDER_NORTHFLANK);
    assert!(!client.has_lease());
    assert!(!client.keeps_memory());
    let defaults = NorthflankConfig::default();
    assert_eq!(defaults.api_url, "https://api.northflank.com/v1");
    assert_eq!(defaults.exec_url, "wss://api.northflank.com/v1");
    assert_eq!(defaults.ready_timeout, Duration::from_secs(600));
    assert_eq!(defaults.poll_interval, Duration::from_secs(1));
    assert_eq!(defaults.fast_poll_window, Duration::from_secs(60));
    assert_eq!(defaults.slow_poll_interval, Duration::from_secs(5));
    assert_eq!(defaults.crash_window, Duration::from_secs(6));
}

#[tokio::test]
async fn project_reads_the_region_with_a_bearer_token() {
    let server = FakeServer::start(vec![Reply::data(&json!({
        "id": "p1", "name": "P", "deployment": {"region": "europe-west"}, "services": []
    }))])
    .await;
    let project = client(&server).project().await.unwrap();
    assert_eq!(
        project,
        ProjectInfo {
            id: "p1".into(),
            region: "europe-west".into()
        }
    );
    let [req] = server.requests().try_into().unwrap();
    assert_eq!(req.line(), "GET /projects/p1");
    assert_eq!(
        req.header("authorization"),
        Some(format!("Bearer {TOKEN}").as_str())
    );
    assert_no_token(&req.target);
}

#[tokio::test]
async fn project_maps_refusals() {
    let leaky = format!("bad token {TOKEN}");
    let server = FakeServer::start(vec![
        Reply::error(401, &leaky),
        Reply::error(403, &leaky),
        Reply::error(404, "Project not found"),
    ])
    .await;
    let client = client(&server);
    let unauthorized = client.project().await.unwrap_err();
    assert_eq!(unauthorized.kind(), CloudErrorKind::Unauthorized);
    assert_eq!(unauthorized.status(), Some(401));
    assert_no_token(&unauthorized.to_string());
    assert_no_token(&format!("{unauthorized:?}"));
    let denied = client.project().await.unwrap_err();
    assert_eq!(denied.kind(), CloudErrorKind::PermissionDenied);
    assert_eq!(denied.action(), Some("read-project"));
    assert_eq!(denied.status(), Some(403));
    assert_no_token(&denied.to_string());
    let missing = client.project().await.unwrap_err();
    assert_eq!(missing.kind(), CloudErrorKind::ProjectMissing);
    assert_eq!(missing.status(), Some(404));
}

#[tokio::test]
async fn check_key_reads_the_project() {
    let server = FakeServer::start(vec![Reply::data(
        &json!({"id": "p1", "deployment": {"region": "us-central"}}),
    )])
    .await;
    client(&server).check_key().await.unwrap();
    assert_eq!(server.lines(), ["GET /projects/p1"]);
}

#[tokio::test]
async fn plans_are_listed_with_cpu_and_memory() {
    let server = FakeServer::start(vec![Reply::data(&json!({"plans": [
        {"id": "nf-compute-200", "name": "nf-compute-200", "cpuResource": 2, "ramResource": 4096},
        {"id": "nf-compute-20", "name": "nf-compute-20", "cpuResource": 0.2, "ramResource": 512}
    ]}))])
    .await;
    let plans = client(&server).plans().await.unwrap();
    assert_eq!(
        plans,
        [
            PlanInfo {
                id: "nf-compute-200".into(),
                cpu: 2.0,
                memory_mib: 4096
            },
            PlanInfo {
                id: "nf-compute-20".into(),
                cpu: 0.2,
                memory_mib: 512
            }
        ]
    );
    assert_eq!(server.lines(), ["GET /plans"]);
}

#[tokio::test]
async fn check_services_needs_the_list_permission() {
    let server = FakeServer::start(vec![
        Reply::json(
            200,
            &json!({"data": {"services": []}, "pagination": {"hasNextPage": false, "count": 0}}),
        ),
        Reply::error(403, "Forbidden"),
    ])
    .await;
    let client = client(&server);
    client.check_services().await.unwrap();
    let err = client.check_services().await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::PermissionDenied);
    assert_eq!(err.action(), Some("list-services"));
    assert!(server.lines()[0].starts_with("GET /projects/p1/services"));
}

// --- create ---

#[tokio::test]
async fn create_makes_tags_volume_then_service_and_waits_until_ready() {
    let server = FakeServer::start_with(
        vec![
            tag_list(&["other"]),
            Reply::data(&json!({"id": "intent-node", "name": NODE_TAG})),
            Reply::data(&json!({"id": "b", "name": BACKEND_TAG})),
            Reply::data(&json!({"id": "vol-1", "name": "intent-x"})),
            Reply::data(&json!({"id": "svc-1", "name": "intent-x"})),
            Reply::data(&service("svc-1", "IN_PROGRESS", 1, &tags())),
            Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
            containers(&["TASK_STARTING"]),
            Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
            containers(&["TASK_RUNNING"]),
        ],
        vec![],
    )
    .await;
    let client = client(&server);
    let created = client.create(&create_request()).await.unwrap();
    assert_eq!(created.id, "svc-1");
    assert_eq!(created.volume_id.as_deref(), Some("vol-1"));
    assert_eq!(created.image, "ghcr.io/intent-hq/intent-node:0.11.184");
    assert!(created.access_token.is_none());

    let requests = server.requests();
    let lines: Vec<String> = requests.iter().map(super::fake::Recorded::line).collect();
    assert!(lines[0].starts_with("GET /tags"), "{lines:?}");
    assert_eq!(
        lines[1..],
        [
            "POST /tags",
            "POST /tags",
            "POST /projects/p1/volumes",
            "POST /projects/p1/services/deployment",
            "GET /projects/p1/services/svc-1",
            "GET /projects/p1/services/svc-1",
            "GET /projects/p1/services/svc-1/containers",
            "GET /projects/p1/services/svc-1",
            "GET /projects/p1/services/svc-1/containers",
        ]
    );
    assert!(
        server.frames().is_empty(),
        "the home is set up by the first command, not a command of its own"
    );
    for req in requests
        .iter()
        .filter(|r| !r.target.starts_with("/command-exec"))
    {
        assert_eq!(
            req.header("authorization"),
            Some(format!("Bearer {TOKEN}").as_str())
        );
        assert_no_token(&req.target);
        assert_no_token(&String::from_utf8_lossy(&req.body));
    }
    assert_eq!(requests[1].json(), json!({"name": NODE_TAG}));
    assert_eq!(requests[2].json(), json!({"name": BACKEND_TAG}));

    let volume = requests[3].json();
    let name = volume["name"].as_str().unwrap().to_string();
    assert!(name.starts_with("intent-") && name.len() == 19, "{name}");
    assert!(name[7..]
        .bytes()
        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    assert_eq!(
        volume,
        json!({
            "name": name,
            "tags": [NODE_TAG, BACKEND_TAG],
            "mounts": [{"containerMountPath": "/home/user"}],
            "spec": {"accessMode": "ReadWriteOnce", "storageSize": 5 * 1024}
        })
    );

    let svc = requests[4].json();
    assert_eq!(svc["name"], json!(name));
    assert_eq!(svc["tags"], json!([NODE_TAG, BACKEND_TAG]));
    assert_eq!(svc["billing"], json!({"deploymentPlan": "nf-compute-400"}));
    assert_eq!(svc["deployment"]["instances"], json!(1));
    assert_eq!(
        svc["deployment"]["external"],
        json!({"imagePath": "ghcr.io/intent-hq/intent-node:0.11.184"})
    );
    let stored: BTreeMap<String, String> =
        serde_json::from_str(svc["runtimeEnvironment"]["INTENT_TAGS"].as_str().unwrap()).unwrap();
    assert_eq!(stored, tags());
    assert_eq!(
        svc["deployment"].get("metadata"),
        None,
        "Northflank drops annotations"
    );
    assert_eq!(svc["ports"], json!([]));
    assert_eq!(svc["createOptions"], json!({"volumesToAttach": ["vol-1"]}));
}

#[tokio::test]
async fn create_reuses_existing_tags_and_defaults_the_disk() {
    let server = FakeServer::start_with(
        vec![
            tag_list(&[NODE_TAG, BACKEND_TAG]),
            Reply::data(&json!({"id": "vol-1"})),
            Reply::data(&json!({"id": "svc-1"})),
            Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
            containers(&["TASK_RUNNING"]),
        ],
        vec![],
    )
    .await;
    let mut request = create_request();
    request.disk_gib = None;
    client(&server).create(&request).await.unwrap();
    let requests = server.requests();
    assert_eq!(requests[1].line(), "POST /projects/p1/volumes");
    assert_eq!(requests[1].json()["spec"]["storageSize"], json!(20 * 1024));
}

#[tokio::test]
async fn create_follows_tag_pages() {
    let server = FakeServer::start_with(
        vec![
            Reply::json(
                200,
                &json!({"data": {"tags": [{"id": "a", "name": NODE_TAG}]},
            "pagination": {"hasNextPage": true, "cursor": "c2", "count": 1}}),
            ),
            tag_list(&[BACKEND_TAG]),
            Reply::data(&json!({"id": "vol-1"})),
            Reply::data(&json!({"id": "svc-1"})),
            Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
            containers(&["TASK_RUNNING"]),
        ],
        vec![],
    )
    .await;
    client(&server).create(&create_request()).await.unwrap();
    let lines = server.lines();
    assert!(
        lines[1].starts_with("GET /tags") && lines[1].contains("cursor=c2"),
        "{lines:?}"
    );
    assert_eq!(lines[2], "POST /projects/p1/volumes");
}

#[tokio::test]
async fn create_checks_its_input_before_any_request() {
    let server = FakeServer::start(vec![]).await;
    let client = client(&server);
    let mut no_plan = create_request();
    no_plan.plan = None;
    let mut no_backend = create_request();
    no_backend.tags.remove("intentBackendId");
    let mut no_tag = create_request();
    no_tag.image = "ghcr.io/intent-hq/intent-node".into();
    let mut zero_disk = create_request();
    zero_disk.disk_gib = Some(0);
    for request in [no_plan, no_backend, no_tag, zero_disk] {
        let err = client.create(&request).await.unwrap_err();
        assert_eq!(err.kind(), CloudErrorKind::InvalidArgument, "{err}");
    }
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn create_without_the_tag_permission_is_denied() {
    let server = FakeServer::start(vec![tag_list(&[]), Reply::error(403, "Forbidden")]).await;
    let err = client(&server).create(&create_request()).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::PermissionDenied);
    assert_eq!(err.action(), Some("create-tag"));
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn create_without_the_volume_permission_is_denied() {
    let server = FakeServer::start(vec![
        tag_list(&[NODE_TAG, BACKEND_TAG]),
        Reply::error(403, "Forbidden"),
    ])
    .await;
    let err = client(&server).create(&create_request()).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::PermissionDenied);
    assert_eq!(err.action(), Some("create-volume"));
}

#[tokio::test]
async fn a_failed_service_create_deletes_the_volume() {
    let server = FakeServer::start(vec![
        tag_list(&[NODE_TAG, BACKEND_TAG]),
        Reply::data(&json!({"id": "vol-1"})),
        Reply::error(403, "Forbidden"),
        Reply::data(&json!({})),
    ])
    .await;
    let err = client(&server).create(&create_request()).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::PermissionDenied);
    assert_eq!(err.action(), Some("create-service"));
    let lines = server.lines();
    assert_eq!(lines[2], "POST /projects/p1/services/deployment");
    assert_eq!(lines[3], "DELETE /projects/p1/volumes/vol-1");
    assert_eq!(lines.len(), 4);
}

#[tokio::test]
async fn a_failed_deployment_is_an_error_and_removes_the_machine() {
    let server = FakeServer::start(vec![
        tag_list(&[NODE_TAG, BACKEND_TAG]),
        Reply::data(&json!({"id": "vol-1"})),
        Reply::data(&json!({"id": "svc-1"})),
        Reply::data(&service("svc-1", "FAILED", 1, &tags())),
        Reply::data(&json!({})),
        Reply::error(404, "volume not found"),
    ])
    .await;
    let err = client(&server).create(&create_request()).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Other);
    assert!(err.to_string().contains("deployment failed"), "{err}");
    let lines = server.lines();
    assert_eq!(
        lines[4],
        "DELETE /projects/p1/services/svc-1?delete_child_objects=true"
    );
    assert_eq!(lines[5], "DELETE /projects/p1/volumes/vol-1");
}

/// Northflank allows a token 1000 requests an hour, shared by every
/// machine of the entry: a long wait polls every second only at first.
#[test]
fn the_readiness_poll_slows_down_after_the_fast_window() {
    let client =
        NorthflankClient::new(SecretString::new(TOKEN), NorthflankConfig::default()).unwrap();
    assert_eq!(client.poll_interval(Duration::ZERO), Duration::from_secs(1));
    assert_eq!(
        client.poll_interval(Duration::from_secs(59)),
        Duration::from_secs(1)
    );
    assert_eq!(
        client.poll_interval(Duration::from_secs(60)),
        Duration::from_secs(5)
    );
    assert_eq!(
        client.poll_interval(Duration::from_secs(500)),
        Duration::from_secs(5)
    );
}

#[tokio::test]
async fn exited_containers_are_a_crash_only_once_the_crash_window_has_passed() {
    let server = FakeServer::start(vec![Reply::data(&service(
        "svc-1",
        "COMPLETED",
        1,
        &tags(),
    ))])
    .await;
    for _ in 0..200 {
        server.push(Reply::data(&service("svc-1", "COMPLETED", 1, &tags())));
        server.push(containers(&["TASK_FAILED"]));
    }
    let mut config = config(&server);
    config.ready_timeout = Duration::from_millis(300);
    config.crash_window = Duration::from_secs(3600);
    let client = NorthflankClient::new(SecretString::new(TOKEN), config).unwrap();
    let err = client.connect("svc-1", 600).await.unwrap_err();
    assert!(
        err.to_string().contains("not ready"),
        "a container that exited within the crash window may still start: {err}"
    );
}

#[tokio::test]
async fn a_crash_looping_container_is_an_error() {
    let mut replies = vec![
        tag_list(&[NODE_TAG, BACKEND_TAG]),
        Reply::data(&json!({"id": "vol-1"})),
        Reply::data(&json!({"id": "svc-1"})),
    ];
    for _ in 0..3 {
        replies.push(Reply::data(&service("svc-1", "COMPLETED", 1, &tags())));
        replies.push(containers(&["TASK_FAILED"]));
    }
    replies.push(Reply::data(&json!({})));
    replies.push(Reply::data(&json!({})));
    let server = FakeServer::start(replies).await;
    let err = client(&server).create(&create_request()).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Other);
    assert!(err.to_string().contains("keeps exiting"), "{err}");
}

#[tokio::test]
async fn a_deployment_that_never_completes_times_out() {
    let server = FakeServer::start(vec![
        tag_list(&[NODE_TAG, BACKEND_TAG]),
        Reply::data(&json!({"id": "vol-1"})),
        Reply::data(&json!({"id": "svc-1"})),
    ])
    .await;
    for _ in 0..400 {
        server.push(Reply::data(&service("svc-1", "IN_PROGRESS", 1, &tags())));
    }
    let mut config = config(&server);
    config.ready_timeout = Duration::from_millis(200);
    let client = NorthflankClient::new(SecretString::new(TOKEN), config).unwrap();
    let err = client.create(&create_request()).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Other);
    assert!(err.to_string().contains("not ready"), "{err}");
}

#[tokio::test]
async fn tags_that_cannot_be_stored_are_refused() {
    let server = FakeServer::start(vec![]).await;
    let mut request = create_request();
    request
        .tags
        .insert("big".into(), "x".repeat(super::client::MAX_TAGS_JSON + 1));
    let err = client(&server).create(&request).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::InvalidArgument);
    assert!(server.requests().is_empty());
}

// --- connect, pause, keep_alive, delete ---

#[tokio::test]
async fn connect_to_a_running_service_waits_and_does_not_resume() {
    let server = FakeServer::start_with(
        vec![
            Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
            Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
            containers(&["TASK_RUNNING"]),
        ],
        vec![],
    )
    .await;
    let connected = client(&server).connect("svc-1", 600).await.unwrap();
    assert!(!connected.resumed);
    assert_eq!(connected.instance.id, "svc-1");
    assert_eq!(
        connected.instance.image,
        "ghcr.io/intent-hq/intent-node:0.11.184"
    );
    assert!(!server.lines().iter().any(|l| l.contains("resume")));
    assert!(server.frames().is_empty(), "connect runs no command");
}

#[tokio::test]
async fn connect_resumes_a_paused_service() {
    let server = FakeServer::start_with(
        vec![
            Reply::data(&service("svc-1", "COMPLETED", 0, &tags())),
            Reply::data(&json!({})),
            Reply::data(&service("svc-1", "IN_PROGRESS", 1, &tags())),
            Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
            containers(&[]),
            Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
            containers(&["TASK_RUNNING"]),
        ],
        vec![],
    )
    .await;
    let connected = client(&server).connect("svc-1", 600).await.unwrap();
    assert!(connected.resumed);
    let requests = server.requests();
    assert_eq!(
        requests[1].line(),
        "POST /projects/p1/services/svc-1/resume"
    );
    assert_eq!(requests[1].json(), json!({"instances": 1}));
    assert!(server.frames().is_empty(), "connect runs no command");
}

#[tokio::test]
async fn connect_maps_a_missing_service_and_a_refused_resume() {
    let server = FakeServer::start(vec![
        Reply::error(404, "Service not found"),
        Reply::data(&service("svc-1", "COMPLETED", 0, &tags())),
        Reply::error(403, "Forbidden"),
    ])
    .await;
    let client = client(&server);
    let missing = client.connect("svc-1", 600).await.unwrap_err();
    assert!(missing.is_not_found());
    let denied = client.connect("svc-1", 600).await.unwrap_err();
    assert_eq!(denied.kind(), CloudErrorKind::PermissionDenied);
    assert_eq!(denied.action(), Some("resume"));
}

#[tokio::test]
async fn ids_are_checked_before_any_request() {
    let server = FakeServer::start(vec![]).await;
    let client = client(&server);
    for id in ["", "../x", "a/b", "a?b"] {
        assert_eq!(
            client.connect(id, 600).await.unwrap_err().kind(),
            CloudErrorKind::InvalidArgument
        );
        assert_eq!(
            client.pause(id).await.unwrap_err().kind(),
            CloudErrorKind::InvalidArgument
        );
        assert_eq!(
            client.delete(id).await.unwrap_err().kind(),
            CloudErrorKind::InvalidArgument
        );
    }
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn pause_posts_and_maps_refusals() {
    let server = FakeServer::start(vec![
        Reply::data(&json!({})),
        Reply::error(404, "not found"),
        Reply::error(409, "Service is already paused"),
        Reply::error(403, "Forbidden"),
    ])
    .await;
    let client = client(&server);
    client.pause("svc-1").await.unwrap();
    assert!(client.pause("svc-1").await.unwrap_err().is_not_found());
    let conflict = client.pause("svc-1").await.unwrap_err();
    assert!(conflict.is_conflict());
    assert!(conflict.is_retryable());
    let denied = client.pause("svc-1").await.unwrap_err();
    assert_eq!(denied.action(), Some("pause"));
    assert_eq!(server.lines()[0], "POST /projects/p1/services/svc-1/pause");
}

#[tokio::test]
async fn keep_alive_makes_no_call() {
    let server = FakeServer::start(vec![]).await;
    client(&server).keep_alive("svc-1", 600).await.unwrap();
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn delete_removes_the_service_with_child_objects_then_the_volume() {
    let server = FakeServer::start(vec![
        Reply::data(&json!({})),
        Reply::error(404, "volume not found"),
        Reply::error(404, "service not found"),
        Reply::error(404, "volume not found"),
    ])
    .await;
    let client = client(&server);
    assert!(client.delete("svc-1").await.unwrap());
    assert!(!client.delete("svc-1").await.unwrap());
    assert_eq!(
        server.lines(),
        [
            "DELETE /projects/p1/services/svc-1?delete_child_objects=true",
            "DELETE /projects/p1/volumes/svc-1",
            "DELETE /projects/p1/services/svc-1?delete_child_objects=true",
            "DELETE /projects/p1/volumes/svc-1",
        ]
    );
}

#[tokio::test]
async fn a_volume_still_attached_to_the_deleted_service_is_left_to_the_reaper() {
    // Northflank deletes a service in the background: its volume is still
    // attached for a while, and the reaper deletes it as an orphan later.
    let server = FakeServer::start(vec![
        Reply::data(&json!({})),
        Reply::error(409, "Volume must be detached before it can be deleted"),
        Reply::error(404, "service not found"),
        Reply::error(409, "Volume must be detached before it can be deleted"),
    ])
    .await;
    let client = client(&server);
    assert!(client.delete("svc-1").await.unwrap());
    let err = client.delete("svc-1").await.unwrap_err();
    assert_eq!(
        err.kind(),
        CloudErrorKind::Conflict,
        "without a service delete, a refused volume delete is an error"
    );
}

#[tokio::test]
async fn delete_of_a_volume_id_deletes_only_the_volume() {
    let server = FakeServer::start(vec![Reply::data(&json!({})), Reply::error(404, "gone")]).await;
    let client = client(&server);
    assert!(client.delete("volume:vol-9").await.unwrap());
    assert!(!client.delete("volume:vol-9").await.unwrap());
    assert_eq!(
        server.lines(),
        [
            "DELETE /projects/p1/volumes/vol-9",
            "DELETE /projects/p1/volumes/vol-9"
        ]
    );
}

#[tokio::test]
async fn delete_refusal_keeps_the_action() {
    let server = FakeServer::start(vec![Reply::error(403, "Forbidden")]).await;
    let err = client(&server).delete("svc-1").await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::PermissionDenied);
    assert_eq!(err.action(), Some("delete-service"));
}

// --- list ---

fn listed_service(id: &str, tags: &[&str]) -> Value {
    json!({"id": id, "name": id, "serviceType": "deployment", "tags": tags,
        "status": {"deployment": {"status": "COMPLETED", "reason": "DEPLOYING"}}})
}

#[tokio::test]
async fn list_filters_by_backend_tag_and_stored_tags_and_finds_orphan_volumes() {
    let mine = tags();
    let other_agent = BTreeMap::from([
        ("intentBackendId".to_string(), BACKEND.to_string()),
        ("agentId".to_string(), "a2".to_string()),
    ]);
    let server = FakeServer::start(vec![
        // Services, two pages.
        Reply::json(200, &json!({"data": {"services": [
            listed_service("svc-1", &[NODE_TAG, BACKEND_TAG]),
            listed_service("foreign", &[NODE_TAG, "intent-backend-000000000000"]),
        ]}, "pagination": {"hasNextPage": true, "cursor": "abcdefabcdefabcdefabcdef", "count": 2}})),
        Reply::json(200, &json!({"data": {"services": [
            listed_service("svc-2", &[NODE_TAG, BACKEND_TAG]),
            listed_service("svc-3", &[NODE_TAG, BACKEND_TAG]),
            listed_service("svc-old", &[NODE_TAG, BACKEND_TAG]),
        ]}, "pagination": {"hasNextPage": false, "count": 3}})),
        Reply::data(&service("svc-1", "COMPLETED", 1, &mine)),
        Reply::data(&service("svc-2", "COMPLETED", 0, &other_agent)),
        Reply::error(404, "deleted meanwhile"),
        Reply::data(&legacy_service("svc-old", "COMPLETED", 0)),
        // Volumes.
        Reply::json(200, &json!({"data": [
            {"id": "svc-1", "name": "svc-1", "tags": [NODE_TAG, BACKEND_TAG],
             "attachedObjects": [{"id": "svc-1", "type": "service"}], "createdAt": "2026-11-11T09:00:00Z"},
            {"id": "vol-orphan", "name": "vol-orphan", "tags": [NODE_TAG, BACKEND_TAG],
             "attachedObjects": [], "createdAt": "2026-11-11T08:00:00Z"},
            {"id": "vol-foreign", "name": "vol-foreign", "tags": [NODE_TAG],
             "attachedObjects": []},
        ], "pagination": {"hasNextPage": false, "count": 3}})),
    ])
    .await;
    let client = client(&server);
    let found = client
        .list(&ListInstances {
            tags: BTreeMap::from([("intentBackendId".to_string(), BACKEND.to_string())]),
            states: vec![],
        })
        .await
        .unwrap();
    let summary: Vec<(String, InstanceState)> =
        found.iter().map(|i| (i.id.clone(), i.state)).collect();
    assert_eq!(
        summary,
        [
            ("svc-1".to_string(), InstanceState::Running),
            ("svc-2".to_string(), InstanceState::Paused),
            ("svc-old".to_string(), InstanceState::Paused),
            ("volume:vol-orphan".to_string(), InstanceState::Paused),
        ]
    );
    let only_backend = BTreeMap::from([("intentBackendId".to_string(), BACKEND.to_string())]);
    assert_eq!(found[0].tags, mine);
    assert_eq!(found[0].started_at, "2026-11-11T10:00:00.000Z");
    assert_eq!(found[1].tags, other_agent);
    assert_eq!(
        found[2].tags, only_backend,
        "a service with no readable tags is the backend's by its team tag"
    );
    assert_eq!(found[2].started_at, "2026-11-11T10:00:00.000Z");
    assert_eq!(found[3].tags, only_backend);
    assert_eq!(found[3].started_at, "2026-11-11T08:00:00Z");
    let lines = server.lines();
    assert!(
        lines[0].starts_with("GET /projects/p1/services?"),
        "{lines:?}"
    );
    assert!(
        lines[1].contains("cursor=abcdefabcdefabcdefabcdef"),
        "{lines:?}"
    );
    assert_eq!(lines[2], "GET /projects/p1/services/svc-1");
    assert!(
        lines[6].starts_with("GET /projects/p1/volumes"),
        "{lines:?}"
    );
}

#[tokio::test]
async fn list_by_more_tags_matches_every_pair_and_skips_volumes() {
    let server = FakeServer::start(vec![
        Reply::json(
            200,
            &json!({"data": {"services": [
            listed_service("svc-1", &[BACKEND_TAG]),
            listed_service("svc-2", &[BACKEND_TAG]),
            listed_service("svc-old", &[BACKEND_TAG]),
        ]}, "pagination": {"hasNextPage": false, "count": 3}}),
        ),
        Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
        Reply::data(&service(
            "svc-2",
            "COMPLETED",
            1,
            &BTreeMap::from([("intentBackendId".to_string(), BACKEND.to_string())]),
        )),
        Reply::data(&legacy_service("svc-old", "COMPLETED", 1)),
    ])
    .await;
    let found = client(&server)
        .list(&ListInstances {
            tags: tags(),
            states: vec![InstanceState::Running, InstanceState::Paused],
        })
        .await
        .unwrap();
    assert_eq!(
        found.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["svc-1"],
        "only the backend is known of a service with no readable tags"
    );
    assert_eq!(server.requests().len(), 4);
}

#[tokio::test]
async fn list_respects_the_states() {
    let server = FakeServer::start(vec![
        Reply::json(
            200,
            &json!({"data": {"services": [
            listed_service("svc-1", &[BACKEND_TAG]),
            listed_service("svc-2", &[BACKEND_TAG]),
        ]}, "pagination": {"hasNextPage": false, "count": 2}}),
        ),
        Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
        Reply::data(&service("svc-2", "COMPLETED", 0, &tags())),
    ])
    .await;
    let found = client(&server)
        .list(&ListInstances {
            tags: BTreeMap::from([("intentBackendId".to_string(), BACKEND.to_string())]),
            states: vec![InstanceState::Running],
        })
        .await
        .unwrap();
    assert_eq!(
        found.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["svc-1"]
    );
    // Only running wanted: no volume listing.
    assert_eq!(server.requests().len(), 3);
}

#[tokio::test]
async fn list_needs_the_backend_id() {
    let server = FakeServer::start(vec![]).await;
    let err = client(&server)
        .list(&ListInstances::default())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::InvalidArgument);
}

// --- error mapping ---

#[tokio::test]
async fn rate_limits_are_retried_then_reported() {
    let server = FakeServer::start(vec![
        Reply::error(429, "Too many requests").with_header("retry-after", "0"),
        Reply::data(&json!({"id": "p1", "deployment": {"region": "r"}})),
        Reply::error(429, "Too many requests").with_header("retry-after", "0"),
        Reply::error(429, "Too many requests").with_header("retry-after", "0"),
        Reply::error(429, "Too many requests").with_header("retry-after", "0"),
        Reply::error(429, "Too many requests").with_header("retry-after", "3600"),
    ])
    .await;
    let client = client(&server);
    client.project().await.unwrap();
    let exhausted = client.project().await.unwrap_err();
    assert_eq!(exhausted.kind(), CloudErrorKind::Quota(QuotaLimit::Rate));
    assert_eq!(exhausted.status(), Some(429));
    assert!(exhausted.is_retryable());
    // A wait longer than max_retry_wait is not slept: it is reported at once.
    let long = client.project().await.unwrap_err();
    assert_eq!(long.quota_limit(), Some(QuotaLimit::Rate));
    assert_eq!(long.retry_after(), Some(Duration::from_secs(3600)));
    assert_eq!(server.requests().len(), 6);
}

#[tokio::test]
async fn rate_limit_reset_header_is_used_without_retry_after() {
    let server = FakeServer::start(vec![
        Reply::error(429, "Too many requests").with_header("x-ratelimit-reset", "1200")
    ])
    .await;
    let err = client(&server).project().await.unwrap_err();
    assert_eq!(err.retry_after(), Some(Duration::from_secs(1200)));
}

#[tokio::test]
async fn plan_and_quota_refusals_are_plan_quota() {
    let server = FakeServer::start(vec![
        tag_list(&[NODE_TAG, BACKEND_TAG]),
        Reply::data(&json!({"id": "vol-1"})),
        Reply::error(400, "Project resource quota exceeded: cpu limit reached"),
        Reply::data(&json!({})),
    ])
    .await;
    let err = client(&server).create(&create_request()).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Quota(QuotaLimit::Plan));
    assert_eq!(err.status(), Some(400));
    assert!(!err.is_retryable());
}

#[tokio::test]
async fn server_errors_are_retryable_api_errors_with_status() {
    let leaky = format!("upstream exploded near {TOKEN}");
    let server = FakeServer::start(vec![
        Reply::error(503, &leaky),
        Reply::error(400, "Invalid request"),
        Reply::json(500, &json!("not an envelope")),
    ])
    .await;
    let client = client(&server);
    let err = client.project().await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Api);
    assert_eq!(err.status(), Some(503));
    assert!(err.is_retryable());
    assert_no_token(&err.to_string());
    let bad = client.project().await.unwrap_err();
    assert_eq!(bad.kind(), CloudErrorKind::Api);
    assert!(!bad.is_retryable());
    assert!(bad.to_string().contains("Invalid request"), "{bad}");
    let odd = client.project().await.unwrap_err();
    assert_eq!(odd.status(), Some(500));
}

#[tokio::test]
async fn an_unreachable_api_is_a_retryable_error() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let client = NorthflankClient::new(
        SecretString::new(TOKEN),
        NorthflankConfig {
            api_url: url,
            project_id: "p1".into(),
            ..NorthflankConfig::default()
        },
    )
    .unwrap();
    let err = client.project().await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Other);
    assert!(err.is_retryable());
    assert!(err.to_string().contains("unreachable"), "{err}");
    assert_no_token(&err.to_string());
}

// --- command execution ---

fn init_ok() -> WsStep {
    WsStep::Send(json!({"type": "init", "data": {"auth": "successful"}}))
}

/// Output as real Northflank sends it for this command's `init`.
fn out(kind: &str, text: &str) -> WsStep {
    WsStep::Output {
        stderr: kind == "stdErr",
        text: text.into(),
    }
}

fn completion(code: i32, status: &str) -> WsStep {
    WsStep::Send(json!({"type": "completion", "data": {"exitCode": code, "status": status}}))
}

async fn exec_server(steps: Vec<WsStep>) -> FakeServer {
    FakeServer::start_with(vec![], vec![WsSession::Steps(steps)]).await
}

/// A connected client: `svc-1` runs, and its first command sets up its home.
async fn connected(sessions: Vec<WsSession>) -> (FakeServer, NorthflankClient) {
    let server = FakeServer::start_with(
        vec![
            Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
            Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
            containers(&["TASK_RUNNING"]),
        ],
        sessions,
    )
    .await;
    let client = client(&server);
    client.connect("svc-1", 600).await.unwrap();
    (server, client)
}

fn session(steps: Vec<WsStep>) -> WsSession {
    let mut all = vec![WsStep::Receive, init_ok()];
    all.extend(steps);
    all.push(WsStep::Close);
    WsSession::Steps(all)
}

/// The user and argv of each command, in order.
fn contexts(server: &FakeServer) -> Vec<(String, Vec<String>)> {
    server
        .frames()
        .iter()
        .map(|f| {
            let context = serde_json::from_str::<Value>(f).unwrap()["data"]["context"].clone();
            (
                context["user"].as_str().unwrap().to_string(),
                serde_json::from_value(context["command"].clone()).unwrap(),
            )
        })
        .collect()
}

fn shell(script: &str) -> ProcessSpec {
    let mut spec = ProcessSpec::new("/bin/sh");
    spec.args = vec!["-c".into(), script.into()];
    spec
}

#[tokio::test]
async fn exec_collects_output_and_the_exit_code() {
    let server = exec_server(vec![
        WsStep::Receive,
        init_ok(),
        out("stdOut", "out\n"),
        out("stdErr", "err\n"),
        completion(3, "Failure"),
        WsStep::Close,
    ])
    .await;
    let output = client(&server)
        .run_command(&instance("svc-1"), &shell("echo out; echo err >&2; exit 3"))
        .await
        .unwrap();
    assert_eq!(output.stdout, b"out\n");
    assert_eq!(output.stderr, b"err\n");
    assert_eq!(output.exit.exit_code, 3);
    assert!(output.exit.exited);
    assert_eq!(output.exit.status, "Failure");
    assert!(output.exit.error.is_some());
    assert!(!output.success());
    assert!(!output.truncated);

    let [req] = server.requests().try_into().unwrap();
    assert_eq!(req.target, "/command-exec/projects/p1/services/svc-1");
    assert_eq!(req.header("authorization"), None);
    assert_no_token(&req.target);
    let [init] = server.frames().try_into().unwrap();
    let init: Value = serde_json::from_str(&init).unwrap();
    assert_eq!(
        init,
        json!({"type": "init", "data": {
            "auth": {"type": "apiToken", "apiToken": TOKEN},
            "context": {
                "command": ["env", "HOME=/home/user", "/bin/sh", "-c", "echo out; echo err >&2; exit 3"],
                "shell": "none",
                "user": "user",
                "tty": false
            }
        }})
    );
}

#[tokio::test]
async fn exec_success_has_no_error() {
    let server = exec_server(vec![
        WsStep::Receive,
        init_ok(),
        out("stdOut", "hi\n"),
        completion(0, "Success"),
        WsStep::Close,
    ])
    .await;
    let output = client(&server)
        .run_command(&instance("svc-1"), &ProcessSpec::new("/bin/echo"))
        .await
        .unwrap();
    assert!(output.success());
    assert_eq!(output.exit.error, None);
}

#[tokio::test]
async fn exec_builds_the_argv_with_cwd_env_home_and_user() {
    let server = exec_server(vec![
        WsStep::Receive,
        init_ok(),
        completion(0, "Success"),
        WsStep::Close,
    ])
    .await;
    let mut spec = ProcessSpec::new("intentd");
    spec.args = vec!["executor".into(), "mint".into()];
    spec.cwd = Some("/home/user/work".into());
    spec.user = Some("user".into());
    spec.envs.insert("HOME".into(), SecretString::new("/root"));
    spec.envs
        .insert("SECRET".into(), SecretString::new("s3cr3t"));
    client(&server)
        .run_command(&instance("svc-1"), &spec)
        .await
        .unwrap();
    let init: Value = serde_json::from_str(&server.frames()[0]).unwrap();
    assert_eq!(
        init["data"]["context"],
        json!({
            "command": ["env", "-C", "/home/user/work", "HOME=/root", "SECRET=s3cr3t", "intentd", "executor", "mint"],
            "shell": "none",
            "user": "user",
            "tty": false
        })
    );
    assert!(!format!("{spec:?}").contains("s3cr3t"));
}

#[tokio::test]
async fn exec_runs_as_the_unprivileged_user_unless_the_spec_names_one() {
    let session = || {
        WsSession::Steps(vec![
            WsStep::Receive,
            init_ok(),
            completion(0, "Success"),
            WsStep::Close,
        ])
    };
    let server = FakeServer::start_with(vec![], vec![session(), session()]).await;
    let client = client(&server);
    client
        .run_command(&instance("svc-1"), &ProcessSpec::new("id"))
        .await
        .unwrap();
    let mut root = ProcessSpec::new("id");
    root.user = Some("root".into());
    client.run_command(&instance("svc-1"), &root).await.unwrap();
    let users: Vec<Value> = server
        .frames()
        .iter()
        .map(|f| serde_json::from_str::<Value>(f).unwrap()["data"]["context"]["user"].clone())
        .collect();
    assert_eq!(users, [json!("user"), json!("root")]);
}

#[tokio::test]
async fn the_first_command_after_a_connect_sets_up_the_home_in_the_same_exec() {
    let ready = format!("{}\n", super::provider::HOME_READY);
    let (server, client) = connected(vec![
        session(vec![
            out("stdErr", &ready),
            out("stdOut", "started\n"),
            out("stdErr", "warn\n"),
            completion(0, "Success"),
        ]),
        session(vec![completion(0, "Success")]),
    ])
    .await;
    let output = client
        .run_command(&instance("svc-1"), &shell("echo started"))
        .await
        .unwrap();
    assert!(output.success());
    assert_eq!(output.stdout, b"started\n");
    assert_eq!(output.stderr, b"warn\n", "the setup's marker is not output");
    client
        .run_command(&instance("svc-1"), &shell("echo mint"))
        .await
        .unwrap();

    let [(first_user, first), (second_user, second)] = contexts(&server).try_into().unwrap();
    assert_eq!(first_user, "root");
    let script = super::provider::setup_then_run_script(RUN_AS);
    let mut wrapped: Vec<String> = [
        "env",
        "HOME=/home/user",
        "/bin/sh",
        "-c",
        &script,
        "intent-setup",
    ]
    .map(String::from)
    .to_vec();
    wrapped.extend(super::exec::argv(&shell("echo started")));
    assert_eq!(first, wrapped);
    assert!(script.contains("exec setpriv"), "{script}");
    assert_eq!(second_user, "user", "the home is set up once per connect");
    assert_eq!(second, super::exec::argv(&shell("echo mint")));
}

/// A terminal stream (`"tty": false` ignored) merges the marker into
/// stdout with a CRLF: the setup still counts as done.
#[tokio::test]
async fn the_home_setup_marker_is_found_in_a_terminal_stream() {
    let ready = format!("{}\r\n", super::provider::HOME_READY);
    let (server, client) = connected(vec![
        session(vec![
            WsStep::Send(json!({"type": "stdOut", "data": format!("{ready}started\r\n")})),
            completion(0, "Success"),
        ]),
        session(vec![completion(0, "Success")]),
    ])
    .await;
    let output = client
        .run_command(&instance("svc-1"), &shell("echo started"))
        .await
        .unwrap();
    assert!(output.success());
    assert_eq!(
        output.stdout, b"started\r\n",
        "the setup's marker is not output"
    );
    client
        .run_command(&instance("svc-1"), &shell("echo mint"))
        .await
        .unwrap();
    let users: Vec<String> = contexts(&server).into_iter().map(|(u, _)| u).collect();
    assert_eq!(
        users,
        ["root", "user"],
        "the home is set up once per connect"
    );
}

#[tokio::test]
async fn a_root_command_after_a_connect_leaves_the_setup_to_the_next_user_command() {
    let ready = format!("{}\n", super::provider::HOME_READY);
    let (server, client) = connected(vec![
        session(vec![completion(0, "Success")]),
        session(vec![out("stdErr", &ready), completion(0, "Success")]),
    ])
    .await;
    let mut root = ProcessSpec::new("id");
    root.user = Some("root".into());
    client.run_command(&instance("svc-1"), &root).await.unwrap();
    client
        .run_command(&instance("svc-1"), &ProcessSpec::new("id"))
        .await
        .unwrap();
    let ran = contexts(&server);
    assert_eq!(ran[0].1, super::exec::argv(&root));
    assert_eq!(ran[1].0, "root");
    assert!(
        ran[1].1.iter().any(|a| a.contains("exec setpriv")),
        "{:?}",
        ran[1]
    );
}

#[tokio::test]
async fn a_failed_setup_is_an_error_and_the_next_command_sets_up_again() {
    let ready = format!("{}\n", super::provider::HOME_READY);
    let (server, client) = connected(vec![
        session(vec![
            out("stdErr", "chown: Operation not permitted\n"),
            completion(1, "Failure"),
        ]),
        session(vec![out("stdErr", &ready), completion(0, "Success")]),
    ])
    .await;
    let err = client
        .run_command(&instance("svc-1"), &shell("echo started"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Other);
    assert!(
        err.to_string().contains("home folder")
            && err.to_string().contains("Operation not permitted"),
        "{err}"
    );
    client
        .run_command(&instance("svc-1"), &shell("echo started"))
        .await
        .unwrap();
    let ran = contexts(&server);
    assert!(ran.iter().all(|(user, _)| user == "root"), "{ran:?}");
    assert!(
        !server.lines().iter().any(|l| l.starts_with("DELETE")),
        "a command never deletes the machine"
    );
}

#[tokio::test]
async fn an_image_without_setpriv_sets_up_then_runs_the_command_as_the_user() {
    let (server, client) = connected(vec![
        session(vec![completion(super::provider::NO_DROP_EXIT, "Failure")]),
        session(vec![out("stdOut", "hi\n"), completion(0, "Success")]),
        session(vec![completion(0, "Success")]),
    ])
    .await;
    let output = client
        .run_command(&instance("svc-1"), &shell("echo hi"))
        .await
        .unwrap();
    assert!(output.success());
    assert_eq!(output.stdout, b"hi\n");
    client
        .run_command(&instance("svc-1"), &shell("echo again"))
        .await
        .unwrap();
    let ran = contexts(&server);
    assert_eq!(ran[0].0, "root");
    assert_eq!(
        ran[1],
        ("user".to_string(), super::exec::argv(&shell("echo hi")))
    );
    assert_eq!(ran[2].0, "user", "the home was set up");
}

/// Readiness is read every second, and the wait ends at the first read
/// that sees a running container: at most a second after it runs.
#[tokio::test]
async fn readiness_ends_at_the_first_poll_that_sees_a_running_container() {
    assert_eq!(
        NorthflankConfig::default().poll_interval,
        Duration::from_secs(1)
    );
    let server = FakeServer::start(vec![
        Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
        Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
        containers(&["TASK_STARTING"]),
        Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
        containers(&["TASK_RUNNING"]),
        Reply::data(&service("svc-1", "COMPLETED", 1, &tags())),
        containers(&["TASK_RUNNING"]),
    ])
    .await;
    client(&server).connect("svc-1", 600).await.unwrap();
    assert_eq!(
        server.lines().len(),
        5,
        "no read after the container runs: {:?}",
        server.lines()
    );
}

/// Run the setup-then-run script as root would, against `home`, with fake
/// `id` (answering `uid:gid`, or failing when `None`), a `chown` that only
/// logs its arguments and, when `setpriv`, a `setpriv` that logs its
/// options and runs the command after `--`. The command prints `ran` and
/// the uid it was given.
#[cfg(unix)]
fn run_setup(
    home: &std::path::Path,
    owner: Option<(u32, u32)>,
    setpriv: bool,
) -> (std::process::Output, String) {
    let shims = tempfile::tempdir().unwrap();
    let log = shims.path().join("calls.log");
    let id = match owner {
        Some((u, g)) => format!("#!/bin/sh\n[ \"$2\" = user ] || exit 1\ncase \"$1\" in -u) echo {u};; -g) echo {g};; esac\n"),
        None => "#!/bin/sh\necho \"id: 'user': no such user\" >&2\nexit 1\n".into(),
    };
    let chown = format!("#!/bin/sh\necho chown \"$@\" >> '{}'\n", log.display());
    let drop = format!(
        "#!/bin/sh\nopts=\"\"\nwhile [ \"$1\" != -- ]; do opts=\"$opts $1\"; shift; done\nshift\necho \"setpriv$opts\" >> '{}'\nexec \"$@\"\n",
        log.display()
    );
    let mut shimmed = vec![("id", id), ("chown", chown)];
    if setpriv {
        shimmed.push(("setpriv", drop));
    }
    for (name, body) in shimmed {
        let path = shims.path().join(name);
        std::fs::write(&path, body).unwrap();
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
    }
    // Only the shims and the tools the script needs: no real setpriv.
    for tool in ["stat", "env", "echo"] {
        let found = ["/usr/bin", "/bin"]
            .iter()
            .map(|dir| std::path::Path::new(dir).join(tool))
            .find(|path| path.exists())
            .unwrap();
        std::os::unix::fs::symlink(found, shims.path().join(tool)).unwrap();
    }
    let script = super::provider::setup_then_run_script("user");
    let output = std::process::Command::new("/bin/sh")
        .args([
            "-c",
            &script,
            "intent-setup",
            "env",
            "/bin/sh",
            "-c",
            "echo ran",
        ])
        .env_clear()
        .env("PATH", shims.path())
        .env("HOME", home)
        .output()
        .unwrap();
    (output, std::fs::read_to_string(&log).unwrap_or_default())
}

#[cfg(unix)]
#[test]
fn the_setup_hands_the_home_over_only_when_the_owner_differs_then_drops_to_the_user() {
    use std::os::unix::fs::MetadataExt as _;
    let home = tempfile::tempdir().unwrap();
    let meta = std::fs::metadata(home.path()).unwrap();
    let (uid, gid) = (meta.uid(), meta.gid());
    let ready = format!("{}\n", super::provider::HOME_READY);
    let drop = |u: u32, g: u32| format!("setpriv --reuid={u} --regid={g} --init-groups\n");

    let (owned, log) = run_setup(home.path(), Some((uid, gid)), true);
    assert!(owned.status.success(), "{owned:?}");
    assert_eq!(log, drop(uid, gid), "an owned home is left alone");
    assert_eq!(String::from_utf8_lossy(&owned.stderr), ready);
    assert_eq!(String::from_utf8_lossy(&owned.stdout), "ran\n");

    let (other, log) = run_setup(home.path(), Some((uid + 1, gid)), true);
    assert!(other.status.success(), "{other:?}");
    assert_eq!(
        log,
        format!(
            "chown -R {}:{gid} {}\n{}",
            uid + 1,
            home.path().display(),
            drop(uid + 1, gid)
        )
    );
    assert_eq!(String::from_utf8_lossy(&other.stdout), "ran\n");

    let (missing, log) = run_setup(home.path(), None, true);
    assert_eq!(missing.status.code(), Some(super::provider::NO_USER_EXIT));
    assert_eq!(log, "");
    assert!(missing.stdout.is_empty(), "nothing runs as root");

    let (no_drop, log) = run_setup(home.path(), Some((uid + 1, gid)), false);
    assert_eq!(no_drop.status.code(), Some(super::provider::NO_DROP_EXIT));
    assert!(log.starts_with("chown -R"), "{log}");
    assert!(no_drop.stdout.is_empty(), "nothing runs as root");
    assert!(!String::from_utf8_lossy(&no_drop.stderr).contains(super::provider::HOME_READY));
}

#[tokio::test]
async fn an_image_without_the_user_account_is_refused_at_the_first_command() {
    let (server, client) = connected(vec![session(vec![completion(
        super::provider::NO_USER_EXIT,
        "Failure",
    )])])
    .await;
    let err = client
        .run_command(&instance("svc-1"), &shell("echo started"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Other);
    assert!(
        err.to_string().contains("predates") && err.to_string().contains("start a new one"),
        "an old image says what to do: {err}"
    );
    assert!(
        !server.lines().iter().any(|l| l.starts_with("DELETE")),
        "a command never deletes the machine"
    );
}

#[tokio::test]
async fn exec_keeps_reading_after_completion_until_close() {
    let server = exec_server(vec![
        WsStep::Receive,
        init_ok(),
        out("stdOut", "a"),
        completion(0, "Success"),
        WsStep::Sleep(Duration::from_millis(100)),
        out("stdOut", "b"),
        out("stdErr", "late"),
        WsStep::Close,
    ])
    .await;
    let output = client(&server)
        .run_command(&instance("svc-1"), &ProcessSpec::new("x"))
        .await
        .unwrap();
    assert_eq!(output.stdout, b"ab");
    assert_eq!(output.stderr, b"late");
}

/// Pings after `completion` do not extend the drain: it counts from the
/// last output frame, so output after the drain is not waited for.
#[tokio::test]
async fn exec_pings_do_not_extend_the_drain() {
    let mut steps = vec![
        WsStep::Receive,
        init_ok(),
        out("stdOut", "early"),
        completion(0, "Success"),
    ];
    for _ in 0..10 {
        steps.push(WsStep::Sleep(Duration::from_millis(100)));
        steps.push(WsStep::Ping);
    }
    steps.push(out("stdOut", "late"));
    steps.push(WsStep::Sleep(Duration::from_secs(30)));
    let server = exec_server(steps).await;
    let output = client(&server)
        .run_command(&instance("svc-1"), &ProcessSpec::new("x"))
        .await
        .unwrap();
    assert_eq!(output.stdout, b"early");
}

#[tokio::test]
async fn exec_gives_up_waiting_for_more_output_after_the_drain_time() {
    let server = exec_server(vec![
        WsStep::Receive,
        init_ok(),
        completion(0, "Success"),
        WsStep::Sleep(Duration::from_millis(100)),
        out("stdOut", "late"),
        WsStep::Sleep(Duration::from_secs(30)),
    ])
    .await;
    let output = client(&server)
        .run_command(&instance("svc-1"), &ProcessSpec::new("x"))
        .await
        .unwrap();
    assert_eq!(output.stdout, b"late");
}

#[tokio::test]
async fn exec_closed_before_completion_is_a_failed_command() {
    for end in [WsStep::Close, WsStep::Drop] {
        let server = exec_server(vec![
            WsStep::Receive,
            init_ok(),
            out("stdOut", "partial"),
            end,
        ])
        .await;
        let err = client(&server)
            .run_command(&instance("svc-1"), &ProcessSpec::new("x"))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), CloudErrorKind::Other);
        assert!(
            err.to_string()
                .contains("the command stream ended before the command finished"),
            "{err}"
        );
    }
}

#[tokio::test]
async fn exec_auth_failures_are_unauthorized_or_denied() {
    let server = FakeServer::start_with(
        vec![],
        vec![
            WsSession::Steps(vec![
                WsStep::Receive,
                WsStep::Send(json!({"type": "init", "data": {"auth": "failed", "message": format!("Invalid API token {TOKEN}")}})),
                WsStep::Close,
            ]),
            WsSession::Steps(vec![
                WsStep::Receive,
                WsStep::Send(json!({"type": "init", "data": {"auth": "failed", "message": "Insufficient permissions to execute commands"}})),
                WsStep::Close,
            ]),
            WsSession::Refuse(403, json!({"error": {"status": 403, "message": "Forbidden"}}).to_string()),
            WsSession::Refuse(404, json!({"error": {"status": 404, "message": "Service not found"}}).to_string()),
        ],
    )
    .await;
    let client = client(&server);
    let spec = ProcessSpec::new("x");
    let rejected = client
        .run_command(&instance("svc-1"), &spec)
        .await
        .unwrap_err();
    assert_eq!(rejected.kind(), CloudErrorKind::Unauthorized);
    assert_no_token(&rejected.to_string());
    let denied = client
        .run_command(&instance("svc-1"), &spec)
        .await
        .unwrap_err();
    assert_eq!(denied.kind(), CloudErrorKind::PermissionDenied);
    assert_eq!(denied.action(), Some("exec"));
    let refused = client
        .run_command(&instance("svc-1"), &spec)
        .await
        .unwrap_err();
    assert_eq!(refused.kind(), CloudErrorKind::PermissionDenied);
    assert_eq!(refused.action(), Some("exec"));
    assert_eq!(refused.status(), Some(403));
    let missing = client
        .run_command(&instance("svc-1"), &spec)
        .await
        .unwrap_err();
    assert!(missing.is_not_found(), "{missing}");
}

#[tokio::test]
async fn exec_error_frame_ends_the_command() {
    let server = exec_server(vec![
        WsStep::Receive,
        init_ok(),
        WsStep::Send(json!({"type": "error", "data": {"message": "container not running"}})),
        WsStep::Close,
    ])
    .await;
    let err = client(&server)
        .run_command(&instance("svc-1"), &ProcessSpec::new("x"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Other);
    assert!(err.to_string().contains("container not running"), "{err}");
}

#[tokio::test]
async fn exec_timeout_bounds_the_whole_command() {
    let server = exec_server(vec![
        WsStep::Receive,
        init_ok(),
        out("stdOut", "working"),
        WsStep::Sleep(Duration::from_secs(30)),
    ])
    .await;
    let mut spec = ProcessSpec::new("sleep");
    spec.timeout = Some(Duration::from_millis(200));
    let err = client(&server)
        .run_command(&instance("svc-1"), &spec)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Other);
    assert!(err.to_string().contains("timed out"), "{err}");
}

#[tokio::test]
async fn exec_output_is_capped() {
    let big = "x".repeat(1024 * 1024);
    let mut steps = vec![WsStep::Receive, init_ok()];
    for _ in 0..17 {
        steps.push(out("stdOut", &big));
    }
    steps.push(completion(0, "Success"));
    steps.push(WsStep::Close);
    let server = exec_server(steps).await;
    let output = client(&server)
        .run_command(&instance("svc-1"), &ProcessSpec::new("x"))
        .await
        .unwrap();
    assert!(output.truncated);
    assert_eq!(output.stdout.len(), 16 * 1024 * 1024);
}

// --- images ---

#[test]
fn image_references_are_parsed() {
    let parse = |s: &str| ImageRef::parse(s).unwrap();
    assert_eq!(
        parse("ghcr.io/intent-hq/intent-node:0.11.184"),
        ImageRef {
            registry: "ghcr.io".into(),
            repository: "intent-hq/intent-node".into(),
            reference: "0.11.184".into()
        }
    );
    assert_eq!(
        parse("alpine:3"),
        ImageRef {
            registry: "registry-1.docker.io".into(),
            repository: "library/alpine".into(),
            reference: "3".into()
        }
    );
    assert_eq!(
        parse("docker.io/foo/bar:1").registry,
        "registry-1.docker.io"
    );
    assert_eq!(parse("docker.io/foo/bar:1").repository, "foo/bar");
    assert_eq!(parse("user/app:latest").repository, "user/app");
    assert_eq!(parse("localhost:5000/app:v1").registry, "localhost:5000");
    assert_eq!(parse("localhost:5000/app:v1").repository, "app");
    assert_eq!(parse("ghcr.io/a/b@sha256:0123").reference, "sha256:0123");
    for bad in ["", "ghcr.io/a/b", "UPPER/x:1", "a b:1", "ghcr.io/:1", "x:"] {
        assert!(ImageRef::parse(bad).is_err(), "{bad}");
    }
}

#[tokio::test]
async fn image_exists_follows_the_anonymous_token_challenge() {
    let server = FakeServer::start(vec![]).await;
    let realm = format!("{}/token", server.url);
    server.push(Reply::status(401).with_header(
        "www-authenticate",
        &format!(r#"Bearer realm="{realm}",service="fake-registry",scope="repository:intent-hq/intent-node:pull""#),
    ));
    server.push(Reply::json(200, &json!({"token": "anon-token"})));
    server.push(Reply::status(200));
    let image = format!("{}/intent-hq/intent-node:0.11.184", server.host());
    assert!(client(&server).image_exists(&image).await.unwrap());
    let requests = server.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[0].line(),
        "HEAD /v2/intent-hq/intent-node/manifests/0.11.184"
    );
    let accept = requests[0].header("accept").unwrap();
    assert!(
        accept.contains("application/vnd.oci.image.index.v1+json"),
        "{accept}"
    );
    assert!(
        accept.contains("application/vnd.docker.distribution.manifest.list.v2+json"),
        "{accept}"
    );
    assert_eq!(requests[0].header("authorization"), None);
    assert!(
        requests[1].target.starts_with("/token?"),
        "{}",
        requests[1].target
    );
    assert!(requests[1].target.contains("service=fake-registry"));
    assert!(requests[1]
        .target
        .contains("scope=repository%3Aintent-hq%2Fintent-node%3Apull"));
    assert_eq!(requests[1].header("authorization"), None);
    assert_eq!(
        requests[2].header("authorization"),
        Some("Bearer anon-token")
    );
}

#[tokio::test]
async fn image_exists_is_false_for_a_missing_tag() {
    let server = FakeServer::start(vec![]).await;
    let realm = format!("{}/token", server.url);
    server.push(Reply::status(404));
    server.push(Reply::status(401).with_header(
        "www-authenticate",
        &format!(r#"Bearer realm="{realm}",service="r",scope="repository:x/private:pull""#),
    ));
    server.push(Reply::json(200, &json!({"access_token": "anon"})));
    server.push(Reply::status(401));
    let client = client(&server);
    let host = server.host();
    assert!(!client
        .image_exists(&format!("{host}/x/y:missing"))
        .await
        .unwrap());
    // A private image is not pullable without credentials: not ready.
    assert!(!client
        .image_exists(&format!("{host}/x/private:1"))
        .await
        .unwrap());
}

#[tokio::test]
async fn image_ready_uses_the_registry() {
    let server = FakeServer::start(vec![Reply::status(200)]).await;
    let image = format!("{}/a/b:1", server.host());
    assert!(client(&server).image_ready(&image).await.unwrap());
}

#[tokio::test]
async fn exec_malformed_frame_is_an_error() {
    let server = exec_server(vec![
        WsStep::Receive,
        init_ok(),
        WsStep::SendText("not json".into()),
        WsStep::Close,
    ])
    .await;
    let err = client(&server)
        .run_command(&instance("svc-1"), &ProcessSpec::new("x"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Other);
    assert!(err.to_string().contains("malformed"), "{err}");
}

#[tokio::test]
async fn registry_url_redirects_every_registry_request() {
    let server = FakeServer::start(vec![
        Reply::status(401).with_header(
            "www-authenticate",
            r#"Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:intent-hq/intent-node:pull""#,
        ),
        Reply::json(200, &json!({"token": "anon"})),
        Reply::status(200),
    ])
    .await;
    let mut config = config(&server);
    config.registry_url = Some(format!("{}/", server.url));
    let client = NorthflankClient::new(SecretString::new(TOKEN), config).unwrap();
    assert!(client
        .image_exists("ghcr.io/intent-hq/intent-node:0.11.184")
        .await
        .unwrap());
    let lines = server.lines();
    assert_eq!(
        lines[0],
        "HEAD /v2/intent-hq/intent-node/manifests/0.11.184"
    );
    assert!(
        lines[1].starts_with("GET /token?service=ghcr.io"),
        "{lines:?}"
    );
    assert_eq!(
        server.requests()[2].header("authorization"),
        Some("Bearer anon")
    );
}
