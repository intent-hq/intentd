use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::fake_api::{
    CommandStart, Reply, VercelApi, FIXTURE_PROJECT, FIXTURE_TEAM, FIXTURE_TOKEN,
};
use super::fake_server::{FakeServer, Host, NoopHost, Ran};
use super::*;
use crate::cloud::{
    CloudErrorKind, CloudInstance, CloudProvider, CreateInstance, InstanceState, ListInstances,
    ProcessSpec, QuotaLimit,
};

const VERSION: &str = "0.11.254";
const BACKEND: &str = "b1";

fn image() -> String {
    image_ref(DEFAULT_IMAGE, VERSION)
}

fn config(server: &FakeServer) -> VercelConfig {
    VercelConfig {
        team_id: FIXTURE_TEAM.into(),
        project_id: FIXTURE_PROJECT.into(),
        api_url: server.url.clone(),
        registry_url: server.url.clone(),
        request_timeout: Duration::from_secs(10),
        connect_timeout: Duration::from_secs(5),
        command_timeout: Duration::from_secs(10),
        slow_timeout: Duration::from_secs(10),
        retry_window: Duration::from_millis(500),
        retry_interval: Duration::from_millis(10),
        ready_timeout: Duration::from_secs(2),
        poll_interval: Duration::from_millis(10),
    }
}

fn client(server: &FakeServer) -> VercelClient {
    VercelClient::new(SecretString::new(FIXTURE_TOKEN), config(server)).unwrap()
}

async fn server() -> FakeServer {
    FakeServer::start(VercelApi::default(), Arc::new(NoopHost)).await
}

fn assert_no_token(text: &str) {
    assert!(!text.contains(FIXTURE_TOKEN), "token leaked into {text}");
}

fn tags(backend: &str, agent: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (BACKEND_ID_TAG.to_string(), backend.to_string()),
        ("agentId".to_string(), agent.to_string()),
        ("workspaceId".to_string(), "w1".to_string()),
        (
            "intentEntryId".to_string(),
            "vercel-000000000001".to_string(),
        ),
        ("intentdVersion".to_string(), VERSION.to_string()),
    ])
}

fn create_request() -> CreateInstance {
    CreateInstance {
        image: image(),
        timeout_secs: Some(1800),
        tags: tags(BACKEND, "a1"),
        vcpu: Some(4),
        memory_gib: Some(8),
        ..CreateInstance::default()
    }
}

fn instance(id: &str) -> CloudInstance {
    CloudInstance {
        id: id.into(),
        image: image(),
        access_token: None,
        volume_id: None,
    }
}

/// Runs each command with the local shell, as the sandbox would, and
/// remembers every one it got.
#[derive(Default)]
struct ShellHost {
    seen: Mutex<Vec<CommandStart>>,
}

impl Host for ShellHost {
    fn run(&self, start: CommandStart) -> Pin<Box<dyn Future<Output = Ran> + Send + '_>> {
        self.seen.lock().unwrap().push(start.clone());
        Box::pin(async move {
            let mut command = tokio::process::Command::new(&start.command);
            command.args(&start.args).envs(start.env.clone());
            if let Some(cwd) = &start.cwd {
                command.current_dir(cwd);
            }
            let out = command.output().await.unwrap();
            (
                out.status.code().unwrap_or(-1),
                String::from_utf8_lossy(&out.stdout).into_owned(),
                String::from_utf8_lossy(&out.stderr).into_owned(),
            )
        })
    }
}

// --- names and settings ---

#[test]
fn ids_and_images_follow_vercels_formats() {
    assert!(valid_team_id("team_3FDuuIrwFJKvP0TFkb69Hd4z"));
    assert!(!valid_team_id("team_"));
    assert!(!valid_team_id("sutterhill"));
    assert!(!valid_team_id("team_a-b"));
    assert!(valid_project_id("prj_nMVmU2e6GsRlzt08mJGH3knIjoSI"));
    assert!(!valid_project_id("intent-updates"));
    assert!(!valid_project_id("prj_x y"));
    assert!(valid_image(DEFAULT_IMAGE));
    assert!(valid_image("intent-node"));
    assert!(!valid_image("sutterhill/intent-node"), "two parts");
    assert!(
        !valid_image("sutterhill/intent-images/intent-node:1"),
        "a tag"
    );
    assert!(
        !valid_image("Sutterhill/intent-images/intent-node"),
        "uppercase"
    );
    assert!(!valid_image(""));
    assert_eq!(
        image_ref(DEFAULT_IMAGE, VERSION),
        "sutterhill/intent-images/intent-node:0.11.254"
    );
}

#[tokio::test]
async fn new_refuses_an_empty_token_and_redacts_it() {
    let err = VercelClient::new(SecretString::new(""), VercelConfig::default()).unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::NotConfigured);
    let client =
        VercelClient::new(SecretString::new(FIXTURE_TOKEN), VercelConfig::default()).unwrap();
    assert_no_token(&format!("{client:?}"));
    assert_eq!(client.kind(), CLOUD_PROVIDER_VERCEL);
    assert!(client.has_lease(), "the session timeout is a lease");
    assert!(
        !client.caps_run(),
        "the session cap is reported by a renewal"
    );
    assert!(!client.keeps_memory(), "a resume keeps only the disk");
    let defaults = VercelConfig::default();
    assert_eq!(defaults.api_url, "https://api.vercel.com");
    assert_eq!(defaults.registry_url, "https://vcr.vercel.com");
}

// --- create ---

#[tokio::test]
async fn create_starts_a_persistent_sandbox_from_the_image() {
    let server = server().await;
    let created = client(&server).create(&create_request()).await.unwrap();
    assert!(
        created.id.starts_with("intent-") && created.id.len() == 19,
        "{}",
        created.id
    );
    assert_eq!(created.image, image());
    assert!(created.volume_id.is_none());
    let requests = server.requests();
    assert_eq!(server.lines(), ["POST /v3/sandboxes"]);
    let request = &requests[0];
    assert!(
        request.path.ends_with(&format!("?teamId={FIXTURE_TEAM}")),
        "{}",
        request.path
    );
    assert_eq!(
        request.header("authorization"),
        Some(format!("Bearer {FIXTURE_TOKEN}").as_str())
    );
    let body = request.json();
    assert_eq!(body["projectId"], FIXTURE_PROJECT);
    assert_eq!(body["name"], created.id.as_str());
    assert_eq!(body["image"], image().as_str());
    assert_eq!(body["persistent"], true);
    assert_eq!(body["resources"], json!({ "vcpus": 4 }));
    assert_eq!(body["timeout"], 1_800_000);
    assert_eq!(
        body["keepLastSnapshots"],
        json!({ "count": 1, "deleteEvicted": true })
    );
    assert_eq!(body["snapshotExpiration"], SNAPSHOT_EXPIRATION_MS);
    assert_eq!(body["tags"][BACKEND_ID_TAG], BACKEND);
    assert_eq!(body["tags"]["agentId"], "a1");
    for absent in ["ports", "env", "networkPolicy", "region", "runtime"] {
        assert!(body.get(absent).is_none(), "{absent} sent");
    }
    assert_no_token(&request.path);
    assert_no_token(&String::from_utf8_lossy(&request.body));
    let api = server.state();
    let sandbox = api.sandbox(&created.id).unwrap();
    assert_eq!(sandbox.session.status, "running");
    assert_eq!(sandbox.vcpus, 4);
}

#[tokio::test]
async fn create_needs_vcpus_the_backend_tag_and_at_most_five_tags() {
    let server = server().await;
    let client = client(&server);
    let mut no_vcpu = create_request();
    no_vcpu.vcpu = None;
    let mut no_backend = create_request();
    no_backend.tags.remove(BACKEND_ID_TAG);
    let mut six = create_request();
    six.tags.insert("extra".into(), "x".into());
    for request in [no_vcpu, no_backend, six] {
        let err = client.create(&request).await.unwrap_err();
        assert_eq!(err.kind(), CloudErrorKind::InvalidArgument, "{err}");
    }
    assert!(server.requests().is_empty(), "nothing reached Vercel");
}

#[tokio::test]
async fn create_maps_vercels_refusals() {
    let server = server().await;
    let client = client(&server);

    let mut missing = create_request();
    missing.image = image_ref(DEFAULT_IMAGE, "0.0.1");
    let err = client.create(&missing).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::NotFound, "{err}");
    assert_eq!(err.status(), Some(404));

    let mut huge = create_request();
    huge.vcpu = Some(16);
    let err = client.create(&huge).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Quota(QuotaLimit::Plan), "{err}");
    assert!(err.to_string().contains("resources.vcpus"), "{err}");
    assert!(!err.is_retryable());

    let mut other = config(&server);
    other.project_id = "prj_missing0000".into();
    let err = VercelClient::new(SecretString::new(FIXTURE_TOKEN), other)
        .unwrap()
        .create(&create_request())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::ProjectMissing, "{err}");

    let err = VercelClient::new(SecretString::new("wrong-token"), config(&server))
        .unwrap()
        .create(&create_request())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Unauthorized, "{err}");

    let mut team = config(&server);
    team.team_id = "team_other".into();
    let err = VercelClient::new(SecretString::new(FIXTURE_TOKEN), team)
        .unwrap()
        .create(&create_request())
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::PermissionDenied, "{err}");
    assert_eq!(err.action(), Some("create-sandbox"));

    server.push_override(
        "POST /v3/sandboxes",
        Reply::error(429, "rate_limited", "Too many requests"),
    );
    let err = client.create(&create_request()).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Quota(QuotaLimit::Rate), "{err}");
    assert!(server.state().sandboxes.is_empty());
}

// --- commands ---

#[tokio::test]
async fn run_command_streams_the_output_as_the_images_user() {
    let host = Arc::new(ShellHost::default());
    let server = FakeServer::start(VercelApi::default(), host.clone()).await;
    let client = client(&server);
    let created = client.create(&create_request()).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut spec = ProcessSpec::new("/bin/sh");
    spec.args = vec![
        "-c".into(),
        "pwd; printf '%s\\n' \"$SECRET_VAR\"; echo oops >&2; exit 3".into(),
    ];
    spec.cwd = Some(dir.path().to_string_lossy().into_owned());
    spec.envs
        .insert("SECRET_VAR".into(), SecretString::new("from-env"));
    spec.user = Some("user".into());
    let out = client.run_command(&created, &spec).await.unwrap();
    assert_eq!(out.exit.exit_code, 3);
    assert!(out.exit.exited);
    assert!(!out.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("from-env"), "{stdout}");
    assert!(stdout.contains(&*dir.path().to_string_lossy()), "{stdout}");
    assert_eq!(String::from_utf8_lossy(&out.stderr), "oops\n");

    let lines = server.lines();
    let session = server
        .state()
        .sandbox(&created.id)
        .unwrap()
        .session
        .id
        .clone();
    assert!(
        lines.contains(&format!("POST /v2/sandboxes/sessions/{session}/cmd")),
        "{lines:?}"
    );
    let command = server
        .requests()
        .into_iter()
        .find(|r| r.path.contains("/cmd"))
        .unwrap()
        .json();
    assert_eq!(command["wait"], true);
    assert_eq!(command["logs"], true);
    assert!(command.get("sudo").is_none(), "never sudo");
    assert_eq!(command["env"]["SECRET_VAR"], "from-env");
    let seen = host.seen.lock().unwrap();
    assert_eq!(seen[0].command, "/bin/sh");
}

#[tokio::test]
async fn a_timed_command_is_ended_by_timeout_in_the_callers_group() {
    let host = Arc::new(ShellHost::default());
    let server = FakeServer::start(VercelApi::default(), host.clone()).await;
    let client = client(&server);
    let created = client.create(&create_request()).await.unwrap();
    let mut spec = ProcessSpec::new("echo");
    spec.args = vec!["hi".into()];
    spec.timeout = Some(Duration::from_secs(7));
    let out = client.run_command(&created, &spec).await.unwrap();
    assert!(out.success(), "{out:?}");
    let seen = host.seen.lock().unwrap();
    assert_eq!(seen[0].command, "timeout");
    assert_eq!(seen[0].args, ["--foreground", "-k", "5", "7", "echo", "hi"]);
}

#[tokio::test]
async fn run_command_refuses_another_user_and_a_stopped_sandbox() {
    let server = server().await;
    let client = client(&server);
    let created = client.create(&create_request()).await.unwrap();
    let mut root = ProcessSpec::new("id");
    root.user = Some("root".into());
    let err = client.run_command(&created, &root).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::InvalidArgument, "{err}");

    client.pause(&created.id).await.unwrap();
    let err = client
        .run_command(&created, &ProcessSpec::new("id"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::NotFound, "{err}");
    let err = client
        .run_command(&instance("intent-gone00000"), &ProcessSpec::new("id"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::NotFound, "{err}");
}

#[tokio::test]
async fn a_stream_error_line_fails_the_command() {
    let server = server().await;
    let client = client(&server);
    let created = client.create(&create_request()).await.unwrap();
    let session = server
        .state()
        .sandbox(&created.id)
        .unwrap()
        .session
        .id
        .clone();
    server.push_override(
        &format!("POST /v2/sandboxes/sessions/{session}/cmd"),
        Reply::error(
            400,
            "executable_not_found",
            "[invalid_argument] executable file not found in $PATH: nope",
        ),
    );
    let err = client
        .run_command(&created, &ProcessSpec::new("nope"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Api, "{err}");
    assert!(err.to_string().contains("executable_not_found"), "{err}");
}

// --- pause, resume, keep alive ---

#[tokio::test]
async fn pause_stops_with_a_snapshot_and_connect_resumes_a_new_session() {
    let server = server().await;
    let client = client(&server);
    let created = client.create(&create_request()).await.unwrap();
    let first = server
        .state()
        .sandbox(&created.id)
        .unwrap()
        .session
        .id
        .clone();

    client.pause(&created.id).await.unwrap();
    {
        let api = server.state();
        let sandbox = api.sandbox(&created.id).unwrap();
        assert_eq!(sandbox.session.status, "stopped");
        assert!(sandbox.snapshot.is_some(), "the disk is kept");
    }
    // A second pause of a stopped sandbox is fine and stops nothing.
    client.pause(&created.id).await.unwrap();
    assert_eq!(
        server
            .lines()
            .iter()
            .filter(|l| l.ends_with("/stop"))
            .count(),
        1
    );

    let connected = client.connect(&created.id, 1800).await.unwrap();
    assert!(connected.resumed);
    assert_eq!(connected.instance.id, created.id);
    let second = server
        .state()
        .sandbox(&created.id)
        .unwrap()
        .session
        .id
        .clone();
    assert_ne!(first, second, "a resume is a new session");
    let resume = server
        .requests()
        .into_iter()
        .rev()
        .find(|r| r.method == "GET" && r.path.contains("resume=true"))
        .unwrap();
    assert!(resume
        .path
        .contains(&format!("projectId={FIXTURE_PROJECT}")));

    let again = client.connect(&created.id, 1800).await.unwrap();
    assert!(!again.resumed, "a running sandbox is not resumed");
    let err = client.connect("intent-gone00000", 1800).await.unwrap_err();
    assert!(err.is_not_found(), "{err}");
}

#[tokio::test]
async fn keep_alive_extends_the_session_only_when_it_ends_too_soon() {
    let server = server().await;
    let client = client(&server);
    let mut short = create_request();
    short.timeout_secs = Some(600);
    let created = client.create(&short).await.unwrap();

    // 10 minutes left, 30 wanted: extend by the missing 20 minutes.
    client.keep_alive(&created.id, 1800).await.unwrap();
    let timeout = server.state().sandbox(&created.id).unwrap().session.timeout;
    assert!(
        (1_800_000..=1_860_000).contains(&timeout),
        "extended to {timeout} ms"
    );
    let extends = |server: &FakeServer| {
        server
            .lines()
            .iter()
            .filter(|l| l.ends_with("/extend-timeout"))
            .count()
    };
    assert_eq!(extends(&server), 1);
    let body = server
        .requests()
        .into_iter()
        .find(|r| r.path.contains("/extend-timeout"))
        .unwrap()
        .json();
    let duration = body["duration"].as_u64().unwrap();
    assert!((1_200_000..=1_260_000).contains(&duration), "{body}");
    assert_eq!(duration % 60_000, 0, "whole minutes");

    // 30 minutes left, 30 wanted: nothing to do.
    client.keep_alive(&created.id, 1800).await.unwrap();
    assert_eq!(extends(&server), 1);
}

#[tokio::test]
async fn keep_alive_reports_the_session_cap_and_a_stopped_sandbox() {
    let server = server().await;
    let client = client(&server);
    server.state().max_session_ms = 45 * 60 * 1000;
    let created = client.create(&create_request()).await.unwrap();
    let err = client.keep_alive(&created.id, 3600).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::SessionCap, "{err}");

    // Vercel stopped it when its timeout ran out: not running, so the
    // backend looks it up and marks it paused.
    server.state().time_out(&created.id);
    let err = client.keep_alive(&created.id, 1800).await.unwrap_err();
    assert!(err.is_not_found(), "{err}");
    let listed = client
        .list(&ListInstances {
            tags: tags(BACKEND, "a1"),
            states: vec![],
        })
        .await
        .unwrap();
    assert_eq!(listed[0].state, InstanceState::Paused);
}

// --- delete, list, key, image ---

#[tokio::test]
async fn delete_removes_the_sandbox_with_its_snapshots_once() {
    let server = server().await;
    let client = client(&server);
    let created = client.create(&create_request()).await.unwrap();
    assert!(client.delete(&created.id).await.unwrap());
    assert!(!client.delete(&created.id).await.unwrap());
    let delete = server
        .requests()
        .into_iter()
        .find(|r| r.method == "DELETE")
        .unwrap();
    assert!(
        delete.path.contains("deleteOrphanSnapshots=true"),
        "{}",
        delete.path
    );
    assert!(delete
        .path
        .contains(&format!("projectId={FIXTURE_PROJECT}")));
    assert!(server.state().sandboxes.is_empty());
    let err = client.delete("bad name").await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::InvalidArgument);
}

#[tokio::test]
async fn list_filters_by_tags_and_state() {
    let server = server().await;
    let client = client(&server);
    let mine = client.create(&create_request()).await.unwrap();
    let mut other = create_request();
    other.tags = tags("b2", "a2");
    let theirs = client.create(&other).await.unwrap();
    client.pause(&theirs.id).await.unwrap();

    let listed = client
        .list(&ListInstances {
            tags: BTreeMap::from([(BACKEND_ID_TAG.to_string(), BACKEND.to_string())]),
            states: vec![],
        })
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, mine.id);
    assert_eq!(listed[0].state, InstanceState::Running);
    assert_eq!(listed[0].tags, tags(BACKEND, "a1"));
    assert!(
        listed[0].started_at.ends_with('Z'),
        "{}",
        listed[0].started_at
    );
    let list = server
        .requests()
        .into_iter()
        .find(|r| r.method == "GET" && r.path.starts_with("/v2/sandboxes?"))
        .unwrap();
    assert!(
        list.path
            .contains(&format!("tags={BACKEND_ID_TAG}:{BACKEND}")),
        "{}",
        list.path
    );

    let paused = client
        .list(&ListInstances {
            tags: BTreeMap::new(),
            states: vec![InstanceState::Paused],
        })
        .await
        .unwrap();
    assert_eq!(paused.len(), 1);
    assert_eq!(paused[0].id, theirs.id);
}

#[tokio::test]
async fn check_key_tells_a_wrong_token_team_and_project_apart() {
    let server = server().await;
    client(&server).check_key().await.unwrap();
    let err = VercelClient::new(SecretString::new("wrong"), config(&server))
        .unwrap()
        .check_key()
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Unauthorized);
    assert_no_token(&err.to_string());
    let mut team = config(&server);
    team.team_id = "team_other".into();
    let err = VercelClient::new(SecretString::new(FIXTURE_TOKEN), team)
        .unwrap()
        .check_key()
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::PermissionDenied);
    assert_eq!(err.action(), Some("list-sandboxes"));
    let mut project = config(&server);
    project.project_id = "prj_other".into();
    let err = VercelClient::new(SecretString::new(FIXTURE_TOKEN), project)
        .unwrap()
        .check_key()
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::ProjectMissing);
}

#[tokio::test]
async fn image_ready_asks_the_registry_with_the_team_and_token() {
    let server = server().await;
    let client = client(&server);
    assert!(client.image_ready(&image()).await.unwrap());
    assert!(!client
        .image_ready(&image_ref(DEFAULT_IMAGE, "0.0.1"))
        .await
        .unwrap());
    let head = server
        .requests()
        .into_iter()
        .find(|r| r.method == "HEAD")
        .unwrap();
    assert_eq!(
        head.path,
        format!("/v2/{DEFAULT_IMAGE}/manifests/{VERSION}")
    );
    assert!(head.header("authorization").unwrap().starts_with("Basic "));
    assert!(head.header("accept").unwrap().contains("oci.image.index"));
    // The registry does not answer another team's token for this
    // repository: the create is the real check.
    let mut team = config(&server);
    team.team_id = "team_other".into();
    let other = VercelClient::new(SecretString::new(FIXTURE_TOKEN), team).unwrap();
    assert!(other.image_ready(&image()).await.unwrap());
}

#[tokio::test]
async fn transient_failures_are_retried_within_the_window() {
    let server = server().await;
    let client = client(&server);
    server.push_override(
        "GET /v2/sandboxes",
        Reply::error(503, "unavailable", "busy"),
    );
    client.check_key().await.unwrap();
    for _ in 0..200 {
        server.push_override(
            "GET /v2/sandboxes",
            Reply::error(503, "unavailable", "busy"),
        );
    }
    let err = client.check_key().await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Api);
    assert!(err.is_retryable());
    assert_eq!(err.status(), Some(503));
}

// --- review regressions ---

#[tokio::test]
async fn keep_alive_extends_only_by_what_is_missing_so_a_hobby_cap_is_not_hit_early() {
    let server = server().await;
    let client = client(&server);
    server.state().max_session_ms = 45 * 60 * 1000;
    let created = client.create(&create_request()).await.unwrap();
    // Ten minutes into a 30-minute session: 20 minutes left, 30 wanted.
    server
        .state()
        .sandbox_mut(&created.id)
        .unwrap()
        .session
        .started_at -= 10 * 60 * 1000;
    client.keep_alive(&created.id, 1800).await.unwrap();
    let timeout = server.state().sandbox(&created.id).unwrap().session.timeout;
    assert!(
        (40 * 60 * 1000..=41 * 60 * 1000).contains(&timeout),
        "extended to {timeout} ms, within the 45-minute cap"
    );
}

#[tokio::test]
async fn connect_keeps_a_running_session_alive_and_tolerates_the_cap() {
    let server = server().await;
    let client = client(&server);
    let mut short = create_request();
    short.timeout_secs = Some(600);
    let created = client.create(&short).await.unwrap();
    let connected = client.connect(&created.id, 1800).await.unwrap();
    assert!(!connected.resumed);
    let timeout = server.state().sandbox(&created.id).unwrap().session.timeout;
    assert!(
        timeout >= 1_800_000,
        "a redial extends the session: {timeout}"
    );

    // At the cap, a connect still succeeds: the supervisor's renewal
    // reports the cap and plans the stop and resume.
    server.state().max_session_ms = timeout;
    server
        .state()
        .sandbox_mut(&created.id)
        .unwrap()
        .session
        .started_at -= 20 * 60 * 1000;
    client.connect(&created.id, 1800).await.unwrap();
}

#[tokio::test]
async fn an_image_in_the_entrys_own_project_is_checked_at_the_create() {
    let server = server().await;
    let client = client(&server);
    assert!(client.image_ready("intent-node:0.0.1").await.unwrap());
    assert!(
        !server.lines().iter().any(|l| l.starts_with("HEAD")),
        "the registry path needs the team and project: no lookup"
    );
}

#[tokio::test]
async fn a_command_timeout_beyond_the_budget_is_clamped_on_both_sides() {
    let host = Arc::new(ShellHost::default());
    let server = FakeServer::start(VercelApi::default(), host.clone()).await;
    let client = client(&server);
    let created = client.create(&create_request()).await.unwrap();
    let mut spec = ProcessSpec::new("true");
    spec.timeout = Some(Duration::from_secs(3600));
    client.run_command(&created, &spec).await.unwrap();
    let seen = host.seen.lock().unwrap();
    assert_eq!(seen[0].args[3], "10", "the client's 10 s budget, not 3600");
}
