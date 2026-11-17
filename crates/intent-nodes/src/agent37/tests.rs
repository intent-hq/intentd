use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::fake_api::{exec_reply, Agent37Api, ExecStart, Reply, FIXTURE_KEY};
use super::fake_server::{FakeServer, Host, NoopHost};
use super::*;
use crate::cloud::{
    CloudErrorKind, CloudInstance, CloudProvider, CreateInstance, InstanceState, ListInstances,
    ProcessSpec, QuotaLimit,
};

const IMAGE: &str = "ghcr.io/intent-hq/intent-node:0.11.236";
const TEMPLATE: &str = "intent-node-0-11-236";
const BACKEND: &str = "b1";

fn config(server: &FakeServer) -> Agent37Config {
    Agent37Config {
        api_url: server.url.clone(),
        exec_url: server.url.clone(),
        request_timeout: Duration::from_secs(10),
        connect_timeout: Duration::from_secs(5),
        exec_timeout: Duration::from_secs(10),
        retry_window: Duration::from_millis(500),
        retry_interval: Duration::from_millis(10),
        ready_timeout: Duration::from_secs(2),
        poll_interval: Duration::from_millis(10),
    }
}

fn client(server: &FakeServer) -> Agent37Client {
    Agent37Client::new(SecretString::new(FIXTURE_KEY), config(server)).unwrap()
}

async fn server() -> FakeServer {
    FakeServer::start(Agent37Api::default(), Arc::new(NoopHost)).await
}

fn assert_no_key(text: &str) {
    assert!(!text.contains(FIXTURE_KEY), "key leaked into {text}");
}

fn tags(backend: &str, agent: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (BACKEND_ID_TAG.to_string(), backend.to_string()),
        ("agentId".to_string(), agent.to_string()),
    ])
}

fn create_request(instance_type: &str, region: &str) -> CreateInstance {
    CreateInstance {
        image: IMAGE.into(),
        tags: tags(BACKEND, "a1"),
        disk_gib: Some(20),
        machine_type: Some(instance_type.into()),
        region: Some(region.into()),
        vcpu: Some(4),
        memory_gib: Some(8),
        ..CreateInstance::default()
    }
}

fn instance(id: &str) -> CloudInstance {
    CloudInstance {
        id: id.into(),
        image: IMAGE.into(),
        access_token: None,
        volume_id: None,
    }
}

/// Runs each exec's command with the local `/bin/sh`, as the instance
/// would, and remembers every one it got.
#[derive(Default)]
struct ShellHost {
    seen: Mutex<Vec<ExecStart>>,
}

impl Host for ShellHost {
    fn exec(&self, start: ExecStart) -> Pin<Box<dyn Future<Output = Reply> + Send + '_>> {
        self.seen.lock().unwrap().push(start.clone());
        Box::pin(async move {
            let out = tokio::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(&start.command)
                .output()
                .await
                .unwrap();
            exec_reply(
                out.status.code().unwrap_or(-1),
                &String::from_utf8_lossy(&out.stdout),
                &String::from_utf8_lossy(&out.stderr),
            )
        })
    }
}

// --- names and settings ---

#[test]
fn a_template_is_named_after_the_image_tag() {
    assert_eq!(template_name(IMAGE), TEMPLATE);
    assert_eq!(
        template_name("ghcr.io/intent-hq/intent-node:1.2.3-RC.1+b"),
        "intent-node-1-2-3-rc-1-b"
    );
    assert_eq!(image_ref("0.11.236"), IMAGE);
    let long = format!("{IMAGE_REPOSITORY}:{}", "9".repeat(80));
    assert_eq!(template_name(&long).len(), 63);
}

#[test]
fn only_vm_instances_run_outside_us() {
    assert!(region_allows("default", "us"));
    assert!(region_allows("vm", "us"));
    assert!(region_allows("vm", "eu"));
    assert!(region_allows("vm", "ap-south"));
    assert!(!region_allows("default", "eu"));
    assert!(!region_allows("default", "ap-south"));
    assert!(!region_allows("performance", "us"));
    assert!(!region_allows("vm", "mars"));
}

#[tokio::test]
async fn new_refuses_an_empty_key_and_redacts_it() {
    let err = Agent37Client::new(SecretString::new(""), Agent37Config::default()).unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::NotConfigured);
    let client =
        Agent37Client::new(SecretString::new(FIXTURE_KEY), Agent37Config::default()).unwrap();
    assert_no_key(&format!("{client:?}"));
    assert_eq!(client.kind(), CLOUD_PROVIDER_AGENT37);
    assert!(
        client.has_lease(),
        "renewals keep Agent37's own auto-sleep off"
    );
    assert!(!client.caps_run(), "Agent37 has no continuous-run cap");
    assert!(!client.keeps_memory(), "a wake can boot fresh");
    let defaults = Agent37Config::default();
    assert_eq!(defaults.api_url, "https://api.agent37.com/v1");
    assert_eq!(defaults.exec_url, "https://sandbox.agent37.app/v1");
}

// --- create ---

#[tokio::test]
async fn create_imports_the_template_once_then_starts_the_instance() {
    let server = server().await;
    let client = client(&server);
    let created = client
        .create(&create_request("default", "us"))
        .await
        .unwrap();
    assert_eq!(created.id, "fx00000001");
    assert_eq!(created.image, IMAGE);
    assert_eq!(
        server.lines(),
        [
            format!("GET /templates/{TEMPLATE}"),
            "POST /templates".into(),
            "POST /instances".into(),
        ]
    );
    let requests = server.requests();
    let import = requests[1].json();
    assert_eq!(import["name"], TEMPLATE);
    assert_eq!(import["image_ref"], IMAGE);
    let body = requests[2].json();
    assert_eq!(body["template"], TEMPLATE);
    assert_eq!(body["type"], "default");
    assert_eq!(body["region"], "us");
    assert_eq!(
        body["resources"],
        json!({"cpu": 4, "memory": 8, "disk": 20})
    );
    assert_eq!(
        body["metadata"],
        json!({"intentBackendId": BACKEND, "agentId": "a1"})
    );
    assert_eq!(body["auto_sleep"], true);
    assert_eq!(body["idle_timeout_seconds"], BACKSTOP_IDLE_SECS);
    let name = body["name"].as_str().unwrap();
    assert!(name.starts_with("intent-") && name.len() == 19, "{name}");
    assert!(body.get("env").is_none() && body.get("public_ports").is_none());
    for request in &requests {
        assert_eq!(
            request.header("authorization"),
            Some(format!("Bearer {FIXTURE_KEY}").as_str())
        );
        assert_no_key(&request.path);
        assert_no_key(&String::from_utf8_lossy(&request.body));
    }

    // The second start reuses the template.
    client.create(&create_request("vm", "eu")).await.unwrap();
    let lines = server.lines();
    assert_eq!(
        lines[3..],
        [
            format!("GET /templates/{TEMPLATE}"),
            "POST /instances".into()
        ]
    );
    assert_eq!(server.state().instance("fx00000002").unwrap().region, "eu");
}

#[tokio::test]
async fn a_template_of_that_name_with_another_image_is_updated() {
    let server = server().await;
    server.state().templates.insert(
        TEMPLATE.into(),
        super::fake_api::Template {
            name: TEMPLATE.into(),
            image_ref: "ghcr.io/intent-hq/intent-node:0.11.200".into(),
            revision: 1,
            created: 1,
            updated: 1,
        },
    );
    client(&server)
        .create(&create_request("default", "us"))
        .await
        .unwrap();
    assert_eq!(
        server.lines(),
        [
            format!("GET /templates/{TEMPLATE}"),
            format!("PATCH /templates/{TEMPLATE}"),
            "POST /instances".into(),
        ]
    );
    assert_eq!(server.state().templates[TEMPLATE].image_ref, IMAGE);
}

#[tokio::test]
async fn a_concurrent_import_is_read_again() {
    let server = server().await;
    let client = client(&server);
    assert!(client.image_ready(IMAGE).await.unwrap());
    // Another start imported it between this one's read and its import.
    server.push_override(
        "GET /templates/*",
        Reply::error(404, "not_found", "Template not found."),
    );
    client
        .create(&create_request("default", "us"))
        .await
        .unwrap();
    assert_eq!(
        server.lines()[2..],
        [
            format!("GET /templates/{TEMPLATE}"),
            "POST /templates".into(),
            format!("GET /templates/{TEMPLATE}"),
            "POST /instances".into(),
        ]
    );
}

#[tokio::test]
async fn an_image_agent37_cannot_pull_is_not_ready() {
    let server = server().await;
    let client = client(&server);
    let missing = "ghcr.io/intent-hq/intent-node:0.0.1";
    assert!(!client.image_ready(missing).await.unwrap());
    assert!(server.state().templates.is_empty());
    let err = client
        .create(&CreateInstance {
            image: missing.into(),
            ..create_request("default", "us")
        })
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::NotFound, "{err}");
    assert!(client.image_ready(IMAGE).await.unwrap());
    assert!(server.state().templates.contains_key(TEMPLATE));
}

#[tokio::test]
async fn a_type_and_region_agent37_does_not_run_are_refused_before_any_call() {
    let server = server().await;
    let client = client(&server);
    let err = client
        .create(&create_request("default", "eu"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::InvalidArgument, "{err}");
    let err = client
        .create(&CreateInstance {
            machine_type: None,
            ..create_request("default", "us")
        })
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::InvalidArgument, "{err}");
    let err = client
        .create(&CreateInstance {
            tags: BTreeMap::new(),
            ..create_request("default", "us")
        })
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::InvalidArgument, "{err}");
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn create_refusals_map_to_their_kinds() {
    let server = server().await;
    let client = client(&server);
    assert!(client.image_ready(IMAGE).await.unwrap());

    server.state().no_capacity.push(("vm".into(), "eu".into()));
    let err = client
        .create(&create_request("vm", "eu"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::NoCapacity, "{err}");
    assert_eq!(err.status(), Some(503));
    assert!(err.is_retryable());

    server.state().funded = false;
    let err = client
        .create(&create_request("default", "us"))
        .await
        .unwrap_err();
    assert_eq!(err.quota_limit(), Some(QuotaLimit::Plan), "{err}");
    assert!(!err.is_retryable());
    server.state().funded = true;

    server.state().memory_limit = 4;
    let err = client
        .create(&create_request("default", "us"))
        .await
        .unwrap_err();
    assert_eq!(err.quota_limit(), Some(QuotaLimit::Concurrency), "{err}");
    assert!(err.is_retryable());

    server.push_override(
        "POST /instances",
        Reply::error(
            403,
            "ip_not_allowed",
            "Request from 203.0.113.9 is not allowed.",
        ),
    );
    let err = client
        .create(&create_request("default", "us"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::PermissionDenied, "{err}");
    assert_eq!(err.action(), Some("create-instance"));

    server.push_override("POST /instances", Reply::error(429, "rate_limited", "slow"));
    server.push_override("POST /instances", Reply::error(429, "rate_limited", "slow"));
    server.push_override("POST /instances", Reply::error(429, "rate_limited", "slow"));
    server.push_override("POST /instances", Reply::error(429, "rate_limited", "slow"));
    let err = client
        .create(&create_request("default", "us"))
        .await
        .unwrap_err();
    assert_eq!(err.quota_limit(), Some(QuotaLimit::Rate), "{err}");

    let bad = Agent37Client::new(SecretString::new("wrong-agent37-key"), config(&server)).unwrap();
    let err = bad
        .create(&create_request("default", "us"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Unauthorized, "{err}");
    assert_eq!(err.status(), Some(401));
    assert!(!err.to_string().contains("wrong-agent37-key"));
}

// --- commands ---

#[tokio::test]
async fn a_command_runs_through_exec_with_its_env_cwd_and_quoted_args() {
    let host = Arc::new(ShellHost::default());
    let server = FakeServer::start(Agent37Api::default(), host.clone()).await;
    let client = client(&server);
    let created = client
        .create(&create_request("default", "us"))
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut spec = ProcessSpec::new("/bin/sh");
    spec.args = vec![
        "-c".into(),
        "printf '%s|%s|%s\\n' \"$1\" \"$SECRET\" \"$(pwd)\"; echo oops >&2; exit 3".into(),
        "arg0".into(),
        "it's \"quoted\" $HOME".into(),
    ];
    spec.envs
        .insert("SECRET".into(), SecretString::new("v a'l\"ue"));
    spec.cwd = Some(dir.path().to_string_lossy().into_owned());
    let out = client.run_command(&created, &spec).await.unwrap();
    assert_eq!(out.exit.exit_code, 3);
    assert!(out.exit.exited);
    assert!(!out.success());
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        format!(
            "it's \"quoted\" $HOME|v a'l\"ue|{}\n",
            dir.path().canonicalize().unwrap().display()
        )
    );
    assert_eq!(String::from_utf8_lossy(&out.stderr), "oops\n");
    assert!(!format!("{spec:?}").contains("v a'l"));

    let exec = server
        .requests()
        .into_iter()
        .find(|r| r.path.ends_with("/exec"))
        .unwrap();
    assert_eq!(exec.path, format!("/instances/{}/exec", created.id));
    assert_eq!(exec.header("x-agent37-key"), Some(FIXTURE_KEY));
    assert_eq!(exec.header("authorization"), None);
    assert!(
        exec.json().get("user").is_none(),
        "runs as the image's user"
    );
    assert_eq!(host.seen.lock().unwrap()[0].user, None);

    let mut root = ProcessSpec::new("id");
    root.user = Some("root".into());
    client.run_command(&created, &root).await.unwrap();
    assert_eq!(host.seen.lock().unwrap()[1].user.as_deref(), Some("root"));

    let mut other = ProcessSpec::new("id");
    other.user = Some("nobody".into());
    let err = client.run_command(&created, &other).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::InvalidArgument);
}

#[tokio::test]
async fn exec_retries_a_wake_in_progress_and_reports_a_gone_instance() {
    let server = server().await;
    let client = client(&server);
    let created = client
        .create(&create_request("default", "us"))
        .await
        .unwrap();
    let route = "POST /instances/*/exec";
    server.push_override(route, Reply::flat(503, "wake_failed"));
    server.push_override(route, Reply::error(409, "try_again", "checkpoint"));
    server.push_override(route, Reply::flat(502, "upstream_unreachable"));
    let out = client
        .run_command(&created, &ProcessSpec::new("true"))
        .await
        .unwrap();
    assert!(out.success());

    for _ in 0..200 {
        server.push_override(route, Reply::flat(503, "wake_timeout"));
    }
    let err = client
        .run_command(&created, &ProcessSpec::new("true"))
        .await
        .unwrap_err();
    assert!(err.is_retryable(), "{err}");
    server.clear_overrides();

    let err = client
        .run_command(&instance("nope000000"), &ProcessSpec::new("true"))
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "{err}");

    let bad = Agent37Client::new(SecretString::new("wrong-agent37-key"), config(&server)).unwrap();
    let err = bad
        .run_command(&created, &ProcessSpec::new("true"))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Unauthorized, "{err}");
}

// --- lifecycle ---

#[tokio::test]
async fn pause_sleeps_and_connect_wakes() {
    let server = server().await;
    let client = client(&server);
    let created = client
        .create(&create_request("default", "us"))
        .await
        .unwrap();
    let id = created.id.as_str();

    let connected = client.connect(id, 1800).await.unwrap();
    assert!(!connected.resumed);

    server.push_override(
        "POST /instances/*/sleep",
        Reply::error(409, "try_again", "another operation"),
    );
    client.pause(id).await.unwrap();
    assert_eq!(server.state().instance(id).unwrap().status, "sleeping");
    client.pause(id).await.unwrap();

    let connected = client.connect(id, 1800).await.unwrap();
    assert!(connected.resumed);
    assert_eq!(connected.instance.id, id);
    assert_eq!(server.state().instance(id).unwrap().status, "running");
    assert!(server
        .lines()
        .contains(&format!("POST /instances/{id}/start")));

    let err = client.connect("nope000000", 60).await.unwrap_err();
    assert!(err.is_not_found());
    server.state().instance_mut(id).unwrap().status = "failed".into();
    let err = client.connect(id, 60).await.unwrap_err();
    assert_eq!(err.kind(), CloudErrorKind::Other, "{err}");
}

#[tokio::test]
async fn connect_waits_out_a_wake_another_call_started() {
    let server = server().await;
    let client = client(&server);
    let created = client
        .create(&create_request("default", "us"))
        .await
        .unwrap();
    server.state().instance_mut(&created.id).unwrap().status = "waking".into();
    let api = server.api.clone();
    let id = created.id.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        api.lock().unwrap().instance_mut(&id).unwrap().status = "running".into();
    });
    let connected = client.connect(&created.id, 60).await.unwrap();
    assert!(connected.resumed);
}

#[tokio::test]
async fn keep_alive_runs_one_cheap_exec_and_never_wakes_a_sleeper() {
    let host = Arc::new(ShellHost::default());
    let server = FakeServer::start(Agent37Api::default(), host.clone()).await;
    let client = client(&server);
    let created = client
        .create(&create_request("default", "us"))
        .await
        .unwrap();
    client.keep_alive(&created.id, 1800).await.unwrap();
    assert_eq!(host.seen.lock().unwrap().len(), 1);
    assert_eq!(host.seen.lock().unwrap()[0].command, "true");

    client.pause(&created.id).await.unwrap();
    let err = client.keep_alive(&created.id, 1800).await.unwrap_err();
    assert!(err.is_not_found(), "{err}");
    assert_eq!(host.seen.lock().unwrap().len(), 1);
    assert_eq!(
        server.state().instance(&created.id).unwrap().status,
        "sleeping"
    );
}

#[tokio::test]
async fn delete_is_idempotent() {
    let server = server().await;
    let client = client(&server);
    let created = client
        .create(&create_request("default", "us"))
        .await
        .unwrap();
    assert!(client.delete(&created.id).await.unwrap());
    assert!(!client.delete(&created.id).await.unwrap());
    assert!(server.state().instance(&created.id).is_none());
    assert_eq!(
        server
            .lines()
            .iter()
            .filter(|l| *l == &format!("DELETE /instances/{}", created.id))
            .count(),
        2
    );
}

#[tokio::test]
async fn list_returns_this_backends_instances_with_their_tags_and_states() {
    let server = server().await;
    let client = client(&server);
    let mine = client
        .create(&create_request("default", "us"))
        .await
        .unwrap();
    let asleep = client
        .create(&CreateInstance {
            tags: tags(BACKEND, "a2"),
            ..create_request("default", "us")
        })
        .await
        .unwrap();
    client.pause(&asleep.id).await.unwrap();
    client
        .create(&CreateInstance {
            tags: tags("other", "a3"),
            ..create_request("default", "us")
        })
        .await
        .unwrap();

    let all = client
        .list(&ListInstances {
            tags: BTreeMap::from([(BACKEND_ID_TAG.into(), BACKEND.into())]),
            states: vec![],
        })
        .await
        .unwrap();
    let mut ids: Vec<(&str, InstanceState)> =
        all.iter().map(|l| (l.id.as_str(), l.state)).collect();
    ids.sort_unstable_by_key(|(id, _)| *id);
    assert_eq!(
        ids,
        [
            (mine.id.as_str(), InstanceState::Running),
            (asleep.id.as_str(), InstanceState::Paused)
        ]
    );
    let listed = all.iter().find(|l| l.id == mine.id).unwrap();
    assert_eq!(listed.tags, tags(BACKEND, "a1"));
    assert!(
        listed.started_at.starts_with("2026-11-"),
        "{}",
        listed.started_at
    );

    let paused = client
        .list(&ListInstances {
            tags: tags(BACKEND, "a2"),
            states: vec![InstanceState::Paused],
        })
        .await
        .unwrap();
    assert_eq!(paused.len(), 1);
    assert_eq!(paused[0].id, asleep.id);
}

#[tokio::test]
async fn check_key_lists_instances() {
    let server = server().await;
    client(&server).check_key().await.unwrap();
    assert_eq!(server.lines(), ["GET /instances"]);
    let bad = Agent37Client::new(SecretString::new("wrong-agent37-key"), config(&server)).unwrap();
    assert_eq!(
        bad.check_key().await.unwrap_err().kind(),
        CloudErrorKind::Unauthorized
    );
}
