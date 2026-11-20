//! Agent37 cloud machines end to end, against the shared fake Agent37
//! (`intent_nodes::agent37::fake_api`, held to the real API by its contract
//! test) behind its HTTP server. Each instance runs its exec commands for
//! real in its own folder (its disk), so its "machine" is a real
//! `intentd serve` that the backend's own start script starts. A sleep
//! stops every process of the instance (SIGSTOP) and a wake continues them,
//! as Agent37's memory checkpoint does; a wake that boots fresh kills them
//! and keeps the folder. The backend dials the daemon directly
//! (`INTENTD_E2B_DIRECT_HOST`): a test cannot join the tailcat relay.
//!
//! Proves, for a `default` instance in `us` and a `vm` instance in `eu`:
//! `configure` imports the template and refuses a wrong key, a type and
//! region Agent37 does not run, and sizes and disks it does not offer,
//! before any save; `target: "cloud"` starts an agent on a new instance of
//! that type, region, size and disk, tagged with the backend; the backend
//! renews activity with an exec while the instance runs, sleeps it when
//! idle, and a new message wakes it with the conversation kept and no
//! notice; a wake that boots fresh restarts intentd from the disk and tells
//! the agent once; `no_capacity` fails the start with `no-capacity` and
//! nothing elsewhere; removing the entry with `stopAgents` deletes the
//! instance; the key never reaches the backend's log.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use intent_nodes::agent37::fake_api::{exec_reply, Agent37Api, Event, ExecStart, Reply};
use intent_nodes::agent37::fake_server::{FakeServer, Host};
use intentd_test_support::ProcessGroup;

use super::*;

const A37_KEY: &str = "e2e-agent37-key-00000000000000000";
const IMAGE_REPO: &str = "ghcr.io/intent-hq/intent-node";
/// The words of the notice a launch after a fresh boot carries.
const DISK_NOTICE: &str = "stopped and started again from its disk";

/// One instance's folder (its disk), environment and current processes.
struct Machine {
    home: std::path::PathBuf,
    env: Vec<(String, String)>,
    group: Arc<ProcessGroup>,
}

#[derive(Default)]
struct Machines {
    by_id: HashMap<String, Machine>,
    /// Every group ever made, ended when the fake drops.
    groups: Vec<Arc<ProcessGroup>>,
    /// Every exec command, by instance.
    execs: Vec<(String, String)>,
    /// Every stdout, to look for the executor token.
    outputs: Vec<String>,
}

/// Runs each instance's commands in its folder, and its lifecycle on its
/// processes.
struct LocalHost {
    root: std::path::PathBuf,
    shim_dir: std::path::PathBuf,
    fixture: String,
    machines: Mutex<Machines>,
}

impl LocalHost {
    fn machines(&self) -> std::sync::MutexGuard<'_, Machines> {
        self.machines.lock().unwrap()
    }

    fn signal(&self, id: &str, signal: &str) {
        let Some(pgid) = self.machines().by_id.get(id).map(|m| m.group.pgid()) else {
            return;
        };
        let _ = std::process::Command::new("kill")
            .args([signal, "--", &format!("-{pgid}")])
            .status();
    }

    /// The instance's environment: a hermetic daemon fixture, the codex
    /// toolchain that runs the node fixture, this test's intentd first on
    /// PATH, and `HOME` on its disk, as Agent37 runs exec as the image's
    /// user.
    fn machine_env(&self, home: &Path) -> Vec<(String, String)> {
        let dir = home.join(".intent");
        std::fs::create_dir_all(dir.join("workspaces")).unwrap();
        let reference = common::hermetic_serve_command(&dir);
        let mut env: Vec<(String, String)> = reference
            .get_envs()
            .filter_map(|(k, v)| Some((k.to_str()?.to_owned(), v?.to_str()?.to_owned())))
            .filter(|(key, _)| key != "INTENTD_DATA_DIR" && key != "HOME")
            .collect();
        let toolchain = common::codex_npx::install(home, &self.fixture);
        let intentd_dir = Path::new(env!("CARGO_BIN_EXE_intentd"))
            .parent()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let shim_dir = self.shim_dir.to_string_lossy().into_owned();
        for (key, value) in toolchain {
            let value = if key == "PATH" {
                format!("{shim_dir}:{intentd_dir}:{value}")
            } else {
                value
            };
            env.push((key, value));
        }
        env.push(("HOME".into(), home.to_string_lossy().into_owned()));
        env.push((
            "INTENTD_WORKSPACES_DIR".into(),
            dir.join("workspaces").to_string_lossy().into_owned(),
        ));
        env.push(("INTENTD_AUTH_TOKEN".into(), TOKEN.into()));
        env.push((
            "NODE_TEST_AGENT_SESSIONS".into(),
            home.join(".node-test-agent").to_string_lossy().into_owned(),
        ));
        // One port for the instance's life: a restarted daemon must listen
        // where the machine row dials.
        env.retain(|(key, _)| key != "INTENTD_TCP_PORT");
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        env.push(("INTENTD_TCP_PORT".into(), port.to_string()));
        env
    }
}

impl Host for LocalHost {
    fn exec(&self, start: ExecStart) -> Pin<Box<dyn Future<Output = Reply> + Send + '_>> {
        Box::pin(async move {
            let Some((home, env, pgid)) = self
                .machines()
                .by_id
                .get(&start.id)
                .map(|m| (m.home.clone(), m.env.clone(), m.group.pgid()))
            else {
                return Reply::flat(404, "not_found");
            };
            self.machines()
                .execs
                .push((start.id.clone(), start.command.clone()));
            let output = tokio::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(&start.command)
                .current_dir(&home)
                .env_clear()
                .envs(env)
                .process_group(pgid)
                .stdin(Stdio::null())
                .output()
                .await
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
            self.machines().outputs.push(stdout.clone());
            exec_reply(
                output.status.code().unwrap_or(-1),
                &stdout,
                &String::from_utf8_lossy(&output.stderr),
            )
        })
    }

    fn events(&self, events: Vec<Event>) {
        for event in events {
            match event {
                Event::Created(id) => {
                    let home = self.root.join(&id);
                    std::fs::create_dir_all(&home).unwrap();
                    let env = self.machine_env(&home);
                    let group = Arc::new(ProcessGroup::new().unwrap());
                    let mut machines = self.machines();
                    machines.groups.push(group.clone());
                    machines.by_id.insert(id, Machine { home, env, group });
                }
                Event::Slept(id) => self.signal(&id, "-STOP"),
                Event::Woke { id, fresh: false } => self.signal(&id, "-CONT"),
                Event::Woke { id, fresh: true } => {
                    let mut machines = self.machines();
                    let fresh = Arc::new(ProcessGroup::new().unwrap());
                    if let Some(machine) = machines.by_id.get_mut(&id) {
                        machine.group.kill();
                        machine.group = fresh.clone();
                    }
                    machines.groups.push(fresh);
                }
                Event::Deleted(id) => {
                    if let Some(machine) = self.machines().by_id.remove(&id) {
                        machine.group.terminate(stop_grace());
                    }
                }
            }
        }
    }
}

/// The fake Agent37 and the scratch dir it owns; its `Drop` ends every
/// instance's processes before that dir goes.
struct FakeAgent37 {
    server: FakeServer,
    host: Arc<LocalHost>,
    scratch: tempfile::TempDir,
}

impl Drop for FakeAgent37 {
    fn drop(&mut self) {
        for group in &self.host.machines().groups {
            group.terminate(stop_grace());
        }
    }
}

impl FakeAgent37 {
    fn start_in(scratch: tempfile::TempDir, fixture: &str) -> impl Future<Output = Self> {
        let root = scratch.path().join("agent37");
        let shim_dir = scratch.path().join("a37-bin");
        let fixture = fixture.to_owned();
        async move {
            std::fs::create_dir_all(&root).unwrap();
            std::fs::create_dir_all(&shim_dir).unwrap();
            let shim = shim_dir.join("setsid");
            std::fs::write(&shim, SETSID_SHIM).unwrap();
            std::fs::set_permissions(&shim, std::os::unix::fs::PermissionsExt::from_mode(0o755))
                .unwrap();
            let host = Arc::new(LocalHost {
                root,
                shim_dir,
                fixture,
                machines: Mutex::default(),
            });
            let server = FakeServer::start(Agent37Api::new(A37_KEY), host.clone()).await;
            Self {
                server,
                host,
                scratch,
            }
        }
    }

    fn api(&self) -> std::sync::MutexGuard<'_, Agent37Api> {
        self.server.state()
    }

    fn only_instance(&self) -> Option<String> {
        self.api().instances.first().map(|i| i.id.clone())
    }

    fn execs_of(&self, id: &str) -> Vec<String> {
        self.host
            .machines()
            .execs
            .iter()
            .filter(|(of, _)| of == id)
            .map(|(_, command)| command.clone())
            .collect()
    }

    fn executor_tokens(&self) -> Vec<String> {
        self.host
            .machines()
            .outputs
            .iter()
            .filter_map(|out| serde_json::from_str::<Value>(out).ok())
            .filter_map(|v| v["token"].as_str().map(str::to_owned))
            .collect()
    }
}

fn agent37_params(extra: &Value) -> Value {
    let mut params = json!({ "provider": "agent37", "name": "Agent37", "apiKey": A37_KEY });
    for (k, v) in extra.as_object().unwrap() {
        params[k] = v.clone();
    }
    params
}

async fn refused(rpc_ws: &mut common::TlsWs, id: i64, method: &str, params: Value) -> Value {
    let reply = rpc(rpc_ws, id, method, params).await;
    assert!(reply.get("error").is_some(), "{reply}");
    reply["error"].clone()
}

/// A backend against a fake Agent37, with one settings minute of 100 ms,
/// and an API-key profile. `None` (the test skips) without node or git.
async fn backend(
    prefix: &str,
) -> Option<(
    FakeAgent37,
    common::DaemonGuard,
    u16,
    Arc<ClientConfig>,
    String,
    String,
    common::TlsWs,
)> {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping Agent37 e2e: node or git not on PATH");
        return None;
    }
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let scratch = common::test_tempdir_in("/tmp", prefix);
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let fake = FakeAgent37::start_in(scratch, &fixture).await;
    let head_dir = fake.scratch.path().join("head");
    let url = fake.server.url.clone();
    let (head, port, cfg, ws_id) = start_backend_with(
        &head_dir,
        &repo,
        &[
            ("INTENTD_AGENT37_API_URL", &url),
            ("INTENTD_AGENT37_EXEC_URL", &url),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
            ("INTENTD_E2B_DAEMON_WAIT_SECS", "30"),
            ("INTENTD_E2B_MINUTE_MS", "100"),
        ],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "a37-life", "name": "Agent37 lifecycle" }),
    )
    .await;
    let keyed = ok(
        &mut rpc_ws,
        2,
        "providers.profiles.create",
        json!({ "baseProviderId": "codex", "name": "Cloud", "env": [
            { "key": "OPENAI_API_KEY", "kind": "set", "value": PROFILE_SECRET },
        ] }),
    )
    .await;
    let keyed_ref = format!("codex@{}", keyed["profile"]["id"].as_str().unwrap());
    Some((fake, head, port, cfg, ws_id, keyed_ref, rpc_ws))
}

/// The life of one agent on an Agent37 instance of `instance_type` in
/// `region`.
async fn life(instance_type: &str, region: &str, prefix: &str) {
    let Some((fake, _head, port, cfg, ws_id, keyed_ref, mut rpc_ws)) = backend(prefix).await else {
        return;
    };
    let head_dir = fake.scratch.path().join("head");
    let version = env!("CARGO_PKG_VERSION");
    let template = intent_nodes::agent37::template_name(&format!("{IMAGE_REPO}:{version}"));

    // Refusals before any save.
    let rejected = refused(
        &mut rpc_ws,
        10,
        "cloudMachines.configure",
        agent37_params(&json!({ "apiKey": "wrong-agent37-key" })),
    )
    .await;
    assert_eq!(rejected["data"]["reason"], "key-rejected", "{rejected}");
    assert_eq!(rejected["data"]["provider"], "agent37", "{rejected}");
    for (n, extra) in [
        json!({ "instanceType": "default", "region": "eu" }),
        json!({ "region": "ap-south" }),
        json!({ "instanceType": "performance" }),
        json!({ "size": "8x16" }),
        json!({ "diskGiB": 200 }),
        json!({ "projectId": "p1" }),
        json!({ "templateProject": "intent" }),
    ]
    .into_iter()
    .enumerate()
    {
        let invalid = refused(
            &mut rpc_ws,
            11 + i64::try_from(n).unwrap(),
            "cloudMachines.configure",
            agent37_params(&extra),
        )
        .await;
        assert_eq!(invalid["code"], -32602, "{extra}: {invalid}");
    }
    let eu_default = refused(
        &mut rpc_ws,
        20,
        "cloudMachines.configure",
        agent37_params(&json!({ "region": "eu" })),
    )
    .await;
    assert!(
        eu_default["message"]
            .as_str()
            .unwrap()
            .contains("instanceType \"vm\""),
        "{eu_default}"
    );
    let checked = ok(
        &mut rpc_ws,
        21,
        "cloudMachines.configure",
        agent37_params(
            &json!({ "validateOnly": true, "instanceType": instance_type, "region": region }),
        ),
    )
    .await;
    assert_eq!(checked["provider"]["id"], "", "{checked}");
    assert!(
        fake.api().templates.contains_key(&template),
        "validateOnly imported the template"
    );
    let listed = ok(&mut rpc_ws, 22, "cloudMachines.list", json!({})).await;
    assert_eq!(
        listed["providers"],
        json!([]),
        "nothing was saved: {listed}"
    );
    assert_eq!(
        listed["providerKinds"],
        json!(["e2b", "northflank", "agent37", "vercel"])
    );

    let saved = ok(
        &mut rpc_ws,
        23,
        "cloudMachines.configure",
        agent37_params(&json!({
            "instanceType": instance_type, "region": region, "size": "2x4", "diskGiB": 30,
            "idlePauseMinutes": 1440,
        })),
    )
    .await;
    let provider = &saved["provider"];
    let entry = provider["id"].as_str().unwrap().to_owned();
    assert!(entry.starts_with("agent37-"), "{provider}");
    assert_eq!(provider["provider"], "agent37");
    assert_eq!(provider["template"], template.as_str());
    assert!(provider.get("templateProject").is_none(), "{provider}");
    assert_eq!(
        provider["agent37"],
        json!({ "instanceType": instance_type, "region": region, "diskGiB": 30 })
    );
    assert_eq!(
        provider["sizes"],
        json!([
            { "id": "2x4", "vcpu": 2, "memoryMiB": 4096 },
            { "id": "4x8", "vcpu": 4, "memoryMiB": 8192 },
            { "id": "4x16", "vcpu": 4, "memoryMiB": 16384 },
            { "id": "4x32", "vcpu": 4, "memoryMiB": 32768 },
        ])
    );

    // No room in the region: the start fails, nothing starts elsewhere.
    fake.api()
        .no_capacity
        .push((instance_type.to_owned(), region.to_owned()));
    let created = ok(
        &mut rpc_ws,
        30,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Sleepy", "model": "default", "provider": keyed_ref,
            "placement": { "target": "cloud" },
        }),
    )
    .await;
    let sleepy = created["agent"]["id"].as_str().unwrap().to_owned();
    assert_eq!(created["agent"]["placement"]["target"], "e2b", "{created}");
    assert_eq!(
        created["agent"]["placement"]["cloudMachineId"],
        entry.as_str()
    );
    let failed = agent_until(&mut rpc_ws, (ws_id.as_str(), sleepy.as_str()), 31, |a| {
        a.get("placementError").is_some()
    })
    .await;
    let error = &failed["placementError"];
    assert_eq!(error["code"], "cloud-machine-unavailable", "{failed}");
    assert_eq!(error["data"]["reason"], "no-capacity", "{failed}");
    assert_eq!(error["retryable"], true, "{failed}");
    let message = error["detail"].as_str().unwrap();
    assert!(
        message.contains(&format!(
            "a {instance_type} 2x4 instance in region {region}"
        )),
        "{message}"
    );
    assert!(fake.api().instances.is_empty());
    fake.api().no_capacity.clear();

    // With room again, an agent starts on a new instance of the entry.
    let created = ok(
        &mut rpc_ws,
        38,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Awake", "model": "default", "provider": keyed_ref,
            "placement": { "target": "cloud" },
        }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    let agent = (ws_id.as_str(), agent_id.as_str());
    let first = echoed(&mut rpc_ws, agent, "first turn", 40).await;
    assert!(!first.contains(DISK_NOTICE), "{first}");
    let id = fake.only_instance().unwrap();
    let machine = machine_until(&mut rpc_ws, 50, &id, |m| m.is_some())
        .await
        .unwrap();
    let machine_id = machine["id"].as_str().unwrap().to_owned();
    assert_eq!(machine["cloud"]["provider"], "agent37", "{machine}");
    assert_eq!(machine["cloud"]["template"], template.as_str(), "{machine}");
    assert!(machine["cloud"].get("volumeId").is_none(), "{machine}");
    {
        let api = fake.api();
        let instance = api.instance(&id).unwrap();
        assert_eq!(instance.instance_type, instance_type);
        assert_eq!(instance.region, region);
        assert_eq!((instance.cpu, instance.memory), (2, 4));
        assert_eq!(instance.disk, if instance_type == "vm" { 30 } else { 12 });
        assert!(instance.auto_sleep);
        assert_eq!(instance.idle_timeout_seconds, 1800);
        assert_eq!(instance.template, template);
        assert!(instance.metadata["intentBackendId"].is_string());
        assert_eq!(instance.metadata["agentId"], agent_id.as_str());
    }
    let home = fake.host.machines().by_id[&id].home.clone();
    assert!(home.join(".intent").join("config.toml").exists());

    // While it runs, an exec renews activity (every 10 minutes = 1 s).
    timeout(common::test_timeout(Duration::from_secs(30)), async {
        while !fake.execs_of(&id).iter().any(|c| c.ends_with("true")) {
            keep_alive(&mut rpc_ws).await;
            // timing-guard: poll interval for the renewal (every 10 minutes = 1 s)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the backend renews the instance's activity");

    // Idle: it sleeps; a message wakes it, memory kept, no notice.
    ok(
        &mut rpc_ws,
        60,
        "cloudMachines.configure",
        json!({ "cloudMachineId": entry, "idlePauseMinutes": 1 }),
    )
    .await;
    machine_until(&mut rpc_ws, 61, &id, |m| {
        m.is_some_and(|m| m["cloud"]["state"] == "paused")
    })
    .await;
    assert_eq!(fake.api().instance(&id).unwrap().status, "sleeping");
    ok(
        &mut rpc_ws,
        70,
        "cloudMachines.configure",
        json!({ "cloudMachineId": entry, "idlePauseMinutes": 1440 }),
    )
    .await;
    let woke = echoed(&mut rpc_ws, agent, "after the sleep", 71).await;
    assert!(
        !woke.contains(DISK_NOTICE),
        "a memory restore needs no notice: {woke}"
    );
    assert!(fake
        .server
        .lines()
        .contains(&format!("POST /instances/{id}/start")));
    let recalled = echoed(&mut rpc_ws, agent, "recall first turn", 75).await;
    assert!(recalled.contains("recall first turn: yes"), "{recalled}");

    // A wake that boots fresh: intentd starts again from the disk, the
    // machine keeps its identity, and the agent is told once.
    ok(
        &mut rpc_ws,
        80,
        "cloudMachines.pause",
        json!({ "machineId": machine_id }),
    )
    .await;
    fake.api().instance_mut(&id).unwrap().wake_fresh = true;
    let fresh = echoed(&mut rpc_ws, agent, "after a fresh boot", 81).await;
    assert!(fresh.contains(DISK_NOTICE), "{fresh}");
    assert_eq!(fresh.matches("Note from Intent").count(), 1, "{fresh}");
    let recalled = echoed(&mut rpc_ws, agent, "recall after a fresh boot", 85).await;
    assert!(
        recalled.contains("recall after a fresh boot: yes"),
        "{recalled}"
    );
    let same = machine_until(&mut rpc_ws, 90, &id, |m| m.is_some())
        .await
        .unwrap();
    assert_eq!(
        same["id"],
        machine_id.as_str(),
        "the identity is kept on the disk"
    );

    // Removing the entry with its agents deletes the instance.
    let removed = ok(
        &mut rpc_ws,
        100,
        "cloudMachines.remove",
        json!({ "cloudMachineId": entry, "stopAgents": true }),
    )
    .await;
    assert_eq!(removed["ok"], true, "{removed}");
    machine_until(&mut rpc_ws, 101, &id, |m| m.is_none()).await;
    assert!(fake.api().instances.is_empty());
    assert!(fake
        .server
        .lines()
        .contains(&format!("DELETE /instances/{id}")));
    for request in fake.server.requests() {
        let exec = request.path.ends_with("/exec");
        assert_eq!(
            request.header("x-agent37-key").is_some(),
            exec,
            "{}",
            request.line()
        );
        if exec {
            assert!(
                request.json().get("user").is_none(),
                "every command runs as the image's user"
            );
        }
    }

    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    let mut secrets = vec![A37_KEY.to_owned(), PROFILE_SECRET.to_owned()];
    secrets.extend(fake.executor_tokens());
    for secret in &secrets {
        assert!(!log.contains(secret.as_str()), "a secret reached the log");
    }
    let _ = port;
    let _ = cfg;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_on_an_agent37_sandbox_sleeps_wakes_and_is_removed() {
    life("default", "us", "itd-a37-").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_on_an_agent37_vm_in_eu_sleeps_wakes_and_is_removed() {
    life("vm", "eu", "itd-a37vm-").await;
}

/// The reaper, with one settings minute of 100 ms, deletes an instance with
/// this backend's metadata that no machine knows, and leaves another
/// backend's alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reaper_deletes_this_backends_leftover_agent37_instances() {
    let Some((fake, _head, _port, _cfg, _ws_id, _keyed, mut rpc_ws)) =
        backend("itd-a37-reap-").await
    else {
        return;
    };
    let backend_info = ok(&mut rpc_ws, 3, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend_info["backendId"].as_str().unwrap().to_owned();
    ok(
        &mut rpc_ws,
        4,
        "cloudMachines.configure",
        agent37_params(&json!({ "size": "2x4" })),
    )
    .await;
    let template = intent_nodes::agent37::template_name(&format!(
        "{IMAGE_REPO}:{}",
        env!("CARGO_PKG_VERSION")
    ));
    let seed = |backend: &str| {
        let mut api = fake.api();
        let key = api.key.clone();
        let reply = api.handle(
            "POST",
            "/instances",
            Some(&key),
            Some(&json!({
                "template": template, "resources": { "cpu": 2, "memory": 4, "disk": 4 },
                "metadata": { "intentBackendId": backend, "agentId": "gone" },
            })),
        );
        assert_eq!(reply.status, 201, "{reply:?}");
        let id = reply.body["id"].as_str().unwrap().to_owned();
        // Made long ago, past the reap grace; it runs no processes here.
        api.instance_mut(&id).unwrap().created = 1_700_000_000;
        api.drain_events();
        id
    };
    let mine = seed(&backend_id);
    let theirs = seed("another-backend");
    timeout(common::test_timeout(Duration::from_secs(60)), async {
        while fake.api().instance(&mine).is_some() {
            keep_alive(&mut rpc_ws).await;
            // timing-guard: poll interval for the reaper (every 10 minutes = 1 s)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("this backend's leftover is reaped");
    assert!(fake.api().instance(&theirs).is_some());
}
