//! Vercel Sandbox cloud machines end to end, against the shared fake
//! Vercel (`intent_nodes::vercel::fake_api`, held to the real API by its
//! contract test) behind its HTTP server. Each sandbox runs its commands
//! for real in its own folder (its disk), so its "machine" is a real
//! `intentd serve` that the backend's own start script starts. A stop ends
//! every process of the sandbox and keeps the folder, as Vercel's stop
//! (a disk snapshot) does; a resume starts a new session with none. The
//! backend dials the daemon directly (`INTENTD_E2B_DIRECT_HOST`): a test
//! cannot join the tailcat relay.
//!
//! Proves: `configure` refuses a wrong token, a team the token cannot
//! reach, a missing project or image, and members and sizes Vercel entries
//! do not take, before any save; `target: "cloud"` starts an agent on a new
//! persistent sandbox of the entry's size, tagged with the backend; a plan
//! that refuses the size fails the start with `cloud-quota` `plan`; the
//! backend extends the session while the sandbox runs; at the plan's
//! session cap it stops and resumes the sandbox when idle; it stops it when
//! idle, and a new message resumes it from the disk with the conversation
//! kept and the disk notice once; a session Vercel ended while the backend
//! was away is marked paused and resumed on demand; removing the entry with
//! `stopAgents` deletes the sandbox with its snapshots; the token never
//! reaches the backend's log or a command.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use intent_nodes::vercel::fake_api::{CommandStart, Event, VercelApi};
use intent_nodes::vercel::fake_server::{FakeServer, Host, Ran};
use intentd_test_support::ProcessGroup;

use super::*;

const VERCEL_TOKEN: &str = "e2e-vercel-token-0000000000000000";
const TEAM: &str = "team_e2e00000000000000000000";
const PROJECT: &str = "prj_e2e000000000000000000000000";
const IMAGE_REPO: &str = "sutterhill/intent-images/intent-node";
/// The words of the notice a launch after a resume carries.
const DISK_NOTICE: &str = "stopped and started again from its disk";

/// One sandbox's folder (its disk), environment and current processes.
struct Machine {
    home: std::path::PathBuf,
    env: Vec<(String, String)>,
    group: Arc<ProcessGroup>,
}

#[derive(Default)]
struct Machines {
    by_name: HashMap<String, Machine>,
    /// Every group ever made, ended when the fake drops.
    groups: Vec<Arc<ProcessGroup>>,
    /// Every command, by sandbox.
    commands: Vec<(String, CommandStart)>,
    /// Every stdout, to look for the executor token.
    outputs: Vec<String>,
}

/// Runs each sandbox's commands in its folder, and its lifecycle on its
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

    /// The sandbox's environment: a hermetic daemon fixture, the codex
    /// toolchain that runs the node fixture, this test's intentd first on
    /// PATH, and `HOME` on its disk, as Vercel runs commands as the
    /// image's user.
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
        // One port for the sandbox's life: a restarted daemon must listen
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

    fn new_group(&self, name: &str) {
        let fresh = Arc::new(ProcessGroup::new().unwrap());
        let mut machines = self.machines();
        if let Some(machine) = machines.by_name.get_mut(name) {
            machine.group = fresh.clone();
        }
        machines.groups.push(fresh);
    }
}

impl Host for LocalHost {
    fn run(&self, start: CommandStart) -> Pin<Box<dyn Future<Output = Ran> + Send + '_>> {
        Box::pin(async move {
            let Some((home, env, pgid)) = self
                .machines()
                .by_name
                .get(&start.sandbox)
                .map(|m| (m.home.clone(), m.env.clone(), m.group.pgid()))
            else {
                return (127, String::new(), "no such sandbox".into());
            };
            self.machines()
                .commands
                .push((start.sandbox.clone(), start.clone()));
            let output = tokio::process::Command::new(&start.command)
                .args(&start.args)
                .current_dir(start.cwd.as_deref().map_or(home.clone(), Into::into))
                .env_clear()
                .envs(env)
                .envs(start.env.clone())
                .process_group(pgid)
                .stdin(Stdio::null())
                .output()
                .await
                .unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
            self.machines().outputs.push(stdout.clone());
            (
                output.status.code().unwrap_or(-1),
                stdout,
                String::from_utf8_lossy(&output.stderr).into_owned(),
            )
        })
    }

    fn events(&self, events: Vec<Event>) {
        for event in events {
            match event {
                Event::Created(name) => {
                    let home = self.root.join(&name);
                    std::fs::create_dir_all(&home).unwrap();
                    let env = self.machine_env(&home);
                    let group = Arc::new(ProcessGroup::new().unwrap());
                    let mut machines = self.machines();
                    machines.groups.push(group.clone());
                    machines.by_name.insert(name, Machine { home, env, group });
                }
                // The session ends: every process with it; the disk stays.
                Event::Stopped(name) => {
                    if let Some(machine) = self.machines().by_name.get(&name) {
                        machine.group.kill();
                    }
                }
                Event::Resumed(name) => self.new_group(&name),
                Event::Deleted(name) => {
                    if let Some(machine) = self.machines().by_name.remove(&name) {
                        machine.group.terminate(stop_grace());
                    }
                }
            }
        }
    }
}

/// The fake Vercel and the scratch dir it owns; its `Drop` ends every
/// sandbox's processes before that dir goes.
struct FakeVercel {
    server: FakeServer,
    host: Arc<LocalHost>,
    scratch: tempfile::TempDir,
}

impl Drop for FakeVercel {
    fn drop(&mut self) {
        for group in &self.host.machines().groups {
            group.terminate(stop_grace());
        }
    }
}

impl FakeVercel {
    async fn start_in(scratch: tempfile::TempDir, fixture: &str) -> Self {
        let root = scratch.path().join("vercel");
        let shim_dir = scratch.path().join("vc-bin");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&shim_dir).unwrap();
        let shim = shim_dir.join("setsid");
        std::fs::write(&shim, SETSID_SHIM).unwrap();
        std::fs::set_permissions(&shim, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
        let host = Arc::new(LocalHost {
            root,
            shim_dir,
            fixture: fixture.to_owned(),
            machines: Mutex::default(),
        });
        let server =
            FakeServer::start(VercelApi::new(VERCEL_TOKEN, TEAM, PROJECT), host.clone()).await;
        Self {
            server,
            host,
            scratch,
        }
    }

    fn api(&self) -> std::sync::MutexGuard<'_, VercelApi> {
        self.server.state()
    }

    fn only_sandbox(&self) -> Option<String> {
        self.api().sandboxes.first().map(|s| s.name.clone())
    }

    fn session_of(&self, name: &str) -> String {
        self.api().sandbox(name).unwrap().session.id.clone()
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

    fn count(&self, suffix: &str) -> usize {
        self.server
            .lines()
            .iter()
            .filter(|l| l.ends_with(suffix))
            .count()
    }
}

fn vercel_params(extra: &Value) -> Value {
    let mut params = json!({
        "provider": "vercel", "name": "Vercel", "apiKey": VERCEL_TOKEN,
        "teamId": TEAM, "projectId": PROJECT,
    });
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

/// A backend against a fake Vercel, with one settings minute of 100 ms,
/// and an API-key profile. `None` (the test skips) without node or git.
async fn backend(
    prefix: &str,
) -> Option<(
    FakeVercel,
    common::DaemonGuard,
    String,
    String,
    common::TlsWs,
)> {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping Vercel e2e: node or git not on PATH");
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
    let fake = FakeVercel::start_in(scratch, &fixture).await;
    let head_dir = fake.scratch.path().join("head");
    let url = fake.server.url.clone();
    let (head, port, cfg, ws_id) = start_backend_with(
        &head_dir,
        &repo,
        &[
            ("INTENTD_VERCEL_API_URL", &url),
            ("INTENTD_VERCEL_REGISTRY_URL", &url),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
            ("INTENTD_E2B_DAEMON_WAIT_SECS", "30"),
            ("INTENTD_E2B_MINUTE_MS", "100"),
        ],
    )
    .await;
    let mut rpc_ws = connect(port, cfg).await;
    ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "vercel-life", "name": "Vercel lifecycle" }),
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
    Some((fake, head, ws_id, keyed_ref, rpc_ws))
}

/// Configure refusals, before anything is saved.
async fn configure_refusals(fake: &FakeVercel, rpc_ws: &mut common::TlsWs) {
    let rejected = refused(
        rpc_ws,
        10,
        "cloudMachines.configure",
        vercel_params(&json!({ "apiKey": "wrong-vercel-token" })),
    )
    .await;
    assert_eq!(rejected["data"]["reason"], "key-rejected", "{rejected}");
    assert_eq!(rejected["data"]["provider"], "vercel", "{rejected}");
    let team = refused(
        rpc_ws,
        11,
        "cloudMachines.configure",
        vercel_params(&json!({ "teamId": "team_other0000" })),
    )
    .await;
    assert_eq!(team["data"]["reason"], "permission-denied", "{team}");
    assert_eq!(team["data"]["action"], "list-sandboxes", "{team}");
    let project = refused(
        rpc_ws,
        12,
        "cloudMachines.configure",
        vercel_params(&json!({ "projectId": "prj_other0000" })),
    )
    .await;
    assert_eq!(project["data"]["reason"], "project-missing", "{project}");
    let image = refused(
        rpc_ws,
        13,
        "cloudMachines.configure",
        vercel_params(&json!({ "image": "sutterhill/intent-images/missing-node" })),
    )
    .await;
    assert_eq!(image["data"]["reason"], "template-missing", "{image}");
    for (n, extra) in [
        json!({ "teamId": "sutterhill" }),
        json!({ "projectId": "intent-updates" }),
        json!({ "image": "sutterhill/intent-node" }),
        json!({ "image": "sutterhill/intent-images/intent-node:1" }),
        json!({ "size": "4x16" }),
        json!({ "size": "16x32" }),
        json!({ "diskGiB": 20 }),
        json!({ "instanceType": "vm" }),
        json!({ "plans": {} }),
        json!({ "templateProject": "intent" }),
        json!({ "teamId": null }),
    ]
    .into_iter()
    .enumerate()
    {
        let mut params = vercel_params(&extra);
        if extra.get("teamId").is_some_and(Value::is_null) {
            params.as_object_mut().unwrap().remove("teamId");
        }
        let invalid = refused(
            rpc_ws,
            20 + i64::try_from(n).unwrap(),
            "cloudMachines.configure",
            params,
        )
        .await;
        assert_eq!(invalid["code"], -32602, "{extra}: {invalid}");
    }
    let checked = ok(
        rpc_ws,
        40,
        "cloudMachines.configure",
        vercel_params(&json!({ "validateOnly": true })),
    )
    .await;
    assert_eq!(checked["provider"]["id"], "", "{checked}");
    let listed = ok(rpc_ws, 41, "cloudMachines.list", json!({})).await;
    assert_eq!(
        listed["providers"],
        json!([]),
        "nothing was saved: {listed}"
    );
    assert_eq!(
        listed["providerKinds"],
        json!(["e2b", "northflank", "agent37", "vercel"])
    );
    assert!(fake.api().sandboxes.is_empty());
}

/// The life of one agent on a Vercel sandbox.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_on_a_vercel_sandbox_stops_resumes_from_disk_and_is_removed() {
    let Some((fake, _head, ws_id, keyed_ref, mut rpc_ws)) = backend("itd-vc-").await else {
        return;
    };
    let head_dir = fake.scratch.path().join("head");
    let image = format!("{IMAGE_REPO}:{}", env!("CARGO_PKG_VERSION"));
    configure_refusals(&fake, &mut rpc_ws).await;

    let saved = ok(
        &mut rpc_ws,
        50,
        "cloudMachines.configure",
        vercel_params(&json!({ "size": "2x4", "idlePauseMinutes": 1440 })),
    )
    .await;
    let provider = &saved["provider"];
    let entry = provider["id"].as_str().unwrap().to_owned();
    assert!(entry.starts_with("vercel-"), "{provider}");
    assert_eq!(provider["provider"], "vercel");
    assert_eq!(provider["template"], image.as_str());
    assert!(provider.get("templateProject").is_none(), "{provider}");
    assert_eq!(
        provider["vercel"],
        json!({ "teamId": TEAM, "projectId": PROJECT, "image": IMAGE_REPO })
    );
    assert_eq!(
        provider["sizes"],
        json!([
            { "id": "2x4", "vcpu": 2, "memoryMiB": 4096 },
            { "id": "4x8", "vcpu": 4, "memoryMiB": 8192 },
            { "id": "8x16", "vcpu": 8, "memoryMiB": 16384 },
        ])
    );

    // A plan that allows fewer vCPUs refuses the start: cloud-quota plan,
    // and no smaller size in its place.
    fake.api().max_vcpus = 1;
    let created = ok(
        &mut rpc_ws,
        60,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Small plan", "model": "default", "provider": keyed_ref,
            "placement": { "target": "cloud" },
        }),
    )
    .await;
    let small = created["agent"]["id"].as_str().unwrap().to_owned();
    assert_eq!(
        created["agent"]["placement"]["cloudMachineId"],
        entry.as_str()
    );
    let failed = agent_until(&mut rpc_ws, (ws_id.as_str(), small.as_str()), 61, |a| {
        a.get("placementError").is_some()
    })
    .await;
    let error = &failed["placementError"];
    assert_eq!(error["code"], "cloud-quota", "{failed}");
    assert_eq!(error["data"]["limit"], "plan", "{failed}");
    assert!(fake.api().sandboxes.is_empty());
    fake.api().max_vcpus = 8;

    // An agent starts on a new persistent sandbox of the entry.
    let created = ok(
        &mut rpc_ws,
        70,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Life", "model": "default", "provider": keyed_ref,
            "placement": { "target": "cloud" },
        }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    let agent = (ws_id.as_str(), agent_id.as_str());
    let first = echoed(&mut rpc_ws, agent, "first turn", 71).await;
    assert!(!first.contains(DISK_NOTICE), "{first}");
    let name = fake.only_sandbox().unwrap();
    let machine = machine_until(&mut rpc_ws, 80, &name, |m| m.is_some())
        .await
        .unwrap();
    let machine_id = machine["id"].as_str().unwrap().to_owned();
    assert_eq!(machine["cloud"]["provider"], "vercel", "{machine}");
    assert_eq!(machine["cloud"]["template"], image.as_str(), "{machine}");
    assert!(machine["cloud"].get("volumeId").is_none(), "{machine}");
    {
        let api = fake.api();
        let sandbox = api.sandbox(&name).unwrap();
        assert_eq!(sandbox.vcpus, 2);
        assert!(sandbox.persistent);
        assert_eq!(sandbox.image, image);
        assert_eq!(sandbox.timeout, 30 * 60 * 1000);
        assert_eq!(sandbox.tags.len(), 5, "{:?}", sandbox.tags);
        assert!(sandbox.tags["intentBackendId"].is_string());
        assert_eq!(sandbox.tags["agentId"], agent_id.as_str());
        assert_eq!(
            sandbox.keep_last_snapshots,
            json!({ "count": 1, "deleteEvicted": true, "expiration": intent_nodes::vercel::SNAPSHOT_EXPIRATION_MS })
        );
    }

    // While it runs, the session is extended before it ends.
    fake.api().sandbox_mut(&name).unwrap().session.timeout = 60_000;
    timeout(common::test_timeout(Duration::from_secs(30)), async {
        while fake.count("/extend-timeout") == 0 {
            keep_alive(&mut rpc_ws).await;
            // timing-guard: poll interval for the renewal (every 10 minutes = 1 s)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the backend extends the session");

    // At the plan's session cap, idle: stopped and resumed at once, from
    // the disk; the next turn is told.
    let before = fake.session_of(&name);
    {
        let mut api = fake.api();
        let sandbox = api.sandbox_mut(&name).unwrap();
        sandbox.session.timeout = 60_000;
        api.max_session_ms = 60_000;
    }
    timeout(common::test_timeout(Duration::from_secs(60)), async {
        while fake.session_of(&name) == before
            || fake.api().sandbox(&name).unwrap().session.status != "running"
        {
            keep_alive(&mut rpc_ws).await;
            // timing-guard: poll interval for the planned stop and resume
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the backend stops and resumes a capped session");
    fake.api().max_session_ms = intent_nodes::vercel::fake_api::PRO_MAX_SESSION_MS;
    machine_until(&mut rpc_ws, 90, &name, |m| {
        m.is_some_and(|m| m["cloud"]["state"] == "running")
    })
    .await;
    let capped = echoed(&mut rpc_ws, agent, "after the cap", 95).await;
    assert!(capped.contains(DISK_NOTICE), "{capped}");
    assert_eq!(capped.matches("Note from Intent").count(), 1, "{capped}");

    // Idle: it stops; a message resumes it from the disk, the
    // conversation kept, and the agent told once.
    ok(
        &mut rpc_ws,
        100,
        "cloudMachines.configure",
        json!({ "cloudMachineId": entry, "idlePauseMinutes": 1 }),
    )
    .await;
    machine_until(&mut rpc_ws, 101, &name, |m| {
        m.is_some_and(|m| m["cloud"]["state"] == "paused")
    })
    .await;
    {
        let api = fake.api();
        let sandbox = api.sandbox(&name).unwrap();
        assert_eq!(sandbox.session.status, "stopped");
        assert!(sandbox.snapshot.is_some());
    }
    ok(
        &mut rpc_ws,
        110,
        "cloudMachines.configure",
        json!({ "cloudMachineId": entry, "idlePauseMinutes": 1440 }),
    )
    .await;
    let resumed = echoed(&mut rpc_ws, agent, "after the stop", 111).await;
    assert!(resumed.contains(DISK_NOTICE), "{resumed}");
    assert_eq!(resumed.matches("Note from Intent").count(), 1, "{resumed}");
    let recalled = echoed(&mut rpc_ws, agent, "recall first turn", 115).await;
    assert!(recalled.contains("recall first turn: yes"), "{recalled}");
    let same = machine_until(&mut rpc_ws, 120, &name, |m| m.is_some())
        .await
        .unwrap();
    assert_eq!(
        same["id"],
        machine_id.as_str(),
        "the identity is kept on the disk"
    );

    // Vercel ended the session while the backend was away: marked paused,
    // resumed on demand.
    let before = fake.session_of(&name);
    fake.api().time_out(&name);
    machine_until(&mut rpc_ws, 130, &name, |m| {
        m.is_some_and(|m| m["cloud"]["state"] == "paused") || fake.session_of(&name) != before
    })
    .await;
    let back = echoed(&mut rpc_ws, agent, "after the timeout", 140).await;
    assert!(back.contains(DISK_NOTICE), "{back}");

    // Removing the entry with its agents deletes the sandbox.
    let removed = ok(
        &mut rpc_ws,
        150,
        "cloudMachines.remove",
        json!({ "cloudMachineId": entry, "stopAgents": true }),
    )
    .await;
    assert_eq!(removed["ok"], true, "{removed}");
    machine_until(&mut rpc_ws, 151, &name, |m| m.is_none()).await;
    assert!(fake.api().sandboxes.is_empty());
    let delete = fake
        .server
        .requests()
        .into_iter()
        .find(|r| r.method == "DELETE")
        .unwrap();
    assert!(
        delete.path.contains("deleteOrphanSnapshots=true"),
        "{}",
        delete.path
    );

    for request in fake.server.requests() {
        assert!(!request.path.contains(VERCEL_TOKEN), "{}", request.line());
        if request.path.contains("/cmd") {
            let body = request.json();
            assert!(body.get("sudo").is_none(), "never sudo");
            assert!(
                !request
                    .body
                    .windows(VERCEL_TOKEN.len())
                    .any(|w| w == VERCEL_TOKEN.as_bytes()),
                "the token never reaches a command"
            );
        }
    }
    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    let mut secrets = vec![VERCEL_TOKEN.to_owned(), PROFILE_SECRET.to_owned()];
    secrets.extend(fake.executor_tokens());
    for secret in &secrets {
        assert!(!log.contains(secret.as_str()), "a secret reached the log");
    }
}

/// The reaper, with one settings minute of 100 ms, deletes a sandbox with
/// this backend's tag that no machine knows, and leaves another backend's
/// alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reaper_deletes_this_backends_leftover_vercel_sandboxes() {
    let Some((fake, _head, _ws_id, _keyed, mut rpc_ws)) = backend("itd-vc-reap-").await else {
        return;
    };
    let backend_info = ok(&mut rpc_ws, 3, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend_info["backendId"].as_str().unwrap().to_owned();
    ok(
        &mut rpc_ws,
        4,
        "cloudMachines.configure",
        vercel_params(&json!({ "size": "2x4" })),
    )
    .await;
    let image = format!("{IMAGE_REPO}:{}", env!("CARGO_PKG_VERSION"));
    let seed = |backend: &str, name: &str| {
        let mut api = fake.api();
        let token = api.token.clone();
        let reply = api.handle(
            "POST",
            &format!("/v3/sandboxes?teamId={TEAM}"),
            Some(&token),
            Some(&json!({
                "projectId": PROJECT, "name": name, "image": image, "persistent": true,
                "resources": { "vcpus": 2 },
                "tags": { "intentBackendId": backend, "agentId": "gone" },
            })),
        );
        assert_eq!(reply.status, 200, "{reply:?}");
        // Made long ago, past the reap grace; it runs no processes here.
        api.sandbox_mut(name).unwrap().created_at = 1_700_000_000_000;
        api.drain_events();
    };
    seed(&backend_id, "intent-leftover0001");
    seed("another-backend", "intent-theirs00001");
    timeout(common::test_timeout(Duration::from_secs(60)), async {
        while fake.api().sandbox("intent-leftover0001").is_some() {
            keep_alive(&mut rpc_ws).await;
            // timing-guard: poll interval for the reaper (every 10 minutes = 1 s)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("this backend's leftover is reaped");
    assert!(fake.api().sandbox("intent-theirs00001").is_some());
}
