//! Northflank cloud machines end to end, against a fake Northflank: its
//! REST API and command streams from the shared model
//! (`intent_nodes::northflank::fake_api`, held to a recording of the real
//! API by its contract tests), its command-exec WebSocket and a public
//! image registry. Each
//! service's "container" is a real `intentd serve` that the backend's own
//! start script starts, with the service's volume as its home folder. A
//! pause stops every process of the container and keeps the volume, as a
//! scale to 0 does; a resume starts nothing, as the image only idles.
//!
//! Proves: `configure` tests the project, the plans, the image and the
//! service list before it saves a Northflank entry, and maps each refusal
//! to its documented error; `target: "cloud"` starts an agent on a new
//! service of that entry (one volume, the size's plan, the backend's tags)
//! with a resolved `target: "e2b"`; a pause and resume, and a container
//! restart Northflank does on its own, keep the machine and its identity
//! and tell the agent's next turn once what its disk kept and lost, with
//! its conversation loaded from the disk;
//! `cloudMachines.kill` deletes the service with its volume; the token never
//! reaches the backend's log.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use intent_nodes::northflank::fake_api::{self, NorthflankApi};
use intentd_test_support::ProcessGroup;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;

/// A JWT about as long as a real Northflank API token (well over 1 KB).
static NF_TOKEN: LazyLock<String> = LazyLock::new(|| {
    let claims = "eyJ0b2tlbklkIjoibmYtdGVzdCIsInRlYW1JZCI6ImludGVudCIsInNjb3BlcyI6W10s".repeat(16);
    let signature = "Qm9ndXNTaWduYXR1cmVGb3JUaGVGYWtlTm9ydGhmbGFua0FQSTAxMjM0NTY3ODk".repeat(6);
    format!("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.{claims}.{signature}")
});
const PROJECT: &str = "intent-cloud";
/// A project the token can see but not read.
const LOCKED_PROJECT: &str = "locked-project";
const REGION: &str = "europe-west";
const IMAGE_REPO: &str = "ghcr.io/intent-hq/intent-node";
/// The words of the notice a launch after a disk resume carries.
const DISK_NOTICE: &str = "stopped and started again from its disk";

/// The test cannot become root or have a `user` account: `id` answers the
/// test's own ids for `user`, so the home is already owned.
const ID_SHIM: &str = "#!/bin/sh\nif [ \"$#\" -eq 2 ] && [ \"$2\" = user ]; then exec /usr/bin/id \"$1\"; fi\nexec /usr/bin/id \"$@\"\n";
/// `setpriv` drops nothing here: it runs the command after `--`.
const SETPRIV_SHIM: &str = "#!/bin/sh\nwhile [ $# -gt 0 ]; do\n  case \"$1\" in --) shift; break ;; *) shift ;; esac\ndone\nexec \"$@\"\n";

/// One fake service's container: its volume (the container's home folder),
/// the environment its commands run with, and the process group of its
/// current container (replaced at each start, since a stop ends every
/// process). What Northflank stores and returns of it is in `api`.
struct FakeService {
    home: std::path::PathBuf,
    env: Vec<(String, String)>,
    group: Arc<ProcessGroup>,
}

struct FakeState {
    /// The REST API: projects, plans, tags, volumes and services.
    api: NorthflankApi,
    /// Containers of the services `api` holds.
    services: HashMap<String, FakeService>,
    /// Every create request body: (path, body).
    creates: Vec<(String, Value)>,
    /// Every lifecycle call: (method, path).
    calls: Vec<(String, String)>,
    /// Every exec init frame's user and argv.
    execs: Vec<(String, Vec<String>)>,
    /// Every stdout a command printed, to look for the executor token.
    outputs: Vec<String>,
    /// While set, exec answers this init auth instead of "successful".
    exec_refusal: Option<String>,
    groups: Vec<Arc<ProcessGroup>>,
}

impl Default for FakeState {
    fn default() -> Self {
        Self {
            api: NorthflankApi::new(PROJECT, REGION),
            services: HashMap::new(),
            creates: Vec::new(),
            calls: Vec::new(),
            execs: Vec::new(),
            outputs: Vec::new(),
            exec_refusal: None,
            groups: Vec::new(),
        }
    }
}

/// The fake Northflank API and registry on one port, its command-exec
/// WebSocket on another. It owns the test's scratch dir; its `Drop` ends
/// every container's processes before that dir goes.
struct FakeNorthflank {
    url: String,
    exec_url: String,
    state: Arc<Mutex<FakeState>>,
    root: std::path::PathBuf,
    shim_dir: std::path::PathBuf,
    fixture: String,
    scratch: tempfile::TempDir,
}

impl Drop for FakeNorthflank {
    fn drop(&mut self) {
        for group in &self.state().groups {
            group.terminate(stop_grace());
        }
    }
}

impl FakeNorthflank {
    async fn start(scratch: tempfile::TempDir, fixture: &str) -> Arc<Self> {
        let root = scratch.path().join("northflank");
        std::fs::create_dir_all(&root).unwrap();
        let shim_dir = scratch.path().join("nf-bin");
        std::fs::create_dir_all(&shim_dir).unwrap();
        for (name, body) in [
            ("setsid", SETSID_SHIM),
            ("id", ID_SHIM),
            ("setpriv", SETPRIV_SHIM),
        ] {
            let shim = shim_dir.join(name);
            std::fs::write(&shim, body).unwrap();
            std::fs::set_permissions(&shim, std::os::unix::fs::PermissionsExt::from_mode(0o755))
                .unwrap();
        }
        let rest = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let exec = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let fake = Arc::new(Self {
            url: format!("http://{}", rest.local_addr().unwrap()),
            exec_url: format!("ws://{}", exec.local_addr().unwrap()),
            state: Arc::default(),
            root,
            shim_dir,
            fixture: fixture.to_owned(),
            scratch,
        });
        let served = Arc::downgrade(&fake);
        tokio::spawn(async move {
            while let Ok((stream, _)) = rest.accept().await {
                let Some(fake) = served.upgrade() else { return };
                tokio::spawn(async move { fake.serve(stream).await });
            }
        });
        let served = Arc::downgrade(&fake);
        tokio::spawn(async move {
            while let Ok((stream, _)) = exec.accept().await {
                let Some(fake) = served.upgrade() else { return };
                tokio::spawn(async move { fake.exec(stream).await });
            }
        });
        fake
    }

    fn scratch(&self) -> &Path {
        self.scratch.path()
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().unwrap()
    }

    /// The only service, once there is one.
    fn service_id(&self) -> Option<String> {
        self.state().services.keys().next().cloned()
    }

    fn calls_for(&self, id: &str) -> Vec<(String, String)> {
        self.state()
            .calls
            .iter()
            .filter(|(_, path)| path.split('/').any(|part| part == id))
            .cloned()
            .collect()
    }

    /// A service with its volume, as an earlier run left it: created
    /// `created_at`, with `tags` as team tags and `runtime_env`, paused.
    fn seed_service(&self, id: &str, tags: &[String], runtime_env: &Value, created_at: &str) {
        let home = self.root.join(id);
        std::fs::create_dir_all(&home).unwrap();
        let group = Arc::new(ProcessGroup::new().unwrap());
        let request = json!({
            "name": id,
            "description": "Intent cloud machine",
            "tags": tags,
            "deployment": {
                "instances": 1,
                "external": { "imagePath": format!("{IMAGE_REPO}:{}", env!("CARGO_PKG_VERSION")) },
            },
            "runtimeEnvironment": runtime_env,
        });
        let mut state = self.state();
        state.groups.push(group.clone());
        state.api.seed(id, request, true, created_at);
        state.services.insert(
            id.to_owned(),
            FakeService {
                env: self.container_env(&home),
                home,
                group,
            },
        );
    }

    /// Northflank restarts the container on its own: every process ends,
    /// the volume stays, and the image starts nothing.
    fn restart_container(&self, id: &str) {
        let mut state = self.state();
        let Some(service) = state.services.get_mut(id) else {
            return;
        };
        service.group.kill();
        let group = Arc::new(ProcessGroup::new().unwrap());
        service.group = group.clone();
        state.groups.push(group);
    }

    async fn serve(&self, mut stream: tokio::net::TcpStream) {
        let mut buf = Vec::new();
        let mut chunk = [0_u8; 8192];
        let header_end = loop {
            let Ok(n) = stream.read(&mut chunk).await else {
                return;
            };
            if n == 0 {
                return;
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
        let mut lines = head.lines();
        let mut first = lines.next().unwrap_or_default().split(' ');
        let method = first.next().unwrap_or_default().to_owned();
        let target = first.next().unwrap_or_default().to_owned();
        let headers: HashMap<String, String> = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
            .collect();
        let length: usize = headers
            .get("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        while buf.len() < header_end + length {
            let Ok(n) = stream.read(&mut chunk).await else {
                return;
            };
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        let body = buf[header_end..].to_vec();
        let (status, payload) = self.route(&method, &target, &headers, &body);
        let reply = format!(
            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload.len()
        );
        let _ = stream.write_all(reply.as_bytes()).await;
        let _ = stream.write_all(&payload).await;
        let _ = stream.shutdown().await;
    }

    fn route(
        &self,
        method: &str,
        target: &str,
        headers: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, Vec<u8>) {
        let reply = |status: u16, value: Value| (status, value.to_string().into_bytes());
        let error = |status: u16, message: &str| {
            reply(
                status,
                json!({ "error": { "status": status, "message": message } }),
            )
        };
        let path = target.split('?').next().unwrap_or_default();
        // The public registry: no credentials.
        if let Some(manifest) = path.strip_prefix("/v2/") {
            let expected = format!(
                "{}/manifests/{}",
                IMAGE_REPO.trim_start_matches("ghcr.io/"),
                env!("CARGO_PKG_VERSION")
            );
            return if method == "HEAD" && manifest == expected {
                (200, Vec::new())
            } else {
                error(404, "manifest unknown")
            };
        }
        if headers.get("authorization").map(String::as_str)
            != Some(&format!("Bearer {}", *NF_TOKEN))
        {
            return error(401, "Invalid API token");
        }
        if method != "GET" {
            self.state()
                .calls
                .push((method.to_owned(), path.to_owned()));
        }
        if path == format!("/projects/{LOCKED_PROJECT}") {
            return error(403, "Forbidden: read-project");
        }
        let request: Option<Value> = serde_json::from_slice(body).ok();
        let answer = self.state().api.handle(method, target, request.as_ref());
        if (200..300).contains(&answer.status) {
            self.container_side_effects(method, path, request.as_ref());
        }
        reply(answer.status, answer.body)
    }

    /// What a successful call does to the containers: a volume is a home
    /// folder, a service's container runs commands, a pause stops every
    /// process and a delete ends them.
    fn container_side_effects(&self, method: &str, path: &str, request: Option<&Value>) {
        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        let name = || {
            request
                .and_then(|r| r["name"].as_str())
                .unwrap_or_default()
                .to_owned()
        };
        match (method, parts.as_slice()) {
            ("POST", ["projects", _, "volumes"]) => {
                std::fs::create_dir_all(self.root.join(name())).unwrap();
                let request = request.cloned().unwrap_or_default();
                self.state().creates.push((path.to_owned(), request));
            }
            ("POST", ["projects", _, "services", "deployment"]) => {
                let name = name();
                let home = self.root.join(&name);
                let env = self.container_env(&home);
                let group = Arc::new(ProcessGroup::new().unwrap());
                let mut state = self.state();
                let request = request.cloned().unwrap_or_default();
                state.creates.push((path.to_owned(), request));
                state.groups.push(group.clone());
                state
                    .services
                    .insert(name, FakeService { home, env, group });
            }
            ("POST", ["projects", _, "services", id, "pause"]) => {
                let mut state = self.state();
                let Some(service) = state.services.get_mut(*id) else {
                    return;
                };
                service.group.kill();
                let group = Arc::new(ProcessGroup::new().unwrap());
                service.group = group.clone();
                state.groups.push(group);
            }
            ("DELETE", ["projects", _, "services", id]) => {
                if let Some(service) = self.state().services.remove(*id) {
                    service.group.terminate(stop_grace());
                }
            }
            _ => {}
        }
    }

    /// The command-exec WebSocket: one command per connection.
    async fn exec(&self, stream: tokio::net::TcpStream) {
        let mut service_id = String::new();
        #[expect(
            clippy::result_large_err,
            reason = "tungstenite's handshake callback type"
        )]
        let ws = tokio_tungstenite::accept_hdr_async(
            stream,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                request
                    .uri()
                    .path()
                    .rsplit('/')
                    .next()
                    .unwrap_or_default()
                    .clone_into(&mut service_id);
                Ok(response)
            },
        )
        .await;
        let Ok(mut ws) = ws else { return };
        let Some(Ok(Message::Text(init))) = ws.next().await else {
            return;
        };
        let init: Value = serde_json::from_str(&init).unwrap();
        let send = |value: Value| fake_api::message(&value);
        // Northflank answers a refused `init` with one frame and then keeps
        // the socket open: the client has to stop on that frame.
        let refused = if init["data"]["auth"]["apiToken"] != NF_TOKEN.as_str() {
            Some(fake_api::refused_frame(fake_api::Refusal::BadToken))
        } else if !self.state().services.contains_key(&service_id) {
            Some(fake_api::refused_frame(fake_api::Refusal::UnknownService(
                &service_id,
            )))
        } else {
            // A token without the exec permission: no recording has its
            // message, so the test names one.
            self.state()
                .exec_refusal
                .clone()
                .map(|message| fake_api::init_failed_frame(&message))
        };
        if let Some(frame) = refused {
            let _ = ws.send(send(frame)).await;
            while let Some(Ok(_)) = ws.next().await {}
            return;
        }
        let argv: Vec<String> =
            serde_json::from_value(init["data"]["context"]["command"].clone()).unwrap();
        let user = init["data"]["context"]["user"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        self.state().execs.push((user.clone(), argv.clone()));
        let _ = ws.send(send(fake_api::init_frame(&service_id))).await;
        let running = self
            .state()
            .services
            .get(&service_id)
            .map(|s| (s.home.clone(), s.env.clone(), s.group.pgid()));
        let Some((home, env, group)) = running else {
            return;
        };
        // The container's home folder is the volume: the same path the
        // adapter names, here the service's directory.
        let home_arg = format!("HOME={}", intent_nodes::northflank::HOME);
        let command: Vec<String> = argv
            .iter()
            .map(|arg| {
                if *arg == home_arg {
                    format!("HOME={}", home.display())
                } else {
                    arg.clone()
                }
            })
            .collect();
        let output = tokio::process::Command::new(&command[0])
            .args(&command[1..])
            .current_dir(&home)
            .env_clear()
            .envs(env)
            .process_group(group)
            .stdin(Stdio::null())
            .output()
            .await
            .unwrap();
        self.state()
            .outputs
            .push(String::from_utf8_lossy(&output.stdout).into_owned());
        let code = output.status.code().unwrap_or(-1);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let context = &init["data"]["context"];
        let frames = fake_api::exec_frames(&service_id, context, &stdout, &stderr, code);
        // The auth answer went before the command ran.
        for frame in frames.into_iter().skip(1) {
            let _ = ws.send(send(frame)).await;
        }
        let _ = ws.close(None).await;
    }

    /// A container's environment: a hermetic daemon fixture, the codex
    /// toolchain that runs the node fixture, and this test's intentd first
    /// on PATH. `HOME` comes from each command, as on Northflank.
    fn container_env(&self, home: &Path) -> Vec<(String, String)> {
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
        env.push((
            "INTENTD_WORKSPACES_DIR".into(),
            dir.join("workspaces").to_string_lossy().into_owned(),
        ));
        env.push(("INTENTD_AUTH_TOKEN".into(), TOKEN.into()));
        // The fixture keeps its sessions on the volume, as a provider keeps
        // its transcripts in the home folder.
        env.push((
            "NODE_TEST_AGENT_SESSIONS".into(),
            home.join(".node-test-agent").to_string_lossy().into_owned(),
        ));
        // One port for the container's life, as its settings would fix one:
        // a restarted daemon must listen where the machine row dials.
        env.retain(|(key, _)| key != "INTENTD_TCP_PORT");
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        env.push(("INTENTD_TCP_PORT".into(), port.to_string()));
        env
    }

    /// How many root setup steps ran so far.
    fn root_setups(&self) -> usize {
        self.state()
            .execs
            .iter()
            .filter(|(user, _)| user == "root")
            .count()
    }

    fn executor_tokens(&self) -> Vec<String> {
        self.state()
            .outputs
            .iter()
            .filter_map(|out| serde_json::from_str::<Value>(out).ok())
            .filter_map(|v| v["token"].as_str().map(str::to_owned))
            .collect()
    }
}

fn northflank_params(extra: &Value) -> Value {
    let mut params = json!({ "provider": "northflank", "name": "Northflank", "apiKey": NF_TOKEN.as_str(), "projectId": PROJECT });
    for (k, v) in extra.as_object().unwrap() {
        params[k] = v.clone();
    }
    params
}

/// The error data of a refused call.
async fn refused(rpc_ws: &mut common::TlsWs, id: i64, method: &str, params: Value) -> Value {
    let reply = rpc(rpc_ws, id, method, params).await;
    assert!(reply.get("error").is_some(), "{reply}");
    reply["error"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_runs_on_a_northflank_machine_that_resumes_from_its_disk() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping Northflank e2e: node or git not on PATH");
        return;
    }
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let scratch = common::test_tempdir_in("/tmp", "itd-nf-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let fake = FakeNorthflank::start(scratch, &fixture).await;
    let head_dir = fake.scratch().join("head");
    let (_head, port, cfg, ws_id) = start_backend_with(
        &head_dir,
        &repo,
        &[
            ("INTENTD_NORTHFLANK_API_URL", &fake.url),
            ("INTENTD_NORTHFLANK_EXEC_URL", &fake.exec_url),
            ("INTENTD_NORTHFLANK_REGISTRY_URL", &fake.url),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
            ("INTENTD_E2B_DAEMON_WAIT_SECS", "30"),
        ],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let hello = ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "nf-life", "name": "Northflank lifecycle" }),
    )
    .await;
    assert_eq!(
        hello["server"]["capabilities"]["cloudMachineProviders"], 1,
        "{hello}"
    );
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

    // Each failed check refuses with its documented error, before any save.
    let missing = refused(
        &mut rpc_ws,
        10,
        "cloudMachines.configure",
        northflank_params(&json!({ "projectId": "no-such-project" })),
    )
    .await;
    assert_eq!(missing["code"], -32602, "{missing}");
    assert_eq!(missing["data"]["reason"], "project-missing", "{missing}");
    assert_eq!(missing["data"]["provider"], "northflank", "{missing}");
    let locked = refused(
        &mut rpc_ws,
        11,
        "cloudMachines.configure",
        northflank_params(&json!({ "projectId": LOCKED_PROJECT })),
    )
    .await;
    assert_eq!(locked["data"]["reason"], "permission-denied", "{locked}");
    assert_eq!(locked["data"]["action"], "read-project", "{locked}");
    let rejected = refused(
        &mut rpc_ws,
        12,
        "cloudMachines.configure",
        northflank_params(&json!({ "apiKey": "nf-wrong-token-0000" })),
    )
    .await;
    assert_eq!(rejected["data"]["reason"], "key-rejected", "{rejected}");
    let wrong_plan = refused(
        &mut rpc_ws,
        13,
        "cloudMachines.configure",
        northflank_params(&json!({ "plans": { "4x8": "nf-compute-200" } })),
    )
    .await;
    assert_eq!(wrong_plan["code"], -32602, "{wrong_plan}");
    assert!(
        wrong_plan["message"]
            .as_str()
            .unwrap()
            .contains("plans.4x8"),
        "{wrong_plan}"
    );
    let no_image = refused(
        &mut rpc_ws,
        14,
        "cloudMachines.configure",
        northflank_params(&json!({ "image": "ghcr.io/intent-hq/missing" })),
    )
    .await;
    assert_eq!(no_image["data"]["reason"], "template-missing", "{no_image}");
    for (n, (member, value)) in [
        ("size", json!("16x32")),
        ("templateProject", json!("intent")),
        ("diskGiB", json!(5)),
    ]
    .into_iter()
    .enumerate()
    {
        let invalid = refused(
            &mut rpc_ws,
            15 + i64::try_from(n).unwrap(),
            "cloudMachines.configure",
            northflank_params(&json!({ member: value })),
        )
        .await;
        assert_eq!(invalid["code"], -32602, "{member}: {invalid}");
        if member == "diskGiB" {
            let message = invalid["message"].as_str().unwrap();
            assert!(
                message.contains("6-500") && message.contains("at least 6 GiB"),
                "{invalid}"
            );
        }
    }
    assert!(NF_TOKEN.len() > 1024, "{}", NF_TOKEN.len());
    let checked = ok(
        &mut rpc_ws,
        18,
        "cloudMachines.configure",
        northflank_params(&json!({ "validateOnly": true })),
    )
    .await;
    assert!(checked["provider"].is_object(), "{checked}");
    let oversized = refused(
        &mut rpc_ws,
        19,
        "cloudMachines.configure",
        northflank_params(&json!({ "apiKey": "x".repeat(8 * 1024 + 1) })),
    )
    .await;
    assert_eq!(oversized["code"], -32602, "{oversized}");
    assert!(
        oversized["message"].as_str().unwrap().contains("apiKey"),
        "{oversized}"
    );
    let listed = ok(&mut rpc_ws, 20, "cloudMachines.list", json!({})).await;
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
        21,
        "cloudMachines.configure",
        northflank_params(&json!({ "size": "2x4", "diskGiB": 6, "idlePauseMinutes": 1440 })),
    )
    .await;
    let provider = &saved["provider"];
    let entry = provider["id"].as_str().unwrap().to_owned();
    assert!(entry.starts_with("northflank-"), "{provider}");
    assert_eq!(provider["provider"], "northflank");
    assert_eq!(
        provider["template"],
        format!("{IMAGE_REPO}:{}", env!("CARGO_PKG_VERSION"))
    );
    assert!(provider.get("templateProject").is_none(), "{provider}");
    assert_eq!(
        provider["northflank"],
        json!({
            "projectId": PROJECT, "region": REGION, "image": IMAGE_REPO, "diskGiB": 6,
            "plans": { "2x4": "nf-compute-200", "4x8": "nf-compute-400", "8x16": "nf-compute-800-16" },
        })
    );
    assert_eq!(
        provider["sizes"],
        json!([
            { "id": "2x4", "vcpu": 2, "memoryMiB": 4096, "deploymentPlan": "nf-compute-200" },
            { "id": "4x8", "vcpu": 4, "memoryMiB": 8192, "deploymentPlan": "nf-compute-400" },
            { "id": "8x16", "vcpu": 8, "memoryMiB": 16384, "deploymentPlan": "nf-compute-800-16" },
        ])
    );

    // target "cloud": a new service of the only entry.
    let created = ok(
        &mut rpc_ws,
        30,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Disk", "model": "default", "provider": keyed_ref,
            "placement": { "target": "cloud" },
        }),
    )
    .await;
    let placement = &created["agent"]["placement"];
    assert_eq!(
        placement["target"], "e2b",
        "a resolved cloud placement: {created}"
    );
    assert_eq!(placement["cloudMachineId"], entry.as_str(), "{created}");
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    let agent = (ws_id.as_str(), agent_id.as_str());
    let first = echoed(&mut rpc_ws, agent, "first turn", 40).await;
    assert!(!first.contains(DISK_NOTICE), "{first}");
    let service = fake.service_id().unwrap();
    let machine = machine_until(&mut rpc_ws, 50, &service, |m| m.is_some())
        .await
        .unwrap();
    let machine_id = machine["id"].as_str().unwrap().to_owned();
    assert_eq!(machine["cloud"]["provider"], "northflank", "{machine}");
    assert_eq!(machine["cloud"]["volumeId"], service.as_str(), "{machine}");
    {
        let state = fake.state();
        let (_, volume) = &state.creates[0];
        assert_eq!(volume["spec"]["storageSize"], 6 * 1024, "{volume}");
        assert_eq!(volume["mounts"][0]["containerMountPath"], "/home/user");
        let (_, created) = &state.creates[1];
        assert_eq!(created["billing"]["deploymentPlan"], "nf-compute-200");
        assert_eq!(
            created["deployment"]["external"]["imagePath"],
            format!("{IMAGE_REPO}:{}", env!("CARGO_PKG_VERSION"))
        );
        assert!(state.api.tags.iter().any(|tag| tag == "intent-node"));
        assert!(state
            .api
            .tags
            .iter()
            .any(|tag| tag.starts_with("intent-backend-")));
        let home = &state.services[&service].home;
        assert!(
            home.join(".intent").join("config.toml").exists(),
            "intentd's data lives on the volume"
        );
    }

    let setups = fake.root_setups();
    assert!(setups >= 1, "the volume is handed to the user at create");
    {
        let state = fake.state();
        let (user, argv) = &state.execs[0];
        assert_eq!(user, "root", "{argv:?}");
        assert!(
            argv.iter().any(|arg| arg.contains("intentd serve")),
            "the start runs in the same command as the setup: {argv:?}"
        );
    }

    // Paused (scaled to 0) and resumed: the same machine, one notice.
    let paused = ok(
        &mut rpc_ws,
        60,
        "cloudMachines.pause",
        json!({ "machineId": machine_id }),
    )
    .await;
    assert_eq!(paused["machine"]["cloud"]["state"], "paused", "{paused}");
    assert!(fake.state().api.services[&service].paused);
    let resumed = echoed(&mut rpc_ws, agent, "after the pause", 70).await;
    assert!(resumed.contains(DISK_NOTICE), "{resumed}");
    assert_eq!(resumed.matches("Note from Intent").count(), 1, "{resumed}");
    // The old process is known gone after a disk resume: the relaunch never
    // waits for its exit record (intent-hq/intent#9238).
    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    assert!(
        !log.contains("exit record did not arrive in time"),
        "the relaunch waited for a lost exit record"
    );
    // The relaunch loaded the session from the disk: the conversation
    // carries over (intent-hq/intent#9236), and the history the load
    // replays never reaches the transcript.
    let recalled = echoed(&mut rpc_ws, agent, "recall first turn", 75).await;
    assert!(recalled.contains("recall first turn: yes"), "{recalled}");
    let convo = ok(
        &mut rpc_ws,
        78,
        "agent.getConversation",
        json!({ "workspaceId": agent.0, "agentId": agent.1 }),
    )
    .await;
    assert!(!convo.to_string().contains("replayed:"), "{convo}");
    assert!(fake
        .calls_for(&service)
        .iter()
        .any(|c| is_call(c, "POST", "/resume")));
    let same = machine_until(&mut rpc_ws, 80, &service, |m| m.is_some())
        .await
        .unwrap();
    assert_eq!(
        same["id"],
        machine_id.as_str(),
        "the identity is kept on the disk"
    );
    assert!(
        fake.root_setups() > setups,
        "the volume is handed to the user again after a resume"
    );
    let setups = fake.root_setups();
    let later = echoed(&mut rpc_ws, agent, "a later turn", 90).await;
    assert!(!later.contains(DISK_NOTICE), "{later}");

    // Northflank restarts the container on its own: the same, without a pause.
    fake.restart_container(&service);
    let restarted = echoed(&mut rpc_ws, agent, "after the restart", 100).await;
    assert!(restarted.contains(DISK_NOTICE), "{restarted}");
    let recalled = echoed(&mut rpc_ws, agent, "recall a later turn", 105).await;
    assert!(recalled.contains("recall a later turn: yes"), "{recalled}");
    let same = machine_until(&mut rpc_ws, 110, &service, |m| m.is_some())
        .await
        .unwrap();
    assert_eq!(same["id"], machine_id.as_str());
    assert!(
        fake.root_setups() > setups,
        "the volume is handed to the user again after a restart"
    );
    for (user, argv) in &fake.state().execs {
        assert!(
            !argv.iter().any(|arg| arg.starts_with("IS_SANDBOX")),
            "{argv:?}"
        );
        let setup = argv.iter().any(|arg| arg.contains("chown -R"));
        assert_eq!(user, if setup { "root" } else { "user" }, "{argv:?}");
        if setup {
            assert!(
                argv.iter().any(|arg| arg.contains("exec setpriv")),
                "a root command drops to the user before it runs anything: {argv:?}"
            );
            assert!(
                argv.iter().any(|arg| arg.contains("intentd")),
                "no command runs only the setup: {argv:?}"
            );
        }
    }

    // Killed: the service and its volume go.
    ok(
        &mut rpc_ws,
        120,
        "cloudMachines.kill",
        json!({ "machineId": machine_id }),
    )
    .await;
    machine_until(&mut rpc_ws, 130, &service, |m| m.is_none()).await;
    assert!(fake.state().services.is_empty());
    assert!(fake.state().api.volumes.is_empty());
    assert!(fake
        .calls_for(&service)
        .iter()
        .any(|c| c.0 == "DELETE" && c.1.ends_with(&format!("/services/{service}"))));

    // A token that can no longer run commands: the next start is refused
    // with the action named.
    fake.state().exec_refusal = Some("Forbidden: missing permission".into());
    let mut events = agent_events(port, cfg.clone(), &ws_id).await;
    ok(
        &mut rpc_ws,
        140,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "no exec" }),
    )
    .await;
    let failed = failure_containing(&mut events, &agent_id, "lacks the permission for exec").await;
    assert!(failed.contains("Northflank token"), "{failed}");
    fake.state().exec_refusal = None;

    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    let mut secrets = vec![NF_TOKEN.clone(), PROFILE_SECRET.to_owned()];
    secrets.extend(fake.executor_tokens());
    for secret in &secrets {
        assert!(!log.contains(secret.as_str()), "a secret reached the log");
    }
}

/// The reaper, with one settings minute of 100 ms, deletes the leftovers of
/// this backend: a service whose tags are in its runtime environment, one
/// made before that (Northflank kept none of its tags: only its team tag
/// says whose it is) and a volume without a service. Another backend's
/// service stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reaper_deletes_this_backends_leftover_northflank_machines() {
    let scratch = common::test_tempdir_in("/tmp", "itd-nf-reap-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let fake = FakeNorthflank::start(scratch, "unused").await;
    let head_dir = fake.scratch().join("head");
    let (_head, port, cfg, _ws_id) = start_backend_with(
        &head_dir,
        &repo,
        &[
            ("INTENTD_NORTHFLANK_API_URL", &fake.url),
            ("INTENTD_NORTHFLANK_EXEC_URL", &fake.exec_url),
            ("INTENTD_NORTHFLANK_REGISTRY_URL", &fake.url),
            ("INTENTD_E2B_MINUTE_MS", "100"),
        ],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let mine = vec![
        intent_nodes::northflank::NODE_TAG.to_owned(),
        intent_nodes::northflank::backend_tag(&backend_id),
    ];
    let theirs = vec![
        intent_nodes::northflank::NODE_TAG.to_owned(),
        intent_nodes::northflank::backend_tag("another-backend"),
    ];
    let tags_env = |backend: &str| json!({ "INTENT_TAGS": json!({ "intentBackendId": backend, "agentId": "gone" }).to_string() });
    let old = "2026-01-01T00:00:00Z";
    fake.seed_service("left-new", &mine, &tags_env(&backend_id), old);
    fake.seed_service("left-legacy", &mine, &json!({}), old);
    fake.seed_service("foreign", &theirs, &tags_env("another-backend"), old);
    fake.state().api.seed_volume("left-volume", &mine, old);

    ok(
        &mut rpc_ws,
        2,
        "cloudMachines.configure",
        northflank_params(&json!({ "size": "2x4", "diskGiB": 6 })),
    )
    .await;
    timeout(common::test_timeout(Duration::from_secs(60)), async {
        loop {
            {
                let state = fake.state();
                if !state.services.contains_key("left-new")
                    && !state.services.contains_key("left-legacy")
                    && !state.api.volumes.contains_key("left-volume")
                {
                    break;
                }
            }
            keep_alive(&mut rpc_ws).await;
            // timing-guard: poll interval for the reaper (every 10 minutes = 1 s)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("this backend's leftovers are reaped");
    let state = fake.state();
    assert!(state.services.contains_key("foreign"));
    assert!(state.api.volumes.contains_key("foreign"));
    assert!(
        !state.api.volumes.contains_key("left-new")
            && !state.api.volumes.contains_key("left-legacy"),
        "a reaped service's volume goes with it"
    );
}
