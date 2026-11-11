//! Northflank cloud machines end to end, against a fake Northflank: its
//! REST API, its command-exec WebSocket and a public image registry. Each
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
//! and tell the agent's next turn once what its disk kept and lost;
//! `cloudMachines.kill` deletes the service with its volume; the token never
//! reaches the backend's log.

use std::collections::HashMap;
use std::sync::Mutex;

use intentd_test_support::ProcessGroup;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;

const NF_TOKEN: &str = "nf-api-token-0123456789abcdef0123456789abcdef";
const PROJECT: &str = "intent-cloud";
/// A project the token can see but not read.
const LOCKED_PROJECT: &str = "locked-project";
const REGION: &str = "europe-west";
const IMAGE_REPO: &str = "ghcr.io/intent-hq/intent-node";
/// The words of the notice a launch after a disk resume carries.
const DISK_NOTICE: &str = "stopped and started again from its disk";

/// One fake service: its volume (the container's home folder), the
/// environment its commands run with, and the process group of its current
/// container (replaced at each start, since a stop ends every process).
struct FakeService {
    home: std::path::PathBuf,
    env: Vec<(String, String)>,
    group: Arc<ProcessGroup>,
    paused: bool,
    image: String,
    annotations: Value,
    tags: Vec<String>,
}

#[derive(Default)]
struct FakeState {
    services: HashMap<String, FakeService>,
    /// Volume id → its tags.
    volumes: HashMap<String, Vec<String>>,
    team_tags: Vec<String>,
    /// Every create request body: (path, body).
    creates: Vec<(String, Value)>,
    /// Every lifecycle call: (method, path).
    calls: Vec<(String, String)>,
    /// Every exec init frame's argv.
    execs: Vec<Vec<String>>,
    /// Every stdout a command printed, to look for the executor token.
    outputs: Vec<String>,
    /// While set, exec answers this init auth instead of "successful".
    exec_refusal: Option<String>,
    groups: Vec<Arc<ProcessGroup>>,
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
        let shim = shim_dir.join("setsid");
        std::fs::write(&shim, SETSID_SHIM).unwrap();
        std::fs::set_permissions(&shim, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
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
        let data = |value: Value| reply(200, json!({ "data": value }));
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
        if headers.get("authorization").map(String::as_str) != Some(&format!("Bearer {NF_TOKEN}")) {
            return error(401, "Invalid API token");
        }
        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        if method != "GET" {
            self.state()
                .calls
                .push((method.to_owned(), path.to_owned()));
        }
        match (method, parts.as_slice()) {
            ("GET", ["plans"]) => data(json!({ "plans": [
                { "id": "nf-compute-50", "cpuResource": 0.5, "ramResource": 1024 },
                { "id": "nf-compute-200", "cpuResource": 2, "ramResource": 4096 },
                { "id": "nf-compute-400", "cpuResource": 4, "ramResource": 8192 },
                { "id": "nf-compute-800-16", "cpuResource": 8, "ramResource": 16384 },
            ] })),
            ("GET", ["tags"]) => {
                let tags: Vec<Value> = self
                    .state()
                    .team_tags
                    .iter()
                    .map(|name| json!({ "id": name, "name": name }))
                    .collect();
                reply(
                    200,
                    json!({ "data": { "tags": tags }, "pagination": { "hasNextPage": false } }),
                )
            }
            ("POST", ["tags"]) => {
                let request: Value = serde_json::from_slice(body).unwrap();
                let name = request["name"].as_str().unwrap().to_owned();
                self.state().team_tags.push(name.clone());
                data(json!({ "id": name, "name": name }))
            }
            ("GET", ["projects", LOCKED_PROJECT]) => error(403, "Forbidden: read-project"),
            ("GET", ["projects", project]) if *project == PROJECT => {
                data(json!({ "id": PROJECT, "deployment": { "region": REGION } }))
            }
            ("GET", ["projects", _]) => error(404, "Project not found"),
            (_, ["projects", project, ..]) if *project != PROJECT => {
                error(404, "Project not found")
            }
            ("GET", ["projects", _, "services"]) => {
                let services: Vec<Value> = self
                    .state()
                    .services
                    .iter()
                    .map(|(id, s)| json!({ "id": id, "tags": s.tags }))
                    .collect();
                reply(
                    200,
                    json!({ "data": { "services": services }, "pagination": { "hasNextPage": false } }),
                )
            }
            ("GET", ["projects", _, "volumes"]) => {
                let state = self.state();
                let volumes: Vec<Value> = state
                    .volumes
                    .iter()
                    .map(|(id, tags)| {
                        let attached: Vec<Value> = state
                            .services
                            .contains_key(id)
                            .then(|| json!({ "id": id }))
                            .into_iter()
                            .collect();
                        json!({ "id": id, "tags": tags, "attachedObjects": attached, "createdAt": "2026-01-01T00:00:00Z" })
                    })
                    .collect();
                reply(
                    200,
                    json!({ "data": volumes, "pagination": { "hasNextPage": false } }),
                )
            }
            ("POST", ["projects", _, "volumes"]) => {
                let request: Value = serde_json::from_slice(body).unwrap();
                let name = request["name"].as_str().unwrap().to_owned();
                let tags: Vec<String> = serde_json::from_value(request["tags"].clone()).unwrap();
                std::fs::create_dir_all(self.root.join(&name)).unwrap();
                let mut state = self.state();
                state.creates.push((path.to_owned(), request));
                state.volumes.insert(name.clone(), tags);
                data(json!({ "id": name, "name": name }))
            }
            ("POST", ["projects", _, "services", "deployment"]) => {
                let request: Value = serde_json::from_slice(body).unwrap();
                let name = request["name"].as_str().unwrap().to_owned();
                let home = self.root.join(&name);
                let env = self.container_env(&home);
                let group = Arc::new(ProcessGroup::new().unwrap());
                let mut state = self.state();
                state.creates.push((path.to_owned(), request.clone()));
                state.groups.push(group.clone());
                state.services.insert(
                    name.clone(),
                    FakeService {
                        home,
                        env,
                        group,
                        paused: false,
                        image: request["deployment"]["external"]["imagePath"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned(),
                        annotations: request["deployment"]["metadata"]["annotations"].clone(),
                        tags: serde_json::from_value(request["tags"].clone()).unwrap(),
                    },
                );
                data(json!({ "id": name, "name": name }))
            }
            ("GET", ["projects", _, "services", id]) => {
                let state = self.state();
                let Some(service) = state.services.get(*id) else {
                    return error(404, "Service not found");
                };
                data(json!({
                    "id": id,
                    "servicePaused": service.paused,
                    "createdAt": "2026-01-01T00:00:00Z",
                    "deployment": {
                        "instances": u8::from(!service.paused),
                        "external": { "imagePath": service.image },
                        "metadata": { "annotations": service.annotations },
                    },
                    "status": { "deployment": { "status": "COMPLETED" } },
                }))
            }
            ("GET", ["projects", _, "services", id, "containers"]) => {
                let state = self.state();
                let Some(service) = state.services.get(*id) else {
                    return error(404, "Service not found");
                };
                let containers: Vec<Value> = (!service.paused)
                    .then(|| json!({ "status": "TASK_RUNNING" }))
                    .into_iter()
                    .collect();
                data(json!({ "containers": containers }))
            }
            ("POST", ["projects", _, "services", id, "pause"]) => {
                let mut state = self.state();
                let Some(service) = state.services.get_mut(*id) else {
                    return error(404, "Service not found");
                };
                service.paused = true;
                service.group.kill();
                let group = Arc::new(ProcessGroup::new().unwrap());
                service.group = group.clone();
                state.groups.push(group);
                data(json!({}))
            }
            ("POST", ["projects", _, "services", id, "resume"]) => {
                let mut state = self.state();
                let Some(service) = state.services.get_mut(*id) else {
                    return error(404, "Service not found");
                };
                service.paused = false;
                data(json!({}))
            }
            ("DELETE", ["projects", _, "services", id]) => {
                let mut state = self.state();
                let Some(service) = state.services.remove(*id) else {
                    return error(404, "Service not found");
                };
                service.group.terminate(stop_grace());
                if target.contains("delete_child_objects=true") {
                    state.volumes.remove(*id);
                }
                data(json!({}))
            }
            ("DELETE", ["projects", _, "volumes", id]) => {
                if self.state().volumes.remove(*id).is_some() {
                    data(json!({}))
                } else {
                    error(404, "Volume not found")
                }
            }
            _ => error(404, "not found"),
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
        let send = |value: Value| Message::text(value.to_string());
        let refusal = self.state().exec_refusal.clone();
        if init["data"]["auth"]["apiToken"] != NF_TOKEN || refusal.is_some() {
            let message = refusal.unwrap_or_else(|| "Invalid token".into());
            let _ = ws
                .send(send(
                    json!({ "type": "init", "data": { "auth": "failed", "message": message } }),
                ))
                .await;
            let _ = ws.close(None).await;
            return;
        }
        let argv: Vec<String> =
            serde_json::from_value(init["data"]["context"]["command"].clone()).unwrap();
        self.state().execs.push(argv.clone());
        let _ = ws
            .send(send(
                json!({ "type": "init", "data": { "auth": "successful" } }),
            ))
            .await;
        let Some((home, env, group)) = self
            .state()
            .services
            .get(&service_id)
            .map(|s| (s.home.clone(), s.env.clone(), s.group.pgid()))
        else {
            let _ = ws
                .send(send(
                    json!({ "type": "error", "data": { "message": "Service not found" } }),
                ))
                .await;
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
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if !stdout.is_empty() {
            let _ = ws
                .send(send(json!({ "type": "stdOut", "data": stdout })))
                .await;
        }
        if !stderr.is_empty() {
            let _ = ws
                .send(send(json!({ "type": "stdErr", "data": stderr })))
                .await;
        }
        let status = if code == 0 { "Success" } else { "Failure" };
        let _ = ws
            .send(send(
                json!({ "type": "completion", "data": { "exitCode": code, "status": status } }),
            ))
            .await;
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
        env
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
    let mut params = json!({ "provider": "northflank", "name": "Northflank", "apiKey": NF_TOKEN, "projectId": PROJECT });
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
        ("diskGiB", json!(4)),
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
    }
    let listed = ok(&mut rpc_ws, 20, "cloudMachines.list", json!({})).await;
    assert_eq!(
        listed["providers"],
        json!([]),
        "nothing was saved: {listed}"
    );
    assert_eq!(listed["providerKinds"], json!(["e2b", "northflank"]));

    let saved = ok(
        &mut rpc_ws,
        21,
        "cloudMachines.configure",
        northflank_params(&json!({ "size": "2x4", "diskGiB": 5, "idlePauseMinutes": 1440 })),
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
            "projectId": PROJECT, "region": REGION, "image": IMAGE_REPO, "diskGiB": 5,
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
        assert_eq!(volume["spec"]["storageSize"], 5 * 1024, "{volume}");
        assert_eq!(volume["mounts"][0]["containerMountPath"], "/home/user");
        let (_, created) = &state.creates[1];
        assert_eq!(created["billing"]["deploymentPlan"], "nf-compute-200");
        assert_eq!(
            created["deployment"]["external"]["imagePath"],
            format!("{IMAGE_REPO}:{}", env!("CARGO_PKG_VERSION"))
        );
        assert!(state.team_tags.iter().any(|tag| tag == "intent-node"));
        assert!(state
            .team_tags
            .iter()
            .any(|tag| tag.starts_with("intent-backend-")));
        let home = &state.services[&service].home;
        assert!(
            home.join(".intent").join("config.toml").exists(),
            "intentd's data lives on the volume"
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
    assert!(fake.state().services[&service].paused);
    let resumed = echoed(&mut rpc_ws, agent, "after the pause", 70).await;
    assert!(resumed.contains(DISK_NOTICE), "{resumed}");
    assert_eq!(resumed.matches("Note from Intent").count(), 1, "{resumed}");
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
    let later = echoed(&mut rpc_ws, agent, "a later turn", 90).await;
    assert!(!later.contains(DISK_NOTICE), "{later}");

    // Northflank restarts the container on its own: the same, without a pause.
    fake.restart_container(&service);
    let restarted = echoed(&mut rpc_ws, agent, "after the restart", 100).await;
    assert!(restarted.contains(DISK_NOTICE), "{restarted}");
    let same = machine_until(&mut rpc_ws, 110, &service, |m| m.is_some())
        .await
        .unwrap();
    assert_eq!(same["id"], machine_id.as_str());

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
    assert!(fake.state().volumes.is_empty());
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
    let failed = failure_containing(&mut events, &agent_id, "permission-denied").await;
    assert!(failed.contains("exec"), "{failed}");
    fake.state().exec_refusal = None;

    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    let mut secrets = vec![NF_TOKEN.to_owned(), PROFILE_SECRET.to_owned()];
    secrets.extend(fake.executor_tokens());
    for secret in &secrets {
        assert!(!log.contains(secret.as_str()), "a secret reached the log");
    }
}
