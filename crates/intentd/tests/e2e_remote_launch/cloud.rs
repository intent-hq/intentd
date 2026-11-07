//! E2B cloud machines end to end, against a fake E2B: the fake answers the
//! REST API the backend uses and runs envd's `process.Process/Start` for
//! real, so each "sandbox" is a real `intentd serve` started by the
//! backend's own bootstrap script in a local directory. The backend dials it
//! directly (`INTENTD_E2B_DIRECT_HOST`): a test cannot join the tailcat
//! relay. The sandbox runs the agent as `codex` through the deterministic
//! node fixture, with an API-key profile.
//!
//! Proves: configure tests the key before saving; placement refusals happen
//! before any sandbox (no profile environment, account-folder profile, not
//! configured); an agent placed on E2B gets `cloudStart`, then runs on its
//! own sandbox and pushes its work to its refs; its sub-agent shares the
//! sandbox; a plan refusal becomes `placementError` `cloud-quota`; the E2B
//! key, the envd token and the executor token never reach the backend's
//! log, its replies or its events.

use std::collections::HashMap;
use std::sync::Mutex;

use base64::Engine as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;

const E2B_KEY: &str = "e2b_test_key_0123456789abcdef0123456789abcdef";
const PROFILE_SECRET: &str = "sk-cloud-profile-secret-7f3a91";

/// One fake sandbox: its directory, envd token and environment.
struct FakeSandbox {
    dir: std::path::PathBuf,
    envd_token: String,
    env: Vec<(String, String)>,
}

#[derive(Default)]
struct FakeState {
    sandboxes: HashMap<String, FakeSandbox>,
    creates: Vec<Value>,
    deletes: Vec<String>,
    /// Every stdout a process printed, to look for the executor token.
    outputs: Vec<String>,
    /// Templates whose create E2B refuses for the plan.
    refused_templates: Vec<String>,
}

/// The fake E2B API and envd on one loopback port.
struct FakeE2b {
    url: String,
    state: Arc<Mutex<FakeState>>,
    root: std::path::PathBuf,
    fixture: String,
}

impl FakeE2b {
    async fn start(root: &Path, fixture: &str) -> Arc<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let fake = Arc::new(Self {
            url,
            state: Arc::default(),
            root: root.to_path_buf(),
            fixture: fixture.to_owned(),
        });
        let served = fake.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let fake = served.clone();
                tokio::spawn(async move { fake.serve(stream).await });
            }
        });
        fake
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().unwrap()
    }

    /// Read one HTTP/1.1 request, answer it and close.
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
        let (status, content_type, payload) = self.route(&method, &target, &headers, &body).await;
        let reply = format!(
            "HTTP/1.1 {status} X\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload.len()
        );
        let _ = stream.write_all(reply.as_bytes()).await;
        let _ = stream.write_all(&payload).await;
        let _ = stream.shutdown().await;
    }

    async fn route(
        &self,
        method: &str,
        target: &str,
        headers: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, &'static str, Vec<u8>) {
        let json_reply = |status: u16, value: Value| {
            (status, "application/json", value.to_string().into_bytes())
        };
        let path = target.split('?').next().unwrap_or_default();
        if path == "/process.Process/Start" {
            return self.process(headers, body).await;
        }
        if headers.get("x-api-key").map(String::as_str) != Some(E2B_KEY) {
            return json_reply(401, json!({ "code": 401, "message": "Invalid API key" }));
        }
        match (method, path) {
            ("GET", "/v2/sandboxes") => json_reply(200, json!([])),
            ("GET", alias) if alias.starts_with("/templates/aliases/") => {
                json_reply(200, json!({ "templateID": "tpl-1", "public": true }))
            }
            ("POST", "/v2/sandboxes") => {
                let request: Value = serde_json::from_slice(body).unwrap();
                let template = request["templateID"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                self.state().creates.push(request);
                if self.state().refused_templates.contains(&template) {
                    return json_reply(
                        400,
                        json!({ "code": 400, "message": "requested 8 vCPU and 16384 MB memory exceed the limit of your plan" }),
                    );
                }
                let id = format!("sbx{}", self.state().creates.len());
                let token = format!("envd-token-{id}-a1b2c3d4e5f6");
                let dir = self.root.join(&id);
                std::fs::create_dir_all(dir.join("workspaces")).unwrap();
                common::enable_ws_api(&dir);
                let env = self.sandbox_env(&dir);
                self.state().sandboxes.insert(
                    id.clone(),
                    FakeSandbox {
                        dir,
                        envd_token: token.clone(),
                        env,
                    },
                );
                json_reply(
                    201,
                    json!({
                        "sandboxID": id, "templateID": template, "envdVersion": "0.2.0",
                        "envdAccessToken": token, "domain": "e2b.test",
                    }),
                )
            }
            ("DELETE", path) if path.starts_with("/sandboxes/") => {
                let id = path.trim_start_matches("/sandboxes/").to_owned();
                self.state().deletes.push(id.clone());
                if let Some(sandbox) = self.state().sandboxes.remove(&id) {
                    stop_sandbox(&sandbox);
                }
                (204, "application/json", Vec::new())
            }
            _ => json_reply(404, json!({ "code": 404, "message": "not found" })),
        }
    }

    /// A sandbox's environment: a hermetic daemon fixture, the codex
    /// toolchain that runs the node fixture, and this test's intentd first
    /// on PATH.
    fn sandbox_env(&self, dir: &Path) -> Vec<(String, String)> {
        let reference = common::hermetic_serve_command(dir);
        let mut env: Vec<(String, String)> = reference
            .get_envs()
            .filter_map(|(k, v)| Some((k.to_str()?.to_owned(), v?.to_str()?.to_owned())))
            .collect();
        let toolchain = common::codex_npx::install(dir, &self.fixture);
        let intentd_dir = Path::new(env!("CARGO_BIN_EXE_intentd"))
            .parent()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        for (key, value) in toolchain {
            let value = if key == "PATH" {
                format!("{intentd_dir}:{value}")
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

    /// Run envd's process start for real in the sandbox's directory and
    /// stream its output as Connect frames.
    async fn process(
        &self,
        headers: &HashMap<String, String>,
        body: &[u8],
    ) -> (u16, &'static str, Vec<u8>) {
        let id = headers.get("e2b-sandbox-id").cloned().unwrap_or_default();
        let (dir, env) = {
            let state = self.state();
            let Some(sandbox) = state.sandboxes.get(&id) else {
                return (502, "application/json", b"{}".to_vec());
            };
            if headers.get("x-access-token") != Some(&sandbox.envd_token) {
                return (401, "application/json", b"{}".to_vec());
            }
            (sandbox.dir.clone(), sandbox.env.clone())
        };
        let request: Value = serde_json::from_slice(&body[5..]).unwrap();
        let process = &request["process"];
        let mut command = tokio::process::Command::new(process["cmd"].as_str().unwrap());
        command
            .args(
                process["args"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|a| a.as_str().unwrap().to_owned()),
            )
            .current_dir(&dir)
            .env_clear()
            .envs(env)
            .stdin(Stdio::null());
        if let Some(envs) = process["envs"].as_object() {
            for (key, value) in envs {
                command.env(key, value.as_str().unwrap());
            }
        }
        let output = command.output().await.unwrap();
        self.state()
            .outputs
            .push(String::from_utf8_lossy(&output.stdout).into_owned());
        let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
        let mut frames = Vec::new();
        let mut push = |flags: u8, value: Value| {
            let payload = value.to_string().into_bytes();
            frames.push(flags);
            frames.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
            frames.extend_from_slice(&payload);
        };
        push(0, json!({ "event": { "start": { "pid": 1 } } }));
        if !output.stdout.is_empty() {
            push(
                0,
                json!({ "event": { "data": { "stdout": b64(&output.stdout) } } }),
            );
        }
        if !output.stderr.is_empty() {
            push(
                0,
                json!({ "event": { "data": { "stderr": b64(&output.stderr) } } }),
            );
        }
        let code = output.status.code().unwrap_or(-1);
        push(
            0,
            json!({ "event": { "end": { "exitCode": code, "exited": true, "status": format!("exit status {code}") } } }),
        );
        push(2, json!({}));
        (200, "application/connect+json", frames)
    }

    /// Every executor token a sandbox minted.
    fn executor_tokens(&self) -> Vec<String> {
        self.state()
            .outputs
            .iter()
            .filter_map(|out| serde_json::from_str::<Value>(out).ok())
            .filter_map(|v| v["token"].as_str().map(str::to_owned))
            .collect()
    }

    fn envd_tokens(&self) -> Vec<String> {
        (1..=self.state().creates.len())
            .map(|n| format!("envd-token-sbx{n}-a1b2c3d4e5f6"))
            .collect()
    }
}

impl Drop for FakeE2b {
    fn drop(&mut self) {
        if let Ok(state) = self.state.lock() {
            for sandbox in state.sandboxes.values() {
                stop_sandbox(sandbox);
            }
        }
    }
}

/// Stop a sandbox's daemon, as deleting the sandbox would.
fn stop_sandbox(sandbox: &FakeSandbox) {
    let _ = std::process::Command::new("/bin/sh")
        .args(["-c", "intentd stop >/dev/null 2>&1 || true"])
        .env_clear()
        .envs(sandbox.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// `agent.get` until `check` holds.
async fn agent_until(
    rpc_ws: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    id: i64,
    check: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(180));
    let mut id = id;
    loop {
        id += 1;
        let got = ok(
            rpc_ws,
            id,
            "agent.get",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if check(&got["agent"]) {
            return got["agent"].clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "agent never matched: {got}"
        );
        // timing-guard: poll interval while the sandbox starts (its steps are also events, but a poll is simpler here)
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_placed_on_e2b_runs_on_its_own_sandbox() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping E2B e2e: node or git not on PATH");
        return;
    }
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let scratch = common::test_tempdir_in("/tmp", "itd-e2b-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let sandboxes = scratch.path().join("e2b");
    std::fs::create_dir_all(&sandboxes).unwrap();
    let fake = FakeE2b::start(&sandboxes, &fixture).await;

    let head_dir = scratch.path().join("head");
    let (_head, port, cfg, ws_id) = start_backend_with(
        &head_dir,
        &repo,
        &[
            ("INTENTD_E2B_API_URL", &fake.url),
            ("INTENTD_E2B_ENVD_URL", &fake.url),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
        ],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let mut transcript = Vec::new();
    let hello = ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "e2b-e2e", "name": "E2B E2E" }),
    )
    .await;
    assert_eq!(
        hello["server"]["capabilities"]["cloudMachines"], 1,
        "{hello}"
    );
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "cloudMachine:changed"], "workspaceId": ws_id }),
    )
    .await;

    // Profiles: one with an API key, one tied to an account folder.
    let keyed = ok(
        &mut rpc_ws,
        3,
        "providers.profiles.create",
        json!({ "baseProviderId": "codex", "name": "Cloud", "env": [
            { "key": "OPENAI_API_KEY", "kind": "set", "value": PROFILE_SECRET },
        ] }),
    )
    .await;
    let keyed_ref = format!("codex@{}", keyed["profile"]["id"].as_str().unwrap());
    let folder = ok(
        &mut rpc_ws,
        4,
        "providers.profiles.create",
        json!({ "baseProviderId": "codex", "name": "Folder", "env": [
            { "key": "CODEX_HOME", "kind": "set", "value": "/tmp/codex-home" },
        ] }),
    )
    .await;
    let folder_ref = format!("codex@{}", folder["profile"]["id"].as_str().unwrap());
    let create = |provider: &str, name: &str| {
        json!({
            "workspaceId": ws_id, "name": name, "model": "default", "provider": provider,
            "placement": { "target": "e2b" },
        })
    };

    // Not configured: refused before anything.
    let refused = rpc(&mut rpc_ws, 5, "agent.create", create(&keyed_ref, "Early")).await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    assert_eq!(
        refused["error"]["data"]["code"],
        "cloud-machine-unavailable"
    );
    assert_eq!(refused["error"]["data"]["reason"], "not-configured");

    // A rejected key saves nothing.
    let rejected = rpc(
        &mut rpc_ws,
        6,
        "cloudMachines.configure",
        json!({ "provider": "e2b", "apiKey": "e2b_wrong_key" }),
    )
    .await;
    assert_eq!(
        rejected["error"]["data"]["reason"], "key-rejected",
        "{rejected}"
    );
    let listed = ok(&mut rpc_ws, 7, "cloudMachines.list", json!({})).await;
    assert_eq!(listed["providers"], json!([]), "{listed}");

    let configured = ok(
        &mut rpc_ws,
        8,
        "cloudMachines.configure",
        json!({ "provider": "e2b", "apiKey": E2B_KEY, "size": "4x8" }),
    )
    .await;
    transcript.push(configured.to_string());
    let provider = &configured["provider"];
    assert_eq!(provider["state"], "ready", "{configured}");
    assert_eq!(provider["size"], "4x8");
    assert!(provider["sizes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|size| size["id"] == "4x8"));
    assert!(provider["template"]
        .as_str()
        .unwrap()
        .ends_with(&format!("/intent-node-4x8:{}", env!("CARGO_PKG_VERSION"))));

    // No profile environment, or an account folder: refused, no sandbox.
    let bare = rpc(&mut rpc_ws, 9, "agent.create", create("codex", "Bare")).await;
    assert_eq!(bare["error"]["code"], -32602, "{bare}");
    assert_eq!(bare["error"]["data"]["code"], "cloud-profile-required");
    assert_eq!(bare["error"]["data"]["providerRef"], "codex");
    let tied = rpc(&mut rpc_ws, 10, "agent.create", create(&folder_ref, "Tied")).await;
    assert_eq!(tied["error"]["code"], -32602, "{tied}");
    assert_eq!(
        tied["error"]["data"]["code"], "profile-not-remote-compatible",
        "{tied}"
    );
    assert!(fake.state().creates.is_empty(), "no sandbox for a refusal");

    // Placed: the reply comes before the sandbox, with cloudStart and no
    // machine yet.
    let created = ok(&mut rpc_ws, 11, "agent.create", create(&keyed_ref, "Cloud")).await;
    transcript.push(created.to_string());
    let agent = &created["agent"];
    let agent_id = agent["id"].as_str().unwrap().to_owned();
    assert_eq!(agent["placement"]["target"], "e2b", "{created}");
    assert_eq!(agent["nodeName"], "E2B");
    assert!(agent.get("nodeId").is_none(), "{created}");
    assert!(agent["cloudStart"]["step"].is_string(), "{created}");

    // The first message waits for the sandbox, then runs there.
    let reply = turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        "hello cloud",
        20,
    )
    .await;
    assert!(reply.contains("hello cloud"), "{reply}");
    let linked = agent_until(&mut rpc_ws, (&ws_id, &agent_id), 30, |a| {
        a.get("nodeId").is_some()
    })
    .await;
    transcript.push(linked.to_string());
    assert!(linked.get("cloudStart").is_none(), "{linked}");
    let node_id = linked["nodeId"].as_str().unwrap().to_owned();
    {
        let state = fake.state();
        assert_eq!(state.creates.len(), 1);
        let request = &state.creates[0];
        assert_eq!(request["autoPause"], true, "{request}");
        assert_eq!(request["metadata"]["agentId"], agent_id.as_str());
        assert_eq!(request["metadata"]["workspaceId"], ws_id.as_str());
    }

    // Hidden from remoteMachines, listed under cloudMachines.
    let exposed = ok(&mut rpc_ws, 60, "remoteMachines.list", json!({})).await;
    assert_eq!(exposed["machines"], json!([]), "{exposed}");
    let cloud = ok(&mut rpc_ws, 61, "cloudMachines.list", json!({})).await;
    transcript.push(cloud.to_string());
    let machines = cloud["machines"].as_array().unwrap();
    assert_eq!(machines.len(), 1, "{cloud}");
    assert_eq!(machines[0]["id"], node_id.as_str());
    assert_eq!(machines[0]["hosts"], json!(["127.0.0.1"]));
    assert_eq!(machines[0]["cloud"]["sandboxId"], "sbx1");
    assert_eq!(machines[0]["cloud"]["agentId"], agent_id.as_str());
    assert_eq!(machines[0]["cloud"]["size"], "4x8");
    assert!(machines[0]["name"].as_str().unwrap().starts_with("E2B · "));

    // Work reaches the agent's refs; a sub-agent shares the sandbox.
    let child_prompt = r#"wsapi await ws.file.write("child.txt", "from child"); return JSON.stringify({ marker: "child-1" });"#;
    let code = format!(
        r#"wsapi await ws.file.write("cloud.txt", "from the sandbox"); const c = await ws.agent.create("Child", {child_prompt:?}, {{ provider: {keyed_ref:?}, model: "default" }}); return JSON.stringify({{ marker: "parent-1", c }});"#
    );
    let made = wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        &code,
        "parent-1",
        70,
    )
    .await;
    transcript.push(made.clone());
    let refs = format!("refs/intent/ws/{ws_id}/agents/{agent_id}");
    let child_id = timeout(wait(), async {
        let mut id = 100;
        loop {
            id += 1;
            let listed = ok(
                &mut rpc_ws,
                id,
                "agent.list",
                json!({ "workspaceId": ws_id }),
            )
            .await;
            if let Some(child) = listed["agents"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|a| a["name"] == "Child")
            {
                return child["id"].as_str().unwrap().to_owned();
            }
            // timing-guard: poll interval for the forwarded create to be listed
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the child was created");
    let child = agent_until(&mut rpc_ws, (&ws_id, &child_id), 200, |a| {
        a.get("nodeId").is_some()
    })
    .await;
    assert_eq!(
        child["nodeId"],
        node_id.as_str(),
        "the child shares the sandbox: {child}"
    );
    assert_eq!(
        fake.state().creates.len(),
        1,
        "no second sandbox for the child"
    );
    let pushed = timeout(wait(), async {
        loop {
            let shown = Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(["show", &format!("{refs}/wip:cloud.txt")])
                .output()
                .unwrap();
            if shown.status.success() {
                return String::from_utf8_lossy(&shown.stdout).into_owned();
            }
            // timing-guard: poll interval for the sandbox's turn-end push
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the sandbox pushed the agent's work");
    assert_eq!(pushed.trim(), "from the sandbox");

    // A size the plan refuses fails the start with cloud-quota, deletes
    // nothing it did not make, and adds no machine.
    let sized = ok(
        &mut rpc_ws,
        300,
        "cloudMachines.configure",
        json!({ "provider": "e2b", "size": "8x16" }),
    )
    .await;
    let refused_template = sized["provider"]["template"].as_str().unwrap().to_owned();
    fake.state().refused_templates.push(refused_template);
    let big = ok(&mut rpc_ws, 301, "agent.create", create(&keyed_ref, "Big")).await;
    let big_id = big["agent"]["id"].as_str().unwrap().to_owned();
    let failed = agent_until(&mut rpc_ws, (&ws_id, &big_id), 310, |a| {
        a.get("placementError").is_some()
    })
    .await;
    transcript.push(failed.to_string());
    assert_eq!(failed["placementError"]["code"], "cloud-quota", "{failed}");
    assert_eq!(
        failed["placementError"]["data"]["limit"], "plan",
        "{failed}"
    );
    assert_eq!(failed["placementError"]["retryable"], false, "{failed}");
    assert!(failed.get("cloudStart").is_none(), "{failed}");
    let cloud = ok(&mut rpc_ws, 400, "cloudMachines.list", json!({})).await;
    assert_eq!(cloud["machines"].as_array().unwrap().len(), 1, "{cloud}");

    // Removing the provider with a live sandbox needs stopAgents.
    let busy = rpc(
        &mut rpc_ws,
        401,
        "cloudMachines.remove",
        json!({ "provider": "e2b" }),
    )
    .await;
    assert_eq!(busy["error"]["data"]["code"], "machine-in-use", "{busy}");
    let removed = ok(
        &mut rpc_ws,
        402,
        "cloudMachines.remove",
        json!({ "provider": "e2b", "stopAgents": true }),
    )
    .await;
    transcript.push(removed.to_string());
    assert_eq!(removed["ok"], true, "{removed}");
    assert_eq!(fake.state().deletes, ["sbx1"], "the sandbox is deleted");
    let after = ok(&mut rpc_ws, 403, "cloudMachines.list", json!({})).await;
    assert_eq!(after, json!({ "providers": [], "machines": [] }), "{after}");

    // No secret anywhere the backend writes or answers.
    let mut secrets = vec![E2B_KEY.to_owned(), PROFILE_SECRET.to_owned()];
    secrets.extend(fake.envd_tokens());
    let minted = fake.executor_tokens();
    assert!(!minted.is_empty(), "the sandbox minted a credential");
    secrets.extend(minted);
    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    let mut seen_events = Vec::new();
    while let Ok(Some(Ok(Message::Text(text)))) = timeout(
        common::test_timeout(Duration::from_millis(200)),
        events.next(),
    )
    .await
    {
        seen_events.push(text.to_string());
    }
    for secret in &secrets {
        assert!(
            !log.contains(secret.as_str()),
            "a secret reached the backend's log"
        );
        for text in transcript.iter().chain(&seen_events) {
            assert!(
                !text.contains(secret.as_str()),
                "a secret reached a reply or event: {text}"
            );
        }
    }
}
