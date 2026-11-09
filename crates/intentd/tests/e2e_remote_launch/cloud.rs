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

mod entries;
mod sub_agents;

const E2B_KEY: &str = "e2b_test_key_0123456789abcdef0123456789abcdef";
/// The key of a second E2B project: its sandboxes are listed, connected
/// and deleted only with this key.
const E2B_KEY_2: &str = "e2b_test_key_second_project_fedcba9876543210";
const PROFILE_SECRET: &str = "sk-cloud-profile-secret-7f3a91";

/// One fake sandbox: its directory, envd token and environment.
struct FakeSandbox {
    dir: std::path::PathBuf,
    template: String,
    envd_token: String,
    env: Vec<(String, String)>,
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag is an independent fault the tests switch on"
)]
#[derive(Default)]
struct FakeState {
    sandboxes: HashMap<String, FakeSandbox>,
    creates: Vec<Value>,
    deletes: Vec<String>,
    /// Every stdout a process printed, to look for the executor token.
    outputs: Vec<String>,
    /// Templates whose create E2B refuses for the plan.
    refused_templates: Vec<String>,
    /// Templates whose sandboxes fail the credential mint.
    failed_mint_templates: Vec<String>,
    /// Templates whose sandboxes come with a settings file that turns the
    /// external listener off (the bootstrap keeps existing settings).
    no_listener_templates: Vec<String>,
    /// Templates whose sandboxes take a while to start intentd.
    slow_start_templates: Vec<String>,
    /// Sandboxes paused now (their daemon is stopped with SIGSTOP).
    paused: std::collections::HashSet<String>,
    /// Every lifecycle call: (method, path).
    calls: Vec<(String, String)>,
    /// The next pauses and connects answer this instead, while set.
    pause_refusal: Option<(u16, Value)>,
    connect_refusal: Option<(u16, Value)>,
    /// Each sandbox's create metadata.
    metadata: HashMap<String, Value>,
    /// Sandboxes only listed (no daemon): `{ sandboxID, metadata, startedAt? }`.
    listed_only: Vec<Value>,
    /// While set, every sandbox delete fails (500).
    refuse_deletes: bool,
    /// The key (E2B project) each sandbox was created with.
    projects: HashMap<String, String>,
    /// While set, every timeout renewal is 404 and every list fails (500).
    timeouts_missing: bool,
    list_failing: bool,
    /// Sandboxes whose next resume brings back another intentd build, one
    /// the backend shares no link protocol with, on this port.
    outdated: HashMap<String, u16>,
    /// The release's template manifest (`e2b-templates.json`), served without
    /// a key; `None` answers 404, as a release without one would.
    manifest: Option<Value>,
    /// Templates E2B does not know: their create answers 404.
    missing_templates: Vec<String>,
    /// While set, E2B rejects the key (401), as after the owner revokes it.
    key_revoked: bool,
}

/// The release manifest listing `sizes` for this build's version.
fn manifest(sizes: &[&str]) -> Value {
    json!({ "schema": 1, "project": "intent", "version": env!("CARGO_PKG_VERSION"), "sizes": sizes })
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
        fake.state().manifest = Some(manifest(&["2x4", "4x8", "8x16", "16x32"]));
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

    /// `INTENTD_E2B_MANIFEST_BASE_URL` for a backend using this fake.
    fn manifest_base(&self) -> String {
        format!("{}/manifests", self.url)
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
        // E2B's edge answers 411 to a POST without a length (intent-hq/intent#8865).
        let unframed = matches!(method.as_str(), "POST" | "PUT" | "PATCH")
            && !headers.contains_key("content-length")
            && !headers.contains_key("transfer-encoding");
        let (status, content_type, payload) = if unframed {
            (411, "text/plain", b"Length Required".to_vec())
        } else {
            self.route(&method, &target, &headers, &body).await
        };
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
        // The release mirror: public, no key.
        if let Some(asset) = path.strip_prefix("/manifests/") {
            let expected = format!("v{}/e2b-templates.json", env!("CARGO_PKG_VERSION"));
            return match self.state().manifest.clone() {
                Some(manifest) if asset == expected => json_reply(200, manifest),
                _ => (404, "text/plain", b"Not Found".to_vec()),
            };
        }
        let key = headers.get("x-api-key").cloned().unwrap_or_default();
        if (key != E2B_KEY && key != E2B_KEY_2) || self.state().key_revoked {
            return json_reply(401, json!({ "code": 401, "message": "Invalid API key" }));
        }
        // Another project's sandbox does not exist for this key.
        let other_project = path
            .split('/')
            .filter(|part| part.starts_with("sbx"))
            .any(|id| {
                self.state()
                    .projects
                    .get(id)
                    .is_some_and(|project| *project != key)
            });
        if other_project {
            return json_reply(404, json!({ "code": 404, "message": "sandbox not found" }));
        }
        if method != "GET" || path == "/v2/sandboxes" {
            self.state()
                .calls
                .push((method.to_owned(), path.to_owned()));
        }
        match (method, path) {
            ("GET", "/v2/sandboxes") if self.state().list_failing => {
                json_reply(500, json!({ "code": 500, "message": "list unavailable" }))
            }
            ("GET", "/v2/sandboxes") => json_reply(200, self.list(target, &key)),
            ("POST", connect)
                if connect.starts_with("/v2/sandboxes/") && connect.ends_with("/connect") =>
            {
                let id = connect
                    .trim_start_matches("/v2/sandboxes/")
                    .trim_end_matches("/connect")
                    .to_owned();
                if let Some((status, body)) = self.state().connect_refusal.clone() {
                    return json_reply(status, body);
                }
                let (resumed, reply) = {
                    let mut state = self.state();
                    let Some(sandbox) = state.sandboxes.get(&id) else {
                        return json_reply(
                            404,
                            json!({ "code": 404, "message": "sandbox not found" }),
                        );
                    };
                    let reply = json!({
                        "sandboxID": id, "templateID": sandbox.template, "envdVersion": "0.2.0",
                        "envdAccessToken": sandbox.envd_token, "domain": "e2b.test",
                    });
                    (state.paused.remove(&id), reply)
                };
                if resumed {
                    let outdated = self.state().outdated.remove(&id);
                    match outdated {
                        Some(port) => self.outdate(&id, port).await,
                        None => self.signal(&id, "-CONT"),
                    }
                }

                json_reply(if resumed { 201 } else { 200 }, reply)
            }
            ("POST", pause) if pause.starts_with("/sandboxes/") && pause.ends_with("/pause") => {
                let id = pause
                    .trim_start_matches("/sandboxes/")
                    .trim_end_matches("/pause")
                    .to_owned();
                if let Some((status, body)) = self.state().pause_refusal.clone() {
                    return json_reply(status, body);
                }
                {
                    let mut state = self.state();
                    if !state.sandboxes.contains_key(&id) {
                        return json_reply(
                            404,
                            json!({ "code": 404, "message": "sandbox not found" }),
                        );
                    }
                    if !state.paused.insert(id.clone()) {
                        return json_reply(
                            409,
                            json!({ "code": 409, "message": "sandbox already paused" }),
                        );
                    }
                }
                self.signal(&id, "-STOP");
                (204, "application/json", Vec::new())
            }
            ("POST", timeout)
                if timeout.starts_with("/sandboxes/") && timeout.ends_with("/timeout") =>
            {
                let id = timeout
                    .trim_start_matches("/sandboxes/")
                    .trim_end_matches("/timeout")
                    .to_owned();
                let state = self.state();
                if state.timeouts_missing
                    || !state.sandboxes.contains_key(&id)
                    || state.paused.contains(&id)
                {
                    return json_reply(404, json!({ "code": 404, "message": "sandbox not found" }));
                }
                (204, "application/json", Vec::new())
            }
            // As E2B answers a key of another team (any user's own project):
            // the alias lookup checks only the key's own templates.
            ("GET", alias) if alias.starts_with("/templates/aliases/") => json_reply(
                400,
                json!({ "code": 400, "message": "namespace 'intent' must match your team 'acme'" }),
            ),
            ("POST", "/v2/sandboxes") => {
                let request: Value = serde_json::from_slice(body).unwrap();
                let template = request["templateID"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                self.state().creates.push(request.clone());
                if self.state().missing_templates.contains(&template) {
                    return json_reply(
                        404,
                        json!({ "code": 404, "message": format!("template '{template}' not found") }),
                    );
                }
                if self.state().refused_templates.contains(&template) {
                    return json_reply(
                        400,
                        json!({ "code": 400, "message": "requested 8 vCPU and 16384 MB memory exceed the limit of your plan" }),
                    );
                }
                let id = format!("sbx{}", self.state().creates.len());
                let token = format!("envd-token-{id}-a1b2c3d4e5f6");
                // As in a real sandbox: a home directory, no data dir set and
                // no daemon settings yet; the bootstrap must write them where
                // the daemon reads them (`$HOME/.intent`).
                let home = self.root.join(&id);
                let dir = home.join(".intent");
                std::fs::create_dir_all(dir.join("workspaces")).unwrap();
                if self.state().no_listener_templates.contains(&template) {
                    std::fs::write(dir.join("config.toml"), "[server.wsApi]\nenabled = false\n")
                        .unwrap();
                }
                let mut env = self.sandbox_env(&dir);
                env.retain(|(key, _)| key != "INTENTD_DATA_DIR");
                env.push(("HOME".into(), home.to_string_lossy().into_owned()));
                self.state()
                    .metadata
                    .insert(id.clone(), request["metadata"].clone());
                self.state().projects.insert(id.clone(), key.clone());
                self.state().sandboxes.insert(
                    id.clone(),
                    FakeSandbox {
                        dir,
                        template: template.clone(),
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
            ("DELETE", path) if path.starts_with("/sandboxes/") && self.state().refuse_deletes => {
                json_reply(500, json!({ "code": 500, "message": "delete unavailable" }))
            }
            ("DELETE", path) if path.starts_with("/sandboxes/") => {
                let id = path.trim_start_matches("/sandboxes/").to_owned();
                self.state().deletes.push(id.clone());
                self.state()
                    .listed_only
                    .retain(|listed| listed["sandboxID"] != id.as_str());
                if self.state().paused.remove(&id) {
                    self.signal(&id, "-CONT");
                }
                let removed = self.state().sandboxes.remove(&id);
                match removed {
                    Some(sandbox) => {
                        stop_sandbox(&sandbox);
                        (204, "application/json", Vec::new())
                    }
                    None => json_reply(404, json!({ "code": 404, "message": "sandbox not found" })),
                }
            }
            _ => json_reply(404, json!({ "code": 404, "message": "not found" })),
        }
    }

    /// The sandboxes of project `key` that `GET /v2/sandboxes` lists,
    /// filtered by its `metadata` query (every pair must match). Listed-only
    /// rows are in the first project.
    fn list(&self, target: &str, key: &str) -> Value {
        let query = target.split_once('?').map_or("", |(_, q)| q);
        let mut wanted: Vec<(String, String)> = Vec::new();
        for pair in query.split('&') {
            if let Some(encoded) = pair.strip_prefix("metadata=") {
                for kv in percent_decode(encoded).split('&') {
                    if let Some((k, v)) = kv.split_once('=') {
                        wanted.push((percent_decode(k), percent_decode(v)));
                    }
                }
            }
        }
        let state = self.state();
        let mut listed: Vec<Value> = state
            .sandboxes
            .iter()
            .filter(|(id, _)| state.projects.get(*id).map_or(E2B_KEY, String::as_str) == key)
            .map(|(id, sandbox)| {
                json!({
                    "sandboxID": id, "templateID": sandbox.template,
                    "state": if state.paused.contains(id) { "paused" } else { "running" },
                    "metadata": state.metadata.get(id).cloned().unwrap_or_else(|| json!({})),
                    "startedAt": "2026-01-01T00:00:00Z", "endAt": "2026-01-01T01:00:00Z",
                    "cpuCount": 4, "memoryMB": 8192, "envdVersion": "0.2.0",
                })
            })
            .collect();
        let first_project = key == E2B_KEY;
        listed.extend(
            state
                .listed_only
                .iter()
                .filter(|_| first_project)
                .map(|only| {
                    let mut row = json!({
                        "templateID": "intent/intent-node-4x8:0", "state": "running",
                        "endAt": "2026-01-01T01:00:00Z", "cpuCount": 4, "memoryMB": 8192,
                        "envdVersion": "0.2.0",
                    });
                    for key in ["sandboxID", "metadata", "startedAt"] {
                        if !only[key].is_null() {
                            row[key] = only[key].clone();
                        }
                    }
                    row
                }),
        );
        listed.retain(|row| {
            wanted
                .iter()
                .all(|(k, v)| row["metadata"][k.as_str()] == v.as_str())
        });
        Value::Array(listed)
    }

    /// Send `signal` to sandbox `id`'s daemon (a pause stops it).
    fn signal(&self, id: &str, signal: &str) {
        let Some(dir) = self.state().sandboxes.get(id).map(|s| s.dir.clone()) else {
            return;
        };
        let Ok(pid) = std::fs::read_to_string(dir.join("intentd.pid")) else {
            return;
        };
        let Some(pid) = pid.split_whitespace().next().map(str::to_owned) else {
            return;
        };
        let _ = std::process::Command::new("kill")
            .args([signal, &pid])
            .status();
    }

    /// E2B removes sandbox `id` on its own: its daemon stops and every
    /// later call about it is 404.
    fn vanish(&self, id: &str) {
        self.signal(id, "-KILL");
        self.state().paused.remove(id);
        self.state().sandboxes.remove(id);
    }

    /// Sandbox `id`'s paused daemon comes back as an older intentd build on
    /// the same port and data: one that speaks only link protocol 6, below
    /// every protocol the backend speaks.
    async fn outdate(&self, id: &str, port: u16) {
        let Some((dir, env)) = self
            .state()
            .sandboxes
            .get(id)
            .map(|s| (s.dir.clone(), s.env.clone()))
        else {
            return;
        };
        self.signal(id, "-KILL");
        let listening = || async {
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
        };
        timeout(common::test_timeout(Duration::from_secs(30)), async {
            while listening().await {
                // timing-guard: poll interval until the killed daemon's port closes
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the old daemon stopped listening");
        let log = std::fs::File::create(dir.join("outdated.log")).unwrap();
        let mut serve = common::hermetic_serve_command_fixed_port(&dir);
        serve
            .current_dir(&dir)
            .envs(env)
            .env("INTENTD_TCP_PORT", port.to_string())
            .env("INTENTD_TEST_NODE_LINK_PROTOCOL", "6")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        common::hermetic_fixture_identity(&mut serve, &dir);
        let mut child = tokio::process::Command::from(serve).spawn().unwrap();
        tokio::spawn(async move { child.wait().await });
        timeout(common::test_timeout(Duration::from_secs(60)), async {
            while !listening().await {
                // timing-guard: poll interval until the new daemon listens
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the outdated daemon listens");
    }

    /// The (method, path) lifecycle calls about sandbox `id`.
    fn calls_for(&self, id: &str) -> Vec<(String, String)> {
        self.state()
            .calls
            .iter()
            .filter(|(_, path)| path.split('/').any(|part| part == id))
            .cloned()
            .collect()
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
        let (dir, env, fail_mint, slow_start) = {
            let state = self.state();
            let Some(sandbox) = state.sandboxes.get(&id) else {
                return (502, "application/json", b"{}".to_vec());
            };
            if headers.get("x-access-token") != Some(&sandbox.envd_token) {
                return (401, "application/json", b"{}".to_vec());
            }
            (
                sandbox.dir.clone(),
                sandbox.env.clone(),
                state.failed_mint_templates.contains(&sandbox.template),
                state.slow_start_templates.contains(&sandbox.template),
            )
        };
        let request: Value = serde_json::from_slice(&body[5..]).unwrap();
        let process = &request["process"];
        let script = process["args"].to_string();
        if fail_mint && script.contains("executor.mint") {
            return (
                200,
                "application/connect+json",
                frames(b"", b"mint refused in the sandbox", 3),
            );
        }
        if slow_start && script.contains("intentd serve") {
            // timing-guard: holds the sandbox start open so a remove can run while it is in flight
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
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
        let code = output.status.code().unwrap_or(-1);
        (
            200,
            "application/connect+json",
            frames(&output.stdout, &output.stderr, code),
        )
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

/// Undo `%XX` escapes (and `+` as space).
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
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
    String::from_utf8_lossy(&out).into_owned()
}

/// envd's process stream for one finished process, as Connect frames.
fn frames(stdout: &[u8], stderr: &[u8], code: i32) -> Vec<u8> {
    let b64 = |bytes: &[u8]| base64::engine::general_purpose::STANDARD.encode(bytes);
    let mut frames = Vec::new();
    let mut push = |flags: u8, value: Value| {
        let payload = value.to_string().into_bytes();
        frames.push(flags);
        frames.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
        frames.extend_from_slice(&payload);
    };
    push(0, json!({ "event": { "start": { "pid": 1 } } }));
    if !stdout.is_empty() {
        push(0, json!({ "event": { "data": { "stdout": b64(stdout) } } }));
    }
    if !stderr.is_empty() {
        push(0, json!({ "event": { "data": { "stderr": b64(stderr) } } }));
    }
    push(
        0,
        json!({ "event": { "end": { "exitCode": code, "exited": true, "status": format!("exit status {code}") } } }),
    );
    push(2, json!({}));
    frames
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
        .env("INTENTD_DATA_DIR", &sandbox.dir)
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
    let manifest_base = fake.manifest_base();
    let (_head, port, cfg, ws_id) = start_backend_with(
        &head_dir,
        &repo,
        &[
            ("INTENTD_E2B_API_URL", &fake.url),
            ("INTENTD_E2B_ENVD_URL", &fake.url),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
            ("INTENTD_E2B_DAEMON_WAIT_SECS", "30"),
            ("INTENTD_E2B_MANIFEST_BASE_URL", &manifest_base),
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

    // The key is tested alone: the sizes come from the release's manifest,
    // never from probing templates with the key (E2B answers that only for
    // the key's own project). A size the manifest does not list is refused.
    fake.state().manifest = Some(manifest(&["2x4"]));
    let unlisted = rpc(
        &mut rpc_ws,
        30,
        "cloudMachines.configure",
        json!({ "provider": "e2b", "apiKey": E2B_KEY, "size": "4x8", "validateOnly": true }),
    )
    .await;
    assert_eq!(
        unlisted["error"]["data"]["reason"], "template-missing",
        "{unlisted}"
    );
    let listed = ok(
        &mut rpc_ws,
        31,
        "cloudMachines.configure",
        json!({ "provider": "e2b", "apiKey": E2B_KEY, "size": "2x4", "validateOnly": true }),
    )
    .await;
    assert_eq!(listed["provider"]["state"], "ready", "{listed}");
    // Without a manifest the backend falls back to the sizes every release
    // builds and does not refuse a size: the create is the real check.
    fake.state().manifest = None;
    let fallback = ok(
        &mut rpc_ws,
        32,
        "cloudMachines.configure",
        json!({ "provider": "e2b", "apiKey": E2B_KEY, "size": "16x32", "validateOnly": true }),
    )
    .await;
    let ids: Vec<&str> = fallback["provider"]["sizes"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|size| size["id"].as_str())
        .collect();
    assert_eq!(ids, ["2x4", "4x8"], "{fallback}");
    fake.state().manifest = Some(manifest(&["2x4", "4x8", "8x16", "16x32"]));
    assert!(
        fake.state().creates.is_empty(),
        "testing a key makes no sandbox"
    );

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

    // The first message waits for the sandbox, then runs there. Polled on
    // the RPC socket, which a slow sandbox start must not leave unread; the
    // event socket, unread meanwhile, is opened again.
    send_and_reply(&mut rpc_ws, (&ws_id, &agent_id), "hello cloud", 20).await;
    events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        22,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "cloudMachine:changed"], "workspaceId": ws_id }),
    )
    .await;
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

    // Keep the events read so far: the socket must not sit unread for the
    // starts below (the backend drops a client that stops answering pings).
    let mut seen_events = Vec::new();
    drain_events(&mut events, &mut seen_events).await;

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

    // A template E2B does not have (create answers 404) fails the start
    // with template-missing and leaves no sandbox.
    let missing = ok(
        &mut rpc_ws,
        340,
        "cloudMachines.configure",
        json!({ "provider": "e2b", "size": "8x16" }),
    )
    .await;
    let missing_template = missing["provider"]["template"].as_str().unwrap().to_owned();
    {
        let mut state = fake.state();
        state.refused_templates.clear();
        state.missing_templates.push(missing_template);
    }
    let sandboxes_before = fake.state().sandboxes.len();
    let gone = ok(&mut rpc_ws, 341, "agent.create", create(&keyed_ref, "Gone")).await;
    let gone_id = gone["agent"]["id"].as_str().unwrap().to_owned();
    let failed = agent_until(&mut rpc_ws, (&ws_id, &gone_id), 350, |a| {
        a.get("placementError").is_some()
    })
    .await;
    transcript.push(failed.to_string());
    assert_eq!(
        failed["placementError"]["code"], "cloud-machine-unavailable",
        "{failed}"
    );
    assert_eq!(
        failed["placementError"]["data"]["reason"], "template-missing",
        "{failed}"
    );
    assert_eq!(fake.state().sandboxes.len(), sandboxes_before, "no sandbox");

    drain_events(&mut events, &mut seen_events).await;
    // A bootstrap that fails (the mint in the sandbox) deletes the sandbox
    // it made and adds no machine.
    let small = ok(
        &mut rpc_ws,
        320,
        "cloudMachines.configure",
        json!({ "provider": "e2b", "size": "2x4" }),
    )
    .await;
    let small_template = small["provider"]["template"].as_str().unwrap().to_owned();
    fake.state().failed_mint_templates.push(small_template);
    let broken = ok(
        &mut rpc_ws,
        321,
        "agent.create",
        create(&keyed_ref, "Broken"),
    )
    .await;
    let broken_id = broken["agent"]["id"].as_str().unwrap().to_owned();
    let failed = agent_until(&mut rpc_ws, (&ws_id, &broken_id), 330, |a| {
        a.get("placementError").is_some()
    })
    .await;
    transcript.push(failed.to_string());
    assert_eq!(
        failed["placementError"]["code"], "cloud-machine-unavailable",
        "{failed}"
    );
    assert_eq!(
        failed["placementError"]["data"]["reason"], "bootstrap-failed",
        "{failed}"
    );
    let broken_sandbox = format!("sbx{}", fake.state().creates.len());
    assert!(
        fake.state().deletes.contains(&broken_sandbox),
        "the failed sandbox is deleted"
    );
    let cloud = ok(&mut rpc_ws, 390, "cloudMachines.list", json!({})).await;
    assert_eq!(cloud["machines"].as_array().unwrap().len(), 1, "{cloud}");

    // A daemon that starts without its external listener: the start fails
    // within the bootstrap's wait, and the error says what was missing and
    // carries the daemon's own last log lines, never a token.
    {
        let mut state = fake.state();
        let template = state.failed_mint_templates.remove(0);
        state.no_listener_templates.push(template);
    }
    let deaf = ok(&mut rpc_ws, 391, "agent.create", create(&keyed_ref, "Deaf")).await;
    let deaf_id = deaf["agent"]["id"].as_str().unwrap().to_owned();
    let failed = agent_until(&mut rpc_ws, (&ws_id, &deaf_id), 392, |a| {
        a.get("placementError").is_some()
    })
    .await;
    transcript.push(failed.to_string());
    assert_eq!(
        failed["placementError"]["data"]["reason"], "bootstrap-failed",
        "{failed}"
    );
    let detail = failed["placementError"]["detail"].as_str().unwrap();
    assert!(
        detail.contains("no external listener"),
        "the error names what was missing: {detail}"
    );
    assert!(!detail.contains(TOKEN), "{detail}");
    let deaf_sandbox = format!("sbx{}", fake.state().creates.len());
    assert!(fake.state().deletes.contains(&deaf_sandbox), "deleted");
    fake.state().no_listener_templates.clear();

    drain_events(&mut events, &mut seen_events).await;
    // A key revoked at E2B: the start fails key-rejected, the provider
    // shows it, and the next placement is refused before any sandbox. A
    // working key saved again clears it.
    fake.state().key_revoked = true;
    let revoked = ok(
        &mut rpc_ws,
        360,
        "agent.create",
        create(&keyed_ref, "Revoked"),
    )
    .await;
    let revoked_id = revoked["agent"]["id"].as_str().unwrap().to_owned();
    let failed = agent_until(&mut rpc_ws, (&ws_id, &revoked_id), 361, |a| {
        a.get("placementError").is_some()
    })
    .await;
    assert_eq!(
        failed["placementError"]["data"]["reason"], "key-rejected",
        "{failed}"
    );
    let listed = ok(&mut rpc_ws, 370, "cloudMachines.list", json!({})).await;
    assert_eq!(listed["providers"][0]["state"], "key-rejected", "{listed}");
    let refused = rpc(
        &mut rpc_ws,
        371,
        "agent.create",
        create(&keyed_ref, "Refused"),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    assert_eq!(
        refused["error"]["data"]["reason"], "key-rejected",
        "{refused}"
    );
    fake.state().key_revoked = false;
    let restored = ok(
        &mut rpc_ws,
        372,
        "cloudMachines.configure",
        json!({ "provider": "e2b", "apiKey": E2B_KEY }),
    )
    .await;
    assert_eq!(restored["provider"]["state"], "ready", "{restored}");

    // Removing the provider with a live sandbox needs stopAgents.
    let busy = rpc(
        &mut rpc_ws,
        401,
        "cloudMachines.remove",
        json!({ "provider": "e2b" }),
    )
    .await;
    assert_eq!(busy["error"]["data"]["code"], "machine-in-use", "{busy}");

    // A start racing remove: remove forgets the settings first, so a new
    // placement is refused before any sandbox; it waits for the start in
    // flight, which finds the settings gone and deletes its own sandbox;
    // then every sandbox is deleted while the key still works.
    let slow = ok(
        &mut rpc_ws,
        410,
        "cloudMachines.configure",
        json!({ "provider": "e2b", "size": "16x32" }),
    )
    .await;
    let slow_template = slow["provider"]["template"].as_str().unwrap().to_owned();
    fake.state()
        .slow_start_templates
        .push(slow_template.clone());
    let racer = ok(
        &mut rpc_ws,
        411,
        "agent.create",
        create(&keyed_ref, "Racer"),
    )
    .await;
    let racer_id = racer["agent"]["id"].as_str().unwrap().to_owned();
    timeout(wait(), async {
        while fake.state().creates.last().map(|c| c["templateID"].clone())
            != Some(json!(slow_template))
        {
            // timing-guard: poll interval until the racer's sandbox create reaches the fake E2B
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the racer's sandbox is being created");
    let mut remove_ws = connect(port, cfg.clone()).await;
    let removal = tokio::spawn(async move {
        ok(
            &mut remove_ws,
            1,
            "cloudMachines.remove",
            json!({ "provider": "e2b", "stopAgents": true }),
        )
        .await
    });
    timeout(wait(), async {
        let mut id = 420;
        loop {
            id += 1;
            let listed = ok(&mut rpc_ws, id, "cloudMachines.list", json!({})).await;
            if listed["providers"] == json!([]) {
                return;
            }
            // timing-guard: poll interval until remove has forgotten the settings
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("remove forgets the settings first");
    drain_events(&mut events, &mut seen_events).await;
    let creates_before = fake.state().creates.len();
    let late = rpc(&mut rpc_ws, 500, "agent.create", create(&keyed_ref, "Late")).await;
    assert_eq!(
        late["error"]["data"]["reason"], "not-configured",
        "a start during remove is refused: {late}"
    );
    let removed = removal.await.unwrap();
    transcript.push(removed.to_string());
    assert_eq!(removed["ok"], true, "{removed}");
    assert_eq!(
        fake.state().creates.len(),
        creates_before,
        "no late sandbox"
    );
    assert!(
        fake.state().sandboxes.is_empty(),
        "every sandbox made is deleted: {:?}",
        fake.state().sandboxes.keys().collect::<Vec<_>>()
    );
    let lost = agent_until(&mut rpc_ws, (&ws_id, &racer_id), 510, |a| {
        a.get("placementError").is_some()
    })
    .await;
    assert!(lost.get("nodeId").is_none(), "{lost}");
    let after = ok(&mut rpc_ws, 403, "cloudMachines.list", json!({})).await;
    assert_eq!(after, json!({ "providers": [], "machines": [] }), "{after}");

    // No secret anywhere the backend writes or answers.
    let mut secrets = vec![E2B_KEY.to_owned(), PROFILE_SECRET.to_owned()];
    secrets.extend(fake.envd_tokens());
    let minted = fake.executor_tokens();
    assert!(!minted.is_empty(), "the sandbox minted a credential");
    secrets.extend(minted);
    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    drain_events(&mut events, &mut seen_events).await;
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

/// The `cloud` of the machine `cloudMachines.list` shows for sandbox
/// `sandbox`, `None` once it is gone.
async fn cloud_of(rpc_ws: &mut common::TlsWs, id: i64, sandbox: &str) -> Option<Value> {
    let listed = ok(rpc_ws, id, "cloudMachines.list", json!({})).await;
    listed["machines"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|machine| machine["cloud"]["sandboxId"] == sandbox)
        .cloned()
}

/// `cloudMachines.list` until sandbox `sandbox`'s machine matches `check`
/// (`None`: no machine for it).
async fn machine_until(
    rpc_ws: &mut common::TlsWs,
    id: i64,
    sandbox: &str,
    check: impl Fn(Option<&Value>) -> bool,
) -> Option<Value> {
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(120));
    let mut id = id;
    loop {
        id += 1;
        let machine = cloud_of(rpc_ws, id, sandbox).await;
        if check(machine.as_ref()) {
            return machine;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "machine of {sandbox} never matched: {machine:?}"
        );
        // timing-guard: poll interval while the lifecycle supervisor acts
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The next `agent:failed` event of `agent_id` whose error holds `needle`.
async fn failure_containing(events: &mut common::TlsWs, agent_id: &str, needle: &str) -> String {
    timeout(wait(), async {
        loop {
            let failed = event(events, "agent:failed").await;
            let text = failed["data"].to_string();
            if failed["data"]["agentId"] == agent_id && text.contains(needle) {
                return text;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no agent:failed with {needle:?}"))
}

/// Read every event `events` has ready into `seen` (text frames), answering
/// pings, until it stays quiet for a moment.
async fn drain_events(events: &mut common::TlsWs, seen: &mut Vec<String>) {
    loop {
        match timeout(
            common::test_timeout(Duration::from_millis(200)),
            events.next(),
        )
        .await
        {
            Ok(Some(Ok(Message::Text(text)))) => seen.push(text.to_string()),
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = events.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(error))) => panic!("event socket failed: {error}"),
            Ok(None) => panic!("event socket closed"),
            Err(_) => return,
        }
    }
}

/// Request ids for [`keep_alive`], clear of the test's own.
static KEEP_ALIVE_ID: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1_000_000);

/// One cheap request on `rpc_ws`, so it reads (and answers) the backend's
/// pings: the backend drops a client that has not answered one for 60 s,
/// which a slow (coverage) run reaches while the test only polls the fake.
async fn keep_alive(rpc_ws: &mut common::TlsWs) {
    let id = KEEP_ALIVE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    ok(rpc_ws, id, "cloudMachines.list", json!({})).await;
}

/// Wait `total` while keeping `rpc_ws` answered.
async fn idle(rpc_ws: &mut common::TlsWs, total: Duration) {
    let until = tokio::time::Instant::now() + total;
    while tokio::time::Instant::now() < until {
        // timing-guard: a short step of a deliberate wait
        tokio::time::sleep(Duration::from_millis(500)).await;
        keep_alive(rpc_ws).await;
    }
}

/// A new connection subscribed to the workspace's agent events, opened
/// right before the step that waits for one: a connection left unread
/// across earlier steps may have been dropped for unanswered pings.
async fn agent_events(port: u16, cfg: Arc<ClientConfig>, ws_id: &str) -> common::TlsWs {
    let mut events = connect(port, cfg).await;
    ok(
        &mut events,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    events
}

/// Send `content` to the agent and wait for its reply, polling the
/// conversation on `rpc_ws` (so the socket keeps answering pings however
/// long the turn takes, for example a sandbox start on a slow runner).
async fn send_and_reply(
    rpc_ws: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    content: &str,
    id: i64,
) {
    ok(
        rpc_ws,
        id,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": content }),
    )
    .await;
    reply_containing(rpc_ws, (ws_id, agent_id), content, id + 1).await;
}

/// Wait until `fake` saw a call matching `check` about `sandbox`.
async fn call_until(
    fake: &FakeE2b,
    rpc_ws: &mut common::TlsWs,
    sandbox: &str,
    check: impl Fn(&[(String, String)]) -> bool,
) {
    timeout(common::test_timeout(Duration::from_secs(120)), async {
        let mut polls = 0_u32;
        while !check(&fake.calls_for(sandbox)) {
            // timing-guard: poll interval for the backend's next E2B call
            tokio::time::sleep(Duration::from_millis(50)).await;
            polls += 1;
            if polls.is_multiple_of(20) {
                keep_alive(rpc_ws).await;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("calls for {sandbox}: {:?}", fake.calls_for(sandbox)));
}

fn is_call(call: &(String, String), method: &str, suffix: &str) -> bool {
    call.0 == method && call.1.ends_with(suffix)
}

/// `cloudMachines.configure` params for E2B with `extra` members.
#[track_caller]
fn configure_params(extra: Value) -> Value {
    let mut params = json!({ "provider": "e2b" });
    let Value::Object(extra) = extra else {
        panic!("configure members must be an object: {extra}");
    };
    for (k, v) in extra {
        params[k] = v;
    }
    params
}

/// A backend against a fake E2B, with a one-commit repo, an API-key codex
/// profile (`keyed_ref`), E2B configured (never idle-paused or deleted
/// unless a test lowers it) and one connection for calls. A test opens
/// its event connection ([`agent_events`]) right before the step that
/// waits for an event.
struct CloudBackend {
    scratch: tempfile::TempDir,
    repo: std::path::PathBuf,
    fake: Arc<FakeE2b>,
    head_dir: std::path::PathBuf,
    head: common::DaemonGuard,
    port: u16,
    cfg: Arc<ClientConfig>,
    ws_id: String,
    keyed_ref: String,
    rpc_ws: common::TlsWs,
}

impl CloudBackend {
    /// `None` (the test skips) without node or git on PATH.
    async fn start(prefix: &str, env: &[(&str, &str)]) -> Option<Self> {
        if intent_providers::resolve_on_path("node").is_none()
            || intent_providers::resolve_on_path("git").is_none()
        {
            eprintln!("skipping E2B e2e: node or git not on PATH");
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
        let sandboxes = scratch.path().join("e2b");
        std::fs::create_dir_all(&sandboxes).unwrap();
        let fake = FakeE2b::start(&sandboxes, &fixture).await;
        let head_dir = scratch.path().join("head");
        let manifest_base = fake.manifest_base();
        let mut backend_env = vec![
            ("INTENTD_E2B_API_URL", fake.url.as_str()),
            ("INTENTD_E2B_ENVD_URL", fake.url.as_str()),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
            ("INTENTD_E2B_DAEMON_WAIT_SECS", "30"),
            ("INTENTD_E2B_MANIFEST_BASE_URL", manifest_base.as_str()),
        ];
        backend_env.extend_from_slice(env);
        let (head, port, cfg, ws_id) = start_backend_with(&head_dir, &repo, &backend_env).await;
        let mut rpc_ws = connect(port, cfg.clone()).await;
        ok(
            &mut rpc_ws,
            1,
            "client.hello",
            json!({ "clientId": "e2b-life", "name": "E2B lifecycle" }),
        )
        .await;
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
        ok(
            &mut rpc_ws,
            4,
            "cloudMachines.configure",
            configure_params(json!({
                "apiKey": E2B_KEY, "size": "4x8", "idlePauseMinutes": 1440, "pausedDeleteDays": 30,
            })),
        )
        .await;
        Some(Self {
            scratch,
            repo,
            fake,
            head_dir,
            head,
            port,
            cfg,
            ws_id,
            keyed_ref,
            rpc_ws,
        })
    }
}

/// The life of a sandbox against the fake E2B, with one settings minute
/// scaled to 100 ms and one day to 3 s. Proves every row of the lifecycle
/// table: the timeout is renewed while the sandbox lives; it is paused and
/// resumed before the run cap at a turn boundary, never during a turn;
/// `cloudMachines.pause` pauses it (E2B's refusal leaves it running) and a
/// new prompt resumes it and redials; a refused resume fails the turn with
/// `cloud-quota`; it pauses when idle; paused too long it is resumed, final
/// stopped and deleted, and the agent's next message starts a new sandbox;
/// a sandbox E2B removes during a turn fails that turn with `sandbox-lost`
/// and the next message starts another; `cloudMachines.kill` deletes it;
/// the reaper deletes a sandbox with this backend's metadata that no machine
/// knows, and leaves other backends' sandboxes alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cloud_sandbox_pauses_resumes_and_is_deleted() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping E2B lifecycle e2e: node or git not on PATH");
        return;
    }
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let scratch = common::test_tempdir_in("/tmp", "itd-e2bl-");
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
            ("INTENTD_E2B_DAEMON_WAIT_SECS", "30"),
            ("INTENTD_E2B_MINUTE_MS", "100"),
            ("INTENTD_E2B_DAY_MS", "3000"),
        ],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "e2b-life", "name": "E2B lifecycle" }),
    )
    .await;
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
    let configure = |extra: Value| {
        let mut params = json!({ "provider": "e2b" });
        for (k, v) in extra.as_object().unwrap() {
            params[k] = v.clone();
        }
        params
    };
    ok(
        &mut rpc_ws,
        4,
        "cloudMachines.configure",
        configure(json!({
            "apiKey": E2B_KEY, "size": "4x8", "idlePauseMinutes": 1440, "pausedDeleteDays": 30,
        })),
    )
    .await;
    let created = ok(
        &mut rpc_ws,
        5,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Life", "model": "default", "provider": keyed_ref,
            "placement": { "target": "e2b" },
        }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    let agent = (ws_id.as_str(), agent_id.as_str());
    send_and_reply(&mut rpc_ws, agent, "hello life", 10).await;
    let machine = machine_until(&mut rpc_ws, 20, "sbx1", |m| m.is_some())
        .await
        .unwrap();
    let machine_id = machine["id"].as_str().unwrap().to_owned();
    assert_eq!(machine["cloud"]["state"], "running", "{machine}");
    assert!(
        machine["cloud"].get("runningSince").is_none(),
        "internal times stay off the wire: {machine}"
    );

    // Renewed while it lives.
    call_until(&fake, &mut rpc_ws, "sbx1", |calls| {
        calls.iter().any(|c| is_call(c, "POST", "/sbx1/timeout"))
    })
    .await;

    // Before the run cap (50 minutes = 5 s), at a turn boundary: paused and
    // resumed at once, with no prompt.
    call_until(&fake, &mut rpc_ws, "sbx1", |calls| {
        calls
            .iter()
            .position(|c| is_call(c, "POST", "/sbx1/pause"))
            .is_some_and(|at| {
                calls[at..]
                    .iter()
                    .any(|c| is_call(c, "POST", "/sbx1/connect"))
            })
    })
    .await;
    machine_until(&mut rpc_ws, 100, "sbx1", |m| {
        m.is_some_and(|m| m["cloud"]["state"] == "running")
    })
    .await;
    send_and_reply(&mut rpc_ws, agent, "after the cap", 200).await;

    // cloudMachines.pause: E2B's refusal leaves it running.
    fake.state().pause_refusal = Some((503, json!({ "code": 503, "message": "node busy" })));
    let refused = rpc(
        &mut rpc_ws,
        210,
        "cloudMachines.pause",
        json!({ "machineId": machine_id }),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32603, "{refused}");
    assert_eq!(
        refused["error"]["data"]["code"], "cloud-machine-unavailable",
        "{refused}"
    );
    assert_eq!(refused["error"]["data"]["retryable"], true, "{refused}");
    assert_eq!(
        cloud_of(&mut rpc_ws, 211, "sbx1").await.unwrap()["cloud"]["state"],
        "running"
    );
    fake.state().pause_refusal = None;
    let paused = ok(
        &mut rpc_ws,
        212,
        "cloudMachines.pause",
        json!({ "machineId": machine_id }),
    )
    .await;
    assert_eq!(paused["machine"]["cloud"]["state"], "paused", "{paused}");
    assert!(fake.state().paused.contains("sbx1"));
    let again = ok(
        &mut rpc_ws,
        213,
        "cloudMachines.pause",
        json!({ "machineId": machine_id }),
    )
    .await;
    assert_eq!(again["machine"]["cloud"]["state"], "paused", "{again}");

    // A new prompt resumes it and redials.
    let seen_calls = fake.calls_for("sbx1").len();
    send_and_reply(&mut rpc_ws, agent, "after the pause", 220).await;
    assert!(
        fake.calls_for("sbx1")[seen_calls..]
            .iter()
            .any(|c| is_call(c, "POST", "/sbx1/connect")),
        "the prompt resumed the sandbox"
    );
    assert!(!fake.state().paused.contains("sbx1"));

    // A resume E2B refuses fails the turn with cloud-quota.
    ok(
        &mut rpc_ws,
        230,
        "cloudMachines.pause",
        json!({ "machineId": machine_id }),
    )
    .await;
    fake.state().connect_refusal = Some((
        429,
        json!({ "code": 429, "message": "You have reached the maximum number of concurrent E2B sandboxes" }),
    ));
    let mut events = agent_events(port, cfg.clone(), &ws_id).await;
    ok(
        &mut rpc_ws,
        231,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "refused resume" }),
    )
    .await;
    let failed = failure_containing(&mut events, &agent_id, "cloud-quota").await;
    assert!(failed.contains("running sandboxes"), "{failed}");
    fake.state().connect_refusal = None;
    ok(
        &mut rpc_ws,
        232,
        "agent.retry",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    reply_containing(&mut rpc_ws, agent, "refused resume", 233).await;

    // Idle for idlePauseMinutes: paused.
    ok(
        &mut rpc_ws,
        240,
        "cloudMachines.configure",
        configure(json!({ "idlePauseMinutes": 1 })),
    )
    .await;
    let paused_idle = machine_until(&mut rpc_ws, 250, "sbx1", |m| {
        m.is_some_and(|m| m["cloud"]["state"] == "paused")
    })
    .await
    .unwrap();
    assert!(
        paused_idle["cloud"]["runningSeconds"].as_u64().unwrap() >= 1,
        "{paused_idle}"
    );

    // Paused for pausedDeleteDays: resumed, final stopped, then deleted.
    ok(
        &mut rpc_ws,
        300,
        "cloudMachines.configure",
        configure(json!({ "pausedDeleteDays": 1 })),
    )
    .await;
    machine_until(&mut rpc_ws, 301, "sbx1", |m| m.is_none()).await;
    // The E2B delete follows the machine row's removal.
    call_until(&fake, &mut rpc_ws, "sbx1", |calls| {
        calls
            .iter()
            .any(|c| is_call(c, "DELETE", "/sandboxes/sbx1"))
    })
    .await;
    let calls = fake.calls_for("sbx1");
    let last_pause = calls
        .iter()
        .rposition(|c| is_call(c, "POST", "/sbx1/pause"))
        .unwrap();
    let resumed = calls[last_pause..]
        .iter()
        .position(|c| is_call(c, "POST", "/sbx1/connect"))
        .expect("resumed before the delete");
    let deleted = calls[last_pause..]
        .iter()
        .position(|c| is_call(c, "DELETE", "/sandboxes/sbx1"))
        .expect("deleted");
    assert!(resumed < deleted, "{calls:?}");
    let refs = format!("refs/intent/ws/{ws_id}/agents/{agent_id}");
    assert!(
        try_rev(&repo, &format!("{refs}/wip")).is_some()
            || try_rev(&repo, &format!("{refs}/head")).is_some(),
        "the final stop kept the agent's work"
    );
    let sandboxless = agent_until(&mut rpc_ws, agent, 350, |a| a.get("nodeId").is_none()).await;
    assert_eq!(sandboxless["placement"]["target"], "e2b", "{sandboxless}");

    // The next message starts a new sandbox.
    ok(
        &mut rpc_ws,
        400,
        "cloudMachines.configure",
        configure(json!({ "idlePauseMinutes": 1440, "pausedDeleteDays": 30 })),
    )
    .await;
    ok(
        &mut rpc_ws,
        401,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "on a new sandbox" }),
    )
    .await;
    reply_containing(&mut rpc_ws, agent, "on a new sandbox", 402).await;
    let second = machine_until(&mut rpc_ws, 410, "sbx2", |m| m.is_some())
        .await
        .unwrap();
    let second_id = second["id"].as_str().unwrap().to_owned();

    // E2B says the sandbox is missing but its list fails: unknown, not lost.
    {
        let mut state = fake.state();
        state.timeouts_missing = true;
        state.list_failing = true;
    }
    let before = fake.calls_for("sbx2").len();
    call_until(&fake, &mut rpc_ws, "sbx2", |calls| {
        calls[before.min(calls.len())..]
            .iter()
            .filter(|c| is_call(c, "POST", "/sbx2/timeout"))
            .count()
            >= 2
    })
    .await;
    {
        let mut state = fake.state();
        state.timeouts_missing = false;
        state.list_failing = false;
    }
    assert!(
        cloud_of(&mut rpc_ws, 420, "sbx2").await.is_some(),
        "a sandbox whose state is unknown is kept"
    );

    // A turn in flight: no pause, neither asked for nor before the cap.
    ok(
        &mut rpc_ws,
        500,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "block here" }),
    )
    .await;
    let busy = timeout(wait(), async {
        let mut id = 510;
        loop {
            id += 1;
            // timing-guard: the blocked turn needs a moment to reach the sandbox (a pause before that is resumed by the turn)
            tokio::time::sleep(Duration::from_millis(500)).await;
            let refused = rpc(
                &mut rpc_ws,
                id,
                "cloudMachines.pause",
                json!({ "machineId": second_id }),
            )
            .await;
            if refused["error"]["data"]["code"] == "machine-in-use" {
                return refused;
            }
        }
    })
    .await
    .expect("pause refused during the turn");
    assert_eq!(busy["error"]["data"]["runningAgents"], 1, "{busy}");
    let before = fake.calls_for("sbx2").len();
    // timing-guard: longer than the run cap (5 s), so a planned pause would have run
    idle(&mut rpc_ws, Duration::from_secs(7)).await;
    assert!(
        !fake.calls_for("sbx2")[before..]
            .iter()
            .any(|c| is_call(c, "POST", "/sbx2/pause")),
        "no pause during a turn: {:?}",
        fake.calls_for("sbx2")
    );

    // E2B removes the sandbox mid-turn: the turn fails with sandbox-lost,
    // and the agent is left without a sandbox.
    let mut events = agent_events(port, cfg.clone(), &ws_id).await;
    fake.vanish("sbx2");
    let failed = failure_containing(&mut events, &agent_id, "sandbox-lost").await;
    assert!(
        failed.contains("next message starts a new sandbox"),
        "{failed}"
    );
    let lost = agent_until(&mut rpc_ws, agent, 600, |a| {
        a.get("nodeId").is_none() && a.get("placementError").is_some()
    })
    .await;
    assert_eq!(
        lost["placementError"]["data"]["reason"], "sandbox-lost",
        "{lost}"
    );
    machine_until(&mut rpc_ws, 650, "sbx2", |m| m.is_none()).await;
    // Retrying the interrupted turn starts a new sandbox for it.
    ok(
        &mut rpc_ws,
        700,
        "agent.retry",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let third = machine_until(&mut rpc_ws, 710, "sbx3", |m| m.is_some())
        .await
        .unwrap();
    agent_until(&mut rpc_ws, agent, 760, |a| a.get("nodeId").is_some()).await;

    // cloudMachines.kill (its turn still blocked): final stop, then delete.
    let killed = ok(
        &mut rpc_ws,
        800,
        "cloudMachines.kill",
        json!({ "machineId": third["id"] }),
    )
    .await;
    assert_eq!(killed["ok"], true, "{killed}");
    assert!(fake.state().deletes.contains(&"sbx3".to_owned()));
    assert!(cloud_of(&mut rpc_ws, 801, "sbx3").await.is_none());
    let unknown = rpc(
        &mut rpc_ws,
        802,
        "cloudMachines.kill",
        json!({ "machineId": third["id"] }),
    )
    .await;
    assert_eq!(unknown["error"]["code"], -32602, "{unknown}");
    assert_eq!(unknown["error"]["data"]["code"], "not-found", "{unknown}");

    // The reaper: this backend's orphan goes, another backend's stays.
    let backend_id = fake.state().creates[0]["metadata"]["intentBackendId"]
        .as_str()
        .unwrap()
        .to_owned();
    fake.state().listed_only.extend([
        json!({ "sandboxID": "orphan1", "startedAt": "2026-01-01T00:00:00Z",
                "metadata": { "intentBackendId": backend_id, "agentId": "gone" } }),
        json!({ "sandboxID": "foreign1", "startedAt": "2026-01-01T00:00:00Z",
                "metadata": { "intentBackendId": "another-backend", "agentId": "theirs" } }),
        json!({ "sandboxID": "undated1",
                "metadata": { "intentBackendId": backend_id, "agentId": "gone-too" } }),
    ]);
    timeout(common::test_timeout(Duration::from_secs(60)), async {
        while !fake.state().deletes.contains(&"orphan1".to_owned()) {
            keep_alive(&mut rpc_ws).await;
            // timing-guard: poll interval for the reaper (every 10 minutes = 1 s)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the orphan is reaped");
    assert!(
        !fake.state().deletes.contains(&"undated1".to_owned()),
        "an orphan without a start time gets the grace from first sight"
    );
    timeout(common::test_timeout(Duration::from_secs(60)), async {
        while !fake.state().deletes.contains(&"undated1".to_owned()) {
            keep_alive(&mut rpc_ws).await;
            // timing-guard: poll interval for the reaper after the 1.5 s grace
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the undated orphan is reaped after the grace");
    assert!(!fake.state().deletes.contains(&"foreign1".to_owned()));

    // No secret in the backend's log.
    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    let mut secrets = vec![E2B_KEY.to_owned(), PROFILE_SECRET.to_owned()];
    secrets.extend(fake.envd_tokens());
    secrets.extend(fake.executor_tokens());
    for secret in &secrets {
        assert!(!log.contains(secret.as_str()), "a secret reached the log");
    }
}

/// A paused sandbox that comes back as an intentd build this backend shares
/// no link protocol with (`machine-update-required`) is deleted without a
/// final stop: the prompt that resumed it fails with a message saying so,
/// the agent stays on E2B without a sandbox and keeps the work it pushed,
/// and retrying the prompt starts a new sandbox from that work.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_sandbox_that_cannot_link_after_an_update_is_deleted() {
    let Some(CloudBackend {
        scratch: _scratch,
        repo,
        fake,
        head_dir,
        head: _head,
        port,
        cfg,
        ws_id,
        keyed_ref,
        mut rpc_ws,
    }) = CloudBackend::start("itd-e2bu-", &[]).await
    else {
        return;
    };
    let created = ok(
        &mut rpc_ws,
        5,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Outdated", "model": "default", "provider": keyed_ref,
            "placement": { "target": "e2b" },
        }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    let agent = (ws_id.as_str(), agent_id.as_str());
    send_and_reply(&mut rpc_ws, agent, "before the update", 10).await;
    let machine = machine_until(&mut rpc_ws, 20, "sbx1", |m| m.is_some())
        .await
        .unwrap();
    let sandbox_port = u16::try_from(machine["port"].as_u64().unwrap()).unwrap();
    let paused = ok(
        &mut rpc_ws,
        30,
        "cloudMachines.pause",
        json!({ "machineId": machine["id"] }),
    )
    .await;
    assert_eq!(paused["machine"]["cloud"]["state"], "paused", "{paused}");
    let refs = format!("refs/intent/ws/{ws_id}/agents/{agent_id}");
    let pushed = (
        try_rev(&repo, &format!("{refs}/head")),
        try_rev(&repo, &format!("{refs}/wip")),
    );

    // The backend was updated past the sandbox's link protocol.
    fake.state()
        .outdated
        .insert("sbx1".to_owned(), sandbox_port);
    let mut events = agent_events(port, cfg.clone(), &ws_id).await;
    ok(
        &mut rpc_ws,
        40,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "after the update" }),
    )
    .await;
    let failed = failure_containing(&mut events, &agent_id, "machine-update-required").await;
    assert!(failed.contains("was deleted"), "{failed}");
    assert!(
        failed.contains("next message starts a new sandbox"),
        "{failed}"
    );
    machine_until(&mut rpc_ws, 50, "sbx1", |m| m.is_none()).await;
    call_until(&fake, &mut rpc_ws, "sbx1", |calls| {
        calls
            .iter()
            .any(|c| is_call(c, "DELETE", "/sandboxes/sbx1"))
    })
    .await;
    let sandboxless = agent_until(&mut rpc_ws, agent, 100, |a| a.get("nodeId").is_none()).await;
    assert_eq!(sandboxless["placement"]["target"], "e2b", "{sandboxless}");
    assert_eq!(
        (
            try_rev(&repo, &format!("{refs}/head")),
            try_rev(&repo, &format!("{refs}/wip")),
        ),
        pushed,
        "the agent keeps the work it pushed"
    );

    // Retrying the prompt starts a new sandbox.
    ok(
        &mut rpc_ws,
        200,
        "agent.retry",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    reply_containing(&mut rpc_ws, agent, "after the update", 201).await;
    machine_until(&mut rpc_ws, 300, "sbx2", |m| m.is_some()).await;
    assert_no_secret_logged(&head_dir, &fake);
}

/// What the agent echoes of its prompt after `content`, once it replied.
async fn echoed(rpc_ws: &mut common::TlsWs, agent: (&str, &str), content: &str, id: i64) -> String {
    ok(
        rpc_ws,
        id,
        "agent.sendMessage",
        json!({ "workspaceId": agent.0, "agentId": agent.1, "content": content }),
    )
    .await;
    reply_containing(rpc_ws, agent, content, id + 1).await
}

/// Retire `agent` and wait until its sandbox `sandbox` is gone, then
/// restore it.
async fn retire_and_restore(
    rpc_ws: &mut common::TlsWs,
    agent: (&str, &str),
    sandbox: &str,
    id: i64,
) {
    let params = json!({ "workspaceId": agent.0, "agentId": agent.1 });
    ok(rpc_ws, id, "agent.retire", params.clone()).await;
    machine_until(rpc_ws, id + 1, sandbox, |m| m.is_none()).await;
    ok(rpc_ws, id + 500, "agent.restore", params).await;
}

/// A remote agent relaunched on another machine than its last run is told
/// so on its first turn there, in one notice that names the machine and
/// also carries the unapplied-work notice. Its first launch, a resume of
/// the same paused sandbox and its later turns get none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restored_agent_is_told_it_runs_on_a_new_machine() {
    let Some(CloudBackend {
        scratch,
        repo,
        fake,
        head_dir,
        head: _head,
        port: _,
        cfg: _,
        ws_id,
        keyed_ref,
        mut rpc_ws,
    }) = CloudBackend::start("itd-e2bn-", &[]).await
    else {
        return;
    };
    const NOTICE: &str = "on a new machine";
    let created = ok(
        &mut rpc_ws,
        5,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Mover", "model": "default", "provider": keyed_ref,
            "placement": { "target": "e2b" },
        }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    let agent = (ws_id.as_str(), agent_id.as_str());
    ok(
        &mut rpc_ws,
        6,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Keeper", "model": "default", "provider": keyed_ref }),
    )
    .await;

    // The first launch: no notice.
    let first = echoed(&mut rpc_ws, agent, "first launch", 10).await;
    assert!(!first.contains(NOTICE), "{first}");
    let machine = machine_until(&mut rpc_ws, 20, "sbx1", |m| m.is_some())
        .await
        .unwrap();

    // The same sandbox, paused and resumed: no notice.
    let paused = ok(
        &mut rpc_ws,
        30,
        "cloudMachines.pause",
        json!({ "machineId": machine["id"] }),
    )
    .await;
    assert_eq!(paused["machine"]["cloud"]["state"], "paused", "{paused}");
    let resumed = echoed(&mut rpc_ws, agent, "after the pause", 40).await;
    assert!(!resumed.contains(NOTICE), "{resumed}");

    // Retired then restored: a new sandbox, one notice naming it.
    retire_and_restore(&mut rpc_ws, agent, "sbx1", 100).await;
    let moved = echoed(&mut rpc_ws, agent, "after restore one", 700).await;
    let second = machine_until(&mut rpc_ws, 710, "sbx2", |m| m.is_some())
        .await
        .unwrap();
    let name = second["name"].as_str().unwrap();
    assert!(
        moved.contains(&format!("You're {NOTICE} ({name})")),
        "{moved}"
    );
    assert!(moved.contains("Re-run any setup you need"), "{moved}");
    assert_eq!(moved.matches("Note from Intent").count(), 1, "{moved}");
    let later = echoed(&mut rpc_ws, agent, "a later turn", 800).await;
    assert!(!later.contains(NOTICE), "{later}");

    // Saved work that does not apply on the new machine: still one notice,
    // which names the new machine and where the work is kept.
    retire_and_restore(&mut rpc_ws, agent, "sbx2", 900).await;
    let refs = format!("refs/intent/ws/{ws_id}/agents/{agent_id}");
    let base = git(&repo, &["rev-parse", "HEAD"]);
    let scratch_tree = scratch.path().join("conflict");
    git(
        &repo,
        &[
            "worktree",
            "add",
            "--quiet",
            "--detach",
            scratch_tree.to_str().unwrap(),
            &base,
        ],
    );
    let commit = |text: &str| {
        git(&scratch_tree, &["checkout", "--quiet", "--detach", &base]);
        std::fs::write(scratch_tree.join("README.md"), text).unwrap();
        git(&scratch_tree, &["commit", "--quiet", "-am", text]);
        git(&scratch_tree, &["rev-parse", "HEAD"])
    };
    let wip = commit("wip\n");
    let saved_head = commit("head\n");
    git(&repo, &["update-ref", &format!("{refs}/wip"), &wip]);
    git(&repo, &["update-ref", &format!("{refs}/head"), &saved_head]);
    git(
        &repo,
        &[
            "worktree",
            "remove",
            "--force",
            scratch_tree.to_str().unwrap(),
        ],
    );
    let combined = echoed(&mut rpc_ws, agent, "after restore two", 1600).await;
    let third = machine_until(&mut rpc_ws, 1610, "sbx3", |m| m.is_some())
        .await
        .unwrap();
    let name = third["name"].as_str().unwrap();
    assert!(
        combined.contains(&format!("You're {NOTICE} ({name})")),
        "{combined}"
    );
    assert!(
        combined.contains(&format!("{refs}/unapplied/{wip}")),
        "{combined}"
    );
    assert_eq!(
        combined.matches("Note from Intent").count(),
        1,
        "{combined}"
    );
    assert_eq!(
        try_rev(&repo, &format!("{refs}/unapplied/{wip}")),
        Some(wip)
    );
    assert_no_secret_logged(&head_dir, &fake);
}

/// No E2B key, envd token, executor token or profile secret in the
/// backend's log.
#[track_caller]
fn assert_no_secret_logged(head_dir: &Path, fake: &FakeE2b) {
    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    let mut secrets = vec![E2B_KEY.to_owned(), PROFILE_SECRET.to_owned()];
    secrets.extend(fake.envd_tokens());
    secrets.extend(fake.executor_tokens());
    for secret in &secrets {
        assert!(!log.contains(secret.as_str()), "a secret reached the log");
    }
}
