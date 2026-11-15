//! Cloud homes against the fake E2B (monorepo `docs/protocol/methods/nodes.md`,
//! "Choosing the home" and "Cloud homes"), on an orchestrator serving homes
//! with only a cloud entry on its machine list:
//!
//! 1. A workspace created with no agent gets the entry as its home and no
//!    sandbox. Its first file call starts a home-only sandbox: the home is
//!    `starting` meanwhile (a call that waits too long fails
//!    `home-unavailable` with `starting: true`), then `attached` on that
//!    sandbox, and the call succeeds.
//! 2. A workspace whose initial agent is placed on the entry: that agent's
//!    sandbox becomes the home; a terminal runs in the home checkout there
//!    and file calls work.
//! 3. Both sandboxes' `cloud.startedBy` is the workspace owner.
//! 4. A home whose sandbox fails to start is detached at once, and calls
//!    fail `home-unavailable` without `starting`.

use base64::Engine as _;

use super::super::homes::HOMES;
use super::*;

/// The workspace's home, from `workspace.get`.
async fn home_of(rpc_ws: &mut common::TlsWs, id: i64, ws_id: &str) -> Value {
    let got = ok(rpc_ws, id, "workspace.get", json!({ "workspaceId": ws_id })).await;
    let home = &got["workspace"]["home"];
    if home.is_object() {
        home.clone()
    } else {
        got["home"].clone()
    }
}

/// `workspace.get` until the home matches `check`.
async fn home_until(
    rpc_ws: &mut common::TlsWs,
    id: &mut i64,
    ws_id: &str,
    what: &str,
    check: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        *id += 1;
        let home = home_of(rpc_ws, *id, ws_id).await;
        if check(&home) {
            return home;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out: {what}: {home}"
        );
        // timing-guard: poll while the home's sandbox starts
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Call `method` until it succeeds; every failure meanwhile must be
/// `home-unavailable` with `starting: true`. Returns the result and how
/// many calls failed that way.
async fn call_while_starting(
    rpc_ws: &mut common::TlsWs,
    id: &mut i64,
    method: &str,
    params: &Value,
) -> (Value, usize) {
    let deadline = tokio::time::Instant::now() + wait();
    let mut starting = 0;
    loop {
        *id += 1;
        let reply = rpc(rpc_ws, *id, method, params.clone()).await;
        if reply.get("error").is_none() {
            return (reply["result"].clone(), starting);
        }
        let data = &reply["error"]["data"];
        assert_eq!(data["code"], "home-unavailable", "{reply}");
        assert_eq!(data["starting"], true, "{reply}");
        assert_eq!(data["retryable"], true, "{reply}");
        starting += 1;
        assert!(
            tokio::time::Instant::now() < deadline,
            "the home never started: {reply}"
        );
    }
}

/// The cloud machine row whose `nodeId` is `node_id`.
async fn machine_of(rpc_ws: &mut common::TlsWs, id: i64, node_id: &str) -> Value {
    let listed = ok(rpc_ws, id, "cloudMachines.list", json!({})).await;
    listed["machines"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|machine| machine["id"] == node_id)
        .cloned()
        .unwrap_or_else(|| panic!("no cloud machine {node_id}: {listed}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cloud_entry_is_a_home_that_starts_lazily() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping cloud homes e2e: node or git not on PATH");
        return;
    }
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let scratch = common::test_tempdir_in("/tmp", "itd-e2b-homes-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let branch = git(&repo, &["rev-parse", "--abbrev-ref", "HEAD"]);
    let fake = FakeE2b::start(scratch, &fixture).await;
    fake.state()
        .sandbox_env
        .push((HOMES.0.to_owned(), HOMES.1.to_owned()));

    let head_dir = fake.scratch().join("head");
    let (head, _, _, _) = start_backend(&head_dir, &repo).await;
    drop(head);
    super::super::orchestrator::orchestrator_config(&head_dir);
    let manifest_base = fake.manifest_base();
    let (_head, port, cfg) = serve_backend(
        &head_dir,
        &[
            ("INTENTD_E2B_API_URL", &fake.url),
            ("INTENTD_E2B_ENVD_URL", &fake.url),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
            ("INTENTD_E2B_DAEMON_WAIT_SECS", "30"),
            ("INTENTD_E2B_MANIFEST_BASE_URL", &manifest_base),
            HOMES,
        ],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "e2b-homes", "name": "E2B homes" }),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"] }),
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
    let configured = ok(
        &mut rpc_ws,
        4,
        "cloudMachines.configure",
        configure_params(json!({
            "apiKey": E2B_KEY, "size": "4x8", "idlePauseMinutes": 1440, "pausedDeleteDays": 30,
        })),
    )
    .await;
    let entry = configured["provider"]["id"].as_str().unwrap().to_owned();
    let template = configured["provider"]["template"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut id = 10;

    // 1. No agent: the entry is the home, and nothing starts yet.
    id += 1;
    let created = ok(
        &mut rpc_ws,
        id,
        "workspace.create",
        json!({ "repositoryPath": repo.to_string_lossy(), "baseRef": branch }),
    )
    .await;
    let lazy = created["workspace"]["id"].as_str().unwrap().to_owned();
    let owner = created["workspace"]["ownerPrincipalId"]
        .as_str()
        .unwrap()
        .to_owned();
    let home = &created["workspace"]["home"];
    assert_eq!(home["cloudMachineId"], entry.as_str(), "{created}");
    assert!(home.get("nodeId").is_none(), "{created}");
    assert_eq!(home["state"], "attached", "{created}");
    let lazy_home = home["homeId"].as_str().unwrap().to_owned();
    assert!(fake.state().creates.is_empty(), "no sandbox before a use");

    // The first file call starts a home-only sandbox and succeeds once it
    // runs; each call waits up to 10 s for it meanwhile.
    let write = json!({ "workspaceId": lazy, "path": "lazy.txt", "content": "lazy home\n" });
    let (_, waited) = call_while_starting(&mut rpc_ws, &mut id, "file.write", &write).await;
    eprintln!("calls that failed while the home started: {waited}");
    assert_eq!(fake.state().creates.len(), 1, "one home-only sandbox");
    let lazy_sandbox = fake.state().metadata.keys().next().unwrap().clone();
    assert_eq!(
        fake.state().metadata[&lazy_sandbox]["agentId"],
        format!("home-{lazy}"),
        "the sandbox is the home's, not an agent's"
    );
    id += 1;
    let read = ok(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": lazy, "path": "lazy.txt" }),
    )
    .await;
    assert_eq!(read, "lazy home\n", "{read}");
    let home = home_until(
        &mut rpc_ws,
        &mut id,
        &lazy,
        "the lazy home attached",
        |home| home["state"] == "attached" && home["nodeId"].is_string(),
    )
    .await;
    assert_eq!(home["homeId"], lazy_home.as_str(), "{home}");
    let lazy_node = home["nodeId"].as_str().unwrap().to_owned();
    id += 1;
    let machine = machine_of(&mut rpc_ws, id, &lazy_node).await;
    assert_eq!(
        machine["cloud"]["sandboxId"],
        lazy_sandbox.as_str(),
        "{machine}"
    );
    assert_eq!(
        machine["cloud"]["startedBy"]["principalId"],
        owner.as_str(),
        "{machine}"
    );
    // It was announced as starting, then attached.
    let mut seen = Vec::new();
    drain_events(&mut events, &mut seen).await;
    let states: Vec<String> = seen
        .iter()
        .filter_map(|text| serde_json::from_str::<Value>(text).ok())
        .map(|frame| frame["params"]["event"].clone())
        .filter(|event| event["data"]["workspaceId"] == lazy.as_str())
        .filter_map(|event| {
            event["data"]["changes"]["home"]["state"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    let starting = states.iter().position(|state| state == "starting");
    let attached = states.iter().rposition(|state| state == "attached");
    assert!(
        matches!((starting, attached), (Some(s), Some(a)) if s < a),
        "home states announced: {states:?}"
    );

    // 2. An initial agent on the entry: its sandbox becomes the home.
    id += 1;
    let created = ok(
        &mut rpc_ws,
        id,
        "workspace.create",
        json!({
            "repositoryPath": repo.to_string_lossy(),
            "baseRef": branch,
            "initialAgent": {
                "model": "default", "provider": keyed_ref, "placement": { "target": "e2b" },
            },
        }),
    )
    .await;
    let with_agent = created["workspace"]["id"].as_str().unwrap().to_owned();
    let agent_id = created["initialAgent"]["id"]
        .as_str()
        .or_else(|| created["initialAgent"]["agentId"].as_str())
        .unwrap()
        .to_owned();
    let home = &created["workspace"]["home"];
    assert_eq!(home["cloudMachineId"], entry.as_str(), "{created}");
    let agent_home = home["homeId"].as_str().unwrap().to_owned();
    let home = home_until(
        &mut rpc_ws,
        &mut id,
        &with_agent,
        "the agent's sandbox became the home",
        |home| home["state"] == "attached" && home["nodeId"].is_string(),
    )
    .await;
    let agent_node = home["nodeId"].as_str().unwrap().to_owned();
    assert_ne!(agent_node, lazy_node);
    assert_eq!(
        fake.state().creates.len(),
        2,
        "no home-only sandbox next to the agent's"
    );
    id += 1;
    let machine = machine_of(&mut rpc_ws, id, &agent_node).await;
    assert_eq!(machine["cloud"]["agentId"], agent_id.as_str(), "{machine}");
    assert_eq!(
        machine["cloud"]["startedBy"]["principalId"],
        owner.as_str(),
        "{machine}"
    );
    // A terminal runs in the home checkout on the agent's sandbox.
    let b64 = base64::engine::general_purpose::STANDARD;
    id += 1;
    let terminal = ok(
        &mut rpc_ws,
        id,
        "terminal.create",
        json!({ "workspaceId": with_agent, "cols": 80, "rows": 24, "command": "/bin/sh" }),
    )
    .await;
    let terminal_id = terminal["terminalId"].as_str().unwrap().to_owned();
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "terminal.write",
        json!({ "terminalId": terminal_id, "data": b64.encode("echo at:$(pwd -P)\n") }),
    )
    .await;
    let expected = format!("/homes/{agent_home}/work");
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        id += 1;
        let buffer = ok(
            &mut rpc_ws,
            id,
            "terminal.getBuffer",
            json!({ "terminalId": terminal_id }),
        )
        .await;
        let text = String::from_utf8_lossy(&b64.decode(buffer["data"].as_str().unwrap()).unwrap())
            .into_owned();
        if text.contains(&expected) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the home terminal never answered {expected:?}: {text:?}"
        );
        // timing-guard: poll while the sandbox streams the terminal's output
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "terminal.kill",
        json!({ "terminalId": terminal_id }),
    )
    .await;
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "file.write",
        json!({ "workspaceId": with_agent, "path": "agent-home.txt", "content": "on the agent's sandbox\n" }),
    )
    .await;
    id += 1;
    let listed = ok(
        &mut rpc_ws,
        id,
        "file.list",
        json!({ "workspaceId": with_agent, "path": "." }),
    )
    .await;
    assert!(listed.to_string().contains("agent-home.txt"), "{listed}");

    // 4. A home whose sandbox fails to start is detached at once.
    fake.state().failed_mint_templates.push(template);
    id += 1;
    let created = ok(
        &mut rpc_ws,
        id,
        "workspace.create",
        json!({ "repositoryPath": repo.to_string_lossy(), "baseRef": branch }),
    )
    .await;
    let failing = created["workspace"]["id"].as_str().unwrap().to_owned();
    id += 1;
    let reply = rpc(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": failing, "path": "README.md" }),
    )
    .await;
    assert_eq!(
        reply["error"]["data"]["code"], "home-unavailable",
        "{reply}"
    );
    let home = home_until(
        &mut rpc_ws,
        &mut id,
        &failing,
        "the failed home detached",
        |home| home["state"] == "detached",
    )
    .await;
    assert!(home.get("nodeId").is_none(), "{home}");
    id += 1;
    let reply = rpc(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": failing, "path": "README.md" }),
    )
    .await;
    assert_eq!(reply["error"]["code"], -32603, "{reply}");
    assert_eq!(
        reply["error"]["data"]["code"], "home-unavailable",
        "{reply}"
    );
    assert!(
        reply["error"]["data"].get("starting").is_none(),
        "a detached home is not starting: {reply}"
    );
    assert_no_secret_logged(&head_dir, &fake);
}

/// A host member's token: a person who may create workspaces here.
const MEMBER_TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
/// A credentialed guest's token: a collaborator once added to a workspace.
const GUEST_TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// Seed a host member and a credentialed guest into the stopped backend's
/// store at `head_dir`. Returns the guest's principal id.
async fn seed_people(head_dir: &Path) -> String {
    let store = intent_store::Store::open(&head_dir.join("intentd.db"))
        .await
        .unwrap();
    let hash = |token: &str| {
        Sha256::digest(token.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let mut guest_id = String::new();
    for (login, token) in [("member", MEMBER_TOKEN), ("guest", GUEST_TOKEN)] {
        let now = intent_core::now_iso();
        let principal = intent_core::Principal {
            id: intent_core::PrincipalId::new(),
            identity: None,
            github_user_id: None,
            login: Some(login.into()),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        store.upsert_principal(&principal).await.unwrap();
        store
            .insert_principal_credential(&principal.id, &hash(token))
            .await
            .unwrap();
        if login == "member" {
            sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
                .bind(principal.id.as_str())
                .bind(&now)
                .execute(store.write_pool())
                .await
                .unwrap();
        } else {
            guest_id = principal.id.to_string();
        }
    }
    guest_id
}

/// An orchestrator serving homes, with only a cloud entry on its machine
/// list, against a fake E2B; a host member and a guest are known. Fields
/// drop in order: the connection and the backend go before the fake, which
/// owns the scratch dir.
struct CloudHomes {
    rpc_ws: common::TlsWs,
    _head: common::DaemonGuard,
    port: u16,
    cfg: Arc<ClientConfig>,
    repo: PathBuf,
    branch: String,
    keyed_ref: String,
    guest_id: String,
    id: i64,
    fake: Arc<FakeE2b>,
}

impl CloudHomes {
    /// `None` (the test skips) without node or git on PATH. `env` goes to
    /// the backend; the entry pauses a sandbox after `idle_minutes`.
    async fn start(prefix: &str, env: &[(&str, &str)], idle_minutes: u32) -> Option<Self> {
        if intent_providers::resolve_on_path("node").is_none()
            || intent_providers::resolve_on_path("git").is_none()
        {
            eprintln!("skipping cloud homes e2e: node or git not on PATH");
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
        let branch = git(&repo, &["rev-parse", "--abbrev-ref", "HEAD"]);
        let fake = FakeE2b::start(scratch, &fixture).await;
        fake.state()
            .sandbox_env
            .push((HOMES.0.to_owned(), HOMES.1.to_owned()));
        let head_dir = fake.scratch().join("head");
        let (head, _, _, _) = start_backend(&head_dir, &repo).await;
        drop(head);
        super::super::orchestrator::orchestrator_config(&head_dir);
        let guest_id = seed_people(&head_dir).await;
        let manifest_base = fake.manifest_base();
        let mut backend_env = vec![
            ("INTENTD_E2B_API_URL", fake.url.as_str()),
            ("INTENTD_E2B_ENVD_URL", fake.url.as_str()),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
            ("INTENTD_E2B_DAEMON_WAIT_SECS", "30"),
            ("INTENTD_E2B_MANIFEST_BASE_URL", manifest_base.as_str()),
            HOMES,
        ];
        backend_env.extend_from_slice(env);
        let (head, port, cfg) = serve_backend(&head_dir, &backend_env).await;
        let mut rpc_ws = connect(port, cfg.clone()).await;
        ok(
            &mut rpc_ws,
            1,
            "client.hello",
            json!({ "clientId": "e2b-home-life", "name": "E2B home lifecycle" }),
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
        ok(
            &mut rpc_ws,
            3,
            "cloudMachines.configure",
            configure_params(json!({
                "apiKey": E2B_KEY, "size": "4x8", "idlePauseMinutes": idle_minutes,
                "pausedDeleteDays": 30,
            })),
        )
        .await;
        Some(Self {
            rpc_ws,
            _head: head,
            port,
            cfg,
            repo,
            branch,
            keyed_ref,
            guest_id,
            id: 10,
            fake,
        })
    }

    fn next(&mut self) -> i64 {
        self.id += 1;
        self.id
    }

    async fn ok(&mut self, method: &str, params: Value) -> Value {
        let id = self.next();
        ok(&mut self.rpc_ws, id, method, params).await
    }

    /// A connection as the person whose token is `token`.
    async fn connect_as(&self, token: &str) -> common::TlsWs {
        common::wss_connect_with_retry(
            self.port,
            self.cfg.clone(),
            &format!("wss://localhost:{}/ws?token={token}", self.port),
        )
        .await
    }

    /// The sandbox E2B made for `owner` (an agent id, or `home-<workspace>`).
    fn sandbox_of(&self, owner: &str) -> String {
        self.fake
            .state()
            .metadata
            .iter()
            .find(|(_, metadata)| metadata["agentId"] == owner)
            .map(|(id, _)| id.clone())
            .unwrap_or_else(|| panic!("no sandbox for {owner}"))
    }

    /// The data dir of sandbox `sandbox`'s daemon.
    fn sandbox_dir(&self, sandbox: &str) -> PathBuf {
        self.fake.state().sandboxes[sandbox].dir.clone()
    }

    fn deleted(&self, sandbox: &str) -> bool {
        self.fake.state().deletes.iter().any(|id| id == sandbox)
    }

    fn pause_calls(&self, sandbox: &str) -> usize {
        let path = format!("/{sandbox}/pause");
        self.fake
            .state()
            .calls
            .iter()
            .filter(|(method, at)| method == "POST" && at.ends_with(&path))
            .count()
    }

    /// The terminal buffer of `terminal_id` until it holds `needle`.
    async fn terminal_until(&mut self, terminal_id: &str, needle: &str) {
        let b64 = base64::engine::general_purpose::STANDARD;
        let deadline = tokio::time::Instant::now() + wait();
        loop {
            let buffer = self
                .ok("terminal.getBuffer", json!({ "terminalId": terminal_id }))
                .await;
            let text =
                String::from_utf8_lossy(&b64.decode(buffer["data"].as_str().unwrap()).unwrap())
                    .into_owned();
            if text.contains(needle) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the home terminal never printed {needle:?}: {text:?}"
            );
            // timing-guard: poll while the sandbox streams the terminal's output
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// A cloud home's sandbox is co-owned by the workspace (nodes.md "Cloud
/// homes" 1-2, "The home checkout" 1):
///
/// 1. The initial agent placed on the entry with no checkout works in the
///    home checkout on its sandbox, which became the home.
/// 2. Deleting that agent keeps the sandbox: the home still answers.
/// 3. Archiving the workspace deletes the sandbox and its machine row.
/// 4. A home-only sandbox of a workspace a host member owns: a
///    collaborator can neither pause nor kill it; the workspace owner
///    pauses it, then kills it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cloud_home_sandbox_is_co_owned_by_its_workspace() {
    let Some(mut homes) = CloudHomes::start("itd-e2b-coown-", &[], 1440).await else {
        return;
    };
    let (repo, branch) = (homes.repo.clone(), homes.branch.clone());

    // 1. The initial agent's sandbox becomes the home, and the agent works
    // in the home checkout there.
    let created = homes
        .ok(
            "workspace.create",
            json!({
                "repositoryPath": repo.to_string_lossy(),
                "baseRef": branch,
                "initialAgent": {
                    "model": "default", "provider": homes.keyed_ref, "placement": { "target": "e2b" },
                },
            }),
        )
        .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_owned();
    let home_id = created["workspace"]["home"]["homeId"]
        .as_str()
        .unwrap()
        .to_owned();
    let agent_id = created["initialAgent"]["id"]
        .as_str()
        .or_else(|| created["initialAgent"]["agentId"].as_str())
        .unwrap()
        .to_owned();
    let mut id = homes.next();
    let home = home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "the agent's sandbox became the home",
        |home| home["state"] == "attached" && home["nodeId"].is_string(),
    )
    .await;
    homes.id = id;
    let node_id = home["nodeId"].as_str().unwrap().to_owned();
    let sandbox = homes.sandbox_of(&agent_id);
    homes
        .ok(
            "agent.sendMessage",
            json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "hello" }),
        )
        .await;
    let sandbox_dir = homes.sandbox_dir(&sandbox);
    let pid_file = super::super::homes::until("the agent started in the home checkout", || {
        super::super::homes::home_checkout(&sandbox_dir, &home_id)
            .map(|work| work.join("agent.pid"))
            .filter(|pid| pid.is_file())
    })
    .await;
    let pids: Value = serde_json::from_str(&std::fs::read_to_string(&pid_file).unwrap()).unwrap();
    let agent_pid = pids["agent"].to_string();

    // 2. Deleting the agent stops it but keeps its sandbox: the home still
    // answers there.
    homes
        .ok(
            "agent.delete",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
    super::super::homes::until("the deleted agent's process ended", || {
        super::super::homes::process_gone(&agent_pid).then_some(())
    })
    .await;
    homes
        .ok(
            "file.write",
            json!({ "workspaceId": ws_id, "path": "after-agent.txt", "content": "still home\n" }),
        )
        .await;
    let read = homes
        .ok(
            "file.read",
            json!({ "workspaceId": ws_id, "path": "after-agent.txt" }),
        )
        .await;
    assert_eq!(read, "still home\n", "{read}");
    assert!(!homes.deleted(&sandbox), "the home's sandbox was deleted");
    let id = homes.next();
    assert!(
        cloud_of(&mut homes.rpc_ws, id, &sandbox).await.is_some(),
        "the home's machine row is gone"
    );

    // 3. Archiving the workspace deletes the sandbox and its row.
    homes
        .ok("workspace.archive", json!({ "workspaceId": ws_id }))
        .await;
    let id = homes.next();
    machine_until(&mut homes.rpc_ws, id, &sandbox, |machine| machine.is_none()).await;
    homes.id += 1000;
    assert!(homes.deleted(&sandbox), "the archived home's sandbox stays");
    let id = homes.next();
    let listed = ok(&mut homes.rpc_ws, id, "cloudMachines.list", json!({})).await;
    assert!(
        !listed.to_string().contains(&node_id),
        "the row stays: {listed}"
    );

    // 4. A member's workspace with a home-only sandbox.
    let mut member = homes.connect_as(MEMBER_TOKEN).await;
    let created = ok(
        &mut member,
        1,
        "workspace.create",
        json!({ "repositoryPath": repo.to_string_lossy(), "baseRef": branch }),
    )
    .await;
    let member_ws = created["workspace"]["id"].as_str().unwrap().to_owned();
    let write = json!({ "workspaceId": member_ws, "path": "m.txt", "content": "m\n" });
    let mut mid = 10;
    call_while_starting(&mut member, &mut mid, "file.write", &write).await;
    let home_only = homes.sandbox_of(&format!("home-{member_ws}"));
    let id = homes.next();
    let machine = cloud_of(&mut homes.rpc_ws, id, &home_only)
        .await
        .expect("the home-only machine");
    let machine_id = machine["id"].as_str().unwrap().to_owned();
    let guest_id = homes.guest_id.clone();
    mid += 1;
    ok(
        &mut member,
        mid,
        "workspace.members.add",
        json!({ "workspaceId": member_ws, "principalId": guest_id }),
    )
    .await;
    let mut guest = homes.connect_as(GUEST_TOKEN).await;
    for (gid, method) in [(1, "cloudMachines.pause"), (2, "cloudMachines.kill")] {
        let refused = rpc(&mut guest, gid, method, json!({ "machineId": machine_id })).await;
        assert_eq!(refused["error"]["code"], -32003, "{method}: {refused}");
    }
    assert_eq!(homes.pause_calls(&home_only), 0, "a collaborator paused it");
    assert!(!homes.deleted(&home_only), "a collaborator killed it");
    mid += 1;
    let paused = ok(
        &mut member,
        mid,
        "cloudMachines.pause",
        json!({ "machineId": machine_id }),
    )
    .await;
    assert_eq!(paused["machine"]["cloud"]["state"], "paused", "{paused}");
    mid += 1;
    ok(
        &mut member,
        mid,
        "cloudMachines.kill",
        json!({ "machineId": machine_id }),
    )
    .await;
    assert!(homes.deleted(&home_only), "the owner's kill left the sandbox");
}

/// A cloud home's idle pause (nodes.md "Cloud homes" 3-4, "Continuous
/// backup" 2), with one settings minute scaled to 3 s and an idle pause of
/// one minute: an open terminal keeps the home-only sandbox running past
/// several idle periods; once it closes, the sandbox pauses, and the
/// uncommitted file the terminal wrote is in `home/wip` by then (the home
/// pushed before the pause).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_terminal_keeps_a_cloud_home_awake_and_it_pushes_before_pausing() {
    let Some(mut homes) =
        CloudHomes::start("itd-e2b-idle-", &[("INTENTD_E2B_MINUTE_MS", "3000")], 1).await
    else {
        return;
    };
    let (repo, branch) = (homes.repo.clone(), homes.branch.clone());
    let created = homes
        .ok(
            "workspace.create",
            json!({ "repositoryPath": repo.to_string_lossy(), "baseRef": branch }),
        )
        .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_owned();
    // The terminal starts the home-only sandbox, and is open from then on.
    let mut id = homes.id;
    let (terminal, _) = call_while_starting(
        &mut homes.rpc_ws,
        &mut id,
        "terminal.create",
        &json!({ "workspaceId": ws_id, "cols": 80, "rows": 24, "command": "/bin/sh" }),
    )
    .await;
    homes.id = id;
    let terminal_id = terminal["terminalId"].as_str().unwrap().to_owned();
    let sandbox = homes.sandbox_of(&format!("home-{ws_id}"));
    let b64 = base64::engine::general_purpose::STANDARD;
    homes
        .ok(
            "terminal.write",
            json!({ "terminalId": terminal_id, "data": b64.encode("echo pending > wip.txt && echo wrote:$(cat wip.txt)\n") }),
        )
        .await;
    homes.terminal_until(&terminal_id, "wrote:pending").await;
    let wip_ref = format!("refs/intent/ws/{ws_id}/home/wip");
    let wip_files = |repo: &Path| {
        try_rev(repo, &wip_ref).map(|wip| git(repo, &["ls-tree", "--name-only", &wip]))
    };
    assert!(
        !wip_files(&repo).is_some_and(|files| files.contains("wip.txt")),
        "home/wip already holds the terminal's file"
    );

    // Four idle periods with the terminal open: no pause.
    // timing-guard: the open terminal must keep the sandbox running across several idle periods
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(homes.pause_calls(&sandbox), 0, "paused with a terminal open");
    let id = homes.next();
    let machine = cloud_of(&mut homes.rpc_ws, id, &sandbox)
        .await
        .expect("the home's machine");
    assert_eq!(machine["cloud"]["state"], "running", "{machine}");

    // Closed: the sandbox pauses once idle, after pushing home/wip.
    homes
        .ok("terminal.kill", json!({ "terminalId": terminal_id }))
        .await;
    let id = homes.next();
    machine_until(&mut homes.rpc_ws, id, &sandbox, |machine| {
        machine.is_some_and(|machine| machine["cloud"]["state"] == "paused")
    })
    .await;
    homes.id += 1000;
    let files = wip_files(&repo).expect("home/wip was pushed before the pause");
    assert!(files.contains("wip.txt"), "home/wip: {files}");
    let mut id = homes.next();
    let home = home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "the pause's push is the last backup",
        |home| home["lastBackupAt"].is_string(),
    )
    .await;
    homes.id = id;
    assert!(home.get("backupWarning").is_none_or(|w| w == false), "{home}");
}
