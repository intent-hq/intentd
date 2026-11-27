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
//! 5. A home whose sandbox was deleted while paused is rebuilt at its next
//!    use, under a new `homeId`, from its backup.
//! 6. A home moves from a machine to the entry (a new home-only sandbox),
//!    then to an agent's sandbox; a move whose sandbox fails to start
//!    rolls back.

use base64::Engine as _;

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

/// A host member's token: a person who neither owns nor started the
/// sandboxes here.
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
        use std::fmt::Write as _;
        Sha256::digest(token.as_bytes())
            .iter()
            .fold(String::with_capacity(64), |mut text, byte| {
                let _ = write!(text, "{byte:02x}");
                text
            })
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
/// list, against a fake E2B; a host member and a guest are known. Its
/// `Drop` stops the backend before any field drops, so the backend is gone
/// before the fake (which owns the scratch dir); no field can be moved out
/// (E0509).
struct CloudHomes {
    rpc_ws: common::TlsWs,
    head: common::DaemonGuard,
    port: u16,
    cfg: Arc<ClientConfig>,
    repo: PathBuf,
    branch: String,
    keyed_ref: String,
    guest_id: String,
    /// The E2B entry's `cloudMachineId`.
    entry: String,
    id: i64,
    fake: Arc<FakeE2b>,
}

impl Drop for CloudHomes {
    fn drop(&mut self) {
        let _ = self.head.kill_tree();
    }
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
        let configured = ok(
            &mut rpc_ws,
            3,
            "cloudMachines.configure",
            configure_params(json!({
                "apiKey": E2B_KEY, "size": "4x8", "idlePauseMinutes": idle_minutes,
                "pausedDeleteDays": 30,
            })),
        )
        .await;
        let entry = configured["provider"]["id"].as_str().unwrap().to_owned();
        Some(Self {
            rpc_ws,
            head,
            port,
            cfg,
            repo,
            branch,
            keyed_ref,
            guest_id,
            entry,
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
            .map_or_else(|| panic!("no sandbox for {owner}"), |(id, _)| id.clone())
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

/// The directory of agent `agent_id` on the machine whose data dir is
/// `node_dir`, while the machine keeps one.
fn agent_dir(node_dir: &Path, agent_id: &str) -> Option<PathBuf> {
    std::fs::read_dir(node_dir.join("w"))
        .ok()?
        .filter_map(Result::ok)
        .flat_map(|workspace| std::fs::read_dir(workspace.path()).into_iter().flatten())
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|dir| {
            std::fs::read_to_string(dir.join(".agent")).is_ok_and(|id| id.trim() == agent_id)
        })
}

/// A cloud home's sandbox is co-owned by the workspace (nodes.md "Cloud
/// homes" 1-2, "The home checkout" 1):
///
/// 1. The initial agent placed on the entry with no checkout works in the
///    home checkout on its sandbox, which became the home.
/// 2. Deleting that agent keeps the sandbox: the home still answers.
/// 3. Archiving the workspace deletes the sandbox and its machine row.
/// 4. A home-only sandbox: a collaborator of its workspace and another host
///    member can neither pause nor kill it; the workspace owner pauses it,
///    then kills it.
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
            json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "echo-raw" }),
        )
        .await;
    let id = homes.next();
    let raw = reply_containing(&mut homes.rpc_ws, (&ws_id, &agent_id), "raw: ", id).await;
    homes.id += 1000;
    assert!(
        raw.contains(&format!("/homes/{home_id}/work")),
        "the agent's session is not in the home checkout: {raw}"
    );
    let sandbox_dir = homes.sandbox_dir(&sandbox);
    assert!(agent_dir(&sandbox_dir, &agent_id).is_some());

    // 2. Deleting the agent stops it (its machine drops its directory) but
    // keeps its sandbox: the home still answers there.
    homes
        .ok(
            "agent.delete",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
    super::super::homes::until("the deleted agent's final stop", || {
        agent_dir(&sandbox_dir, &agent_id).is_none().then_some(())
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
    // timing-guard: a wrong delete is started in the background after the stop; give it time to land
    tokio::time::sleep(Duration::from_secs(2)).await;
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
    // The row goes before the E2B delete is sent.
    super::super::homes::until("E2B deletes the archived home's sandbox", || {
        homes.deleted(&sandbox).then_some(())
    })
    .await;
    let id = homes.next();
    let listed = ok(&mut homes.rpc_ws, id, "cloudMachines.list", json!({})).await;
    assert!(
        !listed.to_string().contains(&node_id),
        "the row stays: {listed}"
    );

    // 4. A home-only sandbox: a collaborator of its workspace and another
    // host member can neither pause nor kill it; the workspace owner can.
    let created = homes
        .ok(
            "workspace.create",
            json!({ "repositoryPath": repo.to_string_lossy(), "baseRef": branch }),
        )
        .await;
    let only_ws = created["workspace"]["id"].as_str().unwrap().to_owned();
    let owner = created["workspace"]["ownerPrincipalId"].clone();
    let write = json!({ "workspaceId": only_ws, "path": "o.txt", "content": "o\n" });
    let mut id = homes.id;
    call_while_starting(&mut homes.rpc_ws, &mut id, "file.write", &write).await;
    homes.id = id;
    let home_only = homes.sandbox_of(&format!("home-{only_ws}"));
    let id = homes.next();
    let machine = cloud_of(&mut homes.rpc_ws, id, &home_only)
        .await
        .expect("the home-only machine");
    assert_eq!(
        machine["cloud"]["startedBy"]["principalId"], owner,
        "{machine}"
    );
    let machine_id = machine["id"].as_str().unwrap().to_owned();
    let guest_id = homes.guest_id.clone();
    homes
        .ok(
            "workspace.members.add",
            json!({ "workspaceId": only_ws, "principalId": guest_id }),
        )
        .await;
    for token in [GUEST_TOKEN, MEMBER_TOKEN] {
        let mut other = homes.connect_as(token).await;
        for (oid, method) in [(1, "cloudMachines.pause"), (2, "cloudMachines.kill")] {
            let refused = rpc(&mut other, oid, method, json!({ "machineId": machine_id })).await;
            assert_eq!(refused["error"]["code"], -32003, "{method}: {refused}");
        }
    }
    assert_eq!(homes.pause_calls(&home_only), 0, "another person paused it");
    assert!(!homes.deleted(&home_only), "another person killed it");
    let paused = homes
        .ok("cloudMachines.pause", json!({ "machineId": machine_id }))
        .await;
    assert_eq!(paused["machine"]["cloud"]["state"], "paused", "{paused}");
    homes
        .ok("cloudMachines.kill", json!({ "machineId": machine_id }))
        .await;
    assert!(
        homes.deleted(&home_only),
        "the owner's kill left the sandbox"
    );
}

/// The gap the backend holds open between an idle pause's pushes and the
/// pause in the idle test.
const AFTER_PUSH_MS: u64 = 2000;

/// A cloud home's idle pause (nodes.md "Cloud homes" 3-4, "Continuous
/// backup" 2), with one settings minute scaled to 3 s and an idle pause of
/// one minute: an open terminal keeps the home-only sandbox running past
/// several idle periods; once it closes, the home pushes the uncommitted
/// file the terminal wrote to `home/wip`. An app edit while that pause runs
/// waits for it, then resumes the sandbox and lands; the next idle pause
/// pushes the edit too, then pauses. A saved script started while a later
/// pause runs waits for it too, then resumes the sandbox and runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_terminal_keeps_a_cloud_home_awake_and_it_pushes_before_pausing() {
    let Some(mut homes) = CloudHomes::start(
        "itd-e2b-idle-",
        &[
            ("INTENTD_E2B_MINUTE_MS", "3000"),
            ("INTENTD_E2B_AFTER_PUSH_MS", &AFTER_PUSH_MS.to_string()),
        ],
        1,
    )
    .await
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
    // The new home's first backup, so the file below is not in it.
    let mut id = homes.id;
    home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "the new home's first backup",
        |home| home["lastBackupAt"].is_string(),
    )
    .await;
    homes.id = id;
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
    assert_eq!(
        homes.pause_calls(&sandbox),
        0,
        "paused with a terminal open"
    );
    let id = homes.next();
    let machine = cloud_of(&mut homes.rpc_ws, id, &sandbox)
        .await
        .expect("the home's machine");
    assert_eq!(machine["cloud"]["state"], "running", "{machine}");

    // Closed: once idle, the homes push before the pause.
    let mut id = homes.id;
    let backed_up = home_of(&mut homes.rpc_ws, id + 1, &ws_id).await["lastBackupAt"].clone();
    id += 1;
    homes
        .ok("terminal.kill", json!({ "terminalId": terminal_id }))
        .await;
    let home = home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "the push before the idle pause",
        |home| home["lastBackupAt"].is_string() && home["lastBackupAt"] != backed_up,
    )
    .await;
    homes.id = id;
    let pushed = home["lastBackupAt"].clone();
    assert!(
        wip_files(&repo).is_some_and(|files| files.contains("wip.txt")),
        "home/wip lacks the terminal's file after the push"
    );

    // An app edit while the pause runs (the seam holds the gap between its
    // push and the pause open): it waits for the pause, then resumes the
    // sandbox and lands. Nothing slips through before the pause.
    homes
        .ok(
            "file.write",
            json!({ "workspaceId": ws_id, "path": "late.txt", "content": "late\n" }),
        )
        .await;
    assert_eq!(
        homes.pause_calls(&sandbox),
        1,
        "the edit did not wait for the pause"
    );
    let read = homes
        .ok(
            "file.read",
            json!({ "workspaceId": ws_id, "path": "late.txt" }),
        )
        .await;
    assert_eq!(read, "late\n", "{read}");

    // Idle again: it pushes once more, with the late edit, then pauses.
    let id = homes.next();
    machine_until(&mut homes.rpc_ws, id, &sandbox, |machine| {
        machine.is_some_and(|machine| machine["cloud"]["state"] == "paused")
    })
    .await;
    homes.id += 1000;
    assert_eq!(
        homes.pause_calls(&sandbox),
        2,
        "{:?}",
        homes.fake.state().calls
    );
    let files = wip_files(&repo).expect("home/wip was pushed before the pause");
    assert!(
        files.contains("wip.txt") && files.contains("late.txt"),
        "home/wip: {files}"
    );
    let mut id = homes.next();
    let home = home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "the pause's push is the last backup",
        |home| home["lastBackupAt"].is_string() && home["lastBackupAt"] != pushed,
    )
    .await;
    homes.id = id;
    assert!(
        home.get("backupWarning").is_none_or(|w| w == false),
        "{home}"
    );

    // A saved script started while the next pause runs waits for it too,
    // then resumes the sandbox and runs.
    let script = homes
        .ok(
            "script.create",
            json!({
                "workspaceId": ws_id, "name": "late", "command": "echo ran:$(cat late.txt)",
                "mode": "command", "purpose": "saved",
            }),
        )
        .await;
    let script_id = script["id"].as_str().unwrap().to_owned();
    homes
        .ok(
            "file.read",
            json!({ "workspaceId": ws_id, "path": "late.txt" }),
        )
        .await;
    let pushed = home["lastBackupAt"].clone();
    let mut id = homes.id;
    home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "the third pause's push",
        |home| home["lastBackupAt"].is_string() && home["lastBackupAt"] != pushed,
    )
    .await;
    homes.id = id;
    let ran = homes
        .ok(
            "script.run",
            json!({ "workspaceId": ws_id, "scriptId": script_id, "timeoutSeconds": 60 }),
        )
        .await;
    assert!(
        ran["output"]
            .as_str()
            .unwrap_or_default()
            .contains("ran:late"),
        "{ran}"
    );
    assert_eq!(
        homes.pause_calls(&sandbox),
        3,
        "the start did not wait for the pause"
    );
}

/// The titles of workspace `ws_id`'s notes.
async fn note_titles(homes: &mut CloudHomes, ws_id: &str) -> Vec<String> {
    let notes = homes.ok("note.list", json!({ "workspaceId": ws_id })).await;
    notes["notes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|note| note["title"].as_str().map(str::to_owned))
        .collect()
}

/// The `home.state` values announced for workspace `ws_id` in `seen`.
fn announced_states(seen: &[String], ws_id: &str) -> Vec<String> {
    seen.iter()
        .filter_map(|text| serde_json::from_str::<Value>(text).ok())
        .map(|frame| frame["params"]["event"].clone())
        .filter(|event| event["data"]["workspaceId"] == ws_id)
        .filter_map(|event| {
            event["data"]["changes"]["home"]["state"]
                .as_str()
                .map(str::to_owned)
        })
        .collect()
}

/// A cloud home whose sandbox was deleted while paused is rebuilt at its
/// next use (nodes.md "Cloud homes" 5-6), with one settings minute scaled
/// to 3 s, one day to 300 ms (so `pausedDeleteDays` 30 is 9 s) and an idle
/// pause of one minute:
///
/// 1. A workspace whose initial agent's sandbox is the home: an app edit is
///    pushed by the idle pause; the paused sandbox is then deleted for
///    staying paused. The home gets a new `homeId` with no sandbox, and is
///    never detached. The next file call starts a new sandbox from the same
///    entry, whose home has the edit; the workspace gets a notice.
/// 2. The agent that shared the old home starts in a checkout of its own,
///    with the moved-home notice.
/// 3. A home-only sandbox E2B removes while paused: a call that finds it
///    gone is told the home is starting; its rebuild fails, which detaches
///    the home at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cloud_home_deleted_while_paused_is_rebuilt_from_its_backup() {
    let Some(mut homes) = CloudHomes::start(
        "itd-e2b-rebuild-",
        &[
            ("INTENTD_E2B_MINUTE_MS", "3000"),
            ("INTENTD_E2B_DAY_MS", "300"),
        ],
        1,
    )
    .await
    else {
        return;
    };
    let (repo, branch) = (homes.repo.clone(), homes.branch.clone());
    let mut events = connect(homes.port, homes.cfg.clone()).await;
    ok(
        &mut events,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"] }),
    )
    .await;
    let mut seen = Vec::new();

    // 1. The initial agent's sandbox is the home; an app edit lands there.
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
    let old_home = created["workspace"]["home"]["homeId"]
        .as_str()
        .unwrap()
        .to_owned();
    let agent_id = created["initialAgent"]["id"]
        .as_str()
        .or_else(|| created["initialAgent"]["agentId"].as_str())
        .unwrap()
        .to_owned();
    let mut id = homes.id;
    home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "the agent's sandbox became the home",
        |home| home["state"] == "attached" && home["nodeId"].is_string(),
    )
    .await;
    homes.id = id;
    let old_sandbox = homes.sandbox_of(&agent_id);
    // The agent runs a turn in the home checkout there.
    homes
        .ok(
            "agent.sendMessage",
            json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "echo-raw" }),
        )
        .await;
    let id = homes.next();
    let raw = reply_containing(&mut homes.rpc_ws, (&ws_id, &agent_id), "raw: ", id).await;
    homes.id += 1000;
    assert!(
        raw.contains(&format!("/homes/{old_home}/work")),
        "the agent's session is not in the home checkout: {raw}"
    );
    homes
        .ok(
            "file.write",
            json!({ "workspaceId": ws_id, "path": "saved.txt", "content": "saved before the pause\n" }),
        )
        .await;

    // 3 (set up). A home-only workspace, started by a file call.
    let created = homes
        .ok(
            "workspace.create",
            json!({ "repositoryPath": repo.to_string_lossy(), "baseRef": branch }),
        )
        .await;
    let lost_ws = created["workspace"]["id"].as_str().unwrap().to_owned();
    let lost_home = created["workspace"]["home"]["homeId"]
        .as_str()
        .unwrap()
        .to_owned();
    let write = json!({ "workspaceId": lost_ws, "path": "l.txt", "content": "l\n" });
    let mut id = homes.id;
    call_while_starting(&mut homes.rpc_ws, &mut id, "file.write", &write).await;
    homes.id = id;
    let lost_sandbox = homes.sandbox_of(&format!("home-{lost_ws}"));

    // 3. Once paused, E2B removes it. A call finds it gone and is told the
    // home starts; the rebuild fails (the mint does), and the home is
    // detached at once.
    let id = homes.next();
    machine_until(&mut homes.rpc_ws, id, &lost_sandbox, |machine| {
        machine.is_some_and(|machine| machine["cloud"]["state"] == "paused")
    })
    .await;
    homes.id += 1000;
    let template = homes.fake.state().sandboxes[&lost_sandbox].template.clone();
    homes.fake.vanish(&lost_sandbox);
    homes.fake.state().failed_mint_templates.push(template);
    let deadline = tokio::time::Instant::now() + wait();
    let reply = loop {
        let id = homes.next();
        let reply = rpc(
            &mut homes.rpc_ws,
            id,
            "file.read",
            json!({ "workspaceId": lost_ws, "path": "l.txt" }),
        )
        .await;
        assert_eq!(
            reply["error"]["data"]["code"], "home-unavailable",
            "{reply}"
        );
        if reply["error"]["data"]["starting"] != true {
            break reply;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the rebuild never ended: {reply}"
        );
    };
    let id = homes.next();
    let home = home_of(&mut homes.rpc_ws, id, &lost_ws).await;
    assert_eq!(home["state"], "detached", "{home} after {reply}");
    assert_ne!(home["homeId"], lost_home.as_str(), "{home}");
    assert!(home.get("nodeId").is_none(), "{home}");
    homes.fake.state().failed_mint_templates.clear();

    // 1. The agent's sandbox: the idle pause pushes the edit, and once it
    // stays paused past pausedDeleteDays it is deleted.
    let wip_ref = format!("refs/intent/ws/{ws_id}/home/wip");
    super::super::homes::until("the idle pause pushed the edit", || {
        try_rev(&repo, &wip_ref)
            .map(|wip| git(&repo, &["ls-tree", "--name-only", &wip]))
            .filter(|files| files.contains("saved.txt"))
    })
    .await;
    let id = homes.next();
    machine_until(&mut homes.rpc_ws, id, &old_sandbox, |machine| {
        machine.is_none()
    })
    .await;
    homes.id += 1000;
    super::super::homes::until("E2B deletes the paused sandbox", || {
        homes.deleted(&old_sandbox).then_some(())
    })
    .await;
    let mut id = homes.id;
    let home = home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "a new home with no sandbox",
        |home| home["homeId"] != old_home.as_str(),
    )
    .await;
    homes.id = id;
    assert_eq!(home["state"], "attached", "{home}");
    assert!(home.get("nodeId").is_none(), "{home}");
    let new_home = home["homeId"].as_str().unwrap().to_owned();
    let made = homes.fake.state().creates.len();

    // The next use starts a new sandbox of the same entry, from the backup.
    let read = json!({ "workspaceId": ws_id, "path": "saved.txt" });
    let mut id = homes.id;
    let (content, _) = call_while_starting(&mut homes.rpc_ws, &mut id, "file.read", &read).await;
    homes.id = id;
    assert_eq!(content, "saved before the pause\n", "{content}");
    assert_eq!(homes.fake.state().creates.len(), made + 1);
    let rebuilt = homes.sandbox_of(&format!("home-{ws_id}"));
    let mut id = homes.id;
    let home = home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "the rebuilt home attached",
        |home| home["state"] == "attached" && home["nodeId"].is_string(),
    )
    .await;
    homes.id = id;
    assert_eq!(home["homeId"], new_home.as_str(), "{home}");
    let id = homes.next();
    let machine = machine_of(&mut homes.rpc_ws, id, home["nodeId"].as_str().unwrap()).await;
    assert_eq!(machine["cloud"]["sandboxId"], rebuilt.as_str(), "{machine}");
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        let titles = note_titles(&mut homes, &ws_id).await;
        if titles.iter().any(|title| title == "The home was rebuilt") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no notice: {titles:?}"
        );
        // timing-guard: poll until the rebuilt home's notice is written
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    drain_events(&mut events, &mut seen).await;
    let states = announced_states(&seen, &ws_id);
    assert!(
        !states.iter().any(|state| state == "detached"),
        "the rebuilt home was detached: {states:?}"
    );
    assert!(
        states.iter().any(|state| state == "starting"),
        "the rebuild was not announced as starting: {states:?}"
    );

    // 2. The agent that shared the old home starts in its own checkout.
    let made = homes.fake.state().creates.len();
    homes
        .ok(
            "agent.sendMessage",
            json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "echo-raw" }),
        )
        .await;
    let id = homes.next();
    let raw = reply_containing(
        &mut homes.rpc_ws,
        (&ws_id, &agent_id),
        "The workspace's home moved.",
        id,
    )
    .await;
    homes.id += 1000;
    assert!(
        raw.contains("The workspace's home moved."),
        "the turn carries the moved-home notice: {raw}"
    );
    assert!(!raw.contains("/homes/"), "still in a home checkout: {raw}");
    assert_eq!(
        homes.fake.state().creates.len(),
        made + 1,
        "the agent starts on a sandbox of its own"
    );
}

/// The sandbox E2B made for `owner`, once it has.
async fn sandbox_until(homes: &CloudHomes, owner: &str) -> String {
    super::super::homes::until(&format!("a sandbox for {owner}"), || {
        homes
            .fake
            .state()
            .metadata
            .iter()
            .find(|(_, metadata)| metadata["agentId"] == owner)
            .map(|(id, _)| id.clone())
    })
    .await
}

/// Whether `states` holds `wanted` in that order (not necessarily next to
/// each other).
fn in_order(states: &[String], wanted: &[&str]) -> bool {
    let mut next = wanted.iter().peekable();
    for state in states {
        if next.peek().is_some_and(|want| *want == state) {
            next.next();
        }
    }
    next.peek().is_none()
}

/// Moving a home to a cloud entry and to an agent's sandbox (nodes.md
/// "Moving the home", "Cloud homes"), on an orchestrator with machine A
/// first on its list and an E2B entry:
///
/// 1. A home on machine A, with a commit and uncommitted work, moves to the
///    entry: the move is announced `moving`, then `starting` while a
///    home-only sandbox starts, then `attached` on that sandbox, whose home
///    checkout has the commit and the uncommitted file. A gets the `moved`
///    stop: its last push lands on orphaned refs and its checkout goes.
///    The sandbox's `startedBy` is the workspace owner.
/// 2. The home moves to the sandbox of an agent placed on the entry; the
///    home-only sandbox, which holds nothing any more, is deleted.
/// 3. A move to the entry whose sandbox fails to start rolls back: the home
///    is the agent's sandbox again, with its old state and `since`, and
///    still answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_home_moves_to_a_cloud_entry_and_to_an_agents_sandbox() {
    let Some(mut homes) = CloudHomes::start("itd-e2b-move-", &[], 1440).await else {
        return;
    };
    let (repo, branch) = (homes.repo.clone(), homes.branch.clone());
    let mut events = connect(homes.port, homes.cfg.clone()).await;
    ok(
        &mut events,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"] }),
    )
    .await;
    let mut seen = Vec::new();
    let entry = homes.entry.clone();

    // Machine A, first on the list: new workspaces get their home there.
    let backend = homes.ok("remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let a_dir = homes.fake.scratch().join("node-a");
    std::fs::create_dir_all(&a_dir).unwrap();
    let a = start_machine_with(&a_dir, &backend_id, &[]).await;
    homes
        .ok("remoteMachines.add", add_params(&a, "Machine A"))
        .await;
    let order = homes.ok("remoteMachines.order", json!({})).await;
    let mut rows: Vec<Value> = order["order"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| match row["kind"].as_str() {
            Some("machine") => json!({ "kind": "machine", "nodeId": row["nodeId"] }),
            Some("cloud") => json!({ "kind": "cloud", "cloudMachineId": row["cloudMachineId"] }),
            _ => json!({ "kind": "local" }),
        })
        .collect();
    rows.sort_by_key(|row| row["nodeId"] != a.daemon_id.as_str());
    homes
        .ok("remoteMachines.setOrder", json!({ "order": rows }))
        .await;
    let deadline = tokio::time::Instant::now() + wait();
    let created = loop {
        let id = homes.next();
        let response = rpc(
            &mut homes.rpc_ws,
            id,
            "workspace.create",
            json!({ "repositoryPath": repo.to_string_lossy(), "baseRef": branch }),
        )
        .await;
        let workspace = &response["result"]["workspace"];
        if workspace["home"]["nodeId"] == a.daemon_id.as_str() {
            break workspace.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "machine A never became a home: {response}"
        );
        // timing-guard: retry while the new machine finishes its first bind
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let ws_id = created["id"].as_str().unwrap().to_owned();
    let owner = created["ownerPrincipalId"].as_str().unwrap().to_owned();
    let a_home = created["home"]["homeId"].as_str().unwrap().to_owned();
    let mirror = PathBuf::from(created["worktreePath"].as_str().unwrap());
    let base = git(&mirror, &["rev-parse", "HEAD"]);
    let work = super::super::homes::until("the home checkout on A", || {
        super::super::homes::home_checkout(&a_dir, &a_home)
            .filter(|work| try_rev(work, "HEAD").as_deref() == Some(base.as_str()))
    })
    .await;
    let kept = super::super::homes::commit_on(&work, "kept.txt", "kept\n", "kept");
    std::fs::write(work.join("dirty.txt"), "left uncommitted\n").unwrap();

    // 1. To the entry: a new home-only sandbox.
    let made = homes.fake.state().creates.len();
    let started = homes
        .ok(
            "workspace.setHome",
            json!({ "workspaceId": ws_id, "to": { "kind": "cloud", "cloudMachineId": entry } }),
        )
        .await;
    assert_eq!(started["home"]["state"], "moving", "{started}");
    assert_eq!(started["home"]["homeId"], a_home.as_str(), "{started}");
    let mut id = homes.id;
    let home = home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "the home is on the entry's sandbox",
        |home| {
            home["state"] == "attached"
                && home["cloudMachineId"] == entry.as_str()
                && home["nodeId"].is_string()
        },
    )
    .await;
    homes.id = id;
    let cloud_home = home["homeId"].as_str().unwrap().to_owned();
    assert_ne!(cloud_home, a_home);
    let cloud_node = home["nodeId"].as_str().unwrap().to_owned();
    assert_eq!(homes.fake.state().creates.len(), made + 1, "one sandbox");
    let home_sandbox = homes.sandbox_of(&format!("home-{ws_id}"));
    let id = homes.next();
    let machine = machine_of(&mut homes.rpc_ws, id, &cloud_node).await;
    assert_eq!(
        machine["cloud"]["sandboxId"],
        home_sandbox.as_str(),
        "{machine}"
    );
    assert_eq!(
        machine["cloud"]["startedBy"]["principalId"],
        owner.as_str(),
        "{machine}"
    );
    let sandbox_dir = homes.sandbox_dir(&home_sandbox);
    let moved = super::super::homes::until("the home checkout on the sandbox", || {
        super::super::homes::home_checkout(&sandbox_dir, &cloud_home)
            .filter(|work| try_rev(work, "HEAD").as_deref() == Some(kept.as_str()))
    })
    .await;
    assert_eq!(
        std::fs::read_to_string(moved.join("dirty.txt")).unwrap(),
        "left uncommitted\n"
    );
    let read = homes
        .ok(
            "file.read",
            json!({ "workspaceId": ws_id, "path": "dirty.txt" }),
        )
        .await;
    assert_eq!(read, "left uncommitted\n", "{read}");
    drain_events(&mut events, &mut seen).await;
    let states = announced_states(&seen, &ws_id);
    assert!(
        in_order(&states, &["moving", "starting", "attached"]),
        "home states announced: {states:?}"
    );
    // A got its moved stop: its last push lands on orphaned refs, then its
    // checkout goes.
    super::super::homes::until("the old home checkout on A is removed", || {
        super::super::homes::home_checkout(&a_dir, &a_home)
            .is_none()
            .then_some(())
    })
    .await;
    let orphaned = git(
        &mirror,
        &[
            "for-each-ref",
            "--format=%(refname)",
            &format!("refs/intent/ws/{ws_id}/home/orphaned/"),
        ],
    );
    let orphaned_head = orphaned
        .lines()
        .find(|name| name.ends_with("/head"))
        .unwrap_or_else(|| panic!("no orphaned head: {orphaned:?}"));
    assert_eq!(git(&mirror, &["rev-parse", orphaned_head]), kept);

    // 2. To an agent's sandbox on the entry.
    let agent = homes
        .ok(
            "agent.create",
            json!({
                "workspaceId": ws_id,
                "name": "Cloud agent",
                "model": "default",
                "provider": homes.keyed_ref,
                "placement": { "target": "e2b", "cloudMachineId": entry, "checkout": "isolated" },
            }),
        )
        .await;
    let agent_id = agent["agent"]["id"].as_str().unwrap().to_owned();
    let agent_sandbox = sandbox_until(&homes, &agent_id).await;
    let id = homes.next();
    let agent_machine = machine_until(&mut homes.rpc_ws, id, &agent_sandbox, |machine| {
        machine.is_some_and(|machine| machine["id"].is_string())
    })
    .await
    .unwrap();
    homes.id += 1000;
    let agent_node = agent_machine["id"].as_str().unwrap().to_owned();
    // The agent's placement links once its sandbox runs; the move needs it.
    let deadline = tokio::time::Instant::now() + wait();
    let started = loop {
        let id = homes.next();
        let response = rpc(
            &mut homes.rpc_ws,
            id,
            "workspace.setHome",
            json!({ "workspaceId": ws_id, "to": { "agentId": agent_id } }),
        )
        .await;
        if response.get("error").is_none() {
            break response["result"].clone();
        }
        assert_eq!(
            response["error"]["data"]["code"], "home-target-unavailable",
            "{response}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the agent's sandbox never became a home target: {response}"
        );
        // timing-guard: retry while the agent's sandbox links
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(started["home"]["state"], "moving", "{started}");
    let mut id = homes.id;
    let home = home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "the home is on the agent's sandbox",
        |home| home["state"] == "attached" && home["nodeId"] == agent_node.as_str(),
    )
    .await;
    homes.id = id;
    assert_eq!(home["cloudMachineId"], entry.as_str(), "{home}");
    let agent_home = home["homeId"].as_str().unwrap().to_owned();
    let agent_dir = homes.sandbox_dir(&agent_sandbox);
    super::super::homes::until("the home checkout on the agent's sandbox", || {
        super::super::homes::home_checkout(&agent_dir, &agent_home)
            .filter(|work| work.join("dirty.txt").exists())
    })
    .await;
    let read = homes
        .ok(
            "file.read",
            json!({ "workspaceId": ws_id, "path": "kept.txt" }),
        )
        .await;
    assert_eq!(read, "kept\n", "{read}");
    // The home-only sandbox held only the old home: it goes.
    super::super::homes::until("the home-only sandbox is deleted", || {
        homes.deleted(&home_sandbox).then_some(())
    })
    .await;

    // 3. A move to the entry whose sandbox fails to start rolls back.
    let before = home.clone();
    let template = homes.fake.state().sandboxes[&agent_sandbox]
        .template
        .clone();
    homes.fake.state().failed_mint_templates.push(template);
    let made = homes.fake.state().creates.len();
    let started = homes
        .ok(
            "workspace.setHome",
            json!({ "workspaceId": ws_id, "to": { "kind": "cloud", "cloudMachineId": entry } }),
        )
        .await;
    assert_eq!(started["home"]["state"], "moving", "{started}");
    let mut id = homes.id;
    let home = home_until(
        &mut homes.rpc_ws,
        &mut id,
        &ws_id,
        "the failed move rolls back",
        |home| home["state"] != "moving" && home["state"] != "starting",
    )
    .await;
    homes.id = id;
    assert_eq!(home["homeId"], before["homeId"], "{home}");
    assert_eq!(home["nodeId"], before["nodeId"], "{home}");
    assert_eq!(home["state"], "attached", "{home}");
    assert_eq!(home["since"], before["since"], "{home}");
    assert_eq!(
        homes.fake.state().creates.len(),
        made + 1,
        "one start tried"
    );
    homes.fake.state().failed_mint_templates.clear();
    let failed_sandbox = homes
        .fake
        .state()
        .metadata
        .iter()
        .filter(|(_, metadata)| metadata["agentId"] == format!("home-{ws_id}"))
        .map(|(id, _)| id.clone())
        .find(|id| *id != home_sandbox)
        .expect("the failed start's sandbox");
    super::super::homes::until("the failed start's sandbox is deleted", || {
        homes.deleted(&failed_sandbox).then_some(())
    })
    .await;
    let read = homes
        .ok(
            "file.read",
            json!({ "workspaceId": ws_id, "path": "kept.txt" }),
        )
        .await;
    assert_eq!(read, "kept\n", "{read}");
    drop(a);
}
