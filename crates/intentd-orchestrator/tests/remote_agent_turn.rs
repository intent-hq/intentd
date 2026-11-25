//! An agent of a Postgres tenant runs its turn on a machine: serving a
//! tenant builds and attaches its agent manager, so a client's
//! `agent.sendMessage` reaches the machine as a Launch and a Prompt, and the
//! machine's journal completes the turn into head's transcript. Without the
//! manager the message was stored and nothing ever reached the machine
//! (`system.status` reported `maxAgents: 0`). The tenant's agent limits
//! apply to these agents as `intentd`'s do: an idle one is reaped, and the
//! cap counts only agents on the orchestrator host.
//!
//! The machine is served in process: its WSS listener with the executor
//! link installed and a scripted executor host that hands each admitted
//! link to the test, which answers as a machine's executor would.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(all(feature = "postgres", unix))]

mod common;

use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::machines::ExecutorCredential;
use intent_core::node_link::{
    JournalBody, JournalRecord, LaunchCommand, LinkFrame, LinkHello, LinkWelcome,
    MachineCapabilities, ProviderCapability, ProviderStatus, StopOutcome,
};
use intent_core::nodes::{NodeArch, NodeCounter, NodeOs};
use intent_core::placement::AgentPlatform;
use intent_core::{AgentId, ExecutorCredentialId, WorkspaceId};
use intent_services::{EventBus, Services};
use intent_store::postgres::{self, PgPool};
use intent_store::{OrchestratorOnly, Store};
use intent_transport::node_link::{
    ExecutorHost, ExecutorLinks, GitConnection, LinkEvent, LinkEvents, NodeConnection, NodeLink,
};
use intent_transport::{
    generate_certificate, hash_token, AsyncTokenStore, TlsCertificate, TokenStore, WsApiServer,
    WsOptions,
};
use intentd_orchestrator::listen::ListenConfig;
use intentd_orchestrator::serve::{serve_tenant, LeaseTiming, ServedTenant};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use common::{result, until, wait, Uds};

async fn pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("INTENT_STORE_TEST_PG_URL") else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_PG").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_PG=1 but INTENT_STORE_TEST_PG_URL is unset"
        );
        eprintln!("skipping: INTENT_STORE_TEST_PG_URL is unset");
        return None;
    };
    let pool = postgres::connect(&url, 8).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    Some(pool)
}

fn keyring() -> Arc<postgres::SecretKeyring> {
    Arc::new(postgres::SecretKeyring::new("test", &[0x5a; 32]).unwrap())
}

#[track_caller]
fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// A repository with one commit.
fn repository(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    git(dir, &["init", "-q", "-b", "main"]);
    std::fs::write(dir.join("README.md"), "base\n").unwrap();
    git(dir, &["add", "README.md"]);
    git(
        dir,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-q",
            "-m",
            "base",
        ],
    );
}

/// What the machine runs: Linux with two agent slots and the mock provider
/// signed in.
fn capabilities() -> MachineCapabilities {
    let mut capabilities = MachineCapabilities::new(
        AgentPlatform {
            os: NodeOs::Linux,
            arch: NodeArch::X86_64,
        },
        "machine-test",
        2,
        intent_core::now_iso(),
    );
    capabilities.providers.push(ProviderCapability {
        id: "mock".into(),
        visible: true,
        status: ProviderStatus::SignedIn,
        models: vec!["mock-model".into()],
        models_stale: false,
    });
    capabilities
}

struct OwnerToken;

impl TokenStore for OwnerToken {
    fn load_token(&self) -> Option<String> {
        Some("o".repeat(64))
    }
    fn store_token(&self, _token: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

/// The machine's executor service, scripted: a fixed welcome, and every
/// admitted connection handed to the test.
struct Host {
    node_id: String,
    accepted: mpsc::UnboundedSender<NodeConnection>,
}

impl ExecutorHost for Host {
    fn ready(&self) -> bool {
        true
    }
    fn node_id(&self) -> String {
        self.node_id.clone()
    }
    fn welcome(&self, _credential: &ExecutorCredential, _hello: &LinkHello) -> LinkWelcome {
        LinkWelcome {
            protocol: 0,
            node_id: String::new(),
            running_agents: Vec::new(),
            capabilities: capabilities(),
            hosts: vec!["127.0.0.1".into()],
            tc_address: None,
        }
    }
    fn accept(&self, _credential: ExecutorCredential, connection: NodeConnection) {
        let _ = self.accepted.send(connection);
    }
}

/// A machine serving the executor link in process, with a credential
/// minted for one backend.
struct Machine {
    server: WsApiServer,
    port: u16,
    certificate: TlsCertificate,
    node_id: String,
    credential: ExecutorCredential,
    token: String,
    accepted: mpsc::UnboundedReceiver<NodeConnection>,
    /// Head's git connections, kept open (this machine serves no git).
    git: Arc<Mutex<Vec<GitConnection>>>,
    _dir: tempfile::TempDir,
}

impl Machine {
    async fn start(backend_id: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("machine.sqlite"))
            .await
            .unwrap();
        let token = "e".repeat(64);
        let credential = ExecutorCredential {
            id: ExecutorCredentialId::new(),
            backend_id: backend_id.into(),
            backend_name: "Orchestrator".into(),
            backend_fingerprint: None,
            created_at: intent_core::now_iso(),
            last_used_at: None,
            revoked_at: None,
        };
        store
            .insert_executor_credential(&credential, &hash_token(&token))
            .await
            .unwrap();
        let node_id = store.daemon_id().await.unwrap();
        let certificate =
            generate_certificate(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        let (tx, accepted) = mpsc::unbounded_channel();
        let host = Arc::new(Host {
            node_id: node_id.clone(),
            accepted: tx,
        });
        let tokens = Arc::new(AsyncTokenStore::new(Arc::new(OwnerToken)));
        let mut server = WsApiServer::new(
            Arc::new(Services::new(store.clone())),
            EventBus::new(store),
            &certificate,
            &tokens,
            WsOptions {
                bind_addresses: vec![std::net::IpAddr::from([127, 0, 0, 1])],
                base_port: 0,
                ..WsOptions::default()
            },
            None,
        )
        .unwrap();
        server.install_executor_links(ExecutorLinks::new(host));
        let port = server.start_with_port_assignment(|_| Ok(())).await.unwrap();
        Self {
            server,
            port,
            certificate,
            node_id,
            credential,
            token,
            accepted,
            git: Arc::default(),
            _dir: dir,
        }
    }

    /// The `remoteMachines.add` params the frontend relays.
    fn add_params(&self) -> Value {
        json!({
            "machineId": self.node_id,
            "name": "Test machine",
            "hosts": ["127.0.0.1"],
            "port": self.port,
            "fingerprint": self.certificate.fingerprint256,
            "credentialId": self.credential.id,
            "token": self.token,
        })
    }

    /// The link head launches the agent on, and the launch: links that
    /// close without one (`remoteMachines.add`'s probe, which a send may
    /// still reach just before it closes) are skipped. The machine has
    /// nothing to replay.
    async fn launched(&mut self) -> (MachineEnd, LaunchCommand) {
        loop {
            let mut end = self.accept().await;
            let _ = end
                .link
                .send(&LinkFrame::ReplayComplete {
                    through_seq: NodeCounter(0),
                })
                .await;
            let launch = end
                .next(wait(), |frame| match frame {
                    LinkFrame::Launch(command) => Some(command),
                    _ => None,
                })
                .await;
            if let Some(launch) = launch {
                return (end, launch);
            }
        }
    }

    /// The next link head binds; its git connections are kept aside.
    async fn accept(&mut self) -> MachineEnd {
        loop {
            match tokio::time::timeout(wait(), self.accepted.recv()).await {
                Ok(Some(NodeConnection::Link { link, events, .. })) => {
                    return MachineEnd { link, events };
                }
                Ok(Some(NodeConnection::Git(git))) => self.git.lock().unwrap().push(git),
                other => panic!("expected a link, got {other:?}"),
            }
        }
    }
}

/// The machine's end of a bound link.
struct MachineEnd {
    link: NodeLink,
    events: LinkEvents,
}

impl MachineEnd {
    async fn send(&self, frame: LinkFrame) {
        self.link.send(&frame).await.unwrap();
    }

    async fn journal(
        &self,
        seq: u64,
        agent: &AgentId,
        workspace: &WorkspaceId,
        turn: Option<&str>,
        body: JournalBody,
    ) {
        self.send(LinkFrame::Journal(JournalRecord {
            seq: NodeCounter(seq),
            owner: agent.clone().into(),
            workspace_id: workspace.clone(),
            turn_id: turn.map(str::to_owned),
            recorded_at: intent_core::now_iso(),
            body,
        }))
        .await;
    }

    /// The next frame `pick` accepts, answering capability requests and
    /// skipping everything else. `None` when none arrives within `within`
    /// or the link closes first.
    async fn next<T>(
        &mut self,
        within: Duration,
        mut pick: impl FnMut(LinkFrame) -> Option<T>,
    ) -> Option<T> {
        tokio::time::timeout(within, async {
            loop {
                match self.events.recv().await {
                    Some(LinkEvent::Frame(LinkFrame::Capabilities)) => {
                        // A closing link is seen on the next receive.
                        let _ = self
                            .link
                            .send(&LinkFrame::CapabilitiesReply {
                                capabilities: capabilities(),
                            })
                            .await;
                    }
                    Some(LinkEvent::Frame(frame)) => {
                        if let Some(found) = pick(frame) {
                            return Some(found);
                        }
                    }
                    Some(LinkEvent::Request(_)) => {}
                    None => return None,
                }
            }
        })
        .await
        .ok()
        .flatten()
    }
}

/// A tenant agent's turn running on the tenant's machine: the machine got
/// the prompt and has not ended the turn.
struct RunningTurn {
    served: intentd_orchestrator::serve::ServedTenant,
    listeners: intentd_orchestrator::listen::ClientListeners,
    client: Uds,
    machine: Machine,
    end: MachineEnd,
    agent: AgentId,
    workspace: WorkspaceId,
    turn: String,
    _dir: tempfile::TempDir,
}

/// Write `config` as `tenant`'s first settings document, before any
/// process claims the tenant.
async fn seed_tenant_settings(pool: &PgPool, tenant: &str, config: &str) {
    use intent_services::{SettingsDocumentStore as _, SettingsWrite};
    let store = Store::open_postgres(
        pool.clone(),
        postgres::TenantId::new(tenant).unwrap(),
        postgres::PostgresOptions::default(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .unwrap();
    let settings = intentd_orchestrator::settings::TenantSettings::new(&store).unwrap();
    let written = settings.compare_and_set(0, config).unwrap();
    assert!(
        matches!(written, SettingsWrite::Written(_)),
        "the tenant's settings row is seeded: {written:?}"
    );
}

impl RunningTurn {
    /// Serve `tenant`, add its machine, create an agent on it over the
    /// tenant's local socket and message it, until the machine has the
    /// prompt. `None` when the test skips.
    async fn start(pool: PgPool, tenant: &str) -> Option<Self> {
        Self::start_with(pool, tenant, "").await
    }

    /// [`Self::start`], with `config` as the tenant's settings document:
    /// its Postgres `tenant_config` row, which serving reads (never a
    /// `config.toml`). An empty `config` writes no row.
    async fn start_with(pool: PgPool, tenant: &str, config: &str) -> Option<Self> {
        if intent_providers_missing_git() {
            return None;
        }
        let dir = tempfile::tempdir().unwrap();
        let tenant_dir = intentd_orchestrator::tenant_dir(dir.path(), tenant);
        std::fs::create_dir_all(&tenant_dir).unwrap();
        if !config.is_empty() {
            seed_tenant_settings(&pool, tenant, config).await;
        }
        let served = serve_tenant(
            pool,
            tenant,
            dir.path(),
            &format!("orchestrator-test-{tenant}"),
            LeaseTiming::default(),
            keyring(),
            OrchestratorOnly::for_tests(),
        )
        .await
        .expect("serve the tenant");
        let services = served.services().clone();
        assert!(
            services.agent_manager().is_some(),
            "serving the tenant attaches its agent manager"
        );
        let listeners = served
            .listen(&ListenConfig {
                socket_path: dir.path().join("o.sock"),
                bind_addresses: vec![std::net::IpAddr::from([127, 0, 0, 1])],
                ws_port: 0,
                tls_dir: dir.path().join("tls"),
                insecure: true,
            })
            .await
            .unwrap();
        let mut client = Uds::connect(listeners.socket_path()).await;
        result(
            &client
                .rpc(
                    1,
                    "client.hello",
                    json!({ "clientId": "agents-test", "name": "Agents test" }),
                )
                .await,
        );
        let status = client.rpc(2, "system.status", json!({})).await;
        assert!(
            result(&status)["maxAgents"]
                .as_u64()
                .is_some_and(|cap| cap > 0),
            "the tenant can run agents: {status}"
        );

        // The tenant's machine.
        let backend = client.rpc(3, "remoteMachines.backendInfo", json!({})).await;
        let backend_id = result(&backend)["backendId"].as_str().unwrap().to_owned();
        let mut machine = Machine::start(&backend_id).await;
        result(
            &client
                .rpc(4, "remoteMachines.add", machine.add_params())
                .await,
        );

        // In orchestrator mode `workspace.create` needs a home machine, so the
        // workspace is put in the tenant's store directly, over a repository on
        // head; everything after that goes over the wire.
        let repo = dir.path().join("repo");
        repository(&repo);
        let workspace = WorkspaceId(format!("ws-{}", uuid::Uuid::new_v4().simple()));
        let now = intent_core::now_iso();
        services
            .store()
            .insert_workspace(&intent_core::Workspace {
                id: workspace.clone(),
                title: "Remote turn".into(),
                branch: "main".into(),
                repository_path: Some(repo.to_string_lossy().into_owned()),
                created_at: now.clone(),
                updated_at: now,
                ..Default::default()
            })
            .await
            .expect("seed the workspace");

        // An unplaced agent runs on the machine once it reports its providers.
        let deadline = tokio::time::Instant::now() + wait();
        let mut id = 10;
        let agent = loop {
            id += 1;
            let created = client
                .rpc(
                    id,
                    "agent.create",
                    json!({ "workspaceId": workspace, "name": "Remote", "model": "default", "provider": "mock" }),
                )
                .await;
            if created.get("error").is_none() {
                break created["result"]["agent"].clone();
            }
            assert_eq!(created["error"]["data"]["retryable"], true, "{created}");
            assert!(
                tokio::time::Instant::now() < deadline,
                "the machine never took the agent: {created}"
            );
            // timing-guard: poll interval while head reads the machine's providers (no event exposes it)
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        assert_eq!(agent["nodeId"], machine.node_id.as_str(), "{agent}");
        let agent_id = AgentId::from(agent["id"].as_str().unwrap());

        result(
            &client
                .rpc(
                    50,
                    "agent.sendMessage",
                    json!({ "workspaceId": workspace, "agentId": agent_id, "content": "hello machine" }),
                )
                .await,
        );

        // Head dials the machine for the agent: it is told to launch it, then
        // gets the message.
        let (mut end, launch) = machine.launched().await;
        assert_eq!(launch.agent_id, agent_id);
        assert_eq!(launch.provider, "mock");
        end.journal(
            1,
            &agent_id,
            &workspace,
            None,
            JournalBody::Started {
                base_commit: launch.base_commit.clone(),
            },
        )
        .await;
        let (turn, prompt) = end
            .next(wait(), |frame| match frame {
                LinkFrame::Prompt {
                    agent_id: to,
                    turn_id,
                    prompt,
                } if to == agent_id => Some((turn_id, prompt)),
                _ => None,
            })
            .await
            .expect("the message reaches the machine as a prompt");
        assert!(
            serde_json::to_string(&prompt)
                .unwrap()
                .contains("hello machine"),
            "{prompt:?}"
        );
        Some(Self {
            served,
            listeners,
            client,
            machine,
            end,
            agent: agent_id,
            workspace,
            turn,
            _dir: dir,
        })
    }

    /// The machine streams `text` into the turn.
    async fn reply(&self, seq: u64, text: &str) {
        self.end
            .journal(
                seq,
                &self.agent,
                &self.workspace,
                Some(&self.turn),
                JournalBody::Notification {
                    notification: json!({
                        "sessionId": "machine-session",
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": { "type": "text", "text": text },
                        },
                    }),
                },
            )
            .await;
    }

    /// Wait until the agent is idle on head with `text` in its transcript.
    async fn wait_idle_with(&mut self, text: &str) {
        let deadline = tokio::time::Instant::now() + wait();
        let mut id = 100;
        loop {
            id += 1;
            let got = self
                .client
                .rpc(
                    id,
                    "agent.get",
                    json!({ "workspaceId": self.workspace, "agentId": self.agent }),
                )
                .await;
            if result(&got).to_string().contains(text) && result(&got)["agent"]["status"] == "idle"
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the turn never completed on head: {got}"
            );
            // timing-guard: poll interval for the turn's end on head (read over the wire)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The machine ends the turn; whether the send went out (not once the
    /// link closed).
    async fn end_turn(&self, seq: u64) -> bool {
        self.end
            .link
            .send(&LinkFrame::Journal(JournalRecord {
                seq: NodeCounter(seq),
                owner: self.agent.clone().into(),
                workspace_id: self.workspace.clone(),
                turn_id: Some(self.turn.clone()),
                recorded_at: intent_core::now_iso(),
                body: JournalBody::TurnEnded {
                    stop_reason: "end_turn".into(),
                    error: None,
                },
            }))
            .await
            .is_ok()
    }
}

/// A Postgres tenant's agent, created and messaged by a client over the
/// tenant's local socket, is launched on the tenant's machine, gets the
/// message as its prompt, and the machine's reply completes the turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tenant_agent_runs_its_turn_on_a_machine() {
    let Some(pool) = pool().await else { return };
    let tenant = format!("agents-{}", uuid::Uuid::new_v4().simple());
    let Some(mut running) = RunningTurn::start(pool, &tenant).await else {
        return;
    };

    // The machine's reply completes the turn on head.
    running.reply(2, "hi from the machine").await;
    assert!(running.end_turn(3).await);
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 60;
    loop {
        id += 1;
        let got = running
            .client
            .rpc(
                id,
                "agent.get",
                json!({ "workspaceId": running.workspace, "agentId": running.agent }),
            )
            .await;
        let text = result(&got).to_string();
        if text.contains("hi from the machine") && result(&got)["agent"]["status"] == "idle" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the turn never completed on head: {got}"
        );
        // timing-guard: poll interval for the turn's end on head (read over the wire)
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(running.end);
    running.listeners.stop().await;
    running.machine.server.stop().await;
    drop(running.machine.git);
}

/// How many rows the tenant's work wrote: its events and transcript
/// messages, and its interrupted-agent rows.
async fn written(pool: &PgPool, tenant: &str) -> (i64, i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT count(*) FROM event WHERE tenant_id = $1), \
                (SELECT count(*) FROM agent_message WHERE tenant_id = $1), \
                (SELECT count(*) FROM interrupted_agent WHERE tenant_id = $1)",
    )
    .bind(tenant)
    .fetch_one(pool)
    .await
    .expect("count the tenant's rows")
}

/// Stopping a tenant mid-turn ends the turn as `intentd`'s shutdown does:
/// it is recorded as interrupted (with what the machine streamed so far)
/// before the fence refuses the tenant's writes, and nothing of the tenant
/// is written once its lease is released, not even the machine's late end
/// of the turn.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_the_tenant_mid_turn_records_the_turn_interrupted() {
    let Some(pool) = pool().await else { return };
    let tenant = format!("drain-{}", uuid::Uuid::new_v4().simple());
    let Some(mut running) = RunningTurn::start(pool.clone(), &tenant).await else {
        return;
    };
    // The turn runs on head from the prompt on; the machine is mid-reply.
    running.reply(2, "partial from the machine").await;

    // As the binary stops a tenant on SIGTERM: drain, fence, release.
    running.served.drain(None).await;
    assert!(running.served.fence().fire("the test stops the tenant"));
    let report = running.served.stopped().await;
    assert!(report.overran.is_empty(), "{report:?}");
    assert!(
        running.served.heartbeat().release().await.expect("release"),
        "the lease was still ours"
    );

    let (resolution, prev_status): (String, String) = sqlx::query_as(
        "SELECT resolution, prev_status FROM interrupted_agent \
         WHERE tenant_id = $1 AND agent_id = $2",
    )
    .bind(&tenant)
    .bind(&running.agent.0)
    .fetch_optional(&pool)
    .await
    .unwrap()
    .expect("the running turn is recorded as interrupted");
    assert_eq!(resolution, "pending", "it resumes on the next claim");
    assert_eq!(prev_status, "active", "it was recorded mid-turn");

    // Nothing of the tenant lands after the release: the machine's late end
    // of the turn included.
    let released = written(&pool, &tenant).await;
    // The fence closed the link, so the send may fail.
    let _ = running.end_turn(3).await;
    // timing-guard: a late write would land within this window; no event exposes its absence
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(written(&pool, &tenant).await, released);

    drop(running.end);
    running.listeners.stop().await;
    running.machine.server.stop().await;
    drop(running.machine.git);
}

/// Serve `tenant` from `dir` as `instance`, with its client socket at
/// `socket`.
async fn claim(
    pool: &PgPool,
    tenant: &str,
    dir: &Path,
    instance: &str,
    socket: &str,
) -> (
    ServedTenant,
    intentd_orchestrator::listen::ClientListeners,
    Uds,
) {
    let served = serve_tenant(
        pool.clone(),
        tenant,
        dir,
        instance,
        LeaseTiming::default(),
        keyring(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("serve the tenant");
    let listeners = served
        .listen(&ListenConfig {
            socket_path: dir.join(socket),
            bind_addresses: vec![std::net::IpAddr::from([127, 0, 0, 1])],
            ws_port: 0,
            tls_dir: dir.join("tls"),
            insecure: true,
        })
        .await
        .unwrap();
    let mut client = Uds::connect(listeners.socket_path()).await;
    result(
        &client
            .rpc(
                1,
                "client.hello",
                json!({ "clientId": "resume-test", "name": "Resume test" }),
            )
            .await,
    );
    (served, listeners, client)
}

/// Stop serving the tenant as the orchestrator does on SIGTERM: drain it
/// (its running turns are recorded as interrupted), fire the fence, wait
/// for its work to stop, then release the lease so the next claim takes it
/// at once.
async fn release(mut served: ServedTenant) {
    served.drain(None).await;
    served.fence().fire("test: the tenant stops");
    tokio::time::timeout(wait(), served.stopped())
        .await
        .expect("the tenant stops");
    assert!(
        served
            .heartbeat()
            .release()
            .await
            .expect("release the lease"),
        "the lease was still ours"
    );
}

/// How many resume continuations the agent's transcript holds.
async fn continuations(services: &Services, agent: &AgentId) -> usize {
    services
        .store()
        .get_agent_session_with_transcript(agent)
        .await
        .unwrap()
        .messages
        .iter()
        .filter(|m| {
            m.metadata
                .as_ref()
                .is_some_and(|meta| meta["type"] == "resume_continuation")
        })
        .count()
}

/// A tenant stopped while its agent's turn runs on a machine records the
/// turn as interrupted; the next instance that claims the tenant resumes
/// the agent on the machine, and a claim after that does not resume it
/// again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_claimed_tenant_resumes_an_interrupted_agent_exactly_once() {
    let Some(pool) = pool().await else { return };
    if intent_providers_missing_git() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let tenant = format!("resume-{}", uuid::Uuid::new_v4().simple());
    let (first, listeners, mut client) = claim(
        &pool,
        &tenant,
        dir.path(),
        &format!("first-{tenant}"),
        "1.sock",
    )
    .await;

    let backend = client.rpc(2, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = result(&backend)["backendId"].as_str().unwrap().to_owned();
    let mut machine = Machine::start(&backend_id).await;
    result(
        &client
            .rpc(3, "remoteMachines.add", machine.add_params())
            .await,
    );
    let repo = dir.path().join("repo");
    repository(&repo);
    let workspace = WorkspaceId(format!("ws-{}", uuid::Uuid::new_v4().simple()));
    let now = intent_core::now_iso();
    first
        .services()
        .store()
        .insert_workspace(&intent_core::Workspace {
            id: workspace.clone(),
            title: "Resume".into(),
            branch: "main".into(),
            repository_path: Some(repo.to_string_lossy().into_owned()),
            created_at: now.clone(),
            updated_at: now,
            ..Default::default()
        })
        .await
        .expect("seed the workspace");
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 10;
    let agent = loop {
        id += 1;
        let created = client
            .rpc(
                id,
                "agent.create",
                json!({ "workspaceId": workspace, "name": "Remote", "model": "default", "provider": "mock" }),
            )
            .await;
        if created.get("error").is_none() {
            break created["result"]["agent"].clone();
        }
        assert_eq!(created["error"]["data"]["retryable"], true, "{created}");
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the agent: {created}"
        );
        // timing-guard: poll interval while head reads the machine's providers (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let agent_id = AgentId::from(agent["id"].as_str().unwrap());
    result(
        &client
            .rpc(
                50,
                "agent.sendMessage",
                json!({ "workspaceId": workspace, "agentId": agent_id, "content": "long task" }),
            )
            .await,
    );

    // The turn runs on the machine and has not ended when the tenant stops.
    let (end, launch) = machine.launched().await;
    end.journal(
        1,
        &agent_id,
        &workspace,
        None,
        JournalBody::Started {
            base_commit: launch.base_commit.clone(),
        },
    )
    .await;
    let mut end = end;
    end.next(wait(), |frame| match frame {
        LinkFrame::Prompt { agent_id: to, .. } if to == agent_id => Some(()),
        _ => None,
    })
    .await
    .expect("the first turn reaches the machine");
    drop(client);
    listeners.stop().await;
    release(first).await;
    drop(end);

    // The next claim resumes the agent on the machine, unattended.
    let (second, listeners, client) = claim(
        &pool,
        &tenant,
        dir.path(),
        &format!("second-{tenant}"),
        "2.sock",
    )
    .await;
    let (mut end, launch) = machine.launched().await;
    assert_eq!(launch.agent_id, agent_id);
    end.journal(
        1,
        &agent_id,
        &workspace,
        None,
        JournalBody::Started {
            base_commit: launch.base_commit.clone(),
        },
    )
    .await;
    let turn = end
        .next(wait(), |frame| match frame {
            LinkFrame::Prompt {
                agent_id: to,
                turn_id,
                ..
            } if to == agent_id => Some(turn_id),
            _ => None,
        })
        .await
        .expect("the claim resumes the interrupted agent on the machine");
    end.journal(
        2,
        &agent_id,
        &workspace,
        Some(&turn),
        JournalBody::TurnEnded {
            stop_reason: "end_turn".into(),
            error: None,
        },
    )
    .await;
    tokio::time::timeout(wait(), second.runtime().recovered())
        .await
        .expect("the second claim's recovery finishes");
    assert_eq!(continuations(second.services(), &agent_id).await, 1);
    assert!(
        second
            .services()
            .store()
            .get_interrupted_agent(&agent_id)
            .await
            .unwrap()
            .is_none(),
        "the resume resolved the interruption"
    );
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        let status = second
            .services()
            .store()
            .get_agent_session_status(&agent_id)
            .await
            .unwrap();
        if !intent_services::is_stale_in_flight_status(status) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the resumed turn never ended: {status:?}"
        );
        // timing-guard: poll interval for the resumed turn's end on head
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(client);
    listeners.stop().await;
    release(second).await;
    drop(end);

    // A later claim finds nothing left to resume.
    let (third, listeners, client) = claim(
        &pool,
        &tenant,
        dir.path(),
        &format!("third-{tenant}"),
        "3.sock",
    )
    .await;
    tokio::time::timeout(wait(), third.runtime().recovered())
        .await
        .expect("the third claim's recovery finishes");
    assert_eq!(
        continuations(third.services(), &agent_id).await,
        1,
        "the agent resumed exactly once"
    );
    drop(client);
    listeners.stop().await;
    release(third).await;
    machine.server.stop().await;
    drop(machine.git);
}

/// The tenant's agent cap counts only agents running on the orchestrator
/// host: with `agents.maxConcurrent = 1`, a second agent launches on the
/// machine while the first one's turn runs (the machine's own capacity
/// bounds them), and `system.status` reports none against the tenant's cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_agents_do_not_count_against_the_tenant_cap() {
    let Some(pool) = pool().await else { return };
    let tenant = format!("cap-{}", uuid::Uuid::new_v4().simple());
    let Some(mut running) =
        RunningTurn::start_with(pool, &tenant, "[agents]\nmaxConcurrent = 1\n").await
    else {
        return;
    };
    let created = running
        .client
        .rpc(
            60,
            "agent.create",
            json!({ "workspaceId": running.workspace, "name": "Second", "model": "default", "provider": "mock" }),
        )
        .await;
    let second = AgentId::from(result(&created)["agent"]["id"].as_str().unwrap());
    result(
        &running
            .client
            .rpc(
                61,
                "agent.sendMessage",
                json!({ "workspaceId": running.workspace, "agentId": second, "content": "hello second" }),
            )
            .await,
    );
    let launch = running
        .end
        .next(wait(), |frame| match frame {
            LinkFrame::Launch(command) => Some(command),
            _ => None,
        })
        .await
        .expect("the second agent launches while the first one's turn runs");
    assert_eq!(launch.agent_id, second);
    running
        .end
        .journal(
            2,
            &second,
            &running.workspace,
            None,
            JournalBody::Started {
                base_commit: launch.base_commit.clone(),
            },
        )
        .await;
    running
        .end
        .next(wait(), |frame| match frame {
            LinkFrame::Prompt { agent_id, .. } if agent_id == second => Some(()),
            _ => None,
        })
        .await
        .expect("the second agent gets its prompt");

    assert_eq!(
        running.served.agents().registry().size(),
        2,
        "both agents are running"
    );
    let status = result(&running.client.rpc(62, "system.status", json!({})).await).clone();
    assert_eq!(status["maxAgents"], 1, "{status}");
    assert_eq!(
        status["agents"], 0,
        "no agent runs on the orchestrator host: {status}"
    );
    drop(running.end);
    running.listeners.stop().await;
    running.machine.server.stop().await;
    drop(running.machine.git);
}

/// Sets one env var for the test and restores it on drop (nextest runs
/// each test in its own process).
struct EnvVar(&'static str, Option<std::ffi::OsString>);

impl EnvVar {
    fn set(key: &'static str, value: &str) -> Self {
        let old = std::env::var_os(key);
        std::env::set_var(key, value);
        Self(key, old)
    }
}

impl Drop for EnvVar {
    fn drop(&mut self) {
        match self.1.take() {
            Some(value) => std::env::set_var(self.0, value),
            None => std::env::remove_var(self.0),
        }
    }
}

/// An agent idle on its machine past the tenant's idle timeout is reaped:
/// head stops it on the machine (not a final stop: the agent is not
/// deleted) and frees its slot, as `intentd` reaps its idle agents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_remote_agent_is_reaped_after_the_timeout() {
    // The idle-reap test seam: a sub-second timeout and sweep.
    let _reap = EnvVar::set("INTENTD_IDLE_REAP_MS", "300");
    let Some(pool) = pool().await else { return };
    let tenant = format!("reap-{}", uuid::Uuid::new_v4().simple());
    let Some(mut running) = RunningTurn::start(pool, &tenant).await else {
        return;
    };
    running.reply(2, "done").await;
    assert!(running.end_turn(3).await);
    running.wait_idle_with("done").await;

    let agent = running.agent.clone();
    let (stop_id, final_stop) = running
        .end
        .next(wait(), |frame| match frame {
            LinkFrame::Stop {
                agent_id,
                stop_id,
                final_stop,
            } if agent_id == agent => Some((stop_id, final_stop)),
            _ => None,
        })
        .await
        .expect("head stops the idle agent on its machine");
    assert!(!final_stop, "a reap is not a delete");
    running
        .end
        .send(LinkFrame::StopAck {
            stop_id,
            agent_id: agent,
            outcome: StopOutcome::Stopped,
            final_push: None,
        })
        .await;
    let agents = running.served.agents().clone();
    until("the reaped agent's slot is free", || {
        agents.registry().size() == 0
    })
    .await;
    drop(running.end);
    running.listeners.stop().await;
    running.machine.server.stop().await;
    drop(running.machine.git);
}

/// Whether `git` is missing (the test then skips).
fn intent_providers_missing_git() -> bool {
    let missing = Command::new("git").arg("--version").output().is_err();
    if missing {
        eprintln!("skipping: git is not on PATH");
    }
    missing
}

/// The tool the test's HTTP MCP server offers.
const MCP_TOOL: &str = "lookup_forecast";

/// A streamable-HTTP MCP server on loopback that answers `initialize` and
/// `tools/list` (one tool, [`MCP_TOOL`]), as a user's remote server would.
/// Its URL; it serves until the test ends.
async fn http_mcp_server() -> String {
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut socket = tokio::io::BufReader::new(socket);
                loop {
                    let mut length = 0;
                    let mut line = String::new();
                    loop {
                        line.clear();
                        if socket.read_line(&mut line).await.unwrap_or(0) == 0 {
                            return;
                        }
                        if line == "\r\n" {
                            break;
                        }
                        if let Some((name, value)) = line.split_once(':') {
                            if name.eq_ignore_ascii_case("content-length") {
                                length = value.trim().parse().unwrap_or(0);
                            }
                        }
                    }
                    let mut body = vec![0; length];
                    if socket.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let reply = match request["method"].as_str() {
                        Some("initialize") => Some(json!({
                            "protocolVersion": "2025-03-26",
                            "capabilities": { "tools": {} },
                            "serverInfo": { "name": "forecast", "version": "1" },
                        })),
                        Some("tools/list") => Some(json!({ "tools": [{
                            "name": MCP_TOOL,
                            "description": "The forecast for a city",
                            "inputSchema": { "type": "object" },
                        }] })),
                        _ => None,
                    };
                    let response = match (reply, request.get("id")) {
                        (Some(result), Some(id)) => {
                            let body =
                                json!({ "jsonrpc": "2.0", "id": id, "result": result }).to_string();
                            format!(
                                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                                body.len()
                            )
                        }
                        // Notifications and the session DELETE.
                        _ => "HTTP/1.1 202 Accepted\r\ncontent-length: 0\r\n\r\n".to_owned(),
                    };
                    if socket
                        .get_mut()
                        .write_all(response.as_bytes())
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });
    url
}

/// Serve `tenant` as `instance`, over the tenant's local socket under `dir`.
async fn serve_with_client(
    pool: PgPool,
    tenant: &str,
    dir: &Path,
    instance: &str,
) -> (
    intentd_orchestrator::serve::ServedTenant,
    intentd_orchestrator::listen::ClientListeners,
    Uds,
) {
    let served = serve_tenant(
        pool,
        tenant,
        dir,
        instance,
        LeaseTiming::default(),
        keyring(),
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("serve the tenant");
    let listeners = served
        .listen(&ListenConfig {
            socket_path: dir.join(format!("{instance}.sock")),
            bind_addresses: vec![std::net::IpAddr::from([127, 0, 0, 1])],
            ws_port: 0,
            tls_dir: dir.join("tls"),
            insecure: true,
        })
        .await
        .unwrap();
    let mut client = Uds::connect(listeners.socket_path()).await;
    result(
        &client
            .rpc(
                1,
                "client.hello",
                json!({ "clientId": "mcp-test", "name": "MCP test" }),
            )
            .await,
    );
    (served, listeners, client)
}

/// A tenant's HTTP MCP server in its settings is started when the tenant
/// is served, and an agent on the tenant's machine gets it: the launch
/// names it and the machine lists its tools through head. A stdio server
/// is refused: an orchestrator starts no process for a tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tenant_http_mcp_server_reaches_its_machine_agents_and_stdio_is_refused() {
    let Some(pool) = pool().await else { return };
    if intent_providers_missing_git() {
        return;
    }
    let mcp_url = http_mcp_server().await;
    let dir = tempfile::tempdir().unwrap();
    let tenant = format!("mcp-{}", uuid::Uuid::new_v4().simple());

    // The first instance saves the servers, as the settings page does.
    let (first, listeners, mut client) =
        serve_with_client(pool.clone(), &tenant, dir.path(), "first").await;
    let stdio = client
        .rpc(
            2,
            "mcp.servers.create",
            json!({ "config": {
                "id": "local-tool", "name": "Local", "transport": "stdio",
                "command": "/bin/cat", "enabled": true,
            } }),
        )
        .await;
    let refusal = stdio["error"]["message"].as_str().unwrap_or_default();
    assert!(
        refusal.contains("stdio") && refusal.contains("http"),
        "a stdio server is refused with a clear message: {stdio}"
    );
    result(
        &client
            .rpc(
                3,
                "mcp.servers.create",
                json!({ "config": {
                    "id": "forecast", "name": "Forecast", "transport": "http",
                    "url": mcp_url, "enabled": true,
                } }),
            )
            .await,
    );
    let servers = client.rpc(4, "mcp.servers.list", json!({})).await;
    assert_eq!(
        result(&servers)["servers"]
            .as_array()
            .map(|servers| servers.iter().map(|s| s["id"].clone()).collect::<Vec<_>>()),
        Some(vec![json!("forecast")]),
        "only the HTTP server is saved: {servers}"
    );
    drop(client);
    listeners.stop().await;
    let mut first = first;
    first.fence().fire("test: the first instance stops");
    tokio::time::timeout(wait(), first.stopped())
        .await
        .expect("the first instance stops");
    first
        .heartbeat()
        .release()
        .await
        .expect("release the lease");

    // The instance that takes the tenant over starts the saved server.
    let (served, listeners, mut client) =
        serve_with_client(pool, &tenant, dir.path(), "second").await;
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 10;
    loop {
        id += 1;
        let status = client
            .rpc(
                id,
                "mcp.servers.getStatus",
                json!({ "serverId": "forecast" }),
            )
            .await;
        if result(&status)["status"]["state"] == "running" {
            assert_eq!(result(&status)["status"]["toolCount"], 1, "{status}");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "serving the tenant never started its MCP server: {status}"
        );
        // timing-guard: poll interval for the tenant's background MCP start (read over the wire)
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // An agent on the tenant's machine gets the server.
    let backend = client
        .rpc(30, "remoteMachines.backendInfo", json!({}))
        .await;
    let backend_id = result(&backend)["backendId"].as_str().unwrap().to_owned();
    let mut machine = Machine::start(&backend_id).await;
    result(
        &client
            .rpc(31, "remoteMachines.add", machine.add_params())
            .await,
    );
    let repo = dir.path().join("repo");
    repository(&repo);
    let workspace = WorkspaceId(format!("ws-{}", uuid::Uuid::new_v4().simple()));
    let now = intent_core::now_iso();
    served
        .services()
        .store()
        .insert_workspace(&intent_core::Workspace {
            id: workspace.clone(),
            title: "MCP".into(),
            branch: "main".into(),
            repository_path: Some(repo.to_string_lossy().into_owned()),
            created_at: now.clone(),
            updated_at: now,
            ..Default::default()
        })
        .await
        .expect("seed the workspace");
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 40;
    let agent = loop {
        id += 1;
        let created = client
            .rpc(
                id,
                "agent.create",
                json!({ "workspaceId": workspace, "name": "Remote", "model": "default", "provider": "mock" }),
            )
            .await;
        if created.get("error").is_none() {
            break created["result"]["agent"].clone();
        }
        assert_eq!(created["error"]["data"]["retryable"], true, "{created}");
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the agent: {created}"
        );
        // timing-guard: poll interval while head reads the machine's providers (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let agent_id = AgentId::from(agent["id"].as_str().unwrap());
    result(
        &client
            .rpc(
                90,
                "agent.sendMessage",
                json!({ "workspaceId": workspace, "agentId": agent_id, "content": "forecast?" }),
            )
            .await,
    );
    let (end, launch) = machine.launched().await;
    assert_eq!(launch.agent_id, agent_id);
    assert_eq!(
        launch.mcp_proxies,
        vec![intent_core::node_link::McpProxy {
            name: "Forecast".into(),
            server_id: "forecast".into(),
        }],
        "the launch names the tenant's HTTP server"
    );
    end.journal(
        1,
        &agent_id,
        &workspace,
        None,
        JournalBody::Started {
            base_commit: launch.base_commit.clone(),
        },
    )
    .await;
    // The agent's provider lists the server's tools: the machine forwards
    // `tools/list` to head, whose hub runs it.
    let listed = end
        .link
        .call(
            agent_id.clone(),
            intent_core::remote_launch::workspace_api::CALL_METHOD,
            json!({
                "method": intent_core::remote_launch::workspace_api::MCP_PROXY_LIST_TOOLS_METHOD,
                "args": { "serverId": "forecast" },
            }),
            wait(),
        )
        .await
        .expect("head answers the proxied tools/list");
    match listed {
        intent_core::node_link::LinkOutcome::Ok(tools) => {
            assert_eq!(tools["tools"][0]["name"], MCP_TOOL, "{tools}");
        }
        intent_core::node_link::LinkOutcome::Err(other) => {
            panic!("the proxied tools/list failed: {other:?}")
        }
    }
    drop(end);
    listeners.stop().await;
    machine.server.stop().await;
    drop(machine.git);
}
