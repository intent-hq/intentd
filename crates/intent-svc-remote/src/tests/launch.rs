//! The remote-launch port against an exposed machine: admission, placing
//! an agent, its first-turn launch, the projected placement, bind-time
//! claims, git confinement and forwarded `workspace_api` calls.

use std::path::Path;
use std::process::Command;

use intent_core::placement::{AgentPlacement, PlacementUnavailableReason};
use intent_core::remote_launch::{
    EffectiveIsolation, RemoteLaunchNeeds, RemoteLaunchRequest, RemotePlacementView,
    REMOTE_PLACEMENT_METADATA_KEY,
};
use intent_core::{Error, Workspace};
use intent_git::node_git::{self, AgentRemote, GitService};
use intent_services::feature_api::feature_ports::RemoteLaunchPort;
use intent_services::feature_api::Services;
use intent_transport::node_link::NodeGitAccess;

use super::*;

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["-c", "user.name=t", "-c", "user.email=t@t"])
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "git {args:?}: {output:?}");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// A backend with a workspace on a one-commit repository and an exposed
/// machine, connected through the feature's own connector.
struct Fixture {
    backend: Backend,
    services: Services,
    node: RemoteNode,
    repo: tempfile::TempDir,
    base: String,
}

/// The fixture, the machine's end of the link and the machine.
async fn fixture() -> (Fixture, NodeEnd, Machine) {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    let (fixture, link) = Fixture::start_on(backend, &mut machine).await;
    (fixture, link, machine)
}

impl Fixture {
    /// `machine` must have minted its credential for `backend`.
    async fn start_on(backend: Backend, machine: &mut Machine) -> (Self, NodeEnd) {
        let services = backend.services.clone();
        let store = services.store().clone();
        let repo = tempfile::tempdir().unwrap();
        git(repo.path(), &["init", "--quiet"]);
        std::fs::write(repo.path().join("README.md"), "base\n").unwrap();
        git(repo.path(), &["add", "README.md"]);
        git(repo.path(), &["commit", "--quiet", "-m", "base"]);
        let base = git(repo.path(), &["rev-parse", "HEAD"]);
        let now = intent_core::now_iso();
        store
            .insert_workspace(&Workspace {
                id: workspace(),
                title: "Remote".into(),
                branch: "main".into(),
                repository_path: Some(repo.path().to_string_lossy().into_owned()),
                created_at: now.clone(),
                updated_at: now,
                ..Default::default()
            })
            .await
            .unwrap();
        let node = backend.expose(machine).await;
        node.connect().await.unwrap();
        let link = machine.accept().await;
        link.replay_complete(0).await;
        let fixture = Self {
            backend,
            services,
            node,
            repo,
            base,
        };
        fixture.connected().await;
        (fixture, link)
    }

    async fn connected(&self) {
        tokio::time::timeout(wait(), async {
            while !self.node.status().connected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the link binds");
    }

    fn port(&self) -> &dyn RemoteLaunchPort {
        self.services.feature_remote_launch_port()
    }

    /// A created agent, placed on the node with `view`.
    async fn agent(&self, view: &RemotePlacementView) -> AgentId {
        self.agent_with(view, "mock").await
    }

    /// A created agent of `provider`, placed on the node with `view`.
    async fn agent_with(&self, view: &RemotePlacementView, provider: &str) -> AgentId {
        let agent = AgentId::new();
        let now = intent_core::now_iso();
        sqlx::query("INSERT INTO agent_session (id, workspace_id, name, status, provider, created_at, updated_at) VALUES (?, ?, 'remote', 'idle', ?, ?, ?)")
            .bind(agent.as_str())
            .bind(workspace().as_str())
            .bind(provider)
            .bind(&now)
            .bind(&now)
            .execute(self.services.store().write_pool())
            .await
            .unwrap();
        self.services
            .store()
            .set_agent_session_metadata_key_json(
                &workspace(),
                &agent,
                REMOTE_PLACEMENT_METADATA_KEY,
                &serde_json::to_string(view).unwrap(),
                &now,
            )
            .await
            .unwrap();
        agent
    }

    async fn view(&self, agent: &AgentId) -> RemotePlacementView {
        let session = self
            .services
            .store()
            .get_agent_session(agent)
            .await
            .unwrap();
        RemotePlacementView::from_metadata(session.metadata.as_ref()).unwrap()
    }

    async fn admit(&self, placement: &str) -> intent_core::Result<RemotePlacementView> {
        let placement: AgentPlacement = serde_json::from_str(placement).unwrap();
        let needs = RemoteLaunchNeeds {
            provider: "mock".into(),
            ..RemoteLaunchNeeds::default()
        };
        self.port()
            .admit(&self.services, &workspace(), &placement, &needs)
            .await
    }

    async fn placed(&self) -> (AgentId, RemotePlacementView) {
        let view = self.admit(r#"{"target":"remote"}"#).await.unwrap();
        let agent = self.agent(&view).await;
        self.port()
            .launch(
                &self.services,
                RemoteLaunchRequest {
                    workspace_id: workspace(),
                    agent_id: agent.clone(),
                    view: view.clone(),
                },
            )
            .await
            .unwrap();
        (agent, view)
    }
}

fn unavailable(
    result: intent_core::Result<RemotePlacementView>,
) -> intent_core::placement::PlacementUnavailable {
    match result {
        Err(Error::PlacementUnavailable(failure)) => *failure,
        other => panic!("expected placement-unavailable, got {other:?}"),
    }
}

#[tokio::test]
async fn admission_matches_the_machines_platform_and_capacity() {
    let (f, _link, machine) = fixture().await;
    let id = machine.id();

    let view = f
        .admit(r#"{"target":"remote","os":"macos","arch":"aarch64"}"#)
        .await
        .unwrap();
    assert_eq!(view.node_id, id);
    assert_eq!(view.node_name, "Test machine");
    assert_eq!(view.lease_id, f.node.status().lease_id);
    assert_eq!(view.base_commit, f.base);
    assert_eq!(view.effective_isolation, EffectiveIsolation::Pending);
    assert_eq!(
        serde_json::to_value(&view.placement).unwrap(),
        json!({"target":"remote","checkout":"isolated","os":"macos","arch":"aarch64","nodeId":id})
    );

    let linux = unavailable(f.admit(r#"{"os":"linux"}"#).await);
    assert_eq!(
        linux.reason,
        PlacementUnavailableReason::UnsupportedPlatform
    );
    assert_eq!(
        linux.message(),
        "Unsupported linux; available: macos/aarch64; supported: macos/aarch64"
    );
    let elsewhere = unavailable(f.admit(r#"{"nodeId":"other-node"}"#).await);
    assert_eq!(elsewhere.reason, PlacementUnavailableReason::MachineUnknown);
    assert_eq!(
        elsewhere.message(),
        "No machine other-node is exposed to this backend"
    );

    // Both of the node's slots taken by this backend's agents (newer than
    // the machine's report): retryable no-capacity naming the limit.
    let _one = f.node.attach(&AgentId::new(), &workspace());
    let _two = f.node.attach(&AgentId::new(), &workspace());
    let full = unavailable(f.admit(r#"{"target":"remote"}"#).await);
    assert_eq!(full.reason, PlacementUnavailableReason::NoCapacity);
    assert_eq!(
        full.message(),
        "No capacity on Test machine: 2 of 2 agents running"
    );
    assert_eq!(full.data()["retryable"], true);
    assert_eq!(
        full.data()["supportedPlatforms"],
        json!([{"os":"macos","arch":"aarch64"}])
    );

    let help = f.port().placement_help(&f.services).await.unwrap();
    assert!(help.contains("\"Test machine\""), "{help}");
    assert!(
        help.contains(&format!("nodeId `{id}`, macos/aarch64, connected")),
        "{help}"
    );
    assert!(
        help.contains("uncommitted changes are not carried"),
        "{help}"
    );
    f.node.shutdown();
}

#[tokio::test]
async fn an_unreachable_machine_admits_nothing() {
    let (f, link, machine) = fixture().await;
    machine.stop().await;
    link.link.close();
    disconnected(&f.node).await;
    let offline = unavailable(f.admit(r#"{"target":"remote"}"#).await);
    assert_eq!(
        offline.reason,
        PlacementUnavailableReason::MachineUnreachable
    );
    assert_eq!(offline.message(), "Test machine is unreachable");
    assert_eq!(offline.data()["retryable"], true);
    assert!(offline.availability.available_platforms.is_empty());
}

/// Placing an agent records it on the lease and writes its base ref; its
/// first runtime sends the launch with that commit and the repository's
/// cache key; `Started` then marks its checkout isolated.
#[tokio::test]
async fn a_placed_agent_launches_on_its_first_runtime() {
    let (f, mut link, _machine) = fixture().await;
    let (agent, view) = f.placed().await;
    let record = f
        .services
        .store()
        .get_remote_agent(&agent)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.lease_id, view.lease_id);
    let base_ref = AgentRemote::new(workspace().as_str(), agent.as_str())
        .unwrap()
        .ref_name(intent_git::hub::AgentRef::Base);
    assert_eq!(git(f.repo.path(), &["rev-parse", &base_ref]), f.base);

    let runtime = f.port().runtime(&f.services, &agent).await.unwrap();
    let launch = link
        .expect(|frame| match frame {
            LinkFrame::Launch(command) => Some(command),
            _ => None,
        })
        .await;
    assert_eq!(launch.agent_id, agent);
    assert_eq!(launch.provider, "mock");
    // No profile selected: nothing is sent and the node uses its own
    // provider login.
    assert_eq!(launch.profile_env, None);
    assert_eq!(launch.base_commit, f.base);
    assert_eq!(launch.repo_key, node_git::repo_key(f.repo.path()).unwrap());
    // The node gates the agent's workspace API with its session's
    // captured features (review of intentd#2786).
    let session = f.services.store().get_agent_session(&agent).await.unwrap();
    assert_eq!(
        launch.agent_features,
        serde_json::to_value(f.services.session_agent_features(&session)).unwrap()
    );
    assert!(!launch.is_sub_agent);
    assert!(runtime.is_alive());

    link.journal(JournalRecord {
        seq: NodeCounter(1),
        agent_id: agent.clone(),
        workspace_id: workspace(),
        turn_id: None,
        recorded_at: intent_core::now_iso(),
        body: JournalBody::Started {
            base_commit: f.base.clone(),
        },
    })
    .await;
    link.acked(1).await;
    let placed = f.view(&agent).await;
    assert_eq!(placed.effective_isolation, EffectiveIsolation::Isolated);
    assert_eq!(placed.placement_error, None);
    f.node.shutdown();
}

/// The placement change `Started` records is announced as `agent:updated`,
/// so the agent channel pushes the agent's list row with `isolated`
/// instead of leaving it at `pending` until the agent is opened (remote
/// agents showed "Provisioning" on the list indefinitely in
/// cloudlands-fe v2.268.0).
#[tokio::test]
async fn the_isolation_change_is_announced_to_the_agent_list() {
    let (f, mut link, _machine) = fixture().await;
    let (agent, _view) = f.placed().await;
    let _runtime = f.port().runtime(&f.services, &agent).await.unwrap();
    link.expect(|frame| match frame {
        LinkFrame::Launch(command) => Some(command),
        _ => None,
    })
    .await;
    let mut sub = f
        .backend
        .bus
        .subscribe(intent_services::feature_api::SubscriptionFilter {
            event_types: vec![intent_core::events::AGENT_UPDATED.to_owned()],
            ..Default::default()
        });

    link.journal(JournalRecord {
        seq: NodeCounter(1),
        agent_id: agent.clone(),
        workspace_id: workspace(),
        turn_id: None,
        recorded_at: intent_core::now_iso(),
        body: JournalBody::Started {
            base_commit: f.base.clone(),
        },
    })
    .await;
    link.acked(1).await;
    let announced = tokio::time::timeout(wait(), async {
        loop {
            let events = sub.recv().await.expect("subscription open");
            if let Some(event) = events
                .into_iter()
                .find(|e| e.session_id.as_deref() == Some(agent.as_str()))
            {
                return event;
            }
        }
    })
    .await
    .expect("agent:updated arrives for the placed agent");
    assert_eq!(announced.workspace_id, workspace());
    assert_eq!(announced.data["agentId"], agent.as_str());
    assert_eq!(
        f.view(&agent).await.effective_isolation,
        EffectiveIsolation::Isolated
    );
    f.node.shutdown();
}

/// An agent of a saved provider profile starts on the node with the
/// profile's environment: the base provider id, and the overlay with
/// account directories as typed (the node expands `~`).
#[tokio::test]
async fn a_profile_agent_launches_with_the_profiles_environment() {
    use intent_core::WorkspaceApi as _;
    let (f, mut link, _machine) = fixture().await;
    let created = intent_core::caller::with_caller(
        intent_core::caller::Caller::Daemon,
        f.services.provider_profiles_create(json!({
            "baseProviderId": "codex",
            "name": "Work",
            "env": [
                { "key": "CODEX_HOME", "kind": "set", "value": "~/.codex-work" },
                { "key": "CODEX_ACCESS_TOKEN", "kind": "set", "value": "canary-head-secret" },
                { "key": "OPENAI_BASE_URL", "kind": "unset" },
            ],
        })),
    )
    .await
    .expect("create profile");
    let reference = created["profile"]["providerRef"].as_str().unwrap();
    let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
    let agent = f.agent_with(&view, reference).await;
    f.port()
        .launch(
            &f.services,
            RemoteLaunchRequest {
                workspace_id: workspace(),
                agent_id: agent.clone(),
                view,
            },
        )
        .await
        .unwrap();
    let runtime = f.port().runtime(&f.services, &agent).await.unwrap();
    let launch = link
        .expect(|frame| match frame {
            LinkFrame::Launch(command) => Some(command),
            _ => None,
        })
        .await;
    assert_eq!(launch.provider, "codex");
    let wire = launch.profile_env.expect("the profile's overlay").0;
    assert_eq!(wire["providerRef"], reference);
    assert_eq!(
        wire["env"],
        json!([
            { "key": "CODEX_HOME", "value": "~/.codex-work" },
            { "key": "CODEX_ACCESS_TOKEN", "value": "canary-head-secret" },
            { "key": "OPENAI_BASE_URL", "value": null },
        ])
    );
    assert!(runtime.is_alive());
    f.node.shutdown();
}

/// A selected profile that no longer resolves fails the start: nothing
/// is sent, so the node never runs it on its own login.
#[tokio::test]
async fn an_unresolved_profile_fails_the_start_without_launching() {
    let (f, mut link, _machine) = fixture().await;
    let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
    let agent = f.agent_with(&view, "codex@gone").await;
    f.port()
        .launch(
            &f.services,
            RemoteLaunchRequest {
                workspace_id: workspace(),
                agent_id: agent.clone(),
                view,
            },
        )
        .await
        .unwrap();
    let Err(error) = f.port().runtime(&f.services, &agent).await else {
        panic!("an unresolved profile must fail the start");
    };
    assert!(
        matches!(&error, Error::ProviderProfile(failure)
            if failure.data["code"] == "provider-profile-replacement-required"),
        "{error:?}"
    );
    assert!(
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_millis(200)),
            link.expect(|frame| match frame {
                LinkFrame::Launch(command) => Some(command),
                _ => None,
            })
        )
        .await
        .is_err(),
        "no launch is sent for an unresolved profile"
    );
    f.node.shutdown();
}

#[tokio::test]
async fn a_start_failure_is_the_agents_placement_error() {
    let (f, mut link, _machine) = fixture().await;
    let (agent, _) = f.placed().await;
    let runtime = f.port().runtime(&f.services, &agent).await.unwrap();
    link.journal(JournalRecord {
        seq: NodeCounter(1),
        agent_id: agent.clone(),
        workspace_id: workspace(),
        turn_id: None,
        recorded_at: intent_core::now_iso(),
        body: JournalBody::Exited {
            exit_code: None,
            error: Some("node-provider-unavailable: provider \"mock\" is not configured on this node (configured: codex)".into()),
            failure: None,
        },
    })
    .await;
    link.acked(1).await;
    let view = f.view(&agent).await;
    assert_eq!(view.effective_isolation, EffectiveIsolation::Pending);
    let error = view.placement_error.unwrap();
    assert_eq!(error.code, "node-provider-unavailable");
    assert!(
        error.detail.contains("not configured on this node"),
        "{}",
        error.detail
    );
    assert!(!runtime.is_alive());
    f.node.shutdown();
}

#[tokio::test]
async fn an_agent_never_placed_has_no_remote_runtime() {
    let (f, _link, _machine) = fixture().await;
    let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
    let unplaced = f.agent(&view).await;
    let result = f.port().runtime(&f.services, &unplaced).await;
    assert!(
        matches!(&result, Err(Error::InvalidParams(m)) if m.starts_with("node-agent-unplaced")),
        "{:?}",
        result.err()
    );
    f.node.shutdown();
}

/// At bind, an agent the node runs that head has no record of is
/// stopped; one head placed keeps running, and its output reaches the
/// runtime head later takes for it, with no second launch.
#[tokio::test]
async fn binding_stops_unclaimed_agents_and_keeps_claimed_ones() {
    let (f, link, mut machine) = fixture().await;
    let (claimed, _) = f.placed().await;
    let config = f.node.status();
    let feature = f.services.remote_feature().unwrap();
    feature.take_node(&machine.id()).unwrap().shutdown();
    drop(link);

    // A backend restart: a new connector on the same store binds on its
    // own, since an agent is recorded on its lease, and claims it.
    let stray = AgentId::new();
    *machine.host.running.lock().unwrap() = vec![claimed.clone(), stray.clone()];
    let node = f.backend.node(&machine.id()).await;
    assert_eq!(node.status().lease_id, config.lease_id);
    let mut link = machine.accept().await;
    let stopped = link
        .expect(|frame| match frame {
            LinkFrame::Stop { agent_id, .. } => Some(agent_id),
            _ => None,
        })
        .await;
    assert_eq!(stopped, stray);
    link.replay_complete(0).await;
    link.journal(note(1, &claimed, "still working")).await;
    link.acked(1).await;
    let runtime = f.port().runtime(&f.services, &claimed).await.unwrap();
    let notes = runtime.notifications();
    let received = tokio::time::timeout(wait(), async { notes.lock().await.recv().await })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        received.params["update"]["content"]["text"],
        "still working"
    );
    node.shutdown();
}

/// Head serves an agent's refs only on the current binding and only for
/// an agent recorded on that binding's lease.
#[tokio::test]
async fn git_sessions_are_confined_to_the_leases_agents() {
    let (f, link, _machine) = fixture().await;
    let (agent, _) = f.placed().await;
    let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
    let unplaced = f.agent(&view).await;
    let access = crate::git::HeadGitAccess::new(f.node.shared());
    let binding = link.hello.binding.clone();
    let remote = |agent: &AgentId| AgentRemote::new(workspace().as_str(), agent.as_str()).unwrap();

    let path = access
        .repository(&binding, &remote(&agent), GitService::UploadPack)
        .await
        .unwrap();
    assert_eq!(path, f.repo.path());
    assert!(matches!(
        access
            .repository(&binding, &remote(&unplaced), GitService::UploadPack)
            .await,
        Err(Error::Forbidden(_))
    ));
    let other_workspace = AgentRemote::new("ws-other", agent.as_str()).unwrap();
    assert!(matches!(
        access
            .repository(&binding, &other_workspace, GitService::UploadPack)
            .await,
        Err(Error::Forbidden(_))
    ));
    let mut stale = binding.clone();
    stale.generation = NodeCounter(binding.generation.0 - 1);
    assert!(access
        .repository(&stale, &remote(&agent), GitService::UploadPack)
        .await
        .is_err());
    let _ = GitService::UploadPack;
    f.node.shutdown();
}

/// A final stop admits the agent's last push. Below nodeProtocol 9 the
/// machine may acknowledge before pushing, so the push stays admitted for
/// the grace after the ack, then is refused.
#[tokio::test]
async fn a_final_push_is_admitted_through_the_grace_after_a_below_9_ack() {
    let (f, mut link, _machine) = fixture().await;
    let (agent, _) = f.placed().await;
    let access = crate::git::HeadGitAccess::new(f.node.shared());
    let binding = link.hello.binding.clone();
    let remote = AgentRemote::new(workspace().as_str(), agent.as_str()).unwrap();
    f.node
        .shared_ref()
        .set_protocol_for_test(intent_core::node_link::FINAL_STOP_PROTOCOL - 1);
    let stop_id = f
        .node
        .final_stop(
            &agent,
            intent_core::remote_launch::FinalStopReason::AgentDeleted,
        )
        .await;
    assert_eq!(link.stop_for(&agent).await, stop_id);
    link.ack_stop(stop_id, &agent, StopOutcome::Stopped).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        access
            .repository(&binding, &remote, GitService::ReceivePack)
            .await
            .unwrap(),
        f.repo.path(),
        "a push inside the grace is admitted"
    );
    tokio::time::timeout(wait(), async {
        while f
            .backend
            .store()
            .get_final_stop(&agent)
            .await
            .unwrap()
            .is_some()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the grace ends");
    assert!(
        matches!(
            access
                .repository(&binding, &remote, GitService::ReceivePack)
                .await,
            Err(Error::Forbidden(_))
        ),
        "a push after the grace is refused"
    );
    f.node.shutdown();
}

/// A final-stop record admits receive-pack only, never upload-pack; when
/// the workspace's repository is gone the push is unavailable, not denied.
#[tokio::test]
async fn a_final_stop_admits_only_the_push_and_reports_a_missing_repository() {
    let (f, mut link, _machine) = fixture().await;
    let (agent, _) = f.placed().await;
    let access = crate::git::HeadGitAccess::new(f.node.shared());
    let binding = link.hello.binding.clone();
    let remote = AgentRemote::new(workspace().as_str(), agent.as_str()).unwrap();
    let stop_id = f
        .node
        .final_stop(
            &agent,
            intent_core::remote_launch::FinalStopReason::AgentDeleted,
        )
        .await;
    assert_eq!(link.stop_for(&agent).await, stop_id);
    assert_eq!(
        access
            .repository(&binding, &remote, GitService::ReceivePack)
            .await
            .unwrap(),
        f.repo.path()
    );
    assert!(matches!(
        access
            .repository(&binding, &remote, GitService::UploadPack)
            .await,
        Err(Error::Forbidden(_))
    ));
    sqlx::query(
        "UPDATE remote_final_stop SET repo_path = '/nonexistent/intent-repo' WHERE agent_id = ?",
    )
    .bind(agent.as_str())
    .execute(f.backend.store().write_pool())
    .await
    .unwrap();
    match access
        .repository(&binding, &remote, GitService::ReceivePack)
        .await
    {
        Err(Error::Internal(message)) => assert!(
            message.starts_with(intent_transport::node_link::git_codes::UNAVAILABLE),
            "{message}"
        ),
        other => panic!("expected node-git-unavailable, got {other:?}"),
    }
    f.node.shutdown();
}

/// Forward one `workspace_api` frame for `agent` over the node's link.
async fn forward(link: &NodeEnd, agent: &AgentId, method: &str, args: Value) -> LinkOutcome {
    link.link
        .call(
            agent.clone(),
            intent_core::remote_launch::workspace_api::CALL_METHOD,
            json!({ "method": method, "args": args }),
            wait(),
        )
        .await
        .unwrap()
}

fn refused(outcome: LinkOutcome) -> intent_core::node_link::LinkRpcError {
    match outcome {
        LinkOutcome::Err(error) => error,
        LinkOutcome::Ok(value) => panic!("expected a refusal, got {value}"),
    }
}

/// A placed agent's forwarded frame runs on head as that agent, in its
/// workspace.
#[tokio::test]
async fn a_lease_agents_forwarded_call_runs_on_head_as_the_agent() {
    let (f, link, _machine) = fixture().await;
    let (agent, _) = f.placed().await;
    let created = match forward(
        &link,
        &agent,
        "note.create",
        json!({ "title": "From the node", "content": "remote body" }),
    )
    .await
    {
        LinkOutcome::Ok(value) => value,
        LinkOutcome::Err(error) => panic!("forwarded call failed: {error:?}"),
    };
    let notes = f.services.store().list_notes(&workspace()).await.unwrap();
    let note = notes
        .iter()
        .find(|note| note.title == "From the node")
        .unwrap_or_else(|| panic!("note created on head: {created}"));
    assert_eq!(created["id"], json!(note.id.as_str()), "{created}");
    f.node.shutdown();
}

/// Head refuses what the node must not get: an agent not on the lease,
/// a frame head does not forward (even if the node sends it), a
/// malformed call and an unknown link method.
#[tokio::test]
async fn forwarded_calls_are_confined_to_the_leases_agents_and_the_allowlist() {
    let (f, link, _machine) = fixture().await;
    let (agent, _) = f.placed().await;
    let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
    let unplaced = f.agent(&view).await;

    let other = refused(forward(&link, &unplaced, "note.list", json!({})).await);
    assert!(other.message.starts_with("node-agent-denied"), "{other:?}");
    let unknown = refused(forward(&link, &AgentId::new(), "note.list", json!({})).await);
    assert!(
        unknown.message.starts_with("node-agent-denied"),
        "{unknown:?}"
    );

    // `script.*` is forwarded by a protocol-8 machine, but this backend
    // does not run machine runs yet, so it refuses them too.
    for method in [
        "script.list",
        "script.start",
        "file.read",
        "hook.list",
        "workspace.info",
    ] {
        let error = refused(forward(&link, &agent, method, json!({})).await);
        assert!(
            error.message.starts_with("remote-agent-unsupported"),
            "{method}: {error:?}"
        );
    }
    let malformed = refused(
        link.link
            .call(
                agent.clone(),
                intent_core::remote_launch::workspace_api::CALL_METHOD,
                json!({ "args": {} }),
                wait(),
            )
            .await
            .unwrap(),
    );
    assert_eq!(malformed.code, -32602, "{malformed:?}");
    let method = refused(
        link.link
            .call(agent.clone(), "note.list", json!({}), wait())
            .await
            .unwrap(),
    );
    assert_eq!(method.code, -32601, "{method:?}");
    assert!(f
        .services
        .store()
        .list_notes(&workspace())
        .await
        .unwrap()
        .is_empty());
    f.node.shutdown();
}

/// A request that arrived on a replaced binding is refused.
#[tokio::test]
async fn a_forwarded_call_on_a_replaced_binding_is_refused() {
    let (f, link, _machine) = fixture().await;
    let (agent, _) = f.placed().await;
    let calls = crate::rpc::HeadCalls::new(f.services.clone());
    let shared = f.node.shared();
    let current = link.hello.binding.clone();
    let mut stale = current.clone();
    stale.generation = NodeCounter(current.generation.0 - 1);
    let request = intent_core::node_link::LinkRequest {
        request_id: uuid::Uuid::new_v4(),
        agent_id: agent.clone(),
        method: intent_core::remote_launch::workspace_api::CALL_METHOD.into(),
        params: json!({ "method": "note.list", "args": {} }),
        timeout_ms: 5_000,
    };
    for current in [Some(&current), None] {
        let error = refused(calls.outcome(&shared, current, &stale, &request).await);
        assert!(error.message.starts_with("node-link-stale"), "{error:?}");
    }
    let fresh = calls
        .outcome(&shared, Some(&current), &current, &request)
        .await;
    assert!(matches!(fresh, LinkOutcome::Ok(_)), "{fresh:?}");
    f.node.shutdown();
}

/// Review of intentd#2786: the node reads the workspace's auto-commit
/// policy before an agent-initiated commit; head answers it only for a
/// lease agent, from the workspace's own setting.
#[tokio::test]
async fn the_commit_policy_is_the_workspaces_and_only_for_lease_agents() {
    use intent_core::remote_launch::workspace_api::COMMIT_POLICY_METHOD;
    let (f, link, _machine) = fixture().await;
    let (agent, _) = f.placed().await;
    for enabled in [false, true] {
        f.services
            .store()
            .set_workspace_auto_commit(&workspace(), enabled)
            .await
            .unwrap();
        match forward(&link, &agent, COMMIT_POLICY_METHOD, Value::Null).await {
            LinkOutcome::Ok(value) => assert_eq!(value, json!({ "autoCommit": enabled })),
            LinkOutcome::Err(error) => panic!("policy read failed: {error:?}"),
        }
    }
    let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
    let unplaced = f.agent(&view).await;
    let denied = refused(forward(&link, &unplaced, COMMIT_POLICY_METHOD, Value::Null).await);
    assert!(
        denied.message.starts_with("node-agent-denied"),
        "{denied:?}"
    );
    f.node.shutdown();
}

/// A TCP proxy on `ip`:`port` to 127.0.0.1:`port` that reports each
/// accepted connection and holds it until `release` fires.
#[cfg(target_os = "linux")]
async fn held_proxy(
    ip: [u8; 4],
    port: u16,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<()>,
    tokio::sync::watch::Sender<bool>,
) {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::from(ip), port))
        .await
        .unwrap();
    let (accepted_tx, accepted) = tokio::sync::mpsc::unbounded_channel();
    let (release, released) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        while let Ok((mut inbound, _)) = listener.accept().await {
            let _ = accepted_tx.send(());
            let mut released = released.clone();
            tokio::spawn(async move {
                let _ = released.wait_for(|go| *go).await;
                let Ok(mut outbound) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await
                else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
            });
        }
    });
    (accepted, release)
}

/// Review of intentd#2872: a credential replacement whose probe is slow
/// must not let an agent start under the old credential meanwhile, only to
/// abandon it when the replacement lands. The start waits for the
/// replacement and is then refused, since its lease is gone.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_start_during_a_credential_replacement_waits_and_is_refused() {
    let (f, _link, machine) = fixture().await;
    let view = f.admit(r#"{"target":"remote"}"#).await.unwrap();
    let agent = f.agent(&view).await;
    f.port()
        .launch(
            &f.services,
            RemoteLaunchRequest {
                workspace_id: workspace(),
                agent_id: agent.clone(),
                view: view.clone(),
            },
        )
        .await
        .unwrap();

    let (mut accepted, release) = held_proxy([127, 0, 0, 2], machine.port).await;
    let token = "b".repeat(64);
    let credential = Machine::mint(machine.services.store(), &f.backend.id().await, &token).await;
    let mut params = machine.add_params();
    params["credentialId"] = json!(credential.id);
    params["token"] = json!(token);
    params["hosts"] = json!(["127.0.0.2"]);
    let services = f.services.clone();
    let add = tokio::spawn(async move {
        with_caller(Caller::Daemon, services.remote_machines("add", params)).await
    });
    // The replacement is past its in-use check and probing.
    tokio::time::timeout(wait(), accepted.recv())
        .await
        .expect("the probe dials the proxy");

    let services = f.services.clone();
    let start_agent = agent.clone();
    let start = tokio::spawn(async move {
        services
            .feature_remote_launch_port()
            .runtime(&services, &start_agent)
            .await
            .map(|_| ())
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    release.send_replace(true);

    let added = tokio::time::timeout(wait(), add).await.unwrap().unwrap();
    let started = tokio::time::timeout(wait(), start).await.unwrap().unwrap();
    let added = added.expect("the replacement lands");
    assert_eq!(added["replacedCredentialId"], json!(machine.credential.id));
    let error = started.expect_err("the start under the old lease is refused");
    assert!(
        error.to_string().contains("node-agent-unplaced"),
        "{error:?}"
    );
}

/// The connected-machine count is the exposed machines holding a live link
/// now: one while the link is bound, none once the machine closes it.
#[tokio::test]
async fn connected_machines_count_live_links() {
    let (fixture, link, _machine) = fixture().await;
    let feature = fixture.services.remote_feature().unwrap();
    assert_eq!(feature.connected_machines(), 1);
    link.link.close();
    disconnected(&fixture.node).await;
    assert_eq!(feature.connected_machines(), 0);
}

/// The sessions in the fixture's workspace.
async fn sessions(f: &Fixture) -> Vec<intent_core::AgentSession> {
    f.services
        .store()
        .list_agent_sessions(&workspace())
        .await
        .unwrap()
}

/// A forwarded call's value, or a panic with its refusal.
fn answered(outcome: LinkOutcome) -> Value {
    match outcome {
        LinkOutcome::Ok(value) => value,
        LinkOutcome::Err(error) => panic!("forwarded call failed: {error:?}"),
    }
}

/// A sub-agent `create` frame with the mock provider the machine offers.
fn child(extra: Value) -> Value {
    let mut args = json!({
        "name": "child",
        "message": "hello",
        "provider": "mock",
        "model": "mock-model",
    });
    if let (Some(args), Value::Object(extra)) = (args.as_object_mut(), extra) {
        args.extend(extra);
    }
    args
}

/// The remote placement of `agent`, if it has one.
async fn placement_of(f: &Fixture, agent: &str) -> Option<RemotePlacementView> {
    let session = f
        .services
        .store()
        .get_agent_session(&AgentId::from(agent))
        .await
        .unwrap();
    RemotePlacementView::from_metadata(session.metadata.as_ref())
}

/// A remote agent's sub-agent with no placement runs on the same machine,
/// launched like an explicit placement, as the parent's child.
#[tokio::test]
async fn a_remote_agents_sub_agent_defaults_to_its_machine() {
    let (f, link, machine) = fixture().await;
    let (parent, _) = f.placed().await;
    let created = answered(forward(&link, &parent, "agent.create", child(json!({}))).await);
    let id = created["agentId"].as_str().unwrap();
    let view = placement_of(&f, id).await.expect("the child is placed");
    assert_eq!(view.node_id, machine.id());
    assert_eq!(view.placement_error, None);
    assert_eq!(created["nodeId"], json!(machine.id()), "{created}");
    assert!(
        created["notice"]
            .as_str()
            .is_some_and(|n| n.contains("Test machine")),
        "{created}"
    );
    let session = f
        .services
        .store()
        .get_agent_session(&AgentId::from(id))
        .await
        .unwrap();
    assert_eq!(session.parent_agent_id, Some(parent.clone()));

    // agent.delegate inherits the machine the same way.
    let delegated = answered(
        forward(
            &link,
            &parent,
            "agent.delegate",
            json!({ "taskText": "do it", "provider": "mock", "model": "mock-model" }),
        )
        .await,
    );
    let id = delegated["agentId"].as_str().unwrap();
    assert_eq!(
        placement_of(&f, id).await.expect("placed").node_id,
        machine.id()
    );
    f.node.shutdown();
}

/// An inherited placement the machine cannot honour fails with the
/// normal error, never quietly running on the backend, and says how to.
#[tokio::test]
async fn an_inherited_placement_fails_without_falling_back() {
    let (f, link, _machine) = fixture().await;
    let (parent, _) = f.placed().await;
    let before = sessions(&f).await.len();
    let hint = "The sub-agent defaults to \"Test machine\" because its parent runs there; pass placement { target: \"local\" } to run it on the backend.";

    let model = refused(
        forward(
            &link,
            &parent,
            "agent.create",
            child(json!({ "model": "no-such-model" })),
        )
        .await,
    );
    assert!(
        model.message.contains("isn't available") && model.message.contains(hint),
        "{model:?}"
    );
    assert!(
        model.message.contains("\"code\":\"model-unavailable\""),
        "{model:?}"
    );

    // Both of the machine's slots in use: no-capacity on that machine.
    let _one = f.node.attach(&AgentId::new(), &workspace());
    let _two = f.node.attach(&AgentId::new(), &workspace());
    let full = refused(forward(&link, &parent, "agent.create", child(json!({}))).await);
    assert!(
        full.message
            .starts_with("No capacity on Test machine: 2 of 2 agents running"),
        "{full:?}"
    );
    assert!(full.message.contains(hint), "{full:?}");
    assert!(
        full.message.contains("\"code\":\"placement-unavailable\""),
        "{full:?}"
    );
    assert_eq!(sessions(&f).await.len(), before, "nothing was created");
    f.node.shutdown();
}

/// `placement: { target: "local" }` runs a remote agent's sub-agent on the
/// backend; with isolation it may combine on delegate, while isolation on
/// an inherited placement is refused.
#[tokio::test]
async fn target_local_runs_a_remote_agents_sub_agent_on_the_backend() {
    let (f, link, _machine) = fixture().await;
    let (parent, _) = f.placed().await;
    let created = answered(
        forward(
            &link,
            &parent,
            "agent.create",
            child(json!({ "placement": { "target": "local" } })),
        )
        .await,
    );
    let id = created["agentId"].as_str().unwrap();
    assert!(placement_of(&f, id).await.is_none(), "{created}");
    assert!(created.get("nodeId").is_none(), "{created}");

    let mixed = refused(
        forward(
            &link,
            &parent,
            "agent.create",
            child(json!({ "placement": { "target": "local", "os": "macos" } })),
        )
        .await,
    );
    assert!(
        mixed.message.contains(
            "placement.target \"local\" runs the agent on the backend; omit os, arch, nodeId, checkout and exclusive"
        ),
        "{mixed:?}"
    );

    let isolated = refused(
        forward(
            &link,
            &parent,
            "agent.delegate",
            json!({ "taskText": "do it", "provider": "mock", "isolation": "cow" }),
        )
        .await,
    );
    assert!(
        isolated.message.contains(
            "agent.delegate: a remote agent's sub-agents run on its machine in their own checkout, so isolation does not apply; pass placement { target: \"local\" } to use isolation on the backend"
        ),
        "{isolated:?}"
    );
    let batch = refused(
        forward(
            &link,
            &parent,
            "agent.delegate",
            json!({ "tasks": ["note-1"], "isolation": "cow" }),
        )
        .await,
    );
    assert!(
        batch.message.contains("so isolation does not apply"),
        "{batch:?}"
    );
    f.node.shutdown();
}

/// Creates by the user and by a local agent are unchanged: no placement
/// means the backend.
#[tokio::test]
async fn user_and_local_agent_creates_stay_on_the_backend() {
    let (f, _link, _machine) = fixture().await;
    let local = AgentId::new();
    let now = intent_core::now_iso();
    sqlx::query("INSERT INTO agent_session (id, workspace_id, name, status, provider, created_at, updated_at) VALUES (?, ?, 'local', 'idle', 'mock', ?, ?)")
        .bind(local.as_str())
        .bind(workspace().as_str())
        .bind(&now)
        .bind(&now)
        .execute(f.services.store().write_pool())
        .await
        .unwrap();
    let session = f.services.store().get_agent_session(&local).await.unwrap();
    let host = intent_acp::make_workspace_host_for_bridge(
        std::sync::Arc::new(f.services.clone()),
        workspace(),
        local.clone(),
        None,
        f.services.session_agent_features(&session),
        false,
        wait(),
    );
    let created = host(json!({ "method": "agent.create", "args": child(json!({})) }))
        .await
        .expect("a local agent's create");
    let id = created["agentId"].as_str().unwrap();
    assert!(placement_of(&f, id).await.is_none(), "{created}");

    let created = with_caller(
        Caller::Daemon,
        f.services.agent_create(
            workspace(),
            Some("user".into()),
            Some("mock-model".into()),
            None,
            None,
            None,
            intent_core::AgentCreateExtra {
                provider: Some("mock".into()),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("a user's create");
    let id = created["agent"]["id"].as_str().unwrap();
    assert!(placement_of(&f, id).await.is_none(), "{created}");
    f.node.shutdown();
}

/// A remote agent reads its workspace's details through head; the
/// setters, and `workspace.info` (answered on the machine), are refused.
#[tokio::test]
async fn a_remote_agent_reads_workspace_details_but_cannot_set_them() {
    let (f, link, _machine) = fixture().await;
    let (agent, _) = f.placed().await;
    let details = answered(forward(&link, &agent, "workspace.details", json!({})).await);
    assert_eq!(details["id"], json!(workspace().as_str()), "{details}");
    assert_eq!(details["title"], json!("Remote"), "{details}");
    for (method, args) in [
        ("workspace.setTitle", json!({ "title": "Renamed" })),
        (
            "workspace.setStatusMessage",
            json!({ "statusMessage": "x" }),
        ),
        ("workspace.archive", json!({})),
        ("workspace.info", json!({})),
    ] {
        let error = refused(forward(&link, &agent, method, args).await);
        assert!(
            error.message.starts_with("remote-agent-unsupported"),
            "{method}: {error:?}"
        );
    }
    let workspace = f
        .services
        .store()
        .get_workspace(&workspace())
        .await
        .unwrap();
    assert_eq!(workspace.title, "Remote");
    f.node.shutdown();
}

/// A not-started task note in the fixture's workspace.
async fn seed_task(f: &Fixture, title: &str) -> String {
    intent_core::caller::with_caller(
        intent_core::caller::Caller::Daemon,
        seed_task_as_daemon(f, title),
    )
    .await
}

async fn seed_task_as_daemon(f: &Fixture, title: &str) -> String {
    use intent_core::WorkspaceApi as _;
    let note = f
        .services
        .create_note(
            workspace(),
            intent_core::NoteCreate {
                title: title.into(),
                content: Some(format!("{title} body")),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .unwrap()
        .note;
    f.services
        .mark_as_task(
            workspace(),
            note.id.clone(),
            "not_started".into(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    note.id.0
}

/// The batch row for `task` in an `agent.delegate` `tasks` response.
fn batch_row<'a>(response: &'a Value, task: &str) -> &'a Value {
    response["tasks"]
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["taskNoteId"] == json!(task)))
        .unwrap_or_else(|| panic!("row for {task} in {response}"))
}

/// A remote agent's batch delegate starts its children on its machine even
/// when the provider is not available on the backend ("mock" is not
/// runnable here, only on the machine).
#[tokio::test]
async fn a_remote_agents_batch_delegate_needs_the_provider_only_on_its_machine() {
    let (f, link, machine) = fixture().await;
    let (parent, _) = f.placed().await;
    let first = seed_task(&f, "First").await;
    let second = seed_task(&f, "Second").await;
    let response = answered(
        forward(
            &link,
            &parent,
            "agent.delegate",
            json!({ "tasks": [first, second], "provider": "mock", "model": "mock-model" }),
        )
        .await,
    );
    for task in [&first, &second] {
        let row = batch_row(&response, task);
        assert_eq!(row["disposition"], "started", "{response}");
        let id = row["agentId"].as_str().unwrap();
        assert_eq!(
            placement_of(&f, id).await.expect("placed").node_id,
            machine.id()
        );
    }
    f.node.shutdown();
}

/// One batch child the machine refuses fails in its own row; another
/// eligible task still starts on the machine.
#[tokio::test]
async fn a_batch_child_the_machine_refuses_does_not_stop_the_others() {
    let (f, link, machine) = fixture().await;
    let (parent, _) = f.placed().await;
    let refused_task = seed_task(&f, "Refused").await;
    let eligible = seed_task(&f, "Eligible").await;
    let response = answered(
        forward(
            &link,
            &parent,
            "agent.delegate",
            json!({
                "tasks": [
                    { "taskNoteId": refused_task, "model": "no-such-model" },
                    eligible,
                ],
                "provider": "mock",
                "model": "mock-model",
            }),
        )
        .await,
    );
    let row = batch_row(&response, &refused_task);
    assert_eq!(row["disposition"], "error", "{response}");
    assert!(
        row["reason"].as_str().is_some_and(
            |reason| reason.contains("model-unavailable") || reason.contains("isn't available")
        ),
        "{response}"
    );
    let row = batch_row(&response, &eligible);
    assert_eq!(row["disposition"], "started", "{response}");
    let id = row["agentId"].as_str().unwrap();
    assert_eq!(
        placement_of(&f, id).await.expect("placed").node_id,
        machine.id()
    );
    f.node.shutdown();
}
