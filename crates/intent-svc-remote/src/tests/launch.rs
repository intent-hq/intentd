//! The remote-launch port against an exposed machine: admission, placing
//! an agent, its first-turn launch, the projected placement, bind-time
//! claims, git confinement and forwarded `workspace_api` calls.

use std::path::Path;
use std::process::Command;

use intent_core::placement::{AgentPlacement, PlacementUnavailableReason};
use intent_core::remote_launch::{
    EffectiveIsolation, RemoteLaunchRequest, RemotePlacementView, REMOTE_PLACEMENT_METADATA_KEY,
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
        self.port()
            .admit(&self.services, &workspace(), &placement)
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
    assert_eq!(
        elsewhere.reason,
        PlacementUnavailableReason::UnsupportedPlatform
    );

    // Both of the node's slots taken: retryable no-capacity.
    let _one = f.node.attach(&AgentId::new(), &workspace());
    let _two = f.node.attach(&AgentId::new(), &workspace());
    let full = unavailable(f.admit(r#"{"target":"remote"}"#).await);
    assert_eq!(full.reason, PlacementUnavailableReason::NoCapacity);
    assert_eq!(full.data()["retryable"], true);
    assert_eq!(
        full.data()["supportedPlatforms"],
        json!([{"os":"macos","arch":"aarch64"}])
    );

    let help = f.port().placement_help(&f.services).await.unwrap();
    assert!(help.contains("\"Test machine\""), "{help}");
    assert!(help.contains(&format!("nodeId `{id}`, macos/aarch64, connected")), "{help}");
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
    assert_eq!(offline.reason, PlacementUnavailableReason::NoCapacity);
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
            LinkFrame::Stop { agent_id } => Some(agent_id),
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
    let remote =
        |agent: &AgentId| AgentRemote::new(workspace().as_str(), agent.as_str()).unwrap();

    let path = access.repository(&binding, &remote(&agent)).await.unwrap();
    assert_eq!(path, f.repo.path());
    assert!(matches!(
        access.repository(&binding, &remote(&unplaced)).await,
        Err(Error::Forbidden(_))
    ));
    let other_workspace = AgentRemote::new("ws-other", agent.as_str()).unwrap();
    assert!(matches!(
        access.repository(&binding, &other_workspace).await,
        Err(Error::Forbidden(_))
    ));
    let mut stale = binding.clone();
    stale.generation = NodeCounter(binding.generation.0 - 1);
    assert!(access.repository(&stale, &remote(&agent)).await.is_err());
    let _ = GitService::UploadPack;
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

    for method in ["script.list", "file.read", "hook.list", "workspace.info"] {
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
