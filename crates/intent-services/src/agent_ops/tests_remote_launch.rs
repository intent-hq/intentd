//! Creating agents with a remote `placement`: admission before any effect,
//! the daemon-owned placement on the created agent, its projection, and
//! the create seams that carry or refuse a placement.

use std::sync::{Arc, Mutex};

use intent_core::placement::{AgentPlacement, PlatformAvailability};
use intent_core::remote_launch::{
    EffectiveIsolation, RemoteLaunchNeeds, RemoteLaunchRequest, RemotePlacementView,
    REMOTE_PLACEMENT_METADATA_KEY,
};
use intent_core::{AgentCreateExtra, AgentDelegateInput, AgentId, BoxFuture, Error, WorkspaceId};
use serde_json::{json, Value};

use super::tests::setup;
use crate::feature_ports::{RemoteLaunchPort, RemoteRuntime};
use crate::Services;

const BASE: &str = "0123456789abcdef0123456789abcdef01234567";

/// A remote-launch port that admits every placement onto one node and
/// records what it was asked.
#[derive(Default)]
struct FakeRemote {
    launched: Mutex<Vec<RemoteLaunchRequest>>,
    needs: Mutex<Vec<RemoteLaunchNeeds>>,
    /// Every placement admitted, in order.
    admitted: Mutex<Vec<AgentPlacement>>,
    fail_launch: bool,
    /// A `nodeId` admission refuses with `placement-unavailable`.
    refuse_node: Option<&'static str>,
    /// The node's own sign-in is out: a machine sign-in launch is refused
    /// with `machine-not-signed-in`, as placement would.
    signed_out: bool,
    /// Each final stop recorded, and whether the agent's script-stop wait
    /// (`end_agent_machine_runs`) had already run then.
    final_stops: Mutex<Vec<(AgentId, bool)>>,
}

fn view(placement: &AgentPlacement) -> RemotePlacementView {
    let mut resolved = placement.clone();
    resolved.node_id = Some("node-1".into());
    RemotePlacementView {
        node_id: "node-1".into(),
        lease_id: "lease-1".into(),
        node_name: "Mac Studio".into(),
        placement: resolved,
        effective_isolation: EffectiveIsolation::Pending,
        placement_error: None,
        base_commit: BASE.into(),
        submodules: None,
        setup: None,
        setup_script: None,
        skills: None,
        shared_checkout_of: None,
        parent_work: None,
        launched_on: None,
        cloud_start: None,
    }
}

impl RemoteLaunchPort for FakeRemote {
    fn final_stop<'a>(
        &'a self,
        svc: &'a Services,
        agent_ids: &'a [AgentId],
        _reason: intent_core::remote_launch::FinalStopReason,
    ) -> BoxFuture<'a, ()> {
        let scripts_stopped = crate::test_support::fake_runtime::FakeRuntime::of(svc)
            .calls()
            .contains(&"end_agent_machine_runs");
        for agent in agent_ids {
            self.final_stops
                .lock()
                .unwrap()
                .push((agent.clone(), scripts_stopped));
        }
        Box::pin(async {})
    }

    fn placements<'a>(
        &'a self,
        _svc: &'a Services,
    ) -> BoxFuture<'a, intent_core::Result<PlatformAvailability>> {
        Box::pin(async { Ok(PlatformAvailability::none(intent_core::now_iso())) })
    }

    fn placement_help<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a intent_core::WorkspaceId,
        _render: intent_core::machine_selection::PlacementHelpRenderer,
    ) -> BoxFuture<'a, Option<String>> {
        Box::pin(async { Some("Remote placement: node \"Mac Studio\"".into()) })
    }

    fn admit<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        placement: &'a AgentPlacement,
        needs: &'a RemoteLaunchNeeds,
    ) -> BoxFuture<'a, intent_core::Result<RemotePlacementView>> {
        self.needs.lock().unwrap().push(needs.clone());
        if self.signed_out
            && needs.machine_sign_in
            && !needs.profile_fallback
            && !placement.is_cloud()
        {
            let error = intent_core::machines::machine_not_signed_in(
                &needs.provider,
                &[intent_core::machines::SignedOutMachine {
                    node_id: "node-1".into(),
                    name: "Mac Studio".into(),
                    status: intent_core::node_link::ProviderStatus::SignedOut,
                    observed_at: intent_core::now_iso(),
                }],
                false,
            );
            return Box::pin(async move { Err(error) });
        }
        if placement.node_id.is_some() && placement.node_id.as_deref() == self.refuse_node {
            let failure =
                PlatformAvailability::none(intent_core::now_iso()).unavailable(placement.clone());
            return Box::pin(async move { Err(Error::PlacementUnavailable(Box::new(failure))) });
        }
        self.admitted.lock().unwrap().push(placement.clone());
        Box::pin(async move { Ok(view(placement)) })
    }

    fn launch<'a>(
        &'a self,
        _svc: &'a Services,
        request: RemoteLaunchRequest,
    ) -> BoxFuture<'a, intent_core::Result<()>> {
        Box::pin(async move {
            self.launched.lock().unwrap().push(request);
            if self.fail_launch {
                return Err(Error::Internal("base ref write failed".into()));
            }
            Ok(())
        })
    }

    fn runtime<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, intent_core::Result<Arc<RemoteRuntime>>> {
        Box::pin(async { Err(Error::InvalidParams("node-unavailable: test".into())) })
    }
}

fn placement(json: Value) -> AgentPlacement {
    serde_json::from_value(json).unwrap()
}

async fn create(
    svc: &Services,
    ws: &WorkspaceId,
    extra: AgentCreateExtra,
) -> intent_core::Result<Value> {
    svc.agent_create_op(
        ws.clone(),
        Some("Remote".into()),
        Some("sonnet4.5".into()),
        None,
        None,
        None,
        false,
        AgentCreateExtra {
            provider: Some("auggie".into()),
            ..extra
        },
    )
    .await
}

#[tokio::test]
async fn a_placed_create_persists_its_node_and_reports_what_it_starts_from() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    let created = create(
        &svc,
        &ws,
        AgentCreateExtra {
            placement: Some(placement(json!({"target": "remote", "os": "macos"}))),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let agent = &created["agent"];
    assert_eq!(agent["nodeId"], "node-1");
    assert_eq!(agent["leaseId"], "lease-1");
    assert_eq!(agent["nodeName"], "Mac Studio");
    assert_eq!(agent["effectiveIsolation"], "pending");
    assert_eq!(
        agent["placement"],
        json!({"target": "remote", "os": "macos", "nodeId": "node-1"})
    );
    assert!(agent.get("placementError").is_none());
    let notice = created["notice"].as_str().unwrap();
    assert!(notice.contains("\"Mac Studio\""), "{notice}");
    assert!(notice.contains(&BASE[..12]), "{notice}");
    assert!(
        notice.contains("Uncommitted changes in the workspace are not carried"),
        "{notice}"
    );

    // Placement sees the provider and model, and that no profile is sent.
    assert_eq!(
        remote.needs.lock().unwrap().clone(),
        [RemoteLaunchNeeds {
            provider: "auggie".into(),
            model: Some("sonnet4.5".into()),
            with_profile: false,
            sets_profile_env: false,
            provider_ref: "auggie".into(),
            inherited: false,
            machine_sign_in: false,
            profile_fallback: false,
        }]
    );

    let id = AgentId::from(agent["id"].as_str().unwrap());
    let launched = remote.launched.lock().unwrap().clone();
    assert_eq!(launched.len(), 1);
    assert_eq!(launched[0].agent_id, id);
    assert_eq!(launched[0].workspace_id, ws);
    // Every read projection serves the same fields.
    let got = svc
        .agent_get_op(id.clone(), Some(ws.clone()))
        .await
        .unwrap();
    assert_eq!(got.node_name.as_deref(), Some("Mac Studio"));
    assert_eq!(got.effective_isolation, Some(EffectiveIsolation::Pending));
    // List rows carry the placement so the list can badge remote agents;
    // only the lease stays detail-only.
    let listed = svc.agent_list_op(ws.clone()).await.unwrap();
    let row = serde_json::to_value(listed.iter().find(|a| a.id == id).unwrap()).unwrap();
    assert_eq!(row["nodeId"], "node-1", "{row}");
    assert_eq!(row["nodeName"], "Mac Studio", "{row}");
    assert_eq!(row["effectiveIsolation"], "pending", "{row}");
    assert_eq!(row["placement"], agent["placement"], "{row}");
    assert!(row.get("leaseId").is_none(), "{row}");
}

#[tokio::test]
async fn list_rows_carry_placement_for_remote_agents_only() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    let placed = create(
        &svc,
        &ws,
        AgentCreateExtra {
            placement: Some(placement(json!({"target": "remote", "os": "macos"}))),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let local = create(&svc, &ws, AgentCreateExtra::default())
        .await
        .unwrap();
    let placed_id = placed["agent"]["id"].as_str().unwrap();
    let local_id = local["agent"]["id"].as_str().unwrap();

    let listed = svc.agent_list_op(ws.clone()).await.unwrap();
    let row =
        |id: &str| serde_json::to_value(listed.iter().find(|a| a.id.0 == id).unwrap()).unwrap();
    let placed_row = row(placed_id);
    for key in ["nodeId", "nodeName", "placement", "effectiveIsolation"] {
        assert!(
            placed_row.get(key).is_some(),
            "remote row lacks `{key}`: {placed_row}"
        );
    }
    assert!(placed_row.get("leaseId").is_none(), "{placed_row}");
    let local_row = row(local_id);
    for key in [
        "nodeId",
        "leaseId",
        "nodeName",
        "placement",
        "effectiveIsolation",
        "placementError",
    ] {
        assert!(
            local_row.get(key).is_none(),
            "local row carries `{key}`: {local_row}"
        );
    }
}

#[tokio::test]
async fn a_local_create_carries_no_placement_even_if_the_caller_forges_one() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    let forged = json!({ REMOTE_PLACEMENT_METADATA_KEY: view(&AgentPlacement::default()) });
    let created = create(
        &svc,
        &ws,
        AgentCreateExtra {
            metadata: Some(forged),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(created["agent"].get("nodeId").is_none(), "{created}");
    assert!(created.get("notice").is_none());
    assert!(remote.launched.lock().unwrap().is_empty());
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let session = svc
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert!(RemotePlacementView::from_metadata(session.metadata.as_ref()).is_none());
}

#[tokio::test]
async fn a_placement_no_node_can_take_creates_nothing() {
    let (_t, svc, ws) = setup().await;
    // The default port: no execution node.
    let before = svc.agent_list_op(ws.clone()).await.unwrap().len();
    let result = create(
        &svc,
        &ws,
        AgentCreateExtra {
            placement: Some(placement(json!({"target": "remote", "arch": "x86_64"}))),
            ..Default::default()
        },
    )
    .await;
    let Err(Error::PlacementUnavailable(failure)) = result else {
        panic!("expected placement-unavailable, got {result:?}");
    };
    assert_eq!(failure.data()["code"], "placement-unavailable");
    assert_eq!(failure.data()["reason"], "unsupported-platform");
    assert_eq!(svc.agent_list_op(ws.clone()).await.unwrap().len(), before);
}

#[tokio::test]
async fn unsupported_placements_are_refused_before_any_effect() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    for (placement_json, expected) in [
        (
            json!({"target": "local", "os": "linux"}),
            "omit os, arch, nodeId, checkout and exclusive",
        ),
        (
            json!({"target": "remote", "exclusive": true}),
            "exclusive is not supported",
        ),
    ] {
        let result = create(
            &svc,
            &ws,
            AgentCreateExtra {
                placement: Some(placement(placement_json)),
                ..Default::default()
            },
        )
        .await;
        assert!(
            matches!(&result, Err(Error::InvalidParams(m)) if m.contains(expected)),
            "{result:?}"
        );
    }
    assert!(remote.launched.lock().unwrap().is_empty());
    // `{ target: "local" }` alone runs the agent on the backend.
    let result = create(
        &svc,
        &ws,
        AgentCreateExtra {
            placement: Some(placement(json!({"target": "local"}))),
            ..Default::default()
        },
    )
    .await;
    assert!(result.is_ok(), "{result:?}");
    assert!(remote.needs.lock().unwrap().is_empty());
    assert!(remote.launched.lock().unwrap().is_empty());
    // A selected profile that does not resolve fails admission, never
    // running on the node's own login instead.
    let result = svc
        .admit_remote_agent(
            &ws,
            "claude-code@work",
            None,
            &placement(json!({"target": "remote"})),
            false,
        )
        .await;
    assert!(
        matches!(&result, Err(Error::ProviderProfile(f))
            if f.data["code"] == "provider-profile-replacement-required"),
        "{result:?}"
    );
    assert!(remote.launched.lock().unwrap().is_empty());
}

/// A create that selects a saved provider profile is admitted (its
/// environment is sent when the agent starts on the node).
#[tokio::test]
async fn a_saved_profile_is_admitted_for_a_remote_agent() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    let created = intent_core::caller::with_caller(
        intent_core::caller::Caller::Daemon,
        svc.provider_profiles_create_impl(json!({
            "baseProviderId": "codex",
            "name": "Work",
            "env": [{ "key": "OPENAI_API_KEY", "kind": "set", "value": "sk-test-remote" }],
        })),
    )
    .await
    .expect("create profile");
    let reference = created["profile"]["providerRef"].as_str().unwrap();
    let (view, _) = svc
        .admit_remote_agent(
            &ws,
            reference,
            Some("gpt-5"),
            &placement(json!({"target": "remote"})),
            false,
        )
        .await
        .expect("a resolvable profile is admitted");
    assert_eq!(view.node_id, "node-1");
    // The machines report the base provider; the profile's environment is
    // sent, so their own sign-in does not apply.
    assert_eq!(
        remote.needs.lock().unwrap().clone(),
        [RemoteLaunchNeeds {
            provider: "codex".into(),
            model: Some("gpt-5".into()),
            with_profile: true,
            sets_profile_env: true,
            provider_ref: reference.into(),
            inherited: false,
            machine_sign_in: false,
            profile_fallback: false,
        }]
    );
}

/// A launch that fails after the agent exists leaves it placed (so it can
/// never run locally) with a placement error, and reports the failure.
#[tokio::test]
async fn a_failed_launch_marks_the_agent_and_fails_the_create() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote {
        fail_launch: true,
        ..FakeRemote::default()
    });
    let svc = svc.with_remote_launch_port(remote.clone());
    let result = create(
        &svc,
        &ws,
        AgentCreateExtra {
            placement: Some(placement(json!({"target": "remote"}))),
            ..Default::default()
        },
    )
    .await;
    assert!(
        matches!(&result, Err(Error::Internal(m)) if m.contains("base ref write failed")),
        "{result:?}"
    );
    let id = remote.launched.lock().unwrap()[0].agent_id.clone();
    let got = svc.agent_get_op(id, Some(ws)).await.unwrap();
    let error = got.placement_error.expect("placement error recorded");
    assert_eq!(error.code, "remote-launch-failed");
    assert!(error.detail.contains("base ref write failed"));
}

#[tokio::test]
async fn delegate_refuses_a_remote_placement_with_isolation() {
    let (_t, svc, ws) = setup().await;
    let svc = svc.with_remote_launch_port(Arc::new(FakeRemote::default()));
    let remote = || Some(placement(json!({"target": "remote"})));
    let delegate = |isolation: &str| {
        svc.agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                placement: remote(),
                isolation: Some(isolation.into()),
                task_text: Some("Do it".into()),
                ..Default::default()
            },
            None,
        )
    };
    let shared = delegate("shared").await;
    assert!(
        matches!(&shared, Err(Error::InvalidParams(m)) if m.contains("cannot be combined")),
        "{shared:?}"
    );
    // The retired value is refused first, whatever the placement.
    let cow = delegate("cow").await;
    assert!(matches!(&cow, Err(Error::IsolationRemoved)), "{cow:?}");
}

#[tokio::test]
async fn the_placement_wire_shape_is_closed() {
    let parse = |v: Value| serde_json::from_value::<AgentCreateExtra>(v);
    assert!(parse(json!({})).unwrap().placement.is_none());
    assert!(parse(json!({"placement": {}})).unwrap().placement.is_some());
    assert!(parse(json!({"placement": null})).is_err());
    assert!(parse(json!({"placement": {"node": "x"}})).is_err());
    let delegate = |v: Value| serde_json::from_value::<AgentDelegateInput>(v);
    assert!(delegate(json!({"placement": null})).is_err());
    assert_eq!(
        delegate(json!({"placement": {"os": "macos"}}))
            .unwrap()
            .placement
            .unwrap()
            .os,
        Some(intent_core::nodes::NodeOs::Macos)
    );
    let wake = |v: Value| serde_json::from_value::<intent_core::AgentWakeCreateOptions>(v);
    assert!(wake(json!({"placement": {"target": "remote"}}))
        .unwrap()
        .placement
        .is_some());
}

/// Review of intentd#2779: caller metadata that is not an object must not
/// drop the placement (the agent would then run locally).
#[tokio::test]
async fn a_placed_create_keeps_its_placement_with_non_object_metadata() {
    let (_t, svc, ws) = setup().await;
    let svc = svc.with_remote_launch_port(Arc::new(FakeRemote::default()));
    for metadata in [json!([1, 2]), json!("legacy"), json!(7)] {
        let created = create(
            &svc,
            &ws,
            AgentCreateExtra {
                placement: Some(placement(json!({"target": "remote"}))),
                metadata: Some(metadata.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            created["agent"]["nodeId"], "node-1",
            "{metadata}: {created}"
        );
        let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
        let session = svc
            .store
            .get_agent_session_with_transcript(&id)
            .await
            .unwrap();
        assert!(RemotePlacementView::from_metadata(session.metadata.as_ref()).is_some());
        assert_eq!(
            session.metadata.unwrap()["priorNonObjectMetadata"],
            metadata
        );
    }
}

/// The agent's machine ("node-1"), exposed to this backend with a report
/// listing `providers`.
async fn expose_node(svc: &Services, providers: Vec<intent_core::node_link::ProviderCapability>) {
    use intent_core::node_link::{MachineCapabilities, MachineCapacity};
    let platform = intent_core::placement::AgentPlatform {
        os: intent_core::nodes::NodeOs::Macos,
        arch: intent_core::nodes::NodeArch::Aarch64,
    };
    let observed_at = intent_core::now_iso();
    svc.store
        .save_exposed_machine(&intent_core::machines::ExposedMachine {
            id: "node-1".into(),
            name: "Mac Studio".into(),
            hosts: vec!["10.0.0.1".into()],
            port: 7443,
            tc_address: None,
            fingerprint: "ab".repeat(32),
            credential_id: intent_core::ids::ExecutorCredentialId::from("cred-1".to_string()),
            platform: Some(platform),
            capabilities: Some(MachineCapabilities {
                platform,
                intentd_version: "0.11.87".into(),
                capacity: MachineCapacity {
                    running_agents: 1,
                    agent_cap: 4,
                    charged_memory_bytes: 0,
                    memory_budget_bytes: None,
                },
                providers,
                disabled_providers: vec![],
                observed_at: observed_at.clone(),
            }),
            observed_at: Some(observed_at),
            state: intent_core::machines::RemoteMachineState::Disconnected,
            added_at: intent_core::now_iso(),
            last_connected_at: None,
            node_protocol: None,
            cloud: None,
        })
        .await
        .unwrap();
}

fn offered(
    id: &str,
    status: intent_core::node_link::ProviderStatus,
    models: &[&str],
) -> intent_core::node_link::ProviderCapability {
    intent_core::node_link::ProviderCapability {
        id: id.into(),
        visible: true,
        status,
        models: models.iter().map(|m| (*m).to_owned()).collect(),
        models_stale: false,
    }
}

async fn remote_agent(svc: &Services, ws: &WorkspaceId) -> AgentId {
    let created = create(
        svc,
        ws,
        AgentCreateExtra {
            placement: Some(placement(json!({"target": "remote"}))),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    AgentId::from(created["agent"]["id"].as_str().unwrap())
}

async fn daemon_profile(svc: &Services, base: &str, name: &str, env: Value) -> String {
    let created = intent_core::caller::with_caller(
        intent_core::caller::Caller::Daemon,
        svc.provider_profiles_create_impl(json!({
            "baseProviderId": base,
            "name": name,
            "env": env,
        })),
    )
    .await
    .expect("create profile");
    created["profile"]["providerRef"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn assert_not_remote_compatible(
    result: &intent_core::Result<impl std::fmt::Debug>,
    reference: &str,
    key: &str,
) {
    let Err(Error::ProviderProfile(failure)) = result else {
        panic!("expected profile-not-remote-compatible, got {result:?}");
    };
    assert!(!failure.internal);
    assert_eq!(failure.data["code"], "profile-not-remote-compatible");
    assert_eq!(failure.data["providerRef"], reference);
    assert_eq!(failure.data["reason"], "account-folder");
    assert_eq!(failure.data["keys"], json!([key]));
    assert!(failure.data["name"].is_string(), "{}", failure.data);
    assert!(
        failure.message.contains("account folder") && failure.message.contains(key),
        "{}",
        failure.message
    );
}

/// A profile that sets an account folder never runs on another machine, so
/// placement refuses it before asking any machine (§5.54 "Remote
/// compatibility"). A Default that sets one is never marked: with machine
/// sign-in on (the default) it sends nothing and the machine's own sign-in
/// runs it.
#[tokio::test]
async fn placement_refuses_a_configuration_tied_to_this_backend() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    let folder = daemon_profile(
        &svc,
        "codex",
        "Work",
        json!([{ "key": "CODEX_HOME", "kind": "set", "value": "~/.codex-work" }]),
    )
    .await;
    let result = svc
        .admit_remote_agent(
            &ws,
            &folder,
            Some("gpt-5"),
            &placement(json!({"target": "remote"})),
            false,
        )
        .await;
    assert_not_remote_compatible(&result, &folder, "CODEX_HOME");

    intent_core::caller::with_caller(
        intent_core::caller::Caller::Daemon,
        svc.provider_profiles_update_default_impl(json!({
            "baseProviderId": "claude-code",
            "env": [{ "key": "CLAUDE_CONFIG_DIR", "kind": "set", "value": "~/.claude-a" }],
        })),
    )
    .await
    .unwrap();
    assert!(
        remote.needs.lock().unwrap().is_empty(),
        "no machine was asked for the folder profile"
    );
    svc.admit_remote_agent(
        &ws,
        "claude-code",
        None,
        &placement(json!({"target": "remote"})),
        false,
    )
    .await
    .expect("an unmarked Default runs through machine sign-in");
    let needs = remote.needs.lock().unwrap().clone();
    assert_eq!(needs.len(), 1);
    assert!(
        !needs[0].with_profile,
        "the folder never leaves the backend"
    );
    assert!(needs[0].machine_sign_in);
}

/// A remote agent switches to another model its machine offers: saved, and
/// the next start relaunches it there.
#[tokio::test]
async fn a_remote_agent_switches_to_a_model_its_machine_offers() {
    use intent_core::node_link::ProviderStatus;
    let (_t, svc, ws) = setup().await;
    let svc = svc.with_remote_launch_port(Arc::new(FakeRemote::default()));
    expose_node(
        &svc,
        vec![offered(
            "auggie",
            ProviderStatus::SignedIn,
            &["sonnet4.5", "opus4.5"],
        )],
    )
    .await;
    let id = remote_agent(&svc, &ws).await;
    svc.agent_set_model_op(id.clone(), "opus4.5".into(), None)
        .await
        .expect("a model the machine lists is accepted");
    let session = svc
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.model.as_deref(), Some("opus4.5"));
    assert_eq!(session.provider.as_deref(), Some("auggie"));
}

/// A model the agent's machine does not list, or a provider it cannot run,
/// is refused with `model-unavailable` naming that machine only, and nothing
/// changes.
#[tokio::test]
async fn a_remote_agent_cannot_switch_to_what_its_machine_does_not_offer() {
    use intent_core::node_link::ProviderStatus;
    let (_t, svc, ws) = setup().await;
    let svc = svc.with_remote_launch_port(Arc::new(FakeRemote::default()));
    expose_node(
        &svc,
        vec![
            offered("auggie", ProviderStatus::SignedIn, &["sonnet4.5"]),
            offered("claude-code", ProviderStatus::SignedOut, &["opus"]),
        ],
    )
    .await;
    let id = remote_agent(&svc, &ws).await;
    let result = svc
        .agent_set_model_op(id.clone(), "opus4.5".into(), None)
        .await;
    let Err(Error::ModelUnavailable(failure)) = &result else {
        panic!("expected model-unavailable, got {result:?}");
    };
    let data = failure.data();
    assert_eq!(data["code"], "model-unavailable");
    assert_eq!(data["model"], "opus4.5");
    let machines = data["machines"].as_array().unwrap();
    assert_eq!(machines.len(), 1, "{data}");
    assert_eq!(machines[0]["nodeId"], "node-1");
    // The Default sends nothing, and the machine is signed out there.
    let result = svc
        .agent_set_model_op(id.clone(), "opus".into(), Some("claude-code".into()))
        .await;
    let Err(Error::RemoteMachine(failure)) = &result else {
        panic!("expected machine-not-signed-in, got {result:?}");
    };
    assert_eq!(failure.data["code"], "machine-not-signed-in");
    assert_eq!(failure.data["machines"][0]["nodeId"], "node-1");
    assert_eq!(failure.data["machines"][0]["status"], "signed-out");
    let session = svc
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.model.as_deref(), Some("sonnet4.5"));
    assert_eq!(session.provider.as_deref(), Some("auggie"));
}

/// Switching to a profile: an account-folder profile is refused as not
/// remote-compatible; an API-key profile runs even where the machine's own
/// sign-in is missing, as at placement.
#[tokio::test]
async fn a_remote_agent_switches_only_to_a_remote_compatible_profile() {
    use intent_core::node_link::ProviderStatus;
    let (_t, svc, ws) = setup().await;
    let svc = svc.with_remote_launch_port(Arc::new(FakeRemote::default()));
    expose_node(
        &svc,
        vec![
            offered("auggie", ProviderStatus::SignedIn, &["sonnet4.5"]),
            offered("codex", ProviderStatus::SignedOut, &["gpt-5"]),
        ],
    )
    .await;
    let id = remote_agent(&svc, &ws).await;
    let folder = daemon_profile(
        &svc,
        "codex",
        "Work",
        json!([{ "key": "CODEX_HOME", "kind": "set", "value": "~/.codex-work" }]),
    )
    .await;
    let result = svc
        .agent_set_model_op(id.clone(), "gpt-5".into(), Some(folder.clone()))
        .await;
    assert_not_remote_compatible(&result, &folder, "CODEX_HOME");
    let session = svc
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.provider.as_deref(), Some("auggie"));

    let api_key = daemon_profile(
        &svc,
        "codex",
        "API key",
        json!([{ "key": "OPENAI_API_KEY", "kind": "set", "value": "sk-test-remote" }]),
    )
    .await;
    svc.agent_set_model_op(id.clone(), "gpt-5".into(), Some(api_key.clone()))
        .await
        .expect("an API-key profile the machine's provider can run is accepted");
    let session = svc
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.provider.as_deref(), Some(api_key.as_str()));
    assert_eq!(session.model.as_deref(), Some("gpt-5"));
}

/// A remote agent whose machine is no longer exposed cannot switch: there
/// is nowhere to relaunch it.
#[tokio::test]
async fn a_remote_agent_on_a_removed_machine_cannot_switch() {
    let (_t, svc, ws) = setup().await;
    let svc = svc.with_remote_launch_port(Arc::new(FakeRemote::default()));
    let id = remote_agent(&svc, &ws).await;
    let result = svc
        .agent_set_model_op(id.clone(), "opus4.5".into(), None)
        .await;
    let Err(Error::PlacementUnavailable(failure)) = &result else {
        panic!("expected placement-unavailable, got {result:?}");
    };
    assert_eq!(failure.data()["reason"], "machine-unknown");
    let session = svc
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    assert_eq!(session.model.as_deref(), Some("sonnet4.5"));
}

/// Remote parity S6: a message with image or file attachments to a remote
/// agent is accepted and stored like a local agent's (images ride the
/// prompt, files are fetched by `ws.file.getAttachment` on the machine).
#[tokio::test]
async fn a_message_with_attachments_to_a_remote_agent_is_accepted() {
    let (_t, svc, ws) = setup().await;
    let svc = svc.with_remote_launch_port(Arc::new(FakeRemote::default()));
    let created = create(
        &svc,
        &ws,
        AgentCreateExtra {
            placement: Some(placement(json!({"target": "remote"}))),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let before = svc.store.get_agent_messages(&id, None).await.unwrap().len();
    let images = json!([{ "data": "aGVsbG8=", "mimeType": "image/png" }]);
    let files = json!([{ "attachmentId": "att-1", "fileName": "notes.txt" }]);
    for (images, files) in [(Some(images.clone()), None), (None, Some(files.clone()))] {
        svc.agent_send_message_op(id.clone(), "look".into(), None, images, files, None)
            .await
            .unwrap();
    }
    assert!(svc.store.get_agent_messages(&id, None).await.unwrap().len() > before);
}

/// The `"default"` model sentinel asks the machines for no particular
/// model.
#[tokio::test]
async fn the_default_model_sentinel_requests_no_model() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    svc.admit_remote_agent(
        &ws,
        "auggie",
        Some("default"),
        &placement(json!({"target": "remote"})),
        false,
    )
    .await
    .unwrap();
    assert_eq!(remote.needs.lock().unwrap()[0].model, None);
}

/// A failed read of a remote parent's session fails its sub-agent's create
/// and delegate, never quietly running the child on the backend.
#[tokio::test]
async fn a_failed_parent_read_fails_the_sub_agent_instead_of_running_it_locally() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    let parent = remote_agent(&svc, &ws).await;
    let launched = remote.launched.lock().unwrap().len();
    let count = || async {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_session")
            .fetch_one(svc.store().write_pool())
            .await
            .unwrap()
    };
    let before = count().await;
    sqlx::query("UPDATE agent_session SET metadata = '{not json' WHERE id = ?")
        .bind(parent.as_str())
        .execute(svc.store().write_pool())
        .await
        .unwrap();
    let caller = intent_core::Caller::Agent {
        agent_id: parent.clone(),
    };

    let read =
        intent_core::with_caller(caller.clone(), svc.inherited_placement(Some(&parent))).await;
    assert!(matches!(&read, Err(Error::Internal(_))), "{read:?}");

    let created = intent_core::with_caller(caller.clone(), async {
        svc.agent_create_op(
            ws.clone(),
            Some("Child".into()),
            Some("sonnet4.5".into()),
            None,
            Some(parent.clone()),
            None,
            false,
            AgentCreateExtra {
                provider: Some("auggie".into()),
                ..Default::default()
            },
        )
        .await
    })
    .await;
    assert!(matches!(&created, Err(Error::Internal(_))), "{created:?}");

    let delegated = intent_core::with_caller(caller, async {
        svc.agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                task_text: Some("Do it".into()),
                ..Default::default()
            },
            Some(parent.clone()),
        )
        .await
    })
    .await;
    assert!(
        matches!(&delegated, Err(Error::Internal(_))),
        "{delegated:?}"
    );

    assert_eq!(remote.launched.lock().unwrap().len(), launched);
    assert_eq!(count().await, before, "nothing was created");
}

/// Docs K (lease end): `agent.delete` records a remote agent's final stop
/// only after the bounded wait for its machine runs' script stops, so the
/// machine never ends a script without a requested stop first.
#[tokio::test]
async fn deleting_a_remote_agent_records_its_final_stop_after_the_script_stop_wait() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = crate::test_support::fake_runtime::FakeRuntime::install(svc)
        .with_remote_launch_port(remote.clone());
    create(
        &svc,
        &ws,
        AgentCreateExtra {
            placement: Some(placement(json!({"target": "remote"}))),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let id = remote.launched.lock().unwrap()[0].agent_id.clone();
    svc.agent_delete_op(id.clone(), Some(ws)).await.unwrap();
    assert_eq!(*remote.final_stops.lock().unwrap(), vec![(id, true)]);
}

async fn seed_task(svc: &Services, ws: &WorkspaceId, title: &str) -> intent_core::NoteId {
    use intent_core::WorkspaceApi as _;
    intent_core::caller::with_caller(intent_core::caller::Caller::Daemon, async {
        let note = svc
            .create_note(
                ws.clone(),
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
            .expect("create note")
            .note;
        svc.mark_as_task(
            ws.clone(),
            note.id.clone(),
            "not_started".into(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .expect("markAsTask");
        note.id
    })
    .await
}

/// A batch `tasks` entry from its wire form.
fn entry(value: Value) -> intent_core::BatchTaskEntry {
    serde_json::from_value(value).expect("batch entry")
}

fn bare(id: &intent_core::NoteId) -> intent_core::BatchTaskEntry {
    intent_core::BatchTaskEntry::Id(id.clone())
}

/// A batch delegation made by `parent` (a remote agent) through its
/// `workspace_api`.
async fn batch_from(
    svc: &Services,
    ws: &WorkspaceId,
    parent: &AgentId,
    input: AgentDelegateInput,
) -> intent_core::Result<Value> {
    let caller = intent_core::Caller::Agent {
        agent_id: parent.clone(),
    };
    intent_core::with_caller(
        caller,
        svc.agent_delegate_op(ws.clone(), input, Some(parent.clone())),
    )
    .await
}

fn row<'a>(result: &'a Value, task: &intent_core::NoteId) -> &'a Value {
    result["tasks"]
        .as_array()
        .expect("tasks rows")
        .iter()
        .find(|r| r["taskNoteId"] == task.0.as_str())
        .unwrap_or_else(|| panic!("no row for {task:?}: {result}"))
}

async fn child(svc: &Services, ws: &WorkspaceId, row: &Value) -> intent_core::AgentLite {
    let id = AgentId::from(row["agentId"].as_str().expect("agentId"));
    svc.agent_get_op(id, Some(ws.clone())).await.expect("child")
}

async fn agent_count(svc: &Services) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_session")
        .fetch_one(svc.store().write_pool())
        .await
        .unwrap()
}

/// G2: a remote agent sends a whole batch to another machine with one
/// batch-level placement, instead of every child inheriting its machine.
#[tokio::test]
async fn a_remote_agents_batch_runs_on_another_machine() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    let parent = remote_agent(&svc, &ws).await;
    let first = seed_task(&svc, &ws, "First").await;
    let second = seed_task(&svc, &ws, "Second").await;
    let before = remote.admitted.lock().unwrap().len();

    let result = batch_from(
        &svc,
        &ws,
        &parent,
        AgentDelegateInput {
            placement: Some(placement(json!({"target": "remote", "nodeId": "node-2"}))),
            tasks: Some(vec![bare(&first), bare(&second)]),
            ..Default::default()
        },
    )
    .await
    .expect("batch delegate");

    assert_eq!(result["summary"]["started"], 2, "{result}");
    let admitted = remote.admitted.lock().unwrap()[before..].to_vec();
    assert_eq!(admitted.len(), 2, "{admitted:?}");
    for placement in admitted {
        assert_eq!(placement.node_id.as_deref(), Some("node-2"));
    }
}

/// G2: `placement: { target: "local" }` on a batch runs every task on the
/// backend, so a remote agent no longer delegates them one at a time.
#[tokio::test]
async fn a_remote_agents_batch_runs_on_the_backend_with_target_local() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    let parent = remote_agent(&svc, &ws).await;
    let first = seed_task(&svc, &ws, "First").await;
    let second = seed_task(&svc, &ws, "Second").await;
    let before = remote.needs.lock().unwrap().len();

    let result = batch_from(
        &svc,
        &ws,
        &parent,
        AgentDelegateInput {
            placement: Some(placement(json!({"target": "local"}))),
            tasks: Some(vec![bare(&first), bare(&second)]),
            ..Default::default()
        },
    )
    .await
    .expect("batch delegate");

    assert_eq!(result["summary"]["started"], 2, "{result}");
    assert_eq!(
        remote.needs.lock().unwrap().len(),
        before,
        "no machine asked"
    );
    for task in [&first, &second] {
        let agent = child(&svc, &ws, row(&result, task)).await;
        assert!(agent.node_id.is_none(), "{agent:?}");
        assert!(agent.placement.is_none(), "{agent:?}");
    }
}

/// G2: each task is admitted as its own delegation. An entry's placement
/// replaces the batch's; a task whose machine refuses it gets an error row
/// with nothing written, and the others still start.
#[tokio::test]
async fn a_batch_task_failing_admission_does_not_stop_the_others() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote {
        refuse_node: Some("node-gone"),
        ..FakeRemote::default()
    });
    let svc = svc.with_remote_launch_port(remote.clone());
    let parent = remote_agent(&svc, &ws).await;
    let on_node_2 = seed_task(&svc, &ws, "On node 2").await;
    let refused = seed_task(&svc, &ws, "Refused").await;
    let on_backend = seed_task(&svc, &ws, "On the backend").await;
    let before = remote.admitted.lock().unwrap().len();
    let agents = agent_count(&svc).await;

    let result = batch_from(
        &svc,
        &ws,
        &parent,
        AgentDelegateInput {
            placement: Some(placement(json!({"target": "remote", "nodeId": "node-2"}))),
            tasks: Some(vec![
                bare(&on_node_2),
                entry(json!({
                    "taskNoteId": refused.0,
                    "placement": {"target": "remote", "nodeId": "node-gone"},
                })),
                entry(json!({
                    "taskNoteId": on_backend.0,
                    "placement": {"target": "local"},
                })),
            ]),
            ..Default::default()
        },
    )
    .await
    .expect("batch delegate");

    assert_eq!(result["summary"]["started"], 2, "{result}");
    assert_eq!(result["summary"]["errors"], 1, "{result}");
    let failed = row(&result, &refused);
    assert_eq!(failed["disposition"], "error", "{result}");
    assert!(
        failed["reason"]
            .as_str()
            .is_some_and(|r| r.contains("placement-unavailable")),
        "{result}"
    );
    assert_eq!(
        agent_count(&svc).await,
        agents + 2,
        "nothing written for it"
    );

    let admitted = remote.admitted.lock().unwrap()[before..].to_vec();
    assert_eq!(admitted.len(), 1, "{admitted:?}");
    assert_eq!(admitted[0].node_id.as_deref(), Some("node-2"));
    let placed = child(&svc, &ws, row(&result, &on_node_2)).await;
    assert_eq!(placed.node_id.as_deref(), Some("node-1"), "fake view node");
    let local = child(&svc, &ws, row(&result, &on_backend)).await;
    assert!(local.node_id.is_none(), "{local:?}");
}

/// G2: every placement in a batch is checked before any task starts: a
/// malformed entry placement, or a task that would run remotely in a call
/// with `isolation`, fails the whole batch.
#[tokio::test]
async fn a_batch_refuses_bad_placements_before_any_task_starts() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    let parent = remote_agent(&svc, &ws).await;
    let first = seed_task(&svc, &ws, "First").await;
    let second = seed_task(&svc, &ws, "Second").await;
    let agents = agent_count(&svc).await;
    let shared = || Some("shared".to_string());

    let cases = [
        (
            "batch remote + isolation",
            AgentDelegateInput {
                placement: Some(placement(json!({"target": "remote"}))),
                isolation: shared(),
                tasks: Some(vec![bare(&first), bare(&second)]),
                ..Default::default()
            },
            "cannot be combined",
        ),
        (
            "entry remote + isolation",
            AgentDelegateInput {
                placement: Some(placement(json!({"target": "local"}))),
                isolation: shared(),
                tasks: Some(vec![
                    bare(&first),
                    entry(json!({"taskNoteId": second.0, "placement": {"target": "remote"}})),
                ]),
                ..Default::default()
            },
            "cannot be combined",
        ),
        (
            "inherited + isolation",
            AgentDelegateInput {
                isolation: shared(),
                tasks: Some(vec![
                    entry(json!({"taskNoteId": first.0, "placement": {"target": "local"}})),
                    bare(&second),
                ]),
                ..Default::default()
            },
            "isolation does not apply",
        ),
        (
            "local with a machine filter",
            AgentDelegateInput {
                tasks: Some(vec![
                    bare(&first),
                    entry(json!({
                        "taskNoteId": second.0,
                        "placement": {"target": "local", "os": "macos"},
                    })),
                ]),
                ..Default::default()
            },
            "omit os, arch",
        ),
        (
            "malformed batch placement every entry replaces",
            AgentDelegateInput {
                placement: Some(placement(json!({"target": "local", "os": "macos"}))),
                tasks: Some(vec![
                    entry(json!({"taskNoteId": first.0, "placement": {"target": "local"}})),
                    entry(json!({"taskNoteId": second.0, "placement": {"target": "local"}})),
                ]),
                ..Default::default()
            },
            "omit os, arch",
        ),
        (
            "exclusive entry",
            AgentDelegateInput {
                tasks: Some(vec![
                    bare(&first),
                    entry(json!({
                        "taskNoteId": second.0,
                        "placement": {"target": "remote", "exclusive": true},
                    })),
                ]),
                ..Default::default()
            },
            "exclusive",
        ),
    ];
    for (name, input, want) in cases {
        let result = batch_from(&svc, &ws, &parent, input).await;
        assert!(
            matches!(&result, Err(Error::InvalidParams(m)) if m.contains(want)),
            "{name}: {result:?}"
        );
    }
    assert_eq!(agent_count(&svc).await, agents, "nothing started");
}

/// An entry's `placement` keeps the closed wire shape: `null` is refused.
#[test]
fn a_batch_entry_placement_is_never_null() {
    let null = serde_json::from_value::<intent_core::BatchTaskOptions>(
        json!({"taskNoteId": "n1", "placement": null}),
    );
    assert!(null.is_err(), "{null:?}");
    let omitted =
        serde_json::from_value::<intent_core::BatchTaskOptions>(json!({"taskNoteId": "n1"}))
            .expect("omitted placement");
    assert!(omitted.placement.is_none());
}

/// A duplicated task id resolves its placement as the batch starts it: the
/// last object entry wins and a bare duplicate does not reset it, so the
/// `isolation` check never refuses a task for a placement it does not get.
#[tokio::test]
async fn a_duplicated_batch_task_is_checked_with_the_placement_it_starts_with() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    let parent = remote_agent(&svc, &ws).await;
    let first = seed_task(&svc, &ws, "First").await;
    let second = seed_task(&svc, &ws, "Second").await;
    let before = remote.needs.lock().unwrap().len();
    let shared = || Some("shared".to_string());

    let local_then_bare = batch_from(
        &svc,
        &ws,
        &parent,
        AgentDelegateInput {
            isolation: shared(),
            tasks: Some(vec![
                entry(json!({"taskNoteId": first.0, "placement": {"target": "local"}})),
                bare(&first),
            ]),
            ..Default::default()
        },
    )
    .await
    .expect("a bare duplicate keeps the local placement");
    assert_eq!(
        local_then_bare["summary"]["started"], 1,
        "{local_then_bare}"
    );

    let remote_then_plain = batch_from(
        &svc,
        &ws,
        &parent,
        AgentDelegateInput {
            placement: Some(placement(json!({"target": "local"}))),
            isolation: shared(),
            tasks: Some(vec![
                entry(json!({"taskNoteId": second.0, "placement": {"target": "remote"}})),
                entry(json!({"taskNoteId": second.0})),
            ]),
            ..Default::default()
        },
    )
    .await
    .expect("the last object entry drops the remote placement");
    assert_eq!(
        remote_then_plain["summary"]["started"], 1,
        "{remote_then_plain}"
    );

    assert_eq!(
        remote.needs.lock().unwrap().len(),
        before,
        "no machine asked"
    );
    for (result, task) in [(&local_then_bare, &first), (&remote_then_plain, &second)] {
        let agent = child(&svc, &ws, row(result, task)).await;
        assert!(agent.node_id.is_none(), "{agent:?}");
    }
}

/// Create an agent under `parent`, on the backend or at `placement`.
async fn spawn_under(
    svc: &Services,
    ws: &WorkspaceId,
    parent: Option<&AgentId>,
    placement_json: Option<Value>,
) -> AgentId {
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Child".into()),
            Some("sonnet4.5".into()),
            None,
            parent.cloned(),
            None,
            false,
            AgentCreateExtra {
                provider: Some("auggie".into()),
                placement: placement_json.map(placement),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    AgentId::from(created["agent"]["id"].as_str().unwrap())
}

async fn unsettled(svc: &Services, parent: &AgentId) -> u64 {
    svc.store
        .count_child_agents(parent)
        .await
        .unwrap()
        .unsettled
}

/// A parent stops, then deletes, sub-agents on the backend, on a remote
/// machine and on an E2B sandbox: every one gets its final stop where it
/// runs, a grandchild goes with its parent, and the parent's unsettled
/// count drops as each ends.
#[tokio::test]
async fn a_parent_stops_and_deletes_its_sub_agents_wherever_they_run() {
    let (_t, svc, ws) = setup().await;
    let svc = crate::test_support::fake_runtime::FakeRuntime::install(svc);
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    let parent = spawn_under(&svc, &ws, None, None).await;
    let local = spawn_under(&svc, &ws, Some(&parent), None).await;
    let machine = spawn_under(&svc, &ws, Some(&parent), Some(json!({"target": "remote"}))).await;
    let sandbox = spawn_under(&svc, &ws, Some(&parent), Some(json!({"target": "e2b"}))).await;
    let grandchild = spawn_under(&svc, &ws, Some(&machine), None).await;
    assert_eq!(unsettled(&svc, &parent).await, 3);

    let stopped = svc
        .agent_end_descendant_op(parent.clone(), machine.clone(), false, None)
        .await
        .unwrap();
    assert_eq!(stopped["ok"], true, "{stopped}");
    assert_eq!(
        stopped["stopped"],
        json!([machine.0, grandchild.0]),
        "{stopped}"
    );
    for id in [&machine, &grandchild] {
        let row = svc.store.get_agent_session_summary(id).await.unwrap();
        assert!(row.retired_at.is_some(), "{id} not retired");
    }
    assert_eq!(unsettled(&svc, &parent).await, 2);
    assert_eq!(unsettled(&svc, &machine).await, 0);

    let stopped = svc
        .agent_end_descendant_op(parent.clone(), local.clone(), false, Some("done".into()))
        .await
        .unwrap();
    assert_eq!(stopped["stopped"], json!([local.0]), "{stopped}");
    assert_eq!(unsettled(&svc, &parent).await, 1);

    let deleted = svc
        .agent_end_descendant_op(parent.clone(), sandbox.clone(), true, None)
        .await
        .unwrap();
    assert_eq!(deleted["deleted"], json!([sandbox.0]), "{deleted}");
    assert!(matches!(
        svc.store.get_agent_session_summary(&sandbox).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(unsettled(&svc, &parent).await, 0);

    // Deleting a stopped sub-agent removes it and its subtree.
    let deleted = svc
        .agent_end_descendant_op(parent.clone(), machine.clone(), true, None)
        .await
        .unwrap();
    assert_eq!(
        deleted["deleted"],
        json!([grandchild.0, machine.0]),
        "{deleted}"
    );

    let stops: Vec<AgentId> = remote
        .final_stops
        .lock()
        .unwrap()
        .iter()
        .map(|(id, _)| id.clone())
        .collect();
    for id in [&machine, &sandbox, &local, &grandchild] {
        assert!(stops.contains(id), "{id} got no final stop: {stops:?}");
    }
    // The parent itself is untouched.
    let row = svc.store.get_agent_session_summary(&parent).await.unwrap();
    assert!(row.retired_at.is_none());
}

/// Only a strict ancestor may end an agent: a sibling, a child's parent
/// seen from below, an unrelated agent and the caller itself are refused
/// before any effect.
#[tokio::test]
async fn only_an_ancestor_may_stop_or_delete_an_agent() {
    let (_t, svc, ws) = setup().await;
    let parent = spawn_under(&svc, &ws, None, None).await;
    let first = spawn_under(&svc, &ws, Some(&parent), None).await;
    let second = spawn_under(&svc, &ws, Some(&parent), None).await;
    let stranger = spawn_under(&svc, &ws, None, None).await;
    for (caller, target) in [
        (&first, &second),
        (&first, &parent),
        (&stranger, &first),
        (&parent, &parent),
    ] {
        for delete in [false, true] {
            let refused = svc
                .agent_end_descendant_op(caller.clone(), target.clone(), delete, None)
                .await;
            assert!(
                matches!(refused, Err(Error::InvalidParams(_))),
                "{caller} -> {target}: {refused:?}"
            );
        }
    }
    let missing = svc
        .agent_end_descendant_op(parent.clone(), AgentId::from("agent-missing"), true, None)
        .await;
    assert!(matches!(missing, Err(Error::NotFound(_))), "{missing:?}");
    for id in [&parent, &first, &second, &stranger] {
        let row = svc.store.get_agent_session_summary(id).await.unwrap();
        assert!(row.retired_at.is_none(), "{id} changed");
    }
    assert_eq!(unsettled(&svc, &parent).await, 2);
}

// --- Auto-pick on a signed-out machine ---------------------------------------

use intent_core::node_link::ProviderStatus;

async fn marked_profile(svc: &Services, base: &str, name: &str) -> String {
    let key = if base == "codex" {
        "OPENAI_API_KEY"
    } else {
        "ANTHROPIC_API_KEY"
    };
    daemon_profile(
        svc,
        base,
        name,
        json!([{ "key": key, "kind": "set", "value": "sk-remote-pick" }]),
    )
    .await
}

async fn unmarked_profile(svc: &Services, base: &str, name: &str) -> String {
    let reference = marked_profile(svc, base, name).await;
    let id = reference.split_once('@').unwrap().1.to_owned();
    intent_core::caller::with_caller(
        intent_core::caller::Caller::Daemon,
        svc.provider_profiles_update_impl(id, json!({ "remoteCompatible": false })),
    )
    .await
    .expect("unmark");
    reference
}

/// The plain Default on a machine signed out of the provider sends the
/// first profile marked for remote machines, in the saved profile order.
#[tokio::test]
async fn a_plain_default_on_a_signed_out_machine_sends_the_first_marked_profile() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote {
        signed_out: true,
        ..FakeRemote::default()
    });
    let svc = svc.with_remote_launch_port(remote.clone());
    expose_node(
        &svc,
        vec![offered("claude-code", ProviderStatus::SignedOut, &["opus"])],
    )
    .await;
    unmarked_profile(&svc, "claude-code", "Personal").await;
    let first = marked_profile(&svc, "claude-code", "Work").await;
    marked_profile(&svc, "claude-code", "Team").await;
    let (view, picked) = svc
        .admit_remote_agent(
            &ws,
            "claude-code",
            None,
            &placement(json!({"target": "remote"})),
            false,
        )
        .await
        .expect("a marked profile is sent instead");
    assert_eq!(view.node_id, "node-1");
    let picked = picked.expect("a profile was picked");
    assert_eq!(picked.provider_ref, first);
    assert_eq!(picked.reason, "machine-signed-out");
    assert_eq!(picked.machine, "Mac Studio");
    // The machine is chosen first, with signed-out machines kept as
    // candidates; the profile is picked for the machine that was chosen.
    let needs = remote.needs.lock().unwrap().clone();
    assert_eq!(needs.len(), 1, "{needs:?}");
    assert!(needs[0].machine_sign_in && needs[0].profile_fallback);
}

#[tokio::test]
async fn a_plain_default_with_no_marked_profile_is_machine_not_signed_in() {
    let (_t, svc, ws) = setup().await;
    let svc = svc.with_remote_launch_port(Arc::new(FakeRemote {
        signed_out: true,
        ..FakeRemote::default()
    }));
    unmarked_profile(&svc, "claude-code", "Personal").await;
    marked_profile(&svc, "codex", "Other provider").await;
    let result = svc
        .admit_remote_agent(
            &ws,
            "claude-code",
            None,
            &placement(json!({"target": "remote"})),
            false,
        )
        .await;
    let Err(Error::RemoteMachine(failure)) = &result else {
        panic!("expected machine-not-signed-in, got {result:?}");
    };
    assert_eq!(failure.data["code"], "machine-not-signed-in");
}

/// A profile picked explicitly is never replaced, even by a marked one.
#[tokio::test]
async fn an_explicit_profile_is_never_swapped() {
    let (_t, svc, ws) = setup().await;
    let svc = svc.with_remote_launch_port(Arc::new(FakeRemote {
        signed_out: true,
        ..FakeRemote::default()
    }));
    let unmarked = unmarked_profile(&svc, "claude-code", "Personal").await;
    marked_profile(&svc, "claude-code", "Work").await;
    let result = svc
        .admit_remote_agent(
            &ws,
            &unmarked,
            None,
            &placement(json!({"target": "remote"})),
            false,
        )
        .await;
    let Err(Error::ProviderProfile(failure)) = &result else {
        panic!("expected profile-not-remote-compatible, got {result:?}");
    };
    assert_eq!(failure.data["reason"], "not-marked");
}

/// A machine whose sign-in is unknown (or signed in) runs the plain Default
/// on its own sign-in; nothing is picked.
#[tokio::test]
async fn an_unknown_sign_in_tries_the_machines_own_sign_in() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    expose_node(
        &svc,
        vec![offered("claude-code", ProviderStatus::Unknown, &["opus"])],
    )
    .await;
    marked_profile(&svc, "claude-code", "Work").await;
    let (_view, picked) = svc
        .admit_remote_agent(
            &ws,
            "claude-code",
            None,
            &placement(json!({"target": "remote"})),
            false,
        )
        .await
        .unwrap();
    assert!(picked.is_none());
    let needs = remote.needs.lock().unwrap().clone();
    assert_eq!(needs.len(), 1);
    assert!(needs[0].machine_sign_in);
}

/// E2B has no sign-in: the plain Default sends the first marked profile
/// that sends a sign-in, instead of being refused.
#[tokio::test]
async fn a_plain_default_on_e2b_sends_the_first_marked_profile() {
    let (_t, svc, ws) = setup().await;
    let remote = Arc::new(FakeRemote::default());
    let svc = svc.with_remote_launch_port(remote.clone());
    // Marked but sends nothing: a sandbox cannot use it.
    daemon_profile(
        &svc,
        "claude-code",
        "Empty",
        json!([{ "key": "ANTHROPIC_BASE_URL", "kind": "unset" }]),
    )
    .await;
    let token = marked_profile(&svc, "claude-code", "Token").await;
    let (_view, picked) = svc
        .admit_remote_agent(
            &ws,
            "claude-code",
            None,
            &placement(json!({"target": "e2b"})),
            false,
        )
        .await
        .expect("a marked token profile is sent");
    let picked = picked.expect("picked");
    assert_eq!(picked.provider_ref, token);
    assert_eq!(picked.reason, "sandbox-no-sign-in");

    // With no marked profile the refusal stays.
    let (_t2, svc2, ws2) = setup().await;
    let svc2 = svc2.with_remote_launch_port(Arc::new(FakeRemote::default()));
    let result = svc2
        .admit_remote_agent(
            &ws2,
            "claude-code",
            None,
            &placement(json!({"target": "e2b"})),
            false,
        )
        .await;
    assert!(result.is_err(), "{result:?}");
}

/// The created agent runs on the picked profile, and its transcript says so
/// the way a failover switch is reported.
#[tokio::test]
async fn the_agent_shows_which_profile_ran() {
    let (_t, svc, ws) = setup().await;
    let svc = svc.with_remote_launch_port(Arc::new(FakeRemote {
        signed_out: true,
        ..FakeRemote::default()
    }));
    expose_node(
        &svc,
        vec![offered("claude-code", ProviderStatus::SignedOut, &["opus"])],
    )
    .await;
    let work = marked_profile(&svc, "claude-code", "Work").await;
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some("Remote".into()),
            Some("opus".into()),
            None,
            None,
            None,
            false,
            AgentCreateExtra {
                provider: Some("claude-code".into()),
                placement: Some(placement(json!({"target": "remote"}))),
                ..Default::default()
            },
        )
        .await
        .expect("created on the picked profile");
    assert_eq!(created["agent"]["provider"], work.as_str());
    let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let messages = svc.store.get_agent_messages(&id, None).await.unwrap();
    let notice = messages
        .iter()
        .find_map(|m| {
            m.metadata
                .as_ref()
                .filter(|meta| meta["type"] == "provider_auto_selected")
                .cloned()
        })
        .expect("an auto-selection notice");
    assert_eq!(notice["fromProvider"], "claude-code");
    assert_eq!(notice["toProvider"], work.as_str());
    assert_eq!(notice["reason"], "machine-signed-out");
    assert_eq!(notice["machine"], "Mac Studio");
}
