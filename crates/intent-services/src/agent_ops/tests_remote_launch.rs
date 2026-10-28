//! Creating agents with a remote `placement`: admission before any effect,
//! the daemon-owned placement on the created agent, its projection, and
//! the create seams that carry or refuse a placement.

use std::sync::{Arc, Mutex};

use intent_core::placement::{AgentPlacement, PlatformAvailability};
use intent_core::remote_launch::{
    EffectiveIsolation, RemoteLaunchRequest, RemotePlacementView, REMOTE_PLACEMENT_METADATA_KEY,
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
    fail_launch: bool,
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
    }
}

impl RemoteLaunchPort for FakeRemote {
    fn placements<'a>(
        &'a self,
        _svc: &'a Services,
    ) -> BoxFuture<'a, intent_core::Result<PlatformAvailability>> {
        Box::pin(async { Ok(PlatformAvailability::none(intent_core::now_iso())) })
    }

    fn placement_help<'a>(&'a self, _svc: &'a Services) -> BoxFuture<'a, Option<String>> {
        Box::pin(async { Some("Remote placement: node \"Mac Studio\"".into()) })
    }

    fn admit<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a WorkspaceId,
        placement: &'a AgentPlacement,
    ) -> BoxFuture<'a, intent_core::Result<RemotePlacementView>> {
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
    let listed = svc.agent_list_op(ws.clone()).await.unwrap();
    let row = listed.iter().find(|a| a.id == id).unwrap();
    assert_eq!(row.node_id.as_deref(), Some("node-1"));
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
    let session = svc.store.get_agent_session(&id).await.unwrap();
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
        (json!({"target": "local"}), "not \"local\""),
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
    // A saved provider profile cannot be sent to a node: refused, never run
    // on the node's own login instead.
    let result = svc
        .admit_remote_agent(
            &ws,
            "claude-code@work",
            &placement(json!({"target": "remote"})),
        )
        .await;
    assert!(
        matches!(&result, Err(Error::InvalidParams(m)) if m.starts_with("remote-auth-unsupported")),
        "{result:?}"
    );
    assert!(remote.launched.lock().unwrap().is_empty());
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
async fn delegate_takes_a_placement_only_for_a_single_task_without_isolation() {
    let (_t, svc, ws) = setup().await;
    let svc = svc.with_remote_launch_port(Arc::new(FakeRemote::default()));
    let remote = || Some(placement(json!({"target": "remote"})));
    let batch = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                placement: remote(),
                tasks: Some(Vec::new()),
                ..Default::default()
            },
            None,
        )
        .await;
    assert!(
        matches!(&batch, Err(Error::InvalidParams(m)) if m.contains("single delegation")),
        "{batch:?}"
    );
    let cow = svc
        .agent_delegate_op(
            ws.clone(),
            AgentDelegateInput {
                placement: remote(),
                isolation: Some("cow".into()),
                task_text: Some("Do it".into()),
                ..Default::default()
            },
            None,
        )
        .await;
    assert!(
        matches!(&cow, Err(Error::InvalidParams(m)) if m.contains("cannot be combined")),
        "{cow:?}"
    );
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
        let session = svc.store.get_agent_session(&id).await.unwrap();
        assert!(RemotePlacementView::from_metadata(session.metadata.as_ref()).is_some());
        assert_eq!(
            session.metadata.unwrap()["priorNonObjectMetadata"],
            metadata
        );
    }
}

/// Review of intentd#2779: a remote agent's model cannot be switched (the
/// node keeps running the launched identity), so the switch is refused.
#[tokio::test]
async fn a_remote_agents_model_change_is_refused() {
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
    let result = svc
        .agent_set_model_op(id.clone(), "opus".into(), None)
        .await;
    assert!(
        matches!(&result, Err(Error::InvalidParams(m)) if m.starts_with("remote-model-change-unsupported")),
        "{result:?}"
    );
    let session = svc.store.get_agent_session(&id).await.unwrap();
    assert_eq!(session.model.as_deref(), Some("sonnet4.5"));
}
