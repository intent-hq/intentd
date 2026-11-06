use std::time::Duration;

use intent_core::link_traffic::{LinkTap, PeerKind, TrafficDirection, TrafficPeer};
use intent_core::node_link::LinkFrame;
use intent_core::nodes::NodeCounter;
use intent_core::{with_caller, AgentId, Caller, Error, HostRole, PrincipalId};
use intent_store::Store;
use serde_json::{json, Value};

use crate::tests::TempDb;
use crate::{EventBus, Services, SubscriptionFilter};

struct Fixture {
    _tmp: TempDb,
    store: Store,
    services: Services,
    bus: EventBus,
    member: PrincipalId,
}

async fn fixture(node_mode: bool) -> Fixture {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let member = intent_core::Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: Some(77),
        login: Some("member".into()),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: intent_core::now_iso(),
        updated_at: intent_core::now_iso(),
    };
    store.upsert_principal(&member).await.expect("member");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_node_mode(node_mode)
        .with_event_bus(bus.clone());
    Fixture {
        _tmp: tmp,
        store,
        services,
        bus,
        member: member.id,
    }
}

async fn owner(f: &Fixture) -> Caller {
    Caller::Wire {
        principal_id: f.store.get_primary_principal().await.unwrap().id,
        host_role: HostRole::Owner,
    }
}

async fn op(f: &Fixture, method: &'static str, params: Value) -> intent_core::Result<Value> {
    with_caller(
        owner(f).await,
        f.services.remote_link_traffic_op(method, params),
    )
    .await
}

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(5))
}

#[tokio::test]
async fn only_the_owner_reaches_link_traffic() {
    let f = fixture(false).await;
    let refused = [
        Caller::Wire {
            principal_id: f.member.clone(),
            host_role: HostRole::Member,
        },
        Caller::Wire {
            principal_id: f.member.clone(),
            host_role: HostRole::Guest,
        },
        Caller::Agent {
            agent_id: AgentId::new(),
        },
        Caller::Wire {
            principal_id: f.member.clone(),
            host_role: HostRole::Owner,
        },
    ];
    for caller in refused {
        for method in ["start", "stop", "snapshot"] {
            let result = with_caller(
                caller.clone(),
                f.services.remote_link_traffic_op(method, json!({})),
            )
            .await;
            assert!(
                matches!(result, Err(Error::Forbidden(_))),
                "{caller:?} {method}: {result:?}"
            );
        }
    }
    assert!(!f.services.link_traffic().is_active(), "nothing started");
}

#[tokio::test]
async fn node_mode_refuses_link_traffic() {
    let f = fixture(true).await;
    for method in ["start", "stop", "snapshot"] {
        let result = op(&f, method, json!({})).await;
        assert!(
            matches!(result, Err(Error::NodeMode(_))),
            "{method}: {result:?}"
        );
    }
    assert!(!f.services.link_traffic().is_active());
}

#[tokio::test]
async fn start_snapshot_stop_round_trip() {
    let f = fixture(false).await;
    let off = op(&f, "snapshot", json!({})).await.unwrap();
    assert_eq!(off["capture"]["active"], false);
    assert_eq!(off["records"], json!([]));

    let started = op(&f, "start", json!({})).await.unwrap();
    assert_eq!(started["capture"]["active"], true);
    assert_eq!(started["capture"]["payloads"], true);
    let tap = LinkTap::new(
        f.services.link_traffic().clone(),
        TrafficPeer {
            kind: PeerKind::Machine,
            id: "m-1".into(),
            name: None,
        },
    );
    for seq in 1..=3 {
        tap.observe(
            TrafficDirection::In,
            &LinkFrame::Ack {
                seq: NodeCounter(seq),
            },
            20,
        );
    }
    let deadline = tokio::time::Instant::now() + wait();
    while op(&f, "snapshot", json!({})).await.unwrap()["capture"]["retained"] != 3 {
        assert!(tokio::time::Instant::now() < deadline, "records never kept");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let page = op(&f, "snapshot", json!({ "afterSeq": 1, "limit": 1 }))
        .await
        .unwrap();
    assert_eq!(page["records"].as_array().unwrap().len(), 1);
    assert_eq!(page["records"][0]["seq"], 2);
    assert_eq!(page["records"][0]["frameType"], "ack");

    let stopped = op(&f, "stop", json!({})).await.unwrap();
    assert_eq!(stopped["capture"]["active"], false);
    assert_eq!(stopped["capture"]["retained"], 0);
    assert_eq!(
        op(&f, "snapshot", json!({})).await.unwrap()["records"],
        json!([])
    );
}

#[tokio::test]
async fn bad_params_are_invalid() {
    let f = fixture(false).await;
    for (method, params) in [
        ("start", json!({ "payloads": "yes" })),
        ("snapshot", json!({ "limit": 0 })),
        ("snapshot", json!({ "limit": 2001 })),
        ("snapshot", json!({ "limit": -1 })),
        ("snapshot", json!({ "afterSeq": "1" })),
    ] {
        let result = op(&f, method, params.clone()).await;
        assert!(
            matches!(result, Err(Error::InvalidParams(_))),
            "{method} {params}: {result:?}"
        );
    }
    assert!(!f.services.link_traffic().is_active());
    let max = op(&f, "snapshot", json!({ "limit": 2000, "afterSeq": 0 })).await;
    assert!(max.is_ok(), "{max:?}");
}

#[tokio::test]
async fn records_event_is_global_owner_only_and_announces_start_and_stop() {
    let f = fixture(false).await;
    let mut sub = f.bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::REMOTE_LINK_TRAFFIC_RECORDS.to_owned()],
        ..SubscriptionFilter::default()
    });
    op(&f, "start", json!({ "payloads": false })).await.unwrap();
    let batch = tokio::time::timeout(wait(), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch[0].data["records"], json!([]));
    assert_eq!(batch[0].data["capture"]["active"], true);
    assert_eq!(batch[0].data["capture"]["payloads"], false);
    assert_eq!(batch[0].workspace_id.as_str(), "", "a global event");
    op(&f, "stop", json!({})).await.unwrap();
    let batch = tokio::time::timeout(wait(), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch[0].data["capture"]["active"], false);
    assert!(!intent_core::events::is_collaborator_event_type(
        intent_core::events::REMOTE_LINK_TRAFFIC_RECORDS
    ));
    assert!(!intent_core::events::is_member_execution_event_type(
        intent_core::events::REMOTE_LINK_TRAFFIC_RECORDS
    ));
    // Transient: nothing reaches the durable log.
    let persisted = f
        .store
        .query_events(&intent_store::EventQuery {
            event_types: vec![intent_core::events::REMOTE_LINK_TRAFFIC_RECORDS.to_owned()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(persisted.is_empty());
}
