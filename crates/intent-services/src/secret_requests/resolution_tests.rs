//! Durable secret-request resolutions (PROTOCOL §5.56): answering a request
//! records `requestId -> "provided" | "declined"` in session metadata,
//! served on `agent.get` (not list rows) and pushed in `agent:updated`, so a
//! client can settle the card without finding the notice row.

use intent_core::events::AGENT_UPDATED;
use intent_core::{now_iso, AgentId, WorkspaceId};
use intent_store::Store;
use serde_json::{json, Value};
use tokio::time::{timeout, Duration};
use zeroize::Zeroizing;

use super::{Outcome, MAX_SECRET_REQUEST_RESOLUTIONS};
use crate::test_support::fixtures::{session, workspace, TempDb};
use crate::{EventBus, Services, SubscriptionFilter};

async fn setup() -> (TempDb, Services, WorkspaceId, AgentId, EventBus) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let agent = AgentId::from("agent-secret-resolutions");
    store
        .insert_agent_session(&session(&agent, &ws))
        .await
        .expect("session");
    let bus = EventBus::new(store.clone());
    let svc = Services::new_isolated(store).with_event_bus(bus.clone());
    (tmp, svc, ws, agent, bus)
}

async fn request(svc: &Services, ws: &WorkspaceId, agent: &AgentId, name: &str) -> String {
    let ticket = svc
        .secret_request_op(ws.clone(), agent.clone(), name.into(), "tests".into())
        .await
        .expect("request");
    ticket["requestId"].as_str().expect("requestId").to_string()
}

async fn resolutions(svc: &Services, agent: &AgentId) -> Value {
    let session = svc
        .store
        .get_agent_session_summary(agent)
        .await
        .expect("session");
    Value::Object(session.secret_request_resolutions())
}

#[intent_test_macros::daemon_test]
async fn answers_persist_and_converge_through_agent_updated() {
    let (_tmp, svc, ws, agent, bus) = setup().await;
    let declined = request(&svc, &ws, &agent, "NAMECHEAP_USERNAME").await;
    let provided = request(&svc, &ws, &agent, "NAMECHEAP_API_KEY").await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_UPDATED.to_string()],
        ..Default::default()
    });

    svc.secret_answer_op(ws.clone(), declined.clone(), Outcome::Declined, None)
        .await
        .expect("decline");
    assert_eq!(
        resolutions(&svc, &agent).await,
        json!({ declined.clone(): "declined" })
    );
    let event = loop {
        let batch = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(2)),
            sub.recv(),
        )
        .await
        .expect("agent:updated timed out")
        .expect("subscription closed");
        if let Some(event) = batch
            .into_iter()
            .find(|e| e.data.get("secretRequestResolutions").is_some())
        {
            break event;
        }
    };
    assert_eq!(event.data["agentId"], json!(agent.0));
    assert_eq!(
        event.data["secretRequestResolutions"],
        json!({ declined.clone(): "declined" })
    );

    svc.secret_answer_op(
        ws.clone(),
        provided.clone(),
        Outcome::Provided,
        Some(Zeroizing::new("value-never-persisted".into())),
    )
    .await
    .expect("provide");
    let both = json!({ declined.clone(): "declined", provided.clone(): "provided" });
    assert_eq!(resolutions(&svc, &agent).await, both);
    let raw = svc
        .store
        .get_agent_session_summary(&agent)
        .await
        .expect("session")
        .metadata
        .clone()
        .expect("metadata");
    assert!(
        !raw.to_string().contains("value-never-persisted"),
        "only the outcome is stored, never the value"
    );

    let lite = svc
        .agent_get_op(agent.clone(), Some(ws.clone()))
        .await
        .expect("agent.get");
    assert_eq!(
        Value::Object(lite.metadata.secret_request_resolutions.clone()),
        both
    );
    let row = svc
        .agent_list_op(ws.clone())
        .await
        .expect("agent.list")
        .into_iter()
        .find(|row| row.id == agent)
        .expect("list row");
    assert!(
        row.metadata.secret_request_resolutions.is_empty(),
        "detail-only: list rows omit the map"
    );
}

/// The map is bounded: past the cap the oldest entries go first.
#[intent_test_macros::daemon_test]
async fn resolutions_evict_the_oldest_past_the_cap() {
    let (_tmp, svc, ws, agent, _bus) = setup().await;
    let seeded: serde_json::Map<String, Value> = (0..MAX_SECRET_REQUEST_RESOLUTIONS)
        .map(|n| (format!("sec-old-{n}"), json!("declined")))
        .collect();
    svc.store
        .set_agent_session_metadata_key_json(
            &ws,
            &agent,
            intent_core::SECRET_REQUEST_RESOLUTIONS_KEY,
            &serde_json::to_string(&seeded).unwrap(),
            &now_iso(),
        )
        .await
        .expect("seed");
    let fresh = request(&svc, &ws, &agent, "KEY").await;
    svc.secret_answer_op(ws.clone(), fresh.clone(), Outcome::Declined, None)
        .await
        .expect("decline");
    let map = resolutions(&svc, &agent).await;
    let map = map.as_object().unwrap();
    assert_eq!(map.len(), MAX_SECRET_REQUEST_RESOLUTIONS);
    assert!(!map.contains_key("sec-old-0"), "oldest evicted");
    assert!(map.contains_key("sec-old-1"));
    assert_eq!(map.get(&fresh), Some(&json!("declined")));
}

/// Seeded metadata cannot smuggle an unknown outcome into the projection.
#[intent_test_macros::daemon_test]
async fn reader_keeps_only_known_outcomes() {
    let (_tmp, svc, ws, agent, _bus) = setup().await;
    svc.store
        .set_agent_session_metadata_key_json(
            &ws,
            &agent,
            intent_core::SECRET_REQUEST_RESOLUTIONS_KEY,
            &json!({ "a": "provided", "b": "declined", "c": "expired", "": "declined", "d": 1 })
                .to_string(),
            &now_iso(),
        )
        .await
        .expect("seed");
    assert_eq!(
        resolutions(&svc, &agent).await,
        json!({ "a": "provided", "b": "declined" })
    );
}
