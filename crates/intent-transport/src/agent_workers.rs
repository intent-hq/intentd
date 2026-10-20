//! `agent.workers.subscribe` fast path (PROTOCOL §5.5b).
//!
//! A per-connection channel of complete `WorkerSnapshot` replacements: an ack,
//! a seq-0 snapshot, then one snapshot per change of the selected agent's
//! projection. The connection task (`conn.rs`) owns the registry and the
//! forwarder; this module classifies the request and owns its bounds.
//! Teardown is the shared `events.unsubscribe`.

use serde_json::{Map, Value};

use crate::events::IdInfo;

/// Worker subscriptions one connection may hold at once (§5.5b).
pub(crate) const MAX_WORKER_SUBSCRIPTIONS: usize = 8;

/// A classified `agent.workers.subscribe` request.
pub(crate) struct WorkersSubscribe {
    pub id: IdInfo,
    pub params: Map<String, Value>,
}

/// Classify a parsed frame. Same envelope pre-check as [`crate::events::classify`]:
/// a malformed envelope falls through to the dispatcher's `-32600`.
pub(crate) fn classify(value: &Value) -> Option<WorkersSubscribe> {
    let obj = value.as_object()?;
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return None;
    }
    let id_member = obj.get("id");
    if let Some(v) = id_member {
        if !v.is_null() && !v.is_string() && !v.is_number() {
            return None;
        }
    }
    match obj.get("method").and_then(Value::as_str)? {
        "agent.workers.subscribe" => Some(WorkersSubscribe {
            id: IdInfo {
                present: id_member.is_some(),
                echo: id_member.cloned().unwrap_or(Value::Null),
            },
            params: obj
                .get("params")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn classifies_only_well_formed_subscribe_requests() {
        let ok = classify(
            &json!({ "jsonrpc": "2.0", "id": 7, "method": "agent.workers.subscribe",
                                   "params": { "workspaceId": "w", "agentId": "a" } }),
        )
        .expect("classified");
        assert_eq!(ok.id.echo, json!(7));
        assert_eq!(ok.params["agentId"], "a");
        let no_params =
            classify(&json!({ "jsonrpc": "2.0", "id": "x", "method": "agent.workers.subscribe" }))
                .expect("classified");
        assert!(no_params.params.is_empty());
        for frame in [
            json!({ "jsonrpc": "1.0", "id": 1, "method": "agent.workers.subscribe" }),
            json!({ "jsonrpc": "2.0", "id": [], "method": "agent.workers.subscribe" }),
            json!({ "jsonrpc": "2.0", "id": 1, "method": "agent.workers.list" }),
            json!("agent.workers.subscribe"),
        ] {
            assert!(classify(&frame).is_none(), "{frame}");
        }
    }
}

#[cfg(test)]
mod channel_tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::future::BoxFuture;
    use intent_core::events::{AGENT_WORKERS_CHANGED, WORKSPACE_UPDATED};
    use intent_core::{
        ActorType, AgentId, Caller, Error, EventActor, PrincipalId, WorkspaceApi, WorkspaceId,
    };
    use intent_services::EventBus;
    use intent_store::{NewEvent, Store};
    use serde_json::{json, Value};

    use crate::conn::{handle_workers_subscribe, outbound_channel, ConnSubs};

    /// `agent.workers.list` stand-in: a snapshot for member workspaces,
    /// `NotFound` once membership is revoked (the service-layer shape).
    struct WorkersApi {
        members: Arc<Mutex<HashSet<String>>>,
    }

    impl WorkspaceApi for WorkersApi {
        fn agent_workers_list(
            &self,
            workspace_id: WorkspaceId,
            agent_id: AgentId,
        ) -> BoxFuture<'_, intent_core::Result<Value>> {
            let allowed = self.members.lock().unwrap().contains(workspace_id.as_str());
            Box::pin(async move {
                if allowed {
                    Ok(json!({ "workspaceId": workspace_id, "agentId": agent_id, "workers": [] }))
                } else {
                    Err(Error::NotFound(format!("agent {agent_id}")))
                }
            })
        }
    }

    fn event(event_type: &str, data: Value) -> NewEvent {
        NewEvent {
            workspace_id: WorkspaceId::from("ws-1"),
            timestamp: intent_core::now_iso(),
            event_type: event_type.to_string(),
            actor: EventActor {
                actor_type: ActorType::System,
                id: Some("system".to_string()),
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        }
    }

    async fn finished(subs: &ConnSubs, id: &str) -> bool {
        for _ in 0..100 {
            if subs.forwarder_finished(id) == Some(true) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    /// §5.5b: ack, seq-0, a push per change of the selected agent only; a
    /// guest's membership revocation ends the stream even while the workers
    /// are quiet, and the ended forwarder no longer holds a capacity slot.
    #[tokio::test]
    async fn channel_pushes_changes_and_ends_on_revocation_releasing_its_slot() {
        let dir = tempfile::Builder::new()
            .prefix("intent-workers-channel-")
            .tempdir()
            .unwrap();
        let bus = EventBus::new(Store::open(&dir.path().join("bus.db")).await.unwrap());
        let members = Arc::new(Mutex::new(HashSet::from(["ws-1".to_string()])));
        let api: Arc<dyn WorkspaceApi> = Arc::new(WorkersApi {
            members: Arc::clone(&members),
        });
        let (out_tx, mut rx) = outbound_channel();
        let mut subs = ConnSubs::default();
        let guest = Caller::Wire {
            principal_id: PrincipalId::new(),
            host_role: intent_core::HostRole::Guest,
        };
        let subscribe = |id: i64| {
            super::classify(
                &json!({ "jsonrpc": "2.0", "id": id, "method": "agent.workers.subscribe",
                                     "params": { "workspaceId": "ws-1", "agentId": "agent-A" } }),
            )
            .unwrap()
        };
        let (bus_ref, out_ref, api_ref) = (&bus, &out_tx, &api);
        let subs_ref = &mut subs;
        let ok = crate::context::with_request_context(true, Some(guest.clone()), async move {
            handle_workers_subscribe(subscribe(1), api_ref, bus_ref, out_ref, subs_ref).await
        })
        .await;
        assert!(ok);
        let ack: Value = serde_json::from_str(&rx.priority.recv().await.unwrap()).unwrap();
        let sub_id = ack["result"]["subscriptionId"]
            .as_str()
            .unwrap()
            .to_string();
        let push = |frame: crate::OutboundFrame| -> Value { serde_json::from_str(&frame).unwrap() };
        let first = push(rx.bulk.recv().await.unwrap());
        assert_eq!(first["params"]["seq"], 0);
        assert_eq!(first["params"]["kind"], "snapshot");

        let _ = bus.publish_transient(&event(
            AGENT_WORKERS_CHANGED,
            json!({ "agentId": "agent-B" }),
        ));
        let _ = bus.publish_transient(&event(
            AGENT_WORKERS_CHANGED,
            json!({ "agentId": "agent-A" }),
        ));
        let second = push(rx.bulk.recv().await.unwrap());
        assert_eq!(
            second["params"]["seq"], 1,
            "only the selected agent's change pushes"
        );
        assert_eq!(second["params"]["subscriptionId"], json!(sub_id));

        members.lock().unwrap().remove("ws-1");
        bus.publish(&event(
            WORKSPACE_UPDATED,
            json!({ "changes": { "members": true } }),
        ))
        .await
        .unwrap();
        assert!(
            finished(&subs, &sub_id).await,
            "revocation ends a quiet stream"
        );
        let _ = bus.publish_transient(&event(
            AGENT_WORKERS_CHANGED,
            json!({ "agentId": "agent-A" }),
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(300), rx.bulk.recv())
                .await
                .is_err(),
            "no push after revocation"
        );

        members.lock().unwrap().insert("ws-1".into());
        for n in 0..super::MAX_WORKER_SUBSCRIPTIONS {
            let (bus_ref, out_ref, api_ref, subs_ref) = (&bus, &out_tx, &api, &mut subs);
            let req = subscribe(10 + i64::try_from(n).unwrap());
            crate::context::with_request_context(true, Some(guest.clone()), async move {
                handle_workers_subscribe(req, api_ref, bus_ref, out_ref, subs_ref).await
            })
            .await;
            let reply: Value = serde_json::from_str(&rx.priority.recv().await.unwrap()).unwrap();
            assert!(
                reply["result"]["subscriptionId"].is_string(),
                "slot {n} free: {reply}"
            );
        }
        drop(subs);
    }
}
