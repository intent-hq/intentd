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
