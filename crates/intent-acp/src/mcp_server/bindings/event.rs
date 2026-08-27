//! `ws.event.*` bindings (WSAPI-4).
//!
//! Thin JS wrappers around `host({ method, args })` that route to the shared
//! [`WorkspaceApi`] event surface (§5.10). The subscribe wildcard `"*"` is
//! passed through to the daemon, which expands it per-subscriber: agent
//! callers get the non-agent categories only (monorepo#1229 — agent events
//! are watched via `ws.agent.watch`, not the event bus).

use std::sync::Arc;

use intent_core::{AgentId, EventQueryParams, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_bool, opt_str, opt_vec_str, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.event = {
        agentActivity: (agentId, minutesAgo) =>
            host({ method: 'event.agentActivity', args: { agentId, minutesAgo } }),
        workspaceSummary: (minutesAgo) =>
            host({ method: 'event.workspaceSummary', args: { minutesAgo } }),
        query: (options) => host({ method: 'event.query', args: { ...(options || {}) } }),
        subscribe: (eventTypes, opts) =>
            host({ method: 'event.subscribe', args: { eventTypes, ...(opts || {}) } }),
        unsubscribe: (subscriptionId) =>
            host({ method: 'event.unsubscribe', args: { subscriptionId } }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "agentActivity" => agent_activity(api, ws, args).await,
        "workspaceSummary" => workspace_summary(api, ws, args).await,
        "query" => query(api, ws, args).await,
        "subscribe" => subscribe(api, ws, caller, args).await,
        "unsubscribe" => unsubscribe(api, ws, args).await,
        other => Err(format!("host: unknown method `event.{other}`")),
    }
}

async fn agent_activity(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let agent_id = opt_str(args, "agentId");
    let minutes_ago = args.get("minutesAgo").and_then(Value::as_i64);
    // Binding-local (monorepo#3647): with `agentId`, serve ALL of that
    // agent's persisted events in the window via the generic query — the
    // wire method's agentId arm selects `file:changed` rows only, which
    // harness-side edits never produce, so a live tool-heavy turn read as
    // zero events here. `agent:tool:call` rows are persisted per call
    // MID-turn, so this now shows advancing activity while a turn runs.
    // The wire `event.agentActivity` contract is untouched.
    if let Some(agent_id) = agent_id {
        return api
            .event_query(
                ws.clone(),
                EventQueryParams {
                    actor_type: Some("agent".to_string()),
                    actor_id: Some(agent_id),
                    minutes_ago: minutes_ago.or(Some(30)),
                    ..Default::default()
                },
            )
            .await
            .map_err(map_err);
    }
    api.event_agent_activity(ws.clone(), None, minutes_ago)
        .await
        .map_err(map_err)
}

async fn workspace_summary(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let minutes_ago = args.get("minutesAgo").and_then(Value::as_i64);
    let summary = api
        .event_workspace_summary(ws.clone(), minutes_ago)
        .await
        .map_err(map_err)?;
    serde_json::to_value(summary).map_err(|e| e.to_string())
}

async fn query(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let params = EventQueryParams {
        event_type: opt_str(args, "eventType"),
        actor_type: opt_str(args, "actorType"),
        actor_id: opt_str(args, "actorId"),
        path: opt_str(args, "path"),
        minutes_ago: args.get("minutesAgo").and_then(Value::as_i64),
        limit: args.get("limit").and_then(Value::as_i64),
        paginate: opt_bool(args, "paginate"),
        page_token: opt_str(args, "pageToken"),
    };
    api.event_query(ws.clone(), params).await.map_err(map_err)
}

async fn subscribe(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let event_types = opt_vec_str(args, "eventTypes").ok_or_else(|| {
        "eventTypes is required. Specify category wildcards like \"file:*\", \"task:*\" or specific types like \"file:changed\". Agent events are not subscribable — use ws.agent.watch(agentId) instead.".to_string()
    })?;
    if event_types.is_empty() {
        return Err(
            "eventTypes is required. Specify category wildcards like \"file:*\", \"task:*\" or specific types like \"file:changed\". Agent events are not subscribable — use ws.agent.watch(agentId) instead.".to_string(),
        );
    }
    let r = api
        .event_subscribe(
            ws.clone(),
            caller.cloned(),
            event_types,
            opt_bool(args, "excludeSelf"),
            args.get("batchWindow").and_then(Value::as_i64),
        )
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

async fn unsubscribe(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let subscription_id =
        req_str(args, "subscriptionId").map_err(|_| "subscriptionId is required".to_string())?;
    let r = api
        .event_unsubscribe(ws.clone(), subscription_id)
        .await
        .map_err(map_err)?;
    serde_json::to_value(r).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{BoxFuture, Result};
    use serde_json::json;
    use std::sync::Mutex;

    type QueryCall = (Option<String>, Option<String>, Option<i64>);

    /// Records which API surface `event.agentActivity` routes through.
    #[derive(Default)]
    struct FakeApi {
        query_calls: Mutex<Vec<QueryCall>>,
        activity_calls: Mutex<Vec<(Option<String>, Option<i64>)>>,
    }

    impl WorkspaceApi for FakeApi {
        fn event_query(
            &self,
            _workspace_id: WorkspaceId,
            params: EventQueryParams,
        ) -> BoxFuture<'_, Result<Value>> {
            self.query_calls.lock().unwrap().push((
                params.actor_type.clone(),
                params.actor_id.clone(),
                params.minutes_ago,
            ));
            Box::pin(async move { Ok(json!([{ "eventType": "agent:tool:call" }])) })
        }

        fn event_agent_activity(
            &self,
            _workspace_id: WorkspaceId,
            agent_id: Option<String>,
            minutes_ago: Option<i64>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.activity_calls
                .lock()
                .unwrap()
                .push((agent_id, minutes_ago));
            Box::pin(async move { Ok(json!([])) })
        }
    }

    /// monorepo#3647: the `agentId` arm routes through the generic event
    /// query (all event types for that actor, default 30-minute window)
    /// instead of the wire method's `file:changed`-only arm, so mid-turn
    /// tool-call events surface as liveness.
    #[tokio::test]
    async fn agent_activity_with_agent_id_uses_generic_query() {
        let fake = Arc::new(FakeApi::default());
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        let ws = WorkspaceId::from("ws-1");

        let out = agent_activity(&api, &ws, &json!({ "agentId": "agent-1" }))
            .await
            .expect("dispatch");
        assert_eq!(out[0]["eventType"], "agent:tool:call");
        assert_eq!(
            fake.query_calls.lock().unwrap().as_slice(),
            &[(
                Some("agent".to_string()),
                Some("agent-1".to_string()),
                Some(30)
            )]
        );
        assert!(fake.activity_calls.lock().unwrap().is_empty());

        // Explicit window is passed through.
        agent_activity(&api, &ws, &json!({ "agentId": "agent-1", "minutesAgo": 5 }))
            .await
            .expect("dispatch");
        assert_eq!(fake.query_calls.lock().unwrap()[1].2, Some(5));

        // Without agentId the aggregate wire surface is untouched.
        agent_activity(&api, &ws, &json!({}))
            .await
            .expect("dispatch");
        assert_eq!(
            fake.activity_calls.lock().unwrap().as_slice(),
            &[(None, None)]
        );
        assert_eq!(fake.query_calls.lock().unwrap().len(), 2);
    }
}
