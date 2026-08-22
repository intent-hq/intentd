//! `ws.hook.*` bindings — background hooks (agent-owned scheduled scripts).
//!
//! Thin wrappers over the `WorkspaceApi` hook surface (`hook_manager` in
//! intent-services). `hook.schedule` is MCP-only (there is no wire
//! `hook.schedule` — hooks are agent-authored by design, §6.8) and attributes
//! the calling agent as the hook's owner, so it requires an agent caller
//! context; `list` / `cancel` / `runNow` mirror the wire methods of the same
//! names. `cancel` is ownership-scoped and therefore also requires an agent
//! caller context: an agent can only cancel its own hooks, and that cancel
//! does not wake the owner — only the FE cancel path does.

use std::sync::Arc;

use intent_core::{AgentId, HookId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.hook = {
        schedule: (opts) => host({ method: 'hook.schedule', args: opts || {} }),
        list: () => host({ method: 'hook.list' }),
        cancel: (hookId) => host({ method: 'hook.cancel', args: { hookId } }),
        runNow: (hookId) => host({ method: 'hook.runNow', args: { hookId } }),
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
        "schedule" => schedule(api, ws, caller, args).await,
        "list" => list(api, ws).await,
        "cancel" => cancel(api, ws, caller, args).await,
        "runNow" => run_now(api, ws, args).await,
        other => Err(format!("hook: unknown method `hook.{other}`")),
    }
}

async fn schedule(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    // Hooks are agent-owned: the schedule call must carry an agent caller
    // context (the FE front door and tests dispatch without one).
    let Some(owner) = caller else {
        return Err(
            "hook.schedule requires an agent caller context to attribute ownership".to_string(),
        );
    };
    api.hook_schedule(ws.clone(), owner.clone(), args.clone())
        .await
        .map_err(map_err)
}

async fn list(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    let raw = api.hook_list(ws.clone(), None).await.map_err(map_err)?;
    // The service returns `{ hooks: [...] }` (the wire shape); JS callers get
    // the bare array, mirroring `ws.script.list`.
    if let Some(inner) = raw.get("hooks") {
        return Ok(inner.clone());
    }
    Ok(raw)
}

async fn cancel(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    // Cancel is ownership-scoped: without an agent caller context there is
    // no owner to check against (mirrors `hook.schedule`).
    let Some(caller) = caller else {
        return Err(
            "hook.cancel requires an agent caller context to verify hook ownership".to_string(),
        );
    };
    let hook_id = req_str(args, "hookId").map_err(|_| "hookId is required".to_string())?;
    api.hook_cancel(
        ws.clone(),
        HookId::from(hook_id.as_str()),
        Some(caller.clone()),
    )
    .await
    .map_err(map_err)
}

async fn run_now(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let hook_id = req_str(args, "hookId").map_err(|_| "hookId is required".to_string())?;
    api.hook_run_now(ws.clone(), HookId::from(hook_id.as_str()))
        .await
        .map_err(map_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{BoxFuture, Result};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// `WorkspaceApi` that records whether the ownership-scoped hook methods
    /// were reached at all — the caller-context guards must reject before the
    /// service layer sees the call.
    #[derive(Default)]
    struct SpyApi {
        cancel_called: AtomicBool,
        schedule_called: AtomicBool,
    }

    impl WorkspaceApi for SpyApi {
        fn hook_cancel(
            &self,
            _workspace_id: WorkspaceId,
            _hook_id: HookId,
            _caller: Option<AgentId>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.cancel_called.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(json!({ "ok": true })) })
        }

        fn hook_schedule(
            &self,
            _workspace_id: WorkspaceId,
            _agent_id: AgentId,
            _params: Value,
        ) -> BoxFuture<'_, Result<Value>> {
            self.schedule_called.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(json!({ "ok": true })) })
        }
    }

    fn spy() -> (Arc<SpyApi>, Arc<dyn WorkspaceApi>, WorkspaceId) {
        let spy = Arc::new(SpyApi::default());
        let api: Arc<dyn WorkspaceApi> = spy.clone();
        (spy, api, WorkspaceId::from_string("ws-hook"))
    }

    /// Acceptance criterion (intent-hq/monorepo#1563): `hook.cancel` without
    /// an agent caller context is rejected before the service is touched, so
    /// the hook is untouched — mirroring the `hook.schedule` guard.
    #[tokio::test]
    async fn cancel_without_caller_context_is_rejected_and_never_reaches_the_service() {
        let (spy, api, ws) = spy();
        let err = dispatch(&api, &ws, None, "cancel", &json!({ "hookId": "hook-1" }))
            .await
            .unwrap_err();
        assert!(
            err.contains("requires an agent caller context"),
            "unexpected error: {err}"
        );
        assert!(
            !spy.cancel_called.load(Ordering::SeqCst),
            "service must not be reached"
        );
    }

    #[tokio::test]
    async fn cancel_with_caller_context_reaches_the_service() {
        let (spy, api, ws) = spy();
        let caller = AgentId::from("agent-caller");
        dispatch(
            &api,
            &ws,
            Some(&caller),
            "cancel",
            &json!({ "hookId": "hook-1" }),
        )
        .await
        .expect("cancel dispatched");
        assert!(spy.cancel_called.load(Ordering::SeqCst));
    }

    /// The mirrored `hook.schedule` guard, previously untested.
    #[tokio::test]
    async fn schedule_without_caller_context_is_rejected_and_never_reaches_the_service() {
        let (spy, api, ws) = spy();
        let err = dispatch(&api, &ws, None, "schedule", &json!({ "name": "watch" }))
            .await
            .unwrap_err();
        assert!(
            err.contains("requires an agent caller context"),
            "unexpected error: {err}"
        );
        assert!(
            !spy.schedule_called.load(Ordering::SeqCst),
            "service must not be reached"
        );
    }
}
