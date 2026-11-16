//! `ws.hook.*` bindings — background hooks (agent-owned scheduled scripts).
//!
//! Thin wrappers over the `WorkspaceApi` hook surface (`hook_manager` in
//! intent-services). `hook.schedule` is MCP-only (there is no wire
//! `hook.schedule` — hooks are agent-authored by design, §6.8) and attributes
//! the calling agent as the hook's owner, so it requires an agent caller
//! context; `list` / `cancel` / `runNow` mirror the wire methods of the same
//! names. `cancel` is ownership-scoped and therefore also requires an agent
//! caller context: an agent can only cancel its own hooks, and that cancel
//! does not wake the owner — only the FE cancel path does. `list` returns
//! ACTIVE hooks by default; `{ includeRetired: true }` appends the retired
//! rows as a light projection (no `code` / `lastState` / `lastLogs`). `get`
//! is MCP-only like `schedule` but read-only and unguarded (mirrors `list`
//! visibility): the full hook row including `code`, active or retired, so an
//! agent can recover a retired hook's script to re-arm it.

use std::sync::Arc;

use intent_core::{AgentId, HookId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_bool, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.hook = {
        schedule: (opts) => host({ method: 'hook.schedule', args: opts || {} }),
        list: (opts) => host({ method: 'hook.list', args: opts || {} }),
        get: async (hookId) => {
            // A hook whose code is over the read budget arrives in parts
            // (`codeOffset` in, `nextCodeOffset` out); join them, restart
            // if the code changes between parts, and refuse an incomplete
            // result.
            for (let attempt = 0; attempt < 3; attempt++) {
                const first = await host({ method: 'hook.get', args: { hookId } });
                if (!first || first.codeTruncated !== true) return first;
                const size = first.codeBytes;
                let code = first.code;
                let next = first.nextCodeOffset;
                let changed = false;
                // Parts are at least 64 KiB of JSON, so 4096 parts covers any
                // code the daemon accepts; more, or a part that does not move
                // forward, means the result can never complete.
                let parts = 1;
                while (next !== null && next !== undefined) {
                    if (++parts > 4096) throw new Error(`hook.get: code for ${hookId} is incomplete`);
                    const part = await host({ method: 'hook.get', args: { hookId, codeOffset: next } });
                    if (part.codeBytes !== size || part.codeOffset !== next) { changed = true; break; }
                    code += part.code;
                    const after = part.nextCodeOffset;
                    if (after !== null && after !== undefined && !(after > next)) {
                        throw new Error(`hook.get: code for ${hookId} is incomplete`);
                    }
                    next = after;
                }
                if (changed) continue;
                let bytes = 0;
                for (const ch of code) {
                    const cp = ch.codePointAt(0);
                    bytes += cp < 0x80 ? 1 : cp < 0x800 ? 2 : cp < 0x10000 ? 3 : 4;
                }
                if (bytes !== size) {
                    throw new Error(`hook.get: code for ${hookId} is incomplete`);
                }
                const whole = { ...first, code };
                for (const key of ['codeTruncated', 'codeBytes', 'codeOffset', 'nextCodeOffset']) delete whole[key];
                return whole;
            }
            throw new Error(`hook.get: code for ${hookId} kept changing while it was read`);
        },
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
        "list" => list(api, ws, args).await,
        "get" => get(api, ws, args).await,
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
    let mut scheduled = api
        .hook_schedule(ws.clone(), owner.clone(), args.clone())
        .await
        .map_err(map_err)?;
    let code = args.get("code").and_then(Value::as_str).unwrap_or_default();
    let mut monitored = Vec::new();
    for (pr_number, repo) in snapshot_targets(code) {
        if let Some(hint) = api
            .pr_monitor_sub_agent_hint(ws.clone(), owner.clone(), pr_number, repo)
            .await
        {
            monitored.push(hint);
        }
    }
    // A validation run that dispatched persisted no hook: nothing to cancel.
    let persisted = scheduled.get("dispatched") != Some(&Value::Bool(true));
    if let (true, false, Some(object)) =
        (persisted, monitored.is_empty(), scheduled.as_object_mut())
    {
        let instructions: Vec<&str> = monitored
            .iter()
            .filter_map(|hint| hint["instruction"].as_str())
            .collect();
        object.insert(
            "advisory".to_string(),
            Value::String(format!(
                "This hook polls a PR your sub-agent already monitors. {} Cancel this \
                 hook with ws.hook.cancel unless it watches something the sub-agent \
                 will not report.",
                instructions.join(" ")
            )),
        );
        object.insert("subAgentMonitors".to_string(), Value::Array(monitored));
    }
    Ok(scheduled)
}

/// Most literal PR reads a hook makes: `pr.snapshot(` / `mr.snapshot(`
/// followed by a number literal, with no options or a string-literal `repo`
/// option. Calls with a computed number, a computed `repo`, or an addressed
/// selector (`gitRootId` / `target`) are skipped: the advisory is a hint and
/// must not guess the repository.
fn snapshot_targets(code: &str) -> Vec<(u64, Option<String>)> {
    const MAX_TARGETS: usize = 8;
    let mut targets: Vec<(u64, Option<String>)> = Vec::new();
    for namespace in ["pr.snapshot(", "mr.snapshot("] {
        for (start, _) in code.match_indices(namespace) {
            let rest = code[start + namespace.len()..].trim_start();
            let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
            let Ok(pr_number) = rest[..digits].parse::<u64>() else {
                continue;
            };
            let call = &rest[digits..rest.find(')').unwrap_or(rest.len())];
            if call.contains("gitRootId") || call.contains("target") {
                continue;
            }
            let Ok(repo) = literal_repo(call) else {
                continue;
            };
            let target = (pr_number, repo);
            if !targets.contains(&target) && targets.len() < MAX_TARGETS {
                targets.push(target);
            }
        }
    }
    targets
}

/// The call's `repo` option: `Ok(None)` when it has no `repo` key,
/// `Ok(Some(slug))` for one string-literal value, and `Err` when the key is
/// present but not a single string literal (shorthand, a variable, or
/// repeated).
fn literal_repo(call: &str) -> Result<Option<String>, ()> {
    let word = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
    let mut found = None;
    for (at, key) in call.match_indices("repo") {
        let after = &call[at + key.len()..];
        if call[..at]
            .chars()
            .next_back()
            .is_some_and(|c| word(c) || c == '.')
            || after.starts_with(word)
        {
            continue;
        }
        let after = after.strip_prefix(['"', '\'']).unwrap_or(after);
        let value = after.trim_start().strip_prefix(':').ok_or(())?.trim_start();
        let quote = value
            .chars()
            .next()
            .filter(|c| matches!(c, '"' | '\'' | '`'))
            .ok_or(())?;
        let body = &value[1..];
        if found.is_some() {
            return Err(());
        }
        found = Some(body[..body.find(quote).ok_or(())?].to_string());
    }
    Ok(found)
}

async fn list(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let include_retired = opt_bool(args, "includeRetired").unwrap_or(false);
    let raw = api
        .hook_list(ws.clone(), None, include_retired)
        .await
        .map_err(map_err)?;
    // The service returns `{ hooks: [...] }` (the wire shape); JS callers get
    // the bare array, mirroring `ws.script.list`.
    if let Some(inner) = raw.get("hooks") {
        return Ok(inner.clone());
    }
    Ok(raw)
}

/// Bytes one `hook.get` host reply may carry: the shared 480,000-byte read
/// budget. Larger `code` is served in parts (`codeOffset` /
/// `nextCodeOffset`), which the `ws.hook.get` prelude joins.
const HOOK_GET_BUDGET_BYTES: usize = 480_000;

async fn get(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId, args: &Value) -> Result<Value, String> {
    let hook_id = req_str(args, "hookId").map_err(|_| "hookId is required".to_string())?;
    let offset = match args.get("codeOffset") {
        None | Some(Value::Null) => 0,
        Some(v) => v
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| "codeOffset must be a non-negative integer".to_string())?,
    };
    let mut row = api
        .hook_get(ws.clone(), HookId::from(hook_id.as_str()))
        .await
        .map_err(map_err)?;
    intent_core::text_field_page::page_text_field(&mut row, "code", offset, HOOK_GET_BUDGET_BYTES)?;
    Ok(row)
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
    use std::sync::Mutex;

    /// `WorkspaceApi` that records whether the ownership-scoped hook methods
    /// were reached at all — the caller-context guards must reject before the
    /// service layer sees the call — and the `include_retired` flag `list`
    /// forwarded.
    #[derive(Default)]
    struct SpyApi {
        cancel_called: AtomicBool,
        schedule_called: AtomicBool,
        get_called: AtomicBool,
        list_include_retired: Mutex<Option<bool>>,
        sub_agent_monitored: Vec<(u64, Option<String>)>,
        hint_calls: Mutex<Vec<(u64, Option<String>)>>,
    }

    impl WorkspaceApi for SpyApi {
        fn hook_list(
            &self,
            _workspace_id: WorkspaceId,
            _agent_id: Option<AgentId>,
            include_retired: bool,
        ) -> BoxFuture<'_, Result<Value>> {
            *self.list_include_retired.lock().unwrap() = Some(include_retired);
            Box::pin(async { Ok(json!({ "hooks": [{ "hookId": "hook-1" }] })) })
        }

        fn hook_get(
            &self,
            _workspace_id: WorkspaceId,
            hook_id: HookId,
        ) -> BoxFuture<'_, Result<Value>> {
            self.get_called.store(true, Ordering::SeqCst);
            Box::pin(async move { Ok(json!({ "hookId": hook_id.0, "code": "return;" })) })
        }

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

        fn pr_monitor_sub_agent_hint(
            &self,
            _workspace_id: WorkspaceId,
            _caller: AgentId,
            pr_number: u64,
            repo: Option<String>,
        ) -> BoxFuture<'_, Option<Value>> {
            let target = (pr_number, repo);
            self.hint_calls.lock().unwrap().push(target.clone());
            let hint = self.sub_agent_monitored.contains(&target).then(|| {
                json!({ "prNumber": pr_number, "watchAgentId": "agent-child", "instruction": format!("#{pr_number}: watch agent-child.") })
            });
            Box::pin(async { hint })
        }
    }

    /// `hook.schedule` still schedules a hook that polls a PR a sub-agent
    /// monitors, and adds an advisory naming the sub-agent route; every
    /// literal snapshot read is looked up once, computed numbers are not
    /// seen, and a hook with no sub-agent-monitored PR gets no advisory.
    #[tokio::test]
    async fn schedule_advises_when_the_hook_polls_a_pr_a_sub_agent_monitors() {
        let caller = AgentId::from("agent-parent");
        let ws = WorkspaceId::from_string("ws-hook");
        let code = r#"const a = await ws.pr.snapshot(3408, { repo: "intent-hq/intentd" });
            const b = await ws.mr.snapshot( 12 );
            const c = await ws.pr.snapshot(3408, { repo: 'intent-hq/intentd' });
            const d = await ws.pr.snapshot(n, { repo: "intent-hq/intentd" });
            const e = await ws.pr.snapshot(9, {repo:`o/r`});
            const f = await ws.pr.snapshot(5, { gitRootId: "root-1" });
            const g = await ws.pr.snapshot(6, { repo });
            const h = await ws.pr.snapshot(7, { repoRoot: x, repo: slug });
            const i = await ws.pr.snapshot(8, { "repo": "q/s" });
            return { dispatch: false };"#;
        let looked_up = vec![
            (8, Some("q/s".to_string())),
            (9, Some("o/r".to_string())),
            (12, None),
            (3408, Some("intent-hq/intentd".to_string())),
        ];
        for monitored in [vec![(3408, Some("intent-hq/intentd".to_string()))], vec![]] {
            let spy = Arc::new(SpyApi {
                sub_agent_monitored: monitored.clone(),
                ..SpyApi::default()
            });
            let api: Arc<dyn WorkspaceApi> = spy.clone();
            let scheduled = dispatch(
                &api,
                &ws,
                Some(&caller),
                "schedule",
                &json!({ "name": "poll", "delayMs": 60_000, "code": code }),
            )
            .await
            .expect("an advisory is not a refusal");
            assert_eq!(scheduled["ok"], json!(true), "{scheduled}");
            assert!(spy.schedule_called.load(Ordering::SeqCst));
            let mut calls = spy.hint_calls.lock().unwrap().clone();
            calls.sort();
            assert_eq!(calls, looked_up);
            if monitored.is_empty() {
                assert!(scheduled.get("advisory").is_none(), "{scheduled}");
                assert!(scheduled.get("subAgentMonitors").is_none(), "{scheduled}");
                continue;
            }
            let advisory = scheduled["advisory"].as_str().expect("advisory");
            assert!(
                advisory.starts_with("This hook polls a PR your sub-agent already monitors. #3408: watch agent-child."),
                "{advisory}"
            );
            assert!(advisory.contains("ws.hook.cancel"), "{advisory}");
            assert_eq!(scheduled["subAgentMonitors"][0]["prNumber"], json!(3408));
            assert_eq!(scheduled["subAgentMonitors"].as_array().unwrap().len(), 1);
        }
    }

    /// A validation run that dispatched persisted no hook, so there is
    /// nothing to cancel and no advisory.
    #[tokio::test]
    async fn schedule_gives_no_advisory_when_the_validation_run_dispatched() {
        struct DispatchedApi;
        impl WorkspaceApi for DispatchedApi {
            fn hook_schedule(
                &self,
                _workspace_id: WorkspaceId,
                _agent_id: AgentId,
                _params: Value,
            ) -> BoxFuture<'_, Result<Value>> {
                Box::pin(async { Ok(json!({ "hook": { "hookId": "h" }, "dispatched": true })) })
            }

            fn pr_monitor_sub_agent_hint(
                &self,
                _workspace_id: WorkspaceId,
                _caller: AgentId,
                pr_number: u64,
                _repo: Option<String>,
            ) -> BoxFuture<'_, Option<Value>> {
                Box::pin(
                    async move { Some(json!({ "prNumber": pr_number, "instruction": "watch" })) },
                )
            }
        }
        let api: Arc<dyn WorkspaceApi> = Arc::new(DispatchedApi);
        let scheduled = dispatch(
            &api,
            &WorkspaceId::from_string("ws-hook"),
            Some(&AgentId::from("agent-parent")),
            "schedule",
            &json!({ "name": "poll", "delayMs": 60_000, "code": "return await ws.pr.snapshot(1);" }),
        )
        .await
        .expect("scheduled");
        assert_eq!(scheduled["dispatched"], json!(true));
        assert!(scheduled.get("advisory").is_none(), "{scheduled}");
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

    /// `hook.get` is read-only and unguarded (mirrors `list` visibility):
    /// no caller context is required, and the hook row passes through
    /// verbatim — including `code`.
    #[tokio::test]
    async fn get_without_caller_context_reaches_the_service_and_returns_the_row() {
        let (spy, api, ws) = spy();
        let row = dispatch(&api, &ws, None, "get", &json!({ "hookId": "hook-1" }))
            .await
            .expect("get dispatched");
        assert!(spy.get_called.load(Ordering::SeqCst));
        assert_eq!(row, json!({ "hookId": "hook-1", "code": "return;" }));
    }

    #[tokio::test]
    async fn get_without_hook_id_is_rejected_before_the_service() {
        let (spy, api, ws) = spy();
        let err = dispatch(&api, &ws, None, "get", &json!({}))
            .await
            .unwrap_err();
        assert!(
            err.contains("hookId is required"),
            "unexpected error: {err}"
        );
        assert!(
            !spy.get_called.load(Ordering::SeqCst),
            "service must not be reached"
        );
    }

    /// Regression (intent-hq/intent#5307): a bare `ws.hook.list()` forwards
    /// `include_retired = false` (active-only default) and still unwraps the
    /// `{ hooks }` envelope to the bare array.
    #[tokio::test]
    async fn list_defaults_to_active_only() {
        let (spy, api, ws) = spy();
        let rows = dispatch(&api, &ws, None, "list", &json!({}))
            .await
            .expect("list dispatched");
        assert_eq!(*spy.list_include_retired.lock().unwrap(), Some(false));
        assert_eq!(rows, json!([{ "hookId": "hook-1" }]));
    }

    #[tokio::test]
    async fn list_forwards_include_retired() {
        let (spy, api, ws) = spy();
        dispatch(&api, &ws, None, "list", &json!({ "includeRetired": true }))
            .await
            .expect("list dispatched");
        assert_eq!(*spy.list_include_retired.lock().unwrap(), Some(true));
    }
}
