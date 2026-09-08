//! `ws.map.*` semantic-codebase map bindings.

use std::sync::Arc;

use intent_core::{NoteId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_str, opt_vec_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.map = {
        get: () => host({ method: 'map.get', args: {} }),
        setManifest: (json) => host({ method: 'map.setManifest', args: { json } }),
        classify: (paths) => host({ method: 'map.classify', args: { paths } }),
        activity: (options) => host({ method: 'map.activity', args: { ...(options || {}) } }),
        route: (options) => host({ method: 'map.route', args: { ...(options || {}) } }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "get" => api.map_get(ws.clone()).await.map_err(map_err),
        "setManifest" => {
            let json = args
                .get("json")
                .filter(|value| !value.is_null())
                .cloned()
                .ok_or_else(|| "json is required".to_string())?;
            api.map_set_manifest(ws.clone(), json)
                .await
                .map_err(map_err)
        }
        "classify" => {
            let paths = args
                .get("paths")
                .and_then(Value::as_array)
                .cloned()
                .ok_or_else(|| "paths must be an array".to_string())?;
            api.map_classify(ws.clone(), paths).await.map_err(map_err)
        }
        "activity" => api
            .map_activity(
                ws.clone(),
                opt_str(args, "sinceTs"),
                args.get("minutesAgo").and_then(Value::as_i64),
                opt_str(args, "agentId"),
                opt_vec_str(args, "kinds").unwrap_or_default(),
                args.get("limit").and_then(Value::as_i64),
            )
            .await
            .map_err(map_err),
        "route" => {
            let agent_id = opt_str(args, "agentId").filter(|value| !value.trim().is_empty());
            let task_note_id = opt_str(args, "taskNoteId")
                .filter(|value| !value.trim().is_empty())
                .map(NoteId::from);
            if agent_id.is_some() == task_note_id.is_some() {
                return Err("exactly one of agentId or taskNoteId is required".to_string());
            }
            api.map_route(ws.clone(), agent_id, task_note_id, opt_str(args, "sinceTs"))
                .await
                .map_err(map_err)
        }
        other => Err(format!("host: unknown method `map.{other}`")),
    }
}
