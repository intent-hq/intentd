//! `ws.help` binding — the runtime self-describing entrypoint.
//!
//! `ws.help()` returns the `Namespaces` index and `ws.help("pr")` returns one
//! namespace's full doc lines, both cut from the same assembled description
//! the MCP tool advertises (`super::super::tools::workspace_api_description`),
//! so chief-ness and `[agentFeatures]` gating apply automatically and the two
//! surfaces cannot drift. This gives agents whose MCP client truncated the
//! tool description an in-sandbox way to recover the full API reference.

use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::WorkspaceId;
use serde_json::Value;

use super::super::tools::{help_index, help_namespace};
use super::opt_str;

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.help = (namespace) => host({ method: 'help.get', args: { namespace } });
";

pub(crate) fn dispatch(
    workspace_id: &WorkspaceId,
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "get" => {
            let is_chief = workspace_id.is_chief();
            match opt_str(args, "namespace") {
                Some(ns) => {
                    help_namespace(is_chief, features, is_sub_agent, &ns).map(Value::String)
                }
                None => Ok(Value::String(help_index(is_chief, features))),
            }
        }
        other => Err(format!("host: unknown method `help.{other}`")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn get_returns_index_or_namespace_docs() {
        let ws: WorkspaceId = "ws-1".parse().unwrap();
        let features = AgentFeaturesSettings::default();
        let index = dispatch(&ws, &features, false, "get", &json!({})).unwrap();
        assert!(index.as_str().unwrap().starts_with("Namespaces"));
        let pr = dispatch(&ws, &features, false, "get", &json!({ "namespace": "pr" })).unwrap();
        assert!(pr.as_str().unwrap().contains("ws.pr.snapshot("));
        // Chief workspaces get the chief surface.
        let app = dispatch(
            &WorkspaceId::chief(),
            &features,
            false,
            "get",
            &json!({ "namespace": "app" }),
        )
        .unwrap();
        assert!(app.as_str().unwrap().contains("ws.app.workspaces.list("));
    }

    #[test]
    fn unknown_method_errors() {
        let ws: WorkspaceId = "ws-1".parse().unwrap();
        let err = dispatch(
            &ws,
            &AgentFeaturesSettings::default(),
            false,
            "nope",
            &Value::Null,
        )
        .unwrap_err();
        assert!(err.contains("help.nope"));
    }
}
