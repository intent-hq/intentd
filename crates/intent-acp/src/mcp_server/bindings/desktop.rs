//! Authenticated-agent-only desktop bindings; no generic client/RPC passthrough.
use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::Value;
use std::sync::Arc;
pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.desktop = Object.fromEntries(['startControl','endControl','listDisplay','screenshot','move','click','type','keypress','scroll','drag'].map(method => [method, (...args) => {
        if (args.length > 1) throw new Error('Desktop methods accept at most one argument');
        return host({method: 'desktop.' + method, args: args.length ? args[0] : {}});
    }]));
";
pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    let method = match method {
        "startControl" => "startControl",
        "endControl" => "endControl",
        "listDisplay" => "listDisplay",
        "screenshot" => "screenshot",
        "move" => "move",
        "click" => "click",
        "type" => "type",
        "keypress" => "keypress",
        "scroll" => "scroll",
        "drag" => "drag",
        other => return Err(format!("Unknown desktop method: {other}")),
    };
    intent_core::desktop::validate_action(method, args).map_err(|e| e.to_string())?;
    api.desktop_agent_call(workspace.clone(), method.to_string(), args.clone())
        .await
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use intent_core::settings_file::AgentFeaturesSettings;
    #[test]
    fn desktop_feature_gates_acquisition_but_never_cleanup_or_delegation() {
        let features = AgentFeaturesSettings::default();
        for delegated in [false, true] {
            let enabled = super::super::prelude_for_bridge(&features, delegated);
            assert!(enabled.contains("'startControl'"));
            assert!(enabled.contains("'listDisplay'"));
            assert!(enabled.contains("'move'"));
            let disabled = super::super::prelude_for_bridge(
                &AgentFeaturesSettings {
                    desktop_control: false,
                    ..features.clone()
                },
                delegated,
            );
            assert!(!disabled.contains("'startControl'"));
            assert!(!disabled.contains("'listDisplay'"));
            assert!(!disabled.contains("'move'"));
            assert!(!disabled.contains("ws.desktop ="));
        }
    }
}
