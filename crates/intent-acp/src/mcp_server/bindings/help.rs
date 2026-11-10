//! `ws.help` binding — the runtime self-describing entrypoint.
//!
//! `ws.help()` returns the `Namespaces` index and `ws.help("pr")` returns one
//! namespace's full doc lines, both cut from the same assembled description
//! the MCP tool advertises (`super::super::tools::workspace_api_description`),
//! so chief-ness and `[agentFeatures]` gating apply automatically and the two
//! surfaces cannot drift. The host then passes each reply through the
//! session's `WorkspaceApiDocs::help`, which harness 3.10 extends. This gives agents whose MCP client truncated the
//! tool description an in-sandbox way to recover the full API reference.
//! In a plain-folder workspace (§5.1) the git and forge surfaces are left out
//! exactly as in the tool description (`plain_folder_description`).

use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::WorkspaceId;
use serde_json::Value;

use super::super::tools::{help_index, help_namespace, plain_folder_description};
use super::opt_str;

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.help = (namespace) => host({ method: 'help.get', args: { namespace } });
";

/// The git and forge namespaces `ws.help` refuses in a plain-folder
/// workspace, matching the dispatch refusal of their methods.
const PLAIN_FOLDER_NAMESPACES: &[&str] = &["git", "pr", "mr"];

/// `ws.help` topics that also get the live remote-placement reference: the
/// agent namespace, and `agent.machineModels`, whose docs live only there.
const PLACEMENT_HELP_TOPICS: &[&str] = &["agent", "agent.machineModels"];

/// `ws.help`'s argument without the `ws.` prefix or a trailing `.` / `.*`.
fn normalize(topic: &str) -> &str {
    topic
        .trim()
        .trim_start_matches("ws.")
        .trim_end_matches('*')
        .trim_end_matches('.')
}

/// Whether `ws.help(topic)` should carry the remote-placement reference.
pub(crate) fn wants_placement_help(topic: Option<&str>) -> bool {
    topic.is_some_and(|topic| PLACEMENT_HELP_TOPICS.contains(&normalize(topic)))
}

pub(crate) fn dispatch(
    workspace_id: &WorkspaceId,
    features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    plain_folder: bool,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    let shape = |docs: String| {
        Value::String(if plain_folder {
            plain_folder_description(&docs)
        } else {
            docs
        })
    };
    match method {
        "get" => {
            let is_chief = workspace_id.is_chief();
            match opt_str(args, "namespace") {
                Some(ns) => {
                    let name = normalize(&ns);
                    let top = name.split('.').next().unwrap_or_default();
                    if plain_folder && PLAIN_FOLDER_NAMESPACES.contains(&top) {
                        return Err(format!(
                            "ws.{top} is not available in plain-folder workspaces"
                        ));
                    }
                    // A method path (`"agent.machineModels"`) gets its own doc
                    // line when it has one, else its namespace's docs. Only an
                    // unknown topic falls back: a gated namespace or method
                    // keeps its "disabled" or denial error.
                    help_namespace(is_chief, features, is_sub_agent, name)
                        .or_else(|err| {
                            let parent = name
                                .rsplit_once('.')
                                .map(|(parent, _)| parent)
                                .filter(|_| err.starts_with("unknown namespace"))
                                .ok_or_else(|| err.clone())?;
                            help_namespace(is_chief, features, is_sub_agent, parent)
                                .map_err(|_| err)
                        })
                        .map(shape)
                }
                None => Ok(shape(help_index(is_chief, features))),
            }
        }
        "suggest" => Err(super::suggest::unknown_method_message(
            workspace_id.is_chief(),
            features,
            is_sub_agent,
            args,
        )),
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
        let index = dispatch(&ws, &features, false, false, "get", &json!({})).unwrap();
        assert!(index.as_str().unwrap().starts_with("Namespaces"));
        let pr = dispatch(
            &ws,
            &features,
            false,
            false,
            "get",
            &json!({ "namespace": "pr" }),
        )
        .unwrap();
        assert!(pr.as_str().unwrap().contains("ws.pr.snapshot("));
        // Chief workspaces get the chief surface.
        let app = dispatch(
            &WorkspaceId::chief(),
            &features,
            false,
            false,
            "get",
            &json!({ "namespace": "app" }),
        )
        .unwrap();
        assert!(app.as_str().unwrap().contains("ws.app.workspaces.list("));
    }

    #[test]
    fn method_paths_return_the_method_or_its_namespace() {
        let ws: WorkspaceId = "ws-1".parse().unwrap();
        let features = AgentFeaturesSettings::default();
        let get = |topic: &str| {
            dispatch(
                &ws,
                &features,
                false,
                false,
                "get",
                &json!({ "namespace": topic }),
            )
        };
        // A documented method: its own doc line.
        let create = get("ws.script.create").unwrap();
        let create = create.as_str().unwrap();
        assert!(create.contains("ws.script.create("));
        assert!(!create.contains("ws.script.start("));
        // A method without its own line (capability-gated): the namespace.
        let models = get("agent.machineModels").unwrap();
        assert!(models.as_str().unwrap().contains("ws.agent.create("));
        assert!(wants_placement_help(Some("agent.machineModels")));
        assert!(wants_placement_help(Some("ws.agent")));
        assert!(!wants_placement_help(Some("agent.create")));
        // Unknown namespaces and methods of unknown namespaces still error.
        assert!(get("nope.method").is_err());
        assert!(get("nope").is_err());
    }

    #[test]
    fn method_paths_keep_the_namespace_gates() {
        let ws: WorkspaceId = "ws-1".parse().unwrap();
        let features = AgentFeaturesSettings {
            background_hooks: false,
            ..AgentFeaturesSettings::default()
        };
        let err = dispatch(
            &ws,
            &features,
            false,
            false,
            "get",
            &json!({ "namespace": "hook.schedule" }),
        )
        .unwrap_err();
        assert!(err.contains("disabled in settings"), "{err}");
        // A method-level gate keeps its error too, rather than falling back
        // to the namespace docs that leave the method out.
        let features = AgentFeaturesSettings {
            attention_requests: false,
            ..AgentFeaturesSettings::default()
        };
        let err = dispatch(
            &ws,
            &features,
            false,
            false,
            "get",
            &json!({ "namespace": "agent.requestDiscussion" }),
        )
        .unwrap_err();
        assert!(err.contains("attentionRequests"), "{err}");
        let plain = dispatch(
            &ws,
            &AgentFeaturesSettings::default(),
            false,
            true,
            "get",
            &json!({ "namespace": "git.commit" }),
        )
        .unwrap_err();
        assert!(plain.contains("plain-folder"), "{plain}");
    }

    #[test]
    fn unknown_method_errors() {
        let ws: WorkspaceId = "ws-1".parse().unwrap();
        let err = dispatch(
            &ws,
            &AgentFeaturesSettings::default(),
            false,
            false,
            "nope",
            &Value::Null,
        )
        .unwrap_err();
        assert!(err.contains("help.nope"));
    }
}
