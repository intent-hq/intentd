use intent_core::events::{
    AGENT_STREAM_ACTIVITY, AGENT_TOOL_CALL, FILE_CHANGED, FILE_CREATED, FILE_DELETED,
};
use intent_core::{ActorType, Event};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::classifier::Classifier;
use super::manifest::Manifest;
use super::paths::WorkspacePaths;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MapActivityKind {
    Read,
    Edit,
    Create,
    Delete,
    Move,
    Tool,
    Thinking,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MapActivity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub kind: MapActivityKind,
    pub ts: String,
}

pub fn project(manifest: &Manifest, event: &Event) -> Option<MapActivity> {
    project_with_paths(manifest, &WorkspacePaths::default(), event)
}

pub fn project_with_paths(
    manifest: &Manifest,
    workspace_paths: &WorkspacePaths,
    event: &Event,
) -> Option<MapActivity> {
    let classifier = Classifier::new(manifest);
    match event.event_type.as_str() {
        FILE_CHANGED | FILE_CREATED | FILE_DELETED | "file:renamed" => {
            let kind = file_kind(event)?;
            let path = event
                .data
                .get("relativePath")
                .or_else(|| event.data.get("path"))
                .and_then(Value::as_str)
                .filter(|path| !path.is_empty())
                .map(|path| workspace_paths.normalize(path, event_git_root_id(event)));
            let region_id = path
                .as_deref()
                .map(|path| classifier.classify(path).region_id);
            let (agent_id, agent_name) = agent_identity(event);
            Some(MapActivity {
                region_id,
                agent_id,
                agent_name,
                path,
                kind,
                ts: event.timestamp.clone(),
            })
        }
        AGENT_TOOL_CALL => {
            let tool_name = event
                .data
                .get("toolName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let tool_kind = event
                .data
                .get("toolKind")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let input = event.data.get("input");
            let path = input.and_then(find_path).map(|path| {
                workspace_paths.normalize(
                    &path,
                    event_git_root_id(event).or_else(|| input.and_then(find_git_root_id)),
                )
            });
            let is_read = path.is_some() && is_read_tool(tool_name, tool_kind);
            let region_id = is_read.then(|| {
                classifier
                    .classify(path.as_deref().expect("checked above"))
                    .region_id
            });
            let (agent_id, agent_name) = agent_identity(event);
            Some(MapActivity {
                region_id,
                agent_id,
                agent_name,
                path: is_read.then_some(path).flatten(),
                kind: if is_read {
                    MapActivityKind::Read
                } else {
                    MapActivityKind::Tool
                },
                ts: event.timestamp.clone(),
            })
        }
        AGENT_STREAM_ACTIVITY => {
            let (agent_id, agent_name) = agent_identity(event);
            Some(MapActivity {
                region_id: None,
                agent_id,
                agent_name,
                path: None,
                kind: MapActivityKind::Thinking,
                ts: event.timestamp.clone(),
            })
        }
        _ => None,
    }
}

fn event_git_root_id(event: &Event) -> Option<&str> {
    event.data.get("gitRootId").and_then(Value::as_str)
}

fn file_kind(event: &Event) -> Option<MapActivityKind> {
    match event.data.get("action").and_then(Value::as_str) {
        Some("modify" | "change" | "changed") => Some(MapActivityKind::Edit),
        Some("create" | "created") => Some(MapActivityKind::Create),
        Some("delete" | "deleted") => Some(MapActivityKind::Delete),
        Some("rename" | "renamed" | "move" | "moved") => Some(MapActivityKind::Move),
        Some(_) => None,
        None if event.event_type == FILE_CHANGED => Some(MapActivityKind::Edit),
        None if event.event_type == FILE_CREATED => Some(MapActivityKind::Create),
        None if event.event_type == FILE_DELETED => Some(MapActivityKind::Delete),
        None if event.event_type == "file:renamed" => Some(MapActivityKind::Move),
        None => None,
    }
}

fn agent_identity(event: &Event) -> (Option<String>, Option<String>) {
    let actor_is_agent = event.actor.actor_type == ActorType::Agent;
    let id = actor_is_agent
        .then(|| event.actor.id.clone())
        .flatten()
        .or_else(|| {
            event
                .data
                .get("agentId")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
    let name = actor_is_agent
        .then(|| event.actor.name.clone())
        .flatten()
        .or_else(|| {
            event
                .data
                .get("agentName")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
    (id, name)
}

fn is_read_tool(name: &str, kind: &str) -> bool {
    if kind.eq_ignore_ascii_case("search") {
        return true;
    }
    let name = name.to_ascii_lowercase();
    ["read", "view", "search", "grep", "glob", "find", "list"]
        .iter()
        .any(|verb| name.contains(verb))
}

fn find_path(value: &Value) -> Option<String> {
    match value {
        Value::Object(object) => {
            for key in ["path", "filePath", "relativePath"] {
                if let Some(path) = object
                    .get(key)
                    .and_then(Value::as_str)
                    .filter(|path| !path.is_empty())
                {
                    return Some(path.to_string());
                }
            }
            object.values().find_map(find_path)
        }
        Value::Array(values) => values.iter().find_map(find_path),
        _ => None,
    }
}

fn find_git_root_id(value: &Value) -> Option<&str> {
    match value {
        Value::Object(object) => object
            .get("gitRootId")
            .and_then(Value::as_str)
            .or_else(|| object.values().find_map(find_git_root_id)),
        Value::Array(values) => values.iter().find_map(find_git_root_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use intent_core::{EventActor, WorkspaceId};
    use serde_json::json;

    use super::*;
    use crate::semantic_map::{ManifestSource, Region};

    fn manifest() -> Manifest {
        Manifest {
            version: 1,
            source: ManifestSource::Curated,
            regions: vec![Region {
                id: "code".into(),
                label: "Code".into(),
                responsibility: "Code".into(),
                parent: None,
                anchor: [0.5, 0.5],
                paths: vec!["src/**".into()],
                color: None,
            }],
            crossings: vec![],
        }
    }

    fn event(event_type: &str, data: Value) -> Event {
        Event {
            id: "event-1".into(),
            workspace_id: WorkspaceId::from("ws-1"),
            timestamp: "2026-09-01T00:00:00Z".into(),
            event_type: event_type.into(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some("agent-1".into()),
                name: Some("Ada".into()),
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        }
    }

    #[test]
    fn projects_each_file_action() {
        for (event_type, action, expected) in [
            (FILE_CHANGED, "modify", MapActivityKind::Edit),
            (FILE_CREATED, "create", MapActivityKind::Create),
            (FILE_DELETED, "delete", MapActivityKind::Delete),
            ("file:renamed", "rename", MapActivityKind::Move),
        ] {
            let activity = project(
                &manifest(),
                &event(
                    event_type,
                    json!({"relativePath":"src/lib.rs","action":action}),
                ),
            )
            .unwrap();
            assert_eq!(activity.kind, expected);
            assert_eq!(activity.region_id.as_deref(), Some("code"));
        }
    }

    #[test]
    fn projects_read_tool_generic_tool_and_thinking() {
        let read = project(
            &manifest(),
            &event(
                AGENT_TOOL_CALL,
                json!({"toolName":"view","toolKind":"file","input":{"path":"src/lib.rs"}}),
            ),
        )
        .unwrap();
        assert_eq!(read.kind, MapActivityKind::Read);
        assert_eq!(read.region_id.as_deref(), Some("code"));

        let tool = project(
            &manifest(),
            &event(
                AGENT_TOOL_CALL,
                json!({"toolName":"launch-process","toolKind":"terminal","input":{"cwd":"src"}}),
            ),
        )
        .unwrap();
        assert_eq!(tool.kind, MapActivityKind::Tool);
        assert_eq!(tool.region_id, None);

        let thinking = project(&manifest(), &event(AGENT_STREAM_ACTIVITY, json!({}))).unwrap();
        assert_eq!(thinking.kind, MapActivityKind::Thinking);
        assert_eq!(thinking.agent_id.as_deref(), Some("agent-1"));
    }

    #[test]
    fn projects_registered_root_paths_relative_to_the_workspace() {
        let mut manifest = manifest();
        manifest.regions[0].paths = vec!["packages/intentd/crates/**".into()];
        let paths = WorkspacePaths::with_prefix("intentd-root", "packages/intentd");
        let activity = project_with_paths(
            &manifest,
            &paths,
            &event(
                FILE_CHANGED,
                json!({
                    "relativePath": "./crates/intent-transport/src/router.rs",
                    "gitRootId": "intentd-root",
                    "action": "modify"
                }),
            ),
        )
        .unwrap();

        assert_eq!(activity.region_id.as_deref(), Some("code"));
        assert_eq!(
            activity.path.as_deref(),
            Some("packages/intentd/crates/intent-transport/src/router.rs")
        );
    }
}
