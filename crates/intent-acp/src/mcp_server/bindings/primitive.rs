//! `ws.primitive.*` bindings (WSAPI-3).
//!
//! Each entry point forwards to a [`WorkspaceApi::primitive_add_*`] method;
//! the daemon owns fenced `ws-block:<type>` block construction and note
//! append, so the binding here is a thin argument peel.

use std::sync::Arc;

use intent_core::{NoteId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_str, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.primitive = {
        addReference: (noteId, semanticId, description, snapshot) =>
            host({
                method: 'primitive.addReference',
                args: { noteId, semanticId, description, snapshot },
            }),
        addCli: (noteId, command, description, workingDirectory) =>
            host({
                method: 'primitive.addCli',
                args: { noteId, command, description, workingDirectory },
            }),
        addPatch: (noteId, filePath, diff, description) =>
            host({
                method: 'primitive.addPatch',
                args: { noteId, filePath, diff, description },
            }),
        addAgentAction: (noteId, agentId, goal, description) =>
            host({
                method: 'primitive.addAgentAction',
                args: { noteId, agentId, goal, description },
            }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "addReference" => add_reference(api, ws, args).await,
        "addCli" => add_cli(api, ws, args).await,
        "addPatch" => add_patch(api, ws, args).await,
        "addAgentAction" => add_agent_action(api, ws, args).await,
        other => Err(format!("host: unknown method `primitive.{other}`")),
    }
}

async fn add_reference(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId")?;
    let semantic_id = req_str(args, "semanticId")?;
    let description = req_str(args, "description")?;
    let snapshot = opt_str(args, "snapshot");
    api.primitive_add_reference(
        ws.clone(),
        NoteId::from_string(&note_id),
        semantic_id,
        description,
        snapshot,
    )
    .await
    .map_err(map_err)
}

async fn add_cli(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId")?;
    let command = req_str(args, "command")?;
    let description = req_str(args, "description")?;
    let working_directory = opt_str(args, "workingDirectory");
    api.primitive_add_cli(
        ws.clone(),
        NoteId::from_string(&note_id),
        command,
        description,
        working_directory,
    )
    .await
    .map_err(map_err)
}

async fn add_patch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId")?;
    let file_path = req_str(args, "filePath")?;
    let diff = req_str(args, "diff")?;
    let description = req_str(args, "description")?;
    api.primitive_add_patch(
        ws.clone(),
        NoteId::from_string(&note_id),
        file_path,
        diff,
        description,
    )
    .await
    .map_err(map_err)
}

async fn add_agent_action(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let note_id = req_str(args, "noteId")?;
    let agent_id = req_str(args, "agentId")?;
    let goal = req_str(args, "goal")?;
    let description = req_str(args, "description")?;
    api.primitive_add_agent_action(
        ws.clone(),
        NoteId::from_string(&note_id),
        agent_id,
        goal,
        description,
    )
    .await
    .map_err(map_err)
}
