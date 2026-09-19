//! `ws.workspace.*` bindings (WSAPI-5).
//!
//! Extends the WSAPI-2 `workspace.info` proof point with the reference
//! `ws-workspace-api.ts` surface. Methods without a backing daemon RPC
//! (`context`, `referenceDocs`, `emitNotification`, and the timeline data
//! source itself) surface a clear "not yet available in port" error so the
//! JS caller sees why the binding cannot resolve, instead of inventing
//! behavior.

use std::sync::Arc;

use intent_core::{
    AgentId, AgentStatus, ConversationProjection, Error, WorkspaceApi, WorkspaceCreate,
    WorkspaceId, WorkspaceStatus, WorkspaceUpdate, PROPOSAL_OUTCOME_APPLIED,
    PROPOSAL_OUTCOME_DISMISSED, WORKSPACE_STATUS_MESSAGE_MAX_LENGTH,
};
use serde_json::{json, Value};

use super::app::proposal::PROPOSAL_RESOURCE_MIME_TYPE;
use super::{map_err, req_str, strip_agent_hidden_fields};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.workspace = {
        info: () => host({ method: 'workspace.info' }),
        details: () => host({ method: 'workspace.details' }),
        setTitle: (title) => host({ method: 'workspace.setTitle', args: { title } }),
        setStatusMessage: (statusMessage) =>
            host({ method: 'workspace.setStatusMessage', args: { statusMessage } }),
        setStatusImage: (image) =>
            host({ method: 'workspace.setStatusImage', args: { image } }),
        setAgentName: (name) => host({ method: 'workspace.setAgentName', args: { name } }),
        archive: () => host({ method: 'workspace.archive' }),
        unarchive: () => host({ method: 'workspace.unarchive' }),
        proposeSibling: (params) =>
            host({ method: 'workspace.proposeSibling', args: params || {} }),
        applyProposal: (proposalId, options) =>
            host({ method: 'workspace.applyProposal', args: { proposalId, options } }),
        context: () => host({ method: 'workspace.context' }),
        timeline: (limit, type) =>
            host({ method: 'workspace.timeline', args: { limit, type } }),
        referenceDocs: (topic) =>
            host({ method: 'workspace.referenceDocs', args: { topic } }),
        emitNotification: (topic, message, metadata) =>
            host({ method: 'workspace.emitNotification', args: { topic, message, metadata } }),
    };
";

const PROPOSE_SIBLING_PRELUDE: &str = "        proposeSibling: (params) =>\n            host({ method: 'workspace.proposeSibling', args: params || {} }),\n";
const APPLY_PROPOSAL_PRELUDE: &str = "        applyProposal: (proposalId, options) =>\n            host({ method: 'workspace.applyProposal', args: { proposalId, options } }),\n";

pub(crate) fn prelude_for(is_sub_agent: bool) -> String {
    if is_sub_agent {
        PRELUDE
            .replacen(PROPOSE_SIBLING_PRELUDE, "", 1)
            .replacen(APPLY_PROPOSAL_PRELUDE, "", 1)
    } else {
        PRELUDE.to_string()
    }
}

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "info" => info(api, ws).await,
        "details" => details(api, ws).await,
        "setTitle" => set_title(api, ws, args).await,
        "setStatusMessage" => set_status_message(api, ws, args).await,
        "setStatusImage" => set_status_image(api, ws, args).await,
        "setAgentName" => set_agent_name(api, caller_agent_id, args).await,
        "archive" => archive(api, ws, caller_agent_id).await,
        "unarchive" => unarchive(api, ws).await,
        "proposeSibling" => propose_sibling(api, ws, args).await,
        "applyProposal" => apply_proposal(api, ws, caller_agent_id, args).await,
        "context" => {
            Err("ws.workspace.context is not yet available in this daemon port".to_string())
        }
        "timeline" => {
            Err("ws.workspace.timeline is not yet available in this daemon port".to_string())
        }
        "referenceDocs" => {
            Err("ws.workspace.referenceDocs is not yet available in this daemon port".to_string())
        }
        "emitNotification" => Err(
            "ws.workspace.emitNotification is not yet available in this daemon port".to_string(),
        ),
        other => Err(format!("host: unknown method `workspace.{other}`")),
    }
}

const SIBLING_PROPOSAL_ALLOWED_KEYS: &[&str] = &["title", "initialPrompt", "specialist", "baseRef"];

fn strict_non_empty_string(
    args: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<String, String> {
    match args.get(key) {
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(value.trim().to_string()),
        Some(_) => Err(format!("{key} must be a non-empty string")),
        None => Err(format!("{key} is required and must be a non-empty string")),
    }
}

fn strict_optional_string(
    args: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<String>, String> {
    match args.get(key) {
        None => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => {
            Ok(Some(value.trim().to_string()))
        }
        Some(_) => Err(format!("{key} must be a non-empty string when provided")),
    }
}

fn new_sibling_idempotency_key() -> String {
    format!("sibling-workspace-{}", uuid::Uuid::new_v4())
}

async fn propose_sibling(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let params = args
        .as_object()
        .ok_or_else(|| "proposeSibling requires one options object".to_string())?;
    if let Some(key) = params
        .keys()
        .find(|key| !SIBLING_PROPOSAL_ALLOWED_KEYS.contains(&key.as_str()))
    {
        return Err(format!(
            "unknown proposeSibling field `{key}`; allowed fields are title, initialPrompt, specialist, baseRef"
        ));
    }
    let title = strict_non_empty_string(params, "title")?;
    let initial_prompt = strict_non_empty_string(params, "initialPrompt")?;
    let specialist = strict_optional_string(params, "specialist")?;
    let base_ref = strict_optional_string(params, "baseRef")?;

    let workspace = api
        .get_workspace(workspace_id.clone())
        .await
        .map_err(map_err)?;
    let repository_path = workspace
        .repository_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .filter(|path| std::path::Path::new(path).is_dir())
        .ok_or_else(|| {
            "The current workspace has no usable repository; a sibling workspace cannot be proposed"
                .to_string()
        })?
        .to_string();

    let repo_path = repository_path.clone();
    let default_branch = tokio::task::spawn_blocking(move || {
        intent_git::branches::repo_default_branch(std::path::Path::new(&repo_path))
    })
    .await
    .map_err(|error| format!("Could not inspect the current workspace repository: {error}"))?
    .map_err(|_| {
        "The current workspace has no usable Git repository; a sibling workspace cannot be proposed"
            .to_string()
    })?;
    let resolved_base_ref = base_ref.as_deref().unwrap_or(&default_branch);

    let mut warnings = Vec::new();
    if let Some(named_ref) = base_ref.as_deref() {
        let repo_path = repository_path.clone();
        let canonical = intent_git::refs::canonicalise_base_ref(named_ref);
        let resolves = tokio::task::spawn_blocking(move || {
            intent_git::worktree::base_ref_resolves(
                std::path::Path::new(&repo_path),
                &canonical,
                "origin",
            )
        })
        .await
        .map_err(|error| format!("Could not validate baseRef: {error}"))?
        .unwrap_or(false);
        if !resolves {
            warnings.push(format!(
                "Base ref '{named_ref}' does not exist in the current repository; applying this proposal will fail until the ref exists"
            ));
        }
    }

    let idempotency_key = new_sibling_idempotency_key();
    let mut create_params = serde_json::Map::new();
    create_params.insert("title".to_string(), json!(title));
    create_params.insert("repositoryPath".to_string(), json!(repository_path));
    if let Some(owner) = workspace.repository_owner.as_deref() {
        create_params.insert("repositoryOwner".to_string(), json!(owner));
    }
    if let Some(name) = workspace.repository_name.as_deref() {
        create_params.insert("repositoryName".to_string(), json!(name));
    }
    create_params.insert("baseRef".to_string(), json!(resolved_base_ref));
    let mut initial_agent = serde_json::Map::new();
    initial_agent.insert("name".to_string(), json!("Coordinator"));
    initial_agent.insert("prompt".to_string(), json!(initial_prompt));
    initial_agent.insert("agentType".to_string(), json!("workspace"));
    let mut metadata = serde_json::Map::new();
    metadata.insert("isInitialAgent".to_string(), json!(true));
    if let Some(value) = specialist.as_deref() {
        initial_agent.insert("specialist".to_string(), json!(value));
        metadata.insert("specialist".to_string(), json!(value));
    }
    initial_agent.insert("metadata".to_string(), Value::Object(metadata));
    create_params.insert("initialAgent".to_string(), Value::Object(initial_agent));
    create_params.insert("idempotencyKey".to_string(), json!(idempotency_key));

    let github_url = match (
        workspace.repository_owner.as_deref(),
        workspace.repository_name.as_deref(),
    ) {
        (Some(owner), Some(name)) => Some(format!("https://github.com/{owner}/{name}")),
        _ => None,
    };
    let mut workspace_create = serde_json::Map::new();
    workspace_create.insert("mode".to_string(), json!("sibling"));
    workspace_create.insert("title".to_string(), json!(title));
    workspace_create.insert("initialPrompt".to_string(), json!(initial_prompt));
    workspace_create.insert("repoPath".to_string(), json!(repository_path));
    workspace_create.insert("repoType".to_string(), json!("local"));
    workspace_create.insert("branch".to_string(), json!(resolved_base_ref));
    workspace_create.insert("isNewRepo".to_string(), json!(false));
    if let Some(url) = github_url {
        workspace_create.insert("githubUrl".to_string(), json!(url));
    }
    if let Some(value) = specialist.as_deref() {
        workspace_create.insert("specialist".to_string(), json!(value));
    }

    let mut preview = serde_json::Map::new();
    preview.insert(
        "title".to_string(),
        json!(format!("Create workspace: {title}")),
    );
    preview.insert(
        "summary".to_string(),
        json!("Review this follow-up workspace before creating it."),
    );
    preview.insert(
        "workspaceCreate".to_string(),
        Value::Object(workspace_create),
    );
    if !warnings.is_empty() {
        preview.insert("warnings".to_string(), json!(warnings));
    }
    let proposal = json!({
        "kind": "workspace-create",
        "payload": {
            "operation": "workspace.create",
            "params": create_params,
        },
        "preview": preview,
    });
    let mut result = super::app::workspaces::proposal_result(&proposal)?;
    // Surface the stable handle applyProposal accepts across resolution
    // (additive): the idempotencyKey only addresses the proposal while pending.
    if let Some(proposal_id) = proposal_identity(&proposal).map(str::to_string) {
        if let Some(obj) = result.as_object_mut() {
            obj.insert("proposalId".to_string(), json!(proposal_id));
        }
        if let Some(text) = result
            .get_mut("__mcpContentItems")
            .and_then(Value::as_array_mut)
            .and_then(|items| items.first_mut())
            .and_then(|item| item.get_mut("text"))
        {
            if let Some(mut body) = text
                .as_str()
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
            {
                if let Some(obj) = body.as_object_mut() {
                    obj.insert("proposalId".to_string(), json!(proposal_id));
                }
                *text =
                    json!(serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".to_string()));
            }
        }
    }
    Ok(result)
}

const APPLY_PROPOSAL_ALLOWED_KEYS: &[&str] = &["userRequested", "title", "initialPrompt"];

const APPLY_PROPOSAL_USER_REQUESTED_REQUIRED: &str = "applyProposal requires { userRequested: true } — only call it when the user explicitly asked to apply this proposal in chat";

/// Parse one proposal-resource content block into its embedded proposal
/// JSON; `None` for any other block (wrong type / MIME / unparseable text).
fn proposal_in_block(block: &Value) -> Option<Value> {
    if block.get("type").and_then(Value::as_str) != Some("resource") {
        return None;
    }
    let resource = block.get("resource")?;
    if resource.get("mimeType").and_then(Value::as_str) != Some(PROPOSAL_RESOURCE_MIME_TYPE) {
        return None;
    }
    serde_json::from_str(resource.get("text")?.as_str()?).ok()
}

/// The proposal identity the pending-tracking records
/// (`applyToolCallId ?? preview.title`, the same rule as
/// `intent_services::tool_block::proposal_block_id`).
fn proposal_identity(proposal: &Value) -> Option<&str> {
    proposal
        .get("applyToolCallId")
        .and_then(Value::as_str)
        .or_else(|| {
            proposal
                .get("preview")
                .and_then(|p| p.get("title"))
                .and_then(Value::as_str)
        })
        .filter(|id| !id.is_empty())
}

fn proposal_idempotency_key(proposal: &Value) -> Option<&str> {
    proposal
        .get("payload")
        .and_then(|p| p.get("params"))
        .and_then(|p| p.get("idempotencyKey"))
        .and_then(Value::as_str)
        .filter(|key| !key.trim().is_empty())
}

/// Load the proposal blocks of ONE carrying message: a single-message seek
/// page (`limit: 1`, `aroundMessageId`) — the same bounded pattern the
/// service's own resolve path uses, never a transcript hydration.
async fn proposals_in_message(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    agent_id: &AgentId,
    message_id: &str,
) -> Result<Vec<Value>, String> {
    let page = api
        .agent_get_conversation(
            agent_id.clone(),
            Some(1),
            Some(ws.clone()),
            None,
            Some(message_id.to_string()),
            None,
            Some(ConversationProjection::Slim),
            false,
        )
        .await
        .map_err(map_err)?;
    let blocks = page
        .get("messages")
        .and_then(Value::as_array)
        .and_then(|messages| {
            messages
                .iter()
                .find(|m| m.get("id").and_then(Value::as_str) == Some(message_id))
        })
        .and_then(|m| m.get("contentBlocks"))
        .and_then(Value::as_array);
    Ok(blocks
        .map(|blocks| blocks.iter().filter_map(proposal_in_block).collect())
        .unwrap_or_default())
}

/// `ws.workspace.applyProposal(proposalIdOrIdempotencyKey, { userRequested:
/// true, title?, initialPrompt? })` (intent-hq/intent#5413): apply one of the
/// CALLER's own pending `workspace-create` proposals on explicit user
/// instruction. The lookup is scoped to the caller's session, so another
/// agent's proposal is never applicable. The proposal's stored
/// `idempotencyKey` is reused verbatim (with or without overrides) so agent
/// Apply, card Apply and card Retry converge on one workspace; on create
/// success the proposal is resolved `applied` (same notice + `agent:updated`
/// the UI Apply produces), on create failure nothing is recorded so the card
/// stays pending for Retry.
async fn apply_proposal(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let proposal_ref = match args.get("proposalId") {
        Some(Value::String(value)) if !value.trim().is_empty() => value.clone(),
        _ => {
            return Err(
                "applyProposal requires a non-empty proposal id (or the proposal's idempotencyKey) as its first argument"
                    .to_string(),
            )
        }
    };
    let options = args
        .get("options")
        .and_then(Value::as_object)
        .ok_or_else(|| APPLY_PROPOSAL_USER_REQUESTED_REQUIRED.to_string())?;
    if let Some(key) = options
        .keys()
        .find(|key| !APPLY_PROPOSAL_ALLOWED_KEYS.contains(&key.as_str()))
    {
        return Err(format!(
            "unknown applyProposal option `{key}`; allowed options are userRequested, title, initialPrompt"
        ));
    }
    if options.get("userRequested") != Some(&Value::Bool(true)) {
        return Err(APPLY_PROPOSAL_USER_REQUESTED_REQUIRED.to_string());
    }
    let title_override = strict_optional_string(options, "title")?;
    let prompt_override = strict_optional_string(options, "initialPrompt")?;
    let caller = caller_agent_id
        .cloned()
        .ok_or_else(|| "Could not determine agent ID from request context".to_string())?;

    let lite = api
        .agent_get(caller.clone(), Some(ws.clone()))
        .await
        .map_err(map_err)?;
    let pending = &lite.metadata.pending_proposals;
    let resolutions = &lite.metadata.proposal_resolutions;

    // (a) a pending entry's `proposalId` verbatim; (b) the `idempotencyKey`
    // of a pending entry's proposal block.
    let mut matched: Option<(String, Value)> = None;
    if let Some(entry) = pending.iter().find(|p| p.proposal_id == proposal_ref) {
        let proposal = proposals_in_message(api, ws, &caller, &entry.message_id)
            .await?
            .into_iter()
            .find(|p| proposal_identity(p) == Some(entry.proposal_id.as_str()))
            .ok_or_else(|| {
                format!(
                    "proposal `{proposal_ref}` is pending but its proposal block could not be loaded from message {}",
                    entry.message_id
                )
            })?;
        matched = Some((entry.proposal_id.clone(), proposal));
    } else {
        for entry in pending {
            let found = proposals_in_message(api, ws, &caller, &entry.message_id)
                .await?
                .into_iter()
                .find(|p| {
                    proposal_identity(p) == Some(entry.proposal_id.as_str())
                        && proposal_idempotency_key(p) == Some(proposal_ref.as_str())
                });
            if let Some(proposal) = found {
                matched = Some((entry.proposal_id.clone(), proposal));
                break;
            }
        }
    }
    let Some((proposal_id, proposal)) = matched else {
        return match resolutions.get(&proposal_ref).and_then(Value::as_str) {
            Some(PROPOSAL_OUTCOME_APPLIED) => Ok(json!({
                "ok": true,
                "proposalId": proposal_ref,
                "outcome": PROPOSAL_OUTCOME_APPLIED,
                "alreadyResolved": true,
            })),
            Some(PROPOSAL_OUTCOME_DISMISSED) => Err(format!(
                "proposal `{proposal_ref}` was dismissed by the user; propose it again with ws.workspace.proposeSibling if still wanted"
            )),
            _ => Err(format!(
                "proposal `{proposal_ref}` matched neither a pending proposalId/idempotencyKey nor a resolved proposalId on this agent. \
                 An idempotencyKey only addresses a proposal while it is pending; if the card may already have been applied or dismissed, retry with the `proposalId` from the proposeSibling result. \
                 A proposal emitted in the CURRENT turn is recorded as pending only at turn end — end your turn and wait for the user's instruction before applying it."
            )),
        };
    };

    let kind = proposal
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let operation = proposal
        .get("payload")
        .and_then(|p| p.get("operation"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if kind != "workspace-create" || operation != "workspace.create" {
        return Err(format!(
            "proposal `{proposal_id}` is a `{kind}` proposal ({operation}); applyProposal only applies workspace-create proposals"
        ));
    }
    let mut params = proposal
        .get("payload")
        .and_then(|p| p.get("params"))
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| format!("proposal `{proposal_id}` has no payload.params object"))?;
    let idempotency_key = proposal_idempotency_key(&proposal)
        .ok_or_else(|| {
            format!("proposal `{proposal_id}` has no payload.params.idempotencyKey; it cannot be applied safely")
        })?
        .to_string();
    if let Some(title) = title_override.as_deref() {
        params.insert("title".to_string(), json!(title));
    }
    if let Some(prompt) = prompt_override.as_deref() {
        let Some(initial_agent) = params
            .get_mut("initialAgent")
            .and_then(Value::as_object_mut)
        else {
            return Err(format!(
                "proposal `{proposal_id}` has no initialAgent; initialPrompt cannot be overridden"
            ));
        };
        initial_agent.insert("prompt".to_string(), json!(prompt));
    }
    if params
        .get("initialAgent")
        .and_then(|a| a.get("agentId"))
        .is_some_and(|v| !v.is_null())
    {
        return Err(
            "initialAgent.agentId: agent IDs are server-assigned and the field must be omitted"
                .to_string(),
        );
    }
    let input: WorkspaceCreate = serde_json::from_value(Value::Object(params))
        .map_err(|e| format!("invalid proposal params: {e}"))?;
    let created = api
        .create_workspace(input, Some(idempotency_key))
        .await
        .map_err(map_err)?;

    let override_note = match (title_override.is_some(), prompt_override.is_some()) {
        (true, true) => " with overridden title and prompt",
        (true, false) => " with overridden title",
        (false, true) => " with overridden prompt",
        (false, false) => "",
    };
    let detail = format!(
        "Created workspace {} ({}) via ws.workspace.applyProposal{override_note}",
        created.workspace.id.as_str(),
        created.workspace.title
    );
    // The resolver echoes the persisted outcome (no rewrite) when the card was
    // resolved concurrently, so a UI dismissal that landed during create is
    // reported rather than silently overwritten.
    let (outcome, resolve_warning) = match api
        .agent_resolve_proposal(
            ws.clone(),
            caller,
            proposal_id.clone(),
            PROPOSAL_OUTCOME_APPLIED.to_string(),
            Some(detail),
        )
        .await
    {
        Ok(resolved) => match resolved.get("outcome").and_then(Value::as_str) {
            Some(persisted) if persisted != PROPOSAL_OUTCOME_APPLIED => (
                persisted.to_string(),
                Some(format!(
                    "workspace {} exists (it was created by this call), but the proposal had \
                     already been resolved '{persisted}' from the UI while it was being created; \
                     that resolution was kept and the card does not show applied — tell the user \
                     the workspace exists",
                    created.workspace.id.as_str()
                )),
            ),
            _ => (PROPOSAL_OUTCOME_APPLIED.to_string(), None),
        },
        Err(e) => (
            PROPOSAL_OUTCOME_APPLIED.to_string(),
            Some(format!(
                "workspace {} was created but the proposal could not be marked applied: {e}",
                created.workspace.id.as_str()
            )),
        ),
    };

    let mut out = json!({
        "ok": true,
        "proposalId": proposal_id,
        "outcome": outcome,
        "workspace": {
            "id": created.workspace.id.as_str(),
            "title": created.workspace.title,
            "branch": created.workspace.branch,
            "path": created.workspace.effective_path(),
        },
    });
    if let Some(mut agent) = created.initial_agent {
        strip_agent_hidden_fields(&mut agent);
        out["initialAgent"] = agent;
    }
    if title_override.is_some() || prompt_override.is_some() {
        let mut overrides = serde_json::Map::new();
        if title_override.is_some() {
            overrides.insert("title".to_string(), json!(true));
        }
        if prompt_override.is_some() {
            overrides.insert("initialPrompt".to_string(), json!(true));
        }
        out["overrides"] = Value::Object(overrides);
    }
    if let Some(warning) = resolve_warning {
        out["resolveWarning"] = json!(warning);
    }
    Ok(out)
}

async fn info(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    let workspace = api.get_workspace(ws.clone()).await.map_err(map_err)?;
    let path = workspace.effective_path().map(String::from);
    Ok(json!({
        "id": ws.as_str(),
        "path": path,
    }))
}

async fn details(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    match api.get_workspace(ws.clone()).await {
        Ok(w) => {
            let title = w.title.trim();
            let has_title = !title.is_empty() && title != w.id.as_str();
            // Legacy rows persisted before the services-layer clear
            // normalization (and any other writer that still emits `""`
            // or whitespace) can leak an empty string here, which would
            // break the documented clear contract (`empty/null ⇒ null`).
            // Normalize on read so `details()` always surfaces `null`
            // for a cleared status message.
            let status_message = w
                .status_message
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map_or(Value::Null, |s| Value::String(s.to_string()));
            Ok(json!({
                "id": w.id.as_str(),
                "title": if title.is_empty() { "(untitled)" } else { title },
                "hasTitle": has_title,
                "status": w.status,
                "statusMessage": status_message,
                "statusImageAssetId": w.status_image_asset_id,
                "branch": w.branch,
                "repositoryName": w.repository_name,
                "tags": w.tags,
            }))
        }
        Err(Error::NotFound(_)) => Ok(json!({
            "id": ws.as_str(),
            "title": "(untitled)",
            "hasTitle": false,
            "status": WorkspaceStatus::Active,
            "statusMessage": Value::Null,
            "statusImageAssetId": Value::Null,
            "branch": Value::Null,
            "repositoryName": Value::Null,
            "tags": Vec::<String>::new(),
        })),
        Err(e) => Err(e.to_string()),
    }
}

async fn set_title(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let title = req_str(args, "title").map_err(|_| "title is required".to_string())?;
    let trimmed = title.trim().to_string();
    if trimmed.is_empty() {
        return Err("title is required".to_string());
    }
    let existing = api.get_workspace(ws.clone()).await.map_err(map_err)?;
    let existing_title = existing.title.trim();
    if !existing_title.is_empty() && existing_title != existing.id.as_str() {
        return Ok(json!({
            "ok": true,
            "skipped": true,
            "title": existing_title,
            "branch": existing.branch,
        }));
    }
    let update = WorkspaceUpdate {
        title: Some(trimmed.clone()),
        ..Default::default()
    };
    let updated = api
        .update_workspace(ws.clone(), update)
        .await
        .map_err(map_err)?;
    Ok(json!({
        "ok": true,
        "title": updated.title,
        "branch": updated.branch,
    }))
}

async fn set_status_message(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let raw = match args.get("statusMessage") {
        Some(Value::Null) | None => String::new(),
        Some(Value::String(s)) => s.clone(),
        _ => return Err("statusMessage must be a string or null".to_string()),
    };
    let trimmed = raw.trim().to_string();
    // The reference contract (`WORKSPACE_STATUS_MESSAGE_MAX_LENGTH`,
    // `src/shared/types.ts`) is a *character* limit, not a byte limit —
    // count Unicode scalars via `chars()` so multi-byte characters (emoji,
    // CJK, etc.) are not rejected well below 500 characters.
    if trimmed.chars().count() > WORKSPACE_STATUS_MESSAGE_MAX_LENGTH {
        return Err(format!(
            "statusMessage must be {WORKSPACE_STATUS_MESSAGE_MAX_LENGTH} characters or fewer"
        ));
    }
    let update = WorkspaceUpdate {
        status_message: Some(trimmed.clone()),
        ..Default::default()
    };
    let updated = api
        .update_workspace(ws.clone(), update)
        .await
        .map_err(map_err)?;
    // Preserve the `Option<String>` shape end-to-end: `None` maps to
    // `Value::Null`, `Some(v)` to `Value::String(v)`. Never collapse to `""`
    // via `unwrap_or_default()` — that would conflate a cleared value with
    // an explicitly empty string and reintroduce the exact empty-vs-null
    // mismatch the services-side clear normalization is fixing.
    let out = updated.status_message.map_or(Value::Null, Value::String);
    Ok(json!({ "ok": true, "statusMessage": out }))
}

/// `ws.workspace.setStatusImage({ data, mimeType, originalName? } | null)`
/// (intent-hq/monorepo#997 part 1): store an agent-authored status screenshot
/// through the content-addressed asset machinery (`note.saveAsset`) and point
/// `Workspace.statusImageAssetId` at it; `null` clears the reference. The
/// asset write happens BEFORE the workspace update so a failed save never
/// leaves a dangling asset id on the row.
async fn set_status_image(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    chief_guard(ws, "setStatusImage")?;
    // Missing vs explicit `null` matters: a clear is destructive, so a no-arg
    // call (the prelude's JSON.stringify drops `undefined` keys) errors
    // instead of silently clearing — only an explicit `null` clears.
    let Some(image) = args.get("image") else {
        return Err(
            "image is required: pass { data, mimeType, originalName? } to set or null to clear"
                .to_string(),
        );
    };
    if image.is_null() {
        let update = WorkspaceUpdate {
            status_image_asset_id: Some(None),
            ..Default::default()
        };
        api.update_workspace(ws.clone(), update)
            .await
            .map_err(map_err)?;
        return Ok(json!({ "ok": true, "statusImageAssetId": Value::Null }));
    }
    let Some(obj) = image.as_object() else {
        return Err(
            "image must be an object { data, mimeType, originalName? } or null".to_string(),
        );
    };
    let data = obj
        .get("data")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "image.data (base64) is required".to_string())?;
    let mime_type = obj
        .get("mimeType")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "image.mimeType is required".to_string())?;
    if !mime_type.starts_with("image/") {
        return Err(format!(
            "image.mimeType must be an image/* type, got `{mime_type}`"
        ));
    }
    let original_name = obj
        .get("originalName")
        .and_then(Value::as_str)
        .map(str::to_string);
    let saved = api
        .save_asset(
            ws.clone(),
            data.to_string(),
            mime_type.to_string(),
            original_name,
        )
        .await
        .map_err(map_err)?;
    let update = WorkspaceUpdate {
        status_image_asset_id: Some(Some(saved.asset_id.clone())),
        ..Default::default()
    };
    api.update_workspace(ws.clone(), update)
        .await
        .map_err(map_err)?;
    Ok(json!({
        "ok": true,
        "statusImageAssetId": saved.asset_id,
        "url": saved.url,
    }))
}

async fn set_agent_name(
    api: &Arc<dyn WorkspaceApi>,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let name = req_str(args, "name").map_err(|_| "name is required".to_string())?;
    let agent_id = caller_agent_id
        .cloned()
        .ok_or_else(|| "Could not determine agent ID from request context".to_string())?;
    let r = api
        .agent_rename(agent_id, name, true)
        .await
        .map_err(map_err)?;
    Ok(r)
}

/// The chief workspace is virtual and cannot be archived/unarchived. The
/// service methods silently no-op for chief (they return the synthesized
/// `chief_workspace()` unchanged), which would misleadingly look like
/// success to the agent — so the refusal lives here in the binding layer.
fn chief_guard(ws: &WorkspaceId, method: &str) -> Result<(), String> {
    if ws.is_chief() {
        return Err(format!(
            "ws.workspace.{method} is not available in the chief-of-staff workspace"
        ));
    }
    Ok(())
}

/// Whether an [`intent_core::AgentLite`] projection counts as running/queued
/// for the archive guardrail: the daemon-owned in-flight signal
/// (`is_responding`) or a persisted in-flight/queued status. `RuntimeIdle`,
/// `Idle`, `Completed`, `Error`, and `Deleted` sessions do not block.
fn is_running_or_queued(agent: &intent_core::AgentLite) -> bool {
    agent.is_responding
        || matches!(
            agent.status,
            AgentStatus::Pending
                | AgentStatus::Active
                | AgentStatus::Processing
                | AgentStatus::Waiting
        )
}

async fn archive(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
) -> Result<Value, String> {
    chief_guard(ws, "archive")?;
    // Hard refusal when OTHER agents are running/queued — no force override.
    // The calling agent is necessarily mid-turn, so it is excluded.
    let agents = api.agent_list(ws.clone()).await.map_err(map_err)?;
    let blocking: Vec<String> = agents
        .iter()
        .filter(|a| Some(&a.id) != caller_agent_id)
        .filter(|a| is_running_or_queued(a))
        .map(|a| format!("{} ({})", a.name, a.id.as_str()))
        .collect();
    if !blocking.is_empty() {
        return Err(format!(
            "Cannot archive: {} other agent(s) running or queued in this workspace: {}. \
             Wait for them to finish or stop them first.",
            blocking.len(),
            blocking.join(", ")
        ));
    }
    // The caller rides along so the service-layer interrupt sweep skips it:
    // the calling agent is mid-turn awaiting this tool result, and
    // interrupting it would abort the worker owning this dispatch.
    let updated = api
        .archive_workspace(ws.clone(), caller_agent_id.cloned())
        .await
        .map_err(map_err)?;
    Ok(json!({
        "ok": true,
        "status": updated.status,
        "archivedAt": updated.archived_at,
    }))
}

async fn unarchive(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    chief_guard(ws, "unarchive")?;
    let updated = api.unarchive_workspace(ws.clone()).await.map_err(map_err)?;
    Ok(json!({
        "ok": true,
        "status": updated.status,
    }))
}
