//! Transport-agnostic JSON-RPC 2.0 router (PROTOCOL §3, §9).
//!
//! [`handle_message`] takes a single request string and returns the response
//! string, or `None` for notifications (a request without an `id` member).
//! Envelope validation, the notification-vs-request distinction, and the
//! `-32700/-32600/-32601/-32602/-32603` error matrix all live here so every
//! transport (UDS today, WS/TLS later) shares one code path.

use intent_core::{
    AgentCreateExtra, AgentDelegateInput, AgentId, AgentWakeCreateOptions, AgentWakeOrCreateInput,
    ContextItem, Error, EventQueryParams, MessageOrigin, NoteAddInput, NoteCreate, NoteEditInput,
    NoteEditLinesInput, NoteId, NoteUpdateInput, ScriptCreateParams, ScriptMode, TaskAgentLink,
    WorkspaceApi, WorkspaceCreate, WorkspaceGitRootId, WorkspaceId, WorkspaceUpdate,
};
use serde_json::{json, Map, Value};
use tracing::Instrument;

/// Target of the per-dispatch profiling span wrapped around [`dispatch`] in
/// [`handle_message`]. Matched (together with [`RPC_DISPATCH_SPAN_NAME`]) by
/// the statement-count / duration profiling layer installed by the `intentd`
/// composition root, which attributes `sqlx::query` statement events and
/// wall-clock duration to the active RPC and WARNs when either exceeds its
/// budget. Logging only — no wire-contract impact.
pub const RPC_DISPATCH_SPAN_TARGET: &str = "intent_transport::rpc_dispatch";
/// Name of the per-dispatch profiling span (the literal passed to
/// `info_span!` in [`handle_message`]).
pub const RPC_DISPATCH_SPAN_NAME: &str = "rpc_dispatch";

const PARSE_ERROR: i32 = -32700;
const INVALID_REQUEST: i32 = -32600;
const METHOD_NOT_FOUND: i32 = -32601;
const INVALID_PARAMS: i32 = -32602;
/// A serialized response exceeded [`crate::MAX_OUTBOUND_MESSAGE_BYTES`] and was
/// replaced with this error so the client fails fast instead of timing out on
/// a dropped frame.
const OVERSIZED_RESPONSE: i32 = -32010;

/// A JSON-RPC error to surface to the client.
struct RpcErr {
    code: i32,
    message: String,
    data: Option<Value>,
}

fn rpc(code: i32, message: impl Into<String>) -> RpcErr {
    RpcErr {
        code,
        message: message.into(),
        data: None,
    }
}

/// `-32602` for bad or missing request params, carrying the machine-readable
/// discriminator `error.data.code = "invalid-params"` (monorepo#1320).
fn invalid_params(message: impl Into<String>) -> RpcErr {
    RpcErr {
        code: INVALID_PARAMS,
        message: message.into(),
        data: Some(json!({ "code": "invalid-params" })),
    }
}

/// `-32602` for a lookup of an entity that does not exist, carrying the
/// machine-readable discriminator `error.data.code = "not-found"` so clients
/// can distinguish deletion from bad params (monorepo#1320).
fn not_found(message: impl Into<String>) -> RpcErr {
    RpcErr {
        code: INVALID_PARAMS,
        message: message.into(),
        data: Some(json!({ "code": "not-found" })),
    }
}

/// Map a domain [`Error`] to its JSON-RPC representation (§9). Internal errors
/// surface as `-32603 "Internal error"` carrying the original cause in `data`.
fn domain_to_rpc(e: Error) -> RpcErr {
    match e {
        Error::Internal(msg) => RpcErr {
            code: -32603,
            message: "Internal error".to_string(),
            data: Some(Value::String(msg)),
        },
        // Optimistic-concurrency conflict: -32005 carrying the current entity
        // under `data.current` so the client can reconcile (PROTOCOL §4, §5.6).
        Error::Conflict { current } => RpcErr {
            code: -32005,
            message: "Conflict".to_string(),
            data: Some(json!({ "code": "conflict", "current": current })),
        },
        // Unresolvable base ref during checkout provisioning: same -32602 code
        // and human message as before, plus machine-readable data so clients
        // stop matching on prose (monorepo#761).
        ref e @ Error::BaseRefUnresolvable { ref base_ref } => RpcErr {
            code: e.code(),
            message: e.to_string(),
            data: Some(json!({ "code": "base-ref-unresolvable", "baseRef": base_ref })),
        },
        // Classified clone/provisioning failure: human message carries the
        // sanitized git stderr tail, `data.code` carries the machine-readable
        // category so clients stop showing a bare "Internal error"
        // (monorepo#826).
        ref e @ Error::CloneFailed {
            category,
            ref detail,
        } => RpcErr {
            code: e.code(),
            message: e.to_string(),
            data: Some(json!({ "code": category.as_str(), "detail": detail })),
        },
        // Missing voice provider API key: same -32603 code and "Internal
        // error" message as before (deliberately not the variant's Display,
        // which carries the detail), plus machine-readable `data.code` with
        // the descriptive text preserved in `data.detail` (monorepo#1448).
        ref e @ Error::VoiceNotConfigured { ref detail } => RpcErr {
            code: e.code(),
            message: "Internal error".to_string(),
            data: Some(json!({ "code": "voice-no-api-key", "detail": detail })),
        },
        // `git.showFile` on a non-blob tree entry (gitlink / tree): -32602
        // with machine-readable `data = { code: "not-a-file", path, mode }`
        // so clients can route submodule pins to a dedicated presentation
        // instead of matching on prose (monorepo#1739).
        ref e @ Error::NotAFile { ref path, ref mode } => RpcErr {
            code: e.code(),
            message: e.to_string(),
            data: Some(json!({ "code": "not-a-file", "path": path, "mode": mode })),
        },
        // Opportunistic warm rejected because one is already in flight:
        // -32603 with machine-readable `data = { code: "warm-in-flight",
        // owner, repo }` naming the repo currently being warmed, so the FE
        // can stay silent without matching on prose.
        ref e @ Error::WarmInFlight {
            ref owner,
            ref repo,
        } => RpcErr {
            code: e.code(),
            message: e.to_string(),
            data: Some(json!({ "code": "warm-in-flight", "owner": owner, "repo": repo })),
        },
        // `agent.completeOnce` queued past its own timeout at the daemon-wide
        // ephemeral-adapter bound: -32603 with machine-readable
        // `data = { code: "adapter-busy", provider, waitedMs, limit }` so a
        // client tells daemon saturation apart from a slow model — and knows
        // the retry is safe, since nothing was spawned (monorepo#2062).
        ref e @ Error::AdapterBusy {
            ref provider,
            waited_ms,
            limit,
        } => RpcErr {
            code: e.code(),
            message: e.to_string(),
            data: Some(json!({
                "code": "adapter-busy",
                "provider": provider,
                "waitedMs": waited_ms,
                "limit": limit,
            })),
        },
        // -32602 discriminator (monorepo#1320): `data.code` distinguishes a
        // nonexistent entity from bad request params; messages are unchanged.
        e @ Error::NotFound(_) => not_found(e.to_string()),
        e @ (Error::InvalidParams(_) | Error::InvalidInput(_)) => invalid_params(e.to_string()),
        other => RpcErr {
            code: other.code(),
            message: other.to_string(),
            data: None,
        },
    }
}

/// Classification of one parsed JSON-RPC envelope — the single implementation
/// of the §3/§9 envelope-validity rules. Consumed by [`handle_message`], which
/// maps each failure to its exact `-32600` message, and by the dispatch
/// pre-check in `conn.rs`, which only needs valid-vs-not.
pub(crate) enum EnvelopeCheck<'a> {
    /// The frame is JSON but not an object (answered with id `null`).
    NotObject,
    /// `jsonrpc` is missing or not exactly `"2.0"`. The request id is echoed
    /// only when its type is valid, else `null`.
    BadJsonRpc { echo_id: Value },
    /// `method` is missing, not a string, or empty (same id-echo rule as
    /// [`EnvelopeCheck::BadJsonRpc`]).
    BadMethod { echo_id: Value },
    /// `id` is present but not a string, number, or null — an invalid id is
    /// never echoed, so the response id is `null`.
    BadId,
    /// The envelope is valid and may be dispatched. `is_notification` is true
    /// when the `id` member is absent entirely (`id: null` is a request).
    Valid {
        echo_id: Value,
        method: &'a str,
        is_notification: bool,
    },
}

/// Classify a parsed frame against the envelope-validity rules (§3, §9):
/// `jsonrpc` must be exactly `"2.0"`, `method` a non-empty string, and `id` —
/// when present — a string, number, or null. Failures are reported in
/// jsonrpc → method → id order, matching the message precedence in
/// [`handle_message`].
pub(crate) fn check_envelope(value: &Value) -> EnvelopeCheck<'_> {
    let Some(obj) = value.as_object() else {
        return EnvelopeCheck::NotObject;
    };
    let id_member = obj.get("id");
    let id_type_ok = match id_member {
        None => true,
        Some(v) => v.is_string() || v.is_number() || v.is_null(),
    };
    let echo_id = match id_member {
        Some(v) if id_type_ok => v.clone(),
        _ => Value::Null,
    };
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return EnvelopeCheck::BadJsonRpc { echo_id };
    }
    let method = obj
        .get("method")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty());
    let Some(method) = method else {
        return EnvelopeCheck::BadMethod { echo_id };
    };
    if !id_type_ok {
        return EnvelopeCheck::BadId;
    }
    EnvelopeCheck::Valid {
        echo_id,
        method,
        is_notification: id_member.is_none(),
    }
}

/// Handle one JSON-RPC frame. Returns `Some(response)` for requests and `None`
/// for notifications (including unknown / failed ones, per §3.4).
pub async fn handle_message(api: &dyn WorkspaceApi, message: &str) -> Option<String> {
    let value: Value = match serde_json::from_str(message) {
        Ok(v) => v,
        // Parse errors are always answered with id null (§9), even for
        // would-be notifications — notification status is not yet known.
        Err(_) => return Some(error_string(&Value::Null, PARSE_ERROR, "Parse error", None)),
    };

    // Envelope validation (-32600). Answered even for notification-shaped
    // frames: notification status is not trusted until the envelope is valid.
    let (echo_id, method, is_notification) = match check_envelope(&value) {
        EnvelopeCheck::NotObject => {
            return Some(error_string(
                &Value::Null,
                INVALID_REQUEST,
                "Invalid Request: expected an object",
                None,
            ))
        }
        EnvelopeCheck::BadJsonRpc { echo_id } => {
            return Some(error_string(
                &echo_id,
                INVALID_REQUEST,
                "Invalid Request: jsonrpc must be \"2.0\"",
                None,
            ))
        }
        EnvelopeCheck::BadMethod { echo_id } => {
            return Some(error_string(
                &echo_id,
                INVALID_REQUEST,
                "Invalid Request: method must be a non-empty string",
                None,
            ))
        }
        EnvelopeCheck::BadId => {
            return Some(error_string(
                &Value::Null,
                INVALID_REQUEST,
                "Invalid Request: id must be a string, number, or null",
                None,
            ))
        }
        EnvelopeCheck::Valid {
            echo_id,
            method,
            is_notification,
        } => (echo_id, method, is_notification),
    };

    // params: object kept as-is; positional array coerced to {}; absent/null
    // treated as empty; any other scalar is invalid (§3.1).
    let params: Map<String, Value> = match value.get("params") {
        Some(Value::Object(m)) => m.clone(),
        None | Some(Value::Null | Value::Array(_)) => Map::new(),
        Some(_) => {
            if is_notification {
                return None;
            }
            return Some(error_string(
                &echo_id,
                INVALID_PARAMS,
                "Invalid params",
                Some(json!({ "code": "invalid-params" })),
            ));
        }
    };

    // Per-dispatch profiling span: carries the method name so the composition
    // root's profiling layer can count `sqlx::query` statement events scoped
    // to this dispatch and time the handler (see RPC_DISPATCH_SPAN_TARGET).
    let span =
        tracing::info_span!(target: RPC_DISPATCH_SPAN_TARGET, RPC_DISPATCH_SPAN_NAME, method);
    let result = dispatch(api, method, &params).instrument(span).await;

    // Notifications never get a response, even on error / unknown method (§3.4).
    if is_notification {
        return None;
    }
    // The log-only large-frame warning for outbound responses lives in
    // `panic_guard::guard_frame` (the chokepoint covering fast-path responses
    // that bypass this dispatcher, e.g. `host.exec`). The `-32010`
    // replacement below hands a small error frame to that check, so an
    // oversized response is never double-warned on top of its `error!`.
    Some(match result {
        Ok(v) => {
            let frame = success_string(&echo_id.clone(), &v);
            if frame.len() > crate::MAX_OUTBOUND_MESSAGE_BYTES {
                oversized_response_string(&echo_id, method, frame.len())
            } else {
                frame
            }
        }
        Err(e) => error_string(&echo_id, e.code, &e.message, e.data),
    })
}

/// Dispatch a validated request to the injected [`WorkspaceApi`].
async fn dispatch(
    api: &dyn WorkspaceApi,
    method: &str,
    params: &Map<String, Value>,
) -> Result<Value, RpcErr> {
    match method {
        "workspace.list" => {
            let include_archived = params
                .get("includeArchived")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let workspaces = api
                .list_workspaces(include_archived)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({ "workspaces": workspaces }))
        }
        "workspace.get" => {
            let id = require_workspace_id(params)?;
            let ws = api.get_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.create" => {
            // Agent ids are server-assigned: reject stale clients that still
            // send `initialAgent.agentId` before any provisioning runs.
            if params
                .get("initialAgent")
                .and_then(|a| a.get("agentId"))
                .is_some_and(|v| !v.is_null())
            {
                return Err(invalid_params(
                    "initialAgent.agentId: agent IDs are server-assigned and the field must be omitted",
                ));
            }
            let idempotency_key = opt_str(params, "idempotencyKey");
            let input: WorkspaceCreate = serde_json::from_value(Value::Object(params.clone()))
                .map_err(|e| invalid_params(format!("invalid params: {e}")))?;
            let res = api
                .create_workspace(input, idempotency_key)
                .await
                .map_err(workspace_err)?;
            let mut result = json!({ "workspace": res.workspace });
            if let Some(agent) = res.initial_agent {
                result["initialAgent"] = agent;
            }
            Ok(result)
        }
        "workspace.update" => {
            let id = require_workspace_id(params)?;
            let mut rest = params.clone();
            rest.remove("workspaceId");
            let update: WorkspaceUpdate = serde_json::from_value(Value::Object(rest))
                .map_err(|e| invalid_params(format!("invalid params: {e}")))?;
            let ws = api
                .update_workspace(id, update)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.delete" => {
            let id = require_workspace_id(params)?;
            // Delete grace window (§5.1): `undoDelayMs > 0` schedules an
            // in-memory pending deletion instead of committing now. Absent
            // or 0 keeps the immediate-delete behavior byte-identical.
            let undo_delay_ms = match params.get("undoDelayMs") {
                None | Some(Value::Null) => 0,
                Some(v) => v.as_u64().ok_or_else(|| {
                    invalid_params("Invalid parameter: undoDelayMs must be a non-negative integer")
                })?,
            };
            if undo_delay_ms > 0 {
                let delete_at = api
                    .schedule_workspace_delete(id, undo_delay_ms)
                    .await
                    .map_err(workspace_err)?;
                Ok(json!({ "success": true, "scheduled": true, "deleteAt": delete_at }))
            } else {
                api.delete_workspace(id).await.map_err(workspace_err)?;
                Ok(json!({ "success": true }))
            }
        }
        "workspace.cancelDelete" => {
            let id = require_workspace_id(params)?;
            let cancelled = api
                .cancel_workspace_delete(id)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "cancelled": cancelled }))
        }
        "workspace.archive" => {
            let id = require_workspace_id(params)?;
            let ws = api
                .archive_workspace(id, None)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.unarchive" => {
            let id = require_workspace_id(params)?;
            let ws = api.unarchive_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.diskUsage" => {
            let id = require_workspace_id(params)?;
            let result = api.workspace_disk_usage(id).await.map_err(workspace_err)?;
            Ok(result)
        }
        "workspace.transfer.plan" => {
            let id = require_workspace_id(params)?;
            let plan = api
                .workspace_transfer_plan(id)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "plan": plan }))
        }
        "workspace.import.begin" => {
            let manifest = params
                .get("manifest")
                .cloned()
                .ok_or_else(|| invalid_params("manifest is required"))?;
            let archive_size_bytes = require_u64(params, "archiveSizeBytes")?;
            let archive_sha256 = require_str_param(params, "archiveSha256")?;
            api.workspace_import_begin(manifest, archive_size_bytes, archive_sha256)
                .await
                .map_err(workspace_err)
        }
        "workspace.import.chunk" => {
            let import_id = require_str_param(params, "importId")?;
            let seq = require_u64(params, "seq")?;
            let data = require_str_param(params, "data")?;
            api.workspace_import_chunk(import_id, seq, data)
                .await
                .map_err(workspace_err)
        }
        "workspace.import.commit" => {
            let import_id = require_str_param(params, "importId")?;
            api.workspace_import_commit(import_id)
                .await
                .map_err(workspace_err)
        }
        "workspace.import.abort" => {
            let import_id = require_str_param(params, "importId")?;
            api.workspace_import_abort(import_id)
                .await
                .map_err(workspace_err)
        }
        "workspace.export.start" => {
            let id = require_workspace_id(params)?;
            api.workspace_export_start(id).await.map_err(workspace_err)
        }
        "workspace.export.read" => {
            let export_id = require_str_param(params, "exportId")?;
            let seq = require_u64(params, "seq")?;
            api.workspace_export_read(export_id, seq)
                .await
                .map_err(workspace_err)
        }
        "workspace.export.finalize" => {
            let export_id = require_str_param(params, "exportId")?;
            let archive_source = opt_bool_strict(params, "archiveSource")?.unwrap_or(false);
            let final_status_message = opt_str(params, "finalStatusMessage");
            api.workspace_export_finalize(export_id, archive_source, final_status_message)
                .await
                .map_err(workspace_err)
        }
        "workspace.export.abort" => {
            let export_id = require_str_param(params, "exportId")?;
            api.workspace_export_abort(export_id)
                .await
                .map_err(workspace_err)
        }
        "workspace.dismissAttention" => {
            let id = require_workspace_id(params)?;
            let ws = api.dismiss_attention(id).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.markSeen" => {
            let id = require_workspace_id(params)?;
            let ws = api.mark_seen(id).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.getTokenUsage" => {
            let id = require_workspace_id(params)?;
            let token_usage = api.get_token_usage(id).await.map_err(workspace_err)?;
            Ok(json!({ "tokenUsage": token_usage }))
        }
        "workspace.getAutoCommit" => {
            let id = require_workspace_id(params)?;
            let auto_commit = api
                .get_workspace_auto_commit(id)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "autoCommit": auto_commit }))
        }
        "workspace.setAutoCommit" => {
            let id = require_workspace_id(params)?;
            let enabled = match params.get("enabled") {
                Some(Value::Bool(b)) => *b,
                Some(_) => {
                    return Err(invalid_params(
                        "Invalid parameter: enabled must be a boolean",
                    ))
                }
                None => {
                    return Err(invalid_params(
                        "Missing required parameter: enabled (boolean)",
                    ))
                }
            };
            let auto_commit = api
                .set_workspace_auto_commit(id, enabled)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "autoCommit": auto_commit }))
        }
        "workspace.getSetupScript" => {
            let id = require_workspace_id(params)?;
            let setup_script = api.get_setup_script(id).await.map_err(workspace_err)?;
            Ok(json!({ "setupScript": setup_script }))
        }
        "workspace.saveSetupScript" => {
            let id = require_workspace_id(params)?;
            let script = require_str_param(params, "script")?;
            let setup_script = api
                .save_setup_script(id, script)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "setupScript": setup_script }))
        }
        "workspace.detectProjectType" => {
            let id = require_workspace_id(params)?;
            let project_type = api.detect_project_type(id).await.map_err(workspace_err)?;
            Ok(json!({ "projectType": project_type }))
        }
        "workspace.generateSetupScript" => {
            let id = require_workspace_id(params)?;
            let setup_script = api.generate_setup_script(id).await.map_err(workspace_err)?;
            Ok(json!({ "setupScript": setup_script }))
        }
        "repoConfig.get" => {
            let id = require_workspace_id(params)?;
            let config = api.get_repo_config(id).await.map_err(workspace_err)?;
            Ok(json!({ "config": config }))
        }
        "repoConfig.save" => {
            let id = require_workspace_id(params)?;
            let config_value = params
                .get("config")
                .ok_or_else(|| invalid_params("Missing required parameter: config"))?;
            // Keep the raw JSON object so the service can distinguish absent
            // keys (preserve) from explicit `null` (clear) when merging.
            let patch = config_value
                .as_object()
                .cloned()
                .ok_or_else(|| invalid_params("invalid config: expected a JSON object"))?;
            // Validate field types up front so malformed payloads fail with -32602.
            serde_json::from_value::<intent_core::RepoConfig>(Value::Object(patch.clone()))
                .map_err(|e| invalid_params(format!("invalid config: {e}")))?;
            let saved_config = api
                .save_repo_config(id, patch)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "config": saved_config }))
        }
        "repoConfig.has" => {
            let id = require_workspace_id(params)?;
            let exists = api.has_repo_config(id).await.map_err(workspace_err)?;
            Ok(json!({ "exists": exists }))
        }
        "repoConfig.ensureDir" => {
            let id = require_workspace_id(params)?;
            api.ensure_repo_intent_dir(id)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "ok": true }))
        }
        "workspace.getContext" => {
            let id = require_workspace_id(params)?;
            let items = api.get_workspace_context(id).await.map_err(workspace_err)?;
            Ok(json!({ "items": items }))
        }
        "workspace.updateContext" => {
            let id = require_workspace_id(params)?;
            let items = require_context_items(params)?;
            let items = api
                .update_workspace_context(id, items)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "items": items }))
        }
        "workspace.getUiContext" => {
            let id = require_workspace_id(params)?;
            let ui_context = api
                .get_workspace_ui_context(id)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "uiContext": ui_context }))
        }
        "workspace.updateUiContext" => {
            let id = require_workspace_id(params)?;
            let ui_context = params
                .get("uiContext")
                .ok_or_else(|| invalid_params("uiContext required"))?
                .clone();
            let persisted = api
                .update_workspace_ui_context(id, ui_context)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "uiContext": persisted }))
        }
        "workspace.duplicate" => {
            let id = require_workspace_id(params)?;
            let new_title = opt_str(params, "newTitle");
            let ws = api
                .duplicate_workspace(id, new_title)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.restore" => {
            let id = require_workspace_id(params)?;
            let ws = api.restore_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "workspace": ws }))
        }
        "workspace.cleanup" => {
            let id = require_workspace_id(params)?;
            api.cleanup_workspace(id).await.map_err(workspace_err)?;
            Ok(json!({ "success": true }))
        }
        "workspace.findRepositories" => {
            let directory = require_str_param(params, "directory")?;
            let repositories = api
                .find_repositories(directory)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({ "repositories": repositories }))
        }
        "workspace.initializeRepository" => {
            let path = require_str_param(params, "path")?;
            api.initialize_repository(path)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({ "success": true }))
        }
        "note.list" => {
            let ws_id = match params.get("workspaceId").and_then(Value::as_str) {
                Some(s) if !s.is_empty() => WorkspaceId::from(s),
                _ => return Err(invalid_params("workspaceId is required")),
            };
            let notes = api.list_notes(&ws_id).await.map_err(domain_to_rpc)?;
            Ok(json!({ "notes": notes }))
        }
        "note.get" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            match api.get_note(ws, note_id).await {
                Ok(note) => Ok(json!({ "note": note })),
                Err(Error::NotFound(_)) => Err(not_found("Note not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "note.create" => {
            let ws = require_ws_note(params)?;
            let title = require_str_param(params, "title")?;
            let idempotency_key = opt_str(params, "idempotencyKey");
            let input = NoteCreate {
                title,
                content: opt_str(params, "content"),
                tags: opt_tags(params, "tags"),
                parent_id: opt_str(params, "parentId"),
            };
            // Transport (UDS + WSS) is the user-originated JSON-RPC path; no
            // caller-agent context is threaded here, so note-version author
            // resolves to the user. Agent writes come through MCP bindings.
            // Result is `{note, convertedCount, createdTaskNoteIds,
            // createdTasks, warnings}` — additive over the old `{note}` shape.
            let result = api
                .create_note(ws, input, idempotency_key, None)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.update" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let input = NoteUpdateInput {
                content: opt_str(params, "content"),
                title: opt_str(params, "title"),
                tags: opt_tags(params, "tags"),
                expected_version: opt_int(params, "expectedVersion"),
            };
            let note = api
                .update_note(ws, note_id, input)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({ "note": note }))
        }
        "note.add" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let content = require_str_param(params, "content")?;
            let input = NoteAddInput {
                content,
                heading: opt_str(params, "heading"),
                position: opt_str(params, "position"),
            };
            let result = api
                .add_to_note(ws, note_id, input, None)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.edit" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let old = require_str_param(params, "old")?;
            let new = require_str_param(params, "new")?;
            let input = NoteEditInput { old, new };
            let result = api
                .edit_note(ws, note_id, input, None)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.editLines" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            require_present(params, "start")?;
            require_present(params, "end")?;
            let content = require_str_param(params, "content")?;
            // Non-numeric/absent coerce to 0 so the service emits the TS
            // "must be a positive integer" message.
            let start = parse_int_loose(params.get("start")).unwrap_or(0);
            let end = parse_int_loose(params.get("end")).unwrap_or(0);
            let input = NoteEditLinesInput {
                start,
                end,
                content,
            };
            let result = api
                .edit_note_lines(ws, note_id, input, None)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.setContent" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let content = require_str_param(params, "content")?;
            let confirm = parse_confirm(params);
            let expected_version = opt_int(params, "expectedVersion");
            let result = api
                .set_note_content(ws, note_id, content, confirm, expected_version, None)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.updateMetadata" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let title = opt_str(params, "title");
            let tags = opt_tags(params, "tags");
            let expected_version = opt_int(params, "expectedVersion");
            let result = api
                .update_note_metadata(ws, note_id, title, tags, expected_version, None)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.delete" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let expected_version = opt_int(params, "expectedVersion");
            let result = api
                .delete_note(ws, note_id, expected_version)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.listTasks" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let tasks = api
                .list_note_tasks(ws, note_id)
                .await
                .map_err(domain_to_rpc)?;
            // The TS peer returns a bare array.
            to_result_value(&tasks)
        }
        "note.readAsset" => {
            let ws = require_ws_note(params)?;
            let asset = require_str_param(params, "asset")?;
            let result = api.read_asset(ws, asset).await.map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.saveAsset" => {
            let ws = require_ws_note(params)?;
            let data = require_str_param(params, "data")?;
            let mime_type = require_str_param(params, "mimeType")?;
            let original_name = opt_str(params, "originalName");
            let result = api
                .save_asset(ws, data, mime_type, original_name)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.listVersions" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let versions = api
                .list_note_versions(ws, note_id)
                .await
                .map_err(domain_to_rpc)?;
            // Bare array, like `note.listTasks`.
            to_result_value(&versions)
        }
        "note.getVersion" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let v = require_int_param(params, "v")?;
            let version = api
                .get_note_version(ws, note_id, v)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&version)
        }
        "note.restoreVersion" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let v = require_int_param(params, "v")?;
            let result = api
                .restore_note_version(ws, note_id, v, None)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "note.lineAttribution.load" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let data = api
                .line_attribution_load(ws, note_id)
                .await
                .map_err(domain_to_rpc)?;
            // Bare LineAttributionData | null so the FE gutter’s
            // `line-attribution:load` decoder round-trips as-is.
            to_result_value(&data)
        }
        "note.lineAttribution.computeNow" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let result = api
                .line_attribution_compute_now(ws, note_id)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.updateStatus" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let task_text = require_str_param(params, "taskText")?;
            let status = require_str_param(params, "status")?;
            let result = api
                .task_update_status(ws, note_id, task_text, status)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.updateNoteStatus" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let status = require_str_param(params, "status")?;
            let expected_version = opt_int(params, "expectedVersion");
            // FE/RPC front door: no agent provenance (the MCP path passes the
            // caller agent so `task:status-changed` carries `agentId`).
            let result = api
                .task_update_note_status(ws, note_id, status, expected_version, None)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.update" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            require_present(params, "line")?;
            // Non-numeric/absent coerce to 0 so the service emits the TS
            // "Line number must be a positive integer" message.
            let line = parse_int_loose(params.get("line")).unwrap_or(0);
            let text = opt_str(params, "text");
            let status = opt_str(params, "status");
            let expected = opt_str(params, "expected");
            let result = api
                .task_update(ws, note_id, line, text, status, expected)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.getMyTask" => {
            let ws = require_ws_note(params)?;
            let task_note_id = require_str_param(params, "taskNoteId").map(NoteId::from)?;
            let result = api
                .get_my_task(ws, task_note_id)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.markAsTask" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let status = require_str_param(params, "status")?;
            let acceptance_criteria = normalize_acceptance_criteria(params);
            let effort = opt_str(params, "effort");
            let depends_on = opt_note_id_array(params, "dependsOn");
            let conflicts_with = opt_note_id_array(params, "conflictsWith");
            let result = api
                .mark_as_task(
                    ws,
                    note_id,
                    status,
                    acceptance_criteria,
                    effort,
                    depends_on,
                    conflicts_with,
                    None,
                )
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.setRelations" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let depends_on = opt_note_id_array(params, "dependsOn");
            let conflicts_with = opt_note_id_array(params, "conflictsWith");
            let result = api
                .task_set_relations(ws, note_id, depends_on, conflicts_with)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.convertBlocks" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let result = api
                .convert_task_blocks(ws, note_id, None)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.createPrerequisite" => {
            let ws = require_ws_note(params)?;
            let dependent_note_id =
                require_str_param(params, "dependentNoteId").map(NoteId::from)?;
            let title = require_str_param(params, "title")?;
            let content = opt_str(params, "content");
            let status = opt_str(params, "status");
            let result = api
                .create_prerequisite(ws, dependent_note_id, title, content, status, None)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.assignAgent" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let agent_id = require_str_param(params, "agentId")?;
            let force = opt_bool(params, "force");
            let result = api
                .assign_agent(ws, note_id, agent_id, force)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.removeAgentFromAllTasks" => {
            let ws = require_ws_note(params)?;
            let agent_id = require_str_param(params, "agentId").map(AgentId::from)?;
            let result = api
                .remove_agent_from_all_tasks(ws, agent_id)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "task.list" => {
            let ws = require_ws_note(params)?;
            let status = opt_str(params, "status");
            let result = api.task_list(ws, status).await.map_err(domain_to_rpc)?;
            Ok(json!({ "tasks": result.tasks, "stats": result.stats }))
        }
        "task.get" => {
            let ws = require_ws_note(params)?;
            let task_note_id = require_str_param(params, "taskNoteId").map(NoteId::from)?;
            match api.task_get(ws, task_note_id).await {
                Ok(task) => Ok(json!({ "task": task })),
                Err(Error::NotFound(_)) => Err(not_found("Task not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "task.linkAgent" => {
            let workspace_id = require_workspace_id(params)?;
            let note_id = require_note_id(params)?;
            let task_text = require_non_empty_str(params, "taskText")?;
            let agent_id = require_non_empty_str(params, "agentId")?;
            let task_key = opt_nonempty_str(params, "taskKey").unwrap_or_else(|| task_text.clone());
            let link = api
                .link_task_agent(workspace_id, note_id, task_key, task_text, agent_id)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "link": link }))
        }
        "task.unlinkAgent" => {
            let workspace_id = require_workspace_id(params)?;
            let note_id = require_note_id(params)?;
            let task_key = require_non_empty_str(params, "taskKey")?;
            let removed = api
                .unlink_task_agent(workspace_id, note_id, task_key)
                .await
                .map_err(workspace_err)?;
            Ok(json!({ "removed": removed }))
        }
        "task.listAgentLinks" => {
            let workspace_id = require_workspace_id(params)?;
            let links = api
                .list_task_agent_links(workspace_id)
                .await
                .map_err(workspace_err)?;
            Ok(json!({
                "links": links,
                "linksByNoteId": links_by_note_id(&links),
            }))
        }
        "comment.add" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let search_context = require_str_param(params, "searchContext")?;
            let comment_target = require_str_param(params, "commentTarget")?;
            let comment = require_str_param(params, "comment")?;
            let kind = opt_str(params, "type");
            let author = opt_str(params, "author");
            let author_type = opt_str(params, "authorType");
            let idempotency_key = opt_nonempty_str(params, "idempotencyKey");
            let comment_id = opt_str(params, "commentId");
            let result = api
                .comment_add(
                    ws,
                    note_id,
                    search_context,
                    comment_target,
                    comment,
                    kind,
                    author,
                    author_type,
                    idempotency_key,
                    comment_id,
                )
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "comment.list" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let since = opt_str(params, "since");
            let author_type = opt_str(params, "authorType");
            let status = opt_str(params, "status");
            let include_comments = parse_bool(params, "includeComments");
            let result = api
                .comment_list(ws, note_id, since, author_type, status, include_comments)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "comment.getThread" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let thread_id = opt_str(params, "threadId");
            let comment_id = opt_str(params, "commentId");
            let result = api
                .comment_get_thread(ws, note_id, thread_id, comment_id)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "comment.respond" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let comment = require_str_param(params, "comment")?;
            let thread_id = opt_str(params, "threadId");
            let comment_id = opt_str(params, "commentId");
            let kind = opt_str(params, "type");
            let author = opt_str(params, "author");
            let author_type = opt_str(params, "authorType");
            let suggestion_original = opt_str(params, "suggestionOriginal");
            let suggestion_proposed = opt_str(params, "suggestionProposed");
            let result = api
                .comment_respond(
                    ws,
                    note_id,
                    thread_id,
                    comment_id,
                    comment,
                    kind,
                    author,
                    author_type,
                    suggestion_original,
                    suggestion_proposed,
                )
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "comment.delete" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let comment_id = require_str_param(params, "commentId")?;
            let result = api
                .comment_delete(ws, note_id, comment_id)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "comment.resolveThread" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let thread_id = opt_str(params, "threadId");
            let comment_id = opt_str(params, "commentId");
            let resolved = opt_bool(params, "resolved").unwrap_or(true);
            let result = api
                .comment_resolve_thread(ws, note_id, thread_id, comment_id, resolved)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "event.agentActivity" => {
            let ws = require_ws_note(params)?;
            let agent_id = opt_str(params, "agentId");
            let minutes_ago = opt_int(params, "minutesAgo");
            let result = api
                .event_agent_activity(ws, agent_id, minutes_ago)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "event.workspaceSummary" => {
            let ws = require_ws_note(params)?;
            let result = api
                .event_workspace_summary(ws, opt_int(params, "minutesAgo"))
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&result)
        }
        "event.query" => {
            let ws = require_ws_note(params)?;
            let query = EventQueryParams {
                event_type: opt_str(params, "eventType"),
                actor_type: opt_str(params, "actorType"),
                actor_id: opt_str(params, "actorId"),
                path: opt_str(params, "path"),
                minutes_ago: opt_int(params, "minutesAgo"),
                limit: opt_int(params, "limit"),
                paginate: params.get("paginate").and_then(Value::as_bool),
                page_token: opt_str(params, "nextToken"),
            };
            api.event_query(ws, query).await.map_err(domain_to_rpc)
        }
        "agent.list" => {
            let ws = require_ws_note(params)?;
            let agents = api.agent_list(ws).await.map_err(domain_to_rpc)?;
            Ok(json!({ "agents": agents }))
        }
        "agent.listActive" => api.agent_list_active().await.map_err(domain_to_rpc),
        "agent.get" => {
            let agent_id = require_agent_id(params)?;
            let ws = opt_workspace_id(params);
            match api.agent_get(agent_id, ws).await {
                Ok(agent) => Ok(json!({ "agent": agent })),
                Err(Error::NotFound(_)) => Err(not_found("Agent not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "agent.getConversation" => {
            let agent_id = require_agent_id(params)?;
            let limit = opt_int(params, "limit");
            let ws = opt_workspace_id(params);
            let page_token = opt_str(params, "nextToken");
            // Additive seek params (§5.5): `aroundMessageId` — the page
            // containing this message (unknown ids surface as `-32602`
            // naming the id via domain_to_rpc; empty/whitespace ids are
            // treated as absent) — and `aroundIndex` — the page containing
            // this 0-based ordinal from the oldest message (out-of-range
            // clamps in the service; negative or non-integer is `-32602`).
            // Supplying both is `-32602` naming the conflict.
            let around_message_id =
                opt_str(params, "aroundMessageId").filter(|s| !s.trim().is_empty());
            let around_index = match params.get("aroundIndex") {
                None | Some(Value::Null) => None,
                Some(v) => match v.as_i64() {
                    Some(i) if i >= 0 => Some(i),
                    // Integers beyond i64::MAX are still valid overshoots —
                    // clamp them like any other out-of-range estimate.
                    None if v.as_u64().is_some() => Some(i64::MAX),
                    _ => return Err(invalid_params("aroundIndex must be a non-negative integer")),
                },
            };
            if around_message_id.is_some() && around_index.is_some() {
                return Err(invalid_params(
                    "aroundMessageId and aroundIndex are mutually exclusive",
                ));
            }
            // Additive `projection` param (§5.5): absent / null keeps the
            // response byte-identical to before; `"slim"` bounds tool/image
            // block bodies; any other value is `-32602` (a silently ignored
            // typo would hand the client full-size frames it opted out of).
            let projection = parse_projection(params)?;
            match api
                .agent_get_conversation(
                    agent_id,
                    limit,
                    ws,
                    page_token,
                    around_message_id,
                    around_index,
                    projection,
                )
                .await
            {
                Ok(v) => Ok(v),
                Err(Error::NotFound(_)) => Err(not_found("Agent not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "agent.getMessageBlock" => {
            let agent_id = require_agent_id(params)?;
            let message_id = require_str_param(params, "messageId")?;
            let block_id = require_str_param(params, "blockId")?;
            let ws = opt_workspace_id(params);
            match api
                .agent_get_message_block(agent_id, message_id, block_id, ws)
                .await
            {
                Ok(v) => Ok(v),
                Err(Error::NotFound(_)) => Err(not_found("Agent not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "agent.getSessionStats" => {
            let session_id =
                require_str_param(params, "sessionId").map(|s| AgentId::from(s.as_str()))?;
            let ws = opt_workspace_id(params);
            match api.agent_get_session_stats(session_id, ws).await {
                Ok(v) => Ok(v),
                Err(Error::NotFound(_)) => Err(not_found("Session not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "agent.getSession" => {
            let agent_id = require_agent_id(params)?;
            let ws = opt_workspace_id(params);
            match api.agent_get_session(agent_id, ws).await {
                Ok(session) => Ok(json!({ "session": session })),
                Err(Error::NotFound(_)) => Err(not_found("Agent not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "agent.update" => {
            let agent_id = require_agent_id(params)?;
            let ws = opt_workspace_id(params);
            let changes = params
                .get("changes")
                .cloned()
                .ok_or_else(|| invalid_params("Missing required parameter: changes"))?;
            match api.agent_update(agent_id, ws, changes).await {
                Ok(v) => Ok(v),
                Err(Error::NotFound(_)) => Err(not_found("Agent not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "agent.appendMessage" => {
            let agent_id = require_agent_id(params)?;
            let ws = opt_workspace_id(params);
            let role = require_str_param(params, "role")?;
            let content = params
                .get("contentBlocks")
                .or_else(|| params.get("content"))
                .cloned()
                .ok_or_else(|| invalid_params("Missing required parameter: contentBlocks"))?;
            let metadata = opt_value(params, "metadata");
            match api
                .agent_append_message(agent_id, ws, role, content, metadata)
                .await
            {
                Ok(v) => Ok(v),
                Err(Error::NotFound(_)) => Err(not_found("Agent not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "agent.replaceMessages" => {
            let agent_id = require_agent_id(params)?;
            let ws = opt_workspace_id(params);
            let messages = params
                .get("messages")
                .cloned()
                .ok_or_else(|| invalid_params("Missing required parameter: messages"))?;
            match api.agent_replace_messages(agent_id, ws, messages).await {
                Ok(v) => Ok(v),
                Err(Error::NotFound(_)) => Err(not_found("Agent not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "agent.create" => {
            // Agent ids are server-assigned: reject stale clients that still
            // send `agentId` before the request reaches the service (checked
            // ahead of every other param so the stale-client signal is
            // unambiguous even on otherwise-malformed requests).
            if params.get("agentId").is_some_and(|v| !v.is_null()) {
                return Err(invalid_params(
                    "agentId: agent IDs are server-assigned and the field must be omitted",
                ));
            }
            let ws = require_ws_note(params)?;
            let name = opt_str(params, "name");
            let model = opt_str(params, "model");
            let specialist_id = opt_str(params, "specialistId");
            let idempotency_key = opt_str(params, "idempotencyKey");
            // Widened FE-facing spawn hints (P2-12a): `provider`/`agentType`/
            // `metadata`/`workspacePath`/`workspaceContext`. All optional and
            // additive — omitted params behave exactly as pre-widening
            // callers. Empty/whitespace-only string hints are collapsed to
            // `None` at the boundary so downstream selection logic never sees
            // an ambiguous `Some("")` (Copilot #78 review).
            //
            // `nameExplicitlySet` lets a client mark a supplied `name` as a
            // non-explicit placeholder (`false`) so the agent's guarded
            // opening-turn self-rename (`agent.rename` with
            // `skipIfExplicitlySet: true`) still applies. Omitted/null keeps
            // the service default (`name.is_some()`); non-boolean values are
            // rejected rather than silently dropped because a dropped value
            // would flip the persisted flag's default.
            let name_explicitly_set = opt_bool_strict(params, "nameExplicitlySet")?;
            // `reasoningEffort` deliberately uses `opt_str`, not
            // `opt_nonempty_str`: at creation a present-but-blank value is an
            // explicit clear, and the service seam distinguishes it from an
            // absent param (absent is what lets the settings default
            // `model.defaultReasoningEffort` apply). Collapsing `""` here
            // would silently promote a caller's clear into the settings
            // default. The blank is dropped downstream, so the persisted
            // field is still `NULL` either way.
            let extra = AgentCreateExtra {
                provider: opt_nonempty_str(params, "provider"),
                reasoning_effort: opt_str(params, "reasoningEffort"),
                agent_type: opt_nonempty_str(params, "agentType"),
                metadata: opt_value(params, "metadata"),
                workspace_path: opt_nonempty_str(params, "workspacePath"),
                workspace_context: opt_value(params, "workspaceContext"),
                context_references: opt_value(params, "contextReferences"),
                image_blocks: opt_value(params, "imageBlocks"),
                file_blocks: opt_value(params, "fileBlocks"),
                is_background: opt_bool(params, "isBackground"),
                name_explicitly_set,
            };
            // FE/RPC front door: top-level creates stay parentless.
            let result = api
                .agent_create(ws, name, model, specialist_id, None, idempotency_key, extra)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.delegate" => {
            let ws = require_ws_note(params)?;
            let mut rest = params.clone();
            rest.remove("workspaceId");
            let input: AgentDelegateInput = serde_json::from_value(Value::Object(rest))
                .map_err(|e| invalid_params(format!("invalid params: {e}")))?;
            // FE/RPC front door: top-level creates stay parentless.
            let result = api
                .agent_delegate(ws, input, None)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.sendToTask" => {
            let ws = require_ws_note(params)?;
            let task_note_id = require_str_param(params, "taskNoteId").map(NoteId::from)?;
            let message = require_str_param(params, "message")?;
            let priority = opt_str(params, "priority");
            // Same opaque per-message payload as `agent.sendMessage` below
            // (PROTOCOL §5.5) — persisted on the assignee's user row.
            let message_metadata = opt_value(params, "messageMetadata");
            let result = api
                .agent_send_to_task(ws, task_note_id, message, priority, message_metadata)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.sendMessage" => {
            let agent_id = require_agent_id(params)?;
            let content = require_str_param(params, "content")?;
            let ws = require_ws_note(params)?;
            let message_id = opt_str(params, "messageId");
            let image_blocks = opt_value(params, "imageBlocks");
            let file_blocks = opt_value(params, "fileBlocks");
            let priority = opt_str(params, "priority");
            // Per-turn prompt-assembly hints (PROTOCOL §5.5): `stdinContext`
            // is prepended verbatim to the outbound prompt as a `Context:`
            // block (reference-parity `acp-provider.ts`); `noteIds` and
            // `contextReferences` are threaded to the prompt builder for
            // downstream note-image / context-reference resolution.
            let note_ids = opt_value(params, "noteIds");
            let stdin_context = opt_str(params, "stdinContext");
            let context_references = opt_value(params, "contextReferences");
            // Opaque per-message payload (PROTOCOL §5.5): the FE attaches
            // arbitrary JSON to distinguish daemon-initiated turns (e.g.
            // `{ source: "system" }`). Passed through unmodified and persisted
            // on the user message row via the store's metadata-aware append.
            let message_metadata = opt_value(params, "messageMetadata");
            // `userAppMessageId` (PROTOCOL §5.5): the FE's client-minted
            // logical identity for its optimistic user message. Folded into
            // the row `metadata` here so it persists without a schema change,
            // then lifted back out as the wire `appMessageId` on transcript
            // reads and echoed on the `agent:message` event so the FE dedup
            // guard can match its optimistic insert. The assistant-side ids
            // (`assistantMessageId` / `assistantAppMessageId`) remain
            // unconsumed: assistant rows are keyed on the server-minted
            // UUIDv7 id.
            let message_metadata = merge_user_app_message_id(params, message_metadata)?;
            // Question hold (PROTOCOL §5.5): the FE RPC front door is the
            // ONLY user-originated entry point — user sends are never held.
            // They do not release the hold either: only an answer-tagged row
            // (`messageMetadata.type = "question_answers"`) or
            // `agent.dismissQuestions` retires the pending Q&A.
            let result = api
                .agent_send_message(
                    ws,
                    agent_id,
                    content,
                    message_id,
                    image_blocks,
                    file_blocks,
                    priority,
                    note_ids,
                    stdin_context,
                    context_references,
                    message_metadata,
                    MessageOrigin::User,
                )
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.sendQueuedMessageNow" => {
            let agent_id = require_agent_id(params)?;
            let message_id = require_str_param(params, "messageId")?;
            let ws = require_ws_note(params)?;
            let result = api
                .agent_send_queued_message_now(ws, agent_id, message_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.dismissQuestions" => {
            let agent_id = require_agent_id(params)?;
            let message_id = require_str_param(params, "messageId")?;
            let ws = require_ws_note(params)?;
            let result = api
                .agent_dismiss_questions(ws, agent_id, message_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.markSeen" => {
            let agent_id = require_agent_id(params)?;
            let message_id = require_str_param(params, "messageId")?;
            let ws = require_ws_note(params)?;
            let result = api
                .agent_mark_seen(ws, agent_id, message_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.editAndRegenerate" => {
            let agent_id = require_agent_id(params)?;
            let message_id = require_str_param(params, "messageId")?;
            let content = require_str_param(params, "content")?;
            let ws = require_ws_note(params)?;
            let image_blocks = opt_value(params, "imageBlocks");
            let file_blocks = opt_value(params, "fileBlocks");
            let model = opt_str(params, "model");
            let result = api
                .agent_edit_and_regenerate(
                    ws,
                    agent_id,
                    message_id,
                    content,
                    image_blocks,
                    file_blocks,
                    model,
                )
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.queueMessage" => {
            let agent_id = require_agent_id(params)?;
            let content = require_str_param(params, "content")?;
            let image_blocks = opt_value(params, "imageBlocks");
            let file_blocks = opt_value(params, "fileBlocks");
            let result = api
                .agent_queue_message(agent_id, content, image_blocks, file_blocks)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.editQueuedMessage" => {
            let agent_id = require_agent_id(params)?;
            let message_id = require_str_param(params, "messageId")?;
            let content = require_str_param(params, "content")?;
            let editing = opt_bool(params, "editing");
            let result = api
                .agent_edit_queued_message(agent_id, message_id, content, editing)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.removeQueuedMessage" => {
            let agent_id = require_agent_id(params)?;
            let message_id = require_str_param(params, "messageId")?;
            let result = api
                .agent_remove_queued_message(agent_id, message_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.getQueue" => {
            let agent_id = require_agent_id(params)?;
            let ws = opt_workspace_id(params);
            let result = api
                .agent_get_queue(agent_id, ws)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.stop" => {
            let agent_id = require_agent_id(params)?;
            let result = api.agent_stop(agent_id).await.map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.setModel" => {
            let agent_id = require_agent_id(params)?;
            let model_id = require_str_param(params, "modelId")?;
            // Optional explicit provider (additive, PROTOCOL §5.5): empty /
            // whitespace-only (and JSON null) are treated as absent so older
            // clients sending a blank field keep the historical behavior; a
            // present non-string value is a malformed request — reject it
            // rather than silently falling back to the legacy path.
            match params.get("providerId") {
                None | Some(Value::Null | Value::String(_)) => {}
                Some(_) => {
                    return Err(invalid_params(
                        "agent.setModel: providerId must be a string",
                    ))
                }
            }
            let provider_id = opt_nonempty_str(params, "providerId").map(|s| s.trim().to_string());
            let ws = require_ws_note(params)?;
            let result = api
                .agent_set_model(ws, agent_id, model_id, provider_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.getModels" => {
            let result = api.agent_get_models().await.map_err(domain_to_rpc)?;
            Ok(result)
        }
        "models.list" => {
            // Additive rich model catalog (PROTOCOL §5.30); optional
            // `providerId` (omitted or empty/whitespace → backward-compatible
            // auggie path) and `forceRefresh` (skip the cache read, await a
            // fresh probe).
            let provider_id = opt_nonempty_str(params, "providerId");
            let force_refresh = opt_bool(params, "forceRefresh").unwrap_or(false);
            let result = api
                .models_list(provider_id, force_refresh)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "stats.getUsage" => {
            // Global usage-stats read behind the agentic usage-stats cards;
            // no workspaceId. `period` is "24h" / "month" / "year"; `key`
            // ("YYYY-MM" / "YYYY") is required for month/year and ignored for
            // 24h; `tzOffsetMinutes` (minutes east of UTC, default 0) shifts
            // buckets into the client's local time before grouping.
            let period = require_str_param(params, "period")?;
            let key = opt_str(params, "key");
            let tz_offset_minutes = match params.get("tzOffsetMinutes") {
                None | Some(Value::Null) => 0,
                Some(v) => v
                    .as_i64()
                    .ok_or_else(|| invalid_params("tzOffsetMinutes must be an integer"))?,
            };
            let result = api
                .stats_get_usage(period, key, tz_offset_minutes)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "stats.getRateHistory" => {
            // Global per-minute token-rate history behind the HUD TOK/MIN
            // chart (§5.39); no workspaceId. `limit` (default 60, max 1440)
            // is the number of trailing minute samples returned.
            let limit = match params.get("limit") {
                None | Some(Value::Null) => None,
                Some(v) => Some(
                    v.as_i64()
                        .ok_or_else(|| invalid_params("limit must be an integer"))?,
                ),
            };
            let result = api
                .stats_get_rate_history(limit)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.enhancePrompt" => {
            // One-shot prompt-enhance / AI-layout generation (PROTOCOL §5.31).
            let prompt = require_str_param(params, "prompt")?;
            if prompt.trim().is_empty() {
                return Err(invalid_params("prompt cannot be empty"));
            }
            let mode = opt_str(params, "mode").unwrap_or_else(|| "enhance".to_string());
            if mode != "enhance" && mode != "layout" {
                return Err(invalid_params("mode must be \"enhance\" or \"layout\""));
            }
            let model = opt_str(params, "model");
            let ws = opt_workspace_id(params);
            let timeout_ms = match params.get("timeoutMs") {
                None | Some(Value::Null) => None,
                Some(v) => Some(
                    v.as_u64()
                        .filter(|n| *n > 0)
                        .ok_or_else(|| invalid_params("timeoutMs must be a positive integer"))?,
                ),
            };
            let result = api
                .agent_enhance_prompt(prompt, mode, model, ws, timeout_ms)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.completeOnce" => {
            // Stateless one-shot prompt→completion RPC (PROTOCOL §5.32). Ports
            // the FE `background-request.service.ts` slug-generation +
            // note-status callers so no ACPProvider / ephemeral session is
            // needed. The daemon reaps the CLI on any failure path — no
            // session/agent state is created, so there is nothing to
            // garbage-collect on error.
            let prompt = require_str_param(params, "prompt")?;
            if prompt.trim().is_empty() {
                return Err(invalid_params("prompt cannot be empty"));
            }
            let system_prompt = opt_str(params, "systemPrompt");
            let model = opt_str(params, "model");
            // Optional quick-action `type` hint (`commit` / `pr` / `review` /
            // `fast`): keys `quickActions.typeOverrides` in the daemon-side
            // resolution the op applies when no explicit `model` is sent
            // (monorepo#1734). Free-form on the wire — an unknown key simply
            // misses the override map.
            let quick_action_type = opt_nonempty_str(params, "type");
            let ws = opt_workspace_id(params);
            let timeout_ms = match params.get("timeoutMs") {
                None | Some(Value::Null) => None,
                Some(v) => Some(
                    v.as_u64()
                        .filter(|n| *n > 0)
                        .ok_or_else(|| invalid_params("timeoutMs must be a positive integer"))?,
                ),
            };
            let result = api
                .agent_complete_once(
                    prompt,
                    system_prompt,
                    model,
                    quick_action_type,
                    ws,
                    timeout_ms,
                )
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.respondPermission" => {
            // Resolve an outstanding interactive prompt (PROTOCOL §8). `requestId`
            // is required; the §8 `outcome` object is validated in the handler so
            // a malformed shape surfaces as -32602.
            let request_id = require_str_param(params, "requestId")?;
            require_present(params, "outcome")?;
            let outcome = params.get("outcome").cloned().unwrap_or(Value::Null);
            let result = api
                .agent_respond_permission(request_id, outcome)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.pendingPermissions" => {
            // Snapshot outstanding prompts (PROTOCOL §8), optionally filtered to a
            // single agent by `agentId` (= `sessionId`).
            let agent_id = opt_str(params, "agentId").map(|s| AgentId::from(s.as_str()));
            let result = api
                .agent_pending_permissions(agent_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.rename" => {
            let agent_id = require_agent_id(params)?;
            let name = require_str_param(params, "name")?;
            let trimmed = name.trim().to_string();
            if trimmed.is_empty() {
                return Err(invalid_params("Name cannot be empty"));
            }
            // `skipIfExplicitlySet` (optional, default false): leave an
            // already-explicitly-named session untouched (P3-1.2b; the FE
            // `renameAgent` option) — the result then carries `skipped: true`.
            let skip_if_explicitly_set = opt_bool(params, "skipIfExplicitlySet").unwrap_or(false);
            let result = api
                .agent_rename(agent_id, trimmed, skip_if_explicitly_set)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.delete" => {
            let agent_id = require_agent_id(params)?;
            let ws = opt_workspace_id(params);
            // Delete grace window (§5.5): `undoDelayMs > 0` schedules an
            // in-memory pending deletion instead of committing now. Absent
            // or 0 keeps the immediate-delete behavior byte-identical.
            let undo_delay_ms = match params.get("undoDelayMs") {
                None | Some(Value::Null) => 0,
                Some(v) => v.as_u64().ok_or_else(|| {
                    invalid_params("Invalid parameter: undoDelayMs must be a non-negative integer")
                })?,
            };
            if undo_delay_ms > 0 {
                let delete_at = api
                    .agent_schedule_delete(agent_id, ws, undo_delay_ms)
                    .await
                    .map_err(domain_to_rpc)?;
                Ok(json!({ "success": true, "scheduled": true, "deleteAt": delete_at }))
            } else {
                let result = api
                    .agent_delete(agent_id, ws)
                    .await
                    .map_err(domain_to_rpc)?;
                Ok(result)
            }
        }
        "agent.cancelDelete" => {
            let agent_id = require_agent_id(params)?;
            let ws = opt_workspace_id(params);
            let cancelled = api
                .agent_cancel_delete(agent_id, ws)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({ "cancelled": cancelled }))
        }
        "agent.retry" => {
            let agent_id = require_agent_id(params)?;
            let ws = require_ws_note(params)?;
            let result = api.agent_retry(ws, agent_id).await.map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.wakeOrCreate" => {
            let ws = require_ws_note(params)?;
            let task_note_id = require_str_param(params, "taskNoteId").map(NoteId::from)?;
            let context_message = require_str_param(params, "contextMessage")?;
            // Widened wire input (C1d-10a). All fields optional so the
            // pre-widening 3-required-params call shape stays green;
            // `create.*` is parsed via serde (a missing `create` object
            // collapses to `None`, empty subfields collapse to `None`).
            let create = params
                .get("create")
                .filter(|v| !v.is_null())
                .cloned()
                .map(serde_json::from_value::<AgentWakeCreateOptions>)
                .transpose()
                .map_err(|e| {
                    invalid_params(format!("agent.wakeOrCreate: invalid `create` payload: {e}"))
                })?;
            // `reasoningEffort` uses `opt_str` for the same reason as
            // `agent.create` above: on the create branch a present-but-blank
            // value is an explicit clear that must not fall through to the
            // settings default.
            let input = AgentWakeOrCreateInput {
                model: opt_nonempty_str(params, "model"),
                reasoning_effort: opt_str(params, "reasoningEffort"),
                caller_agent_id: opt_nonempty_str(params, "callerAgentId")
                    .map(|s| AgentId::from(s.as_str())),
                delegation_depth: params.get("delegationDepth").and_then(Value::as_i64),
                message_metadata: opt_value(params, "messageMetadata"),
                create,
            };
            let result = api
                .agent_wake_or_create(ws, task_note_id, context_message, input)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.summary" => {
            let ws = require_ws_note(params)?;
            let agent_id = require_agent_id(params)?;
            let result = api
                .agent_summary(ws, agent_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.reportToParent" => {
            let ws = require_ws_note(params)?;
            require_present(params, "report")?;
            let report = params.get("report").cloned().unwrap_or(Value::Null);
            // No agent-caller context over the RPC front door → always -32603.
            let result = api
                .agent_report_to_parent(ws, report, None)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.getSubscriptions" => {
            let ws = require_ws_note(params)?;
            let agent_id = require_agent_id(params)?;
            let result = api
                .agent_get_subscriptions(ws, agent_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.cancelSubscriptions" => {
            let ws = require_ws_note(params)?;
            let agent_id = require_agent_id(params)?;
            // Optional scoping (additive): cancel one watch and/or one
            // delegation group instead of everything. A present-but-non-string
            // id is rejected — falling back to `None` would silently turn a
            // malformed scoped request into an unscoped cancel-everything.
            let subscription_id = match params.get("subscriptionId") {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) => Some(s.clone()),
                Some(_) => return Err(invalid_params("subscriptionId must be a string")),
            };
            let group_id = match params.get("groupId") {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) => Some(s.clone()),
                Some(_) => return Err(invalid_params("groupId must be a string")),
            };
            let result = api
                .agent_cancel_subscriptions(ws, agent_id, subscription_id, group_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.diagnostics" => {
            let ws = require_ws_note(params)?;
            let agent_id = opt_str(params, "agentId").map(AgentId::from);
            let task_note_id = opt_str(params, "taskNoteId").map(NoteId::from);
            let stale_responding_after_ms = opt_int(params, "staleRespondingAfterMs");
            let result = api
                .agent_diagnostics(ws, agent_id, task_note_id, stale_responding_after_ms)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.listInterrupted" => {
            // No required params; returns pending interrupted agents across all workspaces.
            let result = api.agent_list_interrupted().await.map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.resolveInterrupted" => {
            // Optional resume/abandon arrays; ids must be pending interrupted_agent rows.
            // If present, must be arrays of strings (reject non-array and non-string elements).
            let resume = match params.get("resume") {
                None => None,
                Some(Value::Array(arr)) => {
                    let mut ids = Vec::with_capacity(arr.len());
                    for (i, v) in arr.iter().enumerate() {
                        match v.as_str() {
                            Some(s) => ids.push(s.to_string()),
                            None => {
                                return Err(invalid_params(format!("resume[{i}] must be a string")))
                            }
                        }
                    }
                    Some(ids)
                }
                Some(_) => return Err(invalid_params("resume must be an array")),
            };
            let abandon = match params.get("abandon") {
                None => None,
                Some(Value::Array(arr)) => {
                    let mut ids = Vec::with_capacity(arr.len());
                    for (i, v) in arr.iter().enumerate() {
                        match v.as_str() {
                            Some(s) => ids.push(s.to_string()),
                            None => {
                                return Err(invalid_params(format!(
                                    "abandon[{i}] must be a string"
                                )))
                            }
                        }
                    }
                    Some(ids)
                }
                Some(_) => return Err(invalid_params("abandon must be an array")),
            };
            let result = api
                .agent_resolve_interrupted(resume, abandon)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.subscribe" => {
            let ws = require_ws_note(params)?;
            require_present(params, "eventTypes")?;
            let event_types = match params.get("eventTypes") {
                Some(Value::Array(items)) => items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
                _ => return Err(invalid_params("eventTypes must be an array")),
            };
            // Optional subscriber identity (monorepo#937): when present, the
            // named agent receives batched wake messages on matching events;
            // absent (FE front door) registers a match-only subscription.
            let subscriber = params
                .get("agentId")
                .and_then(Value::as_str)
                .map(AgentId::from);
            let exclude_self = params.get("excludeSelf").and_then(Value::as_bool);
            let batch_window = opt_int(params, "batchWindow");
            let result = api
                .agent_subscribe(ws, subscriber, event_types, exclude_self, batch_window)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "agent.unsubscribe" => {
            let ws = require_ws_note(params)?;
            let subscription_id = require_str_param(params, "subscriptionId")?;
            let result = api
                .agent_unsubscribe(ws, subscription_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "sandbox.cow.merge" => {
            let ws = require_ws_note(params)?;
            let sandbox_id = require_agent_id(params)?;
            let result = api
                .sandbox_merge(ws, sandbox_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "sandbox.cow.discard" => {
            let ws = require_ws_note(params)?;
            let sandbox_id = require_agent_id(params)?;
            let result = api
                .sandbox_discard(ws, sandbox_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(result)
        }
        "git.status" => {
            let ws = require_ws_note(params)?;
            // §5.6 extension (monorepo#2053): optional `gitRootId` scopes the
            // scan to a registered git root; an unknown/foreign id is -32602.
            let git_root_id = opt_git_root_id(params);
            let force_refresh = opt_bool(params, "forceRefresh").unwrap_or(false);
            let status = api
                .git_status_with_options(ws, git_root_id, force_refresh)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&status)
        }
        "gitRoot.list" => {
            // Every registered git root for a workspace (agent-registered and
            // auto-detected) as `{ gitRoots: [...] }`, each row carrying a
            // live-read `branch` (monorepo#2053). Missing workspace → -32602.
            let ws = require_ws_note(params)?;
            let r = api.git_root_list(ws).await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "git.getConfig" => {
            let ws = require_ws_note(params)?;
            let config = api.git_get_config(ws).await.map_err(domain_to_rpc)?;
            Ok(json!({ "config": config }))
        }
        "git.stage" => {
            let ws = require_ws_note(params)?;
            require_present(params, "paths")?;
            let paths = params.get("paths").cloned().unwrap_or(Value::Null);
            let staged = api.git_stage(ws, paths).await.map_err(domain_to_rpc)?;
            Ok(json!({ "ok": true, "paths": staged }))
        }
        "git.unstage" => {
            let ws = require_ws_note(params)?;
            require_present(params, "paths")?;
            let paths = params.get("paths").cloned().unwrap_or(Value::Null);
            let unstaged = api.git_unstage(ws, paths).await.map_err(domain_to_rpc)?;
            Ok(json!({ "ok": true, "paths": unstaged }))
        }
        "git.discard" => {
            let ws = require_ws_note(params)?;
            require_present(params, "paths")?;
            let paths = params.get("paths").cloned().unwrap_or(Value::Null);
            let discarded = api.git_discard(ws, paths).await.map_err(domain_to_rpc)?;
            Ok(json!({ "ok": true, "paths": discarded }))
        }
        "git.stageHunk" => {
            let ws = require_ws_note(params)?;
            let file_path = require_str_param(params, "filePath")?;
            let hunk_patch = require_str_param(params, "hunkPatch")?;
            api.git_stage_hunk(ws, file_path, hunk_patch)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({ "ok": true }))
        }
        "git.unstageHunk" => {
            let ws = require_ws_note(params)?;
            let file_path = require_str_param(params, "filePath")?;
            let hunk_patch = require_str_param(params, "hunkPatch")?;
            api.git_unstage_hunk(ws, file_path, hunk_patch)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({ "ok": true }))
        }
        "git.push" => {
            let ws = require_ws_note(params)?;
            let force = parse_bool(params, "force");
            let r = api.git_push(ws, force).await.map_err(domain_to_rpc)?;
            Ok(json!({
                "ok": true,
                "branch": r.get("branch").cloned().unwrap_or(Value::Null),
                "pushedSha": r.get("pushedSha").cloned().unwrap_or(Value::Null),
            }))
        }
        "git.fetch" => {
            let ws = require_ws_note(params)?;
            api.git_fetch(ws).await.map_err(domain_to_rpc)?;
            Ok(json!({ "ok": true }))
        }
        "git.createBranch" => {
            let ws = require_ws_note(params)?;
            let branch_name = require_str_param(params, "branchName")?;
            // Default is `true` (TS parity with `gitService.createBranch`);
            // callers wanting a bare `git branch <name>` pass `checkout:false`.
            // Uses `parse_bool` (rather than `Value::as_bool`) so a string
            // `"false"` is honoured, matching every other boolean arm.
            let checkout = if params.contains_key("checkout") {
                parse_bool(params, "checkout")
            } else {
                true
            };
            let r = api
                .git_create_branch(ws, branch_name, checkout)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({
                "ok": true,
                "branch": r.get("branch").cloned().unwrap_or(Value::Null),
            }))
        }
        "git.checkoutBranch" => {
            let ws = require_ws_note(params)?;
            let branch_name = require_str_param(params, "branchName")?;
            let r = api
                .git_checkout_branch(ws, branch_name)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({
                "ok": true,
                "branch": r.get("branch").cloned().unwrap_or(Value::Null),
            }))
        }
        "git.renameBranch" => {
            let ws = require_ws_note(params)?;
            let old_branch_name = require_str_param(params, "oldBranchName")?;
            let new_branch_name = require_str_param(params, "newBranchName")?;
            let r = api
                .git_rename_branch(ws, old_branch_name, new_branch_name)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({
                "ok": true,
                "oldBranch": r.get("oldBranch").cloned().unwrap_or(Value::Null),
                "newBranch": r.get("newBranch").cloned().unwrap_or(Value::Null),
            }))
        }
        "git.removeLockFile" => {
            let ws = require_ws_note(params)?;
            let r = api.git_remove_lock_file(ws).await.map_err(domain_to_rpc)?;
            Ok(json!({
                "ok": true,
                "removed": r.get("removed").cloned().unwrap_or(Value::Bool(false)),
            }))
        }
        "git.getBranches" => {
            let repo_path = require_str_param(params, "repoPath")?;
            let include_remote = parse_bool(params, "includeRemote");
            match api.git_get_branches(repo_path, include_remote).await {
                Ok(branches) => to_result_value(&branches),
                // Nonexistent / non-git repo path → -32602 with the service
                // message verbatim (no `invalid params:` prefix from
                // `domain_to_rpc`).
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "git.branchStatus" => {
            // §5.6 extension (monorepo#2053): `workspaceId` + `gitRootId` may
            // stand in for `repoPath` — the registered root resolves to its
            // path (unknown/foreign id → -32602). `repoPath` wins when both
            // are supplied so existing callers are byte-identical.
            let repo_path = match opt_str(params, "repoPath") {
                Some(p) => p,
                None => match opt_git_root_id(params) {
                    Some(id) => {
                        let ws = require_ws_note(params)?;
                        // An unknown/foreign id maps through `domain_to_rpc`
                        // like the other five scoped reads, so all six carry
                        // the identical `invalid params: Unknown git root: X`
                        // message (§5.6).
                        api.git_root_path(ws, id).await.map_err(domain_to_rpc)?
                    }
                    // Neither supplied: the pre-existing missing-param error.
                    None => return Err(require_str_param(params, "repoPath").unwrap_err()),
                },
            };
            let branch_name = require_str_param(params, "branchName")?;
            match api.git_branch_status(repo_path, branch_name).await {
                Ok(status) => to_result_value(&status),
                // Same validation as `git.getBranches`: nonexistent / non-git
                // repo path surfaces verbatim as `-32602` without the
                // `invalid params:` prefix `domain_to_rpc` would add.
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "git.pull" => {
            let repo_path = require_str_param(params, "repoPath")?;
            let branch_name = require_str_param(params, "branchName")?;
            match api.git_pull(repo_path, branch_name).await {
                // Ordinary pull failures are a structured `{ ok: false, error }`
                // result (the FE shows its pull-conflict dialog), never a
                // JSON-RPC error.
                Ok(r) => to_result_value(&r),
                // Same validation as `git.getBranches`: nonexistent / non-git
                // repo path surfaces verbatim as `-32602` without the
                // `invalid params:` prefix `domain_to_rpc` would add.
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "repo.list" => {
            // No params; returns `{ repos: KnownRepo[] }` with camelCase keys.
            let r = api.repo_list().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "repo.remove" => {
            // Delete one known-repo registry entry by path; returns
            // `{ removed: bool }` (false when the path was not registered).
            let path = require_str_param(params, "path")?;
            let r = api.repo_remove(path).await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "repo.warmCache" => {
            // Opportunistic background refresh of the repo cache for one
            // GitHub repo; returns `{ started: true, owner, repo }`
            // immediately. A warm already in flight is rejected with the
            // `warm-in-flight` busy error (PROTOCOL §5.6).
            let github_url = require_str_param(params, "githubUrl")?;
            let r = api
                .repo_warm_cache(github_url)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "git.clone" => {
            let url = require_str_param(params, "url")?;
            let parent_dir = require_str_param(params, "parentDir")?;
            let target_name = opt_str(params, "targetName");
            // Empty/whitespace-only `requestId` is treated as absent so it
            // cannot correlate `git:clone:*` streams (Copilot #78 review).
            let request_id = opt_nonempty_str(params, "requestId");
            let r = api
                .git_clone(url, parent_dir, target_name, request_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "git.commit" => {
            let ws = require_ws_note(params)?;
            let message = require_str_param(params, "message")?;
            let idempotency_key = opt_str(params, "idempotencyKey");
            let r = api
                .git_commit(ws, message, idempotency_key)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({ "ok": true, "hash": r.hash, "files": r.files }))
        }
        "git.agentCommit" => {
            let ws = require_ws_note(params)?;
            let message = require_str_param(params, "message")?;
            let files = opt_str_array(params, "files");
            let user_requested = parse_bool(params, "userRequested");
            // The FE/transport path has no agent context, so no attribution
            // trailers are written here (mirrors the reference, which composes
            // attribution at the agent-context MCP layer).
            let r = api
                .git_agent_commit(ws, message, None, None, files, user_requested)
                .await
                .map_err(domain_to_rpc)?;
            Ok(json!({
                "ok": true,
                "hash": r.hash,
                "files": r.files,
                "fileCount": r.file_count,
            }))
        }
        "git.checkMergeConflicts" => {
            let ws = require_ws_note(params)?;
            let target = opt_str(params, "targetBranch");
            let r = api
                .git_check_merge_conflicts(ws, target)
                .await
                .map_err(domain_to_rpc)?;
            to_result_value(&r)
        }
        "git.changes" => {
            let ws = require_ws_note(params)?;
            // §5.6 extension (monorepo#2053): optional `gitRootId` scopes the
            // scan to a registered git root; an unknown/foreign id is -32602.
            let git_root_id = opt_git_root_id(params);
            let r = api
                .git_changes(ws, git_root_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        // `git.diff` is accepted as an alias for the wire-canonical `git.diffs`.
        "git.diffs" | "git.diff" => {
            let ws = require_ws_note(params)?;
            // §5.6 extension: `paths` narrows the diff to exactly those
            // workspace-relative files (literal matching). The legacy single
            // `path` is folded into the same set; when both are supplied they
            // are unioned. Absent/empty ⇒ full tree; malformed ⇒ -32602.
            let mut paths = strict_opt_str_array(params, "paths")?.unwrap_or_default();
            if let Some(path) = opt_str(params, "path") {
                if !paths.contains(&path) {
                    paths.push(path);
                }
            }
            let paths = if paths.is_empty() { None } else { Some(paths) };
            let staged = parse_bool(params, "staged");
            // §5.6 extension: when `commitHash` is set the result is the hunks
            // for `<commitHash>^..<commitHash>` and `staged` is ignored.
            let commit_hash = opt_str(params, "commitHash");
            // §5.6 extension (monorepo#2053): optional `gitRootId` scopes the
            // walk to a registered git root; an unknown/foreign id is -32602.
            let git_root_id = opt_git_root_id(params);
            let r = api
                .git_diffs(ws, paths, staged, commit_hash, git_root_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "git.commitDetails" => {
            let ws = require_ws_note(params)?;
            let commit_hash = require_str_param(params, "commitHash")?;
            // §5.6 extension (monorepo#2477): optional `gitRootId` scopes the
            // read to a registered git root; an unknown/foreign id is -32602.
            let git_root_id = opt_git_root_id(params);
            let r = api
                .git_commit_details(ws, commit_hash, git_root_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        // `git.log` is accepted as an alias for the wire-canonical `git.commits`.
        "git.commits" | "git.log" => {
            let ws = require_ws_note(params)?;
            // §5.5 page params arrive nested under `page` ({ continuationToken,
            // limit }); fall back to top-level `limit`/`nextToken` for parity
            // with the other paginated reads.
            let (limit, page_token) = parse_page_params(params);
            // §5.6 extension (monorepo#2053): optional `gitRootId` scopes the
            // walk to a registered git root; an unknown/foreign id is -32602.
            let git_root_id = opt_git_root_id(params);
            let r = api
                .git_commits(ws, limit, page_token, git_root_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "git.showFile" => {
            let ws = require_ws_note(params)?;
            let file_path = require_str_param(params, "filePath")?;
            let git_ref = require_str_param(params, "ref")?;
            // §5.6 extension (monorepo#2053): optional `gitRootId` scopes the
            // read to a registered git root; an unknown/foreign id is -32602.
            let git_root_id = opt_git_root_id(params);
            let r = api
                .git_show_file(ws, file_path, git_ref, git_root_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "git.numstat" => {
            // §5.6 extension: per-file additions/deletions for a workspace's
            // tracked changes (or a branch-base two-dot range when `baseRef`
            // / `baseCommitSha` is set). `staged` is honoured only when no
            // base is supplied; `targetRef` defaults to `HEAD`.
            let ws = require_ws_note(params)?;
            let staged = opt_bool(params, "staged");
            let base_ref = opt_nonempty_str(params, "baseRef");
            let base_sha = opt_nonempty_str(params, "baseCommitSha");
            let target_ref = opt_nonempty_str(params, "targetRef");
            let paths = opt_str_array(params, "paths");
            let r = api
                .git_numstat(ws, staged, base_ref, base_sha, target_ref, paths)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "git.branchDiff" => {
            // §5.6 extension: committed diff of `targetRef` vs the branch
            // boundary (merge-base of `targetRef` and `baseRef`, else
            // `baseCommitSha` when it is an ancestor of `targetRef`). At
            // least one of `baseRef` / `baseCommitSha` is required (-32602);
            // `targetRef` defaults to `HEAD`.
            let ws = require_ws_note(params)?;
            let base_ref = opt_nonempty_str(params, "baseRef");
            let base_sha = opt_nonempty_str(params, "baseCommitSha");
            if base_ref.is_none() && base_sha.is_none() {
                return Err(invalid_params(
                    "git.branchDiff requires baseRef or baseCommitSha".to_string(),
                ));
            }
            let target_ref = opt_nonempty_str(params, "targetRef");
            let paths = opt_str_array(params, "paths");
            let r = api
                .git_branch_diff(ws, base_ref, base_sha, target_ref, paths)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "git.getRemoteUrl" => {
            // §5.6 extension: path-based read like `git.getBranches` — a
            // nonexistent / non-git repo path surfaces verbatim as -32602
            // without the `invalid params:` prefix `domain_to_rpc` would add.
            let repo_path = require_str_param(params, "repoPath")?;
            let remote_name = opt_nonempty_str(params, "remoteName");
            match api.git_get_remote_url(repo_path, remote_name).await {
                Ok(r) => Ok(r),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "pr.status" => {
            let ws = require_ws_note(params)?;
            let r = api.pr_status(ws).await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "pr.refresh" => {
            let ws = require_ws_note(params)?;
            let r = api.pr_refresh(ws).await.map_err(workspace_err)?;
            Ok(r)
        }
        // `github.*` explicit-addressing surface (PROTOCOL §5.27): every data
        // method takes `(owner, repo[, number])` rather than resolving from the
        // workspace. `limit` falls back to the FE's `perPage` spelling.
        "github.pulls.create" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let title = require_str_param(params, "title")?;
            let body = require_str_param(params, "body")?;
            let head = require_str_param(params, "head")?;
            let base = require_str_param(params, "base")?;
            let draft = parse_bool(params, "draft");
            let r = api
                .github_pulls_create(owner, repo, title, body, head, base, draft)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        // github.* browse / auth / identity (PROTOCOL §5.27). Repo-addressed
        // GitHub ops backed by the SourceControl engine; the PAT comes from the
        // environment and is never logged or returned. Pagination follows §5.5
        // (`limit` / `nextToken`): reads return a real opaque base64 `nextToken`
        // cursor, which is `null` only on the last page.
        "github.repos.list" => {
            let limit = opt_int(params, "limit").or_else(|| opt_int(params, "perPage"));
            let next_token = opt_str(params, "nextToken");
            let r = api
                .github_repos_list(limit, next_token)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.repos.search" => {
            let query = require_str_param(params, "query")?;
            let limit = opt_int(params, "limit");
            let next_token = opt_str(params, "nextToken");
            let r = api
                .github_repos_search(query, limit, next_token)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.repos.get" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let r = api
                .github_repos_get(owner, repo)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.repoConfig.get" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let git_ref = opt_str(params, "ref");
            let r = api
                .github_repo_config_get(owner, repo, git_ref)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.pulls.get" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let number = require_u64(params, "number")?;
            let r = api
                .github_pulls_get(owner, repo, number)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.branches.list" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let prefix = opt_str(params, "prefix");
            let limit = opt_int(params, "limit").or_else(|| opt_int(params, "perPage"));
            let next_token = opt_str(params, "nextToken");
            let r = api
                .github_branches_list(owner, repo, prefix, limit, next_token)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.branches.listCached" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let r = api
                .github_branches_list_cached(owner, repo)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.pulls.list" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let state = opt_str(params, "state");
            let head = opt_str(params, "head");
            let base = opt_str(params, "base");
            let limit = opt_int(params, "limit").or_else(|| opt_int(params, "perPage"));
            let next_token = opt_str(params, "nextToken");
            let r = api
                .github_pulls_list(owner, repo, state, head, base, limit, next_token)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.pulls.search" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let filter = opt_str(params, "filter");
            let state = opt_str(params, "state");
            let query = opt_str(params, "query");
            let limit = opt_int(params, "limit").or_else(|| opt_int(params, "perPage"));
            let next_token = opt_str(params, "nextToken");
            let r = api
                .github_pulls_search(owner, repo, filter, state, query, limit, next_token)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.pulls.merge" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let number = require_u64(params, "number")?;
            let merge_method = opt_str(params, "mergeMethod");
            let commit_title = opt_str(params, "commitTitle");
            let commit_message = opt_str(params, "commitMessage");
            let r = api
                .github_pulls_merge(
                    owner,
                    repo,
                    number,
                    merge_method,
                    commit_title,
                    commit_message,
                )
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.pulls.updateBranch" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let number = require_u64(params, "number")?;
            let expected_head_sha = opt_str(params, "expectedHeadSha");
            let r = api
                .github_pulls_update_branch(owner, repo, number, expected_head_sha)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.issues.list" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let state = opt_str(params, "state");
            let labels = opt_str(params, "labels");
            let limit = opt_int(params, "limit").or_else(|| opt_int(params, "perPage"));
            let next_token = opt_str(params, "nextToken");
            let r = api
                .github_issues_list(owner, repo, state, labels, limit, next_token)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.issues.search" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let filter = opt_str(params, "filter");
            let state = opt_str(params, "state");
            let query = opt_str(params, "query");
            let limit = opt_int(params, "limit").or_else(|| opt_int(params, "perPage"));
            let next_token = opt_str(params, "nextToken");
            let r = api
                .github_issues_search(owner, repo, filter, state, query, limit, next_token)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.listReviewComments" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let number = require_u64(params, "number")?;
            let limit = opt_int(params, "limit").or_else(|| opt_int(params, "perPage"));
            let next_token = opt_str(params, "nextToken");
            let r = api
                .github_list_review_comments(owner, repo, number, limit, next_token)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.replyReviewComment" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let number = require_u64(params, "number")?;
            let comment_id = require_u64(params, "commentId")?;
            let body = require_str_param(params, "body")?;
            let r = api
                .github_reply_review_comment(owner, repo, number, comment_id, body)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.getReviewThreads" => {
            let owner = require_str_param(params, "owner")?;
            let repo = require_str_param(params, "repo")?;
            let number = require_u64(params, "number")?;
            let limit = opt_int(params, "limit").or_else(|| opt_int(params, "perPage"));
            let next_token = opt_str(params, "nextToken");
            let r = api
                .github_get_review_threads(owner, repo, number, limit, next_token)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.resolveThread" => {
            let thread_id = require_str_param(params, "threadId")?;
            let r = api
                .github_resolve_thread(thread_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.unresolveThread" => {
            let thread_id = require_str_param(params, "threadId")?;
            let r = api
                .github_unresolve_thread(thread_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.authStatus" => {
            let r = api.github_auth_status().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.connect" => {
            let r = api.github_connect().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.cancelAuth" => {
            let r = api.github_cancel_auth().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.revoke" => {
            let r = api.github_revoke().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "github.getUser" => {
            let r = api.github_get_user().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        // `linear.*` (§5.28) is daemon-owned and global: no `workspaceId`. A key
        // that is absent or fails the `viewer` probe ("not configured") and any
        // other Linear failure surface as `-32603`; an invalid `filter` is
        // `-32602` with the descriptive message verbatim.
        "linear.authStatus" => {
            let r = api.linear_auth_status().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "linear.listIssues" => {
            let filter = opt_str(params, "filter");
            let limit = opt_int(params, "limit");
            let next_token = opt_str(params, "nextToken");
            match api.linear_list_issues(filter, limit, next_token).await {
                Ok(page) => Ok(page),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "linear.searchIssues" => {
            let query = require_str_param(params, "query")?;
            let limit = opt_int(params, "limit");
            let next_token = opt_str(params, "nextToken");
            let r = api
                .linear_search_issues(query, limit, next_token)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "linear.getIssue" => {
            let id = opt_str(params, "id")
                .or_else(|| opt_str(params, "identifier"))
                .ok_or_else(|| invalid_params("Missing required parameter: id"))?;
            let r = api.linear_get_issue(id).await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "linear.viewer" => {
            let r = api.linear_viewer().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "linear.listTeams" => {
            let limit = opt_int(params, "limit");
            let r = api.linear_list_teams(limit).await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "linear.listWorkflowStates" => {
            let limit = opt_int(params, "limit");
            let r = api
                .linear_list_workflow_states(limit)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "linear.listProjects" => {
            let limit = opt_int(params, "limit");
            let r = api
                .linear_list_projects(limit)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "linear.listLabels" => {
            let limit = opt_int(params, "limit");
            let r = api.linear_list_labels(limit).await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        // `linear.*` P2 writes (§5.28). Required wire fields are validated here
        // before the request is forwarded; the engine maps any other failure
        // (incl. not-configured) to `-32603` via the services layer.
        "linear.createIssue" => {
            // `title` AND `teamId` are required. Reject missing/empty values
            // up front so we never call the engine with a bogus payload.
            let _title = require_non_empty_str(params, "title")?;
            let _team_id = require_non_empty_str(params, "teamId")?;
            let request = Value::Object(params.clone());
            match api.linear_create_issue(request).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "linear.updateIssue" => {
            // `issueId` is required; every other field is optional and only
            // forwarded when present.
            let _issue_id = require_non_empty_str(params, "issueId")?;
            let request = Value::Object(params.clone());
            match api.linear_update_issue(request).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        // `sentry.*` (§5.29) is daemon-owned and global: no `workspaceId`. A
        // credential pair that is absent or fails the org probe ("not
        // configured") and any other Sentry failure surface as `-32603`; an
        // invalid `status` is `-32602` with the descriptive message verbatim.
        "sentry.authStatus" => {
            let r = api.sentry_auth_status().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "sentry.listIssues" => {
            let project = opt_str(params, "project");
            let status = opt_str(params, "status");
            let query = opt_str(params, "query");
            let limit = opt_int(params, "limit");
            let next_token = opt_str(params, "nextToken");
            match api
                .sentry_list_issues(project, status, query, limit, next_token)
                .await
            {
                Ok(page) => Ok(page),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "sentry.searchIssues" => {
            let query = require_str_param(params, "query")?;
            let project = opt_str(params, "project");
            let limit = opt_int(params, "limit");
            let next_token = opt_str(params, "nextToken");
            let r = api
                .sentry_search_issues(query, project, limit, next_token)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "sentry.listProjects" => {
            let limit = opt_int(params, "limit");
            let r = api
                .sentry_list_projects(limit)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "sentry.getIssue" => {
            // Either `id` or `shortId` is required; both missing → `-32602`.
            let id = opt_str(params, "id")
                .or_else(|| opt_str(params, "shortId"))
                .ok_or_else(|| invalid_params("Missing required parameter: id"))?;
            let r = api.sentry_get_issue(id).await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "sentry.resolveIssue" => {
            let id = require_non_empty_str(params, "id")?;
            let r = api.sentry_resolve_issue(id).await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "sentry.ignoreIssue" => {
            let id = require_non_empty_str(params, "id")?;
            let r = api.sentry_ignore_issue(id).await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "sentry.assignIssue" => {
            // `id` is required; `assignedTo` is optional — an absent value
            // unassigns the issue.
            let id = require_non_empty_str(params, "id")?;
            let assigned_to = opt_str(params, "assignedTo");
            let r = api
                .sentry_assign_issue(id, assigned_to)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "file-tracking.getChanges" => {
            let ws = require_ws_note(params)?;
            let filter = opt_value(params, "filter");
            let r = api
                .file_tracking_get_changes(ws, filter)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "file-tracking.loadCommits" => {
            let ws = require_ws_note(params)?;
            let limit = opt_int(params, "limit");
            let page_token = opt_str(params, "nextToken");
            let include_older = params.get("includeOlder").and_then(Value::as_bool);
            let r = api
                .file_tracking_load_commits(ws, limit, page_token, include_older)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "file-tracking.getLineStats" => {
            let ws = require_ws_note(params)?;
            let r = api
                .file_tracking_get_line_stats(ws)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "file-tracking.stage" => {
            let ws = require_ws_note(params)?;
            require_present(params, "paths")?;
            let paths = params.get("paths").cloned().unwrap_or(Value::Null);
            let r = api
                .file_tracking_stage(ws, paths)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "file-tracking.unstage" => {
            let ws = require_ws_note(params)?;
            require_present(params, "paths")?;
            let paths = params.get("paths").cloned().unwrap_or(Value::Null);
            let r = api
                .file_tracking_unstage(ws, paths)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "metrics.getWorkspaceStats" => {
            let ws = require_ws_note(params)?;
            let r = api
                .metrics_get_workspace_stats(ws)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "metrics.getAgentStats" => {
            let agent_id = require_str_param(params, "agentId")?;
            let r = api
                .metrics_get_agent_stats(agent_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "metrics.getAllWorkspaceStats" => {
            let r = api
                .metrics_get_all_workspace_stats()
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "metrics.clearAgentStats" => {
            let agent_id = require_str_param(params, "agentId")?;
            let r = api
                .metrics_clear_agent_stats(agent_id)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "settings.list" => {
            // Global namespace (no workspaceId); sensitive values are redacted.
            let r = api.settings_list().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "settings.get" => {
            let path = require_str_param(params, "path")?;
            match api.settings_get(path).await {
                Ok(v) => Ok(v),
                // Unknown path → -32602 with the raw message (no prefix).
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "settings.update" => {
            require_present(params, "changes")?;
            let changes = params.get("changes").cloned().unwrap_or(Value::Null);
            match api.settings_update(changes).await {
                Ok(v) => Ok(v),
                // Unknown path / read-only / failed validation → -32602.
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "settings.reset" => {
            let path = require_str_param(params, "path")?;
            match api.settings_reset(path).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "system.capabilities" => {
            // Machine-level capabilities, no workspaceId (PROTOCOL §5.7).
            // Router method (unlike the system.* control fast-path): the
            // cowSupported probe lives in the service layer's aggregate cache.
            let r = api.system_capabilities().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "debug.sampleStacks" => {
            // Point-in-time sample of the daemon's own thread stacks
            // (PROTOCOL §5.43, monorepo#1755); no workspaceId — the daemon
            // process is global. Both params are optional; out-of-range
            // values are clamped in the service layer, but a present
            // non-numeric value is a caller error.
            for name in ["durationMs", "frequencyHz"] {
                if params
                    .get(name)
                    .is_some_and(|v| !v.is_number() && !v.is_null())
                {
                    return Err(invalid_params(format!(
                        "invalid params: {name} must be a number"
                    )));
                }
            }
            let duration_ms = opt_int(params, "durationMs");
            let frequency_hz = opt_int(params, "frequencyHz");
            let r = api
                .debug_sample_stacks(duration_ms, frequency_hz)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "providers.catalog" => {
            // The static provider registry (monorepo#928); no params, no
            // workspaceId — the registry is compiled-in daemon data.
            let r = api.providers_catalog().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "unsloth.status" => {
            // Observe the daemon-managed singleton Unsloth server
            // (monorepo#878); no workspaceId — the server is daemon-global.
            let r = api.unsloth_status().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "unsloth.stop" => {
            // Gracefully terminate the managed Unsloth server; a no-op
            // (`{ stopped: false }`) when none is running, not an error.
            let r = api.unsloth_stop().await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "voice.transcribe" => {
            // Daemon-owned and global: no required `workspaceId`. `audio`
            // (base64) is required; the service layer validates shape/size,
            // selects the provider (per-call override else the
            // `voice.provider` setting), and handles the optional
            // `workspaceId?` vocabulary injection (§5.41, v4.6 — an unknown
            // or stale id is tolerated, only a non-string value errors).
            // Missing/oversized/invalid audio → -32602.
            require_str_param(params, "audio")?;
            let request = Value::Object(params.clone());
            match api.voice_transcribe(request).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "voice.getWorkspaceVocabulary" => {
            // The auto-derived workspace vocabulary — derived terms only —
            // for client-side (OS-engine) transcription and Settings
            // previews (§5.41, v4.6). Unlike the tolerant `workspaceId?` on
            // `voice.transcribe`, the param here is required and validated.
            let ws = require_workspace_id(params)?;
            match api.voice_get_workspace_vocabulary(ws).await {
                Ok(v) => Ok(v),
                Err(Error::NotFound(_)) => Err(not_found("Workspace not found")),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "accept-changes.getStatus" => {
            let ws = require_ws_note(params)?;
            let r = api
                .accept_changes_get_status(ws)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "accept-changes.prepare" => {
            let ws = require_ws_note(params)?;
            let action = require_str_param(params, "action")?;
            let files = opt_str_array(params, "files");
            let r = api
                .accept_changes_prepare(ws, action, files)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "accept-changes.execute" => {
            let ws = require_ws_note(params)?;
            require_str_param(params, "action")?;
            let r = api
                .accept_changes_execute(ws, Value::Object(params.clone()))
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "accept-changes.mergePR" => {
            let ws = require_ws_note(params)?;
            let pr_number = params
                .get("prNumber")
                .and_then(Value::as_u64)
                .ok_or_else(|| invalid_params("Missing required parameter: prNumber"))?;
            let merge_method = opt_str(params, "mergeMethod");
            let commit_title = opt_str(params, "commitTitle");
            let commit_message = opt_str(params, "commitMessage");
            let r = api
                .accept_changes_merge_pr(ws, pr_number, merge_method, commit_title, commit_message)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "accept-changes.addRemote" => {
            let ws = require_ws_note(params)?;
            let remote_url = require_str_param(params, "remoteUrl")?;
            let r = api
                .accept_changes_add_remote(ws, remote_url)
                .await
                .map_err(domain_to_rpc)?;
            Ok(r)
        }
        "search.inFiles" => {
            let ws = require_ws_note(params)?;
            let query = require_str_param(params, "query")?;
            let opts = opt_value(params, "opts");
            let request_id = opt_str(params, "requestId");
            match api.search_in_files(ws, query, opts, request_id).await {
                Ok(v) => Ok(v),
                // Malformed regex / glob → -32602 with the raw message
                // ("Invalid regex"), not the `invalid params:` prefix.
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "search.fileNames" => {
            let ws = require_ws_note(params)?;
            let pattern = require_str_param(params, "pattern")?;
            let limit = opt_int(params, "limit");
            let request_id = opt_str(params, "requestId");
            match api.search_file_names(ws, pattern, limit, request_id).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "search.cancel" => {
            let request_id = require_str_param(params, "requestId")?;
            let r = api.search_cancel(request_id).await.map_err(domain_to_rpc)?;
            Ok(r)
        }
        "search.messages" => {
            // `workspaceId` is optional (absent → global search across all
            // workspaces); `preferWorkspaceId` is a soft ranking boost, and
            // archived-workspace matches get a soft penalty on the same
            // bm25-unit scale, yielding the default tier order preferred →
            // other active → archived.
            let ws = opt_workspace_id(params);
            let query = require_str_param(params, "query")?;
            let agent_id = opt_str(params, "agentId");
            let role = opt_str(params, "role");
            let limit = opt_int(params, "limit");
            let prefer_ws = params
                .get("preferWorkspaceId")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(WorkspaceId::from);
            let request_id = opt_str(params, "requestId");
            api.search_messages(ws, query, agent_id, role, limit, prefer_ws, request_id)
                .await
                .map_err(domain_to_rpc)
        }
        "search.events" => {
            let query = require_str_param(params, "query")?;
            let workspace_id = opt_workspace_id(params);
            let limit = opt_int(params, "limit");
            let request_id = opt_str(params, "requestId");
            api.search_events(query, workspace_id, limit, request_id)
                .await
                .map_err(domain_to_rpc)
        }
        "search.notes" => {
            let query = require_str_param(params, "query")?;
            let request_id = opt_str(params, "requestId");
            api.search_notes(query, request_id)
                .await
                .map_err(domain_to_rpc)
        }
        "search.codebase" => {
            let ws = require_ws_note(params)?;
            let query = require_str_param(params, "query")?;
            let request_id = opt_str(params, "requestId");
            match api.search_codebase(ws, query, request_id).await {
                Ok(v) => Ok(v),
                // A malformed regex from the content-search reuse surfaces raw.
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "terminal.create" => {
            let ws = require_ws_note(params)?;
            let cols = opt_dim(params, "cols", 80);
            let rows = opt_dim(params, "rows", 24);
            let cwd = opt_str(params, "cwd");
            let command = opt_str(params, "command");
            let env = opt_string_map(params, "env");
            api.terminal_create(ws, cols, rows, cwd, command, env)
                .await
                .map_err(domain_to_rpc)
        }
        "terminal.write" => {
            let terminal_id = require_str_param(params, "terminalId")?;
            let data = require_str_param(params, "data")?;
            api.terminal_write(terminal_id, data)
                .await
                .map_err(domain_to_rpc)
        }
        "terminal.resize" => {
            let terminal_id = require_str_param(params, "terminalId")?;
            let cols = opt_dim(params, "cols", 80);
            let rows = opt_dim(params, "rows", 24);
            api.terminal_resize(terminal_id, cols, rows)
                .await
                .map_err(domain_to_rpc)
        }
        "terminal.kill" => {
            let terminal_id = require_str_param(params, "terminalId")?;
            api.terminal_kill(terminal_id).await.map_err(domain_to_rpc)
        }
        "terminal.getBuffer" => {
            let terminal_id = require_str_param(params, "terminalId")?;
            let max_bytes = opt_int(params, "maxBytes");
            api.terminal_get_buffer(terminal_id, max_bytes)
                .await
                .map_err(domain_to_rpc)
        }
        "terminal.list" => {
            let ws = require_ws_note(params)?;
            api.terminal_list(ws).await.map_err(domain_to_rpc)
        }
        "terminal.readOutput" => {
            let ws = require_ws_note(params)?;
            let terminal_id = require_str_param(params, "terminalId")?;
            let max_lines = opt_int(params, "maxLines");
            let paginate = params.get("paginate").and_then(Value::as_bool);
            let page_token = opt_str(params, "nextToken");
            api.terminal_read_output(ws, terminal_id, max_lines, paginate, page_token)
                .await
                .map_err(domain_to_rpc)
        }
        "file.read" => {
            let ws = require_ws_note(params)?;
            let path = require_str_param(params, "path")?;
            api.file_read(ws, path, None).await.map_err(domain_to_rpc)
        }
        "file.readChunk" => {
            let ws = require_ws_note(params)?;
            let path = require_str_param(params, "path")?;
            let offset = require_u64(params, "offset")?;
            let length = require_u64(params, "length")?;
            api.file_read_chunk(ws, path, offset, length, None)
                .await
                .map_err(domain_to_rpc)
        }
        "file.write" => {
            let ws = require_ws_note(params)?;
            let path = require_str_param(params, "path")?;
            let content = require_str_param(params, "content")?;
            api.file_write(ws, path, content, None)
                .await
                .map_err(domain_to_rpc)
        }
        "file.list" => {
            // `path` is optional, defaulting to "." (TS builder default).
            let ws = require_ws_note(params)?;
            let path = opt_str(params, "path").unwrap_or_else(|| ".".to_string());
            api.file_list(ws, path, None).await.map_err(domain_to_rpc)
        }
        "file.tree" => {
            // `path` is optional, defaulting to the workspace root ("."). The FE
            // anchors the explorer here and lazy-lists children via `file.list`.
            let ws = require_ws_note(params)?;
            let path = opt_str(params, "path").unwrap_or_else(|| ".".to_string());
            api.file_tree(ws, path).await.map_err(domain_to_rpc)
        }
        "file.delete" => {
            let ws = require_ws_note(params)?;
            let path = require_str_param(params, "path")?;
            api.file_delete(ws, path, None).await.map_err(domain_to_rpc)
        }
        "file.mkdir" => {
            let ws = require_ws_note(params)?;
            let path = require_str_param(params, "path")?;
            api.file_mkdir(ws, path, None).await.map_err(domain_to_rpc)
        }
        "file.rename" => {
            let ws = require_ws_note(params)?;
            let old_path = require_str_param(params, "oldPath")?;
            let new_path = require_str_param(params, "newPath")?;
            api.file_rename(ws, old_path, new_path, None)
                .await
                .map_err(domain_to_rpc)
        }
        "file.exists" => {
            let ws = require_ws_note(params)?;
            let path = require_str_param(params, "path")?;
            api.file_exists(ws, path).await.map_err(domain_to_rpc)
        }
        "file.stat" => {
            let ws = require_ws_note(params)?;
            let path = require_str_param(params, "path")?;
            api.file_stat(ws, path).await.map_err(domain_to_rpc)
        }
        "file.placeAttachment" => {
            // Exactly-one-of `data` / `sourcePath` is validated in the service
            // (→ -32602); the router only enforces the always-required params.
            let ws = require_ws_note(params)?;
            let file_name = require_str_param(params, "fileName")?;
            let data = opt_str(params, "data");
            let source_path = opt_str(params, "sourcePath");
            let mime_type = opt_str(params, "mimeType");
            api.file_place_attachment(ws, file_name, data, source_path, mime_type)
                .await
                .map_err(domain_to_rpc)
        }
        "file.getAttachmentInfo" => {
            let attachment_id = require_str_param(params, "attachmentId")?;
            api.file_get_attachment_info(attachment_id)
                .await
                .map_err(domain_to_rpc)
        }
        "file.attachmentUpload.begin" => {
            let ws = require_ws_note(params)?;
            let file_name = require_str_param(params, "fileName")?;
            let size_bytes = require_u64(params, "sizeBytes")?;
            let sha256 = require_str_param(params, "sha256")?;
            let mime_type = opt_str(params, "mimeType");
            api.file_attachment_upload_begin(ws, file_name, size_bytes, sha256, mime_type)
                .await
                .map_err(domain_to_rpc)
        }
        "file.attachmentUpload.chunk" => {
            let upload_id = require_str_param(params, "uploadId")?;
            let seq = require_u64(params, "seq")?;
            let data = require_str_param(params, "data")?;
            api.file_attachment_upload_chunk(upload_id, seq, data)
                .await
                .map_err(domain_to_rpc)
        }
        "file.attachmentUpload.commit" => {
            let upload_id = require_str_param(params, "uploadId")?;
            api.file_attachment_upload_commit(upload_id)
                .await
                .map_err(domain_to_rpc)
        }
        "file.attachmentUpload.abort" => {
            let upload_id = require_str_param(params, "uploadId")?;
            api.file_attachment_upload_abort(upload_id)
                .await
                .map_err(domain_to_rpc)
        }
        "primitive.addReference" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let semantic_id = require_str_param(params, "semanticId")?;
            let description = require_str_param(params, "description")?;
            let snapshot = opt_str(params, "snapshot");
            api.primitive_add_reference(ws, note_id, semantic_id, description, snapshot)
                .await
                .map_err(domain_to_rpc)
        }
        "primitive.addCli" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let command = require_str_param(params, "command")?;
            let description = require_str_param(params, "description")?;
            let working_directory = opt_str(params, "workingDirectory");
            api.primitive_add_cli(ws, note_id, command, description, working_directory)
                .await
                .map_err(domain_to_rpc)
        }
        "primitive.addPatch" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let file_path = require_str_param(params, "filePath")?;
            let diff = require_str_param(params, "diff")?;
            let description = require_str_param(params, "description")?;
            api.primitive_add_patch(ws, note_id, file_path, diff, description)
                .await
                .map_err(domain_to_rpc)
        }
        "primitive.addAgentAction" => {
            let ws = require_ws_note(params)?;
            let note_id = require_note_id(params)?;
            let agent_id = require_str_param(params, "agentId")?;
            let goal = require_str_param(params, "goal")?;
            let description = require_str_param(params, "description")?;
            api.primitive_add_agent_action(ws, note_id, agent_id, goal, description)
                .await
                .map_err(domain_to_rpc)
        }
        "crossWorkspace.listSiblings" => {
            let ws = require_ws_note(params)?;
            api.cross_workspace_list_siblings(ws)
                .await
                .map_err(domain_to_rpc)
        }
        "crossWorkspace.readNote" => {
            let ws = require_ws_note(params)?;
            let target = require_str_param(params, "targetWorkspaceId").map(WorkspaceId::from)?;
            let note_id = require_note_id(params)?;
            api.cross_workspace_read_note(ws, target, note_id)
                .await
                .map_err(domain_to_rpc)
        }
        "crossWorkspace.listNotes" => {
            let ws = require_ws_note(params)?;
            let target = require_str_param(params, "targetWorkspaceId").map(WorkspaceId::from)?;
            api.cross_workspace_list_notes(ws, target)
                .await
                .map_err(domain_to_rpc)
        }
        "script.list" => {
            let ws = require_ws_note(params)?;
            api.script_list(ws).await.map_err(domain_to_rpc)
        }
        "script.create" => {
            let ws = require_ws_note(params)?;
            let create = parse_script_create(params)?;
            api.script_create(ws, create).await.map_err(domain_to_rpc)
        }
        "script.remove" => {
            let ws = require_ws_note(params)?;
            let script_id = require_str_param(params, "scriptId")?;
            api.script_remove(ws, script_id)
                .await
                .map_err(domain_to_rpc)
        }
        "script.start" => {
            let ws = require_ws_note(params)?;
            let script_id = require_str_param(params, "scriptId")?;
            api.script_start(ws, script_id).await.map_err(domain_to_rpc)
        }
        "script.stop" => {
            let ws = require_ws_note(params)?;
            let script_id = require_str_param(params, "scriptId")?;
            api.script_stop(ws, script_id).await.map_err(domain_to_rpc)
        }
        "script.restart" => {
            let ws = require_ws_note(params)?;
            let script_id = require_str_param(params, "scriptId")?;
            api.script_restart(ws, script_id)
                .await
                .map_err(domain_to_rpc)
        }
        "script.output" => {
            let ws = require_ws_note(params)?;
            let script_id = require_str_param(params, "scriptId")?;
            let max_lines = opt_int(params, "maxLines");
            let paginate = params.get("paginate").and_then(Value::as_bool);
            let page_token = opt_str(params, "nextToken");
            api.script_output(ws, script_id, max_lines, paginate, page_token)
                .await
                .map_err(domain_to_rpc)
        }
        "script.status" => {
            let ws = require_ws_note(params)?;
            let script_id = require_str_param(params, "scriptId")?;
            api.script_status(ws, script_id)
                .await
                .map_err(domain_to_rpc)
        }
        "script.run" => {
            let ws = require_ws_note(params)?;
            let script_id = require_str_param(params, "scriptId")?;
            let max_lines = opt_int(params, "maxLines");
            // `timeoutSeconds` with the `timeout` alias (PROTOCOL §5.8).
            let timeout_seconds =
                opt_int(params, "timeoutSeconds").or_else(|| opt_int(params, "timeout"));
            api.script_run(ws, script_id, max_lines, timeout_seconds)
                .await
                .map_err(domain_to_rpc)
        }
        // Background hooks (§6.8): the FE reads and manages hooks; there is
        // NO wire `hook.schedule` — hooks are agent-authored via the
        // `ws.hook.schedule` MCP binding only. An unknown/foreign `hookId`
        // surfaces as `-32602` (`Error::NotFound` → invalid params).
        "hook.list" => {
            let ws = require_ws_note(params)?;
            api.hook_list(ws, None).await.map_err(domain_to_rpc)
        }
        "hook.cancel" => {
            let ws = require_ws_note(params)?;
            let hook_id = require_str_param(params, "hookId")?;
            // FE cancel (no agent caller): any hook can be cancelled and the
            // owning agent is woken with a cancellation notice.
            api.hook_cancel(ws, intent_core::HookId::from(hook_id.as_str()), None)
                .await
                .map_err(domain_to_rpc)
        }
        "hook.runNow" => {
            let ws = require_ws_note(params)?;
            let hook_id = require_str_param(params, "hookId")?;
            api.hook_run_now(ws, intent_core::HookId::from(hook_id.as_str()))
                .await
                .map_err(domain_to_rpc)
        }
        // Centralized PR monitors (§6.9): the FE reads, cancels and flushes
        // monitors; there is NO wire registration method — monitors are
        // agent-owned via the `ws.pr.monitor` MCP binding only.
        "prMonitor.list" => {
            let ws = require_ws_note(params)?;
            api.pr_monitor_list(ws, None).await.map_err(domain_to_rpc)
        }
        "prMonitor.cancel" => {
            let ws = require_ws_note(params)?;
            let monitor_id = require_str_param(params, "monitorId")?;
            // FE cancel: any monitor can be cancelled and the owning agent is
            // woken with a cancellation notice.
            api.pr_monitor_cancel_by_id(ws, intent_core::PrMonitorId::from(monitor_id.as_str()))
                .await
                .map_err(domain_to_rpc)
        }
        "prMonitor.flush" => {
            let ws = require_ws_note(params)?;
            let monitor_id = require_str_param(params, "monitorId")?;
            let check = opt_bool_strict(params, "check")?.unwrap_or(false);
            api.pr_monitor_flush_pending(
                ws,
                intent_core::PrMonitorId::from(monitor_id.as_str()),
                check,
            )
            .await
            .map_err(domain_to_rpc)
        }
        "rules.list" => {
            // Optional workspaceId: present → include the workspace's read-only
            // rule files; omitted → global user-override set only.
            let workspace_id = opt_workspace_id(params);
            api.rules_list(workspace_id).await.map_err(domain_to_rpc)
        }
        "rules.get" => {
            let ws = require_ws_note(params)?;
            let rule_type = require_str_param(params, "ruleType")?;
            api.rules_get(ws, rule_type).await.map_err(domain_to_rpc)
        }
        "rules.update" => {
            let ws = require_ws_note(params)?;
            let rule_type = require_str_param(params, "ruleType")?;
            let content = require_str_param(params, "content")?;
            let enabled = opt_bool(params, "enabled");
            match api.rules_update(ws, rule_type, content, enabled).await {
                Ok(v) => Ok(v),
                // Empty ruleType / over-long content → -32602 with the raw message.
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "specialist.list" => {
            // Matches the TS WSS `specialist.list` signature: no params; merges
            // user > bundled tiers only (the project tier is not part of the live
            // wire contract iOS calls). `specialist.get` still accepts an optional
            // `workspacePath` for the project tier (PROTOCOL §5.11). The optional
            // `provider` supplies the resolution context for the additive
            // `resolvedModel`/`resolvedProvider` preview fields.
            let provider = opt_str(params, "provider");
            match api.specialist_list(None, provider).await {
                Ok(v) => Ok(v),
                // Unknown provider → -32602 with the raw message.
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "specialist.get" => {
            let id = require_str_param(params, "id")?;
            let workspace_path = opt_str(params, "workspacePath");
            let provider = opt_str(params, "provider");
            match api.specialist_get(id, workspace_path, provider).await {
                Ok(v) => Ok(v),
                // Unknown id / invalid id → -32602 with the raw message.
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "specialist.create" => {
            let id = require_str_param(params, "id")?;
            require_present(params, "spec")?;
            let spec = params.get("spec").cloned().unwrap_or(Value::Null);
            let scope = opt_str(params, "scope");
            let workspace_path = opt_str(params, "workspacePath");
            match api.specialist_create(id, spec, scope, workspace_path).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "specialist.edit" => {
            let id = require_str_param(params, "id")?;
            require_present(params, "spec")?;
            let spec = params.get("spec").cloned().unwrap_or(Value::Null);
            let scope = require_str_param(params, "scope")?;
            let workspace_path = opt_str(params, "workspacePath");
            match api.specialist_edit(id, spec, scope, workspace_path).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "specialist.delete" => {
            let id = require_str_param(params, "id")?;
            let scope = require_str_param(params, "scope")?;
            let workspace_path = opt_str(params, "workspacePath");
            match api.specialist_delete(id, scope, workspace_path).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "skill.list" => {
            let ws_id = require_workspace_id(params)?;
            match api.skill_list(ws_id).await {
                Ok(v) => Ok(v),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "mcp.servers.list" => {
            // Global (no required scope); optional workspaceId per PROTOCOL §5.22.
            let workspace_id = opt_workspace_id(params);
            api.mcp_servers_list(workspace_id)
                .await
                .map_err(domain_to_rpc)
        }
        "mcp.servers.create" => {
            require_present(params, "config")?;
            let config = params.get("config").cloned().unwrap_or(Value::Null);
            match api.mcp_servers_create(config).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "mcp.servers.update" => {
            let server_id = require_str_param(params, "serverId")?;
            require_present(params, "config")?;
            let config = params.get("config").cloned().unwrap_or(Value::Null);
            match api.mcp_servers_update(server_id, config).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "mcp.servers.delete" => {
            let server_id = require_str_param(params, "serverId")?;
            match api.mcp_servers_delete(server_id).await {
                Ok(v) => Ok(v),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "mcp.servers.toggle" => {
            let server_id = require_str_param(params, "serverId")?;
            let enabled = params
                .get("enabled")
                .and_then(Value::as_bool)
                .ok_or_else(|| invalid_params("enabled is required"))?;
            match api.mcp_servers_toggle(server_id, enabled).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "mcp.servers.restart" => {
            let server_id = require_str_param(params, "serverId")?;
            match api.mcp_servers_restart(server_id).await {
                Ok(v) => Ok(v),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "mcp.servers.getStatus" => {
            let server_id = require_str_param(params, "serverId")?;
            match api.mcp_servers_get_status(server_id).await {
                Ok(v) => Ok(v),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "mcp.oauth.list" => api.mcp_oauth_list().await.map_err(domain_to_rpc),
        "mcp.oauth.get" => {
            let server_id = require_str_param(params, "serverId")?;
            match api.mcp_oauth_get(server_id).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "mcp.oauth.set" => {
            let server_id = require_str_param(params, "serverId")?;
            require_present(params, "tokenBag")?;
            let token_bag = params.get("tokenBag").cloned().unwrap_or(Value::Null);
            match api.mcp_oauth_set(server_id, token_bag).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "mcp.oauth.delete" => {
            let server_id = require_str_param(params, "serverId")?;
            match api.mcp_oauth_delete(server_id).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(Error::NotFound(m)) => Err(not_found(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        "mcp.testConnection" => {
            let url = require_str_param(params, "url")?;
            if url.trim().is_empty() {
                return Err(invalid_params("url is required"));
            }
            let headers = params.get("headers").cloned().filter(|v| !v.is_null());
            let server_name = opt_str(params, "serverName");
            match api.mcp_test_connection(url, headers, server_name).await {
                Ok(v) => Ok(v),
                Err(Error::InvalidParams(m)) => Err(invalid_params(m)),
                Err(e) => Err(domain_to_rpc(e)),
            }
        }
        _ => Err(rpc(METHOD_NOT_FOUND, "Method not found")),
    }
}

/// Extract a required `workspaceId` for `note.*` methods, matching the TS
/// message `workspaceId is required` (distinct from the `workspace.*` wording).
fn require_ws_note(params: &Map<String, Value>) -> Result<WorkspaceId, RpcErr> {
    match params.get("workspaceId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => Ok(WorkspaceId::from(s)),
        _ => Err(invalid_params("workspaceId is required")),
    }
}

/// Require a `noteId` string param (`requireParam` parity: present & non-null).
fn require_note_id(params: &Map<String, Value>) -> Result<NoteId, RpcErr> {
    require_str_param(params, "noteId").map(NoteId::from)
}

/// Require an `agentId` string param (`requireParam` parity).
fn require_agent_id(params: &Map<String, Value>) -> Result<AgentId, RpcErr> {
    require_str_param(params, "agentId").map(|s| AgentId::from(s.as_str()))
}

/// Optional `workspaceId` for `agent.*` methods where it is a non-required
/// fallback (`agent.get`/`agent.getConversation`/`agent.delete`).
fn opt_workspace_id(params: &Map<String, Value>) -> Option<WorkspaceId> {
    params
        .get("workspaceId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(WorkspaceId::from)
}

/// Optional pass-through JSON param (absent/null → `None`); used for the opaque
/// `imageBlocks` / `noteIds` payloads.
fn opt_value(params: &Map<String, Value>, name: &str) -> Option<Value> {
    match params.get(name) {
        None | Some(Value::Null) => None,
        Some(v) => Some(v.clone()),
    }
}

/// Maximum accepted `userAppMessageId` length (bytes) — parity with the
/// service-layer `messageId` cap so neither client-supplied id can bloat the
/// row metadata unbounded.
const MAX_USER_APP_MESSAGE_ID_LEN: usize = 256;

/// Fold an optional top-level `userAppMessageId` param into the request's
/// `messageMetadata` object under [`intent_core::USER_APP_MESSAGE_ID_KEY`]
/// (PROTOCOL §5.5) so the id persists on the user message row without a
/// schema change and round-trips as the wire `appMessageId`. Absent/empty ids
/// leave the metadata untouched (backward compatible). An explicit
/// `messageMetadata` copy of the key is preserved only when no top-level
/// param is supplied (top-level wins). Errors: oversized id, or an id
/// combined with a non-object `messageMetadata` (nowhere to fold it).
fn merge_user_app_message_id(
    params: &Map<String, Value>,
    message_metadata: Option<Value>,
) -> Result<Option<Value>, RpcErr> {
    // Trim before folding (symmetric with `lift_app_message_id`): padding
    // must not persist verbatim or count against the length cap, and a
    // whitespace-only id reads as absent.
    let Some(id) = opt_nonempty_str(params, intent_core::USER_APP_MESSAGE_ID_KEY)
        .map(|s| s.trim().to_string())
    else {
        return Ok(message_metadata);
    };
    if id.len() > MAX_USER_APP_MESSAGE_ID_LEN {
        return Err(invalid_params(format!(
            "userAppMessageId exceeds maximum length of {MAX_USER_APP_MESSAGE_ID_LEN} bytes"
        )));
    }
    let mut obj = match message_metadata {
        None => Map::new(),
        Some(Value::Object(m)) => m,
        Some(_) => {
            return Err(invalid_params(
                "messageMetadata must be an object when userAppMessageId is supplied",
            ))
        }
    };
    obj.insert(
        intent_core::USER_APP_MESSAGE_ID_KEY.to_string(),
        Value::String(id),
    );
    Ok(Some(Value::Object(obj)))
}

/// Require a string param, mirroring TS `requireParam` (undefined/null → error).
fn require_str_param(params: &Map<String, Value>, name: &str) -> Result<String, RpcErr> {
    match params.get(name) {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(invalid_params(format!(
            "Missing required parameter: {name}"
        ))),
    }
}

/// Require an integer param (e.g. `v` on `note.getVersion`/`restoreVersion`).
fn require_int_param(params: &Map<String, Value>, name: &str) -> Result<i64, RpcErr> {
    match params.get(name).and_then(Value::as_i64) {
        Some(v) => Ok(v),
        None => Err(invalid_params(format!(
            "Missing required parameter: {name}"
        ))),
    }
}

/// Require a non-empty string param (used for `linear.createIssue`/`updateIssue`
/// where the empty string is just as invalid as a missing field).
fn require_non_empty_str(params: &Map<String, Value>, name: &str) -> Result<String, RpcErr> {
    match params.get(name) {
        Some(Value::String(s)) if !s.trim().is_empty() => Ok(s.clone()),
        _ => Err(invalid_params(format!(
            "Missing required parameter: {name}"
        ))),
    }
}

/// Require a `u64` param (used for the explicit `github.*` `number` /
/// `commentId`); absent/non-numeric → `-32602`.
fn require_u64(params: &Map<String, Value>, name: &str) -> Result<u64, RpcErr> {
    params
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid_params(format!("Missing required parameter: {name}")))
}

/// Require a param be present and non-null (used for numeric `start`/`end`).
fn require_present(params: &Map<String, Value>, name: &str) -> Result<(), RpcErr> {
    match params.get(name) {
        Some(Value::Null) | None => Err(invalid_params(format!(
            "Missing required parameter: {name}"
        ))),
        Some(_) => Ok(()),
    }
}

/// Optional string param (absent/null/non-string → `None`).
fn opt_str(params: &Map<String, Value>, name: &str) -> Option<String> {
    params.get(name).and_then(Value::as_str).map(str::to_string)
}

/// Like [`opt_str`] but treats empty and whitespace-only values as absent, so
/// downstream code cannot distinguish `Some("")` from `None`. Used at the
/// router boundary for optional hints where an empty string would be
/// ambiguous (e.g. `git.clone`'s `requestId` correlator or `agent.create`'s
/// widened `provider`/`agentType`/`workspacePath` fields).
fn opt_nonempty_str(params: &Map<String, Value>, name: &str) -> Option<String> {
    opt_str(params, name).filter(|s| !s.trim().is_empty())
}

/// Optional `gitRootId` param on the `git.*` reads (§5.6 extension,
/// monorepo#2053). Empty/whitespace-only values are treated as absent so a
/// client sending `gitRootId: ""` gets the primary-worktree behavior.
fn opt_git_root_id(params: &Map<String, Value>) -> Option<WorkspaceGitRootId> {
    opt_nonempty_str(params, "gitRootId").map(WorkspaceGitRootId::from)
}

/// Parse the `items` array from `workspace.updateContext` into a
/// `Vec<ContextItem>`. The daemon treats each item as an opaque JSON blob
/// authored by the FE (`ContextItem` union in
/// `packages/cloudlands-fe/src/features/context/types.ts`); the only
/// required field is a non-empty string `id`, and ids must be unique inside
/// a single request (the store keys rows by `(workspaceId, id)`). `items`
/// itself must be an array — absent, non-array, per-item shape errors, and
/// duplicate ids surface as `-32602 Invalid params`.
fn require_context_items(params: &Map<String, Value>) -> Result<Vec<ContextItem>, RpcErr> {
    let raw = params
        .get("items")
        .ok_or_else(|| invalid_params("items is required"))?;
    let arr = raw
        .as_array()
        .ok_or_else(|| invalid_params("items must be an array"))?;
    let mut out = Vec::with_capacity(arr.len());
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(arr.len());
    for (idx, entry) in arr.iter().enumerate() {
        let item: ContextItem = serde_json::from_value(entry.clone())
            .map_err(|e| invalid_params(format!("items[{idx}] is not a valid ContextItem: {e}")))?;
        if item.id.trim().is_empty() {
            return Err(invalid_params(format!(
                "items[{idx}].id must be a non-empty string"
            )));
        }
        if !seen.insert(item.id.clone()) {
            return Err(invalid_params(format!(
                "items[{idx}].id is a duplicate: {:?}",
                item.id
            )));
        }
        out.push(item);
    }
    Ok(out)
}

/// Group a flat list of task↔agent links into the FE-parity
/// `byNoteId → byTaskKey → TaskAgentLink` map (matches
/// `TaskAgentAssociationsState.byNoteId[noteId][taskKey]` so the FE
/// hydration is a mechanical cut-over from
/// `localStorage["task-agent-associations:{wsId}"]`). `TaskAgentLink`'s
/// fields (strings + `i64`) are all trivially serializable — a
/// `serde_json::to_value` failure here would indicate a corrupted process,
/// so an `expect` is preferable to silently emitting a malformed
/// `linksByNoteId` shape.
fn links_by_note_id(links: &[TaskAgentLink]) -> Value {
    let mut by_note: Map<String, Value> = Map::new();
    for link in links {
        let note_id = link.note_id.as_str().to_string();
        let entry = by_note
            .entry(note_id)
            .or_insert_with(|| Value::Object(Map::new()));
        if let Value::Object(inner) = entry {
            let value =
                serde_json::to_value(link).expect("TaskAgentLink is always serializable to JSON");
            inner.insert(link.task_key.clone(), value);
        }
    }
    Value::Object(by_note)
}

/// Optional string-array param (absent/null/non-array → `None`); non-string
/// elements are skipped. Used for the `git.agentCommit` `files` list.
fn opt_str_array(params: &Map<String, Value>, name: &str) -> Option<Vec<String>> {
    params.get(name).and_then(Value::as_array).map(|items| {
        items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    })
}

/// Optional note-id-array param — `opt_str_array` mapped into `NoteId`s. Used
/// for the `task.setRelations` / `task.markAsTask` relation lists.
fn opt_note_id_array(params: &Map<String, Value>, name: &str) -> Option<Vec<NoteId>> {
    opt_str_array(params, name).map(|ids| ids.into_iter().map(NoteId::from).collect())
}

/// Strict optional string-array param: absent/null → `Ok(None)`; an array of
/// strings → `Ok(Some(..))`; anything else (non-array, or an array with a
/// non-string element) → `-32602`. Used for the `git.diffs` `paths` set.
fn strict_opt_str_array(
    params: &Map<String, Value>,
    name: &str,
) -> Result<Option<Vec<String>>, RpcErr> {
    let value = match params.get(name) {
        None | Some(Value::Null) => return Ok(None),
        Some(v) => v,
    };
    let items = value
        .as_array()
        .ok_or_else(|| invalid_params(format!("`{name}` must be an array of strings")))?;
    items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| invalid_params(format!("`{name}` must be an array of strings")))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// Build [`ScriptCreateParams`] from `script.create` params: `name`, `command`,
/// and `mode` (`service`|`command`) are required; the rest are optional.
fn parse_script_create(params: &Map<String, Value>) -> Result<ScriptCreateParams, RpcErr> {
    let name = require_str_param(params, "name")?;
    let command = require_str_param(params, "command")?;
    let mode = match require_str_param(params, "mode")?.as_str() {
        "service" => ScriptMode::Service,
        "command" => ScriptMode::Command,
        other => {
            return Err(invalid_params(format!(
                "Invalid mode: {other} (expected \"service\" or \"command\")"
            )))
        }
    };
    Ok(ScriptCreateParams {
        name,
        command,
        mode,
        cwd: opt_str(params, "cwd"),
        env: opt_string_map(params, "env"),
        category: opt_str(params, "category"),
        auto_start: opt_bool(params, "autoStart"),
        script_id: opt_str(params, "scriptId"),
    })
}

/// Optional boolean param (absent/null/non-bool → `None`).
fn opt_bool(params: &Map<String, Value>, name: &str) -> Option<bool> {
    params.get(name).and_then(Value::as_bool)
}

/// Like [`opt_bool`] but strict: absent/null → `None`, a real bool →
/// `Some(..)`, anything else → `-32602`. Used where silently dropping a
/// non-boolean would change a persisted flag's default (e.g. `agent.create`'s
/// `nameExplicitlySet`).
fn opt_bool_strict(params: &Map<String, Value>, name: &str) -> Result<Option<bool>, RpcErr> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(invalid_params(format!("{name} must be a boolean"))),
    }
}

/// Optional string→string map param (absent/non-object → `None`); non-string
/// values are skipped. Used for the `script.create` `env` overrides.
fn opt_string_map(
    params: &Map<String, Value>,
    name: &str,
) -> Option<std::collections::BTreeMap<String, String>> {
    params.get(name).and_then(Value::as_object).map(|obj| {
        obj.iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect()
    })
}

/// Optional integer param from a JSON number (absent/non-number → `None`).
/// Used by the `event.*` `limit` / `minutesAgo` knobs, whose defaults are
/// applied in the service layer (`value || default`).
// Whole-valued floats from JSON clients; float→int casts saturate.
#[allow(clippy::cast_possible_truncation)]
fn opt_int(params: &Map<String, Value>, name: &str) -> Option<i64> {
    match params.get(name) {
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        _ => None,
    }
}

/// Parse the optional `projection` param on conversation reads (§5.5):
/// absent / `null` mean full fidelity (`None`), `"slim"` selects the bounded
/// tool/image projection, anything else is `-32602`.
fn parse_projection(
    params: &Map<String, Value>,
) -> Result<Option<intent_core::ConversationProjection>, RpcErr> {
    match params.get("projection") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s == "slim" => {
            Ok(Some(intent_core::ConversationProjection::Slim))
        }
        Some(_) => Err(invalid_params("projection must be \"slim\"")),
    }
}

/// Optional terminal dimension (`cols`/`rows`) clamped into `u16`, defaulting
/// when absent/non-numeric (mirrors the ancestor's `value || default`).
fn opt_dim(params: &Map<String, Value>, name: &str, default: u16) -> u16 {
    match opt_int(params, name) {
        Some(n) if n > 0 => u16::try_from(n).unwrap_or(default),
        _ => default,
    }
}

/// Normalize a `tags` param (array or comma string) into trimmed, non-empty
/// entries, mirroring the TS `normalizeTags`.
fn opt_tags(params: &Map<String, Value>, name: &str) -> Option<Vec<String>> {
    match params.get(name) {
        Some(Value::Array(items)) => Some(
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        ),
        Some(Value::String(s)) => Some(
            s.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect(),
        ),
        _ => None,
    }
}

/// `confirmReplacement` accepts a boolean or the string `"true"` (TS parity).
fn parse_confirm(params: &Map<String, Value>) -> bool {
    match params.get("confirmReplacement") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    }
}

/// Parse the §5.5 paginated-read page params into `(limit, continuation_token)`.
/// The canonical form nests them under `page` ({ continuationToken, limit });
/// for parity with the other paginated reads we fall back to top-level `limit`
/// and `nextToken` when no `page` object is present.
// Whole-valued floats from JSON clients; float→int casts saturate.
#[allow(clippy::cast_possible_truncation)]
fn parse_page_params(params: &Map<String, Value>) -> (Option<i64>, Option<String>) {
    if let Some(Value::Object(page)) = params.get("page") {
        let limit = page
            .get("limit")
            .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)));
        let token = page
            .get("continuationToken")
            .and_then(Value::as_str)
            .map(str::to_string);
        return (limit, token);
    }
    (opt_int(params, "limit"), opt_str(params, "nextToken"))
}

/// Parse a boolean flag param: a real bool, or the string `"true"`.
fn parse_bool(params: &Map<String, Value>, name: &str) -> bool {
    match params.get(name) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    }
}

/// Normalize `task.markAsTask` `acceptanceCriteria`: a string array as-is, a
/// JSON-array string parsed, or any other string wrapped as a single entry
/// (mirrors the TS `Array.isArray ? … : JSON.parse(…) ?? [value]` branch).
fn normalize_acceptance_criteria(params: &Map<String, Value>) -> Vec<String> {
    match params.get("acceptanceCriteria") {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        Some(Value::String(s)) => match serde_json::from_str::<Vec<String>>(s) {
            Ok(v) => v,
            Err(_) => vec![s.clone()],
        },
        _ => Vec::new(),
    }
}

/// Loosely parse an integer from a JSON number or leading-int string
/// (`parseInt`-like), returning `None` when no integer is present.
// Whole-valued floats from JSON clients; float→int casts saturate.
#[allow(clippy::cast_possible_truncation)]
fn parse_int_loose(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Some(Value::String(s)) => parse_leading_int(s),
        _ => None,
    }
}

fn parse_leading_int(s: &str) -> Option<i64> {
    let t = s.trim_start();
    let bytes = t.as_bytes();
    let mut i = 0;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        i += 1;
    }
    let digit_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == digit_start {
        return None;
    }
    t[..i].parse::<i64>().ok()
}

/// Serialize a typed result into a JSON-RPC `result` value.
fn to_result_value<T: serde::Serialize>(value: &T) -> Result<Value, RpcErr> {
    serde_json::to_value(value)
        .map_err(|e| domain_to_rpc(Error::Internal(format!("serialize result failed: {e}"))))
}

/// Extract a required `workspaceId` string param, or `-32602` with the exact
/// message the TS handler emits via `requireParam` (PROTOCOL §5.1).
fn require_workspace_id(params: &Map<String, Value>) -> Result<WorkspaceId, RpcErr> {
    match params.get("workspaceId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => Ok(WorkspaceId::from(s)),
        _ => Err(invalid_params("Missing required parameter: workspaceId")),
    }
}

/// Map a domain [`Error`] for `workspace.*` methods: a missing workspace surfaces
/// as `-32602 "Workspace not found"`, matching the TS handler (PROTOCOL §5.1).
fn workspace_err(e: Error) -> RpcErr {
    match e {
        Error::NotFound(_) => not_found("Workspace not found"),
        other => domain_to_rpc(other),
    }
}

/// Serialize a success envelope. `result` is always a JSON object (§3.2).
fn success_string(id: &Value, result: &Value) -> String {
    let resp = json!({ "jsonrpc": "2.0", "result": result, "id": id });
    serde_json::to_string(&resp).unwrap_or_else(|_| internal_fallback())
}

/// Serialize an error envelope, optionally carrying `data`.
fn error_string(id: &Value, code: i32, message: &str, data: Option<Value>) -> String {
    let mut err = Map::new();
    err.insert("code".to_string(), json!(code));
    err.insert("message".to_string(), json!(message));
    if let Some(d) = data {
        err.insert("data".to_string(), d);
    }
    let resp = json!({ "jsonrpc": "2.0", "error": Value::Object(err), "id": id });
    serde_json::to_string(&resp).unwrap_or_else(|_| internal_fallback())
}

/// Replace a serialized response that exceeds
/// [`crate::MAX_OUTBOUND_MESSAGE_BYTES`] with an [`OVERSIZED_RESPONSE`] error
/// echoing the request id, so the client fails fast instead of hitting its
/// RPC timeout on a silently dropped frame. The writer-task cap remains as a
/// last-resort backstop for non-response frames (subscription pushes/events).
fn oversized_response_string(id: &Value, method: &str, response_bytes: usize) -> String {
    tracing::error!(
        method,
        response_bytes,
        limit = crate::MAX_OUTBOUND_MESSAGE_BYTES,
        "oversized JSON-RPC response replaced with error"
    );
    error_string(
        id,
        OVERSIZED_RESPONSE,
        &format!(
            "response for {method} exceeds maximum outbound frame size: {response_bytes} bytes > {} bytes",
            crate::MAX_OUTBOUND_MESSAGE_BYTES
        ),
        Some(json!({
            "code": "oversized-response",
            "method": method,
            "responseBytes": response_bytes,
            "limit": crate::MAX_OUTBOUND_MESSAGE_BYTES,
        })),
    )
}

/// Last-resort response if serialization itself fails (should never happen).
fn internal_fallback() -> String {
    r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"},"id":null}"#.to_string()
}

#[cfg(test)]
mod tests;
