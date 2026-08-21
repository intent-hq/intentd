//! Ephemeral one-shot ACP runner: spawn an adapter, `initialize` +
//! `session/new` + one `session/prompt`, collect the streamed reply text, and
//! kill the child.
//!
//! This is the provider-neutral engine behind `agent.completeOnce` for
//! providers served by an ACP adapter (claude-code / codex / pi): a quick
//! action gets a full reply without ever creating a persistent agent session.
//! The launch description, staged npx-aware timeouts, spawn, exit
//! observation, and process-group reaping are shared with the model probe via
//! [`crate::acp_adapter`]; this module owns only the one-shot stage
//! sequencing.
//!
//! Non-interactive by construction: the session is created with no MCP
//! servers and no client filesystem capabilities, and every agent→client
//! request (`session/request_permission` above all) is answered immediately —
//! permissions with a `cancelled` outcome, anything else with a
//! method-not-found error — so a one-shot can never hang waiting on a human.
//! That auto-answer posture covers the ENTIRE lifecycle: incoming requests
//! are serviced concurrently during setup (`initialize` + `session/new`) and
//! the optional model application, not just during the prompt phase.
//! The child is reaped on every exit path (success, timeout, error, drop).
//!
//! A caller-requested model for a provider with no CLI model flag is applied
//! best-effort after `session/new` via `session/set_config_option`
//! (`configOptions[id="model"]`, the same mechanism the persistent agent
//! path uses for claude-code and pi); a failed or unsupported attempt is
//! logged and the completion proceeds on the adapter's default model.

use std::time::Duration;

use intent_acp::{Connection, IncomingRequest, JsonRpcError, PermissionOutcome};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::acp_adapter::{
    adapter_slots, exited_detail, initialize_params, observe_exit_status, reap_child,
    spawn_adapter_in, AcpAdapterCommand, AdapterSlots, SpawnError,
};

/// The one-shot launch description (shared with the model probe).
pub(crate) use crate::acp_adapter::AcpAdapterCommand as OneShotCommand;

/// Machine-readable one-shot failure reasons. The caller maps these onto the
/// `agent.completeOnce` contract (`{ available: false, reason }` vs an error).
#[derive(Debug)]
pub(crate) enum OneShotError {
    /// The caller's timeout expired while queued for a slot in the daemon-wide
    /// adapter bound — nothing was ever spawned, and no model was ever asked
    /// (monorepo#2062).
    QueueTimeout { waited_ms: u64, limit: u32 },
    /// The adapter process could not be spawned.
    Spawn(String),
    /// A request failed at the transport level or timed out.
    Transport(String),
    /// The adapter returned a JSON-RPC error (auth detection keys off this).
    Rpc(JsonRpcError),
    /// The setup phase (`initialize` + `session/new`) hit its hard cap.
    SetupTimeout,
    /// The `session/prompt` phase hit the caller's timeout.
    PromptTimeout,
    /// The turn completed but the adapter streamed no assistant text.
    Empty,
    /// The adapter exited unsuccessfully before the turn completed; carries
    /// the exit status plus a bounded tail of recent stderr when available.
    Exited(String),
}

impl std::fmt::Display for OneShotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OneShotError::QueueTimeout { waited_ms, limit } => write!(
                f,
                "timed out after {waited_ms}ms waiting for a free adapter slot \
                 (limit {limit}); no completion was started"
            ),
            OneShotError::Spawn(e) => write!(f, "failed to spawn adapter: {e}"),
            OneShotError::Transport(e) => write!(f, "one-shot transport failed: {e}"),
            OneShotError::Rpc(e) => write!(f, "adapter returned an error: {e}"),
            OneShotError::SetupTimeout => write!(f, "one-shot session setup timed out"),
            OneShotError::PromptTimeout => write!(f, "one-shot prompt timed out"),
            OneShotError::Empty => write!(f, "adapter returned no text"),
            OneShotError::Exited(detail) => {
                write!(f, "adapter exited before completing the turn: {detail}")
            }
        }
    }
}

/// Run one ephemeral ACP completion: claim a slot in the daemon-wide adapter
/// bound, spawn `cmd`, drive the turn with `prompt` as a single text content
/// block, and return the concatenated assistant text. `prompt_timeout` bounds
/// two independent phases: the wait for a slot, and then the
/// `session/prompt` phase — setup uses the launch's npx-aware staged budgets,
/// as before. `config_option_model`, when set, is applied best-effort after
/// `session/new` via `session/set_config_option` (a failure never fails the
/// completion). The child is reaped before returning on every path.
///
/// Reusing the caller's own timeout as the queue budget keeps the contract
/// legible — you wait for a slot at most as long as you were willing to wait
/// for a reply — and a caller that never gets one fails with
/// [`OneShotError::QueueTimeout`], never a hang and never something a client
/// could mistake for a slow model.
pub(crate) async fn run_one_shot_acp(
    cmd: OneShotCommand,
    prompt: &str,
    config_option_model: Option<&str>,
    prompt_timeout: Duration,
) -> Result<String, OneShotError> {
    run_one_shot_acp_in(
        adapter_slots(),
        cmd,
        prompt,
        config_option_model,
        prompt_timeout,
    )
    .await
}

/// [`run_one_shot_acp`] against a caller-supplied adapter bound instead of the
/// process-global one. Production always goes through [`run_one_shot_acp`];
/// this seam lets a test with a deliberately short prompt budget run against a
/// private [`AdapterSlots`], so slot pressure from sibling tests sharing the
/// global bound cannot turn its asserted failure into a queue timeout
/// (monorepo#2379).
pub(crate) async fn run_one_shot_acp_in(
    slots: &AdapterSlots,
    cmd: OneShotCommand,
    prompt: &str,
    config_option_model: Option<&str>,
    prompt_timeout: Duration,
) -> Result<String, OneShotError> {
    let mut adapter = spawn_adapter_in(slots, &cmd, prompt_timeout)
        .await
        .map_err(|e| match e {
            SpawnError::QueueTimeout { waited, limit } => OneShotError::QueueTimeout {
                waited_ms: u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
                limit,
            },
            SpawnError::Spawn(detail) => OneShotError::Spawn(detail),
        })?;

    let result = drive_one_shot(
        &adapter.conn,
        &mut adapter.notifications,
        &mut adapter.requests,
        &cmd,
        prompt,
        config_option_model,
        prompt_timeout,
    )
    .await;

    let result = match result {
        Ok(text) => Ok(text),
        Err(err) => Err(attribute_early_exit(err, &mut adapter.child, &adapter.conn).await),
    };
    reap_child(&mut adapter.child).await;
    result
}

/// `initialize` → `session/new` (both under the launch's staged setup cap) →
/// best-effort model application → one `session/prompt` bounded by
/// `prompt_timeout`, accumulating `agent_message_chunk` text while answering
/// agent→client requests inline through every phase.
async fn drive_one_shot(
    conn: &Connection,
    notifications: &mut mpsc::UnboundedReceiver<intent_acp::IncomingNotification>,
    requests: &mut mpsc::UnboundedReceiver<IncomingRequest>,
    cmd: &AcpAdapterCommand,
    prompt: &str,
    config_option_model: Option<&str>,
    prompt_timeout: Duration,
) -> Result<String, OneShotError> {
    // Setup is serviced too: an adapter that sends
    // `session/request_permission` during `initialize` or `session/new`
    // still gets the immediate auto-deny instead of stalling setup into a
    // misreported SetupTimeout.
    let session_id = serve_requests_while(
        conn,
        requests,
        tokio::time::timeout(
            cmd.setup_timeout(),
            setup_session(
                conn,
                cmd.working_dir(),
                cmd.initialize_timeout(),
                cmd.session_new_timeout(),
            ),
        ),
    )
    .await
    .unwrap_or(Err(OneShotError::SetupTimeout))?;

    if let Some(model) = config_option_model {
        apply_config_option_model(
            conn,
            requests,
            &session_id,
            model,
            cmd.session_new_timeout(),
        )
        .await;
    }

    let params = json!({
        "sessionId": session_id,
        "prompt": [{ "type": "text", "text": prompt }],
    });
    // `prompt_timeout` is passed as the transport request timeout, so it is
    // the single bound on the prompt phase. Dropping the request future on
    // timeout is cancel-safe — the transport's drop guard removes the
    // pending entry.
    let prompt_fut = conn.request_timeout("session/prompt", params, prompt_timeout);
    tokio::pin!(prompt_fut);

    let mut text = String::new();
    let mut notifications_open = true;
    let mut requests_open = true;
    let outcome = loop {
        tokio::select! {
            resp = &mut prompt_fut => break resp,
            note = notifications.recv(), if notifications_open => match note {
                // The turn's assistant text arrives as streamed chunks; the
                // `session/prompt` response itself carries only a stopReason.
                Some(note) => append_chunk_text(&note, &mut text),
                // Channel closed (connection dropped the sender): disable this
                // branch so the select! cannot busy-spin.
                None => notifications_open = false,
            },
            req = requests.recv(), if requests_open => match req {
                Some(req) => auto_respond(conn, req).await,
                None => requests_open = false,
            },
        }
    };

    match outcome {
        Ok(_) => {
            // Drain any chunk that raced the prompt response into the channel
            // before deciding the turn produced nothing.
            while let Ok(note) = notifications.try_recv() {
                append_chunk_text(&note, &mut text);
            }
            if text.trim().is_empty() {
                Err(OneShotError::Empty)
            } else {
                Ok(text)
            }
        }
        Err(intent_acp::AcpError::Timeout(_)) => Err(OneShotError::PromptTimeout),
        Err(err) => Err(map_acp_error(err)),
    }
}

/// Drive `fut` to completion while answering agent→client requests inline
/// (the same auto-deny/refuse posture as the prompt phase), so no phase of
/// the one-shot lifecycle can hang on an unanswered client-served request.
async fn serve_requests_while<F: std::future::Future>(
    conn: &Connection,
    requests: &mut mpsc::UnboundedReceiver<IncomingRequest>,
    fut: F,
) -> F::Output {
    tokio::pin!(fut);
    let mut requests_open = true;
    loop {
        tokio::select! {
            out = &mut fut => return out,
            req = requests.recv(), if requests_open => match req {
                Some(req) => auto_respond(conn, req).await,
                // Channel closed (connection dropped the sender): disable
                // this branch so the select! cannot busy-spin.
                None => requests_open = false,
            },
        }
    }
}

/// Best-effort `session/set_config_option { configId: "model" }`: the model
/// mechanism for providers with no CLI model flag (claude-code / pi expose
/// the model as a `configOptions[id="model"]` select in the `session/new`
/// result). A rejected, unsupported, or timed-out attempt is logged and the
/// completion proceeds on the adapter's default model — a best-effort model
/// is never an error.
async fn apply_config_option_model(
    conn: &Connection,
    requests: &mut mpsc::UnboundedReceiver<IncomingRequest>,
    session_id: &str,
    model: &str,
    timeout: Duration,
) {
    let params = json!({ "sessionId": session_id, "configId": "model", "value": model });
    let outcome = serve_requests_while(
        conn,
        requests,
        conn.request_timeout("session/set_config_option", params, timeout),
    )
    .await;
    if let Err(err) = outcome {
        tracing::debug!(
            "one-shot session/set_config_option(model={model}) failed; \
             continuing with the adapter default: {err}"
        );
    }
}

/// `initialize` then `session/new` with no MCP servers, returning the
/// adapter's session id.
async fn setup_session(
    conn: &Connection,
    cwd: std::path::PathBuf,
    initialize_timeout: Duration,
    session_new_timeout: Duration,
) -> Result<String, OneShotError> {
    conn.request_timeout("initialize", initialize_params(), initialize_timeout)
        .await
        .map_err(map_acp_error)?;

    let session_params = json!({
        "cwd": cwd.to_string_lossy(),
        "mcpServers": [],
    });
    let result = conn
        .request_timeout("session/new", session_params, session_new_timeout)
        .await
        .map_err(map_acp_error)?;
    result
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| OneShotError::Transport("session/new returned no sessionId".to_string()))
}

/// Append the text of an `agent_message_chunk` `session/update` to the
/// accumulated reply. Non-text blocks and every other update variant
/// (thoughts, tool calls, plans) are ignored: the one-shot contract is the
/// assistant's visible answer.
fn append_chunk_text(note: &intent_acp::IncomingNotification, text: &mut String) {
    if note.method != "session/update" {
        return;
    }
    let Some(update) = note.params.get("update") else {
        return;
    };
    if update.get("sessionUpdate").and_then(Value::as_str) != Some("agent_message_chunk") {
        return;
    }
    let Some(content) = update.get("content") else {
        return;
    };
    if content.get("type").and_then(Value::as_str) != Some("text") {
        return;
    }
    if let Some(chunk) = content.get("text").and_then(Value::as_str) {
        text.push_str(chunk);
    }
}

/// Answer one agent→client request without any user interaction: a
/// `session/request_permission` resolves as `cancelled` (the auto-deny the
/// one-shot posture requires), and any other client-served method is refused
/// with method-not-found so the adapter fails fast instead of blocking.
async fn auto_respond(conn: &Connection, req: IncomingRequest) {
    if req.method == "session/request_permission" {
        let _ = conn
            .respond_result(req.id, PermissionOutcome::Cancelled.to_response_value())
            .await;
        return;
    }
    let _ = conn
        .respond_error(
            req.id,
            JsonRpcError {
                code: -32601,
                message: format!("{} is not served by one-shot completions", req.method),
                data: None,
            },
        )
        .await;
}

/// Fold an early adapter exit into the one-shot error: when the child already
/// exited before the turn completed, report the exit status plus a bounded
/// stderr tail instead of a generic transport/timeout/empty reason. Spawn and
/// RPC errors pass through untouched (auth detection keys off `Rpc`), as do
/// clean exits and a queue timeout (which never spawned a child to attribute
/// an exit to).
async fn attribute_early_exit(
    err: OneShotError,
    child: &mut tokio::process::Child,
    conn: &Connection,
) -> OneShotError {
    if matches!(
        err,
        OneShotError::Spawn(_) | OneShotError::Rpc(_) | OneShotError::QueueTimeout { .. }
    ) {
        return err;
    }
    let status = observe_exit_status(child, conn).await;
    match exited_detail(status, &conn.recent_stderr()) {
        Some(detail) => OneShotError::Exited(detail),
        None => err,
    }
}

fn map_acp_error(err: intent_acp::AcpError) -> OneShotError {
    match err {
        // The transport synthesizes a code-0 "agent stdout closed" JSON-RPC
        // error when the child's stdout closes with requests still pending.
        // That is a transport failure, not an adapter response — keeping it
        // out of `Rpc` lets exit attribution rewrite it and keeps auth
        // detection keyed to genuine adapter errors.
        intent_acp::AcpError::Rpc(e) if e.code == 0 && e.message == "agent stdout closed" => {
            OneShotError::Transport(e.message)
        }
        intent_acp::AcpError::Rpc(e) => OneShotError::Rpc(e),
        other => OneShotError::Transport(other.to_string()),
    }
}

#[cfg(test)]
mod tests;
