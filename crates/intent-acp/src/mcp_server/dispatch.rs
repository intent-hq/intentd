//! Tool-call dispatch: after the WSAPI-8 cutover the daemon exposes exactly
//! one MCP tool — `workspace_api` — whose arguments carry agent-supplied
//! JavaScript that is evaluated against the shared `WorkspaceApi` via the
//! `ws.*` bindings in [`super::bindings`] (the "two front doors" rule; the
//! FE's JSON-RPC router uses the same trait, §6.8).

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::{
    new_attachment_id, now_iso, AgentId, AttachmentPolicy, TurnAttachment, TurnAttachmentRegistry,
    WorkspaceApi, WorkspaceId, ATTACHMENT_ID_KEY,
};
use intent_js::{eval as js_eval, BoxFuture, EvalOptions, HostFn, JsError};
use serde_json::{json, Value};

/// Per-invocation collector of turn attachments stamped at binding call time
/// (monorepo#2637): every `host({...})` frame whose result carries
/// `__mcpContentItems` contributes its resource items here, so registration
/// at the tool result no longer depends on the agent's JS returning the
/// envelope. Shared between the host closure and `dispatch_workspace_api`.
///
/// Frames are keyed by a sequence number taken synchronously when the JS
/// invokes `host()` — NOT when the dispatch future resolves — so parallel
/// calls (`Promise.all([...])`) drain in call order even when an earlier
/// call's I/O finishes later.
#[derive(Default)]
struct PendingAttachmentState {
    /// Next `host()` frame sequence number (call order).
    next_seq: u64,
    /// Collected batches, keyed by their frame's sequence number.
    entries: Vec<(u64, Vec<TurnAttachment>)>,
}

impl PendingAttachmentState {
    /// Flatten the collected batches in `host()` call order.
    fn drain_in_call_order(&mut self) -> Vec<TurnAttachment> {
        let mut entries = std::mem::take(&mut self.entries);
        entries.sort_by_key(|(seq, _)| *seq);
        entries.into_iter().flat_map(|(_, batch)| batch).collect()
    }
}

/// Shared handle to [`PendingAttachmentState`] between the host closure and
/// `dispatch_workspace_api`.
type PendingAttachments = Arc<Mutex<PendingAttachmentState>>;

/// Per-dispatch stage tracker (monorepo#2709): names the stage a dispatch is
/// currently in so the sentinel task can attribute a wedge at default log
/// level. Shared between the dispatch body (which advances it) and the
/// sentinel task (which reads it when the eval budget elapses with the
/// dispatch still in flight).
struct DispatchStage(Mutex<&'static str>);

impl DispatchStage {
    fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new("eval")))
    }

    fn set(&self, stage: &'static str) {
        *self.0.lock().unwrap() = stage;
    }

    fn get(&self) -> &'static str {
        *self.0.lock().unwrap()
    }
}

/// Sentinel guard for one dispatch: a separate task that fires ONE warn —
/// naming the in-flight stage and the `summary` arg — when the dispatch is
/// still running well past the eval budget. Because it polls in its own
/// task, it fires even when the dispatch future is wedged inside a single
/// synchronous poll (the exact state the bridge watchdog aborts, which would
/// otherwise preempt any completion-time logging), and warn-level is visible
/// at the daemon's default `info` filter. Dropping the guard (normal
/// completion) aborts the task, so a within-budget call logs nothing extra.
///
/// Fires at 1.5× the eval budget: enough grace that an ordinary eval timeout
/// (which returns shortly after the budget and already warns at completion)
/// never double-warns, and strictly before the bridge watchdog deadline
/// (`max(120s, 2×budget)`) so an await-wedged dispatch is attributed before
/// the watchdog abort drops this guard.
struct StageSentinel(tokio::task::JoinHandle<()>);

impl StageSentinel {
    fn arm(stage: Arc<DispatchStage>, budget: Duration, summary: String) -> Self {
        Self(tokio::spawn(async move {
            tokio::time::sleep(budget + budget / 2).await;
            tracing::warn!(
                summary,
                stage = stage.get(),
                elapsed_ms = (budget + budget / 2).as_millis() as u64,
                budget_ms = budget.as_millis() as u64,
                "workspace_api dispatch still in flight past the JS eval budget"
            );
        }))
    }
}

impl Drop for StageSentinel {
    fn drop(&mut self) {
        self.0.abort();
    }
}

use super::WorkspaceMcpServer;

/// Wall-clock budget for one `workspace_api` invocation — matches the 30s
/// timeout in the reference `workspace-js-api-tool.ts`. Tests override it via
/// [`WorkspaceMcpServer::with_workspace_api_timeout`].
pub(super) const WORKSPACE_API_TIMEOUT: Duration = Duration::from_secs(30);

/// The effective default budget: `INTENTD_WORKSPACE_API_TIMEOUT_MS` when set
/// to a positive integer (defensive knob, monorepo#871), else
/// [`WORKSPACE_API_TIMEOUT`]. Read at server construction time.
pub(super) fn default_workspace_api_timeout() -> Duration {
    workspace_api_timeout_from(
        std::env::var("INTENTD_WORKSPACE_API_TIMEOUT_MS")
            .ok()
            .as_deref(),
    )
}

/// Parse an override in milliseconds; anything unset, non-numeric, or
/// non-positive keeps the [`WORKSPACE_API_TIMEOUT`] default.
fn workspace_api_timeout_from(raw: Option<&str>) -> Duration {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map_or(WORKSPACE_API_TIMEOUT, Duration::from_millis)
}

/// Default for `workspaceApi.maxOutputChars` — mirrors the settings-catalog
/// default in `intent-services`; used when `settings.get` fails.
const DEFAULT_MAX_OUTPUT_CHARS: usize = 100_000;

/// Default for `workspaceApi.toonOutput` — mirrors the settings-catalog
/// default in `intent-services`; used when `settings.get` fails.
const DEFAULT_TOON_OUTPUT: bool = true;

/// Head-preview size (characters) included in the redirect message when an
/// oversized output is written to a file.
const OUTPUT_PREVIEW_CHARS: usize = 2000;

impl WorkspaceMcpServer {
    /// WSAPI-2 dispatch: evaluate agent-supplied JavaScript against the
    /// workspace API and shape the MCP tool result in-line (reference parity
    /// with `workspace-js-api-tool.ts` — a pretty-printed JSON body on
    /// success, `(no return value)` for `undefined`, and a readable text
    /// body with `isError: true` on any JS-side failure).
    pub(super) async fn dispatch_workspace_api(&self, args: &Value) -> Value {
        let started = Instant::now();
        let Some(code) = args.get("code").and_then(Value::as_str) else {
            return workspace_api_error("`code` is required and must be a string");
        };
        // `summary` is required by the input schema but is not fed into the
        // engine — it is a UI hint for the caller, not part of the eval
        // environment. Accept and ignore for now.
        // Sub-agent bridges see `structuredQuestions` forced off in the
        // effective features (prelude/help pruning); the flag itself rides
        // separately so the dispatch deny can name the top-level-only rule.
        let effective_features = self.effective_agent_features();
        // Binding-time attachment collection (monorepo#2637): only when both
        // the registry and a caller agent are wired — otherwise host-time
        // stamping would mint nonces that never register.
        let pending: Option<PendingAttachments> =
            match (&self.turn_attachments, &self.caller_agent_id) {
                (Some(_), Some(_)) => Some(Arc::new(Mutex::new(PendingAttachmentState::default()))),
                _ => None,
            };
        let host = make_workspace_host_with_pending(
            self.api.clone(),
            self.workspace_id.clone(),
            self.caller_agent_id.clone(),
            self.turn_attachments.clone(),
            effective_features.clone(),
            self.is_sub_agent,
            pending.clone(),
        );
        // Wrap user code so the engine sees a small `{__k, __v}` envelope,
        // preserving the `undefined` vs `null` distinction that
        // `serde_json::Value` cannot represent on its own. `__k` is `"u"` for
        // an undefined return (prints "(no return value)") and `"v"` for a
        // JSON-serializable value (prints as pretty JSON, including `null`).
        let bindings_prelude = super::bindings::prelude_for(&effective_features);
        let full_code = format!(
            "{bindings_prelude}\n\
             const __wsapi_user = await (async () => {{ {code}\n}})();\n\
             return {{ __k: __wsapi_user === undefined ? 'u' : 'v', __v: __wsapi_user }};"
        );
        let opts = EvalOptions {
            timeout: self.workspace_api_timeout,
            ..EvalOptions::default()
        };
        // Stage breadcrumbs (monorepo#2709): trace-level markers bracket each
        // dispatch stage so a wedged dispatch — one the bridge watchdog later
        // aborts — leaves a trail naming the stage it never left. Normal
        // calls emit exactly one debug event (below); a dispatch exceeding
        // the eval budget upgrades it to warn. The stage tracker + sentinel
        // additionally warn IN FLIGHT — visible at the default `info` filter —
        // when the dispatch is still running well past the budget, because a
        // watchdog abort would preempt all completion-time logging.
        let stage = DispatchStage::new();
        let _sentinel = StageSentinel::arm(
            stage.clone(),
            self.workspace_api_timeout,
            args.get("summary")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        );
        tracing::trace!("workspace_api dispatch: eval starting");
        let eval_started = Instant::now();
        let eval_result = js_eval(&full_code, &opts, Some(host)).await;
        let eval_ms = eval_started.elapsed().as_millis() as u64;
        let eval_ok = eval_result.is_ok();
        tracing::trace!(eval_ms, eval_ok, "workspace_api dispatch: eval finished");
        // Drain the binding-time collection unconditionally: the attachments
        // were stamped and handed to the agent's JS with `ok: true`, so they
        // register no matter what the script did afterwards — discarded the
        // envelope, returned something else, or even threw (monorepo#2637).
        let pending_batch: Vec<TurnAttachment> = pending
            .map(|p| p.lock().unwrap().drain_in_call_order())
            .unwrap_or_default();
        let mut settings_ms: Option<u64> = None;
        let mut finalize_ms: Option<u64> = None;
        let result = match eval_result {
            Ok(v) => match v.get("__k").and_then(Value::as_str) {
                Some("u") => {
                    self.register_pending_attachments(pending_batch);
                    workspace_api_success("(no return value)")
                }
                Some("v") => {
                    let value = v.get("__v").cloned().unwrap_or(Value::Null);

                    // Check for __mcpContentItems (parity with FE workspace-js-api-tool line 281)
                    if let Some(content_items) =
                        value.get("__mcpContentItems").and_then(Value::as_array)
                    {
                        // §7.1 deterministic attach: register any resource
                        // content item in the turn-attachment registry (nonce
                        // stamped into the outgoing items) so the transcript
                        // writer does not depend on the provider echoing this
                        // result intact. Items already collected at binding
                        // call time (nonce in `pending_batch`) are not
                        // re-stamped or re-registered. No-op unless both the
                        // registry and a caller agent are wired.
                        let content_items =
                            self.register_turn_attachments(content_items, pending_batch);
                        // Return MCP content items directly
                        json!({
                            "content": content_items,
                            "isError": false,
                        })
                    } else {
                        self.register_pending_attachments(pending_batch);
                        // Error results and the `__mcpContentItems` pass-through
                        // above are exempt from both knobs: only the plain
                        // success body is TOON-encoded / size-limited.
                        // Each post-eval stage gets its own start marker: a
                        // dispatch that wedges in one of these awaits never
                        // reaches the tail event, so the last marker in the
                        // log names the stage it never left.
                        stage.set("settings read");
                        tracing::trace!("workspace_api dispatch: settings read starting");
                        let stage_started = Instant::now();
                        let (toon_output, max_chars) = self.workspace_api_output_settings().await;
                        settings_ms = Some(stage_started.elapsed().as_millis() as u64);
                        let (body, ext) = render_workspace_api_value(&value, toon_output);
                        stage.set("output finalize");
                        tracing::trace!("workspace_api dispatch: output finalize starting");
                        let stage_started = Instant::now();
                        let out = self
                            .finalize_workspace_api_output(body, ext, max_chars)
                            .await;
                        finalize_ms = Some(stage_started.elapsed().as_millis() as u64);
                        out
                    }
                }
                _ => {
                    self.register_pending_attachments(pending_batch);
                    workspace_api_error("Error: engine: unexpected workspace_api envelope")
                }
            },
            Err(e) => {
                self.register_pending_attachments(pending_batch);
                workspace_api_error(&format_js_error(&e))
            }
        };
        let total = started.elapsed();
        let total_ms = total.as_millis() as u64;
        if total > self.workspace_api_timeout {
            // Slow-dispatch marker (monorepo#2709): the whole dispatch took
            // longer than the JS eval budget, so the post-eval awaits (or a
            // stalled eval that beat the timeout by little) ate real time.
            // `summary` is the caller's UI hint for the call — safe to log.
            let summary = args.get("summary").and_then(Value::as_str).unwrap_or("");
            tracing::warn!(
                summary,
                eval_ms,
                eval_ok,
                settings_ms = ?settings_ms,
                finalize_ms = ?finalize_ms,
                total_ms,
                budget_ms = self.workspace_api_timeout.as_millis() as u64,
                "workspace_api dispatch exceeded the JS eval budget"
            );
        } else {
            tracing::debug!(
                eval_ms,
                eval_ok,
                settings_ms = ?settings_ms,
                finalize_ms = ?finalize_ms,
                total_ms,
                "workspace_api dispatch complete"
            );
        }
        result
    }

    /// Read the `workspaceApi.*` output knobs live for one invocation via
    /// `settings.get`, falling back to the catalog defaults when the settings
    /// backend errors or returns an unexpected shape.
    async fn workspace_api_output_settings(&self) -> (bool, usize) {
        let toon_output = match self
            .api
            .settings_get("workspaceApi.toonOutput".to_string())
            .await
        {
            Ok(v) => v
                .get("value")
                .and_then(Value::as_bool)
                .unwrap_or(DEFAULT_TOON_OUTPUT),
            Err(_) => DEFAULT_TOON_OUTPUT,
        };
        let max_chars = match self
            .api
            .settings_get("workspaceApi.maxOutputChars".to_string())
            .await
        {
            Ok(v) => v
                .get("value")
                .and_then(Value::as_f64)
                .filter(|n| n.is_finite() && *n >= 0.0)
                .map_or(DEFAULT_MAX_OUTPUT_CHARS, |n| n as usize),
            Err(_) => DEFAULT_MAX_OUTPUT_CHARS,
        };
        (toon_output, max_chars)
    }

    /// Enforce `workspaceApi.maxOutputChars` on one success text body: within
    /// the limit (or unlimited, `0`) the text passes through unchanged; over
    /// the limit the FULL text is written to `<workspace-folder>/tool-outputs/`
    /// — the workspace's own directory, a SIBLING of the repo checkout, never
    /// inside the git tree — and a short redirect message (total size, limit,
    /// absolute path, head preview, inspection hints) is returned instead.
    /// When the redirect cannot be written (e.g. no resolvable workspace
    /// directory) a truncated head — never more than `max_chars` of the
    /// output — is returned instead (monorepo#3038); the tool call never
    /// fails because of the redirect.
    async fn finalize_workspace_api_output(
        &self,
        text: String,
        ext: &str,
        max_chars: usize,
    ) -> Value {
        if max_chars == 0 {
            return workspace_api_success(&text);
        }
        let total_chars = text.chars().count();
        if total_chars <= max_chars {
            return workspace_api_success(&text);
        }
        match self.write_oversized_output(&text, ext).await {
            Ok(path) => {
                let preview: String = text.chars().take(OUTPUT_PREVIEW_CHARS).collect();
                workspace_api_success(&format!(
                    "Output too large: {total_chars} characters (limit: {max_chars}). \
                     The full output was written to:\n{path}\n\n\
                     This file is OUTSIDE the workspace root (a sibling of the repo \
                     checkout), so `ws.file.read` cannot reach it. Inspect it \
                     selectively with terminal commands (grep, head, tail, ranged \
                     reads) or absolute-path file tools instead of reading it \
                     whole.\n\nFirst {OUTPUT_PREVIEW_CHARS} characters:\n{preview}"
                ))
            }
            Err(reason) => {
                tracing::warn!(
                    "workspace_api: oversized output ({total_chars} chars > {max_chars}) \
                     could not be redirected to a file ({reason}); returning truncated head"
                );
                let head: String = text.chars().take(max_chars).collect();
                workspace_api_success(&format!(
                    "Output too large: {total_chars} characters (limit: {max_chars}). \
                     The full output could NOT be written to a file ({reason}), so \
                     everything past the first {max_chars} characters was dropped. \
                     Re-run the call with a narrower query (filter, project fewer \
                     fields, or page the results) to see the rest.\n\n\
                     First {max_chars} characters:\n{head}"
                ))
            }
        }
    }

    /// Write one oversized output to
    /// `<workspace-folder>/tool-outputs/<utc-timestamp>-<short-id>.<ext>`,
    /// resolving `<workspace-folder>` as the parent of the workspace's
    /// checkout (`worktreePath`, else `path`) — today's layout is
    /// `<workspaces-root>/<workspace-name>/<repo-name>`, so the file lands
    /// next to (not inside) the git tree and needs no git exclusion. Writes
    /// through direct tokio fs on purpose: the `ws.file.*` surface is
    /// worktree-rooted and cannot reach this folder. Returns the absolute
    /// file path.
    async fn write_oversized_output(
        &self,
        text: &str,
        ext: &str,
    ) -> std::result::Result<String, String> {
        let ws = self
            .api
            .get_workspace(self.workspace_id.clone())
            .await
            .map_err(|e| format!("get_workspace: {e}"))?;
        let checkout = ws
            .worktree_path
            .as_deref()
            .filter(|p| !p.is_empty())
            .or(ws.path.as_deref().filter(|p| !p.is_empty()))
            .ok_or_else(|| "workspace has no on-disk checkout path".to_string())?;
        let folder = std::path::Path::new(checkout)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .ok_or_else(|| format!("checkout path `{checkout}` has no parent directory"))?;
        let dir = folder.join("tool-outputs");
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| format!("create {}: {e}", dir.display()))?;
        let stamp = now_iso().replace(':', "-");
        let short_id = uuid::Uuid::new_v4().simple().to_string();
        let file = dir.join(format!("{stamp}-{}.{ext}", &short_id[..8]));
        tokio::fs::write(&file, text)
            .await
            .map_err(|e| format!("write {}: {e}", file.display()))?;
        Ok(file.to_string_lossy().into_owned())
    }

    /// §7.1 deterministic attach — registration side. For every well-formed
    /// resource content item (`{ type: "resource", resource: { uri, name,
    /// mimeType, text } }`) in a `workspace_api` result: mint a nonce, stamp
    /// it (as [`ATTACHMENT_ID_KEY`]) into the resource's JSON-object `text`,
    /// and register the canonical payload in the turn-attachment registry
    /// under `AtToolResult`. All resource items of one result register as ONE
    /// batch — together with `pending_batch`, the attachments already
    /// collected at binding call time (monorepo#2637) — so the claim attaches
    /// every one of them together. A resource item whose text already carries
    /// a `pending_batch` nonce was stamped at binding time and is NOT
    /// re-stamped or duplicated. The first nonce is also stamped into every
    /// JSON-object text item so a provider that collapses the content-item
    /// array to the first text item still echoes it — that echo (or, failing
    /// that, the `workspace_api` FIFO fallback) is what links the completed
    /// tool call back to the registered batch. Registration happens strictly
    /// before the result returns to the provider, so the entries always exist
    /// by the time the provider can echo the tool's completion. Pass-through
    /// (no clone mutation, no registration) when the registry or caller agent
    /// is unwired.
    fn register_turn_attachments(
        &self,
        items: &[Value],
        pending_batch: Vec<TurnAttachment>,
    ) -> Vec<Value> {
        let (Some(registry), Some(agent_id)) = (&self.turn_attachments, &self.caller_agent_id)
        else {
            return items.to_vec();
        };
        let known: HashSet<String> = pending_batch.iter().map(|a| a.id.clone()).collect();
        let mut out = items.to_vec();
        let new_entries = stamp_and_collect(&mut out, &known);
        let mut batch = pending_batch;
        batch.extend(new_entries);
        registry.register_all(agent_id, batch);
        out
    }

    /// Register the binding-time attachment batch on the result paths that do
    /// NOT return `__mcpContentItems` — the agent's JS discarded (or threw
    /// past) the envelope, but the proposals it produced still attach to the
    /// tool result via the `workspace_api` FIFO claim (monorepo#2637).
    fn register_pending_attachments(&self, pending_batch: Vec<TurnAttachment>) {
        if let (Some(registry), Some(agent_id)) = (&self.turn_attachments, &self.caller_agent_id) {
            registry.register_all(agent_id, pending_batch);
        }
    }
}

/// Render a `workspace_api` success value as a text body plus the file
/// extension an oversized redirect would use. Object/array results are
/// TOON-encoded when `workspaceApi.toonOutput` is enabled (falling back to
/// pretty JSON if the encoder rejects the value); every other result
/// (strings, numbers, booleans, `null`) keeps the pretty-JSON behavior.
fn render_workspace_api_value(value: &Value, toon_output: bool) -> (String, &'static str) {
    let is_structured = value.is_object() || value.is_array();
    if toon_output && is_structured {
        if let Ok(encoded) = toon_format::encode_default(value) {
            return (encoded, "toon");
        }
    }
    let pretty = serde_json::to_string_pretty(value).unwrap_or_else(|_| "(unserializable)".into());
    (pretty, if is_structured { "json" } else { "txt" })
}

/// Stamp [`ATTACHMENT_ID_KEY`] into a serialized JSON **object** payload,
/// returning the re-serialized text (`pretty` matches the pretty-printed text
/// items the bindings emit; compact otherwise). `None` when the text is not a
/// JSON object — non-object payloads pass through unstamped (their entry is
/// still claimable via the `workspace_api` FIFO fallback).
fn stamp_attachment_id(text: &str, nonce: &str, pretty: bool) -> Option<String> {
    let mut parsed: Value = serde_json::from_str(text).ok()?;
    let obj = parsed.as_object_mut()?;
    obj.insert(
        ATTACHMENT_ID_KEY.to_string(),
        Value::String(nonce.to_string()),
    );
    if pretty {
        serde_json::to_string_pretty(&parsed).ok()
    } else {
        serde_json::to_string(&parsed).ok()
    }
}

/// Read a previously stamped [`ATTACHMENT_ID_KEY`] back out of a serialized
/// JSON-object payload. `None` for non-object payloads or when no nonce is
/// stamped.
fn existing_attachment_id(text: &str) -> Option<String> {
    let parsed: Value = serde_json::from_str(text).ok()?;
    parsed
        .get(ATTACHMENT_ID_KEY)
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Shared stamping/collection core for both registration sites (binding call
/// time and the top-level `__mcpContentItems` path). For every well-formed
/// resource content item in `items`: mint a nonce, stamp it into the
/// resource's JSON-object `text`, and collect the canonical `AtToolResult`
/// payload — SKIPPING items whose text already carries a nonce listed in
/// `known` (they were stamped by an earlier pass; re-stamping would duplicate
/// their registry entry). The first freshly minted nonce is also stamped into
/// every JSON-object text item so a provider that collapses the content-item
/// array to the first text item still echoes it. Returns the collected
/// attachments in item order.
fn stamp_and_collect(items: &mut [Value], known: &HashSet<String>) -> Vec<TurnAttachment> {
    let mut batch: Vec<TurnAttachment> = Vec::new();
    let mut first_nonce: Option<String> = None;
    for item in items.iter_mut() {
        if item.get("type").and_then(Value::as_str) != Some("resource") {
            continue;
        }
        let Some(resource) = item.get_mut("resource").and_then(Value::as_object_mut) else {
            continue;
        };
        let (Some(mime_type), Some(text)) = (
            resource.get("mimeType").and_then(Value::as_str),
            resource.get("text").and_then(Value::as_str),
        ) else {
            continue;
        };
        if existing_attachment_id(text).is_some_and(|id| known.contains(&id)) {
            continue;
        }
        let nonce = new_attachment_id();
        let stamped = stamp_attachment_id(text, &nonce, false).unwrap_or_else(|| text.into());
        batch.push(TurnAttachment {
            id: nonce.clone(),
            policy: AttachmentPolicy::AtToolResult,
            mime_type: mime_type.to_string(),
            uri: resource
                .get("uri")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            name: resource
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            text: stamped.clone(),
        });
        resource.insert("text".to_string(), Value::String(stamped));
        first_nonce.get_or_insert(nonce);
    }
    if let Some(nonce) = first_nonce {
        for item in items.iter_mut() {
            if item.get("type").and_then(Value::as_str) != Some("text") {
                continue;
            }
            let Some(obj) = item.as_object_mut() else {
                continue;
            };
            if let Some(stamped) = obj
                .get("text")
                .and_then(Value::as_str)
                .and_then(|t| stamp_attachment_id(t, &nonce, true))
            {
                obj.insert("text".to_string(), Value::String(stamped));
            }
        }
    }
    batch
}

/// `HostFn` factory plus the sub-agent flag: `app.question.*`
/// frames from a sub-agent caller are denied with the explicit
/// top-level-only redirect error before the feature-gate check, so a raw
/// `host({...})` call cannot bypass the description/prelude pruning and the
/// error never misleads with "disabled in settings".
///
/// `pub` (re-exported as `intent_acp::make_workspace_host_for_bridge`) so
/// the background hook scheduler in `intent-services` applies the same
/// sub-agent gate to hooks owned by background/delegated sessions.
pub fn make_workspace_host_for_bridge(
    api: Arc<dyn WorkspaceApi>,
    workspace_id: WorkspaceId,
    caller_agent_id: Option<AgentId>,
    turn_attachments: Option<Arc<TurnAttachmentRegistry>>,
    agent_features: AgentFeaturesSettings,
    is_sub_agent: bool,
) -> HostFn {
    make_workspace_host_with_pending(
        api,
        workspace_id,
        caller_agent_id,
        turn_attachments,
        agent_features,
        is_sub_agent,
        None,
    )
}

/// [`make_workspace_host_for_bridge`] plus the binding-time attachment
/// collector (monorepo#2637): when `pending` is wired, every host frame whose
/// result carries `__mcpContentItems` gets its resource items nonce-stamped
/// and collected immediately, so `dispatch_workspace_api` registers them at
/// the tool result regardless of what the agent's JS returns. Private —
/// only the `workspace_api` dispatch wires a collector.
fn make_workspace_host_with_pending(
    api: Arc<dyn WorkspaceApi>,
    workspace_id: WorkspaceId,
    caller_agent_id: Option<AgentId>,
    turn_attachments: Option<Arc<TurnAttachmentRegistry>>,
    agent_features: AgentFeaturesSettings,
    is_sub_agent: bool,
    pending: Option<PendingAttachments>,
) -> HostFn {
    let features = Arc::new(agent_features);
    Arc::new(move |arg| {
        let api = api.clone();
        let workspace_id = workspace_id.clone();
        let caller = caller_agent_id.clone();
        let registry = turn_attachments.clone();
        let features = features.clone();
        let pending = pending.clone();
        // Take the frame's sequence number NOW — synchronously, while the JS
        // engine is invoking `host()` — so parallel calls
        // (`Promise.all([...])`) keep call order even when an earlier call's
        // dispatch awaits I/O and resolves later.
        let seq = pending.as_ref().map(|p| {
            let mut state = p.lock().unwrap();
            let seq = state.next_seq;
            state.next_seq += 1;
            seq
        });
        Box::pin(async move {
            let result = workspace_host_dispatch(
                api,
                workspace_id,
                caller,
                registry,
                &features,
                is_sub_agent,
                arg,
            )
            .await;
            match (result, &pending, seq) {
                (Ok(mut value), Some(pending), Some(seq)) => {
                    collect_binding_attachments(&mut value, pending, seq);
                    Ok(value)
                }
                (result, ..) => result,
            }
        }) as BoxFuture<'static, std::result::Result<Value, String>>
    })
}

/// Binding-time collection (monorepo#2637): when a host frame's result
/// carries `__mcpContentItems`, stamp a fresh nonce into each resource item
/// (mutating the value the agent's JS will see, so a returned envelope
/// carries the SAME nonce) and stash the canonical payloads in the
/// per-invocation collector under the frame's call-order sequence number.
/// `dispatch_workspace_api` drains the collector (sorted by that number)
/// into the registry when the tool call completes.
fn collect_binding_attachments(value: &mut Value, pending: &PendingAttachments, seq: u64) {
    let Some(items) = value
        .get_mut("__mcpContentItems")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let batch = stamp_and_collect(items, &HashSet::new());
    if !batch.is_empty() {
        pending.lock().unwrap().entries.push((seq, batch));
    }
}

/// The dispatch-layer denial for a sub-agent's `app.question.*` frame —
/// names the two methods a sub-agent should use instead.
pub(super) const SUB_AGENT_QUESTION_DENIED: &str =
    "ws.app.question.ask is only available to top-level agents — raise \
     ws.agent.requestDiscussion when you need user/coordinator input, or report \
     progress with ws.agent.reportToParent";

/// Route one `host({method, args})` frame to a `WorkspaceApi` method via
/// [`super::bindings::try_dispatch`], which owns the per-namespace method →
/// trait mapping. Sub-agent `app.question.*` frames and methods gated by a
/// disabled `[agentFeatures]` toggle are denied before dispatch.
async fn workspace_host_dispatch(
    api: Arc<dyn WorkspaceApi>,
    workspace_id: WorkspaceId,
    caller_agent_id: Option<AgentId>,
    turn_attachments: Option<Arc<TurnAttachmentRegistry>>,
    agent_features: &AgentFeaturesSettings,
    is_sub_agent: bool,
    arg: Value,
) -> std::result::Result<Value, String> {
    let method = arg
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| "host: `method` is required".to_string())?;
    // Sub-agent question gate FIRST: the redirect error must win over the
    // feature-gate denial (a sub-agent bridge's effective features force
    // `structuredQuestions` off, which would otherwise claim the frame with
    // a misleading "disabled in settings").
    if is_sub_agent && method.starts_with("app.question.") {
        return Err(format!("host: {SUB_AGENT_QUESTION_DENIED}"));
    }
    if let Some(feature) = super::tools::denied_feature(agent_features, method) {
        return Err(format!(
            "host: method `{method}` is disabled in settings ({feature} = false)"
        ));
    }
    let args = arg.get("args").cloned().unwrap_or(Value::Null);
    if let Some(v) = super::bindings::try_dispatch(
        &api,
        &workspace_id,
        &caller_agent_id,
        turn_attachments.as_ref(),
        agent_features,
        is_sub_agent,
        method,
        &args,
    )
    .await?
    {
        return Ok(v);
    }
    Err(format!("host: unknown method `{method}`"))
}

/// Success MCP tool result for `workspace_api`: a single text content block
/// with `isError: false`.
fn workspace_api_success(text: &str) -> Value {
    json!({
        "content": [{ "type": "text", "text": text }],
        "isError": false,
    })
}

/// Error MCP tool result for `workspace_api`: a single text content block
/// with `isError: true`. JS-side failures are surfaced as tool results
/// (not JSON-RPC protocol errors) to mirror the reference TS tool.
fn workspace_api_error(text: &str) -> Value {
    json!({
        "content": [{ "type": "text", "text": text }],
        "isError": true,
    })
}

/// Render a [`JsError`] into the reference tool's error-text style. Syntax
/// errors and `Cannot read properties of undefined` TypeErrors get a
/// clearer human-facing rewrite; everything else falls through as `Error: …`.
fn format_js_error(err: &JsError) -> String {
    match err {
        JsError::Timeout { ms } => format!("Error: javascript execution timed out after {ms}ms"),
        JsError::Engine(msg) => format!("Error: {msg}"),
        JsError::Runtime(msg) => {
            if looks_like_syntax_error(msg) {
                format!(
                    "SyntaxError in your code: {msg}. Check for unclosed brackets, braces, quotes, or template literals."
                )
            } else if msg.contains("TypeError")
                && msg.contains("Cannot read properties of undefined")
            {
                // Reference message: name the missing property to help the
                // agent notice the wrong namespace on `ws.*`.
                let prop = extract_missing_prop(msg);
                match prop {
                    Some(p) => format!(
                        "TypeError: Attempted to call '{p}' on an undefined object. Check that the namespace exists on the `ws` object (e.g. ws.workspace)."
                    ),
                    None => format!("TypeError: {msg}"),
                }
            } else {
                format!("Error: {msg}")
            }
        }
    }
}

/// QuickJS reports syntax errors as bare `Error: ...` with an indicative
/// phrase in the body (e.g. `unexpected token`, `expected identifier`),
/// unlike V8 which stamps `SyntaxError:` on the message. Match both so the
/// friendlier prefix still triggers on either engine.
fn looks_like_syntax_error(msg: &str) -> bool {
    msg.contains("SyntaxError")
        || msg.contains("unexpected token")
        || msg.contains("expected identifier")
        || msg.contains("unexpected end of input")
        || msg.contains("Unexpected end of input")
        || msg.contains("Invalid or unexpected token")
}

/// Pull the property name out of a `Cannot read properties of undefined
/// (reading 'foo')` TypeError message, matching the reference regex.
fn extract_missing_prop(msg: &str) -> Option<String> {
    let key = "(reading '";
    let start = msg.find(key)? + key.len();
    let rest = &msg[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

#[cfg(test)]
mod pending_attachment_order_tests {
    use super::*;

    fn attachment(name: &str) -> TurnAttachment {
        TurnAttachment {
            id: new_attachment_id(),
            policy: AttachmentPolicy::AtToolResult,
            mime_type: "application/vnd.intent.proposal+json".into(),
            uri: format!("intent-proposal://test/{name}"),
            name: name.to_string(),
            text: "{}".into(),
        }
    }

    /// Parallel `host()` calls can resolve out of order (`Promise.all` where
    /// the first call awaits I/O the second does not); the drain must yield
    /// call order — the frame sequence number — not completion order.
    #[test]
    fn drain_yields_call_order_not_completion_order() {
        let mut state = PendingAttachmentState::default();
        let (seq_a, seq_b) = (state.next_seq, state.next_seq + 1);
        state.next_seq += 2;
        // Completion order reversed: the second call's batch lands first.
        state.entries.push((seq_b, vec![attachment("Second")]));
        state.entries.push((seq_a, vec![attachment("First")]));
        let drained = state.drain_in_call_order();
        assert_eq!(
            drained.iter().map(|a| a.name.as_str()).collect::<Vec<_>>(),
            vec!["First", "Second"]
        );
        assert!(state.entries.is_empty(), "drain empties the collector");
    }
}

#[cfg(test)]
mod timeout_override_tests {
    use super::*;

    #[test]
    fn unset_keeps_default() {
        assert_eq!(workspace_api_timeout_from(None), WORKSPACE_API_TIMEOUT);
    }

    #[test]
    fn positive_millis_override() {
        assert_eq!(
            workspace_api_timeout_from(Some("120000")),
            Duration::from_millis(120_000)
        );
        assert_eq!(
            workspace_api_timeout_from(Some(" 500 ")),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn invalid_values_keep_default() {
        for raw in ["0", "-5", "abc", "", "1.5"] {
            assert_eq!(
                workspace_api_timeout_from(Some(raw)),
                WORKSPACE_API_TIMEOUT,
                "raw={raw:?}"
            );
        }
    }
}
