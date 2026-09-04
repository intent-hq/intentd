//! Background hook scheduler (spec: "JS kernel" watchers). A hook is a small
//! agent-owned JavaScript script the daemon runs on one of three schedule
//! kinds — a fixed `delayMs` cadence, a recurring `cron` expression
//! (standard 5-field, evaluated in UTC), or a one-shot `runAt` timestamp —
//! until it signals a dispatch, fails, is cancelled,
//! or its TTL expires (`expiresAt` = creation + clamped `ttlMs`, capped at
//! 24 hours for `delayMs` hooks and 7 days for `cron` hooks; `runAt` implies
//! its own expiry — the fire time + 1 hour grace — and rejects an explicit
//! `ttlMs`. On expiry the owner is woken so it can reschedule). Each
//! active hook owns one tokio task; schedules persist to the `hook` table
//! and rehydrate at boot ([`Services::rehydrate_hooks`]).
//!
//! Scripts evaluate in `QuickJS` via `intent_js::eval` with the exact same
//! `ws.*` prelude + host dispatch the `workspace_api` MCP tool installs —
//! gated by the same `[agentFeatures]` toggles (e.g. no `ws.host.exec` when
//! `agentFeatures.hostExec` is off; with all defaults on the environment is
//! byte-identical to the ungated one) — a 60 s wall-clock budget, and the
//! hook's workspace/agent pinned as the caller. The script's return value is
//! the
//! contract: `{ dispatch: true, message }` wakes the owning agent (queued
//! behind an in-flight turn via the automatic-delivery `agent.sendMessage`
//! path) and terminates the hook — unless the hook is `perpetual`, in which
//! case it counts the fire (`dispatch_count`) and returns to `scheduled`,
//! running on its cadence until TTL expiry, cancel, or eviction;
//! `{ dispatch: false }` / `undefined` sleeps
//! `delayMs` and re-runs; a throw or timeout evicts the hook, persists
//! `last_error`, emits `hook:evicted`, and wakes the owner with the reason.
//! Scripts may call `console.log/info/warn/error`; the last run's captured
//! output persists to `last_logs` (overwritten each run, capped) and is
//! appended to dispatch/evict wake messages, which end with a terminal-state
//! note (the hook is retired and will not run again, with a reschedule
//! pointer). A run may also return a `state`
//! field: its JSON serialization persists to `last_state` (size-capped) and
//! is injected into the next run as the `hookState` global.
//!
//! `ws.host.exec` calls inside a run are additionally observed by an
//! in-envelope wrapper (monorepo#3231): a call that returns a nonzero
//! `exitCode` or `timedOut: true` — without the script throwing — is
//! recorded, and the run's failure summary persists to `last_error` on the
//! (still-active) hook so a silently broken check is observable via
//! `ws.hook.list`; a later all-healthy run clears it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::events::{
    HOOK_CANCELLED, HOOK_DISPATCHED, HOOK_EVICTED, HOOK_EXPIRED, HOOK_RUN_COMPLETED,
    HOOK_RUN_STARTED, HOOK_SCHEDULED,
};
use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::{
    now_iso, AgentId, AgentStatus, Error, Hook, HookId, HookState, Result, WorkspaceApi,
    WorkspaceId,
};
use intent_store::NewEvent;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::mpsc;

use crate::{publish_event, system_actor, Services};

/// Minimum inter-run delay (spec decision 5): `delayMs` below this floor is
/// rejected at schedule time.
pub(crate) const MIN_HOOK_DELAY_MS: i64 = 10_000;

/// Default and maximum hook TTL: every hook expires at most 24 hours after
/// creation. A schedulable `ttlMs` is clamped into
/// `[MIN_HOOK_DELAY_MS, MAX_HOOK_TTL_MS]` (the floor is shared with
/// `delayMs`); expiry counts from creation, not the last run.
pub(crate) const MAX_HOOK_TTL_MS: i64 = 86_400_000;
pub(crate) const DEFAULT_HOOK_TTL_MS: i64 = MAX_HOOK_TTL_MS;

/// Cron hooks lift the 24-hour cap (spec decision): default and maximum TTL
/// are both 7 days; the floor is shared with `delayMs`.
pub(crate) const MAX_CRON_HOOK_TTL_MS: i64 = 7 * 86_400_000;

/// A `runAt` hook's TTL is implied — the fire time plus this grace window —
/// and an explicit `ttlMs` is rejected at schedule time.
pub(crate) const RUN_AT_GRACE_MS: i64 = 3_600_000;

/// Maximum hook name length (spec: name > 50 chars fails validation).
pub(crate) const MAX_HOOK_NAME_LEN: usize = 50;

/// Wall-clock budget for one hook script run (spec: any run exceeding 60 s is
/// killed and the hook evicted). Tests compress it via the `#[cfg(test)]`-only
/// [`Services::with_hook_eval_timeout`].
pub(crate) const HOOK_EVAL_TIMEOUT: Duration = Duration::from_secs(60);

/// Per-run console-capture caps: the in-envelope `console.*` shim buffers at
/// most this many lines / bytes, dropping from the head (with a marker line)
/// so a hot loop cannot bloat the `last_logs` row.
const HOOK_LOG_MAX_LINES: usize = 100;
const HOOK_LOG_MAX_BYTES: usize = 8 * 1024;

/// Cap (in chars) on the `[hook logs]` section appended to dispatch/evict
/// wake messages; longer captures are head-truncated to the most recent tail.
/// Owned by the harness (H6) alongside the section wording; re-exposed here
/// for the byte-exact truncation tests.
#[cfg(test)]
const HOOK_WAKE_LOGS_CAP: usize = crate::harness::v1::HOOK_WAKE_LOGS_CAP;

/// Cap (in bytes) on a run's JSON-serialized carry-over state. An oversized
/// `state` is dropped (the previous state is kept) with a warning line
/// appended to that run's logs.
const HOOK_STATE_MAX_BYTES: usize = 16 * 1024;

/// Caps on the per-run `ws.host.exec` failure capture (monorepo#3231): at
/// most this many failed-exec lines are recorded per run, each truncated to
/// this many chars, so a looping script cannot bloat the summary that lands
/// in `last_error`.
const HOOK_EXEC_FAILURE_MAX: usize = 5;
const HOOK_EXEC_FAILURE_LINE_CAP: usize = 300;

/// Control frames the service ops send to a hook's scheduler task.
enum HookControl {
    /// Run the script immediately and reset the inter-run timer.
    RunNow,
}

/// Live handle for one active hook's scheduler task.
pub(crate) struct HookHandle {
    control: mpsc::Sender<HookControl>,
    abort: tokio::task::AbortHandle,
}

/// Registry of active hook tasks, shared across [`Services`] clones so the
/// RPC/MCP front doors and the tasks themselves observe the same set.
pub(crate) type HookTasks = Arc<Mutex<HashMap<HookId, HookHandle>>>;

/// Outcome of one script run. Every variant carries the run's captured
/// console output (`None` when the script logged nothing).
enum RunOutcome {
    /// `{ dispatch: false }` / `undefined` — sleep and re-run.
    Continue {
        logs: Option<String>,
        state: StateUpdate,
        exec_error: Option<String>,
    },
    /// `{ dispatch: true, message }` — wake the owner; terminates the hook
    /// unless it is perpetual, which stays scheduled.
    Dispatch {
        message: String,
        logs: Option<String>,
        state: StateUpdate,
        exec_error: Option<String>,
    },
    /// Throw or timeout — evict, persist the error, wake the owner.
    Failed { error: String, logs: RunLogs },
}

/// How a completed run updates the hook's persisted carry-over state
/// (`last_state`).
enum StateUpdate {
    /// No `state` field returned (or it was oversized) — keep the previous
    /// state.
    Keep,
    /// `state: null` — clear the persisted state.
    Clear,
    /// Replace the persisted state with this JSON serialization.
    Set(String),
}

impl StateUpdate {
    /// Fold this update into the in-memory hook.
    fn apply(self, hook: &mut Hook) {
        match self {
            StateUpdate::Keep => {}
            StateUpdate::Clear => hook.last_state = None,
            StateUpdate::Set(s) => hook.last_state = Some(s),
        }
    }
}

/// Console capture attached to a failed run. A script throw is caught inside
/// the eval envelope, so its logs survive; a timeout (or engine failure)
/// kills the whole eval before the capture can be returned, so that run's
/// logs are lost — the previously persisted `last_logs` is left untouched.
enum RunLogs {
    Captured(Option<String>),
    Lost,
}

/// ISO-8601 timestamp `delay_ms` in the future — the persisted/emitted
/// `nextRunAt` clients animate countdown bars from.
fn next_run_at_iso(delay_ms: i64) -> String {
    (OffsetDateTime::now_utc() + time::Duration::milliseconds(delay_ms))
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Time remaining until `deadline` as a scheduler sleep — zero when the
/// deadline already passed, so the run starts promptly.
fn duration_until(deadline: OffsetDateTime) -> Duration {
    let remaining = deadline - OffsetDateTime::now_utc();
    Duration::from_millis(u64::try_from(remaining.whole_milliseconds().max(0)).unwrap_or(u64::MAX))
}

/// Resumed countdown for a rehydrated hook, per schedule kind. Returns the
/// timestamp to persist and the time remaining until it — zero when
/// overdue, so the run starts promptly, still gated by the scheduler task's
/// pre-run expiry check.
///
/// - `runAt`: the one-shot deadline is absolute — resume to it verbatim.
///   This includes rows interrupted mid-run (`Running`): the timer never
///   completed a run, and unlike the fixed cadence there is no later tick
///   to defer to, so the interrupted fire re-runs (still bounded by the
///   fire + grace `expiresAt`).
/// - `cron`: a `Scheduled` row resumes to its persisted absolute deadline
///   verbatim (the expression's next occurrence computed before the
///   restart); an absent/unparseable deadline — and a row interrupted
///   mid-run, whose persisted deadline belongs to the run that already
///   started — recomputes from the expression.
/// - `delayMs`: the EARLIER of the persisted `next_run_at` and a fresh
///   `now + delay_ms` countdown, so a restart never pushes a run further
///   out than it was already scheduled (intent-hq/monorepo#2856). An absent
///   or unparseable persisted deadline falls back to the fresh countdown. A
///   row persisted as `Running` (daemon died mid-run) also gets the fresh
///   countdown: its `next_run_at` is the deadline of the run that already
///   STARTED ([`Services::execute_hook_run`] leaves it in place), so
///   honoring it would immediately re-execute a potentially non-idempotent
///   interrupted run.
fn resumed_next_run(hook: &Hook) -> (String, Duration) {
    if let Some(at) = &hook.run_at {
        let remaining = OffsetDateTime::parse(at, &Rfc3339).map_or(Duration::ZERO, duration_until);
        return (at.clone(), remaining);
    }
    if let Some(expr) = &hook.cron {
        // The recompute cannot fail in practice (the expression was
        // validated at schedule time); the fallback resumes promptly and
        // the post-run reschedule surfaces the error.
        let recomputed = || {
            parse_cron(expr)
                .and_then(|c| cron_next_fire(&c))
                .unwrap_or_else(|_| (next_run_at_iso(0), Duration::ZERO))
        };
        if hook.state == HookState::Running {
            return recomputed();
        }
        return match hook
            .next_run_at
            .as_deref()
            .and_then(|raw| Some((raw, OffsetDateTime::parse(raw, &Rfc3339).ok()?)))
        {
            Some((raw, persisted)) => (raw.to_string(), duration_until(persisted)),
            None => recomputed(),
        };
    }
    let now = OffsetDateTime::now_utc();
    let fresh = now + time::Duration::milliseconds(hook.delay_ms.max(0));
    if hook.state == HookState::Running {
        return (
            next_run_at_iso(hook.delay_ms),
            Duration::from_millis(hook.delay_ms.max(0).cast_unsigned()),
        );
    }
    match hook
        .next_run_at
        .as_deref()
        .and_then(|raw| Some((raw, OffsetDateTime::parse(raw, &Rfc3339).ok()?)))
    {
        Some((raw, persisted)) if persisted < fresh => (raw.to_string(), duration_until(persisted)),
        _ => (
            next_run_at_iso(hook.delay_ms),
            Duration::from_millis(hook.delay_ms.max(0).cast_unsigned()),
        ),
    }
}

/// Clamp a schedulable `ttlMs` into `[MIN_HOOK_DELAY_MS, MAX_HOOK_TTL_MS]`;
/// `None` (omitted) takes the default (= the 24-hour cap). `delayMs`-kind
/// hooks only — cron hooks use [`clamp_cron_ttl_ms`].
fn clamp_ttl_ms(ttl_ms: Option<i64>) -> i64 {
    ttl_ms
        .unwrap_or(DEFAULT_HOOK_TTL_MS)
        .clamp(MIN_HOOK_DELAY_MS, MAX_HOOK_TTL_MS)
}

/// Cron-kind TTL clamp: `[MIN_HOOK_DELAY_MS, MAX_CRON_HOOK_TTL_MS]`, with
/// `None` (omitted) taking the 7-day default (= the cron cap).
fn clamp_cron_ttl_ms(ttl_ms: Option<i64>) -> i64 {
    ttl_ms
        .unwrap_or(MAX_CRON_HOOK_TTL_MS)
        .clamp(MIN_HOOK_DELAY_MS, MAX_CRON_HOOK_TTL_MS)
}

/// The exactly-one-of schedule kind `hook.schedule` accepts: a fixed
/// `delayMs` cadence, a recurring `cron` expression (standard 5-field,
/// evaluated in UTC), or a one-shot future `runAt` timestamp (normalized to
/// UTC RFC3339 at validation).
#[derive(Debug, Clone, PartialEq)]
enum ScheduleKind {
    Delay(i64),
    Cron(String),
    RunAt(String),
}

impl ScheduleKind {
    /// The `delay_ms` column value: the cadence for the fixed kind, 0 for
    /// cron/runAt hooks.
    fn delay_ms(&self) -> i64 {
        match self {
            Self::Delay(ms) => *ms,
            _ => 0,
        }
    }

    fn cron(&self) -> Option<String> {
        match self {
            Self::Cron(expr) => Some(expr.clone()),
            _ => None,
        }
    }

    fn run_at(&self) -> Option<String> {
        match self {
            Self::RunAt(at) => Some(at.clone()),
            _ => None,
        }
    }
}

/// Parse a cron expression under the accepted grammar: standard 5-field
/// (minute granularity — a seconds field is rejected), evaluated in UTC.
fn parse_cron(expr: &str) -> Result<croner::Cron> {
    croner::parser::CronParser::builder()
        .seconds(croner::parser::Seconds::Disallowed)
        .build()
        .parse(expr)
        .map_err(|e| {
            Error::InvalidParams(format!(
                "hook.schedule: invalid `cron` expression (standard 5-field, no seconds): {e}"
            ))
        })
}

/// Next fire of `cron` strictly after now (UTC): the RFC3339 timestamp and
/// the time until it. Errors when the expression has no computable next
/// occurrence.
fn cron_next_fire(cron: &croner::Cron) -> Result<(String, Duration)> {
    let now = chrono::Utc::now();
    let next = cron.find_next_occurrence(&now, false).map_err(|e| {
        Error::InvalidParams(format!(
            "hook.schedule: `cron` expression has no computable next fire: {e}"
        ))
    })?;
    let until = (next - now).num_milliseconds().max(0);
    Ok((
        next.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        Duration::from_millis(until.cast_unsigned()),
    ))
}

/// Extract and validate the exactly-one-of `delayMs` | `cron` | `runAt`
/// schedule kind from `hook.schedule` params.
fn parse_schedule_kind(params: &Value) -> Result<ScheduleKind> {
    let present: Vec<&str> = ["delayMs", "cron", "runAt"]
        .into_iter()
        .filter(|k| params.get(k).is_some())
        .collect();
    match present.as_slice() {
        [] => {
            return Err(Error::InvalidParams(
                "hook.schedule: exactly one of `delayMs`, `cron`, or `runAt` is required".into(),
            ))
        }
        [_] => {}
        keys => {
            let keys = keys
                .iter()
                .map(|k| format!("`{k}`"))
                .collect::<Vec<_>>()
                .join(" and ");
            return Err(Error::InvalidParams(format!(
                "hook.schedule: {keys} are mutually exclusive — pass exactly one schedule kind"
            )));
        }
    }
    if let Some(v) = params.get("delayMs") {
        let delay_ms = v.as_i64().ok_or_else(|| {
            Error::InvalidParams("hook.schedule: `delayMs` must be an integer".into())
        })?;
        if delay_ms < MIN_HOOK_DELAY_MS {
            return Err(Error::InvalidParams(format!(
                "hook.schedule: `delayMs` must be at least {MIN_HOOK_DELAY_MS}"
            )));
        }
        return Ok(ScheduleKind::Delay(delay_ms));
    }
    if let Some(v) = params.get("cron") {
        let expr = v
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::InvalidParams("hook.schedule: `cron` must be a non-empty string".into())
            })?;
        let cron = parse_cron(expr)?;
        // Must have a computable next fire at schedule time.
        cron_next_fire(&cron)?;
        return Ok(ScheduleKind::Cron(expr.to_string()));
    }
    let raw = params.get("runAt").and_then(Value::as_str).ok_or_else(|| {
        Error::InvalidParams("hook.schedule: `runAt` must be an RFC3339 timestamp string".into())
    })?;
    let at = OffsetDateTime::parse(raw, &Rfc3339).map_err(|e| {
        Error::InvalidParams(format!(
            "hook.schedule: `runAt` is not a valid RFC3339 timestamp: {e}"
        ))
    })?;
    if at <= OffsetDateTime::now_utc() {
        return Err(Error::InvalidParams(
            "hook.schedule: `runAt` must be in the future".into(),
        ));
    }
    // The expiry is fire + grace; a timestamp at the date-range boundary
    // (e.g. year 9999) would overflow that addition downstream.
    if at
        .checked_add(time::Duration::milliseconds(RUN_AT_GRACE_MS))
        .is_none()
    {
        return Err(Error::InvalidParams(
            "hook.schedule: `runAt` is too far in the future".into(),
        ));
    }
    let normalized = at
        .to_offset(time::UtcOffset::UTC)
        .format(&Rfc3339)
        .map_err(|e| Error::Internal(format!("hook.schedule: format runAt: {e}")))?;
    Ok(ScheduleKind::RunAt(normalized))
}

/// A freshly scheduled kind's next fire: the `nextRunAt` to persist and the
/// scheduler task's first sleep. The cron re-parse cannot fail in practice —
/// the expression was validated by [`parse_schedule_kind`].
fn kind_next_fire(kind: &ScheduleKind) -> Result<(String, Duration)> {
    match kind {
        ScheduleKind::Delay(ms) => Ok((
            next_run_at_iso(*ms),
            Duration::from_millis((*ms).max(0).cast_unsigned()),
        )),
        ScheduleKind::Cron(expr) => cron_next_fire(&parse_cron(expr)?),
        ScheduleKind::RunAt(at) => {
            let deadline = OffsetDateTime::parse(at, &Rfc3339)
                .map_err(|e| Error::Internal(format!("parse validated runAt: {e}")))?;
            Ok((at.clone(), duration_until(deadline)))
        }
    }
}

/// Post-run next fire for a rescheduling hook: the `nextRunAt` to persist
/// and the inter-run sleep. `delayMs` hooks tick their fixed cadence; cron
/// hooks recompute the expression's next occurrence (strictly after now, so
/// a run that overshoots a tick skips it rather than firing twice). Never
/// called for `runAt` hooks — their fire is terminal. Errors only when a
/// cron expression has no computable next occurrence (the schedule is
/// exhausted).
fn hook_next_fire(hook: &Hook) -> Result<(String, Duration)> {
    match &hook.cron {
        Some(expr) => cron_next_fire(&parse_cron(expr)?),
        None => Ok((
            next_run_at_iso(hook.delay_ms),
            Duration::from_millis(hook.delay_ms.max(0).cast_unsigned()),
        )),
    }
}

/// Time remaining until `expires_at`, or `None` when the hook has no
/// deadline (pre-TTL legacy rows). An unparseable or already-passed deadline
/// returns `Some(Duration::ZERO)` — expire immediately. `skew_ms` shifts the
/// "now" the deadline is measured against; production callers always pass 0
/// (via [`Services::hook_clock_skew_ms`]) — tests inject a skew so expiry
/// can be forced deterministically instead of racing wall clock.
fn time_to_expiry(expires_at: Option<&str>, skew_ms: i64) -> Option<Duration> {
    let raw = expires_at?;
    let Ok(deadline) = OffsetDateTime::parse(raw, &Rfc3339) else {
        return Some(Duration::ZERO);
    };
    let remaining = deadline - OffsetDateTime::now_utc() - time::Duration::milliseconds(skew_ms);
    Some(if remaining.is_positive() {
        Duration::from_millis(
            u64::try_from(remaining.whole_milliseconds().max(0)).unwrap_or(u64::MAX),
        )
    } else {
        Duration::ZERO
    })
}

/// Whether `expires_at` has passed (never true for deadline-free legacy
/// rows). Same `skew_ms` contract as [`time_to_expiry`].
fn is_expired(expires_at: Option<&str>, skew_ms: i64) -> bool {
    time_to_expiry(expires_at, skew_ms) == Some(Duration::ZERO)
}

/// Evaluate one hook script in `QuickJS` with the `ws.*` environment gated by
/// the same `[agentFeatures]` toggles as the `workspace_api` tool (prelude
/// pruning + dispatch deny; all-defaults is byte-identical to the ungated
/// environment) and interpret its return value against the script contract.
/// `is_sub_agent` mirrors the owning session's bridge derivation
/// (`parent_agent_id.is_some() || is_background`): a sub-agent-owned hook
/// gets the same `ws.app.question.*` pruning + top-level-only dispatch
/// denial its `workspace_api` bridge would apply.
/// Never panics; every failure mode folds into [`RunOutcome::Failed`].
async fn run_hook_script(
    api: Arc<dyn WorkspaceApi>,
    hook: &Hook,
    timeout: Duration,
    agent_features: &AgentFeaturesSettings,
    is_sub_agent: bool,
) -> RunOutcome {
    let host = intent_acp::make_workspace_host_for_bridge(
        api,
        hook.workspace_id.clone(),
        Some(hook.agent_id.clone()),
        None,
        agent_features.clone(),
        is_sub_agent,
    );
    // Same `{__k, __v}` envelope as the `workspace_api` dispatch so an
    // `undefined` return (no dispatch) survives the JSON bridge, extended
    // with a `console.*` shim whose capped line buffer rides back as
    // `__logs`. A user-code throw is caught in-envelope (`__k: 'e'`) so its
    // logs survive; only a timeout/engine failure loses them.
    let prelude = intent_acp::bindings_prelude_for_bridge(agent_features, is_sub_agent);
    let code = &hook.code;
    let max_lines = HOOK_LOG_MAX_LINES;
    let max_bytes = HOOK_LOG_MAX_BYTES;
    let exec_failure_max = HOOK_EXEC_FAILURE_MAX;
    let exec_failure_line_cap = HOOK_EXEC_FAILURE_LINE_CAP;
    // The previous run's carry-over state, injected as the `hookState`
    // global. Embedded as a JSON.parse of a string literal so arbitrary
    // persisted JSON can never break the envelope; a corrupt row (should
    // never happen) falls back to null in Rust rather than throwing in JS.
    let hook_state_literal = hook
        .last_state
        .as_deref()
        .filter(|s| serde_json::from_str::<Value>(s).is_ok())
        .map_or_else(
            || "null".to_string(),
            |s| format!("JSON.parse({})", Value::String(s.to_string())),
        );
    let full_code = format!(
        "{prelude}\n\
         const __hook_logs = [];\n\
         let __hook_logs_bytes = 0;\n\
         let __hook_logs_dropped = false;\n\
         const __hook_byte_len = (s) => {{\n\
           let n = 0;\n\
           for (let i = 0; i < s.length; i++) {{\n\
             const c = s.codePointAt(i);\n\
             if (c > 0xffff) i++;\n\
             n += c <= 0x7f ? 1 : c <= 0x7ff ? 2 : c <= 0xffff ? 3 : 4;\n\
           }}\n\
           return n;\n\
         }};\n\
         const __hook_log = (...args) => {{\n\
           const line = args.map((a) => {{\n\
             if (typeof a === 'string') return a;\n\
             try {{ const s = JSON.stringify(a); return s === undefined ? String(a) : s; }}\n\
             catch (_e) {{ return String(a); }}\n\
           }}).join(' ');\n\
           __hook_logs.push(line);\n\
           __hook_logs_bytes += __hook_byte_len(line) + 1;\n\
           while (__hook_logs.length > {max_lines} || __hook_logs_bytes > {max_bytes}) {{\n\
             const dropped = __hook_logs.shift();\n\
             if (dropped === undefined) break;\n\
             __hook_logs_bytes -= __hook_byte_len(dropped) + 1;\n\
             __hook_logs_dropped = true;\n\
           }}\n\
         }};\n\
         const console = {{ log: __hook_log, info: __hook_log, warn: __hook_log, error: __hook_log }};\n\
         const __hook_exec_failures = [];\n\
         let __hook_exec_failed_total = 0;\n\
         if (globalThis.ws && ws.host && typeof ws.host.exec === 'function') {{\n\
           const __hook_exec_inner = ws.host.exec;\n\
           ws.host.exec = async (opts) => {{\n\
             const r = await __hook_exec_inner(opts);\n\
             try {{\n\
               if (r && (r.timedOut === true || (typeof r.exitCode === 'number' && r.exitCode !== 0))) {{\n\
                 __hook_exec_failed_total += 1;\n\
                 const argc = ((opts && opts.args) || []).length;\n\
                 const cmd = ((opts && opts.command) || '?') + (argc ? ' (' + argc + ' args)' : '');\n\
                 const why = r.timedOut === true ? 'timed out' : 'exit code ' + r.exitCode;\n\
                 const stderr = (typeof r.stderr === 'string' ? r.stderr : '').trim();\n\
                 let line = cmd + ' -> ' + why + (stderr ? ': ' + stderr : '');\n\
                 if (line.length > {exec_failure_line_cap}) line = line.slice(0, {exec_failure_line_cap}) + '…';\n\
                 if (__hook_exec_failures.length < {exec_failure_max}) __hook_exec_failures.push(line);\n\
               }}\n\
             }} catch (_e) {{}}\n\
             return r;\n\
           }};\n\
         }}\n\
         const hookState = {hook_state_literal};\n\
         let __hook_user;\n\
         let __hook_threw = false;\n\
         let __hook_err = '';\n\
         try {{\n\
           __hook_user = await (async () => {{ {code}\n}})();\n\
         }} catch (e) {{\n\
           __hook_threw = true;\n\
           __hook_err = String(e);\n\
         }}\n\
         const __logs = __hook_logs.length === 0 ? '' :\n\
           (__hook_logs_dropped ? '[earlier log lines truncated]\\n' : '') + __hook_logs.join('\\n');\n\
         if (__hook_threw) return {{ __k: 'e', __v: __hook_err, __logs, __execFailures: __hook_exec_failures, __execFailedTotal: __hook_exec_failed_total }};\n\
         return {{ __k: __hook_user === undefined ? 'u' : 'v', __v: __hook_user, __logs, __execFailures: __hook_exec_failures, __execFailedTotal: __hook_exec_failed_total }};"
    );
    let opts = intent_js::EvalOptions {
        timeout,
        ..intent_js::EvalOptions::default()
    };
    match intent_js::eval(&full_code, &opts, Some(host)).await {
        Ok(v) => {
            let logs = v
                .get("__logs")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            let exec_error =
                parse_exec_failures(v.get("__execFailures"), v.get("__execFailedTotal"));
            match v.get("__k").and_then(Value::as_str) {
                Some("u") => RunOutcome::Continue {
                    logs,
                    state: StateUpdate::Keep,
                    exec_error,
                },
                Some("v") => parse_outcome(
                    &v.get("__v").cloned().unwrap_or(Value::Null),
                    logs,
                    exec_error,
                ),
                Some("e") => RunOutcome::Failed {
                    error: v
                        .get("__v")
                        .and_then(Value::as_str)
                        .unwrap_or("(unknown script error)")
                        .to_string(),
                    logs: RunLogs::Captured(logs),
                },
                _ => RunOutcome::Failed {
                    error: "engine: unexpected hook eval envelope".to_string(),
                    logs: RunLogs::Lost,
                },
            }
        }
        // Timeout or engine failure: the eval died before the envelope (and
        // its console capture) could be returned — the logs are lost.
        Err(e) => RunOutcome::Failed {
            error: e.to_string(),
            logs: RunLogs::Lost,
        },
    }
}

/// Interpret the script's returned value: only `{ dispatch: true }` fires a
/// dispatch; everything else (including `null` and non-objects) re-runs.
fn parse_outcome(v: &Value, logs: Option<String>, exec_error: Option<String>) -> RunOutcome {
    let mut logs = logs;
    let state = parse_state(v, &mut logs);
    if v.get("dispatch").and_then(Value::as_bool) == Some(true) {
        let message = v
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("(hook dispatched with no message)")
            .to_string();
        return RunOutcome::Dispatch {
            message,
            logs,
            state,
            exec_error,
        };
    }
    RunOutcome::Continue {
        logs,
        state,
        exec_error,
    }
}

/// Fold the run's captured `ws.host.exec` failure lines (`__execFailures`
/// from the eval envelope) into the diagnostic summary persisted to
/// `last_error` on non-evicting runs (monorepo#3231): `None` when the run
/// had no failed execs. The harness owns the wording; the line count and
/// per-line length are already capped in the envelope, while
/// `__execFailedTotal` carries the uncapped failure count so the summary
/// names the true total instead of silently understating past the cap.
/// `lastError` is workspace-visible via `hook.list`, so the capture is
/// secret-conscious: the envelope records only the command name + arg count
/// (never raw args), and any URL-embedded `user[:pass]@` credential a tool
/// echoes to stderr is redacted here (monorepo#836 helper).
fn parse_exec_failures(v: Option<&Value>, total: Option<&Value>) -> Option<String> {
    let lines: Vec<String> = v?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(intent_git::redact::redact_credentials)
        .collect();
    if lines.is_empty() {
        return None;
    }
    let total = total
        .and_then(Value::as_u64)
        .map_or(lines.len(), |t| usize::try_from(t).unwrap_or(usize::MAX))
        .max(lines.len());
    let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
    Some(crate::harness::latest().hook_exec_failures_warning(&lines, total))
}

/// Interpret a returned `state` field: absent keeps the previous carry-over
/// state, `null` clears it, anything else replaces it with its JSON
/// serialization — unless that exceeds [`HOOK_STATE_MAX_BYTES`], in which
/// case the previous state is kept and a warning line is appended to the
/// run's logs (the combined capture is re-trimmed to [`HOOK_LOG_MAX_BYTES`],
/// dropping the oldest output first, so `last_logs` stays bounded).
fn parse_state(v: &Value, logs: &mut Option<String>) -> StateUpdate {
    let Some(state) = v.as_object().and_then(|o| o.get("state")) else {
        return StateUpdate::Keep;
    };
    if state.is_null() {
        return StateUpdate::Clear;
    }
    let serialized = state.to_string();
    if serialized.len() > HOOK_STATE_MAX_BYTES {
        let warning = crate::harness::latest()
            .hook_state_dropped_warning(serialized.len(), HOOK_STATE_MAX_BYTES);
        let combined = match logs.take() {
            Some(l) => format!("{l}\n{warning}"),
            None => warning,
        };
        *logs = Some(tail_truncate(combined, HOOK_LOG_MAX_BYTES));
        return StateUpdate::Keep;
    }
    StateUpdate::Set(serialized)
}

/// Keep at most the last `max_bytes` bytes of `s` (on a char boundary),
/// dropping the oldest content first — the same tail-keep policy as the
/// console shim's line buffer.
fn tail_truncate(s: String, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s;
    }
    let mut start = s.len() - max_bytes;
    while !s.is_char_boundary(start) {
        start += 1;
    }
    s[start..].to_string()
}

/// Append a run's captured console output to a wake message as a
/// `[hook logs]` section, head-truncated to [`HOOK_WAKE_LOGS_CAP`] chars so a
/// log-heavy run cannot flood the owner's queue. No-op when the run logged
/// nothing. Wording and truncation owned by the harness (H6).
pub(crate) fn with_wake_logs(message: &str, logs: Option<&str>) -> String {
    crate::harness::latest().hook_wake_logs_section(message, logs)
}

/// Project one active hook into its idle-visibility `waitingOnHooks` entry:
/// `{ hookId, name, nextRunAt?, expiresAt? }` — light metadata only, no
/// code/lastState/logs.
fn waiting_on_hooks_entry(h: Hook) -> Value {
    let mut v = json!({
        "hookId": h.hook_id,
        "name": h.name,
    });
    if let Some(next) = h.next_run_at {
        v["nextRunAt"] = Value::String(next);
    }
    if let Some(exp) = h.expires_at {
        v["expiresAt"] = Value::String(exp);
    }
    v
}

impl Services {
    /// Whether a hook's owning session is a sub-agent
    /// (`parent_agent_id.is_some() || is_background`) — the same derivation
    /// the session's `workspace_api` bridge captures at spawn, so a
    /// sub-agent-owned hook gets the same `ws.app.question.*` gate. A
    /// missing/unreadable session falls back to `false`: the dispatch host
    /// still requires a live turn registry for `question.ask`, so nothing is
    /// exposed by treating an orphan as top-level.
    async fn hook_owner_is_sub_agent(&self, agent_id: &AgentId) -> bool {
        match self.store.get_agent_session_summary(agent_id).await {
            Ok(session) => session.parent_agent_id.is_some() || session.is_background,
            Err(_) => false,
        }
    }

    /// `hook.schedule`: validate, run the script once immediately (a real run
    /// — a dispatch wakes the owner and, for a one-shot hook, never persists
    /// a schedule; a perpetual hook persists and schedules anyway; a failure
    /// rejects the call), then persist the hook and spawn its scheduler task.
    /// Rejected outright when `agentFeatures.backgroundHooks` is off (services
    /// layer defense in depth behind the MCP dispatch deny); already-active
    /// hooks are unaffected by the toggle and run to their terminal state/TTL.
    pub(crate) async fn hook_schedule_op(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        params: &Value,
    ) -> Result<Value> {
        let agent_features = self.effective_settings().agent_features;
        if !agent_features.background_hooks {
            return Err(Error::InvalidParams(
                "hook.schedule: disabled in settings (agentFeatures.backgroundHooks = false)"
                    .into(),
            ));
        }
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::InvalidParams("hook.schedule: `name` is required".into()))?;
        if name.chars().count() > MAX_HOOK_NAME_LEN {
            return Err(Error::InvalidParams(format!(
                "hook.schedule: `name` must be at most {MAX_HOOK_NAME_LEN} characters"
            )));
        }
        let code = params
            .get("code")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| Error::InvalidParams("hook.schedule: `code` is required".into()))?;
        // Exactly one of `delayMs` | `cron` | `runAt` (spec: schedule kinds).
        let kind = parse_schedule_kind(params)?;
        // Perpetual hooks survive a dispatch; omitted defaults to one-shot.
        // A `runAt` hook fires once by definition — `perpetual` is rejected.
        let perpetual = params
            .get("perpetual")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if perpetual && matches!(kind, ScheduleKind::RunAt(_)) {
            return Err(Error::InvalidParams(
                "hook.schedule: `perpetual` cannot be combined with `runAt` — a runAt hook \
                 fires exactly once"
                    .into(),
            ));
        }
        // TTL per kind: `delayMs` keeps the 24-hour clamp, `cron` lifts it
        // to 7 days, and `runAt` implies its own expiry (the fire time +
        // 1h grace) — an explicit `ttlMs` is rejected there.
        let ttl_ms = params.get("ttlMs").and_then(Value::as_i64);
        let ttl_ms = match &kind {
            ScheduleKind::Delay(_) => Some(clamp_ttl_ms(ttl_ms)),
            ScheduleKind::Cron(_) => Some(clamp_cron_ttl_ms(ttl_ms)),
            ScheduleKind::RunAt(_) => {
                if params.get("ttlMs").is_some() {
                    return Err(Error::InvalidParams(
                        "hook.schedule: `ttlMs` cannot be combined with `runAt` — the hook \
                         expires 1 hour after its fire time"
                            .into(),
                    ));
                }
                None
            }
        };
        // Per-agent cap on active hooks (`[hooks] maxPerAgent`).
        let cap = self.hooks_max_per_agent as usize;
        let active = self
            .store
            .list_hooks_by_agent(agent_id)
            .await?
            .into_iter()
            .filter(|h| matches!(h.state, HookState::Scheduled | HookState::Running))
            .count();
        if active >= cap {
            return Err(Error::InvalidParams(format!(
                "hook.schedule: agent already has {active} active hooks (max {cap})"
            )));
        }

        // expiresAt: for `delayMs`/`cron` it counts from creation (one
        // instant feeds both fields); for `runAt` it is the fire time + the
        // grace window.
        let created = OffsetDateTime::now_utc();
        let created_at = created.format(&Rfc3339).unwrap_or_else(|_| now_iso());
        let expires_at = match (&kind, ttl_ms) {
            (ScheduleKind::RunAt(at), _) => {
                let fire = OffsetDateTime::parse(at, &Rfc3339)
                    .map_err(|e| Error::Internal(format!("parse validated runAt: {e}")))?;
                // Overflow rejected by parse_schedule_kind; checked here too
                // so a boundary timestamp can never panic.
                fire.checked_add(time::Duration::milliseconds(RUN_AT_GRACE_MS))
                    .ok_or_else(|| {
                        Error::InvalidParams(
                            "hook.schedule: `runAt` is too far in the future".into(),
                        )
                    })?
                    .format(&Rfc3339)
                    .unwrap_or_default()
            }
            (_, ttl_ms) => (created + time::Duration::milliseconds(ttl_ms.unwrap_or_default()))
                .format(&Rfc3339)
                .unwrap_or_default(),
        };
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: workspace_id.clone(),
            agent_id: agent_id.clone(),
            name: name.to_string(),
            code: code.to_string(),
            delay_ms: kind.delay_ms(),
            cron: kind.cron(),
            run_at: kind.run_at(),
            state: HookState::Running,
            created_at,
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(expires_at),
            perpetual,
            dispatch_count: 0,
        };

        // Validation run (spec decision 4): a REAL run, before anything
        // persists. Failure rejects the schedule (nothing is persisted and no
        // lifecycle event fires — the error surfaces on the call itself); a
        // dispatch wakes the owner immediately and the hook never schedules.
        let api: Arc<dyn WorkspaceApi> = Arc::new(self.clone());
        let is_sub_agent = self.hook_owner_is_sub_agent(agent_id).await;
        let outcome = run_hook_script(
            api,
            &hook,
            self.hook_eval_timeout,
            &agent_features,
            is_sub_agent,
        )
        .await;
        match outcome {
            RunOutcome::Failed { error, .. } => Err(Error::InvalidParams(format!(
                "hook.schedule: first run failed: {error}"
            ))),
            RunOutcome::Dispatch {
                message,
                logs,
                state,
                exec_error,
            } => {
                let mut hook = hook;
                hook.last_run_at = Some(now_iso());
                hook.run_count = 1;
                hook.last_logs = logs;
                hook.last_error = exec_error;
                state.apply(&mut hook);
                // Perpetual (spec decision 4b): a dispatching validation run
                // wakes the owner AND persists the ACTIVE schedule — unlike
                // one-shot, where the hook never schedules.
                if hook.perpetual {
                    hook.dispatch_count = 1;
                    // In-flight-run-at-expiry parity: a validation run can
                    // outlive a short TTL, so a dispatch landing at/after
                    // `expiresAt` must expire the hook instead of persisting
                    // a re-armed active schedule — matching the
                    // scheduler-loop dispatch path, which resolves this
                    // exact race the same way. The dispatch still wins (the
                    // owner is woken below regardless).
                    let expired = is_expired(hook.expires_at.as_deref(), self.hook_clock_skew_ms());
                    hook.state = if expired {
                        HookState::Expired
                    } else {
                        HookState::Scheduled
                    };
                    let mut initial_delay = None;
                    hook.next_run_at = if expired {
                        None
                    } else {
                        let (next, sleep) = kind_next_fire(&kind)?;
                        initial_delay = Some(sleep);
                        Some(next)
                    };
                    self.store.insert_hook(&hook).await?;
                    self.emit_hook_event(HOOK_RUN_COMPLETED, &hook, None).await;
                    let message = with_wake_logs(&message, hook.last_logs.as_deref());
                    self.wake_hook_owner(&hook, &message, "dispatched").await;
                    self.emit_hook_event(HOOK_DISPATCHED, &hook, None).await;
                    if expired {
                        self.finish_expiry(&hook).await;
                        return Ok(json!({ "hook": hook, "dispatched": true }));
                    }
                    self.emit_hook_event(HOOK_SCHEDULED, &hook, hook.next_run_at.clone())
                        .await;
                    self.spawn_hook_task_with_initial_delay(hook.clone(), initial_delay);
                    // A newly persisted active hook can promote the derived
                    // displayStatus to `in_progress` (§6.5) and raise the
                    // orthogonal `waiting` flag (§5.1).
                    self.maybe_emit_display_status_changed(workspace_id).await;
                    self.maybe_emit_waiting_changed(workspace_id).await;
                    return Ok(json!({ "hook": hook, "dispatched": true }));
                }
                hook.state = HookState::Dispatched;
                hook.dispatch_count = 1;
                self.store.insert_hook(&hook).await?;
                self.emit_hook_event(HOOK_RUN_COMPLETED, &hook, None).await;
                let message = with_wake_logs(&message, hook.last_logs.as_deref());
                self.wake_hook_owner(&hook, &message, "dispatched").await;
                self.emit_hook_event(HOOK_DISPATCHED, &hook, None).await;
                // Hook settled terminal: recompute the derived displayStatus
                // (§6.5) and the orthogonal `waiting` flag (§5.1) —
                // best-effort, transition-only emission.
                self.maybe_emit_display_status_changed(workspace_id).await;
                self.maybe_emit_waiting_changed(workspace_id).await;
                Ok(json!({ "hook": hook, "dispatched": true }))
            }
            RunOutcome::Continue {
                logs,
                state,
                exec_error,
            } => {
                let mut hook = hook;
                hook.last_run_at = Some(now_iso());
                hook.run_count = 1;
                hook.last_logs = logs;
                hook.last_error = exec_error;
                state.apply(&mut hook);
                // A validation run that outlasts a short TTL completes
                // normally, but a continue at/after expiresAt expires
                // instead of scheduling (the in-flight-run-at-expiry rule).
                if is_expired(hook.expires_at.as_deref(), self.hook_clock_skew_ms()) {
                    hook.state = HookState::Expired;
                    self.store.insert_hook(&hook).await?;
                    self.emit_hook_event(HOOK_RUN_COMPLETED, &hook, None).await;
                    self.finish_expiry(&hook).await;
                    return Ok(json!({ "hook": hook, "dispatched": false }));
                }
                hook.state = HookState::Scheduled;
                let (next, initial_delay) = kind_next_fire(&kind)?;
                hook.next_run_at = Some(next);
                self.store.insert_hook(&hook).await?;
                self.emit_hook_event(HOOK_RUN_COMPLETED, &hook, hook.next_run_at.clone())
                    .await;
                self.emit_hook_event(HOOK_SCHEDULED, &hook, hook.next_run_at.clone())
                    .await;
                self.spawn_hook_task_with_initial_delay(hook.clone(), Some(initial_delay));
                // A newly persisted active hook can promote the derived
                // displayStatus to `in_progress` (§6.5) and raise the
                // orthogonal `waiting` flag (§5.1).
                self.maybe_emit_display_status_changed(workspace_id).await;
                self.maybe_emit_waiting_changed(workspace_id).await;
                Ok(json!({ "hook": hook, "dispatched": false }))
            }
        }
    }

    /// `hook.list`: hooks in a workspace (optionally one agent's), oldest
    /// first, as `{ hooks: [Hook] }`.
    pub(crate) async fn hook_list_op(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: Option<&AgentId>,
    ) -> Result<Value> {
        let hooks = match agent_id {
            Some(a) => self.store.list_hooks_by_agent(a).await?,
            None => self.store.list_hooks_by_workspace(workspace_id).await?,
        };
        let hooks: Vec<Hook> = hooks
            .into_iter()
            .filter(|h| &h.workspace_id == workspace_id)
            .collect();
        Ok(json!({ "hooks": hooks }))
    }

    /// `hook.get` (MCP-only): one hook row by id — the full row including
    /// `code`, for active AND terminal (retired) hooks, so an agent can
    /// recover a retired hook's script to re-arm it. A hook belonging to
    /// another workspace reads as `NotFound` (mirrors `hook.cancel` /
    /// `hook.runNow`), so hooks are never readable across workspaces.
    pub(crate) async fn hook_get_op(
        &self,
        workspace_id: &WorkspaceId,
        hook_id: &HookId,
    ) -> Result<Value> {
        let hook = self.store.get_hook(hook_id).await?;
        if &hook.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("hook {} not found", hook_id.0)));
        }
        Ok(json!(hook))
    }

    /// Light metadata for `agent_id`'s ACTIVE (`scheduled`/`running`) hooks —
    /// the idle-visibility `waitingOnHooks` payload: one
    /// `{ hookId, name, nextRunAt?, expiresAt? }` object per active hook,
    /// oldest first (no code/lastState/logs — payloads stay light). Empty when
    /// the agent owns no active hook; a store failure is logged and reads as
    /// empty (visibility is best-effort and must never block an idle emit or
    /// wake delivery).
    pub(crate) async fn active_hooks_for_agent(&self, agent_id: &AgentId) -> Vec<Value> {
        let hooks = match self.store.list_hooks_by_agent(agent_id).await {
            Ok(hooks) => hooks,
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id.0,
                    error = %e,
                    "active-hooks lookup failed; waitingOnHooks reads as empty"
                );
                return Vec::new();
            }
        };
        hooks
            .into_iter()
            .filter(|h| matches!(h.state, HookState::Scheduled | HookState::Running))
            .map(waiting_on_hooks_entry)
            .collect()
    }

    /// Workspace-batched variant of
    /// [`active_hooks_for_agent`](Self::active_hooks_for_agent) for
    /// `agent.list`: one store query for the whole workspace, grouped by
    /// owning agent id (agents with no active hook are absent). A store
    /// failure is logged and reads as empty, same as the per-agent lookup.
    pub(crate) async fn active_hooks_by_agent(
        &self,
        workspace_id: &WorkspaceId,
    ) -> HashMap<String, Vec<Value>> {
        let hooks = match self.store.list_hooks_by_workspace(workspace_id).await {
            Ok(hooks) => hooks,
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "active-hooks workspace lookup failed; waitingOnHooks reads as empty"
                );
                return HashMap::new();
            }
        };
        let mut by_agent: HashMap<String, Vec<Value>> = HashMap::new();
        for h in hooks
            .into_iter()
            .filter(|h| matches!(h.state, HookState::Scheduled | HookState::Running))
        {
            let agent = h.agent_id.0.clone();
            by_agent
                .entry(agent)
                .or_default()
                .push(waiting_on_hooks_entry(h));
        }
        by_agent
    }

    /// Whether the workspace owns any ACTIVE (`scheduled`/`running`) hook —
    /// a `Workspace.waiting` signal (§5.1, via
    /// [`Services::workspace_is_waiting`]): an idle agent still watching via
    /// a background hook reads as waiting, without promoting the
    /// `displayStatus` rollup. SQL-side count-only aggregate — no row
    /// hydration and no dependence on how many terminal rows the workspace
    /// accumulated. Best-effort: a store read failure is logged
    /// and fails open to `false` (mirrors
    /// [`Services::workspace_attention_signals`]) so list/get emission is
    /// never wedged and activity is never fabricated.
    pub(crate) async fn workspace_has_active_hooks(&self, workspace_id: &WorkspaceId) -> bool {
        match self
            .store
            .count_active_hooks_by_workspace(workspace_id)
            .await
        {
            Ok(n) => n > 0,
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "active-hooks displayStatus lookup failed; reads as none"
                );
                false
            }
        }
    }

    /// Stamp `waitingOnHooks` onto an `agent:idle`-style event `data` object
    /// when `agent_id` owns at least one active hook (the field is omitted —
    /// never `[]` — otherwise, and an existing stamp is left untouched).
    /// Returns the stamped list (empty when nothing was stamped and no stamp
    /// was present).
    pub(crate) async fn annotate_waiting_on_hooks(
        &self,
        agent_id: &AgentId,
        data: &mut Value,
    ) -> Vec<Value> {
        if let Some(existing) = data.get("waitingOnHooks").and_then(Value::as_array) {
            return existing.clone();
        }
        let hooks = self.active_hooks_for_agent(agent_id).await;
        if !hooks.is_empty() {
            if let Some(obj) = data.as_object_mut() {
                obj.insert("waitingOnHooks".to_string(), Value::Array(hooks.clone()));
            }
        }
        hooks
    }

    /// `hook.cancel`: stop an active hook's task, mark it cancelled, and emit
    /// `hook:cancelled`. `caller` is the cancelling agent (MCP): hooks are
    /// agent-owned, so a non-owner is rejected and an owner cancel delivers
    /// no self-wake. The FE path (`caller = None`) cancels any hook and
    /// additionally wakes the owning agent with a notice.
    pub(crate) async fn hook_cancel_op(
        &self,
        workspace_id: &WorkspaceId,
        hook_id: &HookId,
        caller: Option<&AgentId>,
    ) -> Result<Value> {
        let hook = self.store.get_hook(hook_id).await?;
        if &hook.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("hook {} not found", hook_id.0)));
        }
        if let Some(caller) = caller {
            if caller != &hook.agent_id {
                return Err(Error::InvalidParams(format!(
                    "hook.cancel: hook {} is owned by agent {} — you can only cancel your own hooks",
                    hook_id.0, hook.agent_id.0
                )));
            }
        }
        if !matches!(hook.state, HookState::Scheduled | HookState::Running) {
            return Err(Error::InvalidParams(format!(
                "hook.cancel: hook {} is not active",
                hook_id.0
            )));
        }
        // FE-cancel (no agent caller) wakes the owner with a notice;
        // owner-side cancel delivers no wake.
        let notice = caller
            .is_none()
            .then(|| crate::harness::latest().hook_cancelled_from_app_notice());
        let hook = self.cancel_active_hook(hook, notice.as_deref()).await?;
        Ok(json!({ "ok": true, "hook": hook }))
    }

    /// Core cancel transition shared by [`Services::hook_cancel_op`] and the
    /// archive sweep ([`Services::cancel_workspace_hooks`]): abort the
    /// scheduler task, persist `cancelled`, clear `nextRunAt`, and emit
    /// `hook:cancelled`. With a `wake_notice` the owner is woken (the wake
    /// runs the deferral backstop itself, inside `wake_hook_owner`, after
    /// the delivery attempt); without one, no wake is delivered — a deferred
    /// completion watch on the (idle) owner would otherwise never settle
    /// when this was its last active hook, so the backstop runs directly.
    /// The caller must have verified the hook is ACTIVE.
    async fn cancel_active_hook(&self, mut hook: Hook, wake_notice: Option<&str>) -> Result<Hook> {
        self.abort_hook_task(&hook.hook_id);
        self.store
            .update_hook_state(&hook.hook_id, HookState::Cancelled)
            .await?;
        self.store.update_hook_next_run(&hook.hook_id, None).await?;
        hook.state = HookState::Cancelled;
        hook.next_run_at = None;
        self.emit_hook_event(HOOK_CANCELLED, &hook, None).await;
        match wake_notice {
            Some(notice) => self.wake_hook_owner(&hook, notice, "cancelled").await,
            None => self.resettle_owner_after_hook_terminal(&hook).await,
        }
        // The last active hook settling can demote the derived displayStatus
        // (§6.5) and drop the orthogonal `waiting` flag (§5.1) —
        // best-effort, transition-only emission.
        self.maybe_emit_display_status_changed(&hook.workspace_id)
            .await;
        self.maybe_emit_waiting_changed(&hook.workspace_id).await;
        Ok(hook)
    }

    /// Archive sweep (`workspace.archive`): cancel every ACTIVE
    /// (`scheduled`/`running`) hook in the workspace through the
    /// `hook.cancel` machinery — task aborted, state persisted to
    /// `cancelled`, `hook:cancelled` emitted — plus an owner-wake notice so
    /// the agent learns why its watch stopped. Runs AFTER the archived row
    /// is persisted: the wake rides the archived gate in
    /// [`Services::deliver_wake_message`], so it parks in the queue (at
    /// most) and never starts a turn while the workspace is archived.
    /// Terminal hooks (`dispatched`/`evicted`/`cancelled`/`expired`) are
    /// untouched. Best-effort per hook: a store failure is logged and the
    /// sweep moves on — archiving must not fail because one hook row would
    /// not update.
    pub(crate) async fn cancel_workspace_hooks(&self, workspace_id: &WorkspaceId) {
        let hooks = match self.store.list_hooks_by_workspace(workspace_id).await {
            Ok(hooks) => hooks,
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "archive hook sweep: hook list failed; skipping"
                );
                return;
            }
        };
        for hook in hooks {
            if !matches!(hook.state, HookState::Scheduled | HookState::Running) {
                continue;
            }
            let hook_id = hook.hook_id.clone();
            if let Err(e) = self
                .cancel_active_hook(
                    hook,
                    Some(&crate::harness::latest().hook_cancelled_workspace_archived_notice()),
                )
                .await
            {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    hook = %hook_id.0,
                    error = %e,
                    "archive hook sweep: cancel failed; continuing"
                );
            }
        }
    }

    /// Retire sweep (`ws.agent.retire`): cancel every ACTIVE
    /// (`scheduled`/`running`) hook owned by the retiring agent through the
    /// shared cancel transition ([`Services::cancel_active_hook`]) — task
    /// aborted, state persisted to `cancelled`, `nextRunAt` cleared,
    /// `hook:cancelled` emitted, waiting recomputed (§5.1). NO wake notice:
    /// the owner retired itself and is inert, so parking a notice in its
    /// queue is noise (the backstop in `cancel_active_hook` settles any
    /// deferred watches directly). Restore does NOT resurrect cancelled
    /// hooks (mirrors the unarchive precedent) — the agent re-registers if
    /// the condition still matters. Best-effort per hook: a store failure
    /// is logged and the sweep moves on — retiring must not fail because
    /// one hook row would not update.
    pub(crate) async fn cancel_agent_hooks(&self, agent_id: &AgentId) {
        let hooks = match self.store.list_hooks_by_agent(agent_id).await {
            Ok(hooks) => hooks,
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id.0,
                    error = %e,
                    "retire hook sweep: hook list failed; skipping"
                );
                return;
            }
        };
        for hook in hooks {
            if !matches!(hook.state, HookState::Scheduled | HookState::Running) {
                continue;
            }
            let hook_id = hook.hook_id.clone();
            if let Err(e) = self.cancel_active_hook(hook, None).await {
                tracing::warn!(
                    agent = %agent_id.0,
                    hook = %hook_id.0,
                    error = %e,
                    "retire hook sweep: cancel failed; continuing"
                );
            }
        }
    }

    /// Delete teardown (`workspace.delete`): eagerly abort every live hook
    /// scheduler task owned by the workspace. The store cascade drops the
    /// hook rows themselves, but without this sweep a live task would only
    /// exit lazily at its next tick (the pre-run re-read finds the row
    /// gone) — until then it lingers in `hook_tasks` holding its timer.
    /// Must run BEFORE the cascade so the rows are still listable.
    /// Best-effort: a list failure is logged and skipped (the lazy
    /// next-tick exit still applies).
    pub(crate) async fn abort_workspace_hook_tasks(&self, workspace_id: &WorkspaceId) {
        let hooks = match self.store.list_hooks_by_workspace(workspace_id).await {
            Ok(hooks) => hooks,
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "delete hook sweep: hook list failed; tasks will exit lazily"
                );
                return;
            }
        };
        for hook in hooks {
            self.abort_hook_task(&hook.hook_id);
        }
    }

    /// `hook.runNow`: signal an active hook's task to run immediately (the
    /// inter-run timer resets after the run). On a `runAt` hook the
    /// triggered run IS the one-shot fire — the hook fires early and
    /// retires whether or not it dispatched ([`Services::execute_hook_run`]
    /// treats any `runAt` run as terminal), so the one-shot contract is
    /// honored over the timestamp.
    pub(crate) async fn hook_run_now_op(
        &self,
        workspace_id: &WorkspaceId,
        hook_id: &HookId,
    ) -> Result<Value> {
        let hook = self.store.get_hook(hook_id).await?;
        if &hook.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("hook {} not found", hook_id.0)));
        }
        if !matches!(hook.state, HookState::Scheduled | HookState::Running) {
            return Err(Error::InvalidParams(format!(
                "hook.runNow: hook {} is not active",
                hook_id.0
            )));
        }
        let control = {
            let tasks = self.hook_tasks.lock().unwrap();
            tasks.get(hook_id).map(|h| h.control.clone())
        };
        let Some(control) = control else {
            return Err(Error::Internal(format!(
                "hook.runNow: hook {} has no live scheduler task",
                hook_id.0
            )));
        };
        control
            .send(HookControl::RunNow)
            .await
            .map_err(|_| Error::Internal("hook.runNow: scheduler task is gone".into()))?;
        Ok(json!({ "ok": true, "hookId": hook_id }))
    }

    /// Boot rehydration: reload every active (`scheduled`/`running`) hook row
    /// and respawn its scheduler task. Rows whose owning agent is gone are
    /// cancelled instead of resumed; rows whose `expiresAt` passed while the
    /// daemon was down are expired (owner woken). `running` rows (daemon died
    /// mid-run) are reset to `scheduled` with a kind-appropriate fresh
    /// countdown (their persisted `nextRunAt` is the started-run's deadline,
    /// not a future schedule); every other resumed hook resumes per its
    /// schedule kind ([`resumed_next_run`] — `cron`/`runAt` rows resume to
    /// their ABSOLUTE persisted deadline, `delayMs` rows to the EARLIER of
    /// the persisted deadline and a fresh `now + delayMs` countdown, so a
    /// restart never pushes a run further out, and an overdue row runs
    /// promptly, still gated by the pre-run expiry check;
    /// intent-hq/monorepo#2856) and keeps its ORIGINAL `expiresAt` (the TTL
    /// does not reset on restart).
    /// `agentFeatures.backgroundHooks = false` does
    /// NOT cancel or skip active rows (decided semantics: the toggle only
    /// rejects NEW schedules; existing hooks run to their terminal
    /// state/TTL). Returns the number of resumed hooks.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if loading the persisted hooks or an owner status lookup fails.
    pub async fn rehydrate_hooks(&self) -> Result<usize> {
        let hooks = self.store.load_active_hooks().await?;
        let mut resumed = 0;
        for mut hook in hooks {
            if self.hook_task_alive(&hook.hook_id) {
                continue;
            }
            // Prune hooks whose owner no longer exists (deleted agents keep
            // their session row with status `deleted`) or is soft-retired —
            // the retire-time sweep ([`Services::cancel_agent_hooks`]) could
            // have been missed by a crash window.
            let owner_gone = match self.store.get_agent_session_summary(&hook.agent_id).await {
                Ok(session) => {
                    session.status == AgentStatus::Deleted || session.retired_at.is_some()
                }
                Err(Error::NotFound(_)) => true,
                Err(e) => return Err(e),
            };
            if owner_gone {
                let _ = self
                    .store
                    .update_hook_state(&hook.hook_id, HookState::Cancelled)
                    .await;
                let _ = self.store.update_hook_next_run(&hook.hook_id, None).await;
                hook.state = HookState::Cancelled;
                hook.next_run_at = None;
                self.emit_hook_event(HOOK_CANCELLED, &hook, None).await;
                // The pruned row may have been the workspace's last wait
                // signal (§5.1).
                self.maybe_emit_waiting_changed(&hook.workspace_id).await;
                continue;
            }
            // Expired while the daemon was down: expire at boot (owner
            // woken), never resume.
            if is_expired(hook.expires_at.as_deref(), self.hook_clock_skew_ms()) {
                self.expire_hook(&mut hook).await;
                continue;
            }
            // Must run before the state heal below: `resumed_next_run`
            // branches on `Running` (interrupted mid-run ⇒ fresh countdown).
            let (next_run_at, initial_delay) = resumed_next_run(&hook);
            if hook.state == HookState::Running {
                self.store
                    .update_hook_state(&hook.hook_id, HookState::Scheduled)
                    .await?;
                hook.state = HookState::Scheduled;
            }
            self.store
                .update_hook_next_run(&hook.hook_id, Some(&next_run_at))
                .await?;
            hook.next_run_at = Some(next_run_at.clone());
            self.emit_hook_event(HOOK_SCHEDULED, &hook, Some(next_run_at))
                .await;
            self.spawn_hook_task_with_initial_delay(hook, Some(initial_delay));
            resumed += 1;
        }
        Ok(resumed)
    }

    /// Whether a live scheduler task exists for `hook_id`.
    fn hook_task_alive(&self, hook_id: &HookId) -> bool {
        self.hook_tasks.lock().unwrap().contains_key(hook_id)
    }

    /// Abort and deregister a hook's scheduler task (no-op when absent).
    fn abort_hook_task(&self, hook_id: &HookId) {
        if let Some(handle) = self.hook_tasks.lock().unwrap().remove(hook_id) {
            handle.abort.abort();
        }
    }

    /// Spawn the per-hook scheduler task: sleep until the next fire (or a
    /// `runNow` control frame) raced against the TTL deadline, run the
    /// script, and act on the outcome. A run is never STARTED at/after
    /// `expiresAt` (a run already in flight at expiry completes normally).
    /// The task deregisters itself from [`Services::hook_tasks`] on every
    /// exit path. The explicit first-iteration sleep: schedule passes the
    /// freshly computed time to the kind's next fire, and rehydration
    /// passes the resumed countdown (which may be shorter than the cadence,
    /// or zero for an overdue row) so the persisted `nextRunAt` is honored
    /// across restarts; every later iteration sleeps the duration the
    /// previous run computed for its kind ([`Services::execute_hook_run`] —
    /// the fixed `delayMs` cadence or the cron expression's next
    /// occurrence).
    fn spawn_hook_task_with_initial_delay(&self, hook: Hook, initial_delay: Option<Duration>) {
        let (control_tx, mut control_rx) = mpsc::channel::<HookControl>(4);
        let services = self.clone();
        let hook_id = hook.hook_id.clone();
        let join = tokio::spawn(async move {
            let mut hook = hook;
            let mut delay = initial_delay
                .unwrap_or_else(|| Duration::from_millis(hook.delay_ms.max(0).cast_unsigned()));
            loop {
                // Race the inter-run sleep against the time to `expiresAt`
                // (deadline-free legacy rows never take the expiry arm).
                let to_expiry =
                    time_to_expiry(hook.expires_at.as_deref(), services.hook_clock_skew_ms());
                let expiry = async {
                    match to_expiry {
                        Some(d) => tokio::time::sleep(d).await,
                        None => std::future::pending::<()>().await,
                    }
                };
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = expiry => {
                        services.expire_hook(&mut hook).await;
                        break;
                    }
                    ctl = control_rx.recv() => {
                        match ctl {
                            Some(HookControl::RunNow) => {}
                            // All senders dropped — the registry entry is
                            // gone (cancel path aborts first, but guard the
                            // race anyway).
                            None => break,
                        }
                    }
                }
                // Never start a run at/after expiresAt (guards a `runNow`
                // — or a sleep expiry — racing the deadline).
                if is_expired(hook.expires_at.as_deref(), services.hook_clock_skew_ms()) {
                    services.expire_hook(&mut hook).await;
                    break;
                }
                match services.execute_hook_run(&mut hook).await {
                    // Rescheduled: sleep the duration the run computed for
                    // its kind (fixed cadence, or the cron expression's
                    // next occurrence).
                    Ok(Some(next_delay)) => delay = next_delay,
                    // Terminal outcome (dispatch/evict/one-shot fire): stop
                    // the loop.
                    Ok(None) => break,
                    // Unrecoverable store failure: the row may already be
                    // `running` with no task left to drive it — best-effort
                    // evict so it doesn't linger active-looking forever.
                    Err(e) => {
                        services.evict_hook_after_store_error(&mut hook, &e).await;
                        break;
                    }
                }
            }
            services.hook_tasks.lock().unwrap().remove(&hook.hook_id);
        });
        self.hook_tasks.lock().unwrap().insert(
            hook_id,
            HookHandle {
                control: control_tx,
                abort: join.abort_handle(),
            },
        );
    }

    /// One scheduled run of `hook`'s script. Returns `Ok(Some(sleep))` to
    /// keep the loop alive (script continued and the hook rescheduled —
    /// `sleep` is the inter-run wait its kind computed), `Ok(None)` on a
    /// terminal outcome (dispatched, evicted, expired, or a one-shot
    /// `runAt` fire).
    async fn execute_hook_run(&self, hook: &mut Hook) -> Result<Option<Duration>> {
        // A cancel can race the sleep expiry: re-read the persisted state and
        // stop silently if this hook is no longer active.
        match self.store.get_hook(&hook.hook_id).await {
            Ok(h) if matches!(h.state, HookState::Scheduled | HookState::Running) => {}
            Ok(_) | Err(Error::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        }
        self.store
            .update_hook_state(&hook.hook_id, HookState::Running)
            .await?;
        hook.state = HookState::Running;
        self.emit_hook_event(HOOK_RUN_STARTED, hook, None).await;
        let api: Arc<dyn WorkspaceApi> = Arc::new(self.clone());
        // Feature flags are read fresh per run: a hook outlives sessions and
        // daemon restarts, so the current effective `[agentFeatures]` gate is
        // the same one a newly created session's bridge would capture. The
        // owner's sub-agent status is likewise re-derived per run.
        let agent_features = self.effective_settings().agent_features;
        let is_sub_agent = self.hook_owner_is_sub_agent(&hook.agent_id).await;
        let outcome = run_hook_script(
            api,
            hook,
            self.hook_eval_timeout,
            &agent_features,
            is_sub_agent,
        )
        .await;
        let last_run_at = now_iso();
        match outcome {
            RunOutcome::Continue {
                logs,
                state,
                exec_error,
            } => {
                // In-flight-run-at-expiry: a run already executing when the
                // TTL passes completes normally, but a continue at/after
                // `expiresAt` expires the hook instead of rescheduling it
                // (a dispatch still wins — see the Dispatch arm). A `runAt`
                // fire that continues is likewise terminal — the one-shot
                // timer has fired and there is no later tick — but retires
                // with the fired notice, not the TTL wording. A cron
                // schedule whose expression has no computable next
                // occurrence is exhausted: expire it (owner woken) rather
                // than leave the row active with no future fire.
                let expired = is_expired(hook.expires_at.as_deref(), self.hook_clock_skew_ms());
                let next = if expired || hook.run_at.is_some() {
                    None
                } else {
                    match hook_next_fire(hook) {
                        Ok(next) => Some(next),
                        Err(e) => {
                            tracing::warn!(
                                hook = %hook.hook_id.0,
                                error = %e,
                                "cron schedule exhausted; expiring hook"
                            );
                            None
                        }
                    }
                };
                let Some((next_run_at, next_delay)) = next else {
                    self.store
                        .update_hook_run(&hook.hook_id, &last_run_at, None)
                        .await?;
                    self.store
                        .update_hook_last_logs(&hook.hook_id, logs.as_deref())
                        .await?;
                    self.persist_hook_state(hook, state).await?;
                    self.persist_hook_exec_error(hook, exec_error).await?;
                    self.store.expire_hook(&hook.hook_id).await?;
                    hook.state = HookState::Expired;
                    hook.last_run_at = Some(last_run_at);
                    hook.next_run_at = None;
                    hook.run_count += 1;
                    hook.last_logs = logs;
                    self.emit_hook_event(HOOK_RUN_COMPLETED, hook, None).await;
                    if hook.run_at.is_some() && !expired {
                        self.finish_run_at_fire(hook).await;
                    } else {
                        self.finish_expiry(hook).await;
                    }
                    return Ok(None);
                };
                self.store
                    .update_hook_run(&hook.hook_id, &last_run_at, Some(&next_run_at))
                    .await?;
                self.store
                    .update_hook_last_logs(&hook.hook_id, logs.as_deref())
                    .await?;
                self.persist_hook_state(hook, state).await?;
                self.persist_hook_exec_error(hook, exec_error).await?;
                self.store
                    .update_hook_state(&hook.hook_id, HookState::Scheduled)
                    .await?;
                hook.state = HookState::Scheduled;
                hook.last_run_at = Some(last_run_at);
                hook.next_run_at = Some(next_run_at.clone());
                hook.run_count += 1;
                hook.last_logs = logs;
                self.emit_hook_event(HOOK_RUN_COMPLETED, hook, Some(next_run_at))
                    .await;
                Ok(Some(next_delay))
            }
            RunOutcome::Dispatch {
                message,
                logs,
                state,
                exec_error,
            } => {
                self.store
                    .update_hook_run(&hook.hook_id, &last_run_at, None)
                    .await?;
                self.store
                    .update_hook_last_logs(&hook.hook_id, logs.as_deref())
                    .await?;
                self.persist_hook_state(hook, state).await?;
                self.persist_hook_exec_error(hook, exec_error).await?;
                // A perpetual hook survives its own dispatch: count the fire,
                // wake the owner, then return to `scheduled` and keep the
                // loop alive. `hook:dispatched` is non-terminal here and may
                // fire once per cadence tick.
                if hook.perpetual {
                    self.store
                        .increment_hook_dispatch_count(&hook.hook_id)
                        .await?;
                    hook.last_run_at = Some(last_run_at);
                    hook.run_count += 1;
                    hook.dispatch_count += 1;
                    hook.last_logs = logs;
                    // In-flight-run-at-expiry: the dispatch still wins (the
                    // owner is woken below regardless), but a fire at/after
                    // `expiresAt` expires the hook instead of rescheduling
                    // it. Resolve and persist that outcome BEFORE emitting
                    // `hook:run-completed`/`hook:dispatched` so their `state`
                    // field reflects the real post-dispatch state rather than
                    // the transient `running` set at run start — matching the
                    // schedule-time validation path, which sets `state`
                    // before emitting. An exhausted cron expression (no
                    // computable next occurrence) expires the same way —
                    // `perpetual` is rejected for `runAt`, so re-arming only
                    // ever recomputes a delay or cron cadence.
                    let expired = is_expired(hook.expires_at.as_deref(), self.hook_clock_skew_ms());
                    let next = if expired {
                        None
                    } else {
                        match hook_next_fire(hook) {
                            Ok(next) => Some(next),
                            Err(e) => {
                                tracing::warn!(
                                    hook = %hook.hook_id.0,
                                    error = %e,
                                    "cron schedule exhausted; expiring hook"
                                );
                                None
                            }
                        }
                    };
                    let mut next_delay = None;
                    if let Some((next_run_at, sleep)) = next {
                        self.store
                            .update_hook_next_run(&hook.hook_id, Some(&next_run_at))
                            .await?;
                        self.store
                            .update_hook_state(&hook.hook_id, HookState::Scheduled)
                            .await?;
                        hook.state = HookState::Scheduled;
                        hook.next_run_at = Some(next_run_at);
                        next_delay = Some(sleep);
                    } else {
                        self.store.expire_hook(&hook.hook_id).await?;
                        hook.state = HookState::Expired;
                        hook.next_run_at = None;
                    }
                    self.emit_hook_event(HOOK_RUN_COMPLETED, hook, None).await;
                    let message = with_wake_logs(&message, hook.last_logs.as_deref());
                    self.wake_hook_owner(hook, &message, "dispatched").await;
                    self.emit_hook_event(HOOK_DISPATCHED, hook, None).await;
                    let Some(next_delay) = next_delay else {
                        self.finish_expiry(hook).await;
                        return Ok(None);
                    };
                    self.emit_hook_event(HOOK_SCHEDULED, hook, hook.next_run_at.clone())
                        .await;
                    return Ok(Some(next_delay));
                }
                self.store
                    .increment_hook_dispatch_count(&hook.hook_id)
                    .await?;
                self.store
                    .update_hook_state(&hook.hook_id, HookState::Dispatched)
                    .await?;
                hook.state = HookState::Dispatched;
                hook.last_run_at = Some(last_run_at);
                hook.next_run_at = None;
                hook.run_count += 1;
                hook.dispatch_count += 1;
                hook.last_logs = logs;
                self.emit_hook_event(HOOK_RUN_COMPLETED, hook, None).await;
                let message = with_wake_logs(&message, hook.last_logs.as_deref());
                self.wake_hook_owner(hook, &message, "dispatched").await;
                self.emit_hook_event(HOOK_DISPATCHED, hook, None).await;
                // The last active hook settling can demote the derived
                // displayStatus (§6.5) and drop the `waiting` flag (§5.1).
                self.maybe_emit_display_status_changed(&hook.workspace_id)
                    .await;
                self.maybe_emit_waiting_changed(&hook.workspace_id).await;
                Ok(None)
            }
            RunOutcome::Failed { error, logs } => {
                self.store
                    .update_hook_run(&hook.hook_id, &last_run_at, None)
                    .await?;
                // A timed-out/engine-failed eval dies before the capture can
                // be returned (RunLogs::Lost): leave the previous run's
                // persisted last_logs untouched rather than clobbering it.
                if let RunLogs::Captured(ref logs) = logs {
                    self.store
                        .update_hook_last_logs(&hook.hook_id, logs.as_deref())
                        .await?;
                    hook.last_logs.clone_from(logs);
                }
                // Persist the error before the terminal state so a reader
                // that observes `evicted` always sees `lastError`.
                self.store
                    .update_hook_last_error(&hook.hook_id, Some(&error))
                    .await?;
                self.store
                    .update_hook_state(&hook.hook_id, HookState::Evicted)
                    .await?;
                hook.state = HookState::Evicted;
                hook.last_run_at = Some(last_run_at);
                hook.next_run_at = None;
                hook.run_count += 1;
                hook.last_error = Some(error.clone());
                self.emit_hook_event(HOOK_EVICTED, hook, None).await;
                let notice =
                    crate::harness::latest().hook_evicted_failed_run_notice(&hook.name, &error);
                let notice = match logs {
                    RunLogs::Captured(ref l) => with_wake_logs(&notice, l.as_deref()),
                    RunLogs::Lost => notice,
                };
                self.wake_hook_owner(hook, &notice, "evicted").await;
                // The last active hook settling can demote the derived
                // displayStatus (§6.5) and drop the `waiting` flag (§5.1).
                self.maybe_emit_display_status_changed(&hook.workspace_id)
                    .await;
                self.maybe_emit_waiting_changed(&hook.workspace_id).await;
                Ok(None)
            }
        }
    }

    /// Persist a non-evicting run's `ws.host.exec` failure summary to
    /// `last_error` (monorepo#3231) — or clear a previously recorded one when
    /// this run's execs all succeeded, so a recovered check stops reading as
    /// broken. Skips the write when nothing changes (the common all-healthy
    /// cadence).
    async fn persist_hook_exec_error(
        &self,
        hook: &mut Hook,
        exec_error: Option<String>,
    ) -> Result<()> {
        if hook.last_error == exec_error {
            return Ok(());
        }
        self.store
            .update_hook_last_error(&hook.hook_id, exec_error.as_deref())
            .await?;
        hook.last_error = exec_error;
        Ok(())
    }

    /// Apply a run's [`StateUpdate`] to the persisted row and the in-memory
    /// hook (`Keep` writes nothing).
    async fn persist_hook_state(&self, hook: &mut Hook, state: StateUpdate) -> Result<()> {
        match state {
            StateUpdate::Keep => {}
            StateUpdate::Clear => {
                self.store
                    .update_hook_last_state(&hook.hook_id, None)
                    .await?;
                hook.last_state = None;
            }
            StateUpdate::Set(s) => {
                self.store
                    .update_hook_last_state(&hook.hook_id, Some(&s))
                    .await?;
                hook.last_state = Some(s);
            }
        }
        Ok(())
    }

    /// Best-effort terminalization after a store error killed the scheduler
    /// loop: without this the row can sit in `running`/`scheduled` with no
    /// live task behind it. Every step here is itself best-effort (the store
    /// may still be failing) — log and move on rather than propagate.
    pub(crate) async fn evict_hook_after_store_error(&self, hook: &mut Hook, cause: &Error) {
        let error = format!("scheduler stopped after a store error: {cause}");
        tracing::warn!(hook = %hook.hook_id.0, error = %error, "evicting hook after store error");
        if let Err(e) = self
            .store
            .update_hook_last_error(&hook.hook_id, Some(&error))
            .await
        {
            tracing::warn!(hook = %hook.hook_id.0, error = %e, "failed to persist hook lastError");
        }
        if let Err(e) = self
            .store
            .update_hook_state(&hook.hook_id, HookState::Evicted)
            .await
        {
            tracing::warn!(hook = %hook.hook_id.0, error = %e, "failed to persist hook evicted state");
        }
        hook.state = HookState::Evicted;
        hook.next_run_at = None;
        hook.last_error = Some(error.clone());
        self.emit_hook_event(HOOK_EVICTED, hook, None).await;
        let notice =
            crate::harness::latest().hook_evicted_internal_error_notice(&hook.name, &error);
        self.wake_hook_owner(hook, &notice, "evicted").await;
        // The last active hook settling can demote the derived displayStatus
        // (§6.5) and drop the `waiting` flag (§5.1) — best-effort like
        // everything else on this path.
        self.maybe_emit_display_status_changed(&hook.workspace_id)
            .await;
        self.maybe_emit_waiting_changed(&hook.workspace_id).await;
    }

    /// Expire an active hook whose TTL deadline has passed and no run is in
    /// flight: persist the terminal state, then emit + wake via
    /// [`Services::finish_expiry`]. A cancel can race the expiry timer, so
    /// the persisted state is re-read first and a no-longer-active hook is
    /// left untouched. Persistence is best-effort (mirrors
    /// `evict_hook_after_store_error`): the scheduler task is exiting either
    /// way, so log rather than propagate.
    async fn expire_hook(&self, hook: &mut Hook) {
        match self.store.get_hook(&hook.hook_id).await {
            Ok(h) if matches!(h.state, HookState::Scheduled | HookState::Running) => {}
            Ok(_) | Err(Error::NotFound(_)) => return,
            Err(e) => {
                tracing::warn!(hook = %hook.hook_id.0, error = %e, "expiry state re-read failed");
            }
        }
        if let Err(e) = self.store.expire_hook(&hook.hook_id).await {
            tracing::warn!(hook = %hook.hook_id.0, error = %e, "failed to persist hook expiry");
        }
        hook.state = HookState::Expired;
        hook.next_run_at = None;
        self.finish_expiry(hook).await;
    }

    /// Terminal-expiry tail shared by every expiry path (the caller has
    /// already persisted `state = expired`): emit `hook:expired` (payload
    /// shape parity with `hook:cancelled`) and wake the owner so the model
    /// can consciously reschedule. The wake names the hook, its run count,
    /// and the reschedule option, with `[hook logs]` per the existing wake
    /// conventions (`reason: "expired"` in messageMetadata). A perpetual
    /// hook may have fired repeatedly before expiring, so its notice reports
    /// runs AND dispatches instead of "without a dispatch".
    async fn finish_expiry(&self, hook: &Hook) {
        self.emit_hook_event(HOOK_EXPIRED, hook, None).await;
        let notice = crate::harness::latest().hook_expired_notice(
            &hook.name,
            &hook.hook_id.0,
            hook.perpetual,
            hook.run_count,
            hook.dispatch_count,
        );
        let notice = with_wake_logs(&notice, hook.last_logs.as_deref());
        self.wake_hook_owner(hook, &notice, "expired").await;
        // The last active hook settling can demote the derived displayStatus
        // (§6.5) and drop the `waiting` flag (§5.1) — best-effort,
        // transition-only emission.
        self.maybe_emit_display_status_changed(&hook.workspace_id)
            .await;
        self.maybe_emit_waiting_changed(&hook.workspace_id).await;
    }

    /// Terminal tail for a `runAt` fire that continued (no dispatch): the
    /// one-shot timer fired and is retired — same persisted terminal state
    /// (`expired`) and event/wake plumbing as [`Services::finish_expiry`],
    /// but the notice says the timer FIRED rather than that a TTL elapsed
    /// (the fire is the hook's purpose, not a watchdog giving up). The
    /// caller has already persisted `state = expired`.
    async fn finish_run_at_fire(&self, hook: &Hook) {
        self.emit_hook_event(HOOK_EXPIRED, hook, None).await;
        let notice = crate::harness::latest().hook_run_at_fired_notice(
            &hook.name,
            &hook.hook_id.0,
            hook.run_at.as_deref().unwrap_or_default(),
        );
        let notice = with_wake_logs(&notice, hook.last_logs.as_deref());
        self.wake_hook_owner(hook, &notice, "expired").await;
        // Same settlement recompute as the expiry tail (§6.5 / §5.1).
        self.maybe_emit_display_status_changed(&hook.workspace_id)
            .await;
        self.maybe_emit_waiting_changed(&hook.workspace_id).await;
    }

    /// Idle-visibility deferral backstop: after a hook reaches a terminal
    /// state, re-run the deferred-completion redelivery for the owner. A
    /// completion watch on an idle owner defers while it owns active hooks;
    /// the wake-carrying transitions (dispatch/eviction/expiry — and the FE
    /// cancel notice) resolve via the owner's wake turn ending, but a
    /// terminal transition whose wake was not delivered (owner-side cancel
    /// of the last hook, or a failed wake delivery) would otherwise strand
    /// the deferred watch forever. Routes through
    /// [`Services::redeliver_completion_after_queue_mutation`], whose guards
    /// (interim-skip marker recorded, queue empty, not busy, no REMAINING
    /// active hooks) make this a no-op in every other situation.
    async fn resettle_owner_after_hook_terminal(&self, hook: &Hook) {
        self.redeliver_completion_after_queue_mutation(&hook.agent_id)
            .await;
    }

    /// Wake the hook's owning agent via the automatic-delivery
    /// `agent.sendMessage` path (queue behind an in-flight turn, question
    /// hold respected). Best-effort: a delivery failure is logged, never
    /// propagated — the hook's own lifecycle transition already persisted.
    ///
    /// Every wake reason (`dispatched` / `evicted` / `expired` /
    /// `cancelled`) marks a terminal hook transition, so the deferral
    /// backstop runs after the delivery attempt: a FAILED wake on an idle
    /// owner whose last hook just terminated must still settle the owner's
    /// deferred completion watches (a successful wake makes the backstop a
    /// no-op — the queued/running wake turn owns the settlement).
    ///
    /// `dispatched` / `evicted` wakes additionally end with a state note
    /// (after any `[hook logs]` section): a one-shot dispatch and any
    /// eviction tell the owner the hook is retired and will not run again,
    /// with a reschedule pointer — the expiry notice states this explicitly
    /// in its own wording, and cancellation implies it. A re-armed PERPETUAL
    /// dispatch is the one non-terminal wake: its note says the hook remains
    /// active until `expiresAt`, with a `ws.hook.cancel` pointer.
    /// `dispatched` wakes also carry `hookStillActive` in the `hook_wake`
    /// messageMetadata (`true` only for the re-armed perpetual branch) so
    /// consumers can tell the two apart without parsing the note text.
    async fn wake_hook_owner(&self, hook: &Hook, message: &str, reason: &str) {
        // Only meaningful for `dispatched` wakes: a perpetual dispatch that
        // also lands at/after `expiresAt` is terminalized (Expired), not
        // re-armed — the caller sends a separate `finish_expiry` wake for
        // that, so this wake must NOT claim the hook remains active (it
        // would contradict the immediately-following expiry notice).
        let dispatch_still_active = hook.perpetual && hook.state != HookState::Expired;
        let mut metadata = json!({
            "type": "hook_wake",
            "hookId": hook.hook_id,
            "hookName": hook.name,
            "reason": reason,
        });
        if reason == "dispatched" {
            metadata["hookStillActive"] = json!(dispatch_still_active);
        }
        let harness = crate::harness::latest();
        let state_note = match reason {
            "dispatched" if dispatch_still_active => {
                Some(harness.hook_dispatch_active_note(hook.expires_at.as_deref()))
            }
            "dispatched" => Some(harness.hook_dispatch_retired_note(&hook.hook_id.0)),
            "evicted" => Some(harness.hook_evicted_state_note(&hook.hook_id.0)),
            _ => None,
        };
        let content = harness.hook_wake_framing(&hook.name, message, state_note.as_deref());
        if let Err(e) = self
            .deliver_wake_message(
                &hook.workspace_id,
                &hook.agent_id,
                &content,
                Some(&metadata),
            )
            .await
        {
            tracing::warn!(
                hook = %hook.hook_id.0,
                agent = %hook.agent_id.0,
                error = %e,
                "hook owner wake delivery failed"
            );
        }
        self.resettle_owner_after_hook_terminal(hook).await;
    }

    /// Emit one `hook:*` lifecycle event with the canonical
    /// `{ workspaceId, agentId, hookId, name, nextRunAt?, state, perpetual,
    /// dispatchCount, lastError? }` payload — `perpetual`/`dispatchCount` are
    /// included for FE/inspection parity with `hook.list` (an event
    /// subscriber must be able to tell a non-terminal perpetual
    /// `hook:dispatched` from a terminal one-shot dispatch without a
    /// follow-up `hook.list` call).
    async fn emit_hook_event(&self, event_type: &str, hook: &Hook, next_run_at: Option<String>) {
        let mut data = json!({
            "workspaceId": hook.workspace_id,
            "agentId": hook.agent_id,
            "hookId": hook.hook_id,
            "name": hook.name,
            "state": hook.state,
            "perpetual": hook.perpetual,
            "dispatchCount": hook.dispatch_count,
        });
        if let Some(next) = next_run_at {
            data["nextRunAt"] = Value::String(next);
        }
        if let Some(err) = &hook.last_error {
            if event_type == HOOK_EVICTED {
                data["lastError"] = Value::String(err.clone());
            }
        }
        let event = NewEvent {
            workspace_id: hook.workspace_id.clone(),
            timestamp: now_iso(),
            event_type: event_type.to_string(),
            actor: system_actor(),
            session_id: Some(hook.agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        publish_event(self.event_bus.as_ref(), event).await;
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use intent_core::{
        AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
        WorkspaceStatus,
    };
    use intent_store::Store;

    use super::*;
    use crate::events::EventBus;

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-hook-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    fn workspace(id: &WorkspaceId) -> Workspace {
        let ts = now_iso();
        Workspace {
            id: id.clone(),
            title: "WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            setup_result: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            context_links: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    fn note(ws: &WorkspaceId, id: &str, content: &str) -> intent_core::Note {
        let ts = now_iso();
        intent_core::Note {
            id: intent_core::NoteId::from(id),
            workspace_id: ws.clone(),
            title: "Probe".to_string(),
            content: content.to_string(),
            content_type: intent_core::ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: intent_core::NoteVisibility::Workspace,
            metadata: intent_core::NoteMetadata::default(),
            created_at: ts.clone(),
            rev: 0,
            updated_at: ts,
        }
    }

    fn agent(ws: &WorkspaceId, id: &str) -> AgentSession {
        AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId::from(id),
            workspace_id: ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Owner".to_string(),
            name_explicitly_set: true,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Active,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
        }
    }

    /// Store + Services + workspace + owning agent, with an event bus wired
    /// so `hook:*` lifecycle events persist to the event log. The temp
    /// workspaces root keeps `ws.workspace.*` bindings hermetic (the
    /// cowSupported probe must never touch `~/intent/workspaces`).
    async fn setup() -> (TempDb, tempfile::TempDir, Services, WorkspaceId, AgentId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let owner = AgentId::from("agent-hooks");
        store
            .insert_agent_session(&agent(&ws, "agent-hooks"))
            .await
            .expect("agent");
        let bus = EventBus::new(store.clone());
        let root = tempfile::tempdir().expect("temp workspaces root");
        let services = Services::new(store)
            .with_event_bus(bus)
            .with_workspaces_root(root.path().to_path_buf());
        (tmp, root, services, ws, owner)
    }

    /// Fixed deadline for the polling helpers below. Generous on purpose:
    /// under a loaded host (e.g. a parallel `cargo build`) persistence can
    /// lag far behind the happy path, and a short deadline fails
    /// otherwise-passing tests (intent-hq/monorepo#1358).
    const POLL_DEADLINE: Duration = Duration::from_secs(10);

    /// Poll the persisted hook until `pred` holds or the timeout elapses.
    async fn wait_for_hook<F>(svc: &Services, id: &HookId, pred: F) -> Hook
    where
        F: Fn(&Hook) -> bool,
    {
        let deadline = std::time::Instant::now() + POLL_DEADLINE;
        loop {
            let hook = svc.store().get_hook(id).await.expect("get hook");
            if pred(&hook) {
                return hook;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for hook state; last = {:?}",
                hook.state
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Poll until the hook's scheduler task deregisters itself — the
    /// terminal state persists BEFORE the loop exits, so asserting
    /// `!hook_task_alive` right after a state wait races the teardown.
    async fn wait_for_task_exit(svc: &Services, id: &HookId) {
        let deadline = std::time::Instant::now() + POLL_DEADLINE;
        while svc.hook_task_alive(id) {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for the scheduler task to exit"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Poll the owner's persisted session until some message contains
    /// `needle`, returning the serialized messages. The wake lands after the
    /// terminal state persists, so a plain read can race it.
    async fn wait_for_wake(svc: &Services, owner: &AgentId, needle: &str) -> String {
        let deadline = std::time::Instant::now() + POLL_DEADLINE;
        loop {
            let session = svc.store().get_agent_session(owner).await.unwrap();
            let text = serde_json::to_string(&session.messages).unwrap();
            if text.contains(needle) {
                return text;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "wake never contained {needle:?}; last = {text}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Persisted `hook:*` event types for a workspace, oldest-first. Event
    /// persistence is asynchronous relative to the hook-state writes tests
    /// gate on, so callers pass the types they positively assert and the
    /// helper polls until all are present (on deadline it returns whatever
    /// was seen — the caller's asserts then report the shortfall).
    async fn hook_event_types(svc: &Services, ws: &WorkspaceId, expected: &[&str]) -> Vec<String> {
        let deadline = std::time::Instant::now() + POLL_DEADLINE;
        loop {
            let mut evs = svc
                .store()
                .query_events(&intent_store::EventQuery {
                    workspace_id: Some(ws.clone()),
                    ..Default::default()
                })
                .await
                .expect("query events");
            evs.retain(|e| e.event_type.starts_with("hook:"));
            evs.reverse();
            let types: Vec<String> = evs.into_iter().map(|e| e.event_type).collect();
            let all_present = expected.iter().all(|t| types.iter().any(|ty| ty == t));
            if all_present || std::time::Instant::now() >= deadline {
                return types;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn schedule_validates_name_delay_and_code() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Name too long (51 chars > 50 cap).
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "a".repeat(51), "code": "return;", "delayMs": 10_000 }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("at most 50"), "{err}");
        // Delay below the floor.
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "ok", "code": "return;", "delayMs": 9_999 }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("at least 10000"), "{err}");
        // Missing code.
        let err = svc
            .hook_schedule_op(&ws, &owner, &json!({ "name": "ok", "delayMs": 10_000 }))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("`code` is required"), "{err}");
        // Nothing persisted by any failed validation.
        let hooks = svc.store().list_hooks_by_agent(&owner).await.unwrap();
        assert!(hooks.is_empty());
    }

    /// `hook.schedule` accepts exactly one of `delayMs` | `cron` | `runAt`:
    /// zero kinds and every pairing are rejected, and nothing persists.
    #[tokio::test]
    async fn schedule_requires_exactly_one_schedule_kind() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // No kind at all.
        let err = svc
            .hook_schedule_op(&ws, &owner, &json!({ "name": "ok", "code": "return;" }))
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("exactly one of `delayMs`, `cron`, or `runAt`"),
            "{err}"
        );
        // Every pairing is mutually exclusive.
        let future = (OffsetDateTime::now_utc() + time::Duration::hours(1))
            .format(&Rfc3339)
            .unwrap();
        for params in [
            json!({ "name": "ok", "code": "return;", "delayMs": 10_000, "cron": "*/5 * * * *" }),
            json!({ "name": "ok", "code": "return;", "delayMs": 10_000, "runAt": future }),
            json!({ "name": "ok", "code": "return;", "cron": "*/5 * * * *", "runAt": future }),
        ] {
            let err = svc
                .hook_schedule_op(&ws, &owner, &params)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("mutually exclusive"), "{err}");
        }
        let hooks = svc.store().list_hooks_by_agent(&owner).await.unwrap();
        assert!(hooks.is_empty());
    }

    /// Cron-kind validation: garbage and six-field (seconds) expressions are
    /// rejected; a valid five-field expression schedules with `cron` set,
    /// `delay_ms` 0, a computed `nextRunAt`, and the 7-day default TTL.
    #[tokio::test]
    async fn schedule_validates_cron_expression() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        for bad in ["not a cron", "61 * * * *", "*/5 * * * * *", ""] {
            let err = svc
                .hook_schedule_op(
                    &ws,
                    &owner,
                    &json!({ "name": "bad-cron", "code": "return;", "cron": bad }),
                )
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("cron"),
                "expected cron error for {bad:?}, got: {err}"
            );
        }
        assert!(svc
            .store()
            .list_hooks_by_agent(&owner)
            .await
            .unwrap()
            .is_empty());

        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "cron-hook",
                    "code": "return { dispatch: false };",
                    "cron": "*/5 * * * *",
                }),
            )
            .await
            .expect("schedule cron hook");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.cron.as_deref(), Some("*/5 * * * *"));
        assert_eq!(hook.run_at, None);
        assert_eq!(hook.delay_ms, 0);
        assert_eq!(hook.state, HookState::Scheduled);
        let next = hook.next_run_at.as_deref().expect("nextRunAt computed");
        assert!(OffsetDateTime::parse(next, &Rfc3339).unwrap() > OffsetDateTime::now_utc());
        assert_eq!(ttl_of(&hook), MAX_CRON_HOOK_TTL_MS);
        // Round-trips through the store.
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.cron.as_deref(), Some("*/5 * * * *"));
        assert_eq!(stored.run_at, None);
    }

    /// runAt-kind validation: non-RFC3339 and past timestamps are rejected,
    /// as are `perpetual` and `ttlMs` combinations; a valid future timestamp
    /// schedules with `runAt` normalized to UTC and expiry = fire + 1h.
    #[tokio::test]
    async fn schedule_validates_run_at() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let future = (OffsetDateTime::now_utc() + time::Duration::hours(2))
            .format(&Rfc3339)
            .unwrap();
        // Not a timestamp.
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "bad", "code": "return;", "runAt": "tomorrow" }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("RFC3339"), "{err}");
        // In the past.
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "past", "code": "return;", "runAt": "2020-01-01T00:00:00Z" }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("must be in the future"), "{err}");
        // Date-range boundary: fire + grace would overflow OffsetDateTime —
        // a validation error, not a panic.
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "eon", "code": "return;", "runAt": "9999-12-31T23:59:59Z" }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("too far in the future"), "{err}");
        // A one-shot fire time contradicts `perpetual`.
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "perp", "code": "return;", "runAt": future, "perpetual": true }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("`perpetual`"), "{err}");
        // ...and implies its own TTL, so an explicit `ttlMs` is rejected.
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "ttl", "code": "return;", "runAt": future, "ttlMs": 60_000 }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("`ttlMs`"), "{err}");
        assert!(svc
            .store()
            .list_hooks_by_agent(&owner)
            .await
            .unwrap()
            .is_empty());

        // Valid: offset timestamps normalize to UTC; expiry = fire + grace.
        let offset_future = (OffsetDateTime::now_utc() + time::Duration::hours(2))
            .to_offset(time::UtcOffset::from_hms(2, 0, 0).unwrap())
            .format(&Rfc3339)
            .unwrap();
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "run-at-hook",
                    "code": "return { dispatch: false };",
                    "runAt": offset_future,
                }),
            )
            .await
            .expect("schedule runAt hook");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        let run_at = hook.run_at.as_deref().expect("runAt persisted");
        assert!(run_at.ends_with('Z'), "normalized to UTC: {run_at}");
        assert_eq!(hook.cron, None);
        assert_eq!(hook.delay_ms, 0);
        assert!(!hook.perpetual);
        assert_eq!(hook.next_run_at.as_deref(), Some(run_at));
        let fire = OffsetDateTime::parse(run_at, &Rfc3339).unwrap();
        let expires = OffsetDateTime::parse(hook.expires_at.as_deref().unwrap(), &Rfc3339).unwrap();
        assert_eq!(
            (expires - fire).whole_milliseconds(),
            i128::from(RUN_AT_GRACE_MS)
        );
    }

    /// Wire Hook JSON is additive: `cron` / `runAt` appear only when set.
    #[tokio::test]
    async fn hook_json_carries_schedule_kind_fields_only_when_set() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "legacy", "code": "return { dispatch: false };",
                         "delayMs": 10_000 }),
            )
            .await
            .expect("schedule delay hook");
        let obj = out["hook"].as_object().unwrap();
        assert!(!obj.contains_key("cron"), "cron absent for delayMs hooks");
        assert!(!obj.contains_key("runAt"), "runAt absent for delayMs hooks");

        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "cron", "code": "return { dispatch: false };",
                         "cron": "0 * * * *" }),
            )
            .await
            .expect("schedule cron hook");
        assert_eq!(out["hook"]["cron"], json!("0 * * * *"));
        assert!(!out["hook"].as_object().unwrap().contains_key("runAt"));
    }

    #[tokio::test]
    async fn schedule_accepts_fifty_char_name() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let name = "b".repeat(50);
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": name,
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule with 50-char name");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.name, name);
        assert_eq!(hook.state, HookState::Scheduled);
        // Round-trips through list untouched.
        let listed = svc.hook_list_op(&ws, Some(&owner)).await.unwrap();
        let hooks = listed["hooks"].as_array().unwrap();
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0]["name"], json!(name));
    }

    #[tokio::test]
    async fn schedule_persists_perpetual_flag_and_defaults() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "watcher",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                    "perpetual": true,
                }),
            )
            .await
            .expect("schedule perpetual");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert!(hook.perpetual);
        assert_eq!(hook.dispatch_count, 0);
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert!(stored.perpetual);
        // `hook.list` carries both fields (camelCase).
        let listed = svc.hook_list_op(&ws, Some(&owner)).await.unwrap();
        let hooks = listed["hooks"].as_array().unwrap();
        assert_eq!(hooks[0]["perpetual"], json!(true));
        assert_eq!(hooks[0]["dispatchCount"], json!(0));

        // Omitting `perpetual` keeps the one-shot default.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "one-shot",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule one-shot");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert!(!hook.perpetual);
        assert!(!svc.store().get_hook(&hook.hook_id).await.unwrap().perpetual);
    }

    #[tokio::test]
    async fn schedule_runs_immediately_and_registers_task() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "poller",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        assert_eq!(out["dispatched"], json!(false));
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.state, HookState::Scheduled);
        assert_eq!(hook.run_count, 1);
        assert!(hook.next_run_at.is_some(), "nextRunAt persisted");
        assert!(svc.hook_task_alive(&hook.hook_id), "task registered");
        // Immediate first run emitted run-completed + scheduled.
        let types = hook_event_types(&svc, &ws, &[HOOK_RUN_COMPLETED, HOOK_SCHEDULED]).await;
        assert!(types.contains(&HOOK_RUN_COMPLETED.to_string()), "{types:?}");
        assert!(types.contains(&HOOK_SCHEDULED.to_string()), "{types:?}");
        // list surfaces it.
        let listed = svc.hook_list_op(&ws, Some(&owner)).await.unwrap();
        assert_eq!(listed["hooks"].as_array().unwrap().len(), 1);
    }

    /// Idle-visibility gating: the `waitingOnHooks` stamp applied by every
    /// `agent:idle` emit site carries the owner's ACTIVE (scheduled/running)
    /// hooks only — light metadata, no code/logs — and is omitted entirely
    /// (never `[]`) when the agent owns no active hook.
    #[tokio::test]
    async fn annotate_waiting_on_hooks_stamps_only_when_active_hooks_exist() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // No hooks at all: nothing stamped.
        let mut data = json!({ "agentId": owner.0 });
        let stamped = svc.annotate_waiting_on_hooks(&owner, &mut data).await;
        assert!(stamped.is_empty());
        assert!(
            data.get("waitingOnHooks").is_none(),
            "field omitted when no active hooks: {data}"
        );

        // A terminal (dispatched) hook is not active: still nothing stamped.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "done-now",
                    "code": "return { dispatch: true, message: 'done' };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule dispatching hook");
        assert_eq!(out["dispatched"], json!(true));
        let mut data = json!({ "agentId": owner.0 });
        svc.annotate_waiting_on_hooks(&owner, &mut data).await;
        assert!(
            data.get("waitingOnHooks").is_none(),
            "terminal hooks never stamp: {data}"
        );

        // An active (scheduled) hook stamps the light entry.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "watcher",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule active hook");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        let mut data = json!({ "agentId": owner.0 });
        let stamped = svc.annotate_waiting_on_hooks(&owner, &mut data).await;
        assert_eq!(stamped.len(), 1);
        let entry = &data["waitingOnHooks"][0];
        assert_eq!(entry["hookId"], json!(hook.hook_id));
        assert_eq!(entry["name"], json!("watcher"));
        assert!(entry["nextRunAt"].is_string(), "{entry}");
        assert!(entry["expiresAt"].is_string(), "{entry}");
        // Payloads stay light: no code/lastState/logs.
        assert!(entry.get("code").is_none());
        assert!(entry.get("lastState").is_none());
        assert!(entry.get("lastLogs").is_none());
        // Another agent's idle is unaffected by this owner's hooks.
        let other = AgentId::from("agent-other");
        let mut data = json!({ "agentId": other.0 });
        svc.annotate_waiting_on_hooks(&other, &mut data).await;
        assert!(data.get("waitingOnHooks").is_none());
    }

    #[tokio::test]
    async fn immediate_dispatch_short_circuits_schedule() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "one-shot",
                    "code": "return { dispatch: true, message: 'done already' };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        assert_eq!(out["dispatched"], json!(true));
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.state, HookState::Dispatched);
        // A one-shot hook's sole fire must still be counted (monorepo review
        // nit): `dispatchCount` means "fires so far" for every hook, not
        // just perpetual ones.
        assert_eq!(hook.dispatch_count, 1);
        assert!(!svc.hook_task_alive(&hook.hook_id), "no task spawned");
        // Owner was woken with the dispatch message (store-only path — no
        // AgentManager attached in tests).
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        let last = session.messages.last().expect("wake message persisted");
        let text = serde_json::to_string(&last.content).unwrap();
        assert!(text.contains("done already"), "{text}");
        // The validation-run dispatch wake ends with the terminal note, and
        // its metadata marks the hook as no longer active.
        assert!(
            text.contains("now retired and will not run again"),
            "{text}"
        );
        assert!(text.contains("ws.hook.schedule"), "{text}");
        assert!(text.contains(r#""hookStillActive":false"#), "{text}");
        let types = hook_event_types(&svc, &ws, &[HOOK_DISPATCHED]).await;
        assert!(types.contains(&HOOK_DISPATCHED.to_string()), "{types:?}");
    }

    #[tokio::test]
    async fn perpetual_schedule_time_dispatch_persists_active_schedule() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "perpetual-now",
                    "code": "return { dispatch: true, message: 'fired at once' };",
                    "delayMs": 10_000,
                    "perpetual": true,
                }),
            )
            .await
            .expect("schedule perpetual dispatcher");
        // Unlike one-shot, a dispatching validation run still schedules.
        assert_eq!(out["dispatched"], json!(true));
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.state, HookState::Scheduled);
        assert!(hook.next_run_at.is_some(), "nextRunAt persisted");
        assert_eq!(hook.dispatch_count, 1);
        assert!(svc.hook_task_alive(&hook.hook_id), "task spawned");
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Scheduled);
        assert_eq!(stored.dispatch_count, 1);
        // The wake note says the hook stays active to TTL, and the metadata
        // marks it still active.
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        let last = session.messages.last().expect("wake message persisted");
        let text = serde_json::to_string(&last.content).unwrap();
        assert!(text.contains("fired at once"), "{text}");
        assert!(text.contains("remains active until"), "{text}");
        assert!(
            text.contains(hook.expires_at.as_deref().expect("expiresAt set")),
            "{text}"
        );
        assert!(text.contains("ws.hook.cancel"), "{text}");
        assert!(text.contains(r#""hookStillActive":true"#), "{text}");
        // Never the one-shot retirement wording.
        assert!(!text.contains("retired"), "{text}");
        let types = hook_event_types(&svc, &ws, &[HOOK_DISPATCHED, HOOK_SCHEDULED]).await;
        assert!(types.contains(&HOOK_DISPATCHED.to_string()), "{types:?}");
        assert!(types.contains(&HOOK_SCHEDULED.to_string()), "{types:?}");
    }

    /// Schedule-time counterpart to `perpetual_dispatch_at_expiry_wakes_then_expires`:
    /// a perpetual validation run can outlive a very short TTL, and a
    /// dispatch landing at/after `expiresAt` must expire the hook instead of
    /// persisting a re-armed active schedule — the dispatch still wins (the
    /// owner is woken), but the hook never gets a scheduler task and its own
    /// wake must not contradict the immediately-following expiry notice.
    #[tokio::test]
    async fn perpetual_schedule_time_dispatch_at_expiry_expires_instead_of_scheduling() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let skew = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let svc = svc.with_hook_clock_skew(skew.clone());
        // Skew "now" past the deadline before the validation run even
        // starts, so the dispatching first run is provably at/after expiry.
        skew.store(120_000, std::sync::atomic::Ordering::SeqCst);
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "perpetual-expiring",
                    "code": "return { dispatch: true, message: 'last gasp at schedule' };",
                    "delayMs": 10_000,
                    "ttlMs": 10_000,
                    "perpetual": true,
                }),
            )
            .await
            .expect("schedule perpetual dispatcher at expiry");
        // The dispatch still wins (owner woken, dispatch counted)...
        assert_eq!(out["dispatched"], json!(true));
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        // ...but the hook is terminalized, not re-armed.
        assert_eq!(hook.state, HookState::Expired);
        assert!(hook.next_run_at.is_none(), "no reschedule after expiry");
        assert_eq!(hook.dispatch_count, 1, "the fire still counted");
        assert!(!svc.hook_task_alive(&hook.hook_id), "no task spawned");
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Expired);
        // The dispatch wake must not contradict the expiry notice: the
        // terminalized fire uses the one-shot note and stillActive=false.
        let dispatch_wake = wait_for_wake(&svc, &owner, "last gasp at schedule").await;
        assert!(!dispatch_wake.contains("remains active"), "{dispatch_wake}");
        assert!(
            dispatch_wake.contains(r#""hookStillActive":false"#),
            "{dispatch_wake}"
        );
        let expiry_wake = wait_for_wake(&svc, &owner, "expired after reaching its TTL").await;
        assert!(expiry_wake.contains("1 run, 1 dispatch"), "{expiry_wake}");
        let types = hook_event_types(&svc, &ws, &[HOOK_DISPATCHED, HOOK_EXPIRED]).await;
        assert!(types.contains(&HOOK_DISPATCHED.to_string()), "{types:?}");
        assert!(types.contains(&HOOK_EXPIRED.to_string()), "{types:?}");
        assert!(!types.contains(&HOOK_SCHEDULED.to_string()), "{types:?}");
    }

    #[tokio::test]
    async fn perpetual_dispatch_reschedules_and_counts_each_fire() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // The validation run sees "wait" and continues; flipping the note to
        // "go" makes every subsequent `runNow` dispatch — a perpetual hook
        // must survive each one.
        let mut probe = note(&ws, "perp-note", "wait");
        svc.store().insert_note(&probe).await.unwrap();
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "perp-watch",
                    "code": "const n = await ws.note.read('perp-note'); \
                             if (n.content.includes('go')) { \
                               return { dispatch: true, message: 'still green' }; \
                             } \
                             return { dispatch: false };",
                    "delayMs": 10_000,
                    "perpetual": true,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.state, HookState::Scheduled);
        assert_eq!(hook.dispatch_count, 0);
        probe.content = "go".to_string();
        svc.store().update_note(&probe).await.unwrap();

        for fire in 1..=2 {
            svc.hook_run_now_op(&ws, &hook.hook_id)
                .await
                .expect("runNow");
            // Each fire counts, then returns the hook to an active,
            // rescheduled state with its task still alive.
            wait_for_hook(&svc, &hook.hook_id, |h| h.dispatch_count == fire).await;
            let h = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Scheduled).await;
            assert_eq!(h.run_count, fire + 1, "validation run plus {fire} fires");
            assert!(h.next_run_at.is_some(), "fresh nextRunAt after fire {fire}");
            assert!(svc.hook_task_alive(&hook.hook_id), "task alive after fire");
        }
        let text = wait_for_wake(&svc, &owner, "still green").await;
        assert!(text.contains("remains active until"), "{text}");
        assert!(text.contains(r#""hookStillActive":true"#), "{text}");
        assert!(!text.contains("retired"), "{text}");
    }

    #[tokio::test]
    async fn perpetual_dispatch_at_expiry_wakes_then_expires() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Same clock choreography as `in_flight_dispatch_at_expiry_still_wins`:
        // the dispatch wins (owner woken, dispatch counted), but a perpetual
        // fire at/after `expiresAt` expires instead of rescheduling.
        let skew = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let svc = svc.with_hook_clock_skew(skew.clone());
        let mut gate = note(&ws, "perp-exp-gate", "wait");
        svc.store().insert_note(&gate).await.unwrap();
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "perp-slow-dispatch".to_string(),
            code: "for (;;) { \
                     const n = await ws.note.read('perp-exp-gate'); \
                     if (n.content.includes('go')) { \
                       return { dispatch: true, message: 'last gasp' }; \
                     } \
                   }"
            .to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(60_000)),
            perpetual: true,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Running).await;
        skew.store(120_000, std::sync::atomic::Ordering::SeqCst);
        gate.content = "go".to_string();
        svc.store().update_note(&gate).await.unwrap();
        let expired = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Expired).await;
        assert_eq!(expired.run_count, 1);
        assert_eq!(expired.dispatch_count, 1, "the fire still counted");
        assert!(expired.next_run_at.is_none(), "no reschedule after expiry");
        // The dispatch wake landed, then the expiry notice. A perpetual fire
        // that lands at/after expiry is terminalized, not re-armed, so its
        // own wake must NOT claim "remains active" (that would contradict
        // the immediately-following expiry notice) and must carry
        // stillActive=false.
        let dispatch_wake = wait_for_wake(&svc, &owner, "last gasp").await;
        assert!(!dispatch_wake.contains("remains active"), "{dispatch_wake}");
        assert!(
            dispatch_wake.contains(r#""hookStillActive":false"#),
            "{dispatch_wake}"
        );
        assert!(
            !dispatch_wake.contains(r#""hookStillActive":true"#),
            "{dispatch_wake}"
        );
        let text = wait_for_wake(&svc, &owner, "expired after reaching its TTL").await;
        // Perpetual expiry reports runs AND dispatches.
        assert!(text.contains("1 run, 1 dispatch"), "{text}");
        assert!(!text.contains("without a dispatch"), "{text}");
        let types = hook_event_types(&svc, &ws, &[HOOK_DISPATCHED, HOOK_EXPIRED]).await;
        assert!(types.contains(&HOOK_DISPATCHED.to_string()), "{types:?}");
        assert!(types.contains(&HOOK_EXPIRED.to_string()), "{types:?}");
        // Task deregistered after the terminal outcome.
        let deadline = std::time::Instant::now() + POLL_DEADLINE;
        while svc.hook_task_alive(&hook.hook_id) {
            assert!(std::time::Instant::now() < deadline, "task not removed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn schedule_rejects_when_first_run_throws() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "broken",
                    "code": "throw new Error('boom');",
                    "delayMs": 10_000,
                }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("first run failed"), "{err}");
        assert!(err.to_string().contains("boom"), "{err}");
        let hooks = svc.store().list_hooks_by_agent(&owner).await.unwrap();
        assert!(hooks.is_empty(), "failed validation run persists nothing");
    }

    #[tokio::test]
    async fn per_agent_cap_is_enforced() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let svc = svc.with_hooks_max_per_agent(2);
        for i in 0..2 {
            svc.hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": format!("hook-{i}"),
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule under cap");
        }
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "one-too-many",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("max 2"), "{err}");
    }

    #[tokio::test]
    async fn run_now_dispatch_wakes_owner_and_terminates() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // The script polls a note through the real `ws.*` bindings: the first
        // (validation) run sees "wait" and continues; the test then flips the
        // note to "go" and triggers a `runNow`, exercising the periodic
        // re-run → dispatch contract without waiting out the 10s floor.
        let mut probe = note(&ws, "ci-note", "wait");
        svc.store().insert_note(&probe).await.unwrap();
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "ci-watch",
                    "code": "const n = await ws.note.read('ci-note'); \
                             if (n.content.includes('go')) { \
                               return { dispatch: true, message: 'CI is green' }; \
                             } \
                             return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.state, HookState::Scheduled);
        probe.content = "go".to_string();
        svc.store().update_note(&probe).await.unwrap();
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Dispatched).await;
        assert_eq!(hook.run_count, 2);
        assert!(hook.next_run_at.is_none());
        // The one-shot hook's sole fire must be counted (monorepo review
        // nit): `dispatchCount` means "fires so far" for every hook.
        assert_eq!(hook.dispatch_count, 1);
        let text = wait_for_wake(&svc, &owner, "CI is green").await;
        // The scheduler-run dispatch wake ends with the terminal note, and
        // its metadata marks the hook as no longer active.
        assert!(
            text.contains("now retired and will not run again"),
            "{text}"
        );
        assert!(text.contains(r#""hookStillActive":false"#), "{text}");
        // Task deregistered after the terminal outcome.
        let deadline = std::time::Instant::now() + POLL_DEADLINE;
        while svc.hook_task_alive(&hook.hook_id) {
            assert!(std::time::Instant::now() < deadline, "task not removed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn throwing_run_evicts_and_wakes_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Seed a scheduled row directly (the throwing script would fail the
        // schedule-time validation run) and rehydrate to spawn its task.
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "will-throw".to_string(),
            code: "throw new Error('kaput');".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(MAX_HOOK_TTL_MS)),
            perpetual: false,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Evicted).await;
        assert!(hook.last_error.as_deref().unwrap().contains("kaput"));
        let types = hook_event_types(&svc, &ws, &[HOOK_EVICTED]).await;
        assert!(types.contains(&HOOK_EVICTED.to_string()), "{types:?}");
        let text = wait_for_wake(&svc, &owner, "evicted").await;
        assert!(text.contains("kaput"), "{text}");
        // The eviction wake ends with the will-not-run-again note; only
        // dispatched wakes carry the hookStillActive metadata flag.
        assert!(text.contains("will not run again"), "{text}");
        assert!(text.contains("ws.hook.schedule"), "{text}");
        assert!(!text.contains("hookStillActive"), "{text}");
    }

    #[tokio::test]
    async fn timeout_run_evicts_and_wakes_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let svc = svc.with_hook_eval_timeout(Duration::from_millis(200));
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "spinner".to_string(),
            code: "for (;;) {}".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(MAX_HOOK_TTL_MS)),
            perpetual: false,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Evicted).await;
        assert!(
            hook.last_error.as_deref().unwrap().contains("timed out"),
            "{:?}",
            hook.last_error
        );
        // Timeout kills the eval before the console capture can return: the
        // logs from that run are lost and last_logs stays untouched.
        assert_eq!(hook.last_logs, None);
        let text = wait_for_wake(&svc, &owner, "evicted").await;
        // The eviction wake ends with the will-not-run-again note.
        assert!(text.contains("will not run again"), "{text}");
    }

    #[tokio::test]
    async fn cancel_stops_task_and_fe_cancel_wakes_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "cancel-me",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert!(svc.hook_task_alive(&hook.hook_id));
        // FE-initiated cancel (no agent caller) → owner woken.
        let cancelled = svc
            .hook_cancel_op(&ws, &hook.hook_id, None)
            .await
            .expect("cancel");
        assert_eq!(cancelled["hook"]["state"], json!("cancelled"));
        assert!(!svc.hook_task_alive(&hook.hook_id), "task aborted");
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Cancelled);
        assert!(stored.next_run_at.is_none());
        let types = hook_event_types(&svc, &ws, &[HOOK_CANCELLED]).await;
        assert!(types.contains(&HOOK_CANCELLED.to_string()), "{types:?}");
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        let text = serde_json::to_string(&session.messages).unwrap();
        assert!(text.contains("cancelled from the app"), "{text}");
        // Cancellation keeps its own wording — no dispatch/eviction terminal
        // note, no hookStillActive metadata flag.
        assert!(!text.contains("will not run again"), "{text}");
        assert!(!text.contains("retired"), "{text}");
        assert!(!text.contains("hookStillActive"), "{text}");
        // A second cancel fails: the hook is no longer active.
        let err = svc
            .hook_cancel_op(&ws, &hook.hook_id, Some(&owner))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not active"), "{err}");
    }

    #[tokio::test]
    async fn owner_cancel_does_not_wake_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "self-cancel",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        svc.hook_cancel_op(&ws, &hook.hook_id, Some(&owner))
            .await
            .expect("owner cancel");
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        assert!(
            session.messages.is_empty(),
            "owner-initiated cancel must not wake the owner"
        );
    }

    /// Regression (intent-hq/monorepo#1563): an agent cancelling a sibling
    /// agent's hook is rejected with an error naming the owner, and the hook
    /// stays active with its scheduler task alive — the accidental
    /// list+cancel-all cleanup idiom cannot kill another agent's watch.
    #[tokio::test]
    async fn cross_agent_cancel_is_rejected_and_leaves_hook_active() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let other = AgentId::from("agent-other");
        svc.store()
            .insert_agent_session(&agent(&ws, "agent-other"))
            .await
            .expect("other agent");
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "owned-watch",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();

        let err = svc
            .hook_cancel_op(&ws, &hook.hook_id, Some(&other))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("only cancel your own hooks"), "{msg}");
        assert!(msg.contains(&owner.0), "error names the owner: {msg}");

        // The hook survives untouched: still scheduled, task alive, no wake.
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Scheduled);
        assert!(svc.hook_task_alive(&hook.hook_id), "task still alive");
        let types = hook_event_types(&svc, &ws, &[]).await;
        assert!(
            !types.iter().any(|t| t == HOOK_CANCELLED),
            "no cancel event: {types:?}"
        );
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        assert!(session.messages.is_empty(), "owner not woken");

        // The owner can still cancel it.
        svc.hook_cancel_op(&ws, &hook.hook_id, Some(&owner))
            .await
            .expect("owner cancel");
    }

    /// `workspace.archive` cancels every ACTIVE hook in the workspace per
    /// the existing cancel semantics — state persisted to `cancelled`, task
    /// aborted, `hook:cancelled` emitted, owner told why — while terminal
    /// hooks are untouched.
    #[tokio::test]
    async fn archive_cancels_active_hooks_and_leaves_terminal_hooks_untouched() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // A terminal hook first: an immediate dispatch short-circuits the
        // schedule, leaving a `dispatched` row with no live task.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "already-done",
                    "code": "return { dispatch: true, message: 'done' };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule dispatched");
        let dispatched: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(dispatched.state, HookState::Dispatched);
        // And one active hook with a live scheduler task.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "watching",
                    "code": "return { dispatch: false };",
                    "delayMs": 600_000,
                }),
            )
            .await
            .expect("schedule active");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert!(svc.hook_task_alive(&hook.hook_id));

        let archived = svc
            .archive_workspace(ws.clone(), None)
            .await
            .expect("archive");
        assert!(archived.archived, "workspace archived");

        assert!(!svc.hook_task_alive(&hook.hook_id), "hook task aborted");
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Cancelled);
        assert!(stored.next_run_at.is_none());
        let types = hook_event_types(&svc, &ws, &[HOOK_CANCELLED]).await;
        assert!(types.contains(&HOOK_CANCELLED.to_string()), "{types:?}");
        // Terminal hooks are untouched by the sweep.
        let stored = svc.store().get_hook(&dispatched.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Dispatched);
        // The owner learns why its watch stopped (store-only wake here: no
        // manager attached, so nothing can spawn a turn).
        let text = wait_for_wake(&svc, &owner, "workspace was archived").await;
        assert!(text.contains("cancelled"), "{text}");
    }

    /// Retire sweep (`ws.agent.retire`): the retiring agent's ACTIVE hooks
    /// are cancelled through the shared transition — task aborted, row
    /// `cancelled`, `nextRunAt` cleared, `hook:cancelled` emitted — with NO
    /// wake notice (the owner retired itself and is inert). Terminal hooks
    /// and other agents' hooks are untouched, and `agent.restore` does NOT
    /// resurrect the cancelled hooks.
    #[tokio::test]
    async fn retire_cancels_active_hooks_without_waking_the_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // A terminal hook first: an immediate dispatch short-circuits the
        // schedule, leaving a `dispatched` row with no live task.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "already-done",
                    "code": "return { dispatch: true, message: 'done' };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule dispatched");
        let dispatched: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(dispatched.state, HookState::Dispatched);
        // An active hook with a live scheduler task.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "watching",
                    "code": "return { dispatch: false };",
                    "delayMs": 600_000,
                }),
            )
            .await
            .expect("schedule active");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert!(svc.hook_task_alive(&hook.hook_id));
        // A bystander agent's active hook must survive the sweep.
        let bystander = AgentId::from("agent-bystander");
        svc.store()
            .insert_agent_session(&agent(&ws, "agent-bystander"))
            .await
            .unwrap();
        let out = svc
            .hook_schedule_op(
                &ws,
                &bystander,
                &json!({
                    "name": "bystander",
                    "code": "return { dispatch: false };",
                    "delayMs": 600_000,
                }),
            )
            .await
            .expect("schedule bystander");
        let bystander_hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();

        let res = svc
            .agent_retire_op(owner.clone(), None, None)
            .await
            .expect("retire");
        assert_eq!(res["success"], json!(true));

        assert!(!svc.hook_task_alive(&hook.hook_id), "hook task aborted");
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Cancelled);
        assert!(stored.next_run_at.is_none());
        let types = hook_event_types(&svc, &ws, &[HOOK_CANCELLED]).await;
        assert!(types.contains(&HOOK_CANCELLED.to_string()), "{types:?}");
        // Terminal hooks and the bystander's hook are untouched.
        let stored = svc.store().get_hook(&dispatched.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Dispatched);
        let stored = svc.store().get_hook(&bystander_hook.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Scheduled);
        assert!(svc.hook_task_alive(&bystander_hook.hook_id));
        // NO wake notice from the sweep (contrast the archive sweep, which
        // does notify). The only message is the terminal hook's own earlier
        // dispatch wake.
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        let text = serde_json::to_string(&session.messages).unwrap();
        assert!(
            !text.contains("cancelled"),
            "no cancellation wake queued for the retired owner: {text}"
        );

        // Restore does NOT resurrect: the row stays cancelled and boot
        // rehydration resumes nothing new (the bystander's live task is
        // skipped by idempotence).
        svc.agent_restore_op(owner.clone(), None)
            .await
            .expect("restore");
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 0);
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Cancelled);
    }

    /// Restart backstop: boot rehydration prunes (cancels) active hook rows
    /// whose owner is soft-retired — a crash window could have missed the
    /// retire-time sweep ([`Services::cancel_agent_hooks`]).
    #[tokio::test]
    async fn rehydration_prunes_hooks_of_retired_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let row = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "stranded".to_string(),
            code: "return { dispatch: false };".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(MAX_HOOK_TTL_MS)),
            perpetual: false,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&row).await.unwrap();
        assert!(svc
            .store()
            .set_agent_session_retired_at(&ws, &owner, Some(&now_iso()), &now_iso())
            .await
            .unwrap());

        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 0);
        assert!(!svc.hook_task_alive(&row.hook_id));
        let pruned = svc.store().get_hook(&row.hook_id).await.unwrap();
        assert_eq!(pruned.state, HookState::Cancelled);
        assert!(pruned.next_run_at.is_none());
    }

    /// Persisted `workspace:updated` archive deltas for a workspace,
    /// oldest-first: only the events whose `changes` carry `archived: true`.
    /// Event persistence is asynchronous relative to the store writes tests
    /// gate on, so callers poll.
    async fn archive_delta_events(svc: &Services, ws: &WorkspaceId) -> Vec<Value> {
        let mut evs = svc
            .store()
            .query_events(&intent_store::EventQuery {
                workspace_id: Some(ws.clone()),
                event_types: vec![intent_core::events::WORKSPACE_UPDATED.to_string()],
                ..Default::default()
            })
            .await
            .expect("query workspace:updated events");
        evs.reverse();
        evs.into_iter()
            .map(|e| e.data)
            .filter(|d| d["changes"]["archived"] == json!(true))
            .collect()
    }

    /// Regression (intent-hq/monorepo#1577): a hook script calling
    /// `ws.workspace.archive()` is itself swept by the archive's own hook
    /// cancellation, so the post-persist tail — sweeps, derived fields, and
    /// the §6.5 `workspace:updated` emit — must survive the caller's
    /// cancellation. Contract pinned here: the workspace is archived, the
    /// archive delta is published exactly once, and BOTH the initiating hook
    /// and an unrelated bystander hook end `cancelled` (the initiator does
    /// not keep polling an archived workspace, and its `archive()` call never
    /// returns to the script).
    #[tokio::test]
    async fn hook_initiated_archive_publishes_the_archive_delta() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // An unrelated active hook: the sweep must still cancel it.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "bystander",
                    "code": "return { dispatch: false };",
                    "delayMs": 600_000,
                }),
            )
            .await
            .expect("schedule bystander");
        let bystander: Hook = serde_json::from_value(out["hook"].clone()).unwrap();

        // The archiving hook polls a note: the schedule-time validation run
        // sees "wait" and continues, then flipping the note and driving a
        // `runNow` makes the scheduled run archive the workspace.
        let mut probe = note(&ws, "archive-gate", "wait");
        svc.store().insert_note(&probe).await.unwrap();
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "archiver",
                    "code": "const n = await ws.note.read('archive-gate'); \
                             if (n.content.includes('go')) { \
                               await ws.workspace.archive(); \
                             } \
                             return { dispatch: false };",
                    "delayMs": 600_000,
                }),
            )
            .await
            .expect("schedule archiver");
        let archiver: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(archiver.state, HookState::Scheduled);
        probe.content = "go".to_string();
        svc.store().update_note(&probe).await.unwrap();
        svc.hook_run_now_op(&ws, &archiver.hook_id)
            .await
            .expect("runNow");

        // The archive lands in the store...
        let deadline = std::time::Instant::now() + POLL_DEADLINE;
        loop {
            let row = svc.store().get_workspace(&ws).await.expect("get workspace");
            if row.archived {
                assert_eq!(row.status, WorkspaceStatus::Archived);
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "hook-initiated archive never persisted"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // ...and the §6.5 delta is published exactly once, in full.
        let deadline = std::time::Instant::now() + POLL_DEADLINE;
        let deltas = loop {
            let deltas = archive_delta_events(&svc, &ws).await;
            if !deltas.is_empty() {
                break deltas;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "hook-initiated archive published no workspace:updated delta"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        assert_eq!(deltas.len(), 1, "exactly one archive delta: {deltas:?}");
        let changes = &deltas[0]["changes"];
        assert_eq!(changes["status"], json!("Archived"), "{changes}");
        assert!(
            changes["archivedAt"].is_string(),
            "delta carries archivedAt: {changes}"
        );

        // Both hooks end cancelled: the sweep terminalizes the bystander and
        // the initiator alike, so neither keeps polling an archived
        // workspace.
        for (label, id) in [
            ("bystander", &bystander.hook_id),
            ("archiver", &archiver.hook_id),
        ] {
            let hook = wait_for_hook(&svc, id, |h| h.state == HookState::Cancelled).await;
            assert!(hook.next_run_at.is_none(), "{label} has no nextRunAt");
            assert!(!svc.hook_task_alive(id), "{label} task aborted");
        }
    }

    /// `workspace.delete` aborts the workspace's live hook scheduler tasks
    /// EAGERLY — the task is gone the moment delete returns, not lazily at
    /// its next tick — and the store cascade drops the row.
    #[tokio::test]
    async fn delete_aborts_live_hook_tasks_eagerly() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "doomed",
                    "code": "return { dispatch: false };",
                    "delayMs": 600_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert!(svc.hook_task_alive(&hook.hook_id));

        svc.delete_workspace(ws.clone()).await.expect("delete");

        assert!(
            !svc.hook_task_alive(&hook.hook_id),
            "hook task aborted eagerly, not at its next tick"
        );
        let err = svc.store().get_hook(&hook.hook_id).await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err}");
    }

    /// Persisted `workspace:displayStatus-changed` payload statuses for a
    /// workspace, oldest-first.
    async fn display_status_events(svc: &Services, ws: &WorkspaceId) -> Vec<String> {
        let mut evs =
            svc.store()
                .query_events(&intent_store::EventQuery {
                    workspace_id: Some(ws.clone()),
                    event_types: vec![
                        intent_core::events::WORKSPACE_DISPLAY_STATUS_CHANGED.to_string()
                    ],
                    ..Default::default()
                })
                .await
                .expect("query displayStatus events");
        evs.reverse();
        evs.into_iter()
            .map(|e| e.data["displayStatus"].as_str().unwrap().to_string())
            .collect()
    }

    /// An active (`scheduled`) hook sets the orthogonal `waiting` flag on
    /// the list/get enrichment path without promoting the derived
    /// `displayStatus` — the base rollup (`idle` here) is served as-is; the
    /// flag drops once the hook settles.
    #[tokio::test]
    async fn active_hook_sets_waiting_without_promoting_display_status() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        assert!(!svc.workspace_has_active_hooks(&ws).await);
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "watcher",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert!(svc.workspace_has_active_hooks(&ws).await);
        let mut row = svc.store().get_workspace(&ws).await.unwrap();
        svc.enrich_workspace_aggregates(&mut row).await;
        assert!(
            row.waiting,
            "idle owner with an active hook must read waiting"
        );
        assert_eq!(
            row.display_status,
            Some(intent_core::WorkspaceDisplayStatus::Idle),
            "an active hook never promotes the displayStatus rollup"
        );

        // Settle the hook: the waiting flag lapses; the rollup is unchanged.
        svc.hook_cancel_op(&ws, &hook.hook_id, Some(&owner))
            .await
            .expect("cancel");
        assert!(!svc.workspace_has_active_hooks(&ws).await);
        let mut row = svc.store().get_workspace(&ws).await.unwrap();
        svc.enrich_workspace_aggregates(&mut row).await;
        assert!(!row.waiting, "terminal hooks never read waiting");
        assert_eq!(
            row.display_status,
            Some(intent_core::WorkspaceDisplayStatus::Idle),
        );
    }

    /// `needs_attention` (step 0) coexists with the hook wait signal: a
    /// top-level agent with a pending attention request serves
    /// `needs_attention` while the active hook sets `waiting`.
    #[tokio::test]
    async fn needs_attention_coexists_with_hook_waiting() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        svc.hook_schedule_op(
            &ws,
            &owner,
            &json!({
                "name": "watcher",
                "code": "return { dispatch: false };",
                "delayMs": 10_000,
            }),
        )
        .await
        .expect("schedule");
        svc.store()
            .set_attention_request(&ws, &owner, "discussion", "need input", &now_iso())
            .await
            .unwrap();
        let mut row = svc.store().get_workspace(&ws).await.unwrap();
        svc.enrich_workspace_aggregates(&mut row).await;
        assert_eq!(
            row.display_status,
            Some(intent_core::WorkspaceDisplayStatus::NeedsAttention),
        );
        assert!(row.waiting, "the wait flag is orthogonal to attention");
    }

    /// Hook lifecycle transitions no longer move the derived `displayStatus`
    /// (the wait signal is the orthogonal `waiting` flag): neither schedule,
    /// nor cancel, nor the spawned-task dispatch settle path emits
    /// `workspace:displayStatus-changed`.
    #[tokio::test]
    async fn hook_transitions_never_emit_display_status_changed() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Seed the last-observed baseline (a seed never emits).
        svc.maybe_emit_display_status_changed(&ws).await;
        assert_eq!(display_status_events(&svc, &ws).await, Vec::<String>::new());

        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "watcher",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(display_status_events(&svc, &ws).await, Vec::<String>::new());

        // Re-running the recompute without a transition emits nothing.
        svc.maybe_emit_display_status_changed(&ws).await;
        assert_eq!(display_status_events(&svc, &ws).await, Vec::<String>::new());

        svc.hook_cancel_op(&ws, &hook.hook_id, Some(&owner))
            .await
            .expect("cancel");
        assert_eq!(display_status_events(&svc, &ws).await, Vec::<String>::new());

        // The spawned-task dispatch settle path is silent too. The script
        // polls a note: schedule-time validation sees "wait" and continues;
        // the test flips it to "go" and `runNow` drives the dispatch.
        let mut probe = note(&ws, "gate-note", "wait");
        svc.store().insert_note(&probe).await.unwrap();
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "gate-watch",
                    "code": "const n = await ws.note.read('gate-note'); \
                             if (n.content.includes('go')) { \
                               return { dispatch: true, message: 'done' }; \
                             } \
                             return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        probe.content = "go".to_string();
        svc.store().update_note(&probe).await.unwrap();
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Dispatched).await;
        // The settled dispatch drops the waiting flag without any
        // displayStatus transition.
        let deadline = std::time::Instant::now() + POLL_DEADLINE;
        loop {
            if !svc.workspace_is_waiting(&ws).await {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "dispatch settle never dropped the waiting flag"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(display_status_events(&svc, &ws).await, Vec::<String>::new());
    }

    /// Persisted `workspace:waiting-changed` payload flags for a workspace,
    /// oldest-first.
    async fn waiting_events(svc: &Services, ws: &WorkspaceId) -> Vec<bool> {
        let mut evs = svc
            .store()
            .query_events(&intent_store::EventQuery {
                workspace_id: Some(ws.clone()),
                event_types: vec![intent_core::events::WORKSPACE_WAITING_CHANGED.to_string()],
                ..Default::default()
            })
            .await
            .expect("query waiting events");
        evs.reverse();
        evs.into_iter()
            .map(|e| e.data["waiting"].as_bool().unwrap())
            .collect()
    }

    /// Hook lifecycle transitions emit `workspace:waiting-changed` exactly
    /// once per actual transition with the self-sufficient
    /// `{ workspaceId, waiting }` payload: schedule raises the flag, a no-op
    /// recompute stays silent, and cancel drops it.
    #[tokio::test]
    async fn hook_transitions_emit_waiting_changed_on_transition_only() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Seed the last-observed baseline (a seed never emits).
        svc.maybe_emit_waiting_changed(&ws).await;
        assert_eq!(waiting_events(&svc, &ws).await, Vec::<bool>::new());

        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "watcher",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(waiting_events(&svc, &ws).await, vec![true]);
        // The payload is self-sufficient: it names the workspace too.
        let evs = svc
            .store()
            .query_events(&intent_store::EventQuery {
                workspace_id: Some(ws.clone()),
                event_types: vec![intent_core::events::WORKSPACE_WAITING_CHANGED.to_string()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(evs[0].data["workspaceId"], json!(ws.as_str()));

        // Re-running the recompute without a transition emits nothing.
        svc.maybe_emit_waiting_changed(&ws).await;
        assert_eq!(waiting_events(&svc, &ws).await, vec![true]);

        svc.hook_cancel_op(&ws, &hook.hook_id, Some(&owner))
            .await
            .expect("cancel");
        assert_eq!(waiting_events(&svc, &ws).await, vec![true, false]);
    }

    /// A hook expiring at its TTL drops the workspace's last wait signal, so
    /// the expiry path emits the `waiting: false` transition.
    #[tokio::test]
    async fn hook_expiry_emits_waiting_changed() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let skew = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let svc = svc.with_hook_clock_skew(skew.clone());
        svc.maybe_emit_waiting_changed(&ws).await;

        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "watcher",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                    "ttlMs": 60_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(waiting_events(&svc, &ws).await, vec![true]);

        // Push "now" past the deadline; the next run expires the hook.
        skew.store(120_000, std::sync::atomic::Ordering::SeqCst);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Expired).await;
        let deadline = std::time::Instant::now() + POLL_DEADLINE;
        loop {
            if waiting_events(&svc, &ws).await == vec![true, false] {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "expiry never emitted the waiting:false transition"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn rehydration_resumes_active_hooks_and_prunes_orphans() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let mk = |name: &str, state: HookState, agent: &AgentId| Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: agent.clone(),
            name: name.to_string(),
            code: "return { dispatch: false };".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 3,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(MAX_HOOK_TTL_MS)),
            perpetual: false,
            dispatch_count: 0,
        };
        let scheduled = mk("sched", HookState::Scheduled, &owner);
        let running = mk("mid-run", HookState::Running, &owner);
        let done = mk("done", HookState::Dispatched, &owner);
        // Owned by an agent whose session row is gone.
        let ghost_owner = AgentId::from("agent-ghost");
        svc.store()
            .insert_agent_session(&agent(&ws, "agent-ghost"))
            .await
            .unwrap();
        let orphan = mk("orphan", HookState::Scheduled, &ghost_owner);
        for h in [&scheduled, &running, &done, &orphan] {
            svc.store().insert_hook(h).await.unwrap();
        }
        svc.store()
            .set_agent_session_status(
                &ws,
                &ghost_owner,
                AgentStatus::Deleted,
                false,
                &now_iso(),
                None,
            )
            .await
            .unwrap();

        let resumed = svc.rehydrate_hooks().await.expect("rehydrate");
        assert_eq!(resumed, 2, "scheduled + running resume; others do not");
        assert!(svc.hook_task_alive(&scheduled.hook_id));
        assert!(svc.hook_task_alive(&running.hook_id));
        assert!(!svc.hook_task_alive(&done.hook_id));
        assert!(!svc.hook_task_alive(&orphan.hook_id));
        // The mid-run row was healed back to scheduled with a fresh countdown.
        let healed = svc.store().get_hook(&running.hook_id).await.unwrap();
        assert_eq!(healed.state, HookState::Scheduled);
        assert!(healed.next_run_at.is_some());
        // The orphan was cancelled.
        let orphaned = svc.store().get_hook(&orphan.hook_id).await.unwrap();
        assert_eq!(orphaned.state, HookState::Cancelled);
        // Idempotent: a second pass resumes nothing new.
        assert_eq!(svc.rehydrate_hooks().await.expect("second pass"), 0);
    }

    /// Row template for the rehydration-countdown tests below: a 10-minute
    /// cadence hook whose persisted `next_run_at`/`expires_at` the caller
    /// controls (intent-hq/monorepo#2856).
    fn countdown_row(
        ws: &WorkspaceId,
        owner: &AgentId,
        name: &str,
        next_run_at: Option<String>,
        expires_at: String,
    ) -> Hook {
        Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: name.to_string(),
            code: "return { dispatch: false };".to_string(),
            delay_ms: 600_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at,
            run_count: 1,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(expires_at),
            perpetual: false,
            dispatch_count: 0,
        }
    }

    /// Deterministic pin for the verbatim-preservation contract: when the
    /// persisted deadline is earlier than a fresh `now + delayMs` countdown,
    /// [`resumed_next_run`] returns the persisted string VERBATIM (no
    /// reparse/reformat drift) with the remaining time to it. The
    /// integration test below can only observe this best-effort — under
    /// load the due run may fire before the row is read
    /// (intent-hq/monorepo#3055).
    #[test]
    fn resumed_next_run_preserves_earlier_deadline_verbatim() {
        let (ws, owner) = (WorkspaceId::new(), AgentId::from("agent-hooks"));
        // 60s out: comfortably earlier than the fresh 10-minute cadence, and
        // wide enough that the remaining-delay bounds below hold under any
        // realistic scheduler stall between building the row and the call.
        let due_at = next_run_at_iso(60_000);
        let hook = countdown_row(
            &ws,
            &owner,
            "verbatim",
            Some(due_at.clone()),
            next_run_at_iso(MAX_HOOK_TTL_MS),
        );
        let (next_run_at, initial_delay) = resumed_next_run(&hook);
        assert_eq!(next_run_at, due_at, "persisted deadline kept verbatim");
        assert!(
            initial_delay > Duration::from_secs(30) && initial_delay <= Duration::from_secs(60),
            "remaining time to the persisted deadline — neither an immediate \
             run nor a fresh cadence: {initial_delay:?}"
        );
    }

    /// Kind-aware rehydration ([`resumed_next_run`]): `runAt` rows resume
    /// to the ABSOLUTE one-shot deadline (verbatim, even when overdue or
    /// interrupted mid-run — a `delay_ms` of 0 must never leak in as a
    /// fresh countdown), and `cron` rows resume to the persisted absolute
    /// deadline, recomputing from the expression only when the deadline is
    /// absent/garbled or the row was interrupted mid-run.
    #[test]
    fn resumed_next_run_is_schedule_kind_aware() {
        let (ws, owner) = (WorkspaceId::new(), AgentId::from("agent-hooks"));
        let row = |cron: Option<&str>, run_at: Option<String>, next: Option<String>| {
            let mut h = countdown_row(&ws, &owner, "kind", next, next_run_at_iso(MAX_HOOK_TTL_MS));
            h.delay_ms = 0;
            h.cron = cron.map(str::to_string);
            h.run_at = run_at;
            h
        };

        // runAt, future: absolute deadline verbatim, positive remaining.
        let future = next_run_at_iso(60_000);
        let hook = row(None, Some(future.clone()), Some(future.clone()));
        let (next, delay) = resumed_next_run(&hook);
        assert_eq!(next, future, "one-shot deadline kept verbatim");
        assert!(delay > Duration::from_secs(30) && delay <= Duration::from_secs(60));

        // runAt, overdue (fire passed while the daemon was down, still
        // inside the grace window): resumes to the deadline with a prompt
        // run — not `now + 0`-style drift.
        let overdue = next_run_at_iso(-60_000);
        let hook = row(None, Some(overdue.clone()), Some(overdue.clone()));
        let (next, delay) = resumed_next_run(&hook);
        assert_eq!(next, overdue);
        assert_eq!(delay, Duration::ZERO, "overdue fire runs promptly");

        // runAt interrupted mid-run: the timer never completed a run, so
        // the deadline still stands (no fresh-cadence branch for one-shots).
        let mut hook = row(None, Some(overdue.clone()), Some(overdue.clone()));
        hook.state = HookState::Running;
        let (next, delay) = resumed_next_run(&hook);
        assert_eq!(next, overdue);
        assert_eq!(delay, Duration::ZERO);

        // cron, persisted deadline: resumes to it verbatim (overdue ⇒
        // prompt run), not a recompute that would silently skip the tick.
        let hook = row(Some("*/5 * * * *"), None, Some(overdue.clone()));
        let (next, delay) = resumed_next_run(&hook);
        assert_eq!(next, overdue, "persisted cron deadline kept verbatim");
        assert_eq!(delay, Duration::ZERO);

        // cron, no persisted deadline: recomputes from the expression —
        // strictly in the future, within the 5-minute cadence.
        let hook = row(Some("*/5 * * * *"), None, None);
        let (next, delay) = resumed_next_run(&hook);
        let parsed = OffsetDateTime::parse(&next, &Rfc3339).expect("recomputed deadline parses");
        assert!(parsed > OffsetDateTime::now_utc() - time::Duration::seconds(1));
        assert!(delay <= Duration::from_secs(300), "within the cadence");

        // cron interrupted mid-run: the persisted deadline belongs to the
        // run that already started — recompute instead of re-firing it.
        let mut hook = row(Some("*/5 * * * *"), None, Some(overdue.clone()));
        hook.state = HookState::Running;
        let (next, _) = resumed_next_run(&hook);
        assert_ne!(next, overdue, "interrupted tick's deadline dropped");
        let parsed = OffsetDateTime::parse(&next, &Rfc3339).expect("recomputed deadline parses");
        assert!(
            parsed > OffsetDateTime::now_utc(),
            "recomputed strictly in the future: {next}"
        );
    }

    /// A restart must not push a hook's countdown back: rehydration resumes
    /// with the EARLIER of the persisted `nextRunAt` and a fresh
    /// `now + delayMs` countdown (intent-hq/monorepo#2856), so a
    /// long-cadence hook nearing its run keeps that near deadline — and the
    /// run actually fires on it — instead of restarting a full period out.
    #[tokio::test]
    async fn rehydration_preserves_earlier_persisted_next_run_at() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // 10-minute cadence, but the persisted countdown is nearly done:
        // the run is due 1s out.
        let due_at = next_run_at_iso(1_000);
        let hook = countdown_row(
            &ws,
            &owner,
            "long-cadence",
            Some(due_at.clone()),
            next_run_at_iso(MAX_HOOK_TTL_MS),
        );
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        // The persisted countdown survives the restart verbatim... unless
        // the due run already fired under load (`update_hook_run` replaces
        // `next_run_at` atomically with the `run_count` bump), which itself
        // proves the near deadline was honored. The verbatim min semantics
        // are pinned by `resumed_next_run_preserves_earlier_deadline_verbatim`.
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        if stored.run_count == 1 {
            assert_eq!(stored.next_run_at.as_deref(), Some(due_at.as_str()));
        }
        // ...and the run fires on it, nowhere near the 10-minute cadence a
        // reset countdown would impose (the poll deadline is 10s).
        // `run_count` persists before the Running→Scheduled state write, so
        // wait on both — a `run_count`-only poll can legally observe the row
        // mid-run as `Running` (intent-hq/monorepo#3055).
        let ran = wait_for_hook(&svc, &hook.hook_id, |h| {
            h.run_count == 2 && h.state == HookState::Scheduled
        })
        .await;
        assert!(ran.next_run_at.is_some(), "rescheduled after the run");
    }

    /// A hook whose persisted `nextRunAt` passed while the daemon was down
    /// is overdue: it runs promptly after rehydration — not dropped, not
    /// pushed a full `delayMs` period out (intent-hq/monorepo#2856).
    #[tokio::test]
    async fn rehydration_runs_overdue_hook_promptly() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let hook = countdown_row(
            &ws,
            &owner,
            "overdue",
            Some(next_run_at_iso(-60_000)),
            next_run_at_iso(MAX_HOOK_TTL_MS),
        );
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        // Wait on state too — `run_count` persists before the
        // Running→Scheduled write (intent-hq/monorepo#3055).
        let ran = wait_for_hook(&svc, &hook.hook_id, |h| {
            h.run_count == 2 && h.state == HookState::Scheduled
        })
        .await;
        assert!(ran.next_run_at.is_some(), "rescheduled after the run");
    }

    /// A row persisted mid-run (`Running`) keeps the fresh-cadence recovery:
    /// its `nextRunAt` is the deadline of the run that already started, so
    /// min semantics would immediately re-execute a potentially
    /// non-idempotent interrupted run. It resumes `Scheduled` with a full
    /// `delayMs` countdown instead.
    #[tokio::test]
    async fn rehydration_gives_interrupted_running_row_fresh_countdown() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let mut hook = countdown_row(
            &ws,
            &owner,
            "mid-run",
            Some(next_run_at_iso(-60_000)),
            next_run_at_iso(MAX_HOOK_TTL_MS),
        );
        hook.state = HookState::Running;
        svc.store().insert_hook(&hook).await.unwrap();
        let before = OffsetDateTime::now_utc();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.state, HookState::Scheduled);
        let next = OffsetDateTime::parse(stored.next_run_at.as_deref().unwrap(), &Rfc3339)
            .expect("parse resumed nextRunAt");
        assert!(
            next >= before + time::Duration::milliseconds(600_000),
            "full countdown, not the interrupted run's overdue deadline"
        );
        assert_eq!(stored.run_count, 1, "no immediate re-execution");
    }

    /// Rows with no persisted `nextRunAt` fall back to the fresh
    /// `now + delayMs` countdown, and a persisted deadline LATER than the
    /// fresh countdown is tightened to it (min semantics: a restart never
    /// schedules a hook later than `now + delayMs` either).
    #[tokio::test]
    async fn rehydration_fresh_countdown_when_absent_or_later() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let far_out = next_run_at_iso(MAX_HOOK_TTL_MS);
        let absent = countdown_row(&ws, &owner, "absent", None, far_out.clone());
        let later = countdown_row(&ws, &owner, "later", Some(far_out.clone()), far_out.clone());
        let garbled = countdown_row(
            &ws,
            &owner,
            "garbled",
            Some("not-a-timestamp".into()),
            far_out,
        );
        svc.store().insert_hook(&absent).await.unwrap();
        svc.store().insert_hook(&later).await.unwrap();
        svc.store().insert_hook(&garbled).await.unwrap();
        let before = OffsetDateTime::now_utc();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 3);
        let after = OffsetDateTime::now_utc();
        for hook in [&absent, &later, &garbled] {
            let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
            let next = OffsetDateTime::parse(stored.next_run_at.as_deref().unwrap(), &Rfc3339)
                .expect("parse resumed nextRunAt");
            let delay = time::Duration::milliseconds(600_000);
            assert!(next >= before + delay, "{}: full countdown", hook.name);
            assert!(next <= after + delay, "{}: no later than fresh", hook.name);
        }
    }

    /// The expiry guard outranks an overdue countdown: a hook whose
    /// persisted `nextRunAt` AND `expiresAt` both passed while the daemon
    /// was down expires at boot without ever running.
    #[tokio::test]
    async fn rehydration_expires_overdue_hook_past_deadline() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let hook = countdown_row(
            &ws,
            &owner,
            "overdue-expired",
            Some(next_run_at_iso(-120_000)),
            next_run_at_iso(-60_000),
        );
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 0);
        let expired = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Expired).await;
        assert_eq!(expired.run_count, 1, "no run at/after expiry");
        assert!(expired.next_run_at.is_none());
        assert!(!svc.hook_task_alive(&hook.hook_id));
    }

    /// A cron hook's post-run reschedule recomputes the next fire from the
    /// EXPRESSION — after a `runNow` just like a natural tick — instead of
    /// ticking a fixed `delay_ms` cadence (which is 0 for cron rows and
    /// would busy-loop the scheduler).
    #[tokio::test]
    async fn cron_run_reschedules_from_expression() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "cron-tick",
                    "code": "return { dispatch: false };",
                    "cron": "* * * * *",
                }),
            )
            .await
            .expect("schedule cron hook");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.state, HookState::Scheduled);
        let before = OffsetDateTime::now_utc();
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        // Wait on state too — `run_count` persists before the
        // Running→Scheduled write (intent-hq/monorepo#3055).
        let ran = wait_for_hook(&svc, &hook.hook_id, |h| {
            h.run_count == 2 && h.state == HookState::Scheduled
        })
        .await;
        let next = OffsetDateTime::parse(ran.next_run_at.as_deref().unwrap(), &Rfc3339)
            .expect("parse recomputed nextRunAt");
        assert!(next > before, "strictly future: {next}");
        assert!(
            next <= before + time::Duration::seconds(121),
            "the every-minute expression's next occurrence, not a stale or \
             far-out deadline: {next}"
        );
        assert!(svc.hook_task_alive(&hook.hook_id), "task still alive");
    }

    /// `runNow` on a `runAt` hook fires the one-shot EARLY and retires it —
    /// the manual trigger is honored over the timestamp: the hook goes
    /// terminal with the FIRED notice and there is no re-arm back to the
    /// original fire time.
    #[tokio::test]
    async fn run_now_on_run_at_fires_early_and_retires() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "early-timer",
                    "code": "return { dispatch: false };",
                    "runAt": next_run_at_iso(3_600_000),
                }),
            )
            .await
            .expect("schedule runAt hook");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.state, HookState::Scheduled);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let fired = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Expired).await;
        assert_eq!(fired.run_count, 2, "validation run + the early fire");
        assert!(
            fired.next_run_at.is_none(),
            "no re-arm to the original fire time"
        );
        wait_for_task_exit(&svc, &hook.hook_id).await;
        let text = wait_for_wake(&svc, &owner, "fired and is now retired").await;
        assert!(!text.contains("expired after reaching its TTL"), "{text}");
    }

    /// A `runAt` fire that continues (no dispatch) is terminal: the one-shot
    /// timer retires as `expired` with the FIRED notice — not the TTL
    /// wording — and the task exits (no re-arm).
    #[tokio::test]
    async fn run_at_fire_without_dispatch_retires_hook() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Overdue-but-in-grace row (fire passed while the daemon was down):
        // rehydration resumes it to the absolute deadline and the run starts
        // promptly — the deterministic way to drive a one-shot fire without
        // waiting out a real future timestamp.
        let fire_at = next_run_at_iso(-5_000);
        let mut hook = countdown_row(
            &ws,
            &owner,
            "one-shot-timer",
            Some(fire_at.clone()),
            next_run_at_iso(RUN_AT_GRACE_MS - 5_000),
        );
        hook.delay_ms = 0;
        hook.run_at = Some(fire_at);
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        let fired = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Expired).await;
        assert_eq!(fired.run_count, 2, "the fire ran exactly once");
        assert!(fired.next_run_at.is_none(), "no re-arm after the fire");
        wait_for_task_exit(&svc, &hook.hook_id).await;
        let text = wait_for_wake(&svc, &owner, "fired and is now retired").await;
        assert!(!text.contains("expired after reaching its TTL"), "{text}");
        let types = hook_event_types(&svc, &ws, &[HOOK_RUN_COMPLETED, HOOK_EXPIRED]).await;
        assert!(types.contains(&HOOK_EXPIRED.to_string()), "{types:?}");
    }

    /// A `runAt` fire that dispatches takes the normal one-shot dispatch
    /// path: owner woken with the message + retired note, terminal
    /// `dispatched` state.
    #[tokio::test]
    async fn run_at_fire_with_dispatch_wakes_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let fire_at = next_run_at_iso(-5_000);
        let mut hook = countdown_row(
            &ws,
            &owner,
            "one-shot-dispatch",
            Some(fire_at.clone()),
            next_run_at_iso(RUN_AT_GRACE_MS - 5_000),
        );
        hook.delay_ms = 0;
        hook.run_at = Some(fire_at);
        hook.code = "return { dispatch: true, message: 'timer went off' };".to_string();
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        let fired = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Dispatched).await;
        assert_eq!(fired.dispatch_count, 1);
        assert!(fired.next_run_at.is_none());
        wait_for_task_exit(&svc, &hook.hook_id).await;
        let text = wait_for_wake(&svc, &owner, "timer went off").await;
        assert!(text.contains("now retired"), "{text}");
    }

    #[tokio::test]
    async fn hook_scripts_reach_ws_bindings() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // The script calls a real `ws.*` binding (workspace details) during
        // its immediate validation run, proving the prelude + host dispatch
        // are wired with the hook's workspace pinned (the same environment
        // that carries `ws.host.exec`).
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "ws-probe",
                    "code": "const d = await ws.workspace.details(); \
                             if (!d.id) throw new Error('no workspace'); \
                             return { dispatch: true, message: 'ws id ' + d.id };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        assert_eq!(out["dispatched"], json!(true));
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        let text = serde_json::to_string(&session.messages).unwrap();
        assert!(text.contains(&format!("ws id {}", ws.0)), "{text}");
    }

    #[tokio::test]
    async fn console_logs_persist_on_continue_and_dispatch() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Continue run (the schedule-time validation run) captures logs.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "logger",
                    "code": "console.log('checked', 3, 'PRs'); \
                             console.warn({ ok: true }); \
                             return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(
            hook.last_logs.as_deref(),
            Some("checked 3 PRs\n{\"ok\":true}")
        );
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.last_logs, hook.last_logs);
        // hook.list serializes lastLogs.
        let listed = svc.hook_list_op(&ws, Some(&owner)).await.unwrap();
        assert_eq!(
            listed["hooks"][0]["lastLogs"],
            json!("checked 3 PRs\n{\"ok\":true}")
        );

        // Dispatch run persists that run's logs and appends them to the wake.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "one-shot-log",
                    "code": "console.info('all green'); \
                             return { dispatch: true, message: 'done' };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.last_logs.as_deref(), Some("all green"));
        let text = wait_for_wake(&svc, &owner, "[hook logs]").await;
        assert!(text.contains("all green"), "{text}");
        // The terminal note is the final line — after the [hook logs] section.
        let logs_at = text.find("[hook logs]").expect("logs section");
        let note_at = text
            .find("now retired and will not run again")
            .expect("terminal note");
        assert!(note_at > logs_at, "{text}");
    }

    #[tokio::test]
    async fn no_logging_leaves_last_logs_null() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "silent",
                    "code": "return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.last_logs, None);
        let listed = svc.hook_list_op(&ws, Some(&owner)).await.unwrap();
        assert_eq!(listed["hooks"][0].get("lastLogs"), None);
        // No `[hook logs]` section on a log-free run's wake path either.
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        let text = serde_json::to_string(&session.messages).unwrap();
        assert!(!text.contains("[hook logs]"), "{text}");
    }

    #[tokio::test]
    async fn console_logs_persist_when_run_throws() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Seed a scheduled row directly (the throwing script would fail the
        // schedule-time validation run) and rehydrate to spawn its task.
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "log-then-throw".to_string(),
            code: "console.log('made it here'); throw new Error('kaput');".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(MAX_HOOK_TTL_MS)),
            perpetual: false,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Evicted).await;
        assert!(hook.last_error.as_deref().unwrap().contains("kaput"));
        assert_eq!(hook.last_logs.as_deref(), Some("made it here"));
        // The evict wake carries the logs section, with the terminal note
        // after it.
        let text = wait_for_wake(&svc, &owner, "[hook logs]").await;
        assert!(text.contains("made it here"), "{text}");
        let logs_at = text.find("[hook logs]").expect("logs section");
        let note_at = text.find("will not run again").expect("terminal note");
        assert!(note_at > logs_at, "{text}");
    }

    #[tokio::test]
    async fn console_buffer_truncates_from_the_head() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // 150 lines > the 100-line cap: the head is dropped and the marker
        // line prepended; the newest lines survive.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "chatty",
                    "code": "for (let i = 0; i < 150; i++) console.log('line', i); \
                             return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        let logs = hook.last_logs.expect("logs captured");
        assert!(
            logs.starts_with("[earlier log lines truncated]\n"),
            "{logs}"
        );
        assert!(!logs.contains("line 49\n"), "head dropped: {logs}");
        assert!(logs.contains("line 50"), "{logs}");
        assert!(logs.ends_with("line 149"), "{logs}");
        let lines = logs.lines().count();
        assert_eq!(lines, 101, "100 kept + marker, got {lines}");
    }

    #[test]
    fn wake_logs_section_is_head_truncated() {
        assert_eq!(with_wake_logs("msg", None), "msg");
        assert_eq!(with_wake_logs("msg", Some("")), "msg");
        assert_eq!(
            with_wake_logs("msg", Some("a\nb")),
            "msg\n\n[hook logs]\na\nb"
        );
        let long = "x".repeat(HOOK_WAKE_LOGS_CAP + 100);
        let out = with_wake_logs("msg", Some(&long));
        assert!(out.starts_with("msg\n\n[hook logs]\n[earlier log lines truncated]\n"));
        assert!(out.ends_with(&"x".repeat(HOOK_WAKE_LOGS_CAP)));
        assert_eq!(
            out.len(),
            "msg\n\n[hook logs]\n[earlier log lines truncated]\n".len() + HOOK_WAKE_LOGS_CAP
        );
    }

    #[tokio::test]
    async fn state_persists_from_validation_run_and_carries_into_next_run() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // The validation run sees `hookState === null`, persists a counter,
        // and each later run increments it — dispatching once it reaches 2.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "counter",
                    "code": "const n = (hookState === null) ? 0 : hookState.n; \
                             if (n >= 2) { \
                               return { dispatch: true, message: 'count ' + n }; \
                             } \
                             return { dispatch: false, state: { n: n + 1 } };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        // The validation (arming) run persisted its state.
        assert_eq!(hook.last_state.as_deref(), Some("{\"n\":1}"));
        // hook.list serializes lastState.
        let listed = svc.hook_list_op(&ws, Some(&owner)).await.unwrap();
        assert_eq!(listed["hooks"][0]["lastState"], json!("{\"n\":1}"));
        // Second run reads the injected state and advances it.
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| {
            h.last_state.as_deref() == Some("{\"n\":2}")
        })
        .await;
        // Third run dispatches with the carried-over count.
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow again");
        wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Dispatched).await;
        wait_for_wake(&svc, &owner, "count 2").await;
    }

    /// Regression for intent-hq/monorepo#3231 (functional half): a hook's
    /// `ws.host.exec` with no explicit `cwd` runs from the workspace root,
    /// not the daemon's own process cwd.
    #[cfg(unix)]
    #[tokio::test]
    async fn hook_host_exec_defaults_cwd_to_workspace_root() {
        let (_tmp, _root, svc, _ws, owner) = setup().await;
        // A second workspace WITH a filesystem root (the setup default has
        // none): the hook's exec must land there.
        let root = tempfile::tempdir().expect("workspace root");
        let ws = WorkspaceId::new();
        let mut row = workspace(&ws);
        row.worktree_path = Some(root.path().to_string_lossy().into_owned());
        svc.store().insert_workspace(&row).await.expect("ws");
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "cwd-probe",
                    "code": "const r = await ws.host.exec({ command: 'pwd' }); \
                             return { dispatch: true, message: 'pwd=' + r.stdout.trim() };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        assert_eq!(out["dispatched"], json!(true));
        let expected = root.path().to_string_lossy().into_owned();
        // macOS `/tmp` resolves through a `/private` symlink; accept either.
        let canonical = std::fs::canonicalize(root.path())
            .map_or_else(|_| expected.clone(), |p| p.to_string_lossy().into_owned());
        let session = svc.store().get_agent_session(&owner).await.unwrap();
        let text = serde_json::to_string(&session.messages).unwrap();
        assert!(
            text.contains(&format!("pwd={expected}")) || text.contains(&format!("pwd={canonical}")),
            "hook exec must run from the workspace root ({expected} or {canonical}): {text}"
        );
    }

    /// Regression for intent-hq/monorepo#3231 (observability half): a run
    /// whose `ws.host.exec` fails (nonzero exit) without the script throwing
    /// persists a failure summary to `lastError` on the still-active hook,
    /// and a later all-healthy run clears it.
    #[cfg(unix)]
    #[tokio::test]
    async fn host_exec_failure_persists_last_error_and_recovery_clears_it() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Note-driven command switch: `fail` → exit 3, anything else → ok.
        let mut probe = note(&ws, "exec-note", "fail");
        svc.store().insert_note(&probe).await.unwrap();
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "exec-failures",
                    "code": "const n = await ws.note.read('exec-note'); \
                             const arg = n.content.includes('fail') ? 'echo broken >&2; exit 3' : 'true'; \
                             await ws.host.exec({ command: 'sh', args: ['-c', arg] }); \
                             return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        // The validation run's failed exec landed in lastError, the hook is
        // still active, and hook.list serializes it.
        assert_eq!(hook.state, HookState::Scheduled);
        let err = hook.last_error.as_deref().expect("lastError persisted");
        assert!(err.contains("1 host exec call failed"), "{err}");
        assert!(err.contains("exit code 3"), "{err}");
        assert!(err.contains("broken"), "stderr snippet included: {err}");
        // lastError is workspace-visible: raw args stay out (command name +
        // arg count only), so a token passed as an argument never persists.
        assert!(err.contains("sh (2 args)"), "{err}");
        assert!(
            !err.contains("echo broken"),
            "raw args must not persist: {err}"
        );
        let listed = svc.hook_list_op(&ws, Some(&owner)).await.unwrap();
        assert_eq!(
            listed["hooks"][0]["lastError"].as_str(),
            hook.last_error.as_deref()
        );
        // A recovered run clears the warning.
        probe.content = "ok".to_string();
        svc.store().update_note(&probe).await.unwrap();
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| {
            h.run_count == 2 && h.last_error.is_none()
        })
        .await;
        assert_eq!(hook.last_error, None);
        assert_eq!(hook.state, HookState::Scheduled);
    }

    /// The summary names the uncapped failure total and flags the lines the
    /// per-run capture cap omitted, so >cap failures never read as exactly
    /// cap; an absent total (defensive) falls back to the captured count.
    #[test]
    fn exec_failure_summary_flags_over_cap_total() {
        let lines = json!(["a -> exit code 1", "b -> exit code 1"]);
        let out = parse_exec_failures(Some(&lines), Some(&json!(8))).unwrap();
        assert!(out.contains("8 host exec calls failed"), "{out}");
        assert!(out.contains("…and 6 more not shown"), "{out}");
        let out = parse_exec_failures(Some(&lines), None).unwrap();
        assert!(out.contains("2 host exec calls failed"), "{out}");
        assert!(!out.contains("more not shown"), "{out}");
    }

    #[tokio::test]
    async fn absent_state_keeps_and_null_state_clears() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Validation run sets state; the note-driven script then omits
        // `state` (keep) and finally returns `state: null` (clear).
        let mut probe = note(&ws, "state-note", "keep");
        svc.store().insert_note(&probe).await.unwrap();
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "keep-clear",
                    "code": "const n = await ws.note.read('state-note'); \
                             if (hookState === null) { \
                               return { dispatch: false, state: { armed: true } }; \
                             } \
                             if (n.content.includes('clear')) { \
                               return { dispatch: false, state: null }; \
                             } \
                             return { dispatch: false };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.last_state.as_deref(), Some("{\"armed\":true}"));
        // Omitted `state` keeps the previous value.
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| h.run_count == 2).await;
        assert_eq!(hook.last_state.as_deref(), Some("{\"armed\":true}"));
        // `state: null` clears it.
        probe.content = "clear".to_string();
        svc.store().update_note(&probe).await.unwrap();
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow again");
        // run_count persists before the state clear — wait on both.
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| {
            h.run_count == 3 && h.last_state.is_none()
        })
        .await;
        assert_eq!(hook.last_state, None);
    }

    #[tokio::test]
    async fn oversized_state_is_dropped_with_a_log_warning() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // First run persists a small state; the second returns one beyond
        // the 16 KiB cap — the previous state must survive and the run's
        // logs must carry the warning line.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({
                    "name": "too-big",
                    "code": "if (hookState === null) { \
                               return { dispatch: false, state: { small: true } }; \
                             } \
                             return { dispatch: false, state: { big: 'x'.repeat(20000) } };",
                    "delayMs": 10_000,
                }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(hook.last_state.as_deref(), Some("{\"small\":true}"));
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        // run_count persists before last_logs — wait for the warning too.
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| {
            h.run_count == 2
                && h.last_logs
                    .as_deref()
                    .is_some_and(|l| l.contains("[hook state dropped:"))
        })
        .await;
        assert_eq!(
            hook.last_state.as_deref(),
            Some("{\"small\":true}"),
            "previous state kept on overflow"
        );
        let logs = hook.last_logs.expect("warning logged");
        assert!(logs.contains("[hook state dropped:"), "{logs}");
    }

    #[tokio::test]
    async fn rehydrated_hook_injects_persisted_state_into_next_run() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Seed a scheduled row whose last_state was persisted by a previous
        // daemon lifetime, rehydrate to spawn its task, and prove the next
        // run sees the injected hookState by dispatching with its value.
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "rehydrated".to_string(),
            code: "return { dispatch: true, message: 'saw n=' + hookState.n };".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 1,
            last_error: None,
            last_logs: None,
            last_state: Some("{\"n\":7}".to_string()),
            expires_at: Some(next_run_at_iso(MAX_HOOK_TTL_MS)),
            perpetual: false,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        // A null/missing hookState would throw on `.n` and evict instead.
        wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Dispatched).await;
        wait_for_wake(&svc, &owner, "saw n=7").await;
    }

    /// Wire a settings registry with the given `agentFeatures.*` overrides
    /// applied, so gates under test read them via `effective_settings()`.
    fn features_registry(
        overrides: &[(&str, bool)],
    ) -> (tempfile::TempDir, Arc<crate::SettingsRegistry>) {
        let dir = tempfile::tempdir().expect("temp config dir");
        let registry = Arc::new(
            crate::SettingsRegistry::load(dir.path().join("config.toml")).expect("load registry"),
        );
        let changes: Vec<(String, Value)> = overrides
            .iter()
            .map(|(key, on)| (format!("agentFeatures.{key}"), json!(on)))
            .collect();
        registry.apply(&changes).expect("apply overrides");
        (dir, registry)
    }

    #[tokio::test]
    async fn schedule_rejected_when_background_hooks_disabled() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let (_cfg, registry) = features_registry(&[("backgroundHooks", false)]);
        let svc = svc.with_settings_registry(registry);
        let err = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "gated", "code": "return { dispatch: false };",
                         "delayMs": 10_000 }),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("disabled in settings (agentFeatures.backgroundHooks = false)"),
            "{err}"
        );
        // Rejected before the validation run: nothing persisted.
        assert!(svc.store().load_active_hooks().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rehydration_resumes_active_hooks_when_background_hooks_disabled() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let (_cfg, registry) = features_registry(&[("backgroundHooks", false)]);
        let svc = svc.with_settings_registry(registry);
        // An active row from a previous daemon lifetime: the toggle only
        // rejects NEW schedules, so boot must resume this hook and let it
        // run to its terminal state.
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "survivor".to_string(),
            code: "return { dispatch: true, message: 'ran while disabled' };".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 1,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(MAX_HOOK_TTL_MS)),
            perpetual: false,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow still drives an already-active hook");
        wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Dispatched).await;
        wait_for_wake(&svc, &owner, "ran while disabled").await;
    }

    #[tokio::test]
    async fn hook_runs_use_feature_gated_prelude_and_dispatch() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let (_cfg, registry) = features_registry(&[("hostExec", false)]);
        let svc = svc.with_settings_registry(registry);
        // The validation run executes with the gated environment: `ws.host`
        // is pruned from the prelude and a raw `host({...})` frame is denied
        // at dispatch. Both observations ride back on the dispatch message.
        let code = "let denied = '';\n\
                    try { await host({ method: 'host.exec', args: { command: 'echo' } }); }\n\
                    catch (e) { denied = String(e); }\n\
                    return { dispatch: true, message: typeof ws.host + '|' + denied };";
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "gated-env", "code": code, "delayMs": 10_000 }),
            )
            .await
            .expect("validation run dispatches");
        assert_eq!(out.get("dispatched"), Some(&json!(true)));
        let wake = wait_for_wake(&svc, &owner, "undefined|").await;
        assert!(
            wake.contains("disabled in settings (agentFeatures.hostExec = false)"),
            "{wake}"
        );
    }

    /// A hook owned by a sub-agent session (`is_background`, and equally a
    /// `parent_agent_id`) runs with the sub-agent question gate: the prelude
    /// omits `ws.app.question` and a raw `host({...})` frame is denied with
    /// the top-level-only redirect — never the settings-gate error. The
    /// top-level owner's hook keeps the binding installed.
    #[tokio::test]
    async fn sub_agent_owned_hook_runs_with_question_gate() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let mut bg = agent(&ws, "agent-bg-hooks");
        bg.is_background = true;
        svc.store().insert_agent_session(&bg).await.unwrap();
        let code = "let denied = '';\n\
                    try { await host({ method: 'app.question.ask', args: { question: { header: 'h', question: 'q', options: [{label:'a'},{label:'b'}] } } }); }\n\
                    catch (e) { denied = String(e); }\n\
                    return { dispatch: true, message: typeof (ws.app && ws.app.question) + '|' + denied };";
        let out = svc
            .hook_schedule_op(
                &ws,
                &bg.id,
                &json!({ "name": "sub-agent-gate", "code": code, "delayMs": 10_000 }),
            )
            .await
            .expect("validation run dispatches");
        assert_eq!(out.get("dispatched"), Some(&json!(true)));
        let wake = wait_for_wake(&svc, &bg.id, "undefined|").await;
        assert!(
            wake.contains("only available to top-level agents")
                && wake.contains("ws.agent.requestDiscussion"),
            "{wake}"
        );
        assert!(!wake.contains("disabled in settings"), "{wake}");

        // The top-level owner's hook keeps the question binding installed.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "top-level-env",
                         "code": "return { dispatch: true, message: 'q=' + typeof (ws.app && ws.app.question) };",
                         "delayMs": 10_000 }),
            )
            .await
            .expect("validation run dispatches");
        assert_eq!(out.get("dispatched"), Some(&json!(true)));
        wait_for_wake(&svc, &owner, "q=object").await;
    }

    #[test]
    fn ttl_clamp_defaults_and_bounds() {
        assert_eq!(clamp_ttl_ms(None), MAX_HOOK_TTL_MS);
        assert_eq!(clamp_ttl_ms(Some(100_000_000)), MAX_HOOK_TTL_MS);
        assert_eq!(clamp_ttl_ms(Some(1)), MIN_HOOK_DELAY_MS);
        assert_eq!(clamp_ttl_ms(Some(0)), MIN_HOOK_DELAY_MS);
        assert_eq!(clamp_ttl_ms(Some(-5)), MIN_HOOK_DELAY_MS);
        assert_eq!(clamp_ttl_ms(Some(300_000)), 300_000);
        assert_eq!(clamp_ttl_ms(Some(7_200_000)), 7_200_000);
        // Cron-kind clamp: same floor, 7-day default and cap.
        assert_eq!(clamp_cron_ttl_ms(None), MAX_CRON_HOOK_TTL_MS);
        assert_eq!(clamp_cron_ttl_ms(Some(i64::MAX)), MAX_CRON_HOOK_TTL_MS);
        assert_eq!(clamp_cron_ttl_ms(Some(0)), MIN_HOOK_DELAY_MS);
        // A value over the 24h delay cap but under the cron cap survives.
        assert_eq!(clamp_cron_ttl_ms(Some(2 * 86_400_000)), 2 * 86_400_000);
    }

    /// Milliseconds between a hook's `created_at` and `expires_at`.
    fn ttl_of(hook: &Hook) -> i64 {
        let created = OffsetDateTime::parse(&hook.created_at, &Rfc3339).expect("created_at");
        let expires =
            OffsetDateTime::parse(hook.expires_at.as_deref().expect("expires_at"), &Rfc3339)
                .expect("expires_at");
        i64::try_from((expires - created).whole_milliseconds()).expect("ttl fits in i64")
    }

    #[tokio::test]
    async fn schedule_persists_clamped_expires_at() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Omitted ttlMs → the 24-hour default.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "default-ttl", "code": "return { dispatch: false };",
                         "delayMs": 10_000 }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(ttl_of(&hook), MAX_HOOK_TTL_MS);
        // Oversized ttlMs → clamped to the cap; undersized → the 10s floor.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "big-ttl", "code": "return { dispatch: false };",
                         "delayMs": 10_000, "ttlMs": 100_000_000 }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(ttl_of(&hook), MAX_HOOK_TTL_MS);
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "small-ttl", "code": "return { dispatch: false };",
                         "delayMs": 10_000, "ttlMs": 1 }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(ttl_of(&hook), MIN_HOOK_DELAY_MS);
        // In-range ttlMs persists as-is; the row round-trips and hook.list
        // serializes expiresAt.
        let out = svc
            .hook_schedule_op(
                &ws,
                &owner,
                &json!({ "name": "mid-ttl", "code": "return { dispatch: false };",
                         "delayMs": 10_000, "ttlMs": 300_000 }),
            )
            .await
            .expect("schedule");
        let hook: Hook = serde_json::from_value(out["hook"].clone()).unwrap();
        assert_eq!(ttl_of(&hook), 300_000);
        let stored = svc.store().get_hook(&hook.hook_id).await.unwrap();
        assert_eq!(stored.expires_at, hook.expires_at);
        let listed = svc.hook_list_op(&ws, Some(&owner)).await.unwrap();
        let mid = listed["hooks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|h| h["name"] == json!("mid-ttl"))
            .expect("mid-ttl listed");
        assert_eq!(mid["expiresAt"], json!(hook.expires_at.unwrap()));
    }

    #[tokio::test]
    async fn hook_expires_during_sleep_and_wakes_owner() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Deadline 3s out (wide enough that a loaded host cannot burn
        // through it between insert and rehydrate — monorepo#1358), inter-run
        // delay 10 minutes: the scheduler's expiry arm must win the select
        // and expire the hook without another run.
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "short-ttl".to_string(),
            code: "return { dispatch: false };".to_string(),
            delay_ms: 600_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 2,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(3_000)),
            perpetual: false,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        let expired = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Expired).await;
        assert_eq!(expired.run_count, 2, "no run at/after expiry");
        assert!(expired.next_run_at.is_none());
        let types = hook_event_types(&svc, &ws, &[HOOK_EXPIRED]).await;
        assert!(types.contains(&HOOK_EXPIRED.to_string()), "{types:?}");
        assert!(!types.contains(&HOOK_RUN_STARTED.to_string()), "{types:?}");
        let text = wait_for_wake(&svc, &owner, "expired after reaching its TTL").await;
        assert!(text.contains("2 runs completed"), "{text}");
        assert!(text.contains("ws.hook.schedule"), "{text}");
        // Expiry keeps its own wording — no dispatch/eviction terminal note,
        // no hookStillActive metadata flag.
        assert!(!text.contains("will not run again"), "{text}");
        assert!(!text.contains("retired"), "{text}");
        assert!(!text.contains("hookStillActive"), "{text}");
        // Task deregistered after the terminal outcome.
        let deadline = std::time::Instant::now() + POLL_DEADLINE;
        while svc.hook_task_alive(&hook.hook_id) {
            assert!(std::time::Instant::now() < deadline, "task not removed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn run_now_after_expiry_is_rejected_and_no_run_starts() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Deterministic expiry (monorepo#1358): the deadline is far out and
        // the injected clock skew moves "now" past it while the scheduler
        // sleeps — no wall-clock race. Once expired, runNow must reject
        // (not active) and no run may ever have started.
        let skew = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let svc = svc.with_hook_clock_skew(skew.clone());
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "expired-runnow".to_string(),
            code: "return { dispatch: true, message: 'must never run' };".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(60_000)),
            perpetual: false,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        // Skewed past the deadline before rehydration: the boot pass expires
        // the hook (owner woken) without ever spawning its scheduler task.
        skew.store(120_000, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 0);
        let expired = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Expired).await;
        assert_eq!(expired.run_count, 0, "the dispatching script never ran");
        let err = svc.hook_run_now_op(&ws, &hook.hook_id).await.unwrap_err();
        assert!(err.to_string().contains("not active"), "{err}");
        let types = hook_event_types(&svc, &ws, &[HOOK_EXPIRED]).await;
        assert!(!types.contains(&HOOK_RUN_STARTED.to_string()), "{types:?}");
        assert!(!types.contains(&HOOK_DISPATCHED.to_string()), "{types:?}");
        assert!(types.contains(&HOOK_EXPIRED.to_string()), "{types:?}");
        let text = wait_for_wake(&svc, &owner, "expired after reaching its TTL").await;
        assert!(!text.contains("must never run"), "{text}");
    }

    #[tokio::test]
    async fn in_flight_run_at_expiry_completes_then_expires_on_continue() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Deterministic in-flight expiry (monorepo#1355): the deadline is far
        // out, the script gates on a note, and the test moves the expiry
        // clock past the deadline while the run is provably in flight —
        // no assertion races the wall clock. Order is enforced end-to-end:
        // the skew is stored before the note flips to "go", so the script's
        // continue always lands after the deadline "passed".
        let skew = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let svc = svc.with_hook_clock_skew(skew.clone());
        let mut gate = note(&ws, "exp-gate", "wait");
        svc.store().insert_note(&gate).await.unwrap();
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "slow-continue".to_string(),
            code: "for (;;) { \
                     const n = await ws.note.read('exp-gate'); \
                     if (n.content.includes('go')) { return { dispatch: false }; } \
                   }"
            .to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(60_000)),
            perpetual: false,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        // The run is in flight (state persists to `running` before the script
        // starts; the note gate keeps it there). Now pass the deadline, then
        // release the script — the continue lands strictly after expiry.
        wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Running).await;
        skew.store(120_000, std::sync::atomic::Ordering::SeqCst);
        gate.content = "go".to_string();
        svc.store().update_note(&gate).await.unwrap();
        let expired = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Expired).await;
        assert_eq!(expired.run_count, 1, "the in-flight run completed");
        assert!(expired.last_run_at.is_some());
        assert!(expired.next_run_at.is_none(), "no reschedule after expiry");
        let types = hook_event_types(
            &svc,
            &ws,
            &[HOOK_RUN_STARTED, HOOK_RUN_COMPLETED, HOOK_EXPIRED],
        )
        .await;
        assert!(types.contains(&HOOK_RUN_STARTED.to_string()), "{types:?}");
        assert!(types.contains(&HOOK_RUN_COMPLETED.to_string()), "{types:?}");
        assert!(types.contains(&HOOK_EXPIRED.to_string()), "{types:?}");
        wait_for_wake(&svc, &owner, "expired after reaching its TTL").await;
    }

    #[tokio::test]
    async fn in_flight_dispatch_at_expiry_still_wins() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        // Same clock choreography as the continue variant above, but the
        // released run returns a dispatch — which wins over the passed TTL.
        let skew = Arc::new(std::sync::atomic::AtomicI64::new(0));
        let svc = svc.with_hook_clock_skew(skew.clone());
        let mut gate = note(&ws, "disp-gate", "wait");
        svc.store().insert_note(&gate).await.unwrap();
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "slow-dispatch".to_string(),
            code: "for (;;) { \
                     const n = await ws.note.read('disp-gate'); \
                     if (n.content.includes('go')) { \
                       return { dispatch: true, message: 'made it in time' }; \
                     } \
                   }"
            .to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(60_000)),
            perpetual: false,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        assert_eq!(svc.rehydrate_hooks().await.unwrap(), 1);
        svc.hook_run_now_op(&ws, &hook.hook_id)
            .await
            .expect("runNow");
        // Running persisted ⇒ the pre-run expiry guard already passed. Pass
        // the deadline, then release the script — its dispatch lands
        // strictly after expiry and must still win.
        wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Running).await;
        skew.store(120_000, std::sync::atomic::Ordering::SeqCst);
        gate.content = "go".to_string();
        svc.store().update_note(&gate).await.unwrap();
        let hook = wait_for_hook(&svc, &hook.hook_id, |h| h.state == HookState::Dispatched).await;
        assert_eq!(hook.run_count, 1);
        wait_for_wake(&svc, &owner, "made it in time").await;
        let types = hook_event_types(&svc, &ws, &[HOOK_DISPATCHED]).await;
        assert!(types.contains(&HOOK_DISPATCHED.to_string()), "{types:?}");
        assert!(!types.contains(&HOOK_EXPIRED.to_string()), "{types:?}");
    }

    #[tokio::test]
    async fn rehydration_expires_past_deadline_and_keeps_original_expiry() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let mk = |name: &str, expires_at: String| Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: name.to_string(),
            code: "return { dispatch: false };".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 4,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(expires_at),
            perpetual: false,
            dispatch_count: 0,
        };
        // Expired while the daemon was down vs. still inside its TTL.
        let stale = mk("stale", next_run_at_iso(-60_000));
        let fresh = mk("fresh", next_run_at_iso(MAX_HOOK_TTL_MS));
        svc.store().insert_hook(&stale).await.unwrap();
        svc.store().insert_hook(&fresh).await.unwrap();

        let resumed = svc.rehydrate_hooks().await.expect("rehydrate");
        assert_eq!(resumed, 1, "only the unexpired hook resumes");
        assert!(!svc.hook_task_alive(&stale.hook_id));
        assert!(svc.hook_task_alive(&fresh.hook_id));
        let expired = svc.store().get_hook(&stale.hook_id).await.unwrap();
        assert_eq!(expired.state, HookState::Expired);
        assert!(expired.next_run_at.is_none());
        let types = hook_event_types(&svc, &ws, &[HOOK_EXPIRED]).await;
        assert!(types.contains(&HOOK_EXPIRED.to_string()), "{types:?}");
        // The boot expiry wakes the owner too.
        let text = wait_for_wake(&svc, &owner, "expired after reaching its TTL").await;
        assert!(text.contains("4 runs completed"), "{text}");
        // The resumed hook keeps its ORIGINAL expiresAt (TTL never resets).
        let kept = svc.store().get_hook(&fresh.hook_id).await.unwrap();
        assert_eq!(kept.expires_at, fresh.expires_at);
    }

    #[test]
    fn time_to_expiry_handles_missing_past_and_future() {
        assert_eq!(time_to_expiry(None, 0), None);
        assert!(!is_expired(None, 0), "legacy rows never expire");
        assert_eq!(
            time_to_expiry(Some("not-a-timestamp"), 0),
            Some(Duration::ZERO)
        );
        assert_eq!(
            time_to_expiry(Some(&next_run_at_iso(-5_000)), 0),
            Some(Duration::ZERO)
        );
        assert!(is_expired(Some(&next_run_at_iso(-5_000)), 0));
        let future = next_run_at_iso(30_000);
        let remaining = time_to_expiry(Some(&future), 0).expect("future deadline");
        assert!(remaining > Duration::from_secs(25), "{remaining:?}");
        assert!(!is_expired(Some(&future), 0));
        // A skew past the deadline expires it; one short of it does not.
        assert!(is_expired(Some(&future), 60_000));
        assert!(!is_expired(Some(&future), 5_000));
        assert!(!is_expired(None, 60_000), "skew never expires legacy rows");
    }

    #[test]
    fn parse_state_keeps_clears_sets_and_caps() {
        let mut logs = None;
        assert!(matches!(
            parse_state(&json!({ "dispatch": false }), &mut logs),
            StateUpdate::Keep
        ));
        assert!(matches!(
            parse_state(&json!({ "state": null }), &mut logs),
            StateUpdate::Clear
        ));
        match parse_state(&json!({ "state": { "a": 1 } }), &mut logs) {
            StateUpdate::Set(s) => assert_eq!(s, "{\"a\":1}"),
            _ => panic!("expected Set"),
        }
        assert_eq!(logs, None);
        // Oversized: kept, with the warning appended to existing logs.
        let mut logs = Some("prior line".to_string());
        let big = json!({ "state": "y".repeat(HOOK_STATE_MAX_BYTES + 1) });
        assert!(matches!(parse_state(&big, &mut logs), StateUpdate::Keep));
        let logs = logs.unwrap();
        assert!(
            logs.starts_with("prior line\n[hook state dropped:"),
            "{logs}"
        );
        // Appending the warning never grows the capture past the log cap:
        // the combined text is tail-truncated (oldest output dropped first).
        let mut logs = Some("x".repeat(HOOK_LOG_MAX_BYTES));
        assert!(matches!(parse_state(&big, &mut logs), StateUpdate::Keep));
        let logs = logs.unwrap();
        assert!(logs.len() <= HOOK_LOG_MAX_BYTES, "len = {}", logs.len());
        assert!(logs.contains("[hook state dropped:"), "{logs}");
    }

    /// `hook.get` returns the full row — including `code` — even for a hook
    /// in a TERMINAL state, so a retired hook's script can be recovered for
    /// re-arming.
    #[tokio::test]
    async fn hook_get_returns_full_row_for_terminal_hooks() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "retired".to_string(),
            code: "return { dispatch: true, message: 'done' };".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Dispatched,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 1,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: None,
            perpetual: false,
            dispatch_count: 1,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        let row = svc.hook_get_op(&ws, &hook.hook_id).await.unwrap();
        assert_eq!(row["hookId"], json!(hook.hook_id.0));
        assert_eq!(row["code"], json!(hook.code));
        assert_eq!(row["state"], json!("dispatched"));
    }

    #[tokio::test]
    async fn hook_get_missing_id_is_not_found() {
        let (_tmp, _root, svc, ws, _owner) = setup().await;
        let err = svc.hook_get_op(&ws, &HookId::new()).await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err:?}");
    }

    /// A hook belonging to another workspace reads as `NotFound` — hooks are
    /// never readable across workspaces — while its own workspace still
    /// resolves it.
    #[tokio::test]
    async fn hook_get_from_another_workspace_is_not_found() {
        let (_tmp, _root, svc, ws, owner) = setup().await;
        let hook = Hook {
            hook_id: HookId::new(),
            workspace_id: ws.clone(),
            agent_id: owner.clone(),
            name: "scoped".to_string(),
            code: "return { dispatch: false };".to_string(),
            delay_ms: 10_000,
            cron: None,
            run_at: None,
            state: HookState::Scheduled,
            created_at: now_iso(),
            last_run_at: None,
            next_run_at: None,
            run_count: 0,
            last_error: None,
            last_logs: None,
            last_state: None,
            expires_at: Some(next_run_at_iso(60_000)),
            perpetual: false,
            dispatch_count: 0,
        };
        svc.store().insert_hook(&hook).await.unwrap();
        let other = WorkspaceId::new();
        let err = svc.hook_get_op(&other, &hook.hook_id).await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{err:?}");
        let row = svc.hook_get_op(&ws, &hook.hook_id).await.unwrap();
        assert_eq!(row["code"], json!(hook.code));
    }
}
