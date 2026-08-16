//! Agent orchestration: multiplex many concurrent agents with a lifecycle /
//! concurrency [`ProcessRegistry`] and a concrete [`EventSink`] over the M2
//! event bus (§6.8).
//!
//! [`AgentManager`] owns one [`AgentHandle`] per [`AgentId`] (the spawned child,
//! its ACP [`Connection`], the streaming-notification receiver, and the
//! client-served request loop). Each connection carries its own JSON-RPC id
//! space + pending-request map (`intent-acp`), so response correlation is
//! per-connection and the manager keys everything by `AgentId` — the stable
//! analog of the TS registry's `pid`. The [`ProcessRegistry`] ports
//! `agent-process-registry` (acquire/register/markActive/markIdle/deregister +
//! a global concurrency cap with LRU idle eviction); full timer/memory-pressure
//! reaping is M5, exposed here as the [`AgentManager::reap_idle`] hook.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use intent_acp::handshake::try_bypass_permissions_mode;
use intent_acp::session::{ContentBlock, McpServer, SessionModeState, StopReason};
use intent_acp::{
    apply_baseline_env_to_stdio_servers, build_baseline_mcp_env_from_process, handshake,
    normalize_mcp_servers, normalize_spaced_bridge_command, serve_workspace_mcp_tcp,
    spawn_provider, to_acp_session_mcp_servers, to_auggie_mcp_config, to_opencode_mcp_config,
    ClientRequestHandler, Connection, ConnectionHooks, EnvMap, EventSink, FileService,
    IncomingNotification, IncomingRequest, McpBridge, NormalizedMcpServer, NormalizedMcpServers,
    PermissionOutcome, PermissionPolicy, PermissionRegistry, PermissionRequestData, SinkEvent,
    SpawnOptions, WorkspaceMcpServer,
};
use intent_core::events::AGENT_STATUS_CHANGED;
use intent_core::{
    now_iso, parse_iso, slug::is_workspace_slug, ActorType, AgentId, AgentSession, AgentStatus,
    BoxFuture, Error, EventActor, Result, UsageCost, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_providers::{InjectionMechanism, ProviderConfig};
use intent_store::{NewEvent, NewTrackedChange};
use serde_json::{json, Value};
use tokio::process::Child;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::agent_ops::{new_message_id, user_message_blocks, QueuedMessage, MAX_MESSAGE_ID_LEN};
use crate::agent_session::{agent_actor, InterruptReason, InterruptedBy, ThoughtLevelOption};
use crate::events::EventBus;
use crate::Services;

#[cfg(test)]
pub(crate) mod tests;

/// Capitalize the leading ASCII byte of `s` (leaves the rest of the string
/// untouched). Used to normalize OAuth `token_type` values into the
/// conventional `Bearer` header form when a bag stores the RFC 6749 lower-case
/// spelling.
fn title_case_ascii(s: &str) -> String {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut out = String::with_capacity(s.len());
    out.push(first.to_ascii_uppercase());
    out.push_str(chars.as_str());
    out
}

/// Deterministic system note appended to a STALE queued-message redrive (#576)
/// so a delegated child that already delivered its completion report does not
/// blindly re-report the same content (duplicate parent wake). Wording owned
/// by the harness (H6).
pub(crate) fn stale_redrive_note(report_timestamp: &str) -> String {
    crate::harness::latest().stale_redrive_note(report_timestamp)
}

/// Stable prefix of [`stale_redrive_note`], used to keep the annotation
/// idempotent when a stale entry is requeued and redriven again. Owned by
/// the harness (H6) alongside the note wording.
use crate::harness::v1::STALE_REDRIVE_NOTE_PREFIX;

/// Deterministic system note appended to a drained queue entry so the
/// target agent knows when the message entered the queue and how long it
/// waited before delivery. Messages delivered immediately (never queued)
/// are NOT annotated — the note is applied only on the queue-drain paths,
/// and only when the wait reached [`DEQUEUE_WAIT_ANNOTATION_MIN_MS`]
/// (monorepo#2353). Coexists with the #576 stale-redrive note (both may
/// appear). Wording owned by the harness (H6).
pub(crate) fn dequeue_wait_note(queued_at: &str, waited: &str) -> String {
    crate::harness::latest().dequeue_wait_note(queued_at, waited)
}

/// Stable prefix of [`dequeue_wait_note`], used to keep the annotation
/// idempotent when an already-annotated entry is requeued and drained again
/// (the original wait deliberately stays — a terminal-failure requeue keeps
/// its first-delivery numbers). Owned by the harness (H6) alongside the note
/// wording.
use crate::harness::v1::DEQUEUE_WAIT_NOTE_PREFIX;

/// Minimum wait (milliseconds) before a drained entry earns the dequeue-wait
/// annotation (monorepo#2353). Incidental queue hops — e.g. a question-wizard
/// answer converted into an enqueue + immediate drain by the #1791
/// FIFO-restore branch — deliver within moments, and a "waited 0s" note/chip
/// is pure noise; a sub-threshold wait is treated like an immediate delivery
/// (no [SYSTEM NOTE], no `queueInfo` stamp). Waits at/above the threshold are
/// annotated unchanged (PROTOCOL §5.5).
const DEQUEUE_WAIT_ANNOTATION_MIN_MS: i128 = 5_000;

/// [`DEQUEUE_WAIT_ANNOTATION_MIN_MS`] with an `INTENTD_DEQUEUE_WAIT_MIN_MS`
/// env override (whole milliseconds). Primarily for tests/CI — the e2e
/// suites park entries behind short (~2s) mock busy turns and assert the
/// annotation, so they lower the threshold instead of slowing every turn
/// past 5s. Mirrors the `INTENTD_*_RETRY_BACKOFF_MS` override pattern.
fn dequeue_wait_annotation_min_ms() -> i128 {
    std::env::var("INTENTD_DEQUEUE_WAIT_MIN_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(i128::from)
        .unwrap_or(DEQUEUE_WAIT_ANNOTATION_MIN_MS)
}

/// Human-readable wait for [`dequeue_wait_note`]: `Ns` under a minute, then
/// `Nm Ss`, then `Nh Mm`. Negative waits (clock skew) clamp to `0s`.
/// Wording owned by the harness (H6).
pub(crate) fn format_wait_duration(secs: i64) -> String {
    crate::harness::latest().wait_duration(secs)
}

/// Dequeue-wait annotation: appends [`dequeue_wait_note`] to a drained
/// entry's content so the target knows when it was enqueued and how long it
/// waited. Idempotent across requeues via the stable prefix check.
/// `persisted: true` entries (terminal-failure requeues whose transcript row
/// is already durable) are never rewritten — mirroring the stale-redrive
/// constraint — so those redrives skip the note entirely: the delivered
/// prompt stays byte-identical to the persisted row, at the cost of no wait
/// note for that entry. Fail open: an unparseable `queued_at` leaves the
/// content untouched. Threshold-gated (monorepo#2353): a wait below
/// [`DEQUEUE_WAIT_ANNOTATION_MIN_MS`] — including a negative wait from clock
/// skew — skips both the note and the stamp, treating the sub-threshold hop
/// like an immediate delivery.
///
/// Alongside the content note, the entry's `messageMetadata` is stamped with
/// structured queue info — `queueInfo: { queuedAt, waitedMs }` (PROTOCOL
/// §5.5) — so the persisted user row carries machine-readable enqueue time +
/// wait for clients, riding the same metadata plumbing as the A2A sender
/// attribution. Same guards as the note: an existing `queueInfo` is never
/// overwritten (first-delivery numbers stay across requeues), and the
/// persisted-entry / unparseable-`queued_at` / sub-threshold skips above
/// cover the stamp too.
fn annotate_dequeue_wait(msg: &mut QueuedMessage) {
    if msg.persisted || msg.content.contains(DEQUEUE_WAIT_NOTE_PREFIX) {
        return;
    }
    let Some(queued) = parse_iso(&msg.queued_at) else {
        tracing::warn!(
            queued_at = %msg.queued_at,
            "dequeue-wait annotation skipped: queued_at parse failed"
        );
        return;
    };
    let elapsed = time::OffsetDateTime::now_utc() - queued;
    if elapsed.whole_milliseconds() < dequeue_wait_annotation_min_ms() {
        return;
    }
    msg.content = format!(
        "{}\n\n{}",
        msg.content,
        dequeue_wait_note(
            &msg.queued_at,
            &format_wait_duration(elapsed.whole_seconds())
        )
    );
    // The threshold gate above guarantees a positive wait; the clamp stays as
    // a belt-and-braces guard for the u64 conversion.
    let waited_ms = u64::try_from(elapsed.whole_milliseconds().max(0)).unwrap_or(u64::MAX);
    let queue_info = json!({ "queuedAt": msg.queued_at, "waitedMs": waited_ms });
    match msg.message_metadata.as_mut() {
        None => msg.message_metadata = Some(json!({ "queueInfo": queue_info })),
        Some(Value::Object(map)) => {
            map.entry("queueInfo").or_insert(queue_info);
        }
        Some(_) => {
            tracing::warn!(
                id = %msg.id,
                queued_at = %msg.queued_at,
                "dequeue-wait queueInfo stamp skipped: messageMetadata is not an object"
            );
        }
    }
}

/// Delivery-time "tasks now unblocked" annotation (intent-hq/monorepo#2044):
/// completion wakes stamp only the triggering task ids on their
/// `messageMetadata` at enqueue time; THIS is where the unblocked enumeration
/// is resolved — against task state fetched fresh as the entries are rendered
/// for the model turn — so a wake that sat queued behind a busy parent never
/// carries a stale snapshot. All trigger-carrying entries draining in the
/// same batch coalesce into ONE delta computation and ONE appended section
/// (on the LAST such entry, so the section lands after every completion it
/// covers). Same guards as [`annotate_dequeue_wait`]: `persisted: true`
/// requeues are never rewritten, and a content that already carries the
/// section (terminal-failure requeue) is not annotated twice. Best-effort and
/// advisory only — an empty delta or a snapshot error appends nothing.
async fn annotate_unblocked_hints(
    services: &Services,
    agent_id: &AgentId,
    entries: &mut [QueuedMessage],
) {
    use crate::agent_ops::ready_delta;
    let candidates: Vec<usize> = entries
        .iter()
        .enumerate()
        .filter(|(_, m)| {
            !m.persisted
                && !m.content.contains(ready_delta::UNBLOCKED_SECTION_PREFIX)
                && ready_delta::metadata_has_triggers(m.message_metadata.as_ref())
        })
        .map(|(i, _)| i)
        .collect();
    let Some(&last) = candidates.last() else {
        return;
    };
    let section = services
        .unblocked_section_for_delivery(
            agent_id,
            candidates
                .iter()
                .map(|&i| entries[i].message_metadata.as_ref()),
        )
        .await;
    if let Some(section) = section {
        entries[last].content = format!("{}\n\n{}", entries[last].content, section);
    }
}

/// Combined provider prompt for a batch flush (`agents.flushQueuedMessages`):
/// a header naming the flushed count, then each entry's content under a
/// `Message #N:` label in delivery order. Entry contents already carry their
/// per-entry [`dequeue_wait_note`] (and any #576 stale-redrive note), so each
/// section retains its own queuedAt time and wait duration. Wire-only — the
/// transcript persists each entry as its own user row; this combined text is
/// never persisted.
fn flush_combined_prompt(entries: &[QueuedMessage]) -> String {
    use std::fmt::Write as _;
    let mut out = format!("{} queued messages while you were working", entries.len());
    for (i, m) in entries.iter().enumerate() {
        let _ = write!(out, "\n\nMessage #{}:\n{}", i + 1, m.content);
    }
    out
}

const GB: u64 = 1024 * 1024 * 1024;

/// Whether a `session/cancel` error means the child's transport is already
/// closed (writer task / pipe gone because the child died mid-turn). That is
/// the EXPECTED outcome of cancelling a dead turn — the interrupt path logs it
/// at DEBUG. Anything else (protocol error, malformed payload, timeout on a
/// live socket) is a real anomaly and stays at WARN.
fn is_cancel_transport_closed(e: &intent_acp::AcpError) -> bool {
    matches!(e, intent_acp::AcpError::Transport(_))
}

/// Settle a hung `session/prompt` after the idle timeout WITHOUT killing the
/// child: send `session/cancel` so the agent resolves/abandons the hung turn
/// server-side (its in-flight prompt already had its client-side pending-map
/// entry cleaned by the transport's drop guard when `prompt()` returned
/// early). The child + ACP session stay alive for a follow-up turn — the
/// keep-alive half of [`AgentManager::interrupt`], without the worker-abort /
/// stream-end machinery (the caller still owns the turn).
///
/// Returns whether the cancel was delivered: `false` when the transport is
/// already closed (the child died — the expected race, tolerated exactly like
/// [`is_cancel_transport_closed`] in the interrupt path, logged at DEBUG) or
/// on any other wire error (logged at WARN). Best-effort by design: a `false`
/// tells the caller the child is likely not settleable and a respawn path is
/// more appropriate than a warn-and-continue turn.
async fn cancel_and_settle_idle_prompt(
    conn: &Connection,
    agent_id: &AgentId,
    acp_session_id: &str,
) -> bool {
    match intent_acp::session::cancel(conn, acp_session_id).await {
        Ok(()) => true,
        Err(e) if is_cancel_transport_closed(&e) => {
            tracing::debug!(
                agent = %agent_id,
                error = %e,
                "idle-prompt settle skipped: transport already closed"
            );
            false
        }
        Err(e) => {
            tracing::warn!(agent = %agent_id, error = %e, "idle-prompt settle: session/cancel failed");
            false
        }
    }
}

/// Per-turn prompt-assembly hints threaded through `agent.sendMessage`
/// (PROTOCOL §5.5). `stdin_context` is prepended verbatim to the outbound
/// prompt as a `Context:` block (reference-parity `acp-provider.ts`);
/// `note_ids` and `context_references` are carried forward for downstream
/// note-image / context-reference resolution and are otherwise inert today.
///
/// Only the FIRST turn triggered by a `sendMessage` call carries these
/// options; queue-drained follow-up turns run with [`TurnOptions::default`]
/// since a `QueuedMessage` has no per-turn hints of its own.
#[derive(Debug, Default, Clone)]
pub struct TurnOptions {
    pub stdin_context: Option<String>,
    pub note_ids: Option<serde_json::Value>,
    pub context_references: Option<serde_json::Value>,
    /// FE-supplied image attachments: each `{ data, mimeType }` becomes an ACP
    /// `Image` content block appended after the text prompt (reference-parity
    /// `acp-provider.ts`).
    pub image_blocks: Option<serde_json::Value>,
    /// FE-supplied file attachments: each `{ data, mimeType, fileName }`
    /// becomes an ACP `Resource` content block (`EmbeddedResource` with
    /// `BlobResourceContents`) appended after the text prompt and any image
    /// blocks; the `fileName` becomes the resource `uri` as `file:///<name>`
    /// so downstream consumers can reference it.
    pub file_blocks: Option<serde_json::Value>,
    /// Opaque per-message payload from `agent.sendMessage`'s
    /// `messageMetadata` (PROTOCOL §5.5). Persisted verbatim on the user
    /// message row (via [`Store::append_agent_message_with_metadata`]). When a
    /// send is enqueued behind a running turn the metadata rides along on the
    /// `QueuedMessage` entry; drained turns rebuild their `TurnOptions` with
    /// the entry's captured metadata so both the drain-time persist and a
    /// later terminal-failure requeue keep the tag.
    pub message_metadata: Option<serde_json::Value>,
    /// `true` when this turn delivers a STALE queued-message redrive (#576):
    /// the message was enqueued before the delegated agent's current
    /// completion report was persisted, so the parent has already been woken
    /// with that report. The worker skips the turn-begin
    /// `clear_completion_report_if_present` for such turns, keeping the
    /// delivered report queryable via `agent.get`. Fresh turns leave this
    /// `false` and clear as today.
    pub suppress_report_clear: bool,
    /// The drained entry's original `queued_at`, threaded through so a
    /// terminal-failure requeue (STAB-112) re-enqueues with the ORIGINAL
    /// timestamp instead of stamping `now_iso()` — keeping the #576 staleness
    /// verdict sticky across retries (a stale redrive that fails stays stale,
    /// so the retry still suppresses the report clear). `None` for direct
    /// sends, whose requeue stamps `now_iso()` as before.
    pub queued_at: Option<String>,
    /// STAB-114 / monorepo#1014: text of the user message preempted by a
    /// zero-output interrupt, delivered AHEAD of this turn's own `content` in
    /// the SAME `session/prompt` so both messages are honored in order.
    /// Prompt-only — the preempted user row is already persisted, so this is
    /// never appended to the transcript again.
    pub prepend_content: Option<String>,
    /// Image attachments of the preempted message (same shape as
    /// `image_blocks`), emitted as ACP content blocks AHEAD of this turn's own
    /// attachments. Prompt-only, like `prepend_content`.
    pub prepend_image_blocks: Option<serde_json::Value>,
    /// File attachments of the preempted message (same shape as
    /// `file_blocks`), emitted as ACP content blocks AHEAD of this turn's own
    /// attachments. Prompt-only, like `prepend_content`.
    pub prepend_file_blocks: Option<serde_json::Value>,
    /// Turn correlation id (monorepo#1022): identifies the logical turn across
    /// terminal-failure requeues and redrives. Drained turns carry the queue
    /// entry's `turn_id`; direct sends mint one in `send_message` (before the
    /// user-row persist, so the RPC result and `agent:message` echo carry it),
    /// with [`AgentManager::spawn_worker`] as the fallback mint site.
    /// `publish_error_status_and_requeue` threads it onto the requeued entry
    /// so a retry of the same logical turn keeps the original id.
    pub turn_id: Option<String>,
    /// Who originated this delivery (question hold, PROTOCOL §5.5):
    /// `MessageOrigin::User` (FE `agent.sendMessage` / explicit user actions)
    /// is never held (but never releases the hold either); the `Automatic`
    /// default fails closed — an automatic send to an agent whose question
    /// hold is active enqueues instead of claiming the turn slot, so a
    /// pending Q&A is never buried by a system/agent turn.
    pub origin: intent_core::MessageOrigin,
    /// `true` when this delivery carries `priority: "interrupt"`. Every
    /// fallback path that parks the message in the queue (question hold,
    /// busy race, quarantine park, append-failure auto-queue, terminal-
    /// failure requeue) inserts it with interrupt priority — front of the
    /// queue, behind earlier interrupts (user decision, spec §Decisions).
    pub interrupt_priority: bool,
}

impl TurnOptions {
    /// Bundle the combined-delivery `prepend_*` fields for a queue fallback
    /// (monorepo#1034): when `send_message` parks the message instead of
    /// streaming it (quarantine park, concurrent-send slot race, append-
    /// failure auto-queue), the preempted message's content rides the
    /// `QueuedMessage` so the drain still delivers it ahead of the interrupt
    /// message. `None` when no prepend content is riding (ordinary sends).
    pub(crate) fn queued_prepend(&self) -> Option<crate::agent_ops::QueuedPrepend> {
        if self.prepend_content.is_none()
            && self.prepend_image_blocks.is_none()
            && self.prepend_file_blocks.is_none()
        {
            return None;
        }
        Some(crate::agent_ops::QueuedPrepend {
            content: self.prepend_content.clone(),
            image_blocks: self.prepend_image_blocks.clone(),
            file_blocks: self.prepend_file_blocks.clone(),
        })
    }
}

/// Reconstruct a [`TurnOptions::origin`] from a `QueuedMessage`'s persisted
/// `user_origin` flag at the queue-drain handoffs, so a drained entry keeps
/// its originator's semantics (question-hold bypass recorded at enqueue time;
/// attention-request clear gated on user origin).
fn origin_from_user_flag(user_origin: bool) -> intent_core::MessageOrigin {
    if user_origin {
        intent_core::MessageOrigin::User
    } else {
        intent_core::MessageOrigin::Automatic
    }
}

/// Rebuild the single-entry drain [`TurnOptions`] for one queue entry — the
/// exact shape the non-flush drain arms construct inline. Used by the
/// batch-flush persist-failure path so the failed entry is parked/requeued
/// with the same options a single-entry drain would have used.
fn turn_options_for_entry(entry: &QueuedMessage, stale: bool) -> TurnOptions {
    TurnOptions {
        image_blocks: entry.image_blocks.clone(),
        file_blocks: entry.file_blocks.clone(),
        message_metadata: entry.message_metadata.clone(),
        suppress_report_clear: stale,
        queued_at: Some(entry.queued_at.clone()),
        prepend_content: entry.prepend_content.clone(),
        prepend_image_blocks: entry.prepend_image_blocks.clone(),
        prepend_file_blocks: entry.prepend_file_blocks.clone(),
        turn_id: Some(entry.turn_id.clone()),
        interrupt_priority: entry.interrupt_priority,
        origin: origin_from_user_flag(entry.user_origin),
        ..TurnOptions::default()
    }
}

/// Conservative cap used when total system memory cannot be determined.
const DEFAULT_PROCESS_CAP: usize = 8;

/// Maximum concurrent agent processes for `total_memory_bytes`: reserve 8 GB
/// for the OS/other apps, then budget 1 GB per agent, clamped to [4, 100].
/// The 1 GB/agent budget is 2–4× the measured worst case (auggie ≈ 230 MB RSS
/// avg, claude-code chain ≈ 700 MB), so lower-RAM machines still get a tight
/// cap while high-RAM machines are not artificially throttled (for exact byte
/// counts: 16 GB → 8, 32 GB → 24, 64 GB → 56, ≥108 GB → 100; Linux MemTotal
/// runs slightly below nominal RAM, so a nominal 16 GB box may compute 7).
pub fn compute_process_cap(total_memory_bytes: u64) -> usize {
    let budget_gb = total_memory_bytes.saturating_sub(8 * GB) / GB;
    budget_gb.clamp(4, 100) as usize
}

/// Aggregate memory budget recommended for `total_memory_bytes` (monorepo#2063).
///
/// Reuses [`compute_process_cap`]'s 8 GB OS/other-apps reserve, then halves what
/// is left. The halving is the whole point of the number: the slot cap spends
/// *all* budgetable RAM assuming a 1 GB steady state per agent, but the measured
/// per-agent subtree spans 22x (436 MB idle to 9.6 GB running the repo's own
/// vitest suite), so the aggregate has to leave room for a tail agent on top of
/// the steady state. On a 48 GB seat that is a 20 GB budget with a 9.6 GB tail
/// agent's worth of slack still inside the 40 GB the slot cap already considers
/// spendable — and the 21.5 GB tree measured in #2063 would have crossed it.
///
/// The recommended default: `agents.memoryBudgetMb` defaults to auto (the
/// absent key; explicit 0 = off), and boot wiring resolves auto to this value.
pub fn recommended_memory_budget_bytes(total_memory_bytes: u64) -> u64 {
    (total_memory_bytes.saturating_sub(8 * GB) / 2).max(4 * GB)
}

/// Best-effort process cap from detected system RAM, falling back to
/// [`DEFAULT_PROCESS_CAP`] when total memory is unknown (RAM detection
/// supports Linux and macOS; other platforms fall back to the default).
pub fn default_process_cap() -> usize {
    match total_memory_bytes() {
        Some(bytes) => compute_process_cap(bytes),
        None => DEFAULT_PROCESS_CAP,
    }
}

/// Total system RAM in bytes (Linux/macOS only). Shared beyond this module by
/// [`crate::provider_models`]'s unsloth catalog fit filter, which compares a
/// model's estimated footprint against a fraction of this value.
#[cfg(target_os = "linux")]
pub(crate) fn total_memory_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// See the Linux doc comment above.
#[cfg(target_os = "macos")]
pub(crate) fn total_memory_bytes() -> Option<u64> {
    use std::mem;
    use std::ptr;

    let mut size: u64 = 0;
    let mut len = mem::size_of::<u64>();
    let name = b"hw.memsize\0";

    let result = unsafe {
        libc::sysctlbyname(
            name.as_ptr() as *const libc::c_char,
            &mut size as *mut u64 as *mut libc::c_void,
            &mut len,
            ptr::null_mut(),
            0,
        )
    };

    if result == 0 {
        Some(size)
    } else {
        None
    }
}

/// See the Linux doc comment above.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn total_memory_bytes() -> Option<u64> {
    None
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Async callback that tears down one process when the registry evicts/reaps it
/// (the Rust analog of the TS `ProcessEntry.kill`). The manager wires this to
/// drop the agent's [`AgentHandle`], killing the child and aborting its tasks.
pub type KillFn = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

/// Async callback for process-cap lifecycle events (queueing/resuming/eviction).
/// Invoked by the registry when a spawn queues, resumes, or an idle process is
/// evicted; the manager wires this to log + publish workspace events. The final
/// parameter is the machine-readable `reason` — [`REASON_SLOTS`] or
/// [`REASON_MEMORY_BUDGET`] — naming which admission constraint drove the event
/// (monorepo#2063).
pub type ProcessEventFn =
    Arc<dyn Fn(&AgentId, &str, usize, usize, &str) -> BoxFuture<'static, ()> + Send + Sync>;

/// `reason` value for `agent:process:*` events driven by the concurrency slot
/// cap (all slots active / a slot freed).
pub const REASON_SLOTS: &str = "slots";

/// `reason` value for `agent:process:*` events driven by the aggregate memory
/// budget (monorepo#2063).
pub const REASON_MEMORY_BUDGET: &str = "memory-budget";

struct ProcessEntry {
    last_active_ms: u64,
    is_active: bool,
    kill: KillFn,
}

#[derive(Default)]
struct RegistryInner {
    entries: HashMap<AgentId, ProcessEntry>,
    /// Queue of waiting spawns, each carrying the agent id + oneshot channel +
    /// the reason it queued ([`REASON_SLOTS`] / [`REASON_MEMORY_BUDGET`]), so
    /// the matching `agent:process:resumed` can echo it back.
    wait_queue: Vec<(AgentId, tokio::sync::oneshot::Sender<()>, &'static str)>,
    /// Sample id [`ProcessRegistry::budget_denies`] last corrected against.
    /// `None` until the first sample is consulted.
    budget_sample_seq: Option<u64>,
    /// Signed correction to that sample for spawns admitted and processes
    /// released since it was taken. Reset whenever a newer sample lands.
    budget_pending_bytes: i64,
}

fn pop_waiter(
    inner: &mut RegistryInner,
) -> Option<(AgentId, tokio::sync::oneshot::Sender<()>, &'static str)> {
    // Skip senders whose receiver is gone. A memory-budget waiter re-queues
    // after each [`BUDGET_RECHECK`] and an abandoned `acquire` future drops its
    // receiver outright, so handing the wakeup to a dead entry would consume it
    // and starve a waiter that is still listening.
    while !inner.wait_queue.is_empty() {
        let waiter = inner.wait_queue.remove(0);
        if !waiter.1.is_closed() {
            return Some(waiter);
        }
    }
    None
}

fn lru_idle(inner: &RegistryInner) -> Option<(AgentId, KillFn)> {
    inner
        .entries
        .iter()
        .filter(|(_, e)| !e.is_active)
        .min_by_key(|(_, e)| e.last_active_ms)
        .map(|(id, e)| (id.clone(), e.kill.clone()))
}

/// LRU idle entry excluding `exclude` — the turn-start budget gate's eviction
/// candidate list (monorepo#2063 B8): the gated agent's own process is never
/// its own gate's victim (evicting it would only trade this gate for the spawn
/// gate). Other admission paths — a concurrent spawn `acquire`, or another
/// turn-start gate — select via their own candidate lists and may still evict
/// a process parked here; its waiter then wakes via `deregister`, re-classifies
/// as unregistered, admits, and respawns through the spawn gate (queued, never
/// refused, message intact — the warm session is lost, not the turn).
fn lru_idle_excluding(inner: &RegistryInner, exclude: &AgentId) -> Option<(AgentId, KillFn)> {
    inner
        .entries
        .iter()
        .filter(|(id, e)| !e.is_active && *id != exclude)
        .min_by_key(|(_, e)| e.last_active_ms)
        .map(|(id, e)| (id.clone(), e.kill.clone()))
}

/// Idle entries whose `last_active_ms` is at/older than `cutoff_ms`, ordered
/// least-recently-used first (the TTL idle-reap candidate list).
fn idle_older_than(inner: &RegistryInner, cutoff_ms: u64) -> Vec<(AgentId, KillFn)> {
    let mut candidates: Vec<(AgentId, u64, KillFn)> = inner
        .entries
        .iter()
        .filter(|(_, e)| !e.is_active && e.last_active_ms <= cutoff_ms)
        .map(|(id, e)| (id.clone(), e.last_active_ms, e.kill.clone()))
        .collect();
    candidates.sort_by_key(|(_, ms, _)| *ms);
    candidates
        .into_iter()
        .map(|(id, _, kill)| (id, kill))
        .collect()
}

/// Idle entries ordered for the budget drain (monorepo#2063 level 2): largest
/// attributed subtree first (Phase A attribution), ties and unattributed
/// entries (charged 0 by the sort) falling back to least-recently-used first —
/// so with no attribution at all the order degrades to plain LRU.
fn idle_largest_first(
    inner: &RegistryInner,
    samples: &HashMap<AgentId, u64>,
) -> Vec<(AgentId, KillFn)> {
    let mut candidates: Vec<(AgentId, u64, u64, KillFn)> = inner
        .entries
        .iter()
        .filter(|(_, e)| !e.is_active)
        .map(|(id, e)| {
            (
                id.clone(),
                samples.get(id).copied().unwrap_or(0),
                e.last_active_ms,
                e.kill.clone(),
            )
        })
        .collect();
    candidates.sort_by(|a, b| b.1.cmp(&a.1).then(a.2.cmp(&b.2)));
    candidates
        .into_iter()
        .map(|(id, _, _, kill)| (id, kill))
        .collect()
}

/// The concrete service-layer [`EventSink`]: the bridge `intent-acp`'s
/// client-served request handler publishes its `file:changed` /
/// `agent:permission:*` events through, appended + broadcast over the M2
/// [`EventBus`] (the sink stamps the timestamp; §6.7/§10).
pub struct BusEventSink {
    bus: EventBus,
}

impl BusEventSink {
    /// Wire a sink over the shared event bus.
    pub fn new(bus: EventBus) -> Self {
        Self { bus }
    }
}

impl EventSink for BusEventSink {
    fn publish(&self, event: SinkEvent) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            // An agent `file:changed` also feeds the BE-internal code-review
            // pipeline (§17.1): record diff + attribution after the event lands.
            // The wiring composes `diffs` + `file_tracking` here so neither
            // service module depends on the other (§3.2).
            let track_ctx = (event.event_type == intent_core::events::FILE_CHANGED
                && event.actor.actor_type == ActorType::Agent)
                .then(|| {
                    (
                        event.workspace_id.clone(),
                        event.actor.id.clone(),
                        event.session_id.clone(),
                        event.data.clone(),
                    )
                });

            let new_event = NewEvent {
                workspace_id: event.workspace_id,
                timestamp: now_iso(),
                event_type: event.event_type,
                actor: event.actor,
                session_id: event.session_id,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: event.data,
            };
            if let Err(e) = self.bus.publish(&new_event).await {
                tracing::warn!(error = %e, "failed to publish agent client event");
            }

            if let Some((workspace_id, agent_id, session_id, data)) = track_ctx {
                self.record_agent_file_change(workspace_id, agent_id, session_id, data)
                    .await;
            }
        })
    }
}

impl BusEventSink {
    /// Record the code-review state for one agent `file:changed` (§17.3/§17.4):
    /// compute + persist the file's diff, then upsert its attribution row on
    /// `tracked_changes` (stage `unstaged`). Best-effort — every failure is
    /// logged and swallowed so a tracking miss never breaks the agent's edit.
    async fn record_agent_file_change(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<String>,
        session_id: Option<String>,
        data: Value,
    ) {
        let rel_path = match data
            .get("relativePath")
            .or_else(|| data.get("path"))
            .and_then(Value::as_str)
        {
            Some(p) if !p.is_empty() => p.to_string(),
            _ => return,
        };
        let status = match data.get("action").and_then(Value::as_str) {
            Some("create") => "added",
            Some("delete") => "deleted",
            _ => "modified",
        };

        let store = self.bus.store();
        let ws = match store.get_workspace(&workspace_id).await {
            Ok(ws) => ws,
            Err(e) => {
                tracing::warn!(error = %e, "file-tracking: workspace lookup failed");
                return;
            }
        };
        let Some(worktree) = crate::git_ops::worktree_path(&ws) else {
            return;
        };

        // Diff compute is best-effort: a missing repo / clean worktree still
        // records the attribution row (with zero stats) so provenance is kept.
        let summary = match crate::diffs::compute_and_store(
            store,
            &worktree,
            &workspace_id,
            &rel_path,
            false,
        )
        .await
        {
            Ok(summary) => summary,
            Err(e) => {
                tracing::warn!(error = %e, "file-tracking: diff compute failed");
                None
            }
        };

        let change = NewTrackedChange {
            workspace_id: workspace_id.clone(),
            path: rel_path,
            stage: "unstaged".to_string(),
            status: status.to_string(),
            agent_id,
            session_id,
            turn: None,
            commit_hash: None,
            old_blob_sha: summary.as_ref().and_then(|s| s.old_blob_sha.clone()),
            new_blob_sha: summary.as_ref().and_then(|s| s.new_blob_sha.clone()),
            additions: summary.as_ref().map(|s| s.additions).unwrap_or(0),
            deletions: summary.as_ref().map(|s| s.deletions).unwrap_or(0),
        };
        let attributed_agent = change.agent_id.clone();
        let (lines_added, lines_deleted) =
            match crate::file_tracking::track_change(store, change).await {
                Ok(delta) => delta,
                Err(e) => {
                    tracing::warn!(error = %e, "file-tracking: track_change failed");
                    return;
                }
            };
        // Recompute the durable line-change aggregates so the metrics.* reads
        // (§17.5) reflect this edit. Best-effort: attribution is already recorded.
        if let Err(e) = crate::metrics::recompute(store, &workspace_id).await {
            tracing::warn!(error = %e, "metrics: recompute failed");
        }
        // Global usage-stats (D5): fold the attributed lines-changed delta into
        // the current `usage_stats_hourly` bucket under the acting agent's
        // normalized model. Independent of the clearable metrics aggregates
        // above. Best-effort inside.
        crate::usage_stats::record_lines_changed(
            store,
            &workspace_id,
            attributed_agent.as_deref(),
            lines_added,
            lines_deleted,
        )
        .await;
    }
}

/// Source of the daemon's aggregate descendant-tree memory, implemented by the
/// composition root's `system.status` sampler (intentd#1139) and by fakes in
/// tests.
pub trait TreeMemoryProbe: Send + Sync {
    /// `(resident bytes across the whole descendant tree, monotonic sample id)`,
    /// or `None` before the first sample lands.
    ///
    /// The sample id lets the registry tell a fresh reading from a repeat of the
    /// one it already corrected for; it only has to change when the bytes are
    /// re-measured, and never has to mean anything else.
    fn sample(&self) -> Option<(u64, u64)>;

    /// Per-agent attribution of the same tree: resident bytes bucketed by
    /// nearest registered agent root, from the same sweep as [`Self::sample`]
    /// (monorepo#2063 Phase A). Empty before the first sample lands and for
    /// probes that don't attribute (the default keeps test fakes minimal).
    fn agent_samples(&self) -> HashMap<AgentId, u64> {
        HashMap::new()
    }
}

/// An installed aggregate memory budget (monorepo#2063).
struct MemoryBudget {
    budget_bytes: u64,
    probe: Arc<dyn TreeMemoryProbe>,
}

/// Provisional cost charged against the budget for a spawn that has been
/// admitted but is not yet visible in a tree sample (and credited back when a
/// process is deregistered). The measured median idle agent subtree is ~660 MB
/// across 7 agents (436-756 MB, monorepo#2063). Without it, a burst of spawns
/// all clear the gate against one up-to-5s-stale reading.
const PROVISIONAL_AGENT_BYTES: u64 = 660 * 1024 * 1024;

/// How long a spawn queued behind the memory budget sleeps before re-evaluating.
/// The slot cap's waiter is woken by `deregister`/`mark_idle`, but memory can
/// fall with no registry event at all (an agent's own child processes exit), so
/// the memory path must also re-check on a timer or it would sleep on a wakeup
/// that never comes.
const BUDGET_RECHECK: Duration = Duration::from_secs(5);

/// Global concurrency registry for spawned agent processes (port of
/// `agent-process-registry`). Enforces a hard cap across all workspaces and, on
/// [`ProcessRegistry::acquire`], evicts the least-recently-used idle process (or
/// queues the request when every process is active).
pub struct ProcessRegistry {
    cap: usize,
    inner: Mutex<RegistryInner>,
    /// Optional callback for lifecycle events (queue/resume/evict). Wired by the
    /// manager to publish events + log; the registry stays testable without it.
    event_fn: Option<ProcessEventFn>,
    /// Optional aggregate memory budget, installed once by the composition root
    /// when `agents.memoryBudgetMb` resolves to a positive budget (auto, the
    /// absent key, resolves to the recommended value; explicit 0 = off). Not
    /// installed leaves every path below byte-for-byte identical to the
    /// slot-cap-only behaviour.
    memory: std::sync::OnceLock<MemoryBudget>,
}

/// Last sample plus the signed correction for admissions/releases since it was
/// taken, saturating at 0.
fn charged_bytes(sampled: u64, pending: i64) -> u64 {
    if pending >= 0 {
        sampled.saturating_add(pending as u64)
    } else {
        sampled.saturating_sub(pending.unsigned_abs())
    }
}

/// Whether the aggregate budget admits one more spawn.
///
/// `live == 0` always admits. The tree the probe measures includes processes the
/// registry does not own (one-shot adapter chains, model probes) and, on a busy
/// host, is simply not something the daemon controls; without this the daemon
/// could refuse every spawn forever and never make progress.
fn budget_admits(charged: u64, budget_bytes: u64, live: usize) -> bool {
    live == 0 || charged < budget_bytes
}

impl ProcessRegistry {
    /// A registry with a fixed concurrency `cap`.
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            inner: Mutex::new(RegistryInner::default()),
            event_fn: None,
            memory: std::sync::OnceLock::new(),
        }
    }

    /// Install the aggregate memory budget (monorepo#2063). Called once by the
    /// composition root after the sampler exists; returns false if a budget was
    /// already installed. Not a builder because the probe and the registry are
    /// constructed in either order depending on the caller.
    pub fn set_memory_budget(&self, budget_bytes: u64, probe: Arc<dyn TreeMemoryProbe>) -> bool {
        self.memory
            .set(MemoryBudget {
                budget_bytes,
                probe,
            })
            .is_ok()
    }

    /// Consult the budget under the already-held lock, refreshing the pending
    /// correction when a newer sample has landed. Returns `Some(charged_bytes)`
    /// when the budget denies this spawn; `None` when it admits — including when
    /// no budget is installed and when no sample exists yet, so an unconfigured
    /// or not-yet-sampled daemon behaves exactly as before.
    fn budget_denies(&self, inner: &mut RegistryInner) -> Option<u64> {
        let budget = self.memory.get()?;
        let (sampled, seq) = budget.probe.sample()?;
        if inner.budget_sample_seq != Some(seq) {
            inner.budget_sample_seq = Some(seq);
            inner.budget_pending_bytes = 0;
        }
        let charged = charged_bytes(sampled, inner.budget_pending_bytes);
        (!budget_admits(charged, budget.budget_bytes, inner.entries.len())).then_some(charged)
    }

    /// Read-only budget visibility for `system.status` (monorepo#2063):
    /// `Some((budget_bytes, charged_bytes, queued_spawns))` when a budget is
    /// installed, else `None`. `charged_bytes` is what admission actually
    /// compares — the last tree sample plus the pending correction — and is
    /// `None` until the first sample lands (the budget is inert until then,
    /// see [`Self::budget_denies`]). A sample newer than the one the
    /// correction was accumulated against is served uncorrected, mirroring
    /// the reset `budget_denies` would perform — but without mutating, so a
    /// status read never perturbs admission state. `queued_spawns` counts
    /// live waiters (dropped receivers excluded), whether they queued on the
    /// slot cap or the budget.
    pub fn budget_status(&self) -> Option<(u64, Option<u64>, u64)> {
        let budget = self.memory.get()?;
        let inner = self.inner.lock().unwrap();
        let charged = budget.probe.sample().map(|(sampled, seq)| {
            if inner.budget_sample_seq == Some(seq) {
                charged_bytes(sampled, inner.budget_pending_bytes)
            } else {
                sampled
            }
        });
        let queued = inner
            .wait_queue
            .iter()
            .filter(|(_, tx, _)| !tx.is_closed())
            .count() as u64;
        Some((budget.budget_bytes, charged, queued))
    }

    /// Adjust the pending correction by one agent's provisional cost: `+1` on
    /// admission (the spawn is not in the last sample yet), `-1` on deregister
    /// (the dead process still is). No-op when no budget is installed.
    fn budget_adjust(&self, inner: &mut RegistryInner, agents: i64) {
        if self.memory.get().is_some() {
            inner.budget_pending_bytes = inner
                .budget_pending_bytes
                .saturating_add(agents.saturating_mul(PROVISIONAL_AGENT_BYTES as i64));
        }
    }

    /// Credit raw `bytes` back against the pending correction, beyond what
    /// [`Self::budget_adjust`] already credited. Used by the budget drain: a
    /// victim's *attributed* size can dwarf the fixed provisional credit, and
    /// without this the drain keeps killing smaller idle agents for up to a
    /// full sample period after the real overshoot cleared. An over-credit
    /// from stale attribution self-heals the moment a fresh sample lands
    /// ([`Self::budget_denies`] resets the pending correction on a new seq).
    fn budget_credit_extra_bytes(&self, bytes: u64) {
        if bytes == 0 || self.memory.get().is_none() {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        inner.budget_pending_bytes = inner
            .budget_pending_bytes
            .saturating_sub(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    /// Attach an event callback for lifecycle events (queueing/resuming/eviction).
    /// Chainable builder; returns `Self` so the manager can wire this after
    /// construction. The callback signature is
    /// `(agent_id, event_type, used, cap, reason)` — see [`ProcessEventFn`].
    pub fn with_event_fn(mut self, f: ProcessEventFn) -> Self {
        self.event_fn = Some(f);
        self
    }

    /// The configured concurrency cap.
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Number of registered processes.
    pub fn size(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    /// Whether `agent_id` is currently registered.
    pub fn is_registered(&self, agent_id: &AgentId) -> bool {
        self.inner.lock().unwrap().entries.contains_key(agent_id)
    }

    /// Register a freshly spawned process (starts idle). `kill` tears the
    /// process down when the registry evicts/reaps it.
    pub fn register(&self, agent_id: AgentId, kill: KillFn) {
        self.inner.lock().unwrap().entries.insert(
            agent_id,
            ProcessEntry {
                last_active_ms: now_ms(),
                is_active: false,
                kill,
            },
        );
    }

    /// Remove a process and wake the next queued spawn, if any. When a waiter is
    /// resumed, logs + emits `agent:process:resumed` via the event callback.
    pub fn deregister(&self, agent_id: &AgentId) -> bool {
        let resumed_agent = {
            let mut inner = self.inner.lock().unwrap();
            let had = inner.entries.remove(agent_id).is_some();
            if !had {
                return false;
            }
            // The dead process is still inside the last tree sample; credit its
            // provisional cost back so a spawn queued behind the budget is not
            // held off for up to a full sample period by memory already freed.
            self.budget_adjust(&mut inner, -1);
            pop_waiter(&mut inner)
        };
        if let Some((resumed_id, tx, reason)) = resumed_agent {
            let _ = tx.send(());
            let used = self.size();
            tracing::info!(
                agent = %resumed_id,
                used = used,
                cap = self.cap,
                reason = reason,
                "process registry: queued spawn resumed"
            );
            if let Some(ref f) = self.event_fn {
                let fut = f(&resumed_id, "agent:process:resumed", used, self.cap, reason);
                tokio::spawn(fut);
            }
        }
        true
    }

    /// Mark a process as actively streaming (never evicted while active).
    pub fn mark_active(&self, agent_id: &AgentId) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.entries.get_mut(agent_id) {
            entry.is_active = true;
            entry.last_active_ms = now_ms();
        }
    }

    /// Mark a process idle (eligible for eviction) and wake a queued spawn so it
    /// can take the freed slot immediately. When a waiter is resumed, logs + emits
    /// `agent:process:resumed` via the event callback.
    pub fn mark_idle(&self, agent_id: &AgentId) {
        let resumed_agent = {
            let mut inner = self.inner.lock().unwrap();
            let existed = match inner.entries.get_mut(agent_id) {
                Some(entry) => {
                    entry.is_active = false;
                    entry.last_active_ms = now_ms();
                    true
                }
                None => false,
            };
            if !existed {
                return;
            }
            pop_waiter(&mut inner)
        };
        if let Some((resumed_id, tx, reason)) = resumed_agent {
            let _ = tx.send(());
            let used = self.size();
            tracing::info!(
                agent = %resumed_id,
                used = used,
                cap = self.cap,
                reason = reason,
                "process registry: queued spawn resumed"
            );
            if let Some(ref f) = self.event_fn {
                let fut = f(&resumed_id, "agent:process:resumed", used, self.cap, reason);
                tokio::spawn(fut);
            }
        }
    }

    /// Ensure a slot is free before spawning: returns immediately under the cap,
    /// otherwise evicts the LRU idle process, or queues until one frees. Logs +
    /// emits `agent:process:queued` / `agent:process:evicted` via the event callback.
    ///
    /// When an aggregate memory budget is installed (monorepo#2063), being over
    /// budget denies admission on exactly the same terms as being at the slot
    /// cap: reclaim by evicting the LRU *idle* process, else queue. The budget
    /// therefore never touches anything that is running — it delays a spawn and
    /// takes back idle processes, and the agent that caused the overshoot is
    /// never the one it acts on. That is deliberate: the measured per-agent cost
    /// spans 22x, so a ceiling that permits a legitimate 9.6 GB test run cannot
    /// also prevent several of them, and one that prevents them kills the work
    /// agents exist to do. Admission is the only lever that is cheap, reversible,
    /// and never wrong about which agent to punish.
    pub async fn acquire(&self, agent_id: &AgentId) {
        loop {
            enum Action {
                Slot,
                /// The `&'static str` is the reason the eviction was needed
                /// ([`REASON_SLOTS`] / [`REASON_MEMORY_BUDGET`]).
                Evict(AgentId, KillFn, &'static str),
                /// The `bool` is true when the wait is on memory rather than a
                /// slot, which needs a timed re-check rather than only a wakeup.
                Wait(tokio::sync::oneshot::Receiver<()>, bool),
            }
            let action = {
                let mut inner = self.inner.lock().unwrap();
                let over_budget = self.budget_denies(&mut inner);
                // Which admission constraint is binding right now. When both
                // bind at once, the budget wins the label — matching the log
                // discrimination below, and the budget is the constraint a
                // freed slot alone cannot clear.
                let reason = if over_budget.is_some() {
                    REASON_MEMORY_BUDGET
                } else {
                    REASON_SLOTS
                };
                if inner.entries.len() < self.cap && over_budget.is_none() {
                    // Charge the spawn now: it will not appear in a tree sample
                    // for up to a sampling period, and a burst of spawns must not
                    // all clear the gate against the same stale reading.
                    self.budget_adjust(&mut inner, 1);
                    Action::Slot
                } else if let Some((id, kill)) = lru_idle(&inner) {
                    Action::Evict(id, kill, reason)
                } else {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    // Drop waiters whose receiver is gone before adding ours, so
                    // re-queueing on the budget re-check cannot grow the queue.
                    inner.wait_queue.retain(|(_, tx, _)| !tx.is_closed());
                    inner.wait_queue.push((agent_id.clone(), tx, reason));
                    let used = inner.entries.len();
                    match over_budget {
                        Some(charged) => tracing::info!(
                            agent = %agent_id,
                            used = used,
                            cap = self.cap,
                            charged_memory_bytes = charged,
                            budget_bytes = self.memory.get().map(|b| b.budget_bytes),
                            "process registry: spawn queued (aggregate memory budget)"
                        ),
                        None => tracing::info!(
                            agent = %agent_id,
                            used = used,
                            cap = self.cap,
                            "process registry: spawn queued (all slots active)"
                        ),
                    }
                    if let Some(ref f) = self.event_fn {
                        let fut = f(agent_id, "agent:process:queued", used, self.cap, reason);
                        tokio::spawn(fut);
                    }
                    Action::Wait(rx, over_budget.is_some())
                }
            };
            match action {
                Action::Slot => return,
                Action::Evict(id, kill, reason) => {
                    let used = self.size();
                    tracing::info!(
                        evicted = %id,
                        used = used,
                        cap = self.cap,
                        reason = reason,
                        "process registry: LRU idle process evicted"
                    );
                    if let Some(ref f) = self.event_fn {
                        let fut = f(&id, "agent:process:evicted", used, self.cap, reason);
                        tokio::spawn(fut);
                    }
                    kill().await;
                    self.deregister(&id);
                }
                Action::Wait(rx, true) => {
                    // Memory can fall with no registry event to wake us — an
                    // agent's own children exiting frees the tree without any
                    // process being deregistered — so re-evaluate on a timer.
                    let _ = tokio::time::timeout(BUDGET_RECHECK, rx).await;
                }
                Action::Wait(rx, false) => {
                    let _ = rx.await;
                }
            }
        }
    }

    /// Turn-start budget re-check for an agent whose process is already
    /// registered (monorepo#2063 B8): an idle agent's next turn gates on the
    /// aggregate memory budget on the same terms as a spawn — reclaim by
    /// evicting the LRU idle process, else queue until the budget clears —
    /// never refused. Differences from [`Self::acquire`], all deliberate:
    /// - The slot cap is not consulted: the process already holds its slot.
    /// - Admission charges no provisional cost: the warm process is already
    ///   inside the tree sample, so charging would double-count it.
    /// - Its own idle process is never *this gate's* eviction victim: that
    ///   would only trade this gate for the spawn gate and lose the warm
    ///   session. Other admission paths may still reclaim it while it waits
    ///   (see [`lru_idle_excluding`]); the turn then degrades to the spawn
    ///   gate — still queued, never refused.
    /// - A process marked ACTIVE admits immediately: busy agents are never
    ///   gated mid-turn (regression-pinned).
    ///
    /// An unregistered agent admits immediately too — its child must spawn,
    /// and `create_agent`'s [`Self::acquire`] is that path's gate.
    pub async fn acquire_turn_start(&self, agent_id: &AgentId) {
        loop {
            enum Action {
                Admit,
                Evict(AgentId, KillFn),
                Wait(tokio::sync::oneshot::Receiver<()>),
            }
            let action = {
                let mut inner = self.inner.lock().unwrap();
                let idle_here = matches!(inner.entries.get(agent_id), Some(e) if !e.is_active);
                let over_budget = if idle_here {
                    self.budget_denies(&mut inner)
                } else {
                    None
                };
                if let Some(charged) = over_budget {
                    if let Some((id, kill)) = lru_idle_excluding(&inner, agent_id) {
                        Action::Evict(id, kill)
                    } else {
                        let (tx, rx) = tokio::sync::oneshot::channel();
                        // Same waiter hygiene as `acquire`: drop dead receivers
                        // before re-queueing on the timed budget re-check.
                        inner.wait_queue.retain(|(_, tx, _)| !tx.is_closed());
                        inner
                            .wait_queue
                            .push((agent_id.clone(), tx, REASON_MEMORY_BUDGET));
                        let used = inner.entries.len();
                        tracing::info!(
                            agent = %agent_id,
                            used = used,
                            cap = self.cap,
                            charged_memory_bytes = charged,
                            budget_bytes = self.memory.get().map(|b| b.budget_bytes),
                            "process registry: turn start queued (aggregate memory budget)"
                        );
                        if let Some(ref f) = self.event_fn {
                            let fut = f(
                                agent_id,
                                "agent:process:queued",
                                used,
                                self.cap,
                                REASON_MEMORY_BUDGET,
                            );
                            tokio::spawn(fut);
                        }
                        Action::Wait(rx)
                    }
                } else {
                    Action::Admit
                }
            };
            match action {
                Action::Admit => return,
                Action::Evict(id, kill) => {
                    let used = self.size();
                    tracing::info!(
                        evicted = %id,
                        used = used,
                        cap = self.cap,
                        reason = REASON_MEMORY_BUDGET,
                        "process registry: LRU idle process evicted"
                    );
                    if let Some(ref f) = self.event_fn {
                        let fut = f(
                            &id,
                            "agent:process:evicted",
                            used,
                            self.cap,
                            REASON_MEMORY_BUDGET,
                        );
                        tokio::spawn(fut);
                    }
                    kill().await;
                    self.deregister(&id);
                }
                Action::Wait(rx) => {
                    // Memory can fall with no registry event to wake us (same
                    // as the `acquire` budget wait), so re-evaluate on a timer.
                    let _ = tokio::time::timeout(BUDGET_RECHECK, rx).await;
                }
            }
        }
    }

    /// Set a process's `last_active` timestamp directly (deterministic LRU
    /// ordering in tests).
    #[cfg(test)]
    pub(crate) fn set_last_active(&self, agent_id: &AgentId, ms: u64) {
        if let Some(entry) = self.inner.lock().unwrap().entries.get_mut(agent_id) {
            entry.last_active_ms = ms;
        }
    }

    /// Whether a process is marked active (test observability).
    #[cfg(test)]
    pub(crate) fn is_active(&self, agent_id: &AgentId) -> bool {
        self.inner
            .lock()
            .unwrap()
            .entries
            .get(agent_id)
            .map(|e| e.is_active)
            .unwrap_or(false)
    }

    /// Evict idle processes in LRU order (the idle-reap hook; full
    /// timer/memory-pressure triggering is M5). Returns the number evicted.
    pub async fn evict_idle(&self, max: Option<usize>) -> usize {
        let max = max.unwrap_or(usize::MAX);
        let mut evicted = 0;
        while evicted < max {
            let Some((id, kill)) = ({
                let inner = self.inner.lock().unwrap();
                lru_idle(&inner)
            }) else {
                break;
            };
            kill().await;
            self.deregister(&id);
            evicted += 1;
        }
        evicted
    }

    /// TTL-based idle reaping (§5.6/§6.7): evict every idle process whose last
    /// activity is older than `ttl`. Active processes and those within the TTL
    /// are always kept. Returns the number evicted.
    ///
    /// Claim-before-kill (monorepo#2118): `try_claim` must atomically claim
    /// the candidate against whatever can start work on it (the manager wires
    /// it to check-and-claim under the same lock `try_begin` uses), and
    /// `release` drops that claim after the kill — or immediately when the
    /// re-validation below rejects the candidate. A plain eligibility check
    /// here would be a TOCTOU: earlier kills in the sweep await (SIGTERM
    /// grace + descendant sweep), so a turn could start between the check and
    /// the kill and have its child tree killed.
    ///
    /// NOT cancellation-safe: dropping this future at the `kill().await`
    /// leaks the held claim — `release` never runs for it, so `try_begin`
    /// permanently loses for that agent and its queue never drains until
    /// daemon restart. The reap loop task is the only production caller and
    /// is aborted only at shutdown; a future caller must not wrap this in a
    /// timeout/select that can drop it mid-kill.
    pub async fn evict_idle_older_than<C, R>(
        &self,
        ttl: Duration,
        try_claim: C,
        release: R,
    ) -> usize
    where
        C: Fn(&AgentId) -> bool,
        R: Fn(&AgentId),
    {
        let cutoff = now_ms().saturating_sub(ttl.as_millis() as u64);
        let candidates = {
            let inner = self.inner.lock().unwrap();
            idle_older_than(&inner, cutoff)
        };
        let mut evicted = 0;
        for (id, kill) in candidates {
            if !try_claim(&id) {
                continue;
            }
            // Re-validate under the registry lock: the candidate snapshot is
            // stale by the time earlier kills in this sweep have awaited, so
            // the entry may have run a whole turn (fresh `last_active_ms`),
            // be actively streaming again, or be gone. With the claim held
            // nothing can start new work on it after this check.
            let still_stale = {
                let inner = self.inner.lock().unwrap();
                inner
                    .entries
                    .get(&id)
                    .is_some_and(|e| !e.is_active && e.last_active_ms <= cutoff)
            };
            if !still_stale {
                release(&id);
                continue;
            }
            kill().await;
            self.deregister(&id);
            release(&id);
            evicted += 1;
        }
        evicted
    }

    /// Budget-triggered idle drain (monorepo#2063 level 2): while the
    /// aggregate charge is over the installed budget, evict idle processes
    /// largest-attributed-subtree-first (Phase A attribution; LRU fallback
    /// when attribution is unavailable), re-checking the budget before every
    /// kill so the drain stops the moment the overshoot clears. Each
    /// eviction credits the victim's attributed bytes (at least the
    /// provisional cost) against the charge, so the stop condition holds
    /// within a single sample period — not only after the next sample lands.
    /// Unlike the
    /// [`Self::acquire`] path this fires without a spawn attempt, and unlike
    /// [`Self::evict_idle_older_than`] it ignores the TTL — memory pressure
    /// is now, not in `idleReapMinutes`. No budget installed, no sample yet,
    /// or under budget → 0 evictions, byte-for-byte the old behaviour.
    /// Active processes are never candidates, mirroring both other paths.
    ///
    /// Each eviction logs + emits `agent:process:evicted` with reason
    /// [`REASON_MEMORY_BUDGET`], matching the acquire path's budget eviction.
    ///
    /// Claim-before-kill (monorepo#2118): same `try_claim` / `release`
    /// contract as [`Self::evict_idle_older_than`], including the
    /// re-validation before each kill and the NOT-cancellation-safe caveat —
    /// dropping this future at the `kill().await` leaks the held claim.
    pub async fn evict_while_over_budget<C, R>(&self, try_claim: C, release: R) -> usize
    where
        C: Fn(&AgentId) -> bool,
        R: Fn(&AgentId),
    {
        // The common case (under budget / no budget / no sample) must stay one
        // lock + one probe read. The candidate order is snapshotted here; the
        // per-kill re-check below only decides *whether* to keep draining.
        let (candidates, samples) = {
            let mut inner = self.inner.lock().unwrap();
            if self.budget_denies(&mut inner).is_none() {
                return 0;
            }
            let samples = self
                .memory
                .get()
                .map(|b| b.probe.agent_samples())
                .unwrap_or_default();
            (idle_largest_first(&inner, &samples), samples)
        };
        let mut evicted = 0;
        for (id, kill) in candidates {
            // Stop the moment the budget clears: each eviction credits the
            // victim's full known cost back (below), and a fresh sample may
            // land mid-drain.
            let still_over = {
                let mut inner = self.inner.lock().unwrap();
                self.budget_denies(&mut inner).is_some()
            };
            if !still_over {
                break;
            }
            if !try_claim(&id) {
                continue;
            }
            // Re-validate under the registry lock (the monorepo#2118 TOCTOU):
            // earlier kills in this drain awaited, so the entry may be
            // actively streaming again or gone. With the claim held nothing
            // can start new work on it after this check.
            let still_idle = {
                let inner = self.inner.lock().unwrap();
                inner.entries.get(&id).is_some_and(|e| !e.is_active)
            };
            if !still_idle {
                release(&id);
                continue;
            }
            let used = self.size();
            tracing::info!(
                evicted = %id,
                used = used,
                cap = self.cap,
                reason = REASON_MEMORY_BUDGET,
                "process registry: over-budget idle process evicted"
            );
            if let Some(ref f) = self.event_fn {
                let fut = f(
                    &id,
                    "agent:process:evicted",
                    used,
                    self.cap,
                    REASON_MEMORY_BUDGET,
                );
                tokio::spawn(fut);
            }
            kill().await;
            self.deregister(&id);
            // `deregister` credited the fixed provisional cost; top it up to
            // the victim's attributed bytes when attribution knows more, so
            // the per-kill re-check above sees the real reclaim within this
            // sample period instead of over-draining every idle candidate
            // (the next sample resets the correction either way).
            self.budget_credit_extra_bytes(
                samples
                    .get(&id)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(PROVISIONAL_AGENT_BYTES),
            );
            release(&id);
            evicted += 1;
        }
        evicted
    }
}

/// A generated `--mcp-config` file on disk, removed when the owning agent's
/// handle is dropped (the file only needs to outlive the child that reads it).
struct TempConfigFile {
    path: PathBuf,
}

impl Drop for TempConfigFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

use crate::pi_cli::{resolve_real_pi_command, PI_ACP_PI_COMMAND_ENV};

/// Env var the bundled pi extension reads for the per-agent MCP bridge's
/// loopback TCP address (`host:port`, see [`McpBridge::connect_addr`]).
const INTENTD_MCP_BRIDGE_ADDR_ENV: &str = "INTENTD_MCP_BRIDGE_ADDR";

/// Bundled pi extension source (MCP bridge client + tool registration),
/// embedded at build time and written to a per-agent temp file at spawn.
const PI_MCP_EXTENSION_SOURCE: &str = include_str!("pi_mcp_extension.ts");

/// Per-agent pi-extension MCP delivery files: the bundled extension plus a
/// wrapper script that execs the real pi binary with `-e <extension>`. Both
/// live in the temp dir for the lifetime of the owning agent handle (same
/// pattern as the generated `--mcp-config`).
struct PiExtensionDelivery {
    /// Held for its temp-file lifetime only — the wrapper script carries the
    /// path, so nothing reads this field after construction.
    _extension: TempConfigFile,
    wrapper: TempConfigFile,
}

impl PiExtensionDelivery {
    /// Write the extension + wrapper (0755) files into `dir`. The wrapper only
    /// appends our `-e` flag — user-installed pi extensions stay enabled.
    #[cfg(unix)]
    fn write(real_pi_command: &str, dir: &Path) -> Result<Self> {
        use std::os::unix::fs::PermissionsExt;

        let extension_path = dir.join(format!("intentd-pi-ext-{}.ts", Uuid::new_v4()));
        std::fs::write(&extension_path, PI_MCP_EXTENSION_SOURCE)
            .map_err(|e| Error::Internal(format!("write pi extension failed: {e}")))?;
        let extension = TempConfigFile {
            path: extension_path,
        };

        let wrapper_path = dir.join(format!("intentd-pi-wrapper-{}.sh", Uuid::new_v4()));
        let script = format!(
            "#!/bin/sh\nexec {} -e {} \"$@\"\n",
            sh_squote(real_pi_command),
            sh_squote(&extension.path.to_string_lossy())
        );
        std::fs::write(&wrapper_path, script)
            .map_err(|e| Error::Internal(format!("write pi wrapper failed: {e}")))?;
        let wrapper = TempConfigFile { path: wrapper_path };
        std::fs::set_permissions(&wrapper.path, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| Error::Internal(format!("chmod pi wrapper failed: {e}")))?;
        Ok(Self {
            _extension: extension,
            wrapper,
        })
    }

    /// The delivery relies on an executable `#!/bin/sh` wrapper; there is no
    /// non-unix equivalent, so fail with a clear error instead of spawning pi
    /// with a script it cannot execute.
    #[cfg(not(unix))]
    fn write(_real_pi_command: &str, _dir: &Path) -> Result<Self> {
        Err(Error::Internal(
            "pi extension MCP delivery requires a unix host (sh wrapper script)".to_string(),
        ))
    }

    /// Insert the two spawn env vars: route pi-acp's pi spawn through the
    /// wrapper, and hand the extension the bridge's TCP address.
    fn apply_spawn_env(
        &self,
        extra_env: &mut BTreeMap<String, String>,
        bridge_connect_addr: String,
    ) {
        extra_env.insert(
            PI_ACP_PI_COMMAND_ENV.to_string(),
            self.wrapper.path.to_string_lossy().into_owned(),
        );
        extra_env.insert(INTENTD_MCP_BRIDGE_ADDR_ENV.to_string(), bridge_connect_addr);
    }
}

/// Write the pi-extension delivery files into `dir` for providers flagged
/// `mcp_via_pi_extension`; `None` for every other provider.
fn pi_extension_delivery(
    provider: &ProviderConfig,
    dir: &Path,
) -> Result<Option<PiExtensionDelivery>> {
    if !provider.mcp_via_pi_extension {
        return Ok(None);
    }
    Ok(Some(PiExtensionDelivery::write(
        &resolve_real_pi_command(),
        dir,
    )?))
}

/// Single-quote a string for inert interpolation into a `sh` script: quotes
/// suppress all expansion, and embedded `'` uses the standard `'\''` escape.
fn sh_squote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// One live agent: its ACP [`Connection`] (own id space + pending map), the
/// streaming-notification receiver consumed during a turn, the client-served
/// request loop, the owned child (its process group is killed on teardown via
/// [`kill_child_tree`], with `kill_on_drop` as a direct-child safety net), and
/// the per-agent MCP bridge + generated config that back the agent→BE tool loop.
///
/// `spawned_model` and `spawned_provider` track the model/provider the child was
/// spawned with, enabling `ensure_started` to detect model changes (via `agent.setModel`)
/// and respawn the child with the new model before the next turn.
struct AgentHandle {
    connection: Arc<Connection>,
    notifications: Arc<TokioMutex<mpsc::UnboundedReceiver<IncomingNotification>>>,
    serve_task: JoinHandle<()>,
    _child: Option<Child>,
    /// The child's pid captured at spawn: `Child::id()` reads `None` once a
    /// `try_wait` liveness probe reaps the exit status, and the pgid-based
    /// teardown (`kill_child_tree`) still needs it to sweep same-group
    /// descendants that outlive the leader (monorepo#764).
    child_pid: Option<u32>,
    _mcp_bridge: Option<McpBridge>,
    _mcp_config: Option<TempConfigFile>,
    _rules_config: Option<TempConfigFile>,
    /// Bundled pi-extension MCP delivery files (extension + wrapper script),
    /// removed when the handle drops (pi only).
    _pi_extension: Option<PiExtensionDelivery>,
    /// MCP servers (workspace bridge + user servers) delivered via the ACP
    /// `session/new` / `session/load` `mcpServers` field for providers that
    /// consume them there (claude-code, codex, droid, grok). Empty for providers
    /// that receive MCP config out-of-band (auggie `--mcp-config`, opencode
    /// env config) — passing servers they'd ignore is avoided for wire parity.
    session_mcp_servers: Vec<McpServer>,
    spawned_model: Option<String>,
    spawned_provider: String,
    /// The reasoning-effort (`thought_level`) selector the provider advertised
    /// at session open (PROTOCOL §5.5), with `current_value` tracking the value
    /// the adapter is believed to be on — updated on every successful
    /// `session/set_config_option`. `None` for providers that advertise no such
    /// option, which makes every effort application a silent no-op. Lets
    /// `ensure_started` re-apply a mid-session `reasoningEffort` change on the
    /// LIVE child, so it lands before the next prompt without a respawn.
    thought_level: Option<ThoughtLevelOption>,
    /// Pause gate for the idle wake listener (monorepo#855): while > 0 the
    /// listener neither locks nor consumes `notifications`. Raised around
    /// `start_session` so a `session/load` replay burst is always drained by
    /// the resume path, never opened as an implicit turn.
    wake_gate: Arc<AtomicUsize>,
    /// The per-agent idle-notification listener (monorepo#855), installed by
    /// `create_agent` after the handle lands; aborted when the handle drops.
    wake_listener: Option<JoinHandle<()>>,
}

impl Drop for AgentHandle {
    fn drop(&mut self) {
        self.serve_task.abort();
        if let Some(listener) = &self.wake_listener {
            listener.abort();
        }
    }
}

/// Decrements the paired [`AgentHandle::wake_gate`] on drop, so every raise
/// (however the raising scope exits) is matched by exactly one lower.
struct WakeGateGuard(Arc<AtomicUsize>);

impl Drop for WakeGateGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, AtomicOrdering::SeqCst);
    }
}

/// Quiescence settle window for implicit harness-wake turns (monorepo#855):
/// an implicit turn finalizes once no `session/update` arrived for this long.
#[cfg(not(test))]
const HARNESS_WAKE_SETTLE: Duration = Duration::from_millis(2000);
#[cfg(test)]
const HARNESS_WAKE_SETTLE: Duration = Duration::from_millis(200);

/// Idle-listener poll cadence: how often the per-agent wake listener checks
/// for out-of-turn notifications while no prompt turn owns the receiver.
const HARNESS_WAKE_POLL: Duration = Duration::from_millis(50);

type Handles = Arc<Mutex<HashMap<AgentId, AgentHandle>>>;

/// RAII guard returned by [`AgentManager::stop_many`]: while alive, the swept
/// agents stay in the manager's `stopping` set, fencing them off from the
/// lazy-spawn paths (`ensure_started` / `create_agent`). The caller holds it
/// across the store cascade that deletes the swept session rows, then drops
/// it — after which the rows are gone and a spawn attempt fails `NotFound`
/// on its own store read.
#[must_use = "dropping the fence immediately re-opens the lazy-spawn paths"]
pub struct TeardownFence {
    stopping: Arc<Mutex<HashSet<AgentId>>>,
    ids: Vec<AgentId>,
}

impl Drop for TeardownFence {
    fn drop(&mut self) {
        // Poison recovery mirrors the delete-path sweeps: unfencing is the
        // last chance to keep the set from leaking these ids forever.
        let mut stopping = self.stopping.lock().unwrap_or_else(|e| e.into_inner());
        for id in &self.ids {
            stopping.remove(id);
        }
    }
}

/// Why a [`AgentManager::try_begin_outcome`] claim did or did not start:
/// `Started` claimed the slot; `Busy` lost to a turn already in flight (a
/// prompt worker owns the slot and its receiver); `ReapClaimed` lost to the
/// idle-reap sweep holding the agent mid-kill (monorepo#2118) — nobody owns
/// the slot, the handle is being torn down, so no work may be handed to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TryBeginOutcome {
    Started,
    Busy,
    ReapClaimed,
}

/// Central multiplexer over the ACP client + process registry (§6.8). Owns a
/// [`HashMap<AgentId, AgentHandle>`], the [`ProcessRegistry`], and the shared
/// [`EventSink`]/permission state the per-agent client handlers use.
pub struct AgentManager {
    services: Services,
    registry: Arc<ProcessRegistry>,
    handles: Handles,
    sink: Arc<dyn EventSink>,
    permissions: Arc<PermissionRegistry>,
    policy: PermissionPolicy,
    mcp_bridge_exe: PathBuf,
    /// Root the per-agent stderr capture files live under (STAB-53), laid out
    /// as `<root>/<agent-id>/<YYYY-MM-DD>.log`. The composition root wires
    /// `<data_dir>/agent-logs`; `None` (tests / bare wiring) disables capture.
    agent_log_root: Option<PathBuf>,
    /// Daemon-owned directory the per-agent generated config files
    /// (`--mcp-config`, `--rules`, pi-extension delivery) are written into
    /// (monorepo#1302). The composition root wires `<data_dir>/agent-configs`
    /// and sweeps leftovers at startup; `None` (tests / bare wiring) falls
    /// back to the OS temp dir.
    agent_config_root: Option<PathBuf>,
    /// Dedicated, daemon-owned, empty spawn cwd for chief provider children
    /// (STAB-50). The composition root wires `<data_dir>/chief-cwd`; `None`
    /// (tests / bare wiring) falls back to the temp dir.
    chief_cwd_root: Option<PathBuf>,
    /// Agents with an in-flight turn loop (a worker is draining their stream).
    /// `agent.sendMessage` consults this to flip a message to the queue while a
    /// turn is mid-stream (the TS "queue while streaming" semantics).
    busy: Arc<Mutex<HashSet<AgentId>>>,
    /// Agents claimed by the idle-reap sweep for the duration of their kill
    /// (monorepo#2118). The claim is taken under the `busy` lock (lock order
    /// busy → reap_claims, matching `try_begin`'s read), so "not busy →
    /// claimed" is atomic against a concurrent `try_begin`: a send racing the
    /// sweep queues instead of starting a turn whose child tree the sweep is
    /// about to kill. Released (no `busy` lock needed) after the kill, when
    /// the sweep kicks the queue drain for anything that parked meanwhile.
    reap_claims: Arc<Mutex<HashSet<AgentId>>>,
    /// Workspace each in-flight agent belongs to, recorded when the agent claims
    /// its in-flight slot so the slot release can recompute the workspace's
    /// derived `WorkspaceActivity` (§9.9) even on the `stop` path, which only
    /// knows the agent id.
    agent_ws: Arc<Mutex<HashMap<AgentId, WorkspaceId>>>,
    /// Abortable background turn workers, keyed by agent. `stop` aborts the
    /// in-flight worker (interrupting the current stream).
    workers: Arc<Mutex<HashMap<AgentId, JoinHandle<()>>>>,
    /// Agents whose ACP session was recreated (the resume-impossible fallback in
    /// [`AgentManager::start_session`] replaced a lost `acpSessionId` with a fresh
    /// `session/new`). The next turn prepends the prior conversation history as
    /// `<supervisor>` XML so the fresh session has context, then clears the flag
    /// (parity: TS `sessionWasRecreated`).
    recreated: Arc<Mutex<HashSet<AgentId>>>,
    /// Agents whose NEXT turn must carry the assembled system prompt prepended
    /// as a `<system>` block — the FirstTurnPrepend fallback (§18.1) for
    /// providers with no (usable) native injection mechanism (codex, cortex,
    /// pi, grok, mock). Set when a
    /// FRESH ACP session is opened (`session/new`, brand-new or recreate) for a
    /// provider whose `injection_mechanism` is
    /// [`InjectionMechanism::FirstTurnPrepend`]; NOT set on `session/load`
    /// resume (the provider kept its prior context, which already saw the
    /// prompt). Consumed by [`AgentManager::build_turn_prompt`] so the block
    /// fires exactly once per fresh session and re-fires after a recreate.
    prepend_pending: Arc<Mutex<HashSet<AgentId>>>,
    /// Most recent interrupt-priority `messageId` delivered per agent
    /// (PROTOCOL §5.5). [`AgentManager::interrupt_send_message`] records the
    /// client-supplied id under this lock BEFORE preempting, so the SAME
    /// interrupt delivered twice (client retry / event double-fire) preempts
    /// exactly once — the duplicate is acknowledged idempotently instead of
    /// cancelling the interrupt turn it raced and re-persisting the message.
    interrupt_ids: Arc<Mutex<HashMap<AgentId, String>>>,
    /// Pending prompt-only redelivery payloads armed by a zero-output user
    /// stop (intent-hq/monorepo#1757): a plain `agent.stop` cancelling a turn
    /// that produced NO assistant output drops the turn's user message
    /// provider-side (`session/cancel` discards the in-flight prompt), so the
    /// stopped message's text + attachments are captured here and merged into
    /// the NEXT turn's `prepend_*` options by [`AgentManager::spawn_worker`]
    /// — the same combined-delivery semantics the interrupt-priority
    /// preemption path gets from [`AgentManager::preempt_busy_turn`]
    /// (monorepo#1014). Prompt-only: the user row is already persisted, so
    /// nothing is appended to the transcript. Armed on both the keep-alive
    /// interrupt path and the spawn-window fallback-to-kill path; cleared by
    /// [`AgentManager::edit_and_regenerate`] (the truncation may remove the
    /// captured message, and the recreate's history replay covers survivors).
    stop_redelivery: Arc<Mutex<HashMap<AgentId, crate::agent_ops::QueuedPrepend>>>,
    /// Agents whose NEXT session establishment must SKIP the `session/load`
    /// resume and open a fresh `session/new` instead — armed by
    /// [`AgentManager::edit_and_regenerate`] (immediately after target
    /// validation, before the stop) because a resumed provider session would
    /// retain the truncated turns in its context, and by
    /// [`AgentManager::agent_retry`] when the session is poisoned
    /// (monorepo#940: resuming would replay the exact context the provider
    /// deterministically rejects, so the redrive must open a fresh
    /// `session/new`). Deliberately NOT cleared by
    /// [`AgentManager::stop`] (unlike `recreated`/`prepend_pending`): the
    /// truncation is already persisted, so the stale provider history must
    /// never be resumed regardless of intervening stops. Enforced at BOTH
    /// establishment points — [`AgentManager::start_session`] skips the
    /// resume, and [`AgentManager::ensure_started`]'s live-child reuse path
    /// tears the child down when armed — and consumed only when a fresh
    /// session is successfully opened, so spawn retries keep the flag.
    ///
    /// KNOWN LIMITATION: in-memory only, like `recreated` (and the same gap
    /// `agent.replaceMessages` has today). If the daemon restarts before the
    /// regenerated turn opens its fresh session (e.g. the spawn failed
    /// terminally and the agent parked in `Error` awaiting `agent.retry`),
    /// the intent is lost and the next spawn may resume the stale provider
    /// session via `session/load`. Persisting a needs-recreate marker on the
    /// session row would close this; not done here to keep parity with the
    /// existing replaceMessages semantics.
    force_recreate: Arc<Mutex<HashSet<AgentId>>>,
    /// Agents fenced off from the lazy-spawn paths because a batch teardown
    /// ([`AgentManager::stop_many`]) is in flight and their session rows are
    /// about to be cascade-deleted (`workspace.delete`). While an agent is in
    /// this set, [`AgentManager::ensure_started`] refuses to (re)spawn it and
    /// [`AgentManager::create_agent`] refuses to install a fresh handle —
    /// otherwise a concurrent `agent.sendMessage` that passed its store check
    /// before the sweep could lazily spawn a replacement child during the
    /// shared grace wait, and that child would outlive its deleted session as
    /// a ghost process no sweep ever kills. Armed by `stop_many` BEFORE the
    /// first detach; cleared when the returned [`TeardownFence`] drops (after
    /// the caller's store cascade, at which point the session row is gone and
    /// the spawn path fails `NotFound` on its own).
    stopping: Arc<Mutex<HashSet<AgentId>>>,
    /// Daemon-owned singleton Unsloth server (spec "Proposed design" §4,
    /// monorepo#878): started on demand when an `unsloth`-provider agent
    /// spawns, reused while the served model matches, restarted on model
    /// switch, and killed on daemon [`AgentManager::shutdown`].
    unsloth: Arc<crate::unsloth_server::UnslothServerManager>,
    /// Descendant-tree memory probe for read-only visibility (monorepo#2063
    /// A2): installed unconditionally by the composition root (unlike the
    /// budget probe on the registry, which only exists when
    /// `agents.memoryBudgetMb` resolves to a positive value) so
    /// `agent.diagnostics` can serve per-agent subtree attribution whether or
    /// not a budget is configured. Absent in tests / bare wiring.
    tree_probe: std::sync::OnceLock<Arc<dyn TreeMemoryProbe>>,
}

impl AgentManager {
    /// Wire a manager over the services surface and a concrete event sink, with
    /// a global concurrency `cap`.
    pub fn new(services: Services, sink: Arc<dyn EventSink>, cap: usize) -> Self {
        // Wire the registry event function to publish process-cap lifecycle events.
        let services_clone = services.clone();
        let event_fn: ProcessEventFn = Arc::new(move |agent_id, event_type, used, cap, reason| {
            let services = services_clone.clone();
            let agent_id = agent_id.clone();
            let event_type = event_type.to_string();
            let reason = reason.to_string();
            Box::pin(async move {
                // Best-effort workspace lookup: process-cap events are global across
                // workspaces, so when the session row is missing (mid-create or already
                // deleted) swallow the lookup error and skip the publish rather than
                // blocking the registry path. The tracing log still fires above.
                let workspace_id = match services.store.get_agent_session(&agent_id).await {
                    Ok(session) => session.workspace_id,
                    Err(_) => return,
                };
                services
                    .publish_agent_event(
                        &workspace_id,
                        &agent_id,
                        &event_type,
                        json!({
                            "agentId": agent_id.0,
                            "used": used,
                            "cap": cap,
                            "reason": reason,
                        }),
                    )
                    .await;
            })
        });

        Self {
            services,
            registry: Arc::new(ProcessRegistry::new(cap).with_event_fn(event_fn)),
            handles: Arc::new(Mutex::new(HashMap::new())),
            sink,
            permissions: Arc::new(PermissionRegistry::new()),
            // Shipped default (§6.7/M3.5): `AllowAll` for reference parity with
            // the TS acp-provider — [`start_session`] additionally attempts
            // `session/set_mode bypassPermissions` on providers that advertise
            // set-mode (auggie today), and the local `AllowAll` auto-approve
            // handles anything the provider still surfaces. An FE-attached
            // deployment selects `Interactive` via `with_policy()` (wired from
            // `INTENTD_PERMISSION_POLICY`) to drive the
            // `agent.respondPermission` / `agent.pendingPermissions` RPCs;
            // `AutoByRisk` / `DenyAll` remain selectable via the same env var.
            policy: PermissionPolicy::AllowAll,
            mcp_bridge_exe: std::env::current_exe().unwrap_or_else(|_| PathBuf::from("intentd")),
            agent_log_root: None,
            agent_config_root: None,
            chief_cwd_root: None,
            busy: Arc::new(Mutex::new(HashSet::new())),
            reap_claims: Arc::new(Mutex::new(HashSet::new())),
            agent_ws: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            recreated: Arc::new(Mutex::new(HashSet::new())),
            prepend_pending: Arc::new(Mutex::new(HashSet::new())),
            interrupt_ids: Arc::new(Mutex::new(HashMap::new())),
            stop_redelivery: Arc::new(Mutex::new(HashMap::new())),
            force_recreate: Arc::new(Mutex::new(HashSet::new())),
            stopping: Arc::new(Mutex::new(HashSet::new())),
            unsloth: Arc::new(crate::unsloth_server::UnslothServerManager::default()),
            tree_probe: std::sync::OnceLock::new(),
        }
    }

    /// Override the permission policy used by spawned agents' client handlers.
    pub fn with_policy(mut self, policy: PermissionPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The active permission policy (headless `AutoByRisk` unless overridden).
    pub fn policy(&self) -> PermissionPolicy {
        self.policy
    }

    /// Override the executable used as the generated `--mcp-config` bridge
    /// command (defaults to the current `intentd` binary). Tests point this at
    /// `CARGO_BIN_EXE_intentd` so a spawned child reaches the in-process server.
    pub fn with_mcp_bridge_exe(mut self, exe: impl Into<PathBuf>) -> Self {
        self.mcp_bridge_exe = exe.into();
        self
    }

    /// Enable per-agent stderr capture (STAB-53): every spawned child's stderr
    /// is appended to `<root>/<agent-id>/<YYYY-MM-DD>.log`. The composition
    /// root passes `intent_core::agent_logs_root(&config.data_dir)`.
    pub fn with_agent_log_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.agent_log_root = Some(root.into());
        self
    }

    /// Set the daemon-owned directory the per-agent generated config files
    /// are written into (monorepo#1302). The composition root passes
    /// `intent_core::agent_configs_root(&config.data_dir)` after sweeping
    /// leftovers; the directory is created on demand right before a spawn
    /// writes into it.
    pub fn with_agent_config_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.agent_config_root = Some(root.into());
        self
    }

    /// Set the dedicated spawn cwd for chief provider children (STAB-50).
    /// The composition root passes `intent_core::chief_cwd_root(&config.data_dir)`;
    /// the directory is created on demand right before a chief spawn resolves.
    pub fn with_chief_cwd_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.chief_cwd_root = Some(root.into());
        self
    }

    /// Directory the per-agent generated config files are written into: the
    /// wired `<data_dir>/agent-configs` dir, created on demand — or the OS
    /// temp dir when no root is wired (tests / bare wiring) or creation fails.
    fn agent_config_dir(&self) -> PathBuf {
        let Some(root) = self.agent_config_root.as_ref() else {
            return std::env::temp_dir();
        };
        if let Err(e) = intent_core::agent_configs::create_agent_configs_dir(root) {
            tracing::warn!(
                error = %e,
                path = %root.display(),
                "failed to create agent-configs dir; falling back to temp dir"
            );
            return std::env::temp_dir();
        }
        root.clone()
    }

    /// Stderr capture directory for `agent_id`, when capture is enabled —
    /// the "agent stderr captured at …" hint on terminal-failure WARN lines.
    /// Points at the per-agent directory rather than today's daily file: the
    /// writer rotates by UTC date, so around midnight the last lines may sit
    /// in yesterday's file, making a file path misleading. The directory is
    /// rollover-stable and still immediately actionable.
    fn agent_stderr_log_dir(&self, agent_id: &AgentId) -> Option<PathBuf> {
        self.agent_log_root
            .as_ref()
            .map(|root| root.join(&agent_id.0))
    }

    /// Borrow the process registry (lifecycle / diagnostics).
    pub fn registry(&self) -> &ProcessRegistry {
        &self.registry
    }

    /// Install the descendant-tree memory probe for read-only visibility
    /// (monorepo#2063 A2). Called once by the composition root after the
    /// sampler exists; a second call is a no-op. Deliberately separate from
    /// [`ProcessRegistry::set_memory_budget`]: diagnostics attribution wants
    /// the probe even when no budget is configured.
    pub fn set_tree_probe(&self, probe: Arc<dyn TreeMemoryProbe>) {
        let _ = self.tree_probe.set(probe);
    }

    /// Per-agent subtree memory attribution from the installed probe
    /// (monorepo#2063 A2), or an empty map when no probe is wired (tests /
    /// bare wiring) or no sample has landed yet.
    pub fn agent_memory_samples(&self) -> HashMap<AgentId, u64> {
        self.tree_probe
            .get()
            .map(|p| p.agent_samples())
            .unwrap_or_default()
    }

    /// Snapshot of `spawned child pid -> agent id` for every live handle that
    /// owns a child process (monorepo#2063 Phase A). Handles without a known
    /// pid (fake/transport-only handles) are omitted. The descendant-tree
    /// sampler uses this to bucket subtree RSS by nearest registered agent
    /// root; it is a point-in-time snapshot, so a pid may already be dead by
    /// the time the caller walks the process table — the walk simply finds no
    /// process for it.
    ///
    /// Handles whose child has already been reaped are omitted too: a dead
    /// child's handle stays installed until the exit watcher removes it, and
    /// in that window the OS can recycle the pid for an unrelated process —
    /// mapping it would credit that stranger's subtree to the old agent.
    /// `try_wait` is cheap and idempotent (tokio caches the exit status), so
    /// this costs one non-blocking syscall per live handle. An indeterminate
    /// `try_wait` error is treated as reaped: the pid cannot be trusted, and
    /// its subtree falls back to the aggregate rather than a wrong bucket.
    pub fn agent_root_pids(&self) -> HashMap<u32, AgentId> {
        self.handles
            .lock()
            .unwrap()
            .iter_mut()
            .filter_map(|(agent_id, handle)| {
                let pid = handle.child_pid?;
                if let Some(child) = handle._child.as_mut() {
                    if !matches!(child.try_wait(), Ok(None)) {
                        return None;
                    }
                }
                Some((pid, agent_id.clone()))
            })
            .collect()
    }

    /// Resolve an outstanding interactive permission prompt (`agent.respondPermission`,
    /// PROTOCOL §8): deliver `outcome` to the blocked client handler. Returns
    /// `false` when no such prompt is outstanding (already answered or timed out).
    pub fn respond_permission(&self, request_id: &str, outcome: PermissionOutcome) -> bool {
        self.permissions.resolve(request_id, outcome)
    }

    /// Snapshot every outstanding permission prompt (`agent.pendingPermissions`,
    /// PROTOCOL §8), for a (re)connecting client to recover what awaits an answer.
    pub fn pending_permissions(&self) -> Vec<PermissionRequestData> {
        self.permissions.pending()
    }

    /// Number of tracked agents.
    pub fn len(&self) -> usize {
        self.handles.lock().unwrap().len()
    }

    /// Whether no agents are tracked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `agent_id` is currently tracked (lookup).
    pub fn contains(&self, agent_id: &AgentId) -> bool {
        self.handles.lock().unwrap().contains_key(agent_id)
    }

    /// Number of currently-tracked agents spawned with the given provider id
    /// (e.g. `"unsloth"`), for `unsloth.status`'s `attachedAgentCount`.
    pub fn count_agents_with_provider(&self, provider_id: &str) -> usize {
        self.handles
            .lock()
            .unwrap()
            .values()
            .filter(|h| h.spawned_provider == provider_id)
            .count()
    }

    /// The daemon-owned singleton Unsloth server manager, for the
    /// `unsloth.status` / `unsloth.stop` RPCs.
    pub fn unsloth_manager(&self) -> &Arc<crate::unsloth_server::UnslothServerManager> {
        &self.unsloth
    }

    /// Spawn a provider child, acquire a concurrency slot, stand up the per-agent
    /// agent→BE MCP server + bridge (denylisted for `agent_type`, §6.8/§18.4),
    /// write the generated `--mcp-config` for providers that consume it, wire the
    /// client-served request loop, and track it. Each connection's pending-id
    /// correlation lives in `intent-acp`; the manager keys the handle by
    /// `AgentId` and registers it for lifecycle/eviction.
    pub async fn create_agent(
        &self,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        agent_name: impl Into<String>,
        agent_type: &str,
        cwd: PathBuf,
        opts: &SpawnOptions<'_>,
    ) -> Result<()> {
        self.registry.acquire(&agent_id).await;

        // Session row for the sub-agent derivation below and the prompt
        // assembly further down (one read, shared). The session was inserted
        // by the caller before `create_agent` runs, so propagate any store
        // error rather than silently defaulting to top-level (which would
        // mis-scope both surfaces and hide DB failures).
        let session = self.services.store.get_agent_session(&agent_id).await?;
        // Sub-agent gating: delegated children (`parent_agent_id` set) and
        // background workers (`is_background`) — the same derivation the
        // prompt assembly uses. Captured once here, at bridge creation:
        // parentage never changes for a live session, and while
        // `is_background` can flip at runtime via `agent.update`, the bridge
        // keeps its spawn-time surface until the next respawn re-derives the
        // flag (same snapshot semantics as the `[agentFeatures]` capture
        // below).
        let is_sub_agent = session.parent_agent_id.is_some() || session.is_background;
        // `[agentFeatures]` for this session: the snapshot stamped on the row
        // at creation (`harness_features`, monorepo#2459), with a live-settings
        // fallback for legacy pre-0096 rows. Used for both the MCP bridge and
        // the prompt assembly below, so a respawn (model change, daemon
        // restart) keeps the surface the session was created with — matching
        // what `harnessFeatures` reports on the wire.
        let agent_features = self.services.session_agent_features(&session);

        // Per-agent in-process MCP server over the SAME services surface the FE
        // uses, with the §18.4 denylist for this agent type applied, served over
        // a loopback bridge a real spawned child reaches via `--mcp-config`.
        let api: Arc<dyn WorkspaceApi> = Arc::new(self.services.clone());
        let server = Arc::new(
            WorkspaceMcpServer::for_agent_type(api, workspace_id.clone(), agent_type)
                // Caller-aware tools attribute back to this spawning agent.
                .with_caller_agent_id(Some(agent_id.clone()))
                // §7.1 deterministic attach: tool dispatch registers resource
                // payloads into the same registry the transcript writer claims.
                .with_turn_attachments(Some(self.services.turn_attachments()))
                // The session's captured `[agentFeatures]` snapshot: settings
                // changes after creation never mutate this session's surface,
                // across respawns included.
                .with_agent_features(agent_features.clone())
                // Sub-agent bridges prune/deny `ws.app.question.*` (top-level
                // agents only own a user-facing chat turn).
                .with_sub_agent(is_sub_agent)
                // Specialist `modelOptions` (PROTOCOL §5.11) resolved once
                // at bridge creation, same snapshot semantics as the
                // feature toggles: the delegate docs in this agent's
                // `workspace_api` description list them per specialist.
                .with_specialist_model_options(
                    self.services
                        .specialist_model_options_for_workspace(&workspace_id)
                        .await,
                ),
        );
        let bridge = serve_workspace_mcp_tcp(server)
            .await
            .map_err(|e| Error::Internal(format!("mcp bridge bind failed: {e}")))?;

        // Directory the generated per-agent files below are written into:
        // `<data_dir>/agent-configs` when wired (swept at startup so a killed
        // daemon's leftovers don't accumulate, monorepo#1302), else temp dir.
        let config_dir = self.agent_config_dir();

        // Generated MCP config (auggie format) pointing at the bridge
        // subcommand, written only for providers that consume an MCP-config flag.
        let mut mcp_config: Option<TempConfigFile> = None;
        let mut mcp_config_path: Option<String> = None;
        if opts.provider.supports_mcp_config {
            let config = self.generate_mcp_config(&bridge).await?;
            let path = config_dir.join(format!("intentd-mcp-{}.json", Uuid::new_v4()));
            let bytes = serde_json::to_vec_pretty(&config)
                .map_err(|e| Error::Internal(format!("serialize mcp config failed: {e}")))?;
            std::fs::write(&path, bytes)
                .map_err(|e| Error::Internal(format!("write mcp config failed: {e}")))?;
            mcp_config_path = Some(path.to_string_lossy().into_owned());
            mcp_config = Some(TempConfigFile { path });
        }

        // For env-config providers (opencode), the same normalized server set
        // (workspace bridge + user servers) rides in `OPENCODE_CONFIG_CONTENT`
        // as an `mcp` block instead of an `--mcp-config` file, pointing at the
        // same bridge endpoint.
        let mut env_mcp_config: Option<String> = None;
        if opts.provider.injection_mechanism == InjectionMechanism::EnvConfig {
            env_mcp_config = Some(self.opencode_env_mcp_config(bridge.connect_addr()).await?);
        }

        // For pi, MCP delivery rides a bundled pi extension: pi-acp has no
        // MCP CLI flag and does not wire `session/new` `mcpServers` into the
        // pi process, so the spawn env routes pi-acp's pi spawn through a
        // wrapper script adding `-e <extension>` (PI_ACP_PI_COMMAND) and the
        // extension dials the same bridge endpoint (INTENTD_MCP_BRIDGE_ADDR).
        //
        // Fail fast before spawning when the `pi` CLI the wrapper would exec
        // is missing or known-too-old for the pinned pi-acp adapter
        // (monorepo#1662) — a clear error instead of a silent hang. The probe
        // is blocking (subprocess, ≤3s budget), so it runs off the runtime.
        if opts.provider.mcp_via_pi_extension {
            let status = tokio::task::spawn_blocking(crate::pi_cli::probe_pi_cli)
                .await
                .map_err(|e| Error::Internal(format!("pi CLI probe task failed: {e}")))?;
            crate::pi_cli::check_pi_cli_for_spawn(&status)?;
        }
        let pi_extension = pi_extension_delivery(opts.provider, &config_dir)?;

        // For providers that consume MCP servers from the ACP session setup
        // (claude-code, codex, droid, grok), the same normalized server set is
        // carried as the typed `session/new` / `session/load` `mcpServers`
        // field, pointing at the same bridge endpoint. Kept on the handle so
        // `start_session` (which runs after `create_agent`) can pass it into
        // every session-open branch.
        let mut session_mcp_servers: Vec<McpServer> = Vec::new();
        if opts.provider.supports_session_mcp_servers {
            let servers = self.normalized_mcp_servers(bridge.connect_addr()).await?;
            session_mcp_servers = to_acp_session_mcp_servers(&servers);
        }

        // Assemble the effective system prompt (the §18.1 injection pipeline:
        // base/specialization/workspace user overrides + live workspace rule
        // files, plus — for specialist agents — the PP-1 `<specialist_role>`
        // section and role-reminder footer, and — for top-level agents — the
        // SP-1 `## Suggested Next Steps` directive) into a temp `--rules` file
        // when the caller supplies none. The handle owns the temp file so it
        // outlives the child that reads it.
        let mut rules_config: Option<TempConfigFile> = None;
        let mut rules_file_path: Option<String> = None;
        if opts.rules_file.is_none() {
            let specialist = self
                .services
                .agent_specialist_injection(&agent_id, Some(&cwd))
                .await;
            // `rtk.enabled` is a global (non-workspace-scoped) setting read
            // live at spawn; the `[agentFeatures]` toggles come from the
            // session's captured snapshot (`agent_features` above) so the
            // prompt matches the surface stamped at session creation;
            // auto-commit resolves per-workspace (persisted override → global
            // `git.autoCommit` fallback, spec Diagnosis §3b) so the prompt
            // reflects what the commit gate will actually enforce.
            let settings = self.services.effective_settings();
            let auto_commit_enabled = self.services.effective_auto_commit(&workspace_id).await;
            // Sub-agent gating: `is_sub_agent` (derived from the session read
            // at the top of this function) also skips the suggested-prompts
            // directive, matching the reference `isSubAgent` derivation.
            // Fetch workspace for mode-dependent prompt hints (Task 6).
            let workspace = self.services.store.get_workspace(&workspace_id).await.ok();
            if let Some(prompt) = crate::rules::assemble_system_prompt(
                &self.services.store,
                Some(&cwd),
                agent_type,
                specialist.as_ref(),
                is_sub_agent,
                auto_commit_enabled,
                settings.rtk.enabled,
                &agent_features,
                workspace.as_ref(),
                Some(&session),
            )
            .await
            {
                let path = config_dir.join(format!("intentd-rules-{}.md", Uuid::new_v4()));
                std::fs::write(&path, prompt.as_bytes())
                    .map_err(|e| Error::Internal(format!("write rules file failed: {e}")))?;
                rules_file_path = Some(path.to_string_lossy().into_owned());
                rules_config = Some(TempConfigFile { path });
                // Persist the assembled systemPrompt on the session so
                // `agent.getSession` can return it without re-assembly.
                // Narrow write (system_prompt only): persisting the session
                // snapshot read at the top of this function through the
                // full-row `update_agent_session` silently reverted a
                // concurrent `agent.setModel` landing inside the spawn window,
                // so the next turn never saw the switch (monorepo#1936).
                if let Err(e) = self
                    .services
                    .store
                    .set_agent_session_system_prompt(&workspace_id, &agent_id, &prompt)
                    .await
                {
                    tracing::warn!(agent = %agent_id, error = %e, "failed to persist system_prompt on session");
                }
            }
        }

        // Reconstruct the spawn options with the generated config path injected.
        let mut spawn_opts = rebuild_spawn_opts(
            opts,
            rules_file_path.as_deref(),
            mcp_config_path.as_deref(),
            env_mcp_config.as_deref(),
        );
        if let Some(delivery) = &pi_extension {
            delivery.apply_spawn_env(&mut spawn_opts.extra_env, bridge.connect_addr());
        }

        let (req_tx, mut req_rx) = mpsc::unbounded_channel::<IncomingRequest>();
        let (note_tx, note_rx) = mpsc::unbounded_channel::<IncomingNotification>();
        let hooks = ConnectionHooks {
            requests: Some(req_tx),
            notifications: Some(note_tx),
            auth_error_patterns: opts
                .provider
                .auth_error_patterns
                .map(|p| p.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default(),
            // STAB-53: capture the child's stderr under
            // `<agent-logs>/<agent-id>/<YYYY-MM-DD>.log` so a child that dies
            // mid-turn leaves a diagnosable trace.
            stderr_log_dir: self
                .agent_log_root
                .as_ref()
                .map(|root| root.join(&agent_id.0)),
        };
        // Pre-first-token turn-startup hint: the child process is about to be
        // spawned for this agent, so surface the `launch` phase before the
        // (potentially slow) `spawn_provider` call blocks the turn (STAT-1 /
        // PROTOCOL §7). Emitted whether or not a session is subsequently opened
        // — the parent turn may still be gated on the child coming up.
        self.services
            .publish_status_event(
                &workspace_id,
                &agent_id,
                "launch",
                "Launching agent\u{2026}",
                "info",
            )
            .await;
        let spawned = spawn_provider(&spawn_opts, hooks)
            .map_err(|e| Error::Internal(format!("spawn provider failed: {e}")))?;
        let (child, connection) = spawned.into_parts();
        // Pin the spawned child's pid for the exit watcher armed below: the
        // watcher stands down when the handle's child no longer matches it
        // (a respawn installed a newer child with its own watcher).
        let child_pid = child.id();
        let connection = Arc::new(connection);

        let terminal_host: Arc<dyn intent_acp::TerminalHost> =
            Arc::new(crate::PtyTerminalHost::with_shell_mode(
                self.services.pty(),
                self.services.settings_registry(),
                opts.provider.terminal_requires_shell,
            ));
        let handler = Arc::new(
            ClientRequestHandler::new(
                workspace_id.clone(),
                agent_id.clone(),
                agent_name.into(),
                FileService::new(cwd),
                self.permissions.clone(),
                self.policy,
                self.sink.clone(),
            )
            .with_terminal_host(terminal_host),
        );
        let serve_conn = connection.clone();
        let serve_task = tokio::spawn(async move {
            while let Some(req) = req_rx.recv().await {
                if let Err(e) = handler.serve(serve_conn.as_ref(), req).await {
                    tracing::warn!(error = %e, "client-served request failed");
                }
            }
        });

        self.registry
            .register(agent_id.clone(), self.make_kill(agent_id.clone()));
        let handle = AgentHandle {
            connection,
            notifications: Arc::new(TokioMutex::new(note_rx)),
            serve_task,
            _child: Some(child),
            child_pid,
            _mcp_bridge: Some(bridge),
            _mcp_config: mcp_config,
            _rules_config: rules_config,
            _pi_extension: pi_extension,
            session_mcp_servers,
            spawned_model: opts.model.map(|s| s.to_string()),
            spawned_provider: opts.provider.command.to_string(),
            thought_level: None,
            wake_gate: Arc::new(AtomicUsize::new(0)),
            wake_listener: None,
        };
        // Concurrency safety: fully reap any stale handle + child for this agent
        // BEFORE installing the new one, reusing the process-group teardown.
        // A bare `insert` would only drop the old handle (aborting its serve
        // loop, with `kill_on_drop` reaping just the direct child) — orphaning
        // grandchildren and risking a lingering streamer from a lost/old session
        // that could keep appending to the agentId-keyed transcript. The
        // per-agent single-flight slot serializes turns; this closes the
        // respawn-time window. (Drop the lock before awaiting the kill.)
        let stale = self.handles.lock().unwrap().remove(&agent_id);
        if let Some(mut stale) = stale {
            let stale_pid = stale.child_pid;
            if let Some(child) = stale._child.take() {
                kill_child_tree(child, stale_pid).await;
            }
        }
        // Teardown fence (ghost-agent race): a `workspace.delete` batch stop
        // (`stop_many`) may have swept this agent AFTER the caller's session
        // checks passed — installing the fresh handle now would leave a live
        // child no sweep ever kills once the store cascade drops the session
        // row. Check-and-insert under the `stopping` lock so the install is
        // atomic against `stop_many` arming the fence: fence armed first →
        // the install is refused here (fresh child killed below); handle
        // installed first → `stop_many`'s detach finds it and kills it with
        // the batch. Either interleaving leaves no orphaned process.
        let fenced = {
            let stopping = self.stopping.lock().unwrap();
            if stopping.contains(&agent_id) {
                Some(handle)
            } else {
                self.handles
                    .lock()
                    .unwrap()
                    .insert(agent_id.clone(), handle);
                None
            }
        };
        if let Some(mut handle) = fenced {
            self.registry.deregister(&agent_id);
            let spawn_pid = handle.child_pid;
            if let Some(child) = handle._child.take() {
                kill_child_tree(child, spawn_pid).await;
            }
            return Err(Error::NotFound(format!(
                "agent session {agent_id} is being deleted"
            )));
        }
        // Proactive dead-child detection (monorepo#764): watch the installed
        // child for an unexpected exit so an idle agent's death is reaped
        // (handle + registry) as it happens, not just at the next prompt.
        // Detached: the watcher stands down on its own when the handle goes
        // away (deliberate teardown) or a respawn supersedes the child.
        let _watcher = self.arm_child_exit_watcher(agent_id.clone(), child_pid);
        // Idle wake listener (monorepo#855): stream out-of-turn
        // `session/update` bursts as implicit agent-initiated turns instead of
        // buffering them until the next prompt. Stored on the handle so the
        // teardown paths (stop/detach/respawn) abort it with the handle.
        let listener = self.spawn_wake_listener(agent_id.clone(), workspace_id);
        match self.handles.lock().unwrap().get_mut(&agent_id) {
            Some(h) => h.wake_listener = Some(listener),
            None => listener.abort(),
        }
        Ok(())
    }

    /// Build the generated `--mcp-config` (auggie `{ mcpServers }` shape) from
    /// the normalized spawn server set ([`Self::normalized_mcp_servers`]).
    async fn generate_mcp_config(&self, bridge: &McpBridge) -> Result<serde_json::Value> {
        let servers = self.normalized_mcp_servers(bridge.connect_addr()).await?;
        Ok(to_auggie_mcp_config(&servers))
    }

    /// Serialize the same normalized spawn server set as the OpenCode config
    /// `mcp` block, merged into `OPENCODE_CONFIG_CONTENT` at spawn for
    /// env-config providers. The bridge entry points at the same endpoint the
    /// auggie `--mcp-config` path uses.
    async fn opencode_env_mcp_config(&self, connect_addr: String) -> Result<String> {
        let servers = self.normalized_mcp_servers(connect_addr).await?;
        serde_json::to_string(&to_opencode_mcp_config(&servers))
            .map_err(|e| Error::Internal(format!("serialize opencode mcp config failed: {e}")))
    }

    /// Normalized MCP server set for a spawn: the `workspace-mcp` server is
    /// the `intentd mcp-bridge --connect <addr>` subcommand, with the user's
    /// `mcp.servers` catalog merged in and the safe baseline env injected
    /// across every stdio entry (§6.8, §18.4). Mirrors the FE
    /// `mergeUserMcpServersWithAuth` path: honours the `mcp.enableUserServers`
    /// gate, filters out globally-disabled servers, and — for http/sse
    /// transports — injects an `Authorization` header from the persisted OAuth
    /// token bag when the catalog entry does not already set one.
    /// `workspace-mcp` is reserved and never overridden.
    async fn normalized_mcp_servers(&self, connect_addr: String) -> Result<NormalizedMcpServers> {
        let mut servers = NormalizedMcpServers::new();
        // A whitespace-containing bridge path breaks provider launchers that
        // shell-split the stdio command (monorepo#1049): emit the basename and
        // a PATH override (parent dir + inherited PATH) instead.
        let (command, path_override) = normalize_spaced_bridge_command(
            &self.mcp_bridge_exe,
            std::env::var_os("PATH").as_deref(),
        );
        let mut env = EnvMap::new();
        if let Some(path) = path_override {
            env.insert("PATH".to_string(), path);
        }
        servers.insert(
            "workspace-mcp".to_string(),
            NormalizedMcpServer::Stdio {
                command,
                args: vec![
                    "mcp-bridge".to_string(),
                    "--connect".to_string(),
                    connect_addr,
                ],
                env,
            },
        );
        self.merge_user_mcp_servers(&mut servers).await?;
        let baseline = build_baseline_mcp_env_from_process();
        Ok(apply_baseline_env_to_stdio_servers(&servers, &baseline))
    }

    /// Fold user-configured MCP servers (sensitive `mcp.servers` secret) into
    /// `out`, honouring the `mcp.enableUserServers` gate and the global
    /// `mcp.disabledServers` list, and injecting an `Authorization` header from
    /// the persisted OAuth bag on http/sse entries when the catalog does not
    /// already set one. Any config that collides with a reserved built-in name
    /// (e.g. `workspace-mcp`) is skipped so the bridge cannot be shadowed.
    async fn merge_user_mcp_servers(&self, out: &mut NormalizedMcpServers) -> Result<()> {
        let settings = self.services.effective_settings();
        if !crate::mcp_servers::enable_user_servers(&settings) {
            return Ok(());
        }
        let configs = crate::mcp_servers::read_configs(&self.services.secrets).await;
        if configs.is_empty() {
            return Ok(());
        }
        let disabled = crate::mcp_servers::disabled_servers(&settings);
        let disabled: HashSet<&str> = disabled.iter().map(String::as_str).collect();

        let mut reshaped = serde_json::Map::new();
        for (id, cfg) in &configs {
            let Some(obj) = cfg.as_object() else { continue };
            if !obj.get("enabled").and_then(Value::as_bool).unwrap_or(false) {
                continue;
            }
            if disabled.contains(id.as_str()) {
                continue;
            }
            let name = obj
                .get("name")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(id.as_str())
                .to_string();
            if out.contains_key(&name) {
                tracing::debug!(server = %name, "user MCP server collides with reserved name; skipping");
                continue;
            }
            let Some(entry) = self.reshape_user_mcp_config(id, obj).await? else {
                continue;
            };
            reshaped.insert(name, entry);
        }
        if reshaped.is_empty() {
            return Ok(());
        }
        let normalized = normalize_mcp_servers(&Value::Object(reshaped));
        for (name, server) in normalized {
            out.entry(name).or_insert(server);
        }
        Ok(())
    }

    /// Reshape one `mcp.servers` entry into the shape [`normalize_mcp_servers`]
    /// expects — stdio entries stay untouched (`command`/`args`/`env`), remote
    /// entries get a `type` tag plus an `Authorization` header sourced from the
    /// persisted OAuth bag when the config does not already set one. Returns
    /// `None` for malformed entries (missing `command`/`url`) so they drop out
    /// of the merge silently.
    async fn reshape_user_mcp_config(
        &self,
        id: &str,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<Option<Value>> {
        let transport = obj
            .get("transport")
            .and_then(Value::as_str)
            .unwrap_or("stdio");
        let mut out = serde_json::Map::new();
        match transport {
            "http" | "sse" => {
                let Some(url) = obj
                    .get("url")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                else {
                    return Ok(None);
                };
                out.insert("type".into(), Value::String(transport.to_string()));
                out.insert("url".into(), Value::String(url.to_string()));
                let mut headers = obj
                    .get("headers")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                let has_auth = headers
                    .keys()
                    .any(|k| k.eq_ignore_ascii_case("authorization"));
                if !has_auth {
                    if let Some(auth) = self.oauth_authorization_header(id).await? {
                        headers.insert("Authorization".to_string(), Value::String(auth));
                    }
                }
                if !headers.is_empty() {
                    out.insert("headers".into(), Value::Object(headers));
                }
            }
            _ => {
                let Some(command) = obj
                    .get("command")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                else {
                    return Ok(None);
                };
                out.insert("command".into(), Value::String(command.to_string()));
                if let Some(a) = obj.get("args") {
                    out.insert("args".into(), a.clone());
                }
                if let Some(e) = obj.get("env") {
                    out.insert("env".into(), e.clone());
                }
            }
        }
        Ok(Some(Value::Object(out)))
    }

    /// Build the `Authorization: <token_type> <access_token>` header value from
    /// the persisted OAuth bag for `server_id`, or `None` when no bag is
    /// stored / the bag is malformed / `access_token` is missing. `token_type`
    /// defaults to `Bearer` and is title-cased so a bag storing the RFC 6749
    /// lower-case `bearer` still produces the conventional header form.
    async fn oauth_authorization_header(&self, server_id: &str) -> Result<Option<String>> {
        let Some(raw) = self.services.store.get_mcp_oauth_token(server_id).await? else {
            return Ok(None);
        };
        let Ok(bag) = serde_json::from_str::<Value>(&raw) else {
            return Ok(None);
        };
        let Some(access) = bag
            .get("access_token")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let token_type = bag
            .get("token_type")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or("Bearer");
        Ok(Some(format!("{} {}", title_case_ascii(token_type), access)))
    }

    /// Complete the connection handshake and establish an ACP session for a
    /// spawned agent. The agent→BE MCP server is delivered per the provider's
    /// mechanism: out-of-band via the generated `--mcp-config` (auggie) or env
    /// config (opencode) — those sessions carry no `mcpServers` — or in the
    /// `session/new` / `session/load` `mcpServers` field for providers with
    /// `supports_session_mcp_servers` (claude-code, codex, droid, grok), using the
    /// server list `create_agent` stashed on the handle. On a daemon respawn
    /// the agent may already have a persisted `acpSessionId`:
    ///
    /// 1. Resume it via `session/load` when the agent advertised `loadSession` —
    ///    the agent keeps its prior context, so no history resend is needed.
    /// 2. Otherwise (no `loadSession`, or `session/load` failed) fall back to a
    ///    fresh `session/new` that REPLACES the lost id (relaxing the write-once
    ///    invariant only here) and flag the agent so the next turn resends the
    ///    prior conversation history as `<supervisor>` XML.
    /// 3. With no persisted id (a brand-new agent) open a first session and
    ///    persist it write-once.
    ///
    /// Returns the `acpSessionId` to drive [`AgentManager::run_turn`] (§6.5).
    pub async fn start_session(
        &self,
        agent_id: &AgentId,
        cwd: PathBuf,
        provider: &ProviderConfig,
    ) -> Result<String> {
        let (conn, session_mcp_servers, wake_gate) = {
            let map = self.handles.lock().unwrap();
            let handle = map
                .get(agent_id)
                .ok_or_else(|| Error::NotFound(format!("agent {agent_id}")))?;
            (
                handle.connection.clone(),
                handle.session_mcp_servers.clone(),
                handle.wake_gate.clone(),
            )
        };
        // Pause the idle wake listener for the whole session-open (monorepo#855):
        // a `session/load` replay burst must be drained by the resume path
        // below, never opened as an implicit harness-wake turn. The guard
        // lowers the gate on every exit path.
        wake_gate.fetch_add(1, AtomicOrdering::SeqCst);
        let _wake_gate_guard = WakeGateGuard(wake_gate);
        // Load the agent session record once and reuse both `workspace_id` (for
        // the pre-handshake status hint) and `acp_session_id` (for the resume
        // branch decision below) from the same struct.
        let session_record = self.services.store.get_agent_session(agent_id).await?;
        // Pre-first-token turn-startup hint: the ACP `initialize` handshake is
        // about to run for this agent (STAT-1 / PROTOCOL §7). The status payload
        // carries `workspaceId` (the FE routes hints per-agent but callers key
        // the timeline on it).
        self.services
            .publish_status_event(
                &session_record.workspace_id,
                agent_id,
                "init",
                "Initializing protocol\u{2026}",
                "info",
            )
            .await;
        let handshake = handshake(conn.as_ref(), provider)
            .await
            .map_err(|e| Error::Internal(format!("handshake failed: {e}")))?;

        // Per the ACP schema, http/sse `McpServer` entries are only valid when
        // the agent advertised `mcpCapabilities.http`/`sse` in `initialize` —
        // an agent that didn't may reject the whole `session/new`. Filter here
        // (post-handshake) so a user-configured http/sse catalog entry can't
        // break agent spawn; stdio (the workspace bridge) is mandatory per
        // spec and always passes.
        let mcp_caps = &handshake.initialize.agent_capabilities.mcp_capabilities;
        let session_mcp_servers: Vec<McpServer> = session_mcp_servers
            .into_iter()
            .filter(|s| match s {
                McpServer::Stdio(_) => true,
                McpServer::Http(_) => mcp_caps.http,
                McpServer::Sse(_) => mcp_caps.sse,
                _ => false,
            })
            .collect();

        // The persisted model (bare part of a compound id) feeds the
        // post-session model application for providers with no CLI model
        // flag — `session/set_model` (grok) or `session/set_config_option`
        // (claude-code) — see `maybe_apply_session_model`.
        let stored_model = session_record.model.clone();

        // The persisted `reasoningEffort` (PROTOCOL §5.5) feeds the generic
        // post-session effort application through whatever `thought_level`
        // config option the provider advertised — see
        // `install_and_apply_thought_level`.
        let stored_effort = session_record.reasoning_effort.clone();

        // The persisted id (if any) decides the no-resume branch: a brand-new
        // agent (no id) opens a first session; an agent with a lost id recreates
        // (CAS-replacing exactly this id) and resends history.
        let stored_id = session_record.acp_session_id;

        // Forced recreate (`agent.editAndRegenerate`): the transcript was
        // truncated, so resuming the provider session would retain the
        // truncated turns in its context. Skip the `session/load` attempt and
        // fall through to the recreate/new branches, which open a fresh
        // `session/new` and replay the truncated history as `<supervisor>`
        // XML. Peeked here and consumed only after a fresh session is
        // successfully opened, so a failed spawn attempt retries with the
        // flag still armed instead of resuming the stale session.
        let forced = self.force_recreate.lock().unwrap().contains(agent_id);

        // 1) Try to resume the persisted session (gated on stored id + capability).
        match if forced {
            Ok(None)
        } else {
            self.services
                .resume_acp_session(
                    conn.as_ref(),
                    &handshake.initialize,
                    agent_id,
                    cwd.clone(),
                    session_mcp_servers.clone(),
                )
                .await
        } {
            Ok(Some(opened)) => {
                // `session/load` replays the prior conversation as a buffered
                // `session/update` burst; discard it before the first turn so it
                // is neither re-published as events nor re-accumulated into the
                // transcript (parity with TS's "no active streaming handler ⇒
                // drop"). Only the resume path needs this settle-window drain —
                // new/recreate sessions have no buffered replay.
                let notes = self
                    .handles
                    .lock()
                    .unwrap()
                    .get(agent_id)
                    .map(|h| h.notifications.clone());
                if let Some(notes) = notes {
                    let mut guard = notes.lock().await;
                    Services::drain_replay_notifications(&mut guard).await;
                }
                self.maybe_bypass_permissions(
                    conn.as_ref(),
                    provider,
                    &opened.session_id,
                    opened.modes.as_ref(),
                )
                .await;
                Self::maybe_apply_session_model(
                    conn.as_ref(),
                    provider,
                    &opened.session_id,
                    stored_model.as_deref(),
                )
                .await;
                self.install_and_apply_thought_level(
                    conn.as_ref(),
                    agent_id,
                    &opened.session_id,
                    opened.thought_level.clone(),
                    stored_effort.as_deref(),
                )
                .await;
                return Ok(opened.session_id);
            }
            Ok(None) => {}
            // `session/load` was attempted but failed → fall through to recreate.
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "session/load failed; recreating");
            }
        }

        // 2) Resume impossible but a session existed → recreate + flag for resend.
        // The fresh `session/new` runs on the child just spawned for this turn
        // (the lost session's child, if any, was already reaped before the
        // respawn — see `create_agent`'s defensive teardown), so no streamer from
        // the old session can append to the agentId-keyed transcript. The CAS
        // replace keeps the id canonical, swapping only the exact id we failed to
        // load.
        if let Some(expected_old) = stored_id {
            let opened = self
                .services
                .recreate_acp_session(
                    conn.as_ref(),
                    agent_id,
                    &expected_old,
                    cwd,
                    session_mcp_servers.clone(),
                )
                .await?;
            self.force_recreate.lock().unwrap().remove(agent_id);
            self.recreated.lock().unwrap().insert(agent_id.clone());
            self.arm_first_turn_prepend(agent_id, provider);
            self.maybe_bypass_permissions(
                conn.as_ref(),
                provider,
                &opened.session_id,
                opened.modes.as_ref(),
            )
            .await;
            Self::maybe_apply_session_model(
                conn.as_ref(),
                provider,
                &opened.session_id,
                stored_model.as_deref(),
            )
            .await;
            self.install_and_apply_thought_level(
                conn.as_ref(),
                agent_id,
                &opened.session_id,
                opened.thought_level.clone(),
                stored_effort.as_deref(),
            )
            .await;
            return Ok(opened.session_id);
        }

        // 3) Brand-new agent → open and persist the first session (write-once).
        let opened = self
            .services
            .open_acp_session(conn.as_ref(), agent_id, cwd, session_mcp_servers)
            .await?;
        self.force_recreate.lock().unwrap().remove(agent_id);
        self.arm_first_turn_prepend(agent_id, provider);
        self.maybe_bypass_permissions(
            conn.as_ref(),
            provider,
            &opened.session_id,
            opened.modes.as_ref(),
        )
        .await;
        Self::maybe_apply_session_model(
            conn.as_ref(),
            provider,
            &opened.session_id,
            stored_model.as_deref(),
        )
        .await;
        self.install_and_apply_thought_level(
            conn.as_ref(),
            agent_id,
            &opened.session_id,
            opened.thought_level.clone(),
            stored_effort.as_deref(),
        )
        .await;
        Ok(opened.session_id)
    }

    /// Record the `thought_level` selector a freshly opened/resumed session
    /// advertised on the live handle and apply the session's stored
    /// `reasoningEffort` through it (PROTOCOL §5.5). Generic by construction:
    /// the config id comes from the adapter's own `configOptions`
    /// (claude-agent-acp `effort`, codex-acp `reasoning_effort`), so no
    /// provider capability flag is needed and a provider that advertises no
    /// such option silently ignores the field. The selector's surfaced levels
    /// are persisted inside the open/recreate/resume fns themselves — where
    /// the CAS outcome is known, so a lost CAS never clears them (see
    /// [`Services::persist_session_effort_levels`]). Best-effort — a
    /// rejected call is logged and never fails session startup.
    async fn install_and_apply_thought_level(
        &self,
        conn: &Connection,
        agent_id: &AgentId,
        acp_session_id: &str,
        thought_level: Option<ThoughtLevelOption>,
        stored_effort: Option<&str>,
    ) {
        if let Some(handle) = self.handles.lock().unwrap().get_mut(agent_id) {
            handle.thought_level = thought_level;
        }
        self.apply_thought_level(conn, agent_id, acp_session_id, stored_effort)
            .await;
    }

    /// Send the session's `reasoningEffort` to the provider through the
    /// `thought_level` config option discovered at session open, then update
    /// the handle's tracked `current_value` so the next call is a no-op until
    /// the effort actually changes. A CLEARED effort restores the provider's
    /// own default — the value the adapter reported at session open — so the
    /// clear takes effect on the live session rather than leaving the last
    /// applied level in place. No-op when: the provider advertised no such
    /// option, the adapter is already on that value, or the value is not among
    /// the ones the select accepts (a stale effort from another provider's
    /// vocabulary must not be sent). Matching against the advertised values is
    /// case-insensitive — the stored level keeps the caller's spelling
    /// (validation is case-insensitive too), so the ADAPTER's spelling is what
    /// gets sent. Failures are logged at WARN — the provider keeps its current
    /// effort.
    async fn apply_thought_level(
        &self,
        conn: &Connection,
        agent_id: &AgentId,
        acp_session_id: &str,
        stored_effort: Option<&str>,
    ) {
        let requested = stored_effort.map(str::trim).filter(|e| !e.is_empty());
        let Some((config_id, value)) = ({
            let handles = self.handles.lock().unwrap();
            handles.get(agent_id).and_then(|h| {
                h.thought_level.as_ref().and_then(|t| {
                    // The adapter's own spelling of the requested level; with
                    // no advertised values the stored spelling is all we have.
                    // A cleared effort targets the provider's opening default.
                    let value = match requested {
                        Some(effort) => {
                            match t.values.iter().find(|v| v.eq_ignore_ascii_case(effort)) {
                                Some(v) => v.clone(),
                                None if t.values.is_empty() => effort.to_string(),
                                None => return None,
                            }
                        }
                        None => t.initial_value.clone(),
                    };
                    (!value.is_empty() && !t.current_value.eq_ignore_ascii_case(&value))
                        .then(|| (t.config_id.clone(), value))
                })
            })
        }) else {
            return;
        };
        let effort = value.as_str();
        match intent_acp::session::set_session_config_option(
            conn,
            acp_session_id,
            &config_id,
            effort,
        )
        .await
        {
            Ok(()) => {
                tracing::debug!(
                    agent = %agent_id,
                    session_id = acp_session_id,
                    config_id = %config_id,
                    effort = %effort,
                    "session/set_config_option applied reasoning effort"
                );
                if let Some(handle) = self.handles.lock().unwrap().get_mut(agent_id) {
                    if let Some(t) = handle.thought_level.as_mut() {
                        t.current_value = effort.to_string();
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    session_id = acp_session_id,
                    config_id = %config_id,
                    effort = %effort,
                    error = %e,
                    "session/set_config_option failed; provider keeps its current reasoning effort"
                );
            }
        }
    }

    /// Best-effort post-session model application, gated per provider
    /// capability (parity with the reference acp-provider): `session/set_model`
    /// for providers whose ACP subcommand has no CLI model flag
    /// (`supports_set_model`; grok today), and
    /// `session/set_config_option { configId: "model" }` for providers that
    /// expose the model as a session config option
    /// (`supports_config_option_model`; claude-code today). Compound ids are
    /// honored only when their provider prefix matches the running provider (a
    /// stale id from a pre-spawn provider switch must not be sent); bare ids
    /// are treated as provider-local. The `default` sentinel and empty ids are
    /// no-ops. Failures are logged at WARN and never fail session startup.
    async fn maybe_apply_session_model(
        conn: &Connection,
        provider: &ProviderConfig,
        acp_session_id: &str,
        stored_model: Option<&str>,
    ) {
        if let Some(model_id) = Self::set_model_target(provider, stored_model) {
            match intent_acp::session::set_session_model(conn, acp_session_id, model_id).await {
                Ok(()) => {
                    tracing::debug!(
                        provider = provider.id,
                        session_id = acp_session_id,
                        model = %model_id,
                        "session/set_model accepted"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        provider = provider.id,
                        session_id = acp_session_id,
                        model = %model_id,
                        error = %e,
                        "session/set_model failed; provider keeps its default model"
                    );
                }
            }
        }
        if let Some(model_id) = Self::config_option_model_target(provider, stored_model) {
            match intent_acp::session::set_session_config_option(
                conn,
                acp_session_id,
                "model",
                model_id,
            )
            .await
            {
                Ok(()) => {
                    tracing::debug!(
                        provider = provider.id,
                        session_id = acp_session_id,
                        model = %model_id,
                        "session/set_config_option accepted"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        provider = provider.id,
                        session_id = acp_session_id,
                        model = %model_id,
                        error = %e,
                        "session/set_config_option failed; provider keeps its default model"
                    );
                }
            }
        }
    }

    /// Resolve the model id `maybe_apply_session_model` should send via
    /// `session/set_model`, or `None` when the call should not be issued:
    /// providers without `supports_set_model`, or ids rejected by
    /// [`Self::provider_local_model_target`].
    fn set_model_target<'m>(
        provider: &ProviderConfig,
        stored_model: Option<&'m str>,
    ) -> Option<&'m str> {
        if !provider.supports_set_model {
            return None;
        }
        Self::provider_local_model_target(provider, stored_model)
    }

    /// Resolve the model id `maybe_apply_session_model` should send via
    /// `session/set_config_option { configId: "model" }`, or `None` when the
    /// call should not be issued: providers without
    /// `supports_config_option_model`, or ids rejected by
    /// [`Self::provider_local_model_target`].
    fn config_option_model_target<'m>(
        provider: &ProviderConfig,
        stored_model: Option<&'m str>,
    ) -> Option<&'m str> {
        if !provider.supports_config_option_model {
            return None;
        }
        Self::provider_local_model_target(provider, stored_model)
    }

    /// Shared gating for the post-session model-application paths: `None` for
    /// absent/empty models, the `default` sentinel, and compound ids whose
    /// provider prefix does not match the running provider (a stale id from a
    /// pre-spawn provider switch must not be sent). Bare ids are treated as
    /// provider-local; compound ids are stripped to their bare part.
    /// Whitespace-bearing ids are also `None`: real provider option ids never
    /// contain spaces, so a display name persisted onto `model` by the
    /// pre-monorepo#1534 effective-model resolution (legacy rows, e.g.
    /// `claude-code:Opus 4.8`) must not be sent back as a
    /// `session/set_model` / `session/set_config_option` value.
    fn provider_local_model_target<'m>(
        provider: &ProviderConfig,
        stored_model: Option<&'m str>,
    ) -> Option<&'m str> {
        let model = stored_model?;
        let model_id = match model.split_once(':') {
            Some((prefix, bare)) if prefix == provider.id => bare,
            Some(_) => return None,
            None => model,
        };
        if model_id.is_empty()
            || model_id.eq_ignore_ascii_case("default")
            || model_id.contains(char::is_whitespace)
        {
            return None;
        }
        Some(model_id)
    }

    /// Under the shipped `AllowAll` policy, best-effort ask the provider to run
    /// in a permissive mode via `session/set_mode` (parity with the TS
    /// acp-provider). The mode id is picked by
    /// [`try_bypass_permissions_mode`] from the modes the provider actually
    /// advertised in `session/new` / `session/load`, so agents that don't
    /// offer a bypass-equivalent (auggie today) are left alone rather than
    /// triggering a `-32602`; every other policy is a no-op so Interactive /
    /// `AutoByRisk` / `DenyAll` decisions stay authoritative.
    async fn maybe_bypass_permissions(
        &self,
        conn: &Connection,
        provider: &ProviderConfig,
        acp_session_id: &str,
        modes: Option<&SessionModeState>,
    ) {
        if self.policy != PermissionPolicy::AllowAll {
            return;
        }
        try_bypass_permissions_mode(conn, provider, acp_session_id, modes).await;
    }

    /// Take (clear) the recreate flag for `agent_id`: `true` when the agent's ACP
    /// session was recreated by the resume-impossible fallback since the last
    /// turn, meaning the next prompt must resend the prior conversation history.
    fn take_recreated(&self, agent_id: &AgentId) -> bool {
        self.recreated.lock().unwrap().remove(agent_id)
    }

    /// Arm the FirstTurnPrepend flag for `agent_id` when the provider has no
    /// native system-prompt mechanism (§18.1 fallback). Called only from the
    /// fresh-session branches of [`AgentManager::start_session`] (`session/new`
    /// for a brand-new agent, or the resume-impossible recreate) — never on a
    /// `session/load` resume, where the provider retained the prior context
    /// that already carried the prompt.
    fn arm_first_turn_prepend(&self, agent_id: &AgentId, provider: &ProviderConfig) {
        if provider.injection_mechanism == InjectionMechanism::FirstTurnPrepend {
            self.prepend_pending
                .lock()
                .unwrap()
                .insert(agent_id.clone());
        }
    }

    /// Compute the `<system>`-wrapped assembled system prompt for the
    /// FirstTurnPrepend fallback, or `None` when nothing is pending. The
    /// prompt text comes from the session's persisted `system_prompt`
    /// (written by [`AgentManager::create_agent`] at spawn time from
    /// `assemble_system_prompt`). The pending flag is consumed only on a
    /// definitive outcome (prompt built, or session provably has no usable
    /// prompt); a transient store error keeps it armed so the NEXT turn
    /// retries instead of silently dropping the system prompt for the whole
    /// session.
    async fn build_first_turn_prepend(&self, agent_id: &AgentId) -> Option<String> {
        if !self.prepend_pending.lock().unwrap().contains(agent_id) {
            return None;
        }
        let session = match self.services.store.get_agent_session(agent_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "first-turn prepend: session lookup failed; keeping flag armed for retry"
                );
                return None;
            }
        };
        self.prepend_pending.lock().unwrap().remove(agent_id);
        let prompt = session.system_prompt?;
        let prompt = prompt.trim();
        if prompt.is_empty() {
            return None;
        }
        Some(crate::harness::latest().first_turn_prepend_block(prompt))
    }
    /// Compute the fire-once workspace-naming instruction for the outbound
    /// prompt, or `None` when it should be omitted. Ported from the reference
    /// `agent-backend-handler.service.ts` (`namingInstructions` block):
    ///
    /// * Fires only on the agent's **first** turn — detected by the absence of
    ///   any prior `assistant` message in the persisted transcript.
    /// * Fires only when the workspace lookup succeeds AND the current title
    ///   is empty/whitespace OR still shaped like an auto-generated slug
    ///   ([`intent_core::slug::is_workspace_slug`]).
    /// * Names the concrete daemon tool the agent must call — spelled the way
    ///   the session's provider will actually surface it (see
    ///   [`workspace_naming_tool_reference`]) — not the FE `workspace_api`
    ///   JS surface (which daemon-spawned agents do not have).
    ///
    /// The agent-rename half of the reference block is intentionally SKIPPED:
    /// the daemon currently exposes no `set_agent_name` tool. Restore that
    /// branch once such a tool exists.
    async fn build_workspace_naming_instruction(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Option<String> {
        let messages = self
            .services
            .store
            .get_agent_messages(agent_id, None)
            .await
            .ok()?;
        if messages.iter().any(|m| m.role == "assistant") {
            return None;
        }
        let workspace = self.services.store.get_workspace(workspace_id).await.ok()?;
        let title = workspace.title.trim();
        let needs_rename = title.is_empty() || is_workspace_slug(title);
        if !needs_rename {
            return None;
        }
        // Spell the rename tool the way this session's provider surfaces it;
        // a failed session lookup falls back to the generic phrasing.
        let configured_default =
            crate::agent_session::derived_default_provider(&self.services.effective_settings());
        let tool_ref = match self.services.store.get_agent_session(agent_id).await {
            Ok(s) => workspace_naming_tool_reference(&session_provider_id(
                &s,
                configured_default.as_deref(),
            )),
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "naming nudge: session lookup failed; using generic tool phrasing"
                );
                GENERIC_NAMING_TOOL_REFERENCE
            }
        };
        Some(crate::harness::latest().naming_nudge(tool_ref))
    }

    /// Build the prompt blocks for an agent's next turn. Normally just the user
    /// `content`; but when the ACP session was recreated (the resume-impossible
    /// fallback), prepend the prior conversation history as `<supervisor>` XML so
    /// the fresh session has context, then clear the flag (parity: TS
    /// `sessionWasRecreated` → `formatHistoryAsXml`). The just-persisted current
    /// user message is excluded from the rendered history.
    ///
    /// When `options.stdin_context` is set the prompt is prefixed with a
    /// `Context:\n<stdin>\n\n---\n\n` block, reference-parity with
    /// `acp-provider.ts`; other [`TurnOptions`] fields are reserved for
    /// downstream note-image / context-reference resolution.
    ///
    /// When `options.prepend_content` is set (STAB-114 / monorepo#1014
    /// combined delivery), the preempted message's text is delivered ahead of
    /// `content` in the same prompt body, so the interrupt turn honors both
    /// messages in order.
    async fn build_turn_prompt(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        content: &str,
        options: &TurnOptions,
    ) -> Vec<ContentBlock> {
        // Combined interrupt delivery (monorepo#1014): the preempted
        // message's text precedes the interrupt message's own content.
        // EXCEPT when the ACP session was recreated: `build_turn_body`'s
        // history replay renders every row before the last user row — which
        // includes the already-persisted preempted user row — so injecting
        // the text again here would deliver it twice. The flag is peeked
        // (not consumed); `build_turn_body` still takes it below. The
        // prepend ATTACHMENTS are unaffected: the history XML is text-only,
        // so `append_attachment_blocks` must still emit them.
        let history_covers_prepend = self.recreated.lock().unwrap().contains(agent_id);
        let combined = match options.prepend_content.as_deref() {
            Some(orig) if !orig.is_empty() && !history_covers_prepend => {
                format!("{orig}\n\n{content}")
            }
            _ => content.to_string(),
        };
        // Resolve each envelope layer's data (gating stays here); the
        // harness owns the wording and the layering order.
        //
        // Role reminder is rebuilt every turn (interval = 1, port of
        // acp-provider.ts) and prepended to the outbound prompt for specialist
        // agents; absent for non-specialist agents. Because it fires every turn
        // it also covers the session-recreated case handled by `build_turn_body`.
        let reminder = self.services.agent_role_reminder(agent_id).await;
        let body = self.build_turn_body(agent_id, &combined).await;
        // Fire-once workspace-naming instruction (port of
        // `agent-backend-handler.service.ts` `namingInstructions`): on the
        // first turn of an agent in a still-untitled / slug-titled workspace,
        // a `<system>` block asking the agent to set the workspace title as
        // its first action. Never mutates the persisted user message;
        // agent-rename half is deferred until the daemon exposes a
        // `set_agent_name` tool.
        let naming = self
            .build_workspace_naming_instruction(agent_id, workspace_id)
            .await;
        // `stdinContext` renders verbatim as a `Context:` block; the
        // trailing separator matches the reference `acp-provider.ts` so
        // downstream consumers see the same shape whether the prompt
        // originates from the daemon or the legacy Electron main path.
        // When `stdinContext` is absent/empty we synthesise one from
        // `contextReferences` (port of the FE reference builder in
        // `agent-backend-handler.service.ts`); an explicit `stdinContext`
        // always wins.
        let synthesised = match options.stdin_context.as_deref() {
            Some(ctx) if !ctx.is_empty() => None,
            _ => build_stdin_context_from_context_references(options.context_references.as_ref()),
        };
        let stdin_context = match options.stdin_context.as_deref() {
            Some(ctx) if !ctx.is_empty() => Some(ctx),
            _ => synthesised.as_deref().filter(|ctx| !ctx.is_empty()),
        };
        // Per-turn agent state snapshot (`current ws.agent.snapshot() =>
        // {...}`): the outermost RECURRING per-turn decoration — before
        // context/naming/reminder, after only the fire-once FirstTurnPrepend.
        // Rebuilt every turn for ALL agents (specialist and
        // non-specialist, unlike the role reminder) and never persisted.
        // `agent_state_snapshot_line` reads `agentFeatures.stateSnapshot`
        // LIVE and returns `None` when the toggle is off or the snapshot is
        // trivial (all counts zero, no pending attention), leaving the
        // prompt byte-identical to pre-feature output.
        let snapshot_line = self.services.agent_state_snapshot_line(agent_id).await;
        // FirstTurnPrepend fallback (§18.1): for providers with no (usable)
        // native system-prompt mechanism (codex, cortex, pi, grok, mock), the
        // assembled system prompt is delivered as the OUTERMOST `<system>`
        // block on the first prompt of each fresh ACP session — before
        // context/naming/reminder/user content. Armed by `start_session` on
        // `session/new` (brand-new or recreate, never `session/load` resume)
        // and consumed here so it fires exactly once per fresh session.
        let prepend = self.build_first_turn_prepend(agent_id).await;
        let prompt_text =
            crate::harness::latest().compose_turn_prompt(&crate::harness::TurnEnvelopeParams {
                first_turn_prepend: prepend.as_deref(),
                snapshot_line: snapshot_line.as_deref(),
                stdin_context,
                naming_nudge: naming.as_deref(),
                role_reminder: reminder.as_deref(),
                body: &body,
            });
        let mut blocks = text_prompt(&prompt_text);
        append_attachment_blocks(&mut blocks, options);
        // Resolve `noteIds` to `workspace-asset://` image content blocks
        // (Fidelity B, PROTOCOL §5.5): each note is scanned for markdown
        // image references whose URL is a workspace-asset in the current
        // workspace; the referenced bytes are loaded and appended as ACP
        // `image` content blocks. A single system text block is added when
        // any images are resolved so the agent knows they are inlined for
        // direct viewing (parity with the FE notice).
        if let Some(ids_json) = options.note_ids.as_ref() {
            let ids: Vec<String> = ids_json
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            if !ids.is_empty() {
                let images = self
                    .services
                    .load_note_image_blocks(workspace_id, &ids)
                    .await;
                if !images.is_empty() {
                    for (data, mime) in &images {
                        if let Ok(img) = serde_json::from_value::<ContentBlock>(json!({
                            "type": "image",
                            "data": data,
                            "mimeType": mime,
                        })) {
                            blocks.push(img);
                        }
                    }
                    let notice = note_images_notice(images.len());
                    blocks.extend(text_prompt(&notice));
                }
            }
        }
        blocks
    }

    /// Build the user-turn body: normally just `content`, but when the ACP
    /// session was recreated (the resume-impossible fallback), prepend the prior
    /// conversation history as `<supervisor>` XML so the fresh session has
    /// context, then clear the flag (parity: TS `sessionWasRecreated` →
    /// `formatHistoryAsXml`). The just-persisted current user message is excluded
    /// from the rendered history.
    async fn build_turn_body(&self, agent_id: &AgentId, content: &str) -> String {
        if !self.take_recreated(agent_id) {
            return content.to_string();
        }
        let messages = self
            .services
            .store
            .get_agent_messages(agent_id, None)
            .await
            .unwrap_or_default();
        // The current user message was already appended → render everything
        // BEFORE it. Truncating at the last user row (not just the last row)
        // keeps the current message out of the replay even when turn-start
        // appends trail it (the `model_changed` system notice lands after the
        // user row and before this render); with no user row fall back to
        // dropping the last message.
        let prior = match messages.iter().rposition(|m| m.role == "user") {
            Some(idx) => &messages[..idx],
            None => messages.split_last().map(|(_, rest)| rest).unwrap_or(&[]),
        };
        if prior.is_empty() {
            return content.to_string();
        }
        let history_xml =
            crate::history_xml::format_history_as_xml(prior, crate::history_xml::MAX_HISTORY_CHARS);
        format!("{history_xml}\n\n{content}")
    }

    /// Drive one `session/prompt` turn for `agent_id`, marking it active for the
    /// duration so the registry never evicts a streaming process. Streams
    /// updates onto the event bus via the M3.4 router (`run_prompt_turn`).
    /// `turn_id` is the turn correlation id (monorepo#1022) stamped on the
    /// failure-arm `agent:failed`; bare callers (tests) may pass `None`.
    pub async fn run_turn(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        acp_session_id: &str,
        prompt: Vec<ContentBlock>,
        turn_id: Option<&str>,
    ) -> Result<StopReason> {
        let (conn, notes) = {
            let map = self.handles.lock().unwrap();
            let handle = map
                .get(agent_id)
                .ok_or_else(|| Error::NotFound(format!("agent {agent_id}")))?;
            (handle.connection.clone(), handle.notifications.clone())
        };
        self.registry.mark_active(agent_id);
        let mut guard = notes.lock().await;
        let result = self
            .services
            .run_prompt_turn(
                conn.as_ref(),
                &mut guard,
                agent_id,
                workspace_id,
                acp_session_id,
                prompt,
                turn_id,
            )
            .await;
        self.registry.mark_idle(agent_id);
        result
    }

    /// Stop one agent: abort its in-flight turn worker (interrupting the current
    /// stream), clear its busy flag, drop its handle, and deregister it. The
    /// child's whole process group is signalled (SIGTERM→SIGKILL) so no orphaned
    /// grandchildren linger. Returns whether a handle existed. This is the
    /// `agent.stop` / hard-cancel cancel semantics.
    pub async fn stop(&self, agent_id: &AgentId) -> bool {
        let (removed, child) = self.detach(agent_id).await;
        if let Some((child, spawn_pid)) = child {
            kill_child_tree(child, spawn_pid).await;
        }
        removed
    }

    /// Stop MANY agents under ONE shared grace window: detach each agent with
    /// the exact [`AgentManager::stop`] per-agent semantics (partial-turn
    /// flush, recreate/prepend flag cleanup, live-turn slot release, handle
    /// removal, deregistration), collect the detached children, and kill all
    /// process groups concurrently via [`kill_child_trees`] — total teardown
    /// stays ~one [`PROCESS_GROUP_TERM_GRACE`] period regardless of agent
    /// count, instead of N sequential SIGTERM→grace→SIGKILL cycles. This is
    /// the `workspace.delete` sweep (same detach-many → kill_child_trees
    /// pattern as `shutdown()`, minus the interrupted-session capture).
    ///
    /// NOTE: with exactly one agent this is NEARLY but not exactly
    /// [`AgentManager::stop`]: `kill_child_trees` adds a bounded
    /// [`KILL_SWEEP_REAP_GRACE`] post-SIGKILL reap window that
    /// `kill_child_tree` does not (a SIGTERM-ignoring single child pays up
    /// to ~grace + reap-grace and is properly `wait()`ed), and descendants
    /// are snapshotted via one batched `ps` (`descendant_pids_many`).
    ///
    /// Returns a [`TeardownFence`] that keeps the swept agents fenced off
    /// from the lazy-spawn paths until dropped: the caller must hold it
    /// across the store cascade that deletes the session rows, so a
    /// concurrent `agent.sendMessage` racing the teardown cannot respawn a
    /// child that would outlive its deleted session as a ghost process.
    #[must_use = "hold the fence until the swept agents' session rows are deleted"]
    pub async fn stop_many(&self, agent_ids: &[AgentId]) -> TeardownFence {
        // Arm the fence BEFORE the first detach: from here on, every lazy
        // spawn for a swept agent is refused (`ensure_started` fast-fail +
        // the `create_agent` install fence), so no replacement child can
        // slip in during the shared grace wait below.
        self.stopping
            .lock()
            .unwrap()
            .extend(agent_ids.iter().cloned());
        let fence = TeardownFence {
            stopping: Arc::clone(&self.stopping),
            ids: agent_ids.to_vec(),
        };
        let mut children = Vec::new();
        for id in agent_ids {
            let (_, child) = self.detach(id).await;
            if let Some(child) = child {
                children.push(child);
            }
        }
        if !children.is_empty() {
            kill_child_trees(children).await;
        }
        fence
    }

    /// Shared teardown body of [`AgentManager::stop`]: abort the worker, drop
    /// stale flags, settle the turn, remove the handle, deregister — and hand
    /// back the detached child (if any) so the caller decides how to kill it.
    /// `stop()` kills the single tree inline (SIGTERM→grace→SIGKILL);
    /// `shutdown()` collects every detached child and kills all process groups
    /// concurrently under ONE shared grace window.
    async fn detach(&self, agent_id: &AgentId) -> (bool, Option<(Child, Option<u32>)>) {
        self.detach_with_redelivery(agent_id, None, true).await
    }

    /// [`AgentManager::detach`] with an optional pre-derived zero-output
    /// stop-redelivery payload (intent-hq/monorepo#1757) to install in place
    /// of the default drop. `stop_with_redelivery_arm` (the spawn-window user
    /// stop) passes `Some`: the payload must be visible BEFORE `end_turn`
    /// releases the busy slot, because a concurrent send claims the slot the
    /// moment it frees and its worker spawn consumes the map right then —
    /// arming after the teardown (the previous shape) lost that race.
    ///
    /// `sync_store` controls the durable stop-redelivery mirror
    /// (intent-hq/monorepo#1899): `true` (every hard-stop path) writes the
    /// in-memory outcome through to `agent_stop_redelivery`; `false` (the
    /// graceful-shutdown sweep) leaves the persisted row untouched so an
    /// armed payload survives the restart and is rehydrated at next boot.
    async fn detach_with_redelivery(
        &self,
        agent_id: &AgentId,
        redelivery: Option<crate::agent_ops::QueuedPrepend>,
        sync_store: bool,
    ) -> (bool, Option<(Child, Option<u32>)>) {
        // Pin the live-turn slot BEFORE aborting the worker (the abort drops
        // LiveTurnGuard; the pin keeps the slot published until the flush
        // below persists the row — monorepo#2056), then flush the partial
        // in-flight assistant content AFTER the abort — same convention as the
        // graceful-shutdown flush. The flush re-reads the pinned slot, so an
        // update processed in the pin→abort gap is persisted too
        // (monorepo#2110). A worker append already in flight at abort time can
        // still land, but the `agent_message.id` PK keeps the outcome
        // convergent (exactly one row; the UNIQUE collision is absorbed inside
        // the flush). No-op when the slot is empty or was already flushed by a
        // caller (e.g. shutdown(), which flushes before delegating here).
        self.services.pin_live_turn(agent_id);
        if let Some(worker) = self.workers.lock().unwrap().remove(agent_id) {
            worker.abort();
        }
        self.services
            .flush_pinned_turn_on_interruption(agent_id, InterruptReason::AgentStopped, None)
            .await;
        // Drop any pending recreate/prepend flags: the next spawn re-decides
        // resume vs recreate from scratch, so stale flags must not survive a
        // teardown (a session/load resume must not fire a stale prepend).
        // The zero-output stop-redelivery payload (intent-hq/monorepo#1757)
        // is dropped on the same terms — a hard teardown's next session is
        // either recreated (history replay covers the stopped message) or the
        // agent is being deleted — UNLESS the caller pre-derived a payload
        // (the spawn-window user stop), which is installed here so it is
        // visible before `end_turn` frees the busy slot.
        self.recreated.lock().unwrap().remove(agent_id);
        self.prepend_pending.lock().unwrap().remove(agent_id);
        // Same staleness terms for the streaming path's persisted terminal-
        // error stash (monorepo#2050): the abort above may have landed between
        // `run_prompt_turn`'s stash and the terminal-failure handler's take,
        // and the orphaned context describes the aborted turn, not a future
        // failure.
        self.services.discard_pending_terminal_error(agent_id);
        {
            let mut armed = self.stop_redelivery.lock().unwrap();
            match redelivery {
                Some(payload) => {
                    armed.insert(agent_id.clone(), payload);
                }
                None => {
                    armed.remove(agent_id);
                }
            }
        }
        // Sync unconditionally (not only when the map changed): the durable
        // clear after a consume runs in the spawned worker task, which this
        // detach aborts — a hard stop landing in that gap sees no map entry
        // yet a stale row may survive in the store (the DELETE is a cheap
        // no-op when no row exists).
        if sync_store {
            self.sync_stop_redelivery(agent_id).await;
        }
        self.end_turn(agent_id).await;
        let handle = self.handles.lock().unwrap().remove(agent_id);
        let removed = handle.is_some();
        let child = handle.and_then(|mut h| {
            let spawn_pid = h.child_pid;
            h._child.take().map(|c| (c, spawn_pid))
        });
        self.registry.deregister(agent_id);
        (removed, child)
    }

    /// Interrupt one agent's in-flight turn WITHOUT killing its child — the TS
    /// `agent.stop` keep-alive semantics (`ConsolidatedBackend.backendStop` with
    /// `killProcess: false` → `provider.interrupt()`): cancel the current turn
    /// over the wire (`session/cancel`), abort the draining worker, release the
    /// in-flight slot, mark the process idle, and emit the single terminal
    /// `agent:stream:end` (the aborted worker can no longer emit it). The child +
    /// ACP session stay alive, so a follow-up `agent.sendMessage` resumes the
    /// same session. Falls back to the hard [`AgentManager::stop`] kill ONLY when
    /// no live session is available to interrupt (no handle / no `acpSessionId`),
    /// the Rust analog of TS reserving the kill for `killProcess: true`. Returns
    /// whether an agent was found.
    pub async fn interrupt(&self, agent_id: &AgentId) -> bool {
        self.interrupt_inner(agent_id, InterruptReason::UserStop, None)
            .await
            .0
    }

    /// Shared body of [`AgentManager::interrupt`], parameterized on the
    /// interruption cause. `reason` (+ `interrupted_by` sender attribution for
    /// message preemption) is stamped on the persisted interrupted row and the
    /// terminal `agent:stream:end` payload. A `PreemptedByMessage` reason also
    /// skips the STAB-28 synthetic `agent:idle` (reason: `interrupted`) emit —
    /// the only caller passing it is [`AgentManager::preempt_busy_turn`]: an
    /// interrupt that carries a follow-up message is a preemption, not a
    /// settlement: the child is about to run the interrupt turn, so waking
    /// completion watches here would deliver a spurious "child settled" report
    /// to the parent. The plain `interrupt()` / `agent.stop` keep-alive path
    /// passes `UserStop` so STAB-28 behavior (watches fire on interrupt) is
    /// preserved. `agent:stream:end` is emitted unconditionally in both paths.
    ///
    /// Returns `(agent_found, interrupted_row_message_id)` — the second field
    /// names the interrupted assistant row this call persisted (`None` when
    /// no live-turn slot was open or the call fell back to the kill path), so
    /// `preempt_busy_turn` can exclude that row from its combined-delivery
    /// re-queue check.
    async fn interrupt_inner(
        &self,
        agent_id: &AgentId,
        reason: InterruptReason,
        interrupted_by: Option<InterruptedBy>,
    ) -> (bool, Option<String>) {
        let suppress_idle_emit = reason == InterruptReason::PreemptedByMessage;
        // The live connection is the interrupt capability; grab it WITHOUT
        // removing the handle so the child stays alive for resume.
        let conn = self
            .handles
            .lock()
            .unwrap()
            .get(agent_id)
            .map(|h| h.connection.clone());
        let Some(conn) = conn else {
            // No live session to interrupt → keep-alive is a no-op; fall back to
            // the hard kill path (itself a no-op when the agent is already gone).
            return (self.stop_with_redelivery_arm(agent_id, reason).await, None);
        };
        // Resolve the persisted session for the workspace (terminal event) + the
        // `acpSessionId` to cancel. Without an `acpSessionId` there is no
        // in-flight turn to interrupt, so fall back to the kill path.
        let session = self.services.store.get_agent_session(agent_id).await.ok();
        let acp_session_id = session.as_ref().and_then(|s| s.acp_session_id.clone());
        let Some(acp_session_id) = acp_session_id else {
            return (self.stop_with_redelivery_arm(agent_id, reason).await, None);
        };
        // Pin the live-turn slot BEFORE aborting the worker: the abort drops
        // the worker future and with it the LiveTurnGuard, so an UNPINNED slot
        // read after the abort would race that drop and frequently lose the
        // partial content. The pin keeps the slot published to `chat.subscribe`
        // until the flush below persists the row (monorepo#2056) and lets that
        // flush read the slot as it stands rather than a clone taken here
        // (monorepo#2110). The busy flag is snapshotted alongside (before
        // `end_turn` below releases it) for the zero-output stop-redelivery
        // arm at the bottom of this method.
        self.services.pin_live_turn(agent_id);
        let turn_in_flight = self.is_busy(agent_id);
        // Abort the in-flight worker so it stops draining the turn/queue; the
        // child is kept alive (unlike `stop`, which also kills the child).
        if let Some(worker) = self.workers.lock().unwrap().remove(agent_id) {
            worker.abort();
        }
        // The abort may have landed between the streaming path's terminal-
        // error stash and the handler's take (monorepo#2050); the orphaned
        // context describes the aborted turn, so it must not survive into a
        // later failure's settle.
        self.services.discard_pending_terminal_error(agent_id);
        // Persist the streamed-so-far assistant content as an interrupted
        // assistant row, stamped with the interruption reason (+ sender
        // attribution on preemption). Runs AFTER the abort (a worker append
        // already in flight can still land, but the `agent_message.id` PK
        // keeps the outcome convergent — the flush absorbs the UNIQUE
        // collision) and BEFORE the terminal `agent:stream:end` emit below so
        // the chat-channel terminal reconcile sees the persisted row and
        // keeps the blocks instead of removing them. The row persists even
        // when nothing streamed yet (empty blocks) — EVERY interruption
        // leaves a durable marker row; on the message-preemption path the
        // empty row lands BEFORE `send_message` appends the interrupting
        // message row, so transcript order reads correctly (this supersedes
        // the STAB-114 phantom-row-free zero-output preemption; the
        // combined-delivery re-queue check in `preempt_busy_turn` excludes
        // this row by id).
        let flushed = self
            .services
            .flush_pinned_turn_on_interruption(agent_id, reason, interrupted_by.as_ref())
            .await;
        // `None` means nothing was pinned — no turn in flight, hence no output.
        // A pinned slot cannot vanish before the flush: `LiveTurnGuard::drop`
        // and the normal turn-end clear both leave a pinned slot to the flush
        // that owns it, so a turn that ends in the abort gap is still read here
        // as it really ended (monorepo#2110) — including the zero-output
        // completion that persists no row of its own and must therefore still
        // produce the marker row and arm the redelivery below.
        let (interrupted_message_id, interrupted_text_blocks, had_output) = match flushed {
            Some(f) => (f.message_id, f.text_blocks, f.had_output),
            None => (None, Vec::new(), false),
        };
        // Zero-output user stop (intent-hq/monorepo#1757): the cancelled
        // provider turn dropped the stopped message before producing any
        // output, so arm the prompt-only redelivery payload for the NEXT
        // turn (consumed in `spawn_worker`). Armed HERE — after the marker
        // row flush (so its id is known for exclusion) but BEFORE `end_turn`
        // below releases the busy slot — so a concurrent send that claims
        // the slot the moment it frees cannot spawn before the payload is
        // visible. Only the plain `agent.stop` keep-alive path (`UserStop`)
        // arms it — the message-preemption path threads the same payload
        // through `preempt_busy_turn`'s combined delivery, and
        // shutdown/suspend interrupts have their own resume semantics.
        if reason == InterruptReason::UserStop && turn_in_flight && !had_output {
            self.arm_stop_redelivery(agent_id, interrupted_message_id.as_ref())
                .await;
        }
        // Cancel the current turn over the wire (keep-alive interrupt). The agent
        // resolves its in-flight `session/prompt` with `StopReason::Cancelled`;
        // best-effort — a wire error never blocks the stop.
        if let Err(e) = intent_acp::session::cancel(&conn, &acp_session_id).await {
            if is_cancel_transport_closed(&e) {
                // Child already dead — expected race when cancelling a dead
                // turn; the run_turn branch surfaces the real failure.
                tracing::debug!(agent = %agent_id, error = %e, "session/cancel skipped: transport already closed");
            } else {
                tracing::warn!(agent = %agent_id, error = %e, "session/cancel failed");
            }
        }
        // STAB-124: the cancelled child echoes `tool_call_update`s for the
        // aborted tool call (title-less, status failed). With the worker gone,
        // they buffer in the handle's notification channel and would be drained
        // by the NEXT turn's fresh transcript — which fabricated an anonymous
        // `tool_use` block (`name: ""`) that broke FE conversation loading.
        // Discard them with the same bounded settle-window drain the resume
        // path uses for the `session/load` replay burst. The aborted worker's
        // channel lock is released when its task drops, so this cannot deadlock.
        let notes = self
            .handles
            .lock()
            .unwrap()
            .get(agent_id)
            .map(|h| h.notifications.clone());
        if let Some(notes) = notes {
            let mut guard = notes.lock().await;
            Services::drain_replay_notifications(&mut guard).await;
        }
        // Release the in-flight slot (recomputes workspace activity) and capture
        // the owning workspace BEFORE the slot is dropped so the terminal event
        // is stamped on the right workspace; fall back to the persisted session.
        let workspace_id = self
            .agent_ws
            .lock()
            .unwrap()
            .get(agent_id)
            .cloned()
            .or_else(|| session.as_ref().map(|s| s.workspace_id.clone()));
        self.end_turn(agent_id).await;
        // Mark the process idle (reapable) but keep its handle so it survives for
        // a follow-up resume.
        self.registry.mark_idle(agent_id);
        // Emit the single terminal `agent:stream:end` on stop (parity #14): the
        // aborted worker's `run_prompt_turn` no longer reaches its own emit.
        // Unlike the normal-completion emit, the interrupt terminal carries
        // `stopReason: "interrupted"` (+ `messageId` when an interrupted row
        // was persisted) so clients can render the Stopped indicator live.
        //
        // Deliberately NO `trailingBlocks` here (monorepo#732): the interrupt
        // flush persists only the streamed-so-far live-turn snapshot — the
        // AtTurnEnd registry is NOT drained on this path (pending entries stay
        // for the next turn's drain / registry TTL, per the §7.1 drain
        // contract in `run_prompt_turn`), so there are no persisted trailing
        // blocks for the event to mirror; carrying undrained entries would
        // break the byte-identical persisted↔event invariant.
        if let Some(workspace_id) = workspace_id {
            // `interruptReason`/`interruptedBy` mirror the persisted row's
            // metadata so clients can render the reason-specific Stopped
            // indicator live, without refetching the transcript.
            let mut end_data = json!({
                "agentId": agent_id.0,
                "stopReason": "interrupted",
                "interruptReason": reason.as_str(),
            });
            // Same reason gate as the persisted row: `interruptedBy` is
            // defined only for message preemption.
            if reason == InterruptReason::PreemptedByMessage {
                if let Some(ref by) = interrupted_by {
                    end_data["interruptedBy"] = by.to_json();
                }
            }
            if let Some(ref message_id) = interrupted_message_id {
                end_data["messageId"] = json!(message_id);
            }
            // Final live-preview values from the flushed partial turn (same
            // contract as the normal-completion terminal emit in
            // `run_prompt_turn`) so a preview-tracking client is not left on
            // a stale mid-turn value after an interrupt.
            crate::agent_session::stamp_preview_fields(&mut end_data, &interrupted_text_blocks);
            self.services
                .publish_agent_event(
                    &workspace_id,
                    agent_id,
                    intent_core::events::AGENT_STREAM_END,
                    end_data,
                )
                .await;
            // STAB-28: emit agent:idle after interrupt so completion watches fire.
            // The aborted worker never reaches the settlement idle-emit in
            // `run_prompt_turn` (agent_session.rs), so we must emit here.
            // Without this, a parent that re-messages via agent.send after the
            // child settles registers a completion watch that never fires (no
            // idle event → watch never delivered). Only emit when the agent has
            // no queued ready-to-send messages (settlement coalescing: mirrors
            // the `run_prompt_turn` check) AND the interrupt is not part of
            // interrupt-with-message (`suppress_idle_emit` — the follow-up
            // content has not been queued yet at this point, so the
            // ready-to-send check alone cannot see the imminent interrupt turn).
            if !suppress_idle_emit && !self.services.has_ready_to_send(agent_id) {
                let mut data = json!({
                    "agentId": agent_id.0,
                    "reason": "interrupted",
                    "status": "idle",
                });
                // Enrich with agentName + isBackground + completion report
                // (reuse the session loaded earlier in this method; avoids
                // duplicate I/O).
                if let Some(ref session) = session {
                    data["agentName"] = json!(session.name);
                    data["isBackground"] = json!(session.is_background);
                    if let Some(ref report) = session.completion_report {
                        // `completionReport` is canonical; `report` is kept
                        // for back-compat with older clients.
                        data["completionReport"] = json!(report);
                        data["report"] = json!(report);
                    }
                }
                // Idle-visibility: same `waitingOnHooks` stamp as the
                // settlement idle in `run_prompt_turn` (omitted when the
                // agent owns no active hook).
                self.services
                    .annotate_waiting_on_hooks(agent_id, &mut data)
                    .await;
                // Idle-visibility (unified external-wait): same
                // `waitingOnPrMonitors` stamp as the settlement idle
                // (omitted when the agent owns no active monitor).
                self.services
                    .annotate_waiting_on_pr_monitors(agent_id, &mut data)
                    .await;
                // Archived-workspace suppression hint: same
                // `workspaceArchived` stamp as the settlement idle (omitted
                // when the workspace is not archived). `workspace.archive`
                // persists `Archived` BEFORE its interrupt sweep, so this is
                // exactly the idle that fires in a just-archived workspace.
                self.services
                    .annotate_workspace_archived(&workspace_id, &mut data)
                    .await;
                self.services
                    .publish_agent_event(
                        &workspace_id,
                        agent_id,
                        intent_core::events::AGENT_IDLE,
                        data,
                    )
                    .await;
            }
        }
        (true, interrupted_message_id)
    }

    /// Derive the zero-output stop-redelivery payload (intent-hq/monorepo#1757)
    /// from the transcript's last user row. Returns `None` when the turn
    /// already progressed (any non-user row after the last user message —
    /// same bounded check as `preempt_busy_turn`'s combined delivery, with
    /// the just-persisted EMPTY interrupted marker row excluded by id) or
    /// when the row carries nothing to redeliver.
    async fn derive_stop_redelivery(
        &self,
        agent_id: &AgentId,
        marker_row_id: Option<&String>,
    ) -> Option<crate::agent_ops::QueuedPrepend> {
        let messages = self
            .services
            .store
            .get_agent_messages(agent_id, Some(10))
            .await
            .ok()?;
        let last_user_idx = messages.iter().rposition(|m| m.role == "user")?;
        if turn_progressed_after(&messages, last_user_idx, marker_row_id) {
            return None;
        }
        let payload = extract_user_prepend(&messages[last_user_idx].content);
        if payload.content.is_none()
            && payload.image_blocks.is_none()
            && payload.file_blocks.is_none()
        {
            return None;
        }
        Some(payload)
    }

    /// Arm the zero-output stop-redelivery payload (intent-hq/monorepo#1757)
    /// for consumption by the next `spawn_worker`. A repeat stop before the
    /// payload is consumed re-derives the same last user row, so the insert
    /// stays idempotent. MUST run before the busy slot is released
    /// (`end_turn`): a concurrent send claims the slot the moment it frees,
    /// and its worker spawn must see the payload.
    async fn arm_stop_redelivery(&self, agent_id: &AgentId, marker_row_id: Option<&String>) {
        if let Some(payload) = self.derive_stop_redelivery(agent_id, marker_row_id).await {
            self.stop_redelivery
                .lock()
                .unwrap()
                .insert(agent_id.clone(), payload);
            self.sync_stop_redelivery(agent_id).await;
        }
    }

    /// Write the current in-memory stop-redelivery state for one agent
    /// through to the durable `agent_stop_redelivery` mirror
    /// (intent-hq/monorepo#1899): upsert when a payload is armed, delete when
    /// none is — so a daemon restart between the stop and the follow-up turn
    /// rehydrates exactly the payload the map held. Re-reads the map at call
    /// time (rather than taking a payload argument), so a sync racing an
    /// arm/consume usually lands the newest state — but map-read order is
    /// not commit order on the write pool, so a residual window remains
    /// where e.g. a consume's DELETE commits after a re-arm's UPSERT,
    /// leaving the store missing a payload the map holds. That agent then
    /// degrades to pre-#1899 in-memory-only behavior until the next sync.
    /// Best-effort by design: a store failure is logged and never blocks
    /// the stop/turn path (the in-memory payload still serves the current
    /// daemon lifetime).
    async fn sync_stop_redelivery(&self, agent_id: &AgentId) {
        let armed = self.stop_redelivery.lock().unwrap().get(agent_id).cloned();
        let result = match armed {
            Some(payload) => match serde_json::to_value(&payload) {
                Ok(value) => {
                    self.services
                        .store
                        .set_stop_redelivery(agent_id, &value, &now_iso())
                        .await
                }
                Err(e) => Err(intent_core::Error::Internal(format!(
                    "encode stop redelivery payload failed: {e}"
                ))),
            },
            None => self.services.store.clear_stop_redelivery(agent_id).await,
        };
        if let Err(e) = result {
            tracing::warn!(agent = %agent_id, error = %e, "stop-redelivery persistence sync failed");
        }
    }

    /// Rehydrate the durable stop-redelivery mirror into the in-memory map at
    /// daemon startup (intent-hq/monorepo#1899), so a zero-output stop armed
    /// before a restart still redelivers on the follow-up turn. Agents that
    /// already hold an in-memory payload are skipped (defensive; at boot the
    /// map is empty). Undecodable payloads are dropped from the store (they
    /// can never be consumed) and skipped. Returns the number rehydrated.
    pub async fn rehydrate_stop_redeliveries(&self) -> Result<usize> {
        let rows = self.services.store.load_all_stop_redeliveries().await?;
        let mut count = 0;
        for row in rows {
            match serde_json::from_value::<crate::agent_ops::QueuedPrepend>(row.payload) {
                Ok(payload) => {
                    let mut armed = self.stop_redelivery.lock().unwrap();
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        armed.entry(row.agent_id)
                    {
                        entry.insert(payload);
                        count += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        agent = %row.agent_id,
                        error = %e,
                        "dropping undecodable persisted stop-redelivery payload"
                    );
                    if let Err(e) = self
                        .services
                        .store
                        .clear_stop_redelivery(&row.agent_id)
                        .await
                    {
                        tracing::warn!(agent = %row.agent_id, error = %e, "failed to drop undecodable stop-redelivery row");
                    }
                }
            }
        }
        Ok(count)
    }

    /// Hard-stop fallback for the interrupt paths (no live handle /
    /// `acpSessionId` — the spawn window): derive the zero-output stop
    /// redelivery for a user stop, then run the [`AgentManager::stop`]
    /// teardown with the payload installed BEFORE `detach` releases the busy
    /// slot — a concurrent send claiming the freed slot must already see it
    /// (the same ordering `interrupt_inner` observes on the keep-alive path).
    /// The payload is derived from the pre-flush transcript (no marker row
    /// persisted yet, so there is no marker id to exclude and the progressed
    /// check sees the true turn state).
    async fn stop_with_redelivery_arm(&self, agent_id: &AgentId, reason: InterruptReason) -> bool {
        let turn_in_flight = self.is_busy(agent_id);
        let had_output = self
            .services
            .live_turn(agent_id)
            .map(|live| !live.blocks.is_empty())
            .unwrap_or(false);
        let redelivery = if reason == InterruptReason::UserStop && turn_in_flight && !had_output {
            self.derive_stop_redelivery(agent_id, None).await
        } else {
            None
        };
        let (removed, child) = self
            .detach_with_redelivery(agent_id, redelivery, true)
            .await;
        if let Some((child, spawn_pid)) = child {
            kill_child_tree(child, spawn_pid).await;
        }
        removed
    }

    /// Whether a turn loop is currently in flight for `agent_id` (consulted by
    /// `agent.sendMessage` to decide queue-vs-stream).
    pub fn is_busy(&self, agent_id: &AgentId) -> bool {
        self.busy.lock().unwrap().contains(agent_id)
    }

    /// Snapshot every agent with a turn currently in flight together with its
    /// owning workspace. This is the daemon-global source for
    /// `agent.listActive`; it never scans persisted workspaces or sessions.
    ///
    /// Lock-order invariant: `busy` is always acquired before `agent_ws`
    /// (here and in every busy/agent_ws mutator — `try_begin`,
    /// `release_in_flight_slot`, `end_turn`), and mutators update both maps
    /// while holding the `busy` lock. That makes a claim/release visible
    /// atomically from this snapshot's perspective: a busy agent always has
    /// its `agent_ws` entry.
    pub fn list_busy(&self) -> Vec<(AgentId, WorkspaceId)> {
        let busy = self.busy.lock().unwrap();
        let agent_ws = self.agent_ws.lock().unwrap();
        let mut active = busy
            .iter()
            .filter_map(|agent_id| {
                agent_ws
                    .get(agent_id)
                    .cloned()
                    .map(|workspace_id| (agent_id.clone(), workspace_id))
            })
            .collect::<Vec<_>>();
        active.sort_unstable_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        active
    }

    /// Atomically claim the in-flight slot for `agent_id` in `workspace_id`:
    /// `true` when the agent was idle (now marked busy), `false` when a turn is
    /// already running. On a successful claim the agent's workspace is recorded
    /// and the workspace's derived `WorkspaceActivity` is recomputed (§9.9),
    /// emitting `workspace:activity-changed` on the `Idle → AgentRunning` edge.
    /// Also persists the `agent_session.status` transition to `Active` (and clears
    /// any persisted `stop_reason`) and emits `agent:status-changed` (PROTOCOL
    /// §6.5/§6.7) so a hydrated chat reflects the live runtime rather than the
    /// stored `Pending` placeholder.
    async fn try_begin(&self, agent_id: &AgentId, workspace_id: &WorkspaceId) -> bool {
        self.try_begin_outcome(agent_id, workspace_id, true).await == TryBeginOutcome::Started
    }

    /// [`AgentManager::try_begin`] with the loss reason: callers that behave
    /// differently on "a prompt worker owns the slot" (safe to hand work to)
    /// versus "the idle-reap sweep holds the agent mid-kill" (nobody owns the
    /// slot; the handle is being torn down) need the distinction decided under
    /// the SAME `busy`-lock acquisition as the claim itself — re-probing after
    /// a `bool` loss would race the claim's release (monorepo#2118 review).
    ///
    /// `auto_unarchive` gates the #1216 auto-unarchive on a `Started` claim:
    /// every normal turn start passes `true`. The worker's end-of-turn raced
    /// re-claim passes `false` — its pre-claim archived gate is inherently
    /// check-then-act, so an archive persisting between that row read and
    /// this claim would otherwise be flipped straight back to Active by the
    /// claim itself; suppressing the flip lets the caller re-check the row
    /// while HOLDING the slot and park instead (monorepo#2513, PR #1244
    /// review).
    async fn try_begin_outcome(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        auto_unarchive: bool,
    ) -> TryBeginOutcome {
        // Insert into `agent_ws` while still holding the `busy` lock
        // (busy → agent_ws order, matching `list_busy`) so a concurrent
        // `list_busy` never observes a busy agent without its workspace.
        let outcome = {
            let mut busy = self.busy.lock().unwrap();
            // An agent claimed by the idle-reap sweep (monorepo#2118) counts
            // as busy: the sweep is about to (or is mid-way through) killing
            // its child tree, so starting a turn now would hand that turn's
            // fresh children to the kill. The caller's queue fallback parks
            // the message; the sweep kicks the drain after releasing.
            let outcome = if busy.contains(agent_id) {
                TryBeginOutcome::Busy
            } else if self.reap_claims.lock().unwrap().contains(agent_id) {
                TryBeginOutcome::ReapClaimed
            } else {
                TryBeginOutcome::Started
            };
            if outcome == TryBeginOutcome::Started {
                // Drop a live-turn slot that outlived its turn BEFORE the claim
                // becomes visible (monorepo#2104). A slot can survive its turn:
                // when `flush_partial_turn_on_interruption` hits a non-UNIQUE
                // store error it deliberately keeps the slot as the only copy of
                // the streamed content. This turn's worker replaces the slot only
                // in `begin_live_turn`, which is many awaits away — the user row
                // INSERT, the task spawn, the ACP session setup — so without this
                // clear the pair (busy = true, slot = the PREVIOUS turn's content)
                // would be readable for that whole window, and `chat_snapshot`
                // would serve stale content labelled `isStreaming: true`.
                //
                // Ordering is the point: clearing under the `busy` lock and
                // BEFORE publishing the claim means any reader that observes
                // `busy == true` is guaranteed to observe the stale slot already
                // gone (`chat_snapshot` reads busy first for exactly this
                // reason). Lock order is busy → live_turns, consistent with the
                // busy → agent_ws invariant above; nothing acquires `live_turns`
                // and then `busy`.
                //
                // A slot whose teardown flush is still IN FLIGHT is left alone:
                // that flush re-reads it at flush time (monorepo#2110), and
                // `interrupt_inner` pins without a busy claim, so a stop against
                // an idle agent can have a flush in flight while this claim
                // wins. Clearing there would drop the content and make the flush
                // misread the vanished slot as "the worker persisted the full
                // row". A flush that already GAVE UP is not coming back, so the
                // slot it kept is cleared like any other orphan.
                self.services
                    .clear_live_turn_unless_flush_in_flight(agent_id);
                busy.insert(agent_id.clone());
                self.agent_ws
                    .lock()
                    .unwrap()
                    .insert(agent_id.clone(), workspace_id.clone());
            }
            outcome
        };
        if outcome == TryBeginOutcome::Started {
            // A real turn is starting: if the workspace is Archived, flip it
            // back to Active and emit the stamped §6.5 delta (auto-unarchive
            // on agent activity). Runs BEFORE the activity/status emits so
            // subscribers see the workspace Active by the time the turn's
            // own events arrive. Best-effort: never blocks the turn. The
            // enqueue paths (archived gates in `try_drain_queue` /
            // `deliver_wake_message`) are untouched — only a claimed slot
            // triggers this. Suppressed (`auto_unarchive = false`) only by
            // the worker's raced re-claim, whose own post-claim archived
            // re-check parks instead of un-archiving (monorepo#2513).
            if auto_unarchive {
                self.services
                    .auto_unarchive_on_turn_start(workspace_id, agent_id)
                    .await;
            }
            self.services.agent_activity_begin(workspace_id).await;
            // Clear stop_reason when starting a new turn: successful turns leave it cleared.
            self.persist_status_with_stop_reason(
                agent_id,
                workspace_id,
                AgentStatus::Active,
                true,
                Some(None),
            )
            .await;
        }
        outcome
    }

    /// Release the in-flight slot without persisting agent status (used when
    /// terminal spawn failure already persisted Error status and we only need
    /// to release busy/agent_ws so a future message can restart the worker).
    async fn release_in_flight_slot(&self, agent_id: &AgentId) {
        let Some(workspace_id) = self.release_slot_sync(agent_id) else {
            return;
        };
        if let Some(workspace_id) = workspace_id {
            self.services.agent_activity_end(&workspace_id).await;
        }
    }

    /// Remove `agent_id` from `busy` and `agent_ws` atomically with respect to
    /// `list_busy` (both maps mutated under the `busy` lock, busy → agent_ws
    /// order). Returns `None` when the agent was not busy, otherwise the
    /// removed `agent_ws` entry.
    fn release_slot_sync(&self, agent_id: &AgentId) -> Option<Option<WorkspaceId>> {
        let mut busy = self.busy.lock().unwrap();
        if !busy.remove(agent_id) {
            return None;
        }
        Some(self.agent_ws.lock().unwrap().remove(agent_id))
    }

    /// Release the in-flight slot, recomputing the owning workspace's derived
    /// `WorkspaceActivity` (§9.9) and emitting `workspace:activity-changed` on
    /// the `AgentRunning → Idle` edge. Also persists the `agent_session.status`
    /// transition to `RuntimeIdle` and emits `agent:status-changed` (PROTOCOL
    /// §6.5/§6.7) so a hydrated chat reflects the post-turn idle state.
    async fn end_turn(&self, agent_id: &AgentId) {
        let Some(workspace_id) = self.release_slot_sync(agent_id) else {
            return;
        };
        if let Some(workspace_id) = workspace_id {
            self.services.agent_activity_end(&workspace_id).await;
            self.persist_status(agent_id, &workspace_id, AgentStatus::RuntimeIdle, false)
                .await;
        }
    }

    /// Clear a persisted completion report when a new turn begins. Skips the
    /// store write and event when no report is set (the common case). Emits
    /// `agent:updated` with `completionReportCleared: true` when a report was
    /// present and cleared. Called at the start of each prompt turn (including
    /// queue-drained turns inside a running worker) so a delegated agent's
    /// completion report does not stick across new work.
    async fn clear_completion_report_if_present(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) {
        let ts = now_iso();
        match self
            .services
            .store
            .clear_completion_report(workspace_id, agent_id, &ts)
            .await
        {
            Ok(true) => {
                // Report was present and cleared — emit agent:updated.
                self.services
                    .publish_agent_mutation_event(
                        workspace_id,
                        agent_id,
                        intent_core::events::AGENT_UPDATED,
                        json!({ "agentId": agent_id.0, "completionReportCleared": true }),
                    )
                    .await;
            }
            Ok(false) => {
                // No report was set — skip the event.
            }
            Err(e) => {
                // Store error (session not found, workspace mismatch) — log and
                // swallow so the turn can proceed. The next successful load will
                // reflect the stale report, but the runtime must not abort.
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "clear completion report failed"
                );
            }
        }
    }

    /// Clear a pending attention request when a qualifying turn begins — the
    /// request (`ws.agent.requestDiscussion` / `ws.agent.reportBlocker`) is a
    /// pending state that retires as soon as the agent next receives a
    /// user-origin message (`agent.sendMessage` front door,
    /// `agent.sendQueuedMessageNow`, `agent.editAndRegenerate`, or a drained
    /// user-origin queue entry). For CHILD (`parent_agent_id` set) and
    /// BACKGROUND (`is_background`) sessions, automatic deliveries (A2A
    /// sends, parent / subscription wakes, `agent.sendToTask`,
    /// `agent.wakeOrCreate`, drained automatic entries, stale redrives) ALSO
    /// retire it — the parent/coordinator is those agents' attention surface,
    /// so its follow-up is the acknowledgement. Top-level foreground agents
    /// keep the user-only dismissal: an automatic message must never dismiss
    /// a request the user has not seen. The call site gates on
    /// `TurnOptions::origin.is_user()` OR the child/background session shape.
    /// A stale redrive of a USER-ORIGIN entry still clears: the drain handoff
    /// restores `origin = User`, unlike the completion-report clear, which
    /// staleness suppresses regardless of origin (`suppress_report_clear`).
    /// Skips the store write and event when no request is pending (the common
    /// case). Emits `agent:updated` with `attentionRequestCleared: true` when
    /// one was present and cleared so clients retire the sidebar/footer
    /// indicator.
    pub(crate) async fn clear_attention_request_if_present(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) {
        let ts = now_iso();
        match self
            .services
            .store
            .clear_attention_request(workspace_id, agent_id, &ts)
            .await
        {
            Ok(true) => {
                self.services
                    .publish_agent_mutation_event(
                        workspace_id,
                        agent_id,
                        intent_core::events::AGENT_UPDATED,
                        json!({ "agentId": agent_id.0, "attentionRequestCleared": true }),
                    )
                    .await;
                // Retiring the request can retire the workspace's
                // needs_attention displayStatus (§6.5 step 0):
                // recompute-and-compare.
                self.services
                    .maybe_emit_display_status_changed(workspace_id)
                    .await;
            }
            Ok(false) => {
                // No request was pending — skip the event.
            }
            Err(e) => {
                // Store error (session not found, workspace mismatch) — log
                // and swallow so the turn can proceed.
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "clear attention request failed"
                );
            }
        }
    }

    /// Stale queued-message redrive detection (#576). A dequeued message is
    /// STALE for a delegated agent (`parent_agent_id` set) when its
    /// `queued_at` predates the session's `completion_report_timestamp` —
    /// i.e. it was enqueued before the current completion report was
    /// persisted, so the parent has already been woken with that report.
    ///
    /// For stale messages this appends the [`stale_redrive_note`] annotation
    /// to the content (skipped for already-persisted requeues, whose
    /// transcript row is fixed, and for content already carrying the note so
    /// a requeued stale entry is not double-annotated) and returns `true` so
    /// the caller sets [`TurnOptions::suppress_report_clear`], keeping the
    /// delivered report queryable. Fail open: session-lookup or timestamp
    /// parse failures treat the message as fresh (today's behavior).
    async fn annotate_stale_redrive(&self, agent_id: &AgentId, msg: &mut QueuedMessage) -> bool {
        let session = match self.services.store.get_agent_session(agent_id).await {
            Ok(session) => session,
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "stale-redrive check skipped: session lookup failed"
                );
                return false;
            }
        };
        if session.parent_agent_id.is_none() {
            return false;
        }
        let Some(report_ts) = session.completion_report_timestamp else {
            return false;
        };
        let (Some(queued), Some(reported)) = (parse_iso(&msg.queued_at), parse_iso(&report_ts))
        else {
            tracing::warn!(
                agent = %agent_id,
                queued_at = %msg.queued_at,
                report_timestamp = %report_ts,
                "stale-redrive check skipped: timestamp parse failed (treating as fresh)"
            );
            return false;
        };
        if queued >= reported {
            return false;
        }
        if !msg.persisted && !msg.content.contains(STALE_REDRIVE_NOTE_PREFIX) {
            msg.content = format!("{}\n\n{}", msg.content, stale_redrive_note(&report_ts));
        }
        true
    }

    /// Persist `agent_session.status` + `is_active` and publish the
    /// `agent:status-changed` self-sufficient event (PROTOCOL §6.5/§6.7). All
    /// failures are logged and swallowed: the runtime turn is the source of
    /// truth and a transient store/bus error must not abort the in-flight slot
    /// transition.
    async fn persist_status(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        status: AgentStatus,
        is_active: bool,
    ) {
        let ts = now_iso();
        if let Err(e) = self
            .services
            .store
            .set_agent_session_status(workspace_id, agent_id, status, is_active, &ts, None)
            .await
        {
            // Sessions are persisted before the runtime path opens (see
            // `agent_create_op`), so NotFound here means the row was deleted
            // mid-turn — swallow it the same as any other transient store error.
            tracing::warn!(agent = %agent_id, error = %e, "failed to persist agent status");
            return;
        }
        let serialized_status = match serde_json::to_value(status) {
            Ok(Value::String(s)) => s,
            _ => return,
        };
        let event = NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: ts,
            event_type: AGENT_STATUS_CHANGED.to_string(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some(agent_id.0.clone()),
                ..Default::default()
            },
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({
                "agentId": agent_id.0,
                "status": serialized_status,
                "isActive": is_active,
            }),
        };
        crate::publish_event(&self.services.event_bus, event).await;
        // Schedule debounced lastActivity event (§10.1).
        self.services
            .schedule_last_activity_event(workspace_id.clone());
    }

    /// Persist `agent_session.status` + `is_active` + optional `stop_reason` and
    /// publish the `agent:status-changed` self-sufficient event (PROTOCOL §6.5/§6.7).
    /// Companion to [`persist_status`]; add `stop_reason` control: `None` leaves it
    /// untouched, `Some(None)` clears it, `Some(Some(reason))` sets it. All failures
    /// are logged and swallowed: the runtime turn is the source of truth and a
    /// transient store/bus error must not abort the in-flight slot transition.
    async fn persist_status_with_stop_reason(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        status: AgentStatus,
        is_active: bool,
        stop_reason: Option<Option<String>>,
    ) {
        let ts = now_iso();
        // Clone stop_reason for event emission (we need it after the store call moves it).
        let stop_reason_for_event = stop_reason.clone();
        if let Err(e) = self
            .services
            .store
            .set_agent_session_status(workspace_id, agent_id, status, is_active, &ts, stop_reason)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "failed to persist agent status + stop_reason");
            return;
        }
        let serialized_status = match serde_json::to_value(status) {
            Ok(Value::String(s)) => s,
            _ => return,
        };
        // Build the event data. When stop_reason is Some(_) — i.e. the call sets or
        // clears the persisted value — include "stopReason" in the event: the string
        // when setting (Some(Some(x))), JSON null when clearing (Some(None)). When the
        // parameter is None (unchanged), omit the field so unrelated status changes
        // don't clobber the FE's canonical session state (cloudlands-fe#147).
        // "stopReasonTimestamp" rides along with the same set/clear semantics: the
        // persisted timestamp is coupled to stop_reason (see
        // `Store::set_agent_session_status`), so the event mirrors the store.
        let mut data = json!({
            "agentId": agent_id.0,
            "status": serialized_status,
            "isActive": is_active,
        });
        if let Some(reason) = &stop_reason_for_event {
            data["stopReason"] = match reason {
                Some(r) => Value::String(r.clone()),
                None => Value::Null,
            };
            data["stopReasonTimestamp"] = match reason {
                Some(_) => Value::String(ts.clone()),
                None => Value::Null,
            };
        }
        let event = NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: ts,
            event_type: AGENT_STATUS_CHANGED.to_string(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some(agent_id.0.clone()),
                ..Default::default()
            },
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        crate::publish_event(&self.services.event_bus, event).await;
        // Schedule debounced lastActivity event (§10.1).
        self.services
            .schedule_last_activity_event(workspace_id.clone());
    }

    /// Forget a finished worker's join handle.
    fn clear_worker(&self, agent_id: &AgentId) {
        self.workers.lock().unwrap().remove(agent_id);
    }

    /// `agent.sendMessage` runtime path (§5.5/§6.8): when a turn is already in
    /// flight, enqueue (the worker flips it to in-flight when the current turn
    /// ends); otherwise persist the user message (under the client-supplied
    /// `messageId` when given, else a minted `user-msg-{uuid}`), publish
    /// `agent:message` (role=user) with the persisted row id, and spawn a
    /// background worker that lazily spawns the child on first turn, drives
    /// the ACP turn through [`AgentManager::run_turn`], and drains the queue.
    /// Returns the TS-shaped `{ success, queued, messageId | queuedMessage }`
    /// where `messageId` IS the persisted row id.
    pub async fn send_message(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        content: String,
        message_id: Option<String>,
        mut options: TurnOptions,
    ) -> Result<Value> {
        // Validate the caller-supplied id length BEFORE any state change
        // (mirrors `agent_send_message_op`'s unconditional guard — the row id
        // is now the client id). Hoisted above `try_begin` so a doomed
        // request never claims the slot (no Active→RuntimeIdle status flap)
        // and the busy branch never queues an oversized id.
        if let Some(ref id) = message_id {
            if id.len() > MAX_MESSAGE_ID_LEN {
                return Err(Error::InvalidParams(format!(
                    "messageId exceeds maximum length of {MAX_MESSAGE_ID_LEN} bytes"
                )));
            }
        }
        // monorepo#564: reject nonexistent targets BEFORE any state change —
        // a truncated/mistyped id must not claim the slot or queue a phantom
        // message that never drains (the sender then waits forever).
        let session = self.services.require_agent_session(&agent_id).await?;
        // Quarantine gate (monorepo#840): a provably-poisoned session (parked
        // in Error with a session-fatal provider block, or a streak of
        // identical terminal failures) must NOT be redriven by message
        // delivery — every replay deterministically fails and spams failure
        // events. Park the message in the queue instead; `agent.retry`
        // (which clears the error + streak) redrives it deliberately. An
        // ordinary Error session still redrives here (the documented "fresh
        // agent.sendMessage" recovery path).
        if self.services.session_poisoned(&session) {
            tracing::warn!(
                agent = %agent_id,
                stop_reason = session.stop_reason.as_deref().unwrap_or(""),
                "session is quarantined (poisoned); parking message in queue instead of driving a turn"
            );
            let (queued, position) = self.services.enqueue_message_with_origin(
                &agent_id,
                content,
                options.image_blocks.clone(),
                options.file_blocks.clone(),
                options.message_metadata.clone(),
                options.queued_prepend(),
                options.interrupt_priority,
                options.origin.is_user(),
            );
            let result = json!({
                "success": true,
                "queued": true,
                "quarantined": true,
                "queuedMessage": queued.to_value(position),
                "turnId": queued.turn_id,
            });
            self.services.publish_queue_updated(&agent_id).await;
            // Close the check-then-park race: a concurrent `agent.retry` may
            // have cleared the Error + streak and finished its drain between
            // the `session` snapshot above and the enqueue — leaving the
            // parked message with no drainer. Re-poll and kick the drain if
            // the session is no longer poisoned; the STAB-52 Error gate in
            // `try_drain_queue` makes this a no-op while still quarantined.
            if let Ok(current) = self.services.store.get_agent_session(&agent_id).await {
                if !self.services.session_poisoned(&current) {
                    self.clone()
                        .try_drain_queue(agent_id.clone(), workspace_id.clone())
                        .await;
                }
            }
            return Ok(result);
        }
        // Question hold (PROTOCOL §5.5): an automatic delivery to an agent
        // with un-dismissed pending questions must NOT start a turn — its
        // turn would bury the pending Q&A under later transcript rows and
        // steal the user's chance to answer. Park the message in the queue
        // instead (interrupt priority included — no exceptions, spec
        // §Decisions); only `agent.dismissQuestions` or an answer-tagged user
        // row flips the hold false and kicks the drain. Checked BEFORE
        // `try_begin` so even an idle agent holds the delivery.
        if !options.origin.is_user() && self.services.question_hold_active(&agent_id).await {
            let (queued, position) = self.services.enqueue_message(
                &agent_id,
                content,
                options.image_blocks.clone(),
                options.file_blocks.clone(),
                options.message_metadata.clone(),
                options.queued_prepend(),
                options.interrupt_priority,
            );
            let result = json!({
                "success": true,
                "queued": true,
                "heldForQuestions": true,
                "queuedMessage": queued.to_value(position),
                "turnId": queued.turn_id,
            });
            self.services.publish_queue_updated(&agent_id).await;
            // Race close (hold-check → enqueue vs a concurrent
            // `dismissQuestions`/answer): the hold may have flipped false
            // between the check above and the enqueue just completing — the
            // dismiss's own `try_drain_queue` kick could have fired against
            // a still-empty queue and found nothing to drain. Re-check and
            // kick again if the hold has since cleared, so this entry is not
            // stranded until some unrelated future trigger.
            if !self.services.question_hold_active(&agent_id).await {
                self.clone()
                    .try_drain_queue(agent_id.clone(), workspace_id.clone())
                    .await;
            }
            return Ok(result);
        }
        // FIFO restore under an active hold (monorepo#1791): a USER-origin
        // send to an agent whose hold has parked ready-to-send entries must
        // not bypass them with a direct turn — the parked backlog (e.g. an
        // `after_all` settlement wake) would sit at position 0 while newer
        // user messages run whole turns past it. Convert the send into a
        // queue-fallback enqueue (user-origin) and kick the drain: the batch
        // flush delivers the older parked entries FIFO in the SAME combined
        // turn as this user message. Requires the `all` flush mode (the
        // default): without batching no combined turn exists to carry the
        // parked entries, so the conversion would only add a queue hop —
        // under `systemOnly`/`off` the direct send stays as documented (the
        // hold contract for automatic entries is unchanged either way).
        // Also skipped when nothing is parked, so the common direct-send
        // path is untouched — and for a session parked in `Error`, whose
        // documented recovery IS the direct fresh send (the STAB-52 gate in
        // `try_drain_queue` would strand a converted entry there).
        if options.origin.is_user()
            && session.status != AgentStatus::Error
            && self.services.flush_queued_messages_mode()
                == intent_core::FlushQueuedMessagesMode::All
            && self.services.has_ready_to_send(&agent_id)
            && self.services.question_hold_active(&agent_id).await
        {
            let (queued, position) = self.services.enqueue_message_with_origin(
                &agent_id,
                content,
                options.image_blocks.clone(),
                options.file_blocks.clone(),
                options.message_metadata.clone(),
                options.queued_prepend(),
                options.interrupt_priority,
                true,
            );
            let result = json!({
                "success": true,
                "queued": true,
                "queuedMessage": queued.to_value(position),
                "turnId": queued.turn_id,
            });
            self.services.publish_queue_updated(&agent_id).await;
            self.clone()
                .try_drain_queue(agent_id.clone(), workspace_id.clone())
                .await;
            return Ok(result);
        }
        if !self.try_begin(&agent_id, &workspace_id).await {
            let (queued, position) = self.services.enqueue_message_with_origin(
                &agent_id,
                content,
                options.image_blocks.clone(),
                options.file_blocks.clone(),
                options.message_metadata.clone(),
                options.queued_prepend(),
                options.interrupt_priority,
                options.origin.is_user(),
            );
            let result = json!({
                "success": true,
                "queued": true,
                "queuedMessage": queued.to_value(position),
                "turnId": queued.turn_id,
            });
            self.services.publish_queue_updated(&agent_id).await;
            return Ok(result);
        }
        let message_id = message_id.unwrap_or_else(new_message_id);
        // Delivery-time unblocked hints (monorepo#2044), direct-send arm: an
        // idle target delivers the wake immediately, so delivery time is NOW
        // — compute the section here so an unqueued completion wake carries
        // it too. Queued deliveries get theirs at drain time instead.
        let content = if crate::agent_ops::ready_delta::metadata_has_triggers(
            options.message_metadata.as_ref(),
        ) && !content
            .contains(crate::agent_ops::ready_delta::UNBLOCKED_SECTION_PREFIX)
        {
            match self
                .services
                .unblocked_section_for_delivery(
                    &agent_id,
                    std::iter::once(options.message_metadata.as_ref()),
                )
                .await
            {
                Some(section) => format!("{content}\n\n{section}"),
                None => content,
            }
        } else {
            content
        };
        // Mint the turn correlation id (monorepo#1022) BEFORE the persist so
        // the user-row `agent:message` echo, the RPC result, and the turn
        // worker (via `options`) all carry the SAME id. `spawn_worker` keeps
        // an already-set id, so this is the direct-send mint point.
        let turn_id = match options.turn_id.clone() {
            Some(id) => id,
            None => {
                let id = new_message_id();
                options.turn_id = Some(id.clone());
                id
            }
        };
        // STAB-133: persist FE-supplied attachments alongside the text block so
        // the transcript row carries them (the conversation view renders them).
        let blocks = user_message_blocks(
            &content,
            options.image_blocks.as_ref(),
            options.file_blocks.as_ref(),
        );
        // Persist the row UNDER the resolved `message_id` so the RPC result's
        // `messageId` and the `agent:message` event both name the actual
        // transcript row (PROTOCOL §5.5 — previously the store minted its own
        // UUIDv7 id and the result id named nothing).
        let message = match self
            .services
            .store
            .append_agent_message_with_id(
                &agent_id,
                &message_id,
                "user",
                &blocks,
                options.message_metadata.as_ref(),
                &now_iso(),
            )
            .await
        {
            Ok(message) => {
                self.services.invalidate_agent_list_cache(&workspace_id);
                message
            }
            Err(append_err) => {
                // Store write failed on a validated agent (e.g. duplicate
                // client-supplied messageId) → auto-queue, matching the
                // `agent.sendMessage` fallback (PROTOCOL §5.5). Self-drain:
                // the slot we just released will be reclaimed below if the
                // queue is ready and the agent is otherwise free.
                self.end_turn(&agent_id).await;
                // Check-then-act race guard (monorepo#564): if the session
                // vanished between the up-front validation and the append
                // (concurrent delete), fail closed like the guard rather than
                // auto-queueing a phantom message for a gone agent.
                if self
                    .services
                    .store
                    .get_agent_session(&agent_id)
                    .await
                    .is_err()
                {
                    tracing::warn!(agent = %agent_id, error = %append_err, "agent session vanished mid-send; rejecting instead of auto-queueing");
                    return Err(Error::InvalidParams(format!(
                        "unknown agent id: {}",
                        agent_id.0
                    )));
                }
                let (queued, position) = self.services.enqueue_message_with_origin(
                    &agent_id,
                    content,
                    options.image_blocks.clone(),
                    options.file_blocks.clone(),
                    options.message_metadata.clone(),
                    options.queued_prepend(),
                    options.interrupt_priority,
                    options.origin.is_user(),
                );
                let result = json!({
                    "success": true,
                    "queued": true,
                    "queuedMessage": queued.to_value(position),
                    "turnId": queued.turn_id,
                });
                self.services.publish_queue_updated(&agent_id).await;
                self.clone().try_drain_queue(agent_id, workspace_id).await;
                return Ok(result);
            }
        };
        // Emit `agent:message` (role=user) with the persisted row id — the
        // direct-send branch previously emitted nothing, so an
        // `agent.editAndRegenerate` regenerated user message never reached
        // clients until a full reload (PROTOCOL §5.5 step 6: "the usual
        // agent:message / agent:stream:* events follow"). Mirrors the
        // queue-drain (`persist_user`) and wake-delivery emits.
        self.services
            .publish_agent_mutation_event(
                &workspace_id,
                &agent_id,
                intent_core::events::AGENT_MESSAGE,
                crate::agent_ops::agent_message_event_payload(&agent_id, &message, Some(&turn_id)),
            )
            .await;
        // Answer intake (PROTOCOL §5.5, question hold): a user row tagged
        // `question_answers` naming the marked assistant message resolves the
        // pending Q&A and clears the marker (a stale/foreign
        // `answeredQuestionsMessageId` is a no-op). Only an ANSWER retires the
        // hold now that pendingness is persisted — a plain user row leaves the
        // marker as it was. Runs BEFORE the displayStatus recompute so the
        // retired hold is reflected.
        self.services
            .resolve_pending_questions_for_answer(
                &workspace_id,
                &agent_id,
                options.message_metadata.as_ref(),
            )
            .await;
        // The retired hold can drop the workspace's needs_attention
        // displayStatus (§6.5 step 0): recompute-and-compare. This recompute
        // ALSO produces the visible `failed → in_progress` transition on the
        // errored-agent redrive path (a fresh sendMessage to a non-poisoned
        // Error session): the earlier recompute inside `try_begin`'s
        // `agent_activity_begin` still reads `status = Error` (persisted to
        // Active only afterwards) and is a no-op, so removing or reordering
        // this call would leave the redriven turn stuck on `failed` for its
        // whole duration. Pinned by
        // `send_message_redrive_emits_failed_to_in_progress`.
        self.services
            .maybe_emit_display_status_changed(&workspace_id)
            .await;
        self.spawn_worker(agent_id, workspace_id, content, options, true);
        Ok(json!({
            "success": true,
            "queued": false,
            "messageId": message.id,
            "turnId": turn_id,
        }))
    }

    /// Self-drain entrypoint (PROTOCOL §5.5). Invoked from `agent.queueMessage`
    /// (and the `send_message` auto-queue fallback above) so a queued message
    /// never sits unworked when the agent is idle. Claims the in-flight slot,
    /// dequeues the head of the queue, persists it, and spawns the turn worker
    /// (which then drains the rest of the queue via its end-of-turn loop).
    /// When the slot is already held by another worker this is a no-op — that
    /// worker's drain loop will pick the message up at turn-end.
    pub async fn try_drain_queue(self: Arc<Self>, agent_id: AgentId, workspace_id: WorkspaceId) {
        if self.is_busy(&agent_id) {
            return;
        }
        // Only claim the in-flight slot when at least one ready-to-send (not
        // under edit) message is waiting — an editing-only queue must stay
        // idle (PROTOCOL §5.5/§6.5 invariant: idle is permitted iff every
        // remaining queued item has `editing = true`).
        if !self.services.has_ready_to_send(&agent_id) {
            return;
        }
        // Archived-workspace gate: the archive sweep interrupts in-flight
        // turns but KEEPS pending queues persisted, so the automatic drain
        // must not respawn a turn while the workspace is archived — messages
        // park until unarchive, which kicks this drain for every parked
        // queue (see `unarchive_workspace`). Chief is virtual and never
        // archived, so skip the row read. Fail open on a lookup error: the
        // gate only parks affirmatively-archived workspaces; a transient
        // store error must not strand the queue.
        if !workspace_id.is_chief() {
            match self.services.store.get_workspace(&workspace_id).await {
                Ok(ws) if ws.archived => {
                    tracing::debug!(
                        agent = %agent_id,
                        workspace = %workspace_id.as_str(),
                        "skipping queue drain: workspace is archived"
                    );
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(
                        agent = %agent_id,
                        workspace = %workspace_id.as_str(),
                        error = %e,
                        "queue drain: workspace archived-state lookup failed; proceeding"
                    );
                }
            }
        }
        // Question hold (PROTOCOL §5.5): AUTOMATIC queued messages stay
        // parked while the agent has un-dismissed pending questions — the
        // turn a drained entry starts would bury the pending Q&A. The park
        // now lasts across turns and daemon restarts (the marker is
        // persisted): only `agent.dismissQuestions` and an answer-tagged user
        // row flip the derivation false, and both re-kick this drain (mirrors
        // the STAB-52 Error gate below). A parked USER-origin entry is
        // exempt — it may itself BE the answer that releases the hold, so it
        // drains rather than deadlocking behind it (`hold_drain` below); a
        // plain user entry drains too but leaves the hold armed.
        let hold_drain = if self.services.question_hold_active(&agent_id).await {
            if !self.services.has_user_origin_ready(&agent_id) {
                tracing::debug!(
                    agent = %agent_id,
                    "skipping queue drain: question hold active (awaiting answer or dismissQuestions)"
                );
                return;
            }
            true
        } else {
            false
        };
        // A session parked in `Error` must NOT be auto-redriven (STAB-52): the
        // terminal spawn/turn-failure handler requeues the failed message and
        // persists `Error` so redriving it is a deliberate act — `agent.retry`
        // (which resets the status to `Pending` before draining) or a fresh
        // `agent.sendMessage`. Without this gate any queue kick (queueMessage,
        // edit-save, wake delivery) re-claims the slot, re-spawns the failing
        // turn, and crash-loops the agent — flapping `is_active` and leaking
        // `is_active=1` rows whenever the cycle is interrupted mid-claim.
        // Fail closed: a session lookup error (transient store error, missing
        // row) also skips the drain — a later queue kick retries, and silently
        // redriving a possibly-errored agent is the exact bug this gate stops.
        match self
            .services
            .store
            .get_agent_session_status(&agent_id)
            .await
        {
            Ok(AgentStatus::Error) => {
                tracing::debug!(
                    agent = %agent_id,
                    "skipping queue drain: session parked in error state (awaiting agent.retry)"
                );
                return;
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "skipping queue drain: agent session status lookup failed"
                );
                return;
            }
        }
        if !self.try_begin(&agent_id, &workspace_id).await {
            return;
        }
        // Batch flush (`agents.flushQueuedMessages`, default `all`): with a
        // batching mode and MORE THAN ONE eligible entry waiting, drain them
        // all into ONE combined provider turn while persisting each entry as
        // its own transcript row. Under an active hold (`hold_drain`) the
        // flush fires only because a user-origin entry is ready — the parked
        // automatic entries ride its combined turn FIFO instead of being
        // bypassed (monorepo#1791). A single eligible entry (or the `off`
        // mode) falls through to the existing single-entry path unchanged.
        {
            let mode = self.services.flush_queued_messages_mode();
            if let Some(batch) = self
                .services
                .dequeue_flush_batch(&agent_id, mode, hold_drain, 2)
            {
                match prepare_flush_turn(&self, &agent_id, &workspace_id, batch).await {
                    FlushPrep::Turn { content, options } => {
                        self.spawn_worker(agent_id, workspace_id, content, *options, true);
                    }
                    FlushPrep::Parked => {
                        // Release the slot without overwriting the Error
                        // status just persisted, so `agent.retry` (or a
                        // future message) can redrive.
                        self.release_in_flight_slot(&agent_id).await;
                    }
                }
                return;
            }
        }
        // Under an active hold only a user-origin entry may drain; the
        // normal path pops the queue head as before.
        let dequeued = if hold_drain {
            self.services.dequeue_user_origin_message(&agent_id)
        } else {
            self.services.dequeue_message(&agent_id)
        };
        let mut next = match dequeued {
            Some(msg) => msg,
            None => {
                // Raced with another mutation (e.g. remove) that emptied the
                // ready-to-send queue between the check above and the dequeue.
                self.end_turn(&agent_id).await;
                // monorepo#1280: the racing retraction saw this drain's
                // in-flight slot (`agent_is_busy` true) and skipped its own
                // redelivery, expecting a turn to end with a terminal
                // `agent:idle` — but this arm emits none. Re-run the
                // mutation-path redelivery now that the slot is released;
                // its guards (marker set, queue empty, not busy) make it a
                // no-op in every other interleaving.
                self.services
                    .redeliver_completion_after_queue_mutation(&agent_id)
                    .await;
                return;
            }
        };
        self.services
            .publish_queue_updated_for(
                &agent_id,
                &workspace_id,
                self.services.queue_snapshot(&agent_id),
            )
            .await;
        // Stale-redrive check (#576) BEFORE the transcript append so the
        // annotated content reaches both the persisted user row and the
        // provider prompt.
        let stale = self.annotate_stale_redrive(&agent_id, &mut next).await;
        // Dequeue-wait note: same placement contract — the persisted row and
        // the provider prompt both carry the enqueue time + wait.
        annotate_dequeue_wait(&mut next);
        // Delivery-time unblocked hints (monorepo#2044): resolved NOW, at
        // render time, from the trigger ids the wake stamped at enqueue.
        annotate_unblocked_hints(&self.services, &agent_id, std::slice::from_mut(&mut next)).await;
        // Drain-start signal (monorepo#1022): the entry just flipped to
        // in-flight; its `turnId` covers redrives that skip the user-row
        // append below. Emitted AFTER the stale-redrive annotation so the
        // payload's `content` matches what is persisted/sent to the provider.
        self.services
            .publish_queue_processing(&agent_id, &workspace_id, &next)
            .await;
        // Skip the transcript append for a terminal-failure requeue whose
        // user row already reached the transcript before the failed turn
        // began; otherwise persist now (with `persist_user`'s bounded retry).
        // Fail closed (#547): if the append still fails, do NOT start the
        // turn — park the agent in `Error` with the message requeued
        // (`persisted: false`) so `agent.retry` re-attempts the append,
        // instead of producing assistant output for a user row that never
        // reached the transcript.
        let user_persisted = if next.persisted {
            true
        } else {
            persist_user(
                &self,
                &agent_id,
                &workspace_id,
                &next.content,
                next.image_blocks.as_ref(),
                next.file_blocks.as_ref(),
                next.message_metadata.as_ref(),
                Some(&next.turn_id),
            )
            .await
        };
        // Queue-drained turns carry no per-turn prompt hints of their own,
        // but the FE-supplied attachments and `messageMetadata` captured at
        // enqueue time do ride along so the drained turn receives the same
        // image + file blocks and a terminal-failure requeue keeps the tag.
        // The `prepend_*` fields restore a failed zero-output interrupt's
        // combined delivery on retry (monorepo#1014).
        let options = TurnOptions {
            image_blocks: next.image_blocks.clone(),
            file_blocks: next.file_blocks.clone(),
            message_metadata: next.message_metadata.clone(),
            suppress_report_clear: stale,
            queued_at: Some(next.queued_at.clone()),
            prepend_content: next.prepend_content.clone(),
            prepend_image_blocks: next.prepend_image_blocks.clone(),
            prepend_file_blocks: next.prepend_file_blocks.clone(),
            turn_id: Some(next.turn_id.clone()),
            interrupt_priority: next.interrupt_priority,
            // Restore the entry's captured origin so a user message that
            // parked behind a busy turn still clears a pending attention
            // request when it drains.
            origin: origin_from_user_flag(next.user_origin),
            ..TurnOptions::default()
        };
        if !user_persisted {
            handle_drain_persist_failure(&self, &agent_id, &workspace_id, &next.content, &options)
                .await;
            // Release the slot without overwriting the Error status just
            // persisted, so `agent.retry` (or a future message) can redrive.
            self.release_in_flight_slot(&agent_id).await;
            return;
        }
        self.spawn_worker(
            agent_id,
            workspace_id,
            next.content,
            options,
            user_persisted,
        );
    }

    /// `agent.sendQueuedMessageNow` runtime path (§5.5): atomically remove
    /// the queued entry named by `message_id` from the agent's queue and
    /// deliver it immediately with interrupt priority, PRESERVING the rest of
    /// the queue. An absent entry is `-32602` ("queued message not found")
    /// with NO side effects — deliberately NOT idempotent (unlike
    /// `agent.removeQueuedMessage`), so the client knows the atomic send did
    /// not happen. A busy agent is preempted keep-alive (the same
    /// `session/cancel` + worker-abort as `agent.sendMessage` with
    /// `priority: "interrupt"`; the child is never killed); an idle agent
    /// starts the turn directly.
    ///
    /// Transactional guarantee: once the entry leaves the queue it is either
    /// delivered or restored. When the slot cannot be claimed (turn startup /
    /// a concurrent send won the race) or the user-row append fails, the
    /// entry is restored at the FRONT of the queue (`persisted` untouched, so
    /// a retry drain does not double-append) — the message is never lost.
    ///
    /// A quarantined (poisoned, monorepo#840) session is NOT redriven: the
    /// entry stays in the queue untouched and the result reports
    /// `queued: true, quarantined: true` — `agent.retry` is the deliberate
    /// redrive. An ORDINARY `Error` session (no fatal reason / streak) IS
    /// redriven — the explicit "send now" is a user action, same spirit as
    /// the documented fresh-`agent.sendMessage` recovery path (the STAB-52
    /// drain gate does not apply here by design).
    pub async fn send_queued_message_now(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        message_id: String,
    ) -> Result<Value> {
        // monorepo#564: fail closed on a nonexistent target BEFORE touching
        // the queue.
        let session = self.services.require_agent_session(&agent_id).await?;
        // Quarantine gate (monorepo#840): a provably-poisoned session must
        // not be redriven by delivery — every replay deterministically
        // fails. The entry STAYS in the queue (no side effects); the absent
        // case is still `-32602` so the contract holds.
        if self.services.session_poisoned(&session) {
            let entry = self
                .services
                .queue_snapshot(&agent_id)
                .into_iter()
                .find(|m| m["id"].as_str() == Some(message_id.as_str()))
                .ok_or_else(|| {
                    Error::InvalidParams(format!("queued message not found: {message_id}"))
                })?;
            tracing::warn!(
                agent = %agent_id,
                stop_reason = session.stop_reason.as_deref().unwrap_or(""),
                "session is quarantined (poisoned); sendQueuedMessageNow leaves the entry queued"
            );
            return Ok(json!({
                "success": true,
                "queued": true,
                "quarantined": true,
                "queuedMessage": entry,
            }));
        }
        // Atomic dequeue under the queue lock: no concurrent drain can
        // deliver the same entry twice.
        let mut entry = self
            .services
            .take_queued_message(&agent_id, &message_id)
            .ok_or_else(|| {
                Error::InvalidParams(format!("queued message not found: {message_id}"))
            })?;
        // Stale-redrive parity with the drain paths (#576): a delegated
        // agent's entry that predates the delivered completion report is
        // annotated and keeps the report queryable.
        let stale = self.annotate_stale_redrive(&agent_id, &mut entry).await;
        // Dequeue-wait note: parity with the drain paths — the "send now"
        // delivery tells the target when the entry was enqueued.
        annotate_dequeue_wait(&mut entry);
        // Delivery-time unblocked hints (monorepo#2044): parity with the
        // drain paths — resolved at render time.
        annotate_unblocked_hints(&self.services, &agent_id, std::slice::from_mut(&mut entry)).await;
        // Publish the shrunk snapshot (write-through persist inside) so
        // clients see the entry leave the queue before the turn starts.
        self.services
            .publish_queue_updated_for(
                &agent_id,
                &workspace_id,
                self.services.queue_snapshot(&agent_id),
            )
            .await;
        // Queue-drained turns carry no per-turn prompt hints of their own;
        // the entry's captured attachments and metadata ride along, same as
        // `try_drain_queue`.
        let mut options = TurnOptions {
            turn_id: Some(entry.turn_id.clone()),
            image_blocks: entry.image_blocks.clone(),
            file_blocks: entry.file_blocks.clone(),
            message_metadata: entry.message_metadata.clone(),
            suppress_report_clear: stale,
            queued_at: Some(entry.queued_at.clone()),
            prepend_content: entry.prepend_content.clone(),
            prepend_image_blocks: entry.prepend_image_blocks.clone(),
            prepend_file_blocks: entry.prepend_file_blocks.clone(),
            // Explicit user action (question hold, PROTOCOL §5.5): "send
            // now" bypasses the hold by design, and it delivers with
            // interrupt priority — a terminal-failure requeue keeps the
            // front-of-queue position.
            origin: intent_core::MessageOrigin::User,
            interrupt_priority: true,
            ..TurnOptions::default()
        };
        // Preempt a cancellable in-flight turn keep-alive (no-op when idle
        // or during turn startup, where preemption would kill the child).
        self.preempt_busy_turn(&agent_id, &mut options).await;
        if !self.try_begin(&agent_id, &workspace_id).await {
            // The slot is still held (turn startup, or a concurrent send won
            // the race): restore the entry at the FRONT so it is the next
            // message delivered, and report the queued outcome honestly.
            // The explicit "send now" is a user action: mark the restored
            // entry user-origin so the winner's end-of-turn drain delivers
            // it even while the question hold is active (§5.5 bypass).
            entry.user_origin = true;
            let restored = entry.to_value(0);
            self.services.requeue_front(&agent_id, entry);
            self.services.publish_queue_updated(&agent_id).await;
            return Ok(json!({
                "success": true,
                "queued": true,
                "queuedMessage": restored,
            }));
        }
        // Skip the transcript append for a terminal-failure requeue whose
        // user row already reached the transcript (STAB-112) — the entry id
        // already names that row.
        if !entry.persisted {
            // STAB-133: persist the entry's attachments alongside the text
            // block, under the entry id so the RPC result's `messageId` and
            // the `agent:message` event both name the actual transcript row.
            let blocks = user_message_blocks(
                &entry.content,
                entry.image_blocks.as_ref(),
                entry.file_blocks.as_ref(),
            );
            let message = match self
                .services
                .store
                .append_agent_message_with_id(
                    &agent_id,
                    &entry.id,
                    "user",
                    &blocks,
                    entry.message_metadata.as_ref(),
                    &now_iso(),
                )
                .await
            {
                Ok(message) => {
                    self.services.invalidate_agent_list_cache(&workspace_id);
                    message
                }
                Err(append_err) => {
                    // Transactional guarantee: release the slot and restore
                    // the entry at the FRONT (`persisted: false`, so a retry
                    // re-attempts the append), then surface the failure.
                    self.end_turn(&agent_id).await;
                    self.services.requeue_front(&agent_id, entry);
                    self.services.publish_queue_updated(&agent_id).await;
                    return Err(append_err);
                }
            };
            // Emit `agent:message` (role=user) with the persisted row id,
            // mirroring the `send_message` direct-send branch (§5.5).
            self.services
                .publish_agent_mutation_event(
                    &workspace_id,
                    &agent_id,
                    intent_core::events::AGENT_MESSAGE,
                    crate::agent_ops::agent_message_event_payload(
                        &agent_id,
                        &message,
                        Some(entry.turn_id.as_str()),
                    ),
                )
                .await;
            // Answer intake (PROTOCOL §5.5, question hold): an answer that was
            // queued and then explicitly sent still resolves the pending Q&A.
            // An untagged entry leaves the marker (and the workspace's
            // needs_attention displayStatus, §6.5 step 0) untouched, so the
            // recompute only runs on an actual clear.
            if self
                .services
                .resolve_pending_questions_for_answer(
                    &workspace_id,
                    &agent_id,
                    entry.message_metadata.as_ref(),
                )
                .await
            {
                self.services
                    .maybe_emit_display_status_changed(&workspace_id)
                    .await;
            }
        }
        let entry_id = entry.id.clone();
        let turn_id = entry.turn_id.clone();
        self.spawn_worker(agent_id, workspace_id, entry.content, options, true);
        Ok(json!({
            "success": true,
            "queued": false,
            "messageId": entry_id,
            "turnId": turn_id,
        }))
    }

    /// `agent.editAndRegenerate` runtime path (§5.5): edit a past user message
    /// and regenerate from that point. Orchestrates, in order:
    ///
    /// 1. Validate `message_id` refers to an existing **user** message
    ///    (read-only, BEFORE any state changes — a bad id surfaces `-32602`
    ///    without stopping the turn or touching the transcript).
    /// 2. Stop any in-flight turn (hard-cancel: abort the worker + kill the
    ///    child) and discard the pending queue.
    /// 3. Optionally switch the model (the `model` param, via `agent.setModel`
    ///    semantics) before the regenerated turn spawns.
    /// 4. Truncate the transcript to just before the edited message (emits
    ///    `agent:updated` with `{ truncatedCount, remainingCount }`).
    /// 5. Arm the forced-recreate flag so the next prompt SKIPS `session/load`
    ///    and opens a fresh `session/new` (the provider must not retain the
    ///    truncated turns), plus the `recreated` flag so the truncated history
    ///    replays as `<supervisor>` XML on that prompt.
    /// 6. Send `content` as a fresh user message (normal
    ///    [`AgentManager::send_message`] path; stream events follow).
    pub async fn edit_and_regenerate(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        message_id: String,
        content: String,
        model: Option<String>,
        mut options: TurnOptions,
    ) -> Result<Value> {
        self.services
            .agent_validate_edit_target_op(&agent_id, &message_id)
            .await?;
        // Arm `force_recreate` IMMEDIATELY after validation — before `stop()`
        // — to shrink the window where a concurrent turn could establish a
        // resumed session between the stop and the arm. It survives `stop()`
        // by design, and a spuriously-armed flag is safe (worst case: one
        // unnecessary fresh session + history replay). `ensure_started` also
        // consults it on the live-child reuse path, so an interleaved turn
        // that does establish a session before the truncation still gets torn
        // down and recreated on the next prompt.
        self.force_recreate.lock().unwrap().insert(agent_id.clone());
        self.stop(&agent_id).await;
        if self.services.clear_queue(&agent_id) {
            self.services
                .publish_queue_updated_for(&agent_id, &workspace_id, Vec::new())
                .await;
        }
        // Until the truncation actually lands, a failure must DISARM the
        // flag: nothing was truncated, so leaving it set would force an
        // unnecessary session recreate (lost provider warm state) on the next
        // unrelated turn. After the truncation persists, the flag must stay
        // armed no matter what fails later.
        let pre_truncate = async {
            if let Some(model_id) = model {
                self.services
                    .agent_set_model_op(agent_id.clone(), model_id, None)
                    .await?;
            }
            self.services
                .agent_edit_truncate_op(&agent_id, &message_id)
                .await
        };
        let truncated_count = match pre_truncate.await {
            Ok(count) => count,
            Err(e) => {
                self.force_recreate.lock().unwrap().remove(&agent_id);
                return Err(e);
            }
        };
        // Arm `recreated` AFTER `stop` (which clears it): it makes the next
        // turn prepend the truncated history as `<supervisor>` XML.
        self.recreated.lock().unwrap().insert(agent_id.clone());
        // Drop any pending zero-output stop redelivery
        // (intent-hq/monorepo#1757): the truncation may have removed the
        // captured message, and the history replay above covers survivors.
        // The durable mirror is cleared too (intent-hq/monorepo#1899) so a
        // restart cannot resurrect the dropped payload.
        let dropped = self
            .stop_redelivery
            .lock()
            .unwrap()
            .remove(&agent_id)
            .is_some();
        if dropped {
            self.sync_stop_redelivery(&agent_id).await;
        }
        // Explicit user action (question hold, PROTOCOL §5.5): the send is
        // user-origin so it is never held. The truncation above already
        // re-derived the pending-questions marker against the post-truncation
        // transcript, so a Q&A the edit cut away is gone and one that survived
        // stays pending.
        options.origin = intent_core::MessageOrigin::User;
        let mut result = self
            .send_message(agent_id, workspace_id, content, None, options)
            .await?;
        if let Some(obj) = result.as_object_mut() {
            obj.insert("truncatedCount".to_string(), json!(truncated_count));
        }
        Ok(result)
    }

    /// `agent.sendMessage` with `priority: "interrupt"` (§5.5): preempt the
    /// in-flight turn instead of queueing behind it, then deliver `content`
    /// immediately as a fresh turn on the SAME live session. The preemption is
    /// the keep-alive [`AgentManager::interrupt`] (`session/cancel` + worker
    /// abort) — unlike a hard `stop`, the child process is
    /// never killed and the pending queue is preserved, so the interrupted
    /// agent keeps processing (the queue drains after the interrupt turn). An
    /// idle agent falls through to the normal [`AgentManager::send_message`]
    /// path unchanged.
    ///
    /// **Zero-output interrupt = combined delivery** (STAB-114 /
    /// monorepo#1014): when the preempted turn produced no assistant output,
    /// the preempted user message would otherwise be silently dropped by the
    /// provider-side `session/cancel`. Instead of re-queueing it BEHIND the
    /// interrupt (which inverted the user's intended order), its text and
    /// attachments are threaded into the interrupt turn's [`TurnOptions`]
    /// `prepend_*` fields, so ONE `session/prompt` delivers both messages in
    /// original order. Both user rows are already persisted, so the prepend
    /// is prompt-only and the transcript stays intact.
    ///
    /// Two crash timings from the reference app are guarded here:
    /// - **Duplicate delivery.** The SAME interrupt (same client-supplied
    ///   `messageId`) delivered twice in quick succession preempts exactly
    ///   once: the id is recorded under [`AgentManager::interrupt_ids`] BEFORE
    ///   preempting, so the duplicate returns an idempotent
    ///   `{ success, queued: false, messageId, deduplicated: true }` ack
    ///   without cancelling the interrupt turn it raced and without
    ///   re-persisting the message. Dedup requires a stable `messageId`; a
    ///   distinct id is a genuinely new interrupt and preempts normally.
    /// - **Turn startup.** When the busy slot is claimed but there is no
    ///   cancellable turn yet (child handle / `acpSessionId` not live — the
    ///   spawn/`session/new` window), [`AgentManager::interrupt`] would fall
    ///   back to the hard `stop` kill. Preemption is skipped instead and the
    ///   message queues keep-alive behind the starting turn (`queued: true`),
    ///   draining right after it — the agent is never killed.
    pub async fn interrupt_send_message(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        content: String,
        message_id: Option<String>,
        mut options: TurnOptions,
    ) -> Result<Value> {
        // Every queue fallback below (hold gate, busy race, quarantine park,
        // append-failure auto-queue) must park this message at the FRONT of
        // the queue (spec §Decisions: interrupts always enter ahead of
        // normal entries, arrival-ordered among themselves).
        options.interrupt_priority = true;
        // monorepo#564: reject nonexistent targets BEFORE the dedup record or
        // any preemption — same fail-closed guard as `send_message`.
        self.services.require_agent_session(&agent_id).await?;
        // Duplicate-delivery guard: check-and-record is atomic under the lock,
        // so of two racing duplicates exactly one proceeds. Runs BEFORE the
        // hold check below so a held interrupt still records its id — an
        // interrupt parked by the hold keeps the same at-most-once contract
        // as one that streamed immediately: a duplicate with the same
        // `message_id` arriving while the hold is active is deduplicated
        // instead of double-enqueuing, and a replay arriving after the hold
        // releases is deduplicated too.
        if let Some(mid) = message_id.as_deref() {
            let mut ids = self.interrupt_ids.lock().unwrap();
            if ids.get(&agent_id).map(String::as_str) == Some(mid) {
                return Ok(json!({
                    "success": true,
                    "queued": false,
                    "messageId": mid,
                    "deduplicated": true,
                }));
            }
            ids.insert(agent_id.clone(), mid.to_string());
        }
        // Question hold (PROTOCOL §5.5): an automatic interrupt is ALSO held
        // — no exceptions (spec §Decisions). Skip the preemption entirely
        // (there is nothing to preempt: the asking agent is idle, and a busy
        // agent's hold cannot be active since the Q&A message is terminal)
        // and let `send_message`'s hold gate park the message front-of-queue.
        if !options.origin.is_user() && self.services.question_hold_active(&agent_id).await {
            return self
                .send_message(agent_id, workspace_id, content, message_id, options)
                .await;
        }
        self.preempt_busy_turn(&agent_id, &mut options).await;
        // The slot was just released (or was never held): the send path claims
        // it and streams the interrupt message right away rather than queueing.
        // If a concurrent send wins the race the message queues instead — it is
        // still delivered by that worker's drain loop, never dropped.
        self.send_message(agent_id, workspace_id, content, message_id, options)
            .await
    }

    /// Shared keep-alive preemption for the interrupt-priority delivery paths
    /// ([`AgentManager::interrupt_send_message`] and
    /// [`AgentManager::send_queued_message_now`]): cancel the in-flight turn
    /// without killing the child, threading a zero-output turn's preempted
    /// user message into `options.prepend_*` for combined delivery
    /// (monorepo#1014). A no-op when the agent is idle, or during turn
    /// startup (no live handle / `acpSessionId` yet) where the keep-alive
    /// interrupt would fall back to the `stop` kill path — the caller's send
    /// then queues behind the starting turn instead.
    async fn preempt_busy_turn(self: &Arc<Self>, agent_id: &AgentId, options: &mut TurnOptions) {
        if !self.is_busy(agent_id) {
            return;
        }
        // Preempt only when a cancellable turn is live (handle +
        // `acpSessionId`); during turn startup the keep-alive interrupt
        // would fall back to the `stop` kill path, so skip it and let
        // the caller queue behind the starting turn instead.
        let cancellable = self.contains(agent_id)
            && self
                .services
                .store
                .get_agent_session(agent_id)
                .await
                .ok()
                .and_then(|s| s.acp_session_id)
                .is_some();
        if !cancellable {
            return;
        }
        // STAB-114: Check if the current turn has produced zero output
        // (no assistant content chunks) BEFORE we cancel. Use the live-turn
        // slot (not persisted transcript) to detect zero output: assistant
        // rows are only persisted at turn END, so an interrupted mid-stream
        // turn would incorrectly look like zero output if we checked the
        // transcript. The LiveTurn.blocks are assistant blocks by construction
        // (see Transcript::snapshot_blocks), so non-empty means output exists.
        let has_output = self
            .services
            .live_turn(agent_id)
            .map(|live| !live.blocks.is_empty())
            .unwrap_or(false);

        // Sender attribution for the interrupted row / `stream:end` payload:
        // a user-origin delivery is `{ kind: "user" }`; an agent-to-agent
        // send carries the `messageMetadata` `fromAgentId`/`fromAgentName`
        // sender-attribution payload (PROTOCOL §5.5). Automatic sends with
        // no attribution stamp no `interruptedBy` (reason alone suffices).
        let interrupted_by = if options.origin.is_user() {
            Some(InterruptedBy::User)
        } else {
            options.message_metadata.as_ref().and_then(|md| {
                md.get("fromAgentId")
                    .and_then(Value::as_str)
                    .map(|id| InterruptedBy::Agent {
                        agent_id: id.to_string(),
                        name: md
                            .get("fromAgentName")
                            .and_then(Value::as_str)
                            .map(str::to_string),
                    })
            })
        };
        // Cancel the turn IMMEDIATELY to prevent it from finishing while
        // we prepare the re-queue logic below. This releases the in-flight
        // slot and aborts the draining worker. The STAB-28 synthetic
        // `agent:idle` is suppressed (`PreemptedByMessage`): this interrupt
        // carries a follow-up message (the child is being preempted, not
        // settling), so completion watches must not report "child
        // settled" to the parent here. The returned row id names the
        // interrupted marker row this preemption just persisted (empty
        // blocks on the zero-output path), excluded from the progress
        // check below.
        let (_, interrupted_row_id) = self
            .interrupt_inner(
                agent_id,
                InterruptReason::PreemptedByMessage,
                interrupted_by,
            )
            .await;

        if !has_output {
            // Zero-output condition: the provider dropped the preempted
            // message on `session/cancel`, so deliver it TOGETHER with
            // the interrupt message in ONE combined prompt (original
            // first) instead of re-queueing it behind the interrupt
            // (which inverted the user's intended order, monorepo#1014).
            // Fetch last 10 transcript messages (bounded work) to find
            // the user message + its attachments. If any non-user
            // messages (assistant/tool/system) exist after the last
            // user message, the turn has already progressed and we
            // should NOT re-deliver (avoids duplicate tool calls or
            // re-running side effects). The EMPTY interrupted marker row
            // the preemption itself just appended is NOT progress —
            // exclude it by id, but ONLY while it is actually empty:
            // `has_output` above is snapshotted several awaits before
            // `interrupt_inner` re-reads the slot, so a first block
            // streaming in that window lands in the flushed row — a
            // NON-empty marker row IS progress and must keep blocking
            // the combined re-delivery.
            if let Ok(messages) = self
                .services
                .store
                .get_agent_messages(agent_id, Some(10))
                .await
            {
                if let Some(last_user_msg) = messages.iter().rev().find(|m| m.role == "user") {
                    let last_user_idx = messages
                        .iter()
                        .rposition(|m| m.id == last_user_msg.id)
                        .unwrap();
                    let has_non_user_after = turn_progressed_after(
                        &messages,
                        last_user_idx,
                        interrupted_row_id.as_ref(),
                    );

                    if !has_non_user_after {
                        // Extract the preempted message's text + attachments
                        // (shared with the zero-output user-stop redelivery
                        // arm, intent-hq/monorepo#1757).
                        let payload = extract_user_prepend(&last_user_msg.content);

                        // Prompt-only prepend: both user rows are
                        // already persisted, so nothing is appended to
                        // the transcript and the queue is untouched.
                        // MERGE with any entry-carried prepend payload
                        // (a monorepo#1014-requeued entry delivered via
                        // `send_queued_message_now` already carries its
                        // own `prepend_*`): the entry's older prepend
                        // stays first, the just-preempted message follows
                        // — transcript order, nothing clobbered.
                        if let Some(text) = payload.content.filter(|t| !t.is_empty()) {
                            options.prepend_content = Some(match options.prepend_content.take() {
                                Some(existing) if !existing.is_empty() => {
                                    format!("{existing}\n\n{text}")
                                }
                                _ => text,
                            });
                        }
                        options.prepend_image_blocks = merge_block_arrays(
                            options.prepend_image_blocks.take(),
                            payload.image_blocks,
                        );
                        options.prepend_file_blocks = merge_block_arrays(
                            options.prepend_file_blocks.take(),
                            payload.file_blocks,
                        );
                    }
                }
            }
        }
    }

    /// Spawn (and track) the background turn worker for an agent. The caller must
    /// already hold the in-flight slot (`try_begin`). `user_persisted` reports
    /// whether the initial turn's user row durably reached the transcript, so a
    /// terminal-failure requeue carries the true durability state (STAB-51).
    fn spawn_worker(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        content: String,
        mut options: TurnOptions,
        user_persisted: bool,
    ) {
        // Every worker spawn flows through here, so this is the single mint
        // point for the turn correlation id (monorepo#1022): direct sends get
        // a fresh id; callers that already carry one (a drained queue entry's
        // preserved `turn_id`) keep it.
        if options.turn_id.is_none() {
            options.turn_id = Some(new_message_id());
        }
        // Consume a pending zero-output stop redelivery
        // (intent-hq/monorepo#1757): the stopped turn's user message (text +
        // attachments) is merged into this turn's `prepend_*` options so the
        // provider — which dropped it on `session/cancel` — sees it ahead of
        // this turn's own content. Every worker spawn flows through here, so
        // the payload is consumed by whichever turn runs next (direct send,
        // queue drain, `sendQueuedMessageNow`, retry redrive). MERGE order:
        // an entry-carried prepend (an even earlier preempted message riding
        // a requeued entry) stays FIRST — the armed row is the transcript's
        // last user row at stop time, so it is the newest prepend payload.
        // On a recreated session `build_turn_prompt`'s history replay covers
        // the prepend TEXT (`history_covers_prepend`), same as the
        // preemption path; attachments are still emitted (history is
        // text-only).
        let armed = self.stop_redelivery.lock().unwrap().remove(&agent_id);
        let consumed_redelivery = armed.is_some();
        if let Some(armed) = armed {
            if let Some(text) = armed.content.filter(|t| !t.is_empty()) {
                options.prepend_content = Some(match options.prepend_content.take() {
                    Some(existing) if !existing.is_empty() => format!("{existing}\n\n{text}"),
                    _ => text,
                });
            }
            options.prepend_image_blocks =
                merge_block_arrays(options.prepend_image_blocks.take(), armed.image_blocks);
            options.prepend_file_blocks =
                merge_block_arrays(options.prepend_file_blocks.take(), armed.file_blocks);
        }
        let mgr = self.clone();
        let id = agent_id.clone();
        let handle = tokio::spawn(async move {
            // Clear the durable stop-redelivery mirror before the turn runs
            // (intent-hq/monorepo#1899): the payload was consumed into this
            // turn's prompt above, so a restart after this point must not
            // rehydrate — and redeliver — it a second time. The sync re-reads
            // the map, so a repeat stop that re-armed in the gap upserts the
            // new payload instead of deleting.
            if consumed_redelivery {
                mgr.sync_stop_redelivery(&id).await;
            }
            run_message_worker(mgr, id, workspace_id, content, options, user_persisted).await;
        });
        self.workers.lock().unwrap().insert(agent_id, handle);
    }

    /// Claim the in-flight slot for a delivery-driven turn. Companion to
    /// [`AgentManager::finish_prepersisted_turn_spawn`] and
    /// [`AgentManager::release_slot`]: the caller uses this two-step protocol
    /// so the user-message row is persisted BETWEEN slot claim and worker
    /// spawn — a persist failure at that point releases the slot without ever
    /// having launched a worker that could produce assistant output for a
    /// row that isn't in the transcript.
    ///
    /// Returns `true` when the slot was claimed, `false` when a turn was
    /// already in flight (the caller must enqueue instead).
    pub(crate) async fn try_begin_turn(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> bool {
        self.try_begin(agent_id, workspace_id).await
    }

    /// Arm the per-agent idle-notification listener (monorepo#855): a
    /// background poll that opens an implicit agent-initiated turn when an
    /// out-of-turn `session/update` arrives while no prompt turn is consuming
    /// the handle's notification receiver. Captures only [`Services`] and
    /// re-upgrades the attached manager each tick, so the task never keeps
    /// the manager alive; it stands down on its own when the handle is gone
    /// or the manager was dropped/never attached (bare test wiring).
    fn spawn_wake_listener(&self, agent_id: AgentId, workspace_id: WorkspaceId) -> JoinHandle<()> {
        let services = self.services.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(HARNESS_WAKE_POLL).await;
                let Some(mgr) = services.agent_manager() else {
                    return;
                };
                if !mgr.wake_listener_tick(&agent_id, &workspace_id).await {
                    return;
                }
            }
        })
    }

    /// One idle-listener poll: consume an out-of-turn `session/update` (if
    /// any) into an implicit harness-wake turn, driven by a task registered
    /// in `workers` so the interrupt/stop abort paths apply to it. Returns
    /// `false` when the agent's handle is gone (the listener exits). Skips
    /// the tick — leaving buffered notifications untouched — while the wake
    /// gate is raised (`start_session` resume-replay in flight), while a
    /// prompt turn owns the single-flight slot, or when the receiver is
    /// locked by another consumer.
    async fn wake_listener_tick(
        self: &Arc<Self>,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> bool {
        let (notes, gate) = {
            let map = self.handles.lock().unwrap();
            let Some(handle) = map.get(agent_id) else {
                return false;
            };
            (handle.notifications.clone(), handle.wake_gate.clone())
        };
        if gate.load(AtomicOrdering::SeqCst) > 0 {
            return true;
        }
        if self.busy.lock().unwrap().contains(agent_id) {
            return true;
        }
        // Perf (PR review, PROTOCOL §5.5): a non-consuming peek — in-memory,
        // no store round-trip — so the two `question_hold_active` reads
        // below only run once a notification is actually buffered. Without
        // this, every idle agent with a live handle costs ~2 SQLite reads
        // per `HARNESS_WAKE_POLL` tick (50ms) even when nothing is pending.
        {
            let Ok(peek) = notes.try_lock() else {
                return true;
            };
            if peek.is_empty() {
                return true;
            }
        }
        // Question hold (PROTOCOL §5.5): an implicit harness wake turn would
        // append a fresh assistant message, burying the pending Q&A the hold
        // protects. Skip the tick (buffered notifications stay untouched)
        // until the questions are answered or dismissed.
        if self.services.question_hold_active(agent_id).await {
            return true;
        }
        // Owned lock so the claimed path below can move the receiver guard
        // into the spawned drive task with no unlock/relock gap another
        // consumer could slip into. The notification observed by the peek
        // above may already be gone (a concurrent consumer drained it) —
        // `try_recv` below re-checks and this tick is a no-op either way.
        let Ok(mut guard) = notes.try_lock_owned() else {
            return true;
        };
        let Ok(first) = guard.try_recv() else {
            return true;
        };
        // Only a `session/update` that materializes transcript content opens
        // a turn: a chunk, or a tool call with a derivable name. Everything
        // else is dropped — unmappable variants for parity with the prompt
        // path (`route_notification` ignores them), and name-less tool-call
        // first-sights because the fresh wake transcript's `record_tool`
        // drops them anyway (the STAB-124 late `tool_call_update` echoes
        // that outlast the interrupt drain window): opening on one would
        // emit a phantom `stream:start`/`stream:end` pair with no content
        // and pin the busy slot for the settle window.
        //
        // A `usage_update` cost is the one exception: providers commonly emit
        // the final cost report after the response, so it can lead a buffered
        // burst. It materializes no transcript content, so it is persisted
        // cost-only (§5.23) — no turn, no busy slot, no phantom stream events
        // — instead of being dropped.
        match intent_acp::session::map_notification(&first) {
            // A thought chunk materializes a `thinking` block just like a
            // message chunk materializes a `text` one, so it opens a turn on
            // the same terms (parity with `route_notification`).
            Some(intent_acp::session::MappedUpdate::Chunk { .. }) => {}
            Some(intent_acp::session::MappedUpdate::ToolCall(ref tc))
                if !tc.tool_name.trim().is_empty() => {}
            Some(intent_acp::session::MappedUpdate::UsageCost(cost)) => {
                drop(guard);
                self.services
                    .persist_cost_only_ordered(
                        agent_id,
                        workspace_id,
                        UsageCost {
                            amount: cost.amount,
                            currency: cost.currency,
                        },
                    )
                    .await;
                return true;
            }
            _ => return true,
        }
        // Claim the single-flight slot so a racing `agent.sendMessage` queues
        // instead of interleaving. On the rare loss to a PROMPT turn (a send
        // claimed the slot between the busy check and here), the first
        // notification is already consumed, so still stream it as an implicit
        // turn — but with a ZERO settle window, handing the receiver straight
        // back to the blocked prompt worker — and leave every slot-owner duty
        // (registry active/idle marks, slot release, idle emit, queue drain)
        // to the prompt turn that won the slot: marking idle here would flag
        // the process eviction-eligible (and pop a spawn waiter)
        // mid-prompt-turn.
        match self.try_begin_outcome(agent_id, workspace_id, true).await {
            TryBeginOutcome::Started => {}
            TryBeginOutcome::Busy => {
                self.services
                    .run_harness_wake_turn(
                        &mut guard,
                        first,
                        agent_id,
                        workspace_id,
                        Duration::ZERO,
                    )
                    .await;
                return true;
            }
            // A loss to the idle-reap sweep is NOT a prompt worker owning the
            // slot: the sweep holds the agent mid-kill (monorepo#2118), so
            // driving a harness wake turn here would run unowned concurrent
            // work against the handle being torn down. Drop the consumed
            // notification instead — it is the tail of the dying child's
            // output and would have died with the handle's buffer had the
            // kill won the race to it.
            TryBeginOutcome::ReapClaimed => return true,
        }
        self.registry.mark_active(agent_id);
        // Drive the turn in its own task registered in `workers`, so
        // `interrupt` / `interrupt_send_message` / `stop` abort an open wake
        // turn with the same snapshot→abort→flush semantics as a prompt turn
        // (the abort drops the receiver guard + LiveTurnGuard; the interrupt
        // path then flushes the partial row, releases the slot, and emits
        // the terminal `stream:end`). The busy slot stays held until this
        // task's own `end_turn`, so subsequent listener ticks skip while the
        // turn is open.
        let mgr = self.clone();
        let (id, ws) = (agent_id.clone(), workspace_id.clone());
        let drive = tokio::spawn(async move {
            mgr.services
                .run_harness_wake_turn(&mut guard, first, &id, &ws, HARNESS_WAKE_SETTLE)
                .await;
            mgr.registry.mark_idle(&id);
            drop(guard);
            // Deregister BEFORE releasing the slot: while the slot is held
            // no concurrent send can spawn (and register) a prompt worker,
            // so this provably removes only this task's own entry.
            mgr.clear_worker(&id);
            mgr.end_turn(&id).await;
            mgr.services.publish_harness_wake_idle(&id, &ws).await;
            // A user send that raced in queued behind this turn's slot; with
            // the slot released, kick the self-drain so it streams now
            // (handoff: the drained prompt turn locks the receiver next).
            if mgr.services.has_ready_to_send(&id) {
                mgr.clone().try_drain_queue(id, ws).await;
            }
        });
        self.workers.lock().unwrap().insert(agent_id.clone(), drive);
        true
    }

    /// Retry a failed agent spawn (`agent.retry` RPC path). Only valid when
    /// the agent status is `error`; returns `{ ok: false }` otherwise. Clears
    /// the error status, tears down any stale child, and attempts to redrive
    /// the front-of-queue message (requeued at exhaustion) plus any subsequent
    /// messages. Reuses the spawn-retry/backoff machinery, so a retry that
    /// fails again lands back in the `error` state with events.
    ///
    /// The result carries `redriven` so clients can distinguish "a queued
    /// message is being redriven" (`true` — status cleared to `pending`, drain
    /// started) from "the queue was empty, nothing to redrive" (`false` —
    /// status cleared to `idle`; the next `agent.sendMessage` starts a fresh
    /// turn). Without this, an empty-queue retry was an invisible no-op: the
    /// agent parked in `pending` with no worker and the FE got a bare
    /// `{ ok: true }` (STAB-54).
    pub async fn agent_retry(
        self: &Arc<Self>,
        agent_id: AgentId,
        _workspace_id: WorkspaceId,
    ) -> Result<Value> {
        // Fetch current session status
        let session = self.services.store.get_agent_session(&agent_id).await?;

        // Only allow retry when the session status is `error`
        if session.status != AgentStatus::Error {
            return Ok(json!({ "ok": false }));
        }

        // Use the session's persisted workspace_id for safety (cross-workspace guard)
        let workspace_id = &session.workspace_id;

        // monorepo#940: consult poisoning BEFORE `clear_failure_streak` below —
        // the identical-failure streak feeds `session_poisoned`, so checking
        // after the clear would miss streak-poisoned sessions. When poisoned,
        // resuming the provider session via `session/load` would replay the
        // exact context the provider deterministically rejects; arm
        // `force_recreate` (same mechanism as `agent.editAndRegenerate`) so
        // the redrive's `start_session` skips the resume and opens a fresh
        // `session/new` (recreated-flag history prepend + token-usage
        // baseline fold).
        if self.services.session_poisoned(&session) {
            tracing::warn!(
                agent = %agent_id,
                stop_reason = session.stop_reason.as_deref().unwrap_or(""),
                "retrying a poisoned session: arming force-recreate so the redrive opens a fresh session/new instead of resuming the corrupted one (monorepo#940)"
            );
            self.force_recreate.lock().unwrap().insert(agent_id.clone());
        }

        // agent.retry is the deliberate quarantine escape hatch (monorepo#840):
        // clear the identical-failure streak alongside the status/stop_reason
        // so the redrive starts from a clean slate — and the failure-wake
        // dedup records too, so a post-retry failure with the SAME error text
        // (new information the retrying party is waiting on) still delivers.
        self.services.clear_failure_streak(&agent_id);
        self.services.clear_failure_wake_dedup(&agent_id);

        // Empty queue → nothing will drive a `pending` status forward, so
        // clear the error to `idle` instead (idle is permitted iff no
        // ready-to-send work remains, PROTOCOL §5.5/§6.5 invariant).
        let mut redriven = self.services.has_ready_to_send(&agent_id);
        let next_status = if redriven {
            AgentStatus::Pending
        } else {
            AgentStatus::RuntimeIdle
        };

        // Clear the error status and emit agent:status-changed
        self.persist_retry_status(&agent_id, workspace_id, next_status)
            .await?;

        // Abort any in-flight worker task and release the in-flight slot.
        // Any terminal-error context the aborted worker's streaming path
        // stashed (monorepo#2050) is stale on the same terms as the streak
        // cleared above — retry is the clean-slate escape hatch.
        if let Some(worker) = self.workers.lock().unwrap().remove(&agent_id) {
            worker.abort();
        }
        self.services.discard_pending_terminal_error(&agent_id);
        self.release_in_flight_slot(&agent_id).await;

        // Tear down any stale child handle (use kill_child_only to avoid
        // overwriting the status we just set)
        self.kill_child_only(&agent_id).await;

        // Close the check-then-flip race: a message enqueued between the queue
        // check above and the status flip had its own drain attempt suppressed
        // by the Error gate in `try_drain_queue` (STAB-52), and this path was
        // about to skip the drain too — stranding a ready-to-send message.
        // Re-poll under the post-Error status; anything there is a redrive.
        if !redriven && self.services.has_ready_to_send(&agent_id) {
            redriven = true;
            self.persist_retry_status(&agent_id, workspace_id, AgentStatus::Pending)
                .await?;
        }

        // Start the drain loop to redrive the requeued message. Peek the
        // head entry's turn correlation id BEFORE the drain pops it so the
        // response names the turn being redriven (monorepo#1022).
        let mut turn_id = None;
        if redriven {
            turn_id = self.services.peek_ready_turn_id(&agent_id);
            self.clone()
                .try_drain_queue(agent_id, workspace_id.clone())
                .await;
        }

        let mut result = json!({ "ok": true, "redriven": redriven });
        if let Some(tid) = turn_id {
            result["turnId"] = json!(tid);
        }
        Ok(result)
    }

    /// Persist an `agent.retry` status transition (clearing any persisted
    /// `stop_reason`) and publish the matching `agent:status-changed` event.
    /// Shared by the initial Error-clear flip and the post-flip re-check that
    /// promotes Idle → Pending when a message slipped into the queue during the
    /// retry (see [`AgentManager::agent_retry`]).
    async fn persist_retry_status(
        self: &Arc<Self>,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        status: AgentStatus,
    ) -> Result<()> {
        let is_active = false;
        // Clear stop_reason on retry: the agent is starting fresh, not stuck in an error.
        // Route through persist_status_with_stop_reason to ensure the agent:status-changed
        // event carries stopReason: null.
        self.persist_status_with_stop_reason(agent_id, workspace_id, status, is_active, Some(None))
            .await;
        // Clearing the Error park retires the `failed` displayStatus rung
        // (§6.5 step 0): recompute-and-compare so the demotion emits.
        self.services
            .maybe_emit_display_status_changed(workspace_id)
            .await;
        Ok(())
    }

    /// Spawn the background turn worker after the caller has already claimed
    /// the in-flight slot via [`AgentManager::try_begin_turn`] AND persisted
    /// the user-message row. The worker path does NOT re-persist the initial
    /// `content` (it flows in-memory to `build_turn_prompt`), so the persist
    /// MUST have succeeded before this call.
    pub(crate) fn finish_prepersisted_turn_spawn(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        content: String,
        options: TurnOptions,
    ) {
        self.spawn_worker(agent_id, workspace_id, content, options, true);
    }

    /// Release an in-flight slot claimed via [`AgentManager::try_begin_turn`]
    /// but not followed by a worker spawn (persist failed before
    /// [`AgentManager::finish_prepersisted_turn_spawn`]). Public-in-crate seam
    /// so `Services::deliver_wake_message` can hand control back to the drain
    /// loop after a store error, mirroring the `send_message` self-drain path.
    pub(crate) async fn release_slot(&self, agent_id: &AgentId) {
        self.end_turn(agent_id).await;
    }

    /// Whether the cached handle's child process + transport still look live
    /// (monorepo#764). The connection probe catches a writer task that
    /// already died on a broken pipe, but it is lazy — the writer only
    /// notices on its next write — so a child that died while the agent sat
    /// idle is caught by `Child::try_wait` instead. A missing handle counts
    /// as dead; a handle without an owned child trusts the connection probe
    /// alone, and an errored `try_wait` probe is treated as live so a
    /// transient wait failure never forces a spurious respawn.
    fn handle_is_live(&self, agent_id: &AgentId) -> bool {
        let mut handles = self.handles.lock().unwrap();
        let Some(handle) = handles.get_mut(agent_id) else {
            return false;
        };
        if !handle.connection.is_alive() {
            return false;
        }
        match handle._child.as_mut() {
            Some(child) => !matches!(child.try_wait(), Ok(Some(_))),
            None => true,
        }
    }

    /// Persist the informational `model_changed` transcript row when this
    /// turn's spawn identity (`resolve_spawn`'s model/provider) differs from
    /// the last committed turn's (`agent_session.last_turn_model` /
    /// `last_turn_provider`), then commit the current identity as the new
    /// last-turn pair (deferred commit: `agent.setModel` toggles reverted
    /// before any message never produce a notice because nothing was
    /// committed). No notice on the agent's very first turn (no committed
    /// prior identity). The row is `role: "system"` — excluded from
    /// supervisor-XML replay, which only renders user/assistant/error — with
    /// metadata `{ type: "model_changed", from, to, fromProvider, toProvider }`
    /// (`from`/`to` are the spawn-resolved model ids, `null` = provider
    /// default). Emits `agent:message` so clients update live. Entirely
    /// best-effort: read/append/publish failures are logged and the turn
    /// proceeds. Called only from `ensure_started`'s SUCCESS paths (live-child
    /// reuse, or after `start_session` returned Ok) so a failed spawn/switch
    /// never persists a notice or commits an identity the agent never ran
    /// under; retry attempts within one turn (`retry_spawn`) cannot duplicate
    /// the notice — the identity commit lands with the first success.
    async fn maybe_persist_model_change_notice(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        resolved: &ResolvedSpawn,
    ) {
        let to_model = resolved.model.as_deref();
        let to_provider = resolved.provider.id;
        let (from_model, from_provider) = match self
            .services
            .store
            .get_agent_session_last_turn_model(workspace_id, agent_id)
            .await
        {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "failed to read last-turn model; skipping model-change notice");
                return;
            }
        };
        let changed = match from_provider.as_deref() {
            // No committed prior turn → first turn, never a notice.
            None => false,
            Some(prev_provider) => {
                prev_provider != to_provider || from_model.as_deref() != to_model
            }
        };
        if changed {
            let label = |provider: &str, model: Option<&str>| match model {
                Some(m) => format!("{provider}:{m}"),
                None => format!("{provider} (default model)"),
            };
            let from_label = label(
                from_provider.as_deref().unwrap_or(""),
                from_model.as_deref(),
            );
            let to_label = label(to_provider, to_model);
            let content = json!([{
                "type": "text",
                "text": format!("Model changed from {from_label} to {to_label}."),
            }]);
            let metadata = json!({
                "type": "model_changed",
                "from": from_model,
                "to": to_model,
                "fromProvider": from_provider,
                "toProvider": to_provider,
            });
            match self
                .services
                .store
                .append_agent_message_with_metadata(
                    agent_id,
                    "system",
                    &content,
                    Some(&metadata),
                    &now_iso(),
                )
                .await
            {
                Ok(message) => {
                    self.services.invalidate_agent_list_cache(workspace_id);
                    self.services
                        .publish_agent_mutation_event(
                            workspace_id,
                            agent_id,
                            intent_core::events::AGENT_MESSAGE,
                            crate::agent_ops::agent_message_event_payload(agent_id, &message, None),
                        )
                        .await;
                }
                Err(e) => {
                    tracing::warn!(agent = %agent_id, error = %e, "failed to persist model-change notice");
                }
            }
        }
        // Commit this turn's identity (also the first-turn baseline) so the
        // next turn compares against what THIS turn actually ran under.
        if from_provider.as_deref() != Some(to_provider) || from_model.as_deref() != to_model {
            if let Err(e) = self
                .services
                .store
                .set_agent_session_last_turn_model(workspace_id, agent_id, to_model, to_provider)
                .await
            {
                tracing::warn!(agent = %agent_id, error = %e, "failed to commit last-turn model");
            }
        }
    }

    /// Ensure the agent's child process + ACP session exist, spawning lazily on
    /// first turn (the TS spawn-on-first-message semantics) and reusing the live
    /// session otherwise. When the session's model/provider has changed (via
    /// `agent.setModel`), tears down the existing child and respawns with the
    /// new model before the next turn. Returns the `acpSessionId` to drive the turn.
    async fn ensure_started(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<String> {
        // Teardown fence (ghost-agent race): refuse to (re)spawn an agent a
        // `workspace.delete` batch stop (`stop_many`) is tearing down — its
        // session row is about to be cascade-deleted, so a lazy spawn here
        // would create a child that outlives the row. NotFound is
        // non-retryable for `retry_spawn`, so the turn fails fast instead of
        // burning spawn attempts against a fence that will not lift. The
        // install-time fence in `create_agent` closes the interleaving where
        // this check passes just before the fence arms.
        if self.stopping.lock().unwrap().contains(agent_id) {
            return Err(Error::NotFound(format!(
                "agent session {agent_id} is being deleted"
            )));
        }
        // A delegate with CoW isolation provisions the sandbox in a background
        // task, off the delegate critical path (monorepo#871). Await
        // settlement BEFORE reading the session so the child never spawns
        // against a half-copied sandbox (the spawn cwd is
        // `session.sandbox_path`) and the session read below observes the
        // settled sandbox fields. No-op when no provisioning is in flight —
        // the common case for every turn after the first.
        self.services.await_sandbox_provisioning(agent_id).await;
        let session = self.services.store.get_agent_session(agent_id).await?;
        let workspace = self.services.store.get_workspace(workspace_id).await.ok();
        let settings = self.services.effective_settings();
        let mut resolved = resolve_spawn(
            &session,
            workspace.as_ref(),
            &settings,
            self.chief_cwd_root.as_deref(),
        )?;

        // Check if the agent's model/provider has changed (via agent.setModel).
        // If so, tear down the existing child and force a respawn with the new model.
        if self.contains(agent_id) {
            let needs_respawn = {
                let handles = self.handles.lock().unwrap();
                if let Some(handle) = handles.get(agent_id) {
                    // Compare the session's currently-resolved model/provider against
                    // the values the child was spawned with. A mismatch means
                    // agent.setModel was called while the child was live.
                    let model_changed =
                        handle.spawned_model.as_deref() != resolved.model.as_deref();
                    let provider_changed = handle.spawned_provider != resolved.provider.command;
                    model_changed || provider_changed
                } else {
                    false
                }
            };

            // Forced recreate (`agent.editAndRegenerate`): the live child's
            // provider session predates the truncation, so it must not be
            // reused OR resumed — tear it down and let `start_session` open a
            // fresh `session/new`. Checking here (not just in `start_session`)
            // makes `ensure_started` the single enforcement point regardless
            // of how an edit interleaves with concurrent turns: without it,
            // the live-child reuse branch below would return the stale session
            // with the armed flag sitting unconsumed.
            let forced = self.force_recreate.lock().unwrap().contains(agent_id);
            if needs_respawn || forced {
                // Tear down the existing child (preserving the acpSessionId so
                // start_session can try session/load for providers that support it).
                // This is narrower than stop() — only kills the child/handle, no
                // worker/busy-flag touch, matching the retry-spawn teardown path.
                self.kill_child_only(agent_id).await;
            } else if let Some(acp) = session.acp_session_id.clone() {
                if self.handle_is_live(agent_id) {
                    // Model unchanged and child is live — reuse the existing
                    // session. The notice/commit still runs: the live child may
                    // predate a same-provider model change the reuse tolerates,
                    // and an unchanged identity is a cheap no-op read.
                    self.maybe_persist_model_change_notice(agent_id, workspace_id, &resolved)
                        .await;
                    // A `reasoningEffort` change needs no respawn: re-apply it
                    // on the live session through the `thought_level` config
                    // option discovered at session open, so it takes effect
                    // for the turn about to run (PROTOCOL §5.5). A no-op when
                    // the effort is unchanged or the provider advertised no
                    // such option.
                    let conn = self
                        .handles
                        .lock()
                        .unwrap()
                        .get(agent_id)
                        .map(|h| h.connection.clone());
                    if let Some(conn) = conn {
                        self.apply_thought_level(
                            conn.as_ref(),
                            agent_id,
                            &acp,
                            session.reasoning_effort.as_deref(),
                        )
                        .await;
                    }
                    return Ok(acp);
                }
                // The child/transport died while the agent sat idle
                // (monorepo#764): clear the stale handle + registry entry and
                // fall through to the spawn path below, which respawns the
                // child and resumes via `session/load` (or the recreate
                // fallback in `start_session`).
                tracing::warn!(agent_id = %agent_id, "cached agent child is dead; respawning before turn");
                self.kill_child_only(agent_id).await;
            }
        }
        // unsloth spawn gate (spec "Proposed design" §4): before the child
        // spawns, make sure the daemon-managed Unsloth server is running and
        // ready for the session's model, and thread the resulting endpoint
        // (baseURL/apiKey/limits) into the OPENCODE_CONFIG_CONTENT injection.
        // Only needed when a fresh child will actually spawn — a reused live
        // child already carries the endpoint it was spawned with.
        if resolved.provider.id == "unsloth" && !self.contains(agent_id) {
            let repo_id = resolved.model.clone().ok_or_else(|| {
                Error::InvalidParams(
                    "unsloth provider requires a model (pick one from the unsloth catalog)"
                        .to_string(),
                )
            })?;
            // Progress callback: surface download/loading status as
            // `agent:stream:status` turn-startup hints (D4 — first use can
            // mean a multi-GB download; the FE shows the phase message next
            // to the pre-first-token spinner). The callback's level maps
            // straight onto the event's `level` field, so a model-switch
            // disruption warning arrives as `level: "warning"`. Messages
            // are funneled through a single ordered channel + drainer task
            // (not one task per message): clients keep only the latest
            // message per agent, so publishes must preserve emission order
            // or a restart warning could be clobbered by a later-emitted
            // but earlier-published progress update.
            let (status_tx, mut status_rx) = tokio::sync::mpsc::unbounded_channel::<(
                crate::unsloth_server::StatusLevel,
                String,
            )>();
            {
                let services = self.services.clone();
                let ws = workspace_id.clone();
                let aid = agent_id.clone();
                tokio::spawn(async move {
                    while let Some((level, message)) = status_rx.recv().await {
                        services
                            .publish_status_event(&ws, &aid, "launch", &message, level.as_str())
                            .await;
                    }
                });
            }
            let status_cb = move |level: crate::unsloth_server::StatusLevel, message: String| {
                let _ = status_tx.send((level, message));
            };
            // Live-session snapshot for the restart-on-switch warning:
            // agents spawned with the unsloth provider are attached to the
            // managed server and lose the loaded model if a different-model
            // spawn restarts it.
            let attached_agents = self.count_agents_with_provider("unsloth");
            // `providers.paths["unsloth"]` targets the unsloth CLI the
            // managed-server lifecycle shells out to (NOT the opencode ACP
            // primary, which `resolve_spawn` keys on "opencode").
            let unsloth_cli_override = read_provider_path_setting(&settings, "unsloth");
            let endpoint = self
                .unsloth
                .ensure_endpoint(
                    &repo_id,
                    unsloth_cli_override.as_deref(),
                    attached_agents,
                    &status_cb,
                )
                .await?;
            resolved.unsloth_endpoint = Some(endpoint);
        }
        let mut opts = SpawnOptions::new(&resolved.provider);
        opts.cwd = Some(&resolved.cwd);
        opts.model = resolved.model.as_deref();
        opts.reasoning_effort = resolved.reasoning_effort.as_deref();
        opts.provider_binary = resolved.provider_binary.as_deref();
        opts.npx_fallback_binary = resolved.npx_fallback_binary.as_deref();
        opts.npx_fallback_package = resolved.npx_fallback_package;
        opts.extra_env = resolved.extra_env.clone();
        opts.unsloth_endpoint = resolved.unsloth_endpoint.as_ref();
        // monorepo#884 Phase 2.2: offer the daemon-backed
        // `intentd git-credential` helper to the provider child as a
        // github.com-scoped credential helper — no token bytes in the child
        // env, never raw GITHUB_TOKEN/GH_TOKEN (the MCP secret denylist stays
        // intact). Gated on the opt-out setting and best-effort — setting off
        // or an unresolvable daemon binary path leaves the child env
        // untouched, and the spawn never fails on it.
        let git_credential_expose = settings
            .source_control
            .github
            .expose_git_credential_to_children;
        inject_git_credential_env(&mut opts.extra_env, git_credential_expose);
        if !self.contains(agent_id) {
            // Derive the agent type from the session's specialist `agentType`
            // frontmatter (SP-B); falls back to the default interactive type so
            // plain agents and specialists without `agentType` are unchanged.
            let agent_type = derive_agent_type(&self.services, &session, workspace.as_ref());
            // §18.4 CLI-side denylist: strip provider-native tools (e.g.
            // auggie's built-in `str-replace-editor`, `sub-agent-*`) via
            // `--remove-tool`. MCP-side filtering (§6.8) already blocks
            // workspace-MCP tools, but the provider's native tools can only be
            // stripped through this spawn-time flag.
            opts.tools_to_remove =
                intent_acp::get_tools_to_remove(session.specialist.as_deref(), &agent_type);
            self.create_agent(
                agent_id.clone(),
                workspace_id.clone(),
                session.name.clone(),
                &agent_type,
                resolved.cwd.clone(),
                &opts,
            )
            .await?;
        }
        let acp_session_id = self
            .start_session(agent_id, resolved.cwd.clone(), &resolved.provider)
            .await?;
        // Model-change transcript notice: only AFTER the child + ACP session
        // are provably up under the new identity — a failed spawn/switch must
        // not persist a notice or commit `last_turn_*` to an identity the
        // agent never ran under. Store-based (not handle-based) so detection
        // also covers idle-agent respawns. Best-effort — a notice failure
        // never blocks the turn.
        self.maybe_persist_model_change_notice(agent_id, workspace_id, &resolved)
            .await;
        Ok(acp_session_id)
    }

    /// Tear down every tracked agent (clean daemon shutdown kills all children).
    /// Before stopping each in-flight agent, capture it as an interrupted session
    /// so the FE modal offers resumption on next launch — same as a crash (INT-41
    /// graceful-shutdown gap).
    pub async fn shutdown(&self) {
        let ids: Vec<AgentId> = self.handles.lock().unwrap().keys().cloned().collect();
        let now = intent_core::now_iso();

        // Capture in-flight agents before stop() settles them to RuntimeIdle.
        for id in &ids {
            // Only agents currently in-flight (in the busy set) need interruption rows.
            if !self.busy.lock().unwrap().contains(id) {
                continue;
            }
            // Read the workspace from agent_ws (stop() will clear it via end_turn).
            let workspace_id = match self.agent_ws.lock().unwrap().get(id).cloned() {
                Some(ws) => ws,
                None => continue, // Stale busy entry (should not happen).
            };
            // Pin the live-turn slot BEFORE aborting the worker: the abort
            // drops the worker future and with it the LiveTurnGuard, so an
            // UNPINNED slot read after the abort would race that drop and
            // frequently lose the partial content. The pin both keeps the slot
            // published to `chat.subscribe` until the flush below persists the
            // row (monorepo#2056) and lets that flush re-read the slot as it
            // stands (monorepo#2110).
            self.services.pin_live_turn(id);
            // Abort the turn worker BEFORE flushing so it cannot race the
            // partial flush by persisting the full turn under the same minted
            // message id (which would leave the transcript stuck on the partial
            // snapshot while the worker's own append errors on the UNIQUE id).
            // stop() below removes the (already-gone) worker entry harmlessly.
            if let Some(worker) = self.workers.lock().unwrap().remove(id) {
                worker.abort();
            }
            // Best-effort: persist any partial in-flight assistant content from
            // the pinned slot so the transcript keeps the streamed-so-far
            // output across the restart. Runs before the status guards below so
            // a degenerate status read/encode failure never drops the content.
            self.services
                .flush_pinned_turn_on_interruption(id, InterruptReason::DaemonShutdown, None)
                .await;
            // Read the current persisted status BEFORE end_turn settles it to RuntimeIdle.
            // Use get_agent_session_status (lightweight, skips message log).
            // RACE: try_begin inserts into busy BEFORE persist_status(Active) completes, so
            // shutdown in that window may read Pending. Busy-set membership is authoritative:
            // if the agent is in busy, it's mid-turn regardless of the persisted status.
            let prev_status = match self.services.store.get_agent_session_status(id).await {
                Ok(status) => status,
                Err(e) => {
                    tracing::warn!(agent_id = %id, error = %e, "graceful shutdown: could not read session status");
                    continue;
                }
            };
            // Serialize the status via serde to match the DB form (e.g., "active", "Waiting").
            // If encoding fails or produces a non-string, skip this agent (do not persist an
            // undocumented status string). If the persisted status is non-in-flight (e.g.,
            // Pending due to the try_begin race), fall back to "active" — busy membership
            // proves the agent is mid-turn.
            let prev_str = match serde_json::to_value(prev_status) {
                Ok(serde_json::Value::String(s)) => {
                    // Non-in-flight statuses (pending/idle/error/deleted) mean we raced with
                    // persist_status. Busy membership is authoritative: use "active".
                    if matches!(
                        prev_status,
                        AgentStatus::Pending
                            | AgentStatus::RuntimeIdle
                            | AgentStatus::Idle
                            | AgentStatus::Error
                            | AgentStatus::Deleted
                    ) {
                        "active".to_string()
                    } else {
                        s
                    }
                }
                Ok(other) => {
                    tracing::warn!(agent_id = %id, status = ?prev_status, encoded = ?other, "graceful shutdown: status encoded to non-string, skipping interrupted_agent row");
                    continue;
                }
                Err(e) => {
                    tracing::warn!(agent_id = %id, status = ?prev_status, error = %e, "graceful shutdown: status encoding failed, skipping interrupted_agent row");
                    continue;
                }
            };
            // Insert the interrupted_agent row (idempotent upsert: if a prior crash captured
            // this agent and the daemon was restarted without the FE resolving it, the row
            // is refreshed to the latest state).
            if let Err(e) = self
                .services
                .store
                .insert_interrupted_agent(id, &workspace_id, &prev_str, &now)
                .await
            {
                tracing::warn!(agent_id = %id, workspace_id = %workspace_id, error = %e, "graceful shutdown: failed to insert interrupted_agent row");
            }
        }

        // Now tear down every agent's bookkeeping (settles to RuntimeIdle) and
        // collect the detached children, then kill all process groups in
        // parallel under ONE shared grace window — total teardown stays ~one
        // grace period regardless of agent count, instead of N sequential
        // SIGTERM→grace→SIGKILL cycles (which would blow past the 5s
        // SIGTERM→SIGKILL windows of `intentd stop` and the Electron sidecar).
        // `sync_store: false`: the graceful-shutdown detach drops the
        // in-memory stop-redelivery payload with the rest of the runtime
        // state, but must leave the durable `agent_stop_redelivery` row in
        // place — the whole point of the mirror (intent-hq/monorepo#1899) is
        // that the next boot rehydrates it for the follow-up turn.
        let mut children = Vec::new();
        for id in &ids {
            let (_, child) = self.detach_with_redelivery(id, None, false).await;
            if let Some(child) = child {
                children.push(child);
            }
        }
        kill_child_trees(children).await;
        // The daemon-managed Unsloth server is not an agent child — tear it
        // down explicitly so a clean shutdown never orphans it.
        self.unsloth.shutdown().await;
    }

    /// Idle-reap hook: evict up to `max` idle agents in LRU order (count-based;
    /// the LRU `acquire`-eviction companion).
    pub async fn reap_idle(&self, max: Option<usize>) -> usize {
        self.registry.evict_idle(max).await
    }

    /// TTL idle-reap sweep (§5.6/§6.7): evict every idle agent whose last
    /// activity is older than `ttl`, skipping any with an in-flight prompt (a
    /// live turn loop in `busy`). Active streaming agents are protected by the
    /// registry's `is_active` flag. Returns the number reaped.
    ///
    /// Claim-before-kill (monorepo#2118): instead of a bare busy check, each
    /// candidate is CLAIMED into `reap_claims` under the `busy` lock (the
    /// same lock `try_begin` claims under), so no turn can start between the
    /// eligibility check and the `kill().await` — a send racing the sweep
    /// loses `try_begin` and parks its message on the queue instead. After
    /// the sweep, any released agent with a ready queue gets a drain kick so
    /// a message that parked behind the claim starts a fresh turn (the agent
    /// respawns on demand) rather than stranding until the next queue event.
    pub async fn reap_idle_older_than(self: &Arc<Self>, ttl: Duration) -> usize {
        let (try_claim, release, released) = self.reap_claim_fns();
        let reaped = self
            .registry
            .evict_idle_older_than(ttl, try_claim, release)
            .await;
        self.kick_released(released).await;
        reaped
    }

    /// Budget-triggered idle reap (monorepo#2063 level 2): while the aggregate
    /// charge is over the installed budget, evict idle agents
    /// largest-attributed-subtree-first (Phase A attribution; LRU fallback
    /// when attribution is unavailable) — without waiting for the TTL or for
    /// a spawn attempt. No budget / no sample / under budget → no-op. Same
    /// claim-before-kill semantics (monorepo#2118) and post-sweep drain kick
    /// as [`Self::reap_idle_older_than`]. Returns the number reaped.
    pub async fn reap_over_budget(self: &Arc<Self>) -> usize {
        let (try_claim, release, released) = self.reap_claim_fns();
        let reaped = self
            .registry
            .evict_while_over_budget(try_claim, release)
            .await;
        self.kick_released(released).await;
        reaped
    }

    /// The claim-before-kill wiring (monorepo#2118) shared by every reap
    /// sweep: `try_claim` check-and-claims into `reap_claims` under the
    /// `busy` lock, `release` drops the claim and records the id for the
    /// post-sweep drain kick ([`Self::kick_released`]).
    #[allow(clippy::type_complexity)]
    fn reap_claim_fns(
        &self,
    ) -> (
        impl Fn(&AgentId) -> bool,
        impl Fn(&AgentId),
        Arc<Mutex<Vec<AgentId>>>,
    ) {
        let released: Arc<Mutex<Vec<AgentId>>> = Arc::new(Mutex::new(Vec::new()));
        let try_claim = {
            let busy = self.busy.clone();
            let claims = self.reap_claims.clone();
            move |id: &AgentId| {
                // Lock order busy → reap_claims, matching `try_begin`, so
                // "not busy → claimed" is atomic against a concurrent claim.
                // The insert result IS the claim: `false` (already present)
                // means an overlapping sweep holds this id — treating that as
                // a win would let its release drop the shared claim while
                // this sweep's kill is still in flight, reopening the window.
                let busy = busy.lock().unwrap();
                if busy.contains(id) {
                    return false;
                }
                claims.lock().unwrap().insert(id.clone())
            }
        };
        let release = {
            let claims = self.reap_claims.clone();
            let released = released.clone();
            move |id: &AgentId| {
                claims.lock().unwrap().remove(id);
                released.lock().unwrap().push(id.clone());
            }
        };
        (try_claim, release, released)
    }

    /// Drain kick for messages that parked behind a claim: with the claim
    /// released, a ready queue would otherwise sit unworked until the next
    /// external queue event (PROTOCOL §5.5 "never sits unworked").
    async fn kick_released(self: &Arc<Self>, released: Arc<Mutex<Vec<AgentId>>>) {
        let released = std::mem::take(&mut *released.lock().unwrap());
        for id in released {
            if !self.services.has_ready_to_send(&id) {
                continue;
            }
            if let Ok(session) = self.services.store.get_agent_session(&id).await {
                Arc::clone(self)
                    .try_drain_queue(id, session.workspace_id)
                    .await;
            }
        }
    }

    /// Tear down only the agent's child process + handle, without touching the
    /// worker or busy flag. Safe to call from within the worker itself (e.g.,
    /// retry loop). Use `stop()` for full teardown from external callers.
    async fn kill_child_only(&self, agent_id: &AgentId) {
        let handle = self.handles.lock().unwrap().remove(agent_id);
        if let Some(mut handle) = handle {
            let spawn_pid = handle.child_pid;
            if let Some(child) = handle._child.take() {
                kill_child_tree(child, spawn_pid).await;
            }
        }
        self.registry.deregister(agent_id);
    }

    /// Build the kill callback for `agent_id`: removing the handle signals the
    /// child's whole process group (SIGTERM→SIGKILL) and aborts its request
    /// loop, so no orphaned grandchildren linger.
    fn make_kill(&self, agent_id: AgentId) -> KillFn {
        let handles: Weak<Mutex<HashMap<AgentId, AgentHandle>>> = Arc::downgrade(&self.handles);
        Arc::new(move || {
            let handles = handles.clone();
            let id = agent_id.clone();
            Box::pin(async move {
                let removed = handles
                    .upgrade()
                    .and_then(|h| h.lock().unwrap().remove(&id));
                if let Some(mut handle) = removed {
                    let spawn_pid = handle.child_pid;
                    if let Some(child) = handle._child.take() {
                        kill_child_tree(child, spawn_pid).await;
                    }
                }
            })
        })
    }

    /// Arm the proactive child-exit watcher for a freshly installed handle
    /// (monorepo#764): a background task `try_wait`-polls the handle's owned
    /// child and, when it exits UNEXPECTEDLY while the agent is idle, removes
    /// the handle + deregisters the process slot and logs one WARN carrying
    /// the exit status (plus the stderr-capture dir hint when capture is on).
    ///
    /// Every deliberate teardown path (`kill_child_only`, `detach`/`stop`,
    /// the registry kill callback, `shutdown`, and `create_agent`'s stale
    /// reap) removes the handle from the map BEFORE killing the child, so the
    /// watcher observes the missing handle (or a respawn's pid mismatch) and
    /// stands down without firing. A mid-turn death (agent in `busy`) is left
    /// to the in-flight turn's terminal-failure teardown — the watcher keeps
    /// polling and only fires when the exit is observed while idle. The
    /// handle removal and registry deregistration happen atomically under the
    /// handles lock, and the dead child's process group is swept afterwards
    /// via the spawn-time pid (same-group descendants can outlive the
    /// leader). Persisted agent status is deliberately untouched: the agent
    /// stays resumable and the next message spawns a fresh child
    /// (`ensure_started`).
    ///
    /// Returns the watcher task: resolves `true` when it fired the
    /// unexpected-exit cleanup, `false` when it stood down.
    fn arm_child_exit_watcher(
        &self,
        agent_id: AgentId,
        child_pid: Option<u32>,
    ) -> JoinHandle<bool> {
        let handles = Arc::downgrade(&self.handles);
        let registry = Arc::downgrade(&self.registry);
        let busy = Arc::downgrade(&self.busy);
        let stderr_dir = self.agent_stderr_log_dir(&agent_id);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(CHILD_EXIT_POLL_INTERVAL).await;
                let (Some(handles), Some(registry), Some(busy)) =
                    (handles.upgrade(), registry.upgrade(), busy.upgrade())
                else {
                    return false; // manager torn down
                };
                // Snapshot busy membership BEFORE the handles lock (no nested
                // locking). Losing the race to a just-starting turn is safe:
                // the removal below only happens when the child is confirmed
                // dead, and `ensure_started` spawns fresh over a dead child.
                let is_busy = busy.lock().unwrap().contains(&agent_id);
                let exited = {
                    let mut map = handles.lock().unwrap();
                    // Handle gone: a deliberate teardown (stop / kill /
                    // shutdown / respawn reap) already cleaned up.
                    let Some(handle) = map.get_mut(&agent_id) else {
                        return false;
                    };
                    let Some(child) = handle._child.as_mut() else {
                        return false;
                    };
                    // A respawn installed a NEWER child under this agent id
                    // (which armed its own watcher) — stand down. A child
                    // already reaped by a prior `try_wait` reports
                    // `id() == None` and falls through: `try_wait` then
                    // returns its cached exit status.
                    if let Some(current) = child.id() {
                        if Some(current) != child_pid {
                            return false;
                        }
                    }
                    match child.try_wait() {
                        // Alive — keep polling. A transient probe error is
                        // treated as alive so it never forces a teardown.
                        Ok(None) | Err(_) => None,
                        // Exited. Mid-turn (busy) the in-flight turn's
                        // terminal-failure path owns the teardown: keep
                        // polling until it removes the handle (or the agent
                        // goes idle with the dead child still installed).
                        // Idle deaths are reaped here, removing the handle
                        // under the same lock the probe ran under so a
                        // concurrent respawn's fresh handle is never removed.
                        // The registry slot is deregistered inside the SAME
                        // critical section: a concurrent respawn registers
                        // its fresh slot only after observing the missing
                        // handle (which requires this lock), so a deregister
                        // outside the lock could land AFTER that fresh
                        // `register` and clobber the new child's slot.
                        Ok(Some(status)) => {
                            if is_busy {
                                None
                            } else {
                                let dead = map.remove(&agent_id);
                                registry.deregister(&agent_id);
                                Some((status, dead.and_then(|mut h| h._child.take())))
                            }
                        }
                    }
                };
                if let Some((status, dead_child)) = exited {
                    match &stderr_dir {
                        Some(dir) => tracing::warn!(
                            agent = %agent_id,
                            exit_status = %status,
                            "idle agent child exited unexpectedly; handle reaped (agent stderr captured at {})",
                            dir.display()
                        ),
                        None => tracing::warn!(
                            agent = %agent_id,
                            exit_status = %status,
                            "idle agent child exited unexpectedly; handle reaped"
                        ),
                    }
                    // The direct child is already reaped (`try_wait` above),
                    // but same-group descendants can survive it: sweep the
                    // process group via the spawn-time pid.
                    if let Some(dead_child) = dead_child {
                        kill_child_tree(dead_child, child_pid).await;
                    }
                    return true;
                }
            }
        })
    }
}

/// Poll interval for the proactive child-exit watcher
/// ([`AgentManager::arm_child_exit_watcher`], monorepo#764): how often an
/// installed handle's child is `try_wait`-probed for an unexpected exit.
/// Bounds the idle-death cleanup delay.
const CHILD_EXIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Grace period between SIGTERM and SIGKILL when tearing down a provider's
/// process group, giving the tree a chance to exit cleanly first.
#[cfg(unix)]
const PROCESS_GROUP_TERM_GRACE: Duration = Duration::from_secs(2);

/// Short bounded window after the SIGKILL sweep in [`kill_child_trees`] during
/// which the reap tasks are awaited so killed children are actually `wait()`ed
/// before returning (SIGKILLed children reap almost instantly).
#[cfg(unix)]
const KILL_SWEEP_REAP_GRACE: Duration = Duration::from_millis(500);

/// Terminate a spawned provider's WHOLE process tree (§5.6). The child is its
/// own process-group leader (`process_group(0)` at spawn), so `killpg(pgid,…)`
/// reaches every descendant — `kill_on_drop` alone only reaps the direct child,
/// orphaning grandchildren. SIGTERM first for a clean exit, then SIGKILL after a
/// grace period to sweep anything that ignored it. Descendants that escaped
/// into their OWN process groups survive the `killpg`, so they are snapshotted
/// before the kill and swept afterwards (`intent_acp::descendant_sweep`).
#[cfg(unix)]
async fn kill_child_tree(mut child: Child, spawn_pid: Option<u32>) {
    use intent_acp::{descendant_pids, sweep_escaped_descendants};
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;

    // `Child::id()` reads `None` once a `try_wait` liveness probe has reaped
    // the exit status (`handle_is_live`, the child-exit watcher), so fall
    // back to the spawn-time pid: the process GROUP can outlive its leader,
    // and same-group descendants still need the killpg sweep.
    let Some(pid) = child.id().or(spawn_pid) else {
        let _ = child.start_kill();
        return;
    };
    let descendants = descendant_pids(pid).await;
    let pgid = Pid::from_raw(pid as i32);
    let _ = killpg(pgid, Signal::SIGTERM);
    // Wait briefly for the group to drain, then SIGKILL the whole group so any
    // grandchild that ignored SIGTERM is still removed.
    let _ = tokio::time::timeout(PROCESS_GROUP_TERM_GRACE, child.wait()).await;
    let _ = killpg(pgid, Signal::SIGKILL);
    sweep_escaped_descendants(&descendants).await;
}

/// Non-unix fallback: no process groups, so fall back to killing the direct
/// child (`kill_on_drop` remains the safety net on drop).
#[cfg(not(unix))]
async fn kill_child_tree(mut child: Child, _spawn_pid: Option<u32>) {
    let _ = child.start_kill();
}

/// Parallel shutdown kill sweep: terminate MANY provider process trees under
/// ONE shared grace window. Every group is SIGTERMed up-front, then a single
/// [`PROCESS_GROUP_TERM_GRACE`] window covers the whole batch, then every
/// still-live group is SIGKILLed — so total teardown is ~one grace period
/// regardless of how many agents were running (unlike per-child
/// [`kill_child_tree`], which serialises one grace window per tree). The
/// pre-kill descendant snapshot (bounded at 2s for a hung `ps`) and the
/// post-kill escape sweep (one extra grace window when something escaped)
/// add hard-bounded overhead on top of that shared window.
#[cfg(unix)]
async fn kill_child_trees(children: Vec<(Child, Option<u32>)>) {
    use intent_acp::{descendant_pids_many, sweep_escaped_descendants};
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;

    // Phase 0: snapshot every tree's descendants BEFORE signalling (one shared
    // `ps` for the whole batch) so descendants that escaped into their own
    // process groups can be swept after the group kills — post-kill they
    // reparent to init and become invisible (`intent_acp::descendant_sweep`).
    // The spawn-time pid stands in for children a `try_wait` probe reaped.
    let roots: Vec<u32> = children
        .iter()
        .filter_map(|(c, spawn_pid)| c.id().or(*spawn_pid))
        .collect();
    let descendants = descendant_pids_many(&roots).await;

    // Phase 1: SIGTERM every group up-front so all trees start exiting at once.
    let mut pgids = Vec::new();
    let mut waits = Vec::new();
    for (mut child, spawn_pid) in children {
        match child.id().or(spawn_pid) {
            Some(pid) => {
                let pgid = Pid::from_raw(pid as i32);
                let _ = killpg(pgid, Signal::SIGTERM);
                pgids.push(pgid);
                // Reap on a task so all waits run concurrently.
                waits.push(tokio::spawn(async move {
                    let _ = child.wait().await;
                }));
            }
            None => {
                // Already reaped with no spawn-time pid — nothing to signal.
                let _ = child.start_kill();
            }
        }
    }
    // Phase 2: ONE shared grace window over the whole batch. Handles that
    // don't finish in time are kept so they can be awaited again after the
    // SIGKILL sweep.
    let deadline = tokio::time::Instant::now() + PROCESS_GROUP_TERM_GRACE;
    let mut pending = Vec::new();
    for mut w in waits {
        if tokio::time::timeout_at(deadline, &mut w).await.is_err() {
            pending.push(w);
        }
    }
    // Phase 3: concurrent SIGKILL sweep for anything that ignored SIGTERM
    // (no-op on groups that already exited).
    for pgid in pgids {
        let _ = killpg(pgid, Signal::SIGKILL);
    }
    // Phase 4: bounded reap — await the remaining wait tasks briefly so
    // SIGKILLed children are actually `wait()`ed before returning. Any
    // straggler past this window is still reaped by its background task; the
    // bound just keeps total shutdown within budget.
    let reap_deadline = tokio::time::Instant::now() + KILL_SWEEP_REAP_GRACE;
    for mut w in pending {
        let _ = tokio::time::timeout_at(reap_deadline, &mut w).await;
    }
    // Phase 5: sweep snapshotted descendants that survived the group kills
    // (foreign process groups). No-cost when nothing escaped; otherwise one
    // extra bounded SIGTERM→grace→SIGKILL pass over the survivors.
    sweep_escaped_descendants(&descendants).await;
}

/// Non-unix fallback: no process groups, so kill each direct child; the kills
/// are signal-only (no grace waits), so the sweep is already time-bounded.
#[cfg(not(unix))]
async fn kill_child_trees(children: Vec<(Child, Option<u32>)>) {
    for (child, spawn_pid) in children {
        kill_child_tree(child, spawn_pid).await;
    }
}

#[cfg(all(test, unix))]
mod kill_sweep_tests {
    //! Timing proof for the parallel shutdown kill sweep: N children that
    //! ignore SIGTERM must tear down in ~ONE shared grace window, not N
    //! sequential ones.

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_children_tear_down_in_one_shared_grace_window() {
        const N: usize = 4;
        let mut children = Vec::with_capacity(N);
        for _ in 0..N {
            let mut cmd = tokio::process::Command::new("sh");
            // Ignore SIGTERM so each child only dies on the SIGKILL sweep,
            // forcing the full grace window to elapse.
            cmd.args(["-c", "trap '' TERM; sleep 30"]);
            cmd.process_group(0);
            cmd.kill_on_drop(true);
            children.push((cmd.spawn().expect("spawn slow child"), None));
        }
        // Let each sh install its trap before SIGTERM arrives.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let start = std::time::Instant::now();
        kill_child_trees(children).await;
        let elapsed = start.elapsed();

        // Serial teardown would take ~N * grace (8s for 4 children); the
        // shared window must finish in ~one grace period (<4s total).
        assert!(
            elapsed < PROCESS_GROUP_TERM_GRACE * 2,
            "parallel sweep took {elapsed:?}, expected ~one {PROCESS_GROUP_TERM_GRACE:?} grace window"
        );
        // The children ignored SIGTERM, so the full shared grace must have
        // elapsed (proves the window ran once, not that children died early).
        assert!(
            elapsed >= PROCESS_GROUP_TERM_GRACE - Duration::from_millis(500),
            "sweep returned after {elapsed:?}, before the shared grace window elapsed"
        );
    }
}

/// One `text` ACP prompt content block for a user message.
fn text_prompt(content: &str) -> Vec<ContentBlock> {
    serde_json::from_value(json!([{ "type": "text", "text": content }])).unwrap_or_default()
}

/// STAB-114 combined-delivery progress check (`preempt_busy_turn`): has the
/// turn progressed past the last user message? Any non-user row after it is
/// progress, EXCEPT the interrupted marker row the preemption itself just
/// appended — and that exclusion applies ONLY while the marker row is
/// actually empty. The caller's zero-output snapshot is taken several awaits
/// before `interrupt_inner` re-reads the live-turn slot, so a first block
/// streaming in that window lands in the flushed row: a NON-empty marker row
/// IS progress and must keep blocking the combined re-delivery (duplicate
/// tool-call / re-run-side-effect hazard).
fn turn_progressed_after(
    messages: &[intent_core::AgentMessage],
    last_user_idx: usize,
    interrupted_row_id: Option<&String>,
) -> bool {
    messages.iter().skip(last_user_idx + 1).any(|m| {
        m.role != "user"
            && !(Some(&m.id) == interrupted_row_id
                && m.content.as_array().is_some_and(Vec::is_empty))
    })
}

/// Port of the FE `contextReferences` → `stdinContext` builder
/// (`agent-backend-handler.service.ts` — the ~3170–3248 block). Iterates the
/// raw JSON array in order and emits one context entry per reference,
/// joined by `\n\n`. Only entries the reference supports today are
/// materialised: type-specific labels for `selection` / `task` /
/// `code_chunk` / `file` (with content) / `linear-issue` / `github-issue` /
/// `sentry-issue` / `terminal`, a `Note: <id>` line for `note`, a bare
/// `File: <path>` line for a file reference whose content was not inlined
/// on the wire (the FE variant would try to read from disk here — that
/// on-disk fallback is deferred), and a fall-through that emits the raw
/// content when no `type` matches. Returns `None` when nothing produces a
/// non-empty entry so the caller can leave the prompt untouched.
fn build_stdin_context_from_context_references(refs: Option<&Value>) -> Option<String> {
    let arr = refs?.as_array()?;
    if arr.is_empty() {
        return None;
    }
    let mut parts: Vec<String> = Vec::new();
    for r in arr {
        let obj = match r.as_object() {
            Some(o) => o,
            None => continue,
        };
        // Content resolution mirrors the FE: `content` → `selectedText` →
        // `taskText` → `codeChunk` (first non-empty wins).
        let content = ["content", "selectedText", "taskText", "codeChunk"]
            .iter()
            .find_map(|k| obj.get(*k).and_then(Value::as_str))
            .filter(|s| !s.is_empty());
        // Same aliasing rule for the path field.
        let file_path = obj
            .get("path")
            .or_else(|| obj.get("filePath"))
            .and_then(Value::as_str);
        let ref_type = obj.get("type").and_then(Value::as_str);
        if let Some(content) = content {
            let entry = match ref_type {
                Some("selection") => format!("Selected text:\n{content}"),
                Some("task") => format!("Task:\n{content}"),
                Some("code_chunk") => format!("Code:\n{content}"),
                Some("file") => match file_path {
                    Some(p) => format!("File {p}:\n{content}"),
                    None => content.to_string(),
                },
                Some("linear-issue") => format!("Linear Issue:\n{content}"),
                Some("github-issue") => format!("GitHub Issue:\n{content}"),
                Some("sentry-issue") => format!("Sentry Issue:\n{content}"),
                Some("terminal") => {
                    let meta = obj.get("metadata").and_then(Value::as_object);
                    let terminal_id = meta
                        .and_then(|m| m.get("terminalId"))
                        .and_then(Value::as_str)
                        .unwrap_or("unknown");
                    let terminal_name = meta
                        .and_then(|m| m.get("terminalName"))
                        .and_then(Value::as_str)
                        .or_else(|| obj.get("title").and_then(Value::as_str))
                        .unwrap_or("Terminal");
                    format!("Terminal \"{terminal_name}\" (terminal_id: {terminal_id}):\n{content}")
                }
                _ => content.to_string(),
            };
            parts.push(entry);
        } else if ref_type == Some("file") {
            if let Some(p) = file_path {
                // Reference builds a bare `File: <path>` line when content is
                // not inlined and disk read is skipped/unavailable.
                parts.push(format!("File: {p}"));
            }
        } else if ref_type == Some("note") {
            let note_id = obj.get("noteId").and_then(Value::as_str).or_else(|| {
                obj.get("metadata")
                    .and_then(Value::as_object)
                    .and_then(|m| m.get("noteId"))
                    .and_then(Value::as_str)
            });
            if let Some(id) = note_id {
                parts.push(format!("Note: {id}"));
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}

/// Append one ACP content block per FE-supplied attachment to `blocks`
/// (reference-parity `acp-provider.ts`): image entries `{ data, mimeType }`
/// become `image` content blocks; file entries `{ data, mimeType, fileName }`
/// become `resource` blocks carrying a `BlobResourceContents` with the file
/// name lifted into the resource URI (`file:///<fileName>`). Malformed entries
/// (missing required fields, wrong types) are silently skipped so a partial
/// attachment array can never break the turn.
///
/// Combined interrupt delivery (STAB-114 / monorepo#1014): the preempted
/// message's attachments (`prepend_image_blocks` / `prepend_file_blocks`) are
/// emitted BEFORE this turn's own, so both messages' attachments survive with
/// the original's first.
fn append_attachment_blocks(blocks: &mut Vec<ContentBlock>, options: &TurnOptions) {
    push_image_blocks(blocks, options.prepend_image_blocks.as_ref());
    push_file_blocks(blocks, options.prepend_file_blocks.as_ref());
    push_image_blocks(blocks, options.image_blocks.as_ref());
    push_file_blocks(blocks, options.file_blocks.as_ref());
}

/// Extract a persisted user row's redeliverable payload — its text (joined
/// `text` blocks), image blocks, and file blocks — as a
/// [`QueuedPrepend`](crate::agent_ops::QueuedPrepend). Shared by the
/// zero-output preemption path ([`AgentManager::preempt_busy_turn`],
/// monorepo#1014) and the zero-output user-stop redelivery arm
/// (intent-hq/monorepo#1757).
fn extract_user_prepend(content: &Value) -> crate::agent_ops::QueuedPrepend {
    let blocks = content.as_array();
    let text = blocks
        .map(|blocks| {
            blocks
                .iter()
                .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|b| b.get("text").and_then(Value::as_str))
                .collect::<Vec<&str>>()
                .join("\n")
        })
        .unwrap_or_default();
    let pick = |kind: &str| -> Option<Value> {
        let items: Vec<Value> = blocks?
            .iter()
            .filter(|b| b.get("type").and_then(Value::as_str) == Some(kind))
            .cloned()
            .collect();
        if items.is_empty() {
            None
        } else {
            Some(Value::Array(items))
        }
    };
    crate::agent_ops::QueuedPrepend {
        content: (!text.is_empty()).then_some(text),
        image_blocks: pick("image"),
        file_blocks: pick("file"),
    }
}

/// Merge two optional JSON block arrays, `first`'s entries preceding
/// `second`'s (the older payload stays first — transcript order). Used by the
/// zero-output preemption path to combine an entry-carried `prepend_*`
/// payload with the just-preempted message's attachments instead of
/// clobbering one with the other.
fn merge_block_arrays(first: Option<Value>, second: Option<Value>) -> Option<Value> {
    match (first, second) {
        (Some(Value::Array(mut a)), Some(Value::Array(b))) => {
            a.extend(b);
            Some(Value::Array(a))
        }
        (Some(a), None) | (None, Some(a)) => Some(a),
        (Some(a), Some(_)) => Some(a),
        (None, None) => None,
    }
}

/// Push one `image` content block per well-formed `{ data, mimeType }` entry.
fn push_image_blocks(blocks: &mut Vec<ContentBlock>, image_blocks: Option<&Value>) {
    if let Some(imgs) = image_blocks.and_then(Value::as_array) {
        for img in imgs {
            let data = img.get("data").and_then(Value::as_str);
            let mime = img.get("mimeType").and_then(Value::as_str);
            if let (Some(data), Some(mime)) = (data, mime) {
                if let Ok(block) = serde_json::from_value::<ContentBlock>(json!({
                    "type": "image",
                    "data": data,
                    "mimeType": mime,
                })) {
                    blocks.push(block);
                }
            }
        }
    }
}

/// The system notice appended after note-referenced images are inlined
/// (Fidelity B, PROTOCOL §5.5). Wording owned by the harness (H6).
pub(crate) fn note_images_notice(n: usize) -> String {
    crate::harness::latest().note_images_notice(n)
}

/// The attachment-reference notice (PROTOCOL §5.5): metadata plus the
/// `ws.file.getAttachment` retrieval instruction — the file bytes never ride
/// the prompt for reference blocks. Wording owned by the harness (H6).
pub(crate) fn attachment_reference_notice(
    name: &str,
    mime: Option<&str>,
    size: Option<u64>,
    id: &str,
) -> String {
    crate::harness::latest().attachment_reference_notice(name, mime, size, id)
}

/// Push one content block per well-formed file entry: inline
/// `{ data, mimeType, fileName }` entries become `resource` blocks carrying
/// the blob; attachment-reference `{ attachmentId, fileName }` entries
/// (PROTOCOL §5.5) become a `text` attachment notice naming the metadata and
/// directing the model to `ws.file.getAttachment(attachmentId)` — the file
/// bytes never ride the prompt for reference blocks.
fn push_file_blocks(blocks: &mut Vec<ContentBlock>, file_blocks: Option<&Value>) {
    if let Some(files) = file_blocks.and_then(Value::as_array) {
        for file in files {
            let data = file.get("data").and_then(Value::as_str);
            let mime = file.get("mimeType").and_then(Value::as_str);
            let name = file.get("fileName").and_then(Value::as_str);
            let attachment_id = file
                .get("attachmentId")
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty());
            if let (Some(id), Some(name)) = (attachment_id, name) {
                let size = file.get("size").and_then(Value::as_u64);
                let text = attachment_reference_notice(name, mime, size, id);
                if let Ok(block) =
                    serde_json::from_value::<ContentBlock>(json!({ "type": "text", "text": text }))
                {
                    blocks.push(block);
                }
            } else if let (Some(data), Some(mime), Some(name)) = (data, mime, name) {
                if let Ok(block) = serde_json::from_value::<ContentBlock>(json!({
                    "type": "resource",
                    "resource": {
                        "blob": data,
                        "mimeType": mime,
                        "uri": format!("file:///{name}"),
                    },
                })) {
                    blocks.push(block);
                }
            }
        }
    }
}

/// Resolved spawn inputs for an agent: the provider config plus the owned model,
/// cwd, provider binary path (when resolved), and extra env the borrowing
/// [`SpawnOptions`] reference during a spawn.
struct ResolvedSpawn {
    provider: ProviderConfig,
    model: Option<String>,
    /// The session's persisted `reasoningEffort` (PROTOCOL §5.5, Option B),
    /// threaded into `SpawnOptions.reasoning_effort` so the codex spawn path
    /// keeps emitting `-c model_reasoning_effort=…` for sessions whose
    /// compound `{base}/{effort}` model id was split by migration 0080.
    reasoning_effort: Option<String>,
    cwd: PathBuf,
    provider_binary: Option<PathBuf>,
    extra_env: BTreeMap<String, String>,
    /// When provider_binary is None and the provider has a fallback_npx_package,
    /// this is the resolved npx path. Otherwise None.
    npx_fallback_binary: Option<PathBuf>,
    /// The package name to pass to npx when npx_fallback_binary is set.
    npx_fallback_package: Option<&'static str>,
    /// Unsloth-managed server endpoint for the `unsloth` provider, filled in
    /// by [`AgentManager::ensure_started`] via
    /// [`crate::unsloth_server::UnslothServerManager::ensure_endpoint`]
    /// before a fresh child spawns. Always `None` for other providers (and
    /// straight out of [`resolve_spawn`], which never starts the server).
    unsloth_endpoint: Option<intent_providers::UnslothEndpoint>,
}

/// Resolve the provider config, model, cwd, and extra env for spawning an
/// The default agent type for an agent with no specialist-declared `agentType`
/// (the foreground/interactive type, which has no internal tool denylist).
const DEFAULT_AGENT_TYPE: &str = "interactive";

/// Derive the spawn `agent_type` for a session (SP-B): when the session was
/// created with a specialist that declares an `agentType` frontmatter scalar
/// (e.g. `ralph` → `ralph-loop`), that value becomes the agent's type so the
/// matching internal tool denylist (§18.4,
/// [`get_tool_denylist_for_agent_type`](intent_acp::get_tool_denylist_for_agent_type))
/// engages on spawn. Otherwise (no specialist, or a specialist without
/// `agentType`) the existing [`DEFAULT_AGENT_TYPE`] is kept — no regression for
/// plain agents. The specialist project tier resolves from the workspace path.
fn derive_agent_type(
    services: &Services,
    session: &AgentSession,
    workspace: Option<&intent_core::Workspace>,
) -> String {
    if let Some(specialist) = session.specialist.as_deref().filter(|s| !s.is_empty()) {
        let workspace_path = workspace
            .and_then(|w| w.path.clone().or_else(|| w.worktree_path.clone()))
            .map(PathBuf::from);
        if let Some(agent_type) =
            services.specialist_agent_type(specialist, workspace_path.as_deref())
        {
            return agent_type;
        }
    }
    DEFAULT_AGENT_TYPE.to_string()
}

/// Effective provider id for a session. Provider precedence: when the model
/// carries an explicit `provider:` prefix (e.g., "opencode:kimi-k3"), that
/// prefix wins over `session.provider`, because a cross-provider model switch
/// should spawn the new provider's binary. `session.provider` is only used as
/// a fallback for bare model ids, then `configured_default` (the settings-
/// derived default — `model.default` prefix, else `providers.active`), then
/// the first registered provider (neutral positional last resort).
/// Delegates to [`crate::agent_session::resolve_provider_id`], which also
/// guards against malformed compound ids like `:sonnet` (empty prefixes fall
/// through to the provider field / configured default / last resort).
fn session_provider_id(session: &AgentSession, configured_default: Option<&str>) -> String {
    crate::agent_session::resolve_provider_id(
        session.model.as_deref(),
        session.provider.as_deref(),
        configured_default,
    )
}

/// Fallback phrasing for the workspace-naming nudge when the provider's MCP
/// tool naming convention is unknown (or its workspace-MCP wiring hasn't
/// landed yet). Wording owned by the harness (H5).
pub(crate) use crate::harness::v1::GENERIC_NAMING_TOOL_REFERENCE;

/// Provider-correct spelling of the workspace-MCP rename tool for the naming
/// nudge (e.g. auggie → `set_workspace_title_workspace-mcp`, opencode →
/// `workspace-mcp_set_workspace_title`). Wording owned by the harness (H5).
pub(crate) fn workspace_naming_tool_reference(provider_id: &str) -> &'static str {
    crate::harness::latest().naming_tool_reference(provider_id)
}

/// Resolve everything needed to spawn (or respawn) this
/// agent's child from its persisted session + workspace. The provider id comes
/// from [`session_provider_id`]. The `mock` provider (E2E) reads its script
/// from `MOCK_AGENT_SCRIPT_PATH` and enables `--mcp-config` so a daemon-spawned
/// child reaches the per-agent workspace MCP server, forwarding
/// `MOCK_AGENT_BEHAVIOR` to the child. npx-only providers (claude-code) are
/// always spawned via `npx -y <pinned package>` — no local-binary discovery.
/// Other providers resolve their binary to an absolute path using the
/// precedence: `providers.paths` map (keyed by the binary-owning provider,
/// [`ProviderConfig::primary_binary_provider_id`]) → native-installer location
/// where one exists (e.g. `~/.opencode/bin`) → `~/.augment/bin/<command>`
/// (auggie back-compat tier) → enhanced PATH scan.
fn resolve_spawn(
    session: &AgentSession,
    workspace: Option<&intent_core::Workspace>,
    settings: &intent_core::settings_file::SettingsFile,
    chief_cwd_root: Option<&Path>,
) -> Result<ResolvedSpawn> {
    let provider_id = session_provider_id(
        session,
        crate::agent_session::derived_default_provider(settings).as_deref(),
    );
    // Whitespace-bearing bare ids are effective-model display names persisted
    // onto `model` by the pre-monorepo#1534 D13 resolution (legacy rows, e.g.
    // `claude-code:Opus 4.8`) — stats/attribution values, not spawnable model
    // ids. They must not reach `SpawnOptions.model` (CLI `--model` flags,
    // codex config args, opencode env config); dropping them spawns on the
    // provider default, exactly what the placeholder resolved from.
    let model = session
        .model
        .as_ref()
        .map(|m| intent_providers::parse_compound_model_id(m).1)
        .filter(|m| !m.is_empty() && !m.contains(char::is_whitespace));
    // Session-level reasoning effort (PROTOCOL §5.5, Option B): threaded to
    // `SpawnOptions.reasoning_effort` so the codex spawn path applies it as
    // `-c model_reasoning_effort=…` (an effort embedded in a compound
    // `{base}/{effort}` model id still wins inside the codex arg builder).
    let reasoning_effort = session
        .reasoning_effort
        .clone()
        .filter(|e| !e.trim().is_empty());
    // Chief has no worktree/repo on disk, so its children spawn in the
    // dedicated, daemon-owned, empty `<data_dir>/chief-cwd` directory
    // (STAB-50): providers that index their cwd (auggie with
    // `--allow-indexing`) previously ingested an arbitrarily large shared
    // `/tmp` and blew past their V8 heap cap. Created on demand so a fresh
    // data dir spawns fine; a creation failure falls through to the temp-dir
    // catch-all rather than blocking the spawn.
    //
    // Task 3: If the session has a sandbox_path (CoW isolation), use it as the cwd.
    let cwd = session
        .sandbox_path
        .clone()
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
        .or_else(|| {
            workspace
                .and_then(|w| w.path.clone().or_else(|| w.worktree_path.clone()))
                .map(PathBuf::from)
                .filter(|p| p.is_dir())
        })
        .or_else(|| {
            workspace
                .filter(|w| w.id.is_chief())
                .and(chief_cwd_root)
                .map(|root| {
                    if let Err(e) = intent_core::chief_cwd::create_chief_cwd_dir(root) {
                        tracing::warn!(
                            error = %e,
                            path = %root.display(),
                            "failed to create chief spawn cwd; falling back to temp dir"
                        );
                    }
                    root.to_path_buf()
                })
                .filter(|p| p.is_dir())
        })
        .unwrap_or_else(std::env::temp_dir);

    let mut extra_env = BTreeMap::new();
    if provider_id == "mock" {
        let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").map_err(|_| {
            Error::Internal("mock provider requires MOCK_AGENT_SCRIPT_PATH".to_string())
        })?;
        // `'static` leaks are bounded to the mock (E2E-only) path; real
        // providers carry static `base_args` and never leak.
        let script_static: &'static str = Box::leak(script.into_boxed_str());
        let base_args: &'static [&'static str] = Box::leak(vec![script_static].into_boxed_slice());
        if let Ok(behavior) = std::env::var("MOCK_AGENT_BEHAVIOR") {
            extra_env.insert("MOCK_AGENT_BEHAVIOR".to_string(), behavior);
        }
        let base = intent_providers::find_provider("mock")
            .ok_or_else(|| Error::Internal("mock provider missing from registry".to_string()))?;
        // `MOCK_AGENT_SESSION_MCP=1` flips the mock from `--mcp-config` file
        // delivery to ACP session-setup delivery (`session/new` `mcpServers`),
        // so the E2E suite can exercise the claude-code/codex/droid/grok wire
        // path (STAB-156) against the real daemon.
        let session_mcp = std::env::var("MOCK_AGENT_SESSION_MCP").is_ok_and(|v| v == "1");
        // `MOCK_AGENT_CONFIG_OPTION_MODEL=1` marks the mock as a
        // config-option-model provider (claude-code-like), so the E2E suite
        // can exercise the post-session `session/set_config_option` model
        // application against the real daemon.
        let config_option_model =
            std::env::var("MOCK_AGENT_CONFIG_OPTION_MODEL").is_ok_and(|v| v == "1");
        let provider = ProviderConfig {
            command: "node",
            base_args,
            supports_authenticate: true,
            supports_mcp_config: !session_mcp,
            mcp_config_flag: if session_mcp {
                None
            } else {
                Some("--mcp-config")
            },
            supports_session_mcp_servers: session_mcp,
            supports_config_option_model: config_option_model,
            ..*base
        };
        return Ok(ResolvedSpawn {
            provider,
            model: None,
            reasoning_effort: None,
            cwd,
            provider_binary: None,
            extra_env,
            npx_fallback_binary: None,
            npx_fallback_package: None,
            unsloth_endpoint: None,
        });
    }

    let provider = *intent_providers::provider_config(&provider_id);
    // Filled by `ensure_started` (unsloth spawn gate) — pure resolution here
    // never starts the managed server.
    let unsloth_endpoint = None;

    // npx-only providers (claude-code) are spawned exclusively via
    // `npx -y <pinned package>`; local-binary discovery (settings path /
    // managed bin / PATH scan) is skipped entirely.
    if provider.npx_only_package.is_some() {
        if read_provider_path_setting(settings, &provider_id).is_some() {
            tracing::warn!(
                provider_id = provider_id,
                "providers.paths override ignored: {} always spawns via pinned npx",
                provider_id
            );
        }
        let (npx_binary, npx_package) = resolve_npx_only(&provider, intent_providers::find_npx())?;
        return Ok(ResolvedSpawn {
            provider,
            model,
            reasoning_effort,
            cwd,
            provider_binary: None,
            extra_env,
            npx_fallback_binary: Some(npx_binary),
            npx_fallback_package: Some(npx_package),
            unsloth_endpoint,
        });
    }

    // Resolve provider binary using the precedence: setting → native
    // installer → ~/.augment/bin → PATH (`find_provider_binary`'s tiers).
    // The `providers.paths` key is the provider that OWNS the primary binary
    // ([`ProviderConfig::primary_binary_provider_id`]): unsloth rides the
    // opencode binary, so its primary honors `providers.paths["opencode"]`,
    // while `providers.paths["unsloth"]` targets the unsloth CLI in the
    // managed-server lifecycle (`ensure_started`'s unsloth spawn gate).
    let binary_provider_id = provider.primary_binary_provider_id();
    let explicit_path = read_provider_path_setting(settings, binary_provider_id);
    let provider_binary = intent_providers::find_provider_binary(
        binary_provider_id,
        provider.command,
        explicit_path.as_deref(),
    );

    // When the provider binary is not found but the provider has a fallback npx
    // package, resolve npx itself and record the fallback decision
    let (npx_fallback_binary, npx_fallback_package) = if provider_binary.is_none() {
        if let Some(pkg) = provider.fallback_npx_package {
            if let Some(npx_path) = intent_providers::find_npx() {
                tracing::info!(
                    provider_id = provider_id,
                    npx_path = ?npx_path,
                    package = pkg,
                    "provider binary not found; falling back to npx"
                );
                (Some(npx_path), Some(pkg))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    Ok(ResolvedSpawn {
        provider,
        model,
        reasoning_effort,
        cwd,
        provider_binary,
        extra_env,
        npx_fallback_binary,
        npx_fallback_package,
        unsloth_endpoint,
    })
}

/// Resolve the npx spawn inputs for an npx-only provider. `npx_path` is the
/// caller-supplied `find_npx()` result (parameterized as a test seam). Missing
/// npx is a hard, user-facing error — there is no local-binary fallback.
fn resolve_npx_only(
    provider: &ProviderConfig,
    npx_path: Option<PathBuf>,
) -> Result<(PathBuf, &'static str)> {
    let pkg = provider.npx_only_package.ok_or_else(|| {
        Error::Internal(format!(
            "provider {} is not configured for npx-only spawning",
            provider.id
        ))
    })?;
    let npx = npx_path.ok_or_else(|| {
        // InvalidInput (not Internal): this is an environment misconfiguration,
        // and its Display survives the JSON-RPC envelope (`domain_to_rpc` masks
        // Internal messages behind a literal "Internal error").
        Error::InvalidInput(format!(
            "npx not found — {} is required to run {}. Install Node.js (which provides npx) and try again.",
            intent_providers::CLAUDE_AGENT_ACP_NODE_REQUIREMENT,
            provider.display_name
        ))
    })?;
    tracing::info!(
        provider_id = provider.id,
        npx_path = ?npx,
        package = pkg,
        "spawning npx-only provider via pinned npx package"
    );
    Ok((npx, pkg))
}

/// Rebuild the caller's [`SpawnOptions`] for `create_agent`, injecting the
/// generated rules/MCP config paths while preserving every other field of the
/// incoming opts. Notably the npx fallback pair must survive: dropping it
/// makes `build_command` fall back to the bare provider command and fail with
/// ENOENT when no local provider binary exists (codex fallback / claude-code
/// npx-only spawns).
fn rebuild_spawn_opts<'a>(
    opts: &SpawnOptions<'a>,
    rules_file_path: Option<&'a str>,
    mcp_config_path: Option<&'a str>,
    env_mcp_config: Option<&'a str>,
) -> SpawnOptions<'a> {
    let mut spawn_opts = SpawnOptions::new(opts.provider);
    spawn_opts.model = opts.model;
    spawn_opts.reasoning_effort = opts.reasoning_effort;
    spawn_opts.cwd = opts.cwd;
    spawn_opts.rules_file = opts.rules_file.or(rules_file_path);
    spawn_opts.quiet = opts.quiet;
    spawn_opts.provider_binary = opts.provider_binary;
    spawn_opts.npx_fallback_binary = opts.npx_fallback_binary;
    spawn_opts.npx_fallback_package = opts.npx_fallback_package;
    spawn_opts.extra_env = opts.extra_env.clone();
    spawn_opts.tools_to_remove = opts.tools_to_remove.clone();
    spawn_opts.mcp_config_file = mcp_config_path;
    spawn_opts.env_mcp_config = env_mcp_config;
    spawn_opts.unsloth_endpoint = opts.unsloth_endpoint;
    spawn_opts
}

/// Append the github.com-scoped daemon-backed credential-helper env pair
/// (monorepo#884 Phase 2.2) to a provider spawn's extra env:
/// `GIT_CONFIG_PARAMETERS` carrying the sq-quoted
/// `intentd git-credential` helper entry — **no token bytes ever enter the
/// child environment** (the helper fetches the credential from the daemon
/// over UDS on demand). The daemon's own `GIT_CONFIG_PARAMETERS` (which the
/// child would inherit) is preserved by appending after it, so existing
/// setups keep winning. Setting off or an unresolvable daemon binary path ⇒
/// no changes; pre-existing caller keys are never clobbered.
fn inject_git_credential_env(extra_env: &mut BTreeMap<String, String>, expose: bool) {
    if !expose {
        return;
    }
    let Some(intentd) = crate::daemon_exe_path() else {
        return;
    };
    let inherited = std::env::var(intent_git::auth::GIT_CONFIG_PARAMETERS_ENV).ok();
    for (key, value) in intent_git::auth::daemon_helper_env(&intentd, inherited.as_deref()) {
        extra_env.entry(key).or_insert(value);
    }
}

/// Read the provider path from the `providers.paths` map setting, if set.
fn read_provider_path_setting(
    settings: &intent_core::settings_file::SettingsFile,
    provider_id: &str,
) -> Option<String> {
    let path = settings.providers.paths.get(provider_id)?;
    let trimmed = path.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Background turn worker: drive the current message to completion, then drain
/// any queued messages (flipping each to in-flight). After the slot is released
/// the loop re-checks the queue and reclaims the slot **as long as another
/// message is waiting**; only when the queue is truly empty (or a concurrent
/// worker has won the slot) does the loop exit. Each dequeue publishes
/// `agent:queue:updated` so subscribed FE clients mirror the live queue
/// (§5.5/§6). Spawn/turn failures are logged so the loop always releases the
/// in-flight slot and worker handle.
async fn run_message_worker(
    mgr: Arc<AgentManager>,
    agent_id: AgentId,
    workspace_id: WorkspaceId,
    initial_content: String,
    initial_options: TurnOptions,
    initial_persisted: bool,
) {
    let mut content = initial_content;
    // Only the first turn carries the caller's per-turn prompt-assembly hints
    // (`stdinContext` / `noteIds` / `contextReferences`) — a `QueuedMessage`
    // has none. Attachment blocks (`imageBlocks` / `fileBlocks`) are captured
    // at enqueue time and DO ride along on drain, so a queued turn reaches the
    // agent with the same ACP content blocks as if it had run inline.
    let mut options = initial_options;
    // Whether the CURRENT turn's user row durably reached the transcript
    // (STAB-51). Terminal spawn/turn failures thread it into the requeue so
    // a failed pre-turn persist is re-attempted by the `agent.retry` drain.
    let mut user_persisted = initial_persisted;
    // One-shot silent-redrive budget for the CURRENT message (monorepo#764):
    // a transport-closed prompt failure BEFORE any streamed output redrives
    // the same prompt once on a fresh child instead of surfacing the STAB-6
    // Retry. Reset at each queue-drain handoff so every message gets its own
    // budget; a second pre-output failure on the same message takes the
    // terminal path.
    let mut silent_redrive_used = false;
    // Consecutive idle-timeout streak (warn-and-continue): incremented on
    // each back-to-back silent timeout, reset to zero on any completed turn
    // and restarted at one when the timed-out turn streamed output first
    // (intervening activity). While the streak is within
    // [`MAX_CONSECUTIVE_IDLE_TIMEOUT_REDRIVES`] the worker injects a warning
    // turn instead of failing; past it the timeout takes the terminal path.
    let mut consecutive_idle_timeouts: u32 = 0;
    'outer: loop {
        // Turn-start budget re-check (monorepo#2063 B8): a warm idle process
        // about to go active re-checks the aggregate budget like a spawn
        // would — queued behind eviction, never refused. Sits at the top of
        // the loop, BEFORE `run_turn` marks the process active, so every
        // turn (initial send and each queue-drain handoff) gates while the
        // agent is still idle — and never mid-turn, where the process is
        // marked active and `acquire_turn_start` admits immediately. An
        // unregistered agent also admits immediately: its spawn below goes
        // through `create_agent`'s `acquire`, the existing gate.
        mgr.registry.acquire_turn_start(&agent_id).await;
        match retry_spawn(&mgr, &agent_id, &workspace_id).await {
            Ok(acp_session_id) => {
                // Clear any persisted completion report at the start of this turn
                // (including queue-drained turns). Skip the store write when no
                // report is set; the `agent:idle` wake for a prior turn that set a
                // report still includes it because the clear runs at the NEXT turn's
                // begin (after the `agent:idle` emit at the prior turn's end).
                // Stale redrives (#576) suppress the clear entirely so the
                // already-delivered report stays queryable via `agent.get` — a
                // genuine re-report still overwrites it through `reportToParent`.
                if !options.suppress_report_clear {
                    mgr.clear_completion_report_if_present(&agent_id, &workspace_id)
                        .await;
                }
                // A pending attention request retires on a USER-ORIGIN
                // delivery (sendMessage front door, sendQueuedMessageNow,
                // editAndRegenerate, drained user-origin queue entry) for
                // every agent, and ALSO on an automatic delivery (A2A send,
                // parent wake, sendToTask, wakeOrCreate, subscription batch,
                // drained automatic entry) when the session is a CHILD
                // (`parent_agent_id` set) or BACKGROUND (`is_background`)
                // agent — the parent/coordinator is those agents' attention
                // surface, so its follow-up is the acknowledgement. Top-level
                // foreground agents keep the user-only dismissal: an
                // automatic/system message must never dismiss a request the
                // user has not seen. Fail closed on a session-load error
                // (leave the request pending).
                let clear_attention = options.origin.is_user()
                    || match mgr.services.store.get_agent_session(&agent_id).await {
                        Ok(s) => s.parent_agent_id.is_some() || s.is_background,
                        Err(e) => {
                            tracing::warn!(
                                agent = %agent_id,
                                error = %e,
                                "attention-clear gate: session lookup failed; leaving request pending"
                            );
                            false
                        }
                    };
                if clear_attention {
                    mgr.clear_attention_request_if_present(&agent_id, &workspace_id)
                        .await;
                }
                let prompt = mgr
                    .build_turn_prompt(&agent_id, &workspace_id, &content, &options)
                    .await;
                match mgr
                    .run_turn(
                        &agent_id,
                        &workspace_id,
                        &acp_session_id,
                        prompt,
                        options.turn_id.as_deref(),
                    )
                    .await
                {
                    Ok(_stop_reason) => {
                        // A successful turn resets the identical-failure
                        // streak (monorepo#840): the session is provably not
                        // poisoned.
                        mgr.services.clear_failure_streak(&agent_id);
                        // A completed turn also resets the consecutive
                        // idle-timeout streak (warn-and-continue).
                        consecutive_idle_timeouts = 0;
                    }
                    Err(e) => {
                        if is_benign_turn_error(&e) {
                            // Concurrent stop/cancel won the turn — not a failure.
                            // Keep draining: any queued message re-spawns lazily.
                            tracing::warn!(agent = %agent_id, error = %e, "agent turn ended (benign)");
                            consecutive_idle_timeouts = 0;
                        } else if prompt_idle_timeout_error(&e) {
                            // Warn-and-continue: the turn went the whole idle
                            // window silent. `run_prompt_turn` already flushed
                            // the partial transcript and emitted a NORMAL
                            // `agent:stream:end` while suppressing
                            // `agent:failed` — this arm decides between a
                            // warning redrive and (past the consecutive cap)
                            // the terminal path.
                            consecutive_idle_timeouts = if idle_timeout_turn_streamed(&e) {
                                // Streamed output is intervening activity:
                                // restart the back-to-back accounting at
                                // this timeout.
                                1
                            } else {
                                consecutive_idle_timeouts + 1
                            };
                            if consecutive_idle_timeouts <= MAX_CONSECUTIVE_IDLE_TIMEOUT_REDRIVES {
                                // Settle the hung prompt server-side WITHOUT
                                // killing the child (interrupt keep-alive
                                // semantics). When the transport is already
                                // dead, tear the child down so the warning
                                // turn spawns fresh instead of hanging again.
                                let conn = mgr
                                    .handles
                                    .lock()
                                    .unwrap()
                                    .get(&agent_id)
                                    .map(|h| h.connection.clone());
                                // Response watermark BEFORE `session/cancel`:
                                // the idle timeout dropped `req_fut`, so the
                                // hung prompt's pending-map entry is already
                                // gone (drop guard) — the watermark is the
                                // only way to observe its late response.
                                let since = conn.as_ref().map(|c| c.response_seq());
                                let settled = match conn.as_deref() {
                                    Some(c) => {
                                        cancel_and_settle_idle_prompt(c, &agent_id, &acp_session_id)
                                            .await
                                    }
                                    None => false,
                                };
                                // STAB-124 for the idle-timeout path:
                                // attribution on the turn-less
                                // `session/update` channel is positional,
                                // so the cancelled turn's stragglers
                                // (tool_call_update echoes, tail chunks)
                                // must be discarded before the warning
                                // turn's transcript starts consuming the
                                // channel. The cancelled prompt's RESPONSE
                                // is the deterministic end-of-turn boundary
                                // on the child's ordered stdout — once it
                                // lands, every straggler is already
                                // buffered, so the `try_recv` sweep below
                                // is complete, not racy. Bounded, cannot
                                // deadlock: the timed-out worker released
                                // the receiver lock when `run_prompt_turn`
                                // returned, and the await is
                                // timeout-bounded. A boundary that never
                                // lands means the child may keep streaming
                                // stragglers indefinitely — treated as NOT
                                // settled below, so the child is torn down
                                // and the warning turn spawns fresh (fresh
                                // channel + reset watermark: bleed
                                // impossible).
                                let boundary_landed = if settled {
                                    match (conn.as_ref(), since) {
                                        (Some(conn), Some(since)) => {
                                            let landed = conn
                                                .await_response_after(since, Duration::from_secs(2))
                                                .await;
                                            if !landed {
                                                tracing::warn!(
                                                    agent = %agent_id,
                                                    "idle-timeout settle: cancelled prompt's response did not land within the watermark window; tearing the child down so the warning turn starts on a fresh channel"
                                                );
                                            }
                                            landed
                                        }
                                        _ => false,
                                    }
                                } else {
                                    false
                                };
                                if !boundary_landed {
                                    mgr.kill_child_only(&agent_id).await;
                                } else {
                                    // Hold the notifications lock across the
                                    // drain so the warning turn cannot start
                                    // consuming the channel mid-sweep.
                                    let notes = mgr
                                        .handles
                                        .lock()
                                        .unwrap()
                                        .get(&agent_id)
                                        .map(|h| h.notifications.clone());
                                    if let Some(notes) = notes {
                                        let mut guard = notes.lock().await;
                                        Services::drain_replay_notifications(&mut guard).await;
                                    }
                                }
                                tracing::warn!(
                                    agent = %agent_id,
                                    streak = consecutive_idle_timeouts,
                                    error = %e,
                                    "prompt idle timeout — injecting a warning turn and continuing"
                                );
                                // The warning is a NEW persisted, user-visible
                                // user-role message with its own turn id; the
                                // `agent:message` echo inside `persist_user`
                                // lets clients render it. The busy slot stays
                                // held and this `continue` bypasses the queue
                                // drain below, so queued messages can NOT jump
                                // ahead of the warning turn.
                                let warning = idle_timeout_warning_text(
                                    intent_acp::session::prompt_idle_timeout(),
                                );
                                let warning_turn_id = new_message_id();
                                user_persisted = persist_user(
                                    &mgr,
                                    &agent_id,
                                    &workspace_id,
                                    &warning,
                                    None,
                                    None,
                                    None,
                                    Some(&warning_turn_id),
                                )
                                .await;
                                content = warning;
                                options = TurnOptions {
                                    turn_id: Some(warning_turn_id),
                                    ..TurnOptions::default()
                                };
                                // New message → fresh silent-redrive budget
                                // (monorepo#764).
                                silent_redrive_used = false;
                                // Fail closed (#547): a warning row that never
                                // reached the transcript must not drive a turn.
                                if !user_persisted {
                                    handle_drain_persist_failure(
                                        &mgr,
                                        &agent_id,
                                        &workspace_id,
                                        &content,
                                        &options,
                                    )
                                    .await;
                                    mgr.release_in_flight_slot(&agent_id).await;
                                    break 'outer;
                                }
                                continue 'outer;
                            }
                            // Cap exceeded: the 4th back-to-back silent
                            // timeout takes today's terminal path.
                            // `run_prompt_turn` suppressed `agent:failed` for
                            // the timeout (its stream:end was the normal one),
                            // so emit the failed half here —
                            // `handle_terminal_turn_failure` skips its own
                            // pair for the PROMPT_FAILED_PREFIX wrapper.
                            tracing::warn!(
                                agent = %agent_id,
                                streak = consecutive_idle_timeouts,
                                error = %e,
                                "prompt idle timeout — consecutive-timeout cap spent, failing terminally"
                            );
                            // Durable-before-observable (monorepo#2050): persist
                            // Error + stop_reason and stash it BEFORE this branch
                            // emits its own `agent:failed`, so a client reading
                            // `agent.get`/`agent.getSession` on that event sees the
                            // persisted Error. `handle_terminal_turn_failure` below
                            // reuses the stash (streak recorded / status written
                            // exactly once, monorepo#840).
                            let persist = persist_terminal_error_status(
                                &mgr,
                                &agent_id,
                                &workspace_id,
                                &e.to_string(),
                            )
                            .await;
                            mgr.services
                                .stash_pending_terminal_error(&agent_id, persist);
                            let mut data = json!({ "agentId": agent_id.0, "error": e.to_string() });
                            if let Some(tid) = options.turn_id.as_deref() {
                                data["turnId"] = json!(tid);
                            }
                            mgr.services
                                .publish_agent_event(
                                    &workspace_id,
                                    &agent_id,
                                    intent_core::events::AGENT_FAILED,
                                    data,
                                )
                                .await;
                            handle_terminal_turn_failure(
                                &mgr,
                                &agent_id,
                                &workspace_id,
                                &content,
                                &options,
                                user_persisted,
                                &e,
                            )
                            .await;
                            mgr.release_in_flight_slot(&agent_id).await;
                            break 'outer;
                        } else if suspend_interrupt_error(&e) {
                            // Sleep-induced interruption (Task C):
                            // `run_prompt_turn` already enrolled the turn as
                            // interrupted (partial persisted with
                            // `InterruptReason::SystemSuspend` + an
                            // `interrupted_agent` row) and emitted the
                            // interrupted terminal `agent:stream:end` — NOT
                            // `agent:failed`. Suppress the terminal-failure path
                            // (no Error status, no manual-retry surface): settle
                            // the session to idle and stop the worker, leaving
                            // the enrolled turn for the wake orchestrator (Task
                            // D) to resume. Placed before the pre-output redrive
                            // arm so a suspend-overlapping pre-output failure is
                            // resumed via `session/load` (preserving the partial
                            // turn) rather than silently redriven on a fresh
                            // child.
                            tracing::info!(
                                agent = %agent_id,
                                error = %e,
                                "turn interrupted by system suspend — enrolled for wake-resume, suppressing terminal failure"
                            );
                            // Tear down the local provider child (mirrors the
                            // silent-redrive branch below): the upstream API
                            // stream dropped, but the claude-code/auggie child
                            // and its IPC connection survive the disconnect. If
                            // the handle is left live, the wake/self-heal resume
                            // routes through `ensure_started`'s live-child reuse
                            // branch and returns the stale `acpSessionId`
                            // WITHOUT issuing `session/load` — skipping the very
                            // recovery the enrollment promises. Removing the
                            // handle here forces the resume to spawn a fresh
                            // child and reload the persisted session via
                            // `session/load` (or the recreate fallback).
                            mgr.kill_child_only(&agent_id).await;
                            mgr.end_turn(&agent_id).await;
                            break 'outer;
                        } else if !silent_redrive_used && pre_output_transport_failure(&e) {
                            // Silent redrive (monorepo#764): the transport closed
                            // before the turn streamed anything — the prompt
                            // provably produced no output, so redrive it once on
                            // a fresh child. `run_prompt_turn` suppressed the
                            // terminal `agent:failed` + `agent:stream:end` pair
                            // for this attempt, so nothing user-visible surfaced.
                            // Tear down the dead child and loop back through
                            // `retry_spawn` with the SAME content/options.
                            silent_redrive_used = true;
                            tracing::warn!(
                                agent = %agent_id,
                                error = %e,
                                "transport closed before output — redriving the prompt once on a fresh child"
                            );
                            mgr.kill_child_only(&agent_id).await;
                            continue 'outer;
                        } else {
                            // STAB-53: on a child-death failure, point at the
                            // captured stderr file so the crash is diagnosable.
                            match stderr_capture_hint(&mgr, &agent_id, &e) {
                                Some(log) => tracing::warn!(
                                    agent = %agent_id,
                                    error = %e,
                                    "agent turn failed terminally (agent stderr captured at {})",
                                    log.display()
                                ),
                                None => {
                                    tracing::warn!(agent = %agent_id, error = %e, "agent turn failed terminally")
                                }
                            }
                            handle_terminal_turn_failure(
                                &mgr,
                                &agent_id,
                                &workspace_id,
                                &content,
                                &options,
                                user_persisted,
                                &e,
                            )
                            .await;
                            // Release the in-flight slot without overwriting the
                            // Error status just persisted, so `agent.retry` (or a
                            // future message) can restart the worker.
                            mgr.release_in_flight_slot(&agent_id).await;
                            break 'outer;
                        }
                    }
                }
            }
            Err(e) => {
                match stderr_capture_hint(&mgr, &agent_id, &e) {
                    Some(log) => tracing::warn!(
                        agent = %agent_id,
                        error = %e,
                        "agent spawn failed after all retries (agent stderr captured at {})",
                        log.display()
                    ),
                    None => {
                        tracing::warn!(agent = %agent_id, error = %e, "agent spawn failed after all retries")
                    }
                }
                handle_terminal_spawn_failure(
                    &mgr,
                    &agent_id,
                    &workspace_id,
                    &content,
                    &options,
                    user_persisted,
                    &e,
                )
                .await;
                // Release the in-flight slot without overwriting the Error status
                // that handle_terminal_spawn_failure just persisted. This allows
                // a future message (or agent.retry) to restart the worker.
                mgr.release_in_flight_slot(&agent_id).await;
                break 'outer;
            }
        }
        // Archived-workspace gate (intent-hq/monorepo#2513): the archive
        // sweep keeps pending queues persisted and the enqueue paths
        // (`try_drain_queue` / `deliver_wake_message`) park messages while
        // the workspace is archived — but this end-of-turn drain used to
        // bypass that gate. A wake parked mid-turn (e.g. the hook-cancel
        // notice from an archive initiated by this agent's own hook) was
        // popped at turn end: the pre-release arm ran a stray turn in the
        // archived workspace, and the post-release raced re-check re-claimed
        // the slot via `try_begin`, whose auto-unarchive (#1216) flipped the
        // freshly archived workspace straight back to Active. Mirror the
        // `try_drain_queue` gate here: when the workspace is archived at
        // turn end, leave everything parked (unarchive kicks the drain for
        // every parked queue) and exit the worker. Chief is virtual and
        // never archived, so skip the row read; fail open on a lookup error
        // — the gate only parks affirmatively-archived workspaces. The
        // redelivery call mirrors the empty-queue exit arm below (no-op
        // unless the queue is empty and an interim-skipped idle is marked).
        if !workspace_id.is_chief() {
            match mgr.services.store.get_workspace(&workspace_id).await {
                Ok(ws) if ws.archived => {
                    tracing::debug!(
                        agent = %agent_id,
                        workspace = %workspace_id.as_str(),
                        "worker end-of-turn drain parked: workspace is archived"
                    );
                    // Deregister BEFORE releasing the slot (the wake-listener
                    // teardown order): while the slot is held no concurrent
                    // drain can claim it and register a replacement worker,
                    // so this provably removes only this worker's own entry.
                    // `break 'outer` would instead reach the shared post-loop
                    // `clear_worker` AFTER the release — an unarchive-kicked
                    // drain can spawn (and register) a replacement in that
                    // gap, and the late clear would deregister the
                    // replacement, leaving it running but unreachable by
                    // interrupt/stop (PR #1244 review). Returning also skips
                    // the turn-end attention raise: the queue parked, the
                    // agent did not finish its work.
                    mgr.clear_worker(&agent_id);
                    mgr.end_turn(&agent_id).await;
                    mgr.services
                        .redeliver_completion_after_queue_mutation(&agent_id)
                        .await;
                    // Close the unarchive strand window (PR #1244 review): an
                    // unarchive that persisted between this gate's row read
                    // and the `end_turn` above had its drain kick bounce off
                    // this worker's still-held slot — leaving ready entries
                    // parked in a now-Active workspace with nothing to kick
                    // them. Re-kick now that the slot is released: the kick
                    // self-gates (still archived → parks again; unarchived in
                    // the window → drains), and an unarchive persisting after
                    // ITS row read finds the slot free, so its own kick
                    // proceeds — the window is closed from both sides.
                    mgr.clone()
                        .try_drain_queue(agent_id.clone(), workspace_id.clone())
                        .await;
                    return;
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(
                        agent = %agent_id,
                        workspace = %workspace_id.as_str(),
                        error = %e,
                        "worker end-of-turn drain: workspace archived-state lookup failed; proceeding"
                    );
                }
            }
        }
        // Question hold (PROTOCOL §5.5): questions may still be pending —
        // asked by the turn that just ended, or by an earlier turn the hold
        // has kept armed since — so draining the next AUTOMATIC queued
        // message would bury them. A parked USER-origin entry (a user answer
        // that lost the busy race against this very turn) is exempt: it may
        // itself be the hold's release, so it drains regardless. Otherwise
        // skip the pre-release drain (no `break 'outer` here!) and fall through to the
        // post-`end_turn` raced re-check below, which repeats this same
        // hold-aware `dequeue_user_origin_message` check AFTER the slot is
        // actually released — closing the window where a user answer enqueued
        // between `has_user_origin_ready` returning false and the slot's
        // release would otherwise strand behind a gone worker with nothing to
        // kick `try_drain_queue`.
        let hold_active = mgr.services.question_hold_active(&agent_id).await;
        // Batch flush (`agents.flushQueuedMessages`): same contract as the
        // `try_drain_queue` flush arm — ≥2 ready entries drain into one
        // combined provider turn. Under an active hold the flush fires only
        // when a user-origin entry is ready, and then carries EVERY ready
        // entry FIFO (parked automatic wakes included, monorepo#1791);
        // otherwise the single-entry arm below runs unchanged.
        {
            let mode = mgr.services.flush_queued_messages_mode();
            if let Some(batch) = mgr
                .services
                .dequeue_flush_batch(&agent_id, mode, hold_active, 2)
            {
                match prepare_flush_turn(&mgr, &agent_id, &workspace_id, batch).await {
                    FlushPrep::Turn {
                        content: c,
                        options: o,
                    } => {
                        content = c;
                        options = *o;
                        user_persisted = true;
                        // New messages → fresh silent-redrive budget
                        // (monorepo#764).
                        silent_redrive_used = false;
                        continue 'outer;
                    }
                    FlushPrep::Parked => {
                        mgr.release_in_flight_slot(&agent_id).await;
                        break 'outer;
                    }
                }
            }
        }
        let drained = if hold_active {
            if mgr.services.has_user_origin_ready(&agent_id) {
                mgr.services.dequeue_user_origin_message(&agent_id)
            } else {
                tracing::debug!(
                    agent = %agent_id,
                    "worker drain suspended: question hold active (awaiting answer or dismissQuestions)"
                );
                None
            }
        } else {
            mgr.services.dequeue_message(&agent_id)
        };
        if let Some(mut next) = drained {
            mgr.services
                .publish_queue_updated_for(
                    &agent_id,
                    &workspace_id,
                    mgr.services.queue_snapshot(&agent_id),
                )
                .await;
            // Stale-redrive check (#576) BEFORE the transcript append so the
            // annotated content reaches both the persisted user row and the
            // provider prompt. Runs before the next iteration's report clear,
            // so `completion_report_timestamp` is still visible here.
            let stale = mgr.annotate_stale_redrive(&agent_id, &mut next).await;
            // Dequeue-wait note: same placement contract as the stale check.
            annotate_dequeue_wait(&mut next);
            // Delivery-time unblocked hints (monorepo#2044): resolved at
            // render time, same placement as the single-entry drain arm.
            annotate_unblocked_hints(&mgr.services, &agent_id, std::slice::from_mut(&mut next))
                .await;
            // Drain-start signal (monorepo#1022): covers redrives that skip
            // the user-row append below. Emitted AFTER the stale-redrive
            // annotation so the payload's `content` matches what is
            // persisted/sent to the provider.
            mgr.services
                .publish_queue_processing(&agent_id, &workspace_id, &next)
                .await;
            let next_image_blocks = next.image_blocks.clone();
            let next_file_blocks = next.file_blocks.clone();
            // A terminal-failure requeue whose user row already reached the
            // transcript before its failed turn began must not duplicate the
            // row on retry; otherwise persist now and remember the outcome.
            user_persisted = if next.persisted {
                true
            } else {
                persist_user(
                    &mgr,
                    &agent_id,
                    &workspace_id,
                    &next.content,
                    next_image_blocks.as_ref(),
                    next_file_blocks.as_ref(),
                    next.message_metadata.as_ref(),
                    Some(&next.turn_id),
                )
                .await
            };
            content = next.content;
            options = TurnOptions {
                image_blocks: next_image_blocks,
                file_blocks: next_file_blocks,
                message_metadata: next.message_metadata.clone(),
                suppress_report_clear: stale,
                queued_at: Some(next.queued_at.clone()),
                prepend_content: next.prepend_content.clone(),
                prepend_image_blocks: next.prepend_image_blocks.clone(),
                prepend_file_blocks: next.prepend_file_blocks.clone(),
                turn_id: Some(next.turn_id.clone()),
                interrupt_priority: next.interrupt_priority,
                // Restore the entry's captured origin so a drained user
                // message still clears a pending attention request.
                origin: origin_from_user_flag(next.user_origin),
                ..TurnOptions::default()
            };
            // New message → fresh silent-redrive budget (monorepo#764).
            silent_redrive_used = false;
            // Fail closed (#547): a persist failure that survived the bounded
            // retry parks the agent in Error instead of running the turn.
            if !user_persisted {
                handle_drain_persist_failure(&mgr, &agent_id, &workspace_id, &content, &options)
                    .await;
                mgr.release_in_flight_slot(&agent_id).await;
                break 'outer;
            }
            continue;
        }
        // Queue drained: release the slot, then re-check for a message that
        // raced in just before / after the release. The re-check is wrapped in
        // the outer `'outer` loop (not its own inner loop) so the agent never
        // goes idle while ready-to-send messages remain — each re-claim of the
        // slot continues `'outer` and re-enters the drain at the top. Same
        // question-hold contract as the pre-release arm: while the hold is
        // active only a user-origin entry may drain.
        mgr.end_turn(&agent_id).await;
        let raced_mode = mgr.services.flush_queued_messages_mode();
        // Under an active hold with the `all` flush mode the raced re-check
        // pops the WHOLE ready batch in ONE locked call (it fires only when
        // a user-origin entry is ready). The oldest ready user entry may sit
        // BEHIND older parked automatic entries, so a user-origin pop
        // followed by a later batch fold + prepend would persist and prompt
        // the newer user message ahead of the parked wakes it was dequeued
        // from behind — a FIFO violation in exactly the race window this
        // arm handles (monorepo#1791 review). The atomic batch keeps stored
        // order end-to-end, and the slot-race failure below hands it back
        // unchanged (`requeue_front_batch`).
        let mut raced: Vec<QueuedMessage> = if mgr.services.question_hold_active(&agent_id).await {
            match raced_mode {
                intent_core::FlushQueuedMessagesMode::All => mgr
                    .services
                    .dequeue_ready_batch(&agent_id, true, 1)
                    .unwrap_or_default(),
                _ => mgr
                    .services
                    .dequeue_user_origin_message(&agent_id)
                    .into_iter()
                    .collect(),
            }
        } else {
            mgr.services
                .dequeue_message(&agent_id)
                .into_iter()
                .collect()
        };
        if raced.is_empty() {
            // monorepo#1297: heal a busy-misclassified terminal idle. The
            // turn's `agent:idle` is published while this worker still holds
            // the busy slot (`end_turn` above runs after `run_prompt_turn`
            // returns), so an asynchronous delivery that raced ahead of the
            // release classified it interim on the busy probe and recorded
            // the interim-skip marker — with no further completion event
            // coming. Re-run the mutation-path redelivery now that the slot
            // is released and the queue is empty; its guards (marker set,
            // queue empty, not busy) make it a no-op in every other
            // interleaving, and the delivery pass's own post-skip re-check
            // covers the complementary ordering (marker recorded after this
            // hook ran ⇒ that re-check observes the released slot).
            mgr.services
                .redeliver_completion_after_queue_mutation(&agent_id)
                .await;
            break 'outer;
        };
        if mgr.try_begin_outcome(&agent_id, &workspace_id, false).await == TryBeginOutcome::Started
        {
            // Archived re-check on the raced pop (intent-hq/monorepo#2513):
            // the popped entry can be a wake parked by the archived gates
            // AFTER the gate at the top of this drain ran — e.g. the
            // hook-cancel notice from `workspace.archive`'s post-persist
            // tail, enqueued exactly in this arm's race window. Running that
            // wake would flip the workspace the wake was parked FOR straight
            // back to Active — publishing the archive delta and then
            // un-archiving, so a client that saw the delta reads `archived:
            // false`. The check runs AFTER the slot re-claim, under the held
            // slot (a pre-claim read is check-then-act: an archive
            // persisting between the read and the claim would be flipped
            // back by the claim's own #1216 auto-unarchive — PR #1244
            // review), which is why the claim above SUPPRESSES the
            // auto-unarchive: this read decides instead. Archived → hand the
            // batch back, deregister, release the slot, and re-kick the
            // self-gating drain (same closure as the pre-release arm: an
            // unarchive whose kick bounced off this still-held slot — or
            // whose `has_ready_to_send` probe ran while the batch sat in the
            // local `raced`, invisible to it — is covered by our own kick
            // against the now-requeued entries). Not archived → fall through
            // to the turn with nothing to unarchive; fail open on a lookup
            // error, matching the auto-unarchive's own error path (turn
            // proceeds, workspace stays as-is).
            if !workspace_id.is_chief() {
                match mgr.services.store.get_workspace(&workspace_id).await {
                    Ok(ws) if ws.archived => {
                        tracing::debug!(
                            agent = %agent_id,
                            workspace = %workspace_id.as_str(),
                            "worker raced re-check parked: workspace is archived"
                        );
                        mgr.services.requeue_front_batch(&agent_id, raced);
                        // Deregister BEFORE releasing the slot, then return —
                        // same teardown order and rationale as the
                        // pre-release archived arm above.
                        mgr.clear_worker(&agent_id);
                        mgr.end_turn(&agent_id).await;
                        mgr.clone()
                            .try_drain_queue(agent_id.clone(), workspace_id.clone())
                            .await;
                        return;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(
                            agent = %agent_id,
                            workspace = %workspace_id.as_str(),
                            error = %e,
                            "worker raced re-check: workspace archived-state lookup failed; proceeding"
                        );
                    }
                }
            }
            // A multi-entry raced pop (hold + `all` above) is already the
            // complete ready batch in stored order — run it as one combined
            // turn directly; folding or reordering here would break the
            // FIFO guarantee the atomic pop just preserved.
            if raced.len() > 1 {
                match prepare_flush_turn(&mgr, &agent_id, &workspace_id, raced).await {
                    FlushPrep::Turn {
                        content: c,
                        options: o,
                    } => {
                        content = c;
                        options = *o;
                        user_persisted = true;
                        // New messages → fresh silent-redrive budget
                        // (monorepo#764).
                        silent_redrive_used = false;
                        continue 'outer;
                    }
                    FlushPrep::Parked => {
                        mgr.release_in_flight_slot(&agent_id).await;
                        break 'outer;
                    }
                }
            }
            let mut next = raced.pop().expect("raced batch non-empty");
            // Batch flush (`agents.flushQueuedMessages`): the single `next`
            // was popped before the slot re-claim, so fold any FURTHER
            // eligible entries in behind it and run them as one combined
            // turn. Mode `all`: any further ready entry (min 1 more ⇒ ≥2
            // total; only the multi-entry hold case was handled atomically
            // above, so a hold armed SINCE the pop still requires a
            // user-origin entry — see the arm comment below). Mode
            // `systemOnly`: only when `next` is ITSELF system-origin (and no
            // hold is active) — a user-origin `next` never batches under
            // `systemOnly`, so it falls through to the single-entry path
            // below unchanged. With no extra entry (or the `off` mode) the
            // single-entry path below also runs unchanged.
            let mode = mgr.services.flush_queued_messages_mode();
            let hold = mgr.services.question_hold_active(&agent_id).await;
            let extra_batch = match mode {
                intent_core::FlushQueuedMessagesMode::All => {
                    // A single-entry raced pop under hold + `all` means
                    // `next` was the ONLY ready entry at pop time; a hold
                    // armed since (or an entry that raced in) still requires
                    // a ready user-origin entry unless `next` itself is one
                    // (monorepo#1791).
                    mgr.services
                        .dequeue_ready_batch(&agent_id, hold && !next.user_origin, 1)
                }
                intent_core::FlushQueuedMessagesMode::SystemOnly => {
                    if hold || next.user_origin {
                        None
                    } else {
                        mgr.services.dequeue_system_only_batch(&agent_id, 1)
                    }
                }
                intent_core::FlushQueuedMessagesMode::Off => None,
            };
            if let Some(mut batch) = extra_batch {
                batch.insert(0, next);
                match prepare_flush_turn(&mgr, &agent_id, &workspace_id, batch).await {
                    FlushPrep::Turn {
                        content: c,
                        options: o,
                    } => {
                        content = c;
                        options = *o;
                        user_persisted = true;
                        // New messages → fresh silent-redrive budget
                        // (monorepo#764).
                        silent_redrive_used = false;
                        continue 'outer;
                    }
                    FlushPrep::Parked => {
                        mgr.release_in_flight_slot(&agent_id).await;
                        break 'outer;
                    }
                }
            }
            mgr.services
                .publish_queue_updated_for(
                    &agent_id,
                    &workspace_id,
                    mgr.services.queue_snapshot(&agent_id),
                )
                .await;
            // Stale-redrive check (#576): same contract as the pre-release
            // drain arm. Runs only after the slot is re-claimed so a message
            // handed back via `requeue_front` below is never annotated here.
            let stale = mgr.annotate_stale_redrive(&agent_id, &mut next).await;
            // Dequeue-wait note: same placement contract as the stale check.
            annotate_dequeue_wait(&mut next);
            // Delivery-time unblocked hints (monorepo#2044): same contract
            // as the pre-release drain arm.
            annotate_unblocked_hints(&mgr.services, &agent_id, std::slice::from_mut(&mut next))
                .await;
            // Drain-start signal (monorepo#1022): same contract as the
            // pre-release drain arm — emitted AFTER the stale-redrive
            // annotation so the payload's `content` matches the turn.
            mgr.services
                .publish_queue_processing(&agent_id, &workspace_id, &next)
                .await;
            let next_image_blocks = next.image_blocks.clone();
            let next_file_blocks = next.file_blocks.clone();
            user_persisted = if next.persisted {
                true
            } else {
                persist_user(
                    &mgr,
                    &agent_id,
                    &workspace_id,
                    &next.content,
                    next_image_blocks.as_ref(),
                    next_file_blocks.as_ref(),
                    next.message_metadata.as_ref(),
                    Some(&next.turn_id),
                )
                .await
            };
            content = next.content;
            options = TurnOptions {
                image_blocks: next_image_blocks,
                file_blocks: next_file_blocks,
                message_metadata: next.message_metadata.clone(),
                suppress_report_clear: stale,
                queued_at: Some(next.queued_at.clone()),
                prepend_content: next.prepend_content.clone(),
                prepend_image_blocks: next.prepend_image_blocks.clone(),
                prepend_file_blocks: next.prepend_file_blocks.clone(),
                turn_id: Some(next.turn_id.clone()),
                interrupt_priority: next.interrupt_priority,
                // Same origin restore as the pre-release drain arm.
                origin: origin_from_user_flag(next.user_origin),
                ..TurnOptions::default()
            };
            // New message → fresh silent-redrive budget (monorepo#764).
            silent_redrive_used = false;
            // Fail closed (#547): same contract as the pre-release drain arm.
            if !user_persisted {
                handle_drain_persist_failure(&mgr, &agent_id, &workspace_id, &content, &options)
                    .await;
                mgr.release_in_flight_slot(&agent_id).await;
                break 'outer;
            }
            continue 'outer;
        }
        // A concurrent send won the slot; hand the message(s) back to it in
        // original order and exit — that worker's own drain loop will pick
        // them up.
        mgr.services.requeue_front_batch(&agent_id, raced);
        break 'outer;
    }
    mgr.clear_worker(&agent_id);
    // The agent finished its work (queue drained, slot released): raise the
    // server-owned `attention` blue dot so every client surfaces it (§9.9) —
    // but only for TOP-LEVEL FOREGROUND agents (monorepo#1781): a delegated
    // child or background agent's completion is surfaced to its
    // parent/coordinator, not the user.
    if should_raise_turn_end_unread(&mgr.services, &agent_id).await {
        if let Err(e) = mgr
            .services
            .raise_attention(&workspace_id, WorkspaceAttention::Unread)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "failed to raise attention");
        }
    }
}

/// Turn-end unread gate (monorepo#1781): the drain-end blue dot is reserved
/// for TOP-LEVEL FOREGROUND agents — a delegated child (`parent_agent_id`
/// set) or background agent (`is_background`) is a sub-agent whose
/// completion is its parent/coordinator's attention surface, not the
/// user's. Same sub-agent definition as the attention-clear gate above and
/// rules.rs. `NotFound` means the agent was deleted while its drain
/// finished — nothing to surface, skip. Archived workspaces additionally
/// stay quiet: a turn finishing in a workspace whose status is `Archived`
/// skips the raise (the user parked the workspace; unarchiving restores
/// normal behavior — no persisted suppression state). FAIL OPEN on any
/// other store error, on either the session or the workspace read (raise
/// anyway): a missed blue dot for a real top-level turn is worse than a
/// spurious one on a rare store fault.
pub(crate) async fn should_raise_turn_end_unread(services: &Services, agent_id: &AgentId) -> bool {
    let session = match services.store.get_agent_session_summary(agent_id).await {
        Ok(s) => s,
        Err(Error::NotFound(_)) => return false,
        Err(e) => {
            tracing::warn!(
                agent = %agent_id,
                error = %e,
                "turn-end unread gate: session lookup failed; raising anyway"
            );
            return true;
        }
    };
    if session.parent_agent_id.is_some() || session.is_background {
        return false;
    }
    match services.store.get_workspace(&session.workspace_id).await {
        Ok(ws) => ws.status != WorkspaceStatus::Archived,
        Err(e) => {
            tracing::warn!(
                agent = %agent_id,
                error = %e,
                "turn-end unread gate: workspace lookup failed; raising anyway"
            );
            true
        }
    }
}

/// Outcome of [`prepare_flush_turn`]: either the combined turn is ready to
/// run, or a persist failure parked the agent in `Error` (the caller must
/// release the in-flight slot without starting a turn).
enum FlushPrep {
    Turn {
        content: String,
        options: Box<TurnOptions>,
    },
    Parked,
}

/// Prepare a batch-flushed turn (`agents.flushQueuedMessages`, default on):
/// the caller has already claimed the in-flight slot and batch-dequeued ≥2
/// ready-to-send entries in drain order. This mirrors the single-entry drain
/// sequence once per entry — stale-redrive (#576) + dequeue-wait annotation,
/// then the transcript row append (`persist_user`; entries already persisted
/// by a terminal-failure requeue are not re-appended) — while emitting ONE
/// `agent:queue:updated` (the fully-shrunk queue) and ONE
/// `agent:queue:processing` (the head entry, whose `turn_id` is the combined
/// turn's id). Each row persist emits its normal `agent:message`, so clients
/// render N stacked user rows — and every row echo carries the COMBINED
/// turn's `turn_id` (the head entry's), not the entry's own, so all N echoes
/// correlate with the single `agent:queue:processing`/`agent:stream:*`
/// lifecycle (monorepo#1022 turn-correlation contract). Queue entries keep
/// their own `turn_id`s (ids/messageMetadata/queueInfo are untouched).
///
/// Returns [`FlushPrep::Turn`] with the wire-only combined prompt
/// ([`flush_combined_prompt`]) and merged [`TurnOptions`]: attachments and
/// `prepend_*` payloads from all entries in message order; head entry's
/// `turn_id` / `queued_at` / `interrupt_priority` / `messageMetadata`;
/// `origin` is User when ANY entry is user-origin (a user message is being
/// delivered); the turn-begin report clear is suppressed only when EVERY
/// entry is a stale redrive (any fresh entry means the clear should happen).
///
/// Fail closed (#547, never-lost): when an entry's row append exhausts the
/// bounded retry, the agent parks in `Error` via
/// [`handle_drain_persist_failure`] (which requeues the FAILED entry at the
/// queue front, `persisted: false`) and the other flushed entries are
/// requeued around it in original order — entries whose rows already
/// persisted carry `persisted: true` so the retry drain never
/// double-appends. Returns [`FlushPrep::Parked`].
async fn prepare_flush_turn(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    mut entries: Vec<QueuedMessage>,
) -> FlushPrep {
    mgr.services
        .publish_queue_updated_for(
            agent_id,
            workspace_id,
            mgr.services.queue_snapshot(agent_id),
        )
        .await;
    // Per-entry annotations, same order as the single-entry drain arms: the
    // stale check before the wait note, both before the row persist so the
    // persisted row and the provider prompt carry the same content.
    let mut stale_flags = Vec::with_capacity(entries.len());
    for entry in entries.iter_mut() {
        let stale = mgr.annotate_stale_redrive(agent_id, entry).await;
        annotate_dequeue_wait(entry);
        stale_flags.push(stale);
    }
    // Delivery-time unblocked hints (monorepo#2044): all completion wakes in
    // this batch coalesce into ONE fresh delta, appended to the last
    // trigger-carrying entry. Runs before the row persists (same placement
    // contract as the annotations above).
    annotate_unblocked_hints(&mgr.services, agent_id, &mut entries).await;
    // Drain-start signal (monorepo#1022): one event for the combined turn,
    // keyed on the head entry (its `turn_id` IS the turn's id below).
    mgr.services
        .publish_queue_processing(agent_id, workspace_id, &entries[0])
        .await;
    // All rows persist under the combined turn's id — the provider turn runs
    // once, under the head entry's `turn_id`, so a per-entry id on row #2+
    // would never match any processing/stream event.
    let combined_turn_id = entries[0].turn_id.clone();
    for i in 0..entries.len() {
        if entries[i].persisted {
            continue;
        }
        if persist_user(
            mgr,
            agent_id,
            workspace_id,
            &entries[i].content,
            entries[i].image_blocks.as_ref(),
            entries[i].file_blocks.as_ref(),
            entries[i].message_metadata.as_ref(),
            Some(&combined_turn_id),
        )
        .await
        {
            // The row is durable: a later mid-flush failure requeues this
            // entry with `persisted: true` so the retry drain skips the
            // duplicate append (STAB-51).
            entries[i].persisted = true;
            continue;
        }
        // Fail closed: restore the queue in original order — tail first,
        // then the failed entry (the handler's own front requeue), then the
        // already-persisted head entries ahead of it.
        let stale = stale_flags[i];
        let failed = entries.remove(i);
        let tail = entries.split_off(i);
        let head = entries;
        let options = turn_options_for_entry(&failed, stale);
        mgr.services.requeue_front_batch(agent_id, tail);
        handle_drain_persist_failure(mgr, agent_id, workspace_id, &failed.content, &options).await;
        mgr.services.requeue_front_batch(agent_id, head);
        // The handler's queue publish preceded the head requeue: re-publish
        // so clients see the fully-restored queue.
        mgr.services
            .publish_queue_updated_for(
                agent_id,
                workspace_id,
                mgr.services.queue_snapshot(agent_id),
            )
            .await;
        return FlushPrep::Parked;
    }
    let content = flush_combined_prompt(&entries);
    let mut image_blocks = None;
    let mut file_blocks = None;
    let mut prepend_content: Option<String> = None;
    let mut prepend_image_blocks = None;
    let mut prepend_file_blocks = None;
    for entry in &entries {
        image_blocks = merge_block_arrays(image_blocks, entry.image_blocks.clone());
        file_blocks = merge_block_arrays(file_blocks, entry.file_blocks.clone());
        if let Some(p) = entry.prepend_content.as_deref().filter(|p| !p.is_empty()) {
            prepend_content = Some(match prepend_content.take() {
                Some(existing) => format!("{existing}\n\n{p}"),
                None => p.to_string(),
            });
        }
        prepend_image_blocks =
            merge_block_arrays(prepend_image_blocks, entry.prepend_image_blocks.clone());
        prepend_file_blocks =
            merge_block_arrays(prepend_file_blocks, entry.prepend_file_blocks.clone());
    }
    let options = TurnOptions {
        image_blocks,
        file_blocks,
        message_metadata: entries[0].message_metadata.clone(),
        suppress_report_clear: stale_flags.iter().all(|&s| s),
        queued_at: Some(entries[0].queued_at.clone()),
        prepend_content,
        prepend_image_blocks,
        prepend_file_blocks,
        turn_id: Some(entries[0].turn_id.clone()),
        interrupt_priority: entries[0].interrupt_priority,
        origin: origin_from_user_flag(entries.iter().any(|m| m.user_origin)),
        ..TurnOptions::default()
    };
    FlushPrep::Turn {
        content,
        options: Box::new(options),
    }
}

/// Persist a queued user message into the append-only transcript before its turn
/// and publish the `agent:message` event so chat subscribers and the transcript
/// reflect the dequeued message (STAB-4 fix). FE-supplied attachments captured at
/// enqueue time ride along so the persisted row carries them (STAB-133).
/// `message_metadata` is the queue entry's captured `messageMetadata` (e.g. a
/// parent wake's `event_notification` payload). It is written in BOTH placements
/// the two direct-delivery shapes use — folded onto the text block as
/// `messageMetadata` (parity with `deliver_wake_message`'s in-block tag) AND on
/// the row-level `metadata` column (parity with the direct `agent.sendMessage`
/// persist) — so transcript consumers find the tag regardless of which field
/// they read. The client-identity `userAppMessageId` key is excluded from the
/// in-block copy (it stays row-level only): the block embed exists for
/// attribution tags that history replay should surface, and a queued send's
/// content block should not diverge from its direct-send counterpart just
/// because a dedup id rode along. Best-effort; a store or publish error is
/// logged and the turn still proceeds.
///
/// Returns `true` when the user row was durably appended to the transcript,
/// `false` when the store append failed for every bounded retry attempt
/// (STAB-51 / #547). A transient store blip (busy database, lock contention)
/// self-heals inside the bounded retry (delays from
/// [`persist_retry_backoff_ms`]); on exhaustion the drain call sites fail
/// closed — they do NOT start the turn, park the agent in `Error`, and
/// requeue with `persisted: false` so `agent.retry` re-attempts the append.
#[allow(clippy::too_many_arguments)]
async fn persist_user(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    image_blocks: Option<&Value>,
    file_blocks: Option<&Value>,
    message_metadata: Option<&Value>,
    turn_id: Option<&str>,
) -> bool {
    let created_at = now_iso();
    let mut blocks = user_message_blocks(content, image_blocks, file_blocks);
    let block_md = message_metadata.and_then(|md| match md {
        Value::Object(m) => {
            let mut m = m.clone();
            m.remove(intent_core::USER_APP_MESSAGE_ID_KEY);
            (!m.is_empty()).then_some(Value::Object(m))
        }
        other => Some(other.clone()),
    });
    if let Some(md) = block_md {
        if let Some(text_block) = blocks.get_mut(0).and_then(Value::as_object_mut) {
            text_block.insert("messageMetadata".into(), md);
        }
    }
    // Bounded retry (#547): initial attempt + one retry per backoff delay.
    let backoff = persist_retry_backoff_ms();
    let mut attempt = 0usize;
    let message = loop {
        match mgr
            .services
            .store
            .append_agent_message_with_metadata(
                agent_id,
                "user",
                &blocks,
                message_metadata,
                &created_at,
            )
            .await
        {
            Ok(message) => {
                mgr.services.invalidate_agent_list_cache(workspace_id);
                break message;
            }
            Err(e) => {
                let Some(&delay_ms) = backoff.get(attempt) else {
                    tracing::warn!(
                        agent = %agent_id,
                        error = %e,
                        attempts = attempt + 1,
                        "failed to persist queued user message (all retries exhausted)"
                    );
                    return false;
                };
                attempt += 1;
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    attempt,
                    "failed to persist queued user message; retrying in {delay_ms}ms"
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }
        }
    };
    // Refresh agent_session.updated_at so the FE agent-card timestamp
    // reflects message activity, not just status transitions (STAB-19).
    if let Err(e) = mgr
        .services
        .store
        .refresh_agent_session_timestamp(workspace_id, agent_id, &created_at)
        .await
    {
        tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
    } else {
        // Schedule debounced lastActivity event (§10.1).
        mgr.services
            .schedule_last_activity_event(workspace_id.clone());
    }
    mgr.services
        .publish_agent_mutation_event(
            workspace_id,
            agent_id,
            intent_core::events::AGENT_MESSAGE,
            crate::agent_ops::agent_message_event_payload(agent_id, &message, turn_id),
        )
        .await;
    // Answer intake (PROTOCOL §5.5, question hold): same contract as the
    // direct-send persist — a `question_answers` tag naming the marked
    // assistant message clears the pending-questions marker; anything else is
    // a no-op, and only the clear can move the workspace's needs_attention
    // displayStatus (§6.5 step 0).
    if mgr
        .services
        .resolve_pending_questions_for_answer(workspace_id, agent_id, message_metadata)
        .await
    {
        mgr.services
            .maybe_emit_display_status_changed(workspace_id)
            .await;
    }
    true
}

/// Max number of spawn attempts (includes the initial attempt).
const MAX_SPAWN_ATTEMPTS: u32 = 3;
/// Default backoff delays between retry attempts (in milliseconds). Long
/// enough that retries escape the host load spike that killed the first
/// attempt (monorepo#616); jitter is applied on top (see [`jitter_delay_ms`]).
const DEFAULT_RETRY_BACKOFF_MS: &[u64] = &[5000, 15000];
/// Default backoff delays between pre-turn persist retry attempts (#547).
/// Short: the append is a local SQLite write, so a transient failure (busy
/// database, lock contention) clears quickly or not at all.
const DEFAULT_PERSIST_RETRY_BACKOFF_MS: &[u64] = &[250, 1000];

/// Parse comma-separated millisecond delays; `None` when empty or malformed.
fn parse_backoff_list(val: &str) -> Option<Vec<u64>> {
    let mut delays = Vec::new();
    for part in val.split(',') {
        delays.push(part.trim().parse::<u64>().ok()?);
    }
    (!delays.is_empty()).then_some(delays)
}

/// Parse comma-separated millisecond delays from `var`, falling back to
/// `default` when unset, empty, or malformed. Primarily for tests/CI.
fn env_backoff_ms(var: &str, default: &[u64]) -> Vec<u64> {
    std::env::var(var)
        .ok()
        .and_then(|val| parse_backoff_list(&val))
        .unwrap_or_else(|| default.to_vec())
}

/// Get spawn retry backoff delays plus whether jitter applies, overridable
/// via INTENTD_SPAWN_RETRY_BACKOFF_MS (comma-separated milliseconds, e.g.
/// "100,200"). Env-overridden delays are applied verbatim — no jitter — so
/// tests stay deterministic; the defaults are jittered (monorepo#616).
fn retry_backoff_ms() -> (Vec<u64>, bool) {
    spawn_backoff_from(
        std::env::var("INTENTD_SPAWN_RETRY_BACKOFF_MS")
            .ok()
            .as_deref(),
    )
}

/// Pure core of [`retry_backoff_ms`]: `(delays, apply_jitter)` from an
/// optional env-override value. Split out so unit tests avoid mutating
/// process-global env vars.
fn spawn_backoff_from(env_val: Option<&str>) -> (Vec<u64>, bool) {
    match env_val.and_then(parse_backoff_list) {
        Some(delays) => (delays, false),
        None => (DEFAULT_RETRY_BACKOFF_MS.to_vec(), true),
    }
}

/// Apply uniform jitter in [0.5x, 1.5x) to a backoff delay so concurrent
/// spawn retries desynchronize instead of landing inside the same host load
/// spike (monorepo#616). Entropy comes from a v4 UUID's random low bits
/// rather than pulling in a `rand` dependency.
fn jitter_delay_ms(delay_ms: u64) -> u64 {
    const MANTISSA_BITS: u32 = 53;
    let r = (Uuid::new_v4().as_u128() as u64) & ((1u64 << MANTISSA_BITS) - 1);
    let factor = 0.5 + (r as f64) / ((1u64 << MANTISSA_BITS) as f64);
    (delay_ms as f64 * factor) as u64
}

#[cfg(test)]
mod spawn_backoff_tests {
    //! Spawn retry backoff schedule + jitter (monorepo#616): lengthened
    //! defaults are jittered so concurrent retries desynchronize, while an
    //! explicit `INTENTD_SPAWN_RETRY_BACKOFF_MS` override stays verbatim.

    use super::*;

    #[test]
    fn defaults_are_lengthened_and_jittered() {
        let (delays, jitter) = spawn_backoff_from(None);
        assert_eq!(delays, vec![5000, 15000]);
        assert!(jitter, "defaults must be jittered");
    }

    #[test]
    fn env_override_is_verbatim_no_jitter() {
        let (delays, jitter) = spawn_backoff_from(Some("100,200"));
        assert_eq!(delays, vec![100, 200]);
        assert!(!jitter, "env-overridden delays must be applied verbatim");
    }

    #[test]
    fn malformed_or_empty_env_falls_back_to_jittered_defaults() {
        for bad in ["abc", "100,abc", ""] {
            let (delays, jitter) = spawn_backoff_from(Some(bad));
            assert_eq!(delays, DEFAULT_RETRY_BACKOFF_MS.to_vec(), "input: {bad:?}");
            assert!(
                jitter,
                "fallback defaults must be jittered (input: {bad:?})"
            );
        }
    }

    #[test]
    fn jitter_stays_within_half_to_one_and_a_half_x() {
        let delay = 10_000u64;
        let mut distinct = std::collections::HashSet::new();
        for _ in 0..1000 {
            let jittered = jitter_delay_ms(delay);
            assert!(
                (5_000..=15_000).contains(&jittered),
                "jittered delay {jittered} outside [0.5x, 1.5x] of {delay}"
            );
            distinct.insert(jittered);
        }
        assert!(
            distinct.len() > 1,
            "jitter must vary across draws (concurrent retries desynchronize)"
        );
    }
}

/// Get persist retry backoff delays (#547), overridable via
/// INTENTD_PERSIST_RETRY_BACKOFF_MS with the same format.
fn persist_retry_backoff_ms() -> Vec<u64> {
    env_backoff_ms(
        "INTENTD_PERSIST_RETRY_BACKOFF_MS",
        DEFAULT_PERSIST_RETRY_BACKOFF_MS,
    )
}

/// Classify whether an error from `ensure_started` is retryable. Retryable
/// errors include session/new or session/load timeouts and handshake failures
/// (e.g., "agent stdout closed" when the child dies immediately). Non-retryable
/// errors include InvalidParams, NotFound, Conflict, provider resolution
/// failures, mock provider missing env, and unknown Internal errors (fail-fast
/// by default to avoid retry loops on non-transient errors).
fn is_retryable_spawn_error(err: &Error) -> bool {
    // Non-retryable: InvalidParams, NotFound, Conflict are client/state issues,
    // not transient spawn failures that benefit from retry.
    match err {
        Error::InvalidParams(_) | Error::NotFound(_) | Error::Conflict { .. } => {
            return false;
        }
        _ => {}
    }

    let msg = err.to_string();
    // Retryable: session setup timeout, handshake failures, transport errors
    if msg.contains("session/new failed")
        || msg.contains("session/load failed")
        || msg.contains("handshake failed")
        || msg.contains("agent stdout closed")
        || msg.contains("timed out")
    {
        return true;
    }
    // Non-retryable: provider resolution failures (missing env, etc.)
    if msg.contains("provider") && msg.contains("missing") {
        return false;
    }
    // Default to non-retryable for unexpected Internal errors (conservative:
    // only retry explicitly-known transient failures to avoid masking bugs).
    false
}

/// Retry `ensure_started` up to `MAX_SPAWN_ATTEMPTS` times with jittered
/// backoff. On each retry (after the first failure), tear down the failed
/// child, publish an `agent:stream:status` retry hint, and spawn a fresh
/// process. Returns the `acpSessionId` on success, or the final error after
/// exhausting all attempts.
async fn retry_spawn(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
) -> Result<String> {
    let mut last_error: Option<Error> = None;

    for attempt in 1..=MAX_SPAWN_ATTEMPTS {
        match mgr.ensure_started(agent_id, workspace_id).await {
            Ok(session_id) => return Ok(session_id),
            Err(e) => {
                let retryable = is_retryable_spawn_error(&e);
                let error_msg = e.to_string();
                tracing::warn!(
                    agent = %agent_id,
                    attempt = attempt,
                    max = MAX_SPAWN_ATTEMPTS,
                    retryable = retryable,
                    error = %e,
                    "agent spawn attempt failed"
                );

                last_error = Some(e);

                // If non-retryable or last attempt, fail immediately
                if !retryable || attempt == MAX_SPAWN_ATTEMPTS {
                    break;
                }

                // Tear down the failed child so the next attempt spawns fresh
                // (narrower than full stop() — only kills child/handle, no worker/busy-flag touch)
                mgr.kill_child_only(agent_id).await;

                // Publish retry status hint with the actual failure kind
                let retry_num = attempt;
                let failure_kind = if error_msg.contains("timed out") {
                    "timed out"
                } else if error_msg.contains("agent stdout closed") {
                    "stdout closed"
                } else {
                    "failed"
                };
                let message = format!(
                    "Agent spawn {} — retrying (attempt {}/{})…",
                    failure_kind,
                    retry_num + 1,
                    MAX_SPAWN_ATTEMPTS
                );
                mgr.services
                    .publish_status_event(
                        workspace_id,
                        agent_id,
                        "spawn-retry",
                        &message,
                        "warning",
                    )
                    .await;

                // Backoff before retry — jittered unless the env override
                // pinned exact delays.
                let (backoff, jitter) = retry_backoff_ms();
                if let Some(&delay_ms) = backoff.get((attempt - 1) as usize) {
                    let delay_ms = if jitter {
                        jitter_delay_ms(delay_ms)
                    } else {
                        delay_ms
                    };
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| Error::Internal("spawn retry loop exhausted without error".to_string())))
}

/// Publish the terminal `agent:failed` + `agent:stream:end` event pair for a
/// failure the streaming path did NOT already surface. The error message
/// deliberately excludes recent stderr to avoid leaking secrets (API keys,
/// tokens, file paths) to subscribed clients; stderr stays server-side in logs.
async fn publish_terminal_failure_events(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    error_msg: &str,
    turn_id: Option<&str>,
) {
    use intent_core::events::{AGENT_FAILED, AGENT_STREAM_END};

    let mut failed_data = json!({ "agentId": agent_id.0, "error": error_msg });
    let mut end_data = json!({ "agentId": agent_id.0 });
    if let Some(tid) = turn_id {
        failed_data["turnId"] = json!(tid);
        end_data["turnId"] = json!(tid);
    }
    mgr.services
        .publish_agent_event(workspace_id, agent_id, AGENT_FAILED, failed_data)
        .await;
    mgr.services
        .publish_agent_event(workspace_id, agent_id, AGENT_STREAM_END, end_data)
        .await;
}

/// Durable slice of the terminal-failure path (monorepo#2009): record the
/// identical-failure streak and complete the `status = Error` + `stop_reason`
/// store write WITHOUT touching the event bus. Every terminal-failure handler
/// awaits this BEFORE publishing the terminal `agent:failed` +
/// `agent:stream:end` pair (durable-before-observable), so a client that
/// reads `agent.get`/`agent.getSession` upon observing either event is
/// guaranteed the persisted Error. Returns the context the observable half
/// ([`publish_error_status_and_requeue`]) needs to emit the matching events.
pub(crate) struct PersistedTerminalError {
    /// The failure text persisted into `agent_session.stop_reason`.
    error_text: String,
    /// Identical-failure streak AFTER recording this failure (monorepo#840).
    streak: u32,
    /// Whether the failure classifies the session as corrupted/poisoned
    /// (monorepo#940), surfaced as `sessionCorrupted` on the wire.
    session_corrupted: bool,
    /// Timestamp persisted as `stop_reason_timestamp` alongside the status.
    ts: String,
    /// Whether the store write landed; `agent:status-changed` is only
    /// emitted for a durable write.
    status_persisted: bool,
}

/// Durable slice of the terminal-failure path, over the bare [`Services`]
/// handle (monorepo#2050): the streaming path ([`Services::run_prompt_turn`])
/// has no [`AgentManager`], but the persist touches only the services surface,
/// so it can run there too — ahead of the streaming path's own terminal
/// emits — and the resulting [`PersistedTerminalError`] is handed to the worker
/// via [`Services::stash_pending_terminal_error`]. The `&AgentManager` entry
/// point [`persist_terminal_error_status`] delegates here.
pub(crate) async fn persist_terminal_error_status_via_services(
    services: &Services,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    error_text: &str,
) -> PersistedTerminalError {
    // monorepo#840: record the identical-failure streak BEFORE persisting so
    // `session_poisoned` sees a consistent (status, streak) pair as soon as
    // the Error status lands.
    let streak = services.record_terminal_failure(agent_id, error_text);
    // monorepo#940: the same classification that quarantines the session
    // (`session_poisoned`) is surfaced on the wire as `sessionCorrupted` so
    // clients get a structured "retry will recreate" signal, not just the raw
    // stopReason string.
    let session_corrupted = streak >= crate::POISONED_FAILURE_STREAK_THRESHOLD
        || crate::is_session_fatal_error(error_text);
    if session_corrupted {
        tracing::warn!(
            agent = %agent_id,
            streak,
            "session classified as poisoned; deliveries will park in the queue until agent.retry or a fresh agent (monorepo#840)"
        );
    }
    // Persist agent status as Error WITH stop_reason. Durable-before-observable
    // (monorepo#2009): this write completes BEFORE any terminal event reaches
    // the bus, so subscribers see the canonical fields immediately via
    // agent.get/getSession.
    let ts = now_iso();
    let status_persisted = match services
        .store
        .set_agent_session_status(
            workspace_id,
            agent_id,
            AgentStatus::Error,
            false,
            &ts,
            Some(Some(error_text.to_string())),
        )
        .await
    {
        Ok(_) => true,
        Err(e) => {
            tracing::warn!(agent = %agent_id, error = %e, "failed to persist error status + stop_reason");
            false
        }
    };
    PersistedTerminalError {
        error_text: error_text.to_string(),
        streak,
        session_corrupted,
        ts,
        status_persisted,
    }
}

/// `&AgentManager` entry point for the durable terminal-error persist; thin
/// delegate to [`persist_terminal_error_status_via_services`].
async fn persist_terminal_error_status(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    error_text: &str,
) -> PersistedTerminalError {
    persist_terminal_error_status_via_services(&mgr.services, agent_id, workspace_id, error_text)
        .await
}

/// Observable half of the terminal-failure path: emit `agent:status-changed`
/// for the Error persisted by [`persist_terminal_error_status`] and requeue
/// the failed message to the front of the queue so `agent.retry` — or a
/// future `agent.sendMessage` — can redrive it. Shared by the terminal spawn-
/// and turn-failure paths; runs AFTER the terminal `agent:failed` +
/// `agent:stream:end` pair so the wire order (`agent:failed` →
/// `agent:stream:end` → `agent:status-changed` → `agent:queue:updated`) is
/// unchanged by the monorepo#2009 persist reorder. The persisted `error_text`
/// is included in the `agent:status-changed` event's `stopReason` /
/// `stopReasonTimestamp` fields. `persisted` reports whether the failed
/// turn's user row durably reached the transcript (STAB-51). A system-role
/// transcript notice carrying the error text (`meta.kind = "turn-failure"`,
/// the InterruptionNotice shape, §5.35) is appended best-effort for each
/// DISTINCT terminal failure — a repeat of the identical failure text with
/// no intervening `agent.retry` or successful turn (streak > 1, e.g. a
/// fresh redrive of the same message that fails again the same way) skips
/// the append so the transcript never stacks duplicate cards. `agent.retry`
/// clears the streak (the deliberate quarantine escape hatch), so a failure
/// with the SAME text immediately after a retry still gets its own card —
/// the user acted and it failed again, which is new information.
async fn publish_error_status_and_requeue(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    options: &TurnOptions,
    persisted: bool,
    error: PersistedTerminalError,
) {
    let PersistedTerminalError {
        error_text,
        streak,
        session_corrupted,
        ts,
        status_persisted,
    } = error;
    let error_text = error_text.as_str();
    if status_persisted {
        // Emit agent:status-changed with stopReason + stopReasonTimestamp so live
        // subscribers get the canonical fields (the timestamp matches the value
        // persisted alongside stop_reason by `set_agent_session_status`).
        // `sessionCorrupted: true` is included only when the failure classifies as
        // corrupted/poisoned (absent otherwise, matching the serialized projections).
        let mut data = json!({
            "agentId": agent_id.0,
            "status": "error",
            "isActive": false,
            "stopReason": error_text,
            "stopReasonTimestamp": ts,
        });
        if session_corrupted {
            data["sessionCorrupted"] = json!(true);
        }
        let event = NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: ts.clone(),
            event_type: AGENT_STATUS_CHANGED.to_string(),
            actor: agent_actor(agent_id),
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        crate::publish_event(&mgr.services.event_bus, event).await;
    }

    // Requeue the failed message to the front of the queue. `persisted`
    // carries the CONFIRMED durability of the user row (STAB-51): `true` only
    // when the pre-turn transcript append succeeded, so the retry drain skips
    // the duplicate append; `false` when it failed, so the retry drain
    // re-attempts it. `requeued_after_failure` is set so the wire emits
    // `requeuedAfterFailure: true` (STAB-112). Drained turns carry the
    // entry's ORIGINAL `queued_at` in `options` so the #576 staleness verdict
    // stays sticky across the requeue (a failed stale redrive is still stale
    // on retry); direct sends have no prior timestamp and stamp `now_iso()`.
    // The combined-delivery `prepend_*` fields (monorepo#1014) ride along so
    // a failed zero-output interrupt turn's retry still delivers the
    // preempted message ahead of the interrupt message. The entry gets a NEW
    // `id` but keeps the failed turn's ORIGINAL `turn_id` (monorepo#1022) so
    // the retry correlates with the turn it redrives; a missing option (bare
    // test wiring — spawn_worker always mints one) falls back to the new id.
    let id = new_message_id();
    let queued = crate::agent_ops::QueuedMessage {
        turn_id: options.turn_id.clone().unwrap_or_else(|| id.clone()),
        id,
        content: content.to_string(),
        image_blocks: options.image_blocks.clone(),
        file_blocks: options.file_blocks.clone(),
        queued_at: options.queued_at.clone().unwrap_or_else(now_iso),
        editing: false,
        persisted,
        requeued_after_failure: true,
        message_metadata: options.message_metadata.clone(),
        prepend_content: options.prepend_content.clone(),
        prepend_image_blocks: options.prepend_image_blocks.clone(),
        prepend_file_blocks: options.prepend_file_blocks.clone(),
        interrupt_priority: options.interrupt_priority,
        user_origin: options.origin.is_user(),
    };
    mgr.services.requeue_front(agent_id, queued);

    // Publish queue updated so FE reflects the requeued message
    mgr.services
        .publish_queue_updated_for(
            agent_id,
            workspace_id,
            mgr.services.queue_snapshot(agent_id),
        )
        .await;

    // A top-level agent parked in Error drives the `failed` displayStatus
    // rung (§6.5 step 0): recompute-and-compare so the promotion emits.
    // Deliberately AFTER the requeue: clients key on status-changed →
    // queue-updated ordering, and the recompute must not widen that window.
    mgr.services
        .maybe_emit_display_status_changed(workspace_id)
        .await;

    // Durable transcript record of the terminal failure: a system-role message
    // with a single text block carrying the error text and
    // `meta.kind = "turn-failure"` (the InterruptionNotice shape, §5.35 — the
    // same pattern as the discussion-request/blocker-report notices in
    // `agent_request_attention_op`). Appended only on the FIRST occurrence of
    // a failure text in the current streak (streak == 1): a same-text failure
    // repeating with no intervening `agent.retry` or successful turn already
    // has its card in the transcript, so the append is skipped instead of
    // stacking duplicates. `agent.retry` resets the streak, so a same-text
    // failure right after a retry DOES get a fresh card (streak restarts at
    // 1) — the user acted and it failed again, which is new information.
    // Best-effort and deliberately LAST: the persisted (status, stop_reason,
    // stop_reason_timestamp) and the requeue above are the durable contract —
    // an append failure is logged and swallowed, and the append never delays
    // the status-changed → requeue sequence clients key on.
    if streak == 1 {
        let notice_content = json!([{
            "type": "text",
            "text": error_text,
            "meta": { "kind": "turn-failure" }
        }]);
        match mgr
            .services
            .store
            .append_agent_message(agent_id, "system", &notice_content, &ts)
            .await
        {
            Ok(message) => {
                mgr.services.invalidate_agent_list_cache(workspace_id);
                mgr.services
                    .publish_agent_mutation_event(
                        workspace_id,
                        agent_id,
                        intent_core::events::AGENT_MESSAGE,
                        json!({ "agentId": agent_id.0, "messageId": message.id, "role": "system" }),
                    )
                    .await;
            }
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "failed to append turn-failure transcript notice"
                );
            }
        }
    }
}

/// Single-call composition of [`persist_terminal_error_status`] +
/// [`publish_error_status_and_requeue`] — the exact production halves in
/// production order, minus the terminal `agent:failed`/`agent:stream:end`
/// pair the handlers publish in between. Kept for the unit suite, which
/// exercises the persist + requeue contract without the terminal pair.
#[cfg(test)]
async fn persist_error_and_requeue(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    options: &TurnOptions,
    persisted: bool,
    error_text: &str,
) {
    let persist = persist_terminal_error_status(mgr, agent_id, workspace_id, error_text).await;
    publish_error_status_and_requeue(
        mgr,
        agent_id,
        workspace_id,
        content,
        options,
        persisted,
        persist,
    )
    .await;
}

/// Handle terminal spawn failure after all retries are exhausted. Persists
/// the agent status as `Error` with the error text into `stop_reason`
/// (durable-before-observable, monorepo#2009), publishes terminal
/// `agent:failed` and `agent:stream:end` events, requeues the failed message
/// to the front of the queue, and stops draining further messages.
async fn handle_terminal_spawn_failure(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    options: &TurnOptions,
    persisted: bool,
    error: &Error,
) {
    let error_text = error.to_string();
    // Durable-before-observable (monorepo#2009): the Error + stop_reason store
    // write completes before the terminal pair reaches the bus, so a client
    // reading agent.getSession on agent:stream:end sees the persisted `error`.
    let persist = persist_terminal_error_status(mgr, agent_id, workspace_id, &error_text).await;
    publish_terminal_failure_events(
        mgr,
        agent_id,
        workspace_id,
        &error_text,
        options.turn_id.as_deref(),
    )
    .await;
    publish_error_status_and_requeue(
        mgr,
        agent_id,
        workspace_id,
        content,
        options,
        persisted,
        persist,
    )
    .await;
}

/// Handle a pre-turn persist failure after `persist_user` exhausted its
/// bounded retry (#547, fail-closed drain). The turn was NOT started, so —
/// unlike the spawn/turn failure handlers — there is no child to tear down
/// and no partial stream to close; the terminal event pair + Error park +
/// front requeue (`persisted: false`) make the failure observable and
/// redrivable via `agent.retry`. Callers release the in-flight slot after.
async fn handle_drain_persist_failure(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    options: &TurnOptions,
) {
    let error_text = "failed to persist user message to transcript; turn not started".to_string();
    tracing::warn!(agent = %agent_id, "queue drain failed closed: {error_text}");
    // Durable-before-observable (monorepo#2009): persist Error + stop_reason
    // before the terminal pair reaches the bus.
    let persist = persist_terminal_error_status(mgr, agent_id, workspace_id, &error_text).await;
    publish_terminal_failure_events(
        mgr,
        agent_id,
        workspace_id,
        &error_text,
        options.turn_id.as_deref(),
    )
    .await;
    publish_error_status_and_requeue(
        mgr,
        agent_id,
        workspace_id,
        content,
        options,
        false,
        persist,
    )
    .await;
}

/// Prefix `run_prompt_turn` wraps every post-prompt failure with (see
/// `agent_session.rs`): `Error::Internal(format!("session/prompt failed: {e}"))`.
const PROMPT_FAILED_PREFIX: &str = "session/prompt failed:";

/// The ACP cancellation surface inside the [`PROMPT_FAILED_PREFIX`] wrapper,
/// if any. The structured `AcpError` is flattened to a string at the wrap
/// boundary, so this recovers the cancellation signal from the two known
/// shapes: the JSON-RPC `-32800` request-cancelled code (rendered as
/// `JSON-RPC error -32800: …` by `intent-acp`'s `JsonRpcError` Display), or a
/// provider resolving the prompt with a "cancelled" error message.
///
/// RPC-shaped errors anchor on the code alone (monorepo#518): Display appends
/// the provider-controlled `error.data` payload after the message, and the
/// two are indistinguishable once flattened — a terminal error whose data
/// merely mentions "cancelled" must not be misclassified as benign. The ACP
/// spec's only sanctioned cancel-error shape is code `-32800` (the message is
/// free text there too). The "cancelled" substring heuristic remains for
/// non-RPC renderings, which carry no data suffix.
pub(crate) fn prompt_cancellation_error(err: &Error) -> bool {
    let Error::Internal(msg) = err else {
        return false;
    };
    let Some(inner) = msg.strip_prefix(PROMPT_FAILED_PREFIX) else {
        return false;
    };
    if let Some(rest) = inner.trim_start().strip_prefix("JSON-RPC error ") {
        let code = rest.split(':').next().unwrap_or("").trim();
        return code == "-32800";
    }
    inner.to_ascii_lowercase().contains("cancelled")
}

/// Classify a [`AgentManager::run_turn`] error as benign (an expected outcome
/// of a concurrent stop/cancel — NOT a failure to surface) vs terminal.
///
/// Benign:
/// - `NotFound` — the agent handle disappeared between `ensure_started` and
///   `run_turn`, i.e. a concurrent `agent.stop`/teardown won the race.
/// - a cancellation error inside the `session/prompt failed:` wrapper — the
///   provider resolved the in-flight `session/prompt` with the JSON-RPC
///   `-32800` request-cancelled code (or a "cancelled" message) instead of
///   `StopReason::Cancelled` after a `session/cancel`. Errors that merely
///   mention "cancelled" OUTSIDE that wrapper stay terminal.
///
/// Everything else (transport closed, agent stdout closed, response channel
/// dropped, prompt timeout, provider JSON-RPC errors, store append failures)
/// is terminal: the turn died mid-flight and must be surfaced (STAB-6
/// semantics). Deliberately errs on the side of terminal — a false "failed"
/// surface with a Retry button beats a silently dropped message.
fn is_benign_turn_error(err: &Error) -> bool {
    if matches!(err, Error::NotFound(_)) {
        return true;
    }
    prompt_cancellation_error(err)
}

/// STAB-53: when a terminal failure means the child died mid-turn ("agent
/// stdout closed") and stderr capture is enabled, return the capture directory
/// for `agent_id` so the WARN line can point at the child's last words.
/// Matches on the structured `Error::Internal` payload — the transport's
/// child-death error is always wrapped there (handshake/prompt failures) —
/// avoiding a Display allocation per check.
fn stderr_capture_hint(
    mgr: &AgentManager,
    agent_id: &AgentId,
    err: &Error,
) -> Option<std::path::PathBuf> {
    if !matches!(err, Error::Internal(msg) if msg.contains("agent stdout closed")) {
        return None;
    }
    mgr.agent_stderr_log_dir(agent_id)
}

/// Whether `run_prompt_turn` already emitted the terminal `agent:failed` +
/// `agent:stream:end` pair for this error. Its post-prompt failure path wraps
/// every prompt error as `Internal("session/prompt failed: …")` AFTER emitting
/// both events; errors WITHOUT that prefix (e.g. the transcript-append store
/// error, which propagates via `?` before the emits, or a pre-output
/// transport failure — see [`pre_output_transport_failure`] — whose emits are
/// deliberately suppressed for a possible silent redrive) still need the
/// events. Prefix-anchored on the structured `Error::Internal` payload so an
/// unrelated error that merely mentions the phrase mid-string cannot
/// suppress the terminal events.
fn turn_failure_events_already_emitted(err: &Error) -> bool {
    matches!(err, Error::Internal(msg) if msg.starts_with(PROMPT_FAILED_PREFIX))
}

/// Whether a `run_turn` error carries the pre-output transport marker from
/// `run_prompt_turn` (monorepo#764): the transport to the child closed BEFORE
/// the turn streamed any output, so the prompt provably produced nothing and
/// the worker may redrive it once on a fresh child. `run_prompt_turn`
/// suppressed the terminal `agent:failed` + `agent:stream:end` pair for these
/// errors — when the one-retry budget is already spent, the terminal-failure
/// path emits the pair itself (`turn_failure_events_already_emitted` is false
/// for this prefix). Prefix-anchored for the same mid-string reason as above.
fn pre_output_transport_failure(err: &Error) -> bool {
    matches!(
        err,
        Error::Internal(msg)
            if msg.starts_with(crate::agent_session::PROMPT_PRE_OUTPUT_TRANSPORT_PREFIX)
    )
}

/// Whether a `run_turn` error carries the sleep-induced interrupt marker from
/// `run_prompt_turn` (Task C): the turn died with a transient upstream
/// disconnect whose active window overlapped a detected host suspend.
/// `run_prompt_turn` already enrolled the turn as interrupted (persisted the
/// partial with `InterruptReason::SystemSuspend` + an `interrupted_agent` row)
/// and emitted the interrupted terminal `agent:stream:end` (NOT `agent:failed`),
/// so the worker must SUPPRESS the terminal-failure path — no Error status, no
/// manual-retry surface — and leave the enrolled turn for the wake orchestrator
/// (Task D) to resume. Prefix-anchored for the same mid-string reason as the
/// other markers.
fn suspend_interrupt_error(err: &Error) -> bool {
    matches!(
        err,
        Error::Internal(msg)
            if msg.starts_with(crate::agent_session::PROMPT_SUSPEND_INTERRUPT_PREFIX)
    )
}

/// Whether a `run_turn` error is the `session/prompt` idle timeout
/// (`AcpError::PromptIdleTimeout`: the whole idle window passed with no
/// `session/update` traffic). The structured `AcpError` is flattened to a
/// string at the wrap boundary (`session/prompt failed: {e}` in
/// `run_prompt_turn`), so classification is prefix-anchored on
/// [`intent_acp::PROMPT_IDLE_TIMEOUT_PREFIX`] inside the
/// [`PROMPT_FAILED_PREFIX`] wrapper — mirroring
/// [`prompt_cancellation_error`]. The child process is typically still alive
/// and healthy after this error (the turn merely went silent), which is what
/// makes a warn-and-continue treatment possible.
fn prompt_idle_timeout_error(err: &Error) -> bool {
    let Error::Internal(msg) = err else {
        return false;
    };
    let Some(inner) = msg.strip_prefix(PROMPT_FAILED_PREFIX) else {
        return false;
    };
    inner
        .trim_start()
        .starts_with(intent_acp::PROMPT_IDLE_TIMEOUT_PREFIX)
}

/// Max consecutive idle-timeout warning redrives before the worker falls back
/// to the terminal-failure path (user-confirmed cap of 3): the 1st–3rd
/// back-to-back silent timeouts each get a warning turn; the 4th is terminal.
const MAX_CONSECUTIVE_IDLE_TIMEOUT_REDRIVES: u32 = 3;

/// Whether an idle-timeout error carries the streamed-output marker
/// ([`crate::agent_session::PROMPT_IDLE_TIMEOUT_STREAMED_SUFFIX`]): the
/// timed-out turn produced output before going silent, which counts as
/// intervening activity for the consecutive-timeout accounting (the counter
/// restarts at 1 instead of accumulating).
fn idle_timeout_turn_streamed(err: &Error) -> bool {
    matches!(
        err,
        Error::Internal(msg)
            if msg.ends_with(crate::agent_session::PROMPT_IDLE_TIMEOUT_STREAMED_SUFFIX)
    )
}

/// Render the warn-and-continue message injected after an idle timeout. The
/// window is the ACTUAL configured value ([`intent_acp::session::prompt_idle_timeout`],
/// i.e. `INTENTD_PROMPT_IDLE_TIMEOUT_MS` / 1800s default), not a hardcoded
/// literal. Wording owned by the harness (H6); the caller renders the
/// seconds value (integer form for whole windows, float otherwise).
pub(crate) fn idle_timeout_warning_text(window: std::time::Duration) -> String {
    let secs = window.as_secs_f64();
    let rendered = if secs.fract() == 0.0 {
        window.as_secs().to_string()
    } else {
        secs.to_string()
    };
    crate::harness::latest().idle_timeout_warning(&rendered)
}

/// Handle a terminal mid-turn failure (`run_turn` error that is not a benign
/// cancel): tear down the dead child, persist `AgentStatus::Error` with the
/// error text into `stop_reason` (durable-before-observable, monorepo#2009),
/// ensure the terminal `agent:failed` + `agent:stream:end` pair reached the
/// bus, and requeue the message for `agent.retry`.
/// Mirrors [`handle_terminal_spawn_failure`] but does NOT auto-retry inline —
/// the prompt may have been partially processed, so redriving it is a user
/// decision (the STAB-6 Retry surface).
async fn handle_terminal_turn_failure(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    options: &TurnOptions,
    persisted: bool,
    error: &Error,
) {
    // Tear down the (likely dead) child so the retry path spawns fresh. Safe
    // from within the worker: only kills child/handle, no worker/busy touch.
    mgr.kill_child_only(agent_id).await;

    let error_text = error.to_string();
    // Durable-before-observable (monorepo#2009/#2050): persist Error +
    // stop_reason before the terminal pair (when this path still owes it)
    // reaches the bus. The streaming path (`run_prompt_turn`) that already
    // emitted the pair also already persisted (durable-before-observable there
    // too) and stashed the context — reuse it so the identical-failure streak
    // (monorepo#840) is recorded exactly once and the Error status is not
    // written twice. Fall back to persisting here when no stash exists (the
    // pre-#2050 path: a `?`-propagated store error, or the idle-timeout-cap
    // branch before its own persist). The stash is consumed only when its
    // error text matches THIS failure — a self-validating guard on top of the
    // teardown-path discards: a stale context (however it survived) must not
    // skip the new failure's own persist or mis-describe it on the wire.
    let persist = match mgr.services.take_pending_terminal_error(agent_id) {
        Some(precomputed) if precomputed.error_text == error_text => precomputed,
        _ => persist_terminal_error_status(mgr, agent_id, workspace_id, &error_text).await,
    };
    if !turn_failure_events_already_emitted(error) {
        publish_terminal_failure_events(
            mgr,
            agent_id,
            workspace_id,
            &error_text,
            options.turn_id.as_deref(),
        )
        .await;
    }
    publish_error_status_and_requeue(
        mgr,
        agent_id,
        workspace_id,
        content,
        options,
        persisted,
        persist,
    )
    .await;
}

#[cfg(test)]
mod role_reminder_tests {
    //! Role-reminder injection cadence over [`AgentManager::build_turn_prompt`]
    //! (port of acp-provider.ts): every user turn (interval = 1) and also after a
    //! session recreate for specialist agents; never for non-specialist agents.

    use super::*;
    use crate::events::EventBus;
    use intent_core::{AgentStatus, Workspace, WorkspaceActivity, WorkspaceStatus};
    use intent_store::Store;

    /// Seed a hermetic specialists dir under temp with one `<id>.md`. Keep the
    /// returned RAII guard alive for the test (dropping it removes the dir).
    fn write_specialist(id: &str, content: &str) -> tempfile::TempDir {
        let dir = crate::tests::test_tempdir("intentd-spc-");
        std::fs::write(dir.path().join(format!("{id}.md")), content).unwrap();
        dir
    }

    pub(super) fn workspace(id: &WorkspaceId) -> Workspace {
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
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
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

    pub(super) fn session(
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        specialist: Option<&str>,
    ) -> AgentSession {
        let ts = now_iso();
        AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: workspace_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Builder".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: specialist.map(str::to_string),
            status: AgentStatus::Pending,
            is_active: true,
            messages: Vec::new(),
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
            created_at: ts.clone(),
            updated_at: ts,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        }
    }

    /// Build a manager over a temp store seeded with a workspace + agent
    /// session. The returned RAII guard owns the db dir (db + `-wal`/`-shm`
    /// sidecars); keep it alive for the duration of the test.
    pub(super) async fn manager_with(
        specialist: Option<&str>,
        specialists_dir: Option<PathBuf>,
    ) -> (AgentManager, AgentId, tempfile::TempDir) {
        let db_dir = crate::tests::test_tempdir("intentd-rr-");
        let path = db_dir.path().join("store.db");
        let store = Store::open(&path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_specialist_dirs(
                specialists_dir,
                Some(std::env::temp_dir().join("nonexistent-bundled")),
            );
        let workspace_id = WorkspaceId::from("ws-1");
        let agent_id = AgentId::from("agent-1");
        store
            .insert_workspace(&workspace(&workspace_id))
            .await
            .unwrap();
        store
            .insert_agent_session(&session(&agent_id, &workspace_id, specialist))
            .await
            .unwrap();
        let sink = Arc::new(BusEventSink::new(bus));
        (AgentManager::new(services, sink, 4), agent_id, db_dir)
    }

    /// First text block's text from a built prompt.
    fn prompt_text(prompt: &[ContentBlock]) -> String {
        serde_json::to_value(prompt).unwrap()[0]["text"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// `stop()` clears `recreated`/`prepend_pending` (stale-flag hygiene) but
    /// MUST keep `force_recreate` armed: an `agent.editAndRegenerate`
    /// truncation is already persisted, so the stale provider session must
    /// never be resumed no matter how many stops intervene before the next
    /// turn.
    #[tokio::test]
    async fn stop_preserves_force_recreate_but_clears_recreated() {
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        mgr.force_recreate.lock().unwrap().insert(agent_id.clone());
        mgr.recreated.lock().unwrap().insert(agent_id.clone());
        mgr.stop(&agent_id).await;
        assert!(
            mgr.force_recreate.lock().unwrap().contains(&agent_id),
            "force_recreate survives stop()"
        );
        assert!(
            !mgr.recreated.lock().unwrap().contains(&agent_id),
            "recreated cleared by stop()"
        );
    }

    #[tokio::test]
    async fn injects_reminder_every_turn_for_specialist() {
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nbody",
        );
        let (mgr, agent_id, _db) =
            manager_with(Some("implementor"), Some(dir.path().to_path_buf())).await;
        // Interval = 1 → every turn carries the prefix.
        for _ in 0..2 {
            let prompt = mgr
                .build_turn_prompt(
                    &agent_id,
                    &WorkspaceId::from("ws-role"),
                    "do the thing",
                    &TurnOptions::default(),
                )
                .await;
            let text = prompt_text(&prompt);
            assert!(
                text.starts_with("[Role Reminder: You are a Implementor. Stay in scope.]\n\n"),
                "missing reminder prefix: {text:?}"
            );
            assert!(text.ends_with("do the thing"));
        }
    }

    #[tokio::test]
    async fn injects_reminder_on_session_recreate() {
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nbody",
        );
        let (mgr, agent_id, _db) =
            manager_with(Some("implementor"), Some(dir.path().to_path_buf())).await;
        // Flag the agent's session as recreated; the reminder must still prepend.
        mgr.recreated.lock().unwrap().insert(agent_id.clone());
        let prompt = mgr
            .build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-role"),
                "resume work",
                &TurnOptions::default(),
            )
            .await;
        let text = prompt_text(&prompt);
        assert!(
            text.starts_with("[Role Reminder: You are a Implementor. Stay in scope.]\n\n"),
            "missing reminder prefix on recreate: {text:?}"
        );
        // Flag consumed by the turn.
        assert!(!mgr.recreated.lock().unwrap().contains(&agent_id));
    }

    #[tokio::test]
    async fn no_injection_without_specialist() {
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        let prompt = mgr
            .build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-role"),
                "plain message",
                &TurnOptions::default(),
            )
            .await;
        assert_eq!(prompt_text(&prompt), "plain message");
    }

    #[tokio::test]
    async fn stdin_context_is_prepended_as_context_block() {
        // Reference-parity `acp-provider.ts` §5.5: `stdinContext` is prepended
        // to the outbound prompt as `Context:\n<ctx>\n\n---\n\n<body>` before
        // any role reminder. Applies to both plain and specialist agents.
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        let opts = TurnOptions {
            stdin_context: Some("hello ctx".to_string()),
            ..TurnOptions::default()
        };
        let prompt = mgr
            .build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-role"),
                "user says hi",
                &opts,
            )
            .await;
        let text = prompt_text(&prompt);
        assert_eq!(text, "Context:\nhello ctx\n\n---\n\nuser says hi");
    }

    #[tokio::test]
    async fn stdin_context_empty_string_is_not_prepended() {
        // An empty `stdinContext` is treated as absent so we do not emit a
        // stray `Context:` header with nothing under it.
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        let opts = TurnOptions {
            stdin_context: Some(String::new()),
            ..TurnOptions::default()
        };
        let prompt = mgr
            .build_turn_prompt(&agent_id, &WorkspaceId::from("ws-role"), "body", &opts)
            .await;
        assert_eq!(prompt_text(&prompt), "body");
    }

    #[tokio::test]
    async fn stdin_context_precedes_role_reminder() {
        // Ordering: `Context:` block first, then the role reminder, then the
        // body — matching the reference `acp-provider.ts` prompt-assembly.
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nbody",
        );
        let (mgr, agent_id, _db) =
            manager_with(Some("implementor"), Some(dir.path().to_path_buf())).await;
        let opts = TurnOptions {
            stdin_context: Some("ctx".to_string()),
            ..TurnOptions::default()
        };
        let prompt = mgr
            .build_turn_prompt(&agent_id, &WorkspaceId::from("ws-role"), "do it", &opts)
            .await;
        let text = prompt_text(&prompt);
        assert!(
            text.starts_with("Context:\nctx\n\n---\n\n[Role Reminder:"),
            "unexpected ordering: {text:?}"
        );
        assert!(text.ends_with("do it"));
    }

    // ---- Spawn-prompt specialist injection (PP-1) ----

    #[tokio::test]
    async fn specialist_injection_resolves_prompt_from_file() {
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nImplement the task.",
        );
        let (mgr, agent_id, _db) =
            manager_with(Some("implementor"), Some(dir.path().to_path_buf())).await;
        let inj = mgr
            .services
            .agent_specialist_injection(&agent_id, None)
            .await
            .expect("injection for specialist agent");
        assert_eq!(inj.behavior_prompt.as_deref(), Some("Implement the task."));
        assert_eq!(inj.specialist_name.as_deref(), Some("Implementor"));
        assert_eq!(inj.role_reminder.as_deref(), Some("Stay in scope."));
    }

    #[tokio::test]
    async fn specialist_injection_metadata_behavior_prompt_wins() {
        // The session's persisted `metadata.behaviorPrompt` override wins over
        // the specialist file's body; name/reminder still come from the file.
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nFile body.",
        );
        let (mgr, _first, _db) =
            manager_with(Some("implementor"), Some(dir.path().to_path_buf())).await;
        let agent_id = AgentId::from("agent-2");
        let mut s = session(&agent_id, &WorkspaceId::from("ws-1"), Some("implementor"));
        s.metadata = Some(serde_json::json!({ "behaviorPrompt": "Custom override." }));
        mgr.services
            .store
            .insert_agent_session(&s)
            .await
            .expect("insert session");
        let inj = mgr
            .services
            .agent_specialist_injection(&agent_id, None)
            .await
            .expect("injection");
        assert_eq!(inj.behavior_prompt.as_deref(), Some("Custom override."));
        assert_eq!(inj.specialist_name.as_deref(), Some("Implementor"));
        assert_eq!(inj.role_reminder.as_deref(), Some("Stay in scope."));
    }

    #[tokio::test]
    async fn specialist_injection_none_for_plain_agent() {
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        assert!(mgr
            .services
            .agent_specialist_injection(&agent_id, None)
            .await
            .is_none());
    }

    // ---- FirstTurnPrepend fallback (§18.1) ----

    /// Persist an assembled system prompt on the seeded agent session (the
    /// spawn path does this in `create_agent`; unit tests seed it directly).
    async fn set_system_prompt(mgr: &AgentManager, agent_id: &AgentId, prompt: &str) {
        let mut s = mgr
            .services
            .store
            .get_agent_session(agent_id)
            .await
            .expect("session");
        let ws = s.workspace_id.clone();
        s.system_prompt = Some(prompt.to_string());
        mgr.services
            .store
            .update_agent_session(&ws, &s)
            .await
            .expect("persist system_prompt");
    }

    #[tokio::test]
    async fn first_turn_prepend_fires_once_per_fresh_session() {
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        set_system_prompt(&mgr, &agent_id, "You are helpful.").await;
        let mock = intent_providers::find_provider("mock").unwrap();
        mgr.arm_first_turn_prepend(&agent_id, mock);
        // First turn carries the <system>-wrapped assembled prompt first.
        let prompt = mgr
            .build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "first message",
                &TurnOptions::default(),
            )
            .await;
        let text = prompt_text(&prompt);
        assert!(
            text.starts_with("<system>\nYou are helpful.\n</system>\n\n"),
            "missing first-turn prepend: {text:?}"
        );
        assert!(text.ends_with("first message"));
        // Second turn on the SAME session must not repeat it.
        let prompt = mgr
            .build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "second message",
                &TurnOptions::default(),
            )
            .await;
        let text = prompt_text(&prompt);
        assert!(
            !text.contains("<system>\nYou are helpful."),
            "prepend repeated on second turn: {text:?}"
        );
    }

    #[tokio::test]
    async fn first_turn_prepend_refires_after_recreate() {
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        set_system_prompt(&mgr, &agent_id, "SP body").await;
        let mock = intent_providers::find_provider("mock").unwrap();
        // Fresh session → fires; consumed by the first turn.
        mgr.arm_first_turn_prepend(&agent_id, mock);
        let first = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "one",
                &TurnOptions::default(),
            )
            .await,
        );
        assert!(first.starts_with("<system>\nSP body\n</system>"));
        // Recreate path re-arms (start_session recreate branch) → fires again.
        mgr.arm_first_turn_prepend(&agent_id, mock);
        let again = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "two",
                &TurnOptions::default(),
            )
            .await,
        );
        assert!(
            again.starts_with("<system>\nSP body\n</system>"),
            "prepend must re-fire after session recreation: {again:?}"
        );
    }

    #[tokio::test]
    async fn first_turn_prepend_not_armed_for_native_mechanism_providers() {
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        set_system_prompt(&mgr, &agent_id, "native SP").await;
        // Native-mechanism providers (rules file / _meta / env) never arm the
        // fallback — no double injection.
        for id in ["auggie", "claude-code", "opencode", "droid"] {
            let provider = intent_providers::find_provider(id).unwrap();
            mgr.arm_first_turn_prepend(&agent_id, provider);
        }
        assert!(
            !mgr.prepend_pending.lock().unwrap().contains(&agent_id),
            "native-mechanism providers must not arm the prepend fallback"
        );
        let text = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "hello",
                &TurnOptions::default(),
            )
            .await,
        );
        assert_eq!(text, "hello");
    }

    // ---- Per-turn agent state snapshot injection ----

    /// Like [`manager_with`] but with a writable TOML-backed settings
    /// registry wired, so tests can flip `agentFeatures.stateSnapshot` and
    /// observe the live per-turn read.
    async fn manager_with_registry() -> (AgentManager, AgentId, tempfile::TempDir, tempfile::TempDir)
    {
        let db_dir = crate::tests::test_tempdir("intentd-snap-");
        let path = db_dir.path().join("store.db");
        let store = Store::open(&path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let config_dir = tempfile::tempdir().expect("temp config dir");
        let registry = Arc::new(
            crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
                .expect("load registry"),
        );
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_settings_registry(registry);
        let workspace_id = WorkspaceId::from("ws-1");
        let agent_id = AgentId::from("agent-1");
        store
            .insert_workspace(&workspace(&workspace_id))
            .await
            .unwrap();
        store
            .insert_agent_session(&session(&agent_id, &workspace_id, None))
            .await
            .unwrap();
        let sink = Arc::new(BusEventSink::new(bus));
        (
            AgentManager::new(services, sink, 4),
            agent_id,
            db_dir,
            config_dir,
        )
    }

    /// Make the agent's snapshot non-trivial (one pending queued message).
    fn make_snapshot_nontrivial(mgr: &AgentManager, agent_id: &AgentId) {
        mgr.services
            .enqueue_message(agent_id, "pending".into(), None, None, None, None, false);
    }

    /// A trivial snapshot (all counts zero, no attention) never injects:
    /// prompts stay byte-identical to pre-feature output, for specialist and
    /// non-specialist agents alike.
    #[tokio::test]
    async fn snapshot_skipped_when_trivial() {
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        let text = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "plain message",
                &TurnOptions::default(),
            )
            .await,
        );
        assert_eq!(text, "plain message");
    }

    /// A non-trivial snapshot injects `current ws.agent.snapshot() => {...}`
    /// on EVERY turn (rebuilt per turn like the role reminder), including for
    /// non-specialist agents, and also after a session recreate.
    #[tokio::test]
    async fn snapshot_injected_every_turn_for_all_agents() {
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        make_snapshot_nontrivial(&mgr, &agent_id);
        for _ in 0..2 {
            let text = prompt_text(
                &mgr.build_turn_prompt(
                    &agent_id,
                    &WorkspaceId::from("ws-1"),
                    "do the thing",
                    &TurnOptions::default(),
                )
                .await,
            );
            assert!(
                text.starts_with("current ws.agent.snapshot() => {"),
                "missing snapshot prefix: {text:?}"
            );
            assert!(text.contains("\"queuedMessages\":1"), "counts: {text:?}");
            assert!(text.ends_with("do the thing"));
        }
        // Session-recreate turns keep the injection too.
        mgr.recreated.lock().unwrap().insert(agent_id.clone());
        let text = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "resume",
                &TurnOptions::default(),
            )
            .await,
        );
        assert!(
            text.starts_with("current ws.agent.snapshot() => {"),
            "missing snapshot prefix on recreate: {text:?}"
        );
    }

    /// Ordering: the snapshot line is the outermost RECURRING decoration —
    /// before the `Context:` block, naming instruction, and role reminder —
    /// while the fire-once FirstTurnPrepend `<system>` block stays outermost
    /// overall.
    #[tokio::test]
    async fn snapshot_ordering_outermost_recurring_after_first_turn_prepend() {
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nbody",
        );
        let (mgr, agent_id, _db) =
            manager_with(Some("implementor"), Some(dir.path().to_path_buf())).await;
        make_snapshot_nontrivial(&mgr, &agent_id);
        set_system_prompt(&mgr, &agent_id, "SP").await;
        let mock = intent_providers::find_provider("mock").unwrap();
        mgr.arm_first_turn_prepend(&agent_id, mock);
        let opts = TurnOptions {
            stdin_context: Some("ctx".to_string()),
            ..TurnOptions::default()
        };
        let text = prompt_text(
            &mgr.build_turn_prompt(&agent_id, &WorkspaceId::from("ws-1"), "do it", &opts)
                .await,
        );
        assert!(
            text.starts_with("<system>\nSP\n</system>\n\ncurrent ws.agent.snapshot() => {"),
            "FirstTurnPrepend outermost, snapshot next: {text:?}"
        );
        let snapshot_pos = text.find("current ws.agent.snapshot()").unwrap();
        let context_pos = text.find("Context:\nctx").expect("Context block");
        let reminder_pos = text.find("[Role Reminder:").expect("role reminder");
        assert!(snapshot_pos < context_pos && context_pos < reminder_pos);
        // Second turn: prepend consumed, snapshot line now outermost.
        let text = prompt_text(
            &mgr.build_turn_prompt(&agent_id, &WorkspaceId::from("ws-1"), "again", &opts)
                .await,
        );
        assert!(
            text.starts_with("current ws.agent.snapshot() => {"),
            "snapshot outermost once the prepend is consumed: {text:?}"
        );
    }

    /// Live-read toggle: flipping `agentFeatures.stateSnapshot` off removes
    /// the injection from the very next turn of the SAME session (prompt
    /// byte-identical to pre-feature output), and flipping it back restores
    /// it — no session recreation involved.
    #[tokio::test]
    async fn snapshot_toggle_is_read_live_each_turn() {
        let (mgr, agent_id, _db, _cfg) = manager_with_registry().await;
        make_snapshot_nontrivial(&mgr, &agent_id);
        let ws = WorkspaceId::from("ws-1");

        let on = prompt_text(
            &mgr.build_turn_prompt(&agent_id, &ws, "turn", &TurnOptions::default())
                .await,
        );
        assert!(on.starts_with("current ws.agent.snapshot() => {"));

        mgr.services
            .settings_registry()
            .expect("registry wired")
            .apply(&[(
                "agentFeatures.stateSnapshot".to_string(),
                serde_json::json!(false),
            )])
            .expect("apply toggle");
        let off = prompt_text(
            &mgr.build_turn_prompt(&agent_id, &ws, "turn", &TurnOptions::default())
                .await,
        );
        assert_eq!(off, "turn", "toggle off → byte-identical prompt");

        mgr.services
            .settings_registry()
            .expect("registry wired")
            .apply(&[(
                "agentFeatures.stateSnapshot".to_string(),
                serde_json::json!(true),
            )])
            .expect("apply toggle");
        let back_on = prompt_text(
            &mgr.build_turn_prompt(&agent_id, &ws, "turn", &TurnOptions::default())
                .await,
        );
        assert!(
            back_on.starts_with("current ws.agent.snapshot() => {"),
            "toggle back on → next turn injects again: {back_on:?}"
        );
    }

    #[test]
    fn set_model_target_gates_provider_sentinel_and_compound_prefix() {
        let grok = intent_providers::find_provider("grok").unwrap();
        let auggie = intent_providers::find_provider("auggie").unwrap();

        // Providers without supports_set_model never produce a target.
        assert_eq!(
            AgentManager::set_model_target(auggie, Some("opus4.7")),
            None
        );
        // Absent / empty / sentinel models are no-ops.
        assert_eq!(AgentManager::set_model_target(grok, None), None);
        assert_eq!(AgentManager::set_model_target(grok, Some("")), None);
        assert_eq!(AgentManager::set_model_target(grok, Some("default")), None);
        assert_eq!(
            AgentManager::set_model_target(grok, Some("grok:default")),
            None
        );
        // Bare ids are provider-local.
        assert_eq!(
            AgentManager::set_model_target(grok, Some("grok-4.5")),
            Some("grok-4.5")
        );
        // Matching compound prefix strips to the bare id.
        assert_eq!(
            AgentManager::set_model_target(grok, Some("grok:grok-4.5")),
            Some("grok-4.5")
        );
        // A compound id for a DIFFERENT provider (stale pre-spawn provider
        // switch) must not be sent to grok.
        assert_eq!(
            AgentManager::set_model_target(grok, Some("opencode:kimi-k3")),
            None
        );
    }

    #[test]
    fn config_option_model_target_gates_provider_sentinel_and_compound_prefix() {
        let claude = intent_providers::find_provider("claude-code").unwrap();
        let grok = intent_providers::find_provider("grok").unwrap();

        // Providers without supports_config_option_model never produce a
        // target (grok uses session/set_model instead) — and vice versa,
        // claude-code never produces a session/set_model target.
        assert_eq!(
            AgentManager::config_option_model_target(grok, Some("sonnet")),
            None
        );
        assert_eq!(AgentManager::set_model_target(claude, Some("sonnet")), None);
        // Absent / empty / sentinel models are no-ops.
        assert_eq!(AgentManager::config_option_model_target(claude, None), None);
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("")),
            None
        );
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("default")),
            None
        );
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("claude-code:default")),
            None
        );
        // Bare ids are provider-local.
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("sonnet")),
            Some("sonnet")
        );
        // Matching compound prefix strips to the bare id.
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("claude-code:opus")),
            Some("opus")
        );
        // A compound id for a DIFFERENT provider (stale pre-spawn provider
        // switch) must not be sent to claude-code.
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("grok:grok-4.5")),
            None
        );
        // A persisted effective-model display name (D13, contains spaces) is
        // not a real option id and must not be sent back to the provider.
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("claude-code:Opus 4.8")),
            None
        );
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("Opus 4.8")),
            None
        );
    }

    #[tokio::test]
    async fn first_turn_prepend_skipped_when_no_system_prompt() {
        // Armed but the session has no persisted system_prompt (or blank) —
        // no stray empty <system> block; the flag is still consumed.
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        let mock = intent_providers::find_provider("mock").unwrap();
        mgr.arm_first_turn_prepend(&agent_id, mock);
        let text = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "no sp",
                &TurnOptions::default(),
            )
            .await,
        );
        assert_eq!(text, "no sp");
        assert!(!mgr.prepend_pending.lock().unwrap().contains(&agent_id));
    }

    #[tokio::test]
    async fn first_turn_prepend_precedes_context_and_reminder() {
        // Ordering: the FirstTurnPrepend <system> block is OUTERMOST — before
        // the stdinContext `Context:` block, role reminder, and body.
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nbody",
        );
        let (mgr, agent_id, _db) =
            manager_with(Some("implementor"), Some(dir.path().to_path_buf())).await;
        set_system_prompt(&mgr, &agent_id, "SP").await;
        let mock = intent_providers::find_provider("mock").unwrap();
        mgr.arm_first_turn_prepend(&agent_id, mock);
        let opts = TurnOptions {
            stdin_context: Some("ctx".to_string()),
            ..TurnOptions::default()
        };
        let text = prompt_text(
            &mgr.build_turn_prompt(&agent_id, &WorkspaceId::from("ws-1"), "do it", &opts)
                .await,
        );
        assert!(
            text.starts_with("<system>\nSP\n</system>\n\nContext:\nctx\n\n---\n\n[Role Reminder:"),
            "unexpected ordering: {text:?}"
        );
    }
}

#[cfg(all(test, unix))]
mod dead_child_respawn_tests {
    //! Regression tests for monorepo#764: `ensure_started` must not hand back
    //! the cached ACP session when the provider child died while the agent
    //! sat idle — it clears the stale handle and falls through to the
    //! respawn + resume path. The companion transport probe
    //! (`Connection::is_alive`) is unit-tested in `intent-acp`.

    use super::role_reminder_tests::{manager_with, session};
    use super::tests::EnvGuard;
    use super::*;

    /// Pin the mock provider's env for one test: set the fixture script path
    /// and unset the behavior knobs so a spawned child can't inherit
    /// exit-inducing behavior leaked by a concurrently guarded test (the
    /// guard also holds `ENV_LOCK`, serializing all env-mutating tests).
    fn mock_env(script: &str) -> EnvGuard {
        EnvGuard::apply(&[
            ("MOCK_AGENT_SCRIPT_PATH", Some(script)),
            ("MOCK_AGENT_BEHAVIOR", None),
            ("MOCK_AGENT_ATTEMPT_FILE", None),
        ])
    }

    /// Path to the deterministic mock ACP agent fixture (the node E2E mock),
    /// reused here so the respawn fall-through spawns a real child.
    fn mock_agent_script() -> String {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../intentd/tests/fixtures/mock-acp-agent.mjs")
            .canonicalize()
            .expect("mock-acp-agent.mjs fixture exists")
            .to_string_lossy()
            .into_owned()
    }

    /// Insert a fresh agent session on provider `mock` with a cached acp
    /// session id (the provider is immutable once set, so it must be seeded
    /// at insert time, not patched onto `manager_with`'s default agent).
    async fn seed_mock_session(mgr: &AgentManager, agent_id: &AgentId, acp: &str) {
        let mut s = session(agent_id, &WorkspaceId::from("ws-1"), None);
        s.provider = Some("mock".to_string());
        s.acp_session_id = Some(acp.to_string());
        mgr.services.store.insert_agent_session(&s).await.unwrap();
    }

    /// Install a fake duplex-backed handle (live connection, no responder).
    /// Returns the far ends, which must stay in scope for the connection's
    /// writer to stay alive. `spawned_model`/`spawned_provider` match what
    /// `resolve_spawn` yields for the mock provider so the model-change
    /// respawn branch stays cold.
    pub(super) fn install_fake_handle(
        mgr: &AgentManager,
        agent_id: &AgentId,
        child: Option<Child>,
    ) -> (tokio::io::DuplexStream, tokio::io::DuplexStream) {
        let (c2a_client, c2a_agent) = tokio::io::duplex(4096);
        let (a2c_agent, a2c_client) = tokio::io::duplex(4096);
        let connection = Arc::new(Connection::new(
            c2a_client,
            a2c_client,
            None,
            ConnectionHooks::default(),
        ));
        let (_note_tx, note_rx) = mpsc::unbounded_channel::<IncomingNotification>();
        let child_pid = child.as_ref().and_then(|c| c.id());
        let handle = AgentHandle {
            connection,
            notifications: Arc::new(TokioMutex::new(note_rx)),
            serve_task: tokio::spawn(async {}),
            _child: child,
            child_pid,
            _mcp_bridge: None,
            _mcp_config: None,
            _rules_config: None,
            _pi_extension: None,
            session_mcp_servers: Vec::new(),
            spawned_model: None,
            spawned_provider: "node".to_string(),
            thought_level: None,
            wake_gate: Arc::new(AtomicUsize::new(0)),
            wake_listener: None,
        };
        mgr.handles.lock().unwrap().insert(agent_id.clone(), handle);
        (c2a_agent, a2c_agent)
    }

    /// Live child + unchanged model → the cached session comes back with no
    /// respawn and no extra RPC (the fake connection has no responder, so any
    /// RPC would fail the turn).
    #[tokio::test]
    async fn reuses_cached_session_when_child_alive() {
        let script = mock_agent_script();
        let _env = mock_env(&script);
        let (mgr, _seeded, _db) = manager_with(None, None).await;
        let agent_id = AgentId::from("agent-764-alive");
        seed_mock_session(&mgr, &agent_id, "acp-cached").await;
        let _ends = install_fake_handle(&mgr, &agent_id, None);

        let acp = mgr
            .ensure_started(&agent_id, &WorkspaceId::from("ws-1"))
            .await
            .expect("live-child reuse path succeeds");
        assert_eq!(acp, "acp-cached", "live child reuses the cached session");
        // No respawn happened: the fake handle (no owned child) is untouched.
        let handles = mgr.handles.lock().unwrap();
        assert!(handles.get(&agent_id).unwrap()._child.is_none());
    }

    /// Handle present but the child already exited → `ensure_started` must
    /// NOT return the stale cached session: it reaps the handle and falls
    /// through to a fresh spawn (real mock child) whose session resumes /
    /// recreates.
    #[tokio::test]
    async fn respawns_when_cached_child_is_dead() {
        let script = mock_agent_script();
        let _env = mock_env(&script);
        let (mgr, _seeded, _db) = manager_with(None, None).await;
        let agent_id = AgentId::from("agent-764-dead");
        seed_mock_session(&mgr, &agent_id, "acp-stale").await;

        // A real child that has already exited: `try_wait` reports it dead
        // even though the (fake) transport writer never observed a write
        // failure — the exact died-while-idle shape from monorepo#764.
        let mut child = tokio::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .expect("spawn short-lived child");
        child.wait().await.expect("child exits");
        let _ends = install_fake_handle(&mgr, &agent_id, Some(child));

        let acp = mgr
            .ensure_started(&agent_id, &WorkspaceId::from("ws-1"))
            .await
            .expect("dead child falls through to a fresh spawn");
        // The mock agent advertises `loadSession: false`, so the respawn path
        // recreated the session — the stale cached id must NOT come back.
        assert_ne!(acp, "acp-stale", "stale session must not be reused");
        // The stale fake handle was replaced by a real spawned child.
        {
            let handles = mgr.handles.lock().unwrap();
            assert!(
                handles.get(&agent_id).unwrap()._child.is_some(),
                "respawn installed a real child-owning handle"
            );
        }
        mgr.stop(&agent_id).await;
    }

    /// `agent_root_pids` maps each handle's spawned child pid to its agent id
    /// (monorepo#2063 Phase A) and omits handles with no known pid — the
    /// descendant-tree sampler must not bucket under a root it cannot place
    /// in the process table.
    #[tokio::test]
    async fn agent_root_pids_maps_child_pids_and_skips_pidless_handles() {
        let (mgr, _seeded, _db) = manager_with(None, None).await;

        // A live child whose pid is known: it must appear in the map.
        let with_pid = AgentId::from("agent-root-pid");
        let child = tokio::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleeping child");
        let pid = child.id().expect("live child has a pid");
        let _ends_a = install_fake_handle(&mgr, &with_pid, Some(child));

        // A transport-only handle with no child process: omitted.
        let pidless = AgentId::from("agent-no-pid");
        let _ends_b = install_fake_handle(&mgr, &pidless, None);

        let roots = mgr.agent_root_pids();
        assert_eq!(roots.get(&pid), Some(&with_pid));
        assert_eq!(roots.len(), 1, "pidless handle contributes no entry");

        mgr.stop(&with_pid).await;
        mgr.stop(&pidless).await;
    }

    /// A handle whose child has already exited must drop out of the map even
    /// while the handle itself is still installed (the exit watcher removes
    /// it only on its next poll): in that window the OS can recycle the pid
    /// for an unrelated process, and mapping it would credit that stranger's
    /// subtree to the dead agent.
    #[tokio::test]
    async fn agent_root_pids_omits_handles_whose_child_was_reaped() {
        let (mgr, _seeded, _db) = manager_with(None, None).await;

        let agent_id = AgentId::from("agent-reaped-root");
        let child = tokio::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .expect("spawn short-lived child");
        assert!(child.id().is_some(), "unreaped child still reports a pid");
        let _ends = install_fake_handle(&mgr, &agent_id, Some(child));

        // The child exits on its own; once `try_wait` observes that, the
        // mapping must be gone. Poll up to a deadline — the exit is quick
        // but not synchronous with the spawn returning.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !mgr.agent_root_pids().is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "reaped child stayed in agent_root_pids"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            mgr.handles.lock().unwrap().contains_key(&agent_id),
            "only the mapping drops out; the handle awaits the exit watcher"
        );
        mgr.stop(&agent_id).await;
    }
}

#[cfg(test)]
mod v1_turn_envelope_goldens {
    //! Composed turn-envelope goldens (harness-versioning H0,
    //! intent-hq/monorepo#2459): byte-exact pins of the FULL prompt
    //! [`AgentManager::build_turn_prompt`] emits with every decoration
    //! stacked, for specialist + non-specialist and first-turn +
    //! steady-state. Complements the per-surface literals in
    //! `crate::v1_goldens`. The snapshot line carries a live timestamp, so
    //! it is validated by shape and stripped before the byte comparison.

    use super::role_reminder_tests::{manager_with, session, workspace};
    use super::*;

    /// First text block's text from a built prompt.
    fn prompt_text(prompt: &[ContentBlock]) -> String {
        serde_json::to_value(prompt).unwrap()[0]["text"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// Split off a leading snapshot line (when present): validate its shape
    /// and return the remaining prompt bytes.
    fn strip_snapshot_line(text: &str) -> String {
        let Some(rest) = text.strip_prefix("current ws.agent.snapshot() => ") else {
            return text.to_string();
        };
        let (json_line, remainder) = rest.split_once("\n\n").expect("snapshot separator");
        let v: serde_json::Value = serde_json::from_str(json_line).expect("snapshot JSON");
        assert!(v.get("time").is_some(), "snapshot always carries time");
        remainder.to_string()
    }

    /// Non-specialist steady state: ZERO decorations — the prompt is the
    /// user content, byte-identical.
    #[tokio::test]
    async fn golden_envelope_plain_steady_state() {
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        let text = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "just the message",
                &TurnOptions::default(),
            )
            .await,
        );
        assert_eq!(text, "just the message");
    }

    /// Specialist steady state: role reminder + body only.
    #[tokio::test]
    async fn golden_envelope_specialist_steady_state() {
        let dir = crate::tests::test_tempdir("intentd-envgold-");
        std::fs::write(
            dir.path().join("implementor.md"),
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nbody",
        )
        .unwrap();
        let (mgr, agent_id, _db) =
            manager_with(Some("implementor"), Some(dir.path().to_path_buf())).await;
        let text = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "fix the bug",
                &TurnOptions::default(),
            )
            .await,
        );
        assert_eq!(
            text,
            "[Role Reminder: You are a Implementor. Stay in scope.]\n\nfix the bug"
        );
    }

    /// Fully decorated first turn: FirstTurnPrepend + snapshot line +
    /// Context block + naming nudge + role reminder + body, in that exact
    /// composition order with `\n\n` joins.
    #[tokio::test]
    async fn golden_envelope_first_turn_fully_decorated() {
        let dir = crate::tests::test_tempdir("intentd-envgold-");
        std::fs::write(
            dir.path().join("implementor.md"),
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nbody",
        )
        .unwrap();
        let (mgr, _seeded, _db) =
            manager_with(Some("implementor"), Some(dir.path().to_path_buf())).await;
        // Untitled workspace + auggie-modeled agent → deterministic naming
        // nudge with the auggie tool spelling.
        let ws = WorkspaceId::from("ws-untitled");
        let mut w = workspace(&ws);
        w.title = String::new();
        mgr.services.store.insert_workspace(&w).await.unwrap();
        let agent_id = AgentId::from("agent-env");
        let mut s = session(&agent_id, &ws, Some("implementor"));
        s.model = Some("auggie:sonnet4.5".to_string());
        s.system_prompt = Some("SP body".to_string());
        mgr.services.store.insert_agent_session(&s).await.unwrap();
        // Arm the §18.1 prepend fallback (mock has no native SP mechanism)
        // and make the snapshot non-trivial (one queued message).
        let mock = intent_providers::find_provider("mock").unwrap();
        mgr.arm_first_turn_prepend(&agent_id, mock);
        mgr.services
            .enqueue_message(&agent_id, "pending".into(), None, None, None, None, false);
        let options = TurnOptions {
            stdin_context: Some("repo: demo".to_string()),
            ..Default::default()
        };
        let text = prompt_text(
            &mgr.build_turn_prompt(&agent_id, &ws, "start the task", &options)
                .await,
        );
        // Outermost: the fire-once <system> prepend.
        let rest = text
            .strip_prefix("<system>\nSP body\n</system>\n\n")
            .expect("first-turn prepend outermost");
        // Then the (live-timestamped) snapshot line.
        let rest = strip_snapshot_line(rest);
        assert_eq!(
            rest,
            "Context:\nrepo: demo\n\n---\n\n\
             <system>\n\
             This workspace needs a title. As your first action, call the \
             `set_workspace_title_workspace-mcp` tool with a short 3–5 word sentence-case \
             title describing the task. This can be called in parallel with \
             information-gathering.\n\
             </system>\n\n\
             [Role Reminder: You are a Implementor. Stay in scope.]\n\n\
             start the task"
        );
        // Steady state on the same session: prepend consumed, naming nudge
        // suppressed after an assistant reply, reminder + snapshot remain.
        mgr.services
            .store
            .append_agent_message(
                &agent_id,
                "assistant",
                &serde_json::json!([{ "type": "text", "text": "on it" }]),
                &now_iso(),
            )
            .await
            .unwrap();
        let text = prompt_text(
            &mgr.build_turn_prompt(&agent_id, &ws, "continue", &TurnOptions::default())
                .await,
        );
        let rest = strip_snapshot_line(&text);
        assert_eq!(
            rest,
            "[Role Reminder: You are a Implementor. Stay in scope.]\n\ncontinue"
        );
    }

    /// Combined interrupt delivery (monorepo#1014): the preempted text rides
    /// ahead of the interrupt content inside the same body.
    #[tokio::test]
    async fn golden_envelope_prepend_content_composition() {
        let (mgr, agent_id, _db) = manager_with(None, None).await;
        let options = TurnOptions {
            prepend_content: Some("original message".to_string()),
            ..Default::default()
        };
        let text = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "interrupt message",
                &options,
            )
            .await,
        );
        assert_eq!(text, "original message\n\ninterrupt message");
    }
}

#[cfg(test)]
mod thought_level_tests {
    //! Generic reasoning-effort application (PROTOCOL §5.5): the session's
    //! `reasoningEffort` reaches the provider through whatever
    //! `thought_level`-category config option it advertised at session open,
    //! under the adapter's own config id — no provider capability flag.

    use super::dead_child_respawn_tests::install_fake_handle;
    use super::role_reminder_tests::manager_with;
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    /// Answer every request with `{}` while recording the params of each
    /// `session/set_config_option` the daemon issued.
    fn spawn_recording_responder(
        read: tokio::io::DuplexStream,
        write: tokio::io::DuplexStream,
    ) -> (JoinHandle<()>, Arc<Mutex<Vec<Value>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = calls.clone();
        let task = tokio::spawn(async move {
            let mut lines = BufReader::new(read).lines();
            let mut write = write;
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                let (Some(id), Some(method)) =
                    (value.get("id"), value.get("method").and_then(Value::as_str))
                else {
                    continue;
                };
                if method == "session/set_config_option" {
                    recorded
                        .lock()
                        .unwrap()
                        .push(value.get("params").cloned().unwrap_or(Value::Null));
                }
                let resp = json!({ "jsonrpc": "2.0", "id": id, "result": {} });
                if write
                    .write_all(format!("{resp}\n").as_bytes())
                    .await
                    .is_err()
                {
                    return;
                }
                let _ = write.flush().await;
            }
        });
        (task, calls)
    }

    /// Install a live fake handle wired to a recording responder, seeded with
    /// `thought_level`. Returns the handle's connection plus the recorded
    /// `session/set_config_option` params.
    async fn setup(
        thought_level: Option<ThoughtLevelOption>,
    ) -> (
        AgentManager,
        AgentId,
        Arc<Connection>,
        Arc<Mutex<Vec<Value>>>,
        tempfile::TempDir,
        JoinHandle<()>,
    ) {
        let (mgr, agent_id, db) = manager_with(None, None).await;
        let (c2a_agent, a2c_agent) = install_fake_handle(&mgr, &agent_id, None);
        let (task, calls) = spawn_recording_responder(c2a_agent, a2c_agent);
        let conn = {
            let mut handles = mgr.handles.lock().unwrap();
            let handle = handles.get_mut(&agent_id).unwrap();
            handle.thought_level = thought_level;
            handle.connection.clone()
        };
        (mgr, agent_id, conn, calls, db, task)
    }

    fn option(current: &str) -> ThoughtLevelOption {
        ThoughtLevelOption {
            config_id: "effort".to_string(),
            initial_value: current.to_string(),
            current_value: current.to_string(),
            values: vec!["low".into(), "medium".into(), "high".into()],
        }
    }

    /// The stored effort is sent under the adapter's own config id, and the
    /// handle's tracked current value follows so a repeat is a no-op.
    #[tokio::test]
    async fn applies_effort_under_the_discovered_config_id() {
        let (mgr, agent_id, conn, calls, _db, _task) = setup(Some(option("medium"))).await;
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("high"))
            .await;
        let recorded = calls.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1, "one call issued: {recorded:?}");
        assert_eq!(recorded[0]["configId"], "effort");
        assert_eq!(recorded[0]["value"], "high");
        assert_eq!(recorded[0]["sessionId"], "sid-1");

        // Re-applying the same effort is a no-op (tracked value advanced).
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("high"))
            .await;
        assert_eq!(calls.lock().unwrap().len(), 1, "no redundant re-apply");
    }

    /// A mid-session change re-applies on the SAME live session — the effort
    /// takes effect by the next prompt with no respawn.
    #[tokio::test]
    async fn reapplies_after_a_mid_session_change() {
        let (mgr, agent_id, conn, calls, _db, _task) = setup(Some(option("low"))).await;
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("medium"))
            .await;
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("high"))
            .await;
        let recorded = calls.lock().unwrap().clone();
        let values: Vec<&str> = recorded
            .iter()
            .map(|c| c["value"].as_str().unwrap())
            .collect();
        assert_eq!(values, vec!["medium", "high"]);
    }

    /// The adapter is already on the stored effort → nothing is sent.
    #[tokio::test]
    async fn skips_when_the_adapter_is_already_on_that_effort() {
        let (mgr, agent_id, conn, calls, _db, _task) = setup(Some(option("high"))).await;
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("high"))
            .await;
        assert!(calls.lock().unwrap().is_empty());
    }

    /// A provider that advertised no `thought_level` option silently ignores
    /// the session's `reasoningEffort`.
    #[tokio::test]
    async fn absent_option_is_a_silent_no_op() {
        let (mgr, agent_id, conn, calls, _db, _task) = setup(None).await;
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("high"))
            .await;
        assert!(calls.lock().unwrap().is_empty());
    }

    /// An effort outside the select's vocabulary (e.g. a codex `xhigh` on a
    /// claude session) is never sent — the adapter would reject it.
    #[tokio::test]
    async fn unknown_effort_value_is_not_sent() {
        let (mgr, agent_id, conn, calls, _db, _task) = setup(Some(option("medium"))).await;
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("xhigh"))
            .await;
        assert!(calls.lock().unwrap().is_empty());
    }

    /// The stored level keeps the caller's spelling, but the ADAPTER's own
    /// spelling is what reaches `session/set_config_option` — otherwise a
    /// validated `"HIGH"` would never be applied to a `["low","high"]` select.
    #[tokio::test]
    async fn caller_spelling_is_matched_case_insensitively() {
        let (mgr, agent_id, conn, calls, _db, _task) = setup(Some(option("medium"))).await;
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("HIGH"))
            .await;
        let recorded = calls.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1, "one call issued: {recorded:?}");
        assert_eq!(recorded[0]["value"], "high", "adapter's own spelling sent");

        // The tracked value advanced, so a differently-cased repeat is a no-op.
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("High"))
            .await;
        assert_eq!(calls.lock().unwrap().len(), 1, "no redundant re-apply");
    }

    /// Clearing the effort restores the provider's own opening default on the
    /// live session, rather than leaving the last applied level in place.
    #[tokio::test]
    async fn clearing_the_effort_restores_the_provider_default() {
        let (mgr, agent_id, conn, calls, _db, _task) = setup(Some(option("medium"))).await;
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("high"))
            .await;
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", None)
            .await;
        let values: Vec<String> = calls
            .lock()
            .unwrap()
            .iter()
            .map(|c| c["value"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(values, vec!["high", "medium"]);

        // Already back on the default → a second clear sends nothing.
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("   "))
            .await;
        assert_eq!(calls.lock().unwrap().len(), 2, "no redundant restore");
    }

    /// No stored effort (or a blank one) while the adapter is still on its
    /// opening default is a no-op — nothing to restore.
    #[tokio::test]
    async fn absent_or_blank_stored_effort_is_a_no_op() {
        let (mgr, agent_id, conn, calls, _db, _task) = setup(Some(option("medium"))).await;
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", None)
            .await;
        mgr.apply_thought_level(conn.as_ref(), &agent_id, "sid-1", Some("  "))
            .await;
        assert!(calls.lock().unwrap().is_empty());
    }
}

#[cfg(test)]
mod rebuild_spawn_opts_tests {
    //! Regression tests for the `create_agent` [`SpawnOptions`] reconstruction:
    //! it must preserve the npx fallback pair, otherwise providers without a
    //! local binary (codex fallback / claude-code npx-only) spawn the bare
    //! provider command and fail with ENOENT.

    use super::*;

    #[test]
    fn rebuild_preserves_npx_fallback_and_targets_npx() {
        let provider = intent_providers::find_provider("codex").unwrap();
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        let mut opts = SpawnOptions::new(provider);
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.fallback_npx_package;

        let rebuilt = rebuild_spawn_opts(&opts, Some("/tmp/rules.md"), Some("/tmp/mcp.json"), None);
        assert_eq!(rebuilt.npx_fallback_binary, Some(npx_path.as_path()));
        assert_eq!(rebuilt.npx_fallback_package, provider.fallback_npx_package);

        // Through build_command/build_args: the rebuilt opts must spawn npx
        // with `-y <package>`, not the bare `codex-acp` command.
        let cmd = intent_acp::spawn::build_command(&rebuilt);
        assert_eq!(cmd.as_std().get_program(), npx_path.as_os_str());
        let args = intent_acp::spawn::build_args(&rebuilt);
        assert_eq!(args[0], "-y");
        assert_eq!(
            args[1],
            provider
                .fallback_npx_package
                .expect("codex has npx fallback")
        );
    }

    #[test]
    fn rebuild_injects_generated_paths_and_keeps_caller_fields() {
        let provider = intent_providers::find_provider("codex").unwrap();
        let binary = PathBuf::from("/custom/codex-acp");
        let cwd = PathBuf::from("/work/dir");
        let mut opts = SpawnOptions::new(provider);
        opts.model = Some("gpt-5");
        opts.cwd = Some(&cwd);
        opts.quiet = true;
        opts.provider_binary = Some(&binary);
        opts.extra_env = BTreeMap::from([("K".to_string(), "V".to_string())]);
        opts.tools_to_remove = vec!["shell"];

        let rebuilt = rebuild_spawn_opts(&opts, Some("/tmp/rules.md"), Some("/tmp/mcp.json"), None);
        assert_eq!(rebuilt.model, Some("gpt-5"));
        assert_eq!(rebuilt.cwd, Some(cwd.as_path()));
        assert!(rebuilt.quiet);
        assert_eq!(rebuilt.provider_binary, Some(binary.as_path()));
        assert_eq!(rebuilt.extra_env, opts.extra_env);
        assert_eq!(rebuilt.tools_to_remove, vec!["shell"]);
        assert_eq!(rebuilt.rules_file, Some("/tmp/rules.md"));
        assert_eq!(rebuilt.mcp_config_file, Some("/tmp/mcp.json"));
        assert_eq!(rebuilt.env_mcp_config, None);
    }

    #[test]
    fn rebuild_prefers_caller_rules_file_over_generated() {
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        opts.rules_file = Some("/caller/rules.md");
        let rebuilt = rebuild_spawn_opts(&opts, Some("/tmp/generated.md"), None, None);
        assert_eq!(rebuilt.rules_file, Some("/caller/rules.md"));
    }

    /// monorepo#884 Phase 2.2 on/off matrix for the provider-spawn credential
    /// seam: setting on adds exactly the single `GIT_CONFIG_PARAMETERS` pair
    /// naming the daemon-backed helper — no `INTENT_GIT_GITHUB_TOKEN` and
    /// never raw `GITHUB_TOKEN`/`GH_TOKEN` (no token bytes in the child env);
    /// setting off leaves the env untouched.
    #[test]
    fn inject_git_credential_env_on_off_matrix() {
        let mut env = BTreeMap::new();
        inject_git_credential_env(&mut env, true);
        assert_eq!(
            env.keys().map(String::as_str).collect::<Vec<_>>(),
            vec![intent_git::auth::GIT_CONFIG_PARAMETERS_ENV]
        );
        assert!(
            !env.contains_key(intent_git::auth::TOKEN_ENV),
            "no token env pair may be injected"
        );
        let params = &env[intent_git::auth::GIT_CONFIG_PARAMETERS_ENV];
        assert!(
            params.contains("credential.https://github.com.helper=")
                && params.contains("git-credential"),
            "daemon-backed helper entry present: {params}"
        );

        let mut env = BTreeMap::new();
        inject_git_credential_env(&mut env, false);
        assert!(env.is_empty(), "setting off must not inject");
    }

    /// Pre-existing caller-set extra_env keys survive the injection — the
    /// helper only fills vacant slots, never clobbers.
    #[test]
    fn inject_git_credential_env_preserves_existing_keys() {
        let mut env = BTreeMap::from([(
            intent_git::auth::GIT_CONFIG_PARAMETERS_ENV.to_string(),
            "caller-set".to_string(),
        )]);
        inject_git_credential_env(&mut env, true);
        assert_eq!(
            env[intent_git::auth::GIT_CONFIG_PARAMETERS_ENV],
            "caller-set"
        );
    }
}

#[cfg(all(test, unix))]
mod pi_extension_delivery_tests {
    //! Unit tests for the pi-extension MCP delivery spawn assembly: the two
    //! per-agent temp files (bundled extension + 0755 wrapper), the two spawn
    //! env vars, and the capability gate that leaves non-pi providers alone.
    //! Unix-only, matching the delivery itself (sh wrapper + chmod).

    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn write_creates_extension_and_executable_wrapper() {
        let delivery = PiExtensionDelivery::write("pi", &std::env::temp_dir()).unwrap();

        let ext = std::fs::read_to_string(&delivery._extension.path).unwrap();
        assert_eq!(ext, PI_MCP_EXTENSION_SOURCE);
        assert!(
            !ext.trim().is_empty(),
            "embedded extension must not be empty"
        );

        let mode = std::fs::metadata(&delivery.wrapper.path)
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o755, "wrapper must be executable (0755)");
        let script = std::fs::read_to_string(&delivery.wrapper.path).unwrap();
        assert!(script.starts_with("#!/bin/sh\n"));
        assert!(
            script.contains(&format!(
                "exec 'pi' -e '{}' \"$@\"",
                delivery._extension.path.display()
            )),
            "wrapper must exec the real pi with -e <extension>: {script:?}"
        );
    }

    #[test]
    fn wrapper_single_quotes_special_characters() {
        let delivery =
            PiExtensionDelivery::write("/opt/pi's \"odd$\" bin/pi", &std::env::temp_dir()).unwrap();
        let script = std::fs::read_to_string(&delivery.wrapper.path).unwrap();
        assert!(
            script.contains("exec '/opt/pi'\\''s \"odd$\" bin/pi' -e '"),
            "wrapper must single-quote the pi command with the '\\'' escape: {script:?}"
        );
    }

    #[test]
    fn write_places_files_in_the_given_dir() {
        let dir = std::env::temp_dir().join(format!("intentd-pi-dir-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let delivery = PiExtensionDelivery::write("pi", &dir).unwrap();
        assert_eq!(delivery._extension.path.parent().unwrap(), dir);
        assert_eq!(delivery.wrapper.path.parent().unwrap(), dir);
        drop(delivery);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn temp_files_removed_when_delivery_drops() {
        let delivery = PiExtensionDelivery::write("pi", &std::env::temp_dir()).unwrap();
        let ext = delivery._extension.path.clone();
        let wrapper = delivery.wrapper.path.clone();
        drop(delivery);
        assert!(!ext.exists());
        assert!(!wrapper.exists());
    }

    #[test]
    fn apply_spawn_env_sets_wrapper_and_bridge_addr() {
        let delivery = PiExtensionDelivery::write("pi", &std::env::temp_dir()).unwrap();
        let mut extra_env = BTreeMap::new();
        delivery.apply_spawn_env(&mut extra_env, "127.0.0.1:9999".to_string());
        assert_eq!(
            extra_env.get(PI_ACP_PI_COMMAND_ENV),
            Some(&delivery.wrapper.path.to_string_lossy().into_owned())
        );
        assert_eq!(
            extra_env
                .get(INTENTD_MCP_BRIDGE_ADDR_ENV)
                .map(String::as_str),
            Some("127.0.0.1:9999")
        );
    }

    #[test]
    fn delivery_gated_on_capability_flag() {
        let pi = intent_providers::find_provider("pi").unwrap();
        let delivery = pi_extension_delivery(pi, &std::env::temp_dir()).unwrap();
        assert!(delivery.is_some(), "pi must get the extension delivery");

        for provider in intent_providers::ACP_PROVIDERS
            .iter()
            .filter(|p| p.id != "pi")
        {
            assert!(
                pi_extension_delivery(provider, &std::env::temp_dir())
                    .unwrap()
                    .is_none(),
                "{} must not get a wrapper or extension",
                provider.id
            );
        }
    }
}

#[cfg(all(test, unix))]
mod provider_path_override_tests {
    //! Regression tests for the `providers.paths` key retarget: unsloth rides
    //! the opencode binary as its ACP runtime, so its primary spawn resolution
    //! must honor `providers.paths["opencode"]` — `providers.paths["unsloth"]`
    //! targets the `unsloth` CLI (the managed-server lifecycle,
    //! `unsloth_server.rs`), never the ACP spawn binary.

    use super::role_reminder_tests::session;
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// Write an executable stub so the explicit-path tier of
    /// `find_provider_binary` accepts it (absolute + executable).
    fn exec_stub(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn settings_with_paths(paths: &[(&str, &Path)]) -> intent_core::settings_file::SettingsFile {
        let mut settings = intent_core::settings_file::SettingsFile::default();
        for (id, path) in paths {
            settings
                .providers
                .paths
                .insert((*id).to_string(), path.to_string_lossy().into_owned());
        }
        settings
    }

    fn unsloth_session() -> AgentSession {
        let mut s = session(
            &AgentId::from("agent-paths-unsloth"),
            &WorkspaceId::from("ws-1"),
            None,
        );
        s.provider = Some("unsloth".to_string());
        s
    }

    /// The unsloth primary (opencode) spawn resolution reads the `opencode`
    /// key: with both keys set to distinct stubs, the resolved binary must be
    /// the `opencode` one.
    #[test]
    fn unsloth_primary_spawn_honors_opencode_path_override() {
        let dir = tempfile::tempdir().unwrap();
        let opencode_stub = exec_stub(dir.path(), "opencode-override");
        let unsloth_stub = exec_stub(dir.path(), "unsloth-override");
        let settings =
            settings_with_paths(&[("opencode", &opencode_stub), ("unsloth", &unsloth_stub)]);

        let resolved = resolve_spawn(&unsloth_session(), None, &settings, None).unwrap();
        assert_eq!(
            resolved.provider_binary.as_deref(),
            Some(opencode_stub.as_path()),
            "unsloth's opencode primary must resolve via providers.paths[\"opencode\"]"
        );
    }

    /// `providers.paths["unsloth"]` alone must NOT redirect the opencode
    /// primary — it targets the unsloth CLI (managed server), not the ACP
    /// spawn binary.
    #[test]
    fn unsloth_key_does_not_redirect_the_opencode_primary() {
        let dir = tempfile::tempdir().unwrap();
        let unsloth_stub = exec_stub(dir.path(), "unsloth-override");
        let settings = settings_with_paths(&[("unsloth", &unsloth_stub)]);

        let resolved = resolve_spawn(&unsloth_session(), None, &settings, None).unwrap();
        assert_ne!(
            resolved.provider_binary.as_deref(),
            Some(unsloth_stub.as_path()),
            "providers.paths[\"unsloth\"] must not override the opencode primary"
        );
    }

    /// Sanity: a provider that owns its primary binary (opencode itself)
    /// still honors its own key — the retarget only affects unsloth.
    #[test]
    fn opencode_provider_still_honors_its_own_key() {
        let dir = tempfile::tempdir().unwrap();
        let opencode_stub = exec_stub(dir.path(), "opencode-override");
        let settings = settings_with_paths(&[("opencode", &opencode_stub)]);

        let mut s = session(
            &AgentId::from("agent-paths-opencode"),
            &WorkspaceId::from("ws-1"),
            None,
        );
        s.provider = Some("opencode".to_string());

        let resolved = resolve_spawn(&s, None, &settings, None).unwrap();
        assert_eq!(
            resolved.provider_binary.as_deref(),
            Some(opencode_stub.as_path())
        );
    }
}

#[cfg(test)]
mod retry_tests {
    //! Unit tests for spawn retry logic (classify retryable errors, tear down
    //! failed child between attempts, emit terminal failure events).

    use super::*;

    #[test]
    fn session_new_timeout_is_retryable() {
        let err =
            Error::Internal("session/new failed: request `session/new` timed out".to_string());
        assert!(is_retryable_spawn_error(&err));
    }

    #[test]
    fn session_load_timeout_is_retryable() {
        let err = Error::Internal("session/load failed: timed out after 60s".to_string());
        assert!(is_retryable_spawn_error(&err));
    }

    #[test]
    fn handshake_agent_stdout_closed_is_retryable() {
        let err =
            Error::Internal("handshake failed: JSON-RPC error 0: agent stdout closed".to_string());
        assert!(is_retryable_spawn_error(&err));
    }

    #[test]
    fn not_found_is_not_retryable() {
        let err = Error::NotFound("agent session not found".to_string());
        assert!(!is_retryable_spawn_error(&err));
    }

    #[test]
    fn provider_missing_is_not_retryable() {
        let err =
            Error::Internal("provider auggie missing required env ANTHROPIC_API_KEY".to_string());
        assert!(!is_retryable_spawn_error(&err));
    }

    #[test]
    fn generic_internal_error_is_not_retryable() {
        // Default changed to non-retryable for unknown Internal errors to avoid
        // masking bugs — only explicitly-known transient failures are retried.
        let err = Error::Internal("transport error: connection reset".to_string());
        assert!(!is_retryable_spawn_error(&err));
    }

    #[test]
    fn invalid_params_is_not_retryable() {
        let err = Error::InvalidParams("missing required parameter".to_string());
        assert!(!is_retryable_spawn_error(&err));
    }

    #[test]
    fn conflict_is_not_retryable() {
        let err = Error::Conflict {
            current: serde_json::json!({"rev": 2}),
        };
        assert!(!is_retryable_spawn_error(&err));
    }
}

#[cfg(test)]
mod turn_failure_tests {
    //! Unit tests for the mid-turn failure classifier (benign cancel vs
    //! terminal failure) and the events-already-emitted marker.

    use super::*;

    #[test]
    fn not_found_is_benign() {
        // Handle disappeared mid-worker → a concurrent stop/teardown won.
        let err = Error::NotFound("agent agent-x".to_string());
        assert!(is_benign_turn_error(&err));
    }

    #[test]
    fn cancelled_rpc_error_is_benign() {
        let err = Error::Internal(
            "session/prompt failed: JSON-RPC error -32800: Request cancelled".to_string(),
        );
        assert!(is_benign_turn_error(&err));
    }

    #[test]
    fn cancellation_code_without_message_is_benign() {
        // Some providers omit a human-readable message; the -32800 code alone
        // is the cancellation signal.
        let err = Error::Internal("session/prompt failed: JSON-RPC error -32800: ".to_string());
        assert!(is_benign_turn_error(&err));
    }

    #[test]
    fn cancelled_rpc_error_with_data_detail_is_benign() {
        // `JsonRpcError` Display now appends the `data` payload (strings raw,
        // objects as compact JSON); the richer message must not break the
        // -32800 benign-cancel classification.
        let err = Error::Internal(
            "session/prompt failed: JSON-RPC error -32800: Request cancelled: turn aborted"
                .to_string(),
        );
        assert!(is_benign_turn_error(&err));
        let err = Error::Internal(
            r#"session/prompt failed: JSON-RPC error -32800: Request cancelled: {"reason":"turn aborted"}"#
                .to_string(),
        );
        assert!(is_benign_turn_error(&err));
    }

    #[test]
    fn terminal_rpc_error_with_cancelled_in_data_is_terminal() {
        // monorepo#518 inverse case: a terminal provider error whose appended
        // `data` detail merely mentions "cancelled" must NOT be misclassified
        // as a benign cancel — it needs the full agent:failed / requeue /
        // Retry surface.
        let err = Error::Internal(
            r#"session/prompt failed: JSON-RPC error -32603: Internal error: {"message":"stream cancelled by backend","codex_error_info":"other"}"#
                .to_string(),
        );
        assert!(!is_benign_turn_error(&err));
        let err = Error::Internal(
            "session/prompt failed: JSON-RPC error -32603: Internal error: request cancelled upstream"
                .to_string(),
        );
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn rpc_error_with_cancelled_message_but_non_cancel_code_is_terminal() {
        // RPC-shaped errors anchor on the -32800 code alone: past the code,
        // the rendered message/data suffix is provider-controlled free text
        // (message and data are indistinguishable once flattened), so a
        // "cancelled" mention there errs toward terminal by design.
        let err = Error::Internal(
            "session/prompt failed: JSON-RPC error -32603: task cancelled".to_string(),
        );
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn non_rpc_cancelled_message_is_benign() {
        // The second known cancel shape: a provider resolving the prompt with
        // a plain "cancelled" error message. Non-RPC renderings carry no
        // provider-controlled data suffix, so the substring match is safe.
        let err = Error::Internal("session/prompt failed: prompt was cancelled".to_string());
        assert!(is_benign_turn_error(&err));
    }

    #[test]
    fn cancelled_outside_prompt_wrapper_is_terminal() {
        // "cancelled" in an unrelated error (no session/prompt wrapper) must
        // not be mistaken for a benign turn cancel.
        let err = Error::Internal("store: write cancelled by shutdown".to_string());
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn transport_closed_is_terminal() {
        let err = Error::Internal(
            "session/prompt failed: transport closed: writer task closed".to_string(),
        );
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn stdout_closed_is_terminal() {
        let err = Error::Internal(
            "session/prompt failed: JSON-RPC error 0: agent stdout closed".to_string(),
        );
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn prompt_timeout_is_terminal() {
        let err = Error::Internal(
            "session/prompt failed: request `session/prompt` timed out".to_string(),
        );
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn idle_timeout_marker_is_detected() {
        // The structured AcpError::PromptIdleTimeout flattened at the wrap
        // boundary: `session/prompt failed: ` + the Display rendering pinned
        // to intent_acp::PROMPT_IDLE_TIMEOUT_PREFIX.
        let err = Error::Internal(
            "session/prompt failed: session/prompt idle timeout (1800s of silence)".to_string(),
        );
        assert!(prompt_idle_timeout_error(&err));
        // Compose from the exported consts so the contract cannot drift.
        let err = Error::Internal(format!(
            "{PROMPT_FAILED_PREFIX} {}",
            intent_acp::AcpError::PromptIdleTimeout(std::time::Duration::from_secs(1800))
        ));
        assert!(prompt_idle_timeout_error(&err));
    }

    #[test]
    fn non_idle_timeouts_are_not_idle_marked() {
        // The 24h fallback timeout renders via AcpError::Timeout — a plain
        // request timeout, NOT the idle marker.
        let err = Error::Internal(
            "session/prompt failed: request `session/prompt` timed out".to_string(),
        );
        assert!(!prompt_idle_timeout_error(&err));
        // Mid-string mentions outside the prompt wrapper are inert.
        let err =
            Error::Internal("store: session/prompt idle timeout (1800s of silence)".to_string());
        assert!(!prompt_idle_timeout_error(&err));
        // The idle marker inside a different wrapper is inert too.
        let err = Error::Internal(
            "session/prompt transport closed before output: session/prompt idle timeout (1800s of silence)"
                .to_string(),
        );
        assert!(!prompt_idle_timeout_error(&err));
        // Non-Internal errors never classify.
        let err = Error::NotFound("agent agent-x".to_string());
        assert!(!prompt_idle_timeout_error(&err));
    }

    #[test]
    fn idle_timeout_is_terminal_and_events_already_emitted() {
        // The idle timeout is not benign (the worker's warn-and-continue arm
        // classifies it FIRST via `prompt_idle_timeout_error`); when the
        // consecutive cap routes it to the terminal path,
        // `handle_terminal_turn_failure` must not re-emit the stream:end —
        // run_prompt_turn already emitted the normal one (the worker emits
        // the suppressed agent:failed half itself).
        let err = Error::Internal(
            "session/prompt failed: session/prompt idle timeout (1800s of silence)".to_string(),
        );
        assert!(!is_benign_turn_error(&err));
        assert!(turn_failure_events_already_emitted(&err));
    }

    #[test]
    fn idle_timeout_streamed_suffix_is_detected() {
        // The streamed-output marker rides INSIDE the ordinary wrapper: the
        // error still classifies as an idle timeout AND reports streamed
        // activity (the worker restarts its consecutive counter at 1).
        let err = Error::Internal(format!(
            "session/prompt failed: session/prompt idle timeout (1800s of silence) {}",
            crate::agent_session::PROMPT_IDLE_TIMEOUT_STREAMED_SUFFIX
        ));
        assert!(prompt_idle_timeout_error(&err));
        assert!(idle_timeout_turn_streamed(&err));
        // A bare (silent) idle timeout carries no marker.
        let err = Error::Internal(
            "session/prompt failed: session/prompt idle timeout (1800s of silence)".to_string(),
        );
        assert!(!idle_timeout_turn_streamed(&err));
    }

    #[test]
    fn idle_timeout_warning_text_includes_configured_window() {
        let text = idle_timeout_warning_text(std::time::Duration::from_secs(1800));
        assert_eq!(
            text,
            "[SYSTEM WARNING] Your turn exceeded the inactivity timeout (1800s of silence) and \
             was interrupted. If you were waiting on something external, schedule a \
             `ws.hook.schedule` background hook to watch the condition and end your turn \
             instead of blocking — the hook's wake message resumes you. Assess where you left \
             off and continue the work."
        );
        // The window is the actual configured value, not a hardcoded literal,
        // and sub-second precision is preserved rather than truncated.
        let text = idle_timeout_warning_text(std::time::Duration::from_millis(2500));
        assert!(text.contains("(2.5s of silence)"), "{text}");
        let text = idle_timeout_warning_text(std::time::Duration::from_millis(500));
        assert!(text.contains("(0.5s of silence)"), "{text}");
    }

    #[test]
    fn store_append_failure_is_terminal() {
        let err = Error::Internal("store: database is locked".to_string());
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn prompt_failed_marker_means_events_already_emitted() {
        // run_prompt_turn emits agent:failed + stream:end BEFORE wrapping the
        // error with the "session/prompt failed" prefix.
        let err = Error::Internal(
            "session/prompt failed: transport closed: writer task closed".to_string(),
        );
        assert!(turn_failure_events_already_emitted(&err));
    }

    #[test]
    fn store_error_needs_events_emitted() {
        // The transcript-append store error propagates via `?` before
        // run_prompt_turn reaches its emit path.
        let err = Error::Internal("store: database is locked".to_string());
        assert!(!turn_failure_events_already_emitted(&err));
    }

    #[test]
    fn mid_string_marker_needs_events_emitted() {
        // Prefix-anchored: an unrelated error merely mentioning the phrase
        // mid-string must not suppress the terminal event pair.
        let err =
            Error::Internal("store: could not log that session/prompt failed earlier".to_string());
        assert!(!turn_failure_events_already_emitted(&err));
    }

    #[test]
    fn pre_output_marker_is_detected() {
        // monorepo#764: the marker run_prompt_turn wraps a transport failure
        // with when the turn provably streamed nothing.
        let err = Error::Internal(
            "session/prompt transport closed before output: transport closed: writer task closed"
                .to_string(),
        );
        assert!(pre_output_transport_failure(&err));
        let err = Error::Internal(
            "session/prompt transport closed before output: JSON-RPC error 0: agent stdout closed"
                .to_string(),
        );
        assert!(pre_output_transport_failure(&err));
    }

    #[test]
    fn pre_output_marker_needs_events_emitted() {
        // run_prompt_turn SUPPRESSED the terminal pair for the marker — when
        // the one-retry budget is spent, handle_terminal_turn_failure must
        // emit it (the marker is not the PROMPT_FAILED_PREFIX).
        let err = Error::Internal(
            "session/prompt transport closed before output: transport closed: writer task closed"
                .to_string(),
        );
        assert!(!turn_failure_events_already_emitted(&err));
    }

    #[test]
    fn ordinary_failures_are_not_pre_output_marked() {
        // Post-output / non-transport failures carry the ordinary wrapper and
        // must not trigger the silent redrive; mid-string mentions are inert.
        let err = Error::Internal(
            "session/prompt failed: transport closed: writer task closed".to_string(),
        );
        assert!(!pre_output_transport_failure(&err));
        let err = Error::Internal(
            "store: session/prompt transport closed before output: logged".to_string(),
        );
        assert!(!pre_output_transport_failure(&err));
    }
}

#[cfg(test)]
mod cancel_and_settle_tests {
    //! Unit tests for [`cancel_and_settle_idle_prompt`] over a duplex-backed
    //! `Connection` (no real child): delivered vs transport-closed outcomes.

    use super::*;
    use tokio::io::{AsyncBufReadExt, BufReader};

    /// Live transport → the `session/cancel` notification is written to the
    /// child's stdin and the helper reports it delivered.
    #[tokio::test]
    async fn delivers_cancel_on_live_transport() {
        let (c2a_client, c2a_agent) = tokio::io::duplex(4096);
        let (_a2c_agent, a2c_client) = tokio::io::duplex(4096);
        let conn = Connection::new(c2a_client, a2c_client, None, ConnectionHooks::default());
        let agent_id = AgentId::from("agent-idle-settle");

        assert!(cancel_and_settle_idle_prompt(&conn, &agent_id, "acp-1").await);

        // The cancel frame reached the agent side of the pipe.
        let mut lines = BufReader::new(c2a_agent).lines();
        let line = tokio::time::timeout(Duration::from_secs(2), lines.next_line())
            .await
            .expect("frame arrives")
            .expect("read ok")
            .expect("one frame");
        let frame: serde_json::Value = serde_json::from_str(&line).expect("valid JSON-RPC frame");
        assert_eq!(frame["method"], "session/cancel");
        assert_eq!(frame["params"]["sessionId"], "acp-1");
        assert!(frame.get("id").is_none(), "cancel is a notification");
    }

    /// Dead child (writer task exited on the broken pipe) → the helper
    /// tolerates the transport-closed error and reports not delivered.
    #[tokio::test]
    async fn tolerates_transport_closed() {
        let (c2a_client, c2a_agent) = tokio::io::duplex(4096);
        let (_a2c_agent, a2c_client) = tokio::io::duplex(4096);
        let conn = Connection::new(c2a_client, a2c_client, None, ConnectionHooks::default());
        let agent_id = AgentId::from("agent-idle-settle-dead");

        // Kill the writer: drop the child end of stdin, then poke the writer
        // so it hits the broken pipe and exits, closing the channel.
        drop(c2a_agent);
        let _ = conn.notify("session/ping", serde_json::json!({})).await;
        for _ in 0..200 {
            if !conn.is_alive() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!conn.is_alive(), "writer task exited on broken pipe");

        assert!(!cancel_and_settle_idle_prompt(&conn, &agent_id, "acp-1").await);
    }
}

#[cfg(test)]
mod agent_retry_tests {
    //! Unit tests for agent.retry RPC (retry a failed agent spawn).

    use super::*;
    use crate::events::EventBus;
    use crate::BusEventSink;
    use intent_core::{
        AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
        WorkspaceStatus,
    };
    use intent_store::Store;
    use std::sync::Arc;

    fn workspace(id: &WorkspaceId) -> Workspace {
        let ts = now_iso();
        Workspace {
            id: id.clone(),
            title: "Test WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            pull_requests: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            archived: false,
            archived_at: None,
            active_pull_request: None,
            pr_number: None,
            pr_status: None,
            pr_url: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
            task_stats: None,
        }
    }

    fn session(agent_id: &AgentId, ws: &WorkspaceId, status: AgentStatus) -> AgentSession {
        let ts = now_iso();
        AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: Some("session-1".to_string()),
            name: "Agent".to_string(),
            name_explicitly_set: false,
            model: Some("model-1".to_string()),
            reasoning_effort: None,
            effort_levels: None,
            provider: Some("provider-1".to_string()),
            system_prompt: None,
            specialist: None,
            status,
            is_active: false,
            messages: Vec::new(),
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
            created_at: ts.clone(),
            updated_at: ts,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        }
    }

    /// The returned RAII guard owns the db dir (db + `-wal`/`-shm` sidecars);
    /// keep it alive for the duration of the test.
    async fn manager_with_session(
        agent_id: &AgentId,
        ws: &WorkspaceId,
        status: AgentStatus,
    ) -> (Arc<AgentManager>, tempfile::TempDir) {
        let db_dir = crate::tests::test_tempdir("intentd-retry-");
        let path = db_dir.path().join("store.db");
        let db = Store::open(&path).await.expect("temp store");
        db.insert_workspace(&workspace(ws))
            .await
            .expect("insert workspace");
        db.insert_agent_session(&session(agent_id, ws, status))
            .await
            .expect("insert session");
        let bus = EventBus::new(db.clone());
        let services = Services::new(db).with_event_bus(bus.clone());
        let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus));
        (Arc::new(AgentManager::new(services, sink, 8)), db_dir)
    }

    #[tokio::test]
    async fn retry_from_error_status_with_empty_queue_clears_to_idle() {
        let agent_id = AgentId::from("agent-1");
        let ws = WorkspaceId::from("ws-1");
        let (mgr, _db) = manager_with_session(&agent_id, &ws, AgentStatus::Error).await;

        let result = mgr
            .agent_retry(agent_id.clone(), ws.clone())
            .await
            .expect("retry");
        assert_eq!(result["ok"], true);
        // Nothing queued → nothing redriven; the client is told explicitly
        // (STAB-54: an empty-queue retry must not be a silent no-op).
        assert_eq!(result["redriven"], false);

        // Status should be cleared to Idle — a `pending` status would park the
        // agent forever since no queued message will ever drive it forward.
        let session = mgr
            .services
            .store
            .get_agent_session(&agent_id)
            .await
            .expect("session");
        assert_eq!(session.status, AgentStatus::RuntimeIdle);
    }

    #[tokio::test]
    async fn retry_from_error_status_with_queued_message_redrives() {
        let agent_id = AgentId::from("agent-redrive");
        let ws = WorkspaceId::from("ws-redrive");
        let (mgr, _db) = manager_with_session(&agent_id, &ws, AgentStatus::Error).await;

        // A requeued message is waiting (the persist_error_and_requeue path).
        mgr.services.enqueue_message(
            &agent_id,
            "requeued".to_string(),
            None,
            None,
            None,
            None,
            false,
        );

        let result = mgr
            .agent_retry(agent_id.clone(), ws.clone())
            .await
            .expect("retry");
        assert_eq!(result["ok"], true);
        assert_eq!(result["redriven"], true);

        // The drain loop claimed the queued message (dequeued for redrive).
        assert!(
            !mgr.services.has_ready_to_send(&agent_id),
            "queued message dequeued for redrive"
        );
    }

    /// Regression for the check-then-flip race in `agent_retry`: a message
    /// enqueued while the session is still `Error` has its own drain kick
    /// suppressed (STAB-52 gate), so if it lands after retry's initial queue
    /// check the post-flip re-check must promote Idle → Pending and drain it.
    /// Sweeps interleavings by varying the number of yields before the
    /// concurrent enqueue; whatever the timing, the raced message must never
    /// be stranded ready-to-send on an `Idle` session.
    #[tokio::test]
    async fn retry_racing_concurrent_enqueue_never_strands_message() {
        for yields in 0..8u32 {
            let id = format!("agent-race-{yields}");
            let agent_id = AgentId::from(id.as_str());
            let ws = WorkspaceId::from("ws-race");
            let (mgr, _db) = manager_with_session(&agent_id, &ws, AgentStatus::Error).await;

            let retry_fut = mgr.agent_retry(agent_id.clone(), ws.clone());
            let enqueue_fut = async {
                for _ in 0..yields {
                    tokio::task::yield_now().await;
                }
                mgr.services.enqueue_message(
                    &agent_id,
                    "raced".to_string(),
                    None,
                    None,
                    None,
                    None,
                    false,
                );
                mgr.clone()
                    .try_drain_queue(agent_id.clone(), ws.clone())
                    .await;
            };
            let (retry_result, ()) = tokio::join!(retry_fut, enqueue_fut);
            let result = retry_result.expect("retry");
            assert_eq!(result["ok"], true);

            // Whichever side won the race, the message must have been claimed
            // by a drain: either retry's post-flip re-check redrove it, or the
            // enqueue's own drain kick ran after the Error gate lifted. An
            // `Idle` session with a ready-to-send message means it was
            // stranded — the exact bug the re-check closes.
            let session = mgr
                .services
                .store
                .get_agent_session(&agent_id)
                .await
                .expect("session");
            assert!(
                !(session.status == AgentStatus::RuntimeIdle
                    && mgr.services.has_ready_to_send(&agent_id)),
                "raced message stranded on idle session (yields={yields})"
            );
        }
    }

    #[tokio::test]
    async fn retry_from_pending_status_returns_ok_false() {
        let agent_id = AgentId::from("agent-2");
        let ws = WorkspaceId::from("ws-2");
        let (mgr, _db) = manager_with_session(&agent_id, &ws, AgentStatus::Pending).await;

        let result = mgr
            .agent_retry(agent_id.clone(), ws.clone())
            .await
            .expect("retry");
        assert_eq!(result["ok"], false);

        // Status should remain Pending
        let session = mgr
            .services
            .store
            .get_agent_session(&agent_id)
            .await
            .expect("session");
        assert_eq!(session.status, AgentStatus::Pending);
    }

    #[tokio::test]
    async fn retry_from_active_status_returns_ok_false() {
        let agent_id = AgentId::from("agent-3");
        let ws = WorkspaceId::from("ws-3");
        let (mgr, _db) = manager_with_session(&agent_id, &ws, AgentStatus::Active).await;

        let result = mgr
            .agent_retry(agent_id.clone(), ws.clone())
            .await
            .expect("retry");
        assert_eq!(result["ok"], false);

        // Status should remain Active
        let session = mgr
            .services
            .store
            .get_agent_session(&agent_id)
            .await
            .expect("session");
        assert_eq!(session.status, AgentStatus::Active);
    }
}
