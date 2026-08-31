//! Event-type taxonomy (§10; ported from `~/src/intent/src/features/events/types.ts`
//! `WorkspaceEventType`).
//!
//! These are the canonical `Event::event_type` strings carried on the wire as
//! `type`. They must match the TS values **exactly** so serialized events stay
//! drop-in compatible with the live iOS WebSocket client. The richer event bus
//! (publish/subscribe) lands in later Milestone 2 tasks; this module only fixes
//! the string vocabulary plus a small membership helper for filter wiring.

// File events. The watcher picks the type via the TS `change-processor.ts`
// `getEventType` mapping: `create` → `file:created`, `delete` → `file:deleted`,
// and both `modify` and `rename` → `file:changed`. `data.action` always carries
// the raw `create|modify|delete|rename` verb. `file:renamed` is part of the
// taxonomy but stays reserved-but-unused (no emitter), matching the TS source.
// The shared prefix of the file family. Used by the event bus to apply hybrid
// persistence: only agent-attributed `file:*` events are persisted, the rest are
// broadcast-only.
pub const FILE_PREFIX: &str = "file:";
pub const FILE_CHANGED: &str = "file:changed";
pub const FILE_CREATED: &str = "file:created";
pub const FILE_DELETED: &str = "file:deleted";
pub(crate) const FILE_RENAMED: &str = "file:renamed";

// Agent lifecycle events.
pub const AGENT_STARTED: &str = "agent:started";
pub const AGENT_COMPLETED: &str = "agent:completed";
pub const AGENT_FAILED: &str = "agent:failed";
pub const AGENT_TOOL_CALL: &str = "agent:tool:call";
pub const AGENT_MESSAGE: &str = "agent:message";
// Content-bearing companion to the id-only `agent:message` echo (§6.5):
// emitted alongside EVERY `agent:message`, carrying the persisted preview
// projections the write just computed — `{ agentId, messageId, role,
// appMessageId?, turnId?, lastMessageRole?, lastMessageId?,
// lastAgentResponse?, lastUserMessage?, lastToolUse? }` — so clients update
// agent-card previews with zero follow-up RPCs. `agent:message` stays the
// lean back-compat echo.
pub const AGENT_LAST_MESSAGE: &str = "agent:last-message";

// Agent interaction events (agent-to-agent communication).
pub const AGENT_CREATED: &str = "agent:created";
pub const AGENT_DELETED: &str = "agent:deleted";
// Delete grace window (PROTOCOL §5.5): an `agent.delete` with
// `undoDelayMs > 0` schedules an in-memory pending deletion instead of
// committing immediately. Self-sufficient payloads: `delete-scheduled`
// carries `{ agentId, workspaceId, deleteAt }` (the ISO commit deadline)
// and `delete-cancelled` carries `{ agentId, workspaceId }` so clients
// flip the pending state without a follow-up read.
pub const AGENT_DELETE_SCHEDULED: &str = "agent:delete-scheduled";
pub const AGENT_DELETE_CANCELLED: &str = "agent:delete-cancelled";
// Soft retire (PROTOCOL §5.5): `agent:retired` marks a session inert
// (`retired_at` set — `ws.agent.retire`); `agent:restored` clears it
// (`agent.restore`). Both carry `{ agentId, agentName }`, and the session
// row survives with its full conversation on both sides.
pub const AGENT_RETIRED: &str = "agent:retired";
pub const AGENT_RESTORED: &str = "agent:restored";
pub const AGENT_RENAMED: &str = "agent:renamed";
pub const AGENT_UPDATED: &str = "agent:updated";
pub const AGENT_IDLE: &str = "agent:idle";
pub const AGENT_STATUS_CHANGED: &str = "agent:status-changed";
pub(crate) const AGENT_MESSAGE_SENT: &str = "agent:message:sent";
pub(crate) const AGENT_MESSAGE_RECEIVED: &str = "agent:message:received";
pub(crate) const AGENT_SUBSCRIBED: &str = "agent:subscribed";
pub(crate) const AGENT_UNSUBSCRIBED: &str = "agent:unsubscribed";
pub(crate) const AGENT_WOKEN_BY_SUBSCRIPTION: &str = "agent:woken-by-subscription";
pub(crate) const AGENT_DELIVERY_CONFIRMED: &str = "agent:delivery-confirmed";
pub(crate) const AGENT_EVENT_DELIVERY_FAILED: &str = "agent:event-delivery-failed";
pub(crate) const AGENT_EVENT_DELIVERY_TIMEOUT: &str = "agent:event-delivery-timeout";
pub(crate) const AGENT_SUBSCRIPTIONS_RESTORED: &str = "agent:subscriptions-restored";
pub const AGENT_SUBSCRIPTIONS_CHANGED: &str = "agent:subscriptions-changed";
pub(crate) const AGENT_MESSAGE_DELIVERY_FAILED: &str = "agent:message:delivery-failed";

// Agent streaming events (for the WebSocket API). All share the
// `agent:stream:` prefix — the high-volume stream family the §10.2
// retention/compaction sweep is allowed to trim.
pub const AGENT_STREAM_PREFIX: &str = "agent:stream:";
pub const AGENT_STREAM_START: &str = "agent:stream:start";
// Per-agent activity signal (renamed from `agent:stream:chunk`): broadcast
// while a turn streams so clients can tick busy/stall state and update
// watched-agent preview rows without receiving the transcript firehose.
// Payload is `{ agentId, messageId }` plus the server-derived live preview
// (`lastAgentResponse` / `digest`, each omitted until derivable from the
// streamed-so-far text); leading-edge throttled per agent (first activity of
// a turn emits immediately, then at most one per second). Emitted from BOTH
// the assistant-text-chunk arm and the tool-call arm, sharing the one
// per-agent throttle window, so a tool-heavy stretch keeps ticking; the
// tool-call emit additionally carries `lastToolUse: { name, status }`
// describing the call just recorded (absent on text-chunk emits). Full
// transcript content flows on the internal chat channel
// (`CHAT_STREAM_DELTA`) instead.
pub const AGENT_STREAM_ACTIVITY: &str = "agent:stream:activity";
// Terminal stream frame. The transcript-bearing terminal paths — normal
// prompt-turn completion, harness-wake finalize, and the user-interrupt
// flush — also carry the final `lastAgentResponse` / `digest` preview values
// (same optional fields as the activity signal, derived from the turn's full
// text) so a client tracking the live preview lands on the turn's true final
// state; the last throttled activity may have missed the response tail. The
// pre-output terminal-failure emit has no transcript, so it carries neither.
pub const AGENT_STREAM_END: &str = "agent:stream:end";
// Pre-first-token turn-startup status hints (new in intentd; PROTOCOL §6.5 /
// §7). Emitted while an agent turn is starting so the chat spinner can show
// the current phase (`launch` / `init` / `session-create` / `session-load` /
// `prompt`) before the first `agent:stream:activity` arrives; cleared by the
// FE on the first activity / `agent:stream:end` / `agent:failed`.
// Self-sufficient payload `{ agentId, workspaceId, phase, message, level,
// timestamp }` so a thin client renders the hint directly without a
// follow-up fetch. Mirrors the TS reference `acp-provider.ts` `emitStatus()`
// call sites.
pub const AGENT_STREAM_STATUS: &str = "agent:stream:status";

// Internal chat-channel content delta (PROTOCOL §7.1). Carries the full
// incremental transcript payload (`content` + block identity) that the
// per-agent `chat.subscribe` forwarder accumulates into block deltas.
// Deliberately OUTSIDE the `agent:*` family so `agent:*` (and bare-`*`)
// `events.subscribe` filters never receive the high-volume content firehose —
// external subscribers get the throttled `AGENT_STREAM_ACTIVITY` signal
// instead. Transient / broadcast-only, like the activity event.
pub const CHAT_STREAM_DELTA: &str = "chat:stream:delta";

// Agent queue events (for the WebSocket API).
pub const AGENT_QUEUE_UPDATED: &str = "agent:queue:updated";
pub const AGENT_QUEUE_PROCESSING: &str = "agent:queue:processing";
pub(crate) const AGENT_QUEUE_PROCESSING_CANCELLED: &str = "agent:queue:processing-cancelled";
pub(crate) const AGENT_QUEUE_STALE_MESSAGE: &str = "agent:queue:stale-message";

// Agent process-registry lifecycle events (new in intentd; PROTOCOL §6.5). Emitted
// by the daemon-internal `ProcessRegistry` when a spawn queues for admission
// (all slots active, or the aggregate memory budget is exceeded — monorepo#2063),
// when a queued spawn resumes, and when the registry evicts the LRU idle process.
// Self-sufficient payloads carry `{ agentId, used, cap, reason }` — `reason` is
// `"slots"` or `"memory-budget"`, naming the admission constraint that drove the
// event — so a client can render the saturation state without polling. Mirrors
// the TS reference `agent-process-registry.ts` logging.
pub const AGENT_PROCESS_QUEUED: &str = "agent:process:queued";
pub const AGENT_PROCESS_RESUMED: &str = "agent:process:resumed";
pub const AGENT_PROCESS_EVICTED: &str = "agent:process:evicted";

// Agent user message events (cross-client sync).
pub(crate) const AGENT_USER_MESSAGE_SENT: &str = "agent:user-message:sent";

// Agent permission events (new in intentd; PROTOCOL §8). The TS reference
// surfaced `session/request_permission` over Electron IPC rather than a
// `WorkspaceEvent`; a wire backend instead pushes these to subscribed clients
// and awaits a response RPC. `agent:permission:request` carries the normalized
// `PermissionRequestData`; `agent:permission:resolved` carries the chosen
// outcome (`selected`/`cancelled`). Both are scoped to the agent (`sessionId ==
// agentId`) so a client can route the prompt to the right agent view.
pub const AGENT_PERMISSION_REQUEST: &str = "agent:permission:request";
pub const AGENT_PERMISSION_RESOLVED: &str = "agent:permission:resolved";

// Agent session-stats event (new in intentd; PROTOCOL §5.24 / §6.5). Pushed when
// a session's per-session credit/message/tool rollup changes. Self-sufficient
// payload `{ sessionId, agentId?, stats: SessionStats }` (§6.7) so an agent card
// re-renders without a follow-up `agent.getSessionStats`.
pub const AGENT_SESSION_STATS_CHANGED: &str = "agent:session-stats-changed";

// Agent attention request (new in intentd). Emitted by
// `ws.agent.requestDiscussion` / `ws.agent.reportBlocker` when an agent
// explicitly raises attention before ending its turn. Self-sufficient payload
// `{ workspaceId, agentId, agentName, kind, reason, parentAgentId? }` (`kind`:
// `"discussion" | "blocker"`) drives the FE sticky "Switch To" toast without
// a follow-up fetch. `parentAgentId` is present only when the caller is a
// delegated child — omitted entirely (never `null`) for parentless agents;
// `agent:failed` carries the same optional field (enriched centrally in
// `publish_agent_event`).
pub const AGENT_ATTENTION_REQUESTED: &str = "agent:attention-requested";

// Pull-request events (new in intentd; §7.6). The TS reference broadcasts PR
// refresh deltas over Electron IPC (`workspace:background-enrichment-complete`,
// renderer-only); a wire backend instead emits `pr:*` WorkspaceEvents so the
// iOS WS client updates linked-PR state without polling. Self-sufficient
// payloads carry the new derived values: `pr:linked` →
// `{ workspaceId, prNumber, prUrl, prStatus, activePullRequest }`, `pr:updated`
// → `{ workspaceId, prNumber, prStatus, activePullRequest }`, `pr:unlinked` →
// `{ workspaceId }`. All three are emitted **only on change** by the background
// / on-demand PR refresh.
pub const PR_LINKED: &str = "pr:linked";
pub const PR_UPDATED: &str = "pr:updated";
pub const PR_UNLINKED: &str = "pr:unlinked";

// Git events.
pub const GIT_COMMIT: &str = "git:commit";
pub const GIT_PUSH: &str = "git:push";
pub const GIT_PULL: &str = "git:pull";
pub const GIT_BRANCH: &str = "git:branch";
pub(crate) const GIT_MERGE: &str = "git:merge";

// Workspace-git-root events (multi git root tracking,
// intent-hq/monorepo#2053). Emitted when a secondary git root is registered
// for a workspace, when a registered root's row changes (re-registration
// attribution merge, PR-field refresh), and when a root is unregistered or
// auto-pruned. Self-sufficient payloads: `gitRoot:registered` /
// `gitRoot:updated` carry `{ workspaceId, gitRoot }` (the full wire
// `WorkspaceGitRoot` row); `gitRoot:unregistered` carries
// `{ workspaceId, gitRootId, path }`.
pub const GIT_ROOT_REGISTERED: &str = "gitRoot:registered";
pub const GIT_ROOT_UPDATED: &str = "gitRoot:updated";
pub const GIT_ROOT_UNREGISTERED: &str = "gitRoot:unregistered";

// Note events.
pub const NOTE_CREATED: &str = "note:created";
pub const NOTE_UPDATED: &str = "note:updated";
pub const NOTE_DELETED: &str = "note:deleted";

// Line-attribution events (new in intentd; PROTOCOL §5.2.1). Emitted after the
// daemon recomputes per-line attributions for a note (post-mutation,
// debounced). The self-sufficient payload `{ workspaceId, noteId,
// attributions: { <lineNumber>: { timestamp, author? } } }` lets the FE
// gutter re-render without a follow-up `note.lineAttribution.load`.
pub const LINE_ATTRIBUTION_UPDATED: &str = "line-attribution:updated";

// Task events.
// Emitted once per note becoming a task, across every creation path
// (`@@@task` block conversion, `task.createPrerequisite`, and
// `task.markAsTask` on a note that was not previously a task). The
// self-sufficient payload `{ noteId, noteTitle, status, createdAt, agentId? }`
// parallels `task:status-changed` so a feed renders the new task without a
// follow-up read.
pub const TASK_CREATED: &str = "task:created";
pub const TASK_STATUS_CHANGED: &str = "task:status-changed";
pub const TASK_READY_TASKS_CHANGED: &str = "task:ready-tasks-changed";
// Task↔agent linkage events (new in intentd; PROTOCOL §5.4 / §6.5). Migrate
// the renderer-only `taskAgentAssociations` slice into daemon-emitted events.
// Self-sufficient payloads carry the full row so subscribers rebuild the
// `byNoteId → byTaskKey → link` map without a follow-up `listAgentLinks`.
// `task:agent-linked` → `{ workspaceId, noteId, taskKey, link: TaskAgentLink }`;
// `task:agent-unlinked` → `{ workspaceId, noteId, taskKey }`.
pub const TASK_AGENT_LINKED: &str = "task:agent-linked";
pub const TASK_AGENT_UNLINKED: &str = "task:agent-unlinked";

// Terminal events.
pub(crate) const TERMINAL_COMMAND: &str = "terminal:command";
// Interactive PTY streaming family (new in intentd; PROTOCOL §5.13/§6.5). The
// daemon fans live PTY output to subscribers as `terminal:data` (base64 `chunk`)
// and signals process exit with `terminal:exit`; `terminal:title`/`terminal:cwd`
// carry detected title / working-directory changes. All payloads are
// self-sufficient and carry the `terminalId`.
pub const TERMINAL_DATA: &str = "terminal:data";
pub const TERMINAL_EXIT: &str = "terminal:exit";
pub(crate) const TERMINAL_TITLE: &str = "terminal:title";
pub(crate) const TERMINAL_CWD: &str = "terminal:cwd";

// Script streaming family (new in intentd; PROTOCOL §5.8/§6.5). Scripts run on
// the unified PTY host (§12); the daemon fans live script output to subscribers
// as `script:output` (base64 `chunk`), publishes runtime/state transitions
// (start, exit, auto-restart, URL detection) as `script:state`, and definition
// mutations as `script:changed`. All payloads carry the `scriptId`.
pub const SCRIPT_OUTPUT: &str = "script:output";
pub const SCRIPT_STATE: &str = "script:state";
pub const SCRIPT_CHANGED: &str = "script:changed";

// Background-hook lifecycle events (new in intentd). Emitted by the hook
// scheduler on lifecycle transitions: `hook:scheduled` (schedule accepted),
// `hook:run-started` / `hook:run-completed` (one run of the script),
// `hook:dispatched` (script signalled dispatch; owner woken, hook terminated),
// `hook:evicted` (throw/timeout; owner woken with the reason),
// `hook:cancelled` (owner- or FE-initiated cancel), and `hook:expired` (TTL
// elapsed; owner woken so it can reschedule).
pub const HOOK_SCHEDULED: &str = "hook:scheduled";
pub const HOOK_RUN_STARTED: &str = "hook:run-started";
pub const HOOK_RUN_COMPLETED: &str = "hook:run-completed";
pub const HOOK_DISPATCHED: &str = "hook:dispatched";
pub const HOOK_EVICTED: &str = "hook:evicted";
pub const HOOK_CANCELLED: &str = "hook:cancelled";
pub const HOOK_EXPIRED: &str = "hook:expired";

// PR-monitor lifecycle events (`ws.pr.monitor`). Emitted by the centralized
// monitor loop: `prMonitor:registered` (a monitor armed or re-armed),
// `prMonitor:changed` (a poll detected changes; they accumulate for the
// debounce window), `prMonitor:emitted` (the consolidated wake was
// delivered), `prMonitor:completed` (the PR merged/closed; monitoring
// stopped), and `prMonitor:cancelled` (agent- or FE-initiated cancel).
pub const PR_MONITOR_REGISTERED: &str = "prMonitor:registered";
pub const PR_MONITOR_CHANGED: &str = "prMonitor:changed";
pub const PR_MONITOR_EMITTED: &str = "prMonitor:emitted";
pub const PR_MONITOR_COMPLETED: &str = "prMonitor:completed";
pub const PR_MONITOR_CANCELLED: &str = "prMonitor:cancelled";

// Test events.
pub(crate) const TEST_STARTED: &str = "test:started";
pub(crate) const TEST_COMPLETED: &str = "test:completed";

// Build events.
pub(crate) const BUILD_STARTED: &str = "build:started";
pub(crate) const BUILD_COMPLETED: &str = "build:completed";

// Workspace events.
pub const WORKSPACE_CREATED: &str = "workspace:created";
pub const WORKSPACE_UPDATED: &str = "workspace:updated";
pub const WORKSPACE_DELETED: &str = "workspace:deleted";
// Delete grace window (PROTOCOL §5.1): a `workspace.delete` with
// `undoDelayMs > 0` schedules an in-memory pending deletion instead of
// committing immediately. Self-sufficient payloads: `delete-scheduled`
// carries `{ workspaceId, deleteAt }` (the ISO commit deadline) and
// `delete-cancelled` carries `{ workspaceId }` so clients flip the pending
// state without a follow-up read.
pub const WORKSPACE_DELETE_SCHEDULED: &str = "workspace:delete-scheduled";
pub const WORKSPACE_DELETE_CANCELLED: &str = "workspace:delete-cancelled";
pub const WORKSPACE_OPENED: &str = "workspace:opened";
pub const WORKSPACE_CLOSED: &str = "workspace:closed";
// Setup-script lifecycle of the `workspace.create` setup stage (PROTOCOL
// §6.5). `workspace:setup:started` `{ workspaceId }` fires when an effective
// setup script was resolved and a spawn will be attempted;
// `workspace:setup:completed` `{ workspaceId, ranScript, exitCode? }` fires
// exactly once per logical create on every terminal path (no script, spawn
// failure, script exit) — including create paths that never reach the setup
// stage (`skipWorktree`, no worktree) and `workspace.duplicate`, which runs
// no setup. Idempotent replays publish nothing (same as `workspace:created`).
pub const WORKSPACE_SETUP_STARTED: &str = "workspace:setup:started";
pub const WORKSPACE_SETUP_COMPLETED: &str = "workspace:setup:completed";
pub(crate) const WORKSPACE_ACTIVITY: &str = "workspace:activity";
// Workspace status-change family (new in intentd; PROTOCOL §6.5). Self-sufficient
// payloads carry the new derived value so the FE flips the green/blue dot with no
// follow-up fetch: `workspace:activity-changed` → `{ workspaceId, activity }`,
// `workspace:attention-changed` → `{ workspaceId, attention }`. `activity-changed`
// is reserved-but-unused until the M6 status model lands an `activity` transition;
// `attention-changed` is emitted by `workspace.dismissAttention`/`markSeen` (§9.9).
pub const WORKSPACE_ACTIVITY_CHANGED: &str = "workspace:activity-changed";
pub const WORKSPACE_ATTENTION_CHANGED: &str = "workspace:attention-changed";
// Derived `Workspace.displayStatus` rollup transition (PROTOCOL §6.5):
// recomputed-and-compared after the task/PR mutations that can move the
// derivation; the self-sufficient payload `{ workspaceId, displayStatus }`
// updates the workspace card badge with no follow-up fetch.
pub const WORKSPACE_DISPLAY_STATUS_CHANGED: &str = "workspace:displayStatus-changed";
// Orthogonal `Workspace.waiting` flag transition (PROTOCOL §5.1 / §6.5):
// recomputed
// and compared after the hook / PR-monitor / completion-watch lifecycle
// transitions that can move the derivation; the self-sufficient payload
// `{ workspaceId, waiting }` (§6.7) flips the workspace wait indicator with
// no follow-up fetch.
pub const WORKSPACE_WAITING_CHANGED: &str = "workspace:waiting-changed";
// Token/credit usage recomputed by the daemon-internal scan job (§5.23 / §19.1).
// The self-sufficient payload `{ workspaceId, tokenUsage: TokenUsage }` carries
// the new snapshot so the FE re-renders without a follow-up `getTokenUsage`.
pub const WORKSPACE_TOKEN_USAGE_CHANGED: &str = "workspace:tokenUsage-changed";
// Chat context items changed for a workspace (new in intentd; PROTOCOL §5.1 /
// §6.5). Emitted by `workspace.updateContext`; the self-sufficient payload
// `{ workspaceId, items: ContextItem[] }` carries the new authoritative list
// so subscribers refresh without a follow-up `workspace.getContext`.
pub const WORKSPACE_CONTEXT_CHANGED: &str = "workspace:context-changed";
// Workspace transfer/export lifecycle (PROTOCOL §5.1 / §6.5), emitted by the
// source-side `workspace.export.*` build task. Self-sufficient payloads:
// `progress` carries `{ workspaceId, exportId, stage, bytesWritten? }`,
// `ready` carries `{ workspaceId, exportId, manifest, archiveSizeBytes,
// archiveSha256, maxChunkBytes, totalChunks }` (everything the FE hands to
// `workspace.import.begin` on the target), and `failed` carries
// `{ workspaceId, exportId, reason }`.
pub const WORKSPACE_TRANSFER_PROGRESS: &str = "workspace:transfer:progress";
pub const WORKSPACE_TRANSFER_READY: &str = "workspace:transfer:ready";
pub const WORKSPACE_TRANSFER_FAILED: &str = "workspace:transfer:failed";

// Spec / goal events.
pub(crate) const SPEC_UPDATED: &str = "spec:updated";
pub(crate) const GOAL_UPDATED: &str = "goal:updated";

// Comment events.
pub const COMMENT_ADDED: &str = "comment:added";
// Emitted by `comment.resolveThread` when a thread is (un)resolved. The
// self-sufficient payload `{ noteId, threadId, resolved }` lets a client flip
// the thread's resolved state without a follow-up read.
pub const COMMENT_RESOLVED: &str = "comment:resolved";

// Code-changes-review events (new in intentd; PROTOCOL §5.18–§5.20, §6.5). The
// BE records attribution internally (there is no `file-tracking.trackChange`
// RPC), so these self-sufficient payloads let the FE re-render without polling:
// `changes:tracked` → `{ workspaceId, changes: TrackedChange[] }`,
// `changes:git-status` → `{ workspaceId, status: WorkspaceGitStatus }`,
// `changes:metrics-changed` → `{ workspaceId, agentId?, metrics: Metrics }`.
pub(crate) const CHANGES_TRACKED: &str = "changes:tracked";
pub const CHANGES_GIT_STATUS: &str = "changes:git-status";
pub const CHANGES_METRICS_CHANGED: &str = "changes:metrics-changed";
// `changes:agent-locks` → `{ workspaceId, autoCommitEnabled, lockedAgentIds,
// lockedFilePaths }` — the daemon-computed agent-lock snapshot (§5.19, §6.5):
// which agents' files must not be manually staged/reverted because the agent
// is actively working with auto-commit enabled. Emitted on change only.
pub const CHANGES_AGENT_LOCKS: &str = "changes:agent-locks";

// Search streaming events (new in intentd; §5.15 / §6.5). Large or long-running
// `search.*` requests return `{ requestId }` promptly, then the daemon pushes
// incremental `search:result` batches (`data: { requestId, matches }`) followed
// by a terminal `search:done` (`data: { requestId, total, truncated }`), all
// correlated by `requestId`.
pub const SEARCH_RESULT: &str = "search:result";
pub const SEARCH_DONE: &str = "search:done";

// Drafts events (new in intentd; PROTOCOL §5.16/§6.5). Emitted after
// `drafts.set` / `drafts.clear`; the self-sufficient payload
// `{ workspaceId, agentId, clientId, hasDraft }` deliberately OMITS the draft
// text (no leakage) — it only signals that a client's draft exists or was
// cleared so other connections can sync/refetch.
pub const DRAFT_CHANGED: &str = "draft:changed";

// Streaming `git.clone` events (new in intentd; PROTOCOL §5.6 / §6.5). The
// `git.clone` method returns `{ requestId }` promptly, then the daemon streams
// `git:clone:progress` frames (`data: { requestId, phase, percent, message }`)
// as parsed from `git clone --progress` stderr, followed by a terminal
// `git:clone:done` (`data: { requestId, ok, error? }`), all correlated by
// `requestId`. Payloads never carry the source URL / credentials.
pub const GIT_CLONE_PROGRESS: &str = "git:clone:progress";
pub const GIT_CLONE_DONE: &str = "git:clone:done";

// Streaming `host.execStream` events (new in intentd; PROTOCOL §5.14 / §6.5).
// The `host.execStream` method returns `{ requestId }` promptly, then the daemon
// streams `host:exec:stdout` / `host:exec:stderr` frames (`data: { requestId,
// chunk }` — `chunk` is base64-encoded so binary output crosses the wire
// intact) as the child produces output, followed by a terminal `host:exec:exit`
// (`data: { requestId, exitCode?, timedOut?, cancelled?, ok }`), all correlated
// by `requestId`. Payloads never carry the command's env or argv (secret-safe;
// mirrors the one-shot `host.exec` guarantees).
pub const HOST_EXEC_STDOUT: &str = "host:exec:stdout";
pub const HOST_EXEC_STDERR: &str = "host:exec:stderr";
pub const HOST_EXEC_EXIT: &str = "host:exec:exit";

// MCP events.
pub(crate) const MCP_NOTIFICATION: &str = "mcp:notification";

// External MCP-server lifecycle (new in intentd; PROTOCOL §5.22/§6.5, §18.3).
// Emitted on every health/lifecycle transition (started/stopped/error/
// restarting) of a **user-configured external** MCP server. The self-sufficient
// payload `{ serverId, status: McpServerStatus }` carries the new runtime state
// so a client re-renders without polling. Distinct from the agent→BE callback
// (`mcp:notification`, §6.8).
pub const MCP_SERVERS_STATUS_CHANGED: &str = "mcp.servers:status-changed";

// Settings events (new in intentd; PROTOCOL §5.12/§6.5, §9.8). Emitted after a
// successful `settings.update`/`settings.reset`; the self-sufficient payload
// `{ changes: [{ path, value }] }` carries the applied pairs with **sensitive**
// values redacted (presence/placeholder only) so every connected client stays
// in sync without leaking secrets.
pub const SETTINGS_CHANGED: &str = "settings:changed";

// GitHub auth events (new in intentd; PROTOCOL §5.27). Emitted by the
// daemon-owned device-flow poller on terminal transitions (authorized /
// expired / denied / error) and by `github.revoke`. Global like
// `settings:changed` (empty workspace id). The self-sufficient payload
// `{ status }` carries only the transition — never a token, user code, or
// device code — so subscribers refresh via `github.authStatus`.
pub const GITHUB_AUTH_CHANGED: &str = "github:auth-changed";

// App-UI events (new in intentd; daemon-owned UI-driving surface for the
// chief workspace). `app:ui-navigate` → `{ route, workspaceId, highlightId?,
// durationMs? }`, `app:ui-highlight` → `{ id, workspaceId, durationMs? }`,
// `app:workspace-open` → `{ workspaceId, openInNewWindow? }`. Ported from the
// reference's Electron-main IPC sends (IPC_CHANNELS.APP.UI_NAVIGATE / UI_HIGHLIGHT
// + APP_WORKSPACE_OPERATION_CHANNEL); the daemon emits events and the FE bridge
// subscribes.
pub const APP_UI_NAVIGATE: &str = "app:ui-navigate";
pub const APP_UI_HIGHLIGHT: &str = "app:ui-highlight";
pub const APP_WORKSPACE_OPEN: &str = "app:workspace-open";

// Skills events (new in intentd; PROTOCOL §5.33/§6.5). Emitted when the
// discovered skill set changes for a workspace (file-watch on skill roots).
// Payload: `{ workspaceId }`.
pub const SKILLS_CHANGED: &str = "skills:changed";

// Specialists events (new in intentd; PROTOCOL §5.11/§6.5). Emitted when the
// resolved specialist set changes for a workspace (file-watch on the user
// `~/.intent/specialists/` and project `<workspace>/.intent/specialists/`
// tiers); user-tier changes fan out to one event per workspace. Payload:
// `{ workspaceId }`.
pub const SPECIALISTS_CHANGED: &str = "specialists:changed";

/// Every canonical event-type string in the taxonomy above. Useful for
/// validation and the filter/subscription wiring added in later M2 tasks.
pub const ALL_EVENT_TYPES: &[&str] = &[
    FILE_CHANGED,
    FILE_CREATED,
    FILE_DELETED,
    FILE_RENAMED,
    AGENT_STARTED,
    AGENT_COMPLETED,
    AGENT_FAILED,
    AGENT_TOOL_CALL,
    AGENT_MESSAGE,
    AGENT_LAST_MESSAGE,
    AGENT_CREATED,
    AGENT_DELETED,
    AGENT_DELETE_SCHEDULED,
    AGENT_DELETE_CANCELLED,
    AGENT_RETIRED,
    AGENT_RESTORED,
    AGENT_RENAMED,
    AGENT_UPDATED,
    AGENT_IDLE,
    AGENT_STATUS_CHANGED,
    AGENT_MESSAGE_SENT,
    AGENT_MESSAGE_RECEIVED,
    AGENT_SUBSCRIBED,
    AGENT_UNSUBSCRIBED,
    AGENT_WOKEN_BY_SUBSCRIPTION,
    AGENT_DELIVERY_CONFIRMED,
    AGENT_EVENT_DELIVERY_FAILED,
    AGENT_EVENT_DELIVERY_TIMEOUT,
    AGENT_SUBSCRIPTIONS_RESTORED,
    AGENT_SUBSCRIPTIONS_CHANGED,
    AGENT_MESSAGE_DELIVERY_FAILED,
    AGENT_STREAM_START,
    AGENT_STREAM_ACTIVITY,
    AGENT_STREAM_END,
    AGENT_STREAM_STATUS,
    CHAT_STREAM_DELTA,
    AGENT_QUEUE_UPDATED,
    AGENT_QUEUE_PROCESSING,
    AGENT_QUEUE_PROCESSING_CANCELLED,
    AGENT_QUEUE_STALE_MESSAGE,
    AGENT_PROCESS_QUEUED,
    AGENT_PROCESS_RESUMED,
    AGENT_PROCESS_EVICTED,
    AGENT_USER_MESSAGE_SENT,
    AGENT_PERMISSION_REQUEST,
    AGENT_PERMISSION_RESOLVED,
    AGENT_SESSION_STATS_CHANGED,
    AGENT_ATTENTION_REQUESTED,
    PR_LINKED,
    PR_UPDATED,
    PR_UNLINKED,
    GIT_COMMIT,
    GIT_PUSH,
    GIT_PULL,
    GIT_BRANCH,
    GIT_MERGE,
    GIT_ROOT_REGISTERED,
    GIT_ROOT_UPDATED,
    GIT_ROOT_UNREGISTERED,
    NOTE_CREATED,
    NOTE_UPDATED,
    NOTE_DELETED,
    LINE_ATTRIBUTION_UPDATED,
    TASK_CREATED,
    TASK_STATUS_CHANGED,
    TASK_READY_TASKS_CHANGED,
    TASK_AGENT_LINKED,
    TASK_AGENT_UNLINKED,
    TERMINAL_COMMAND,
    TERMINAL_DATA,
    TERMINAL_EXIT,
    TERMINAL_TITLE,
    TERMINAL_CWD,
    SCRIPT_OUTPUT,
    SCRIPT_STATE,
    SCRIPT_CHANGED,
    HOOK_SCHEDULED,
    HOOK_RUN_STARTED,
    HOOK_RUN_COMPLETED,
    HOOK_DISPATCHED,
    HOOK_EVICTED,
    HOOK_CANCELLED,
    HOOK_EXPIRED,
    PR_MONITOR_REGISTERED,
    PR_MONITOR_CHANGED,
    PR_MONITOR_EMITTED,
    PR_MONITOR_COMPLETED,
    PR_MONITOR_CANCELLED,
    TEST_STARTED,
    TEST_COMPLETED,
    BUILD_STARTED,
    BUILD_COMPLETED,
    WORKSPACE_CREATED,
    WORKSPACE_UPDATED,
    WORKSPACE_DELETED,
    WORKSPACE_DELETE_SCHEDULED,
    WORKSPACE_DELETE_CANCELLED,
    WORKSPACE_OPENED,
    WORKSPACE_CLOSED,
    WORKSPACE_SETUP_STARTED,
    WORKSPACE_SETUP_COMPLETED,
    WORKSPACE_ACTIVITY,
    WORKSPACE_ACTIVITY_CHANGED,
    WORKSPACE_ATTENTION_CHANGED,
    WORKSPACE_DISPLAY_STATUS_CHANGED,
    WORKSPACE_WAITING_CHANGED,
    WORKSPACE_TOKEN_USAGE_CHANGED,
    WORKSPACE_CONTEXT_CHANGED,
    WORKSPACE_TRANSFER_PROGRESS,
    WORKSPACE_TRANSFER_READY,
    WORKSPACE_TRANSFER_FAILED,
    SPEC_UPDATED,
    GOAL_UPDATED,
    COMMENT_ADDED,
    COMMENT_RESOLVED,
    CHANGES_TRACKED,
    CHANGES_GIT_STATUS,
    CHANGES_METRICS_CHANGED,
    CHANGES_AGENT_LOCKS,
    SEARCH_RESULT,
    SEARCH_DONE,
    DRAFT_CHANGED,
    GIT_CLONE_PROGRESS,
    GIT_CLONE_DONE,
    HOST_EXEC_STDOUT,
    HOST_EXEC_STDERR,
    HOST_EXEC_EXIT,
    MCP_NOTIFICATION,
    MCP_SERVERS_STATUS_CHANGED,
    SETTINGS_CHANGED,
    GITHUB_AUTH_CHANGED,
    APP_UI_NAVIGATE,
    APP_UI_HIGHLIGHT,
    APP_WORKSPACE_OPEN,
    SKILLS_CHANGED,
    SPECIALISTS_CHANGED,
];

/// True iff `event_type` is part of the canonical taxonomy.
#[must_use]
pub fn is_known_event_type(event_type: &str) -> bool {
    ALL_EVENT_TYPES.contains(&event_type)
}
