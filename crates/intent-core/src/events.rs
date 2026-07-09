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
pub const FILE_CHANGED: &str = "file:changed";
pub const FILE_CREATED: &str = "file:created";
pub const FILE_DELETED: &str = "file:deleted";
pub const FILE_RENAMED: &str = "file:renamed";

// Agent lifecycle events.
pub const AGENT_STARTED: &str = "agent:started";
pub const AGENT_COMPLETED: &str = "agent:completed";
pub const AGENT_FAILED: &str = "agent:failed";
pub const AGENT_TOOL_CALL: &str = "agent:tool:call";
pub const AGENT_MESSAGE: &str = "agent:message";

// Agent interaction events (agent-to-agent communication).
pub const AGENT_CREATED: &str = "agent:created";
pub const AGENT_DELETED: &str = "agent:deleted";
pub const AGENT_RESTORED: &str = "agent:restored";
pub const AGENT_RENAMED: &str = "agent:renamed";
pub const AGENT_UPDATED: &str = "agent:updated";
pub const AGENT_IDLE: &str = "agent:idle";
pub const AGENT_STATUS_CHANGED: &str = "agent:status-changed";
pub const AGENT_MESSAGE_SENT: &str = "agent:message:sent";
pub const AGENT_MESSAGE_RECEIVED: &str = "agent:message:received";
pub const AGENT_SUBSCRIBED: &str = "agent:subscribed";
pub const AGENT_UNSUBSCRIBED: &str = "agent:unsubscribed";
pub const AGENT_WOKEN_BY_SUBSCRIPTION: &str = "agent:woken-by-subscription";
pub const AGENT_DELIVERY_CONFIRMED: &str = "agent:delivery-confirmed";
pub const AGENT_EVENT_DELIVERY_FAILED: &str = "agent:event-delivery-failed";
pub const AGENT_EVENT_DELIVERY_TIMEOUT: &str = "agent:event-delivery-timeout";
pub const AGENT_SUBSCRIPTIONS_RESTORED: &str = "agent:subscriptions-restored";
pub const AGENT_SUBSCRIPTIONS_CHANGED: &str = "agent:subscriptions-changed";
pub const AGENT_MESSAGE_DELIVERY_FAILED: &str = "agent:message:delivery-failed";

// Agent streaming events (for the WebSocket API). All share the
// `agent:stream:` prefix — the high-volume chunk family the §10.2
// retention/compaction sweep is allowed to trim.
pub const AGENT_STREAM_PREFIX: &str = "agent:stream:";
pub const AGENT_STREAM_START: &str = "agent:stream:start";
pub const AGENT_STREAM_CHUNK: &str = "agent:stream:chunk";
pub const AGENT_STREAM_CONTENT_BLOCKS: &str = "agent:stream:content-blocks";
pub const AGENT_STREAM_END: &str = "agent:stream:end";
pub const AGENT_STREAM_MESSAGE: &str = "agent:stream:message";
pub const AGENT_STREAM_TOOL_USE: &str = "agent:stream:tool_use";
pub const AGENT_STREAM_TOOL_RESULT: &str = "agent:stream:tool_result";

// Agent queue events (for the WebSocket API).
pub const AGENT_QUEUE_UPDATED: &str = "agent:queue:updated";
pub const AGENT_QUEUE_PROCESSING: &str = "agent:queue:processing";
pub const AGENT_QUEUE_PROCESSING_CANCELLED: &str = "agent:queue:processing-cancelled";
pub const AGENT_QUEUE_STALE_MESSAGE: &str = "agent:queue:stale-message";

// Agent user message events (cross-client sync).
pub const AGENT_USER_MESSAGE_SENT: &str = "agent:user-message:sent";

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
pub const GIT_MERGE: &str = "git:merge";

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
pub const TASK_STATUS_CHANGED: &str = "task:status-changed";
pub const TASK_READY_TASKS_CHANGED: &str = "task:ready-tasks-changed";

// Terminal events.
pub const TERMINAL_COMMAND: &str = "terminal:command";
// Interactive PTY streaming family (new in intentd; PROTOCOL §5.13/§6.5). The
// daemon fans live PTY output to subscribers as `terminal:data` (base64 `chunk`)
// and signals process exit with `terminal:exit`; `terminal:title`/`terminal:cwd`
// carry detected title / working-directory changes. All payloads are
// self-sufficient and carry the `terminalId`.
pub const TERMINAL_DATA: &str = "terminal:data";
pub const TERMINAL_EXIT: &str = "terminal:exit";
pub const TERMINAL_TITLE: &str = "terminal:title";
pub const TERMINAL_CWD: &str = "terminal:cwd";

// Script streaming family (new in intentd; PROTOCOL §5.8/§6.5). Scripts run on
// the unified PTY host (§12); the daemon fans live script output to subscribers
// as `script:output` (base64 `chunk`) and publishes runtime/state transitions
// (start, exit, auto-restart, URL detection) as `script:state`. Both payloads
// are self-sufficient and carry the `scriptId`.
pub const SCRIPT_OUTPUT: &str = "script:output";
pub const SCRIPT_STATE: &str = "script:state";

// Test events.
pub const TEST_STARTED: &str = "test:started";
pub const TEST_COMPLETED: &str = "test:completed";

// Build events.
pub const BUILD_STARTED: &str = "build:started";
pub const BUILD_COMPLETED: &str = "build:completed";

// Workspace events.
pub const WORKSPACE_CREATED: &str = "workspace:created";
pub const WORKSPACE_UPDATED: &str = "workspace:updated";
pub const WORKSPACE_DELETED: &str = "workspace:deleted";
pub const WORKSPACE_OPENED: &str = "workspace:opened";
pub const WORKSPACE_CLOSED: &str = "workspace:closed";
pub const WORKSPACE_ACTIVITY: &str = "workspace:activity";
// Workspace status-change family (new in intentd; PROTOCOL §6.5). Self-sufficient
// payloads carry the new derived value so the FE flips the green/blue dot with no
// follow-up fetch: `workspace:activity-changed` → `{ workspaceId, activity }`,
// `workspace:attention-changed` → `{ workspaceId, attention }`. `activity-changed`
// is reserved-but-unused until the M6 status model lands an `activity` transition;
// `attention-changed` is emitted by `workspace.dismissAttention`/`markSeen` (§9.9).
pub const WORKSPACE_ACTIVITY_CHANGED: &str = "workspace:activity-changed";
pub const WORKSPACE_ATTENTION_CHANGED: &str = "workspace:attention-changed";
// Token/credit usage recomputed by the daemon-internal scan job (§5.23 / §19.1).
// The self-sufficient payload `{ workspaceId, tokenUsage: TokenUsage }` carries
// the new snapshot so the FE re-renders without a follow-up `getTokenUsage`.
pub const WORKSPACE_TOKEN_USAGE_CHANGED: &str = "workspace:tokenUsage-changed";

// Spec / goal events.
pub const SPEC_UPDATED: &str = "spec:updated";
pub const GOAL_UPDATED: &str = "goal:updated";

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
pub const CHANGES_TRACKED: &str = "changes:tracked";
pub const CHANGES_GIT_STATUS: &str = "changes:git-status";
pub const CHANGES_METRICS_CHANGED: &str = "changes:metrics-changed";

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
pub const MCP_NOTIFICATION: &str = "mcp:notification";

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
    AGENT_CREATED,
    AGENT_DELETED,
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
    AGENT_STREAM_CHUNK,
    AGENT_STREAM_CONTENT_BLOCKS,
    AGENT_STREAM_END,
    AGENT_STREAM_MESSAGE,
    AGENT_STREAM_TOOL_USE,
    AGENT_STREAM_TOOL_RESULT,
    AGENT_QUEUE_UPDATED,
    AGENT_QUEUE_PROCESSING,
    AGENT_QUEUE_PROCESSING_CANCELLED,
    AGENT_QUEUE_STALE_MESSAGE,
    AGENT_USER_MESSAGE_SENT,
    AGENT_PERMISSION_REQUEST,
    AGENT_PERMISSION_RESOLVED,
    AGENT_SESSION_STATS_CHANGED,
    PR_LINKED,
    PR_UPDATED,
    PR_UNLINKED,
    GIT_COMMIT,
    GIT_PUSH,
    GIT_PULL,
    GIT_BRANCH,
    GIT_MERGE,
    NOTE_CREATED,
    NOTE_UPDATED,
    NOTE_DELETED,
    LINE_ATTRIBUTION_UPDATED,
    TASK_STATUS_CHANGED,
    TASK_READY_TASKS_CHANGED,
    TERMINAL_COMMAND,
    TERMINAL_DATA,
    TERMINAL_EXIT,
    TERMINAL_TITLE,
    TERMINAL_CWD,
    SCRIPT_OUTPUT,
    SCRIPT_STATE,
    TEST_STARTED,
    TEST_COMPLETED,
    BUILD_STARTED,
    BUILD_COMPLETED,
    WORKSPACE_CREATED,
    WORKSPACE_UPDATED,
    WORKSPACE_DELETED,
    WORKSPACE_OPENED,
    WORKSPACE_CLOSED,
    WORKSPACE_ACTIVITY,
    WORKSPACE_ACTIVITY_CHANGED,
    WORKSPACE_ATTENTION_CHANGED,
    WORKSPACE_TOKEN_USAGE_CHANGED,
    SPEC_UPDATED,
    GOAL_UPDATED,
    COMMENT_ADDED,
    COMMENT_RESOLVED,
    CHANGES_TRACKED,
    CHANGES_GIT_STATUS,
    CHANGES_METRICS_CHANGED,
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
];

/// True iff `event_type` is part of the canonical taxonomy.
pub fn is_known_event_type(event_type: &str) -> bool {
    ALL_EVENT_TYPES.contains(&event_type)
}
