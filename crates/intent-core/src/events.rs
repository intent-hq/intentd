//! Event-type taxonomy (§10; ported from `~/src/intent/src/features/events/types.ts`
//! `WorkspaceEventType`).
//!
//! These are the canonical `Event::event_type` strings carried on the wire as
//! `type`. They must match the TS values **exactly** so serialized events stay
//! drop-in compatible with the live iOS WebSocket client. The richer event bus
//! (publish/subscribe) lands in later Milestone 2 tasks; this module only fixes
//! the string vocabulary plus a small membership helper for filter wiring.

// File events. Canonical taxonomy lives on `data.action`
// (`create|modify|delete|rename`); `file:created/deleted/renamed` are
// reserved-but-unused (no emitter) per the TS source.
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

// Agent streaming events (for the WebSocket API).
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

// Task events.
pub const TASK_STATUS_CHANGED: &str = "task:status-changed";
pub const TASK_READY_TASKS_CHANGED: &str = "task:ready-tasks-changed";

// Terminal events.
pub const TERMINAL_COMMAND: &str = "terminal:command";

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

// Spec / goal events.
pub const SPEC_UPDATED: &str = "spec:updated";
pub const GOAL_UPDATED: &str = "goal:updated";

// Comment events.
pub const COMMENT_ADDED: &str = "comment:added";

// MCP events.
pub const MCP_NOTIFICATION: &str = "mcp:notification";

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
    GIT_COMMIT,
    GIT_PUSH,
    GIT_PULL,
    GIT_BRANCH,
    GIT_MERGE,
    NOTE_CREATED,
    NOTE_UPDATED,
    NOTE_DELETED,
    TASK_STATUS_CHANGED,
    TASK_READY_TASKS_CHANGED,
    TERMINAL_COMMAND,
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
    SPEC_UPDATED,
    GOAL_UPDATED,
    COMMENT_ADDED,
    MCP_NOTIFICATION,
];

/// True iff `event_type` is part of the canonical taxonomy.
pub fn is_known_event_type(event_type: &str) -> bool {
    ALL_EVENT_TYPES.contains(&event_type)
}
