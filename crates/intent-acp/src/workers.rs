//! Provider background-worker reports (PROTOCOL §5.5b).
//!
//! Allowlisted adapters (claude-agent-acp, codex-acp) report background shells
//! and tasks through the AIR async-tasks extension once `initialize` requests
//! it. Those reports are not conversation content: they must never reach the
//! turn router, where an out-of-turn frame would open an implicit harness-wake
//! turn, and the ACP schema has no variant for them anyway. The connection
//! reader therefore diverts them (see [`route`]) to a dedicated per-connection
//! [`WorkerFrame`] sink stamped with the receipt time; the service layer owns
//! the bounded projection built from them.

use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};

use crate::error::AcpResult;
use crate::transport::Connection;

/// `_meta` namespace the AIR extension lives under.
pub const JETBRAINS_META_KEY: &str = "jetbrains";
/// AIR key inside [`JETBRAINS_META_KEY`].
pub const AIR_META_KEY: &str = "air";
/// The AIR capability that turns on worker reports.
pub const ASYNC_TASKS_CAPABILITY: &str = "asyncTasks";
/// AIR extension version Intent speaks.
pub const AIR_EXTENSION_VERSION: u64 = 1;
/// Provider extension request that stops one worker without cancelling the turn.
pub const STOP_METHOD: &str = "_session/async_task/stop";
/// How long a stop request may wait for the provider's answer.
pub const STOP_TIMEOUT: Duration = Duration::from_secs(15);

/// The `session/update` variants that carry worker reports.
const WORKER_UPDATES: [&str; 3] = [
    "async_task_spawned",
    "async_task_state_update",
    "async_task_progress",
];

/// Tool fields whose presence makes a backgrounded marker a genuine tool update.
const GENUINE_TOOL_FIELDS: [&str; 7] = [
    "status",
    "title",
    "kind",
    "rawInput",
    "rawOutput",
    "content",
    "locations",
];

/// Merge the async-tasks request into an `initialize` `clientCapabilities._meta`
/// object, preserving every other key (including other AIR keys).
pub fn request_async_tasks(meta: &mut Map<String, Value>) {
    let jetbrains = object_entry(meta, JETBRAINS_META_KEY);
    let air = object_entry(jetbrains, AIR_META_KEY);
    air.insert("version".into(), json!(AIR_EXTENSION_VERSION));
    let capabilities = air.entry("capabilities").or_insert_with(|| json!([]));
    if !capabilities.is_array() {
        *capabilities = json!([]);
    }
    if let Value::Array(capabilities) = capabilities {
        if !capabilities
            .iter()
            .any(|c| c.as_str() == Some(ASYNC_TASKS_CAPABILITY))
        {
            capabilities.push(json!(ASYNC_TASKS_CAPABILITY));
        }
    }
}

/// The object under `key`, replacing an absent or non-object value with `{}`.
fn object_entry<'a>(map: &'a mut Map<String, Value>, key: &str) -> &'a mut Map<String, Value> {
    let slot = map.entry(key).or_insert_with(|| json!({}));
    if !slot.is_object() {
        *slot = json!({});
    }
    match slot {
        Value::Object(object) => object,
        _ => unreachable!("replaced with an object above"),
    }
}

/// One worker report as received on a connection, before any validation.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerFrame {
    /// The notification's `sessionId`, when it is a string.
    pub session_id: Option<String>,
    /// The raw `update` object.
    pub update: Value,
    /// Daemon receipt time, Unix milliseconds.
    pub received_at_ms: u64,
}

/// What the reader does with one `session/update` notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Routing {
    /// A worker report: hand it to the worker sink, never to the turn router.
    Worker,
    /// A metadata-only backgrounded marker: no tool state, no turn, no row.
    Marker,
    /// Ordinary traffic for the turn router (including a mixed marker, whose
    /// genuine tool fields keep their ordinary processing).
    Forward,
}

/// Classify a `session/update` notification's params.
#[must_use]
pub fn route(params: &Value) -> Routing {
    let update = &params["update"];
    let Some(kind) = update.get("sessionUpdate").and_then(Value::as_str) else {
        return Routing::Forward;
    };
    if WORKER_UPDATES.contains(&kind) {
        return Routing::Worker;
    }
    if kind == "tool_call_update"
        && is_backgrounded_marker(update)
        && !GENUINE_TOOL_FIELDS
            .iter()
            .any(|field| update.get(*field).is_some())
    {
        return Routing::Marker;
    }
    Routing::Forward
}

/// Whether an update carries `_meta.jetbrains.air.asyncTasks.backgrounded: true`.
#[must_use]
pub fn is_backgrounded_marker(update: &Value) -> bool {
    update["_meta"][JETBRAINS_META_KEY][AIR_META_KEY][ASYNC_TASKS_CAPABILITY]["backgrounded"]
        == Value::Bool(true)
}

/// Build the frame for a diverted worker report.
#[must_use]
pub fn frame(params: &Value) -> WorkerFrame {
    WorkerFrame {
        session_id: params["sessionId"].as_str().map(str::to_owned),
        update: params["update"].clone(),
        received_at_ms: now_ms(),
    }
}

/// Daemon receipt time in Unix milliseconds, from a monotonic clock anchored
/// to the wall clock once per process: a system clock step can never make a
/// later report look older than an earlier one (§5.5b ordering).
#[must_use]
pub fn now_ms() -> u64 {
    static ANCHOR: OnceLock<(Instant, u64)> = OnceLock::new();
    let (instant, wall) = ANCHOR.get_or_init(|| {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        (Instant::now(), wall)
    });
    wall.saturating_add(u64::try_from(instant.elapsed().as_millis()).unwrap_or(u64::MAX))
}

/// The §5.5b wire-ID rule: a nonempty string of at most 256 UTF-8 bytes
/// without ASCII control characters.
#[must_use]
pub fn is_wire_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(is_control)
}

/// ASCII control characters (U+0000–U+001F and U+007F).
#[must_use]
pub fn is_control(c: char) -> bool {
    c <= '\u{1f}' || c == '\u{7f}'
}

/// Ask the provider to stop one worker. Returns the provider's raw answer.
///
/// # Errors
///
/// Propagates the transport, RPC and timeout errors of the request.
pub async fn request_stop(
    conn: &Connection,
    session_id: &str,
    async_task_id: &str,
) -> AcpResult<Value> {
    conn.request_timeout(
        STOP_METHOD,
        json!({ "sessionId": session_id, "asyncTaskId": async_task_id }),
        STOP_TIMEOUT,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(update: Value) -> Value {
        let mut params = json!({ "sessionId": "s" });
        params["update"] = update;
        params
    }

    #[test]
    fn request_merges_without_clobbering_other_meta() {
        let mut meta = Map::new();
        meta.insert("intent.callbacks".into(), json!({ "version": 1 }));
        meta.insert(
            "jetbrains".into(),
            json!({ "air": { "capabilities": ["other"], "keep": true } }),
        );
        request_async_tasks(&mut meta);
        request_async_tasks(&mut meta);
        assert_eq!(meta["intent.callbacks"], json!({ "version": 1 }));
        assert_eq!(
            meta["jetbrains"]["air"],
            json!({ "capabilities": ["other", "asyncTasks"], "keep": true, "version": 1 })
        );
    }

    #[test]
    fn request_builds_the_documented_shape_from_empty_or_malformed_meta() {
        let mut meta = Map::new();
        request_async_tasks(&mut meta);
        assert_eq!(
            Value::Object(meta),
            json!({ "jetbrains": { "air": { "version": 1, "capabilities": ["asyncTasks"] } } })
        );
        let mut bad = Map::new();
        bad.insert("jetbrains".into(), json!("x"));
        request_async_tasks(&mut bad);
        assert_eq!(
            bad["jetbrains"]["air"]["capabilities"],
            json!(["asyncTasks"])
        );
    }

    #[test]
    fn worker_updates_are_diverted() {
        for kind in WORKER_UPDATES {
            assert_eq!(
                route(&update(json!({ "sessionUpdate": kind }))),
                Routing::Worker
            );
        }
        assert_eq!(
            route(&update(json!({ "sessionUpdate": "agent_message_chunk" }))),
            Routing::Forward
        );
        assert_eq!(route(&json!({})), Routing::Forward);
    }

    #[test]
    fn metadata_only_marker_is_swallowed_but_a_mixed_marker_is_forwarded() {
        let marker = json!({
            "sessionUpdate": "tool_call_update",
            "toolCallId": "call-A",
            "_meta": { "jetbrains": { "air": { "asyncTasks": { "backgrounded": true } } } }
        });
        assert_eq!(route(&update(marker.clone())), Routing::Marker);
        for field in GENUINE_TOOL_FIELDS {
            let mut mixed = marker.clone();
            mixed[field] = json!(null);
            assert_eq!(route(&update(mixed)), Routing::Forward, "{field}");
        }
        let mut not_true = marker.clone();
        not_true["_meta"]["jetbrains"]["air"]["asyncTasks"]["backgrounded"] = json!("true");
        assert_eq!(route(&update(not_true)), Routing::Forward);
        let mut call = marker;
        call["sessionUpdate"] = json!("tool_call");
        assert_eq!(route(&update(call)), Routing::Forward);
    }

    #[test]
    fn frame_keeps_session_and_raw_update() {
        let f = frame(
            &json!({ "sessionId": "s-1", "update": { "sessionUpdate": "async_task_spawned" } }),
        );
        assert_eq!(f.session_id.as_deref(), Some("s-1"));
        assert_eq!(f.update["sessionUpdate"], "async_task_spawned");
        assert!(f.received_at_ms > 0);
        assert!(
            now_ms() >= f.received_at_ms,
            "receipt clock never goes backwards"
        );
        assert!(is_wire_id("t-1") && !is_wire_id("") && !is_wire_id("a\u{7f}"));
        assert!(!is_wire_id(&"x".repeat(257)) && is_wire_id(&"x".repeat(256)));
        assert_eq!(
            frame(&json!({ "sessionId": 4, "update": {} })).session_id,
            None
        );
    }
}
