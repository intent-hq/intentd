//! Pure mapping/aggregation helpers for the `event.*` query methods (§5.10).
//!
//! Ported wire-for-wire from `~/src/intent/src/features/events/main/
//! agent-event-tools.ts`: the `FileActivity` projection (`getAgentFiles`), the
//! `getAgentActivity` grouping, and the `getWorkspaceSummary` aggregation. The
//! store-backed queries that feed these live in `intent-store`; this module is
//! pure (no IO) so the aggregation math is unit-testable in isolation.

use std::collections::{HashMap, HashSet};

use intent_core::events::{AGENT_TOOL_CALL, FILE_CHANGED};
use intent_core::{
    ActorType, AgentActivity, Event, FileActivity, TopChangedFile, WorkspaceEventSummary,
};
use serde_json::Value;

/// The lowercase wire string for an [`ActorType`] (matches the serde form).
fn actor_type_str(actor_type: ActorType) -> &'static str {
    match actor_type {
        ActorType::User => "user",
        ActorType::Agent => "agent",
        ActorType::System => "system",
        ActorType::External => "external",
        ActorType::Tool => "tool",
    }
}

/// `"type:name"` actor label, reproducing the TS template literal
/// `` `${e.actor.type}:${e.actor.name}` `` (an absent name renders `undefined`).
fn combined_actor(ev: &Event) -> String {
    let name = ev.actor.name.as_deref().unwrap_or("undefined");
    format!("{}:{}", actor_type_str(ev.actor.actor_type), name)
}

/// Read a `data.<key>` string, defaulting to empty (the TS path/relativePath/
/// action fields are always present on `file:changed` events).
fn data_str(ev: &Event, key: &str) -> String {
    ev.data
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// Clone a `data.<key>` value, omitting it entirely when the key is absent
/// (mirrors `additions: e.data.additions` being dropped by `JSON.stringify`).
fn data_opt(ev: &Event, key: &str) -> Option<Value> {
    ev.data.get(key).cloned()
}

/// Project a `file:changed` event into a [`FileActivity`] with the combined
/// `"type:name"` actor (workspace summary `recentFiles`).
pub(crate) fn file_activity_combined(ev: &Event) -> FileActivity {
    FileActivity {
        path: data_str(ev, "path"),
        relative_path: data_str(ev, "relativePath"),
        action: data_str(ev, "action"),
        timestamp: ev.timestamp.clone(),
        actor: Some(combined_actor(ev)),
        additions: data_opt(ev, "additions"),
        deletions: data_opt(ev, "deletions"),
    }
}

/// Project a `file:changed` event with the bare actor name (`getAgentFiles`).
pub(crate) fn file_activity_named(ev: &Event) -> FileActivity {
    FileActivity {
        path: data_str(ev, "path"),
        relative_path: data_str(ev, "relativePath"),
        action: data_str(ev, "action"),
        timestamp: ev.timestamp.clone(),
        actor: ev.actor.name.clone(),
        additions: data_opt(ev, "additions"),
        deletions: data_opt(ev, "deletions"),
    }
}

/// Group events by `actor.id` into [`AgentActivity`] rows, preserving first-seen
/// order (`getAgentActivity`). Events lacking an `actor.id` are skipped;
/// `lastActive` ends at the last-iterated event's timestamp and `filesModified`
/// is de-duplicated in insertion order — both faithful to the TS.
pub(crate) fn aggregate_agent_activity(events: &[Event]) -> Vec<AgentActivity> {
    let mut order: Vec<String> = Vec::new();
    let mut map: HashMap<String, AgentActivity> = HashMap::new();
    for ev in events {
        let agent_id = match &ev.actor.id {
            Some(id) => id.clone(),
            None => continue,
        };
        let entry = map.entry(agent_id.clone()).or_insert_with(|| {
            order.push(agent_id.clone());
            AgentActivity {
                agent_id: agent_id.clone(),
                agent_name: ev.actor.name.clone(),
                event_count: 0,
                tool_calls: 0,
                files_modified: Vec::new(),
                last_active: ev.timestamp.clone(),
            }
        });
        entry.event_count += 1;
        entry.last_active.clone_from(&ev.timestamp);
        if ev.event_type == AGENT_TOOL_CALL {
            entry.tool_calls += 1;
            if let Some(files) = ev.data.get("filesModified").and_then(Value::as_array) {
                for f in files.iter().filter_map(Value::as_str) {
                    entry.files_modified.push(f.to_string());
                }
            }
        }
        if ev.event_type == FILE_CHANGED {
            entry.files_modified.push(data_str(ev, "path"));
        }
    }
    for activity in map.values_mut() {
        let mut seen = HashSet::new();
        activity.files_modified.retain(|f| seen.insert(f.clone()));
    }
    order
        .into_iter()
        .map(|id| map.remove(&id).expect("agent id present"))
        .collect()
}

/// Build the `event.workspaceSummary` aggregate from all events in the window
/// (`getWorkspaceSummary`): the first five `file:changed` rows, per-agent
/// activity (agent-typed subset), the events-per-minute rate, and the top five
/// most-changed files (count desc, ties by first appearance).
pub(crate) fn build_workspace_summary(
    all_events: &[Event],
    minutes_ago: i64,
) -> WorkspaceEventSummary {
    let file_events: Vec<&Event> = all_events
        .iter()
        .filter(|e| e.event_type == FILE_CHANGED)
        .collect();
    let recent_files = file_events
        .iter()
        .take(5)
        .map(|e| file_activity_combined(e))
        .collect();
    let agent_events: Vec<Event> = all_events
        .iter()
        .filter(|e| e.actor.actor_type == ActorType::Agent)
        .cloned()
        .collect();
    let active_agents = aggregate_agent_activity(&agent_events);
    // Event counts and window sizes are far below 2^53: loss-free in f64.
    #[allow(clippy::cast_precision_loss)]
    let event_rate = all_events.len() as f64 / minutes_ago as f64;

    let mut order: Vec<String> = Vec::new();
    let mut counts: HashMap<String, i64> = HashMap::new();
    for e in &file_events {
        let path = data_str(e, "path");
        if !counts.contains_key(&path) {
            order.push(path.clone());
        }
        *counts.entry(path).or_insert(0) += 1;
    }
    let mut top_changed_files: Vec<TopChangedFile> = order
        .into_iter()
        .map(|p| {
            let change_count = counts[&p];
            TopChangedFile {
                path: p,
                change_count,
            }
        })
        .collect();
    top_changed_files.sort_by_key(|b| std::cmp::Reverse(b.change_count));
    top_changed_files.truncate(5);

    WorkspaceEventSummary {
        recent_files,
        active_agents,
        event_rate,
        top_changed_files,
    }
}

/// Serialized-size ceiling for one `event.query` response (monorepo#3347).
/// Sized comfortably below the transport's 1 MiB large-frame advisory
/// (`LARGE_MESSAGE_WARN_BYTES`): [`bound_event_rows`] charges each row its
/// ACTUAL post-cap serialized size against this budget, so the response
/// total stays within `budget + Σ(per-row identity fields + marker)` — the
/// identity fields are normally small daemon-generated scalars (uuid ids,
/// ISO timestamps, enum actor types), keeping the worst case under the
/// advisory even at [`EVENT_QUERY_MAX_LEGACY_LIMIT`] rows. Rows whose
/// stored strings are pathological get every trimmable field capped —
/// see [`EVENT_ROW_TRIM_FIELDS`] — so the only untrimmable floor is the
/// bounded scalar set (`id`, `workspaceId`, `type`, `timestamp`).
pub(crate) const EVENT_QUERY_RESPONSE_BUDGET_BYTES: usize = 700 * 1024;

/// Row-count cap for the legacy (non-paginated) `event.query` path
/// (monorepo#3347). Previously the caller-supplied limit was passed to SQL
/// unclamped (a negative value even meant "no limit"), so the byte ceiling
/// above could be defeated by sheer row count — the identity fields of an
/// unbounded row set are themselves unbounded. 500 keeps the FE's 300-row
/// boot snapshot intact; the paginated path keeps its own [1, 200] clamp.
pub(crate) const EVENT_QUERY_MAX_LEGACY_LIMIT: i64 = 500;

/// The per-row fields eligible for trimming, in cap order: the small
/// identity-adjacent fields first (their tiny intact copies are admitted
/// before the payload spends the shared budget), then the bulky
/// type-specific payload and free-form metadata. `actor` and the
/// session/correlation/parent ids are normally small daemon-generated
/// scalars that pass through intact — but they are caller-influenced
/// (`actor.name`, free-form `actor.metadata`) and stored unbounded, so
/// leaving them untrimmable would let a pathological row defeat the
/// response ceiling. The truly untrimmable floor is the bounded
/// daemon-generated scalar set: `id`, `workspaceId`, `type`, `timestamp`,
/// plus the two markers.
const EVENT_ROW_TRIM_FIELDS: [&str; 6] = [
    "actor",
    "sessionId",
    "correlationId",
    "parentEventId",
    "data",
    "metadata",
];

/// Serialize an `event.query` row set to wire-shape JSON values, ready for
/// [`bound_event_rows`]. Pure so the service layer maps the error itself.
pub(crate) fn serialize_event_rows(
    events: Vec<Event>,
) -> std::result::Result<Vec<Value>, serde_json::Error> {
    events.into_iter().map(serde_json::to_value).collect()
}

/// Bound the serialized size of an `event.query` row set (monorepo#3347).
///
/// A row set whose total serialized size fits [`EVENT_QUERY_RESPONSE_BUDGET_BYTES`]
/// is returned untouched (byte-identical wire behavior for the common case).
/// Over budget, rows are walked in wire order (newest→oldest) with a running
/// budget: each row gets a fair share of what remains (`remaining / rows_left`,
/// so small rows donate their unused share to the rows after them), and a row
/// over its share has its [`EVENT_ROW_TRIM_FIELDS`] replaced by
/// [`intent_core::cap_json_value`] bounded previews plus additive row-level
/// markers `truncated: true` and `originalBytes` (the row's full serialized
/// size), so the trimming is observable rather than silent. Row count and the
/// per-row identity shape are preserved — no rows are dropped, and a trimmed
/// row always keeps `actor.type` (required on the wire).
///
/// `cap_json_value` budgets strings by RAW UTF-8 length, but the row is
/// JSON-serialized afterward, so escaping-heavy content (quotes, backslashes,
/// control chars — up to 6 output bytes per input byte) can serialize well
/// past the allowance. The cap therefore measures the row's ACTUAL serialized
/// size after each pass and proportionally shrinks the allowance and re-caps
/// until the row fits its share — the same converging shrink as
/// `AgentLite::cap_list_previews` — so the wire bound holds regardless of
/// content.
///
/// Returns the number of trimmed rows (`0` = response untouched).
pub(crate) fn bound_event_rows(rows: &mut [Value]) -> usize {
    let total: usize = rows.iter().map(intent_core::slim_body_size).sum();
    if total <= EVENT_QUERY_RESPONSE_BUDGET_BYTES {
        return 0;
    }
    let n = rows.len();
    let mut remaining = EVENT_QUERY_RESPONSE_BUDGET_BYTES;
    let mut trimmed = 0usize;
    for (i, row) in rows.iter_mut().enumerate() {
        let size = intent_core::slim_body_size(row);
        let share = remaining / (n - i);
        if size <= share {
            remaining -= size;
            continue;
        }
        if !row.is_object() {
            // Rows are always serialized `Event` objects; charge anything
            // else defensively and move on.
            remaining = remaining.saturating_sub(size);
            continue;
        }
        remaining = remaining.saturating_sub(cap_event_row(row, share, size));
        trimmed += 1;
    }
    trimmed
}

/// Cap one over-share `event.query` row in place: replace its
/// [`EVENT_ROW_TRIM_FIELDS`] with bounded previews, stamp the
/// `truncated` / `originalBytes` markers, and converge on the row's ACTUAL
/// serialized size (escaping-aware — see [`bound_event_rows`]). `size` is the
/// row's full pre-cap serialized size. Returns the post-cap serialized size
/// to charge against the running budget. The row must be an object.
fn cap_event_row(row: &mut Value, share: usize, size: usize) -> usize {
    let obj = row.as_object_mut().expect("caller checked is_object");
    let bulk: usize = EVENT_ROW_TRIM_FIELDS
        .iter()
        .filter_map(|k| obj.get(*k))
        .map(intent_core::slim_body_size)
        .sum();
    let base = size.saturating_sub(bulk);
    // `actor.type` is required on the wire; a starved cap may drop it from
    // the actor preview, so it is restored after every pass (a tiny bounded
    // enum string — "user"/"agent"/"system"/"tool").
    let actor_type = obj.get("actor").and_then(|a| a.get("type")).cloned();
    obj.insert("truncated".to_string(), Value::Bool(true));
    obj.insert("originalBytes".to_string(), Value::from(size));
    // The trimmable fields share the allowance sequentially, small
    // identity-adjacent fields first (see EVENT_ROW_TRIM_FIELDS order).
    let mut allowance = share.saturating_sub(base);
    loop {
        let obj = row.as_object_mut().expect("still an object");
        let mut budget = allowance;
        for key in EVENT_ROW_TRIM_FIELDS {
            if let Some(v) = obj.get_mut(key) {
                let capped = intent_core::cap_json_value(v, &mut budget);
                *v = capped;
            }
        }
        if let (Some(t), Some(actor)) = (&actor_type, obj.get_mut("actor")) {
            if let Some(actor_obj) = actor.as_object_mut() {
                actor_obj.insert("type".to_string(), t.clone());
            }
        }
        let actual = intent_core::slim_body_size(row);
        if actual <= share || allowance == 0 {
            return actual;
        }
        // Escaping made the capped row serialize past its share: shrink the
        // allowance proportionally and re-cap (re-capping the already-capped
        // fields only shrinks them further). `actual > share` makes the new
        // allowance strictly smaller, so the loop converges.
        allowance = allowance * share / actual;
    }
}

#[cfg(test)]
mod tests;
