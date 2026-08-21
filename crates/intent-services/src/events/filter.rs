//! Subscription filter engine (§10).
//!
//! Pure, transport-agnostic matching ported wire-for-wire from
//! `~/src/intent/`: the event-type glob/exact rules in
//! `event-filter-engine.ts` + `agent-subscriptions/.../matching-saga.ts`
//! (`matchesFilter`), the bare-`*` expansion in `ws-event-api.ts`
//! (`resolveSubscriptionEventTypes`), and the `excludeSelf` / `batchWindow`
//! defaults of `ws.event.subscribe`. The richer bus that consumes these lives
//! in [`super::bus`].

use std::time::Duration;

use intent_core::{parse_iso, ActorType, Event};

/// Category wildcards that a bare `*` subscription expands to. Mirrors
/// `VALID_EVENT_CATEGORY_WILDCARDS` in `ws-event-api.ts`.
pub(crate) const VALID_EVENT_CATEGORY_WILDCARDS: &[&str] = &[
    "agent:*",
    "file:*",
    "task:*",
    "git:*",
    "note:*",
    "terminal:*",
    "test:*",
    "build:*",
    "workspace:*",
    "spec:*",
    "goal:*",
    "comment:*",
];

/// Category wildcards a bare `*` expands to for AGENT subscribers
/// (monorepo#1229): every entry of [`VALID_EVENT_CATEGORY_WILDCARDS`] except
/// `agent:*` — agent events are internal for agent callers, who watch other
/// agents via `ws.agent.watch` instead.
pub(crate) const AGENT_SUBSCRIBABLE_CATEGORY_WILDCARDS: &[&str] = &[
    "file:*",
    "task:*",
    "git:*",
    "note:*",
    "terminal:*",
    "test:*",
    "build:*",
    "workspace:*",
    "spec:*",
    "goal:*",
    "comment:*",
];

/// Whether an event type (or subscription pattern) is off-limits to
/// agent-owned event subscriptions (monorepo#1229): every `agent:`-prefixed
/// type — transcript/tool/stream traffic, lifecycle, and the observability
/// events the original TS hard-excluded as `INTERNAL_OBSERVABILITY_EVENTS` —
/// plus `chat:stream:delta`, the one high-volume streaming type outside the
/// `agent:` prefix. The prefix rule also covers the `agent:*` pattern itself.
pub(crate) fn is_agent_restricted_event_type(event_type: &str) -> bool {
    event_type.starts_with("agent:") || event_type == "chat:stream:delta"
}

/// Default coalescing window applied when a subscriber requests batching
/// without a value (TS `options.batchWindow || 500`).
pub(crate) const DEFAULT_BATCH_WINDOW: Duration = Duration::from_millis(500);

/// Criteria a [`super::bus::Subscription`] matches events against. Empty
/// collections / `None` fields are ignored (AND-combined like the TS filter).
#[derive(Debug, Clone, Default)]
pub struct SubscriptionFilter {
    /// Event-type patterns; empty matches every type. Each entry is an exact
    /// type or a `prefix:*` wildcard (see [`event_type_matches`]).
    pub event_types: Vec<String>,
    /// When set, `actor.id` must be one of these.
    pub actor_ids: Vec<String>,
    /// When set, events whose `actor.id` is listed are dropped (`excludeSelf`).
    pub exclude_actor_ids: Vec<String>,
    /// When set, `actor.type` must be one of these.
    pub actor_types: Vec<ActorType>,
    /// When set, drop events with a `timestamp` strictly before this instant.
    pub since: Option<String>,
    /// When set, scope to a single workspace (`createEventTypeSubscriptionFilters`).
    pub workspace_id: Option<String>,
    /// When set, coalesce matched events within this window (`batchWindow`).
    pub batch_window: Option<Duration>,
    /// When set, events failing [`is_agent_restricted_event_type`] never match
    /// — the match-time guard for agent-owned subscriptions (monorepo#1229),
    /// which also narrows rehydrated legacy `agent:*` rows. FE-facing
    /// subscriptions leave this `false` and keep the full stream.
    pub exclude_agent_events: bool,
}

impl SubscriptionFilter {
    /// Build the filter for `ws.event.subscribe(eventTypes, { excludeSelf,
    /// batchWindow })`. Bare `*` expands to the category wildcards;
    /// `exclude_self` (default `true` in the TS) drops the subscriber's own
    /// events; `batch_window` defaults to [`DEFAULT_BATCH_WINDOW`], and a
    /// zero window is treated as unset (TS `batchWindow || 500`).
    #[cfg(test)]
    pub(crate) fn for_subscriber(
        event_types: &[String],
        self_actor_id: Option<&str>,
        exclude_self: bool,
        batch_window: Option<Duration>,
    ) -> Self {
        let exclude_actor_ids = match (exclude_self, self_actor_id) {
            (true, Some(id)) => vec![id.to_string()],
            _ => Vec::new(),
        };
        let batch_window = match batch_window {
            Some(d) if !d.is_zero() => Some(d),
            _ => Some(DEFAULT_BATCH_WINDOW),
        };
        Self {
            event_types: resolve_event_types(event_types),
            exclude_actor_ids,
            batch_window,
            ..Default::default()
        }
    }
}

/// Expand subscription event types: a bare `*` becomes every entry of
/// [`VALID_EVENT_CATEGORY_WILDCARDS`]; all other entries pass through
/// unchanged. Mirrors `resolveSubscriptionEventTypes`.
pub(crate) fn resolve_event_types(event_types: &[String]) -> Vec<String> {
    let mut resolved = Vec::new();
    for ev in event_types {
        if ev == "*" {
            resolved.extend(
                VALID_EVENT_CATEGORY_WILDCARDS
                    .iter()
                    .map(std::string::ToString::to_string),
            );
        } else {
            resolved.push(ev.clone());
        }
    }
    resolved
}

/// [`resolve_event_types`] for AGENT subscribers (monorepo#1229): a bare `*`
/// silently narrows to [`AGENT_SUBSCRIBABLE_CATEGORY_WILDCARDS`] (no
/// `agent:*`); all other entries pass through unchanged — explicit restricted
/// types are rejected upstream at subscribe time, before resolution.
pub(crate) fn resolve_event_types_for_agent(event_types: &[String]) -> Vec<String> {
    let mut resolved = Vec::new();
    for ev in event_types {
        if ev == "*" {
            resolved.extend(
                AGENT_SUBSCRIBABLE_CATEGORY_WILDCARDS
                    .iter()
                    .map(std::string::ToString::to_string),
            );
        } else {
            resolved.push(ev.clone());
        }
    }
    resolved
}

/// Match one event type against one pattern. A `prefix:*` pattern matches any
/// type starting with `prefix:`; every other pattern is an exact match. Mirrors
/// the `matchesFilter` rule (`endsWith(":*")` → `startsWith(slice(0,-1))`).
pub fn event_type_matches(event_type: &str, pattern: &str) -> bool {
    if pattern.ends_with(":*") {
        let prefix = &pattern[..pattern.len() - 1];
        event_type.starts_with(prefix)
    } else {
        event_type == pattern
    }
}

/// Whether `event` satisfies every set criterion of `filter` (AND logic).
/// Field order and short-circuiting mirror the TS `matchesFilter`.
pub(crate) fn event_matches(filter: &SubscriptionFilter, event: &Event) -> bool {
    if filter.exclude_agent_events && is_agent_restricted_event_type(&event.event_type) {
        return false;
    }
    if !filter.event_types.is_empty()
        && !filter
            .event_types
            .iter()
            .any(|p| event_type_matches(&event.event_type, p))
    {
        return false;
    }
    if !filter.actor_ids.is_empty() {
        match &event.actor.id {
            Some(id) if filter.actor_ids.iter().any(|a| a == id) => {}
            _ => return false,
        }
    }
    if !filter.exclude_actor_ids.is_empty() {
        if let Some(id) = &event.actor.id {
            if filter.exclude_actor_ids.iter().any(|a| a == id) {
                return false;
            }
        }
    }
    if !filter.actor_types.is_empty() && !filter.actor_types.contains(&event.actor.actor_type) {
        return false;
    }
    if let Some(since) = &filter.since {
        let keep = match (parse_iso(&event.timestamp), parse_iso(since)) {
            (Some(ev), Some(s)) => ev >= s,
            _ => event.timestamp.as_str() >= since.as_str(),
        };
        if !keep {
            return false;
        }
    }
    if let Some(ws) = &filter.workspace_id {
        if &event.workspace_id.0 != ws {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{EventActor, WorkspaceId};
    use serde_json::json;

    fn event(event_type: &str, actor_id: Option<&str>, actor_type: ActorType) -> Event {
        Event {
            id: "01900000-0000-7000-8000-000000000000".to_string(),
            workspace_id: WorkspaceId::from("ws-1"),
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            event_type: event_type.to_string(),
            actor: EventActor {
                actor_type,
                id: actor_id.map(std::string::ToString::to_string),
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({}),
        }
    }

    #[test]
    fn type_glob_prefix_and_exact() {
        assert!(event_type_matches("agent:idle", "agent:*"));
        assert!(event_type_matches("agent:stream:activity", "agent:*"));
        assert!(!event_type_matches("file:changed", "agent:*"));
        assert!(event_type_matches("file:changed", "file:changed"));
        assert!(!event_type_matches("file:changed", "file:created"));
        // `:*` keeps the colon: `agent*` (no colon) is an exact match only.
        assert!(!event_type_matches("agent:idle", "agent*"));
    }

    #[test]
    fn bare_star_expands_to_category_wildcards() {
        let resolved = resolve_event_types(&["*".to_string()]);
        assert_eq!(resolved, VALID_EVENT_CATEGORY_WILDCARDS);
        let f = SubscriptionFilter {
            event_types: resolved,
            ..Default::default()
        };
        assert!(event_matches(
            &f,
            &event("agent:idle", None, ActorType::Agent)
        ));
        assert!(event_matches(
            &f,
            &event("file:changed", None, ActorType::System)
        ));
        // `mcp:notification` is not a category wildcard → no match.
        assert!(!event_matches(
            &f,
            &event("mcp:notification", None, ActorType::System)
        ));
    }

    #[test]
    fn agent_bare_star_narrows_to_non_agent_categories() {
        let resolved = resolve_event_types_for_agent(&["*".to_string()]);
        assert_eq!(resolved, AGENT_SUBSCRIBABLE_CATEGORY_WILDCARDS);
        assert!(!resolved.contains(&"agent:*".to_string()));
        // Non-star entries pass through unchanged (rejection happens upstream).
        let passthrough = resolve_event_types_for_agent(&["file:changed".to_string()]);
        assert_eq!(passthrough, vec!["file:changed".to_string()]);
    }

    #[test]
    fn agent_restricted_event_types() {
        // Every `agent:`-prefixed type (including the pattern itself and the
        // observability events) plus `chat:stream:delta` is restricted.
        for t in [
            "agent:message",
            "agent:tool:call",
            "agent:*",
            "agent:woken-by-subscription",
            "agent:subscriptions-restored",
            "chat:stream:delta",
        ] {
            assert!(is_agent_restricted_event_type(t), "{t} must be restricted");
        }
        for t in ["file:changed", "task:status-changed", "chat:stream"] {
            assert!(!is_agent_restricted_event_type(t), "{t} must be allowed");
        }
    }

    #[test]
    fn exclude_agent_events_guards_match_time() {
        // A rehydrated legacy `agent:*` subscription (agent-owned) never
        // matches agent events or `chat:stream:delta`, but still matches the
        // other categories it named.
        let f = SubscriptionFilter {
            event_types: vec!["agent:*".to_string(), "file:*".to_string()],
            exclude_agent_events: true,
            ..Default::default()
        };
        assert!(!event_matches(
            &f,
            &event("agent:message", Some("a"), ActorType::Agent)
        ));
        assert!(!event_matches(
            &f,
            &event("agent:tool:call", Some("a"), ActorType::Agent)
        ));
        assert!(!event_matches(
            &f,
            &event("chat:stream:delta", Some("a"), ActorType::Agent)
        ));
        assert!(event_matches(
            &f,
            &event("file:changed", Some("a"), ActorType::Agent)
        ));

        // FE-facing subscriptions (flag unset) keep the full agent stream.
        let fe = SubscriptionFilter {
            event_types: vec!["agent:*".to_string()],
            ..Default::default()
        };
        assert!(event_matches(
            &fe,
            &event("agent:message", Some("a"), ActorType::Agent)
        ));
    }

    #[test]
    fn empty_event_types_matches_all() {
        let f = SubscriptionFilter::default();
        assert!(event_matches(
            &f,
            &event("anything:happens", None, ActorType::User)
        ));
    }

    #[test]
    fn exclude_self_drops_same_actor() {
        let f = SubscriptionFilter::for_subscriber(
            &["agent:*".to_string()],
            Some("agent-7"),
            true,
            None,
        );
        assert_eq!(f.exclude_actor_ids, vec!["agent-7".to_string()]);
        assert_eq!(f.batch_window, Some(DEFAULT_BATCH_WINDOW));
        // Same actor → dropped; a different actor → kept; no actor id → kept.
        assert!(!event_matches(
            &f,
            &event("agent:idle", Some("agent-7"), ActorType::Agent)
        ));
        assert!(event_matches(
            &f,
            &event("agent:idle", Some("agent-9"), ActorType::Agent)
        ));
        assert!(event_matches(
            &f,
            &event("agent:idle", None, ActorType::Agent)
        ));
    }

    #[test]
    fn exclude_self_disabled_keeps_own_events() {
        let f = SubscriptionFilter::for_subscriber(
            &["agent:*".to_string()],
            Some("agent-7"),
            false,
            Some(Duration::from_millis(0)),
        );
        assert!(f.exclude_actor_ids.is_empty());
        // Zero window is treated as unset → default.
        assert_eq!(f.batch_window, Some(DEFAULT_BATCH_WINDOW));
        assert!(event_matches(
            &f,
            &event("agent:idle", Some("agent-7"), ActorType::Agent)
        ));
    }

    #[test]
    fn actor_type_and_since_filters() {
        let mut f = SubscriptionFilter {
            actor_types: vec![ActorType::Agent],
            ..Default::default()
        };
        assert!(event_matches(
            &f,
            &event("note:created", Some("a"), ActorType::Agent)
        ));
        assert!(!event_matches(
            &f,
            &event("note:created", Some("u"), ActorType::User)
        ));

        f.actor_types.clear();
        f.since = Some("2026-01-01T00:00:00Z".to_string());
        let mut older = event("note:created", None, ActorType::System);
        older.timestamp = "2025-12-31T23:59:59Z".to_string();
        assert!(!event_matches(&f, &older));
        let mut newer = event("note:created", None, ActorType::System);
        newer.timestamp = "2026-02-01T00:00:00Z".to_string();
        assert!(event_matches(&f, &newer));
    }
}
