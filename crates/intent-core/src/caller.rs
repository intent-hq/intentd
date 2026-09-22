//! Request caller binding (multiplayer w1).
//!
//! Every request the daemon services is bound to a [`Caller`] carried in
//! tokio task-local storage, established by the entry point that admits the
//! request and visible to service code without threading a parameter through
//! every method (the same mechanism as the transport's `IS_TCP` origin flag):
//!
//! - UDS listener: `Wire` for the primary principal, administrator.
//! - WSS listener: `Wire` for the principal the upgrade credential resolved
//!   to; identity is bound at upgrade, never taken from `client.hello`.
//! - `workspace_api` MCP tool: `Agent` for the calling agent session.
//! - Hook runner: `Daemon`.
//!
//! [`current_caller`] returns `None` outside an established scope. This is
//! fail-closed: consumers treat an absent caller as forbidden, never as the
//! primary user. Code that spawns work on behalf of a request must capture
//! the caller first and re-establish it inside the spawned task with
//! [`with_caller`].

use std::future::Future;

use crate::ids::{AgentId, PrincipalId};

/// Who a request is being serviced for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    /// A client connection (UDS or WSS) bound to a principal at admission.
    Wire {
        principal_id: PrincipalId,
        /// Whether the principal administers this daemon (the primary user).
        is_administrator: bool,
    },
    /// An agent session calling back through the `workspace_api` bridge.
    Agent { agent_id: AgentId },
    /// Daemon-internal background work (hook runs).
    Daemon,
}

impl Caller {
    /// The bound principal for a wire caller; `None` for agents and the
    /// daemon, which act on their own authority rather than a person's.
    #[must_use]
    pub fn principal_id(&self) -> Option<&PrincipalId> {
        match self {
            Caller::Wire { principal_id, .. } => Some(principal_id),
            Caller::Agent { .. } | Caller::Daemon => None,
        }
    }

    /// Whether the caller administers the daemon: the primary user over the
    /// wire, or the daemon itself. Agents are not administrators.
    #[must_use]
    pub fn is_administrator(&self) -> bool {
        match self {
            Caller::Wire {
                is_administrator, ..
            } => *is_administrator,
            Caller::Daemon => true,
            Caller::Agent { .. } => false,
        }
    }
}

tokio::task_local! {
    /// The caller bound to the current request task.
    static CALLER: Caller;
}

/// The caller bound to the current task, or `None` when no entry point
/// established one (fail-closed: treat as forbidden).
#[must_use]
pub fn current_caller() -> Option<Caller> {
    CALLER.try_with(Clone::clone).ok()
}

/// Run `f` with `caller` bound; visible to everything awaited inside `f` via
/// [`current_caller`]. Nested scopes shadow the outer binding.
///
/// Returns the scope future directly rather than being an `async fn`: an
/// `async fn` wrapper would hold `f` both as its argument and inside the
/// scope, doubling the state size of the (very large) request futures this
/// wraps and overflowing the worker stack in debug builds.
pub fn with_caller<F, R>(caller: Caller, f: F) -> impl Future<Output = R>
where
    F: Future<Output = R>,
{
    CALLER.scope(caller, f)
}

/// `tokio::spawn` for daemon-internal background work: the spawned task runs
/// with [`Caller::Daemon`] bound, so the capability gates it reaches (event
/// fan-out, refreshers, timers, finalisers) see the daemon rather than an
/// unbound — and therefore refused — request. Work spawned *on behalf of* a
/// request keeps that request's caller instead: capture [`current_caller`]
/// and re-establish it with [`with_caller`].
pub fn spawn_daemon<F>(f: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(with_caller(Caller::Daemon, f))
}

/// Who a queued-message entry is attributed to under the per-user queue
/// visibility rule (multiplayer): the three tiers of the `agent.getQueue`
/// contract, resolved by [`queue_attribution_with`] from the entry's
/// `messageMetadata` plus the workspace author fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueAttribution {
    /// A person: the entry's principal stamp ([`crate::FROM_PRINCIPAL_ID_KEY`]),
    /// else the workspace fallback author of an unstamped human-origin entry.
    Principal(PrincipalId),
    /// An unstamped entry of human origin (a legacy pre-attribution row)
    /// whose workspace fallback could not be resolved (no owner / legacy
    /// author, or the read failed): SOMEONE wrote it, nobody knows who.
    /// Fails closed — withheld from every non-administrator wire caller,
    /// never surfaced to a guest as author-less.
    UnknownHuman,
    /// No human author at all: an agent-sent or automatic (hook / monitor /
    /// system) entry. Public to every caller.
    Unattributed,
}

/// `true` when an unstamped queue entry's `messageMetadata` still reads as
/// human-authored — the same rule the fe applies to transcript rows: an
/// entry is agent/automatic origin iff its metadata is an object with a
/// string `type` (other than the user-authored `question_answers` wizard
/// tag), a non-empty `fromAgentId`, or `source == "system"`. Absent or
/// non-object metadata reads as human (a legacy typed message).
#[must_use]
pub fn is_human_authored_metadata(message_metadata: Option<&serde_json::Value>) -> bool {
    let Some(serde_json::Value::Object(obj)) = message_metadata else {
        return true;
    };
    match obj.get("type").and_then(serde_json::Value::as_str) {
        Some("question_answers") => return true,
        Some(_) => return false,
        None => {}
    }
    if obj
        .get("fromAgentId")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|id| !id.trim().is_empty())
    {
        return false;
    }
    obj.get("source").and_then(serde_json::Value::as_str) != Some("system")
}

/// The attribution of a queue entry with `metadata`, given the workspace
/// author `fallback` already resolved (`None` when the workspace has none or
/// the read failed): its stamp, else `fallback` for an unstamped entry whose
/// metadata still reads as human-authored ([`is_human_authored_metadata`]) —
/// [`QueueAttribution::UnknownHuman`] when that fallback is missing — else
/// [`QueueAttribution::Unattributed`]. Synchronous so a mutation can evaluate
/// it under the queue lock against the entry it is about to touch.
#[must_use]
pub fn queue_attribution_with(
    metadata: Option<&serde_json::Value>,
    fallback: Option<&PrincipalId>,
) -> QueueAttribution {
    match crate::lift_from_principal_id(metadata) {
        Some(id) => QueueAttribution::Principal(id),
        None if is_human_authored_metadata(metadata) => fallback
            .cloned()
            .map_or(QueueAttribution::UnknownHuman, QueueAttribution::Principal),
        None => QueueAttribution::Unattributed,
    }
}

/// Whether a queue entry with `attribution` may be shown to `caller`. A
/// non-administrator wire principal (a guest collaborator) sees only entries
/// attributed to itself plus [`QueueAttribution::Unattributed`] ones — an
/// [`QueueAttribution::UnknownHuman`] entry is withheld like a foreign one;
/// the administrator (workspace owner), agents and the daemon see the full
/// queue. The one predicate behind `agent.getQueue`, the
/// `agent:queue:updated` / `agent:queue:processing` projections and the
/// per-id mutation gate.
#[must_use]
pub fn queue_attribution_visible_to(caller: &Caller, attribution: &QueueAttribution) -> bool {
    let Caller::Wire {
        principal_id,
        is_administrator: false,
    } = caller
    else {
        return true;
    };
    match attribution {
        QueueAttribution::Principal(author) => author == principal_id,
        QueueAttribution::UnknownHuman => false,
        QueueAttribution::Unattributed => true,
    }
}

/// The attribution of a queued-message entry in wire shape (`author` already
/// attached by the serve-time resolver): an `author` object with a string
/// `principalId` is that principal; otherwise the entry is re-read from its
/// own `messageMetadata` with NO fallback — a stamp still names its
/// principal, an unstamped human-origin entry the resolver left author-less
/// (or a malformed `author`) is an unknown human, and only an agent-sent /
/// automatic entry is unattributed.
#[must_use]
pub fn queue_entry_attribution(entry: &serde_json::Value) -> QueueAttribution {
    if let Some(author) = entry
        .get("author")
        .and_then(|a| a.get("principalId"))
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
    {
        return QueueAttribution::Principal(PrincipalId(author.to_string()));
    }
    queue_attribution_with(entry.get("messageMetadata"), None)
}

/// [`queue_attribution_visible_to`] over a queued-message entry in wire
/// shape ([`queue_entry_attribution`]).
#[must_use]
pub fn queue_visible_to(caller: &Caller, entry: &serde_json::Value) -> bool {
    queue_attribution_visible_to(caller, &queue_entry_attribution(entry))
}

/// `metadata` key of an `agent:queue:processing` event marking the drained
/// entry as an [`QueueAttribution::UnknownHuman`] (`true`), so the transport
/// can redact the frame's `content` for a non-administrator wire subscriber
/// without a principal to stamp; a principal-attributed entry is stamped
/// under [`crate::FROM_PRINCIPAL_ID_KEY`] instead, an unattributed one
/// carries neither.
pub const QUEUE_AUTHOR_UNKNOWN_HUMAN_KEY: &str = "queueAuthorUnknownHuman";

/// The event `metadata` an `agent:queue:processing` publisher stamps for a
/// drained entry with `attribution` (see [`QUEUE_AUTHOR_UNKNOWN_HUMAN_KEY`]);
/// `None` for an unattributed entry.
#[must_use]
pub fn queue_processing_event_metadata(
    attribution: &QueueAttribution,
) -> Option<serde_json::Value> {
    match attribution {
        QueueAttribution::Principal(author) => {
            Some(serde_json::json!({ crate::FROM_PRINCIPAL_ID_KEY: author.0 }))
        }
        QueueAttribution::UnknownHuman => {
            Some(serde_json::json!({ QUEUE_AUTHOR_UNKNOWN_HUMAN_KEY: true }))
        }
        QueueAttribution::Unattributed => None,
    }
}

/// Inverse of [`queue_processing_event_metadata`]: the drained entry's
/// attribution read back from an `agent:queue:processing` event's `metadata`.
#[must_use]
pub fn queue_processing_event_attribution(
    metadata: Option<&serde_json::Value>,
) -> QueueAttribution {
    if let Some(author) = crate::lift_from_principal_id(metadata) {
        return QueueAttribution::Principal(author);
    }
    if metadata
        .and_then(|m| m.get(QUEUE_AUTHOR_UNKNOWN_HUMAN_KEY))
        .and_then(serde_json::Value::as_bool)
        == Some(true)
    {
        return QueueAttribution::UnknownHuman;
    }
    QueueAttribution::Unattributed
}

/// Egress projection of a queue snapshot for `caller`: drops the entries
/// [`queue_visible_to`] hides, keeping drain order and the entries'
/// `position` values as they are (no renumbering). `None` (no bound caller)
/// filters nothing.
#[must_use]
pub fn project_queue_for_caller(
    caller: Option<&Caller>,
    queue: Vec<serde_json::Value>,
) -> Vec<serde_json::Value> {
    match caller {
        Some(
            caller @ Caller::Wire {
                is_administrator: false,
                ..
            },
        ) => queue
            .into_iter()
            .filter(|entry| queue_visible_to(caller, entry))
            .collect(),
        Some(Caller::Wire { .. } | Caller::Agent { .. } | Caller::Daemon) | None => queue,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FROM_PRINCIPAL_ID_KEY;
    use serde_json::{json, Value};

    fn wire(admin: bool) -> Caller {
        Caller::Wire {
            principal_id: PrincipalId("p-1".into()),
            is_administrator: admin,
        }
    }

    fn entry(id: &str, position: u64, author: &Value) -> Value {
        json!({ "id": id, "content": id, "position": position, "author": author })
    }

    fn agent_metadata() -> Value {
        json!({ "type": "agent_message", "fromAgentId": "agent-1" })
    }

    fn mixed_queue() -> Vec<Value> {
        let mut agent = entry("agent", 2, &Value::Null);
        agent["messageMetadata"] = agent_metadata();
        let mut system = json!({ "id": "system", "content": "no author key", "position": 3 });
        system["messageMetadata"] = json!({ "source": "system" });
        vec![
            entry("own", 0, &json!({ "principalId": "p-1", "login": "me" })),
            entry(
                "foreign",
                1,
                &json!({ "principalId": "p-2", "login": "other" }),
            ),
            agent,
            system,
            entry("unknown-human", 4, &Value::Null),
        ]
    }

    fn ids(queue: &[Value]) -> Vec<&str> {
        queue.iter().map(|e| e["id"].as_str().unwrap()).collect()
    }

    #[test]
    fn guest_sees_own_and_unattributed_entries_only() {
        let guest = wire(false);
        let queue = mixed_queue();
        assert!(queue_visible_to(&guest, &queue[0]), "own entry");
        assert!(!queue_visible_to(&guest, &queue[1]), "foreign entry");
        assert!(
            queue_visible_to(&guest, &queue[2]),
            "agent-sent, null author"
        );
        assert!(
            queue_visible_to(&guest, &queue[3]),
            "system, absent author key"
        );
        assert!(
            !queue_visible_to(&guest, &queue[4]),
            "unstamped human entry the resolver could not attribute"
        );

        let projected = project_queue_for_caller(Some(&guest), queue);
        assert_eq!(ids(&projected), ["own", "agent", "system"]);
        let positions: Vec<u64> = projected
            .iter()
            .map(|e| e["position"].as_u64().unwrap())
            .collect();
        assert_eq!(positions, [0, 2, 3], "positions are not renumbered");
    }

    #[test]
    fn administrator_agent_daemon_and_unbound_callers_see_everything() {
        let admin = wire(true);
        let agent = Caller::Agent {
            agent_id: AgentId("a-1".into()),
        };
        for e in mixed_queue() {
            assert!(queue_visible_to(&admin, &e), "admin: {e}");
            assert!(queue_visible_to(&agent, &e), "agent: {e}");
            assert!(queue_visible_to(&Caller::Daemon, &e), "daemon: {e}");
        }
        for caller in [Some(&admin), Some(&agent), Some(&Caller::Daemon), None] {
            assert_eq!(
                project_queue_for_caller(caller, mixed_queue()),
                mixed_queue(),
                "{caller:?}"
            );
        }
    }

    #[test]
    fn attribution_resolves_in_three_tiers() {
        let p1 = PrincipalId("p-1".into());
        let p2 = PrincipalId("p-2".into());
        let stamped = json!({ FROM_PRINCIPAL_ID_KEY: "p-2" });
        assert_eq!(
            queue_attribution_with(Some(&stamped), Some(&p1)),
            QueueAttribution::Principal(p2.clone()),
            "the stamp wins over the fallback"
        );
        assert_eq!(
            queue_attribution_with(None, Some(&p1)),
            QueueAttribution::Principal(p1),
            "unstamped human falls back to the workspace author"
        );
        assert_eq!(
            queue_attribution_with(None, None),
            QueueAttribution::UnknownHuman,
            "unstamped human with no resolvable fallback fails closed"
        );
        let answers = json!({ "type": "question_answers" });
        assert_eq!(
            queue_attribution_with(Some(&answers), None),
            QueueAttribution::UnknownHuman,
            "the wizard answer tag is user-authored"
        );
        for md in [
            agent_metadata(),
            json!({ "source": "system" }),
            json!({ "type": "event_notification" }),
        ] {
            assert_eq!(
                queue_attribution_with(Some(&md), None),
                QueueAttribution::Unattributed,
                "{md}"
            );
        }
    }

    #[test]
    fn attribution_predicate_fails_closed_on_unknown_human() {
        let guest = wire(false);
        let own = QueueAttribution::Principal(PrincipalId("p-1".into()));
        let foreign = QueueAttribution::Principal(PrincipalId("p-2".into()));
        assert!(queue_attribution_visible_to(&guest, &own), "own");
        assert!(!queue_attribution_visible_to(&guest, &foreign), "foreign");
        assert!(
            !queue_attribution_visible_to(&guest, &QueueAttribution::UnknownHuman),
            "unknown human"
        );
        assert!(
            queue_attribution_visible_to(&guest, &QueueAttribution::Unattributed),
            "unattributed"
        );
        for caller in [
            wire(true),
            Caller::Agent {
                agent_id: AgentId("a-1".into()),
            },
            Caller::Daemon,
        ] {
            assert!(
                queue_attribution_visible_to(&caller, &foreign),
                "{caller:?}"
            );
            assert!(
                queue_attribution_visible_to(&caller, &QueueAttribution::UnknownHuman),
                "{caller:?}"
            );
        }
    }

    #[test]
    fn malformed_author_falls_back_to_the_entry_metadata() {
        let guest = wire(false);
        for e in [
            json!({ "id": "s", "author": "p-2" }),
            json!({ "id": "n", "author": { "principalId": 7 } }),
            json!({ "id": "e", "author": {} }),
            json!({ "id": "b", "author": { "principalId": "" } }),
        ] {
            assert!(!queue_visible_to(&guest, &e), "human origin, no stamp: {e}");
        }
        assert!(
            queue_visible_to(
                &guest,
                &json!({ "id": "a", "author": {}, "messageMetadata": agent_metadata() })
            ),
            "agent-sent stays public whatever `author` reads"
        );
        assert!(
            queue_visible_to(
                &guest,
                &json!({ "id": "m", "author": Value::Null,
                    "messageMetadata": { FROM_PRINCIPAL_ID_KEY: "p-1" } })
            ),
            "own stamp on the entry metadata"
        );
        assert!(
            !queue_visible_to(
                &guest,
                &json!({ "id": "f", "author": Value::Null,
                    "messageMetadata": { FROM_PRINCIPAL_ID_KEY: "p-2" } })
            ),
            "foreign stamp on the entry metadata"
        );
    }

    #[test]
    fn processing_event_metadata_round_trips_the_attribution() {
        for attribution in [
            QueueAttribution::Principal(PrincipalId("p-2".into())),
            QueueAttribution::UnknownHuman,
            QueueAttribution::Unattributed,
        ] {
            let metadata = queue_processing_event_metadata(&attribution);
            assert_eq!(
                queue_processing_event_attribution(metadata.as_ref()),
                attribution,
                "{metadata:?}"
            );
        }
        assert_eq!(
            queue_processing_event_metadata(&QueueAttribution::Unattributed),
            None
        );
        assert_eq!(
            queue_processing_event_attribution(Some(
                &json!({ QUEUE_AUTHOR_UNKNOWN_HUMAN_KEY: "yes" })
            )),
            QueueAttribution::Unattributed,
            "only a literal `true` marks an unknown human"
        );
    }

    #[tokio::test]
    async fn absent_caller_is_none() {
        assert_eq!(current_caller(), None);
    }

    #[tokio::test]
    async fn scope_binds_and_restores() {
        let seen = with_caller(wire(true), async { current_caller() }).await;
        assert_eq!(seen, Some(wire(true)));
        assert_eq!(current_caller(), None);
    }

    #[tokio::test]
    async fn nested_scope_shadows_outer() {
        let (inner, outer) = with_caller(wire(false), async {
            let inner = with_caller(Caller::Daemon, async { current_caller() }).await;
            (inner, current_caller())
        })
        .await;
        assert_eq!(inner, Some(Caller::Daemon));
        assert_eq!(outer, Some(wire(false)));
    }

    #[tokio::test]
    async fn spawned_task_does_not_inherit_without_reestablish() {
        let seen = with_caller(Caller::Daemon, async {
            tokio::spawn(async { current_caller() }).await.unwrap()
        })
        .await;
        assert_eq!(seen, None);
    }

    #[tokio::test]
    async fn spawn_daemon_binds_the_daemon_caller() {
        let seen = spawn_daemon(async { current_caller() }).await.unwrap();
        assert_eq!(seen, Some(Caller::Daemon));
        assert_eq!(current_caller(), None);
    }

    #[test]
    fn accessors() {
        assert_eq!(wire(true).principal_id(), Some(&PrincipalId("p-1".into())));
        assert!(wire(true).is_administrator());
        assert!(!wire(false).is_administrator());
        assert!(Caller::Daemon.is_administrator());
        let agent = Caller::Agent {
            agent_id: AgentId("a-1".into()),
        };
        assert!(!agent.is_administrator());
        assert_eq!(agent.principal_id(), None);
    }
}
