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

/// Whether a queued-message entry (wire shape, `author` already attached)
/// may be shown to `caller`. A non-administrator wire principal (a guest
/// collaborator) sees only entries it authored: an entry whose `author` is
/// an object with a `principalId` other than the caller's is hidden. Entries
/// with no `author` / `author: null` (agent-sent and automatic entries) stay
/// visible to everyone; the administrator (workspace owner), agents and the
/// daemon see the full queue.
#[must_use]
pub fn queue_visible_to(caller: &Caller, entry: &serde_json::Value) -> bool {
    let Caller::Wire {
        principal_id,
        is_administrator: false,
    } = caller
    else {
        return true;
    };
    match entry.get("author").and_then(|a| a.get("principalId")) {
        Some(serde_json::Value::String(author)) => author == &principal_id.0,
        _ => true,
    }
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

    fn mixed_queue() -> Vec<Value> {
        vec![
            entry("own", 0, &json!({ "principalId": "p-1", "login": "me" })),
            entry(
                "foreign",
                1,
                &json!({ "principalId": "p-2", "login": "other" }),
            ),
            entry("agent", 2, &Value::Null),
            json!({ "id": "no-key", "content": "no author key", "position": 3 }),
        ]
    }

    fn ids(queue: &[Value]) -> Vec<&str> {
        queue.iter().map(|e| e["id"].as_str().unwrap()).collect()
    }

    #[test]
    fn guest_sees_own_and_unauthored_entries_only() {
        let guest = wire(false);
        let queue = mixed_queue();
        assert!(queue_visible_to(&guest, &queue[0]), "own entry");
        assert!(!queue_visible_to(&guest, &queue[1]), "foreign entry");
        assert!(queue_visible_to(&guest, &queue[2]), "null author");
        assert!(queue_visible_to(&guest, &queue[3]), "absent author key");

        let projected = project_queue_for_caller(Some(&guest), queue);
        assert_eq!(ids(&projected), ["own", "agent", "no-key"]);
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
    fn malformed_author_is_kept() {
        let guest = wire(false);
        assert!(queue_visible_to(
            &guest,
            &json!({ "id": "s", "author": "p-2" })
        ));
        assert!(queue_visible_to(
            &guest,
            &json!({ "id": "n", "author": { "principalId": 7 } })
        ));
        assert!(queue_visible_to(
            &guest,
            &json!({ "id": "e", "author": {} })
        ));
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
