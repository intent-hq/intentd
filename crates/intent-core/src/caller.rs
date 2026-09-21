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

#[cfg(test)]
mod tests {
    use super::*;

    fn wire(admin: bool) -> Caller {
        Caller::Wire {
            principal_id: PrincipalId("p-1".into()),
            is_administrator: admin,
        }
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
