//! Connection context: thread transport origin (UDS vs TCP) through request handling.
//!
//! Uses tokio task-local storage to carry connection origin from the transport
//! layer (`listener.rs`, `ws.rs`) through the router/dispatcher to service code
//! without adding parameters to every method. This lets `ServerControl::is_tcp_connection`
//! return the real value and guard against self-terminating stop calls from TCP clients.
//!
//! ## Invariant: Context Propagation Across Task Spawns
//!
//! The connection origin is established at the transport layer and MUST survive
//! into all spawned handler tasks:
//!
//! 1. **Transport establishes context** (`listener.rs`, `ws.rs`):
//!    - UDS wraps `process_frame` in `with_connection_context(false, ...)`
//!    - WSS wraps `process_frame` in `with_connection_context(true, ...)`
//!
//! 2. **Spawned tasks re-establish context**:
//!    - Before spawning, capture `is_tcp_connection()` from the current context
//!    - Wrap spawned work in `with_connection_context(is_tcp, ...)` with the captured value
//!    - This ensures the transport origin is visible to all code in the spawned task
//!    - All spawns in `conn.rs` follow this pattern: `host::handle`, `browser::handle`,
//!      `handle_message`, and the subscription forwarders (`spawn_forwarder`), whose
//!      seq-0 snapshot and delta re-reads run inside the scope for the task's lifetime
//!
//! 3. **Origin checks run within established context**:
//!    - `server.*` RPCs: inline on read loop, context guaranteed
//!    - `settings.update` WSS guard: runs inside spawned `handle_message` task with re-established context
//!
//! The fallback (`unwrap_or(true)`) is fail-closed: missing context is treated
//! as remote/untrusted. Request-handling paths are guaranteed to establish context;
//! other code (e.g., background tasks) may call this without established context.
//!
//! ## Caller binding
//!
//! The same scopes carry the request's [`Caller`] (`intent_core::caller`):
//! the transport binds it once per connection at admission (UDS → primary
//! principal, administrator; WSS → the principal the upgrade credential
//! resolved to) and [`with_request_context`] establishes both the origin flag
//! and the caller per frame. Spawn sites capture [`current_caller`] alongside
//! `is_tcp_connection()` and re-establish both. A connection whose principal
//! could not be resolved runs with no caller bound, which consumers treat as
//! forbidden (fail-closed).

use std::cell::RefCell;
use std::future::Future;
use std::sync::Arc;

use futures::future::Either;
use intent_core::repository_request::{
    RepositoryReadConnection, RepositoryReadRequestScope, RepositoryWireEntry,
};
pub use intent_core::{current_caller, with_caller, Caller};

tokio::task_local! {
    /// Connection origin for the current request task. Set by the transport layer
    /// (UDS sets `false`, WSS sets `true`) before spawning the request handler.
    /// Queried by `ServerControl::is_tcp_connection()` to enforce safety guards.
    static IS_TCP: RefCell<bool>;
    static READ_CONNECTION: Option<Arc<dyn RepositoryReadConnection>>;
    static SELECTION_FRAME: Option<intent_core::repository_request::RepositorySelectionFrame>;
    static REPOSITORY_FRAME: Option<intent_core::repository_request::RepositoryContextQuery>;
}

/// Bind only the new method's declared root while constructing the ORIGINAL
/// frame future. The synchronous carrier captures it before any slot wait.
pub(crate) fn with_repository_frame<T>(raw: &str, construct: impl FnOnce() -> T) -> T {
    use intent_core::repository_request::{RepositoryContextBoundQuery, RepositoryContextQuery};
    let query = serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|value| {
            let params = value.get("params")?.clone();
            match value.get("method")?.as_str()? {
                "workspace.repositoryContext.capture" => {
                    serde_json::from_value::<RepositoryContextQuery>(params).ok()
                }
                "workspace.repositoryContext" | "workspace.repositoryContext.release" => {
                    serde_json::from_value::<RepositoryContextBoundQuery>(params)
                        .ok()
                        .map(|query| RepositoryContextQuery {
                            workspace_id: query.workspace_id,
                            git_root_id: query.git_root_id,
                        })
                }
                _ => None,
            }
        });
    let selection = serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|value| {
            use intent_core::repository_request::RepositorySelectionFrame as Frame;
            let method = value.get("method")?.as_str()?;
            let params = value.get("params")?.clone();
            match method {
                "workspace.repositorySelection.capture" => {
                    serde_json::from_value(params).ok().map(Frame::Capture)
                }
                "workspace.repositorySelection.save" => {
                    serde_json::from_value(params).ok().map(Frame::Save)
                }
                "workspace.repositorySelection.reset" => {
                    serde_json::from_value(params).ok().map(Frame::Reset)
                }
                "workspace.repositorySelection.reconcile" => {
                    serde_json::from_value(params).ok().map(Frame::Reconcile)
                }
                "workspace.repositorySelection.release" => {
                    serde_json::from_value(params).ok().map(Frame::Release)
                }
                _ => None,
            }
        });
    SELECTION_FRAME.sync_scope(selection, || REPOSITORY_FRAME.sync_scope(query, construct))
}

/// Owned by each actual connection exit path, independently of client ids.
/// Clones are only used by the local reader/writer owners; either exit retires
/// the original cohort. Task-local clones carry the scope, not this guard.
#[derive(Clone)]
pub(crate) struct ReadConnectionGuard(Option<Arc<dyn RepositoryReadConnection>>);

impl ReadConnectionGuard {
    pub(crate) fn bind(api: &dyn intent_core::WorkspaceApi, entry: RepositoryWireEntry) -> Self {
        Self(api.repository_read_connection(entry))
    }

    /// Consume the original owner's bounded control feed on this socket only.
    pub(crate) fn forward_retirements(
        &self,
        output: tokio::sync::mpsc::Sender<String>,
    ) -> RetirementForwarder {
        let selection_output = output.clone();
        let task = self.0.as_ref().and_then(|owner| owner.take_retirements()).map(|mut receiver| {
            tokio::spawn(async move {
                while let Some(notice) = receiver.next().await {
                    let terminal = notice.terminal;
                    let frame = serde_json::json!({
                        "jsonrpc": "2.0", "method": "workspace.repositoryContext.retired", "params": notice,
                    }).to_string();
                    // Receiver loss closes this original feed and all its leases.
                    if !matches!(tokio::time::timeout(std::time::Duration::from_secs(5), output.send(frame)).await, Ok(Ok(()))) || terminal { break; }
                }
            })
        });
        let selection = self.0.as_ref().and_then(|owner| owner.take_selection_retirements()).map(|mut receiver| {
            tokio::spawn(async move {
                while let Some(notice) = receiver.next().await {
                    let terminal = notice.terminal;
                    let frame = serde_json::json!({"jsonrpc":"2.0","method":"workspace.repositorySelection.retired","params":notice}).to_string();
                    if !matches!(tokio::time::timeout(std::time::Duration::from_secs(5), selection_output.send(frame)).await, Ok(Ok(()))) || terminal { break; }
                }
            })
        });
        RetirementForwarder { task, selection }
    }

    pub(crate) fn absent() -> Self {
        Self(None)
    }

    pub(crate) fn retire(&self) {
        if let Some(owner) = &self.0 {
            owner.retire();
        }
    }

    pub(crate) fn run<F: Future>(&self, body: F) -> impl Future<Output = F::Output> {
        READ_CONNECTION.scope(self.0.clone(), body)
    }
}

pub(crate) struct RetirementForwarder {
    task: Option<tokio::task::JoinHandle<()>>,
    selection: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for RetirementForwarder {
    fn drop(&mut self) {
        for task in [&self.task, &self.selection].into_iter().flatten() {
            task.abort();
        }
    }
}

impl Drop for ReadConnectionGuard {
    fn drop(&mut self) {
        self.retire();
    }
}

/// Local listeners historically leave ordinary accepted tasks alive on stop.
/// Retain only weak read-owner allocations so that stop still closes their
/// qualified cohorts, including a bind completing after listener shutdown.
#[cfg(any(unix, windows))]
#[derive(Default)]
pub(crate) struct ReadConnectionOwners {
    state: std::sync::Mutex<ReadOwnerState>,
}

#[cfg(any(unix, windows))]
#[derive(Default)]
struct ReadOwnerState {
    closed: bool,
    owners: Vec<std::sync::Weak<dyn RepositoryReadConnection>>,
}

#[cfg(any(unix, windows))]
impl ReadConnectionOwners {
    pub(crate) fn register(&self, guard: &ReadConnectionGuard) {
        let Some(owner) = &guard.0 else { return };
        let closed = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.owners.retain(|owner| owner.strong_count() != 0);
            state.owners.push(Arc::downgrade(owner));
            state.closed
        };
        if closed {
            owner.retire();
        }
    }

    fn retire(&self) {
        let owners = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state
                .owners
                .iter()
                .filter_map(std::sync::Weak::upgrade)
                .collect::<Vec<_>>()
        };
        for owner in owners {
            owner.retire();
        }
    }
}

#[cfg(any(unix, windows))]
#[derive(Default)]
pub(crate) struct ReadListenerGuard(pub(crate) Arc<ReadConnectionOwners>);

#[cfg(any(unix, windows))]
impl Drop for ReadListenerGuard {
    fn drop(&mut self) {
        self.0.retire();
    }
}

struct RequestCompletion(Arc<dyn RepositoryReadRequestScope>);

impl Drop for RequestCompletion {
    fn drop(&mut self) {
        self.0.retire();
    }
}

/// A transport-owned completion lease; an escaped Core scope cannot extend it.
#[derive(Clone)]
pub(crate) struct CapturedFrame {
    is_tcp: bool,
    caller: Option<Caller>,
    credential: Option<intent_core::caller::WireCredential>,
    completion: Option<Arc<RequestCompletion>>,
}

impl CapturedFrame {
    /// This is synchronous, including construction of the completion guard.
    pub(crate) fn capture() -> Self {
        Self {
            is_tcp: is_tcp_connection(),
            caller: current_caller(),
            credential: intent_core::caller::current_wire_credential(),
            completion: READ_CONNECTION
                .try_with(|owner| {
                    owner.as_ref().and_then(|owner| {
                        if let Some(frame) = SELECTION_FRAME.try_with(Clone::clone).ok().flatten() {
                            return owner
                                .capture_selection(&frame)
                                .map(|scope| Arc::new(RequestCompletion(scope)));
                        }
                        let query = REPOSITORY_FRAME.try_with(Clone::clone).ok().flatten();
                        Some(Arc::new(RequestCompletion(match query {
                            Some(query) => owner.capture_context(&query),
                            None => owner.capture(),
                        })))
                    })
                })
                .ok()
                .flatten(),
        }
    }

    pub(crate) fn read_scope(&self) -> Option<&Arc<dyn RepositoryReadRequestScope>> {
        self.completion.as_ref().map(|completion| &completion.0)
    }

    pub(crate) async fn run<T: Send>(&self, body: impl Future<Output = T> + Send) -> T {
        with_credential_context(
            self.is_tcp,
            self.caller.clone(),
            self.credential.clone(),
            async {
                if let Some(scope) = self.read_scope() {
                    let mut result = None;
                    scope
                        .scope(Box::pin(async { result = Some(body.await) }))
                        .await;
                    result.expect("repository request scope must execute its body exactly once")
                } else {
                    body.await
                }
            },
        )
        .await
    }
}

/// Whether the current request is over a TCP transport (WSS). Returns `true`
/// (remote/untrusted) for TCP connections or when called outside a request
/// context (fail-closed). Thread-safe.
#[must_use]
pub fn is_tcp_connection() -> bool {
    IS_TCP.try_with(|cell| *cell.borrow()).unwrap_or(true)
}

/// Whether the current request is bound to a caller who does **not**
/// administer the daemon — a per-principal (collaborator) credential over the
/// wire. This is the predicate every owner-only transport gate keys on
/// (multiplayer w3).
///
/// An *unbound* request (`current_caller() == None`) is not non-administrator:
/// the only admission path that leaves a wire connection unbound is the
/// legacy administrator token when the composition root exposes no principal
/// store (test stubs), and agents / hooks never enter through the transport.
/// A per-principal credential always binds.
#[must_use]
pub fn is_non_administrator_caller() -> bool {
    current_caller().is_some_and(|caller| !caller.is_administrator())
}

/// Current host execution authority, independent of the connection's role
/// snapshot. Preserve the legacy unbound administrator test wiring.
pub(crate) async fn may_manage_workspaces(api: &dyn intent_core::WorkspaceApi) -> bool {
    if !is_non_administrator_caller() {
        return true;
    }
    match current_caller().and_then(|caller| caller.principal_id().cloned()) {
        Some(id) => api.principal_host_role(id).await.is_ok_and(|role| {
            matches!(
                role,
                intent_core::HostRole::Owner | intent_core::HostRole::Member
            )
        }),
        None => false,
    }
}

/// Run a future within a connection-context scope. The `is_tcp` flag will be
/// visible to all code running within `f` via `is_tcp_connection()`.
///
/// Returns the scope future directly (not an `async fn`): the wrapped
/// per-frame request futures are very large, and an `async fn` wrapper
/// would hold `f` twice (argument + scope), overflowing the tokio worker
/// stack in debug builds.
pub fn with_connection_context<F, R>(is_tcp: bool, f: F) -> impl Future<Output = R>
where
    F: Future<Output = R>,
{
    IS_TCP.scope(RefCell::new(is_tcp), f)
}

/// Run a future within a connection-context scope that also binds the
/// request's [`Caller`]. `None` establishes the origin flag only, leaving no
/// caller bound (fail-closed for principal-gated consumers).
pub fn with_request_context<F, R>(
    is_tcp: bool,
    caller: Option<Caller>,
    f: F,
) -> impl Future<Output = R>
where
    F: Future<Output = R>,
{
    let inner = match caller {
        Some(caller) => Either::Left(with_caller(caller, f)),
        None => Either::Right(f),
    };
    with_connection_context(is_tcp, inner)
}

/// Restore exact bearer admission when dispatch moves to a detached task.
pub(crate) fn with_credential_context<F: Future>(
    is_tcp: bool,
    caller: Option<Caller>,
    credential: Option<intent_core::caller::WireCredential>,
    future: F,
) -> impl Future<Output = F::Output> {
    intent_core::caller::with_wire_credential(
        credential,
        with_request_context(is_tcp, caller, future),
    )
}
