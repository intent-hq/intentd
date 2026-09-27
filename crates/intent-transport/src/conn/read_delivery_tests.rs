//! Disposable carrier fixtures. These are NOT Store/R/NativeRead authority.
//! Actual conn/router and socket paths consume this private Core implementation.

use std::sync::{Mutex, Weak};
use std::time::Duration;

use intent_core::caller::{current_wire_credential, WireCredential};
use intent_core::repository_request::{
    RepositoryReadConnection, RepositoryReadReplyKind, RepositoryReadRequestScope,
    RepositoryWireEntry,
};
use intent_core::{BoxFuture, Caller, Error, HostRole, PrincipalId};
use tokio::sync::Notify;

use super::*;
use crate::context::{with_credential_context, ReadConnectionGuard};
use crate::reverse::{PrimaryReverseRegistry, ReverseTransport};

tokio::task_local! {
    static FIXTURE_SCOPE: Arc<FixtureScope>;
}

#[derive(Default)]
pub(crate) struct Gate {
    pub(crate) entered: Notify,
    pub(crate) release: Notify,
}

impl Gate {
    async fn wait(&self) {
        self.entered.notify_one();
        self.release.notified().await;
    }
}

#[derive(Default)]
struct ScopeState {
    retired: bool,
    qualified: bool,
    public_service_error: bool,
    sent: bool,
    kinds: Vec<RepositoryReadReplyKind>,
}

pub(crate) struct FixtureScope {
    state: Arc<Mutex<ScopeState>>,
    caller: Option<Caller>,
    credential: Option<WireCredential>,
    delivery: Option<Arc<Gate>>,
}

impl RepositoryReadRequestScope for FixtureScope {
    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()> {
        // Fixture context clones share exact state, never reconstruct by RPC id.
        let captured = Arc::new(Self {
            state: self.state.clone(),
            caller: self.caller.clone(),
            credential: self.credential.clone(),
            delivery: self.delivery.clone(),
        });
        Box::pin(FIXTURE_SCOPE.scope(captured, body))
    }

    fn retire(&self) {
        self.state.lock().unwrap().retired = true;
    }

    fn deliver<'a>(
        &'a self,
        kind: RepositoryReadReplyKind,
        transfer: &'a mut (dyn FnMut() -> intent_core::Result<()> + Send),
    ) -> BoxFuture<'a, intent_core::Result<()>> {
        Box::pin(async move {
            if let Some(gate) = &self.delivery {
                gate.wait().await;
            }
            let mut state = self.state.lock().unwrap();
            state.kinds.push(kind);
            let ordinary_error =
                kind == RepositoryReadReplyKind::ServiceError && state.public_service_error;
            if state.sent || (state.qualified && state.retired && !ordinary_error) {
                return Err(Error::Forbidden(
                    "original read delivery unavailable".into(),
                ));
            }
            transfer()?;
            state.sent = true;
            Ok(())
        })
    }
}

#[derive(Default)]
struct Cohort {
    closed: bool,
    scopes: Vec<Weak<FixtureScope>>,
}

pub(crate) struct FixtureConnection {
    cohort: Mutex<Cohort>,
    scopes: Mutex<Vec<Arc<FixtureScope>>>,
    delivery: Option<Arc<Gate>>,
}

impl RepositoryReadConnection for FixtureConnection {
    fn capture(&self) -> Arc<dyn RepositoryReadRequestScope> {
        let mut cohort = self.cohort.lock().unwrap();
        let scope = Arc::new(FixtureScope {
            state: Arc::new(Mutex::new(ScopeState {
                retired: cohort.closed,
                ..ScopeState::default()
            })),
            caller: intent_core::current_caller(),
            credential: current_wire_credential(),
            delivery: self.delivery.clone(),
        });
        cohort.scopes.push(Arc::downgrade(&scope));
        self.scopes.lock().unwrap().push(scope.clone());
        scope
    }

    fn retire(&self) {
        let scopes = {
            let mut cohort = self.cohort.lock().unwrap();
            cohort.closed = true;
            cohort
                .scopes
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        for scope in scopes {
            scope.retire();
        }
    }
}

#[derive(Default)]
pub(crate) struct FixtureApi {
    pub(crate) connections: Mutex<Vec<Arc<FixtureConnection>>>,
    pub(crate) entries: Mutex<Vec<RepositoryWireEntry>>,
    pub(crate) permission: Option<Arc<Gate>>,
    pub(crate) handler: Option<Arc<Gate>>,
    pub(crate) delivery: Option<Arc<Gate>>,
    pub(crate) principal: PrincipalId,
}

impl WorkspaceApi for FixtureApi {
    fn repository_read_connection(
        &self,
        entry: RepositoryWireEntry,
    ) -> Option<Arc<dyn RepositoryReadConnection>> {
        let connection = Arc::new(FixtureConnection {
            cohort: Mutex::default(),
            scopes: Mutex::default(),
            delivery: self.delivery.clone(),
        });
        self.entries.lock().unwrap().push(entry);
        self.connections.lock().unwrap().push(connection.clone());
        Some(connection)
    }

    fn primary_principal_id(&self) -> BoxFuture<'_, intent_core::Result<PrincipalId>> {
        Box::pin(async { Ok(self.principal.clone()) })
    }

    fn principal_host_role(
        &self,
        _id: PrincipalId,
    ) -> BoxFuture<'_, intent_core::Result<HostRole>> {
        Box::pin(async {
            if let Some(gate) = &self.permission {
                gate.wait().await;
            }
            Ok(HostRole::Member)
        })
    }

    fn settings_get(&self, path: String) -> BoxFuture<'_, intent_core::Result<Value>> {
        Box::pin(async move {
            let scope = FIXTURE_SCOPE.try_with(Clone::clone).ok();
            if let Some(scope) = &scope {
                assert_eq!(intent_core::current_caller(), scope.caller);
                assert_eq!(
                    current_wire_credential().map(|value| value.principal_id().clone()),
                    scope
                        .credential
                        .as_ref()
                        .map(|value| value.principal_id().clone())
                );
                let mut state = scope.state.lock().unwrap();
                state.qualified = path != "ordinary";
                state.public_service_error = path == "provider-error";
            }
            if let Some(gate) = &self.handler {
                gate.wait().await;
            }
            match path.as_str() {
                "private-error" => Err(Error::Internal("private service error".into())),
                "provider-error" => Err(Error::Forbidden("actual retained provider error".into())),
                "panic" => panic!("private handler panic"),
                "oversized" => Ok(json!("x".repeat(crate::MAX_OUTBOUND_MESSAGE_BYTES + 1))),
                _ => Ok(
                    json!({"private": "original payload", "error": "a successful value", "scoped": scope.is_some()}),
                ),
            }
        })
    }
}

struct Harness {
    api: Arc<FixtureApi>,
    bus: EventBus,
    tx: OutboundSender,
    rx: OutboundReceiver,
    connection: ReadConnectionGuard,
    caller: Caller,
    credential: WireCredential,
    limiter: RpcLimiter,
    _dir: tempfile::TempDir,
}

impl Harness {
    async fn new(api: FixtureApi, role: HostRole) -> Self {
        let dir = tempfile::Builder::new()
            .prefix("request-carrier-")
            .tempdir()
            .unwrap();
        let store = intent_store::Store::open(&dir.path().join("bus.db"))
            .await
            .unwrap();
        let api = Arc::new(api);
        let caller = Caller::Wire {
            principal_id: api.principal.clone(),
            host_role: role,
        };
        let credential = WireCredential::Principal {
            principal_id: api.principal.clone(),
            token_hash: "fixture-original-hash".into(),
        };
        let connection = with_credential_context(
            true,
            Some(caller.clone()),
            Some(credential.clone()),
            async { ReadConnectionGuard::bind(api.as_ref(), RepositoryWireEntry::Bearer) },
        )
        .await;
        let (tx, rx) = outbound_channel();
        Self {
            api,
            bus: EventBus::new(store),
            tx,
            rx,
            connection,
            caller,
            credential,
            limiter: RpcLimiter::new(1),
            _dir: dir,
        }
    }

    async fn dispatch(&self, raw: &str) -> bool {
        let api: Arc<dyn WorkspaceApi> = self.api.clone();
        let reverse = ReverseChannel::new(self.tx.priority_sender());
        let registry = Arc::new(PrimaryReverseRegistry::new());
        let guard = registry.register(reverse.clone(), ReverseTransport::Wss);
        with_credential_context(
            true,
            Some(self.caller.clone()),
            Some(self.credential.clone()),
            self.connection.run(async {
                process_frame(
                    raw,
                    &api,
                    &self.bus,
                    &self.tx,
                    &mut ConnSubs::default(),
                    &mut ForwardRegistry::default(),
                    &reverse,
                    &guard,
                    None,
                    None,
                    &mut None,
                    false,
                    &self.limiter,
                )
                .await
            }),
        )
        .await
    }

    async fn response(&mut self) -> Value {
        let frame = tokio::time::timeout(Duration::from_secs(5), self.rx.recv_priority())
            .await
            .unwrap()
            .unwrap();
        serde_json::from_str(&frame).unwrap()
    }

    fn original(&self) -> Arc<FixtureConnection> {
        self.api.connections.lock().unwrap()[0].clone()
    }
}

fn request(path: &str) -> String {
    json!({"jsonrpc":"2.0","id":7,"method":"settings.get","params":{"path":path}}).to_string()
}

#[tokio::test]
async fn original_capture_precedes_the_first_permission_await() {
    let permission = Arc::new(Gate::default());
    let h = Harness::new(
        FixtureApi {
            permission: Some(permission.clone()),
            ..FixtureApi::default()
        },
        HostRole::Guest,
    )
    .await;
    let raw = request("private");
    let call = h.dispatch(&raw);
    tokio::pin!(call);
    tokio::select! {
        result = &mut call => panic!("permission did not wait: {result}"),
        () = permission.entered.notified() => {}
    }
    assert_eq!(
        h.original().scopes.lock().unwrap().len(),
        1,
        "original request must exist before permission lookup"
    );
}

#[tokio::test]
async fn original_retirement_prevents_the_prepared_private_response() {
    let handler = Arc::new(Gate::default());
    let mut h = Harness::new(
        FixtureApi {
            handler: Some(handler.clone()),
            ..FixtureApi::default()
        },
        HostRole::Owner,
    )
    .await;
    assert!(h.dispatch(&request("private")).await);
    handler.entered.notified().await;
    h.connection.retire();
    handler.release.notify_one();
    let reply = h.response().await;
    assert!(
        reply.get("result").is_none(),
        "private response escaped original retirement: {reply}"
    );
    assert!(!reply.to_string().contains("original payload"));
}
