//! Disposable carrier fixtures without Store or repository read authority.
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
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent fixture observations of qualification, error policy, retirement and transfer"
)]
struct ScopeState {
    retired: bool,
    qualified: bool,
    public_service_error: bool,
    sent: bool,
    kinds: Vec<RepositoryReadReplyKind>,
}

#[derive(Clone, Copy, Default)]
enum DeliveryFault {
    #[default]
    None,
    Construction,
    BeforeTransfer,
    AfterTransfer,
    Repeat,
    Omit,
}

pub(crate) struct FixtureScope {
    state: Arc<Mutex<ScopeState>>,
    caller: Option<Caller>,
    credential: Option<WireCredential>,
    delivery: Option<Arc<Gate>>,
    fault: DeliveryFault,
}

impl RepositoryReadRequestScope for FixtureScope {
    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()> {
        // Fixture context clones share exact state, never reconstruct by RPC id.
        let captured = Arc::new(Self {
            state: self.state.clone(),
            caller: self.caller.clone(),
            credential: self.credential.clone(),
            delivery: self.delivery.clone(),
            fault: self.fault,
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
        assert!(
            !matches!(self.fault, DeliveryFault::Construction),
            "fixture delivery construction panic"
        );
        Box::pin(async move {
            assert!(
                !matches!(self.fault, DeliveryFault::BeforeTransfer),
                "fixture delivery future panic"
            );
            if matches!(self.fault, DeliveryFault::Omit) {
                return Ok(());
            }
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
            drop(state);
            assert!(
                !matches!(self.fault, DeliveryFault::AfterTransfer),
                "fixture panic after transfer"
            );
            if matches!(self.fault, DeliveryFault::Repeat) {
                return transfer();
            }
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
    fault: DeliveryFault,
}

impl FixtureConnection {
    pub(crate) fn is_closed(&self) -> bool {
        self.cohort.lock().unwrap().closed
    }

    pub(crate) fn captured_are_retired(&self) -> bool {
        let scopes = self.scopes.lock().unwrap();
        !scopes.is_empty()
            && scopes
                .iter()
                .all(|scope| scope.state.lock().unwrap().retired)
    }
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
            fault: self.fault,
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
    primary: Option<Arc<Gate>>,
    member_token_hash: Option<String>,
    revocations: Option<tokio::sync::broadcast::Sender<intent_core::PrincipalRevocation>>,
    revoke_reply: Option<Arc<Gate>>,
    fault: DeliveryFault,
}

impl FixtureApi {
    pub(crate) fn holding_handler(gate: Arc<Gate>) -> Self {
        Self {
            handler: Some(gate),
            ..Self::default()
        }
    }

    pub(crate) fn holding_primary(gate: Arc<Gate>) -> Self {
        Self {
            primary: Some(gate),
            ..Self::default()
        }
    }
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
            fault: self.fault,
        });
        self.entries.lock().unwrap().push(entry);
        self.connections.lock().unwrap().push(connection.clone());
        Some(connection)
    }

    fn primary_principal_id(&self) -> BoxFuture<'_, intent_core::Result<PrincipalId>> {
        Box::pin(async {
            if let Some(gate) = &self.primary {
                gate.wait().await;
            }
            Ok(self.principal.clone())
        })
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

    fn resolve_principal_credential(
        &self,
        token_hash: String,
    ) -> BoxFuture<'_, intent_core::Result<Option<PrincipalId>>> {
        Box::pin(async move {
            Ok((self.member_token_hash.as_ref() == Some(&token_hash))
                .then(|| self.principal.clone()))
        })
    }

    fn subscribe_principal_revocations(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<intent_core::PrincipalRevocation>> {
        self.revocations
            .as_ref()
            .map(tokio::sync::broadcast::Sender::subscribe)
    }

    fn principal_revoke_self(&self) -> BoxFuture<'_, intent_core::Result<Value>> {
        Box::pin(async {
            self.revocations
                .as_ref()
                .unwrap()
                .send(self.principal.clone().into())
                .unwrap();
            if let Some(gate) = &self.revoke_reply {
                gate.wait().await;
            }
            Ok(json!({"revoked":true}))
        })
    }

    fn workspace_members_list(
        &self,
        _workspace_id: intent_core::WorkspaceId,
    ) -> BoxFuture<'_, intent_core::Result<Value>> {
        // A member-visible transport route carries the same explicit fixture
        // payload. This does not install a production repository read entry.
        self.settings_get("private".into())
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

    fn dispatch(&self, raw: &str) -> impl Future<Output = bool> + Send + 'static {
        let raw = raw.to_string();
        let api: Arc<dyn WorkspaceApi> = self.api.clone();
        let tx = self.tx.clone();
        let bus = self.bus.clone();
        let limiter = self.limiter.clone();
        with_credential_context(
            true,
            Some(self.caller.clone()),
            Some(self.credential.clone()),
            self.connection.run(async move {
                let reverse = ReverseChannel::new(tx.priority_sender());
                let registry = Arc::new(PrimaryReverseRegistry::new());
                let guard = registry.register(reverse.clone(), ReverseTransport::Wss);
                process_frame(
                    &raw,
                    &api,
                    &bus,
                    &tx,
                    &mut ConnSubs::default(),
                    &mut ForwardRegistry::default(),
                    &reverse,
                    &guard,
                    None,
                    None,
                    &mut None,
                    false,
                    &limiter,
                )
                .await
            }),
        )
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

async fn until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn capture_precedes_a_full_queue_and_holds_no_limiter_while_waiting() {
    let mut h = Harness::new(FixtureApi::default(), HostRole::Owner).await;
    for _ in 0..PRIORITY_CAPACITY {
        h.tx.priority.send("occupied".into()).await.unwrap();
    }
    let call = h.dispatch(&request("private"));
    tokio::pin!(call);
    tokio::select! {
        biased;
        result = &mut call => panic!("full queue did not wait: {result}"),
        () = tokio::task::yield_now() => {}
    }
    let original = h.original();
    assert_eq!(original.scopes.lock().unwrap().len(), 1);
    assert_eq!(h.limiter.available_permits(), Some(1));
    h.connection.retire();
    assert_eq!(h.rx.priority.recv().await.as_deref(), Some("occupied"));
    assert!(call.await);
    for _ in 1..PRIORITY_CAPACITY {
        assert_eq!(h.rx.priority.recv().await.as_deref(), Some("occupied"));
    }
    assert!(h.response().await.get("result").is_none());
}

#[tokio::test]
async fn unpolled_frame_and_pending_permission_drop_retire_escaped_original_scopes() {
    let h = Harness::new(FixtureApi::default(), HostRole::Owner).await;
    let api: Arc<dyn WorkspaceApi> = h.api.clone();
    let reverse = ReverseChannel::new(h.tx.priority_sender());
    let registry = Arc::new(PrimaryReverseRegistry::new());
    let guard = registry.register(reverse.clone(), ReverseTransport::Wss);
    with_credential_context(
        true,
        Some(h.caller.clone()),
        Some(h.credential.clone()),
        h.connection.run(async {
            let raw = request("private");
            let mut subs = ConnSubs::default();
            let mut forwards = ForwardRegistry::default();
            let mut client = None;
            let future = process_frame(
                &raw,
                &api,
                &h.bus,
                &h.tx,
                &mut subs,
                &mut forwards,
                &reverse,
                &guard,
                None,
                None,
                &mut client,
                false,
                &h.limiter,
            );
            let escaped = h.original().scopes.lock().unwrap()[0].clone();
            assert!(!escaped.state.lock().unwrap().retired);
            drop(future);
            assert!(escaped.state.lock().unwrap().retired);
        }),
    )
    .await;

    let gate = Arc::new(Gate::default());
    let h = Harness::new(
        FixtureApi {
            permission: Some(gate.clone()),
            ..FixtureApi::default()
        },
        HostRole::Guest,
    )
    .await;
    let task = tokio::spawn(h.dispatch(&request("private")));
    gate.entered.notified().await;
    let escaped = h.original().scopes.lock().unwrap()[0].clone();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(escaped.state.lock().unwrap().retired);
    assert_eq!(h.limiter.available_permits(), Some(1));
}

#[tokio::test]
async fn final_delivery_wait_is_handler_work_and_retirement_has_both_orders() {
    for retire_first in [true, false] {
        let gate = Arc::new(Gate::default());
        let mut h = Harness::new(
            FixtureApi {
                delivery: Some(gate.clone()),
                ..FixtureApi::default()
            },
            HostRole::Owner,
        )
        .await;
        assert!(h.dispatch(&request("private")).await);
        gate.entered.notified().await;
        assert_eq!(h.limiter.available_permits(), Some(0));
        assert!(h.rx.priority.try_recv().is_err());
        if retire_first {
            h.connection.retire();
        }
        gate.release.notify_one();
        until(|| h.limiter.available_permits() == Some(1)).await;
        if !retire_first {
            h.connection.retire();
        }
        let reply = h.response().await;
        assert_eq!(reply.get("result").is_some(), !retire_first);
        let escaped = h.original().scopes.lock().unwrap()[0].clone();
        until(|| escaped.state.lock().unwrap().retired).await;
        assert_eq!(escaped.state.lock().unwrap().sent, !retire_first);
    }
}

#[tokio::test]
async fn final_future_abort_releases_original_packet_permit_and_request() {
    let gate = Arc::new(Gate::default());
    let h = Harness::new(
        FixtureApi {
            delivery: Some(gate.clone()),
            ..FixtureApi::default()
        },
        HostRole::Owner,
    )
    .await;
    let captured = with_credential_context(
        true,
        Some(h.caller.clone()),
        Some(h.credential.clone()),
        h.connection
            .run(async { crate::context::CapturedFrame::capture() }),
    )
    .await;
    let scope = h.original().scopes.lock().unwrap()[0].clone();
    let slot = h.tx.reserve_priority().await.unwrap();
    let permit = h.limiter.try_acquire().unwrap();
    let api = h.api.clone();
    let task = tokio::spawn(async move {
        captured
            .run(finish_prepared_rpc(
                &captured,
                permit,
                crate::router::prepare_message(api.as_ref(), &request("private")),
                slot,
            ))
            .await;
    });
    gate.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(scope.state.lock().unwrap().retired);
    assert!(!scope.state.lock().unwrap().sent);
    assert_eq!(h.limiter.available_permits(), Some(1));
    assert!(h.tx.priority_idle());
}

#[tokio::test]
async fn typed_service_failures_and_public_transport_errors_keep_distinct_policies() {
    for path in [
        "private-error",
        "provider-error",
        "oversized",
        "panic",
        "ordinary",
    ] {
        let gate = Arc::new(Gate::default());
        let mut h = Harness::new(
            FixtureApi {
                handler: Some(gate.clone()),
                ..FixtureApi::default()
            },
            HostRole::Owner,
        )
        .await;
        assert!(h.dispatch(&request(path)).await);
        gate.entered.notified().await;
        h.connection.retire();
        gate.release.notify_one();
        let reply = h.response().await;
        assert_eq!(reply["id"], 7);
        match path {
            "private-error" => {
                assert!(!reply.to_string().contains("private service error"));
                assert!(reply
                    .to_string()
                    .contains("original read delivery unavailable"));
            }
            "provider-error" => {
                assert!(reply.to_string().contains("actual retained provider error"));
            }
            "oversized" => assert_eq!(reply["error"]["code"], -32010),
            "panic" => assert_eq!(reply["error"]["code"], -32603),
            "ordinary" => assert!(reply.get("result").is_some()),
            _ => unreachable!(),
        }
        let escaped = h.original().scopes.lock().unwrap()[0].clone();
        until(|| escaped.state.lock().unwrap().retired).await;
        let state = escaped.state.lock().unwrap();
        if matches!(path, "private-error" | "provider-error") {
            assert_eq!(state.kinds, [RepositoryReadReplyKind::ServiceError]);
        } else if matches!(path, "panic" | "oversized") {
            assert!(state.kinds.is_empty());
        }
    }
}

#[tokio::test]
async fn independent_completion_and_equal_public_ids_never_rebind_scopes() {
    let mut first = Harness::new(FixtureApi::default(), HostRole::Owner).await;
    let mut second = Harness::new(
        FixtureApi {
            principal: first.api.principal.clone(),
            ..FixtureApi::default()
        },
        HostRole::Owner,
    )
    .await;
    for _ in 0..2 {
        assert!(first.dispatch(&request("private")).await);
        let reply = first.response().await;
        assert_eq!(reply["result"]["scoped"], true);
        assert!(reply["result"].get("error").is_some());
    }
    let a = first.original();
    let scopes = a.scopes.lock().unwrap().clone();
    assert_eq!(scopes.len(), 2);
    assert!(!Arc::ptr_eq(&scopes[0].state, &scopes[1].state));
    first.connection.retire();
    assert!(second.dispatch(&request("private")).await);
    assert_eq!(second.response().await["result"]["scoped"], true);
    assert!(!second.original().cohort.lock().unwrap().closed);
}

#[tokio::test]
async fn notifications_overload_and_invalid_frames_keep_existing_queue_policy() {
    let gate = Arc::new(Gate::default());
    let mut h = Harness::new(
        FixtureApi {
            handler: Some(gate.clone()),
            ..FixtureApi::default()
        },
        HostRole::Owner,
    )
    .await;
    let raw =
        json!({"jsonrpc":"2.0","method":"settings.get","params":{"path":"private"}}).to_string();
    assert!(h.dispatch(&raw).await);
    gate.entered.notified().await;
    assert_eq!(h.limiter.available_permits(), Some(0));
    assert!(h.dispatch(&request("private")).await);
    assert_eq!(h.response().await["error"]["code"], -32011);
    assert!(h.dispatch("not json").await);
    assert_eq!(h.response().await["error"]["code"], -32700);
    gate.release.notify_one();
    until(|| h.limiter.available_permits() == Some(1)).await;
    assert!(h.rx.priority.try_recv().is_err());
    assert!(h.tx.priority_idle());
}

#[tokio::test]
async fn delivery_faults_preserve_one_original_transfer_and_release_every_guard() {
    for fault in [
        DeliveryFault::Construction,
        DeliveryFault::BeforeTransfer,
        DeliveryFault::AfterTransfer,
        DeliveryFault::Repeat,
        DeliveryFault::Omit,
    ] {
        let mut h = Harness::new(
            FixtureApi {
                fault,
                ..FixtureApi::default()
            },
            HostRole::Owner,
        )
        .await;
        assert!(h.dispatch(&request("private")).await);
        let reply = h.response().await;
        let admitted = matches!(fault, DeliveryFault::AfterTransfer | DeliveryFault::Repeat);
        assert_eq!(reply.get("result").is_some(), admitted);
        if !admitted {
            assert_eq!(reply["error"]["code"], -32603);
        }
        assert_eq!(reply["id"], 7);
        let scope = h.original().scopes.lock().unwrap()[0].clone();
        until(|| scope.state.lock().unwrap().retired).await;
        assert!(h.rx.priority.try_recv().is_err());
        assert!(h.tx.priority_idle());
        assert_eq!(h.limiter.available_permits(), Some(1));
        assert_eq!(scope.state.lock().unwrap().sent, admitted);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn actual_uds_binding_and_listener_shutdown_preserve_original_cohorts() {
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

    let dir = tempfile::Builder::new()
        .prefix("carrier-uds-")
        .tempdir()
        .unwrap();
    let bus = EventBus::new(
        intent_store::Store::open(&dir.path().join("bus.db"))
            .await
            .unwrap(),
    );
    let api = Arc::new(FixtureApi::default());
    let path = dir.path().join("daemon.sock");
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server_api = api.clone();
    let server_path = path.clone();
    let server = tokio::spawn(async move {
        crate::listener::serve_uds(server_api, bus, &server_path, None, async {
            let _ = stopped.await;
        })
        .await
    });
    until(|| path.exists()).await;
    let socket = tokio::net::UnixStream::connect(&path).await.unwrap();
    let (read, mut write) = socket.into_split();
    let mut lines = tokio::io::BufReader::new(read).lines();
    for _ in 0..2 {
        write
            .write_all(format!("{}\n", request("private")).as_bytes())
            .await
            .unwrap();
        let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line).unwrap()["result"]["scoped"],
            true
        );
    }
    let original = api.connections.lock().unwrap()[0].clone();
    let scopes = original.scopes.lock().unwrap().clone();
    assert_eq!(scopes.len(), 2);
    assert!(scopes.iter().all(|scope| scope.credential.is_none()));
    assert_eq!(
        *api.entries.lock().unwrap(),
        [RepositoryWireEntry::AdmittedLocal]
    );
    assert!(!original.cohort.lock().unwrap().closed);
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
    assert!(original.cohort.lock().unwrap().closed);
    // The old listener policy keeps accepted ordinary connections alive.
    // Shutdown retires only qualified scopes, not ordinary response handling.
    for path in ["private", "ordinary"] {
        write
            .write_all(format!("{}\n", request(path)).as_bytes())
            .await
            .unwrap();
        let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&line)
                .unwrap()
                .get("result")
                .is_some(),
            path == "ordinary"
        );
    }
    drop(write);
    drop(lines);
}

struct MemoryToken(Mutex<String>);

impl crate::auth::TokenStore for MemoryToken {
    fn load_token(&self) -> Option<String> {
        Some(self.0.lock().unwrap().clone())
    }
    fn store_token(&self, token: &str) -> intent_core::Result<()> {
        *self.0.lock().unwrap() = token.to_string();
        Ok(())
    }
}

type TestWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;

async fn connect_wss(port: u16, certificate: &crate::TlsCertificate, token: &str) -> TestWebSocket {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut certificate.cert.as_bytes()) {
        roots.add(cert.unwrap()).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let socket = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(
            rustls_pki_types::ServerName::try_from("localhost").unwrap(),
            socket,
        )
        .await
        .unwrap();
    let mut request = format!("wss://localhost:{port}/ws")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Authorization", format!("Bearer {token}").parse().unwrap());
    request
        .headers_mut()
        .insert("Origin", "http://localhost:3000".parse().unwrap());
    tokio_tungstenite::client_async(request, stream)
        .await
        .unwrap()
        .0
}

async fn ws_response(socket: &mut TestWebSocket) -> Value {
    use futures::StreamExt as _;

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match socket.next().await.unwrap().unwrap() {
                tokio_tungstenite::tungstenite::Message::Text(text) => {
                    return serde_json::from_str(&text).unwrap()
                }
                tokio_tungstenite::tungstenite::Message::Ping(_) => {}
                other => panic!("expected original response, got {other:?}"),
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn actual_wss_binding_retains_exact_bearer_across_independent_requests() {
    use futures::SinkExt as _;
    use tokio_tungstenite::tungstenite::Message;

    let dir = tempfile::Builder::new()
        .prefix("carrier-wss-")
        .tempdir()
        .unwrap();
    let bus = EventBus::new(
        intent_store::Store::open(&dir.path().join("bus.db"))
            .await
            .unwrap(),
    );
    let api = Arc::new(FixtureApi::default());
    let certificate = crate::ensure_tls_certificate(dir.path()).unwrap();
    let token = "a".repeat(64);
    let tokens = Arc::new(crate::AsyncTokenStore::new(Arc::new(MemoryToken(
        Mutex::new(token.clone()),
    ))));
    let server = crate::WsApiServer::new(
        api.clone(),
        bus,
        &certificate,
        &tokens,
        crate::WsOptions {
            base_port: 0,
            ..crate::WsOptions::default()
        },
        None,
    )
    .unwrap();
    let port = server.start().await.unwrap();
    let mut socket = connect_wss(port, &certificate, &token).await;
    for _ in 0..2 {
        socket
            .send(Message::Text(request("private").into()))
            .await
            .unwrap();
        assert_eq!(ws_response(&mut socket).await["result"]["scoped"], true);
    }
    let original = api.connections.lock().unwrap()[0].clone();
    let scopes = original.scopes.lock().unwrap().clone();
    assert_eq!(scopes.len(), 2);
    assert_eq!(*api.entries.lock().unwrap(), [RepositoryWireEntry::Bearer]);
    let (
        Some(WireCredential::Legacy {
            authority: first, ..
        }),
        Some(WireCredential::Legacy {
            authority: second, ..
        }),
    ) = (&scopes[0].credential, &scopes[1].credential)
    else {
        panic!("actual WSS bearer missing")
    };
    assert!(Arc::ptr_eq(first, second));
    assert!(!Arc::ptr_eq(&scopes[0].state, &scopes[1].state));
    socket.close(None).await.unwrap();
    until(|| original.cohort.lock().unwrap().closed).await;
    server.stop().await;
}

#[tokio::test]
async fn actual_wss_close_rotation_shutdown_and_heartbeat_retire_held_requests() {
    use futures::SinkExt as _;
    use tokio_tungstenite::tungstenite::Message;

    for mode in ["close", "rotation", "shutdown", "heartbeat"] {
        let dir = tempfile::Builder::new()
            .prefix("carrier-wss-close-")
            .tempdir()
            .unwrap();
        let bus = EventBus::new(
            intent_store::Store::open(&dir.path().join("bus.db"))
                .await
                .unwrap(),
        );
        let handler = Arc::new(Gate::default());
        let api = Arc::new(FixtureApi {
            handler: Some(handler.clone()),
            ..FixtureApi::default()
        });
        let certificate = crate::ensure_tls_certificate(dir.path()).unwrap();
        let token = "c".repeat(64);
        let tokens = Arc::new(crate::AsyncTokenStore::new(Arc::new(MemoryToken(
            Mutex::new(token.clone()),
        ))));
        let (heartbeat, gate) = tokio::sync::watch::channel(false);
        let options = if mode == "heartbeat" {
            crate::WsOptions {
                base_port: 0,
                heartbeat_interval: Duration::from_millis(10),
                heartbeat_timeout: Duration::from_millis(10),
                heartbeat_gate: Some(gate),
                ..crate::WsOptions::default()
            }
        } else {
            crate::WsOptions {
                base_port: 0,
                ..crate::WsOptions::default()
            }
        };
        let server =
            crate::WsApiServer::new(api.clone(), bus, &certificate, &tokens, options, None)
                .unwrap();
        let mut socket = connect_wss(server.start().await.unwrap(), &certificate, &token).await;
        socket
            .send(Message::Text(request("private").into()))
            .await
            .unwrap();
        handler.entered.notified().await;
        let original = api.connections.lock().unwrap()[0].clone();
        let scope = original.scopes.lock().unwrap()[0].clone();
        let shutdown = if mode == "shutdown" {
            let owner = server.clone();
            Some(tokio::spawn(async move {
                owner.stop().await;
            }))
        } else {
            None
        };
        match mode {
            "close" => {
                socket.close(None).await.unwrap();
            }
            "rotation" => {
                tokens.store_token(&"d".repeat(64)).await.unwrap();
            }
            "heartbeat" => {
                heartbeat.send(true).unwrap();
            }
            _ => {}
        }
        until(|| original.cohort.lock().unwrap().closed).await;
        assert!(scope.state.lock().unwrap().retired, "{mode}");
        handler.release.notify_one();
        if mode == "rotation" {
            let reply = ws_response(&mut socket).await;
            assert!(reply.get("result").is_none());
            assert!(!reply.to_string().contains("original payload"));
        }
        until(|| !scope.state.lock().unwrap().kinds.is_empty()).await;
        assert!(!scope.state.lock().unwrap().sent);
        if let Some(task) = shutdown {
            task.await.unwrap();
        }
        server.stop().await;
    }
}

#[tokio::test]
async fn actual_wss_principal_revocation_drains_ordinary_reply_but_denies_held_private_data() {
    use futures::SinkExt as _;
    use tokio_tungstenite::tungstenite::Message;

    // The TLS upgrade and frame loop are real; credential lookup and the
    // committed-revocation notification are explicit carrier fixtures.
    let dir = tempfile::Builder::new()
        .prefix("carrier-wss-revoke-")
        .tempdir()
        .unwrap();
    let bus = EventBus::new(
        intent_store::Store::open(&dir.path().join("bus.db"))
            .await
            .unwrap(),
    );
    let handler = Arc::new(Gate::default());
    let revoke_reply = Arc::new(Gate::default());
    let (revocations, _) = tokio::sync::broadcast::channel(8);
    let token = "e".repeat(64);
    let token_hash = crate::auth::hash_token(&token);
    let api = Arc::new(FixtureApi {
        handler: Some(handler.clone()),
        member_token_hash: Some(token_hash.clone()),
        revocations: Some(revocations),
        revoke_reply: Some(revoke_reply.clone()),
        ..FixtureApi::default()
    });
    let certificate = crate::ensure_tls_certificate(dir.path()).unwrap();
    let tokens = Arc::new(crate::AsyncTokenStore::new(Arc::new(MemoryToken(
        Mutex::new("f".repeat(64)),
    ))));
    let server = crate::WsApiServer::new(
        api.clone(),
        bus,
        &certificate,
        &tokens,
        crate::WsOptions {
            base_port: 0,
            ..crate::WsOptions::default()
        },
        None,
    )
    .unwrap();
    let mut socket = connect_wss(server.start().await.unwrap(), &certificate, &token).await;
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":7,"method":"workspace.members.list","params":{"workspaceId":intent_core::WorkspaceId::new()}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), handler.entered.notified())
        .await
        .unwrap();
    let original = api.connections.lock().unwrap()[0].clone();
    let scope = original.scopes.lock().unwrap()[0].clone();
    assert_eq!(
        scope.caller,
        Some(Caller::Wire {
            principal_id: api.principal.clone(),
            host_role: HostRole::Member
        })
    );
    let Some(WireCredential::Principal {
        principal_id,
        token_hash: actual,
    }) = &scope.credential
    else {
        panic!("actual per-principal WSS bearer missing");
    };
    assert_eq!(principal_id, &api.principal);
    assert_eq!(actual, &token_hash);
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":8,"method":"principal.revokeSelf","params":{}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), revoke_reply.entered.notified())
        .await
        .unwrap();
    until(|| original.is_closed()).await;
    assert!(scope.state.lock().unwrap().retired);
    revoke_reply.release.notify_one();
    handler.release.notify_one();
    let mut replies = [
        ws_response(&mut socket).await,
        ws_response(&mut socket).await,
    ];
    replies.sort_by_key(|reply| reply["id"].as_u64());
    assert_eq!(replies[0]["id"], 7);
    assert!(replies[0].get("result").is_none());
    assert!(!replies[0].to_string().contains("original payload"));
    assert_eq!(replies[1]["id"], 8);
    assert_eq!(replies[1]["result"]["revoked"], true);
    assert!(!scope.state.lock().unwrap().sent);
    until(|| original.captured_are_retired()).await;
    server.stop().await;
}
