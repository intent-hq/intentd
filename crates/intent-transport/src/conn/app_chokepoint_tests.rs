//! Intent apps at the inbound chokepoint (PROTOCOL §5.60): the security
//! sweep of the task's failure modes A1–A5, A8, A10 and A14.
//!
//! A stub `WorkspaceApi` answers only the app hooks. Every other trait
//! method keeps its default, which fails with `-32603` — so a frame that
//! slipped past the allowlist would answer something other than the
//! connection-level `-32003` without `data`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::apps::{AppAttachment, AppPrincipal};
use intent_core::{current_caller, BoxFuture, Caller, HostRole, PrincipalId, Result, WorkspaceApi};
use serde_json::{json, Map, Value};
use tokio::sync::watch;

use super::*;

#[derive(Default)]
struct AppStub {
    end: Mutex<Vec<watch::Sender<bool>>>,
    calls: Mutex<Vec<(String, String)>>,
    ended_sessions: Mutex<Vec<String>>,
}

fn caller_kind() -> String {
    match current_caller() {
        Some(Caller::App(app)) => format!("app:{}:{}", app.app_id, app.level),
        Some(Caller::Wire { host_role, .. }) => format!("wire:{host_role:?}"),
        Some(other) => format!("{other:?}"),
        None => "unbound".into(),
    }
}

impl WorkspaceApi for AppStub {
    fn app_session_attach(&self, token: String) -> BoxFuture<'_, Result<AppAttachment>> {
        Box::pin(async move {
            let level = match token.as_str() {
                "tok-1" => 1,
                "tok-2" => 2,
                "tok-3" => 3,
                _ => {
                    return Err(intent_core::apps::app_refusal(
                        "session-invalid",
                        "the app session is unknown or ended",
                    ))
                }
            };
            let (tx, rx) = watch::channel(false);
            self.end.lock().unwrap().push(tx);
            Ok(AppAttachment {
                principal: Arc::new(AppPrincipal {
                    app_id: "demo".into(),
                    name: "Demo".into(),
                    bundle_hash: "sha256:00".into(),
                    level,
                    features: vec!["models".into(), "notifications".into()],
                    session_id: "as_1".into(),
                }),
                result: json!({ "sessionId": "as_1", "appId": "demo" }),
                ended: rx,
            })
        })
    }

    fn app_call(
        &self,
        method: String,
        _params: Map<String, Value>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let kind = caller_kind();
            self.calls
                .lock()
                .unwrap()
                .push((method.clone(), kind.clone()));
            if method == "app.session.open" {
                return Ok(json!({ "sessionId": "as_opened", "token": "t" }));
            }
            Ok(json!({ "caller": kind }))
        })
    }

    fn principal_me(&self) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(json!({ "caller": caller_kind() })) })
    }

    fn app_sessions_end(&self, session_ids: Vec<String>) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.ended_sessions.lock().unwrap().extend(session_ids);
        })
    }
}

struct Conn {
    api: Arc<dyn WorkspaceApi>,
    stub: Arc<AppStub>,
    bus: EventBus,
    tx: OutboundSender,
    rx: OutboundReceiver,
    reverse: ReverseChannel,
    guard: crate::reverse::PrimaryReverseGuard,
    _primary: Arc<crate::reverse::PrimaryReverseRegistry>,
    limiter: RpcLimiter,
    subs: ConnSubs,
    client: Option<intent_core::ClientId>,
    caller: Caller,
    next_id: u64,
    _dir: tempfile::TempDir,
}

impl Conn {
    async fn new(role: HostRole) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = intent_store::Store::open(&dir.path().join("store.db"))
            .await
            .unwrap();
        let stub = Arc::new(AppStub::default());
        let api: Arc<dyn WorkspaceApi> = stub.clone();
        let (tx, rx) = outbound_channel();
        let reverse = ReverseChannel::new(tx.priority_sender());
        let primary = Arc::new(crate::reverse::PrimaryReverseRegistry::new());
        let guard = primary.register(reverse.clone(), crate::reverse::ReverseTransport::Wss);
        Self {
            api,
            stub,
            bus: EventBus::new(store),
            tx,
            rx,
            reverse,
            guard,
            _primary: primary,
            limiter: RpcLimiter::unlimited(),
            subs: ConnSubs::default(),
            client: None,
            caller: Caller::Wire {
                principal_id: PrincipalId::new(),
                host_role: role,
            },
            next_id: 0,
            _dir: dir,
        }
    }

    /// Send one raw frame and return the next reply frame.
    async fn raw(&mut self, frame: &str) -> Value {
        // The frame future must be built inside the caller scope: it
        // captures the request context when it is created.
        let ok = crate::context::with_request_context(true, Some(self.caller.clone()), async {
            process_frame(
                frame,
                &self.api,
                &self.bus,
                &self.tx,
                &mut self.subs,
                &self.reverse,
                &self.guard,
                None,
                None,
                &mut self.client,
                true,
                &self.limiter,
            )
            .await
        })
        .await;
        assert!(ok, "the outbound channel stays open");
        let reply = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(10)),
            self.rx.recv_priority(),
        )
        .await
        .expect("a reply")
        .expect("channel open");
        serde_json::from_str(&reply).unwrap()
    }

    async fn call(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let frame =
            json!({ "jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params })
                .to_string();
        self.raw(&frame).await
    }

    fn will_close(&self) -> bool {
        self.subs
            .app
            .end_signal()
            .is_some_and(|rx| *rx.borrow() || rx.has_changed().is_err())
    }
}

fn assert_connection_forbidden(reply: &Value, what: &str) {
    assert_eq!(reply["error"]["code"], -32003, "{what}: {reply}");
    assert_eq!(reply["error"]["message"], "Forbidden", "{what}: {reply}");
    assert!(
        reply["error"].get("data").is_none(),
        "{what}: no data: {reply}"
    );
}

fn every_method() -> Vec<String> {
    let mut all: Vec<String> = crate::catalog::tests::client_callable_universe()
        .into_iter()
        .collect();
    all.extend(
        crate::catalog::NOTIFICATIONS
            .iter()
            .chain(crate::catalog::REVERSE_METHODS)
            .map(|m| (*m).to_string()),
    );
    all.extend(
        [
            "no.such.method",
            "app.",
            "App.storage.get",
            "app.storage.get ",
            "principal.me\u{0}",
            "app.storage.*",
            "events.event",
        ]
        .map(String::from),
    );
    all.sort();
    all.dedup();
    all
}

/// A1/A3/A13/A14: at every level, every method outside the level list —
/// every cataloged method, notifications, reverse RPCs and unknown or
/// look-alike names — is the connection-level `-32003` without `data`, and
/// the allowed ones reach the router bound to the app principal.
#[tokio::test]
async fn each_level_is_refused_every_method_outside_its_allowlist() {
    for (level, token, list) in [
        (1u8, "tok-1", crate::catalog::APP_LEVEL1_METHODS),
        (2, "tok-2", crate::catalog::APP_LEVEL2_METHODS),
        (3, "tok-3", crate::catalog::APP_LEVEL3_METHODS),
    ] {
        let mut conn = Conn::new(HostRole::Owner).await;
        let attached = conn
            .call("app.session.attach", json!({ "token": token }))
            .await;
        assert_eq!(attached["result"]["sessionId"], "as_1", "{attached}");
        let mut refused = 0;
        for method in every_method() {
            let reply = conn.call(&method, json!({})).await;
            if list.contains(&method.as_str()) {
                assert_eq!(
                    reply["result"]["caller"],
                    format!("app:demo:{level}"),
                    "level {level} {method}: runs as the app principal: {reply}"
                );
            } else {
                assert_connection_forbidden(&reply, &format!("level {level} {method}"));
                refused += 1;
            }
        }
        assert!(refused > 400, "the sweep covered the catalog ({refused})");
        // Nothing but the allowed router methods ever reached the stub.
        for (method, kind) in conn.stub.calls.lock().unwrap().iter() {
            assert!(
                list.contains(&method.as_str()),
                "{method} reached the service"
            );
            assert_eq!(kind, &format!("app:demo:{level}"));
        }
        assert!(!conn.will_close(), "refusals do not close the connection");
    }
}

/// A4: frames without a usable method never dispatch on an app connection.
#[tokio::test]
async fn malformed_frames_on_an_app_connection_are_refused() {
    let mut conn = Conn::new(HostRole::Owner).await;
    conn.call("app.session.attach", json!({ "token": "tok-2" }))
        .await;
    for frame in [
        r#"{"jsonrpc":"2.0","id":7}"#,
        r#"{"jsonrpc":"2.0","id":7,"method":7}"#,
        r#"[{"jsonrpc":"2.0","id":7,"method":"host.exec"}]"#,
        "not json",
    ] {
        let reply = conn.raw(frame).await;
        assert_eq!(reply["error"]["code"], -32003, "{frame}: {reply}");
    }
    assert!(conn.stub.calls.lock().unwrap().is_empty());
}

/// A5: attach must be the first frame of an administrator connection; a
/// failed attach answers and then closes; a bound connection cannot attach
/// again.
#[tokio::test]
async fn attach_rules() {
    // Not first.
    let mut conn = Conn::new(HostRole::Owner).await;
    let _ = conn.raw("garbage").await;
    let reply = conn
        .call("app.session.attach", json!({ "token": "tok-1" }))
        .await;
    assert_eq!(
        reply["error"]["data"]["code"], "attach-not-first",
        "{reply}"
    );
    assert!(conn.will_close());

    // Unknown token.
    let mut conn = Conn::new(HostRole::Owner).await;
    let reply = conn
        .call("app.session.attach", json!({ "token": "nope" }))
        .await;
    assert_eq!(reply["error"]["code"], -32602, "{reply}");
    assert_eq!(reply["error"]["data"]["code"], "session-invalid", "{reply}");
    assert!(conn.will_close());

    // Guests and host members: connection-level refusal, nothing bound.
    for role in [HostRole::Guest, HostRole::Member] {
        let mut conn = Conn::new(role).await;
        let reply = conn
            .call("app.session.attach", json!({ "token": "tok-1" }))
            .await;
        assert_connection_forbidden(&reply, &format!("{role:?} attach"));
        assert!(conn.subs.app.bound().is_none());
    }

    // Bound: a second attach is outside the allowlist.
    let mut conn = Conn::new(HostRole::Owner).await;
    conn.call("app.session.attach", json!({ "token": "tok-1" }))
        .await;
    let reply = conn
        .call("app.session.attach", json!({ "token": "tok-2" }))
        .await;
    assert_connection_forbidden(&reply, "second attach");
    assert_eq!(conn.subs.app.bound().map(|a| a.level), Some(1));
}

/// A8: the session end reaches the connection's close signal.
#[tokio::test]
async fn session_end_signals_the_connection() {
    let mut conn = Conn::new(HostRole::Owner).await;
    conn.call("app.session.attach", json!({ "token": "tok-1" }))
        .await;
    assert!(!conn.will_close());
    let end = conn.subs.app.end_signal();
    let waiter = tokio::spawn(crate::app_conn::wait_end(end));
    conn.stub.end.lock().unwrap()[0].send(true).unwrap();
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        waiter,
    )
    .await
    .expect("the end signal fires")
    .unwrap();
    // A dropped sender (the session table forgot it) also closes.
    let mut conn = Conn::new(HostRole::Owner).await;
    conn.call("app.session.attach", json!({ "token": "tok-1" }))
        .await;
    conn.stub.end.lock().unwrap().clear();
    assert!(conn.will_close());
}

/// A10: an administrator connection that opened a session ends it when the
/// connection's state drops.
#[tokio::test]
async fn opener_drop_ends_its_sessions() {
    let mut conn = Conn::new(HostRole::Owner).await;
    let reply = conn
        .call("app.session.open", json!({ "appId": "demo" }))
        .await;
    assert_eq!(reply["result"]["sessionId"], "as_opened", "{reply}");
    let stub = conn.stub.clone();
    drop(std::mem::take(&mut conn.subs));
    for _ in 0..100 {
        if !stub.ended_sessions.lock().unwrap().is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(*stub.ended_sessions.lock().unwrap(), ["as_opened"]);
}

/// Impact when Intent apps is off: a connection that never attaches keeps
/// its old behavior. The administrator reaches the router as itself; a
/// guest still gets the collaborator allowlist (allowed methods run as the
/// guest, other methods and every `app.*` method are the connection-level
/// `-32003`); nothing closes and no app state is created.
#[tokio::test]
async fn connections_that_never_attach_are_unchanged() {
    let mut admin = Conn::new(HostRole::Owner).await;
    let me = admin.call("principal.me", json!({})).await;
    assert_eq!(me["result"]["caller"], "wire:Owner", "{me}");
    let listed = admin.call("app.list", json!({})).await;
    assert_eq!(listed["result"]["caller"], "wire:Owner", "{listed}");
    assert!(admin.subs.app.bound().is_none());
    assert!(admin.subs.app.end_signal().is_none());

    let mut guest = Conn::new(HostRole::Guest).await;
    let me = guest.call("principal.me", json!({})).await;
    assert_eq!(me["result"]["caller"], "wire:Guest", "{me}");
    for method in [
        "host.exec",
        "settings.get",
        "app.list",
        "app.session.open",
        "app.storage.get",
    ] {
        let reply = guest.call(method, json!({})).await;
        assert_connection_forbidden(&reply, &format!("guest {method}"));
    }
    assert!(guest.subs.app.end_signal().is_none());
    // The guest's `app.*` refusals never reached the service layer.
    assert!(guest.stub.calls.lock().unwrap().is_empty());
}
