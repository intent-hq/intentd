//! Both listeners join each accepted connection to an installed
//! [`ConnectionSet`]; closing the set ends every connection and refuses new
//! ones.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt as _;
use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use tokio::io::AsyncReadExt as _;

use super::{ConnectionGuard, ConnectionHook, ConnectionSet};
use crate::{PrimaryReverseRegistry, RpcLimiter, WsApiServer, WsOptions};

/// The tenant connection set, in miniature.
#[derive(Default)]
struct TestSet(Mutex<State>);

#[derive(Default)]
struct State {
    next: u64,
    open: HashMap<u64, Box<dyn FnOnce() + Send>>,
    closed: bool,
}

impl TestSet {
    fn len(&self) -> usize {
        self.0.lock().unwrap().open.len()
    }

    fn close_all(&self) -> usize {
        let open: Vec<_> = {
            let mut state = self.0.lock().unwrap();
            state.closed = true;
            state.open.drain().map(|(_, close)| close).collect()
        };
        let closed = open.len();
        for close in open {
            close();
        }
        closed
    }
}

impl ConnectionSet for Arc<TestSet> {
    fn register(&self, close: Box<dyn FnOnce() + Send>) -> Option<ConnectionGuard> {
        let mut state = self.0.lock().unwrap();
        if state.closed {
            drop(state);
            close();
            return None;
        }
        state.next += 1;
        let id = state.next;
        state.open.insert(id, close);
        let set = self.clone();
        Some(ConnectionGuard::new(move || {
            set.0.lock().unwrap().open.remove(&id);
        }))
    }
}

fn hook(set: &Arc<TestSet>) -> ConnectionHook {
    ConnectionHook::new(Arc::new(set.clone()))
}

fn timeout() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(10))
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(timeout(), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

async fn api(dir: &std::path::Path) -> (Arc<dyn WorkspaceApi>, EventBus) {
    let store = intent_store::Store::open(&dir.join("db.sqlite"))
        .await
        .unwrap();
    let bus = EventBus::new(store.clone());
    (Arc::new(Services::new_isolated(store)), bus)
}

async fn ws_server(set: &Arc<TestSet>, dir: &std::path::Path) -> (WsApiServer, u16) {
    let (api, bus) = api(dir).await;
    let options = WsOptions {
        base_port: 0,
        ..WsOptions::default()
    };
    let mut server = WsApiServer::new_insecure(api, bus, options, None);
    server.install_connection_set(hook(set));
    let port = server.start().await.unwrap();
    (server, port)
}

/// The next frame is a close, or the stream ends.
async fn ws_ends<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let next = tokio::time::timeout(timeout(), ws.next())
        .await
        .expect("the connection was not closed");
    assert!(
        matches!(
            next,
            None | Some(Err(_) | Ok(tokio_tungstenite::tungstenite::Message::Close(_)))
        ),
        "{next:?}"
    );
}

#[tokio::test]
async fn closing_the_set_closes_every_ws_connection_and_refuses_new_ones() {
    let dir = tempfile::tempdir().unwrap();
    let set = Arc::new(TestSet::default());
    let (server, port) = ws_server(&set, dir.path()).await;
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (mut a, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let (mut b, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    until("both connections to join", || set.len() == 2).await;

    assert_eq!(set.close_all(), 2);
    ws_ends(&mut a).await;
    ws_ends(&mut b).await;
    until("the listener to forget them", || server.client_count() == 0).await;

    let (mut late, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws_ends(&mut late).await;
    assert_eq!(set.len(), 0);
    assert_eq!(server.client_count(), 0, "a refused connection is not kept");
    server.stop().await;
}

#[tokio::test]
async fn a_ws_connection_that_ends_leaves_the_set() {
    let dir = tempfile::tempdir().unwrap();
    let set = Arc::new(TestSet::default());
    let (server, port) = ws_server(&set, dir.path()).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws"))
        .await
        .unwrap();
    until("the connection to join", || set.len() == 1).await;
    ws.close(None).await.unwrap();
    until("the connection to leave", || set.len() == 0).await;
    server.stop().await;
}

/// The next read sees the socket closed.
async fn uds_ends(stream: &mut tokio::net::UnixStream) {
    let mut buf = [0u8; 64];
    let read = tokio::time::timeout(timeout(), stream.read(&mut buf))
        .await
        .expect("the connection was not closed");
    assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
}

#[tokio::test]
async fn closing_the_set_closes_every_uds_connection_and_refuses_new_ones() {
    let dir = tempfile::tempdir().unwrap();
    let set = Arc::new(TestSet::default());
    let (api, bus) = api(dir.path()).await;
    let socket = dir.path().join("s.sock");
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn({
        let socket = socket.clone();
        let hook = hook(&set);
        async move {
            crate::serve_uds_with_connections(
                api,
                bus,
                &socket,
                None,
                None,
                Arc::new(PrimaryReverseRegistry::new()),
                RpcLimiter::unlimited(),
                Some(hook),
                async {
                    let _ = stopped.await;
                },
            )
            .await
        }
    });
    until("the socket to bind", || socket.exists()).await;
    let mut a = tokio::net::UnixStream::connect(&socket).await.unwrap();
    let mut b = tokio::net::UnixStream::connect(&socket).await.unwrap();
    until("both connections to join", || set.len() == 2).await;

    assert_eq!(set.close_all(), 2);
    uds_ends(&mut a).await;
    uds_ends(&mut b).await;

    let mut late = tokio::net::UnixStream::connect(&socket).await.unwrap();
    uds_ends(&mut late).await;
    assert_eq!(set.len(), 0);
    let _ = stop.send(());
    serve.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_uds_connection_that_ends_leaves_the_set() {
    let dir = tempfile::tempdir().unwrap();
    let set = Arc::new(TestSet::default());
    let (api, bus) = api(dir.path()).await;
    let socket = dir.path().join("s.sock");
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn({
        let socket = socket.clone();
        let hook = hook(&set);
        async move {
            crate::serve_uds_with_connections(
                api,
                bus,
                &socket,
                None,
                None,
                Arc::new(PrimaryReverseRegistry::new()),
                RpcLimiter::unlimited(),
                Some(hook),
                async {
                    let _ = stopped.await;
                },
            )
            .await
        }
    });
    until("the socket to bind", || socket.exists()).await;
    let stream = tokio::net::UnixStream::connect(&socket).await.unwrap();
    until("the connection to join", || set.len() == 1).await;
    drop(stream);
    until("the connection to leave", || set.len() == 0).await;
    let _ = stop.send(());
    serve.await.unwrap().unwrap();
}
