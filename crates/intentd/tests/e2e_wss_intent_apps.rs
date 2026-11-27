//! WSS end-to-end for Intent apps (PROTOCOL §5.60, Labs): the library,
//! approval bound to the bundle hash, `app.session.open` /
//! `app.session.attach`, the app allowlist at the real inbound chokepoint,
//! per-app storage, the `app:*` events, and the connection close when the
//! session ends (an edited bundle needs approval again).
//!
//! Drives a real [`WsApiServer`] over plain `ws://` (insecure dev mode), so
//! the upgrade → JSON-RPC → chokepoint → router → services → store path is
//! the one production clients use.

#![cfg(unix)]

use crate::common;

use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use intent_core::{Workspace, WorkspaceApi, WorkspaceId};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{WsApiServer, WsOptions};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type PlainWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

const WS_ID: &str = "ws-intent-apps";

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    checkout: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

async fn boot() -> Fixture {
    let dir_guard = common::test_tempdir("intentd-apps-");
    let dir = dir_guard.path().to_path_buf();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let checkout = dir.join("checkout");
    std::fs::create_dir_all(&checkout).unwrap();
    store
        .insert_workspace(&Workspace {
            id: WorkspaceId(WS_ID.into()),
            title: "Apps".into(),
            branch: "main".into(),
            worktree_path: Some(checkout.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .await
        .expect("seed workspace");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).unwrap();
    let services = Services::new_isolated(store)
        .with_workspaces_root(workspaces_root)
        .with_apps_dir(&dir.join("data"))
        .with_event_bus(bus.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws = WsApiServer::new_insecure(api, bus, opts, None);
    let port = ws.start().await.expect("start");
    Fixture {
        _ws: ws,
        port,
        checkout,
        _dir: dir_guard,
    }
}

async fn connect(port: u16) -> PlainWs {
    let url = format!("ws://127.0.0.1:{port}/ws");
    tokio_tungstenite::connect_async(&url)
        .await
        .expect("plain ws handshake")
        .0
}

/// Send one request and return its response envelope; notifications seen
/// on the way are appended to `seen`.
async fn rpc_seen(
    ws: &mut PlainWs,
    id: i64,
    method: &str,
    params: Value,
    seen: &mut Vec<Value>,
) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.expect("open").expect("frame") {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        return v;
                    }
                    seen.push(v);
                }
                Message::Ping(_) | Message::Pong(_) => {}
                other => panic!("unexpected message {other:?}"),
            }
        }
    })
    .await
    .expect("response timeout")
}

async fn rpc(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    rpc_seen(ws, id, method, params, &mut Vec::new()).await
}

async fn ok(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    let v = rpc(ws, id, method, params).await;
    assert_eq!(v["jsonrpc"], "2.0", "{v}");
    assert!(v.get("error").is_none(), "{method} errored: {v}");
    v["result"].clone()
}

/// Read until the server closes; return the close frame.
async fn expect_close(ws: &mut PlainWs) -> Option<(CloseCode, String)> {
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(frame))) => {
                    return frame.map(|f| (f.code, f.reason.to_string()));
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return None,
            }
        }
    })
    .await
    .expect("the server closes the app connection")
}

fn write_bundle(root: &Path, app_js: &str) {
    let dir = root.join("apps/notes");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("intent-app.json"),
        json!({
            "manifestVersion": 1,
            "id": "quick-notes",
            "name": "Quick Notes",
            "version": "1.0.0",
            "permissions": { "level": 1 },
            "window": { "width": 800, "height": 600 }
        })
        .to_string(),
    )
    .unwrap();
    std::fs::write(dir.join("index.html"), "<script src=app.js></script>").unwrap();
    std::fs::write(dir.join("app.js"), app_js).unwrap();
}

#[track_caller]
fn assert_forbidden_without_data(v: &Value, what: &str) {
    assert_eq!(v["error"]["code"], -32003, "{what}: {v}");
    assert_eq!(v["error"]["message"], "Forbidden", "{what}: {v}");
    assert!(v["error"].get("data").is_none(), "{what}: {v}");
}

#[intent_test_macros::daemon_test]
async fn app_library_session_allowlist_and_reapproval_over_wss() {
    let fx = boot().await;
    write_bundle(&fx.checkout, "v1");
    let mut admin = connect(fx.port).await;

    // Discovery.
    let hello = ok(
        &mut admin,
        1,
        "client.hello",
        json!({ "clientId": "apps-e2e", "clientName": "e2e" }),
    )
    .await;
    assert_eq!(hello["server"]["capabilities"]["intentApps"], 1, "{hello}");
    let me = ok(&mut admin, 2, "principal.me", json!({})).await;
    assert_eq!(me["kind"], "person", "{me}");

    let sub = ok(
        &mut admin,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["app:installed", "app:approval-changed", "app:removed"] }),
    )
    .await;
    assert!(sub["subscriptionId"].is_string(), "{sub}");

    // Install: pending, then approve the hash the user saw.
    let mut seen = Vec::new();
    let installed = rpc_seen(
        &mut admin,
        4,
        "app.install",
        json!({ "workspaceId": WS_ID, "path": "apps/notes" }),
        &mut seen,
    )
    .await;
    let app = installed["result"]["app"].clone();
    assert_eq!(installed["result"]["replaced"], false, "{installed}");
    assert_eq!(installed["result"]["dryRun"], false);
    assert_eq!(app["id"], "quick-notes");
    assert_eq!(app["entry"], "index.html");
    assert_eq!(app["fileCount"], 3);
    assert_eq!(app["approval"]["status"], "pending");
    assert_eq!(app["open"], false);
    // Reserved keys are always present (null without publisher / signature).
    assert_eq!(app["publisher"], Value::Null);
    assert_eq!(app["updateUrl"], Value::Null);
    assert_eq!(app["signature"], Value::Null);
    assert_eq!(
        app["window"],
        json!({ "width": 800, "height": 600, "minWidth": null, "minHeight": null, "resizable": true, "alwaysOnTop": false })
    );
    assert_eq!(
        app["source"],
        json!({ "workspaceId": WS_ID, "path": "apps/notes" })
    );
    let hash1 = app["bundleHash"].as_str().unwrap().to_owned();
    assert!(hash1.starts_with("sha256:") && hash1.len() == 71, "{hash1}");

    let exists = rpc(
        &mut admin,
        5,
        "app.install",
        json!({ "workspaceId": WS_ID, "path": "apps/notes" }),
    )
    .await;
    assert_eq!(exists["error"]["code"], -32602, "{exists}");
    assert_eq!(exists["error"]["data"]["code"], "app-exists");

    let not_approved = rpc(
        &mut admin,
        6,
        "app.session.open",
        json!({ "appId": "quick-notes" }),
    )
    .await;
    assert_eq!(
        not_approved["error"]["data"]["code"], "not-approved",
        "{not_approved}"
    );

    let approved = ok(
        &mut admin,
        7,
        "app.approve",
        json!({ "appId": "quick-notes", "bundleHash": hash1, "level": 1, "features": [] }),
    )
    .await;
    assert_eq!(approved["app"]["approval"]["status"], "approved");
    assert_eq!(approved["app"]["approval"]["bundleHash"], json!(hash1));

    let session = ok(
        &mut admin,
        8,
        "app.session.open",
        json!({ "appId": "quick-notes" }),
    )
    .await;
    let token = session["token"].as_str().unwrap().to_owned();
    assert_eq!(token.len(), 64);
    assert_eq!(session["level"], 1);
    assert_eq!(session["features"], json!([]));
    assert!(session["sessionId"].as_str().unwrap().starts_with("as_"));

    // The app connection: attach first, then only the level 1 list.
    let mut app_conn = connect(fx.port).await;
    let attached = ok(
        &mut app_conn,
        1,
        "app.session.attach",
        json!({ "token": token }),
    )
    .await;
    assert!(attached.get("token").is_none(), "{attached}");
    assert_eq!(attached["appId"], "quick-notes");
    assert_eq!(attached["bundleHash"], json!(hash1));
    let me = ok(&mut app_conn, 2, "principal.me", json!({})).await;
    assert_eq!(me["kind"], "app", "{me}");
    assert_eq!(me["id"], "app:quick-notes");
    assert_eq!(me["displayName"], "Quick Notes");
    assert_eq!(me["isAdministrator"], false);
    let set = ok(
        &mut app_conn,
        3,
        "app.storage.set",
        json!({ "key": "draft", "value": { "text": "hi" } }),
    )
    .await;
    assert_eq!(set["usedBytes"], 18);
    let got = ok(
        &mut app_conn,
        4,
        "app.storage.get",
        json!({ "key": "draft" }),
    )
    .await;
    assert_eq!(got, json!({ "found": true, "value": { "text": "hi" } }));
    for (i, method) in [
        "client.hello",
        "events.subscribe",
        "host.exec",
        "agent.completeOnce",
        "models.list",
        "workspace.list",
        "app.list",
        "app.install",
        "app.approve",
        "app.session.open",
        "app.session.attach",
        "app.notify",
        "app.complete",
        "system.status",
        "no.such.method",
    ]
    .into_iter()
    .enumerate()
    {
        let id = 100 + i64::try_from(i).unwrap();
        let refused = rpc(&mut app_conn, id, method, json!({})).await;
        assert_forbidden_without_data(&refused, method);
    }

    // The library shows it open; the admin sees no token anywhere.
    let got = ok(&mut admin, 9, "app.get", json!({ "appId": "quick-notes" })).await;
    assert_eq!(got["app"]["open"], true);
    assert!(!got.to_string().contains(&token));

    // Edit one file and reinstall: a new hash, approval reset, and the app
    // connection is closed with 1008.
    write_bundle(&fx.checkout, "v2");
    let mut seen2 = Vec::new();
    let replaced = rpc_seen(
        &mut admin,
        10,
        "app.install",
        json!({ "workspaceId": WS_ID, "path": "apps/notes", "replace": true }),
        &mut seen2,
    )
    .await;
    assert_eq!(replaced["result"]["replaced"], true, "{replaced}");
    assert_eq!(replaced["result"]["approvalReset"], true);
    let hash2 = replaced["result"]["app"]["bundleHash"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(hash1, hash2);
    assert_eq!(replaced["result"]["app"]["approval"]["status"], "pending");
    let close = expect_close(&mut app_conn).await;
    assert_eq!(
        close,
        Some((CloseCode::Policy, "app session ended".to_owned())),
        "the app connection closes when the session ends"
    );

    // The old token is gone; a failed attach answers, then closes.
    let mut stale = connect(fx.port).await;
    let invalid = rpc(
        &mut stale,
        1,
        "app.session.attach",
        json!({ "token": token }),
    )
    .await;
    assert_eq!(invalid["error"]["code"], -32602, "{invalid}");
    assert_eq!(invalid["error"]["data"]["code"], "session-invalid");
    assert!(expect_close(&mut stale).await.is_some());

    // Attach after another request is refused, then closed.
    let mut late = connect(fx.port).await;
    ok(&mut late, 1, "principal.me", json!({})).await;
    let late_attach = rpc(&mut late, 2, "app.session.attach", json!({ "token": "00" })).await;
    assert_eq!(
        late_attach["error"]["data"]["code"], "attach-not-first",
        "{late_attach}"
    );
    assert!(expect_close(&mut late).await.is_some());

    // Re-approval is needed; the old hash cannot be replayed.
    let replay = rpc(
        &mut admin,
        11,
        "app.approve",
        json!({ "appId": "quick-notes", "bundleHash": hash1, "level": 1, "features": [] }),
    )
    .await;
    assert_eq!(replay["error"]["data"]["code"], "hash-mismatch", "{replay}");
    assert_eq!(replay["error"]["data"]["bundleHash"], json!(hash2));
    let open = rpc(
        &mut admin,
        12,
        "app.session.open",
        json!({ "appId": "quick-notes" }),
    )
    .await;
    assert_eq!(open["error"]["data"]["code"], "not-approved");

    // Uninstall publishes app:removed.
    let mut seen3 = Vec::new();
    let removed = rpc_seen(
        &mut admin,
        13,
        "app.uninstall",
        json!({ "appId": "quick-notes" }),
        &mut seen3,
    )
    .await;
    assert_eq!(removed["result"]["removed"], true, "{removed}");

    // Events: installed, installed (replace) + approval-changed (approve and
    // reset), removed. Collect anything still in flight.
    let mut events: Vec<Value> = seen.into_iter().chain(seen2).chain(seen3).collect();
    let _ = timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_millis(500)),
        async {
            while let Some(Ok(Message::Text(text))) = admin.next().await {
                events.push(serde_json::from_str(&text).unwrap());
            }
        },
    )
    .await;
    let types: Vec<String> = events
        .iter()
        .filter(|v| v["method"] == "events.event")
        .filter_map(|v| {
            v["params"]["event"]["type"]
                .as_str()
                .or_else(|| v["params"]["type"].as_str())
                .map(str::to_owned)
        })
        .collect();
    for expected in ["app:installed", "app:approval-changed", "app:removed"] {
        assert!(
            types.iter().any(|t| t == expected),
            "{expected} in {types:?}"
        );
    }
}
