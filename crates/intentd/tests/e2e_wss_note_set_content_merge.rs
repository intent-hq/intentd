//! WSS end-to-end for `note.setContent` with a stale `expectedVersion`
//! (docs/protocol/methods/notes-tasks.md §5.2): an agent-style `note.add`
//! advances the note past the rev a full-content writer read, and the
//! writer's `note.setContent` carrying that pre-add rev succeeds by
//! three-way-merging its intent onto the current text instead of failing with
//! `-32005`. The response carries the post-write `rev` (equal to `note.get`),
//! the merged content holds both edits, and each write emits exactly one
//! `note:updated`. Also pins the committed-`rev` contract of `note.update` /
//! `note.updateMetadata` (intent-hq/intent#5589). Drives a real
//! [`WsApiServer`] over TLS with bearer auth and a fingerprint-pinned client,
//! so the production wire path (TLS upgrade → JSON-RPC → router → services →
//! store) is exercised end-to-end.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{Result as CoreResult, WorkspaceApi};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use common::TlsWs;

/// A fixed 64-char hex token (valid shape) shared by server + client.
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

/// In-memory [`TokenStore`] so tests never touch the real OS keychain.
#[derive(Default)]
struct MemTokenStore(Mutex<Option<String>>);

impl TokenStore for MemTokenStore {
    fn load_token(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
    fn store_token(&self, token: &str) -> CoreResult<()> {
        *self.0.lock().unwrap() = Some(token.to_string());
        Ok(())
    }
}

/// Client cert verifier that pins the server's SHA-256 fingerprint (colon hex)
/// and otherwise validates the handshake signature with the ring provider.
#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fp = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if fp == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("fingerprint mismatch".into()))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config(fingerprint: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

struct Fixture {
    ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    store: Store,
    dir: tempfile::TempDir,
}

/// Boot a TLS + bearer-auth WSS listener over a hermetic workspaces root.
async fn boot() -> Fixture {
    let tmp = common::test_tempdir("intentd-setcontent-");
    let dir = tmp.path().to_path_buf();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store.clone())
        .with_workspaces_root(workspaces_root)
        .with_settings_registry(common::registry_with_default_provider(&dir))
        .with_event_bus(bus.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");
    Fixture {
        ws,
        port,
        cfg,
        store,
        dir: tmp,
    }
}

/// Establish an authenticated WSS connection over pinned TLS (token in the
/// query string).
async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let v = wss_rpc_raw(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

async fn wss_rpc_raw(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        return v;
                    }
                }
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout")
}

/// Drain `events.event` notifications from the subscriber socket until it has
/// been quiet for `settle`, returning the `note:updated` events for `note_id`.
async fn drain_note_updated(evt: &mut TlsWs, note_id: &str, settle: Duration) -> Vec<Value> {
    let mut seen = Vec::new();
    loop {
        match timeout(settle, evt.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    let event = &v["params"]["event"];
                    if event["type"] == "note:updated" && event["data"]["noteId"] == note_id {
                        seen.push(event.clone());
                    }
                }
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = evt.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
            Ok(other) => panic!("subscriber socket ended unexpectedly: {other:?}"),
            Err(_elapsed) => return seen,
        }
    }
}

/// Stale-rev `note.setContent` merges over the wire (§5.2 three-way merge):
/// `note.add` (agent-style append) moves the note to rev 1; a
/// `note.setContent` that read rev 0 and edits a different line succeeds,
/// keeps both edits, returns `rev` equal to `note.get`, and each write emits
/// exactly one `note:updated`.
#[intent_test_macros::daemon_test]
async fn note_set_content_stale_expected_version_merges_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let mut evt = connect(fx.port, fx.cfg.clone()).await;

    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "setContent merge e2e", "path": "." }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_string();

    let note = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Merge me", "content": "alpha\nbeta\ngamma" }),
    )
    .await;
    let note_id = note["note"]["id"].as_str().expect("note id").to_string();
    let base_rev = note["note"]["rev"].as_i64().expect("rev");
    assert_eq!(base_rev, 0, "freshly created note starts at rev 0");

    let sub = wss_rpc(
        &mut evt,
        3,
        "events.subscribe",
        json!({ "workspaceId": ws_id, "eventTypes": ["note:updated"] }),
    )
    .await;
    assert!(sub["subscriptionId"].is_string(), "subscribe: {sub}");

    // Agent-style append lands first and advances the note past `base_rev`.
    wss_rpc(
        &mut rpc,
        4,
        "note.add",
        json!({ "workspaceId": ws_id, "noteId": note_id, "content": "delta" }),
    )
    .await;
    let after_add = wss_rpc(
        &mut rpc,
        5,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    let add_rev = after_add["note"]["rev"].as_i64().expect("rev after add");
    assert_eq!(add_rev, base_rev + 1);
    let add_events = drain_note_updated(&mut evt, &note_id, Duration::from_millis(500)).await;
    assert_eq!(
        add_events.len(),
        1,
        "note.add emits exactly one note:updated: {add_events:?}"
    );

    // The full-content writer still carries the pre-add rev: instead of
    // `-32005` its intent (beta → beta-A) is merged onto the current text.
    let set = wss_rpc(
        &mut rpc,
        6,
        "note.setContent",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "alpha\nbeta-A\ngamma",
            "expectedVersion": base_rev,
        }),
    )
    .await;
    assert_eq!(set["ok"], json!(true));
    assert_eq!(set["noteId"], json!(note_id));
    let new_content = set["newContent"].as_str().expect("newContent");
    assert!(
        new_content.contains("beta-A"),
        "writer's edit applied: {new_content:?}"
    );
    assert!(
        new_content.contains("delta"),
        "concurrent append preserved: {new_content:?}"
    );
    assert!(
        !new_content.contains("\nbeta\n"),
        "replaced base line is gone: {new_content:?}"
    );
    let set_rev = set["rev"].as_i64().expect("result rev");
    assert_eq!(set_rev, add_rev + 1, "merge write bumps rev once");

    let after_set = wss_rpc(
        &mut rpc,
        7,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(after_set["note"]["rev"], json!(set_rev));
    assert_eq!(after_set["note"]["content"], json!(new_content));

    let set_events = drain_note_updated(&mut evt, &note_id, Duration::from_millis(500)).await;
    assert_eq!(
        set_events.len(),
        1,
        "note.setContent emits exactly one note:updated: {set_events:?}"
    );
    assert_eq!(set_events[0]["workspaceId"], json!(ws_id));
    assert_eq!(set_events[0]["data"]["action"], json!("update"));

    // The non-merging conditional writes keep the `-32005` contract.
    let stale_meta = wss_rpc_raw(
        &mut rpc,
        8,
        "note.updateMetadata",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "title": "stale",
            "expectedVersion": base_rev,
        }),
    )
    .await;
    assert_eq!(stale_meta["error"]["code"], json!(-32005), "{stale_meta}");
    assert_eq!(stale_meta["error"]["data"]["code"], json!("conflict"));
    assert_eq!(
        stale_meta["error"]["data"]["current"]["rev"],
        json!(set_rev)
    );
}

/// Assert the full `-32005` error envelope for a `note.setContent` that did
/// not write (docs/protocol/09-error-codes.md §9): `id` echoed, `jsonrpc`
/// `"2.0"`, no `result`, `error.message` `"Conflict"`, `error.data.code`
/// `"conflict"`, and `error.data.current` carrying the untouched entity.
fn assert_conflict_envelope(v: &Value, id: i64, content: &str, rev: i64) {
    assert_eq!(v["id"], json!(id), "{v}");
    assert_eq!(v["jsonrpc"], json!("2.0"), "{v}");
    assert!(v.get("result").is_none(), "no result on conflict: {v}");
    assert_eq!(v["error"]["code"], json!(-32005), "{v}");
    assert_eq!(v["error"]["message"], json!("Conflict"), "{v}");
    assert_eq!(v["error"]["data"]["code"], json!("conflict"), "{v}");
    assert_eq!(v["error"]["data"]["current"]["content"], json!(content));
    assert_eq!(v["error"]["data"]["current"]["rev"], json!(rev));
}

/// The two `note.setContent` paths that do NOT merge share one wire contract
/// (§5.2, §9): an `expectedVersion` above the current rev is rejected
/// immediately, and a read-merge-persist loop whose every attempt misses its
/// gate (a `RAISE(IGNORE)` trigger on the note row) is rejected after the
/// bounded retries. Both return the identical `-32005` envelope carrying the
/// untouched entity, persist nothing, and leave the daemon able to accept the
/// next write on the same connection.
#[intent_test_macros::daemon_test]
async fn note_set_content_future_rev_and_retry_exhaustion_conflict_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;

    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "setContent conflict e2e", "path": "." }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_string();
    let note = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Conflict me", "content": "body" }),
    )
    .await;
    let note_id = note["note"]["id"].as_str().expect("note id").to_string();
    assert_eq!(note["note"]["rev"], json!(0));

    // A rev this note never served: Conflict without a write.
    let future = wss_rpc_raw(
        &mut rpc,
        3,
        "note.setContent",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "body future",
            "expectedVersion": 7,
        }),
    )
    .await;
    assert_conflict_envelope(&future, 3, "body", 0);

    // Every gated UPDATE misses: the bounded loop ends in the same Conflict.
    sqlx::raw_sql(
        "CREATE TABLE cas_misses(n INTEGER);
         INSERT INTO cas_misses VALUES (0);
         CREATE TRIGGER force_cas_miss BEFORE UPDATE ON note BEGIN
             UPDATE cas_misses SET n = n + 1;
             SELECT RAISE(IGNORE);
         END;",
    )
    .execute(fx.store.write_pool())
    .await
    .expect("arm trigger");
    let exhausted = wss_rpc_raw(
        &mut rpc,
        4,
        "note.setContent",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "body attempted",
            "expectedVersion": 0,
        }),
    )
    .await;
    assert_conflict_envelope(&exhausted, 4, "body", 0);
    assert_eq!(
        exhausted["error"], future["error"],
        "identical error payloads"
    );
    let misses: i64 = sqlx::query_scalar("SELECT n FROM cas_misses")
        .fetch_one(fx.store.read_pool())
        .await
        .expect("count misses");
    assert_eq!(misses, 5, "retry budget: exactly five gated attempts");

    let after = wss_rpc(
        &mut rpc,
        5,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(after["note"]["content"], json!("body"));
    assert_eq!(after["note"]["rev"], json!(0));
    let versions = wss_rpc(
        &mut rpc,
        6,
        "note.listVersions",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(
        versions.as_array().map(Vec::len),
        Some(1),
        "only the creation snapshot: {versions}"
    );

    // Same connection, trigger gone: the next write lands at rev 1.
    sqlx::query("DROP TRIGGER force_cas_miss")
        .execute(fx.store.write_pool())
        .await
        .expect("disarm trigger");
    let accepted = wss_rpc(
        &mut rpc,
        7,
        "note.setContent",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "body accepted",
            "expectedVersion": 0,
        }),
    )
    .await;
    assert_eq!(accepted["ok"], json!(true));
    assert_eq!(accepted["newContent"], json!("body accepted"));
    assert_eq!(accepted["rev"], json!(1));
}

/// Regression (intent-hq/intent#5589): every successful `note.update` and
/// `note.updateMetadata` carries the committed `rev`, equal to `note.get`, so
/// a client can chain the returned rev into its next `expectedVersion` —
/// across a content write, a `@@@task` conversion write, the metadata arm of
/// `note.update`, and `note.updateMetadata`. The rev each response supersedes
/// is rejected with `-32005` without a write.
#[intent_test_macros::daemon_test]
async fn note_update_returns_committed_rev_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;

    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "note.update rev e2e", "path": "." }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_string();
    let note = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Rev me", "content": "v0" }),
    )
    .await;
    let note_id = note["note"]["id"].as_str().expect("note id").to_string();
    assert_eq!(note["note"]["rev"], json!(0));

    async fn get_note(rpc: &mut TlsWs, id: i64, ws_id: &str, note_id: &str) -> Value {
        wss_rpc(
            rpc,
            id,
            "note.get",
            json!({ "workspaceId": ws_id, "noteId": note_id }),
        )
        .await["note"]
            .clone()
    }

    // Content write at the created rev: the response is the committed row.
    let content = wss_rpc(
        &mut rpc,
        3,
        "note.update",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "v1",
            "expectedVersion": 0,
        }),
    )
    .await;
    let persisted = get_note(&mut rpc, 4, &ws_id, &note_id).await;
    assert_eq!(persisted["rev"], json!(1));
    assert_eq!(content["note"]["rev"], persisted["rev"], "{content}");
    assert_eq!(content["note"]["updatedAt"], persisted["updatedAt"]);
    assert_eq!(content["note"]["content"], json!("v1"));
    let rev = content["note"]["rev"].as_i64().expect("rev");

    // Chained: a `@@@task` block converts, and the response rev is the rev
    // of the converted row, again equal to `note.get`.
    let converted = wss_rpc(
        &mut rpc,
        5,
        "note.update",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "intro\n\n@@@task\n# Do it\nbody\n@@@\n",
            "expectedVersion": rev,
        }),
    )
    .await;
    let persisted = get_note(&mut rpc, 6, &ws_id, &note_id).await;
    assert!(
        persisted["content"]
            .as_str()
            .is_some_and(|c| c.contains("intent://local/task/")),
        "conversion rewrote the fence: {persisted}"
    );
    assert_eq!(converted["note"]["rev"], persisted["rev"], "{converted}");
    assert_eq!(converted["note"]["content"], persisted["content"]);
    let rev = converted["note"]["rev"].as_i64().expect("rev");
    assert!(
        rev > 1,
        "conversion advanced the rev past the content write"
    );

    // Chained: the metadata arm of `note.update` returns the committed row,
    // content included.
    let renamed = wss_rpc(
        &mut rpc,
        7,
        "note.update",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "title": "Renamed",
            "tags": ["x"],
            "expectedVersion": rev,
        }),
    )
    .await;
    let persisted = get_note(&mut rpc, 8, &ws_id, &note_id).await;
    assert_eq!(persisted["rev"], json!(rev + 1));
    assert_eq!(renamed["note"]["rev"], persisted["rev"], "{renamed}");
    assert_eq!(renamed["note"]["updatedAt"], persisted["updatedAt"]);
    assert_eq!(renamed["note"]["title"], json!("Renamed"));
    assert_eq!(renamed["note"]["tags"], json!(["x"]));
    assert_eq!(renamed["note"]["content"], persisted["content"]);
    let rev = renamed["note"]["rev"].as_i64().expect("rev");

    // Chained: `note.updateMetadata` carries `rev` alongside the stored
    // `updatedAt`.
    let meta = wss_rpc(
        &mut rpc,
        9,
        "note.updateMetadata",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "title": "Renamed again",
            "expectedVersion": rev,
        }),
    )
    .await;
    let persisted = get_note(&mut rpc, 10, &ws_id, &note_id).await;
    assert_eq!(persisted["rev"], json!(rev + 1));
    assert_eq!(meta["ok"], json!(true));
    assert_eq!(meta["rev"], persisted["rev"], "{meta}");
    assert_eq!(meta["updatedAt"], persisted["updatedAt"]);
    assert_eq!(meta["title"], json!("Renamed again"));
    let committed = meta["rev"].as_i64().expect("rev");

    // The rev the last response superseded is stale on both methods: -32005
    // carrying the current entity, nothing written.
    let stale_update = wss_rpc_raw(
        &mut rpc,
        11,
        "note.update",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "should-not-persist",
            "expectedVersion": rev,
        }),
    )
    .await;
    assert_conflict_envelope(
        &stale_update,
        11,
        persisted["content"].as_str().unwrap(),
        committed,
    );
    let stale_meta = wss_rpc_raw(
        &mut rpc,
        12,
        "note.updateMetadata",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "title": "should-not-persist",
            "expectedVersion": rev,
        }),
    )
    .await;
    assert_conflict_envelope(
        &stale_meta,
        12,
        persisted["content"].as_str().unwrap(),
        committed,
    );
    let after = get_note(&mut rpc, 13, &ws_id, &note_id).await;
    assert_eq!(after["rev"], json!(committed));
    assert_eq!(after["title"], json!("Renamed again"));

    // The committed rev still chains.
    let next = wss_rpc(
        &mut rpc,
        14,
        "note.updateMetadata",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "tags": ["y"],
            "expectedVersion": committed,
        }),
    )
    .await;
    assert_eq!(next["rev"], json!(committed + 1));
}

#[tokio::test]
async fn bounded_note_source_pages_preserve_exact_source_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"paged notes", "path":"."}),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap();
    let source = "a😀\r\n重复e\u{301}\t\"\\".repeat(3000);
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({"workspaceId":ws_id,"title":"large", "content":source}),
    )
    .await;
    let note_id = created["note"]["id"].as_str().unwrap();
    let mut page = json!({"kind":"source","maxSourceBytes":127,"maxWireBytes":4096});
    let mut reconstructed = String::new();
    loop {
        let result = wss_rpc(
            &mut rpc,
            3,
            "note.get",
            json!({"workspaceId":ws_id,"noteId":note_id,"page":page}),
        )
        .await;
        assert_eq!(
            result["kind"], "noteSourcePage",
            "must never hydrate a complete Note on the page path"
        );
        assert!(result.get("note").is_none());
        assert!(result.get("content").is_none());
        assert_eq!(
            result["range"]["start"],
            reconstructed.encode_utf16().count()
        );
        let text = result["text"].as_str().unwrap();
        assert!(text.len() <= 127);
        assert!(
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":3,"result":result}))
                .unwrap()
                .len()
                <= 4096
        );
        reconstructed.push_str(text);
        assert_eq!(result["range"]["end"], reconstructed.encode_utf16().count());
        if result["nextCursor"].is_null() {
            break;
        }
        page = json!({"kind":"source","cursor":result["nextCursor"],"maxSourceBytes":127,"maxWireBytes":4096});
    }
    assert_eq!(reconstructed, source);
}

#[tokio::test]
async fn bounded_note_pages_wire_rejections_and_legacy_writer_invalidation() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"page guards","path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({"workspaceId":ws,"title":"a title","content":format!("A😀\r\ntext unique {}", "A😀\r\ntext ".repeat(800))}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    for page in [
        Value::Null,
        json!({"kind":"unknown"}),
        json!({"kind":"source","at":2}),
        json!({"kind":"source","maxSourceBytes":3}),
        json!({"kind":"source","maxWireBytes":4095}),
        json!({"kind":"source","maxItems":129}),
        json!({"kind":"source","at":null}),
        json!({"kind":"source","version":2}),
    ] {
        let got = wss_rpc_raw(
            &mut rpc,
            3,
            "note.get",
            json!({"workspaceId":ws,"noteId":note,"page":page}),
        )
        .await;
        assert_eq!(got["jsonrpc"], "2.0");
        assert_eq!(got["id"], 3);
        assert_eq!(got["error"]["code"], -32602, "{got}");
        assert!(got.to_string().len() <= 4096);
    }
    let source = wss_rpc(
        &mut rpc,
        4,
        "note.get",
        json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","maxSourceBytes":4}}),
    )
    .await;
    let context=wss_rpc(&mut rpc,5,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":source["contextRef"],"maxWireBytes":4096}})).await;
    assert_eq!(context["kind"], "noteContextPage");
    assert_eq!(context["scope"], source["scope"]);
    assert_eq!(context["sourceRevision"], source["sourceRevision"]);
    let metadata=wss_rpc(&mut rpc,6,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"metadata","ref":source["metadataRef"]}})).await;
    assert_eq!(metadata["kind"], "noteMetadataPage");
    assert_eq!(metadata["snapshotId"], source["snapshotId"]);
    assert!(metadata.get("content").is_none());
    for (method, extra) in [
        ("note.updateMetadata", json!({"title":"changed"})),
        ("note.add", json!({"content":"tail"})),
        (
            "comment.add",
            json!({"searchContext":"text unique","commentTarget":"text","comment":"indexed anchor"}),
        ),
        ("note.edit", json!({"old":"text","new":"word"})),
        (
            "note.editLines",
            json!({"start":1,"end":1,"content":"updated first line"}),
        ),
        (
            "note.update",
            json!({"content":"new full content repeated".repeat(100)}),
        ),
        (
            "note.setContent",
            json!({"content":"final full content".repeat(100),"confirmReplacement":true}),
        ),
        (
            "note.update",
            json!({"content":"- [ ] indexed marker\nmore source"}),
        ),
        (
            "task.updateStatus",
            json!({"taskText":"indexed marker","status":"done"}),
        ),
        ("note.restoreVersion", json!({"v":1})),
    ] {
        let before = wss_rpc(
            &mut rpc,
            7,
            "note.get",
            json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","maxSourceBytes":4}}),
        )
        .await;
        let mut params = extra;
        params["workspaceId"] = json!(ws);
        params["noteId"] = json!(note);
        wss_rpc(&mut rpc, 8, method, params).await;
        let stale=wss_rpc_raw(&mut rpc,9,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","cursor":before["nextCursor"],"maxSourceBytes":4}})).await;
        assert_eq!(stale["error"]["code"], -32005, "{method}: {stale}");
        assert_eq!(stale["error"]["data"]["code"], "note-page-stale");
        assert!(stale["error"]["data"].get("current").is_none());
        let legacy = wss_rpc(
            &mut rpc,
            10,
            "note.get",
            json!({"workspaceId":ws,"noteId":note}),
        )
        .await;
        assert!(legacy["note"]["content"].is_string());
        assert!(legacy.get("kind").is_none());
    }
    let huge=wss_rpc_raw(&mut rpc,11,"note.get",json!({"workspaceId":ws,"noteId":note,"padding":"x".repeat(66000),"page":{"kind":"source"}})).await;
    assert_eq!(huge["error"]["data"]["code"], "note-page-budget");
    assert!(huge.to_string().len() <= 4096);
    let hello = wss_rpc(
        &mut rpc,
        12,
        "client.hello",
        json!({"clientId":"bounded-test","clientName":"test","clientVersion":"1"}),
    )
    .await;
    assert!(hello["server"]["capabilities"].get("notePaging").is_none());
}

#[tokio::test]
async fn bounded_note_task_ids_summary_is_authoritative_and_revision_bound_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"task links","path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    let text = "prose [first](intent://local/task/raw%20id) [second](intent://local/task/missing)\n```\n[duplicate](intent://local/task/raw%20id)\n```";
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({"workspaceId":ws,"title":"links","content":text}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    let first = wss_rpc(
        &mut rpc,
        3,
        "note.get",
        json!({"workspaceId":ws,"noteId":note,"page":{"kind":"taskIds","maxItems":1}}),
    )
    .await;
    assert_eq!(first["kind"], "noteTaskIdsPage");
    assert_eq!(first["totalItems"], 2);
    assert_eq!(first["items"][0]["taskNoteId"], "raw%20id");
    let second = wss_rpc(&mut rpc,4,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"taskIds","maxItems":1,"cursor":first["nextCursor"]}})).await;
    assert_eq!(second["items"][0]["taskNoteId"], "missing");
    assert_eq!(second["items"][0]["index"], 1);
    assert_eq!(second["nextCursor"], Value::Null);
    assert_eq!(second["snapshotId"], first["snapshotId"]);
    wss_rpc(
        &mut rpc,
        5,
        "note.updateMetadata",
        json!({"workspaceId":ws,"noteId":note,"title":"renamed"}),
    )
    .await;
    let stale = wss_rpc_raw(&mut rpc,6,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"taskIds","maxItems":1,"cursor":first["nextCursor"]}})).await;
    assert_eq!(stale["error"]["data"]["code"], "note-page-stale");
    let legacy = wss_rpc(
        &mut rpc,
        7,
        "note.get",
        json!({"workspaceId":ws,"noteId":note}),
    )
    .await["note"]
        .clone();
    assert_eq!(legacy["content"], text);
}

#[tokio::test]
async fn bounded_note_pages_restart_expires_cursor_and_preserves_database_identity_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"restart paging","path":"."}),
    )
    .await;
    let ws = created["workspace"]["id"].as_str().unwrap();
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({"workspaceId":ws,"title":"restart","content":"source across restart"}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    let first = wss_rpc(
        &mut rpc,
        3,
        "note.get",
        json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","maxSourceBytes":4}}),
    )
    .await;
    rpc.close(None).await.unwrap();
    fx.ws.stop().await;
    let database = fx.dir.path().join("intentd.db");
    let store = Store::open(&database).await.unwrap();
    let bus = EventBus::new(store.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(Services::new(store).with_event_bus(bus.clone()));
    let tls = ensure_tls_certificate(fx.dir.path()).unwrap();
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let server = WsApiServer::new(
        api,
        bus,
        &tls,
        &token_store,
        WsOptions {
            base_port: 0,
            bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let port = server.start().await.unwrap();
    let mut rpc = connect(port, fx.cfg.clone()).await;
    let expired = wss_rpc_raw(&mut rpc,4,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","maxSourceBytes":4,"cursor":first["nextCursor"]}})).await;
    assert_eq!(expired["error"]["data"]["code"], "note-page-expired");
    let second = wss_rpc(
        &mut rpc,
        5,
        "note.get",
        json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source"}}),
    )
    .await;
    assert_eq!(second["scope"], first["scope"]);
    assert_eq!(second["sourceRevision"], first["sourceRevision"]);
    assert_ne!(second["snapshotId"], first["snapshotId"]);
    assert_eq!(second["text"], "source across restart");
    server.stop().await;
}

#[tokio::test]
async fn bounded_note_table_positions_support_far_cell_seek_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"table coordinates","path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    let text = format!(
        "| A | B | C |\r\n| :--- | :---: | ---: |\r\n{}| {} | same | target😀 |\r\n",
        "| same | same | same |\r\n".repeat(1000),
        "😀x".repeat(20000)
    );
    let at = text[..text.find("target😀").unwrap()]
        .encode_utf16()
        .count();
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({"workspaceId":ws,"title":"table","content":text}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    let source = wss_rpc(&mut rpc, 3, "note.get", json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","at":at,"maxSourceBytes":16}})).await;
    let mut request = json!({"kind":"context","contextRef":source["contextRef"],"maxItems":8,"maxWireBytes":4096});
    let cell = loop {
        let frame = wss_rpc_raw(
            &mut rpc,
            4,
            "note.get",
            json!({"workspaceId":ws,"noteId":note,"page":request}),
        )
        .await;
        assert!(frame.to_string().len() <= 4096);
        let context = &frame["result"];
        if let Some(cell) = context["items"].as_array().unwrap().iter().find(|item| {
            item["construct"] == "tableCell"
                && item["sourceRange"]["start"].as_u64().unwrap() <= at as u64
                && item["sourceRange"]["end"].as_u64().unwrap() > at as u64
        }) {
            break cell.clone();
        }
        assert!(!context["nextCursor"].is_null());
        request["cursor"] = context["nextCursor"].clone();
    };
    assert_eq!(cell["tablePosition"]["rowIndex"], 1001);
    assert_eq!(cell["tablePosition"]["columnIndex"], 2);
    assert_eq!(cell["tablePosition"]["alignment"], "right");
    let owner = wss_rpc(&mut rpc,5,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":cell["tablePosition"]["tableRef"]}})).await;
    assert_eq!(owner["items"][0]["construct"], "table");
    assert_eq!(owner["items"][0]["continuationBefore"], true);
    assert_eq!(owner["snapshotId"], source["snapshotId"]);
    wss_rpc(
        &mut rpc,
        6,
        "note.updateMetadata",
        json!({"workspaceId":ws,"noteId":note,"title":"changed"}),
    )
    .await;
    let stale = wss_rpc_raw(&mut rpc,7,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":cell["tablePosition"]["tableRef"]}})).await;
    assert_eq!(stale["error"]["data"]["code"], "note-page-stale");
}

#[tokio::test]
async fn bounded_note_html_native_maps_preserve_far_header_and_revision_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"HTML maps","path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    let text = format!(
        "<table><tr><td>{}</td><td>SECOND</td><th><strong>TARGET😀</strong></th></tr></table>",
        "x".repeat(2_000_000)
    );
    let at = text.find("TARGET").unwrap();
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({"workspaceId":ws,"title":"HTML","content":text}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    let source=wss_rpc(&mut rpc,3,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","at":at,"maxSourceBytes":10,"maxWireBytes":8192}})).await;
    assert_eq!(source["text"], "TARGET😀");
    let mut request =
        json!({"kind":"context","contextRef":source["contextRef"],"maxWireBytes":8192});
    let cell = loop {
        let frame = wss_rpc_raw(
            &mut rpc,
            4,
            "note.get",
            json!({"workspaceId":ws,"noteId":note,"page":request}),
        )
        .await;
        assert!(frame.to_string().len() <= 8192);
        let context = &frame["result"];
        if let Some(cell) = context["items"].as_array().unwrap().iter().find(|item| {
            item["construct"] == "htmlTableCell" && item["htmlPosition"]["columnIndex"] == 2
        }) {
            break cell.clone();
        }
        assert!(!context["nextCursor"].is_null());
        request["cursor"] = context["nextCursor"].clone();
    };
    assert_eq!(cell["htmlPosition"]["cellRole"], "header");
    let maps=wss_rpc(&mut rpc,5,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":cell["sourceMapRef"],"maxWireBytes":4096}})).await;
    let map = maps["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["mapping"] == "identity" && item["sourceRange"]["start"] == at)
        .unwrap();
    assert_eq!(map["sourceRange"]["end"], at + 8);
    let rendered=wss_rpc(&mut rpc,6,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":map["textRef"],"maxWireBytes":4096}})).await;
    assert_eq!(rendered["items"][0]["text"], "TARGET😀");
    let native=wss_rpc(&mut rpc,7,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":map["textNodeRef"]}})).await;
    assert_eq!(native["items"][0]["id"], map["textNodeId"]);
    assert!(native["items"][0]["marksRef"].is_string());
    let attrs=wss_rpc(&mut rpc,8,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"metadata","ref":cell["attributesRef"]}})).await;
    assert_eq!(attrs["items"][0]["type"], "object");
    wss_rpc(
        &mut rpc,
        9,
        "note.updateMetadata",
        json!({"workspaceId":ws,"noteId":note,"title":"changed"}),
    )
    .await;
    let stale=wss_rpc_raw(&mut rpc,10,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":map["textRef"]}})).await;
    assert_eq!(stale["error"]["data"]["code"], "note-page-stale");
}

#[tokio::test]
async fn bounded_note_inline_code_maps_preserve_far_body_and_delimiters_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"Code maps","path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    let ticks = "`".repeat(100_001);
    let text = format!(
        "before {ticks}{}TARGET😀{ticks} after",
        "x".repeat(2_000_000)
    );
    let at = text.find("TARGET").unwrap();
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({"workspaceId":ws,"title":"Code","content":text}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    let mut body_owner = None;
    for (index, seek) in [50_000, at].into_iter().enumerate() {
        let id = 10 + i64::try_from(index).unwrap() * 10;
        let source=wss_rpc(&mut rpc,id,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","at":seek,"maxSourceBytes":10,"maxWireBytes":8192}})).await;
        let frame=wss_rpc_raw(&mut rpc,id+1,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":source["contextRef"],"maxWireBytes":8192}})).await;
        assert!(frame.to_string().len() <= 8192);
        let owner = frame["result"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["role"] == "code")
            .unwrap();
        assert_eq!(
            owner["codeSource"]["openingRange"],
            json!({"start":7,"end":100_008})
        );
        let maps=wss_rpc(&mut rpc,id+2,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":owner["sourceMapRef"],"maxWireBytes":8192}})).await;
        if seek == 50_000 {
            assert!(maps["items"]
                .as_array()
                .unwrap()
                .iter()
                .all(|map| map["mapping"] == "omitted" && map["textRef"].is_null()));
            assert_eq!(maps["items"][0]["sourceRange"], source["range"]);
        } else {
            assert_eq!(source["text"], "TARGET😀");
            let map = maps["items"]
                .as_array()
                .unwrap()
                .iter()
                .find(|map| map["mapping"] == "identity" && map["sourceRange"]["start"] == at)
                .unwrap();
            assert_eq!(map["sourceRange"]["end"], at + 8);
            assert_eq!(map["textNodeRef"], owner["nativeRef"]);
            let rendered=wss_rpc(&mut rpc,id+3,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":map["textRef"]}})).await;
            assert_eq!(rendered["items"][0]["text"], "TARGET😀");
            let stable=wss_rpc(&mut rpc,id+4,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":map["ownerRef"]}})).await;
            assert_eq!(stable["items"][0]["id"], owner["id"]);
            assert!(stable["items"][0].get("sourceMapRef").is_none());
            assert!(stable["items"][0].get("continuationBefore").is_none());
            body_owner = Some(owner["sourceMapRef"].clone());
        }
    }
    wss_rpc(
        &mut rpc,
        40,
        "note.updateMetadata",
        json!({"workspaceId":ws,"noteId":note,"title":"invalidated"}),
    )
    .await;
    let stale=wss_rpc_raw(&mut rpc,41,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":body_owner.unwrap()}})).await;
    assert_eq!(stale["error"]["data"]["code"], "note-page-stale");
}

#[tokio::test]
async fn bounded_note_document_owner_and_native_primitive_values_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"Canonical sources","path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    let text = "<table><tr><td>x</td></tr></table>\n\n`After`";
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({"workspaceId":ws,"title":"HTML tail","content":text}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    let at = text.find('`').unwrap();
    let source = wss_rpc(&mut rpc, 3, "note.get", json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","at":at,"maxSourceBytes":32,"maxWireBytes":8192}})).await;
    let frame = wss_rpc_raw(&mut rpc, 4, "note.get", json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":source["contextRef"],"maxWireBytes":8192}})).await;
    assert!(frame.to_string().len() <= 8192);
    let items = frame["result"]["items"].as_array().unwrap();
    assert!(!items.iter().any(|item| item["role"] == "code"));
    let document = items
        .iter()
        .find(|item| item["construct"] == "htmlDocument")
        .unwrap();
    let maps = wss_rpc(&mut rpc, 5, "note.get", json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":document["sourceMapRef"],"maxWireBytes":4096}})).await;
    let map = maps["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["mapping"] == "identity")
        .unwrap();
    let rendered = wss_rpc(&mut rpc, 6, "note.get", json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":map["textRef"],"maxWireBytes":4096}})).await;
    assert_eq!(rendered["items"][0]["text"], "`After`");
    let direct = wss_rpc(&mut rpc, 7, "note.get", json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":map["ownerRef"]}})).await;
    assert_eq!(direct["items"][0]["id"], document["id"]);
    assert!(direct["items"][0].get("sourceMapRef").is_none());
    let primitive = wss_rpc(&mut rpc, 8, "note.create", json!({"workspaceId":ws,"title":"Native source","content":"```diff title\n-café & old\n+世界 <new>\n```"})).await;
    let primitive_id = primitive["note"]["id"].as_str().unwrap();
    let source = wss_rpc(&mut rpc, 9, "note.get", json!({"workspaceId":ws,"noteId":primitive_id,"page":{"kind":"source","maxSourceBytes":4096}})).await;
    let context = wss_rpc(&mut rpc, 10, "note.get", json!({"workspaceId":ws,"noteId":primitive_id,"page":{"kind":"context","contextRef":source["contextRef"]}})).await;
    let atom = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["nodeType"] == "diffBlock")
        .unwrap();
    assert!(atom["nativeRef"].is_string());
    let root = wss_rpc(&mut rpc, 11, "note.get", json!({"workspaceId":ws,"noteId":primitive_id,"page":{"kind":"metadata","ref":atom["attributesRef"]}})).await;
    let fields = wss_rpc(&mut rpc, 12, "note.get", json!({"workspaceId":ws,"noteId":primitive_id,"page":{"kind":"metadata","ref":root["items"][0]["childrenRef"]}})).await;
    let code = fields["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|field| field["key"] == "code")
        .unwrap();
    let value = wss_rpc(&mut rpc, 13, "note.get", json!({"workspaceId":ws,"noteId":primitive_id,"page":{"kind":"context","contextRef":code["valueRef"],"maxWireBytes":4096}})).await;
    assert_eq!(value["items"][0]["text"], "-café & old\n+世界 <new>");
    let cross_note = wss_rpc_raw(&mut rpc, 14, "note.get", json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":code["valueRef"]}})).await;
    assert!(cross_note.get("error").is_some());
    wss_rpc(
        &mut rpc,
        15,
        "note.updateMetadata",
        json!({"workspaceId":ws,"noteId":primitive_id,"title":"Changed"}),
    )
    .await;
    let stale = wss_rpc_raw(&mut rpc, 16, "note.get", json!({"workspaceId":ws,"noteId":primitive_id,"page":{"kind":"context","contextRef":code["valueRef"]}})).await;
    assert_eq!(stale["error"]["data"]["code"], "note-page-stale");
}

#[tokio::test]
async fn bounded_note_markdown_paragraph_maps_and_breaks_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"Markdown paragraph","path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    let text = format!(
        "## Before\r\n\r\n<div>{} **TARGET** &amp; `code`\r\nEND</div>",
        "é😀".repeat(10_000)
    );
    let at = text[..text.find("TARGET").unwrap()].encode_utf16().count();
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({"workspaceId":ws,"title":"Escaped tags","content":text}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    let source=wss_rpc(&mut rpc,3,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","at":at,"maxSourceBytes":6,"maxWireBytes":8192}})).await;
    assert_eq!(source["text"], "TARGET");
    let frame=wss_rpc_raw(&mut rpc,4,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":source["contextRef"],"maxWireBytes":8192}})).await;
    assert!(frame.to_string().len() <= 8192);
    let owner = frame["result"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["construct"] == "markdownBlock")
        .unwrap();
    assert_eq!(owner["entryPath"], "markdown");
    assert!(owner.get("parentRef").is_none());
    let maps=wss_rpc(&mut rpc,5,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":owner["sourceMapRef"],"maxWireBytes":8192}})).await;
    let map = maps["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["textRef"].is_string())
        .unwrap();
    assert_eq!(map["sourceRange"], source["range"]);
    let leaf=wss_rpc(&mut rpc,6,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":map["textNodeRef"]}})).await;
    assert!(leaf["items"][0]["marksRef"].is_string());
    assert_eq!(leaf["items"][0]["parentRef"], owner["nativeRef"]);
    let value=wss_rpc(&mut rpc,7,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":map["textRef"]}})).await;
    assert_eq!(value["items"][0]["text"], "TARGET");
    let direct=wss_rpc(&mut rpc,8,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":map["ownerRef"]}})).await;
    assert_eq!(direct["items"][0]["id"], owner["id"]);
    assert!(direct["items"][0].get("sourceMapRef").is_none());
    let newline = text[..text.find("\r\nEND").unwrap()].encode_utf16().count();
    let line=wss_rpc(&mut rpc,9,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","at":newline,"maxSourceBytes":4,"snapshotId":source["snapshotId"],"sourceRevision":source["sourceRevision"],"noteInstanceId":source["scope"]["noteInstanceId"]}})).await;
    let context=wss_rpc(&mut rpc,10,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":line["contextRef"],"maxItems":128}})).await;
    let hard_break = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["nodeType"] == "hardBreak")
        .unwrap();
    assert_eq!(hard_break["nodeClass"], "atom");
    assert_eq!(
        hard_break["sourceRange"],
        json!({"start":newline,"end":newline+2})
    );
    assert_eq!(hard_break["parentRef"], owner["nativeRef"]);
    let second = context["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["construct"] == "markdownBlock")
        .unwrap();
    assert_eq!(second["id"], owner["id"]);
    assert_ne!(second["sourceMapRef"], owner["sourceMapRef"]);
    wss_rpc(
        &mut rpc,
        11,
        "note.updateMetadata",
        json!({"workspaceId":ws,"noteId":note,"title":"New revision"}),
    )
    .await;
    let stale=wss_rpc_raw(&mut rpc,12,"note.get",json!({"workspaceId":ws,"noteId":note,"page":{"kind":"context","contextRef":owner["sourceMapRef"]}})).await;
    assert_eq!(stale["error"]["data"]["code"], "note-page-stale");
}

/// Explicit editing uses the complete document and strict legacy CAS; paged
/// reads remain revision-bound and no partial mutation endpoint is advertised.
#[tokio::test]
async fn read_capability_and_complete_edit_cas_preserve_paged_source_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let hello = wss_rpc(&mut rpc, 1, "client.hello", json!({})).await;
    let capabilities = &hello["server"]["capabilities"];
    assert_eq!(capabilities["notePagingRead"], 1);
    assert_eq!(capabilities["noteAnnotations"], 1);
    assert!(capabilities.get("notePaging").is_none());
    let workspace = wss_rpc(
        &mut rpc,
        2,
        "workspace.create",
        json!({"title":"full edit", "path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    let source = "😀 untouched \"quoted\" \\ line\r\n".repeat(1000);
    let created = wss_rpc(
        &mut rpc,
        3,
        "note.create",
        json!({"workspaceId":ws,"title":"full source","content":source}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    let before = wss_rpc(
        &mut rpc,
        4,
        "note.get",
        json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","maxSourceBytes":4096}}),
    )
    .await;
    assert_eq!(
        before["scope"]["backendId"],
        capabilities["notePagingBackendId"]
    );
    let full = wss_rpc(
        &mut rpc,
        5,
        "note.get",
        json!({"workspaceId":ws,"noteId":note}),
    )
    .await;
    assert_eq!(full["note"]["content"], source);
    let rev = full["note"]["rev"].as_i64().unwrap();
    let edited = format!("prefix\n{source}\nsuffix");
    let saved = wss_rpc(
        &mut rpc,
        6,
        "note.update",
        json!({"workspaceId":ws,"noteId":note,"content":edited,"expectedVersion":rev}),
    )
    .await;
    assert_eq!(saved["note"]["content"], edited);
    assert!(saved["note"]["rev"].as_i64().unwrap() > rev);
    // Pruned history must not turn strict update into setContent's missing-base
    // last-writer-wins fallback. Remove only this fixture's loaded base snapshot.
    let removed =
        sqlx::query("DELETE FROM note_version WHERE workspace_id = ? AND note_id = ? AND rev = ?")
            .bind(ws)
            .bind(note)
            .bind(rev)
            .execute(fx.store.write_pool())
            .await
            .unwrap();
    assert_eq!(removed.rows_affected(), 1);
    let remaining: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM note_version WHERE workspace_id = ? AND note_id = ? AND rev = ?",
    )
    .bind(ws)
    .bind(note)
    .bind(rev)
    .fetch_one(fx.store.read_pool())
    .await
    .unwrap();
    assert_eq!(remaining, 0);
    let conflict = wss_rpc_raw(
        &mut rpc,
        7,
        "note.update",
        json!({"workspaceId":ws,"noteId":note,"content":"stale draft","expectedVersion":rev}),
    )
    .await;
    assert_eq!(conflict["error"]["code"], -32005);
    let stale = wss_rpc_raw(&mut rpc, 8, "note.get", json!({"workspaceId":ws,"noteId":note,"page":{"kind":"source","cursor":before["nextCursor"],"maxSourceBytes":4096}})).await;
    assert_eq!(stale["error"]["data"]["code"], "note-page-stale");
    let reopened = wss_rpc(
        &mut rpc,
        9,
        "note.get",
        json!({"workspaceId":ws,"noteId":note}),
    )
    .await;
    assert_eq!(reopened["note"], saved["note"]);
    let mut reconstructed = String::new();
    let mut page = json!({"kind":"source","maxSourceBytes":4096});
    loop {
        let response = wss_rpc(
            &mut rpc,
            10,
            "note.get",
            json!({"workspaceId":ws,"noteId":note,"page":page}),
        )
        .await;
        reconstructed.push_str(response["text"].as_str().unwrap());
        let Some(cursor) = response["nextCursor"].as_str() else {
            break;
        };
        page = json!({"kind":"source","cursor":cursor,"maxSourceBytes":4096});
    }
    assert_eq!(reconstructed, edited);
    for method in [
        "note.applySplices",
        "note.operationStatus",
        "note.operation.begin",
        "note.operation.append",
        "note.operation.seal",
        "note.operation.commit",
        "note.operation.cancel",
        "note.operation.read",
    ] {
        let response = wss_rpc_raw(
            &mut rpc,
            11,
            method,
            json!({"workspaceId":ws,"noteId":note}),
        )
        .await;
        assert_eq!(response["error"]["code"], -32601, "{method}: {response}");
    }
    rpc.close(None).await.unwrap();
    fx.ws.stop().await;
}
/// The grace contract keeps the authoritative row live and Undo cancels the
/// operation rather than recreating its source or identity.
#[intent_test_macros::daemon_test]
async fn note_delete_grace_cancel_preserves_authoritative_record_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let hello = wss_rpc(&mut rpc, 0, "client.hello", json!({})).await;
    assert_eq!(hello["server"]["capabilities"]["noteDeleteGrace"], 1);
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({"title":"grace cancellation", "path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    let source = "😀 exact source\r\n\n[link](https://example.test/)\n";
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({"workspaceId":ws,"title":"Preserve me","content":source,"tags":["undo"]}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    let before = wss_rpc(
        &mut rpc,
        3,
        "note.get",
        json!({"workspaceId":ws,"noteId":note}),
    )
    .await;
    assert_eq!(
        before["note"]["content"], source,
        "fixture stores exact Unicode/CRLF source"
    );
    let state = wss_rpc(
        &mut rpc,
        4,
        "note.deleteStatus",
        json!({"workspaceId":ws,"noteId":note}),
    )
    .await;
    assert!(state["current"]["noteInstanceId"].is_string());
    assert_eq!(state["current"]["revision"], before["note"]["rev"]);
    assert!(state["current"].get("content").is_none());
    let key = json!({"epoch":state["epoch"],"issuedTickMs":state["serverTickMs"],"nonce":"2bda6166-ea20-4a40-983c-aab99a8bca61"});
    let params = json!({"workspaceId":ws,"noteId":note,"noteInstanceId":state["current"]["noteInstanceId"],"expectedVersion":state["current"]["revision"],"sourceRevision":state["current"]["sourceRevision"],"operationKey":key,"undoDelayMs":60000});
    let mut evt = connect(fx.port, fx.cfg.clone()).await;
    wss_rpc(
        &mut evt,
        1,
        "events.subscribe",
        json!({"workspaceId":ws,"eventTypes":["note:delete-operation"]}),
    )
    .await;
    let scheduled_frame = wss_rpc_raw(&mut rpc, 5, "note.deleteSchedule", params.clone()).await;
    assert_eq!(scheduled_frame["jsonrpc"], "2.0");
    assert_eq!(scheduled_frame["id"], 5);
    assert!(scheduled_frame.get("error").is_none());
    let scheduled = scheduled_frame["result"].clone();
    let pending_event = next_delete_operation(&mut evt).await;
    assert_eq!(
        pending_event,
        json!({"workspaceId":ws,"noteId":note,"noteInstanceId":state["current"]["noteInstanceId"],"epoch":state["epoch"],"sequence":scheduled["operation"]["sequence"],"operationKey":key,"state":"PENDING","deadlineTickMs":scheduled["operation"]["deadlineTickMs"]})
    );
    assert_eq!(scheduled["operation"]["state"], "PENDING");
    let live = wss_rpc(
        &mut rpc,
        6,
        "note.get",
        json!({"workspaceId":ws,"noteId":note}),
    )
    .await;
    assert_eq!(
        live["note"], before["note"],
        "scheduling must not mutate the authoritative note"
    );
    let repeated = wss_rpc(&mut rpc, 7, "note.deleteSchedule", params).await;
    assert_eq!(
        repeated["operation"], scheduled["operation"],
        "same intent cannot extend its deadline"
    );
    let cancelled = wss_rpc(
        &mut rpc,
        8,
        "note.deleteCancel",
        json!({"workspaceId":ws,"noteId":note,"operationKey":key}),
    )
    .await;
    assert_eq!(cancelled["operation"]["state"], "CANCELLED");
    let cancelled_event = next_delete_operation(&mut evt).await;
    assert_eq!(cancelled_event["state"], "CANCELLED");
    assert_eq!(cancelled_event["operationKey"], key);
    assert!(
        cancelled_event["sequence"].as_u64().unwrap() > pending_event["sequence"].as_u64().unwrap()
    );
    assert_eq!(
        cancelled_event["deadlineTickMs"],
        pending_event["deadlineTickMs"]
    );
    let after = wss_rpc(
        &mut rpc,
        9,
        "note.get",
        json!({"workspaceId":ws,"noteId":note}),
    )
    .await;
    assert_eq!(
        after["note"], before["note"],
        "Undo preserves full record identity and metadata"
    );
    let status = wss_rpc(
        &mut rpc,
        10,
        "note.deleteStatus",
        json!({"workspaceId":ws,"noteId":note,"operationKey":key}),
    )
    .await;
    assert_eq!(status["operation"]["state"], "CANCELLED");
    assert_eq!(status["current"], state["current"]);
    assert_eq!(status["pending"], json!([]));
    evt.close(None).await.unwrap();
    rpc.close(None).await.unwrap();
    fx.ws.stop().await;
}

async fn next_delete_operation(evt: &mut TlsWs) -> Value {
    timeout(common::rpc_read_timeout(), async {
        loop {
            match evt.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let frame: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(frame["jsonrpc"], "2.0");
                    assert_eq!(frame["method"], "events.event");
                    assert_eq!(frame["params"]["event"]["type"], "note:delete-operation");
                    return frame["params"]["event"]["data"].clone();
                }
                Message::Ping(p) => evt.send(Message::Pong(p)).await.unwrap(),
                Message::Pong(_) => {}
                other => panic!("unexpected grace event frame: {other:?}"),
            }
        }
    })
    .await
    .unwrap()
}

#[intent_test_macros::daemon_test]
async fn note_delete_grace_wss_principal_replay_foreign_cancel_and_revoked_commit() {
    let fx = boot().await;
    let mut admin = connect(fx.port, fx.cfg.clone()).await;
    let workspace = wss_rpc(
        &mut admin,
        1,
        "workspace.create",
        json!({"title":"grace authority","path":"."}),
    )
    .await;
    let ws = workspace["workspace"]["id"].as_str().unwrap();
    let created = wss_rpc(
        &mut admin,
        2,
        "note.create",
        json!({"workspaceId":ws,"title":"authority source","content":"exact authority body"}),
    )
    .await;
    let note = created["note"]["id"].as_str().unwrap();
    let primary = fx.store.get_primary_principal().await.unwrap();
    let mut guest = primary.clone();
    guest.id = "grace-guest".into();
    guest.is_primary = false;
    guest.identity = None;
    guest.github_user_id = None;
    guest.login = None;
    fx.store.upsert_principal(&guest).await.unwrap();
    fx.store
        .add_workspace_member(
            &ws.into(),
            &guest.id,
            intent_core::WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    let tokens = ["ab".repeat(32), "ef".repeat(32), "12".repeat(32)];
    let hashes: Vec<_> = tokens
        .iter()
        .map(|t| {
            use std::fmt::Write as _;
            Sha256::digest(t.as_bytes()).iter().fold(
                String::with_capacity(64),
                |mut encoded, byte| {
                    write!(encoded, "{byte:02x}").unwrap();
                    encoded
                },
            )
        })
        .collect();
    for (i, hash) in hashes.iter().enumerate() {
        fx.store
            .insert_principal_credential(if i == 2 { &guest.id } else { &primary.id }, hash)
            .await
            .unwrap();
    }
    let mut owner_a = common::wss_connect_with_retry(
        fx.port,
        fx.cfg.clone(),
        &format!("wss://localhost:{}/ws?token={}", fx.port, tokens[0]),
    )
    .await;
    let mut owner_b = common::wss_connect_with_retry(
        fx.port,
        fx.cfg.clone(),
        &format!("wss://localhost:{}/ws?token={}", fx.port, tokens[1]),
    )
    .await;
    let mut other = common::wss_connect_with_retry(
        fx.port,
        fx.cfg.clone(),
        &format!("wss://localhost:{}/ws?token={}", fx.port, tokens[2]),
    )
    .await;
    let current = wss_rpc(
        &mut owner_a,
        3,
        "note.deleteStatus",
        json!({"workspaceId":ws,"noteId":note}),
    )
    .await;
    let key = json!({"epoch":current["epoch"],"issuedTickMs":current["serverTickMs"],"nonce":uuid::Uuid::new_v4().to_string()});
    let params = json!({"workspaceId":ws,"noteId":note,"noteInstanceId":current["current"]["noteInstanceId"],"expectedVersion":current["current"]["revision"],"sourceRevision":current["current"]["sourceRevision"],"operationKey":key,"undoDelayMs":60000});
    let scheduled = wss_rpc(&mut owner_a, 4, "note.deleteSchedule", params.clone()).await;
    owner_a.close(None).await.unwrap();
    let replay = wss_rpc(&mut owner_b, 5, "note.deleteSchedule", params.clone()).await;
    assert_eq!(
        replay["operation"], scheduled["operation"],
        "stable principal replay over renewed bearer"
    );
    let visible = wss_rpc(
        &mut other,
        6,
        "note.deleteStatus",
        json!({"workspaceId":ws,"noteId":note}),
    )
    .await;
    assert_eq!(visible["pending"][0]["canCancel"], false);
    for method in ["note.deleteCancel", "note.deleteStatus"] {
        let denied = wss_rpc_raw(
            &mut other,
            7,
            method,
            json!({"workspaceId":ws,"noteId":note,"operationKey":key}),
        )
        .await;
        assert_eq!(denied["error"]["code"], -32003);
    }
    wss_rpc(
        &mut owner_b,
        8,
        "note.deleteCancel",
        json!({"workspaceId":ws,"noteId":note,"operationKey":key}),
    )
    .await;
    timeout(common::rpc_read_timeout(), async {
        loop {
            let status = wss_rpc(
                &mut owner_b,
                9,
                "note.deleteStatus",
                json!({"workspaceId":ws,"noteId":note,"operationKey":key}),
            )
            .await;
            if status["operation"]["expiresTickMs"].is_number() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Revoke a captured principal bearer within a measured long grace window.
    // Observe terminal completion with a separately authorized same-principal
    // connection; never infer survival from transport disconnect alone.
    let mut owner_a = common::wss_connect_with_retry(
        fx.port,
        fx.cfg.clone(),
        &format!("wss://localhost:{}/ws?token={}", fx.port, tokens[0]),
    )
    .await;
    let mut second = params;
    second["operationKey"]["nonce"] = json!(uuid::Uuid::new_v4().to_string());
    second["undoDelayMs"] = json!(5000);
    let next = wss_rpc(&mut owner_a, 10, "note.deleteSchedule", second.clone()).await;
    let nextkey = next["operation"]["operationKey"].clone();
    fx.store
        .revoke_principal_credential(&hashes[0])
        .await
        .unwrap();
    let checkpoint = wss_rpc(
        &mut owner_b,
        11,
        "note.deleteStatus",
        json!({"workspaceId":ws,"noteId":note,"operationKey":nextkey}),
    )
    .await;
    assert!(
        checkpoint["serverTickMs"].as_u64().unwrap()
            < next["operation"]["deadlineTickMs"].as_u64().unwrap(),
        "revocation must precede deadline in this control"
    );
    timeout(common::rpc_read_timeout(), async {
        loop {
            let status = wss_rpc(
                &mut owner_b,
                12,
                "note.deleteStatus",
                json!({"workspaceId":ws,"noteId":note,"operationKey":nextkey}),
            )
            .await;
            if status["operation"]["expiresTickMs"].is_number() {
                assert_eq!(status["operation"]["state"], "CONFLICT");
                assert_eq!(status["operation"]["reason"], "authorityLost");
                break;
            }
            // timing-guard: bounded status polling stops as soon as the owned operation settles.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let preserved = wss_rpc(
        &mut owner_b,
        13,
        "note.get",
        json!({"workspaceId":ws,"noteId":note}),
    )
    .await;
    assert_eq!(preserved["note"], created["note"]);
    let _ = owner_a.close(None).await;
    owner_b.close(None).await.unwrap();
    other.close(None).await.unwrap();
    admin.close(None).await.unwrap();
    fx.ws.stop().await;
}
