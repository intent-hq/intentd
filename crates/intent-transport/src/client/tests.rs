//! Unit tests for the `client.hello` handshake fast-path (§5.17, §16).

use std::sync::Mutex;

use intent_core::{BoxFuture, ClientId, Result, WorkspaceApi};
use serde_json::{json, Value};

use super::*;

/// One recorded `upsert_client` call: `(clientId, name, capabilities)`.
type UpsertCall = (String, Option<String>, Option<Value>);

/// Records the last `upsert_client` call so tests can assert persistence wiring.
#[derive(Default)]
struct RecordingApi {
    last: Mutex<Option<UpsertCall>>,
}

impl WorkspaceApi for RecordingApi {
    fn upsert_client(
        &self,
        client_id: ClientId,
        name: Option<String>,
        capabilities: Option<Value>,
    ) -> BoxFuture<'_, Result<()>> {
        *self.last.lock().unwrap() = Some((client_id.0, name, capabilities));
        Box::pin(async { Ok(()) })
    }
}

fn parsed(outcome: HelloOutcome) -> Value {
    serde_json::from_str(&outcome.frame.expect("a response frame")).unwrap()
}

#[tokio::test]
async fn mints_client_id_when_omitted() {
    let api = RecordingApi::default();
    let mut binding: Option<ClientId> = None;
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "client.hello", "params": { "name": "Intent" }
    }))
    .unwrap();
    let outcome = handle(req, &api, &mut binding, true).await;
    let bound = outcome
        .bound
        .clone()
        .expect("a successful hello binds an identity");
    let resp = parsed(outcome);
    let cid = resp["result"]["clientId"].as_str().unwrap();
    assert!(!cid.is_empty(), "server mints a clientId when omitted");
    assert_eq!(
        binding.as_ref().unwrap().0,
        cid,
        "binding is set to the minted id"
    );
    assert_eq!(bound.client_id.as_str(), cid);
    assert_eq!(bound.name.as_deref(), Some("Intent"));
    assert_eq!(
        bound.capabilities,
        json!({}),
        "omitted capabilities normalize to an empty object"
    );
    assert!(!bound.browser_exec());
    assert_eq!(
        resp["result"]["protocolVersion"],
        json!(crate::protocol::PROTOCOL_VERSION),
        "explicit top-level protocolVersion in the hello result"
    );
    let server = &resp["result"]["server"];
    assert_eq!(server["locality"], json!("local"));
    assert_eq!(server["version"], json!(env!("CARGO_PKG_VERSION")));
    match crate::BUILD_COMMIT {
        Some(build_commit) => assert_eq!(server["buildCommit"], json!(build_commit)),
        None => assert!(server.get("buildCommit").is_none()),
    }
    assert_eq!(
        server["protocolVersion"],
        json!(crate::protocol::PROTOCOL_VERSION)
    );
    assert!(
        server["osArch"].as_str().unwrap().contains('/'),
        "osArch is os/arch"
    );
    assert!(server.get("hasDisplay").is_some());
    assert_eq!(
        server["capabilities"]["liveState"],
        json!(true),
        "server advertises the liveState capability (§5.17)"
    );
}

#[test]
fn server_identity_omits_an_unknown_build_commit() {
    let server = server_json(true, "linux", "x86_64", "1.2.3", None, true);
    assert_eq!(server["version"], "1.2.3");
    assert!(server.get("buildCommit").is_none());
}

#[tokio::test]
async fn re_presents_persisted_id_and_is_idempotent() {
    let api = RecordingApi::default();
    let mut binding: Option<ClientId> = None;
    let req = |id: i64| {
        classify(&json!({
            "jsonrpc": "2.0", "id": id, "method": "client.hello",
            "params": { "clientId": "cli-7f3a", "name": "A", "capabilities": { "forward": true } }
        }))
        .unwrap()
    };
    let r1 = parsed(handle(req(1), &api, &mut binding, false).await);
    assert_eq!(r1["result"]["clientId"], json!("cli-7f3a"));
    assert_eq!(binding.as_ref().unwrap().0, "cli-7f3a");
    assert_eq!(
        r1["result"]["protocolVersion"],
        json!(crate::protocol::PROTOCOL_VERSION)
    );
    assert_eq!(r1["result"]["server"]["locality"], json!("remote"));
    // Re-sending updates name/capabilities and re-returns the same server block.
    let r2 = parsed(handle(req(2), &api, &mut binding, false).await);
    assert_eq!(r2["result"]["clientId"], json!("cli-7f3a"));
    assert_eq!(r1["result"]["server"], r2["result"]["server"]);
    let last = api.last.lock().unwrap().clone().unwrap();
    assert_eq!(last.0, "cli-7f3a");
    assert_eq!(last.2, Some(json!({ "forward": true })));
}

#[tokio::test]
async fn non_string_client_id_is_invalid_params() {
    let api = RecordingApi::default();
    let mut binding: Option<ClientId> = None;
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "client.hello", "params": { "clientId": 42 }
    }))
    .unwrap();
    let outcome = handle(req, &api, &mut binding, true).await;
    assert!(
        outcome.bound.is_none(),
        "an invalid hello binds no reverse identity"
    );
    let resp = parsed(outcome);
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert_eq!(resp["error"]["data"]["code"], "invalid-params");
    assert!(
        binding.is_none(),
        "an invalid hello leaves the binding unset"
    );
}

#[tokio::test]
async fn notification_has_no_response_but_sets_binding() {
    let api = RecordingApi::default();
    let mut binding: Option<ClientId> = None;
    let req = classify(&json!({
        "jsonrpc": "2.0", "method": "client.hello",
        "params": { "clientId": "cli-9b21", "capabilities": { "browserExec": true } }
    }))
    .unwrap();
    let outcome = handle(req, &api, &mut binding, true).await;
    assert!(outcome.frame.is_none());
    assert_eq!(binding.as_ref().unwrap().0, "cli-9b21");
    let bound = outcome.bound.expect("notification hello still binds");
    assert_eq!(bound.client_id.as_str(), "cli-9b21");
    assert!(bound.browser_exec());
}

#[test]
fn classify_ignores_other_methods_and_bad_envelope() {
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "host.status" })).is_none());
    assert!(classify(&json!({ "jsonrpc": "1.0", "id": 1, "method": "client.hello" })).is_none());
}
