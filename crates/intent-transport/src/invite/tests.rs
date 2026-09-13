//! Unit tests for the invite fast paths: link construction, classify, the
//! `invite.redeem` phase split, and the invite-error `data.code` mapping.

use std::sync::Arc;

use intent_core::{InviteErrorKind, WorkspaceApi};
use serde_json::{json, Value};

use super::*;

#[test]
fn invite_uri_carries_envelope_minus_token_plus_invite_fields() {
    let uri = build_invite_uri(
        &["192.168.1.10".to_string(), "10.0.0.5".to_string()],
        7443,
        "AB:CD",
        "inv-1",
        "s3cret",
        None,
    );
    assert_eq!(
        uri,
        "intent://invite?v=1&host=192.168.1.10,10.0.0.5&port=7443&fp=AB:CD&inviteId=inv-1&secret=s3cret"
    );
    assert!(!uri.contains("token="));
    let with_tc = build_invite_uri(&[], 7443, "AB", "i", "s", Some("tc-abc"));
    assert!(with_tc.ends_with("&tc=tc-abc"), "{with_tc}");
    let encoded = build_invite_uri(&[], 1, "AB", "a&b", "x=y", None);
    assert!(encoded.contains("inviteId=a%26b&secret=x%3Dy"), "{encoded}");
}

#[test]
fn classify_picks_the_two_invite_methods_only() {
    let create = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "workspace.invite.create",
        "params": { "workspaceId": "ws" }
    }))
    .expect("classified");
    assert_eq!(create.method, InviteMethod::Create);
    assert!(create.id_present);
    let redeem =
        classify(&json!({ "jsonrpc": "2.0", "method": "invite.redeem" })).expect("classified");
    assert_eq!(redeem.method, InviteMethod::Redeem);
    assert!(!redeem.id_present);
    assert!(
        classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": "workspace.invite.list" }))
            .is_none()
    );
    assert!(classify(&json!({ "jsonrpc": "1.0", "id": 1, "method": "invite.redeem" })).is_none());
    assert!(classify(&json!({ "jsonrpc": "2.0", "id": {}, "method": "invite.redeem" })).is_none());
}

/// Records which redeem phase ran and returns a canned invite error.
struct RedeemStub {
    calls: std::sync::Mutex<Vec<String>>,
}

impl WorkspaceApi for RedeemStub {
    fn invite_redeem_start(
        &self,
        invite_id: String,
        secret: String,
    ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("start:{invite_id}:{secret}"));
        Box::pin(async { Err(intent_core::Error::Invite(InviteErrorKind::Expired)) })
    }
    fn invite_redeem_wait(
        &self,
        flow_id: String,
    ) -> intent_core::BoxFuture<'_, intent_core::Result<Value>> {
        self.calls.lock().unwrap().push(format!("wait:{flow_id}"));
        Box::pin(async { Ok(json!({ "status": "authorized", "token": "t" })) })
    }
}

#[tokio::test]
async fn redeem_routes_phases_and_maps_invite_errors_to_data_code() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 7, "method": "invite.redeem",
        "params": { "inviteId": "inv", "secret": "sec" }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_redeem(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["id"], json!(7));
    assert_eq!(frame["error"]["data"]["code"], json!("invite-expired"));
    assert_eq!(
        frame["error"]["code"],
        json!(intent_core::Error::Invite(InviteErrorKind::Expired).code())
    );

    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 8, "method": "invite.redeem",
        "params": { "flowId": " flow-1 " }
    }))
    .unwrap();
    let frame: Value = serde_json::from_str(&handle_redeem(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["result"]["status"], json!("authorized"));

    let req =
        classify(&json!({ "jsonrpc": "2.0", "id": 9, "method": "invite.redeem", "params": {} }))
            .unwrap();
    let frame: Value = serde_json::from_str(&handle_redeem(req, &api).await.unwrap()).unwrap();
    assert_eq!(frame["error"]["code"], json!(-32602));

    let req =
        classify(&json!({ "jsonrpc": "2.0", "method": "invite.redeem", "params": {} })).unwrap();
    assert!(
        handle_redeem(req, &api).await.is_none(),
        "notification: no frame"
    );

    assert_eq!(
        *stub.calls.lock().unwrap(),
        vec!["start:inv:sec".to_string(), "wait:flow-1".to_string()]
    );
}

#[tokio::test]
async fn create_without_a_pairing_provider_is_unsupported_and_mints_nothing() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "workspace.invite.create",
        "params": { "workspaceId": "ws" }
    }))
    .unwrap();
    let frame: Value =
        serde_json::from_str(&handle_create(req, &api, None).await.unwrap()).unwrap();
    assert_eq!(
        frame["error"]["code"],
        json!(intent_core::Error::Unsupported(String::new()).code())
    );
    assert!(stub.calls.lock().unwrap().is_empty());
}

#[test]
fn non_invite_method_on_invite_endpoint_is_unauthorized() {
    let frame =
        refuse_non_invite(&json!({ "jsonrpc": "2.0", "id": 3, "method": "workspace.list" }))
            .expect("frame");
    let v: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(v["error"]["code"], json!(-32001));
    assert_eq!(v["error"]["message"], json!(INVITE_ENDPOINT_ONLY_MESSAGE));
    assert!(refuse_non_invite(&json!({ "jsonrpc": "2.0", "method": "workspace.list" })).is_none());
}

fn start_req(id: i64) -> InviteRequest {
    classify(&json!({
        "jsonrpc": "2.0", "id": id, "method": "invite.redeem",
        "params": { "inviteId": "inv", "secret": format!("guess-{id}") }
    }))
    .unwrap()
}

/// The start throttle is a token bucket: the burst is admitted, the next
/// serial attempt is refused with `invite-flow-busy` until a whole refill
/// interval has elapsed, one token comes back per interval, and the bucket
/// never overfills past the burst.
#[test]
fn redeem_start_throttle_refuses_after_the_burst_until_refill() {
    let t0 = Instant::now();
    let throttle = Arc::new(Mutex::new(RedeemThrottle::new(t0)));
    for i in 0..INVITE_START_BURST {
        assert!(
            admit_redeem(&start_req(i64::from(i)), &throttle, t0).is_ok(),
            "burst attempt {i} admitted"
        );
    }
    let refused = admit_redeem(&start_req(100), &throttle, t0).expect_err("burst exhausted");
    let v: Value = serde_json::from_str(&refused.expect("frame")).unwrap();
    assert_eq!(v["id"], json!(100));
    assert_eq!(v["error"]["data"]["code"], json!("invite-flow-busy"));
    let just_short = t0 + INVITE_START_REFILL.saturating_sub(Duration::from_millis(1));
    assert!(
        admit_redeem(&start_req(101), &throttle, just_short).is_err(),
        "still refused just before the refill interval"
    );
    let refilled = t0 + INVITE_START_REFILL;
    assert!(
        admit_redeem(&start_req(102), &throttle, refilled).is_ok(),
        "one token back after one interval"
    );
    assert!(
        admit_redeem(&start_req(103), &throttle, refilled).is_err(),
        "exactly one token per interval"
    );
    let much_later = t0 + INVITE_START_REFILL * (INVITE_START_BURST * 4);
    for i in 0..INVITE_START_BURST {
        assert!(
            admit_redeem(&start_req(200 + i64::from(i)), &throttle, much_later).is_ok(),
            "refilled burst attempt {i}"
        );
    }
    assert!(
        admit_redeem(&start_req(300), &throttle, much_later).is_err(),
        "never overfills past the burst"
    );
    // A throttled notification is dropped without a frame.
    let note =
        classify(&json!({ "jsonrpc": "2.0", "method": "invite.redeem", "params": { "inviteId": "inv", "secret": "s" } }))
            .unwrap();
    assert_eq!(admit_redeem(&note, &throttle, much_later), Err(None));
}

/// Phase-2 waits are never throttled (the start already paid), and a
/// throttled start reaches neither the store nor the upstream flow: the
/// refusal is produced before `handle_redeem` runs.
#[tokio::test]
async fn throttled_starts_make_no_upstream_calls_and_waits_pass() {
    let stub = Arc::new(RedeemStub {
        calls: std::sync::Mutex::new(Vec::new()),
    });
    let api: Arc<dyn WorkspaceApi> = stub.clone();
    let t0 = Instant::now();
    let throttle = Arc::new(Mutex::new(RedeemThrottle::new(t0)));
    for i in 0..INVITE_START_BURST {
        assert!(admit_redeem(&start_req(i64::from(i)), &throttle, t0).is_ok());
    }
    let mut refusals = 0;
    for i in 0..20 {
        match admit_redeem(&start_req(500 + i), &throttle, t0) {
            Ok(()) => {
                handle_redeem(start_req(500 + i), &api).await;
            }
            Err(_) => refusals += 1,
        }
    }
    assert_eq!(refusals, 20, "every post-burst start refused");
    assert!(
        stub.calls.lock().unwrap().is_empty(),
        "no store/upstream call while throttled: {:?}",
        stub.calls.lock().unwrap()
    );
    let wait = classify(&json!({
        "jsonrpc": "2.0", "id": 9, "method": "invite.redeem", "params": { "flowId": "flow-1" }
    }))
    .unwrap();
    assert_eq!(admit_redeem(&wait, &throttle, t0), Ok(()));
    let frame: Value = serde_json::from_str(&handle_redeem(wait, &api).await.unwrap()).unwrap();
    assert_eq!(frame["result"]["status"], json!("authorized"));
    assert_eq!(*stub.calls.lock().unwrap(), vec!["wait:flow-1".to_string()]);
}
