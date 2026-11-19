//! Protocol contract: request parsing, error codes, the RP ID re-check, the
//! one-ceremony rule, cancel / timeout, and PIN / account routing, against a
//! scripted authenticator.

mod support;

use serde_json::{json, Value};

use intent_passkey::authenticator::{Authenticator, CeremonyIo};
use intent_passkey::protocol::{
    Account, BluetoothState, Bytes, CreateExtensionOutputs, CreateParams, CreateResult, GetParams,
    GetResult, PasskeyError, PinKind, Status,
};
use support::{create_params, error_code, get_params, Client};

struct Scripted;

fn created(credential_id: Vec<u8>) -> CreateResult {
    CreateResult {
        credential_id: Bytes(credential_id),
        client_data_json: Bytes(b"{}".to_vec()),
        attestation_object: Bytes(vec![1]),
        authenticator_data: Bytes(vec![2]),
        public_key: None,
        public_key_algorithm: -7,
        transports: vec!["usb".into()],
        authenticator_attachment: "cross-platform".into(),
        extensions: CreateExtensionOutputs::default(),
    }
}

impl Authenticator for Scripted {
    async fn bluetooth(&self) -> BluetoothState {
        BluetoothState::Off
    }

    fn usb_supported(&self) -> bool {
        true
    }

    async fn create(
        &self,
        params: CreateParams,
        mut io: CeremonyIo,
    ) -> Result<CreateResult, PasskeyError> {
        io.status(Status::TouchKey);
        match params.user.name.as_str() {
            "wait" => Err(io.stopped().await.error()),
            "pin" => {
                let first = io.request_pin(PinKind::Usb, Some(8), false).await?;
                assert_eq!(first.as_str(), "0000");
                let pin = io.request_pin(PinKind::Usb, Some(7), true).await?;
                Ok(created(pin.as_bytes().to_vec()))
            }
            "choose" => {
                let accounts = (0..2)
                    .map(|index| Account {
                        index,
                        user_name: format!("user{index}"),
                        user_display_name: format!("User {index}"),
                    })
                    .collect();
                let index = io.choose_account(accounts).await?;
                Ok(created(vec![u8::try_from(index).unwrap()]))
            }
            _ => Ok(created(vec![7])),
        }
    }

    async fn get(&self, _params: GetParams, _io: CeremonyIo) -> Result<GetResult, PasskeyError> {
        Ok(GetResult {
            credential_id: Bytes(vec![7]),
            client_data_json: Bytes(b"{}".to_vec()),
            authenticator_data: Bytes(vec![2]),
            signature: Bytes(vec![3]),
            user_handle: None,
            authenticator_attachment: "cross-platform".into(),
        })
    }
}

fn with_user(mut params: Value, name: &str) -> Value {
    params["user"]["name"] = json!(name);
    params
}

#[tokio::test]
async fn hello_reports_protocol_and_transports() {
    let mut client = Client::start(Scripted);
    let hello = client.call("1", "hello", json!({})).await;
    assert_eq!(hello["result"]["protocol"], 1);
    assert_eq!(
        hello["result"]["transports"],
        json!({ "hybrid": true, "usb": true, "device": false })
    );
    assert_eq!(hello["result"]["bluetooth"], "off");
    assert!(hello["result"]["version"].is_string());
}

#[tokio::test]
async fn malformed_requests_get_error_codes() {
    let mut client = Client::start(Scripted);
    client.send_raw("not json").await;
    let line = client.next().await;
    assert_eq!(line["id"], Value::Null);
    assert_eq!(error_code(&line), "Unknown");

    let unknown = client.call("2", "frobnicate", json!({})).await;
    assert_eq!(error_code(&unknown), "NotSupported");

    let missing = client
        .call("3", "create", json!({ "transport": "usb" }))
        .await;
    assert_eq!(error_code(&missing), "Unknown");

    let bad_base64 = client
        .call("4", "get", json!({ "transport": "usb", "origin": "https://example.com", "rpId": "example.com", "challenge": "***" }))
        .await;
    assert_eq!(error_code(&bad_base64), "Unknown");

    for (id, method, params) in [
        ("5", "device.list", json!({})),
        ("6", "device.delete", json!({ "credentialId": "AAAA" })),
    ] {
        assert_eq!(
            error_code(&client.call(id, method, params).await),
            "NotSupported"
        );
    }
    let device = client
        .call(
            "7",
            "create",
            create_params("device", "https://example.com", "example.com"),
        )
        .await;
    assert_eq!(error_code(&device), "NotSupported");

    let extra = client
        .call("8", "get", {
            let mut params = get_params("usb", "https://example.com", "example.com");
            params["somethingNew"] = json!(true);
            params
        })
        .await;
    assert_eq!(extra["result"]["credentialId"], "Bw");
}

#[tokio::test]
async fn rp_id_must_match_the_origin() {
    let mut client = Client::start(Scripted);
    let cases = [
        ("https://login.example.com", "example.com", None),
        ("https://example.com", "example.com", None),
        ("http://localhost:8080", "localhost", None),
        ("https://example.com", "evil.com", Some("Security")),
        ("https://example.com", "ample.com", Some("Security")),
        ("https://a.example.co.uk", "co.uk", Some("Security")),
        ("https://example.com", "com", Some("Security")),
        ("https://user.github.io", "github.io", Some("Security")),
        ("https://example.com", "Example.com", Some("Security")),
        ("http://example.com", "example.com", Some("Security")),
        ("https://192.168.1.10", "192.168.1.10", Some("Security")),
        ("https://example.com/path", "example.com", Some("Security")),
        ("file:///etc", "etc", Some("Security")),
    ];
    for (index, (origin, rp_id, expected)) in cases.into_iter().enumerate() {
        let id = format!("rp{index}");
        let response = client
            .call(&id, "create", create_params("usb", origin, rp_id))
            .await;
        match expected {
            None => assert!(
                response["result"].is_object(),
                "{origin} {rp_id}: {response}"
            ),
            Some(code) => assert_eq!(error_code(&response), code, "{origin} {rp_id}"),
        }
        let response = client
            .call(&format!("{id}g"), "get", get_params("usb", origin, rp_id))
            .await;
        match expected {
            None => assert!(
                response["result"].is_object(),
                "{origin} {rp_id}: {response}"
            ),
            Some(code) => assert_eq!(error_code(&response), code, "{origin} {rp_id}"),
        }
    }

    let mut mismatched = create_params("usb", "https://example.com", "example.com");
    mismatched["rp"]["id"] = json!("other.com");
    assert_eq!(
        error_code(&client.call("m", "create", mismatched).await),
        "Security"
    );

    let mut framed = get_params("usb", "https://example.com", "example.com");
    framed["topOrigin"] = json!("https://top.example.net");
    assert_eq!(
        error_code(&client.call("f", "get", framed).await),
        "NotAllowed"
    );

    let mut cross = create_params("usb", "https://example.com", "example.com");
    cross["crossOrigin"] = json!(true);
    assert_eq!(
        error_code(&client.call("x", "create", cross).await),
        "NotAllowed"
    );
}

#[tokio::test]
async fn one_ceremony_at_a_time_and_cancel() {
    let mut client = Client::start(Scripted);
    let waiting = with_user(
        create_params("usb", "https://example.com", "example.com"),
        "wait",
    );
    client.send("a", "create", waiting.clone()).await;
    let status = client.event("status").await;
    assert_eq!(
        status,
        json!({ "event": "status", "state": "touch-key", "ceremony": "a" })
    );

    let second = client
        .call(
            "b",
            "get",
            get_params("usb", "https://example.com", "example.com"),
        )
        .await;
    assert_eq!(error_code(&second), "NotAllowed");

    client.send("c", "cancel", json!({ "ceremony": "a" })).await;
    let mut seen = Vec::new();
    while seen.len() < 2 {
        seen.push(client.next().await);
    }
    let cancel = seen.iter().find(|line| line["id"] == "c").unwrap();
    assert_eq!(cancel["result"], json!({}));
    let first = seen.iter().find(|line| line["id"] == "a").unwrap();
    assert_eq!(error_code(first), "Abort");

    let after = client
        .call(
            "d",
            "get",
            get_params("usb", "https://example.com", "example.com"),
        )
        .await;
    assert!(after["result"].is_object());
    let stale = client.call("e", "cancel", json!({ "ceremony": "a" })).await;
    assert_eq!(stale["result"], json!({}));
}

#[tokio::test(start_paused = true)]
async fn ceremonies_time_out() {
    let mut client = Client::start(Scripted);
    let mut waiting = with_user(
        create_params("usb", "https://example.com", "example.com"),
        "wait",
    );
    waiting["timeoutMs"] = json!(6000);
    let (response, _) = {
        client.send("t", "create", waiting).await;
        client.response("t").await
    };
    assert_eq!(error_code(&response), "Timeout");
}

#[tokio::test]
async fn pin_and_choice_reach_the_running_ceremony() {
    let mut client = Client::start(Scripted);
    let wrong_ceremony = client
        .call("p0", "pin", json!({ "ceremony": "nope", "pin": "1" }))
        .await;
    assert_eq!(error_code(&wrong_ceremony), "NotAllowed");

    client
        .send(
            "p",
            "create",
            with_user(
                create_params("usb", "https://example.com", "example.com"),
                "pin",
            ),
        )
        .await;
    let needed = client.event("pin-needed").await;
    assert_eq!(
        needed,
        json!({ "event": "pin-needed", "ceremony": "p", "kind": "usb", "attemptsLeft": 8, "wrongPin": false })
    );
    assert_eq!(
        client
            .call("p1", "pin", json!({ "ceremony": "p", "pin": "0000" }))
            .await["result"],
        json!({})
    );
    let again = client.event("pin-needed").await;
    assert_eq!(again["wrongPin"], true);
    assert_eq!(again["attemptsLeft"], 7);
    client
        .send("p2", "pin", json!({ "ceremony": "p", "pin": "123456" }))
        .await;
    let (result, _) = client.response("p").await;
    assert_eq!(
        result["result"]["credentialId"],
        Bytes(b"123456".to_vec()).encode()
    );

    client
        .send(
            "c",
            "create",
            with_user(
                create_params("usb", "https://example.com", "example.com"),
                "choose",
            ),
        )
        .await;
    let choose = client.event("choose-account").await;
    assert_eq!(choose["ceremony"], "c");
    assert_eq!(
        choose["accounts"][1],
        json!({ "index": 1, "userName": "user1", "userDisplayName": "User 1" })
    );
    client
        .send("c0", "choose", json!({ "ceremony": "c", "index": 5 }))
        .await;
    client
        .send("c1", "choose", json!({ "ceremony": "c", "index": 1 }))
        .await;
    let mut lines = Vec::new();
    while !lines.iter().any(|line: &Value| line["id"] == "c") {
        lines.push(client.next().await);
    }
    let result = lines.iter().find(|line| line["id"] == "c").unwrap();
    assert_eq!(result["result"]["credentialId"], "AQ");
}
