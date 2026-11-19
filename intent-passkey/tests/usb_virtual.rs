//! USB security key round trip through the real libwebauthn backend and the stdio
//! protocol, with a software CTAP2 key on libwebauthn's virtual HID pipe: PIN prompts
//! (including a wrong PIN), excluded credentials, signature checks, the account
//! chooser and PIN lockout.
#![cfg(target_os = "linux")]

mod support;

use p256::ecdsa::signature::Verifier;
use p256::ecdsa::{Signature, VerifyingKey};
use p256::pkcs8::DecodePublicKey;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use intent_passkey::protocol::Bytes;
use intent_passkey::webauthn::LibWebAuthn;
use support::soft_key::SoftKey;
use support::{create_params, error_code, get_params, Client};

const ORIGIN: &str = "https://login.example.com";
const RP_ID: &str = "example.com";

fn decode(value: &Value) -> Vec<u8> {
    Bytes::decode(value.as_str().unwrap()).unwrap().0
}

/// Answers every `pin-needed` with the next PIN, collecting events, until `id` answers.
async fn run_with_pins(client: &mut Client, id: &str, pins: &[&str]) -> (Value, Vec<Value>) {
    let mut pins = pins.iter();
    let mut events = Vec::new();
    loop {
        let line = client.next().await;
        if line["event"] == "pin-needed" {
            let pin = pins.next().expect("no more PINs to answer with");
            client
                .send(
                    &format!("{id}-pin"),
                    "pin",
                    json!({ "ceremony": id, "pin": pin }),
                )
                .await;
            events.push(line);
        } else if line.get("event").is_some() {
            events.push(line);
        } else if line["id"] == id {
            return (line, events);
        }
    }
}

fn client_data(result: &Value) -> Value {
    serde_json::from_slice(&decode(&result["clientDataJSON"])).unwrap()
}

#[tokio::test]
async fn usb_create_and_get_round_trip() {
    let key = SoftKey::new("4321", 8);
    let mut client = Client::start(LibWebAuthn::with_usb_devices(vec![key.device()]));

    client
        .send("c1", "create", create_params("usb", ORIGIN, RP_ID))
        .await;
    let (created, events) = run_with_pins(&mut client, "c1", &["1111", "4321"]).await;
    let result = &created["result"];
    assert!(result.is_object(), "create failed: {created}");
    let pin_events: Vec<&Value> = events
        .iter()
        .filter(|e| e["event"] == "pin-needed")
        .collect();
    assert_eq!(pin_events.len(), 2, "{events:?}");
    assert_eq!(pin_events[0]["kind"], "usb");
    assert_eq!(pin_events[0]["wrongPin"], false);
    assert_eq!(pin_events[1]["wrongPin"], true);
    assert_eq!(pin_events[1]["attemptsLeft"], 7);
    assert!(events
        .iter()
        .any(|e| e["event"] == "status" && e["state"] == "touch-key"));
    assert!(events.iter().all(|e| e["ceremony"] == "c1"));

    // WebAuthn L3 §5.8.1.2 key order; main uses these bytes verbatim.
    assert_eq!(
        String::from_utf8(decode(&result["clientDataJSON"])).unwrap(),
        format!(
            r#"{{"type":"webauthn.create","challenge":{},"origin":"{ORIGIN}","crossOrigin":false}}"#,
            create_params("usb", ORIGIN, RP_ID)["challenge"]
        )
    );
    let data = client_data(result);
    assert_eq!(data["type"], "webauthn.create");
    assert_eq!(data["origin"], ORIGIN);
    assert_eq!(
        data["challenge"],
        create_params("usb", ORIGIN, RP_ID)["challenge"]
    );
    assert_eq!(data["crossOrigin"], false);
    let auth_data = decode(&result["authenticatorData"]);
    assert_eq!(&auth_data[..32], Sha256::digest(RP_ID).as_slice());
    assert_eq!(auth_data[32] & 0x05, 0x05, "UP and UV set");
    assert_eq!(result["publicKeyAlgorithm"], -7);
    assert_eq!(result["authenticatorAttachment"], "cross-platform");
    assert_eq!(result["transports"], json!(["usb"]));
    assert!(!decode(&result["attestationObject"]).is_empty());
    let credential_id = result["credentialId"].clone();
    let public_key = VerifyingKey::from(
        p256::PublicKey::from_public_key_der(&decode(&result["publicKey"])).unwrap(),
    );

    let mut excluded = create_params("usb", ORIGIN, RP_ID);
    excluded["excludeCredentials"] = json!([{ "id": credential_id, "transports": ["usb"] }]);
    client.send("c2", "create", excluded).await;
    let (refused, _) = run_with_pins(&mut client, "c2", &["4321"]).await;
    assert_eq!(error_code(&refused), "InvalidState");
    assert_eq!(key.credential_count(), 1);

    let mut get = get_params("usb", ORIGIN, RP_ID);
    get["allowCredentials"] = json!([{ "id": credential_id }]);
    client.send("g1", "get", get).await;
    let (asserted, _) = run_with_pins(&mut client, "g1", &["4321"]).await;
    let assertion = &asserted["result"];
    assert_eq!(assertion["credentialId"], credential_id, "{asserted}");
    assert_eq!(
        String::from_utf8(decode(&assertion["clientDataJSON"])).unwrap(),
        format!(
            r#"{{"type":"webauthn.get","challenge":{},"origin":"{ORIGIN}","crossOrigin":false}}"#,
            get_params("usb", ORIGIN, RP_ID)["challenge"]
        )
    );
    let mut signed = decode(&assertion["authenticatorData"]);
    signed.extend(Sha256::digest(decode(&assertion["clientDataJSON"])));
    let signature = Signature::from_der(&decode(&assertion["signature"])).unwrap();
    public_key
        .verify(&signed, &signature)
        .expect("assertion signature verifies");

    let mut second_user = create_params("usb", ORIGIN, RP_ID);
    second_user["user"] =
        json!({ "id": "dXNlci0y", "name": "grace@example.com", "displayName": "Grace" });
    client.send("c3", "create", second_user).await;
    let (created_second, _) = run_with_pins(&mut client, "c3", &["4321"]).await;
    assert!(created_second["result"].is_object(), "{created_second}");

    client
        .send("g2", "get", get_params("usb", ORIGIN, RP_ID))
        .await;
    let mut pins = 0;
    let choose = loop {
        let line = client.next().await;
        if line["event"] == "pin-needed" {
            pins += 1;
            client
                .send("g2-pin", "pin", json!({ "ceremony": "g2", "pin": "4321" }))
                .await;
        } else if line["event"] == "choose-account" {
            break line;
        } else {
            assert!(
                line.get("event").is_some() || line["id"] == "g2-pin",
                "unexpected {line}"
            );
        }
    };
    assert_eq!(pins, 1);
    let names: Vec<&str> = choose["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|account| account["userName"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["ada@example.com", "grace@example.com"]);
    client
        .send(
            "g2-choose",
            "choose",
            json!({ "ceremony": "g2", "index": 1 }),
        )
        .await;
    let (chosen, _) = run_with_pins(&mut client, "g2", &[]).await;
    assert_eq!(chosen["result"]["userHandle"], "dXNlci0y", "{chosen}");
}

#[tokio::test]
async fn usb_wrong_pins_lock_the_key() {
    let key = SoftKey::new("4321", 2);
    let mut client = Client::start(LibWebAuthn::with_usb_devices(vec![key.device()]));
    client
        .send("c", "create", create_params("usb", ORIGIN, RP_ID))
        .await;
    let (locked, events) = run_with_pins(&mut client, "c", &["0000", "0000"]).await;
    assert_eq!(error_code(&locked), "PinLocked", "{events:?}");
    assert_eq!(key.credential_count(), 0);
}

#[tokio::test]
async fn usb_cancel_while_waiting_for_the_pin() {
    let key = SoftKey::new("4321", 8);
    let mut client = Client::start(LibWebAuthn::with_usb_devices(vec![key.device()]));
    client
        .send("c", "create", create_params("usb", ORIGIN, RP_ID))
        .await;
    client.event("pin-needed").await;
    client.send("x", "cancel", json!({ "ceremony": "c" })).await;
    let mut cancelled = None;
    while cancelled.is_none() {
        let line = client.next().await;
        if line["id"] == "c" {
            cancelled = Some(line);
        }
    }
    assert_eq!(error_code(&cancelled.unwrap()), "Abort");
    let next = client.call("h", "hello", json!({})).await;
    assert_eq!(next["result"]["transports"]["usb"], true);
}

#[tokio::test]
async fn usb_uses_the_key_the_user_touches() {
    let idle = SoftKey::new("1111", 8).untouched();
    let chosen = SoftKey::new("4321", 8);
    let mut client = Client::start(LibWebAuthn::with_usb_devices(vec![
        idle.device(),
        chosen.device(),
    ]));

    client
        .send("c", "create", create_params("usb", ORIGIN, RP_ID))
        .await;
    let (created, _) = run_with_pins(&mut client, "c", &["4321"]).await;
    assert!(created["result"].is_object(), "{created}");
    assert_eq!((idle.credential_count(), chosen.credential_count()), (0, 1));

    let mut get = get_params("usb", ORIGIN, RP_ID);
    get["allowCredentials"] = json!([{ "id": created["result"]["credentialId"] }]);
    client.send("g", "get", get).await;
    let (asserted, _) = run_with_pins(&mut client, "g", &["4321"]).await;
    assert_eq!(
        asserted["result"]["credentialId"], created["result"]["credentialId"],
        "{asserted}"
    );
}
