//! Device passkeys end to end through the stdio protocol: create → get with a
//! signature that verifies under the returned public key, the PIN attempt limit,
//! the PIN-change re-wrap and a damaged store. PAM, the lock screen and the clock
//! are test doubles; the store, crypto and limiter files are real.
#![cfg(target_os = "linux")]

mod support;

use std::collections::VecDeque;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ciborium::value::Value as Cbor;
use p256::pkcs8::DecodePublicKey as _;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

use intent_passkey::device::{Device, DeviceConfig, KdfParams, PinVerifier, ScreenLock};
use intent_passkey::protocol::PasskeyError;
use support::{create_params, error_code, get_params, Client};

const ORIGIN: &str = "https://example.com";
const RP: &str = "example.com";
const START_MS: u64 = 1_800_000_000_000;

struct Pam(Arc<Mutex<String>>, Arc<AtomicU64>);

impl PinVerifier for Pam {
    fn verify(&self, pin: &str) -> Result<bool, PasskeyError> {
        std::thread::sleep(Duration::from_millis(self.1.load(Ordering::SeqCst)));
        Ok(*self.0.lock().unwrap() == pin)
    }
}

struct Screen(Arc<AtomicBool>);

impl ScreenLock for Screen {
    fn locked(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

struct Rig {
    home: tempfile::TempDir,
    pin: Arc<Mutex<String>>,
    pam_delay_ms: Arc<AtomicU64>,
    screen: Arc<AtomicBool>,
    clock: Arc<AtomicU64>,
}

impl Rig {
    fn new(pin: &str) -> Self {
        let rig = Self {
            home: tempfile::tempdir().unwrap(),
            pin: Arc::new(Mutex::new(pin.to_owned())),
            pam_delay_ms: Arc::new(AtomicU64::new(0)),
            screen: Arc::new(AtomicBool::new(false)),
            clock: Arc::new(AtomicU64::new(START_MS)),
        };
        std::fs::write(rig.pam_file(), "auth include login\n").unwrap();
        rig
    }

    fn pam_file(&self) -> PathBuf {
        self.home.path().join("pam.d-intent-passkey")
    }

    fn store_dir(&self) -> PathBuf {
        self.home.path().join("share/intent/passkeys")
    }

    fn store_path(&self) -> PathBuf {
        self.store_dir().join("store.json")
    }

    fn set_pin(&self, pin: &str) {
        pin.clone_into(&mut self.pin.lock().unwrap());
    }

    fn advance(&self, ms: u64) {
        self.clock.fetch_add(ms, Ordering::SeqCst);
    }

    /// A fresh sidecar over the same files, as after an Intent restart.
    fn start(&self) -> Client {
        let clock = Arc::clone(&self.clock);
        let config = DeviceConfig {
            dir: self.store_dir(),
            kdf: KdfParams {
                memory_kib: 64,
                iterations: 1,
                parallelism: 1,
            },
            pam_service_file: self.pam_file(),
            boot_id: Some("boot-1".into()),
            screen_poll: Duration::from_millis(10),
            clock: Arc::new(move || clock.load(Ordering::SeqCst)),
        };
        Client::start(Device::new(
            config,
            Pam(Arc::clone(&self.pin), Arc::clone(&self.pam_delay_ms)),
            Screen(Arc::clone(&self.screen)),
        ))
    }

    fn edit_store(&self, edit: impl FnOnce(&mut Value)) {
        let mut store: Value =
            serde_json::from_slice(&std::fs::read(self.store_path()).unwrap()).unwrap();
        edit(&mut store);
        std::fs::write(self.store_path(), serde_json::to_vec(&store).unwrap()).unwrap();
    }
}

/// Runs `method` and answers each `pin-needed` with the next PIN and each
/// `choose-account` with `choice`. Returns the response and the events.
async fn ceremony(
    client: &mut Client,
    id: &str,
    method: &str,
    params: Value,
    pins: &[&str],
    choice: usize,
) -> (Value, Vec<Value>) {
    let mut pins: VecDeque<&str> = pins.iter().copied().collect();
    client.send(id, method, params).await;
    let mut events = Vec::new();
    let mut answers = 0;
    loop {
        let line = client.next().await;
        match line["event"].as_str() {
            Some("pin-needed") => {
                answers += 1;
                let pin = pins.pop_front().expect("a PIN for every pin-needed");
                let answer = format!("{id}-pin-{answers}");
                client
                    .send(&answer, "pin", json!({ "ceremony": id, "pin": pin }))
                    .await;
                events.push(line);
            }
            Some("choose-account") => {
                answers += 1;
                let answer = format!("{id}-choose-{answers}");
                client
                    .send(
                        &answer,
                        "choose",
                        json!({ "ceremony": id, "index": choice }),
                    )
                    .await;
                events.push(line);
            }
            Some(_) => events.push(line),
            None if line["id"] == id => return (line, events),
            None => assert!(line.get("error").is_none(), "answer failed: {line}"),
        }
    }
}

/// The next `pin-needed`, skipping status events and the `{}` answers to `pin`.
async fn next_prompt(client: &mut Client) -> Value {
    loop {
        let line = client.next().await;
        if line["event"] == "pin-needed" {
            return line;
        }
        if line["event"] == "status" {
            continue;
        }
        assert_eq!(line["result"], json!({}), "unexpected line: {line}");
    }
}

fn pin_prompts(events: &[Value]) -> Vec<&Value> {
    events
        .iter()
        .filter(|event| event["event"] == "pin-needed")
        .collect()
}

fn bytes(value: &Value) -> Vec<u8> {
    URL_SAFE_NO_PAD
        .decode(value.as_str().expect("a base64url string"))
        .unwrap()
}

fn user_params(user: &str, algs: &[i64]) -> Value {
    let mut params = create_params("device", ORIGIN, RP);
    params["user"] = json!({
        "id": URL_SAFE_NO_PAD.encode(user),
        "name": format!("{user}@example.com"),
        "displayName": user,
    });
    params["pubKeyCredParams"] = algs
        .iter()
        .map(|alg| json!({ "type": "public-key", "alg": alg }))
        .collect();
    params
}

fn allow(params: &mut Value, credential_id: &Value) {
    params["allowCredentials"] = json!([{ "id": credential_id, "transports": ["internal"] }]);
}

fn verify_assertion(public_key_spki: &[u8], alg: i64, assertion: &Value) {
    let mut signed = bytes(&assertion["authenticatorData"]);
    signed.extend(Sha256::digest(bytes(&assertion["clientDataJSON"])));
    let signature = bytes(&assertion["signature"]);
    match alg {
        -7 => {
            use p256::ecdsa::signature::Verifier as _;
            let key = p256::ecdsa::VerifyingKey::from_public_key_der(public_key_spki).unwrap();
            let signature = p256::ecdsa::Signature::from_der(&signature).unwrap();
            key.verify(&signed, &signature)
                .expect("ES256 signature verifies");
        }
        -8 => {
            let key = ed25519_dalek::VerifyingKey::from_public_key_der(public_key_spki).unwrap();
            let signature = ed25519_dalek::Signature::from_slice(&signature).unwrap();
            key.verify_strict(&signed, &signature)
                .expect("EdDSA signature verifies");
        }
        other => panic!("unexpected algorithm {other}"),
    }
}

fn check_get(result: &Value, credential_id: &Value, user: &str) {
    let auth = bytes(&result["authenticatorData"]);
    assert_eq!(auth.len(), 37, "no attested data or extensions on get");
    assert_eq!(&auth[..32], Sha256::digest(RP).as_slice());
    assert_eq!(auth[32], 0x05, "UP and UV, not BE or BS");
    assert_eq!(&auth[33..37], &[0, 0, 0, 0], "sign count 0");
    assert_eq!(&result["credentialId"], credential_id);
    assert_eq!(bytes(&result["userHandle"]), user.as_bytes());
    assert_eq!(result["authenticatorAttachment"], "platform");
    let client_data: Value = serde_json::from_slice(&bytes(&result["clientDataJSON"])).unwrap();
    assert_eq!(
        client_data,
        json!({
            "type": "webauthn.get",
            "challenge": "Z2V0LWNoYWxsZW5nZS1nZXQtY2hhbGxlbmdl",
            "origin": ORIGIN,
            "crossOrigin": false,
        })
    );
}

#[tokio::test]
async fn create_then_get_verifies_with_the_returned_key() {
    let rig = Rig::new("246810");
    let mut client = rig.start();

    let hello = client.call("hello", "hello", json!({})).await;
    assert_eq!(hello["result"]["transports"]["device"], true);

    let (created, events) = ceremony(
        &mut client,
        "c1",
        "create",
        user_params("ada", &[-7, -8]),
        &["246810"],
        0,
    )
    .await;
    let result = &created["result"];
    assert_eq!(
        pin_prompts(&events),
        [
            &json!({ "event": "pin-needed", "ceremony": "c1", "kind": "device", "attemptsLeft": 10, "wrongPin": false })
        ]
    );
    assert_eq!(result["publicKeyAlgorithm"], -7);
    assert_eq!(result["authenticatorAttachment"], "platform");
    assert_eq!(result["transports"], json!(["internal"]));
    assert_eq!(result["extensions"], json!({ "credProps": { "rk": true } }));
    let client_data: Value = serde_json::from_slice(&bytes(&result["clientDataJSON"])).unwrap();
    assert_eq!(
        String::from_utf8(bytes(&result["clientDataJSON"])).unwrap(),
        r#"{"type":"webauthn.create","challenge":"Y2hhbGxlbmdlLWNoYWxsZW5nZS1jaGFsbGVuZ2U","origin":"https://example.com","crossOrigin":false}"#,
        "the shared clientDataJSON builder: {client_data}"
    );

    let auth = bytes(&result["authenticatorData"]);
    let credential_id = bytes(&result["credentialId"]);
    assert_eq!(&auth[..32], Sha256::digest(RP).as_slice());
    assert_eq!(auth[32], 0x45, "UP, UV and AT; BE and BS clear");
    assert_eq!(&auth[33..37], &[0, 0, 0, 0], "sign count 0");
    assert_eq!(&auth[37..53], &[0u8; 16], "zero AAGUID");
    assert_eq!(
        usize::from(u16::from_be_bytes([auth[53], auth[54]])),
        credential_id.len()
    );
    assert_eq!(
        &auth[55..55 + credential_id.len()],
        credential_id.as_slice()
    );
    let cose: Cbor = ciborium::from_reader(&auth[55 + credential_id.len()..]).unwrap();
    let cose = cose.as_map().unwrap();
    assert_eq!(cose[0], (1.into(), 2.into()), "kty EC2");
    assert_eq!(cose[1], (3.into(), (-7).into()), "alg ES256");
    let attestation: Cbor =
        ciborium::from_reader(bytes(&result["attestationObject"]).as_slice()).unwrap();
    assert_eq!(
        attestation,
        Cbor::Map(vec![
            ("fmt".into(), "none".into()),
            ("attStmt".into(), Cbor::Map(vec![])),
            ("authData".into(), Cbor::Bytes(auth.clone())),
        ])
    );

    let public_key = bytes(&result["publicKey"]);
    let mut params = get_params("device", ORIGIN, RP);
    allow(&mut params, &result["credentialId"]);
    let (assertion, _) = ceremony(&mut client, "g1", "get", params, &["246810"], 0).await;
    check_get(&assertion["result"], &result["credentialId"], "ada");
    verify_assertion(&public_key, -7, &assertion["result"]);

    // The excluded credential is refused before any PIN prompt.
    let mut params = user_params("ada", &[-7]);
    params["excludeCredentials"] = json!([{ "id": result["credentialId"] }]);
    let (excluded, events) = ceremony(&mut client, "c2", "create", params, &[], 0).await;
    assert_eq!(error_code(&excluded), "InvalidState");
    assert!(pin_prompts(&events).is_empty());

    // EdDSA when the site lists it first; two accounts then need the chooser.
    let (eddsa, _) = ceremony(
        &mut client,
        "c3",
        "create",
        user_params("bob", &[-8, -7]),
        &["246810"],
        0,
    )
    .await;
    assert_eq!(eddsa["result"]["publicKeyAlgorithm"], -8);
    let (chosen, events) = ceremony(
        &mut client,
        "g2",
        "get",
        get_params("device", ORIGIN, RP),
        &["246810"],
        1,
    )
    .await;
    let accounts = &events
        .iter()
        .find(|event| event["event"] == "choose-account")
        .expect("an account chooser")["accounts"];
    assert_eq!(accounts.as_array().unwrap().len(), 2);
    let picked = &accounts[1];
    let user = picked["userDisplayName"].as_str().unwrap();
    let (expected_id, key, alg) = if user == "bob" {
        (
            &eddsa["result"]["credentialId"],
            bytes(&eddsa["result"]["publicKey"]),
            -8,
        )
    } else {
        (&result["credentialId"], public_key.clone(), -7)
    };
    check_get(&chosen["result"], expected_id, user);
    verify_assertion(&key, alg, &chosen["result"]);

    // Another site sees none of them.
    let (other, events) = ceremony(
        &mut client,
        "g3",
        "get",
        get_params("device", "https://example.org", "example.org"),
        &[],
        0,
    )
    .await;
    assert_eq!(error_code(&other), "NotAllowed");
    assert!(pin_prompts(&events).is_empty());

    let list = client.call("l1", "device.list", json!({})).await;
    let credentials = list["result"]["credentials"].as_array().unwrap();
    assert_eq!(credentials.len(), 2);
    let ada = credentials
        .iter()
        .find(|credential| credential["userName"] == "ada@example.com")
        .unwrap();
    assert_eq!(ada["rpId"], RP);
    assert_eq!(ada["userDisplayName"], "ada");
    assert_eq!(ada["createdAt"], START_MS);
    assert_eq!(ada["lastUsedAt"], START_MS);

    let deleted = client
        .call(
            "d1",
            "device.delete",
            json!({ "credentialId": result["credentialId"] }),
        )
        .await;
    assert_eq!(deleted["result"], json!({}));
    let list = client.call("l2", "device.list", json!({})).await;
    assert_eq!(list["result"]["credentials"].as_array().unwrap().len(), 1);

    let dir = std::fs::metadata(rig.store_dir())
        .unwrap()
        .permissions()
        .mode();
    let file = std::fs::metadata(rig.store_path())
        .unwrap()
        .permissions()
        .mode();
    assert_eq!((dir & 0o777, file & 0o777), (0o700, 0o600));
}

#[tokio::test]
async fn wrong_pins_wait_then_lock_until_the_screen_is_unlocked() {
    let rig = Rig::new("246810");
    let mut client = rig.start();

    // Five wrong PINs are free; the next try waits 30 s.
    client.send("c1", "create", user_params("ada", &[-7])).await;
    let mut prompts = Vec::new();
    for attempt in 0..6 {
        let prompt = next_prompt(&mut client).await;
        prompts.push(prompt);
        if attempt < 5 {
            client
                .send(
                    &format!("p{attempt}"),
                    "pin",
                    json!({ "ceremony": "c1", "pin": "000000" }),
                )
                .await;
        }
    }
    let left: Vec<_> = prompts.iter().map(|p| p["attemptsLeft"].clone()).collect();
    assert_eq!(left, [10, 9, 8, 7, 6, 5].map(Value::from));
    assert_eq!(prompts[0]["wrongPin"], false);
    assert!(prompts[1..].iter().all(|p| p["wrongPin"] == true));
    assert!(prompts[..5].iter().all(|p| p.get("retryAfterMs").is_none()));
    assert_eq!(prompts[5]["retryAfterMs"], 30_000);

    // The right PIN is not even checked during the wait.
    rig.advance(10_000);
    client
        .send("p5", "pin", json!({ "ceremony": "c1", "pin": "246810" }))
        .await;
    let prompt = next_prompt(&mut client).await;
    assert_eq!(prompt["retryAfterMs"], 20_000);
    assert_eq!(prompt["attemptsLeft"], 5);

    // After the wait a wrong PIN doubles it.
    rig.advance(20_000);
    client
        .send("p6", "pin", json!({ "ceremony": "c1", "pin": "000000" }))
        .await;
    let prompt = next_prompt(&mut client).await;
    assert_eq!(prompt["retryAfterMs"], 60_000);
    assert_eq!(prompt["attemptsLeft"], 4);
    client
        .send("x1", "cancel", json!({ "ceremony": "c1" }))
        .await;
    let (cancelled, _) = client.response("c1").await;
    assert_eq!(error_code(&cancelled), "Abort");

    // The counter survives a restart.
    drop(client);
    let mut client = rig.start();
    let mut wait = 60_000;
    client.send("c2", "create", user_params("ada", &[-7])).await;
    for (attempt, left) in (0..4).zip([4, 3, 2, 1]) {
        let prompt = next_prompt(&mut client).await;
        assert_eq!(prompt["attemptsLeft"], left);
        assert_eq!(prompt["retryAfterMs"], wait);
        rig.advance(wait);
        wait *= 2;
        client
            .send(
                &format!("q{attempt}"),
                "pin",
                json!({ "ceremony": "c2", "pin": "000000" }),
            )
            .await;
    }
    let (locked, _) = client.response("c2").await;
    assert_eq!(error_code(&locked), "PinLocked");

    // Locked: no prompt at all, even much later or after a restart.
    rig.advance(24 * 3_600_000);
    drop(client);
    let mut client = rig.start();
    let (locked, events) = ceremony(
        &mut client,
        "c3",
        "create",
        user_params("ada", &[-7]),
        &[],
        0,
    )
    .await;
    assert_eq!(error_code(&locked), "PinLocked");
    assert!(pin_prompts(&events).is_empty());

    // The lock screen opens and closes (the user typed the PIN there).
    rig.screen.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    rig.screen.store(false, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (created, events) = ceremony(
        &mut client,
        "c4",
        "create",
        user_params("ada", &[-7]),
        &["246810"],
        0,
    )
    .await;
    assert!(created.get("result").is_some(), "{created}");
    assert_eq!(pin_prompts(&events)[0]["attemptsLeft"], 10);
}

#[tokio::test]
async fn a_changed_pin_asks_once_for_the_old_pin_and_rewraps() {
    let rig = Rig::new("1234");
    let mut client = rig.start();
    let (created, _) = ceremony(
        &mut client,
        "c1",
        "create",
        user_params("ada", &[-7]),
        &["1234"],
        0,
    )
    .await;
    let credential_id = created["result"]["credentialId"].clone();
    let public_key = bytes(&created["result"]["publicKey"]);
    let salt = |rig: &Rig| {
        let store: Value =
            serde_json::from_slice(&std::fs::read(rig.store_path()).unwrap()).unwrap();
        store["kdf"]["salt"].clone()
    };
    let first_salt = salt(&rig);

    // A wrong previous PIN fails the ceremony and counts as a wrong try.
    rig.set_pin("55667788");
    let (failed, events) = ceremony(
        &mut client,
        "g1",
        "get",
        get_params("device", ORIGIN, RP),
        &["55667788", "4321"],
        0,
    )
    .await;
    assert_eq!(error_code(&failed), "NotAllowed");
    let prompts = pin_prompts(&events);
    assert_eq!(prompts[1]["oldPin"], true);
    assert_eq!(prompts[1]["attemptsLeft"], 10);
    assert_eq!(salt(&rig), first_salt, "nothing re-wrapped");

    // Wrong old PINs are an offline-style oracle: they add up under the same
    // limit although PAM accepts the new PIN each time, and wait like any PIN.
    for (round, left) in (2..6).zip([9, 8, 7, 6]) {
        let (failed, events) = ceremony(
            &mut client,
            &format!("w{round}"),
            "get",
            get_params("device", ORIGIN, RP),
            &["55667788", "4321"],
            0,
        )
        .await;
        assert_eq!(error_code(&failed), "NotAllowed");
        let prompts = pin_prompts(&events);
        assert_eq!(prompts[0]["attemptsLeft"], left);
        assert_eq!(prompts[1]["attemptsLeft"], left);
    }
    client
        .send("w6", "get", get_params("device", ORIGIN, RP))
        .await;
    let prompt = next_prompt(&mut client).await;
    assert_eq!(prompt["retryAfterMs"], 30_000);
    rig.advance(30_000);
    client
        .send(
            "w6-1",
            "pin",
            json!({ "ceremony": "w6", "pin": "55667788" }),
        )
        .await;
    let prompt = next_prompt(&mut client).await;
    assert_eq!(prompt["oldPin"], true);
    assert_eq!(prompt["attemptsLeft"], 5);
    assert!(prompt.get("retryAfterMs").is_none());
    client
        .send("w6-x", "cancel", json!({ "ceremony": "w6" }))
        .await;
    let (cancelled, _) = client.response("w6").await;
    assert_eq!(error_code(&cancelled), "Abort");

    let (assertion, events) = ceremony(
        &mut client,
        "g2",
        "get",
        get_params("device", ORIGIN, RP),
        &["55667788", "1234"],
        0,
    )
    .await;
    let prompts = pin_prompts(&events);
    assert_eq!(prompts.len(), 2);
    assert_eq!(prompts[0]["attemptsLeft"], 5);
    assert!(prompts[0].get("oldPin").is_none());
    assert_eq!(prompts[1]["oldPin"], true);
    check_get(&assertion["result"], &credential_id, "ada");
    verify_assertion(&public_key, -7, &assertion["result"]);
    assert_ne!(salt(&rig), first_salt, "re-wrapped with a new salt");

    // The new PIN alone works from now on, also after a restart.
    drop(client);
    let mut client = rig.start();
    let (assertion, events) = ceremony(
        &mut client,
        "g3",
        "get",
        get_params("device", ORIGIN, RP),
        &["55667788"],
        0,
    )
    .await;
    assert_eq!(pin_prompts(&events).len(), 1);
    verify_assertion(&public_key, -7, &assertion["result"]);
}

#[tokio::test]
async fn a_damaged_store_is_an_error_not_a_crash() {
    let rig = Rig::new("246810");
    let mut client = rig.start();
    let (created, _) = ceremony(
        &mut client,
        "c1",
        "create",
        user_params("ada", &[-7]),
        &["246810"],
        0,
    )
    .await;
    assert!(created.get("result").is_some());
    let pristine = std::fs::read(rig.store_path()).unwrap();
    let get = || get_params("device", ORIGIN, RP);

    // A flipped ciphertext byte in the credential key.
    rig.edit_store(|store| {
        let sealed = &mut store["credentials"][0]["privateKey"]["ciphertext"];
        let mut raw = bytes(sealed);
        raw[0] ^= 1;
        *sealed = json!(URL_SAFE_NO_PAD.encode(raw));
    });
    let (damaged, _) = ceremony(&mut client, "g1", "get", get(), &["246810"], 0).await;
    assert_eq!(error_code(&damaged), "Unknown");

    // The key moved to another site: the AAD binds it to its RP ID.
    std::fs::write(rig.store_path(), &pristine).unwrap();
    rig.edit_store(|store| store["credentials"][0]["rpId"] = json!("example.org"));
    let (moved, _) = ceremony(
        &mut client,
        "g2",
        "get",
        get_params("device", "https://example.org", "example.org"),
        &["246810"],
        0,
    )
    .await;
    assert_eq!(error_code(&moved), "Unknown");

    // Absurd key derivation cost.
    std::fs::write(rig.store_path(), &pristine).unwrap();
    rig.edit_store(|store| store["kdf"]["memoryKib"] = json!(u32::MAX));
    let (costly, _) = ceremony(&mut client, "g3", "get", get(), &["246810"], 0).await;
    assert_eq!(error_code(&costly), "Unknown");

    // Wrong field types and plain garbage.
    rig.edit_store(|store| store["credentials"] = json!("nope"));
    let list = client.call("l1", "device.list", json!({})).await;
    assert_eq!(error_code(&list), "Unknown");
    std::fs::write(rig.store_path(), b"\x00not json").unwrap();
    let list = client.call("l2", "device.list", json!({})).await;
    assert_eq!(error_code(&list), "Unknown");
    let (create, _) = ceremony(
        &mut client,
        "c2",
        "create",
        user_params("bob", &[-7]),
        &[],
        0,
    )
    .await;
    assert_eq!(error_code(&create), "Unknown");

    // Still serving, and the original file works again.
    let hello = client.call("h1", "hello", json!({})).await;
    assert_eq!(hello["result"]["protocol"], 1);
    std::fs::write(rig.store_path(), &pristine).unwrap();
    let (assertion, _) = ceremony(&mut client, "g4", "get", get(), &["246810"], 0).await;
    assert!(assertion.get("result").is_some(), "{assertion}");
}

#[tokio::test]
async fn device_passkeys_are_off_without_the_pam_service() {
    let rig = Rig::new("246810");
    std::fs::remove_file(rig.pam_file()).unwrap();
    let mut client = rig.start();
    let hello = client.call("h1", "hello", json!({})).await;
    assert_eq!(hello["result"]["transports"]["device"], false);
    let (create, events) = ceremony(
        &mut client,
        "c1",
        "create",
        user_params("ada", &[-7]),
        &[],
        0,
    )
    .await;
    assert_eq!(error_code(&create), "NotSupported");
    assert!(events.is_empty());
}

/// Sends `pin` to `ceremony`, cancels it while PAM is still checking, and returns
/// the ceremony's response.
async fn cancel_during_pam(
    client: &mut Client,
    id: &str,
    method: &str,
    params: Value,
    pin: &str,
) -> Value {
    client.send(id, method, params).await;
    next_prompt(client).await;
    client
        .send(
            &format!("{id}-pin"),
            "pin",
            json!({ "ceremony": id, "pin": pin }),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    client
        .send(&format!("{id}-cancel"), "cancel", json!({ "ceremony": id }))
        .await;
    client.response(id).await.0
}

#[tokio::test]
async fn a_cancel_during_the_pin_check_commits_nothing() {
    let rig = Rig::new("246810");
    let mut client = rig.start();
    let (created, _) = ceremony(
        &mut client,
        "c1",
        "create",
        user_params("ada", &[-7]),
        &["246810"],
        0,
    )
    .await;
    let original = created["result"]["credentialId"].clone();
    rig.pam_delay_ms.store(400, Ordering::SeqCst);

    // A cancelled re-registration keeps the existing passkey.
    let response = cancel_during_pam(
        &mut client,
        "c2",
        "create",
        user_params("ada", &[-7]),
        "246810",
    )
    .await;
    assert_eq!(error_code(&response), "Abort");

    // A cancelled sign-in returns no assertion and records no use.
    let response = cancel_during_pam(
        &mut client,
        "g1",
        "get",
        get_params("device", ORIGIN, RP),
        "246810",
    )
    .await;
    assert_eq!(error_code(&response), "Abort");
    let list = client.call("l1", "device.list", json!({})).await;
    let credentials = list["result"]["credentials"].as_array().unwrap();
    assert_eq!(credentials.len(), 1);
    assert_eq!(credentials[0]["credentialId"], original);
    assert_eq!(credentials[0]["lastUsedAt"], Value::Null);

    // A wrong PIN still counts when the ceremony is cancelled during its check.
    let response = cancel_during_pam(
        &mut client,
        "g2",
        "get",
        get_params("device", ORIGIN, RP),
        "000000",
    )
    .await;
    assert_eq!(error_code(&response), "Abort");
    rig.pam_delay_ms.store(0, Ordering::SeqCst);
    let (_, events) = ceremony(
        &mut client,
        "g3",
        "get",
        get_params("device", ORIGIN, RP),
        &["246810"],
        0,
    )
    .await;
    assert_eq!(pin_prompts(&events)[0]["attemptsLeft"], 9);
}
