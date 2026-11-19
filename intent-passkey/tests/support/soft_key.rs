//! A minimal software CTAP2.0 security key behind libwebauthn's `virt` HID pipe:
//! getInfo, clientPIN (protocol 1: getRetries, getKeyAgreement, getPinToken),
//! makeCredential, getAssertion and getNextAssertion. User presence is immediate.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use aes::cipher::block_padding::NoPadding;
use aes::cipher::{BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use ciborium::Value;
use hmac::{Hmac, Mac};
use libwebauthn::transport::hid::framing::{HidCommand, HidMessage};
use libwebauthn::transport::hid::{virtual_device, HidDevice, HidPipeBackend};
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use p256::elliptic_curve::sec1::{FromEncodedPoint, ToEncodedPoint};
use p256::{EncodedPoint, PublicKey, SecretKey};
use rand::RngCore;
use sha2::{Digest, Sha256};

const OK: u8 = 0x00;
const INVALID_COMMAND: u8 = 0x01;
const INVALID_PARAMETER: u8 = 0x02;
const CREDENTIAL_EXCLUDED: u8 = 0x19;
const NO_CREDENTIALS: u8 = 0x2E;
const NOT_ALLOWED: u8 = 0x30;
const PIN_INVALID: u8 = 0x31;
const PIN_BLOCKED: u8 = 0x32;
const PIN_AUTH_INVALID: u8 = 0x33;
const PIN_REQUIRED: u8 = 0x36;
const USER_ACTION_TIMEOUT: u8 = 0x2F;

const AAGUID: [u8; 16] = *b"intent-test-key!";

#[derive(Clone)]
struct Credential {
    id: Vec<u8>,
    rp_id: String,
    user_id: Vec<u8>,
    user_name: String,
    display_name: String,
    key: SigningKey,
}

struct State {
    pin_hash: [u8; 16],
    retries: u8,
    key_agreement: SecretKey,
    token: Option<[u8; 32]>,
    credentials: Vec<Credential>,
    pending_assertions: VecDeque<Value>,
    outbox: VecDeque<HidMessage>,
    touchable: bool,
}

/// Shared handle so tests can inspect the key while libwebauthn drives it.
#[derive(Clone)]
pub struct SoftKey(Arc<Mutex<State>>);

impl std::fmt::Debug for SoftKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SoftKey")
    }
}

fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

fn int(value: i64) -> Value {
    Value::Integer(value.into())
}

fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

fn get(map: &Value, key: i64) -> Option<&Value> {
    map.as_map()?
        .iter()
        .find(|(k, _)| k.as_integer() == Some(key.into()))
        .map(|(_, v)| v)
}

fn get_text<'a>(map: &'a Value, key: &str) -> Option<&'a Value> {
    map.as_map()?
        .iter()
        .find(|(k, _)| k.as_text() == Some(key))
        .map(|(_, v)| v)
}

fn bytes(value: Option<&Value>) -> Option<Vec<u8>> {
    value?.as_bytes().cloned()
}

fn encode(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::into_writer(value, &mut out).unwrap();
    out
}

fn cose_key(public: &PublicKey, alg: i64) -> Value {
    let point = public.to_encoded_point(false);
    Value::Map(vec![
        (int(1), int(2)),
        (int(3), int(alg)),
        (int(-1), int(1)),
        (int(-2), Value::Bytes(point.x().unwrap().to_vec())),
        (int(-3), Value::Bytes(point.y().unwrap().to_vec())),
    ])
}

fn shared_secret(state: &State, platform_key: &Value) -> Option<[u8; 32]> {
    let x = bytes(get(platform_key, -2))?;
    let y = bytes(get(platform_key, -3))?;
    let point =
        EncodedPoint::from_affine_coordinates(x.as_slice().into(), y.as_slice().into(), false);
    let public = Option::<PublicKey>::from(PublicKey::from_encoded_point(&point))?;
    let shared =
        p256::ecdh::diffie_hellman(state.key_agreement.to_nonzero_scalar(), public.as_affine());
    Some(sha256(shared.raw_secret_bytes()))
}

type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;
type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;

fn pin_auth_ok(token: Option<[u8; 32]>, client_data_hash: &[u8], pin_auth: &[u8]) -> bool {
    let Some(token) = token else { return false };
    let mut mac = Hmac::<Sha256>::new_from_slice(&token).unwrap();
    mac.update(client_data_hash);
    let full = mac.finalize().into_bytes();
    full.get(..pin_auth.len()) == Some(pin_auth)
}

impl SoftKey {
    pub fn new(pin: &str, retries: u8) -> Self {
        let mut pin_hash = [0u8; 16];
        pin_hash.copy_from_slice(&sha256(pin.as_bytes())[..16]);
        Self(Arc::new(Mutex::new(State {
            pin_hash,
            retries,
            key_agreement: SecretKey::random(&mut rand::thread_rng()),
            token: None,
            credentials: Vec::new(),
            pending_assertions: VecDeque::new(),
            outbox: VecDeque::new(),
            touchable: true,
        })))
    }

    /// A key the user never touches: requests that need presence time out.
    #[must_use]
    pub fn untouched(self) -> Self {
        self.0.lock().unwrap().touchable = false;
        self
    }

    pub fn device(&self) -> HidDevice {
        virtual_device(self.clone())
    }

    pub fn credential_count(&self) -> usize {
        self.0.lock().unwrap().credentials.len()
    }

    fn handle_cbor(state: &mut State, payload: &[u8]) -> Vec<u8> {
        let Some((&command, body)) = payload.split_first() else {
            return vec![INVALID_COMMAND];
        };
        let request: Value = if body.is_empty() {
            Value::Map(Vec::new())
        } else {
            match ciborium::from_reader(body) {
                Ok(value) => value,
                Err(_) => return vec![INVALID_PARAMETER],
            }
        };
        let result = match command {
            0x04 => Ok(Self::get_info()),
            0x06 => Self::client_pin(state, &request),
            0x01 => Self::make_credential(state, &request),
            0x02 => Self::get_assertion(state, &request),
            0x08 => state.pending_assertions.pop_front().ok_or(NOT_ALLOWED),
            _ => Err(INVALID_COMMAND),
        };
        match result {
            Ok(value) => {
                let mut out = vec![OK];
                out.extend(encode(&value));
                out
            }
            Err(status) => vec![status],
        }
    }

    fn get_info() -> Value {
        Value::Map(vec![
            (int(1), Value::Array(vec![text("FIDO_2_0")])),
            (int(3), Value::Bytes(AAGUID.to_vec())),
            (
                int(4),
                Value::Map(vec![
                    (text("rk"), Value::Bool(true)),
                    (text("up"), Value::Bool(true)),
                    (text("plat"), Value::Bool(false)),
                    (text("clientPin"), Value::Bool(true)),
                ]),
            ),
            (int(5), int(1200)),
            (int(6), Value::Array(vec![int(1)])),
        ])
    }

    fn client_pin(state: &mut State, request: &Value) -> Result<Value, u8> {
        let sub = get(request, 2)
            .and_then(Value::as_integer)
            .ok_or(INVALID_PARAMETER)?;
        match i64::try_from(sub).unwrap_or(0) {
            0x01 => Ok(Value::Map(vec![(int(3), int(i64::from(state.retries)))])),
            0x02 => Ok(Value::Map(vec![(
                int(1),
                cose_key(&state.key_agreement.public_key(), -25),
            )])),
            0x05 => {
                if state.retries == 0 {
                    return Err(PIN_BLOCKED);
                }
                let shared = get(request, 3)
                    .and_then(|key| shared_secret(state, key))
                    .ok_or(INVALID_PARAMETER)?;
                let encrypted = bytes(get(request, 6)).ok_or(INVALID_PARAMETER)?;
                let hash = Aes256CbcDec::new(&shared.into(), &[0u8; 16].into())
                    .decrypt_padded_vec_mut::<NoPadding>(&encrypted)
                    .map_err(|_| INVALID_PARAMETER)?;
                state.key_agreement = SecretKey::random(&mut rand::thread_rng());
                if hash != state.pin_hash {
                    state.retries -= 1;
                    return Err(if state.retries == 0 {
                        PIN_BLOCKED
                    } else {
                        PIN_INVALID
                    });
                }
                state.retries = 8;
                let mut token = [0u8; 32];
                rand::thread_rng().fill_bytes(&mut token);
                state.token = Some(token);
                let encrypted = Aes256CbcEnc::new(&shared.into(), &[0u8; 16].into())
                    .encrypt_padded_vec_mut::<NoPadding>(&token);
                Ok(Value::Map(vec![(int(2), Value::Bytes(encrypted))]))
            }
            _ => Err(INVALID_PARAMETER),
        }
    }

    fn check_uv(
        state: &State,
        request: &Value,
        client_data_hash: &[u8],
        auth_key: i64,
    ) -> Result<bool, u8> {
        match bytes(get(request, auth_key)) {
            Some(auth) if pin_auth_ok(state.token, client_data_hash, &auth) => Ok(true),
            Some(_) => Err(PIN_AUTH_INVALID),
            None => Ok(false),
        }
    }

    fn make_credential(state: &mut State, request: &Value) -> Result<Value, u8> {
        let client_data_hash = bytes(get(request, 1)).ok_or(INVALID_PARAMETER)?;
        let rp = get(request, 2).ok_or(INVALID_PARAMETER)?;
        let rp_id = get_text(rp, "id")
            .and_then(Value::as_text)
            .ok_or(INVALID_PARAMETER)?
            .to_owned();
        let user = get(request, 3).ok_or(INVALID_PARAMETER)?;
        if !state.touchable {
            return Err(USER_ACTION_TIMEOUT);
        }
        // CTAP 2.0 selection: a zero-length pinAuth waits for a touch, then fails.
        if bytes(get(request, 8)).is_some_and(|auth| auth.is_empty()) {
            return Err(PIN_INVALID);
        }
        let uv = Self::check_uv(state, request, &client_data_hash, 8)?;
        if !uv {
            return Err(PIN_REQUIRED);
        }
        if let Some(exclude) = get(request, 5).and_then(Value::as_array) {
            for descriptor in exclude {
                let id = bytes(get_text(descriptor, "id"));
                if state
                    .credentials
                    .iter()
                    .any(|cred| Some(&cred.id) == id.as_ref() && cred.rp_id == rp_id)
                {
                    return Err(CREDENTIAL_EXCLUDED);
                }
            }
        }
        let mut id = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut id);
        let key = SigningKey::random(&mut rand::thread_rng());
        let credential = Credential {
            id: id.clone(),
            rp_id: rp_id.clone(),
            user_id: bytes(get_text(user, "id")).ok_or(INVALID_PARAMETER)?,
            user_name: get_text(user, "name")
                .and_then(Value::as_text)
                .unwrap_or_default()
                .to_owned(),
            display_name: get_text(user, "displayName")
                .and_then(Value::as_text)
                .unwrap_or_default()
                .to_owned(),
            key: key.clone(),
        };
        let mut auth_data = sha256(rp_id.as_bytes()).to_vec();
        auth_data.push(0x01 | 0x04 | 0x40);
        auth_data.extend(0u32.to_be_bytes());
        auth_data.extend(AAGUID);
        auth_data.extend(u16::try_from(id.len()).unwrap().to_be_bytes());
        auth_data.extend(&id);
        auth_data.extend(encode(&cose_key(&PublicKey::from(key.verifying_key()), -7)));
        state.credentials.push(credential);
        Ok(Value::Map(vec![
            (int(1), text("none")),
            (int(2), Value::Bytes(auth_data)),
            (int(3), Value::Map(Vec::new())),
        ]))
    }

    fn assertion(
        credential: &Credential,
        client_data_hash: &[u8],
        flags: u8,
        count: Option<usize>,
    ) -> Value {
        let mut auth_data = sha256(credential.rp_id.as_bytes()).to_vec();
        auth_data.push(flags);
        auth_data.extend(1u32.to_be_bytes());
        let mut message = auth_data.clone();
        message.extend(client_data_hash);
        let signature: Signature = credential.key.sign(&message);
        let mut fields = vec![
            (
                int(1),
                Value::Map(vec![
                    (text("id"), Value::Bytes(credential.id.clone())),
                    (text("type"), text("public-key")),
                ]),
            ),
            (int(2), Value::Bytes(auth_data)),
            (int(3), Value::Bytes(signature.to_der().as_bytes().to_vec())),
            (
                int(4),
                Value::Map(vec![
                    (text("id"), Value::Bytes(credential.user_id.clone())),
                    (text("name"), text(&credential.user_name)),
                    (text("displayName"), text(&credential.display_name)),
                ]),
            ),
        ];
        if let Some(count) = count {
            fields.push((int(5), int(i64::try_from(count).unwrap())));
        }
        Value::Map(fields)
    }

    fn get_assertion(state: &mut State, request: &Value) -> Result<Value, u8> {
        let rp_id = get(request, 1)
            .and_then(Value::as_text)
            .ok_or(INVALID_PARAMETER)?
            .to_owned();
        let client_data_hash = bytes(get(request, 2)).ok_or(INVALID_PARAMETER)?;
        let allow: Vec<Vec<u8>> = get(request, 3)
            .and_then(Value::as_array)
            .map(|list| {
                list.iter()
                    .filter_map(|d| bytes(get_text(d, "id")))
                    .collect()
            })
            .unwrap_or_default();
        let up = get(request, 5)
            .and_then(|options| get_text(options, "up"))
            .and_then(Value::as_bool)
            .unwrap_or(true);
        if up && !state.touchable {
            return Err(USER_ACTION_TIMEOUT);
        }
        let uv = Self::check_uv(state, request, &client_data_hash, 6)?;
        let matching: Vec<Credential> = state
            .credentials
            .iter()
            .filter(|cred| cred.rp_id == rp_id && (allow.is_empty() || allow.contains(&cred.id)))
            .cloned()
            .collect();
        let Some((first, rest)) = matching.split_first() else {
            return Err(NO_CREDENTIALS);
        };
        let flags = u8::from(up) | if uv { 0x04 } else { 0 };
        let count = (matching.len() > 1).then_some(matching.len());
        state.pending_assertions = rest
            .iter()
            .map(|cred| Self::assertion(cred, &client_data_hash, flags, None))
            .collect();
        Ok(Self::assertion(first, &client_data_hash, flags, count))
    }
}

impl HidPipeBackend for SoftKey {
    fn send(&mut self, msg: &HidMessage) {
        let mut state = self.0.lock().unwrap();
        match msg.cmd {
            HidCommand::Init => {
                let mut payload = msg.payload.clone();
                payload.extend(0x0102_0304u32.to_be_bytes());
                payload.extend([2, 1, 0, 0, 0x04 | 0x08]);
                state
                    .outbox
                    .push_back(HidMessage::new(msg.cid, HidCommand::Init, &payload));
            }
            HidCommand::Cbor => {
                let response = Self::handle_cbor(&mut state, &msg.payload);
                state
                    .outbox
                    .push_back(HidMessage::new(msg.cid, HidCommand::Cbor, &response));
            }
            HidCommand::Cancel => {}
            other => {
                state.outbox.push_back(HidMessage::new(
                    msg.cid,
                    HidCommand::Error,
                    &[INVALID_COMMAND],
                ));
                eprintln!("soft key: unexpected HID command {other:?}");
            }
        }
    }

    fn recv(&mut self) -> HidMessage {
        self.0
            .lock()
            .unwrap()
            .outbox
            .pop_front()
            .expect("the soft key has a response queued")
    }
}
