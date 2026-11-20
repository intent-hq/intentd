//! Device credential keys (ES256, `EdDSA`) and the `WebAuthn` byte formats a device
//! authenticator returns: COSE public keys, authenticator data and a `none`
//! attestation object.

use ciborium::value::Value as Cbor;
use p256::ecdsa::signature::Signer as _;
use p256::pkcs8::EncodePublicKey as _;
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

use crate::protocol::{CredParam, ErrorCode, PasskeyError};

pub(crate) const ES256: i64 = -7;
pub(crate) const EDDSA: i64 = -8;

const FLAG_UP: u8 = 0x01;
const FLAG_UV: u8 = 0x04;
const FLAG_AT: u8 = 0x40;
const AAGUID: [u8; 16] = [0; 16];

/// The first algorithm in the site's preference order that we support.
pub(crate) fn pick_algorithm(params: &[CredParam]) -> Option<i64> {
    params
        .iter()
        .filter(|param| param.kind == "public-key")
        .map(|param| param.alg)
        .find(|alg| [ES256, EDDSA].contains(alg))
}

pub(crate) struct KeyPair {
    pub secret: Zeroizing<Vec<u8>>,
    pub spki: Vec<u8>,
    pub cose: Vec<u8>,
}

fn cbor(value: &Cbor) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::into_writer(value, &mut out).expect("CBOR encoding into a Vec cannot fail");
    out
}

fn internal(message: &str) -> PasskeyError {
    PasskeyError::new(ErrorCode::Unknown, message)
}

pub(crate) fn generate(alg: i64) -> Result<KeyPair, PasskeyError> {
    let mut rng = rand::rngs::OsRng;
    match alg {
        ES256 => {
            let key = p256::ecdsa::SigningKey::random(&mut rng);
            let public = key.verifying_key();
            let point = public.to_encoded_point(false);
            let (Some(x), Some(y)) = (point.x(), point.y()) else {
                return Err(internal("invalid P-256 public key"));
            };
            let cose = cbor(&Cbor::Map(vec![
                (1.into(), 2.into()),
                (3.into(), ES256.into()),
                ((-1).into(), 1.into()),
                ((-2).into(), Cbor::Bytes(x.to_vec())),
                ((-3).into(), Cbor::Bytes(y.to_vec())),
            ]));
            let spki = public
                .to_public_key_der()
                .map_err(|_| internal("could not encode the public key"))?;
            Ok(KeyPair {
                secret: Zeroizing::new(key.to_bytes().to_vec()),
                spki: spki.into_vec(),
                cose,
            })
        }
        EDDSA => {
            let key = ed25519_dalek::SigningKey::generate(&mut rng);
            let public = key.verifying_key();
            let cose = cbor(&Cbor::Map(vec![
                (1.into(), 1.into()),
                (3.into(), EDDSA.into()),
                ((-1).into(), 6.into()),
                ((-2).into(), Cbor::Bytes(public.to_bytes().to_vec())),
            ]));
            let spki = public
                .to_public_key_der()
                .map_err(|_| internal("could not encode the public key"))?;
            Ok(KeyPair {
                secret: Zeroizing::new(key.to_bytes().to_vec()),
                spki: spki.into_vec(),
                cose,
            })
        }
        _ => Err(PasskeyError::new(
            ErrorCode::NotSupported,
            "unsupported algorithm",
        )),
    }
}

/// Signs `message` (`authenticatorData || SHA-256(clientDataJSON)`): ES256 as a DER
/// ECDSA signature, `EdDSA` as the raw 64 bytes.
pub(crate) fn sign(alg: i64, secret: &[u8], message: &[u8]) -> Result<Vec<u8>, PasskeyError> {
    let damaged = || super::store::damaged("a credential key is invalid");
    match alg {
        ES256 => {
            let key = p256::ecdsa::SigningKey::from_slice(secret).map_err(|_| damaged())?;
            let signature: p256::ecdsa::Signature = key.sign(message);
            Ok(signature.to_der().as_bytes().to_vec())
        }
        EDDSA => {
            let seed: Zeroizing<[u8; 32]> =
                Zeroizing::new(secret.try_into().map_err(|_| damaged())?);
            let key = ed25519_dalek::SigningKey::from_bytes(&seed);
            Ok(ed25519_dalek::Signer::sign(&key, message)
                .to_bytes()
                .to_vec())
        }
        _ => Err(damaged()),
    }
}

fn auth_data_head(rp_id: &str, flags: u8) -> Vec<u8> {
    let mut data = Sha256::digest(rp_id.as_bytes()).to_vec();
    data.push(flags);
    data.extend_from_slice(&0u32.to_be_bytes());
    data
}

/// UP and UV set, BE and BS clear (device-bound), sign count 0, zero AAGUID.
pub(crate) fn create_auth_data(rp_id: &str, credential_id: &[u8], cose: &[u8]) -> Vec<u8> {
    let mut data = auth_data_head(rp_id, FLAG_UP | FLAG_UV | FLAG_AT);
    data.extend_from_slice(&AAGUID);
    let id_len = u16::try_from(credential_id.len()).expect("credential IDs are 32 bytes");
    data.extend_from_slice(&id_len.to_be_bytes());
    data.extend_from_slice(credential_id);
    data.extend_from_slice(cose);
    data
}

pub(crate) fn get_auth_data(rp_id: &str) -> Vec<u8> {
    auth_data_head(rp_id, FLAG_UP | FLAG_UV)
}

pub(crate) fn none_attestation(auth_data: &[u8]) -> Vec<u8> {
    cbor(&Cbor::Map(vec![
        ("fmt".into(), "none".into()),
        ("attStmt".into(), Cbor::Map(Vec::new())),
        ("authData".into(), Cbor::Bytes(auth_data.to_vec())),
    ]))
}

pub(crate) fn signed_message(auth_data: &[u8], client_data_json: &[u8]) -> Vec<u8> {
    let mut message = auth_data.to_vec();
    message.extend_from_slice(&Sha256::digest(client_data_json));
    message
}
