//! Daemon identity and request proofs for the push relay
//! (`intent-hq/intent-push`, README → Contract).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ring::signature::{Ed25519KeyPair, KeyPair as _};
use sha2::{Digest as _, Sha256};

const HEADER: &str = r#"{"alg":"EdDSA","typ":"intent-daemon+jwt"}"#;

pub(crate) fn b64(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

pub(crate) fn sha256_b64(bytes: &[u8]) -> String {
    b64(&Sha256::digest(bytes))
}

/// The daemon's Ed25519 key. Signatures are deterministic, so a fixed seed
/// and request give a fixed proof (the shared golden vector).
pub(crate) struct DaemonKey {
    pair: Ed25519KeyPair,
}

#[derive(serde::Serialize)]
struct Claims<'a> {
    sub: &'a str,
    htm: &'a str,
    htu: &'a str,
    bsh: String,
    iat: i64,
    jti: &'a str,
}

impl DaemonKey {
    pub(crate) fn from_seed(seed: &[u8]) -> Option<Self> {
        Ed25519KeyPair::from_seed_unchecked(seed)
            .ok()
            .map(|pair| Self { pair })
    }

    pub(crate) fn generate_seed() -> [u8; 32] {
        let mut seed = [0u8; 32];
        seed[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        seed[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
        seed
    }

    /// Raw 32-byte public key, base64url.
    pub(crate) fn public_key(&self) -> String {
        b64(self.pair.public_key().as_ref())
    }

    /// `daemonId`: base64url SHA-256 of the raw public key.
    pub(crate) fn daemon_id(&self) -> String {
        sha256_b64(self.pair.public_key().as_ref())
    }

    /// The compact JWS for `Authorization: IntentProof <JWS>`.
    pub(crate) fn proof(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        iat: i64,
        jti: &str,
    ) -> String {
        let sub = self.daemon_id();
        let claims = Claims {
            sub: &sub,
            htm: method,
            htu: path,
            bsh: sha256_b64(body),
            iat,
            jti,
        };
        let payload = serde_json::to_vec(&claims).unwrap_or_default();
        let input = format!("{}.{}", b64(HEADER.as_bytes()), b64(&payload));
        let signature = self.pair.sign(input.as_bytes());
        format!("{input}.{}", b64(signature.as_ref()))
    }
}
