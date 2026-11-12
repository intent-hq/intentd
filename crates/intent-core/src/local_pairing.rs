//! Local code pairing (§5.58, §2.7): shared limits, code normalization, wire
//! shapes, typed errors and the service seam the transport calls.
//!
//! A host daemon shows a short code; another Intent on the same computer
//! redeems it on the loopback-only `/local-pair` endpoint and receives a
//! separate, revocable credential bound to the host's primary principal. The
//! stateful pieces (code slot, HMAC key, credential table) live in
//! `intent-services`; the transport gates locality and renders the frames.

use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Value};

use crate::BoxFuture;

/// The `localCodePairing` capability value a daemon advertises in
/// `client.hello`, `system.capabilities` and its discovery record when it
/// implements §5.58. Only exactly this value means support.
pub const CAPABILITY_VERSION: u8 = 1;

/// Code alphabet: the browser sign-in alphabet (no `0`/`O`, `1`/`I`/`L`).
pub const CODE_ALPHABET: &[u8; 31] = b"ABCDEFGHJKMNPQRSTUVWXYZ23456789";
/// Characters in a code, before the display hyphen.
pub const CODE_LENGTH: usize = 6;
/// How long a code stays redeemable after creation.
pub const CODE_LIFETIME: Duration = Duration::from_secs(10 * 60);
/// Wrong tries before the code is cancelled (`locked`).
pub const MAX_WRONG_TRIES: u32 = 5;
/// Domain-separation prefix of the stored code HMAC.
pub const CODE_HMAC_CONTEXT: &str = "intent-local-pairing-code-v1|";
/// Longest accepted `appName`, in characters, after trimming.
pub const MAX_APP_NAME_CHARS: usize = 64;

/// Normalize a typed code: drop spaces and hyphens, uppercase, then require
/// exactly [`CODE_LENGTH`] characters from [`CODE_ALPHABET`]. `None` is a
/// malformed code, which can never match and never counts as a try.
#[must_use]
pub fn normalize_code(input: &str) -> Option<String> {
    let normalized: String = input
        .chars()
        .filter(|c| *c != '-' && !c.is_whitespace())
        .map(|c| c.to_ascii_uppercase())
        .collect();
    (normalized.len() == CODE_LENGTH && normalized.bytes().all(|b| CODE_ALPHABET.contains(&b)))
        .then_some(normalized)
}

/// Display form of a normalized code: two groups of three joined by a hyphen.
#[must_use]
pub fn display_code(normalized: &str) -> String {
    let (head, tail) = normalized.split_at(normalized.len().min(CODE_LENGTH / 2));
    format!("{head}-{tail}")
}

/// Validate a requester's `appName`: trimmed, 1–64 characters, no control
/// characters.
///
/// # Errors
///
/// [`LocalPairingError::InvalidParams`] when the name is empty, too long or
/// contains a control character.
pub fn validate_app_name(input: &str) -> Result<String, LocalPairingError> {
    let name = input.trim();
    let chars = name.chars().count();
    if chars == 0 || chars > MAX_APP_NAME_CHARS || name.chars().any(char::is_control) {
        return Err(LocalPairingError::InvalidParams(format!(
            "appName must be 1-{MAX_APP_NAME_CHARS} characters with no control characters"
        )));
    }
    Ok(name.to_owned())
}

/// The lifecycle state of one code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LocalPairingCodeState {
    Active,
    Redeemed,
    Expired,
    Cancelled,
    Locked,
}

/// Why a code was cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LocalPairingCancelReason {
    User,
    Replaced,
}

/// `LocalPairingCode` on the wire. Never carries the code or its hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalPairingCode {
    pub code_id: String,
    pub state: LocalPairingCodeState,
    pub created_at: String,
    pub expires_at: String,
    pub attempts_remaining: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<LocalPairingCancelReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_name: Option<String>,
}

/// `LocalPairingCredential` on the wire. Never carries a token or hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalPairingCredential {
    pub credential_id: String,
    pub app_name: String,
    pub created_at: String,
    pub last_used_at: Option<String>,
    pub connected: bool,
}

/// A freshly created code: the plaintext display form, returned once.
pub struct CreatedLocalCode {
    pub code: String,
    pub local_code: LocalPairingCode,
}

/// The outcome of a successful redemption: the minted credential. The
/// transport adds the listener's address, fingerprint and names to build the
/// `LocalPairingGrant`.
pub struct RedeemedLocalCode {
    pub credential_id: String,
    /// 64 hex characters; returned only in the grant, stored only as SHA-256.
    pub token: String,
    pub app_name: String,
}

/// Typed local pairing failures with their wire rendering (§5.58 Errors).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalPairingError {
    /// Wrong code; at least one try remains.
    Invalid { attempts_remaining: u32 },
    /// The fifth wrong try, or any try after it.
    Locked,
    /// No active code (expired, used, cancelled, never created, restarted).
    Expired,
    /// Malformed `code`, `appName`, `codeId` or `credentialId`.
    InvalidParams(String),
    /// Unknown `credentialId` on revoke.
    NotFound,
    /// The peer is not on this computer.
    NotLoopback,
    /// The host has no TCP listener.
    ListenerDown,
    /// Any other failure (storage, randomness).
    Internal(String),
}

impl LocalPairingError {
    /// JSON-RPC error code.
    #[must_use]
    pub fn code(&self) -> i32 {
        match self {
            Self::Invalid { .. }
            | Self::Locked
            | Self::Expired
            | Self::InvalidParams(_)
            | Self::NotFound => -32602,
            Self::NotLoopback => -32001,
            Self::ListenerDown | Self::Internal(_) => -32603,
        }
    }

    /// Human-readable message; never carries a code, hash or token.
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::Invalid { attempts_remaining } => {
                format!("wrong code; {attempts_remaining} tries left")
            }
            Self::Locked => "too many wrong tries; ask the host for a new code".into(),
            Self::Expired => "no active code; ask the host for a new code".into(),
            Self::InvalidParams(message) => message.clone(),
            Self::NotFound => "unknown local pairing credential".into(),
            Self::NotLoopback => "local pairing is only available on this computer".into(),
            Self::ListenerDown => {
                "connections are off; turn on connections before creating a code".into()
            }
            Self::Internal(message) => format!("internal error: {message}"),
        }
    }

    /// `error.data`, when the contract defines one.
    #[must_use]
    pub fn data(&self) -> Option<Value> {
        Some(match self {
            Self::Invalid { attempts_remaining } => {
                json!({ "code": "local-code-invalid", "attemptsRemaining": attempts_remaining })
            }
            Self::Locked => json!({ "code": "local-code-locked" }),
            Self::Expired => json!({ "code": "local-code-expired" }),
            Self::InvalidParams(_) => json!({ "code": "invalid-params" }),
            Self::NotFound => json!({ "code": "not-found" }),
            Self::NotLoopback => json!({ "code": "not-loopback" }),
            Self::ListenerDown => json!({ "code": "listener-down" }),
            Self::Internal(_) => return None,
        })
    }
}

impl std::fmt::Display for LocalPairingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message())
    }
}

impl std::error::Error for LocalPairingError {}

/// The host-side local pairing service. Locality, listener state and the
/// endpoint rules are the transport's job; this owns the code slot, the
/// credential table and the two events.
pub trait LocalPairingService: Send + Sync {
    /// Create a code, cancelling any active one (`replaced`).
    fn create_code(&self) -> BoxFuture<'_, Result<CreatedLocalCode, LocalPairingError>>;

    /// Cancel the active code (or only `code_id` when given). `false` when
    /// nothing matched.
    fn cancel_code(&self, code_id: Option<String>) -> BoxFuture<'_, bool>;

    /// Spend the active code and mint one credential. `code` and `app_name`
    /// are the raw request params; validation happens here in contract order.
    fn redeem(
        &self,
        code: String,
        app_name: String,
    ) -> BoxFuture<'_, Result<RedeemedLocalCode, LocalPairingError>>;

    /// Unrevoked credentials ordered by `createdAt`, then `credentialId`.
    fn list_credentials(
        &self,
    ) -> BoxFuture<'_, Result<Vec<LocalPairingCredential>, LocalPairingError>>;

    /// Revoke one credential and close its live connections. `false` when it
    /// was already revoked; [`LocalPairingError::NotFound`] when unknown.
    fn revoke_credential(
        &self,
        credential_id: String,
    ) -> BoxFuture<'_, Result<bool, LocalPairingError>>;

    /// Admission: resolve a presented token's hex SHA-256 to an unrevoked
    /// credential id, touching `lastUsedAt`.
    fn resolve_credential(&self, token_hash: String) -> BoxFuture<'_, Option<String>>;

    /// Revalidation: whether the credential is still unrevoked.
    fn credential_active(&self, credential_id: String) -> BoxFuture<'_, bool>;

    /// Ids of credentials as they are revoked; subscribe before admission.
    fn subscribe_revocations(&self) -> tokio::sync::broadcast::Receiver<String>;

    /// A `/ws` connection admitted with this credential opened.
    fn connection_opened(&self, credential_id: &str);

    /// A `/ws` connection admitted with this credential closed.
    fn connection_closed(&self, credential_id: &str);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_case_spaces_and_hyphens() {
        assert_eq!(normalize_code("ktp-82m").as_deref(), Some("KTP82M"));
        assert_eq!(normalize_code(" K T P 8 2 M ").as_deref(), Some("KTP82M"));
        assert_eq!(normalize_code("KTP--82M").as_deref(), Some("KTP82M"));
    }

    #[test]
    fn rejects_wrong_length_and_lookalikes() {
        assert_eq!(normalize_code("KTP-82"), None);
        assert_eq!(normalize_code("KTP-82MM"), None);
        assert_eq!(normalize_code("KTP-820"), None);
        assert_eq!(normalize_code("KTO-82M"), None);
        assert_eq!(normalize_code("KTI-82M"), None);
        assert_eq!(normalize_code("KTL-82M"), None);
        assert_eq!(normalize_code("KT1-82M"), None);
        assert_eq!(normalize_code("KTP_82M"), None);
        assert_eq!(normalize_code("KTPé2M"), None);
    }

    #[test]
    fn displays_two_groups() {
        assert_eq!(display_code("KTP82M"), "KTP-82M");
    }

    #[test]
    fn alphabet_has_no_lookalikes() {
        for c in b"01OIL" {
            assert!(!CODE_ALPHABET.contains(c));
        }
        assert_eq!(CODE_ALPHABET.len(), 31);
    }

    #[test]
    fn app_name_rules() {
        assert_eq!(
            validate_app_name("  Intent Alpha ").unwrap(),
            "Intent Alpha"
        );
        assert!(validate_app_name("   ").is_err());
        assert!(validate_app_name("a\u{7}b").is_err());
        assert!(validate_app_name(&"x".repeat(64)).is_ok());
        assert!(validate_app_name(&"x".repeat(65)).is_err());
        assert!(validate_app_name(&"é".repeat(64)).is_ok());
    }

    #[test]
    fn error_rendering_matches_contract() {
        let invalid = LocalPairingError::Invalid {
            attempts_remaining: 3,
        };
        assert_eq!(invalid.code(), -32602);
        assert_eq!(
            invalid.data().unwrap(),
            json!({"code": "local-code-invalid", "attemptsRemaining": 3})
        );
        assert_eq!(
            LocalPairingError::Locked.data().unwrap()["code"],
            "local-code-locked"
        );
        assert_eq!(
            LocalPairingError::Expired.data().unwrap()["code"],
            "local-code-expired"
        );
        assert_eq!(LocalPairingError::NotLoopback.code(), -32001);
        assert_eq!(LocalPairingError::ListenerDown.code(), -32603);
        assert_eq!(
            LocalPairingError::NotFound.data().unwrap()["code"],
            "not-found"
        );
        assert!(LocalPairingError::Internal("x".into()).data().is_none());
    }

    #[test]
    fn code_serializes_optional_fields_only_when_present() {
        let code = LocalPairingCode {
            code_id: "c1".into(),
            state: LocalPairingCodeState::Active,
            created_at: "t0".into(),
            expires_at: "t1".into(),
            attempts_remaining: 5,
            cancel_reason: None,
            credential_id: None,
            app_name: None,
        };
        assert_eq!(
            serde_json::to_value(&code).unwrap(),
            json!({"codeId":"c1","state":"active","createdAt":"t0","expiresAt":"t1","attemptsRemaining":5})
        );
    }
}
