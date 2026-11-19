//! Wire types for the stdio JSON-lines protocol (monorepo spec "Sidecar protocol").
//! Binary fields are base64url without padding. Both sides ignore unknown fields.

use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use base64::engine::DecodePaddingMode;
use base64::{alphabet, Engine as _};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const PROTOCOL_VERSION: u32 = 1;

const B64: GeneralPurpose = GeneralPurpose::new(
    &alphabet::URL_SAFE,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// Bytes carried as base64url (no padding) on the wire.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Bytes(pub Vec<u8>);

impl Bytes {
    #[must_use]
    pub fn encode(&self) -> String {
        B64.encode(&self.0)
    }

    /// # Errors
    /// Fails when `text` is not base64url.
    pub fn decode(text: &str) -> Result<Self, base64::DecodeError> {
        B64.decode(text).map(Self)
    }
}

impl std::fmt::Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bytes({})", self.encode())
    }
}

impl Serialize for Bytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.encode())
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::decode(&text).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    Hybrid,
    Usb,
    Device,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpEntity {
    pub id: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserEntity {
    pub id: Bytes,
    pub name: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CredParam {
    #[serde(rename = "type")]
    pub kind: String,
    pub alg: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CredentialDescriptor {
    pub id: Bytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transports: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticatorSelection {
    pub resident_key: Option<String>,
    pub require_resident_key: Option<bool>,
    pub user_verification: Option<String>,
    pub authenticator_attachment: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateExtensions {
    pub cred_props: Option<bool>,
}

/// Shared by `create` and `get`: who asks, for which RP, and with which challenge.
pub trait CeremonyScope {
    fn transport(&self) -> Transport;
    fn origin(&self) -> &str;
    fn top_origin(&self) -> Option<&str>;
    fn cross_origin(&self) -> bool;
    fn rp_id(&self) -> &str;
    fn timeout_ms(&self) -> Option<u64>;
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateParams {
    pub transport: Transport,
    pub origin: String,
    pub top_origin: Option<String>,
    /// From main; v1 rejects `true` (no passkeys in cross-origin frames).
    #[serde(default)]
    pub cross_origin: bool,
    pub rp_id: String,
    pub challenge: Bytes,
    pub rp: RpEntity,
    pub user: UserEntity,
    pub pub_key_cred_params: Vec<CredParam>,
    #[serde(default)]
    pub exclude_credentials: Vec<CredentialDescriptor>,
    #[serde(default)]
    pub authenticator_selection: AuthenticatorSelection,
    pub attestation: Option<String>,
    #[serde(default)]
    pub extensions: CreateExtensions,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetParams {
    pub transport: Transport,
    pub origin: String,
    pub top_origin: Option<String>,
    /// From main; v1 rejects `true` (no passkeys in cross-origin frames).
    #[serde(default)]
    pub cross_origin: bool,
    pub rp_id: String,
    pub challenge: Bytes,
    #[serde(default)]
    pub allow_credentials: Vec<CredentialDescriptor>,
    pub user_verification: Option<String>,
    pub timeout_ms: Option<u64>,
}

macro_rules! impl_scope {
    ($ty:ty) => {
        impl CeremonyScope for $ty {
            fn transport(&self) -> Transport {
                self.transport
            }
            fn origin(&self) -> &str {
                &self.origin
            }
            fn top_origin(&self) -> Option<&str> {
                self.top_origin.as_deref()
            }
            fn cross_origin(&self) -> bool {
                self.cross_origin
            }
            fn rp_id(&self) -> &str {
                &self.rp_id
            }
            fn timeout_ms(&self) -> Option<u64> {
                self.timeout_ms
            }
        }
    };
}
impl_scope!(CreateParams);
impl_scope!(GetParams);

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CredProps {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rk: Option<bool>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateExtensionOutputs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cred_props: Option<CredProps>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateResult {
    pub credential_id: Bytes,
    #[serde(rename = "clientDataJSON")]
    pub client_data_json: Bytes,
    pub attestation_object: Bytes,
    pub authenticator_data: Bytes,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_key: Option<Bytes>,
    pub public_key_algorithm: i64,
    pub transports: Vec<String>,
    pub authenticator_attachment: String,
    pub extensions: CreateExtensionOutputs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetResult {
    pub credential_id: Bytes,
    #[serde(rename = "clientDataJSON")]
    pub client_data_json: Bytes,
    pub authenticator_data: Bytes,
    pub signature: Bytes,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_handle: Option<Bytes>,
    pub authenticator_attachment: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BluetoothState {
    On,
    Off,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Transports {
    pub hybrid: bool,
    pub usb: bool,
    pub device: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HelloResult {
    pub protocol: u32,
    pub version: String,
    pub transports: Transports,
    pub bluetooth: BluetoothState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Status {
    WaitingForPhone,
    Connecting,
    WaitingForApproval,
    TouchKey,
    Processing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PinKind {
    Usb,
    Device,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Account {
    pub index: usize,
    pub user_name: String,
    pub user_display_name: String,
}

/// Sidecar → main notification; serialized with `event` and `ceremony` added.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "event", rename_all = "kebab-case")]
pub enum Event {
    Qr {
        data: String,
    },
    Status {
        state: Status,
    },
    #[serde(rename_all = "camelCase")]
    PinNeeded {
        kind: PinKind,
        #[serde(skip_serializing_if = "Option::is_none")]
        attempts_left: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
        wrong_pin: bool,
        /// Device only: the PIN changed, so this asks once for the previous PIN.
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        old_pin: bool,
    },
    ChooseAccount {
        accounts: Vec<Account>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ErrorCode {
    NotAllowed,
    InvalidState,
    NotSupported,
    Security,
    Abort,
    Timeout,
    PinLocked,
    BluetoothUnavailable,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, thiserror::Error)]
#[error("{code:?}: {message}")]
pub struct PasskeyError {
    pub code: ErrorCode,
    pub message: String,
}

impl PasskeyError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CeremonyRef {
    pub ceremony: String,
}

#[derive(Deserialize)]
pub struct PinParams {
    pub ceremony: String,
    pub pin: zeroize::Zeroizing<String>,
}

#[derive(Debug, Deserialize)]
pub struct ChooseParams {
    pub ceremony: String,
    pub index: usize,
}

/// One `device.list` entry. Times are milliseconds since the Unix epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceCredential {
    pub credential_id: Bytes,
    pub rp_id: String,
    pub user_name: String,
    pub user_display_name: String,
    pub created_at: u64,
    pub last_used_at: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceDeleteParams {
    pub credential_id: Bytes,
}
