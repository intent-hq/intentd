//! Node invites (monorepo `docs/protocol/methods/nodes.md`, "Node invites and
//! claim", and `docs/protocol/node-link.md`, "Node invite claim"): the
//! `intent://node` link and the one-exchange frames of `/node/claim`.
//!
//! The link carries routes, the certificate pin and the invite, never the
//! owner token. Its `secret` is the only authority of `/node/claim`, so
//! `Debug` never shows it.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::node_link::ProtocolRange;
use crate::placement::AgentPlatform;

/// URL path of the anonymous claim exchange on the machine's WSS listener.
pub const CLAIM_PATH: &str = "/node/claim";

/// The only `v` an `intent://node` link has.
pub const NODE_LINK_VERSION: u32 = 1;

/// Default and bounds of `executor.invite.create`'s `expiresInSecs`.
pub const DEFAULT_EXPIRES_SECS: u64 = 86_400;
pub const MIN_EXPIRES_SECS: u64 = 300;
pub const MAX_EXPIRES_SECS: u64 = 604_800;

/// Largest claim frame, either way.
pub const MAX_CLAIM_FRAME_BYTES: usize = 16 * 1024;

/// Refusal codes of the claim exchange; each is also the public
/// `data.code` of `remoteMachines.claim`, except [`codes::VERSION`].
pub mod codes {
    pub const INVALID_LINK: &str = "invalid-node-link";
    pub const INVALID: &str = "node-invite-invalid";
    pub const EXPIRED: &str = "node-invite-expired";
    pub const USED: &str = "node-invite-used";
    pub const REVOKED: &str = "node-invite-revoked";
    pub const SELF: &str = "node-invite-self";
    /// The two protocol ranges do not meet.
    pub const VERSION: &str = "node-link-version";
    pub const PROTOCOL: &str = "node-link-protocol";
}

/// A parsed `intent://node` link.
#[derive(Clone, PartialEq, Eq)]
pub struct NodeInviteLink {
    pub hosts: Vec<String>,
    pub port: u16,
    pub fingerprint: String,
    pub invite_id: String,
    pub secret: String,
    pub tc_address: Option<String>,
}

impl std::fmt::Debug for NodeInviteLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeInviteLink")
            .field("hosts", &self.hosts)
            .field("port", &self.port)
            .field("fingerprint", &self.fingerprint)
            .field("invite_id", &self.invite_id)
            .field("tc_address", &self.tc_address)
            .finish_non_exhaustive()
    }
}

impl NodeInviteLink {
    /// `intent://node?v=1&host=<ip[,ip...]>&port=<p>&fp=<sha256>&invite=<id>&secret=<s>[&tc=<addr>]`,
    /// with the query encoding of `intent://pair`.
    #[must_use]
    pub fn to_uri(&self) -> String {
        let hosts = self
            .hosts
            .iter()
            .map(|host| encode(host))
            .collect::<Vec<_>>()
            .join(",");
        let mut uri = format!(
            "intent://node?v={NODE_LINK_VERSION}&host={hosts}&port={}&fp={}&invite={}&secret={}",
            self.port,
            encode(&self.fingerprint),
            encode(&self.invite_id),
            encode(&self.secret),
        );
        if let Some(tc) = &self.tc_address {
            let _ = write!(uri, "&tc={}", encode(tc));
        }
        uri
    }

    /// Parse a link; `None` unless it is a v1 `intent://node` link with a
    /// port, a pin, an invite, a secret and at least one route.
    #[must_use]
    pub fn parse(uri: &str) -> Option<Self> {
        let query = uri.trim().strip_prefix("intent://node?")?;
        let mut version = None;
        let mut hosts = None;
        let mut port = None;
        let mut fingerprint = None;
        let mut invite_id = None;
        let mut secret = None;
        let mut tc_address = None;
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=')?;
            let slot = match key {
                "v" => &mut version,
                "host" => &mut hosts,
                "port" => &mut port,
                "fp" => &mut fingerprint,
                "invite" => &mut invite_id,
                "secret" => &mut secret,
                "tc" => &mut tc_address,
                _ => continue,
            };
            if slot.replace(value.to_owned()).is_some() {
                return None;
            }
        }
        if version?.parse::<u32>().ok()? != NODE_LINK_VERSION {
            return None;
        }
        let hosts = hosts
            .unwrap_or_default()
            .split(',')
            .filter(|host| !host.is_empty())
            .map(decode)
            .collect::<Option<Vec<_>>>()?;
        let port = port?.parse::<u16>().ok().filter(|port| *port != 0)?;
        let nonempty = |value: Option<String>| {
            value
                .as_deref()
                .and_then(decode)
                .filter(|value| !value.is_empty())
        };
        let fingerprint = nonempty(fingerprint)?;
        let invite_id = nonempty(invite_id)?;
        let secret = nonempty(secret)?;
        let tc_address = match tc_address {
            None => None,
            Some(tc) => Some(decode(&tc).filter(|tc| !tc.is_empty())?),
        };
        if hosts.is_empty() && tc_address.is_none() {
            return None;
        }
        Some(Self {
            hosts,
            port,
            fingerprint,
            invite_id,
            secret,
            tc_address,
        })
    }
}

fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b':' => {
                out.push(char::from(byte));
            }
            _ => {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
}

fn decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = std::str::from_utf8(bytes.get(index + 1..index + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Backend → machine: the one frame of a claim.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClaimFrame {
    pub invite_id: String,
    pub secret: String,
    pub backend_id: String,
    pub backend_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_fingerprint: Option<String>,
    pub protocol: ProtocolRange,
}

impl ClaimFrame {
    /// The frame's JSON text, with its `"type": "claim"` tag.
    ///
    /// # Errors
    ///
    /// When serialization fails.
    pub fn encode(&self) -> serde_json::Result<String> {
        let mut value = serde_json::to_value(self)?;
        if let Some(object) = value.as_object_mut() {
            object.insert("type".into(), serde_json::Value::from("claim"));
        }
        serde_json::to_string(&value)
    }

    /// Parse a claim frame; `None` unless it is exactly a `claim` with the
    /// documented members.
    #[must_use]
    pub fn decode(text: &str) -> Option<Self> {
        let mut value: serde_json::Value = serde_json::from_str(text).ok()?;
        let object = value.as_object_mut()?;
        if object.remove("type")? != "claim" {
            return None;
        }
        serde_json::from_value(value).ok()
    }
}

impl std::fmt::Debug for ClaimFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClaimFrame")
            .field("invite_id", &self.invite_id)
            .field("backend_id", &self.backend_id)
            .field("protocol", &self.protocol)
            .finish_non_exhaustive()
    }
}

/// The machine fields of a successful claim, as `executor.mint` returns them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClaimedMachine {
    pub daemon_id: String,
    pub name: String,
    pub platform: AgentPlatform,
    pub port: u16,
    pub hosts: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tc_address: Option<String>,
    pub fingerprint: String,
}

/// Machine → backend: the one reply of a claim.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum ClaimReply {
    #[serde(rename_all = "camelCase")]
    Claimed {
        credential_id: String,
        token: String,
        machine: ClaimedMachine,
        machine_version: String,
        protocol: ProtocolRange,
    },
    #[serde(rename_all = "camelCase")]
    ClaimRefused {
        code: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_at: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        machine_version: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        protocol: Option<ProtocolRange>,
    },
}

impl ClaimReply {
    /// A refusal with only its code.
    #[must_use]
    pub fn refused(code: &str) -> Self {
        Self::ClaimRefused {
            code: code.to_owned(),
            expires_at: None,
            machine_version: None,
            protocol: None,
        }
    }
}

impl std::fmt::Debug for ClaimReply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Claimed {
                credential_id,
                machine,
                ..
            } => f
                .debug_struct("Claimed")
                .field("credential_id", credential_id)
                .field("machine", machine)
                .finish_non_exhaustive(),
            Self::ClaimRefused { code, .. } => f
                .debug_struct("ClaimRefused")
                .field("code", code)
                .finish_non_exhaustive(),
        }
    }
}

#[cfg(test)]
mod tests;
