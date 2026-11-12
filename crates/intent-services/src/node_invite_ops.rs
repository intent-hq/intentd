//! Machine side of node invites (monorepo `docs/protocol/methods/nodes.md`,
//! "Node invites and claim"): owner-only `executor.invite.create` and
//! `executor.revoke { inviteId }`, and the anonymous `/node/claim` decision.
//!
//! A claim mints an ordinary executor credential, exactly as
//! `executor.mint` does, in the transaction that spends the invite. The
//! secret and the minted token are returned once and never logged.

use intent_core::machines::ExecutorCredential;
use intent_core::node_invite::{
    codes, ClaimFrame, ClaimReply, ClaimedMachine, NodeInviteLink, DEFAULT_EXPIRES_SECS,
    MAX_EXPIRES_SECS, MIN_EXPIRES_SECS,
};
use intent_core::node_link::NODE_LINK_PROTOCOLS;
use intent_core::placement::AgentPlatform;
use intent_core::{iso_ms_from_now, now_iso, Error, ExecutorCredentialId, Result};
use intent_store::{NodeInvite, NodeInviteRefusal, NodeInviteRevocation};
use serde_json::{json, Value};

use crate::executor_ops::{random_token, text_param};
use crate::Services;

/// Longest backend id, name and fingerprint in a claim, in UTF-8 bytes.
const MAX_CLAIM_TEXT_BYTES: usize = 128;

/// The `NodeInvite` wire row.
pub(crate) fn invite_json(invite: &NodeInvite) -> Value {
    let mut row = json!({
        "inviteId": invite.id,
        "createdAt": invite.created_at,
        "expiresAt": invite.expires_at,
    });
    if let Some(label) = &invite.label {
        row["label"] = json!(label);
    }
    row
}

fn expires_in_secs(params: &Value) -> Result<u64> {
    match params.get("expiresInSecs") {
        None | Some(Value::Null) => Ok(DEFAULT_EXPIRES_SECS),
        Some(value) => value
            .as_u64()
            .filter(|secs| (MIN_EXPIRES_SECS..=MAX_EXPIRES_SECS).contains(secs))
            .ok_or_else(|| {
                Error::InvalidParams(format!(
                    "expiresInSecs must be an integer from {MIN_EXPIRES_SECS} to {MAX_EXPIRES_SECS}"
                ))
            }),
    }
}

fn link_host(params: &Value) -> Result<Option<String>> {
    let Some(host) = text_param(params, "host", false)? else {
        return Ok(None);
    };
    let host = host.trim();
    if host.contains(['/', '?', '#', '@', ' ', ',', '&']) || host.chars().any(char::is_control) {
        return Err(Error::InvalidParams(
            "host must be an IP address or a host name".into(),
        ));
    }
    Ok(Some(host.to_owned()))
}

fn claim_text_ok(text: &str) -> bool {
    !text.trim().is_empty()
        && text.len() <= MAX_CLAIM_TEXT_BYTES
        && !text.chars().any(char::is_control)
}

impl Services {
    /// `executor.invite.create`: see
    /// [`intent_core::WorkspaceApi::executor_invite_create`].
    pub(crate) async fn executor_invite_create_op(&self, params: Value) -> Result<Value> {
        self.require_machine_owner("executor.invite.create").await?;
        self.refuse_in_orchestrator_mode("executor.invite.create")?;
        let label = text_param(&params, "label", false)?.map(str::to_owned);
        let secs = expires_in_secs(&params)?;
        let host = link_host(&params)?;
        let routes = self
            .machine_routes
            .get()
            .ok_or_else(|| Error::Unsupported("this daemon has no executor listener".into()))?
            .machine_routes()
            .await?;
        let hosts = host.map_or_else(|| routes.hosts.clone(), |host| vec![host]);
        if hosts.is_empty() && routes.tc_address.is_none() {
            return Err(Error::remote_machine(
                true,
                "This machine has no address another backend can dial: turn on the Tailcat \
                 tunnel or listen on a network interface, or name a host.",
                json!({ "code": "no-route" }),
            ));
        }
        let secret = random_token()?;
        let invite = NodeInvite {
            id: format!("ni-{}", uuid::Uuid::new_v4()),
            label,
            created_at: now_iso(),
            expires_at: iso_ms_from_now(secs.saturating_mul(1000)),
            used_at: None,
            credential_id: None,
            revoked_at: None,
        };
        self.store
            .insert_node_invite(&invite, &crate::invite_ops::hash_secret(&secret))
            .await?;
        let uri = NodeInviteLink {
            hosts: hosts.clone(),
            port: routes.port,
            fingerprint: routes.fingerprint.clone(),
            invite_id: invite.id.clone(),
            secret: secret.clone(),
            tc_address: routes.tc_address.clone(),
        }
        .to_uri();
        let mut result = json!({
            "invite": invite_json(&invite),
            "secret": secret,
            "uri": uri,
            "hosts": hosts,
            "port": routes.port,
            "fingerprint": routes.fingerprint,
        });
        if let Some(tc_address) = routes.tc_address {
            result["tcAddress"] = json!(tc_address);
        }
        Ok(result)
    }

    /// `executor.revoke { inviteId }`: see
    /// [`intent_core::WorkspaceApi::executor_invite_revoke`].
    pub(crate) async fn executor_invite_revoke_op(&self, invite_id: &str) -> Result<Value> {
        self.require_machine_owner("executor.revoke").await?;
        if invite_id.is_empty() {
            return Err(Error::InvalidParams("inviteId is required".into()));
        }
        match self.store.revoke_node_invite(invite_id).await? {
            NodeInviteRevocation::NotFound => {
                Err(Error::NotFound(format!("node invite {invite_id}")))
            }
            NodeInviteRevocation::Revoked | NodeInviteRevocation::Closed => {
                Ok(json!({ "ok": true }))
            }
        }
    }

    /// Decide one `/node/claim` exchange: see
    /// [`intent_core::WorkspaceApi::claim_node_invite`].
    pub(crate) async fn claim_node_invite_op(&self, frame: ClaimFrame) -> Result<ClaimReply> {
        let fingerprint_ok = frame
            .backend_fingerprint
            .as_deref()
            .is_none_or(claim_text_ok);
        if !claim_text_ok(&frame.backend_id)
            || !claim_text_ok(&frame.backend_name)
            || !fingerprint_ok
            || frame.invite_id.is_empty()
            || frame.secret.is_empty()
        {
            return Ok(ClaimReply::refused(codes::PROTOCOL));
        }
        let Some(platform) = AgentPlatform::host() else {
            return Ok(ClaimReply::refused(codes::VERSION));
        };
        let routes = self
            .machine_routes
            .get()
            .ok_or_else(|| Error::Unsupported("this daemon has no executor listener".into()))?
            .machine_routes()
            .await?;
        let daemon_id = self.store.daemon_id().await?;
        let late = if frame.backend_id == daemon_id {
            Some(codes::SELF)
        } else if NODE_LINK_PROTOCOLS.highest_common(frame.protocol).is_none() {
            Some(codes::VERSION)
        } else {
            None
        };
        let token = random_token()?;
        let credential = ExecutorCredential {
            id: ExecutorCredentialId::new(),
            backend_id: frame.backend_id.clone(),
            backend_name: frame.backend_name.clone(),
            backend_fingerprint: frame.backend_fingerprint.clone(),
            created_at: now_iso(),
            last_used_at: None,
            revoked_at: None,
        };
        let claimed = self
            .store
            .claim_node_invite(
                &frame.invite_id,
                &crate::invite_ops::hash_secret(&frame.secret),
                &frame.backend_id,
                &credential,
                &crate::invite_ops::hash_secret(&token),
                late,
            )
            .await?;
        let machine_version = env!("CARGO_PKG_VERSION").to_owned();
        Ok(match claimed {
            Ok(()) => ClaimReply::Claimed {
                credential_id: credential.id.as_str().to_owned(),
                token,
                machine: ClaimedMachine {
                    daemon_id,
                    name: routes.name,
                    platform,
                    port: routes.port,
                    hosts: routes.hosts,
                    tc_address: routes.tc_address,
                    fingerprint: routes.fingerprint,
                },
                machine_version,
                protocol: NODE_LINK_PROTOCOLS,
            },
            Err(NodeInviteRefusal::Invalid) => ClaimReply::refused(codes::INVALID),
            Err(NodeInviteRefusal::Revoked) => ClaimReply::refused(codes::REVOKED),
            Err(NodeInviteRefusal::Used) => ClaimReply::refused(codes::USED),
            Err(NodeInviteRefusal::Expired { expires_at }) => ClaimReply::ClaimRefused {
                code: codes::EXPIRED.to_owned(),
                expires_at: Some(expires_at),
                machine_version: None,
                protocol: None,
            },
            Err(NodeInviteRefusal::Late(code)) if code == codes::VERSION => {
                ClaimReply::ClaimRefused {
                    code: code.to_owned(),
                    expires_at: None,
                    machine_version: Some(machine_version),
                    protocol: Some(NODE_LINK_PROTOCOLS),
                }
            }
            Err(NodeInviteRefusal::Late(code)) => ClaimReply::refused(code),
        })
    }
}
