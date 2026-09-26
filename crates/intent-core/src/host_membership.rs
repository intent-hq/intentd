//! Durable host membership and invitation domain values. Workspace grants
//! remain independent; the primary principal is always the host owner.

use serde::{Deserialize, Serialize};
use time::{format_description::well_known::Rfc3339, Duration};

use crate::{now_iso, parse_iso, Error, PrincipalId, PrincipalIdentity, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HostRole {
    Owner,
    Member,
    Guest,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InviteScope {
    #[default]
    Workspace,
    Host,
}

/// Stored scalars, updated in the same transaction as authority changes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HostMembershipState {
    pub revision: u64,
    /// Active non-primary members; the owner does not spend a member row.
    pub member_count: u64,
    /// Capture at proof challenge time; compare to the resolved principal's
    /// last revocation at commit, once its identity is known.
    pub authorization_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostMember {
    pub principal_id: PrincipalId,
    pub host_role: HostRole,
    pub login: Option<String>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<PrincipalIdentity>,
    pub added_at: String,
}

/// A pinned, single-use host invitation. As with `WorkspaceInvite`, the
/// service adds the fixed scope/role/reusable fields and optional rebuilt URL
/// to the public view. Secrets are never serialized as row fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostInvite {
    pub id: String,
    #[serde(skip_serializing)]
    pub secret_hash: String,
    #[serde(default, skip_serializing)]
    pub secret: Option<String>,
    pub created_by_principal_id: PrincipalId,
    pub pin_identity: PrincipalIdentity,
    pub pin_login: String,
    pub created_at: String,
    pub expires_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redeemed_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redeemed_by_principal_id: Option<PrincipalId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<String>,
    pub redemption_count: u64,
}

impl HostInvite {
    pub const LIFETIME_SECONDS: i64 = 7 * 24 * 60 * 60;

    /// Construct a seven-day invitation from an already resolved canonical
    /// pin. Provider verification and secret generation belong to services.
    ///
    /// # Errors
    /// Returns `InvalidInput` for an empty pin or secret hash.
    pub fn new(
        id: String,
        created_by_principal_id: PrincipalId,
        pin_identity: PrincipalIdentity,
        pin_login: String,
        secret_hash: String,
        secret: Option<String>,
    ) -> Result<Self> {
        let created_at = now_iso();
        let expires_at = (parse_iso(&created_at)
            .ok_or_else(|| Error::Internal("invalid clock timestamp".into()))?
            + Duration::seconds(Self::LIFETIME_SECONDS))
        .format(&Rfc3339)
        .map_err(|e| Error::Internal(format!("format host invite expiry failed: {e}")))?;
        let invite = Self {
            id,
            secret_hash,
            secret,
            created_by_principal_id,
            pin_identity,
            pin_login,
            created_at,
            expires_at,
            redeemed_at: None,
            redeemed_by_principal_id: None,
            revoked_at: None,
            redemption_count: 0,
        };
        invite.validate_new()?;
        Ok(invite)
    }

    /// Validate the insert boundary, including the fixed lifetime.
    ///
    /// # Errors
    /// Returns `InvalidInput` for malformed or already consumed invitations.
    pub fn validate_new(&self) -> Result<()> {
        let valid_lifetime = parse_iso(&self.created_at)
            .zip(parse_iso(&self.expires_at))
            .is_some_and(|(created, expires)| {
                expires - created == Duration::seconds(Self::LIFETIME_SECONDS)
            });
        if self.id.trim().is_empty()
            || self.secret_hash.trim().is_empty()
            || self.pin_login.trim().is_empty()
            || self.pin_identity.provider.trim().is_empty()
            || self.pin_identity.host.trim().is_empty()
            || self.pin_identity.external_user_id.trim().is_empty()
            || !valid_lifetime
            || self.redemption_count != 0
            || self.redeemed_at.is_some()
            || self.redeemed_by_principal_id.is_some()
            || self.revoked_at.is_some()
        {
            return Err(Error::InvalidInput("invalid new host invitation".into()));
        }
        Ok(())
    }

    #[must_use]
    pub fn is_open_at(&self, now: &str) -> bool {
        self.revoked_at.is_none()
            && self.redeemed_at.is_none()
            && self.redemption_count == 0
            && parse_iso(now)
                .zip(parse_iso(&self.expires_at))
                .is_some_and(|(now, expires)| now < expires)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roles_and_scope_keep_owner_member_guest_distinct() {
        for (role, wire) in [
            (HostRole::Owner, "owner"),
            (HostRole::Member, "member"),
            (HostRole::Guest, "guest"),
        ] {
            assert_eq!(serde_json::to_value(role).unwrap(), json!(wire));
            assert_eq!(
                serde_json::from_value::<HostRole>(json!(wire)).unwrap(),
                role
            );
        }
        assert_eq!(InviteScope::default(), InviteScope::Workspace);
        assert!(serde_json::from_value::<InviteScope>(json!("unknown")).is_err());
        assert!(serde_json::from_value::<HostRole>(json!("administrator")).is_err());
    }

    #[test]
    fn host_invite_has_exact_lifetime_and_never_serializes_secret_fields() {
        let mut invite = HostInvite::new(
            "invite".into(),
            PrincipalId::new(),
            PrincipalIdentity::github(42),
            "account".into(),
            "hash-value".into(),
            Some("secret-value".into()),
        )
        .unwrap();
        assert_eq!(
            parse_iso(&invite.expires_at).unwrap() - parse_iso(&invite.created_at).unwrap(),
            Duration::days(7)
        );
        assert!(invite.is_open_at(&invite.created_at));
        assert!(!invite.is_open_at(&invite.expires_at));
        let json = serde_json::to_value(&invite).unwrap();
        assert!(json.get("secretHash").is_none());
        assert!(json.get("secret").is_none());
        assert_eq!(json["pinIdentity"]["externalUserId"], "42");
        invite.revoked_at = Some(invite.created_at.clone());
        assert!(!invite.is_open_at(&invite.created_at));
        assert!(invite.validate_new().is_err());
        invite.revoked_at = None;
        invite.redeemed_at = Some(invite.created_at.clone());
        invite.redeemed_by_principal_id = Some(PrincipalId::new());
        invite.redemption_count = 1;
        assert!(!invite.is_open_at(&invite.created_at));
        assert!(invite.validate_new().is_err());
    }
}
