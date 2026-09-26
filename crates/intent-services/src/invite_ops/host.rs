//! Owner-only host invitations reuse the ordinary proof and credential admission.
use super::{
    closed_kind, hash_secret, now_iso, random_hex_secret, Error, HostInvite, HostInviteJoinOutcome,
    HostJoinCredential, HostRole, InviteErrorKind, InviteLinkEnvelope, InvitePin, InviteScope,
    Principal, PrincipalIdentity, Provider, Result, Services, WorkspaceId, WorkspaceInvite,
};
use serde_json::{json, Value};

pub(super) enum ScopedInvite {
    Workspace(WorkspaceInvite),
    Host(HostInvite),
}

impl ScopedInvite {
    pub(super) fn id(&self) -> &str {
        match self {
            Self::Workspace(i) => &i.id,
            Self::Host(i) => &i.id,
        }
    }

    pub(super) fn secret_hash(&self) -> &str {
        match self {
            Self::Workspace(i) => &i.secret_hash,
            Self::Host(i) => &i.secret_hash,
        }
    }

    pub(super) fn pin_identity_key(&self) -> Option<PrincipalIdentity> {
        match self {
            Self::Workspace(i) => i.pin_identity_key(),
            Self::Host(i) => Some(i.pin_identity.clone()),
        }
    }

    pub(super) fn check_open(&self, requested: InviteScope) -> Result<()> {
        let (scope, closed) = match self {
            Self::Workspace(i) => (InviteScope::Workspace, closed_kind(i, &now_iso())),
            Self::Host(i) => (
                InviteScope::Host,
                if i.revoked_at.is_some() {
                    Some(InviteErrorKind::Revoked)
                } else if i.redeemed_at.is_some() {
                    Some(InviteErrorKind::Redeemed)
                } else if !i.is_open_at(&now_iso()) {
                    Some(InviteErrorKind::Expired)
                } else {
                    None
                },
            ),
        };
        if let Some(kind) = closed {
            return Err(Error::Invite(kind));
        }
        if scope != requested {
            return Err(Error::Invite(InviteErrorKind::ScopeMismatch));
        }
        Ok(())
    }
}

fn host_invite_to_wire(invite: &HostInvite, envelope: Option<&dyn InviteLinkEnvelope>) -> Value {
    let mut row = json!(invite);
    row["scope"] = json!("host");
    row["role"] = json!("member");
    row["reusable"] = json!(false);
    if let Some(id) = invite.pin_identity.github_user_id() {
        row["pinGithubUserId"] = json!(id);
    }
    if let (Some(envelope), Some(secret)) = (envelope, invite.secret.as_deref()) {
        row["url"] = json!(envelope.scoped_invite_url(&invite.id, secret, InviteScope::Host));
    }
    row
}

impl Services {
    pub(crate) async fn host_members_list_op(&self) -> Result<Value> {
        Self::require_administrator("host.members.list")?;
        let snapshot = self.store.list_host_members().await?;
        Ok(json!({"members":snapshot.members, "revision":snapshot.revision}))
    }

    /// The transport resolves listener/tunnel availability before this write.
    pub(crate) async fn host_invite_create_op(&self, mut pin: InvitePin) -> Result<Value> {
        Self::require_administrator("host.invite.create")?;
        let provider = pin
            .provider
            .as_deref()
            .ok_or_else(|| Error::InvalidParams("pinProvider is required".into()))?;
        let provider = Provider::parse(provider)?;
        if pin.login.trim().is_empty() {
            return Err(Error::InvalidParams("pinLogin must not be blank".into()));
        }
        if pin.host.is_none() && provider == Provider::Gitlab {
            pin.host = Some("gitlab.com".into());
        }
        let creator = self.inviting_principal().await?;
        let (identity, login) = self.resolve_pin(None, pin).await?;
        let secret = random_hex_secret();
        let invite = HostInvite::new(
            uuid::Uuid::new_v4().to_string(),
            creator.id.clone(),
            identity,
            login,
            hash_secret(&secret),
            Some(secret.clone()),
        )?;
        {
            let _transition = self.identity_transition.lock().await;
            if self.store.get_principal(&creator.id).await?.identity_key() != creator.identity_key()
            {
                return Err(Error::Internal(
                    "the inviting identity changed while minting; retry".into(),
                ));
            }
            self.store.insert_host_invite(&invite).await?;
        }
        self.host_invite_event(&invite.id, "created").await;
        Ok(json!({"invite":host_invite_to_wire(&invite, None), "secret":secret}))
    }

    pub(crate) async fn host_invite_list_op(&self) -> Result<Value> {
        Self::require_administrator("host.invite.list")?;
        let rows = self.store.list_open_host_invites().await?;
        let envelope = self.invite_link_envelope().await;
        Ok(
            json!({"invites":rows.iter().map(|i| host_invite_to_wire(i, envelope.as_deref())).collect::<Vec<_>>()}),
        )
    }

    pub(crate) async fn host_invite_revoke_op(&self, invite_id: &str) -> Result<Value> {
        Self::require_administrator("host.invite.revoke")?;
        let revoked = self.store.revoke_host_invite(invite_id).await?;
        if revoked {
            self.host_invite_event(invite_id, "revoked").await;
        }
        Ok(json!({"revoked":revoked}))
    }

    async fn host_invite_event(&self, invite_id: &str, action: &str) {
        self.host_membership_event(
            intent_core::events::HOST_INVITES_CHANGED,
            json!({"inviteId":invite_id,"action":action}),
        )
        .await;
    }

    async fn host_membership_event(&self, event_type: &str, data: Value) {
        crate::publish_event(
            self.event_bus.as_ref(),
            intent_store::NewEvent {
                workspace_id: WorkspaceId::from_string(String::new()),
                timestamp: now_iso(),
                event_type: event_type.to_string(),
                actor: crate::system_actor(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data,
            },
        )
        .await;
    }

    pub(super) async fn commit_host_invite_join(
        &self,
        invite: &HostInvite,
        identity: &Principal,
        credential: HostJoinCredential<'_>,
        token: &str,
    ) -> Result<Value> {
        let (principal, added, revision) = match self
            .store
            .join_host_by_invite(&invite.id, identity, credential)
            .await?
        {
            HostInviteJoinOutcome::Joined {
                principal,
                membership_added,
                revision,
            } => (principal, membership_added, revision),
            outcome => {
                return Err(Error::Invite(match outcome {
                    HostInviteJoinOutcome::NotFound => InviteErrorKind::NotFound,
                    HostInviteJoinOutcome::Redeemed => InviteErrorKind::Redeemed,
                    HostInviteJoinOutcome::Revoked => InviteErrorKind::Revoked,
                    HostInviteJoinOutcome::Expired => InviteErrorKind::Expired,
                    HostInviteJoinOutcome::PinMismatch => InviteErrorKind::PinMismatch,
                    HostInviteJoinOutcome::OwnerSelfJoin => InviteErrorKind::OwnerSelfJoin,
                    HostInviteJoinOutcome::CredentialInvalid => InviteErrorKind::CredentialInvalid,
                    HostInviteJoinOutcome::AccessRevoked => InviteErrorKind::AccessRevoked,
                    HostInviteJoinOutcome::Joined { .. } => unreachable!(),
                }))
            }
        };
        self.presence_profile_changed(&principal).await;
        if added {
            self.host_membership_event(intent_core::events::HOST_MEMBERS_CHANGED,
                json!({"revision":revision,"principalId":principal.id,"hostRole":"member","action":"added"})).await;
        }
        self.host_invite_event(&invite.id, "redeemed").await;
        Ok(
            json!({"status":"authorized","token":token,"principalId":principal.id,"login":principal.login,
            "identity":principal.identity_key(),"hostRole":HostRole::Member,"scope":"host"}),
        )
    }
}
