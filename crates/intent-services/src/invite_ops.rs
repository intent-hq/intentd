//! Workspace invite links and the gist identity-proof join (multiplayer
//! w4): `workspace.invite.create` / `.list` / `.revoke` on the owner side,
//! `invite.inspect`, `invite.challenge` / `invite.prove` and
//! `invite.accept` on the unauthenticated `/invite` side, plus
//! `workspace.members.leave` / `principal.revokeSelf`.
//!
//! An invite is an expiring `(id, secret)` pair; a join matches the hex
//! SHA-256 of the secret, and the plaintext is kept only so the owner can
//! copy the link again (`invites[].url` on `.list`, built through the
//! transport's [`intent_core::InviteLinkBuilder`]; the secret itself never
//! serialises). An unpinned invite is **reusable**: any number of distinct
//! GitHub accounts may redeem it until it expires or is revoked (each join
//! is one more collaborator, a member re-joining is idempotent); a pinned
//! invite is single-use and closes on its redemption. The daemon learns
//! *who* the invitee is (stable `github_user_id`) and nothing else. The
//! joined principal is minted (or reused, keyed by `github_user_id`), added
//! as a `collaborator`, and issued a fresh per-principal credential that is
//! returned exactly once.
//!
//! A first-time guest proves its identity from its **own** daemon (gist
//! identity proof): `invite.challenge` issues a single-use nonce bound to
//! the invite ([`NONCE_TTL`]); the guest publishes it in a secret gist with
//! its own token and `invite.prove` reads the gist back (`GET /gists/{id}`
//! with the host's stored token, anonymously otherwise), checks owner /
//! content / creation time, resolves the account (`GET /users/{login}`) and
//! commits the join. The host never issues a device code and never sees the
//! guest's token.
//!
//! A guest that already holds such a credential for this host skips the
//! proof on later invites: `invite.inspect` previews the link (same
//! validation, no nonce) and `invite.accept` joins with the credential as
//! proof of identity — the principal it resolves to is the one whose GitHub
//! account was proven earlier, so the join commits with the stored identity
//! and GitHub is never contacted. The presented credential is revoked in
//! the same transaction that mints the new one.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use intent_core::{
    current_caller, iso_ms_from_now, now_iso, Caller, Error, InviteErrorKind, InviteLinkEnvelope,
    Principal, PrincipalId, Result, Workspace, WorkspaceId, WorkspaceInvite, WorkspaceRole,
};
use intent_sourcecontrol::identity_proof::ProofGistView;
use intent_sourcecontrol::{SourceControl, UserIdentity};
use intent_store::InviteJoinOutcome;
use serde_json::{json, Value};
use tokio::sync::{broadcast, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::{github_auth_ops, pr_ops, Services};

/// Default invite lifetime (7 days) when `expiresInSecs` is omitted.
pub(crate) const DEFAULT_INVITE_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Longest lifetime a client may request (30 days).
pub(crate) const MAX_INVITE_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// Env override for the GitHub API base the identity reads (`invite.prove`'s
/// gist / account lookups, the primary's `GET /user` refresh) talk to — the
/// spawned-daemon test seam (e2e points it at a local mock). Honored under
/// the same loopback-or-https rule as the login host.
pub(crate) const API_BASE_URI_ENV: &str = "INTENTD_GITHUB_API_BASE_URI";

/// Lifetime of an `invite.challenge` nonce: the guest has this long to
/// publish the gist and call `invite.prove`.
pub(crate) const NONCE_TTL: Duration = Duration::from_secs(10 * 60);

/// Outstanding (issued, not yet consumed or expired) nonces one invite may
/// have at once: a retry or two per invitee is legitimate; an anonymous
/// peer holding one valid link cannot grow the store past this.
pub(crate) const MAX_NONCES_PER_INVITE: usize = 8;

/// Outstanding nonces daemon-wide, enforced as a semaphore whose permit
/// lives in the nonce's slot, so the store is bounded however many links
/// are open.
pub(crate) const MAX_OUTSTANDING_NONCES: usize = 256;

/// One issued `invite.challenge` nonce awaiting its `invite.prove`.
pub(crate) struct NonceSlot {
    invite_id: String,
    /// Wall-clock issue time — the proof gist must not predate it (GitHub
    /// reports `created_at` at second precision, so the comparison floors).
    issued_at: SystemTime,
    expires_at: Instant,
    /// The [`MAX_OUTSTANDING_NONCES`] permit; released with the slot.
    _permit: OwnedSemaphorePermit,
}

/// Issued nonces keyed by nonce.
pub(crate) type InviteNonceState = Arc<tokio::sync::Mutex<HashMap<String, NonceSlot>>>;

/// Admission permits for nonces ([`MAX_OUTSTANDING_NONCES`]).
pub(crate) type InviteNoncePermits = Arc<Semaphore>;

pub(crate) fn new_nonce_permits() -> InviteNoncePermits {
    Arc::new(Semaphore::new(MAX_OUTSTANDING_NONCES))
}

/// 32 random bytes (two `UUIDv4`s, OS randomness) as unpadded base64url —
/// the `invite.challenge` nonce, safe on a gist line and in a URL.
pub(crate) fn random_nonce() -> String {
    use base64::Engine as _;
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Drop nonces whose lifetime passed, returning their permits.
fn purge_nonces(nonces: &mut HashMap<String, NonceSlot>, now: Instant) {
    nonces.retain(|_, slot| slot.expires_at > now);
}

/// Whether a proof gist created at `created_at` (RFC 3339, as GitHub reports
/// it) is no older than the nonce issued at `issued_at`. Unparseable input
/// never passes.
fn gist_created_after(created_at: &str, issued_at: SystemTime) -> bool {
    let Ok(created) = chrono::DateTime::parse_from_rfc3339(created_at) else {
        return false;
    };
    let Ok(issued) = issued_at.duration_since(SystemTime::UNIX_EPOCH) else {
        return false;
    };
    created.timestamp() >= i64::try_from(issued.as_secs()).unwrap_or(i64::MAX)
}

/// The `invite.prove` verdict on a gist: owned by the claimed `login`
/// (case-insensitively), its proof file starts with the nonce, and it was
/// created no earlier than the nonce was issued.
fn proof_matches(gist: &ProofGistView, login: &str, nonce: &str, issued_at: SystemTime) -> bool {
    // repo-slug-fold: allow — a GitHub user login (case-insensitive on GitHub), not a repo slug
    gist.owner_login.eq_ignore_ascii_case(login)
        && gist.proof_first_line.as_deref() == Some(nonce)
        && gist_created_after(&gist.created_at, issued_at)
}

/// A GitHub login as `invite.prove` may claim it: 1–39 ASCII alphanumerics
/// or hyphens (GitHub's own rule), so it is safe in a request path.
fn valid_login(login: &str) -> bool {
    !login.is_empty()
        && login.len() <= 39
        && login.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

/// Live feed of principal ids whose credentials were just revoked; the
/// transport closes the connections still bound to them.
pub(crate) type PrincipalRevocations = broadcast::Sender<PrincipalId>;

/// Broadcast capacity: revocations are rare and a lagging listener only
/// misses closes for connections that fail on their next RPC anyway.
pub(crate) const REVOCATION_CHANNEL_CAPACITY: usize = 64;

/// 64 lowercase hex chars from two `UUIDv4`s (OS randomness, 244 random bits):
/// the invite secret and the minted credential share this shape with the
/// legacy file token so the transport's hashing treats them alike.
pub(crate) fn random_hex_secret() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Hex SHA-256 of a secret — the only form the store ever sees.
pub(crate) fn hash_secret(secret: &str) -> String {
    use sha2::Digest as _;
    use std::fmt::Write as _;
    sha2::Sha256::digest(secret.as_bytes()).iter().fold(
        String::with_capacity(64),
        |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        },
    )
}

/// The invite's wire shape (`secret` / `secretHash` never included), plus
/// `reusable` (derived from the pin, see [`WorkspaceInvite::is_reusable`])
/// and `url` when the row still holds its secret and a link envelope
/// resolved.
pub(crate) fn invite_to_wire(
    invite: &WorkspaceInvite,
    envelope: Option<&dyn InviteLinkEnvelope>,
) -> Value {
    let mut wire = serde_json::to_value(invite).unwrap_or_else(|_| json!({ "id": invite.id }));
    if let Some(obj) = wire.as_object_mut() {
        obj.insert("reusable".into(), invite.is_reusable().into());
        if let (Some(env), Some(secret)) = (envelope, invite.secret.as_deref()) {
            obj.insert("url".into(), env.invite_url(&invite.id, secret).into());
        }
    }
    wire
}

/// RFC-3339 UTC timestamp `secs` seconds from now (same formatter as every
/// other timestamp column, so lexical comparison stays valid).
fn iso_after(secs: u64) -> String {
    iso_ms_from_now(secs.saturating_mul(1000))
}

/// Map a resolved GitHub account onto a principal row (fresh or existing).
fn apply_identity(p: &mut Principal, user: &UserIdentity) {
    p.github_user_id = user.id.and_then(|id| i64::try_from(id).ok());
    p.login = Some(user.login.clone());
    p.display_name.clone_from(&user.name);
    p.avatar_url.clone_from(&user.avatar_url);
    p.updated_at = now_iso();
}

/// Constant-time equality of two hex digests (both sides are hashes, so the
/// lengths are public).
fn hashes_match(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// The terminal [`InviteErrorKind`] for an invite that is not open at `now`
/// (`None` when it is open). `Redeemed` is reachable only for a pinned,
/// single-use invite: a reusable one is never closed by its redemptions.
fn closed_kind(invite: &WorkspaceInvite, now: &str) -> Option<InviteErrorKind> {
    if invite.revoked_at.is_some() {
        Some(InviteErrorKind::Revoked)
    } else if !invite.is_reusable() && invite.redeemed_at.is_some() {
        Some(InviteErrorKind::Redeemed)
    } else if invite.expires_at.as_str() <= now {
        Some(InviteErrorKind::Expired)
    } else {
        None
    }
}

impl Services {
    /// The bound non-administrator wire principal, for the self-directed
    /// ops (`members.leave`, `principal.revokeSelf`). Administrators, agents
    /// and the daemon resolve to the primary principal.
    async fn self_principal(&self) -> Result<(PrincipalId, bool)> {
        match current_caller() {
            None => Err(crate::principal_ops::no_caller()),
            Some(Caller::Wire {
                principal_id,
                is_administrator,
            }) => Ok((principal_id, is_administrator)),
            Some(Caller::Agent { .. } | Caller::Daemon) => {
                Ok((self.store.get_primary_principal().await?.id, true))
            }
        }
    }

    /// The principal an invite is minted by: the bound caller (the owner,
    /// or the administrator acting as the primary). Ensures it carries a
    /// GitHub identity, else [`InviteErrorKind::GithubIdentityRequired`].
    ///
    /// For the primary that identity is only as good as the daemon's GitHub
    /// auth *right now*: a `github.revoke` leaves the cached `github_user_id`
    /// on the row, so the cache alone does not qualify — `check_auth` must
    /// confirm a configured, working credential on every mint, and the
    /// profile is (re)fetched inline while the row is still unlinked. A
    /// joined collaborator's identity was proven by its own gist proof and
    /// is accepted as cached.
    async fn inviting_principal(&self) -> Result<Principal> {
        let id = crate::principal_ops::caller_principal_id(&self.store)
            .await?
            .ok_or_else(crate::principal_ops::no_caller)?;
        let principal = self.store.get_principal(&id).await?;
        if !principal.is_primary {
            return if principal.github_user_id.is_some() {
                Ok(principal)
            } else {
                Err(Error::Invite(InviteErrorKind::GithubIdentityRequired))
            };
        }
        let authenticated = match self.identity_source_control().await {
            Ok(sc) => sc.check_auth().await.is_ok_and(|s| s.authenticated),
            Err(_) => false,
        };
        if !authenticated {
            return Err(Error::Invite(InviteErrorKind::GithubIdentityRequired));
        }
        if principal.github_user_id.is_some() {
            return Ok(principal);
        }
        if let Ok(refreshed) = self.refresh_primary_identity(principal).await {
            if refreshed.github_user_id.is_some() {
                return Ok(refreshed);
            }
        }
        Err(Error::Invite(InviteErrorKind::GithubIdentityRequired))
    }

    /// The forge used for identity lookups — the invite pin's
    /// `GET /users/{login}` and the primary's `GET /user` refresh: the
    /// injected engine when one is wired, else the active provider built
    /// from defaults with the API-base override applied, so every identity
    /// read talks to the same host (the e2e mock in tests, `api.github.com`
    /// in production).
    pub(crate) async fn identity_source_control(
        &self,
    ) -> Result<Arc<dyn intent_sourcecontrol::SourceControl>> {
        if let Some(sc) = self.source_control.clone() {
            return Ok(sc);
        }
        let mut settings = intent_sourcecontrol::SourceControlSettings::default();
        settings.github.api_base_url = resolve_api_base_uri(self.github_api_base_uri.as_deref());
        intent_sourcecontrol::SourceControlRegistry::from_settings(&settings)
            .await
            .map_err(pr_ops::map_sc_err)
    }

    /// The forge `invite.prove` reads the proof gist and the claimed account
    /// with: [`Self::identity_source_control`] (the host's stored token), or
    /// — when the host holds no GitHub token at all — an anonymous client on
    /// the same API host. Both reads are public on GitHub (a secret gist is
    /// unlisted, not private), so the fallback only forgoes the higher
    /// authenticated rate limit.
    async fn proof_source_control(&self) -> Result<Arc<dyn SourceControl>> {
        if let Some(sc) = self.source_control.clone() {
            return Ok(sc);
        }
        let api_base = resolve_api_base_uri(self.github_api_base_uri.as_deref());
        let mut settings = intent_sourcecontrol::SourceControlSettings::default();
        settings.github.api_base_url.clone_from(&api_base);
        match intent_sourcecontrol::SourceControlRegistry::from_settings(&settings).await {
            Ok(sc) => Ok(sc),
            Err(intent_sourcecontrol::Error::NotConfigured(_)) => Ok(Arc::new(
                intent_sourcecontrol::GitHubSourceControl::anonymous(api_base.as_deref())
                    .map_err(pr_ops::map_sc_err)?,
            )),
            Err(e) => Err(pr_ops::map_sc_err(e)),
        }
    }

    /// Current `workspace_member` row count of one workspace, for the
    /// `memberCount` carried by membership-changing `workspace:updated`
    /// events (multiplayer w4).
    pub(crate) async fn member_count(&self, workspace_id: &WorkspaceId) -> Result<u64> {
        Ok(self
            .store
            .workspace_membership_summaries(None, std::slice::from_ref(workspace_id))
            .await?
            .get(workspace_id)
            .map_or(0, |m| m.member_count))
    }

    /// The effective `sharing.maxGuestsPerWorkspace` — read live, so a
    /// settings change applies to the next mint / join without a restart.
    pub(crate) fn max_guests_per_workspace(&self) -> u32 {
        self.effective_settings().sharing.max_guests_per_workspace
    }

    /// The listener's link envelope for stamping listed invites with `url`,
    /// once per call: `None` (never an error) when no builder is attached or
    /// no link can be built right now — the invite rows are still answered.
    async fn invite_link_envelope(&self) -> Option<Box<dyn InviteLinkEnvelope>> {
        self.invite_links.get()?.invite_link_envelope().await
    }

    /// `workspace.invite.create`: see
    /// [`intent_core::WorkspaceApi::workspace_invite_create`]. The returned
    /// `invite` carries no `url`: the transport resolves the link envelope
    /// exactly once per create and stamps the same link as both the top-level
    /// `url` and `invite.url`, so this path never resolves it a second time.
    pub(crate) async fn workspace_invite_create_op(
        &self,
        workspace_id: &WorkspaceId,
        pin_login: Option<String>,
        expires_in_secs: Option<u64>,
    ) -> Result<Value> {
        self.require_owner(workspace_id, "workspace.invite.create")
            .await?;
        let ws = self.store.get_workspace(workspace_id).await?;
        let ttl = expires_in_secs.unwrap_or(DEFAULT_INVITE_TTL_SECS);
        if ttl == 0 || ttl > MAX_INVITE_TTL_SECS {
            return Err(Error::InvalidParams(format!(
                "expiresInSecs must be between 1 and {MAX_INVITE_TTL_SECS}"
            )));
        }
        let creator = self.inviting_principal().await?;
        // Every open invite reserves a guest seat, so the cap is spent by
        // collaborators plus open invites — else N open links could all be
        // redeemed against one remaining seat. Re-checked under the
        // transition lock below, right before the insert.
        let max_guests = self.max_guests_per_workspace();
        if self
            .store
            .count_workspace_guests(workspace_id)
            .await?
            .committed()
            >= u64::from(max_guests)
        {
            return Err(Error::Invite(InviteErrorKind::GuestLimit));
        }
        let pin_login = pin_login
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty());
        let (pin_github_user_id, pin_login) = match pin_login {
            None => (None, None),
            Some(login) => {
                let sc = self.identity_source_control().await?;
                match sc.get_user_by_login(&login).await {
                    Ok(user) => match user.id.and_then(|id| i64::try_from(id).ok()) {
                        Some(id) => (Some(id), Some(user.login)),
                        None => return Err(Error::Invite(InviteErrorKind::PinUnknown)),
                    },
                    Err(intent_sourcecontrol::Error::NotFound(_)) => {
                        return Err(Error::Invite(InviteErrorKind::PinUnknown));
                    }
                    Err(e) => return Err(pr_ops::map_sc_err(e)),
                }
            }
        };
        let secret = random_hex_secret();
        let invite = WorkspaceInvite {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: workspace_id.clone(),
            secret_hash: hash_secret(&secret),
            secret: Some(secret.clone()),
            created_by_principal_id: creator.id.clone(),
            pin_github_user_id,
            pin_login,
            created_at: now_iso(),
            expires_at: iso_after(ttl),
            redeemed_at: None,
            redeemed_by_principal_id: None,
            revoked_at: None,
            redemption_count: 0,
        };
        // The insert is what locks the primary identity, so it is
        // serialised with the identity transition and the creator's
        // identity is revalidated under the lock: a switch that landed
        // since `inviting_principal` qualified it means this link would
        // have been minted from an identity that no longer holds.
        {
            let _transition = self.identity_transition.lock().await;
            let current = self.store.get_principal(&creator.id).await?;
            if current.github_user_id.is_none() || current.github_user_id != creator.github_user_id
            {
                return Err(Error::Internal(
                    "the inviting GitHub identity changed while minting; retry".to_string(),
                ));
            }
            // Mints are serialised by this lock, so the recount here is what
            // keeps two concurrent mints from both taking the last seat.
            if self.store.count_workspace_guests(&ws.id).await?.committed() >= u64::from(max_guests)
            {
                return Err(Error::Invite(InviteErrorKind::GuestLimit));
            }
            // The first invite of a workspace pins its legacy author:
            // content authored before anyone else could have joined is the
            // owner's. Pinned *before* the insert — the secret travels only
            // in this response, so nothing fallible may follow the insert
            // or a failure would leave an open link the owner never saw.
            if let Some(fallback) = self.store.get_workspace_author_fallback(&ws.id).await? {
                if fallback.legacy_author_principal_id.is_none() {
                    let owner = fallback.owner_principal_id.unwrap_or(creator.id.clone());
                    self.store
                        .set_workspace_legacy_author_principal_id(&ws.id, Some(&owner))
                        .await?;
                }
            }
            self.store.insert_workspace_invite(&invite).await?;
        }
        crate::publish_event(
            self.event_bus.as_ref(),
            crate::workspace_updated_event(workspace_id, &json!({ "invites": true })),
        )
        .await;
        Ok(json!({
            "invite": invite_to_wire(&invite, None),
            "secret": secret,
        }))
    }

    /// `workspace.invite.list`: see
    /// [`intent_core::WorkspaceApi::workspace_invite_list`]. The envelope is
    /// resolved once per call, so the cost stays O(rows) formatting.
    pub(crate) async fn workspace_invite_list_op(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Value> {
        self.require_owner(workspace_id, "workspace.invite.list")
            .await?;
        self.store.get_workspace(workspace_id).await?;
        let invites = self.store.list_open_workspace_invites(workspace_id).await?;
        let envelope = self.invite_link_envelope().await;
        Ok(json!({
            "invites": invites
                .iter()
                .map(|i| invite_to_wire(i, envelope.as_deref()))
                .collect::<Vec<_>>(),
        }))
    }

    /// `workspace.invite.revoke`: see
    /// [`intent_core::WorkspaceApi::workspace_invite_revoke`].
    pub(crate) async fn workspace_invite_revoke_op(
        &self,
        workspace_id: &WorkspaceId,
        invite_id: &str,
    ) -> Result<Value> {
        self.require_owner(workspace_id, "workspace.invite.revoke")
            .await?;
        self.store.get_workspace(workspace_id).await?;
        match self.store.get_workspace_invite(invite_id).await? {
            Some(invite) if invite.workspace_id == *workspace_id => {}
            _ => {
                return Err(Error::NotFound(format!(
                    "invite {invite_id} not found in workspace {workspace_id}"
                )));
            }
        }
        let revoked = self.store.revoke_workspace_invite(invite_id).await?;
        if revoked {
            crate::publish_event(
                self.event_bus.as_ref(),
                crate::workspace_updated_event(workspace_id, &json!({ "invites": true })),
            )
            .await;
        }
        Ok(json!({ "revoked": revoked }))
    }

    /// `workspace.members.leave`: see
    /// [`intent_core::WorkspaceApi::workspace_members_leave`].
    pub(crate) async fn workspace_members_leave_op(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Value> {
        let (principal_id, _) = self.self_principal().await?;
        match self
            .store
            .get_workspace_member_role(workspace_id, &principal_id)
            .await?
        {
            None => Err(Error::NotFound(format!(
                "workspace {workspace_id} not found"
            ))),
            Some(WorkspaceRole::Owner) => Err(Error::InvalidParams(format!(
                "the owner of workspace {workspace_id} cannot leave it"
            ))),
            Some(WorkspaceRole::Collaborator) => {
                let left = self
                    .detach_collaborator(workspace_id, &principal_id)
                    .await?;
                Ok(json!({ "left": left }))
            }
        }
    }

    /// `principal.revokeSelf`: see
    /// [`intent_core::WorkspaceApi::principal_revoke_self`].
    pub(crate) async fn principal_revoke_self_op(&self) -> Result<Value> {
        let (principal_id, is_administrator) = self.self_principal().await?;
        if is_administrator {
            return Err(Error::InvalidParams(
                "the daemon administrator cannot revoke itself; use github.revoke or \
                 rotate the server token"
                    .to_string(),
            ));
        }
        let mut workspaces = 0u64;
        for m in self.store.list_principal_memberships(&principal_id).await? {
            if m.role == WorkspaceRole::Collaborator
                && self
                    .detach_collaborator(&m.workspace_id, &principal_id)
                    .await?
            {
                workspaces += 1;
            }
        }
        let credentials = self
            .store
            .revoke_all_principal_credentials(&principal_id)
            .await?;
        let _ = self.principal_revocations.send(principal_id);
        Ok(json!({ "revoked": true, "credentials": credentials, "workspaces": workspaces }))
    }
}

/// Resolve the GitHub API base for the identity reads: the builder override,
/// else the env seam, else `None` (api.github.com). Non-loopback cleartext
/// overrides are ignored like the login host's.
pub(crate) fn resolve_api_base_uri(override_uri: Option<&str>) -> Option<String> {
    override_uri
        .map(str::to_string)
        .or_else(|| {
            std::env::var(API_BASE_URI_ENV)
                .ok()
                .filter(|s| !s.trim().is_empty())
        })
        .filter(|uri| {
            if github_auth_ops::is_safe_login_base_uri(uri) {
                true
            } else {
                tracing::warn!(
                    uri,
                    "ignoring github api base-uri override: must be https:// \
                     or cleartext http:// on a loopback host"
                );
                false
            }
        })
}

impl Services {
    /// The open invite `(invite_id, secret)` names and its workspace — the
    /// validation every `/invite` method starts with. A wrong id and a
    /// wrong secret are the same [`InviteErrorKind::NotFound`]; a closed
    /// invite is its [`closed_kind`].
    async fn open_invite(
        &self,
        invite_id: &str,
        secret: &str,
    ) -> Result<(WorkspaceInvite, Workspace)> {
        let invite = match self.store.get_workspace_invite(invite_id).await? {
            Some(invite) if hashes_match(&invite.secret_hash, &hash_secret(secret)) => invite,
            _ => return Err(Error::Invite(InviteErrorKind::NotFound)),
        };
        if let Some(kind) = closed_kind(&invite, &now_iso()) {
            return Err(Error::Invite(kind));
        }
        let ws = self
            .store
            .get_workspace(&invite.workspace_id)
            .await
            .map_err(|_| Error::Invite(InviteErrorKind::NotFound))?;
        Ok((invite, ws))
    }

    /// `invite.inspect`: see [`intent_core::WorkspaceApi::invite_inspect`].
    /// Reads only — no nonce slot, no permit, no upstream call.
    pub(crate) async fn invite_inspect_op(&self, invite_id: &str, secret: &str) -> Result<Value> {
        let (invite, ws) = self.open_invite(invite_id, secret).await?;
        Ok(json!({
            "workspaceId": invite.workspace_id,
            "workspaceTitle": ws.title,
        }))
    }

    /// `invite.accept`: see [`intent_core::WorkspaceApi::invite_accept`].
    pub(crate) async fn invite_accept_op(
        &self,
        invite_id: &str,
        secret: &str,
        credential: &str,
    ) -> Result<Value> {
        let (invite, principal) = self
            .invite_accept_resolve(invite_id, secret, credential)
            .await?;
        // The presented credential is validated again and rotated out inside
        // the join transaction (exactly one active row must flip, or the
        // join is refused `CredentialInvalid`): the guest leaves with exactly
        // one active credential for this host, and of two concurrent accepts
        // presenting the same credential exactly one mints.
        self.commit_invite_join(&invite, &principal, Some(&hash_secret(credential)))
            .await
    }

    /// The pre-transaction half of `invite.accept`: name the principal the
    /// credential identifies and the open invite, and enforce the pin. The
    /// active-credential check here is advisory — it gives the early, cheap
    /// refusal and the `github_user_id` the pin needs — the authoritative
    /// check is the rotate inside the join transaction.
    async fn invite_accept_resolve(
        &self,
        invite_id: &str,
        secret: &str,
        credential: &str,
    ) -> Result<(WorkspaceInvite, Principal)> {
        // The credential is the proof of identity: the same active-only
        // resolve the `/ws` bearer gate runs, so a revoked one is refused
        // here exactly as it would be at upgrade.
        let principal_id = self
            .store
            .resolve_active_principal_credential(&hash_secret(credential))
            .await?
            .ok_or(Error::Invite(InviteErrorKind::CredentialInvalid))?;
        let principal = self.store.get_principal(&principal_id).await?;
        // The host owner's own account cannot join as a guest: a credential
        // that resolves to the primary row (minted before this guard
        // existed) identifies the owner, not a guest.
        if principal.is_primary {
            return Err(Error::Invite(InviteErrorKind::OwnerSelfJoin));
        }
        // Only a GitHub-verified guest can join by invite (the join is keyed
        // by `github_user_id`); a credential bound to any other principal
        // does not identify one.
        let Some(github_user_id) = principal.github_user_id else {
            return Err(Error::Invite(InviteErrorKind::CredentialInvalid));
        };
        let (invite, _) = self.open_invite(invite_id, secret).await?;
        if invite
            .pin_github_user_id
            .is_some_and(|pinned| pinned != github_user_id)
        {
            return Err(Error::Invite(InviteErrorKind::PinMismatch));
        }
        Ok((invite, principal))
    }

    /// `invite.challenge`: see
    /// [`intent_core::WorkspaceApi::invite_challenge`]. The nonce takes a
    /// daemon-wide permit and counts against the invite's outstanding
    /// nonces; either bound spent is [`InviteErrorKind::FlowBusy`].
    pub(crate) async fn invite_challenge_op(&self, invite_id: &str, secret: &str) -> Result<Value> {
        let (invite, ws) = self.open_invite(invite_id, secret).await?;
        let nonce = random_nonce();
        let now = Instant::now();
        let issued_at = SystemTime::now();
        let expires_at = now + NONCE_TTL;
        let nonce_expires_at = iso_ms_from_now(u64::try_from(NONCE_TTL.as_millis()).unwrap_or(0));
        {
            let mut nonces = self.invite_nonces.lock().await;
            purge_nonces(&mut nonces, now);
            if nonces
                .values()
                .filter(|slot| slot.invite_id == invite.id)
                .count()
                >= MAX_NONCES_PER_INVITE
            {
                return Err(Error::Invite(InviteErrorKind::FlowBusy));
            }
            let Ok(permit) = self.invite_nonce_permits.clone().try_acquire_owned() else {
                return Err(Error::Invite(InviteErrorKind::FlowBusy));
            };
            nonces.insert(
                nonce.clone(),
                NonceSlot {
                    invite_id: invite.id.clone(),
                    issued_at,
                    expires_at,
                    _permit: permit,
                },
            );
        }
        Ok(json!({
            "workspaceId": invite.workspace_id,
            "workspaceTitle": ws.title,
            "nonce": nonce,
            "nonceExpiresAt": nonce_expires_at,
        }))
    }

    /// `invite.prove`: see [`intent_core::WorkspaceApi::invite_prove`].
    pub(crate) async fn invite_prove_op(
        &self,
        invite_id: &str,
        secret: &str,
        nonce: &str,
        gist_id: &str,
        login: &str,
    ) -> Result<Value> {
        let login = login.trim();
        if !valid_login(login) {
            return Err(Error::InvalidParams(
                "`login` must be a GitHub login (1-39 alphanumerics or hyphens)".to_string(),
            ));
        }
        let gist_id = gist_id.trim();
        if gist_id.is_empty() || !gist_id.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(Error::InvalidParams(
                "`gistId` must be a non-empty alphanumeric gist id".to_string(),
            ));
        }
        let (invite, _) = self.open_invite(invite_id, secret).await?;

        // Consume the nonce under the lock: exactly one concurrent attempt
        // gets the slot; every other one is `ProofInvalid` right here. The
        // slot (and its permit) is held by this frame until the verdict —
        // put back only when GitHub could not be consulted.
        let slot = {
            let mut nonces = self.invite_nonces.lock().await;
            nonces.remove(nonce.trim())
        };
        let Some(slot) = slot else {
            return Err(Error::Invite(InviteErrorKind::ProofInvalid));
        };
        if slot.invite_id != invite.id {
            return Err(Error::Invite(InviteErrorKind::ProofInvalid));
        }
        if slot.expires_at <= Instant::now() {
            return Err(Error::Invite(InviteErrorKind::ProofExpired));
        }

        let sc = self.proof_source_control().await?;
        let gist = match sc.get_proof_gist(gist_id).await {
            Ok(gist) => gist,
            Err(intent_sourcecontrol::Error::NotFound(_)) => {
                return Err(Error::Invite(InviteErrorKind::ProofInvalid));
            }
            Err(e) => {
                tracing::debug!(error = %e, gist_id, "proof gist read failed");
                self.restore_nonce(nonce, slot).await;
                return Err(Error::Invite(InviteErrorKind::GithubUnreachable));
            }
        };
        if !proof_matches(&gist, login, nonce.trim(), slot.issued_at) {
            return Err(Error::Invite(InviteErrorKind::ProofInvalid));
        }
        let user = match sc.get_user_by_login(login).await {
            Ok(user) => user,
            Err(intent_sourcecontrol::Error::NotFound(_)) => {
                return Err(Error::Invite(InviteErrorKind::ProofInvalid));
            }
            Err(e) => {
                tracing::debug!(error = %e, login, "proof account lookup failed");
                self.restore_nonce(nonce, slot).await;
                return Err(Error::Invite(InviteErrorKind::GithubUnreachable));
            }
        };
        // `GET /users/{login}` is the authority on the account; the gist's
        // owner already matched the claim case-insensitively.
        // repo-slug-fold: allow — a GitHub user login (case-insensitive on GitHub), not a repo slug
        if !user.login.eq_ignore_ascii_case(&gist.owner_login) {
            return Err(Error::Invite(InviteErrorKind::ProofInvalid));
        }
        drop(slot);
        self.complete_invite_join(&invite.id, &user).await
    }

    /// Put a consumed nonce back for a retry after GitHub could not be
    /// reached (its lifetime keeps running).
    async fn restore_nonce(&self, nonce: &str, slot: NonceSlot) {
        let mut nonces = self.invite_nonces.lock().await;
        nonces.entry(nonce.trim().to_string()).or_insert(slot);
    }

    /// The first-time join, once the invitee's GitHub identity is proven:
    /// refuse the host owner's own account
    /// ([`InviteErrorKind::OwnerSelfJoin`] — the earliest point the identity
    /// is known, so the owner never receives a guest credential), re-check
    /// the invite (pin, still open), map the resolved account onto a fresh
    /// principal row and commit through [`Self::commit_invite_join`].
    async fn complete_invite_join(&self, invite_id: &str, user: &UserIdentity) -> Result<Value> {
        let github_user_id = user
            .id
            .and_then(|id| i64::try_from(id).ok())
            .ok_or_else(|| Error::Internal("github identity carries no account id".to_string()))?;
        if self.store.get_primary_principal().await?.github_user_id == Some(github_user_id) {
            return Err(Error::Invite(InviteErrorKind::OwnerSelfJoin));
        }
        let invite = self
            .store
            .get_workspace_invite(invite_id)
            .await?
            .ok_or(Error::Invite(InviteErrorKind::NotFound))?;
        if let Some(kind) = closed_kind(&invite, &now_iso()) {
            return Err(Error::Invite(kind));
        }
        if invite
            .pin_github_user_id
            .is_some_and(|pinned| pinned != github_user_id)
        {
            return Err(Error::Invite(InviteErrorKind::PinMismatch));
        }
        let mut identity = Principal {
            id: PrincipalId::new(),
            github_user_id: None,
            login: None,
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        apply_identity(&mut identity, user);
        self.commit_invite_join(&invite, &identity, None).await
    }

    /// The join shared by the gist proof and `invite.accept`: in one store
    /// transaction
    /// ([`intent_store::Store::join_workspace_by_invite`]) mint or reuse the
    /// principal keyed by `identity.github_user_id`, redeem the invite (the
    /// conditional UPDATE is the single-use guard of a pinned invite; a
    /// reusable one stays open), add the `collaborator` membership (a
    /// returning member's re-join is idempotent), record a fresh
    /// per-principal credential and consume
    /// `rotate_from_hash` (the credential an `invite.accept` presented) when
    /// given — a hash that is not exactly one active credential of the
    /// joining principal at that moment refuses the whole join as
    /// [`InviteErrorKind::CredentialInvalid`] with nothing written, and an
    /// account that resolves to the primary principal as
    /// [`InviteErrorKind::OwnerSelfJoin`] (the transaction-level guard
    /// behind the early checks above). The
    /// event is published only after the commit; the credential is returned
    /// exactly once, in the `authorized` result.
    async fn commit_invite_join(
        &self,
        invite: &WorkspaceInvite,
        identity: &Principal,
        rotate_from_hash: Option<&str>,
    ) -> Result<Value> {
        let invite_id = invite.id.as_str();
        let token = random_hex_secret();
        let principal = match self
            .store
            .join_workspace_by_invite(
                invite_id,
                &invite.workspace_id,
                identity,
                &hash_secret(&token),
                rotate_from_hash,
                self.max_guests_per_workspace(),
            )
            .await?
        {
            InviteJoinOutcome::Joined(principal) => principal,
            InviteJoinOutcome::Closed => {
                let kind = self
                    .store
                    .get_workspace_invite(invite_id)
                    .await?
                    .and_then(|i| closed_kind(&i, &now_iso()))
                    .unwrap_or(InviteErrorKind::NotFound);
                return Err(Error::Invite(kind));
            }
            InviteJoinOutcome::WorkspaceFull => {
                return Err(Error::Invite(InviteErrorKind::WorkspaceFull));
            }
            InviteJoinOutcome::CredentialInvalid => {
                return Err(Error::Invite(InviteErrorKind::CredentialInvalid));
            }
            InviteJoinOutcome::OwnerSelfJoin => {
                return Err(Error::Invite(InviteErrorKind::OwnerSelfJoin));
            }
        };
        let member_count = self.member_count(&invite.workspace_id).await?;
        crate::publish_event(
            self.event_bus.as_ref(),
            crate::workspace_updated_event(
                &invite.workspace_id,
                &json!({
                    "members": true,
                    "invites": true,
                    "addedPrincipalId": principal.id,
                    "memberCount": member_count,
                }),
            ),
        )
        .await;
        Ok(json!({
            "status": "authorized",
            "token": token,
            "principalId": principal.id,
            "login": principal.login,
            "workspaceId": invite.workspace_id,
        }))
    }
}

#[cfg(test)]
mod tests;
