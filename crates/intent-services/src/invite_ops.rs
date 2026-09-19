//! Workspace invite links and the identity-only device-flow join
//! (multiplayer w4): `workspace.invite.create` / `.list` / `.revoke` on the
//! owner side, `invite.redeem` (start + wait) on the unauthenticated
//! `/invite` side, plus `workspace.members.leave` / `principal.revokeSelf`.
//!
//! An invite is a single-use, expiring `(id, secret)` pair; only the hex
//! SHA-256 of the secret is stored. Redemption runs the same GitHub device
//! grant as `github.connect` but with **no scopes** and through
//! [`intent_sourcecontrol::IdentityFlow`], whose access token is spent on
//! one `GET /user` inside the engine and never persisted — the daemon
//! learns *who* the invitee is (stable `github_user_id`) and nothing else.
//! The joined principal is minted (or reused, keyed by `github_user_id`),
//! added as a `collaborator`, and issued a fresh per-principal credential
//! that is returned exactly once.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    current_caller, iso_ms_from_now, now_iso, Caller, Error, InviteErrorKind, Principal,
    PrincipalId, Result, WorkspaceId, WorkspaceInvite, WorkspaceRole,
};
use intent_sourcecontrol::{IdentityFlow, IdentityPollStatus, UserIdentity};
use serde_json::{json, Value};
use tokio::sync::{broadcast, watch, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::{github_auth_ops, pr_ops, Services};

/// Default invite lifetime (7 days) when `expiresInSecs` is omitted.
pub(crate) const DEFAULT_INVITE_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Longest lifetime a client may request (30 days).
pub(crate) const MAX_INVITE_TTL_SECS: u64 = 30 * 24 * 60 * 60;

/// Concurrent identity-only device flows the daemon keeps in flight; the
/// `/invite` endpoint is unauthenticated, so this bounds what an anonymous
/// peer holding one valid link can make the daemon poll for. Enforced as a
/// semaphore whose permit is taken *before* the device-code request and
/// lives in the flow's slot, so refused starts cost no upstream call.
pub(crate) const MAX_INFLIGHT_INVITE_FLOWS: usize = 16;

/// Consecutive poll errors tolerated before a flow is marked failed.
const MAX_CONSECUTIVE_POLL_ERRORS: u32 = github_auth_ops::MAX_CONSECUTIVE_POLL_ERRORS;

/// How long a settled flow's result stays collectable before it is dropped.
const SETTLED_FLOW_GRACE: Duration = Duration::from_secs(120);

/// How much longer a timed-out waiter stays attached once the poll task has
/// started committing the join, so the one outcome carrying the credential
/// is collected rather than dropped with the slot.
const JOIN_COMMIT_GRACE: Duration = Duration::from_secs(30);

/// Env override for the GitHub API base the identity flow's `GET /user`
/// talks to — the spawned-daemon test seam (e2e points it at a local mock).
/// Honored under the same loopback-or-https rule as the login host.
pub(crate) const API_BASE_URI_ENV: &str = "INTENTD_GITHUB_API_BASE_URI";

/// Where one identity-only flow stands. `outcome` is written exactly once by
/// the poll task; `done` flips to `true` at the same moment so a waiting
/// `invite.redeem` wakes without polling.
pub(crate) struct InviteFlowSlot {
    invite_id: String,
    deadline: Instant,
    settled_at: Option<Instant>,
    /// Set once the grant is in hand and the join is being committed: a
    /// waiter that times out meanwhile must not purge the slot.
    committing: bool,
    outcome: Option<Result<Value>>,
    done: watch::Sender<bool>,
    /// The [`MAX_INFLIGHT_INVITE_FLOWS`] permit; released when the slot is
    /// collected or purged.
    _permit: OwnedSemaphorePermit,
}

/// In-flight and recently settled identity flows keyed by flow id.
pub(crate) type InviteFlowState = Arc<tokio::sync::Mutex<HashMap<String, InviteFlowSlot>>>;

/// Admission permits for identity flows ([`MAX_INFLIGHT_INVITE_FLOWS`]).
pub(crate) type InviteFlowPermits = Arc<Semaphore>;

pub(crate) fn new_flow_permits() -> InviteFlowPermits {
    Arc::new(Semaphore::new(MAX_INFLIGHT_INVITE_FLOWS))
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

/// The invite's wire shape (`secret` never included).
pub(crate) fn invite_to_wire(invite: &WorkspaceInvite) -> Value {
    serde_json::to_value(invite).unwrap_or_else(|_| json!({ "id": invite.id }))
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
/// (`None` when it is open).
fn closed_kind(invite: &WorkspaceInvite, now: &str) -> Option<InviteErrorKind> {
    if invite.revoked_at.is_some() {
        Some(InviteErrorKind::Revoked)
    } else if invite.redeemed_at.is_some() {
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
    /// joined collaborator's identity was proven by its own device grant and
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
    /// from defaults with the identity flow's API-base override applied, so
    /// every identity read and the invitee's `GET /user` talk to the same
    /// host (the e2e mock in tests, `api.github.com` in production).
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

    /// `workspace.invite.create`: see
    /// [`intent_core::WorkspaceApi::workspace_invite_create`].
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
            created_by_principal_id: creator.id.clone(),
            pin_github_user_id,
            pin_login,
            created_at: now_iso(),
            expires_at: iso_after(ttl),
            redeemed_at: None,
            redeemed_by_principal_id: None,
            revoked_at: None,
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
        Ok(json!({ "invite": invite_to_wire(&invite), "secret": secret }))
    }

    /// `workspace.invite.list`: see
    /// [`intent_core::WorkspaceApi::workspace_invite_list`].
    pub(crate) async fn workspace_invite_list_op(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Value> {
        self.require_owner(workspace_id, "workspace.invite.list")
            .await?;
        self.store.get_workspace(workspace_id).await?;
        let invites = self.store.list_open_workspace_invites(workspace_id).await?;
        Ok(json!({ "invites": invites.iter().map(invite_to_wire).collect::<Vec<_>>() }))
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

/// Resolve the GitHub API base for the identity flow: the builder override,
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
    /// `invite.redeem` phase 1: see
    /// [`intent_core::WorkspaceApi::invite_redeem_start`].
    pub(crate) async fn invite_redeem_start_op(
        &self,
        invite_id: &str,
        secret: &str,
    ) -> Result<Value> {
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

        // Reserve the flow's capacity before the upstream device-code
        // request: purge collectable slots (returning their permits), then
        // take one — a refused start never reaches GitHub. The permit is
        // held by this frame until it moves into the slot below, so a start
        // that fails upstream releases it on return.
        purge_flows(&mut *self.invite_flows.lock().await);
        let Ok(permit) = self.invite_flow_permits.clone().try_acquire_owned() else {
            return Err(Error::Invite(InviteErrorKind::FlowBusy));
        };
        let client_id = self
            .effective_settings()
            .source_control
            .github
            .oauth_client_id;
        let login_base =
            github_auth_ops::resolve_login_base_uri(self.github_login_base_uri.as_deref());
        let api_base = resolve_api_base_uri(self.github_api_base_uri.as_deref());
        let (auth, flow) = intent_sourcecontrol::device_flow::start_identity_at(
            &login_base,
            api_base.as_deref(),
            &client_id,
        )
        .await
        .map_err(pr_ops::map_sc_err)?;

        let flow_id = uuid::Uuid::new_v4().to_string();
        let deadline = Instant::now() + Duration::from_secs(auth.expires_in);
        let (done, _) = watch::channel(false);
        self.invite_flows.lock().await.insert(
            flow_id.clone(),
            InviteFlowSlot {
                invite_id: invite.id.clone(),
                deadline,
                settled_at: None,
                committing: false,
                outcome: None,
                done,
                _permit: permit,
            },
        );
        tokio::spawn(
            self.clone()
                .poll_invite_flow(flow_id.clone(), flow, deadline),
        );
        Ok(json!({
            "flowId": flow_id,
            "userCode": auth.user_code,
            "verificationUri": auth.verification_uri,
            "expiresIn": auth.expires_in,
            "interval": auth.interval,
            "workspaceId": invite.workspace_id,
            "workspaceTitle": ws.title,
        }))
    }

    /// `invite.redeem` phase 2: see
    /// [`intent_core::WorkspaceApi::invite_redeem_wait`].
    pub(crate) async fn invite_redeem_wait_op(&self, flow_id: &str) -> Result<Value> {
        let (mut done, deadline) = {
            let mut flows = self.invite_flows.lock().await;
            let Some(slot) = flows.get_mut(flow_id) else {
                return Err(Error::Invite(InviteErrorKind::FlowNotFound));
            };
            if slot.outcome.is_some() {
                let slot = flows.remove(flow_id).expect("slot present");
                return slot.outcome.expect("settled outcome");
            }
            (slot.done.subscribe(), slot.deadline)
        };
        let budget = deadline
            .saturating_duration_since(Instant::now())
            .saturating_add(Duration::from_secs(5));
        let mut settled = tokio::time::timeout(budget, done.wait_for(|d| *d))
            .await
            .is_ok_and(|r| r.is_ok());
        // A timeout while the poll task is committing the join must not
        // purge the slot: the grant is spent and the credential is (about to
        // be) stored, and this outcome is the only copy of the token. Stay
        // attached for the commit's grace and collect it.
        if !settled {
            let committing = {
                let flows = self.invite_flows.lock().await;
                flows
                    .get(flow_id)
                    .is_some_and(|slot| slot.committing && slot.outcome.is_none())
            };
            if committing {
                settled = tokio::time::timeout(JOIN_COMMIT_GRACE, done.wait_for(|d| *d))
                    .await
                    .is_ok_and(|r| r.is_ok());
            }
        }
        let mut flows = self.invite_flows.lock().await;
        let Some(slot) = flows.remove(flow_id) else {
            return Err(Error::Invite(InviteErrorKind::FlowNotFound));
        };
        if let Some(outcome) = slot.outcome {
            return outcome;
        }
        debug_assert!(!settled, "done flipped without an outcome");
        Err(Error::Invite(InviteErrorKind::FlowExpired))
    }

    /// Background poll loop of one identity flow: ticks at the engine's
    /// interval until GitHub settles the grant or `deadline` passes, then
    /// records the outcome on the slot (which may already have been purged —
    /// then the result is dropped and the invite stays open).
    async fn poll_invite_flow(self, flow_id: String, mut flow: IdentityFlow, deadline: Instant) {
        let mut consecutive_errors = 0u32;
        let outcome = loop {
            tokio::time::sleep(Duration::from_secs(flow.interval_secs())).await;
            if Instant::now() >= deadline {
                break Err(Error::Invite(InviteErrorKind::FlowExpired));
            }
            if !self.invite_flows.lock().await.contains_key(&flow_id) {
                tracing::debug!(flow_id, "invite flow abandoned; poll task exiting");
                return;
            }
            match flow.poll_once().await {
                Ok(IdentityPollStatus::Pending) => consecutive_errors = 0,
                Ok(IdentityPollStatus::Authorized(user)) => {
                    // Claim the slot for the commit under the lock: from
                    // here a timed-out waiter keeps the slot (see
                    // `invite_redeem_wait_op`) instead of dropping the
                    // outcome that carries the credential.
                    let invite_id = {
                        let mut flows = self.invite_flows.lock().await;
                        flows.get_mut(&flow_id).map(|s| {
                            s.committing = true;
                            s.invite_id.clone()
                        })
                    };
                    let Some(invite_id) = invite_id else {
                        return;
                    };
                    break self.complete_invite_join(&invite_id, &user).await;
                }
                Ok(IdentityPollStatus::Expired) => {
                    break Err(Error::Invite(InviteErrorKind::FlowExpired));
                }
                Ok(IdentityPollStatus::Denied) => {
                    break Err(Error::Invite(InviteErrorKind::FlowDenied));
                }
                Err(e) => {
                    // A failure *after* the grant (the one `GET /user`) is
                    // terminal: the device code is spent and must not be
                    // polled again. Pre-grant blips keep the retry budget.
                    if flow.is_spent() {
                        tracing::debug!(error = %e, "invite identity lookup failed after grant");
                        break Err(Error::Invite(InviteErrorKind::FlowError));
                    }
                    consecutive_errors += 1;
                    tracing::debug!(
                        error = %e,
                        attempt = consecutive_errors,
                        "invite identity flow poll failed"
                    );
                    if consecutive_errors >= MAX_CONSECUTIVE_POLL_ERRORS {
                        break Err(Error::Invite(InviteErrorKind::FlowError));
                    }
                }
            }
        };
        let mut flows = self.invite_flows.lock().await;
        if let Some(slot) = flows.get_mut(&flow_id) {
            slot.outcome = Some(outcome);
            slot.settled_at = Some(Instant::now());
            let _ = slot.done.send(true);
        }
    }

    /// The join itself, once the invitee's GitHub identity is proven:
    /// re-check the invite (pin, still open), then — in one store
    /// transaction ([`intent_store::Store::join_workspace_by_invite`]) —
    /// mint or reuse the principal keyed by `github_user_id`, redeem the
    /// invite (the conditional UPDATE is the single-use guard), add the
    /// `collaborator` membership and record a fresh per-principal
    /// credential. The event is published only after the commit.
    async fn complete_invite_join(&self, invite_id: &str, user: &UserIdentity) -> Result<Value> {
        let github_user_id = user
            .id
            .and_then(|id| i64::try_from(id).ok())
            .ok_or_else(|| Error::Internal("github identity carries no account id".to_string()))?;
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
        let token = random_hex_secret();
        let Some(principal) = self
            .store
            .join_workspace_by_invite(
                invite_id,
                &invite.workspace_id,
                &identity,
                &hash_secret(&token),
            )
            .await?
        else {
            let kind = self
                .store
                .get_workspace_invite(invite_id)
                .await?
                .and_then(|i| closed_kind(&i, &now_iso()))
                .unwrap_or(InviteErrorKind::NotFound);
            return Err(Error::Invite(kind));
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

/// Drop settled slots past their collection grace and pending slots whose
/// codes expired long enough ago that no waiter can still be attached.
fn purge_flows(flows: &mut HashMap<String, InviteFlowSlot>) {
    let now = Instant::now();
    flows.retain(|_, slot| match slot.settled_at {
        Some(at) => now.saturating_duration_since(at) < SETTLED_FLOW_GRACE,
        None => now.saturating_duration_since(slot.deadline) < SETTLED_FLOW_GRACE,
    });
}

#[cfg(test)]
mod tests;
