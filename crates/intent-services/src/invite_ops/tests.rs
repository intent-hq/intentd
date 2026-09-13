//! Unit tests for the invite / identity-only join surface (multiplayer w4):
//! the primary-identity reconnect guard, invite create/list/revoke, the
//! redemption phases, the join itself, `members.leave` and
//! `principal.revokeSelf`. Everything below `complete_invite_join` is
//! exercised without a forge: the identity is passed in directly.

use super::*;
use crate::tests::pr::StubForge;
use crate::tests::{workspace, TempDb};
use intent_core::{with_caller, WorkspaceApi};
use intent_store::Store;

/// One workspace owned by a non-administrator principal (the primary is
/// demoted so the one-owner index admits the promotion) plus a collaborator
/// member. Both carry a GitHub identity so invite creation needs no forge.
struct Fixture {
    services: Services,
    store: Store,
    ws: WorkspaceId,
    primary: PrincipalId,
    owner: PrincipalId,
    collaborator: PrincipalId,
}

fn principal(login: &str, github_user_id: Option<i64>) -> Principal {
    Principal {
        id: PrincipalId::new(),
        github_user_id,
        login: Some(login.to_string()),
        display_name: Some(format!("{login} name")),
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    }
}

fn identity(login: &str, id: u64) -> UserIdentity {
    UserIdentity {
        login: login.to_string(),
        id: Some(id),
        name: Some(format!("{login} name")),
        avatar_url: Some(format!("https://avatars.example/{login}")),
        html_url: None,
    }
}

fn wire(principal_id: &PrincipalId) -> Caller {
    Caller::Wire {
        principal_id: principal_id.clone(),
        is_administrator: false,
    }
}

async fn fixture(tmp: &TempDb) -> Fixture {
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let primary = store.get_primary_principal().await.expect("primary").id;
    let owner = principal("owner", Some(1001));
    let collaborator = principal("collab", Some(2002));
    for p in [&owner, &collaborator] {
        store.upsert_principal(p).await.expect("principal");
    }
    store
        .set_workspace_member_role(&ws, &primary, WorkspaceRole::Collaborator)
        .await
        .expect("demote primary");
    store
        .add_workspace_member(&ws, &owner.id, WorkspaceRole::Owner)
        .await
        .expect("owner");
    store
        .add_workspace_member(&ws, &collaborator.id, WorkspaceRole::Collaborator)
        .await
        .expect("collaborator");
    Fixture {
        services: Services::new(store.clone()),
        store,
        ws,
        primary,
        owner: owner.id,
        collaborator: collaborator.id,
    }
}

impl Fixture {
    async fn create_invite(&self, ttl: Option<u64>) -> Value {
        with_caller(
            wire(&self.owner),
            self.services
                .workspace_invite_create_op(&self.ws, None, ttl),
        )
        .await
        .expect("create invite")
    }
}

fn invite_kind<T: std::fmt::Debug>(r: &Result<T>) -> InviteErrorKind {
    match r {
        Err(Error::Invite(kind)) => *kind,
        other => panic!("expected an invite error, got {other:?}"),
    }
}

fn id_of(created: &Value) -> String {
    created["invite"]["id"]
        .as_str()
        .expect("invite id")
        .to_string()
}

// --- reconnect guard -------------------------------------------------------

/// Single-user daemon: a `GET /user` naming a different account simply
/// switches the cached identity (no other principal, no open invite).
#[tokio::test]
async fn primary_identity_switches_while_single_user() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services = Services::new(store.clone());
    let mut primary = store.get_primary_principal().await.expect("primary");
    primary.github_user_id = Some(10);
    primary.login = Some("first".into());
    store
        .upsert_principal(&primary)
        .await
        .expect("seed identity");

    let updated = services
        .apply_primary_identity(primary.clone(), &identity("second", 20))
        .await
        .expect("switch applies");
    assert_eq!(updated.github_user_id, Some(20));
    assert_eq!(updated.login.as_deref(), Some("second"));
    let stored = store.get_primary_principal().await.expect("primary");
    assert_eq!(stored.github_user_id, Some(20));
}

/// With another principal row present, a different account id is refused
/// with `IdentityLocked` and the cached identity is untouched; the same
/// account id still refreshes the profile fields.
#[tokio::test]
async fn primary_identity_locked_once_another_principal_exists() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services = Services::new(store.clone());
    let mut primary = store.get_primary_principal().await.expect("primary");
    primary.github_user_id = Some(10);
    primary.login = Some("first".into());
    store
        .upsert_principal(&primary)
        .await
        .expect("seed identity");
    store
        .upsert_principal(&principal("guest", Some(77)))
        .await
        .expect("second principal");

    let err = services
        .apply_primary_identity(primary.clone(), &identity("second", 20))
        .await;
    assert_eq!(invite_kind(&err), InviteErrorKind::IdentityLocked);
    let stored = store.get_primary_principal().await.expect("primary");
    assert_eq!(stored.github_user_id, Some(10));
    assert_eq!(stored.login.as_deref(), Some("first"));

    let refreshed = services
        .apply_primary_identity(stored, &identity("first-renamed", 10))
        .await
        .expect("same account refreshes");
    assert_eq!(refreshed.github_user_id, Some(10));
    assert_eq!(refreshed.login.as_deref(), Some("first-renamed"));
}

/// While locked, a profile without a stable account id is unverifiable and
/// refused: the cached identity stays.
#[tokio::test]
async fn locked_identity_refuses_a_missing_account_id() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services = Services::new(store.clone());
    let mut primary = store.get_primary_principal().await.expect("primary");
    primary.github_user_id = Some(10);
    primary.login = Some("original-owner".into());
    store
        .upsert_principal(&primary)
        .await
        .expect("seed identity");
    store
        .upsert_principal(&principal("existing-guest", Some(20)))
        .await
        .expect("second principal");
    let mut unverified = identity("different-account", 30);
    unverified.id = None;

    let err = services
        .apply_primary_identity(primary.clone(), &unverified)
        .await;
    assert_eq!(invite_kind(&err), InviteErrorKind::IdentityLocked);
    let stored = store.get_primary_principal().await.expect("primary");
    assert_eq!(stored.github_user_id, primary.github_user_id);
    assert_eq!(stored.login, primary.login);
}

/// The pre-persist guard fails closed: when the lock state cannot be read
/// (the invite table is unavailable) the grant is refused and the cached
/// identity stays, so no token is written on an unverified account.
#[tokio::test]
async fn connect_guard_refuses_when_the_lock_state_is_unreadable() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services = Services::new(store.clone());
    let mut primary = store.get_primary_principal().await.expect("primary");
    primary.github_user_id = Some(10);
    primary.login = Some("original-owner".into());
    store
        .upsert_principal(&primary)
        .await
        .expect("seed identity");
    sqlx::query("ALTER TABLE workspace_invite RENAME TO unavailable_invites")
        .execute(store.write_pool())
        .await
        .expect("inject query failure");

    let guard = services.connect_identity_guard();
    let result = guard(Arc::new(StubForge::default())).await;
    assert!(result.is_err(), "{result:?}");
    let stored = store.get_primary_principal().await.expect("primary");
    assert_eq!(stored.github_user_id, primary.github_user_id);
    assert_eq!(stored.login, primary.login);
}

/// An open invite alone (still a single principal row) locks the identity
/// too: the link was minted from it. Revoking the invite unlocks the switch.
#[tokio::test]
async fn primary_identity_locked_while_an_invite_is_open() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services = Services::new(store.clone()).with_source_control(Arc::new(StubForge::default()));
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let mut primary = store.get_primary_principal().await.expect("primary");
    primary.github_user_id = Some(10);
    store
        .upsert_principal(&primary)
        .await
        .expect("seed identity");
    let created = with_caller(
        Caller::Daemon,
        services.workspace_invite_create_op(&ws, None, None),
    )
    .await
    .expect("create invite");

    let err = services
        .apply_primary_identity(primary.clone(), &identity("second", 20))
        .await;
    assert_eq!(invite_kind(&err), InviteErrorKind::IdentityLocked);

    store
        .revoke_workspace_invite(&id_of(&created))
        .await
        .expect("revoke");
    let updated = services
        .apply_primary_identity(primary, &identity("second", 20))
        .await
        .expect("switch applies once no invite is open");
    assert_eq!(updated.github_user_id, Some(20));
}

/// Poll a spawned task a few scheduler turns and report whether it settled.
async fn settles(handle: &tokio::task::JoinHandle<impl Send>) -> bool {
    for _ in 0..20 {
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        if handle.is_finished() {
            return true;
        }
    }
    false
}

/// The identity switch's lock check and write are one critical section:
/// a switch parked on the transition lock re-reads the lock state once it
/// gets in, so an invite minted meanwhile is seen and the switch refused —
/// the read cannot go stale between count and write.
#[tokio::test]
async fn identity_switch_rechecks_the_lock_after_a_concurrent_mint() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services = Services::new(store.clone()).with_source_control(Arc::new(StubForge::default()));
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let mut primary = store.get_primary_principal().await.expect("primary");
    primary.github_user_id = Some(10);
    primary.login = Some("first".into());
    store
        .upsert_principal(&primary)
        .await
        .expect("seed identity");
    assert!(!services
        .primary_identity_locked()
        .await
        .expect("lock state"));

    let held = services.identity_transition.clone().lock_owned().await;
    let switch = tokio::spawn({
        let services = services.clone();
        let primary = primary.clone();
        async move {
            services
                .apply_primary_identity(primary, &identity("second", 20))
                .await
        }
    });
    assert!(
        !settles(&switch).await,
        "switch waits for the transition lock"
    );
    // The mint lands while the switch is parked (the store path: the
    // service path would itself queue behind the same lock).
    let invite = WorkspaceInvite {
        id: uuid::Uuid::new_v4().to_string(),
        workspace_id: ws.clone(),
        secret_hash: hash_secret("s"),
        created_by_principal_id: primary.id.clone(),
        pin_github_user_id: None,
        pin_login: None,
        created_at: now_iso(),
        expires_at: iso_after(60),
        redeemed_at: None,
        redeemed_by_principal_id: None,
        revoked_at: None,
    };
    store
        .insert_workspace_invite(&invite)
        .await
        .expect("mint while parked");
    drop(held);

    let err = switch.await.expect("switch task");
    assert_eq!(invite_kind(&err), InviteErrorKind::IdentityLocked);
    let stored = store.get_primary_principal().await.expect("primary");
    assert_eq!(stored.github_user_id, Some(10));
    assert_eq!(stored.login.as_deref(), Some("first"));
}

/// Invite minting queues behind the transition lock and revalidates the
/// creator's identity under it: a switch that landed while the mint was
/// parked refuses the mint and writes no invite; an unchanged identity
/// mints once the lock is released.
#[tokio::test]
async fn invite_mint_waits_for_the_transition_and_revalidates_the_creator() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;

    let held = f.services.identity_transition.clone().lock_owned().await;
    let mint = tokio::spawn({
        let services = f.services.clone();
        let (ws, owner) = (f.ws.clone(), f.owner.clone());
        async move {
            with_caller(
                wire(&owner),
                services.workspace_invite_create_op(&ws, None, None),
            )
            .await
        }
    });
    assert!(!settles(&mint).await, "mint waits for the transition lock");
    let mut owner = f.store.get_principal(&f.owner).await.expect("owner");
    owner.github_user_id = Some(9999);
    f.store
        .upsert_principal(&owner)
        .await
        .expect("identity switched while parked");
    drop(held);
    let r = mint.await.expect("mint task");
    assert!(
        matches!(r, Err(Error::Internal(ref m)) if m.contains("changed while minting")),
        "{r:?}"
    );
    assert_eq!(
        f.store
            .count_open_workspace_invites()
            .await
            .expect("count invites"),
        0
    );

    let held = f.services.identity_transition.clone().lock_owned().await;
    let mint = tokio::spawn({
        let services = f.services.clone();
        let (ws, owner) = (f.ws.clone(), f.owner.clone());
        async move {
            with_caller(
                wire(&owner),
                services.workspace_invite_create_op(&ws, None, None),
            )
            .await
        }
    });
    assert!(!settles(&mint).await, "second mint waits too");
    drop(held);
    mint.await.expect("mint task").expect("mints once released");
    assert_eq!(
        f.store
            .count_open_workspace_invites()
            .await
            .expect("count invites"),
        1
    );
}

/// The primary's cached `github_user_id` survives `github.revoke`, so the
/// cache alone must not mint: with no working credential the create is
/// `GithubIdentityRequired`, and no invite row is written.
#[tokio::test]
async fn primary_cannot_mint_without_a_live_credential() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services =
        Services::new(store.clone()).with_source_control(Arc::new(StubForge::unauthenticated()));
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let mut primary = store.get_primary_principal().await.expect("primary");
    primary.github_user_id = Some(10);
    store
        .upsert_principal(&primary)
        .await
        .expect("seed identity");
    let r = with_caller(
        Caller::Daemon,
        services.workspace_invite_create_op(&ws, None, None),
    )
    .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubIdentityRequired);
    assert_eq!(
        store
            .count_open_workspace_invites()
            .await
            .expect("count invites"),
        0
    );
}

// --- create / list / revoke ------------------------------------------------

/// Create returns the invite (secret never on it) plus the secret once, pins
/// the workspace's legacy author on the first invite, lists it as open, and
/// revoke closes it (idempotently).
#[tokio::test]
async fn invite_lifecycle_create_list_revoke() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let created = f.create_invite(Some(3600)).await;
    let invite = &created["invite"];
    let id = id_of(&created);
    let secret = created["secret"].as_str().expect("secret");
    assert_eq!(secret.len(), 64);
    assert!(invite.get("secretHash").is_none() && invite.get("secret").is_none());
    assert_eq!(invite["workspaceId"], json!(f.ws.0));
    assert_eq!(invite["createdByPrincipalId"], json!(f.owner.0));
    assert!(invite.get("pinLogin").is_none());
    let stored = f
        .store
        .get_workspace_invite(&id)
        .await
        .expect("get")
        .expect("row");
    assert_eq!(stored.secret_hash, hash_secret(secret));
    let fallback = f
        .store
        .get_workspace_author_fallback(&f.ws)
        .await
        .expect("fallback")
        .expect("row");
    // The first invite pins the legacy author to the workspace's owner
    // column (the primary here: the fixture's role swap does not rewrite
    // the mirror) — and only once.
    assert!(fallback.owner_principal_id.is_some());
    assert_eq!(
        fallback.legacy_author_principal_id,
        fallback.owner_principal_id
    );
    f.store
        .set_workspace_legacy_author_principal_id(&f.ws, Some(&f.collaborator))
        .await
        .expect("repoint legacy author");
    let _ = f.create_invite(Some(3600)).await;
    let fallback = f
        .store
        .get_workspace_author_fallback(&f.ws)
        .await
        .expect("fallback")
        .expect("row");
    assert_eq!(
        fallback.legacy_author_principal_id,
        Some(f.collaborator.clone())
    );

    let listed = with_caller(wire(&f.owner), f.services.workspace_invite_list_op(&f.ws))
        .await
        .expect("list");
    let ids: Vec<&str> = listed["invites"]
        .as_array()
        .expect("invites")
        .iter()
        .filter_map(|i| i["id"].as_str())
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&id.as_str()));

    let revoked = with_caller(
        wire(&f.owner),
        f.services.workspace_invite_revoke_op(&f.ws, &id),
    )
    .await
    .expect("revoke");
    assert_eq!(revoked, json!({ "revoked": true }));
    let again = with_caller(
        wire(&f.owner),
        f.services.workspace_invite_revoke_op(&f.ws, &id),
    )
    .await
    .expect("revoke again");
    assert_eq!(again, json!({ "revoked": false }));
    let listed = with_caller(wire(&f.owner), f.services.workspace_invite_list_op(&f.ws))
        .await
        .expect("list");
    assert_eq!(listed["invites"].as_array().map(Vec::len), Some(1));
    assert_ne!(listed["invites"][0]["id"], json!(id));
}

/// Owner-only: a collaborator cannot mint or list; an out-of-range TTL is
/// `InvalidParams`; revoking an invite through another workspace is
/// `NotFound`; an owner without a GitHub identity cannot mint.
#[tokio::test]
async fn invite_create_guards() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let r = with_caller(
        wire(&f.collaborator),
        f.services.workspace_invite_create_op(&f.ws, None, None),
    )
    .await;
    assert!(matches!(r, Err(Error::Forbidden(_))), "{r:?}");
    let r = with_caller(
        wire(&f.collaborator),
        f.services.workspace_invite_list_op(&f.ws),
    )
    .await;
    assert!(matches!(r, Err(Error::Forbidden(_))), "{r:?}");

    for ttl in [0, MAX_INVITE_TTL_SECS + 1] {
        let r = with_caller(
            wire(&f.owner),
            f.services
                .workspace_invite_create_op(&f.ws, None, Some(ttl)),
        )
        .await;
        assert!(
            matches!(r, Err(Error::InvalidParams(_))),
            "ttl {ttl}: {r:?}"
        );
    }

    let created = f.create_invite(None).await;
    let other = WorkspaceId::new();
    f.store
        .insert_workspace(&workspace(&other))
        .await
        .expect("other ws");
    let r = with_caller(
        Caller::Daemon,
        f.services
            .workspace_invite_revoke_op(&other, &id_of(&created)),
    )
    .await;
    assert!(matches!(r, Err(Error::NotFound(_))), "{r:?}");

    let mut unlinked = f.store.get_principal(&f.owner).await.expect("owner");
    unlinked.github_user_id = None;
    f.store.upsert_principal(&unlinked).await.expect("unlink");
    let r = with_caller(
        wire(&f.owner),
        f.services.workspace_invite_create_op(&f.ws, None, None),
    )
    .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubIdentityRequired);
}

// --- redeem ----------------------------------------------------------------

/// Phase 1 never reaches the device flow for a bad link: an unknown id or a
/// wrong secret is `NotFound` (indistinguishable), an expired invite
/// `Expired`, a revoked one `Revoked`; phase 2 on an unknown flow is
/// `FlowNotFound`.
#[tokio::test]
async fn redeem_start_refuses_closed_or_unknown_invites() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let created = f.create_invite(None).await;
    let id = id_of(&created);
    let secret = created["secret"].as_str().expect("secret").to_string();

    let r = f.services.invite_redeem_start_op("missing", &secret).await;
    assert_eq!(invite_kind(&r), InviteErrorKind::NotFound);
    let r = f.services.invite_redeem_start_op(&id, "wrong").await;
    assert_eq!(invite_kind(&r), InviteErrorKind::NotFound);

    let mut expired = f
        .store
        .get_workspace_invite(&id)
        .await
        .expect("get")
        .expect("row");
    expired.id = "expired".to_string();
    expired.secret_hash = hash_secret("expired-secret");
    expired.expires_at = "2000-01-01T00:00:00.000Z".to_string();
    f.store
        .insert_workspace_invite(&expired)
        .await
        .expect("insert expired");
    let r = f
        .services
        .invite_redeem_start_op("expired", "expired-secret")
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::Expired);

    f.store.revoke_workspace_invite(&id).await.expect("revoke");
    let r = f.services.invite_redeem_start_op(&id, &secret).await;
    assert_eq!(invite_kind(&r), InviteErrorKind::Revoked);

    let r = f.services.invite_redeem_wait_op("no-such-flow").await;
    assert_eq!(invite_kind(&r), InviteErrorKind::FlowNotFound);
}

/// The join: a fresh principal keyed by the GitHub account id, a
/// collaborator membership, one active credential whose hash resolves to the
/// principal, the invite redeemed by it — and the same link cannot be
/// redeemed twice. A returning account reuses its principal row.
#[tokio::test]
async fn complete_join_mints_principal_membership_and_credential_once() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let created = f.create_invite(None).await;
    let id = id_of(&created);

    let joined = f
        .services
        .complete_invite_join(&id, &identity("guest", 4242))
        .await
        .expect("join");
    assert_eq!(joined["status"], json!("authorized"));
    assert_eq!(joined["workspaceId"], json!(f.ws.0));
    assert_eq!(joined["login"], json!("guest"));
    let token = joined["token"].as_str().expect("token");
    assert_eq!(token.len(), 64);
    let guest = PrincipalId(joined["principalId"].as_str().expect("pid").to_string());

    let row = f.store.get_principal(&guest).await.expect("principal");
    assert_eq!(row.github_user_id, Some(4242));
    assert!(!row.is_primary);
    assert_eq!(
        f.store
            .get_workspace_member_role(&f.ws, &guest)
            .await
            .expect("role"),
        Some(WorkspaceRole::Collaborator)
    );
    let cred = f
        .store
        .lookup_principal_credential(&hash_secret(token))
        .await
        .expect("lookup")
        .expect("credential");
    assert_eq!(cred.principal_id, guest);
    assert!(cred.is_active());
    let invite = f
        .store
        .get_workspace_invite(&id)
        .await
        .expect("get")
        .expect("row");
    assert!(invite.redeemed_at.is_some());
    assert_eq!(invite.redeemed_by_principal_id, Some(guest.clone()));

    let again = f
        .services
        .complete_invite_join(&id, &identity("guest", 4242))
        .await;
    assert_eq!(invite_kind(&again), InviteErrorKind::Redeemed);

    let second = f.create_invite(None).await;
    let joined = f
        .services
        .complete_invite_join(&id_of(&second), &identity("guest-renamed", 4242))
        .await
        .expect("second join");
    assert_eq!(joined["principalId"], json!(guest.0));
    let row = f.store.get_principal(&guest).await.expect("principal");
    assert_eq!(row.login.as_deref(), Some("guest-renamed"));
    assert_eq!(f.store.count_principals().await.expect("count"), 4);
}

/// A pinned invite admits only the pinned account id (the login may have
/// changed meanwhile); another account is `PinMismatch` and the invite stays
/// open for the right one.
#[tokio::test]
async fn complete_join_enforces_the_pin_by_account_id() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let created = f.create_invite(None).await;
    let mut pinned = f
        .store
        .get_workspace_invite(&id_of(&created))
        .await
        .expect("get")
        .expect("row");
    pinned.id = "pinned".to_string();
    pinned.secret_hash = hash_secret("pinned-secret");
    pinned.pin_github_user_id = Some(5555);
    pinned.pin_login = Some("pinned-login".to_string());
    f.store
        .insert_workspace_invite(&pinned)
        .await
        .expect("insert pinned");

    let r = f
        .services
        .complete_invite_join("pinned", &identity("intruder", 6666))
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::PinMismatch);
    assert!(f
        .store
        .get_workspace_invite("pinned")
        .await
        .expect("get")
        .expect("row")
        .redeemed_at
        .is_none());

    let joined = f
        .services
        .complete_invite_join("pinned", &identity("renamed-login", 5555))
        .await
        .expect("pinned account joins");
    assert_eq!(joined["login"], json!("renamed-login"));
}

// --- leave / revokeSelf ----------------------------------------------------

/// `members.leave`: a collaborator leaves (membership gone), the owner
/// cannot, and a non-member sees the workspace as `NotFound`.
#[tokio::test]
async fn members_leave_detaches_collaborators_only() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let r = with_caller(wire(&f.owner), f.services.workspace_members_leave_op(&f.ws)).await;
    assert!(matches!(r, Err(Error::InvalidParams(_))), "{r:?}");

    let left = with_caller(
        wire(&f.collaborator),
        f.services.workspace_members_leave_op(&f.ws),
    )
    .await
    .expect("leave");
    assert_eq!(left, json!({ "left": true }));
    assert_eq!(
        f.store
            .get_workspace_member_role(&f.ws, &f.collaborator)
            .await
            .expect("role"),
        None
    );
    let r = with_caller(
        wire(&f.collaborator),
        f.services.workspace_members_leave_op(&f.ws),
    )
    .await;
    assert!(matches!(r, Err(Error::NotFound(_))), "{r:?}");
}

/// `principal.revokeSelf`: every active credential of the caller is revoked,
/// its collaborator memberships are dropped, and the principal id is
/// broadcast so the transport closes its connections. The administrator is
/// refused.
#[tokio::test]
async fn revoke_self_revokes_credentials_memberships_and_broadcasts() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    for n in 0..2 {
        f.store
            .insert_principal_credential(&f.collaborator, &hash_secret(&format!("t{n}")))
            .await
            .expect("credential");
    }
    let mut revocations = f
        .services
        .subscribe_principal_revocations()
        .expect("revocation feed");

    let r = with_caller(
        Caller::Wire {
            principal_id: f.primary.clone(),
            is_administrator: true,
        },
        f.services.principal_revoke_self_op(),
    )
    .await;
    assert!(matches!(r, Err(Error::InvalidParams(_))), "{r:?}");

    let revoked = with_caller(wire(&f.collaborator), f.services.principal_revoke_self_op())
        .await
        .expect("revoke self");
    assert_eq!(
        revoked,
        json!({ "revoked": true, "credentials": 2, "workspaces": 1 })
    );
    assert!(f
        .store
        .list_principal_credentials(&f.collaborator)
        .await
        .expect("credentials")
        .iter()
        .all(|c| !c.is_active()));
    assert_eq!(
        f.store
            .get_workspace_member_role(&f.ws, &f.collaborator)
            .await
            .expect("role"),
        None
    );
    assert_eq!(revocations.try_recv().expect("broadcast"), f.collaborator);
}

// --- verifier probes (adopted as regression tests) --------------------------

/// The primary's cached `github_user_id` does not admit an invite when the
/// live credential check fails: a real forge client pointed at a closed
/// loopback port reports `isConfigured: false`, the mint is refused with
/// `GithubIdentityRequired`, and no invite row is written.
#[tokio::test]
async fn cached_primary_identity_requires_live_auth() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let mut primary = f.store.get_principal(&f.primary).await.expect("primary");
    primary.github_user_id = Some(10);
    f.store
        .upsert_principal(&primary)
        .await
        .expect("seed identity");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let base = format!("http://{}", listener.local_addr().expect("address"));
    drop(listener);
    let sc = intent_sourcecontrol::GitHubSourceControl::new("dummy-offline-token", Some(&base))
        .expect("offline forge");
    let services = f.services.with_source_control(Arc::new(sc));
    let status = with_caller(Caller::Daemon, services.github_auth_status())
        .await
        .expect("auth status");
    assert_eq!(status["isConfigured"], false);
    let r = with_caller(
        Caller::Daemon,
        services.workspace_invite_create_op(&f.ws, None, None),
    )
    .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubIdentityRequired);
    assert_eq!(
        f.store.count_open_workspace_invites().await.expect("count"),
        0
    );
}

/// Fixture with an event bus and one note created by the collaborator.
async fn attribution_fixture() -> (TempDb, Fixture, intent_core::NoteId) {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    f.services = f
        .services
        .with_event_bus(crate::events::EventBus::new(f.store.clone()));
    let created = with_caller(
        wire(&f.collaborator),
        f.services.create_note(
            f.ws.clone(),
            intent_core::NoteCreate {
                title: "Attribution probe".into(),
                content: Some("Probe anchor text".into()),
                ..Default::default()
            },
            None,
            None,
        ),
    )
    .await
    .expect("create note");
    (tmp, f, created.note.id)
}

/// A collaborator's `comment.add` is attributed to its principal: the
/// client-supplied `author`/`authorType` are ignored.
#[tokio::test]
async fn comment_author_cannot_be_spoofed_by_a_collaborator() {
    let (_tmp, f, note_id) = attribution_fixture().await;
    with_caller(
        wire(&f.collaborator),
        f.services.comment_add(
            f.ws.clone(),
            note_id.clone(),
            "Probe anchor text".into(),
            "anchor".into(),
            "Verifier comment".into(),
            None,
            Some("owner".into()),
            Some("agent".into()),
            None,
            None,
        ),
    )
    .await
    .expect("add comment");
    let rows = f
        .store
        .list_comments_in_workspace(&f.ws, &note_id)
        .await
        .expect("comments");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].author, "collab");
    assert_eq!(rows[0].author_type, intent_core::AuthorType::User);
}

/// Same for `comment.respond`: the reply carries the collaborator, not the
/// claimed author.
#[tokio::test]
async fn reply_author_cannot_be_spoofed_by_a_collaborator() {
    let (_tmp, f, note_id) = attribution_fixture().await;
    let added = with_caller(
        wire(&f.collaborator),
        f.services.comment_add(
            f.ws.clone(),
            note_id.clone(),
            "Probe anchor text".into(),
            "anchor".into(),
            "Verifier root".into(),
            None,
            Some("collab".into()),
            Some("user".into()),
            None,
            None,
        ),
    )
    .await
    .expect("add comment");
    with_caller(
        wire(&f.collaborator),
        f.services.comment_respond(
            f.ws.clone(),
            note_id.clone(),
            None,
            Some(added.comment_id),
            "Verifier reply".into(),
            None,
            Some("owner".into()),
            Some("agent".into()),
            None,
            None,
        ),
    )
    .await
    .expect("reply");
    let rows = f
        .store
        .list_comments_in_workspace(&f.ws, &note_id)
        .await
        .expect("comments");
    let reply = rows
        .iter()
        .find(|row| row.parent_id.is_some())
        .expect("reply row");
    assert_eq!(reply.author, "collab");
    assert_eq!(reply.author_type, intent_core::AuthorType::User);
}

/// A system-actored event emitted inside a collaborator's request carries
/// `{ type: user, id: principalId, name: login }`.
#[tokio::test]
async fn event_actor_is_the_acting_collaborator() {
    let (_tmp, f, _) = attribution_fixture().await;
    let rows = f
        .store
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(f.ws.clone()),
            event_types: vec!["note:created".into()],
            ..Default::default()
        })
        .await
        .expect("events");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].actor.actor_type, intent_core::ActorType::User);
    assert_eq!(rows[0].actor.id.as_deref(), Some(f.collaborator.as_str()));
    assert_eq!(rows[0].actor.name.as_deref(), Some("collab"));
}

/// The re-stamp is unconditional on the emitting path's actor: a pre-set
/// non-system actor naming someone else is still replaced by the acting
/// collaborator, so no code path (or client-influenced payload) can attribute
/// a collaborator's action to another principal. Only an agent-actored event
/// keeps its actor — the agent is the subject, not the person who acted.
#[tokio::test]
async fn event_actor_restamp_overrides_a_preset_non_system_actor() {
    let (_tmp, f, _) = attribution_fixture().await;
    let bus = crate::events::EventBus::new(f.store.clone());
    let event = |event_type: &str, actor: intent_core::EventActor| intent_store::NewEvent {
        workspace_id: f.ws.clone(),
        timestamp: intent_core::now_iso(),
        event_type: event_type.to_string(),
        actor,
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({}),
    };
    let (preset_user, agent) = with_caller(wire(&f.collaborator), async {
        let preset_user = bus
            .publish(&event(
                "note:updated",
                intent_core::EventActor {
                    actor_type: intent_core::ActorType::User,
                    id: Some(f.primary.0.clone()),
                    name: Some("owner".into()),
                    ..Default::default()
                },
            ))
            .await
            .expect("publish preset-user event");
        let agent = bus
            .publish(&event(
                "agent:status:changed",
                intent_core::EventActor {
                    actor_type: intent_core::ActorType::Agent,
                    id: Some("agent-1".into()),
                    name: Some("Agent".into()),
                    ..Default::default()
                },
            ))
            .await
            .expect("publish agent event");
        (preset_user, agent)
    })
    .await;
    assert_eq!(preset_user.actor.actor_type, intent_core::ActorType::User);
    assert_eq!(
        preset_user.actor.id.as_deref(),
        Some(f.collaborator.as_str()),
        "a pre-set user actor is re-stamped with the acting collaborator"
    );
    assert_eq!(preset_user.actor.name.as_deref(), Some("collab"));
    assert_eq!(agent.actor.actor_type, intent_core::ActorType::Agent);
    assert_eq!(agent.actor.id.as_deref(), Some("agent-1"));
}

/// The administrator's events keep the actor the operation supplied.
#[tokio::test]
async fn event_actor_is_untouched_for_the_administrator() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    f.services = f
        .services
        .with_event_bus(crate::events::EventBus::new(f.store.clone()));
    with_caller(
        Caller::Wire {
            principal_id: f.primary.clone(),
            is_administrator: true,
        },
        f.services.create_note(
            f.ws.clone(),
            intent_core::NoteCreate {
                title: "Administrator note".into(),
                ..Default::default()
            },
            None,
            None,
        ),
    )
    .await
    .expect("create note");
    let rows = f
        .store
        .query_events(&intent_store::EventQuery {
            workspace_id: Some(f.ws.clone()),
            event_types: vec!["note:created".into()],
            ..Default::default()
        })
        .await
        .expect("events");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].actor.actor_type, intent_core::ActorType::System);
}

/// Invite rows survive a reopen of the migrated store.
#[tokio::test]
async fn migration_reopen_preserves_invites() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let created = f.create_invite(None).await;
    let id = id_of(&created);
    let reopened = Store::open(&tmp.path).await.expect("reopen migrated store");
    assert!(reopened
        .get_workspace_invite(&id)
        .await
        .expect("read invite")
        .is_some());
    assert_eq!(
        reopened
            .count_open_workspace_invites()
            .await
            .expect("count"),
        1
    );
}

/// Removing a member drops only that member's queued messages, publishes
/// the changed queue, and revokes the member's read access.
#[tokio::test]
async fn member_removal_drops_only_the_guest_queue() {
    let (_tmp, f, _) = attribution_fixture().await;
    let id = intent_core::AgentId::new();
    let session: intent_core::AgentSession = serde_json::from_value(json!({
        "id": id, "workspaceId": f.ws, "name": "Queue probe", "status": "idle",
        "createdAt": now_iso(), "updatedAt": now_iso()
    }))
    .expect("session fixture");
    f.store
        .insert_agent_session(&session)
        .await
        .expect("session");
    for (principal, message) in [
        (&f.owner, "owner survives"),
        (&f.collaborator, "guest removed"),
    ] {
        with_caller(
            wire(principal),
            f.services
                .agent_queue_message(id.clone(), message.into(), None, None, None),
        )
        .await
        .expect("queue message");
    }
    let queue_query = intent_store::EventQuery {
        workspace_id: Some(f.ws.clone()),
        event_types: vec!["agent:queue:updated".into()],
        ..Default::default()
    };
    let before_events = f
        .store
        .query_events(&queue_query)
        .await
        .expect("before events")
        .len();
    with_caller(
        wire(&f.owner),
        f.services
            .workspace_members_remove(f.ws.clone(), f.collaborator.clone()),
    )
    .await
    .expect("remove member");
    let queue = with_caller(
        wire(&f.owner),
        f.services.agent_get_queue(id.clone(), Some(f.ws.clone())),
    )
    .await
    .expect("queue");
    assert_eq!(queue["queue"].as_array().expect("entries").len(), 1);
    assert_eq!(queue["queue"][0]["content"], "owner survives");
    let events = f
        .store
        .query_events(&queue_query)
        .await
        .expect("queue events");
    assert_eq!(events.len(), before_events + 1);
    let r = with_caller(
        wire(&f.collaborator),
        f.services.get_workspace(f.ws.clone()),
    )
    .await;
    assert!(matches!(r, Err(Error::NotFound(_))), "{r:?}");
}

/// The API-base seam accepts loopback cleartext and https overrides and
/// ignores a cleartext non-loopback host.
#[test]
fn api_base_override_is_loopback_or_https_only() {
    assert_eq!(
        resolve_api_base_uri(Some("http://127.0.0.1:9")).as_deref(),
        Some("http://127.0.0.1:9")
    );
    assert_eq!(
        resolve_api_base_uri(Some("https://ghe.example.com/api/v3")).as_deref(),
        Some("https://ghe.example.com/api/v3")
    );
    assert_eq!(resolve_api_base_uri(Some("http://evil.example.com")), None);
}

/// `hash_secret` is hex SHA-256 and `hashes_match` is length-strict.
#[test]
fn secret_hashing_shape() {
    let h = hash_secret("abc");
    assert_eq!(
        h,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert!(hashes_match(&h, &hash_secret("abc")));
    assert!(!hashes_match(&h, &hash_secret("abd")));
    assert!(!hashes_match(&h, "ba78"));
}
