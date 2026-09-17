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

/// The caller's snapshot never decides the guard: a `GET /user` that
/// completes with a snapshot taken before a switch to account 20 landed
/// (and before the daemon became locked) is judged against the current
/// row — refused for the old account, and the snapshot's stale fields are
/// never written back over the current identity.
#[tokio::test]
async fn stale_snapshot_cannot_bypass_the_reconnect_guard() {
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
    let stale = primary.clone();

    services
        .apply_primary_identity(primary, &identity("second", 20))
        .await
        .expect("single-user switch");
    store
        .upsert_principal(&principal("guest", Some(77)))
        .await
        .expect("second principal");

    let err = services
        .apply_primary_identity(stale.clone(), &identity("first", 10))
        .await;
    assert_eq!(invite_kind(&err), InviteErrorKind::IdentityLocked);
    let stored = store.get_primary_principal().await.expect("primary");
    assert_eq!(stored.github_user_id, Some(20));
    assert_eq!(stored.login.as_deref(), Some("second"));

    let refreshed = services
        .apply_primary_identity(stale, &identity("second-renamed", 20))
        .await
        .expect("current account refreshes from a stale snapshot");
    assert_eq!(refreshed.github_user_id, Some(20));
    assert_eq!(refreshed.login.as_deref(), Some("second-renamed"));
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
    assert!(
        result.is_err(),
        "unreadable lock state must refuse the grant"
    );
    let stored = store.get_primary_principal().await.expect("primary");
    assert_eq!(stored.github_user_id, primary.github_user_id);
    assert_eq!(stored.login, primary.login);
}

/// An admitted grant hands the transition lock back to the flow as its
/// lease: while the flow holds it (across the token write) no invite can
/// be minted and no other switch can run, and both proceed once the lease
/// is dropped. The verdict and the credential landing are one critical
/// section.
#[tokio::test]
async fn connect_guard_lease_pins_the_transition_until_dropped() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let services = Services::new(store.clone()).with_source_control(Arc::new(StubForge::default()));
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");

    let guard = services.connect_identity_guard();
    let lease = guard(Arc::new(StubForge::default()))
        .await
        .unwrap_or_else(|reason| panic!("single-user grant admitted: {reason}"));
    let applied = store.get_primary_principal().await.expect("primary");
    assert_eq!(applied.github_user_id, Some(583_231));
    assert_eq!(applied.login.as_deref(), Some("octocat"));

    let mint = tokio::spawn({
        let services = services.clone();
        let ws = ws.clone();
        async move {
            with_caller(
                Caller::Daemon,
                services.workspace_invite_create_op(&ws, None, None),
            )
            .await
        }
    });
    assert!(!settles(&mint).await, "mint waits for the lease");
    let switch = tokio::spawn({
        let services = services.clone();
        async move {
            services
                .apply_primary_identity(applied, &identity("other", 20))
                .await
        }
    });
    assert!(!settles(&switch).await, "switch waits for the lease");

    drop(lease);
    mint.await
        .expect("mint task")
        .expect("mint after the lease");
    let err = switch.await.expect("switch task");
    assert_eq!(invite_kind(&err), InviteErrorKind::IdentityLocked);
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
        secret: None,
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

/// An expired invite is already closed: revoking it reports `false` and
/// keeps its terminal state (no `revokedAt`).
#[tokio::test]
async fn revoke_leaves_an_expired_invite_expired() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let created = f.create_invite(Some(3600)).await;
    let mut expired = f
        .store
        .get_workspace_invite(&id_of(&created))
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

    let revoked = with_caller(
        wire(&f.owner),
        f.services.workspace_invite_revoke_op(&f.ws, "expired"),
    )
    .await
    .expect("revoke expired");
    assert_eq!(revoked, json!({ "revoked": false }));
    let stored = f
        .store
        .get_workspace_invite("expired")
        .await
        .expect("get")
        .expect("row");
    assert!(stored.revoked_at.is_none());
    assert_eq!(
        closed_kind(&stored, &now_iso()),
        Some(InviteErrorKind::Expired)
    );
}

/// Stand-in for the transport's link builder: `Some` formats
/// `stub://<inviteId>/<secret>`, `None` models a listener nobody can dial.
struct StubLinks {
    envelope: bool,
    resolves: std::sync::atomic::AtomicUsize,
}

struct StubEnvelope;

impl intent_core::InviteLinkEnvelope for StubEnvelope {
    fn invite_url(&self, invite_id: &str, secret: &str) -> String {
        format!("stub://{invite_id}/{secret}")
    }
}

impl intent_core::InviteLinkBuilder for StubLinks {
    fn invite_link_envelope(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = intent_core::ResolvedInviteLinkEnvelope> + Send + '_>,
    > {
        self.resolves
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let envelope = self.envelope;
        Box::pin(async move {
            envelope.then(|| Box::new(StubEnvelope) as Box<dyn intent_core::InviteLinkEnvelope>)
        })
    }
}

/// Every wire form of an invite: no `secret` / `secretHash` key, and the raw
/// secret nowhere but inside `url` (when present).
fn assert_secret_hidden(wire: &Value, secret: &str) {
    assert!(wire.get("secret").is_none(), "{wire}");
    assert!(wire.get("secretHash").is_none(), "{wire}");
    let mut without_url = wire.clone();
    without_url.as_object_mut().expect("object").remove("url");
    let text = serde_json::to_string(&without_url).expect("json");
    assert!(!text.contains(secret), "raw secret leaked: {text}");
    assert!(!text.contains(&hash_secret(secret)), "hash leaked: {text}");
}

/// The mint stores the secret and, with a link builder attached, every
/// `list` row carries `url` rebuilt from it — the envelope resolved once per
/// list, not per row, and never by `create` (the transport resolves the
/// create envelope itself and stamps both `url`s from it); a row minted
/// before the secret was stored gets no `url`; the secret itself never
/// serialises.
#[tokio::test]
async fn invite_list_rebuilds_the_link_from_the_stored_secret() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let links = Arc::new(StubLinks {
        envelope: true,
        resolves: std::sync::atomic::AtomicUsize::new(0),
    });
    f.services.attach_invite_link_builder(links.clone());

    let created = f.create_invite(Some(3600)).await;
    let id = id_of(&created);
    let secret = created["secret"].as_str().expect("secret").to_string();
    let expected_url = format!("stub://{id}/{secret}");
    assert_eq!(
        links.resolves.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "create never resolves the envelope through the builder"
    );
    assert!(
        created["invite"].get("url").is_none(),
        "create leaves url to the transport: {created}"
    );
    assert_secret_hidden(&created["invite"], &secret);
    let stored = f
        .store
        .get_workspace_invite(&id)
        .await
        .expect("get")
        .expect("row");
    assert_eq!(stored.secret.as_deref(), Some(secret.as_str()));
    assert_eq!(stored.secret_hash, hash_secret(&secret));

    // A second open invite plus a pre-0128 row (no stored secret).
    let created_2 = f.create_invite(Some(3600)).await;
    let id_2 = id_of(&created_2);
    let secret_2 = created_2["secret"].as_str().expect("secret").to_string();
    let legacy_secret = "f".repeat(64);
    let legacy = WorkspaceInvite {
        id: "inv-legacy".to_string(),
        workspace_id: f.ws.clone(),
        secret_hash: hash_secret(&legacy_secret),
        secret: None,
        created_by_principal_id: f.owner.clone(),
        pin_github_user_id: None,
        pin_login: None,
        created_at: now_iso(),
        expires_at: iso_after(60),
        redeemed_at: None,
        redeemed_by_principal_id: None,
        revoked_at: None,
    };
    f.store
        .insert_workspace_invite(&legacy)
        .await
        .expect("insert legacy");

    let before = links.resolves.load(std::sync::atomic::Ordering::SeqCst);
    let listed = with_caller(wire(&f.owner), f.services.workspace_invite_list_op(&f.ws))
        .await
        .expect("list");
    assert_eq!(
        links.resolves.load(std::sync::atomic::Ordering::SeqCst),
        before + 1,
        "one envelope resolve per list call"
    );
    let rows = listed["invites"].as_array().expect("invites");
    assert_eq!(rows.len(), 3);
    for row in rows {
        let row_id = row["id"].as_str().expect("id");
        match row_id {
            _ if row_id == id => {
                assert_eq!(row["url"], json!(expected_url));
                assert_secret_hidden(row, &secret);
            }
            _ if row_id == id_2 => {
                assert_eq!(row["url"], json!(format!("stub://{id_2}/{secret_2}")));
                assert_secret_hidden(row, &secret_2);
            }
            "inv-legacy" => {
                assert!(row.get("url").is_none(), "no secret, no url: {row}");
                assert_secret_hidden(row, &legacy_secret);
            }
            other => panic!("unexpected invite {other}"),
        }
    }
    let text = serde_json::to_string(&listed).expect("json");
    assert!(!text.contains("\"secret\""), "{text}");
}

/// Without a builder, or with one that cannot build a link right now
/// (listener down, no dialable route), `create` and `list` still answer —
/// their rows simply carry no `url`.
#[tokio::test]
async fn invite_list_omits_the_url_when_no_link_can_be_built() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let created = f.create_invite(Some(3600)).await;
    assert!(created["invite"].get("url").is_none(), "{created}");
    let listed = with_caller(wire(&f.owner), f.services.workspace_invite_list_op(&f.ws))
        .await
        .expect("list without builder");
    assert_eq!(listed["invites"].as_array().map(Vec::len), Some(1));
    assert!(listed["invites"][0].get("url").is_none(), "{listed}");

    f.services.attach_invite_link_builder(Arc::new(StubLinks {
        envelope: false,
        resolves: std::sync::atomic::AtomicUsize::new(0),
    }));
    let created = f.create_invite(Some(3600)).await;
    assert!(created["invite"].get("url").is_none(), "{created}");
    let secret = created["secret"].as_str().expect("secret");
    assert_secret_hidden(&created["invite"], secret);
    let listed = with_caller(wire(&f.owner), f.services.workspace_invite_list_op(&f.ws))
        .await
        .expect("list with an unresolvable envelope");
    let rows = listed["invites"].as_array().expect("invites");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.get("url").is_none()), "{listed}");
}

/// A waiter whose budget runs out while the poll task is committing the
/// join stays attached and collects the outcome: the grant is spent and the
/// credential stored, and this outcome is the only copy of the token.
#[tokio::test]
async fn timed_out_waiter_collects_a_committing_join() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let permit = f
        .services
        .invite_flow_permits
        .clone()
        .try_acquire_owned()
        .expect("permit");
    let (done, _) = watch::channel(false);
    let flow_id = "committing-flow".to_string();
    f.services.invite_flows.lock().await.insert(
        flow_id.clone(),
        InviteFlowSlot {
            invite_id: "invite".to_string(),
            deadline: Instant::now(),
            settled_at: None,
            committing: true,
            outcome: None,
            done,
            _permit: permit,
        },
    );
    let services = f.services.clone();
    let settle = tokio::spawn({
        let flow_id = flow_id.clone();
        async move {
            // Past the waiter's own budget (deadline + 5 s).
            tokio::time::sleep(Duration::from_millis(5_500)).await;
            let mut flows = services.invite_flows.lock().await;
            let slot = flows.get_mut(&flow_id).expect("slot kept while committing");
            slot.outcome = Some(Ok(json!({ "status": "authorized", "token": "t" })));
            slot.settled_at = Some(Instant::now());
            let _ = slot.done.send(true);
        }
    });
    let outcome = f
        .services
        .invite_redeem_wait_op(&flow_id)
        .await
        .expect("outcome collected after the commit");
    assert_eq!(outcome["token"], json!("t"));
    settle.await.expect("settle task");
    assert!(!f.services.invite_flows.lock().await.contains_key(&flow_id));
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

// --- returning guest: invite.inspect / invite.accept -----------------------

impl Fixture {
    /// A second workspace owned by the same owner, with one open invite on
    /// it — the link a returning guest inspects / accepts.
    async fn second_workspace_invite(&self) -> (WorkspaceId, String, String) {
        let ws = WorkspaceId::new();
        let mut row = workspace(&ws);
        row.title = "Second".to_string();
        self.store.insert_workspace(&row).await.expect("ws2");
        self.store
            .set_workspace_member_role(&ws, &self.primary, WorkspaceRole::Collaborator)
            .await
            .expect("demote primary on ws2");
        self.store
            .add_workspace_member(&ws, &self.owner, WorkspaceRole::Owner)
            .await
            .expect("owner of ws2");
        let created = with_caller(
            wire(&self.owner),
            self.services.workspace_invite_create_op(&ws, None, None),
        )
        .await
        .expect("create invite on ws2");
        let secret = created["secret"].as_str().expect("secret").to_string();
        (ws, id_of(&created), secret)
    }

    /// The device-flow join of `identity` on a fresh invite of the fixture
    /// workspace: the returning guest's first credential.
    async fn first_join(&self, user: &UserIdentity) -> (PrincipalId, String) {
        let created = self.create_invite(None).await;
        let joined = self
            .services
            .complete_invite_join(&id_of(&created), user)
            .await
            .expect("first join");
        (
            PrincipalId(joined["principalId"].as_str().expect("pid").to_string()),
            joined["token"].as_str().expect("token").to_string(),
        )
    }
}

/// `invite.inspect` answers the workspace hint for an open link with the
/// same refusals as a redeem start — and takes no flow permit, so a
/// daemon whose flow capacity is fully spent still answers it.
#[tokio::test]
async fn inspect_previews_an_open_invite_without_a_flow() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let (ws2, invite_id, secret) = f.second_workspace_invite().await;

    let permits_before = f.services.invite_flow_permits.available_permits();
    let r = f
        .services
        .invite_inspect_op(&invite_id, &secret)
        .await
        .expect("inspect");
    assert_eq!(
        r,
        json!({ "workspaceId": ws2.0, "workspaceTitle": "Second" }),
        "{r}"
    );
    assert_eq!(
        f.services.invite_flow_permits.available_permits(),
        permits_before,
        "inspect never touches the flow permits"
    );
    assert!(f.services.invite_flows.lock().await.is_empty());

    // Spent flow capacity does not affect inspect.
    let _all: Vec<_> = (0..permits_before)
        .map(|_| {
            f.services
                .invite_flow_permits
                .clone()
                .try_acquire_owned()
                .expect("permit")
        })
        .collect();
    assert_eq!(f.services.invite_flow_permits.available_permits(), 0);
    f.services
        .invite_inspect_op(&invite_id, &secret)
        .await
        .expect("inspect with no flow capacity");

    // Same refusals as a redeem start; the invite stays open throughout.
    let r = f.services.invite_inspect_op("missing", &secret).await;
    assert_eq!(invite_kind(&r), InviteErrorKind::NotFound);
    let r = f.services.invite_inspect_op(&invite_id, "wrong").await;
    assert_eq!(invite_kind(&r), InviteErrorKind::NotFound);
    f.store
        .revoke_workspace_invite(&invite_id)
        .await
        .expect("revoke");
    let r = f.services.invite_inspect_op(&invite_id, &secret).await;
    assert_eq!(invite_kind(&r), InviteErrorKind::Revoked);
}

/// `invite.accept` with the credential a prior redeem minted: the same
/// principal joins the second workspace as a collaborator, a fresh
/// credential comes back (the earlier one is untouched), the invite is
/// redeemed by that principal, the stored profile is not refreshed, no
/// forge is consulted and the member event names the principal.
#[tokio::test]
async fn accept_joins_a_returning_guest_with_its_credential() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    let bus = crate::events::EventBus::new(f.store.clone());
    f.services = f.services.with_event_bus(bus.clone());
    let (guest, first_token) = f.first_join(&identity("guest", 4242)).await;
    let (ws2, invite_id, secret) = f.second_workspace_invite().await;
    let mut events = bus.subscribe(crate::events::SubscriptionFilter {
        event_types: vec!["workspace:updated".into()],
        workspace_id: Some(ws2.0.clone()),
        ..Default::default()
    });

    let r = f
        .services
        .invite_accept_op(&invite_id, &secret, &first_token)
        .await
        .expect("accept");
    assert_eq!(r["status"], json!("authorized"));
    assert_eq!(r["principalId"], json!(guest.0));
    assert_eq!(r["login"], json!("guest"));
    assert_eq!(r["workspaceId"], json!(ws2.0));
    let second_token = r["token"].as_str().expect("token").to_string();
    assert_eq!(second_token.len(), 64);
    assert_ne!(second_token, first_token);
    assert!(
        r.get("hostname").is_none(),
        "decoration is the transport's: {r}"
    );

    assert_eq!(
        f.store
            .get_workspace_member_role(&ws2, &guest)
            .await
            .expect("role"),
        Some(WorkspaceRole::Collaborator)
    );
    // The presented credential is rotated out in the join transaction: the
    // guest leaves with exactly one active credential for this host.
    for (token, active) in [(&first_token, false), (&second_token, true)] {
        let cred = f
            .store
            .lookup_principal_credential(&hash_secret(token))
            .await
            .expect("lookup")
            .expect("credential row");
        assert_eq!(cred.principal_id, guest);
        assert_eq!(cred.is_active(), active, "rotation: {token}");
    }
    assert_eq!(
        f.store
            .list_principal_credentials(&guest)
            .await
            .expect("credentials")
            .iter()
            .filter(|c| c.is_active())
            .count(),
        1
    );
    let invite = f
        .store
        .get_workspace_invite(&invite_id)
        .await
        .expect("get")
        .expect("row");
    assert_eq!(invite.redeemed_by_principal_id, Some(guest.clone()));
    let row = f.store.get_principal(&guest).await.expect("principal");
    assert_eq!(row.github_user_id, Some(4242));
    assert_eq!(row.login.as_deref(), Some("guest"));
    assert_eq!(
        f.store.count_principals().await.expect("count"),
        4,
        "no new principal row"
    );
    assert!(f.services.invite_flows.lock().await.is_empty());

    let batch = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("event in time")
        .expect("event batch");
    let ev = batch
        .iter()
        .find(|e| e.data["changes"]["addedPrincipalId"] == json!(guest.0))
        .unwrap_or_else(|| panic!("member event: {batch:?}"));
    assert_eq!(ev.event_type, "workspace:updated");
    assert_eq!(ev.data["workspaceId"], json!(ws2.0));
    assert_eq!(ev.data["changes"]["members"], json!(true));
    // owner + the demoted primary + the guest
    assert_eq!(ev.data["changes"]["memberCount"], json!(3));

    // Single use, like the device-flow join (with the live credential; the
    // rotated-out one is refused before the invite is even looked at).
    let again = f
        .services
        .invite_accept_op(&invite_id, &secret, &second_token)
        .await;
    assert_eq!(invite_kind(&again), InviteErrorKind::Redeemed);
    let stale = f
        .services
        .invite_accept_op(&invite_id, &secret, &first_token)
        .await;
    assert_eq!(invite_kind(&stale), InviteErrorKind::CredentialInvalid);

    // `revokeSelf` sees the one active credential the rotation left.
    let revoked = with_caller(wire(&guest), f.services.principal_revoke_self_op())
        .await
        .expect("revoke self");
    assert_eq!(revoked["credentials"], json!(1), "{revoked}");
}

/// `invite.accept` refusals: an unknown or revoked credential is
/// `CredentialInvalid` (checked before the invite, so a bad credential on a
/// bad link is still `CredentialInvalid`); a credential of a principal
/// without a GitHub identity is refused the same way; a wrong secret is
/// `NotFound`; a closed invite is its kind; a pin to another account is
/// `PinMismatch`. None of them writes anything: the invite stays open.
#[tokio::test]
async fn accept_refuses_bad_credentials_closed_invites_and_pins() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let (guest, token) = f.first_join(&identity("guest", 4242)).await;
    let (ws2, invite_id, secret) = f.second_workspace_invite().await;

    let r = f
        .services
        .invite_accept_op(&invite_id, &secret, "not-a-credential")
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::CredentialInvalid);
    let r = f
        .services
        .invite_accept_op("missing", "wrong", "not-a-credential")
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::CredentialInvalid);

    // A principal with no GitHub identity cannot join by invite.
    let anonymous = principal("anon", None);
    f.store.upsert_principal(&anonymous).await.expect("anon");
    f.store
        .insert_principal_credential(&anonymous.id, &hash_secret("anon-token"))
        .await
        .expect("anon credential");
    let r = f
        .services
        .invite_accept_op(&invite_id, &secret, "anon-token")
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::CredentialInvalid);

    let r = f
        .services
        .invite_accept_op(&invite_id, "wrong", &token)
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::NotFound);

    // Pinned to another account: the stored github_user_id decides.
    let mut pinned = f
        .store
        .get_workspace_invite(&invite_id)
        .await
        .expect("get")
        .expect("row");
    pinned.id = "pinned".to_string();
    pinned.secret_hash = hash_secret("pinned-secret");
    pinned.pin_github_user_id = Some(5555);
    pinned.pin_login = Some("someone-else".to_string());
    f.store
        .insert_workspace_invite(&pinned)
        .await
        .expect("insert pinned");
    let r = f
        .services
        .invite_accept_op("pinned", "pinned-secret", &token)
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

    // Revoked credential: the token that joined once no longer identifies.
    f.store
        .revoke_all_principal_credentials(&guest)
        .await
        .expect("revoke credentials");
    let r = f
        .services
        .invite_accept_op(&invite_id, &secret, &token)
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::CredentialInvalid);
    assert_eq!(
        f.store
            .get_workspace_member_role(&ws2, &guest)
            .await
            .expect("role"),
        None,
        "nothing joined"
    );
    assert!(f
        .store
        .get_workspace_invite(&invite_id)
        .await
        .expect("get")
        .expect("row")
        .redeemed_at
        .is_none());

    // Closed invite with a (fresh) valid credential.
    f.store
        .insert_principal_credential(&guest, &hash_secret("fresh"))
        .await
        .expect("fresh credential");
    f.store
        .revoke_workspace_invite(&invite_id)
        .await
        .expect("revoke invite");
    let r = f
        .services
        .invite_accept_op(&invite_id, &secret, "fresh")
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::Revoked);
}

// --- gist identity proof: invite.challenge / invite.prove -------------------

/// A gist view created "now" (well after any nonce issued in the test),
/// owned by `owner` and carrying `first_line` as the proof file's first line.
fn gist(owner: &str, first_line: Option<&str>) -> ProofGistView {
    ProofGistView {
        owner_login: owner.to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        proof_first_line: first_line.map(str::to_string),
    }
}

/// The fixture with a scripted forge: `guest` (id 4242) and `other` (id
/// 5555) resolve by login; the proof gists are the caller's.
fn with_forge(f: &mut Fixture, gists: Vec<(&str, std::result::Result<ProofGistView, String>)>) {
    let mut forge = StubForge::default();
    for (login, id) in [("guest", 4242), ("other", 5555)] {
        forge
            .users_by_login
            .insert(login.to_string(), identity(login, id));
    }
    for (id, view) in gists {
        forge.proof_gists.insert(id.to_string(), view);
    }
    f.services = f.services.clone().with_source_control(Arc::new(forge));
}

fn nonce_of(challenge: &Value) -> String {
    challenge["nonce"].as_str().expect("nonce").to_string()
}

/// `invite.challenge` answers the inspect payload plus a fresh nonce bound to
/// the invite: 32 random bytes as unpadded base64url, expiring in
/// `NONCE_TTL`, distinct per call, with no flow slot taken and the forge
/// never contacted. A closed invite is its kind and issues nothing.
#[tokio::test]
async fn challenge_issues_a_nonce_without_a_flow() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let created = f.create_invite(None).await;
    let (invite_id, secret) = (
        id_of(&created),
        created["secret"].as_str().unwrap().to_string(),
    );

    let before = now_iso();
    let a = f
        .services
        .invite_challenge_op(&invite_id, &secret)
        .await
        .expect("challenge");
    let b = f
        .services
        .invite_challenge_op(&invite_id, &secret)
        .await
        .expect("challenge");
    assert_eq!(a["workspaceId"], json!(f.ws.0));
    assert_eq!(a["workspaceTitle"], json!("WS"));
    assert!(a.get("hostname").is_none(), "decoration is the transport's");
    assert!(a.get("flowId").is_none() && a.get("userCode").is_none());
    let nonce = nonce_of(&a);
    assert_eq!(nonce.len(), 43, "32 bytes base64url unpadded: {nonce}");
    assert!(nonce
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    assert_ne!(nonce, nonce_of(&b));
    let expires = a["nonceExpiresAt"].as_str().expect("nonceExpiresAt");
    assert!(expires > before.as_str(), "{expires} > {before}");
    assert!(expires < iso_after(NONCE_TTL.as_secs() + 5).as_str());
    {
        let nonces = f.services.invite_nonces.lock().await;
        assert_eq!(nonces.len(), 2);
        assert!(nonces.values().all(|s| s.invite_id == invite_id));
    }
    assert!(f.services.invite_flows.lock().await.is_empty());
    assert_eq!(
        f.services.invite_flow_permits.available_permits(),
        MAX_INFLIGHT_INVITE_FLOWS
    );

    let r = f.services.invite_challenge_op(&invite_id, "wrong").await;
    assert_eq!(invite_kind(&r), InviteErrorKind::NotFound);
    f.store
        .revoke_workspace_invite(&invite_id)
        .await
        .expect("revoke");
    let r = f.services.invite_challenge_op(&invite_id, &secret).await;
    assert_eq!(invite_kind(&r), InviteErrorKind::Revoked);
    assert_eq!(f.services.invite_nonces.lock().await.len(), 2);
}

/// One invite may hold at most `MAX_NONCES_PER_INVITE` outstanding nonces
/// (`FlowBusy` past it); expired ones are purged on the next challenge and
/// return their permits.
#[tokio::test]
async fn challenge_bounds_outstanding_nonces_per_invite() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let created = f.create_invite(None).await;
    let (invite_id, secret) = (
        id_of(&created),
        created["secret"].as_str().unwrap().to_string(),
    );
    for _ in 0..MAX_NONCES_PER_INVITE {
        f.services
            .invite_challenge_op(&invite_id, &secret)
            .await
            .expect("challenge");
    }
    let r = f.services.invite_challenge_op(&invite_id, &secret).await;
    assert_eq!(invite_kind(&r), InviteErrorKind::FlowBusy);
    assert_eq!(
        f.services.invite_nonce_permits.available_permits(),
        MAX_OUTSTANDING_NONCES - MAX_NONCES_PER_INVITE
    );
    // Another invite is not affected by this one's bound.
    let (_, other_id, other_secret) = f.second_workspace_invite().await;
    f.services
        .invite_challenge_op(&other_id, &other_secret)
        .await
        .expect("other invite still challenges");

    // Age every nonce past its lifetime: the next challenge purges them.
    for slot in f.services.invite_nonces.lock().await.values_mut() {
        slot.expires_at = Instant::now() - Duration::from_secs(1);
    }
    f.services
        .invite_challenge_op(&invite_id, &secret)
        .await
        .expect("challenge after purge");
    assert_eq!(f.services.invite_nonces.lock().await.len(), 1);
    assert_eq!(
        f.services.invite_nonce_permits.available_permits(),
        MAX_OUTSTANDING_NONCES - 1
    );
}

/// The happy path: a gist owned by the claimed login (case-insensitively),
/// whose proof file starts with the nonce and which postdates the nonce,
/// mints the principal, membership and credential exactly like the device
/// flow, consumes the nonce and publishes the member event. The gist is
/// read exactly once.
#[tokio::test]
async fn prove_joins_on_a_matching_gist_and_consumes_the_nonce() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    let bus = crate::events::EventBus::new(f.store.clone());
    f.services = f.services.with_event_bus(bus.clone());
    let created = f.create_invite(None).await;
    let (invite_id, secret) = (
        id_of(&created),
        created["secret"].as_str().unwrap().to_string(),
    );
    let challenge = f
        .services
        .invite_challenge_op(&invite_id, &secret)
        .await
        .expect("challenge");
    let nonce = nonce_of(&challenge);
    with_forge(&mut f, vec![("g1", Ok(gist("Guest", Some(&nonce))))]);
    let mut events = bus.subscribe(crate::events::SubscriptionFilter {
        event_types: vec!["workspace:updated".into()],
        workspace_id: Some(f.ws.0.clone()),
        ..Default::default()
    });

    let r = f
        .services
        .invite_prove_op(&invite_id, &secret, &nonce, "g1", "guest")
        .await
        .expect("prove");
    assert_eq!(r["status"], json!("authorized"));
    assert_eq!(r["login"], json!("guest"));
    assert_eq!(r["workspaceId"], json!(f.ws.0));
    let token = r["token"].as_str().expect("token");
    assert_eq!(token.len(), 64);
    let guest = PrincipalId(r["principalId"].as_str().expect("pid").to_string());
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
        .expect("credential row");
    assert_eq!(cred.principal_id, guest);
    let row = f.store.get_principal(&guest).await.expect("principal");
    assert_eq!(row.github_user_id, Some(4242));
    assert_eq!(row.login.as_deref(), Some("guest"));
    assert_eq!(row.display_name.as_deref(), Some("guest name"));
    assert!(f.services.invite_nonces.lock().await.is_empty(), "consumed");
    assert_eq!(
        f.services.invite_nonce_permits.available_permits(),
        MAX_OUTSTANDING_NONCES
    );
    assert!(f.services.invite_flows.lock().await.is_empty());
    let invite = f
        .store
        .get_workspace_invite(&invite_id)
        .await
        .expect("get")
        .expect("row");
    assert_eq!(invite.redeemed_by_principal_id, Some(guest.clone()));

    let batch = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("event in time")
        .expect("event batch");
    assert!(
        batch
            .iter()
            .any(|e| e.data["changes"]["addedPrincipalId"] == json!(guest.0)),
        "member event: {batch:?}"
    );

    // Spent nonce: the same proof does not join again.
    let again = f
        .services
        .invite_prove_op(&invite_id, &secret, &nonce, "g1", "guest")
        .await;
    assert_eq!(invite_kind(&again), InviteErrorKind::Redeemed);
}

/// Every `ProofInvalid` branch: a gist of another owner, a missing proof
/// file, a first line that is not the nonce, a gist created before the
/// nonce, an unknown gist, a nonce this host never issued, a nonce issued
/// for another invite, a login GitHub does not know — and each attempt
/// consumes the nonce it named (a later, otherwise-valid proof on the same
/// nonce is refused too). Nothing joins; the invite stays open.
#[tokio::test]
async fn prove_refuses_mismatched_gists_and_spends_the_nonce() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    let created = f.create_invite(None).await;
    let (invite_id, secret) = (
        id_of(&created),
        created["secret"].as_str().unwrap().to_string(),
    );
    let (_, other_invite, other_secret) = f.second_workspace_invite().await;
    let challenge = |f: &Fixture, id: &str, s: &str| {
        let (services, id, s) = (f.services.clone(), id.to_string(), s.to_string());
        async move {
            nonce_of(
                &services
                    .invite_challenge_op(&id, &s)
                    .await
                    .expect("challenge"),
            )
        }
    };
    let n = challenge(&f, &invite_id, &secret).await;
    let stale = ProofGistView {
        owner_login: "guest".to_string(),
        created_at: "2020-01-01T00:00:00Z".to_string(),
        proof_first_line: Some(n.clone()),
    };
    with_forge(
        &mut f,
        vec![
            ("wrongowner", Ok(gist("other", Some(&n)))),
            ("nofile", Ok(gist("guest", None))),
            ("wrongline", Ok(gist("guest", Some("not-the-nonce")))),
            ("stale", Ok(stale)),
            ("ok", Ok(gist("guest", Some(&n)))),
            ("unknownlogin", Ok(gist("nobody", Some(&n)))),
        ],
    );
    let prove = |f: &Fixture, nonce: &str, gist_id: &str, login: &str| {
        let services = f.services.clone();
        let (invite_id, secret) = (invite_id.clone(), secret.clone());
        let (nonce, gist_id, login) = (nonce.to_string(), gist_id.to_string(), login.to_string());
        async move {
            services
                .invite_prove_op(&invite_id, &secret, &nonce, &gist_id, &login)
                .await
        }
    };

    for (gist_id, login) in [
        ("wrongowner", "guest"),
        ("nofile", "guest"),
        ("wrongline", "guest"),
        ("stale", "guest"),
        ("missing", "guest"),
        ("unknownlogin", "nobody"),
    ] {
        let n = challenge(&f, &invite_id, &secret).await;
        let r = prove(&f, &n, gist_id, login).await;
        assert_eq!(invite_kind(&r), InviteErrorKind::ProofInvalid, "{gist_id}");
        // The nonce is spent by the attempt: a matching gist is now too late.
        let again = prove(&f, &n, "ok", "guest").await;
        assert_eq!(
            invite_kind(&again),
            InviteErrorKind::ProofInvalid,
            "{gist_id} retry"
        );
    }
    // Never issued / issued for another invite.
    let r = prove(&f, "never-issued", "ok", "guest").await;
    assert_eq!(invite_kind(&r), InviteErrorKind::ProofInvalid);
    let foreign = challenge(&f, &other_invite, &other_secret).await;
    let r = prove(&f, &foreign, "ok", "guest").await;
    assert_eq!(invite_kind(&r), InviteErrorKind::ProofInvalid);
    // A closed invite is its kind, checked before the nonce is touched.
    let n2 = challenge(&f, &invite_id, &secret).await;
    with_forge(&mut f, vec![("ok2", Ok(gist("guest", Some(&n2))))]);
    let r = prove(&f, &n2, "ok2", "guest").await.map(|_| ());
    assert!(r.is_ok(), "{r:?}");
    let r = prove(&f, &n, "ok", "guest").await;
    assert_eq!(invite_kind(&r), InviteErrorKind::Redeemed);
    // Malformed claims are refused up front.
    let r = prove(&f, &n, "ok", "not a login!").await;
    assert!(matches!(r, Err(Error::InvalidParams(_))), "{r:?}");
    let r = prove(&f, &n, "../x", "guest").await;
    assert!(matches!(r, Err(Error::InvalidParams(_))), "{r:?}");
    // Only `n` is left: refusals before the nonce lookup (closed invite,
    // malformed claim) leave it outstanding until its TTL.
    let nonces = f.services.invite_nonces.lock().await;
    assert_eq!(nonces.keys().collect::<Vec<_>>(), vec![&n]);
}

/// An expired nonce is `ProofExpired` (and spent); GitHub failing is
/// `GithubUnreachable` and puts the nonce back so the guest can retry.
#[tokio::test]
async fn prove_reports_expiry_and_unreachable_github() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    let created = f.create_invite(None).await;
    let (invite_id, secret) = (
        id_of(&created),
        created["secret"].as_str().unwrap().to_string(),
    );
    let n = nonce_of(
        &f.services
            .invite_challenge_op(&invite_id, &secret)
            .await
            .expect("challenge"),
    );
    with_forge(
        &mut f,
        vec![
            ("ok", Ok(gist("guest", Some(&n)))),
            ("down", Err("bad gateway".to_string())),
        ],
    );

    // Unreachable: the nonce survives for a retry with the same proof.
    let r = f
        .services
        .invite_prove_op(&invite_id, &secret, &n, "down", "guest")
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubUnreachable);
    assert_eq!(f.services.invite_nonces.lock().await.len(), 1);
    // Unreachable on the account lookup as well (unscripted login →
    // `Unsupported` stands in for a transport failure).
    let mut forge = StubForge::default();
    forge
        .proof_gists
        .insert("ok".into(), Ok(gist("guest", Some(&n))));
    f.services = f.services.clone().with_source_control(Arc::new(forge));
    let r = f
        .services
        .invite_prove_op(&invite_id, &secret, &n, "ok", "guest")
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GithubUnreachable);
    assert_eq!(f.services.invite_nonces.lock().await.len(), 1);
    with_forge(&mut f, vec![("ok", Ok(gist("guest", Some(&n))))]);

    // Expired: spent, and no forge call is made.
    for slot in f.services.invite_nonces.lock().await.values_mut() {
        slot.expires_at = Instant::now() - Duration::from_secs(1);
    }
    let r = f
        .services
        .invite_prove_op(&invite_id, &secret, &n, "ok", "guest")
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::ProofExpired);
    assert!(f.services.invite_nonces.lock().await.is_empty());
    let r = f
        .services
        .invite_prove_op(&invite_id, &secret, &n, "ok", "guest")
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::ProofInvalid, "spent");
    assert!(
        f.store
            .get_workspace_invite(&invite_id)
            .await
            .expect("get")
            .expect("row")
            .redeemed_at
            .is_none(),
        "the invite stays open"
    );
}

/// A pinned invite still enforces its pin on the proven account, and the
/// pin is checked against `GET /users/{login}`'s id, not the claim.
#[tokio::test]
async fn prove_enforces_the_pin_on_the_proven_account() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    with_forge(&mut f, vec![]);
    let created = with_caller(
        wire(&f.owner),
        f.services
            .workspace_invite_create_op(&f.ws, Some("other".into()), None),
    )
    .await
    .expect("pinned invite");
    let (invite_id, secret) = (
        id_of(&created),
        created["secret"].as_str().unwrap().to_string(),
    );
    let n = nonce_of(
        &f.services
            .invite_challenge_op(&invite_id, &secret)
            .await
            .expect("challenge"),
    );
    with_forge(&mut f, vec![("g", Ok(gist("guest", Some(&n))))]);
    let r = f
        .services
        .invite_prove_op(&invite_id, &secret, &n, "g", "guest")
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::PinMismatch);
}

/// Under concurrent `invite.prove` attempts on one nonce exactly one reaches
/// the verification (and joins); every other one is refused — `ProofInvalid`
/// when it lost the nonce race, `Redeemed` when it arrived after the join.
#[tokio::test]
async fn prove_consumes_the_nonce_exactly_once_under_concurrency() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    let created = f.create_invite(None).await;
    let (invite_id, secret) = (
        id_of(&created),
        created["secret"].as_str().unwrap().to_string(),
    );
    let n = nonce_of(
        &f.services
            .invite_challenge_op(&invite_id, &secret)
            .await
            .expect("challenge"),
    );
    let mut forge = StubForge::default();
    forge
        .users_by_login
        .insert("guest".into(), identity("guest", 4242));
    forge
        .proof_gists
        .insert("g".into(), Ok(gist("guest", Some(&n))));
    let forge = Arc::new(forge);
    f.services = f.services.clone().with_source_control(forge.clone());

    let mut handles = Vec::new();
    for _ in 0..16 {
        let services = f.services.clone();
        let (invite_id, secret, n) = (invite_id.clone(), secret.clone(), n.clone());
        handles.push(tokio::spawn(async move {
            services
                .invite_prove_op(&invite_id, &secret, &n, "g", "guest")
                .await
        }));
    }
    let (mut joined, mut refused) = (0, 0);
    for h in handles {
        match h.await.expect("task") {
            Ok(v) => {
                assert_eq!(v["status"], json!("authorized"));
                joined += 1;
            }
            Err(Error::Invite(InviteErrorKind::ProofInvalid | InviteErrorKind::Redeemed)) => {
                refused += 1;
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
    assert_eq!((joined, refused), (1, 15));
    assert_eq!(
        forge.seen_proof_gists.lock().unwrap().len(),
        1,
        "the gist is read by the one attempt that held the nonce"
    );
}

#[test]
fn proof_time_and_login_rules() {
    let issued = SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_500);
    assert!(
        gist_created_after("2023-11-14T22:13:20Z", issued),
        "same second"
    );
    assert!(gist_created_after("2023-11-14T22:13:21Z", issued));
    assert!(!gist_created_after("2023-11-14T22:13:19Z", issued));
    assert!(!gist_created_after("yesterday", issued));
    assert!(valid_login("octocat") && valid_login("a-b-1"));
    assert!(!valid_login("") && !valid_login("a/b") && !valid_login(&"x".repeat(40)));
    assert_eq!(random_nonce().len(), 43);
    assert_ne!(random_nonce(), random_nonce());
}

// --- guest caps ------------------------------------------------------------

/// The fixture wired to a settings registry with
/// `sharing.maxGuestsPerWorkspace = cap`; the registry is returned so a
/// test can move the cap while the daemon runs (the ops read it live).
async fn capped_fixture(
    tmp: &TempDb,
    cap: u32,
) -> (Fixture, Arc<crate::SettingsRegistry>, tempfile::TempDir) {
    let cfg_dir = crate::test_support::test_tempdir("intentd-guest-caps");
    let registry = Arc::new(
        crate::SettingsRegistry::load(cfg_dir.path().join("config.toml")).expect("load registry"),
    );
    set_cap(&registry, cap);
    let mut f = fixture(tmp).await;
    f.services = Services::new(f.store.clone()).with_settings_registry(registry.clone());
    (f, registry, cfg_dir)
}

fn set_cap(registry: &crate::SettingsRegistry, cap: u32) {
    registry
        .apply(&[("sharing.maxGuestsPerWorkspace".to_string(), json!(cap))])
        .expect("apply cap");
}

async fn guest_summary(f: &Fixture) -> (u64, u64) {
    let v = with_caller(wire(&f.owner), f.services.workspace_members_list_op(&f.ws))
        .await
        .expect("members.list");
    (
        v["guestCount"].as_u64().expect("guestCount"),
        v["guestLimit"].as_u64().expect("guestLimit"),
    )
}

/// `workspace.invite.create` spends the cap on collaborators PLUS open
/// invites (the fixture seats two collaborators): at the cap it is
/// `GuestLimit`, revoking an invite frees the seat, lowering the cap live
/// applies to the next mint, cap `0` closes a fresh workspace to guests,
/// and `members.list` reports the same count / limit.
#[tokio::test]
async fn invite_create_refuses_at_the_guest_limit() {
    let tmp = TempDb::new();
    let (f, registry, _cfg) = capped_fixture(&tmp, 3).await;
    assert_eq!(guest_summary(&f).await, (2, 3));

    let created = f.create_invite(None).await;
    assert_eq!(guest_summary(&f).await, (3, 3));
    let r = with_caller(
        wire(&f.owner),
        f.services.workspace_invite_create_op(&f.ws, None, None),
    )
    .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GuestLimit);
    assert_eq!(
        f.store
            .list_open_workspace_invites(&f.ws)
            .await
            .expect("list")
            .len(),
        1,
        "a refused mint leaves no row"
    );

    with_caller(
        wire(&f.owner),
        f.services
            .workspace_invite_revoke_op(&f.ws, &id_of(&created)),
    )
    .await
    .expect("revoke");
    assert_eq!(guest_summary(&f).await, (2, 3));
    f.create_invite(None).await;

    // Live cap change: two collaborators already fill a cap of 2.
    set_cap(&registry, 2);
    assert_eq!(guest_summary(&f).await, (3, 2));
    let r = with_caller(
        wire(&f.owner),
        f.services.workspace_invite_create_op(&f.ws, None, None),
    )
    .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GuestLimit);

    // Cap 0: even a workspace with no guest at all admits none. The owner
    // (GitHub-linked) owns it alone; the primary seat is removed.
    set_cap(&registry, 0);
    let empty = WorkspaceId::new();
    f.store
        .insert_workspace(&workspace(&empty))
        .await
        .expect("empty ws");
    let primary = f.store.get_primary_principal().await.expect("primary").id;
    f.store
        .remove_workspace_member(&empty, &primary)
        .await
        .expect("remove primary");
    f.store
        .add_workspace_member(&empty, &f.owner, WorkspaceRole::Owner)
        .await
        .expect("owner");
    assert_eq!(
        f.store
            .count_workspace_guests(&empty)
            .await
            .expect("count")
            .committed(),
        0
    );
    let r = with_caller(
        wire(&f.owner),
        f.services.workspace_invite_create_op(&empty, None, None),
    )
    .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::GuestLimit);
}

/// The join re-checks the cap against collaborators inside the store
/// transaction: an open invite minted under a higher cap is `WorkspaceFull`
/// once the cap drops to the seated count — nothing is written and the
/// invite stays open — an account that is already a member re-joins without
/// a seat, and the same invite admits a newcomer once the cap is raised.
#[tokio::test]
async fn complete_join_refuses_a_full_workspace_and_keeps_the_invite_open() {
    let tmp = TempDb::new();
    let (f, registry, _cfg) = capped_fixture(&tmp, 3).await;
    let id = id_of(&f.create_invite(None).await);
    let principals = f.store.count_principals().await.expect("count");

    set_cap(&registry, 2);
    let r = f
        .services
        .complete_invite_join(&id, &identity("newcomer", 7001))
        .await;
    assert_eq!(invite_kind(&r), InviteErrorKind::WorkspaceFull);
    assert_eq!(f.store.count_principals().await.expect("count"), principals);
    let invite = f
        .store
        .get_workspace_invite(&id)
        .await
        .expect("get")
        .expect("row");
    assert!(invite.redeemed_at.is_none(), "the invite stays open");

    // A seated collaborator (github 2002) needs no new seat.
    let rejoined = f
        .services
        .complete_invite_join(&id, &identity("collab", 2002))
        .await
        .expect("member re-joins under a full cap");
    assert_eq!(rejoined["principalId"], json!(f.collaborator.0));

    set_cap(&registry, 3);
    let id = id_of(&f.create_invite(None).await);
    let joined = f
        .services
        .complete_invite_join(&id, &identity("newcomer", 7001))
        .await
        .expect("join once the cap is raised");
    assert_eq!(joined["login"], json!("newcomer"));
    assert_eq!(guest_summary(&f).await, (3, 3));
}

/// Concurrent joins on distinct open invites for the last seat: exactly one
/// commits, the rest are `WorkspaceFull` with their invites still open.
#[tokio::test]
async fn concurrent_joins_cannot_overshoot_the_guest_cap() {
    let tmp = TempDb::new();
    let (f, registry, _cfg) = capped_fixture(&tmp, 5).await;
    let mut invite_ids = Vec::new();
    for _ in 0..3 {
        invite_ids.push(id_of(&f.create_invite(None).await));
    }
    set_cap(&registry, 3);

    let mut handles = Vec::new();
    for (n, id) in (9000u64..).zip(invite_ids) {
        let services = f.services.clone();
        handles.push(tokio::spawn(async move {
            services
                .complete_invite_join(&id, &identity(&format!("racer-{n}"), n))
                .await
        }));
    }
    let mut joined = 0;
    let mut full = 0;
    for h in handles {
        match h.await.expect("task") {
            Ok(_) => joined += 1,
            r => {
                assert_eq!(invite_kind(&r), InviteErrorKind::WorkspaceFull);
                full += 1;
            }
        }
    }
    assert_eq!((joined, full), (1, 2));
    assert_eq!(
        f.store
            .list_open_workspace_invites(&f.ws)
            .await
            .expect("list")
            .len(),
        2,
        "refused joins leave their invites open"
    );
    assert_eq!(guest_summary(&f).await, (5, 3));
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

/// The stamped type and id come from the caller binding alone: with a cold
/// name cache and the principal row unreadable, a bound collaborator's
/// event still carries `{ type: user, id: principalId }` (the id doubles
/// as the name) rather than the supplied System or other-user actor.
#[tokio::test]
async fn event_actor_is_still_the_bound_principal_when_the_row_is_unreadable() {
    let (_tmp, f, _) = attribution_fixture().await;
    let bus = crate::events::EventBus::new(f.store.clone());
    sqlx::query("ALTER TABLE principal RENAME TO unavailable_principals")
        .execute(f.store.write_pool())
        .await
        .expect("inject read failure");
    let event = |actor: intent_core::EventActor| intent_store::NewEvent {
        workspace_id: f.ws.clone(),
        timestamp: intent_core::now_iso(),
        event_type: "note:updated".to_string(),
        actor,
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({}),
    };
    let (system, other_user) = with_caller(wire(&f.collaborator), async {
        let system = bus
            .publish(&event(crate::system_actor()))
            .await
            .expect("publish system-actored event");
        let other_user = bus
            .publish(&event(intent_core::EventActor {
                actor_type: intent_core::ActorType::User,
                id: Some(f.primary.0.clone()),
                name: Some("owner".into()),
                ..Default::default()
            }))
            .await
            .expect("publish other-user event");
        (system, other_user)
    })
    .await;
    for published in [system, other_user] {
        assert_eq!(published.actor.actor_type, intent_core::ActorType::User);
        assert_eq!(published.actor.id.as_deref(), Some(f.collaborator.as_str()));
        assert_eq!(
            published.actor.name.as_deref(),
            Some(f.collaborator.as_str())
        );
    }
}

/// The administrator — the owner over UDS or its own wire credential — is a
/// bound principal like any other: its events carry `{ type: user, id:
/// primaryPrincipalId, name }`, with the display name when no GitHub
/// identity is attached.
#[tokio::test]
async fn event_actor_is_the_primary_for_the_administrator() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    f.services = f
        .services
        .with_event_bus(crate::events::EventBus::new(f.store.clone()));
    let mut primary = f.store.get_primary_principal().await.expect("primary");
    primary.login = None;
    primary.display_name = Some("Local owner".into());
    f.store.upsert_principal(&primary).await.expect("seed");
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
    assert_eq!(rows[0].actor.actor_type, intent_core::ActorType::User);
    assert_eq!(rows[0].actor.id.as_deref(), Some(f.primary.0.as_str()));
    assert_eq!(rows[0].actor.name.as_deref(), Some("Local owner"));
}

/// The administrator's comments are stamped with the primary's GitHub
/// login once one is attached; a single-user daemon that never connected
/// GitHub keeps the author its client supplied.
#[tokio::test]
async fn administrator_comment_author_follows_the_attached_identity() {
    let (_tmp, f, note_id) = attribution_fixture().await;
    let admin = || Caller::Wire {
        principal_id: f.primary.clone(),
        is_administrator: true,
    };
    let mut primary = f.store.get_primary_principal().await.expect("primary");
    primary.login = None;
    primary.github_user_id = None;
    f.store.upsert_principal(&primary).await.expect("seed");
    with_caller(
        admin(),
        f.services.comment_add(
            f.ws.clone(),
            note_id.clone(),
            "Probe anchor text".into(),
            "anchor".into(),
            "Legacy comment".into(),
            None,
            Some("Legacy Author".into()),
            Some("user".into()),
            None,
            None,
        ),
    )
    .await
    .expect("legacy comment");

    primary.login = Some("primary-gh".into());
    primary.github_user_id = Some(10);
    f.store.upsert_principal(&primary).await.expect("attach");
    with_caller(
        admin(),
        f.services.comment_add(
            f.ws.clone(),
            note_id.clone(),
            "Probe anchor text".into(),
            "anchor".into(),
            "Attached comment".into(),
            None,
            Some("Someone Else".into()),
            Some("agent".into()),
            None,
            None,
        ),
    )
    .await
    .expect("attached comment");

    let rows = f
        .store
        .list_comments_in_workspace(&f.ws, &note_id)
        .await
        .expect("comments");
    let by_text = |text: &str| {
        rows.iter()
            .find(|row| row.content == text)
            .unwrap_or_else(|| panic!("comment {text:?}"))
    };
    let legacy = by_text("Legacy comment");
    assert_eq!(legacy.author, "Legacy Author");
    assert_eq!(legacy.author_type, intent_core::AuthorType::User);
    let attached = by_text("Attached comment");
    assert_eq!(attached.author, "primary-gh");
    assert_eq!(attached.author_type, intent_core::AuthorType::User);
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
