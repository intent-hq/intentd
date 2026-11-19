//! Principals, credentials, workspace membership and invites, and host
//! membership on both backends.
//!
//! Bodies use fixed ids, leave rows behind and assert exact values from an
//! empty start; profiles carry the tenant (`alice-{tenant}`), so a row read
//! from another tenant shows. Ids the subject probes as absent are created
//! by the decoy after its own probes, and rows the subject's writes reach
//! are put back in their pre-write state at the end of the decoy run.

use intent_core::node_link::HomeStopReason;
use intent_core::{
    AgentId, AgentSession, Error, HomeId, HomeState, HostInvite, HostRole, Principal, PrincipalId,
    PrincipalIdentity, Workspace, WorkspaceHome, WorkspaceId, WorkspaceInvite, WorkspaceRole,
    WorkspaceStatus,
};
use intent_store::{
    AgentQueueRow, CollaboratorAddOutcome, HomeMove, HostInviteJoinOutcome, HostJoinCredential,
    InviteInsertOutcome, InviteJoinOutcome, Store,
};
use serde_json::json;

use crate::harness::{exec, open, parity_test, test_db, TestDb};

const T1: &str = "2026-02-01T00:00:01.000Z";
const T2: &str = "2026-02-01T00:00:02.000Z";
const T3: &str = "2026-02-01T00:00:03.000Z";
const T4: &str = "2026-02-01T00:00:04.000Z";
const FAR: &str = "2099-01-01T00:00:00.000Z";

fn pid(id: &str) -> PrincipalId {
    PrincipalId(id.into())
}

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn person(id: &str, identity: PrincipalIdentity, tenant: &str, created_at: &str) -> Principal {
    Principal {
        id: pid(id),
        github_user_id: identity.github_user_id(),
        identity: Some(identity),
        login: Some(format!("{id}-{tenant}")),
        display_name: Some(format!("{id} of {tenant}")),
        avatar_url: None,
        is_primary: false,
        created_at: created_at.into(),
        updated_at: created_at.into(),
    }
}

fn gh(n: i64) -> PrincipalIdentity {
    PrincipalIdentity::github(n)
}

fn ids(people: &[Principal]) -> Vec<&str> {
    people.iter().map(|p| p.id.as_str()).collect()
}

async fn workspace(store: &Store, id: &str, tenant: &str) {
    store
        .insert_workspace(&Workspace {
            id: wid(id),
            title: format!("{id} of {tenant}"),
            branch: "main".into(),
            created_at: T1.into(),
            updated_at: T1.into(),
            ..Default::default()
        })
        .await
        .unwrap();
}

/// Raw SQL on the decoy tenant only, for putting rows back in the state the
/// subject's writes expect. A no-op on `SQLite` and for the subject.
async fn decoy_sql(db: Option<&TestDb>, tenant: &str, sql: &str) {
    if let (Some(db), "decoy") = (db, tenant) {
        exec(&db.admin, sql.replace("{w}", "tenant_id = 'decoy'")).await;
    }
}

/// Make `p` a host member through a fresh host invite.
async fn host_member(store: &Store, owner: &PrincipalId, invite_id: &str, p: &Principal) {
    let identity = p.identity.clone().unwrap();
    let invite = HostInvite::new(
        invite_id.into(),
        owner.clone(),
        identity,
        p.login.clone().unwrap(),
        format!("secret-{invite_id}"),
        None,
    )
    .unwrap();
    store.insert_host_invite(&invite).await.unwrap();
    let generation = store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    let joined = store
        .join_host_by_invite(
            invite_id,
            p,
            HostJoinCredential::Proof {
                token_hash: &format!("h-{}", p.id),
                authorization_generation: generation,
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        joined,
        HostInviteJoinOutcome::Joined {
            membership_added: true,
            ..
        }
    ));
}

parity_test!(principals_and_credentials, |store, tenant, db| {
    let primary = store.get_primary_principal().await.unwrap();
    assert!(primary.is_primary);
    assert_eq!(store.count_principals().await.unwrap(), 1);
    assert_eq!(
        ids(&store.list_principals().await.unwrap()),
        [primary.id.as_str()]
    );

    let alice = person("alice", gh(101), tenant, T1);
    let bob = person(
        "bob",
        PrincipalIdentity {
            provider: "gitlab".into(),
            host: "gitlab.example".into(),
            external_user_id: "b7".into(),
        },
        tenant,
        T2,
    );
    let carol = person("carol", gh(103), tenant, T3);
    for p in [&alice, &bob, &carol] {
        store.upsert_principal(p).await.unwrap();
    }
    assert_eq!(store.get_principal(&pid("alice")).await.unwrap(), alice);
    assert!(matches!(
        store.get_principal(&pid("ghost")).await,
        Err(Error::NotFound(_))
    ));
    let mut some = store
        .get_principals(&[pid("bob"), pid("alice"), pid("ghost")])
        .await
        .unwrap();
    some.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    assert_eq!(some, [alice.clone(), bob.clone()]);
    assert_eq!(
        store
            .find_principal_by_identity(bob.identity.as_ref().unwrap())
            .await
            .unwrap(),
        Some(bob.clone())
    );
    assert_eq!(
        store.find_principal_by_github_user_id(101).await.unwrap(),
        Some(alice.clone())
    );
    assert_eq!(
        store.find_principal_by_github_user_id(999).await.unwrap(),
        None
    );

    let mut renamed = alice.clone();
    renamed.display_name = Some(format!("Alice R of {tenant}"));
    renamed.updated_at = T4.into();
    renamed.created_at = T4.into();
    renamed.is_primary = true;
    store.upsert_principal(&renamed).await.unwrap();
    let stored = store.get_principal(&pid("alice")).await.unwrap();
    assert_eq!(stored.display_name, renamed.display_name);
    assert_eq!(stored.updated_at, T4);
    assert_eq!(stored.created_at, T1);
    assert!(!stored.is_primary);
    assert_eq!(
        ids(&store.list_principals().await.unwrap()),
        [primary.id.as_str(), "alice", "bob", "carol"]
    );
    assert_eq!(store.count_principals().await.unwrap(), 4);

    let created = store
        .insert_principal_credential(&pid("alice"), "h-alice")
        .await
        .unwrap();
    assert!(created.is_active());
    store
        .insert_principal_credential(&pid("bob"), "h-bob")
        .await
        .unwrap();
    store
        .insert_principal_credential(&pid("bob"), "h-bob2")
        .await
        .unwrap();
    assert_eq!(
        store.lookup_principal_credential("h-alice").await.unwrap(),
        Some(created)
    );
    assert_eq!(
        store.lookup_principal_credential("h-ghost").await.unwrap(),
        None
    );
    assert_eq!(
        ids(&store.list_credentialed_guest_principals().await.unwrap()),
        ["alice", "bob"]
    );
    let sharing = store.list_sharing_principals().await.unwrap();
    assert_eq!(
        sharing
            .iter()
            .map(|(p, r)| (p.login.clone().unwrap(), *r))
            .collect::<Vec<_>>(),
        [
            (format!("alice-{tenant}"), HostRole::Guest),
            (format!("bob-{tenant}"), HostRole::Guest)
        ]
    );
    let probe = [primary.id.clone(), pid("alice"), pid("carol"), pid("ghost")];
    let mut devices = store.get_device_principals(&probe, None).await.unwrap();
    devices.sort_by_key(|(p, _)| !p.is_primary);
    devices[1..].sort_by(|a, b| a.0.id.0.cmp(&b.0.id.0));
    assert_eq!(
        devices
            .iter()
            .map(|(p, r)| (p.id.as_str(), *r))
            .collect::<Vec<_>>(),
        [
            (primary.id.as_str(), HostRole::Owner),
            ("alice", HostRole::Guest),
            ("carol", HostRole::Guest)
        ]
    );
    let seen_by_alice = store
        .get_device_principals(&probe, Some(&pid("alice")))
        .await
        .unwrap();
    assert_eq!(seen_by_alice.len(), 1);
    assert_eq!(seen_by_alice[0].0.id, pid("alice"));
    assert_eq!(
        store
            .get_device_principals(&probe, Some(&primary.id))
            .await
            .unwrap()
            .len(),
        3
    );

    assert!(store.touch_principal_credential("h-alice").await.unwrap());
    let touched = store
        .lookup_principal_credential("h-alice")
        .await
        .unwrap()
        .unwrap();
    assert!(touched.last_used_at.is_some());
    assert_eq!(
        store
            .resolve_active_principal_credential("h-bob")
            .await
            .unwrap(),
        Some(pid("bob"))
    );
    assert!(store.revoke_principal_credential("h-bob").await.unwrap());
    assert!(!store.revoke_principal_credential("h-bob").await.unwrap());
    assert_eq!(
        store
            .resolve_active_principal_credential("h-bob")
            .await
            .unwrap(),
        None
    );
    assert!(!store.touch_principal_credential("h-bob").await.unwrap());
    assert_eq!(
        store
            .revoke_all_principal_credentials(&pid("bob"))
            .await
            .unwrap(),
        1
    );
    let bobs = store.list_principal_credentials(&pid("bob")).await.unwrap();
    assert_eq!(
        bobs.iter()
            .map(|c| c.token_hash.as_str())
            .collect::<Vec<_>>(),
        ["h-bob2", "h-bob"]
    );
    assert!(bobs.iter().all(|c| c.revoked_at.is_some()));
    assert_eq!(
        ids(&store.list_credentialed_guest_principals().await.unwrap()),
        ["alice"]
    );

    if tenant == "decoy" {
        store
            .upsert_principal(&person("ghost", gh(999), tenant, T4))
            .await
            .unwrap();
        store
            .insert_principal_credential(&pid("ghost"), "h-ghost")
            .await
            .unwrap();
        decoy_sql(
            db,
            tenant,
            "UPDATE principal_credential SET revoked_at = NULL, last_used_at = NULL WHERE {w}; \
             INSERT INTO host_member (tenant_id, principal_id, added_at) \
             VALUES ('decoy', 'alice', '2026-02-01T00:00:05.000Z'), \
                    ('decoy', 'bob', '2026-02-01T00:00:05.000Z')",
        )
        .await;
    }
});

parity_test!(workspace_members_and_roles, |store, tenant, db| {
    let primary = store.get_primary_principal().await.unwrap().id;
    workspace(&store, "ws-a", tenant).await;
    workspace(&store, "ws-b", tenant).await;
    assert_eq!(
        store
            .get_workspace_owner_principal_id(&wid("ws-a"))
            .await
            .unwrap(),
        Some(primary.clone())
    );
    assert_eq!(
        store
            .get_workspace_owner_principal_id(&wid("ws-ghost"))
            .await
            .unwrap(),
        None
    );
    let alice = person("alice", gh(201), tenant, T1);
    let bob = person("bob", gh(202), tenant, T2);
    let carol = person("carol", gh(203), tenant, T3);
    let dave = person("dave", gh(204), tenant, T4);
    let erin = person("erin", gh(205), tenant, T4);
    for p in [&alice, &bob, &carol, &dave, &erin] {
        store.upsert_principal(p).await.unwrap();
    }
    for p in ["alice", "bob", "erin"] {
        store
            .insert_principal_credential(&pid(p), &format!("h-{p}"))
            .await
            .unwrap();
    }
    host_member(&store, &primary, "hi-carol", &carol).await;

    let (a, b) = (wid("ws-a"), wid("ws-b"));
    assert!(store
        .add_workspace_member(&a, &pid("alice"), WorkspaceRole::Collaborator)
        .await
        .unwrap());
    assert!(!store
        .add_workspace_member(&a, &pid("alice"), WorkspaceRole::Manager)
        .await
        .unwrap());
    assert!(matches!(
        store
            .add_workspace_member(&a, &pid("bob"), WorkspaceRole::Owner)
            .await,
        Err(Error::InvalidInput(_))
    ));
    let cap = |p: &'static str, max| {
        let store = &store;
        let a = a.clone();
        async move {
            store
                .add_workspace_collaborator_within_cap(&a, &pid(p), max)
                .await
                .unwrap()
        }
    };
    assert_eq!(cap("bob", 5).await, CollaboratorAddOutcome::Added);
    assert_eq!(
        cap("dave", 5).await,
        CollaboratorAddOutcome::NoActiveCredential
    );
    assert_eq!(cap("alice", 5).await, CollaboratorAddOutcome::AlreadyMember);
    assert_eq!(cap("carol", 5).await, CollaboratorAddOutcome::AlreadyMember);
    assert_eq!(cap("erin", 2).await, CollaboratorAddOutcome::WorkspaceFull);

    let members = store.list_workspace_members(&a).await.unwrap();
    assert_eq!(
        members
            .iter()
            .map(|m| (m.principal_id.as_str(), m.role))
            .collect::<Vec<_>>(),
        [
            (primary.as_str(), WorkspaceRole::Owner),
            ("alice", WorkspaceRole::Collaborator),
            ("bob", WorkspaceRole::Collaborator)
        ]
    );
    assert_eq!(
        store
            .get_workspace_member_role(&a, &pid("alice"))
            .await
            .unwrap(),
        Some(WorkspaceRole::Collaborator)
    );
    assert_eq!(
        store
            .get_workspace_member_role(&a, &pid("dave"))
            .await
            .unwrap(),
        None
    );
    store
        .set_workspace_member_role(&a, &pid("alice"), WorkspaceRole::Manager)
        .await
        .unwrap();
    assert!(matches!(
        store
            .set_workspace_member_role(&a, &pid("dave"), WorkspaceRole::Manager)
            .await,
        Err(Error::NotFound(_))
    ));
    let guest_role = |p: &'static str, role, actor: Option<PrincipalId>| {
        let store = &store;
        let a = a.clone();
        async move {
            store
                .set_workspace_guest_role(&a, &pid(p), role, actor.as_ref())
                .await
        }
    };
    assert!(
        guest_role("bob", WorkspaceRole::Manager, Some(primary.clone()))
            .await
            .unwrap()
    );
    assert!(!guest_role("bob", WorkspaceRole::Manager, None)
        .await
        .unwrap());
    assert!(matches!(
        guest_role("bob", WorkspaceRole::Collaborator, Some(pid("alice"))).await,
        Err(Error::Forbidden(_))
    ));
    assert!(matches!(
        guest_role("dave", WorkspaceRole::Collaborator, None).await,
        Err(Error::NotFound(_))
    ));
    assert!(store.has_workspace_membership(&pid("alice")).await.unwrap());
    assert!(!store.has_workspace_membership(&pid("dave")).await.unwrap());
    let alice_in = store
        .list_principal_memberships(&pid("alice"))
        .await
        .unwrap();
    assert_eq!(alice_in.len(), 1);
    assert_eq!(
        (alice_in[0].workspace_id.as_str(), alice_in[0].role),
        ("ws-a", WorkspaceRole::Manager)
    );

    let roster = store.list_effective_workspace_members(&a).await.unwrap();
    assert_eq!(
        roster
            .iter()
            .map(|m| (m.principal.login.clone(), m.role, m.host_role))
            .collect::<Vec<_>>(),
        [
            (None, WorkspaceRole::Owner, HostRole::Owner),
            (
                Some(format!("carol-{tenant}")),
                WorkspaceRole::Collaborator,
                HostRole::Member
            ),
            (
                Some(format!("alice-{tenant}")),
                WorkspaceRole::Manager,
                HostRole::Guest
            ),
            (
                Some(format!("bob-{tenant}")),
                WorkspaceRole::Manager,
                HostRole::Guest
            ),
        ]
    );
    assert!(store
        .list_effective_workspace_members(&wid("ws-ghost"))
        .await
        .unwrap()
        .is_empty());
    let summaries = store
        .workspace_membership_summaries(Some(&pid("alice")), &[a.clone(), wid("ws-ghost")])
        .await
        .unwrap();
    assert_eq!(summaries.len(), 1);
    let summary = &summaries[&a];
    assert_eq!(summary.owner_principal_id, Some(primary.clone()));
    assert_eq!(summary.my_role, Some(WorkspaceRole::Manager));
    assert!(!summary.can_manage);
    assert_eq!(summary.member_count, 4);
    assert_eq!(summary.open_invite_count, 0);
    let as_carol = store
        .workspace_membership_summaries(Some(&pid("carol")), &[a.clone(), b.clone()])
        .await
        .unwrap();
    assert!(as_carol[&b].can_manage);
    assert_eq!(as_carol[&b].my_role, Some(WorkspaceRole::Collaborator));
    assert_eq!(as_carol[&b].member_count, 2);
    let anonymous = store
        .workspace_membership_summaries(None, std::slice::from_ref(&b))
        .await
        .unwrap();
    assert_eq!(anonymous[&b].my_role, None);
    assert!(!anonymous[&b].can_manage);

    assert_eq!(
        store
            .effective_principal_workspace_ids(&pid("carol"))
            .await
            .unwrap(),
        [a.clone(), b.clone()]
    );
    assert_eq!(
        store
            .effective_principal_workspace_ids(&pid("alice"))
            .await
            .unwrap(),
        std::slice::from_ref(&a)
    );

    store
        .set_workspace_legacy_author_principal_id(&a, Some(&pid("alice")))
        .await
        .unwrap();
    let fallback = store
        .get_workspace_author_fallback(&a)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fallback.legacy_author_principal_id, Some(pid("alice")));
    assert_eq!(fallback.owner_principal_id, Some(primary.clone()));
    assert_eq!(
        store
            .get_workspace_author_fallback(&wid("ws-ghost"))
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        store
            .set_workspace_legacy_author_principal_id(&wid("ws-ghost"), None)
            .await,
        Err(Error::NotFound(_))
    ));

    assert!(matches!(
        store.remove_workspace_guest(&a, &pid("carol")).await,
        Err(Error::HostMembershipRequired)
    ));
    assert!(matches!(
        store.remove_workspace_guest(&a, &primary).await,
        Err(Error::InvalidParams(_))
    ));
    assert!(store.remove_workspace_guest(&a, &pid("bob")).await.unwrap());
    assert!(!store.remove_workspace_guest(&a, &pid("bob")).await.unwrap());
    assert!(store
        .remove_workspace_member(&a, &pid("alice"))
        .await
        .unwrap());
    assert!(!store
        .remove_workspace_member(&a, &pid("alice"))
        .await
        .unwrap());

    store
        .set_workspace_member_role(&b, &primary, WorkspaceRole::Manager)
        .await
        .unwrap();
    assert_eq!(
        store.get_workspace_owner_principal_id(&b).await.unwrap(),
        None
    );
    store
        .add_workspace_member(&b, &pid("alice"), WorkspaceRole::Owner)
        .await
        .unwrap();
    assert_eq!(
        store.get_workspace_owner_principal_id(&b).await.unwrap(),
        Some(pid("alice"))
    );
    assert!(store
        .remove_workspace_member(&b, &pid("alice"))
        .await
        .unwrap());
    assert_eq!(
        store.get_workspace_owner_principal_id(&b).await.unwrap(),
        None
    );

    if tenant == "decoy" {
        workspace(&store, "ws-ghost", tenant).await;
        store
            .insert_principal_credential(&pid("dave"), "h-dave")
            .await
            .unwrap();
        for (w, p) in [(&a, "alice"), (&a, "bob"), (&a, "dave"), (&b, "alice")] {
            store
                .add_workspace_member(w, &pid(p), WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
        decoy_sql(
            db,
            tenant,
            "UPDATE workspace SET legacy_author_principal_id = NULL WHERE {w} AND id = 'ws-a'",
        )
        .await;
    }
});

fn invite(
    id: &str,
    created_by: &PrincipalId,
    pin: Option<i64>,
    expires_at: &str,
) -> WorkspaceInvite {
    WorkspaceInvite {
        id: id.into(),
        workspace_id: wid("ws-i"),
        secret_hash: format!("sh-{id}"),
        secret: Some(format!("secret-{id}")),
        created_by_principal_id: created_by.clone(),
        pin_identity: pin.map(gh),
        pin_github_user_id: pin,
        pin_login: pin.map(|n| format!("user{n}")),
        created_at: T1.into(),
        expires_at: expires_at.into(),
        redeemed_at: None,
        redeemed_by_principal_id: None,
        revoked_at: None,
        redemption_count: 0,
    }
}

parity_test!(workspace_invites_and_joins, |store, tenant, db| {
    let primary = store.get_primary_principal().await.unwrap().id;
    workspace(&store, "ws-i", tenant).await;
    let w = wid("ws-i");
    let alice = person("alice", gh(301), tenant, T1);
    store.upsert_principal(&alice).await.unwrap();
    store
        .insert_principal_credential(&pid("alice"), "h-alice")
        .await
        .unwrap();
    let bob = person("bob", gh(302), tenant, T2);

    assert_eq!(
        store
            .insert_workspace_invite(&invite("inv-open", &primary, None, FAR))
            .await
            .unwrap(),
        InviteInsertOutcome::Inserted
    );
    assert_eq!(
        store
            .insert_workspace_invite(&invite("inv-alice", &pid("alice"), None, FAR))
            .await
            .unwrap(),
        InviteInsertOutcome::IssuerForbidden
    );
    assert_eq!(
        store
            .insert_workspace_invite_within_cap(&invite("inv-full", &primary, None, FAR), 1)
            .await
            .unwrap(),
        InviteInsertOutcome::WorkspaceFull
    );
    assert_eq!(
        store
            .insert_workspace_invite_within_cap(&invite("inv-bob", &primary, Some(302), FAR), 5)
            .await
            .unwrap(),
        InviteInsertOutcome::Inserted
    );
    store
        .insert_workspace_invite(&invite(
            "inv-old",
            &primary,
            None,
            "2020-01-01T00:00:00.000Z",
        ))
        .await
        .unwrap();
    assert_eq!(store.count_open_workspace_invites().await.unwrap(), 2);
    let open = store.list_open_workspace_invites(&w).await.unwrap();
    assert_eq!(
        open.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["inv-bob", "inv-open"]
    );
    assert_eq!(open[0], invite("inv-bob", &primary, Some(302), FAR));
    assert_eq!(store.get_workspace_invite("inv-ghost").await.unwrap(), None);
    let guests = store.count_workspace_guests(&w).await.unwrap();
    assert_eq!((guests.collaborators, guests.open_invites), (0, 2));

    let joined = store
        .join_workspace_by_invite(
            "inv-bob",
            &w,
            &bob,
            HostJoinCredential::Proof {
                token_hash: "h-bob",
                authorization_generation: 0,
            },
            5,
        )
        .await
        .unwrap();
    let InviteJoinOutcome::Joined(joined) = joined else {
        panic!("bob did not join: {joined:?}");
    };
    assert_eq!(joined.login, Some(format!("bob-{tenant}")));
    assert_eq!(
        store
            .lookup_principal_credential("h-bob")
            .await
            .unwrap()
            .unwrap()
            .principal_id,
        pid("bob")
    );
    let again = store
        .join_workspace_by_invite(
            "inv-bob",
            &w,
            &bob,
            HostJoinCredential::Existing {
                token_hash: "h-bob",
            },
            5,
        )
        .await
        .unwrap();
    assert_eq!(again, InviteJoinOutcome::Closed);
    let existing = HostJoinCredential::Existing {
        token_hash: "h-alice",
    };
    let first = store
        .join_workspace_by_invite("inv-open", &w, &alice, existing, 5)
        .await
        .unwrap();
    assert!(matches!(first, InviteJoinOutcome::Joined(_)));
    let second = store
        .join_workspace_by_invite("inv-open", &w, &alice, existing, 5)
        .await
        .unwrap();
    assert!(matches!(second, InviteJoinOutcome::Rejoined(_)));
    let wrong = store
        .join_workspace_by_invite(
            "inv-open",
            &w,
            &person("carol", gh(303), tenant, T3),
            HostJoinCredential::Existing {
                token_hash: "h-alice",
            },
            5,
        )
        .await
        .unwrap();
    assert_eq!(wrong, InviteJoinOutcome::CredentialInvalid);
    assert!(store
        .redeem_workspace_invite("inv-open", &pid("alice"))
        .await
        .unwrap());
    let reused = store
        .get_workspace_invite("inv-open")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reused.redemption_count, 2);
    assert_eq!(reused.redeemed_by_principal_id, Some(pid("alice")));
    assert!(store.revoke_workspace_invite("inv-open").await.unwrap());
    assert!(!store.revoke_workspace_invite("inv-open").await.unwrap());
    assert!(!store
        .redeem_workspace_invite("inv-open", &pid("alice"))
        .await
        .unwrap());
    assert_eq!(store.count_open_workspace_invites().await.unwrap(), 0);
    let guests = store.count_workspace_guests(&w).await.unwrap();
    assert_eq!((guests.collaborators, guests.open_invites), (2, 0));
    assert_eq!(
        store
            .workspace_membership_summaries(None, std::slice::from_ref(&w))
            .await
            .unwrap()[&w]
            .member_count,
        3
    );

    if tenant == "decoy" {
        store
            .upsert_principal(&person("carol", gh(303), tenant, T3))
            .await
            .unwrap();
        store
            .insert_workspace_invite(&invite("inv-ghost", &primary, None, FAR))
            .await
            .unwrap();
        decoy_sql(
            db,
            tenant,
            "UPDATE workspace_invite SET redeemed_at = NULL, redeemed_by_principal_id = NULL, \
             redemption_count = 0, revoked_at = NULL WHERE {w}",
        )
        .await;
    }
});

parity_test!(host_members_invites_and_removal, |store, tenant, db| {
    let primary = store.get_primary_principal().await.unwrap().id;
    let state = store.host_membership_state().await.unwrap();
    assert_eq!(
        (
            state.revision,
            state.member_count,
            state.authorization_generation
        ),
        (0, 0, 0)
    );
    workspace(&store, "ws-h", tenant).await;
    let w = wid("ws-h");
    let carol = person("carol", gh(403), tenant, T1);
    let dave = person("dave", gh(404), tenant, T2);
    let erin = person("erin", gh(405), tenant, T3);
    for p in [&dave, &erin] {
        store.upsert_principal(p).await.unwrap();
        store
            .insert_principal_credential(&p.id, &format!("h-{}", p.id))
            .await
            .unwrap();
    }
    store
        .add_workspace_member(&w, &pid("dave"), WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let new_invite = |id: &str, identity, by: &PrincipalId| {
        HostInvite::new(
            id.into(),
            by.clone(),
            identity,
            format!("{id}-{tenant}"),
            format!("sh-{id}"),
            None,
        )
        .unwrap()
    };
    for (id, n) in [("hi-carol", 403), ("hi-erin", 405), ("hi-x", 406)] {
        store
            .insert_host_invite(&new_invite(id, gh(n), &primary))
            .await
            .unwrap();
    }
    assert!(matches!(
        store
            .insert_host_invite(&new_invite("hi-dave", gh(407), &pid("dave")))
            .await,
        Err(Error::InvalidInput(_))
    ));
    let open = store.list_open_host_invites().await.unwrap();
    assert_eq!(
        open.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["hi-carol", "hi-erin", "hi-x"]
    );
    assert_eq!(
        store
            .get_host_invite("hi-erin")
            .await
            .unwrap()
            .unwrap()
            .pin_login,
        format!("hi-erin-{tenant}")
    );
    assert_eq!(store.get_host_invite("hi-ghost").await.unwrap(), None);
    assert!(store.revoke_host_invite("hi-x").await.unwrap());
    assert!(!store.revoke_host_invite("hi-x").await.unwrap());
    assert!(matches!(
        store.revoke_host_invite("hi-ghost").await,
        Err(Error::NotFound(_))
    ));

    let proof = HostJoinCredential::Proof {
        token_hash: "h-carol",
        authorization_generation: 0,
    };
    let joined = store
        .join_host_by_invite("hi-carol", &carol, proof)
        .await
        .unwrap();
    let HostInviteJoinOutcome::Joined {
        principal,
        membership_added,
        revision,
    } = joined
    else {
        panic!("carol did not join: {joined:?}");
    };
    assert_eq!(principal.login, Some(format!("carol-{tenant}")));
    assert!(membership_added);
    assert_eq!(revision, 1);
    let join = |id: &'static str, p: Principal, c: HostJoinCredential<'static>| {
        let store = &store;
        async move { store.join_host_by_invite(id, &p, c).await.unwrap() }
    };
    assert_eq!(
        join("hi-carol", carol.clone(), proof).await,
        HostInviteJoinOutcome::Redeemed
    );
    assert_eq!(
        join("hi-x", carol.clone(), proof).await,
        HostInviteJoinOutcome::Revoked
    );
    assert_eq!(
        join("hi-ghost", carol.clone(), proof).await,
        HostInviteJoinOutcome::NotFound
    );
    assert_eq!(
        join("hi-erin", dave.clone(), proof).await,
        HostInviteJoinOutcome::PinMismatch
    );
    assert_eq!(
        join(
            "hi-erin",
            erin.clone(),
            HostJoinCredential::Existing {
                token_hash: "h-dave"
            }
        )
        .await,
        HostInviteJoinOutcome::CredentialInvalid
    );
    let erin_joined = join(
        "hi-erin",
        erin.clone(),
        HostJoinCredential::Existing {
            token_hash: "h-erin",
        },
    )
    .await;
    assert!(matches!(
        erin_joined,
        HostInviteJoinOutcome::Joined {
            membership_added: true,
            revision: 2,
            ..
        }
    ));

    assert_eq!(
        store.get_host_role(&pid("carol")).await.unwrap(),
        HostRole::Member
    );
    assert_eq!(
        store.get_host_role(&pid("dave")).await.unwrap(),
        HostRole::Guest
    );
    assert_eq!(
        store.get_host_role(&primary).await.unwrap(),
        HostRole::Owner
    );
    assert!(matches!(
        store.get_host_role(&pid("ghost")).await,
        Err(Error::NotFound(_))
    ));
    let roster = store.list_host_members().await.unwrap();
    assert_eq!(roster.revision, 2);
    assert_eq!(
        roster
            .members
            .iter()
            .map(|m| (m.principal_id.as_str(), m.host_role, m.login.clone()))
            .collect::<Vec<_>>(),
        [
            (primary.as_str(), HostRole::Owner, None),
            ("carol", HostRole::Member, Some(format!("carol-{tenant}"))),
            ("erin", HostRole::Member, Some(format!("erin-{tenant}"))),
        ]
    );

    store
        .add_workspace_member(&w, &pid("carol"), WorkspaceRole::Manager)
        .await
        .unwrap();
    assert_eq!(
        store
            .insert_workspace_invite(&WorkspaceInvite {
                workspace_id: w.clone(),
                ..invite("inv-c", &pid("carol"), None, FAR)
            })
            .await
            .unwrap(),
        InviteInsertOutcome::Inserted
    );
    store
        .insert_agent_session(&AgentSession {
            id: AgentId("agent-h".into()),
            workspace_id: w.clone(),
            name: format!("agent of {tenant}"),
            created_at: T1.into(),
            updated_at: T1.into(),
            ..AgentSession::default()
        })
        .await
        .unwrap();
    let queued = |id: &str, from: &str| AgentQueueRow {
        id: id.into(),
        agent_id: AgentId("agent-h".into()),
        position: 0,
        payload: json!({ "text": tenant, "messageMetadata": { "fromPrincipalId": from } }),
        created_at: T1.into(),
        turn_id: format!("turn-{id}"),
    };
    store
        .replace_agent_queue(
            &AgentId("agent-h".into()),
            &[queued("q-carol", "carol"), queued("q-dave", "dave")],
        )
        .await
        .unwrap();

    assert!(matches!(
        store
            .remove_host_member_by_owner(&pid("dave"), &pid("carol"))
            .await,
        Err(Error::Forbidden(_))
    ));
    assert!(matches!(
        store.remove_host_member_by_owner(&primary, &primary).await,
        Err(Error::InvalidInput(_))
    ));
    let removed = store
        .remove_host_member_by_owner(&primary, &pid("carol"))
        .await
        .unwrap();
    assert!(removed.removed);
    assert_eq!(removed.revision, 3);
    assert_eq!(removed.credentials, 1);
    assert_eq!(removed.workspaces, std::slice::from_ref(&w));
    assert_eq!(removed.revoked_invites, ["inv-c"]);
    assert_eq!(removed.affected_workspaces, std::slice::from_ref(&w));
    assert!(store
        .principal_instructions_revoked(&pid("carol"))
        .await
        .unwrap());
    assert!(!store
        .principal_instructions_revoked(&pid("dave"))
        .await
        .unwrap());
    assert_eq!(
        store
            .load_all_agent_queues()
            .await
            .unwrap()
            .iter()
            .map(|q| q.id.as_str())
            .collect::<Vec<_>>(),
        ["q-dave"]
    );

    let revoked = store.revoke_principal_access(&pid("dave")).await.unwrap();
    assert!(!revoked.removed);
    assert_eq!(revoked.credentials, 1);
    assert_eq!(revoked.workspaces, std::slice::from_ref(&w));
    assert!(revoked.affected_workspaces.is_empty());
    assert!(store
        .principal_instructions_revoked(&pid("dave"))
        .await
        .unwrap());
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    let none = store.remove_host_member(&pid("carol")).await.unwrap();
    assert!(!none.removed);
    assert_eq!(none.credentials, 0);
    let erin_out = store.remove_host_member(&pid("erin")).await.unwrap();
    assert!(erin_out.removed);
    let state = store.host_membership_state().await.unwrap();
    assert_eq!(
        (
            state.revision,
            state.member_count,
            state.authorization_generation
        ),
        (4, 0, 3)
    );
    assert!(store.list_host_members().await.unwrap().members.len() == 1);

    if tenant == "decoy" {
        store
            .upsert_principal(&person("ghost", gh(499), tenant, T4))
            .await
            .unwrap();
        store
            .insert_host_invite(&new_invite("hi-ghost", gh(498), &primary))
            .await
            .unwrap();
        for p in ["carol", "dave"] {
            store
                .add_workspace_member(&w, &pid(p), WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
        store
            .replace_agent_queue(
                &AgentId("agent-h".into()),
                &[queued("q-carol", "carol"), queued("q-dave", "dave")],
            )
            .await
            .unwrap();
        decoy_sql(
            db,
            tenant,
            "UPDATE host_invite SET redeemed_at = NULL, redeemed_by_principal_id = NULL, \
             redemption_count = 0, revoked_at = NULL WHERE {w}; \
             UPDATE principal_credential SET revoked_at = NULL WHERE {w}; \
             UPDATE workspace_invite SET revoked_at = NULL WHERE {w}; \
             INSERT INTO host_member (tenant_id, principal_id, added_at) \
             VALUES ('decoy', 'carol', '2026-02-01T00:00:05.000Z'), \
                    ('decoy', 'erin', '2026-02-01T00:00:05.000Z'), \
                    ('decoy', 'dave', '2026-02-01T00:00:05.000Z')",
        )
        .await;
    }
});

fn ws_invite(
    id: &str,
    ws: &str,
    by: &PrincipalId,
    pin: Option<i64>,
    expires_at: &str,
) -> WorkspaceInvite {
    WorkspaceInvite {
        workspace_id: wid(ws),
        ..invite(id, by, pin, expires_at)
    }
}

fn proof(token_hash: &str, authorization_generation: u64) -> HostJoinCredential<'_> {
    HostJoinCredential::Proof {
        token_hash,
        authorization_generation,
    }
}

parity_test!(
    workspace_join_refusals_and_summaries,
    |store, tenant, db| {
        let primary = store.get_primary_principal().await.unwrap();
        let me = primary.id.clone();
        workspace(&store, "ws-r", tenant).await;
        workspace(&store, "ws-o", tenant).await;
        store
            .insert_workspace(&Workspace {
                id: wid("ws-x"),
                title: format!("archived of {tenant}"),
                branch: "main".into(),
                created_at: T1.into(),
                updated_at: T1.into(),
                status: WorkspaceStatus::Archived,
                archived: true,
                archived_at: Some(T2.into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let alice = person("alice", gh(501), tenant, T1);
        store.upsert_principal(&alice).await.unwrap();
        store
            .insert_principal_credential(&pid("alice"), "h-alice")
            .await
            .unwrap();
        let (r, o, x) = (wid("ws-r"), wid("ws-o"), wid("ws-x"));

        assert_eq!(
            store
                .add_workspace_collaborator_within_cap(&x, &pid("alice"), 5)
                .await
                .unwrap(),
            CollaboratorAddOutcome::WorkspaceArchived
        );
        let archived_invite = ws_invite("inv-x", "ws-x", &me, None, FAR);
        assert_eq!(
            store
                .insert_workspace_invite(&archived_invite)
                .await
                .unwrap(),
            InviteInsertOutcome::WorkspaceArchived
        );
        assert_eq!(
            store
                .insert_workspace_invite_within_cap(&archived_invite, 5)
                .await
                .unwrap(),
            InviteInsertOutcome::WorkspaceArchived
        );
        let join = |id: &'static str,
                    ws: WorkspaceId,
                    p: Principal,
                    c: HostJoinCredential<'static>,
                    max| {
            let store = &store;
            async move { store.join_workspace_by_invite(id, &ws, &p, c, max).await }
        };
        assert_eq!(
            join("inv-x", x.clone(), alice.clone(), proof("h-a2", 0), 5)
                .await
                .unwrap(),
            InviteJoinOutcome::WorkspaceArchived
        );

        for inv in [
            ws_invite("inv-open", "ws-r", &me, None, FAR),
            ws_invite("inv-pin", "ws-r", &me, Some(502), FAR),
            ws_invite("inv-past", "ws-r", &me, None, "2020-01-01T00:00:00.000Z"),
        ] {
            assert_eq!(
                store.insert_workspace_invite(&inv).await.unwrap(),
                InviteInsertOutcome::Inserted
            );
        }
        assert_eq!(
            join(
                "inv-open",
                r.clone(),
                person("frank", gh(506), tenant, T2),
                proof("h-f", 0),
                0
            )
            .await
            .unwrap(),
            InviteJoinOutcome::WorkspaceFull
        );
        let boss = Principal {
            is_primary: true,
            ..person("boss", gh(509), tenant, T2)
        };
        assert_eq!(
            join("inv-open", r.clone(), boss, proof("h-boss", 0), 5)
                .await
                .unwrap(),
            InviteJoinOutcome::OwnerSelfJoin
        );
        assert_eq!(
            join(
                "inv-pin",
                r.clone(),
                person("gina", gh(503), tenant, T2),
                proof("h-g", 0),
                5
            )
            .await
            .unwrap(),
            InviteJoinOutcome::PinMismatch
        );
        assert_eq!(
            join(
                "inv-open",
                r.clone(),
                person("hank", gh(504), tenant, T2),
                proof("h-h", 7),
                5
            )
            .await
            .unwrap(),
            InviteJoinOutcome::AccessRevoked
        );
        assert!(matches!(
            join(
                "inv-open",
                r.clone(),
                person("ivy", gh(505), tenant, T2),
                proof("", 0),
                5
            )
            .await,
            Err(Error::InvalidInput(_))
        ));

        let as_primary = store
            .workspace_membership_summaries(Some(&me), std::slice::from_ref(&r))
            .await
            .unwrap();
        assert!(as_primary[&r].can_manage);
        assert_eq!(as_primary[&r].my_role, Some(WorkspaceRole::Owner));
        assert_eq!(as_primary[&r].open_invite_count, 2);
        assert_eq!(as_primary[&r].member_count, 1);
        let guests = store.count_workspace_guests(&r).await.unwrap();
        assert_eq!((guests.collaborators, guests.open_invites), (0, 2));
        assert!(store
            .get_workspace_invite("inv-past")
            .await
            .unwrap()
            .is_some());

        let bob = person("bob", gh(507), tenant, T3);
        store.upsert_principal(&bob).await.unwrap();
        store
            .set_workspace_member_role(&o, &me, WorkspaceRole::Manager)
            .await
            .unwrap();
        store
            .add_workspace_member(&o, &pid("bob"), WorkspaceRole::Owner)
            .await
            .unwrap();
        let as_bob = store
            .workspace_membership_summaries(Some(&pid("bob")), std::slice::from_ref(&o))
            .await
            .unwrap();
        assert!(as_bob[&o].can_manage);
        assert_eq!(as_bob[&o].owner_principal_id, Some(pid("bob")));
        assert_eq!(
            store
                .insert_workspace_invite(&ws_invite("inv-bob", "ws-o", &pid("bob"), None, FAR))
                .await
                .unwrap(),
            InviteInsertOutcome::Inserted
        );
        store
            .set_workspace_member_role(&o, &pid("bob"), WorkspaceRole::Manager)
            .await
            .unwrap();
        assert!(
            !store
                .workspace_membership_summaries(Some(&pid("bob")), std::slice::from_ref(&o))
                .await
                .unwrap()[&o]
                .can_manage
        );
        assert_eq!(
            join(
                "inv-bob",
                o.clone(),
                person("jack", gh(508), tenant, T2),
                proof("h-j", 0),
                5
            )
            .await
            .unwrap(),
            InviteJoinOutcome::Closed
        );

        if tenant == "decoy" {
            for (id, n) in [
                ("frank", 506),
                ("boss", 509),
                ("hank", 504),
                ("ivy", 505),
                ("jack", 508),
            ] {
                store
                    .upsert_principal(&person(id, gh(n), tenant, T4))
                    .await
                    .unwrap();
            }
            store
                .set_workspace_member_role(&o, &pid("bob"), WorkspaceRole::Owner)
                .await
                .unwrap();
            decoy_sql(
                db,
                tenant,
                "UPDATE workspace_invite SET redeemed_at = NULL, redeemed_by_principal_id = NULL, \
             redemption_count = 0, revoked_at = NULL WHERE {w}",
            )
            .await;
        }
    }
);

parity_test!(host_join_refusals_and_guest_roles, |store, tenant, db| {
    let me = store.get_primary_principal().await.unwrap().id;
    workspace(&store, "ws-g", tenant).await;
    workspace(&store, "ws-g2", tenant).await;
    let carol = person("carol", gh(603), tenant, T1);
    host_member(&store, &me, "hi-carol", &carol).await;
    let new_invite = |id: &str, n: i64| {
        HostInvite::new(
            id.into(),
            me.clone(),
            gh(n),
            format!("{id}-{tenant}"),
            format!("sh-{id}"),
            None,
        )
        .unwrap()
    };
    store
        .insert_host_invite(&new_invite("hi-carol2", 603))
        .await
        .unwrap();
    let again = store
        .join_host_by_invite(
            "hi-carol2",
            &carol,
            HostJoinCredential::Existing {
                token_hash: "h-carol",
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        again,
        HostInviteJoinOutcome::Joined {
            membership_added: false,
            revision: 1,
            ..
        }
    ));
    store
        .insert_host_invite(&HostInvite {
            id: "hi-old".into(),
            secret_hash: "sh-hi-old".into(),
            secret: None,
            created_by_principal_id: me.clone(),
            pin_identity: gh(610),
            pin_login: format!("old-{tenant}"),
            created_at: "2020-01-01T00:00:00Z".into(),
            expires_at: "2020-01-08T00:00:00Z".into(),
            redeemed_at: None,
            redeemed_by_principal_id: None,
            revoked_at: None,
            redemption_count: 0,
        })
        .await
        .unwrap();
    for (id, n) in [
        ("hi-x", 611),
        ("hi-boss", 601),
        ("hi-rev", 604),
        ("hi-empty", 605),
    ] {
        store.insert_host_invite(&new_invite(id, n)).await.unwrap();
    }
    assert!(store.revoke_host_invite("hi-x").await.unwrap());
    assert!(!store.revoke_host_invite("hi-old").await.unwrap());
    assert_eq!(
        store
            .list_open_host_invites()
            .await
            .unwrap()
            .iter()
            .map(|i| i.id.as_str())
            .collect::<Vec<_>>(),
        ["hi-boss", "hi-rev", "hi-empty"]
    );
    let join = |id: &'static str, p: Principal, c: HostJoinCredential<'static>| {
        let store = &store;
        async move { store.join_host_by_invite(id, &p, c).await }
    };
    assert_eq!(
        join(
            "hi-old",
            person("old", gh(610), tenant, T2),
            proof("h-o", 0)
        )
        .await
        .unwrap(),
        HostInviteJoinOutcome::Expired
    );
    let boss = Principal {
        is_primary: true,
        ..person("boss", gh(601), tenant, T2)
    };
    assert_eq!(
        join("hi-boss", boss, proof("h-b", 0)).await.unwrap(),
        HostInviteJoinOutcome::OwnerSelfJoin
    );
    assert_eq!(
        join(
            "hi-rev",
            person("rex", gh(604), tenant, T2),
            proof("h-r", 9)
        )
        .await
        .unwrap(),
        HostInviteJoinOutcome::AccessRevoked
    );
    assert!(matches!(
        join(
            "hi-empty",
            person("emma", gh(605), tenant, T2),
            proof("", 0)
        )
        .await,
        Err(Error::InvalidInput(_))
    ));
    let sharing = store.list_sharing_principals().await.unwrap();
    assert_eq!(
        sharing
            .iter()
            .map(|(p, r)| (p.login.clone(), *r))
            .collect::<Vec<_>>(),
        [(Some(format!("carol-{tenant}")), HostRole::Member)]
    );

    let (g, g2) = (wid("ws-g"), wid("ws-g2"));
    store
        .add_workspace_member(&g, &pid("carol"), WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let guest_role = |ws: &WorkspaceId, p: &PrincipalId, role| {
        let store = &store;
        let (ws, p) = (ws.clone(), p.clone());
        async move { store.set_workspace_guest_role(&ws, &p, role, None).await }
    };
    assert!(matches!(
        guest_role(&g, &pid("carol"), WorkspaceRole::Owner).await,
        Err(Error::InvalidParams(_))
    ));
    assert!(matches!(
        guest_role(&g, &me, WorkspaceRole::Manager).await,
        Err(Error::InvalidParams(_))
    ));
    assert!(matches!(
        guest_role(&g, &pid("carol"), WorkspaceRole::Manager).await,
        Err(Error::HostMembershipRequired)
    ));
    store
        .set_workspace_member_role(&g2, &me, WorkspaceRole::Manager)
        .await
        .unwrap();
    assert!(matches!(
        guest_role(&g2, &me, WorkspaceRole::Collaborator).await,
        Err(Error::HostMembershipRequired)
    ));
    assert_eq!(
        store
            .get_workspace_member_role(&g, &pid("carol"))
            .await
            .unwrap(),
        Some(WorkspaceRole::Collaborator)
    );

    if tenant == "decoy" {
        for (id, n) in [("boss", 601), ("rex", 604), ("emma", 605)] {
            store
                .upsert_principal(&person(id, gh(n), tenant, T4))
                .await
                .unwrap();
        }
        decoy_sql(
            db,
            tenant,
            "UPDATE host_invite SET redeemed_at = NULL, redeemed_by_principal_id = NULL, \
             redemption_count = 0, revoked_at = NULL WHERE {w}",
        )
        .await;
    }
});

parity_test!(
    owner_queue_permit_binds_owner_and_exact_key,
    |store, tenant, db| {
        // Each Postgres tenant is provisioned with a random owner id; a fixed
        // owner id lets the decoy's credentials match the subject's probes.
        let owner = match db {
            Some(db) => {
                store
                    .upsert_principal(&person("qowner", gh(700), tenant, T1))
                    .await
                    .unwrap();
                exec(
                    &db.admin,
                    format!(
                        "UPDATE principal SET is_primary = 0 \
                         WHERE tenant_id = '{tenant}' AND is_primary = 1; \
                         UPDATE principal SET is_primary = 1 \
                         WHERE tenant_id = '{tenant}' AND id = 'qowner'"
                    ),
                )
                .await;
                pid("qowner")
            }
            None => store.get_primary_principal().await.unwrap().id,
        };
        let guest = person("qguest", gh(701), tenant, T1);
        store.upsert_principal(&guest).await.unwrap();
        store
            .insert_principal_credential(&guest.id, "q-guest-key")
            .await
            .unwrap();
        for key in ["q-own-key", "q-old-key"] {
            store
                .insert_principal_credential(&owner, key)
                .await
                .unwrap();
        }
        let permit = |id: PrincipalId, key: Option<&'static str>| {
            let store = &store;
            async move { store.owner_queue_permit(&id, key).await.unwrap().is_some() }
        };

        assert!(
            !permit(guest.id.clone(), None).await,
            "a guest is not the owner"
        );
        assert!(!permit(guest.id.clone(), Some("q-guest-key")).await);
        assert!(permit(owner.clone(), None).await);
        assert!(
            !permit(owner.clone(), Some("q-guest-key")).await,
            "another principal's key"
        );
        assert!(!permit(owner.clone(), Some("q-missing-key")).await);
        assert!(permit(owner.clone(), Some("q-old-key")).await);
        assert!(store
            .revoke_principal_credential("q-old-key")
            .await
            .unwrap());
        assert!(
            !permit(owner.clone(), Some("q-old-key")).await,
            "a revoked key"
        );
        assert!(permit(owner.clone(), Some("q-own-key")).await);

        if tenant == "decoy" {
            store
                .insert_principal_credential(&owner, "q-missing-key")
                .await
                .unwrap();
            decoy_sql(
                db,
                tenant,
                "UPDATE principal SET is_primary = 0 WHERE {w} AND is_primary = 1; \
                 UPDATE principal SET is_primary = 1 WHERE {w} AND id = 'qguest'",
            )
            .await;
            decoy_sql(
                db,
                tenant,
                &format!(
                    "UPDATE principal_credential SET principal_id = '{}', revoked_at = NULL \
                 WHERE {{w}} AND token_hash IN ('q-guest-key', 'q-old-key')",
                    owner.as_str()
                ),
            )
            .await;
        }
    }
);

/// The permit holds the tenant writer lock (and the store's writer slot): a
/// revoke waits for it, and dropping the permit (a rollback in sqlx's drop) lets the revoke finish.
#[tokio::test]
async fn owner_queue_permit_blocks_revoke_until_dropped() {
    use std::time::Duration;
    let Some(db) = test_db(false).await else {
        return;
    };
    let store = open(&db, "tenant-a").await;
    let other = open(&db, "tenant-b").await;
    let owner = store.get_primary_principal().await.unwrap().id;
    store
        .insert_principal_credential(&owner, "k")
        .await
        .unwrap();
    other
        .insert_principal_credential(&other.get_primary_principal().await.unwrap().id, "k")
        .await
        .unwrap();

    let held = store
        .owner_queue_permit(&owner, Some("k"))
        .await
        .unwrap()
        .unwrap();
    assert!(tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        other.revoke_principal_credential("k")
    )
    .await
    .expect("another tenant's writer is not blocked")
    .unwrap());
    let revoke = tokio::spawn({
        let store = store.clone();
        async move { store.revoke_principal_credential("k").await }
    });
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        async {
            while store.postgres_writers_waiting() == 0 {
                // timing-guard: poll interval; the store emits no event when a writer queues.
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
    )
    .await
    .expect("the revoke queues on the writer lock");
    assert!(!revoke.is_finished(), "revoke waits for the permit");
    drop(held);
    assert!(tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        revoke
    )
    .await
    .expect("dropping the permit releases the lock")
    .unwrap()
    .unwrap());
    assert!(store
        .owner_queue_permit(&owner, Some("k"))
        .await
        .unwrap()
        .is_none());
}

fn machine_home(home_id: &str, node: &str) -> WorkspaceHome {
    WorkspaceHome {
        home_id: HomeId(home_id.into()),
        node_id: Some(node.into()),
        cloud_machine_id: None,
        name: "Mac".into(),
        state: HomeState::Attached,
        since: T1.into(),
        last_backup_at: None,
        backup_warning: false,
        setup: None,
    }
}

/// `ws-ar`'s home on `n-ar` and a move of it to `h-ar2` on `n-ar2`.
async fn home_moving(store: &Store) {
    let ws = wid("ws-ar");
    store
        .set_workspace_home(&ws, &machine_home("h-ar", "n-ar"))
        .await
        .unwrap();
    let target = machine_home("h-ar2", "n-ar2");
    assert!(store
        .begin_home_move(&HomeMove {
            workspace_id: ws,
            target,
            from_home_id: HomeId("h-ar".into()),
            from_state: HomeState::Attached,
            from_since: T1.into(),
            pushed: false,
            started_at: T2.into(),
        })
        .await
        .unwrap());
}

parity_test!(
    workspace_archive_detaches_guests_and_ends_the_home,
    |store, tenant, db| {
        let primary = store.get_primary_principal().await.unwrap().id;
        for id in ["ws-ar", "ws-opt", "ws-ret", "ws-auto"] {
            workspace(&store, id, tenant).await;
        }
        let ar = wid("ws-ar");
        let alice = person("alice", gh(601), tenant, T1);
        let carol = person("carol", gh(603), tenant, T1);
        store.upsert_principal(&alice).await.unwrap();
        store.upsert_principal(&carol).await.unwrap();
        host_member(&store, &primary, "hi-carol-ar", &carol).await;
        for p in ["alice", "carol"] {
            assert!(store
                .add_workspace_member(&ar, &pid(p), WorkspaceRole::Collaborator)
                .await
                .unwrap());
        }
        for (id, expires) in [("inv-ar", FAR), ("inv-ar-old", "2020-01-01T00:00:00.000Z")] {
            let mut open = invite(id, &primary, None, expires);
            open.workspace_id = ar.clone();
            store.insert_workspace_invite(&open).await.unwrap();
        }
        home_moving(&store).await;

        let sweep = store
            .archive_workspace_detaching_guests(&ar, T3)
            .await
            .unwrap();
        assert_eq!(sweep.removed_collaborators, [pid("alice")]);
        assert_eq!(sweep.revoked_invites, 1);
        assert_eq!(sweep.member_count, 2);
        let archived = store.get_workspace(&ar).await.unwrap();
        assert!(archived.archived);
        assert_eq!(archived.status, WorkspaceStatus::Archived);
        assert_eq!(archived.archived_at.as_deref(), Some(T3));
        assert_eq!(
            store
                .list_workspace_members(&ar)
                .await
                .unwrap()
                .iter()
                .map(|m| m.principal_id.as_str())
                .collect::<Vec<_>>(),
            [primary.as_str(), "carol"]
        );
        assert!(store
            .list_open_workspace_invites(&ar)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(store.get_workspace_home(&ar).await.unwrap(), None);
        assert_eq!(store.get_home_move(&ar).await.unwrap(), None);
        let stops = store.list_workspace_home_stops(&ar).await.unwrap();
        assert_eq!(
            stops
                .iter()
                .map(|s| (s.home_id.as_str(), s.node_id.as_str(), s.reason))
                .collect::<Vec<_>>(),
            [
                ("h-ar", "n-ar", HomeStopReason::Retired),
                ("h-ar2", "n-ar2", HomeStopReason::Retired)
            ]
        );
        assert!(!store
            .home_stop_discards_push(&HomeId("h-ar".into()))
            .await
            .unwrap());
        assert!(store
            .home_stop_discards_push(&HomeId("h-ar2".into()))
            .await
            .unwrap());
        assert!(matches!(
            store
                .archive_workspace_detaching_guests(&wid("ws-missing"), T3)
                .await,
            Err(Error::NotFound(_))
        ));

        let opt = wid("ws-opt");
        store
            .set_workspace_auto_archive_disabled(&opt, true)
            .await
            .unwrap();
        assert_eq!(
            store
                .archive_workspace_detaching_guests_unless_auto_archive_disabled(&opt, T3)
                .await
                .unwrap(),
            None
        );
        assert!(!store.get_workspace(&opt).await.unwrap().archived);
        let manual = store
            .archive_workspace_detaching_guests(&opt, T3)
            .await
            .unwrap();
        assert!(manual.removed_collaborators.is_empty());
        assert!(store.get_workspace(&opt).await.unwrap().archived);
        let auto = store
            .archive_workspace_detaching_guests_unless_auto_archive_disabled(&wid("ws-auto"), T3)
            .await
            .unwrap()
            .expect("not opted out");
        assert_eq!(auto.revoked_invites, 0);
        assert!(store.get_workspace(&wid("ws-auto")).await.unwrap().archived);

        let ret = wid("ws-ret");
        store
            .archive_workspace_detaching_guests(&ret, T3)
            .await
            .unwrap();
        assert!(store.retire_workspace_if_archived(&ret, T4).await.unwrap());
        assert!(matches!(
            store.archive_workspace_detaching_guests(&ret, T4).await,
            Err(Error::WorkspaceRetired { .. })
        ));
        assert!(matches!(
            store
                .archive_workspace_detaching_guests_unless_auto_archive_disabled(&ret, T4)
                .await,
            Err(Error::WorkspaceRetired { .. })
        ));

        if tenant == "decoy" {
            workspace(&store, "ws-missing", tenant).await;
            home_moving(&store).await;
            decoy_sql(
                db,
                tenant,
                "UPDATE workspace_invite SET revoked_at = NULL WHERE {w} AND id = 'inv-ar'",
            )
            .await;
            decoy_sql(
                db,
                tenant,
                "UPDATE workspace SET archived = 0, status = 'active' \
                 WHERE {w} AND id IN ('ws-ar', 'ws-opt', 'ws-auto')",
            )
            .await;
            decoy_sql(
                db,
                tenant,
                "INSERT INTO workspace_member (tenant_id, workspace_id, principal_id, role, \
                 added_at) VALUES ('decoy', 'ws-ar', 'alice', 'collaborator', \
                 '2026-02-01T00:00:01.000Z')",
            )
            .await;
        }
    }
);
