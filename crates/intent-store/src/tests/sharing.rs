use super::*;
use crate::{CollaboratorAddOutcome, InviteInsertOutcome};
use sqlx::Row;

#[tokio::test]
async fn sharing_archive_preserves_retained_member_rows_and_effective_counts() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Archive", false))
        .await
        .unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let people = [guest_identity(42), guest_identity(43), guest_identity(44)];
    for (index, person) in people.iter().enumerate() {
        store.upsert_principal(person).await.unwrap();
        if index < 2 {
            sqlx::query("INSERT INTO host_member VALUES (?,?)")
                .bind(person.id.as_str())
                .bind(now_iso())
                .execute(store.write_pool())
                .await
                .unwrap();
        }
        if index > 0 {
            store
                .add_workspace_member(&ws, &person.id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
    }
    store
        .insert_workspace_invite(&guest_invite("archive", &ws, &owner.id))
        .await
        .unwrap();
    let retained = store.list_workspace_members(&ws).await.unwrap();
    let swept = store
        .archive_workspace_detaching_guests(&ws, &now_iso())
        .await
        .unwrap();
    assert_eq!(swept.removed_collaborators, vec![people[2].id.clone()]);
    assert_eq!(swept.member_count, 3);
    assert_eq!(swept.revoked_invites, 1);
    assert_eq!(
        store.list_workspace_members(&ws).await.unwrap(),
        retained
            .into_iter()
            .filter(|m| m.principal_id != people[2].id)
            .collect::<Vec<_>>()
    );
    assert_projection(&store, &ws).await;
    let repeated = store
        .archive_workspace_detaching_guests(&ws, &now_iso())
        .await
        .unwrap();
    assert!(repeated.removed_collaborators.is_empty());
    assert_eq!(repeated.member_count, 3);
}

#[tokio::test]
async fn sharing_invite_insert_rechecks_a_seat_taken_after_the_service_precheck() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Last seat", false))
        .await
        .unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let guest = guest_identity(42);
    store.upsert_principal(&guest).await.unwrap();
    store
        .insert_principal_credential(&guest.id, "last-seat")
        .await
        .unwrap();
    // The service precheck finishes; a direct add commits before the mint's
    // transaction. Only the insert's own check can close this interleaving.
    assert_eq!(
        store.count_workspace_guests(&ws).await.unwrap().committed(),
        0
    );
    assert_eq!(
        store
            .add_workspace_collaborator_within_cap(&ws, &guest.id, 1)
            .await
            .unwrap(),
        CollaboratorAddOutcome::Added
    );
    let inserted = store
        .insert_workspace_invite_within_cap(&guest_invite("last-seat", &ws, &owner.id), 1)
        .await
        .unwrap();
    assert_eq!(inserted, InviteInsertOutcome::WorkspaceFull);
    assert!(store
        .get_workspace_invite("last-seat")
        .await
        .unwrap()
        .is_none());
}

async fn assert_projection(store: &Store, ws: &WorkspaceId) {
    let actual = store.count_workspace_guests(ws).await.unwrap();
    let expected:i64=sqlx::query_scalar("SELECT COUNT(*) FROM workspace_member m WHERE workspace_id=? AND role='collaborator' AND NOT EXISTS(SELECT 1 FROM host_member h WHERE h.principal_id=m.principal_id)")
        .bind(ws.as_str()).fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(actual.collaborators, u64::try_from(expected).unwrap());
    let now = now_iso();
    let expected:i64=sqlx::query_scalar("SELECT COUNT(*) FROM workspace_invite i WHERE workspace_id=? AND revoked_at IS NULL AND expires_at>? AND ((pin_identity_provider IS NULL AND pin_github_user_id IS NULL) OR redeemed_at IS NULL)")
        .bind(ws.as_str()).bind(now).fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(actual.open_invites, u64::try_from(expected).unwrap());
    let effective = store.list_effective_workspace_members(ws).await.unwrap();
    let summary = store
        .workspace_membership_summaries(None, std::slice::from_ref(ws))
        .await
        .unwrap();
    assert_eq!(summary[ws].member_count, effective.len() as u64);
    assert_eq!(summary[ws].open_invite_count, actual.open_invites);
}

#[tokio::test]
async fn sharing_projection_tracks_upgrades_noops_cascades_and_invite_history() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let a = WorkspaceId::new();
    let b = WorkspaceId::new();
    for ws in [&a, &b] {
        store
            .insert_workspace(&sample_workspace(ws, "Sharing", false))
            .await
            .unwrap();
    }
    let owner = store.get_primary_principal().await.unwrap();
    let person = guest_identity(42);
    store.upsert_principal(&person).await.unwrap();
    store
        .insert_principal_credential(&person.id, "sharing-token")
        .await
        .unwrap();
    for ws in [&a, &b] {
        store
            .add_workspace_member(ws, &person.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
    }
    assert_projection(&store, &a).await;
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(person.id.as_str())
        .bind(now_iso())
        .execute(store.write_pool())
        .await
        .unwrap();
    for ws in [&a, &b] {
        assert_projection(&store, ws).await;
        assert_eq!(
            store
                .count_workspace_guests(ws)
                .await
                .unwrap()
                .collaborators,
            0
        );
        assert!(!store
            .add_workspace_member(ws, &person.id, WorkspaceRole::Collaborator)
            .await
            .unwrap());
        store
            .set_workspace_member_role(ws, &person.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
        assert_projection(&store, ws).await;
    }
    sqlx::query("DELETE FROM host_member WHERE principal_id=?")
        .bind(person.id.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    for ws in [&a, &b] {
        assert_projection(&store, ws).await;
        assert_eq!(
            store
                .count_workspace_guests(ws)
                .await
                .unwrap()
                .collaborators,
            1
        );
    }
    for (id, pinned) in [
        ("reusable", false),
        ("pinned", true),
        ("revoked", false),
        ("expired", false),
    ] {
        let mut invite = guest_invite(id, &a, &owner.id);
        if pinned {
            invite.pin_identity = person.identity_key();
            invite.pin_github_user_id = Some(42);
        }
        if id == "expired" {
            invite.expires_at = "2000-01-01T00:00:00Z".into();
        }
        store.insert_workspace_invite(&invite).await.unwrap();
        assert_projection(&store, &a).await;
    }
    store
        .redeem_workspace_invite("reusable", &person.id)
        .await
        .unwrap();
    store
        .redeem_workspace_invite("pinned", &person.id)
        .await
        .unwrap();
    store.revoke_workspace_invite("revoked").await.unwrap();
    assert_projection(&store, &a).await;
    assert_eq!(
        store.count_workspace_guests(&a).await.unwrap().open_invites,
        1
    );
    assert!(store
        .get_workspace_invite("expired")
        .await
        .unwrap()
        .unwrap()
        .revoked_at
        .is_none());
    sqlx::query("DELETE FROM workspace_member WHERE principal_id=?")
        .bind(person.id.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    for ws in [&a, &b] {
        assert_projection(&store, ws).await;
    }
    sqlx::query("DELETE FROM workspace WHERE id=?")
        .bind(a.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    let n: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM workspace_sharing_summary WHERE workspace_id=?")
            .bind(a.as_str())
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(n, 0);
    assert_projection(&store, &b).await;
}

#[tokio::test]
async fn sharing_expiry_reconciles_at_exact_observation_boundary() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Expiry", false))
        .await
        .unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let mut invite = guest_invite("expiry", &ws, &owner.id);
    invite.expires_at = "2031-01-01T00:00:00Z".into();
    store.insert_workspace_invite(&invite).await.unwrap();
    for (time, expected) in [
        ("2030-01-01T00:00:00Z", 1),
        ("2031-01-01T00:00:00Z", 0),
        ("2032-01-01T00:00:00Z", 0),
    ] {
        let mut tx = store
            .sharing_snapshot(std::slice::from_ref(&ws), Some(time))
            .await
            .unwrap();
        let count: i64 = sqlx::query_scalar(
            "SELECT open_invite_count FROM workspace_sharing_summary WHERE workspace_id=?",
        )
        .bind(ws.as_str())
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(count, expected);
        tx.commit().await.unwrap();
        assert_eq!(
            store.get_workspace_invite("expiry").await.unwrap().unwrap(),
            invite
        );
    }
}

#[tokio::test]
async fn sharing_expiry_reconciles_on_first_read_after_restart() {
    // Use a separate database from the injected-clock boundary test so this
    // production-clock observation never travels backwards in simulated time.
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Downtime", false))
        .await
        .unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let mut invite = guest_invite("downtime", &ws, &owner.id);
    invite.expires_at = "2000-01-01T00:00:00Z".into();
    store.insert_workspace_invite(&invite).await.unwrap();
    drop(store);
    let restarted = Store::open(&tmp.path).await.unwrap();
    assert_eq!(
        restarted
            .count_workspace_guests(&ws)
            .await
            .unwrap()
            .open_invites,
        0
    );
    assert_eq!(
        restarted
            .get_workspace_invite("downtime")
            .await
            .unwrap()
            .unwrap(),
        invite
    );
}

#[tokio::test]
async fn sharing_legacy_backfill_preserves_members_and_closed_invites() {
    use sqlx::{migrate::Migrator, sqlite::SqliteConnectOptions, SqlitePool};
    use std::borrow::Cow;
    let tmp = TempDb::new();
    let pool = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&tmp.path)
            .create_if_missing(true),
    )
    .await
    .unwrap();
    Migrator {
        migrations: Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version <= 132)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    }
    .run(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO workspace(id,title,branch,status,created_at,updated_at) VALUES ('legacy','Legacy','main','Active','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").execute(&pool).await.unwrap();
    for (id, is_member) in [("inherited", true), ("retained", true), ("guest", false)] {
        sqlx::query("INSERT INTO principal(id,is_primary,created_at,updated_at) VALUES (?,0,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')").bind(id).execute(&pool).await.unwrap();
        if id != "inherited" {
            sqlx::query("INSERT INTO workspace_member VALUES ('legacy',?,'collaborator','2026-01-01T00:00:00Z')").bind(id).execute(&pool).await.unwrap();
        }
        if is_member {
            sqlx::query("INSERT INTO host_member VALUES (?,'2026-02-01T00:00:00Z')")
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
        }
    }
    for (id, revoked, expires) in [
        ("live", None, "2099-01-01T00:00:00Z"),
        ("expired", None, "2000-01-01T00:00:00Z"),
        (
            "closed",
            Some("2026-01-01T00:00:00Z"),
            "2099-01-01T00:00:00Z",
        ),
    ] {
        sqlx::query("INSERT INTO workspace_invite(id,workspace_id,secret_hash,created_by_principal_id,created_at,expires_at,revoked_at) SELECT ?,'legacy',?,id,'2026-01-01T00:00:00Z',?,? FROM principal WHERE is_primary=1").bind(id).bind(id).bind(expires).bind(revoked).execute(&pool).await.unwrap();
    }
    pool.close().await;
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::from("legacy");
    assert_projection(&store, &ws).await;
    assert_eq!(
        store
            .list_effective_workspace_members(&ws)
            .await
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        store.count_workspace_guests(&ws).await.unwrap(),
        crate::WorkspaceGuestCount {
            collaborators: 1,
            open_invites: 1
        }
    );
    assert_eq!(store.list_workspace_members(&ws).await.unwrap().len(), 3);
}

#[tokio::test]
async fn sharing_target_upgrade_fences_remove_before_deleting_retained_grant() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Race", false))
        .await
        .unwrap();
    let p = guest_identity(42);
    store.upsert_principal(&p).await.unwrap();
    store
        .add_workspace_member(&ws, &p.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    sqlx::query("INSERT INTO host_member VALUES (?,?)")
        .bind(p.id.as_str())
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .unwrap();
    let other = store.clone();
    let wid = ws.clone();
    let pid = p.id.clone();
    let (started, ready) = tokio::sync::oneshot::channel();
    let remove = tokio::spawn(async move {
        started.send(()).unwrap();
        other.remove_workspace_guest(&wid, &pid).await
    });
    ready.await.unwrap();
    tx.commit().await.unwrap();
    assert!(matches!(
        remove.await.unwrap(),
        Err(Error::HostMembershipRequired)
    ));
    assert_eq!(
        store.get_workspace_member_role(&ws, &p.id).await.unwrap(),
        Some(WorkspaceRole::Collaborator)
    );
    assert_projection(&store, &ws).await;
}

#[tokio::test]
async fn sharing_expiry_probe_is_indexed_and_scoped_to_returned_workspaces() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let rows=sqlx::query("EXPLAIN QUERY PLAN SELECT 1 FROM workspace_invite_seat WHERE workspace_id IN (?,?) AND expires_at <= ? LIMIT 1")
        .bind("first").bind("second").bind(now_iso()).fetch_all(store.read_pool()).await.unwrap();
    let plan: Vec<String> = rows.iter().map(|r| r.get("detail")).collect();
    assert!(
        plan.iter()
            .any(|r| r.contains("workspace_invite_seat_expiry_idx") && r.contains("SEARCH")),
        "{plan:?}"
    );
    assert!(!plan.iter().any(|r| r.contains("SCAN")), "{plan:?}");
}

#[tokio::test]
async fn sharing_summary_work_is_bounded_by_returned_workspaces_not_grants_or_invites() {
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    // A single instrumented connection measures the actual public projection,
    // including its expiry probe/transaction, rather than a copied SQL fragment.
    let measured = Store {
        read_pool: store.write_pool.clone(),
        ..store.clone()
    };
    let ids = [WorkspaceId::new(), WorkspaceId::new(), WorkspaceId::new()];
    for id in &ids {
        store
            .insert_workspace(&sample_workspace(id, "Projection work", false))
            .await
            .unwrap();
    }
    let owner = store.get_primary_principal().await.unwrap();
    let steps = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&steps);
    let mut conn = measured.write_pool().acquire().await.unwrap();
    conn.lock_handle()
        .await
        .unwrap()
        .set_progress_handler(1, move || {
            counter.fetch_add(1, Ordering::SeqCst);
            true
        });
    drop(conn);
    let selected = &ids[..2];
    measured
        .workspace_membership_summaries(Some(&owner.id), selected)
        .await
        .unwrap();
    steps.store(0, Ordering::SeqCst);
    measured
        .workspace_membership_summaries(Some(&owner.id), selected)
        .await
        .unwrap();
    let before = steps.load(Ordering::SeqCst);
    for i in 0..256 {
        let person = guest_identity(10_000 + i);
        store.upsert_principal(&person).await.unwrap();
        for id in [&ids[0], &ids[2]] {
            store
                .add_workspace_member(id, &person.id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
            store
                .insert_workspace_invite(&guest_invite(&format!("{}-{i}", id.0), id, &owner.id))
                .await
                .unwrap();
        }
    }
    // Warm the same statement after schema statistics/cache changes, then count.
    measured
        .workspace_membership_summaries(Some(&owner.id), selected)
        .await
        .unwrap();
    steps.store(0, Ordering::SeqCst);
    let summaries = measured
        .workspace_membership_summaries(Some(&owner.id), selected)
        .await
        .unwrap();
    let after = steps.load(Ordering::SeqCst);
    let mut conn = measured.write_pool().acquire().await.unwrap();
    conn.lock_handle().await.unwrap().remove_progress_handler();
    drop(conn);
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[&ids[0]].member_count, 257);
    assert_eq!(summaries[&ids[0]].open_invite_count, 256);
    assert_eq!(summaries[&ids[1]].member_count, 1);
    assert_eq!(summaries[&ids[1]].open_invite_count, 0);
    eprintln!("sharing projection VM steps: empty={before}, 512 grants + 512 invites={after}");
    assert!(before > 0);
    assert!(
        after <= before + 64,
        "projection work grew with grant/invite rows: {before} -> {after}"
    );
}

#[tokio::test]
async fn sharing_import_and_principal_cascades_rebuild_only_local_counts() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let original = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&original, "Export", false))
        .await
        .unwrap();
    let p = guest_identity(42);
    store.upsert_principal(&p).await.unwrap();
    store
        .add_workspace_member(&original, &p.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    sqlx::query("INSERT INTO host_member VALUES (?,?)")
        .bind(p.id.as_str())
        .bind(now_iso())
        .execute(store.write_pool())
        .await
        .unwrap();
    let exported = store.transfer_export_rows(&original).await.unwrap();
    assert!(
        !exported
            .iter()
            .any(|(table, _)| table == "workspace_sharing_summary"
                || table == "workspace_invite_seat")
    );
    let imported = WorkspaceId::new();
    let mut row = exported
        .iter()
        .find(|(table, _)| table == "workspace")
        .unwrap()
        .1[0]
        .clone();
    row["id"] = json!(imported.0);
    row["owner_principal_id"] = json!(null);
    row["legacy_author_principal_id"] = json!(null);
    store
        .transfer_import_rows(&[("workspace".into(), vec![row])])
        .await
        .unwrap();
    for ws in [&original, &imported] {
        assert_projection(&store, ws).await;
    }
    sqlx::query("DELETE FROM principal WHERE id=?")
        .bind(p.id.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    for ws in [&original, &imported] {
        assert_projection(&store, ws).await;
        assert_eq!(
            store
                .workspace_membership_summaries(None, std::slice::from_ref(ws))
                .await
                .unwrap()[ws]
                .member_count,
            1
        );
    }
}

#[tokio::test]
async fn sharing_capped_invite_observes_serialized_direct_add_and_expiry() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Concurrent seat", false))
        .await
        .unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let guest = guest_identity(42);
    store.upsert_principal(&guest).await.unwrap();
    let mut tx = store
        .write_pool()
        .begin_with("BEGIN IMMEDIATE")
        .await
        .unwrap();
    sqlx::query("INSERT INTO workspace_member VALUES (?,?,?,?)")
        .bind(ws.as_str())
        .bind(guest.id.as_str())
        .bind("collaborator")
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .unwrap();
    let invite = guest_invite("concurrent", &ws, &owner.id);
    let other = store.clone();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let mint = tokio::spawn(async move {
        entered.send(()).unwrap();
        other
            .insert_workspace_invite_within_cap(&invite, 1)
            .await
            .unwrap()
    });
    ready.await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(mint.await.unwrap(), InviteInsertOutcome::WorkspaceFull);
    assert!(store
        .get_workspace_invite("concurrent")
        .await
        .unwrap()
        .is_none());
    // Upgrading the retained guest frees its seat. An expired reservation
    // must also be reconciled inside mint's admission transaction.
    sqlx::query("INSERT INTO host_member VALUES (?,?)")
        .bind(guest.id.as_str())
        .bind(now_iso())
        .execute(store.write_pool())
        .await
        .unwrap();
    let mut expired = guest_invite("already-expired", &ws, &owner.id);
    expired.expires_at = "2000-01-01T00:00:00Z".into();
    store.insert_workspace_invite(&expired).await.unwrap();
    assert_eq!(
        store
            .insert_workspace_invite_within_cap(&guest_invite("fresh", &ws, &owner.id), 1)
            .await
            .unwrap(),
        InviteInsertOutcome::Inserted
    );
    assert_eq!(
        store.count_workspace_guests(&ws).await.unwrap().committed(),
        1
    );
    assert_eq!(
        store
            .get_workspace_invite("already-expired")
            .await
            .unwrap()
            .unwrap(),
        expired
    );
}
