//! Attachments, stored diffs and tracked changes on both backends.
//!
//! Decoy rule: the body leaves the same keyed rows on every tenant, and the
//! decoy ends its run with rows behind every id the subject probes as absent
//! (`missing`, `att-k2`, `ws-empty`, the `nope` path, the `ws-c` diffs) and
//! behind every row the subject's stage moves select (unstaged `f`, `g`).
//! Values carry the tenant name. Ids mix case and cutoffs end in a lowercase
//! `t`, so ordering and expiry depend on `COLLATE "C"`: `<date>t` sorts after
//! `<date>T…` bytewise but before it under a case-insensitive locale.

use intent_core::{Error, Workspace, WorkspaceId};
use intent_store::{AttachmentRecord, NewDiff, NewTrackedChange, Store};

use crate::harness::parity_test;

const T1: &str = "2026-01-01T00:00:00.000Z";
const T2: &str = "2026-01-02T00:00:00.000Z";
/// After `T1` bytewise, before it under a case-insensitive collation.
const AFTER_T1: &str = "2026-01-01t";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

async fn add_workspace(store: &Store, id: &str) {
    store
        .insert_workspace(&Workspace {
            id: wid(id),
            title: format!("title {id}"),
            branch: "main".into(),
            created_at: T1.into(),
            updated_at: T1.into(),
            ..Default::default()
        })
        .await
        .unwrap();
}

fn attachment(id: &str, ws: &str, tenant: &str, uploaded_at: &str) -> AttachmentRecord {
    AttachmentRecord {
        id: id.into(),
        workspace_id: wid(ws),
        file_name: format!("{tenant}-{id}.png"),
        mime_type: (id.len() % 2 == 0).then(|| "image/png".into()),
        size: i64::try_from(tenant.len()).unwrap(),
        uploaded_at: uploaded_at.into(),
        stored_path: format!(".intent/attachments/{tenant}-{id}.png"),
    }
}

#[track_caller]
fn assert_not_found<T: std::fmt::Debug>(result: intent_core::Result<T>) {
    assert!(
        matches!(result, Err(Error::NotFound(_))),
        "expected NotFound, got {result:?}"
    );
}

parity_test!(attachments_insert_get_and_list, |store, tenant| {
    assert_not_found(store.get_attachment("missing").await);
    assert!(store
        .list_attachments(&wid("ws-empty"))
        .await
        .unwrap()
        .is_empty());

    let a = attachment("att-a", "ws-1", tenant, T1);
    let b = attachment("att-B", "ws-1", tenant, T2);
    let c = attachment("att-c", "ws-2", tenant, T1);
    for record in [&a, &b, &c] {
        store.insert_attachment(record).await.unwrap();
    }
    assert_eq!(store.get_attachment("att-a").await.unwrap(), a);
    assert_eq!(
        store.list_attachments(&wid("ws-1")).await.unwrap(),
        vec![b.clone(), a.clone()],
        "ordered by id bytewise"
    );
    assert_eq!(store.list_attachments(&wid("ws-2")).await.unwrap(), vec![c]);

    let duplicate = store
        .insert_attachment(&attachment("att-a", "ws-9", "dup", T2))
        .await;
    assert!(
        matches!(duplicate, Err(Error::Internal(_))),
        "{duplicate:?}"
    );
    assert_eq!(store.get_attachment("att-a").await.unwrap(), a);

    if tenant == "decoy" {
        for (id, ws) in [("missing", "ws-1"), ("decoy-only", "ws-empty")] {
            store
                .insert_attachment(&attachment(id, ws, tenant, T1))
                .await
                .unwrap();
        }
    }
});

parity_test!(
    attachment_idempotency_keys_bind_expire_and_sweep,
    |store, tenant| {
        let ws1 = wid("ws-1");
        assert!(store
            .get_attachment_by_idempotency_key(&ws1, "missing", "2000")
            .await
            .unwrap()
            .is_none());

        let k1 = attachment("att-k1", "ws-1", tenant, T1);
        store
            .insert_attachment_with_idempotency_key(&k1, "k1", &format!("fp-{tenant}"), "2000")
            .await
            .unwrap();
        let (binding, record) = store
            .get_attachment_by_idempotency_key(&ws1, "k1", "2000")
            .await
            .unwrap()
            .expect("live binding");
        assert_eq!(record, k1);
        assert_eq!(binding.workspace_id, ws1);
        assert_eq!(binding.key, "k1");
        assert_eq!(binding.attachment_id, "att-k1");
        assert_eq!(binding.fingerprint, format!("fp-{tenant}"));
        assert_eq!(binding.created_at, T1);
        // A binding created at the cutoff is expired.
        assert!(store
            .get_attachment_by_idempotency_key(&ws1, "k1", T1)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .get_attachment_by_idempotency_key(&ws1, "k1", AFTER_T1)
            .await
            .unwrap()
            .is_none());

        // A live binding refuses the key, and the attachment row rolls back.
        let refused = store
            .insert_attachment_with_idempotency_key(
                &attachment("att-k2", "ws-1", tenant, T2),
                "k1",
                "fp-2",
                "2000",
            )
            .await;
        assert!(
            matches!(refused, Err(Error::InvalidParams(_))),
            "{refused:?}"
        );
        assert_not_found(store.get_attachment("att-k2").await);

        // The same key in another workspace is a separate binding.
        let w2 = attachment("att-w2", "ws-2", tenant, T1);
        store
            .insert_attachment_with_idempotency_key(&w2, "k1", "fp-w2", "2000")
            .await
            .unwrap();

        // An expired binding is replaced; its attachment row stays.
        let k3 = attachment("att-k3", "ws-1", tenant, T2);
        store
            .insert_attachment_with_idempotency_key(&k3, "k1", "fp-3", AFTER_T1)
            .await
            .unwrap();
        let (binding, record) = store
            .get_attachment_by_idempotency_key(&ws1, "k1", "2000")
            .await
            .unwrap()
            .expect("replaced binding");
        assert_eq!(
            (binding.attachment_id.as_str(), binding.created_at.as_str()),
            ("att-k3", T2)
        );
        assert_eq!(record, k3);
        assert_eq!(store.get_attachment("att-k1").await.unwrap(), k1);

        assert_eq!(
            store
                .sweep_expired_attachment_idempotency_keys("2000")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .sweep_expired_attachment_idempotency_keys(AFTER_T1)
                .await
                .unwrap(),
            1
        );
        assert!(store
            .get_attachment_by_idempotency_key(&wid("ws-2"), "k1", "2000")
            .await
            .unwrap()
            .is_none());
        assert_eq!(store.get_attachment("att-w2").await.unwrap(), w2);
        assert!(store
            .get_attachment_by_idempotency_key(&ws1, "k1", "2000")
            .await
            .unwrap()
            .is_some());

        if tenant == "decoy" {
            store
                .insert_attachment(&attachment("att-k2", "ws-1", tenant, T2))
                .await
                .unwrap();
            for (id, ws) in [("decoy-missing", "ws-1"), ("decoy-w2", "ws-2")] {
                store
                    .insert_attachment_with_idempotency_key(
                        &attachment(id, ws, tenant, T1),
                        if id == "decoy-missing" {
                            "missing"
                        } else {
                            "k1"
                        },
                        "fp-decoy",
                        "2000",
                    )
                    .await
                    .unwrap();
            }
        }
    }
);

fn diff(ws: &str, path: &str, staged: bool, hunks: &str) -> NewDiff {
    NewDiff {
        workspace_id: wid(ws),
        file_path: path.into(),
        staged,
        old_content: None,
        new_content: staged.then(|| format!("new {path}")),
        hunks_json: hunks.into(),
    }
}

parity_test!(diffs_upsert_and_list, |store, tenant| {
    for ws in ["ws-a", "ws-c"] {
        add_workspace(&store, ws).await;
    }
    assert!(store.list_diffs(&wid("ws-c")).await.unwrap().is_empty());

    let hunks = format!("[\"{tenant}\"]");
    store
        .upsert_diff(&diff("ws-a", "a.rs", false, "[]"))
        .await
        .unwrap();
    store
        .upsert_diff(&diff("ws-a", "a.rs", true, &hunks))
        .await
        .unwrap();
    store
        .upsert_diff(&diff("ws-a", "b.rs", false, &hunks))
        .await
        .unwrap();
    let first = store.list_diffs(&wid("ws-a")).await.unwrap();
    let keys: Vec<(&str, bool)> = first
        .iter()
        .map(|d| (d.file_path.as_str(), d.staged))
        .collect();
    assert_eq!(keys, vec![("a.rs", false), ("a.rs", true), ("b.rs", false)]);

    let mut changed = diff("ws-a", "a.rs", false, &hunks);
    changed.old_content = Some(format!("old {tenant}"));
    store.upsert_diff(&changed).await.unwrap();
    let after = store.list_diffs(&wid("ws-a")).await.unwrap();
    assert_eq!(after.len(), 3);
    let row = &after[0];
    assert_eq!(
        (row.id.as_str(), row.created_at.as_str()),
        (first[0].id.as_str(), first[0].created_at.as_str())
    );
    assert_eq!(row.workspace_id, wid("ws-a"));
    assert_eq!(row.old_content, Some(format!("old {tenant}")));
    assert_eq!(row.new_content, None);
    assert_eq!(row.hunks_json, hunks);
    assert!(row.updated_at >= row.created_at);
    assert_eq!(after[1..], first[1..]);

    if tenant == "decoy" {
        store
            .upsert_diff(&diff("ws-c", "a.rs", false, &hunks))
            .await
            .unwrap();
    }
});

fn change(
    ws: &str,
    path: &str,
    stage: &str,
    agent: Option<&str>,
    additions: i64,
) -> NewTrackedChange {
    NewTrackedChange {
        workspace_id: wid(ws),
        path: path.into(),
        stage: stage.into(),
        status: "modified".into(),
        agent_id: agent.map(Into::into),
        session_id: agent.map(|a| format!("session-{a}")),
        turn: agent.map(|a| i64::try_from(a.len()).unwrap()),
        commit_hash: None,
        old_blob_sha: Some("old".into()),
        new_blob_sha: None,
        additions,
        deletions: additions / 2,
    }
}

parity_test!(tracked_changes_upsert_stage_and_sweep, |store, tenant| {
    let k = i64::try_from(tenant.len()).unwrap();
    for ws in ["ws-a", "ws-B", "ws-c"] {
        add_workspace(&store, ws).await;
    }
    assert!(store
        .list_workspaces_with_working_tracked_changes()
        .await
        .unwrap()
        .is_empty());
    let ws = wid("ws-a");

    let upsert = |c: NewTrackedChange| {
        let store = &store;
        async move { store.upsert_tracked_change(&c).await.unwrap() }
    };
    assert_eq!(
        upsert(change("ws-a", "f", "unstaged", Some("A"), 1)).await,
        None
    );
    assert_eq!(
        upsert(change("ws-a", "f", "unstaged", Some("A"), 9 * k)).await,
        Some((1, 0))
    );
    assert_eq!(
        upsert(change("ws-a", "f", "unstaged", Some("B"), 5)).await,
        None
    );
    assert_eq!(upsert(change("ws-a", "f", "unstaged", None, 2)).await, None);
    assert_eq!(
        upsert(change("ws-a", "f", "unstaged", None, 3)).await,
        Some((2, 1))
    );
    assert_eq!(store.list_tracked_changes(&ws).await.unwrap().len(), 3);

    let max = |agent: Option<&'static str>, path: &'static str| {
        let store = &store;
        let ws = ws.clone();
        async move {
            store
                .max_sibling_tracked_change_counters(&ws, path, "unstaged", agent)
                .await
                .unwrap()
        }
    };
    assert_eq!(max(Some("A"), "f").await, Some((5, 2)));
    assert_eq!(max(None, "f").await, Some((9 * k, 9 * k / 2)));
    assert_eq!(max(Some("C"), "f").await, Some((9 * k, 9 * k / 2)));
    assert_eq!(max(Some("A"), "nope").await, None);

    assert_eq!(
        store
            .set_tracked_change_stage(&ws, "f", "unstaged", "staged")
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        store
            .set_tracked_change_stage(&ws, "f", "unstaged", "staged")
            .await
            .unwrap(),
        0
    );
    let rows = store.list_tracked_changes(&ws).await.unwrap();
    let a = rows
        .iter()
        .find(|r| r.agent_id.as_deref() == Some("A"))
        .unwrap();
    assert_eq!(a.stage, "staged");
    assert_eq!(a.session_id.as_deref(), Some("session-A"));
    assert_eq!(
        (a.turn, a.additions, a.deletions),
        (Some(1), 9 * k, 9 * k / 2)
    );
    assert_eq!(a.old_blob_sha.as_deref(), Some("old"));
    assert_eq!(a.workspace_id, ws);

    upsert(change("ws-a", "g", "unstaged", Some("A"), 1)).await;
    upsert(change("ws-B", "h", "unstaged", Some("A"), 1)).await;
    upsert(change("ws-c", "i", "committed", Some("A"), 1)).await;
    assert_eq!(
        store
            .list_workspaces_with_working_tracked_changes()
            .await
            .unwrap(),
        vec![wid("ws-B"), wid("ws-a")],
        "ordered bytewise"
    );

    assert_eq!(
        store
            .set_tracked_change_stage(&ws, "g", "unstaged", "committed")
            .await
            .unwrap(),
        1
    );
    let mut judged: Vec<(String, String)> = store
        .list_tracked_changes(&ws)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.id, r.updated_at))
        .collect();
    assert_eq!(judged.len(), 4);
    let a_id = a.id.clone();
    // The caller judged an older version of A's row.
    judged.iter_mut().find(|(id, _)| *id == a_id).unwrap().1 = "2000".into();
    assert_eq!(
        store
            .delete_unchanged_working_tracked_changes(&wid("ws-B"), &judged)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .delete_unchanged_working_tracked_changes(&ws, &[])
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .delete_unchanged_working_tracked_changes(&ws, &judged)
            .await
            .unwrap(),
        2
    );
    let mut left: Vec<(String, String)> = store
        .list_tracked_changes(&ws)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.path, r.stage))
        .collect();
    left.sort();
    assert_eq!(
        left,
        vec![
            ("f".to_owned(), "staged".to_owned()),
            ("g".to_owned(), "committed".to_owned())
        ]
    );

    if tenant == "decoy" {
        // Unstaged rows the subject's stage moves would reach without
        // their tenant filter.
        for path in ["nope", "f", "g"] {
            upsert(change("ws-a", path, "unstaged", Some("D"), 1)).await;
        }
    }
});
