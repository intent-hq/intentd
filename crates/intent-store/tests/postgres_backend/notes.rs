//! Notes, versions, tasks, comments, drafts, line attribution, search and
//! the retired paging cleanup on both backends. Every body uses fixed ids,
//! leaves its rows behind and asserts exact values from an empty start, so a
//! Postgres statement without its tenant filter fails the subject run.

use intent_core::{
    AgentId, AuthorType, ClientId, Comment, CommentAnchor, CommentStatus, CommentType, ContentType,
    Error, LineAttributionData, Note, NoteId, NoteMetadata, NoteVersionAuthor, NoteVisibility,
    TaskMetadata, TaskStatus, WorkspaceId,
};
use intent_store::note_paging_cleanup::{NotePagingCleanupPolicy, RetiredNotePagingStep};
use intent_store::{NoteFtsOptions, Store};

use crate::harness::{open, parity_test, sqlite_store, test_db};

const WS: &str = "__chief__";

fn ws() -> WorkspaceId {
    WorkspaceId(WS.into())
}

fn nid(id: &str) -> NoteId {
    NoteId(id.into())
}

fn note(id: &str, title: &str, content: &str, at: &str) -> Note {
    Note {
        id: nid(id),
        workspace_id: ws(),
        title: title.into(),
        content: content.into(),
        content_type: ContentType::Markdown,
        tags: vec!["t1".into()],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: at.into(),
        rev: 0,
        updated_at: at.into(),
    }
}

fn task(id: &str, parent: Option<&str>, status: TaskStatus, at: &str) -> Note {
    let mut n = note(id, &format!("Task {id}"), "", at);
    n.parent_id = parent.map(nid);
    n.metadata.task = Some(TaskMetadata {
        status,
        depends_on: vec![nid("t-dep")],
        ..TaskMetadata::default()
    });
    n
}

fn author() -> NoteVersionAuthor {
    NoteVersionAuthor {
        id: "u1".into(),
        name: "User".into(),
        author_type: "user".into(),
    }
}

fn comment(id: &str, thread: &str, note_id: &str, at: &str) -> Comment {
    Comment {
        id: id.into(),
        thread_id: thread.into(),
        note_id: Some(nid(note_id)),
        kind: CommentType::Comment,
        content: format!("body {id}"),
        author: "alice".into(),
        author_type: AuthorType::User,
        author_principal_id: None,
        author_identity: None,
        status: CommentStatus::Open,
        parent_id: None,
        anchor: Some(CommentAnchor::default()),
        anchor_text: Some("anchor".into()),
        anchor_before: None,
        anchor_after: None,
        suggestion_original: None,
        suggestion_proposed: None,
        agent_id: None,
        is_orphaned: None,
        created_at: at.into(),
        updated_at: at.into(),
    }
}

fn ids(notes: &[Note]) -> Vec<&str> {
    notes.iter().map(|n| n.id.as_str()).collect()
}

async fn notes_crud(store: &Store) {
    assert!(store.list_all_notes().await.unwrap().is_empty());
    assert_eq!(store.max_note_updated_at(&ws()).await.unwrap(), None);
    assert!(!store.note_exists(&ws(), &nid("n1")).await.unwrap());

    store
        .insert_note(&note("n2", "Two", "second", "2026-01-02T00:00:00Z"))
        .await
        .unwrap();
    store
        .insert_note(&note("n1", "One", "first", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    assert!(store.note_exists(&ws(), &nid("n1")).await.unwrap());
    assert_eq!(ids(&store.list_notes(&ws()).await.unwrap()), ["n1", "n2"]);
    assert_eq!(ids(&store.list_all_notes().await.unwrap()), ["n1", "n2"]);
    let got = store.get_note(&ws(), &nid("n1")).await.unwrap();
    assert_eq!(got, note("n1", "One", "first", "2026-01-01T00:00:00Z"));
    let meta = store.get_note_metadata(&ws(), &nid("n1")).await.unwrap();
    assert_eq!((meta.content.as_str(), meta.title.as_str()), ("", "One"));
    assert_eq!(
        store.max_note_updated_at(&ws()).await.unwrap().as_deref(),
        Some("2026-01-02T00:00:00Z")
    );
    let other = WorkspaceId("ws-none".into());
    let by_ws = store
        .max_note_updated_at_by_workspace(&[ws(), other.clone()])
        .await
        .unwrap();
    assert_eq!(by_ws.len(), 1);
    assert_eq!(by_ws[&ws()], "2026-01-02T00:00:00Z");
    assert!(matches!(
        store.get_note(&other, &nid("n1")).await,
        Err(Error::NotFound(_))
    ));

    assert!(matches!(
        store
            .delete_note_versioned(&ws(), &nid("n2"), Some(7))
            .await,
        Err(Error::Conflict { .. })
    ));
    store
        .delete_note_versioned(&ws(), &nid("n2"), Some(0))
        .await
        .unwrap();
    assert!(matches!(
        store.delete_note(&ws(), &nid("n2")).await,
        Err(Error::NotFound(_))
    ));
    store.delete_note(&ws(), &nid("n1")).await.unwrap();
    assert!(store.list_notes(&ws()).await.unwrap().is_empty());
    // Left behind for the next run.
    store
        .insert_note(&note("n3", "Three", "kept", "2026-01-03T00:00:00Z"))
        .await
        .unwrap();
}

parity_test!(notes_insert_list_get_and_delete, |store| {
    notes_crud(&store).await;
});

async fn note_updates(store: &Store) {
    let base = note("u1", "Title", "v0", "2026-02-01T00:00:00Z");
    store.insert_note(&base).await.unwrap();
    let lock = store.lock_note_content(&ws(), &nid("u1")).await;
    #[cfg(feature = "test-support")]
    {
        assert_eq!(store.note_content_lock_users(&ws(), &nid("u1")), 1);
        assert!(!store.note_content_locks_idle());
    }

    let mut next = base.clone();
    next.content = "v1".into();
    next.title = "Title 1".into();
    assert_eq!(store.update_note(&lock, &next).await.unwrap(), 1);
    assert!(matches!(
        store.update_note_versioned(&lock, &next, Some(0)).await,
        Err(Error::Conflict { .. })
    ));
    next.content = "v2".into();
    assert_eq!(
        store
            .update_note_versioned(&lock, &next, Some(1))
            .await
            .unwrap(),
        2
    );

    let mut meta = next.clone();
    meta.content = "ignored".into();
    meta.is_pinned = true;
    meta.tags = vec!["a".into(), "b".into()];
    assert_eq!(store.update_note_metadata(&lock, &meta).await.unwrap(), 3);
    assert_eq!(
        store
            .update_note_metadata_versioned(&lock, &meta, Some(3))
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        store
            .update_note_metadata_if_agent_linked(&lock, &meta, 4, &AgentId("a1".into()))
            .await
            .unwrap(),
        None
    );
    let stored = store.get_note(&ws(), &nid("u1")).await.unwrap();
    assert_eq!(stored.content, "v2");
    assert!(stored.is_pinned);
    assert_eq!(stored.tags, ["a", "b"]);
    assert_eq!(stored.rev, 4);

    // A lock for another note is refused before anything is written.
    let mut wrong = stored.clone();
    wrong.id = nid("other");
    assert!(matches!(
        store.update_note(&lock, &wrong).await,
        Err(Error::Internal(_))
    ));
    drop(lock);
    #[cfg(feature = "test-support")]
    assert!(store.note_content_locks_idle());

    let missing = note("absent", "x", "x", "2026-02-01T00:00:00Z");
    let lock = store.lock_note_content(&ws(), &nid("absent")).await;
    assert!(matches!(
        store.update_note(&lock, &missing).await,
        Err(Error::NotFound(_))
    ));
}

parity_test!(note_updates_are_gated_and_scoped, |store| {
    note_updates(&store).await;
});

async fn note_versions(store: &Store) {
    let n = note("v1", "Versioned", "c0", "2026-03-01T00:00:00Z");
    assert_eq!(
        store
            .insert_note_with_version(&n, &author(), "d0")
            .await
            .unwrap(),
        1
    );
    let lock = store.lock_note_content(&ws(), &nid("v1")).await;
    let mut next = n.clone();
    next.content = "c1".into();
    assert_eq!(
        store
            .update_note_with_version(&lock, &next, Some(0), &author(), "d1")
            .await
            .unwrap(),
        (1, 2)
    );
    assert!(matches!(
        store
            .update_note_with_version(&lock, &next, Some(0), &author(), "d1")
            .await,
        Err(Error::Conflict { .. })
    ));
    next.content = "c2".into();
    let mut child = note("v1-child", "Child", "child body", "2026-03-02T00:00:00Z");
    child.parent_id = Some(nid("v1"));
    assert_eq!(
        store
            .update_note_with_version_and_children(&lock, &next, Some(1), &[child], &author(), "d2")
            .await
            .unwrap(),
        (2, 3)
    );
    drop(lock);
    assert_eq!(
        store
            .get_note(&ws(), &nid("v1-child"))
            .await
            .unwrap()
            .parent_id,
        Some(nid("v1"))
    );
    assert_eq!(
        store
            .list_note_versions(&ws(), &nid("v1-child"))
            .await
            .unwrap()[0]
            .v,
        1
    );

    let versions = store.list_note_versions(&ws(), &nid("v1")).await.unwrap();
    let summary: Vec<_> = versions
        .iter()
        .map(|v| (v.v, v.date.as_str(), v.content_length))
        .collect();
    assert_eq!(summary, [(1, "d0", 2), (2, "d1", 2), (3, "d2", 2)]);
    let v2 = store.get_note_version(&ws(), &nid("v1"), 2).await.unwrap();
    assert_eq!((v2.content.as_str(), v2.author), ("c1", author()));
    assert!(matches!(
        store.get_note_version(&ws(), &nid("v1"), 9).await,
        Err(Error::NotFound(_))
    ));
    for (rev, content) in [(0, Some("c0")), (1, Some("c1")), (5, Some("c2"))] {
        assert_eq!(
            store
                .get_note_version_content_by_rev(&ws(), &nid("v1"), rev)
                .await
                .unwrap()
                .as_deref(),
            content
        );
    }
    assert_eq!(
        store
            .get_note_version_content_by_rev(&ws(), &nid("v1"), -1)
            .await
            .unwrap(),
        None
    );

    // Appends prune to the newest 50.
    for i in 0..50 {
        store
            .append_note_version(&next, &author(), &format!("a{i}"), 10 + i)
            .await
            .unwrap();
    }
    let versions = store.list_note_versions(&ws(), &nid("v1")).await.unwrap();
    assert_eq!(versions.len(), 50);
    assert_eq!((versions[0].v, versions[49].v), (4, 53));
}

parity_test!(note_versions_snapshot_and_prune, |store| {
    note_versions(&store).await;
});

async fn tasks(store: &Store) {
    let empty = store.list_workspace_tasks(&ws(), None).await.unwrap();
    assert!(empty.tasks.is_empty());
    assert_eq!(empty.stats.total, 0);

    let mut spec = note("spec", "Spec", "", "2026-04-01T00:00:00Z");
    spec.content = "- [ ] [A](intent://local/task/t-a)\n- [ ] [B](intent://local/task/t-b)".into();
    store.insert_note(&spec).await.unwrap();
    for t in [
        task(
            "t-a",
            Some("spec"),
            TaskStatus::Complete,
            "2026-04-02T00:00:00Z",
        ),
        task(
            "t-b",
            Some("spec"),
            TaskStatus::InProgress,
            "2026-04-03T00:00:00Z",
        ),
        task(
            "t-c",
            Some("spec"),
            TaskStatus::NotStarted,
            "2026-04-04T00:00:00Z",
        ),
        task("t-dep", None, TaskStatus::Complete, "2026-04-05T00:00:00Z"),
    ] {
        store.insert_note(&t).await.unwrap();
    }

    let all = store.list_workspace_tasks(&ws(), None).await.unwrap();
    let rows: Vec<_> = all
        .tasks
        .iter()
        .map(|t| (t.id.as_str(), t.spec_linked, t.unmet_depends_on.len()))
        .collect();
    assert_eq!(
        rows,
        [
            ("t-a", true, 0),
            ("t-b", true, 0),
            ("t-c", false, 0),
            ("t-dep", false, 0)
        ]
    );
    assert_eq!(all.tasks[0].depends_on, [nid("t-dep")]);
    // Linked spec: only t-a and t-b count.
    assert_eq!(
        (all.stats.total, all.stats.completed, all.stats.in_progress),
        (2, 1, 1)
    );
    let (filtered, keys) = store
        .list_workspace_tasks_keyed(&ws(), Some(TaskStatus::Complete))
        .await
        .unwrap();
    assert_eq!(
        filtered
            .tasks
            .iter()
            .map(|t| t.id.as_str())
            .collect::<Vec<_>>(),
        ["t-a", "t-dep"]
    );
    assert_eq!(keys, ["2026-04-02T00:00:00Z", "2026-04-05T00:00:00Z"]);
    assert_eq!(filtered.stats, all.stats);

    assert_eq!(store.list_tasks(&ws()).await.unwrap().len(), 4);
    let stats = store.count_task_stats(&ws()).await.unwrap();
    assert_eq!(stats, all.stats);
    let by_ws = store
        .count_task_stats_by_workspace(&[ws(), WorkspaceId("ws-none".into())])
        .await
        .unwrap();
    assert_eq!(by_ws[&ws()], all.stats);
    assert_eq!(by_ws[&WorkspaceId("ws-none".into())].total, 0);
    let counts = store.count_tasks_by_status(&ws()).await.unwrap();
    assert_eq!(
        counts.into_iter().collect::<Vec<_>>(),
        [
            ("complete".to_string(), 2),
            ("in_progress".to_string(), 1),
            ("not_started".to_string(), 1)
        ]
    );
}

parity_test!(task_projections_and_counts, |store| {
    tasks(&store).await;
});

async fn adopt_spec(store: &Store) {
    assert_eq!(store.adopt_stray_spec_note(&ws()).await.unwrap(), None);
    store
        .insert_note_with_version(
            &note("stray", " spec ", "body", "2026-05-01T00:00:00Z"),
            &author(),
            "d0",
        )
        .await
        .unwrap();
    let mut child = note("stray-child", "Child", "", "2026-05-02T00:00:00Z");
    child.parent_id = Some(nid("stray"));
    store.insert_note(&child).await.unwrap();
    store
        .insert_comment(
            &ws(),
            &comment("c-stray", "th-stray", "stray", "2026-05-03"),
        )
        .await
        .unwrap();
    store
        .upsert_note_line_attribution(&LineAttributionData {
            note_id: nid("stray"),
            workspace_id: ws(),
            computed_at: "2026-05-04".into(),
            attributions: std::collections::BTreeMap::new(),
        })
        .await
        .unwrap();

    assert_eq!(
        store.adopt_stray_spec_note(&ws()).await.unwrap(),
        Some((nid("stray"), " spec ".to_string()))
    );
    let spec = store.get_note(&ws(), &nid("spec")).await.unwrap();
    assert!(spec.is_pinned && spec.is_default);
    assert_eq!(spec.tags, ["t1", "spec"]);
    assert_eq!(spec.content, "body");
    assert!(!store.note_exists(&ws(), &nid("stray")).await.unwrap());
    assert_eq!(
        store
            .get_note(&ws(), &nid("stray-child"))
            .await
            .unwrap()
            .parent_id,
        Some(nid("spec"))
    );
    assert_eq!(
        store
            .list_note_versions(&ws(), &nid("spec"))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store.get_comment("c-stray").await.unwrap().note_id,
        Some(nid("spec"))
    );
    assert!(store
        .get_note_line_attribution(&ws(), &nid("spec"))
        .await
        .unwrap()
        .is_some());
    // A spec now exists: nothing more to adopt.
    assert_eq!(store.adopt_stray_spec_note(&ws()).await.unwrap(), None);
}

parity_test!(stray_spec_note_is_adopted_with_its_dependents, |store| {
    adopt_spec(&store).await;
});

async fn comments(store: &Store) {
    store
        .insert_note(&note("cn", "Commented", "text", "2026-06-01T00:00:00Z"))
        .await
        .unwrap();
    assert!(store.list_comments(&nid("cn")).await.unwrap().is_empty());
    assert!(matches!(
        store.get_thread("th1").await,
        Err(Error::NotFound(_))
    ));

    store
        .insert_comment(&ws(), &comment("c2", "th1", "cn", "2026-06-02"))
        .await
        .unwrap();
    let mut legacy = serde_json::Map::new();
    legacy.insert("legacyKey".into(), serde_json::json!(1));
    store
        .insert_comment_with_extras(&ws(), &comment("c1", "th1", "cn", "2026-06-01"), &legacy)
        .await
        .unwrap();
    let listed: Vec<_> = store
        .list_comments(&nid("cn"))
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(listed, ["c1", "c2"]);
    assert_eq!(
        store
            .list_comments_in_workspace(&ws(), &nid("cn"))
            .await
            .unwrap()
            .len(),
        2
    );
    assert!(store
        .list_comments_in_workspace(&WorkspaceId("ws-none".into()), &nid("cn"))
        .await
        .unwrap()
        .is_empty());

    let mut edited = store.get_comment("c1").await.unwrap();
    edited.content = "edited".into();
    edited.anchor_before = Some("before".into());
    edited.updated_at = "2026-06-03".into();
    store.update_comment(&ws(), &edited).await.unwrap();
    assert_eq!(store.get_comment("c1").await.unwrap(), edited);
    let mut absent = edited.clone();
    absent.id = "c-none".into();
    assert!(matches!(
        store.update_comment(&ws(), &absent).await,
        Err(Error::NotFound(_))
    ));

    assert!(store
        .mark_comment_orphaned(&ws(), &nid("cn"), "c1", "2026-06-04")
        .await
        .unwrap());
    assert_eq!(
        store.get_comment("c1").await.unwrap().is_orphaned,
        Some(true)
    );
    assert!(!store
        .mark_comment_orphaned(&ws(), &nid("other"), "c1", "2026-06-04")
        .await
        .unwrap());

    assert_eq!(
        store
            .set_thread_status(&ws(), "th1", CommentStatus::Resolved, "2026-06-05")
            .await
            .unwrap(),
        2
    );
    let thread = store.get_thread("th1").await.unwrap();
    assert!(thread
        .comments
        .iter()
        .all(|c| c.status == CommentStatus::Resolved));

    assert_eq!(
        store
            .delete_comment_in_note(&ws(), &nid("cn"), "c2")
            .await
            .unwrap(),
        "th1"
    );
    assert!(matches!(
        store.delete_comment_in_note(&ws(), &nid("cn"), "c2").await,
        Err(Error::NotFound(_))
    ));
    store.delete_comment(&ws(), "c1").await.unwrap();
    assert!(matches!(
        store.delete_comment(&ws(), "c1").await,
        Err(Error::NotFound(_))
    ));

    let lock = store.lock_note_content(&ws(), &nid("cn")).await;
    let mut updated = store.get_note(&ws(), &nid("cn")).await.unwrap();
    updated.content = "text with comment".into();
    updated.updated_at = "2026-06-06T00:00:00Z".into();
    let c3 = comment("c3", "th3", "cn", "2026-06-06");
    assert_eq!(
        store
            .update_note_with_comment(&lock, &updated, Some(0), &c3, &author())
            .await
            .unwrap(),
        1
    );
    assert!(matches!(
        store
            .update_note_with_comment(&lock, &updated, Some(1), &c3, &author())
            .await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        store
            .update_note_with_comment(&lock, &updated, Some(0), &c3, &author())
            .await,
        Err(Error::Conflict { .. })
    ));
    drop(lock);
    assert_eq!(store.get_note(&ws(), &nid("cn")).await.unwrap().rev, 1);
    assert_eq!(
        store.list_note_versions(&ws(), &nid("cn")).await.unwrap()[0].date,
        "2026-06-06T00:00:00Z"
    );
    assert_eq!(store.get_thread("th3").await.unwrap().comments, [c3]);
}

parity_test!(comments_round_trip, |store| {
    comments(&store).await;
});

parity_test!(line_attribution_round_trip, |store| {
    store
        .insert_note(&note("la", "Lines", "a\nb", "2026-07-01T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(
        store
            .get_note_line_attribution(&ws(), &nid("la"))
            .await
            .unwrap(),
        None
    );
    let mut data = LineAttributionData {
        note_id: nid("la"),
        workspace_id: ws(),
        computed_at: "first".into(),
        attributions: std::collections::BTreeMap::new(),
    };
    store.upsert_note_line_attribution(&data).await.unwrap();
    data.computed_at = "second".into();
    data.attributions.insert(
        "1".into(),
        serde_json::from_value(serde_json::json!({"timestamp": 5})).unwrap(),
    );
    store.upsert_note_line_attribution(&data).await.unwrap();
    assert_eq!(
        store
            .get_note_line_attribution(&ws(), &nid("la"))
            .await
            .unwrap(),
        Some(data)
    );
});

parity_test!(retired_note_paging_cleanup_finishes, |store| {
    let outcome = store
        .run_retired_note_paging_cleanup(NotePagingCleanupPolicy {
            min_pause: std::time::Duration::ZERO,
            ..NotePagingCleanupPolicy::default()
        })
        .await;
    assert!(outcome.finished);
    assert_eq!(outcome.rows_removed, 0);
    assert_eq!(
        store.retired_note_paging_step(10).await.unwrap(),
        RetiredNotePagingStep::Done
    );
});

parity_test!(task_event_baseline_is_captured_before_a_write, |store| {
    let baselines = store.task_event_baselines();
    assert!(baselines.lock(&ws()).await.is_none());
    store
        .insert_note(&task("te", None, TaskStatus::NotStarted, "2026-08-01"))
        .await
        .unwrap();
    let baseline = baselines.lock(&ws()).await;
    assert_eq!(baseline.as_ref().map(|b| b.tasks.len()), Some(0));
});

fn search_ids(hits: &[intent_store::NoteFtsMatch]) -> std::collections::BTreeSet<String> {
    hits.iter().map(|h| h.note_id.clone()).collect()
}

async fn search(store: &Store) {
    let options = NoteFtsOptions::default();
    assert!(store
        .search_notes_fts("\"zebra\"", &options)
        .await
        .unwrap()
        .is_empty());
    for (id, title, content, archived) in [
        ("s1", "Zebra crossing", "stripes everywhere", false),
        ("s2", "Plain", "a zebra in the text", false),
        ("s3", "Other", "zebrafish tank", false),
        ("s4", "Archived", "zebra archived", true),
    ] {
        let mut n = note(id, title, content, "2026-09-01T00:00:00Z");
        n.is_archived = archived;
        store.insert_note(&n).await.unwrap();
    }
    let all = store.search_notes_fts("\"zebra\"", &options).await.unwrap();
    assert_eq!(
        search_ids(&all),
        ["s1", "s2", "s4"].map(String::from).into()
    );
    assert_eq!(all[0].tags, ["t1"]);
    let prefix = store
        .search_notes_fts("(\"zebra\" OR \"zebra\"*)", &options)
        .await
        .unwrap();
    assert_eq!(
        search_ids(&prefix),
        ["s1", "s2", "s3", "s4"].map(String::from).into()
    );
    let both = store
        .search_notes_fts("\"zebra\" AND \"stripes\"", &options)
        .await
        .unwrap();
    assert_eq!(search_ids(&both), ["s1"].map(String::from).into());

    let live = NoteFtsOptions {
        include_archived: false,
        ..NoteFtsOptions::default()
    };
    assert_eq!(
        search_ids(&store.search_notes_fts("\"zebra\"", &live).await.unwrap()),
        ["s1", "s2"].map(String::from).into()
    );
    let scoped_away = NoteFtsOptions {
        workspace_id: Some(&WorkspaceId("ws-none".into())),
        ..NoteFtsOptions::default()
    };
    assert!(store
        .search_notes_fts("\"zebra\"", &scoped_away)
        .await
        .unwrap()
        .is_empty());
    let allowed = [ws()];
    let permitted = NoteFtsOptions {
        allowed_workspace_ids: Some(&allowed),
        limit: Some(2),
        ..NoteFtsOptions::default()
    };
    let page = store
        .search_notes_fts("\"zebra\"", &permitted)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    let per_ws = NoteFtsOptions {
        per_workspace_limit: Some(1),
        ..NoteFtsOptions::default()
    };
    assert_eq!(
        store
            .search_notes_fts("\"zebra\"", &per_ws)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        store
            .search_notes_fts(
                "\"zebra\"",
                &NoteFtsOptions {
                    limit: Some(-1),
                    ..NoteFtsOptions::default()
                }
            )
            .await,
        Err(Error::InvalidParams(_))
    ));

    // Keyset paging: positions re-anchor the first page, and the next page
    // continues strictly after it with no overlap.
    let anchors: Vec<(String, String)> = page
        .iter()
        .map(|h| (h.workspace_id.clone(), h.note_id.clone()))
        .collect();
    let positions = store
        .note_fts_positions("\"zebra\"", &options, &anchors)
        .await
        .unwrap();
    assert_eq!(positions.len(), 2);
    let last = positions
        .iter()
        .find(|p| p.note_id == page[1].note_id)
        .unwrap()
        .clone();
    let rest = store
        .search_notes_fts(
            "\"zebra\"",
            &NoteFtsOptions {
                after: Some(&last),
                ..NoteFtsOptions::default()
            },
        )
        .await
        .unwrap();
    let mut seen = search_ids(&page);
    seen.extend(search_ids(&rest));
    assert_eq!(seen, ["s1", "s2", "s4"].map(String::from).into());
    assert_eq!(rest.len(), 1);
}

parity_test!(note_search_matches_the_same_notes, |store| {
    search(&store).await;
});

async fn drafts(store: &Store) {
    let (agent, client) = (AgentId("agent-1".into()), ClientId("client-1".into()));
    assert_eq!(store.get_draft(&ws(), &agent, &client).await.unwrap(), None);
    store
        .upsert_draft(&ws(), &agent, &client, "first", None)
        .await
        .unwrap();
    let attachments = serde_json::json!([{"name": "a.png"}]);
    let at = store
        .upsert_draft(&ws(), &agent, &client, "second", Some(&attachments))
        .await
        .unwrap();
    let draft = store
        .get_draft(&ws(), &agent, &client)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (draft.text.as_str(), draft.attachments, draft.updated_at),
        ("second", Some(attachments), at)
    );
    assert!(store.delete_draft(&ws(), &agent, &client).await.unwrap());
    assert!(!store.delete_draft(&ws(), &agent, &client).await.unwrap());
    store
        .upsert_draft(&ws(), &agent, &client, "kept", None)
        .await
        .unwrap();
}

/// Drafts reference a client row; clients are not on the Postgres store
/// yet, so the Postgres runs seed it directly.
#[tokio::test]
async fn drafts_round_trip() {
    let (sqlite, _dir) = sqlite_store().await;
    sqlite
        .ensure_client(&ClientId("client-1".into()))
        .await
        .unwrap();
    drafts(&sqlite).await;
    let Some(db) = test_db(false).await else {
        return;
    };
    for tenant in ["decoy", "subject"] {
        sqlx::query(
            "INSERT INTO client (tenant_id, id, first_seen, last_seen) VALUES ($1, 'client-1', 'x', 'x')",
        )
        .bind(tenant)
        .execute(&db.admin)
        .await
        .unwrap();
        drafts(&open(&db, tenant).await).await;
    }
}

/// The agent-link guard's subquery is tenant-scoped: a link in one tenant
/// never authorizes the same ids in another.
#[tokio::test]
async fn agent_linked_metadata_update_checks_the_link_in_the_same_tenant() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant-a").await;
    let b = open(&db, "tenant-b").await;
    sqlx::query(
        "INSERT INTO agent_session (tenant_id, id, workspace_id, name, status, created_at, \
         updated_at, task_note_id) VALUES ('tenant-a', 'agent-1', $1, 'A', 'idle', 'x', 'x', 'tn')",
    )
    .bind(WS)
    .execute(&db.admin)
    .await
    .unwrap();
    let n = note("tn", "Task", "", "2026-10-01T00:00:00Z");
    for store in [&a, &b] {
        store.insert_note(&n).await.unwrap();
    }
    let agent = AgentId("agent-1".into());
    let lock = b.lock_note_content(&ws(), &nid("tn")).await;
    assert_eq!(
        b.update_note_metadata_if_agent_linked(&lock, &n, 0, &agent)
            .await
            .unwrap(),
        None
    );
    let lock = a.lock_note_content(&ws(), &nid("tn")).await;
    assert_eq!(
        a.update_note_metadata_if_agent_linked(&lock, &n, 0, &agent)
            .await
            .unwrap(),
        Some(1)
    );
}

/// The retired paging tables are shared: a tenant's cleanup deletes only its
/// own rows and never drops a table.
#[tokio::test]
async fn retired_note_paging_cleanup_deletes_only_the_tenants_rows() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant-a").await;
    let b = open(&db, "tenant-b").await;
    let n = note("pn", "Paged", "", "2026-10-01T00:00:00Z");
    for (tenant, store) in [("tenant-a", &a), ("tenant-b", &b)] {
        store.insert_note(&n).await.unwrap();
        sqlx::query(
            "INSERT INTO note_page_head (tenant_id, workspace_id, note_id, instance_id, \
             current_rev, generation) VALUES ($1, $2, 'pn', 'i', 0, 'g')",
        )
        .bind(tenant)
        .bind(WS)
        .execute(&db.admin)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO note_page_entry (tenant_id, workspace_id, note_id, collection, \
             position, value) SELECT $1, $2, 'pn', 'c', p, 'v' FROM generate_series(1, 5) p",
        )
        .bind(tenant)
        .bind(WS)
        .execute(&db.admin)
        .await
        .unwrap();
    }
    assert_eq!(
        a.retired_note_paging_step(3).await.unwrap(),
        RetiredNotePagingStep::Deleted {
            table: "note_page_entry",
            rows: 3
        }
    );
    let outcome = a
        .run_retired_note_paging_cleanup(NotePagingCleanupPolicy {
            batch_rows: 1,
            min_pause: std::time::Duration::ZERO,
            ..NotePagingCleanupPolicy::default()
        })
        .await;
    assert!(outcome.finished);
    assert_eq!((outcome.rows_removed, outcome.tables_dropped), (3, 0));
    let left: Vec<(String, i64)> = sqlx::query_as(
        "SELECT tenant_id, count(*) FROM note_page_entry GROUP BY 1 \
         UNION ALL SELECT tenant_id || '/head', count(*) FROM note_page_head GROUP BY 1 ORDER BY 1",
    )
    .fetch_all(&db.admin)
    .await
    .unwrap();
    assert_eq!(left, [("tenant-b".into(), 5), ("tenant-b/head".into(), 1)]);
}
