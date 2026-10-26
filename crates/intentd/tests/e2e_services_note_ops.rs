//! E2E coverage for note.* operations (intent-services `note_ops.rs` coverage boost).
//!
//! Tests call `intent_services::Services` directly (not via WSS transport) for hermetic
//! in-process coverage. Tests note.add, note.edit, note.editLines, note.updateMetadata,
//! note.listTasks paths.

#![cfg(unix)]

use crate::common;

use std::path::Path;
use std::sync::Arc;

use intent_core::{
    now_iso, NoteAddInput, NoteCreate, NoteEditInput, NoteEditLinesInput, NoteId, NoteUpdateInput,
    TaskStatus, Workspace, WorkspaceApi, WorkspaceId,
};
use intent_services::{EventBus, Services};
use intent_store::Store;

fn workspace(id: &WorkspaceId, path: &Path) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "E2E-note-ops".to_string(),
        branch: "main".to_string(),
        created_at: ts.clone(),
        updated_at: ts.clone(),
        path: Some(path.display().to_string()),
        worktree_path: Some(path.display().to_string()),
        ..Default::default()
    }
}

/// Scratch layout: `<tmp>/intentd.db` plus the workspace checkout at
/// `<tmp>/ws`; the returned guard removes both on drop.
async fn setup() -> (Arc<Services>, WorkspaceId, tempfile::TempDir) {
    let tmp = common::test_tempdir("intentd-e2e-note-ops-");
    let db = tmp.path().join("intentd.db");
    let ws_root = tmp.path().join("ws");
    std::fs::create_dir_all(&ws_root).expect("create ws root");

    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_workspaces_root(tmp.path().to_path_buf())
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, &ws_root))
        .await
        .expect("insert ws");

    (Arc::new(services), ws, tmp)
}

#[intent_test_macros::daemon_test]
async fn note_add_edit_edit_lines() {
    let (services, ws, _tmp) = setup().await;

    // Create a note using the proper API
    let create_input = NoteCreate {
        title: "Test Note".to_string(),
        content: Some("Initial content".to_string()),
        tags: None,
        parent_id: None,
    };
    let note = services
        .create_note(ws.clone(), create_input, None, None)
        .await
        .expect("create note")
        .note;
    let note_id = note.id.clone();

    // Test note.add
    let add_input = NoteAddInput {
        content: "Added section".to_string(),
        heading: None,
        position: Some("end".to_string()),
    };
    let add_result = services
        .add_to_note(ws.clone(), note_id.clone(), add_input, None)
        .await
        .expect("add to note");
    assert_eq!(add_result.note_id, note_id);

    // Read back and verify
    let note = services
        .get_note(ws.clone(), note_id.clone())
        .await
        .expect("get note");
    assert!(note.content.contains("Initial content"));
    assert!(note.content.contains("Added section"));

    // Test note.edit
    let edit_input = NoteEditInput {
        old: "Initial".to_string(),
        new: "Modified".to_string(),
    };
    let edit_result = services
        .edit_note(ws.clone(), note_id.clone(), edit_input, None)
        .await
        .expect("edit note");
    assert!(edit_result.match_position >= 0);
    // Verify the edit actually occurred
    assert!(!edit_result.new_content.contains("Initial"));
    assert!(edit_result.new_content.contains("Modified"));

    // Test note.editLines - always invoke it
    let note_after_edit = services
        .get_note(ws.clone(), note_id.clone())
        .await
        .expect("get note");
    assert!(
        !note_after_edit.content.is_empty(),
        "note content must be non-empty"
    );
    let edit_lines_input = NoteEditLinesInput {
        start: 1,
        end: 1,
        content: "Line replaced".to_string(),
    };
    let edit_lines_result = services
        .edit_note_lines(ws.clone(), note_id.clone(), edit_lines_input, None)
        .await
        .expect("edit note lines");
    assert_eq!(edit_lines_result.note_id, note_id);
    // Verify the line edit actually occurred in the content
    let note_after_line_edit = services
        .get_note(ws.clone(), note_id.clone())
        .await
        .expect("get note after line edit");
    assert!(note_after_line_edit.content.contains("Line replaced"));

    // Cleanup
    drop(services); // Drop store handles before DB cleanup
}

#[intent_test_macros::daemon_test]
async fn note_list_tasks() {
    let (services, ws, _tmp) = setup().await;

    // Create a note with tasks
    let content = "# Tasks\n\n- [ ] Task 1\n- [x] Task 2\n- [ ] Task 3";
    let create_input = NoteCreate {
        title: "Task Note".to_string(),
        content: Some(content.to_string()),
        tags: None,
        parent_id: None,
    };
    let note = services
        .create_note(ws.clone(), create_input, None, None)
        .await
        .expect("create note")
        .note;

    // List tasks
    let tasks = services
        .list_note_tasks(ws.clone(), note.id.clone())
        .await
        .expect("list tasks");

    assert_eq!(tasks.len(), 3);
    assert_eq!(tasks[0].text, "Task 1");
    assert_eq!(tasks[0].status, "todo");
    assert_eq!(tasks[1].text, "Task 2");
    assert_eq!(tasks[1].status, "done");
    assert_eq!(tasks[2].text, "Task 3");
    assert_eq!(tasks[2].status, "todo");

    // Cleanup
    drop(services); // Drop store handles before DB cleanup
}

#[intent_test_macros::daemon_test]
async fn note_update_metadata() {
    let (services, ws, _tmp) = setup().await;

    // Create a note
    let create_input = NoteCreate {
        title: "Original Title".to_string(),
        content: Some("Content".to_string()),
        tags: Some(vec!["tag1".to_string()]),
        parent_id: None,
    };
    let note = services
        .create_note(ws.clone(), create_input, None, None)
        .await
        .expect("create note")
        .note;

    // Update metadata (title and tags)
    let update_result = services
        .update_note_metadata(
            ws.clone(),
            note.id.clone(),
            Some("New Title".to_string()),
            Some(vec!["tag2".to_string()]),
            None,
            None,
        )
        .await
        .expect("update metadata");

    assert!(update_result.ok);
    assert_eq!(update_result.note_id, note.id);

    // Verify the changes
    let updated_note = services
        .get_note(ws.clone(), note.id.clone())
        .await
        .expect("get note");
    assert_eq!(updated_note.title, "New Title");
    assert_eq!(updated_note.tags, vec!["tag2".to_string()]);
    assert_eq!(updated_note.content, "Content"); // Content unchanged

    // Cleanup
    drop(services); // Drop store handles before DB cleanup
}

/// Parallel writers per concurrency test. The issue (intent-hq/intent#7184)
/// saw 2–3 of 8 parallel `ws.note.edit` calls fail; more writers than the
/// 5-attempt retry bound makes the pre-fix failure deterministic.
const WRITERS: usize = 16;

/// Upper bound (before the slow-runner multiplier) for one concurrency
/// test, so a lock-ordering bug fails fast instead of hanging the suite.
const CONCURRENCY_TIMEOUT_SECS: u64 = 60;

async fn create_note_with(services: &Services, ws: &WorkspaceId, content: String) -> NoteId {
    services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Shared Spec".to_string(),
                content: Some(content),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note")
        .note
        .id
}

/// Run `count` writers released together by a barrier and return each
/// writer's result in index order.
async fn run_together<T, F, Fut>(count: usize, writer: F) -> Vec<T>
where
    T: Send + 'static,
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = T> + Send + 'static,
{
    let barrier = Arc::new(tokio::sync::Barrier::new(count));
    let handles: Vec<_> = (0..count)
        .map(|i| {
            let barrier = Arc::clone(&barrier);
            let fut = writer(i);
            intent_core::spawn_daemon(async move {
                barrier.wait().await;
                fut.await
            })
        })
        .collect();
    let budget = intentd_test_support::test_timeout(std::time::Duration::from_secs(
        CONCURRENCY_TIMEOUT_SECS,
    ));
    let joined = tokio::time::timeout(budget, async {
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.await.expect("writer task panicked"));
        }
        out
    })
    .await;
    joined.expect("concurrent writers did not finish (deadlock?)")
}

fn failures<T, E: std::fmt::Display>(results: &[Result<T, E>]) -> Vec<String> {
    results
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.as_ref().err().map(|e| format!("writer {i}: {e}")))
        .collect()
}

/// Regression for intent-hq/intent#7184: parallel exact-match `note.edit`
/// calls on different lines of one note must all apply — none may surface
/// `conflict: version mismatch`.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_edits_on_different_lines_all_apply() {
    let (services, ws, _tmp) = setup().await;
    let content = (0..WRITERS)
        .map(|i| format!("line {i}: todo"))
        .collect::<Vec<_>>()
        .join("\n");
    let note_id = create_note_with(&services, &ws, content).await;

    let results = run_together(WRITERS, |i| {
        let (services, ws, note_id) = (Arc::clone(&services), ws.clone(), note_id.clone());
        async move {
            services
                .edit_note(
                    ws,
                    note_id,
                    NoteEditInput {
                        old: format!("line {i}: todo"),
                        new: format!("line {i}: done"),
                    },
                    None,
                )
                .await
        }
    })
    .await;
    assert_eq!(failures(&results), Vec::<String>::new());

    let note = services
        .get_note(ws.clone(), note_id)
        .await
        .expect("get note");
    for i in 0..WRITERS {
        assert!(
            note.content.contains(&format!("line {i}: done")),
            "edit {i} missing from final content:\n{}",
            note.content
        );
    }
    assert!(!note.content.contains("todo"), "{}", note.content);
}

/// Parallel `note.add` calls on one note must all land.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_adds_all_apply() {
    let (services, ws, _tmp) = setup().await;
    let note_id = create_note_with(&services, &ws, "# Log".to_string()).await;

    let results = run_together(WRITERS, |i| {
        let (services, ws, note_id) = (Arc::clone(&services), ws.clone(), note_id.clone());
        async move {
            services
                .add_to_note(
                    ws,
                    note_id,
                    NoteAddInput {
                        content: format!("entry {i} from writer"),
                        heading: None,
                        position: Some("end".to_string()),
                    },
                    None,
                )
                .await
        }
    })
    .await;
    assert_eq!(failures(&results), Vec::<String>::new());

    let note = services
        .get_note(ws.clone(), note_id)
        .await
        .expect("get note");
    for i in 0..WRITERS {
        assert!(
            note.content.contains(&format!("entry {i} from writer")),
            "add {i} missing from final content:\n{}",
            note.content
        );
    }
}

/// Parallel `task.updateStatus` calls on different checkboxes of one note
/// must all apply.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_task_status_updates_all_apply() {
    let (services, ws, _tmp) = setup().await;
    let content = std::iter::once("# Tasks".to_string())
        .chain((0..WRITERS).map(|i| format!("- [ ] Step {i:02}")))
        .collect::<Vec<_>>()
        .join("\n");
    let note_id = create_note_with(&services, &ws, content).await;

    let results = run_together(WRITERS, |i| {
        let (services, ws, note_id) = (Arc::clone(&services), ws.clone(), note_id.clone());
        async move {
            services
                .task_update_status(
                    ws,
                    note_id,
                    format!("Step {i:02}"),
                    "done".to_string(),
                    None,
                )
                .await
        }
    })
    .await;
    assert_eq!(failures(&results), Vec::<String>::new());

    let tasks = services
        .list_note_tasks(ws.clone(), note_id)
        .await
        .expect("list tasks");
    assert_eq!(tasks.len(), WRITERS);
    for task in &tasks {
        assert_eq!(task.status, "done", "{} not done", task.text);
    }
}

/// Two parallel edits anchored on the SAME text: the first to run applies,
/// the second re-reads the note, finds its `old` text gone, and fails with
/// "Text not found" — never a silent double-apply or merged mash-up.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_edits_on_same_text_apply_once() {
    let (services, ws, _tmp) = setup().await;
    let note_id = create_note_with(
        &services,
        &ws,
        "header\nStatus: pending\nfooter".to_string(),
    )
    .await;

    let results = run_together(2, |i| {
        let (services, ws, note_id) = (Arc::clone(&services), ws.clone(), note_id.clone());
        async move {
            services
                .edit_note(
                    ws,
                    note_id,
                    NoteEditInput {
                        old: "Status: pending".to_string(),
                        new: format!("Status: claimed by writer {i}"),
                    },
                    None,
                )
                .await
        }
    })
    .await;

    let ok: Vec<usize> = (0..2).filter(|&i| results[i].is_ok()).collect();
    assert_eq!(
        ok.len(),
        1,
        "exactly one edit must apply: {:?}",
        failures(&results)
    );
    let loser = 1 - ok[0];
    let err = results[loser]
        .as_ref()
        .expect_err("loser must fail")
        .to_string();
    assert!(err.contains("Text not found"), "unexpected error: {err}");

    let note = services
        .get_note(ws.clone(), note_id)
        .await
        .expect("get note");
    assert_eq!(
        note.content,
        format!("header\nStatus: claimed by writer {}\nfooter", ok[0])
    );
}

/// Writers on two different notes run side by side and all apply: the
/// per-note serialization never couples unrelated notes.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_edits_across_two_notes_all_apply() {
    let (services, ws, _tmp) = setup().await;
    let content = (0..WRITERS / 2)
        .map(|i| format!("item {i}: open"))
        .collect::<Vec<_>>()
        .join("\n");
    let notes = [
        create_note_with(&services, &ws, content.clone()).await,
        create_note_with(&services, &ws, content).await,
    ];

    let results = run_together(WRITERS, |i| {
        let (services, ws) = (Arc::clone(&services), ws.clone());
        let note_id = notes[i % 2].clone();
        async move {
            services
                .edit_note(
                    ws,
                    note_id,
                    NoteEditInput {
                        old: format!("item {}: open", i / 2),
                        new: format!("item {}: closed", i / 2),
                    },
                    None,
                )
                .await
        }
    })
    .await;
    assert_eq!(failures(&results), Vec::<String>::new());

    for note_id in notes {
        let note = services
            .get_note(ws.clone(), note_id)
            .await
            .expect("get note");
        assert!(!note.content.contains("open"), "{}", note.content);
    }
}

/// Parallel `task.updateStatus` calls on LINKED checkbox lines of one note:
/// each redirects to its task note and then rewrites its own line in the
/// shared parent (materialization). Every task must end `complete` and every
/// line must show `[x]` — no projection may be dropped after losing races
/// against the other parent rewrites.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_linked_task_status_updates_all_materialize() {
    let (services, ws, _tmp) = setup().await;
    let content = std::iter::once("# Plan".to_string())
        .chain((0..WRITERS).map(|i| format!("@@@task\n# Linked step {i:02}\nDo step {i}.\n@@@")))
        .collect::<Vec<_>>()
        .join("\n\n");
    let note_id = create_note_with(&services, &ws, content).await;
    let before = services
        .list_note_tasks(ws.clone(), note_id.clone())
        .await
        .expect("list tasks");
    assert_eq!(
        before.len(),
        WRITERS,
        "task blocks converted to linked lines"
    );

    let results = run_together(WRITERS, |i| {
        let (services, ws, note_id) = (Arc::clone(&services), ws.clone(), note_id.clone());
        async move {
            services
                .task_update_status(
                    ws,
                    note_id,
                    format!("Linked step {i:02}"),
                    "done".to_string(),
                    None,
                )
                .await
        }
    })
    .await;
    assert_eq!(failures(&results), Vec::<String>::new());

    let after = services
        .list_note_tasks(ws.clone(), note_id.clone())
        .await
        .expect("list tasks");
    let stale: Vec<&str> = after
        .iter()
        .filter(|t| t.status != "done")
        .map(|t| t.text.as_str())
        .collect();
    let note = services
        .get_note(ws.clone(), note_id)
        .await
        .expect("get note");
    assert!(
        stale.is_empty(),
        "lines not materialized to [x]: {stale:?}\n{}",
        note.content
    );
    for task in &after {
        let id = task
            .task_note_id
            .clone()
            .expect("linked line carries its task note id");
        let task_note = services
            .get_note(ws.clone(), NoteId::from(id.as_str()))
            .await
            .expect("get task note");
        assert_eq!(
            task_note.metadata.task.as_ref().map(|t| t.status),
            Some(TaskStatus::Complete),
            "{}",
            task.text
        );
    }
}

/// The note's stored content must be the content of its newest version
/// snapshot: every text write commits its row and snapshot together.
async fn assert_content_matches_latest_version(services: &Services, ws: &WorkspaceId, id: &NoteId) {
    let note = services
        .get_note(ws.clone(), id.clone())
        .await
        .expect("get note");
    let latest = services
        .list_note_versions(ws.clone(), id.clone())
        .await
        .expect("list versions")
        .last()
        .expect("at least one version")
        .v;
    let version = services
        .get_note_version(ws.clone(), id.clone(), latest)
        .await
        .expect("get version");
    assert_eq!(note.content, version.content);
}

/// Whole-note editor saves (`note.update` with `content`) racing parallel
/// `note.add` writers on one note: the saves take the note's write lane like
/// every other text writer, so no add exhausts its bounded `Conflict` retry
/// against an unlaned save landing inside its read→persist window.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_editor_saves_and_adds_all_apply() {
    let (services, ws, _tmp) = setup().await;
    let note_id = create_note_with(&services, &ws, "# Log".to_string()).await;

    let results = run_together(WRITERS, |i| {
        let (services, ws, note_id) = (Arc::clone(&services), ws.clone(), note_id.clone());
        async move {
            if i % 2 == 0 {
                services
                    .update_note(
                        ws,
                        note_id,
                        NoteUpdateInput {
                            content: Some(format!("# Log\nsaved by editor {i}")),
                            ..Default::default()
                        },
                    )
                    .await
                    .map(|_| ())
            } else {
                services
                    .add_to_note(
                        ws,
                        note_id,
                        NoteAddInput {
                            content: format!("entry {i} from writer"),
                            heading: None,
                            position: Some("end".to_string()),
                        },
                        None,
                    )
                    .await
                    .map(|_| ())
            }
        }
    })
    .await;
    assert_eq!(failures(&results), Vec::<String>::new());
    assert_content_matches_latest_version(&services, &ws, &note_id).await;
}

/// `note.restoreVersion` calls racing parallel `note.add` writers on one
/// note: restores take the note's write lane, so every writer lands.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_restores_and_adds_all_apply() {
    let (services, ws, _tmp) = setup().await;
    let note_id = create_note_with(&services, &ws, "# Log".to_string()).await;
    let first = services
        .list_note_versions(ws.clone(), note_id.clone())
        .await
        .expect("list versions")
        .first()
        .expect("initial version")
        .v;

    let results = run_together(WRITERS, |i| {
        let (services, ws, note_id) = (Arc::clone(&services), ws.clone(), note_id.clone());
        async move {
            if i % 2 == 0 {
                services
                    .restore_note_version(ws, note_id, first, None)
                    .await
                    .map(|_| ())
            } else {
                services
                    .add_to_note(
                        ws,
                        note_id,
                        NoteAddInput {
                            content: format!("entry {i} from writer"),
                            heading: None,
                            position: Some("end".to_string()),
                        },
                        None,
                    )
                    .await
                    .map(|_| ())
            }
        }
    })
    .await;
    assert_eq!(failures(&results), Vec::<String>::new());
    assert_content_matches_latest_version(&services, &ws, &note_id).await;
}

/// Agent ids for the metadata racers, one per writer index.
fn racer_agent(i: usize) -> String {
    format!("agent-00000000-0000-4000-8000-{i:012}")
}

/// Whole-row text writers (`note.restoreVersion`, or a blind `note.update`
/// content save) racing metadata writes on one task note: each metadata
/// write either lands before a text writer's read or waits for its persist,
/// so no text writer reverts it from the copy it read. Every assignment and
/// the completion must survive.
async fn race_whole_row_writers_against_metadata(blind_save: bool) {
    let (services, ws, _tmp) = setup().await;
    let note_id = create_note_with(&services, &ws, "# Task".to_string()).await;
    services
        .mark_as_task(
            ws.clone(),
            note_id.clone(),
            "not_started".into(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .expect("markAsTask");
    let first = services
        .list_note_versions(ws.clone(), note_id.clone())
        .await
        .expect("list versions")
        .first()
        .expect("initial version")
        .v;

    let results = run_together(WRITERS, |i| {
        let (services, ws, note_id) = (Arc::clone(&services), ws.clone(), note_id.clone());
        async move {
            match i % 4 {
                1 => services
                    .assign_agent(ws, note_id, racer_agent(i), None)
                    .await
                    .map(|_| ()),
                3 => services
                    .task_update_note_status(ws, note_id, "complete".into(), None, None)
                    .await
                    .map(|_| ()),
                _ if blind_save => services
                    .update_note(
                        ws,
                        note_id,
                        NoteUpdateInput {
                            content: Some(format!("# Task\nsaved by editor {i}")),
                            ..Default::default()
                        },
                    )
                    .await
                    .map(|_| ()),
                _ => services
                    .restore_note_version(ws, note_id, first, None)
                    .await
                    .map(|_| ()),
            }
        }
    })
    .await;
    assert_eq!(failures(&results), Vec::<String>::new());

    let note = services
        .get_note(ws.clone(), note_id.clone())
        .await
        .expect("get note");
    let task = note.metadata.task.expect("still a task");
    assert_eq!(task.status, TaskStatus::Complete, "completion reverted");
    let mut assigned: Vec<String> = task
        .assigned_agent_ids
        .iter()
        .map(|a| a.0.clone())
        .collect();
    assigned.sort();
    let expected: Vec<String> = (0..WRITERS)
        .filter(|i| i % 4 == 1)
        .map(racer_agent)
        .collect();
    assert_eq!(assigned, expected, "assignments reverted");
    assert_content_matches_latest_version(&services, &ws, &note_id).await;
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_restores_and_metadata_writes_keep_metadata() {
    race_whole_row_writers_against_metadata(false).await;
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_editor_saves_and_metadata_writes_keep_metadata() {
    race_whole_row_writers_against_metadata(true).await;
}

/// Metadata writes per burst writer in
/// [`concurrent_metadata_burst_and_adds_all_apply`]: enough rev bumps that an
/// add whose read→persist window they may enter would exhaust its 5 attempts.
const BURST_WRITES: usize = 12;

/// A burst of title/tags writes racing `note.add` on one note: metadata
/// writes take the lane, so none moves the rev inside an add's read→persist
/// window and every add lands without `-32005`.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_metadata_burst_and_adds_all_apply() {
    let (services, ws, _tmp) = setup().await;
    let note_id = create_note_with(&services, &ws, "# Log".to_string()).await;

    let results = run_together(WRITERS, |i| {
        let (services, ws, note_id) = (Arc::clone(&services), ws.clone(), note_id.clone());
        async move {
            if i % 2 == 0 {
                for n in 0..BURST_WRITES {
                    services
                        .update_note_metadata(
                            ws.clone(),
                            note_id.clone(),
                            Some(format!("Title {i}-{n}")),
                            Some(vec![format!("tag-{i}-{n}")]),
                            None,
                            None,
                        )
                        .await?;
                }
                Ok(())
            } else {
                services
                    .add_to_note(
                        ws,
                        note_id,
                        NoteAddInput {
                            content: format!("entry {i} from writer"),
                            heading: None,
                            position: Some("end".to_string()),
                        },
                        None,
                    )
                    .await
                    .map(|_| ())
            }
        }
    })
    .await;
    assert_eq!(failures(&results), Vec::<String>::new());
    let note = services
        .get_note(ws.clone(), note_id.clone())
        .await
        .expect("get note");
    for i in (0..WRITERS).filter(|i| i % 2 == 1) {
        assert!(
            note.content.contains(&format!("entry {i} from writer")),
            "add {i} missing"
        );
    }
    assert_content_matches_latest_version(&services, &ws, &note_id).await;
}
