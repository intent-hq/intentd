//! Isolated task.list baseline and a deterministic read-cost reproducer.
//! Timings are diagnostic only: no wall-clock thresholds run in CI.

use super::{note, workspace, TempDb};
use crate::{compute_task_stats, workspace_task_list, Services};
use intent_core::{NoteId, TaskListResult, TaskMetadata, WorkspaceApi, WorkspaceId};
use intent_store::Store;
use std::fmt::Write as _;
use std::time::Instant;

async fn seed(store: &Store, tasks: usize, plain: usize) -> WorkspaceId {
    let ws = WorkspaceId::from("task-list-benchmark");
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let mut spec = String::new();
    for i in 0..tasks {
        writeln!(spec, "- [Task](intent://local/task/task-{i:03})").unwrap();
    }
    store.insert_note(&note(&ws, "spec", &spec)).await.unwrap();
    for i in 0..tasks + plain {
        let id = if i < tasks {
            format!("task-{i:03}")
        } else {
            format!("plain-{i:03}")
        };
        let mut n = note(&ws, &id, "");
        n.title.clone_from(&id);
        n.created_at = format!("2026-01-01T00:00:00.{i:03}Z");
        n.updated_at.clone_from(&n.created_at);
        if i < tasks {
            n.parent_id = Some(NoteId::from("spec"));
            n.metadata.task = Some(TaskMetadata::default());
        }
        store.insert_note(&n).await.unwrap();
    }
    ws
}

async fn bodies(store: &Store, ws: &WorkspaceId, task_bytes: usize, plain_bytes: usize) {
    // Direct fixture writes avoid version-history/event overhead; never production data.
    sqlx::query("UPDATE note SET content = CASE WHEN task_json IS NULL THEN ? ELSE ? END WHERE workspace_id = ? AND id != 'spec'")
        .bind("p".repeat(plain_bytes))
        .bind("t".repeat(task_bytes))
        .bind(ws.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
}

#[intent_test_macros::daemon_test]
async fn task_list_large_bodies_preserve_response() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = seed(&store, 4, 8).await;
    let svc = Services::new(store.clone());
    let before = serde_json::to_value(svc.task_list(ws.clone(), None).await.unwrap()).unwrap();
    bodies(&store, &ws, 128 * 1024, 256 * 1024).await;
    let after = serde_json::to_value(svc.task_list(ws, None).await.unwrap()).unwrap();
    assert_eq!(before, after);
    assert_eq!(after["tasks"].as_array().unwrap().len(), 4);
    assert_eq!(after["stats"]["total"], 4);
}

/// Fails on the baseline's full-note read. Enable when the optimized projection
/// lands. A view makes evaluating any non-spec body fail at the SQL boundary,
/// even with tiny fixtures: this tests reads, not timing or response omission.
#[intent_test_macros::daemon_test]
#[ignore = "baseline reproducer: task.list currently reads non-spec bodies"]
async fn task_list_does_not_read_non_spec_bodies() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = seed(&store, 4, 8).await;
    let svc = Services::new(store.clone());
    let expected = serde_json::to_value(svc.task_list(ws.clone(), None).await.unwrap()).unwrap();
    sqlx::query("ALTER TABLE note RENAME TO task_list_body_probe")
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::query("CREATE VIEW note AS SELECT id, workspace_id, title, CASE WHEN id = 'spec' THEN content ELSE json_extract('body read forbidden', '$') END AS content, content_type, tags, is_pinned, is_archived, is_default, parent_id, visibility, task_json, created_at, rev, updated_at FROM task_list_body_probe")
        .execute(store.write_pool()).await.unwrap();
    // Validate the trap independently: spec reads work, full-note reads fail.
    store.get_note(&ws, &NoteId::from("spec")).await.unwrap();
    assert!(store.list_notes(&ws).await.is_err());
    let actual = svc
        .task_list(ws, None)
        .await
        .expect("task.list must not evaluate task or unrelated note bodies");
    assert_eq!(serde_json::to_value(actual).unwrap(), expected);
}

#[intent_test_macros::daemon_test]
#[ignore = "manual synthetic task.list timing baseline; run with --ignored --nocapture"]
async fn task_list_latency_baseline() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = seed(&store, 64, 192).await;
    let svc = Services::new(store.clone());
    let mut expected = None;
    for (label, task_bytes, plain_bytes) in [
        ("empty", 0, 0),
        ("large_tasks", 1024 * 1024, 0),
        ("large_plain", 0, 1024 * 1024),
        ("large_both", 1024 * 1024, 1024 * 1024),
    ] {
        bodies(&store, &ws, task_bytes, plain_bytes).await;
        // First request after the fixture write, NOT an OS-cold disk measurement.
        let start = Instant::now();
        let first = svc.task_list(ws.clone(), None).await.unwrap();
        let first_us = start.elapsed().as_micros();
        let bytes = serde_json::to_vec(&first).unwrap();
        if let Some(ref expected) = expected {
            assert_eq!(&bytes, expected);
        } else {
            expected = Some(bytes.clone());
        }
        eprintln!("TASK_LIST fixture={label} tasks=64 plain=192 task_body_bytes={task_bytes} plain_body_bytes={plain_bytes} result_bytes={} first_handler_us={first_us}", bytes.len());
        for sample in 0..7 {
            let start = Instant::now();
            svc.require_member(&ws).await.unwrap();
            let authorization_us = start.elapsed().as_micros();
            let start = Instant::now();
            let notes = store.list_notes(&ws).await.unwrap();
            let database_and_decode_us = start.elapsed().as_micros();
            let hydrated_bytes: usize = notes.iter().map(|n| n.content.len()).sum();
            let start = Instant::now();
            let projected = TaskListResult {
                tasks: workspace_task_list(&notes),
                stats: compute_task_stats(&notes),
            };
            let projection_us = start.elapsed().as_micros();
            let start = Instant::now();
            assert_eq!(serde_json::to_vec(&projected).unwrap(), bytes);
            let serialization_us = start.elapsed().as_micros();
            drop(notes);
            let start = Instant::now();
            let actual = svc.task_list(ws.clone(), None).await.unwrap();
            let handler_us = start.elapsed().as_micros();
            assert_eq!(serde_json::to_vec(&actual).unwrap(), bytes);
            eprintln!("TASK_LIST fixture={label} sample={sample} authorization_us={authorization_us} database_and_decode_us={database_and_decode_us} projection_us={projection_us} serialization_us={serialization_us} handler_us={handler_us} hydrated_bytes={hydrated_bytes}");
        }
        let (result, statements) =
            crate::test_tracing::count_sqlx_statements(svc.task_list(ws.clone(), None)).await;
        result.unwrap();
        eprintln!("TASK_LIST fixture={label} handler_statements={statements} caller=daemon");
    }
}
