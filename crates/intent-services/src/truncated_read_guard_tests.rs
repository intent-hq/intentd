//! The truncated-read write guard (`baseContentBytes`) on whole-content note
//! writes: `note.setContent` ([`WorkspaceApi::set_note_content_guarded`]) and
//! the content arm of `note.update`. Pins the contract in
//! `docs/protocol/methods/notes-tasks.md` "Truncated note reads": the base the
//! daemon resolves per path, the refusal (`Error::ContentBaseMismatch`, no
//! write), the error precedence, and that unguarded writers (agents, MCP,
//! surgical ops) are untouched.

use super::tests::{setup, setup_versioned};
use super::*;
use std::fmt::Write as _;

fn bytes(s: &str) -> u64 {
    u64::try_from(s.len()).expect("size fits")
}

fn large_note() -> String {
    (0..2_000).fold(String::new(), |mut out, i| {
        let _ = writeln!(out, "line {i} — ünïcode");
        out
    })
}

async fn guarded_set(
    svc: &Services,
    ws: &WorkspaceId,
    id: &NoteId,
    content: &str,
    expected_version: Option<i64>,
    base: Option<u64>,
) -> Result<NoteSetContentResult> {
    svc.set_note_content_guarded(
        ws.clone(),
        id.clone(),
        content.to_string(),
        false,
        expected_version,
        base,
        None,
    )
    .await
}

fn assert_mismatch<T: std::fmt::Debug>(r: Result<T>, base: u64, resolved: Option<u64>) {
    match r {
        Err(Error::ContentBaseMismatch {
            base_content_bytes,
            resolved_base_bytes,
        }) => {
            assert_eq!(base_content_bytes, base);
            assert_eq!(resolved_base_bytes, resolved);
        }
        other => panic!("expected content-base-mismatch, got {other:?}"),
    }
}

/// A full read's size matches the stored content, so the guarded write
/// proceeds exactly as an unguarded one.
#[intent_test_macros::daemon_test]
async fn set_content_with_matching_base_writes() {
    let body = large_note();
    let (_tmp, svc, ws, id) = setup(&body).await;
    let edited = format!("{body}tail\n");
    let r = guarded_set(&svc, &ws, &id, &edited, Some(0), Some(bytes(&body)))
        .await
        .expect("matching base writes");
    assert_eq!(r.new_content, edited);
    assert_eq!(r.rev, 1);
}

/// A truncated prefix is always smaller than the stored note: the write is
/// refused with the stored size as the resolved base, and nothing changes.
#[intent_test_macros::daemon_test]
async fn set_content_from_truncated_read_is_refused_without_write() {
    let body = large_note();
    let (_tmp, svc, ws, id) = setup(&body).await;
    let cut = intent_core::truncated_content_len(&body, 1_000).expect("truncated");
    let prefix = &body[..cut];
    let edited = format!("{prefix}typed in the read-only view\n");
    let r = guarded_set(&svc, &ws, &id, &edited, Some(0), Some(bytes(prefix))).await;
    assert_mismatch(r, bytes(prefix), Some(bytes(&body)));
    let stored = svc.store.get_note(&ws, &id).await.expect("get");
    assert_eq!(stored.content, body, "content after the cut survives");
    assert_eq!(stored.rev, 0, "no version");
    // Without `expectedVersion` the base is the stored content too.
    let r = guarded_set(&svc, &ws, &id, &edited, None, Some(bytes(prefix))).await;
    assert_mismatch(r, bytes(prefix), Some(bytes(&body)));
    assert_eq!(svc.store.get_note(&ws, &id).await.expect("get").rev, 0);
}

/// The base check runs before the reduction guard: a refused write reports
/// the mismatch, not the `-32603` reduction message.
#[intent_test_macros::daemon_test]
async fn base_mismatch_precedes_reduction_guard() {
    let body = large_note();
    let (_tmp, svc, ws, id) = setup(&body).await;
    let r = guarded_set(&svc, &ws, &id, "tiny\n", None, Some(5)).await;
    assert_mismatch(r, 5, Some(bytes(&body)));
}

/// Stale `expectedVersion` with a retained snapshot: the resolved base is the
/// snapshot, not the stored note, so the writer's own base size passes and
/// the write merges; the stored size is a mismatch.
#[intent_test_macros::daemon_test]
async fn stale_expected_version_resolves_the_merge_snapshot() {
    let base = "alpha\nbeta\ngamma";
    let (_tmp, svc, ws, id) = setup_versioned(base).await;
    let current = "alpha\nbeta\ngamma\ndelta";
    svc.set_note_content(ws.clone(), id.clone(), current.into(), false, None, None)
        .await
        .expect("concurrent writer");

    let wrong = guarded_set(
        &svc,
        &ws,
        &id,
        "alpha\nbeta-A\ngamma",
        Some(0),
        Some(bytes(current)),
    )
    .await;
    assert_mismatch(wrong, bytes(current), Some(bytes(base)));
    assert_eq!(svc.store.get_note(&ws, &id).await.expect("get").rev, 1);

    let merged = guarded_set(
        &svc,
        &ws,
        &id,
        "alpha\nbeta-A\ngamma",
        Some(0),
        Some(bytes(base)),
    )
    .await
    .expect("writer's base size passes and merges");
    assert_eq!(merged.new_content, "alpha\nbeta-A\ngamma\ndelta");
    assert_eq!(merged.rev, 2);
}

/// Stale `expectedVersion` with no retained snapshot: the base is unknown,
/// so a guarded write is refused (`resolvedBaseBytes: null`) instead of
/// degrading to last-writer-wins.
#[intent_test_macros::daemon_test]
async fn stale_expected_version_without_snapshot_is_refused() {
    let (_tmp, svc, ws, id) = setup("v0 body").await;
    svc.set_note_content(ws.clone(), id.clone(), "v1 body".into(), false, None, None)
        .await
        .expect("other writer");
    let r = guarded_set(&svc, &ws, &id, "v2 body", Some(0), Some(bytes("v0 body"))).await;
    assert_mismatch(r, bytes("v0 body"), None);
    let stored = svc.store.get_note(&ws, &id).await.expect("get");
    assert_eq!(stored.content, "v1 body");
    assert_eq!(stored.rev, 1);
}

/// An `expectedVersion` above the current rev is `Conflict` before the base
/// check (precedence 2 before 3).
#[intent_test_macros::daemon_test]
async fn future_expected_version_conflicts_before_base_check() {
    let (_tmp, svc, ws, id) = setup("body").await;
    let r = guarded_set(&svc, &ws, &id, "new body", Some(7), Some(1)).await;
    assert!(matches!(r, Err(Error::Conflict { .. })), "{r:?}");
}

/// Agents, MCP and surgical writes never send a base size, so large notes
/// stay fully writable through them.
#[intent_test_macros::daemon_test]
async fn unguarded_writes_to_large_notes_still_work() {
    let body = large_note();
    let (_tmp, svc, ws, id) = setup(&body).await;
    let replaced = format!("{body}agent line\n");
    let r = svc
        .set_note_content(ws.clone(), id.clone(), replaced.clone(), false, None, None)
        .await
        .expect("unguarded setContent");
    assert_eq!(r.new_content, replaced);
    let r = guarded_set(&svc, &ws, &id, "small\n", None, None).await;
    assert!(
        matches!(r, Err(Error::Internal(ref m)) if m.contains("CONTENT REDUCTION")),
        "None behaves exactly like set_note_content: {r:?}"
    );
    svc.add_to_note(
        ws.clone(),
        id.clone(),
        NoteAddInput {
            content: "appended by an agent".into(),
            heading: None,
            position: None,
        },
        None,
    )
    .await
    .expect("surgical add");
    let stored = svc.store.get_note(&ws, &id).await.expect("get");
    assert!(stored.content.ends_with("appended by an agent"));
}

/// `note.update` content arm: the base is the stored content after the
/// strict `expectedVersion` check; the metadata arm ignores the param.
#[intent_test_macros::daemon_test]
async fn update_note_content_arm_is_guarded() {
    let body = large_note();
    let (_tmp, svc, ws, id) = setup(&body).await;
    let update =
        |content: Option<&str>, expected: Option<i64>, base: Option<u64>| NoteUpdateInput {
            content: content.map(str::to_string),
            title: content.is_none().then(|| "Renamed".to_string()),
            expected_version: expected,
            base_content_bytes: base,
            ..Default::default()
        };

    let r = svc
        .update_note(
            ws.clone(),
            id.clone(),
            update(Some("x\n"), Some(5), Some(2)),
        )
        .await;
    assert!(
        matches!(r, Err(Error::Conflict { .. })),
        "stale first: {r:?}"
    );

    let r = svc
        .update_note(
            ws.clone(),
            id.clone(),
            update(Some("x\n"), Some(0), Some(2)),
        )
        .await;
    assert_mismatch(r, 2, Some(bytes(&body)));
    let r = svc
        .update_note(ws.clone(), id.clone(), update(Some("x\n"), None, Some(2)))
        .await;
    assert_mismatch(r, 2, Some(bytes(&body)));
    assert_eq!(svc.store.get_note(&ws, &id).await.expect("get").rev, 0);

    let note = svc
        .update_note(ws.clone(), id.clone(), update(None, None, Some(2)))
        .await
        .expect("metadata arm ignores baseContentBytes");
    assert_eq!(note.title, "Renamed");
    assert_eq!(note.content, body);

    let note = svc
        .update_note(
            ws.clone(),
            id.clone(),
            update(Some("x\n"), None, Some(bytes(&body))),
        )
        .await
        .expect("matching base writes");
    assert_eq!(note.content, "x\n");
}

const RACING_TEXT: &str = "a concurrent writer text\n";

/// Arm a one-shot trigger that lands a concurrent writer between the guard
/// check and the gated persist: the next `UPDATE` of the note row instead
/// writes [`RACING_TEXT`] at `rev + 1` and is itself ignored, so the
/// guarded write's rev gate misses deterministically (no timing).
async fn arm_racing_writer(svc: &Services) {
    sqlx::raw_sql(&format!(
        "CREATE TABLE race_armed(n INTEGER);
         INSERT INTO race_armed VALUES (1);
         CREATE TRIGGER race_writer BEFORE UPDATE ON note
         WHEN (SELECT n FROM race_armed) = 1 BEGIN
             UPDATE race_armed SET n = 0;
             UPDATE note SET content = '{RACING_TEXT}', rev = OLD.rev + 1
                 WHERE id = OLD.id AND workspace_id = OLD.workspace_id;
             SELECT RAISE(IGNORE);
         END;"
    ))
    .execute(svc.store.write_pool())
    .await
    .expect("arm racing writer");
}

async fn assert_racing_text_survived(svc: &Services, ws: &WorkspaceId, id: &NoteId) {
    let stored = svc.store.get_note(ws, id).await.expect("get");
    assert_eq!(
        stored.content, RACING_TEXT,
        "the concurrent writer's text survives"
    );
    assert_eq!(stored.rev, 1);
}

/// Guarded `note.setContent` without `expectedVersion` that loses the rev
/// gate to a concurrent writer is refused as a base mismatch on the retry —
/// never `-32005`, never an overwrite of the text it did not read.
#[intent_test_macros::daemon_test]
async fn guarded_set_content_without_expected_version_refuses_a_lost_race() {
    let body = large_note();
    let (_tmp, svc, ws, id) = setup(&body).await;
    arm_racing_writer(&svc).await;
    let edited = format!("{body}mine\n");
    let r = guarded_set(&svc, &ws, &id, &edited, None, Some(bytes(&body))).await;
    assert_mismatch(r, bytes(&body), Some(bytes(RACING_TEXT)));
    assert_racing_text_survived(&svc, &ws, &id).await;
}

/// Guarded write with a current `expectedVersion` whose retry finds no
/// snapshot for that rev: the base is unknown, so it is refused
/// (`resolvedBaseBytes: null`) instead of degrading to last-writer-wins.
#[intent_test_macros::daemon_test]
async fn guarded_retry_without_snapshot_never_falls_back_to_lww() {
    // `setup` inserts the row directly: no snapshot exists at rev 0.
    let (_tmp, svc, ws, id) = setup("v0 body\n").await;
    arm_racing_writer(&svc).await;
    let r = guarded_set(
        &svc,
        &ws,
        &id,
        "v0 body\nmine\n",
        Some(0),
        Some(bytes("v0 body\n")),
    )
    .await;
    assert_mismatch(r, bytes("v0 body\n"), None);
    assert_racing_text_survived(&svc, &ws, &id).await;
}

/// With a retained snapshot the same race keeps the existing merge
/// behavior: the retry re-checks the base against the snapshot and merges
/// the writer's intent onto the concurrent text.
#[intent_test_macros::daemon_test]
async fn guarded_retry_with_snapshot_rechecks_and_merges() {
    let (_tmp, svc, ws, id) = setup_versioned("v0 body\n").await;
    arm_racing_writer(&svc).await;
    let r = guarded_set(
        &svc,
        &ws,
        &id,
        "v0 body\nmine\n",
        Some(0),
        Some(bytes("v0 body\n")),
    )
    .await
    .expect("retry merges from the checked snapshot");
    assert!(r.new_content.contains("mine"), "{}", r.new_content);
    assert!(
        r.new_content.contains("concurrent writer"),
        "{}",
        r.new_content
    );
    assert_eq!(r.rev, 2);
}

/// `note.update` content arm: a guarded write without `expectedVersion`
/// that loses the race is a base mismatch, not `-32005`; with
/// `expectedVersion` the strict compare-and-set `Conflict` is unchanged.
#[intent_test_macros::daemon_test]
async fn guarded_update_note_refuses_a_lost_race() {
    let body = large_note();
    let (_tmp, svc, ws, id) = setup(&body).await;
    arm_racing_writer(&svc).await;
    let input = NoteUpdateInput {
        content: Some("rewritten\n".into()),
        base_content_bytes: Some(bytes(&body)),
        ..Default::default()
    };
    let r = svc.update_note(ws.clone(), id.clone(), input).await;
    assert_mismatch(r, bytes(&body), Some(bytes(RACING_TEXT)));
    assert_racing_text_survived(&svc, &ws, &id).await;

    let (_tmp2, svc, ws, id) = setup(&body).await;
    arm_racing_writer(&svc).await;
    let input = NoteUpdateInput {
        content: Some("rewritten\n".into()),
        expected_version: Some(0),
        base_content_bytes: Some(bytes(&body)),
        ..Default::default()
    };
    let r = svc.update_note(ws.clone(), id.clone(), input).await;
    assert!(matches!(r, Err(Error::Conflict { .. })), "{r:?}");
    assert_racing_text_survived(&svc, &ws, &id).await;
}
