//! Conformance table for the conditional-write error precedence of the note
//! content pipeline (`persist_merged_content`): `Conflict` → reduction guard →
//! merged-text validation → persist. Every row is (method × precondition) →
//! outcome, names the protocol clause it pins, and asserts the store is
//! untouched on rejection. intentd#1863 (`dd4e5783`) shipped two precedence
//! regressions past a green suite of per-case tests; this table exists so a
//! refactor that reorders the pipeline fails here, naming the row.
//!
//! Clause keys used in `Row::clause`:
//! - `notes-tasks §5.2 row`: `docs/protocol/methods/notes-tasks.md` §5.2, the
//!   `note.setContent` table row.
//! - `notes-tasks §5.2 merge`: same file, the "Three-way merge on stale
//!   `expectedVersion`" paragraph.
//! - `errors §9 -32005`: `docs/protocol/09-error-codes.md`, the `Conflict` row.
//! - `versioning 9.11 setContent`: `docs/protocol/versioning.md`, the 9.11
//!   paragraph "`note.setContent` merges on a stale `expectedVersion`".

use intent_core::{Error, WorkspaceApi};

use super::tests::{setup, setup_versioned};

/// Store state the writer's call runs against.
struct Precondition {
    /// Content the note is seeded with at rev 0.
    seed: String,
    /// Whether a `note_version` snapshot exists at rev 0 (a recoverable base).
    snapshot_at_rev0: bool,
    /// An unconditional write by another writer that moves the note to rev 1:
    /// `(content, confirm_replacement)`.
    intervening: Option<(String, bool)>,
    /// The writer's `note.setContent` arguments.
    content: String,
    confirm_replacement: bool,
    expected_version: Option<i64>,
}

enum Expect {
    /// `Error::Conflict` carrying the current row; nothing persisted.
    Conflict,
    /// `Error::Internal` whose message starts with this prefix; nothing
    /// persisted.
    Rejected { message_prefix: &'static str },
    /// The write lands with exactly this text; `rev` = pre-write rev + 1 in
    /// both the result and the store.
    Persisted { content: String },
}

struct Row {
    name: &'static str,
    clause: &'static str,
    method: &'static str,
    precondition: Precondition,
    expect: Expect,
}

const SET_CONTENT: &str = "note.setContent";
const EMPTY_MSG: &str = "Content cannot be empty.";
const TRUNCATED_MSG: &str = "Content appears to be truncated";
const REDUCTION_MSG: &str = "⚠️ CONTENT REDUCTION DETECTED";

/// One `note.setContent` row with a rev-0 snapshot, an intervening
/// unconfirmed write to rev 1, and an unconfirmed writer call.
fn set_content_row(
    name: &'static str,
    clause: &'static str,
    seed: &str,
    intervening: &str,
    content: &str,
    expected_version: Option<i64>,
    expect: Expect,
) -> Row {
    Row {
        name,
        clause,
        method: SET_CONTENT,
        precondition: Precondition {
            seed: seed.to_string(),
            snapshot_at_rev0: true,
            intervening: Some((intervening.to_string(), false)),
            content: content.to_string(),
            confirm_replacement: false,
            expected_version,
        },
        expect,
    }
}

/// `n` lines of 18 chars each (`line-{i}-0123456789`), joined by `\n`.
fn numbered_lines(range: std::ops::Range<usize>) -> Vec<String> {
    range.map(|i| format!("line-{i}-0123456789")).collect()
}

fn set_content_rows() -> Vec<Row> {
    let conflict = Expect::Conflict;
    let mut rows = vec![
        // 1. A rev above the stored one was never served: plain -32005, no write.
        set_content_row(
            "1 future expectedVersion + valid content -> Conflict",
            "notes-tasks §5.2 merge; errors §9 -32005",
            "body",
            "body v1",
            "impossible base",
            Some(7),
            conflict,
        ),
        // 2. Conflict wins over the empty guard (normalization is error-free
        //    and validation runs on the merged text, never reached).
        set_content_row(
            "2a future expectedVersion + empty payload -> Conflict (not the empty guard)",
            "notes-tasks §5.2 row (-32005 in exactly two cases); errors §9 -32005",
            "body",
            "body v1",
            "",
            Some(999),
            Expect::Conflict,
        ),
        set_content_row(
            "2b future expectedVersion + quoted-empty payload -> Conflict (not the empty guard)",
            "notes-tasks §5.2 row (-32005 in exactly two cases); errors §9 -32005",
            "body",
            "body v1",
            "\"\"",
            Some(999),
            Expect::Conflict,
        ),
        // 3. Conflict wins over the truncation guard.
        set_content_row(
            "3 future expectedVersion + short `...` payload -> Conflict (not the truncation guard)",
            "notes-tasks §5.2 row (-32005 in exactly two cases); errors §9 -32005",
            "body",
            "body v1",
            "short...",
            Some(999),
            Expect::Conflict,
        ),
        // 4. Conflict wins over the reduction guard (20 chars -> 1 char, unconfirmed).
        set_content_row(
            "4 future expectedVersion + >50% shorter unconfirmed content -> Conflict (not the reduction guard)",
            "notes-tasks §5.2 row (-32005 in exactly two cases); errors §9 -32005",
            "0123456789ABCDEFGHIJ",
            "0123456789ABCDEFGHIJK",
            "x",
            Some(7),
            Expect::Conflict,
        ),
        // 5. Two zero-conflict partial deletions (`ab` -> `a`, `ab` -> `b`,
        //    each exactly 50 % so the reduction guard passes) merge to the
        //    empty string: the empty guard applies to the MERGED text.
        set_content_row(
            "5 stale expectedVersion, zero-conflict merge empties the note -> empty guard, no write",
            "notes-tasks §5.2 merge (merged text is what the daemon cleans and persists)",
            "ab",
            "a",
            "b",
            Some(0),
            Expect::Rejected {
                message_prefix: EMPTY_MSG,
            },
        ),
    ];
    rows.extend(truncation_and_reduction_rows());
    rows.extend(persisting_rows());
    rows
}

/// Rows 6–8: the merged-text truncation guard and the base-measured
/// reduction guard.
fn truncation_and_reduction_rows() -> Vec<Row> {
    // 6. Replaces `tests::set_content_stale_merge_that_looks_truncated_is_rejected`,
    //    whose incoming text (`one twothree...`) already failed
    //    `validate_set_content` on its own and so passed whether validation
    //    ran before or after the merge. Here BOTH inputs validate alone:
    //    the current text keeps a newline (`alpha\n… omega...`), the
    //    incoming text keeps the 30-char filler (72 chars, ≥ 50), and only
    //    the merged text — newline replaced by a space AND filler deleted —
    //    is a 41-char single line ending in `...`. The intervening write
    //    drops 31 of 72 chars (43 %), under the unconfirmed reduction cap.
    let filler = "x".repeat(30);
    let mid = "quick brown fox jumps over";
    let tail = " omega...";
    let base = format!("alpha\n{mid} {filler}{tail}");
    let current = format!("alpha\n{mid}{tail}");
    let incoming = format!("alpha {mid} {filler}{tail}");
    debug_assert_eq!(format!("alpha {mid}{tail}").chars().count(), 41);

    // 7–8. The writer's base is 10 lines; a concurrent writer tripled the
    //      note to 30. Guard measured against the base: −10 % vs base (−70 %
    //      vs current) lands; −60 % vs base is rejected unless confirmed.
    let lines = numbered_lines(0..10);
    let base10 = lines.join("\n");
    let tripled = numbered_lines(0..30).join("\n");
    let minus_ten_pct = lines[1..].join("\n");
    let minus_sixty_pct = lines[6..].join("\n");
    let merged_minus_ten = numbered_lines(1..30).join("\n");
    let merged_minus_sixty = numbered_lines(6..30).join("\n");

    let mut rows = vec![
        set_content_row(
            "6 stale expectedVersion, merged text looks truncated while both inputs validate -> truncation guard, no write",
            "notes-tasks §5.2 merge (merged text is what the daemon cleans and persists)",
            &base,
            &current,
            &incoming,
            Some(0),
            Expect::Rejected {
                message_prefix: TRUNCATED_MSG,
            },
        ),
        set_content_row(
            "7 stale expectedVersion, incoming >50% shorter than current but not than base -> persisted",
            "notes-tasks §5.2 row (reduction guard measured base -> content); versioning 9.11 setContent",
            &base10,
            &tripled,
            &minus_ten_pct,
            Some(0),
            Expect::Persisted {
                content: merged_minus_ten,
            },
        ),
        set_content_row(
            "8a stale expectedVersion, incoming >50% shorter than base, unconfirmed -> reduction guard, no write",
            "notes-tasks §5.2 row (reduction guard measured base -> content, -32603)",
            &base10,
            &tripled,
            &minus_sixty_pct,
            Some(0),
            Expect::Rejected {
                message_prefix: REDUCTION_MSG,
            },
        ),
    ];
    let mut confirmed = set_content_row(
        "8b stale expectedVersion, incoming >50% shorter than base, confirmReplacement -> persisted",
        "notes-tasks §5.2 row (>50 % shorter without confirmReplacement)",
        &base10,
        &tripled,
        &minus_sixty_pct,
        Some(0),
        Expect::Persisted {
            content: merged_minus_sixty,
        },
    );
    confirmed.precondition.confirm_replacement = true;
    rows.push(confirmed);
    rows
}

/// Rows 9–11: the exact path, last-writer-wins without a base, and a clean
/// three-way merge.
fn persisting_rows() -> Vec<Row> {
    let mut exact_absent = set_content_row(
        "9a absent expectedVersion -> replaced verbatim, rev = current + 1",
        "notes-tasks §5.2 row (absent or equal to the current rev -> content replaces the note as-is)",
        "body",
        "",
        "body v1",
        None,
        Expect::Persisted {
            content: "body v1".into(),
        },
    );
    exact_absent.precondition.intervening = None;
    let exact_matching = set_content_row(
        "9b matching expectedVersion -> replaced verbatim, rev = current + 1",
        "notes-tasks §5.2 row (absent or equal to the current rev -> content replaces the note as-is)",
        "body",
        "body v1",
        "body v2",
        Some(1),
        Expect::Persisted {
            content: "body v2".into(),
        },
    );
    // 10. `setup` inserts the row directly: no snapshot exists at rev 0.
    let mut lww = set_content_row(
        "10 stale expectedVersion with no surviving snapshot -> last-writer-wins, persisted verbatim",
        "notes-tasks §5.2 merge (no snapshot survives -> honest last-writer-wins); versioning 9.11 setContent",
        "v0 body",
        "v1 body (other writer)",
        "v2 body (stale writer)",
        Some(0),
        Expect::Persisted {
            content: "v2 body (stale writer)".into(),
        },
    );
    lww.precondition.snapshot_at_rev0 = false;
    let merged = set_content_row(
        "11 stale expectedVersion, clean three-way merge -> merged text persisted, rev bumped",
        "notes-tasks §5.2 merge (non-overlapping hunks from either side apply); versioning 9.11 setContent",
        "alpha\nbeta\ngamma",
        "alpha\nbeta\ngamma\ndelta",
        "alpha\nbeta-A\ngamma",
        Some(0),
        Expect::Persisted {
            content: "alpha\nbeta-A\ngamma\ndelta".into(),
        },
    );
    vec![exact_absent, exact_matching, lww, merged]
}

/// Drive one row against a fresh store and assert its outcome; every panic
/// message carries `row.name` and `row.clause`.
async fn run_row(row: Row) {
    let ctx = format!("row [{}] ({}) pins <{}>", row.name, row.method, row.clause);
    let p = row.precondition;
    let (_tmp, svc, ws, id) = if p.snapshot_at_rev0 {
        setup_versioned(&p.seed).await
    } else {
        setup(&p.seed).await
    };
    if let Some((content, confirm)) = p.intervening {
        svc.set_note_content(ws.clone(), id.clone(), content, confirm, None, None)
            .await
            .unwrap_or_else(|e| panic!("{ctx}: intervening write failed: {e:?}"));
    }
    if !p.snapshot_at_rev0 {
        let base = svc
            .store
            .get_note_version_content_by_rev(&ws, &id, 0)
            .await
            .unwrap_or_else(|e| panic!("{ctx}: snapshot lookup failed: {e:?}"));
        assert_eq!(base, None, "{ctx}: precondition: no snapshot at rev 0");
    }
    let before = svc
        .store
        .get_note(&ws, &id)
        .await
        .unwrap_or_else(|e| panic!("{ctx}: read before write failed: {e:?}"));

    let result = svc
        .set_note_content(
            ws.clone(),
            id.clone(),
            p.content,
            p.confirm_replacement,
            p.expected_version,
            None,
        )
        .await;

    let after = svc
        .store
        .get_note(&ws, &id)
        .await
        .unwrap_or_else(|e| panic!("{ctx}: read after write failed: {e:?}"));
    match row.expect {
        Expect::Conflict => {
            match result {
                Err(Error::Conflict { current }) => {
                    assert_eq!(current["rev"], serde_json::json!(before.rev), "{ctx}");
                    assert_eq!(
                        current["content"],
                        serde_json::json!(before.content),
                        "{ctx}"
                    );
                }
                other => panic!("{ctx}: expected Error::Conflict, got {other:?}"),
            }
            assert_unchanged(&ctx, &before, &after);
        }
        Expect::Rejected { message_prefix } => {
            match result {
                Err(Error::Internal(msg)) if msg.starts_with(message_prefix) => {}
                other => panic!(
                    "{ctx}: expected Error::Internal starting with {message_prefix:?}, got {other:?}"
                ),
            }
            assert_unchanged(&ctx, &before, &after);
        }
        Expect::Persisted { content } => {
            let r =
                result.unwrap_or_else(|e| panic!("{ctx}: expected a persisted write, got {e:?}"));
            assert_eq!(r.new_content, content, "{ctx}: result newContent");
            assert_eq!(r.rev, before.rev + 1, "{ctx}: result rev");
            assert_eq!(after.content, content, "{ctx}: stored content");
            assert_eq!(after.rev, before.rev + 1, "{ctx}: stored rev");
        }
    }
}

fn assert_unchanged(ctx: &str, before: &intent_core::Note, after: &intent_core::Note) {
    assert_eq!(
        after.content, before.content,
        "{ctx}: a rejected write must not change the stored content"
    );
    assert_eq!(
        after.rev, before.rev,
        "{ctx}: a rejected write must not bump the stored rev"
    );
}

/// Run every row of one method in its own task so a failing row does not
/// hide the others; the final panic lists every row that failed.
async fn run_table(method: &str, rows: Vec<Row>) {
    let mut failures = Vec::new();
    for row in rows {
        assert_eq!(
            row.method, method,
            "row [{}] is filed under the wrong table",
            row.name
        );
        let name = row.name;
        if let Err(join) = tokio::spawn(run_row(row)).await {
            let payload = join.into_panic();
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(ToString::to_string))
                .unwrap_or_else(|| "<non-string panic payload>".to_string());
            failures.push(format!("- [{name}]: {msg}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{method} precedence table: {} row(s) failed\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[tokio::test]
async fn note_set_content_precedence_table() {
    run_table(SET_CONTENT, set_content_rows()).await;
}
