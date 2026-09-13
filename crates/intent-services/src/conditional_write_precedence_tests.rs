//! Conformance table for the conditional-write error precedence of the note
//! content pipeline (`persist_merged_content`): `Conflict` → reduction guard →
//! merged-text validation → persist. Every row is (method × precondition) →
//! outcome, names the protocol clause it pins, and asserts the store is
//! untouched on rejection. intentd#1863 (`dd4e5783`) shipped two precedence
//! regressions past a green suite of per-case tests; this table exists so a
//! refactor that reorders the pipeline fails here, naming the row.
//!
//! Methods covered, one table each:
//! - `note.setContent` — the full precedence chain over `WorkspaceApi`.
//! - `note.add`, `note.edit`, `note.editLines`, `task.updateStatus`,
//!   `task.update` (unlinked line) — the surgical writes. They take no caller
//!   `expectedVersion`: each applies its transform to the row it read and
//!   calls `persist_merged_content` with `ContentWritePolicy::Surgical` and
//!   the read's `rev` as the base. Pinned at the pipeline level (the call the
//!   method makes, with a stale / future seed row) and with one happy-path
//!   row over `WorkspaceApi`.
//! - `task.updateNoteStatus` — the plain CAS metadata write, which keeps the
//!   `-32005` contract on a stale `expectedVersion` and never merges.
//!
//! Not covered here: `note.update` (metadata arm and content arm),
//! `note.updateMetadata` and `note.delete` — plain CAS writes pinned by
//! `tests::update_note_expected_version_gate_hit_miss_absent` and
//! `tests::set_content_merge_leaves_other_conditional_writes_conflicting`
//! ("Conflict stays where it belongs").
//!
//! Clause keys used in `Row::clause`:
//! - `notes-tasks §5.2 row`: `docs/protocol/methods/notes-tasks.md` §5.2, the
//!   `note.setContent` table row.
//! - `notes-tasks §5.2 merge`: same file, the "Three-way merge on stale
//!   `expectedVersion`" paragraph.
//! - `notes-tasks §5.2 surgical`: same paragraph, "The surgical mutations run
//!   through the same gated loop … with that read's `rev` as the base".
//! - `notes-tasks §5.4 updateNoteStatus`: same file, the
//!   `task.updateNoteStatus` table row (`expectedVersion?` enables optimistic
//!   concurrency against the note `rev`).
//! - `errors §9 -32005`: `docs/protocol/09-error-codes.md`, the `Conflict` row.
//! - `versioning 9.11 setContent`: `docs/protocol/versioning.md`, the 9.11
//!   paragraph "`note.setContent` merges on a stale `expectedVersion`".
//! - `versioning 9.11 surgical`: same paragraph, "The surgical writes …
//!   persist through the same gated loop with the `rev` of their own read".
//! - `versioning 9.11 plain CAS`: same paragraph's final sentence
//!   ("… `task.updateNoteStatus` keep the plain `-32005` contract").

use intent_core::{
    Error, NoteAddInput, NoteEditInput, NoteEditLinesInput, TaskStatus, WorkspaceApi,
};

use super::tests::{setup, setup_versioned};
use super::{ContentWrite, ContentWritePolicy};

/// Store state the writer's call runs against.
struct Precondition {
    /// Content the note is seeded with at rev 0.
    seed: String,
    /// Whether a `note_version` snapshot exists at rev 0 (a recoverable base).
    snapshot_at_rev0: bool,
    /// `task.markAsTask(not_started)` after seeding, for the `task.*` rows
    /// that need a task note.
    mark_as_task: bool,
    /// An unconditional `note.setContent` by another writer that bumps the
    /// rev after the writer's read: `(content, confirm_replacement)`.
    intervening: Option<(String, bool)>,
}

/// A rev expressed against the rows the driver observes.
#[derive(Clone, Copy)]
enum Rev {
    /// The rev of the row the writer read before the intervening write
    /// (equal to the current rev when there is none).
    Read,
    /// A rev above the current one — one the note never served.
    Future,
}

/// The writer's call.
enum Call {
    /// `note.setContent` over `WorkspaceApi`.
    SetContent {
        content: String,
        confirm_replacement: bool,
        expected_version: Option<i64>,
    },
    /// `persist_merged_content` with `ContentWritePolicy::Surgical`, exactly
    /// as the surgical methods call it: the seed is the row the writer read
    /// (its `rev` overridden per `seed_rev`), `expected_version` is that
    /// seed's `rev`, `incoming` is the transform's result against it.
    Surgical {
        op: &'static str,
        incoming: String,
        seed_rev: Rev,
    },
    Add(NoteAddInput),
    Edit(NoteEditInput),
    EditLines(NoteEditLinesInput),
    TaskUpdateStatus {
        task_text: &'static str,
        status: &'static str,
    },
    TaskUpdate {
        line: i64,
        text: Option<&'static str>,
        status: Option<&'static str>,
    },
    TaskUpdateNoteStatus {
        status: &'static str,
        expected_version: Option<Rev>,
    },
}

enum Expect {
    /// `Error::Conflict` carrying the current row; nothing persisted.
    Conflict,
    /// `Error::Internal` whose message starts with this prefix; nothing
    /// persisted.
    Rejected { message_prefix: &'static str },
    /// The write lands with exactly this text; `rev` = pre-write rev + 1 in
    /// the store (and in the result, where the result carries it).
    Persisted { content: String },
    /// The task-note status lands; content unchanged, `rev` = pre-write
    /// rev + 1 in the store.
    StatusPersisted { status: TaskStatus },
}

struct Row {
    name: &'static str,
    clause: &'static str,
    method: &'static str,
    precondition: Precondition,
    call: Call,
    expect: Expect,
}

/// What a successful call reported, where the result carries it.
#[derive(Debug)]
struct Written {
    content: Option<String>,
    rev: Option<i64>,
}

const SET_CONTENT: &str = "note.setContent";
const NOTE_ADD: &str = "note.add";
const NOTE_EDIT: &str = "note.edit";
const NOTE_EDIT_LINES: &str = "note.editLines";
const TASK_UPDATE_STATUS: &str = "task.updateStatus";
const TASK_UPDATE: &str = "task.update";
const TASK_UPDATE_NOTE_STATUS: &str = "task.updateNoteStatus";
const EMPTY_MSG: &str = "Content cannot be empty.";
const TRUNCATED_MSG: &str = "Content appears to be truncated";
const REDUCTION_MSG: &str = "⚠️ CONTENT REDUCTION DETECTED";

fn precondition(seed: &str, snapshot_at_rev0: bool, intervening: Option<&str>) -> Precondition {
    Precondition {
        seed: seed.to_string(),
        snapshot_at_rev0,
        mark_as_task: false,
        intervening: intervening.map(|c| (c.to_string(), false)),
    }
}

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
        precondition: precondition(seed, true, Some(intervening)),
        call: Call::SetContent {
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
    if let Call::SetContent {
        confirm_replacement,
        ..
    } = &mut confirmed.call
    {
        *confirm_replacement = true;
    }
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

/// The pipeline-level rows of one surgical method: the call it makes
/// (`persist_merged_content`, `Surgical`, seed = the row it read) against a
/// future / stale / matching seed rev. `Surgical` never validates the text,
/// so a future rev must still yield `Conflict` — never a write — when the
/// transform's result is empty or `...`-shaped.
fn surgical_pipeline_rows(op: &'static str) -> Vec<Row> {
    let surgical = |name, clause, pre: Precondition, incoming: &str, seed_rev, expect| Row {
        name,
        clause,
        method: op,
        precondition: pre,
        call: Call::Surgical {
            op,
            incoming: incoming.to_string(),
            seed_rev,
        },
        expect,
    };
    vec![
        surgical(
            "P1 future seed rev + valid incoming -> Conflict, no write",
            "notes-tasks §5.2 surgical; errors §9 -32005",
            precondition("body", true, Some("body v1")),
            "body + surgical",
            Rev::Future,
            Expect::Conflict,
        ),
        surgical(
            "P2 future seed rev + empty incoming -> Conflict (Surgical never validates; Conflict still wins)",
            "notes-tasks §5.2 surgical; notes-tasks §5.2 merge (future rev rejects without a write)",
            precondition("body", true, Some("body v1")),
            "",
            Rev::Future,
            Expect::Conflict,
        ),
        surgical(
            "P3 future seed rev + short `...` incoming -> Conflict (Surgical never validates; Conflict still wins)",
            "notes-tasks §5.2 surgical; notes-tasks §5.2 merge (future rev rejects without a write)",
            precondition("body", true, Some("body v1")),
            "short...",
            Rev::Future,
            Expect::Conflict,
        ),
        surgical(
            "P4 stale seed rev with snapshot -> transform merged onto the current text, rev bumped",
            "notes-tasks §5.2 surgical (a write that lands in between is merged into); versioning 9.11 surgical",
            precondition("alpha\nbeta\ngamma", true, Some("alpha\nbeta\ngamma\ndelta")),
            "alpha\nbeta-A\ngamma",
            Rev::Read,
            Expect::Persisted {
                content: "alpha\nbeta-A\ngamma\ndelta".into(),
            },
        ),
        surgical(
            "P5 matching seed rev -> incoming persisted verbatim, rev = current + 1",
            "notes-tasks §5.2 surgical (that read's rev as the base); versioning 9.11 surgical",
            precondition("alpha\nbeta\ngamma", true, None),
            "alpha\nBETA\ngamma",
            Rev::Read,
            Expect::Persisted {
                content: "alpha\nBETA\ngamma".into(),
            },
        ),
        surgical(
            "P6 stale seed rev without snapshot -> last-writer-wins, incoming persisted verbatim",
            "notes-tasks §5.2 merge (no snapshot survives -> honest last-writer-wins); versioning 9.11 surgical",
            precondition("v0 body", false, Some("v1 body (other writer)")),
            "v0 body + surgical",
            Rev::Read,
            Expect::Persisted {
                content: "v0 body + surgical".into(),
            },
        ),
    ]
}

/// The public-API happy-path row of one surgical method: the transformed
/// text lands and the rev bumps by exactly one.
fn surgical_api_row(method: &'static str, seed: &str, call: Call, content: &str) -> Row {
    Row {
        name: "A1 WorkspaceApi happy path -> transformed text persisted, rev = current + 1",
        clause: "notes-tasks §5.2 surgical; versioning 9.11 surgical",
        method,
        precondition: precondition(seed, true, None),
        call,
        expect: Expect::Persisted {
            content: content.to_string(),
        },
    }
}

fn surgical_rows(method: &'static str) -> Vec<Row> {
    let mut rows = surgical_pipeline_rows(method);
    let api = match method {
        NOTE_ADD => surgical_api_row(
            method,
            "alpha\nbeta",
            Call::Add(NoteAddInput {
                content: "gamma".into(),
                heading: None,
                position: None,
            }),
            "alpha\nbeta\n\ngamma",
        ),
        NOTE_EDIT => surgical_api_row(
            method,
            "alpha\nbeta\ngamma",
            Call::Edit(NoteEditInput {
                old: "beta".into(),
                new: "BETA".into(),
            }),
            "alpha\nBETA\ngamma",
        ),
        NOTE_EDIT_LINES => surgical_api_row(
            method,
            "alpha\nbeta\ngamma",
            Call::EditLines(NoteEditLinesInput {
                start: 2,
                end: 2,
                content: "BETA".into(),
            }),
            "alpha\nBETA\ngamma",
        ),
        TASK_UPDATE_STATUS => surgical_api_row(
            method,
            "- [ ] alpha\nbeta",
            Call::TaskUpdateStatus {
                task_text: "alpha",
                status: "done",
            },
            "- [x] alpha\nbeta",
        ),
        TASK_UPDATE => surgical_api_row(
            method,
            "- [ ] alpha\nbeta",
            Call::TaskUpdate {
                line: 1,
                text: Some("alpha AGENT"),
                status: Some("in-progress"),
            },
            "- [/] alpha AGENT\nbeta",
        ),
        other => panic!("no WorkspaceApi row for {other}"),
    };
    rows.push(api);
    rows
}

/// `task.updateNoteStatus`: a plain CAS write on the task metadata — stale
/// and future `expectedVersion` both `Conflict` with the status untouched;
/// matching and absent persist.
fn task_update_note_status_rows() -> Vec<Row> {
    let row = |name, intervening: Option<&str>, expected_version, expect| {
        let mut pre = precondition("# Parent task", false, intervening);
        pre.mark_as_task = true;
        Row {
            name,
            clause:
                "notes-tasks §5.4 updateNoteStatus; versioning 9.11 plain CAS; errors §9 -32005",
            method: TASK_UPDATE_NOTE_STATUS,
            precondition: pre,
            call: Call::TaskUpdateNoteStatus {
                status: "in_progress",
                expected_version,
            },
            expect,
        }
    };
    vec![
        row(
            "C1 stale expectedVersion -> Conflict carrying current, status unchanged (no merge)",
            Some("# Parent task v1"),
            Some(Rev::Read),
            Expect::Conflict,
        ),
        row(
            "C2 future expectedVersion -> Conflict carrying current, status unchanged",
            Some("# Parent task v1"),
            Some(Rev::Future),
            Expect::Conflict,
        ),
        row(
            "C3 matching expectedVersion -> status persisted, rev = current + 1",
            None,
            Some(Rev::Read),
            Expect::StatusPersisted {
                status: TaskStatus::InProgress,
            },
        ),
        row(
            "C4 absent expectedVersion -> status persisted, rev = current + 1",
            Some("# Parent task v1"),
            None,
            Expect::StatusPersisted {
                status: TaskStatus::InProgress,
            },
        ),
    ]
}

/// Issue `call` against the prepared store. `read` is the row the writer
/// read before the intervening write; `current` is the row at call time.
async fn issue(
    svc: &super::Services,
    ws: &intent_core::WorkspaceId,
    id: &intent_core::NoteId,
    call: Call,
    read: &intent_core::Note,
    current: &intent_core::Note,
) -> Result<Written, Error> {
    let resolve = |rev: Rev| match rev {
        Rev::Read => read.rev,
        Rev::Future => current.rev + 6,
    };
    match call {
        Call::SetContent {
            content,
            confirm_replacement,
            expected_version,
        } => svc
            .set_note_content(
                ws.clone(),
                id.clone(),
                content,
                confirm_replacement,
                expected_version,
                None,
            )
            .await
            .map(|r| Written {
                content: Some(r.new_content),
                rev: Some(r.rev),
            }),
        Call::Surgical {
            op,
            incoming,
            seed_rev,
        } => {
            let mut seed = read.clone();
            seed.rev = resolve(seed_rev);
            let author = super::system_version_author();
            super::persist_merged_content(
                &svc.store,
                ws,
                id,
                ContentWrite {
                    expected_version: Some(seed.rev),
                    seed: Some(seed),
                    incoming: &incoming,
                    policy: ContentWritePolicy::Surgical,
                    author: &author,
                    op,
                },
            )
            .await
            .map(|w| Written {
                content: Some(w.content),
                rev: Some(w.rev),
            })
        }
        Call::Add(input) => svc
            .add_to_note(ws.clone(), id.clone(), input, None)
            .await
            .map(|r| Written {
                content: Some(r.new_content),
                rev: None,
            }),
        Call::Edit(input) => svc
            .edit_note(ws.clone(), id.clone(), input, None)
            .await
            .map(|r| Written {
                content: Some(r.new_content),
                rev: None,
            }),
        Call::EditLines(input) => svc
            .edit_note_lines(ws.clone(), id.clone(), input, None)
            .await
            .map(|r| Written {
                content: Some(r.new_content),
                rev: None,
            }),
        Call::TaskUpdateStatus { task_text, status } => svc
            .task_update_status(
                ws.clone(),
                id.clone(),
                task_text.into(),
                status.into(),
                None,
            )
            .await
            .map(|_| Written {
                content: None,
                rev: None,
            }),
        Call::TaskUpdate { line, text, status } => svc
            .task_update(
                ws.clone(),
                id.clone(),
                line,
                text.map(Into::into),
                status.map(Into::into),
                None,
                None,
            )
            .await
            .map(|_| Written {
                content: None,
                rev: None,
            }),
        Call::TaskUpdateNoteStatus {
            status,
            expected_version,
        } => svc
            .task_update_note_status(
                ws.clone(),
                id.clone(),
                status.into(),
                expected_version.map(resolve),
                None,
            )
            .await
            .map(|_| Written {
                content: None,
                rev: None,
            }),
    }
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
    if p.mark_as_task {
        svc.mark_as_task(
            ws.clone(),
            id.clone(),
            "not_started".into(),
            vec![],
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("{ctx}: markAsTask failed: {e:?}"));
    }
    let read = svc
        .store
        .get_note(&ws, &id)
        .await
        .unwrap_or_else(|e| panic!("{ctx}: writer's read failed: {e:?}"));
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

    let result = issue(&svc, &ws, &id, row.call, &read, &before).await;

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
            let w =
                result.unwrap_or_else(|e| panic!("{ctx}: expected a persisted write, got {e:?}"));
            if let Some(reported) = w.content {
                assert_eq!(reported, content, "{ctx}: result content");
            }
            if let Some(rev) = w.rev {
                assert_eq!(rev, before.rev + 1, "{ctx}: result rev");
            }
            assert_eq!(after.content, content, "{ctx}: stored content");
            assert_eq!(after.rev, before.rev + 1, "{ctx}: stored rev");
        }
        Expect::StatusPersisted { status } => {
            result.unwrap_or_else(|e| panic!("{ctx}: expected a persisted write, got {e:?}"));
            assert_eq!(task_status(&after), Some(status), "{ctx}: stored status");
            assert_eq!(after.content, before.content, "{ctx}: content untouched");
            assert_eq!(after.rev, before.rev + 1, "{ctx}: stored rev");
        }
    }
}

fn task_status(note: &intent_core::Note) -> Option<TaskStatus> {
    note.metadata.task.as_ref().map(|t| t.status)
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
    assert_eq!(
        task_status(after),
        task_status(before),
        "{ctx}: a rejected write must not change the task status"
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

#[tokio::test]
async fn note_add_precedence_table() {
    run_table(NOTE_ADD, surgical_rows(NOTE_ADD)).await;
}

#[tokio::test]
async fn note_edit_precedence_table() {
    run_table(NOTE_EDIT, surgical_rows(NOTE_EDIT)).await;
}

#[tokio::test]
async fn note_edit_lines_precedence_table() {
    run_table(NOTE_EDIT_LINES, surgical_rows(NOTE_EDIT_LINES)).await;
}

#[tokio::test]
async fn task_update_status_precedence_table() {
    run_table(TASK_UPDATE_STATUS, surgical_rows(TASK_UPDATE_STATUS)).await;
}

#[tokio::test]
async fn task_update_precedence_table() {
    run_table(TASK_UPDATE, surgical_rows(TASK_UPDATE)).await;
}

#[tokio::test]
async fn task_update_note_status_precedence_table() {
    run_table(TASK_UPDATE_NOTE_STATUS, task_update_note_status_rows()).await;
}
