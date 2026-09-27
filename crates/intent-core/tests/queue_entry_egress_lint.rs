//! Queue-entry egress lint.
//!
//! `crates/intent-core/src/queue_visibility_contract.rs` is the table of every
//! wire surface a queue entry crosses (`QueueSurface`) and what each caller
//! class sees there; the services / transport harnesses walk that table. A
//! surface the table does not name is a surface the harnesses never drive, so
//! a new handler that serializes `queue_snapshot()` onto the wire, or a new
//! `agent:queue:*` publish, ships with no visibility contract at all. This
//! lint makes that registration mechanical: every egress in production code
//! must be claimed by a row of [`REGISTERED_EGRESS`] below, and every row must
//! name a live `QueueSurface` variant and a live fn / const.
//!
//! Scope: `crates/*/src/**` minus test code (`tests/` directories, `tests.rs`,
//! `*_tests.rs`, `tests_*.rs`, and `#[cfg(test)]` items — an attribute followed
//! by a brace-bodied item is blanked to its closing `}`, a `mod x;` / `const` /
//! `use` item to its `;`). Comments, string and char literals are blanked
//! before scanning, so a mention in a doc comment or a literal is never a hit.
//! Lexing, marker classification, and `#[cfg(test)]` blanking come from
//! `intentd_test_support::source_lint`.
//!
//! An egress is one of:
//!
//! - **Snapshot call**: the identifier `queue_snapshot` or
//!   `queue_snapshot_preview` immediately followed by `(` and not preceded by
//!   `fn` (so `persist_queue_snapshot(` and the definitions themselves are not
//!   hits). It is claimed when the enclosing fn — the nearest `fn <name>`
//!   definition before it in the file — is named by a `Key::Fn` row.
//! - **Queue-event publish**: an `event_type: <path::>AGENT_QUEUE_*`
//!   struct-field initializer (`.to_string()` and any path prefix allowed)
//!   naming a queue event const. The watched consts are derived every run
//!   from `crates/intent-core/src/events.rs`: every `const` / `static` whose
//!   string value starts with `agent:queue:` — so a new `agent:queue:*`
//!   constant is watched the moment it is defined, with no edit here. The
//!   publish is claimed when the const is named by a `Key::Event` row.
//! - **Queue-event reference**: any other identifier naming a watched const
//!   in a file other than `events.rs` — a helper argument
//!   (`publish_agent_event(…, AGENT_QUEUE_UPDATED, …)`), a local binding
//!   (`let t = AGENT_QUEUE_UPDATED;`), a `json!` value
//!   (`json!({ "event_type": AGENT_QUEUE_UPDATED })`), a comparison
//!   (`event_type == AGENT_QUEUE_UPDATED`). Its definition (`const NAME`)
//!   and `use` items are not references. Like a snapshot call it is claimed
//!   by its enclosing fn being named by a `Key::Fn` row — never by a
//!   `Key::Event` row, or a publish through a helper in a new fn would pass
//!   on the strength of the existing const. A consumer that merely matches on
//!   the type (a retention sweep, a subscription filter) opts out with a
//!   reason; the transport projection that rewrites the event before it
//!   leaves the daemon is registered as a fn.
//! - **Queue-event literal**: a string literal starting with `agent:queue:`
//!   in a file other than `events.rs` (a publish that bypasses the consts, or
//!   a second const defined outside `events.rs` that the derived set would
//!   not watch). Claimed like a reference, by its enclosing fn.
//!
//! Table coherence, checked every run: each `QueueSurface` variant (parsed
//! from the enum body in the contract file) must be claimed by at least one
//! row; a row whose label is not a variant, or whose `Key::Fn` / `Key::Event`
//! names no `fn <name>` / `const <name>` / `static <name>` definition in the
//! scanned sources, is stale and fails — moving or deleting an egress without
//! updating the table is caught. A row may claim a fn that carries no
//! detected hit (the per-id mutations serialize the entry through other means
//! than `queue_snapshot`); the fn-existence check is what keeps such rows
//! honest.
//!
//! Opt-out: `// queue-egress: allow — <reason>` as a standalone line comment
//! on the line immediately above the call / initializer, or above the first
//! line of its statement (the statement is the text back to the nearest `;`,
//! `{` or `}`). The token must be exactly `queue-egress: allow` (a longer word
//! such as `allowance` is malformed), followed by whitespace, an em dash or
//! hyphen, and a nonempty reason. A malformed marker never suppresses and is
//! reported as malformed. Opt out only when no entry leaves the daemon (an
//! id-only idempotency probe, an internal projection whose callers are the
//! registered egress).
//!
//! Limits (textual heuristic, no parser): the enclosing fn is the last `fn`
//! definition in the file before the hit, so a call inside a nested `fn` item
//! is attributed to that item, and a reference or literal at module level
//! (a const table of event types) has no fn to claim it and must opt out; a
//! fn name registered once claims a same-named fn in any scanned file; a
//! `Key::Fn` row claims every reference inside that fn, so a registered fn may
//! grow a second publish path unnoticed; a publish whose `event_type` value
//! is bound to a local is seen only at the binding (`let t =
//! AGENT_QUEUE_UPDATED`), not at `event_type: t`, and `json!({ "event_type":
//! CONST })` is seen as a reference to `CONST`, not as a publish (the key is a
//! literal, blanked before scanning) — both are caught, but as references
//! claimed by fn rather than publishes claimed by const; a type built by
//! concatenation (`format!("agent:{}", "queue:x")`) is not seen; entries
//! serialized via `QueuedMessage::to_value` outside `queue_snapshot` (the
//! enqueue echoes) are out of scope.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use intentd_test_support::source_lint::{
    blank_cfg_test_items, cfg_test_item_ranges, lex, markers_by_line, skip_whitespace,
    statement_line, word_at, Marker,
};

const SELF_FILE: &str = "crates/intent-core/tests/queue_entry_egress_lint.rs";
const CONTRACT_FILE: &str = "crates/intent-core/src/queue_visibility_contract.rs";
const EVENTS_FILE: &str = "crates/intent-core/src/events.rs";
const SURFACE_ENUM: &str = "QueueSurface";
const TABLE_CONST: &str = "REGISTERED_EGRESS";
const OPT_OUT_TAG: &str = "queue-egress";
const SNAPSHOT_CALLS: &[&str] = &["queue_snapshot", "queue_snapshot_preview"];
const EVENT_FIELD: &str = "event_type";
const QUEUE_EVENT_PREFIX: &str = "agent:queue:";
const EXCERPT_CHARS: usize = 120;
/// Qualifiers that turn `const` into `const fn` / `const unsafe fn` / ….
const FN_QUALIFIERS: &[&str] = &["fn", "unsafe", "extern", "async"];

/// What a [`REGISTERED_EGRESS`] row points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Key {
    /// A production `fn` whose body serializes `queue_snapshot()` /
    /// `queue_snapshot_preview()` onto the surface (or performs the surface's
    /// mutation).
    Fn(&'static str),
    /// The `intent_core::events` const a publish sets as `event_type`.
    Event(&'static str),
}

impl Key {
    fn describe(self) -> String {
        match self {
            Key::Fn(n) => format!("fn `{n}`"),
            Key::Event(n) => format!("event const `{n}`"),
        }
    }
}

/// `(QueueSurface variant, egress)` — every production egress of a queue
/// entry and the contract-table surface it is served on. Add a row here when
/// a new handler serializes the queue (and a variant in `CONTRACT_FILE` when
/// the surface is new); the lint fails until both agree.
const REGISTERED_EGRESS: &[(&str, Key)] = &[
    ("GetQueue", Key::Fn("agent_get_queue_op")),
    ("QueueUpdatedEvent", Key::Fn("publish_queue_event")),
    ("QueueUpdatedEvent", Key::Event("AGENT_QUEUE_UPDATED")),
    (
        "QueueUpdatedEvent",
        Key::Fn("project_queue_event_for_current_caller"),
    ),
    ("QueueProcessingEvent", Key::Event("AGENT_QUEUE_PROCESSING")),
    (
        "QueueProcessingEvent",
        Key::Fn("project_queue_event_for_current_caller"),
    ),
    ("EditQueuedMessage", Key::Fn("agent_edit_queued_message_op")),
    (
        "RemoveQueuedMessage",
        Key::Fn("agent_remove_queued_message_op"),
    ),
    ("SendQueuedMessageNow", Key::Fn("send_queued_message_now")),
    ("Diagnostics", Key::Fn("agent_diagnostics_op")),
    ("Diagnostics", Key::Fn("queue_snapshot_preview")),
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum Egress {
    /// `callee(` inside `enclosing` (`None` when no `fn` precedes it).
    Call {
        callee: String,
        enclosing: Option<String>,
    },
    /// `event_type: <const>`.
    Publish { event: String },
    /// Any other identifier naming a watched const, inside `enclosing`.
    Reference {
        event: String,
        enclosing: Option<String>,
    },
    /// A string literal starting with [`QUEUE_EVENT_PREFIX`], inside
    /// `enclosing`.
    Literal {
        value: String,
        enclosing: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    egress: Egress,
    excerpt: String,
    /// A line above carried something that starts like the opt-out marker
    /// but is malformed (longer token, or no reason).
    marker_malformed: bool,
}

/// One scanned file: its unsuppressed hits and the names it defines.
#[derive(Debug, Default)]
struct Scan {
    hits: Vec<Hit>,
    fns: BTreeSet<String>,
    consts: BTreeSet<String>,
}

/// Index just past the delimiter group (`(…)` or `[…]`) opening at `j`.
fn skip_group(chars: &[char], mut j: usize, open: char, close: char) -> usize {
    let mut depth = 0usize;
    while let Some(&c) = chars.get(j) {
        j += 1;
        if c == open {
            depth += 1;
        } else if c == close {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }
    j
}

/// An ASCII identifier token (`[A-Za-z_][A-Za-z0-9_]*`) in blanked text, by
/// char index.
struct Token {
    start: usize,
    end: usize,
    word: String,
}

fn tokens(chars: &[char]) -> Vec<Token> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_ascii_alphabetic() || c == '_' {
            let (word, end) = word_at(chars, i).expect("identifier start");
            out.push(Token {
                start: i,
                end,
                word,
            });
            i = end;
        } else if c.is_ascii_digit() {
            // A number's trailing letters (`1u8`) are not an identifier.
            while chars
                .get(i)
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_')
            {
                i += 1;
            }
        } else {
            i += 1;
        }
    }
    out
}

/// 1-based line of char index `at`.
fn line_of_index(chars: &[char], at: usize) -> usize {
    1 + chars[..at.min(chars.len())]
        .iter()
        .filter(|c| **c == '\n')
        .count()
}

fn excerpt(chars: &[char], at: usize) -> String {
    let line = line_of_index(chars, at);
    let text: String = chars
        .split(|c| *c == '\n')
        .nth(line - 1)
        .map(|l| l.iter().collect())
        .unwrap_or_default();
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out: String = collapsed.chars().take(EXCERPT_CHARS).collect();
    if out.len() < collapsed.len() {
        out.push('…');
    }
    out
}

/// The identifier that `event_type:` at token `idx` is initialized with,
/// after any `path::` prefix, and its token index; `None` when the token is
/// not such an initializer (a comparison, a type annotation reached through
/// `&`, …).
fn event_initializer(chars: &[char], toks: &[Token], idx: usize) -> Option<(String, usize)> {
    let colon = skip_whitespace(chars, toks[idx].end);
    if chars.get(colon) != Some(&':') || chars.get(colon + 1) == Some(&':') {
        return None;
    }
    let mut k = idx + 1;
    let mut expect_at = skip_whitespace(chars, colon + 1);
    loop {
        let tok = toks.get(k)?;
        if tok.start != expect_at {
            return None;
        }
        let after = skip_whitespace(chars, tok.end);
        if chars.get(after) == Some(&':') && chars.get(after + 1) == Some(&':') {
            expect_at = skip_whitespace(chars, after + 2);
            k += 1;
            continue;
        }
        return Some((tok.word.clone(), k));
    }
}

/// The name of every non-test `const` / `static` in the events source whose
/// string value starts with [`QUEUE_EVENT_PREFIX`]: the publishes the lint
/// watches. Derived from the source rather than listed in this file, so a new
/// `agent:queue:*` constant needs no edit here to be caught.
fn queue_event_consts(events_src: &str) -> Result<BTreeSet<String>, String> {
    let raw: Vec<char> = events_src.chars().collect();
    let blanked = blank_cfg_test_items(&lex(events_src).blanked);
    let chars: Vec<char> = blanked.chars().collect();
    let toks = tokens(&chars);
    let mut out = BTreeSet::new();
    for (idx, tok) in toks.iter().enumerate() {
        let lifetime = raw.get(tok.start.wrapping_sub(1)) == Some(&'\'');
        if (tok.word != "const" && tok.word != "static") || lifetime {
            continue;
        }
        let Some(name) = toks
            .get(idx + 1)
            .filter(|t| t.start == skip_whitespace(&chars, tok.end))
            .filter(|t| !FN_QUALIFIERS.contains(&t.word.as_str()))
        else {
            continue;
        };
        let mut eq = name.end;
        while chars.get(eq).is_some_and(|c| !matches!(c, '=' | ';')) {
            eq += 1;
        }
        if chars.get(eq) != Some(&'=') {
            continue;
        }
        let open = skip_whitespace(&raw, eq + 1);
        if raw.get(open) != Some(&'"') {
            continue;
        }
        let value: String = raw[open + 1..].iter().take_while(|c| **c != '"').collect();
        if value.starts_with(QUEUE_EVENT_PREFIX) {
            out.insert(name.word.clone());
        }
    }
    if out.is_empty() {
        return Err(format!(
            "{EVENTS_FILE}: no `const` with a `{QUEUE_EVENT_PREFIX}*` string value found — the \
             queue event constants moved; update `EVENTS_FILE` in {SELF_FILE}"
        ));
    }
    Ok(out)
}

/// Scans one Rust source file's text: every egress not suppressed by a
/// reasoned opt-out marker, plus the `fn` / `const` / `static` names the file
/// defines outside test code. `queue_events` is the watched const set from
/// [`queue_event_consts`].
fn scan_source(src: &str, queue_events: &BTreeSet<String>) -> Scan {
    let raw: Vec<char> = src.chars().collect();
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, OPT_OUT_TAG);
    let blanked: Vec<char> = lexed.blanked.chars().collect();
    let untested = cfg_test_item_ranges(&blanked);
    let chars: Vec<char> = blank_cfg_test_items(&lexed.blanked).chars().collect();
    let toks = tokens(&chars);
    let mut scan = Scan::default();
    let mut enclosing: Option<String> = None;
    let mut fn_defs: Vec<(usize, String)> = Vec::new();
    let mut raw_hits: Vec<(usize, Egress)> = Vec::new();
    let mut use_until = 0usize;
    let mut publish_tok: Option<usize> = None;

    for (idx, tok) in toks.iter().enumerate() {
        let prev = idx.checked_sub(1).map(|p| toks[p].word.as_str());
        let next_tok = toks.get(idx + 1);
        let directly_next = |t: &Token| skip_whitespace(&chars, tok.end) == t.start;
        match tok.word.as_str() {
            "fn" => {
                if let Some(name) = next_tok.filter(|t| directly_next(t)) {
                    scan.fns.insert(name.word.clone());
                    enclosing = Some(name.word.clone());
                    fn_defs.push((name.start, name.word.clone()));
                }
            }
            "use" => {
                use_until = chars[tok.end..]
                    .iter()
                    .position(|c| *c == ';')
                    .map_or(chars.len(), |p| tok.end + p);
            }
            "const" | "static" => {
                if let Some(name) = next_tok
                    .filter(|t| directly_next(t))
                    .filter(|t| !FN_QUALIFIERS.contains(&t.word.as_str()))
                {
                    scan.consts.insert(name.word.clone());
                }
            }
            w if SNAPSHOT_CALLS.contains(&w) => {
                let called = chars.get(skip_whitespace(&chars, tok.end)) == Some(&'(');
                if called && prev != Some("fn") {
                    raw_hits.push((
                        tok.start,
                        Egress::Call {
                            callee: w.to_string(),
                            enclosing: enclosing.clone(),
                        },
                    ));
                }
            }
            EVENT_FIELD => {
                if let Some((event, k)) =
                    event_initializer(&chars, &toks, idx).filter(|(e, _)| queue_events.contains(e))
                {
                    publish_tok = Some(k);
                    raw_hits.push((tok.start, Egress::Publish { event }));
                }
            }
            w if queue_events.contains(w) => {
                let definition = matches!(prev, Some("const" | "static"));
                if !definition && tok.start >= use_until && publish_tok != Some(idx) {
                    raw_hits.push((
                        tok.start,
                        Egress::Reference {
                            event: w.to_string(),
                            enclosing: enclosing.clone(),
                        },
                    ));
                }
            }
            _ => {}
        }
    }

    for lit in &lexed.literals {
        let value = lit.cooked();
        if !value.starts_with(QUEUE_EVENT_PREFIX)
            || untested
                .iter()
                .any(|&(start, end)| (start..end).contains(&lit.offset))
        {
            continue;
        }
        let enclosing = fn_defs
            .iter()
            .rev()
            .find(|(start, _)| *start < lit.offset)
            .map(|(_, name)| name.clone());
        raw_hits.push((lit.offset, Egress::Literal { value, enclosing }));
    }
    raw_hits.sort_by_key(|(at, _)| *at);

    for (at, egress) in raw_hits {
        let line = line_of_index(&chars, at);
        let stmt = statement_line(&chars, at);
        let marker_at = |l: usize| markers.get(l.wrapping_sub(1)).copied();
        let states = [marker_at(line), marker_at(stmt)];
        if states.contains(&Some(Marker::WithReason)) {
            continue;
        }
        let excerpt_chars = if matches!(egress, Egress::Literal { .. }) {
            &raw
        } else {
            &chars
        };
        scan.hits.push(Hit {
            line,
            egress,
            excerpt: excerpt(excerpt_chars, at),
            marker_malformed: states.contains(&Some(Marker::Malformed)),
        });
    }
    scan
}

/// The variant names of `enum QueueSurface { … }` in the contract source.
fn surface_labels(contract_src: &str) -> Result<Vec<String>, String> {
    let chars: Vec<char> = lex(contract_src).blanked.chars().collect();
    let toks = tokens(&chars);
    let Some(idx) = toks
        .windows(2)
        .position(|w| w[0].word == "enum" && w[1].word == SURFACE_ENUM)
    else {
        return Err(format!(
            "{CONTRACT_FILE}: no `enum {SURFACE_ENUM}` found — the contract table moved; \
             update `CONTRACT_FILE` / `SURFACE_ENUM` in {SELF_FILE}"
        ));
    };
    let open = skip_whitespace(&chars, toks[idx + 1].end);
    if chars.get(open) != Some(&'{') {
        return Err(format!(
            "{CONTRACT_FILE}: `enum {SURFACE_ENUM}` is not followed by a brace body"
        ));
    }
    let close = skip_group(&chars, open, '{', '}');
    let labels: Vec<String> = toks
        .iter()
        .filter(|t| t.start > open && t.end < close)
        .filter(|t| matches!(chars.get(skip_whitespace(&chars, t.end)), Some(',' | '}')))
        .map(|t| t.word.clone())
        .collect();
    if labels.is_empty() {
        return Err(format!(
            "{CONTRACT_FILE}: `enum {SURFACE_ENUM}` has no variants"
        ));
    }
    Ok(labels)
}

/// Runs the lint over already-read sources: `files` is `(relative path,
/// text)`; `labels` the `QueueSurface` variants; `rows` the registration
/// table; `queue_events` the watched const set. Returns every failure, each
/// naming the file and the table to edit.
fn check(
    labels: &[String],
    rows: &[(&str, Key)],
    files: &[(&str, &str)],
    queue_events: &BTreeSet<String>,
) -> Vec<String> {
    let mut failures = Vec::new();
    let scans: Vec<(&str, Scan)> = files
        .iter()
        .map(|(rel, src)| (*rel, scan_source(src, queue_events)))
        .collect();
    let fns: BTreeSet<&str> = scans
        .iter()
        .flat_map(|(_, s)| s.fns.iter().map(String::as_str))
        .collect();
    let consts: BTreeSet<&str> = scans
        .iter()
        .flat_map(|(_, s)| s.consts.iter().map(String::as_str))
        .collect();

    for label in labels {
        if !rows.iter().any(|(l, _)| l == label) {
            failures.push(format!(
                "{CONTRACT_FILE}: `{SURFACE_ENUM}::{label}` is claimed by no `{TABLE_CONST}` row \
                 in {SELF_FILE} — add a row naming the fn / event const that serves it"
            ));
        }
    }
    for (label, key) in rows {
        if !labels.iter().any(|l| l == label) {
            failures.push(format!(
                "{SELF_FILE}: `{TABLE_CONST}` row `{label}` names no `{SURFACE_ENUM}` variant \
                 in {CONTRACT_FILE} — rename or drop the row"
            ));
        }
        let defined = match key {
            Key::Fn(n) => fns.contains(n),
            Key::Event(n) => consts.contains(n),
        };
        if !defined {
            failures.push(format!(
                "{SELF_FILE}: `{TABLE_CONST}` row `{label}` → {} is defined by no non-test \
                 source under crates/*/src — the egress moved or was removed; update or drop \
                 the row",
                key.describe()
            ));
        }
    }

    let claimed_fns: BTreeSet<&str> = rows
        .iter()
        .filter_map(|(_, k)| match k {
            Key::Fn(n) => Some(*n),
            Key::Event(_) => None,
        })
        .collect();
    let claimed_events: BTreeSet<&str> = rows
        .iter()
        .filter_map(|(_, k)| match k {
            Key::Event(n) => Some(*n),
            Key::Fn(_) => None,
        })
        .collect();
    let by_fn = |what: &str, enclosing: &Option<String>| {
        (
            enclosing
                .as_deref()
                .is_some_and(|f| claimed_fns.contains(f)),
            match enclosing {
                Some(f) => format!("{what} in fn `{f}`: `{f}` is named by no `Key::Fn` row"),
                None => format!("{what} outside any fn: no `Key::Fn` row can claim it"),
            },
        )
    };
    for (rel, scan) in &scans {
        for hit in &scan.hits {
            let (claimed, what) = match &hit.egress {
                Egress::Call { callee, enclosing } => by_fn(&format!("`{callee}(`"), enclosing),
                Egress::Publish { event } => (
                    claimed_events.contains(event.as_str()),
                    format!("publish of `{event}`: it is named by no `Key::Event` row"),
                ),
                Egress::Reference { .. } | Egress::Literal { .. } if *rel == EVENTS_FILE => {
                    continue;
                }
                Egress::Reference { event, enclosing } => {
                    by_fn(&format!("reference to `{event}`"), enclosing)
                }
                Egress::Literal { value, enclosing } => {
                    by_fn(&format!("literal `\"{value}\"`"), enclosing)
                }
            };
            if claimed {
                continue;
            }
            let note = if hit.marker_malformed {
                "  (opt-out marker is malformed: expected `// queue-egress: allow — <reason>`)"
            } else {
                ""
            };
            failures.push(format!(
                "{rel}:{}: {what} of `{TABLE_CONST}` in {SELF_FILE} — {}{note}",
                hit.line, hit.excerpt
            ));
        }
    }
    failures
}

/// Every `*.rs` under `dir`, skipping test code by path (`tests/` directories,
/// `tests.rs`, `*_tests.rs`, `tests_*.rs`).
fn collect_rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            if name != "tests" {
                collect_rust_sources(&path, out);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs")
            && name != "tests.rs"
            && !name.ends_with("_tests.rs")
            && !name.starts_with("tests_")
        {
            out.push(path);
        }
    }
}

fn display_rel(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[test]
fn every_queue_entry_egress_is_registered_in_the_contract_table() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    let contract_path = root.join(CONTRACT_FILE);
    let contract = fs::read_to_string(&contract_path).unwrap_or_else(|e| {
        panic!("read {CONTRACT_FILE}: {e} — the contract table moved; update `CONTRACT_FILE` in {SELF_FILE}")
    });
    let labels = surface_labels(&contract).unwrap_or_else(|e| panic!("{e}"));
    let events_path = root.join(EVENTS_FILE);
    let events_src = fs::read_to_string(&events_path).unwrap_or_else(|e| {
        panic!("read {EVENTS_FILE}: {e} — the events module moved; update `EVENTS_FILE` in {SELF_FILE}")
    });
    let queue_events = queue_event_consts(&events_src).unwrap_or_else(|e| panic!("{e}"));

    let mut files = Vec::new();
    let crates_dir = root.join("crates");
    let crates = fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", crates_dir.display()));
    for entry in crates {
        let src = entry.expect("crate dir entry").path().join("src");
        if src.is_dir() {
            collect_rust_sources(&src, &mut files);
        }
    }
    files.sort();
    assert!(
        !files.is_empty(),
        "no Rust sources found under {}",
        crates_dir.display()
    );
    let sources: Vec<(String, String)> = files
        .iter()
        .map(|file| {
            let rel = file
                .strip_prefix(&root)
                .expect("source path under the workspace root");
            let src =
                fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
            (display_rel(rel), src)
        })
        .collect();
    let borrowed: Vec<(&str, &str)> = sources
        .iter()
        .map(|(rel, src)| (rel.as_str(), src.as_str()))
        .collect();

    let report = check(&labels, REGISTERED_EGRESS, &borrowed, &queue_events);
    assert!(
        report.is_empty(),
        "queue-entry egress is out of step with the visibility contract table:\n\n{}\n\n\
         Every production `queue_snapshot()` / `queue_snapshot_preview()` call, every \
         `event_type:` publish of an `{QUEUE_EVENT_PREFIX}*` const from {EVENTS_FILE} \
         (currently: {}), and — outside {EVENTS_FILE} — every other reference to one of \
         those consts or `\"{QUEUE_EVENT_PREFIX}*\"` literal must be claimed by a \
         `{TABLE_CONST}` row in {SELF_FILE} naming the `{SURFACE_ENUM}` variant it serves \
         ({CONTRACT_FILE}; add the variant and its contract cells when the surface is new, so \
         the harnesses drive it): a publish by its `Key::Event` const, anything else by a \
         `Key::Fn` row naming its enclosing fn. A call or reference that lets no entry leave \
         the daemon (a consumer matching on the type) may opt out with \
         `// queue-egress: allow — <reason>` on the line immediately above it or its \
         statement; the reason is required.",
        report.join("\n"),
        queue_events
            .iter()
            .map(|e| format!("`{e}`"))
            .collect::<Vec<_>>()
            .join(", ")
    );
}

// ---- fixtures ---------------------------------------------------------------

/// 1-based line of the first line containing `needle`.
fn line_of(src: &str, needle: &str) -> usize {
    src.lines()
        .position(|l| l.contains(needle))
        .map_or_else(|| panic!("fixture lacks {needle:?}"), |i| i + 1)
}

fn hit_lines(src: &str) -> Vec<usize> {
    scan_source(src, &queue_events())
        .hits
        .into_iter()
        .map(|h| h.line)
        .collect()
}

fn labels(names: &[&str]) -> Vec<String> {
    names.iter().map(ToString::to_string).collect()
}

/// The events-module shapes: queue consts among non-queue ones, a doc-comment
/// and a `#[cfg(test)]` mention that must not count.
const EVENTS_FIXTURE: &str = r#"
pub const AGENT_STARTED: &str = "agent:started";
/// `agent:queue:*` events carry `data.queue`; see AGENT_QUEUE_DOC_ONLY.
pub const AGENT_QUEUE_UPDATED: &str = "agent:queue:updated";
pub(crate) const AGENT_QUEUE_PROCESSING: &'static str = "agent:queue:processing";
pub static QUEUE_PREFIX_NOT_AN_EVENT: &str = "queue:agent:";
#[cfg(test)]
pub const AGENT_QUEUE_TEST_ONLY: &str = "agent:queue:test-only";
pub const ALL_EVENT_TYPES: &[&str] = &[AGENT_STARTED, AGENT_QUEUE_UPDATED, AGENT_QUEUE_PROCESSING];
"#;

fn queue_events() -> BTreeSet<String> {
    queue_event_consts(EVENTS_FIXTURE).unwrap()
}

const CONTRACT_FIXTURE: &str = r"
/// Every place a queue entry crosses the daemon boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueSurface {
    /// `agent.getQueue` result (`project_queue_for_caller`).
    GetQueue,
    /// `agent:queue:updated` event `data.queue`.
    QueueUpdatedEvent,
}

impl QueueSurface {
    pub const fn all() -> &'static [QueueSurface] {
        &[QueueSurface::GetQueue, QueueSurface::QueueUpdatedEvent]
    }
}
";

/// The production shapes: a handler serving the snapshot, the publish choke
/// point (the event const itself is defined in [`EVENTS_FIXTURE`]), and a
/// `use` import that is not a reference.
const PRODUCTION_FIXTURE: &str = r#"
use intent_core::events::{AGENT_QUEUE_UPDATED, AGENT_STARTED};

impl Services {
    pub(crate) async fn agent_get_queue_op(&self, agent_id: AgentId) -> Result<Value> {
        let mut queue = self.queue_snapshot(&agent_id);
        Ok(json!({ "queue": queue }))
    }

    async fn publish_queue_event(&self, agent_id: &AgentId, workspace_id: &WorkspaceId) {
        let mut queue = self.queue_snapshot(agent_id);
        let event = intent_store::NewEvent {
            workspace_id: workspace_id.clone(),
            event_type: AGENT_QUEUE_UPDATED.to_string(),
            data: json!({ "queue": queue }),
        };
        publish_event(self.event_bus.as_ref(), event).await;
    }

    pub(crate) fn queue_snapshot(&self, agent_id: &AgentId) -> Vec<Value> {
        Vec::new()
    }

    pub(crate) async fn persist_queue_snapshot(&self, agent_id: &AgentId) {
        let rows = self.queue_rows(agent_id);
    }
}
"#;

const PRODUCTION_ROWS: &[(&str, Key)] = &[
    ("GetQueue", Key::Fn("agent_get_queue_op")),
    ("QueueUpdatedEvent", Key::Fn("publish_queue_event")),
    ("QueueUpdatedEvent", Key::Event("AGENT_QUEUE_UPDATED")),
];

#[test]
fn surface_labels_come_from_the_enum_body_only() {
    assert_eq!(
        surface_labels(CONTRACT_FIXTURE).unwrap(),
        labels(&["GetQueue", "QueueUpdatedEvent"])
    );
    assert!(surface_labels("pub enum Other { A, B }")
        .unwrap_err()
        .contains("no `enum QueueSurface`"));
}

#[test]
fn queue_event_consts_are_derived_from_the_events_source_by_string_value() {
    assert_eq!(
        queue_events(),
        BTreeSet::from([
            "AGENT_QUEUE_UPDATED".to_string(),
            "AGENT_QUEUE_PROCESSING".to_string()
        ])
    );
    assert!(
        queue_event_consts("pub const AGENT_STARTED: &str = \"agent:started\";")
            .unwrap_err()
            .contains("no `const` with a `agent:queue:*` string value found")
    );
}

#[test]
fn new_queue_event_const_is_watched_without_editing_the_lint() {
    let events = format!(
        "{EVENTS_FIXTURE}pub const AGENT_QUEUE_VERIFIER_PROBE: &str = \"agent:queue:verifier-probe\";\n"
    );
    let watched = queue_event_consts(&events).unwrap();
    assert!(
        watched.contains("AGENT_QUEUE_VERIFIER_PROBE"),
        "{watched:?}"
    );

    let src = "fn probe(&self) {\n    let event = NewEvent {\n        event_type: AGENT_QUEUE_VERIFIER_PROBE.to_string(),\n        data: json!({ \"queue\": self.queue_rows() }),\n    };\n}\n";
    let files = [(EVENTS_FILE, events.as_str()), ("crates/x/src/lib.rs", src)];

    let unregistered = check(&labels(&[]), &[], &files, &watched);
    assert_eq!(unregistered.len(), 1, "{unregistered:#?}");
    assert!(
        unregistered[0]
            .starts_with("crates/x/src/lib.rs:3: publish of `AGENT_QUEUE_VERIFIER_PROBE`"),
        "{}",
        unregistered[0]
    );
    assert!(unregistered[0]
        .contains("`REGISTERED_EGRESS` in crates/intent-core/tests/queue_entry_egress_lint.rs"));

    let registered = check(
        &labels(&["QueueUpdatedEvent"]),
        &[(
            "QueueUpdatedEvent",
            Key::Event("AGENT_QUEUE_VERIFIER_PROBE"),
        )],
        &files,
        &watched,
    );
    assert!(registered.is_empty(), "{registered:#?}");

    assert!(
        check(&labels(&[]), &[], &files, &queue_events()).is_empty(),
        "a const the events source does not define is not a queue event"
    );
}

#[test]
fn registered_production_shapes_pass() {
    let fixture_labels = surface_labels(CONTRACT_FIXTURE).unwrap();
    let failures = check(
        &fixture_labels,
        PRODUCTION_ROWS,
        &[
            (EVENTS_FILE, EVENTS_FIXTURE),
            ("crates/x/src/lib.rs", PRODUCTION_FIXTURE),
        ],
        &queue_events(),
    );
    assert!(failures.is_empty(), "{failures:#?}");
}

#[test]
fn definitions_use_items_and_persist_call_are_not_hits() {
    let scan = scan_source(PRODUCTION_FIXTURE, &queue_events());
    let lines: Vec<usize> = scan.hits.iter().map(|h| h.line).collect();
    assert_eq!(
        lines,
        vec![
            line_of(
                PRODUCTION_FIXTURE,
                "let mut queue = self.queue_snapshot(&agent_id)"
            ),
            line_of(
                PRODUCTION_FIXTURE,
                "let mut queue = self.queue_snapshot(agent_id)"
            ),
            line_of(PRODUCTION_FIXTURE, "event_type: AGENT_QUEUE_UPDATED"),
        ]
    );
    assert_eq!(
        scan.hits[2].egress,
        Egress::Publish {
            event: "AGENT_QUEUE_UPDATED".into()
        },
        "the initializer is a publish, not also a reference"
    );
    assert!(scan.fns.contains("queue_snapshot"));
    assert!(scan.fns.contains("persist_queue_snapshot"));

    let events = scan_source(EVENTS_FIXTURE, &queue_events());
    assert!(events.consts.contains("AGENT_QUEUE_UPDATED"));
    assert!(
        events
            .hits
            .iter()
            .all(|h| matches!(h.egress, Egress::Reference { .. } | Egress::Literal { .. })),
        "{:#?}",
        events.hits
    );
    assert!(
        check(
            &labels(&[]),
            &[],
            &[(EVENTS_FILE, EVENTS_FIXTURE)],
            &queue_events()
        )
        .is_empty(),
        "the definitions and `ALL_EVENT_TYPES` in the events file are not egress"
    );
}

#[test]
fn unregistered_snapshot_call_fails_naming_the_fn_and_the_table() {
    let src = "impl S {\n    pub(crate) async fn agent_peek_op(&self) -> Result<Value> {\n        let q = self.queue_snapshot(&id);\n        Ok(json!({ \"queue\": q }))\n    }\n}\n";
    let failures = check(
        &labels(&[]),
        &[],
        &[("crates/x/src/lib.rs", src)],
        &queue_events(),
    );
    assert_eq!(failures.len(), 1, "{failures:#?}");
    assert!(
        failures[0].starts_with("crates/x/src/lib.rs:3: `queue_snapshot(` in fn `agent_peek_op`")
    );
    assert!(failures[0]
        .contains("`REGISTERED_EGRESS` in crates/intent-core/tests/queue_entry_egress_lint.rs"));
}

#[test]
fn unregistered_publish_fails_naming_the_const() {
    let src = "fn publish(&self) {\n    let event = NewEvent {\n        event_type: intent_core::events::AGENT_QUEUE_PROCESSING.to_string(),\n        data: json!({}),\n    };\n}\n";
    let failures = check(
        &labels(&[]),
        &[],
        &[("crates/x/src/lib.rs", src)],
        &queue_events(),
    );
    assert_eq!(failures.len(), 1, "{failures:#?}");
    assert!(failures[0].starts_with("crates/x/src/lib.rs:3: publish of `AGENT_QUEUE_PROCESSING`"));
}

#[test]
fn event_type_comparisons_and_annotations_are_references_not_publishes() {
    let src = "fn f(e: &Event) -> bool {\n    let t: &str = AGENT_QUEUE_UPDATED;\n    e.event_type == AGENT_QUEUE_UPDATED\n        || matches!(e, Event { event_type, .. } if event_type == AGENT_QUEUE_PROCESSING)\n}\nstruct Row { event_type: String }\n";
    let scan = scan_source(src, &queue_events());
    let lines: Vec<usize> = scan.hits.iter().map(|h| h.line).collect();
    assert_eq!(lines, vec![2, 3, 4], "{:#?}", scan.hits);
    assert!(
        scan.hits.iter().all(|h| matches!(
            &h.egress,
            Egress::Reference { enclosing: Some(f), .. } if f == "f"
        )),
        "{:#?}",
        scan.hits
    );

    let files = [("crates/x/src/lib.rs", src)];
    let unregistered = check(&labels(&[]), &[], &files, &queue_events());
    assert_eq!(unregistered.len(), 3, "{unregistered:#?}");
    assert!(
        unregistered[1].starts_with(
            "crates/x/src/lib.rs:3: reference to `AGENT_QUEUE_UPDATED` in fn `f`: `f` is named by no `Key::Fn` row"
        ),
        "{}",
        unregistered[1]
    );
    let registered = check(
        &labels(&["QueueUpdatedEvent"]),
        &[("QueueUpdatedEvent", Key::Fn("f"))],
        &files,
        &queue_events(),
    );
    assert!(registered.is_empty(), "{registered:#?}");
}

#[test]
fn helper_arg_and_local_binding_publishes_are_references_claimed_by_fn_only() {
    let helper_arg = "fn notify(&self, ws: &WorkspaceId) {\n    publish_agent_event(self, ws, AGENT_QUEUE_PROCESSING, json!({}));\n}\n";
    let local_binding = "fn notify(&self, ws: &WorkspaceId) {\n    let t = AGENT_QUEUE_PROCESSING;\n    let event = NewEvent { event_type: t.to_string(), data: json!({}) };\n}\n";
    for src in [helper_arg, local_binding] {
        let files = [("crates/x/src/lib.rs", src)];
        let unregistered = check(&labels(&[]), &[], &files, &queue_events());
        assert_eq!(unregistered.len(), 1, "{src}\n{unregistered:#?}");
        assert!(
            unregistered[0].starts_with(
                "crates/x/src/lib.rs:2: reference to `AGENT_QUEUE_PROCESSING` in fn `notify`"
            ),
            "{}",
            unregistered[0]
        );

        let const_only = check(
            &labels(&["QueueProcessingEvent"]),
            &[("QueueProcessingEvent", Key::Event("AGENT_QUEUE_PROCESSING"))],
            &[(EVENTS_FILE, EVENTS_FIXTURE), files[0]],
            &queue_events(),
        );
        assert_eq!(
            const_only.len(),
            1,
            "a `Key::Event` row must not claim a reference\n{const_only:#?}"
        );

        let registered = check(
            &labels(&["QueueProcessingEvent"]),
            &[("QueueProcessingEvent", Key::Fn("notify"))],
            &files,
            &queue_events(),
        );
        assert!(registered.is_empty(), "{src}\n{registered:#?}");
    }
}

#[test]
fn json_key_publish_is_seen_as_a_reference_or_a_literal() {
    let by_const = "fn emit(&self) -> Value {\n    json!({ \"event_type\": AGENT_QUEUE_UPDATED, \"data\": { \"queue\": self.queue_rows() } })\n}\n";
    let by_literal = "fn emit(&self) -> Value {\n    json!({ \"event_type\": \"agent:queue:updated\", \"data\": {} })\n}\n";
    let files = [
        ("crates/x/src/a.rs", by_const),
        ("crates/x/src/b.rs", by_literal),
    ];
    let unregistered = check(&labels(&[]), &[], &files, &queue_events());
    assert_eq!(unregistered.len(), 2, "{unregistered:#?}");
    assert!(
        unregistered[0]
            .starts_with("crates/x/src/a.rs:2: reference to `AGENT_QUEUE_UPDATED` in fn `emit`"),
        "{}",
        unregistered[0]
    );
    assert!(
        unregistered[1].starts_with(
            "crates/x/src/b.rs:2: literal `\"agent:queue:updated\"` in fn `emit`: `emit` is named by no `Key::Fn` row"
        ),
        "{}",
        unregistered[1]
    );
    assert!(
        unregistered[1].contains("json!({ \"event_type\": \"agent:queue:updated\""),
        "the literal excerpt comes from the raw line: {}",
        unregistered[1]
    );
    let registered = check(
        &labels(&["QueueUpdatedEvent"]),
        &[("QueueUpdatedEvent", Key::Fn("emit"))],
        &files,
        &queue_events(),
    );
    assert!(registered.is_empty(), "{registered:#?}");
}

#[test]
fn queue_literals_outside_the_events_file_are_hits_unless_test_or_comment() {
    let src = "// \"agent:queue:in-a-comment\"\n/// doc: `agent:queue:*`\npub const MY_QUEUE_EVENT: &str = \"agent:queue:shadow\";\nfn raw() -> &'static str {\n    r#\"agent:queue:raw\"#\n}\n#[cfg(test)]\nmod tests {\n    const T: &str = \"agent:queue:test-only\";\n}\n";
    let scan = scan_source(src, &queue_events());
    assert_eq!(
        scan.hits
            .iter()
            .map(|h| (h.line, h.egress.clone()))
            .collect::<Vec<_>>(),
        vec![
            (
                3,
                Egress::Literal {
                    value: "agent:queue:shadow".into(),
                    enclosing: None,
                }
            ),
            (
                5,
                Egress::Literal {
                    value: "agent:queue:raw".into(),
                    enclosing: Some("raw".into()),
                }
            ),
        ],
        "{:#?}",
        scan.hits
    );
    let failures = check(
        &labels(&[]),
        &[],
        &[("crates/x/src/lib.rs", src)],
        &queue_events(),
    );
    assert_eq!(failures.len(), 2, "{failures:#?}");
    assert!(
        failures[0].starts_with(
            "crates/x/src/lib.rs:3: literal `\"agent:queue:shadow\"` outside any fn: no `Key::Fn` row can claim it"
        ),
        "{}",
        failures[0]
    );
}

#[test]
fn module_level_reference_needs_an_opt_out() {
    let table = "pub const SWEPT: &[&str] = &[AGENT_STARTED, AGENT_QUEUE_UPDATED];\n";
    let failures = check(
        &labels(&[]),
        &[],
        &[("crates/x/src/lib.rs", table)],
        &queue_events(),
    );
    assert_eq!(failures.len(), 1, "{failures:#?}");
    assert!(
        failures[0].starts_with(
            "crates/x/src/lib.rs:1: reference to `AGENT_QUEUE_UPDATED` outside any fn: no `Key::Fn` row can claim it"
        ),
        "{}",
        failures[0]
    );
    let opted_out = "// queue-egress: allow — retention sweep, deletes by type\npub const SWEPT: &[&str] = &[AGENT_STARTED, AGENT_QUEUE_UPDATED];\n";
    assert!(hit_lines(opted_out).is_empty());
    let comparison = "fn keep(e: &Event) -> bool {\n    // queue-egress: allow — subscription filter matches on the type only\n    e.event_type != AGENT_QUEUE_PROCESSING\n}\n";
    assert!(hit_lines(comparison).is_empty());
}

#[test]
fn every_surface_variant_must_be_claimed_and_rows_must_be_live() {
    let fixture_labels = surface_labels(CONTRACT_FIXTURE).unwrap();
    let rows: &[(&str, Key)] = &[
        ("GetQueue", Key::Fn("agent_get_queue_op")),
        ("Diagnostics", Key::Fn("agent_diagnostics_op")),
        ("QueueUpdatedEvent", Key::Event("AGENT_QUEUE_REMOVED")),
    ];
    let src = "fn agent_get_queue_op() {}\n";
    let failures = check(
        &fixture_labels,
        rows,
        &[("crates/x/src/lib.rs", src)],
        &queue_events(),
    );
    let joined = failures.join("\n");
    assert!(
        joined.contains("row `Diagnostics` names no `QueueSurface` variant"),
        "{joined}"
    );
    assert!(
        joined.contains(
            "row `Diagnostics` → fn `agent_diagnostics_op` is defined by no non-test source"
        ),
        "{joined}"
    );
    assert!(
        joined.contains("row `QueueUpdatedEvent` → event const `AGENT_QUEUE_REMOVED` is defined by no non-test source"),
        "{joined}"
    );
    assert!(
        !joined.contains("`QueueSurface::GetQueue` is claimed by no"),
        "{joined}"
    );
    assert!(
        !joined.contains("`QueueSurface::QueueUpdatedEvent` is claimed by no"),
        "{joined}"
    );
    assert_eq!(failures.len(), 3, "{failures:#?}");

    let failures = check(
        &fixture_labels,
        &rows[..1],
        &[("crates/x/src/lib.rs", src)],
        &queue_events(),
    );
    assert_eq!(failures.len(), 1, "{failures:#?}");
    assert!(failures[0]
        .contains("`QueueSurface::QueueUpdatedEvent` is claimed by no `REGISTERED_EGRESS` row"));
}

#[test]
fn opt_out_marker_with_a_reason_suppresses_above_the_call_or_its_statement() {
    let above_call = "fn probe(&self) -> bool {\n    let queued = self\n        // queue-egress: allow — id-only idempotency probe\n        .queue_snapshot(&id)\n        .iter()\n        .any(|e| e[\"id\"] == id);\n    queued\n}\n";
    assert!(hit_lines(above_call).is_empty(), "{above_call}");
    let above_statement = "fn probe(&self) -> bool {\n    // queue-egress: allow - id-only idempotency probe\n    let queued = self\n        .queue_snapshot(&id)\n        .iter()\n        .any(|e| e[\"id\"] == id);\n    queued\n}\n";
    assert!(hit_lines(above_statement).is_empty(), "{above_statement}");
    let publish = "fn publish(&self) {\n    let event = NewEvent {\n        // queue-egress: allow — replayed verbatim from the store\n        event_type: AGENT_QUEUE_UPDATED.to_string(),\n    };\n}\n";
    assert!(hit_lines(publish).is_empty(), "{publish}");
}

#[test]
fn opt_out_marker_without_a_reason_still_fails_as_malformed() {
    for marker in [
        "// queue-egress: allow",
        "// queue-egress: allow —",
        "// queue-egress: allow -",
        "// queue-egress: allowance — longer token",
        "// queue-egress: allow reason without a dash",
    ] {
        let src = format!(
            "fn probe(&self) {{\n    {marker}\n    let q = self.queue_snapshot(&id);\n}}\n"
        );
        let scan = scan_source(&src, &queue_events());
        assert_eq!(scan.hits.len(), 1, "{marker}");
        assert!(scan.hits[0].marker_malformed, "{marker}");
    }
    let two_above = "fn probe(&self) {\n    // queue-egress: allow — two lines above\n\n    let q = self.queue_snapshot(&id);\n}\n";
    let scan = scan_source(two_above, &queue_events());
    assert_eq!(scan.hits.len(), 1);
    assert!(!scan.hits[0].marker_malformed);
    let trailing = "fn probe(&self) {\n    let q = self.queue_snapshot(&id); // queue-egress: allow — trailing\n}\n";
    assert_eq!(hit_lines(trailing), vec![2]);
}

#[test]
fn comments_strings_and_cfg_test_items_are_blanked() {
    let src = "// self.queue_snapshot(&id) in a comment\n/* event_type: AGENT_QUEUE_UPDATED */\nfn doc() -> &'static str {\n    \"queue_snapshot(&id) event_type: AGENT_QUEUE_UPDATED\"\n}\n#[cfg(test)]\nmod tests {\n    fn t(&self) { let q = self.queue_snapshot(&id); }\n}\n#[cfg(test)]\nfn helper(&self) -> Vec<Value> { self.queue_snapshot_preview(&id) }\n";
    let scan = scan_source(src, &queue_events());
    assert!(scan.hits.is_empty(), "{:#?}", scan.hits);
    assert!(scan.fns.contains("doc"));
    assert!(!scan.fns.contains("helper"));
}

#[test]
fn preview_call_is_attributed_to_its_enclosing_fn() {
    let src = "fn diag(&self) -> Value {\n    let entries = self.queue_snapshot_preview(&id);\n    json!({ \"entries\": entries })\n}\n";
    let scan = scan_source(src, &queue_events());
    assert_eq!(
        scan.hits[0].egress,
        Egress::Call {
            callee: "queue_snapshot_preview".into(),
            enclosing: Some("diag".into()),
        }
    );
    let failures = check(
        &labels(&[]),
        &[("Diagnostics", Key::Fn("diag"))],
        &[("crates/x/src/lib.rs", src)],
        &queue_events(),
    );
    assert_eq!(failures.len(), 1, "{failures:#?}");
    assert!(failures[0].contains("row `Diagnostics` names no `QueueSurface` variant"));
}
