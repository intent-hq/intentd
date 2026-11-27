//! Source lint: an intentd e2e test must not write to the store of a daemon
//! it spawned while that daemon is still running.
//!
//! A test that opens `intentd.db` with `Store::open` and writes while the
//! live daemon writes too races it across processes: the store's write
//! paths open deferred transactions, and `busy_timeout` cannot resolve a
//! cross-process read-to-write upgrade, so the test fails at random with
//! "database is locked" (`SQLite` codes 5 / 517). One such seed failed a full
//! local gate on intent-hq/intentd#3792 and 4 of 40 runs under
//! `make test-stress LOAD=1` until intentd#3799 moved it before startup.
//! Seed before the spawn with `common::seed_then_start`, or stop the daemon
//! first; reads while it runs are fine.
//!
//! The heuristic, per function in `crates/intentd/tests/**` (each top-level
//! test module — `X.rs` plus `X/` — is analysed together with `common/`):
//!
//! - **Daemon handles.** A statement that spawns (`DaemonGuard::new(` /
//!   `::process_only(`; `GuardedChild::spawn(`, `spawn_guarded(` or
//!   `.spawn()` in a function that calls a hermetic serve builder or a helper
//!   returning a serve `Command`, or holds a `"serve"` literal; or a call to a
//!   helper that leaves a daemon running) makes its `let` binding — or the
//!   receiver of a method-call statement — a live handle. A later `let` whose
//!   initializer names a handle aliases it (`let daemon = Daemon { child }`).
//!   A handle stops at `drop(handle)`, at a statement naming it that kills or
//!   reaps it (`kill` without a graceful signal, `kill_tree`, `wait`,
//!   `wait_with_output`, `SIGKILL`, `await_wss_stopped`, or
//!   `wait_with_timeout` with `.unwrap().expect(..)` chained onto it) or calls
//!   a helper that does, or when its block ends. `try_wait`, a timed wait that
//!   may have returned `Ok(None)`, and a `SIGTERM` / `SIGINT` / `SIGHUP` alone
//!   do not stop it: the daemon may still be writing.
//! - **Store writes.** `let s = Store::open(…)` (or `open_for_daemon`, or a
//!   helper returning a `Store`) and a `Store`-typed parameter are store
//!   bindings; `s.method(` is a write unless `method` reads (`get_*`,
//!   `list_*`, `count_*`, …, see [`is_read_method`]). A call to a helper that
//!   writes the store (transitively) is a write too.
//! - A write while any handle is live fails, naming `file:line` and the
//!   method or helper. So does a malformed marker, or one that excuses no
//!   live write (stale).
//!
//! This is not name resolution or control-flow analysis: helpers are matched
//! by name within the module, handles passed between functions or held in
//! fields, writes through fields, and loops that restart a daemon are outside
//! the contract. A write opts out with `// live-store-write: allow —
//! <reason>` on the line above the write or its statement (or trailing
//! either), or on the line above the `let` that opens the store, which covers
//! every write through that binding, only when it cannot race (the daemon is
//! stopped by other means, or the one still running owns another data dir) or
//! when the test needs state behind a live daemon that no RPC can produce (a
//! fault, a revoked row, an Owner role) and the write either retries busy
//! upgrades (`with_write_txn_retry`) or is one autocommit statement, so the
//! test's own side waits instead of failing.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use intentd_test_support::source_lint::{
    classify_marker, lex, matching_close, rust_files, tokenize, workspace_root, Lexed, Literal,
    Marker, Token,
};

const TAG: &str = "live-store-write";
const TESTS_DIR: &str = "crates/intentd/tests";

const READ_PREFIXES: &[&str] = &[
    "get_", "list_", "count_", "load_", "find_", "has_", "is_", "search_", "latest_", "max_",
    "read_", "query_", "fetch_", "select_", "exists_",
];
const READ_NAMES: &[&str] = &[
    "get",
    "list",
    "count",
    "load",
    "find",
    "exists",
    "close",
    "path",
    "pool",
    "read_pool",
    "clone",
    "as_ref",
    "desktop_terminal",
    "host_membership_state",
    "pending_script_runs",
    "script_monitor_pending_for_agent",
    "script_monitor_wake_pending",
    "script_notification_jobs",
];
/// Calls that end a daemon handle when a statement naming the handle makes
/// them: a kill or a reap. `try_wait` only probes, and a graceful signal
/// (`SIGTERM` / `SIGINT` / `SIGHUP`) leaves the daemon writing while it shuts
/// down, so a `kill` sending one does not count; a later reap does.
const STOP_PRIMITIVES: &[&str] = &[
    "kill",
    "kill_tree",
    "wait",
    "wait_with_output",
    "wait_with_timeout",
    "SIGKILL",
    "await_wss_stopped",
    "await_wss_stopped_logged",
];
const GRACEFUL_SIGNALS: &[&str] = &["SIGTERM", "SIGINT", "SIGHUP"];

/// How many `.unwrap()` / `.expect(..)` calls chain directly onto the call
/// whose argument list closes at `close`.
fn chained_unwraps(tokens: &[Token], mut close: usize) -> usize {
    let mut count = 0;
    while tokens.get(close + 1).is_some_and(|t| t.is("."))
        && tokens
            .get(close + 2)
            .is_some_and(|t| t.is("unwrap") || t.is("expect"))
        && tokens.get(close + 3).is_some_and(|t| t.is("("))
    {
        let Some(next) = matching_close(tokens, close + 3) else {
            break;
        };
        count += 1;
        close = next;
    }
    count
}

/// Whether `tokens[k]` stops a daemon within the statement `range`.
fn is_stop(tokens: &[Token], k: usize, range: (usize, usize)) -> bool {
    let t = &tokens[k];
    if !STOP_PRIMITIVES.contains(&t.text.as_str())
        || (k > 0 && tokens[k - 1].is("fn"))
        || !tokens.get(k + 1).is_some_and(|n| n.is("(")) && !t.is("SIGKILL")
    {
        return false;
    }
    if t.is("wait_with_timeout") {
        // `Ok(None)` is a timeout with the daemon still running: only
        // unwrapping both the `Result` and the `Option` of this very call
        // (`.unwrap().expect(..)` chained onto it) establishes the exit.
        return matching_close(tokens, k + 1)
            .is_some_and(|close| chained_unwraps(tokens, close) >= 2);
    }
    !t.is("kill")
        || !(range.0..range.1).any(|j| GRACEFUL_SIGNALS.contains(&tokens[j].text.as_str()))
}

/// Whether a store method only reads.
fn is_read_method(name: &str) -> bool {
    READ_NAMES.contains(&name) || READ_PREFIXES.iter().any(|p| name.starts_with(p))
}

/// A function item: its name, the `impl` type it belongs to, and the token
/// ranges of its parameters, return type and body.
#[derive(Debug, Clone)]
struct FnItem {
    name: String,
    impl_ty: Option<String>,
    params: (usize, usize),
    ret: (usize, usize),
    body: (usize, usize),
}

/// One source file of a module, tokenized.
struct Source {
    rel: String,
    lexed: Lexed,
    tokens: Vec<Token>,
    fns: Vec<FnItem>,
}

impl Source {
    fn new(rel: String, src: &str) -> Self {
        let lexed = lex(src);
        let chars: Vec<char> = lexed.blanked.chars().collect();
        let tokens = tokenize(&chars);
        let fns = fn_items(&tokens);
        Self {
            rel,
            lexed,
            tokens,
            fns,
        }
    }

    /// The string literals opening at char offsets in `[from, to)`.
    fn literals_in(&self, from: usize, to: usize) -> &[Literal] {
        let lits = &self.lexed.literals;
        let a = lits.partition_point(|l| l.offset < from);
        let b = lits.partition_point(|l| l.offset < to);
        &lits[a..b]
    }

    /// The line of the well-formed marker covering `line`: a standalone
    /// marker on the line above, or one trailing `line`.
    fn marker(&self, line: usize) -> Option<usize> {
        self.lexed
            .line_comments
            .iter()
            .find(|c| {
                let adjacent = if c.standalone {
                    c.line + 1 == line
                } else {
                    c.line == line
                };
                adjacent && classify_marker(&c.text, TAG) == Marker::WithReason
            })
            .map(|c| c.line)
    }

    /// Lines of every marker in the file, well-formed or not.
    fn markers(&self) -> Vec<(usize, Marker)> {
        self.lexed
            .line_comments
            .iter()
            .map(|c| (c.line, classify_marker(&c.text, TAG)))
            .filter(|(_, m)| *m != Marker::Absent)
            .collect()
    }
}

/// The `impl` blocks of a token stream as `(body_open, body_close, type)`.
fn impl_blocks(tokens: &[Token]) -> Vec<(usize, usize, String)> {
    let mut out = Vec::new();
    for (i, t) in tokens.iter().enumerate() {
        if !t.is("impl") {
            continue;
        }
        let Some(open) = (i + 1..tokens.len())
            .take_while(|&k| !tokens[k].is(";"))
            .find(|&k| tokens[k].is("{") && tokens[k].depth == t.depth)
        else {
            continue;
        };
        let header = &tokens[i + 1..open];
        let after_for = header.iter().position(|t| t.is("for"));
        let mut angle = 0i32;
        let ty = header
            .iter()
            .skip(after_for.map_or(0, |p| p + 1))
            .find(|t| {
                match t.text.as_str() {
                    "<" => angle += 1,
                    ">" => angle -= 1,
                    _ => {}
                }
                angle == 0 && t.is_ident() && !t.is("dyn") && !t.is("mut")
            })
            .map(|t| t.text.clone());
        if let (Some(ty), Some(close)) = (ty, matching_close(tokens, open)) {
            out.push((open, close, ty));
        }
    }
    out
}

fn fn_items(tokens: &[Token]) -> Vec<FnItem> {
    let impls = impl_blocks(tokens);
    let mut out = Vec::new();
    for (i, t) in tokens.iter().enumerate() {
        if !t.is("fn") || !tokens.get(i + 1).is_some_and(Token::is_ident) {
            continue;
        }
        let Some(p_open) = (i + 2..tokens.len()).find(|&k| tokens[k].is("(")) else {
            continue;
        };
        let Some(p_close) = matching_close(tokens, p_open) else {
            continue;
        };
        let Some(open) = (p_close + 1..tokens.len())
            .find(|&k| tokens[k].depth == t.depth && (tokens[k].is("{") || tokens[k].is(";")))
        else {
            continue;
        };
        if tokens[open].is(";") {
            continue;
        }
        let Some(close) = matching_close(tokens, open) else {
            continue;
        };
        let impl_ty = impls
            .iter()
            .filter(|(o, c, _)| *o < i && i < *c)
            .max_by_key(|(o, _, _)| *o)
            .map(|(_, _, ty)| ty.clone());
        out.push(FnItem {
            name: tokens[i + 1].text.clone(),
            impl_ty,
            params: (p_open, p_close),
            ret: (p_close + 1, open),
            body: (open, close),
        });
    }
    out
}

/// What calling a function does to its caller's state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[expect(clippy::struct_excessive_bools)]
struct Effects {
    /// Leaves a daemon running for the caller.
    spawns: bool,
    /// Stops a daemon it is handed.
    stops: bool,
    /// Writes the store (anywhere in its body).
    writes: bool,
    /// Returns a `Store` it opened.
    opens_store: bool,
    /// Returns an `intentd serve` command.
    serve_command: bool,
}

/// A function of the module: `(impl type, name)`.
type Key = (Option<String>, String);

/// What the analysis knows about a module beyond one function body.
#[derive(Debug, Default)]
struct Ctx {
    /// Effects of each function, from the previous fixed-point round.
    effects: BTreeMap<Key, Effects>,
    /// Per element of a function's tuple return type: `(holds a daemon,
    /// holds a store)`.
    rets: BTreeMap<Key, Vec<(bool, bool)>>,
    /// Per named-field struct: each field and whether its type holds a
    /// daemon.
    structs: BTreeMap<String, Vec<(String, bool)>>,
    /// The `impl` types defining a method of each name, for `.name(` calls.
    methods: BTreeMap<String, Vec<String>>,
}

/// The entries of `map` for the call whose name token is `tokens[i]`,
/// resolved by name within the module: `.name(` matches any method,
/// `Type::name(` that type's associated function (or a free function behind
/// a module path), `Self::` the enclosing `impl`, and a bare `name(` a free
/// function.
fn resolve<'m, V>(
    tokens: &[Token],
    i: usize,
    own_impl: Option<&str>,
    methods: &BTreeMap<String, Vec<String>>,
    map: &'m BTreeMap<Key, V>,
) -> Vec<&'m V> {
    if !tokens[i].is_ident() || !tokens.get(i + 1).is_some_and(|t| t.is("(")) {
        return Vec::new();
    }
    let name = &tokens[i].text;
    match i.checked_sub(1).map(|k| tokens[k].text.as_str()) {
        Some(".") => methods
            .get(name)
            .into_iter()
            .flatten()
            .filter_map(|ty| map.get(&(Some(ty.clone()), name.clone())))
            .collect(),
        Some("::") => {
            let owner = &tokens[i - 2].text;
            let ty = if owner == "Self" {
                own_impl.map(str::to_owned)
            } else if owner.starts_with(char::is_uppercase) {
                Some(owner.clone())
            } else {
                None
            };
            map.get(&(ty, name.clone())).into_iter().collect()
        }
        _ => map.get(&(None, name.clone())).into_iter().collect(),
    }
}

fn call_effects(tokens: &[Token], i: usize, own_impl: Option<&str>, ctx: &Ctx) -> Effects {
    resolve(tokens, i, own_impl, &ctx.methods, &ctx.effects)
        .into_iter()
        .fold(Effects::default(), |acc, e| Effects {
            spawns: acc.spawns || e.spawns,
            stops: acc.stops || e.stops,
            writes: acc.writes || e.writes,
            opens_store: acc.opens_store || e.opens_store,
            serve_command: acc.serve_command || e.serve_command,
        })
}

/// Types that hold a running daemon.
const DAEMON_TYPES: &[&str] = &["GuardedChild", "DaemonGuard", "DaemonChild", "Child"];

/// Whether `ty` names a daemon-holding type (directly or as a struct of the
/// module that holds one).
fn holds_daemon(ty: &[Token], daemon_structs: &BTreeSet<String>) -> bool {
    ty.iter()
        .any(|t| DAEMON_TYPES.contains(&t.text.as_str()) || daemon_structs.contains(&t.text))
}

/// The top-level comma-separated elements of the group opened at `open`.
fn group_elements(tokens: &[Token], open: usize) -> Vec<(usize, usize)> {
    let Some(close) = matching_close(tokens, open) else {
        return Vec::new();
    };
    let depth = tokens[open].depth + 1;
    let mut out = Vec::new();
    let mut start = open + 1;
    for (k, t) in tokens.iter().enumerate().take(close + 1).skip(open + 1) {
        if k == close || (t.is(",") && t.depth == depth) {
            if start < k {
                out.push((start, k));
            }
            start = k + 1;
        }
    }
    out
}

/// A struct's fields: each name and the token range of its type.
type Fields = Vec<(String, (usize, usize))>;

/// Named-field structs of a token stream with their fields.
fn struct_items(tokens: &[Token]) -> Vec<(String, Fields)> {
    let mut out = Vec::new();
    for (i, t) in tokens.iter().enumerate() {
        if !t.is("struct") || !tokens.get(i + 1).is_some_and(Token::is_ident) {
            continue;
        }
        let Some(open) = (i + 2..tokens.len())
            .take_while(|&k| !tokens[k].is(";") && !tokens[k].is("("))
            .find(|&k| tokens[k].is("{"))
        else {
            continue;
        };
        let fields = group_elements(tokens, open)
            .into_iter()
            .filter_map(|(a, b)| {
                let colon = (a..b).find(|&k| tokens[k].is(":"))?;
                let name = tokens[a..colon].iter().rev().find(|t| t.is_ident())?;
                Some((name.text.clone(), (colon + 1, b)))
            })
            .collect();
        out.push((tokens[i + 1].text.clone(), fields));
    }
    out
}

/// A live-daemon store write found in a function body.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    what: String,
}

#[derive(Debug, Default)]
struct Analysis {
    effects: Effects,
    hits: Vec<Hit>,
    /// Lines of the markers that excused a live write.
    used: BTreeSet<usize>,
}

/// A named binding with the depth of the `let` that made it.
#[derive(Debug, Clone)]
struct Binding {
    name: Option<String>,
    depth: usize,
    /// The line of the opt-out marker on a store binding's `let`.
    marker: Option<usize>,
}

/// A process spawn at `tokens[i]`. Child-process spawns count only in a
/// function that builds an `intentd serve` command (`serve`); a
/// `DaemonGuard` always holds a daemon.
fn is_spawn_primitive(tokens: &[Token], i: usize, serve: bool) -> bool {
    let t = &tokens[i];
    let next_is = |k: usize, s: &str| tokens.get(i + k).is_some_and(|t| t.is(s));
    let prev_is = |k: usize, s: &str| i >= k && tokens[i - k].is(s);
    let child =
        (t.is("spawn") && prev_is(1, "::") && prev_is(2, "GuardedChild") && next_is(1, "("))
            || (t.is("spawn_guarded") && next_is(1, "("))
            || (t.is("spawn") && prev_is(1, ".") && next_is(1, "(") && next_is(2, ")"));
    (serve && child)
        || ((t.is("new") || t.is("process_only"))
            && prev_is(1, "::")
            && prev_is(2, "DaemonGuard")
            && next_is(1, "("))
}

/// Index of the `;` (or block-closing `}`) ending the statement holding
/// `tokens[i]`. A token inside a call's `(…)` or an index's `[…]` belongs to
/// the statement around that group.
fn statement_end(tokens: &[Token], i: usize, body_close: usize) -> usize {
    let mut depth = tokens[i].depth;
    for (k, t) in tokens.iter().enumerate().take(body_close).skip(i) {
        if (t.is(")") || t.is("]")) && t.depth < depth {
            depth = t.depth;
        } else if (t.is(";") && t.depth == depth) || (t.is("}") && t.depth < depth) {
            return k;
        }
    }
    body_close
}

/// Index of the first token of the statement holding `tokens[i]`, climbing
/// out of `(…)` / `[…]` groups the same way as [`statement_end`].
fn statement_start(tokens: &[Token], i: usize, body_open: usize) -> usize {
    let mut depth = tokens[i].depth;
    for k in (body_open + 1..i).rev() {
        let t = &tokens[k];
        if (t.is("(") || t.is("[")) && t.depth < depth {
            depth = t.depth;
        } else if ((t.is(";") || t.is("}")) && t.depth == depth) || (t.is("{") && t.depth < depth) {
            return k + 1;
        }
    }
    body_open + 1
}

/// The names a `let` at `i` binds: the identifiers of its pattern (up to the
/// `=` or type `:`), without `_`, `mut`, `ref` and paths of tuple structs.
fn let_names(tokens: &[Token], i: usize) -> Vec<String> {
    let depth = tokens[i].depth;
    tokens[i + 1..]
        .iter()
        .enumerate()
        .take_while(|(_, t)| !((t.is("=") || t.is(":") || t.is(";")) && t.depth == depth))
        .filter(|(j, t)| {
            t.is_ident()
                && !["mut", "ref", "_", "Some", "Ok"].contains(&t.text.as_str())
                && !tokens
                    .get(i + 2 + j)
                    .is_some_and(|n| n.is("(") || n.is("::"))
        })
        .map(|(_, t)| t.text.clone())
        .collect()
}

/// Live handles named in `range`. A name right after `.` is a field, not
/// the handle.
fn mentions(tokens: &[Token], range: (usize, usize), names: &[Binding]) -> Vec<String> {
    (range.0..range.1)
        .filter(|&k| k == 0 || !tokens[k - 1].is("."))
        .filter_map(|k| {
            names
                .iter()
                .find(|b| b.name.as_deref() == Some(tokens[k].text.as_str()))
                .and_then(|b| b.name.clone())
        })
        .collect()
}

/// The live handles `range` moves: named neither borrowed (`&h`) nor as a
/// method receiver or field base (`h.`).
fn moved_handles(tokens: &[Token], range: (usize, usize), names: &[Binding]) -> Vec<String> {
    (range.0..range.1)
        .filter(|&k| {
            !(k > 0 && (tokens[k - 1].is(".") || tokens[k - 1].is("&")))
                && !tokens.get(k + 1).is_some_and(|n| n.is("."))
                && !(k > 1 && tokens[k - 1].is("mut") && tokens[k - 2].is("&"))
        })
        .filter(|&k| {
            names
                .iter()
                .any(|b| b.name.as_deref() == Some(tokens[k].text.as_str()))
        })
        .map(|k| tokens[k].text.clone())
        .collect()
}

/// Whether a helper body stops a daemon: a kill or reap (a bare `wait` is
/// too common to count here), or dropping a field of `self`.
fn body_stops(tokens: &[Token], (open, close): (usize, usize)) -> bool {
    (open + 1..close).any(|j| {
        let t = &tokens[j];
        (!t.is("wait")
            && is_stop(
                tokens,
                j,
                (
                    statement_start(tokens, j, open),
                    statement_end(tokens, j, close),
                ),
            ))
            || (t.is("drop")
                && tokens.get(j + 1).is_some_and(|n| n.is("("))
                && tokens.get(j + 2).is_some_and(|n| n.is("self")))
    })
}

fn analyse(src: &Source, f: &FnItem, ctx: &Ctx) -> Analysis {
    let tokens = &src.tokens;
    let (open, close) = f.body;
    let own_impl = f.impl_ty.as_deref();
    let mut handles: Vec<Binding> = Vec::new();
    let mut stores: Vec<Binding> = Vec::new();
    let mut out = Analysis::default();
    let mut escapes = false;
    let mut spawned_at: Option<usize> = None;
    let calls: Vec<Effects> = (open..=close)
        .map(|k| call_effects(tokens, k, own_impl, ctx))
        .collect();
    let serve = builds_serve(src, f, &calls);
    let spawns_at: Vec<bool> = (open..=close)
        .map(|k| is_spawn_primitive(tokens, k, serve) || calls[k - open].spawns)
        .collect();
    let opens_at: Vec<bool> = (open..=close)
        .map(|k| opens_store_primitive(tokens, k) || calls[k - open].opens_store)
        .collect();

    for (p_name, ty_has_store) in store_params(tokens, f.params) {
        if ty_has_store {
            stores.push(Binding {
                name: Some(p_name),
                depth: tokens[open].depth + 1,
                marker: None,
            });
        }
    }

    // `let` statements whose end binds their name: (end, let index).
    let mut pending: Vec<(usize, usize)> = Vec::new();
    let mut k = open + 1;
    while k < close {
        let t = &tokens[k];
        if t.is("}") {
            handles.retain(|b| b.depth <= t.depth);
            stores.retain(|b| b.depth <= t.depth);
        }
        if t.is("let") {
            pending.push((statement_end(tokens, k, close), k));
        }
        if t.is("return")
            && !handles.is_empty()
            && !mentions(tokens, (k, statement_end(tokens, k, close)), &handles).is_empty()
        {
            escapes = true;
        }
        let call = calls[k - open];
        if spawns_at[k - open] {
            spawned_at.get_or_insert(k);
            if !pending.iter().any(|(end, l)| *l < k && k < *end) {
                let start = statement_start(tokens, k, open);
                let receiver = (tokens[start].is_ident()
                    && tokens
                        .get(start + 1)
                        .is_some_and(|t| t.is(".") || t.is("=")))
                .then(|| tokens[start].text.clone())
                .filter(|n| n != "self");
                if receiver.is_none() {
                    escapes = true;
                }
                handles.push(Binding {
                    name: receiver,
                    depth: tokens[start].depth,
                    marker: None,
                });
            }
        } else if !handles.is_empty() {
            let candidate = (t.is("drop") && tokens.get(k + 1).is_some_and(|n| n.is("(")))
                || STOP_PRIMITIVES.contains(&t.text.as_str())
                || call.stops;
            let range = candidate.then(|| {
                (
                    statement_start(tokens, k, open),
                    statement_end(tokens, k, close),
                )
            });
            if let Some(range) =
                range.filter(|&range| t.is("drop") || call.stops || is_stop(tokens, k, range))
            {
                let named = mentions(tokens, range, &handles);
                handles.retain(|b| !b.name.as_ref().is_some_and(|n| named.contains(n)));
            }
        }
        let receiver = (k >= 2
            && tokens[k - 1].is(".")
            && t.is_ident()
            && tokens.get(k + 1).is_some_and(|n| n.is("("))
            && (k < 3 || !tokens[k - 3].is(".")))
        .then(|| {
            stores
                .iter()
                .rev()
                .find(|b| b.name.as_deref() == Some(tokens[k - 2].text.as_str()))
        })
        .flatten();
        let store_method = receiver.is_some();
        let reads_sql = store_method && t.is("write_pool") && {
            let from = tokens[statement_start(tokens, k, open)].start;
            let to = tokens[statement_end(tokens, k, close)].start;
            src.literals_in(from, to).iter().any(|l| {
                let sql = l.cooked().trim_start().to_ascii_uppercase();
                sql.starts_with("SELECT") || sql.starts_with("PRAGMA")
            })
        };
        let write = if store_method && !is_read_method(&t.text) && !reads_sql {
            Some(format!("{}.{}", tokens[k - 2].text, t.text))
        } else if call.writes && !call.spawns && !store_method {
            Some(format!("{}()", t.text))
        } else {
            None
        };
        if let Some(what) = write {
            out.effects.writes = true;
            if !handles.is_empty() {
                let statement_line = tokens[statement_start(tokens, k, open)].line;
                match src
                    .marker(t.line)
                    .or_else(|| src.marker(statement_line))
                    .or_else(|| receiver.and_then(|b| b.marker))
                {
                    Some(marker) => {
                        out.used.insert(marker);
                    }
                    None => out.hits.push(Hit { line: t.line, what }),
                }
            }
        }
        // Close every `let` ending here: bind handles, aliases and stores.
        while let Some(&(end, l)) = pending.last() {
            if end > k {
                break;
            }
            pending.pop();
            let depth = tokens[l].depth;
            let (handle_names, store_names) = let_bindings(tokens, l, end, own_impl, ctx);
            let spawned_inside = spawns_at[l - open..end - open].contains(&true);
            let moved = moved_handles(tokens, (l + 1, end), &handles);
            if spawned_inside || !moved.is_empty() {
                handles.retain(|b| !b.name.as_ref().is_some_and(|n| moved.contains(n)));
                handles.extend(handle_names.into_iter().map(|name| Binding {
                    name: Some(name),
                    depth,
                    marker: None,
                }));
            }
            if opens_at[l - open..end - open].contains(&true) {
                let marker = src.marker(tokens[l].line);
                stores.extend(store_names.into_iter().map(|name| Binding {
                    name: Some(name),
                    depth,
                    marker,
                }));
            }
        }
        k += 1;
    }

    // A daemon outlives the function when an anonymous or `self` handle is
    // live, a `return` hands one back, or the tail expression names one.
    let tail_start = statement_start(tokens, close, open);
    let returned = |name: &str| (tail_start..close).any(|j| tokens[j].is(name));
    out.effects.spawns = spawned_at.is_some()
        && (escapes
            || handles
                .iter()
                .any(|b| b.name.as_deref().is_none_or(returned)));
    out.effects.stops = body_stops(tokens, f.body);
    let ret = &tokens[f.ret.0..f.ret.1];
    out.effects.serve_command = serve && ret.iter().any(|t| t.is("Command"));
    out.effects.opens_store = ret.iter().any(|t| t.is("Store"))
        && (open + 1..close).any(|j| opens_store_primitive(tokens, j));
    out
}

/// `Store::open(` / `Store::open_for_daemon(` at `tokens[j]`.
fn opens_store_primitive(tokens: &[Token], j: usize) -> bool {
    (tokens[j].is("open") || tokens[j].is("open_for_daemon"))
        && j >= 2
        && tokens[j - 1].is("::")
        && tokens[j - 2].is("Store")
}

/// Whether the body of `f` builds an `intentd serve` command: it calls a
/// hermetic serve builder or a helper returning a serve command, or holds a
/// `"serve"` literal.
fn builds_serve(src: &Source, f: &FnItem, calls: &[Effects]) -> bool {
    let tokens = &src.tokens;
    let (open, close) = f.body;
    let (from, to) = (tokens[open].start, tokens[close].start);
    (open + 1..close).any(|j| {
        tokens[j].is("hermetic_serve_command")
            || tokens[j].is("hermetic_serve_command_fixed_port")
            || calls[j - open].serve_command
    }) || src.literals_in(from, to).iter().any(|l| l.text == "serve")
}

/// `(name, type mentions Store)` for each named parameter.
fn store_params(tokens: &[Token], (p_open, p_close): (usize, usize)) -> Vec<(String, bool)> {
    let depth = tokens[p_open].depth + 1;
    let mut out = Vec::new();
    let mut k = p_open + 1;
    while k < p_close {
        let end = (k..p_close)
            .find(|&j| tokens[j].is(",") && tokens[j].depth == depth)
            .unwrap_or(p_close);
        if let Some(colon) = (k..end).find(|&j| tokens[j].is(":") && tokens[j].depth == depth) {
            if let Some(name) = tokens[k..colon]
                .iter()
                .rev()
                .find(|t| t.is_ident() && !t.is("mut"))
            {
                let typed = tokens[colon..end].iter().any(|t| t.is("Store"));
                out.push((name.text.clone(), typed));
            }
        }
        k = end + 1;
    }
    out
}

/// The struct field and tuple return types of a module, as [`Ctx`] keeps
/// them.
fn module_types(sources: &[&Source]) -> Ctx {
    let structs: Vec<(&Source, String, Fields)> = sources
        .iter()
        .flat_map(|src| {
            struct_items(&src.tokens)
                .into_iter()
                .map(move |(name, fields)| (*src, name, fields))
        })
        .collect();
    let mut daemon_structs = BTreeSet::new();
    loop {
        let before = daemon_structs.len();
        for (src, name, fields) in &structs {
            if fields
                .iter()
                .any(|(_, (a, b))| holds_daemon(&src.tokens[*a..*b], &daemon_structs))
            {
                daemon_structs.insert(name.clone());
            }
        }
        if daemon_structs.len() == before {
            break;
        }
    }
    let mut ctx = Ctx::default();
    for (src, name, fields) in &structs {
        let typed = fields
            .iter()
            .map(|(field, (a, b))| {
                (
                    field.clone(),
                    holds_daemon(&src.tokens[*a..*b], &daemon_structs),
                )
            })
            .collect();
        ctx.structs.insert(name.clone(), typed);
    }
    for src in sources {
        for f in &src.fns {
            if let Some(ty) = &f.impl_ty {
                ctx.methods
                    .entry(f.name.clone())
                    .or_default()
                    .push(ty.clone());
            }
        }
    }
    for src in sources {
        for f in &src.fns {
            let tokens = &src.tokens;
            let Some(open) = (f.ret.0..f.ret.1).find(|&k| tokens[k].is("(")) else {
                continue;
            };
            let elements = group_elements(tokens, open)
                .into_iter()
                .map(|(a, b)| {
                    let ty = &tokens[a..b];
                    (
                        holds_daemon(ty, &daemon_structs),
                        ty.iter().any(|t| t.is("Store")),
                    )
                })
                .collect();
            ctx.rets
                .insert((f.impl_ty.clone(), f.name.clone()), elements);
        }
    }
    ctx
}

/// The `(daemon handles, store bindings)` a `let` at `l` (ending at `end`)
/// may bind. A tuple pattern takes them from the called function's tuple
/// return type and a struct pattern from the struct's field types; failing
/// those, a multi-name pattern holds a store only in a name saying so and a
/// daemon only in the others.
fn let_bindings(
    tokens: &[Token],
    l: usize,
    end: usize,
    own_impl: Option<&str>,
    ctx: &Ctx,
) -> (Vec<String>, Vec<String>) {
    let depth = tokens[l].depth;
    let eq = (l + 1..end).find(|&k| tokens[k].is("=") && tokens[k].depth == depth);
    let mut first = l + 1;
    if tokens.get(first).is_some_and(|t| t.is("mut")) {
        first += 1;
    }
    let binding = |(a, b): (usize, usize)| -> Option<String> {
        tokens[a..b]
            .iter()
            .rev()
            .find(|t| t.is_ident() && !["mut", "ref", "_"].contains(&t.text.as_str()))
            .map(|t| t.text.clone())
    };
    if tokens.get(first).is_some_and(|t| t.is("(")) {
        let elements: Vec<Option<String>> = group_elements(tokens, first)
            .into_iter()
            .map(binding)
            .collect();
        let ret = eq.and_then(|eq| {
            (eq + 1..end)
                .find_map(|k| {
                    resolve(tokens, k, own_impl, &ctx.methods, &ctx.rets)
                        .into_iter()
                        .next()
                })
                .filter(|ret| ret.len() == elements.len())
        });
        if let Some(ret) = ret {
            let pick = |want: fn(&(bool, bool)) -> bool| {
                elements
                    .iter()
                    .zip(ret)
                    .filter(|(_, r)| want(r))
                    .filter_map(|(n, _)| n.clone())
                    .collect()
            };
            return (pick(|r| r.0), pick(|r| r.1));
        }
    } else if tokens.get(first).is_some_and(Token::is_ident)
        && tokens.get(first + 1).is_some_and(|t| t.is("{"))
    {
        if let Some(fields) = ctx.structs.get(&tokens[first].text) {
            let handles = group_elements(tokens, first + 1)
                .into_iter()
                .filter_map(|(a, b)| {
                    let field = tokens[a..b].iter().find(|t| t.is_ident() && !t.is("mut"))?;
                    let daemon = fields.iter().any(|(name, d)| *d && *name == field.text);
                    daemon.then(|| binding((a, b))).flatten()
                })
                .collect();
            return (handles, Vec::new());
        }
    }
    let names = let_names(tokens, l);
    if names.len() > 1 {
        let (stores, handles) = names.into_iter().partition(|n| n.contains("store"));
        (handles, stores)
    } else {
        (names.clone(), names)
    }
}

/// One round of the effects fixed point over `sources`, on top of `base`
/// (the effects of `common/`, already settled).
fn settle_effects(sources: &[&Source], ctx: &mut Ctx, base: &BTreeMap<Key, Effects>) {
    for _ in 0..8 {
        let mut next = base.clone();
        for src in sources {
            for f in &src.fns {
                let e = analyse(src, f, ctx).effects;
                let slot = next.entry((f.impl_ty.clone(), f.name.clone())).or_default();
                slot.spawns |= e.spawns;
                slot.stops |= e.stops;
                slot.writes |= e.writes;
                slot.opens_store |= e.opens_store;
                slot.serve_command |= e.serve_command;
            }
        }
        if next == ctx.effects {
            return;
        }
        ctx.effects = next;
    }
}

/// The settled effects of `common/`'s own functions.
fn common_effects(common: &[&Source]) -> BTreeMap<Key, Effects> {
    let mut ctx = module_types(common);
    settle_effects(common, &mut ctx, &BTreeMap::new());
    ctx.effects
}

/// Offenders in the module's own files `own` (`sources` also holds
/// `common/`, whose effects are `base`), as `(file, line, what)`: live
/// writes, and markers that are malformed or excuse no live write.
fn module_hits(
    sources: &[&Source],
    own: &[&Source],
    base: &BTreeMap<Key, Effects>,
) -> Vec<(String, usize, String)> {
    let mut ctx = module_types(sources);
    ctx.effects = base.clone();
    settle_effects(own, &mut ctx, base);
    let mut out = Vec::new();
    for src in own {
        let mut seen = BTreeSet::new();
        let mut used = BTreeSet::new();
        for f in &src.fns {
            let analysis = analyse(src, f, &ctx);
            used.extend(analysis.used);
            for hit in analysis.hits {
                if seen.insert(hit.line) {
                    out.push((src.rel.clone(), hit.line, hit.what));
                }
            }
        }
        for (line, marker) in src.markers() {
            if marker == Marker::Malformed {
                out.push((
                    src.rel.clone(),
                    line,
                    "malformed marker: needs a reason".into(),
                ));
            } else if !used.contains(&line) {
                out.push((
                    src.rel.clone(),
                    line,
                    "stale marker: excuses no live write".into(),
                ));
            }
        }
    }
    out
}

fn module_key(rel_to_tests: &Path) -> String {
    let first = rel_to_tests
        .components()
        .next()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .unwrap_or_default();
    first.strip_suffix(".rs").unwrap_or(&first).to_owned()
}

fn scan(root: &Path) -> (usize, Vec<String>) {
    let dir = root.join(TESTS_DIR);
    let mut modules: BTreeMap<String, Vec<Source>> = BTreeMap::new();
    for file in rust_files(&dir) {
        let rel_tests: PathBuf = file.strip_prefix(&dir).unwrap_or(&file).to_path_buf();
        let name = rel_tests.to_string_lossy();
        if name.ends_with("_lint.rs") {
            continue;
        }
        let rel = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .to_string_lossy()
            .into_owned();
        let text = fs::read_to_string(&file).expect("read test source");
        modules
            .entry(module_key(&rel_tests))
            .or_default()
            .push(Source::new(rel, &text));
    }
    let common = modules.remove("common").unwrap_or_default();
    let common: Vec<&Source> = common.iter().collect();
    let base = common_effects(&common);
    let mut scanned = common.len();
    let mut offenders = Vec::new();
    for files in modules.values() {
        scanned += files.len();
        let own: Vec<&Source> = files.iter().collect();
        let sources: Vec<&Source> = own.iter().chain(common.iter()).copied().collect();
        for (rel, line, what) in module_hits(&sources, &own, &base) {
            offenders.push(format!("{rel}:{line} ({what})"));
        }
    }
    offenders.sort();
    (scanned, offenders)
}

#[test]
fn tests_do_not_write_the_store_of_a_live_daemon() {
    let root = workspace_root();
    let (scanned, offenders) = scan(&root);
    assert!(
        scanned > 100,
        "scanned only {scanned} files under {TESTS_DIR}: file discovery is broken"
    );
    assert!(
        offenders.is_empty(),
        "\nstore writes while a spawned daemon is still running ({} site{}):\n  {}\n\n\
         A second writer racing the live daemon fails at random with \"database is locked\" \
         (SQLite 5/517; intentd#3799). Seed the store before the daemon starts with \
         `common::seed_then_start(data_dir, seed, start)` (tests/common/mod.rs) instead of a \
         per-file seed hook; for state only a daemon can make, stop it, then seed and restart \
         through the same helper. Reads are fine. If the write cannot race, put \
         `// {TAG}: allow — <reason>` on the line above it.\n",
        offenders.len(),
        if offenders.len() == 1 { "" } else { "s" },
        offenders.join("\n  "),
    );
}

#[cfg(test)]
fn hits_of(src: &str) -> Vec<(usize, String)> {
    let source = Source::new("t.rs".into(), src);
    module_hits(&[&source], &[&source], &BTreeMap::new())
        .into_iter()
        .map(|(_, line, what)| (line, what))
        .collect()
}

#[test]
fn flags_a_write_between_spawn_and_stop() {
    let src = "\
async fn t() {
    let store = Store::open(&db).await.unwrap();
    store.insert_workspace(&w).await.unwrap();
    let mut cmd = common::hermetic_serve_command(&d);
    let daemon = GuardedChild::spawn(&mut cmd).unwrap();
    store.list_workspaces().await.unwrap();
    store.insert_note(&n).await.unwrap();
    let late = intent_store::Store::open(&db).await.unwrap();
    late.get_note(&id).await.unwrap();
    late.set_setting(&k).await.unwrap();
    sqlx::query(\"DELETE FROM note\").execute(late.write_pool()).await.unwrap();
    drop(daemon);
    late.set_setting(&k).await.unwrap();
}
";
    assert_eq!(
        hits_of(src),
        vec![
            (7, "store.insert_note".to_owned()),
            (10, "late.set_setting".to_owned()),
            (11, "late.write_pool".to_owned()),
        ]
    );
}

#[test]
fn only_serve_commands_and_daemon_guards_spawn_daemons() {
    let src = "\
async fn mock_forge(dir: &Path) -> GuardedChild {
    let mut cmd = Command::new(\"python3\");
    GuardedChild::spawn(&mut cmd).unwrap()
}
fn serve_cmd(dir: &Path) -> Command {
    let mut cmd = Command::new(bin);
    cmd.arg(\"serve\");
    cmd
}
async fn t(dir: &Path) {
    let forge = mock_forge(dir).await;
    let store = Store::open(&db).await.unwrap();
    store.insert_workspace(&w).await.unwrap();
    let mut cmd = serve_cmd(dir);
    let child = cmd.spawn().unwrap();
    store.insert_workspace(&w).await.unwrap();
    let mut probe = cmd_for(\"doctor\");
    let _guard = DaemonGuard::process_only(probe.spawn().unwrap());
}
async fn u(store: &Store) {
    let _d = DaemonGuard::process_only(probe.spawn().unwrap());
    store.delete_note(&n).await.unwrap();
    let _ = DaemonGuard::process_only(probe.spawn().unwrap());
}
async fn v(store: &Store) {
    let _ = DaemonGuard::process_only(probe.spawn().unwrap());
    store.delete_note(&n).await.unwrap();
}
";
    assert_eq!(
        hits_of(src),
        vec![
            (16, "store.insert_workspace".to_owned()),
            (22, "store.delete_note".to_owned()),
        ]
    );
}

#[test]
fn stops_by_kill_wait_scope_and_helper() {
    let src = "\
struct Fixture { child: Option<GuardedChild> }
impl Fixture {
    fn start(&mut self) {
        let mut cmd = common::hermetic_serve_command(&self.dir);
        self.child = Some(GuardedChild::spawn(&mut cmd).unwrap());
    }
    async fn stop(&mut self) { self.child.take().unwrap().kill().unwrap(); }
}
async fn t(fixture: &mut Fixture) {
    let mut cmd = common::hermetic_serve_command(&d);
    {
        let mut d = DaemonGuard::process_only(spawn_guarded(&mut cmd).unwrap());
        d.kill_tree().unwrap();
        let s = Store::open(&db).await.unwrap();
        s.insert_agent(&a).await.unwrap();
    }
    {
        let _d = spawn_guarded(&mut cmd).unwrap();
    }
    let mut child = GuardedChild::spawn(&mut cmd).unwrap();
    let status = child.wait_with_timeout(limit).unwrap().expect(\"exited\");
    let s = Store::open(&db).await.unwrap();
    s.insert_agent(&a).await.unwrap();
    fixture.start();
    s.insert_agent(&a).await.unwrap();
    fixture.stop().await;
    s.insert_agent(&a).await.unwrap();
}
";
    assert_eq!(hits_of(src), vec![(25, "s.insert_agent".to_owned())]);
}

/// `common::seed_then_start` writes in its seed closure before the start
/// closure spawns, and the daemon it returns is a live handle.
#[test]
fn seed_then_start_seeds_before_the_daemon_and_returns_a_live_one() {
    let src = "\
fn spawn_serve(d: &Path) -> GuardedChild {
    let mut cmd = common::hermetic_serve_command(d);
    GuardedChild::spawn(&mut cmd).unwrap()
}
async fn seed(store: &Store) { store.upsert_note(&n).await.unwrap(); }
async fn t(d: &Path) {
    let (child, id) = common::seed_then_start(
        d,
        async |store, _| seed(store).await,
        async |id| (spawn_serve(d), id),
    )
    .await;
    let store = Store::open(&db).await.unwrap();
    store.insert_note(&n).await.unwrap();
}
";
    assert_eq!(hits_of(src), vec![(14, "store.insert_note".to_owned())]);
}

#[test]
fn follows_helpers_aliases_and_store_params() {
    let src = "\
fn spawn_serve(d: &Path) -> GuardedChild {
    let mut cmd = common::hermetic_serve_command(d);
    GuardedChild::spawn(&mut cmd).unwrap()
}
fn run_once(d: &Path) {
    let child = spawn_serve(d);
    drop(child);
}
async fn seed(store: &Store) { store.upsert_note(&n).await.unwrap(); }
async fn seed_dir(d: &Path) {
    let store = Store::open(&d.join(\"intentd.db\")).await.unwrap();
    seed(&store).await;
}
async fn t(d: &Path) {
    run_once(d);
    seed_dir(d).await;
    let child = spawn_serve(d);
    let socket = child.socket_path(d);
    let daemon = Daemon { child, data_dir: d.to_owned() };
    seed_dir(d).await;
    // live-store-write: allow — the test races two writers on purpose
    seed_dir(d).await;
    seed_dir(d).await; // live-store-write: allow
    drop(daemon);
    seed_dir(d).await;
    let (daemon, store) = (spawn_serve(d), Store::open(&db).await.unwrap());
    store.insert_note(&n).await.unwrap();
    daemon.kill().unwrap();
    store.insert_note(&n).await.unwrap();
}
";
    assert_eq!(
        hits_of(src),
        vec![
            (20, "seed_dir()".to_owned()),
            (23, "seed_dir()".to_owned()),
            (27, "store.insert_note".to_owned()),
            (23, "malformed marker: needs a reason".to_owned()),
        ]
    );
}

#[test]
fn destructuring_keeps_only_the_daemon_binding_live() {
    let src = "\
struct Booted { daemon: Daemon, port: u16 }
struct Daemon { child: GuardedChild }
fn boot(d: &Path) -> Booted {
    let mut cmd = common::hermetic_serve_command(d);
    Booted { daemon: Daemon { child: GuardedChild::spawn(&mut cmd).unwrap() }, port: 1 }
}
fn start(d: &Path) -> (common::DaemonGuard, u16, Store) {
    let mut cmd = common::hermetic_serve_command(d);
    let guard = DaemonGuard::process_only(spawn_guarded(&mut cmd).unwrap());
    (guard, 1, Store::open(&db).await.unwrap())
}
async fn t(d: &Path) {
    let Booted { daemon, port } = boot(d);
    let s = Store::open(&db).await.unwrap();
    s.insert_note(&n).await.unwrap();
    drop(daemon);
    s.insert_note(&n).await.unwrap();
    let (guard, _port, db) = start(d);
    let n: i64 = sqlx::query_scalar(\"SELECT count(*) FROM note\").fetch_one(db.write_pool()).await.unwrap();
    sqlx::query(\"DELETE FROM note\").execute(db.write_pool()).await.unwrap();
    drop(guard);
    db.insert_note(&n).await.unwrap();
}
";
    assert_eq!(
        hits_of(src),
        vec![
            (15, "s.insert_note".to_owned()),
            (20, "db.write_pool".to_owned()),
        ]
    );
}

#[test]
fn a_marker_on_the_store_binding_covers_its_writes() {
    let src = "\
async fn t(d: &Path) {
    let mut cmd = common::hermetic_serve_command(d);
    let _daemon = GuardedChild::spawn(&mut cmd).unwrap();
    // live-store-write: allow — the test injects a fault on purpose
    let faults = Store::open(&db).await.unwrap();
    faults.insert_note(&n).await.unwrap();
    faults.delete_note(&n).await.unwrap();
    let other = Store::open(&db).await.unwrap();
    other.insert_note(&n).await.unwrap();
    // live-store-write: allow — one marked statement
    other
        .delete_note(&n)
        .await
        .unwrap();
    other
        .delete_note(&n)
        .await
        .unwrap();
}
";
    assert_eq!(
        hits_of(src),
        vec![
            (9, "other.insert_note".to_owned()),
            (16, "other.delete_note".to_owned()),
        ]
    );
}

#[test]
fn probes_and_graceful_signals_do_not_stop_a_daemon() {
    let src = "\
async fn t(d: &Path, s: &Store) {
    let mut cmd = common::hermetic_serve_command(d);
    let mut daemon = GuardedChild::spawn(&mut cmd).unwrap();
    assert!(daemon.try_wait().unwrap().is_none());
    s.insert_note(&n).await.unwrap();
    signal::kill(Pid::from_raw(daemon.id()), Signal::SIGTERM).unwrap();
    s.insert_note(&n).await.unwrap();
    let timed_out = daemon.wait_with_timeout(limit).unwrap();
    s.insert_note(&n).await.unwrap();
    assert_eq!(daemon.wait_with_timeout(limit).unwrap(), daemon.try_wait().unwrap());
    s.insert_note(&n).await.unwrap();
    daemon.wait().unwrap();
    s.insert_note(&n).await.unwrap();
}
";
    assert_eq!(
        hits_of(src),
        vec![
            (5, "s.insert_note".to_owned()),
            (7, "s.insert_note".to_owned()),
            (9, "s.insert_note".to_owned()),
            (11, "s.insert_note".to_owned()),
        ]
    );
}

#[test]
fn resolves_self_returns_store_helpers_and_pragmas() {
    let src = "\
struct Fixture { child: Option<GuardedChild> }
impl Fixture {
    fn boot(&mut self) {
        let mut cmd = common::hermetic_serve_command(&self.dir);
        let child = GuardedChild::spawn(&mut cmd).unwrap();
        if self.keep { return child; }
        drop(child);
    }
    async fn run(&mut self) {
        let s = open_db(&self.dir).await;
        let daemon = Self::boot(self);
        s.set_setting(&k).await.unwrap();
        sqlx::query(\"PRAGMA wal_checkpoint\").execute(s.write_pool()).await.unwrap();
    }
}
async fn open_db(dir: &Path) -> Store {
    Store::open(&dir.join(\"intentd.db\")).await.unwrap()
}
";
    assert_eq!(hits_of(src), vec![(12, "s.set_setting".to_owned())]);
}

#[test]
fn a_marker_that_excuses_nothing_is_stale() {
    let src = "\
async fn t(d: &Path, s: &Store) {
    // live-store-write: allow — no daemon runs here
    s.insert_note(&n).await.unwrap();
    let mut cmd = common::hermetic_serve_command(d);
    let _daemon = GuardedChild::spawn(&mut cmd).unwrap();
    // live-store-write: allow — the test injects a fault on purpose
    s.insert_note(&n).await.unwrap();
    // live-store-write: allow — nothing below writes
    s.get_note(&n).await.unwrap();
}
";
    assert_eq!(
        hits_of(src),
        vec![
            (2, "stale marker: excuses no live write".to_owned()),
            (8, "stale marker: excuses no live write".to_owned()),
        ]
    );
}
