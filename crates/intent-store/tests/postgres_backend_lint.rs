//! Source lint for the Postgres backend (`crates/intent-store/src/postgres/`).
//!
//! 1. Every statement the backend runs filters each table it reads or
//!    writes by a bound tenant parameter (`tenant_id = $n`), and every
//!    INSERT sets `tenant_id`. One database holds every tenant, so a
//!    statement without the filter reads or changes other tenants' rows.
//! 2. Every function that runs a write (INSERT, UPDATE or DELETE, also
//!    inside a data-modifying CTE) holds the tenant's writer lock: it calls
//!    `begin_write()` or holds a `WriteTx`, owned or `&mut`. The per-tenant
//!    writer lock orders only writers that take it, so one writer on plain
//!    `begin()` can commit between another's read and write.
//! 3. `postgres/pending/<module>.txt` lists exactly the `Store` methods whose
//!    `pending_postgres!` forwarder (no Postgres implementation yet) lives in
//!    `src/<module>.rs` or `src/<module>/`. Porting a method removes its line,
//!    and the last port in a module deletes its file. One file per module
//!    keeps ports of different domains from editing the same file. A new
//!    `Store` method should ship with its Postgres version (`dispatch!`) or
//!    be listed on purpose.
//! 4. `postgres/sqlite_only.txt` lists exactly the `Store` methods marked
//!    `sqlite_only!` (no Postgres meaning), each with a reason. Postgres
//!    fails loudly on them, so only the files in `SQLITE_ONLY_CALLERS`,
//!    which run on a `SQLite` store alone, may call one; a new caller must
//!    prove the same or the method needs a Postgres version. The methods in
//!    `TEST_ONLY_SQLITE_ONLY` (the raw pools) are exempt from that caller
//!    list instead: their forwarder must be gated to tests
//!    (`cfg(any(test, feature = "test-support"))`), so only test code can
//!    call them and the release-graph guard keeps `test-support` out of the
//!    shipped daemon.
//!
//! 5. A function that runs more than one statement on one plain `begin()`
//!    transaction, directly or through helpers it passes the transaction
//!    to, uses `begin_read_snapshot()` instead, so its statements see one
//!    commit as a `SQLite` read transaction does. A statement in a loop
//!    (one that does not open the transaction) counts as more than one. So
//!    do statements on the pool (`self.pool`, `self.pool()`) or on one
//!    `acquire()`d connection, which each commit on their own.
//!    `MULTI_READ_ALLOWED` names the exceptions with a reason, by file and
//!    function. A `.begin()` follows `.await`, `?` and `timeout(..)`; one
//!    no `let` binds, or whose binding moves anywhere but an executor, a
//!    helper the lint follows, `commit`, `rollback` or `drop` (into a
//!    struct, an Option, a return or a closure), fails unless
//!    `BEGIN_ESCAPE_ALLOWED` names it, as the lint could not count its
//!    statements; so does one bound by any pattern but a name (`let
//!    Ok(tx) = .. else`). A helper is followed by parameter, also for
//!    escapes, and a statement counts once per chain of calls reaching it.
//!    Limits: iterator closures (`.map`, `for_each`) are not loops, a
//!    helper reached only through a trait object or a function pointer is
//!    not followed, and an allowlist entry covers every read in its
//!    function.
//!
//! Rules 1 and 2 find SQL by use (`support/sql_uses.rs`): the text that
//! reaches a sqlx call, followed through locals, consts, helpers and
//! appended text. They check it parsed by sqlparser
//! (`support/postgres_ast.rs`), which sees CTE scope, comments, strings and
//! parenthesized joins as Postgres does. A `format!` template is checked in
//! every combination of its holes' alternatives. A statement that does not
//! parse, or a sqlx argument or hole the lint cannot follow, fails unless
//! `postgres/tenant-lint-unparsed.txt` lists it with a reason. A string
//! literal that parses as a statement but reaches no sqlx call fails too
//! (orphan SQL), so SQL the follower misses cannot pass unseen.
//! `TRANSFER_PREDICATES` (repositories.rs), spliced in at run time, is
//! checked on its own. A string literal no sqlx call reaches that names a
//! schema table after FROM, JOIN, INSERT INTO or UPDATE..SET fails as an
//! SQL fragment.
//!
//! Limits of that fragment check: it reads the word right after the
//! keyword, so a table later in a comma list (`FROM (SELECT 1) s, task t`)
//! is not seen; the statement checks still see it once the text reaches a
//! sqlx call.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

#[path = "support/postgres_ast.rs"]
mod postgres_ast;
#[path = "support/sql_uses.rs"]
mod sql_uses;
use sql_uses::{list_key, rust_files, Index, LockProblem, Pos, Sql, TxKind, MAX_EXPANSIONS};
use syn::{Expr, Lit};

fn src() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// Tables that belong to the schema rather than a tenant, with why.
const NON_TENANT_TABLES: &[(&str, &str)] = &[(
    "intent_schema_migrations",
    "applied-migration ledger written by postgres::migrate, one per schema",
)];

fn non_tenant_tables() -> Vec<&'static str> {
    NON_TENANT_TABLES.iter().map(|(t, _)| *t).collect()
}

/// The Postgres backend's functions and sinks, with the `&str` consts of
/// the rest of `src/` for template holes.
fn postgres_index() -> Index {
    let read = |dir: &Path, skip: Option<&Path>| {
        let mut files = Vec::new();
        rust_files(dir, &mut files);
        files.sort();
        files
            .into_iter()
            .filter(|f| skip.is_none_or(|s| !f.starts_with(s)))
            .map(|f| {
                let text = std::fs::read_to_string(&f).expect("read");
                (f, text)
            })
            .collect::<Vec<_>>()
    };
    let postgres = src().join("postgres");
    // `nul_free` expands `NUL_FREE(col)` into an expression on `col`.
    Index::new(&read(&postgres, None), &read(&src(), Some(&postgres)))
        .with_rewriter("nul_free")
        .with_listed(unparsed_listed().keys())
}

fn unparsed_list() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("postgres/tenant-lint-unparsed.txt")
}

/// `postgres/tenant-lint-unparsed.txt`: `<file> | <prefix> — <reason>` per
/// line; `#` lines are comments.
fn unparsed_listed() -> BTreeMap<String, String> {
    let text = std::fs::read_to_string(unparsed_list()).expect("read tenant-lint-unparsed.txt");
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| {
            let (key, reason) = l
                .split_once(" — ")
                .unwrap_or_else(|| panic!("tenant-lint-unparsed.txt: `{l}` needs ` — <reason>`"));
            assert!(
                !reason.trim().is_empty(),
                "tenant-lint-unparsed.txt: `{l}` needs a reason"
            );
            (key.trim().to_owned(), reason.trim().to_owned())
        })
        .collect()
}

/// What rules 1 and 2 find in an index.
#[derive(Default)]
struct Report {
    /// Statements that parse: tenant violations.
    tenant: Vec<String>,
    /// Texts that do not parse, and sqlx arguments or holes the lint cannot
    /// follow, not listed in `tenant-lint-unparsed.txt`.
    unparsed: Vec<String>,
    /// Sinks that run a write without the writer lock.
    unlocked: BTreeSet<String>,
    /// `DEFERRED_WRITES` entries that excused an unlocked write.
    deferred_used: BTreeSet<String>,
    /// Literals that parse as a statement but reach no sqlx call.
    orphans: Vec<String>,
    /// The allowlist keys that matched.
    listed_used: BTreeSet<String>,
    parsed: usize,
}

/// The allowlist, and which keys matched at which sites.
struct Listed<'a> {
    keys: &'a BTreeMap<String, String>,
    sites: BTreeMap<String, BTreeSet<(PathBuf, Pos)>>,
}

impl Listed<'_> {
    /// `None` when `key` is listed (and records the site), else the key.
    fn unlisted(&mut self, key: String, file: &Path, at: Pos) -> Option<String> {
        if self.keys.contains_key(&key) {
            self.sites
                .entry(key)
                .or_default()
                .insert((file.to_owned(), at));
            None
        } else {
            Some(key)
        }
    }
}

/// Functions that build a write and hand it, with the `WriteTx` from their
/// `begin_write()`, to a caller that runs it on that transaction, as
/// `(file, function, why)`. The lint cannot see that executor, so it
/// excuses these only where no executor is visible.
const DEFERRED_WRITES: &[(&str, &str, &str)] = &[
    (
        "pr_monitors.rs",
        "prepare_monitor_insert",
        "returns PreparedPgWrite { tx, query }; PreparedPgWrite runs the query on that tx",
    ),
    (
        "pr_monitors.rs",
        "prepare_monitor_replace",
        "returns PreparedPgWrite { tx, query }; PreparedPgWrite runs the query on that tx",
    ),
    (
        "pr_monitors.rs",
        "prepare_monitor_cancel",
        "returns PreparedPgWrite { tx, query }; PreparedPgWrite runs the query on that tx",
    ),
];

/// The function that takes the tenant writer lock, as `(file, function,
/// table)`: it may write `table` (seeding the tenant's lock row) without
/// the lock, because writing that row is how the lock is taken.
const WRITER_LOCK_TAKER: (&str, &str, &str) = ("mod.rs", "lock_tenant_writer", "tenant_writer");

/// Reads allowed to run more than one statement without one snapshot (on
/// a plain `begin()`, an acquired connection or the pool), as `(file,
/// function, why)`. Rule 5 sends every other one to `begin_read_snapshot()`.
const MULTI_READ_ALLOWED: &[(&str, &str, &str)] = &[
    (
        "mod.rs",
        "begin_write",
        "not a read: sets the writer limits and takes the tenant writer lock \
         (lock_tenant_writer) on the transaction it then returns as WriteTx",
    ),
    (
        "mod.rs",
        "migrate_with",
        "schema migration, not a tenant read: runs DDL on one detached connection under the \
         migration advisory lock, and each step records its own progress so a crash resumes",
    ),
];

/// Plain `begin()` transactions allowed to leave the lint's view, as
/// `(file, function, why)`.
const BEGIN_ESCAPE_ALLOWED: &[(&str, &str, &str)] = &[
    (
        "mod.rs",
        "begin_write",
        "wraps the transaction in WriteTx once it holds the writer lock; every WriteTx \
         is a write transaction, which rule 2 checks",
    ),
    (
        "mod.rs",
        "bind_tenant",
        "returns the transaction it binds to the tenant to begin() and \
         begin_read_snapshot(), whose callers the lint checks",
    ),
];

/// Consts the lint does not follow because a test of their own checks
/// them, as `(file, const, test)`; their literals are not orphans.
const CHECKED_ELSEWHERE: &[(&str, &str, &str)] = &[(
    "repositories.rs",
    "TRANSFER_PREDICATES",
    "every_transfer_predicate_is_scoped",
)];

const LIST_IT: &str = "in postgres/tenant-lint-unparsed.txt";

/// Every table the Postgres migrations create.
fn schema_tables() -> BTreeSet<String> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("postgres/migrations");
    let mut out = BTreeSet::new();
    for entry in std::fs::read_dir(&dir).expect("read postgres/migrations") {
        let sql = std::fs::read_to_string(entry.expect("entry").path())
            .expect("read migration")
            .to_lowercase();
        for (at, _) in sql.match_indices("create table ") {
            let rest = sql[at + "create table ".len()..].trim_start();
            let rest = rest.strip_prefix("if not exists ").unwrap_or(rest);
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            out.insert(name);
        }
    }
    out
}

/// The functions the migrations define whose body writes (INSERT INTO,
/// UPDATE .. SET, DELETE FROM), or calls a function that does. A statement
/// that calls one writes (`SELECT intent_provision_tenant($1)`), so rule 2
/// needs the writer lock for it as for a plain INSERT.
fn writing_functions() -> BTreeSet<String> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("postgres/migrations");
    // In migration order, so a later `CREATE OR REPLACE` wins.
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("read postgres/migrations")
        .map(|entry| entry.expect("entry").path())
        .collect();
    paths.sort();
    let mut bodies = BTreeMap::new();
    for path in paths {
        let sql = std::fs::read_to_string(path)
            .expect("read migration")
            .to_lowercase();
        bodies.extend(function_bodies(&sql));
    }
    let mut writing: BTreeSet<String> = bodies
        .iter()
        .filter(|(_, body)| body_writes(body))
        .map(|(name, _)| name.clone())
        .collect();
    // Callers of writing functions write too.
    loop {
        let more: Vec<String> = bodies
            .iter()
            .filter(|(name, body)| {
                !writing.contains(*name) && writing.iter().any(|w| calls(body, w))
            })
            .map(|(name, _)| name.clone())
            .collect();
        if more.is_empty() {
            return writing;
        }
        writing.extend(more);
    }
}

/// `(name, body)` of each `CREATE [OR REPLACE] FUNCTION` in lower-cased
/// migration SQL; the body is the text between its `$$` quotes.
fn function_bodies(sql: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = sql;
    while let Some(at) = rest.find("create ") {
        rest = &rest[at + "create ".len()..];
        let head = rest.trim_start();
        let head = head.strip_prefix("or replace ").unwrap_or(head);
        let Some(head) = head.strip_prefix("function ") else {
            continue;
        };
        let name: String = head
            .trim_start()
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
            .collect();
        let name = name.rsplit('.').next().unwrap_or(&name).to_owned();
        let Some(open) = head.find("$$") else { break };
        let body = &head[open + 2..];
        let Some(close) = body.find("$$") else { break };
        out.push((name, body[..close].to_owned()));
        rest = &body[close + 2..];
    }
    out
}

fn body_writes(body: &str) -> bool {
    let words: Vec<&str> = body
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
        .collect();
    words.windows(2).enumerate().any(|(i, w)| match w {
        ["insert", "into"] | ["delete", "from"] => true,
        ["update", _] => words[i + 1..].iter().take(4).any(|x| *x == "set"),
        _ => false,
    })
}

/// Whether `body` calls function `name`.
fn calls(body: &str, name: &str) -> bool {
    body.match_indices(name).any(|(at, _)| {
        let before = body[..at].chars().next_back();
        let after = body[at + name.len()..].trim_start();
        !before.is_some_and(|c| c.is_alphanumeric() || c == '_') && after.starts_with('(')
    })
}

/// The `FROM`/`JOIN`/`INTO`/`UPDATE <table>` of a text that names a table
/// in `tables`, if any: an SQL fragment.
fn names_a_table(text: &str, tables: &BTreeSet<String>) -> Option<String> {
    let lower = text.to_lowercase();
    let words: Vec<&str> = lower
        .split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.'))
        .filter(|w| !w.is_empty())
        .collect();
    // As in SQL: INTO after INSERT, UPDATE with a SET after it; FROM and
    // UPDATE may say ONLY; a name may carry a schema (`public.task`).
    (0..words.len().saturating_sub(1)).find_map(|i| {
        let kw = words[i];
        let mut at = i + 1;
        if matches!(kw, "from" | "update") && words[at] == "only" {
            at += 1;
        }
        let table = words.get(at)?.rsplit('.').next()?;
        let shaped = match kw {
            "from" | "join" => true,
            "into" => i > 0 && words[i - 1] == "insert",
            "update" => words[at..].contains(&"set"),
            _ => false,
        };
        (shaped && tables.contains(table)).then(|| format!("{} {table}", kw.to_uppercase()))
    })
}

fn check(
    index: &Index,
    listed: &BTreeMap<String, String>,
    deferred: &[(&str, &str, &str)],
    tables: &BTreeSet<String>,
) -> Report {
    let exempt = non_tenant_tables();
    let mut report = Report::default();
    let mut list = Listed {
        keys: listed,
        sites: BTreeMap::new(),
    };
    let mut tenant = BTreeSet::new();
    let mut unparsed = BTreeSet::new();
    let mut parsed = BTreeSet::new();
    let writing = writing_functions();
    let uses = index.uses();
    for u in &uses {
        let file_name = u.file.file_name().map(|f| f.to_string_lossy().into_owned());
        let excuse = deferred
            .iter()
            .find(|(f, n, _)| Some(*f) == file_name.as_deref() && *n == u.function);
        for s in &u.sources {
            let at = format!("{}:{}", s.file.display(), s.start.line);
            let expansions = s.expansions();
            if let Some(n) = expansions.iter().map(|e| e.capped).max().filter(|n| *n > 0) {
                let key = list_key(&s.file, &s.function, &format!("combinations: {}", s.text()));
                if let Some(key) = list.unlisted(key, &s.file, s.start) {
                    unparsed.insert(format!(
                        "{at}: {} combinations (more than {MAX_EXPANSIONS}); each hole was checked \
                         alone; split the statement or list `{key} — <reason>` {LIST_IT}",
                        if n == usize::MAX { "too many".to_owned() } else { n.to_string() }
                    ));
                }
            }
            for exp in expansions {
                let mut blocked = false;
                for x in &exp.unresolved {
                    let key = list_key(&x.file, &x.function, &x.expr);
                    if let Some(key) = list.unlisted(key, &x.file, x.start) {
                        blocked = true;
                        unparsed.insert(format!(
                            "{}:{}: runs SQL the lint cannot follow (`{}`); \
                             fix it or list `{key} — <reason>` {LIST_IT}",
                            x.file.display(),
                            x.start.line,
                            x.expr
                        ));
                    }
                }
                if blocked {
                    continue;
                }
                match postgres_ast::parse_pieces(&exp.pieces) {
                    Ok(stmts) => {
                        parsed.insert((s.file.clone(), s.start));
                        for v in postgres_ast::violations(&stmts, &exempt) {
                            tenant.insert(format!("{at}: {v} is not filtered by tenant_id = $n"));
                        }
                        let (taker_file, taker, seeded) = WRITER_LOCK_TAKER;
                        let is_taker =
                            file_name.as_deref() == Some(taker_file) && u.function == taker;
                        let writes = postgres_ast::written_tables(&stmts)
                            .into_iter()
                            .any(|t| !exempt.contains(&t.as_str()) && (!is_taker || t != seeded))
                            || postgres_ast::called_functions(&stmts)
                                .iter()
                                .any(|f| writing.contains(f));
                        match (writes, excuse, &u.lock_problem) {
                            (true, Some((_, f, _)), Some(LockProblem::Hidden)) if u.takes_lock => {
                                report.deferred_used.insert((*f).to_owned());
                            }
                            (true, _, Some(why)) => {
                                report.unlocked.insert(format!(
                                    "{}:{}: {} ({why})",
                                    u.file.display(),
                                    u.line,
                                    u.function
                                ));
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        let key = list_key(&s.file, &s.function, s.text());
                        if let Some(key) = list.unlisted(key, &s.file, s.start) {
                            unparsed.insert(format!(
                                "{at}: does not parse ({e}); fix it or list \
                                 `{key} — <reason>` {LIST_IT}"
                            ));
                        }
                    }
                }
            }
        }
    }
    for r in index.unreached_literals(&uses) {
        let file_name = r.file.file_name().map(|f| f.to_string_lossy().into_owned());
        if CHECKED_ELSEWHERE
            .iter()
            .any(|(f, c, _)| Some(*f) == file_name.as_deref() && *c == r.function)
        {
            continue;
        }
        let value = index.fill_consts(&r.file, &r.value);
        let what = if postgres_ast::is_statement(&value) {
            "SQL that no sqlx call runs".to_owned()
        } else if let Some(table) = names_a_table(&value, tables) {
            format!("an SQL fragment ({table}) that no sqlx call runs")
        } else {
            continue;
        };
        let key = list_key(&r.file, &r.function, &r.value);
        if let Some(key) = list.unlisted(key, &r.file, r.start) {
            report.orphans.push(format!(
                "{}:{}: {what}; run it through sqlx, remove it, or list \
                 `{key} — <reason>` {LIST_IT}",
                r.file.display(),
                r.start.line
            ));
        }
    }
    for (key, sites) in &list.sites {
        if sites.len() > 1 {
            let at: Vec<String> = sites
                .iter()
                .map(|(f, p)| format!("{}:{}:{}", f.display(), p.line, p.column))
                .collect();
            unparsed.insert(format!(
                "`{key}` in {LIST_IT} matches {} sites ({}); a key must name one site",
                sites.len(),
                at.join(", ")
            ));
        }
    }
    report.listed_used = list.sites.into_keys().collect();
    report.tenant = tenant.into_iter().collect();
    report.unparsed = unparsed.into_iter().collect();
    report.parsed = parsed.len();
    report
}

#[test]
fn every_postgres_statement_parses_and_is_filtered_by_the_bound_tenant() {
    let listed = unparsed_listed();
    let report = check(
        &postgres_index(),
        &listed,
        DEFERRED_WRITES,
        &schema_tables(),
    );
    let mut findings = report.tenant;
    findings.extend(report.unparsed);
    findings.extend(report.orphans);
    for key in listed.keys().filter(|k| !report.listed_used.contains(*k)) {
        findings.push(format!(
            "postgres/tenant-lint-unparsed.txt: `{key}` matches no unparsed statement; remove it"
        ));
    }
    assert!(report.parsed > 0, "parsed no SQL in src/postgres");
    assert!(
        findings.is_empty(),
        "tenant rule on Postgres SQL:\n{}",
        findings.join("\n")
    );
}

#[test]
fn every_postgres_write_takes_the_tenant_writer_lock() {
    let report = check(
        &postgres_index(),
        &unparsed_listed(),
        DEFERRED_WRITES,
        &schema_tables(),
    );
    for (_, f, _) in DEFERRED_WRITES {
        assert!(
            report.deferred_used.contains(*f),
            "DEFERRED_WRITES names {f}, which runs no unlocked write; remove it"
        );
    }
    assert!(
        report.unlocked.is_empty(),
        "these Postgres writes do not run on the tenant's WriteTx:\n{}",
        report.unlocked.into_iter().collect::<Vec<_>>().join("\n")
    );
}

/// Parses a `format!` template whose holes `resolve` may know (unit tests).
fn parse_template(
    sql: &str,
    resolve: &dyn Fn(&str) -> Option<String>,
) -> Result<Vec<sqlparser::ast::Statement>, String> {
    let pieces: Vec<Option<String>> = sql_uses::split_format(sql)
        .into_iter()
        .map(|p| p.or_else(|hole| resolve(&hole).ok_or(())).ok())
        .collect();
    postgres_ast::parse_pieces(&pieces)
}

/// The AST rule's violations in one SQL text (unit tests).
fn ast_violations(sql: &str) -> Vec<String> {
    let stmts = postgres_ast::parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    postgres_ast::violations(&stmts, &non_tenant_tables())
}

#[test]
fn the_parsed_tenant_rule_catches_unscoped_shapes() {
    let ok = |sql: &str| assert_eq!(ast_violations(sql), Vec::<String>::new(), "{sql}");
    let bad = |sql: &str, want: &[&str]| assert_eq!(ast_violations(sql), want, "{sql}");

    ok("select value from settings where tenant_id = $1 and key = $2");
    ok("select value from settings where $1 = tenant_id and key = $2");
    // A Neki session setting touches no table; other settings stay unmodeled.
    ok("SET __neki.tx_mode = 'single'");
    bad("SET search_path = public", &["UNSUPPORTED SET search_path"]);
    bad(
        "SET __neki.tx_mode = $1",
        &["UNSUPPORTED SET __neki.tx_mode"],
    );
    bad(
        "select value from settings where key = $1",
        &["FROM settings"],
    );
    // Only a top-level conjunct counts: an OR branch does not filter.
    bad(
        "select value from settings where tenant_id = $1 or key = $2",
        &["FROM settings"],
    );
    ok("select value from settings where (key = $2 and tenant_id = $1)");
    bad(
        "update note set x = 1 where id in (select id from note where tenant_id = $1)",
        &["UPDATE note"],
    );
    ok("insert into settings (tenant_id, key) values ($1, $2)");
    // The parsed rule needs tenant_id in the column list, in any position.
    ok("insert into settings (key, tenant_id) values ($1, $2)");
    bad("insert into settings values ($1, $2)", &["INTO settings"]);
    ok("delete from settings where tenant_id = $1 and key = $2");
    bad("delete from settings where key = $1", &["FROM settings"]);
    // An ON CONFLICT target or a SET list does not stand in for the filter.
    bad(
        "insert into settings (key, value) values ($1, $2) on conflict (tenant_id, key) do nothing",
        &["INTO settings"],
    );
    bad(
        "update settings set tenant_id = $1 where key = $2",
        &["UPDATE settings"],
    );
    ok("update settings set value = $3 where tenant_id = $1 and key = $2");
    ok("update workspace_invite as i set revoked_at = $3 where i.tenant_id = $1 and i.id = $2");
    // A qualified filter must name the table's own alias.
    bad(
        "select n.id from note n join task t on t.id = n.id where t.tenant_id = $1",
        &["FROM note"],
    );
    ok(
        "select n.id from note n join task t on t.tenant_id = $1 and t.id = n.id \
        where n.tenant_id = $1",
    );
    // A table right before a closing parenthesis is still a table.
    bad(
        "select id from task where tenant_id = $1 and id in (select id from note)",
        &["FROM note"],
    );
    ok("select id from task where tenant_id = $1 \
        and id in (select id from note where tenant_id = $1)");
    // A subquery's filter does not cover the outer level.
    bad(
        "select id from task where id in (select id from note where tenant_id = $1)",
        &["FROM task"],
    );
    // FROM inside a function call's arguments names no table.
    ok(
        "select extract(epoch from created_at), substring(name from $2) \
        from note where tenant_id = $1",
    );
    ok("select x.value from note n, json_each(n.tags) x where n.tenant_id = $1");
    // A join inside grouping parentheses leaves its tables checked.
    bad(
        "select n.id from (note n join task t on t.id = n.id) where n.id = $1",
        &["FROM note", "JOIN task"],
    );
    ok(
        "select n.id from (note n join task t on t.tenant_id = $1 and t.id = n.id) \
        where n.tenant_id = $1",
    );
    // `(with` subqueries, quoted parentheses and comments.
    bad(
        "select id from task where tenant_id = $1 \
         and id in (with c as (select id from note) select id from c)",
        &["FROM note"],
    );
    bad("select '(' || name from note where id = $1", &["FROM note"]);
    bad(
        "select name -- count(\nfrom note where id = $1",
        &["FROM note"],
    );
    bad("select x /* f( */ from note where id = $1", &["FROM note"]);
    bad(
        "select distinct (select id from note) from task where tenant_id = $1",
        &["FROM note"],
    );
    bad("select * from \"note\" where id = $1", &["FROM note"]);
    // A CTE in scope is not a table; its body is checked.
    ok("with c as (select id from note where tenant_id = $1) select id from c");
    bad(
        "with c as (select id from note where id = $2) select id from c",
        &["FROM note"],
    );
    ok(
        "with a as (select id from note where tenant_id = $1), b as (select id from a) \
        select id from b",
    );
    // Inside its own non-recursive body, a CTE's name is the real table.
    bad(
        "with note as (select id from note n) select id from note",
        &["FROM note"],
    );
    bad(
        "with note as (select ')' as marker, id from note n) select id from note",
        &["FROM note"],
    );
    bad(
        "with note as (select /* ) */ id from note n) select id from note",
        &["FROM note"],
    );
    bad(
        "with note as (select /* /* ) */ */ id from note n) select id from note",
        &["FROM note"],
    );
    bad(
        "with note as (select $q$)$q$ as m, id from note n) select id from note",
        &["FROM note"],
    );
    bad(
        "with note as (select E'\\')' as m, id from note n) select id from note",
        &["FROM note"],
    );
    bad(
        "with note as (select 1 as \")\", id from note n) select id from note",
        &["FROM note"],
    );
    bad(
        "with c as (select ', note as (' as marker) select id from note",
        &["FROM note"],
    );
    // Under WITH RECURSIVE every sibling name is visible in every body.
    ok(
        "with recursive a as (select id from b), b as (select id from note where tenant_id = $1) \
        select id from a",
    );
    bad(
        "with recursive a as (select id from b), b as (select id from note) select id from a",
        &["FROM note"],
    );
    // A CTE is in scope only after its body, and in the statement it heads.
    bad(
        "with a as (select id from b), b as (select id from note where tenant_id = $1) \
         select id from a",
        &["FROM b"],
    );
    ok(
        "with recursive t as (select id from note where tenant_id = $1 \
        union all select n.id from note n join t on n.parent_id = t.id where n.tenant_id = $1) \
        select id from t",
    );
    // An UPDATE's target is the real table even where a CTE shares its name.
    bad(
        "with note as (select 1) update note set x = 1 where id = $2",
        &["UPDATE note"],
    );
    // UPDATE FROM and DELETE USING tables need their own filter.
    bad(
        "update note n set x = 1 from task t where n.tenant_id = $1 and t.id = n.id",
        &["FROM task"],
    );
    bad(
        "delete from note n using task t where n.tenant_id = $1 and t.id = n.id",
        &["USING task"],
    );
    // A join's ON filters only a side whose unmatched rows it drops.
    ok(
        "select n.id from note n join task t on n.tenant_id = $1 and t.tenant_id = $1 \
        and t.note_id = n.id",
    );
    ok(
        "select n.id from note n left join task t on t.tenant_id = $1 and t.note_id = n.id \
        where n.tenant_id = $1",
    );
    bad(
        "select n.id from note n left join task t on n.tenant_id = $1 and t.tenant_id = $1 \
         and t.note_id = n.id",
        &["FROM note"],
    );
    bad(
        "select t.id from note n right join task t on t.tenant_id = $1 and t.note_id = n.id \
         where n.tenant_id = $1",
        &["JOIN task"],
    );
    ok(
        "select t.id from note n right join task t on n.tenant_id = $1 and t.note_id = n.id \
        where t.tenant_id = $1",
    );
    bad(
        "select n.id from note n full join task t on n.tenant_id = $1 and t.tenant_id = $1 \
         and t.note_id = n.id",
        &["FROM note", "JOIN task"],
    );
    bad(
        "select n.id from pg_catalog.pg_class c full outer join public.note n \
         on n.tenant_id = $1 and false",
        &["JOIN public.note"],
    );
    // The same holds for the ON of a join around a parenthesized group.
    ok(
        "select n.id from note n left join (task t join hook h on h.tenant_id = $1 \
        and h.id = t.id) on t.tenant_id = $1 where n.tenant_id = $1",
    );
    bad(
        "select n.id from note n right join (task t join hook h on h.id = t.id) \
         on t.tenant_id = $1 and h.tenant_id = $1 where n.tenant_id = $1",
        &["JOIN task", "JOIN hook"],
    );
    // Shapes the rule does not model fail instead of passing.
    bad(
        "select id from task where tenant_id = $1 and id in (table note)",
        &["FROM note"],
    );
    for sql in [
        "merge into note n using task t on t.id = n.id when matched then delete",
        "truncate note",
    ] {
        let v = ast_violations(sql);
        assert!(
            v.len() == 1 && v[0].starts_with("UNSUPPORTED "),
            "{sql}: {v:?}"
        );
    }
    // An ON CONFLICT target must include tenant_id.
    bad(
        "insert into settings (tenant_id, key, value) values ($1, $2, $3) \
         on conflict (key) do update set value = excluded.value",
        &["ON CONFLICT settings"],
    );
    bad(
        "insert into settings (tenant_id, key) values ($1, $2) \
         on conflict on constraint settings_pkey do nothing",
        &["ON CONFLICT settings"],
    );
    ok(
        "insert into settings (tenant_id, key, value) values ($1, $2, $3) \
        on conflict (tenant_id, key) do update set value = excluded.value",
    );
    ok("insert into settings (tenant_id, key) values ($1, $2) on conflict do nothing");
    // Every tenant filter in a statement uses the same placeholder.
    bad(
        "select n.id from note n join task t on t.tenant_id = $2 and t.id = n.id \
         where n.tenant_id = $1",
        &["TENANT PARAMS $1 $2"],
    );
    bad(
        "select id from task where tenant_id = $1 \
         and id in (select id from note where tenant_id = $3)",
        &["TENANT PARAMS $1 $3"],
    );
    // Schema tables and the system catalogs are exempt; any other
    // schema-qualified name is checked.
    ok("select version from intent_schema_migrations");
    ok("select c.relname from pg_catalog.pg_class c \
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace where n.nspname = $1");
    bad(
        "select id from public.note where id = $1",
        &["FROM public.note"],
    );
}

#[test]
fn templates_parse_with_consts_and_fillers() {
    let none = |_: &str| None;
    let cols = |n: &str| (n == "COLS").then(|| "id, name".to_owned());
    let stmts =
        parse_template("select {COLS} from note where tenant_id = $1", &cols).expect("parses");
    assert_eq!(
        stmts[0].to_string(),
        "SELECT id, name FROM note WHERE tenant_id = $1"
    );
    // A resolved const is checked in place: its tables count.
    let live = |n: &str| (n == "LIVE").then(|| "exists(select 1 from workspace)".to_owned());
    let stmts = parse_template("select id from note where tenant_id = $1 and {LIVE}", &live)
        .expect("parses");
    assert_eq!(postgres_ast::violations(&stmts, &[]), ["FROM workspace"]);
    for sql in [
        "select id from browser_tab where tenant_id = $1 and {filter} = $2 {ORDER}",
        "select * from ({ranked}) r where true{after} limit $11",
        "update note set title = $4, {content}tags = $7 where tenant_id = $1",
        "insert into script (tenant_id, id) values {}",
        "update workspace set {column} = $3 where tenant_id = $1 and id = $2",
        "select id from m where tenant_id = $1 and m.sequence {comparison} $6 order by m.sequence {order}",
        "select id from note where tenant_id = $1 and tags @> '{{a}}'",
    ] {
        assert!(parse_template(sql, &none).is_ok(), "{sql}");
    }
    assert!(parse_template("delete note failed: {e}", &none).is_err());
}

/// The tables unit cases treat as the schema.
fn unit_tables() -> BTreeSet<String> {
    ["note", "task", "hook", "settings"]
        .into_iter()
        .map(str::to_owned)
        .collect()
}

/// [`check`] on in-memory source files.
fn check_files(files: &[(&str, &str)]) -> Report {
    let files: Vec<(PathBuf, String)> = files
        .iter()
        .map(|(p, s)| (PathBuf::from(p), (*s).to_owned()))
        .collect();
    check(
        &Index::new(&files, &[]),
        &BTreeMap::new(),
        &[],
        &unit_tables(),
    )
}

/// [`check`] on one in-memory source file named `x.rs`.
fn check_source(source: &str) -> Report {
    check_files(&[("x.rs", source)])
}

/// The texts (or unresolved expressions) that reach a sqlx call in `source`.
fn texts_by_use(source: &str) -> BTreeSet<String> {
    let index = Index::new(&[(PathBuf::from("x.rs"), source.to_owned())], &[]);
    index
        .uses()
        .into_iter()
        .flat_map(|u| u.sources)
        .map(|s| match s.sql {
            Sql::Text { text, .. } => text,
            Sql::Unresolved(expr) => format!("UNRESOLVED {expr}"),
        })
        .collect()
}

/// The tenant findings without their `file:line: ` prefix.
fn tenant_findings(report: &Report) -> Vec<&str> {
    report
        .tenant
        .iter()
        .map(|f| f.split(": ").nth(1).unwrap_or(f))
        .collect()
}

#[test]
fn sql_is_found_where_sqlx_runs_it() {
    let source = r#"
        const COLS: &str = "id, title";
        const BY_ID: &str = concat!("select ", "id from note where tenant_id = $1");
        const UNUSED: &str = "select id from note";

        fn fts(filters: &str) -> String {
            format!("select id from note n where n.tenant_id = $1{filters}")
        }

        impl PgStore {
            async fn literal(&self) {
                sqlx::query("select id from task where tenant_id = $1");
            }
            async fn local(&self) {
                let sql = format!("select {COLS} from note where tenant_id = $1");
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
            }
            async fn branches(&self, human: bool) {
                let sql = if human { "delete from a where tenant_id = $1" } else { BY_ID };
                sqlx::query_scalar::<_, i64>(sql);
            }
            async fn helper_return(&self) {
                sqlx::query_as::<_, (String,)>(&fts(""));
            }
            async fn caller(&self, mut tx: WriteTx) {
                self.run(tx, "update b set x = 1 where tenant_id = $1").await;
            }
            async fn run(&self, tx: WriteTx, sql: &'static str) {
                sqlx::query(sql).execute(&mut *tx);
            }
            async fn looped(&self) {
                for sql in ["delete from c where tenant_id = $1", "delete from d where tenant_id = $1"] {
                    sqlx::query(sql);
                }
                for (sql, what) in [("select 1 from e where tenant_id = $1", "read e failed")] {
                    sqlx::query(sql);
                }
                let exec = |sql: &'static str| sqlx::query(sql).bind(1);
                exec("select 1 from f where tenant_id = $1");
            }
            async fn field(&self) {
                sqlx::query(self.sql.as_str());
            }
            async fn prose(&self) -> Error {
                let what = "delete note failed";
                tracing::warn!("select failed: {what}");
                Error::Internal(format!("update hook state failed: {what}"))
            }
        }
    "#;
    let want: BTreeSet<String> = [
        "select id from task where tenant_id = $1",
        "select {COLS} from note where tenant_id = $1",
        "delete from a where tenant_id = $1",
        "select id from note where tenant_id = $1",
        "select id from note n where n.tenant_id = $1{filters}",
        "update b set x = 1 where tenant_id = $1",
        "delete from c where tenant_id = $1",
        "delete from d where tenant_id = $1",
        "select 1 from e where tenant_id = $1",
        "select 1 from f where tenant_id = $1",
        "UNRESOLVED self . sql",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    assert_eq!(texts_by_use(source), want);
    let report = check_source(source);
    assert_eq!(report.tenant, Vec::<String>::new());
    assert_eq!(report.unparsed.len(), 1, "{:?}", report.unparsed);
    assert_eq!(report.parsed, 10, "{:?}", report.unparsed);
    assert!(report.unparsed[0].contains("cannot follow (`self . sql`)"));
    // The unused statement is orphan SQL; error text is not SQL at all.
    assert_eq!(report.orphans.len(), 1, "{:?}", report.orphans);
    assert!(report.orphans[0].starts_with("x.rs:4: "));
}

#[test]
fn sql_that_does_not_parse_or_scope_is_reported() {
    // A const spliced into a template is checked as part of it.
    let report = check_source(
        r#"const BASE: &str = "select id from task";
        async fn f(&self) {
            sqlx::query!("selec id frm note where tenant_id = $1");
            sqlx::query(&format!("{BASE} where id = $1"));
            sqlx::query(&format!("{} where id = $1", BASE));
            sqlx::query_as!(Row, "select id from note");
            sqlx::raw_sql("select id from task where tenant_id = $1 and");
        }"#,
    );
    assert_eq!(report.unparsed.len(), 2, "{:?}", report.unparsed);
    assert!(report.unparsed.iter().all(|f| f.contains("does not parse")));
    assert_eq!(
        tenant_findings(&report),
        ["FROM task is not filtered by tenant_id = $n"; 2]
            .into_iter()
            .chain(["FROM note is not filtered by tenant_id = $n"])
            .collect::<Vec<_>>()
    );
    // A listed statement passes, and its key counts as used.
    let index = Index::new(
        &[(
            PathBuf::from("x.rs"),
            r#"fn f() { sqlx::query("selec 1"); }"#.to_owned(),
        )],
        &[],
    );
    let key = list_key(Path::new("x.rs"), "f", "selec 1");
    let listed = BTreeMap::from([(key.clone(), "spike".to_owned())]);
    let report = check(&index, &listed, &[], &unit_tables());
    assert!(report.unparsed.is_empty());
    assert!(report.listed_used.contains(&key));
}

/// Each probe hides an unscoped or unfollowable statement; the lint must
/// report it, not pass it.
#[test]
fn the_lint_fails_closed_on_every_probe() {
    let unscoped = |source: &str, want: &str| {
        let report = check_source(source);
        assert_eq!(
            tenant_findings(&report),
            [format!("{want} is not filtered by tenant_id = $n").as_str()],
            "{source}\n{:?}",
            report.unparsed
        );
    };
    let unfollowed = |source: &str, expr: &str| {
        let report = check_source(source);
        assert!(
            report
                .unparsed
                .iter()
                .any(|f| f.contains("cannot follow (`") && f.contains(expr)),
            "{source}\n{:?}",
            report.unparsed
        );
    };
    // Executor methods given the statement, in method and path form.
    unscoped(
        r#"async fn f(mut tx: WriteTx) { (&mut *tx).execute("DELETE FROM note WHERE id = $1").await; }"#,
        "FROM note",
    );
    unscoped(
        r#"async fn f(mut tx: WriteTx) { tx.fetch_all("SELECT id FROM note").await; }"#,
        "FROM note",
    );
    unscoped(
        r#"async fn f(mut tx: WriteTx) { sqlx::Executor::execute(&mut *tx, "DELETE FROM task").await; }"#,
        "FROM task",
    );
    // Every alternative of a hole is checked.
    unscoped(
        r#"fn f(flag: bool) {
            let sub = if flag { "select id from task" } else { "select 1" };
            sqlx::query(&format!("select id from note where tenant_id = $1 and id in ({sub})"));
        }"#,
        "FROM task",
    );
    // Appended text and builder pushes are part of the statement.
    unscoped(
        r#"fn f() {
            let mut sql = String::from("select id from note where tenant_id = $1");
            sql.push_str(" and id in (select id from task)");
            sqlx::query(&sql);
        }"#,
        "FROM task",
    );
    unscoped(
        r#"fn f() {
            let mut qb = sqlx::QueryBuilder::new("select id from note where tenant_id = $1");
            qb.push(" union select id from task");
        }"#,
        "FROM task",
    );
    unscoped(
        r#"fn f(args: A) { sqlx::QueryBuilder::with_arguments("select id from task", args); }"#,
        "FROM task",
    );
    // Aliased and bare sink names.
    unscoped(
        r#"use sqlx::query as q; fn f() { q("select id from task"); }"#,
        "FROM task",
    );
    unscoped(r#"fn f() { query!("select id from task"); }"#, "FROM task");
    // A qualified helper resolves in its own module.
    let report = check_files(&[
        (
            "x.rs",
            r#"fn sql() -> &'static str { "select id from note where tenant_id = $1" }
            fn f() { sqlx::query(sql()); sqlx::query(other::sql()); }"#,
        ),
        (
            "other.rs",
            r#"pub fn sql() -> &'static str { "select id from task" }"#,
        ),
    ]);
    assert_eq!(
        tenant_findings(&report),
        ["FROM task is not filtered by tenant_id = $n"]
    );
    // Holes and sinks the walk cannot follow.
    unfollowed(
        r#"fn f(&self) { sqlx::query(&format!("select n.id from note n {} where n.tenant_id = $1", self.join)); }"#,
        "self . join",
    );
    unfollowed(
        r#"fn f() {
            let cols = vec!["id", "title"].join(", ");
            sqlx::query(&format!("select {cols} from note where tenant_id = $1"));
        }"#,
        r#"vec ! ["id" , "title"] . join (", ")"#,
    );
    unfollowed(
        r#"fn f(o: Option<&str>) {
            let order = o.unwrap_or("id");
            sqlx::query(&format!("select id from note where tenant_id = $1 order by {order}"));
        }"#,
        r#"o . unwrap_or ("id")"#,
    );
    unfollowed(
        r#"async fn f(p: P) {
            tokio::select! { r = sqlx::query("select id from task").fetch_one(&p) => {} }
        }"#,
        "tokio :: select !",
    );
    unfollowed(
        r#"macro_rules! m { () => { sqlx::query("select id from task") }; }"#,
        "sqlx :: query",
    );
    unfollowed(
        r#"fn f() { ["select id from task where tenant_id = $1"].iter().map(sqlx::query); }"#,
        "sqlx :: query",
    );
    unfollowed(
        r#"fn f() {
            let run = |s: &str| sqlx::query(s);
            ["select id from task where tenant_id = $1"].iter().map(run);
        }"#,
        "s",
    );
    // An unresolved hole is listed by its expression.
    let index = Index::new(
        &[(
            PathBuf::from("x.rs"),
            r#"fn f(&self) { sqlx::query(&format!("select n.id from note n where n.tenant_id = $1 {}", self.order)); }"#
                .to_owned(),
        )],
        &[],
    );
    let key = list_key(Path::new("x.rs"), "f", "self . order");
    let report = check(
        &index,
        &BTreeMap::from([(key.clone(), "order by".to_owned())]),
        &[],
        &unit_tables(),
    );
    assert!(report.unparsed.is_empty(), "{:?}", report.unparsed);
    assert!(report.listed_used.contains(&key));
}

/// Regression cases from the second review: each hides an unscoped
/// statement, a write off the lock, or a fragment, and must be reported.
#[test]
fn the_lint_fails_closed_on_the_second_review_probes() {
    let unscoped = |source: &str, want: &str| {
        let report = check_source(source);
        assert!(
            tenant_findings(&report)
                .contains(&format!("{want} is not filtered by tenant_id = $n").as_str()),
            "{source}\n{:?}\n{:?}",
            report.tenant,
            report.unparsed
        );
    };
    // A local's field that gained text is that text (Filters::sql).
    unscoped(
        r#"fn f() {
            let mut f = Filters::default();
            f.push(" AND id IN (SELECT id FROM task)");
            let sql = format!("SELECT id FROM note WHERE tenant_id = $1{}", f.sql);
            sqlx::query(&sql);
        }"#,
        "FROM task",
    );
    // Every alternative of each hole is checked.
    unscoped(
        r#"fn f() {
            for a in ["a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "(select id from task)"] {
                for b in ["true", "true", "true", "true", "true", "true", "true", "true", "true"] {
                    sqlx::query(&format!("select {a} from note where tenant_id = $1 and {b}"));
                }
            }
        }"#,
        "FROM task",
    );
    // A hole with MAX_EXPANSIONS + 1 alternatives (match arms) shows each.
    let arms = (0..MAX_EXPANSIONS).fold(String::new(), |mut out, i| {
        let _ = write!(out, "{i} => \"true\", ");
        out
    });
    unscoped(
        &format!(
            "fn f(k: u8) {{ let c = match k {{ {arms}_ => \"id in (select id from task)\" }}; \
             sqlx::query(&format!(\"select id from note where tenant_id = $1 and {{c}}\")); }}"
        ),
        "FROM task",
    );
    // A cast and a `+` of texts are followed as text.
    unscoped(
        r#"fn f() {
            let clause = " and id in (select id from task)";
            sqlx::query(&format!("select id from note where tenant_id = $1{}", clause as &str));
        }"#,
        "FROM task",
    );
    unscoped(
        r#"fn f() {
            let head = "select id from note where tenant_id = $1".to_owned();
            let tail = " union select id from task";
            sqlx::query(&(head + &tail));
        }"#,
        "FROM task",
    );
    // A key that matches two sites fails.
    let index = Index::new(
        &[(
            PathBuf::from("x.rs"),
            r#"fn f(&self) {
                sqlx::query(&format!("select id from note where tenant_id = $1 {}", self.order));
                sqlx::query(&format!("select id from task where tenant_id = $1 {}", self.order));
            }"#
            .to_owned(),
        )],
        &[],
    );
    let key = list_key(Path::new("x.rs"), "f", "self . order");
    let report = check(
        &index,
        &BTreeMap::from([(key, "order by".to_owned())]),
        &[],
        &unit_tables(),
    );
    assert!(
        report
            .unparsed
            .iter()
            .any(|f| f.contains("matches 2 sites")),
        "{:?}",
        report.unparsed
    );
    // cfg(not(test)) code is production code; cfg(any(test, ..)) is not.
    let report = check_source(
        r#"#[cfg(not(test))]
        fn f() { let s = "DELETE FROM note WHERE tenant_id = $1"; }
        #[cfg(any(test, feature = "test-support"))]
        fn g() { let s = "DELETE FROM hook"; }"#,
    );
    assert_eq!(report.orphans.len(), 1, "{:?}", report.orphans);
    assert!(report.orphans[0].starts_with("x.rs:2: "));
}

/// Regression cases from the third review.
#[test]
fn the_lint_reports_capped_statements_and_shaped_fragments() {
    // A statement past the cap is a listable finding.
    let arms = (0..MAX_EXPANSIONS).fold(String::new(), |mut out, i| {
        let _ = write!(out, "{i} => \"true\", ");
        out
    });
    let report = check_source(&format!(
        "fn f(k: u8) {{ let c = match k {{ {arms}_ => \"false\" }}; \
         sqlx::query(&format!(\"select id from note where tenant_id = $1 and {{c}}\")); }}"
    ));
    assert!(
        report
            .unparsed
            .iter()
            .any(|f| f.contains(&format!("{} combinations", MAX_EXPANSIONS + 1))),
        "{:?}",
        report.unparsed
    );
    // Within the cap every combination is checked: true/true is unscoped.
    let nine = r#"["tenant_id = $1", "tenant_id = $1", "tenant_id = $1", "tenant_id = $1",
        "tenant_id = $1", "tenant_id = $1", "tenant_id = $1", "tenant_id = $1", "true"]"#;
    let report = check_source(&format!(
        "fn f() {{ for a in {nine} {{ for b in {nine} {{ \
         sqlx::query(&format!(\"select id from note where {{a}} and {{b}}\")); }} }} }}"
    ));
    assert_eq!(
        tenant_findings(&report),
        ["FROM note is not filtered by tenant_id = $n"],
        "{:?}",
        report.unparsed
    );
    // Fragments: concat! of literals, a {CONST} hole, ONLY and a schema.
    let report = check_source(
        r#"const TASK: &str = "task";
        fn f() {
            let a = concat!(" UNION SELECT id ", "FROM task");
            let b = " UNION SELECT id FROM {TASK}";
            let c = " UNION SELECT id FROM ONLY public.task";
        }"#,
    );
    assert_eq!(report.orphans.len(), 3, "{:?}", report.orphans);
    // Two listed sites on one line still count as two.
    let index = Index::new(
        &[(
            PathBuf::from("x.rs"),
            r#"fn f(&self) { sqlx::query(&format!("select id from note where tenant_id = $1 {}", self.order)); sqlx::query(&format!("select id from note where tenant_id = $1 {}", self.order)); }"#
                .to_owned(),
        )],
        &[],
    );
    let key = list_key(Path::new("x.rs"), "f", "self . order");
    let report = check(
        &index,
        &BTreeMap::from([(key, "order by".to_owned())]),
        &[],
        &unit_tables(),
    );
    assert!(
        report
            .unparsed
            .iter()
            .any(|f| f.contains("matches 2 sites")),
        "{:?}",
        report.unparsed
    );
}

#[test]
fn unreached_fragments_that_name_a_table_fail() {
    let report = check_source(
        r#"fn f() {
            let mut sql = String::from("select id from note where tenant_id = $1");
            sql.push_str(" and id in (select id from note where tenant_id = $1)");
            sqlx::query(&sql);
            let stray = " UNION SELECT id FROM task";
            let prose = "insert note failed";
        }"#,
    );
    assert_eq!(report.orphans.len(), 1, "{:?}", report.orphans);
    assert!(report.orphans[0].starts_with("x.rs:5: an SQL fragment (FROM task)"));
}

#[test]
fn the_writer_lock_follows_the_transaction_binding() {
    let unlocked = |source: &str, deferred: &[(&str, &str, &str)]| -> Vec<String> {
        let index = Index::new(&[(PathBuf::from("x.rs"), source.to_owned())], &[]);
        check(&index, &BTreeMap::new(), deferred, &unit_tables())
            .unlocked
            .into_iter()
            .collect()
    };
    // Only a binding whose every branch is begin_write() holds the lock.
    let branch = r#"async fn g(&self, flag: bool) {
        let mut tx = if flag { self.begin_write().await? } else { self.begin().await? };
        sqlx::query("DELETE FROM t WHERE tenant_id = $1").execute(&mut *tx).await; }"#;
    // A later binding of the same name that is not a WriteTx hides it.
    let shadow = r#"async fn h(&self) {
        let mut tx = self.begin_write().await?;
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM t WHERE tenant_id = $1").execute(&mut *tx).await; }"#;
    let both = r#"async fn k(&self, flag: bool) {
        let mut tx = if flag { self.begin_write().await? } else { self.begin_write().await.map_err(db)? };
        sqlx::query("DELETE FROM t WHERE tenant_id = $1").execute(&mut *tx).await; }"#;
    assert_eq!(unlocked(branch, &[]).len(), 1);
    assert_eq!(unlocked(shadow, &[]).len(), 1);
    assert!(unlocked(both, &[]).is_empty());
    // DEFERRED_WRITES excuses a hidden executor, not a visible one.
    let deferred = [("x.rs", "p", "hands the query and tx to its caller")];
    let hidden = r#"async fn p(&self) {
        let q = sqlx::query("DELETE FROM t WHERE tenant_id = $1");
        let tx = self.begin_write().await?; }"#;
    let visible = r#"async fn p(&self) {
        sqlx::query("DELETE FROM t WHERE tenant_id = $1").execute(&self.pool).await; }"#;
    // A deferred function still takes the lock; only the executor hides.
    let lockless = r#"async fn p(&self) {
        let q = sqlx::query("DELETE FROM t WHERE tenant_id = $1"); }"#;
    assert!(unlocked(hidden, &deferred).is_empty());
    assert_eq!(unlocked(visible, &deferred).len(), 1);
    assert_eq!(unlocked(lockless, &deferred).len(), 1);
}

#[test]
fn orphan_sql_fails_the_lint() {
    let report = check_source(
        r#"fn f() {
            let reached = "DELETE FROM note WHERE tenant_id = $1";
            sqlx::query(reached);
            let orphan = "DELETE FROM task WHERE tenant_id = $1";
            let prose = "delete note failed";
        }
        #[cfg(test)]
        mod tests {
            fn t() { let fixture = "DELETE FROM hook"; }
        }"#,
    );
    assert_eq!(report.orphans.len(), 1, "{:?}", report.orphans);
    assert!(report.orphans[0].starts_with("x.rs:4: "));
}

#[test]
fn the_writer_lock_rule_catches_unlocked_writes() {
    let unlocked = |source: &str| -> Vec<String> {
        check_source(source)
            .unlocked
            .into_iter()
            .map(|f| {
                let f = f.split(": ").nth(1).unwrap_or(&f).to_owned();
                f.split(" (").next().unwrap_or(&f).to_owned()
            })
            .collect()
    };
    let locked = r#"async fn a(&self) { let tx = self.begin_write().await?;
        sqlx::query("UPDATE t SET x = 1 WHERE tenant_id = $1"); }"#;
    let begin = r#"async fn b(&self) { let tx = self.begin().await?;
        sqlx::query("DELETE FROM t WHERE tenant_id = $1"); }"#;
    let read = r#"async fn c(&self) { let tx = self.begin().await?;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1"); }"#;
    // An owned WriteTx (parameter or typed binding) also proves the lock.
    let owned = r#"async fn d(mut tx: super::WriteTx) {
        sqlx::query("DELETE FROM t WHERE tenant_id = $1"); }"#;
    let bound = r#"async fn e(self) { let mut tx: WriteTx = self.tx;
        sqlx::query("DELETE FROM t WHERE tenant_id = $1"); }"#;
    let borrowed = r#"async fn g(tx: &mut WriteTx) {
        sqlx::query("INSERT INTO t (tenant_id) VALUES ($1)").execute(&mut **tx); }"#;
    let shared = r#"async fn h(tx: &WriteTx) {
        sqlx::query("INSERT INTO t (tenant_id) VALUES ($1)"); }"#;
    let mentioned = r#"async fn f(&self) { // not a WriteTx
        sqlx::query("DELETE FROM t WHERE tenant_id = $1"); }"#;
    // A data-modifying CTE writes.
    let cte = r#"async fn i(&self) { sqlx::query("WITH d AS (DELETE FROM t \
        WHERE tenant_id = $1 RETURNING id) SELECT id FROM d"); }"#;
    // A helper's executor counts when every caller passes the WriteTx.
    let helper = r#"async fn j(&self) { let mut tx = self.begin_write().await?;
        run(&mut tx, "DELETE FROM t WHERE tenant_id = $1").await; }
        async fn run(conn: &mut PgConnection, sql: &str) { sqlx::query(sql).execute(conn).await; }"#;
    let helper_unlocked = format!(
        "{helper}\nasync fn k(&self) {{ let mut c = self.pool.acquire().await?;
        run(&mut c, \"DELETE FROM t WHERE tenant_id = $1\").await; }}"
    );
    // The migration ledger has no tenant to lock.
    let ledger = r#"async fn k(&self) {
        sqlx::query("INSERT INTO intent_schema_migrations (version) VALUES ($1)"); }"#;
    // The lock taken after the write, a write on the pool next to a held
    // lock, a write in a spawned task, and a lock that is not the executor.
    let after = r#"async fn l(&self) { let q = sqlx::query("DELETE FROM t WHERE tenant_id = $1");
        let tx = self.begin_write().await?; }"#;
    let pool = r#"async fn m(&self) { let tx = self.begin_write().await?;
        sqlx::query("DELETE FROM t WHERE tenant_id = $1").execute(&self.pool).await; }"#;
    let spawned = r#"async fn n(&self) { let mut tx = self.begin_write().await?;
        tokio::spawn(async move {
            sqlx::query("DELETE FROM t WHERE tenant_id = $1").execute(&mut *tx).await
        }); }"#;
    let beside = r#"async fn del(conn: &mut PgConnection, _w: &mut WriteTx) {
        sqlx::query("DELETE FROM t WHERE tenant_id = $1").execute(conn).await; }"#;
    // Calling a function whose body writes is a write; a pure one is not.
    let provision = r#"async fn o(&self) { let mut tx = self.begin().await?;
        sqlx::query("SELECT intent_provision_tenant($1)").execute(&mut *tx).await; }"#;
    let provision_locked = r#"async fn q(&self) { let mut tx = self.begin_write().await?;
        sqlx::query("SELECT intent_provision_tenant($1)").execute(&mut *tx).await; }"#;
    let provision_from = r#"async fn r(&self) { let mut tx = self.begin().await?;
        sqlx::query("SELECT * FROM intent_provision_tenant($1)").execute(&mut *tx).await; }"#;
    let provision_schema = r#"async fn t(&self) { let mut tx = self.begin().await?;
        sqlx::query("SELECT public.intent_provision_tenant($1)").execute(&mut *tx).await; }"#;
    let pure = r#"async fn s(&self) { let mut tx = self.begin().await?;
        sqlx::query("SELECT intent_now_iso() FROM t WHERE tenant_id = $1").execute(&mut *tx).await; }"#;
    for ok in [
        locked,
        read,
        owned,
        bound,
        borrowed,
        ledger,
        helper,
        provision_locked,
        pure,
    ] {
        assert_eq!(unlocked(ok), Vec::<String>::new(), "{ok}");
    }
    assert_eq!(unlocked(begin), ["b"]);
    assert_eq!(unlocked(shared), ["h"]);
    assert_eq!(unlocked(mentioned), ["f"]);
    assert_eq!(unlocked(cte), ["i"]);
    assert_eq!(unlocked(&helper_unlocked), ["run"]);
    assert_eq!(unlocked(after), ["l"]);
    assert_eq!(unlocked(pool), ["m"]);
    assert_eq!(unlocked(spawned), ["n"]);
    assert_eq!(unlocked(beside), ["del"]);
    assert_eq!(unlocked(provision), ["o"]);
    assert_eq!(unlocked(provision_from), ["r"]);
    assert_eq!(unlocked(provision_schema), ["t"]);
    assert_eq!(unlocked(&format!("{locked}\n{read}\n{begin}")), ["b"]);
}

/// `(table, predicate)` of `TRANSFER_PREDICATES` in repositories.rs.
fn transfer_predicates() -> Vec<(String, String)> {
    let path = src().join("postgres/repositories.rs");
    let file = syn::parse_file(&std::fs::read_to_string(path).expect("read")).expect("parse");
    let mut out = Vec::new();
    for item in &file.items {
        let syn::Item::Const(item_const) = item else {
            continue;
        };
        if item_const.ident != "TRANSFER_PREDICATES" {
            continue;
        }
        let Expr::Reference(reference) = &*item_const.expr else {
            continue;
        };
        let Expr::Array(array) = &*reference.expr else {
            continue;
        };
        let text = |e: &Expr| match e {
            Expr::Lit(syn::ExprLit {
                lit: Lit::Str(value),
                ..
            }) => value.value(),
            _ => String::new(),
        };
        for pair in &array.elems {
            let Expr::Tuple(tuple) = pair else { continue };
            out.push((text(&tuple.elems[0]), text(&tuple.elems[1])));
        }
    }
    out
}

/// The tenant violations of a transfer read of `table` scoped by
/// `predicate`, as `transfer_table_stats` and the snapshot build it.
fn predicate_violations(table: &str, predicate: &str) -> Vec<String> {
    let sql = format!("SELECT 1 FROM \"{table}\" WHERE tenant_id = $1 AND ({predicate})");
    match postgres_ast::parse(&sql) {
        Ok(stmts) => postgres_ast::violations(&stmts, &non_tenant_tables()),
        Err(e) => vec![format!("does not parse: {e}")],
    }
}

#[test]
fn every_transfer_predicate_is_scoped() {
    let predicates = transfer_predicates();
    assert!(predicates.len() > 10, "found no TRANSFER_PREDICATES");
    let findings: Vec<String> = predicates
        .iter()
        .flat_map(|(t, p)| {
            predicate_violations(t, p)
                .into_iter()
                .map(move |v| format!("{t}: {v}"))
        })
        .collect();
    assert!(findings.is_empty(), "{findings:#?}");
    assert_eq!(
        predicate_violations(
            "agent_message",
            "agent_id IN (SELECT id FROM agent_session WHERE workspace_id = $2)"
        ),
        ["FROM agent_session"]
    );
}

fn pending_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("postgres/pending")
}

/// The top-level module under `src/` a file belongs to: `lib.rs` is `lib`,
/// `pr_monitor_repo/prepared_write.rs` is `pr_monitor_repo`.
fn module_of(file: &Path) -> String {
    let rel = file.strip_prefix(src()).expect("file under src");
    let first = rel.components().next().expect("nonempty path");
    let first = first.as_os_str().to_string_lossy();
    first.strip_suffix(".rs").unwrap_or(&first).to_owned()
}

/// `(module, method)` for every `pending_postgres!` forwarder.
fn pending_in_source() -> BTreeSet<(String, String)> {
    let mut files = Vec::new();
    rust_files(&src(), &mut files);
    let mut out = BTreeSet::new();
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read");
        for part in text.split("pending_postgres!(").skip(1) {
            let part = part.strip_prefix("panic ").unwrap_or(part);
            if let Some(name) = part.strip_prefix('"').and_then(|p| p.split('"').next()) {
                out.insert((module_of(&file), name.to_owned()));
            }
        }
    }
    out
}

/// `(module, method)` for every line of `postgres/pending/<module>.txt`, and
/// the modules whose file lists nothing. Git drops the directory once the
/// last file is deleted, so a missing directory lists nothing.
fn pending_listed() -> (BTreeSet<(String, String)>, Vec<String>) {
    let mut listed = BTreeSet::new();
    let mut empty = Vec::new();
    let entries = match std::fs::read_dir(pending_dir()) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (listed, empty),
        Err(e) => panic!("read postgres/pending: {e}"),
    };
    for entry in entries {
        let path = entry.expect("entry").path();
        if path.extension().is_none_or(|e| e != "txt") {
            continue;
        }
        let module = path
            .file_stem()
            .expect("stem")
            .to_string_lossy()
            .into_owned();
        let before = listed.len();
        for line in std::fs::read_to_string(&path).expect("read list").lines() {
            let line = line.trim();
            if !line.is_empty() && !line.starts_with('#') {
                listed.insert((module.clone(), line.to_owned()));
            }
        }
        if listed.len() == before {
            empty.push(module);
        }
    }
    (listed, empty)
}

fn list_entries<'a>(items: impl Iterator<Item = &'a (String, String)>) -> String {
    items
        .map(|(module, name)| format!("  postgres/pending/{module}.txt: {name}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn pending_postgres_methods_match_the_lists() {
    let pending = pending_in_source();
    let (listed, empty) = pending_listed();
    let unlisted: Vec<_> = pending.difference(&listed).collect();
    let ported: Vec<_> = listed.difference(&pending).collect();
    assert!(
        unlisted.is_empty(),
        "new pending_postgres! methods: give them a Postgres implementation (dispatch!) \
         or add each line below:\n{}",
        list_entries(unlisted.into_iter())
    );
    assert!(
        ported.is_empty(),
        "these lines name no pending_postgres! forwarder in that module; remove them:\n{}",
        list_entries(ported.into_iter())
    );
    assert!(
        empty.is_empty(),
        "these modules have no pending methods left; delete their file: {empty:?}"
    );
}

fn sqlite_only_in_source() -> BTreeSet<String> {
    let mut files = Vec::new();
    rust_files(&src(), &mut files);
    let mut out = BTreeSet::new();
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read");
        for part in text.split("sqlite_only!(").skip(1) {
            let part = part.strip_prefix("panic ").unwrap_or(part);
            if let Some(name) = part.strip_prefix('"').and_then(|p| p.split('"').next()) {
                out.insert(name.to_owned());
            }
        }
    }
    out
}

/// `name — reason` lines of `postgres/sqlite_only.txt`, and lines without a reason.
fn sqlite_only_listed() -> (BTreeSet<String>, Vec<String>) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("postgres/sqlite_only.txt");
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let mut listed = BTreeSet::new();
    let mut unexplained = Vec::new();
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        match line.split_once(" — ") {
            Some((name, reason)) if !reason.trim().is_empty() => {
                listed.insert(name.trim().to_owned());
            }
            _ => unexplained.push(line.to_owned()),
        }
    }
    (listed, unexplained)
}

/// Files (relative to `crates/`) that may call a SQLite-only method, with why
/// they only ever hold a `SQLite` store. `intent-store/src` outside
/// `postgres/` is the `SQLite` backend itself.
const SQLITE_ONLY_CALLERS: &[(&str, &str)] = &[
    (
        "intentd/src/main.rs",
        "opens the daemon store with Store::prepare_for_daemon, which is SQLite-only",
    ),
    (
        "intent-services/src/retention.rs",
        "its vacuum drain runs only from intentd's maintenance loop on the daemon store",
    ),
];

/// `sqlite_only!` methods any test code may call: the raw `SQLite` pools.
/// Their `Store` forwarders compile only under `TEST_GATE`, so the compiler,
/// not `SQLITE_ONLY_CALLERS`, keeps them out of production code
/// (`scripts/check-test-hooks-feature.sh` with `GUARDED_FEATURE=test-support`
/// keeps the feature out of the intentd release graph).
const TEST_ONLY_SQLITE_ONLY: &[&str] = &["inject_pending_delete", "read_pool", "write_pool"];

const TEST_GATE: &str = r#"#[cfg(any(test, feature = "test-support"))]"#;

/// Whether the function holding `sqlite_only!(panic "name"` or
/// `sqlite_only!("name"` in `text` carries `TEST_GATE` among the attributes
/// and doc comments right above its `fn`.
fn sqlite_only_forwarder_is_test_gated(text: &str, name: &str) -> bool {
    let at = [
        format!("sqlite_only!(panic \"{name}\""),
        format!("sqlite_only!(\"{name}\""),
    ]
    .iter()
    .find_map(|marker| text.find(marker.as_str()));
    let Some(at) = at else {
        return false;
    };
    let head = &text[..at];
    let Some(fn_at) = head.rfind("fn ") else {
        return false;
    };
    let line_start = head[..fn_at].rfind('\n').map_or(0, |i| i + 1);
    head[..line_start]
        .lines()
        .rev()
        .map(str::trim)
        .take_while(|line| line.starts_with("#[") || line.starts_with("///"))
        .any(|line| line == TEST_GATE)
}

fn may_call_sqlite_only(rel: &str) -> bool {
    (rel.starts_with("intent-store/src/") && !rel.starts_with("intent-store/src/postgres/"))
        || SQLITE_ONLY_CALLERS.iter().any(|(file, _)| *file == rel)
}

/// `file: <use>` for every method call (`.name(`), path call (`::name(`)
/// or path used as a function value (`::name,` / `::name)`) of a name in
/// `methods` in `text`.
fn sqlite_only_calls(rel: &str, text: &str, methods: &BTreeSet<String>) -> Vec<String> {
    let mut out = Vec::new();
    for m in methods {
        for shape in [
            format!(".{m}("),
            format!("::{m}("),
            format!("::{m},"),
            format!("::{m})"),
        ] {
            if text.contains(&shape) {
                out.push(format!("{rel}: {shape}"));
            }
        }
    }
    out
}

#[test]
fn sqlite_only_methods_match_the_list_and_stay_off_postgres() {
    let marked = sqlite_only_in_source();
    let (listed, unexplained) = sqlite_only_listed();
    assert!(
        unexplained.is_empty(),
        "postgres/sqlite_only.txt lines need `name — reason`: {unexplained:?}"
    );
    assert_eq!(
        marked, listed,
        "postgres/sqlite_only.txt must list exactly the sqlite_only! methods"
    );
    let pending: BTreeSet<String> = pending_in_source().into_iter().map(|(_, m)| m).collect();
    let both: Vec<_> = marked.intersection(&pending).collect();
    assert!(both.is_empty(), "both pending and SQLite-only: {both:?}");

    let mut sources = Vec::new();
    rust_files(&src(), &mut sources);
    let texts: Vec<String> = sources
        .iter()
        .map(|f| std::fs::read_to_string(f).expect("read"))
        .collect();
    for name in TEST_ONLY_SQLITE_ONLY {
        assert!(
            marked.contains(*name),
            "TEST_ONLY_SQLITE_ONLY names {name}, which is not a sqlite_only! method"
        );
        assert!(
            texts
                .iter()
                .any(|t| sqlite_only_forwarder_is_test_gated(t, name)),
            "the Store::{name} forwarder must carry {TEST_GATE}: TEST_ONLY_SQLITE_ONLY \
             methods skip the caller list because only test code can compile a call"
        );
    }
    let marked: BTreeSet<String> = marked
        .into_iter()
        .filter(|m| !TEST_ONLY_SQLITE_ONLY.contains(&m.as_str()))
        .collect();

    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&crates).expect("read crates") {
        let dir = entry.expect("entry").path();
        for sub in ["src", "tests", "benches"] {
            if dir.join(sub).is_dir() {
                rust_files(&dir.join(sub), &mut files);
            }
        }
    }
    let mut findings = Vec::new();
    for file in &files {
        let rel = file
            .strip_prefix(&crates)
            .expect("under crates")
            .to_string_lossy()
            .replace('\\', "/");
        if may_call_sqlite_only(&rel) {
            continue;
        }
        let text = std::fs::read_to_string(file).expect("read");
        findings.extend(sqlite_only_calls(&rel, &text, &marked));
    }
    assert!(
        findings.is_empty(),
        "these files call SQLite-only Store methods, which fail on Postgres. Give the \
         method a Postgres version, or add the file to SQLITE_ONLY_CALLERS with proof it \
         only holds a SQLite store:\n{}",
        findings.join("\n")
    );
}

#[test]
fn the_sqlite_only_rule_catches_new_callers() {
    let methods: BTreeSet<String> = ["optimize".to_owned()].into();
    assert!(may_call_sqlite_only("intent-store/src/lib.rs"));
    assert!(may_call_sqlite_only("intentd/src/main.rs"));
    assert!(!may_call_sqlite_only("intent-store/src/postgres/notes.rs"));
    assert!(!may_call_sqlite_only("intent-services/src/settings.rs"));
    assert!(sqlite_only_calls("x.rs", "store.optimize_plan().await", &methods).is_empty());
    // Built with concat! so this file does not itself call the method.
    assert_eq!(
        sqlite_only_calls("x.rs", concat!("store.", "optimize().await"), &methods),
        [concat!("x.rs: .", "optimize(")]
    );
    assert_eq!(
        sqlite_only_calls("x.rs", concat!("Store::", "optimize(&store)"), &methods),
        [concat!("x.rs: ::", "optimize(")]
    );
    assert_eq!(
        sqlite_only_calls("x.rs", concat!("run(Store::", "optimize, 3)"), &methods),
        [concat!("x.rs: ::", "optimize,")]
    );
    assert_eq!(
        sqlite_only_calls("x.rs", concat!("map(Store::", "optimize)"), &methods),
        [concat!("x.rs: ::", "optimize)")]
    );
    assert!(sqlite_only_calls("x.rs", "Store::optimize_plan(&store)", &methods).is_empty());
}

#[test]
fn the_test_gate_rule_needs_the_cfg_on_the_forwarder() {
    // Built with concat! so this file does not itself mark a method.
    let gated = concat!(
        "    /// Borrow it.\n",
        "    #[cfg(any(test, feature = \"test-support\"))]\n",
        "    #[must_use]\n",
        "    pub fn pool(&self) -> &P {\n",
        "        sqlite_only!(panic \"pool\", self, b => b.pool())\n",
        "    }\n",
    );
    assert!(sqlite_only_forwarder_is_test_gated(gated, "pool"));
    let ungated = gated.replace(TEST_GATE, "#[inline]");
    assert!(!sqlite_only_forwarder_is_test_gated(&ungated, "pool"));
    // A gate on an earlier item, separated by code, does not count.
    let elsewhere = concat!(
        "    #[cfg(any(test, feature = \"test-support\"))]\n",
        "    pub fn other(&self) {}\n",
        "\n",
        "    pub fn pool(&self) -> &P {\n",
        "        sqlite_only!(panic \"pool\", self, b => b.pool())\n",
        "    }\n",
    );
    assert!(!sqlite_only_forwarder_is_test_gated(elsewhere, "pool"));
    assert!(!sqlite_only_forwarder_is_test_gated(gated, "other"));
}

#[test]
fn writing_functions_come_from_the_migrations() {
    let writing = writing_functions();
    // Writes seed rows; a trigger function writes its side tables.
    assert!(writing.contains("intent_provision_tenant"), "{writing:?}");
    // Pure helpers do not write.
    for pure in ["intent_now_iso", "sqlite_json_extract"] {
        assert!(!writing.contains(pure), "{pure} does not write");
    }
    let sql = "create or replace function public.a() returns void language sql as $$ \
               select b(1) $$; create function b(x int) returns void language plpgsql as $$ \
               begin update t set y = 1; end $$; create function c() returns int as $$ \
               select 1 from updates $$;";
    let bodies = function_bodies(sql);
    let names: Vec<&str> = bodies.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["a", "b", "c"]);
    assert!(!body_writes(&bodies[0].1) && body_writes(&bodies[1].1) && !body_writes(&bodies[2].1));
    assert!(calls(&bodies[0].1, "b") && !calls(&bodies[2].1, "b"));
}

/// An allowlist key: `(file name, function)`.
type AllowKey = (String, String);

/// An allowlist file: the path under `src/postgres/` (`mod.rs` is the
/// top module only), else the file name.
fn allow_file(path: &Path) -> String {
    let text = path.to_string_lossy().replace('\\', "/");
    match text.rfind("src/postgres/") {
        Some(i) => text[i + "src/postgres/".len()..].to_owned(),
        None => path
            .file_name()
            .map_or_else(String::new, |f| f.to_string_lossy().into_owned()),
    }
}

/// Splits findings into those `allowed` names and the rest; returns the
/// rest and the keys used.
fn apply_allowed(
    findings: Vec<(PathBuf, String, String)>,
    allowed: &[(&str, &str, &str)],
) -> (Vec<String>, BTreeSet<AllowKey>) {
    let mut found = Vec::new();
    let mut used = BTreeSet::new();
    for (file, function, message) in findings {
        let key = (allow_file(&file), function);
        if allowed.iter().any(|(a, f, _)| *a == key.0 && *f == key.1) {
            used.insert(key);
        } else {
            found.push(message);
        }
    }
    (found, used)
}

/// The `allowed` entries no finding used, as messages.
fn stale_entries(allowed: &[(&str, &str, &str)], used: &BTreeSet<AllowKey>) -> Vec<String> {
    allowed
        .iter()
        .filter(|(a, f, _)| !used.contains(&((*a).to_owned(), (*f).to_owned())))
        .map(|(a, f, _)| format!("{a} {f}"))
        .collect()
}

/// Rule 5: a read of more than one statement takes one snapshot.
fn multi_statement_reads(
    index: &Index,
    allowed: &[(&str, &str, &str)],
) -> (Vec<String>, BTreeSet<AllowKey>) {
    let findings = index
        .multi_statement_reads()
        .into_iter()
        .map(|tx| {
            let (on, fix) = match tx.kind {
                TxKind::Begin => ("one plain begin()", "use begin_read_snapshot()"),
                TxKind::Connection => (
                    "one acquired connection, outside any transaction",
                    "use begin_read_snapshot() (a read) or begin_write() (a write), or list \
                     it in MULTI_READ_ALLOWED with a reason",
                ),
                TxKind::Pool => (
                    "the pool, each on its own connection",
                    "use begin_read_snapshot() (a read) or begin_write() (a write), or list \
                     it in MULTI_READ_ALLOWED with a reason (a retry loop of one statement)",
                ),
            };
            let message = format!(
                "{}:{}: {} runs {} statements on {on} ({}); {fix}",
                tx.file.display(),
                tx.line,
                tx.function,
                tx.sinks.len(),
                tx.sinks.join(", ")
            );
            (tx.file, tx.function, message)
        })
        .collect();
    apply_allowed(findings, allowed)
}

/// Rule 5: a plain `begin()` transaction stays where the lint can count
/// its statements.
fn begin_escapes(
    index: &Index,
    allowed: &[(&str, &str, &str)],
) -> (Vec<String>, BTreeSet<AllowKey>) {
    let findings = index
        .begin_escapes()
        .into_iter()
        .map(|e| {
            let message = format!(
                "{}:{}: {} {}; bind it with `let mut tx = self.begin_read_snapshot().await?` \
                 (a read) or `self.begin_write()` (a write) and pass helpers `&mut tx`",
                e.file.display(),
                e.line,
                e.function,
                e.what
            );
            (e.file, e.function, message)
        })
        .collect();
    apply_allowed(findings, allowed)
}

#[test]
fn every_multi_statement_read_takes_one_snapshot() {
    let (found, used) = multi_statement_reads(&postgres_index(), MULTI_READ_ALLOWED);
    assert!(
        found.is_empty(),
        "these Postgres reads run more than one statement without one snapshot:\n{}",
        found.join("\n")
    );
    let stale = stale_entries(MULTI_READ_ALLOWED, &used);
    assert!(
        stale.is_empty(),
        "MULTI_READ_ALLOWED names functions that run no multi-statement read; remove them:\n{}",
        stale.join("\n")
    );
}

#[test]
fn every_plain_begin_is_bound_where_its_statements_are_counted() {
    let (found, used) = begin_escapes(&postgres_index(), BEGIN_ESCAPE_ALLOWED);
    assert!(
        found.is_empty(),
        "these plain begin() transactions leave the lint's view:\n{}",
        found.join("\n")
    );
    let stale = stale_entries(BEGIN_ESCAPE_ALLOWED, &used);
    assert!(
        stale.is_empty(),
        "BEGIN_ESCAPE_ALLOWED names functions whose begin() does not escape; remove them:\n{}",
        stale.join("\n")
    );
}

/// `file function` of each finding over `sources`, and the allowlist keys
/// used.
type Rule5Check = fn(&Index, &[(&str, &str, &str)]) -> (Vec<String>, BTreeSet<AllowKey>);

fn rule5(
    check: Rule5Check,
    sources: &[(&str, &str)],
    allowed: &[(&str, &str, &str)],
) -> (Vec<String>, BTreeSet<AllowKey>) {
    let files: Vec<(PathBuf, String)> = sources
        .iter()
        .map(|(f, s)| (PathBuf::from(f), (*s).to_owned()))
        .collect();
    let (found, used) = check(&Index::new(&files, &[]), allowed);
    let names = found
        .into_iter()
        .map(|f| {
            let (file, rest) = f.split_once(':').unwrap_or(("", &f));
            let function = rest
                .split(": ")
                .nth(1)
                .and_then(|r| r.split(' ').next())
                .unwrap_or("");
            format!("{file} {function}")
        })
        .collect();
    (names, used)
}

fn reads(source: &str) -> Vec<String> {
    rule5(multi_statement_reads, &[("x.rs", source)], &[]).0
}

fn escapes(source: &str) -> Vec<String> {
    rule5(begin_escapes, &[("x.rs", source)], &[]).0
}

#[test]
fn multi_statement_plain_reads_need_one_snapshot() {
    let one = r#"async fn a(&self) { let mut tx = self.begin().await?;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await; }"#;
    let two = r#"async fn b(&self) { let mut tx = self.begin().await?;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await;
        sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(&mut *tx).await; }"#;
    let snapshot = r#"async fn c(&self) { let mut tx = self.begin_read_snapshot().await?;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await;
        sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(&mut *tx).await; }"#;
    let write = r#"async fn d(&self) { let mut tx = self.begin_write().await?;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await;
        sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(&mut *tx).await; }"#;
    // Helpers count toward the transaction their caller passes them, as a
    // method or a free function, through `map_err` on the begin().
    let helpers = r#"async fn e(&self) { let mut tx = self.begin().await.map_err(db)?;
        self.first(&mut tx).await; second(&mut tx).await; }
        async fn first(&self, conn: &mut PgConnection) {
            sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(conn).await; }
        async fn second(c: &mut PgConnection) {
            sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(&mut *c).await; }"#;
    // Two transactions of one statement each are two reads, not one.
    let separate = r#"async fn f(&self) {
        let mut tx = self.begin().await?;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await;
        let mut tx = self.begin().await?;
        sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(&mut *tx).await; }"#;
    for ok in [one, snapshot, write, separate] {
        assert_eq!(reads(ok), Vec::<String>::new(), "{ok}");
    }
    assert_eq!(reads(two), ["x.rs b"]);
    assert_eq!(reads(helpers), ["x.rs e"]);
    let allowed = [("x.rs", "b", "why")];
    assert!(rule5(multi_statement_reads, &[("x.rs", two)], &allowed)
        .0
        .is_empty());
}

#[test]
fn a_begin_is_followed_through_call_arguments() {
    let timeout = r#"async fn g(&self) {
        let mut tx = tokio::time::timeout(d, self.begin()).await.map_err(busy)??;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await;
        sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(&mut *tx).await; }"#;
    assert_eq!(reads(timeout), ["x.rs g"]);
    assert_eq!(escapes(timeout), Vec::<String>::new());
    let one = r#"async fn h(&self) {
        let mut tx = timeout(d, self.begin()).await??;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await; }"#;
    assert_eq!(reads(one), Vec::<String>::new());
}

#[test]
fn a_begin_the_lint_cannot_count_fails() {
    // The transaction reaches no `let`, or its binding moves somewhere the
    // lint does not follow: a struct, an Option, a return, a closure.
    let caught = [
        r"async fn wrapper(&self) -> Result<Tx> { self.begin().await }",
        r"async fn wrapped(&self) -> Result<Tx> { Ok(self.begin().await?) }",
        r"async fn optional(&self) { let tx = Some(self.begin().await?); }",
        r"async fn field(&self) { let h = Holder { tx: self.begin().await? }; }",
        r"async fn moved(&self) { let tx = self.begin().await?; let h = Holder { tx }; }",
        r"async fn some(&self) { let tx = self.begin().await?; let o = Some(tx); }",
        r"async fn returned(&self) -> Result<Tx> { let tx = self.begin().await?; Ok(tx) }",
        r"async fn pushed(&self) { let tx = self.begin().await?; v.push(tx); }",
        r#"async fn closure(&self) { let mut tx = self.begin().await?;
            let run = |c: &mut PgConnection| sqlx::query("SELECT 1").fetch_all(c);
            run(&mut tx).await; }"#,
        r"async fn handed(&self) { run(self.begin().await?).await; }",
        r"async fn alias(&self) { let tx = self.begin().await?; let c = tx; let o = Some(c); }",
    ];
    for source in caught {
        assert_eq!(escapes(source).len(), 1, "{source}");
    }
    // Bound by `let` (or an assignment), used as an executor, passed to a
    // helper the lint follows, committed, rolled back or dropped.
    let fine = r#"async fn fine(&self) {
        let mut tx = self.begin().await.map_err(db)?;
        sqlx::query("SELECT 1 FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await;
        self.helper(&mut tx).await; helper(tx.as_mut()).await;
        let c = &mut *tx; helper(c).await;
        tx.commit().await?;
        let mut tx = timeout(d, self.begin()).await??;
        tx.rollback().await?;
        let mut tx; tx = self.begin().await?; drop(tx); }
        async fn helper(&self, c: &mut PgConnection) {}
        async fn helper(c: &mut PgConnection) {}"#;
    assert_eq!(escapes(fine), Vec::<String>::new());
    // Not a plain begin().
    let other = r"async fn other(&self) { let tx = self.begin_write().await?; Ok(tx) }";
    assert_eq!(escapes(other), Vec::<String>::new());
    let allowed = [("x.rs", "wrapper", "why")];
    let (found, used) = rule5(begin_escapes, &[("x.rs", caught[0])], &allowed);
    assert!(found.is_empty());
    assert!(used.contains(&("x.rs".to_owned(), "wrapper".to_owned())));
}

#[test]
fn statements_outside_a_transaction_count_as_one_read() {
    let pool = r#"async fn p(&self) {
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&self.pool).await;
        sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(&self.pool).await; }"#;
    let pool_method = r#"async fn q(&self) { let pool = self.pool();
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(pool).await;
        sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(pool).await; }"#;
    let conn = r#"async fn r(&self) {
        let mut conn = self.pool.acquire().await.map_err(db)?;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *conn).await;
        sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(&mut *conn).await; }"#;
    let via_helpers = r#"async fn s(&self) { first(&self.pool).await; second(&self.pool).await; }
        async fn first(p: &PgPool) {
            sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(p).await; }
        async fn second(p: &PgPool) {
            sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(p).await; }"#;
    assert_eq!(reads(pool), ["x.rs p"]);
    assert_eq!(reads(pool_method), ["x.rs q"]);
    assert_eq!(reads(conn), ["x.rs r"]);
    assert_eq!(reads(via_helpers), ["x.rs s"]);
    let one_each = r#"async fn t(&self) {
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&self.pool).await;
        let mut conn = self.pool.acquire().await?;
        sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(&mut *conn).await;
        let mut tx = self.begin().await?;
        sqlx::query("SELECT z FROM v WHERE tenant_id = $1").fetch_all(&mut *tx).await; }"#;
    assert_eq!(reads(one_each), Vec::<String>::new());
}

#[test]
fn the_multi_read_allowlist_is_keyed_by_file_and_function() {
    let two = |f: &str| {
        format!(
            r#"async fn {f}(&self) {{ let mut tx = self.begin().await?;
            sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await;
            sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(&mut *tx).await; }}"#
        )
    };
    let (x, y) = (two("b"), two("b"));
    let sources = [("x.rs", x.as_str()), ("y.rs", y.as_str())];
    // An entry for one file does not cover the same name in another.
    let allowed = [("x.rs", "b", "why")];
    let (found, used) = rule5(multi_statement_reads, &sources, &allowed);
    assert_eq!(found, ["y.rs b"]);
    assert!(stale_entries(&allowed, &used).is_empty());
    // An entry whose file runs no such read is stale, even when another
    // file's function of that name is allowed.
    let allowed = [("x.rs", "b", "why"), ("z.rs", "b", "why")];
    let (_, used) = rule5(multi_statement_reads, &sources, &allowed);
    assert_eq!(stale_entries(&allowed, &used), ["z.rs b"]);
}

#[test]
fn a_statement_in_a_loop_runs_more_than_once() {
    let looped = r#"async fn l(&self, ids: &[i64]) { let mut tx = self.begin().await?;
        for id in ids {
            sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await; } }"#;
    let while_loop = r#"async fn w(&self) { let mut tx = self.begin().await?;
        while more {
            sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await; } }"#;
    let helper_in_loop = r#"async fn m(&self) { let mut tx = self.begin().await?;
        loop { self.page(&mut tx).await; } }
        async fn page(&self, c: &mut PgConnection) {
            sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(c).await; }"#;
    let helper_loops = r#"async fn n(&self) { let mut tx = self.begin().await?;
        self.pages(&mut tx).await; }
        async fn pages(&self, c: &mut PgConnection) { for _ in 0..2 {
            sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *c).await; } }"#;
    assert_eq!(reads(looped), ["x.rs l"]);
    assert_eq!(reads(while_loop), ["x.rs w"]);
    assert_eq!(reads(helper_in_loop), ["x.rs m"]);
    assert_eq!(reads(helper_loops), ["x.rs n"]);
    // A transaction opened in each pass runs one statement.
    let per_pass = r#"async fn o(&self) { loop { let mut tx = self.begin().await?;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await; } }"#;
    assert_eq!(reads(per_pass), Vec::<String>::new());
}

#[test]
fn review_shapes_of_rule5_are_caught() {
    let q = r#"sqlx::query("SELECT x FROM t WHERE tenant_id = $1")"#;
    let two = |head: &str, ex: &str| {
        format!(
            "async fn f(&self) {{ {head} {q}.fetch_all({ex}).await; {q}.fetch_all({ex}).await; }}"
        )
    };
    // `let .. else` (any pattern but a name) does not bind it: it fails.
    let let_else = two(
        "let Ok(mut tx) = self.begin().await else { return };",
        "&mut *tx",
    );
    assert_eq!(escapes(&let_else), ["x.rs f"]);
    // One helper called twice runs two statements.
    let twice = format!(
        "async fn f(&self) {{ let mut tx = self.begin().await?; h(&mut tx).await; h(&mut tx).await; }}
         async fn h(c: &mut PgConnection) {{ {q}.fetch_all(c).await; }}"
    );
    assert_eq!(reads(&twice), ["x.rs f"]);
    // A snapshot shadowing `tx` in an inner block does not hide the outer one.
    let shadowed = format!(
        "async fn f(&self) {{ let mut tx = self.begin().await?; {q}.fetch_all(&mut *tx).await;
         {{ let mut tx = self.begin_read_snapshot().await?; {q}.fetch_all(&mut *tx).await; }}
         {q}.fetch_all(&mut *tx).await; }}"
    );
    assert_eq!(reads(&shadowed), ["x.rs f"]);
    // A closure written before a loop and called in it.
    let closure_loop = format!(
        "async fn f(&self, v: Vec<i64>) {{ let mut tx = self.begin().await?;
         let mut run = || {q}.fetch_all(&mut *tx); for i in v {{ run().await; }} }}"
    );
    assert_eq!(reads(&closure_loop), ["x.rs f"]);
    // An assignment in an inner block keeps the outer binding's scope.
    let inner_assign = format!(
        "async fn f(&self) {{ let mut tx = self.begin_read_snapshot().await?;
         {{ tx = self.begin().await?; }}
         {q}.fetch_all(&mut *tx).await; {q}.fetch_all(&mut *tx).await; }}"
    );
    assert_eq!(reads(&inner_assign), ["x.rs f"]);
    // A declaration without a value scopes a later assignment to it.
    let deferred = format!(
        "async fn f(&self) {{ let mut tx; {{ tx = self.begin().await?; }}
         {q}.fetch_all(&mut *tx).await; {q}.fetch_all(&mut *tx).await; }}"
    );
    assert_eq!(reads(&deferred), ["x.rs f"]);
    // A `Self::` helper is followed like a method: returning the
    // transaction is an escape in the helper.
    let self_helper = format!(
        "impl S {{ async fn f(&self) {{ let tx = self.begin().await?;
         let mut tx = Self::identity(tx);
         {q}.fetch_all(&mut *tx).await; {q}.fetch_all(&mut *tx).await; }}
         fn identity(tx: Tx) -> Tx {{ tx }} }}"
    );
    assert_eq!(escapes(&self_helper), ["x.rs identity"]);
    // A pool-typed parameter no indexed caller passes is a pool itself.
    let pool_param = format!(
        "async fn f(pool: &PgPool) {{ {q}.fetch_all(pool).await; {q}.fetch_all(pool).await; }}"
    );
    assert_eq!(reads(&pool_param), ["x.rs f"]);
    // Pools by clone and by a `read_pool` field.
    assert_eq!(reads(&two("let p = self.pool.clone();", "&p")), ["x.rs f"]);
    assert_eq!(reads(&two("", "&self.read_pool")), ["x.rs f"]);
    // Moves by assignment, and through helpers that keep or return it.
    for source in [
        "async fn f(&mut self) { let tx = self.begin().await?; self.tx = tx; }",
        "async fn f(&self, slot: &mut Option<Tx>) { let tx = self.begin().await?; *slot = tx; }",
        "async fn f(&self) { let tx = self.begin().await?; let h = Holder::new(tx); }
         fn new(tx: Tx) -> Self { Self { tx } }",
        "async fn f(&self) { let tx = self.begin().await?; let mut tx = identity(tx); }
         fn identity(tx: Tx) -> Tx { tx }",
    ] {
        assert_eq!(escapes(source).len(), 1, "{source}");
    }
}

#[test]
fn rule5_messages_and_keys_fit_the_finding() {
    let q = r#"sqlx::query("INSERT INTO t (tenant_id) VALUES ($1)")"#;
    let pool = format!(
        "async fn f(&self) {{ {q}.execute(&self.pool).await; {q}.execute(&self.pool).await; }}"
    );
    let index = Index::new(&[(PathBuf::from("x.rs"), pool)], &[]);
    let (found, _) = multi_statement_reads(&index, &[]);
    assert!(found[0].contains("begin_write()"), "{found:?}");
    assert!(found[0].contains("MULTI_READ_ALLOWED"), "{found:?}");
    // Keys are paths under src/postgres, so `mod.rs` is the top module only.
    let read = r#"async fn b(&self) { let mut tx = self.begin().await?;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1").fetch_all(&mut *tx).await;
        sqlx::query("SELECT y FROM u WHERE tenant_id = $1").fetch_all(&mut *tx).await; }"#;
    let sources = [("src/postgres/sub/mod.rs", read)];
    let (found, _) = rule5(multi_statement_reads, &sources, &[("mod.rs", "b", "why")]);
    assert_eq!(found.len(), 1);
    let (found, used) = rule5(
        multi_statement_reads,
        &sources,
        &[("sub/mod.rs", "b", "why")],
    );
    assert!(found.is_empty());
    assert!(used.contains(&("sub/mod.rs".to_owned(), "b".to_owned())));
}
