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
//! Rules 1 and 2 find SQL by use (`support/sql_uses.rs`): the text that
//! reaches a sqlx call, followed through locals, consts and helpers. They
//! check it parsed by sqlparser (`support/postgres_ast.rs`), which sees CTE
//! scope, comments, strings and parenthesized joins as Postgres does. A
//! `format!` template is parsed with its `{CONST}` holes resolved from
//! `const` strings in `src/` and its other holes filled with text that
//! names no table. A statement that does not parse, or a sqlx argument the
//! lint cannot follow, fails unless `postgres/tenant-lint-unparsed.txt`
//! lists it with a reason.
//!
//! Limit: text appended at runtime (`push_str`, a `format!` argument) is
//! checked only where it reaches a sqlx call on its own.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[path = "support/postgres_ast.rs"]
mod postgres_ast;
#[path = "support/sql_uses.rs"]
mod sql_uses;
use sql_uses::{rust_files, Index, Sql};

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
    Index::new(&read(&postgres, None), &read(&src(), Some(&postgres))).with_rewriter("nul_free")
}

fn unparsed_list() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("postgres/tenant-lint-unparsed.txt")
}

/// The key a source is listed under: its file name and the first 80
/// characters of its text (or Rust expression) with whitespace collapsed.
fn unparsed_key(file: &Path, text: &str) -> String {
    let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    format!(
        "{} | {}",
        file.file_name().unwrap().to_string_lossy(),
        flat.chars().take(80).collect::<String>()
    )
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
    /// Texts that do not parse and sqlx arguments the lint cannot follow,
    /// not listed in `tenant-lint-unparsed.txt`.
    unparsed: Vec<String>,
    /// Functions that run a write without the writer lock.
    unlocked: BTreeSet<String>,
    /// The allowlist keys that matched.
    listed_used: BTreeSet<String>,
    parsed: usize,
}

fn check(index: &Index, listed: &BTreeMap<String, String>) -> Report {
    let exempt = non_tenant_tables();
    let mut report = Report::default();
    let mut seen = BTreeSet::new();
    for u in index.uses() {
        for s in &u.sources {
            let first = seen.insert((s.file.clone(), s.line, s.column));
            let at = format!("{}:{}", s.file.display(), s.line);
            let (text, parsed) = match &s.sql {
                Sql::Text { text, .. } => (text, postgres_ast::parse_pieces(&s.sql.pieces())),
                Sql::Unresolved(expr) => (expr, Err(String::new())),
            };
            let stmts = match parsed {
                Ok(stmts) => stmts,
                Err(e) => {
                    let key = unparsed_key(&s.file, text);
                    if listed.contains_key(&key) {
                        report.listed_used.insert(key);
                    } else if first {
                        let why = match &s.sql {
                            Sql::Text { .. } => format!("does not parse ({e})"),
                            Sql::Unresolved(_) => {
                                format!("runs SQL the lint cannot follow (`{text}`)")
                            }
                        };
                        report.unparsed.push(format!(
                            "{at}: {why}; fix it or list `{key} — <reason>` \
                             in postgres/tenant-lint-unparsed.txt"
                        ));
                    }
                    continue;
                }
            };
            if first {
                report.parsed += 1;
                for v in postgres_ast::violations(&stmts, &exempt) {
                    report
                        .tenant
                        .push(format!("{at}: {v} is not filtered by tenant_id = $n"));
                }
            }
            let writes = postgres_ast::written_tables(&stmts)
                .into_iter()
                .any(|t| !exempt.contains(&t.as_str()));
            if writes && !u.locked {
                report
                    .unlocked
                    .insert(format!("{}: {}", u.file.display(), u.function));
            }
        }
    }
    report
}

#[test]
fn every_postgres_statement_parses_and_is_filtered_by_the_bound_tenant() {
    let listed = unparsed_listed();
    let report = check(&postgres_index(), &listed);
    let mut findings = report.tenant;
    findings.extend(report.unparsed);
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
    let report = check(&postgres_index(), &unparsed_listed());
    assert!(
        report.unlocked.is_empty(),
        "these Postgres functions write without begin_write() or a WriteTx:\n{}",
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

/// [`check`] on one in-memory source file named `x.rs`.
fn check_source(source: &str) -> Report {
    let index = Index::new(&[(PathBuf::from("x.rs"), source.to_owned())], &[]);
    check(&index, &BTreeMap::new())
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
                Error::Internal(format!("update hook set failed: {what}"))
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
    // Unused SQL-looking strings (UNUSED, error text) are not statements.
    assert_eq!(texts_by_use(source), want);
    let report = check_source(source);
    assert_eq!(report.tenant, Vec::<String>::new());
    assert_eq!(report.parsed, 10);
    assert_eq!(report.unparsed.len(), 1, "{:?}", report.unparsed);
    assert!(report.unparsed[0].contains("cannot follow"));
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
    let tenant: Vec<&str> = report
        .tenant
        .iter()
        .map(|f| f.split_once(": ").unwrap().1)
        .collect();
    assert_eq!(
        tenant,
        [
            "FROM task is not filtered by tenant_id = $n",
            "FROM task is not filtered by tenant_id = $n",
            "FROM note is not filtered by tenant_id = $n",
        ]
    );
    // A listed statement passes, and its key counts as used.
    let index = Index::new(
        &[(
            PathBuf::from("x.rs"),
            r#"fn f() { sqlx::query("selec 1"); }"#.to_owned(),
        )],
        &[],
    );
    let key = unparsed_key(Path::new("x.rs"), "selec 1");
    let listed = BTreeMap::from([(key.clone(), "spike".to_owned())]);
    let report = check(&index, &listed);
    assert!(report.unparsed.is_empty());
    assert!(report.listed_used.contains(&key));
}

#[test]
fn the_writer_lock_rule_catches_unlocked_writes() {
    let unlocked = |source: &str| -> Vec<String> {
        check_source(source)
            .unlocked
            .into_iter()
            .map(|f| f.trim_start_matches("x.rs: ").to_owned())
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
        sqlx::query("INSERT INTO t (tenant_id) VALUES ($1)"); }"#;
    let shared = r#"async fn h(tx: &WriteTx) {
        sqlx::query("INSERT INTO t (tenant_id) VALUES ($1)"); }"#;
    let mentioned = r#"async fn f(&self) { // not a WriteTx
        sqlx::query("DELETE FROM t WHERE tenant_id = $1"); }"#;
    // A data-modifying CTE writes; so does SQL a helper runs for a caller.
    let cte = r#"async fn i(&self) { sqlx::query("WITH d AS (DELETE FROM t \
        WHERE tenant_id = $1 RETURNING id) SELECT id FROM d"); }"#;
    let helper = r#"async fn j(&self) { let tx = self.begin_write().await?;
        run(&mut tx, "DELETE FROM t WHERE tenant_id = $1").await; }
        async fn run(conn: &mut PgConnection, sql: &str) { sqlx::query(sql); }"#;
    // The migration ledger has no tenant to lock.
    let ledger = r#"async fn k(&self) {
        sqlx::query("INSERT INTO intent_schema_migrations (version) VALUES ($1)"); }"#;
    for ok in [locked, read, owned, bound, borrowed, ledger] {
        assert_eq!(unlocked(ok), Vec::<String>::new(), "{ok}");
    }
    assert_eq!(unlocked(begin), ["b"]);
    assert_eq!(unlocked(shared), ["h"]);
    assert_eq!(unlocked(mentioned), ["f"]);
    assert_eq!(unlocked(cte), ["i"]);
    assert_eq!(unlocked(helper), ["run"]);
    assert_eq!(unlocked(&format!("{locked}\n{read}\n{begin}")), ["b"]);
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
const TEST_ONLY_SQLITE_ONLY: &[&str] = &["read_pool", "write_pool"];

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
