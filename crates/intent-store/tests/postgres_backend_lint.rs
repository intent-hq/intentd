//! Source lint for the Postgres backend (`crates/intent-store/src/postgres/`).
//!
//! 1. Every SQL string literal there filters each table it reads or writes by
//!    a bound tenant parameter (`tenant_id = $n`) in the same query, and
//!    every INSERT sets `tenant_id`. One database holds every tenant, so a
//!    statement without the filter reads or changes other tenants' rows.
//! 2. Every function there that writes (an SQL literal starting with
//!    INSERT, UPDATE or DELETE) opens its transaction with `begin_write()`.
//!    The per-tenant writer lock orders only writers that take it, so one
//!    writer on plain `begin()` can commit between another's read and write.
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
//!    prove the same or the method needs a Postgres version.
//!
//! Rule 1 runs twice: on the text, and on the statement parsed by sqlparser
//! (`support/postgres_ast.rs`), which sees CTE scope, comments, strings and
//! parenthesized joins as Postgres does. Both must pass. A `format!`
//! template is parsed with its `{CONST}` holes resolved from `const` strings
//! in `src/` and its other holes filled with text that names no table. A
//! literal that looks like SQL but does not parse must be listed in
//! `postgres/tenant-lint-unparsed.txt` with a reason.
//!
//! Limit: SQL built at runtime from fragments is checked only where a
//! literal holds the table reference.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[path = "support/postgres_sql.rs"]
mod postgres_sql;
use postgres_sql::{same_level, table_refs};
#[path = "support/postgres_ast.rs"]
mod postgres_ast;
#[path = "support/sql_literals.rs"]
mod sql_literals;
use sql_literals::{is_sql, rust_files, string_literals, unescape};

fn src() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// Tables that belong to the schema rather than a tenant, with why.
const NON_TENANT_TABLES: &[(&str, &str)] = &[(
    "intent_schema_migrations",
    "applied-migration ledger written by postgres::migrate, one per schema",
)];

/// Every table the Postgres migrations create.
fn schema_tables() -> BTreeSet<String> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("postgres/migrations");
    let mut out = BTreeSet::new();
    for entry in std::fs::read_dir(&dir).expect("read postgres/migrations") {
        let sql = std::fs::read_to_string(entry.unwrap().path())
            .unwrap()
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

/// The names a marker comment declares as a query's own `WITH` subqueries:
/// `// tenant-lint: not-tables a b — <reason>`, on one of the three lines
/// above the literal's first line (`line` is 1-based). The tenant rule
/// skips those names in that query only, and each must be defined there
/// (`a as (`) and must not be a real table, so a marker cannot hide one.
/// Every real table in the query is still checked.
#[track_caller]
fn declared_subqueries(text: &str, line: usize, sql: &str) -> Vec<String> {
    let lines: Vec<&str> = text.lines().collect();
    for k in 1..=3 {
        let Some(i) = line.checked_sub(1 + k) else {
            break;
        };
        let Some(rest) = lines
            .get(i)
            .and_then(|l| l.trim().strip_prefix("// tenant-lint: not-tables "))
        else {
            continue;
        };
        let (names, reason) = rest
            .split_once(" — ")
            .expect("tenant-lint marker needs ` — <reason>`");
        assert!(
            !reason.trim().is_empty(),
            "tenant-lint marker needs a reason"
        );
        let names: Vec<String> = names.split_whitespace().map(str::to_lowercase).collect();
        let tables = schema_tables();
        for name in &names {
            assert!(
                !tables.contains(name),
                "tenant-lint marker names the real table `{name}`; rename the subquery"
            );
            assert!(
                sql.contains(&format!("{name} as (")),
                "tenant-lint marker names `{name}`, which this query does not define"
            );
        }
        return names;
    }
    Vec::new()
}

/// The violations in one SQL literal (lower-cased).
fn violations(sql: &str) -> Vec<String> {
    violations_skipping(sql, &[])
}

/// [`violations`], treating the `skip` names as the query's own subqueries.
fn violations_skipping(sql: &str, skip: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for r in table_refs(sql) {
        if NON_TENANT_TABLES.iter().any(|(t, _)| *t == r.table) || skip.contains(&r.table) {
            continue;
        }
        let ok = if r.keyword == "INTO" {
            let after_insert = sql[..r.at].trim_end().ends_with("insert");
            // The column list right after the table must start with tenant_id.
            !after_insert
                || sql[r.at..]
                    .split_once('(')
                    .is_some_and(|(_, cols)| cols.trim_start().starts_with("tenant_id"))
        } else {
            // The filter must be in the WHERE (or a JOIN's ON) part of the
            // same query, not in an UPDATE's SET list.
            let level = same_level(sql, r.at);
            let filter = level.split_once(" where ").map_or("", |(_, w)| w);
            let qualified = format!("{}.tenant_id = $", r.alias);
            filter.contains(&qualified)
                || filter.contains("tenant_id = $")
                || (r.keyword == "JOIN" && level.contains(&qualified))
        };
        if !ok {
            out.push(format!("{} {}", r.keyword, r.table));
        }
    }
    out
}

#[test]
fn every_postgres_query_is_filtered_by_the_bound_tenant() {
    let mut files = Vec::new();
    rust_files(&src().join("postgres"), &mut files);
    let mut findings = Vec::new();
    let mut checked = 0;
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read");
        for (line, lit) in string_literals(&text) {
            if !is_sql(&lit) {
                continue;
            }
            checked += 1;
            let sql = lit.to_lowercase();
            let skip = declared_subqueries(&text, line, &sql);
            for v in violations_skipping(&sql, &skip) {
                findings.push(format!(
                    "{}:{line}: {v} is not filtered by tenant_id = $n",
                    file.display()
                ));
            }
        }
    }
    assert!(checked > 0, "found no SQL in src/postgres");
    assert!(
        findings.is_empty(),
        "unscoped Postgres queries:\n{}",
        findings.join("\n")
    );
}

#[test]
fn the_tenant_rule_catches_unscoped_shapes() {
    assert!(is_sql("UPDATE hook SET state = $3 WHERE hook_id = $2"));
    assert!(is_sql("update hook\nset state = $3 where hook_id = $2"));
    assert!(!is_sql("update hook state failed: {e}"));
    assert!(violations("select value from settings where tenant_id = $1 and key = $2").is_empty());
    assert!(!violations("select value from settings where key = $1").is_empty());
    assert!(!violations(
        "update note set x = 1 where id in (select id from note where tenant_id = $1)"
    )
    .is_empty());
    assert!(violations("insert into settings (tenant_id, key) values ($1, $2)").is_empty());
    assert!(!violations("insert into settings (key, tenant_id) values ($1, $2)").is_empty());
    assert!(violations("delete from settings where tenant_id = $1 and key = $2").is_empty());
    // An ON CONFLICT target or a SET list does not stand in for the filter.
    assert!(!violations(
        "insert into settings (key, value) values ($1, $2) on conflict (tenant_id, key) do nothing"
    )
    .is_empty());
    assert!(!violations("update settings set tenant_id = $1 where key = $2").is_empty());
    assert!(
        violations("update settings set value = $3 where tenant_id = $1 and key = $2").is_empty()
    );
    // A table right before a closing parenthesis is still a table.
    assert_eq!(
        violations("select id from task where tenant_id = $1 and id in (select id from note)"),
        ["FROM note"]
    );
    assert!(violations(
        "select id from task where tenant_id = $1 \
         and id in (select id from note where tenant_id = $1)"
    )
    .is_empty());
    // FROM inside a function call's arguments names no table.
    assert!(violations(
        "select extract(epoch from created_at), substring(name from $2) \
         from note where tenant_id = $1"
    )
    .is_empty());
    // A join inside grouping parentheses, a `(with` subquery and a
    // parenthesis in a quoted string all leave their tables checked.
    assert!(
        violations("select n.id from (note n join task t on t.id = n.id) where n.id = $1")
            .contains(&"JOIN task".to_owned())
    );
    assert!(violations(
        "select id from task where tenant_id = $1 \
         and id in (with c as (select id from note) select id from c)"
    )
    .contains(&"FROM note".to_owned()));
    assert_eq!(
        violations("select '(' || name from note where id = $1"),
        ["FROM note"]
    );
    assert_eq!(
        violations("select name -- count(\nfrom note where id = $1"),
        ["FROM note"]
    );
    // A SELECT-led group is a subquery whatever word precedes it.
    assert_eq!(
        violations("select distinct (select id from note) from task where tenant_id = $1"),
        ["FROM note"]
    );
    // A marker skips only the names it declares; real tables stay checked.
    let declared = ["c".to_owned()];
    assert!(violations_skipping(
        "with c as (select id from note where tenant_id = $1) select id from c",
        &declared
    )
    .is_empty());
    assert_eq!(
        violations_skipping(
            "with c as (select id from note where id = $2) select id from c",
            &declared
        ),
        ["FROM note"]
    );
    let text = "x\n// tenant-lint: not-tables c — own subquery\nlet q = f(\n\"with c as (...";
    assert_eq!(
        declared_subqueries(text, 4, "with c as (select 1) select * from c"),
        ["c"]
    );
    assert!(declared_subqueries("a\nb\nc\nd", 4, "select 1").is_empty());
}

#[test]
#[should_panic(expected = "names the real table `note`")]
fn a_marker_cannot_name_a_real_table() {
    // A subquery named after a table would hide that table's own reads.
    let text = "x\n// tenant-lint: not-tables note — own subquery\nlet q = f(\n\"with note...";
    declared_subqueries(
        text,
        4,
        "with note as (select id from note n) select id from note",
    );
}

/// `const NAME: &str = "..."` values in `text`. A const set to another
/// const's path (`crate::agent_repo::X`) maps to that const's name, resolved
/// later.
fn str_consts(text: &str) -> BTreeMap<String, Result<String, String>> {
    let mut out = BTreeMap::new();
    for (at, _) in text.match_indices("const ") {
        let rest = &text[at + "const ".len()..];
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
            .collect();
        let Some(value) = rest[name.len()..]
            .trim_start()
            .strip_prefix(": &str =")
            .map(str::trim_start)
        else {
            continue;
        };
        if name.is_empty() {
            continue;
        }
        if value.starts_with('"') {
            if let Some((_, lit)) = string_literals(value).into_iter().next() {
                out.insert(name, Ok(unescape(&lit)));
            }
        } else {
            let path: String = value
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
                .collect();
            let target = path.rsplit("::").next().unwrap_or_default().to_owned();
            out.insert(name, Err(target));
        }
    }
    out
}

/// Every `&str` const in `src/`, per file and by name across the crate.
struct Consts {
    by_file: BTreeMap<PathBuf, BTreeMap<String, Result<String, String>>>,
    by_name: BTreeMap<String, Vec<String>>,
}

impl Consts {
    fn load() -> Self {
        let mut files = Vec::new();
        rust_files(&src(), &mut files);
        let mut by_file = BTreeMap::new();
        let mut by_name: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for file in files {
            let consts = str_consts(&std::fs::read_to_string(&file).expect("read"));
            for (name, value) in &consts {
                if let Ok(v) = value {
                    by_name.entry(name.clone()).or_default().push(v.clone());
                }
            }
            by_file.insert(file, consts);
        }
        Self { by_file, by_name }
    }

    /// `name` in `file`, else the one crate-wide const of that name.
    fn resolve(&self, file: &Path, name: &str) -> Option<String> {
        let global = |n: &str| match self.by_name.get(n).map(Vec::as_slice) {
            Some([one]) => Some(one.clone()),
            _ => None,
        };
        match self.by_file.get(file).and_then(|c| c.get(name)) {
            Some(Ok(v)) => Some(v.clone()),
            Some(Err(target)) => global(target),
            None => global(name),
        }
    }
}

/// Short prose with no SQL punctuation, such as an error message
/// (`"delete note failed"`) that [`is_sql`] takes for a statement.
fn looks_like_prose(sql: &str) -> bool {
    sql.split_whitespace().count() <= 8 && !sql.contains(['$', '=', '(', '*', ',', '\''])
}

fn unparsed_list() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("postgres/tenant-lint-unparsed.txt")
}

/// The key a literal is listed under: its file name and the first 80
/// characters of its text with whitespace collapsed.
fn unparsed_key(file: &Path, sql: &str) -> String {
    let flat: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    format!(
        "{} | {}",
        file.file_name().unwrap().to_string_lossy(),
        flat.chars().take(80).collect::<String>()
    )
}

/// `postgres/tenant-lint-unparsed.txt`: `<file> | <sql prefix> — <reason>`
/// per line; `#` lines are comments.
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

/// The AST rule's violations in one SQL text (unit tests).
fn ast_violations(sql: &str) -> Vec<String> {
    let stmts = postgres_ast::parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    postgres_ast::violations(&stmts, &non_tenant_tables())
}

fn non_tenant_tables() -> Vec<&'static str> {
    NON_TENANT_TABLES.iter().map(|(t, _)| *t).collect()
}

#[test]
fn every_postgres_query_passes_the_tenant_rule_on_its_parsed_form() {
    let consts = Consts::load();
    let listed = unparsed_listed();
    let mut used = BTreeSet::new();
    let mut files = Vec::new();
    rust_files(&src().join("postgres"), &mut files);
    files.sort();
    let mut findings = Vec::new();
    let mut parsed = 0;
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read");
        for (line, lit) in string_literals(&text) {
            if !is_sql(&lit) {
                continue;
            }
            let sql = unescape(&lit);
            let resolve = |name: &str| consts.resolve(&file, name);
            match postgres_ast::parse_template(&sql, &resolve) {
                Ok(stmts) => {
                    parsed += 1;
                    for v in postgres_ast::violations(&stmts, &non_tenant_tables()) {
                        findings.push(format!(
                            "{}:{line}: {v} is not filtered by tenant_id = $n",
                            file.display()
                        ));
                    }
                }
                Err(e) => {
                    let key = unparsed_key(&file, &sql);
                    if listed.contains_key(&key) {
                        used.insert(key);
                    } else if !looks_like_prose(&sql) {
                        findings.push(format!(
                            "{}:{line}: does not parse ({e}); fix it or list `{key} — <reason>` \
                             in postgres/tenant-lint-unparsed.txt",
                            file.display()
                        ));
                    }
                }
            }
        }
    }
    for key in listed.keys().filter(|k| !used.contains(*k)) {
        findings.push(format!(
            "postgres/tenant-lint-unparsed.txt: `{key}` matches no unparsed literal; remove it"
        ));
    }
    assert!(parsed > 0, "parsed no SQL in src/postgres");
    assert!(
        findings.is_empty(),
        "tenant rule on parsed Postgres SQL:\n{}",
        findings.join("\n")
    );
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
    let stmts = postgres_ast::parse_template("select {COLS} from note where tenant_id = $1", &cols)
        .expect("parses");
    assert_eq!(
        stmts[0].to_string(),
        "SELECT id, name FROM note WHERE tenant_id = $1"
    );
    // A resolved const is checked in place: its tables count.
    let live = |n: &str| (n == "LIVE").then(|| "exists(select 1 from workspace)".to_owned());
    let stmts =
        postgres_ast::parse_template("select id from note where tenant_id = $1 and {LIVE}", &live)
            .expect("parses");
    assert_eq!(postgres_ast::violations(&stmts, &[]), ["FROM workspace"]);
    for sql in [
        "select id from browser_tab where tenant_id = $1 and {filter} = $2 {ORDER}",
        "select * from ({ranked}) r where true{after} limit $11",
        "update note set title = $4, {content}tags = $7 where tenant_id = $1",
        "insert into script (tenant_id, id) values {}",
        "update workspace set {column} = $3 where tenant_id = $1 and id = $2",
        "select id from m where tenant_id = $1 and m.sequence {comparison} $6 order by m.sequence {order}",
    ] {
        assert!(postgres_ast::parse_template(sql, &none).is_ok(), "{sql}");
    }
    assert!(postgres_ast::parse_template("delete note failed: {e}", &none).is_err());
    assert!(looks_like_prose("delete note failed: {e}"));
    assert!(!looks_like_prose(
        "select id from note where tenant_id = $1"
    ));
}

/// The functions in `text` that write without the tenant's writer lock:
/// their text (up to the next `fn`) holds an INSERT, UPDATE or DELETE
/// literal but no `begin_write()` call.
fn writers_without_lock(text: &str) -> Vec<String> {
    let starts: Vec<usize> = text
        .match_indices("fn ")
        .map(|(i, _)| i)
        .filter(|&i| i == 0 || text.as_bytes()[i - 1].is_ascii_whitespace())
        .collect();
    let mut out = Vec::new();
    for (k, &start) in starts.iter().enumerate() {
        let chunk = &text[start..starts.get(k + 1).copied().unwrap_or(text.len())];
        // Schema-level tables (the migration ledger) have no tenant to lock.
        let writes = string_literals(chunk).iter().any(|(_, lit)| {
            let l = lit.trim_start().to_lowercase();
            ["insert ", "update ", "delete "]
                .iter()
                .any(|w| l.starts_with(w))
                && !NON_TENANT_TABLES.iter().any(|(t, _)| l.contains(t))
        });
        // `&mut WriteTx` can only come from begin_write().
        let locked = chunk.contains("begin_write()") || chunk.contains("&mut WriteTx");
        if writes && !locked {
            let name = chunk[3..]
                .split(|c: char| !(c.is_alphanumeric() || c == '_'))
                .next()
                .unwrap_or_default();
            out.push(name.to_owned());
        }
    }
    out
}

#[test]
fn every_postgres_write_takes_the_tenant_writer_lock() {
    let mut files = Vec::new();
    rust_files(&src().join("postgres"), &mut files);
    let findings: Vec<String> = files
        .iter()
        .flat_map(|file| {
            let text = std::fs::read_to_string(file).expect("read");
            writers_without_lock(&text)
                .into_iter()
                .map(move |name| format!("{}: {name}", file.display()))
        })
        .collect();
    assert!(
        findings.is_empty(),
        "these Postgres functions write without begin_write():\n{}",
        findings.join("\n")
    );
}

#[test]
fn the_writer_lock_rule_catches_unlocked_writes() {
    let locked = r#"async fn a(&self) { let tx = self.begin_write().await?;
        sqlx::query("UPDATE t SET x = 1 WHERE tenant_id = $1"); }"#;
    let unlocked = r#"async fn b(&self) { let tx = self.begin().await?;
        sqlx::query("DELETE FROM t WHERE tenant_id = $1"); }"#;
    let read = r#"async fn c(&self) { let tx = self.begin().await?;
        sqlx::query("SELECT x FROM t WHERE tenant_id = $1"); }"#;
    assert!(writers_without_lock(locked).is_empty());
    assert!(writers_without_lock(read).is_empty());
    assert_eq!(writers_without_lock(unlocked), ["b"]);
    assert_eq!(
        writers_without_lock(&format!("{locked}\n{read}\n{unlocked}")),
        ["b"]
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
