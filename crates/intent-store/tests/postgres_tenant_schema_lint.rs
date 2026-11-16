//! Schema lint for the multi-tenant Postgres migrations in
//! `crates/intent-store/postgres/migrations/`.
//!
//! A Postgres store handle serves exactly one tenant, and tenants share one
//! database, so every row, key and trigger statement must carry `tenant_id`:
//! - every table starts with `tenant_id text NOT NULL`; it leads the primary
//!   key and every unique constraint;
//! - every index leads with `tenant_id` (listed GIN search indexes excepted);
//! - every foreign key pairs `tenant_id` on both sides, so cascades stay
//!   inside one tenant;
//! - every trigger statement filters each table it reads or writes by the
//!   triggering row's tenant in the same query (not only in a subquery), and
//!   every trigger INSERT sets `tenant_id`; tenant provisioning functions do
//!   the same with their `p_tenant` parameter.
//!
//! It also pairs migrations: each `SQLite` migration after the baseline needs
//! a Postgres migration whose header names it (`-- sqlite: <file>`), or an
//! entry with a reason in `postgres/sqlite-noop.txt`.
//!
//! Limit: a text-level check of the generated DDL shapes, not a SQL parser.
//! A table reference spelled through a CTE or a view is not seen.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

#[path = "support/postgres_sql.rs"]
mod postgres_sql;
use postgres_sql::{same_level, table_refs};

/// Last `SQLite` migration folded into the Postgres baseline (`0002_tables.sql`).
const SQLITE_BASELINE_LAST: u32 = 196;

/// GIN full-text indexes cannot lead with a text column without `btree_gin`;
/// every query on them filters `tenant_id` from the btree primary key instead.
const GIN_EXCEPTIONS: &[&str] = &["agent_message_search_tsv", "note_search_tsv"];

fn store_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf()
}

fn migrations() -> Vec<(String, String)> {
    let dir = store_dir().join("postgres/migrations");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("read postgres/migrations")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|p| {
            let name = p.file_name().unwrap().to_string_lossy().into_owned();
            (name, std::fs::read_to_string(&p).expect("read migration"))
        })
        .collect()
}

/// Strip `--` comments so prose never satisfies or trips a rule.
fn without_comments(sql: &str) -> String {
    sql.lines()
        .map(|l| l.split("--").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Text from `open` (an index of `(`) to its matching `)`, exclusive.
#[track_caller]
fn paren_body(s: &str, open: usize) -> &str {
    let mut depth = 0usize;
    for (i, c) in s[open..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return &s[open + 1..open + i];
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced parentheses after byte {open}");
}

fn word_after<'a>(s: &'a str, keyword: &str) -> Option<&'a str> {
    let rest = s.trim_start().strip_prefix(keyword)?;
    rest.split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .find(|w| !w.is_empty())
}

#[derive(Default)]
struct Findings(Vec<String>);

impl Findings {
    fn fail(&mut self, file: &str, what: &str) {
        self.0.push(format!("{file}: {what}"));
    }
}

fn check_tables(file: &str, sql: &str, out: &mut Findings) -> usize {
    let mut count = 0;
    let upper = sql.to_uppercase();
    let mut from = 0;
    while let Some(pos) = upper[from..].find("CREATE TABLE ") {
        let start = from + pos;
        let name = word_after(&sql[start..], "CREATE TABLE")
            .unwrap_or("?")
            .to_owned();
        let open = start + sql[start..].find('(').expect("table body");
        let body = paren_body(sql, open);
        count += 1;
        let first = body.trim_start().lines().next().unwrap_or("").trim();
        if !first.starts_with("tenant_id text NOT NULL") {
            out.fail(
                file,
                &format!("table {name}: first column must be `tenant_id text NOT NULL`"),
            );
        }
        if !body.contains("PRIMARY KEY (tenant_id,") {
            out.fail(
                file,
                &format!("table {name}: primary key must lead with tenant_id"),
            );
        }
        for (i, _) in body.match_indices("UNIQUE (") {
            if !body[i..].starts_with("UNIQUE (tenant_id,") {
                out.fail(
                    file,
                    &format!("table {name}: unique constraint must lead with tenant_id"),
                );
            }
        }
        from = open + body.len();
    }
    count
}

fn check_indexes(file: &str, sql: &str, out: &mut Findings) -> usize {
    let mut count = 0;
    for stmt in sql.split(';') {
        let s = stmt.trim();
        let upper = s.to_uppercase();
        if !(upper.starts_with("CREATE INDEX") || upper.starts_with("CREATE UNIQUE INDEX")) {
            continue;
        }
        count += 1;
        let name = s
            .split_whitespace()
            .skip_while(|w| !w.eq_ignore_ascii_case("INDEX"))
            .nth(1)
            .unwrap_or("?");
        let on = upper.find(" ON ").expect("index ON");
        let open = on + s[on..].find('(').expect("index columns");
        let cols = paren_body(s, open).trim_start();
        if upper.contains("USING GIN") && GIN_EXCEPTIONS.contains(&name) {
            continue;
        }
        if !cols.starts_with("tenant_id,") {
            out.fail(
                file,
                &format!("index {name}: columns must lead with tenant_id"),
            );
        }
    }
    count
}

fn check_foreign_keys(file: &str, sql: &str, out: &mut Findings) -> usize {
    let mut count = 0;
    for (i, _) in sql.match_indices("FOREIGN KEY (") {
        count += 1;
        let rest = &sql[i..];
        let stmt = &rest[..rest.find(';').unwrap_or(rest.len())];
        let refs = stmt.find("REFERENCES").map(|r| &stmt[r..]);
        let paired = stmt.starts_with("FOREIGN KEY (tenant_id,")
            && refs.is_some_and(|r| r.contains("(tenant_id,"));
        if !paired {
            out.fail(
                file,
                &format!(
                    "foreign key must pair tenant_id on both sides: {}",
                    stmt.lines().next().unwrap_or("")
                ),
            );
        }
    }
    count
}

/// The expression a function's statements must compare `tenant_id` with:
/// the triggering row for `trg_*`, the `p_tenant` parameter otherwise.
fn tenant_of(name: &str, body: &str) -> String {
    let lower = body.to_lowercase();
    if !name.starts_with("trg_") {
        return "p_tenant".to_owned();
    }
    if lower.contains("old.tenant_id") && !lower.contains("new.tenant_id") {
        "old.tenant_id".to_owned()
    } else {
        "new.tenant_id".to_owned()
    }
}

fn check_functions(file: &str, sql: &str, out: &mut Findings) -> usize {
    let mut count = 0;
    let mut rest = sql;
    while let Some(pos) = rest.find("CREATE FUNCTION ") {
        let after = &rest[pos..];
        let name = word_after(after, "CREATE FUNCTION")
            .unwrap_or("?")
            .to_owned();
        let body_start = after.find("$$").expect("function body") + 2;
        let body_len = after[body_start..].find("$$").expect("function body end");
        let body = &after[body_start..body_start + body_len];
        rest = &after[body_start + body_len + 2..];
        if !name.starts_with("trg_") && !body.contains("p_tenant") {
            continue; // compat helpers touch no tenant table
        }
        count += 1;
        let tenant = tenant_of(&name, body);
        for stmt in body.split(';') {
            let lower = stmt.to_lowercase();
            for r in table_refs(&lower) {
                let ok = if r.keyword == "INTO" {
                    let after_insert = lower[..r.at].trim_end().ends_with("insert");
                    !after_insert
                        || lower
                            .find(&format!("into {} (", r.table))
                            .is_some_and(|i| lower[i..].contains("(tenant_id,"))
                } else {
                    same_level(&lower, r.at).contains(&format!("{}.tenant_id = {tenant}", r.alias))
                };
                if !ok {
                    out.fail(
                        file,
                        &format!(
                            "{name}: {} {} is not filtered by {tenant} in the same query",
                            r.keyword, r.table
                        ),
                    );
                }
            }
        }
    }
    count
}

#[test]
fn every_postgres_object_is_tenant_scoped() {
    let mut out = Findings::default();
    let (mut tables, mut indexes, mut fks, mut triggers) = (0, 0, 0, 0);
    for (file, text) in migrations() {
        let sql = without_comments(&text);
        tables += check_tables(&file, &sql, &mut out);
        indexes += check_indexes(&file, &sql, &mut out);
        fks += check_foreign_keys(&file, &sql, &mut out);
        triggers += check_functions(&file, &sql, &mut out);
    }
    assert!(tables > 50 && indexes > 50 && fks > 50 && triggers > 50, "scanned too little: {tables} tables, {indexes} indexes, {fks} foreign keys, {triggers} trigger functions");
    assert!(
        out.0.is_empty(),
        "tenant scoping violations:\n{}",
        out.0.join("\n")
    );
}

#[test]
fn every_sqlite_migration_after_the_baseline_has_a_postgres_counterpart() {
    let sqlite: BTreeSet<String> = std::fs::read_dir(store_dir().join("migrations"))
        .expect("read migrations")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .filter(|n| {
            n.split('_')
                .next()
                .and_then(|v| v.parse::<u32>().ok())
                .is_some_and(|v| v > SQLITE_BASELINE_LAST)
        })
        .collect();
    let mut covered: BTreeSet<String> = migrations()
        .iter()
        .flat_map(|(_, text)| {
            text.lines()
                .filter_map(|l| l.trim().strip_prefix("-- sqlite: "))
                .map(|s| s.trim().to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    let noop = store_dir().join("postgres/sqlite-noop.txt");
    if let Ok(text) = std::fs::read_to_string(noop) {
        for line in text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
        {
            let (file, reason) = line.split_once(' ').unwrap_or((line, ""));
            assert!(
                !reason.trim().is_empty(),
                "sqlite-noop.txt: {file} needs a reason"
            );
            covered.insert(file.to_owned());
        }
    }
    let missing: Vec<_> = sqlite.difference(&covered).collect();
    assert!(
        missing.is_empty(),
        "SQLite migrations without a Postgres counterpart (add a postgres/migrations file \
         with `-- sqlite: <file>`, or a reasoned line in postgres/sqlite-noop.txt): {missing:?}"
    );
}
