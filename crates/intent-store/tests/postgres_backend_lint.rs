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
//! 3. `postgres/pending-methods.txt` lists exactly the `Store` methods that
//!    still go through `pending_postgres!` (no Postgres implementation yet).
//!    Porting a method removes its line; a new `Store` method should ship
//!    with its Postgres version (`dispatch!`) or be listed here on purpose.
//!
//! Limit: text-level. SQL built at runtime from fragments is checked only
//! where a literal holds the table reference.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

#[path = "support/postgres_sql.rs"]
mod postgres_sql;
use postgres_sql::{same_level, table_refs};

fn src() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The contents of every `"..."` literal (escapes kept, `\` line
/// continuations joined) with its line number.
fn string_literals(text: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut line = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\n' => line += 1,
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'"' => {
                let start_line = line;
                let mut lit = String::new();
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' && bytes.get(i + 1) == Some(&b'\n') {
                        i += 2;
                        line += 1;
                        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                            if bytes[i] == b'\n' {
                                line += 1;
                            }
                            i += 1;
                        }
                        lit.push(' ');
                        continue;
                    }
                    if bytes[i] == b'\\' {
                        lit.push(char::from(bytes[i]));
                        i += 1;
                    }
                    if bytes[i] == b'\n' {
                        line += 1;
                    }
                    lit.push(char::from(bytes[i]));
                    i += 1;
                }
                out.push((start_line, lit));
            }
            _ => {}
        }
        i += 1;
    }
    out
}

fn is_sql(lit: &str) -> bool {
    let l = lit.trim_start().to_lowercase();
    ["select ", "insert ", "update ", "delete ", "with "]
        .iter()
        .any(|k| l.starts_with(k))
}

/// Tables that belong to the schema rather than a tenant, with why.
const NON_TENANT_TABLES: &[(&str, &str)] = &[(
    "intent_schema_migrations",
    "applied-migration ledger written by postgres::migrate, one per schema",
)];

/// The violations in one SQL literal (lower-cased).
fn violations(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    for r in table_refs(sql) {
        if NON_TENANT_TABLES.iter().any(|(t, _)| *t == r.table) {
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
            for v in violations(&lit.to_lowercase()) {
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

fn pending_in_source() -> BTreeSet<String> {
    let mut files = Vec::new();
    rust_files(&src(), &mut files);
    let mut out = BTreeSet::new();
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read");
        for part in text.split("pending_postgres!(").skip(1) {
            let part = part.strip_prefix("panic ").unwrap_or(part);
            if let Some(name) = part.strip_prefix('"').and_then(|p| p.split('"').next()) {
                out.insert(name.to_owned());
            }
        }
    }
    out
}

#[test]
fn pending_postgres_methods_match_the_list() {
    let list_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("postgres/pending-methods.txt");
    let listed: BTreeSet<String> = std::fs::read_to_string(&list_path)
        .expect("read postgres/pending-methods.txt")
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_owned)
        .collect();
    let pending = pending_in_source();
    let unlisted: Vec<_> = pending.difference(&listed).collect();
    let ported: Vec<_> = listed.difference(&pending).collect();
    assert!(
        unlisted.is_empty(),
        "new pending_postgres! methods: give them a Postgres implementation (dispatch!) \
         or add them to postgres/pending-methods.txt: {unlisted:?}"
    );
    assert!(
        ported.is_empty(),
        "these methods no longer use pending_postgres!; remove them from \
         postgres/pending-methods.txt: {ported:?}"
    );
}
