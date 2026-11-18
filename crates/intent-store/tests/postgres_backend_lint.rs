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

/// Whether a literal is a SQL statement. An UPDATE needs its SET clause, so
/// error text such as `"update hook state failed: {e}"` (the `SQLite`
/// wording) is not mistaken for a statement.
fn is_sql(lit: &str) -> bool {
    let l = lit.trim_start().to_lowercase();
    if l.starts_with("update ") {
        return l.split_whitespace().any(|w| w == "set");
    }
    ["select ", "insert ", "delete ", "with "]
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
