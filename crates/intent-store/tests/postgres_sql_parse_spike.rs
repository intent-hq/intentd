//! Spike: can `sqlparser` (`PostgreSqlDialect`) parse the SQL the Postgres
//! lints check? Parses every SQL literal in `src/postgres` and every
//! statement in `postgres/migrations/*.sql`, then prints how many parse and
//! groups the failures by cause. Reports only; it never fails.
//!
//! `cargo test -p intent-store --test postgres_sql_parse_spike -- --ignored --nocapture`

use std::collections::BTreeMap;
use std::path::Path;

use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[path = "support/sql_literals.rs"]
mod sql_literals;
use sql_literals::{is_sql, rust_files, string_literals};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Cause {
    /// A complete statement the grammar rejects.
    GrammarGap,
    /// A `format!` template (`{name}` holes) or a fragment that is not a
    /// whole statement.
    Fragment,
    /// A PL/pgSQL function body or `DO` block.
    PlPgSql,
    /// Text that `is_sql` takes for SQL but is not: error and log messages
    /// such as `"delete note failed"`.
    NotSql,
    Other,
}

struct Failure {
    cause: Cause,
    origin: String,
    sql: String,
    error: String,
}

#[derive(Default)]
struct Report {
    parsed: usize,
    opaque_bodies: usize,
    recovered_by_placeholder: usize,
    failures: Vec<Failure>,
}

/// Rust escapes in a literal, undone (`string_literals` keeps them).
fn unescape(lit: &str) -> String {
    let mut out = String::with_capacity(lit.len());
    let mut chars = lit.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some(o) => out.push(o),
            None => {}
        }
    }
    out
}

/// Whether the text holds a `format!` hole such as `{}`, `{name}`, `{0}`.
fn has_format_hole(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    bytes.iter().enumerate().any(|(i, &b)| {
        b == b'{'
            && bytes[i + 1..]
                .iter()
                .take_while(|&&c| c != b'}')
                .all(|&c| c.is_ascii_alphanumeric() || c == b'_' || c == b':')
            && bytes[i + 1..].contains(&b'}')
    })
}

fn fill_format_holes(sql: &str) -> String {
    let mut out = String::new();
    let mut rest = sql;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        if let Some(close) = rest[open..].find('}') {
            out.push_str(" true ");
            rest = &rest[open + close + 1..];
        } else {
            out.push_str(&rest[open..]);
            rest = "";
        }
    }
    out.push_str(rest);
    out
}

fn balanced(sql: &str) -> bool {
    let mut depth = 0i32;
    for c in sql.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        if depth < 0 {
            return false;
        }
    }
    depth == 0
}

fn is_plpgsql(sql: &str) -> bool {
    let l = sql.to_lowercase();
    l.contains("plpgsql") || l.trim_start().starts_with("do ")
}

fn parse(sql: &str) -> Result<Vec<Statement>, String> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| e.to_string())
}

/// Short prose with no SQL punctuation or parameters.
fn looks_like_prose(sql: &str) -> bool {
    let words = sql.split_whitespace().count();
    words <= 8 && !sql.contains(['$', '=', '(', '*', ',', '\''])
}

fn classify(sql: &str, fragment: bool) -> Cause {
    if looks_like_prose(&fill_format_holes(sql)) {
        Cause::NotSql
    } else if is_plpgsql(sql) {
        Cause::PlPgSql
    } else if fragment || !balanced(sql) {
        Cause::Fragment
    } else {
        Cause::GrammarGap
    }
}

impl Report {
    fn check(&mut self, origin: String, sql: &str) {
        match parse(sql) {
            Ok(stmts) => {
                self.parsed += 1;
                self.opaque_bodies += stmts
                    .iter()
                    .filter(|s| matches!(s, Statement::CreateFunction(_)))
                    .count();
            }
            Err(error) => {
                let hole = has_format_hole(sql);
                let mut error = error;
                if hole {
                    match parse(&fill_format_holes(sql)) {
                        Ok(_) => self.recovered_by_placeholder += 1,
                        Err(filled) => error = format!("{error}; with holes filled: {filled}"),
                    }
                }
                let cause = if sql.trim().is_empty() {
                    Cause::Other
                } else {
                    classify(sql, hole)
                };
                self.failures.push(Failure {
                    cause,
                    origin,
                    sql: sql.trim().to_owned(),
                    error,
                });
            }
        }
    }

    fn print(&self, title: &str) {
        let total = self.parsed + self.failures.len();
        println!("\n== {title}: {}/{total} parse", self.parsed);
        if self.opaque_bodies > 0 {
            println!(
                "   ({} CREATE FUNCTION bodies parsed as opaque strings)",
                self.opaque_bodies
            );
        }
        if self.recovered_by_placeholder > 0 {
            println!(
                "   ({} format! templates parse once each hole is `true`)",
                self.recovered_by_placeholder
            );
        }
        let mut groups: BTreeMap<Cause, Vec<&Failure>> = BTreeMap::new();
        for f in &self.failures {
            groups.entry(f.cause).or_default().push(f);
        }
        for (cause, items) in groups {
            println!("-- {cause:?}: {}", items.len());
            let shown = if cause == Cause::GrammarGap || std::env::var_os("SPIKE_VERBOSE").is_some()
            {
                items.len()
            } else {
                1
            };
            for f in items.iter().take(shown) {
                let flat = f.sql.split_whitespace().collect::<Vec<_>>().join(" ");
                let mut sql: String = flat.chars().take(240).collect();
                if sql.len() < flat.len() {
                    sql.push('…');
                }
                println!(
                    "   {}\n     error: {}\n     sql:   {sql}",
                    f.origin, f.error
                );
            }
        }
    }
}

/// Statements of a migration file, split on `;` outside quotes, quoted
/// identifiers, dollar quotes and comments.
fn split_statements(sql: &str) -> Vec<(usize, String)> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let (mut start, mut i, mut line, mut start_line) = (0, 0, 1, 1);
    let mut dollar: Option<String> = None;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\n' {
            line += 1;
        }
        if let Some(tag) = &dollar {
            if sql[i..].starts_with(tag.as_str()) {
                i += tag.len();
                dollar = None;
                continue;
            }
            i += 1;
            continue;
        }
        match b {
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let end = sql[i + 2..]
                    .find("*/")
                    .map_or(bytes.len(), |p| i + 2 + p + 2);
                line += sql[i..end].matches('\n').count();
                i = end;
                continue;
            }
            b'\'' | b'"' => {
                let mut j = i + 1;
                while j < bytes.len() && bytes[j] != b {
                    if bytes[j] == b'\n' {
                        line += 1;
                    }
                    j += 1;
                }
                i = j + 1;
                continue;
            }
            b'$' => {
                let tag_end = sql[i + 1..]
                    .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .map(|p| i + 1 + p);
                if let Some(e) = tag_end.filter(|&e| bytes[e] == b'$') {
                    let first = bytes.get(i + 1).copied().unwrap_or(b'$');
                    if !first.is_ascii_digit() {
                        dollar = Some(sql[i..=e].to_owned());
                        i = e + 1;
                        continue;
                    }
                }
            }
            b';' => {
                let stmt = &sql[start..i];
                if !strip_comments(stmt).trim().is_empty() {
                    out.push((start_line, stmt.to_owned()));
                }
                start = i + 1;
                start_line = line;
            }
            _ => {}
        }
        if start == i && b.is_ascii_whitespace() {
            start = i + 1;
            start_line = line;
        }
        i += 1;
    }
    if !strip_comments(&sql[start..]).trim().is_empty() {
        out.push((start_line, sql[start..].to_owned()));
    }
    out
}

fn strip_comments(sql: &str) -> String {
    sql.lines()
        .map(|l| l.split("--").next().unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
#[ignore = "report-only spike; run with --ignored --nocapture"]
fn sqlparser_parses_the_postgres_sql() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));

    let mut literals = Report::default();
    let mut files = Vec::new();
    rust_files(&root.join("src/postgres"), &mut files);
    files.sort();
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read");
        let rel = file
            .strip_prefix(root)
            .unwrap_or(&file)
            .display()
            .to_string();
        for (line, lit) in string_literals(&text) {
            if is_sql(&lit) {
                literals.check(format!("{rel}:{line}"), &unescape(&lit));
            }
        }
    }

    let mut migrations = Report::default();
    let dir = root.join("postgres/migrations");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("read postgres/migrations")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    files.sort();
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read migration");
        let name = file.file_name().unwrap().to_string_lossy().into_owned();
        for (line, stmt) in split_statements(&text) {
            migrations.check(format!("{name}:{line}"), &stmt);
        }
    }

    literals.print("SQL literals in src/postgres");
    migrations.print("Statements in postgres/migrations");
}
