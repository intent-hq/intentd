//! SQL string literals in Rust source, shared by the Postgres lints.

use std::path::{Path, PathBuf};

pub fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
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
pub fn string_literals(text: &str) -> Vec<(usize, String)> {
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
pub fn is_sql(lit: &str) -> bool {
    let l = lit.trim_start().to_lowercase();
    if l.starts_with("update ") {
        return l.split_whitespace().any(|w| w == "set");
    }
    ["select ", "insert ", "delete ", "with "]
        .iter()
        .any(|k| l.starts_with(k))
}
