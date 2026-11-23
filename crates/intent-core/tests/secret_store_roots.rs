//! Guard: only a composition root may build the process-wide default secrets
//! store (`FileSecretStore::new()`, the `FileTokenStore::default()` that wraps
//! it, or a `Services::new` that builds one). `FileSecretStore` has no
//! `Default` impl, so `Default::default()` / `unwrap_or_default` /
//! `#[derive(Default)]` cannot build it implicitly.
//!
//! Every other reader or writer takes the store its `Services` owns
//! (`Services::secret_source`), so orchestrator tenants sharing one process
//! never read each other's — or the host's — credentials. A leaf crate that
//! reaches for the default file again fails this test.

use std::path::{Path, PathBuf};

/// Calls that resolve the process-wide default file.
const DEFAULT_STORE_CALLS: &[&str] = &[
    "FileSecretStore::new()",
    "FileTokenStore::default()",
    "Services::new(",
];

/// Where non-test code may still build the default store, and how often.
/// `None` allows any number (the definition, and the `intentd` binary that is
/// itself the desktop composition root).
const ALLOWED: &[(&str, Option<usize>)] = &[
    // The definition (`impl Default` and the constructor itself).
    ("intent-core/src/secrets.rs", None),
    // `Services::new`: the desktop / `intentd` default composition.
    ("intent-services/src/lib.rs", Some(1)),
    // `FileTokenStore`'s `Default`: the daemon's server auth token, built only
    // by the `intentd` binary.
    ("intent-transport/src/auth.rs", Some(1)),
    // The `intentd` binary is the desktop composition root.
    ("intentd/src/", None),
];

/// One default-store call found in non-test code.
#[derive(Debug, PartialEq, Eq)]
struct Hit {
    line: usize,
    text: String,
}

/// Default-store calls in `source`, skipping comments and `#[cfg(test)]`
/// items (`cfg(test)`, `cfg(all(test, …))`, `cfg(any(test, …))`).
fn default_store_calls(source: &str) -> Vec<Hit> {
    let test_lines = test_item_lines(source);
    source
        .lines()
        .enumerate()
        .filter(|(i, _)| !test_lines.contains(&(i + 1)))
        .filter_map(|(i, line)| {
            let code = strip_line_comment(line);
            DEFAULT_STORE_CALLS
                .iter()
                .any(|call| code.contains(call))
                .then(|| Hit {
                    line: i + 1,
                    text: line.trim().to_owned(),
                })
        })
        .collect()
}

/// `line` without its `//` comment; a `//` inside a string literal (a URL)
/// is code, not a comment.
fn strip_line_comment(line: &str) -> &str {
    let mut in_str = false;
    let mut escaped = false;
    let mut prev_slash = false;
    for (i, c) in line.char_indices() {
        if in_str {
            match (escaped, c) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_str = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => {
                in_str = true;
                prev_slash = false;
            }
            '/' if prev_slash => return &line[..i - 1],
            '/' => prev_slash = true,
            _ => prev_slash = false,
        }
    }
    line
}

fn is_test_cfg(attr: &str) -> bool {
    let compact: String = attr.chars().filter(|c| !c.is_whitespace()).collect();
    ["#[cfg(test)]", "#[cfg(all(test,", "#[cfg(any(test,"]
        .iter()
        .any(|p| compact.starts_with(p))
}

/// 1-based line numbers covered by `#[cfg(test)]` items: from the attribute
/// to the brace closing the item it gates (or its `;`, for `mod x;`).
fn test_item_lines(source: &str) -> std::collections::HashSet<usize> {
    let lines: Vec<&str> = source.lines().collect();
    let mut covered = std::collections::HashSet::new();
    let mut i = 0;
    while i < lines.len() {
        if !is_test_cfg(lines[i].trim_start()) {
            i += 1;
            continue;
        }
        let start = i;
        let mut depth = 0usize;
        let mut opened = false;
        let mut end = lines.len() - 1;
        'scan: for (j, line) in lines.iter().enumerate().skip(start + 1) {
            let code = strip_line_comment(line);
            let mut in_str = false;
            let mut escaped = false;
            for c in code.chars() {
                if in_str {
                    match (escaped, c) {
                        (true, _) => escaped = false,
                        (false, '\\') => escaped = true,
                        (false, '"') => in_str = false,
                        _ => {}
                    }
                    continue;
                }
                match c {
                    '"' => in_str = true,
                    '{' => {
                        depth += 1;
                        opened = true;
                    }
                    '}' => {
                        depth = depth.saturating_sub(1);
                        if opened && depth == 0 {
                            end = j;
                            break 'scan;
                        }
                    }
                    ';' if !opened => {
                        end = j;
                        break 'scan;
                    }
                    _ => {}
                }
            }
        }
        covered.extend(start + 1..=end + 1);
        i = end + 1;
    }
    covered
}

/// Whole files that are test-only by name or location.
fn is_test_file(rel: &str) -> bool {
    let name = rel.rsplit('/').next().unwrap_or(rel);
    rel.contains("/tests/")
        || rel.contains("/benches/")
        || rel.contains("/test_support/")
        || name == "tests.rs"
        || name.ends_with("_tests.rs")
        || name.ends_with("_test.rs")
        || name == "test_support.rs"
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn only_composition_roots_build_the_default_secret_store() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates dir");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(crates)
        .expect("read crates dir")
        .flatten()
    {
        rust_sources(&entry.path().join("src"), &mut files);
    }
    assert!(
        files.len() > 100,
        "the guard found too few sources to be scanning the workspace"
    );
    let mut violations = Vec::new();
    for file in files {
        let rel = file
            .strip_prefix(crates)
            .expect("under crates")
            .to_string_lossy()
            .replace('\\', "/");
        if is_test_file(&rel) {
            continue;
        }
        let source = std::fs::read_to_string(&file).expect("read source");
        let hits = default_store_calls(&source);
        if hits.is_empty() {
            continue;
        }
        let allowed = ALLOWED.iter().find(|(prefix, _)| {
            rel == *prefix || (prefix.ends_with('/') && rel.starts_with(prefix))
        });
        match allowed {
            Some((_, None)) => {}
            Some((_, Some(max))) if hits.len() <= *max => {}
            _ => violations.extend(
                hits.into_iter()
                    .map(|h| format!("crates/{rel}:{}: {}", h.line, h.text)),
            ),
        }
    }
    assert!(
        violations.is_empty(),
        "FileSecretStore::new()/default() outside a composition root reads the \
         process-wide secrets file, which orchestrator tenants share. Take the \
         store from `Services::secret_source()` (or the caller's \
         `SharedSecretStore`) instead:\n{}",
        violations.join("\n")
    );
}

#[test]
fn guard_flags_a_leaf_crate_default_store() {
    let source = r#"
pub async fn token() -> Option<String> {
    intent_core::FileSecretStore::new().load("x").ok().flatten()
}
"#;
    assert_eq!(
        default_store_calls(source),
        vec![Hit {
            line: 3,
            text: r#"intent_core::FileSecretStore::new().load("x").ok().flatten()"#.into(),
        }]
    );
    assert_eq!(
        default_store_calls("let t = intent_transport::FileTokenStore::default();").len(),
        1
    );
    assert_eq!(
        default_store_calls("let s = intent_services::Services::new(store);").len(),
        1
    );
    assert_eq!(
        default_store_calls(r#"let u = "https://example.com"; let s = FileSecretStore::new();"#)
            .len(),
        1,
        "a URL before the call must not hide it"
    );
    assert!(default_store_calls("Services::new_with_secret_file(store, file)").is_empty());
}

#[test]
fn guard_ignores_comments_and_test_items() {
    let source = r#"
// FileSecretStore::new() in a comment
fn prod() {}

#[cfg(test)]
mod tests {
    fn inner() {
        let _ = "}";
        let s = FileSecretStore::new();
    }
}

#[cfg(all(test, unix))]
mod unix_tests {
    fn f() { FileSecretStore::new(); }
}

#[cfg(test)]
mod more;

fn after() { FileSecretStore::new(); }
"#;
    let hits = default_store_calls(source);
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].text, "fn after() { FileSecretStore::new(); }");
}

#[test]
fn guard_classifies_test_files() {
    assert!(is_test_file(
        "intent-services/src/saved_credentials_tests.rs"
    ));
    assert!(is_test_file("intent-services/src/agent_manager/tests.rs"));
    assert!(is_test_file("intent-services/src/tests/home_isolation.rs"));
    assert!(!is_test_file("intent-linear/src/token.rs"));
}
