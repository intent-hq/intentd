//! Source lint: every `Services` field and every `intent_app::tenant` loop is
//! classified in `crates/intent-services/state-inventory.toml`.
//!
//! Running more than one stateless orchestrator replica per tenant needs to
//! know which process-local state blocks it: caches are safe to lose, gates
//! guard invariants the Postgres tenant writer lock may or may not cover, and
//! owned work needs one owner. `Services` has over 200 fields, so the
//! inventory is only useful if it cannot drift. This lint fails when:
//!
//! - a field of `pub struct Services` (`crates/intent-services/src/lib.rs`,
//!   `#[cfg]`-gated fields included) has no `[[field]]` row, or a row names
//!   no field;
//! - a restore, sweep or loop that `crates/intent-app/src/tenant.rs` calls
//!   has no `[[loop]]` row, or a row names no call. A call is a `.name(` or
//!   `::name(` whose name starts with one of [`LOOP_PREFIXES`], minus
//!   [`NOT_LOOPS`];
//! - a row is malformed: unknown class, empty note, a `gate` without
//!   `invariant` and `writer_lock` (`covered` / `partial` / `no`), or those
//!   keys on a non-gate row.
//!
//! Fix a failure by adding, removing or fixing the row in the inventory. The
//! file uses a small TOML subset (`[[field]]` / `[[loop]]` headers, one-line
//! `key = "value"` pairs, `#` comments) parsed here without a TOML dependency.
//! There is no opt-out marker. Limit: a tenant loop started through a helper
//! whose name lacks the prefixes is not seen.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use intentd_test_support::source_lint::{lex, matching_close, tokenize_str, workspace_root};

const INVENTORY: &str = "crates/intent-services/state-inventory.toml";
const SERVICES_SRC: &str = "crates/intent-services/src/lib.rs";
const TENANT_SRC: &str = "crates/intent-app/src/tenant.rs";

const CLASSES: &[&str] = &[
    "durable",
    "cache",
    "gate",
    "volatile",
    "owner",
    "connection",
    "config",
    "test",
];
const WRITER_LOCK: &[&str] = &["covered", "partial", "no"];
const LOOP_KINDS: &[&str] = &["restore", "sweep", "loop"];

/// Name prefixes of the startup restores, sweeps and loops `tenant.rs` calls.
const LOOP_PREFIXES: &[&str] = &[
    "spawn_",
    "sweep_",
    "rehydrate_",
    "hydrate_",
    "heal_",
    "resume_",
    "refresh_",
];
/// Calls with a [`LOOP_PREFIXES`] name that start no tenant work of their own.
const NOT_LOOPS: &[&str] = &["spawn_daemon", "spawn_blocking"];

#[derive(Debug, Default, Clone)]
struct Row {
    section: String,
    line: usize,
    keys: BTreeMap<String, String>,
}

impl Row {
    fn get(&self, key: &str) -> Option<&str> {
        self.keys.get(key).map(String::as_str)
    }
}

/// Parses the inventory's TOML subset. Errors name `INVENTORY:line`.
fn parse_inventory(text: &str) -> Result<Vec<Row>, Vec<String>> {
    let mut rows: Vec<Row> = Vec::new();
    let mut errors = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = i + 1;
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some(section) = trimmed
            .strip_prefix("[[")
            .and_then(|rest| rest.strip_suffix("]]"))
        {
            if section != "field" && section != "loop" {
                errors.push(format!("{INVENTORY}:{line}: unknown table [[{section}]]"));
            }
            rows.push(Row {
                section: section.to_string(),
                line,
                keys: BTreeMap::new(),
            });
            continue;
        }
        let Some((key, value)) = trimmed.split_once(" = ") else {
            errors.push(format!("{INVENTORY}:{line}: expected `key = \"value\"`"));
            continue;
        };
        let Some(value) = value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .filter(|v| !v.contains('"') && !v.contains('\\'))
        else {
            errors.push(format!(
                "{INVENTORY}:{line}: value must be one plain double-quoted string"
            ));
            continue;
        };
        let Some(row) = rows.last_mut() else {
            errors.push(format!(
                "{INVENTORY}:{line}: key before any [[field]] / [[loop]]"
            ));
            continue;
        };
        if row
            .keys
            .insert(key.to_string(), value.to_string())
            .is_some()
        {
            errors.push(format!("{INVENTORY}:{line}: duplicate key `{key}`"));
        }
    }
    if errors.is_empty() {
        Ok(rows)
    } else {
        Err(errors)
    }
}

/// Schema problems of one row, as `INVENTORY:line: …`.
fn row_errors(row: &Row) -> Vec<String> {
    let at = format!("{INVENTORY}:{}", row.line);
    let mut out = Vec::new();
    let mut allowed: Vec<&str> = vec!["name", "class", "note"];
    match row.get("name") {
        Some(n) if !n.is_empty() => {}
        _ => out.push(format!("{at}: missing `name`")),
    }
    let class = row.get("class").unwrap_or_default();
    if !CLASSES.contains(&class) {
        out.push(format!(
            "{at}: class `{class}` is not one of {}",
            CLASSES.join(", ")
        ));
    }
    if row.get("note").is_none_or(str::is_empty) {
        out.push(format!("{at}: missing `note`"));
    }
    if row.section == "loop" {
        allowed.push("kind");
        let kind = row.get("kind").unwrap_or_default();
        if !LOOP_KINDS.contains(&kind) {
            out.push(format!(
                "{at}: kind `{kind}` is not one of {}",
                LOOP_KINDS.join(", ")
            ));
        }
    }
    if class == "gate" {
        allowed.extend(["invariant", "writer_lock"]);
        if row.get("invariant").is_none_or(str::is_empty) {
            out.push(format!("{at}: a gate row must name its `invariant`"));
        }
        let lock = row.get("writer_lock").unwrap_or_default();
        if !WRITER_LOCK.contains(&lock) {
            out.push(format!(
                "{at}: a gate row needs `writer_lock` = {}",
                WRITER_LOCK.join(" | ")
            ));
        }
    }
    for key in row.keys.keys() {
        if !allowed.contains(&key.as_str()) {
            out.push(format!("{at}: key `{key}` is not allowed on this row"));
        }
    }
    out
}

/// The field names of `pub struct Services { … }` in `src`, in order.
fn services_fields(src: &str) -> Vec<String> {
    let blanked = lex(src).blanked;
    let tokens = tokenize_str(&blanked);
    let open = tokens
        .windows(4)
        .position(|w| w[0].is("pub") && w[1].is("struct") && w[2].is("Services") && w[3].is("{"))
        .unwrap_or_else(|| panic!("{SERVICES_SRC}: `pub struct Services {{` not found"))
        + 3;
    let close = matching_close(&tokens, open)
        .unwrap_or_else(|| panic!("{SERVICES_SRC}: `pub struct Services` never closes"));
    let depth = tokens[open].depth + 1;
    let body = &tokens[open + 1..close];
    body.windows(2)
        .filter(|w| w[0].depth == depth && w[0].is_ident() && w[1].is(":"))
        .map(|w| w[0].text.clone())
        .collect()
}

/// The restore, sweep and loop functions `src` (tenant.rs) calls, with or
/// without a turbofish (`.spawn_x::<T>(…)`).
fn tenant_loops(src: &str) -> BTreeSet<String> {
    let blanked = lex(src).blanked;
    let tokens = tokenize_str(&blanked);
    let mut out = BTreeSet::new();
    for i in 1..tokens.len() {
        let name = &tokens[i];
        if !(tokens[i - 1].is(".") || tokens[i - 1].is("::")) || !name.is_ident() {
            continue;
        }
        if !is_call_after(&tokens, i + 1) {
            continue;
        }
        let name = name.text.as_str();
        if LOOP_PREFIXES.iter().any(|p| name.starts_with(p)) && !NOT_LOOPS.contains(&name) {
            out.insert(name.to_string());
        }
    }
    out
}

/// Whether the tokens from `at` open a call: `(`, or `::<…>` then `(`.
/// Angle brackets are counted per character, so `>>` closes two levels.
fn is_call_after(tokens: &[intentd_test_support::source_lint::Token], at: usize) -> bool {
    match tokens.get(at) {
        Some(t) if t.is("(") => true,
        Some(t) if t.is("::") => {
            if !tokens.get(at + 1).is_some_and(|t| t.text.starts_with('<')) {
                return false;
            }
            let mut depth = 0usize;
            let mut j = at + 1;
            while let Some(t) = tokens.get(j) {
                j += 1;
                for c in t.text.chars() {
                    match c {
                        '<' => depth += 1,
                        '>' if depth == 0 => return false,
                        '>' => depth -= 1,
                        _ => {}
                    }
                }
                if depth == 0 {
                    break;
                }
            }
            depth == 0 && tokens.get(j).is_some_and(|t| t.is("("))
        }
        _ => false,
    }
}

/// Names in `code` with no row, and rows naming nothing in `code`.
fn drift(what: &str, code: &BTreeSet<String>, rows: &[&Row], hint: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut listed = BTreeSet::new();
    for row in rows {
        // A row without a name already fails `row_errors`; no drift noise.
        let Some(name) = row.get("name").filter(|name| !name.is_empty()) else {
            continue;
        };
        if !listed.insert(name.to_string()) {
            out.push(format!(
                "{INVENTORY}:{}: duplicate {what} row `{name}`",
                row.line
            ));
        }
        if !code.contains(name) {
            out.push(format!(
                "{INVENTORY}:{}: {what} `{name}` no longer exists in {hint}; remove its row",
                row.line
            ));
        }
    }
    for name in code.difference(&listed) {
        out.push(format!(
            "{what} `{name}` in {hint} is not classified: add a [[{}]] row to {INVENTORY}",
            if what == "field" { "field" } else { "loop" }
        ));
    }
    out
}

/// Every inventory problem for the given sources.
fn check(inventory: &str, services_src: &str, tenant_src: &str) -> Vec<String> {
    let rows = match parse_inventory(inventory) {
        Ok(rows) => rows,
        Err(errors) => return errors,
    };
    let mut out: Vec<String> = rows.iter().flat_map(row_errors).collect();
    let fields: BTreeSet<String> = services_fields(services_src).into_iter().collect();
    let field_rows: Vec<&Row> = rows.iter().filter(|r| r.section == "field").collect();
    out.extend(drift(
        "field",
        &fields,
        &field_rows,
        &format!("`pub struct Services` ({SERVICES_SRC})"),
    ));
    let loops = tenant_loops(tenant_src);
    let loop_rows: Vec<&Row> = rows.iter().filter(|r| r.section == "loop").collect();
    out.extend(drift("loop", &loops, &loop_rows, TENANT_SRC));
    out
}

fn read(rel: &str) -> String {
    let path = workspace_root().join(rel);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[test]
fn services_state_inventory_matches_the_code() {
    let inventory = read(INVENTORY);
    let problems = check(&inventory, &read(SERVICES_SRC), &read(TENANT_SRC));
    assert!(
        problems.is_empty(),
        "process-local state inventory is out of date ({INVENTORY}; see its header for the \
         classes):\n  {}",
        problems.join("\n  ")
    );
    // Counts per class, for the inventory's readers (`--nocapture`).
    let rows = parse_inventory(&inventory).expect("parsed above");
    for section in ["field", "loop"] {
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for row in rows.iter().filter(|r| r.section == section) {
            *counts
                .entry(row.get("class").unwrap_or_default())
                .or_default() += 1;
        }
        eprintln!("{section}: {counts:?}");
    }
}

const SAMPLE_SERVICES: &str = r"
pub struct Other { ignored: u8 }
/// Doc.
#[derive(Clone)]
pub struct Services {
    /// A store.
    store: Store,
    #[cfg(test)]
    park: Option<Arc<Park>>,
    #[expect(clippy::option_option)]
    map: Arc<Mutex<HashMap<(String, u8), std::sync::Arc<dyn Fn() + Send>>>>,
}
";

const SAMPLE_TENANT: &str = r"
fn start(services: &Services) {
    intent_core::spawn_daemon(async move { services.sweep_trash().await });
    let a = orchestration.spawn(|| services.spawn_a_loop());
    tokio::task::spawn_blocking(|| intent_core::sweep_tmp(&root));
    services.other_call();
}
";

const SAMPLE_INVENTORY: &str = r#"
# comment
[[field]]
name = "store"
class = "config"
note = "Store handle."

[[field]]
name = "park"
class = "test"
note = "Park."

[[field]]
name = "map"
class = "gate"
invariant = "Something stays ordered."
writer_lock = "partial"
note = "Map."

[[loop]]
name = "sweep_trash"
kind = "sweep"
class = "owner"
note = "Trash."

[[loop]]
name = "spawn_a_loop"
kind = "loop"
class = "owner"
note = "A."

[[loop]]
name = "sweep_tmp"
kind = "loop"
class = "owner"
note = "Tmp."
"#;

#[test]
fn parses_fields_including_cfg_gated_ones() {
    assert_eq!(services_fields(SAMPLE_SERVICES), ["store", "park", "map"]);
}

#[test]
fn finds_tenant_loop_calls() {
    let loops: Vec<String> = tenant_loops(SAMPLE_TENANT).into_iter().collect();
    assert_eq!(loops, ["spawn_a_loop", "sweep_tmp", "sweep_trash"]);
}

#[test]
fn finds_tenant_loop_calls_with_a_turbofish() {
    let tenant = r"
fn start(services: &Services) {
    services.refresh_typed::<Vec<Option<u8>>>(cx);
    Self::heal_generic::<T>();
    let f = services.spawn_not_called::<T>;
    services.sweep_ordered::<A, B>(1 << 2);
}
";
    let loops: Vec<String> = tenant_loops(tenant).into_iter().collect();
    assert_eq!(loops, ["heal_generic", "refresh_typed", "sweep_ordered"]);
}

#[test]
fn sample_inventory_passes() {
    assert_eq!(
        check(SAMPLE_INVENTORY, SAMPLE_SERVICES, SAMPLE_TENANT),
        Vec::<String>::new()
    );
}

#[test]
fn an_added_field_fails_naming_the_inventory() {
    let services =
        SAMPLE_SERVICES.replace("    store: Store,", "    store: Store,\n    added: u8,");
    let problems = check(SAMPLE_INVENTORY, &services, SAMPLE_TENANT);
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert!(problems[0].contains("`added`"), "{problems:?}");
    assert!(problems[0].contains(INVENTORY), "{problems:?}");
}

#[test]
fn a_removed_field_or_loop_fails() {
    let services = SAMPLE_SERVICES.replace("    store: Store,\n", "");
    let problems = check(SAMPLE_INVENTORY, &services, SAMPLE_TENANT);
    assert!(
        problems.len() == 1 && problems[0].contains("field `store` no longer exists"),
        "{problems:?}"
    );
    let tenant = SAMPLE_TENANT.replace("services.spawn_a_loop()", "services.other()");
    let problems = check(SAMPLE_INVENTORY, SAMPLE_SERVICES, &tenant);
    assert!(
        problems.len() == 1 && problems[0].contains("loop `spawn_a_loop` no longer exists"),
        "{problems:?}"
    );
}

#[test]
fn an_added_loop_fails() {
    let tenant = SAMPLE_TENANT.replace(
        "services.other_call();",
        "services.other_call();\n    services.spawn_new_loop();",
    );
    let problems = check(SAMPLE_INVENTORY, SAMPLE_SERVICES, &tenant);
    assert!(
        problems.len() == 1 && problems[0].contains("loop `spawn_new_loop`"),
        "{problems:?}"
    );
}

#[test]
fn a_row_without_a_name_fails_once() {
    let unnamed = format!(
        "{SAMPLE_INVENTORY}\n[[field]]\nclass = \"config\"\nnote = \"No name.\"\n\n[[field]]\nclass = \"config\"\nnote = \"No name either.\"\n"
    );
    let problems = check(&unnamed, SAMPLE_SERVICES, SAMPLE_TENANT);
    assert_eq!(problems.len(), 2, "one error per unnamed row: {problems:?}");
    assert!(problems.iter().all(|p| p.contains("name")), "{problems:?}");
    assert!(
        !problems
            .iter()
            .any(|p| p.contains("no longer exists") || p.contains("duplicate")),
        "{problems:?}"
    );
}

#[test]
fn malformed_rows_fail() {
    let bad_class = SAMPLE_INVENTORY.replace("class = \"test\"", "class = \"misc\"");
    assert!(check(&bad_class, SAMPLE_SERVICES, SAMPLE_TENANT)
        .iter()
        .any(|p| p.contains("class `misc`")));
    let no_lock = SAMPLE_INVENTORY.replace("writer_lock = \"partial\"\n", "");
    assert!(check(&no_lock, SAMPLE_SERVICES, SAMPLE_TENANT)
        .iter()
        .any(|p| p.contains("writer_lock")));
    let lock_on_cache = SAMPLE_INVENTORY.replace(
        "class = \"test\"",
        "class = \"cache\"\nwriter_lock = \"no\"",
    );
    assert!(check(&lock_on_cache, SAMPLE_SERVICES, SAMPLE_TENANT)
        .iter()
        .any(|p| p.contains("key `writer_lock` is not allowed")));
    let dup = format!(
        "{SAMPLE_INVENTORY}\n[[field]]\nname = \"store\"\nclass = \"config\"\nnote = \"Again.\"\n"
    );
    assert!(check(&dup, SAMPLE_SERVICES, SAMPLE_TENANT)
        .iter()
        .any(|p| p.contains("duplicate field row `store`")));
}
