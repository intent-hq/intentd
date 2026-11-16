//! Manifest lint: only `intent-store` may take `sqlx` as a normal (non-dev)
//! dependency. Without it, a crate cannot build or run a query in shipped
//! code, so every production query stays behind the store, where a second
//! (Postgres) backend can replace it. Test code may still use `sqlx`
//! against the `SQLite` store through `[dev-dependencies]`.

use intentd_test_support::source_lint::workspace_root;

const ALLOWED: &[&str] = &["intent-store"];

fn normal_dependency_tables(manifest: &toml::Table) -> Vec<(&str, &toml::Table)> {
    let mut tables = Vec::new();
    if let Some(deps) = manifest.get("dependencies").and_then(toml::Value::as_table) {
        tables.push(("[dependencies]", deps));
    }
    if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
        for target in targets.values().filter_map(toml::Value::as_table) {
            if let Some(deps) = target.get("dependencies").and_then(toml::Value::as_table) {
                tables.push(("[target.*.dependencies]", deps));
            }
        }
    }
    tables
}

fn names_sqlx(key: &str, spec: &toml::Value) -> bool {
    let package = spec
        .as_table()
        .and_then(|t| t.get("package"))
        .and_then(toml::Value::as_str);
    package.unwrap_or(key) == "sqlx"
}

#[test]
fn only_intent_store_depends_on_sqlx_outside_tests() {
    let crates = workspace_root().join("crates");
    let mut offenders = Vec::new();
    let mut checked = 0;
    for entry in std::fs::read_dir(&crates).expect("read crates/") {
        let manifest_path = entry.expect("crate entry").path().join("Cargo.toml");
        let Ok(text) = std::fs::read_to_string(&manifest_path) else {
            continue;
        };
        let manifest: toml::Table = text.parse().expect("parse Cargo.toml");
        let name = manifest
            .get("package")
            .and_then(|p| p.get("name"))
            .and_then(toml::Value::as_str)
            .expect("package.name")
            .to_owned();
        checked += 1;
        if ALLOWED.contains(&name.as_str()) {
            continue;
        }
        for (section, deps) in normal_dependency_tables(&manifest) {
            if deps.iter().any(|(key, spec)| names_sqlx(key, spec)) {
                offenders.push(format!("{name} {section}"));
            }
        }
    }
    assert!(checked > 10, "found only {checked} crate manifests");
    assert!(
        offenders.is_empty(),
        "sqlx must be a [dev-dependencies] entry outside intent-store; move the \
         query behind a Store method instead: {offenders:?}"
    );
}
