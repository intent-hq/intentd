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

/// The package a dependency entry resolves to: its own `package = ...`
/// rename, or the rename on the `[workspace.dependencies]` entry it inherits.
fn names_sqlx(key: &str, spec: &toml::Value, workspace_deps: &toml::Table) -> bool {
    let package_of = |spec: &toml::Value| {
        spec.as_table()
            .and_then(|t| t.get("package"))
            .and_then(toml::Value::as_str)
            .map(str::to_owned)
    };
    let inherits = spec
        .as_table()
        .and_then(|t| t.get("workspace"))
        .and_then(toml::Value::as_bool)
        .unwrap_or(false);
    let package = package_of(spec).or_else(|| {
        inherits
            .then(|| workspace_deps.get(key).and_then(package_of))
            .flatten()
    });
    package.as_deref().unwrap_or(key) == "sqlx"
}

#[test]
fn only_intent_store_depends_on_sqlx_outside_tests() {
    let root = workspace_root();
    let root_manifest: toml::Table = std::fs::read_to_string(root.join("Cargo.toml"))
        .expect("read workspace Cargo.toml")
        .parse()
        .expect("parse workspace Cargo.toml");
    let workspace_deps = root_manifest
        .get("workspace")
        .and_then(|w| w.get("dependencies"))
        .and_then(toml::Value::as_table)
        .cloned()
        .unwrap_or_default();
    let crates = root.join("crates");
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
            if deps
                .iter()
                .any(|(key, spec)| names_sqlx(key, spec, &workspace_deps))
            {
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
