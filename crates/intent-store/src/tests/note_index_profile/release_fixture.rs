//! Release simulation must not rewrite an external dependency with the same version.

pub(super) fn bump_manifest(manifest: &str, version: &str) -> String {
    let mut manifest: toml::Table = manifest.parse().expect("valid fixture manifest");
    bump_scope(&mut manifest, version);
    if let Some(workspace) = manifest
        .get_mut("workspace")
        .and_then(toml::Value::as_table_mut)
    {
        bump_scope(workspace, version);
    }
    if let Some(targets) = manifest
        .get_mut("target")
        .and_then(toml::Value::as_table_mut)
    {
        for target in targets
            .iter_mut()
            .filter_map(|(_, value)| value.as_table_mut())
        {
            bump_scope(target, version);
        }
    }
    toml::to_string(&manifest).expect("serialize fixture manifest")
}

fn bump_scope(scope: &mut toml::Table, version: &str) {
    if let Some(package) = scope.get_mut("package").and_then(toml::Value::as_table_mut) {
        bump_version(package, version);
    }
    for section in ["dependencies", "dev-dependencies", "build-dependencies"] {
        if let Some(dependencies) = scope.get_mut(section).and_then(toml::Value::as_table_mut) {
            for dependency in dependencies
                .iter_mut()
                .filter_map(|(_, value)| value.as_table_mut())
            {
                if dependency
                    .get("path")
                    .and_then(toml::Value::as_str)
                    .is_some()
                {
                    bump_version(dependency, version);
                }
            }
        }
    }
}

fn bump_version(table: &mut toml::Table, version: &str) {
    if table.get("version").and_then(toml::Value::as_str) == Some(version) {
        table.insert(
            "version".to_owned(),
            toml::Value::String("99.0.0".to_owned()),
        );
    }
}

#[test]
fn release_bump_preserves_colliding_external_versions() {
    let original = r#"
[package]
name = "intent-store"
version = "0.11.0"

[dependencies]
sha2 = "0.11.0"
external = { version = "0.11.0", features = ["0.11.0"] }
intent-core = { path = "../intent-core", version = "0.11.0" }
renamed = { package = "intent-services", path = "../intent-services", version = "0.11.0" }
other = { path = "../other", version = "1.2.3" }
inherited = { workspace = true }
unversioned = { path = "../unversioned" }

[dev-dependencies.sha2]
version = "0.11.0"

[target.'cfg(unix)'.build-dependencies]
external = { version = "0.11.0" }
intent-core = { path = "../intent-core", version = "0.11.0" }

[workspace.package]
version = "0.11.0"

[workspace.dependencies]
sha2 = { version = "0.11.0" }
intent-core = { path = "crates/intent-core", version = "0.11.0" }

[package.metadata]
version = "0.11.0"
"#;
    let mut expected: toml::Table = original.parse().unwrap();
    let bumped = toml::Value::String("99.0.0".to_owned());
    expected["package"]["version"] = bumped.clone();
    expected["workspace"]["package"]["version"] = bumped.clone();
    expected["dependencies"]["intent-core"]["version"] = bumped.clone();
    expected["dependencies"]["renamed"]["version"] = bumped.clone();
    expected["target"]["cfg(unix)"]["build-dependencies"]["intent-core"]["version"] =
        bumped.clone();
    expected["workspace"]["dependencies"]["intent-core"]["version"] = bumped;
    assert_eq!(
        bump_manifest(original, "0.11.0")
            .parse::<toml::Table>()
            .unwrap(),
        expected
    );
}
