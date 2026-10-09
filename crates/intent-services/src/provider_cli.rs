//! Advisory adapter-declared CLI minima for startup discovery. Never probe an
//! adapter/SDK as the CLI, or perform package-registry requests at startup.
use intent_providers::adapter_cli::{requirement, CliRequirement};
use intent_providers::installed_cli::InstalledCli;
use serde_json::{json, Map, Value};

/// Add optional adapter-declared requirements using launch-time CLI selection.
/// Pi's separately maintained hard launch gate is left intact.
pub async fn add_installed_cli_versions(discovery: &mut Value) {
    let Some(providers) = discovery.get_mut("providers").and_then(Value::as_array_mut) else {
        return;
    };
    for provider in providers {
        let Some(id) = provider.get("id").and_then(Value::as_str) else {
            continue;
        };
        if provider.get("gatedOff").is_some() {
            continue;
        }
        let Some(requirement) = requirement(id) else {
            continue;
        };
        let (path, version) = if let Some(cli) = InstalledCli::for_provider(id) {
            let context = crate::installed_cli::InstalledContext::discover(cli)
                .await
                .ok();
            let path = context.as_ref().map(|c| c.runtime.path().to_owned());
            let version = match context {
                Some(context) => {
                    let mut launch = tokio::process::Command::new(context.runtime.path());
                    context.apply(&mut launch);
                    context
                        .observe(&launch)
                        .await
                        .ok()
                        .map(|(_, version)| version)
                }
                None => None,
            };
            (path, version)
        } else if id == "pi" {
            let Ok(status) = tokio::task::spawn_blocking(crate::pi_cli::probe_pi_cli).await else {
                continue;
            };
            (status.resolved_path, status.version_output)
        } else {
            continue;
        };
        if let Some(row) = provider.as_object_mut() {
            apply_version_fields(row, &requirement, path.as_deref(), version.as_deref());
        }
    }
}

fn apply_version_fields(
    row: &mut Map<String, Value>,
    requirement: &CliRequirement,
    path: Option<&std::path::Path>,
    version: Option<&str>,
) {
    for key in ["cliVersion", "cliVersionOk", "cliResolvedPath"] {
        row.remove(key);
    }
    row.insert("cliCommand".into(), json!(requirement.command));
    row.insert("cliResolved".into(), json!(path.is_some()));
    row.insert(
        "cliRequirement".into(),
        json!(format!("{} {}", requirement.command, requirement.range)),
    );
    row.insert("cliVersionRange".into(), json!(requirement.range));
    row.insert(
        "cliMinimumVersion".into(),
        json!(requirement.minimum.to_string()),
    );
    if let Some(path) = path {
        row.insert("cliResolvedPath".into(), json!(path));
    }
    if let Some(version) = version {
        row.insert("cliVersion".into(), json!(version));
        if let Some(ok) = requirement.version_ok(version) {
            row.insert("cliVersionOk".into(), json!(ok));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advisory_fields_preserve_unknown_and_do_not_change_availability() {
        let requirement = requirement("codex").unwrap();
        let minimum = requirement.minimum.to_string();
        let path = std::path::Path::new("/selected/codex");
        for (version, expected) in [
            (Some("codex-cli 0.0.1"), Some(false)),
            (Some(minimum.as_str()), Some(true)),
            (Some("codex-cli 99.0.0"), Some(true)),
            (Some("unknown"), None),
            (None, None),
        ] {
            let mut row = Map::from_iter([("installed".into(), json!(true))]);
            apply_version_fields(&mut row, &requirement, Some(path), version);
            assert_eq!(row.get("cliVersionOk"), expected.map(Value::Bool).as_ref());
            assert_eq!(row["cliMinimumVersion"], minimum);
            assert_eq!(row["cliVersionRange"], requirement.range);
            assert_eq!(row["cliResolvedPath"], json!(path));
            assert_eq!(row["installed"], true, "Codex minimum is advisory");
        }
        let mut missing = Map::new();
        apply_version_fields(&mut missing, &requirement, None, None);
        assert_eq!(missing["cliResolved"], false);
        assert!(!missing.contains_key("cliVersionOk"));
        assert!(!missing.contains_key("cliVersion"));
    }

    #[tokio::test]
    async fn absent_declarations_leave_legacy_pi_and_other_rows_unchanged() {
        let original = json!({"providers":[
            {"id":"pi", "cliVersionOk":false, "cliRequirement":"legacy Pi gate"},
            {"id":"claude-code", "installed":true}, {"id":"unknown"},
            {"id":"codex", "gatedOff":"disabled"}
        ]});
        let mut discovery = original.clone();
        add_installed_cli_versions(&mut discovery).await;
        assert_eq!(discovery, original);
    }
}
