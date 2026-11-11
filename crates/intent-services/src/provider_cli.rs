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
        let Some(id) = provider
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            continue;
        };
        let id = id.as_str();
        if provider.get("gatedOff").is_some() {
            continue;
        }
        if id == "antigravity" {
            if let Some(row) = provider.as_object_mut() {
                apply_agy_fields(row, crate::agy_cli::last_update_failure());
            }
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
                    // Match npx-backed launch PATH and neutral working directory.
                    // In particular, an env-node CLI must not become "unknown"
                    // merely because the daemon inherited a restricted PATH.
                    let cli = context.runtime.cli();
                    let prepared = tokio::task::spawn_blocking(move || {
                        let npx = if cli == InstalledCli::Codex {
                            intent_providers::find_codex_npx()
                        } else {
                            intent_providers::find_npx()
                        };
                        let path = intent_providers::enhanced_path(npx.as_deref());
                        intent_acp::NpxLaunchDir::create(None).map(|dir| (dir, path))
                    })
                    .await;
                    if let Ok(Ok((dir, launch_path))) = prepared {
                        let mut launch = tokio::process::Command::new(context.runtime.path());
                        launch.current_dir(dir.path()).env("PATH", launch_path);
                        context.apply(&mut launch);
                        context
                            .observe(&launch)
                            .await
                            .ok()
                            .map(|(_, version)| version)
                    } else {
                        None
                    }
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
            apply_version_fields(row, id, &requirement, path.as_deref(), version.as_deref());
        }
    }
}

fn apply_version_fields(
    row: &mut Map<String, Value>,
    id: &str,
    requirement: &CliRequirement,
    path: Option<&std::path::Path>,
    version: Option<&str>,
) {
    for key in [
        "cliVersion",
        "cliVersionOk",
        "cliResolvedPath",
        "cliBundledVersion",
    ] {
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
        // Launches substitute Intent's bundled CLI for this confirmed-old one;
        // discovery only reports it and never installs anything.
        if let Some(bundled) = intent_providers::adapter_cli::bundled_fallback(id, version) {
            row.insert(
                "cliBundledVersion".into(),
                json!(bundled.version.to_string()),
            );
        }
    }
}

/// agy has no adapter-declared requirement: its minimum is the first release
/// with `agy acp`. Reuses the display `cliVersion` already on the row. A
/// confirmed-old agy is updated at the next launch (`agy_cli`);
/// `cliUpdateError` reports a recent automatic update that failed.
fn apply_agy_fields(row: &mut Map<String, Value>, update_error: Option<String>) {
    use intent_providers::antigravity::{COMMAND, MIN_VERSION, MIN_VERSION_TRIPLE};
    let installed = row.get("installed").and_then(Value::as_bool) == Some(true);
    let range = format!(">={MIN_VERSION}");
    row.insert("cliCommand".into(), json!(COMMAND));
    row.insert("cliResolved".into(), json!(installed));
    row.insert("cliRequirement".into(), json!(format!("{COMMAND} {range}")));
    row.insert("cliVersionRange".into(), json!(range));
    row.insert("cliMinimumVersion".into(), json!(MIN_VERSION));
    row.remove("cliVersionOk");
    row.remove("cliUpdateError");
    if !installed {
        return;
    }
    let found = row
        .get("cliVersion")
        .and_then(Value::as_str)
        .and_then(intent_providers::version_gate::parse_cli_version);
    if let Some(found) = found {
        let ok = found >= MIN_VERSION_TRIPLE;
        row.insert("cliVersionOk".into(), json!(ok));
        if let (false, Some(error)) = (ok, update_error) {
            row.insert("cliUpdateError".into(), json!(error));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agy_row_reports_minimum_verdict_and_only_a_relevant_update_error() {
        let error = Some("agy update exited with 3".to_owned());
        for (installed, version, ok, shows_error) in [
            (true, Some("1.5.5"), Some(true), false),
            (true, Some("1.3.4"), Some(false), true),
            (true, Some("agy dev build"), None, false),
            (true, None, None, false),
            (false, None, None, false),
        ] {
            let mut row = Map::new();
            row.insert("installed".into(), json!(installed));
            if let Some(version) = version {
                row.insert("cliVersion".into(), json!(version));
            }
            apply_agy_fields(&mut row, error.clone());
            assert_eq!(row["cliMinimumVersion"], "1.5.0");
            assert_eq!(row["cliResolved"], installed);
            assert_eq!(
                row.get("cliVersionOk").and_then(Value::as_bool),
                ok,
                "{version:?}"
            );
            assert_eq!(
                row.contains_key("cliUpdateError"),
                shows_error,
                "{version:?}"
            );
        }
    }

    #[test]
    fn advisory_fields_preserve_unknown_and_do_not_change_availability() {
        let requirement = requirement("codex").unwrap();
        let minimum = requirement.minimum.to_string();
        let path = std::path::Path::new("/selected/codex");
        for (version, expected) in [
            // cli-fixture-version: allow — discovery reports an old Codex
            (Some("codex-cli 0.0.1"), Some(false)),
            (Some(minimum.as_str()), Some(true)),
            // cli-fixture-version: allow — discovery reports a far-newer Codex as current
            (Some("codex-cli 99.0.0"), Some(true)),
            (Some("unknown"), None),
            (None, None),
        ] {
            let mut row = Map::from_iter([("installed".into(), json!(true))]);
            apply_version_fields(&mut row, "codex", &requirement, Some(path), version);
            assert_eq!(row.get("cliVersionOk"), expected.map(Value::Bool).as_ref());
            let bundled = requirement.bundled.as_ref().unwrap().version.to_string();
            assert_eq!(
                row.get("cliBundledVersion"),
                (expected == Some(false)).then(|| json!(bundled)).as_ref(),
                "only a confirmed-old CLI is replaced: {version:?}"
            );
            assert_eq!(row["cliMinimumVersion"], minimum);
            assert_eq!(row["cliVersionRange"], requirement.range);
            assert_eq!(row["cliResolvedPath"], json!(path));
            assert_eq!(row["installed"], true, "Codex minimum is advisory");
        }
        let mut missing = Map::from_iter([("cliBundledVersion".into(), json!("stale"))]);
        apply_version_fields(&mut missing, "codex", &requirement, None, None);
        assert_eq!(missing["cliResolved"], false);
        assert!(!missing.contains_key("cliVersionOk"));
        assert!(!missing.contains_key("cliVersion"));
        assert!(
            !missing.contains_key("cliBundledVersion"),
            "missing CLIs are not replaced"
        );
    }

    #[test]
    fn claude_rows_report_the_same_minor_minimum_and_bundled_copy() {
        let requirement = requirement("claude-code").unwrap();
        let bundled = requirement.bundled.as_ref().unwrap().version.to_string();
        let path = std::path::Path::new("/selected/claude");
        let mut row = Map::new();
        apply_version_fields(
            &mut row,
            "claude-code",
            &requirement,
            Some(path),
            // cli-fixture-version: allow — discovery reports the bundled CLI for an old Claude
            Some("1.0.128 (Claude Code)"),
        );
        assert_eq!(row["cliCommand"], "claude");
        assert_eq!(row["cliVersionOk"], false);
        assert_eq!(row["cliBundledVersion"], bundled);
        assert_eq!(row["cliMinimumVersion"], requirement.minimum.to_string());
        apply_version_fields(
            &mut row,
            "claude-code",
            &requirement,
            Some(path),
            Some(&format!("{bundled} (Claude Code)")),
        );
        assert_eq!(row["cliVersionOk"], true);
        assert!(!row.contains_key("cliBundledVersion"));
    }

    #[test]
    fn providers_without_a_bundled_launch_never_report_one() {
        let requirement = requirement("codex").unwrap();
        let mut row = Map::new();
        // cli-fixture-version: allow — non-Codex/Claude rows ignore the version
        apply_version_fields(&mut row, "pi", &requirement, None, Some("codex-cli 0.0.1"));
        assert_eq!(row["cliVersionOk"], false);
        assert!(!row.contains_key("cliBundledVersion"));
    }

    #[tokio::test]
    async fn absent_declarations_leave_legacy_pi_and_other_rows_unchanged() {
        let original = json!({"providers":[
            {"id":"pi", "cliVersionOk":false, "cliRequirement":"legacy Pi gate"},
            {"id":"opencode", "installed":true}, {"id":"unknown"},
            {"id":"codex", "gatedOff":"disabled"}
        ]});
        let mut discovery = original.clone();
        add_installed_cli_versions(&mut discovery).await;
        assert_eq!(discovery, original);
    }
}
