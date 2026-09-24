//! Doctor's rendering boundary: only sanitized reports and fixed messages.
//! No provider-health observation changes the command's daemon-health exit code.

use std::collections::BTreeSet;
use std::fmt::Write;

use intent_core::settings_file::SettingsFile;
use intent_services::codex_diagnostics::{
    CatalogOutcome, CodexCatalogReport, CodexLaunch, CodexRuntimeReport, LaunchSource,
    RuntimeSource, VersionMeasurement,
};

pub async fn report(settings: SettingsFile, live: bool) {
    // Production discovery can capture the login shell's PATH. Keep that
    // synchronous work off the async executor and keep errors private.
    let Ok(launch) = tokio::task::spawn_blocking(move || CodexLaunch::discover(&settings)).await
    else {
        println!("  [--] codex effective runtime: selection unavailable");
        return;
    };
    if live {
        println!("  [--] codex fresh catalogs: checking (may download the managed npm package)");
        let report = launch.fresh_catalogs().await;
        print!("{}", render_runtime(&report.runtime));
        print!("{}", render_catalogs(&report));
    } else {
        let inspection = launch.inspect_local().await;
        print!("{}", render_runtime(&inspection.report));
        println!("    Fresh catalogs not requested; use intentd doctor --codex-models (may download the managed package).");
    }
}

fn render_runtime(report: &CodexRuntimeReport) -> String {
    let mut text = String::from("  codex effective runtime:\n");
    let source = match report.launch_source {
        LaunchSource::SettingsOverride => "providers.paths override",
        LaunchSource::LocalDiscovery => "local discovery",
        LaunchSource::ManagedNpm => "managed npm package",
        LaunchSource::Unresolved => "unavailable",
    };
    writeln!(text, "    selected adapter: {source}").unwrap();
    writeln!(text, "    launch program: {}", report.launch_program).unwrap();
    writeln!(
        text,
        "    configured managed package (not a measured version): {}{}",
        report.configured_package,
        if report.launch_source == LaunchSource::ManagedNpm {
            ""
        } else {
            " (fallback only)"
        }
    )
    .unwrap();
    if report.removes_codex_overrides {
        text.push_str("    managed launch policy: CODEX_PATH and CODEX_CONFIG removed\n");
    }
    if let Some(path) = &report.adapter_path {
        writeln!(text, "    adapter path: {path}").unwrap();
    }
    render_version(&mut text, "adapter", &report.adapter_version);
    let runtime_source = match report.runtime_source {
        RuntimeSource::AdapterDependency => "selected adapter dependency",
        RuntimeSource::EnvironmentOverride => "effective CODEX_PATH override",
        RuntimeSource::Unknown => "unknown",
    };
    writeln!(text, "    runtime source: {runtime_source}").unwrap();
    if let Some(path) = &report.runtime_path {
        writeln!(text, "    runtime path: {path}").unwrap();
    }
    render_version(&mut text, "runtime", &report.runtime_version);
    text
}

fn render_version(text: &mut String, name: &str, version: &VersionMeasurement) {
    match version {
        VersionMeasurement::Measured(value) => {
            writeln!(text, "    [ok] measured {name} version: {value}").unwrap();
        }
        VersionMeasurement::Unknown(reason) => {
            writeln!(
                text,
                "    [--] {name} version: unknown ({})",
                reason.message()
            )
            .unwrap();
        }
    }
}

fn render_catalogs(report: &CodexCatalogReport) -> String {
    let mut text = String::new();
    let mut ids = BTreeSet::new();
    for (name, outcome) in [("ACP", &report.acp), ("raw runtime", &report.raw)] {
        match outcome {
            CatalogOutcome::Failed(reason) => {
                writeln!(
                    text,
                    "    [--] {name} catalog: unavailable ({})",
                    reason.message()
                )
                .unwrap();
            }
            CatalogOutcome::Success(catalog) => {
                if catalog.advertised {
                    writeln!(
                        text,
                        "    [ok] {name} catalog: advertised, {} model rows",
                        catalog.models.len()
                    )
                    .unwrap();
                } else {
                    writeln!(text, "    [--] {name} catalog: not advertised").unwrap();
                }
                if catalog.withheld_model_count != 0 {
                    writeln!(
                        text,
                        "      withheld model IDs: {} (comparison inconclusive)",
                        catalog.withheld_model_count
                    )
                    .unwrap();
                }
                for row in &catalog.models {
                    ids.insert(row.id.as_str());
                    write!(text, "      {}", row.id).unwrap();
                    if let Some(model) = &row.model {
                        ids.insert(model.as_str());
                        write!(text, "; model alias: {model}").unwrap();
                    }
                    if let Some(hidden) = row.hidden {
                        write!(text, "; hidden: {hidden}").unwrap();
                    }
                    writeln!(text, " — {}", row.source.message()).unwrap();
                }
            }
        }
    }
    if ids.is_empty() {
        text.push_str("    No model IDs available to compare.\n");
    } else {
        text.push_str("    Exact model ID/alias comparison:\n");
        for id in ids {
            writeln!(text, "      {id}: {}", report.observe(id).message()).unwrap();
        }
    }
    text.push_str("    Absence does not establish an account restriction; presence does not verify entitlement. No prompts were sent.\n");
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_services::codex_diagnostics::{
        Catalog, CatalogFailure, CatalogModel, CatalogSource, UnknownReason,
    };

    fn runtime() -> CodexRuntimeReport {
        CodexRuntimeReport {
            launch_source: LaunchSource::ManagedNpm,
            launch_program: "/fixture/npx".into(),
            configured_package: "@agentclientprotocol/codex-acp@1.13.1",
            removes_codex_overrides: true,
            adapter_path: None,
            adapter_version: VersionMeasurement::Unknown(UnknownReason::ManagedPackageNotInspected),
            runtime_source: RuntimeSource::Unknown,
            runtime_path: None,
            runtime_version: VersionMeasurement::Unknown(UnknownReason::ManagedPackageNotInspected),
        }
    }

    #[test]
    fn configured_pin_never_becomes_a_measured_version() {
        let text = render_runtime(&runtime());
        assert!(text.contains("configured managed package (not a measured version)"));
        assert!(text.contains("adapter version: unknown"));
        assert!(text.contains("runtime version: unknown"));
        assert!(!text.contains("[ok] measured"));
    }

    #[test]
    fn runtime_override_has_its_own_provenance() {
        let mut report = runtime();
        report.launch_source = LaunchSource::SettingsOverride;
        report.removes_codex_overrides = false;
        report.runtime_source = RuntimeSource::EnvironmentOverride;
        report.runtime_path = Some("/fixture/override.js".into());
        report.runtime_version = VersionMeasurement::Measured("0.333.4".into());
        let text = render_runtime(&report);
        assert!(text.contains("(fallback only)"));
        assert!(text.contains("runtime source: effective CODEX_PATH override"));
        assert!(text.contains("measured runtime version: 0.333.4"));
        assert!(!text.contains("managed launch policy"));
    }

    #[test]
    fn partial_success_retains_rows_and_fixed_failure_text() {
        let report = CodexCatalogReport {
            runtime: runtime(),
            acp: CatalogOutcome::Failed(CatalogFailure::RequestFailed),
            raw: CatalogOutcome::Success(Catalog {
                advertised: true,
                models: vec![CatalogModel {
                    id: "model-high".into(),
                    source: CatalogSource::CodexModelList,
                    model: Some("model".into()),
                    hidden: Some(true),
                }],
                withheld_model_count: 0,
            }),
        };
        let text = render_catalogs(&report);
        assert!(
            text.contains("ACP catalog: unavailable (provider rejected the diagnostic request)")
        );
        assert!(text.contains("model-high; model alias: model; hidden: true"));
        assert!(text.contains("model-high: comparison unavailable"));
        assert!(text.contains("model: comparison unavailable"));
    }
}
