//! Nightly smoke test of every pinned npm provider package, run the way
//! Intent runs it, on a real npm from an empty npm cache
//! (`scripts/npm-provider-smoke.sh`, `.github/workflows/nightly-npm-provider-smoke.yml`).
//! npm 12 stopped running install scripts by default and broke npm-run
//! providers for alpha users (intentd#3827); nothing caught it before a user
//! report, because unit tests only compare argv against stub npx scripts.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::provider_cli_launch::ProviderCli;

const NPM_VERSION_ENV: &str = "INTENT_NPM_SMOKE_VERSION";

/// A cold download of a package with a native binary on a slow runner.
fn download_budget() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(300))
}

/// The output of a CLI whose install script never ran (`opencode-ai`,
/// `@anthropic-ai/claude-code`), as `remove_unfinished_install` matches it.
const STUB_OUTPUT: [&str; 2] = [
    "postinstall script was not run",
    "native binary not installed",
];

struct Run {
    status: Option<i32>,
    stdout: String,
    stderr: String,
}

async fn run(mut command: tokio::process::Command, budget: Duration) -> Result<Run, String> {
    command
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    match tokio::time::timeout(budget, command.output()).await {
        Err(_) => Err(format!("timed out after {}s", budget.as_secs())),
        Ok(Err(error)) => Err(format!("could not start: {error}")),
        Ok(Ok(output)) => Ok(Run {
            status: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).trim().to_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        }),
    }
}

/// Why `run` does not show a working CLI, if it does not. `npm_version` is
/// what npx prints for its own `--version`: npm before 7.7 parses flags after
/// the package itself, so `npx -y <pkg> --version` exits 0 without running it.
fn verdict(run: Result<Run, String>, npm_version: &str) -> Result<String, String> {
    let run = run?;
    let output = format!("{}\n{}", run.stdout, run.stderr);
    let tail = |text: &str| {
        let lines: Vec<_> = text.lines().collect();
        lines[lines.len().saturating_sub(8)..].join("\n    ")
    };
    if run.status != Some(0) {
        let status = run
            .status
            .map_or("by a signal".to_owned(), |code| code.to_string());
        return Err(format!("exit {status}\n    {}", tail(output.trim())));
    }
    if let Some(stub) = STUB_OUTPUT.iter().find(|stub| output.contains(*stub)) {
        return Err(format!("printed the stub's \"{stub}\""));
    }
    if run.stdout.is_empty() {
        return Err(format!("printed nothing\n    {}", tail(&run.stderr)));
    }
    if run.stdout == npm_version {
        return Err(format!(
            "npx printed its own version {npm_version}: npm parsed the CLI's flags"
        ));
    }
    Ok(run.stdout.lines().next().unwrap_or_default().to_owned())
}

fn npm_version_of(tool: &Path) -> String {
    let output = std::process::Command::new(tool)
        .arg("--version")
        .output()
        .unwrap_or_else(|e| panic!("{} --version: {e}", tool.display()));
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn is_empty_dir(dir: &Path) -> bool {
    std::fs::read_dir(dir).map_or(true, |mut entries| entries.next().is_none())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "downloads every pinned npm provider package; run by scripts/npm-provider-smoke.sh"]
async fn pinned_npm_provider_packages_run_from_an_empty_npm_cache() {
    assert!(
        intentd_test_support::test_egress_allowed(),
        "set {}=1: the smoke test downloads from the npm registry",
        intentd_test_support::ALLOW_TEST_EGRESS_ENV
    );
    let npm_version = std::env::var(NPM_VERSION_ENV)
        .unwrap_or_else(|_| panic!("set {NPM_VERSION_ENV} to the npm version under test"));
    let cache = std::env::var_os("npm_config_cache")
        .map(PathBuf::from)
        .expect("set npm_config_cache to an empty folder: a warm cache hides a broken install");
    assert!(
        is_empty_dir(&cache),
        "npm cache {} is not empty",
        cache.display()
    );
    let npx = intent_providers::find_npx().expect("npx is on PATH");
    let npm = crate::bundled_cli::find_npm().expect("npm is next to npx");
    for tool in [&npx, &npm] {
        assert_eq!(
            npm_version_of(tool),
            npm_version,
            "{} is not the npm under test",
            tool.display()
        );
    }

    let bundled_root = tempfile::tempdir().unwrap();
    let mut results = Vec::new();
    for provider in intent_providers::ACP_PROVIDERS {
        let bundled = intent_providers::adapter_cli::missing_fallback(provider.id);
        if provider.fallback_npx_package.is_none() && bundled.is_none() {
            continue;
        }
        let Some((cli, _)) = ProviderCli::npx_with_bundled(provider.id) else {
            results.push((
                provider.id.to_owned(),
                Err("Intent found no npx launch".to_owned()),
            ));
            continue;
        };
        let ProviderCli::Npx { package, .. } = &cli else {
            unreachable!("npx_with_bundled returns an npx launch");
        };
        // OpenCode's `--version` answers from its JS wrapper; `models` needs the native binary.
        let args: &[&str] = if provider.id == "opencode" {
            &["models"]
        } else {
            &["--version"]
        };
        let label = format!("npx -y {package} {}", args.join(" "));
        let outcome = verdict(
            run(cli.command(args), download_budget()).await,
            &npm_version,
        );
        results.push((label, outcome));

        let Some(bundled) = bundled else { continue };
        let installed = intent_providers::installed_cli::InstalledCli::for_provider(provider.id)
            .expect("a bundled CLI has an installed CLI kind");
        let label = format!(
            "npm install {} (Intent's bundled CLI) {} --version",
            bundled.spec(),
            installed.command()
        );
        let outcome = match crate::bundled_cli::ensure_in(
            bundled_root.path(),
            &npm,
            &intent_providers::enhanced_path(Some(&npm)),
            installed,
            &bundled,
            download_budget(),
        )
        .await
        {
            Err(error) => Err(error),
            Ok(runtime) => {
                let mut command = tokio::process::Command::new(runtime.path());
                command.arg("--version");
                verdict(run(command, download_budget()).await, &npm_version)
            }
        };
        results.push((label, outcome));
    }

    let mut failures = Vec::new();
    for (label, outcome) in &results {
        match outcome {
            Ok(first_line) => println!("ok   {label} on npm {npm_version}: {first_line}"),
            Err(why) => {
                println!("FAIL {label} on npm {npm_version}: {why}");
                failures.push(format!("{label}: {why}"));
            }
        }
    }
    assert!(
        results
            .iter()
            .any(|(label, _)| label.contains(intent_providers::OPENCODE_NPX_PACKAGE)),
        "the registry yielded no OpenCode npx launch: {results:?}",
        results = results.iter().map(|(label, _)| label).collect::<Vec<_>>()
    );
    assert!(
        failures.is_empty(),
        "{} of {} npm provider launches failed on npm {npm_version}:\n{}",
        failures.len(),
        results.len(),
        failures.join("\n")
    );
}
