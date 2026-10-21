//! Offline, adapter-declared installed CLI requirements. Package snapshots are
//! generated alongside adapter pins; SDK and Node engine versions are not CLIs.
//!
//! A requirement may also name the exact CLI package Intent downloads and
//! launches instead when the installed CLI is confirmed too old: Codex uses
//! the adapter-declared minimum; Claude uses the CLI version its pinned Agent
//! SDK was built against (`claudeCodeVersion`), with a same-minor minimum.
use node_semver::{Range, Version};
use serde_json::Value;

const PACKAGES: &str = include_str!("../data/adapter-cli-packages.json");

#[derive(Debug)]
pub struct CliRequirement {
    pub command: String,
    pub range: String,
    pub minimum: Version,
    /// Exact npm package the daemon may install when the installed CLI is
    /// confirmed below `minimum`. Always satisfies `minimum`.
    pub bundled: Option<BundledCli>,
}

/// An exact, offline-pinned CLI package (`package@version`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BundledCli {
    pub package: String,
    pub version: Version,
}

impl BundledCli {
    /// The exact npm spec, e.g. `@openai/codex@0.160.1`.
    #[must_use]
    pub fn spec(&self) -> String {
        format!("{}@{}", self.package, self.version)
    }
}

impl CliRequirement {
    /// Warn only below the derived lower bound, not outside an upper bound.
    /// Unknown output preserves its unknown state instead of asserting oldness.
    #[must_use]
    pub fn version_ok(&self, output: &str) -> Option<bool> {
        let found = output
            .split_whitespace()
            .find_map(|token| Version::parse(token.strip_prefix('v').unwrap_or(token)).ok())?;
        Some(found >= self.minimum)
    }
}

fn from_package(mapping: &Value) -> Option<CliRequirement> {
    let package = &mapping["package"];
    let cli_package = mapping["cliPackage"].as_str()?;
    // npm optionalDependencies override dependencies of the same package.
    let dependency = package["optionalDependencies"][cli_package]
        .as_str()
        .or_else(|| package["dependencies"][cli_package].as_str());
    let peer = package["peerDependencies"][cli_package].as_str();
    let declarations: Vec<_> = dependency.into_iter().chain(peer).collect();
    let mut ranges = declarations.iter().map(Range::parse);
    let first = ranges.next()?.ok()?;
    let range = ranges.try_fold(first, |left, right| left.intersect(&right.ok()?))?;
    let minimum = range.min_version()?;
    // Wildcards/unbounded lower ranges do not declare a useful minimum.
    if minimum <= Version::parse("0.0.0").ok()? {
        return None;
    }
    let bundled = exact_stable(&minimum).then(|| BundledCli {
        package: cli_package.to_owned(),
        version: minimum.clone(),
    });
    Some(CliRequirement {
        command: mapping["cliCommand"].as_str()?.to_owned(),
        range: declarations.join(" and "),
        minimum,
        bundled,
    })
}

fn exact_stable(version: &Version) -> bool {
    version.pre_release.is_empty() && version.build.is_empty()
}

/// A runtime the adapter depends on exactly (the Claude Agent SDK) records
/// the CLI version it was built against. Older patch releases of that minor
/// remain acceptable; the exact recorded version is the bundled fallback.
fn from_runtime(mapping: &Value) -> Option<CliRequirement> {
    let package = &mapping["package"];
    let runtime_package = mapping["runtimePackage"].as_str()?;
    let field = mapping["runtimeVersionField"].as_str()?;
    let runtime = &mapping["runtime"];
    let declared = package["optionalDependencies"][runtime_package]
        .as_str()
        .or_else(|| package["dependencies"][runtime_package].as_str())?;
    // A stale runtime snapshot must not describe another SDK's CLI.
    if runtime["name"].as_str()? != runtime_package || runtime["version"].as_str()? != declared {
        return None;
    }
    let built_for = Version::parse(runtime[field].as_str()?).ok()?;
    if !exact_stable(&built_for) || built_for <= Version::parse("0.0.0").ok()? {
        return None;
    }
    let minimum = Version::parse(format!("{}.{}.0", built_for.major, built_for.minor)).ok()?;
    Some(CliRequirement {
        command: mapping["cliCommand"].as_str()?.to_owned(),
        range: format!(">={minimum}"),
        minimum,
        bundled: Some(BundledCli {
            package: mapping["cliPackage"].as_str()?.to_owned(),
            version: built_for,
        }),
    })
}

#[must_use]
pub fn requirement(provider: &str) -> Option<CliRequirement> {
    let snapshots: Value = serde_json::from_str(PACKAGES).ok()?;
    let mapping = snapshots.get(provider)?;
    let package = &mapping["package"];
    let snapshot_pin = format!(
        "{}@{}",
        package["name"].as_str()?,
        package["version"].as_str()?
    );
    if crate::find_provider(provider)?.npx_only_package? != snapshot_pin {
        return None;
    }
    if mapping.get("runtimePackage").is_some() {
        return from_runtime(mapping);
    }
    from_package(mapping)
}

fn display_name(provider: &str) -> Option<&'static str> {
    match provider {
        "codex" => Some("Codex"),
        "claude-code" => Some("Claude Code"),
        _ => None,
    }
}

/// The bundled CLI to launch instead of an installed CLI whose parsed version
/// is confirmed below the requirement. Unknown versions keep the installed CLI.
#[must_use]
pub fn bundled_fallback(provider: &str, version: &str) -> Option<BundledCli> {
    display_name(provider)?;
    let required = requirement(provider)?;
    (required.version_ok(version) == Some(false))
        .then_some(required.bundled)
        .flatten()
}

/// Codex and Claude require their adapter-derived CLI minimum at launch. The
/// daemon first substitutes the bundled CLI for a confirmed-old installed one
/// (`bundled_fallback`); this check guards whatever is finally launched.
/// Discovery remains advisory; unknown versions and absent metadata do not
/// become a false assertion that a CLI is too old. Other providers keep their
/// existing independent launch rules.
/// # Errors
/// Returns an actionable detected/required version diagnostic for an old
/// Codex or Claude CLI.
pub fn validate_launch_version(provider: &str, version: &str) -> Result<(), String> {
    let Some(name) = display_name(provider) else {
        return Ok(());
    };
    let Some(required) = requirement(provider) else {
        return Ok(());
    };
    if required.version_ok(version) == Some(false) {
        return Err(format!(
            "Installed {name} CLI {version} is below the required minimum {} (adapter declares {}). Upgrade {name} on the execution host and retry.",
            required.minimum, required.range
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn snapshots_match_every_pinned_adapter() {
        let snapshots: Value = serde_json::from_str(PACKAGES).unwrap();
        for provider in crate::ACP_PROVIDERS {
            let Some(pin) = provider.npx_only_package else {
                continue;
            };
            let package = &snapshots[provider.id]["package"];
            assert_eq!(
                format!(
                    "{}@{}",
                    package["name"].as_str().unwrap(),
                    package["version"].as_str().unwrap()
                ),
                pin,
                "refresh exact adapter metadata: python3 -S scripts/provider_cli_metadata.py"
            );
        }
    }

    fn declared(range: &str) -> Value {
        json!({"cliPackage":"cli", "cliCommand":"cli", "package":{"dependencies":{"cli":range}}})
    }

    #[test]
    fn derives_npm_lower_bound_and_does_not_warn_on_newer_outside_range() {
        for (range, minimum) in [
            ("^0.159.1", "0.159.1"),
            ("~1.2.3", "1.2.3"),
            (">=1.2.3 <2", "1.2.3"),
            (">1.2.3", "1.2.4"),
            ("1.2 - 2.3", "1.2.0"),
            ("^2.0.0 || ^1.2.3", "1.2.3"),
            ("1.2.x", "1.2.0"),
            (">=1.2.3-beta.2", "1.2.3-beta.2"),
        ] {
            assert_eq!(
                from_package(&declared(range)).unwrap().minimum.to_string(),
                minimum
            );
        }
        let requirement = from_package(&declared("^0.159.1")).unwrap();
        assert_eq!(requirement.version_ok("codex-cli 0.159.0"), Some(false));
        assert_eq!(requirement.version_ok("codex-cli 0.159.1"), Some(true));
        assert_eq!(requirement.version_ok("codex-cli 0.160.0"), Some(true));
        assert_eq!(
            requirement.version_ok("codex-cli 0.159.1-beta.1"),
            Some(false)
        );
        assert_eq!(requirement.version_ok("v0.160.0+build"), Some(true));
        for output in ["", "unknown", "1.2.3.4", "build 20261009"] {
            assert_eq!(requirement.version_ok(output), None);
        }
        for range in [
            "*",
            "",
            "<2",
            "latest",
            "workspace:*",
            "file:../cli",
            ">2 <1",
        ] {
            assert!(from_package(&declared(range)).is_none(), "{range}");
        }
    }

    #[test]
    fn launch_gate_covers_codex_and_claude_and_uses_the_derived_minimum() {
        let minimum = requirement("codex").unwrap().minimum.to_string();
        let error = validate_launch_version("codex", "codex-cli 0.114.0").unwrap_err();
        assert!(error.contains("0.114.0") && error.contains(&minimum));
        assert!(error.contains("Codex CLI"), "{error}");
        for output in [minimum.as_str(), "codex-cli 99.0.0", "unknown"] {
            assert!(validate_launch_version("codex", output).is_ok(), "{output}");
        }
        let claude = requirement("claude-code").unwrap().minimum.to_string();
        let error = validate_launch_version("claude-code", "1.0.0 (Claude Code)").unwrap_err();
        assert!(
            error.contains("Claude Code CLI 1.0.0") && error.contains(&claude),
            "{error}"
        );
        assert!(validate_launch_version("claude-code", &format!("{claude} (Claude Code)")).is_ok());
        for provider in ["pi", "unknown"] {
            assert!(validate_launch_version(provider, "0.0.1").is_ok());
        }
    }

    #[test]
    fn bundled_pins_satisfy_their_own_minimum_and_follow_the_adapter_metadata() {
        let snapshots: Value = serde_json::from_str(PACKAGES).unwrap();
        let codex = requirement("codex").unwrap();
        let bundled = codex.bundled.clone().unwrap();
        assert_eq!(bundled.package, "@openai/codex");
        assert_eq!(bundled.version, codex.minimum);
        let claude = requirement("claude-code").unwrap();
        let bundled = claude.bundled.clone().unwrap();
        assert_eq!(bundled.package, "@anthropic-ai/claude-code");
        assert_eq!(
            bundled.version.to_string(),
            snapshots["claude-code"]["runtime"]["claudeCodeVersion"]
                .as_str()
                .unwrap()
        );
        assert_eq!(
            claude.minimum.to_string(),
            format!("{}.{}.0", bundled.version.major, bundled.version.minor)
        );
        for required in [&codex, &claude] {
            let version = required.bundled.as_ref().unwrap().version.to_string();
            assert_eq!(required.version_ok(&version), Some(true), "{version}");
        }
    }

    #[test]
    fn fallback_only_for_confirmed_old_codex_or_claude() {
        let codex = requirement("codex").unwrap().bundled.unwrap();
        assert_eq!(bundled_fallback("codex", "codex-cli 0.114.0"), Some(codex));
        let claude = requirement("claude-code").unwrap().bundled.unwrap();
        assert_eq!(
            bundled_fallback("claude-code", "1.0.128 (Claude Code)"),
            Some(claude.clone())
        );
        let patch_lag = format!(
            "{}.{}.0 (Claude Code)",
            claude.version.major, claude.version.minor
        );
        assert_eq!(bundled_fallback("claude-code", &patch_lag), None);
        for (provider, output) in [
            ("codex", "codex-cli 99.0.0"),
            ("codex", "unknown"),
            ("claude-code", ""),
            ("pi", "0.0.1"),
            ("unknown", "0.0.1"),
        ] {
            assert_eq!(
                bundled_fallback(provider, output),
                None,
                "{provider} {output}"
            );
        }
    }

    fn runtime_mapping(declared: &str, name: &str, version: &str, built_for: &str) -> Value {
        json!({
            "cliPackage": "@anthropic-ai/claude-code", "cliCommand": "claude",
            "runtimePackage": "sdk", "runtimeVersionField": "cliVersion",
            "package": {"dependencies": {"sdk": declared}},
            "runtime": {"name": name, "version": version, "cliVersion": built_for},
        })
    }

    #[test]
    fn runtime_requirement_is_same_minor_and_rejects_stale_snapshots() {
        let required =
            from_runtime(&runtime_mapping("0.3.280", "sdk", "0.3.280", "2.1.280")).unwrap();
        assert_eq!(required.minimum.to_string(), "2.1.0");
        assert_eq!(required.range, ">=2.1.0");
        assert_eq!(
            required.bundled.unwrap().spec(),
            "@anthropic-ai/claude-code@2.1.280"
        );
        for mapping in [
            runtime_mapping("0.3.281", "sdk", "0.3.280", "2.1.280"),
            runtime_mapping("0.3.280", "other", "0.3.280", "2.1.280"),
            runtime_mapping("0.3.280", "sdk", "0.3.280", "2.1.280-beta.1"),
            runtime_mapping("0.3.280", "sdk", "0.3.280", "^2.1.0"),
            runtime_mapping("0.3.280", "sdk", "0.3.280", "0.0.0"),
        ] {
            assert!(from_runtime(&mapping).is_none(), "{mapping}");
        }
    }

    #[test]
    fn only_mapped_cli_dependencies_count() {
        let mut mapping = declared("^1.2.3");
        mapping["package"] =
            json!({"engines":{"node":">=22"},"version":"99.0.0", "dependencies":{"sdk":"^90.0.0"}});
        assert!(from_package(&mapping).is_none());
        mapping["package"]["peerDependencies"] = json!({"cli":">=2.1.0"});
        assert_eq!(from_package(&mapping).unwrap().minimum.to_string(), "2.1.0");
        mapping["package"]["dependencies"] = json!({"cli":"^1.0.0"});
        assert!(
            from_package(&mapping).is_none(),
            "conflicting peer/dependency ranges"
        );
        mapping["package"]["optionalDependencies"] = json!({"cli":"^2.2.0"});
        assert_eq!(from_package(&mapping).unwrap().minimum.to_string(), "2.2.0");
        assert!(requirement("codex").is_some());
        assert!(requirement("claude-code").is_some());
        assert!(requirement("pi").is_none());
        assert!(requirement("unknown").is_none());
    }
}
