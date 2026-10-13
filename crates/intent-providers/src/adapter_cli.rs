//! Offline, adapter-declared installed CLI requirements. Package snapshots are
//! generated alongside adapter pins; SDK and Node engine versions are not CLIs.
use node_semver::{Range, Version};
use serde_json::Value;

const PACKAGES: &str = include_str!("../data/adapter-cli-packages.json");

#[derive(Debug)]
pub struct CliRequirement {
    pub command: String,
    pub range: String,
    pub minimum: Version,
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
    Some(CliRequirement {
        command: mapping["cliCommand"].as_str()?.to_owned(),
        range: declarations.join(" and "),
        minimum,
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
    from_package(mapping)
}

/// Codex requires its adapter-declared CLI minimum before an adapter launch.
/// Discovery remains advisory; unknown versions and absent metadata do not
/// become a false assertion that a CLI is too old. Other providers keep their
/// existing independent launch rules.
/// # Errors
/// Returns an actionable detected/required version diagnostic only for old Codex.
pub fn validate_launch_version(provider: &str, version: &str) -> Result<(), String> {
    if provider != "codex" {
        return Ok(());
    }
    let Some(required) = requirement(provider) else {
        return Ok(());
    };
    if required.version_ok(version) == Some(false) {
        return Err(format!(
            "Installed Codex CLI {version} is below the required minimum {} (adapter declares {}). Upgrade Codex on the execution host and retry.",
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
    fn launch_gate_is_codex_only_and_uses_the_derived_minimum() {
        let minimum = requirement("codex").unwrap().minimum.to_string();
        let error = validate_launch_version("codex", "codex-cli 0.114.0").unwrap_err();
        assert!(error.contains("0.114.0") && error.contains(&minimum));
        for output in [minimum.as_str(), "codex-cli 99.0.0", "unknown"] {
            assert!(validate_launch_version("codex", output).is_ok(), "{output}");
        }
        for provider in ["claude-code", "pi", "unknown"] {
            assert!(validate_launch_version(provider, "0.0.1").is_ok());
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
        assert!(requirement("claude-code").is_none());
        assert!(requirement("pi").is_none());
        assert!(requirement("unknown").is_none());
    }
}
