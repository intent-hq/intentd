//! Supported, bounded Codex 0.160.0 controls for a known source inventory.
//!
//! The native fixture proves plugin suppression, per-name MCP and exact
//! SKILL.md-path denials, including bundled skills. A new skill still appears on reload.
//! These controls are useful building blocks for a frozen source view, NEVER a
//! universal discovery shutoff or justification to clear capability gaps.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, PathBuf};

use serde_json::{json, Value};

use super::{ProfileError, ProfilePurpose, ProfileResult};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SkillOrigin {
    Ambient,
    /// Only skills actually bundled with the pinned runtime, not plugin/admin.
    Bundled,
}

pub struct KnownSkill {
    /// Exact absolute SKILL.md path returned by the native loader inventory.
    pub path: PathBuf,
    pub origin: SkillOrigin,
}

/// Flat config keys are shared by native `-c` and app-server config overrides.
/// Does not include auth/model configuration or read any native configuration.
pub struct KnownSourceDenials {
    overrides: BTreeMap<String, Value>,
}

impl KnownSourceDenials {
    #[must_use]
    pub fn overrides(&self) -> &BTreeMap<String, Value> {
        &self.overrides
    }
    /// Serialize TOML values rather than shell fragments. Pass these argv items
    /// directly to the native executable; never concatenate them into a shell.
    #[must_use]
    pub fn runtime_args(&self) -> Vec<String> {
        self.overrides
            .iter()
            .flat_map(|(key, value)| {
                let value = toml_value(value);
                ["-c".into(), format!("{key}={value}")]
            })
            .collect()
    }
}

fn toml_value(value: &Value) -> toml_edit::Value {
    match value {
        Value::Bool(value) => (*value).into(),
        Value::String(value) => value.as_str().into(),
        Value::Array(values) => {
            let mut array = toml_edit::Array::new();
            for value in values {
                array.push(toml_value(value));
            }
            toml_edit::Value::Array(array)
        }
        Value::Object(values) => {
            let mut table = toml_edit::InlineTable::new();
            for (key, value) in values {
                table.insert(key, toml_value(value));
            }
            toml_edit::Value::InlineTable(table)
        }
        _ => {
            unreachable!("private control map contains only typed boolean/string/collection values")
        }
    }
}

/// Deny known ambient server names and skill paths without conflating a protocol
/// name with an Intent settings ID. Approved-name collisions require a stronger
/// source boundary/atomic replacement, so they fail instead of losing an approved
/// server or leaving its native counterpart enabled. Interactive bundled skills
/// remain; ephemeral also disables every enumerated bundled skill.
///
/// # Errors
/// Ambiguous names or non-absolute/non-UTF8/non-SKILL.md paths are rejected.
pub fn known_source_denials(
    native_server_names: &BTreeSet<String>,
    approved_server_names: &BTreeSet<String>,
    skills: &[KnownSkill],
    purpose: ProfilePurpose,
) -> ProfileResult<KnownSourceDenials> {
    let failure = || {
        ProfileError::UnsupportedIsolation {
        provider: "codex".into(),
        missing: "known-source denials require disjoint server names and exact absolute skill paths; use a verified source boundary for dynamic/native collisions",
    }
    };
    if !native_server_names.is_disjoint(approved_server_names)
        || native_server_names.iter().any(String::is_empty)
    {
        return Err(failure());
    }
    let mut overrides = BTreeMap::from([("features.plugins".into(), json!(false))]);
    let mut servers = serde_json::Map::new();
    for name in native_server_names {
        servers.insert(name.clone(), json!({"enabled":false}));
    }
    if !servers.is_empty() {
        // Codex splits -c keys on literal dots; TOML quoting in the key does NOT
        // escape those separators. Put names inside the TOML VALUE instead.
        overrides.insert("mcp_servers".into(), Value::Object(servers));
    }
    let mut paths = BTreeSet::new();
    for skill in skills {
        if purpose == ProfilePurpose::Interactive && skill.origin == SkillOrigin::Bundled {
            continue;
        }
        if !skill.path.is_absolute()
            || skill.path.file_name().is_none_or(|s| s != "SKILL.md")
            || skill
                .path
                .components()
                .any(|c| matches!(c, Component::ParentDir))
        {
            return Err(failure());
        }
        paths.insert(skill.path.to_str().ok_or_else(failure)?.to_owned());
    }
    overrides.insert(
        "skills.config".into(),
        json!(paths
            .into_iter()
            .map(|path| json!({"path":path,"enabled":false}))
            .collect::<Vec<_>>()),
    );
    Ok(KnownSourceDenials { overrides })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn purpose_separates_bundled_from_ambient_and_deduplicates_paths() {
        let skills = vec![
            KnownSkill {
                path: "/fixture/native/SKILL.md".into(),
                origin: SkillOrigin::Ambient,
            },
            KnownSkill {
                path: "/fixture/bundled/SKILL.md".into(),
                origin: SkillOrigin::Bundled,
            },
            KnownSkill {
                path: "/fixture/native/SKILL.md".into(),
                origin: SkillOrigin::Ambient,
            },
        ];
        for (purpose, count) in [
            (ProfilePurpose::Interactive, 1),
            (ProfilePurpose::Ephemeral, 2),
        ] {
            let result =
                known_source_denials(&BTreeSet::new(), &BTreeSet::new(), &skills, purpose).unwrap();
            assert_eq!(
                result.overrides["skills.config"].as_array().unwrap().len(),
                count
            );
        }
    }

    #[test]
    fn argv_toml_roundtrips_quoted_names_and_paths_without_key_injection() {
        let name = "native.with.dots\"and[brackets]";
        let skill = KnownSkill {
            path: "/fixture/quotes\"and\\slash/SKILL.md".into(),
            origin: SkillOrigin::Ambient,
        };
        let result = known_source_denials(
            &BTreeSet::from([name.into()]),
            &BTreeSet::new(),
            &[skill],
            ProfilePurpose::Ephemeral,
        )
        .unwrap();
        let args = result.runtime_args();
        let text = args
            .chunks_exact(2)
            .map(|p| {
                assert_eq!(p[0], "-c");
                p[1].as_str()
            })
            .collect::<Vec<_>>()
            .join("\n");
        let decoded = super::super::auth::toml_json(&text).unwrap();
        assert_eq!(decoded["mcp_servers"][name]["enabled"], false);
        assert_eq!(
            decoded["skills"]["config"][0]["path"],
            "/fixture/quotes\"and\\slash/SKILL.md"
        );
        assert_eq!(decoded["mcp_servers"].as_object().unwrap().len(), 1);
    }

    #[test]
    fn rejects_collisions_and_paths_that_do_not_match_native_inventory() {
        let names = BTreeSet::from(["same".into()]);
        assert!(known_source_denials(&names, &names, &[], ProfilePurpose::Interactive).is_err());
        for path in [
            "relative/SKILL.md",
            "/fixture/skill",
            "/fixture/../other/SKILL.md",
        ] {
            assert!(known_source_denials(
                &BTreeSet::new(),
                &BTreeSet::new(),
                &[KnownSkill {
                    path: path.into(),
                    origin: SkillOrigin::Ambient
                }],
                ProfilePurpose::Ephemeral
            )
            .is_err());
        }
    }

    #[test]
    #[ignore = "requires Linux, bwrap and Codex 0.160.0 at INTENT_CODEX_FIXTURE_RUNTIME"]
    fn generated_controls_pass_native_loader_fixture() {
        let runtime =
            std::env::var("INTENT_CODEX_FIXTURE_RUNTIME").expect("set Codex 0.160.0 executable");
        let mut skills: Vec<_> = [
            "/__fixture__/home/.agents/skills/home-sentinel/SKILL.md",
            "/__fixture__/repo/.agents/skills/ancestor-sentinel/SKILL.md",
            "/__fixture__/repo/nested/.agents/skills/project-sentinel/SKILL.md",
            "/etc/codex/skills/admin-sentinel/SKILL.md",
            "/__fixture__/codex/plugins/cache/fixture/sentinel/1.0.0/skills/plugin-sentinel/SKILL.md",
        ].into_iter().map(|p| KnownSkill { path:p.into(), origin:SkillOrigin::Ambient }).collect();
        for name in [
            "imagegen",
            "openai-docs",
            "review-agent",
            "skill-creator",
            "skill-installer",
        ] {
            skills.push(KnownSkill {
                path: format!("/__fixture__/codex/skills/.system/{name}/SKILL.md").into(),
                origin: SkillOrigin::Bundled,
            });
        }
        let names = ["home.with.dot", "ancestor", "project", "admin"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let controls =
            known_source_denials(&names, &BTreeSet::new(), &skills, ProfilePurpose::Ephemeral)
                .unwrap();
        let interactive = known_source_denials(
            &names,
            &BTreeSet::new(),
            &skills,
            ProfilePurpose::Interactive,
        )
        .unwrap();
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("controls.json");
        std::fs::write(
            &file,
            json!({"runtime_args":controls.runtime_args(), "interactive_runtime_args":interactive.runtime_args()}).to_string(),
        )
        .unwrap();
        let output = std::process::Command::new("python3")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("src/provider_profile/fixtures/codex-controls.py"),
            )
            .arg(runtime)
            .arg(file)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "Codex loader fixture failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("not universal isolation"));
    }
}
