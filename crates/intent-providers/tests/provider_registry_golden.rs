//! The one reviewed copy of each provider's npm packages, login hints, CLI
//! requirement and CLI version gate (`tests/goldens/provider_registry.json`). Other crates read
//! these facts from the registry instead of restating them, so changing one
//! (intentd#3577 gave Auggie an npm package) is a single golden diff here.

use std::path::Path;

use intent_providers::adapter_cli::requirement;
use intent_providers::installed_cli::InstalledCli;
use intent_providers::{cli_version_gate, ProviderConfig, ACP_PROVIDERS};
use serde_json::{json, Value};

const GOLDEN_REL: &str = "tests/goldens/provider_registry.json";
const REGENERATE: &str =
    "INTENTD_UPDATE_GOLDENS=1 cargo test -p intent-providers --test provider_registry_golden";

/// Adapters whose pin, and the adapter metadata derived from it, the
/// auto-pin-acp-adapters workflow bumps (`scripts/auto-pin-acp-adapter.py`
/// `ADAPTERS`). That workflow may change nothing else, so their versions are
/// recorded as [`AUTO_PINNED`] and checked to be exact pins instead.
const AUTO_PINNED_PROVIDERS: &[&str] = &["codex", "claude-code"];
const AUTO_PINNED: &str = "{auto-pinned}";

fn exact_version(version: &str) -> bool {
    let parts: Vec<&str> = version.split('.').collect();
    parts.len() == 3
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
}

/// `name@version` with an auto-pinned version replaced by [`AUTO_PINNED`].
#[track_caller]
fn package(spec: &str, auto_pinned: bool) -> String {
    if !auto_pinned {
        return spec.to_owned();
    }
    let (name, version) = spec.rsplit_once('@').expect("package spec has a version");
    assert!(exact_version(version), "{spec} must pin an exact version");
    format!("{name}@{AUTO_PINNED}")
}

fn cli_requirement(id: &str, auto_pinned: bool) -> Value {
    let Some(required) = requirement(id) else {
        return Value::Null;
    };
    let bundled = required
        .bundled
        .map(|cli| package(&cli.spec(), auto_pinned));
    let (range, minimum) = if auto_pinned {
        (AUTO_PINNED.to_owned(), AUTO_PINNED.to_owned())
    } else {
        (required.range, required.minimum.to_string())
    };
    json!({
        "command": required.command,
        "range": range,
        "minimum": minimum,
        "bundled": bundled,
    })
}

fn npx_login_command_hint(p: &ProviderConfig, auto_pinned: bool) -> Option<String> {
    let hint = p.npx_login_command_hint()?;
    let Some(rest) = hint.strip_prefix("npx -y ") else {
        return Some(hint);
    };
    let (spec, args) = rest.split_once(' ').unwrap_or((rest, ""));
    Some(
        format!("npx -y {} {args}", package(spec, auto_pinned))
            .trim_end()
            .to_owned(),
    )
}

fn provider_row(p: &ProviderConfig) -> Value {
    let auto_pinned = AUTO_PINNED_PROVIDERS.contains(&p.id);
    json!({
        "id": p.id,
        "command": p.command,
        "fallbackNpxPackage": p.fallback_npx_package.map(|s| package(s, auto_pinned)),
        "npxOnlyPackage": p.npx_only_package.map(|s| package(s, auto_pinned)),
        "npxOnlyHonorsPathOverride": p.npx_only_honors_path_override,
        "loginCommandHint": p.login_command_hint,
        "npxLoginCommandHint": npx_login_command_hint(p, auto_pinned),
        "requiresSecondaryBinary": p.requires_secondary_binary,
        "installedCli": InstalledCli::for_provider(p.id).map(InstalledCli::command),
        "cliRequirement": cli_requirement(p.id, auto_pinned),
        "cliVersionGate": cli_version_gate(p.id).map(|gate| json!({
            "command": gate.command,
            "minimum": gate.min_version,
            "requirement": gate.requirement,
        })),
        "profile": p.profile.map(|spec| json!({
            "credentialKeys": spec.credential_keys,
            "accountDirKeys": spec.account_dir_keys,
            "apiKeyHome": spec.api_key_home.map(|home| json!({
                "key": home.key,
                "homeKey": home.home_key,
                "dirName": home.dir_name,
                "launchEnv": home.launch_env.iter().map(|(k, v)| json!([k, v])).collect::<Vec<_>>(),
            })),
            "credentialLaunchEnv": spec.credential_launch_env.iter().map(|(k, v)| json!([k, v])).collect::<Vec<_>>(),
            "reservedKeys": spec.reserved_keys.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
        })),
    })
}

fn render() -> String {
    let providers: Vec<Value> = ACP_PROVIDERS.iter().map(provider_row).collect();
    let golden = json!({
        "note": format!(
            "Generated; do not hand-edit. Regenerate: {REGENERATE}. {AUTO_PINNED} marks a version the auto-pin-acp-adapters workflow owns."
        ),
        "providers": providers,
    });
    format!("{}\n", serde_json::to_string_pretty(&golden).unwrap())
}

#[test]
fn provider_registry_matches_golden() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(GOLDEN_REL);
    let expected = render();
    if std::env::var("INTENTD_UPDATE_GOLDENS").is_ok_and(|v| v == "1") {
        std::fs::write(&path, &expected).expect("write provider registry golden");
        return;
    }
    let actual = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| {
            panic!(
                "cannot read {}: {e}. Generate it with:\n  {REGENERATE}",
                path.display()
            )
        })
        .replace("\r\n", "\n");
    if actual == expected {
        return;
    }
    panic!(
        "Provider registry facts differ from {GOLDEN_REL}:\n{}\nIf the change is intended, regenerate and review the diff:\n  {REGENERATE}",
        describe_drift(&actual, &expected)
    );
}

fn describe_drift(actual: &str, expected: &str) -> String {
    let rows = |text: &str| -> Vec<Value> {
        serde_json::from_str::<Value>(text)
            .ok()
            .and_then(|v| v["providers"].as_array().cloned())
            .unwrap_or_default()
    };
    let (golden, registry) = (rows(actual), rows(expected));
    let mut lines = Vec::new();
    for row in &registry {
        let id = &row["id"];
        let Some(old) = golden.iter().find(|g| &g["id"] == id) else {
            lines.push(format!("  {id}: not in the golden"));
            continue;
        };
        for (field, value) in row.as_object().into_iter().flatten() {
            if old.get(field) != Some(value) {
                lines.push(format!(
                    "  {id}.{field}: golden {} -> registry {value}",
                    old.get(field).unwrap_or(&Value::Null)
                ));
            }
        }
    }
    for row in &golden {
        if !registry.iter().any(|r| r["id"] == row["id"]) {
            lines.push(format!("  {}: no longer in the registry", row["id"]));
        }
    }
    if lines.is_empty() {
        lines.push("  (formatting or order only)".into());
    }
    lines.join("\n")
}
