//! Shared native/ACP loader controls for Claude SDK 0.3.280 / CLI 2.1.280.
//!
//! These controls suppress native resource discovery, not filesystem access.
//! An empty settings-source list also disables native CLAUDE.md loading. The
//! caller must supply its resolved workspace instructions explicitly, and must
//! still acquire/enforce managed policy before launching. Managed MCP, hosted
//! connectors and other platforms need separate evidence; this is no certificate.

use std::path::Path;

use serde_json::{json, Value};

use super::{auth, ProfileError, ProfileResult};

/// Contains projected auth settings, so deliberately has no Debug implementation.
pub struct ClaudeLoaderControls {
    pub session_meta: Value,
    pub runtime_args: Vec<String>,
}

/// Build equivalent controls for native one-shots and ACP new/load requests.
/// `mcp_config` must name the caller's owned, policy-checked MCP configuration;
/// ephemeral callers write an empty `mcpServers` object there. An empty
/// `workspace_instructions` is an explicit caller decision, never a native fallback.
/// Do not append ambient flags/options after these authoritative controls.
///
/// # Errors
/// Invalid auth projection or a non-absolute/non-UTF8 owned config path fails.
pub fn loader_controls(
    native_settings: &Value,
    mcp_config: &Path,
    workspace_instructions: &str,
) -> ProfileResult<ClaudeLoaderControls> {
    if !mcp_config.is_absolute() {
        return Err(ProfileError::InvalidAuth(
            "Claude MCP profile path must be absolute",
        ));
    }
    let mcp_config = mcp_config.to_str().ok_or(ProfileError::Io)?;
    let mut settings = auth::project_claude_settings(native_settings)?;
    // Documented any-source-true setting. Applies to CLI/SDK-fetched connectors;
    // it is not a promise about servers injected by a separate desktop/cloud host.
    settings["disableClaudeAiConnectors"] = json!(true);
    let session_meta = json!({
        "systemPrompt": {"type":"preset", "preset":"claude_code", "append":workspace_instructions},
        "claudeCode": {"options": {
            "strictMcpConfig": true,
            "settingSources": [],
            "settings": settings,
            "extraArgs": {"disable-slash-commands":""}
        }}
    });
    let runtime_args = vec![
        "--strict-mcp-config".into(),
        "--mcp-config".into(),
        mcp_config.into(),
        "--setting-sources".into(),
        String::new(),
        "--settings".into(),
        settings.to_string(),
        "--disable-slash-commands".into(),
        "--append-system-prompt".into(),
        workspace_instructions.into(),
    ];
    Ok(ClaudeLoaderControls {
        session_meta,
        runtime_args,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_and_acp_controls_retain_only_projected_auth_and_explicit_instructions() {
        let controls = loader_controls(
            &json!({"model":"fixture-model", "env":{"ANTHROPIC_API_KEY":"literal-$key"},
                "hooks":{"SessionStart":[{"command":"forbidden"}]}, "disableClaudeAiConnectors":false}),
            Path::new("/owned/mcp.json"),
            "Workspace instructions\nwith literal $ and quotes \".",
        ).unwrap();
        let options = &controls.session_meta["claudeCode"]["options"];
        assert_eq!(options["strictMcpConfig"], true);
        assert_eq!(options["settingSources"], json!([]));
        assert_eq!(options["settings"]["disableClaudeAiConnectors"], true);
        assert!(options["settings"].get("hooks").is_none());
        assert_eq!(
            options["settings"]["env"]["ANTHROPIC_API_KEY"],
            "literal-$key"
        );
        assert_eq!(options["settings"]["model"], "fixture-model");
        let settings_index = controls
            .runtime_args
            .iter()
            .position(|s| s == "--settings")
            .unwrap();
        let native_settings: Value =
            serde_json::from_str(&controls.runtime_args[settings_index + 1]).unwrap();
        assert_eq!(native_settings, options["settings"]);
        assert_eq!(
            controls.runtime_args.last().unwrap(),
            controls.session_meta["systemPrompt"]["append"]
                .as_str()
                .unwrap()
        );
        // Resource exclusion must not silently change approval/tool/model semantics.
        assert!(options.get("tools").is_none());
        assert!(options.get("permissionMode").is_none());
    }

    #[test]
    fn rejects_relative_profile_path_without_echoing_sensitive_input() {
        let result = loader_controls(&json!({}), Path::new("secret/mcp.json"), "");
        let Err(error) = result else {
            panic!("relative profile accepted")
        };
        assert!(!error.to_string().contains("secret"));
    }

    #[test]
    #[ignore = "requires Linux x64, bwrap and pinned Claude packages at INTENT_CLAUDE_FIXTURE_MODULES"]
    fn generated_controls_pass_native_loader_fixture() {
        let modules = std::env::var("INTENT_CLAUDE_FIXTURE_MODULES")
            .expect("set pinned Claude node_modules directory");
        let controls = loader_controls(
            &json!({}),
            Path::new("/__fixture__/mcp.json"),
            "Fixture workspace instructions.",
        )
        .unwrap();
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("controls.json");
        std::fs::write(
            &file,
            json!({"session_meta": controls.session_meta, "runtime_args": controls.runtime_args})
                .to_string(),
        )
        .unwrap();
        let output = std::process::Command::new("node")
            .env_remove("NODE_OPTIONS")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("src/provider_profile/fixtures/claude-controls.mjs"),
            )
            .arg(modules)
            .arg(file)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "Claude loader fixture failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("managed-mcp.json rejects strict"));
    }
}
