//! Unit tests for the provider-model parsers (canned adapter payloads only,
//! no network) and the codex probe's isolated-`CODEX_HOME` construction.

use serde_json::json;

use super::parse::{
    is_auth_required_error, parse_acp_models, parse_codex_acp_models, parse_opencode_models,
};

#[test]
fn parse_acp_models_from_session_new_result() {
    // claude-code / droid shape: models.availableModels in the session/new result.
    let payload = json!({
        "sessionId": "sess_1",
        "models": {
            "availableModels": [
                { "modelId": "claude-sonnet-4-5", "name": "Claude Sonnet 4.5",
                  "description": "Balanced" },
                { "modelId": "claude-opus-4-1", "name": "Claude Opus 4.1" }
            ],
            "currentModelId": "claude-sonnet-4-5"
        }
    });
    let rows = parse_acp_models(&payload, "claude-code");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        json!({ "id": "claude-sonnet-4-5", "name": "Claude Sonnet 4.5",
                "provider": "claude-code", "description": "Balanced" })
    );
    // description omitted (not null) when the adapter doesn't report one
    assert_eq!(
        rows[1],
        json!({ "id": "claude-opus-4-1", "name": "Claude Opus 4.1",
                "provider": "claude-code" })
    );
}

#[test]
fn parse_acp_models_normalizes_pi_payload_shapes() {
    // pi shape 1: bare availableModels
    let bare = json!({ "availableModels": [ { "modelId": "m1", "name": "M1" } ] });
    assert_eq!(parse_acp_models(&bare, "pi").len(), 1);

    // pi shape 2: models.available
    let available = json!({ "models": { "available": [ { "modelId": "m2" } ] } });
    let rows = parse_acp_models(&available, "pi");
    assert_eq!(rows.len(), 1);
    // name falls back to the id
    assert_eq!(
        rows[0],
        json!({ "id": "m2", "name": "m2", "provider": "pi" })
    );

    // pi shape 3: wrapped under `update` (session/update notification)
    let wrapped = json!({
        "update": { "models": { "availableModels": [ { "modelId": "m3", "name": "M3" } ] } }
    });
    assert_eq!(parse_acp_models(&wrapped, "pi").len(), 1);

    // wrapped under `sessionUpdate`
    let wrapped2 = json!({
        "sessionUpdate": { "availableModels": [ { "modelId": "m4" } ] }
    });
    assert_eq!(parse_acp_models(&wrapped2, "pi").len(), 1);
}

#[test]
fn parse_acp_models_tolerates_field_aliases_and_skips_bad_entries() {
    let payload = json!({
        "models": {
            "availableModels": [
                { "id": "a", "displayName": "A" },
                { "value": "b", "label": "B" },
                { "name": "no-id-entry" },
                { "modelId": "   " },
                42
            ]
        }
    });
    let rows = parse_acp_models(&payload, "droid");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], "a");
    assert_eq!(rows[0]["name"], "A");
    assert_eq!(rows[1]["id"], "b");
    assert_eq!(rows[1]["name"], "B");
}

#[test]
fn parse_acp_models_empty_payloads_yield_no_rows() {
    assert!(parse_acp_models(&json!({}), "pi").is_empty());
    assert!(parse_acp_models(&json!(null), "pi").is_empty());
    assert!(parse_acp_models(&json!({ "models": { "availableModels": [] } }), "pi").is_empty());
}

#[test]
fn parse_codex_models_expands_effort_variants() {
    let payload = json!({
        "models": {
            "available": [
                { "modelId": "gpt-5.3-codex", "name": "GPT-5.3 Codex",
                  "description": "Flagship coding model" },
                { "modelId": "gpt-5.4", "name": "GPT-5.4" }
            ]
        }
    });
    let rows = parse_codex_acp_models(&payload);
    // 4 effort variants + 1 bare model
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0]["id"], "gpt-5.3-codex/low");
    assert_eq!(rows[0]["name"], "GPT-5.3 Codex (Low)");
    assert_eq!(
        rows[0]["description"],
        "Flagship coding model — faster responses with less deliberation"
    );
    assert_eq!(rows[3]["id"], "gpt-5.3-codex/xhigh");
    assert_eq!(rows[3]["name"], "GPT-5.3 Codex (Xhigh)");
    assert_eq!(
        rows[4],
        json!({ "id": "gpt-5.4", "name": "GPT-5.4", "provider": "codex" })
    );
    assert!(rows.iter().all(|r| r["provider"] == "codex"));
}

#[test]
fn parse_codex_models_effort_variant_without_description() {
    let payload = json!({ "models": { "availableModels": [ { "modelId": "gpt-5.2-codex" } ] } });
    let rows = parse_codex_acp_models(&payload);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[1]["id"], "gpt-5.2-codex/medium");
    assert_eq!(rows[1]["description"], "Balanced speed and reasoning depth");
}

#[test]
fn parse_opencode_models_one_provider_model_per_line() {
    let stdout = "\
INFO loading providers
anthropic/claude-sonnet-4
openai/gpt-5.2
openai/o4-mini
not-a-model-line
/leading-slash
trailing-slash/
";
    let rows = parse_opencode_models(stdout);
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        json!({ "id": "anthropic/claude-sonnet-4", "name": "Anthropic Claude Sonnet 4",
                "provider": "opencode" })
    );
    assert_eq!(rows[1]["id"], "openai/gpt-5.2");
    assert_eq!(rows[1]["name"], "Openai Gpt 5.2");
    assert_eq!(rows[2]["id"], "openai/o4-mini");
    assert_eq!(rows[2]["name"], "Openai O4 Mini");
}

#[test]
fn parse_opencode_models_nested_model_ids_keep_full_value() {
    // Model ids may themselves contain '/'; only the first '/' splits provider.
    let rows = parse_opencode_models("bedrock/anthropic/claude-3\n");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "bedrock/anthropic/claude-3");
    assert_eq!(rows[0]["name"], "Bedrock Anthropic/claude 3");
}

#[test]
fn parse_opencode_models_empty_output() {
    assert!(parse_opencode_models("").is_empty());
    assert!(parse_opencode_models("no models\n").is_empty());
}

#[test]
fn isolated_codex_home_seeds_auth_but_never_config() {
    let user = tempfile::tempdir().unwrap();
    std::fs::write(
        user.path().join("config.toml"),
        "[mcp_servers.codebase-retrieval]\ncommand = \"auggie\"\n",
    )
    .unwrap();
    std::fs::write(user.path().join("auth.json"), "{\"tokens\":{}}").unwrap();

    let home = super::isolated_codex_home(Some(user.path())).unwrap();
    assert!(home.path().is_dir());
    assert_ne!(home.path(), user.path());
    assert!(home.path().join("auth.json").is_file());
    assert!(!home.path().join("config.toml").exists());

    let probe_home = home.path().to_path_buf();
    drop(home);
    assert!(!probe_home.exists());
}

#[test]
fn isolated_codex_home_without_user_dir_is_empty() {
    let home = super::isolated_codex_home(None).unwrap();
    assert!(home.path().is_dir());
    assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);
}

#[test]
fn codex_probe_command_env_carries_isolated_codex_home() {
    let cmd = super::probe::AcpProbeCommand::binary(std::path::PathBuf::from("codex-acp"), vec![]);
    let (cmd, home) = super::codex_probe_with_isolated_home(cmd).unwrap();
    let envs = cmd.env_vars();
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].0, "CODEX_HOME");
    assert_eq!(std::path::PathBuf::from(&envs[0].1), home.path());
    if let Some(user_dir) = super::user_codex_dir() {
        assert_ne!(std::path::PathBuf::from(&envs[0].1), user_dir);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn acp_probe_child_receives_env_overrides() {
    let out = tempfile::tempdir().unwrap();
    let out_file = out.path().join("codex_home.txt");
    let script = format!("printf %s \"$CODEX_HOME\" > '{}'", out_file.display());
    let cmd = super::probe::AcpProbeCommand::binary(
        std::path::PathBuf::from("/bin/sh"),
        vec!["-c".to_string(), script],
    )
    .env("CODEX_HOME", "/tmp/intentd-test-isolated-codex-home");

    // The shell exits immediately, so the handshake fails — the assertion is
    // only that the env override reached the child.
    let _ = super::probe::run_acp_probe(cmd, |_| Vec::new()).await;

    let recorded = std::fs::read_to_string(&out_file).unwrap();
    assert_eq!(recorded, "/tmp/intentd-test-isolated-codex-home");
}

#[test]
fn auth_required_detection() {
    assert!(is_auth_required_error(401, "whatever"));
    assert!(is_auth_required_error(-32000, "Authentication required"));
    assert!(is_auth_required_error(-32000, "auth_required"));
    assert!(is_auth_required_error(-32000, "you are not logged in"));
    assert!(is_auth_required_error(-32000, "Not authenticated"));
    assert!(is_auth_required_error(-32000, "Unauthorized"));
    assert!(is_auth_required_error(-32000, "Please log in to continue"));
    assert!(is_auth_required_error(-32000, "please sign in first"));
    assert!(!is_auth_required_error(-32000, "internal error"));
    assert!(!is_auth_required_error(0, "model not found"));
}
