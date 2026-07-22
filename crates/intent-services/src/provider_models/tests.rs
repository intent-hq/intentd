//! Unit tests for the provider-model parsers (canned adapter payloads only,
//! no network) and the codex probe's isolated-`CODEX_HOME` construction.

use serde_json::json;

use super::finish;
use super::parse::{
    is_auth_required_error, parse_acp_models, parse_codex_acp_models, parse_opencode_models,
};
use super::probe::{exit_attribution, ProbeError};

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
fn parse_acp_models_from_claude_code_config_options() {
    // Canned from a live claude-agent-acp@0.60.0 session/new result
    // (2026-07-21): models live in configOptions[id="model"].options; the
    // sibling mode/effort/fast select options must be ignored, and values are
    // preserved verbatim (including effort-suffixed ids like "opus[1m]").
    let payload = json!({
        "sessionId": "sess_1",
        "modes": { "currentModeId": "acceptEdits", "availableModes": [] },
        "configOptions": [
            { "id": "mode", "name": "Mode", "category": "mode", "type": "select",
              "currentValue": "acceptEdits",
              "options": [ { "value": "auto", "name": "Auto" },
                           { "value": "acceptEdits", "name": "Accept Edits" } ] },
            { "id": "model", "name": "Model", "description": "AI model to use",
              "category": "model", "type": "select", "currentValue": "opus[1m]",
              "options": [
                { "value": "default", "name": "Default (recommended)",
                  "description": "Opus 4.8 with 1M context · Best for everyday, complex tasks" },
                { "value": "opus[1m]", "name": "Opus",
                  "description": "Opus 4.8 with 1M context · Best for everyday, complex tasks" },
                { "value": "claude-fable-5[1m]", "name": "Fable",
                  "description": "Fable 5 · Most capable for your hardest and longest-running tasks" },
                { "value": "sonnet", "name": "Sonnet",
                  "description": "Sonnet 5 · Efficient for routine tasks" },
                { "value": "haiku", "name": "Haiku",
                  "description": "Haiku 4.5 · Fastest for quick answers" }
              ] },
            { "id": "effort", "name": "Effort", "category": "thought_level", "type": "select",
              "currentValue": "default",
              "options": [ { "value": "default", "name": "Default" },
                           { "value": "low", "name": "Low" } ] },
            { "id": "fast", "name": "Fast mode", "category": "model_config", "type": "select",
              "currentValue": "off",
              "options": [ { "value": "on", "name": "On" }, { "value": "off", "name": "Off" } ] }
        ]
    });
    let rows = parse_acp_models(&payload, "claude-code");
    let ids: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(
        ids,
        [
            "default",
            "opus[1m]",
            "claude-fable-5[1m]",
            "sonnet",
            "haiku"
        ]
    );
    assert_eq!(
        rows[0],
        json!({ "id": "default", "name": "Default (recommended)", "provider": "claude-code",
                "description": "Opus 4.8 with 1M context · Best for everyday, complex tasks" })
    );
    assert_eq!(rows[2]["name"], "Fable");
    assert!(rows.iter().all(|r| r["provider"] == "claude-code"));
}

#[test]
fn parse_config_options_wrapped_in_session_update() {
    let wrapped = json!({
        "update": {
            "configOptions": [
                { "id": "model", "options": [ { "value": "sonnet", "name": "Sonnet" } ] }
            ]
        }
    });
    let rows = parse_acp_models(&wrapped, "claude-code");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "sonnet");
}

#[test]
fn parse_config_options_falls_back_to_model_category() {
    let payload = json!({
        "configOptions": [
            { "id": "primary-model", "category": "model",
              "options": [ { "value": "m1", "name": "M1" } ] }
        ]
    });
    assert_eq!(parse_acp_models(&payload, "claude-code").len(), 1);
}

#[test]
fn parse_config_options_without_model_entry_yields_no_rows() {
    // Only non-model select options: extraction must not grab mode values.
    let no_model = json!({
        "configOptions": [
            { "id": "mode", "category": "mode",
              "options": [ { "value": "auto", "name": "Auto" } ] }
        ]
    });
    assert!(parse_acp_models(&no_model, "claude-code").is_empty());

    let empty_options = json!({
        "configOptions": [ { "id": "model", "options": [] } ]
    });
    assert!(parse_acp_models(&empty_options, "claude-code").is_empty());
}

#[test]
fn parse_config_options_id_match_wins_over_category() {
    // When one option matches by id and a different one by category, the id
    // match takes precedence.
    let payload = json!({
        "configOptions": [
            { "category": "model", "options": [ { "value": "by-category", "name": "C" } ] },
            { "id": "model", "options": [ { "value": "by-id", "name": "I" } ] }
        ]
    });
    let rows = parse_acp_models(&payload, "claude-code");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "by-id");
}

#[test]
fn parse_config_options_id_without_options_falls_back_to_category_sibling() {
    // An id == "model" entry without a usable options array must not abort
    // the extraction; a category == "model" sibling with options still wins.
    let payload = json!({
        "configOptions": [
            { "id": "model" },
            { "id": "primary", "category": "model",
              "options": [ { "value": "m1", "name": "M1" } ] }
        ]
    });
    let rows = parse_acp_models(&payload, "claude-code");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "m1");
}

#[test]
fn parse_empty_available_models_still_reads_config_options() {
    // A transitional adapter emitting an empty availableModels alongside a
    // populated configOptions catalog must not short-circuit to zero models.
    let payload = json!({
        "models": { "availableModels": [] },
        "configOptions": [
            { "id": "model", "options": [ { "value": "sonnet", "name": "Sonnet" } ] }
        ]
    });
    let rows = parse_acp_models(&payload, "claude-code");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "sonnet");
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
fn parse_codex_models_from_config_options() {
    // Canned from a live codex-acp@0.16.0 session/new result (2026-07-21):
    // same configOptions[id="model"].options shape as claude-code. None of
    // these ids are effort-variant base models, so no expansion happens.
    let payload = json!({
        "sessionId": "sess_2",
        "modes": { "currentModeId": "auto", "availableModes": [] },
        "configOptions": [
            { "id": "mode", "name": "Approval Preset", "category": "mode", "type": "select",
              "currentValue": "auto",
              "options": [ { "value": "read-only", "name": "Read Only" },
                           { "value": "auto", "name": "Default" } ] },
            { "id": "model", "name": "Model",
              "description": "Choose which model Codex should use",
              "category": "model", "type": "select", "currentValue": "gpt-5.6-sol",
              "options": [
                { "value": "gpt-5.6-sol", "name": "gpt-5.6-sol" },
                { "value": "gpt-5.5", "name": "GPT-5.5",
                  "description": "Frontier model for complex coding, research, and real-world work." },
                { "value": "gpt-5.4", "name": "GPT-5.4",
                  "description": "Strong model for everyday coding." },
                { "value": "gpt-5.4-mini", "name": "GPT-5.4-Mini",
                  "description": "Small, fast, and cost-efficient model for simpler coding tasks." },
                { "value": "gpt-5.3-codex-spark", "name": "GPT-5.3-Codex-Spark",
                  "description": "Ultra-fast coding model." }
              ] }
        ]
    });
    let rows = parse_codex_acp_models(&payload);
    let ids: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(
        ids,
        [
            "gpt-5.6-sol",
            "gpt-5.5",
            "gpt-5.4",
            "gpt-5.4-mini",
            "gpt-5.3-codex-spark"
        ]
    );
    assert_eq!(
        rows[1],
        json!({ "id": "gpt-5.5", "name": "GPT-5.5", "provider": "codex",
                "description": "Frontier model for complex coding, research, and real-world work." })
    );
}

#[test]
fn parse_codex_config_options_expand_effort_variants() {
    // Effort-variant base models expand even when reported via configOptions.
    let payload = json!({
        "configOptions": [
            { "id": "model",
              "options": [ { "value": "gpt-5.3-codex", "name": "GPT-5.3 Codex" } ] }
        ]
    });
    let rows = parse_codex_acp_models(&payload);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0]["id"], "gpt-5.3-codex/low");
    assert_eq!(rows[3]["id"], "gpt-5.3-codex/xhigh");
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
fn grok_outcome_maps_text_rows_to_wire_shape() {
    // Canned `grok models` text output: the shared intent-providers parser
    // extracts the rows; this seam maps them onto §5.30 wire rows.
    let fetch = super::grok_fetch_outcome(
        "You are logged in with grok.com.\ngrok-build  Grok Build  Default model\nopus-4-8  Opus 4.8",
        true,
        "",
    );
    let rows = fetch.models.expect("models present");
    assert!(fetch.warning.is_none());
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        json!({ "id": "grok-build", "name": "Grok Build", "provider": "grok",
                "description": "Default model" })
    );
    // description omitted (not null) when the CLI doesn't report one
    assert_eq!(
        rows[1],
        json!({ "id": "opus-4-8", "name": "Opus 4.8", "provider": "grok" })
    );
}

#[test]
fn grok_outcome_json_payload_wins_over_text_rows() {
    let fetch = super::grok_fetch_outcome(
        r#"{"models":{"availableModels":[{"modelId":"grok-4.5","name":"Grok 4.5","description":"Flagship"}]}}"#,
        true,
        "",
    );
    let rows = fetch.models.expect("models present");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        json!({ "id": "grok-4.5", "name": "Grok 4.5", "provider": "grok",
                "description": "Flagship" })
    );
}

#[test]
fn grok_outcome_logged_out_degrades_to_auth_required() {
    // An explicit logged-out marker wins even on exit 0 — the grok CLI exits
    // 0 in both auth states, so the exit code is never trusted.
    let fetch = super::grok_fetch_outcome(
        "You are not authenticated. Please log in with `grok login`.",
        true,
        "",
    );
    assert!(fetch.models.is_none());
    assert_eq!(
        fetch.warning.as_deref(),
        Some("grok: authentication required")
    );
}

#[test]
fn grok_outcome_empty_success_degrades_to_no_models() {
    let fetch = super::grok_fetch_outcome("", true, "");
    assert!(fetch.models.is_none());
    assert_eq!(fetch.warning.as_deref(), Some("grok: no models reported"));
}

#[test]
fn grok_outcome_failed_exit_without_rows_is_attributed() {
    let fetch = super::grok_fetch_outcome("", false, "grok: command crashed\n");
    assert!(fetch.models.is_none());
    let warning = fetch.warning.expect("warning present");
    assert!(
        warning.starts_with("grok: grok models exited with an error"),
        "{warning}"
    );
    assert!(warning.contains("command crashed"), "{warning}");
}

#[test]
fn grok_outcome_rows_win_over_failed_exit() {
    // Parsed rows with a non-zero exit still serve the catalog — stdout is
    // the contract, not the exit code.
    let fetch = super::grok_fetch_outcome("grok-build  Grok Build", false, "noise");
    let rows = fetch.models.expect("models present");
    assert_eq!(rows[0]["id"], "grok-build");
}

#[cfg(unix)]
#[tokio::test]
async fn opencode_models_cli_child_path_includes_binary_dir() {
    // A fake opencode whose success is gated on its own parent dir being on
    // the child's $PATH — the enhanced-path contract shared with the ACP
    // probe spawns. The temp dir is not on the process PATH, so the run only
    // succeeds when the spawn sets the child's PATH explicitly.
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("opencode");
    let script = format!(
        "#!/bin/sh\ncase \":$PATH:\" in\n  *\":{dir}:\"*) printf '%s\\n' 'anthropic/claude-3' ;;\n  *) exit 1 ;;\nesac\n",
        dir = dir.path().display(),
    );
    std::fs::write(&bin, script).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let stdout = super::run_opencode_models_cli(bin, super::OPENCODE_CLI_TIMEOUT)
        .await
        .expect("exit 0 when the child PATH carries the binary dir");
    assert!(stdout.contains("anthropic/claude-3"));
}

#[cfg(unix)]
#[tokio::test]
async fn grok_models_cli_child_path_includes_binary_dir() {
    // Same enhanced-path contract as the opencode CLI spawn: the fake grok
    // only succeeds when its own parent dir is on the child's $PATH.
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("grok");
    let script = format!(
        "#!/bin/sh\ncase \":$PATH:\" in\n  *\":{dir}:\"*) printf '%s\\n' 'grok-build  Grok Build' ;;\n  *) exit 1 ;;\nesac\n",
        dir = dir.path().display(),
    );
    std::fs::write(&bin, script).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let output = super::run_grok_models_cli(bin, super::GROK_CLI_TIMEOUT)
        .await
        .expect("spawn succeeds");
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("grok-build"));
}

#[cfg(unix)]
#[tokio::test]
async fn grok_cli_timeout_flows_into_attributed_warning() {
    // A wedged `grok models` must be cut short and the timeout reason must
    // surface through the fetch attribution (`grok: ...`).
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("grok");
    std::fs::write(&bin, "#!/bin/sh\nsleep 30\n").unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let start = std::time::Instant::now();
    let fetch = super::ProviderModelsFetch::unavailable(
        "grok",
        super::run_grok_models_cli(bin, std::time::Duration::from_millis(100))
            .await
            .unwrap_err(),
    );
    assert!(
        start.elapsed() < std::time::Duration::from_secs(5),
        "timeout must cut the wedged CLI short"
    );
    assert!(fetch.models.is_none());
    assert_eq!(
        fetch.warning.as_deref(),
        Some("grok: grok models timed out")
    );
}

#[test]
fn isolated_codex_home_seeds_auth_but_never_mcp_servers() {
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
    // No allowlisted scalar keys ⇒ no config.toml at all; mcp_servers never
    // reaches the probe home.
    assert!(!home.path().join("config.toml").exists());

    let probe_home = home.path().to_path_buf();
    drop(home);
    assert!(!probe_home.exists());
}

#[test]
fn isolated_codex_home_seeds_only_allowlisted_config_scalars() {
    let user = tempfile::tempdir().unwrap();
    std::fs::write(
        user.path().join("config.toml"),
        concat!(
            "model = \"gpt-5.6-sol\"\n",
            "model_reasoning_effort = \"high\"\n",
            "sandbox_mode = \"danger-full-access\"\n",
            "[mcp_servers.codebase-retrieval]\n",
            "command = \"auggie\"\n",
        ),
    )
    .unwrap();

    let home = super::isolated_codex_home(Some(user.path())).unwrap();
    let seeded = std::fs::read_to_string(home.path().join("config.toml")).unwrap();
    let doc: toml_edit::DocumentMut = seeded.parse().unwrap();
    assert_eq!(doc["model"].as_str(), Some("gpt-5.6-sol"));
    assert_eq!(doc["model_reasoning_effort"].as_str(), Some("high"));
    assert_eq!(doc.as_table().len(), 2);
    assert!(doc.get("mcp_servers").is_none());
    assert!(doc.get("sandbox_mode").is_none());
}

#[test]
fn isolated_codex_home_seeds_model_without_effort() {
    let user = tempfile::tempdir().unwrap();
    std::fs::write(user.path().join("config.toml"), "model = \"gpt-5.6-sol\"\n").unwrap();

    let home = super::isolated_codex_home(Some(user.path())).unwrap();
    let seeded = std::fs::read_to_string(home.path().join("config.toml")).unwrap();
    let doc: toml_edit::DocumentMut = seeded.parse().unwrap();
    assert_eq!(doc["model"].as_str(), Some("gpt-5.6-sol"));
    assert_eq!(doc.as_table().len(), 1);
}

#[test]
fn isolated_codex_home_seeds_effort_without_model() {
    let user = tempfile::tempdir().unwrap();
    std::fs::write(
        user.path().join("config.toml"),
        "model_reasoning_effort = \"high\"\n",
    )
    .unwrap();

    let home = super::isolated_codex_home(Some(user.path())).unwrap();
    let seeded = std::fs::read_to_string(home.path().join("config.toml")).unwrap();
    let doc: toml_edit::DocumentMut = seeded.parse().unwrap();
    assert_eq!(doc["model_reasoning_effort"].as_str(), Some("high"));
    assert_eq!(doc.as_table().len(), 1);
}

#[test]
fn isolated_codex_home_skips_non_string_allowlisted_keys() {
    let user = tempfile::tempdir().unwrap();
    std::fs::write(
        user.path().join("config.toml"),
        "[model]\nnested = \"not-a-scalar\"\n",
    )
    .unwrap();

    let home = super::isolated_codex_home(Some(user.path())).unwrap();
    assert!(!home.path().join("config.toml").exists());
}

#[test]
fn isolated_codex_home_tolerates_malformed_config() {
    let user = tempfile::tempdir().unwrap();
    std::fs::write(user.path().join("config.toml"), "model = [unclosed\n").unwrap();
    std::fs::write(user.path().join("auth.json"), "{\"tokens\":{}}").unwrap();

    let home = super::isolated_codex_home(Some(user.path())).unwrap();
    assert!(home.path().join("auth.json").is_file());
    assert!(!home.path().join("config.toml").exists());
}

#[test]
fn isolated_codex_home_tolerates_absent_config() {
    let user = tempfile::tempdir().unwrap();
    std::fs::write(user.path().join("auth.json"), "{\"tokens\":{}}").unwrap();

    let home = super::isolated_codex_home(Some(user.path())).unwrap();
    assert!(home.path().join("auth.json").is_file());
    assert!(!home.path().join("config.toml").exists());
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
fn probe_ok_but_zero_models_degrades_with_warning() {
    // A successful handshake that reports zero models must still degrade to
    // an unavailable fetch with a provider-attributed warning.
    let fetch = finish("claude-code", Err(ProbeError::Empty));
    assert!(fetch.models.is_none());
    assert_eq!(
        fetch.warning.as_deref(),
        Some("claude-code: no models reported")
    );
}

#[test]
fn early_adapter_exit_is_surfaced_in_warning() {
    // A dead adapter (e.g. corrupt npx cache → ENOENT) must be attributed in
    // the warning text instead of a generic "no models reported".
    let fetch = finish(
        "codex",
        Err(ProbeError::Exited(
            "exit status: 1; stderr: npm error enoent ENOENT: no such file or directory"
                .to_string(),
        )),
    );
    assert!(fetch.models.is_none());
    let warning = fetch.warning.expect("warning present");
    assert!(warning.starts_with("codex: adapter exited before reporting models"));
    assert!(warning.contains("enoent"));
}

#[cfg(unix)]
fn exit_status(code: i32) -> std::process::ExitStatus {
    use std::os::unix::process::ExitStatusExt;
    std::process::ExitStatus::from_raw(code << 8)
}

#[cfg(unix)]
#[test]
fn exit_attribution_rewrites_generic_errors_on_unsuccessful_exit() {
    let err = exit_attribution(
        ProbeError::Timeout,
        Some(exit_status(1)),
        &[
            "npm error enoent ENOENT: no such file or directory".to_string(),
            "npm error A complete log of this run can be found in: /tmp/log".to_string(),
        ],
    );
    let ProbeError::Exited(detail) = err else {
        panic!("expected Exited, got {err}");
    };
    // The tail must include the actual cause, not just npm's final log-path line.
    assert!(detail.contains("ENOENT"), "detail: {detail}");
    assert!(detail.contains("complete log"), "detail: {detail}");
}

#[cfg(unix)]
#[test]
fn exit_attribution_passes_through_spawn_rpc_clean_exit_and_live_child() {
    // Rpc must survive a dead child: auth detection keys off it.
    let rpc = ProbeError::Rpc(intent_acp::JsonRpcError {
        code: -32000,
        message: "auth required".to_string(),
        data: None,
    });
    assert!(matches!(
        exit_attribution(rpc, Some(exit_status(1)), &[]),
        ProbeError::Rpc(_)
    ));
    assert!(matches!(
        exit_attribution(
            ProbeError::Spawn("nope".to_string()),
            Some(exit_status(1)),
            &[]
        ),
        ProbeError::Spawn(_)
    ));
    // A clean exit after an empty handshake is genuinely "no models reported".
    assert!(matches!(
        exit_attribution(ProbeError::Empty, Some(exit_status(0)), &[]),
        ProbeError::Empty
    ));
    // A still-running (slow) child must not be reported as exited.
    assert!(matches!(
        exit_attribution(ProbeError::Timeout, None, &[]),
        ProbeError::Timeout
    ));
}

#[cfg(unix)]
#[tokio::test]
async fn probe_reports_exit_status_and_stderr_for_crashing_adapter() {
    use super::probe::{run_acp_probe, AcpProbeCommand};
    let cmd = AcpProbeCommand::binary(
        "/bin/sh".into(),
        vec!["-c".to_string(), "echo boom >&2; exit 7".to_string()],
    );
    let err = run_acp_probe(cmd, |_| Vec::new()).await.unwrap_err();
    let ProbeError::Exited(detail) = err else {
        panic!("expected Exited, got {err}");
    };
    assert!(detail.contains("boom"), "detail: {detail}");
}

#[cfg(unix)]
#[tokio::test]
async fn probe_rpc_error_survives_dead_child() {
    use super::probe::{run_acp_probe, AcpProbeCommand};
    // Respond to the initialize request (id 1) with a JSON-RPC error, then
    // exit non-zero: the Rpc error must pass through exit attribution so
    // auth detection still sees it.
    let script = r#"read line; printf '{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"Authentication required"}}\n'; exit 1"#;
    let cmd = AcpProbeCommand::binary("/bin/sh".into(), vec!["-c".to_string(), script.to_string()]);
    let err = run_acp_probe(cmd, |_| Vec::new()).await.unwrap_err();
    let ProbeError::Rpc(rpc) = err else {
        panic!("expected Rpc, got {err}");
    };
    assert_eq!(rpc.message, "Authentication required");
}

#[cfg(unix)]
#[tokio::test]
async fn opencode_cli_timeout_kills_child_and_reports_timeout() {
    // A wedged `opencode models` must be reaped when the timeout elapses and
    // the failure must be attributable as a timeout. The fake CLI records its
    // PID first thing, then sleeps far past the injected timeout — a 500ms
    // budget leaves slow runners ample time to write the PID file before the
    // deadline while keeping the test fast.
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let pid_file = dir.path().join("pid");
    let bin = dir.path().join("opencode");
    std::fs::write(
        &bin,
        format!("#!/bin/sh\necho $$ > '{}'\nsleep 30\n", pid_file.display()),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();

    let start = std::time::Instant::now();
    let err = super::run_opencode_models_cli(bin, std::time::Duration::from_millis(500))
        .await
        .unwrap_err();
    assert!(
        start.elapsed() < std::time::Duration::from_secs(5),
        "timeout must cut the wedged CLI short"
    );
    assert_eq!(err, "opencode models timed out");

    // kill_on_drop reaps the child when the timed-out output future drops:
    // signal `None` (sig 0) probes liveness without touching the process.
    // (A recycled PID within the 5s window would false-positive the probe;
    // accepted as vanishingly unlikely.)
    let pid: i32 = std::fs::read_to_string(&pid_file)
        .expect("fake CLI must have started")
        .trim()
        .parse()
        .expect("pid");
    let pid = nix::unistd::Pid::from_raw(pid);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while nix::sys::signal::kill(pid, None).is_ok() {
        assert!(
            std::time::Instant::now() < deadline,
            "child {pid} must be killed after the timeout"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn opencode_timeout_flows_into_attributed_warning() {
    // The timeout reason must surface through the fetch result attribution
    // (`opencode: ...`), matching what models.list callers see.
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("opencode");
    std::fs::write(&bin, "#!/bin/sh\nsleep 30\n").unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let fetch = super::ProviderModelsFetch::unavailable(
        "opencode",
        super::run_opencode_models_cli(bin, std::time::Duration::from_millis(100))
            .await
            .unwrap_err(),
    );
    assert!(fetch.models.is_none());
    assert_eq!(
        fetch.warning.as_deref(),
        Some("opencode: opencode models timed out")
    );
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
