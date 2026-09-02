//! Unit tests for the provider-model parsers (canned adapter payloads only,
//! no network) and the codex probe's isolated-`CODEX_HOME` construction.

use serde_json::json;

use super::finish;
use super::parse::{
    build_unsloth_rows, estimate_model_bytes, fits_within_ram, gguf_bytes_fit_within_ram,
    parse_hf_unsloth_response, parse_param_count_billions,
};
use super::parse::{
    is_auth_required_error, parse_acp_models, parse_codex_acp_models, parse_opencode_models,
};
use super::probe::{exit_attribution, ProbeError};

const GIB: u64 = 1024 * 1024 * 1024;

/// Serializes the child-spawning timeout/PID tests against each other and the
/// rest of the parallel suite. These tests exec a real fake CLI and depend on
/// the child being scheduled promptly (to write its PID file / hit the injected
/// timeout); under full-suite parallel load an unserialized child can be
/// starved past its budget, flaking the probe. `unwrap_or_else(into_inner)`
/// recovers from a poisoned lock so one panicking test does not cascade.
static CHILD_SPAWN_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Recorded (trimmed) response from
/// `https://huggingface.co/api/models?author=unsloth&filter=gguf&limit=1000`
/// (captured 2026-07-27), covering: a dense model (`Ornith-1.0-35B-GGUF`), an
/// `MoE` model whose name carries both total and active param counts
/// (`Qwen3.6-35B-A3B-GGUF`), a huge dense model that must be filtered out on
/// a typical machine (`Ornith-1.0-397B-GGUF`), a small dense model
/// (`gpt-oss-20b-GGUF`), a repo with no parseable size in its name
/// (`grok-2-GGUF`), and a `private: true` entry that must never surface.
const UNSLOTH_HF_FIXTURE: &str = r#"[
  {"id":"unsloth/Ornith-1.0-35B-GGUF","downloads":99353,"trendingScore":65,"private":false},
  {"id":"unsloth/Qwen3.6-35B-A3B-GGUF","downloads":811019,"trendingScore":48,"private":false},
  {"id":"unsloth/Ornith-1.0-397B-GGUF","downloads":5227,"trendingScore":12,"private":false},
  {"id":"unsloth/gpt-oss-20b-GGUF","downloads":522935,"trendingScore":6,"private":false},
  {"id":"unsloth/grok-2-GGUF","downloads":17569,"trendingScore":2,"private":false},
  {"id":"unsloth/secret-model-GGUF","downloads":1,"trendingScore":0,"private":true},
  {"id":"","downloads":1,"trendingScore":0,"private":false}
]"#;

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
    // sibling mode/fast select options are ignored; the effort select supplies
    // the catalog-wide fallback. Model values remain verbatim (including
    // effort-suffixed ids like "opus[1m]"). The select's currentValue names a
    // real row, so it is marked isDefault and the "default" pseudo-row drops.
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
    assert_eq!(ids, ["opus[1m]", "claude-fable-5[1m]", "sonnet", "haiku"]);
    assert_eq!(
        rows[0],
        json!({ "id": "opus[1m]", "name": "Opus", "provider": "claude-code",
                "description": "Opus 4.8 with 1M context · Best for everyday, complex tasks",
                "effortLevels": ["low"], "isDefault": true })
    );
    assert_eq!(rows[1]["name"], "Fable");
    assert!(rows.iter().all(|r| r["provider"] == "claude-code"));
}

#[test]
fn parse_acp_models_resolves_default_pseudo_row_via_family_match() {
    // currentValue is the pseudo-row itself, so the default resolves by
    // matching the pseudo-row's version-bearing family ("Opus 4.8") against
    // the sibling rows: the pseudo-row drops, the sibling gains isDefault.
    let payload = json!({
        "configOptions": [
            { "id": "model", "category": "model", "type": "select",
              "currentValue": "default",
              "options": [
                { "value": "default", "name": "Default (recommended)",
                  "description": "Opus 4.8 with 1M context · Best for everyday, complex tasks" },
                { "value": "opus[1m]", "name": "Opus",
                  "description": "Opus 4.8 with 1M context · Best for everyday, complex tasks" },
                { "value": "sonnet", "name": "Sonnet",
                  "description": "Sonnet 5 · Efficient for routine tasks" }
              ] }
        ]
    });
    let rows = parse_acp_models(&payload, "claude-code");
    let ids: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["opus[1m]", "sonnet"]);
    assert_eq!(rows[0]["isDefault"], json!(true));
    assert!(!rows[1].as_object().unwrap().contains_key("isDefault"));
}

#[test]
fn parse_acp_models_drops_pseudo_row_for_live_opus_5_payload() {
    // Repro of the live claude-agent-acp payload observed on intentd v0.7.42
    // (2026-08, monorepo model-picker screenshot): the pseudo-row and the
    // "Opus (1M context)" sibling share the same description, the model
    // select's currentValue is "default". The pseudo-row must never reach the
    // wire catalog while real rows exist.
    let payload = json!({
        "configOptions": [
            { "id": "model", "name": "Model", "category": "model", "type": "select",
              "currentValue": "default",
              "options": [
                { "value": "default", "name": "Default (recommended)",
                  "description": "Opus 5 with 1M context · Best for everyday, complex tasks" },
                { "value": "opus[1m]", "name": "Opus (1M context)",
                  "description": "Opus 5 with 1M context · Best for everyday, complex tasks" },
                { "value": "claude-fable-5", "name": "Fable",
                  "description": "Fable 5 · Most capable for your hardest and longest tasks" },
                { "value": "sonnet", "name": "Sonnet",
                  "description": "Sonnet 5 · Efficient for routine tasks" }
              ] }
        ]
    });
    let rows = parse_acp_models(&payload, "claude-code");
    let ids: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["opus[1m]", "claude-fable-5", "sonnet"]);
    assert_eq!(rows[0]["isDefault"], json!(true));
}

#[test]
fn parse_acp_models_drops_unresolvable_default_pseudo_row() {
    // An unresolvable pseudo-row — no version-bearing family in its
    // name/description, or a family matching zero or several siblings — is
    // still dropped when real rows exist; nothing is marked isDefault (no
    // guessing).
    let no_family = json!({
        "configOptions": [
            { "id": "model", "currentValue": "default",
              "options": [
                { "value": "default", "name": "Default (recommended)" },
                { "value": "opus", "name": "Opus 4.8" },
                { "value": "sonnet", "name": "Sonnet 5" }
              ] }
        ]
    });
    let ambiguous = json!({
        "configOptions": [
            { "id": "model", "currentValue": "default",
              "options": [
                { "value": "default", "name": "Default", "description": "Opus 4.8" },
                { "value": "opus", "name": "Opus 4.8" },
                { "value": "opus[1m]", "name": "Opus 4.8 (1M context)" }
              ] }
        ]
    });
    for payload in [no_family, ambiguous] {
        let rows = parse_acp_models(&payload, "claude-code");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r["id"] != "default"));
        assert!(rows
            .iter()
            .all(|r| !r.as_object().unwrap().contains_key("isDefault")));
    }

    // A catalog whose ONLY row is the pseudo-row keeps it — the catalog must
    // not come back empty (D1).
    let only_default = json!({
        "configOptions": [
            { "id": "model", "currentValue": "default",
              "options": [ { "value": "default", "name": "Default (recommended)" } ] }
        ]
    });
    let rows = parse_acp_models(&only_default, "claude-code");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "default");

    // The drop requires a real (non-pseudo) row: a pathological catalog of
    // only duplicate pseudo-rows is served unchanged and nothing is marked
    // isDefault.
    let all_pseudo = json!({
        "configOptions": [
            { "id": "model", "currentValue": "default",
              "options": [
                { "value": "default", "name": "Default (recommended)" },
                { "value": "DEFAULT", "name": "Default (dup)" }
              ] }
        ]
    });
    let rows = parse_acp_models(&all_pseudo, "claude-code");
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|r| !r.as_object().unwrap().contains_key("isDefault")));
}

#[test]
fn parse_acp_models_drops_every_pseudo_row_when_a_real_row_exists() {
    // EVERY pseudo-row is dropped once a real row exists — not just the
    // first — matching the cache-load sanitization: a `default` id must
    // never ship next to a real model row.
    let payload = json!({
        "configOptions": [
            { "id": "model", "currentValue": "sonnet",
              "options": [
                { "value": "default", "name": "Default (recommended)" },
                { "value": "DEFAULT", "name": "Default (dup)" },
                { "value": "sonnet", "name": "Sonnet" }
              ] }
        ]
    });
    let rows = parse_acp_models(&payload, "claude-code");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "sonnet");
    assert_eq!(rows[0]["isDefault"], json!(true));
}

#[test]
fn parse_acp_models_current_value_marks_default_without_pseudo_row() {
    // A real (non-"default") currentValue marks its row isDefault even when
    // the catalog carries no pseudo-row at all.
    let payload = json!({
        "configOptions": [
            { "id": "model", "currentValue": "sonnet",
              "options": [
                { "value": "opus", "name": "Opus" },
                { "value": "sonnet", "name": "Sonnet" }
              ] }
        ]
    });
    let rows = parse_acp_models(&payload, "claude-code");
    assert_eq!(rows.len(), 2);
    assert!(!rows[0].as_object().unwrap().contains_key("isDefault"));
    assert_eq!(rows[1]["isDefault"], json!(true));
}

#[test]
fn parse_acp_models_current_value_wins_over_unresolvable_pseudo_row() {
    // The currentValue resolution is authoritative: it marks its row and
    // drops the pseudo-row even when the family match alone could not have
    // resolved it (family-less pseudo-row description).
    let payload = json!({
        "configOptions": [
            { "id": "model", "currentValue": "haiku",
              "options": [
                { "value": "Default", "name": "Default (recommended)" },
                { "value": "opus", "name": "Opus" },
                { "value": "haiku", "name": "Haiku" }
              ] }
        ]
    });
    let rows = parse_acp_models(&payload, "claude-code");
    let ids: Vec<&str> = rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
    // The pseudo-row id matches case-insensitively.
    assert_eq!(ids, ["opus", "haiku"]);
    assert_eq!(rows[1]["isDefault"], json!(true));
}

#[test]
fn parse_acp_models_family_less_default_row_drops_without_config_options() {
    // Catalogs from the models.availableModels shapes have no model select to
    // read a currentValue from, so only the family-match fallback applies.
    // This family-less "default" row cannot resolve — it is still dropped
    // (a real sibling exists) and no row is marked isDefault. (A legacy
    // default row whose name/description DOES carry a version-bearing family
    // shared with exactly one sibling would still resolve via the fallback.)
    let payload = json!({
        "models": {
            "availableModels": [
                { "modelId": "default", "name": "Default" },
                { "modelId": "opus", "name": "Opus" }
            ],
            "currentModelId": "opus"
        }
    });
    let rows = parse_acp_models(&payload, "claude-code");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "opus");
    assert!(rows
        .iter()
        .all(|r| !r.as_object().unwrap().contains_key("isDefault")));
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
fn parse_acp_models_carry_adapter_advertised_effort_levels() {
    // claude-agent-acp advertises per-model effort support as
    // `supportedEffortLevels`; it surfaces as `effortLevels` (PROTOCOL §5.30).
    let payload = json!({
        "models": { "availableModels": [
            { "modelId": "opus", "name": "Opus",
              "supportedEffortLevels": ["low", "medium", "high", "max"] },
            { "modelId": "haiku", "name": "Haiku" },
            { "modelId": "sonnet", "name": "Sonnet", "supportedEffortLevels": [] }
        ] }
    });
    let rows = parse_acp_models(&payload, "claude-code");
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0]["effortLevels"],
        json!(["low", "medium", "high", "max"])
    );
    assert!(!rows[1].as_object().unwrap().contains_key("effortLevels"));
    assert!(!rows[2].as_object().unwrap().contains_key("effortLevels"));
}

#[test]
fn parse_acp_models_fall_back_to_session_thought_levels() {
    let payload = json!({
        "configOptions": [
            { "id": "model", "category": "model", "type": "select",
              "options": [
                  { "value": "opus", "name": "Opus" },
                  { "value": "haiku", "name": "Haiku" }
              ] },
            { "id": "effort", "category": "thought_level", "type": "select",
              "currentValue": "default", "options": [
                  { "value": "default", "name": "Default" },
                  { "value": "low", "name": "Low" },
                  { "value": "medium", "name": "Medium" },
                  { "value": "high", "name": "High" },
                  { "value": "max", "name": "Max" }
              ] }
        ]
    });
    let rows = parse_acp_models(&payload, "claude-code");
    assert_eq!(rows.len(), 2);
    assert!(rows
        .iter()
        .all(|row| row["effortLevels"] == json!(["low", "medium", "high", "max"])));
}

#[test]
fn parse_acp_models_preserve_per_model_effort_levels_over_session_fallback() {
    let payload = json!({
        "models": { "availableModels": [
            { "modelId": "opus", "supportedEffortLevels": ["low", "high"] },
            { "modelId": "sonnet", "effortLevels": ["medium", "max"] },
            { "modelId": "haiku" }
        ] },
        "configOptions": [
            { "id": "effort", "category": "thought_level", "type": "select",
              "options": [
                  { "value": "default", "name": "Default" },
                  { "value": "low", "name": "Low" },
                  { "value": "medium", "name": "Medium" },
                  { "value": "high", "name": "High" }
              ] }
        ]
    });
    let rows = parse_acp_models(&payload, "claude-code");
    assert_eq!(rows[0]["effortLevels"], json!(["low", "high"]));
    assert_eq!(rows[1]["effortLevels"], json!(["medium", "max"]));
    assert_eq!(rows[2]["effortLevels"], json!(["low", "medium", "high"]));
}

#[test]
fn parse_acp_models_ignore_non_levels_and_empty_thought_level_selects() {
    for payload in [
        json!({
            "models": { "availableModels": [{ "modelId": "sonnet" }] },
            "configOptions": [{
                "id": "effort", "category": "thought_level", "type": "select",
                "options": [{ "value": "DEFAULT" }, { "value": "  " }, { "name": "Missing" }]
            }]
        }),
        json!({
            "models": { "availableModels": [{ "modelId": "sonnet" }] },
            "configOptions": [{
                "id": "effort", "category": "thought_level", "type": "boolean",
                "options": [{ "value": "high" }]
            }]
        }),
    ] {
        let rows = parse_acp_models(&payload, "claude-code");
        assert!(!rows[0].as_object().unwrap().contains_key("effortLevels"));
    }
}

#[test]
fn parse_codex_models_collapse_effort_capable_models_to_one_row() {
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
    // One base row per model — no `{model}/{effort}` expansion.
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        json!({ "id": "gpt-5.3-codex", "name": "GPT-5.3 Codex", "provider": "codex",
                "description": "Flagship coding model" })
    );
    assert_eq!(
        rows[1],
        json!({ "id": "gpt-5.4", "name": "GPT-5.4", "provider": "codex" })
    );
}

#[test]
fn parse_codex_models_bare_id_without_description() {
    let payload = json!({ "models": { "availableModels": [ { "modelId": "gpt-5.2-codex" } ] } });
    let rows = parse_codex_acp_models(&payload);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        json!({ "id": "gpt-5.2-codex", "name": "gpt-5.2-codex", "provider": "codex" })
    );
}

#[test]
fn parse_codex_models_bare_ids_do_not_invent_effort_levels() {
    for model_id in [
        "gpt-5.6-sol",
        "gpt-5.3-codex",
        "gpt-5.2-codex",
        "gpt-5.1-codex-max",
    ] {
        let payload = json!({ "models": { "availableModels": [{ "modelId": model_id }] } });
        let rows = parse_codex_acp_models(&payload);
        assert!(
            !rows[0].as_object().unwrap().contains_key("effortLevels"),
            "unexpected invented levels for {model_id}"
        );
    }
}

#[test]
fn parse_codex_models_prefer_adapter_advertised_effort_levels() {
    let payload = json!({
        "models": { "availableModels": [
            { "modelId": "gpt-5.3-codex", "name": "GPT-5.3 Codex",
              "supportedEffortLevels": ["low", "high"] }
        ] }
    });
    let rows = parse_codex_acp_models(&payload);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["effortLevels"], json!(["low", "high"]));
}

#[test]
fn parse_codex_models_collapse_parenthesized_variants_for_unknown_family() {
    let payload = json!({
        "models": { "availableModels": [
            { "modelId": "gpt-6-nova", "name": "GPT-6 Nova (low)" },
            { "modelId": "gpt-6-nova", "name": "GPT-6 Nova (medium)" },
            { "modelId": "gpt-6-nova", "name": "GPT-6 Nova (max)" }
        ] }
    });
    let rows = parse_codex_acp_models(&payload);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        json!({ "id": "gpt-6-nova", "name": "GPT-6 Nova", "provider": "codex",
                "effortLevels": ["low", "medium", "max"] })
    );
}

#[test]
fn parse_codex_models_collapse_effort_suffixed_ids() {
    let payload = json!({
        "configOptions": [
            { "id": "model", "options": [
                { "value": "gpt-5.6-sol/low", "name": "GPT-5.6-Sol (low)" },
                { "value": "gpt-5.6-sol/medium", "name": "GPT-5.6-Sol (medium)" },
                { "value": "gpt-5.6-sol/high", "name": "GPT-5.6-Sol (high)" },
                { "value": "gpt-5.6-sol/xhigh", "name": "GPT-5.6-Sol (xhigh)" },
                { "value": "gpt-5.6-sol/max", "name": "GPT-5.6-Sol (max)" }
            ] }
        ]
    });
    let rows = parse_codex_acp_models(&payload);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "gpt-5.6-sol");
    assert_eq!(rows[0]["name"], "GPT-5.6-Sol");
    assert_eq!(
        rows[0]["effortLevels"],
        json!(["low", "medium", "high", "xhigh", "max"])
    );
}

#[test]
fn parse_codex_models_collapse_live_bracket_effort_ids() {
    let payload = json!({
        "models": { "availableModels": [
            { "modelId": "gpt-5.6-sol[LOW]", "name": "GPT-5.6-Sol",
              "description": "Low effort", "effortLevels": ["medium"] },
            { "modelId": "gpt-5.6-sol[medium]", "name": "GPT-5.6-Sol",
              "description": "Medium effort", "effortLevels": ["medium"] },
            { "modelId": "gpt-5.6-sol[high]", "name": "GPT-5.6-Sol",
              "effortLevels": ["medium"] },
            { "modelId": "gpt-5.6-sol[xhigh]", "name": "GPT-5.6-Sol",
              "effortLevels": ["medium"] },
            { "modelId": "gpt-5.6-sol[max]", "name": "GPT-5.6-Sol",
              "effortLevels": ["medium"] },
            { "modelId": "gpt-5.6-sol[ultra]", "name": "GPT-5.6-Sol (ultra)",
              "effortLevels": ["medium"] },
            { "modelId": "gpt-5.6-sol[none]", "name": "GPT-5.6-Sol",
              "effortLevels": ["medium"] }
        ] }
    });
    let rows = parse_codex_acp_models(&payload);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        json!({ "id": "gpt-5.6-sol", "name": "GPT-5.6-Sol", "provider": "codex",
                "effortLevels": ["low", "medium", "high", "xhigh", "max", "ultra"] })
    );
}

#[test]
fn parse_codex_models_none_only_variant_has_no_effort_evidence() {
    let payload = json!({
        "models": { "availableModels": [
            { "modelId": "gpt-5.6-sol[none]", "name": "GPT-5.6-Sol" }
        ] }
    });
    let rows = parse_codex_acp_models(&payload);
    assert!(
        !rows[0].as_object().unwrap().contains_key("effortLevels"),
        "none is not usable effort evidence"
    );
}

#[test]
fn parse_codex_collapsed_variants_merge_adapter_levels() {
    let payload = json!({
        "models": { "availableModels": [
            { "modelId": "future-model/low", "name": "Future Model (low)",
              "supportedEffortLevels": ["medium", "max"] },
            { "modelId": "future-model/high", "name": "Future Model (high)" }
        ] }
    });
    let rows = parse_codex_acp_models(&payload);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["effortLevels"],
        json!(["low", "medium", "high", "max"])
    );
}

#[test]
fn parse_codex_models_from_config_options() {
    // Canned from a live codex-acp@0.16.0 session/new result (2026-07-21):
    // same configOptions[id="model"].options shape as claude-code. Bare rows
    // carry no `effortLevels` without adapter evidence.
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
    assert!(
        !rows[0].as_object().unwrap().contains_key("effortLevels"),
        "bare model rows must not invent effort levels"
    );
    assert_eq!(
        rows[1],
        json!({ "id": "gpt-5.5", "name": "GPT-5.5", "provider": "codex",
                "description": "Frontier model for complex coding, research, and real-world work." })
    );
}

#[test]
fn parse_codex_config_options_bare_id_has_no_effort_levels() {
    let payload = json!({
        "configOptions": [
            { "id": "model",
              "options": [ { "value": "gpt-5.3-codex", "name": "GPT-5.3 Codex" } ] }
        ]
    });
    let rows = parse_codex_acp_models(&payload);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        json!({ "id": "gpt-5.3-codex", "name": "GPT-5.3 Codex", "provider": "codex" })
    );
}

#[test]
fn parse_codex_models_merge_standard_and_configured_catalogs() {
    let payload = json!({
        "models": { "availableModels": [
            { "modelId": "gpt-5.6-sol", "name": "GPT-5.6-Sol",
              "description": "Standard catalog metadata",
              "supportedEffortLevels": ["medium"] },
            { "modelId": "gpt-5.6-sol[high]", "name": "GPT-5.6-Sol" },
            { "modelId": "gpt-5.5", "name": "GPT-5.5" }
        ] },
        "configOptions": [
            { "id": "model", "options": [
                { "value": "GPT-5.6-SOL/max", "name": "Configured duplicate",
                  "description": "Must not replace standard metadata" },
                { "value": "gpt-5.7-pro[ultra]", "name": "GPT-5.7 Pro (ultra)",
                  "description": "Configured only" },
                { "value": "gpt-5.4", "name": "GPT-5.4" }
            ] }
        ]
    });

    let rows = parse_codex_acp_models(&payload);
    assert_eq!(
        rows,
        vec![
            json!({ "id": "gpt-5.6-sol", "name": "GPT-5.6-Sol", "provider": "codex",
                    "description": "Standard catalog metadata",
                    "effortLevels": ["medium", "high"] }),
            json!({ "id": "gpt-5.5", "name": "GPT-5.5", "provider": "codex" }),
            json!({ "id": "gpt-5.7-pro", "name": "GPT-5.7 Pro", "provider": "codex",
                    "description": "Configured only" }),
            json!({ "id": "gpt-5.4", "name": "GPT-5.4", "provider": "codex" }),
        ]
    );
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

/// A successful [`std::process::ExitStatus`] for the pure grok outcome seam.
fn exit_ok() -> std::process::ExitStatus {
    std::process::ExitStatus::default()
}

#[test]
fn grok_outcome_maps_text_rows_to_wire_shape() {
    // Canned `grok models` text output: the shared intent-providers parser
    // extracts the rows; this seam maps them onto §5.30 wire rows.
    let fetch = super::grok_fetch_outcome(
        "You are logged in with grok.com.\ngrok-build  Grok Build  Default model\nopus-4-8  Opus 4.8",
        exit_ok(),
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
        exit_ok(),
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
        exit_ok(),
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
    let fetch = super::grok_fetch_outcome("", exit_ok(), "");
    assert!(fetch.models.is_none());
    assert_eq!(fetch.warning.as_deref(), Some("grok: no models reported"));
}

#[cfg(unix)]
#[test]
fn grok_outcome_failed_exit_without_rows_is_attributed() {
    // The warning must carry the actual exit status (parity with the
    // opencode warning) plus the stderr tail.
    let fetch = super::grok_fetch_outcome("", exit_status(1), "grok: command crashed\n");
    assert!(fetch.models.is_none());
    let warning = fetch.warning.expect("warning present");
    assert!(
        warning.starts_with("grok: grok models exited with exit status: 1"),
        "{warning}"
    );
    assert!(warning.contains("command crashed"), "{warning}");
}

#[test]
fn stderr_tail_keeps_last_200_chars() {
    assert_eq!(super::stderr_tail("  boom \n"), "boom");
    let long = format!("{}{}", "x".repeat(500), "y".repeat(200));
    let tail = super::stderr_tail(&long);
    assert_eq!(tail.chars().count(), 200);
    assert_eq!(tail, "y".repeat(200));
    // Multi-byte chars: the boundary walk must never split a char.
    let unicode = "é".repeat(300);
    let tail = super::stderr_tail(&unicode);
    assert_eq!(tail.chars().count(), 200);
}

#[cfg(unix)]
#[test]
fn grok_outcome_rows_win_over_failed_exit() {
    // Parsed rows with a non-zero exit still serve the catalog — stdout is
    // the contract, not the exit code.
    let fetch = super::grok_fetch_outcome("grok-build  Grok Build", exit_status(1), "noise");
    let rows = fetch.models.expect("models present");
    assert_eq!(rows[0]["id"], "grok-build");
}

#[cfg(unix)]
#[tokio::test]
// Holds CHILD_SPAWN_SERIAL across the spawn/await on purpose: the guard must
// cover the whole child-spawning body so these fake-CLI execs never run
// concurrently and starve one another.
#[allow(clippy::await_holding_lock)]
async fn opencode_models_cli_child_path_includes_binary_dir() {
    use std::os::unix::fs::PermissionsExt;
    // A fake opencode whose success is gated on its own parent dir being on
    // the child's $PATH — the enhanced-path contract shared with the ACP
    // probe spawns. The temp dir is not on the process PATH, so the run only
    // succeeds when the spawn sets the child's PATH explicitly.
    //
    // The timeout is deliberately generous (not the 10s production
    // `OPENCODE_CLI_TIMEOUT`): this test asserts PATH composition, not the
    // timeout path, and under full parallel-suite load (plus a first-exec
    // Gatekeeper scan on macOS) the spawn alone can take seconds
    // (monorepo#921).
    let _serial = CHILD_SPAWN_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("opencode");
    let script = format!(
        "#!/bin/sh\ncase \":$PATH:\" in\n  *\":{dir}:\"*) printf '%s\\n' 'anthropic/claude-3' ;;\n  *) exit 1 ;;\nesac\n",
        dir = dir.path().display(),
    );
    std::fs::write(&bin, script).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let stdout = super::run_opencode_models_cli(bin, std::time::Duration::from_secs(60))
        .await
        .expect("exit 0 when the child PATH carries the binary dir");
    assert!(stdout.contains("anthropic/claude-3"));
}

#[cfg(unix)]
#[tokio::test]
#[allow(clippy::await_holding_lock)] // deliberate: serialize the whole child spawn (see above)
async fn grok_models_cli_child_path_includes_binary_dir() {
    use std::os::unix::fs::PermissionsExt;
    // Same enhanced-path contract as the opencode CLI spawn: the fake grok
    // only succeeds when its own parent dir is on the child's $PATH. Same
    // generous timeout rationale as the opencode analog above (monorepo#921).
    let _serial = CHILD_SPAWN_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("grok");
    let script = format!(
        "#!/bin/sh\ncase \":$PATH:\" in\n  *\":{dir}:\"*) printf '%s\\n' 'grok-build  Grok Build' ;;\n  *) exit 1 ;;\nesac\n",
        dir = dir.path().display(),
    );
    std::fs::write(&bin, script).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let output = super::run_grok_models_cli(bin, std::time::Duration::from_secs(60))
        .await
        .expect("spawn succeeds");
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("grok-build"));
}

#[cfg(unix)]
#[tokio::test]
#[allow(clippy::await_holding_lock)] // deliberate: serialize the whole child spawn (see above)
async fn grok_cli_timeout_flows_into_attributed_warning() {
    use std::os::unix::fs::PermissionsExt;
    // A wedged `grok models` must be cut short and the timeout reason must
    // surface through the fetch attribution (`grok: ...`). No wall-clock
    // bound (parity with the opencode analog): a first-exec Gatekeeper scan
    // on macOS can delay the spawn itself by seconds, and the attributed
    // warning already proves the timeout path fired.
    let _serial = CHILD_SPAWN_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("grok");
    std::fs::write(&bin, "#!/bin/sh\nsleep 30\n").unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let fetch = super::ProviderModelsFetch::unavailable(
        "grok",
        super::run_grok_models_cli(bin, std::time::Duration::from_secs(5))
            .await
            .unwrap_err(),
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
    let (cmd, home) = super::with_isolated_codex_home(cmd).unwrap();
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
fn codex_probe_launch_npx_fallback_strips_codex_env() {
    // The pinned npx fallback is daemon-managed: CODEX_PATH / CODEX_CONFIG
    // must be removed from its child env (#555).
    let cmd = super::codex_probe_launch(None, Some(std::path::PathBuf::from("/usr/local/bin/npx")))
        .expect("npx fallback must produce a probe command");
    let removed = cmd.removed_env_vars();
    assert!(removed.iter().any(|k| k == "CODEX_PATH"));
    assert!(removed.iter().any(|k| k == "CODEX_CONFIG"));
}

#[test]
fn codex_probe_launch_resolved_binary_keeps_codex_env() {
    // A resolved codex-acp binary (providers.paths override / PATH scan) is
    // the user's escape hatch — its env must be left untouched.
    let cmd = super::codex_probe_launch(
        Some(std::path::PathBuf::from("/custom/codex-acp")),
        Some(std::path::PathBuf::from("/usr/local/bin/npx")),
    )
    .expect("resolved binary must produce a probe command");
    assert!(cmd.removed_env_vars().is_empty());
    assert!(cmd.env_vars().is_empty());
}

#[test]
fn codex_probe_launch_without_binary_or_npx_is_none() {
    assert!(super::codex_probe_launch(None, None).is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn acp_probe_child_env_removals_reach_child() {
    // `env_remove` must win even over an explicit `env` set, proving the
    // removal reaches the spawned child's environment.
    let out = tempfile::tempdir().unwrap();
    let out_file = out.path().join("codex_path.txt");
    let script = format!(
        "printf %s \"${{CODEX_PATH-UNSET}}\" > '{}'",
        out_file.display()
    );
    let cmd = super::probe::AcpProbeCommand::binary(
        std::path::PathBuf::from("/bin/sh"),
        vec!["-c".to_string(), script],
    )
    .env("CODEX_PATH", "/nonexistent/bogus")
    .env_remove("CODEX_PATH");

    let _ = super::probe::run_acp_probe(cmd, |_| Vec::new()).await;

    let recorded = std::fs::read_to_string(&out_file).unwrap();
    assert_eq!(recorded, "UNSET");
}

/// intent-hq/intent#3941: the claude-code ACP auth fallback's outcome →
/// tri-state mapping (no adapter spawn). Only the adapter's explicit
/// auth-required RPC error (intent-hq/intent#3178) may demote to a hard
/// false; everything else stays unknown — including a NON-EMPTY model
/// list, because claude-agent-acp serves its catalog uncredentialed (the
/// auth error only fires at prompt time), so a model list alone is never
/// proof of auth.
#[test]
fn claude_code_acp_auth_verdict_mapping() {
    use super::claude_code_acp_auth_verdict;
    let rpc = |code: i64, message: &str| {
        ProbeError::Rpc(intent_acp::JsonRpcError {
            code,
            message: message.to_string(),
            data: None,
        })
    };
    // Regression: a non-empty model list must NOT harden into Some(true) —
    // the adapter returns the full catalog even when logged out.
    assert_eq!(
        claude_code_acp_auth_verdict(Ok(vec![json!({"id": "claude-opus-4"})])),
        None
    );
    assert_eq!(
        claude_code_acp_auth_verdict(Err(rpc(-32000, "Authentication required"))),
        Some(false)
    );
    // Inconclusive outcomes must stay unknown — never a hard false.
    assert_eq!(claude_code_acp_auth_verdict(Ok(Vec::new())), None);
    assert_eq!(claude_code_acp_auth_verdict(Err(ProbeError::Empty)), None);
    assert_eq!(claude_code_acp_auth_verdict(Err(ProbeError::Timeout)), None);
    assert_eq!(
        claude_code_acp_auth_verdict(Err(ProbeError::Spawn("nope".to_string()))),
        None
    );
    assert_eq!(
        claude_code_acp_auth_verdict(Err(rpc(-32603, "internal error"))),
        None
    );
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
#[allow(clippy::await_holding_lock)] // deliberate: serialize the whole child spawn (see above)
async fn opencode_cli_timeout_kills_child_and_reports_timeout() {
    use std::os::unix::fs::PermissionsExt;
    // A wedged `opencode models` must be reaped when the timeout elapses and
    // the failure must be attributable as a timeout. The fake CLI records its
    // PID first thing, then sleeps far past the injected timeout — a ~5s
    // budget leaves slow runners ample time to write the PID file before the
    // deadline even under full-suite parallel load.
    let _serial = CHILD_SPAWN_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    let err = super::run_opencode_models_cli(bin, std::time::Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(
        start.elapsed() < std::time::Duration::from_secs(20),
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
#[allow(clippy::await_holding_lock)] // deliberate: serialize the whole child spawn (see above)
async fn opencode_timeout_flows_into_attributed_warning() {
    use std::os::unix::fs::PermissionsExt;
    // The timeout reason must surface through the fetch result attribution
    // (`opencode: ...`), matching what models.list callers see.
    let _serial = CHILD_SPAWN_SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("opencode");
    std::fs::write(&bin, "#!/bin/sh\nsleep 30\n").unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let fetch = super::ProviderModelsFetch::unavailable(
        "opencode",
        super::run_opencode_models_cli(bin, std::time::Duration::from_secs(5))
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
fn parse_param_count_billions_handles_dense_and_moe_names() {
    // Dense: bare "<N>B" size token.
    assert_eq!(
        parse_param_count_billions("unsloth/Ornith-1.0-35B-GGUF"),
        Some(35.0)
    );
    // MoE: total ("35B") wins over the active-parameter marker ("A3B").
    assert_eq!(
        parse_param_count_billions("unsloth/Qwen3.6-35B-A3B-GGUF"),
        Some(35.0)
    );
    // Small dense model in lowercase ("20b").
    assert_eq!(
        parse_param_count_billions("unsloth/gpt-oss-20b-GGUF"),
        Some(20.0)
    );
    // Sub-billion size in millions.
    assert_eq!(
        parse_param_count_billions("unsloth/functiongemma-270m-it-GGUF"),
        Some(0.27)
    );
    // Fractional size.
    assert_eq!(
        parse_param_count_billions("unsloth/Qwen3.5-0.8B-MTP-GGUF"),
        Some(0.8)
    );
    // No size token anywhere in the name.
    assert_eq!(parse_param_count_billions("unsloth/grok-2-GGUF"), None);
    assert_eq!(
        parse_param_count_billions("unsloth/Qwen3-Coder-Next-GGUF"),
        None
    );
}

#[test]
// Small test constants: float→int casts are exact and saturating.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn fits_within_ram_applies_the_seventy_percent_threshold() {
    // A 20B dense model: ~20e9 * 0.6 + 1GiB headroom ≈ 12.99 GB. Comfortably
    // under 70% of 32 GiB (~22.4 GB).
    assert!(fits_within_ram(20.0, 32 * GIB));
    // A 397B dense model: ~397e9 * 0.6 + 1GiB ≈ 239 GB, far over 70% of a
    // typical 64 GiB machine (~44.8 GB).
    assert!(!fits_within_ram(397.0, 64 * GIB));
    // Boundary: exactly at the threshold must fit (<=, not <).
    let exact = estimate_model_bytes(10.0);
    let total_ram = (exact / 0.7).ceil() as u64;
    assert!(fits_within_ram(10.0, total_ram));
}

#[test]
// Small test constants: float↔int casts are exact and saturating.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn gguf_bytes_fit_within_ram_shares_the_catalog_budget() {
    // 15 GB of weights + 1 GiB headroom ≈ 16.07 GB, under 70% of 32 GiB
    // (~24.05 GB) but over 70% of 16 GiB (~12.03 GB).
    assert!(gguf_bytes_fit_within_ram(15_000_000_000, 32 * GIB));
    assert!(!gguf_bytes_fit_within_ram(15_000_000_000, 16 * GIB));
    // Boundary: exactly at the threshold must fit (<=, not <).
    let model_bytes = 10 * GIB;
    let total_ram = (((model_bytes + GIB) as f64) / 0.7).ceil() as u64;
    assert!(gguf_bytes_fit_within_ram(model_bytes, total_ram));
}

#[test]
fn parse_hf_unsloth_response_drops_private_and_empty_id_rows() {
    let repos = parse_hf_unsloth_response(UNSLOTH_HF_FIXTURE);
    // 7 entries in the fixture minus 1 private minus 1 empty-id = 5.
    assert_eq!(repos.len(), 5);
    assert!(repos.iter().all(|r| r.id != "unsloth/secret-model-GGUF"));
    assert!(repos.iter().any(|r| r.id == "unsloth/Ornith-1.0-35B-GGUF"));
}

#[test]
fn parse_hf_unsloth_response_malformed_json_yields_empty() {
    assert!(parse_hf_unsloth_response("not json").is_empty());
    assert!(parse_hf_unsloth_response(r#"{"not":"an array"}"#).is_empty());
    assert!(parse_hf_unsloth_response("").is_empty());
}

#[test]
fn build_unsloth_rows_one_row_per_repo_sorted_by_downloads() {
    let repos = parse_hf_unsloth_response(UNSLOTH_HF_FIXTURE);
    // No RAM info: every parsed repo becomes a row, unfiltered.
    let (rows, hidden) = build_unsloth_rows(&repos, None);
    assert_eq!(rows.len(), 5);
    assert_eq!(hidden, 0);
    // Sorted by downloads descending: Qwen3.6-35B-A3B (811019) first.
    assert_eq!(rows[0]["id"], "unsloth/Qwen3.6-35B-A3B-GGUF");
    assert_eq!(rows[0]["name"], "Qwen3.6-35B-A3B");
    assert_eq!(rows[0]["provider"], "unsloth");
    assert_eq!(rows[0]["description"], "811,019 downloads");
    assert_eq!(rows[1]["id"], "unsloth/gpt-oss-20b-GGUF");
}

#[test]
fn build_unsloth_rows_filters_by_ram_fit_and_counts_hidden() {
    let repos = parse_hf_unsloth_response(UNSLOTH_HF_FIXTURE);
    // 24 GiB machine: the 35B models (~2 rows, ≈22 GB estimated) and the
    // 397B model (≈239 GB) don't fit within 70% (≈16.8 GB); grok-2 has no
    // parseable size and is also excluded (unknown ⇒ hidden); only the 20B
    // gpt-oss model (≈13 GB) fits.
    let (rows, hidden) = build_unsloth_rows(&repos, Some(24 * GIB));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "unsloth/gpt-oss-20b-GGUF");
    assert_eq!(hidden, 4);
}

#[test]
fn build_unsloth_rows_empty_input_yields_no_rows() {
    let (rows, hidden) = build_unsloth_rows(&[], Some(32 * GIB));
    assert!(rows.is_empty());
    assert_eq!(hidden, 0);
}

#[test]
fn build_unsloth_rows_tolerates_non_ascii_repo_names() {
    // Regression: a multi-byte name whose len-5 offset is not a char
    // boundary must not panic in the `-GGUF` suffix strip.
    let repos = parse_hf_unsloth_response(r#"[{"id": "unsloth/ééé", "downloads": 1}]"#);
    let (rows, _hidden) = build_unsloth_rows(&repos, None);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], "ééé");
}

#[test]
fn unsloth_fetch_outcome_unknown_size_repo_hidden_even_with_abundant_ram() {
    // On a generous 512 GiB machine every parseable model fits, but grok-2
    // (unparseable size) is still hidden and still surfaces a warning.
    let fetch = super::unsloth_fetch_outcome(UNSLOTH_HF_FIXTURE, Some(512 * GIB));
    let rows = fetch.models.expect("models present");
    assert_eq!(rows.len(), 4);
    assert!(fetch.warning.is_some());
    assert!(fetch.warning.unwrap().contains("1 repo(s) hidden"));
}

#[test]
fn unsloth_fetch_outcome_all_parseable_and_fitting_has_no_warning() {
    let body = r#"[
        {"id": "unsloth/gpt-oss-20b-GGUF", "downloads": 5, "trendingScore": 1.0},
        {"id": "unsloth/Qwen3.6-35B-A3B-GGUF", "downloads": 9, "trendingScore": 2.0}
    ]"#;
    let fetch = super::unsloth_fetch_outcome(body, Some(512 * GIB));
    let rows = fetch.models.expect("models present");
    assert_eq!(rows.len(), 2);
    assert!(fetch.warning.is_none());
}

#[test]
fn unsloth_fetch_outcome_notes_hidden_count_in_warning() {
    let fetch = super::unsloth_fetch_outcome(UNSLOTH_HF_FIXTURE, Some(24 * GIB));
    let rows = fetch.models.expect("models present");
    assert_eq!(rows.len(), 1);
    let warning = fetch.warning.expect("warning present");
    assert!(warning.contains("unsloth:"), "{warning}");
    assert!(warning.contains("4 repo(s) hidden"), "{warning}");
}

#[test]
fn unsloth_fetch_outcome_all_hidden_degrades_to_unavailable() {
    // A tiny RAM budget filters out every parseable repo.
    let fetch = super::unsloth_fetch_outcome(UNSLOTH_HF_FIXTURE, Some(1024));
    assert!(fetch.models.is_none());
    let warning = fetch.warning.expect("warning present");
    assert!(
        warning.starts_with("unsloth: no models reported"),
        "{warning}"
    );
}

#[test]
fn unsloth_fetch_outcome_empty_catalog_degrades_to_unavailable() {
    let fetch = super::unsloth_fetch_outcome("[]", Some(32 * GIB));
    assert!(fetch.models.is_none());
    assert_eq!(
        fetch.warning.as_deref(),
        Some("unsloth: no models reported")
    );
}

#[test]
fn unsloth_fetch_outcome_malformed_response_degrades_to_unavailable() {
    let fetch = super::unsloth_fetch_outcome("not json", Some(32 * GIB));
    assert!(fetch.models.is_none());
    assert_eq!(
        fetch.warning.as_deref(),
        Some("unsloth: no models reported")
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
