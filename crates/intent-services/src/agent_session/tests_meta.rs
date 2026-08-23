//! Tests for `_meta` payload construction (§18.1 system-prompt injection via
//! ACP extensibility).

use super::{build_session_meta, derived_default_provider, resolve_provider_id};
use intent_core::settings_file::SettingsFile;

#[test]
fn resolve_provider_id_from_compound_model() {
    // Compound model id takes precedence over provider field
    let provider_id = resolve_provider_id(Some("opencode:kimi-k3"), Some("claude-code"), None);
    assert_eq!(
        provider_id.as_deref(),
        Some("opencode"),
        "compound model prefix wins"
    );
}

#[test]
fn resolve_provider_id_from_provider_field() {
    // Bare model id (no colon) falls back to provider field
    let provider_id = resolve_provider_id(Some("gpt-5.3-codex"), Some("codex"), None);
    assert_eq!(
        provider_id.as_deref(),
        Some("codex"),
        "provider field is fallback for bare model"
    );
}

/// Regression (monorepo#3044): with nothing to resolve from there is NO
/// positional last resort — resolution yields `None` and callers fail loudly.
#[test]
fn resolve_provider_id_none_yields_none() {
    let provider_id = resolve_provider_id(None, None, None);
    assert_eq!(
        provider_id, None,
        "None model + None provider + no default -> None (no positional fallback)"
    );
}

/// Regression (monorepo#3044): a bare model carries no provider — with no
/// provider field and no configured default there is nothing to resolve to.
#[test]
fn resolve_provider_id_bare_model_none_provider_yields_none() {
    let provider_id = resolve_provider_id(Some("sonnet4.5"), None, None);
    assert_eq!(
        provider_id, None,
        "bare model + None provider + no default -> None (no positional fallback)"
    );
}

/// A [`SettingsFile`] with the given `model.default` / `providers.active`.
fn settings(model_default: Option<&str>, providers_active: Option<&str>) -> SettingsFile {
    let mut s = SettingsFile::default();
    s.model.default = model_default.map(str::to_string);
    s.providers.active = providers_active.map(str::to_string);
    s
}

#[test]
fn derived_default_provider_prefix_beats_active() {
    // model.default compound prefix outranks providers.active.
    let s = settings(Some("claude-code:sonnet4.5"), Some("codex"));
    assert_eq!(
        derived_default_provider(&s).as_deref(),
        Some("claude-code"),
        "model.default prefix wins over providers.active"
    );
}

#[test]
fn derived_default_provider_bare_model_falls_through_to_active() {
    // A bare model.default (no colon) carries no provider — providers.active wins.
    let s = settings(Some("sonnet4.5"), Some("claude-code"));
    assert_eq!(
        derived_default_provider(&s).as_deref(),
        Some("claude-code"),
        "bare model.default falls through to providers.active"
    );
}

#[test]
fn derived_default_provider_malformed_prefix_falls_through() {
    // ":sonnet" yields an empty prefix — falls through to providers.active.
    let s = settings(Some(":sonnet"), Some("codex"));
    assert_eq!(
        derived_default_provider(&s).as_deref(),
        Some("codex"),
        "malformed compound model.default falls through to providers.active"
    );
}

#[test]
fn derived_default_provider_unknown_prefix_falls_through() {
    // An unregistered prefix (stale value, typo, foreign-build id) must not
    // be trusted — providers.active stays reachable.
    let s = settings(Some("typo:foo"), Some("claude-code"));
    assert_eq!(
        derived_default_provider(&s).as_deref(),
        Some("claude-code"),
        "unknown model.default prefix falls through to providers.active"
    );
}

#[test]
fn derived_default_provider_unknown_active_yields_none() {
    // An unregistered providers.active is not surfaced either.
    let s = settings(None, Some("not-a-provider"));
    assert_eq!(
        derived_default_provider(&s),
        None,
        "unknown providers.active is not surfaced"
    );
}

#[test]
fn derived_default_provider_trims_whitespace() {
    // Padded settings values still resolve to the registered id.
    let s = settings(None, Some("  claude-code  "));
    assert_eq!(
        derived_default_provider(&s).as_deref(),
        Some("claude-code"),
        "whitespace-padded providers.active resolves trimmed"
    );
}

#[test]
fn derived_default_provider_both_unset_yields_none() {
    let s = settings(None, None);
    assert_eq!(
        derived_default_provider(&s),
        None,
        "neither setting set -> None (callers fail loudly, monorepo#3044)"
    );
}

#[test]
fn resolve_provider_id_malformed_compound_falls_back() {
    // Malformed compound id like ":sonnet" yields empty prefix -> falls back to provider field
    let provider_id = resolve_provider_id(Some(":sonnet"), Some("codex"), None);
    assert_eq!(
        provider_id.as_deref(),
        Some("codex"),
        "malformed compound :sonnet falls back to provider field"
    );

    // Malformed compound with no provider field and no default -> None
    let provider_id = resolve_provider_id(Some(":sonnet"), None, None);
    assert_eq!(
        provider_id, None,
        "malformed compound :sonnet with no provider -> None"
    );

    // Empty provider field with no default -> None
    let provider_id = resolve_provider_id(Some("gpt-4"), Some(""), None);
    assert_eq!(
        provider_id, None,
        "empty provider string with no default -> None"
    );
}

/// `configured_default` (spec Decision D2) resolves when neither the model's
/// compound prefix nor the `provider` field yield one.
#[test]
fn resolve_provider_id_uses_configured_default() {
    let provider_id = resolve_provider_id(None, None, Some("claude-code"));
    assert_eq!(
        provider_id.as_deref(),
        Some("claude-code"),
        "configured default resolves when nothing stronger is present"
    );
}

/// The model's compound prefix still wins over `configured_default` — a
/// caller-selected cross-provider model is a stronger signal than the
/// user's ambient default.
#[test]
fn resolve_provider_id_compound_model_wins_over_configured_default() {
    let provider_id = resolve_provider_id(Some("opencode:kimi-k3"), None, Some("claude-code"));
    assert_eq!(provider_id.as_deref(), Some("opencode"));
}

/// The session's `provider` field still wins over `configured_default` — a
/// persisted session's own provider is a stronger signal than the ambient
/// default at read time.
#[test]
fn resolve_provider_id_provider_field_wins_over_configured_default() {
    let provider_id = resolve_provider_id(Some("sonnet4.5"), Some("codex"), Some("claude-code"));
    assert_eq!(provider_id.as_deref(), Some("codex"));
}

/// An empty `configured_default` is treated the same as `None` — resolution
/// yields `None` rather than an empty provider id (monorepo#3044: no
/// positional fallback).
#[test]
fn resolve_provider_id_empty_configured_default_yields_none() {
    let provider_id = resolve_provider_id(None, None, Some(""));
    assert_eq!(provider_id, None);
}

#[test]
fn claude_code_meta_appends_system_prompt() {
    let meta = build_session_meta("claude-code", Some("Test prompt"), Some("Builder"));
    assert!(meta.is_some(), "claude-code gets _meta");
    let meta_map = meta.unwrap();
    assert_eq!(
        meta_map.len(),
        2,
        "claude-code _meta has two top-level keys (claudeCode + systemPrompt)"
    );

    // Check claudeCode.options.disallowedTools
    let claude_code_value = meta_map.get("claudeCode");
    assert!(
        claude_code_value.is_some(),
        "claude-code _meta contains claudeCode"
    );
    let claude_code_obj = claude_code_value.unwrap().as_object().unwrap();
    let options_obj = claude_code_obj.get("options").unwrap().as_object().unwrap();
    let disallowed = options_obj
        .get("disallowedTools")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(disallowed.len(), 1);
    assert_eq!(disallowed[0].as_str(), Some("Task"));

    // Check systemPrompt.append
    let system_prompt_value = meta_map.get("systemPrompt");
    assert!(
        system_prompt_value.is_some(),
        "claude-code _meta contains systemPrompt"
    );
    let system_prompt_obj = system_prompt_value.unwrap().as_object();
    assert!(
        system_prompt_obj.is_some(),
        "claude-code _meta.systemPrompt is an object"
    );
    let obj = system_prompt_obj.unwrap();
    assert_eq!(
        obj.len(),
        2,
        "claude-code _meta.systemPrompt has exactly two keys (append + excludeDynamicSections)"
    );
    let append_value = obj.get("append");
    assert!(
        append_value.is_some(),
        "claude-code _meta.systemPrompt contains append"
    );
    assert_eq!(
        append_value.unwrap().as_str(),
        Some("Test prompt"),
        "claude-code _meta.systemPrompt.append is the prompt text"
    );
    assert_eq!(
        obj.get("excludeDynamicSections")
            .and_then(serde_json::Value::as_bool),
        Some(true),
        "claude-code _meta.systemPrompt.excludeDynamicSections is true"
    );
}

/// monorepo#3151: codex `_meta` carries ONLY `sessionTitle` (the task-derived
/// agent name) — the system prompt stays on the first-turn prepend fallback
/// (#479) and is never moved into `_meta`, regardless of the prompt.
#[test]
fn codex_meta_carries_session_title_only() {
    for prompt in [Some("Test prompt"), None, Some(""), Some("   \n\t  ")] {
        let meta = build_session_meta("codex", prompt, Some("Fix login bug"));
        let meta_map = meta.expect("codex gets _meta when a session title is present");
        assert_eq!(
            meta_map.len(),
            1,
            "codex _meta has exactly one key (sessionTitle) — no systemPrompt (prompt {prompt:?})"
        );
        assert_eq!(
            meta_map.get("sessionTitle").and_then(|v| v.as_str()),
            Some("Fix login bug"),
            "codex _meta.sessionTitle is the task-derived agent name"
        );
    }
}

/// monorepo#3151: the session title is trimmed before emission.
#[test]
fn codex_session_title_is_trimmed() {
    let meta = build_session_meta("codex", None, Some("  Fix login bug  "));
    let meta_map = meta.expect("codex gets _meta");
    assert_eq!(
        meta_map.get("sessionTitle").and_then(|v| v.as_str()),
        Some("Fix login bug"),
        "sessionTitle is trimmed"
    );
}

/// monorepo#3151: without a usable title codex builds NO `_meta` — the
/// resume (`session/load`) path passes `None` and stays unchanged, and blank
/// names never emit an empty title.
#[test]
fn codex_without_title_gets_no_meta() {
    for title in [None, Some(""), Some("   \n\t  ")] {
        let meta = build_session_meta("codex", Some("Test prompt"), title);
        assert!(
            meta.is_none(),
            "codex without a non-blank title builds no _meta (title {title:?})"
        );
    }
}

#[test]
fn auggie_gets_no_meta() {
    let meta = build_session_meta("auggie", Some("Test prompt"), Some("Builder"));
    assert!(
        meta.is_none(),
        "auggie uses --rules flag, not _meta injection"
    );
}

#[test]
fn droid_gets_no_meta() {
    let meta = build_session_meta("droid", Some("Test prompt"), Some("Builder"));
    assert!(
        meta.is_none(),
        "droid uses --append-system-prompt-file flag, not _meta"
    );
}

#[test]
fn opencode_gets_no_meta() {
    let meta = build_session_meta("opencode", Some("Test prompt"), Some("Builder"));
    assert!(
        meta.is_none(),
        "opencode uses OPENCODE_CONFIG_CONTENT env, not _meta"
    );
}

#[test]
fn cortex_gets_no_meta() {
    let meta = build_session_meta("cortex", Some("Test prompt"), Some("Builder"));
    assert!(
        meta.is_none(),
        "cortex uses first-turn prepend fallback, not _meta"
    );
}

#[test]
fn mock_gets_no_meta() {
    let meta = build_session_meta("mock", Some("Test prompt"), Some("Builder"));
    assert!(
        meta.is_none(),
        "mock uses first-turn prepend fallback, not _meta"
    );
}

#[test]
fn resolved_provider_with_claude_code_compound_model_gets_meta() {
    // When model is "claude-code:sonnet4.5", resolve_provider_id extracts "claude-code"
    let provider_id =
        resolve_provider_id(Some("claude-code:sonnet4.5"), Some("auggie"), None).unwrap();
    let meta = build_session_meta(&provider_id, Some("Test prompt"), Some("Builder"));
    assert!(
        meta.is_some(),
        "claude-code compound model → claude-code provider → _meta"
    );
}

#[test]
fn claude_code_no_prompt_still_injects_disallowed_tools() {
    let meta = build_session_meta("claude-code", None, Some("Builder"));
    assert!(
        meta.is_some(),
        "claude-code always gets _meta (disallowedTools)"
    );
    let meta_map = meta.unwrap();
    assert_eq!(
        meta_map.len(),
        1,
        "claude-code _meta with no prompt has only claudeCode key"
    );

    // Check claudeCode.options.disallowedTools
    let claude_code_value = meta_map.get("claudeCode");
    assert!(
        claude_code_value.is_some(),
        "claude-code _meta contains claudeCode"
    );
    let claude_code_obj = claude_code_value.unwrap().as_object().unwrap();
    let options_obj = claude_code_obj.get("options").unwrap().as_object().unwrap();
    let disallowed = options_obj
        .get("disallowedTools")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(disallowed.len(), 1);
    assert_eq!(disallowed[0].as_str(), Some("Task"));

    // No systemPrompt key
    assert!(meta_map.get("systemPrompt").is_none());
}

/// Regression (monorepo#3044): with nothing to resolve from, resolution
/// yields `None` instead of a positional default provider.
#[test]
fn resolved_default_provider_with_no_model_no_provider_returns_none() {
    let provider_id = resolve_provider_id(None, None, None);
    assert_eq!(
        provider_id, None,
        "no model, no provider, no default → None (callers fail loudly)"
    );
}

#[test]
fn unknown_provider_returns_none() {
    let meta = build_session_meta("unknown-provider", Some("Test prompt"), Some("Builder"));
    assert!(
        meta.is_none(),
        "unknown provider id → no _meta (fallback to first-turn prepend)"
    );
}

#[test]
fn claude_code_blank_prompt_still_injects_disallowed_tools() {
    let meta = build_session_meta("claude-code", Some(""), Some("Builder"));
    assert!(
        meta.is_some(),
        "claude-code always gets _meta (disallowedTools)"
    );
    let meta_map = meta.unwrap();
    assert_eq!(
        meta_map.len(),
        1,
        "claude-code _meta with blank prompt has only claudeCode key"
    );

    // Check claudeCode.options.disallowedTools
    let claude_code_value = meta_map.get("claudeCode");
    assert!(
        claude_code_value.is_some(),
        "claude-code _meta contains claudeCode"
    );
    let claude_code_obj = claude_code_value.unwrap().as_object().unwrap();
    let options_obj = claude_code_obj.get("options").unwrap().as_object().unwrap();
    let disallowed = options_obj
        .get("disallowedTools")
        .unwrap()
        .as_array()
        .unwrap();
    assert_eq!(disallowed.len(), 1);
    assert_eq!(disallowed[0].as_str(), Some("Task"));

    // No systemPrompt key (blank filtered)
    assert!(meta_map.get("systemPrompt").is_none());
}

/// Parse `configOptions` JSON into the typed schema vec for the
/// [`resolve_effective_model`] tests.
fn config_options(v: serde_json::Value) -> Vec<intent_acp::session::SessionConfigOption> {
    serde_json::from_value(v).expect("valid configOptions")
}

#[test]
fn resolve_effective_model_from_claude_code_default() {
    use super::resolve_effective_model;
    // Canned from a live claude-agent-acp@0.60.0 session/new result: the
    // default option's name carries no family; its description does.
    let options = config_options(serde_json::json!([
        { "id": "mode", "name": "Mode", "category": "mode", "type": "select",
          "currentValue": "acceptEdits",
          "options": [ { "value": "acceptEdits", "name": "Accept Edits" } ] },
        { "id": "model", "name": "Model", "description": "AI model to use",
          "category": "model", "type": "select", "currentValue": "default",
          "options": [
            { "value": "default", "name": "Default (recommended)",
              "description": "Opus 4.8 with 1M context · Best for everyday, complex tasks" },
            { "value": "sonnet", "name": "Sonnet",
              "description": "Sonnet 5 · Efficient for routine tasks" }
          ] }
    ]));
    assert_eq!(
        resolve_effective_model(Some(&options)),
        Some("Opus 4.8".to_string()),
        "currentValue 'default' resolves via its option's description"
    );
}

#[test]
fn resolve_effective_model_prefers_name_then_description_then_value() {
    use super::resolve_effective_model;
    // A version-bearing family in the option NAME wins outright.
    let options = config_options(serde_json::json!([
        { "id": "model", "name": "Model", "type": "select", "currentValue": "sonnet",
          "options": [ { "value": "sonnet", "name": "Sonnet 5",
                         "description": "Efficient for routine tasks" } ] }
    ]));
    assert_eq!(
        resolve_effective_model(Some(&options)),
        Some("Sonnet 5".to_string())
    );
    // A version-less name ("Opus") is skipped in favor of the description.
    let options = config_options(serde_json::json!([
        { "id": "model", "name": "Model", "type": "select", "currentValue": "opus[1m]",
          "options": [ { "value": "opus[1m]", "name": "Opus",
                         "description": "Opus 4.8 with 1M context" } ] }
    ]));
    assert_eq!(
        resolve_effective_model(Some(&options)),
        Some("Opus 4.8".to_string())
    );
    // No option entry for currentValue → the raw value itself is the last
    // candidate (a real version-bearing model id resolves).
    let options = config_options(serde_json::json!([
        { "id": "model", "name": "Model", "type": "select",
          "currentValue": "claude-haiku-4-5", "options": [] }
    ]));
    assert_eq!(
        resolve_effective_model(Some(&options)),
        Some("Haiku 4.5".to_string())
    );
}

#[test]
fn resolve_effective_model_none_when_unresolvable() {
    use super::resolve_effective_model;
    // No configOptions at all.
    assert_eq!(resolve_effective_model(None), None);
    assert_eq!(resolve_effective_model(Some(&[])), None);
    // A model select whose strings carry no version-bearing family.
    let options = config_options(serde_json::json!([
        { "id": "model", "name": "Model", "type": "select", "currentValue": "default",
          "options": [ { "value": "default", "name": "Default (recommended)",
                         "description": "Best for everyday tasks" } ] }
    ]));
    assert_eq!(resolve_effective_model(Some(&options)), None);
    // No id=="model" select, but a category=="model" sibling matches.
    let options = config_options(serde_json::json!([
        { "id": "primary-model", "name": "Model", "category": "model", "type": "select",
          "currentValue": "x",
          "options": [ { "value": "x", "name": "Gemini 2.5 Pro" } ] }
    ]));
    assert_eq!(
        resolve_effective_model(Some(&options)),
        Some("Gemini 2.5 Pro".to_string()),
        "category fallback finds the model select"
    );
}

/// A model select shaped like a live claude-agent-acp option list carrying
/// bracketed explicit ids (D14).
fn explicit_pick_options() -> Vec<intent_acp::session::SessionConfigOption> {
    config_options(serde_json::json!([
        { "id": "model", "name": "Model", "description": "AI model to use",
          "category": "model", "type": "select", "currentValue": "default",
          "options": [
            { "value": "default", "name": "Default (recommended)",
              "description": "Opus 4.8 with 1M context · Best for everyday, complex tasks" },
            { "value": "claude-fable-5[1m]", "name": "Fable",
              "description": "Fable 5 with 1M context · Powerful model for complex work" },
            { "value": "sonnet", "name": "Sonnet",
              "description": "Sonnet 5 · Efficient for routine tasks" }
          ] }
    ]))
}

#[test]
fn resolve_explicit_display_model_matches_option_by_value() {
    use super::resolve_explicit_display_model;
    let options = explicit_pick_options();
    // The bare explicit id matches its option entry; the version-less name
    // ("Fable") is skipped in favor of the version-bearing description.
    assert_eq!(
        resolve_explicit_display_model("claude-fable-5[1m]", Some(&options)),
        Some("Fable 5".to_string())
    );
    // The name itself resolves when it carries a version... via description
    // here for "sonnet" (name "Sonnet" is version-less).
    assert_eq!(
        resolve_explicit_display_model("sonnet", Some(&options)),
        Some("Sonnet 5".to_string())
    );
    // currentValue ("default") plays no part: an id with no option entry
    // resolves to None even though the select's currentValue would.
    assert_eq!(
        resolve_explicit_display_model("claude-haiku-4-5", Some(&options)),
        None,
        "unmatched id must not fall back to currentValue or the raw id"
    );
    assert_eq!(resolve_explicit_display_model("sonnet", None), None);
}
