//! Tests for `_meta` payload construction (§18.1 system-prompt injection via
//! ACP extensibility).

use super::{build_session_meta, derived_default_provider, resolve_provider_id};
use intent_core::settings_file::SettingsFile;

#[test]
fn resolve_provider_id_from_provider_field() {
    let provider_id = resolve_provider_id(Some("codex"), None);
    assert_eq!(
        provider_id.as_deref(),
        Some("codex"),
        "explicit provider field resolves"
    );
}

/// Regression (monorepo#3044): with nothing to resolve from there is NO
/// positional last resort — resolution yields `None` and callers fail loudly.
#[test]
fn resolve_provider_id_none_yields_none() {
    let provider_id = resolve_provider_id(None, None);
    assert_eq!(
        provider_id, None,
        "None provider + no default -> None (no positional fallback)"
    );
}

/// A [`SettingsFile`] with the given `model.defaultProvider`.
fn settings(default_provider: Option<&str>) -> SettingsFile {
    let mut s = SettingsFile::default();
    s.model.default_provider = default_provider.map(str::to_string);
    s
}

#[test]
fn derived_default_provider_reads_model_default_provider() {
    let s = settings(Some("claude-code"));
    assert_eq!(
        derived_default_provider(&s).as_deref(),
        Some("claude-code"),
        "model.defaultProvider is the settings-derived default"
    );
}

#[test]
fn derived_default_provider_unknown_yields_none() {
    // An unregistered id (stale value, typo, foreign-build id) is not
    // surfaced.
    let s = settings(Some("not-a-provider"));
    assert_eq!(
        derived_default_provider(&s),
        None,
        "unknown model.defaultProvider is not surfaced"
    );
}

#[test]
fn derived_default_provider_trims_whitespace() {
    // Padded settings values still resolve to the registered id.
    let s = settings(Some("  claude-code  "));
    assert_eq!(
        derived_default_provider(&s).as_deref(),
        Some("claude-code"),
        "whitespace-padded model.defaultProvider resolves trimmed"
    );
}

#[test]
fn derived_default_provider_unset_yields_none() {
    let s = settings(None);
    assert_eq!(
        derived_default_provider(&s),
        None,
        "unset -> None (callers fail loudly, monorepo#3044)"
    );
}

/// The deprecated `providers.active` is no longer consulted: only
/// `model.defaultProvider` derives the default.
#[test]
fn derived_default_provider_ignores_providers_active() {
    let mut s = settings(None);
    s.providers.active = Some("claude-code".to_string());
    assert_eq!(
        derived_default_provider(&s),
        None,
        "providers.active is deprecated and never read"
    );

    let mut s = settings(Some("codex"));
    s.providers.active = Some("claude-code".to_string());
    assert_eq!(
        derived_default_provider(&s).as_deref(),
        Some("codex"),
        "model.defaultProvider rules regardless of providers.active"
    );
}

/// `configured_default` (spec Decision D2) resolves when the `provider`
/// field is absent.
#[test]
fn resolve_provider_id_uses_configured_default() {
    let provider_id = resolve_provider_id(None, Some("claude-code"));
    assert_eq!(
        provider_id.as_deref(),
        Some("claude-code"),
        "configured default resolves when nothing stronger is present"
    );
}

/// The session's `provider` field wins over `configured_default` — a
/// persisted session's own provider is a stronger signal than the ambient
/// default at read time.
#[test]
fn resolve_provider_id_provider_field_wins_over_configured_default() {
    let provider_id = resolve_provider_id(Some("codex"), Some("claude-code"));
    assert_eq!(provider_id.as_deref(), Some("codex"));
}

/// Empty strings are treated the same as `None` — resolution yields `None`
/// rather than an empty provider id (monorepo#3044: no positional fallback).
#[test]
fn resolve_provider_id_empty_values_yield_none() {
    assert_eq!(resolve_provider_id(Some(""), None), None);
    assert_eq!(resolve_provider_id(None, Some("")), None);
}

#[test]
fn claude_code_meta_replaces_system_prompt() {
    let meta = build_session_meta("claude-code", Some("Test prompt"), Some("Builder"), false);
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

    // Check systemPrompt is a plain string (full replacement of the
    // claude_code preset prompt — not the { append, ... } object shape).
    let system_prompt_value = meta_map.get("systemPrompt");
    assert!(
        system_prompt_value.is_some(),
        "claude-code _meta contains systemPrompt"
    );
    assert_eq!(
        system_prompt_value.unwrap().as_str(),
        Some("Test prompt"),
        "claude-code _meta.systemPrompt is the prompt text as a plain string (full replacement)"
    );
}

/// monorepo#3151: codex `_meta` carries ONLY `sessionTitle` (the task-derived
/// agent name) — the system prompt stays on the first-turn prepend fallback
/// (#479) and is never moved into `_meta`, regardless of the prompt.
#[test]
fn codex_meta_carries_session_title_only() {
    for prompt in [Some("Test prompt"), None, Some(""), Some("   \n\t  ")] {
        let meta = build_session_meta("codex", prompt, Some("Fix login bug"), false);
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
    let meta = build_session_meta("codex", None, Some("  Fix login bug  "), false);
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
        let meta = build_session_meta("codex", Some("Test prompt"), title, false);
        assert!(
            meta.is_none(),
            "codex without a non-blank title builds no _meta (title {title:?})"
        );
    }
}

#[test]
fn auggie_gets_no_meta() {
    let meta = build_session_meta("auggie", Some("Test prompt"), Some("Builder"), false);
    assert!(
        meta.is_none(),
        "auggie uses --rules flag, not _meta injection"
    );
}

#[test]
fn droid_gets_no_meta() {
    let meta = build_session_meta("droid", Some("Test prompt"), Some("Builder"), false);
    assert!(
        meta.is_none(),
        "droid uses --append-system-prompt-file flag, not _meta"
    );
}

#[test]
fn opencode_gets_no_meta() {
    let meta = build_session_meta("opencode", Some("Test prompt"), Some("Builder"), false);
    assert!(
        meta.is_none(),
        "opencode uses OPENCODE_CONFIG_CONTENT env, not _meta"
    );
}

#[test]
fn cortex_gets_no_meta() {
    let meta = build_session_meta("cortex", Some("Test prompt"), Some("Builder"), false);
    assert!(
        meta.is_none(),
        "cortex uses first-turn prepend fallback, not _meta"
    );
}

#[test]
fn mock_gets_no_meta() {
    let meta = build_session_meta("mock", Some("Test prompt"), Some("Builder"), false);
    assert!(
        meta.is_none(),
        "mock uses first-turn prepend fallback, not _meta"
    );
}

#[test]
fn resolved_provider_with_claude_code_provider_field_gets_meta() {
    let provider_id = resolve_provider_id(Some("claude-code"), None).unwrap();
    let meta = build_session_meta(&provider_id, Some("Test prompt"), Some("Builder"), false);
    assert!(
        meta.is_some(),
        "claude-code provider field → claude-code provider → _meta"
    );
}

#[test]
fn claude_code_no_prompt_still_injects_disallowed_tools() {
    let meta = build_session_meta("claude-code", None, Some("Builder"), false);
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
fn resolved_default_provider_with_no_provider_returns_none() {
    let provider_id = resolve_provider_id(None, None);
    assert_eq!(
        provider_id, None,
        "no provider, no default → None (callers fail loudly)"
    );
}

#[test]
fn unknown_provider_returns_none() {
    let meta = build_session_meta(
        "unknown-provider",
        Some("Test prompt"),
        Some("Builder"),
        false,
    );
    assert!(
        meta.is_none(),
        "unknown provider id → no _meta (fallback to first-turn prepend)"
    );
}

#[test]
fn claude_code_blank_prompt_still_injects_disallowed_tools() {
    let meta = build_session_meta("claude-code", Some(""), Some("Builder"), false);
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

/// Extract `claudeCode.options.disallowedTools` from a built `_meta` map.
fn disallowed_tools(meta_map: &intent_acp::session::Meta) -> Vec<String> {
    meta_map
        .get("claudeCode")
        .and_then(|v| v.get("options"))
        .and_then(|v| v.get("disallowedTools"))
        .and_then(|v| v.as_array())
        .expect("claudeCode.options.disallowedTools present")
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect()
}

/// §18.4: orchestrator-role agents on claude-code get the SDK's built-in
/// file-write tools appended to `disallowedTools` (after the always-present
/// `Task`), removing Edit/Write/NotebookEdit from the model's context.
#[test]
fn claude_code_orchestrator_disallows_file_write_tools() {
    let meta = build_session_meta("claude-code", Some("Test prompt"), Some("Builder"), true);
    let disallowed = disallowed_tools(&meta.expect("claude-code gets _meta"));
    assert_eq!(
        disallowed,
        vec!["Task", "Edit", "Write", "NotebookEdit"],
        "orchestrator disallowedTools = Task + CLAUDE_CODE_ORCHESTRATOR_DISALLOWED_TOOLS"
    );
}

/// Non-orchestrators keep only the always-present `Task` denial — the
/// file-write tools stay available.
#[test]
fn claude_code_non_orchestrator_keeps_file_write_tools() {
    let meta = build_session_meta("claude-code", Some("Test prompt"), Some("Builder"), false);
    let disallowed = disallowed_tools(&meta.expect("claude-code gets _meta"));
    assert_eq!(
        disallowed,
        vec!["Task"],
        "non-orchestrator disallowedTools carries only Task"
    );
}

/// The orchestrator flag is claude-code-specific: other providers' `_meta`
/// (or lack of it) is unchanged by it.
#[test]
fn is_orchestrator_does_not_affect_other_providers() {
    let meta = build_session_meta("codex", Some("Test prompt"), Some("Fix login bug"), true);
    let meta_map = meta.expect("codex gets _meta when a session title is present");
    assert_eq!(meta_map.len(), 1, "codex _meta still only sessionTitle");
    assert!(meta_map.get("claudeCode").is_none());
    assert!(
        build_session_meta("auggie", Some("Test prompt"), Some("Builder"), true).is_none(),
        "auggie still builds no _meta regardless of orchestrator role"
    );
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
