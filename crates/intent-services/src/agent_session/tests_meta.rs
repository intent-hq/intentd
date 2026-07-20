//! Tests for `_meta` payload construction (§18.1 system-prompt injection via
//! ACP extensibility).

use super::{build_session_meta, resolve_provider_id};

#[test]
fn resolve_provider_id_from_compound_model() {
    // Compound model id takes precedence over provider field
    let provider_id = resolve_provider_id(Some("opencode:kimi-k3"), Some("claude-code"));
    assert_eq!(provider_id, "opencode", "compound model prefix wins");
}

#[test]
fn resolve_provider_id_from_provider_field() {
    // Bare model id (no colon) falls back to provider field
    let provider_id = resolve_provider_id(Some("gpt-5.3-codex"), Some("codex"));
    assert_eq!(
        provider_id, "codex",
        "provider field is fallback for bare model"
    );
}

#[test]
fn resolve_provider_id_none_uses_default() {
    // Both None -> default provider
    let provider_id = resolve_provider_id(None, None);
    assert_eq!(
        provider_id,
        intent_providers::default_provider_id(),
        "None model + None provider -> default"
    );
}

#[test]
fn resolve_provider_id_bare_model_none_provider_uses_default() {
    // Bare model + None provider -> default provider
    let provider_id = resolve_provider_id(Some("sonnet4.5"), None);
    assert_eq!(
        provider_id,
        intent_providers::default_provider_id(),
        "bare model + None provider -> default"
    );
}

#[test]
fn resolve_provider_id_malformed_compound_falls_back() {
    // Malformed compound id like ":sonnet" yields empty prefix -> falls back to provider field
    let provider_id = resolve_provider_id(Some(":sonnet"), Some("codex"));
    assert_eq!(
        provider_id, "codex",
        "malformed compound :sonnet falls back to provider field"
    );

    // Malformed compound with no provider field -> default
    let provider_id = resolve_provider_id(Some(":sonnet"), None);
    assert_eq!(
        provider_id,
        intent_providers::default_provider_id(),
        "malformed compound :sonnet with no provider -> default"
    );
}

#[test]
fn claude_code_meta_appends_system_prompt() {
    let meta = build_session_meta("claude-code", Some("Test prompt"));
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
        1,
        "claude-code _meta.systemPrompt has exactly one key"
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
}

#[test]
fn codex_meta_has_developer_instructions() {
    let meta = build_session_meta("codex", Some("Test prompt"));
    assert!(meta.is_some(), "codex gets _meta");
    let meta_map = meta.unwrap();
    assert_eq!(meta_map.len(), 1, "codex _meta has exactly one key");
    let dev_instructions = meta_map.get("developerInstructions");
    assert!(
        dev_instructions.is_some(),
        "codex _meta contains developerInstructions"
    );
    assert_eq!(
        dev_instructions.unwrap().as_str(),
        Some("Test prompt"),
        "codex _meta.developerInstructions is the prompt text"
    );
}

#[test]
fn auggie_gets_no_meta() {
    let meta = build_session_meta("auggie", Some("Test prompt"));
    assert!(
        meta.is_none(),
        "auggie uses --rules flag, not _meta injection"
    );
}

#[test]
fn droid_gets_no_meta() {
    let meta = build_session_meta("droid", Some("Test prompt"));
    assert!(
        meta.is_none(),
        "droid uses --append-system-prompt-file flag, not _meta"
    );
}

#[test]
fn opencode_gets_no_meta() {
    let meta = build_session_meta("opencode", Some("Test prompt"));
    assert!(
        meta.is_none(),
        "opencode uses OPENCODE_CONFIG_CONTENT env, not _meta"
    );
}

#[test]
fn cortex_gets_no_meta() {
    let meta = build_session_meta("cortex", Some("Test prompt"));
    assert!(
        meta.is_none(),
        "cortex uses first-turn prepend fallback, not _meta"
    );
}

#[test]
fn mock_gets_no_meta() {
    let meta = build_session_meta("mock", Some("Test prompt"));
    assert!(
        meta.is_none(),
        "mock uses first-turn prepend fallback, not _meta"
    );
}

#[test]
fn resolved_provider_with_claude_code_compound_model_gets_meta() {
    // When model is "claude-code:sonnet4.5", resolve_provider_id extracts "claude-code"
    let provider_id = resolve_provider_id(Some("claude-code:sonnet4.5"), Some("auggie"));
    let meta = build_session_meta(&provider_id, Some("Test prompt"));
    assert!(
        meta.is_some(),
        "claude-code compound model → claude-code provider → _meta"
    );
}

#[test]
fn codex_no_prompt_returns_none() {
    let meta = build_session_meta("codex", None);
    assert!(meta.is_none(), "codex with no prompt → no _meta");
}

#[test]
fn codex_blank_prompt_returns_none() {
    let meta = build_session_meta("codex", Some(""));
    assert!(meta.is_none(), "codex with blank prompt → no _meta");
}

#[test]
fn claude_code_no_prompt_still_injects_disallowed_tools() {
    let meta = build_session_meta("claude-code", None);
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

#[test]
fn resolved_default_provider_with_no_model_no_provider_returns_none() {
    // When both model and provider are None, resolve_provider_id returns default provider
    let provider_id = resolve_provider_id(None, None);
    // Default provider (auggie) doesn't use _meta
    let meta = build_session_meta(&provider_id, Some("Test prompt"));
    assert!(
        meta.is_none(),
        "default provider (auggie) + prompt → no _meta (uses --rules)"
    );
}

#[test]
fn unknown_provider_returns_none() {
    let meta = build_session_meta("unknown-provider", Some("Test prompt"));
    assert!(
        meta.is_none(),
        "unknown provider id → no _meta (fallback to first-turn prepend)"
    );
}

#[test]
fn claude_code_blank_prompt_still_injects_disallowed_tools() {
    let meta = build_session_meta("claude-code", Some(""));
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

#[test]
fn whitespace_prompt_returns_none() {
    let meta = build_session_meta("codex", Some("   \n\t  "));
    assert!(meta.is_none(), "whitespace-only prompt → no _meta");
}
