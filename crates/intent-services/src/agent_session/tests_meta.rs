//! Tests for `_meta` payload construction (§18.1 system-prompt injection via
//! ACP extensibility).

use super::build_session_meta;

#[test]
fn claude_code_meta_appends_system_prompt() {
    let meta = build_session_meta(Some("claude-code"), Some("Test prompt"));
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
    let meta = build_session_meta(Some("codex"), Some("Test prompt"));
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
    let meta = build_session_meta(Some("auggie"), Some("Test prompt"));
    assert!(
        meta.is_none(),
        "auggie uses --rules flag, not _meta injection"
    );
}

#[test]
fn droid_gets_no_meta() {
    let meta = build_session_meta(Some("droid"), Some("Test prompt"));
    assert!(
        meta.is_none(),
        "droid uses --append-system-prompt-file flag, not _meta"
    );
}

#[test]
fn opencode_gets_no_meta() {
    let meta = build_session_meta(Some("opencode"), Some("Test prompt"));
    assert!(
        meta.is_none(),
        "opencode uses OPENCODE_CONFIG_CONTENT env, not _meta"
    );
}

#[test]
fn cortex_gets_no_meta() {
    let meta = build_session_meta(Some("cortex"), Some("Test prompt"));
    assert!(
        meta.is_none(),
        "cortex uses first-turn prepend fallback, not _meta"
    );
}

#[test]
fn mock_gets_no_meta() {
    let meta = build_session_meta(Some("mock"), Some("Test prompt"));
    assert!(
        meta.is_none(),
        "mock uses first-turn prepend fallback, not _meta"
    );
}

#[test]
fn no_provider_returns_none() {
    let meta = build_session_meta(None, Some("Test prompt"));
    assert!(meta.is_none(), "no provider id → no _meta");
}

#[test]
fn codex_no_prompt_returns_none() {
    let meta = build_session_meta(Some("codex"), None);
    assert!(meta.is_none(), "codex with no prompt → no _meta");
}

#[test]
fn codex_blank_prompt_returns_none() {
    let meta = build_session_meta(Some("codex"), Some(""));
    assert!(meta.is_none(), "codex with blank prompt → no _meta");
}

#[test]
fn claude_code_no_prompt_still_injects_disallowed_tools() {
    let meta = build_session_meta(Some("claude-code"), None);
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
fn both_missing_returns_none() {
    let meta = build_session_meta(None, None);
    assert!(meta.is_none(), "no provider or prompt → no _meta");
}

#[test]
fn unknown_provider_returns_none() {
    let meta = build_session_meta(Some("unknown-provider"), Some("Test prompt"));
    assert!(
        meta.is_none(),
        "unknown provider id → no _meta (fallback to first-turn prepend)"
    );
}

#[test]
fn claude_code_blank_prompt_still_injects_disallowed_tools() {
    let meta = build_session_meta(Some("claude-code"), Some(""));
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
    let meta = build_session_meta(Some("codex"), Some("   \n\t  "));
    assert!(meta.is_none(), "whitespace-only prompt → no _meta");
}
