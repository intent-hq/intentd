//! Unit tests for the provider registry, arg/env assembly, PATH enrichment,
//! and model resolution — parity-checked against `provider-config.ts`.

use super::*;

#[test]
fn registry_default_and_lookups() {
    assert_eq!(default_provider_id(), "auggie");
    assert!(default_provider_config().is_default);
    assert_eq!(
        all_provider_ids(),
        vec![
            "auggie",
            "claude-code",
            "codex",
            "cortex",
            "opencode",
            "droid",
            "mock"
        ]
    );
    assert!(find_provider("nope").is_none());
    // Unknown ids fall back to the default provider.
    assert_eq!(provider_config("nope").id, "auggie");
    assert_eq!(find_provider("codex").unwrap().command, "codex-acp");
}

#[test]
fn registry_field_parity() {
    let auggie = find_provider("auggie").unwrap();
    assert_eq!(auggie.display_name, "Augment Auggie");
    assert_eq!(auggie.command, "auggie");
    assert_eq!(auggie.base_args, &["--acp", "--allow-indexing"]);
    assert_eq!(auggie.model_flag, Some("--model"));
    assert_eq!(auggie.rules_flag, Some("--rules"));
    assert_eq!(auggie.mcp_config_flag, Some("--mcp-config"));
    assert_eq!(auggie.quiet_flag, Some("--quiet"));
    assert_eq!(auggie.remove_tool_flag, Some("--remove-tool"));
    assert!(auggie.supports_authenticate && auggie.supports_set_mode);
    assert!(auggie.supports_mcp_config && auggie.supports_rules_file);
    assert!(!auggie.can_be_disabled);
    assert_eq!(auggie.login_command_hint, Some("auggie login"));

    let cc = find_provider("claude-code").unwrap();
    assert_eq!(cc.command, "claude-agent-acp");
    assert_eq!(cc.base_args, &[] as &[&str]);
    assert_eq!(cc.auth_check_args, Some(&["auth", "status"][..]));
    assert!(cc.model_flag.is_none() && cc.can_be_disabled);

    let codex = find_provider("codex").unwrap();
    assert_eq!(codex.auth_check_args, Some(&["login", "status"][..]));

    let cortex = find_provider("cortex").unwrap();
    assert_eq!(cortex.command, "cortex-acp");
    assert_eq!(cortex.requires_feature_code, Some("cortex"));

    let oc = find_provider("opencode").unwrap();
    assert_eq!(oc.base_args, &["acp"]);
    assert_eq!(oc.auth_check_args, Some(&["models"][..]));
    assert!(oc.model_flag.is_none());

    let droid = find_provider("droid").unwrap();
    assert_eq!(droid.base_args, &["exec", "--output-format", "acp"]);
    assert_eq!(droid.model_flag, Some("--model"));
    assert!(droid.supports_rules_file);
    assert_eq!(droid.rules_flag, Some("--append-system-prompt-file"));

    let mock = find_provider("mock").unwrap();
    assert_eq!(mock.command, "node");
    assert_eq!(mock.requires_env_var, Some("MOCK_AGENT_SCRIPT_PATH"));
}

#[test]
fn auth_error_pattern_matching() {
    assert!(is_provider_authentication_error(
        "auggie",
        "Error: Authentication Required to continue"
    ));
    assert!(!is_provider_authentication_error(
        "auggie",
        "some other error"
    ));
    // Providers without patterns never match.
    assert!(!is_provider_authentication_error(
        "codex",
        "authentication required"
    ));
}

#[test]
fn arg_assembly_auggie() {
    let auggie = find_provider("auggie").unwrap();
    let base = build_provider_args(auggie, &ArgInputs::default());
    assert_eq!(base, vec!["--acp", "--allow-indexing"]);

    let full = build_provider_args(
        auggie,
        &ArgInputs {
            model: Some("sonnet4.5"),
            rules_file: Some("/tmp/rules.md"),
            mcp_config_file: Some("/tmp/mcp.json"),
            quiet: true,
            ..Default::default()
        },
    );
    assert_eq!(
        full,
        vec![
            "--acp",
            "--allow-indexing",
            "--model",
            "sonnet4.5",
            "--quiet",
            "--rules",
            "/tmp/rules.md",
            "--mcp-config",
            "/tmp/mcp.json",
        ]
    );
}

#[test]
fn arg_assembly_respects_capabilities_and_sentinel() {
    // 'default' sentinel is never passed as a real model id.
    let droid = find_provider("droid").unwrap();
    assert_eq!(
        build_provider_args(
            droid,
            &ArgInputs {
                model: Some("default"),
                ..Default::default()
            }
        ),
        vec!["exec", "--output-format", "acp"]
    );
    // claude-code has no model flag and no rules/mcp support: flags are dropped.
    let cc = find_provider("claude-code").unwrap();
    assert!(build_provider_args(
        cc,
        &ArgInputs {
            model: Some("sonnet"),
            rules_file: Some("/tmp/r.md"),
            mcp_config_file: Some("/tmp/m.json"),
            quiet: true,
            ..Default::default()
        }
    )
    .is_empty());
    // opencode passes model via env, not args.
    let oc = find_provider("opencode").unwrap();
    assert_eq!(
        build_provider_args(
            oc,
            &ArgInputs {
                model: Some("claude-sonnet-4"),
                ..Default::default()
            }
        ),
        vec!["acp"]
    );
}

#[test]
fn arg_assembly_emits_remove_tool_flags_for_auggie() {
    let auggie = find_provider("auggie").unwrap();
    let args = build_provider_args(
        auggie,
        &ArgInputs {
            tools_to_remove: &["str-replace-editor", "sub-agent-explore"],
            ..Default::default()
        },
    );
    assert_eq!(
        args,
        vec![
            "--acp",
            "--allow-indexing",
            "--remove-tool",
            "str-replace-editor",
            "--remove-tool",
            "sub-agent-explore",
        ]
    );
}

#[test]
fn arg_assembly_dedupes_remove_tool_names() {
    let auggie = find_provider("auggie").unwrap();
    let args = build_provider_args(
        auggie,
        &ArgInputs {
            tools_to_remove: &["str-replace-editor", "str-replace-editor", ""],
            ..Default::default()
        },
    );
    // Empty names are skipped and duplicates are collapsed.
    assert_eq!(
        args,
        vec![
            "--acp",
            "--allow-indexing",
            "--remove-tool",
            "str-replace-editor",
        ]
    );
}

#[test]
fn arg_assembly_skips_remove_tool_for_providers_without_support() {
    // Providers with `remove_tool_flag = None` silently drop the input — we
    // never pass an unknown flag to claude/codex/cortex/opencode/droid.
    for id in ["claude-code", "codex", "cortex", "opencode", "droid"] {
        let provider = find_provider(id).unwrap();
        assert!(
            provider.remove_tool_flag.is_none(),
            "{id} unexpectedly opted into --remove-tool"
        );
        let args = build_provider_args(
            provider,
            &ArgInputs {
                tools_to_remove: &["str-replace-editor", "sub-agent-explore"],
                ..Default::default()
            },
        );
        assert!(
            !args.iter().any(|a| a == "--remove-tool"),
            "{id} unexpectedly received a --remove-tool flag: {args:?}"
        );
    }
}

#[test]
fn arg_assembly_remove_tool_after_mcp_config() {
    // Emission order: base → model → quiet → rules → mcp → remove-tool.
    let auggie = find_provider("auggie").unwrap();
    let args = build_provider_args(
        auggie,
        &ArgInputs {
            model: Some("sonnet4.5"),
            rules_file: Some("/tmp/rules.md"),
            mcp_config_file: Some("/tmp/mcp.json"),
            quiet: true,
            tools_to_remove: &["str-replace-editor"],
        },
    );
    assert_eq!(
        args,
        vec![
            "--acp",
            "--allow-indexing",
            "--model",
            "sonnet4.5",
            "--quiet",
            "--rules",
            "/tmp/rules.md",
            "--mcp-config",
            "/tmp/mcp.json",
            "--remove-tool",
            "str-replace-editor",
        ]
    );
}

#[test]
fn provider_runtimes() {
    let runtime = |id: &str| find_provider(id).unwrap().runtime;
    assert_eq!(runtime("auggie"), ProviderRuntime::Node);
    assert_eq!(runtime("claude-code"), ProviderRuntime::Node);
    assert_eq!(runtime("opencode"), ProviderRuntime::Node);
    assert_eq!(runtime("mock"), ProviderRuntime::Node);
    assert_eq!(runtime("cortex"), ProviderRuntime::Electron);
    assert_eq!(runtime("codex"), ProviderRuntime::Native);
    assert_eq!(runtime("droid"), ProviderRuntime::Native);
}

#[test]
fn env_assembly_quirks() {
    let cortex = build_provider_env(find_provider("cortex").unwrap(), None, None);
    assert_eq!(
        cortex.get("ELECTRON_RUN_AS_NODE").map(String::as_str),
        Some("1")
    );

    let opencode = find_provider("opencode").unwrap();
    // With model: permission.task=deny is merged with the model key.
    let oc_with_model = build_provider_env(opencode, Some("claude-sonnet-4"), None);
    assert_eq!(
        oc_with_model
            .get("OPENCODE_CONFIG_CONTENT")
            .map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"model":"claude-sonnet-4"}"#)
    );
    // No model, no rules file: permission.task=deny is still emitted.
    let oc_no_model_no_rules = build_provider_env(opencode, None, None);
    assert_eq!(
        oc_no_model_no_rules
            .get("OPENCODE_CONFIG_CONTENT")
            .map(String::as_str),
        Some(r#"{"permission":{"task":"deny"}}"#)
    );
}

#[test]
fn opencode_model_sentinel_filtered_from_env() {
    let opencode = find_provider("opencode").unwrap();

    // Model sentinel "default" alone → permission.task=deny only (model filtered).
    let sentinel_only = build_provider_env(opencode, Some("default"), None);
    assert_eq!(
        sentinel_only
            .get("OPENCODE_CONFIG_CONTENT")
            .map(String::as_str),
        Some(r#"{"permission":{"task":"deny"}}"#)
    );

    // Rules file alone (no model) → permission + instructions.
    let rules_only = build_provider_env(opencode, None, Some("/tmp/rules.md"));
    assert_eq!(
        rules_only
            .get("OPENCODE_CONFIG_CONTENT")
            .map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"instructions":["/tmp/rules.md"]}"#)
    );

    // Real model + rules file → all three fields.
    let both = build_provider_env(opencode, Some("gpt-4"), Some("/tmp/rules.md"));
    assert_eq!(
        both.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"model":"gpt-4","instructions":["/tmp/rules.md"]}"#)
    );

    // Sentinel model + rules file → permission + instructions (model filtered).
    let sentinel_with_rules = build_provider_env(opencode, Some("default"), Some("/tmp/rules.md"));
    assert_eq!(
        sentinel_with_rules
            .get("OPENCODE_CONFIG_CONTENT")
            .map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"instructions":["/tmp/rules.md"]}"#)
    );
}

#[test]
fn json_escape_handles_control_characters() {
    use crate::args::json_escape;

    // Basic escaping.
    assert_eq!(json_escape(r#"foo"bar"#), r#"foo\"bar"#);
    assert_eq!(json_escape(r"foo\bar"), r"foo\\bar");
    assert_eq!(json_escape("foo\nbar"), r"foo\nbar");
    assert_eq!(json_escape("foo\rbar"), r"foo\rbar");
    assert_eq!(json_escape("foo\tbar"), r"foo\tbar");

    // Backspace and form feed.
    assert_eq!(json_escape("foo\x08bar"), r"foo\bbar");
    assert_eq!(json_escape("foo\x0Cbar"), r"foo\fbar");

    // Other control characters → \uXXXX.
    assert_eq!(json_escape("foo\x01bar"), r"foo\u0001bar");
    assert_eq!(json_escape("foo\x1Fbar"), r"foo\u001fbar");

    // Round-trip safety: a path with control chars produces valid JSON.
    let weird_path = "/tmp/rules\x08\x0C\x01.md";
    let escaped = json_escape(weird_path);
    // Verify the expected escaping.
    assert_eq!(escaped, r"/tmp/rules\b\f\u0001.md");
    // The escaped value can be embedded in a JSON string.
    let json = format!(r#"{{"path":"{}"}}"#, escaped);
    assert_eq!(json, r#"{"path":"/tmp/rules\b\f\u0001.md"}"#);
}

/// Serializes env-var mutation across tests in this binary: the vars are
/// process-global, so concurrent mutation would race with parallel tests.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Restores an env var to its prior state on drop so tests stay hermetic.
/// Snapshots via `var_os` so a pre-existing non-UTF8 value is restored
/// exactly rather than being dropped.
struct EnvGuard {
    key: &'static str,
    prev: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn new(key: &'static str) -> Self {
        Self {
            key,
            prev: std::env::var_os(key),
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var(self.key, v),
            None => std::env::remove_var(self.key),
        }
    }
}

/// STAB-50: NODE_OPTIONS heap-cap injection for V8-runtime (Node/Electron)
/// providers. All scenarios run inside one test fn because they mutate
/// process-global env vars — parallel test threads must not race on
/// `NODE_OPTIONS` / the override seam.
#[test]
fn v8_runtime_node_options_heap_cap() {
    let _env_lock = ENV_LOCK.lock().unwrap();
    let _node_options_guard = EnvGuard::new("NODE_OPTIONS");
    let _max_old_space_guard = EnvGuard::new("INTENTD_ACP_NODE_MAX_OLD_SPACE_MB");

    // Pure composition helper: append vs skip vs fresh.
    assert_eq!(
        args::node_options_with_heap_cap(None, 8192).as_deref(),
        Some("--max-old-space-size=8192")
    );
    assert_eq!(
        args::node_options_with_heap_cap(Some(""), 8192).as_deref(),
        Some("--max-old-space-size=8192")
    );
    // Inherited NODE_OPTIONS is appended to, not clobbered.
    assert_eq!(
        args::node_options_with_heap_cap(Some("--enable-source-maps"), 8192).as_deref(),
        Some("--enable-source-maps --max-old-space-size=8192")
    );
    // A user-set --max-old-space-size wins: no injection at all.
    assert_eq!(
        args::node_options_with_heap_cap(Some("--max-old-space-size=2048"), 8192),
        None
    );
    assert_eq!(
        args::node_options_with_heap_cap(
            Some("--enable-source-maps --max-old-space-size=2048"),
            8192
        ),
        None
    );

    let env_for = |id: &str| build_provider_env(find_provider(id).unwrap(), None, None);

    // Env-driven scenarios (serialized within this single test).
    std::env::remove_var("NODE_OPTIONS");
    std::env::remove_var("INTENTD_ACP_NODE_MAX_OLD_SPACE_MB");

    // Default cap is 8192 for every Node/Electron provider.
    assert_eq!(args::max_old_space_mb(), 8192);
    for id in ["auggie", "claude-code", "opencode", "cortex", "mock"] {
        assert_eq!(
            env_for(id).get("NODE_OPTIONS").map(String::as_str),
            Some("--max-old-space-size=8192"),
            "provider {id} should get the heap cap"
        );
    }
    // Native runtimes get no NODE_OPTIONS.
    for id in ["codex", "droid"] {
        assert!(
            !env_for(id).contains_key("NODE_OPTIONS"),
            "native provider {id} must not get NODE_OPTIONS"
        );
    }

    // Override seam produces the requested cap for all V8 providers.
    std::env::set_var("INTENTD_ACP_NODE_MAX_OLD_SPACE_MB", "4096");
    assert_eq!(args::max_old_space_mb(), 4096);
    for id in ["auggie", "claude-code", "opencode", "cortex", "mock"] {
        assert_eq!(
            env_for(id).get("NODE_OPTIONS").map(String::as_str),
            Some("--max-old-space-size=4096"),
            "provider {id} should honor the env override"
        );
    }

    // Unparseable override falls back to the default (WARN logged).
    std::env::set_var("INTENTD_ACP_NODE_MAX_OLD_SPACE_MB", "not-a-number");
    assert_eq!(args::max_old_space_mb(), 8192);
    std::env::remove_var("INTENTD_ACP_NODE_MAX_OLD_SPACE_MB");

    // Parent NODE_OPTIONS is appended to, not clobbered.
    std::env::set_var("NODE_OPTIONS", "--enable-source-maps");
    for id in ["auggie", "claude-code", "opencode", "cortex", "mock"] {
        assert_eq!(
            env_for(id).get("NODE_OPTIONS").map(String::as_str),
            Some("--enable-source-maps --max-old-space-size=8192"),
            "provider {id} should append to inherited NODE_OPTIONS"
        );
    }

    // Parent already caps old-space → left alone (no double flag).
    std::env::set_var("NODE_OPTIONS", "--max-old-space-size=2048");
    for id in ["auggie", "claude-code", "opencode", "cortex", "mock"] {
        assert!(
            !env_for(id).contains_key("NODE_OPTIONS"),
            "provider {id} must respect a user-set --max-old-space-size"
        );
    }
}

#[test]
fn enhanced_path_prepends_bin_and_augment() {
    std::env::set_var("HOME", "/home/tester");
    std::env::set_var("USERPROFILE", "/home/tester");
    std::env::set_var("PATH", "/usr/bin:/bin");
    let bin = std::path::PathBuf::from("/opt/tools/auggie");
    let path = enhanced_path(Some(&bin));
    let parts: Vec<&str> = path.split([':', ';']).collect();
    assert_eq!(parts[0], "/opt/tools");
    assert!(path.contains(".augment"));
    assert!(parts.contains(&"/usr/bin") && parts.contains(&"/bin"));
    // Relative provider paths contribute no parent dir.
    let rel = enhanced_path(Some(std::path::Path::new("auggie")));
    assert!(!rel.starts_with("auggie"));
}

#[test]
fn compound_model_id_round_trip() {
    assert_eq!(
        parse_compound_model_id("opencode:claude-sonnet-4"),
        ("opencode".to_string(), "claude-sonnet-4".to_string())
    );
    // Only the first ':' splits; the model may itself contain ':'.
    assert_eq!(
        parse_compound_model_id("codex:gpt-5.3-codex/high"),
        ("codex".to_string(), "gpt-5.3-codex/high".to_string())
    );
    // Bare id belongs to the default provider.
    assert_eq!(
        parse_compound_model_id("opus4.7"),
        ("auggie".to_string(), "opus4.7".to_string())
    );
    assert_eq!(
        create_compound_model_id("codex", "gpt-5.3-codex/high"),
        "codex:gpt-5.3-codex/high"
    );
}

#[test]
fn tier_table_and_resolution() {
    assert_eq!(tiers_for("auggie").unwrap().smart, "opus4.7");
    // Dynamic-model providers are intentionally absent.
    assert!(tiers_for("opencode").is_none() && tiers_for("droid").is_none());
    // Falls back to auggie's tier for providers without mappings.
    assert_eq!(
        default_model_for_provider("opencode", ModelTier::Fast),
        "haiku4.5"
    );
    assert_eq!(
        default_model_for_provider("codex", ModelTier::Smart),
        "gpt-5.3-codex/xhigh"
    );

    assert_eq!(
        model_tier_from_model("sonnet4.5", None),
        Some(ModelTier::Balanced)
    );
    assert_eq!(
        model_tier_from_model("opus4.7", Some("auggie")),
        Some(ModelTier::Smart)
    );
    assert_eq!(model_tier_from_model("nonexistent", None), None);

    assert!(is_model_valid_for_provider(
        "codex:gpt-5.3-codex/high",
        "codex"
    ));
    assert!(is_model_valid_for_provider("opus4.7", "auggie"));
    assert!(!is_model_valid_for_provider(
        "codex:gpt-5.3-codex/high",
        "auggie"
    ));
}

#[test]
fn fuzzy_and_override_resolution() {
    // Longest normalized-prefix wins: 'sonnet' -> 'sonnet4.5' (not 'haiku4.5').
    assert_eq!(
        normalize_model_override("sonnet", "auggie").as_deref(),
        Some("auggie:sonnet4.5")
    );
    // claude- brand prefix is stripped for normalized-exact matching.
    assert_eq!(
        normalize_model_override("claude-sonnet-4-5", "cortex").as_deref(),
        Some("cortex:claude-sonnet-4-5")
    );
    // Already-qualified candidates pass through unchanged.
    assert_eq!(
        normalize_model_override("codex:foo", "auggie").as_deref(),
        Some("codex:foo")
    );
    // Dynamic-model providers have no tier pool.
    assert_eq!(normalize_model_override("sonnet", "opencode"), None);

    let pool = ["sonnet4.5", "sonnet4.6", "haiku4.5"];
    assert_eq!(
        fuzzy_match_model_in_pool("sonnet", &pool).as_deref(),
        Some("sonnet4.6")
    );
    assert_eq!(
        fuzzy_match_model_in_pool("SONNET4.5", &pool).as_deref(),
        Some("sonnet4.5")
    );
    assert_eq!(fuzzy_match_model_in_pool("gpt", &pool), None);

    assert_eq!(
        resolve_preferred_model(&["opus4.7", "sonnet4.5"], &["sonnet4.5", "haiku4.5"]).as_deref(),
        Some("sonnet4.5")
    );
    assert_eq!(resolve_preferred_model(&["x"], &["y"]), None);
}

#[test]
fn codex_reasoning_effort_parsing() {
    assert_eq!(
        parse_codex_reasoning_effort("gpt-5.3-codex/high"),
        ("gpt-5.3-codex".to_string(), Some("high".to_string()))
    );
    assert_eq!(
        parse_codex_reasoning_effort("gpt-5.3-codex"),
        ("gpt-5.3-codex".to_string(), None)
    );
}

#[test]
fn codex_upsert_config_args_quotes_and_replaces() {
    // Fresh insert appends `-c key="value"`.
    let args = upsert_codex_config_args(&[], "model", "gpt-5.3-codex");
    assert_eq!(args, vec!["-c", "model=\"gpt-5.3-codex\""]);

    // Existing value for the same key is replaced (old `-c model=…` dropped).
    let prior = vec![
        "exec".to_string(),
        "-c".to_string(),
        "model=\"old\"".to_string(),
        "-c".to_string(),
        "sandbox=\"danger\"".to_string(),
    ];
    let next = upsert_codex_config_args(&prior, "model", "new");
    assert_eq!(
        next,
        vec!["exec", "-c", "sandbox=\"danger\"", "-c", "model=\"new\""]
    );

    // Embedded quotes are escaped.
    let escaped = upsert_codex_config_args(&[], "model", "a\"b");
    assert_eq!(escaped, vec!["-c", "model=\"a\\\"b\""]);
}

#[test]
fn codex_apply_config_args_effort_resolution() {
    // Effort embedded in the model id wins over the env fallback.
    let from_model = apply_codex_config_args(
        vec!["exec".to_string()],
        Some("gpt-5.3-codex/high"),
        Some("low"),
    );
    assert_eq!(
        from_model,
        vec![
            "exec",
            "-c",
            "model=\"gpt-5.3-codex\"",
            "-c",
            "model_reasoning_effort=\"high\""
        ]
    );

    // Bare model id falls back to env effort.
    let from_env = apply_codex_config_args(vec![], Some("gpt-5.3-codex"), Some("medium"));
    assert_eq!(
        from_env,
        vec![
            "-c",
            "model=\"gpt-5.3-codex\"",
            "-c",
            "model_reasoning_effort=\"medium\""
        ]
    );

    // The `default` sentinel and `None` are no-ops.
    assert_eq!(
        apply_codex_config_args(vec!["exec".to_string()], Some("default"), None),
        vec!["exec"]
    );
    assert_eq!(
        apply_codex_config_args(vec!["exec".to_string()], None, Some("high")),
        vec!["exec"]
    );
}

#[test]
fn auth_error_message_uses_login_hint() {
    // auggie has an explicit login_command_hint.
    let msg = auth_error_message("auggie", false);
    assert_eq!(
        msg,
        "Augment Auggie needs to be authenticated. Run \"auggie login\" in a terminal."
    );

    // Remote variant.
    let remote = auth_error_message("auggie", true);
    assert!(remote.contains("on the remote server"));
    assert!(remote.contains("auggie login"));

    // Providers without a hint fall back to `{command} login`.
    let codex = auth_error_message("codex", false);
    assert!(codex.contains("codex-acp login"));
}

#[test]
fn disableable_and_always_enabled_partition_registry() {
    let disableable: Vec<&str> = disableable_providers().iter().map(|p| p.id).collect();
    assert_eq!(
        disableable,
        vec![
            "claude-code",
            "codex",
            "cortex",
            "opencode",
            "droid",
            "mock"
        ]
    );
    assert!(disableable_providers().iter().all(|p| p.can_be_disabled));

    let always: Vec<&str> = always_enabled_providers().iter().map(|p| p.id).collect();
    assert_eq!(always, vec!["auggie"]);
    assert!(always_enabled_providers()
        .iter()
        .all(|p| !p.can_be_disabled));

    // Partition is total — every registered provider is in exactly one bucket.
    assert_eq!(disableable.len() + always.len(), all_provider_ids().len());
}

#[test]
fn auth_error_pattern_matching_is_case_insensitive_across_patterns() {
    // The first registered pattern ("authentication required") matches with
    // arbitrary input casing (the matcher lower-cases both sides).
    assert!(is_provider_authentication_error(
        "auggie",
        "AUTHENTICATION REQUIRED"
    ));
    // A later pattern in the list ("auggie login") also matches — proves the
    // matcher walks past the first entry rather than short-circuiting on it.
    assert!(is_provider_authentication_error(
        "auggie",
        "please run AUGGIE LOGIN now"
    ));
    // The third pattern, which contains backticks, is matched substring-wise.
    assert!(is_provider_authentication_error(
        "auggie",
        "hint: Please Run `Auggie Login`"
    ));
    // Unknown provider ids fall through to the default provider's patterns.
    assert!(is_provider_authentication_error(
        "nope",
        "Authentication required"
    ));
    // Providers without patterns never match, regardless of message content.
    assert!(!is_provider_authentication_error("cortex", "auggie login"));
}

#[test]
fn auth_error_message_remote_falls_back_to_command_login() {
    // claude-code has no login_command_hint → falls back to `{command} login`,
    // and the remote variant includes the remote-server phrasing.
    let msg = auth_error_message("claude-code", true);
    assert!(msg.contains("Anthropic Claude Code"));
    assert!(msg.contains("claude-agent-acp login"));
    assert!(msg.contains("on the remote server"));

    // Unknown provider ids resolve to the default provider's message.
    let unknown = auth_error_message("not-a-real-provider", false);
    assert_eq!(unknown, auth_error_message("auggie", false));
}

#[test]
fn registry_invariants() {
    // Exactly one provider is the default.
    assert_eq!(ACP_PROVIDERS.iter().filter(|p| p.is_default).count(), 1);
    // ids are unique.
    let ids = all_provider_ids();
    let mut sorted = ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), ids.len());
    // Every provider has a non-empty id / display_name / command.
    for p in ACP_PROVIDERS {
        assert!(!p.id.is_empty());
        assert!(!p.display_name.is_empty());
        assert!(!p.command.is_empty());
    }
}

#[test]
fn injection_mechanism_registry() {
    use InjectionMechanism::*;
    assert_eq!(
        find_provider("auggie").unwrap().injection_mechanism,
        RulesFileFlag
    );
    assert_eq!(
        find_provider("droid").unwrap().injection_mechanism,
        RulesFileFlag
    );
    assert_eq!(
        find_provider("claude-code").unwrap().injection_mechanism,
        SessionMeta
    );
    assert_eq!(
        find_provider("codex").unwrap().injection_mechanism,
        SessionMeta
    );
    assert_eq!(
        find_provider("opencode").unwrap().injection_mechanism,
        EnvConfig
    );
    assert_eq!(
        find_provider("cortex").unwrap().injection_mechanism,
        FirstTurnPrepend
    );
    assert_eq!(
        find_provider("mock").unwrap().injection_mechanism,
        FirstTurnPrepend
    );
}

#[test]
fn droid_arg_assembly_includes_append_system_prompt_file() {
    let droid = find_provider("droid").unwrap();
    let args = build_provider_args(
        droid,
        &ArgInputs {
            model: Some("gpt-5"),
            rules_file: Some("/tmp/rules.md"),
            ..Default::default()
        },
    );
    assert_eq!(
        args,
        vec![
            "exec",
            "--output-format",
            "acp",
            "--model",
            "gpt-5",
            "--append-system-prompt-file",
            "/tmp/rules.md",
        ]
    );
}

#[test]
fn opencode_env_includes_instructions_with_model() {
    let opencode = find_provider("opencode").unwrap();
    let env = build_provider_env(opencode, Some("claude-sonnet-4"), Some("/tmp/rules.md"));
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(
            r#"{"permission":{"task":"deny"},"model":"claude-sonnet-4","instructions":["/tmp/rules.md"]}"#
        )
    );
}

#[test]
fn opencode_env_includes_instructions_without_model() {
    let opencode = find_provider("opencode").unwrap();
    let env = build_provider_env(opencode, None, Some("/tmp/rules.md"));
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"instructions":["/tmp/rules.md"]}"#)
    );
}

#[test]
fn opencode_env_model_only_no_instructions() {
    let opencode = find_provider("opencode").unwrap();
    let env = build_provider_env(opencode, Some("claude-sonnet-4"), None);
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"model":"claude-sonnet-4"}"#)
    );
}

#[test]
fn opencode_env_escapes_json_in_instructions_path() {
    let opencode = find_provider("opencode").unwrap();
    let env = build_provider_env(opencode, None, Some(r#"/tmp/"rules".md"#));
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"instructions":["/tmp/\"rules\".md"]}"#)
    );
}

#[test]
fn auggie_mechanism_unchanged() {
    // auggie still uses RulesFileFlag, not affected by opencode/droid changes
    let auggie = find_provider("auggie").unwrap();
    assert_eq!(
        auggie.injection_mechanism,
        InjectionMechanism::RulesFileFlag
    );
    assert_eq!(auggie.rules_flag, Some("--rules"));

    // Env assembly doesn't add OPENCODE_CONFIG_CONTENT for auggie
    let env = build_provider_env(auggie, Some("sonnet4.5"), Some("/tmp/rules.md"));
    assert!(!env.contains_key("OPENCODE_CONFIG_CONTENT"));
}
