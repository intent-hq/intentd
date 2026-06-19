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
fn env_assembly_quirks() {
    assert!(build_provider_env("auggie", Some("sonnet4.5")).is_empty());

    let cortex = build_provider_env("cortex", None);
    assert_eq!(
        cortex.get("ELECTRON_RUN_AS_NODE").map(String::as_str),
        Some("1")
    );

    let oc = build_provider_env("opencode", Some("claude-sonnet-4"));
    assert_eq!(
        oc.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"model":"claude-sonnet-4"}"#)
    );
    // No model → no opencode env.
    assert!(build_provider_env("opencode", None).is_empty());
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
