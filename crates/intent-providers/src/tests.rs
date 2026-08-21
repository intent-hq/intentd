//! Unit tests for the provider registry, arg/env assembly, PATH enrichment,
//! and model resolution — parity-checked against `provider-config.ts`.

use super::*;

#[test]
fn registry_first_provider_and_lookups() {
    // The first registered provider is a neutral positional last resort —
    // no provider carries a privileged default designation.
    assert_eq!(first_provider_id(), ACP_PROVIDERS[0].id);
    assert_eq!(first_provider_config().id, first_provider_id());
    assert_eq!(
        all_provider_ids(),
        vec![
            "auggie",
            "claude-code",
            "codex",
            "cortex",
            "opencode",
            "unsloth",
            "pi",
            "droid",
            "grok",
            "mock"
        ]
    );
    assert!(find_provider("nope").is_none());
    // Unknown ids fall back to the first registered provider.
    assert_eq!(provider_config("nope").id, first_provider_id());
    assert_eq!(find_provider("codex").unwrap().command, "codex-acp");
}

/// Unknown provider ids still resolve to the first registered provider
/// (behavior unchanged), and the warn gate fires only for genuinely unknown
/// ids — not for empty ids or legacy default aliases.
#[test]
fn unknown_provider_fallback_warn_gate() {
    // Fallback behavior is preserved for every suppressed alias and for
    // genuinely unknown ids.
    for id in ["", "default", "acp", "augment", "nope"] {
        assert_eq!(provider_config(id).id, first_provider_id());
    }
    // Genuinely unknown ids warn.
    assert!(config::warns_on_unknown_provider("nope"));
    assert!(config::warns_on_unknown_provider("pi-typo"));
    // Empty ids and legacy default aliases stay silent.
    assert!(!config::warns_on_unknown_provider(""));
    assert!(!config::warns_on_unknown_provider("default"));
    assert!(!config::warns_on_unknown_provider("acp"));
    assert!(!config::warns_on_unknown_provider("augment"));
}

#[test]
fn claude_agent_acp_pin_is_single_sourced() {
    assert!(!CLAUDE_AGENT_ACP_VERSION.is_empty());
    assert_eq!(
        CLAUDE_AGENT_ACP_NPX_PACKAGE,
        format!("@agentclientprotocol/claude-agent-acp@{CLAUDE_AGENT_ACP_VERSION}")
    );
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
    assert!(auggie.can_be_disabled);
    assert_eq!(auggie.login_command_hint, Some("auggie login"));
    // `auggie token print` exits 0 iff logged in; its output is the auth
    // session secret, so the probe must ride the generic exit-code arm.
    assert_eq!(auggie.auth_check_args, Some(&["token", "print"][..]));
    // Installed-but-logged-out auggie needs a login affordance on the generic
    // provider row, which reads the catalog's `login_docs_url`.
    assert!(auggie.login_docs_url.is_some());

    let cc = find_provider("claude-code").unwrap();
    assert_eq!(cc.command, "claude-agent-acp");
    assert_eq!(cc.base_args, &[] as &[&str]);
    assert_eq!(cc.auth_check_args, Some(&["auth", "status"][..]));
    assert!(cc.model_flag.is_none() && cc.can_be_disabled);
    assert_eq!(cc.npx_only_package, Some(CLAUDE_AGENT_ACP_NPX_PACKAGE));
    assert_eq!(cc.fallback_npx_package, None);

    let codex = find_provider("codex").unwrap();
    assert_eq!(codex.auth_check_args, Some(&["login", "status"][..]));
    assert_eq!(codex.npx_only_package, None);

    let cortex = find_provider("cortex").unwrap();
    assert_eq!(cortex.command, "cortex-acp");
    // Un-gated (monorepo#1902): cortex carries no feature code.
    assert_eq!(cortex.requires_feature_code, None);

    let oc = find_provider("opencode").unwrap();
    assert_eq!(oc.base_args, &["acp"]);
    assert_eq!(oc.auth_check_args, Some(&["models"][..]));
    assert!(oc.model_flag.is_none());

    let pi = find_provider("pi").unwrap();
    assert_eq!(pi.display_name, "Pi");
    assert_eq!(pi.command, "pi-acp");
    assert_eq!(pi.base_args, &[] as &[&str]);
    assert!(pi.model_flag.is_none() && pi.can_be_disabled);
    assert_eq!(pi.npx_only_package, Some(PI_ACP_NPX_PACKAGE));
    assert_eq!(pi.fallback_npx_package, None);
    assert_eq!(
        pi.login_docs_url,
        Some("https://pi.dev/docs/latest/quickstart")
    );
    // MCP rides the bundled pi extension (wrapper + PI_ACP_PI_COMMAND) — pi
    // has no MCP CLI flag and ignores the ACP session field.
    assert!(pi.mcp_via_pi_extension);
    assert!(!pi.supports_mcp_config && !pi.supports_session_mcp_servers);
    for p in ACP_PROVIDERS.iter().filter(|p| p.id != "pi") {
        assert!(
            !p.mcp_via_pi_extension,
            "{} must not use the pi-extension MCP delivery",
            p.id
        );
    }

    let droid = find_provider("droid").unwrap();
    assert_eq!(droid.base_args, &["exec", "--output-format", "acp"]);
    assert_eq!(droid.model_flag, Some("--model"));
    assert!(droid.supports_rules_file);
    assert_eq!(droid.rules_flag, Some("--append-system-prompt-file"));

    let grok = find_provider("grok").unwrap();
    assert_eq!(grok.display_name, "Grok Build");
    assert_eq!(grok.command, "grok");
    assert_eq!(grok.base_args, &["agent", "stdio"]);
    assert!(grok.terminal_requires_shell);
    // Grok selects models after session creation via session/set_model — no
    // CLI model flag.
    assert!(grok.model_flag.is_none() && grok.supports_set_model);
    assert!(!grok.supports_authenticate && !grok.supports_set_mode);
    // No CLI MCP flag — the workspace bridge rides the ACP `session/new`
    // `mcpServers` field instead.
    assert!(!grok.supports_mcp_config && grok.supports_session_mcp_servers);
    assert!(!grok.supports_rules_file);
    assert!(grok.can_be_disabled);
    assert_eq!(grok.login_command_hint, Some("grok login"));
    assert_eq!(grok.auth_check_args, Some(&["models"][..]));
    assert_eq!(
        grok.login_docs_url,
        Some("https://docs.x.ai/build/enterprise#authentication")
    );
    assert_eq!(grok.npx_only_package, None);
    assert_eq!(grok.fallback_npx_package, None);

    let mock = find_provider("mock").unwrap();
    assert_eq!(mock.command, "node");
    assert_eq!(mock.requires_env_var, Some("MOCK_AGENT_SCRIPT_PATH"));

    // unsloth rides the opencode binary as its ACP runtime: same command,
    // base args, runtime, and env-config injection as opencode — but its own
    // id (the custom `provider.unsloth` block keys off it) and no CLI auth
    // probe (the injected config carries its own apiKey).
    let unsloth = find_provider("unsloth").unwrap();
    assert_eq!(unsloth.display_name, "Unsloth");
    assert_eq!(unsloth.command, "opencode");
    assert_eq!(unsloth.base_args, &["acp"]);
    assert_eq!(unsloth.injection_mechanism, InjectionMechanism::EnvConfig);
    assert!(unsloth.model_flag.is_none());
    assert!(!unsloth.supports_mcp_config && !unsloth.supports_session_mcp_servers);
    assert!(unsloth.can_be_disabled);
    assert_eq!(unsloth.auth_check_args, None);
    assert_eq!(unsloth.npx_only_package, None);
    assert_eq!(unsloth.fallback_npx_package, None);
}

/// Exactly claude-code, codex, droid, and grok consume MCP servers from the
/// ACP `session/new` / `session/load` `mcpServers` field; every other
/// provider receives MCP config out-of-band (auggie `--mcp-config`, opencode
/// env config) or not at all. Asserted over the full registry so a newly
/// added provider can't accidentally opt in without updating this partition.
#[test]
fn session_mcp_servers_partition() {
    let opted_in = ["claude-code", "codex", "droid", "grok"];
    for id in all_provider_ids() {
        let p = find_provider(id).unwrap();
        assert_eq!(
            p.supports_session_mcp_servers,
            opted_in.contains(&id),
            "{id}: supports_session_mcp_servers must match the pinned opt-in set {opted_in:?}"
        );
    }
}

/// Exactly claude-code and pi apply the stored model post-session via
/// `session/set_config_option { configId: "model" }` (their pinned adapters
/// expose the model as a `configOptions[id="model"]` select and have no CLI
/// model flag). Asserted over the full registry so a newly added provider
/// can't accidentally opt in without updating this partition.
#[test]
fn config_option_model_partition() {
    let opted_in = ["claude-code", "pi"];
    for id in all_provider_ids() {
        let p = find_provider(id).unwrap();
        assert_eq!(
            p.supports_config_option_model,
            opted_in.contains(&id),
            "{id}: supports_config_option_model must match the pinned opt-in set {opted_in:?}"
        );
        // The two post-session model paths are mutually exclusive for EVERY
        // provider: `maybe_apply_session_model` would issue both calls for a
        // provider carrying both flags.
        assert!(
            !(p.supports_set_model && p.supports_config_option_model),
            "{id}: supports_set_model and supports_config_option_model are mutually exclusive"
        );
    }
    // claude-code and pi additionally have no CLI model flag and no
    // set_model path.
    let cc = find_provider("claude-code").unwrap();
    assert!(cc.model_flag.is_none() && !cc.supports_set_model);
    let pi = find_provider("pi").unwrap();
    assert!(pi.model_flag.is_none() && !pi.supports_set_model);
}

/// Regression (pi harness selection): `provider_config("pi")` must resolve to
/// the pi entry — before pi was registered it silently fell back to the
/// default provider (auggie), which then rejected pi model ids.
#[test]
fn pi_resolves_in_registry_with_pinned_npx_package() {
    assert_eq!(provider_config("pi").id, "pi");
    let pi = find_provider("pi").expect("pi is registered");
    assert_eq!(pi.command, "pi-acp");
    assert_eq!(PI_ACP_NPX_PACKAGE, "pi-acp@0.0.33");
    assert_eq!(pi.npx_only_package, Some(PI_ACP_NPX_PACKAGE));
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
    // never pass an unknown flag to claude/codex/cortex/opencode/droid/grok.
    for id in [
        "claude-code",
        "codex",
        "cortex",
        "opencode",
        "droid",
        "grok",
    ] {
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
    assert_eq!(runtime("unsloth"), ProviderRuntime::Node);
    assert_eq!(runtime("mock"), ProviderRuntime::Node);
    assert_eq!(runtime("cortex"), ProviderRuntime::Electron);
    assert_eq!(runtime("codex"), ProviderRuntime::Native);
    assert_eq!(runtime("droid"), ProviderRuntime::Native);
    assert_eq!(runtime("grok"), ProviderRuntime::Native);
}

#[test]
fn env_assembly_quirks() {
    let cortex = build_provider_env(find_provider("cortex").unwrap(), None, None, None);
    assert_eq!(
        cortex.get("ELECTRON_RUN_AS_NODE").map(String::as_str),
        Some("1")
    );

    let opencode = find_provider("opencode").unwrap();
    // With model: permission.task=deny is merged with the model key.
    let oc_with_model = build_provider_env(opencode, Some("claude-sonnet-4"), None, None);
    assert_eq!(
        oc_with_model
            .get("OPENCODE_CONFIG_CONTENT")
            .map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"model":"claude-sonnet-4"}"#)
    );
    // No model, no rules file: permission.task=deny is still emitted.
    let oc_no_model_no_rules = build_provider_env(opencode, None, None, None);
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
    let sentinel_only = build_provider_env(opencode, Some("default"), None, None);
    assert_eq!(
        sentinel_only
            .get("OPENCODE_CONFIG_CONTENT")
            .map(String::as_str),
        Some(r#"{"permission":{"task":"deny"}}"#)
    );

    // Rules file alone (no model) → permission + instructions.
    let rules_only = build_provider_env(opencode, None, Some("/tmp/rules.md"), None);
    assert_eq!(
        rules_only
            .get("OPENCODE_CONFIG_CONTENT")
            .map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"instructions":["/tmp/rules.md"]}"#)
    );

    // Real model + rules file → all three fields.
    let both = build_provider_env(opencode, Some("gpt-4"), Some("/tmp/rules.md"), None);
    assert_eq!(
        both.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"model":"gpt-4","instructions":["/tmp/rules.md"]}"#)
    );

    // Sentinel model + rules file → permission + instructions (model filtered).
    let sentinel_with_rules =
        build_provider_env(opencode, Some("default"), Some("/tmp/rules.md"), None);
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
    let json = format!(r#"{{"path":"{escaped}"}}"#);
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

/// STAB-50: `NODE_OPTIONS` heap-cap injection for V8-runtime (Node/Electron)
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

    let env_for = |id: &str| build_provider_env(find_provider(id).unwrap(), None, None, None);

    // Env-driven scenarios (serialized within this single test).
    std::env::remove_var("NODE_OPTIONS");
    std::env::remove_var("INTENTD_ACP_NODE_MAX_OLD_SPACE_MB");

    // Default cap is 8192 for every Node/Electron provider.
    assert_eq!(args::max_old_space_mb(), 8192);
    for id in [
        "auggie",
        "claude-code",
        "opencode",
        "unsloth",
        "cortex",
        "mock",
    ] {
        assert_eq!(
            env_for(id).get("NODE_OPTIONS").map(String::as_str),
            Some("--max-old-space-size=8192"),
            "provider {id} should get the heap cap"
        );
    }
    // Native runtimes get no NODE_OPTIONS.
    for id in ["codex", "droid", "grok"] {
        assert!(
            !env_for(id).contains_key("NODE_OPTIONS"),
            "native provider {id} must not get NODE_OPTIONS"
        );
    }

    // Spawn-time npx signal: an npx spawn always runs a Node child, so even
    // a declared-Native provider (codex's npx fallback) gets the cap; the
    // same provider without the signal (resolved native binary) stays
    // untouched (intent-hq/monorepo#1661).
    let codex = find_provider("codex").unwrap();
    let codex_via_npx = args::build_provider_env_for_spawn(codex, None, None, None, None, true);
    assert_eq!(
        codex_via_npx.get("NODE_OPTIONS").map(String::as_str),
        Some("--max-old-space-size=8192"),
        "codex npx-fallback spawn must get the heap cap"
    );
    let codex_native = args::build_provider_env_for_spawn(codex, None, None, None, None, false);
    assert!(
        !codex_native.contains_key("NODE_OPTIONS"),
        "codex resolved-binary spawn must not get NODE_OPTIONS"
    );

    // Override seam produces the requested cap for all V8 providers.
    std::env::set_var("INTENTD_ACP_NODE_MAX_OLD_SPACE_MB", "4096");
    assert_eq!(args::max_old_space_mb(), 4096);
    for id in [
        "auggie",
        "claude-code",
        "opencode",
        "unsloth",
        "cortex",
        "mock",
    ] {
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
    for id in [
        "auggie",
        "claude-code",
        "opencode",
        "unsloth",
        "cortex",
        "mock",
    ] {
        assert_eq!(
            env_for(id).get("NODE_OPTIONS").map(String::as_str),
            Some("--enable-source-maps --max-old-space-size=8192"),
            "provider {id} should append to inherited NODE_OPTIONS"
        );
    }

    // Parent already caps old-space → left alone (no double flag).
    std::env::set_var("NODE_OPTIONS", "--max-old-space-size=2048");
    for id in [
        "auggie",
        "claude-code",
        "opencode",
        "unsloth",
        "cortex",
        "mock",
    ] {
        assert!(
            !env_for(id).contains_key("NODE_OPTIONS"),
            "provider {id} must respect a user-set --max-old-space-size"
        );
    }
}

#[test]
fn enhanced_path_prepends_bin_and_augment() {
    // Injected home / inherited PATH (never mutates process-global env — the
    // env vars are shared with parallel PATH-dependent tests, monorepo#628).
    let home = std::path::Path::new("/home/tester");
    let inherited = std::ffi::OsStr::new("/usr/bin:/bin");
    let bin = std::path::PathBuf::from("/opt/tools/auggie");
    let path = args::enhanced_path_with(Some(&bin), Some(home), Some(inherited));
    let parts: Vec<&str> = path.split([':', ';']).collect();
    assert_eq!(parts[0], "/opt/tools");
    assert!(path.contains(".augment"));
    assert!(parts.contains(&"/usr/bin") && parts.contains(&"/bin"));
    // Relative provider paths contribute no parent dir.
    let rel = args::enhanced_path_with(
        Some(std::path::Path::new("auggie")),
        Some(home),
        Some(inherited),
    );
    assert!(!rel.starts_with("auggie"));
}

#[test]
fn enhanced_path_dirs_mirror_the_joined_spawn_path() {
    // `enhanced_path_dirs_with` is the directory-list view of the exact PATH
    // the spawned child gets (`enhanced_path_with`): same entries, same
    // precedence order. `find_pi_cli` scans this list so its probe resolves
    // the same binary the pi-acp child would.
    let home = std::path::Path::new("/home/tester");
    let inherited = std::ffi::OsStr::new("/usr/bin:/bin");
    let bin = std::path::PathBuf::from("/opt/node/npx");
    let joined = args::enhanced_path_with(Some(&bin), Some(home), Some(inherited));
    let dirs = args::enhanced_path_dirs_with(Some(&bin), Some(home), Some(inherited));
    let joined_parts: Vec<String> = joined.split([':', ';']).map(str::to_string).collect();
    let dir_parts: Vec<String> = dirs
        .iter()
        .map(|d| d.to_string_lossy().into_owned())
        .collect();
    assert_eq!(dir_parts, joined_parts, "dirs and joined PATH must agree");
    // Spawn precedence: npx parent dir first, so a `pi` co-located with npx
    // shadows one later on the inherited PATH — for probe and child alike.
    assert_eq!(dirs[0], std::path::PathBuf::from("/opt/node"));
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
fn model_validity_follows_compound_prefix() {
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
            "auggie",
            "claude-code",
            "codex",
            "cortex",
            "opencode",
            "unsloth",
            "pi",
            "droid",
            "grok",
            "mock"
        ]
    );
    assert!(disableable_providers().iter().all(|p| p.can_be_disabled));

    // Every provider — auggie included — is now disableable.
    let always: Vec<&str> = always_enabled_providers().iter().map(|p| p.id).collect();
    assert!(always.is_empty());
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

    // Unknown provider ids resolve to the first registered provider's message.
    let unknown = auth_error_message("not-a-real-provider", false);
    assert_eq!(unknown, auth_error_message(first_provider_id(), false));
}

#[test]
fn registry_invariants() {
    // ids are unique.
    let ids = all_provider_ids();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
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
    // codex uses FirstTurnPrepend: the pinned codex-acp adapter (1.1.14)
    // ignores `_meta.developerInstructions` (#479).
    assert_eq!(
        find_provider("codex").unwrap().injection_mechanism,
        FirstTurnPrepend
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
        find_provider("grok").unwrap().injection_mechanism,
        FirstTurnPrepend
    );
    assert_eq!(
        find_provider("pi").unwrap().injection_mechanism,
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
    let env = build_provider_env(
        opencode,
        Some("claude-sonnet-4"),
        Some("/tmp/rules.md"),
        None,
    );
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
    let env = build_provider_env(opencode, None, Some("/tmp/rules.md"), None);
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"instructions":["/tmp/rules.md"]}"#)
    );
}

#[test]
fn opencode_env_model_only_no_instructions() {
    let opencode = find_provider("opencode").unwrap();
    let env = build_provider_env(opencode, Some("claude-sonnet-4"), None, None);
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"model":"claude-sonnet-4"}"#)
    );
}

#[test]
fn opencode_env_escapes_json_in_instructions_path() {
    let opencode = find_provider("opencode").unwrap();
    let env = build_provider_env(opencode, None, Some(r#"/tmp/"rules".md"#), None);
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"instructions":["/tmp/\"rules\".md"]}"#)
    );
}

/// The endpoint fixture matching the config a real Unsloth install generates
/// via `unsloth start opencode --no-launch` (`http://127.0.0.1:<port>/v1` +
/// Bearer apiKey + one served model keyed by full repo id, with discovered
/// context limits and a compaction reserve).
fn unsloth_endpoint() -> UnslothEndpoint {
    UnslothEndpoint {
        base_url: "http://127.0.0.1:8752/v1".to_string(),
        api_key: "sk-unsloth-key".to_string(),
        model_id: "unsloth/stub-model-GGUF".to_string(),
        model_display_name: Some("Stub Model".to_string()),
        limit: Some(UnslothModelLimit {
            context: 262_144,
            output: 8192,
        }),
        compaction_reserved: Some(8192),
    }
}

#[test]
fn unsloth_env_injects_provider_block_matching_unsloth_generated_shape() {
    // With an endpoint and no session model: the provider.unsloth-studio
    // block (id, models-map keying, limit fields), model + small_model
    // defaults, and the compaction block — permission.task=deny first. This
    // is the exact shape Unsloth itself writes into opencode.json via
    // `unsloth start opencode --no-launch` (validated 2026-07-27).
    let unsloth = find_provider("unsloth").unwrap();
    let ep = unsloth_endpoint();
    let env = build_provider_env_with_unsloth(unsloth, None, None, None, Some(&ep));
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(
            r#"{"permission":{"task":"deny"},"provider":{"unsloth-studio":{"npm":"@ai-sdk/openai-compatible","name":"Unsloth (local)","options":{"baseURL":"http://127.0.0.1:8752/v1","apiKey":"sk-unsloth-key"},"models":{"unsloth/stub-model-GGUF":{"name":"Stub Model","limit":{"context":262144,"output":8192}}}}},"model":"unsloth-studio/unsloth/stub-model-GGUF","small_model":"unsloth-studio/unsloth/stub-model-GGUF","compaction":{"auto":true,"reserved":8192}}"#
        )
    );
}

#[test]
fn unsloth_env_omits_limit_and_compaction_when_undiscovered() {
    // Lifecycle didn't discover limits: no `limit` in the models map and no
    // top-level `compaction` block.
    let unsloth = find_provider("unsloth").unwrap();
    let mut ep = unsloth_endpoint();
    ep.limit = None;
    ep.compaction_reserved = None;
    let env = build_provider_env_with_unsloth(unsloth, None, None, None, Some(&ep));
    let config = env.get("OPENCODE_CONFIG_CONTENT").unwrap();
    assert!(!config.contains("\"limit\""), "no limit block: {config}");
    assert!(
        !config.contains("\"compaction\""),
        "no compaction block: {config}"
    );
}

#[test]
fn unsloth_env_session_model_overrides_endpoint_model() {
    // A session model (not the sentinel) wins over the endpoint's served
    // model and is registered in the provider's `models` map so the
    // top-level `model` keeps matching a key (spike constraint).
    let unsloth = find_provider("unsloth").unwrap();
    let ep = unsloth_endpoint();
    let env = build_provider_env_with_unsloth(unsloth, Some("other-model"), None, None, Some(&ep));
    let config = env.get("OPENCODE_CONFIG_CONTENT").unwrap();
    assert!(
        config.contains(r#""model":"unsloth-studio/other-model""#),
        "session model must win: {config}"
    );
    assert!(
        config.contains(r#""small_model":"unsloth-studio/other-model""#),
        "small_model must mirror model: {config}"
    );
    assert!(
        config.contains(r#""other-model":{"name":"other-model"}"#),
        "session model must be registered in the models map: {config}"
    );
    assert!(
        config.contains(
            r#""unsloth/stub-model-GGUF":{"name":"Stub Model","limit":{"context":262144,"output":8192}}"#
        ),
        "served model stays in the models map with its limits: {config}"
    );

    // The "default" sentinel falls back to the endpoint's served model.
    let env = build_provider_env_with_unsloth(unsloth, Some("default"), None, None, Some(&ep));
    let config = env.get("OPENCODE_CONFIG_CONTENT").unwrap();
    assert!(
        config.contains(r#""model":"unsloth-studio/unsloth/stub-model-GGUF""#),
        "sentinel model must fall back to the endpoint model: {config}"
    );
}

#[test]
fn unsloth_env_without_endpoint_is_permission_only() {
    // No endpoint (managed-server lifecycle not yet run): permission-only
    // config, no provider block, no model key.
    let unsloth = find_provider("unsloth").unwrap();
    let env = build_provider_env(unsloth, None, None, None);
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"permission":{"task":"deny"}}"#)
    );
    // Even with a session model — without the provider block there is no
    // `unsloth-studio/<model>` key to reference, so the model is dropped.
    let env = build_provider_env(unsloth, Some("stub-model-1"), None, None);
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"permission":{"task":"deny"}}"#)
    );
}

#[test]
fn unsloth_env_merges_instructions_and_mcp_after_provider_block() {
    // Rules file + MCP block ride alongside the provider block, in the same
    // positions opencode's own assembly puts them (instructions then mcp).
    let unsloth = find_provider("unsloth").unwrap();
    let ep = unsloth_endpoint();
    let mcp = r#"{"workspace-mcp":{"type":"local","command":["intentd","mcp-bridge","--connect","127.0.0.1:9999"],"enabled":true,"environment":{}}}"#;
    let env =
        build_provider_env_with_unsloth(unsloth, None, Some("/tmp/rules.md"), Some(mcp), Some(&ep));
    let config = env.get("OPENCODE_CONFIG_CONTENT").unwrap();
    assert!(
        config.starts_with(r#"{"permission":{"task":"deny"},"provider":{"unsloth-studio":"#),
        "permission then provider block first: {config}"
    );
    assert!(
        config.contains(r#""instructions":["/tmp/rules.md"]"#),
        "instructions must survive: {config}"
    );
    assert!(
        config.ends_with(&format!(r#""mcp":{mcp}}}"#)),
        "mcp block spliced last: {config}"
    );
}

#[test]
fn unsloth_env_escapes_endpoint_values() {
    // Endpoint values are user/lifecycle-supplied strings — quotes and
    // backslashes must be JSON-escaped, and the emitted value must parse.
    let unsloth = find_provider("unsloth").unwrap();
    let ep = UnslothEndpoint {
        base_url: r#"http://127.0.0.1:8752/v1"x"#.to_string(),
        api_key: r"key\with\slashes".to_string(),
        model_id: "m1".to_string(),
        model_display_name: None,
        limit: None,
        compaction_reserved: None,
    };
    let env = build_provider_env_with_unsloth(unsloth, None, None, None, Some(&ep));
    let config = env.get("OPENCODE_CONFIG_CONTENT").unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(config).expect("emitted config must be valid JSON");
    assert_eq!(
        parsed["provider"]["unsloth-studio"]["options"]["apiKey"],
        ep.api_key
    );
    assert!(
        config.contains(r#""baseURL":"http://127.0.0.1:8752/v1\"x""#),
        "baseURL must be escaped: {config}"
    );
    assert!(
        config.contains(r#""apiKey":"key\\with\\slashes""#),
        "apiKey must be escaped: {config}"
    );
    // Display name falls back to the model id.
    assert!(
        config.contains(r#""m1":{"name":"m1"}"#),
        "display name falls back to model id: {config}"
    );
}

#[test]
fn non_unsloth_providers_ignore_unsloth_endpoint() {
    // The endpoint is unsloth-only: opencode keeps its plain shape and
    // non-env-config providers emit nothing.
    let ep = unsloth_endpoint();
    let opencode = find_provider("opencode").unwrap();
    let env = build_provider_env_with_unsloth(opencode, Some("gpt-4"), None, None, Some(&ep));
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(r#"{"permission":{"task":"deny"},"model":"gpt-4"}"#)
    );
    for id in ["auggie", "claude-code", "codex", "droid", "grok"] {
        let env = build_provider_env_with_unsloth(
            find_provider(id).unwrap(),
            None,
            None,
            None,
            Some(&ep),
        );
        assert!(
            !env.contains_key("OPENCODE_CONFIG_CONTENT"),
            "{id} unexpectedly emitted OPENCODE_CONFIG_CONTENT"
        );
    }
}

#[test]
fn grok_arg_assembly_has_no_model_or_rules_flags() {
    // Grok's `agent stdio` subcommand has no model/rules/mcp flags: the model
    // is applied post-session via session/set_model, so the arg assembly must
    // emit only the base args.
    let grok = find_provider("grok").unwrap();
    assert_eq!(
        build_provider_args(
            grok,
            &ArgInputs {
                model: Some("grok-4.5"),
                rules_file: Some("/tmp/rules.md"),
                mcp_config_file: Some("/tmp/mcp.json"),
                quiet: true,
                ..Default::default()
            }
        ),
        vec!["agent", "stdio"]
    );
}

#[test]
fn grok_is_the_only_set_model_provider() {
    for p in ACP_PROVIDERS {
        assert_eq!(
            p.supports_set_model,
            p.id == "grok",
            "{} supports_set_model mismatch",
            p.id
        );
    }
}

#[test]
fn grok_parses_initialize_models_after_update_banner_preamble() {
    let fixture = format!(
        "Update available 0.1.0 → 0.2.0\n{}",
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "protocolVersion": 1,
                "modelState": {
                    "currentModelId": "grok-build",
                    "availableModels": [
                        { "modelId": "grok-build", "name": "Grok Build", "description": "Default Grok build model" },
                        { "modelId": "gpt-5-5", "name": "GPT-5.5", "agentType": "reasoning", "contextWindow": 1_048_576 }
                    ]
                }
            }
        })
    );

    let parsed = parse_grok_initialize_response_from_stdout(&fixture, 1).unwrap();
    assert_eq!(parsed.current_model_id.as_deref(), Some("grok-build"));
    assert_eq!(
        parsed.models,
        vec![
            GrokModel {
                model_id: "grok-build".into(),
                name: "Grok Build".into(),
                description: Some("Default Grok build model".into()),
            },
            GrokModel {
                model_id: "gpt-5-5".into(),
                name: "GPT-5.5".into(),
                description: Some("reasoning · 1,048,576 token context".into()),
            },
        ]
    );
}

#[test]
fn grok_models_command_parses_readiness_without_trusting_exit_code() {
    let parsed = parse_grok_models_command_output(
        "Update available 0.1.0 → 0.2.0\nYou are logged in with grok.com.\ngrok-build  Grok Build  Default model\nopus-4-8  Opus 4.8",
    );
    assert_eq!(parsed.authenticated, Some(true));
    assert_eq!(
        parsed.models,
        vec![
            GrokModel {
                model_id: "grok-build".into(),
                name: "Grok Build".into(),
                description: Some("Default model".into()),
            },
            GrokModel {
                model_id: "opus-4-8".into(),
                name: "Opus 4.8".into(),
                description: None,
            },
        ]
    );
}

#[test]
fn grok_models_command_parses_default_marker_without_using_it_as_label() {
    let parsed = parse_grok_models_command_output(
        "You are logged in with grok.com.\n\nDefault model: grok-4.5\n\nAvailable models:\n  * grok-4.5 (default)\n  - grok-composer-2.5-fast\n  - opus-4-6",
    );
    assert_eq!(parsed.authenticated, Some(true));
    assert_eq!(parsed.current_model_id.as_deref(), Some("grok-4.5"));
    assert_eq!(
        parsed.models,
        vec![
            GrokModel {
                model_id: "grok-4.5".into(),
                name: "Grok 4.5".into(),
                description: None,
            },
            GrokModel {
                model_id: "grok-composer-2.5-fast".into(),
                name: "Grok Composer 2.5 Fast".into(),
                description: None,
            },
            GrokModel {
                model_id: "opus-4-6".into(),
                name: "Opus 4 6".into(),
                description: None,
            },
        ]
    );
    assert!(parsed.models.iter().all(|m| m.name != "(default)"));
}

#[test]
fn grok_models_command_strips_trailing_current_marker() {
    let parsed = parse_grok_models_command_output(
        "grok-current  Grok Current  Current default alias (current)",
    );
    assert_eq!(parsed.current_model_id.as_deref(), Some("grok-current"));
    assert_eq!(
        parsed.models,
        vec![GrokModel {
            model_id: "grok-current".into(),
            name: "Grok Current".into(),
            description: Some("Current default alias".into()),
        }]
    );
    assert!(parsed.models.iter().all(|m| m.name != "(current)"));
}

#[test]
fn grok_models_command_detects_logged_out_state() {
    let parsed = parse_grok_models_command_output(
        "You are not authenticated. Please log in with `grok login`.",
    );
    assert_eq!(parsed.authenticated, Some(false));
    assert!(parsed.models.is_empty());
    assert_eq!(parsed.current_model_id, None);
}

#[test]
fn grok_initialize_models_null_model_state_falls_through() {
    // An explicit `"modelState": null` must fall through to top-level model
    // containers (TS nullish-coalescing parity).
    let result = serde_json::json!({
        "modelState": null,
        "currentModelId": "grok-4.5",
        "availableModels": [ { "modelId": "grok-4.5" } ]
    });
    let parsed = parse_grok_initialize_models(&result);
    assert_eq!(parsed.current_model_id.as_deref(), Some("grok-4.5"));
    assert_eq!(parsed.models.len(), 1);
    assert_eq!(parsed.models[0].model_id, "grok-4.5");
}

#[test]
fn grok_initialize_stdout_skips_errors_and_unrelated_ids() {
    // Error responses and other ids are skipped; a missing match yields None.
    let stdout = format!(
        "{}\n{}",
        serde_json::json!({ "jsonrpc": "2.0", "id": 1, "error": { "code": 401, "message": "auth required" } }),
        serde_json::json!({ "jsonrpc": "2.0", "id": 2, "result": { "models": [] } }),
    );
    assert_eq!(parse_grok_initialize_response_from_stdout(&stdout, 1), None);
    // The id-2 success response parses (empty model list).
    let parsed = parse_grok_initialize_response_from_stdout(&stdout, 2).unwrap();
    assert!(parsed.models.is_empty() && parsed.current_model_id.is_none());
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
    let env = build_provider_env(auggie, Some("sonnet4.5"), Some("/tmp/rules.md"), None);
    assert!(!env.contains_key("OPENCODE_CONFIG_CONTENT"));
}

#[test]
fn opencode_env_merges_mcp_block_preserving_other_keys() {
    let opencode = find_provider("opencode").unwrap();
    let mcp = r#"{"workspace-mcp":{"type":"local","command":["intentd","mcp-bridge","--connect","127.0.0.1:9999"],"enabled":true,"environment":{}}}"#;
    let env = build_provider_env(
        opencode,
        Some("claude-sonnet-4"),
        Some("/tmp/rules.md"),
        Some(mcp),
    );
    // permission, model, and instructions all survive; mcp is spliced last.
    let expected = format!(
        r#"{{"permission":{{"task":"deny"}},"model":"claude-sonnet-4","instructions":["/tmp/rules.md"],"mcp":{mcp}}}"#
    );
    assert_eq!(
        env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
        Some(expected.as_str())
    );

    // Empty or whitespace-only mcp json is ignored rather than emitting
    // invalid JSON.
    for blank in ["", " ", "\n\t "] {
        let env = build_provider_env(opencode, None, None, Some(blank));
        assert_eq!(
            env.get("OPENCODE_CONFIG_CONTENT").map(String::as_str),
            Some(r#"{"permission":{"task":"deny"}}"#),
            "mcp json {blank:?} must be ignored"
        );
    }
}

#[test]
fn non_env_config_providers_ignore_mcp_json() {
    let mcp = r#"{"workspace-mcp":{"type":"local","command":["x"],"enabled":true}}"#;
    for id in ["auggie", "claude-code", "codex", "droid"] {
        let env = build_provider_env(find_provider(id).unwrap(), None, None, Some(mcp));
        assert!(
            !env.contains_key("OPENCODE_CONFIG_CONTENT"),
            "{id} unexpectedly emitted OPENCODE_CONFIG_CONTENT"
        );
    }
}

#[test]
fn only_grok_requires_terminal_shell_by_default() {
    for p in crate::ACP_PROVIDERS {
        if p.id == "grok" {
            assert!(
                p.terminal_requires_shell,
                "grok must shell-wrap ACP terminals"
            );
        } else {
            assert!(
                !p.terminal_requires_shell,
                "{} should not require terminal shell wrap",
                p.id
            );
        }
    }
}
