//! `ProviderConfig` registry — provider quirks are data, not code (§6.9).
//!
//! Static port of `ACP_PROVIDERS` / `ACPProviderConfig`
//! (`src/shared/config/provider-config.ts`). Each entry is data; adding a
//! provider is a config change, not a code change.

/// Single source of truth for the pinned `@agentclientprotocol/claude-agent-acp`
/// version (macro so the literal can feed both `CLAUDE_AGENT_ACP_VERSION` and
/// the `concat!`-built package spec). Bumping the adapter is a deliberate,
/// one-line code change here.
macro_rules! claude_agent_acp_version {
    () => {
        "0.66.0"
    };
}

/// Pinned version of the `@agentclientprotocol/claude-agent-acp` npm package
/// the claude-code provider is spawned with (via `npx`).
pub const CLAUDE_AGENT_ACP_VERSION: &str = claude_agent_acp_version!();

/// Full pinned npx package spec for the claude-code adapter
/// (`@agentclientprotocol/claude-agent-acp@<CLAUDE_AGENT_ACP_VERSION>`).
pub const CLAUDE_AGENT_ACP_NPX_PACKAGE: &str = concat!(
    "@agentclientprotocol/claude-agent-acp@",
    claude_agent_acp_version!()
);

/// Node.js version requirement for the pinned adapter, for user-facing
/// messages. Must match the npm `engines.node` field of the pinned
/// [`CLAUDE_AGENT_ACP_NPX_PACKAGE`] (currently `>=22`); re-check when bumping
/// the pin.
pub const CLAUDE_AGENT_ACP_NODE_REQUIREMENT: &str = "Node.js 22+";

/// Pinned npx package spec for the codex ACP fallback. intentd is the only
/// pin site (cloudlands-fe no longer pins a managed codex-acp version);
/// bumping the version is a deliberate code change.
pub const CODEX_ACP_NPX_PACKAGE: &str = "@agentclientprotocol/codex-acp@1.6.2";

/// Pinned npx package spec the pi provider is ALWAYS spawned with (via
/// `npx -y`). Mirrors the FE pin (`PI_ACP_NPX_PACKAGE` in `pi-resolver.ts`);
/// bumping the version is a deliberate code change. Also feeds the pi
/// model-catalog probe in `intent-services::provider_models`.
pub const PI_ACP_NPX_PACKAGE: &str = "pi-acp@0.0.33";

/// Minimum `pi` CLI version the pinned [`PI_ACP_NPX_PACKAGE`] adapter
/// requires. Feeds the pure version-gate decision in
/// [`crate::version_gate`]; re-check when bumping the pin.
pub const PI_CLI_MIN_VERSION: &str = "0.80.4";

/// Pi CLI version requirement for user-facing messages. Must match
/// [`PI_CLI_MIN_VERSION`]; re-check when bumping the pin.
pub const PI_CLI_REQUIREMENT: &str = "Pi CLI 0.80.4+";

/// Minimum `auggie` CLI version the ACP agent-spawn path requires. The daemon
/// launches auggie with `--acp --allow-indexing --model … --remove-tool …`;
/// the full flag set landed in auggie 0.7.0 (ACP with model selection and
/// `--allow-indexing`), so an older binary rejects the launch with an "Unknown
/// arguments" error. Feeds the pure version-gate decision in
/// [`crate::version_gate`]; re-check when the launch flags change.
pub const AUGGIE_CLI_MIN_VERSION: &str = "0.7.0";

/// Auggie CLI version requirement for user-facing messages. Must match
/// [`AUGGIE_CLI_MIN_VERSION`]; re-check when bumping the minimum.
pub const AUGGIE_CLI_REQUIREMENT: &str = "auggie 0.7.0+";

/// The runtime a provider's subprocess executes on. Drives runtime-specific
/// env assembly — V8-backed runtimes (`Node`, `Electron`) get a
/// `--max-old-space-size` heap cap injected via `NODE_OPTIONS` (STAB-50);
/// `Native` binaries are left untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderRuntime {
    /// Plain Node.js subprocess (V8).
    Node,
    /// Electron binary run with `ELECTRON_RUN_AS_NODE=1` (still V8).
    Electron,
    /// Natively-compiled binary — not V8, no `NODE_OPTIONS` handling.
    Native,
}

/// How a provider receives the assembled system prompt (base override,
/// specialist role, user rules, workspace rules, skills, isolation hints).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectionMechanism {
    /// CLI flag pointing to a rules file (e.g. `--rules`, `--append-system-prompt-file`).
    RulesFileFlag,
    /// ACP `session/new` / `session/load` `_meta` field (claude-code).
    SessionMeta,
    /// Environment variable config (opencode: `OPENCODE_CONFIG_CONTENT` with `instructions`).
    EnvConfig,
    /// First-turn prepend in a `<system>` block (codex, cortex, pi, grok, mock — fallback).
    FirstTurnPrepend,
    /// No injection mechanism (provider doesn't support system prompts).
    None,
}

/// Configuration for an ACP provider (port of `ACPProviderConfig`).
///
/// UI-only fields from the TS interface (`ipcChannelPrefix`, `iconPath`) are
/// intentionally omitted — they are Electron IPC / renderer concerns and are
/// not part of the §6.9 field list.
// Static registry entries port the TS `ACPProviderConfig` field-for-field;
// the independent capability bools stay flat for parity.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderConfig {
    /// Unique identifier (e.g., `auggie`, `opencode`).
    pub id: &'static str,
    /// Runtime the provider subprocess executes on (see [`ProviderRuntime`]).
    pub runtime: ProviderRuntime,
    /// Display name shown in UI (e.g., `Augment Auggie`).
    pub display_name: &'static str,
    /// Short display name shown in compact UI (e.g., `Auggie`). Port of the
    /// TS `shortName`.
    pub short_name: &'static str,
    /// CLI command to spawn the agent.
    pub command: &'static str,
    /// Default arguments for ACP mode.
    pub base_args: &'static [&'static str],
    /// Flag for model selection (e.g., `--model`). `None` when the provider
    /// passes model config through other mechanisms (env vars, custom args).
    pub model_flag: Option<&'static str>,
    /// Default agent name for the ACP session (e.g., `build` for `OpenCode`).
    pub default_agent: Option<&'static str>,
    /// Whether the provider implements the ACP `authenticate` method.
    pub supports_authenticate: bool,
    /// Whether the provider supports `session/set_mode`.
    pub supports_set_mode: bool,
    /// Whether the provider applies the selected model via `session/set_model`
    /// after session creation. Set for providers whose ACP subcommand has no
    /// CLI model flag (grok's `agent stdio`).
    pub supports_set_model: bool,
    /// Whether the provider exposes the model as an ACP session config option
    /// (`configOptions[id="model"]` in the `session/new` result) and applies
    /// it via `session/set_config_option` after session establishment
    /// (claude-code's pinned adapter).
    pub supports_config_option_model: bool,
    /// Whether the provider's stored model ids may embed a reasoning effort
    /// as a `{base}/{effort}` suffix that must be stripped before the id is
    /// sent as the post-session `session/set_config_option` model value
    /// (codex: the adapter's `configOptions[id="model"]` select values are
    /// bare base ids, and the effort rides the separate `reasoning_effort`
    /// option). Only meaningful alongside `supports_config_option_model`.
    pub config_option_model_strips_effort: bool,
    /// Whether the provider supports MCP server configuration via CLI args.
    pub supports_mcp_config: bool,
    /// Whether the provider consumes MCP servers from the ACP `session/new` /
    /// `session/load` request's `mcpServers` field (claude-code, codex,
    /// droid, grok).
    pub supports_session_mcp_servers: bool,
    /// Whether MCP delivery rides a bundled pi extension: `create_agent`
    /// writes the embedded extension plus a wrapper script that execs the
    /// real `pi` binary with `-e <extension>`, points pi-acp at the wrapper
    /// via `PI_ACP_PI_COMMAND`, and hands the extension the per-agent bridge
    /// TCP address via `INTENTD_MCP_BRIDGE_ADDR` (pi only).
    pub mcp_via_pi_extension: bool,
    /// Whether the provider supports rules files via CLI args.
    pub supports_rules_file: bool,
    /// Flag for the rules file (e.g., `--rules`).
    pub rules_flag: Option<&'static str>,
    /// How the provider receives the assembled system prompt.
    pub injection_mechanism: InjectionMechanism,
    /// Flag for the MCP config file (e.g., `--mcp-config`).
    pub mcp_config_flag: Option<&'static str>,
    /// Flag for quiet mode (e.g., `--quiet`).
    pub quiet_flag: Option<&'static str>,
    /// Flag for removing a provider-native tool at spawn time (e.g.
    /// `--remove-tool`). Repeated once per tool name. `None` when the provider
    /// exposes no equivalent knob — spawn-time tool restrictions are dropped
    /// for that provider (MCP-side filtering, §6.8, still applies).
    pub remove_tool_flag: Option<&'static str>,
    /// Optional provider-specific mode-map overrides (`logical -> provider`).
    pub mode_map: Option<&'static [(&'static str, &'static str)]>,
    /// Optional filter of available models for this provider.
    pub supported_models: Option<&'static [&'static str]>,
    /// Whether this provider can be disabled in settings.
    pub can_be_disabled: bool,
    /// Authentication error patterns used to detect auth failures.
    pub auth_error_patterns: Option<&'static [&'static str]>,
    /// Login command hint surfaced on authentication errors.
    pub login_command_hint: Option<&'static str>,
    /// If set, the provider is only visible when this env var is defined.
    pub requires_env_var: Option<&'static str>,
    /// If set, the provider is only visible when this feature code is active.
    pub requires_feature_code: Option<&'static str>,
    /// CLI args to check auth status (exit 0 == authenticated).
    pub auth_check_args: Option<&'static [&'static str]>,
    /// URL to login/auth docs for this provider.
    pub login_docs_url: Option<&'static str>,
    /// When provider binary cannot be resolved, fall back to spawning this npm
    /// package via `npx -y <package>`. Only set for providers shipped as npm
    /// packages (e.g. codex's `@agentclientprotocol/codex-acp`).
    pub fallback_npx_package: Option<&'static str>,
    /// When set, the provider is ALWAYS spawned via `npx -y <package>` with a
    /// version pinned by us — local binary discovery (settings path, managed
    /// bin, PATH scan) is skipped entirely, so the adapter version is under our
    /// release cadence (claude-code's [`CLAUDE_AGENT_ACP_NPX_PACKAGE`]).
    pub npx_only_package: Option<&'static str>,
    /// When set, discovery (`discover_providers`) only reports this provider
    /// as `installed` when BOTH `command` AND this secondary CLI resolve.
    /// Unsloth rides the `opencode` binary as its ACP runtime (`command`) but
    /// also requires the `unsloth` CLI itself (the daemon-managed server
    /// lifecycle, `unsloth_server.rs`) — reporting availability off
    /// `opencode` alone is misleading when the Unsloth CLI isn't installed.
    pub requires_secondary_binary: Option<&'static str>,
    /// When true, ACP `terminal/create` requests from this provider are spawned
    /// via a shell (`/bin/sh -c` on POSIX; PowerShell `-Command` / `cmd /c` on
    /// Windows) rather than as raw argv. Needed for agents that pack a full
    /// shell line into the
    /// `command` field with empty `args` (Node-style `shell: true` semantics).
    /// Grok Build's ACP adapter does this (`/bin/bash -lc '…'` in `command`);
    /// argv-only clients (most providers) leave this false.
    pub terminal_requires_shell: bool,
    /// When true, the provider's client silently truncates long MCP tool
    /// descriptions (claude-code cuts at ~2k chars — see
    /// <https://github.com/anthropics/claude-code/issues/53933>), so the
    /// `workspace_api` tool is served a compact description and the full
    /// `ws.*` API reference is appended to the system prompt instead.
    pub truncates_tool_descriptions: bool,
    /// When true, the keep-alive interrupt (`agent.stop` / message
    /// preemption) tears the child process down AFTER sending the polite
    /// `session/cancel`, instead of keeping it alive for an in-place resume.
    /// For providers whose cancellation is unreliable (auggie leaks the
    /// cancelled turn's subprocesses and keeps burning tokens — see
    /// intent-hq/monorepo#2763), a live-but-wedged child is worse than a
    /// respawn: the persisted `acpSessionId` survives the teardown, so the
    /// next `agent.sendMessage` respawns the child and resumes the session
    /// via the normal `session/load` ladder.
    pub kills_child_on_interrupt: bool,
}

impl ProviderConfig {
    const fn empty(id: &'static str, display_name: &'static str, command: &'static str) -> Self {
        Self {
            id,
            // Safest "do nothing" default: `Native` opts out of NODE_OPTIONS
            // injection. V8-backed providers override this per entry.
            runtime: ProviderRuntime::Native,
            display_name,
            // Registry entries override this with the FE's `shortName`;
            // falling back to the full display name keeps `empty()` total.
            short_name: display_name,
            command,
            base_args: &[],
            model_flag: None,
            default_agent: None,
            supports_authenticate: false,
            supports_set_mode: false,
            supports_set_model: false,
            supports_config_option_model: false,
            config_option_model_strips_effort: false,
            supports_mcp_config: false,
            supports_session_mcp_servers: false,
            mcp_via_pi_extension: false,
            supports_rules_file: false,
            rules_flag: None,
            injection_mechanism: InjectionMechanism::None,
            mcp_config_flag: None,
            quiet_flag: None,
            remove_tool_flag: None,
            mode_map: None,
            supported_models: None,
            can_be_disabled: false,
            auth_error_patterns: None,
            login_command_hint: None,
            requires_env_var: None,
            requires_feature_code: None,
            auth_check_args: None,
            login_docs_url: None,
            fallback_npx_package: None,
            npx_only_package: None,
            requires_secondary_binary: None,
            terminal_requires_shell: false,
            truncates_tool_descriptions: false,
            kills_child_on_interrupt: false,
        }
    }

    /// The provider id that OWNS this provider's primary (`command`) binary
    /// for `providers.paths` override purposes. Unsloth rides the `opencode`
    /// binary as its ACP runtime, so its primary spawn resolution honors
    /// `providers.paths["opencode"]` — `providers.paths["unsloth"]` instead
    /// targets the `unsloth` CLI itself (the secondary binary the
    /// daemon-managed server lifecycle shells out to). Every other provider
    /// owns its own primary.
    #[must_use]
    pub fn primary_binary_provider_id(&self) -> &'static str {
        match self.id {
            "unsloth" => "opencode",
            _ => self.id,
        }
    }
}

/// All available ACP providers, in the same definition order as
/// `ACP_PROVIDERS` (`provider-config.ts`).
pub static ACP_PROVIDERS: &[ProviderConfig] = &[
    // NOTE (auggie modes): auggie advertises `session/set_mode` support but its
    // `availableModes` today are `default` + `ask` — it does NOT offer a
    // `bypassPermissions` (or otherwise-permissive) mode. We keep
    // `supports_set_mode: true` because the method itself is implemented, but
    // deliberately leave `mode_map: None` so `select_preferred_mode` finds no
    // advertised bypass-equivalent and `try_bypass_permissions_mode` skips the
    // call rather than triggering a `-32602` invalid-params error. Under the
    // shipped `AllowAll` policy the local auto-approve path in
    // `ClientRequestHandler` remains authoritative for auggie sessions.
    ProviderConfig {
        runtime: ProviderRuntime::Node,
        base_args: &["--acp", "--allow-indexing"],
        model_flag: Some("--model"),
        can_be_disabled: true,
        supports_authenticate: true,
        supports_set_mode: true,
        supports_mcp_config: true,
        supports_rules_file: true,
        rules_flag: Some("--rules"),
        injection_mechanism: InjectionMechanism::RulesFileFlag,
        mcp_config_flag: Some("--mcp-config"),
        quiet_flag: Some("--quiet"),
        remove_tool_flag: Some("--remove-tool"),
        auth_error_patterns: Some(&[
            "authentication required",
            "auggie login",
            "please run `auggie login`",
        ]),
        // `auggie token print` exits 0 when logged in, non-zero when logged
        // out. Its stdout IS the auth session secret, so the probe must stay on
        // the generic exit-code arm of `check_provider_auth_cli` (stdout and
        // stderr nulled) — never captured, logged, or surfaced.
        auth_check_args: Some(&["token", "print"]),
        login_command_hint: Some("auggie login"),
        login_docs_url: Some("https://docs.augmentcode.com/cli/overview"),
        short_name: "Auggie",
        // auggie's `session/cancel` is unreliable: the cancelled turn's
        // subprocesses leak and keep consuming tokens
        // (intent-hq/monorepo#2763), so interrupts tear the child down and
        // the next send respawns + resumes off the persisted `acpSessionId`.
        kills_child_on_interrupt: true,
        ..ProviderConfig::empty("auggie", "Augment Auggie", "auggie")
    },
    ProviderConfig {
        runtime: ProviderRuntime::Node,
        can_be_disabled: true,
        injection_mechanism: InjectionMechanism::SessionMeta,
        // The pinned claude-agent-acp adapter maps `session/new` `mcpServers`
        // (stdio entries without a `type` tag) into the Claude Agent SDK's
        // `options.mcpServers`, so the workspace bridge rides the ACP request.
        supports_session_mcp_servers: true,
        // The adapter's `session/new` result advertises the model as a
        // `configOptions[id="model"]` select; the stored model is applied
        // post-session via `session/set_config_option` (there is no CLI
        // model flag on the pinned adapter).
        supports_config_option_model: true,
        auth_check_args: Some(&["auth", "status"]),
        login_docs_url: Some(
            "https://code.claude.com/docs/en/quickstart#step-2-log-in-to-your-account",
        ),
        npx_only_package: Some(CLAUDE_AGENT_ACP_NPX_PACKAGE),
        short_name: "Claude Code",
        // Claude Code silently truncates MCP tool descriptions at ~2k chars
        // (anthropics/claude-code#53933): serve the compact `workspace_api`
        // description and carry the full API reference in the system prompt.
        truncates_tool_descriptions: true,
        ..ProviderConfig::empty("claude-code", "Anthropic Claude Code", "claude-agent-acp")
    },
    ProviderConfig {
        // Declared Native (the `empty()` default) for the Rust `codex-acp`
        // binary: no V8 heap-cap env on that path. The npx fallback
        // (`@agentclientprotocol/codex-acp`, pure Node) is detected at spawn
        // time and DOES get the NODE_OPTIONS heap cap
        // (`build_provider_env_for_spawn`, intent-hq/monorepo#1661).
        can_be_disabled: true,
        // The pinned @agentclientprotocol/codex-acp adapter (1.6.2) ignores
        // `_meta.developerInstructions` (verified empirically, #479; still
        // true at 1.6.2 — the adapter never reads that key from session
        // params), so the system prompt is delivered via the first-turn
        // `<system>` prepend instead of SessionMeta.
        injection_mechanism: InjectionMechanism::FirstTurnPrepend,
        // codex-acp folds `session/new` `mcpServers` (stdio + http) into its
        // session config (`build_session_config`), so the workspace bridge
        // rides the ACP request rather than `-c mcp_servers.*` overrides.
        supports_session_mcp_servers: true,
        // The npx fallback adapter ignores `-c model=…` argv overrides (its
        // CLI parses no config flags), and its `session/set_model` handler
        // (1.1.14) is unusable for our ids — `ModelId.fromString` accepts
        // only `{base}[{effort}]` with the effort REQUIRED, rejecting both
        // bare and `{base}/{effort}` ids. The stored model is instead
        // applied post-session via `session/set_config_option`: the adapter
        // advertises the model as a `configOptions[id="model"]` select over
        // bare base ids (`createModelConfigOption`), falling back to the
        // current/default reasoning effort when the model changes. A
        // `{base}/{effort}` suffix is stripped daemon-side before sending
        // (`config_option_model_target`); the effort itself rides the
        // generic `thought_level` option (`reasoning_effort`). The `-c`
        // args (`apply_codex_config_args`) are kept for the native Rust
        // codex-acp binary path, which does consume them.
        supports_config_option_model: true,
        config_option_model_strips_effort: true,
        auth_check_args: Some(&["login", "status"]),
        login_docs_url: Some("https://developers.openai.com/codex/cli#cli-setup"),
        fallback_npx_package: Some(CODEX_ACP_NPX_PACKAGE),
        short_name: "Codex",
        ..ProviderConfig::empty("codex", "OpenAI Codex", "codex-acp")
    },
    ProviderConfig {
        // Electron binary run with `ELECTRON_RUN_AS_NODE=1` — still V8, so it
        // gets the NODE_OPTIONS heap cap like plain Node providers (STAB-50).
        runtime: ProviderRuntime::Electron,
        can_be_disabled: true,
        injection_mechanism: InjectionMechanism::FirstTurnPrepend,
        // Hidden by default (not yet well-tested); set INTENTD_ENABLE_CORTEX
        // in the daemon environment to re-enable (same mechanism as mock's
        // MOCK_AGENT_SCRIPT_PATH gate).
        requires_env_var: Some("INTENTD_ENABLE_CORTEX"),
        short_name: "Cortex",
        // Cortex defers ALL MCP tools by default (`settings.toolSearch !==
        // false` — default ON): a names-only reminder ("Schemas are NOT
        // loaded in your context") replaces description text unless the
        // model calls `tool_search`. However, this entry has NO MCP delivery
        // channel yet (no `supports_mcp_config` / `supports_session_mcp_servers`
        // / env config / pi extension), so the workspace bridge never reaches
        // cortex sessions — flipping `truncates_tool_descriptions` here would
        // inject the full ws.* reference for tools cortex cannot call. Flip it
        // together with bridge delivery: intent-hq/monorepo#3303.
        ..ProviderConfig::empty("cortex", "Snowflake Cortex", "cortex-acp")
    },
    ProviderConfig {
        runtime: ProviderRuntime::Node,
        base_args: &["acp"],
        can_be_disabled: true,
        injection_mechanism: InjectionMechanism::EnvConfig,
        auth_check_args: Some(&["models"]),
        login_docs_url: Some("https://opencode.ai/docs#configure"),
        short_name: "OpenCode",
        ..ProviderConfig::empty("opencode", "OpenCode", "opencode")
    },
    ProviderConfig {
        // Rides the opencode binary (`opencode acp`) as its ACP runtime: the
        // Unsloth-managed local OpenAI-compatible server is injected as a
        // custom `provider.unsloth-studio` block via `OPENCODE_CONFIG_CONTENT`
        // (`build_provider_env`, args.rs). Endpoint/apiKey/model come from
        // the managed-server lifecycle at spawn time (`UnslothEndpoint`);
        // no CLI auth probe — the injected config carries its own apiKey.
        runtime: ProviderRuntime::Node,
        base_args: &["acp"],
        can_be_disabled: true,
        injection_mechanism: InjectionMechanism::EnvConfig,
        // Availability requires BOTH `opencode` (the ACP runtime, `command`
        // above) AND the `unsloth` CLI itself — the daemon-managed server
        // lifecycle (`unsloth_server.rs`) shells out to `unsloth run` /
        // `unsloth start opencode` directly, independent of the ACP spawn.
        requires_secondary_binary: Some("unsloth"),
        short_name: "Unsloth",
        ..ProviderConfig::empty("unsloth", "Unsloth", "opencode")
    },
    ProviderConfig {
        runtime: ProviderRuntime::Node,
        can_be_disabled: true,
        // pi-acp (0.0.33) has no `_meta` system-prompt path and no rules/MCP
        // CLI flags (it advertises `mcpCapabilities: { http: false, sse:
        // false }` and does not wire `session/new` `mcpServers` into the pi
        // process), so the assembled prompt is prepended on the first turn.
        injection_mechanism: InjectionMechanism::FirstTurnPrepend,
        // MCP delivery instead rides a bundled pi extension: the spawn env
        // routes pi-acp's pi spawn through a wrapper script adding
        // `-e <extension>` (PI_ACP_PI_COMMAND), and the extension dials the
        // per-agent workspace bridge over TCP (INTENTD_MCP_BRIDGE_ADDR).
        mcp_via_pi_extension: true,
        // The adapter's `session/new` result advertises the model as a
        // `configOptions[id="model"]` select; the stored model is applied
        // post-session via `session/set_config_option` (verified against
        // pi-acp@0.0.33's `setSessionConfigOption`; no CLI model flag).
        supports_config_option_model: true,
        login_docs_url: Some("https://pi.dev/docs/latest/quickstart"),
        npx_only_package: Some(PI_ACP_NPX_PACKAGE),
        short_name: "Pi",
        ..ProviderConfig::empty("pi", "Pi", "pi-acp")
    },
    ProviderConfig {
        // Native binary (the `empty()` default): no V8 heap-cap env.
        base_args: &["exec", "--output-format", "acp"],
        model_flag: Some("--model"),
        can_be_disabled: true,
        supports_rules_file: true,
        rules_flag: Some("--append-system-prompt-file"),
        injection_mechanism: InjectionMechanism::RulesFileFlag,
        // droid's ACP mode accepts `session/new` `mcpServers` (the standard
        // ACP session-setup field); the CLI has no per-spawn MCP flag, so the
        // ACP request is the only spawn-scoped delivery mechanism.
        supports_session_mcp_servers: true,
        login_docs_url: Some("https://docs.factory.ai/cli/getting-started/overview"),
        // Hidden by default (not yet well-tested); set INTENTD_ENABLE_DROID
        // in the daemon environment to re-enable.
        requires_env_var: Some("INTENTD_ENABLE_DROID"),
        short_name: "Droid",
        // Defensive: droid's remote Statsig feature `mcp_tool_search` defers
        // every non-github MCP server's tools behind a "Deferred tools:"
        // reminder whose per-tool summary is the description's first line
        // truncated to 200 chars — and the flag can flip server-side without
        // a CLI update. Serve the compact `workspace_api` description and
        // carry the full ws.* reference in the system prompt
        // (`--append-system-prompt-file`).
        truncates_tool_descriptions: true,
        ..ProviderConfig::empty("droid", "Factory Droid", "droid")
    },
    ProviderConfig {
        // Native binary (the `empty()` default): no V8 heap-cap env. Grok's
        // ACP stdio mode selects models after session creation via
        // `session/set_model`, so there is no CLI model flag here.
        base_args: &["agent", "stdio"],
        can_be_disabled: true,
        supports_set_model: true,
        injection_mechanism: InjectionMechanism::FirstTurnPrepend,
        // grok's ACP stdio mode accepts `session/new` `mcpServers` (the
        // standard ACP session-setup field; verified on grok 0.2.111 — stdio
        // entries are exposed and callable); the CLI has no per-spawn MCP
        // flag, so the ACP request is the only spawn-scoped delivery
        // mechanism.
        supports_session_mcp_servers: true,
        login_command_hint: Some("grok login"),
        // `grok models` prints auth/readiness details to stdout (exit code 0
        // in both auth states); the daemon parses that output instead of
        // using ACP `authenticate` (see `models::parse_grok_models_command_output`).
        auth_check_args: Some(&["models"]),
        // Grok never puts MCP tools in the model's tool list
        // (`tool_definitions_builtins_only()` filters MCP-qualified names);
        // discovery rides its `search_tool` meta-tool, which truncates every
        // description at 2,048 chars (`MAX_MCP_DESCRIPTION_LENGTH`). Serve
        // the compact `workspace_api` description and carry the full ws.*
        // reference in the system prompt (first-turn prepend).
        truncates_tool_descriptions: true,
        login_docs_url: Some("https://docs.x.ai/build/enterprise#authentication"),
        short_name: "Grok",
        // Grok's ACP terminal adapter packs `/bin/bash -lc '…'` into `command`
        // with empty `args` (Node shell:true style). intentd argv-only spawn
        // would ENOENT that string; shell-wrap on terminal/create instead.
        terminal_requires_shell: true,
        ..ProviderConfig::empty("grok", "Grok Build", "grok")
    },
    ProviderConfig {
        runtime: ProviderRuntime::Node,
        supports_authenticate: true,
        can_be_disabled: true,
        injection_mechanism: InjectionMechanism::FirstTurnPrepend,
        requires_env_var: Some("MOCK_AGENT_SCRIPT_PATH"),
        short_name: "Mock",
        ..ProviderConfig::empty("mock", "Mock (E2E)", "node")
    },
];

/// Find a provider by id, or `None` if unknown.
#[must_use]
pub fn find_provider(provider_id: &str) -> Option<&'static ProviderConfig> {
    ACP_PROVIDERS.iter().find(|p| p.id == provider_id)
}

/// The first registered provider — a neutral positional last resort used
/// ONLY when no settings-derived default (provider of `model.default`, else
/// `providers.active`) is reachable. No provider carries a privileged
/// default designation.
pub(crate) fn first_provider_config() -> &'static ProviderConfig {
    ACP_PROVIDERS
        .first()
        .expect("at least one ACP provider must be configured")
}

/// The first registered provider id (see [`first_provider_config`]).
#[must_use]
pub fn first_provider_id() -> &'static str {
    first_provider_config().id
}

/// Legacy aliases for the default provider that are expected to miss the
/// registry and must not trigger the unknown-provider warning. Port of the
/// suppression list in `getProviderConfig`.
const DEFAULT_PROVIDER_ALIASES: &[&str] = &["default", "acp", "augment"];

/// Resolve a provider by id, falling back to the first registered provider
/// when unknown. Unknown ids warn (see [`warns_on_unknown_provider`]) so
/// registry gaps surface in logs instead of silently spawning the fallback
/// agent. Port of `getProviderConfig`.
#[must_use]
pub fn provider_config(provider_id: &str) -> &'static ProviderConfig {
    find_provider(provider_id).unwrap_or_else(|| {
        let fallback = first_provider_config();
        if warns_on_unknown_provider(provider_id) {
            tracing::warn!(
                provider_id = provider_id,
                fallback_id = fallback.id,
                "unknown provider id; falling back to the first registered provider"
            );
        }
        fallback
    })
}

/// Whether an id missing from the registry should emit the unknown-provider
/// warning: empty ids and legacy default aliases ([`DEFAULT_PROVIDER_ALIASES`])
/// are expected fallbacks and stay silent.
pub(crate) fn warns_on_unknown_provider(provider_id: &str) -> bool {
    !provider_id.is_empty() && !DEFAULT_PROVIDER_ALIASES.contains(&provider_id)
}

/// All registered provider ids, in definition order. Port of `getAllProviderIds`.
#[must_use]
pub fn all_provider_ids() -> Vec<&'static str> {
    ACP_PROVIDERS.iter().map(|p| p.id).collect()
}

/// Providers that can be disabled in settings. Port of `getDisableableProviders`.
#[cfg(test)]
pub(crate) fn disableable_providers() -> Vec<&'static ProviderConfig> {
    ACP_PROVIDERS.iter().filter(|p| p.can_be_disabled).collect()
}

/// Providers that are always enabled. Port of `getAlwaysEnabledProviders`.
#[cfg(test)]
pub(crate) fn always_enabled_providers() -> Vec<&'static ProviderConfig> {
    ACP_PROVIDERS
        .iter()
        .filter(|p| !p.can_be_disabled)
        .collect()
}

/// Build the user-facing authentication-required message for a provider,
/// including the login command hint (`login_command_hint`, else
/// `{command} login`). Port of `getProviderAuthErrorMessage`.
pub fn auth_error_message(provider_id: &str, is_remote: bool) -> String {
    let config = provider_config(provider_id);
    let login_cmd = config.login_command_hint.map_or_else(
        || format!("{} login", config.command),
        std::string::ToString::to_string,
    );

    if is_remote {
        format!(
            "{} needs to be authenticated on the remote server. Run \"{}\" in a terminal connected to the remote environment.",
            config.display_name, login_cmd
        )
    } else {
        format!(
            "{} needs to be authenticated. Run \"{}\" in a terminal.",
            config.display_name, login_cmd
        )
    }
}

/// Whether an error message indicates the provider needs authentication,
/// using the provider's configured `auth_error_patterns` (case-insensitive).
/// Port of `isProviderAuthenticationError`.
#[must_use]
pub fn is_provider_authentication_error(provider_id: &str, error_message: &str) -> bool {
    let config = provider_config(provider_id);
    let Some(patterns) = config.auth_error_patterns else {
        return false;
    };
    let lower = error_message.to_lowercase();
    patterns
        .iter()
        .any(|pattern| lower.contains(&pattern.to_lowercase()))
}
