# Adding a New ACP Provider

A checklist for wiring a new ACP (Agent Client Protocol) provider into intentd. Each item
cites the file/symbol it refers to; the code is the source of truth — audit it rather than
trusting this document if the two ever disagree.

Provider quirks are **data, not code**: most of the work is a new `ProviderConfig`
registry entry plus deciding which existing delivery mechanism each capability rides on.
The areas below are everything the opencode/claude-code/codex/droid parity effort had to
touch; treat the full list as the definition of done for a new provider.

## TL;DR checklist

- [ ] Registry entry in `ACP_PROVIDERS` (`crates/intent-providers/src/config.rs`)
- [ ] Binary discovery verified (`find_provider_binary`, `crates/intent-providers/src/discover.rs`)
- [ ] System-prompt delivery mechanism chosen + wired (`InjectionMechanism`)
- [ ] Workspace-MCP bridge delivery chosen + wired (one of three paths, see below)
- [ ] Workspace naming nudge spelling registered once empirically captured
      (`workspace_naming_tool_reference`, `crates/intent-services/src/agent_manager.rs`)
- [ ] Tool-name/kind derivation extended from captured ACP traffic
      (`derive_tool_name` / `tool_kind_word`, `crates/intent-acp/src/session.rs`)
- [ ] Policy items: native-subagent denial, V8 heap cap (`runtime`), model-id resolution
- [ ] Unit tests per area + WSS e2e (`crates/intentd/tests/`), gates green
      (`cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`)
- [ ] End-to-end smoke test via `make dev` (see last section)

## 1. Registry entry (`ProviderConfig`)

Add an entry to `ACP_PROVIDERS` in `crates/intent-providers/src/config.rs`, starting from
`ProviderConfig::empty(id, display_name, command)` and overriding only what the provider
needs. Fields that matter most:

- **`id`** — stable identifier (`auggie`, `opencode`, …). Keys everything: settings
  (`providers.paths`), session `provider` fields, per-provider match arms.
- **`command` / `base_args`** — the CLI and its ACP-mode args (e.g. opencode: `["acp"]`,
  droid: `["exec", "--output-format", "acp"]`, grok: `["agent", "stdio"]`).
- **`runtime`** (`ProviderRuntime`) — `Node`, `Electron`, or `Native`. Anything V8-backed
  (`Node`/`Electron`) gets `NODE_OPTIONS=--max-old-space-size=<MB>` injected by
  `build_provider_env` (`crates/intent-providers/src/args.rs`) to raise the ~1.7 GB V8
  default old-space cap that OOM-killed long coordinator sessions (STAB-50). The cap
  resolves as an inherited `NODE_OPTIONS` that already contains `--max-old-space-size`
  (left untouched — the daemon never clobbers a parent cap) >
  `INTENTD_ACP_NODE_MAX_OLD_SPACE_MB` env var (only when it parses as a `u32`; a
  malformed value is WARN-logged and skipped) > `agents.acpNodeMaxOldSpaceMb`
  daemon setting (1024–65536, threaded in via `SpawnOptions::node_max_old_space_mb`) >
  8192 MB default (`DEFAULT_ACP_NODE_MAX_OLD_SPACE_MB` in intent-core); it is read live
  per spawn, so a settings change applies to the next spawned provider process.
  `Native` (Rust/Go binaries: codex, droid, grok) opts out. Getting this wrong is silent —
  a `Native` mislabel on a Node provider reintroduces mid-turn SIGABRT crashes.
- **`injection_mechanism`** (`InjectionMechanism`) — how the assembled system prompt
  reaches the agent. See §2.
- **`supports_mcp_config` / `mcp_config_flag`** — provider takes an MCP config *file* via
  CLI flag (auggie: `--mcp-config`). See §3a.
- **`supports_session_mcp_servers`** — provider consumes the typed `mcpServers` field on
  ACP `session/new` / `session/load` (claude-code, codex, droid, grok). See §3c.
- **`model_flag`** — CLI model selection (auggie/droid: `--model`). Providers that select
  models post-session via `session/set_model` (grok) set `supports_set_model: true` and no
  flag; providers whose adapter exposes the model as a `configOptions[id="model"]` select
  (claude-code, pi, codex) set `supports_config_option_model: true` instead; see
  `AgentManager::maybe_apply_session_model`
  (`crates/intent-services/src/agent_manager.rs`). codex also emits `-c model=…` config
  overrides for the native binary path — `apply_codex_config_args`
  (`crates/intent-providers/src/args.rs`) — and sets
  `config_option_model_strips_effort: true` so a `{base}/{effort}` id is stripped to its
  base before it is sent as the config-option value.
- **`remove_tool_flag`** — CLI-side tool stripping (auggie: `--remove-tool`), used for
  subagent denial (§6). `None` means spawn-time restrictions are silently dropped for
  this provider and only the MCP-side denylist applies.
- **Auth fields** — `auth_check_args` (exit-0 probe or parsed output; see grok's
  `parse_grok_models_command_output` in `crates/intent-providers/src/models.rs`),
  `auth_error_patterns` (stderr matching), `login_command_hint`, `login_docs_url`.
- **npx fields** — `fallback_npx_package` (spawn `npx -y <pkg>` only when no local binary
  resolves; codex) vs `npx_only_package` (spawn via npx with a version we pin, skipping
  auto-discovery entirely; claude-code, pi). The one npx-only exception, for providers
  that opt in via `npx_only_honors_path_override` (claude-code), is a valid
  `providers.paths[id]` override (absolute + executable): it is exec'd directly in place
  of the pinned npx spawn (`resolve_npx_only_override`, intent-hq/monorepo#4352) and the
  same override drives discovery's `installed`, the one-shot / test-prompt launches, and
  the claude-code ACP auth fallback probe, so every launch surface runs the same adapter
  (the model-catalog fetch stays on the pinned package). pi does not opt in: its adapter
  also depends on the version-gated real `pi` CLI, so it keeps pinned-npx-only
  semantics. Resolution:
  `resolve_npx_only` in `crates/intent-services/src/agent_manager.rs` and the
  `npx_fallback_*` fields on `SpawnOptions` (`crates/intent-acp/src/spawn.rs`).

**Binary discovery** — `find_provider_binary` (`crates/intent-providers/src/discover.rs`)
resolves in precedence order: (1) explicit `providers.paths[id]` setting (must be absolute
+ executable), (2) the provider's native-installer location where one exists
(`find_provider_native_binary`: grok `~/.grok/bin/grok`, opencode
`~/.opencode/bin/opencode`), (3) `~/.augment/bin/<command>` (auggie back-compat tier),
(4) enhanced-PATH scan (nvm/homebrew/volta/asdf dirs). At spawn
time `enhanced_path` (`crates/intent-providers/src/args.rs`) prepends the binary's parent
dir so `#!/usr/bin/env node` shebangs resolve a co-located node.

The `providers.paths` key is the provider that OWNS the binary
(`ProviderConfig::primary_binary_provider_id`). For providers that ride another
provider's binary (unsloth's ACP primary is `opencode`), the primary spawn honors the
owning provider's key (`providers.paths["opencode"]`), while the provider's own key
targets its secondary CLI — `providers.paths["unsloth"]` overrides the `unsloth` CLI
resolution in the daemon-managed server lifecycle
(`crates/intent-services/src/unsloth_server.rs`).

Registry/args/env behavior is covered by `crates/intent-providers/src/tests.rs` — add
cases for every field you set.

## 2. System-prompt delivery

intentd assembles one effective system prompt per agent (`assemble_system_prompt`,
`crates/intent-services/src/rules.rs`) in `AgentManager::create_agent`
(`crates/intent-services/src/agent_manager.rs`) and persists it on the session
(`system_prompt`). The provider's `injection_mechanism` decides how it is delivered:

| Mechanism | Providers | Wiring |
|---|---|---|
| `RulesFileFlag` | auggie (`--rules`), droid (`--append-system-prompt-file`) | `create_agent` writes a temp rules file; `build_provider_args` (`crates/intent-providers/src/args.rs`) appends `rules_flag` + path, gated on `supports_rules_file`. |
| `SessionMeta` | claude-code (system prompt), codex (session title only) | `build_session_meta` (`crates/intent-services/src/agent_session.rs`) builds a provider-shaped `_meta`: claude-code `{ "claudeCode": { "options": { "disallowedTools": ["Task"] } }, "systemPrompt": "<prompt>" }` (a string `systemPrompt` fully replaces the claude_code preset prompt — at adapter 0.66.0 the string is passed to the SDK as-is and treated as a custom prompt, so the model sees only our assembled instructions), sent on `session/new` **and** `session/load` (and the recreate path); codex `{ "sessionTitle": "<agent name>" }`, sent on `session/new` only (create + recreate; never on `session/load` — monorepo#3151), while its system prompt still travels via `FirstTurnPrepend` below. Carried by `session::new_session` / `load_session` (`crates/intent-acp/src/session.rs`). |
| `EnvConfig` | opencode | `build_provider_env` (`crates/intent-providers/src/args.rs`) emits `OPENCODE_CONFIG_CONTENT` with an `instructions: [<rules file path>]` key (plus `model`, `permission`, `mcp` — see §3b and §6). |
| `FirstTurnPrepend` | codex (the pinned codex-acp adapter ignores `_meta.developerInstructions`, #479), cortex, pi, grok (fallback), antigravity, plus the e2e-only `mock` provider | `arm_first_turn_prepend` / `build_first_turn_prepend` (`crates/intent-services/src/agent_manager.rs`): the persisted prompt is prepended as a `<system>` block on the first turn of a *fresh* session only (never on a `session/load` resume, which retained context). |
| `None` | — | Provider gets no system prompt. Avoid if at all possible. |

Add a new mechanism only if the provider genuinely supports none of these; prefer reusing
an existing arm. `_meta` shapes must be verified against the provider's adapter source or
captured traffic — they are not part of the ACP spec proper.

## 3. Workspace-MCP bridge delivery

Every spawned agent gets a per-agent in-process MCP server over the same `WorkspaceApi`
surface the FE uses (`WorkspaceMcpServer::for_agent_type`,
`crates/intent-acp/src/mcp_server.rs`), served over a loopback TCP bridge
(`serve_workspace_mcp_tcp`, `crates/intent-acp/src/mcp_bridge.rs`). The child reaches it
by spawning `intentd mcp-bridge --connect <addr>` as a stdio MCP server.
`AgentManager::normalized_mcp_servers` builds the canonical server set: the reserved
`workspace-mcp` bridge entry plus the user's `mcp.servers` catalog (OAuth header
injection, `mcp.enableUserServers` gate). That one normalized set is then translated
per provider — pick exactly one delivery path:

- **(a) `--mcp-config` file** (auggie): `supports_mcp_config: true` + `mcp_config_flag`.
  `create_agent` writes a temp JSON file via `generate_mcp_config` →
  `to_auggie_mcp_config` (`crates/intent-acp/src/mcp_config.rs`).
- **(b) env config** (opencode): `injection_mechanism: EnvConfig`.
  `opencode_env_mcp_config` → `to_opencode_mcp_config` serializes the set as the
  OpenCode `mcp` block, merged into `OPENCODE_CONFIG_CONTENT` by `build_provider_env`
  (`SpawnOptions.env_mcp_config`, `crates/intent-acp/src/spawn.rs`).
- **(c) ACP session field** (claude-code, codex, droid, grok, antigravity):
  `supports_session_mcp_servers: true`. `create_agent` stashes
  `to_acp_session_mcp_servers(...)` on the agent handle; `start_session`
  (`crates/intent-services/src/agent_manager.rs`) passes it into every session-open
  branch (`session/new`, `session/load`, recreate). Http/sse entries are filtered
  post-handshake against the agent's advertised `mcpCapabilities` from `initialize` —
  an agent that didn't advertise them may reject the whole `session/new`; stdio (the
  workspace bridge) always passes.
- **(d) bundled pi extension** (pi): `mcp_via_pi_extension: true`. `create_agent`
  writes the embedded extension (`pi_mcp_extension.ts`, via `include_str!`) plus a
  wrapper script that execs the real `pi` binary with `-e <extension>`, then sets
  `PI_ACP_PI_COMMAND=<wrapper>` and `INTENTD_MCP_BRIDGE_ADDR=<bridge host:port>` in
  the spawn env; the extension dials the bridge over TCP and registers the tools
  with pi. Only the reserved workspace bridge is delivered this way (no user
  `mcp.servers` translation).

**Verify tools actually reach the agent** — adapters differ in which session-setup fields
they honor; do not assume wiring works because the spawn succeeded. Unit-test the
translator output (tests in `crates/intent-acp/src/tests.rs`), integration-test with the
mock fixtures (`crates/intentd/tests/fixtures/mock-acp-agent.mjs`, `mock-mcp-server.mjs`),
and dogfood: ask the new agent to list its tools and to call `set_workspace_title` (§8).

## 4. Workspace naming nudge

On an agent's first turn in a still-untitled workspace, the daemon prepends a nudge to
call the workspace-rename tool (`build_workspace_naming_instruction`,
`crates/intent-services/src/agent_manager.rs`). The tool must be spelled the way the
provider actually surfaces it — providers affix the MCP server name differently:

- auggie: trailing *suffix* — `set_workspace_title_workspace-mcp`
- opencode: leading *prefix* — `workspace-mcp_set_workspace_title` (confirmed against
  captured opencode 1.18.3 traffic)
- everyone else: the generic fallback phrasing (`GENERIC_NAMING_TOOL_REFERENCE`,
  "the `set_workspace_title` tool from the workspace MCP server")

Register the new provider's spelling in `workspace_naming_tool_reference`
(`crates/intent-services/src/agent_manager.rs`) **only after empirically capturing it**
(spawn the agent, list its tools or observe a tool call). Until then the generic fallback
is correct and safe. Tests: the `build_turn_prompt_naming_instruction_*` cases in
`crates/intent-services/src/agent_manager/tests.rs`.

## 5. Tool-name / kind derivation

The FE renders tool calls from `tool_name` / `tool_kind`, derived from ACP
`session/update` tool-call notifications by `derive_tool_name` and `tool_kind_word`
(`crates/intent-acp/src/session.rs`). Providers title their native tools differently
(auggie: `name: input` prefixes handled by `split_name_prefix`; opencode: bare camelCase
titles like `webfetch`, plus camelCase `raw_input` shapes captured from opencode 1.18.3;
workspace-MCP affixes stripped by `strip_workspace_mcp_affix`), so a new provider almost
certainly needs new normalization arms:

1. **Capture real ACP traffic** for the provider's native tools (file read/write, edit,
   shell, search, fetch). Run a session and log the raw `session/update` payloads, or use
   the agent stderr logs (`<data-dir>/agent-logs/<agent-id>/`, STAB-53).
2. Extend `derive_tool_name` / `derive_tool_name_from_input` / `tool_kind_word` so the
   provider's spellings map onto the canonical names (`str-replace-editor`, `web-fetch`,
   `codebase-retrieval`, …) and the intentd kind taxonomy
   (`file|terminal|search|note|git|other`).
3. Add tests **using the captured payloads verbatim** — see the captured-opencode-frames
   mapping test in `crates/intent-acp/src/tests.rs` for the pattern.

## 6. Policy items

- **Native-subagent denial** — spawned agents must delegate through the workspace
  `ws.agent.*` surface, not provider-native subagents (which have no UI representation).
  Each provider needs its own knob:
  - auggie: `--remove-tool` per `SUBAGENT_TOOLS` entry (`get_tools_to_remove`,
    `crates/intent-acp/src/tool_restrictions.rs`), applied at spawn.
  - opencode: `"permission": { "task": "deny" }` always emitted in
    `OPENCODE_CONFIG_CONTENT` (`build_provider_env`, `crates/intent-providers/src/args.rs`).
  - claude-code: `disallowedTools: ["Task"]` in the `session/new` `_meta`
    (`build_session_meta`, `crates/intent-services/src/agent_session.rs`).
  Audit the new provider for an equivalent (config key, CLI flag, or `_meta` option) and
  wire it into whichever delivery mechanism the provider already uses. Independent of
  this, the MCP-side denylist (`WorkspaceMcpServer::for_agent_type` →
  `get_tool_denylist_for_agent_type`) filters workspace tools by agent type; it covers
  MCP tools only, never provider-native ones.
- **V8 heap cap** — set `runtime` correctly (§1); this *is* the policy knob.
- **Model ids** — models are stored as BARE ids; the provider is a separate field
  everywhere (compound `provider:model` ids are rejected at the wire). The model id
  feeds `model_flag` / `session/set_model`, and provider resolution
  (`resolve_provider_id`, `crates/intent-services/src/agent_session.rs`) is
  session `provider` field → settings-derived default (`model.defaultProvider`).
  Model discovery is fully dynamic (`models.list` sources,
  `crates/intent-services/src/model_catalog.rs`); there is no static tier catalog.
  Fuzzy model matching against a dynamic pool goes through
  `resolve_preferred_model`. codex additionally splits reasoning effort from
  the model id (`parse_codex_reasoning_effort`).

## 7. Tests and gates

Per-area expectations (all in the intentd repo):

- Registry/args/env/discovery/models: `crates/intent-providers/src/tests.rs`.
- Spawn command assembly (args, env merge, PATH): `crates/intent-acp/src/spawn.rs` tests.
- MCP translator output: `crates/intent-acp/src/tests.rs`.
- Tool-name/kind mapping from captured payloads: `crates/intent-acp/src/tests.rs`.
- `_meta` shapes + provider resolution: `crates/intent-services/src/agent_session/tests_meta.rs`.
- Naming nudge: `crates/intent-services/src/agent_manager/tests.rs`.
- **WSS e2e** — every feature needs an end-to-end test that drives the real WSS transport
  (`crates/intentd/tests/`, mock provider fixture
  `crates/intentd/tests/fixtures/mock-acp-agent.mjs`); see `AGENTS.md` in the repo root
  for the harness and requirements.

Gates before any PR: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`
(from the monorepo root: `make check` and `make test`).

## 8. Smoke test (dogfooding)

From the monorepo root, run the stack end-to-end with the new provider:

```bash
make dev            # FE with intentd as a sidecar; or: make dev-daemon + make run-fe
```

Then, in a fresh workspace, spawn an agent on the new provider and verify:

1. **Spawn + auth** — the agent comes up; if unauthenticated, the auth error surfaces
   with the provider's `login_command_hint` / `login_docs_url` rather than a hang.
2. **Title set** — the workspace gets a real title on the first turn (proves the naming
   nudge fired *and* the workspace-MCP bridge is reachable end-to-end).
3. **Workspace tools callable** — ask the agent to list its tools (note the exact
   workspace-MCP tool spellings for §4) and to read/update a note.
4. **Tool rendering** — provider-native tool calls render with sensible names and kinds
   in the FE timeline, not raw provider titles (§5).
5. **System prompt honored** — the agent observably follows a workspace rule (§2).
6. Check the agent stderr logs (`<data-dir>/agent-logs/<agent-id>/`) for warnings.

## Known follow-ups / gotchas

- **claude-code / codex / droid / grok naming-nudge spelling** — these providers receive the
  workspace bridge via `session/new` `mcpServers` (§3c), but their exact MCP tool
  spellings have not been empirically captured yet, so
  `workspace_naming_tool_reference` intentionally uses the generic phrasing for them.
  Capture the spellings from a live session and add match arms (§4).
- **`UNIFIED_WORKSPACE_TOOLS` spellings** — the list in
  `crates/intent-acp/src/tool_restrictions.rs` currently carries only the bare name and
  auggie's *suffixed* spelling (`workspace_api_workspace-mcp`). Other affixing
  conventions (e.g. opencode's prefixed `workspace-mcp_workspace_api`) are not listed;
  extend it when tightening restrictions for prefix-style providers.
- **No workspace-MCP delivery = no workspace tools** — a provider with none of the three
  §3 paths (today: cortex) spawns fine but gets no workspace tools, which also
  means no naming nudge can succeed. Prefer wiring §3c if the provider accepts
  `session/new` `mcpServers`.

## Terminal spawn (`terminal_requires_shell`)

Most providers send ACP `terminal/create` as real argv (`command` = program,
`args` = argv[1..]). Set `terminal_requires_shell: true` only when the provider
packs a full shell line into `command` with empty `args` (Node `shell: true`
semantics). Today that is **Grok Build** only (`/bin/bash -lc '…'` in
`command`). intentd then spawns the packed line exactly as Node `shell: true`
would — `/bin/sh -c` on POSIX, the native shell (PowerShell `-Command` /
`cmd /c`) on Windows — instead of exec'ing the packed string as argv[0].

## Antigravity setup and compatibility boundary

Antigravity uses the official native ACP server, not `agy` and not an npm adapter.
The initial verified target is macOS Apple Silicon with personal Google OAuth.
The tested upstream build is `agy_acp_server_20260818_01_RC01`.

1. Get the archive from the [official ACP registry](https://github.com/agentclientprotocol/registry/blob/a3d294f480dee2e506a1c51f802455d4d49783a2/antigravity-acp/agent.json).
   Keep `agy_acp_server.par` and `localharness_external` together.
2. Set `providers.paths.antigravity` to the server executable, or provide an
   `antigravity-acp` launcher that starts it. Intent does not install or update this archive.
3. On the daemon host, run `intentd provider login antigravity`.
   Use `--path /absolute/path/to/agy_acp_server.par` for an explicit login-path override.
   Follow the official Google authorization URL printed in that terminal.
   The command verifies a fresh guarded session after authentication.
4. Refresh provider status and enable Antigravity explicitly in Settings.
   Refresh the model list before selecting a model.

The [official setup guide](https://antigravity.google/docs/ide/extensions/zed)
describes the upstream authentication choices. Intent currently uses only personal OAuth.
Credentials stay in the official macOS Keychain entry. Intent never copies tokens into profiles.
Normal sessions and discovery probes cannot open a browser. Unknown authentication stays unknown.

`intent-services/src/antigravity.rs` owns profile isolation and the native tool guard.
Profiles use private permissions and persist under `<data-dir>/antigravity/` by workspace and agent identity.
They contain conversation state, not token copies. They survive daemon restarts and are not temporary configuration files.
Global Gemini MCP settings and hooks are excluded, without changing those global files.

The guard permits only known native tools and the caller-filtered workspace tool set.
Native subagent aliases are always denied. Permission mode stays `default`. Unrestricted Intent permissions do not select `yolo`.
User MCP tools require an exact schema under the permitted server in the current private conversation:
`antigravity-acp/brain/<conversationId>/mcp/<server>/<tool>.json`.
This is an observed upstream layout, not a public contract. A missing or changed schema denies the tool.
Recheck this behavior before certifying another server build. These hooks are provider policy controls, not an OS sandbox.

Cold load can reset the upstream model. Intent reapplies the exact saved ID and requires confirmation before sending a prompt.
Model rejection is an actionable setup error, never silent fallback.
This ACP build does not emit usage events. Do not infer usage from stderr or display missing accounting as measured zero.
One-shot quick actions and the generic onboarding test prompt are not enabled for this provider.

To roll back, disable Antigravity and select another provider. Do not delete global credentials or configuration.
Private conversation profiles remain under the daemon data directory. If their history is no longer needed, remove the profiles.
Backend protocol support must land before frontend support. Do not create a manual submodule-pin bump.
After authorized merges, monitor the carrying cloudlands-fe alpha release before calling the work shipped.
