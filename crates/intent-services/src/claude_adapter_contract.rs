//! Real-adapter contract for claude-code utility completions.
//!
//! [`one_shot_session_shape`] relies on the pinned
//! `@agentclientprotocol/claude-agent-acp` adapter (and the Claude Agent SDK it
//! bundles) to turn the utility session's `_meta` into specific Claude CLI
//! behaviour: no built-in tools, only the `user` settings tier, strict MCP
//! mode, and a system prompt that replaces the `claude_code` preset. The pin
//! moves automatically (`auto-pin-acp-adapters.yml`), so this test runs the
//! exact pinned adapter for real and fails before merge if any of that stops
//! holding.
//!
//! The adapter is started through Intent's own one-shot runner with the same
//! `_meta` production sends. The Claude CLI it would spawn is replaced (via the
//! adapter's `CLAUDE_CODE_EXECUTABLE`) by `tests/fixtures/fake_claude_cli.mjs`,
//! which records the argv and stream-json messages the SDK hands it, so no
//! network access or Claude account is needed.
//!
//! The adapter comes from `scripts/prepare-claude-adapter-pin.py`, whose
//! output directory goes in `INTENT_CLAUDE_ADAPTER_PIN_FIXTURE`. Without it the
//! tests skip (keeping ordinary runs offline) unless
//! `INTENT_REQUIRE_CLAUDE_ADAPTER_CONTRACT=1` is set, as CI's `check` job does:
//!
//! ```sh
//! INTENT_CLAUDE_ADAPTER_PIN_FIXTURE=$(python3 -I -B -S scripts/prepare-claude-adapter-pin.py \
//!   --cache-dir /tmp/claude-adapter-pin) \
//!   cargo test -p intent-services --lib claude_adapter_contract::
//! ```

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;

use crate::complete_ops::one_shot_session_shape;
use crate::one_shot_acp::{run_one_shot_acp_in, OneShotCommand, OneShotEffort};
use crate::test_support::test_tempdir;

const FIXTURE_ENV: &str = "INTENT_CLAUDE_ADAPTER_PIN_FIXTURE";
const REQUIRE_ENV: &str = "INTENT_REQUIRE_CLAUDE_ADAPTER_CONTRACT";
const FAKE_CLI: &str = include_str!("../tests/fixtures/fake_claude_cli.mjs");
const REPLY: &str = "contract-reply-7d1c";
const TIMEOUT: Duration = Duration::from_secs(60);
/// Per-stage `initialize` / `session/new` budget for the real adapter. The
/// production 4s `initialize` limit is right for users but not for CI: on the
/// hosted `gh-linux-8x` runners these four tests cold-start Node together and
/// the passing runs already took 3.4–4.5s, so roughly one run in six timed out
/// on `initialize` (intentd merge-group run 39386342220). The budget only
/// bounds a hang; it changes nothing the contract asserts.
const STARTUP_BUDGET: Duration = Duration::from_secs(30);
const MISSING: &str = "<missing value>";
/// Inherited variables the adapter, SDK or CLI read; cleared so the host's
/// own Claude/Anthropic configuration cannot change what the contract sees.
const CLEARED_ENV_PREFIXES: &[&str] = &["ANTHROPIC_", "CLAUDE_", "CLAUDECODE", "AWS_", "VERTEX_"];

/// The provisioned adapter package directory, or `None` to skip.
fn adapter_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os(FIXTURE_ENV) {
        return Some(PathBuf::from(dir));
    }
    assert!(
        std::env::var_os(REQUIRE_ENV).is_none(),
        "{REQUIRE_ENV} is set but {FIXTURE_ENV} is not: provision the pinned adapter with \
         scripts/prepare-claude-adapter-pin.py and export its output directory"
    );
    eprintln!("skipping: {FIXTURE_ENV} is not set (see claude_adapter_contract docs)");
    None
}

/// The provisioned adapter's version, which must be exactly the pin.
fn checked_adapter_version(dir: &Path) -> String {
    let manifest: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.join("package.json"))
            .unwrap_or_else(|e| panic!("{FIXTURE_ENV}={} has no package.json: {e}", dir.display())),
    )
    .unwrap();
    assert_eq!(
        manifest["name"], "@agentclientprotocol/claude-agent-acp",
        "{FIXTURE_ENV} does not point at the claude-agent-acp package"
    );
    let version = manifest["version"].as_str().unwrap_or_default().to_owned();
    assert_eq!(
        version,
        intent_providers::CLAUDE_AGENT_ACP_VERSION,
        "{FIXTURE_ENV} holds claude-agent-acp {version}, but Intent pins {}; re-run \
         scripts/prepare-claude-adapter-pin.py",
        intent_providers::CLAUDE_AGENT_ACP_VERSION
    );
    version
}

/// What the stand-in CLI observed during one completion.
struct Observed {
    adapter: String,
    reply: String,
    argv: Vec<String>,
    stdin: Vec<Value>,
    log: String,
}

impl Observed {
    fn context(&self) -> String {
        format!(
            "claude-agent-acp {} (Intent pin {}); stand-in CLI log:\n{}",
            self.adapter,
            intent_providers::CLAUDE_AGENT_ACP_VERSION,
            self.log
        )
    }

    /// Every value of `--name`, accepting both `--name value` and
    /// `--name=value` (adapter/SDK releases have used each spelling). A bare
    /// `--name` with no value (last argument, or followed by another flag)
    /// yields [`MISSING`], never an empty value, so it cannot pass for
    /// `--tools ""`.
    fn flag_values(&self, name: &str) -> Vec<&str> {
        let prefix = format!("{name}=");
        let mut values = Vec::new();
        let mut args = self.argv.iter().peekable();
        while let Some(arg) = args.next() {
            if let Some(value) = arg.strip_prefix(&prefix) {
                values.push(value);
            } else if arg == name {
                match args.peek() {
                    Some(next) if !next.starts_with("--") => {
                        values.push(args.next().unwrap().as_str());
                    }
                    _ => values.push(MISSING),
                }
            }
        }
        values
    }

    /// Whether any argument is `--name`, `--name=…`, or a longer flag
    /// starting with `--name` (`--append-system-prompt-file`, …).
    fn has_flag_family(&self, name: &str) -> bool {
        self.argv.iter().any(|a| a.starts_with(name))
    }

    fn has_flag(&self, name: &str) -> bool {
        let prefix = format!("{name}=");
        self.argv
            .iter()
            .any(|a| a == name || a.starts_with(&prefix))
    }

    /// The SDK's `initialize` control request.
    fn initialize(&self) -> &Value {
        self.stdin
            .iter()
            .find(|m| m["type"] == "control_request" && m["request"]["subtype"] == "initialize")
            .map_or_else(
                || panic!("the SDK never sent `initialize`\n{}", self.context()),
                |m| &m["request"],
            )
    }

    /// The replacement system prompt the CLI received, from whichever channel
    /// the SDK used (`initialize.systemPrompt` as a string or string parts, or
    /// a `--system-prompt` flag). `None` means the `claude_code` preset.
    fn system_prompt(&self) -> Option<String> {
        let mut found = Vec::new();
        match &self.initialize()["systemPrompt"] {
            Value::Null => {}
            Value::String(s) => found.push(s.clone()),
            Value::Array(parts) => found.push(
                parts
                    .iter()
                    .map(|p| {
                        p.as_str().unwrap_or_else(|| {
                            panic!("non-text systemPrompt part {p}\n{}", self.context())
                        })
                    })
                    .collect(),
            ),
            other => panic!(
                "unexpected initialize.systemPrompt {other}\n{}",
                self.context()
            ),
        }
        found.extend(
            self.flag_values("--system-prompt")
                .into_iter()
                .map(str::to_owned),
        );
        assert!(
            found.len() <= 1,
            "the system prompt reached the CLI through more than one channel: {found:?}\n{}",
            self.context()
        );
        found.pop()
    }

    /// Any append-to-preset channel: an `append*SystemPrompt*` field on
    /// `initialize` or an `--append-system-prompt*` flag.
    fn appends_to_preset(&self) -> bool {
        let init = self.initialize().as_object().into_iter().flatten();
        init.into_iter().any(|(key, value)| {
            let key = key.to_ascii_lowercase();
            key.starts_with("append") && key.contains("systemprompt") && !value.is_null()
        }) || self.has_flag_family("--append-system-prompt")
    }

    /// Any MCP server configuration reaching the CLI.
    fn passes_mcp_config(&self) -> bool {
        self.has_flag_family("--mcp-config")
            || self
                .initialize()
                .get("sdkMcpServers")
                .is_some_and(|v| v.as_array().is_some_and(|a| !a.is_empty()))
    }

    fn user_turn_text(&self) -> String {
        let turn = self
            .stdin
            .iter()
            .find(|m| m["type"] == "user")
            .unwrap_or_else(|| panic!("no user turn reached the CLI\n{}", self.context()));
        match &turn["message"]["content"] {
            Value::String(s) => s.clone(),
            Value::Array(blocks) => blocks.iter().filter_map(|b| b["text"].as_str()).collect(),
            other => panic!("unexpected user turn content {other}\n{}", self.context()),
        }
    }
}

/// Run one utility completion through the real adapter, exactly as
/// `complete_ops` does for claude-code, with `meta` as `session/new` `_meta`.
async fn complete(adapter: &Path, prompt: &str, meta: Option<Value>) -> Observed {
    let version = checked_adapter_version(adapter);
    let scratch = test_tempdir("claude-adapter-contract");
    let root = scratch.path();
    let home = root.join("home");
    let project = root.join("project");
    for dir in [&home, &project, &root.join("tmp")] {
        std::fs::create_dir_all(dir).unwrap();
    }
    // `.mjs` so Node loads the ESM stand-in without relying on module-type
    // detection (absent before Node 22.12).
    let fake = root.join("claude.mjs");
    std::fs::write(&fake, FAKE_CLI).unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let log = root.join("cli.ndjson");

    let mut cmd = OneShotCommand::binary(
        PathBuf::from("node"),
        vec![adapter.join("dist/index.js").display().to_string()],
    )
    .cwd(project)
    .setup_budget(STARTUP_BUDGET)
    .env("CLAUDE_CODE_EXECUTABLE", &fake)
    .env("INTENT_FAKE_CLAUDE_LOG", &log)
    .env("INTENT_FAKE_CLAUDE_REPLY", REPLY)
    .env("HOME", &home)
    .env("CLAUDE_CONFIG_DIR", home.join(".claude"))
    .env("XDG_CONFIG_HOME", home.join(".config"))
    .env("TMPDIR", root.join("tmp"))
    .env("NODE_OPTIONS", "")
    .env("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
    .env("DO_NOT_TRACK", "1")
    .env("DD_TRACE_ENABLED", "false")
    .env("DD_INSTRUMENTATION_TELEMETRY_ENABLED", "false");
    // Removals apply after overrides at spawn, so the sweep skips the
    // variables set above.
    let own: Vec<String> = cmd.env_vars().iter().map(|(k, _)| k.clone()).collect();
    for (key, _) in std::env::vars_os() {
        let Some(key) = key.to_str() else { continue };
        if CLEARED_ENV_PREFIXES.iter().any(|p| key.starts_with(p)) && !own.iter().any(|k| k == key)
        {
            cmd = cmd.env_remove(key);
        }
    }

    let result = run_one_shot_acp_in(
        None,
        &crate::acp_adapter::AdapterSlots::new(1),
        cmd,
        prompt,
        None,
        meta,
        TIMEOUT,
        &OneShotEffort::default(),
    )
    .await;
    let text = std::fs::read_to_string(&log).unwrap_or_default();
    let reply = result.unwrap_or_else(|e| {
        panic!("claude-agent-acp {version} failed the utility completion: {e:?}\nstand-in CLI log:\n{text}")
    });
    let entries: Vec<Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    // The session is the CLI spawn with stream-json input; other spawns are
    // probes (`auth status`).
    let argv = entries
        .iter()
        .filter(|e| e["kind"] == "argv")
        .map(|e| {
            e["argv"]
                .as_array()
                .unwrap()
                .iter()
                .map(|a| a.as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .filter(|argv| argv.iter().any(|a| a.starts_with("--input-format")))
        .collect::<Vec<_>>();
    assert_eq!(
        argv.len(),
        1,
        "expected exactly one Claude CLI session spawn from claude-agent-acp {version}\n{text}"
    );
    Observed {
        adapter: version,
        reply,
        argv: argv.into_iter().next().unwrap(),
        stdin: entries
            .iter()
            .filter(|e| e["kind"] == "stdin")
            .map(|e| e["message"].clone())
            .collect(),
        log: text,
    }
}

/// Every expectation documented on [`one_shot_session_shape`].
fn assert_utility_contract(seen: &Observed, prompt: &str, system_prompt: &str) {
    let cx = seen.context();
    assert_eq!(
        seen.reply, REPLY,
        "the completion did not return the CLI's reply\n{cx}"
    );
    assert_eq!(
        seen.flag_values("--tools"),
        [""],
        "built-in tools must be off: `_meta.claudeCode.options.tools: []` should reach the CLI \
         as an empty --tools (the adapter's option spread over its `claude_code` tools default)\n{cx}"
    );
    assert_eq!(
        seen.flag_values("--setting-sources"),
        ["user"],
        "only the user settings tier may load: `_meta.claudeCode.options.settingSources` should \
         override the adapter's user,project,local default\n{cx}"
    );
    assert!(
        seen.has_flag("--strict-mcp-config"),
        "`_meta.claudeCode.options.strictMcpConfig: true` should reach the CLI as \
         --strict-mcp-config\n{cx}"
    );
    assert!(
        !seen.passes_mcp_config(),
        "a utility session sends no MCP servers, so no --mcp-config may be passed\n{cx}"
    );
    assert_eq!(
        seen.system_prompt().as_deref(),
        Some(system_prompt),
        "a string `_meta.systemPrompt` must reach the CLI verbatim as the replacement system \
         prompt (absent means the claude_code preset loaded)\n{cx}"
    );
    assert!(
        !seen.appends_to_preset(),
        "the system prompt must replace the claude_code preset, not append to it\n{cx}"
    );
    assert_eq!(
        seen.user_turn_text(),
        prompt,
        "the turn must carry the prompt alone (the system prompt rides `_meta`)\n{cx}"
    );
}

#[tokio::test]
async fn caller_system_prompt_utility_session_is_applied() {
    let Some(adapter) = adapter_dir() else { return };
    let prompt = "Summarize: contract prompt 41b9";
    let system = "Contract system prompt 9f3e. Reply with the summary only.";
    let (turn, meta) = one_shot_session_shape("claude-code", prompt, Some(system));
    let seen = complete(&adapter, &turn, meta).await;
    assert_utility_contract(&seen, prompt, system);
}

#[tokio::test]
async fn default_utility_system_prompt_is_applied() {
    let Some(adapter) = adapter_dir() else { return };
    let prompt = "Write a commit title: contract prompt 5a21";
    let (turn, meta) = one_shot_session_shape("claude-code", prompt, None);
    let system = meta
        .as_ref()
        .and_then(|m| m["systemPrompt"].as_str())
        .map(str::to_owned);
    let system = system
        .filter(|s| !s.is_empty())
        .expect("Intent always fills the system prompt");
    let seen = complete(&adapter, &turn, meta).await;
    assert_utility_contract(&seen, prompt, &system);
}

/// Control: without `_meta` the adapter's agentic defaults reach the CLI. This
/// proves the stand-in actually observes each setting, so the assertions above
/// cannot pass vacuously.
#[tokio::test]
async fn without_meta_the_adapter_defaults_are_observed() {
    let Some(adapter) = adapter_dir() else { return };
    let seen = complete(&adapter, "control prompt", None).await;
    let cx = seen.context();
    assert_eq!(seen.reply, REPLY, "{cx}");
    assert_ne!(seen.flag_values("--tools"), [""], "{cx}");
    assert_ne!(seen.flag_values("--setting-sources"), ["user"], "{cx}");
    assert!(!seen.has_flag("--strict-mcp-config"), "{cx}");
    assert_eq!(seen.system_prompt(), None, "{cx}");
    assert!(!seen.appends_to_preset(), "{cx}");
}

/// Control: an object `_meta.systemPrompt` asks the adapter to APPEND to the
/// preset. Proves the append detector (asserted absent above) observes that
/// channel on this adapter/SDK.
#[tokio::test]
async fn object_system_prompt_append_is_observed() {
    let Some(adapter) = adapter_dir() else { return };
    let meta = serde_json::json!({ "systemPrompt": { "append": "control append 3c7d" } });
    let seen = complete(&adapter, "control prompt", Some(meta)).await;
    let cx = seen.context();
    assert!(seen.appends_to_preset(), "{cx}");
    assert_eq!(seen.system_prompt(), None, "{cx}");
}

#[test]
fn flag_values_accept_both_spellings() {
    let seen = Observed {
        adapter: String::new(),
        reply: String::new(),
        argv: [
            "--tools",
            "",
            "--setting-sources=user",
            "--strict-mcp-config",
            "--tools=",
        ]
        .map(String::from)
        .to_vec(),
        stdin: Vec::new(),
        log: String::new(),
    };
    assert_eq!(seen.flag_values("--tools"), ["", ""]);
    assert_eq!(seen.flag_values("--setting-sources"), ["user"]);
    assert!(seen.has_flag("--strict-mcp-config"));
    assert!(!seen.has_flag_family("--mcp-config"));
}

#[test]
fn flag_values_never_read_a_missing_value_as_empty() {
    let observed = |argv: &[&str]| Observed {
        adapter: String::new(),
        reply: String::new(),
        argv: argv.iter().map(ToString::to_string).collect(),
        stdin: Vec::new(),
        log: String::new(),
    };
    let last = observed(&["--verbose", "--tools"]);
    assert_eq!(last.flag_values("--tools"), [MISSING]);
    let before_flag = observed(&["--tools", "--setting-sources", "user"]);
    assert_eq!(before_flag.flag_values("--tools"), [MISSING]);
    assert_eq!(before_flag.flag_values("--setting-sources"), ["user"]);
    let file = observed(&["--append-system-prompt-file", "x"]);
    assert!(file.has_flag_family("--append-system-prompt"));
}
