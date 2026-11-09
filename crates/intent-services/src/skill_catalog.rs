//! The harness 3.10 skills block: skills that cannot work in this agent are
//! hidden (with a reason and a fix), and the rest are listed within a
//! character budget.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::skills::SkillMetadata;

/// Maximum characters of one skill description in the block.
pub(crate) const DESCRIPTION_CAP: usize = 250;
/// Target size of the whole block; the lowest-priority skills overflow to a
/// names-only line past it.
pub(crate) const BLOCK_BUDGET: usize = 4000;
/// Intent's own MCP server, delivered to every agent.
pub(crate) const WORKSPACE_MCP_SERVER: &str = "workspace-mcp";

/// How Intent delivers one MCP server to new agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum McpServerState {
    Enabled,
    /// The server is configured in Intent but switched off.
    Disabled,
    /// The server is configured, but user MCP servers are turned off.
    UserServersOff,
}

/// What decides which skills an agent can use.
#[derive(Debug, Clone, Default)]
pub(crate) struct SkillEnvironment {
    /// The environments the agent runs in: Intent and its provider.
    names: Vec<String>,
    /// The MCP servers Intent knows, keyed by their `mcp__<name>__` tool
    /// prefix. `None` when unknown: no skill is hidden for its MCP servers.
    mcp_servers: Option<BTreeMap<String, McpServerState>>,
}

impl SkillEnvironment {
    pub(crate) fn new(
        provider_id: &str,
        mcp_servers: Option<BTreeMap<String, McpServerState>>,
    ) -> Self {
        let mut names = vec!["Intent".to_string()];
        if let Some(provider) = intent_providers::config::find_provider_or_legacy_alias(provider_id)
        {
            names.extend(
                [provider.display_name, provider.short_name, provider.id].map(str::to_string),
            );
        } else if !provider_id.is_empty() {
            names.push(provider_id.to_string());
        }
        let mcp_servers = mcp_servers.map(|mut servers| {
            servers.insert(WORKSPACE_MCP_SERVER.to_string(), McpServerState::Enabled);
            servers
        });
        Self { names, mcp_servers }
    }

    /// Marks servers the provider loads from its own config as enabled: the
    /// agent gets their tools whatever Intent's settings say.
    pub(crate) fn add_provider_servers(&mut self, names: impl IntoIterator<Item = String>) {
        if let Some(servers) = self.mcp_servers.as_mut() {
            for name in names {
                servers.insert(tool_prefix_name(&name), McpServerState::Enabled);
            }
        }
    }
}

/// The MCP servers a provider loads from its own config for an agent in
/// `cwd`; `config_file` comes from the agent's launch environment
/// (`InstalledContext::mcp_config_file`). Claude Code plugin servers are not
/// read.
pub(crate) fn provider_mcp_servers(
    cli: intent_providers::installed_cli::InstalledCli,
    config_file: Option<&std::path::Path>,
    cwd: &std::path::Path,
) -> Vec<String> {
    use intent_providers::installed_cli::InstalledCli;
    let config = config_file.and_then(|path| std::fs::read_to_string(path).ok());
    match cli {
        InstalledCli::Claude => claude_mcp_server_names(
            config.as_deref(),
            std::fs::read_to_string(cwd.join(".mcp.json"))
                .ok()
                .as_deref(),
            cwd,
        ),
        InstalledCli::Codex => config.map_or_else(Vec::new, |text| codex_mcp_server_names(&text)),
    }
}

/// Server names from Claude Code's `.claude.json` (user scope and the
/// `cwd` project) and the repository's `.mcp.json`.
fn claude_mcp_server_names(
    claude_json: Option<&str>,
    repo_mcp_json: Option<&str>,
    cwd: &std::path::Path,
) -> Vec<String> {
    let parse = |text: Option<&str>| {
        text.and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
    };
    let keys = |servers: Option<&serde_json::Value>| -> Vec<String> {
        servers
            .and_then(serde_json::Value::as_object)
            .map_or_else(Vec::new, |servers| {
                servers.keys().map(|name| tool_prefix_name(name)).collect()
            })
    };
    let mut names = Vec::new();
    if let Some(config) = parse(claude_json) {
        names.extend(keys(config.get("mcpServers")));
        let project = config
            .get("projects")
            .and_then(|projects| projects.get(cwd.to_string_lossy().as_ref()));
        names.extend(keys(project.and_then(|project| project.get("mcpServers"))));
    }
    if let Some(config) = parse(repo_mcp_json) {
        names.extend(keys(config.get("mcpServers")));
    }
    names
}

/// Enabled server names from Codex's `config.toml` `[mcp_servers.<name>]`.
fn codex_mcp_server_names(config: &str) -> Vec<String> {
    let Ok(config) = config.parse::<toml_edit::DocumentMut>() else {
        return Vec::new();
    };
    let Some(servers) = config
        .get("mcp_servers")
        .and_then(toml_edit::Item::as_table_like)
    else {
        return Vec::new();
    };
    servers
        .iter()
        .filter(|(_, server)| {
            server.get("enabled").and_then(toml_edit::Item::as_bool) != Some(false)
        })
        .map(|(name, _)| tool_prefix_name(name))
        .collect()
}

/// The MCP servers Intent delivers to a new agent, from the `mcp.servers`
/// configs (keyed by id) and the MCP settings; mirrors the spawn-time merge.
pub(crate) fn intent_mcp_servers(
    settings: &intent_core::settings_file::SettingsFile,
    configs: &serde_json::Map<String, serde_json::Value>,
) -> BTreeMap<String, McpServerState> {
    let user_servers = crate::mcp_servers::enable_user_servers(settings);
    let disabled = crate::mcp_servers::disabled_servers(settings);
    let mut servers = BTreeMap::new();
    for (id, config) in configs {
        let Some(config) = config.as_object() else {
            continue;
        };
        let name = config
            .get("name")
            .and_then(serde_json::Value::as_str)
            .filter(|name| !name.is_empty())
            .unwrap_or(id);
        if name == WORKSPACE_MCP_SERVER || !has_endpoint(config) {
            continue;
        }
        let state = if !user_servers {
            McpServerState::UserServersOff
        } else if config
            .get("enabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
            && !disabled.contains(id)
        {
            McpServerState::Enabled
        } else {
            McpServerState::Disabled
        };
        let slot = servers.entry(tool_prefix_name(name)).or_insert(state);
        if state == McpServerState::Enabled {
            *slot = state;
        }
    }
    servers
}

/// Whether spawn keeps the entry: http/sse need a `url`, stdio a `command`.
fn has_endpoint(config: &serde_json::Map<String, serde_json::Value>) -> bool {
    let field = match config.get("transport").and_then(serde_json::Value::as_str) {
        Some("http" | "sse") => "url",
        _ => "command",
    };
    config
        .get(field)
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.is_empty())
}

/// A server name as it appears in `mcp__<name>__` tool names.
fn tool_prefix_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub(crate) use intent_core::prompt_breakdown::{HiddenSkill, HiddenSkillReason};

/// The skills block with the skills it left out.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SkillsBlock {
    pub text: String,
    pub hidden: Vec<HiddenSkill>,
}

/// Agent products a `compatibility` field may name. A field that names none
/// of them (e.g. "Requires python3") does not hide the skill.
const AGENT_PRODUCTS: &[&str] = &[
    "Intent",
    "Claude Code",
    "Claude desktop",
    "claude.ai",
    "Cowork",
    "Codex",
    "Cursor",
    "Augment",
    "Auggie",
    "OpenCode",
    "Droid",
    "Grok",
    "Gemini CLI",
    "Copilot",
    "Windsurf",
    "Cline",
    "Antigravity",
];

/// Builds the 3.10 block: hides the skills this agent cannot use, then lists
/// the rest within [`BLOCK_BUDGET`].
pub(crate) fn build(skills: &[SkillMetadata], environment: &SkillEnvironment) -> SkillsBlock {
    let mut shown = Vec::new();
    let mut hidden = Vec::new();
    for skill in skills {
        match hidden_reason(skill, environment) {
            Some(reason) => hidden.push(reason),
            None => shown.push(skill),
        }
    }
    SkillsBlock {
        text: render(&shown, BLOCK_BUDGET),
        hidden,
    }
}

/// Why `skill` cannot work in `environment`, or `None` when it can.
pub(crate) fn hidden_reason(
    skill: &SkillMetadata,
    environment: &SkillEnvironment,
) -> Option<HiddenSkill> {
    if let Some(compatibility) = skill.compatibility.as_deref() {
        let names_product = AGENT_PRODUCTS
            .iter()
            .copied()
            .chain(provider_names())
            .any(|product| names(compatibility, product));
        let names_ours = environment
            .names
            .iter()
            .any(|ours| names(compatibility, ours));
        if names_product && !names_ours {
            return Some(HiddenSkill {
                name: skill.name.clone(),
                reason: HiddenSkillReason::Compatibility,
                detail: format!("compatibility: {}", cap_description(compatibility, 160)),
                fix: format!(
                    "Runs only where its compatibility field says; use it there, or edit that \
                     field in {}",
                    skill.location
                ),
            });
        }
    }
    let servers = environment.mcp_servers.as_ref()?;
    let needed: Vec<&str> = skill.mcp_servers.iter().map(String::as_str).collect();
    if needed.is_empty()
        || needed
            .iter()
            .any(|server| servers.get(*server) == Some(&McpServerState::Enabled))
    {
        return None;
    }
    let detail = format!(
        "needs {}",
        needed
            .iter()
            .map(|server| format!("mcp__{server}__"))
            .collect::<Vec<_>>()
            .join(" or ")
    );
    let configured: Vec<(&str, McpServerState)> = needed
        .iter()
        .filter_map(|server| servers.get(*server).map(|state| (*server, *state)))
        .collect();
    let (reason, fix) = match configured.first() {
        Some((_, McpServerState::UserServersOff)) => (
            HiddenSkillReason::McpServerDisabled,
            "Turn on user MCP servers in Intent's MCP settings".to_string(),
        ),
        Some((server, _)) => (
            HiddenSkillReason::McpServerDisabled,
            format!("Turn on the {server} MCP server in Intent's MCP settings"),
        ),
        None => (
            HiddenSkillReason::McpServerMissing,
            format!(
                "Add the {} MCP server in Intent's MCP settings",
                needed.join(" or ")
            ),
        ),
    };
    Some(HiddenSkill {
        name: skill.name.clone(),
        reason,
        detail,
        fix,
    })
}

fn provider_names() -> impl Iterator<Item = &'static str> {
    intent_providers::config::ACP_PROVIDERS
        .iter()
        .filter(|provider| provider.id != "mock")
        .flat_map(|provider| [provider.display_name, provider.short_name])
        .filter(|name| name.len() >= 3)
}

/// Whether `text` names `product` as whole words, ignoring case.
fn names(text: &str, product: &str) -> bool {
    let (text, product) = (text.to_lowercase(), product.to_lowercase());
    if product.is_empty() {
        return false;
    }
    let is_word = |c: Option<char>| c.is_some_and(char::is_alphanumeric);
    text.match_indices(&product).any(|(at, _)| {
        !is_word(text[..at].chars().next_back())
            && !is_word(text[at + product.len()..].chars().next())
    })
}

/// `text` on one line, cut to at most `cap` characters: at the last sentence
/// end that fits, else at a word with an ellipsis.
pub(crate) fn cap_description(text: &str, cap: usize) -> String {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if text.chars().count() <= cap {
        return text;
    }
    let limit = text
        .char_indices()
        .nth(cap)
        .map_or(text.len(), |(at, _)| at);
    let head = &text[..limit];
    let sentence_end = head
        .char_indices()
        .filter(|(at, c)| {
            matches!(c, '.' | '!' | '?') && text[at + c.len_utf8()..].starts_with(' ')
        })
        .map(|(at, c)| at + c.len_utf8())
        .next_back();
    if let Some(end) = sentence_end.filter(|end| *end >= limit / 3) {
        return text[..end].to_string();
    }
    let word_limit = text
        .char_indices()
        .nth(cap - 1)
        .map_or(text.len(), |(at, _)| at);
    let cut = text[..word_limit].rfind(' ').unwrap_or(word_limit);
    format!("{}…", text[..cut].trim_end_matches([',', ';', ':', ' ']))
}

/// One listed skill: its root, its path relative to it (when the directory
/// name differs from the skill name), and its full entry line.
struct Entry<'a> {
    skill: &'a SkillMetadata,
    root: String,
    relative: Option<String>,
    line: String,
    names_only: bool,
}

fn entry(skill: &SkillMetadata) -> Entry<'_> {
    let location = std::path::Path::new(&skill.location);
    let directory = location.parent();
    let root = directory
        .and_then(std::path::Path::parent)
        .map_or_else(String::new, |root| root.to_string_lossy().into_owned());
    let relative = location
        .strip_prefix(&root)
        .ok()
        .map(|path| path.to_string_lossy().into_owned())
        .filter(|path| *path != format!("{}/SKILL.md", skill.name))
        .or_else(|| root.is_empty().then(|| skill.location.clone()));
    let mut line = format!("- {}", escape(&skill.name));
    if let Some(relative) = &relative {
        let _ = write!(line, " ({})", escape(relative));
    }
    line.push_str(": ");
    line.push_str(&escape(&cap_description(
        &skill.description,
        DESCRIPTION_CAP,
    )));
    if let Some(resources) = &skill.resource_directory {
        let _ = write!(line, " [resources: {}]", escape(resources));
    }
    Entry {
        skill,
        root,
        relative,
        line,
        names_only: false,
    }
}

const INTRO: &str = "The following skills provide specialized instructions for specific tasks.\n\
When a task matches a skill's description, read its SKILL.md with your file-read\n\
tool before proceeding. Each skill is at <root>/<name>/SKILL.md unless a path is\n\
shown; resolve its relative paths against that directory (or its listed resources)\n\
and use absolute paths in tool calls.";

/// The listing for `skills` in priority order: project skills first, then
/// user skills, each in discovery order. Past `budget` characters the lowest
/// priority skills keep only their names.
fn render(skills: &[&SkillMetadata], budget: usize) -> String {
    if skills.is_empty() {
        return String::new();
    }
    let mut entries: Vec<Entry<'_>> = skills
        .iter()
        .filter(|skill| skill.scope == "project")
        .chain(skills.iter().filter(|skill| skill.scope != "project"))
        .map(|skill| entry(skill))
        .collect();
    let mut text = layout(&entries);
    for at in (0..entries.len()).rev() {
        if text.chars().count() <= budget {
            break;
        }
        entries[at].names_only = true;
        text = layout(&entries);
    }
    text
}

fn layout(entries: &[Entry<'_>]) -> String {
    let mut roots: Vec<&str> = Vec::new();
    for entry in entries {
        if !roots.contains(&entry.root.as_str()) {
            roots.push(&entry.root);
        }
    }
    let mut out = String::from(INTRO);
    out.push('\n');
    for root in roots {
        let group = entries.iter().filter(|entry| entry.root == root);
        let _ = writeln!(out, "\n<available_skills root=\"{}\">", escape(root));
        for entry in group.clone().filter(|entry| !entry.names_only) {
            out.push_str(&entry.line);
            out.push('\n');
        }
        let names: Vec<String> = group
            .filter(|entry| entry.names_only)
            .map(|entry| match &entry.relative {
                Some(relative) => format!("{} ({})", escape(&entry.skill.name), escape(relative)),
                None => escape(&entry.skill.name),
            })
            .collect();
        if !names.is_empty() {
            let _ = writeln!(
                out,
                "- Also (read SKILL.md for when to use): {}",
                names.join(", ")
            );
        }
        out.push_str("</available_skills>\n");
    }
    out.truncate(out.trim_end().len());
    out
}

fn escape(text: &str) -> String {
    crate::skills::escape_xml(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(home: &std::path::Path, name: &str, frontmatter: &str, body: &str) {
        let dir = home.join(".claude/skills").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("SKILL.md"),
            format!(
                "---\nname: {name}\ndescription: Does {name} things.\n{frontmatter}---\n\n{body}\n"
            ),
        )
        .unwrap();
    }

    fn discover(home: &std::path::Path) -> Vec<SkillMetadata> {
        crate::skills::discover_skills_sync(None, Some(home.to_path_buf()))
    }

    /// Known MCP servers; `None` (unknown) is passed directly.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "reads as the `mcp_servers` argument"
    )]
    fn servers(entries: &[(&str, McpServerState)]) -> Option<BTreeMap<String, McpServerState>> {
        Some(
            entries
                .iter()
                .map(|(name, state)| ((*name).to_string(), *state))
                .collect(),
        )
    }

    fn hidden_names(block: &SkillsBlock) -> Vec<(&str, HiddenSkillReason)> {
        block
            .hidden
            .iter()
            .map(|skill| (skill.name.as_str(), skill.reason))
            .collect()
    }

    #[test]
    fn compatibility_naming_only_other_products_hides_the_skill() {
        let home = tempfile::tempdir().unwrap();
        write(
            home.path(),
            "desktop-only",
            "compatibility: \"Cowork in the Claude desktop app\"\n",
            "",
        );
        write(
            home.path(),
            "claude-code-too",
            "compatibility: \"Claude Code and Cowork\"\n",
            "",
        );
        write(
            home.path(),
            "needs-python",
            "compatibility: \"Requires python3 and network access\"\n",
            "",
        );
        let skills = discover(home.path());
        let claude = build(&skills, &SkillEnvironment::new("claude-code", servers(&[])));
        assert_eq!(
            hidden_names(&claude),
            [("desktop-only", HiddenSkillReason::Compatibility)]
        );
        assert!(
            claude.text.contains("- claude-code-too: "),
            "{}",
            claude.text
        );
        assert!(claude.text.contains("- needs-python: "), "{}", claude.text);
        let codex = build(&skills, &SkillEnvironment::new("codex", servers(&[])));
        assert_eq!(
            hidden_names(&codex),
            [
                ("claude-code-too", HiddenSkillReason::Compatibility),
                ("desktop-only", HiddenSkillReason::Compatibility),
            ]
        );
        assert!(codex.hidden[1]
            .detail
            .contains("Cowork in the Claude desktop app"));
    }

    #[test]
    fn mcp_server_not_configured_in_intent_hides_the_skill() {
        let home = tempfile::tempdir().unwrap();
        write(
            home.path(),
            "import-memory",
            "",
            "Call mcp__memory__create_entities.",
        );
        let block = build(
            &discover(home.path()),
            &SkillEnvironment::new(
                "claude-code",
                servers(&[("github", McpServerState::Enabled)]),
            ),
        );
        assert_eq!(
            block.hidden,
            [HiddenSkill {
                name: "import-memory".into(),
                reason: HiddenSkillReason::McpServerMissing,
                detail: "needs mcp__memory__".into(),
                fix: "Add the memory MCP server in Intent's MCP settings".into(),
            }]
        );
        assert_eq!(block.text, "");
    }

    #[test]
    fn mcp_server_configured_and_enabled_shows_the_skill() {
        let home = tempfile::tempdir().unwrap();
        write(
            home.path(),
            "import-memory",
            "",
            "Call mcp__memory__create_entities.",
        );
        write(
            home.path(),
            "browser",
            "",
            "Use mcp__claude-in-chrome__navigate or mcp__Claude_Browser__open.",
        );
        write(
            home.path(),
            "ws-only",
            "",
            "Call mcp__workspace-mcp__workspace_api.",
        );
        let block = build(
            &discover(home.path()),
            &SkillEnvironment::new(
                "claude-code",
                servers(&[
                    ("memory", McpServerState::Enabled),
                    ("claude-in-chrome", McpServerState::Enabled),
                ]),
            ),
        );
        assert_eq!(block.hidden, []);
        for name in ["import-memory", "browser", "ws-only"] {
            assert!(
                block.text.contains(&format!("- {name}: ")),
                "{}",
                block.text
            );
        }
    }

    #[test]
    fn mcp_server_configured_but_disabled_hides_the_skill() {
        let home = tempfile::tempdir().unwrap();
        write(
            home.path(),
            "import-memory",
            "",
            "Call mcp__memory__create_entities.",
        );
        let skills = discover(home.path());
        let block = build(
            &skills,
            &SkillEnvironment::new("codex", servers(&[("memory", McpServerState::Disabled)])),
        );
        assert_eq!(
            hidden_names(&block),
            [("import-memory", HiddenSkillReason::McpServerDisabled)]
        );
        assert_eq!(
            block.hidden[0].fix,
            "Turn on the memory MCP server in Intent's MCP settings"
        );
        let off = build(
            &skills,
            &SkillEnvironment::new(
                "codex",
                servers(&[("memory", McpServerState::UserServersOff)]),
            ),
        );
        assert_eq!(
            off.hidden[0].fix,
            "Turn on user MCP servers in Intent's MCP settings"
        );
    }

    #[test]
    fn skill_without_compatibility_or_mcp_references_is_shown() {
        let home = tempfile::tempdir().unwrap();
        write(
            home.path(),
            "pdf",
            "",
            "Use pypdf. Not an mcp__<server>__ tool.",
        );
        let skills = discover(home.path());
        for environment in [
            SkillEnvironment::new("claude-code", servers(&[])),
            SkillEnvironment::new("codex", None),
        ] {
            let block = build(&skills, &environment);
            assert_eq!(block.hidden, []);
            assert!(
                block.text.contains("- pdf: Does pdf things."),
                "{}",
                block.text
            );
        }
    }

    #[test]
    fn unknown_mcp_servers_hide_nothing() {
        let home = tempfile::tempdir().unwrap();
        write(
            home.path(),
            "import-memory",
            "",
            "Call mcp__memory__create_entities.",
        );
        let block = build(
            &discover(home.path()),
            &SkillEnvironment::new("codex", None),
        );
        assert_eq!(block.hidden, []);
    }

    #[test]
    fn intent_mcp_servers_mirror_the_spawn_merge() {
        let mut settings = intent_core::settings_file::SettingsFile::default();
        settings.mcp.disabled_servers = vec!["off-by-list".into()];
        let configs = serde_json::json!({
            "a": { "name": "memory", "enabled": true, "command": "x" },
            "b": { "name": "github", "enabled": false, "command": "x" },
            "off-by-list": { "enabled": true, "command": "x" },
            "c": { "name": "My Server", "enabled": true, "transport": "sse", "url": "u" },
        });
        let configs = configs.as_object().unwrap();
        assert_eq!(
            intent_mcp_servers(&settings, configs),
            BTreeMap::from([
                ("memory".into(), McpServerState::Enabled),
                ("github".into(), McpServerState::Disabled),
                ("off-by-list".into(), McpServerState::Disabled),
                ("My_Server".into(), McpServerState::Enabled),
            ])
        );
        settings.mcp.enable_user_servers = false;
        assert_eq!(
            intent_mcp_servers(&settings, configs)["memory"],
            McpServerState::UserServersOff
        );
    }

    #[test]
    fn intent_mcp_servers_skip_entries_spawn_drops() {
        let settings = intent_core::settings_file::SettingsFile::default();
        let configs = serde_json::json!({
            "a": { "name": "no-command", "enabled": true, "transport": "stdio" },
            "b": { "name": "no-url", "enabled": true, "transport": "http" },
            "c": { "name": "workspace-mcp", "enabled": false, "command": "x" },
        });
        let servers = intent_mcp_servers(&settings, configs.as_object().unwrap());
        assert_eq!(servers.get("no-command"), None);
        assert_eq!(servers.get("no-url"), None);
        assert_eq!(servers.get(WORKSPACE_MCP_SERVER), None);
    }

    #[test]
    fn a_server_the_provider_configures_shows_the_skill() {
        let home = tempfile::tempdir().unwrap();
        write(home.path(), "tracker", "", "Use mcp__linear__create_issue.");
        write(home.path(), "switched-off", "", "Use mcp__github__search.");
        let mut environment = SkillEnvironment::new(
            "claude-code",
            servers(&[("github", McpServerState::Disabled)]),
        );
        environment.add_provider_servers(["linear".to_string(), "github".to_string()]);
        let block = build(&discover(home.path()), &environment);
        assert_eq!(block.hidden, []);
    }

    #[test]
    fn claude_mcp_servers_come_from_user_project_and_repo_configs() {
        let claude_json = serde_json::json!({
            "mcpServers": { "linear": {} },
            "projects": {
                "/repo": { "mcpServers": { "sentry": {} } },
                "/other": { "mcpServers": { "elsewhere": {} } },
            },
        })
        .to_string();
        let repo_json = serde_json::json!({ "mcpServers": { "My Docs": {} } }).to_string();
        let mut names = claude_mcp_server_names(
            Some(&claude_json),
            Some(&repo_json),
            std::path::Path::new("/repo"),
        );
        names.sort();
        assert_eq!(names, ["My_Docs", "linear", "sentry"]);
        assert_eq!(
            claude_mcp_server_names(Some("not json"), None, std::path::Path::new("/repo")),
            Vec::<String>::new()
        );
    }

    #[test]
    fn codex_mcp_servers_come_from_its_config_and_skip_disabled_ones() {
        let config = "model = \"x\"\n[mcp_servers.linear]\ncommand = \"npx\"\n\
                      [mcp_servers.off]\ncommand = \"y\"\nenabled = false\n";
        assert_eq!(codex_mcp_server_names(config), ["linear"]);
        assert_eq!(codex_mcp_server_names("[[broken"), Vec::<String>::new());
    }

    #[test]
    fn descriptions_are_cut_at_a_sentence_within_the_cap() {
        let first = format!("First {}sentence.", "long ".repeat(20));
        let long = format!("{first} Second sentence. {}", "word ".repeat(80));
        assert_eq!(
            cap_description(&long, 250),
            format!("{first} Second sentence.")
        );
        let one = "word ".repeat(80);
        let cut = cap_description(&one, 250);
        assert!(
            cut.ends_with("word…") && cut.chars().count() <= 250,
            "{cut}"
        );
        assert_eq!(cap_description("Short\n  text.", 250), "Short text.");
    }

    #[test]
    fn the_block_states_each_root_once_and_overflows_to_names() {
        let skill = |name: &str, dir: &str, scope: &str| SkillMetadata {
            name: name.into(),
            description: format!("{name} {}", "detail ".repeat(60)),
            location: format!("/root/{scope}/{dir}/SKILL.md"),
            resource_directory: None,
            scope: scope.into(),
            allowed_tools: None,
            compatibility: None,
            mcp_servers: Vec::new(),
        };
        let mut skills: Vec<SkillMetadata> = (0..30)
            .map(|i| skill(&format!("user-{i}"), &format!("user-{i}"), "user"))
            .collect();
        skills.push(skill("odd", "other-dir", "project"));
        let block = build(&skills, &SkillEnvironment::new("codex", None));
        let text = &block.text;
        assert!(
            text.chars().count() <= BLOCK_BUDGET,
            "{}",
            text.chars().count()
        );
        assert_eq!(
            text.matches("<available_skills root=\"/root/user\">")
                .count(),
            1
        );
        assert!(text
            .contains("<available_skills root=\"/root/project\">\n- odd (other-dir/SKILL.md): "));
        assert!(!text.contains("/root/user/user-"), "{text}");
        assert!(text.contains("- user-0: "), "{text}");
        assert!(text.contains("read SKILL.md for when to use): "), "{text}");
        assert!(text.ends_with("user-29\n</available_skills>"), "{text}");
        for i in 0..30 {
            assert!(text.contains(&format!("user-{i}")), "user-{i}");
        }
    }

    /// The developer's real skills meet the block target. Run with
    /// `INTENTD_SKILLS_CHECK_HOME=$HOME` (and the project dir in
    /// `INTENTD_SKILLS_CHECK_WORKSPACE`).
    #[test]
    #[ignore = "reads the developer's skills"]
    fn developer_skills_fit_the_budget() {
        let home = std::env::var_os("INTENTD_SKILLS_CHECK_HOME").expect("set the home");
        let workspace = std::env::var_os("INTENTD_SKILLS_CHECK_WORKSPACE");
        let skills = crate::skills::discover_skills_sync(
            workspace.as_deref().map(std::path::Path::new),
            Some(home.into()),
        );
        let old = crate::skills::build_skills_catalog(&skills);
        let block = build(&skills, &SkillEnvironment::new("claude-code", servers(&[])));
        println!("{}\n", block.text);
        println!("{:#?}", block.hidden);
        println!(
            "skills {} hidden {} old {} chars new {} chars",
            skills.len(),
            block.hidden.len(),
            old.chars().count(),
            block.text.chars().count()
        );
        assert!(block.text.chars().count() <= BLOCK_BUDGET);
    }
}
