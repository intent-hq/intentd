//! BE-owned prompt **rules** (§18.1, PROTOCOL §5.21): user-rule overrides
//! persisted in the `endUserRules` settings key, the live workspace rule-file
//! loader (ports `rules-loader.ts`), and the **internal** prompt-assembly /
//! injection pipeline (ports `instruction-service.ts` + `formatUserRulesForContext`).
//!
//! `rules.list`/`rules.get` are reads; `rules.update` upserts a user override.
//! Assembling these into an agent's system prompt runs **internally** as agents
//! start (§6.8) — there is no wire method for it. File-sourced entries are
//! read-only over the wire (edit the files directly).

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use intent_core::{Error, Result};
use intent_store::Store;
use serde_json::{json, Map, Value};

use crate::prompt_sections::{AssembledPrompt, LayerTag};
use crate::rtk;
use intent_core::prompt_breakdown::ControlHintKind;

/// Settings-store key the per-rule-type user overrides persist under (§9.12).
const END_USER_RULES_KEY: &str = "endUserRules";

/// Upper bound on a single override body, matching the FE editor cap
/// (`AgentRulesEditor.svelte` `MAX_RULES_LENGTH`).
const MAX_RULE_CONTENT_LEN: usize = 50_000;

/// Current epoch-ms (the wire `updatedAt` unit per PROTOCOL §5.21).
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Wrap user-rule content for prompt injection (port of
/// `formatUserRulesForContext`). Wording owned by the harness (H5); prompt
/// assembly routes through the session's pinned harness directly (H2), so
/// this latest-bound wrapper remains for the golden pins.
#[cfg(test)]
pub(crate) fn format_user_rules_for_context(content: &str, source: &str) -> String {
    crate::harness::latest().user_rules_wrapper(content, source)
}

/// The harness registry row for a session's stamped `harnessVersion` (H2):
/// the pinned harness + doctrine when a session is supplied, else the latest
/// (session-less callers — previews, background one-shots).
fn session_harness_entry(
    agent_session: Option<&intent_core::AgentSession>,
) -> &'static crate::harness::HarnessEntry {
    match agent_session {
        Some(s) => crate::harness::resolve_entry(&s.harness_version),
        None => crate::harness::latest_entry(),
    }
}

/// A workspace rule file resolved off the worktree.
struct WorkspaceRuleFile {
    source: String,
    path: String,
    content: String,
    updated_at: i64,
}

/// File mtime in epoch-ms (0 when unavailable).
fn file_mtime_ms(path: &Path) -> i64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Strip an optional leading YAML frontmatter block, returning the body (port of
/// `parseRuleFile`; the `type` field is unused by the assembled prompt).
fn strip_frontmatter(content: &str) -> &str {
    if let Some(rest) = content.strip_prefix("---") {
        if let Some(end) = rest.find("---") {
            return rest[end + 3..].trim_start();
        }
    }
    content
}

/// Enumerate the workspace rule files that exist, in `rules-loader.ts`
/// precedence: `CLAUDE.md`, `AGENTS.md`, `.intent/guidelines.md`,
/// `.augment/guidelines.md` (auggie convention), then every `.md` under
/// `.intent/rules/` and `.augment/rules/` (sorted). Each becomes a read-only entry.
fn list_workspace_rule_files(workspace_path: &Path) -> Vec<WorkspaceRuleFile> {
    let mut out = Vec::new();
    let mut push_file = |rel: &str| {
        let abs = workspace_path.join(rel);
        if let Ok(content) = std::fs::read_to_string(&abs) {
            out.push(WorkspaceRuleFile {
                source: rel.to_string(),
                path: abs.to_string_lossy().into_owned(),
                content,
                updated_at: file_mtime_ms(&abs),
            });
        }
    };
    push_file("CLAUDE.md");
    push_file("AGENTS.md");
    push_file(".intent/guidelines.md");
    push_file(".augment/guidelines.md");

    // Scan .intent/rules/ (app-owned)
    let intent_rules_dir = workspace_path.join(".intent").join("rules");
    if let Ok(entries) = std::fs::read_dir(&intent_rules_dir) {
        let mut md: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "md"))
            .collect();
        md.sort();
        for abs in md {
            if let Ok(content) = std::fs::read_to_string(&abs) {
                out.push(WorkspaceRuleFile {
                    source: format!(
                        ".intent/rules/{}",
                        abs.file_name().unwrap().to_string_lossy()
                    ),
                    path: abs.to_string_lossy().into_owned(),
                    content,
                    updated_at: file_mtime_ms(&abs),
                });
            }
        }
    }

    // Scan .augment/rules/ (auggie convention, kept for compatibility)
    let augment_rules_dir = workspace_path.join(".augment").join("rules");
    if let Ok(entries) = std::fs::read_dir(&augment_rules_dir) {
        let mut md: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "md"))
            .collect();
        md.sort();
        for abs in md {
            if let Ok(content) = std::fs::read_to_string(&abs) {
                out.push(WorkspaceRuleFile {
                    source: format!(
                        ".augment/rules/{}",
                        abs.file_name().unwrap().to_string_lossy()
                    ),
                    path: abs.to_string_lossy().into_owned(),
                    content,
                    updated_at: file_mtime_ms(&abs),
                });
            }
        }
    }
    out
}

/// The repo rules a harness gives an agent: the text injected as user
/// rules, and what happens to each workspace rule file (for the prompt
/// breakdown).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RepoRules {
    /// The injected text and its source; `None` injects none.
    pub injected: Option<(String, String)>,
    /// Every workspace rule file found, in precedence order.
    pub files: Vec<RepoRuleFile>,
}

/// One workspace rule file and how the agent gets it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepoRuleFile {
    /// Path relative to the workspace, e.g. `AGENTS.md`.
    pub source: String,
    /// File size in bytes.
    pub bytes: usize,
    /// File length in UTF-16 code units, the breakdown's `chars`.
    pub chars: u64,
    pub loading: RuleLoading,
}

/// How an agent gets a workspace rule file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuleLoading {
    /// Intent injects it into the system prompt.
    Injected,
    /// The provider loads it by itself (its display name), so Intent does
    /// not inject it.
    LoadedNatively(&'static str),
    /// A lower-precedence file: neither Intent nor the provider loads it.
    NotLoaded,
}

impl RuleLoading {
    /// The breakdown wording, e.g. "loaded natively by Claude Code".
    #[must_use]
    pub(crate) fn describe(self) -> String {
        match self {
            Self::Injected => "injected".to_owned(),
            Self::LoadedNatively(provider) => format!("loaded natively by {provider}"),
            Self::NotLoaded => "not loaded".to_owned(),
        }
    }
}

/// Which rule files the `rules-loader.ts` precedence picks: the first
/// single file (`CLAUDE.md` / `AGENTS.md` / `.intent` or `.augment`
/// guidelines), else every `.md` under `.intent/rules/` and
/// `.augment/rules/`.
enum RuleSelection {
    Single(usize),
    Joined(Vec<usize>),
    None,
}

fn is_rules_dir_file(f: &WorkspaceRuleFile) -> bool {
    f.source.starts_with(".intent/rules/") || f.source.starts_with(".augment/rules/")
}

fn select_rule_files(files: &[WorkspaceRuleFile]) -> RuleSelection {
    if let Some(i) = files.iter().position(|f| !is_rules_dir_file(f)) {
        return RuleSelection::Single(i);
    }
    let joined: Vec<usize> = (0..files.len())
        .filter(|&i| is_rules_dir_file(&files[i]))
        .collect();
    if joined.is_empty() {
        RuleSelection::None
    } else {
        RuleSelection::Joined(joined)
    }
}

/// The injected `(content, source)` for `selection`.
fn render_rule_selection(
    workspace_path: &Path,
    files: &[WorkspaceRuleFile],
    selection: &RuleSelection,
) -> Option<(String, String)> {
    match selection {
        RuleSelection::Single(i) => Some((files[*i].content.clone(), files[*i].path.clone())),
        RuleSelection::Joined(indices) => {
            let parts: Vec<String> = indices
                .iter()
                .map(|&i| strip_frontmatter(&files[i].content).to_string())
                .collect();
            // Report the dir that actually contributed content; prefer .intent/rules
            // when both tiers contributed.
            let dir = if files.iter().any(|f| f.source.starts_with(".intent/rules/")) {
                workspace_path.join(".intent").join("rules")
            } else {
                workspace_path.join(".augment").join("rules")
            };
            Some((
                parts.join("\n\n---\n\n"),
                dir.to_string_lossy().into_owned(),
            ))
        }
        RuleSelection::None => None,
    }
}

fn is_selected(selection: &RuleSelection, i: usize) -> bool {
    match selection {
        RuleSelection::Single(s) => *s == i,
        RuleSelection::Joined(indices) => indices.contains(&i),
        RuleSelection::None => false,
    }
}

/// The repo-rule policy of harness 1.0 through 3.9: the first live rule
/// source, injected whatever the provider loads itself.
pub(crate) fn workspace_rules_v1(inputs: &crate::harness::RepoRulesInputs<'_>) -> RepoRules {
    let files = list_workspace_rule_files(inputs.workspace_path);
    let selection = select_rule_files(&files);
    RepoRules {
        injected: render_rule_selection(inputs.workspace_path, &files, &selection),
        files: files
            .iter()
            .enumerate()
            .map(|(i, f)| RepoRuleFile {
                source: f.source.clone(),
                bytes: f.content.len(),
                chars: intent_core::prompt_breakdown::utf16_len(&f.content),
                loading: if is_selected(&selection, i) {
                    RuleLoading::Injected
                } else {
                    RuleLoading::NotLoaded
                },
            })
            .collect(),
    }
}

/// The repo-rule policy of harness 3.10: [`workspace_rules_v1`]'s pick,
/// except that a picked file the executing provider loads by itself
/// ([`crate::native_rules::native_loader`]) is not injected. Skipping it
/// does not promote the next file: that one was not injected before either.
pub(crate) fn workspace_rules_native_aware(
    inputs: &crate::harness::RepoRulesInputs<'_>,
) -> RepoRules {
    let path = inputs.workspace_path;
    let files = list_workspace_rule_files(path);
    let selection = select_rule_files(&files);
    let native: Vec<Option<&'static str>> = files
        .iter()
        .map(|f| crate::native_rules::native_loader(&f.source, path, inputs.executor))
        .collect();
    let injected = match selection {
        RuleSelection::Single(i) if native[i].is_some() => None,
        _ => render_rule_selection(path, &files, &selection),
    };
    RepoRules {
        files: files
            .iter()
            .enumerate()
            .map(|(i, f)| RepoRuleFile {
                source: f.source.clone(),
                bytes: f.content.len(),
                chars: intent_core::prompt_breakdown::utf16_len(&f.content),
                loading: match native[i] {
                    Some(provider) => RuleLoading::LoadedNatively(provider),
                    None if injected.is_some() && is_selected(&selection, i) => {
                        RuleLoading::Injected
                    }
                    None => RuleLoading::NotLoaded,
                },
            })
            .collect(),
        injected,
    }
}

/// Read the `endUserRules` overrides map (`{ ruleType: { enabled, content,
/// updatedAt } }`); a missing/garbled row yields an empty map.
async fn read_overrides(store: &Store) -> Map<String, Value> {
    match store.get_setting(END_USER_RULES_KEY).await {
        Ok(Some(raw)) => serde_json::from_str::<Value>(&raw)
            .ok()
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default(),
        _ => Map::new(),
    }
}

/// The enabled, non-empty override body for `rule_type`, if any.
fn enabled_override(overrides: &Map<String, Value>, rule_type: &str) -> Option<String> {
    let cfg = overrides.get(rule_type)?;
    if !cfg.get("enabled").and_then(Value::as_bool).unwrap_or(true) {
        return None;
    }
    let content = cfg.get("content").and_then(Value::as_str).unwrap_or("");
    if content.trim().is_empty() {
        None
    } else {
        Some(content.to_string())
    }
}

/// Resolve the specialization rules for `agent_type` with the reference's 3-tier
/// fallback (port of `InstructionService.getSpecializationRules`,
/// `instruction-service.ts` ~186–255):
/// 1. the enabled `endUserRules` override for `agent_type` (settings **wins**),
/// 2. else a non-empty `<ws>/.intent/agent-rules/{agent_type}.md` workspace file,
/// 3. else the bundled built-in via
///    [`crate::instructions::get_instruction_with_common`].
///
/// Tier 3 always yields content (unknown types fall back to the `workspace`
/// body), so this returns a non-optional `String`.
///
/// PARITY NOTE: [`crate::agent_manager`]'s `DEFAULT_AGENT_TYPE = "interactive"`
/// is an unknown instruction id, so tier 3 composes `common + workspace +
/// workspace` via the reference's `fallbackToWorkspace` path (harness < 3.3;
/// 3.3+ composes `common + workspace`, intent-hq/intent#5835). The FE file-watch
/// cache-invalidation around the workspace file is intentionally not ported — the
/// daemon re-resolves per spawn, so the file is always read fresh.
///
/// `agent_features` gates feature-specific sections of the tier-3 bundled
/// bodies only; tier 1/2 user-supplied content is never filtered.
///
/// Latest-bound convenience form kept for unit tests; prompt assembly
/// resolves the session's pinned set via [`get_specialization_rules_sourced`].
#[cfg(test)]
pub(crate) async fn get_specialization_rules(
    store: &Store,
    workspace_path: Option<&Path>,
    agent_type: &str,
    agent_features: &intent_core::settings_file::AgentFeaturesSettings,
) -> String {
    get_specialization_rules_sourced(
        crate::harness::latest_entry().doctrine.instructions,
        store,
        workspace_path,
        agent_type,
        agent_features,
        false,
    )
    .await
    .0
}

/// Which tier of [`get_specialization_rules_sourced`] supplied the text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpecializationSource {
    /// The user's settings override.
    SettingsOverride,
    /// `<ws>/.intent/agent-rules/{agent_type}.md`.
    WorkspaceFile,
    /// Intent's bundled instructions.
    Bundled,
}

impl SpecializationSource {
    fn layer_tag(self) -> LayerTag {
        match self {
            Self::SettingsOverride => {
                LayerTag::user("agent-rules-override", "Agent instructions override")
            }
            Self::WorkspaceFile => LayerTag::user("agent-rules-file", "Agent instructions file")
                .hint(ControlHintKind::EditRepoRules, "Edit .intent/agent-rules"),
            Self::Bundled => LayerTag::intent("doctrine", "Intent instructions"),
        }
    }
}

/// [`get_specialization_rules`] with an explicit tier-3 instruction set —
/// the SESSION's pinned doctrine when called from [`assemble_system_prompt`]
/// (H2): tier 1/2 user-supplied content is unversioned, only the bundled
/// built-in comes from `set`. `plain_folder` (from
/// [`crate::instructions::plain_folder_gated`]) swaps the bundled git and
/// forge guidance for the plain-folder note.
///
/// Returns the text with the tier that supplied it.
pub(crate) async fn get_specialization_rules_sourced(
    set: &'static crate::instructions::InstructionSet,
    store: &Store,
    workspace_path: Option<&Path>,
    agent_type: &str,
    agent_features: &intent_core::settings_file::AgentFeaturesSettings,
    plain_folder: bool,
) -> (String, SpecializationSource) {
    // 1. User-settings override (highest precedence — settings win over file/bundled).
    let overrides = read_overrides(store).await;
    if let Some(c) = enabled_override(&overrides, agent_type) {
        return (c, SpecializationSource::SettingsOverride);
    }
    // 2. Workspace file `<ws>/.intent/agent-rules/{agent_type}.md` (non-empty wins).
    if let Some(path) = workspace_path {
        let file = path
            .join(".intent")
            .join("agent-rules")
            .join(format!("{agent_type}.md"));
        if let Ok(content) = std::fs::read_to_string(&file) {
            if !content.trim().is_empty() {
                return (content, SpecializationSource::WorkspaceFile);
            }
        }
    }
    // 3. Bundled built-in (composed with common/workspace per the reference),
    // from the caller's pinned instruction set.
    (
        crate::instructions::get_instruction_with_common_in(
            set,
            agent_type,
            agent_features,
            plain_folder,
        ),
        SpecializationSource::Bundled,
    )
}

/// Specialist inputs for the spawn-prompt injection (PP-1, reference
/// `instruction-service.ts` layers 4.8 and 9): the resolved behavior prompt is
/// wrapped in a `<specialist_role>` section after specialization and user
/// rules, and the role identity feeds a `## Role Reminder` footer near the end
/// of the prompt (recency). For top-level (non-sub-agent) interactive agents
/// the SP-1 `## Suggested Next Steps` directive is appended by
/// `assemble_system_prompt` after the role reminder, so the reminder is the
/// last section only for sub-agents. All fields optional: a behavior prompt
/// without a specialist name yields the section but no footer, and vice versa.
#[derive(Debug, Clone, Default)]
pub(crate) struct SpecialistPromptInjection {
    pub behavior_prompt: Option<String>,
    pub specialist_name: Option<String>,
    pub role_reminder: Option<String>,
}

/// Format the RTK instruction line for the given usable subcommands. Wording
/// owned by the given harness (H5); session-scoped assembly passes the
/// session's pinned harness, session-less callers pass
/// `crate::harness::latest()`.
pub(crate) fn rtk_instruction_line(
    harness: &'static dyn crate::harness::Harness,
    subcommands: &[String],
) -> String {
    harness.rtk_instruction_line(subcommands)
}

/// Build the RTK instruction line when enabled and available, worded by
/// `harness` (the session's pinned harness in session-scoped assembly).
/// Returns `None` when `rtk.enabled` is false or rtk is unavailable/has no
/// usable subcommands. Mirrors `cloudlands-fe rtk-detector.ts getRtkPromptInstruction()`.
async fn build_rtk_instruction(
    harness: &'static dyn crate::harness::Harness,
    rtk_enabled: bool,
) -> Option<String> {
    if !rtk_enabled {
        return None;
    }

    // Run detection on blocking thread pool to avoid blocking async runtime
    let status = tokio::task::spawn_blocking(rtk::detect_rtk)
        .await
        .unwrap_or_else(|_| rtk::RtkStatus {
            available: false,
            subcommands: vec![],
        });

    if !status.available || status.subcommands.is_empty() {
        return None;
    }

    Some(rtk_instruction_line(harness, &status.subcommands))
}

/// The section tag of [`with_provider_prompt_note`]'s note.
pub(crate) fn provider_note_tag() -> LayerTag {
    LayerTag::intent("provider-note", "Provider note")
}

/// Append the provider's prompt-only note
/// ([`intent_providers::ProviderConfig::system_prompt_note`]; Cursor's
/// native-subagent restriction) to an assembled prompt. Other providers'
/// prompts are returned unchanged.
pub(crate) fn with_provider_prompt_note(
    prompt: String,
    provider: &intent_providers::ProviderConfig,
) -> String {
    match provider.system_prompt_note {
        Some(note) => format!("{}\n\n{note}", prompt.trim_end()),
        None => prompt,
    }
}

/// Assemble the effective system prompt (the **internal** injection pipeline,
/// §18.1) in documented precedence: base-system-prompt override →
/// specialization rules (the 3-tier resolver: agent-type override → workspace
/// `.augment/agent-rules/{type}.md` → bundled built-in) → workspace override →
/// live workspace rule files → mode-dependent isolation hints (Task 6: `CoW`
/// sandboxing context for implementors, parallel delegation safety for
/// coordinators when `CoW` is enabled) → specialist role section (PP-1, reference
/// layer 4.8: after specialization/user rules, when the session has one) →
/// mandatory-actions footer (recency; the reference `getMandatoryActionsFooter`)
/// which contributes the `## Role Reminder` (specialist agents only) and — for
/// top-level (non-sub-agent) interactive agents — the `## Asking the User
/// Questions` hint (nudging structured `ws.app.question.ask` questions) plus
/// the `## Suggested Next Steps` directive that tells the model to emit a
/// `<!-- suggested-prompts ... -->`
/// block at the end of user-facing responses. The specialization slot is always
/// populated (tier 3 always resolves), so this returns `None` only in the
/// unreachable case where even the bundled specialization is empty.
///
/// `agent_features` (the `[agentFeatures]` toggles — the session's captured
/// snapshot resolved by the caller via `session_agent_features`, falling back
/// to live settings only for legacy NULL-snapshot rows) gates
/// feature-specific prompt sections: the bundled specialization bodies via
/// [`get_specialization_rules_sourced`], and the `## Asking the User Questions`
/// footer block (`structuredQuestions`). With all defaults on the assembled
/// prompt is byte-identical to the ungated one.
///
/// H2 (intent-hq/monorepo#2459): every harness-owned surface and the tier-3
/// bundled doctrine resolve through the SESSION's stamped `harnessVersion`
/// (via the harness registry), so an existing session keeps the exact prompt
/// text it was created with even after the binary ships a newer doctrine set;
/// a `None` session resolves to the latest.
///
/// `workspace_api_docs` — the condensed `workspace_api` reference for
/// providers whose MCP client truncates long tool descriptions
/// (`ProviderConfig::truncates_tool_descriptions`): the bridge serves the
/// compact description, and this carries the condensed `ws.*` reference into
/// the prompt under [`intent_acp::WORKSPACE_API_SYSTEM_PROMPT_HEADING`] (the
/// section the compact header points at). `None` — every non-flagged
/// provider — leaves the prompt byte-identical to before.
///
/// Test form: the spawn path calls [`assemble_system_prompt_sections`].
#[cfg(test)]
#[expect(clippy::too_many_arguments)]
pub(crate) async fn assemble_system_prompt(
    store: &Store,
    workspace_path: Option<&Path>,
    agent_type: &str,
    specialist: Option<&SpecialistPromptInjection>,
    is_sub_agent: bool,
    auto_commit_enabled: bool,
    rtk_enabled: bool,
    agent_features: &intent_core::settings_file::AgentFeaturesSettings,
    workspace: Option<&intent_core::Workspace>,
    agent_session: Option<&intent_core::AgentSession>,
    workspace_api_docs: Option<&str>,
) -> Option<String> {
    assemble_system_prompt_layers(
        store,
        workspace_path,
        agent_type,
        specialist,
        is_sub_agent,
        auto_commit_enabled,
        rtk_enabled,
        agent_features,
        workspace,
        agent_session,
        workspace_api_docs,
        Some(&crate::skill_catalog::SkillEnvironment::new(
            agent_type, None,
        )),
        None,
    )
    .await
    .map(|assembled| assembled.prompt)
}

/// [`assemble_system_prompt_sections`] without the sections: the prompt
/// text alone, for tests that do not check the breakdown.
#[cfg(test)]
#[expect(clippy::too_many_arguments)]
pub(crate) async fn assemble_agent_system_prompt(
    store: &Store,
    workspace_path: Option<&Path>,
    agent_type: &str,
    specialist: Option<&SpecialistPromptInjection>,
    is_sub_agent: bool,
    auto_commit_enabled: bool,
    rtk_enabled: bool,
    agent_features: &intent_core::settings_file::AgentFeaturesSettings,
    workspace: Option<&intent_core::Workspace>,
    agent_session: Option<&intent_core::AgentSession>,
    workspace_api_docs: Option<&str>,
    skills_environment: &crate::skill_catalog::SkillEnvironment,
    rule_executor: Option<&crate::native_rules::RuleExecutor>,
) -> Option<String> {
    assemble_system_prompt_sections(
        store,
        workspace_path,
        agent_type,
        specialist,
        is_sub_agent,
        auto_commit_enabled,
        rtk_enabled,
        agent_features,
        workspace,
        agent_session,
        workspace_api_docs,
        skills_environment,
        rule_executor,
    )
    .await
    .map(|assembled| assembled.prompt)
}

/// [`assemble_system_prompt`] for a local agent spawn, with the prompt's
/// measured sections for the `promptBreakdown` the spawn path records.
/// `rule_executor` says what runs the agent, so the session's harness can
/// skip rule files the provider loads by itself ([`crate::native_rules`]);
/// the skills block is built for `skills_environment`.
#[expect(clippy::too_many_arguments)]
pub(crate) async fn assemble_system_prompt_sections(
    store: &Store,
    workspace_path: Option<&Path>,
    agent_type: &str,
    specialist: Option<&SpecialistPromptInjection>,
    is_sub_agent: bool,
    auto_commit_enabled: bool,
    rtk_enabled: bool,
    agent_features: &intent_core::settings_file::AgentFeaturesSettings,
    workspace: Option<&intent_core::Workspace>,
    agent_session: Option<&intent_core::AgentSession>,
    workspace_api_docs: Option<&str>,
    skills_environment: &crate::skill_catalog::SkillEnvironment,
    rule_executor: Option<&crate::native_rules::RuleExecutor>,
) -> Option<AssembledPrompt> {
    assemble_system_prompt_layers(
        store,
        workspace_path,
        agent_type,
        specialist,
        is_sub_agent,
        auto_commit_enabled,
        rtk_enabled,
        agent_features,
        workspace,
        agent_session,
        workspace_api_docs,
        Some(skills_environment),
        rule_executor,
    )
    .await
}

/// [`assemble_system_prompt`] for an agent that runs on another machine:
/// the same layers without the skills catalog, whose paths are on this
/// host and cannot be read there. The machine reports no CLI version, so
/// every rule file stays injected ([`crate::native_rules::RuleExecutor::remote`]).
#[expect(clippy::too_many_arguments)]
pub(crate) async fn assemble_remote_system_prompt(
    store: &Store,
    workspace_path: Option<&Path>,
    agent_type: &str,
    specialist: Option<&SpecialistPromptInjection>,
    is_sub_agent: bool,
    auto_commit_enabled: bool,
    rtk_enabled: bool,
    agent_features: &intent_core::settings_file::AgentFeaturesSettings,
    workspace: Option<&intent_core::Workspace>,
    agent_session: Option<&intent_core::AgentSession>,
    workspace_api_docs: Option<&str>,
) -> Option<AssembledPrompt> {
    let remote = agent_session
        .and_then(|s| s.provider.as_deref())
        .map(|p| crate::native_rules::RuleExecutor::remote(intent_providers::base_provider_id(p)));
    assemble_system_prompt_layers(
        store,
        workspace_path,
        agent_type,
        specialist,
        is_sub_agent,
        auto_commit_enabled,
        rtk_enabled,
        agent_features,
        workspace,
        agent_session,
        workspace_api_docs,
        None,
        remote.as_ref(),
    )
    .await
}

/// `skills` is the environment the skills block is built for; `None` adds
/// no skills block.
#[expect(clippy::too_many_arguments)]
async fn assemble_system_prompt_layers(
    store: &Store,
    workspace_path: Option<&Path>,
    agent_type: &str,
    specialist: Option<&SpecialistPromptInjection>,
    is_sub_agent: bool,
    auto_commit_enabled: bool,
    rtk_enabled: bool,
    agent_features: &intent_core::settings_file::AgentFeaturesSettings,
    workspace: Option<&intent_core::Workspace>,
    agent_session: Option<&intent_core::AgentSession>,
    workspace_api_docs: Option<&str>,
    skills: Option<&crate::skill_catalog::SkillEnvironment>,
    rule_executor: Option<&crate::native_rules::RuleExecutor>,
) -> Option<AssembledPrompt> {
    // The session's pinned harness + doctrine (H2): a stamped session keeps
    // assembling the exact version it was created with; session-less calls
    // (previews, background one-shots) resolve to the latest.
    let entry = session_harness_entry(agent_session);
    let harness = entry.harness;
    let assistant = agent_session
        .map(|session| &session.workspace_id)
        .or_else(|| workspace.map(|workspace| &workspace.id))
        .filter(|id| id.is_chief())
        .and(entry.doctrine.instructions.assistant);
    // Plain-folder workspaces (§5.1) get no git, forge or PR guidance: the
    // bundled common sections and the commit policy are replaced by the
    // plain-folder note (latest harness only).
    let plain_folder =
        crate::instructions::plain_folder_gated(entry.doctrine.instructions, workspace);
    let overrides = read_overrides(store).await;
    let mut parts: Vec<(LayerTag, String)> = Vec::new();
    let mut hidden_skills = Vec::new();
    let mut provider_sections = Vec::new();
    if let Some(c) = enabled_override(&overrides, "base-system-prompt") {
        parts.push((LayerTag::user("base-override", "Base prompt override"), c));
    }
    if let Some(instructions) = assistant {
        parts.push((
            LayerTag::intent("assistant-instructions", "Assistant instructions"),
            instructions.to_string(),
        ));
    } else {
        let (specialization, source) = get_specialization_rules_sourced(
            entry.doctrine.instructions,
            store,
            workspace_path,
            agent_type,
            agent_features,
            plain_folder,
        )
        .await;
        if !specialization.trim().is_empty() {
            parts.push((source.layer_tag(), specialization));
        }
        if let Some(c) = enabled_override(&overrides, "workspace") {
            parts.push((
                LayerTag::user("workspace-override", "Workspace prompt override"),
                c,
            ));
        }
        if let Some(path) = workspace_path {
            let rules = crate::harness::RepoRulesInputs {
                workspace_path: path,
                executor: rule_executor,
            };
            let repo_rules = (entry.context.repo_rules)(&rules);
            provider_sections = native_repo_rule_sections(&repo_rules.files);
            if let Some((content, source)) = repo_rules.injected {
                if !content.trim().is_empty() {
                    parts.push((
                        repo_rules_tag(&source, path),
                        harness.user_rules_wrapper(&content, &source),
                    ));
                }
            }
        }
        // Repo config instructions (FE parity: instruction-service.ts L1019-1022):
        // append repo-level instructions from `.intent/config.json` when present.
        if let Some(ws) = workspace {
            if let Some(repo_path) = crate::git_ops::worktree_path(ws) {
                let repo_config = crate::repo_config::read_repo_config(&repo_path).await;
                if let Some(instructions) = repo_config.instructions {
                    if !instructions.trim().is_empty() {
                        let source = format!("{}/.intent/config.json", repo_path.display());
                        parts.push((
                            LayerTag::user("repo-config", "Repo config instructions")
                                .hint(ControlHintKind::EditRepoRules, "Edit .intent/config.json"),
                            harness.user_rules_wrapper(&instructions, &source),
                        ));
                    }
                }
            }
        }
        // RTK layer: when rtk.enabled is true and rtk is detected with ≥1 usable
        // subcommand, append the instruction line (worded by the session's pinned
        // harness). Placed after workspace-rules, before skills / isolation hint /
        // specialist role.
        if let Some(rtk_instruction) = build_rtk_instruction(harness, rtk_enabled).await {
            parts.push((LayerTag::intent("rtk", "RTK command hint"), rtk_instruction));
        }
    }
    // Skills catalog layer (reference layer 4.7: after specialization rules, user
    // rules, and skills — before isolation hint / specialist role). When a
    // workspace path is available, discover and inject the skills catalog. Empty
    // catalog ⇒ no layer appended. Discovery failures degrade gracefully (log
    // warn, omit layer) — never fail prompt assembly.
    if let (Some(ws), Some(environment)) = (workspace, skills) {
        if let Some(repo_path) = crate::git_ops::worktree_path(ws) {
            let skills =
                crate::skills::discover_skills_for_prompt(&repo_path.to_string_lossy()).await;
            match (entry.context.skills_block)(&crate::harness::SkillsBlockInputs {
                skills: &skills,
                environment,
            }) {
                catalog if !catalog.trim().is_empty() => {
                    parts.push((LayerTag::user("skills", "Skills"), catalog));
                }
                _ => {}
            }
            hidden_skills = (entry.context.hidden_skills)(&crate::harness::SkillsBlockInputs {
                skills: &skills,
                environment,
            });
        }
    }
    // Workspace API reference layer (truncating providers only): the
    // condensed `workspace_api` reference under the heading the compact tool
    // description points at. Placed with the other reference material —
    // after skills, before isolation hint / specialist role.
    if let Some(docs) = workspace_api_docs.map(str::trim).filter(|d| !d.is_empty()) {
        parts.push((
            LayerTag::intent("ws-reference", "Workspace API reference"),
            entry.context.workspace_api.prompt_section(docs),
        ));
    }
    // A backend sub-agent of a remote agent: where its parent's work is.
    if let Some(session) = agent_session {
        if let Some(work) =
            intent_core::remote_launch::BackendParentWork::from_metadata(session.metadata.as_ref())
        {
            parts.push((
                LayerTag::intent("parent-work", "Parent's work"),
                format!(
                    "## Parent's work\n\n{}",
                    work.notice(session.workspace_id.as_str())
                ),
            ));
        }
    }
    // Specialist role section (reference layer 4.8: after specialization
    // rules, user rules, and skills — before the parent-only layers).
    if let Some(bp) = specialist
        .and_then(|s| s.behavior_prompt.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        parts.push((
            LayerTag::user("specialist-prompt", "Specialist prompt")
                .hint(ControlHintKind::ChangeSpecialist, "Change the specialist"),
            harness.specialist_role_section(bp),
        ));
    }
    // Repository agents retain the commit policy; the Assistant uses app proposals.
    // The prompt no longer branches on the effective auto-commit state — the
    // OFF-state gate in `git_ops` and the auto-commit-on-idle subscriber
    // enforce the actual behavior.
    if assistant.is_none() && !plain_folder {
        parts.push((
            LayerTag::intent("commit-policy", "Commit policy"),
            harness.commit_policy_clause(),
        ));
    }
    // Mandatory-actions footer (reference layer 9 / `getMandatoryActionsFooter`,
    // pinned to the VERY END of the prompt to leverage recency bias). Three
    // independent sub-blocks, joined with `---` like every other layer:
    //   1. Role Reminder — only for specialist agents.
    //   2. Asking the User Questions — only for top-level (non-sub-agent) agents.
    //   3. Suggested Next Steps — only for top-level (non-sub-agent) agents.
    // The per-turn `[Role Reminder: …]` prefix in
    // `agent_manager::build_turn_prompt` stays and is independent of this.
    if let Some(name) = specialist
        .and_then(|s| s.specialist_name.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let reminder = specialist
            .and_then(|s| s.role_reminder.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty());
        parts.push((
            LayerTag::intent("role-reminder", "Role reminder")
                .hint(ControlHintKind::ChangeSpecialist, "Change the specialist"),
            harness.role_reminder_footer(name, reminder),
        ));
    }
    // Asking the User Questions + Suggested Next Steps — top-level
    // interactive agents only. Sub-agents don't own a user-facing chat turn
    // (they report to a parent), so they skip both blocks, matching the
    // reference gating. The questions block is additionally gated by
    // `agentFeatures.structuredQuestions` (spec audit row 8).
    if !is_sub_agent {
        if agent_features.structured_questions {
            parts.push((
                LayerTag::intent("ask-questions", "Asking the user questions").hint(
                    ControlHintKind::AgentFeatures,
                    "Turn off Structured questions in Agent features",
                ),
                harness.ask_questions_block(),
            ));
        }
        // Per-session effective state for the SP-1 footer wording: a session
        // that opted out via `skipAutoCommit` (delegation/creation while the
        // workspace was OFF) never auto-commits, so the footer must follow the
        // effective state even when the workspace toggle is currently on. The
        // commit-policy clause above is deliberately status-neutral.
        let effective_auto_commit =
            auto_commit_enabled && !agent_session.is_some_and(|s| s.skip_auto_commit);
        if assistant.is_none() {
            let next_steps = match entry
                .doctrine
                .instructions
                .plain_folder
                .filter(|_| plain_folder)
            {
                // Plain folders have no commits, branches or pull requests:
                // their footer examples and wording say so (no auto-commit
                // clause either).
                Some(doctrine) => doctrine.suggested_next_steps.to_string(),
                None => harness.suggested_next_steps_block(effective_auto_commit),
            };
            parts.push((next_steps_tag(), next_steps));
        }
    }
    if parts.is_empty() {
        return None;
    }
    let texts: Vec<String> = parts.iter().map(|(_, text)| text.clone()).collect();
    let joined = harness.join_prompt_layers(&texts);
    let mut assembled = AssembledPrompt::from_layers(&parts, joined, hidden_skills);
    assembled.provider_sections = provider_sections;
    Some(assembled)
}

fn next_steps_tag() -> LayerTag {
    LayerTag::intent("suggested-next-steps", "Suggested next steps")
}

/// The section tag of the repo-rules layer, with a hint naming the rule
/// file (or rules folder) it came from, relative to the workspace.
/// The `provider` sections for rule files the provider loads by itself
/// (harness 3.10 skips injecting them): one per provider, sized by its
/// files, e.g. "Repo rules: loaded natively by Claude Code".
pub(crate) fn native_repo_rule_sections(
    files: &[RepoRuleFile],
) -> Vec<intent_core::prompt_breakdown::PromptSection> {
    let mut by_provider: Vec<(RuleLoading, Vec<&str>, u64)> = Vec::new();
    for f in files {
        if !matches!(f.loading, RuleLoading::LoadedNatively(_)) {
            continue;
        }
        match by_provider.iter_mut().find(|(l, _, _)| *l == f.loading) {
            Some((_, sources, chars)) => {
                sources.push(&f.source);
                *chars += f.chars;
            }
            None => by_provider.push((f.loading, vec![&f.source], f.chars)),
        }
    }
    by_provider
        .into_iter()
        .map(|(loading, sources, chars)| {
            let tag = LayerTag::user("repo-rules", &format!("Repo rules: {}", loading.describe()))
                .hint(
                    ControlHintKind::EditRepoRules,
                    format!("Edit {}", sources.join(", ")),
                );
            crate::prompt_sections::provider_section(&tag, chars)
        })
        .collect()
}

fn repo_rules_tag(source: &str, workspace_path: &Path) -> LayerTag {
    let path = Path::new(source);
    let shown = path
        .strip_prefix(workspace_path)
        .ok()
        .filter(|rel| !rel.as_os_str().is_empty())
        .or_else(|| path.file_name().map(Path::new))
        .map_or_else(|| source.to_string(), |p| p.to_string_lossy().into_owned());
    let text = format!("Edit {shown}");
    LayerTag::user("repo-rules", "Repo rules").hint(ControlHintKind::EditRepoRules, text)
}

/// Stateless executor for the `rules.*` namespace over the settings [`Store`].
/// Construct one per call from the long-lived `Services`.
pub(crate) struct RulesService<'a> {
    store: &'a Store,
}

impl<'a> RulesService<'a> {
    pub(crate) fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// `rules.list` → `{ rules: RuleSet }`: every user-override type that has
    /// content (editable), plus — when a worktree path is known — each live
    /// workspace rule file as a read-only entry (PROTOCOL §5.21).
    pub(crate) async fn list(
        &self,
        workspace_id: Option<&str>,
        workspace_path: Option<&Path>,
    ) -> Result<Value> {
        let overrides = read_overrides(self.store).await;
        let mut entries: Vec<Value> = Vec::new();
        for (rule_type, cfg) in &overrides {
            let content = cfg.get("content").and_then(Value::as_str).unwrap_or("");
            if content.trim().is_empty() {
                continue;
            }
            entries.push(json!({
                "ruleType": rule_type,
                "source": "user-override",
                "content": content,
                "enabled": cfg.get("enabled").and_then(Value::as_bool).unwrap_or(true),
                "updatedAt": cfg.get("updatedAt").and_then(Value::as_i64).unwrap_or(0),
                "editable": true,
            }));
        }
        if let Some(path) = workspace_path {
            for f in list_workspace_rule_files(path) {
                entries.push(json!({
                    "ruleType": "workspace",
                    "source": f.source,
                    "path": f.path,
                    "content": f.content,
                    "enabled": true,
                    "updatedAt": f.updated_at,
                    "editable": false,
                }));
            }
        }
        let mut rule_set = Map::new();
        if let Some(ws) = workspace_id {
            rule_set.insert("workspaceId".into(), json!(ws));
        }
        rule_set.insert("rules".into(), Value::Array(entries));
        Ok(json!({ "rules": Value::Object(rule_set) }))
    }

    /// `rules.get` → `{ enabled, content, updatedAt }` for one user-override
    /// type; an absent type reads back as a disabled empty default.
    pub(crate) async fn get(&self, rule_type: &str) -> Result<Value> {
        let overrides = read_overrides(self.store).await;
        Ok(match overrides.get(rule_type) {
            Some(cfg) => json!({
                "enabled": cfg.get("enabled").and_then(Value::as_bool).unwrap_or(true),
                "content": cfg.get("content").and_then(Value::as_str).unwrap_or(""),
                "updatedAt": cfg.get("updatedAt").and_then(Value::as_i64).unwrap_or(0),
            }),
            None => json!({ "enabled": false, "content": "", "updatedAt": 0 }),
        })
    }

    /// `rules.update` — upsert the override body (+ `enabled`, defaulting to the
    /// prior flag or `true`), persist, and re-read the set. Returns
    /// `({ rules: RuleSet }, settings:changed payload)`; over-long content →
    /// `-32602` (PROTOCOL §5.21).
    pub(crate) async fn update(
        &self,
        rule_type: &str,
        content: &str,
        enabled: Option<bool>,
        workspace_id: Option<&str>,
        workspace_path: Option<&Path>,
    ) -> Result<(Value, Value)> {
        if rule_type.trim().is_empty() {
            return Err(Error::InvalidParams(
                "ruleType must not be empty".to_string(),
            ));
        }
        if content.len() > MAX_RULE_CONTENT_LEN {
            return Err(Error::InvalidParams(format!(
                "rule content exceeds {MAX_RULE_CONTENT_LEN} characters"
            )));
        }
        let mut overrides = read_overrides(self.store).await;
        let prev_enabled = overrides
            .get(rule_type)
            .and_then(|c| c.get("enabled"))
            .and_then(Value::as_bool);
        let enabled = enabled.or(prev_enabled).unwrap_or(true);
        overrides.insert(
            rule_type.to_string(),
            json!({ "enabled": enabled, "content": content, "updatedAt": now_ms() }),
        );
        let raw = serde_json::to_string(&Value::Object(overrides.clone()))
            .map_err(|e| Error::Internal(format!("encode endUserRules failed: {e}")))?;
        self.store.set_setting(END_USER_RULES_KEY, &raw).await?;
        let rules = self.list(workspace_id, workspace_path).await?;
        let changed = json!({ "path": END_USER_RULES_KEY, "value": Value::Object(overrides) });
        Ok((rules, changed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::settings_file::AgentFeaturesSettings;

    /// Cursor sessions get the prompt-only native-subagent restriction;
    /// every other provider's prompt is unchanged.
    #[test]
    fn provider_prompt_note_reaches_cursor_only() {
        let base = "## Base\n\nInstructions.\n".to_string();
        for provider in intent_providers::ACP_PROVIDERS {
            let out = with_provider_prompt_note(base.clone(), provider);
            if provider.id == "cursor" {
                assert!(out.starts_with("## Base\n\nInstructions."), "{out}");
                assert!(
                    out.ends_with(intent_providers::CURSOR_SUBAGENT_PROMPT_NOTE),
                    "{out}"
                );
            } else {
                assert_eq!(out, base, "{}", provider.id);
            }
        }
    }
    use intent_core::Workspace;
    use intent_store::Store;
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// A unique temp DB path that cleans up its `.db`/`-wal`/`-shm` files on drop.
    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-test-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let p = PathBuf::from(format!("{}{suffix}", self.path.display()));
                let _ = std::fs::remove_file(p);
            }
        }
    }

    /// Helper to create a test workspace with a repository path
    fn make_test_workspace(repo_path: &Path) -> Workspace {
        let ts = intent_core::now_iso();
        Workspace {
            id: intent_core::WorkspaceId("test-ws".to_string()),
            title: "Test Workspace".to_string(),
            branch: "main".to_string(),
            created_at: ts.clone(),
            updated_at: ts,
            path: Some(repo_path.to_string_lossy().to_string()),
            repository_path: Some(repo_path.to_string_lossy().to_string()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_assemble_system_prompt_with_skills() {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();
        let skills_dir = repo_path.join(".augment").join("skills").join("test-skill");
        tokio::fs::create_dir_all(&skills_dir).await.unwrap();

        let skill_content = r"---
name: test-skill
description: A test skill for prompt assembly
---

This is a test skill.
";
        tokio::fs::write(skills_dir.join("SKILL.md"), skill_content)
            .await
            .unwrap();

        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();
        let workspace = make_test_workspace(repo_path);

        let prompt = assemble_system_prompt(
            &store,
            Some(repo_path),
            "workspace",
            None,
            false,
            false,
            false,
            &AgentFeaturesSettings::default(),
            Some(&workspace),
            None,
            None,
        )
        .await;

        assert!(prompt.is_some());
        let prompt_text = prompt.unwrap();

        // Assert the skills catalog is present
        assert!(
            prompt_text.contains("<available_skills>"),
            "Skills catalog block should be present"
        );
        assert!(
            prompt_text.contains("<skill>"),
            "Skills catalog should contain skill entries"
        );
        assert!(
            prompt_text.contains("<name>test-skill</name>"),
            "Skills catalog should contain the test skill name"
        );
        assert!(
            prompt_text.contains("<description>A test skill for prompt assembly</description>"),
            "Skills catalog should contain the test skill description"
        );

        // Assert layer ordering: skills should come after user rules but before
        // specialist role (if present)
        let skills_pos = prompt_text.find("<available_skills>").unwrap();
        let specialist_pos = prompt_text.find("<specialist_role>");

        // If specialist role is present, skills should come before it
        if let Some(sp_pos) = specialist_pos {
            assert!(
                skills_pos < sp_pos,
                "Skills catalog should come before specialist role"
            );
        }
    }

    #[tokio::test]
    async fn test_assemble_system_prompt_without_skills() {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();

        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();
        let workspace = make_test_workspace(repo_path);

        let prompt = assemble_system_prompt(
            &store,
            Some(repo_path),
            "workspace",
            None,
            false,
            false,
            false,
            &AgentFeaturesSettings::default(),
            Some(&workspace),
            None,
            None,
        )
        .await;

        assert!(prompt.is_some());
        let prompt_text = prompt.unwrap();

        // Assert the skills catalog is NOT present when no skills exist
        assert!(
            !prompt_text.contains("<available_skills>"),
            "Skills catalog block should be absent when no skills exist"
        );
    }

    #[tokio::test]
    async fn test_assemble_system_prompt_no_workspace() {
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();

        let prompt = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            false,
            false,
            false,
            &AgentFeaturesSettings::default(),
            None,
            None,
            None,
        )
        .await;

        assert!(prompt.is_some());
        let prompt_text = prompt.unwrap();

        // Assert the skills catalog is NOT present when no workspace is provided
        assert!(
            !prompt_text.contains("<available_skills>"),
            "Skills catalog block should be absent when no workspace provided"
        );
    }

    #[tokio::test]
    async fn test_workspace_api_docs_layer() {
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();

        // `None` (every non-flagged provider): byte-identical to before —
        // no heading, no docs.
        let without = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            false,
            false,
            false,
            &AgentFeaturesSettings::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        assert!(
            !without.contains(intent_acp::WORKSPACE_API_SYSTEM_PROMPT_HEADING),
            "no docs supplied: prompt must not carry the API reference section"
        );

        // `Some(docs)` (truncating providers): the docs appear under the
        // heading the compact tool description points at, and the rest of
        // the prompt is unchanged.
        let docs = "ws.note.read(id) → { id, title, content }";
        let with = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            false,
            false,
            false,
            &AgentFeaturesSettings::default(),
            None,
            None,
            Some(docs),
        )
        .await
        .unwrap();
        assert!(
            with.contains(intent_acp::WORKSPACE_API_SYSTEM_PROMPT_HEADING),
            "docs supplied: prompt must carry the API reference heading"
        );
        assert!(
            with.contains(docs),
            "docs supplied: prompt must carry the docs verbatim"
        );
        let heading_pos = with
            .find(intent_acp::WORKSPACE_API_SYSTEM_PROMPT_HEADING)
            .unwrap();
        let docs_pos = with.find(docs).unwrap();
        assert!(heading_pos < docs_pos, "docs must sit under the heading");

        // Empty/whitespace docs: treated as absent.
        let blank = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            false,
            false,
            false,
            &AgentFeaturesSettings::default(),
            None,
            None,
            Some("  \n"),
        )
        .await
        .unwrap();
        assert_eq!(blank, without, "blank docs must leave the prompt unchanged");
    }

    #[tokio::test]
    async fn test_ask_questions_hint_present_for_top_level_agents() {
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();

        let prompt = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            false,
            false,
            false,
            &AgentFeaturesSettings::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert!(
            prompt.contains("## Asking the User Questions"),
            "Ask-questions hint should be present for top-level agents"
        );
        assert!(
            prompt.contains("ws.app.question.ask"),
            "Ask-questions hint should reference ws.app.question.ask"
        );
        // Same gating as Suggested Next Steps: both footer blocks appear together.
        assert!(
            prompt.contains("## Suggested Next Steps"),
            "Suggested Next Steps should be present for top-level agents"
        );
    }

    #[tokio::test]
    async fn test_ask_questions_hint_absent_for_sub_agents() {
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();

        let prompt = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            true,
            false,
            false,
            &AgentFeaturesSettings::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert!(
            !prompt.contains("## Asking the User Questions"),
            "Ask-questions hint should be absent for sub-agents"
        );
        // Same gating as Suggested Next Steps: both footer blocks are skipped.
        assert!(
            !prompt.contains("## Suggested Next Steps"),
            "Suggested Next Steps should be absent for sub-agents"
        );
    }

    /// The exact status-neutral commit-policy clause injected for every agent
    /// in both auto-commit states.
    const COMMIT_POLICY_CLAUSE: &str = "## Commit Policy\n\n\
         Commit through `ws.git.commit` — never run `git commit` yourself \
         unless the user explicitly asks for a git workflow that \
         `ws.git.commit` cannot express (e.g. multiple scoped commits on a \
         branch). You may commit when it makes sense for the work; the system \
         may also automatically commit any remaining changes when your turn \
         ends.";

    #[tokio::test]
    async fn test_commit_policy_clause_when_auto_commit_on() {
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();

        let prompt = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            false,
            true,
            false,
            &AgentFeaturesSettings::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert!(
            prompt.contains(COMMIT_POLICY_CLAUSE),
            "status-neutral commit-policy clause should be present when auto-commit is on"
        );
        assert!(
            !prompt.contains("Auto-commit is OFF."),
            "old OFF-state clause should be gone"
        );
        assert!(
            !prompt.contains("Do not commit on your own initiative"),
            "old ON-state clause should be gone"
        );
    }

    /// Per-session effective state: a session that opted out via
    /// `skipAutoCommit` still gets the same status-neutral commit-policy
    /// clause, while the SP-1 suggested-prompts footer keeps following the
    /// effective auto-commit state.
    #[tokio::test]
    async fn test_commit_policy_neutral_when_session_skips_auto_commit() {
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();

        let ts = intent_core::now_iso();
        let session = intent_core::AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: intent_core::AgentId::from("agent-skip"),
            workspace_id: intent_core::WorkspaceId::from("ws-skip"),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Skip Agent".into(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: true,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
            notifications_muted: false,
            pinned: false,
            is_background: false,
            metadata: None,
            created_at: ts.clone(),
            updated_at: ts,
        };

        let prompt = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            false,
            true,
            false,
            &AgentFeaturesSettings::default(),
            None,
            Some(&session),
            None,
        )
        .await
        .unwrap();

        assert!(
            prompt.contains(COMMIT_POLICY_CLAUSE),
            "opted-out session gets the same status-neutral clause"
        );
        assert!(
            !prompt.contains("Auto-commit is OFF."),
            "old OFF-state clause should be gone for an opted-out session"
        );
        assert!(
            !prompt.contains("Auto-commit is enabled;"),
            "suggested-prompts auto-commit clause must follow the effective state"
        );
    }

    #[tokio::test]
    async fn test_commit_policy_clause_when_auto_commit_off() {
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();

        // Sub-agent gating does not apply: the clause is injected for every
        // agent, so assert it with `is_sub_agent = true`. Sub-agent prompts
        // have no auto-commit-aware footer, so the ON and OFF prompts must be
        // byte-identical.
        let prompt_off = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            true,
            false,
            false,
            &AgentFeaturesSettings::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        let prompt_on = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            true,
            true,
            false,
            &AgentFeaturesSettings::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert!(
            prompt_off.contains(COMMIT_POLICY_CLAUSE),
            "status-neutral commit-policy clause should be present when auto-commit is off"
        );
        assert_eq!(
            prompt_off, prompt_on,
            "sub-agent prompt must be identical in both auto-commit states"
        );
    }

    #[tokio::test]
    async fn test_structured_questions_off_removes_ask_questions_block_only() {
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();

        let features = AgentFeaturesSettings {
            structured_questions: false,
            ..AgentFeaturesSettings::default()
        };
        let prompt = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            false,
            false,
            false,
            &features,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert!(
            !prompt.contains("## Asking the User Questions"),
            "Ask-questions block should be absent when structuredQuestions is off"
        );
        assert!(
            !prompt.contains("ws.app.question.ask"),
            "structured-questions binding should not be referenced when off"
        );
        assert!(
            prompt.contains("## Suggested Next Steps"),
            "Suggested Next Steps must survive structuredQuestions gating"
        );
    }

    #[tokio::test]
    async fn test_attention_requests_off_removes_raising_attention_section() {
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();

        let features = AgentFeaturesSettings {
            attention_requests: false,
            ..AgentFeaturesSettings::default()
        };
        let prompt = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            false,
            false,
            false,
            &features,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        assert!(
            !prompt.contains("## Raising Attention"),
            "Raising Attention section should be absent when attentionRequests is off"
        );
        assert!(
            !prompt.contains("ws.agent.reportBlocker")
                && !prompt.contains("ws.agent.requestDiscussion"),
            "attention-request bindings should not be referenced when off"
        );
        assert!(
            prompt.contains("## Waiting on External Conditions"),
            "neighboring sections must survive attentionRequests gating"
        );
    }

    #[tokio::test]
    async fn test_agent_features_defaults_keep_all_gated_sections() {
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();

        // All defaults on: every gated section is present and the bundled
        // specialization rides in untouched (byte-identity of the bundled
        // bodies themselves is asserted in `instructions::tests`).
        let with_defaults = assemble_system_prompt(
            &store,
            None,
            "workspace",
            None,
            false,
            false,
            false,
            &AgentFeaturesSettings::default(),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        assert!(with_defaults.contains("## Waiting on External Conditions"));
        assert!(with_defaults.contains("## Rich Chat Rendering"));
        assert!(with_defaults.contains("## Asking the User Questions"));
        assert!(with_defaults.contains("## Raising Attention"));

        // The bundled specialization slice is the untouched composition.
        let expected_specialization = crate::instructions::get_instruction_with_common(
            "workspace",
            &AgentFeaturesSettings::default(),
        );
        assert!(with_defaults.contains(&expected_specialization));
    }

    /// Headings and API names a plain-folder prompt must never carry (Brief
    /// Must 6): the git, forge and sibling-workspace guidance.
    const PLAIN_FOLDER_ABSENT: &[&str] = &[
        "## GitHub & Git Operations",
        "## Commit Policy",
        "## Follow-up Workspaces",
        "## Link other workspaces",
        "## Waiting on External Conditions",
        "ws.git",
        "ws.pr",
        "ws.mr",
        "proposeSibling",
    ];

    fn plain_folder_workspace(folder: &Path) -> Workspace {
        Workspace {
            branch: String::new(),
            plain_folder: true,
            ..make_test_workspace(folder)
        }
    }

    async fn assembled(
        store: &Store,
        folder: &Path,
        agent_type: &str,
        is_sub_agent: bool,
        workspace: &Workspace,
    ) -> String {
        assemble_system_prompt(
            store,
            Some(folder),
            agent_type,
            None,
            is_sub_agent,
            true,
            false,
            &AgentFeaturesSettings::default(),
            Some(workspace),
            None,
            None,
        )
        .await
        .expect("prompt")
    }

    #[tokio::test]
    async fn plain_folder_prompt_has_no_git_guidance_and_carries_the_note() {
        let temp_dir = TempDir::new().unwrap();
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();
        let workspace = plain_folder_workspace(temp_dir.path());
        for agent_type in ["interactive", "workspace", "workspace-agent", "task-loop"] {
            for is_sub_agent in [false, true] {
                let prompt = assembled(
                    &store,
                    temp_dir.path(),
                    agent_type,
                    is_sub_agent,
                    &workspace,
                )
                .await;
                let probe = prompt.replace("ws.primitive", "");
                for absent in PLAIN_FOLDER_ABSENT {
                    assert!(
                        !probe.contains(absent),
                        "{agent_type} (sub-agent {is_sub_agent}) still carries {absent:?}"
                    );
                }
                for present in [
                    "## Plain folder workspace",
                    "no undo",
                    "Confirm with the user before you delete files or rewrite many files",
                    "Do not run `git init`",
                    "## Delegating Tasks",
                    "## Note Editing",
                ] {
                    assert!(
                        prompt.contains(present),
                        "{agent_type} (sub-agent {is_sub_agent}) lacks {present:?}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn git_workspace_prompt_keeps_git_guidance_and_has_no_plain_folder_note() {
        let temp_dir = TempDir::new().unwrap();
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();
        let workspace = make_test_workspace(temp_dir.path());
        let prompt = assembled(&store, temp_dir.path(), "interactive", false, &workspace).await;
        for present in [
            "## GitHub & Git Operations",
            "## Commit Policy",
            "## Follow-up Workspaces",
            "## Link other workspaces",
            "## Waiting on External Conditions",
        ] {
            assert!(prompt.contains(present), "git workspace lacks {present:?}");
        }
        assert!(!prompt.contains("## Plain folder workspace"));
        // The flag, not the folder contents or an empty branch, selects the
        // mode: an otherwise identical row without `plainFolder` is a git
        // workspace.
        let unflagged = Workspace {
            branch: String::new(),
            ..make_test_workspace(temp_dir.path())
        };
        assert_eq!(
            assembled(&store, temp_dir.path(), "interactive", false, &unflagged).await,
            prompt
        );
    }

    /// Words Intent-authored prompt text must never use in a plain-folder
    /// workspace (Brief Must 6), matched case-insensitively as whole words.
    const PLAIN_FOLDER_BANNED_WORDS: &[&str] = &[
        "git",
        "commit",
        "commits",
        "committing",
        "branch",
        "branches",
        "pull request",
        "pull requests",
        "pr",
        "prs",
        "merge request",
        "mr",
        "merge",
        "merging",
        "push",
    ];

    /// Every banned whole word in `text`, with the line it appears on.
    fn plain_folder_word_leaks(text: &str) -> Vec<String> {
        let pattern = format!(
            r"(?i)\b({})\b",
            PLAIN_FOLDER_BANNED_WORDS
                .iter()
                .map(|w| regex::escape(w))
                .collect::<Vec<_>>()
                .join("|")
        );
        let re = regex::Regex::new(&pattern).unwrap();
        text.lines()
            .filter_map(|line| {
                re.find(line)
                    .map(|m| format!("{:?} in: {}", m.as_str(), line.trim()))
            })
            .collect()
    }

    struct NoApi;
    impl intent_core::WorkspaceApi for NoApi {}

    /// The full assembled prompt for a plain-folder workspace — every
    /// Intent-authored layer (bundled doctrine, commit policy slot, the
    /// condensed `workspace_api` reference, role reminder, questions and
    /// suggested-next-steps footers) on the latest harness — never mentions
    /// git, commits, branches, pull/merge requests, merging or pushing, for
    /// every default role, the coordinator role, top-level and sub-agents and
    /// both auto-commit states. User-supplied specialist prompts (Brief
    /// Must 7) and provider-owned text are not part of the assembly here.
    #[tokio::test]
    async fn plain_folder_prompt_never_mentions_git_or_forge_words() {
        let temp_dir = TempDir::new().unwrap();
        let tmp_db = TempDb::new();
        let store = Store::open(&tmp_db.path).await.unwrap();
        let workspace = plain_folder_workspace(temp_dir.path());
        let coordinator = SpecialistPromptInjection {
            behavior_prompt: None,
            specialist_name: Some("Coordinator".to_string()),
            role_reminder: None,
        };
        let service = crate::specialists::SpecialistsService::new(
            Some(temp_dir.path().join("user")),
            Some(temp_dir.path().join("bundled")),
        )
        .with_embedded(crate::specialists::EMBEDDED_BUNDLED_V3_8);
        let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("resources/specialists/v3.8");
        let bundled: Vec<_> = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
            .map(|path| {
                let id = path.file_stem().unwrap().to_str().unwrap();
                let (body, name, reminder) = service
                    .resolve_prompt_injection_in(id, None, true)
                    .expect("every v3.8 file must be in the bundled catalog");
                SpecialistPromptInjection {
                    behavior_prompt: body,
                    specialist_name: Some(name),
                    role_reminder: reminder,
                }
            })
            .collect();
        assert!(!bundled.is_empty());
        let mut leaks = Vec::new();
        for agent_type in [
            "interactive",
            "workspace",
            "workspace-agent",
            "task-loop",
            "task-focused",
            "task-debug",
            "task-breakdown",
            "debug",
            "chat",
        ] {
            for specialist in [None, Some(&coordinator)]
                .into_iter()
                .chain(bundled.iter().map(Some))
            {
                for is_sub_agent in [false, true] {
                    for auto_commit in [false, true] {
                        let docs = intent_acp::WorkspaceMcpServer::new(
                            std::sync::Arc::new(NoApi),
                            workspace.id.clone(),
                        )
                        .with_sub_agent(is_sub_agent)
                        .with_plain_folder(true)
                        .condensed_workspace_api_description();
                        let prompt = assemble_system_prompt(
                            &store,
                            Some(temp_dir.path()),
                            agent_type,
                            specialist,
                            is_sub_agent,
                            auto_commit,
                            false,
                            &AgentFeaturesSettings::default(),
                            Some(&workspace),
                            None,
                            Some(&docs),
                        )
                        .await
                        .expect("prompt");
                        // The plain-folder note must name `git init` (Brief
                        // Must 6); it is the one sanctioned mention.
                        let note = crate::harness::latest_entry()
                            .doctrine
                            .instructions
                            .plain_folder
                            .expect("latest set gates plain folders")
                            .note;
                        assert_eq!(prompt.matches(note).count(), 1, "{agent_type}");
                        if !is_sub_agent {
                            assert!(prompt.contains("## Suggested Next Steps"), "{agent_type}");
                        }
                        let prompt = prompt.replace(note, "");
                        for leak in plain_folder_word_leaks(&prompt) {
                            let entry = format!("{agent_type}: {leak}");
                            if !leaks.contains(&entry) {
                                leaks.push(entry);
                            }
                        }
                    }
                }
            }
        }
        assert!(
            leaks.is_empty(),
            "{} leaks:\n{}",
            leaks.len(),
            leaks.join("\n")
        );
    }

    /// Native repo-rule dedupe (harness 3.10): what the assembled prompt of
    /// a session on `version`, run by `executor`, carries from the
    /// workspace rule files.
    mod native_repo_rules {
        use super::*;
        use crate::native_rules::test_versions::{BELOW, NEW};
        use crate::native_rules::RuleExecutor;

        const AGENTS: &str = "AGENTS-RULE-TEXT: run make gate before a PR.";
        const CLAUDE: &str = "CLAUDE-RULE-TEXT: prefer small commits.";

        fn session_on(version: &str, provider: &str) -> intent_core::AgentSession {
            intent_core::AgentSession {
                harness_version: version.to_owned(),
                harness_features: None,
                id: intent_core::AgentId::from("agent-skip"),
                workspace_id: intent_core::WorkspaceId::from("ws-skip"),
                parent_agent_id: None,
                backend_session_id: None,
                acp_session_id: None,
                name: "Skip Agent".into(),
                name_explicitly_set: false,
                model: None,
                reasoning_effort: None,
                effort_levels: None,
                provider: Some(provider.to_owned()),
                system_prompt: None,
                specialist: None,
                status: intent_core::AgentStatus::Active,
                is_active: false,
                messages: vec![],
                stats: None,
                task_note_id: None,
                skip_auto_commit: false,
                completion_report: None,
                completion_report_timestamp: None,
                attention_request_kind: None,
                attention_request_reason: None,
                attention_request_timestamp: None,
                delegation_depth: None,
                initial_message: None,
                context_references: None,
                image_blocks: None,
                file_blocks: None,
                stop_reason: None,
                stop_reason_timestamp: None,
                session_corrupted: false,
                pending_delete_at: None,
                retired_at: None,
                notifications_muted: false,
                pinned: false,
                is_background: false,
                metadata: None,
                created_at: intent_core::now_iso(),
                updated_at: intent_core::now_iso(),
            }
        }

        fn claude(version: Option<&str>) -> RuleExecutor {
            RuleExecutor {
                provider_id: "claude-code".into(),
                cli_version: version.map(str::to_owned),
                codex_doc_cap: None,
            }
        }

        fn codex() -> RuleExecutor {
            RuleExecutor {
                provider_id: "codex".into(),
                cli_version: None,
                codex_doc_cap: Some(crate::native_rules::CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES),
            }
        }

        fn repo(files: &[(&str, &str)]) -> TempDir {
            let dir = TempDir::new().unwrap();
            std::fs::create_dir(dir.path().join(".git")).unwrap();
            for (name, body) in files {
                std::fs::write(dir.path().join(name), body).unwrap();
            }
            dir
        }

        async fn prompt(
            dir: &Path,
            version: &str,
            executor: Option<&RuleExecutor>,
            remote: bool,
        ) -> String {
            let tmp_db = TempDb::new();
            let store = Store::open(&tmp_db.path).await.unwrap();
            let provider = executor.map_or("claude-code", |e| e.provider_id.as_str());
            let session = session_on(version, provider);
            let features = intent_core::settings_file::AgentFeaturesSettings::default();
            let assembled = if remote {
                assemble_remote_system_prompt(
                    &store,
                    Some(dir),
                    "workspace",
                    None,
                    false,
                    false,
                    false,
                    &features,
                    None,
                    Some(&session),
                    None,
                )
                .await
                .map(|assembled| assembled.prompt)
            } else {
                assemble_agent_system_prompt(
                    &store,
                    Some(dir),
                    "workspace",
                    None,
                    false,
                    false,
                    false,
                    &features,
                    None,
                    Some(&session),
                    None,
                    &crate::skill_catalog::SkillEnvironment::default(),
                    executor,
                )
                .await
            };
            assembled.unwrap()
        }

        #[tokio::test]
        async fn known_good_claude_cli_gets_a_small_agents_md_once() {
            let dir = repo(&[("AGENTS.md", AGENTS)]);
            let cli = claude(Some(NEW));
            let new = prompt(dir.path(), "3.10", Some(&cli), false).await;
            assert!(!new.contains(AGENTS), "Claude Code loads it natively");
            let old = prompt(dir.path(), "3.8", Some(&cli), false).await;
            assert_eq!(
                old.matches(AGENTS).count(),
                1,
                "older harnesses keep injecting"
            );
        }

        #[tokio::test]
        async fn unknown_or_old_claude_cli_keeps_the_injection() {
            let dir = repo(&[("AGENTS.md", AGENTS)]);
            for cli in [claude(None), claude(Some(BELOW))] {
                let p = prompt(dir.path(), "3.10", Some(&cli), false).await;
                assert_eq!(p.matches(AGENTS).count(), 1, "{cli:?}");
            }
            let p = prompt(dir.path(), "3.10", None, false).await;
            assert_eq!(p.matches(AGENTS).count(), 1, "no executor");
        }

        #[tokio::test]
        async fn remote_agents_keep_the_injection() {
            let dir = repo(&[("AGENTS.md", AGENTS)]);
            for provider in ["claude-code", "codex"] {
                let exec = RuleExecutor::remote(provider);
                let p = prompt(dir.path(), "3.10", Some(&exec), true).await;
                assert_eq!(p.matches(AGENTS).count(), 1, "{provider}");
            }
        }

        #[tokio::test]
        async fn codex_injects_an_agents_md_over_its_cap() {
            let cap = crate::native_rules::CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES;
            let big = format!("{AGENTS}\n{}", "x".repeat(cap));
            let small = repo(&[("AGENTS.md", AGENTS)]);
            let large = repo(&[("AGENTS.md", &big)]);
            assert!(!prompt(small.path(), "3.10", Some(&codex()), false)
                .await
                .contains(AGENTS));
            let p = prompt(large.path(), "3.10", Some(&codex()), false).await;
            assert_eq!(p.matches(AGENTS).count(), 1);
        }

        #[tokio::test]
        async fn skipping_a_file_does_not_promote_the_next() {
            let dir = repo(&[("CLAUDE.md", CLAUDE), ("AGENTS.md", AGENTS)]);
            let cli = claude(Some(NEW));
            let p = prompt(dir.path(), "3.10", Some(&cli), false).await;
            assert!(!p.contains(CLAUDE) && !p.contains(AGENTS), "{p}");
            // Codex never loads CLAUDE.md: it stays injected, and AGENTS.md
            // (loaded by Codex) is not added.
            let p = prompt(dir.path(), "3.10", Some(&codex()), false).await;
            assert_eq!(p.matches(CLAUDE).count(), 1);
            assert!(!p.contains(AGENTS));
        }

        #[test]
        fn the_breakdown_names_each_file_and_who_loads_it() {
            let dir = repo(&[("CLAUDE.md", CLAUDE), ("AGENTS.md", AGENTS)]);
            let cx = codex();
            let rules = workspace_rules_native_aware(&crate::harness::RepoRulesInputs {
                workspace_path: dir.path(),
                executor: Some(&cx),
            });
            let rows: Vec<_> = rules
                .files
                .iter()
                .map(|f| (f.source.as_str(), f.bytes, f.loading.describe()))
                .collect();
            assert_eq!(
                rows,
                [
                    ("CLAUDE.md", CLAUDE.len(), "injected".to_owned()),
                    (
                        "AGENTS.md",
                        AGENTS.len(),
                        "loaded natively by Codex".to_owned()
                    ),
                ]
            );
            let cli = claude(Some(NEW));
            let rules = workspace_rules_native_aware(&crate::harness::RepoRulesInputs {
                workspace_path: dir.path(),
                executor: Some(&cli),
            });
            assert_eq!(rules.injected, None);
            assert_eq!(
                rules.files.iter().map(|f| f.loading).collect::<Vec<_>>(),
                [
                    RuleLoading::LoadedNatively("Claude Code"),
                    RuleLoading::NotLoaded
                ]
            );
        }

        async fn sections(
            dir: &Path,
            version: &str,
            executor: &RuleExecutor,
        ) -> crate::prompt_sections::AssembledPrompt {
            let tmp_db = TempDb::new();
            let store = Store::open(&tmp_db.path).await.unwrap();
            let session = session_on(version, &executor.provider_id);
            let features = intent_core::settings_file::AgentFeaturesSettings::default();
            assemble_system_prompt_sections(
                &store,
                Some(dir),
                "workspace",
                None,
                false,
                false,
                false,
                &features,
                None,
                Some(&session),
                None,
                &crate::skill_catalog::SkillEnvironment::default(),
                Some(executor),
            )
            .await
            .unwrap()
        }

        /// The section map names a skipped rule file as a `provider`
        /// section, outside the `systemPrompt` sum.
        #[tokio::test]
        async fn a_skipped_rule_file_is_a_provider_section() {
            use intent_core::prompt_breakdown::{utf16_len, SectionLocation};
            let dir = repo(&[("CLAUDE.md", CLAUDE), ("AGENTS.md", AGENTS)]);
            let a = sections(
                dir.path(),
                intent_core::NEWEST_HARNESS_VERSION,
                &claude(Some(NEW)),
            )
            .await;
            let native: Vec<_> = a
                .provider_sections
                .iter()
                .map(|s| {
                    (
                        s.id.as_str(),
                        s.label.as_str(),
                        s.location,
                        s.chars,
                        s.control_hint.as_ref().map(|h| h.text.as_str()),
                    )
                })
                .collect();
            assert_eq!(
                native,
                [(
                    "repo-rules",
                    "Repo rules: loaded natively by Claude Code",
                    SectionLocation::Provider,
                    utf16_len(CLAUDE),
                    Some("Edit CLAUDE.md"),
                )]
            );
            assert!(!a.sections.iter().any(|s| s.id == "repo-rules"));
            let b = a.breakdown(intent_core::NEWEST_HARNESS_VERSION, "t".into(), Vec::new());
            assert_eq!(b.system_prompt_chars, utf16_len(&a.prompt));
            let in_prompt: u64 = b
                .sections
                .iter()
                .filter(|s| s.location == SectionLocation::SystemPrompt)
                .map(|s| s.chars)
                .sum();
            assert_eq!(in_prompt, b.system_prompt_chars);
            assert_eq!(b.total_chars, b.system_prompt_chars);

            // Codex loads AGENTS.md itself; CLAUDE.md stays injected.
            let a = sections(dir.path(), intent_core::NEWEST_HARNESS_VERSION, &codex()).await;
            assert_eq!(a.provider_sections.len(), 1);
            assert_eq!(
                a.provider_sections[0].label,
                "Repo rules: loaded natively by Codex"
            );
            assert_eq!(a.provider_sections[0].chars, utf16_len(AGENTS));
            assert!(a.sections.iter().any(|s| s.id == "repo-rules"));

            // Older harnesses inject as before: nothing is loaded natively.
            let a = sections(dir.path(), "3.8", &claude(Some(NEW))).await;
            assert!(a.provider_sections.is_empty());
        }

        #[test]
        fn without_an_executor_3_10_picks_what_3_9_picks() {
            let dir = repo(&[("AGENTS.md", AGENTS)]);
            std::fs::create_dir_all(dir.path().join(".intent/rules")).unwrap();
            std::fs::write(dir.path().join(".intent/rules/a.md"), "a").unwrap();
            let inputs = crate::harness::RepoRulesInputs {
                workspace_path: dir.path(),
                executor: None,
            };
            assert_eq!(
                workspace_rules_native_aware(&inputs),
                workspace_rules_v1(&inputs)
            );
        }
    }
}
