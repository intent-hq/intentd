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

use crate::rtk;

/// Settings-store key the per-rule-type user overrides persist under (§9.12).
const END_USER_RULES_KEY: &str = "endUserRules";

/// Upper bound on a single override body, matching the FE editor cap
/// (`AgentRulesEditor.svelte` `MAX_RULES_LENGTH`).
const MAX_RULE_CONTENT_LEN: usize = 50_000;

/// Current epoch-ms (the wire `updatedAt` unit per PROTOCOL §5.21).
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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

/// Resolve the single live workspace rule source for prompt injection — the
/// first match in `rules-loader.ts` precedence (custom `--rules` path then
/// `CLAUDE.md` / `AGENTS.md` / `.intent/guidelines.md` / `.augment/guidelines.md`),
/// else every `.md` under `.intent/rules/` and `.augment/rules/` joined.
/// Returns `(content, source)` or `None`.
fn load_workspace_rules(
    workspace_path: &Path,
    custom_rules_path: Option<&Path>,
) -> Option<(String, String)> {
    if let Some(custom) = custom_rules_path {
        if let Ok(content) = std::fs::read_to_string(custom) {
            return Some((content, custom.to_string_lossy().into_owned()));
        }
    }
    let files = list_workspace_rule_files(workspace_path);
    // First single-file match (CLAUDE.md/AGENTS.md/.intent or .augment guidelines) wins.
    for f in &files {
        if !f.source.starts_with(".intent/rules/") && !f.source.starts_with(".augment/rules/") {
            return Some((f.content.clone(), f.path.clone()));
        }
    }
    // Otherwise concatenate every `.intent/rules/*.md` and `.augment/rules/*.md` body.
    let parts: Vec<String> = files
        .iter()
        .filter(|f| {
            f.source.starts_with(".intent/rules/") || f.source.starts_with(".augment/rules/")
        })
        .map(|f| strip_frontmatter(&f.content).to_string())
        .collect();
    if parts.is_empty() {
        None
    } else {
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
/// workspace` via the reference's `fallbackToWorkspace` path. The FE file-watch
/// cache-invalidation around the workspace file is intentionally not ported — the
/// daemon re-resolves per spawn, so the file is always read fresh.
///
/// `agent_features` gates feature-specific sections of the tier-3 bundled
/// bodies only; tier 1/2 user-supplied content is never filtered.
///
/// Latest-bound convenience form kept for unit tests; prompt assembly
/// resolves the session's pinned set via [`get_specialization_rules_for`].
#[cfg(test)]
pub(crate) async fn get_specialization_rules(
    store: &Store,
    workspace_path: Option<&Path>,
    agent_type: &str,
    agent_features: &intent_core::settings_file::AgentFeaturesSettings,
) -> String {
    get_specialization_rules_for(
        crate::harness::latest_entry().doctrine.instructions,
        store,
        workspace_path,
        agent_type,
        agent_features,
    )
    .await
}

/// [`get_specialization_rules`] with an explicit tier-3 instruction set —
/// the SESSION's pinned doctrine when called from [`assemble_system_prompt`]
/// (H2): tier 1/2 user-supplied content is unversioned, only the bundled
/// built-in comes from `set`.
pub(crate) async fn get_specialization_rules_for(
    set: &'static crate::instructions::InstructionSet,
    store: &Store,
    workspace_path: Option<&Path>,
    agent_type: &str,
    agent_features: &intent_core::settings_file::AgentFeaturesSettings,
) -> String {
    // 1. User-settings override (highest precedence — settings win over file/bundled).
    let overrides = read_overrides(store).await;
    if let Some(c) = enabled_override(&overrides, agent_type) {
        return c;
    }
    // 2. Workspace file `<ws>/.intent/agent-rules/{agent_type}.md` (non-empty wins).
    if let Some(path) = workspace_path {
        let file = path
            .join(".intent")
            .join("agent-rules")
            .join(format!("{agent_type}.md"));
        if let Ok(content) = std::fs::read_to_string(&file) {
            if !content.trim().is_empty() {
                return content;
            }
        }
    }
    // 3. Bundled built-in (composed with common/workspace per the reference),
    // from the caller's pinned instruction set.
    crate::instructions::get_instruction_with_common_for(set, agent_type, agent_features)
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

/// Build the mode-dependent isolation hint for Task 6 (CoW agent sandboxes).
/// Returns `Some(hint)` when the agent's isolation mode and specialist warrant
/// a context block, `None` otherwise. The hint selection keys off the agent's
/// actual effective isolation (session.sandbox_path presence) and workspace mode,
/// not just the workspace cowIsolation setting, so it reflects what the agent is
/// actually running under.
///
/// Hint matrix (per spec line 104-110):
/// - Sandboxed implementor (session.sandbox_path present + specialist="implementor"):
///   isolation context block with sandbox path, branch, base commit, caches-warm
///   notice, branch-switching warning, and conflict-bounce resolution instructions.
/// - Coordinator in CoW-enabled workspace (specialist="spec-writer" + workspace
///   direct-mode + cow_supported=true): parallel delegation safety guidance.
/// - All other modes: no hint (worktree-mode unchanged, shared-mode direct unchanged).
pub(crate) fn build_isolation_hint(
    workspace: Option<&intent_core::Workspace>,
    agent_session: Option<&intent_core::AgentSession>,
    specialist: Option<&SpecialistPromptInjection>,
) -> Option<String> {
    // Determine if this agent is a sandboxed implementor
    let is_sandboxed = agent_session
        .and_then(|s| s.sandbox_path.as_ref())
        .is_some();

    let specialist_name = specialist
        .and_then(|s| s.specialist_name.as_deref())
        .unwrap_or("");

    // The session's pinned harness owns the hint wording (H2); session-less
    // calls resolve to the latest.
    let harness = session_harness_entry(agent_session).harness;

    // Case 1: Sandboxed implementor — inject isolation context
    if is_sandboxed && specialist_name.eq_ignore_ascii_case("implementor") {
        let session = agent_session?;
        let sandbox_path = session.sandbox_path.as_deref().unwrap_or("<sandbox-path>");
        let sandbox_branch = session.sandbox_branch.as_deref().unwrap_or("sb/<id>");
        return Some(harness.sandboxed_implementor_hint(sandbox_path, sandbox_branch));
    }

    // Case 2: Coordinator in CoW-enabled direct-mode workspace
    // "spec-writer" is the coordinator specialist (per SPECIALISTS constant in FE)
    if specialist_name.eq_ignore_ascii_case("coordinator")
        || specialist_name.eq_ignore_ascii_case("spec-writer")
    {
        if let Some(ws) = workspace {
            // Direct mode: skip_worktree=true OR worktree_path=None
            let is_direct_mode = ws.skip_worktree || ws.worktree_path.is_none();
            let cow_supported = ws.cow_supported.unwrap_or(false);

            if is_direct_mode && cow_supported {
                return Some(harness.coordinator_cow_hint());
            }
        }
    }

    // Case 3: Worktree mode or shared-mode direct — no hint (behavior unchanged)
    None
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

/// Assemble the effective system prompt (the **internal** injection pipeline,
/// §18.1) in documented precedence: base-system-prompt override →
/// specialization rules (the 3-tier resolver: agent-type override → workspace
/// `.augment/agent-rules/{type}.md` → bundled built-in) → workspace override →
/// live workspace rule files → mode-dependent isolation hints (Task 6: CoW
/// sandboxing context for implementors, parallel delegation safety for
/// coordinators when CoW is enabled) → specialist role section (PP-1, reference
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
/// [`get_specialization_rules_for`], and the `## Asking the User Questions`
/// footer block (`structuredQuestions`). With all defaults on the assembled
/// prompt is byte-identical to the ungated one.
///
/// H2 (intent-hq/monorepo#2459): every harness-owned surface and the tier-3
/// bundled doctrine resolve through the SESSION's stamped `harnessVersion`
/// (via the harness registry), so an existing session keeps the exact prompt
/// text it was created with even after the binary ships a newer doctrine set;
/// a `None` session resolves to the latest.
#[allow(clippy::too_many_arguments)]
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
) -> Option<String> {
    // The session's pinned harness + doctrine (H2): a stamped session keeps
    // assembling the exact version it was created with; session-less calls
    // (previews, background one-shots) resolve to the latest.
    let entry = session_harness_entry(agent_session);
    let harness = entry.harness;
    let overrides = read_overrides(store).await;
    let mut parts: Vec<String> = Vec::new();
    if let Some(c) = enabled_override(&overrides, "base-system-prompt") {
        parts.push(c);
    }
    let specialization = get_specialization_rules_for(
        entry.doctrine.instructions,
        store,
        workspace_path,
        agent_type,
        agent_features,
    )
    .await;
    if !specialization.trim().is_empty() {
        parts.push(specialization);
    }
    if let Some(c) = enabled_override(&overrides, "workspace") {
        parts.push(c);
    }
    if let Some(path) = workspace_path {
        if let Some((content, source)) = load_workspace_rules(path, None) {
            if !content.trim().is_empty() {
                parts.push(harness.user_rules_wrapper(&content, &source));
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
                    parts.push(harness.user_rules_wrapper(&instructions, &source));
                }
            }
        }
    }
    // RTK layer: when rtk.enabled is true and rtk is detected with ≥1 usable
    // subcommand, append the instruction line (worded by the session's pinned
    // harness). Placed after workspace-rules, before skills / isolation hint /
    // specialist role.
    if let Some(rtk_instruction) = build_rtk_instruction(harness, rtk_enabled).await {
        parts.push(rtk_instruction);
    }
    // Skills catalog layer (reference layer 4.7: after specialization rules, user
    // rules, and skills — before isolation hint / specialist role). When a
    // workspace path is available, discover and inject the skills catalog. Empty
    // catalog ⇒ no layer appended. Discovery failures degrade gracefully (log
    // warn, omit layer) — never fail prompt assembly.
    if let Some(ws) = workspace {
        if let Some(repo_path) = crate::git_ops::worktree_path(ws) {
            match crate::skills::format_skills_catalog_for_prompt(&repo_path.to_string_lossy())
                .await
            {
                catalog if !catalog.trim().is_empty() => {
                    parts.push(catalog);
                }
                _ => {}
            }
        }
    }
    // Mode-dependent isolation hints (Task 6): inject context about CoW
    // sandboxing for implementors and parallel delegation safety for coordinators
    // when appropriate, before the specialist role section so the specialist
    // behavior prompt can reference them.
    if let Some(hint) = build_isolation_hint(workspace, agent_session, specialist) {
        parts.push(hint);
    }
    // Specialist role section (reference layer 4.8: after specialization
    // rules, user rules, and skills — before the parent-only layers).
    if let Some(bp) = specialist
        .and_then(|s| s.behavior_prompt.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        parts.push(harness.specialist_role_section(bp));
    }
    // Commit-policy layer: one status-neutral clause, injected for every agent
    // (top-level and sub-agents alike) regardless of the auto-commit state.
    // The prompt no longer branches on the effective auto-commit state — the
    // OFF-state gate in `git_ops` and the auto-commit-on-idle subscriber
    // enforce the actual behavior.
    parts.push(harness.commit_policy_clause());
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
        parts.push(harness.role_reminder_footer(name, reminder));
    }
    // Asking the User Questions + Suggested Next Steps — top-level
    // interactive agents only. Sub-agents don't own a user-facing chat turn
    // (they report to a parent), so they skip both blocks, matching the
    // reference gating. The questions block is additionally gated by
    // `agentFeatures.structuredQuestions` (spec audit row 8).
    if !is_sub_agent {
        if agent_features.structured_questions {
            parts.push(harness.ask_questions_block());
        }
        // Per-session effective state for the SP-1 footer wording: a session
        // that opted out via `skipAutoCommit` (delegation/creation while the
        // workspace was OFF) never auto-commits, so the footer must follow the
        // effective state even when the workspace toggle is currently on. The
        // commit-policy clause above is deliberately status-neutral.
        let effective_auto_commit =
            auto_commit_enabled && !agent_session.map(|s| s.skip_auto_commit).unwrap_or(false);
        parts.push(harness.suggested_next_steps_block(effective_auto_commit));
    }
    if parts.is_empty() {
        None
    } else {
        Some(harness.join_prompt_layers(&parts))
    }
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
    fn make_test_workspace(repo_path: PathBuf) -> Workspace {
        let ts = intent_core::now_iso();
        Workspace {
            id: intent_core::WorkspaceId("test-ws".to_string()),
            title: "Test Workspace".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: intent_core::WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: intent_core::WorkspaceActivity::Idle,
            attention: intent_core::WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            tags: vec![],
            path: Some(repo_path.to_string_lossy().to_string()),
            repository_path: Some(repo_path.to_string_lossy().to_string()),
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
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
        let workspace = make_test_workspace(repo_path.to_path_buf());

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
        let workspace = make_test_workspace(repo_path.to_path_buf());

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
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
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
}
