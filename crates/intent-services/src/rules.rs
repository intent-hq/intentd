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
/// `formatUserRulesForContext`).
pub(crate) fn format_user_rules_for_context(content: &str, source: &str) -> String {
    format!(
        "## User Rules & Guidelines\n\nThe following rules and guidelines have been configured for this project. Please follow these conventions and best practices:\n\n```\n{content}\n```\n\nThese rules are loaded from: {source}"
    )
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
/// precedence: `CLAUDE.md`, `AGENTS.md`, `.augment/guidelines.md`, then every
/// `.md` under `.augment/rules/` (sorted). Each becomes a read-only entry.
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
    push_file(".augment/guidelines.md");

    let rules_dir = workspace_path.join(".augment").join("rules");
    if let Ok(entries) = std::fs::read_dir(&rules_dir) {
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
/// `CLAUDE.md` / `AGENTS.md` / `.augment/guidelines.md`), else every `.md` under
/// `.augment/rules/` joined. Returns `(content, source)` or `None`.
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
    // First single-file match (CLAUDE.md/AGENTS.md/guidelines.md) wins.
    for f in &files {
        if !f.source.starts_with(".augment/rules/") {
            return Some((f.content.clone(), f.path.clone()));
        }
    }
    // Otherwise concatenate every `.augment/rules/*.md` body.
    let parts: Vec<String> = files
        .iter()
        .filter(|f| f.source.starts_with(".augment/rules/"))
        .map(|f| strip_frontmatter(&f.content).to_string())
        .collect();
    if parts.is_empty() {
        None
    } else {
        let dir = workspace_path.join(".augment").join("rules");
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
/// 2. else a non-empty `<ws>/.augment/agent-rules/{agent_type}.md` workspace file,
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
pub(crate) async fn get_specialization_rules(
    store: &Store,
    workspace_path: Option<&Path>,
    agent_type: &str,
) -> String {
    // 1. User-settings override (highest precedence — settings win over file/bundled).
    let overrides = read_overrides(store).await;
    if let Some(c) = enabled_override(&overrides, agent_type) {
        return c;
    }
    // 2. Workspace file `<ws>/.augment/agent-rules/{agent_type}.md` (non-empty wins).
    if let Some(path) = workspace_path {
        let file = path
            .join(".augment")
            .join("agent-rules")
            .join(format!("{agent_type}.md"));
        if let Ok(content) = std::fs::read_to_string(&file) {
            if !content.trim().is_empty() {
                return content;
            }
        }
    }
    // 3. Bundled built-in (composed with common/workspace per the reference).
    crate::instructions::get_instruction_with_common(agent_type)
}

/// Assemble the effective system prompt (the **internal** injection pipeline,
/// §18.1) in documented precedence: base-system-prompt override →
/// specialization rules (the 3-tier resolver: agent-type override → workspace
/// `.augment/agent-rules/{type}.md` → bundled built-in) → workspace override →
/// live workspace rule files. The specialization slot is always populated (tier
/// 3 always resolves), so this returns `None` only in the unreachable case where
/// even the bundled specialization is empty.
pub(crate) async fn assemble_system_prompt(
    store: &Store,
    workspace_path: Option<&Path>,
    agent_type: &str,
) -> Option<String> {
    let overrides = read_overrides(store).await;
    let mut parts: Vec<String> = Vec::new();
    if let Some(c) = enabled_override(&overrides, "base-system-prompt") {
        parts.push(c);
    }
    let specialization = get_specialization_rules(store, workspace_path, agent_type).await;
    if !specialization.trim().is_empty() {
        parts.push(specialization);
    }
    if let Some(c) = enabled_override(&overrides, "workspace") {
        parts.push(c);
    }
    if let Some(path) = workspace_path {
        if let Some((content, source)) = load_workspace_rules(path, None) {
            if !content.trim().is_empty() {
                parts.push(format_user_rules_for_context(&content, &source));
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n---\n\n"))
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
