//! File-backed specialist definitions (§18.2, PROTOCOL §5.11): markdown-with-
//! frontmatter files resolved in **3 tiers**, highest priority first — project
//! (`<workspacePath>/.intent/specialists/`), then user
//! (`~/.intent/specialists/`), then bundled (`resources/specialists/`,
//! read-only). Ports `specialist-file-loader.ts` + `specialists.ipc.ts`'s
//! combined load. Nothing is persisted in `SQLite`; `create`/`edit`/`delete`
//! write user/project files only and `bundled` definitions are read-only.
//! [`REPLACEMENT_DIR_ENV`] (startup-pinned) wholesale-replaces the base tier
//! with an operator-supplied directory, excluding the embedded bundle and the
//! bundled directory entirely.

use std::path::{Path, PathBuf};

use intent_core::{Error, Result};
use serde_json::{json, Map, Value};

/// Folder name under `.intent/` (and the bundled `resources/`) holding files.
const SPECIALISTS_FOLDER: &str = "specialists";
/// Env override for the bundled (read-only) specialists directory; lets the
/// daemon and tests point at app-shipped resources hermetically.
const BUNDLED_DIR_ENV: &str = "INTENTD_BUNDLED_SPECIALISTS_DIR";
/// Startup-pinned env var that wholesale-REPLACES the base specialist tier:
/// when set to a non-empty path, the embedded bundle and the bundled
/// directory ([`BUNDLED_DIR_ENV`]/exe-relative) are both excluded and this
/// directory becomes the sole base (`bundled`, read-only) tier — shipped ids
/// resolve only if present here or in the user/project tiers, which fold on
/// top unchanged. A missing or empty directory yields an empty base tier.
/// Reported as a settings pin (`specialists.dir`) like the other `INTENTD_*`
/// env pins; the empty string counts as unset (no replacement).
const REPLACEMENT_DIR_ENV: &str = "INTENTD_SPECIALISTS_DIR";

/// The reference specialist definitions embedded at compile time (PP-2,
/// byte-identical to the reference bundle, kept under the versioned
/// `resources/specialists/v1/` layout — H2, intent-hq/monorepo#2459). They
/// form the floor of the bundled tier so daemon-side resolution works with
/// zero local files; an on-disk bundled file (env override / packaged
/// resources) with the same id still wins over the embedded copy. The v1
/// harness doctrine (`crate::harness::v1::ENTRY`) points at this same set.
pub(crate) const EMBEDDED_BUNDLED_V1: &[(&str, &str)] = &[
    (
        "chief-of-staff",
        include_str!("../resources/specialists/v1/chief-of-staff.md"),
    ),
    (
        "developer",
        include_str!("../resources/specialists/v1/developer.md"),
    ),
    (
        "implementor",
        include_str!("../resources/specialists/v1/implementor.md"),
    ),
    (
        "pr-reviewer",
        include_str!("../resources/specialists/v1/pr-reviewer.md"),
    ),
    (
        "ralph",
        include_str!("../resources/specialists/v1/ralph.md"),
    ),
    (
        "spec-writer",
        include_str!("../resources/specialists/v1/spec-writer.md"),
    ),
    (
        "ui-designer",
        include_str!("../resources/specialists/v1/ui-designer.md"),
    ),
    (
        "verifier",
        include_str!("../resources/specialists/v1/verifier.md"),
    ),
];

/// The v1.1 embedded specialist bundle (`resources/specialists/v1.1/`):
/// the v1 files with body-identical prompts (the v1→v1.1 doctrine diff is
/// instruction-only — the feature-section rewrites in `common.md`) plus the
/// picker-metadata frontmatter keys (`role`/`teamAgents`/`icon`), kept as
/// a separate directory so each version's resources stay self-contained.
/// The v1.1 harness doctrine (`crate::harness::v1_1::ENTRY`) points here.
pub(crate) const EMBEDDED_BUNDLED_V1_1: &[(&str, &str)] = &[
    (
        "chief-of-staff",
        include_str!("../resources/specialists/v1.1/chief-of-staff.md"),
    ),
    (
        "developer",
        include_str!("../resources/specialists/v1.1/developer.md"),
    ),
    (
        "implementor",
        include_str!("../resources/specialists/v1.1/implementor.md"),
    ),
    (
        "pr-reviewer",
        include_str!("../resources/specialists/v1.1/pr-reviewer.md"),
    ),
    (
        "ralph",
        include_str!("../resources/specialists/v1.1/ralph.md"),
    ),
    (
        "spec-writer",
        include_str!("../resources/specialists/v1.1/spec-writer.md"),
    ),
    (
        "ui-designer",
        include_str!("../resources/specialists/v1.1/ui-designer.md"),
    ),
    (
        "verifier",
        include_str!("../resources/specialists/v1.1/verifier.md"),
    ),
];

/// The v2.1 embedded specialist bundle: the frozen v1.1 definitions plus the
/// bundled Vulnerability Scanner. Unchanged specialist bytes keep reusing the
/// v1.1 resources; the new definition lives under `resources/specialists/v2.1/`.
/// Harness 2.0 remains pinned to [`EMBEDDED_BUNDLED_V1_1`], while 2.1 sessions
/// resolve this extended set.
pub(crate) const EMBEDDED_BUNDLED_V2_1: &[(&str, &str)] = &[
    (
        "chief-of-staff",
        include_str!("../resources/specialists/v1.1/chief-of-staff.md"),
    ),
    (
        "developer",
        include_str!("../resources/specialists/v1.1/developer.md"),
    ),
    (
        "implementor",
        include_str!("../resources/specialists/v1.1/implementor.md"),
    ),
    (
        "pr-reviewer",
        include_str!("../resources/specialists/v1.1/pr-reviewer.md"),
    ),
    (
        "ralph",
        include_str!("../resources/specialists/v1.1/ralph.md"),
    ),
    (
        "spec-writer",
        include_str!("../resources/specialists/v1.1/spec-writer.md"),
    ),
    (
        "ui-designer",
        include_str!("../resources/specialists/v1.1/ui-designer.md"),
    ),
    (
        "verifier",
        include_str!("../resources/specialists/v1.1/verifier.md"),
    ),
    (
        "vulnerability-scanner",
        include_str!("../resources/specialists/v2.1/vulnerability-scanner.md"),
    ),
];

/// The embedded bundled floor the specialist 3-tier resolution uses by
/// default — the LATEST version's set (the file tiers above it are
/// user-owned and unversioned). Session-scoped resolution swaps in the
/// session's pinned bundle via [`SpecialistsService::with_embedded`] (H2).
const EMBEDDED_BUNDLED: &[(&str, &str)] = EMBEDDED_BUNDLED_V2_1;

/// The empty embedded floor used when [`REPLACEMENT_DIR_ENV`] replaces the
/// base tier: no shipped specialist survives the replacement.
const EMPTY_BUNDLE: &[(&str, &str)] = &[];

/// Parse a raw [`REPLACEMENT_DIR_ENV`] value: a non-empty path is the
/// replacement base-tier directory; unset/empty means no replacement.
fn replacement_dir(raw: Option<std::ffi::OsString>) -> Option<PathBuf> {
    raw.map(PathBuf::from).filter(|p| !p.as_os_str().is_empty())
}

/// Resolve an embedded bundled specialist by id from `bundle` (the lowest
/// tier).
fn load_embedded(bundle: &[(&str, &str)], id: &str) -> Option<Value> {
    bundle
        .iter()
        .find(|(k, _)| *k == id)
        .map(|(k, content)| build_def(k, content, "bundled", Path::new("")))
}

/// Resolve the user's home directory from the environment (cross-platform).
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

/// Default user specialists directory (`~/.intent/specialists/`).
fn default_user_dir() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".intent").join(SPECIALISTS_FOLDER))
}

/// Default bundled specialists directory: the [`BUNDLED_DIR_ENV`] override if
/// set, else `resources/specialists/` next to the running executable.
/// Deliberately UNVERSIONED (no `v1/` segment): this on-disk tier is an
/// install-owned override layer (like the user/project tiers), not a copy of
/// the repo's versioned `resources/specialists/<ver>/` source layout — the
/// versioned bundles ship embedded via `include_str!`, and no packaging step
/// places files here.
fn default_bundled_dir() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os(BUNDLED_DIR_ENV) {
        let p = PathBuf::from(p);
        if !p.as_os_str().is_empty() {
            return Some(p);
        }
    }
    std::env::current_exe().ok().and_then(|e| {
        e.parent()
            .map(|d| d.join("resources").join(SPECIALISTS_FOLDER))
    })
}

/// The project specialists directory for a worktree.
fn project_dir(workspace_path: &Path) -> PathBuf {
    workspace_path.join(".intent").join(SPECIALISTS_FOLDER)
}

/// Reject ids that are empty or would escape the specialists directory; ids map
/// 1:1 to `<id>.md` filenames so they must be a single safe path component.
fn validate_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id == "."
        || id == ".."
        || id.contains('/')
        || id.contains('\\')
        || id.contains(std::path::MAIN_SEPARATOR)
    {
        return Err(Error::InvalidParams(format!(
            "invalid specialist id: {id:?}"
        )));
    }
    Ok(())
}

/// Parse a `scope` string into a writable tier; `bundled`/unknown are rejected
/// (bundled is read-only, PROTOCOL §5.11).
fn parse_scope(scope: &str) -> Result<&'static str> {
    match scope {
        "project" => Ok("project"),
        "user" => Ok("user"),
        "bundled" => Err(Error::InvalidParams(
            "bundled specialists are read-only".to_string(),
        )),
        other => Err(Error::InvalidParams(format!("invalid scope: {other:?}"))),
    }
}

/// Escape a string for a double-quoted YAML frontmatter scalar (port of
/// `escapeYamlValue`).
fn escape_yaml(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\t', "\\t")
}

/// Reverse [`escape_yaml`] for a double-quoted scalar. Decoded in a single
/// left-to-right pass so an escaped backslash never re-combines with the
/// following character (sequential `replace` calls corrupted `foo\nbar` —
/// literal backslash + `n` — into backslash + real newline); an unrecognized
/// escape is carried verbatim (lenient, like the rest of the parser).
fn unescape_yaml(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('"') => out.push('"'),
            Some('\'') => out.push('\''),
            // A trailing lone backslash is carried verbatim, like `\\`.
            Some('\\') | None => out.push('\\'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
        }
    }
    out
}

/// Split optional leading `---`-delimited YAML frontmatter from the markdown
/// body (port of `parseFrontmatter`). Returns `(frontmatter, body)`; when there
/// is no valid frontmatter block the whole content is the body.
pub(crate) fn parse_frontmatter(content: &str) -> (Map<String, Value>, String) {
    let norm = content.replace("\r\n", "\n");
    let mut lines = norm.split('\n');
    if lines.next().map(str::trim) != Some("---") {
        return (Map::new(), norm.trim().to_string());
    }
    let mut fm_lines: Vec<&str> = Vec::new();
    let mut found_end = false;
    let mut body_lines: Vec<&str> = Vec::new();
    for line in lines {
        if found_end {
            body_lines.push(line);
        } else if line.trim() == "---" {
            found_end = true;
        } else {
            fm_lines.push(line);
        }
    }
    if !found_end {
        return (Map::new(), norm.trim().to_string());
    }
    let mut fm = Map::new();
    for line in fm_lines {
        let Some(colon) = line.find(':') else {
            continue;
        };
        let key = line[..colon].trim();
        if key.is_empty() || RETIRED_FRONTMATTER_KEYS.contains(&key) {
            continue;
        }
        let mut value = line[colon + 1..].trim().to_string();
        if value.len() >= 2
            && ((value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\'')))
        {
            let quote = value.as_bytes()[0];
            value = value[1..value.len() - 1].to_string();
            if quote == b'"' {
                value = unescape_yaml(&value);
            }
        }
        fm.insert(key.to_string(), Value::String(value));
    }
    (fm, body_lines.join("\n").trim().to_string())
}

/// Optional frontmatter scalar keys carried through `build_def`/`render_file`
/// verbatim so parse→write→parse round-trips losslessly (port of
/// `SpecialistFileFrontmatter`'s optional fields: `codingAgent`, `model`,
/// `roleReminder`, `agentType`, plus `reasoningEffort`, `role` and `icon`).
///
/// NOTE: the config scalars `codingAgent`/`model`/`agentType`/
/// `reasoningEffort`/`role`/`icon` ([`INHERITED_CONFIG_KEYS`]) resolve with
/// inherit-on-omit semantics across tiers, like `hidden` (PROTOCOL §5.11,
/// intent-hq/monorepo#718): an omitted key keeps the lower tiers' effective
/// value, an explicit empty value (`model: ""`) clears it, and an explicit
/// non-empty value overrides it.
/// `role` is the picker-orchestration enum (`orchestrator` | `internal`;
/// absent = standard) and `icon` names a client-side avatar design. `icon`
/// is render-only picker metadata; `role` additionally gates the spawn-time
/// orchestrator tool denylist (§18.4,
/// [`SpecialistsService::resolve_is_orchestrator`]) but is still never
/// consulted at delegation time.
/// `role` is validated on `specialist.create`/`edit` ([`validate_role_spec`])
/// but read leniently — an out-of-enum on-disk value is normalized to
/// omitted (which inherits), so `list`/`get` never serve a value the strict
/// write validation would reject when a client echoes the def back.
/// `roleReminder` stays winner-takes-all — it is carried through only when
/// present in the winning file (an omitted key falls back to auto-derivation
/// from the winning body, so inheriting a lower tier's reminder would pin a
/// stale summary of a body that no longer exists).
const OPTIONAL_FRONTMATTER_KEYS: &[&str] = &[
    "codingAgent",
    "model",
    "reasoningEffort",
    "roleReminder",
    "agentType",
    "role",
    "icon",
];

/// The subset of [`OPTIONAL_FRONTMATTER_KEYS`] with inherit-on-omit semantics
/// across tiers; each key inherits independently.
const INHERITED_CONFIG_KEYS: &[&str] = &[
    "codingAgent",
    "model",
    "reasoningEffort",
    "agentType",
    "role",
    "icon",
];

/// Wire/frontmatter values accepted for the `role` enum on write (PROTOCOL
/// §5.11): `orchestrator` (powers the team-mode card), `internal` (excluded
/// from the New Workspace modal's single-agent picker only), or the
/// explicit-clear empty string.
const ROLE_VALUES: &[&str] = &["orchestrator", "internal", ""];

/// The tier-folded resolution state of a specialist's `role` frontmatter
/// ([`SpecialistsService::resolve_role_state`]): the wire def drops the
/// explicit `role: ""` clear (it folds to an absent key), but the
/// orchestrator gate needs to tell that deliberate clear apart from a role
/// that was never set (fail-closed historical-name fallback, §18.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RoleResolution {
    /// The highest role-bearing tier set a non-empty in-enum role.
    Role(String),
    /// The highest role-bearing tier wrote the explicit `role: ""` clear.
    Cleared,
    /// The specialist resolves but no tier ever touched the key (an
    /// out-of-enum on-disk value reads as untouched, like the lenient wire
    /// normalization in [`build_def_inheriting`]).
    Absent,
    /// The id does not resolve to any specialist.
    Unknown,
}

/// Fold one tier's raw frontmatter into a role resolution `state`
/// ([`SpecialistsService::resolve_role_state`]): a valid in-enum `role`
/// value overrides (empty ⇒ [`RoleResolution::Cleared`], non-empty ⇒
/// [`RoleResolution::Role`]); an omitted or out-of-enum value leaves the
/// lower tiers' state untouched — the same per-key inherit-on-omit /
/// lenient-read semantics as [`build_def_inheriting`].
fn fold_role_directive(state: &mut RoleResolution, content: &str) {
    let (fm, _) = parse_frontmatter(content);
    if let Some(v) = fm
        .get("role")
        .and_then(Value::as_str)
        .filter(|v| ROLE_VALUES.contains(v))
    {
        *state = if v.is_empty() {
            RoleResolution::Cleared
        } else {
            RoleResolution::Role(v.to_string())
        };
    }
}

/// The picker/routing-metadata frontmatter keys (PROTOCOL §5.11) — the only
/// frontmatter allowed to diverge between the v1 and v1.1 bundled specialist
/// copies (none of them reach assembled prompt bytes, so the v1 doctrine
/// stays frozen). Consumed only by the cross-version goldens
/// (`v1_1_goldens`, `harness::tests`), which compare frontmatter modulo this
/// set — hence the allow: the lib build has no reader.
#[allow(dead_code)]
pub(crate) const PICKER_METADATA_KEYS: &[&str] = &["role", "icon", TEAM_AGENTS_KEY, ALIASES_KEY];

/// Strictly validate a wire `role` value (`specialist.create`/`edit` specs):
/// when present it must be a string in [`ROLE_VALUES`] (`""` is the
/// explicit clear); anything else → `-32602`. Files are read leniently — an
/// out-of-enum on-disk value is normalized to omitted by
/// [`build_def_inheriting`] (like an unparseable `teamAgents`), never served.
fn validate_role_spec(value: Option<&Value>) -> Result<()> {
    let Some(value) = value else { return Ok(()) };
    match value.as_str() {
        Some(s) if ROLE_VALUES.contains(&s) => Ok(()),
        _ => Err(Error::InvalidParams(
            "role must be \"orchestrator\", \"internal\", or \"\"".to_string(),
        )),
    }
}

/// Strictly validate a wire `icon` value (`specialist.create`/`edit` specs):
/// when present it must be a string (`""` is the explicit clear); any other
/// JSON type → `-32602`. Icon names are free-form (they name client-side
/// avatar designs), so no enum is enforced.
fn validate_icon_spec(value: Option<&Value>) -> Result<()> {
    match value {
        None | Some(Value::String(_)) => Ok(()),
        Some(_) => Err(Error::InvalidParams("icon must be a string".to_string())),
    }
}

/// Frontmatter/wire key for the ordered list of delegation model options —
/// `{ model, hint, reasoningEffort? }` entries a delegating agent can pick
/// from (PROTOCOL §5.11).
/// Encoded in frontmatter as a **single-line JSON-array scalar** (e.g.
/// `modelOptions: [{"model":"opencode:kimi-k3","hint":"cheap"}]`) so it fits
/// the line-based parser and round-trips losslessly. Resolution follows the
/// same inherit-on-omit fold as [`INHERITED_CONFIG_KEYS`]: an omitted key
/// inherits the lower tiers' effective list, an explicit `[]` clears it, and
/// a non-empty list overrides it wholesale (entries never merge across tiers).
const MODEL_OPTIONS_KEY: &str = "modelOptions";

/// Normalize one `modelOptions` entry to its documented fields, or `None` when
/// the entry is unusable: `model` must be a non-empty (non-whitespace) string;
/// `hint` is carried when it is a string and defaults to `""` otherwise;
/// `reasoningEffort` is carried only when it is a non-empty string (the
/// per-option effort level, PROTOCOL §5.11) and omitted otherwise.
fn normalize_model_option_entry(entry: &Value) -> Option<Value> {
    let obj = entry.as_object()?;
    let model = obj
        .get("model")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())?;
    let hint = obj.get("hint").and_then(Value::as_str).unwrap_or("");
    let mut out = json!({ "model": model, "hint": hint });
    if let Some(effort) = obj
        .get("reasoningEffort")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
    {
        out["reasoningEffort"] = json!(effort);
    }
    Some(out)
}

/// Strictly validate a wire `modelOptions` value (`specialist.create`/`edit`
/// specs): must be a JSON array of `{ model, hint?, reasoningEffort? }`
/// objects with a non-empty string `model`, a string `hint` (defaulting to
/// `""` when absent), and — when present — a string `reasoningEffort`.
/// Returns the normalized entries in input order (`None` when the key is
/// absent — the inherit-on-omit case); any invalid shape → `-32602`.
fn validate_model_options_spec(value: Option<&Value>) -> Result<Option<Vec<Value>>> {
    let Some(value) = value else { return Ok(None) };
    let Some(arr) = value.as_array() else {
        return Err(Error::InvalidParams(
            "modelOptions must be an array of { model, hint } objects".to_string(),
        ));
    };
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        if !entry.is_object() {
            return Err(Error::InvalidParams(
                "modelOptions entries must be { model, hint } objects".to_string(),
            ));
        }
        match entry.get("hint") {
            None | Some(Value::String(_)) => {}
            Some(_) => {
                return Err(Error::InvalidParams(
                    "modelOptions entry hint must be a string".to_string(),
                ));
            }
        }
        match entry.get("reasoningEffort") {
            None | Some(Value::String(_)) => {}
            Some(_) => {
                return Err(Error::InvalidParams(
                    "modelOptions entry reasoningEffort must be a string".to_string(),
                ));
            }
        }
        let Some(normalized) = normalize_model_option_entry(entry) else {
            return Err(Error::InvalidParams(
                "modelOptions entry model must be a non-empty string".to_string(),
            ));
        };
        out.push(normalized);
    }
    Ok(Some(out))
}

/// Leniently parse a frontmatter `modelOptions` scalar (the single-line
/// JSON-array string). Files are never rejected on read: an unparseable value
/// or a non-array yields `None` (treated like an omitted key, which inherits),
/// and invalid entries are skipped individually. Only a literal `[]` is the
/// explicit clear (`Some(vec![])`); a non-empty array whose entries are all
/// unusable also yields `None`, so one bad hand-authored entry does not
/// silently drop an inherited list.
fn parse_model_options_frontmatter(raw: &str) -> Option<Vec<Value>> {
    let parsed: Value = serde_json::from_str(raw.trim()).ok()?;
    let arr = parsed.as_array()?;
    let normalized: Vec<Value> = arr
        .iter()
        .filter_map(normalize_model_option_entry)
        .collect();
    if normalized.is_empty() && !arr.is_empty() {
        return None;
    }
    Some(normalized)
}

/// A lenient frontmatter parser for a single-line JSON-array scalar key
/// ([`parse_model_options_frontmatter`] / [`parse_team_agents_frontmatter`]).
type FrontmatterListParser = fn(&str) -> Option<Vec<Value>>;

/// Frontmatter/wire key for the orchestrator's advisory team roster — the
/// specialist ids it delegates to (PROTOCOL §5.11), used by clients to render
/// the team-mode card; never enforced at delegation time.
/// Encoded in frontmatter as a **single-line JSON-array scalar** (e.g.
/// `teamAgents: ["implementor","verifier"]`) so it fits the line-based
/// parser and round-trips losslessly. Resolution follows the same
/// inherit-on-omit fold as [`MODEL_OPTIONS_KEY`]: an omitted key inherits the
/// lower tiers' effective list, an explicit `[]` clears it, and a non-empty
/// list overrides it wholesale (entries never merge across tiers).
const TEAM_AGENTS_KEY: &str = "teamAgents";

/// Strictly validate a wire `teamAgents` value (`specialist.create`/`edit`
/// specs): must be a JSON array of non-empty (non-whitespace) strings.
/// Returns the entries in input order (`None` when the key is absent — the
/// inherit-on-omit case); any invalid shape → `-32602`.
fn validate_team_agents_spec(value: Option<&Value>) -> Result<Option<Vec<Value>>> {
    let Some(value) = value else { return Ok(None) };
    let Some(arr) = value.as_array() else {
        return Err(Error::InvalidParams(
            "teamAgents must be an array of specialist-id strings".to_string(),
        ));
    };
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        match entry.as_str() {
            Some(s) if !s.trim().is_empty() => out.push(json!(s)),
            _ => {
                return Err(Error::InvalidParams(
                    "teamAgents entries must be non-empty strings".to_string(),
                ));
            }
        }
    }
    Ok(Some(out))
}

/// Leniently parse a frontmatter `teamAgents` scalar (the single-line
/// JSON-array string), mirroring [`parse_model_options_frontmatter`]: an
/// unparseable value or a non-array yields `None` (treated like an omitted
/// key, which inherits), and unusable entries (non-strings, empty strings)
/// are skipped individually. Only a literal `[]` is the explicit clear
/// (`Some(vec![])`); a non-empty array whose entries are all unusable also
/// yields `None`, so a bad hand-authored entry does not silently drop an
/// inherited list.
fn parse_team_agents_frontmatter(raw: &str) -> Option<Vec<Value>> {
    let parsed: Value = serde_json::from_str(raw.trim()).ok()?;
    let arr = parsed.as_array()?;
    let normalized: Vec<Value> = arr
        .iter()
        .filter_map(|e| {
            e.as_str()
                .filter(|s| !s.trim().is_empty())
                .map(|s| json!(s))
        })
        .collect();
    if normalized.is_empty() && !arr.is_empty() {
        return None;
    }
    Some(normalized)
}

/// Frontmatter/wire key for a specialist's alternate ids (PROTOCOL §5.11):
/// spawn/delegation callers may address the specialist by any listed alias,
/// and resolution maps the alias to this canonical definition — the CANONICAL
/// id is what gets persisted on created sessions (`metadata.specialist`), so
/// downstream consumers keying on the id never see an alias. Encoded in
/// frontmatter as a **single-line JSON-array scalar** (e.g.
/// `aliases: ["coordinator"]`) so it fits the line-based parser and
/// round-trips losslessly. Resolution follows the same inherit-on-omit fold
/// as [`TEAM_AGENTS_KEY`]: an omitted key inherits the lower tiers' effective
/// list, an explicit `[]` clears it, and a non-empty list overrides it
/// wholesale.
///
/// Collision rules (deterministic):
/// - A canonical specialist id always wins over any alias — alias lookup only
///   runs after direct resolution misses, so an alias shadowing a real id is
///   simply never consulted.
/// - When multiple specialists claim the same alias, the one with the
///   lexicographically smallest canonical id wins (ids are scanned in
///   ascending order).
const ALIASES_KEY: &str = "aliases";

/// Strictly validate a wire `aliases` value (`specialist.create`/`edit`
/// specs): must be a JSON array of non-empty (non-whitespace) strings, and —
/// stricter than `teamAgents` — each entry must itself pass `validate_id`
/// (aliases are looked up exactly like specialist ids, so an entry
/// `alias_target` could never match, e.g. `"foo/bar"`, would be a silently
/// dead alias). Returns the entries in input order (`None` when the key is
/// absent — the inherit-on-omit case); any invalid shape → `-32602`.
fn validate_aliases_spec(value: Option<&Value>) -> Result<Option<Vec<Value>>> {
    let Some(value) = value else { return Ok(None) };
    let Some(arr) = value.as_array() else {
        return Err(Error::InvalidParams(
            "aliases must be an array of specialist-id strings".to_string(),
        ));
    };
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        match entry.as_str() {
            Some(s) if !s.trim().is_empty() => {
                validate_id(s).map_err(|_| {
                    Error::InvalidParams(format!(
                        "aliases entries must be valid specialist ids: {s:?}"
                    ))
                })?;
                out.push(json!(s));
            }
            _ => {
                return Err(Error::InvalidParams(
                    "aliases entries must be non-empty strings".to_string(),
                ));
            }
        }
    }
    Ok(Some(out))
}

/// Retired frontmatter/wire keys, tolerated-and-ignored like the retired
/// `model.workspaceOverrides` setting (PROTOCOL §5.11/§5.12): old files and
/// old-client `specialist.create`/`edit` specs may still carry them, but they
/// are stripped on parse (never echoed by `get`/`list`), silently skipped by
/// `render_file` (never rejected with `-32602`), and dropped from the file on
/// its next rewrite. `modelTier` is retired: a specialist's model is either an
/// explicit `model:` pin or inherited via the settings chain (§5.5).
const RETIRED_FRONTMATTER_KEYS: &[&str] = &["modelTier"];

/// Tri-state read of a frontmatter/spec `hidden` value: `Some(true)`/`Some(false)`
/// when explicitly set, `None` when absent — an absent key **inherits** the
/// effective value from lower tiers at resolution time (PROTOCOL §5.11). The
/// frontmatter parser yields strings (matched case-insensitively so YAML-style
/// `true`/`True`/`TRUE` all count), while wire specs carry the JSON boolean.
///
/// Accepted forms, intentionally:
/// - The string arms also apply to wire specs, so JSON strings `"true"`/`"false"`
///   on `specialist.create`/`edit` count even though PROTOCOL §5.11 declares
///   `hidden?: boolean` (deliberately liberal in what we accept).
/// - YAML 1.1 truthy spellings (`yes`, `on`, `1`) are **not** recognized —
///   only case-insensitive `true`/`false`; unrecognized values inherit.
fn hidden_state(value: Option<&Value>) -> Option<bool> {
    match value {
        Some(Value::Bool(b)) => Some(*b),
        Some(Value::String(s)) if s.eq_ignore_ascii_case("true") => Some(true),
        Some(Value::String(s)) if s.eq_ignore_ascii_case("false") => Some(false),
        _ => None,
    }
}

/// The effective `hidden` of an already-built def (the field is emitted only
/// when the resolved value is true).
fn effective_hidden(def: &Value) -> bool {
    def.get("hidden").and_then(Value::as_bool).unwrap_or(false)
}

/// Build a wire `SpecialistDef` from one file's `content`. `source` is the
/// winning tier; `path` is the resolved file (omitted for `bundled`,
/// PROTOCOL §5.11). `prompt` is the markdown body; the optional frontmatter
/// scalars ([`OPTIONAL_FRONTMATTER_KEYS`]) are carried through when present so
/// they round-trip losslessly, and the optional boolean `hidden` is emitted as
/// `true` when the effective value resolves to true (absent otherwise,
/// PROTOCOL §5.11). This wrapper is the no-inheritance form — it passes
/// `inherited = None` to [`build_def_inheriting`], so only explicit
/// frontmatter values are emitted; callers merging across tiers use
/// [`build_def_inheriting`] directly to carry the lower tiers' resolved def.
/// `behaviorPrompt` mirrors `prompt` and `isCustomized` is `true` for any
/// non-`bundled` source (port of `serializeSpecialist`).
fn build_def(id: &str, content: &str, source: &str, path: &Path) -> Value {
    build_def_inheriting(id, content, source, path, None)
}

/// [`build_def`] with tier-inheritance (PROTOCOL §5.11): `inherited` is the
/// already-resolved def folded from the lower tiers. When the file's
/// frontmatter omits `hidden`, the lower tiers' effective value applies; an
/// explicit `hidden: true`/`false` overrides it, and the field is emitted only
/// when the effective value is true. The config scalars
/// ([`INHERITED_CONFIG_KEYS`]) inherit independently per key: an omitted key
/// keeps the lower tiers' effective value, an explicit empty value clears it,
/// and an explicit non-empty value overrides it; [`MODEL_OPTIONS_KEY`] and
/// [`TEAM_AGENTS_KEY`] follow the same fold with `[]` as the explicit clear.
/// `roleReminder` does not inherit — it is emitted only when present in this
/// file.
fn build_def_inheriting(
    id: &str,
    content: &str,
    source: &str,
    path: &Path,
    inherited: Option<&Value>,
) -> Value {
    let (fm, body) = parse_frontmatter(content);
    let name = fm
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or(id)
        .to_string();
    let description = fm
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let mut def = Map::new();
    def.insert("id".into(), json!(id));
    def.insert("name".into(), json!(name));
    def.insert("description".into(), json!(description));
    for &key in OPTIONAL_FRONTMATTER_KEYS {
        // Lenient read normalization: an out-of-enum on-disk `role` is
        // treated like an omitted key (which inherits), mirroring the
        // unparseable-`teamAgents` case — `list`/`get` must never serve a
        // value the strict write validation would reject on echo-back.
        let value = fm
            .get(key)
            .and_then(Value::as_str)
            .filter(|v| key != "role" || ROLE_VALUES.contains(v));
        match value {
            Some(v) if !v.is_empty() => {
                def.insert(key.into(), json!(v));
            }
            // Explicit empty value: clears any inherited value (nothing
            // emitted). For `roleReminder` this matches the absent case.
            // Absent key: the config scalars inherit the lower tiers'
            // effective value; `roleReminder` does not.
            None if INHERITED_CONFIG_KEYS.contains(&key) => {
                if let Some(v) = inherited
                    .and_then(|d| d.get(key))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    def.insert(key.into(), json!(v));
                }
            }
            Some(_) | None => {}
        }
    }
    // `modelOptions` / `teamAgents` / `aliases` (PROTOCOL §5.11): same
    // inherit-on-omit fold as the config scalars — an omitted key inherits
    // the lower tiers' effective list, an explicit `[]` clears it, and a
    // non-empty list overrides it wholesale. Unparseable frontmatter is
    // tolerated like an omitted key (files are never rejected on read).
    // `aliases` reuses the `teamAgents` lenient string-array parser.
    let json_array_keys: [(&str, FrontmatterListParser); 3] = [
        (MODEL_OPTIONS_KEY, parse_model_options_frontmatter),
        (TEAM_AGENTS_KEY, parse_team_agents_frontmatter),
        (ALIASES_KEY, parse_team_agents_frontmatter),
    ];
    for (key, parse) in json_array_keys {
        match fm.get(key).and_then(Value::as_str).and_then(parse) {
            Some(entries) if !entries.is_empty() => {
                def.insert(key.into(), Value::Array(entries));
            }
            // Explicit `[]`: clears any inherited list (nothing emitted).
            Some(_) => {}
            None => {
                if let Some(entries) = inherited
                    .and_then(|d| d.get(key))
                    .and_then(Value::as_array)
                    .filter(|a| !a.is_empty())
                {
                    def.insert(key.into(), Value::Array(entries.clone()));
                }
            }
        }
    }
    // Optional boolean: emitted only when the effective value is true so
    // pickers can filter hidden specialists (absent ⇒ not hidden, PROTOCOL
    // §5.11). A file that omits the key inherits from lower tiers.
    if hidden_state(fm.get("hidden")).unwrap_or_else(|| inherited.is_some_and(effective_hidden)) {
        def.insert("hidden".into(), json!(true));
    }
    def.insert("prompt".into(), json!(body));
    // `behaviorPrompt` is the wire alias for the body (port of
    // `serializeSpecialist`: both `prompt` and `behaviorPrompt` carry it).
    def.insert("behaviorPrompt".into(), json!(body));
    def.insert("source".into(), json!(source));
    // Any non-bundled (user/project) definition is a customization.
    def.insert("isCustomized".into(), json!(source != "bundled"));
    // `bundled` is read-only and exposes no editable path (PROTOCOL §5.11).
    if source != "bundled" {
        def.insert("path".into(), json!(path.to_string_lossy()));
    }
    Value::Object(def)
}

/// Serialize a wire `spec` into markdown-with-frontmatter (port of
/// `writeSpecialistFile`): quoted `name`/`description` scalars, then any
/// supplied optional scalars ([`OPTIONAL_FRONTMATTER_KEYS`]) in declaration
/// order, then `hidden: true`/`false` when the spec sets it explicitly (an
/// omitted `hidden` writes no key, which inherits from lower tiers at
/// resolution time — explicit false is the opt-out, PROTOCOL §5.11), then
/// the prompt body. For the config scalars ([`INHERITED_CONFIG_KEYS`]) an
/// explicit empty string writes `key: ""` — the explicit-clear that stops
/// inheritance — while an absent key writes nothing (inherits); an empty
/// `roleReminder` is skipped like an absent one. Supplied
/// [`MODEL_OPTIONS_KEY`] / [`TEAM_AGENTS_KEY`] / [`ALIASES_KEY`] lists are
/// written as single-line JSON-array scalars (an explicit `[]` is the clear;
/// an absent key writes nothing). The body is
/// taken from `prompt`, falling back to the `behaviorPrompt` alias (mirroring
/// `SpecialistProposalPayload`). Only documented fields are written so
/// parse→write→parse round-trips losslessly.
fn render_file(id: &str, spec: &Value) -> String {
    let name = spec.get("name").and_then(Value::as_str).unwrap_or(id);
    let description = spec
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("");
    let mut fm = vec![
        format!("name: \"{}\"", escape_yaml(name)),
        format!("description: \"{}\"", escape_yaml(description)),
    ];
    for &key in OPTIONAL_FRONTMATTER_KEYS {
        if let Some(v) = spec.get(key).and_then(Value::as_str) {
            if !v.is_empty() || INHERITED_CONFIG_KEYS.contains(&key) {
                fm.push(format!("{key}: \"{}\"", escape_yaml(v)));
            }
        }
    }
    // `modelOptions` / `teamAgents` are written as single-line JSON-array
    // scalars; an explicit empty array writes `key: []` — the explicit clear
    // that stops inheritance — while an absent key writes nothing (inherits).
    // `create`/`edit` validate the values before rendering (`-32602` on
    // invalid shapes); `render_file` itself silently skips anything invalid.
    if let Ok(Some(opts)) = validate_model_options_spec(spec.get(MODEL_OPTIONS_KEY)) {
        fm.push(format!("{MODEL_OPTIONS_KEY}: {}", Value::Array(opts)));
    }
    if let Ok(Some(agents)) = validate_team_agents_spec(spec.get(TEAM_AGENTS_KEY)) {
        fm.push(format!("{TEAM_AGENTS_KEY}: {}", Value::Array(agents)));
    }
    if let Ok(Some(aliases)) = validate_aliases_spec(spec.get(ALIASES_KEY)) {
        fm.push(format!("{ALIASES_KEY}: {}", Value::Array(aliases)));
    }
    if let Some(hidden) = hidden_state(spec.get("hidden")) {
        fm.push(format!("hidden: {hidden}"));
    }
    let prompt = spec
        .get("prompt")
        .and_then(Value::as_str)
        .or_else(|| spec.get("behaviorPrompt").and_then(Value::as_str))
        .unwrap_or("");
    format!("---\n{}\n---\n\n{}", fm.join("\n"), prompt)
}

/// Extract the first numbered bold rule (`N. **text**`) under a `## Hard Rules`
/// heading, if any (port of the `getRoleReminder` Hard-Rules regex). Scans from
/// the heading until the next `##` heading and returns the first non-empty inner
/// `**…**` (rejecting any inner `*`, mirroring the TS `[^*]+`).
fn first_hard_rule(behavior_prompt: &str) -> Option<String> {
    let mut in_section = false;
    for line in behavior_prompt.split('\n') {
        let trimmed = line.trim();
        if !in_section {
            if let Some(rest) = trimmed.strip_prefix("##") {
                if rest.trim().to_lowercase().starts_with("hard rules") {
                    in_section = true;
                }
            }
            continue;
        }
        if trimmed.starts_with("##") {
            break;
        }
        // Match a leading `N.` then a `**…**` span.
        let digits = trimmed.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 || trimmed.as_bytes().get(digits) != Some(&b'.') {
            continue;
        }
        let after_num = trimmed[digits + 1..].trim_start();
        let Some(after_open) = after_num.strip_prefix("**") else {
            continue;
        };
        let Some(end) = after_open.find("**") else {
            continue;
        };
        let inner = &after_open[..end];
        if inner.is_empty() || inner.contains('*') {
            continue;
        }
        return Some(inner.trim().to_string());
    }
    None
}

/// Auto-generate a role reminder from a behavior prompt (port of
/// `autoGenerateRoleReminder`): the first meaningful (non-header, non-bold-only)
/// line, suffixed with the first numbered rule under a `## Hard Rules` section
/// when present.
fn auto_generate_role_reminder(behavior_prompt: &str) -> String {
    if behavior_prompt.is_empty() {
        return String::new();
    }
    let mut first_meaningful = String::new();
    for line in behavior_prompt.split('\n') {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if trimmed.starts_with("**") && trimmed.ends_with("**") {
            continue;
        }
        // Strip a leading and/or trailing `**` marker (port of the
        // `replace(/^\*\*|\*\*$/g, '')` core-role normalization).
        let stripped = trimmed.strip_prefix("**").unwrap_or(trimmed);
        let stripped = stripped.strip_suffix("**").unwrap_or(stripped);
        first_meaningful = stripped.trim().to_string();
        break;
    }
    if let Some(rule) = first_hard_rule(behavior_prompt) {
        if !first_meaningful.is_empty() {
            return format!("{first_meaningful} {rule}.");
        }
        return rule;
    }
    first_meaningful
}

/// Stateless executor for the file-backed `specialist.*` namespace. Construct
/// one per call from the long-lived `Services`; it carries the resolved user and
/// bundled directory roots (project comes from each call's `workspacePath`).
pub(crate) struct SpecialistsService {
    user_dir: Option<PathBuf>,
    bundled_dir: Option<PathBuf>,
    /// The embedded bundled floor (H2): the latest bundle by default;
    /// session-scoped resolution swaps in the session's pinned version's
    /// bundle via [`Self::with_embedded`]. The file tiers above it are
    /// user-owned and unversioned.
    embedded: &'static [(&'static str, &'static str)],
    /// Whether [`REPLACEMENT_DIR_ENV`] replaced the base tier at construction:
    /// `bundled_dir` is the replacement directory, `embedded` is empty, and
    /// [`Self::with_embedded`] keeps it that way (session pins never restore
    /// the shipped bundle behind a startup-pinned replacement).
    base_replaced: bool,
}

impl SpecialistsService {
    /// Build the service, resolving any unset directory from the environment
    /// (`~/.intent/specialists/` for user, [`BUNDLED_DIR_ENV`]/exe-relative for
    /// bundled). When no bundled root is injected and [`REPLACEMENT_DIR_ENV`]
    /// is set to a non-empty path the base tier is wholesale-replaced instead
    /// ([`Self::with_base_replacement`]) — an explicitly injected
    /// `bundled_dir` wins over the env var (matching [`BUNDLED_DIR_ENV`],
    /// consulted only inside `default_bundled_dir`), so tests that inject
    /// explicit roots stay hermetic even when the var is exported.
    pub(crate) fn new(user_dir: Option<PathBuf>, bundled_dir: Option<PathBuf>) -> Self {
        if bundled_dir.is_none() {
            if let Some(dir) = replacement_dir(std::env::var_os(REPLACEMENT_DIR_ENV)) {
                return Self::with_base_replacement(user_dir, dir);
            }
        }
        Self {
            user_dir: user_dir.or_else(default_user_dir),
            bundled_dir: bundled_dir.or_else(default_bundled_dir),
            embedded: EMBEDDED_BUNDLED,
            base_replaced: false,
        }
    }

    /// Build the service with the base tier wholesale-replaced by `dir`
    /// (the effective `specialists.dir` setting — [`REPLACEMENT_DIR_ENV`]
    /// startup pin or config.toml): the embedded bundle and the bundled
    /// directory are excluded, and `dir` is the sole base (`bundled`,
    /// read-only) tier — a missing/empty `dir` yields an empty base tier.
    /// The user/project tiers fold on top unchanged. Split out of
    /// [`Self::new`] so tests cover replacement hermetically, without
    /// mutating process-global env.
    pub(crate) fn with_base_replacement(user_dir: Option<PathBuf>, dir: PathBuf) -> Self {
        Self {
            user_dir: user_dir.or_else(default_user_dir),
            bundled_dir: Some(dir),
            embedded: EMPTY_BUNDLE,
            base_replaced: true,
        }
    }

    /// Replace the embedded bundled floor with a pinned version's bundle
    /// (H2): session-scoped resolution (prompt injection, role reminder)
    /// resolves specialists against the doctrine the session was stamped
    /// with, so a respawn under a newer binary keeps the pinned prompts.
    ///
    /// The pin deliberately covers PROMPT CONTENT only: frontmatter scalar
    /// resolutions (e.g. `specialist_agent_type` → spawn-time tool denylist)
    /// stay latest-bound, so if a future bundle changes a specialist's
    /// scalar, pinned sessions adopt the new behavior while keeping their
    /// pinned prompt text.
    ///
    /// No-op when [`REPLACEMENT_DIR_ENV`] replaced the base tier: the
    /// replacement directory stays the sole base tier, so pinned sessions
    /// never resurrect shipped bundles the operator excluded at startup.
    pub(crate) fn with_embedded(mut self, bundle: &'static [(&'static str, &'static str)]) -> Self {
        if !self.base_replaced {
            self.embedded = bundle;
        }
        self
    }

    /// Load one specialist file from `dir` as `source`, if it exists and reads;
    /// `inherited` is the def resolved from lower tiers, whose effective
    /// `hidden` and config scalars apply when this file omits them.
    fn load_from_dir(
        dir: &Path,
        id: &str,
        source: &str,
        inherited: Option<&Value>,
    ) -> Option<Value> {
        let path = dir.join(format!("{id}.md"));
        let content = std::fs::read_to_string(&path).ok()?;
        Some(build_def_inheriting(id, &content, source, &path, inherited))
    }

    /// Resolve a single id through the 3-tier order project > user > bundled,
    /// falling back to alias lookup ([`ALIASES_KEY`]) when no specialist
    /// carries the id directly. A canonical id therefore always wins over an
    /// alias with the same spelling — the alias scan only runs after direct
    /// resolution misses. The returned def is the canonical specialist's
    /// (its `id` field carries the CANONICAL id, never the alias).
    fn resolve(&self, id: &str, workspace_path: Option<&Path>) -> Option<Value> {
        if let Some(def) = self.resolve_direct(id, workspace_path) {
            return Some(def);
        }
        let canonical = self.alias_target(id, workspace_path)?;
        self.resolve_direct(&canonical, workspace_path)
    }

    /// Resolve a single id through the 3-tier order project > user > bundled
    /// — direct lookup only, no alias fallback.
    /// Within the bundled tier an on-disk file wins over the embedded copy;
    /// the compile-time embedded set (`self.embedded`, the latest bundle
    /// unless a session pinned one via [`Self::with_embedded`]) is the
    /// always-available floor.
    /// Tiers are folded from the floor upward so `hidden` and the config
    /// scalars ([`INHERITED_CONFIG_KEYS`]) inherit across them: a higher-tier
    /// file that omits a key keeps the lower tiers' effective value, while an
    /// explicit `hidden: false` unhides and an explicit empty scalar clears
    /// (PROTOCOL §5.11).
    /// SECURITY: validates the id before file access to prevent path traversal
    /// (review thread `PRRT_kwDOS9Wxuc6SIlcV`).
    fn resolve_direct(&self, id: &str, workspace_path: Option<&Path>) -> Option<Value> {
        // Validate id before passing to load_from_dir to prevent path traversal
        // attacks on ALL frontmatter lookups (resolve_agent_type, resolve_model,
        // resolve_role_reminder, resolve_prompt_injection).
        validate_id(id).ok()?;
        let mut resolved = load_embedded(self.embedded, id);
        let project = workspace_path.map(project_dir);
        let tiers = [
            (self.bundled_dir.as_deref(), "bundled"),
            (self.user_dir.as_deref(), "user"),
            (project.as_deref(), "project"),
        ];
        for (dir, source) in tiers {
            let Some(dir) = dir else { continue };
            if let Some(def) = Self::load_from_dir(dir, id, source, resolved.as_ref()) {
                resolved = Some(def);
            }
        }
        resolved
    }

    /// Map an alias to the canonical id of the specialist claiming it via
    /// [`ALIASES_KEY`], or `None` when no resolved specialist does. Scans the
    /// full resolved catalog ([`Self::collect_catalog`]) in ascending
    /// canonical-id order, so when multiple specialists claim the same alias
    /// the lexicographically smallest canonical id wins deterministically.
    /// Only runs after direct resolution misses (see [`Self::resolve`]), so
    /// an alias can never shadow a canonical id.
    fn alias_target(&self, alias: &str, workspace_path: Option<&Path>) -> Option<String> {
        validate_id(alias).ok()?;
        let catalog = self.collect_catalog(workspace_path);
        for (canonical, def) in catalog {
            let claims = def
                .get(ALIASES_KEY)
                .and_then(Value::as_array)
                .is_some_and(|a| a.iter().any(|v| v.as_str() == Some(alias)));
            if claims {
                return Some(canonical);
            }
        }
        None
    }

    /// Map an id-or-alias to its canonical specialist id: a directly-known
    /// id returns itself; an alias returns the canonical id of the specialist
    /// claiming it ([`Self::alias_target`]); an unknown id returns `None`.
    /// Spawn/delegation seams call this before persisting a session's
    /// `specialist` so `metadata.specialist` always carries the canonical id
    /// (e.g. spawning with `"coordinator"` persists `"spec-writer"`).
    pub(crate) fn canonical_id(&self, id: &str, workspace_path: Option<&Path>) -> Option<String> {
        if self.resolve_direct(id, workspace_path).is_some() {
            return Some(id.to_string());
        }
        self.alias_target(id, workspace_path)
    }

    /// Strict form of [`Self::canonical_id`] for the spawn/update seams
    /// (monorepo#3497): an unknown id is rejected with `-32602` naming the
    /// id and the known catalog ids, instead of being persisted verbatim
    /// with no behavior prompt. The known-id list matches [`Self::list`]
    /// (retired `ralph` excluded).
    pub(crate) fn canonical_id_or_err(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
    ) -> Result<String> {
        // Ralph remains resolvable for existing sessions (inheritance uses
        // the lenient `canonical_id`), but is retired from new-session
        // catalogs ([`Self::list`]) — so the strict seams reject it like any
        // other undiscoverable id.
        let mut catalog = self.collect_catalog(workspace_path);
        catalog.remove("ralph");
        if let Some(canonical) = self.canonical_id(id, workspace_path) {
            if catalog.contains_key(&canonical) {
                return Ok(canonical);
            }
        }
        let known = catalog.into_keys().collect::<Vec<_>>().join(", ");
        Err(Error::InvalidParams(format!(
            "unknown specialist: {id} (known specialists: {known}; aliases are accepted)"
        )))
    }

    /// The def inherited from the tiers **below** `scope` — the same fold
    /// [`Self::resolve`] applies, stopped before `scope` — so that
    /// `specialist.create`/`edit` responses agree with an immediately-following
    /// `specialist.get` when the written spec omits `hidden` or a config
    /// scalar (PROTOCOL §5.11).
    fn inherited_below(
        &self,
        id: &str,
        scope: &str,
        workspace_path: Option<&Path>,
    ) -> Option<Value> {
        let mut resolved = load_embedded(self.embedded, id);
        let project = workspace_path.map(project_dir);
        let tiers = [
            (self.bundled_dir.as_deref(), "bundled"),
            (self.user_dir.as_deref(), "user"),
            (project.as_deref(), "project"),
        ];
        for (dir, source) in tiers {
            if source == scope {
                break;
            }
            let Some(dir) = dir else { continue };
            if let Some(def) = Self::load_from_dir(dir, id, source, resolved.as_ref()) {
                resolved = Some(def);
            }
        }
        resolved
    }

    /// Resolve a specialist's `agentType` frontmatter scalar through the 3-tier
    /// order (project > user > bundled), used at spawn time to derive a created
    /// agent's `agent_type` (SP-B / §18.2 → §18.4 denylist). Returns `None` when
    /// the specialist is unknown or declares no `agentType`, leaving the caller's
    /// default agent type intact.
    pub(crate) fn resolve_agent_type(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
    ) -> Option<String> {
        self.resolve(id, workspace_path).and_then(|def| {
            def.get("agentType")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
    }

    /// Resolve a specialist's `role` frontmatter enum (PROTOCOL §5.11:
    /// `orchestrator` | `internal`) through the 3-tier order (project >
    /// user > bundled). Returns `None` when the specialist is unknown or
    /// carries no effective role (omitted, explicitly cleared, or
    /// normalized-out on read); callers that must tell the explicit
    /// `role: ""` clear apart from omission use [`Self::resolve_role_state`]
    /// — the sole production reader ([`Self::resolve_is_orchestrator`]) now
    /// does, leaving this wire-shaped view test-only.
    #[cfg(test)]
    pub(crate) fn resolve_role(&self, id: &str, workspace_path: Option<&Path>) -> Option<String> {
        match self.resolve_role_state(id, workspace_path) {
            RoleResolution::Role(role) => Some(role),
            _ => None,
        }
    }

    /// The tier-folded resolution state of a specialist's `role` frontmatter
    /// (PROTOCOL §5.11), distinguishing the explicit `role: ""` clear from a
    /// role that was never set. The wire def cannot make that distinction —
    /// the clear folds to an absent key inside `resolve()` — but the
    /// orchestrator gate must: an explicit clear is a deliberate opt-out of
    /// the historical-name fallback, plain omission stays fail-closed
    /// ([`Self::resolve_is_orchestrator`]). The fold mirrors
    /// [`build_def_inheriting`]'s per-key inheritance: raw frontmatter is
    /// read tier by tier from the embedded floor upward, an omitted (or
    /// out-of-enum, read-leniently) key keeps the lower tiers' state, and
    /// the highest tier that touches the key decides.
    pub(crate) fn resolve_role_state(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
    ) -> RoleResolution {
        let Some(canonical) = self.canonical_id(id, workspace_path) else {
            return RoleResolution::Unknown;
        };
        let mut state = RoleResolution::Absent;
        if let Some((_, content)) = self.embedded.iter().find(|(k, _)| *k == canonical) {
            fold_role_directive(&mut state, content);
        }
        let project = workspace_path.map(project_dir);
        let tiers = [
            self.bundled_dir.as_deref(),
            self.user_dir.as_deref(),
            project.as_deref(),
        ];
        for dir in tiers.into_iter().flatten() {
            if let Ok(content) = std::fs::read_to_string(dir.join(format!("{canonical}.md"))) {
                fold_role_directive(&mut state, &content);
            }
        }
        state
    }

    /// Whether a specialist id resolves to the `orchestrator` role — the
    /// spawn-time gate for the orchestrator tool denylist (§18.4,
    /// `intent_acp::get_native_tools_to_remove`). An explicitly resolved role
    /// decides directly, and an explicit `role: ""` clear reads as NOT an
    /// orchestrator (the user deliberately cleared the inherited role); only
    /// when no tier ever touched the key — or the id no longer resolves at
    /// all — do the historical orchestrator ids `spec-writer`/`coordinator`
    /// fall back to orchestrator by name. The fallback exists because
    /// sessions can carry those ids without a resolvable role: the v1
    /// embedded floor predates the `role` key (picker metadata landed in
    /// v1.1), and a session's specialist may no longer resolve at all
    /// (deleted custom file, [`REPLACEMENT_DIR_ENV`] base replacement, v1
    /// floor without the `coordinator` alias) — dropping the restriction
    /// there would silently hand an orchestrator its file-editing tools back.
    pub(crate) fn resolve_is_orchestrator(&self, id: &str, workspace_path: Option<&Path>) -> bool {
        match self.resolve_role_state(id, workspace_path) {
            RoleResolution::Role(role) => role == "orchestrator",
            RoleResolution::Cleared => false,
            RoleResolution::Absent | RoleResolution::Unknown => {
                matches!(id, "spec-writer" | "coordinator")
            }
        }
    }

    /// Resolve a specialist's display name (frontmatter `name`, defaulting to
    /// the id inside `resolve()`) through the 3-tier order (project > user >
    /// bundled), used at spawn time to derive a created agent's name when the
    /// caller omits one. Returns `None` when the specialist is unknown,
    /// leaving the caller's `Agent {6-hex}` fallback intact.
    pub(crate) fn resolve_display_name(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
    ) -> Option<String> {
        self.resolve(id, workspace_path).and_then(|def| {
            def.get("name")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
    }

    /// Resolve a specialist's `model` frontmatter scalar through the 3-tier
    /// order (project > user > bundled), used at spawn time when no explicit
    /// model parameter is supplied. Returns `None` when the specialist is
    /// unknown or declares no `model`, allowing the caller to fall through to
    /// the settings chain. Validation is now performed inside `resolve()`.
    pub(crate) fn resolve_model(&self, id: &str, workspace_path: Option<&Path>) -> Option<String> {
        self.resolve(id, workspace_path).and_then(|def| {
            def.get("model")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
    }

    /// Resolve a specialist's `reasoningEffort` frontmatter scalar through the
    /// same 3-tier order (project > user > bundled) as [`Self::resolve_model`],
    /// used as the frontmatter rung of the delegation reasoning-effort
    /// resolution (PROTOCOL §5.11). Returns `None` when the specialist is
    /// unknown or declares no `reasoningEffort`, leaving the session field
    /// unset.
    pub(crate) fn resolve_reasoning_effort(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
    ) -> Option<String> {
        self.resolve(id, workspace_path).and_then(|def| {
            def.get("reasoningEffort")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
    }

    /// Resolve the `reasoningEffort` declared by the specialist's
    /// [`MODEL_OPTIONS_KEY`] entry whose `model` equals `model` (PROTOCOL
    /// §5.11) — the model-option rung of the delegation effort resolution.
    /// Returns `None` when the specialist is unknown, declares no matching
    /// option, or the matching option carries no effort.
    pub(crate) fn resolve_model_option_effort(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
        model: &str,
    ) -> Option<String> {
        self.resolve(id, workspace_path).and_then(|def| {
            def.get(MODEL_OPTIONS_KEY)
                .and_then(Value::as_array)?
                .iter()
                .find(|o| o.get("model").and_then(Value::as_str) == Some(model))
                .and_then(|o| o.get("reasoningEffort"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
    }

    /// Resolve a specialist's `codingAgent` frontmatter scalar through the
    /// same 3-tier order (project > user > bundled) as [`Self::resolve_model`],
    /// used by the delegate/spawn provider resolution (D2 step 1) so a
    /// specialist that pins a provider always runs on it instead of the
    /// caller's configured default. Returns `None` when the specialist is
    /// unknown or declares no `codingAgent`, letting the caller fall through
    /// to the settings chain.
    pub(crate) fn resolve_coding_agent(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
    ) -> Option<String> {
        self.resolve(id, workspace_path).and_then(|def| {
            def.get("codingAgent")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
    }

    /// Enumerate every `<id>.md` in `dir`, inserting resolved defs into `acc`
    /// keyed by id (later tiers overwrite earlier — the precedence merge).
    /// `hidden` and the config scalars ([`INHERITED_CONFIG_KEYS`]) inherit
    /// across the merge: a file that omits a key keeps the accumulated
    /// effective value from lower tiers (PROTOCOL §5.11).
    fn collect_dir(dir: &Path, source: &str, acc: &mut std::collections::BTreeMap<String, Value>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("md") {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            if let Ok(content) = std::fs::read_to_string(&path) {
                let def = build_def_inheriting(stem, &content, source, &path, acc.get(stem));
                acc.insert(stem.to_string(), def);
            }
        }
    }

    /// The full resolved catalog in tier order (embedded < bundled dir <
    /// user < project), higher tiers overriding lower ones for the same id
    /// while `hidden` and the config scalars inherit across tiers (PROTOCOL
    /// §5.11). Keyed by id (a `BTreeMap`, so iteration is ascending-id — the
    /// deterministic order the alias-collision rule relies on). Shared by
    /// [`Self::list`] and [`Self::alias_target`].
    fn collect_catalog(
        &self,
        workspace_path: Option<&Path>,
    ) -> std::collections::BTreeMap<String, Value> {
        let mut acc = std::collections::BTreeMap::new();
        for (id, content) in self.embedded {
            acc.insert(
                id.to_string(),
                build_def(id, content, "bundled", Path::new("")),
            );
        }
        if let Some(dir) = &self.bundled_dir {
            Self::collect_dir(dir, "bundled", &mut acc);
        }
        if let Some(dir) = &self.user_dir {
            Self::collect_dir(dir, "user", &mut acc);
        }
        if let Some(wp) = workspace_path {
            Self::collect_dir(&project_dir(wp), "project", &mut acc);
        }
        acc
    }

    /// `specialist.list` → `{ specialists: SpecialistDef[] }`, the resolved
    /// catalog ([`Self::collect_catalog`]); `workspace_path` adds the project
    /// tier.
    #[allow(clippy::unnecessary_wraps)] // WorkspaceApi surface; keeps the uniform Result shape
    pub(crate) fn list(&self, workspace_path: Option<&Path>) -> Result<Value> {
        let mut acc = self.collect_catalog(workspace_path);
        // Ralph remains in the pinned v1 doctrine for existing sessions, but
        // is retired from new-session catalogs (including Settings).
        acc.remove("ralph");
        let specialists: Vec<Value> = acc.into_values().collect();
        Ok(json!({ "specialists": specialists }))
    }

    /// `specialist.get` → `{ specialist: SpecialistDef }`, the resolved view;
    /// unknown id → `-32602` (PROTOCOL §5.11).
    pub(crate) fn get(&self, id: &str, workspace_path: Option<&Path>) -> Result<Value> {
        validate_id(id)?;
        match self.resolve(id, workspace_path) {
            Some(def) => Ok(json!({ "specialist": def })),
            None => Err(Error::NotFound(format!("specialist not found: {id}"))),
        }
    }

    /// Resolve `(name, roleReminder)` for an agent's specialist id, or `None`
    /// when the specialist is unknown or yields no usable reminder (port of
    /// `resolveSpecialistForAgent` + `getRoleReminder`). The reminder is the
    /// explicit `roleReminder` frontmatter scalar when present, else
    /// auto-generated from the behavior prompt.
    pub(crate) fn resolve_role_reminder(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
    ) -> Option<(String, String)> {
        let def = self.resolve(id, workspace_path)?;
        let name = def
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or(id)
            .to_string();
        let reminder = def
            .get("roleReminder")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map_or_else(
                || {
                    let body = def
                        .get("behaviorPrompt")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    auto_generate_role_reminder(body)
                },
                str::to_string,
            );
        if reminder.is_empty() {
            return None;
        }
        Some((name, reminder))
    }

    /// Resolve the spawn-prompt injection fields for a specialist id (PP-1):
    /// `(behaviorPrompt body, display name, roleReminder)`. The reminder falls
    /// back to the auto-generated one (same policy as
    /// [`Self::resolve_role_reminder`]); empty values resolve to `None`.
    /// Returns `None` when the specialist is unknown.
    #[allow(clippy::type_complexity)]
    pub(crate) fn resolve_prompt_injection(
        &self,
        id: &str,
        workspace_path: Option<&Path>,
    ) -> Option<(Option<String>, String, Option<String>)> {
        let def = self.resolve(id, workspace_path)?;
        let behavior_prompt = def
            .get("behaviorPrompt")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let name = def
            .get("name")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or(id)
            .to_string();
        let reminder = def
            .get("roleReminder")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                let auto = auto_generate_role_reminder(behavior_prompt.as_deref().unwrap_or(""));
                (!auto.is_empty()).then_some(auto)
            });
        Some((behavior_prompt, name, reminder))
    }

    /// Resolve the writable directory for `scope`, creating it; `project`
    /// requires a `workspace_path`.
    fn writable_dir(&self, scope: &str, workspace_path: Option<&Path>) -> Result<PathBuf> {
        let dir = match scope {
            "project" => {
                let wp = workspace_path.ok_or_else(|| {
                    Error::InvalidParams("workspacePath is required for project scope".to_string())
                })?;
                project_dir(wp)
            }
            _ => self
                .user_dir
                .clone()
                .ok_or_else(|| Error::Internal("user specialists directory unavailable".into()))?,
        };
        std::fs::create_dir_all(&dir)
            .map_err(|e| Error::Internal(format!("create specialists dir failed: {e}")))?;
        Ok(dir)
    }

    /// `specialist.create` → write a new user/project file (default scope
    /// `user`); an existing id in that scope → `-32602` (PROTOCOL §5.11). The
    /// returned def folds `hidden` and the config scalars from the tiers below
    /// `scope` ([`Self::inherited_below`]) so it agrees with an
    /// immediately-following `specialist.get` when the spec omits a key.
    pub(crate) fn create(
        &self,
        id: &str,
        spec: &Value,
        scope: Option<&str>,
        workspace_path: Option<&Path>,
    ) -> Result<Value> {
        validate_id(id)?;
        let scope = parse_scope(scope.unwrap_or("user"))?;
        if !spec.is_object() {
            return Err(Error::InvalidParams("spec must be an object".to_string()));
        }
        validate_model_options_spec(spec.get(MODEL_OPTIONS_KEY))?;
        validate_team_agents_spec(spec.get(TEAM_AGENTS_KEY))?;
        validate_aliases_spec(spec.get(ALIASES_KEY))?;
        validate_role_spec(spec.get("role"))?;
        validate_icon_spec(spec.get("icon"))?;
        let dir = self.writable_dir(scope, workspace_path)?;
        let path = dir.join(format!("{id}.md"));
        if path.exists() {
            return Err(Error::InvalidParams(format!(
                "specialist already exists in {scope} scope: {id}"
            )));
        }
        let rendered = render_file(id, spec);
        std::fs::write(&path, &rendered)
            .map_err(|e| Error::Internal(format!("write specialist file failed: {e}")))?;
        let inherited = self.inherited_below(id, scope, workspace_path);
        Ok(json!({
            "specialist": build_def_inheriting(id, &rendered, scope, &path, inherited.as_ref())
        }))
    }

    /// `specialist.edit` → overwrite an existing user/project file; a missing
    /// file (e.g. a `bundled`-only id) → `-32602` (PROTOCOL §5.11). The
    /// returned def folds `hidden` and the config scalars from the tiers below
    /// `scope` ([`Self::inherited_below`]) so it agrees with an
    /// immediately-following `specialist.get` when the spec omits a key.
    pub(crate) fn edit(
        &self,
        id: &str,
        spec: &Value,
        scope: &str,
        workspace_path: Option<&Path>,
    ) -> Result<Value> {
        validate_id(id)?;
        let scope = parse_scope(scope)?;
        if !spec.is_object() {
            return Err(Error::InvalidParams("spec must be an object".to_string()));
        }
        validate_model_options_spec(spec.get(MODEL_OPTIONS_KEY))?;
        validate_team_agents_spec(spec.get(TEAM_AGENTS_KEY))?;
        validate_aliases_spec(spec.get(ALIASES_KEY))?;
        validate_role_spec(spec.get("role"))?;
        validate_icon_spec(spec.get("icon"))?;
        let dir = self.writable_dir(scope, workspace_path)?;
        let path = dir.join(format!("{id}.md"));
        if !path.exists() {
            return Err(Error::NotFound(format!(
                "specialist not found in {scope} scope: {id}"
            )));
        }
        let rendered = render_file(id, spec);
        std::fs::write(&path, &rendered)
            .map_err(|e| Error::Internal(format!("write specialist file failed: {e}")))?;
        let inherited = self.inherited_below(id, scope, workspace_path);
        Ok(json!({
            "specialist": build_def_inheriting(id, &rendered, scope, &path, inherited.as_ref())
        }))
    }

    /// `specialist.delete` → remove a user/project file; a missing file
    /// (including any `bundled`-only id) → `-32602` (PROTOCOL §5.11).
    pub(crate) fn delete(
        &self,
        id: &str,
        scope: &str,
        workspace_path: Option<&Path>,
    ) -> Result<Value> {
        validate_id(id)?;
        let scope = parse_scope(scope)?;
        let dir = match scope {
            "project" => {
                let wp = workspace_path.ok_or_else(|| {
                    Error::InvalidParams("workspacePath is required for project scope".to_string())
                })?;
                project_dir(wp)
            }
            _ => self
                .user_dir
                .clone()
                .ok_or_else(|| Error::Internal("user specialists directory unavailable".into()))?,
        };
        let path = dir.join(format!("{id}.md"));
        if !path.exists() {
            return Err(Error::NotFound(format!(
                "specialist not found in {scope} scope: {id}"
            )));
        }
        std::fs::remove_file(&path)
            .map_err(|e| Error::Internal(format!("delete specialist file failed: {e}")))?;
        Ok(json!({ "success": true }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_frontmatter_captures_optional_scalars() {
        let content = "---\nname: \"Ralph\"\ndescription: \"Loops\"\ncodingAgent: \"claude\"\nmodel: \"opus4.5\"\nmodelTier: \"smart\"\nroleReminder: \"Never stop early\"\nagentType: \"task-loop\"\n---\n\nYou loop.";
        let (fm, body) = parse_frontmatter(content);
        assert_eq!(fm.get("codingAgent").unwrap(), "claude");
        assert_eq!(fm.get("model").unwrap(), "opus4.5");
        // Retired keys are stripped on parse (RETIRED_FRONTMATTER_KEYS).
        assert!(fm.get("modelTier").is_none());
        assert_eq!(fm.get("roleReminder").unwrap(), "Never stop early");
        assert_eq!(fm.get("agentType").unwrap(), "task-loop");
        assert_eq!(body, "You loop.");
    }

    #[test]
    fn build_def_emits_wire_fields() {
        let content = "---\nname: \"Ralph\"\ndescription: \"Loops\"\ncodingAgent: \"claude\"\nmodel: \"opus4.5\"\nmodelTier: \"smart\"\nroleReminder: \"Never stop early\"\nagentType: \"task-loop\"\n---\n\nYou loop.";
        let def = build_def("ralph", content, "user", Path::new("/tmp/ralph.md"));
        assert_eq!(def["id"], "ralph");
        assert_eq!(def["name"], "Ralph");
        assert_eq!(def["description"], "Loops");
        assert_eq!(def["codingAgent"], "claude");
        assert_eq!(def["model"], "opus4.5");
        // A retired `modelTier:` frontmatter line is never echoed on the wire.
        assert!(def.get("modelTier").is_none());
        assert_eq!(def["roleReminder"], "Never stop early");
        assert_eq!(def["agentType"], "task-loop");
        assert_eq!(def["prompt"], "You loop.");
        assert_eq!(def["behaviorPrompt"], "You loop.");
        assert_eq!(def["source"], "user");
        assert_eq!(def["isCustomized"], true);
        assert_eq!(def["path"], "/tmp/ralph.md");
    }

    #[test]
    fn build_def_bundled_is_not_customized_and_omits_path() {
        let content = "---\nname: \"Impl\"\ndescription: \"d\"\n---\n\nbody";
        let def = build_def("impl", content, "bundled", Path::new("/tmp/impl.md"));
        assert_eq!(def["isCustomized"], false);
        assert!(def.get("path").is_none());
        // Absent optional scalars are not emitted.
        assert!(def.get("codingAgent").is_none());
        assert!(def.get("agentType").is_none());
    }

    #[test]
    fn render_file_round_trips_losslessly() {
        let spec = json!({
            "id": "ralph",
            "name": "Ralph",
            "description": "Loops",
            "codingAgent": "claude",
            "model": "opus4.5",
            "modelTier": "smart",
            "roleReminder": "Never stop early",
            "agentType": "task-loop",
            "prompt": "You loop.\nForever."
        });
        let rendered = render_file("ralph", &spec);
        // A retired `modelTier` in the wire spec is dropped, never written.
        assert!(!rendered.contains("modelTier"));
        let def = build_def("ralph", &rendered, "user", Path::new("/tmp/ralph.md"));
        assert_eq!(def["codingAgent"], "claude");
        assert_eq!(def["model"], "opus4.5");
        assert!(def.get("modelTier").is_none());
        assert_eq!(def["roleReminder"], "Never stop early");
        assert_eq!(def["agentType"], "task-loop");
        assert_eq!(def["prompt"], "You loop.\nForever.");
        assert_eq!(def["behaviorPrompt"], "You loop.\nForever.");
    }

    #[test]
    fn escape_yaml_round_trips_backslash_sequences() {
        // Regression: the sequential-replace unescaper corrupted a literal
        // backslash followed by `n`/`t`/`\` — `foo\nbar` (backslash + n)
        // escaped to `foo\\nbar` but decoded back as backslash + newline.
        // The single-pass decoder round-trips every such value.
        let cases = [
            "foo\\nbar",
            "foo\\\\nbar",
            "tab\\tstop",
            "trailing\\",
            "real\nnewline",
            "mixed \\n and \n and \\\\ and \"quotes\"",
        ];
        for value in cases {
            assert_eq!(
                unescape_yaml(&escape_yaml(value)),
                value,
                "escape→unescape round-trips {value:?}"
            );
        }
        // The full file path round-trips a description carrying the same
        // hazard (frontmatter scalars are the consumers of the escaper).
        let spec = json!({
            "name": "Z",
            "description": "path C:\\new\\table",
            "prompt": "body"
        });
        let rendered = render_file("z", &spec);
        let def = build_def("z", &rendered, "user", Path::new("/tmp/z.md"));
        assert_eq!(def["description"], "path C:\\new\\table");
    }

    #[test]
    fn render_file_accepts_behavior_prompt_alias() {
        let spec = json!({
            "name": "Ralph",
            "description": "Loops",
            "behaviorPrompt": "body via alias"
        });
        let rendered = render_file("ralph", &spec);
        let def = build_def("ralph", &rendered, "user", Path::new("/tmp/ralph.md"));
        assert_eq!(def["prompt"], "body via alias");
    }

    #[test]
    fn build_def_emits_hidden_when_frontmatter_sets_it() {
        let content = "---\nname: \"Ghost\"\ndescription: \"d\"\nhidden: true\n---\n\nbody";
        let def = build_def("ghost", content, "bundled", Path::new(""));
        assert_eq!(def["hidden"], true);
        // YAML-style capitalized booleans also count.
        let content = "---\nname: \"Ghost\"\ndescription: \"d\"\nhidden: True\n---\n\nbody";
        let def = build_def("ghost", content, "bundled", Path::new(""));
        assert_eq!(def["hidden"], true, "hidden: True (YAML casing) counts");
    }

    #[test]
    fn build_def_omits_hidden_when_absent_or_false() {
        let absent = "---\nname: \"Impl\"\ndescription: \"d\"\n---\n\nbody";
        let def = build_def("impl", absent, "bundled", Path::new(""));
        assert!(def.get("hidden").is_none(), "absent frontmatter → no field");
        let falsy = "---\nname: \"Impl\"\ndescription: \"d\"\nhidden: false\n---\n\nbody";
        let def = build_def("impl", falsy, "bundled", Path::new(""));
        assert!(def.get("hidden").is_none(), "hidden: false → no field");
    }

    #[test]
    fn render_file_round_trips_hidden() {
        // Wire specs carry the JSON boolean (specialist.create/edit).
        let spec = json!({
            "name": "Ghost",
            "description": "d",
            "hidden": true,
            "prompt": "body"
        });
        let rendered = render_file("ghost", &spec);
        let def = build_def("ghost", &rendered, "user", Path::new("/tmp/ghost.md"));
        assert_eq!(def["hidden"], true);
        // A spec without hidden does not write the key.
        let spec = json!({ "name": "Ghost", "description": "d", "prompt": "body" });
        let def = build_def(
            "ghost",
            &render_file("ghost", &spec),
            "user",
            Path::new("/tmp/ghost.md"),
        );
        assert!(def.get("hidden").is_none());
    }

    #[test]
    fn auto_generate_role_reminder_combines_first_line_and_hard_rule() {
        let body = "# Implementor\n\nImplement your assigned task — nothing more.\n\n## Hard Rules\n1. **No scope creep** — only what the task note asks\n2. **No refactors** — ask first\n";
        assert_eq!(
            auto_generate_role_reminder(body),
            "Implement your assigned task — nothing more. No scope creep."
        );
    }

    #[test]
    fn auto_generate_role_reminder_first_line_only_without_hard_rules() {
        let body = "# Verifier\n\nVerify the work thoroughly.\n";
        assert_eq!(
            auto_generate_role_reminder(body),
            "Verify the work thoroughly."
        );
    }

    #[test]
    fn auto_generate_role_reminder_empty_is_empty() {
        assert_eq!(auto_generate_role_reminder(""), "");
        assert_eq!(auto_generate_role_reminder("# Only A Header\n"), "");
    }

    /// A temp dir holding seeded `<id>.md` specialist files for hermetic
    /// resolver tests; removed on drop.
    struct TempSpecialistsDir {
        path: PathBuf,
    }

    impl TempSpecialistsDir {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-specialists-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn write(&self, id: &str, content: &str) {
            std::fs::write(self.path.join(format!("{id}.md")), content).unwrap();
        }
    }

    impl Drop for TempSpecialistsDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn service_over(dir: &TempSpecialistsDir) -> SpecialistsService {
        SpecialistsService::new(Some(dir.path.clone()), Some(dir.path.clone()))
    }

    /// `with_embedded` swaps the embedded floor (H2): resolution and `list`
    /// read the pinned bundle instead of the latest one, while the file tiers
    /// above stay in play.
    #[test]
    fn with_embedded_pins_the_floor_bundle() {
        static PINNED: &[(&str, &str)] = &[(
            "implementor",
            "---\nname: \"Pinned Implementor\"\ndescription: \"d\"\n---\n\npinned body",
        )];
        let empty = TempSpecialistsDir::new();
        let svc = service_over(&empty).with_embedded(PINNED);
        let def = svc.resolve("implementor", None).expect("resolves");
        assert_eq!(def["name"], "Pinned Implementor");
        assert_eq!(def["behaviorPrompt"], "pinned body");
        // list() reflects the pinned floor too (one entry, the pinned id).
        let listed = svc.list(None).unwrap();
        let ids: Vec<&str> = listed["specialists"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["implementor"]);
        // A file tier still overrides the pinned floor.
        empty.write(
            "implementor",
            "---\nname: \"File Implementor\"\ndescription: \"d\"\n---\n\nfile body",
        );
        let def = svc.resolve("implementor", None).expect("resolves");
        assert_eq!(def["name"], "File Implementor");
    }

    /// Role-based orchestrator resolution (§18.4): an explicit
    /// `role: "orchestrator"` on any custom specialist engages the
    /// orchestrator gate; `internal` and role-less specialists do not.
    #[test]
    fn resolve_is_orchestrator_keys_off_role_frontmatter() {
        let dir = TempSpecialistsDir::new();
        dir.write(
            "planner",
            "---\nname: \"Planner\"\ndescription: \"d\"\nrole: \"orchestrator\"\n---\n\nbody",
        );
        dir.write(
            "helper",
            "---\nname: \"Helper\"\ndescription: \"d\"\nrole: \"internal\"\n---\n\nbody",
        );
        dir.write(
            "plain",
            "---\nname: \"Plain\"\ndescription: \"d\"\n---\n\nbody",
        );
        let svc = service_over(&dir);
        assert_eq!(
            svc.resolve_role("planner", None).as_deref(),
            Some("orchestrator")
        );
        assert!(svc.resolve_is_orchestrator("planner", None));
        assert!(!svc.resolve_is_orchestrator("helper", None));
        assert!(!svc.resolve_is_orchestrator("plain", None));
    }

    /// The bundled orchestrator still resolves as one via its v1.1 `role`
    /// frontmatter — including through the `coordinator` alias — while the
    /// bundled internal specialists do not.
    #[test]
    fn resolve_is_orchestrator_bundled_spec_writer_and_alias() {
        let dir = TempSpecialistsDir::new();
        let svc = service_over(&dir);
        assert!(svc.resolve_is_orchestrator("spec-writer", None));
        assert!(svc.resolve_is_orchestrator("coordinator", None));
        assert!(!svc.resolve_is_orchestrator("implementor", None));
    }

    /// Name-based fallback: sessions can carry an orchestrator id without a
    /// resolvable role — a v1-pinned floor predates the `role` key, and an
    /// id may no longer resolve at all (no `coordinator` alias in the v1
    /// floor, deleted custom file). The historical ids stay orchestrators;
    /// unknown ids do not.
    #[test]
    fn resolve_is_orchestrator_name_fallback_when_role_unresolvable() {
        static V1_LIKE: &[(&str, &str)] = &[(
            "spec-writer",
            "---\nname: \"Coordinator\"\ndescription: \"d\"\n---\n\nbody",
        )];
        let empty = TempSpecialistsDir::new();
        let svc = service_over(&empty).with_embedded(V1_LIKE);
        // Resolves, but the pinned floor carries no `role` key.
        assert!(svc.resolve_is_orchestrator("spec-writer", None));
        // Does not resolve at all (no alias in the pinned floor).
        assert!(svc.resolve_is_orchestrator("coordinator", None));
        // Unknown non-orchestrator id takes no fallback.
        assert!(!svc.resolve_is_orchestrator("mystery", None));
    }

    /// An explicit non-orchestrator role on a historical orchestrator id
    /// wins over the name fallback.
    #[test]
    fn resolve_is_orchestrator_explicit_role_overrides_name_fallback() {
        let dir = TempSpecialistsDir::new();
        dir.write(
            "spec-writer",
            "---\nname: \"Coordinator\"\ndescription: \"d\"\nrole: \"internal\"\n---\n\nbody",
        );
        let svc = service_over(&dir);
        assert!(!svc.resolve_is_orchestrator("spec-writer", None));
    }

    /// An explicit `role: ""` clear is a deliberate opt-out: it reads as
    /// [`RoleResolution::Cleared`] (not `Absent`) and defeats the
    /// historical-name orchestrator fallback, while plain omission stays
    /// fail-closed.
    #[test]
    fn resolve_is_orchestrator_explicit_clear_defeats_name_fallback() {
        let dir = TempSpecialistsDir::new();
        // A user-tier spec-writer override that explicitly clears the role.
        dir.write(
            "spec-writer",
            "---\nname: \"Coordinator\"\ndescription: \"d\"\nrole: \"\"\n---\n\nbody",
        );
        // A plain override that never touches the key inherits the bundled
        // v1.1 `role: "orchestrator"` and stays an orchestrator.
        dir.write(
            "coordinator-like",
            "---\nname: \"C\"\ndescription: \"d\"\n---\n\nbody",
        );
        let svc = service_over(&dir);
        assert_eq!(
            svc.resolve_role_state("spec-writer", None),
            RoleResolution::Cleared
        );
        assert!(!svc.resolve_is_orchestrator("spec-writer", None));
        // The wire-facing resolve_role still reads the clear as no role.
        assert_eq!(svc.resolve_role("spec-writer", None), None);
        assert_eq!(
            svc.resolve_role_state("coordinator-like", None),
            RoleResolution::Absent
        );
        assert!(!svc.resolve_is_orchestrator("coordinator-like", None));
    }

    /// The cleared state folds tier by tier like the other config scalars:
    /// a higher tier that re-sets the role overrides a lower-tier clear, an
    /// out-of-enum on-disk value is read leniently as untouched, and an
    /// unknown id reads as [`RoleResolution::Unknown`] (name fallback stays
    /// fail-closed there).
    #[test]
    fn resolve_role_state_folds_across_tiers() {
        static FLOOR: &[(&str, &str)] = &[(
            "spec-writer",
            "---\nname: \"Coordinator\"\ndescription: \"d\"\nrole: \"orchestrator\"\n---\n\nbody",
        )];
        let dir = TempSpecialistsDir::new();
        // The user/project file re-sets the role above an embedded floor.
        dir.write(
            "spec-writer",
            "---\nname: \"Coordinator\"\ndescription: \"d\"\nrole: \"internal\"\n---\n\nbody",
        );
        // Out-of-enum value: lenient read leaves the floor's state in place.
        dir.write(
            "bogus-role",
            "---\nname: \"B\"\ndescription: \"d\"\nrole: \"captain\"\n---\n\nbody",
        );
        let svc = service_over(&dir).with_embedded(FLOOR);
        assert_eq!(
            svc.resolve_role_state("spec-writer", None),
            RoleResolution::Role("internal".to_string())
        );
        assert_eq!(
            svc.resolve_role_state("bogus-role", None),
            RoleResolution::Absent
        );
        assert_eq!(
            svc.resolve_role_state("missing", None),
            RoleResolution::Unknown
        );
        assert!(!svc.resolve_is_orchestrator("missing", None));
    }

    /// Alias resolution (PROTOCOL §5.11): an `aliases` entry resolves to the
    /// claiming specialist's def — the def's `id` carries the CANONICAL id,
    /// never the alias — and `canonical_id` maps id-or-alias accordingly.
    #[test]
    fn aliases_resolve_to_the_canonical_specialist() {
        let dir = TempSpecialistsDir::new();
        dir.write(
            "spec-writer",
            "---\nname: \"Coordinator\"\ndescription: \"d\"\naliases: [\"coordinator\"]\n---\n\ncoordinator body",
        );
        let svc = service_over(&dir);
        // Direct id still resolves.
        assert_eq!(
            svc.resolve("spec-writer", None).unwrap()["id"],
            "spec-writer"
        );
        // Alias resolves to the canonical def (id field is canonical).
        let via_alias = svc.resolve("coordinator", None).expect("alias resolves");
        assert_eq!(via_alias["id"], "spec-writer");
        assert_eq!(via_alias["behaviorPrompt"], "coordinator body");
        assert_eq!(via_alias["aliases"], json!(["coordinator"]));
        // canonical_id: direct id → itself, alias → canonical, unknown → None.
        assert_eq!(
            svc.canonical_id("spec-writer", None).as_deref(),
            Some("spec-writer")
        );
        assert_eq!(
            svc.canonical_id("coordinator", None).as_deref(),
            Some("spec-writer")
        );
        assert_eq!(svc.canonical_id("nope", None), None);
        // `specialist.get` serves the alias as the canonical resolved view.
        let got = svc.get("coordinator", None).unwrap();
        assert_eq!(got["specialist"]["id"], "spec-writer");
    }

    /// A canonical id always beats an alias with the same spelling, and
    /// duplicate alias claims resolve to the lexicographically smallest
    /// canonical id.
    #[test]
    fn alias_collisions_are_deterministic() {
        let dir = TempSpecialistsDir::new();
        // `verifier` exists directly AND is claimed as an alias — the direct
        // definition wins.
        dir.write(
            "verifier",
            "---\nname: \"Verifier\"\ndescription: \"d\"\n---\n\nverifier body",
        );
        dir.write(
            "grabber",
            "---\nname: \"Grabber\"\ndescription: \"d\"\naliases: [\"verifier\",\"helper\"]\n---\n\ngrabber body",
        );
        // Two specialists claim `helper`; `aaa` < `grabber` wins.
        dir.write(
            "aaa",
            "---\nname: \"Aaa\"\ndescription: \"d\"\naliases: [\"helper\"]\n---\n\naaa body",
        );
        let svc = service_over(&dir);
        assert_eq!(svc.resolve("verifier", None).unwrap()["id"], "verifier");
        assert_eq!(svc.canonical_id("helper", None).as_deref(), Some("aaa"));
    }

    /// The bundled v1.1 spec-writer carries the `coordinator` alias, so the
    /// embedded floor alone resolves `coordinator` → `spec-writer`.
    #[test]
    fn bundled_coordinator_alias_resolves_to_spec_writer() {
        let empty = TempSpecialistsDir::new();
        let svc = service_over(&empty);
        assert_eq!(
            svc.canonical_id("coordinator", None).as_deref(),
            Some("spec-writer")
        );
        let def = svc.resolve("coordinator", None).expect("alias resolves");
        assert_eq!(def["id"], "spec-writer");
        assert_eq!(def["name"], "Coordinator");
    }

    /// Strict-seam contract (monorepo#3497): `canonical_id_or_err` accepts
    /// ids and aliases from the `specialist.list` catalog, and rejects both
    /// an unknown id and the retired `ralph` — which the lenient
    /// `canonical_id` still resolves for legacy stored sessions — with a
    /// `-32602` naming the id and never listing `ralph` among the known ids.
    #[test]
    fn canonical_id_or_err_rejects_unknown_and_retired_ralph() {
        let empty = TempSpecialistsDir::new();
        let svc = service_over(&empty);
        assert_eq!(
            svc.canonical_id_or_err("implementor", None).unwrap(),
            "implementor"
        );
        assert_eq!(
            svc.canonical_id_or_err("coordinator", None).unwrap(),
            "spec-writer"
        );
        let err = svc.canonical_id_or_err("nope", None).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
        assert!(err.to_string().contains("unknown specialist: nope"));
        // Retired: lenient resolution still works (inheritance), strict rejects.
        assert_eq!(svc.canonical_id("ralph", None).as_deref(), Some("ralph"));
        let err = svc.canonical_id_or_err("ralph", None).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
        assert!(err.to_string().contains("unknown specialist: ralph"));
        assert!(
            !err.to_string().contains("ralph,") && !err.to_string().contains(", ralph"),
            "known-id list must not advertise the retired ralph: {err}"
        );
    }

    /// `render_file` writes a supplied `aliases` list as a single-line JSON
    /// array that round-trips through `build_def`, and rejects bad shapes on
    /// the write path (`validate_aliases_spec`).
    #[test]
    fn render_file_round_trips_aliases() {
        let spec = json!({
            "name": "Coordinator",
            "description": "d",
            "aliases": ["coordinator"],
            "prompt": "body"
        });
        let rendered = render_file("spec-writer", &spec);
        assert!(rendered.contains("aliases: [\"coordinator\"]"));
        let def = build_def("spec-writer", &rendered, "user", Path::new("/tmp/s.md"));
        assert_eq!(def["aliases"], json!(["coordinator"]));
        // Invalid shapes are -32602 on create/edit.
        assert!(validate_aliases_spec(Some(&json!("coordinator"))).is_err());
        assert!(validate_aliases_spec(Some(&json!([""]))).is_err());
        assert!(validate_aliases_spec(Some(&json!([42]))).is_err());
        // Entries that could never resolve (fail validate_id) are rejected
        // too — a stored dead alias would silently never match.
        assert!(validate_aliases_spec(Some(&json!(["foo/bar"]))).is_err());
        assert!(validate_aliases_spec(Some(&json!(["foo\\bar"]))).is_err());
        assert!(validate_aliases_spec(Some(&json!([".."]))).is_err());
        assert!(validate_aliases_spec(Some(&json!(["ok", "."]))).is_err());
        assert!(matches!(validate_aliases_spec(None), Ok(None)));
        assert!(matches!(
            validate_aliases_spec(Some(&json!([]))),
            Ok(Some(v)) if v.is_empty()
        ));
    }

    /// The base-tier replacement ([`REPLACEMENT_DIR_ENV`]) excludes the
    /// embedded set wholesale: only ids present in the replacement directory
    /// exist in the base tier, and shipped ids not restated there are gone.
    #[test]
    fn base_replacement_excludes_the_embedded_set() {
        let user = TempSpecialistsDir::new();
        let replacement = TempSpecialistsDir::new();
        replacement.write(
            "custom",
            "---\nname: \"Custom\"\ndescription: \"d\"\n---\n\ncustom body",
        );
        let svc = SpecialistsService::with_base_replacement(
            Some(user.path.clone()),
            replacement.path.clone(),
        );
        let listed = svc.list(None).unwrap();
        let ids: Vec<&str> = listed["specialists"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["custom"]);
        let custom = svc.get("custom", None).unwrap();
        assert_eq!(custom["specialist"]["source"], "bundled");
        assert_eq!(custom["specialist"]["prompt"], "custom body");
        // A shipped id absent from the replacement directory does not exist.
        assert!(matches!(
            svc.get("implementor", None),
            Err(Error::NotFound(_))
        ));
    }

    /// A shipped id restated in the replacement directory resolves from there,
    /// with the replacement's content — never the embedded copy's.
    #[test]
    fn base_replacement_restated_shipped_id_uses_replacement_content() {
        let user = TempSpecialistsDir::new();
        let replacement = TempSpecialistsDir::new();
        replacement.write(
            "implementor",
            "---\nname: \"Replaced Implementor\"\ndescription: \"d\"\n---\n\nreplaced body",
        );
        let svc = SpecialistsService::with_base_replacement(
            Some(user.path.clone()),
            replacement.path.clone(),
        );
        let def = svc.resolve("implementor", None).expect("resolves");
        assert_eq!(def["name"], "Replaced Implementor");
        assert_eq!(def["behaviorPrompt"], "replaced body");
        assert_eq!(def["source"], "bundled");
    }

    /// The user tier folds on top of the replacement tier unchanged: it
    /// overrides same-id entries (inheriting omitted config scalars from the
    /// replacement) and adds new ids alongside it.
    #[test]
    fn user_tier_overrides_the_replacement_tier() {
        let user = TempSpecialistsDir::new();
        let replacement = TempSpecialistsDir::new();
        replacement.write(
            "custom",
            "---\nname: \"Custom\"\ndescription: \"d\"\nmodel: \"base-model\"\n---\n\nbase body",
        );
        user.write(
            "custom",
            "---\nname: \"User Custom\"\ndescription: \"d\"\n---\n\nuser body",
        );
        user.write(
            "extra",
            "---\nname: \"Extra\"\ndescription: \"d\"\n---\n\nextra body",
        );
        let svc = SpecialistsService::with_base_replacement(
            Some(user.path.clone()),
            replacement.path.clone(),
        );
        let def = svc.resolve("custom", None).expect("resolves");
        assert_eq!(def["name"], "User Custom");
        assert_eq!(def["source"], "user");
        assert_eq!(
            def["model"], "base-model",
            "omitted config scalar inherits from the replacement tier"
        );
        let listed = svc.list(None).unwrap();
        let ids: Vec<&str> = listed["specialists"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["custom", "extra"]);
    }

    /// A missing (or empty) replacement directory yields an EMPTY base tier —
    /// no fallback to the embedded set — while the user tier stays in play.
    #[test]
    fn missing_replacement_dir_yields_empty_base_tier() {
        let user = TempSpecialistsDir::new();
        user.write(
            "mine",
            "---\nname: \"Mine\"\ndescription: \"d\"\n---\n\nmine body",
        );
        let missing =
            std::env::temp_dir().join(format!("intentd-missing-{}", uuid::Uuid::new_v4()));
        let svc = SpecialistsService::with_base_replacement(Some(user.path.clone()), missing);
        let listed = svc.list(None).unwrap();
        let ids: Vec<&str> = listed["specialists"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["mine"], "user tier only; no embedded fallback");
        assert!(matches!(
            svc.get("implementor", None),
            Err(Error::NotFound(_))
        ));
    }

    /// A session's pinned bundle ([`SpecialistsService::with_embedded`]) never
    /// resurrects shipped specialists behind a startup-pinned replacement.
    #[test]
    fn with_embedded_is_a_no_op_under_base_replacement() {
        static PINNED: &[(&str, &str)] = &[(
            "implementor",
            "---\nname: \"Pinned Implementor\"\ndescription: \"d\"\n---\n\npinned body",
        )];
        let user = TempSpecialistsDir::new();
        let replacement = TempSpecialistsDir::new();
        replacement.write(
            "custom",
            "---\nname: \"Custom\"\ndescription: \"d\"\n---\n\ncustom body",
        );
        let svc = SpecialistsService::with_base_replacement(
            Some(user.path.clone()),
            replacement.path.clone(),
        )
        .with_embedded(PINNED);
        assert!(matches!(
            svc.get("implementor", None),
            Err(Error::NotFound(_))
        ));
        assert!(svc.resolve("custom", None).is_some());
    }

    /// [`replacement_dir`] parsing: unset and empty mean no replacement; a
    /// non-empty value is the replacement path.
    #[test]
    fn replacement_dir_parses_unset_empty_and_set() {
        assert_eq!(replacement_dir(None), None);
        assert_eq!(replacement_dir(Some(std::ffi::OsString::new())), None);
        assert_eq!(
            replacement_dir(Some(std::ffi::OsString::from("/tmp/specialists"))),
            Some(PathBuf::from("/tmp/specialists"))
        );
    }

    #[test]
    fn resolve_role_reminder_uses_explicit_reminder() {
        let dir = TempSpecialistsDir::new();
        dir.write(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope. No refactors.\"\n---\n\nbody",
        );
        let svc = service_over(&dir);
        let (name, reminder) = svc.resolve_role_reminder("implementor", None).unwrap();
        assert_eq!(name, "Implementor");
        assert_eq!(reminder, "Stay in scope. No refactors.");
    }

    #[test]
    fn resolve_role_reminder_auto_generates_when_absent() {
        let dir = TempSpecialistsDir::new();
        dir.write(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\n---\n\nImplement your assigned task.\n\n## Hard Rules\n1. **No scope creep** — only the task\n",
        );
        let svc = service_over(&dir);
        let (name, reminder) = svc.resolve_role_reminder("implementor", None).unwrap();
        assert_eq!(name, "Implementor");
        assert_eq!(reminder, "Implement your assigned task. No scope creep.");
    }

    #[test]
    fn resolve_role_reminder_none_for_unknown_or_empty() {
        let dir = TempSpecialistsDir::new();
        // No file → unknown specialist.
        let svc = service_over(&dir);
        assert!(svc.resolve_role_reminder("missing", None).is_none());
        // Present but no reminder and an unparseable body → None.
        dir.write(
            "blank",
            "---\nname: \"Blank\"\ndescription: \"d\"\n---\n\n# Only Headers\n",
        );
        assert!(svc.resolve_role_reminder("blank", None).is_none());
    }

    /// The nine latest specialist ids embedded via `include_str!` (PP-2).
    const LATEST_EMBEDDED_IDS: [&str; 9] = [
        "spec-writer",
        "implementor",
        "verifier",
        "developer",
        "chief-of-staff",
        "ralph",
        "ui-designer",
        "pr-reviewer",
        "vulnerability-scanner",
    ];

    #[test]
    fn embedded_bundled_resolves_all_nine_with_zero_local_files() {
        // Empty user + bundled dirs: every embedded id still resolves through
        // get()/resolve_agent_type()/resolve_role_reminder(). Ralph is the one
        // retired id intentionally omitted from list().
        let dir = TempSpecialistsDir::new();
        let svc = service_over(&dir);
        for id in LATEST_EMBEDDED_IDS {
            let got = svc.get(id, None).expect("embedded specialist resolves");
            let def = &got["specialist"];
            assert_eq!(def["source"], "bundled", "{id}");
            assert_eq!(def["isCustomized"], false, "{id}");
            assert!(def.get("path").is_none(), "{id}: bundled exposes no path");
            assert!(
                !def["behaviorPrompt"].as_str().unwrap().trim().is_empty(),
                "{id}: non-empty body"
            );
        }
        let list = svc.list(None).unwrap();
        let specs = list["specialists"].as_array().unwrap();
        assert_eq!(specs.len(), 8, "Ralph is excluded from the catalog");
        for id in LATEST_EMBEDDED_IDS.into_iter().filter(|id| *id != "ralph") {
            assert!(specs.iter().any(|s| s["id"] == id), "{id} listed");
        }
        assert!(!specs.iter().any(|s| s["id"] == "ralph"));
        // The bundled chief-of-staff is flagged hidden; every other embedded
        // definition omits the field (absent ⇒ not hidden).
        for spec in specs {
            if spec["id"] == "chief-of-staff" {
                assert_eq!(spec["hidden"], true, "chief-of-staff carries hidden");
            } else {
                assert!(spec.get("hidden").is_none(), "{}: not hidden", spec["id"]);
            }
        }
        // Ralph remains fully resolvable for pinned v1 sessions even though it
        // is absent from the catalog.
        let ralph = svc.get("ralph", None).unwrap();
        assert_eq!(ralph["specialist"]["hidden"], true);
        assert_eq!(
            svc.resolve_agent_type("ralph", None).as_deref(),
            Some("ralph-loop")
        );
        let (name, reminder) = svc.resolve_role_reminder("ralph", None).unwrap();
        assert_eq!(name, "Ralph");
        assert!(reminder.starts_with("You are Ralph."));

        // Implementor also declares an explicit roleReminder.
        let (name, reminder) = svc.resolve_role_reminder("implementor", None).unwrap();
        assert_eq!(name, "Implementor");
        assert!(reminder.starts_with("Stay within task scope."));
    }

    #[test]
    fn bundled_vulnerability_scanner_resolves_supplied_definition() {
        let dir = TempSpecialistsDir::new();
        let svc = service_over(&dir);
        let got = svc
            .get("vulnerability-scanner", None)
            .expect("embedded vulnerability scanner resolves");
        let def = &got["specialist"];
        assert_eq!(def["id"], "vulnerability-scanner");
        assert_eq!(def["name"], "Vulnerability Scanner");
        assert_eq!(
            def["description"],
            "Finds real, exploitable security vulnerabilities in code"
        );
        assert!(def.get("codingAgent").is_none());
        assert!(def.get("model").is_none());
        assert_eq!(def["icon"], "pr-reviewer");
        assert!(def["prompt"]
            .as_str()
            .is_some_and(|body| body.starts_with("## Vulnerability Scanner\n")));
        assert_eq!(def["source"], "bundled");
        assert_eq!(def["isCustomized"], false);
    }

    #[test]
    fn bundled_chief_prompts_pin_exact_completion_relay_contract() {
        for (version, bundle) in [("v1", EMBEDDED_BUNDLED_V1), ("v1.1", EMBEDDED_BUNDLED_V1_1)] {
            let prompt = bundle
                .iter()
                .find_map(|(id, content)| (*id == "chief-of-staff").then_some(*content))
                .expect("bundled Chief prompt exists");
            assert!(
                prompt.contains("On the one completion wake"),
                "{version}: one completion wake"
            );
            assert!(
                prompt.contains("return await ws.app.agents.ask(agentId, message, priority)"),
                "{version}: ask result is returned without a cross-turn local"
            );
            assert!(
                !prompt.contains("const asked") && !prompt.contains("asked."),
                "{version}: no ask-local reference survives across executions"
            );
            assert!(
                prompt.contains("ws.app.agents.readConversation(target.workspaceId, target.agentId, { lastN: 20 })"),
                "{version}: one bounded conversation read"
            );
            assert!(
                prompt.contains("agentId === \"agent-id-from-completion-wake\"")
                    && prompt.contains("return { target, conversation }")
                    && prompt.contains("Do not use a variable from the earlier `ask` execution"),
                "{version}: wake identity and conversation are resolved in one execution"
            );
            assert!(
                prompt.contains("[${conversation.workspaceTitle}](intent://local/${conversation.workspaceId}/agent/${conversation.agentId}/message/${finalAssistant.id})"),
                "{version}: exact target-message link with title label"
            );
            assert!(
                prompt
                    .contains("Build this URL only from the one bounded `readConversation` result"),
                "{version}: link identifiers come from the conversation read"
            );
            assert!(
                prompt.contains(
                    "Never expose a raw workspace ID or agent ID in relay prose or link text"
                ),
                "{version}: raw IDs stay out of user-visible relay text"
            );
            assert!(
                prompt.contains("Relay that assistant message once"),
                "{version}: one final relay"
            );
        }
    }

    #[test]
    fn user_file_overrides_embedded_bundled() {
        let dir = TempSpecialistsDir::new();
        dir.write(
            "implementor",
            "---\nname: \"Custom Implementor\"\ndescription: \"d\"\n---\n\nCustom body",
        );
        let svc = service_over(&dir);
        let got = svc.get("implementor", None).unwrap();
        assert_eq!(got["specialist"]["source"], "user");
        assert_eq!(got["specialist"]["name"], "Custom Implementor");
        let list = svc.list(None).unwrap();
        let specs = list["specialists"].as_array().unwrap();
        let imp = specs.iter().find(|s| s["id"] == "implementor").unwrap();
        assert_eq!(imp["source"], "user", "user tier wins in list too");
    }

    /// Rot-check regression test: the bundled `verifier.md` prompt must instruct
    /// the verifier to mark verified tasks complete with `update_note_task_status`
    /// and explain the completion policy (only APPROVED tasks, DEVIATION/MISSING
    /// stay in `review_required`). This prevents future prompt rewrites from
    /// silently dropping the workflow instruction and leaving tasks stuck in
    /// `review_required`.
    #[test]
    fn verifier_prompt_mentions_update_note_task_status_and_marking_complete() {
        let dir = TempSpecialistsDir::new();
        let svc = service_over(&dir);
        let got = svc
            .get("verifier", None)
            .expect("embedded verifier resolves");
        let body = got["specialist"]["behaviorPrompt"]
            .as_str()
            .expect("prompt body");

        // Assert the prompt contains the `update_note_task_status` tool.
        assert!(
            body.contains("update_note_task_status"),
            "verifier.md must mention update_note_task_status tool"
        );

        // Assert the prompt uses object-style call syntax with placeholder noteId value.
        assert!(
            body.contains(
                r#"update_note_task_status({ noteId: "<task-note-id>", status: "complete" })"#
            ),
            "verifier.md must show complete object-style call with placeholder noteId"
        );

        // Assert the prompt instructs marking verified tasks complete with the exact phrase.
        assert!(
            body.contains("mark each verified task note `complete`"),
            "verifier.md must instruct marking verified tasks complete"
        );

        // Assert the prompt specifies the APPROVED → complete policy and that
        // tasks with DEVIATION/MISSING stay in review_required.
        assert!(
            (body.contains("APPROVED") || body.contains("✅ APPROVED"))
                && (body.contains("DEVIATION") || body.contains("⚠️ DEVIATION"))
                && (body.contains("MISSING") || body.contains("❌ MISSING")),
            "verifier.md must specify APPROVED/DEVIATION/MISSING completion policy"
        );
    }

    #[test]
    fn bundled_dir_file_overrides_embedded_copy() {
        // An on-disk bundled file (env override / packaged resources) with the
        // same id wins over the embedded copy within the bundled tier.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write(
            "verifier",
            "---\nname: \"Patched Verifier\"\ndescription: \"d\"\n---\n\nPatched body",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("verifier", None).unwrap();
        assert_eq!(got["specialist"]["source"], "bundled");
        assert_eq!(got["specialist"]["name"], "Patched Verifier");
        assert_eq!(got["specialist"]["prompt"], "Patched body");
    }

    #[test]
    fn user_override_without_hidden_inherits_hidden_from_embedded() {
        // Regression: a user-tier chief-of-staff.md materialized before the
        // hidden feature (no `hidden` key) must not resurface the specialist —
        // `hidden` inherits from the embedded bundled floor (PROTOCOL §5.11).
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        user.write(
            "chief-of-staff",
            "---\nname: \"Chief of Staff\"\ndescription: \"User override\"\n---\n\nYou orchestrate.",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("chief-of-staff", None).unwrap();
        assert_eq!(got["specialist"]["source"], "user");
        assert_eq!(
            got["specialist"]["hidden"], true,
            "hidden inherited from lower tier on get"
        );
        let list = svc.list(None).unwrap();
        let specs = list["specialists"].as_array().unwrap();
        let chief = specs.iter().find(|s| s["id"] == "chief-of-staff").unwrap();
        assert_eq!(chief["source"], "user");
        assert_eq!(
            chief["hidden"], true,
            "hidden inherited from lower tier in list"
        );
    }

    #[test]
    fn explicit_hidden_false_in_higher_tier_unhides() {
        // Explicit `hidden: false` in a higher tier is the opt-out.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        user.write(
            "chief-of-staff",
            "---\nname: \"Chief of Staff\"\ndescription: \"User override\"\nhidden: false\n---\n\nYou orchestrate.",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("chief-of-staff", None).unwrap();
        assert!(
            got["specialist"].get("hidden").is_none(),
            "explicit false unhides on get"
        );
        let list = svc.list(None).unwrap();
        let specs = list["specialists"].as_array().unwrap();
        let chief = specs.iter().find(|s| s["id"] == "chief-of-staff").unwrap();
        assert!(
            chief.get("hidden").is_none(),
            "explicit false unhides in list"
        );
    }

    #[test]
    fn project_tier_inherits_hidden_and_explicit_false_overrides() {
        // A user tier hides "ghost"; a project override without the key
        // inherits, and an explicit false unhides.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        let work = TempSpecialistsDir::new();
        user.write(
            "ghost",
            "---\nname: \"Ghost\"\ndescription: \"d\"\nhidden: true\n---\n\nbody",
        );
        let proj = work.path.join(".intent").join(SPECIALISTS_FOLDER);
        std::fs::create_dir_all(&proj).unwrap();
        std::fs::write(
            proj.join("ghost.md"),
            "---\nname: \"Ghost\"\ndescription: \"proj\"\n---\n\nbody",
        )
        .unwrap();
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("ghost", Some(&work.path)).unwrap();
        assert_eq!(got["specialist"]["source"], "project");
        assert_eq!(
            got["specialist"]["hidden"], true,
            "project file without the key inherits"
        );
        let list = svc.list(Some(&work.path)).unwrap();
        let specs = list["specialists"].as_array().unwrap();
        let ghost = specs.iter().find(|s| s["id"] == "ghost").unwrap();
        assert_eq!(ghost["hidden"], true, "list inherits through the tiers");
        std::fs::write(
            proj.join("ghost.md"),
            "---\nname: \"Ghost\"\ndescription: \"proj\"\nhidden: false\n---\n\nbody",
        )
        .unwrap();
        let got = svc.get("ghost", Some(&work.path)).unwrap();
        assert!(
            got["specialist"].get("hidden").is_none(),
            "explicit false in the project tier unhides"
        );
    }

    #[test]
    fn render_file_preserves_explicit_hidden_false() {
        // Explicit false is written verbatim (the opt-out that unhides);
        // omission writes no key, which inherits at resolution time.
        let spec =
            json!({ "name": "Ghost", "description": "d", "hidden": false, "prompt": "body" });
        assert!(render_file("ghost", &spec).contains("hidden: false"));
        let spec = json!({ "name": "Ghost", "description": "d", "prompt": "body" });
        assert!(!render_file("ghost", &spec).contains("hidden"));
    }

    #[test]
    fn bundled_dir_override_without_hidden_inherits_from_embedded() {
        // The first step of the fold: an on-disk bundled_dir file that omits
        // the key inherits `hidden` from the embedded floor (PROTOCOL §5.11).
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write(
            "chief-of-staff",
            "---\nname: \"Chief of Staff\"\ndescription: \"Patched bundled\"\n---\n\nPatched body",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("chief-of-staff", None).unwrap();
        assert_eq!(got["specialist"]["source"], "bundled");
        assert_eq!(got["specialist"]["description"], "Patched bundled");
        assert_eq!(
            got["specialist"]["hidden"], true,
            "bundled dir file without the key inherits from the embedded floor on get"
        );
        let list = svc.list(None).unwrap();
        let specs = list["specialists"].as_array().unwrap();
        let chief = specs.iter().find(|s| s["id"] == "chief-of-staff").unwrap();
        assert_eq!(
            chief["hidden"], true,
            "bundled dir file without the key inherits from the embedded floor in list"
        );
    }

    #[test]
    fn unrecognized_hidden_values_inherit() {
        // Only case-insensitive `true`/`false` are recognized; YAML 1.1 truthy
        // spellings (`yes`, `on`, `1`) parse as `None` and inherit the lower
        // tier's value instead of unhiding (or hiding) anything.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        // Over a hidden lower tier the unrecognized value keeps it hidden.
        user.write(
            "chief-of-staff",
            "---\nname: \"Chief of Staff\"\ndescription: \"d\"\nhidden: yes\n---\n\nbody",
        );
        // Over a visible lower tier the unrecognized value does not hide.
        user.write(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nhidden: on\n---\n\nbody",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("chief-of-staff", None).unwrap();
        assert_eq!(
            got["specialist"]["hidden"], true,
            "hidden: yes inherits the embedded floor's hidden: true"
        );
        let got = svc.get("implementor", None).unwrap();
        assert!(
            got["specialist"].get("hidden").is_none(),
            "hidden: on inherits the embedded floor's not-hidden"
        );
    }

    #[test]
    fn create_and_edit_responses_fold_hidden_from_lower_tiers() {
        // The create/edit response must agree with an immediately-following
        // get: a spec that omits `hidden` inherits from lower tiers, and an
        // explicit false is the opt-out (PROTOCOL §5.11).
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let spec = json!({ "name": "Chief of Staff", "description": "d", "prompt": "body" });
        let created = svc
            .create("chief-of-staff", &spec, Some("user"), None)
            .unwrap();
        assert_eq!(
            created["specialist"]["hidden"], true,
            "create response inherits hidden from the embedded floor"
        );
        let got = svc.get("chief-of-staff", None).unwrap();
        assert_eq!(
            got["specialist"]["hidden"], created["specialist"]["hidden"],
            "create response agrees with the following get"
        );
        let spec = json!({
            "name": "Chief of Staff",
            "description": "d",
            "hidden": false,
            "prompt": "body"
        });
        let edited = svc.edit("chief-of-staff", &spec, "user", None).unwrap();
        assert!(
            edited["specialist"].get("hidden").is_none(),
            "edit response honors the explicit false opt-out"
        );
        let got = svc.get("chief-of-staff", None).unwrap();
        assert!(
            got["specialist"].get("hidden").is_none(),
            "edit response agrees with the following get"
        );
    }

    #[test]
    fn user_override_omitting_scalars_inherits_bundled_values() {
        // A user override that omits the config scalars (codingAgent, model,
        // agentType) inherits the bundled tier's effective values on get,
        // list, and the spawn-time resolvers. A retired `modelTier:` line in
        // the bundled file is ignored and never surfaces.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"d\"\ncodingAgent: \"claude\"\nmodel: \"opus4.5\"\nmodelTier: \"smart\"\nagentType: \"zeta-type\"\n---\n\nbody",
        );
        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user override\"\n---\n\nuser body",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("zeta", None).unwrap();
        let def = &got["specialist"];
        assert_eq!(def["source"], "user");
        assert_eq!(def["codingAgent"], "claude", "codingAgent inherited on get");
        assert_eq!(def["model"], "opus4.5", "model inherited on get");
        assert!(def.get("modelTier").is_none(), "retired modelTier ignored");
        assert_eq!(def["agentType"], "zeta-type", "agentType inherited on get");
        let list = svc.list(None).unwrap();
        let specs = list["specialists"].as_array().unwrap();
        let zeta = specs.iter().find(|s| s["id"] == "zeta").unwrap();
        assert_eq!(
            zeta["codingAgent"], "claude",
            "codingAgent inherited in list"
        );
        assert_eq!(zeta["model"], "opus4.5", "model inherited in list");
        assert!(
            zeta.get("modelTier").is_none(),
            "retired modelTier ignored in list"
        );
        assert_eq!(
            zeta["agentType"], "zeta-type",
            "agentType inherited in list"
        );
        assert_eq!(svc.resolve_model("zeta", None).as_deref(), Some("opus4.5"));
        assert_eq!(
            svc.resolve_agent_type("zeta", None).as_deref(),
            Some("zeta-type")
        );
    }

    #[test]
    fn user_override_of_embedded_inherits_scalars_at_spawn() {
        // The embedded floor participates in the fold: a user ralph.md that
        // omits agentType keeps the embedded value at spawn time, but remains
        // absent from list() as a retired catalog id.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        user.write(
            "ralph",
            "---\nname: \"Custom Ralph\"\ndescription: \"d\"\n---\n\nCustom body",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        assert_eq!(
            svc.resolve_agent_type("ralph", None).as_deref(),
            Some("ralph-loop"),
            "agentType inherited from the embedded floor"
        );
        let got = svc.get("ralph", None).unwrap();
        assert_eq!(got["specialist"]["source"], "user");
        assert_eq!(got["specialist"]["hidden"], true);
        assert!(svc.list(None).unwrap()["specialists"]
            .as_array()
            .unwrap()
            .iter()
            .all(|spec| spec["id"] != "ralph"));
        assert!(
            got["specialist"].get("modelTier").is_none(),
            "modelTier is retired and never emitted"
        );
    }

    #[test]
    fn create_and_edit_tolerate_and_drop_retired_model_tier() {
        // Retirement regression (PROTOCOL §5.11): a `modelTier` in
        // `specialist.create`/`edit` params succeeds (no -32602), is never
        // echoed, and is never written to the file.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let created = svc
            .create(
                "tiered",
                &json!({
                    "id": "tiered", "name": "Tiered", "description": "d",
                    "modelTier": "smart", "prompt": "body"
                }),
                None,
                None,
            )
            .expect("create with retired modelTier succeeds");
        assert!(created["specialist"].get("modelTier").is_none());
        let content = std::fs::read_to_string(user.path.join("tiered.md")).unwrap();
        assert!(!content.contains("modelTier"), "never written: {content}");

        let edited = svc
            .edit(
                "tiered",
                &json!({
                    "id": "tiered", "name": "Tiered", "description": "d",
                    "modelTier": "fast", "prompt": "body v2"
                }),
                "user",
                None,
            )
            .expect("edit with retired modelTier succeeds");
        assert!(edited["specialist"].get("modelTier").is_none());
        let content = std::fs::read_to_string(user.path.join("tiered.md")).unwrap();
        assert!(!content.contains("modelTier"), "never written: {content}");
    }

    #[test]
    fn edit_rewrite_drops_preexisting_model_tier_line() {
        // Retirement regression (PROTOCOL §5.11): a pre-existing `modelTier:`
        // frontmatter line is ignored on parse (never echoed) and dropped from
        // the file on the next `specialist.edit` rewrite.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        user.write(
            "legacy",
            "---\nname: \"Legacy\"\ndescription: \"d\"\nmodel: \"opus4.5\"\nmodelTier: \"smart\"\n---\n\nbody",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("legacy", None).unwrap();
        assert!(got["specialist"].get("modelTier").is_none());
        assert_eq!(got["specialist"]["model"], "opus4.5");

        let edited = svc
            .edit(
                "legacy",
                &json!({
                    "id": "legacy", "name": "Legacy", "description": "d",
                    "model": "opus4.5", "prompt": "body v2"
                }),
                "user",
                None,
            )
            .expect("edit succeeds");
        assert!(edited["specialist"].get("modelTier").is_none());
        let content = std::fs::read_to_string(user.path.join("legacy.md")).unwrap();
        assert!(
            !content.contains("modelTier"),
            "rewrite drops the retired key: {content}"
        );
        assert!(content.contains("model: \"opus4.5\""), "model kept");
    }

    #[test]
    fn explicit_scalar_in_higher_tier_overrides_inherited() {
        // An explicit non-empty value in the higher tier wins per key; the
        // other scalars still inherit independently.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"d\"\nmodel: \"opus4.5\"\nagentType: \"zeta-type\"\n---\n\nbody",
        );
        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\nmodel: \"haiku\"\n---\n\nbody",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("zeta", None).unwrap();
        assert_eq!(got["specialist"]["model"], "haiku", "explicit value wins");
        assert_eq!(
            got["specialist"]["agentType"], "zeta-type",
            "omitted key still inherits independently"
        );
        assert_eq!(svc.resolve_model("zeta", None).as_deref(), Some("haiku"));
    }

    #[test]
    fn explicit_empty_scalar_clears_inherited_value() {
        // An explicit empty value (`model: ""`) clears the inherited value:
        // the resolved def carries no key and resolve_model returns None.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"d\"\nmodel: \"opus4.5\"\nagentType: \"zeta-type\"\n---\n\nbody",
        );
        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\nmodel: \"\"\n---\n\nbody",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("zeta", None).unwrap();
        assert!(
            got["specialist"].get("model").is_none(),
            "explicit empty clears the inherited model"
        );
        assert_eq!(
            got["specialist"]["agentType"], "zeta-type",
            "other scalars still inherit"
        );
        assert_eq!(svc.resolve_model("zeta", None), None);
    }

    #[test]
    fn role_reminder_does_not_inherit_across_tiers() {
        // roleReminder stays winner-takes-all: a higher tier that omits it
        // drops the lower tier's value and the auto-derive fallback applies.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"d\"\nroleReminder: \"Bundled reminder.\"\n---\n\nBundled body.",
        );
        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\n---\n\nUser first line.",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("zeta", None).unwrap();
        assert!(
            got["specialist"].get("roleReminder").is_none(),
            "roleReminder is not inherited"
        );
        let (_, reminder) = svc.resolve_role_reminder("zeta", None).unwrap();
        assert_eq!(
            reminder, "User first line.",
            "auto-derive fallback applies instead of the lower tier's reminder"
        );
    }

    #[test]
    fn create_and_edit_responses_fold_scalars_from_lower_tiers() {
        // The create/edit response must agree with an immediately-following
        // get when the written spec omits (or explicitly clears) a scalar.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"d\"\nmodel: \"opus4.5\"\nagentType: \"zeta-type\"\n---\n\nbody",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let spec = json!({ "name": "Zeta", "description": "user", "prompt": "body" });
        let created = svc.create("zeta", &spec, Some("user"), None).unwrap();
        assert_eq!(
            created["specialist"]["model"], "opus4.5",
            "create response inherits model from the bundled tier"
        );
        assert_eq!(
            created["specialist"]["agentType"], "zeta-type",
            "create response inherits agentType from the bundled tier"
        );
        let got = svc.get("zeta", None).unwrap();
        assert_eq!(
            got["specialist"]["model"], created["specialist"]["model"],
            "create response agrees with the following get"
        );
        let spec = json!({
            "name": "Zeta",
            "description": "user",
            "model": "",
            "prompt": "body"
        });
        let edited = svc.edit("zeta", &spec, "user", None).unwrap();
        assert!(
            edited["specialist"].get("model").is_none(),
            "edit response honors the explicit-empty clear"
        );
        assert_eq!(
            edited["specialist"]["agentType"], "zeta-type",
            "edit response still inherits the untouched scalar"
        );
        let got = svc.get("zeta", None).unwrap();
        assert!(
            got["specialist"].get("model").is_none(),
            "edit response agrees with the following get"
        );
    }

    #[test]
    fn render_file_writes_explicit_empty_scalar_for_clear() {
        // A wire spec that explicitly carries `""` for a config scalar writes
        // `key: ""` (the explicit-clear form); an absent key writes nothing,
        // and roleReminder rendering is unchanged (empty is skipped).
        let spec = json!({ "name": "Zeta", "description": "d", "model": "", "prompt": "body" });
        let rendered = render_file("zeta", &spec);
        assert!(
            rendered.contains("model: \"\""),
            "explicit empty is written"
        );
        let (fm, _) = parse_frontmatter(&rendered);
        assert_eq!(
            fm.get("model").unwrap(),
            "",
            "round-trips as an explicit-clear (Some(\"\"))"
        );
        let spec = json!({ "name": "Zeta", "description": "d", "prompt": "body" });
        assert!(
            !render_file("zeta", &spec).contains("model"),
            "absent key writes nothing"
        );
        let spec = json!({
            "name": "Zeta",
            "description": "d",
            "roleReminder": "",
            "prompt": "body"
        });
        assert!(
            !render_file("zeta", &spec).contains("roleReminder"),
            "empty roleReminder is still skipped"
        );
    }

    #[test]
    fn model_options_round_trip_losslessly() {
        // A wire spec's modelOptions list is written as a single-line
        // JSON-array frontmatter scalar and parses back byte-identical.
        let spec = json!({
            "name": "Zeta",
            "description": "d",
            "modelOptions": [
                { "model": "opencode:kimi-k3", "hint": "cheap & fast" },
                { "model": "opus4.5", "hint": "smart, \"expensive\"" }
            ],
            "prompt": "body"
        });
        let rendered = render_file("zeta", &spec);
        assert!(
            rendered.contains(
                r#"modelOptions: [{"model":"opencode:kimi-k3","hint":"cheap & fast"},{"model":"opus4.5","hint":"smart, \"expensive\""}]"#
            ),
            "single-line JSON-array scalar is written: {rendered}"
        );
        let def = build_def("zeta", &rendered, "user", Path::new("/tmp/zeta.md"));
        assert_eq!(
            def["modelOptions"],
            json!([
                { "model": "opencode:kimi-k3", "hint": "cheap & fast" },
                { "model": "opus4.5", "hint": "smart, \"expensive\"" }
            ]),
            "parse→write→parse round-trips losslessly"
        );
        // A second write of the parsed def is byte-identical (stable order).
        assert_eq!(render_file("zeta", &def), rendered);
    }

    #[test]
    fn model_options_hint_defaults_to_empty_string() {
        // An entry without a hint normalizes to hint: "" on parse and write.
        let spec = json!({
            "name": "Zeta",
            "description": "d",
            "modelOptions": [{ "model": "opus4.5" }],
            "prompt": "body"
        });
        let rendered = render_file("zeta", &spec);
        let def = build_def("zeta", &rendered, "user", Path::new("/tmp/zeta.md"));
        assert_eq!(
            def["modelOptions"],
            json!([{ "model": "opus4.5", "hint": "" }])
        );
    }

    #[test]
    fn model_options_carry_reasoning_effort() {
        // A per-option `reasoningEffort` round-trips through the single-line
        // JSON-array scalar; an empty/whitespace-only one is dropped and a
        // non-string one is rejected by the strict wire validator.
        let spec = json!({
            "name": "Zeta",
            "description": "d",
            "modelOptions": [
                { "model": "fable-5", "hint": "hard", "reasoningEffort": "high" },
                { "model": "sonnet5", "hint": "", "reasoningEffort": "  " }
            ],
            "prompt": "body"
        });
        let rendered = render_file("zeta", &spec);
        let def = build_def("zeta", &rendered, "user", Path::new("/tmp/zeta.md"));
        assert_eq!(
            def["modelOptions"],
            json!([
                { "model": "fable-5", "hint": "hard", "reasoningEffort": "high" },
                { "model": "sonnet5", "hint": "" }
            ])
        );
        assert_eq!(render_file("zeta", &def), rendered);
        assert!(validate_model_options_spec(Some(&json!([
            { "model": "fable-5", "reasoningEffort": 3 }
        ])))
        .is_err());
    }

    #[test]
    fn reasoning_effort_scalar_inherits_across_tiers() {
        // `reasoningEffort` folds like the other config scalars: an omitted
        // key keeps the lower tier's value, an explicit empty value clears it,
        // and a non-empty value overrides it.
        let bundled =
            "---\nname: \"Z\"\ndescription: \"d\"\nreasoningEffort: \"high\"\n---\n\nbody";
        let base = build_def("z", bundled, "bundled", Path::new("/tmp/z.md"));
        assert_eq!(base["reasoningEffort"], json!("high"));

        let omits = "---\nname: \"Z\"\ndescription: \"d\"\n---\n\nuser body";
        let inherited =
            build_def_inheriting("z", omits, "user", Path::new("/tmp/z.md"), Some(&base));
        assert_eq!(
            inherited["reasoningEffort"],
            json!("high"),
            "omitted inherits"
        );

        let overrides = "---\nname: \"Z\"\ndescription: \"d\"\nreasoningEffort: \"low\"\n---\n\nb";
        let overridden =
            build_def_inheriting("z", overrides, "user", Path::new("/tmp/z.md"), Some(&base));
        assert_eq!(overridden["reasoningEffort"], json!("low"));

        let clears = "---\nname: \"Z\"\ndescription: \"d\"\nreasoningEffort: \"\"\n---\n\nb";
        let cleared =
            build_def_inheriting("z", clears, "user", Path::new("/tmp/z.md"), Some(&base));
        assert!(
            cleared.get("reasoningEffort").is_none(),
            "explicit empty clears the inherited value"
        );
    }

    #[test]
    fn model_options_frontmatter_is_lenient_on_read() {
        // Unparseable frontmatter is tolerated like an omitted key and
        // unusable entries are skipped individually — files are never
        // rejected on read.
        let bad_json = "---\nname: \"Z\"\ndescription: \"d\"\nmodelOptions: not-json\n---\n\nbody";
        let def = build_def("z", bad_json, "user", Path::new("/tmp/z.md"));
        assert!(
            def.get("modelOptions").is_none(),
            "unparseable value is treated as omitted"
        );
        let mixed = "---\nname: \"Z\"\ndescription: \"d\"\nmodelOptions: [{\"model\":\"opus4.5\",\"hint\":\"ok\"},{\"model\":\"\"},{\"hint\":\"no model\"},\"scalar\"]\n---\n\nbody";
        let def = build_def("z", mixed, "user", Path::new("/tmp/z.md"));
        assert_eq!(
            def["modelOptions"],
            json!([{ "model": "opus4.5", "hint": "ok" }]),
            "invalid entries are skipped individually"
        );
        let all_bad =
            "---\nname: \"Z\"\ndescription: \"d\"\nmodelOptions: [{\"hint\":\"no model\"},{\"model\":\"\"}]\n---\n\nbody";
        let def = build_def("z", all_bad, "user", Path::new("/tmp/z.md"));
        assert!(
            def.get("modelOptions").is_none(),
            "a non-empty array with only unusable entries is treated as omitted, not a clear"
        );
    }

    #[test]
    fn model_options_inherit_across_tiers_with_explicit_clear() {
        // Inherit-on-omit fold: a user file that omits the key inherits the
        // bundled tier's list; an explicit `[]` clears it; a non-empty list
        // overrides it wholesale (entries never merge).
        let bundled_fm = "---\nname: \"Zeta\"\ndescription: \"d\"\nmodelOptions: [{\"model\":\"opus4.5\",\"hint\":\"smart\"}]\n---\n\nbody";

        // Omit → inherit.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write("zeta", bundled_fm);
        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\n---\n\nbody",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("zeta", None).unwrap();
        assert_eq!(
            got["specialist"]["modelOptions"],
            json!([{ "model": "opus4.5", "hint": "smart" }]),
            "omitted key inherits the lower tier's list"
        );

        // Explicit `[]` → clear.
        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\nmodelOptions: []\n---\n\nbody",
        );
        let got = svc.get("zeta", None).unwrap();
        assert!(
            got["specialist"].get("modelOptions").is_none(),
            "explicit [] clears the inherited list"
        );

        // Non-empty → wholesale override.
        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\nmodelOptions: [{\"model\":\"opencode:kimi-k3\",\"hint\":\"cheap\"}]\n---\n\nbody",
        );
        let got = svc.get("zeta", None).unwrap();
        assert_eq!(
            got["specialist"]["modelOptions"],
            json!([{ "model": "opencode:kimi-k3", "hint": "cheap" }]),
            "a non-empty list overrides wholesale, never merges"
        );

        // Non-empty but all-unusable → inherit, not clear.
        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\nmodelOptions: [{\"hint\":\"no model\"}]\n---\n\nbody",
        );
        let got = svc.get("zeta", None).unwrap();
        assert_eq!(
            got["specialist"]["modelOptions"],
            json!([{ "model": "opus4.5", "hint": "smart" }]),
            "an all-unusable non-empty array inherits instead of clearing"
        );
    }

    #[test]
    fn create_and_edit_responses_fold_model_options_from_lower_tiers() {
        // The create/edit response agrees with an immediately-following get
        // when the written spec omits (or explicitly clears) modelOptions.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"d\"\nmodelOptions: [{\"model\":\"opus4.5\",\"hint\":\"smart\"}]\n---\n\nbody",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let spec = json!({ "name": "Zeta", "description": "user", "prompt": "body" });
        let created = svc.create("zeta", &spec, Some("user"), None).unwrap();
        assert_eq!(
            created["specialist"]["modelOptions"],
            json!([{ "model": "opus4.5", "hint": "smart" }]),
            "create response inherits modelOptions from the bundled tier"
        );
        assert_eq!(
            svc.get("zeta", None).unwrap()["specialist"]["modelOptions"],
            created["specialist"]["modelOptions"],
            "create response agrees with the following get"
        );
        let spec = json!({
            "name": "Zeta",
            "description": "user",
            "modelOptions": [],
            "prompt": "body"
        });
        let edited = svc.edit("zeta", &spec, "user", None).unwrap();
        assert!(
            edited["specialist"].get("modelOptions").is_none(),
            "edit response honors the explicit [] clear"
        );
        assert!(
            svc.get("zeta", None).unwrap()["specialist"]
                .get("modelOptions")
                .is_none(),
            "edit response agrees with the following get"
        );
    }

    #[test]
    fn create_and_edit_reject_invalid_model_options() {
        // Any invalid wire shape → InvalidParams (-32602), for both create
        // and edit, before anything is written.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let invalid_specs = [
            json!({ "name": "Z", "description": "d", "modelOptions": "not-an-array" }),
            json!({ "name": "Z", "description": "d", "modelOptions": ["scalar"] }),
            json!({ "name": "Z", "description": "d", "modelOptions": [{ "hint": "no model" }] }),
            json!({ "name": "Z", "description": "d", "modelOptions": [{ "model": "" }] }),
            json!({ "name": "Z", "description": "d", "modelOptions": [{ "model": "   " }] }),
            json!({ "name": "Z", "description": "d", "modelOptions": [{ "model": 42 }] }),
            json!({ "name": "Z", "description": "d",
                "modelOptions": [{ "model": "opus4.5", "hint": 42 }] }),
        ];
        for spec in &invalid_specs {
            let err = svc.create("zeta", spec, Some("user"), None).unwrap_err();
            assert!(
                matches!(err, Error::InvalidParams(_)),
                "create rejects {spec} with InvalidParams, got {err:?}"
            );
            assert!(
                !user.path.join("zeta.md").exists(),
                "nothing is written on a rejected create"
            );
        }
        // Seed a valid file, then verify edit rejects the same shapes.
        let valid = json!({ "name": "Z", "description": "d", "prompt": "body" });
        svc.create("zeta", &valid, Some("user"), None).unwrap();
        for spec in &invalid_specs {
            let err = svc.edit("zeta", spec, "user", None).unwrap_err();
            assert!(
                matches!(err, Error::InvalidParams(_)),
                "edit rejects {spec} with InvalidParams, got {err:?}"
            );
        }
    }

    #[test]
    fn role_icon_and_team_agents_round_trip_losslessly() {
        // The picker-metadata fields write as frontmatter (role/icon as
        // quoted scalars, teamAgents as a single-line JSON-array scalar) and
        // parse back byte-identical.
        let spec = json!({
            "name": "Zeta",
            "description": "d",
            "role": "orchestrator",
            "icon": "coordinator",
            "teamAgents": ["implementor", "verifier"],
            "prompt": "body"
        });
        let rendered = render_file("zeta", &spec);
        assert!(rendered.contains("role: \"orchestrator\""), "{rendered}");
        assert!(rendered.contains("icon: \"coordinator\""), "{rendered}");
        assert!(
            rendered.contains(r#"teamAgents: ["implementor","verifier"]"#),
            "single-line JSON-array scalar is written: {rendered}"
        );
        let def = build_def("zeta", &rendered, "user", Path::new("/tmp/zeta.md"));
        assert_eq!(def["role"], "orchestrator");
        assert_eq!(def["icon"], "coordinator");
        assert_eq!(def["teamAgents"], json!(["implementor", "verifier"]));
        // A second write of the parsed def is byte-identical (stable order).
        assert_eq!(render_file("zeta", &def), rendered);
    }

    #[test]
    fn role_icon_inherit_across_tiers_with_explicit_clear() {
        // role/icon fold like the other config scalars: omit → inherit,
        // explicit empty → clear, non-empty → override.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"d\"\nrole: \"internal\"\nicon: \"verifier\"\n---\n\nbody",
        );
        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\n---\n\nbody",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("zeta", None).unwrap();
        assert_eq!(
            got["specialist"]["role"], "internal",
            "omitted role inherits"
        );
        assert_eq!(
            got["specialist"]["icon"], "verifier",
            "omitted icon inherits"
        );

        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\nrole: \"\"\nicon: \"ralph\"\n---\n\nbody",
        );
        let got = svc.get("zeta", None).unwrap();
        assert!(
            got["specialist"].get("role").is_none(),
            "explicit empty clears the inherited role"
        );
        assert_eq!(
            got["specialist"]["icon"], "ralph",
            "explicit non-empty icon overrides"
        );
    }

    #[test]
    fn team_agents_inherit_across_tiers_with_explicit_clear() {
        // Inherit-on-omit fold: omit → inherit, explicit `[]` → clear,
        // non-empty → wholesale override (entries never merge).
        let bundled_fm = "---\nname: \"Zeta\"\ndescription: \"d\"\nteamAgents: [\"implementor\",\"verifier\"]\n---\n\nbody";
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write("zeta", bundled_fm);
        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\n---\n\nbody",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("zeta", None).unwrap();
        assert_eq!(
            got["specialist"]["teamAgents"],
            json!(["implementor", "verifier"]),
            "omitted key inherits the lower tier's list"
        );

        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\nteamAgents: []\n---\n\nbody",
        );
        let got = svc.get("zeta", None).unwrap();
        assert!(
            got["specialist"].get("teamAgents").is_none(),
            "explicit [] clears the inherited list"
        );

        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\nteamAgents: [\"reviewer\"]\n---\n\nbody",
        );
        let got = svc.get("zeta", None).unwrap();
        assert_eq!(
            got["specialist"]["teamAgents"],
            json!(["reviewer"]),
            "a non-empty list overrides wholesale, never merges"
        );
    }

    #[test]
    fn role_and_team_agents_frontmatter_are_lenient_on_read() {
        // Files are never rejected on read: an out-of-enum role is
        // normalized to omitted (so get→modify→edit never echoes a value the
        // strict write validation rejects), an unparseable teamAgents is
        // treated as omitted, and unusable entries are skipped individually
        // (all-unusable ⇒ omitted, not clear).
        let content =
            "---\nname: \"Z\"\ndescription: \"d\"\nrole: \"mystery\"\nteamAgents: not-json\n---\n\nbody";
        let def = build_def("z", content, "user", Path::new("/tmp/z.md"));
        assert!(
            def.get("role").is_none(),
            "out-of-enum role is normalized to omitted"
        );
        assert!(
            def.get("teamAgents").is_none(),
            "unparseable value is treated as omitted"
        );
        // An out-of-enum role inherits like an omitted key: the def echoed
        // by get is always writable back through the strict edit validation.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        bundled.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"d\"\nrole: \"internal\"\n---\n\nbody",
        );
        user.write(
            "zeta",
            "---\nname: \"Zeta\"\ndescription: \"user\"\nrole: \"mystery\"\n---\n\nbody",
        );
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let got = svc.get("zeta", None).unwrap();
        assert_eq!(
            got["specialist"]["role"], "internal",
            "an out-of-enum role inherits the lower tier's value like an omitted key"
        );
        let echoed = got["specialist"].clone();
        svc.edit("zeta", &echoed, "user", None)
            .expect("a get→edit echo of a def never fails role validation");
        let mixed = "---\nname: \"Z\"\ndescription: \"d\"\nteamAgents: [\"implementor\", 42, \"\", \"verifier\"]\n---\n\nbody";
        let def = build_def("z", mixed, "user", Path::new("/tmp/z.md"));
        assert_eq!(
            def["teamAgents"],
            json!(["implementor", "verifier"]),
            "unusable entries are skipped individually"
        );
        let all_bad = "---\nname: \"Z\"\ndescription: \"d\"\nteamAgents: [42, \"\"]\n---\n\nbody";
        let def = build_def("z", all_bad, "user", Path::new("/tmp/z.md"));
        assert!(
            def.get("teamAgents").is_none(),
            "a non-empty array with only unusable entries is treated as omitted, not a clear"
        );
    }

    #[test]
    fn create_and_edit_reject_invalid_role_and_team_agents() {
        // Invalid wire shapes → InvalidParams (-32602) for both create and
        // edit; the valid enum values (and explicit clears) are accepted.
        let user = TempSpecialistsDir::new();
        let bundled = TempSpecialistsDir::new();
        let svc = SpecialistsService::new(Some(user.path.clone()), Some(bundled.path.clone()));
        let invalid_specs = [
            json!({ "name": "Z", "description": "d", "role": "manager" }),
            json!({ "name": "Z", "description": "d", "role": 42 }),
            json!({ "name": "Z", "description": "d", "icon": 42 }),
            json!({ "name": "Z", "description": "d", "icon": ["coordinator"] }),
            json!({ "name": "Z", "description": "d", "teamAgents": "not-an-array" }),
            json!({ "name": "Z", "description": "d", "teamAgents": [42] }),
            json!({ "name": "Z", "description": "d", "teamAgents": [""] }),
            json!({ "name": "Z", "description": "d", "teamAgents": ["   "] }),
        ];
        for spec in &invalid_specs {
            let err = svc.create("zeta", spec, Some("user"), None).unwrap_err();
            assert!(
                matches!(err, Error::InvalidParams(_)),
                "create rejects {spec} with InvalidParams, got {err:?}"
            );
            assert!(
                !user.path.join("zeta.md").exists(),
                "nothing is written on a rejected create"
            );
        }
        let valid = json!({
            "name": "Z",
            "description": "d",
            "role": "internal",
            "teamAgents": ["implementor"],
            "prompt": "body"
        });
        let created = svc.create("zeta", &valid, Some("user"), None).unwrap();
        assert_eq!(created["specialist"]["role"], "internal");
        assert_eq!(created["specialist"]["teamAgents"], json!(["implementor"]));
        for spec in &invalid_specs {
            let err = svc.edit("zeta", spec, "user", None).unwrap_err();
            assert!(
                matches!(err, Error::InvalidParams(_)),
                "edit rejects {spec} with InvalidParams, got {err:?}"
            );
        }
        // Explicit clears are valid wire values.
        let cleared = json!({
            "name": "Z",
            "description": "d",
            "role": "",
            "teamAgents": [],
            "prompt": "body"
        });
        let edited = svc.edit("zeta", &cleared, "user", None).unwrap();
        assert!(edited["specialist"].get("role").is_none());
        assert!(edited["specialist"].get("teamAgents").is_none());
    }

    #[test]
    fn embedded_bundle_carries_picker_metadata() {
        // The latest embedded bundle resolves the picker-metadata fields:
        // spec-writer is the orchestrator with its advisory roster,
        // implementor/verifier are internal, and every def carries an icon.
        let dir = TempSpecialistsDir::new();
        let svc = service_over(&dir);
        let sw = svc.get("spec-writer", None).unwrap();
        assert_eq!(sw["specialist"]["role"], "orchestrator");
        assert_eq!(
            sw["specialist"]["teamAgents"],
            json!(["implementor", "verifier"])
        );
        assert_eq!(sw["specialist"]["icon"], "coordinator");
        for id in ["implementor", "verifier"] {
            let got = svc.get(id, None).unwrap();
            assert_eq!(got["specialist"]["role"], "internal", "{id}");
            assert_eq!(got["specialist"]["icon"], id, "{id}");
        }
        for id in LATEST_EMBEDDED_IDS {
            let got = svc.get(id, None).unwrap();
            assert!(
                got["specialist"]["icon"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty()),
                "{id}: carries an icon"
            );
        }
    }
}
