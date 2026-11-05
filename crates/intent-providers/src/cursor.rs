//! Cursor CLI (`cursor-agent`) discovery, identity, and `models` parsing.
//!
//! Pure helpers (no process spawning): the daemon layer runs
//! `cursor-agent --version` / `cursor-agent models` and hands the output here.
//!
//! Facts these helpers encode (Cursor CLI 2026.11.05-86047df, Linux x64):
//!
//! - The installer puts the real launcher at
//!   `~/.local/share/cursor-agent/versions/<ver>/cursor-agent` and symlinks
//!   both `~/.local/bin/cursor-agent` and `~/.local/bin/agent` to it. A bare
//!   `agent` is never resolved: it is a generic name and it is the first link
//!   the Grok agent host channel repoints.
//! - `cursor-agent update --channel grok` swaps the links to
//!   `$GROK_HOME/bin/agent-host` (default `~/.grok/bin/agent-host`), records
//!   `<cursor config dir>/grok-switch.json` (`state: "pending" | "active"`),
//!   and leaves the versioned Cursor install untouched. The Grok host's
//!   `--version` prints `grok <x.y.z> (…)`, not a Cursor date-hash version.
//! - `cursor/list_available_models` (ACP extension, no session needed) lists
//!   the base models (`value`, e.g. `grok-4.7`; `default` = Auto). A session's
//!   `model` option values are variants of those bases
//!   (`grok-4.7[context=256k,…]`), applied with `session/set_config_option`.
//!   Applying a model also saves it as the user's Cursor CLI default.
//! - `cursor-agent models` signed in exits 0 and prints `id - Name` rows
//!   after an `Available models` header, then a `Tip:` footer. Signed out it
//!   exits 1 with `Error: Authentication required. …`.

use std::path::{Path, PathBuf};

use intent_core::path_utils::is_executable_file;

/// The Cursor CLI command. Never the bare `agent` alias.
pub const CURSOR_COMMAND: &str = "cursor-agent";

/// Install root of the versioned Cursor CLI, relative to `$HOME`.
const VERSIONS_DIR: &[&str] = &[".local", "share", "cursor-agent", "versions"];

/// Where the Grok agent host channel keeps its state file and binaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorHostDirs {
    /// `$HOME`, for the versioned install tier.
    pub home: Option<PathBuf>,
    /// Cursor's config dir (`$CURSOR_CONFIG_DIR`, else `~/.cursor`).
    pub cursor_config_dir: Option<PathBuf>,
    /// Grok's home (`$GROK_HOME`, else `~/.grok`).
    pub grok_home: Option<PathBuf>,
}

impl CursorHostDirs {
    /// Resolve the dirs from `home` plus the process environment.
    #[must_use]
    pub fn from_env(home: Option<&Path>) -> Self {
        Self::with_env(home, &|name| std::env::var_os(name).map(PathBuf::from))
    }

    /// [`Self::from_env`] with an injected env lookup (test seam).
    #[must_use]
    pub fn with_env(home: Option<&Path>, env: &dyn Fn(&str) -> Option<PathBuf>) -> Self {
        let non_empty = |name: &str| env(name).filter(|p| !p.as_os_str().is_empty());
        Self {
            home: home.map(Path::to_path_buf),
            cursor_config_dir: non_empty("CURSOR_CONFIG_DIR")
                .or_else(|| home.map(|h| h.join(".cursor"))),
            grok_home: non_empty("GROK_HOME").or_else(|| home.map(|h| h.join(".grok"))),
        }
    }
}

/// Whether `path` is (or resolves to) the Grok agent host rather than a
/// Cursor CLI: its real path is named `agent-host*` or lies under
/// `grok_home`. Unresolvable links count as not-Grok (the executable check
/// rejects them separately).
#[must_use]
pub fn is_grok_agent_host_path(path: &Path, grok_home: Option<&Path>) -> bool {
    let real = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let named_agent_host = real
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with("agent-host"));
    let under_grok_home = grok_home.is_some_and(|g| {
        let g = std::fs::canonicalize(g).unwrap_or_else(|_| g.to_path_buf());
        real.starts_with(g)
    });
    named_agent_host || under_grok_home
}

/// Whether Cursor's `grok-switch.json` records an active switch to the Grok
/// agent host channel. A missing or unreadable file is "not active".
#[must_use]
pub fn grok_switch_active(cursor_config_dir: Option<&Path>) -> bool {
    let Some(dir) = cursor_config_dir else {
        return false;
    };
    let Ok(text) = std::fs::read_to_string(dir.join("grok-switch.json")) else {
        return false;
    };
    serde_json::from_str::<serde_json::Value>(&text)
        .ok()
        .and_then(|v| v.get("state").and_then(|s| s.as_str()).map(str::to_owned))
        .is_some_and(|state| state == "active")
}

/// Versioned installs under `~/.local/share/cursor-agent/versions/`, newest
/// first. Version dir names are `YYYY.MM.DD-<hash>`, so a reverse
/// lexicographic sort is newest-first by date.
#[must_use]
pub fn versioned_install_candidates(home: &Path) -> Vec<PathBuf> {
    let root = VERSIONS_DIR
        .iter()
        .fold(home.to_path_buf(), |p, c| p.join(c));
    let Ok(entries) = std::fs::read_dir(&root) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .filter_map(Result::ok)
        .filter_map(|e| e.file_name().into_string().ok())
        .filter(|n| !n.starts_with('.'))
        .collect();
    names.sort_unstable_by(|a, b| b.cmp(a));
    names
        .into_iter()
        .map(|n| root.join(n).join(CURSOR_COMMAND))
        .filter(|p| is_executable_file(p))
        .collect()
}

/// Resolve the Cursor CLI after the explicit `providers.paths.cursor` tier:
/// the enhanced-PATH `cursor-agent` (skipped when it resolves to the Grok
/// agent host, or entirely while a Grok switch is active, since the switch
/// can repoint it), then the newest versioned install.
#[must_use]
pub fn find_cursor_auto(
    dirs: &CursorHostDirs,
    path_hit: impl FnOnce() -> Option<PathBuf>,
) -> Option<PathBuf> {
    let grok_home = dirs.grok_home.as_deref();
    if !grok_switch_active(dirs.cursor_config_dir.as_deref()) {
        if let Some(hit) = path_hit().filter(|p| !is_grok_agent_host_path(p, grok_home)) {
            return Some(hit);
        }
    }
    dirs.home
        .as_deref()
        .map(versioned_install_candidates)
        .unwrap_or_default()
        .into_iter()
        .find(|p| !is_grok_agent_host_path(p, grok_home))
}

/// What `cursor-agent --version` says the binary is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorCliIdentity {
    /// A Cursor CLI date-hash version (`2026.11.05-86047df`).
    Cursor(String),
    /// The Grok agent host (`grok 0.3.1 (…) [unified]`).
    GrokAgentHost(String),
    /// Output in neither shape. Permissive: a future Cursor version format
    /// must not take the provider away.
    Unrecognized(String),
}

/// Classify the first line of `--version` output.
#[must_use]
pub fn classify_cursor_version(first_line: &str) -> CursorCliIdentity {
    let line = first_line.trim().to_owned();
    if line.starts_with("grok ") {
        return CursorCliIdentity::GrokAgentHost(line);
    }
    let is_date_hash = line.split_once('-').is_some_and(|(date, hash)| {
        let parts: Vec<&str> = date.split('.').collect();
        parts.len() == 3
            && parts
                .iter()
                .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
            && !hash.is_empty()
            && hash.bytes().all(|b| b.is_ascii_alphanumeric())
    });
    if is_date_hash {
        CursorCliIdentity::Cursor(line)
    } else {
        CursorCliIdentity::Unrecognized(line)
    }
}

/// The user-facing reason a resolved `cursor-agent` is refused.
#[must_use]
pub fn grok_agent_host_reason(version_line: &str) -> String {
    format!(
        "cursor-agent runs the Grok agent host ({version_line}), not the Cursor CLI. \
         Run \"cursor-agent update --channel cursor\" to switch back, or set \
         providers.paths.cursor to the Cursor install under \
         ~/.local/share/cursor-agent/versions/."
    )
}

/// The unavailable reason for a `providers.paths.cursor` override that
/// resolves to the Grok agent host, or `None` for any other override
/// (blank, invalid, or a real Cursor CLI). Such an override is refused on
/// every surface (discovery, auth, models, spawn) rather than silently
/// replaced by auto-discovery.
#[must_use]
pub fn grok_host_override_reason(override_path: &str, dirs: &CursorHostDirs) -> Option<String> {
    let trimmed = override_path.trim();
    if trimmed.is_empty() {
        return None;
    }
    let path = Path::new(trimmed);
    (path.is_absolute()
        && is_executable_file(path)
        && is_grok_agent_host_path(path, dirs.grok_home.as_deref()))
    .then(|| {
        format!(
            "providers.paths.cursor ({trimmed}) points at the Grok agent host, not the Cursor CLI. \
             Point it at the Cursor install under ~/.local/share/cursor-agent/versions/, or clear it."
        )
    })
}

/// One model row from `cursor-agent models`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorModel {
    /// The id passed to `--model` / the `model` config option.
    pub id: String,
    /// Display name with Cursor's `(current, default)` markers removed.
    pub name: String,
    /// Whether the row carried the `default` marker.
    pub is_default: bool,
}

/// Parsed `cursor-agent models` run.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CursorModelsOutput {
    /// `Some(false)` on the explicit auth-required error; `Some(true)` when
    /// the run succeeded and listed models; `None` otherwise.
    pub authenticated: Option<bool>,
    /// Model rows in CLI order.
    pub models: Vec<CursorModel>,
}

/// Whether CLI output carries Cursor's auth-required error.
#[must_use]
pub fn is_cursor_auth_required(text: &str) -> bool {
    text.to_ascii_lowercase()
        .contains("authentication required")
}

/// Parse one `cursor-agent models` run. The signed-out error may arrive on
/// stdout or stderr, so both are checked.
#[must_use]
pub fn parse_cursor_models_command_output(
    stdout: &str,
    stderr: &str,
    success: bool,
) -> CursorModelsOutput {
    if is_cursor_auth_required(stdout) || is_cursor_auth_required(stderr) {
        return CursorModelsOutput {
            authenticated: Some(false),
            models: Vec::new(),
        };
    }
    let models: Vec<CursorModel> = if success {
        stdout.lines().filter_map(parse_model_row).collect()
    } else {
        Vec::new()
    };
    CursorModelsOutput {
        authenticated: (!models.is_empty()).then_some(true),
        models,
    }
}

/// Drop zero-width and BOM characters and collapse whitespace runs (the CLI
/// pads some names with U+200B and double spaces).
#[must_use]
pub fn clean_display_text(text: &str) -> String {
    clean(text)
}

fn clean(text: &str) -> String {
    text.chars()
        .filter(|c| {
            !matches!(
                c,
                '\u{200B}' | '\u{200C}' | '\u{200D}' | '\u{2060}' | '\u{FEFF}'
            )
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Parse one `id - Name` row; headers, blank lines and the `Tip:` footer
/// have no ` - ` separator after a single-token id and are skipped.
fn parse_model_row(line: &str) -> Option<CursorModel> {
    let line = clean(line);
    let (id, name) = line.split_once(" - ")?;
    let id = id.trim();
    if id.is_empty()
        || !id.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b'[' | b']' | b'=' | b',')
        })
    {
        return None;
    }
    let mut name = name.trim().to_owned();
    let mut is_default = false;
    if let Some(open) = name.rfind('(') {
        let markers = name[open + 1..].trim_end_matches(')');
        let words: Vec<&str> = markers.split(',').map(str::trim).collect();
        if name.ends_with(')') && words.iter().all(|w| matches!(*w, "current" | "default")) {
            is_default = words.contains(&"default");
            name = name[..open].trim_end().to_owned();
        }
    }
    if name.is_empty() {
        id.clone_into(&mut name);
    }
    Some(CursorModel {
        id: id.to_owned(),
        name,
        is_default,
    })
}

/// The id Cursor's `model` option uses for its Auto router.
pub const CURSOR_AUTO_MODEL: &str = "default";

/// The session `model` option value for a catalog base id: the exact value,
/// else the first variant of that base (`<base>[…]`). `options` is the
/// session's `configOptions` array (from `session/new` / `session/load`).
#[must_use]
pub fn cursor_session_model_value(options: &serde_json::Value, base: &str) -> Option<String> {
    let values: Vec<&str> = options
        .as_array()?
        .iter()
        .find(|o| o.get("id").and_then(serde_json::Value::as_str) == Some("model"))?
        .get("options")?
        .as_array()?
        .iter()
        .filter_map(|o| o.get("value").and_then(serde_json::Value::as_str))
        .collect();
    let prefix = format!("{base}[");
    values
        .iter()
        .find(|v| **v == base)
        .or_else(|| values.iter().find(|v| v.starts_with(&prefix)))
        .map(|v| (*v).to_string())
}

#[cfg(test)]
mod session_model_tests {
    use super::*;
    use serde_json::json;

    /// Option values from a captured cursor-agent 2026.11.05-86047df
    /// `session/new` (stored login).
    fn options() -> serde_json::Value {
        json!([
            {"id":"mode","type":"select","currentValue":"agent","options":[{"value":"agent","name":"Agent"}]},
            {"id":"model","type":"select","currentValue":"default[]","options":[
                {"value":"default[]","name":"Auto"},
                {"value":"grok-4.7[context=256k,reasoning_effort=high,fast=true]","name":"grok-4.7"},
                {"value":"grok-4.6[effort=high,fast=true]","name":"grok-4.6"},
                {"value":"claude-sonnet-4-5[thinking=true,context=200k]","name":"claude-sonnet-4-5"},
                {"value":"claude-sonnet-4[thinking=false,context=200k]","name":"claude-sonnet-4"},
                {"value":"gpt-5.2-codex[reasoning=high,fast=false]","name":"gpt-5.2-codex"},
                {"value":"gpt-5.2[reasoning=medium,fast=false]","name":"gpt-5.2"}
            ]}
        ])
    }

    #[test]
    fn base_ids_resolve_to_the_session_variant() {
        let o = options();
        assert_eq!(
            cursor_session_model_value(&o, CURSOR_AUTO_MODEL).as_deref(),
            Some("default[]")
        );
        assert_eq!(
            cursor_session_model_value(&o, "grok-4.7").as_deref(),
            Some("grok-4.7[context=256k,reasoning_effort=high,fast=true]")
        );
        // `claude-sonnet-4` must not pick `claude-sonnet-4-5[…]`.
        assert_eq!(
            cursor_session_model_value(&o, "claude-sonnet-4").as_deref(),
            Some("claude-sonnet-4[thinking=false,context=200k]")
        );
        // An exact variant id passes through.
        assert_eq!(
            cursor_session_model_value(&o, "grok-4.6[effort=high,fast=true]").as_deref(),
            Some("grok-4.6[effort=high,fast=true]")
        );
        // Near-collisions: a longer id listed first never wins the prefix match.
        assert_eq!(
            cursor_session_model_value(&o, "gpt-5.2").as_deref(),
            Some("gpt-5.2[reasoning=medium,fast=false]")
        );
        assert_eq!(
            cursor_session_model_value(&o, "gpt-5.2-codex").as_deref(),
            Some("gpt-5.2-codex[reasoning=high,fast=false]")
        );
        assert_eq!(cursor_session_model_value(&o, "gpt-5"), None);
        assert_eq!(cursor_session_model_value(&o, "gpt-9"), None);
        assert_eq!(cursor_session_model_value(&json!([]), "grok-4.7"), None);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};

    fn executable(path: &Path, body: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn dirs(home: &Path) -> CursorHostDirs {
        CursorHostDirs::with_env(Some(home), &|_| None)
    }

    fn versioned(home: &Path, ver: &str) -> PathBuf {
        let p = home
            .join(".local/share/cursor-agent/versions")
            .join(ver)
            .join(CURSOR_COMMAND);
        executable(&p, "#!/bin/sh\n");
        p
    }

    #[test]
    fn host_dirs_honor_env_overrides_and_ignore_blank_values() {
        let home = Path::new("/home/u");
        let d = CursorHostDirs::with_env(Some(home), &|n| match n {
            "CURSOR_CONFIG_DIR" => Some(PathBuf::from("/cfg")),
            "GROK_HOME" => Some(PathBuf::new()),
            _ => None,
        });
        assert_eq!(d.cursor_config_dir.as_deref(), Some(Path::new("/cfg")));
        assert_eq!(d.grok_home.as_deref(), Some(Path::new("/home/u/.grok")));
    }

    #[test]
    fn path_hit_wins_when_it_is_the_cursor_cli() {
        let home = tempfile::tempdir().unwrap();
        let real = versioned(home.path(), "2026.11.05-86047df");
        let link = home.path().join(".local/bin").join(CURSOR_COMMAND);
        std::fs::create_dir_all(link.parent().unwrap()).unwrap();
        symlink(&real, &link).unwrap();
        let found = find_cursor_auto(&dirs(home.path()), || Some(link.clone()));
        assert_eq!(found, Some(link));
    }

    #[test]
    fn falls_back_to_newest_versioned_install() {
        let home = tempfile::tempdir().unwrap();
        versioned(home.path(), "2026.10.01-aaaaaaa");
        let newest = versioned(home.path(), "2026.11.05-86047df");
        assert_eq!(find_cursor_auto(&dirs(home.path()), || None), Some(newest));
    }

    #[test]
    fn grok_host_symlink_on_path_is_skipped_for_the_versioned_install() {
        let home = tempfile::tempdir().unwrap();
        let cursor = versioned(home.path(), "2026.11.05-86047df");
        let host = home.path().join(".grok/downloads/agent-host-0.3.1-linux");
        executable(&host, "#!/bin/sh\necho grok 0.3.1\n");
        let host_link = home.path().join(".grok/bin/agent-host");
        std::fs::create_dir_all(host_link.parent().unwrap()).unwrap();
        symlink(&host, &host_link).unwrap();
        let link = home.path().join(".local/bin").join(CURSOR_COMMAND);
        std::fs::create_dir_all(link.parent().unwrap()).unwrap();
        symlink(&host_link, &link).unwrap();
        assert!(is_grok_agent_host_path(
            &link,
            dirs(home.path()).grok_home.as_deref()
        ));
        assert_eq!(
            find_cursor_auto(&dirs(home.path()), || Some(link)),
            Some(cursor)
        );
    }

    #[test]
    fn agent_host_named_binary_outside_grok_home_is_still_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let host = dir.path().join("agent-host-0.3.1");
        executable(&host, "#!/bin/sh\n");
        assert!(is_grok_agent_host_path(&host, None));
        assert!(!is_grok_agent_host_path(
            &dir.path().join("cursor-agent"),
            None
        ));
    }

    #[test]
    fn active_grok_switch_skips_the_path_tier() {
        let home = tempfile::tempdir().unwrap();
        let cursor = versioned(home.path(), "2026.11.05-86047df");
        let other = home.path().join("bin").join(CURSOR_COMMAND);
        executable(&other, "#!/bin/sh\n");
        std::fs::create_dir_all(home.path().join(".cursor")).unwrap();
        let switch = home.path().join(".cursor/grok-switch.json");
        std::fs::write(
            &switch,
            r#"{"schema":1,"provider":"cursor-cli","state":"pending"}"#,
        )
        .unwrap();
        assert!(!grok_switch_active(Some(&home.path().join(".cursor"))));
        assert_eq!(
            find_cursor_auto(&dirs(home.path()), || Some(other.clone())),
            Some(other.clone())
        );
        std::fs::write(
            &switch,
            r#"{"schema":1,"provider":"cursor-cli","state":"active"}"#,
        )
        .unwrap();
        assert_eq!(
            find_cursor_auto(&dirs(home.path()), || Some(other)),
            Some(cursor)
        );
    }

    #[test]
    fn nothing_found_without_path_hit_or_install() {
        let home = tempfile::tempdir().unwrap();
        assert_eq!(find_cursor_auto(&dirs(home.path()), || None), None);
    }

    #[test]
    fn version_output_identifies_cursor_grok_and_unknown() {
        assert_eq!(
            classify_cursor_version("2026.11.05-86047df\n"),
            CursorCliIdentity::Cursor("2026.11.05-86047df".into())
        );
        assert_eq!(
            classify_cursor_version("grok 0.3.1 (abc123) [unified]"),
            CursorCliIdentity::GrokAgentHost("grok 0.3.1 (abc123) [unified]".into())
        );
        assert!(matches!(
            classify_cursor_version("cursor-agent v3"),
            CursorCliIdentity::Unrecognized(_)
        ));
        let reason = grok_agent_host_reason("grok 0.3.1");
        assert!(reason.contains("update --channel cursor"), "{reason}");
        assert!(reason.contains("providers.paths.cursor"), "{reason}");
    }

    /// Verbatim excerpt of signed-in `cursor-agent models` (2026.11.05-86047df),
    /// including the U+200B padding and double spaces the CLI emits.
    const SIGNED_IN: &str = include_str!("../tests/fixtures/cursor-models-signed-in.txt");

    #[test]
    fn signed_in_models_parse_ids_names_and_default() {
        let parsed = parse_cursor_models_command_output(SIGNED_IN, "", true);
        assert_eq!(parsed.authenticated, Some(true));
        let first = &parsed.models[0];
        assert_eq!(first.id, "auto");
        assert_eq!(first.name, "Auto");
        assert!(first.is_default);
        assert_eq!(parsed.models.iter().filter(|m| m.is_default).count(), 1);
        let grok = parsed
            .models
            .iter()
            .find(|m| m.id == "grok-4.7-low-fast")
            .unwrap();
        assert_eq!(grok.name, "Grok 4.7 Low Fast");
        let fable = parsed
            .models
            .iter()
            .find(|m| m.id == "claude-fable-5-1-high")
            .unwrap();
        assert_eq!(fable.name, "Claude Fable 5.1 1M (NO ZDR)");
        assert!(parsed.models.iter().all(|m| !m.id.contains("Tip")));
        assert!(!parsed.models.iter().any(|m| m.id == "Available"));
    }

    #[test]
    fn signed_out_models_is_an_explicit_logout_on_either_stream() {
        let msg = "Error: Authentication required. Run 'agent login', pass --api-key/--auth-token, or set CURSOR_API_KEY/CURSOR_AUTH_TOKEN.\n";
        for (out, err) in [(msg, ""), ("", msg)] {
            let parsed = parse_cursor_models_command_output(out, err, false);
            assert_eq!(parsed.authenticated, Some(false));
            assert!(parsed.models.is_empty());
        }
    }

    #[test]
    fn failed_or_empty_runs_are_unknown() {
        assert_eq!(
            parse_cursor_models_command_output("auto - Auto\n", "boom", false),
            CursorModelsOutput::default()
        );
        assert_eq!(
            parse_cursor_models_command_output("Available models\n\n", "", true),
            CursorModelsOutput::default()
        );
    }
}
