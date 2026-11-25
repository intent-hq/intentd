//! DeepSeek Harness (`dsh`) runs as an ACP server: `dsh --profile acp`.
//!
//! Every launch adds a daemon-owned `--patch` overlay ([`OVERLAY`]) and sets
//! [`TELEMETRY_DISABLED_ENV`], so Intent agents never upload their session
//! log or telemetry to DeepSeek and never use DSH's native subagent tools.
//! DSH applies `--patch` overlays after the user's own profile patches, and
//! refuses to start when an overlay file is missing, so a failed write fails
//! closed. A patch for a row id DSH no longer ships is ignored silently;
//! [`check_dump_config`] guards against that on the composed profile.

use std::path::{Path, PathBuf};

pub const PROVIDER_ID: &str = "dsh";
pub const COMMAND: &str = "dsh";

/// Launch-time opt-out read by DSH's profile launcher: any non-empty value
/// disables the `session-telemetry-otel` row.
pub const TELEMETRY_DISABLED_ENV: &str = "DSH_TELEMETRY_DISABLED";

/// The key DSH's `deepseek-official` route reads, from the launch
/// environment or from `$DSH_HOME/.credentials.yaml`.
pub const API_KEY_ENV: &str = "DEEPSEEK_API_KEY";

/// Overrides the DSH home (`~/.dsh` by default).
pub const HOME_ENV: &str = "DSH_HOME";

/// DSH's default home directory name under the user's home.
pub const DEFAULT_HOME_DIR: &str = ".dsh";

/// Launcher flag for an extra patch-list overlay (repeatable).
pub const PATCH_FLAG: &str = "--patch";

/// Launcher flag that prints the composed profile and exits.
pub const DUMP_CONFIG_FLAG: &str = "--dump-config";

/// Profile args that select DSH's ACP stdio server.
pub const ACP_ARGS: &[&str] = &["--profile", "acp"];

/// Profile rows the overlay disables. The first two upload data to DeepSeek
/// (`dsh_session_log` on official API requests; OTel session records); the
/// rest are DSH's native delegation tools, which Intent agents do not use.
pub const DISABLED_ROWS: &[&str] = &[
    "session-log-deepseek",
    "session-telemetry-otel",
    "tool-subagent",
    "tool-subagent-fork",
    "tool-agent-team",
];

/// The overlay written to [`overlay_path_in`]; one `disabled: true` entry per
/// [`DISABLED_ROWS`] id.
pub const OVERLAY: &str = "\
# Written by Intent for the DeepSeek Harness agents it starts. Intent rewrites
# this file before each launch; edits are not kept.
- id: session-log-deepseek
  disabled: true
- id: session-telemetry-otel
  disabled: true
- id: tool-subagent
  disabled: true
- id: tool-subagent-fork
  disabled: true
- id: tool-agent-team
  disabled: true
";

/// Package-name prefixes of DSH plugins that send session data off the host.
/// Every composed row from these packages must be disabled before a launch.
pub const UPLOAD_PACKAGE_PREFIXES: &[&str] = &[
    "@deepseek-ai/dsh-session-log-",
    "@deepseek-ai/dsh-session-telemetry-",
];

/// Argument used in place of the overlay path when no home directory is
/// known. DSH cannot read it and refuses to start.
const UNAVAILABLE_OVERLAY_ARG: &str = "intent-dsh-overlay-unavailable.yml";

/// Unique per write, so concurrent first writes never share a temp file.
static NEXT_TMP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// `<home>/.local/share/intent/providers/dsh/intent-overlay.yml`.
#[must_use]
pub fn overlay_path_in(home: &Path) -> PathBuf {
    home.join(".local/share/intent/providers/dsh/intent-overlay.yml")
}

/// Write [`OVERLAY`] to [`overlay_path_in`] unless it already holds exactly
/// that content. Private permissions on unix; atomic replace.
///
/// # Errors
/// Returns the I/O error when the folder or file cannot be written.
pub fn ensure_overlay_in(home: &Path) -> std::io::Result<PathBuf> {
    let path = overlay_path_in(home);
    if std::fs::read_to_string(&path).is_ok_and(|current| current == OVERLAY) {
        return Ok(path);
    }
    let dir = path
        .parent()
        .ok_or_else(|| std::io::Error::other("overlay path has no parent"))?;
    create_private_dir(dir)?;
    let tmp = dir.join(format!(
        ".intent-overlay-{}-{}.tmp",
        std::process::id(),
        NEXT_TMP.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    write_private_file(&tmp, OVERLAY)?;
    std::fs::rename(&tmp, &path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })?;
    Ok(path)
}

#[cfg(unix)]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

#[cfg(not(unix))]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)
}

#[cfg(unix)]
fn write_private_file(path: &Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()
}

#[cfg(not(unix))]
fn write_private_file(path: &Path, contents: &str) -> std::io::Result<()> {
    std::fs::write(path, contents)
}

/// The `--patch` value for a launch: the overlay under the launch host's
/// home, written first. A failed write still returns the path (or a
/// placeholder without a home), so DSH refuses to start rather than run
/// without the overlay.
#[must_use]
pub fn launch_overlay_arg() -> String {
    let Some(home) = overlay_home() else {
        tracing::warn!("no home directory for the DSH overlay; DSH will refuse to start");
        return UNAVAILABLE_OVERLAY_ARG.to_string();
    };
    match ensure_overlay_in(&home) {
        Ok(path) => path.display().to_string(),
        Err(e) => {
            let path = overlay_path_in(&home);
            tracing::warn!(
                path = %path.display(),
                error = %e,
                "cannot write the DSH overlay; DSH will refuse to start"
            );
            path.display().to_string()
        }
    }
}

#[cfg(not(test))]
fn overlay_home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|home| home.is_absolute())
}

/// Unit tests never write into the real home.
#[cfg(test)]
#[expect(clippy::unnecessary_wraps, reason = "matches the non-test signature")]
fn overlay_home() -> Option<PathBuf> {
    static HOME: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    Some(
        HOME.get_or_init(|| tempfile::tempdir().expect("temp home"))
            .path()
            .to_path_buf(),
    )
}

/// The DSH home a launch uses: `DSH_HOME` when set (and absolute), else
/// `<home>/.dsh`.
#[must_use]
pub fn dsh_home_in(home: &Path, dsh_home_env: Option<&str>) -> PathBuf {
    dsh_home_env
        .map(str::trim)
        .filter(|v| !v.is_empty() && Path::new(v).is_absolute())
        .map_or_else(|| home.join(DEFAULT_HOME_DIR), PathBuf::from)
}

/// Whether a DSH home stores a non-empty DeepSeek API key: in
/// `.credentials.yaml` (written by the `dsh web` Models page) or in the
/// `.env` fallback layer. Presence only; the value is never returned.
#[must_use]
pub fn home_has_api_key(dsh_home: &Path) -> bool {
    let read = |name: &str| std::fs::read_to_string(dsh_home.join(name)).ok();
    read(".credentials.yaml").is_some_and(|doc| credentials_yaml_has_key(&doc, API_KEY_ENV))
        || read(".env").is_some_and(|doc| dotenv_has_key(&doc, API_KEY_ENV))
}

/// `.credentials.yaml` is a flat mapping of credential reference to string.
fn credentials_yaml_has_key(doc: &str, key: &str) -> bool {
    doc.lines()
        .filter(|line| !line.starts_with([' ', '\t', '#']))
        .filter_map(|line| line.split_once(':'))
        .any(|(k, v)| unquote(k.trim()) == key && !unquote(v.trim()).is_empty())
}

fn dotenv_has_key(doc: &str, key: &str) -> bool {
    doc.lines()
        .map(str::trim)
        .map(|line| line.strip_prefix("export ").unwrap_or(line))
        .filter_map(|line| line.split_once('='))
        .any(|(k, v)| k.trim() == key && !unquote(v.trim()).is_empty())
}

fn unquote(value: &str) -> &str {
    let value = value.split(" #").next().unwrap_or(value).trim();
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
        .unwrap_or(value)
        .trim()
}

/// The version from `dsh --version` (a bare semver such as `0.2.2-rc.5`).
/// `None` for anything else, such as the unrelated distributed shell `dsh`.
#[must_use]
pub fn parse_version_output(stdout: &str) -> Option<String> {
    let line = stdout.lines().map(str::trim).find(|l| !l.is_empty())?;
    node_semver::Version::parse(line)
        .is_ok()
        .then(|| line.to_string())
}

/// One composed profile row from `dsh --dump-config`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpRow {
    pub id: String,
    pub name: Option<String>,
    pub disabled: bool,
}

/// Parse the composed profile printed by `dsh --dump-config`: a YAML list of
/// `- id:` rows with `name:` and `disabled:` fields. Line-based on purpose:
/// the dump carries `!!js` tags that a plain YAML reader rejects. Only a
/// literal `disabled: true` counts as disabled.
#[must_use]
pub fn parse_dump_rows(dump: &str) -> Vec<DumpRow> {
    let mut rows: Vec<DumpRow> = Vec::new();
    let mut open: Vec<(usize, usize)> = Vec::new();
    for line in dump.lines() {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent = line.len() - trimmed.len();
        while open
            .last()
            .is_some_and(|&(field_indent, _)| indent < field_indent)
        {
            open.pop();
        }
        if let Some(id) = trimmed.strip_prefix("- id:") {
            rows.push(DumpRow {
                id: unquote(id.trim()).to_string(),
                name: None,
                disabled: false,
            });
            open.push((indent + 2, rows.len() - 1));
            continue;
        }
        let Some(&(field_indent, row)) = open.last() else {
            continue;
        };
        if indent != field_indent {
            continue;
        }
        if let Some(name) = trimmed.strip_prefix("name:") {
            rows[row].name = Some(unquote(name.trim()).to_string());
        } else if let Some(value) = trimmed.strip_prefix("disabled:") {
            rows[row].disabled = value.trim() == "true";
        }
    }
    rows
}

/// Check that the composed profile turns off every upload row.
///
/// # Errors
/// Returns a user-facing reason when the dump has no rows or any row from an
/// [`UPLOAD_PACKAGE_PREFIXES`] package is still enabled.
pub fn check_dump_config(dump: &str) -> Result<(), String> {
    let rows = parse_dump_rows(dump);
    if rows.is_empty() {
        return Err(
            "Intent cannot read this DeepSeek Harness version's profile, so it cannot turn \
             off DSH's session-log upload. Update DSH or report this to Intent."
                .to_string(),
        );
    }
    let enabled: Vec<&str> = rows
        .iter()
        .filter(|row| !row.disabled)
        .filter(|row| {
            row.name.as_deref().is_some_and(|name| {
                UPLOAD_PACKAGE_PREFIXES
                    .iter()
                    .any(|prefix| name.starts_with(prefix))
            })
        })
        .map(|row| row.id.as_str())
        .collect();
    if enabled.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "Intent cannot turn off session-log upload in this DeepSeek Harness version \
             (still enabled: {}). Intent does not start DSH agents that would upload \
             their sessions.",
            enabled.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DUMP: &str = include_str!("../tests/fixtures/dsh-dump-config-0.2.2-rc.5.txt");

    #[test]
    fn overlay_disables_every_listed_row() {
        let rows = parse_dump_rows(OVERLAY);
        let ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, DISABLED_ROWS);
        assert!(rows.iter().all(|r| r.disabled));
    }

    #[test]
    fn ensure_overlay_writes_private_file_and_repairs_edits() {
        let home = tempfile::tempdir().unwrap();
        let path = ensure_overlay_in(home.path()).unwrap();
        assert_eq!(path, overlay_path_in(home.path()));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), OVERLAY);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
            let dir_mode = std::fs::metadata(path.parent().unwrap())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(dir_mode & 0o777, 0o700);
        }
        std::fs::write(&path, "[]\n").unwrap();
        ensure_overlay_in(home.path()).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), OVERLAY);
    }

    #[test]
    fn concurrent_first_writes_all_succeed_with_the_full_overlay() {
        for _ in 0..20 {
            let home = tempfile::tempdir().unwrap();
            let barrier = std::sync::Barrier::new(16);
            let results: Vec<std::io::Result<PathBuf>> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..16)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            ensure_overlay_in(home.path())
                        })
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            for result in results {
                let path = result.expect("every concurrent write succeeds");
                assert_eq!(std::fs::read_to_string(path).unwrap(), OVERLAY);
            }
            let leftovers = std::fs::read_dir(overlay_path_in(home.path()).parent().unwrap())
                .unwrap()
                .count();
            assert_eq!(leftovers, 1, "no temp files are left behind");
        }
    }

    #[cfg(unix)]
    #[test]
    fn ensure_overlay_fails_when_the_folder_cannot_be_created() {
        let home = tempfile::tempdir().unwrap();
        std::fs::write(home.path().join(".local"), "not a dir").unwrap();
        assert!(ensure_overlay_in(home.path()).is_err());
    }

    #[test]
    fn launch_overlay_arg_points_at_the_written_overlay() {
        let arg = launch_overlay_arg();
        assert_eq!(std::fs::read_to_string(&arg).unwrap(), OVERLAY);
    }

    #[test]
    fn recorded_dump_with_overlay_passes_the_guard() {
        assert_eq!(check_dump_config(DUMP), Ok(()));
        let rows = parse_dump_rows(DUMP);
        for id in DISABLED_ROWS {
            let row = rows.iter().find(|r| r.id == *id).expect(id);
            assert!(row.disabled, "{id} must be disabled");
        }
    }

    #[test]
    fn recorded_dump_without_overlay_fails_the_guard() {
        let without = DUMP.replace(
            "- id: session-log-deepseek\n  name: '@deepseek-ai/dsh-session-log-deepseek'\n  disabled: true",
            "- id: session-log-deepseek\n  name: '@deepseek-ai/dsh-session-log-deepseek'",
        );
        assert_ne!(without, DUMP, "fixture shape changed");
        let err = check_dump_config(&without).unwrap_err();
        assert!(err.contains("session-log-deepseek"), "{err}");
    }

    #[test]
    fn renamed_upload_row_fails_the_guard() {
        let dump = "\
- id: session-log-deepseek
  name: '@deepseek-ai/dsh-session-log-deepseek'
  disabled: true
- id: session-log-deepseek-v2
  name: '@deepseek-ai/dsh-session-log-deepseek'
  config:
    disabled: true
";
        let err = check_dump_config(dump).unwrap_err();
        assert!(err.contains("session-log-deepseek-v2"), "{err}");
    }

    #[test]
    fn conditional_disable_does_not_count() {
        let dump = "\
- id: session-telemetry-otel
  name: '@deepseek-ai/dsh-session-telemetry-otel'
  disabled: !!js process.env.X === '1'
";
        assert!(check_dump_config(dump).is_err());
    }

    #[test]
    fn unreadable_dump_fails_the_guard() {
        assert!(check_dump_config("").is_err());
        assert!(check_dump_config("error: unknown option '--dump-config'\n").is_err());
    }

    #[test]
    fn version_output_identifies_dsh() {
        assert_eq!(
            parse_version_output("0.2.2-rc.5\n").as_deref(),
            Some("0.2.2-rc.5")
        );
        assert_eq!(parse_version_output("1.0.0").as_deref(), Some("1.0.0"));
        assert_eq!(
            parse_version_output("Distributed Shell / Dancer's shell. version 0.25.10\n"),
            None
        );
        assert_eq!(parse_version_output(""), None);
    }

    #[test]
    fn api_key_presence_in_dsh_home() {
        let home = tempfile::tempdir().unwrap();
        assert!(!home_has_api_key(home.path()));
        std::fs::write(home.path().join(".credentials.yaml"), "OTHER_KEY: x\n").unwrap();
        assert!(!home_has_api_key(home.path()));
        std::fs::write(
            home.path().join(".credentials.yaml"),
            "DEEPSEEK_API_KEY: ''\n",
        )
        .unwrap();
        assert!(!home_has_api_key(home.path()));
        std::fs::write(
            home.path().join(".credentials.yaml"),
            "OTHER: a\nDEEPSEEK_API_KEY: 'sk-test'\n",
        )
        .unwrap();
        assert!(home_has_api_key(home.path()));
        std::fs::remove_file(home.path().join(".credentials.yaml")).unwrap();
        std::fs::write(home.path().join(".env"), "export DEEPSEEK_API_KEY=\"sk\"\n").unwrap();
        assert!(home_has_api_key(home.path()));
        std::fs::write(home.path().join(".env"), "DEEPSEEK_API_KEY=\n").unwrap();
        assert!(!home_has_api_key(home.path()));
    }

    #[test]
    fn dsh_home_prefers_absolute_env() {
        let home = Path::new("/home/u");
        assert_eq!(dsh_home_in(home, None), PathBuf::from("/home/u/.dsh"));
        assert_eq!(dsh_home_in(home, Some("")), PathBuf::from("/home/u/.dsh"));
        assert_eq!(
            dsh_home_in(home, Some("rel")),
            PathBuf::from("/home/u/.dsh")
        );
        assert_eq!(
            dsh_home_in(home, Some("/opt/dsh")),
            PathBuf::from("/opt/dsh")
        );
    }
}
