//! Platform-parametrized runner selection for the workspace setup-script
//! terminal (`workspace.create`). POSIX always spawns the historic
//! `/bin/sh -c SETUP_SCRIPT_WRAPPER` invocation byte-for-byte. Windows
//! prefers a discovered Git-for-Windows `sh.exe` running the same POSIX
//! wrapper (so bash setup scripts keep working) and falls back to a
//! cmd.exe-native `.cmd` wrapper preserving the same completion-summary line
//! format. The helpers take an explicit `is_windows` flag so the logic is
//! unit-testable on any host OS; the call site wires `cfg!(windows)`.

use std::path::{Path, PathBuf};

/// How the setup-script terminal runs the script file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SetupScriptRunner {
    /// A POSIX `sh` runs the inline timing wrapper
    /// (`<sh> -c <wrapper> sh <script>`). On POSIX `sh` is always
    /// `/bin/sh`; on Windows it is a discovered Git-for-Windows `sh.exe`.
    PosixSh { sh: PathBuf, is_windows: bool },
    /// Windows with no POSIX sh available: `cmd.exe` runs a generated
    /// `.cmd` timing wrapper file with the script path as `%1`.
    CmdWrapper,
}

/// Select the runner for the platform. POSIX ignores `windows_sh` and
/// always picks `/bin/sh`; Windows uses the discovered sh when present,
/// else the cmd.exe wrapper fallback.
pub(crate) fn select(is_windows: bool, windows_sh: Option<PathBuf>) -> SetupScriptRunner {
    if !is_windows {
        return SetupScriptRunner::PosixSh {
            sh: PathBuf::from("/bin/sh"),
            is_windows: false,
        };
    }
    match windows_sh {
        Some(sh) => SetupScriptRunner::PosixSh {
            sh,
            is_windows: true,
        },
        None => SetupScriptRunner::CmdWrapper,
    }
}

impl SetupScriptRunner {
    /// Extension for the on-disk script file: `sh` for POSIX-sh runs
    /// (unchanged unix behavior), `cmd` for the cmd.exe fallback.
    pub(crate) fn script_extension(&self) -> &'static str {
        match self {
            Self::PosixSh { .. } => "sh",
            Self::CmdWrapper => "cmd",
        }
    }

    /// True when a `.cmd` wrapper file must be written next to the script
    /// before spawning.
    pub(crate) fn needs_cmd_wrapper_file(&self) -> bool {
        matches!(self, Self::CmdWrapper)
    }

    /// Build the `(program, args, extra_env)` triple for the PTY spawn.
    /// `wrapper_path` is the on-disk `.cmd` wrapper (required for
    /// `CmdWrapper`, ignored otherwise). The POSIX shape is byte-identical
    /// to the historic `/bin/sh -c SETUP_SCRIPT_WRAPPER sh <script>`
    /// spawn; the Windows sh path forward-slashes the script argument and
    /// templates the discovered sh into the wrapper (MSYS sh has no
    /// `/bin/sh` guarantee from a native-process spawn).
    ///
    /// `CmdWrapper` passes the script path via `extra_env`
    /// (`INTENT_SETUP_SCRIPT`) rather than as a second argv token: with
    /// both the wrapper and script under the same (spaces-prone)
    /// `.intent` dir, `portable-pty` double-quotes each spaced argument,
    /// and cmd.exe's `/C` quote handling only preserves quoting when
    /// *exactly two* quote characters appear on the whole command line.
    /// Two quoted args produce four quotes and get mis-parsed; one
    /// quoted arg (the wrapper) keeps the count at two.
    pub(crate) fn command(
        &self,
        script_path: &Path,
        wrapper_path: Option<&Path>,
    ) -> (String, Vec<String>, Vec<(String, String)>) {
        match self {
            Self::PosixSh { sh, is_windows } => {
                let (wrapper, script_arg) = if *is_windows {
                    (
                        wrapper_for_sh(&forward_slashes(sh)),
                        forward_slashes(script_path),
                    )
                } else {
                    (
                        crate::SETUP_SCRIPT_WRAPPER.to_string(),
                        script_path.to_string_lossy().to_string(),
                    )
                };
                (
                    sh.to_string_lossy().to_string(),
                    vec!["-c".to_string(), wrapper, "sh".to_string(), script_arg],
                    Vec::new(),
                )
            }
            Self::CmdWrapper => {
                let wrapper = wrapper_path.expect("CmdWrapper requires a wrapper file path");
                (
                    "cmd.exe".to_string(),
                    vec![
                        "/d".to_string(),
                        "/c".to_string(),
                        wrapper.to_string_lossy().to_string(),
                    ],
                    vec![(
                        "INTENT_SETUP_SCRIPT".to_string(),
                        script_path.to_string_lossy().to_string(),
                    )],
                )
            }
        }
    }
}

/// Forward-slash a path for MSYS `sh` consumption (`C:\x` → `C:/x`);
/// a no-op on paths without backslashes.
fn forward_slashes(p: &Path) -> String {
    p.to_string_lossy().replace('\\', "/")
}

/// [`crate::SETUP_SCRIPT_WRAPPER`] with the inner interpreter templated:
/// identical text except the script runs through the double-quoted `sh`
/// instead of bare `/bin/sh` (drift-guarded against the const in tests).
pub(crate) fn wrapper_for_sh(sh: &str) -> String {
    format!(
        r#"start=$(date +%s); "{sh}" "$1"; code=$?; elapsed=$(( $(date +%s) - start )); if [ "$code" -eq 0 ]; then printf '\nSetup script completed in %ss (exit code %s)\n' "$elapsed" "$code"; else printf '\nSetup script failed in %ss (exit code %s)\n' "$elapsed" "$code"; fi; exit "$code""#
    )
}

/// cmd.exe-native timing wrapper (Windows fallback when no POSIX sh is
/// found): the script path is read from the `INTENT_SETUP_SCRIPT`
/// environment variable rather than passed as `%1`, so the spawn only
/// has one quoted argv token (the wrapper itself) — see the doc comment
/// on [`SetupScriptRunner::command`] for why a second quoted path
/// argument breaks under cmd.exe's `/C` quote-stripping when the
/// workspace path contains spaces. Mirrors the POSIX wrapper's output
/// contract — blank separator line, then `Setup script completed in <N>s
/// (exit code <C>)` (or the `failed` variant) — and exits with the
/// script's own code. Elapsed seconds derive from `%TIME%` (space padding
/// normalized to `0`, `1%x%-100` avoids octal parsing of zero-padded
/// fields, day rollover corrected). CRLF line endings for cmd.exe.
pub(crate) const SETUP_SCRIPT_WRAPPER_CMD: &str = concat!(
    "@echo off\r\n",
    "setlocal\r\n",
    "set \"_st=%TIME: =0%\"\r\n",
    "call \"%INTENT_SETUP_SCRIPT%\"\r\n",
    "set \"code=%ERRORLEVEL%\"\r\n",
    "set \"_en=%TIME: =0%\"\r\n",
    "set /a \"_s=(1%_st:~0,2%-100)*3600+(1%_st:~3,2%-100)*60+(1%_st:~6,2%-100)\"\r\n",
    "set /a \"_e=(1%_en:~0,2%-100)*3600+(1%_en:~3,2%-100)*60+(1%_en:~6,2%-100)\"\r\n",
    "if %_e% lss %_s% set /a \"_e+=86400\"\r\n",
    "set /a \"elapsed=_e-_s\"\r\n",
    "if %code%==0 (set \"verb=completed\") else (set \"verb=failed\")\r\n",
    "echo.\r\n",
    "echo Setup script %verb% in %elapsed%s (exit code %code%)\r\n",
    "exit /b %code%\r\n",
);

/// Standard Git-for-Windows `sh.exe` install locations under the given
/// environment roots (`%ProgramFiles%`, `%ProgramFiles(x86)%`,
/// `%LocalAppData%`), in probe order. Pure so tests can drive it on any
/// host.
pub(crate) fn git_sh_candidates(
    program_files: Option<&str>,
    program_files_x86: Option<&str>,
    local_app_data: Option<&str>,
) -> Vec<PathBuf> {
    let mut roots: Vec<PathBuf> = Vec::new();
    for pf in [program_files, program_files_x86].into_iter().flatten() {
        roots.push(Path::new(pf).join("Git"));
    }
    if let Some(lad) = local_app_data {
        roots.push(Path::new(lad).join("Programs").join("Git"));
    }
    let mut out = Vec::new();
    for git in roots {
        out.push(git.join("bin").join("sh.exe"));
        out.push(git.join("usr").join("bin").join("sh.exe"));
    }
    out
}

/// Scan a PATH-style string for a directory containing `sh.exe`. The
/// separator is parametrized (`;` on Windows) so the scan is testable on
/// POSIX hosts; empty entries are skipped.
pub(crate) fn find_sh_in_path(path_var: &str, separator: char) -> Option<PathBuf> {
    path_var
        .split(separator)
        .filter(|d| !d.is_empty())
        .map(|d| Path::new(d).join("sh.exe"))
        .find(|p| p.is_file())
}

/// Discover a POSIX `sh.exe` on Windows: Git-for-Windows standard install
/// locations first, then a `%PATH%` scan. `None` when no sh is found (the
/// caller falls back to the cmd.exe wrapper).
pub(crate) fn find_windows_sh() -> Option<PathBuf> {
    let pf = std::env::var("ProgramFiles").ok();
    let pf86 = std::env::var("ProgramFiles(x86)").ok();
    let lad = std::env::var("LOCALAPPDATA").ok();
    git_sh_candidates(pf.as_deref(), pf86.as_deref(), lad.as_deref())
        .into_iter()
        .find(|p| p.is_file())
        .or_else(|| {
            std::env::var("PATH")
                .ok()
                .and_then(|p| find_sh_in_path(&p, ';'))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posix_select_ignores_windows_sh_and_is_bin_sh() {
        let runner = select(false, Some(PathBuf::from("C:/Git/bin/sh.exe")));
        assert_eq!(
            runner,
            SetupScriptRunner::PosixSh {
                sh: PathBuf::from("/bin/sh"),
                is_windows: false
            }
        );
        assert_eq!(runner.script_extension(), "sh");
        assert!(!runner.needs_cmd_wrapper_file());
    }

    /// The POSIX spawn shape is byte-identical to the historic
    /// `/bin/sh -c SETUP_SCRIPT_WRAPPER sh <script>` invocation.
    #[test]
    fn posix_command_is_byte_identical_to_historic_spawn() {
        let runner = select(false, None);
        let (program, args, extra_env) =
            runner.command(Path::new("/w/.intent/setup-a.sh"), None);
        assert_eq!(program, "/bin/sh");
        assert_eq!(
            args,
            vec![
                "-c".to_string(),
                crate::SETUP_SCRIPT_WRAPPER.to_string(),
                "sh".to_string(),
                "/w/.intent/setup-a.sh".to_string(),
            ]
        );
        assert!(extra_env.is_empty());
    }

    #[test]
    fn windows_with_sh_runs_posix_wrapper_through_it() {
        let sh = PathBuf::from(r"C:\Program Files\Git\bin\sh.exe");
        let runner = select(true, Some(sh.clone()));
        assert_eq!(runner.script_extension(), "sh");
        assert!(!runner.needs_cmd_wrapper_file());
        let (program, args, extra_env) =
            runner.command(Path::new(r"C:\w\.intent\setup-a.sh"), None);
        assert_eq!(program, sh.to_string_lossy());
        assert_eq!(args[0], "-c");
        assert!(
            args[1].contains(r#""C:/Program Files/Git/bin/sh.exe" "$1""#),
            "wrapper must template the forward-slashed sh: {}",
            args[1]
        );
        assert!(args[1].contains(r"printf '\nSetup script completed in %ss (exit code %s)\n'"));
        assert!(args[1].contains(r"printf '\nSetup script failed in %ss (exit code %s)\n'"));
        assert_eq!(args[2], "sh");
        assert_eq!(args[3], "C:/w/.intent/setup-a.sh");
        assert!(extra_env.is_empty());
    }

    /// The script path travels via env (`INTENT_SETUP_SCRIPT`), not as a
    /// second quoted argv token, so a spaced workspace path (common on
    /// Windows, e.g. under `C:\Users\John Smith\...`) doesn't blow past
    /// cmd.exe's `/C` exactly-two-quotes rule (only the wrapper path is
    /// quoted on the command line).
    #[test]
    fn windows_without_sh_falls_back_to_cmd_wrapper() {
        let runner = select(true, None);
        assert_eq!(runner, SetupScriptRunner::CmdWrapper);
        assert_eq!(runner.script_extension(), "cmd");
        assert!(runner.needs_cmd_wrapper_file());
        let (program, args, extra_env) = runner.command(
            Path::new(r"C:\w\.intent\setup-a.cmd"),
            Some(Path::new(r"C:\w\.intent\setup-wrapper-a.cmd")),
        );
        assert_eq!(program, "cmd.exe");
        assert_eq!(
            args,
            vec![
                "/d".to_string(),
                "/c".to_string(),
                r"C:\w\.intent\setup-wrapper-a.cmd".to_string(),
            ]
        );
        assert_eq!(
            extra_env,
            vec![(
                "INTENT_SETUP_SCRIPT".to_string(),
                r"C:\w\.intent\setup-a.cmd".to_string(),
            )]
        );
    }

    /// Regression for the spaced-workspace-path bug: only the wrapper
    /// path is a quoted argv token even when both the wrapper and script
    /// live under a directory containing spaces, keeping the command
    /// line at exactly two quote characters.
    #[test]
    fn windows_cmd_wrapper_command_line_has_one_quoted_token_with_spaced_paths() {
        let runner = select(true, None);
        let (_, args, extra_env) = runner.command(
            Path::new(r"C:\a b\.intent\setup-x.cmd"),
            Some(Path::new(r"C:\a b\.intent\setup-wrapper-x.cmd")),
        );
        assert_eq!(args.len(), 3);
        assert_eq!(args[2], r"C:\a b\.intent\setup-wrapper-x.cmd");
        assert_eq!(
            extra_env,
            vec![(
                "INTENT_SETUP_SCRIPT".to_string(),
                r"C:\a b\.intent\setup-x.cmd".to_string(),
            )]
        );
    }

    /// Drift guard: the templated wrapper for `/bin/sh` matches the POSIX
    /// const except for the double quotes around the interpreter.
    #[test]
    fn wrapper_for_sh_matches_posix_const() {
        assert_eq!(
            wrapper_for_sh("/bin/sh").replacen("\"/bin/sh\"", "/bin/sh", 1),
            crate::SETUP_SCRIPT_WRAPPER
        );
    }

    #[test]
    fn cmd_wrapper_preserves_summary_format_and_exit_code_contract() {
        let w = SETUP_SCRIPT_WRAPPER_CMD;
        assert!(w.starts_with("@echo off\r\n"));
        assert!(w.contains("call \"%INTENT_SETUP_SCRIPT%\"\r\n"));
        assert!(
            w.contains("echo.\r\necho Setup script %verb% in %elapsed%s (exit code %code%)")
        );
        assert!(w.contains("(set \"verb=completed\")"));
        assert!(w.contains("(set \"verb=failed\")"));
        assert!(w.ends_with("exit /b %code%\r\n"));
    }

    #[test]
    fn git_sh_candidates_probe_order_and_roots() {
        let got = git_sh_candidates(
            Some(r"C:\Program Files"),
            Some(r"C:\Program Files (x86)"),
            Some(r"C:\Users\u\AppData\Local"),
        );
        let expected: Vec<PathBuf> = [
            Path::new(r"C:\Program Files").join("Git"),
            Path::new(r"C:\Program Files (x86)").join("Git"),
            Path::new(r"C:\Users\u\AppData\Local")
                .join("Programs")
                .join("Git"),
        ]
        .iter()
        .flat_map(|root| {
            [
                root.join("bin").join("sh.exe"),
                root.join("usr").join("bin").join("sh.exe"),
            ]
        })
        .collect();
        assert_eq!(got, expected);
        assert!(git_sh_candidates(None, None, None).is_empty());
    }

    #[test]
    fn find_sh_in_path_scans_entries_and_skips_empties() {
        let dir = std::env::temp_dir().join(format!(
            "setup-runner-path-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let sh = dir.join("sh.exe");
        std::fs::write(&sh, "").unwrap();
        let path_var = format!(";/nonexistent;{};", dir.to_string_lossy());
        assert_eq!(find_sh_in_path(&path_var, ';'), Some(sh));
        assert_eq!(find_sh_in_path("/nonexistent;/also-missing", ';'), None);
        std::fs::remove_dir_all(&dir).ok();
    }
}
