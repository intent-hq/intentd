//! Default-shell selection shared by `terminal.create` (command omitted) and
//! saved-script runs (`script_ops`).
//!
//! POSIX mirrors the ancestor's reliance on the user's login shell (`$SHELL`,
//! then `/bin/sh`). Windows never consults `$SHELL`: it resolves a native shell
//! instead — PowerShell first (`pwsh`, then `powershell`), then `%COMSPEC%`,
//! then `cmd.exe` — so daemon-spawned shells never point at `/bin/sh`.

/// Inherited launcher environment variables removed from daemon-spawned PTY children.
/// `npm_config_prefix` is set by the app launcher and makes nvm abort shell
/// initialization. Explicit environment overlay keys are preserved so user input wins.
pub(crate) const SCRUBBED_ENV_VARS: &[&str] = &["npm_config_prefix"];

/// Build an environment-removal list for [`intent_pty::SpawnSpec`], excluding
/// keys present in the explicit environment overlay.
pub(crate) fn scrubbed_env_vars_except(overlay: &[(String, String)]) -> Vec<String> {
    SCRUBBED_ENV_VARS
        .iter()
        .copied()
        .filter(|name| !overlay.iter().any(|(key, _)| key.as_str() == *name))
        .map(str::to_string)
        .collect()
}

/// The default shell for daemon-spawned terminals and scripts, resolved from
/// the host platform and environment.
pub(crate) fn default_shell() -> String {
    default_shell_for(
        cfg!(windows),
        std::env::var("SHELL").ok().as_deref(),
        std::env::var("COMSPEC").ok().as_deref(),
        |name| intent_providers::resolve_on_path(name).is_some(),
    )
}

/// Platform-parametrized selection (test seam: both arms unit-test on any
/// host). POSIX: `env_shell`, else `/bin/sh`. Windows: the first of `pwsh` →
/// `powershell` that `resolves`, else a non-empty `comspec`, else `cmd.exe`.
fn default_shell_for(
    is_windows: bool,
    env_shell: Option<&str>,
    comspec: Option<&str>,
    resolves: impl Fn(&str) -> bool,
) -> String {
    if !is_windows {
        return env_shell.map_or_else(|| "/bin/sh".to_string(), str::to_string);
    }
    for candidate in ["pwsh", "powershell"] {
        if resolves(candidate) {
            return candidate.to_string();
        }
    }
    match comspec {
        Some(c) if !c.is_empty() => c.to_string(),
        _ => "cmd.exe".to_string(),
    }
}


/// Build argv for running `command` under `shell` (platform-aware `-c` /
/// PowerShell `-Command` / `cmd /c`). Shared by saved-script runs and ACP
/// `terminal/create` when a provider sets `terminal_requires_shell`.
pub(crate) fn shell_args(shell: &str, command: &str) -> Vec<String> {
    shell_args_for(shell, command, cfg!(windows))
}

/// Platform-parametrized [`shell_args`] (test seam: the Windows arms are
/// unit-tested on any host).
pub(crate) fn shell_args_for(shell: &str, command: &str, is_windows: bool) -> Vec<String> {
    let file = std::path::Path::new(shell)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(shell)
        .to_lowercase();
    // On a real Windows host `Path::file_name` already splits on `\`; this
    // extra split only matters when the Windows arm runs under a POSIX test.
    let file = if is_windows {
        file.rsplit(|c| c == '\\' || c == '/')
            .next()
            .unwrap_or(&file)
            .to_string()
    } else {
        file
    };
    let base = file.strip_suffix(".exe").unwrap_or(&file);
    if is_windows {
        if base == "powershell" || base == "pwsh" {
            return vec![
                "-NoProfile".to_string(),
                "-NoLogo".to_string(),
                "-NonInteractive".to_string(),
                "-Command".to_string(),
                command.to_string(),
            ];
        }
        return vec!["/c".to_string(), command.to_string()];
    }
    // `/bin/sh` is not a login shell and does not need `-l`. Login shells
    // (bash/zsh/fish) get `-l` so nvm/fnm PATH setup still runs.
    if base == "sh" || base == "dash" {
        return vec!["-c".to_string(), command.to_string()];
    }
    vec!["-l".to_string(), "-c".to_string(), command.to_string()]
}

/// Node-style `shell: true` packaging for an ACP terminal request: join the
/// program and args into one command line, then wrap with [`shell_args`].
/// Returns `(shell_program, shell_args)`.
pub(crate) fn shell_true_invocation(command: &str, args: &[String]) -> (String, Vec<String>) {
    let shell = default_shell();
    let line = if args.is_empty() {
        command.to_string()
    } else {
        // Match Node child_process shell:true: space-join program + args
        // without re-quoting (Grok sends the full line in `command` alone).
        let mut parts = Vec::with_capacity(1 + args.len());
        parts.push(command);
        parts.extend(args.iter().map(String::as_str));
        parts.join(" ")
    };
    let shell_argv = shell_args(&shell, &line);
    (shell, shell_argv)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn never(_: &str) -> bool {
        false
    }

    #[test]
    fn posix_uses_env_shell_then_bin_sh() {
        assert_eq!(
            default_shell_for(false, Some("/bin/zsh"), None, never),
            "/bin/zsh"
        );
        assert_eq!(
            default_shell_for(false, None, Some(r"C:\Windows\system32\cmd.exe"), never),
            "/bin/sh"
        );
    }

    #[test]
    fn windows_prefers_pwsh_then_powershell_and_ignores_env_shell() {
        assert_eq!(
            default_shell_for(true, Some("/bin/zsh"), None, |n: &str| n == "pwsh"
                || n == "powershell"),
            "pwsh"
        );
        assert_eq!(
            default_shell_for(true, None, None, |n: &str| n == "powershell"),
            "powershell"
        );
    }

    #[test]
    fn windows_falls_back_to_comspec_then_cmd_exe() {
        assert_eq!(
            default_shell_for(true, None, Some(r"C:\Windows\system32\cmd.exe"), never),
            r"C:\Windows\system32\cmd.exe"
        );
        assert_eq!(default_shell_for(true, None, Some(""), never), "cmd.exe");
        assert_eq!(
            default_shell_for(true, Some("/bin/zsh"), None, never),
            "cmd.exe"
        );
    }

    #[test]
    fn scrubbed_env_removes_only_inherited_launcher_values() {
        assert_eq!(
            scrubbed_env_vars_except(&[]),
            ["npm_config_prefix"],
            "an omitted overlay still scrubs inherited launcher state"
        );

        let overlay = vec![("npm_config_prefix".to_string(), "/custom".to_string())];
        assert!(
            scrubbed_env_vars_except(&overlay).is_empty(),
            "an explicit overlay value must win"
        );
    }

    #[test]
    fn shell_args_posix_sh_uses_c_only() {
        assert_eq!(
            shell_args_for("/bin/sh", "echo hi", false),
            vec!["-c", "echo hi"]
        );
    }

    #[test]
    fn shell_args_posix_bash_uses_login_c() {
        assert_eq!(
            shell_args_for("/bin/bash", "echo hi", false),
            vec!["-l", "-c", "echo hi"]
        );
    }

    #[test]
    fn shell_args_windows_powershell_and_cmd() {
        let ps = ["-NoProfile", "-NoLogo", "-NonInteractive", "-Command", "x"];
        assert_eq!(shell_args_for("powershell.exe", "x", true), ps);
        assert_eq!(shell_args_for("cmd.exe", "x", true), vec!["/c", "x"]);
    }

    #[test]
    fn shell_true_invocation_preserves_packed_command_line() {
        let (_shell, args) = shell_true_invocation("/bin/bash -lc 'echo hi'", &[]);
        assert_eq!(
            args.last().map(String::as_str),
            Some("/bin/bash -lc 'echo hi'")
        );
    }
}
