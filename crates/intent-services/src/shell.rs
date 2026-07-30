//! Default-shell selection shared by `terminal.create` (command omitted) and
//! saved-script runs (`script_ops`).
//!
//! POSIX mirrors the ancestor's reliance on the user's login shell (`$SHELL`,
//! then `/bin/sh`). Windows never consults `$SHELL`: it resolves a native shell
//! instead — PowerShell first (`pwsh`, then `powershell`), then `%COMSPEC%`,
//! then `cmd.exe` — so daemon-spawned shells never point at `/bin/sh`.

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
}
