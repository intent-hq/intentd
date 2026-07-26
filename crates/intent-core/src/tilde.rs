//! Home-directory tilde expansion for caller-supplied paths.
//!
//! IPC clients send paths like `~/Developer/repo` (the FE onboarding default),
//! but neither Rust nor git expands `~` — git treats it as the literal
//! relative directory `./~`, which fails on the packaged sidecar's read-only
//! cwd (intent-hq/monorepo#822). Path-typed params are expanded at the daemon
//! boundary with these helpers: a leading `~` / `~/` resolves to the user's
//! home directory; `~user` forms and non-tilde paths pass through unchanged.

use std::path::{Path, PathBuf};

/// Expand a leading `~` / `~/` in `input` against `home`. Anything else —
/// including `~user` forms — passes through verbatim.
pub fn expand_tilde_with(input: &str, home: &Path) -> PathBuf {
    if input == "~" {
        return home.to_path_buf();
    }
    if let Some(rest) = input.strip_prefix("~/") {
        return home.join(rest);
    }
    PathBuf::from(input)
}

/// Expand a leading `~` / `~/` against the process environment's home
/// directory (`$HOME`, with `USERPROFILE` as the Windows fallback). When no
/// home can be resolved the input passes through unchanged.
pub fn expand_tilde(input: &str) -> PathBuf {
    match env_home_dir() {
        Some(home) => expand_tilde_with(input, &home),
        None => PathBuf::from(input),
    }
}

/// Resolve the home directory from the environment. `None` when unset so
/// callers degrade to passthrough instead of guessing.
fn env_home_dir() -> Option<PathBuf> {
    if let Some(home) = std::env::var_os("HOME") {
        if !home.is_empty() {
            return Some(PathBuf::from(home));
        }
    }
    #[cfg(windows)]
    if let Some(profile) = std::env::var_os("USERPROFILE") {
        if !profile.is_empty() {
            return Some(PathBuf::from(profile));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_tilde_expands_to_home() {
        assert_eq!(
            expand_tilde_with("~", Path::new("/home/u")),
            PathBuf::from("/home/u")
        );
    }

    #[test]
    fn tilde_slash_prefix_expands_under_home() {
        assert_eq!(
            expand_tilde_with("~/Developer/repo", Path::new("/home/u")),
            PathBuf::from("/home/u/Developer/repo")
        );
    }

    #[test]
    fn tilde_user_form_passes_through() {
        assert_eq!(
            expand_tilde_with("~alice/repo", Path::new("/home/u")),
            PathBuf::from("~alice/repo")
        );
    }

    #[test]
    fn non_tilde_paths_pass_through() {
        assert_eq!(
            expand_tilde_with("/abs/path", Path::new("/home/u")),
            PathBuf::from("/abs/path")
        );
        assert_eq!(
            expand_tilde_with("rel/path", Path::new("/home/u")),
            PathBuf::from("rel/path")
        );
        assert_eq!(expand_tilde_with("", Path::new("/home/u")), PathBuf::new());
    }

    #[test]
    fn interior_tilde_passes_through() {
        assert_eq!(
            expand_tilde_with("/data/~backup", Path::new("/home/u")),
            PathBuf::from("/data/~backup")
        );
    }

    #[test]
    fn env_variant_expands_against_process_home() {
        let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
            eprintln!("skipping env tilde test: HOME not set");
            return;
        };
        assert_eq!(expand_tilde("~/x"), home.join("x"));
        assert_eq!(expand_tilde("/abs/x"), PathBuf::from("/abs/x"));
    }
}
