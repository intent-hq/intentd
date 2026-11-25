//! Locating the tailcat sidecar binary, which every backend that reaches
//! machines over their tailcat address needs (the daemon and the
//! orchestrator alike).

use std::path::PathBuf;

/// Resolve the tailcat binary: `INTENTD_TAILCAT_BIN` env override (dev seam)
/// → `libexec/tailcat` next to the daemon binary (release archive layout) →
/// `tailcat` next to the daemon binary → bare `tailcat` (PATH lookup).
#[must_use]
pub fn resolve_tailcat_bin() -> PathBuf {
    if let Ok(p) = std::env::var("INTENTD_TAILCAT_BIN") {
        if !p.is_empty() {
            return PathBuf::from(p);
        }
    }
    let name = if cfg!(windows) {
        "tailcat.exe"
    } else {
        "tailcat"
    };
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let libexec = dir.join("libexec").join(name);
            if libexec.exists() {
                return libexec;
            }
            let sibling = dir.join(name);
            if sibling.exists() {
                return sibling;
            }
        }
    }
    PathBuf::from(name)
}
