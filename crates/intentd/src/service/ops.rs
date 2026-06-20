//! Install / uninstall / validate the platform service unit (§5.8).

use intent_core::Config;

use super::plan;

/// Install (or refresh) the platform service unit, writing it under the user's
/// service directory. Idempotent: rewrites the file to the current contents so
/// an upgraded `intentd` path is picked up. Prints the enable hint on success.
pub fn install(config: &Config) -> anyhow::Result<()> {
    let target = plan(config)?;
    if let Some(parent) = target.path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("create {}: {e}", parent.display()))?;
    }
    // Ensure the log directory referenced by the unit exists (launchd writes to
    // it at load time; a missing dir would make the service fail to start).
    std::fs::create_dir_all(config.data_dir.join("logs"))
        .map_err(|e| anyhow::anyhow!("create log dir: {e}"))?;
    std::fs::write(&target.path, target.content.as_bytes())
        .map_err(|e| anyhow::anyhow!("write {}: {e}", target.path.display()))?;
    println!("installed {} unit: {}", target.kind, target.path.display());
    println!("enable it with: {}", target.enable_hint);
    Ok(())
}

/// Remove the installed service unit. A missing unit is not an error (the end
/// state — no unit — is the same), but is reported so the operator knows.
pub fn uninstall(config: &Config) -> anyhow::Result<()> {
    let target = plan(config)?;
    if target.path.exists() {
        std::fs::remove_file(&target.path)
            .map_err(|e| anyhow::anyhow!("remove {}: {e}", target.path.display()))?;
        println!("removed {} unit: {}", target.kind, target.path.display());
    } else {
        println!(
            "no {} unit installed at {}",
            target.kind,
            target.path.display()
        );
    }
    Ok(())
}

/// Report whether the service unit is installed and current (its on-disk
/// contents match what this `intentd` would generate). Returns `true` only when
/// installed AND current, so callers can map it to an exit code.
pub fn status(config: &Config) -> anyhow::Result<bool> {
    let target = plan(config)?;
    if !target.path.exists() {
        println!(
            "{} unit not installed (expected at {})",
            target.kind,
            target.path.display()
        );
        return Ok(false);
    }
    let on_disk = std::fs::read_to_string(&target.path)
        .map_err(|e| anyhow::anyhow!("read {}: {e}", target.path.display()))?;
    if on_disk == target.content {
        println!(
            "{} unit installed and current: {}",
            target.kind,
            target.path.display()
        );
        Ok(true)
    } else {
        println!(
            "{} unit installed but STALE (run `intentd service install` to refresh): {}",
            target.kind,
            target.path.display()
        );
        Ok(false)
    }
}
