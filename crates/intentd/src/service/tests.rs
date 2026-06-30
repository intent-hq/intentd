//! Golden-string tests for the launchd plist + systemd unit (§5.8).

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, OnceLock};

use intent_core::Config;
use uuid::Uuid;

use super::*;

#[test]
fn launchd_plist_matches_golden() {
    let plist = launchd_plist(
        "/usr/local/bin/intentd",
        "/data/logs/intentd.out.log",
        "/data/logs/intentd.err.log",
    );
    let expected = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>ai.intent.intentd</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/intentd</string>
        <string>serve</string>
        <string>--listen</string>
        <string>uds</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>Crashed</key>
        <true/>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>/data/logs/intentd.out.log</string>
    <key>StandardErrorPath</key>
    <string>/data/logs/intentd.err.log</string>
</dict>
</plist>
"#;
    assert_eq!(plist, expected);
}

#[test]
fn systemd_unit_matches_golden() {
    let unit = systemd_unit("/usr/local/bin/intentd");
    let expected = "[Unit]\n\
Description=Intent backend daemon (intentd)\n\
After=network.target\n\
\n\
[Service]\n\
Type=simple\n\
ExecStart=/usr/local/bin/intentd serve\n\
ExecStop=/usr/local/bin/intentd stop\n\
Restart=on-failure\n\
\n\
[Install]\n\
WantedBy=default.target\n";
    assert_eq!(unit, expected);
}

#[test]
fn launchd_keepalive_does_not_relaunch_clean_stop() {
    // §5.8: KeepAlive must gate on Crashed=true + SuccessfulExit=false so a
    // graceful `intentd stop` (clean exit) is NOT relaunched by launchd.
    let plist = launchd_plist("intentd", "/o", "/e");
    assert!(plist.contains("<key>SuccessfulExit</key>\n        <false/>"));
    assert!(plist.contains("<key>Crashed</key>\n        <true/>"));
}

#[test]
fn systemd_unit_stops_via_cli_and_restarts_on_failure() {
    let unit = systemd_unit("intentd");
    assert!(unit.contains("ExecStop=intentd stop"));
    assert!(unit.contains("Restart=on-failure"));
    assert!(unit.contains("Type=simple"));
    assert!(unit.contains("WantedBy=default.target"));
}

// --- service::plan + service::ops glue ---------------------------------------
//
// These tests mutate the process-wide `HOME` env var, so they serialize on a
// shared mutex and restore the prior value on drop. They are still pure unit
// tests — no daemon process is spawned and the filesystem is confined to
// per-test temp directories.

fn home_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

struct HomeOverride {
    _lock: MutexGuard<'static, ()>,
    original: Option<OsString>,
    home: PathBuf,
}

impl HomeOverride {
    fn new() -> Self {
        let lock = home_lock();
        let original = std::env::var_os("HOME");
        let home = std::env::temp_dir().join(format!("intentd-svc-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&home).unwrap();
        std::env::set_var("HOME", &home);
        Self {
            _lock: lock,
            original,
            home,
        }
    }
}

impl Drop for HomeOverride {
    fn drop(&mut self) {
        match self.original.take() {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

fn temp_data_dir() -> PathBuf {
    std::env::temp_dir().join(format!("intentd-data-{}", Uuid::new_v4()))
}

fn mk_config(data_dir: PathBuf) -> Config {
    Config {
        config_path: data_dir.join("config.toml"),
        db_path: data_dir.join("intentd.db"),
        socket_path: data_dir.join("intentd.sock"),
        pid_path: data_dir.join("intentd.pid"),
        idle_reap_minutes: 30,
        stream_retention_hours: 0,
        data_dir,
    }
}

#[test]
fn plan_uses_platform_unit_path_and_content() {
    let _home = HomeOverride::new();
    let data_dir = temp_data_dir();
    let config = mk_config(data_dir.clone());

    let target = plan(&config).expect("plan resolves on a supported platform");

    if cfg!(target_os = "macos") {
        assert_eq!(target.kind, "launchd");
        let plist_name = format!("{LAUNCHD_LABEL}.plist");
        assert!(
            target
                .path
                .ends_with(format!("Library/LaunchAgents/{plist_name}")),
            "unexpected launchd path: {}",
            target.path.display()
        );
        assert!(target.content.starts_with("<?xml"));
        assert!(target.content.contains(LAUNCHD_LABEL));
        assert!(target.enable_hint.contains("launchctl"));
        assert!(target.enable_hint.contains("Library/LaunchAgents"));
    } else if cfg!(target_os = "linux") {
        assert_eq!(target.kind, "systemd");
        assert!(
            target
                .path
                .ends_with(format!(".config/systemd/user/{SYSTEMD_UNIT_NAME}")),
            "unexpected systemd path: {}",
            target.path.display()
        );
        assert!(target.content.starts_with("[Unit]"));
        assert!(target.content.contains(" serve"));
        assert!(target.content.contains(" stop"));
        assert_eq!(target.enable_hint, "systemctl --user enable --now intentd");
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
#[cfg(target_os = "macos")]
fn plan_macos_embeds_data_dir_log_paths() {
    let _home = HomeOverride::new();
    let data_dir = temp_data_dir();
    let config = mk_config(data_dir.clone());
    let target = plan(&config).expect("plan resolves on macOS");
    let out_log = data_dir.join("logs").join("intentd.out.log");
    let err_log = data_dir.join("logs").join("intentd.err.log");
    assert!(target.content.contains(&*out_log.to_string_lossy()));
    assert!(target.content.contains(&*err_log.to_string_lossy()));
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn plan_fails_when_home_is_empty() {
    let _lock = home_lock();
    let original = std::env::var_os("HOME");
    std::env::set_var("HOME", "");
    let data_dir = temp_data_dir();
    let config = mk_config(data_dir.clone());

    let result = plan(&config);

    match original {
        Some(v) => std::env::set_var("HOME", v),
        None => std::env::remove_var("HOME"),
    }

    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("empty HOME must error out"),
    };
    assert!(
        err.to_string().contains("HOME"),
        "error should mention HOME: {err}"
    );
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn install_writes_unit_and_creates_log_dir() {
    let _home = HomeOverride::new();
    let data_dir = temp_data_dir();
    let config = mk_config(data_dir.clone());

    install(&config).expect("install succeeds with a writable HOME");

    let target = plan(&config).unwrap();
    assert!(target.path.exists(), "unit file written to disk");
    assert_eq!(
        std::fs::read_to_string(&target.path).unwrap(),
        target.content,
        "on-disk content matches the planned unit"
    );
    assert!(
        data_dir.join("logs").is_dir(),
        "data_dir/logs created for the daemon's stdio"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn install_is_idempotent_and_refreshes_stale_unit() {
    let _home = HomeOverride::new();
    let data_dir = temp_data_dir();
    let config = mk_config(data_dir.clone());

    install(&config).expect("first install");
    let target = plan(&config).unwrap();
    // Simulate an upgrade leaving stale contents on disk.
    std::fs::write(&target.path, b"stale contents\n").unwrap();
    install(&config).expect("second install refreshes the unit");
    assert_eq!(
        std::fs::read_to_string(&target.path).unwrap(),
        target.content,
        "re-install rewrites the file with the current contents"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn uninstall_removes_existing_unit() {
    let _home = HomeOverride::new();
    let data_dir = temp_data_dir();
    let config = mk_config(data_dir.clone());

    install(&config).expect("install");
    let target = plan(&config).unwrap();
    assert!(target.path.exists());

    uninstall(&config).expect("uninstall removes the installed unit");
    assert!(!target.path.exists(), "unit file deleted");

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn uninstall_missing_unit_is_not_an_error() {
    let _home = HomeOverride::new();
    let data_dir = temp_data_dir();
    let config = mk_config(data_dir.clone());

    // No prior install; the end state (no unit) is already the goal.
    uninstall(&config).expect("uninstall of a missing unit is reported, not an error");

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn status_reports_false_when_not_installed() {
    let _home = HomeOverride::new();
    let data_dir = temp_data_dir();
    let config = mk_config(data_dir.clone());

    assert!(
        !status(&config).expect("status without an installed unit is Ok(false)"),
        "absent unit must report not-current"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn status_reports_true_after_fresh_install() {
    let _home = HomeOverride::new();
    let data_dir = temp_data_dir();
    let config = mk_config(data_dir.clone());

    install(&config).expect("install");
    assert!(
        status(&config).expect("status after install"),
        "freshly-installed unit must be reported as current"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn status_reports_false_when_unit_is_stale() {
    let _home = HomeOverride::new();
    let data_dir = temp_data_dir();
    let config = mk_config(data_dir.clone());

    install(&config).expect("install");
    let target = plan(&config).unwrap();
    std::fs::write(&target.path, b"drifted contents\n").unwrap();

    assert!(
        !status(&config).expect("status when contents drift"),
        "stale on-disk unit must report not-current"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}
