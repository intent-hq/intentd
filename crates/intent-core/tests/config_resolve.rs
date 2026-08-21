//! Coverage for `Config::resolve()` env-var override branches (§11.2).
//!
//! Lives in its own integration-test binary so the `INTENTD_*` env mutations
//! cannot bleed into other test files. Within this binary the tests still share
//! one process, so they hold a global mutex while they tweak env vars.

use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, OnceLock};

use intent_core::config::{
    Config, DEFAULT_HOOKS_MAX_PER_AGENT, DEFAULT_IDLE_REAP_MINUTES,
    DEFAULT_SERVER_MAX_OUTSTANDING_RPCS, DEFAULT_STREAM_RETENTION_HOURS,
    DEFAULT_WAKE_RESUME_ENABLED, DEFAULT_WAKE_RESUME_THRESHOLD_SECONDS,
    MIN_WAKE_RESUME_THRESHOLD_SECONDS,
};
use intent_core::settings_file::DEFAULT_CONFIG_TEMPLATE;

/// Serializes env-mutating tests in this binary. Cargo runs `#[test]`s on
/// multiple threads by default, so without this guard `INTENTD_*` reads and
/// writes interleave across tests and the `Config::resolve()` assertions race.
fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let m = LOCK.get_or_init(|| Mutex::new(()));
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Returns a unique temp directory path (not necessarily created) for this
/// test run. We do not need the directory to exist for `resolve()` to succeed,
/// since `resolve()` only joins paths — it never reads from `data_dir` itself.
fn unique_temp(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}", uuid_like()))
}

fn uuid_like() -> String {
    // Cheap unique-ish suffix without taking a uuid dependency in the test
    // (uuid IS a dep of the crate, so use it via the same crate's surface).
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{nanos}-{:p}", &nanos as *const _)
}

#[test]
fn resolve_honors_data_dir_and_config_env_overrides() {
    let _g = env_lock();
    let data_dir = unique_temp("intentd-data");
    let config_path = unique_temp("intentd-config").join("config.toml");

    // Wipe any inherited overrides so this test owns the slot.
    std::env::remove_var("INTENTD_IDLE_REAP_MINUTES");
    std::env::remove_var("INTENTD_STREAM_RETENTION_HOURS");
    std::env::set_var("INTENTD_DATA_DIR", &data_dir);
    std::env::set_var("INTENTD_CONFIG", &config_path);

    let cfg = Config::resolve().expect("resolve with env overrides should succeed");

    assert_eq!(cfg.data_dir, data_dir);
    assert_eq!(cfg.config_path, config_path);
    assert_eq!(cfg.db_path, data_dir.join("intentd.db"));
    assert_eq!(cfg.socket_path, data_dir.join("intentd.sock"));
    assert_eq!(cfg.pid_path, data_dir.join("intentd.pid"));
    // Config file did not exist → resolve() initialized it with the commented
    // default template and used the documented defaults.
    assert_eq!(cfg.idle_reap_minutes, DEFAULT_IDLE_REAP_MINUTES);
    assert_eq!(cfg.stream_retention_hours, DEFAULT_STREAM_RETENTION_HOURS);
    assert_eq!(cfg.hooks_max_per_agent, DEFAULT_HOOKS_MAX_PER_AGENT);
    assert_eq!(
        cfg.server_max_outstanding_rpcs,
        DEFAULT_SERVER_MAX_OUTSTANDING_RPCS
    );
    assert_eq!(cfg.wake_resume_enabled, DEFAULT_WAKE_RESUME_ENABLED);
    assert_eq!(
        cfg.wake_resume_threshold_seconds,
        DEFAULT_WAKE_RESUME_THRESHOLD_SECONDS
    );
    let written = std::fs::read_to_string(&config_path).expect("config.toml was initialized");
    assert_eq!(written, DEFAULT_CONFIG_TEMPLATE);

    // Config is Clone + PartialEq + Eq + Debug — exercise the derives.
    let cloned = cfg.clone();
    assert_eq!(cfg, cloned);
    let dbg = format!("{cfg:?}");
    assert!(
        dbg.contains("data_dir"),
        "Debug should expose fields: {dbg}"
    );

    std::env::remove_var("INTENTD_DATA_DIR");
    std::env::remove_var("INTENTD_CONFIG");
    std::fs::remove_dir_all(config_path.parent().unwrap()).ok();
}

#[test]
fn resolve_defaults_config_path_into_data_dir() {
    let _g = env_lock();
    let data_dir = unique_temp("intentd-datadir-cfg");

    std::env::remove_var("INTENTD_IDLE_REAP_MINUTES");
    std::env::remove_var("INTENTD_STREAM_RETENTION_HOURS");
    std::env::remove_var("INTENTD_CONFIG");
    std::env::set_var("INTENTD_DATA_DIR", &data_dir);

    let cfg = Config::resolve().expect("resolve without INTENTD_CONFIG should succeed");
    assert_eq!(cfg.config_path, data_dir.join("config.toml"));
    assert!(
        cfg.config_path.is_file(),
        "resolve() initializes config.toml in the data dir"
    );

    std::env::remove_var("INTENTD_DATA_DIR");
    std::fs::remove_dir_all(&data_dir).ok();
}

#[test]
fn resolve_fails_on_malformed_config_file() {
    let _g = env_lock();
    let data_dir = unique_temp("intentd-data-bad");
    let config_dir = unique_temp("intentd-cfgdir-bad");
    std::fs::create_dir_all(&config_dir).unwrap();
    let config_path = config_dir.join("config.toml");
    std::fs::write(&config_path, "[agents]\nidleReapMinuets = 5\n").unwrap();

    std::env::remove_var("INTENTD_IDLE_REAP_MINUTES");
    std::env::remove_var("INTENTD_STREAM_RETENTION_HOURS");
    std::env::set_var("INTENTD_DATA_DIR", &data_dir);
    std::env::set_var("INTENTD_CONFIG", &config_path);

    let err = Config::resolve().expect_err("unknown key must fail resolve()");
    let msg = err.to_string();
    assert!(msg.contains("idleReapMinuets"), "names the bad key: {msg}");
    assert!(msg.contains("config.toml"), "names the file: {msg}");

    std::env::remove_var("INTENTD_DATA_DIR");
    std::env::remove_var("INTENTD_CONFIG");
    std::fs::remove_dir_all(&config_dir).ok();
}

#[test]
fn resolve_reads_config_file_when_present() {
    let _g = env_lock();
    let data_dir = unique_temp("intentd-data2");
    let config_dir = unique_temp("intentd-cfgdir2");
    std::fs::create_dir_all(&config_dir).unwrap();
    let config_path = config_dir.join("config.toml");
    std::fs::write(
        &config_path,
        "[agents]\nidleReapMinutes = 7\n\n[events]\nstreamRetentionHours = 24\n\n[hooks]\nmaxPerAgent = 3\n\n[server]\nmaxOutstandingRpcs = 12\n\n[wakeResume]\nenabled = false\nthresholdSeconds = 30\n",
    )
    .unwrap();

    std::env::remove_var("INTENTD_IDLE_REAP_MINUTES");
    std::env::remove_var("INTENTD_STREAM_RETENTION_HOURS");
    std::env::set_var("INTENTD_DATA_DIR", &data_dir);
    std::env::set_var("INTENTD_CONFIG", &config_path);

    let cfg = Config::resolve().expect("resolve with populated config should succeed");
    assert_eq!(cfg.idle_reap_minutes, 7);
    assert_eq!(cfg.stream_retention_hours, 24);
    assert_eq!(cfg.hooks_max_per_agent, 3);
    assert_eq!(cfg.server_max_outstanding_rpcs, 12);
    assert!(!cfg.wake_resume_enabled);
    assert_eq!(cfg.wake_resume_threshold_seconds, 30);

    std::env::remove_var("INTENTD_DATA_DIR");
    std::env::remove_var("INTENTD_CONFIG");
    std::fs::remove_file(&config_path).ok();
    std::fs::remove_dir_all(&config_dir).ok();
}

#[test]
fn resolve_clamps_zero_threshold_to_minimum() {
    let _g = env_lock();
    let data_dir = unique_temp("intentd-data-thresh0");
    let config_dir = unique_temp("intentd-cfgdir-thresh0");
    std::fs::create_dir_all(&config_dir).unwrap();
    let config_path = config_dir.join("config.toml");
    // `thresholdSeconds = 0` would make the clock-skew detector flag every
    // ~1s sampling tick as a suspend; resolve() must clamp it up to the floor.
    std::fs::write(
        &config_path,
        "[wakeResume]\nenabled = true\nthresholdSeconds = 0\n",
    )
    .unwrap();

    std::env::remove_var("INTENTD_IDLE_REAP_MINUTES");
    std::env::remove_var("INTENTD_STREAM_RETENTION_HOURS");
    std::env::set_var("INTENTD_DATA_DIR", &data_dir);
    std::env::set_var("INTENTD_CONFIG", &config_path);

    let cfg = Config::resolve().expect("resolve with zero threshold should succeed");
    assert!(cfg.wake_resume_enabled);
    assert_eq!(
        cfg.wake_resume_threshold_seconds, MIN_WAKE_RESUME_THRESHOLD_SECONDS,
        "a configured thresholdSeconds of 0 is clamped up to the sane minimum"
    );
    assert_ne!(
        cfg.wake_resume_threshold_seconds, 0,
        "the clamped threshold is never 0 (which would misclassify every tick)"
    );

    std::env::remove_var("INTENTD_DATA_DIR");
    std::env::remove_var("INTENTD_CONFIG");
    std::fs::remove_file(&config_path).ok();
    std::fs::remove_dir_all(&config_dir).ok();
}

#[test]
fn resolve_env_overrides_for_idle_and_retention_take_precedence() {
    let _g = env_lock();
    let data_dir = unique_temp("intentd-data3");
    let config_path = unique_temp("intentd-config3").join("config.toml");

    std::env::set_var("INTENTD_DATA_DIR", &data_dir);
    std::env::set_var("INTENTD_CONFIG", &config_path);
    std::env::set_var("INTENTD_IDLE_REAP_MINUTES", "99");
    std::env::set_var("INTENTD_STREAM_RETENTION_HOURS", "42");

    let cfg = Config::resolve().unwrap();
    assert_eq!(cfg.idle_reap_minutes, 99);
    assert_eq!(cfg.stream_retention_hours, 42);

    // Garbage values fall through to the next source (file/default here).
    std::env::set_var("INTENTD_IDLE_REAP_MINUTES", "not-a-number");
    std::env::set_var("INTENTD_STREAM_RETENTION_HOURS", "definitely-not");
    let cfg = Config::resolve().unwrap();
    assert_eq!(cfg.idle_reap_minutes, DEFAULT_IDLE_REAP_MINUTES);
    assert_eq!(cfg.stream_retention_hours, DEFAULT_STREAM_RETENTION_HOURS);

    std::env::remove_var("INTENTD_IDLE_REAP_MINUTES");
    std::env::remove_var("INTENTD_STREAM_RETENTION_HOURS");
    std::env::remove_var("INTENTD_DATA_DIR");
    std::env::remove_var("INTENTD_CONFIG");
    std::fs::remove_dir_all(config_path.parent().unwrap()).ok();
}
