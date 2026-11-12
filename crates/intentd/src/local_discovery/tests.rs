use std::ffi::OsString;
use std::sync::Mutex;

use super::*;

fn fields(instance_id: &str, data_dir: &Path) -> StaticFields {
    StaticFields {
        instance_id: instance_id.to_string(),
        pid: std::process::id(),
        started_at: "2026-01-01T00:00:00Z".to_string(),
        data_dir: data_dir.to_path_buf(),
        daemon_version: "0.0.0-test".to_string(),
        protocol_version: "18.0".to_string(),
        fingerprint: "AA:BB".to_string(),
        product_name: "Intent Test".to_string(),
    }
}

fn record(instance_id: &str, pid: u32) -> LocalDaemonRecord {
    let dir = PathBuf::from("/data");
    let mut fields = fields(instance_id, &dir);
    fields.pid = pid;
    fields.record(&LiveFields {
        port: Some(4242),
        fingerprint: None,
        machine_name: None,
    })
}

/// A pid that is not running: a child that has already been reaped.
fn dead_pid() -> u32 {
    let mut child = std::process::Command::new(if cfg!(windows) { "cmd" } else { "true" })
        .args(if cfg!(windows) {
            &["/C", "exit"][..]
        } else {
            &[][..]
        })
        .spawn()
        .expect("spawn short-lived child");
    let pid = child.id();
    child.wait().expect("reap child");
    pid
}

#[test]
fn record_serializes_with_contract_field_names() {
    let value = serde_json::to_value(record("abc", 7)).unwrap();
    let mut keys: Vec<_> = value.as_object().unwrap().keys().cloned().collect();
    keys.sort();
    assert_eq!(
        keys,
        [
            "capabilities",
            "daemonVersion",
            "dataDir",
            "displayName",
            "fingerprint",
            "instanceId",
            "pid",
            "port",
            "protocolVersion",
            "startedAt",
            "updatedAt",
            "version"
        ]
    );
    assert_eq!(value["version"], 1);
    assert_eq!(value["port"], 4242);
    assert_eq!(value["fingerprint"], "AA:BB");
    assert_eq!(value["displayName"], "Intent Test");
}

#[test]
fn capability_follows_the_build_flag() {
    let value = serde_json::to_value(record("abc", 7)).unwrap();
    // Readers treat a missing value as unsupported; only exactly 1 is support.
    let expected = if LOCAL_CODE_PAIRING_SUPPORTED {
        serde_json::json!({ "localCodePairing": 1 })
    } else {
        serde_json::json!({})
    };
    assert_eq!(value["capabilities"], expected);
}

#[test]
fn capability_serializes_as_exact_integer_one_when_supported() {
    let caps = Capabilities {
        local_code_pairing: Some(1),
    };
    assert_eq!(
        serde_json::to_value(caps).unwrap(),
        serde_json::json!({ "localCodePairing": 1 })
    );
}

#[test]
fn port_is_null_while_listener_is_down() {
    let fields = fields("abc", Path::new("/data"));
    let value = serde_json::to_value(fields.record(&LiveFields {
        port: None,
        fingerprint: None,
        machine_name: None,
    }))
    .unwrap();
    assert!(value["port"].is_null());
    assert_eq!(value["fingerprint"], "AA:BB");
}

#[test]
fn display_name_prefers_machine_name_then_product_name() {
    let fields = fields("abc", Path::new("/data"));
    let with = |name: Option<&str>| {
        fields
            .record(&LiveFields {
                port: None,
                fingerprint: Some("CC:DD".into()),
                machine_name: name.map(str::to_string),
            })
            .display_name
    };
    assert_eq!(with(Some(" Studio Mac ")), "Studio Mac");
    assert_eq!(with(Some("  ")), "Intent Test");
    assert_eq!(with(None), "Intent Test");
}

#[test]
fn product_name_env_override_and_build_default() {
    assert_eq!(product_name_from(Some(" Intent Alpha ")), "Intent Alpha");
    let default = if cfg!(debug_assertions) {
        "Intent (dev build)"
    } else {
        "Intent"
    };
    assert_eq!(product_name_from(None), default);
    assert_eq!(product_name_from(Some("")), default);
    assert_eq!(product_name_from(Some("bad\nname")), default);
}

#[test]
fn dir_override_wins_on_every_platform() {
    let env = |name: &str| (name == DIR_ENV).then(|| OsString::from("/tmp/override"));
    assert_eq!(
        resolve_dir_from(&env, Some(Path::new("/home/u"))),
        Some(PathBuf::from("/tmp/override"))
    );
    let empty = |name: &str| (name == DIR_ENV).then(OsString::new);
    assert_ne!(
        resolve_dir_from(&empty, Some(Path::new("/home/u"))),
        Some(PathBuf::new())
    );
}

#[cfg(target_os = "macos")]
#[test]
fn dir_default_macos() {
    assert_eq!(
        resolve_dir_from(&|_| None, Some(Path::new("/Users/u"))),
        Some(PathBuf::from(
            "/Users/u/Library/Application Support/Intent/local-daemons"
        ))
    );
}

#[cfg(all(unix, not(target_os = "macos")))]
#[test]
fn dir_default_linux_prefers_xdg_runtime_dir() {
    let runtime = |name: &str| (name == "XDG_RUNTIME_DIR").then(|| OsString::from("/run/user/1"));
    assert_eq!(
        resolve_dir_from(&runtime, Some(Path::new("/home/u"))),
        Some(PathBuf::from("/run/user/1/intent/local-daemons"))
    );
    assert_eq!(
        resolve_dir_from(&|_| None, Some(Path::new("/home/u"))),
        Some(PathBuf::from("/home/u/.local/state/intent/local-daemons"))
    );
}

#[test]
fn instance_id_is_created_once_and_kept() {
    let data = tempfile::tempdir().unwrap();
    let first = load_or_create_instance_id(data.path()).unwrap();
    assert!(uuid::Uuid::parse_str(&first).is_ok());
    assert_eq!(load_or_create_instance_id(data.path()).unwrap(), first);
    std::fs::write(data.path().join(INSTANCE_ID_FILE), "garbage").unwrap();
    let replaced = load_or_create_instance_id(data.path()).unwrap();
    assert_ne!(replaced, first);
    assert!(uuid::Uuid::parse_str(&replaced).is_ok());
}

#[cfg(unix)]
#[test]
fn folder_is_0700_and_record_is_0600() {
    use std::os::unix::fs::PermissionsExt as _;
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("nested").join("local-daemons");
    ensure_dir(&dir).unwrap();
    write_record(&dir, &record("abc", 7)).unwrap();
    let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&dir), 0o700);
    assert_eq!(mode(&record_path(&dir, "abc")), 0o600);
    // No temporary file is left behind.
    let names: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(names, [OsString::from("abc.json")]);
}

#[cfg(unix)]
#[test]
fn existing_loose_folder_is_tightened_to_0700() {
    use std::os::unix::fs::PermissionsExt as _;
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("local-daemons");
    std::fs::create_dir(&dir).unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    ensure_dir(&dir).unwrap();
    assert_eq!(
        std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
        0o700
    );
}

#[cfg(unix)]
#[test]
fn symlinked_folder_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("elsewhere");
    std::fs::create_dir(&target).unwrap();
    let dir = root.path().join("local-daemons");
    std::os::unix::fs::symlink(&target, &dir).unwrap();
    assert!(ensure_dir(&dir).is_err());
}

#[test]
fn file_in_place_of_folder_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("local-daemons");
    std::fs::write(&dir, b"x").unwrap();
    assert!(ensure_dir(&dir).is_err());
}

#[cfg(unix)]
#[test]
fn planted_symlink_at_record_path_is_replaced_not_followed() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("local-daemons");
    ensure_dir(&dir).unwrap();
    let victim = root.path().join("victim");
    std::fs::write(&victim, b"keep").unwrap();
    std::os::unix::fs::symlink(&victim, record_path(&dir, "abc")).unwrap();
    // A planted temp-file symlink is also removed, never written through.
    std::os::unix::fs::symlink(&victim, dir.join(".abc.json.tmp")).unwrap();
    write_record(&dir, &record("abc", 7)).unwrap();
    assert_eq!(std::fs::read(&victim).unwrap(), b"keep");
    let meta = std::fs::symlink_metadata(record_path(&dir, "abc")).unwrap();
    assert!(meta.file_type().is_file());
    let written = read_record(&record_path(&dir, "abc")).expect("record readable");
    assert_eq!(written.instance_id, "abc");
    assert_eq!(written.port, Some(4242));
}

#[test]
fn rewrite_replaces_previous_record() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("d");
    ensure_dir(&dir).unwrap();
    let mut rec = record("abc", 7);
    write_record(&dir, &rec).unwrap();
    rec.port = Some(5555);
    write_record(&dir, &rec).unwrap();
    assert_eq!(
        read_record(&record_path(&dir, "abc")).unwrap().port,
        Some(5555)
    );
    remove_record(&dir, "abc").unwrap();
    assert!(!record_path(&dir, "abc").exists());
    // Removing an absent record is not an error.
    remove_record(&dir, "abc").unwrap();
}

#[test]
fn reader_skips_oversized_and_unparseable_files() {
    let root = tempfile::tempdir().unwrap();
    let big = root.path().join("big.json");
    std::fs::write(&big, vec![b' '; 17 * 1024]).unwrap();
    assert_eq!(read_record(&big), None);
    let junk = root.path().join("junk.json");
    std::fs::write(&junk, b"{not json").unwrap();
    assert_eq!(read_record(&junk), None);
}

#[cfg(unix)]
#[test]
fn reader_skips_symlinks() {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real.json");
    std::fs::write(&real, serde_json::to_vec(&record("abc", 7)).unwrap()).unwrap();
    let link = root.path().join("abc.json");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    assert!(read_record(&real).is_some());
    assert_eq!(read_record(&link), None);
}

#[test]
fn sweep_removes_dead_records_and_keeps_live_own_and_foreign_files() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("d");
    ensure_dir(&dir).unwrap();
    let dead = dead_pid();
    assert!(!pid_is_running(dead));
    write_record(&dir, &record("dead", dead)).unwrap();
    write_record(&dir, &record("live", std::process::id())).unwrap();
    // Our own record is replaced, never swept, even with a dead pid.
    write_record(&dir, &record("own", dead)).unwrap();
    // A file whose name does not match its instanceId is not ours to judge.
    std::fs::write(
        dir.join("other-name.json"),
        serde_json::to_vec(&record("mismatch", dead)).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.join("junk.json"), b"{").unwrap();
    std::fs::write(dir.join("notes.txt"), b"x").unwrap();

    assert_eq!(sweep_stale(&dir, "own"), 1);
    assert!(!record_path(&dir, "dead").exists());
    assert!(record_path(&dir, "live").exists());
    assert!(record_path(&dir, "own").exists());
    assert!(dir.join("other-name.json").exists());
    assert!(dir.join("junk.json").exists());
    assert!(dir.join("notes.txt").exists());
}

#[test]
fn pid_probe() {
    assert!(pid_is_running(std::process::id()));
    assert!(!pid_is_running(0));
    assert!(!pid_is_running(dead_pid()));
}

struct FakeSource(Mutex<LiveFields>);

impl LiveSource for FakeSource {
    fn live(&self) -> Pin<Box<dyn Future<Output = LiveFields> + Send + '_>> {
        let live = self.0.lock().unwrap().clone();
        Box::pin(async move { live })
    }
}

async fn wait_for(path: &Path, pred: impl Fn(Option<&LocalDaemonRecord>) -> bool) {
    let deadline =
        tokio::time::Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(10));
    loop {
        if pred(read_record(path).as_ref()) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "record at {} never reached the expected state; last: {:?}",
            path.display(),
            read_record(path)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn publisher_writes_updates_and_removes_on_shutdown() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("d");
    let data = root.path().join("data");
    let stale = dead_pid();
    ensure_dir(&dir).unwrap();
    write_record(&dir, &record("stale", stale)).unwrap();

    let source = Arc::new(FakeSource(Mutex::new(LiveFields {
        port: Some(1111),
        fingerprint: None,
        machine_name: None,
    })));
    let publisher = Publisher::start(dir.clone(), fields("me", &data), source.clone())
        .await
        .expect("publisher starts");
    let path = record_path(&dir, "me");
    // Written synchronously at start, and the stale record is swept.
    let first = read_record(&path).expect("record written at start");
    assert_eq!(first.port, Some(1111));
    assert_eq!(first.pid, std::process::id());
    assert!(!record_path(&dir, "stale").exists());

    // Listener stops: port becomes null.
    source.0.lock().unwrap().port = None;
    wait_for(&path, |r| r.is_some_and(|r| r.port.is_none())).await;
    // Listener restarts on a new port with a new name.
    {
        let mut live = source.0.lock().unwrap();
        live.port = Some(2222);
        live.machine_name = Some("Desk".into());
    }
    wait_for(&path, |r| {
        r.is_some_and(|r| r.port == Some(2222) && r.display_name == "Desk")
    })
    .await;
    // A deleted record is restored.
    std::fs::remove_file(&path).unwrap();
    wait_for(&path, |r| r.is_some()).await;

    publisher.shutdown().await;
    assert!(!path.exists(), "clean shutdown removes the record");
}

#[cfg(unix)]
#[tokio::test]
async fn publisher_declines_an_unsafe_folder() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("elsewhere");
    std::fs::create_dir(&target).unwrap();
    let dir = root.path().join("d");
    std::os::unix::fs::symlink(&target, &dir).unwrap();
    let source = Arc::new(FakeSource(Mutex::new(LiveFields {
        port: Some(1),
        fingerprint: None,
        machine_name: None,
    })));
    assert!(Publisher::start(dir, fields("me", root.path()), source)
        .await
        .is_none());
    assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
}
