use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::time::{Duration, SystemTime};

use intent_core::blob::{BlobEntry, TempSweep, TEMP_SUFFIX as COMPACT_EXT};
use serde_json::Value;

use super::*;
use intent_services::feature_api::pagination::paginate_text_lines;

thread_local! {
    /// Tests set this to make every compaction replace fail, like Windows
    /// refusing to replace a log another process holds open.
    static FAIL_RENAME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// [`LocalBlobStorage`] whose `replace` fails while [`FAIL_RENAME`] is set.
#[derive(Debug)]
struct RenameGate(LocalBlobStorage);

impl BlobStorage for RenameGate {
    fn display_path(&self, key: &BlobKey) -> PathBuf {
        self.0.display_path(key)
    }
    fn open_append(&self, key: &BlobKey) -> io::Result<Box<dyn BlobFile>> {
        self.0.open_append(key)
    }
    fn replace(&self, key: &BlobKey, contents: &[u8]) -> io::Result<Box<dyn BlobFile>> {
        if FAIL_RENAME.with(std::cell::Cell::get) {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied));
        }
        self.0.replace(key, contents)
    }
    fn open_read(&self, key: &BlobKey) -> io::Result<Option<Box<dyn BlobFile>>> {
        self.0.open_read(key)
    }
    fn exists(&self, key: &BlobKey) -> io::Result<bool> {
        self.0.exists(key)
    }
    fn modified(&self, key: &BlobKey) -> io::Result<SystemTime> {
        self.0.modified(key)
    }
    fn delete(&self, key: &BlobKey) -> io::Result<()> {
        self.0.delete(key)
    }
    fn delete_prefix(&self, prefix: &BlobKey) -> io::Result<()> {
        self.0.delete_prefix(prefix)
    }
    fn list(&self, prefix: &BlobKey) -> io::Result<Vec<BlobEntry>> {
        self.0.list(prefix)
    }
    fn prune(&self, prefix: &BlobKey) {
        self.0.prune(prefix);
    }
    fn remove_stale_temps(&self, prefix: &BlobKey, cutoff: SystemTime) -> TempSweep {
        self.0.remove_stale_temps(prefix, cutoff)
    }
}

fn store(cap: u64) -> (tempfile::TempDir, ScriptOutputStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = RenameGate(LocalBlobStorage::new(dir.path().join(SCRIPT_OUTPUT_DIR)));
    let store = ScriptOutputStore::with_storage(Arc::new(storage), cap);
    (dir, store)
}

fn read_all(store: &ScriptOutputStore, ws: &str, script: &str, run: &str) -> String {
    fs::read_to_string(store.run_path(ws, script, run).unwrap()).unwrap()
}

fn backdate(path: &Path, age: Duration) {
    let f = OpenOptions::new().write(true).open(path).unwrap();
    f.set_modified(SystemTime::now() - age).unwrap();
}

#[test]
fn layout_is_root_workspace_script_run_log() {
    let (dir, store) = store(1024);
    let path = store.run_path("ws-1", "script_a", "run-1").unwrap();
    assert_eq!(
        path,
        dir.path()
            .join("script-output")
            .join("ws-1")
            .join("script_a")
            .join("run-1.log")
    );
}

#[test]
fn append_then_read_sees_every_byte_without_closing_writer() {
    let (_dir, store) = store(1024);
    let mut w = store.open_writer("ws", "s", "r");
    w.append(b"hello\n");
    w.append(b"");
    w.append(b"world");
    assert!(w.is_enabled());
    assert_eq!(w.len(), 11);
    assert_eq!(read_all(&store, "ws", "s", "r"), "hello\nworld");
    assert!(store.exists("ws", "s", "r").unwrap());
}

#[test]
fn reopening_a_run_appends_to_its_log() {
    let (_dir, store) = store(1024);
    store.open_writer("ws", "s", "r").append(b"one\n");
    let mut w = store.open_writer("ws", "s", "r");
    assert_eq!(w.len(), 4);
    w.append(b"two\n");
    assert_eq!(read_all(&store, "ws", "s", "r"), "one\ntwo\n");
}

#[test]
fn cap_drops_oldest_bytes_and_never_exceeds_limit() {
    let cap = 100;
    let (_dir, store) = store(cap);
    let mut w = store.open_writer("ws", "s", "r");
    for i in 0..200 {
        w.append(format!("line {i:03}\n").as_bytes());
        let on_disk = fs::metadata(w.path()).unwrap().len();
        assert!(on_disk <= cap, "log grew to {on_disk} bytes");
        assert_eq!(on_disk, w.len());
    }
    let text = read_all(&store, "ws", "s", "r");
    assert!(text.ends_with("line 199\n"), "newest bytes kept: {text:?}");
    assert!(!text.contains("line 000"), "oldest bytes dropped");
    assert!(
        text.starts_with("line "),
        "compaction realigns to a line start: {text:?}"
    );
}

#[test]
fn chunk_larger_than_cap_keeps_its_newest_bytes() {
    let (_dir, store) = store(10);
    let mut w = store.open_writer("ws", "s", "r");
    w.append(b"abc");
    w.append(b"0123456789ABCDEF");
    assert_eq!(read_all(&store, "ws", "s", "r"), "6789ABCDEF");
    w.append(b"xy");
    assert_eq!(read_all(&store, "ws", "s", "r"), "BCDEFxy");
}

#[test]
fn compaction_keeps_mid_line_window_when_no_newline_exists() {
    let (_dir, store) = store(8);
    let mut w = store.open_writer("ws", "s", "r");
    w.append(b"abcdef");
    w.append(b"ghi");
    assert_eq!(read_all(&store, "ws", "s", "r"), "cdefghi");
}

#[test]
fn disk_error_disables_writer_without_panicking() {
    let (dir, store) = store(1024);
    // A file where the workspace folder should be makes the open fail.
    fs::create_dir_all(dir.path().join("script-output")).unwrap();
    fs::write(dir.path().join("script-output").join("ws"), b"not a dir").unwrap();
    let mut w = store.open_writer("ws", "s", "r");
    assert!(!w.is_enabled());
    w.append(b"still streaming");
    assert_eq!(w.len(), 0);
}

#[test]
fn invalid_id_yields_disabled_writer() {
    let (_dir, store) = store(1024);
    let mut w = store.open_writer("", "s", "r");
    assert!(!w.is_enabled());
    w.append(b"x");
}

#[test]
fn tail_matches_legacy_last_n_lines_model() {
    let (_dir, store) = store(1 << 20);
    let mut w = store.open_writer("ws", "s", "r");
    let body = (0..1000).fold(String::new(), |mut s, i| {
        let _ = writeln!(s, "row {i}");
        s
    });
    w.append(body.as_bytes());
    let tail = store.tail("ws", "s", "r", 3).unwrap().unwrap();
    assert_eq!(tail.text, "row 998\nrow 999\n");
    assert_eq!(tail.total_lines, body.split('\n').count());
    let all = store.tail("ws", "s", "r", 5000).unwrap().unwrap();
    assert_eq!(all.text, body);
}

#[test]
fn tail_spanning_multiple_scan_chunks() {
    let (_dir, store) = store(1 << 22);
    let mut w = store.open_writer("ws", "s", "r");
    let long = "x".repeat(SCAN_CHUNK + 17);
    w.append(format!("first\n{long}\nlast").as_bytes());
    let tail = store.tail("ws", "s", "r", 2).unwrap().unwrap();
    assert_eq!(tail.text, format!("{long}\nlast"));
    assert_eq!(tail.total_lines, 3);
}

#[test]
fn tail_of_missing_and_empty_logs() {
    let (_dir, store) = store(1024);
    assert_eq!(store.tail("ws", "s", "nope", 10).unwrap(), None);
    let _w = store.open_writer("ws", "s", "empty");
    let tail = store.tail("ws", "s", "empty", 10).unwrap().unwrap();
    assert_eq!(
        tail,
        TailText {
            text: String::new(),
            total_lines: 1
        }
    );
}

#[test]
fn paginate_matches_in_memory_helper_across_pages() {
    let (_dir, store) = store(1 << 20);
    let cases = [
        String::new(),
        "only".to_string(),
        "a\nb\n\n  \n".to_string(),
        (0..137).fold(String::new(), |mut s, i| {
            let _ = writeln!(s, "line {i}");
            s
        }),
        "x\n\ny\n".to_string(),
    ];
    for (n, body) in cases.iter().enumerate() {
        let run = format!("run{n}");
        store.open_writer("ws", "s", &run).append(body.as_bytes());
        for limit in [None, Some(1), Some(7), Some(50), Some(500)] {
            let mut token: Option<String> = None;
            loop {
                let got = store
                    .paginate("ws", "s", &run, limit, token.as_deref())
                    .unwrap();
                let want = paginate_text_lines(body, limit, token.as_deref());
                assert_eq!(got, want, "body {n} limit {limit:?} token {token:?}");
                match got["nextToken"].as_str() {
                    Some(t) => token = Some(t.to_owned()),
                    None => break,
                }
            }
        }
    }
}

#[test]
fn paginate_missing_log_is_empty_page() {
    let (_dir, store) = store(1024);
    let page = store.paginate("ws", "s", "r", None, None).unwrap();
    assert_eq!(page["items"], Value::Array(vec![]));
    assert_eq!(page["nextToken"], Value::Null);
}

#[test]
fn path_escape_ids_stay_inside_root() {
    let (_dir, store) = store(1024);
    for id in [
        "..",
        ".",
        "../../etc",
        "a/b",
        "a\\b",
        "/abs",
        ".hidden",
        "spaced id",
        "ünï",
    ] {
        let path = store.run_path(id, id, id).unwrap();
        assert!(
            path.starts_with(store.root()),
            "{id:?} -> {}",
            path.display()
        );
        let rel = path.strip_prefix(store.root()).unwrap();
        assert_eq!(rel.components().count(), 3, "{id:?} -> {}", rel.display());
        for c in rel.components() {
            assert!(
                matches!(c, std::path::Component::Normal(_)),
                "{id:?} -> {c:?}"
            );
        }
        assert_eq!(
            decode_segment(&encode_segment(id).unwrap()).as_deref(),
            Some(id)
        );
    }
    let mut w = store.open_writer("../x", "..", "../../r");
    w.append(b"contained");
    assert!(w.path().starts_with(store.root()));
}

#[test]
fn rejects_empty_nul_and_overlong_ids() {
    let (_dir, store) = store(1024);
    assert!(matches!(
        store.run_path("", "s", "r"),
        Err(Error::InvalidParams(_))
    ));
    assert!(matches!(
        store.run_path("ws", "", "r"),
        Err(Error::InvalidParams(_))
    ));
    assert!(matches!(
        store.run_path("ws", "s", ""),
        Err(Error::InvalidParams(_))
    ));
    assert!(matches!(
        store.run_path("w\0s", "s", "r"),
        Err(Error::InvalidParams(_))
    ));
    assert!(matches!(
        store.run_path("ws", "s", &"r".repeat(300)),
        Err(Error::InvalidParams(_))
    ));
    assert!(store.delete_workspace("").is_err());
}

#[test]
fn decode_rejects_names_encode_cannot_produce() {
    for name in ["", "a.b", "%2e", "%41", "%zz", "%2", "a/b"] {
        assert_eq!(decode_segment(name), None, "{name:?}");
    }
}

#[test]
fn delete_run_script_and_workspace_scopes() {
    let (_dir, store) = store(1024);
    for (ws, script, run) in [
        ("w1", "s1", "r1"),
        ("w1", "s1", "r2"),
        ("w1", "s2", "r1"),
        ("w2", "s1", "r1"),
    ] {
        store.open_writer(ws, script, run).append(b"x");
    }
    store.delete_run("w1", "s1", "r1").unwrap();
    assert!(!store.exists("w1", "s1", "r1").unwrap());
    assert!(store.exists("w1", "s1", "r2").unwrap());

    store.delete_script("w1", "s1").unwrap();
    assert!(!store.script_dir("w1", "s1").unwrap().exists());
    assert!(store.exists("w1", "s2", "r1").unwrap());

    store.delete_workspace("w1").unwrap();
    assert!(!store.workspace_dir("w1").unwrap().exists());
    assert!(store.exists("w2", "s1", "r1").unwrap());

    store.delete_run("w9", "s", "r").unwrap();
    store.delete_script("w9", "s").unwrap();
    store.delete_workspace("w9").unwrap();
}

#[test]
fn sweep_deletes_expired_keeps_live_and_fresh() {
    let (_dir, store) = store(1024);
    let day = Duration::from_secs(24 * 3600);
    let runs = [
        ("w1", "s1", "old"),
        ("w1", "s1", "live"),
        ("w1", "s1", "fresh"),
        ("w2", "s.x", "old"),
    ];
    for (ws, script, run) in runs {
        store.open_writer(ws, script, run).append(b"x\n");
    }
    for (ws, script, run) in [runs[0], runs[1], runs[3]] {
        backdate(&store.run_path(ws, script, run).unwrap(), 10 * day);
    }
    fs::write(
        store.script_dir("w1", "s1").unwrap().join("notes.txt"),
        b"foreign",
    )
    .unwrap();
    backdate(
        &store.script_dir("w1", "s1").unwrap().join("notes.txt"),
        10 * day,
    );

    let report = store.sweep(7 * day, |key| key.run == "live");
    assert_eq!(
        report,
        SweepReport {
            deleted: 2,
            skipped_live: 1,
            failed: 0,
            stale_temps_deleted: 0,
        }
    );
    assert!(!store.exists("w1", "s1", "old").unwrap());
    assert!(store.exists("w1", "s1", "live").unwrap());
    assert!(store.exists("w1", "s1", "fresh").unwrap());
    assert!(store
        .script_dir("w1", "s1")
        .unwrap()
        .join("notes.txt")
        .exists());
    assert!(
        !store.workspace_dir("w2").unwrap().exists(),
        "emptied folders pruned"
    );
}

#[test]
fn sweep_passes_decoded_ids_to_live_check() {
    let (_dir, store) = store(1024);
    store.open_writer("ws/1", "a b", "r.1").append(b"x");
    backdate(
        &store.run_path("ws/1", "a b", "r.1").unwrap(),
        Duration::from_secs(7200),
    );
    let mut seen = Vec::new();
    let seen_ref = std::cell::RefCell::new(&mut seen);
    store.sweep(Duration::from_secs(3600), |key| {
        seen_ref.borrow_mut().push(key.clone());
        true
    });
    assert_eq!(
        seen,
        vec![RunKey {
            workspace: "ws/1".into(),
            script: "a b".into(),
            run: "r.1".into()
        }]
    );
}

#[test]
fn zero_retention_disables_sweep() {
    let (_dir, store) = store(1024);
    store.open_writer("ws", "s", "r").append(b"x");
    backdate(
        &store.run_path("ws", "s", "r").unwrap(),
        Duration::from_secs(1 << 30),
    );
    assert_eq!(
        store.sweep(Duration::ZERO, |_| false),
        SweepReport::default()
    );
    assert!(store.exists("ws", "s", "r").unwrap());
}

#[test]
fn sweep_on_missing_root_is_a_noop() {
    let (_dir, store) = store(1024);
    assert_eq!(
        store.sweep(Duration::from_secs(1), |_| false),
        SweepReport::default()
    );
}

#[test]
fn cap_is_clamped_and_adjustable() {
    let (_dir, mut store) = store(0);
    assert_eq!(store.max_bytes_per_run(), 1);
    store.set_max_bytes_per_run(4);
    let mut w = store.open_writer("ws", "s", "r");
    w.append(b"abcdefgh");
    assert_eq!(read_all(&store, "ws", "s", "r"), "efgh");
}

#[test]
fn paginate_never_buffers_a_huge_line_it_does_not_return() {
    let (_dir, store) = store(1 << 26);
    let mut body = vec![b'x'; 8 * 1024 * 1024];
    body.extend_from_slice(b"\nsmall\n");
    store.open_writer("ws", "s", "r").append(&body);
    let file = File::open(store.run_path("ws", "s", "r").unwrap()).unwrap();
    let (page, peak) = paginate_file(Box::new(file), Some(1), None).unwrap();
    assert_eq!(page["items"], serde_json::json!(["small"]));
    assert!(
        peak <= SCAN_CHUNK,
        "buffered {peak} bytes for an unreturned line"
    );
}

#[cfg(unix)]
#[test]
fn symlinked_folders_below_root_are_never_followed() {
    let (dir, store) = store(1024);
    let outside = dir.path().join("outside");
    fs::create_dir_all(&outside).unwrap();
    fs::create_dir_all(store.root()).unwrap();
    std::os::unix::fs::symlink(&outside, store.root().join("linked")).unwrap();

    let mut w = store.open_writer("linked", "s", "r");
    w.append(b"escaped");
    assert!(
        !w.is_enabled(),
        "writer through a symlinked workspace folder"
    );
    assert!(
        !outside.join("s").exists(),
        "nothing created outside the root"
    );

    fs::create_dir_all(store.workspace_dir("ws").unwrap()).unwrap();
    std::os::unix::fs::symlink(&outside, store.script_dir("ws", "s").unwrap()).unwrap();
    let mut w = store.open_writer("ws", "s", "r");
    w.append(b"escaped");
    assert!(!w.is_enabled(), "writer through a symlinked script folder");
    assert!(!outside.join("r.log").exists());

    fs::write(outside.join("r.log"), b"secret\n").unwrap();
    assert!(
        store.tail("ws", "s", "r", 10).is_err(),
        "read through a symlinked folder"
    );
    assert!(store.paginate("ws", "s", "r", None, None).is_err());
}

#[cfg(unix)]
#[test]
fn symlinked_log_file_is_never_followed() {
    let (dir, store) = store(1024);
    let target = dir.path().join("target.txt");
    fs::write(&target, b"secret\n").unwrap();
    let path = store.run_path("ws", "s", "r").unwrap();
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::os::unix::fs::symlink(&target, &path).unwrap();

    let mut w = store.open_writer("ws", "s", "r");
    w.append(b"escaped");
    assert!(!w.is_enabled());
    assert_eq!(fs::read(&target).unwrap(), b"secret\n");
    assert!(store.tail("ws", "s", "r", 10).is_err());
    assert!(store.paginate("ws", "s", "r", None, None).is_err());
}

#[test]
fn reopen_under_a_smaller_cap_truncates_to_newest_bytes() {
    let (_dir, mut store) = store(131_072);
    let body = (0..30_000).fold(String::new(), |mut s, i| {
        let _ = writeln!(s, "{i:05}");
        s
    });
    let body = &body.as_bytes()[body.len() - 131_072..];
    store.open_writer("ws", "s", "r").append(body);
    store.set_max_bytes_per_run(65_536);
    let w = store.open_writer("ws", "s", "r");
    let on_disk = fs::metadata(w.path()).unwrap().len();
    assert!(on_disk <= 65_536, "reopened log is {on_disk} bytes");
    assert_eq!(on_disk, w.len());
    let text = read_all(&store, "ws", "s", "r");
    assert!(text.ends_with("29999\n"), "newest bytes kept");
    assert!(text.len() > 60_000, "kept close to the cap: {}", text.len());
}

#[test]
fn paginate_blank_trim_matches_lossy_trim_for_unicode_and_invalid_bytes() {
    let (_dir, store) = store(1 << 22);
    let mut straddle = vec![b'a'; SCAN_CHUNK - 1];
    straddle.extend_from_slice("\u{00A0}\n".as_bytes());
    straddle.extend_from_slice(&vec![b' '; SCAN_CHUNK + 3]);
    straddle.extend_from_slice("\n\u{3000}\u{00A0}\n".as_bytes());
    let mut long_blank = b"x\n".to_vec();
    long_blank.extend_from_slice(&vec![b'\t'; SCAN_CHUNK * 2 + 1]);
    long_blank.push(b'\n');
    let cases: Vec<Vec<u8>> = vec![
        "a\n\u{00A0}\n\u{2003}\u{3000}\n".into(),
        "a\n\u{0085}\n".into(),
        b"a\n\xff\n \n".to_vec(),
        b"a\n\xe3\x80\n".to_vec(),
        b"a\n \xc2".to_vec(),
        b"a\n\xc0\xa0\n".to_vec(),
        b"a\n\xed\xa0\x80\n".to_vec(),
        "\u{1F600}\n\u{FEFF}\n".into(),
        b"a\r\n\r\n\x0b\x0c\n".to_vec(),
        straddle,
        long_blank,
    ];
    for (n, body) in cases.iter().enumerate() {
        let run = format!("u{n}");
        store.open_writer("ws", "s", &run).append(body);
        let text = String::from_utf8_lossy(body);
        for limit in [Some(1), Some(2), None] {
            let got = store.paginate("ws", "s", &run, limit, None).unwrap();
            assert_eq!(
                got,
                paginate_text_lines(&text, limit, None),
                "case {n} limit {limit:?}"
            );
        }
    }
}

#[cfg(unix)]
#[test]
fn delete_never_reaches_through_a_symlinked_folder() {
    let (dir, store) = store(1024);
    let outside = dir.path().join("outside");
    fs::create_dir_all(outside.join("s")).unwrap();
    fs::write(outside.join("s").join("r.log"), b"keep").unwrap();
    fs::create_dir_all(store.root()).unwrap();
    std::os::unix::fs::symlink(&outside, store.root().join("linked")).unwrap();
    assert!(store.delete_run("linked", "s", "r").is_err());
    assert!(store.delete_script("linked", "s").is_err());
    store.delete_workspace("linked").unwrap();
    assert!(
        !store.root().join("linked").exists(),
        "the link itself is removed"
    );
    assert_eq!(fs::read(outside.join("s").join("r.log")).unwrap(), b"keep");
}

#[test]
fn workspace_ids_lists_decoded_workspace_folders_only() {
    let (_dir, store) = store(1024);
    assert!(store.workspace_ids().is_empty(), "no root yet");
    store.open_writer("ws-1", "s", "r").append(b"x");
    store.open_writer("ws/odd.id", "s", "r").append(b"x");
    fs::write(store.root().join("stray-file"), b"x").unwrap();
    let mut ids = store.workspace_ids();
    ids.sort();
    assert_eq!(ids, vec!["ws-1".to_string(), "ws/odd.id".to_string()]);
}

/// Hammer `tail` and `paginate` from another thread while `mutate` keeps
/// compacting the log. The log is never empty after the first fill, so every
/// read must succeed with some content: no `UnexpectedEof` from a file that
/// shrank mid-read and no spurious empty result.
fn assert_reads_survive_compaction(
    store: &ScriptOutputStore,
    mutate: impl FnOnce(&std::sync::atomic::AtomicBool) + Send,
) {
    use std::sync::atomic::{AtomicBool, Ordering};
    let done = AtomicBool::new(false);
    let (reads, failures) = std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let mut reads = 0usize;
            let mut failures = Vec::new();
            while !done.load(Ordering::Acquire) || reads < 200 {
                reads += 1;
                match store.tail("ws", "s", "r", 100) {
                    Ok(Some(t)) if !t.text.trim().is_empty() => {}
                    other => failures.push(format!("tail: {other:?}")),
                }
                match store.paginate("ws", "s", "r", Some(50), None) {
                    Ok(page) if page["items"].as_array().is_some_and(|i| !i.is_empty()) => {}
                    other => failures.push(format!("paginate: {other:?}")),
                }
            }
            (reads, failures)
        });
        mutate(&done);
        done.store(true, Ordering::Release);
        reader.join().unwrap()
    });
    assert!(
        failures.is_empty(),
        "{} of {reads} live reads failed during compaction; first: {}",
        failures.len(),
        failures[0]
    );
}

#[test]
fn live_reads_never_see_append_compaction_mid_rewrite() {
    let (_dir, store) = store(64 * 1024);
    let line = format!("{}\n", "x".repeat(63));
    let mut w = store.open_writer("ws", "s", "r");
    let chunk = line.repeat(64);
    for _ in 0..16 {
        w.append(chunk.as_bytes());
    }
    assert_reads_survive_compaction(&store, |_| {
        // Each 4 KiB chunk past the cap compacts to the newest half.
        for _ in 0..20_000 {
            w.append(chunk.as_bytes());
        }
    });
    assert!(w.is_enabled());
    assert!(w.len() <= 64 * 1024);
    assert_eq!(fs::metadata(w.path()).unwrap().len(), w.len());
}

#[test]
fn live_reads_never_see_reopen_compaction_mid_rewrite() {
    let (dir, small) = store(32 * 1024);
    let big = ScriptOutputStore::new(dir.path(), 1 << 20);
    let line = format!("{}\n", "y".repeat(63));
    let chunk = line.repeat(1024);
    big.open_writer("ws", "s", "r").append(chunk.as_bytes());
    assert_reads_survive_compaction(&small, |_| {
        for _ in 0..1_000 {
            // Regrow past the small cap, then reopen under it (compacts).
            big.open_writer("ws", "s", "r").append(chunk.as_bytes());
            let w = small.open_writer("ws", "s", "r");
            assert!(w.is_enabled());
            assert!(w.len() <= 32 * 1024);
        }
    });
}

#[test]
fn compaction_leaves_no_temp_files_behind() {
    let (_dir, store) = store(1024);
    let mut w = store.open_writer("ws", "s", "r");
    for i in 0..200 {
        w.append(format!("line {i}\n").as_bytes());
    }
    let dir = store.script_dir("ws", "s").unwrap();
    let names: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(names, vec!["r.log".to_string()]);
    assert!(read_all(&store, "ws", "s", "r").ends_with("line 199\n"));
}

fn compact_temps(store: &ScriptOutputStore) -> Vec<String> {
    let mut names: Vec<_> = fs::read_dir(store.script_dir("ws", "s").unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.ends_with(COMPACT_EXT))
        .collect();
    names.sort();
    names
}

/// Run `f` with every compaction rename failing, like Windows refusing to
/// replace a log another process holds open without `FILE_SHARE_DELETE`.
fn with_failing_rename<T>(f: impl FnOnce() -> T) -> T {
    FAIL_RENAME.with(|c| c.set(true));
    let out = f();
    FAIL_RENAME.with(|c| c.set(false));
    out
}

#[test]
fn failed_compaction_rename_keeps_every_byte_and_retries_later() {
    let (_dir, store) = store(1000);
    let mut w = store.open_writer("ws", "s", "r");
    let lines: Vec<String> = (0..40)
        .map(|i| format!("line {i:04} {}\n", "z".repeat(28)))
        .collect();
    let mut expected = String::new();
    with_failing_rename(|| {
        // 40 * 40 bytes = 1600 < 2 * cap: nothing is dropped while renames fail.
        for line in &lines {
            w.append(line.as_bytes());
            expected.push_str(line);
        }
    });
    assert!(w.is_enabled());
    assert_eq!(read_all(&store, "ws", "s", "r"), expected);
    assert_eq!(w.len(), expected.len() as u64);
    assert!(compact_temps(&store).is_empty());

    // Renames work again: the next append past the retry point compacts.
    for line in &lines {
        w.append(line.as_bytes());
    }
    assert!(w.is_enabled());
    assert!(w.len() <= 1000, "len {}", w.len());
    let text = read_all(&store, "ws", "s", "r");
    assert_eq!(text.len() as u64, w.len());
    assert!(text.ends_with(&lines[39]));
    assert!(compact_temps(&store).is_empty());
}

fn numbered_line(i: usize) -> String {
    format!("line {i:04} {}\n", "q".repeat(29))
}

/// Append 500 40-byte lines to a 1000-byte-cap log while compaction keeps
/// failing, with a reader holding the log open from the start. The log must
/// never be truncated or rewritten in place: the reader's handle only ever
/// grows, stays the same file as the path, and always reads back an exact
/// prefix of the output; the log never passes twice the cap.
fn assert_failed_compaction_never_truncates(
    store: &ScriptOutputStore,
    mut append_all: impl FnMut(&mut dyn FnMut(&[u8])),
) {
    let mut w = store.open_writer("ws", "s", "r");
    w.append(numbered_line(0).as_bytes());
    let path = w.path().to_path_buf();
    let mut reader = File::open(&path).unwrap();
    let mut last_len = 0u64;
    let mut check = |w: &RunOutputWriter| {
        let len = reader.metadata().unwrap().len();
        assert!(
            len >= last_len,
            "log shrank from {last_len} to {len} under a reader"
        );
        assert!(len <= 2000, "log grew to {len}, past twice the cap");
        assert_eq!(len, w.len());
        last_len = len;
        let mut text = String::new();
        reader.seek(SeekFrom::Start(0)).unwrap();
        (&reader).read_to_string(&mut text).unwrap();
        let expected: String = (0..text.len() / 40).map(numbered_line).collect();
        assert_eq!(text, expected, "the log is an exact prefix of the output");
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            assert_eq!(
                fs::metadata(&path).unwrap().ino(),
                reader.metadata().unwrap().ino()
            );
        }
    };
    append_all(&mut |chunk| {
        w.append(chunk);
        check(&w);
    });
    assert!(w.is_enabled());
    assert_eq!(w.len(), 2000, "appends continue right up to twice the cap");
    assert!(compact_temps(store).is_empty());
}

#[test]
fn persistently_failing_compaction_never_truncates_and_stops_at_twice_the_cap() {
    let (_dir, store) = store(1000);
    with_failing_rename(|| {
        assert_failed_compaction_never_truncates(&store, |append| {
            for i in 1..500 {
                append(numbered_line(i).as_bytes());
            }
        });
    });
}

/// A real replacement failure: the run folder is read-only, so the temp file
/// cannot be created while the log itself stays writable.
#[cfg(unix)]
#[test]
fn uncreatable_compaction_temp_never_truncates_the_log() {
    use std::os::unix::fs::PermissionsExt as _;
    let (_dir, store) = store(1000);
    store.open_writer("ws", "s", "r");
    let dir = store.script_dir("ws", "s").unwrap();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).unwrap();
    let probe = dir.join("probe");
    if fs::write(&probe, b"").is_ok() {
        // Running as root: permissions are not enforced, so nothing to test.
        let _ = fs::remove_file(&probe);
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
        return;
    }
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
    let _ = fs::remove_file(store.run_path("ws", "s", "r").unwrap());
    let dir_for_lock = dir.clone();
    assert_failed_compaction_never_truncates(&store, move |append| {
        fs::set_permissions(&dir_for_lock, fs::Permissions::from_mode(0o555)).unwrap();
        for i in 1..500 {
            append(numbered_line(i).as_bytes());
        }
        fs::set_permissions(&dir_for_lock, fs::Permissions::from_mode(0o755)).unwrap();
    });
}

#[test]
fn compaction_recovers_after_dropping_output_past_twice_the_cap() {
    let (_dir, store) = store(1000);
    let mut w = store.open_writer("ws", "s", "r");
    with_failing_rename(|| {
        for i in 0..200 {
            w.append(numbered_line(i).as_bytes());
        }
    });
    assert_eq!(w.len(), 2000);
    // Renames work again: within a quarter cap the next attempt compacts.
    for i in 200..210 {
        w.append(numbered_line(i).as_bytes());
    }
    assert!(w.len() <= 1000, "len {}", w.len());
    let text = read_all(&store, "ws", "s", "r");
    assert_eq!(text.len() as u64, w.len());
    assert!(text.ends_with(&numbered_line(209)));
    assert!(compact_temps(&store).is_empty());
}

#[test]
fn failed_reopen_compaction_keeps_the_oversized_log() {
    let (dir, small) = store(100);
    let big = ScriptOutputStore::new(dir.path(), 1 << 20);
    let body = (0..20).fold(String::new(), |mut body, i| {
        let _ = writeln!(body, "line {i}");
        body
    });
    big.open_writer("ws", "s", "r").append(body.as_bytes());
    let mut w = with_failing_rename(|| small.open_writer("ws", "s", "r"));
    assert!(w.is_enabled());
    assert_eq!(w.len(), body.len() as u64);
    assert_eq!(read_all(&small, "ws", "s", "r"), body);
    // The next append compacts once renames work.
    w.append(b"next\n");
    assert!(w.len() <= 100);
    assert!(read_all(&small, "ws", "s", "r").ends_with("line 19\nnext\n"));
}

#[test]
fn sweep_deletes_stale_compaction_temps_even_with_retention_disabled() {
    let (_dir, store) = store(1024);
    store.open_writer("ws", "s", "r").append(b"keep\n");
    let dir = store.script_dir("ws", "s").unwrap();
    let stale = dir.join("r.log.1-0.compact");
    let fresh = dir.join("r.log.1-1.compact");
    fs::write(&stale, b"old").unwrap();
    fs::write(&fresh, b"new").unwrap();
    backdate(&stale, STALE_COMPACT_AGE + Duration::from_secs(60));
    #[cfg(unix)]
    {
        let link = dir.join("link.compact");
        std::os::unix::fs::symlink(&stale, &link).unwrap();
    }

    let report = store.sweep(Duration::ZERO, |_| false);
    assert_eq!(report.stale_temps_deleted, 1, "{report:?}");
    assert_eq!(report.deleted, 0);
    assert!(!stale.exists());
    assert!(fresh.exists(), "an in-flight compaction temp is kept");
    assert!(store.exists("ws", "s", "r").unwrap());

    backdate(&fresh, STALE_COMPACT_AGE + Duration::from_secs(60));
    let report = store.sweep(Duration::from_secs(3600), |_| false);
    assert_eq!(report.stale_temps_deleted, 1, "{report:?}");
    assert_eq!(report.deleted, 0);
    assert!(!fresh.exists());
    assert!(store.exists("ws", "s", "r").unwrap());
    #[cfg(unix)]
    assert!(
        fs::symlink_metadata(dir.join("link.compact")).is_ok(),
        "symlinks are left alone"
    );
}

#[test]
fn paginate_budgets_bytes_and_rebuilds_an_oversized_escape_heavy_line() {
    let (_dir, store) = store(1 << 26);
    let quotes = format!("{}é{}", "\"".repeat(400_000), "\\".repeat(200_000));
    let wide = "y".repeat(200_000);
    let body = format!("old\n{wide}\n{wide}\n{wide}\n{quotes}\n");
    store.open_writer("ws", "s", "r").append(body.as_bytes());
    let mut newest_first: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    let mut continues = false;
    loop {
        let got = store
            .paginate("ws", "s", "r", Some(50), token.as_deref())
            .unwrap();
        assert_eq!(got, paginate_text_lines(&body, Some(50), token.as_deref()));
        let size = serde_json::to_vec(&got).unwrap().len();
        assert!(size <= 480_100, "page is {size} bytes");
        for (n, item) in got["items"].as_array().unwrap().iter().enumerate() {
            let item = item.as_str().unwrap();
            if n == 0 && continues {
                let last = newest_first.last_mut().unwrap();
                *last = format!("{item}{last}");
            } else {
                newest_first.push(item.to_owned());
            }
        }
        continues = got["lineContinues"] == true;
        match got["nextToken"].as_str() {
            Some(t) => token = Some(t.to_owned()),
            None => break,
        }
    }
    newest_first.reverse();
    assert_eq!(
        newest_first,
        vec!["old".to_owned(), wide.clone(), wide.clone(), wide, quotes]
    );
}

/// The first `n` numbered lines: stream position `p` holds byte `p` of this.
fn numbered_stream(n: usize) -> Vec<u8> {
    (0..n).map(numbered_line).collect::<String>().into_bytes()
}

/// Every cursor read is an exact slice of the run's output stream.
fn assert_cursor_slice(tail: &CursorTail, stream: &[u8]) {
    let (start, end) = (
        usize::try_from(tail.start).unwrap(),
        usize::try_from(tail.end).unwrap(),
    );
    assert!(tail.log_start <= tail.start && tail.start <= tail.end);
    assert_eq!(tail.bytes.len(), end - start, "range length {tail:?}");
    assert_eq!(
        tail.bytes,
        &stream[start..end],
        "bytes sit at their positions"
    );
}

fn cursor(store: &ScriptOutputStore, lines: usize) -> CursorTail {
    store
        .cursor_tail("ws", "s", "r", lines, u64::MAX)
        .unwrap()
        .expect("log exists")
}

#[test]
fn cursor_positions_follow_chunk_boundaries() {
    let (_dir, store) = store(1 << 20);
    let mut w = store.open_writer("ws", "s", "r");
    assert_eq!(w.end(), 0);
    let stream = numbered_stream(10);
    for chunk in stream.chunks(7) {
        let before = w.end();
        w.append(chunk);
        assert_eq!(w.end(), before + chunk.len() as u64);
    }
    let all = cursor(&store, 10_000);
    assert_eq!((all.start, all.end, all.log_start), (0, 400, 0));
    assert_eq!(all.total_lines, 11);
    assert_cursor_slice(&all, &stream);
    let last_two = cursor(&store, 3);
    assert_eq!(
        last_two.start, 320,
        "the newest lines start at their position"
    );
    assert_cursor_slice(&last_two, &stream);
}

#[test]
fn cursor_positions_survive_cap_compaction() {
    let (_dir, store) = store(1000);
    let mut w = store.open_writer("ws", "s", "r");
    let stream = numbered_stream(200);
    for line in stream.chunks(40) {
        w.append(line);
    }
    assert_eq!(w.end(), 8000, "the count never drops with the oldest bytes");
    let tail = cursor(&store, 10_000);
    assert_eq!(tail.end, 8000);
    assert!(tail.log_start > 0, "the cap dropped older output");
    assert_eq!(tail.start, tail.log_start);
    assert_eq!(tail.end - tail.log_start, w.len());
    assert_cursor_slice(&tail, &stream);
}

#[test]
fn reopening_a_run_continues_its_positions() {
    let (_dir, store) = store(1000);
    let stream = numbered_stream(120);
    let mut lines = stream.chunks(40);
    {
        let mut first = store.open_writer("ws", "s", "r");
        for line in lines.by_ref().take(60) {
            first.append(line);
        }
    }
    let mut second = store.open_writer("ws", "s", "r");
    assert_eq!(
        second.end(),
        2400,
        "a service auto-restart appends at the run's end"
    );
    for line in lines {
        second.append(line);
    }
    assert_eq!(second.end(), 4800);
    assert_cursor_slice(&cursor(&store, 10_000), &stream);
}

#[test]
fn reopening_under_a_smaller_cap_keeps_positions() {
    let (_dir, mut store) = store(4000);
    let stream = numbered_stream(100);
    store.open_writer("ws", "s", "r").append(&stream);
    store.set_max_bytes_per_run(1000);
    let w = store.open_writer("ws", "s", "r");
    assert_eq!(w.end(), 4000);
    let tail = cursor(&store, 10_000);
    assert!(
        tail.log_start >= 3000,
        "reopen compaction moved the log start"
    );
    assert_cursor_slice(&tail, &stream);
}

#[test]
fn writers_of_one_run_share_its_positions() {
    let (_dir, store) = store(1000);
    let stream = numbered_stream(80);
    let mut a = store.open_writer("ws", "s", "r");
    let mut b = store.open_writer("ws", "s", "r");
    for (i, line) in stream.chunks(40).enumerate() {
        if i % 2 == 0 {
            a.append(line)
        } else {
            b.append(line)
        }
    }
    assert_eq!((a.end(), b.end()), (3200, 3200));
    assert_cursor_slice(&cursor(&store, 10_000), &stream);
}

#[test]
fn unsaved_output_still_advances_positions() {
    let (_dir, store) = store(1000);
    let mut w = store.open_writer("ws", "s", "r");
    let stream = numbered_stream(200);
    with_failing_rename(|| {
        for line in stream.chunks(40).take(100) {
            w.append(line);
        }
    });
    assert_eq!(w.len(), 2000, "output past twice the cap is not saved");
    assert_eq!(w.end(), 4000, "but it still has positions");
    let tail = cursor(&store, 1);
    assert_eq!(tail.end, 4000);
    for line in stream.chunks(40).skip(100) {
        w.append(line);
    }
    let tail = cursor(&store, 5);
    assert_eq!(tail.end, 8000);
    assert_cursor_slice(&tail, &stream);
}

#[test]
fn a_disabled_writer_still_advances_positions() {
    let (dir, store) = store(1024);
    fs::create_dir_all(dir.path().join("script-output")).unwrap();
    fs::write(dir.path().join("script-output").join("ws"), b"not a dir").unwrap();
    let mut w = store.open_writer("ws", "s", "r");
    assert!(!w.is_enabled());
    w.append(b"streamed but not saved");
    assert_eq!(w.end(), 22);
}

#[test]
fn a_new_store_counts_an_existing_log_from_zero() {
    let (dir, store) = store(1000);
    let stream = numbered_stream(100);
    store.open_writer("ws", "s", "r").append(&stream);
    let next_boot = ScriptOutputStore::new(dir.path(), 1000);
    let tail = next_boot
        .cursor_tail("ws", "s", "r", 10_000, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(tail.log_start, 0);
    assert_eq!(tail.end, tail.bytes.len() as u64);
    assert_eq!(next_boot.open_writer("ws", "s", "r").end(), tail.end);
}

#[test]
fn cursor_byte_window_starts_on_a_line() {
    let (_dir, store) = store(1 << 20);
    let stream = numbered_stream(100);
    store.open_writer("ws", "s", "r").append(&stream);
    let tail = store
        .cursor_tail("ws", "s", "r", 10_000, 100)
        .unwrap()
        .unwrap();
    assert_eq!(tail.end, 4000);
    assert_eq!(tail.start, 3920, "the cut realigns after the next newline");
    assert_cursor_slice(&tail, &stream);
    let mut w = store.open_writer("ws", "s", "x");
    w.append(&[b'a'; 300]);
    let tail = store.cursor_tail("ws", "s", "x", 10, 100).unwrap().unwrap();
    assert_eq!(
        (tail.start, tail.end),
        (200, 300),
        "no newline: keep the window"
    );
}

#[test]
fn cursor_of_missing_and_empty_logs() {
    let (_dir, store) = store(1024);
    assert!(store
        .cursor_tail("ws", "s", "r", 10, u64::MAX)
        .unwrap()
        .is_none());
    let w = store.open_writer("ws", "s", "r");
    let tail = cursor(&store, 10);
    assert_eq!((tail.start, tail.end, tail.log_start), (0, 0, 0));
    assert!(tail.bytes.is_empty());
    drop(w);
}

/// A reader racing appends and compactions always gets bytes that sit
/// exactly at the positions it reports.
#[test]
fn cursor_reads_are_atomic_with_appends_and_compaction() {
    let (_dir, store) = store(2000);
    let stream = numbered_stream(3000);
    let writer_store = store.clone();
    let writer_stream = stream.clone();
    let writer = std::thread::spawn(move || {
        let mut w = writer_store.open_writer("ws", "s", "r");
        for line in writer_stream.chunks(40) {
            w.append(line);
        }
    });
    let mut reads = 0;
    while !writer.is_finished() || reads == 0 {
        if let Some(tail) = store.cursor_tail("ws", "s", "r", 30, u64::MAX).unwrap() {
            assert_cursor_slice(&tail, &stream);
            reads += 1;
        }
    }
    writer.join().unwrap();
    assert_cursor_slice(&cursor(&store, 10_000), &stream);
}
