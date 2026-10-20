use std::fs;
use std::time::{Duration, SystemTime};

use serde_json::Value;

use super::*;
use crate::pagination::paginate_text_lines;

fn store(cap: u64) -> (tempfile::TempDir, ScriptOutputStore) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = ScriptOutputStore::new(dir.path(), cap);
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
    let body: String = (0..1000).map(|i| format!("row {i}\n")).collect();
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
        (0..137).map(|i| format!("line {i}\n")).collect::<String>(),
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

    let report = store.sweep(7 * day, |key| key.run_id == "live");
    assert_eq!(
        report,
        SweepReport {
            deleted: 2,
            skipped_live: 1,
            failed: 0
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
            workspace_id: "ws/1".into(),
            script_id: "a b".into(),
            run_id: "r.1".into()
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
