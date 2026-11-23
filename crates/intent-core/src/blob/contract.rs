//! The [`BlobStorage`] contract, written once and run per backend. Each
//! check takes a fresh, empty storage.

use std::io::{Seek, SeekFrom, Write};
use std::time::{Duration, SystemTime};

use super::{BlobEntry, BlobFile, BlobKey, BlobKind, BlobStorage, LocalBlobStorage};

fn key(path: &str) -> BlobKey {
    path.split('/')
        .try_fold(BlobKey::root(), |key, segment| key.child(segment))
        .unwrap()
}

fn read_all(file: &mut dyn BlobFile) -> Vec<u8> {
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut out = Vec::new();
    file.read_to_end(&mut out).unwrap();
    out
}

fn read_key(storage: &dyn BlobStorage, key: &BlobKey) -> Vec<u8> {
    read_all(&mut *storage.open_read(key).unwrap().expect("blob exists"))
}

fn appends_are_visible_to_a_reader_while_the_writer_stays_open(storage: &dyn BlobStorage) {
    let k = key("a/b/run.log");
    let mut w = storage.open_append(&k).unwrap();
    w.write_all(b"one\n").unwrap();
    w.flush().unwrap();
    assert_eq!(read_key(storage, &k), b"one\n");
    w.write_all(b"two\n").unwrap();
    w.flush().unwrap();
    assert_eq!(read_key(storage, &k), b"one\ntwo\n");
    assert_eq!(w.len().unwrap(), 8);
    assert!(storage.exists(&k).unwrap());
}

fn reopening_to_append_keeps_what_is_there(storage: &dyn BlobStorage) {
    let k = key("a/run.log");
    storage
        .open_append(&k)
        .unwrap()
        .write_all(b"old\n")
        .unwrap();
    let mut w = storage.open_append(&k).unwrap();
    assert_eq!(w.len().unwrap(), 4);
    // Even after a seek, writes land at the end.
    w.seek(SeekFrom::Start(0)).unwrap();
    w.write_all(b"new\n").unwrap();
    w.flush().unwrap();
    assert_eq!(read_key(storage, &k), b"old\nnew\n");
}

fn replace_is_atomic_for_open_readers_and_returns_an_append_handle(storage: &dyn BlobStorage) {
    let k = key("a/run.log");
    storage
        .open_append(&k)
        .unwrap()
        .write_all(b"old old old\n")
        .unwrap();
    let mut before = storage.open_read(&k).unwrap().unwrap();
    let mut replaced = storage.replace(&k, b"new\n").unwrap();
    assert_eq!(read_all(&mut *before), b"old old old\n");
    assert_eq!(read_key(storage, &k), b"new\n");
    replaced.write_all(b"more\n").unwrap();
    replaced.flush().unwrap();
    assert_eq!(read_key(storage, &k), b"new\nmore\n");
    assert_eq!(replaced.len().unwrap(), 9);
}

fn a_missing_key_is_empty_not_an_error(storage: &dyn BlobStorage) {
    let k = key("nope/run.log");
    assert!(storage.open_read(&k).unwrap().is_none());
    assert!(!storage.exists(&k).unwrap());
    assert_eq!(
        storage.modified(&k).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    storage.delete(&k).unwrap();
    storage.delete_prefix(&key("nope")).unwrap();
    assert!(storage.list(&key("nope")).unwrap().is_empty());
    assert!(storage.list(&BlobKey::root()).unwrap().is_empty());
    storage.prune(&key("nope"));
}

fn list_names_direct_children_by_kind(storage: &dyn BlobStorage) {
    storage.open_append(&key("ws/s1/r1.log")).unwrap();
    storage.open_append(&key("ws/s2/r2.log")).unwrap();
    storage.open_append(&key("ws/top.log")).unwrap();
    let mut entries = storage.list(&key("ws")).unwrap();
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    let entry = |name: &str, kind| BlobEntry {
        name: name.into(),
        kind,
    };
    assert_eq!(
        entries,
        vec![
            entry("s1", BlobKind::Folder),
            entry("s2", BlobKind::Folder),
            entry("top.log", BlobKind::Blob),
        ]
    );
}

fn delete_and_delete_prefix_remove_what_they_name(storage: &dyn BlobStorage) {
    storage.open_append(&key("ws/s1/r1.log")).unwrap();
    storage.open_append(&key("ws/s1/r2.log")).unwrap();
    storage.open_append(&key("ws/s2/r3.log")).unwrap();
    storage.delete(&key("ws/s1/r1.log")).unwrap();
    assert!(!storage.exists(&key("ws/s1/r1.log")).unwrap());
    assert!(storage.exists(&key("ws/s1/r2.log")).unwrap());
    storage.delete_prefix(&key("ws/s1")).unwrap();
    assert!(!storage.exists(&key("ws/s1/r2.log")).unwrap());
    assert!(storage.list(&key("ws/s1")).unwrap().is_empty());
    assert!(storage.exists(&key("ws/s2/r3.log")).unwrap());
}

fn prune_forgets_only_an_empty_prefix(storage: &dyn BlobStorage) {
    storage.open_append(&key("ws/full/r.log")).unwrap();
    storage.open_append(&key("ws/empty/r.log")).unwrap();
    storage.delete(&key("ws/empty/r.log")).unwrap();
    storage.prune(&key("ws/full"));
    storage.prune(&key("ws/empty"));
    let names: Vec<_> = storage
        .list(&key("ws"))
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, vec!["full".to_string()]);
    assert!(storage.exists(&key("ws/full/r.log")).unwrap());
}

fn modified_moves_forward_on_write(storage: &dyn BlobStorage) {
    let k = key("a/run.log");
    let start = SystemTime::now() - Duration::from_secs(1);
    storage.open_append(&k).unwrap().write_all(b"x").unwrap();
    assert!(storage.modified(&k).unwrap() >= start);
}

fn the_stale_temp_sweep_never_touches_blobs(storage: &dyn BlobStorage) {
    let k = key("ws/s/run.log");
    storage.open_append(&k).unwrap().write_all(b"x\n").unwrap();
    storage.replace(&k, b"y\n").unwrap();
    storage.open_append(&key("ws/s/other")).unwrap();
    // Every blob is older than a cutoff in the future: none is a temp.
    let sweep =
        storage.remove_stale_temps(&key("ws/s"), SystemTime::now() + Duration::from_secs(3600));
    assert_eq!(sweep.failed, 0);
    assert_eq!(read_key(storage, &k), b"y\n");
    assert!(storage.exists(&key("ws/s/other")).unwrap());
}

/// Compaction reads the newest bytes back through the writer's own handle.
fn append_and_replace_handles_read_back_what_they_hold(storage: &dyn BlobStorage) {
    let k = key("a/run.log");
    let mut w = storage.open_append(&k).unwrap();
    w.write_all(b"one\ntwo\n").unwrap();
    w.flush().unwrap();
    assert_eq!(read_all(&mut *w), b"one\ntwo\n");
    let mut replaced = storage.replace(&k, b"two\n").unwrap();
    assert_eq!(read_all(&mut *replaced), b"two\n");
}

/// After a seek, writes through either handle still land at the end.
fn writes_after_a_seek_append(storage: &dyn BlobStorage) {
    let k = key("a/run.log");
    storage.open_append(&k).unwrap();
    let mut replaced = storage.replace(&k, b"head\n").unwrap();
    replaced.seek(SeekFrom::Start(0)).unwrap();
    replaced.write_all(b"tail\n").unwrap();
    replaced.flush().unwrap();
    assert_eq!(read_key(storage, &k), b"head\ntail\n");
}

/// One contract check over a fresh storage.
type Check = fn(&dyn BlobStorage);

/// Every contract check, each over a fresh storage from `fresh`.
fn run(fresh: impl Fn() -> (tempfile::TempDir, Box<dyn BlobStorage>)) {
    let checks: &[(&str, Check)] = &[
        (
            "appends_are_visible_to_a_reader_while_the_writer_stays_open",
            appends_are_visible_to_a_reader_while_the_writer_stays_open,
        ),
        (
            "reopening_to_append_keeps_what_is_there",
            reopening_to_append_keeps_what_is_there,
        ),
        (
            "replace_is_atomic_for_open_readers_and_returns_an_append_handle",
            replace_is_atomic_for_open_readers_and_returns_an_append_handle,
        ),
        (
            "a_missing_key_is_empty_not_an_error",
            a_missing_key_is_empty_not_an_error,
        ),
        (
            "list_names_direct_children_by_kind",
            list_names_direct_children_by_kind,
        ),
        (
            "delete_and_delete_prefix_remove_what_they_name",
            delete_and_delete_prefix_remove_what_they_name,
        ),
        (
            "prune_forgets_only_an_empty_prefix",
            prune_forgets_only_an_empty_prefix,
        ),
        (
            "modified_moves_forward_on_write",
            modified_moves_forward_on_write,
        ),
        (
            "the_stale_temp_sweep_never_touches_blobs",
            the_stale_temp_sweep_never_touches_blobs,
        ),
        (
            "append_and_replace_handles_read_back_what_they_hold",
            append_and_replace_handles_read_back_what_they_hold,
        ),
        ("writes_after_a_seek_append", writes_after_a_seek_append),
    ];
    for (name, check) in checks {
        let (_dir, storage) = fresh();
        eprintln!("blob contract: {name}");
        check(&*storage);
    }
}

fn local() -> (tempfile::TempDir, Box<dyn BlobStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalBlobStorage::new(dir.path().join("blobs"));
    (dir, Box::new(storage))
}

#[test]
fn local_storage_meets_the_contract() {
    run(local);
}

#[test]
fn keys_refuse_segments_that_could_leave_the_root() {
    for bad in ["", ".", "..", "a/b", "a\\b", "a\0b"] {
        let err = BlobKey::root().child(bad).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "{bad:?}");
    }
    assert_eq!(key("a/b.log").segments(), ["a", "b.log"]);
}

#[test]
fn local_keys_map_to_files_below_the_root() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalBlobStorage::new(dir.path());
    let k = key("ws/s/run.log");
    storage.open_append(&k).unwrap().write_all(b"x").unwrap();
    let path = dir.path().join("ws").join("s").join("run.log");
    assert_eq!(storage.display_path(&k), path);
    assert_eq!(std::fs::read(path).unwrap(), b"x");
}

#[test]
fn a_failed_local_replace_keeps_the_old_blob_and_no_temp() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalBlobStorage::new(dir.path());
    let k = key("s/run.log");
    storage
        .open_append(&k)
        .unwrap()
        .write_all(b"old\n")
        .unwrap();
    super::local::FAIL_RENAME.with(|c| c.set(true));
    let result = storage.replace(&k, b"new\n");
    super::local::FAIL_RENAME.with(|c| c.set(false));
    assert!(result.is_err());
    assert_eq!(read_key(&storage, &k), b"old\n");
    let names: Vec<_> = storage
        .list(&key("s"))
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, vec!["run.log".to_string()]);
}

#[test]
fn a_stale_local_temp_is_swept_and_a_fresh_one_kept() {
    let dir = tempfile::tempdir().unwrap();
    let storage = LocalBlobStorage::new(dir.path());
    storage.open_append(&key("s/run.log")).unwrap();
    let folder = dir.path().join("s");
    let stale = folder.join(format!("run.log.1-0{}", super::TEMP_SUFFIX));
    let fresh = folder.join(format!("run.log.1-1{}", super::TEMP_SUFFIX));
    std::fs::write(&stale, b"x").unwrap();
    std::fs::write(&fresh, b"x").unwrap();
    let old = SystemTime::now() - Duration::from_secs(3600);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&stale)
        .unwrap()
        .set_modified(old)
        .unwrap();
    let sweep = storage.remove_stale_temps(&key("s"), SystemTime::now() - Duration::from_secs(60));
    assert_eq!(sweep.deleted, 1);
    assert!(!stale.exists());
    assert!(fresh.exists());
    assert!(folder.join("run.log").exists());
}

#[cfg(unix)]
#[test]
fn local_storage_never_follows_a_symlink_below_the_root() {
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("run.log"), b"secret").unwrap();
    let storage = LocalBlobStorage::new(dir.path());
    std::os::unix::fs::symlink(outside.path(), dir.path().join("ws")).unwrap();
    let k = key("ws/run.log");
    assert!(storage.open_read(&k).is_err());
    assert!(storage.open_append(&k).is_err());
    assert!(storage.exists(&k).is_err());
    // Deleting the linked folder removes the link, never the target.
    storage.delete_prefix(&key("ws")).unwrap();
    assert!(outside.path().join("run.log").exists());
    assert!(!dir.path().join("ws").exists());
}
