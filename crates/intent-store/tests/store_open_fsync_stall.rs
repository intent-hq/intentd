//! `Store::open` must not create a brand-new database inside the write pool's
//! acquire deadline.
//!
//! Creating a database file switches it to WAL before `synchronous = NORMAL`
//! applies, so creation commits several full fsyncs. When those ran inside the
//! single write connection's first connect, one fsync stalled behind a busy
//! disk (llvm-cov runners flushing profile data, a loaded Bazel host) failed
//! the open with "write pool exhausted (acquire timeout exceeded)" — the
//! deadline meant for waiting on a contended writer.
//!
//! This binary installs a default `SQLite` VFS that wraps the platform VFS
//! and can stall `xSync`, so the slow disk is reproduced deterministically.
//! The VFS is process-global, so these tests share one serial lock.

use std::ffi::{c_char, c_int, c_void};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use libsqlite3_sys as ffi;

/// Longer than the write pool's 10 s acquire timeout.
const STALL: Duration = Duration::from_secs(11);

/// Every `xSync` the process has issued through the shim.
static SYNCS: AtomicU64 = AtomicU64::new(0);
/// The `SYNCS` value whose sync stalls for [`STALL`]; `u64::MAX` disarms it.
static STALL_AT: AtomicU64 = AtomicU64::new(u64::MAX);
/// Serializes the tests: the VFS and its counters are process-global.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[repr(C)]
struct ShimFile {
    base: ffi::sqlite3_file,
    real: *mut ffi::sqlite3_file,
}

struct BaseVfs(*mut ffi::sqlite3_vfs);
// SAFETY: the platform VFS is a process-lifetime static that SQLite
// documents as safe to use from any thread.
unsafe impl Send for BaseVfs {}
unsafe impl Sync for BaseVfs {}

static BASE: OnceLock<BaseVfs> = OnceLock::new();

unsafe fn real(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    unsafe { (*file.cast::<ShimFile>()).real }
}

unsafe fn methods(file: *mut ffi::sqlite3_file) -> &'static ffi::sqlite3_io_methods {
    unsafe { &*(*real(file)).pMethods }
}

macro_rules! forward {
    ($name:ident, $method:ident, ($($arg:ident: $ty:ty),*) -> $ret:ty) => {
        unsafe extern "C" fn $name(file: *mut ffi::sqlite3_file $(, $arg: $ty)*) -> $ret {
            unsafe { (methods(file).$method.expect(stringify!($method)))(real(file) $(, $arg)*) }
        }
    };
}

forward!(shim_close, xClose, () -> c_int);
forward!(shim_read, xRead, (buf: *mut c_void, amount: c_int, offset: ffi::sqlite3_int64) -> c_int);
forward!(shim_write, xWrite, (buf: *const c_void, amount: c_int, offset: ffi::sqlite3_int64) -> c_int);
forward!(shim_truncate, xTruncate, (size: ffi::sqlite3_int64) -> c_int);
forward!(shim_file_size, xFileSize, (size: *mut ffi::sqlite3_int64) -> c_int);
forward!(shim_lock, xLock, (level: c_int) -> c_int);
forward!(shim_unlock, xUnlock, (level: c_int) -> c_int);
forward!(shim_check_reserved_lock, xCheckReservedLock, (out: *mut c_int) -> c_int);
forward!(shim_file_control, xFileControl, (op: c_int, arg: *mut c_void) -> c_int);
forward!(shim_sector_size, xSectorSize, () -> c_int);
forward!(shim_device_characteristics, xDeviceCharacteristics, () -> c_int);
forward!(shim_shm_map, xShmMap, (page: c_int, size: c_int, extend: c_int, out: *mut *mut c_void) -> c_int);
forward!(shim_shm_lock, xShmLock, (offset: c_int, n: c_int, flags: c_int) -> c_int);
forward!(shim_shm_unmap, xShmUnmap, (delete: c_int) -> c_int);
forward!(shim_fetch, xFetch, (offset: ffi::sqlite3_int64, amount: c_int, out: *mut *mut c_void) -> c_int);
forward!(shim_unfetch, xUnfetch, (offset: ffi::sqlite3_int64, page: *mut c_void) -> c_int);

unsafe extern "C" fn shim_shm_barrier(file: *mut ffi::sqlite3_file) {
    unsafe { (methods(file).xShmBarrier.expect("xShmBarrier"))(real(file)) }
}

unsafe extern "C" fn shim_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    if SYNCS.fetch_add(1, Ordering::SeqCst) == STALL_AT.load(Ordering::SeqCst) {
        // timing-guard: the stall IS the fixture — a disk that blocks one fsync past the deadline.
        std::thread::sleep(STALL);
    }
    unsafe { (methods(file).xSync.expect("xSync"))(real(file), flags) }
}

static SHIM_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(shim_close),
    xRead: Some(shim_read),
    xWrite: Some(shim_write),
    xTruncate: Some(shim_truncate),
    xSync: Some(shim_sync),
    xFileSize: Some(shim_file_size),
    xLock: Some(shim_lock),
    xUnlock: Some(shim_unlock),
    xCheckReservedLock: Some(shim_check_reserved_lock),
    xFileControl: Some(shim_file_control),
    xSectorSize: Some(shim_sector_size),
    xDeviceCharacteristics: Some(shim_device_characteristics),
    xShmMap: Some(shim_shm_map),
    xShmLock: Some(shim_shm_lock),
    xShmBarrier: Some(shim_shm_barrier),
    xShmUnmap: Some(shim_shm_unmap),
    xFetch: Some(shim_fetch),
    xUnfetch: Some(shim_unfetch),
};

unsafe extern "C" fn shim_open(
    _vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let base = BASE.get().expect("shim VFS installed").0;
    unsafe {
        let shim = file.cast::<ShimFile>();
        // `szOsFile` reserves the real file directly after `ShimFile`, whose
        // `repr(C)` size is a multiple of its (8-byte) alignment, so the real
        // file is as aligned as `sqlite3_file` needs.
        let real = shim.add(1).cast::<ffi::sqlite3_file>();
        (*shim).real = real;
        (*real).pMethods = std::ptr::null();
        let rc = ((*base).xOpen.expect("xOpen"))(base, name, real, flags, out_flags);
        (*file).pMethods = if (*real).pMethods.is_null() {
            std::ptr::null()
        } else {
            &raw const SHIM_METHODS
        };
        rc
    }
}

/// Make the stalling shim the default VFS for every connection in this process.
fn install_shim() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| unsafe {
        let base = ffi::sqlite3_vfs_find(std::ptr::null());
        assert!(!base.is_null(), "no default SQLite VFS");
        assert!((*base).iVersion >= 1);
        BASE.set(BaseVfs(base)).ok().expect("base VFS set once");
        let mut shim = std::ptr::read(base);
        shim.szOsFile =
            c_int::try_from(std::mem::size_of::<ShimFile>()).unwrap() + (*base).szOsFile;
        shim.zName = c"intent-stall-sync".as_ptr();
        shim.pNext = std::ptr::null_mut();
        shim.xOpen = Some(shim_open);
        let rc = ffi::sqlite3_vfs_register(Box::into_raw(Box::new(shim)), 1);
        assert_eq!(rc, ffi::SQLITE_OK, "register shim VFS");
    });
}

fn stall_next_sync() {
    STALL_AT.store(SYNCS.load(Ordering::SeqCst), Ordering::SeqCst);
}

fn disarm() {
    STALL_AT.store(u64::MAX, Ordering::SeqCst);
}

async fn open(path: &Path) -> intent_store::Store {
    intent_store::Store::open(path)
        .await
        .unwrap_or_else(|error| panic!("open store: {error}"))
}

/// Regression: a fresh database whose first fsync stalls past the write
/// pool's acquire timeout still opens, migrated and writable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_store_open_survives_a_creation_fsync_stall() {
    let _serial = SERIAL.lock().await;
    install_shim();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    let before = SYNCS.load(Ordering::SeqCst);
    stall_next_sync();
    let store = open(&path).await;
    disarm();
    assert!(
        SYNCS.load(Ordering::SeqCst) > before,
        "creation issued no fsync, so the stall was never exercised"
    );
    let status = store.migration_status().await.expect("migration status");
    assert!(status.is_current(), "fresh store is not fully migrated");
    store.close().await;
}

/// The invariant the fix relies on: once the file exists, connecting the
/// write pool issues no fsync, so a slow disk cannot spend its deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reopening_a_created_store_connects_without_fsync() {
    let _serial = SERIAL.lock().await;
    install_shim();
    disarm();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.db");
    open(&path).await.close().await;
    let before = SYNCS.load(Ordering::SeqCst);
    let pool = intent_store::connect_write(&path)
        .await
        .expect("connect write pool");
    assert_eq!(
        SYNCS.load(Ordering::SeqCst),
        before,
        "connecting the write pool to an existing store issued an fsync"
    );
    pool.close().await;
}

/// `auto_vacuum` and WAL of a closed database file, read on a fresh
/// connection so no pool setting can mask the file's own header.
async fn file_modes(path: &Path) -> (i64, String) {
    use sqlx::{ConnectOptions, Connection, Row};
    let mut conn = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .connect()
        .await
        .expect("inspect database file");
    let auto_vacuum = sqlx::query("PRAGMA auto_vacuum")
        .fetch_one(&mut conn)
        .await
        .expect("read auto_vacuum")
        .get(0);
    let journal_mode = sqlx::query("PRAGMA journal_mode")
        .fetch_one(&mut conn)
        .await
        .expect("read journal_mode")
        .get(0);
    conn.close().await.expect("close inspection connection");
    (auto_vacuum, journal_mode)
}

/// Pool connections no longer set `auto_vacuum`, so the creation step alone
/// must leave every new database incremental and in WAL — on the plain and
/// the daemon-owned open paths alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_databases_are_created_incremental_and_wal() {
    let _serial = SERIAL.lock().await;
    install_shim();
    disarm();
    let dir = tempfile::tempdir().unwrap();

    let plain = dir.path().join("plain.db");
    open(&plain).await.close().await;
    assert_eq!(
        file_modes(&plain).await,
        (2, "wal".to_string()),
        "Store::open"
    );

    let owned = dir.path().join("owned.db");
    intent_store::Store::open_for_daemon(&owned)
        .await
        .unwrap_or_else(|error| panic!("open daemon store: {error}"))
        .close()
        .await;
    assert_eq!(
        file_modes(&owned).await,
        (2, "wal".to_string()),
        "Store::open_for_daemon"
    );
}
