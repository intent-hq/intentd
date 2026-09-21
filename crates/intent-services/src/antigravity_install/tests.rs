use super::*;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::AsyncReadExt;

fn archive(root: &Path, names: &[&str]) -> PathBuf {
    let path = root.join("fixture.zip");
    let mut writer = zip::ZipWriter::new(File::create(&path).unwrap());
    let options = zip::write::SimpleFileOptions::default().unix_permissions(0o700);
    for name in names {
        writer.start_file(*name, options).unwrap();
        writer.write_all(b"fixture").unwrap();
    }
    writer.finish().unwrap();
    path
}

#[test]
fn extraction_keeps_the_complete_bundle_and_rejects_unsafe_members() {
    for names in [
        vec![SERVER, HARNESS],
        vec![SERVER],
        vec![SERVER, "../escape"],
        vec![SERVER, "/escape"],
        vec![SERVER, "nested/harness"],
        vec![SERVER, HARNESS, "extra"],
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = archive(root.path(), &names);
        let target = root.path().join("bundle");
        fs::create_dir(&target).unwrap();
        let result = extract_bundle(
            &path,
            &target,
            &Cancellation::default(),
            Instant::now() + Duration::from_secs(5),
        );
        if names == [SERVER, HARNESS] {
            result.unwrap();
            assert_eq!(fs::read(target.join(SERVER)).unwrap(), b"fixture");
            assert_eq!(fs::read(target.join(HARNESS)).unwrap(), b"fixture");
        } else {
            assert_eq!(result, Err(InstallError::InvalidArchive));
            assert!(!root.path().join("escape").exists());
        }
    }
}

#[test]
fn extraction_rejects_archive_symlinks_and_corruption() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("link.zip");
    let mut writer = zip::ZipWriter::new(File::create(&path).unwrap());
    writer
        .add_symlink(SERVER, "outside", zip::write::SimpleFileOptions::default())
        .unwrap();
    writer
        .start_file(HARNESS, zip::write::SimpleFileOptions::default())
        .unwrap();
    writer.write_all(b"fixture").unwrap();
    writer.finish().unwrap();
    let target = root.path().join("bundle");
    fs::create_dir(&target).unwrap();
    assert_eq!(
        extract_bundle(
            &path,
            &target,
            &Cancellation::default(),
            Instant::now() + Duration::from_secs(5)
        ),
        Err(InstallError::InvalidArchive)
    );
    fs::write(&path, b"not a ZIP").unwrap();
    assert_eq!(
        extract_bundle(
            &path,
            &target,
            &Cancellation::default(),
            Instant::now() + Duration::from_secs(5)
        ),
        Err(InstallError::InvalidArchive)
    );
}

#[test]
fn extraction_obeys_cancellation_and_deadline() {
    let root = tempfile::tempdir().unwrap();
    let path = archive(root.path(), &[SERVER, HARNESS]);
    for cancelled in [false, true] {
        let target = tempfile::tempdir().unwrap();
        let cancel = Cancellation::default();
        if cancelled {
            cancel.cancel();
        }
        assert_eq!(
            extract_bundle(&path, target.path(), &cancel, Instant::now()),
            Err(if cancelled {
                InstallError::Cancelled
            } else {
                InstallError::TimedOut
            })
        );
    }
}

#[cfg(unix)]
#[test]
fn root_rejects_symlinks_without_touching_their_target() {
    let parent = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), parent.path().join("link")).unwrap();
    assert_eq!(
        prepare_root(&parent.path().join("link/managed")),
        Err(InstallError::DiskError)
    );
    assert!(!outside.path().join("managed").exists());
}

#[test]
fn failed_activation_restores_previous_installation() {
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join(VERSION);
    fs::create_dir(&destination).unwrap();
    fs::write(destination.join("previous"), b"preserved").unwrap();
    assert_eq!(
        activate(&root.path().join("absent"), &destination, root.path()),
        Err(InstallError::DiskError)
    );
    assert_eq!(
        fs::read(destination.join("previous")).unwrap(),
        b"preserved"
    );
}

#[cfg(unix)]
#[test]
fn readiness_marker_replaces_links_without_overwriting_their_target() {
    let root = tempfile::tempdir().unwrap();
    let outside = root.path().join("outside");
    fs::write(&outside, b"preserved").unwrap();
    let bundle = root.path().join("bundle");
    fs::create_dir(&bundle).unwrap();
    std::os::unix::fs::symlink(&outside, bundle.join("ready")).unwrap();
    write_ready(&bundle).unwrap();
    assert_eq!(fs::read(&outside).unwrap(), b"preserved");
    assert_eq!(
        fs::read_to_string(bundle.join("ready")).unwrap(),
        ARCHIVE_SHA256
    );
    assert!(fs::symlink_metadata(bundle.join("ready"))
        .unwrap()
        .is_file());
}

#[tokio::test]
async fn cancellation_is_sticky_for_late_waiters() {
    let cancel = Cancellation::default();
    cancel.cancel();
    tokio::time::timeout(Duration::from_millis(50), cancel.cancelled())
        .await
        .unwrap();
}

#[tokio::test]
async fn install_timeout_does_not_cancel_the_callers_operation() {
    // Waiting for another install exercises the real outer deadline without
    // network access or a platform-specific executable fixture.
    let _lock = install_lock().lock().await;
    let root = tempfile::tempdir().unwrap();
    let caller = Cancellation::default();
    assert_eq!(
        install_with_timeout(
            root.path().join("managed"),
            caller.clone(),
            Arc::new(|_| panic!("queued install must not download")),
            Duration::from_millis(1),
        )
        .await,
        Err(InstallError::TimedOut)
    );
    assert!(
        !caller.is_cancelled(),
        "caller must publish the timeout failure"
    );
    assert!(!root.path().join("managed").exists());

    caller.cancel();
    assert_eq!(
        install_with_timeout(
            root.path().join("managed"),
            caller,
            Arc::new(|_| panic!("cancelled install must not download")),
            Duration::ZERO,
        )
        .await,
        Err(InstallError::Cancelled),
        "explicit cancellation takes precedence over the deadline"
    );
}

async fn http_response(response: &'static [u8]) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let _ = stream.read(&mut request).await;
        let _ = stream.write_all(response).await;
    });
    (url, task)
}

#[tokio::test]
async fn downloads_enforce_integrity_and_size_even_without_content_length() {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    for (response, limit, hash, expected) in [
        (
            &b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc"[..],
            3,
            hex(&Sha256::digest(b"abc")),
            Ok(()),
        ),
        (
            &b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc"[..],
            3,
            "wrong".into(),
            Err(InstallError::IntegrityFailed),
        ),
        (
            &b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nabcd"[..],
            3,
            "wrong".into(),
            Err(InstallError::DownloadFailed),
        ),
        (
            &b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nabcd"[..],
            3,
            "wrong".into(),
            Err(InstallError::InvalidArchive),
        ),
        (
            &b"HTTP/1.1 302 Found\r\nLocation: https://other.invalid\r\nContent-Length: 0\r\n\r\n"
                [..],
            3,
            "wrong".into(),
            Err(InstallError::DownloadFailed),
        ),
    ] {
        let (url, task) = http_response(response).await;
        let root = tempfile::tempdir().unwrap();
        let progress: Progress = Arc::new(|_| {});
        assert_eq!(
            download(
                &client,
                &url,
                &root.path().join("download"),
                limit,
                &hash,
                &Cancellation::default(),
                &progress
            )
            .await,
            expected
        );
        task.await.unwrap();
    }
}

#[tokio::test]
async fn incomplete_and_wrong_architecture_bundles_never_reach_codesign() {
    let root = tempfile::tempdir().unwrap();
    fs::write(root.path().join(SERVER), b"MZ wrong platform").unwrap();
    assert_eq!(
        valid_bundle(root.path(), &Cancellation::default()).await,
        Err(InstallError::IntegrityFailed)
    );
    fs::write(root.path().join(HARNESS), b"MZ wrong platform").unwrap();
    assert_eq!(
        valid_bundle(root.path(), &Cancellation::default()).await,
        Err(InstallError::IntegrityFailed)
    );
}

/// Explicit maintainer check: downloads the pinned official archive once.
/// Never changes the user's installed runtime or credentials.
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
#[ignore = "downloads the official 316 MB bundle and verifies Apple signatures"]
async fn official_bundle_install_is_single_flight_and_reuses_cache() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().canonicalize().unwrap().join("managed");
    let downloads = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&downloads);
    let progress: Progress = Arc::new(move |event| {
        if matches!(event, InstallProgress::Downloading { received: 0, .. }) {
            count.fetch_add(1, Ordering::SeqCst);
        }
    });
    let (first, second) = tokio::join!(
        install(path.clone(), Cancellation::default(), Arc::clone(&progress)),
        install(path.clone(), Cancellation::default(), Arc::clone(&progress)),
    );
    assert_eq!(first.unwrap(), second.unwrap());
    assert_eq!(downloads.load(Ordering::SeqCst), 1);
    assert_eq!(
        fs::read_to_string(path.join(VERSION).join("ready")).unwrap(),
        ARCHIVE_SHA256
    );
    assert_eq!(fs::read_dir(&path).unwrap().count(), 1);
}
