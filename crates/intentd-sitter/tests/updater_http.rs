//! Integration tests for the update engine against a local HTTP fixture
//! server (127.0.0.1 only — no real network access).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::thread;

use sha2::{Digest, Sha256};

use intentd_sitter::cli::Channel;
use intentd_sitter::manifest::TARGET_TRIPLE;
use intentd_sitter::paths::{SitterPaths, DAEMON_BIN_NAME};
use intentd_sitter::state::{self, SitterState};
use intentd_sitter::updater::{UpdateError, UpdateOutcome, Updater};

/// Minimal single-purpose HTTP/1.1 fixture server: serves a fixed
/// path → body map, closing each connection after one response.
fn serve(routes: HashMap<String, Vec<u8>>) -> String {
    serve_on(TcpListener::bind("127.0.0.1:0").unwrap(), routes)
}

fn handle(mut stream: TcpStream, routes: &HashMap<String, Vec<u8>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() {
        return;
    }
    loop {
        let mut header = String::new();
        match reader.read_line(&mut header) {
            Ok(_) if header != "\r\n" && !header.is_empty() => {}
            _ => break,
        }
    }
    let path = request_line.split_whitespace().nth(1).unwrap_or("/");
    let (status, body) = match routes.get(path) {
        Some(body) => ("200 OK", body.clone()),
        None => ("404 Not Found", b"not found".to_vec()),
    };
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(&body);
}

/// A `.tar.xz` archive holding `intentd-<triple>/intentd[.exe]` — the same
/// layout cargo-dist produces for unix targets.
fn make_tar_xz(bin_contents: &[u8]) -> Vec<u8> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bin_contents.len() as u64);
    header.set_mode(0o755);
    header.set_cksum();
    let encoder = liblzma::write::XzEncoder::new(Vec::new(), 6);
    let mut builder = tar::Builder::new(encoder);
    builder
        .append_data(
            &mut header,
            format!("intentd-{TARGET_TRIPLE}/{DAEMON_BIN_NAME}"),
            bin_contents,
        )
        .unwrap();
    builder.into_inner().unwrap().finish().unwrap()
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// Schema-v1 manifest with a single platform entry for this build's triple.
fn manifest_json(version: &str, base_url: &str, asset: &str, sha256: &str) -> Vec<u8> {
    serde_json::json!({
        "schema": 1,
        "channel": "stable",
        "version": version,
        "tag": format!("v{version}"),
        "published_at": "2026-07-21T00:00:00Z",
        "platforms": {
            TARGET_TRIPLE: {
                "asset": asset,
                "url": format!("{base_url}/{asset}"),
                "sha256": sha256,
            }
        }
    })
    .to_string()
    .into_bytes()
}

/// Serve a manifest for `version` (on the stable channel) plus its archive.
/// Returns the base URL. `tamper_sha` swaps in a wrong digest.
fn serve_release(version: &str, bin_contents: &[u8], tamper_sha: bool) -> String {
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let archive = make_tar_xz(bin_contents);
    let sha = if tamper_sha {
        sha256_hex(b"something else entirely")
    } else {
        sha256_hex(&archive)
    };

    // The manifest embeds absolute archive URLs, so learn the bound address
    // first, then register routes against it. The same listener is handed to
    // the server thread — dropping and rebinding by address (the previous
    // approach) left a window where a parallel test could bind the port
    // (intent-hq/monorepo#1211).
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let mut routes = HashMap::new();
    routes.insert(
        "/channel-stable/stable.json".to_string(),
        manifest_json(version, &base_url, &asset, &sha),
    );
    routes.insert(format!("/{asset}"), archive);
    serve_on(listener, routes)
}

/// Serve `routes` on an already-bound listener; returns its base URL.
fn serve_on(listener: TcpListener, routes: HashMap<String, Vec<u8>>) -> String {
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let routes = Arc::new(routes);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let routes = Arc::clone(&routes);
            thread::spawn(move || handle(stream, &routes));
        }
    });
    base_url
}

fn paths_in(dir: &Path) -> SitterPaths {
    SitterPaths::from_data_dir(dir)
}

fn preinstall(paths: &SitterPaths, version: &str, channel: Channel) {
    let bin = paths.daemon_binary(version);
    fs::create_dir_all(bin.parent().unwrap()).unwrap();
    fs::write(&bin, format!("fake daemon {version}")).unwrap();
    let state = SitterState {
        channel,
        current_version: Some(version.to_string()),
        ..SitterState::default()
    };
    state::save(&paths.state_path, &state).unwrap();
}

fn installed_versions(paths: &SitterPaths) -> Vec<String> {
    let mut versions: Vec<String> = fs::read_dir(&paths.versions_dir)
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|e| e.file_name().to_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    versions.sort();
    versions
}

#[test]
fn happy_path_downloads_verifies_installs_and_updates_state() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    let base_url = serve_release("0.2.0", b"#!/bin/sh\necho fake daemon 0.2.0\n", false);

    let updater = Updater::with_base_url(paths.clone(), &base_url).unwrap();
    let outcome = updater.check_and_install(Channel::Stable).unwrap();
    assert_eq!(
        outcome,
        UpdateOutcome::Installed {
            version: "0.2.0".to_string(),
            previous: None,
        }
    );

    let bin = paths.daemon_binary("0.2.0");
    assert_eq!(
        fs::read(&bin).unwrap(),
        b"#!/bin/sh\necho fake daemon 0.2.0\n"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&bin).unwrap().permissions().mode();
        assert_eq!(mode & 0o111, 0o111, "expected exec bits, got {mode:o}");
    }

    let state = state::load(&paths.state_path);
    assert_eq!(state.current_version.as_deref(), Some("0.2.0"));
    assert_eq!(state.channel, Channel::Stable);

    // In-flight download dirs are cleaned up.
    let leftovers = fs::read_dir(&paths.tmp_dir)
        .map(std::iter::Iterator::count)
        .unwrap_or_default();
    assert_eq!(leftovers, 0);
}

#[test]
fn sha256_mismatch_is_rejected_and_nothing_installed() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    let base_url = serve_release("0.2.0", b"evil bytes", true);

    let updater = Updater::with_base_url(paths.clone(), &base_url).unwrap();
    let err = updater.check_and_install(Channel::Stable).unwrap_err();
    assert!(
        matches!(err, UpdateError::Sha256Mismatch { .. }),
        "expected Sha256Mismatch, got {err:?}"
    );

    assert!(installed_versions(&paths).is_empty());
    assert_eq!(state::load(&paths.state_path).current_version, None);
}

#[test]
fn invalid_manifest_version_is_rejected_and_nothing_installed() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    // Fresh install (no current version): a non-semver manifest version must
    // be rejected before it becomes a `versions/` directory name.
    let base_url = serve_release("../not-semver", b"evil bytes", false);

    let updater = Updater::with_base_url(paths.clone(), &base_url).unwrap();
    let err = updater.check_and_install(Channel::Stable).unwrap_err();
    assert!(
        matches!(err, UpdateError::InvalidManifestVersion { .. }),
        "expected InvalidManifestVersion, got {err:?}"
    );

    assert!(installed_versions(&paths).is_empty());
    assert_eq!(state::load(&paths.state_path).current_version, None);
}

#[test]
fn network_down_is_a_soft_failure() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    let base_url = unreachable_base();

    let updater = Updater::with_base_url(paths.clone(), &base_url).unwrap();
    let err = updater.check_and_install(Channel::Stable).unwrap_err();
    assert!(
        matches!(err, UpdateError::Network { .. }),
        "expected Network, got {err:?}"
    );
    assert_eq!(state::load(&paths.state_path).current_version, None);
}

#[test]
fn http_error_status_is_a_soft_failure() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    let base_url = serve(HashMap::new()); // every path 404s

    let updater = Updater::with_base_url(paths, &base_url).unwrap();
    let err = updater.check_and_install(Channel::Stable).unwrap_err();
    assert!(
        matches!(err, UpdateError::HttpStatus { status, .. } if status.as_u16() == 404),
        "expected HttpStatus 404, got {err:?}"
    );
}

#[test]
fn noop_when_manifest_is_equal_or_older() {
    for manifest_version in ["0.2.0", "0.1.0"] {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths_in(dir.path());
        preinstall(&paths, "0.2.0", Channel::Stable);
        let base_url = serve_release(manifest_version, b"newer bytes", false);

        let updater = Updater::with_base_url(paths.clone(), &base_url).unwrap();
        let outcome = updater.check_and_install(Channel::Stable).unwrap();
        assert_eq!(
            outcome,
            UpdateOutcome::AlreadyCurrent {
                version: "0.2.0".to_string()
            },
            "manifest {manifest_version} should be a no-op"
        );
        // The preinstalled binary is untouched.
        assert_eq!(
            fs::read(paths.daemon_binary("0.2.0")).unwrap(),
            b"fake daemon 0.2.0"
        );
    }
}

#[test]
fn reinstalls_when_state_points_at_a_missing_binary() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    // state.json says 0.2.0 but versions/ was wiped.
    let state = SitterState {
        current_version: Some("0.2.0".to_string()),
        ..SitterState::default()
    };
    state::save(&paths.state_path, &state).unwrap();
    let base_url = serve_release("0.2.0", b"restored daemon", false);

    let updater = Updater::with_base_url(paths.clone(), &base_url).unwrap();
    let outcome = updater.check_and_install(Channel::Stable).unwrap();
    assert_eq!(
        outcome,
        UpdateOutcome::Installed {
            version: "0.2.0".to_string(),
            previous: Some("0.2.0".to_string()),
        }
    );
    assert!(paths.daemon_binary("0.2.0").exists());
}

#[test]
fn unknown_manifest_schema_is_a_soft_failure() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    let mut routes = HashMap::new();
    routes.insert(
        "/channel-stable/stable.json".to_string(),
        br#"{"schema": 2, "shape": "of things to come"}"#.to_vec(),
    );
    let base_url = serve(routes);

    let updater = Updater::with_base_url(paths, &base_url).unwrap();
    let err = updater.check_and_install(Channel::Stable).unwrap_err();
    assert!(
        matches!(err, UpdateError::Manifest(_)),
        "expected Manifest, got {err:?}"
    );
}

#[test]
fn missing_platform_entry_is_a_soft_failure() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    let mut routes = HashMap::new();
    routes.insert(
        "/channel-stable/stable.json".to_string(),
        br#"{"schema": 1, "version": "0.2.0", "platforms": {}}"#.to_vec(),
    );
    let base_url = serve(routes);

    let updater = Updater::with_base_url(paths, &base_url).unwrap();
    let err = updater.check_and_install(Channel::Stable).unwrap_err();
    assert!(
        matches!(err, UpdateError::NoPlatformEntry { .. }),
        "expected NoPlatformEntry, got {err:?}"
    );
}

#[test]
fn prune_keeps_current_and_one_previous_version() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    // Two older versions on disk; 0.2.0 is current.
    preinstall(&paths, "0.1.0", Channel::Stable);
    preinstall(&paths, "0.2.0", Channel::Stable);
    let base_url = serve_release("0.3.0", b"daemon 0.3.0", false);

    let updater = Updater::with_base_url(paths.clone(), &base_url).unwrap();
    let outcome = updater.check_and_install(Channel::Stable).unwrap();
    assert_eq!(
        outcome,
        UpdateOutcome::Installed {
            version: "0.3.0".to_string(),
            previous: Some("0.2.0".to_string()),
        }
    );

    assert_eq!(installed_versions(&paths), vec!["0.2.0", "0.3.0"]);
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.3.0")
    );
}

/// A base URL whose port refuses requests (network down).
///
/// The listener stays bound for the life of the process and a detached
/// thread accepts each connection and immediately drops it, so the updater's
/// fetch deterministically fails. Binding and then dropping the listener
/// (the previous approach) released the ephemeral port back to the OS, which
/// could reassign it to a sibling test's fixture server before the updater
/// connected — turning the "dead" URL into a live one under parallel test
/// load (intent-hq/monorepo#1211).
fn unreachable_base() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    thread::spawn(move || {
        for stream in listener.incoming() {
            drop(stream);
        }
    });
    base_url
}

#[test]
fn primary_base_is_preferred_when_both_bases_serve() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    let primary = serve_release("0.3.0", b"daemon from primary", false);
    let fallback = serve_release("0.2.0", b"daemon from fallback", false);

    let updater = Updater::with_base_urls(paths.clone(), [primary, fallback]).unwrap();
    let outcome = updater.check_and_install(Channel::Stable).unwrap();
    assert_eq!(
        outcome,
        UpdateOutcome::Installed {
            version: "0.3.0".to_string(),
            previous: None,
        }
    );
    assert_eq!(
        fs::read(paths.daemon_binary("0.3.0")).unwrap(),
        b"daemon from primary"
    );
}

#[test]
fn falls_back_to_second_base_when_primary_manifest_fetch_fails() {
    let unparseable = HashMap::from([(
        "/channel-stable/stable.json".to_string(),
        b"not json".to_vec(),
    )]);
    let cases = [
        ("404", serve(HashMap::new())),
        ("connection reset", unreachable_base()),
        ("unparseable manifest", serve(unparseable)),
    ];
    for (case, primary) in cases {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths_in(dir.path());
        let fallback = serve_release("0.2.0", b"daemon from fallback", false);

        let updater = Updater::with_base_urls(paths.clone(), [primary, fallback]).unwrap();
        let outcome = updater.check_and_install(Channel::Stable).unwrap();
        assert_eq!(
            outcome,
            UpdateOutcome::Installed {
                version: "0.2.0".to_string(),
                previous: None,
            },
            "case: {case}"
        );
        assert_eq!(
            fs::read(paths.daemon_binary("0.2.0")).unwrap(),
            b"daemon from fallback",
            "case: {case}"
        );
    }
}

#[test]
fn all_bases_failing_reports_the_last_error() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    let primary = serve(HashMap::new());
    let fallback = serve(HashMap::new());

    let updater = Updater::with_base_urls(paths.clone(), [primary, fallback.clone()]).unwrap();
    let err = updater.check_and_install(Channel::Stable).unwrap_err();
    match err {
        UpdateError::HttpStatus { url, status } => {
            assert_eq!(status.as_u16(), 404);
            assert!(
                url.starts_with(&fallback),
                "expected the last error to come from the fallback base, got {url}"
            );
        }
        other => panic!("expected HttpStatus, got {other:?}"),
    }
    assert!(installed_versions(&paths).is_empty());
    assert_eq!(state::load(&paths.state_path).current_version, None);
}

#[test]
fn empty_base_url_list_is_a_soft_error() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    let Err(err) = Updater::with_base_urls(paths, Vec::<String>::new()) else {
        panic!("expected an error for an empty base URL list")
    };
    assert!(
        matches!(err, UpdateError::NoBaseUrls),
        "expected NoBaseUrls, got {err:?}"
    );
}

/// Serve a release for `version` whose archive download triggers
/// `on_archive_request` (once, before the response bytes are sent) — used to
/// interleave a concurrent install mid-download deterministically.
fn serve_release_with_archive_hook(
    version: &str,
    bin_contents: &[u8],
    on_archive_request: impl FnOnce() + Send + 'static,
) -> String {
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let archive = make_tar_xz(bin_contents);
    let sha = sha256_hex(&archive);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let manifest = manifest_json(version, &base_url, &asset, &sha);
    let archive_path = format!("/{asset}");
    let exact_archive_path = format!("/v{version}/{asset}");
    let sidecar_path = format!("{exact_archive_path}.sha256");

    let hook = std::sync::Mutex::new(Some(on_archive_request));
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                continue;
            }
            loop {
                let mut header = String::new();
                match reader.read_line(&mut header) {
                    Ok(_) if header != "\r\n" && !header.is_empty() => {}
                    _ => break,
                }
            }
            let path = request_line.split_whitespace().nth(1).unwrap_or("/");
            let (status, body) = if path == "/channel-stable/stable.json" {
                ("200 OK", manifest.clone())
            } else if path == sidecar_path {
                ("200 OK", sha.as_bytes().to_vec())
            } else if path == archive_path || path == exact_archive_path {
                if let Some(hook) = hook.lock().unwrap().take() {
                    hook();
                }
                ("200 OK", archive.clone())
            } else {
                ("404 Not Found", b"not found".to_vec())
            };
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(&body);
        }
    });
    base_url
}

#[test]
fn losing_an_install_race_keeps_the_concurrent_winners_state() {
    // A concurrent updater (the serve-mode sitter's periodic check) installs
    // 0.3.0 while this check_and_install of 0.2.0 is mid-download: the state
    // write must yield to the newer winner instead of activating a downgrade.
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    preinstall(&paths, "0.1.0", Channel::Stable);

    let winner_paths = paths.clone();
    let base_url = serve_release_with_archive_hook("0.2.0", b"slow daemon 0.2.0", move || {
        preinstall(&winner_paths, "0.3.0", Channel::Stable);
    });

    let updater = Updater::with_base_url(paths.clone(), &base_url).unwrap();
    let outcome = updater.check_and_install(Channel::Stable).unwrap();
    assert_eq!(
        outcome,
        UpdateOutcome::AlreadyCurrent {
            version: "0.3.0".to_string()
        },
        "the older invocation must lose the race, not overwrite the winner"
    );
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.3.0"),
        "state.json must keep the concurrent winner's version"
    );
}

#[test]
fn force_install_still_overwrites_a_concurrent_newer_install() {
    // force_install is the explicit downgrade path (`sitter channel
    // <value> --redownload`): the race guard must not apply to it.
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    preinstall(&paths, "0.1.0", Channel::Stable);

    let winner_paths = paths.clone();
    let base_url = serve_release_with_archive_hook("0.2.0", b"forced daemon 0.2.0", move || {
        preinstall(&winner_paths, "0.3.0", Channel::Stable);
    });

    let updater = Updater::with_base_url(paths.clone(), &base_url).unwrap();
    let outcome = updater.force_install(Channel::Stable).unwrap();
    assert_eq!(
        outcome,
        UpdateOutcome::Installed {
            version: "0.2.0".to_string(),
            previous: Some("0.3.0".to_string()),
        }
    );
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("0.2.0")
    );
}

#[test]
fn with_base_url_means_exactly_one_base_and_never_falls_back() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    let base_url = serve(HashMap::new());

    let updater = Updater::with_base_url(paths.clone(), &base_url).unwrap();
    let err = updater.check_and_install(Channel::Stable).unwrap_err();
    match err {
        UpdateError::HttpStatus { url, status } => {
            assert_eq!(status.as_u16(), 404);
            assert!(
                url.starts_with(&base_url),
                "expected the error to come from the single configured base, got {url}"
            );
        }
        other => panic!("expected HttpStatus, got {other:?}"),
    }
    assert!(installed_versions(&paths).is_empty());
}

#[cfg(unix)]
#[test]
fn exact_release_uses_immutable_artifacts_and_preserves_channel() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    preinstall(&paths, "1.2.1", Channel::Alpha);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
    let archive = make_tar_xz(b"requested B");
    let mut routes = HashMap::new();
    routes.insert(
        format!("/v1.2.2/{asset}.sha256"),
        format!("{}  {asset}\n", sha256_hex(&archive)).into_bytes(),
    );
    routes.insert(format!("/v1.2.2/{asset}"), archive);
    routes.insert(
        "/channel-alpha/alpha.json".into(),
        manifest_json("1.2.3", &base, "latest.tar.xz", "bad"),
    );
    let updater = Updater::with_base_url(paths.clone(), serve_on(listener, routes)).unwrap();
    assert!(
        matches!(updater.install_exact("1.2.2").unwrap(), UpdateOutcome::Installed { version, .. } if version == "1.2.2")
    );
    assert_eq!(
        fs::read(paths.daemon_binary("1.2.2")).unwrap(),
        b"requested B"
    );
    assert_eq!(state::load(&paths.state_path).channel, Channel::Alpha);
    assert!(!paths.daemon_binary("1.2.3").exists());
    assert!(matches!(
        updater.install_exact("1.2.1"),
        Err(UpdateError::Downgrade { .. })
    ));
}

#[cfg(unix)]
#[test]
fn exact_failures_never_install_or_fall_back_to_latest() {
    for failure in ["missing", "checksum", "invalid-sidecar", "missing-archive"] {
        let dir = tempfile::tempdir().unwrap();
        let paths = paths_in(dir.path());
        preinstall(&paths, "1.2.1", Channel::Alpha);
        let before = fs::read(&paths.state_path).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let asset = format!("intentd-{TARGET_TRIPLE}.tar.xz");
        let archive = make_tar_xz(b"B");
        let mut routes = HashMap::new();
        routes.insert(
            "/channel-alpha/alpha.json".into(),
            manifest_json("1.2.3", &base, &asset, &sha256_hex(&archive)),
        );
        routes.insert(format!("/{asset}"), archive.clone());
        if failure != "missing" {
            let checksum = if failure == "checksum" {
                "0".repeat(64)
            } else if failure == "invalid-sidecar" {
                "garbage".into()
            } else {
                sha256_hex(&archive)
            };
            routes.insert(format!("/v1.2.2/{asset}.sha256"), checksum.into_bytes());
        }
        if failure != "missing-archive" {
            routes.insert(format!("/v1.2.2/{asset}"), archive);
        }
        let updater = Updater::with_base_url(paths.clone(), serve_on(listener, routes)).unwrap();
        assert!(updater.install_exact("1.2.2").is_err(), "{failure}");
        assert_eq!(fs::read(&paths.state_path).unwrap(), before, "{failure}");
        assert_eq!(installed_versions(&paths), ["1.2.1"], "{failure}");
    }
}

#[test]
fn invalid_exact_version_does_not_create_updater_state() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    let updater = Updater::with_base_url(paths.clone(), "http://127.0.0.1:1").unwrap();
    for version in [
        "",
        "../bad",
        "v1.2.3",
        "1.2.3-01",
        "1.2.3-",
        "1.2.3-beta..1",
        "1.2.3-beta.01",
        "1.2.3-beta_1",
        "1.2.3-beta/1",
        "1.2.3+sha",
        "01.2.3",
        "latest",
    ] {
        assert!(matches!(
            updater.install_exact(version),
            Err(UpdateError::InvalidExactVersion)
        ));
    }
    assert!(!paths.sitter_dir.exists());
}

#[cfg(unix)]
#[test]
fn in_flight_channel_download_rejects_exact_request_before_acceptance() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    preinstall(&paths, "1.2.1", Channel::Stable);
    let requester_paths = paths.clone();
    let base = serve_release_with_archive_hook("1.2.3", b"channel C", move || {
        let exact = Updater::with_base_url(requester_paths.clone(), "http://127.0.0.1:1").unwrap();
        assert!(matches!(
            exact.install_exact("1.2.2"),
            Err(UpdateError::Busy)
        ));
        assert_eq!(
            state::load(&requester_paths.state_path)
                .current_version
                .as_deref(),
            Some("1.2.1")
        );
        assert!(!requester_paths.daemon_binary("1.2.2").exists());
    });
    let automatic = Updater::with_base_url(paths, base).unwrap();
    assert!(
        matches!(automatic.check_and_install(Channel::Stable).unwrap(), UpdateOutcome::Installed { version, .. } if version == "1.2.3")
    );
}

#[cfg(unix)]
#[test]
fn exact_install_never_reports_a_noncooperating_newer_winner_as_its_target() {
    let dir = tempfile::tempdir().unwrap();
    let paths = paths_in(dir.path());
    preinstall(&paths, "1.2.1", Channel::Stable);
    let writer_paths = paths.clone();
    let base = serve_release_with_archive_hook("1.2.2", b"B", move || {
        // Models an old external installer that does not know the new lock.
        preinstall(&writer_paths, "1.2.3", Channel::Stable);
    });
    let updater = Updater::with_base_url(paths.clone(), base).unwrap();
    assert!(
        matches!(updater.install_exact("1.2.2"), Err(UpdateError::Downgrade { installed, requested }) if installed == "1.2.3" && requested == "1.2.2")
    );
    assert_eq!(
        state::load(&paths.state_path).current_version.as_deref(),
        Some("1.2.3")
    );
}
