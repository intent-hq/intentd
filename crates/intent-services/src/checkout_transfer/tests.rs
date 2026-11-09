//! How a pull can go wrong, and what each test pins:
//!
//! 1. The source path leaves the checkout: `..`, an absolute path, a link
//!    to outside (file or folder), or `.git` at any depth in any case ->
//!    refused, nothing read.
//! 2. A link that stays inside the checkout is followed, for a file and
//!    inside a packed folder; `.git` and links that lead outside a folder
//!    are left out and counted.
//! 3. The file or packed folder is over the byte or entry cap -> refused by
//!    the source and by the destination, nothing written.
//! 4. Pieces arrive out of order, short, from another snapshot, or with
//!    other bytes -> the copy is refused and nothing is left behind.
//! 5. The link drops mid-transfer -> nothing is left at the destination
//!    (no part file, no parent it made); the snapshot expires.
//! 6. The destination leaves the checkout, writes into `.git`, puts a file
//!    onto a folder, a folder onto a file or a link, or a path onto itself
//!    -> refused before anything is fetched or written.
//! 7. A source serves at most 4 live snapshots; a transfer id answers only
//!    its own source and path.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use intent_core::node_link::{CheckoutEntryKind, CheckoutPiece};

use super::*;

const SRC: &str = "agent-src";

fn source(staging: &Path) -> PieceSource {
    PieceSource::new(staging.to_path_buf())
}

fn first(path: &str) -> PieceRequest {
    PieceRequest {
        path: path.to_owned(),
        offset: 0,
        length: 1024,
        transfer_id: None,
    }
}

fn read(src: &PieceSource, checkout: &Path, path: &str) -> Result<CheckoutPiece> {
    src.read(SRC, checkout, &first(path))
}

fn code(result: Result<impl std::fmt::Debug>) -> String {
    let error = result.expect_err("refused").to_string();
    error
        .split_once("file-pull-")
        .map(|(_, rest)| format!("file-pull-{}", rest.split(':').next().unwrap_or_default()))
        .unwrap_or(error)
}

/// Every entry under `dir`, relative and sorted, `.intent/pulls` left out
/// except for anything but its marker.
fn entries(dir: &Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            let rel = path
                .strip_prefix(dir)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            if path.is_dir() && !path.is_symlink() {
                stack.push(path);
            }
            if !matches!(
                rel.as_str(),
                ".intent" | ".intent/pulls" | ".intent/pulls/.gitignore"
            ) {
                found.push(rel);
            }
        }
    }
    found.sort();
    found
}

/// Pull `path` from `from` through `src` with pieces of `piece` bytes, as a
/// destination drives the loop over the link.
async fn pull(
    src: &Arc<PieceSource>,
    from: &Path,
    path: &str,
    dest: &Path,
    to: &str,
    piece: u64,
) -> Result<serde_json::Value> {
    let (src, from, path_owned) = (src.clone(), from.to_path_buf(), path.to_owned());
    let same = (from == dest).then_some(path);
    pull_into(
        dest,
        to,
        same,
        MAX_PULL_BYTES,
        move |offset, transfer_id| {
            let (src, from) = (src.clone(), from.clone());
            let request = PieceRequest {
                path: path_owned.clone(),
                offset,
                length: piece,
                transfer_id,
            };
            async move { src.read(SRC, &from, &request) }
        },
    )
    .await
}

#[cfg(unix)]
#[test]
fn source_paths_must_stay_inside_the_checkout() {
    let staging = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret.txt"), "secret").unwrap();
    let checkout = tempfile::tempdir().unwrap();
    let root = checkout.path();
    std::fs::create_dir_all(root.join("out/sub/.GIT")).unwrap();
    std::fs::write(root.join("out/report.txt"), "report").unwrap();
    std::fs::write(root.join("out/sub/.GIT/config"), "[remote]").unwrap();
    std::fs::create_dir_all(root.join(".git")).unwrap();
    std::fs::write(root.join(".git/config"), "[remote]").unwrap();
    std::os::unix::fs::symlink(outside.path().join("secret.txt"), root.join("leak.txt")).unwrap();
    std::os::unix::fs::symlink(outside.path(), root.join("leakdir")).unwrap();
    std::os::unix::fs::symlink(root.join(".git/config"), root.join("gitlink")).unwrap();
    std::os::unix::fs::symlink(root.join("out/report.txt"), root.join("inside.txt")).unwrap();
    let src = source(staging.path());

    for path in [
        "../secret.txt",
        "out/../../secret.txt",
        "out/../report.txt",
        "leak.txt",
        "leakdir",
        "leakdir/secret.txt",
        ".git/config",
        ".git",
        "out/sub/.GIT/config",
        "gitlink",
    ] {
        assert_eq!(
            code(read(&src, root, path)),
            "file-pull-outside-checkout",
            "{path}"
        );
    }
    let absolute = root.join("out/report.txt");
    assert_eq!(
        code(read(&src, root, absolute.to_str().unwrap())),
        "file-pull-outside-checkout"
    );
    assert_eq!(
        code(read(&src, root, "out/missing.txt")),
        "file-pull-not-found"
    );
    assert_eq!(
        code(read(&src, &root.join("gone"), "out/report.txt")),
        "file-pull-source-unavailable"
    );

    let followed = read(&src, root, "inside.txt").unwrap();
    assert_eq!(followed.kind, CheckoutEntryKind::File);
    assert_eq!(decode(&followed.data).unwrap(), b"report");
    assert_eq!(followed.size, 6);
    assert_eq!(
        followed.sha256.as_deref(),
        Some(sha256_of(b"report").as_str())
    );
    assert!(!followed.transfer_id.is_empty());
    assert!(
        entries(staging.path()).is_empty(),
        "a snapshot read whole in one piece is removed"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_folder_packs_inner_links_and_skips_git_and_escaping_links() {
    let staging = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret.txt"), "secret").unwrap();
    let from = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let root = from.path();
    std::fs::create_dir_all(root.join("bundle/nested/.Git")).unwrap();
    std::fs::write(root.join("bundle/nested/.Git/HEAD"), "ref").unwrap();
    std::fs::write(root.join("bundle/a.txt"), "a").unwrap();
    std::fs::write(root.join("shared.txt"), "shared").unwrap();
    std::os::unix::fs::symlink(root.join("shared.txt"), root.join("bundle/nested/link.txt"))
        .unwrap();
    std::os::unix::fs::symlink(
        outside.path().join("secret.txt"),
        root.join("bundle/leak.txt"),
    )
    .unwrap();
    std::os::unix::fs::symlink(root.join("bundle"), root.join("bundle/nested/up")).unwrap();
    let src = Arc::new(source(staging.path()));

    let result = pull(&src, root, "bundle", dest.path(), "got", 512)
        .await
        .unwrap();
    assert_eq!(result["kind"], "directory", "{result}");
    assert_eq!(result["files"], 2, "{result}");
    assert_eq!(
        result["skipped"], 3,
        ".Git, the escaping link and the loop: {result}"
    );
    assert_eq!(
        entries(dest.path()),
        ["got", "got/a.txt", "got/nested", "got/nested/link.txt"]
    );
    assert_eq!(
        std::fs::read_to_string(dest.path().join("got/nested/link.txt")).unwrap(),
        "shared"
    );
    assert!(!dest.path().join("got/nested/link.txt").is_symlink());
}

#[tokio::test]
async fn a_file_arrives_in_pieces_and_is_kept_only_when_whole() {
    let staging = tempfile::tempdir().unwrap();
    let from = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let bytes: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::create_dir_all(from.path().join("out")).unwrap();
    std::fs::write(from.path().join("out/data.bin"), &bytes).unwrap();
    let src = Arc::new(source(staging.path()));

    let result = pull(
        &src,
        from.path(),
        "out/data.bin",
        dest.path(),
        "in/deep/data.bin",
        4096,
    )
    .await
    .unwrap();
    assert_eq!(
        result,
        serde_json::json!({
            "path": "in/deep/data.bin", "kind": "file", "size": 10_000,
            "sha256": sha256_of(&bytes), "files": 1, "skipped": 0,
        })
    );
    assert_eq!(
        std::fs::read(dest.path().join("in/deep/data.bin")).unwrap(),
        bytes
    );
    assert_eq!(entries(dest.path()), ["in", "in/deep", "in/deep/data.bin"]);
    assert_eq!(
        std::fs::read_to_string(dest.path().join(".intent/pulls/.gitignore")).unwrap(),
        "*\n"
    );
    assert!(entries(staging.path()).is_empty());

    // Pulling again replaces the copy; an empty file copies too.
    std::fs::write(from.path().join("out/data.bin"), b"").unwrap();
    let empty = pull(
        &src,
        from.path(),
        "out/data.bin",
        dest.path(),
        "in/deep/data.bin",
        4096,
    )
    .await
    .unwrap();
    assert_eq!(empty["size"], 0);
    assert!(std::fs::read(dest.path().join("in/deep/data.bin"))
        .unwrap()
        .is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn a_folder_merges_into_an_existing_folder_with_its_modes() {
    use std::os::unix::fs::PermissionsExt as _;
    let staging = tempfile::tempdir().unwrap();
    let from = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let root = from.path();
    std::fs::create_dir_all(root.join("bundle/nested/empty")).unwrap();
    std::fs::write(root.join("bundle/a.txt"), "a").unwrap();
    std::fs::write(root.join("bundle/nested/run.sh"), "#!/bin/sh\n").unwrap();
    std::fs::set_permissions(
        root.join("bundle/nested/run.sh"),
        std::fs::Permissions::from_mode(0o4750),
    )
    .unwrap();
    let long = "n".repeat(120);
    std::fs::create_dir_all(root.join("bundle").join(&long)).unwrap();
    std::fs::write(root.join("bundle").join(&long).join("f.txt"), "long").unwrap();
    let big: Vec<u8> = (0..20_000u32).map(|i| (i % 13) as u8).collect();
    std::fs::write(root.join("bundle/nested/big.bin"), &big).unwrap();
    std::fs::create_dir_all(dest.path().join("got/keep")).unwrap();
    std::fs::write(dest.path().join("got/keep/mine.txt"), "mine").unwrap();
    std::fs::write(dest.path().join("got/a.txt"), "old").unwrap();
    let src = Arc::new(source(staging.path()));

    let result = pull(&src, root, "bundle", dest.path(), "got", 4096)
        .await
        .unwrap();
    assert_eq!(result["files"], 4, "{result}");
    let got = dest.path().join("got");
    assert_eq!(std::fs::read_to_string(got.join("a.txt")).unwrap(), "a");
    assert_eq!(std::fs::read(got.join("nested/big.bin")).unwrap(), big);
    assert_eq!(
        std::fs::read_to_string(got.join(&long).join("f.txt")).unwrap(),
        "long",
        "a long path rides a pax record"
    );
    assert!(got.join("nested/empty").is_dir());
    let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o7777;
    assert_eq!(mode(&got.join("nested/run.sh")) & 0o7000, 0, "no setuid");
    assert_eq!(mode(&got.join("nested/run.sh")) & 0o111, 0o111);
    assert_eq!(mode(&got.join("a.txt")) & 0o111, 0);
    assert_eq!(
        std::fs::read_to_string(got.join("keep/mine.txt")).unwrap(),
        "mine"
    );
    assert!(
        entries(staging.path()).is_empty(),
        "removed after its last piece"
    );
    let pulls: Vec<_> = std::fs::read_dir(dest.path().join(".intent/pulls"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(
        pulls,
        [".gitignore"],
        "no part file or unpacked folder left"
    );
}

#[tokio::test]
async fn the_cap_is_checked_by_the_source_and_the_destination() {
    let staging = tempfile::tempdir().unwrap();
    let from = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    std::fs::write(from.path().join("big.bin"), vec![0u8; 2048]).unwrap();
    std::fs::create_dir_all(from.path().join("dir")).unwrap();
    std::fs::write(from.path().join("dir/big.bin"), vec![0u8; 2048]).unwrap();
    std::fs::create_dir_all(from.path().join("many")).unwrap();
    for i in 0..5 {
        std::fs::write(from.path().join(format!("many/{i}")), "x").unwrap();
    }
    let small = PieceSource::with_caps(staging.path().to_path_buf(), 1024, 100);
    for path in ["big.bin", "dir"] {
        assert_eq!(
            code(small.read(SRC, from.path(), &first(path))),
            "file-pull-too-large",
            "{path}"
        );
    }
    let few = PieceSource::with_caps(staging.path().to_path_buf(), MAX_PULL_BYTES, 4);
    assert_eq!(
        code(few.read(SRC, from.path(), &first("many"))),
        "file-pull-too-large"
    );
    assert!(entries(staging.path()).is_empty(), "no snapshot kept");
    let at_cap = PieceSource::with_caps(staging.path().to_path_buf(), 2048, 100);
    assert!(at_cap.read(SRC, from.path(), &first("big.bin")).is_ok());

    // A source that ignores the cap is refused by the destination.
    let src = Arc::new(source(staging.path()));
    let from_path = from.path().to_path_buf();
    let refused = pull_into(
        dest.path(),
        "big.bin",
        None,
        1024,
        move |offset, transfer_id| {
            let (src, from) = (src.clone(), from_path.clone());
            let request = PieceRequest {
                path: "big.bin".into(),
                offset,
                length: 512,
                transfer_id,
            };
            async move { src.read(SRC, &from, &request) }
        },
    )
    .await;
    assert_eq!(code(refused), "file-pull-too-large");
    assert!(entries(dest.path()).is_empty());
}

/// Changes the piece a fetch returns at its call number.
type Tamper = Box<dyn Fn(usize, &mut CheckoutPiece) -> Result<()> + Send + Sync>;

/// A fetch that reads `path` from `from` and lets `tamper` change the
/// piece at its call number.
fn fetcher(
    src: &Arc<PieceSource>,
    from: &Path,
    path: &'static str,
    tamper: impl Fn(usize, &mut CheckoutPiece) -> Result<()> + Send + Sync + 'static,
) -> impl FnMut(
    u64,
    Option<String>,
) -> std::pin::Pin<Box<dyn Future<Output = Result<CheckoutPiece>> + Send>> {
    let (src, from) = (src.clone(), from.to_path_buf());
    let calls = Arc::new(AtomicUsize::new(0));
    let tamper = Arc::new(tamper);
    move |offset, transfer_id| {
        let (src, from, tamper) = (src.clone(), from.clone(), tamper.clone());
        let n = calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let mut piece = src.read(
                SRC,
                &from,
                &PieceRequest {
                    path: path.into(),
                    offset,
                    length: 4096,
                    transfer_id,
                },
            )?;
            tamper(n, &mut piece)?;
            Ok(piece)
        })
    }
}

#[tokio::test]
async fn a_dropped_link_or_bad_pieces_leave_nothing_behind() {
    let staging = tempfile::tempdir().unwrap();
    let from = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(from.path().join("dir")).unwrap();
    std::fs::write(from.path().join("dir/a.bin"), vec![7u8; 9000]).unwrap();
    std::fs::write(from.path().join("a.bin"), vec![7u8; 9000]).unwrap();
    let src = Arc::new(source(staging.path()));

    for path in ["a.bin", "dir"] {
        let lost = pull_into(
            dest.path(),
            "out/deep/x",
            None,
            MAX_PULL_BYTES,
            fetcher(&src, from.path(), path, |n, _| {
                if n == 1 {
                    Err(Error::Internal("node-link-closed".into()))
                } else {
                    Ok(())
                }
            }),
        )
        .await;
        assert!(lost.unwrap_err().to_string().contains("node-link-closed"));
        assert!(
            entries(dest.path()).is_empty(),
            "{path}: {:?}",
            entries(dest.path())
        );
    }

    let cases: Vec<(&str, Tamper)> = vec![
        (
            "other bytes",
            Box::new(|n, piece| {
                if n == 1 {
                    piece.data = encode(&vec![8u8; decode(&piece.data).unwrap().len()]);
                }
                Ok(())
            }),
        ),
        (
            "a gap",
            Box::new(|n, piece| {
                if n == 1 {
                    piece.offset += 1;
                }
                Ok(())
            }),
        ),
        (
            "another snapshot",
            Box::new(|n, piece| {
                if n == 1 {
                    piece.transfer_id = "t-other".into();
                }
                Ok(())
            }),
        ),
        (
            "a short source",
            Box::new(|n, piece| {
                if n == 1 {
                    piece.data = String::new();
                }
                Ok(())
            }),
        ),
        (
            "no sha256",
            Box::new(|n, piece| {
                if n == 0 {
                    piece.sha256 = None;
                }
                Ok(())
            }),
        ),
    ];
    for (what, tamper) in cases {
        // Each failed pull leaves its snapshot to expire; free the slots.
        src.forget(SRC);
        let refused = pull_into(
            dest.path(),
            "x",
            None,
            MAX_PULL_BYTES,
            fetcher(&src, from.path(), "a.bin", move |n, piece| tamper(n, piece)),
        )
        .await;
        assert_eq!(code(refused), "file-pull-mismatch", "{what}");
        assert!(
            entries(dest.path()).is_empty(),
            "{what}: {:?}",
            entries(dest.path())
        );
    }
}

#[test]
fn snapshots_expire_answer_only_their_own_transfer_and_are_capped_per_source() {
    let staging = tempfile::tempdir().unwrap();
    let from = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(from.path().join("dir")).unwrap();
    std::fs::write(from.path().join("dir/a.bin"), vec![1u8; 9000]).unwrap();
    std::fs::write(from.path().join("a.bin"), vec![1u8; 9000]).unwrap();
    let later = |path: &str, offset: u64, id: &str| PieceRequest {
        path: path.into(),
        offset,
        length: 1024,
        transfer_id: Some(id.into()),
    };

    let src = PieceSource::new(staging.path().to_path_buf()).idle_ttl(Duration::ZERO);
    let piece = src.read(SRC, from.path(), &first("dir")).unwrap();
    assert_eq!(entries(staging.path()).len(), 1);
    assert_eq!(
        code(src.read(SRC, from.path(), &later("dir", 1024, &piece.transfer_id))),
        "file-pull-transfer-expired"
    );
    assert!(entries(staging.path()).is_empty(), "expired");

    let src = PieceSource::new(staging.path().to_path_buf());
    let piece = src.read(SRC, from.path(), &first("a.bin")).unwrap();
    let id = piece.transfer_id.clone();
    for (who, request) in [
        ("other source", ("agent-other", later("a.bin", 1024, &id))),
        ("other path", (SRC, later("dir", 1024, &id))),
        ("unknown id", (SRC, later("a.bin", 1024, "t-unknown"))),
    ] {
        assert_eq!(
            code(src.read(request.0, from.path(), &request.1)),
            "file-pull-transfer-expired",
            "{who}"
        );
    }
    for (what, request) in [
        ("an offset at the end", later("a.bin", 9000, &id)),
        ("an id at offset 0", later("a.bin", 0, &id)),
        (
            "no id past offset 0",
            PieceRequest {
                transfer_id: None,
                ..later("a.bin", 1024, "")
            },
        ),
    ] {
        assert_eq!(
            code(src.read(SRC, from.path(), &request)),
            "file-pull-mismatch",
            "{what}"
        );
    }
    let rest = src
        .read(SRC, from.path(), &later("a.bin", 1024, &id))
        .unwrap();
    assert_eq!(rest.transfer_id, id);
    assert!(rest.sha256.is_none());

    for _ in 0..3 {
        src.read(SRC, from.path(), &first("a.bin")).unwrap();
    }
    assert_eq!(
        code(src.read(SRC, from.path(), &first("a.bin"))),
        "file-pull-busy"
    );
    assert!(src
        .read("agent-other", from.path(), &first("a.bin"))
        .is_ok());
    src.forget(SRC);
    assert!(src.read(SRC, from.path(), &first("a.bin")).is_ok());
    drop(src);
    assert!(
        entries(staging.path()).is_empty(),
        "dropping the source clears it"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn the_destination_must_stay_inside_the_checkout_and_fit_what_is_there() {
    let staging = tempfile::tempdir().unwrap();
    let from = tempfile::tempdir().unwrap();
    let dest = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(from.path().join("a.txt"), "a").unwrap();
    std::fs::create_dir_all(from.path().join("dir/sub")).unwrap();
    std::fs::write(from.path().join("dir/sub/b.txt"), "b").unwrap();
    std::os::unix::fs::symlink(outside.path(), dest.path().join("escape")).unwrap();
    std::fs::create_dir_all(dest.path().join("taken")).unwrap();
    std::fs::create_dir_all(dest.path().join("merge")).unwrap();
    std::os::unix::fs::symlink(outside.path(), dest.path().join("merge/sub")).unwrap();
    std::fs::write(dest.path().join("file"), "f").unwrap();
    std::fs::create_dir_all(dest.path().join(".intent")).unwrap();
    std::os::unix::fs::symlink(dest.path().join(".intent"), dest.path().join("meta")).unwrap();
    std::os::unix::fs::symlink(outside.path().join("a.txt"), dest.path().join("link.txt")).unwrap();
    let src = Arc::new(source(staging.path()));
    let asked = Arc::new(AtomicUsize::new(0));
    let absolute = outside.path().join("a.txt");
    for to in [
        "../a.txt",
        "escape/a.txt",
        ".git/hooks/pre-commit",
        "sub/.GIT/config",
        ".intent/x.txt",
        ".INTENT/pulls/x.part",
        "meta/pulls/x.part",
        absolute.to_str().unwrap(),
        "",
    ] {
        let counted = asked.clone();
        let refused = pull_into(dest.path(), to, None, MAX_PULL_BYTES, move |_, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            async { Err::<CheckoutPiece, _>(Error::Internal("never asked".into())) }
        })
        .await;
        assert_eq!(code(refused), "file-pull-outside-checkout", "{to}");
    }
    assert_eq!(asked.load(Ordering::SeqCst), 0, "refused before any fetch");

    for (path, to, expected) in [
        ("a.txt", "taken", "file-pull-destination-conflict"),
        ("dir", "file", "file-pull-destination-conflict"),
        ("dir", "file/sub", "file-pull-destination-conflict"),
        ("dir", "link.txt", "file-pull-outside-checkout"),
        ("dir", "merge", "file-pull-outside-checkout"),
    ] {
        assert_eq!(
            code(pull(&src, from.path(), path, dest.path(), to, 1024).await),
            expected,
            "{path} -> {to}"
        );
    }
    // Onto itself, or a folder into itself, in one checkout.
    for (path, to) in [("a.txt", "a.txt"), ("dir", "dir/sub/copy")] {
        assert_eq!(
            code(pull(&src, from.path(), path, from.path(), to, 1024).await),
            "file-pull-destination-conflict",
            "{path} -> {to}"
        );
    }
    // A file replaces a link as a link, never writing through it.
    pull(&src, from.path(), "a.txt", dest.path(), "link.txt", 1024)
        .await
        .unwrap();
    assert!(!dest.path().join("link.txt").is_symlink());
    assert!(
        entries(outside.path()).is_empty(),
        "nothing written outside"
    );
}
