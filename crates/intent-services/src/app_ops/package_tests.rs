//! App packages (PROTOCOL §5.60 "App packages", "Package upload"): the
//! `app-package` upload, `app.install { uploadId }`, hash identity with a
//! workspace install, every hostile archive the contract refuses, the
//! bundle limits inside a package and the caller checks.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use base64::Engine as _;
use intent_core::{with_caller, AgentId, Caller, Error, HostRole, PrincipalId, WorkspaceApi as _};
use intent_store::Store;
use serde_json::{json, Map, Value};
use sha2::Digest as _;

use crate::Services;

const MIB: usize = 1024 * 1024;

struct Fx {
    _dir: tempfile::TempDir,
    svc: Services,
    ws: WorkspaceIdStr,
    root: PathBuf,
}

type WorkspaceIdStr = String;

async fn fx() -> Fx {
    let dir = crate::test_support::test_tempdir("app-package-");
    let store = Store::open(&dir.path().join("store.db")).await.unwrap();
    let ws = intent_core::WorkspaceId("ws-apps".to_string());
    let root = dir.path().join("checkout");
    std::fs::create_dir_all(&root).unwrap();
    let mut row = crate::tests::workspace(&ws);
    row.worktree_path = Some(root.to_string_lossy().into_owned());
    store.insert_workspace(&row).await.unwrap();
    let svc = Services::new_isolated(store)
        .with_apps_dir(&dir.path().join("data"))
        .with_workspaces_root(dir.path().join("workspaces"));
    Fx {
        _dir: dir,
        svc,
        ws: ws.0,
        root,
    }
}

fn owner() -> Caller {
    Caller::Wire {
        principal_id: PrincipalId::new(),
        host_role: HostRole::Owner,
    }
}

fn agent() -> Caller {
    Caller::Agent {
        agent_id: AgentId("agent-1".into()),
    }
}

fn guest() -> Caller {
    Caller::Wire {
        principal_id: PrincipalId::new(),
        host_role: HostRole::Guest,
    }
}

fn params(v: &Value) -> Map<String, Value> {
    v.as_object().cloned().unwrap_or_default()
}

fn code(e: &Error) -> String {
    match e {
        Error::App(app) => app.data_code().to_owned(),
        Error::Forbidden(detail) => format!("forbidden: {detail}"),
        other => format!("other: {other:?}"),
    }
}

fn manifest(id: &str) -> String {
    json!({
        "manifestVersion": 1,
        "id": id,
        "name": format!("App {id}"),
        "version": "1.0.0",
        "permissions": { "level": 1, "features": [] },
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// A raw ZIP writer: full control over names, modes, flags, methods, sizes
// and CRCs, so each hostile archive can be built exactly.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct RawEntry {
    name: Vec<u8>,
    data: Vec<u8>,
    /// 0 stored, 8 deflate, anything else as given.
    method: u16,
    /// Declared uncompressed size; `None` = the real size.
    declared: Option<u32>,
    /// CRC-32 override; `None` = the real CRC.
    crc: Option<u32>,
    flags: u16,
    /// Unix `st_mode` (made by Unix); `None` = DOS attributes.
    mode: Option<u32>,
}

impl RawEntry {
    fn file(name: &str, content: &[u8]) -> Self {
        Self {
            name: name.as_bytes().to_vec(),
            data: content.to_vec(),
            method: 8,
            declared: None,
            crc: None,
            flags: 0,
            mode: Some(0o100_644),
        }
    }

    fn dir(name: &str) -> Self {
        Self {
            name: name.as_bytes().to_vec(),
            data: Vec::new(),
            method: 0,
            declared: None,
            crc: None,
            flags: 0,
            mode: Some(0o040_755),
        }
    }
}

fn deflate(data: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(data).unwrap();
    enc.finish().unwrap()
}

fn crc(data: &[u8]) -> u32 {
    let mut c = flate2::Crc::new();
    c.update(data);
    c.sum()
}

fn u32_of(n: usize) -> u32 {
    u32::try_from(n).expect("a test archive field fits in u32")
}

fn u16_of(n: usize) -> u16 {
    u16::try_from(n).expect("a test archive field fits in u16")
}

fn zip_raw(entries: &[RawEntry], disk_number: u16) -> Vec<u8> {
    let mut out = Vec::new();
    let mut central = Vec::new();
    for e in entries {
        let payload = if e.method == 8 {
            deflate(&e.data)
        } else {
            e.data.clone()
        };
        let size = e.declared.unwrap_or_else(|| u32_of(e.data.len()));
        let crc = e.crc.unwrap_or_else(|| crc(&e.data));
        let offset = u32_of(out.len());
        let name_len = u16_of(e.name.len());
        // Local file header.
        out.extend_from_slice(&0x0403_4b50_u32.to_le_bytes());
        out.extend_from_slice(&20u16.to_le_bytes());
        out.extend_from_slice(&e.flags.to_le_bytes());
        out.extend_from_slice(&e.method.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes()); // time + date
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&u32_of(payload.len()).to_le_bytes());
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(&name_len.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&e.name);
        out.extend_from_slice(&payload);
        // Central directory header.
        let (made_by, external) = match e.mode {
            Some(mode) => (0x0314, mode << 16), // made by Unix, version 2.0
            None => (20u16, 0u32),
        };
        central.extend_from_slice(&0x0201_4b50_u32.to_le_bytes());
        central.extend_from_slice(&made_by.to_le_bytes());
        central.extend_from_slice(&20u16.to_le_bytes());
        central.extend_from_slice(&e.flags.to_le_bytes());
        central.extend_from_slice(&e.method.to_le_bytes());
        central.extend_from_slice(&0u32.to_le_bytes());
        central.extend_from_slice(&crc.to_le_bytes());
        central.extend_from_slice(&u32_of(payload.len()).to_le_bytes());
        central.extend_from_slice(&size.to_le_bytes());
        central.extend_from_slice(&name_len.to_le_bytes());
        central.extend_from_slice(&0u16.to_le_bytes()); // extra
        central.extend_from_slice(&0u16.to_le_bytes()); // comment
        central.extend_from_slice(&0u16.to_le_bytes()); // disk start
        central.extend_from_slice(&0u16.to_le_bytes()); // internal attrs
        central.extend_from_slice(&external.to_le_bytes());
        central.extend_from_slice(&offset.to_le_bytes());
        central.extend_from_slice(&e.name);
    }
    let cd_offset = u32_of(out.len());
    let count = u16_of(entries.len());
    out.extend_from_slice(&central);
    out.extend_from_slice(&0x0605_4b50_u32.to_le_bytes());
    out.extend_from_slice(&disk_number.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // disk with the central directory
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&u32_of(central.len()).to_le_bytes());
    out.extend_from_slice(&cd_offset.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out
}

fn zip(entries: &[RawEntry]) -> Vec<u8> {
    zip_raw(entries, 0)
}

/// A valid package of app `id` with extra entries appended.
fn package(id: &str, extra: &[RawEntry]) -> Vec<u8> {
    let mut entries = vec![
        RawEntry::file("intent-app.json", manifest(id).as_bytes()),
        RawEntry::file("index.html", b"<h1>app</h1>"),
    ];
    entries.extend_from_slice(extra);
    zip(&entries)
}

impl Fx {
    async fn upload_as(&self, caller: Caller, bytes: &[u8]) -> intent_core::Result<String> {
        let sha = crate::app_ops::bundle::hex(&sha2::Sha256::digest(bytes));
        let begun = with_caller(
            caller.clone(),
            self.svc
                .file_app_package_upload_begin("pkg.intentapp".into(), bytes.len() as u64, sha),
        )
        .await?;
        let upload_id = begun["uploadId"].as_str().unwrap().to_owned();
        for (seq, chunk) in bytes.chunks(4 * MIB).enumerate() {
            let data = base64::engine::general_purpose::STANDARD.encode(chunk);
            with_caller(
                caller.clone(),
                self.svc
                    .file_attachment_upload_chunk(upload_id.clone(), seq as u64, data),
            )
            .await?;
        }
        Ok(upload_id)
    }

    async fn upload(&self, bytes: &[u8]) -> String {
        self.upload_as(owner(), bytes).await.expect("upload")
    }

    async fn install_as(&self, caller: Caller, p: Value) -> intent_core::Result<Value> {
        with_caller(caller, self.svc.app_call_op("app.install", &params(&p))).await
    }

    async fn install_upload(
        &self,
        upload_id: &str,
        replace: bool,
        dry_run: bool,
    ) -> intent_core::Result<Value> {
        self.install_as(
            owner(),
            json!({ "uploadId": upload_id, "replace": replace, "dryRun": dry_run }),
        )
        .await
    }

    /// Upload `bytes` and install them; the refusal code, or `"ok"`.
    async fn refusal(&self, bytes: &[u8]) -> String {
        let upload_id = self.upload(bytes).await;
        let got = match self.install_upload(&upload_id, false, true).await {
            Ok(_) => "ok".into(),
            Err(e) => code(&e),
        };
        with_caller(owner(), self.svc.file_attachment_upload_abort(upload_id))
            .await
            .unwrap();
        got
    }

    fn upload_live(&self, upload_id: &str) -> bool {
        self.svc.is_app_package_upload(upload_id)
    }

    fn write_folder(&self, rel: &str, files: &[(&str, &[u8])]) {
        for (path, content) in files {
            let p = self.root.join(rel).join(path);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, content).unwrap();
        }
    }
}

// ---------------------------------------------------------------------------
// Hash identity and the install flow
// ---------------------------------------------------------------------------

/// A package and a workspace install of the same files give the same
/// hash, in both package layouts; zip metadata is outside the hash.
#[intent_test_macros::daemon_test]
async fn package_and_workspace_installs_of_the_same_files_have_the_same_hash() {
    let f = fx().await;
    let man = manifest("same-hash");
    let files: Vec<(&str, &[u8])> = vec![
        ("intent-app.json", man.as_bytes()),
        ("index.html", b"<h1>app</h1>"),
        ("js/app.js", b"console.log(1)"),
        ("assets/img/a.png", &[0x89, b'P', b'N', b'G', 0, 1, 2, 3]),
    ];
    f.write_folder("apps/same", &files);
    let ws = f
        .install_as(
            owner(),
            json!({ "workspaceId": f.ws, "path": "apps/same", "dryRun": true }),
        )
        .await
        .unwrap();
    let expected = ws["app"]["bundleHash"].as_str().unwrap().to_owned();

    // Root layout, stored entries, Unix modes.
    let root_layout: Vec<RawEntry> = files
        .iter()
        .map(|(p, c)| RawEntry {
            method: 0,
            ..RawEntry::file(p, c)
        })
        .collect();
    // One top folder with directory entries, DOS attributes, deflate, a
    // different entry order and macOS resource forks.
    let mut folder_layout = vec![
        RawEntry::dir("same/"),
        RawEntry::dir("same/js/"),
        RawEntry::file("__MACOSX/same/._index.html", b"fork"),
    ];
    for (p, c) in files.iter().rev() {
        folder_layout.push(RawEntry {
            mode: None,
            ..RawEntry::file(&format!("same/{p}"), c)
        });
    }
    for entries in [root_layout, folder_layout] {
        let upload_id = f.upload(&zip(&entries)).await;
        let got = f.install_upload(&upload_id, false, true).await.unwrap();
        assert_eq!(got["app"]["bundleHash"], json!(expected));
        assert_eq!(got["app"]["fileCount"], json!(4));
        assert_eq!(got["app"]["source"], Value::Null, "a package has no source");
        assert_eq!(got["dryRun"], json!(true));
    }
}

/// `dryRun` and failures keep the upload; a successful install ends it;
/// the result and events match a workspace install.
#[intent_test_macros::daemon_test]
async fn an_upload_survives_dry_runs_and_failures_and_ends_on_install() {
    let f = fx().await;
    let bytes = package("flow-app", &[]);
    let upload_id = f.upload(&bytes).await;

    let preview = f.install_upload(&upload_id, false, true).await.unwrap();
    assert_eq!(preview["app"]["approval"]["status"], json!("pending"));
    assert!(f.upload_live(&upload_id), "dryRun keeps the upload");

    let installed = f.install_upload(&upload_id, false, false).await.unwrap();
    assert_eq!(installed["replaced"], json!(false));
    assert_eq!(installed["app"]["bundleHash"], preview["app"]["bundleHash"]);
    assert_eq!(installed["app"]["source"], Value::Null);
    assert!(!f.upload_live(&upload_id), "an install ends the upload");
    let again = f
        .install_upload(&upload_id, false, false)
        .await
        .unwrap_err();
    assert_eq!(code(&again), "upload-invalid");
    let aborted = with_caller(owner(), f.svc.file_attachment_upload_abort(upload_id))
        .await
        .unwrap();
    assert_eq!(aborted["aborted"], json!(false));

    // app-exists keeps the upload, and the retry with replace installs.
    let second = f.upload(&bytes).await;
    let exists = f.install_upload(&second, false, false).await.unwrap_err();
    assert_eq!(code(&exists), "app-exists");
    assert!(f.upload_live(&second), "a failure keeps the upload");
    let replaced = f.install_upload(&second, true, false).await.unwrap();
    assert_eq!(replaced["replaced"], json!(true));
    assert_eq!(
        replaced["approvalReset"],
        json!(false),
        "same files, same hash"
    );

    // The library lists the app.
    let list = with_caller(owner(), f.svc.app_call_op("app.list", &Map::new()))
        .await
        .unwrap();
    assert_eq!(list["apps"][0]["id"], json!("flow-app"));
}

#[intent_test_macros::daemon_test]
async fn upload_state_and_source_params_are_checked() {
    let f = fx().await;
    let bytes = package("state-app", &[]);
    // Incomplete: begin declares more bytes than were sent.
    let sha = crate::app_ops::bundle::hex(&sha2::Sha256::digest(&bytes));
    let begun = with_caller(
        owner(),
        f.svc
            .file_app_package_upload_begin("p.zip".into(), bytes.len() as u64 + 1, sha),
    )
    .await
    .unwrap();
    let short = begun["uploadId"].as_str().unwrap().to_owned();
    let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
    with_caller(
        owner(),
        f.svc.file_attachment_upload_chunk(short.clone(), 0, data),
    )
    .await
    .unwrap();
    let e = f.install_upload(&short, false, false).await.unwrap_err();
    assert_eq!(code(&e), "upload-invalid");

    // A SHA-256 mismatch.
    let begun = with_caller(
        owner(),
        f.svc
            .file_app_package_upload_begin("p.zip".into(), bytes.len() as u64, "0".repeat(64)),
    )
    .await
    .unwrap();
    let wrong = begun["uploadId"].as_str().unwrap().to_owned();
    let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
    with_caller(
        owner(),
        f.svc.file_attachment_upload_chunk(wrong.clone(), 0, data),
    )
    .await
    .unwrap();
    let e = f.install_upload(&wrong, false, false).await.unwrap_err();
    assert_eq!(code(&e), "upload-invalid");
    assert!(f.upload_live(&wrong));

    // At most two live app-package uploads.
    let e = f.upload_as(owner(), &bytes).await.unwrap_err();
    assert!(
        e.to_string()
            .contains("app package uploads are in progress"),
        "{e}"
    );
    for id in [&short, &wrong] {
        with_caller(owner(), f.svc.file_attachment_upload_abort(id.clone()))
            .await
            .unwrap();
    }

    // Unknown upload, and both sources at once.
    let e = f
        .install_upload("upload-nope", false, false)
        .await
        .unwrap_err();
    assert_eq!(code(&e), "upload-invalid");
    let e = f
        .install_as(
            owner(),
            json!({ "uploadId": "upload-nope", "workspaceId": f.ws, "path": "x" }),
        )
        .await
        .unwrap_err();
    assert_eq!(code(&e), "invalid-params");
    let e = f
        .install_as(owner(), json!({ "path": "x" }))
        .await
        .unwrap_err();
    assert_eq!(code(&e), "invalid-params");

    // An attachment upload is not an app package; an app package does not commit.
    let sha = crate::app_ops::bundle::hex(&sha2::Sha256::digest(b"x"));
    let attachment = with_caller(
        owner(),
        f.svc.file_attachment_upload_begin(
            intent_core::WorkspaceId(f.ws.clone()),
            "a.txt".into(),
            1,
            sha,
            None,
            None,
        ),
    )
    .await
    .unwrap();
    let attachment = attachment["uploadId"].as_str().unwrap().to_owned();
    let e = f
        .install_upload(&attachment, false, false)
        .await
        .unwrap_err();
    assert_eq!(code(&e), "upload-invalid");
    let pkg = f.upload(&bytes).await;
    let e = with_caller(owner(), f.svc.file_attachment_upload_commit(pkg.clone()))
        .await
        .unwrap_err();
    assert!(e.to_string().contains("use app.install"), "{e}");
    assert!(f.upload_live(&pkg));

    // The size cap at begin.
    let e = with_caller(
        owner(),
        f.svc.file_app_package_upload_begin(
            "big.zip".into(),
            super::package::MAX_PACKAGE_BYTES + 1,
            "0".repeat(64),
        ),
    )
    .await
    .unwrap_err();
    assert!(matches!(e, Error::InvalidParams(_)), "{e:?}");
}

// ---------------------------------------------------------------------------
// Callers
// ---------------------------------------------------------------------------

/// Agents and guests never stage or install a package; an app-package
/// upload looks unknown to them.
#[intent_test_macros::daemon_test]
async fn agents_and_guests_are_refused_packages() {
    let f = fx().await;
    let bytes = package("caller-app", &[]);
    let e = f.upload_as(agent(), &bytes).await.unwrap_err();
    assert_eq!(code(&e), "forbidden: user-only");
    let e = f.upload_as(guest(), &bytes).await.unwrap_err();
    assert!(matches!(e, Error::Forbidden(_)), "{e:?}");

    let upload_id = f.upload(&bytes).await;
    let e = f
        .install_as(agent(), json!({ "uploadId": upload_id }))
        .await
        .unwrap_err();
    assert_eq!(code(&e), "forbidden: user-only");
    let e = f
        .install_as(guest(), json!({ "uploadId": upload_id }))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Forbidden(_)), "{e:?}");

    for caller in [agent(), guest()] {
        let data = base64::engine::general_purpose::STANDARD.encode(b"x");
        let e = with_caller(
            caller.clone(),
            f.svc
                .file_attachment_upload_chunk(upload_id.clone(), 9, data),
        )
        .await
        .unwrap_err();
        assert!(matches!(e, Error::NotFound(_)), "{e:?}");
        let aborted = with_caller(
            caller,
            f.svc.file_attachment_upload_abort(upload_id.clone()),
        )
        .await
        .unwrap();
        assert_eq!(aborted["aborted"], json!(false));
        assert!(
            f.upload_live(&upload_id),
            "the upload survives a foreign abort"
        );
    }
    // The agent may still install from its workspace.
    f.write_folder(
        "apps/agent",
        &[
            ("intent-app.json", manifest("agent-app").as_bytes()),
            ("index.html", b"x"),
        ],
    );
    f.install_as(
        agent(),
        json!({ "workspaceId": f.ws, "path": "apps/agent" }),
    )
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------
// Hostile archives
// ---------------------------------------------------------------------------

fn named(name: &[u8]) -> RawEntry {
    RawEntry {
        name: name.to_vec(),
        ..RawEntry::file("x", b"evil")
    }
}

#[intent_test_macros::daemon_test]
async fn hostile_archives_are_refused() {
    let f = fx().await;
    let mut cases: Vec<(&str, Vec<u8>, &str)> = Vec::new();
    for name in [
        &b"../evil.js"[..],
        b"/etc/evil.js",
        b"a\\..\\evil.js",
        b"C:/evil.js",
        b"js//evil.js",
        b"./evil.js",
        b"js/../../evil.js",
        b"js/\x01evil.js",
        b"js/ev\0il.js",
        b"js/\xff\xfe.js",
    ] {
        cases.push((
            "path escape",
            package("hostile", &[named(name)]),
            "invalid-package",
        ));
    }
    let symlink = RawEntry {
        mode: Some(0o120_777),
        ..RawEntry::file("link.js", b"/etc/passwd")
    };
    cases.push(("symlink", package("hostile", &[symlink]), "invalid-bundle"));
    let fifo = RawEntry {
        mode: Some(0o010_644),
        ..RawEntry::file("pipe", b"")
    };
    cases.push(("fifo", package("hostile", &[fifo]), "invalid-bundle"));
    let encrypted = RawEntry {
        flags: 1,
        ..RawEntry::file("secret.js", b"x")
    };
    cases.push((
        "encrypted",
        package("hostile", &[encrypted]),
        "invalid-package",
    ));
    let bzip2 = RawEntry {
        method: 12,
        ..RawEntry::file("b.js", b"x")
    };
    cases.push(("bzip2", package("hostile", &[bzip2]), "invalid-package"));
    let multi_disk = zip_raw(
        &[
            RawEntry::file("intent-app.json", manifest("hostile").as_bytes()),
            RawEntry::file("index.html", b"x"),
        ],
        1,
    );
    cases.push(("multi-disk", multi_disk, "invalid-package"));
    cases.push((
        "duplicate",
        package("hostile", &[RawEntry::file("index.html", b"again")]),
        "invalid-package",
    ));
    cases.push((
        "file and folder",
        package(
            "hostile",
            &[RawEntry::file("js", b"x"), RawEntry::file("js/a.js", b"y")],
        ),
        "invalid-package",
    ));
    cases.push((
        "two top folders",
        zip(&[
            RawEntry::file("a/intent-app.json", manifest("hostile").as_bytes()),
            RawEntry::file("b/index.html", b"x"),
        ]),
        "invalid-package",
    ));
    cases.push((
        "manifest two folders deep",
        zip(&[RawEntry::file(
            "a/b/intent-app.json",
            manifest("hostile").as_bytes(),
        )]),
        "invalid-package",
    ));
    cases.push((
        "not a zip",
        b"PK this is not a zip archive".to_vec(),
        "invalid-package",
    ));
    // Declares 10 bytes, inflates to 1 MiB.
    let liar = RawEntry {
        declared: Some(10),
        ..RawEntry::file("bomb.js", &vec![0u8; MIB])
    };
    cases.push(("size lie", package("hostile", &[liar]), "invalid-package"));
    let bad_crc = RawEntry {
        crc: Some(0xdead_beef),
        ..RawEntry::file("crc.js", b"content")
    };
    cases.push(("bad crc", package("hostile", &[bad_crc]), "invalid-package"));
    let many: Vec<RawEntry> = (0..=super::package::MAX_ENTRIES)
        .map(|i| RawEntry::dir(&format!("d{i}/")))
        .collect();
    cases.push((
        "too many entries",
        package("hostile", &many),
        "invalid-package",
    ));

    for (label, bytes, want) in cases {
        let got = f.refusal(&bytes).await;
        assert_eq!(got, want, "{label}");
    }
    // Nothing was installed and nothing escaped the scratch directory.
    let list = with_caller(owner(), f.svc.app_call_op("app.list", &Map::new()))
        .await
        .unwrap();
    assert_eq!(list["apps"], json!([]));
    assert!(!std::env::temp_dir().join("evil.js").exists()); // tmp-hygiene: allow — only checks that nothing escaped
}

/// The bundle limits hold inside a package, from declared sizes and from
/// what entries really inflate to.
#[intent_test_macros::daemon_test]
async fn bundle_limits_hold_inside_a_package() {
    let f = fx().await;
    let too_many: Vec<RawEntry> = (0..2000)
        .map(|i| RawEntry::file(&format!("f/{i}.txt"), b"x"))
        .collect();
    assert_eq!(
        f.refusal(&package("limits", &too_many)).await,
        "invalid-bundle"
    );

    let big_file = RawEntry::file("big.bin", &vec![0u8; 16 * MIB + 1]);
    assert_eq!(
        f.refusal(&package("limits", &[big_file])).await,
        "invalid-bundle"
    );

    let total: Vec<RawEntry> = (0..5)
        .map(|i| RawEntry::file(&format!("part{i}.bin"), &vec![0u8; 13 * MIB]))
        .collect();
    assert_eq!(
        f.refusal(&package("limits", &total)).await,
        "invalid-bundle"
    );

    let long_name = format!("{}.js", "a".repeat(600));
    assert_eq!(
        f.refusal(&package("limits", &[RawEntry::file(&long_name, b"x")]))
            .await,
        "invalid-bundle"
    );

    let huge_manifest = zip(&[RawEntry::file(
        "intent-app.json",
        &vec![b' '; 64 * 1024 + 1],
    )]);
    assert_eq!(f.refusal(&huge_manifest).await, "invalid-manifest");

    // At the limits: 2000 files, a 16 MiB file, accepted.
    let mut at_limit: Vec<RawEntry> = (0..1997)
        .map(|i| RawEntry::file(&format!("f/{i}.txt"), b"x"))
        .collect();
    at_limit.push(RawEntry::file("max.bin", &vec![0u8; 16 * MIB]));
    assert_eq!(f.refusal(&package("limits", &at_limit)).await, "ok");
}

/// The unpacked files land only under the scratch directory.
#[test]
fn unpack_writes_only_inside_the_destination() {
    let dir = crate::test_support::test_tempdir("app-package-unpack-");
    let pkg: &Path = &dir.path().join("p.zip");
    std::fs::write(
        pkg,
        zip(&[
            RawEntry::dir("top/"),
            RawEntry::file("top/intent-app.json", manifest("unpack").as_bytes()),
            RawEntry::file("top/a/b/c.js", b"c"),
        ]),
    )
    .unwrap();
    let dest = dir.path().join("dest");
    std::fs::create_dir(&dest).unwrap();
    super::package::unpack_package(pkg, &dest).unwrap();
    assert!(dest.join("intent-app.json").is_file());
    assert_eq!(std::fs::read(dest.join("a/b/c.js")).unwrap(), b"c");
    assert!(!dest.join("top").exists(), "the top folder is removed");
}
