//! App bundles (PROTOCOL §5.60 "App format"): the walk, limits, manifest
//! rules and the bundle hash. Pure except for [`read_bundle_dir`], which
//! reads the directory once: the hash, the manifest and the copy written to
//! the library all come from the same bytes, so a file that changes during
//! install cannot be approved under another file's hash.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use intent_core::apps::{AppError, APP_FEATURES};
use intent_core::Error;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

pub(crate) const MANIFEST_FILE: &str = "intent-app.json";
/// The detached signature file at the bundle root (reserved in M1): outside
/// the hash, stored with the record, never served as a bundle file.
pub(crate) const SIGNATURE_FILE: &str = "intent-app.sig";
pub(crate) const MAX_SIGNATURE_BYTES: u64 = 16 * 1024;
pub(crate) const MAX_FILES: usize = 2000;
pub(crate) const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const MAX_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
pub(crate) const MAX_PATH_BYTES: usize = 512;

/// One regular file of a bundle, with its content.
pub(crate) struct BundleFile {
    /// Bundle path: relative, `/` separators, as stored on disk.
    pub(crate) path: String,
    pub(crate) bytes: Vec<u8>,
}

/// A read bundle: files sorted by path bytes, the hash and totals. The root
/// `intent-app.sig` is not in `files` (nor in the hash or totals).
pub(crate) struct Bundle {
    pub(crate) files: Vec<BundleFile>,
    /// The root `intent-app.sig` bytes, if present.
    pub(crate) signature: Option<Vec<u8>>,
    pub(crate) hash: String,
    pub(crate) size_bytes: u64,
}

impl Bundle {
    pub(crate) fn file(&self, path: &str) -> Option<&BundleFile> {
        self.files
            .binary_search_by(|f| f.path.as_bytes().cmp(path.as_bytes()))
            .ok()
            .map(|i| &self.files[i])
    }
}

fn invalid_bundle(detail: impl Into<String>, path: Option<&str>) -> Error {
    let detail = detail.into();
    AppError::params(
        "invalid-bundle",
        format!("invalid bundle: {detail}"),
        json!({ "detail": detail, "path": path }),
    )
    .into_error()
}

fn invalid_manifest(detail: impl Into<String>, field: &str) -> Error {
    let detail = detail.into();
    AppError::params(
        "invalid-manifest",
        format!("invalid manifest: {detail}"),
        json!({ "detail": detail, "field": field }),
    )
    .into_error()
}

/// Whether `path` follows the bundle path rules (§5.60 "App format"):
/// relative, `/` separators, no `..` (or `.` / empty) segment, no leading
/// `/`, no `\`, no NUL, at most 512 UTF-8 bytes.
pub(crate) fn is_bundle_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= MAX_PATH_BYTES
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.contains('\0')
        && path
            .split('/')
            .all(|seg| !seg.is_empty() && seg != "." && seg != "..")
}

/// The bundle hash (§5.60 "Bundle hash"): files sorted by path bytes; for
/// each, the path bytes, `0x00`, the length as ASCII decimal, `0x00`, the
/// content. `"sha256:"` + lowercase hex.
pub(crate) fn bundle_hash<'a>(files: impl IntoIterator<Item = (&'a str, &'a [u8])>) -> String {
    let mut sorted: Vec<(&str, &[u8])> = files.into_iter().collect();
    sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut hasher = Sha256::new();
    for (path, bytes) in sorted {
        hasher.update(path.as_bytes());
        hasher.update([0u8]);
        hasher.update(bytes.len().to_string().as_bytes());
        hasher.update([0u8]);
        hasher.update(bytes);
    }
    format!("sha256:{}", hex(&hasher.finalize()))
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// The hex part of a `sha256:` hash (the library directory name).
pub(crate) fn hash_hex(hash: &str) -> &str {
    hash.strip_prefix("sha256:").unwrap_or(hash)
}

/// Walk and read `root` (an existing directory). Refuses symlinks and
/// special files anywhere under it, non-UTF-8 or rule-breaking names, and
/// every limit, from metadata before any content is read. Empty
/// directories are ignored.
pub(crate) fn read_bundle_dir(root: &Path) -> Result<Bundle, Error> {
    let mut entries: Vec<(String, PathBuf, u64)> = Vec::new();
    let mut total: u64 = 0;
    let mut stack: Vec<(PathBuf, String)> = vec![(root.to_path_buf(), String::new())];
    while let Some((dir, prefix)) = stack.pop() {
        let read = std::fs::read_dir(&dir)
            .map_err(|e| invalid_bundle(format!("cannot read a directory: {e}"), None))?;
        for entry in read {
            let entry =
                entry.map_err(|e| invalid_bundle(format!("cannot read a directory: {e}"), None))?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                return Err(invalid_bundle("a file name is not UTF-8", None));
            };
            let path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            if !is_bundle_path(&path) {
                return Err(invalid_bundle("a bundle path breaks the path rules", None));
            }
            let meta = std::fs::symlink_metadata(entry.path())
                .map_err(|e| invalid_bundle(format!("cannot stat a file: {e}"), Some(&path)))?;
            let kind = meta.file_type();
            if kind.is_symlink() {
                return Err(invalid_bundle("symlinks are not allowed", Some(&path)));
            }
            if kind.is_dir() {
                stack.push((entry.path(), path));
                continue;
            }
            if !kind.is_file() {
                return Err(invalid_bundle("special files are not allowed", Some(&path)));
            }
            let len = meta.len();
            if path == SIGNATURE_FILE && len > MAX_SIGNATURE_BYTES {
                return Err(invalid_bundle(
                    "intent-app.sig is larger than 16 KiB",
                    Some(SIGNATURE_FILE),
                ));
            }
            if len > MAX_FILE_BYTES {
                return Err(invalid_bundle("a file is larger than 16 MiB", Some(&path)));
            }
            total = total.saturating_add(len);
            if total > MAX_TOTAL_BYTES {
                return Err(invalid_bundle("the bundle is larger than 64 MiB", None));
            }
            entries.push((path, entry.path(), len));
            if entries.len() > MAX_FILES {
                return Err(invalid_bundle("the bundle has more than 2000 files", None));
            }
        }
    }
    entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut files = Vec::with_capacity(entries.len());
    let mut size_bytes: u64 = 0;
    let mut total_read: u64 = 0;
    let mut signature = None;
    for (path, abs, _) in entries {
        // Read through a no-follow open where the platform has one, so a
        // file swapped for a symlink after the walk is not followed.
        let bytes = read_no_follow(&abs)
            .map_err(|e| invalid_bundle(format!("cannot read a file: {e}"), Some(&path)))?;
        if bytes.len() as u64 > MAX_FILE_BYTES {
            return Err(invalid_bundle("a file is larger than 16 MiB", Some(&path)));
        }
        total_read = total_read.saturating_add(bytes.len() as u64);
        if total_read > MAX_TOTAL_BYTES {
            return Err(invalid_bundle("the bundle is larger than 64 MiB", None));
        }
        if path == SIGNATURE_FILE {
            if bytes.len() as u64 > MAX_SIGNATURE_BYTES {
                return Err(invalid_bundle(
                    "intent-app.sig is larger than 16 KiB",
                    Some(SIGNATURE_FILE),
                ));
            }
            signature = Some(bytes);
            continue;
        }
        size_bytes = size_bytes.saturating_add(bytes.len() as u64);
        files.push(BundleFile { path, bytes });
    }
    let hash = bundle_hash(files.iter().map(|f| (f.path.as_str(), f.bytes.as_slice())));
    Ok(Bundle {
        files,
        signature,
        hash,
        size_bytes,
    })
}

#[cfg(unix)]
fn read_no_follow(path: &Path) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(std::io::Error::other("not a regular file"));
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[cfg(not(unix))]
fn read_no_follow(path: &Path) -> std::io::Result<Vec<u8>> {
    use std::io::Read as _;
    if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(std::io::Error::other("symlink"));
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// The `window` member of a manifest and of `App` / `AppSession`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WindowSpec {
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) min_width: Option<u32>,
    pub(crate) min_height: Option<u32>,
    pub(crate) resizable: bool,
    pub(crate) always_on_top: bool,
}

/// A validated manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Manifest {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) description: Option<String>,
    pub(crate) icon: Option<String>,
    pub(crate) entry: String,
    pub(crate) level: u8,
    /// Requested features, sorted.
    pub(crate) features: Vec<String>,
    pub(crate) window: WindowSpec,
    /// Reserved (stored, never verified in M1).
    pub(crate) publisher: Option<Publisher>,
    /// Reserved (stored, never fetched in M1).
    pub(crate) update_url: Option<String>,
}

/// The manifest `publisher` (reserved).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Publisher {
    pub(crate) name: String,
    pub(crate) url: Option<String>,
    pub(crate) key_id: Option<String>,
}

/// A well-formed `intent-app.sig` (reserved): stored as given, never
/// verified in M1.
#[expect(clippy::struct_field_names, reason = "the wire key is `signature`")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Signature {
    pub(crate) sig_version: u8,
    pub(crate) algorithm: String,
    pub(crate) key_id: String,
    pub(crate) bundle_hash: String,
    pub(crate) signature: String,
}

fn id_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-z0-9](?:[a-z0-9-]{1,62})[a-z0-9]$").expect("valid"))
}

fn semver_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$",
        )
        .expect("valid")
    })
}

/// A valid app id (§5.60 manifest `id`).
pub(crate) fn is_app_id(id: &str) -> bool {
    id_re().is_match(id)
}

/// Control characters (§5.60 "Validation rules"): C0, DEL and C1.
pub(crate) fn is_control_char(c: char) -> bool {
    matches!(c, '\u{0}'..='\u{1f}' | '\u{7f}'..='\u{9f}')
}

fn has_control(s: &str) -> bool {
    s.chars().any(is_control_char)
}

/// Printable ASCII (U+0021–U+007E), `min..=max` chars.
fn is_printable_ascii(s: &str, max: usize) -> bool {
    !s.is_empty() && s.len() <= max && s.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

/// The §5.60 https URL rule: printable ASCII, at most 2048 chars, the
/// lowercase `https://` prefix, no `\`, not `/` right after the prefix, and
/// an absolute WHATWG URL with a non-empty host.
pub(crate) fn is_https_url(s: &str) -> bool {
    is_printable_ascii(s, 2048)
        && s.starts_with("https://")
        && !s.contains('\\')
        && s.as_bytes().get(8).is_some_and(|b| *b != b'/')
        && reqwest::Url::parse(s)
            .is_ok_and(|u| u.scheme() == "https" && u.host_str().is_some_and(|h| !h.is_empty()))
}

/// Parse a root `intent-app.sig` (reserved): every key required, no others.
pub(crate) fn parse_signature(bytes: &[u8]) -> Result<Signature, Error> {
    const KEYS: [&str; 5] = [
        "sigVersion",
        "algorithm",
        "keyId",
        "bundleHash",
        "signature",
    ];
    let bad =
        |detail: &str| invalid_bundle(format!("intent-app.sig: {detail}"), Some(SIGNATURE_FILE));
    let value: Value = serde_json::from_slice(bytes).map_err(|_| bad("not JSON"))?;
    let Value::Object(obj) = value else {
        return Err(bad("not an object"));
    };
    if obj.len() != KEYS.len() || !KEYS.iter().all(|k| obj.contains_key(*k)) {
        return Err(bad(
            "needs exactly sigVersion, algorithm, keyId, bundleHash, signature",
        ));
    }
    if !(obj["sigVersion"].is_u64() && obj["sigVersion"].as_u64() == Some(1)) {
        return Err(bad("sigVersion must be 1"));
    }
    let text = |k: &str| {
        obj[k]
            .as_str()
            .ok_or_else(|| bad(&format!("{k} must be a string")))
    };
    let algorithm = text("algorithm")?;
    if algorithm.is_empty()
        || algorithm.len() > 32
        || !algorithm
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(bad("algorithm breaks its rule"));
    }
    let key_id = text("keyId")?;
    if !is_printable_ascii(key_id, 256) {
        return Err(bad("keyId breaks its rule"));
    }
    let bundle_hash = text("bundleHash")?;
    let hex_ok = bundle_hash.strip_prefix("sha256:").is_some_and(|h| {
        h.len() == 64
            && h.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    });
    if !hex_ok {
        return Err(bad("bundleHash breaks its rule"));
    }
    let signature = text("signature")?;
    if signature.is_empty()
        || signature.len() > 1024
        || !signature
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(bad("signature breaks its rule"));
    }
    Ok(Signature {
        sig_version: 1,
        algorithm: algorithm.to_owned(),
        key_id: key_id.to_owned(),
        bundle_hash: bundle_hash.to_owned(),
        signature: signature.to_owned(),
    })
}

fn check_keys(obj: &Map<String, Value>, allowed: &[&str], at: &str) -> Result<(), Error> {
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(invalid_manifest(
                format!("unknown key {key:?}"),
                &format!("{at}/{}", pointer_escape(key)),
            ));
        }
    }
    Ok(())
}

fn pointer_escape(key: &str) -> String {
    key.replace('~', "~0").replace('/', "~1")
}

fn opt_u32(
    obj: &Map<String, Value>,
    key: &str,
    at: &str,
    range: std::ops::RangeInclusive<u32>,
) -> Result<Option<u32>, Error> {
    let field = format!("{at}/{key}");
    match obj.get(key) {
        None => Ok(None),
        Some(v) => {
            let n = v
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| invalid_manifest(format!("{key} must be an integer"), &field))?;
            if !range.contains(&n) {
                return Err(invalid_manifest(
                    format!("{key} must be {}–{}", range.start(), range.end()),
                    &field,
                ));
            }
            Ok(Some(n))
        }
    }
}

fn opt_bool(obj: &Map<String, Value>, key: &str, at: &str) -> Result<Option<bool>, Error> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(_) => Err(invalid_manifest(
            format!("{key} must be a boolean"),
            &format!("{at}/{key}"),
        )),
    }
}

/// Parse and validate `intent-app.json` against the bundle (§5.60
/// "Manifest file"). Level 3 parses here; the caller refuses it.
pub(crate) fn parse_manifest(bundle: &Bundle) -> Result<Manifest, Error> {
    let file = bundle
        .file(MANIFEST_FILE)
        .ok_or_else(|| invalid_manifest("intent-app.json is missing", ""))?;
    if file.bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(invalid_manifest(
            "intent-app.json is larger than 64 KiB",
            "",
        ));
    }
    let value: Value = serde_json::from_slice(&file.bytes)
        .map_err(|e| invalid_manifest(format!("not JSON: {e}"), ""))?;
    let Value::Object(top) = value else {
        return Err(invalid_manifest("the manifest must be an object", ""));
    };
    check_keys(
        &top,
        &[
            "manifestVersion",
            "id",
            "name",
            "version",
            "description",
            "icon",
            "entry",
            "permissions",
            "window",
            "publisher",
            "updateUrl",
        ],
        "",
    )?;
    if top.get("manifestVersion").and_then(Value::as_u64) != Some(1)
        || !top.get("manifestVersion").is_some_and(Value::is_u64)
    {
        return Err(invalid_manifest(
            "manifestVersion must be 1",
            "/manifestVersion",
        ));
    }
    let id = match top.get("id") {
        Some(Value::String(s)) if is_app_id(s) => s.clone(),
        _ => return Err(invalid_manifest("id breaks the id rule", "/id")),
    };
    let name = match top.get("name") {
        Some(Value::String(s)) => {
            let t = s.trim();
            let n = t.chars().count();
            if n == 0 || n > 64 || has_control(t) {
                return Err(invalid_manifest(
                    "name must be 1–64 chars without control chars",
                    "/name",
                ));
            }
            t.to_owned()
        }
        _ => return Err(invalid_manifest("name must be a string", "/name")),
    };
    let version = match top.get("version") {
        Some(Value::String(s)) if s.len() <= 64 && semver_re().is_match(s) => s.clone(),
        _ => {
            return Err(invalid_manifest(
                "version must be SemVer 2.0.0, at most 64 chars",
                "/version",
            ))
        }
    };
    let description = match top.get("description") {
        None => None,
        Some(Value::String(s)) if s.chars().count() <= 280 => Some(s.clone()),
        Some(_) => {
            return Err(invalid_manifest(
                "description must be a string of at most 280 chars",
                "/description",
            ))
        }
    };
    let bundle_file = |key: &str, ext: &[&str]| -> Result<Option<String>, Error> {
        let field = format!("/{key}");
        match top.get(key) {
            None => Ok(None),
            Some(Value::String(p)) => {
                let lower = p.to_ascii_lowercase();
                if !is_bundle_path(p) || !ext.iter().any(|e| lower.ends_with(e)) {
                    return Err(invalid_manifest(
                        format!("{key} must be a bundle path of a {} file", ext[0]),
                        &field,
                    ));
                }
                if bundle.file(p).is_none() {
                    return Err(invalid_manifest(
                        format!("{key} names no file of the bundle"),
                        &field,
                    ));
                }
                Ok(Some(p.clone()))
            }
            Some(_) => Err(invalid_manifest(format!("{key} must be a string"), &field)),
        }
    };
    let icon = bundle_file("icon", &[".png"])?;
    let entry = if let Some(e) = bundle_file("entry", &[".html"])? {
        e
    } else {
        if bundle.file("index.html").is_none() {
            return Err(invalid_manifest(
                "the default entry index.html is missing",
                "/entry",
            ));
        }
        "index.html".to_owned()
    };
    let Some(Value::Object(perms)) = top.get("permissions") else {
        return Err(invalid_manifest(
            "permissions must be an object",
            "/permissions",
        ));
    };
    check_keys(perms, &["level", "features"], "/permissions")?;
    let level = match perms.get("level").and_then(Value::as_u64) {
        Some(n @ 1..=3) if perms.get("level").is_some_and(Value::is_u64) => {
            u8::try_from(n).unwrap_or(3)
        }
        _ => {
            return Err(invalid_manifest(
                "level must be 1, 2 or 3",
                "/permissions/level",
            ))
        }
    };
    let mut features: Vec<String> = Vec::new();
    match perms.get("features") {
        None => {}
        Some(Value::Array(items)) => {
            for (i, item) in items.iter().enumerate() {
                let field = format!("/permissions/features/{i}");
                let Some(name) = item.as_str() else {
                    return Err(invalid_manifest("a feature must be a string", &field));
                };
                if !APP_FEATURES.contains(&name) {
                    return Err(invalid_manifest(
                        format!("unknown feature {name:?}"),
                        &field,
                    ));
                }
                if features.iter().any(|f| f == name) {
                    return Err(invalid_manifest(
                        format!("duplicate feature {name:?}"),
                        &field,
                    ));
                }
                if level == 1 {
                    return Err(invalid_manifest("a level 1 app has no features", &field));
                }
                features.push(name.to_owned());
            }
        }
        Some(_) => {
            return Err(invalid_manifest(
                "features must be an array",
                "/permissions/features",
            ))
        }
    }
    features.sort();
    let window = match top.get("window") {
        None => WindowSpec {
            width: 960,
            height: 640,
            min_width: None,
            min_height: None,
            resizable: true,
            always_on_top: false,
        },
        Some(Value::Object(w)) => {
            check_keys(
                w,
                &[
                    "width",
                    "height",
                    "minWidth",
                    "minHeight",
                    "resizable",
                    "alwaysOnTop",
                ],
                "/window",
            )?;
            let width = opt_u32(w, "width", "/window", 320..=4096)?.unwrap_or(960);
            let height = opt_u32(w, "height", "/window", 240..=4096)?.unwrap_or(640);
            let min_width = opt_u32(w, "minWidth", "/window", 320..=width)?;
            let min_height = opt_u32(w, "minHeight", "/window", 240..=height)?;
            let resizable = opt_bool(w, "resizable", "/window")?.unwrap_or(true);
            let always_on_top = opt_bool(w, "alwaysOnTop", "/window")?.unwrap_or(false);
            if always_on_top && !features.iter().any(|f| f == "alwaysOnTop") {
                return Err(invalid_manifest(
                    "alwaysOnTop needs the alwaysOnTop feature",
                    "/window/alwaysOnTop",
                ));
            }
            WindowSpec {
                width,
                height,
                min_width,
                min_height,
                resizable,
                always_on_top,
            }
        }
        Some(_) => return Err(invalid_manifest("window must be an object", "/window")),
    };
    let publisher = match top.get("publisher") {
        None => None,
        Some(Value::Object(p)) => {
            check_keys(p, &["name", "url", "keyId"], "/publisher")?;
            let name = match p.get("name") {
                Some(Value::String(n)) => {
                    let t = n.trim();
                    let len = t.chars().count();
                    if len == 0 || len > 128 || has_control(t) {
                        return Err(invalid_manifest(
                            "publisher name must be 1–128 chars without control chars",
                            "/publisher/name",
                        ));
                    }
                    t.to_owned()
                }
                _ => {
                    return Err(invalid_manifest(
                        "publisher name is required",
                        "/publisher/name",
                    ))
                }
            };
            let url = match p.get("url") {
                None => None,
                Some(Value::String(u)) if is_https_url(u) => Some(u.clone()),
                Some(_) => {
                    return Err(invalid_manifest(
                        "publisher url must be an absolute https URL",
                        "/publisher/url",
                    ))
                }
            };
            let key_id = match p.get("keyId") {
                None => None,
                Some(Value::String(k)) if is_printable_ascii(k, 256) => Some(k.clone()),
                Some(_) => {
                    return Err(invalid_manifest(
                        "publisher keyId must be 1–256 printable ASCII chars",
                        "/publisher/keyId",
                    ))
                }
            };
            Some(Publisher { name, url, key_id })
        }
        Some(_) => {
            return Err(invalid_manifest(
                "publisher must be an object",
                "/publisher",
            ))
        }
    };
    let update_url = match top.get("updateUrl") {
        None => None,
        Some(Value::String(u)) if is_https_url(u) => Some(u.clone()),
        Some(_) => {
            return Err(invalid_manifest(
                "updateUrl must be an absolute https URL",
                "/updateUrl",
            ))
        }
    };
    Ok(Manifest {
        id,
        name,
        version,
        description,
        icon,
        entry,
        level,
        features,
        window,
        publisher,
        update_url,
    })
}

/// `app.readBundle` MIME type from the extension (§5.60).
pub(crate) fn mime_type(path: &str) -> &'static str {
    let ext = path
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "html" => "text/html",
        "js" | "mjs" => "text/javascript",
        "css" => "text/css",
        "json" | "map" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "wasm" => "application/wasm",
        "txt" => "text/plain",
        "mp3" => "audio/mpeg",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        _ => "application/octet-stream",
    }
}

/// Write `bundle` into `dir` (which must not exist yet) through a staging
/// directory next to it, then rename. On any failure nothing is left at
/// `dir`.
pub(crate) fn write_bundle(bundle: &Bundle, dir: &Path) -> std::io::Result<()> {
    let parent = dir
        .parent()
        .ok_or_else(|| std::io::Error::other("library dir has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let staging = parent.join(format!(".staging-{}", uuid::Uuid::new_v4().simple()));
    let result = (|| {
        for file in &bundle.files {
            let target = staging.join(file.path.replace('/', std::path::MAIN_SEPARATOR_STR));
            if let Some(p) = target.parent() {
                std::fs::create_dir_all(p)?;
            }
            std::fs::write(&target, &file.bytes)?;
        }
        std::fs::create_dir_all(&staging)?;
        std::fs::rename(&staging, dir)
    })();
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&staging);
    }
    result
}

/// Read one installed file for `app.readBundle`. `None` when `path` breaks
/// the path rules or names no regular file under `dir`.
pub(crate) fn read_installed(dir: &Path, path: &str) -> Option<Vec<u8>> {
    if !is_bundle_path(path) || path == SIGNATURE_FILE {
        return None;
    }
    let mut target = dir.to_path_buf();
    for seg in path.split('/') {
        // Each segment must be one plain name on this platform: on Windows
        // `C:x` is a drive-relative prefix that would replace `dir`.
        if !is_plain_segment(seg) {
            return None;
        }
        target.push(seg);
        // Every component must be a real directory or (last) a file; the
        // library is daemon-written, but never follow a planted link.
        let meta = std::fs::symlink_metadata(&target).ok()?;
        if meta.file_type().is_symlink() {
            return None;
        }
    }
    // Containment: the resolved file must be under the installed bundle.
    let root = std::fs::canonicalize(dir).ok()?;
    let resolved = std::fs::canonicalize(&target).ok()?;
    if !resolved.starts_with(&root) {
        return None;
    }
    read_no_follow(&resolved).ok()
}

/// Whether `seg` is exactly one normal path component (no prefix, root,
/// `.` / `..`, separator or, on Windows, drive or stream colon).
pub(crate) fn is_plain_segment(seg: &str) -> bool {
    if cfg!(windows) && seg.contains(':') {
        return false;
    }
    let mut parts = Path::new(seg).components();
    matches!(
        (parts.next(), parts.next()),
        (Some(std::path::Component::Normal(name)), None) if name == std::ffi::OsStr::new(seg)
    )
}
