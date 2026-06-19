//! Small path/line helpers shared by the content and filename searches.

use std::path::Path;

/// The workspace-relative, forward-slashed form of `path` under `root`. Falls
/// back to the full path when `path` is not under `root`.
pub fn normalize_rel(root: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(root).unwrap_or(path);
    rel.to_string_lossy().replace('\\', "/")
}

/// The first line of a match's bytes, without the trailing `\n`/`\r`.
pub fn first_line(bytes: &[u8]) -> &[u8] {
    let end = bytes
        .iter()
        .position(|&b| b == b'\n')
        .unwrap_or(bytes.len());
    let mut line = &bytes[..end];
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    line
}
