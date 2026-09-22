//! Header-only Markdown image dimension probe behind the assistant text
//! block `media` sidecar (§7.1): `{ [markdownSrc]: { width, height } }`.
//!
//! The probe resolves a Markdown image `src` to a file on the daemon host —
//! `workspace-asset://<wsId>/<assetId>` under the assets root,
//! `intent://local/[<wsId>/]file/<path>` and bare workspace-relative paths
//! under the workspace root (all three through the `file.*` within-root
//! guards, symlink-aware) — and reads ONLY the image header
//! (`ImageReader::into_dimensions`): no pixel decode, no full read.
//! `http(s)://`, `data:`, non-`local` `intent://` authorities and every other
//! scheme are never probed. Every failure is silent (`None`): a missing file,
//! an out-of-root path, a non-image, or an unreadable header simply yields no
//! entry, and the FE falls back to its unsized rendering.
//!
//! Cost ladder (`AGENTS.md` → Performance): rung 1 — the probe runs on the
//! streaming WRITE path (once per turn per `src`, see the `Transcript`
//! cache) and the result is persisted on the block; no read path ever probes.

use std::path::{Path, PathBuf};

use crate::file_ops;

/// What the probe needs to resolve a Markdown `src` to a host path.
#[derive(Clone, Debug, Default)]
pub(crate) struct ProbeContext {
    /// The turn's workspace id: `workspace-asset://` and long-form
    /// `intent://…/<wsId>/file/…` sources naming another workspace resolve
    /// to nothing.
    pub(crate) workspace_id: String,
    /// The workspace root (or the calling agent's sandbox path); empty when
    /// unresolvable, which makes every path-shaped source resolve to nothing
    /// (the within-root guard rejects an empty root).
    pub(crate) workspace_root: String,
    /// The note-assets root (`Services::assets_root`); `None` disables
    /// `workspace-asset://` resolution.
    pub(crate) assets_root: Option<PathBuf>,
}

impl ProbeContext {
    /// Header-only dimensions `(width, height)` of the image `src` names, or
    /// `None` when the source is not probeable or unreadable.
    pub(crate) fn probe(&self, src: &str) -> Option<(u32, u32)> {
        probe_file(&self.resolve(src)?)
    }

    /// Map a Markdown `src` to a host path under one of the two roots, or
    /// `None` when the source is not a probeable shape.
    pub(crate) fn resolve(&self, src: &str) -> Option<PathBuf> {
        let src = src.trim();
        if src.is_empty() {
            return None;
        }
        if let Some(rest) = src.strip_prefix("workspace-asset://") {
            let rest = strip_query_fragment(rest);
            let (ws, asset) = rest.split_once('/')?;
            if ws != self.workspace_id {
                return None;
            }
            let asset = decode_segment(asset)?;
            if !is_safe_segment(&asset) {
                return None;
            }
            // Same symlink-aware guard as workspace files, rooted at the
            // assets root so a symlinked `<wsId>/` directory or asset leaf
            // pointing outside it resolves to nothing.
            let root = self.assets_root.as_ref()?.to_str()?;
            let rel = format!("{}/{asset}", self.workspace_id);
            return file_ops::resolve_within(root, &rel).ok();
        }
        if let Some(rest) = src.strip_prefix("intent://") {
            let rest = strip_query_fragment(rest);
            let mut segments = rest.split('/');
            if segments.next()? != "local" {
                return None;
            }
            let segments: Vec<&str> = segments.collect();
            let path_segments: &[&str] = match segments.first().copied() {
                Some("file") => &segments[1..],
                Some(ws) if segments.get(1).copied() == Some("file") => {
                    if decode_segment(ws)? != self.workspace_id {
                        return None;
                    }
                    &segments[2..]
                }
                _ => return None,
            };
            if path_segments.is_empty() {
                return None;
            }
            let mut decoded = Vec::with_capacity(path_segments.len());
            for segment in path_segments {
                let segment = decode_segment(segment)?;
                if !is_safe_segment(&segment) {
                    return None;
                }
                decoded.push(segment);
            }
            return self.resolve_in_workspace(&decoded.join("/"));
        }
        if has_scheme(src) {
            return None;
        }
        self.resolve_in_workspace(src)
    }

    fn resolve_in_workspace(&self, rel: &str) -> Option<PathBuf> {
        if self.workspace_root.is_empty() {
            return None;
        }
        file_ops::resolve_within(&self.workspace_root, rel).ok()
    }
}

/// Longest span (bytes) an unclosed `![` opener or `](` url is followed for
/// before the scanner gives up on it as a reference. Bounds the rescan of a
/// streaming buffer whose opener never closes (a literal `![` in prose): past
/// the span the opener is skipped and the scan position advances.
const MAX_IMAGE_REF_SPAN: usize = 4096;

/// Scan `text` from `*scan_pos` for COMPLETED `![alt](src)` Markdown image
/// references and return their `src` values in document order, advancing
/// `*scan_pos` past the last consumed reference — or to the start of a
/// still-open one, so a reference split across streamed chunks is detected
/// once its closing `)` arrives, never twice.
///
/// Follows the `CommonMark` inline-image shape the FE renderer (`marked`)
/// parses: balanced / backslash-escaped brackets in the label, a destination
/// that is either `<…>`-delimited (spaces allowed) or bare with balanced
/// parentheses, and an optional `"title"` / `'title'` / `(title)`. The
/// returned `src` is the destination as the renderer keys it — angle
/// brackets stripped and backslash escapes removed — so the `media` key
/// matches the rendered `src` exactly.
pub(crate) fn scan_image_refs(text: &str, scan_pos: &mut usize) -> Vec<String> {
    let bytes = text.as_bytes();
    let len = bytes.len();
    let mut out = Vec::new();
    let mut i = (*scan_pos).min(len);
    while i + 1 < len {
        if bytes[i] != b'!' || bytes[i + 1] != b'[' {
            i += 1;
            continue;
        }
        match parse_image_ref(bytes, i + 2) {
            RefParse::Complete { src, end } => {
                if !src.is_empty() {
                    out.push(src);
                }
                i = end;
            }
            RefParse::Incomplete => {
                if len - i > MAX_IMAGE_REF_SPAN {
                    i += 1;
                    continue;
                }
                break;
            }
            RefParse::Invalid => i += 1,
        }
    }
    *scan_pos = i;
    out
}

/// Outcome of parsing one `![` opener at a position in the streamed buffer.
enum RefParse {
    /// A complete reference: its rendered `src` and the byte offset just past
    /// the closing `)`.
    Complete { src: String, end: usize },
    /// The buffer ended inside the reference — more chunks may close it.
    Incomplete,
    /// Not an image reference (the opener is literal text).
    Invalid,
}

/// Parse the label, destination and optional title of an image reference
/// whose `![` ends just before `start`.
fn parse_image_ref(bytes: &[u8], start: usize) -> RefParse {
    let len = bytes.len();
    // Label: balanced brackets, backslash escapes skipped.
    let mut pos = start;
    let mut depth = 1usize;
    loop {
        if pos >= len {
            return RefParse::Incomplete;
        }
        match bytes[pos] {
            b'\\' if pos + 1 < len && bytes[pos + 1].is_ascii_punctuation() => pos += 2,
            b'[' => {
                depth += 1;
                pos += 1;
            }
            b']' => {
                depth -= 1;
                pos += 1;
                if depth == 0 {
                    break;
                }
            }
            _ => pos += 1,
        }
    }
    if pos >= len {
        return RefParse::Incomplete;
    }
    if bytes[pos] != b'(' {
        return RefParse::Invalid;
    }
    pos += 1;
    pos = skip_whitespace(bytes, pos);
    if pos >= len {
        return RefParse::Incomplete;
    }
    // Destination: `<…>` (no newline, no nested `<`) or a bare run with
    // balanced parentheses ending at whitespace or the unbalanced `)`.
    let dest_start;
    let dest_end;
    if bytes[pos] == b'<' {
        dest_start = pos + 1;
        pos = dest_start;
        loop {
            if pos >= len {
                return RefParse::Incomplete;
            }
            match bytes[pos] {
                b'\\' if pos + 1 < len && bytes[pos + 1].is_ascii_punctuation() => pos += 2,
                b'>' => break,
                b'<' | b'\n' | b'\r' => return RefParse::Invalid,
                _ => pos += 1,
            }
        }
        dest_end = pos;
        pos += 1;
    } else {
        dest_start = pos;
        let mut parens = 0usize;
        loop {
            if pos >= len {
                return RefParse::Incomplete;
            }
            match bytes[pos] {
                b'\\' if pos + 1 < len && bytes[pos + 1].is_ascii_punctuation() => pos += 2,
                b'(' => {
                    parens += 1;
                    pos += 1;
                }
                b')' if parens == 0 => break,
                b')' => {
                    parens -= 1;
                    pos += 1;
                }
                b if b.is_ascii_whitespace() => break,
                _ => pos += 1,
            }
        }
        dest_end = pos;
    }
    pos = skip_whitespace(bytes, pos);
    if pos >= len {
        return RefParse::Incomplete;
    }
    // Optional title, then the closing `)`.
    if bytes[pos] != b')' {
        let closer = match bytes[pos] {
            b'"' => b'"',
            b'\'' => b'\'',
            b'(' => b')',
            _ => return RefParse::Invalid,
        };
        pos += 1;
        loop {
            if pos >= len {
                return RefParse::Incomplete;
            }
            match bytes[pos] {
                b'\\' if pos + 1 < len && bytes[pos + 1].is_ascii_punctuation() => pos += 2,
                b if b == closer => break,
                _ => pos += 1,
            }
        }
        pos = skip_whitespace(bytes, pos + 1);
        if pos >= len {
            return RefParse::Incomplete;
        }
        if bytes[pos] != b')' {
            return RefParse::Invalid;
        }
    }
    let Ok(dest) = std::str::from_utf8(&bytes[dest_start..dest_end]) else {
        return RefParse::Invalid;
    };
    RefParse::Complete {
        src: unescape_punctuation(dest.trim()),
        end: pos + 1,
    }
}

fn skip_whitespace(bytes: &[u8], mut pos: usize) -> usize {
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

/// Drop the backslash of every `\<ASCII punctuation>` escape — the
/// renderer's destination unescape, so the key matches its `src`.
fn unescape_punctuation(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' && chars.peek().is_some_and(char::is_ascii_punctuation) {
            continue;
        }
        out.push(c);
    }
    out
}

/// Header-only `(width, height)` of the image file at `path`; `None` on any
/// IO, format-detection, or header error.
pub(crate) fn probe_file(path: &Path) -> Option<(u32, u32)> {
    image::ImageReader::open(path)
        .ok()?
        .with_guessed_format()
        .ok()?
        .into_dimensions()
        .ok()
}

/// Whether `src` starts with a URL scheme (`scheme:`, RFC 3986 shape) — such
/// a source is never a workspace-relative path.
fn has_scheme(src: &str) -> bool {
    let Some(colon) = src.find(':') else {
        return false;
    };
    let scheme = &src[..colon];
    let mut chars = scheme.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
}

fn strip_query_fragment(s: &str) -> &str {
    s.split(['?', '#']).next().unwrap_or_default()
}

/// A decoded path segment safe to join: non-empty, no dot-segment, no
/// separator (mirrors the FE `intent://…/file/` link parser).
fn is_safe_segment(segment: &str) -> bool {
    !segment.is_empty() && segment != "." && segment != ".." && !segment.contains(['/', '\\'])
}

/// RFC 3986 percent-decode of one path segment; `None` on a malformed escape
/// or non-UTF-8 result (the FE rejects such links too).
fn decode_segment(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = (*bytes.get(i + 1)? as char).to_digit(16)?;
            let lo = (*bytes.get(i + 2)? as char).to_digit(16)?;
            out.push(u8::try_from(hi * 16 + lo).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::test_support::test_tempdir;

    /// Encode a solid `width`×`height` RGB image as `format` into `path`.
    pub(crate) fn write_image(path: &Path, width: u32, height: u32, format: image::ImageFormat) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut buf = std::io::Cursor::new(Vec::new());
        image::RgbImage::from_pixel(width, height, image::Rgb([200, 40, 40]))
            .write_to(&mut buf, format)
            .unwrap();
        std::fs::write(path, buf.into_inner()).unwrap();
    }

    fn context(dir: &Path) -> ProbeContext {
        ProbeContext {
            workspace_id: "ws-1".to_string(),
            workspace_root: dir.join("root").to_string_lossy().into_owned(),
            assets_root: Some(dir.join("assets")),
        }
    }

    #[test]
    fn probe_file_reads_dimensions_for_each_supported_format() {
        let dir = test_tempdir("img-probe-formats-");
        let cases = [
            ("a.png", image::ImageFormat::Png, (64, 48)),
            ("b.jpg", image::ImageFormat::Jpeg, (30, 20)),
            ("c.gif", image::ImageFormat::Gif, (12, 34)),
            ("d.webp", image::ImageFormat::WebP, (100, 7)),
        ];
        for (name, format, (w, h)) in cases {
            let path = dir.path().join(name);
            write_image(&path, w, h, format);
            assert_eq!(probe_file(&path), Some((w, h)), "{name}");
        }
        std::fs::write(dir.path().join("not-an-image.png"), b"hello").unwrap();
        assert_eq!(probe_file(&dir.path().join("not-an-image.png")), None);
        assert_eq!(probe_file(&dir.path().join("missing.png")), None);
    }

    #[test]
    fn resolves_workspace_asset_intent_file_and_relative_sources() {
        let dir = test_tempdir("img-probe-resolve-");
        let ctx = context(dir.path());
        write_image(
            &dir.path().join("assets/ws-1/shot.png"),
            640,
            480,
            image::ImageFormat::Png,
        );
        write_image(
            &dir.path().join("root/docs/my pic.png"),
            8,
            9,
            image::ImageFormat::Png,
        );
        assert_eq!(
            ctx.probe("workspace-asset://ws-1/shot.png"),
            Some((640, 480))
        );
        assert_eq!(
            ctx.probe("workspace-asset://ws-1/shot.png?v=2"),
            Some((640, 480))
        );
        assert_eq!(
            ctx.probe("intent://local/file/docs/my%20pic.png"),
            Some((8, 9))
        );
        assert_eq!(
            ctx.probe("intent://local/ws-1/file/docs/my%20pic.png"),
            Some((8, 9))
        );
        assert_eq!(ctx.probe("docs/my pic.png"), Some((8, 9)));
        assert_eq!(ctx.probe("./docs/my pic.png"), Some((8, 9)));
    }

    #[test]
    fn unprobeable_sources_yield_no_entry() {
        let dir = test_tempdir("img-probe-deny-");
        let ctx = context(dir.path());
        write_image(
            &dir.path().join("outside.png"),
            5,
            5,
            image::ImageFormat::Png,
        );
        write_image(
            &dir.path().join("assets/ws-2/other.png"),
            5,
            5,
            image::ImageFormat::Png,
        );
        std::fs::create_dir_all(dir.path().join("root")).unwrap();
        let outside = dir.path().join("outside.png");
        assert_eq!(ctx.probe("https://example.com/a.png"), None);
        assert_eq!(ctx.probe("data:image/png;base64,AAAA"), None);
        assert_eq!(ctx.probe("../outside.png"), None);
        assert_eq!(ctx.probe(&outside.to_string_lossy()), None);
        assert_eq!(ctx.probe("intent://local/file/../outside.png"), None);
        assert_eq!(ctx.probe("intent://local/file/%2e%2e/outside.png"), None);
        assert_eq!(ctx.probe("intent://local/ws-2/file/outside.png"), None);
        // Only the `local` authority names this daemon's files.
        std::fs::create_dir_all(dir.path().join("root/docs")).unwrap();
        write_image(
            &dir.path().join("root/docs/pic.png"),
            5,
            5,
            image::ImageFormat::Png,
        );
        assert_eq!(ctx.probe("intent://local/file/docs/pic.png"), Some((5, 5)));
        assert_eq!(ctx.probe("intent://remote/file/docs/pic.png"), None);
        assert_eq!(ctx.probe("intent://remote/ws-1/file/docs/pic.png"), None);
        assert_eq!(ctx.probe("intent://LOCAL/file/docs/pic.png"), None);
        assert_eq!(ctx.probe("intent:///file/docs/pic.png"), None);
        assert_eq!(ctx.probe("workspace-asset://ws-2/other.png"), None);
        assert_eq!(ctx.probe("workspace-asset://ws-1/../ws-2/other.png"), None);
        assert_eq!(ctx.probe("workspace-asset://ws-1/"), None);
        assert_eq!(ctx.probe("missing.png"), None);
        assert_eq!(ctx.probe(""), None);
        let no_roots = ProbeContext {
            workspace_id: "ws-1".to_string(),
            workspace_root: String::new(),
            assets_root: None,
        };
        assert_eq!(no_roots.probe("workspace-asset://ws-1/shot.png"), None);
        assert_eq!(no_roots.probe("docs/pic.png"), None);
    }

    /// A symlink planted under the assets root — an asset leaf or the whole
    /// `<wsId>/` directory — pointing outside it resolves to nothing, exactly
    /// as the workspace-file guard treats symlinked workspace paths.
    #[cfg(unix)]
    #[test]
    fn asset_symlink_escapes_yield_no_entry() {
        use std::os::unix::fs::symlink;
        let dir = test_tempdir("img-probe-asset-symlink-");
        let ctx = context(dir.path());
        let outside = dir.path().join("outside.png");
        write_image(&outside, 37, 19, image::ImageFormat::Png);
        write_image(
            &dir.path().join("assets/ws-1/real.png"),
            4,
            4,
            image::ImageFormat::Png,
        );
        symlink(&outside, dir.path().join("assets/ws-1/shot.png")).unwrap();
        assert_eq!(ctx.probe("workspace-asset://ws-1/real.png"), Some((4, 4)));
        assert_eq!(ctx.probe("workspace-asset://ws-1/shot.png"), None);

        let elsewhere = dir.path().join("elsewhere");
        write_image(&elsewhere.join("pic.png"), 37, 19, image::ImageFormat::Png);
        let dir_ctx = ProbeContext {
            workspace_id: "ws-link".to_string(),
            ..context(dir.path())
        };
        symlink(&elsewhere, dir.path().join("assets/ws-link")).unwrap();
        assert_eq!(dir_ctx.probe("workspace-asset://ws-link/pic.png"), None);

        // The workspace-file guard denies the same shapes for `intent://` and
        // relative sources.
        std::fs::create_dir_all(dir.path().join("root")).unwrap();
        symlink(&outside, dir.path().join("root/leak.png")).unwrap();
        assert_eq!(ctx.probe("leak.png"), None);
        assert_eq!(ctx.probe("intent://local/file/leak.png"), None);
    }

    /// The scanner follows the `CommonMark` inline-image shape the FE
    /// renderer parses, keying by the destination as rendered.
    #[test]
    fn scan_parses_commonmark_labels_destinations_and_titles() {
        let cases: [(&str, &[&str]); 12] = [
            (
                "![shot](intent://local/file/shot(1).png)",
                &["intent://local/file/shot(1).png"],
            ),
            (
                "![shot](<intent://local/file/my shot.png>)",
                &["intent://local/file/my shot.png"],
            ),
            ("![shot [1]](a.png)", &["a.png"]),
            ("![a\\]b](b.png)", &["b.png"]),
            ("![e](c\\(1\\).png \"t\")", &["c(1).png"]),
            ("![t](d.png 'title')", &["d.png"]),
            ("![t](e.png (title))", &["e.png"]),
            ("![x](<a b>  \"title (x)\")", &["a b"]),
            ("![x](f.png)) ![y](g.png", &["f.png"]),
            ("![bad](<a\nb>) ![ok](ok.png)", &["ok.png"]),
            ("![bad](h.png junk) ![ok](ok.png)", &["ok.png"]),
            ("![i](  i.png  ) ![j](\nj.png\n)", &["i.png", "j.png"]),
        ];
        for (text, expected) in cases {
            let mut pos = 0;
            assert_eq!(scan_image_refs(text, &mut pos), expected, "{text:?}");
        }
    }

    /// The same forms split across streamed chunks at every delimiter
    /// resolve exactly once, when the closing `)` arrives.
    #[test]
    fn scan_detects_commonmark_forms_split_across_chunks_exactly_once() {
        let cases: [(&[&str], &str); 4] = [
            (
                &["![shot](intent://local/file/shot(1", ").png", ")"],
                "intent://local/file/shot(1).png",
            ),
            (&["![s](<a", " b", ">", ")"], "a b"),
            (&["![n [1", "]", "](n.png", ")"], "n.png"),
            (&["![t](t.png \"ti", "tle\"", ")"], "t.png"),
        ];
        for (chunks, expected) in cases {
            let mut text = String::new();
            let mut pos = 0;
            let (last, opening) = chunks.split_last().unwrap();
            for chunk in opening {
                text.push_str(chunk);
                assert!(
                    scan_image_refs(&text, &mut pos).is_empty(),
                    "{text:?} still open"
                );
                assert_eq!(pos, 0, "{text:?} holds the scan at the opener");
            }
            text.push_str(last);
            assert_eq!(
                scan_image_refs(&text, &mut pos),
                vec![expected.to_string()],
                "{text:?}"
            );
            assert_eq!(pos, text.len());
            assert!(scan_image_refs(&text, &mut pos).is_empty());
        }
    }

    #[test]
    fn scan_detects_references_split_across_chunks_exactly_once() {
        let mut text = String::new();
        let mut pos = 0;
        text.push_str("look ![al");
        assert!(scan_image_refs(&text, &mut pos).is_empty());
        text.push_str("t](work");
        assert!(scan_image_refs(&text, &mut pos).is_empty());
        text.push_str("space-asset://ws/a.png");
        assert!(scan_image_refs(&text, &mut pos).is_empty());
        text.push_str(") and ![b](b.png \"title\") tail ![c](");
        assert_eq!(
            scan_image_refs(&text, &mut pos),
            vec![
                "workspace-asset://ws/a.png".to_string(),
                "b.png".to_string()
            ]
        );
        assert!(scan_image_refs(&text, &mut pos).is_empty());
        text.push_str("c.png)");
        assert_eq!(scan_image_refs(&text, &mut pos), vec!["c.png".to_string()]);
        assert_eq!(pos, text.len());
    }

    #[test]
    fn scan_skips_plain_links_and_gives_up_on_runaway_openers() {
        let mut pos = 0;
        assert!(scan_image_refs("[not image](x.png) ![]() ![no] (x) !x", &mut pos).is_empty());
        let runaway = format!("![{}", "a".repeat(MAX_IMAGE_REF_SPAN + 1));
        let mut pos = 0;
        assert!(scan_image_refs(&runaway, &mut pos).is_empty());
        assert_eq!(pos, runaway.len() - 1);
        let text = format!("{runaway}](late.png) ![ok](ok.png)");
        assert_eq!(scan_image_refs(&text, &mut pos), vec!["ok.png".to_string()]);
    }
}
