//! Header-only Markdown image dimension probe behind the assistant text
//! block `media` sidecar (§7.1): `{ [markdownSrc]: { width, height } }`.
//!
//! The probe resolves a Markdown image `src` to a file on the daemon host —
//! `workspace-asset://<wsId>/<assetId>` under the assets root,
//! `intent://<org>/[<wsId>/]file/<path>` and bare workspace-relative paths
//! under the workspace root (both through the `file.*` within-root guards) —
//! and reads ONLY the image header (`ImageReader::into_dimensions`): no pixel
//! decode, no full read. `http(s)://`, `data:` and every other scheme are
//! never probed. Every failure is silent (`None`): a missing file, an
//! out-of-root path, a non-image, or an unreadable header simply yields no
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
            return Some(
                self.assets_root
                    .as_ref()?
                    .join(&self.workspace_id)
                    .join(asset),
            );
        }
        if let Some(rest) = src.strip_prefix("intent://") {
            let rest = strip_query_fragment(rest);
            let mut segments = rest.split('/');
            let org = segments.next()?;
            if org.is_empty() {
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
/// once its closing `)` arrives, never twice. An `![alt](src "title")` form
/// yields only the `src` token (the FE keys `media` by the rendered `src`).
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
        let Some(close) = bytes[i + 2..].iter().position(|&b| b == b']') else {
            if len - i > MAX_IMAGE_REF_SPAN {
                i += 1;
                continue;
            }
            break;
        };
        let j = i + 2 + close;
        if j + 1 >= len {
            break;
        }
        if bytes[j + 1] != b'(' {
            i += 1;
            continue;
        }
        let Some(end) = bytes[j + 2..].iter().position(|&b| b == b')') else {
            if len - i > MAX_IMAGE_REF_SPAN {
                i += 1;
                continue;
            }
            break;
        };
        let k = j + 2 + end;
        let src = text[j + 2..k]
            .trim()
            .split_ascii_whitespace()
            .next()
            .unwrap_or_default();
        if !src.is_empty() {
            out.push(src.to_string());
        }
        i = k + 1;
    }
    *scan_pos = i;
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
