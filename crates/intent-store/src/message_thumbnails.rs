//! Write-time image thumbnails for the slim conversation projection (0097).
//!
//! When a message being persisted contains an `image` block whose base64
//! `data` exceeds [`SLIM_PROJECTION_BUDGET_BYTES`], the write path generates a
//! small thumbnail (longest edge ≤ [`THUMBNAIL_MAX_EDGE`] px, re-encoded,
//! target ≤ [`THUMBNAIL_TARGET_BASE64_BYTES`] base64) and persists it on the
//! row's `thumbnails` column as a JSON map keyed by the block's image ordinal
//! (the i-th `image` block in the message — stable under the serve-time
//! tool-block strip). Slim reads substitute the thumbnail for the full image
//! data; the read path never decodes or resizes (RPC cost contract rung 1).
//! Generation failure is non-fatal: the block is skipped with a WARN and slim
//! reads degrade to serving the image with `data` omitted.
//!
//! The same write path also stamps the intrinsic pixel `width` / `height` of
//! every base64 `image` block onto the block itself (protocol v10.7 image
//! dimension sidecar) via a header-only decode — the stored block carries the
//! ORIGINAL dimensions, so the slim projection (which swaps only `data` /
//! `mimeType` for the thumbnail) serves them unchanged next to
//! `dataIsThumbnail`. Undecodable data leaves the block untouched; rows are
//! never backfilled.

use base64::Engine as _;
use intent_core::SLIM_PROJECTION_BUDGET_BYTES;
use serde_json::{json, Value};

/// Longest edge of a generated thumbnail, in pixels.
const THUMBNAIL_MAX_EDGE: u32 = 256;

/// Target upper bound for a thumbnail's base64 length. A PNG re-encode that
/// exceeds this falls back to JPEG, which compresses photographic content far
/// smaller at 256px.
const THUMBNAIL_TARGET_BASE64_BYTES: usize = 16 * 1024;

/// JPEG quality for the PNG-overflow fallback encode.
const THUMBNAIL_JPEG_QUALITY: u8 = 60;

/// Cheap predicate for the write path: does `content` carry an `image` block
/// whose base64 `data` exceeds the slim budget — i.e. would
/// [`generate_message_thumbnails`] do real (decode/resize/encode) work? Lets
/// callers skip the content clone and blocking-pool hop for the common
/// no-oversized-image message.
pub(crate) fn needs_thumbnails(content: &Value) -> bool {
    content.as_array().is_some_and(|blocks| {
        blocks.iter().any(|b| {
            b.get("type").and_then(Value::as_str) == Some("image")
                && b.get("data")
                    .and_then(Value::as_str)
                    .is_some_and(|d| d.len() > SLIM_PROJECTION_BUDGET_BYTES)
        })
    })
}

/// Build the `thumbnails` column value for a message's content blocks:
/// `{"<image ordinal>": {"data": "<base64>", "mimeType": "image/..."}}` for
/// every `image` block whose base64 `data` exceeds the slim-projection
/// budget. Returns `None` when the message has no such block (the common
/// case) or when every generation attempt failed.
pub(crate) fn generate_message_thumbnails(content: &Value) -> Option<Value> {
    let blocks = content.as_array()?;
    let mut map = serde_json::Map::new();
    let mut image_ordinal: usize = 0;
    for block in blocks {
        if block.get("type").and_then(Value::as_str) != Some("image") {
            continue;
        }
        let ordinal = image_ordinal;
        image_ordinal += 1;
        let Some(data) = block.get("data").and_then(Value::as_str) else {
            continue;
        };
        if data.len() <= SLIM_PROJECTION_BUDGET_BYTES {
            continue;
        }
        match generate_thumbnail(data) {
            Ok((thumb_b64, mime)) => {
                map.insert(
                    ordinal.to_string(),
                    json!({ "data": thumb_b64, "mimeType": mime }),
                );
            }
            Err(e) => {
                tracing::warn!(
                    image_ordinal = ordinal,
                    data_len = data.len(),
                    error = %e,
                    "image thumbnail generation failed; slim reads will omit \
                     this block's data"
                );
            }
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

/// Cheap predicate for the write path: does `content` carry a base64 `image`
/// block not yet stamped with both `width` and `height`? Lets callers skip
/// the content clone and blocking-pool hop for the common no-image message
/// and for re-persisted content (transfer, replace) that already carries its
/// dimensions.
pub(crate) fn needs_dimensions(content: &Value) -> bool {
    content
        .as_array()
        .is_some_and(|blocks| blocks.iter().any(image_block_needs_dimensions))
}

/// A slim-read block (`dataIsThumbnail: true`) carries the downscaled
/// thumbnail as `data`, so decoding it would record the thumbnail's size as
/// the original's. Such a block is stampable only through dimensions it
/// already carries; a pre-v10.7 one re-persisted by a transfer/replace stays
/// without `width`/`height` rather than lying about them.
fn image_block_needs_dimensions(block: &Value) -> bool {
    block.get("type").and_then(Value::as_str) == Some("image")
        && block.get("data").and_then(Value::as_str).is_some()
        && block.get("dataIsThumbnail").and_then(Value::as_bool) != Some(true)
        && !(block.get("width").and_then(Value::as_u64).is_some()
            && block.get("height").and_then(Value::as_u64).is_some())
}

/// Stamp the intrinsic pixel `width` / `height` onto every base64 `image`
/// block of `content` that does not carry both yet (header-only decode — the
/// pixels are never materialized). A block whose data does not decode is left
/// untouched (logged at DEBUG: garbage data is a client concern, not a daemon
/// fault). Returns `true` when at least one block was stamped.
pub(crate) fn stamp_image_dimensions(content: &mut Value) -> bool {
    let Some(blocks) = content.as_array_mut() else {
        return false;
    };
    let mut stamped = false;
    for (ordinal, block) in blocks.iter_mut().enumerate() {
        if !image_block_needs_dimensions(block) {
            continue;
        }
        let Some(data) = block.get("data").and_then(Value::as_str) else {
            continue;
        };
        match decode_dimensions(data) {
            Ok((width, height)) => {
                if let Some(obj) = block.as_object_mut() {
                    obj.insert("width".to_string(), json!(width));
                    obj.insert("height".to_string(), json!(height));
                    stamped = true;
                }
            }
            Err(e) => {
                tracing::debug!(
                    block_ordinal = ordinal,
                    data_len = data.len(),
                    error = %e,
                    "image dimensions undecodable; block persisted without width/height"
                );
            }
        }
    }
    stamped
}

/// Read `(width, height)` from base64 image data by decoding only the image
/// header (format sniffed from the bytes, not the block's `mimeType`).
fn decode_dimensions(data: &str) -> Result<(u32, u32), String> {
    let bytes = decode_base64(data)?;
    image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|e| format!("image format sniff: {e}"))?
        .into_dimensions()
        .map_err(|e| format!("image header decode: {e}"))
}

/// Decode base64 image data, accepting both padded and unpadded input.
fn decode_base64(data: &str) -> Result<Vec<u8>, String> {
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(data))
        .map_err(|e| format!("base64 decode: {e}"))
}

/// Decode base64 image data, downscale to fit [`THUMBNAIL_MAX_EDGE`], and
/// re-encode as PNG (JPEG fallback when the PNG overflows the base64 target).
/// Returns `(base64, mimeType)`.
fn generate_thumbnail(data: &str) -> Result<(String, String), String> {
    let std_engine = base64::engine::general_purpose::STANDARD;
    let bytes = decode_base64(data)?;
    let img = image::load_from_memory(&bytes).map_err(|e| format!("image decode: {e}"))?;
    // `thumbnail` preserves aspect ratio within the bounding box and never
    // upscales smaller inputs.
    let thumb = img.thumbnail(THUMBNAIL_MAX_EDGE, THUMBNAIL_MAX_EDGE);

    let mut png_buf = Vec::new();
    thumb
        .write_to(
            &mut std::io::Cursor::new(&mut png_buf),
            image::ImageFormat::Png,
        )
        .map_err(|e| format!("png encode: {e}"))?;
    let png_b64 = std_engine.encode(&png_buf);
    if png_b64.len() <= THUMBNAIL_TARGET_BASE64_BYTES {
        return Ok((png_b64, "image/png".to_string()));
    }

    // PNG overflow (photographic / high-entropy content): fall back to JPEG,
    // backing off quality then edge size until the base64 fits the target.
    // JPEG has no alpha channel, so flatten to RGB first.
    let mut best: Option<String> = None;
    for (edge, quality) in [
        (THUMBNAIL_MAX_EDGE, THUMBNAIL_JPEG_QUALITY),
        (THUMBNAIL_MAX_EDGE, 35),
        (THUMBNAIL_MAX_EDGE / 2, 35),
        (THUMBNAIL_MAX_EDGE / 4, 35),
    ] {
        let scaled = if edge == THUMBNAIL_MAX_EDGE {
            thumb.clone()
        } else {
            thumb.thumbnail(edge, edge)
        };
        let rgb = image::DynamicImage::ImageRgb8(scaled.to_rgb8());
        let mut jpeg_buf = Vec::new();
        {
            let mut jpeg_cursor = std::io::Cursor::new(&mut jpeg_buf);
            let mut encoder =
                image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg_cursor, quality);
            encoder
                .encode_image(&rgb)
                .map_err(|e| format!("jpeg encode: {e}"))?;
        }
        let jpeg_b64 = std_engine.encode(&jpeg_buf);
        let fits = jpeg_b64.len() <= THUMBNAIL_TARGET_BASE64_BYTES;
        if best.as_ref().is_none_or(|b| jpeg_b64.len() < b.len()) {
            best = Some(jpeg_b64);
        }
        if fits {
            break;
        }
    }
    let jpeg_b64 = best.expect("at least one JPEG encode attempt");
    if jpeg_b64.len() < png_b64.len() {
        Ok((jpeg_b64, "image/jpeg".to_string()))
    } else {
        Ok((png_b64, "image/png".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Base64 PNG of a `width`×`height` noise image — noise defeats PNG's
    /// filters, so even modest dimensions exceed the slim budget.
    fn noise_png_base64(width: u32, height: u32) -> String {
        let img = image::RgbImage::from_fn(width, height, |x, y| {
            let v = (x.wrapping_mul(31).wrapping_add(y.wrapping_mul(17)) % 251) as u8;
            image::Rgb([v, v.wrapping_add(97), v.wrapping_add(193)])
        });
        let mut buf = Vec::new();
        image::DynamicImage::ImageRgb8(img)
            .write_to(&mut std::io::Cursor::new(&mut buf), image::ImageFormat::Png)
            .expect("encode test png");
        base64::engine::general_purpose::STANDARD.encode(&buf)
    }

    /// An oversized image block gets a thumbnail keyed by its image ordinal:
    /// decodable, downscaled to ≤ 256px on the longest edge, and within the
    /// base64 target size.
    #[test]
    fn oversized_image_gets_bounded_thumbnail() {
        let data = noise_png_base64(512, 384);
        assert!(
            data.len() > SLIM_PROJECTION_BUDGET_BYTES,
            "test image too small"
        );
        let content = json!([
            { "type": "text", "text": "hi" },
            { "type": "image", "data": data, "mimeType": "image/png" },
        ]);
        let thumbs = generate_message_thumbnails(&content).expect("thumbnail generated");
        let entry = thumbs.get("0").expect("keyed by image ordinal");
        let thumb_b64 = entry.get("data").and_then(Value::as_str).expect("data");
        assert!(thumb_b64.len() <= THUMBNAIL_TARGET_BASE64_BYTES);
        let mime = entry.get("mimeType").and_then(Value::as_str).expect("mime");
        assert!(mime == "image/png" || mime == "image/jpeg");
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(thumb_b64)
            .expect("thumbnail decodes");
        let img = image::load_from_memory(&bytes).expect("thumbnail is a valid image");
        assert!(img.width() <= THUMBNAIL_MAX_EDGE && img.height() <= THUMBNAIL_MAX_EDGE);
    }

    /// Under-budget images and non-image blocks produce no thumbnail map at
    /// all (the common case persists NULL).
    #[test]
    fn under_budget_and_text_only_produce_none() {
        let small = noise_png_base64(8, 8);
        assert!(small.len() <= SLIM_PROJECTION_BUDGET_BYTES);
        assert!(generate_message_thumbnails(&json!([
            { "type": "image", "data": small, "mimeType": "image/png" },
            { "type": "text", "text": "hello" },
        ]))
        .is_none());
        assert!(generate_message_thumbnails(&json!([{ "type": "text", "text": "x" }])).is_none());
        assert!(generate_message_thumbnails(&json!("not an array")).is_none());
    }

    /// Undecodable image data is non-fatal: the block is skipped (logged) and
    /// ordinals still count it, so a later valid image keys correctly.
    #[test]
    fn generation_failure_is_skipped_and_ordinals_stay_stable() {
        let garbage = "A".repeat(SLIM_PROJECTION_BUDGET_BYTES + 1);
        let valid = noise_png_base64(512, 384);
        let content = json!([
            { "type": "image", "data": garbage, "mimeType": "image/png" },
            { "type": "image", "data": valid, "mimeType": "image/png" },
        ]);
        let thumbs = generate_message_thumbnails(&content).expect("second image thumbnailed");
        assert!(thumbs.get("0").is_none(), "failed block persists nothing");
        assert!(thumbs.get("1").is_some(), "ordinal counts the failed block");
    }

    /// Every decodable base64 image block — under or over the slim budget —
    /// is stamped with its intrinsic dimensions; non-image blocks and blocks
    /// without `data` are untouched.
    #[test]
    fn stamps_intrinsic_dimensions_on_decodable_image_blocks() {
        let big = noise_png_base64(512, 384);
        let small = noise_png_base64(8, 8);
        assert!(small.len() <= SLIM_PROJECTION_BUDGET_BYTES);
        let mut content = json!([
            { "type": "text", "text": "hi" },
            { "type": "image", "data": big, "mimeType": "image/png" },
            { "type": "image", "data": small, "mimeType": "image/png" },
            { "type": "image", "attachmentId": "att-1", "mimeType": "image/png" },
        ]);
        assert!(needs_dimensions(&content));
        assert!(stamp_image_dimensions(&mut content));
        assert_eq!(content[1]["width"], 512);
        assert_eq!(content[1]["height"], 384);
        assert_eq!(content[2]["width"], 8);
        assert_eq!(content[2]["height"], 8);
        assert!(content[0].get("width").is_none());
        assert!(
            content[3].get("width").is_none(),
            "no data → nothing to decode"
        );
        assert!(
            !needs_dimensions(&content),
            "a stamped message is idempotent on re-persist"
        );
        assert!(!stamp_image_dimensions(&mut content));
    }

    /// Undecodable data leaves the block untouched (no `width`/`height`,
    /// nothing else changed); a stamped sibling is unaffected.
    #[test]
    fn undecodable_image_data_is_left_untouched() {
        let garbage = "A".repeat(64);
        let valid = noise_png_base64(16, 4);
        let mut content = json!([
            { "type": "image", "data": garbage, "mimeType": "image/png" },
            { "type": "image", "data": valid, "mimeType": "image/png" },
        ]);
        let before = content[0].clone();
        assert!(stamp_image_dimensions(&mut content));
        assert_eq!(content[0], before);
        assert_eq!(content[1]["width"], 16);
        assert_eq!(content[1]["height"], 4);
        assert!(!needs_dimensions(&json!([{ "type": "text", "text": "x" }])));
        assert!(!needs_dimensions(&json!("not an array")));
        assert!(!stamp_image_dimensions(&mut json!("not an array")));
    }

    /// A block arriving with client-supplied dimensions keeps them (the
    /// slim-read shape `dataIsThumbnail: true` re-persisted by a transfer
    /// must keep the ORIGINAL dimensions, not the thumbnail's).
    #[test]
    fn existing_dimensions_are_kept() {
        let thumb = noise_png_base64(8, 8);
        let mut content = json!([
            { "type": "image", "data": thumb, "mimeType": "image/png",
              "dataIsThumbnail": true, "width": 1024, "height": 768 },
        ]);
        assert!(!needs_dimensions(&content));
        assert!(!stamp_image_dimensions(&mut content));
        assert_eq!(content[0]["width"], 1024);
        assert_eq!(content[0]["height"], 768);
    }

    /// A pre-v10.7 slim-read block (`dataIsThumbnail: true`, no dimensions)
    /// re-persisted by a transfer/replace is NOT stamped: its `data` is the
    /// downscaled thumbnail, so decoding it would record the thumbnail's size
    /// as the original's. The block stays dimensionless; a sibling full
    /// image block in the same message is still stamped.
    #[test]
    fn legacy_thumbnail_block_without_dimensions_is_not_stamped() {
        let thumb = noise_png_base64(8, 8);
        let full = noise_png_base64(16, 4);
        let mut content = json!([
            { "type": "image", "data": thumb, "mimeType": "image/png",
              "dataIsThumbnail": true, "dataTruncated": true, "dataBytes": 123_456 },
            { "type": "image", "data": full, "mimeType": "image/png" },
        ]);
        let legacy_before = content[0].clone();
        assert!(needs_dimensions(&content), "sibling full image needs dims");
        assert!(stamp_image_dimensions(&mut content));
        assert_eq!(content[0], legacy_before, "thumbnail block untouched");
        assert_eq!(content[1]["width"], 16);
        assert_eq!(content[1]["height"], 4);
        let mut only_legacy = json!([legacy_before.clone()]);
        assert!(!needs_dimensions(&only_legacy));
        assert!(!stamp_image_dimensions(&mut only_legacy));
        assert_eq!(only_legacy[0], legacy_before);
    }
}
