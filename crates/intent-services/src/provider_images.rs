//! Validate and prepare provider-bound image copies, never persisted originals.
//!
//! Budgets include base64 expansion and leave room below provider request limits.
//! A prompt is returned only when every image is valid and their aggregate fits.

use std::io::Cursor;

use base64::{engine::general_purpose, Engine as _};
use image::{DynamicImage, GenericImageView, ImageDecoder, ImageFormat, ImageReader, Limits};
use intent_acp::session::ContentBlock;
use tokio::sync::Semaphore;

use crate::{Error, Result};

const MAX_INPUT_BASE64: usize = 40 * 1024 * 1024;
const MAX_INPUT_BYTES: usize = 30 * 1024 * 1024;
const MAX_IMAGE_BYTES: usize = 3 * 1024 * 1024;
const MAX_PROMPT_BASE64: usize = 8 * 1024 * 1024;
const MAX_PIXELS: u32 = 40_000_000;
const MAX_DECODE_ALLOC: u64 = 256 * 1024 * 1024;
const MAX_EDGE: u32 = 2000;
const RESIZE_ATTEMPTS: usize = 6;

static IMAGE_WORKERS: Semaphore = Semaphore::const_new(2);

/// Prepare all images together without exposing partially prepared prompts.
pub(crate) async fn prepare_prompt_images(prompt: Vec<ContentBlock>) -> Result<Vec<ContentBlock>> {
    if !prompt
        .iter()
        .any(|block| matches!(block, ContentBlock::Image(_)))
    {
        return Ok(prompt);
    }
    let permit = IMAGE_WORKERS
        .acquire()
        .await
        .map_err(|_| Error::Internal("Image preparation is unavailable; please retry.".into()))?;
    tokio::task::spawn_blocking(move || {
        // A cancelled caller cannot release this slot while its CPU work continues.
        let _permit = permit;
        prepare_images(prompt)
    })
    .await
    .map_err(|_| Error::Internal("Image preparation failed; please retry with PNG/JPEG.".into()))?
}

fn prepare_images(mut prompt: Vec<ContentBlock>) -> Result<Vec<ContentBlock>> {
    let mut image_index = 0;
    let mut base64_bytes = 0;
    for block in &mut prompt {
        let ContentBlock::Image(image) = block else {
            continue;
        };
        image_index += 1;
        if let Some((data, mime_type)) = prepare_image(&image.data, &image.mime_type, image_index)?
        {
            image.data = data;
            image.mime_type = mime_type.into();
        }
        // Each individual result is <= 4 MiB, and the running total is <= 8 MiB.
        base64_bytes += image.data.len();
        if base64_bytes > MAX_PROMPT_BASE64 {
            return Err(invalid_image(
                image_index,
                "the prepared images together exceed the 8 MiB base64 prompt budget",
            ));
        }
    }
    Ok(prompt)
}

fn prepare_image(
    data: &str,
    mime_type: &str,
    index: usize,
) -> Result<Option<(String, &'static str)>> {
    if data.len() > MAX_INPUT_BASE64 {
        return Err(invalid_image(
            index,
            "the original exceeds 40 MiB of base64 data",
        ));
    }
    let bytes = general_purpose::STANDARD
        .decode(data)
        .or_else(|_| general_purpose::STANDARD_NO_PAD.decode(data))
        .map_err(|_| invalid_image(index, "the image data is not valid base64"))?;
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(invalid_image(
            index,
            "the original exceeds 30 MiB of image data",
        ));
    }
    let format = image::guess_format(&bytes)
        .map_err(|_| invalid_image(index, "the image format is unrecognized or corrupt"))?;
    if !matches!(
        format,
        ImageFormat::Png | ImageFormat::Jpeg | ImageFormat::Gif | ImageFormat::WebP
    ) {
        return Err(invalid_image(
            index,
            "the image format is unsupported (use PNG, JPEG, GIF, or WebP)",
        ));
    }
    let jpeg_alias = format == ImageFormat::Jpeg && mime_type.eq_ignore_ascii_case("image/jpg");
    if !mime_type.eq_ignore_ascii_case(format.to_mime_type()) && !jpeg_alias {
        return Err(invalid_image(
            index,
            "the MIME type does not match the image data",
        ));
    }

    let mut limits = Limits::default();
    limits.max_image_width = Some(MAX_PIXELS);
    limits.max_image_height = Some(MAX_PIXELS);
    limits.max_alloc = Some(MAX_DECODE_ALLOC);
    let mut reader = ImageReader::with_format(Cursor::new(&bytes), format);
    // In particular, PNG needs its allocation limit before decoder construction.
    reader.limits(limits.clone());
    let mut decoder = reader
        .into_decoder()
        .map_err(|error| decode_error(index, &error))?;
    let (width, height) = decoder.dimensions();
    if width == 0 || height == 0 || u64::from(width) * u64::from(height) > u64::from(MAX_PIXELS) {
        return Err(invalid_image(
            index,
            "image dimensions must be nonzero and at most 40 million pixels",
        ));
    }
    // Match ImageReader::decode's output-buffer reservation, after checking pixels.
    // max_alloc is best-effort in the dependency; this buffer check is explicit.
    limits.reserve(decoder.total_bytes()).map_err(|_| {
        invalid_image(
            index,
            "decoding the image exceeds the 256 MiB allocation budget",
        )
    })?;
    decoder
        .set_limits(limits)
        .map_err(|error| decode_error(index, &error))?;
    let oversized = bytes.len() > MAX_IMAGE_BYTES || width > MAX_EDGE || height > MAX_EDGE;
    let orientation = if oversized {
        Some(
            decoder
                .orientation()
                .map_err(|error| decode_error(index, &error))?,
        )
    } else {
        None
    };
    // Always decode: a small file with valid headers can still contain corrupt pixels.
    let source_image =
        DynamicImage::from_decoder(decoder).map_err(|error| decode_error(index, &error))?;
    if !oversized {
        return Ok(None);
    }
    drop(bytes);
    // Decide before resizing: averaging may round a near-opaque alpha to fully opaque.
    let transparent = has_transparency(&source_image);
    let mut resized = source_image.thumbnail(MAX_EDGE, MAX_EDGE);
    drop(source_image);
    if let Some(orientation) = orientation {
        resized.apply_orientation(orientation);
    }
    let (data, mime_type) = encode_with_budget(resized, transparent, index)?;
    Ok(Some((general_purpose::STANDARD.encode(data), mime_type)))
}

fn has_transparency(image: &DynamicImage) -> bool {
    // Preserve 16-bit alpha instead of quantizing it through GenericImageView's RGBA8.
    match image {
        DynamicImage::ImageLumaA16(pixels) => pixels.pixels().any(|pixel| pixel.0[1] != u16::MAX),
        DynamicImage::ImageRgba16(pixels) => pixels.pixels().any(|pixel| pixel.0[3] != u16::MAX),
        _ => image.has_alpha() && image.pixels().any(|(_, _, pixel)| pixel.0[3] != 255),
    }
}

fn encode_with_budget(
    mut image: DynamicImage,
    transparent: bool,
    index: usize,
) -> Result<(Vec<u8>, &'static str)> {
    for _ in 0..RESIZE_ATTEMPTS {
        let mut png = Cursor::new(Vec::new());
        image
            .write_to(&mut png, ImageFormat::Png)
            .map_err(|_| invalid_image(index, "the image could not be encoded as PNG"))?;
        let png = png.into_inner();
        if png.len() <= MAX_IMAGE_BYTES {
            return Ok((png, "image/png"));
        }
        drop(png);
        if !transparent {
            // JPEG accepts RGB, including when the source had an unused alpha channel.
            let rgb = image.to_rgb8();
            for quality in [85, 70] {
                let mut jpeg = Vec::new();
                image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, quality)
                    .encode_image(&rgb)
                    .map_err(|_| invalid_image(index, "the image could not be encoded as JPEG"))?;
                if jpeg.len() <= MAX_IMAGE_BYTES {
                    return Ok((jpeg, "image/jpeg"));
                }
            }
        }
        let (width, height) = image.dimensions();
        if width == 1 && height == 1 {
            break;
        }
        image = image.thumbnail((width * 3 / 4).max(1), (height * 3 / 4).max(1));
    }
    Err(invalid_image(
        index,
        "the image could not be reduced below 3 MiB without discarding transparency",
    ))
}

fn decode_error(index: usize, error: &image::ImageError) -> Error {
    // Do not include codec error text: input metadata and bytes are untrusted/private.
    let reason = if matches!(error, image::ImageError::Limits(_)) {
        "the image exceeds safe decoding dimensions or the 256 MiB allocation budget"
    } else {
        "the image data is corrupt or cannot be decoded"
    };
    invalid_image(index, reason)
}

fn invalid_image(index: usize, reason: &str) -> Error {
    Error::InvalidParams(format!(
        "Image {index}: {reason}. Crop or export a smaller PNG/JPEG, or send fewer images."
    ))
}

#[cfg(test)]
mod tests;
