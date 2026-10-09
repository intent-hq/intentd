use std::io::Cursor;

use base64::{engine::general_purpose, Engine as _};
use image::{
    DynamicImage, GenericImageView, ImageFormat, ImageReader, Rgb, RgbImage, Rgba, RgbaImage,
};
use intent_acp::session::ContentBlock;
use serde_json::json;

use super::prepare_prompt_images;
use crate::Error;

fn encode(image: &DynamicImage, format: ImageFormat) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    image.write_to(&mut bytes, format).unwrap();
    bytes.into_inner()
}

fn image_block(data: impl AsRef<str>, mime_type: &str) -> ContentBlock {
    serde_json::from_value(json!({
        "type": "image", "data": data.as_ref(), "mimeType": mime_type,
        "uri": "workspace-asset://test/original",
        "annotations": {"priority": 0.8}, "_meta": {"original": true}
    }))
    .unwrap()
}

fn png_block(image: &DynamicImage) -> ContentBlock {
    image_block(
        general_purpose::STANDARD.encode(encode(image, ImageFormat::Png)),
        "image/png",
    )
}

fn noise(width: u32, height: u32, transparent: bool) -> DynamicImage {
    let mut state = 0x1234_5678_u32;
    let mut pixel = || {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        state.to_le_bytes()
    };
    if transparent {
        DynamicImage::ImageRgba8(RgbaImage::from_fn(width, height, |_, _| {
            let [r, g, b, _] = pixel();
            Rgba([r, g, b, 128])
        }))
    } else {
        DynamicImage::ImageRgb8(RgbImage::from_fn(width, height, |_, _| {
            let [r, g, b, _] = pixel();
            Rgb([r, g, b])
        }))
    }
}

fn decoded_output(block: &ContentBlock) -> DynamicImage {
    let ContentBlock::Image(image) = block else {
        panic!("missing image")
    };
    let bytes = general_purpose::STANDARD.decode(&image.data).unwrap();
    assert!(bytes.len() <= 3 * 1024 * 1024, "provider raw-image budget");
    assert!(
        image.data.len() <= 4 * 1024 * 1024,
        "provider base64-image budget"
    );
    let reader = ImageReader::new(Cursor::new(&bytes))
        .with_guessed_format()
        .unwrap();
    assert_eq!(reader.format().unwrap().to_mime_type(), image.mime_type);
    reader.decode().unwrap()
}

fn assert_invalid(error: Error, index: usize, reason: &str) {
    let Error::InvalidParams(message) = error else {
        panic!("expected invalid params")
    };
    assert!(message.contains(&format!("Image {index}:")), "{message}");
    assert!(message.contains(reason), "{message}");
    assert!(
        message.contains("PNG/JPEG") && message.contains("fewer images"),
        "{message}"
    );
    assert!(message.len() < 350, "error must not echo image data");
    assert!(
        !message.contains("private-"),
        "error must not echo invalid input"
    );
}

#[tokio::test]
async fn small_supported_images_and_other_blocks_are_preserved_exactly() {
    let source = DynamicImage::ImageRgb8(RgbImage::from_pixel(5, 3, Rgb([23, 67, 91])));
    let mut prompt = vec![ContentBlock::from("before")];
    for format in [
        ImageFormat::Png,
        ImageFormat::Jpeg,
        ImageFormat::Gif,
        ImageFormat::WebP,
    ] {
        let bytes = encode(&source, format);
        for engine in [general_purpose::STANDARD, general_purpose::STANDARD_NO_PAD] {
            prompt.push(image_block(engine.encode(&bytes), format.to_mime_type()));
            prompt.push(ContentBlock::from("between"));
        }
    }
    prompt.push(
        serde_json::from_value(json!({
            "type": "resource_link", "name": "readme", "uri": "file:///README.md"
        }))
        .unwrap(),
    );
    assert_eq!(prepare_prompt_images(prompt.clone()).await.unwrap(), prompt);
    let text_only = vec![ContentBlock::from("no images")];
    assert_eq!(
        prepare_prompt_images(text_only.clone()).await.unwrap(),
        text_only
    );
    assert!(prepare_prompt_images(Vec::new()).await.unwrap().is_empty());
}

#[tokio::test]
async fn high_entropy_png_is_resized_with_aspect_ratio_and_original_metadata_preserved() {
    for (width, height, ratio, mime) in [(2400, 1200, 2, "image/jpeg"), (3200, 400, 8, "image/png")]
    {
        let original = png_block(&noise(width, height, false));
        let ContentBlock::Image(source) = &original else {
            unreachable!()
        };
        assert!(source.data.len() > 4 * 1024 * 1024);
        // Two original payloads exceed the aggregate budget; prepared copies fit.
        let prompt = vec![
            ContentBlock::from("before"),
            original.clone(),
            ContentBlock::from("between"),
            original.clone(),
        ];
        let prepared = prepare_prompt_images(prompt.clone()).await.unwrap();
        assert_eq!(prepared.len(), 4);
        assert_eq!(prepared[0], prompt[0]);
        assert_eq!(prepared[2], prompt[2]);
        for position in [1, 3] {
            let decoded = decoded_output(&prepared[position]);
            assert!(decoded.width() <= 2000 && decoded.height() <= 2000);
            assert!(decoded.width().abs_diff(ratio * decoded.height()) <= ratio);
            let ContentBlock::Image(output) = &prepared[position] else {
                unreachable!()
            };
            assert_eq!(
                output.mime_type, mime,
                "prefer fitting PNG, otherwise opaque JPEG"
            );
            assert_ne!(output.data, source.data);
            assert_eq!(output.uri, source.uri);
            assert_eq!(output.annotations, source.annotations);
            assert_eq!(output.meta, source.meta);
        }
    }
}

#[tokio::test]
async fn oversized_transparent_png_remains_png_and_keeps_alpha() {
    let original = png_block(&noise(2100, 1050, true));
    let prepared = prepare_prompt_images(vec![original]).await.unwrap();
    let decoded = decoded_output(&prepared[0]);
    let ContentBlock::Image(output) = &prepared[0] else {
        unreachable!()
    };
    assert_eq!(output.mime_type, "image/png");
    assert!(decoded.has_alpha());
    assert!(
        decoded.width() < 2000,
        "transparent entropy needs further dimension reduction"
    );
    assert!(decoded.width().abs_diff(2 * decoded.height()) <= 2);
    assert!(decoded
        .pixels()
        .all(|(_, _, pixel)| pixel.0[3].abs_diff(128) <= 1));
}

#[tokio::test]
async fn compressible_images_obey_the_dimension_cap_without_changing_in_budget_images() {
    for (width, height, expected_width, expected_height) in [
        (2000, 1000, 2000, 1000),
        (4000, 1000, 2000, 500),
        (1000, 4000, 500, 2000),
    ] {
        let source =
            DynamicImage::ImageRgb8(RgbImage::from_pixel(width, height, Rgb([23, 67, 91])));
        let bytes = encode(&source, ImageFormat::Png);
        assert!(
            bytes.len() < 3 * 1024 * 1024,
            "only dimensions should trigger resizing"
        );
        let original = image_block(general_purpose::STANDARD.encode(bytes), "image/png");
        let prepared = prepare_prompt_images(vec![original.clone()]).await.unwrap();
        let decoded = decoded_output(&prepared[0]);
        assert_eq!(decoded.dimensions(), (expected_width, expected_height));
        assert_eq!(decoded.to_rgb8().get_pixel(0, 0), &Rgb([23, 67, 91]));
        if width == expected_width && height == expected_height {
            assert_eq!(
                prepared[0], original,
                "exactly at the dimension cap is unchanged"
            );
        } else {
            assert_ne!(
                prepared[0], original,
                "byte-small oversized dimensions must be reduced"
            );
        }
    }
}

#[tokio::test]
async fn aggregate_budget_accepts_fitting_images_and_rejects_the_first_excess() {
    let image = png_block(&noise(800, 800, false));
    let ContentBlock::Image(source) = &image else {
        unreachable!()
    };
    // Independently establish that each image fits but only three fit together.
    assert!(source.data.len() <= 4 * 1024 * 1024);
    assert!(source.data.len() * 3 <= 8 * 1024 * 1024);
    assert!(source.data.len() * 4 > 8 * 1024 * 1024);
    let accepted = vec![image.clone(); 3];
    assert_eq!(
        prepare_prompt_images(accepted.clone()).await.unwrap(),
        accepted
    );
    let mut rejected = accepted;
    rejected.insert(0, ContentBlock::from("does not count as an image"));
    rejected.push(image);
    assert_invalid(
        prepare_prompt_images(rejected).await.unwrap_err(),
        4,
        "8 MiB",
    );
}

#[tokio::test]
async fn invalid_images_fail_the_whole_prompt_with_index_and_actionable_private_errors() {
    let valid = png_block(&noise(8, 8, false));
    let png = encode(&noise(8, 8, false), ImageFormat::Png);
    let corrupt = &png[..png.len() / 2];
    for (data, mime, reason) in [
        ("private-invalid-base64!".into(), "image/png", "base64"),
        (
            general_purpose::STANDARD.encode(b"private-not-image"),
            "image/png",
            "unrecognized",
        ),
        (
            general_purpose::STANDARD.encode(corrupt),
            "image/png",
            "corrupt",
        ),
        (general_purpose::STANDARD.encode(&png), "image/jpeg", "MIME"),
        (
            general_purpose::STANDARD.encode(b"BMunsupported-format"),
            "image/bmp",
            "unsupported",
        ),
    ] {
        let prompt = vec![
            ContentBlock::from("before"),
            valid.clone(),
            image_block(data, mime),
        ];
        assert_invalid(prepare_prompt_images(prompt).await.unwrap_err(), 2, reason);
    }
}

#[tokio::test]
async fn original_base64_over_the_input_budget_is_rejected_before_decoding() {
    let image = image_block("A".repeat(40 * 1024 * 1024 + 4), "image/png");
    assert_invalid(
        prepare_prompt_images(vec![image]).await.unwrap_err(),
        1,
        "40 MiB",
    );
}

fn change_png_dimensions(bytes: &mut [u8], width: u32, height: u32) {
    bytes[16..20].copy_from_slice(&width.to_be_bytes());
    bytes[20..24].copy_from_slice(&height.to_be_bytes());
    // PNG's IHDR CRC covers the chunk type and payload, not its length.
    let mut crc = u32::MAX;
    for byte in &bytes[12..29] {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    bytes[29..33].copy_from_slice(&(!crc).to_be_bytes());
}

#[tokio::test]
async fn header_dimensions_and_decoded_allocation_are_guarded_before_pixels() {
    let rgba16 = DynamicImage::ImageRgba16(image::ImageBuffer::from_pixel(
        1,
        1,
        Rgba([0_u16, 0, 0, u16::MAX]),
    ));
    for (width, height, reason) in [(10_000, 4_001, "40 million"), (6_000, 6_000, "256 MiB")] {
        let mut bytes = encode(&rgba16, ImageFormat::Png);
        change_png_dimensions(&mut bytes, width, height);
        // The fixture has a readable header: rejection must be the safety guard,
        // not merely a bad PNG signature or checksum.
        assert_eq!(
            ImageReader::new(Cursor::new(&bytes))
                .with_guessed_format()
                .unwrap()
                .into_dimensions()
                .unwrap(),
            (width, height)
        );
        let block = image_block(general_purpose::STANDARD.encode(bytes), "image/png");
        assert_invalid(
            prepare_prompt_images(vec![block]).await.unwrap_err(),
            1,
            reason,
        );
    }
}
