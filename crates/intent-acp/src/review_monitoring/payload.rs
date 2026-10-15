//! Strict text-only v1 classification. The Store digest stays opaque and no
//! classification grants authority or delivery acknowledgement.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fmt::Write;

use crate::error::{AcpError, AcpResult};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TextPrompt {
    session_id: String,
    prompt: [TextBlock; 1],
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TextBlock {
    #[serde(rename = "type")]
    kind: String,
    text: String,
}

fn unavailable() -> AcpError {
    AcpError::Protocol("Review monitoring context unavailable".into())
}
fn digest(session: &str, message: &str, store: &str, text: &str) -> String {
    let bytes = serde_json::to_vec(&json!([
        "intent-review-monitor-acp-text-v1",
        session,
        message,
        store,
        text,
    ]))
    .expect("strings are serializable");
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String cannot fail");
            output
        })
}

/// Bind the complete accepted prompt to the committed Store correlation. This
/// rejects extra fields instead of silently hashing a partial rich envelope.
/// The caller must independently retain the original recipient and authority
/// through final transfer; neither returned metadata nor its hash establishes it.
///
/// # Errors
/// Rejects any unsupported prompt shape or malformed bounded correlation.
///
/// # Panics
/// Panics if serialization of the text-only prompt unexpectedly fails.
pub fn classify_prompt(
    params: Value,
    message_id: &str,
    store_envelope_digest: &str,
) -> AcpResult<Value> {
    let request: TextPrompt = serde_json::from_value(params).map_err(|_| unavailable())?;
    if request.session_id.is_empty()
        || request.session_id.len() > 256
        || message_id.is_empty()
        || message_id.len() > 256
        || request.prompt[0].kind != "text"
        || store_envelope_digest.len() != 64
        || !store_envelope_digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(unavailable());
    }
    let hash = digest(
        &request.session_id,
        message_id,
        store_envelope_digest,
        &request.prompt[0].text,
    );
    let mut value = serde_json::to_value(request).expect("text prompt is serializable");
    value["_meta"] = json!({"reviewMonitoringMessage":{
        "version":1,"messageId":message_id,"storeEnvelopeDigest":store_envelope_digest,"acpPayloadDigest":hash,
    }});
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classified_payload_matches_shared_receiver_vectors() {
        let vectors: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/review-monitoring-payload-v1.json"
        ))
        .unwrap();
        for vector in vectors.as_array().unwrap() {
            let actual = classify_prompt(
                vector["params"].clone(),
                vector["messageId"].as_str().unwrap(),
                vector["storeEnvelopeDigest"].as_str().unwrap(),
            )
            .unwrap();
            assert_eq!(actual, vector["classified"]);
        }
    }

    #[test]
    fn classified_payload_refuses_extras_without_affecting_ordinary_serializer() {
        let base = json!({"sessionId":"original","prompt":[{"type":"text","text":"private"}]});
        for path in ["top", "block", "meta", "attachment", "second"] {
            let mut input = base.clone();
            match path {
                "top" => input["extra"] = json!(true),
                "block" => input["prompt"][0]["extra"] = json!(true),
                "meta" => input["_meta"] = json!({"ordinary":true}),
                "attachment" => {
                    input["prompt"][0] =
                        json!({"type":"image","data":"private","mimeType":"image/png"});
                }
                "second" => input["prompt"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"type":"text","text":"extra"})),
                _ => unreachable!(),
            }
            assert!(
                classify_prompt(input, "message", &"a".repeat(64)).is_err(),
                "{path}"
            );
        }
        let first = classify_prompt(base.clone(), "message", &"a".repeat(64)).unwrap();
        let second = classify_prompt(base, "different-message", &"a".repeat(64)).unwrap();
        assert_ne!(first["_meta"], second["_meta"]);
    }
}
