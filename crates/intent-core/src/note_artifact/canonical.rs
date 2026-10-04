//! RFC 8785 bytes for bounded artifact integrity envelopes, not arbitrary trees.
use super::{preflight, RecordError};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Validate raw JSON before recursive allocation, then serialize exact JCS bytes.
/// This uses the artifact envelope's structural limits. Record strings are never
/// recursively parsed or normalized by this serializer.
pub fn canonical_json(raw: &str) -> Result<String, RecordError> {
    preflight(raw, crate::note_page::WIRE_BYTES)?;
    let value: Value = serde_json::from_str(raw).map_err(|_| RecordError::Syntax)?;
    let mut output = String::with_capacity(raw.len());
    write(&value, &mut output)?;
    Ok(output)
}

pub fn digest(raw: &str) -> Result<String, RecordError> {
    let canonical = canonical_json(raw)?;
    Ok(format!("{:x}", Sha256::digest(canonical.as_bytes())))
}

fn write(value: &Value, output: &mut String) -> Result<(), RecordError> {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => {
            // RFC 8785 operates on binary64, including integer JSON spellings.
            let number = value.as_f64().ok_or(RecordError::Syntax)?;
            if !number.is_finite() {
                return Err(RecordError::Syntax);
            }
            output.push_str(ryu_js::Buffer::new().format_finite(number));
        }
        Value::String(value) => {
            output.push_str(&serde_json::to_string(value).map_err(|_| RecordError::Syntax)?);
        }
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                write(value, output)?;
            }
            output.push(']');
        }
        Value::Object(values) => {
            let mut fields: Vec<_> = values.iter().collect();
            fields.sort_unstable_by(|(a, _), (b, _)| a.encode_utf16().cmp(b.encode_utf16()));
            output.push('{');
            for (index, (key, value)) in fields.into_iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key).map_err(|_| RecordError::Syntax)?);
                output.push(':');
                write(value, output)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_protocol_canonical_utf8_and_sha256_vectors() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/native_artifact_canonicalization.json"
        ))
        .unwrap();
        let fixture = &fixture["canonicalization"];
        let vectors = fixture["values"]
            .as_array()
            .unwrap()
            .iter()
            .chain(fixture["numbers"].as_array().unwrap())
            .chain(std::iter::once(&fixture["header"]))
            .chain(fixture["append"].as_array().unwrap());
        for vector in vectors {
            let raw = vector["rawJson"].as_str().unwrap();
            let canonical = canonical_json(raw).unwrap();
            assert_eq!(canonical, vector["canonical"], "{}", vector["id"]);
            let hex: String = canonical
                .as_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            assert_eq!(hex, vector["utf8Hex"], "{}", vector["id"]);
            assert_eq!(digest(raw).unwrap(), vector["sha256"], "{}", vector["id"]);
            if let Some(bits) = vector["binary64Hex"].as_str() {
                let number: f64 = raw.parse().unwrap();
                assert_eq!(format!("{:016x}", number.to_bits()), bits);
            }
        }
    }

    #[test]
    fn invalid_or_ambiguous_json_never_gets_a_digest() {
        for raw in [
            r#"{"a":1,"\u0061":2}"#,
            r#""\ud800""#,
            r#""\u0000""#,
            "1e400",
            "NaN",
            "[1,]",
        ] {
            assert!(digest(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn exact_record_string_changes_append_digest() {
        assert_ne!(
            digest(r#"["append","{\"left\":1}"]"#).unwrap(),
            digest(r#"["append","{\"left\":1.0}"]"#).unwrap()
        );
        assert_eq!(
            digest("{\"left\":1}").unwrap(),
            digest("{\"left\":1.0}").unwrap()
        );
    }

    #[test]
    fn escaped_append_envelope_can_exceed_decoded_record_budget() {
        let record = format!(
            "{{\"kind\":\"diff.fragment\",\"value\":{{\"text\":{}}}}}",
            serde_json::to_string(&"\"".repeat(7_000)).unwrap()
        );
        assert!(super::super::decode_record(&record).is_ok());
        let envelope = serde_json::json!([
            "note.artifact.append.v1",
            "0".repeat(64),
            0,
            "0".repeat(64),
            record
        ])
        .to_string();
        assert!(envelope.len() > super::super::RECORD_BYTES);
        assert!(digest(&envelope).is_ok());
    }
}
