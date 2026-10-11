//! Compact authenticated-token payloads. Wire references remain opaque.
use super::Token;

fn integer(bytes: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        bytes.push(u8::try_from(value & 127).expect("seven bits") | 128);
        value >>= 7;
    }
    bytes.push(u8::try_from(value).expect("final seven bits"));
}

const WORDS: &[&str] = &[
    "", "c", "d", "f", "m", "a", "h", "t", "root", "details", "pieces",
];
const KINDS: &[&str] = &["r", "s+", "s-", "context", "metadata", "taskIds"];

pub(super) fn encode(token: &Token) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(
        uuid::Uuid::parse_str(&token.0)
            .expect("snapshot UUID")
            .as_bytes(),
    );
    bytes.push(
        u8::try_from(
            KINDS
                .iter()
                .position(|kind| *kind == token.1)
                .expect("token kind"),
        )
        .expect("kind byte"),
    );
    let parts: Vec<_> = token.2.split(':').collect();
    bytes.push(u8::try_from(parts.len()).expect("bounded collection parts"));
    for part in parts {
        if let Some(word) = WORDS.iter().position(|word| *word == part) {
            bytes.push(u8::try_from(word).expect("word byte"));
        } else if part.len() == 16
            && part
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            bytes.push(11);
            integer(
                &mut bytes,
                u64::from_str_radix(part, 16).expect("hex index id"),
            );
        } else if let Some(value) = part
            .parse::<u64>()
            .ok()
            .filter(|value| value.to_string() == part)
        {
            bytes.push(12);
            integer(&mut bytes, value);
        } else {
            // Keep internal collection extensions lossless without changing the
            // public reference format or interpreting arbitrary client strings.
            bytes.push(13);
            integer(&mut bytes, part.len() as u64);
            bytes.extend_from_slice(part.as_bytes());
        }
    }
    for value in [token.3, token.4 as u64, token.5 as u64, token.6 as u64] {
        integer(&mut bytes, value);
    }
    bytes
}

struct Decoder<'a>(&'a [u8]);
impl<'a> Decoder<'a> {
    fn take(&mut self, count: usize) -> Option<&'a [u8]> {
        let value = self.0.get(..count)?;
        self.0 = &self.0[count..];
        Some(value)
    }
    fn byte(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn integer(&mut self) -> Option<u64> {
        let mut value = 0;
        for shift in (0..=63).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                return None;
            }
            value |= u64::from(byte & 127) << shift;
            if byte < 128 {
                if shift > 0 && byte == 0 {
                    return None;
                }
                return Some(value);
            }
        }
        None
    }
}

pub(super) fn decode(bytes: &[u8]) -> Option<Token> {
    let mut input = Decoder(bytes);
    if input.byte()? != 1 {
        return None;
    }
    let snapshot = uuid::Uuid::from_slice(input.take(16)?)
        .ok()?
        .simple()
        .to_string();
    let kind = KINDS.get(usize::from(input.byte()?))?.to_string();
    let count = input.byte()?;
    if count == 0 || count > 8 {
        return None;
    }
    let mut parts = Vec::with_capacity(usize::from(count));
    for _ in 0..count {
        let tag = input.byte()?;
        parts.push(match tag {
            0..=10 => WORDS[usize::from(tag)].to_string(),
            11 => format!("{:016x}", input.integer()?),
            12 => input.integer()?.to_string(),
            13 => {
                let length = usize::try_from(input.integer()?).ok()?;
                std::str::from_utf8(input.take(length)?).ok()?.to_owned()
            }
            _ => return None,
        });
    }
    let token = Token(
        snapshot,
        kind,
        parts.join(":"),
        input.integer()?,
        usize::try_from(input.integer()?).ok()?,
        usize::try_from(input.integer()?).ok()?,
        usize::try_from(input.integer()?).ok()?,
    );
    if !input.0.is_empty() {
        return None;
    }
    Some(token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::note_page_repo::Runtime;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    #[test]
    fn compact_tokens_preserve_all_resource_and_cursor_addresses() {
        let runtime = Runtime {
            key: vec![23; 32],
            ..Runtime::default()
        };
        let snapshot = uuid::Uuid::new_v4().simple().to_string();
        for kind in KINDS {
            for resource in [
                "",
                "c:4096:4100:4110",
                "d:0000000000000001",
                "d:0000000000000001:details",
                "d:0000000000000001:pieces",
                "d:0000000000000001:4000000:4000006",
                "m:root",
                "a:0000000000000010:root",
                "m:0000010000000010",
                "t:root",
                "h:0000000000000010:2000047:2000080",
                "f:0000000000000040:100:106",
            ] {
                let token = Token(
                    snapshot.clone(),
                    (*kind).into(),
                    resource.into(),
                    9_007_199_254_740_991,
                    16384,
                    65536,
                    128,
                );
                let encoded = runtime.token(&token);
                assert!(encoded.len() <= 128, "{resource}: {}", encoded.len());
                assert_eq!(
                    serde_json::to_value(runtime.decode(&encoded).unwrap()).unwrap(),
                    serde_json::to_value(&token).unwrap()
                );
                // Authenticate legacy payloads with the same full MAC, so a
                // process restart still reports expired leases, not bad scope.
                let legacy = serde_json::to_vec(&token).unwrap();
                let mut mac = Hmac::<Sha256>::new_from_slice(&runtime.key).unwrap();
                mac.update(&legacy);
                let legacy = format!(
                    "{}.{}",
                    URL_SAFE_NO_PAD.encode(legacy),
                    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
                );
                assert_eq!(
                    serde_json::to_value(runtime.decode(&legacy).unwrap()).unwrap(),
                    serde_json::to_value(&token).unwrap()
                );
                let mut tampered = encoded.into_bytes();
                tampered[2] = if tampered[2] == b'A' { b'B' } else { b'A' };
                assert!(runtime
                    .decode(std::str::from_utf8(&tampered).unwrap())
                    .is_err());
            }
        }
    }

    #[test]
    fn compact_payload_rejects_truncation_trailing_bytes_and_overflow() {
        let token = Token(
            uuid::Uuid::new_v4().simple().to_string(),
            "r".into(),
            "d:0000000000000001".into(),
            0,
            0,
            0,
            0,
        );
        let valid = encode(&token);
        for length in 0..valid.len() {
            assert!(decode(&valid[..length]).is_none());
        }
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(decode(&trailing).is_none());
        let mut invalid = valid.clone();
        invalid[0] = 2;
        assert!(decode(&invalid).is_none());
        let mut overflow = valid[..valid.len() - 4].to_vec();
        overflow.extend_from_slice(&[255; 10]);
        assert!(decode(&overflow).is_none());
        let mut nonminimal = valid[..valid.len() - 4].to_vec();
        nonminimal.extend_from_slice(&[128, 0, 0, 0, 0]);
        assert!(decode(&nonminimal).is_none());
    }

    #[test]
    fn reference_preserves_noncanonical_numeric_resource_identity() {
        let runtime = Runtime {
            key: vec![23; 32],
            ..Runtime::default()
        };
        let source = "00000000000000000000000000000002";
        for resource in [
            "j:00000000000000000000000000000001",
            "f:01",
            "f:+1",
            "f:0",
            "f:18446744073709551615",
            "f:18446744073709551616",
        ] {
            let token = Token(source.into(), "r".into(), resource.into(), 0, 0, 0, 0);
            let encoded = runtime.token(&token);
            assert!(encoded.len() <= 256);
            assert_eq!(runtime.decode(&encoded).unwrap().2, resource);
        }
    }
}
