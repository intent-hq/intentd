//! Compact authenticated annotation references (bounded independently of IDs).
use super::{invalid, Error, NotePageError, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use sha2::Sha256;

pub(super) const PREFIX: &str = "na1.";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Token {
    pub snapshot: uuid::Uuid,
    pub kind: u8,
    pub owner: u64,
    pub position: u64,
    pub utf16: u64,
    pub items: u16,
    pub wire: u32,
    pub binding: [u8; 16],
}

fn bad() -> Error {
    Error::NotePage(NotePageError::CursorInvalid)
}

impl Token {
    pub fn encode(&self, key: &[u8]) -> Result<String> {
        let mut bytes = Vec::with_capacity(95);
        bytes.extend_from_slice(self.snapshot.as_bytes());
        bytes.push(self.kind);
        bytes.extend_from_slice(&self.owner.to_le_bytes());
        bytes.extend_from_slice(&self.position.to_le_bytes());
        bytes.extend_from_slice(&self.utf16.to_le_bytes());
        bytes.extend_from_slice(&self.items.to_le_bytes());
        bytes.extend_from_slice(&self.wire.to_le_bytes());
        bytes.extend_from_slice(&self.binding);
        let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| invalid())?;
        mac.update(b"intent-note-annotation-v1\0");
        mac.update(&bytes);
        bytes.extend_from_slice(&mac.finalize().into_bytes());
        Ok(format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
    }
    pub fn decode(value: &str, key: &[u8]) -> Result<Self> {
        if value.len() > 256 {
            return Err(bad());
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(value.strip_prefix(PREFIX).ok_or_else(bad)?)
            .map_err(|_| bad())?;
        if bytes.len() != 95 {
            return Err(bad());
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| bad())?;
        mac.update(b"intent-note-annotation-v1\0");
        mac.update(&bytes[..63]);
        mac.verify_slice(&bytes[63..]).map_err(|_| bad())?;
        Ok(Self {
            snapshot: uuid::Uuid::from_slice(&bytes[..16]).map_err(|_| bad())?,
            kind: bytes[16],
            owner: u64::from_le_bytes(bytes[17..25].try_into().map_err(|_| bad())?),
            position: u64::from_le_bytes(bytes[25..33].try_into().map_err(|_| bad())?),
            utf16: u64::from_le_bytes(bytes[33..41].try_into().map_err(|_| bad())?),
            items: u16::from_le_bytes(bytes[41..43].try_into().map_err(|_| bad())?),
            wire: u32::from_le_bytes(bytes[43..47].try_into().map_err(|_| bad())?),
            binding: bytes[47..63].try_into().map_err(|_| bad())?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn annotation_tokens_bind_every_field_and_authenticate_before_decoding() {
        let token = Token {
            snapshot: uuid::Uuid::new_v4(),
            kind: 13,
            owner: 45,
            position: 1024,
            utf16: 700,
            items: 64,
            wire: 65_536,
            binding: [0; 16],
        };
        let key = [7; 32];
        let encoded = token.encode(&key).unwrap();
        assert!(encoded.len() <= 256);
        assert_eq!(Token::decode(&encoded, &key).unwrap(), token);
        assert!(Token::decode(&encoded, &[8; 32]).is_err());
        let original = URL_SAFE_NO_PAD
            .decode(encoded.strip_prefix(PREFIX).unwrap())
            .unwrap();
        for index in 0..original.len() {
            let mut bytes = original.clone();
            bytes[index] ^= 1;
            assert!(
                Token::decode(&format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)), &key).is_err()
            );
        }
        assert!(Token::decode("na1.bad", &key).is_err());
        assert!(Token::decode(&"x".repeat(257), &key).is_err());
    }
}
