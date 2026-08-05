//! Provider implementations of [`crate::VoiceEngine`].

pub mod elevenlabs;
pub mod openai;

#[cfg(test)]
pub(crate) mod mock_http;
