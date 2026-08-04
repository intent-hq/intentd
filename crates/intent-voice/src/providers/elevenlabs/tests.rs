use super::*;
use crate::providers::mock_http;

fn request() -> TranscribeRequest {
    TranscribeRequest {
        audio: vec![1, 2, 3, 4],
        mime_type: "audio/webm".to_string(),
        language: Some("en".to_string()),
        keyterms: vec!["intentd".to_string(), "clippy".to_string()],
        prompt: None,
    }
}

#[test]
fn debug_redacts_api_key() {
    let engine = ElevenLabsEngine::new("xi-supersecret", None).unwrap();
    let dbg = format!("{engine:?}");
    assert!(!dbg.contains("supersecret"));
    assert!(dbg.contains("redacted"));
    assert!(dbg.contains(ELEVENLABS_API_BASE_URL));
}

#[test]
fn file_names_follow_mime_type() {
    assert_eq!(file_name_for("audio/webm"), "audio.webm");
    assert_eq!(file_name_for("audio/wav"), "audio.wav");
    assert_eq!(file_name_for("audio/mpeg"), "audio.mp3");
    assert_eq!(file_name_for("audio/webm;codecs=opus"), "audio.webm");
    assert_eq!(file_name_for("application/octet-stream"), "audio.webm");
}

#[tokio::test]
async fn transcribes_and_sends_model_and_keyterms() {
    let body = serde_json::json!({
        "text": "hello world",
        "words": [
            { "text": "hello", "start": 0.0, "end": 0.4 },
            { "text": "world", "start": 0.5, "end": 1.2 }
        ]
    })
    .to_string();
    let (base_url, captured) = mock_http::spawn(vec![(200, body)]).await;
    let engine = ElevenLabsEngine::new("xi-key", Some(&base_url)).unwrap();
    let t = engine.transcribe(request()).await.unwrap();
    assert_eq!(t.text, "hello world");
    assert_eq!(t.duration_ms, Some(1200));

    let reqs = captured.lock().unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(reqs[0].head.contains("POST /v1/speech-to-text"));
    assert!(reqs[0].head.contains("xi-api-key: xi-key"));
    let body = reqs[0].body_text();
    assert!(body.contains("name=\"model_id\""));
    assert!(body.contains("scribe_v2"));
    assert!(body.contains("name=\"language_code\""));
    // Repeated keyterms parts, one per term.
    assert_eq!(body.matches("name=\"keyterms\"").count(), 2);
    assert!(body.contains("intentd"));
    assert!(body.contains("clippy"));
}

#[tokio::test]
async fn missing_text_maps_to_decode_error() {
    let (base_url, _) = mock_http::spawn(vec![(200, "{}".to_string())]).await;
    let engine = ElevenLabsEngine::new("xi-key", Some(&base_url)).unwrap();
    let err = engine.transcribe(request()).await.unwrap_err();
    assert!(matches!(err, Error::Decode(_)));
}

#[tokio::test]
async fn unauthorized_maps_to_auth_error() {
    let (base_url, _) =
        mock_http::spawn(vec![(401, r#"{"detail":"invalid api key"}"#.to_string())]).await;
    let engine = ElevenLabsEngine::new("xi-bad", Some(&base_url)).unwrap();
    let err = engine.transcribe(request()).await.unwrap_err();
    assert!(matches!(err, Error::Auth(_)), "got {err:?}");
}

#[tokio::test]
async fn rate_limit_maps_to_rate_limited() {
    let (base_url, _) = mock_http::spawn(vec![(429, "{}".to_string())]).await;
    let engine = ElevenLabsEngine::new("xi-key", Some(&base_url)).unwrap();
    let err = engine.transcribe(request()).await.unwrap_err();
    assert!(matches!(err, Error::RateLimited(_)), "got {err:?}");
}
