use super::*;
use crate::providers::mock_http;

fn request() -> TranscribeRequest {
    TranscribeRequest {
        audio: vec![9, 9, 9],
        mime_type: "audio/webm".to_string(),
        language: Some("en".to_string()),
        keyterms: vec![],
        prompt: Some("Technical dictation. Vocabulary: intentd.".to_string()),
    }
}

#[test]
fn debug_redacts_api_key() {
    let engine = OpenAiEngine::new("sk-supersecret", None, None).unwrap();
    let dbg = format!("{engine:?}");
    assert!(!dbg.contains("supersecret"));
    assert!(dbg.contains("redacted"));
    assert!(dbg.contains(OPENAI_API_BASE_URL));
}

#[test]
fn model_defaults_when_unset_or_blank() {
    let engine = OpenAiEngine::new("sk-key", None, None).unwrap();
    assert_eq!(engine.model, DEFAULT_MODEL);
    let engine = OpenAiEngine::new("sk-key", None, Some("  ")).unwrap();
    assert_eq!(engine.model, DEFAULT_MODEL);
    let engine = OpenAiEngine::new("sk-key", None, Some("whisper-1")).unwrap();
    assert_eq!(engine.model, "whisper-1");
}

#[tokio::test]
async fn transcribes_with_default_model_and_prompt() {
    let body = serde_json::json!({ "text": "release notes", "duration": 2.5 }).to_string();
    let (base_url, captured) = mock_http::spawn(vec![(200, body)]).await;
    let engine = OpenAiEngine::new("sk-key", Some(&base_url), None).unwrap();
    let t = engine.transcribe(request()).await.unwrap();
    assert_eq!(t.text, "release notes");
    assert_eq!(t.duration_ms, Some(2500));

    let reqs = captured.lock().unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(reqs[0].head.contains("POST /v1/audio/transcriptions"));
    assert!(
        reqs[0].head.contains("authorization: Bearer sk-key")
            || reqs[0].head.contains("Authorization: Bearer sk-key")
    );
    let body = reqs[0].body_text();
    assert!(body.contains("name=\"model\""));
    assert!(body.contains("gpt-4o-transcribe"));
    assert!(body.contains("name=\"prompt\""));
    assert!(body.contains("Vocabulary: intentd."));
    assert!(body.contains("name=\"language\""));
}

#[tokio::test]
async fn transcribes_with_configured_model() {
    let body = serde_json::json!({ "text": "mini output" }).to_string();
    let (base_url, captured) = mock_http::spawn(vec![(200, body)]).await;
    let engine =
        OpenAiEngine::new("sk-key", Some(&base_url), Some("gpt-4o-mini-transcribe")).unwrap();
    let t = engine.transcribe(request()).await.unwrap();
    assert_eq!(t.text, "mini output");

    let reqs = captured.lock().unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(reqs[0].body_text().contains("gpt-4o-mini-transcribe"));
}

#[tokio::test]
async fn falls_back_to_whisper_when_selected_model_unavailable() {
    let not_found = r#"{"error":{"message":"model not found"}}"#.to_string();
    let ok = serde_json::json!({ "text": "fallback works" }).to_string();
    let (base_url, captured) = mock_http::spawn(vec![(404, not_found), (200, ok)]).await;
    let engine =
        OpenAiEngine::new("sk-key", Some(&base_url), Some("gpt-4o-mini-transcribe")).unwrap();
    let t = engine.transcribe(request()).await.unwrap();
    assert_eq!(t.text, "fallback works");
    assert_eq!(t.duration_ms, None);

    let reqs = captured.lock().unwrap();
    assert_eq!(reqs.len(), 2);
    assert!(reqs[0].body_text().contains("gpt-4o-mini-transcribe"));
    assert!(reqs[1].body_text().contains("whisper-1"));
}

#[tokio::test]
async fn no_fallback_when_whisper_selected() {
    let not_found = r#"{"error":{"message":"model not found"}}"#.to_string();
    let (base_url, captured) = mock_http::spawn(vec![(404, not_found)]).await;
    let engine = OpenAiEngine::new("sk-key", Some(&base_url), Some("whisper-1")).unwrap();
    let err = engine.transcribe(request()).await.unwrap_err();
    assert!(matches!(err, Error::NotConfigured(_)), "got {err:?}");

    let reqs = captured.lock().unwrap();
    assert_eq!(reqs.len(), 1, "no fallback when whisper-1 is selected");
    assert!(reqs[0].body_text().contains("whisper-1"));
}

#[tokio::test]
async fn unauthorized_maps_to_auth_error_without_fallback() {
    let (base_url, captured) =
        mock_http::spawn(vec![(401, r#"{"error":"bad key"}"#.to_string())]).await;
    let engine = OpenAiEngine::new("sk-bad", Some(&base_url), None).unwrap();
    let err = engine.transcribe(request()).await.unwrap_err();
    assert!(matches!(err, Error::Auth(_)), "got {err:?}");
    assert_eq!(captured.lock().unwrap().len(), 1, "no fallback on 401");
}

#[tokio::test]
async fn missing_text_maps_to_decode_error() {
    let (base_url, _) = mock_http::spawn(vec![(200, "{}".to_string())]).await;
    let engine = OpenAiEngine::new("sk-key", Some(&base_url), None).unwrap();
    let err = engine.transcribe(request()).await.unwrap_err();
    assert!(matches!(err, Error::Decode(_)));
}
