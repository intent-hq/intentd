use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn reply(
    status: u16,
    body: String,
    headers: String,
) -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        loop {
            let mut chunk = [0; 1024];
            let n = stream.read(&mut chunk).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&chunk[..n]);
            if bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let response = format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n{body}", body.len());
        let _ = stream.write_all(response.as_bytes()).await;
        String::from_utf8(bytes).unwrap()
    });
    (base, task)
}

#[tokio::test]
async fn github_public_search_encodes_and_bounds_results() {
    let body = serde_json::json!({"items":[{"id":42,"login":"octo","name":null,"avatar_url":"https://avatars.example/42"},{"id":43,"login":"other"}]}).to_string();
    let (base, request) = reply(200, body, String::new()).await;
    let users = github(Some(&base), "octo", 1).await.unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].id, Some(42));
    assert_eq!(
        users[0].avatar_url.as_deref(),
        Some("https://avatars.example/42")
    );
    let request = request.await.unwrap();
    assert!(
        request.starts_with("GET /search/users?q=octo+in%3Alogin+type%3Auser&per_page=1&page=1 "),
        "{request}"
    );
    assert!(!request.to_ascii_lowercase().contains("authorization:"));
}

#[tokio::test]
async fn gitlab_selected_origin_search_has_one_encoded_page() {
    let (base, request) = reply(
        200,
        r#"[{"id":7,"username":"a_b","name":"A B","email":"private"}]"#.into(),
        String::new(),
    )
    .await;
    let host = GitlabHost::parse("gitlab.custom.example:8443")
        .unwrap()
        .with_api_origin(&base)
        .unwrap();
    let users = gitlab(&host, Some("fixture-token"), "a_b", 8)
        .await
        .unwrap();
    assert_eq!(users[0].login, "a_b");
    assert_eq!(users[0].name.as_deref(), Some("A B"));
    let request = request.await.unwrap();
    assert!(
        request.starts_with("GET /api/v4/users?search=a_b&per_page=8&page=1 "),
        "{request}"
    );
    assert!(request.contains("authorization: Bearer fixture-token"));
}

#[tokio::test]
async fn restricted_rate_disabled_and_malformed_are_errors() {
    for (status, body, kind) in [
        (401, "{}", "auth"),
        (403, "{}", "auth"),
        (429, "{}", "rate"),
        (404, "{}", "api"),
        (500, "{}", "api"),
        (200, "{}", "decode"),
        (200, "not json", "decode"),
    ] {
        let (base, request) = reply(status, body.into(), String::new()).await;
        let host = GitlabHost::parse(&base).unwrap();
        let error = gitlab(&host, None, "ab", 8).await.unwrap_err();
        assert!(
            match kind {
                "auth" => matches!(error, Error::Auth(_)),
                "rate" => matches!(error, Error::RateLimited(_)),
                "decode" => matches!(error, Error::Decode(_)),
                _ => matches!(error, Error::Api(_)),
            },
            "{error:?}"
        );
        request.await.unwrap();
    }
    let (base, request) = reply(
        403,
        r#"{"message":"API rate limit exceeded"}"#.into(),
        String::new(),
    )
    .await;
    assert!(matches!(
        github(Some(&base), "ab", 8).await,
        Err(Error::RateLimited(_))
    ));
    request.await.unwrap();
}

#[tokio::test]
async fn never_follows_redirect_or_forwards_credentials() {
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    for status in [301, 302, 307, 308] {
        let (base, request) = reply(
            status,
            String::new(),
            format!(
                "Location: http://{}/stolen\r\n",
                destination.local_addr().unwrap()
            ),
        )
        .await;
        assert!(matches!(
            gitlab(
                &GitlabHost::parse(&base).unwrap(),
                Some("fixture-token"),
                "ab",
                8
            )
            .await,
            Err(Error::Api(_))
        ));
        request.await.unwrap();
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), destination.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn oversized_response_is_rejected() {
    let (base, request) = reply(200, " ".repeat(MAX_RESPONSE_BYTES + 1), String::new()).await;
    assert!(matches!(
        github(Some(&base), "ab", 8).await,
        Err(Error::Decode(_))
    ));
    request.await.unwrap();
}
