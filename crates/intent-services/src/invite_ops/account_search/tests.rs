use super::*;
use crate::invite_ops::tests::fixture;
use crate::tests::TempDb;
use intent_core::{with_caller, Caller};
use intent_sourcecontrol::gitlab_auth;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::test]
async fn invitation_account_search_authorizes_before_validation_or_io() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    assert!(matches!(
        f.services
            .host_invite_search_accounts_op("github", None, "ab", None)
            .await,
        Err(Error::Forbidden(_))
    ));
    for host_role in [intent_core::HostRole::Guest, intent_core::HostRole::Member] {
        let caller = Caller::Wire {
            principal_id: f.collaborator.clone(),
            host_role,
        };
        let error = with_caller(
            caller,
            f.services
                .host_invite_search_accounts_op("invalid", Some("invalid/path"), "ab", None),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::Forbidden(_)), "{error}");
    }
}

#[tokio::test]
async fn invitation_account_search_validates_and_short_circuits_without_credentials() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    for (provider, host) in [
        ("github", None),
        ("github", Some(" GITHUB.COM ")),
        ("gitlab", None),
        ("gitlab", Some(" GL.Custom.Example:8443 ")),
    ] {
        for query in ["", " ", "@", " @a "] {
            let result = with_caller(
                Caller::Daemon,
                f.services
                    .host_invite_search_accounts_op(provider, host, query, None),
            )
            .await
            .unwrap();
            assert_eq!(result, json!({"users":[]}));
        }
    }
    for (provider, host, query, limit) in [
        ("other", None, "ab", None),
        ("github", Some("gitlab.com"), "ab", None),
        ("gitlab", Some("https://gitlab.com"), "ab", None),
        ("gitlab", Some("gitlab.com/path"), "ab", None),
        ("gitlab", Some("user@gitlab.com"), "ab", None),
        ("gitlab", Some("gitlab.com?q=x"), "ab", None),
        ("gitlab", Some(""), "ab", None),
        ("github", None, "a OR b", None),
        ("github", None, "a.b", None),
        ("gitlab", None, "a/b", None),
        ("gitlab", None, "a\nb", None),
        ("github", None, "ab", Some(0)),
        ("gitlab", None, "ab", Some(11)),
    ] {
        assert!(matches!(
            with_caller(
                Caller::Daemon,
                f.services
                    .host_invite_search_accounts_op(provider, host, query, limit)
            )
            .await,
            Err(Error::InvalidParams(_))
        ));
    }
    for (provider, length) in [("github", 40), ("gitlab", 256)] {
        assert!(matches!(
            with_caller(
                Caller::Daemon,
                f.services.host_invite_search_accounts_op(
                    provider,
                    None,
                    &"x".repeat(length),
                    None
                )
            )
            .await,
            Err(Error::InvalidParams(_))
        ));
    }
}

async fn provider_reply(body: Value, status: u16) -> (String, tokio::task::JoinHandle<String>) {
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
        let body = body.to_string();
        stream.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        String::from_utf8(bytes).unwrap()
    });
    (base, task)
}

#[tokio::test]
async fn invitation_account_search_qualifies_filters_and_deduplicates() {
    for provider in ["github", "gitlab"] {
        let tmp = TempDb::new();
        let mut f = fixture(&tmp).await;
        let rows = json!([
            {"id":42,"login":"al-ice","name":"Alice","avatar_url":"https://avatar.example/a","email":"private"},
            {"id":42,"login":"al-ice"}, {"id":0,"login":"invalid-id"}, {"id":8,"login":"bad/path"},
            {"id":9,"login":"alex"}
        ]);
        let (base, request) = provider_reply(
            if provider == "github" {
                json!({"items":rows})
            } else {
                rows
            },
            200,
        )
        .await;
        let cfg = crate::test_support::test_tempdir("invitation-search-settings");
        let registry =
            Arc::new(crate::SettingsRegistry::load(cfg.path().join("config.toml")).unwrap());
        registry
            .apply(&[
                (
                    "sourceControl.gitlab.host".into(),
                    json!("gl.custom.example:8443"),
                ),
                ("sourceControl.gitlab.apiBaseUrl".into(), json!(base)),
            ])
            .unwrap();
        f.services = f
            .services
            .with_github_api_base_uri(&base)
            .with_settings_registry(registry);
        let host = if provider == "gitlab" {
            "gl.custom.example:8443"
        } else {
            "github.com"
        };
        let caller = Caller::Wire {
            principal_id: f.primary.clone(),
            host_role: intent_core::HostRole::Owner,
        };
        let result = with_caller(
            caller,
            f.services.host_invite_search_accounts_op(
                provider,
                Some(&host.to_ascii_uppercase()),
                " @al ",
                None,
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            result,
            json!({"users":[
                {"identity":{"provider":provider,"host":host,"externalUserId":"42"},"login":"al-ice","name":"Alice","avatarUrl":"https://avatar.example/a"},
                {"identity":{"provider":provider,"host":host,"externalUserId":"9"},"login":"alex","name":null,"avatarUrl":null}
            ]})
        );
        let request = request.await.unwrap();
        assert!(!request.to_ascii_lowercase().contains("authorization:"));
        assert!(f.store.list_open_host_invites().await.unwrap().is_empty());
        assert!(f
            .services
            .own_gitlab_token(&gitlab_auth::GitlabHost::parse("another.example").unwrap())
            .await
            .is_none());
    }
}

#[tokio::test]
async fn invitation_search_never_sends_bound_token_to_other_host_or_port() {
    let tmp = TempDb::new();
    let mut f = fixture(&tmp).await;
    let cfg = crate::test_support::test_tempdir("invitation-search-origin");
    let registry = Arc::new(crate::SettingsRegistry::load(cfg.path().join("config.toml")).unwrap());
    registry
        .apply(&[(
            "sourceControl.gitlab.host".into(),
            json!("gitlab.bound.example"),
        )])
        .unwrap();
    // Synthetic fixture only; search must leave even this token file unchanged.
    let secrets = intent_core::FileSecretStore::with_path(cfg.path().join("fixture-secrets.json"));
    secrets
        .store(
            intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT,
            "synthetic-fixture-token",
        )
        .unwrap();
    let before = std::fs::read(secrets.path()).unwrap();
    f.services = f
        .services
        .with_settings_registry(registry)
        .with_gitlab_secret_store(secrets.clone());
    for canonical in ["gitlab.foreign.example", "gitlab.bound.example:8443"] {
        let (base, request) = provider_reply(json!({}), 401).await;
        let host = intent_sourcecontrol::GitlabHost::parse(canonical)
            .unwrap()
            .with_api_origin(&base)
            .unwrap();
        let result = f
            .services
            .search_gitlab_invitation_accounts(&host, "ab", 8)
            .await;
        assert!(
            matches!(result, Err(intent_sourcecontrol::Error::Auth(_))),
            "{result:?}"
        );
        assert!(!request
            .await
            .unwrap()
            .to_ascii_lowercase()
            .contains("authorization:"));
    }
    assert_eq!(std::fs::read(secrets.path()).unwrap(), before);
}
