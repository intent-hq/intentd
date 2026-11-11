//! Opt-in round trip against the real E2B service. It costs a few cents and
//! does nothing unless `INTENT_E2B_LIVE_API_KEY` is set, so CI skips it.
//!
//! ```sh
//! INTENT_E2B_LIVE_API_KEY=e2b_... [INTENT_E2B_LIVE_TEMPLATE=base] \
//!   cargo nextest run -p intent-nodes live_e2b --no-capture
//! ```

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

const KEY_ENV: &str = "INTENT_E2B_LIVE_API_KEY";
const TEMPLATE_ENV: &str = "INTENT_E2B_LIVE_TEMPLATE";

#[tokio::test]
async fn live_e2b_round_trip() {
    let Some(key) = std::env::var(KEY_ENV).ok().filter(|k| !k.is_empty()) else {
        eprintln!("skipped: set {KEY_ENV} to run the live E2B test");
        return;
    };
    let template = std::env::var(TEMPLATE_ENV).unwrap_or_else(|_| "base".into());
    let client = E2bClient::new(SecretString::new(key), E2bConfig::default()).unwrap();
    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();
    let metadata = BTreeMap::from([("intentLiveTest".to_string(), run.clone())]);

    let sandbox = client
        .create(&CreateSandbox {
            template_id: template,
            timeout_secs: Some(120),
            auto_pause: Some(false),
            metadata: metadata.clone(),
            idempotency_key: Some(format!("intent-live-{run}")),
        })
        .await
        .unwrap();
    let id = sandbox.sandbox_id.clone();
    eprintln!("created {id}");

    let result = async {
        let mut spec = ProcessSpec::new("/bin/sh");
        spec.args = vec!["-c".into(), "echo out; echo err >&2; exit 3".into()];
        let output = client.run_process(&sandbox, &spec).await?;
        assert_eq!(output.stdout, b"out\n");
        assert_eq!(output.stderr, b"err\n");
        assert_eq!(output.exit.exit_code, 3);

        client.set_timeout(&id, 180).await?;
        let listed = client
            .list(&ListSandboxes {
                metadata: metadata.clone(),
                states: vec![SandboxState::Running, SandboxState::Paused],
            })
            .await?;
        assert_eq!(
            listed
                .iter()
                .map(|s| s.sandbox_id.as_str())
                .collect::<Vec<_>>(),
            [id.as_str()]
        );

        client.pause(&id).await?;
        let connected = client.connect(&id, 120).await?;
        assert!(connected.resumed);
        let echo = client
            .run_process(&connected.sandbox, &{
                let mut spec = ProcessSpec::new("/bin/echo");
                spec.args = vec!["resumed".into()];
                spec
            })
            .await?;
        assert_eq!(echo.stdout, b"resumed\n");
        Ok::<_, E2bError>(())
    }
    .await;

    let deleted = client.delete(&id).await;
    result.unwrap();
    assert!(deleted.unwrap());
    assert!(!client.delete(&id).await.unwrap());
}

/// The same round trip through [`crate::cloud::CloudProvider`].
#[tokio::test]
async fn live_e2b_provider_round_trip() {
    use crate::cloud::{CloudProvider, CreateInstance, InstanceState, ListInstances};
    let Some(key) = std::env::var(KEY_ENV).ok().filter(|k| !k.is_empty()) else {
        eprintln!("skipped: set {KEY_ENV} to run the live E2B test");
        return;
    };
    let template = std::env::var(TEMPLATE_ENV).unwrap_or_else(|_| "base".into());
    let provider: Box<dyn CloudProvider> =
        Box::new(E2bClient::new(SecretString::new(key), E2bConfig::default()).unwrap());
    provider.check_key().await.unwrap();
    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();
    let tags = BTreeMap::from([("intentLiveTest".to_string(), run.clone())]);
    let instance = provider
        .create(&CreateInstance {
            image: template,
            timeout_secs: Some(120),
            pause_on_timeout: Some(false),
            tags: tags.clone(),
            idempotency_key: Some(format!("intent-live-provider-{run}")),
            ..CreateInstance::default()
        })
        .await
        .unwrap();
    let id = instance.id.clone();
    eprintln!("created {id}");

    let result = async {
        let mut spec = ProcessSpec::new("/bin/sh");
        spec.args = vec!["-c".into(), "echo out; exit 3".into()];
        let output = provider.run_command(&instance, &spec).await?;
        assert_eq!(output.stdout, b"out\n");
        assert_eq!(output.exit.exit_code, 3);

        provider.keep_alive(&id, 180).await?;
        let listed = provider
            .list(&ListInstances {
                tags: tags.clone(),
                states: vec![InstanceState::Running, InstanceState::Paused],
            })
            .await?;
        assert_eq!(
            listed.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
            [id.as_str()]
        );

        provider.pause(&id).await?;
        let connected = provider.connect(&id, 120).await?;
        assert!(connected.resumed);
        let mut echo = ProcessSpec::new("/bin/echo");
        echo.args = vec!["resumed".into()];
        let echoed = provider.run_command(&connected.instance, &echo).await?;
        assert_eq!(echoed.stdout, b"resumed\n");
        Ok::<_, crate::cloud::CloudError>(())
    }
    .await;

    let deleted = provider.delete(&id).await;
    result.unwrap();
    assert!(deleted.unwrap());
    assert!(!provider.delete(&id).await.unwrap());
}
