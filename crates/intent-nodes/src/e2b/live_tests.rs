//! Opt-in round trip against the real E2B service. It costs a few cents and
//! does nothing unless `INTENT_E2B_LIVE_API_KEY` is set, so CI skips it.
//!
//! ```sh
//! INTENT_E2B_LIVE_API_KEY=e2b_... [INTENT_E2B_LIVE_TEMPLATE=base] \
//!   cargo nextest run -p intent-nodes live_e2b_round_trip --no-capture
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
