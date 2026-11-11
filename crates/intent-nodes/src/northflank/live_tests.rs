//! Opt-in round trip against the real Northflank API. It starts a small
//! service for a few minutes and does nothing unless both
//! `NORTHFLANK_API_TOKEN` and `NORTHFLANK_PROJECT_ID` are set, so CI skips it.
//! It also runs a command over the command execution WebSocket, whose
//! protocol is not in Northflank's REST reference, so a change there
//! fails here first.
//!
//! ```sh
//! NORTHFLANK_API_TOKEN=... NORTHFLANK_PROJECT_ID=... \
//!   [NORTHFLANK_LIVE_IMAGE=ghcr.io/intent-hq/intent-node:latest] \
//!   [NORTHFLANK_LIVE_PLAN=nf-compute-200] \
//!   cargo nextest run -p intent-nodes live_northflank --no-capture
//! ```

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;
use crate::cloud::{
    CloudError, CloudProvider, CreateInstance, InstanceState, ListInstances, ProcessSpec,
};

const TOKEN_ENV: &str = "NORTHFLANK_API_TOKEN";
const PROJECT_ENV: &str = "NORTHFLANK_PROJECT_ID";
const IMAGE_ENV: &str = "NORTHFLANK_LIVE_IMAGE";
const PLAN_ENV: &str = "NORTHFLANK_LIVE_PLAN";

fn shell(script: &str) -> ProcessSpec {
    let mut spec = ProcessSpec::new("/bin/sh");
    spec.args = vec!["-c".into(), script.into()];
    spec
}

#[tokio::test]
async fn live_northflank_round_trip() {
    let token = std::env::var(TOKEN_ENV).ok().filter(|v| !v.is_empty());
    let project = std::env::var(PROJECT_ENV).ok().filter(|v| !v.is_empty());
    let (Some(token), Some(project)) = (token, project) else {
        eprintln!("skipped: set {TOKEN_ENV} and {PROJECT_ENV} to run the live Northflank test");
        return;
    };
    let image =
        std::env::var(IMAGE_ENV).unwrap_or_else(|_| "ghcr.io/intent-hq/intent-node:latest".into());
    let plan = std::env::var(PLAN_ENV).unwrap_or_else(|_| "nf-compute-200".into());
    let client = NorthflankClient::new(
        SecretString::new(token),
        NorthflankConfig {
            project_id: project.clone(),
            ..NorthflankConfig::default()
        },
    )
    .unwrap();

    client.check_key().await.unwrap();
    let info = client.project().await.unwrap();
    assert_eq!(info.id, project);
    eprintln!("project {} in {}", info.id, info.region);
    assert!(
        client.image_ready(&image).await.unwrap(),
        "{image} is not public"
    );

    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string();
    let tags = BTreeMap::from([
        ("intentBackendId".to_string(), format!("live-{run}")),
        ("intentLiveTest".to_string(), run.clone()),
    ]);
    let instance = client
        .create(&CreateInstance {
            image,
            tags: tags.clone(),
            plan: Some(plan),
            disk_gib: Some(5),
            ..CreateInstance::default()
        })
        .await
        .unwrap();
    let id = instance.id.clone();
    eprintln!("created {id} with volume {:?}", instance.volume_id);

    let result = async {
        let output = client
            .run_command(&instance, &shell("echo out; echo err >&2; exit 3"))
            .await?;
        assert_eq!(output.stdout, b"out\n");
        assert_eq!(output.stderr, b"err\n");
        assert_eq!(output.exit.exit_code, 3);
        let wrote = client
            .run_command(
                &instance,
                &shell("echo kept > /home/user/live-test && echo $HOME"),
            )
            .await?;
        assert!(wrote.success(), "{wrote:?}");
        assert_eq!(wrote.stdout, b"/home/user\n");

        let listed = client
            .list(&ListInstances {
                tags: tags.clone(),
                states: vec![],
            })
            .await?;
        assert_eq!(listed.len(), 1, "{listed:?}");
        assert_eq!(listed[0].id, id);
        assert_eq!(listed[0].state, InstanceState::Running);
        assert_eq!(listed[0].tags, tags);

        client.pause(&id).await?;
        let connected = client.connect(&id, 600).await?;
        assert!(connected.resumed);
        let read = client
            .run_command(&connected.instance, &shell("cat /home/user/live-test"))
            .await?;
        assert_eq!(read.stdout, b"kept\n");
        Ok::<_, CloudError>(())
    }
    .await;

    let deleted = client.delete(&id).await;
    result.unwrap();
    assert!(deleted.unwrap());
    assert!(!client.delete(&id).await.unwrap());
}
