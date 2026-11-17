//! Opt-in round trip against the real Northflank API. It starts a small
//! service for a few minutes and does nothing unless both
//! a token (`NORTHFLANK_API_TOKEN`, or a file holding it in
//! `NORTHFLANK_API_TOKEN_FILE`) and `NORTHFLANK_PROJECT_ID` are set. No CI
//! job sets them: it runs by hand only.
//! It also runs a command over the command execution WebSocket, whose
//! protocol is not in Northflank's REST reference, so a change there
//! fails here first.
//!
//! The machine, its volume and the run's backend team tag are deleted
//! whatever happens after the create, and the test then checks nothing of
//! the run is left. `NORTHFLANK_LIVE_FAIL=1` makes the run fail on purpose
//! once the machine is up, to show that.
//!
//! ```sh
//! NORTHFLANK_API_TOKEN_FILE=~/.northflank-token NORTHFLANK_PROJECT_ID=... \
//!   [NORTHFLANK_LIVE_IMAGE=ghcr.io/intent-hq/intent-node:latest] \
//!   [NORTHFLANK_LIVE_PLAN=nf-compute-200] \
//!   cargo nextest run -p intent-nodes live_northflank --no-capture
//! ```

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::FutureExt as _;
use reqwest::Method;

use super::client::Call;
use super::*;
use crate::cloud::{
    CloudError, CloudProvider, CreateInstance, InstanceState, ListInstances, ProcessSpec,
};

const TOKEN_ENV: &str = "NORTHFLANK_API_TOKEN";
const TOKEN_FILE_ENV: &str = "NORTHFLANK_API_TOKEN_FILE";
const PROJECT_ENV: &str = "NORTHFLANK_PROJECT_ID";
const IMAGE_ENV: &str = "NORTHFLANK_LIVE_IMAGE";
const PLAN_ENV: &str = "NORTHFLANK_LIVE_PLAN";
const FAIL_ENV: &str = "NORTHFLANK_LIVE_FAIL";

#[derive(serde::Deserialize)]
struct TeamTag {
    id: String,
    name: String,
}

/// Delete everything listed with `backend`'s tags until nothing is: a
/// Northflank delete returns before the service and its volume are gone.
/// The team tag goes last, once nothing carries it, so a leftover is never
/// stripped of the tag that finds it.
async fn clean_up(client: &NorthflankClient, backend: &str) -> Result<(), String> {
    let filter = ListInstances {
        tags: BTreeMap::from([("intentBackendId".to_string(), backend.to_string())]),
        states: vec![],
    };
    for _ in 0..60 {
        let left = client.list(&filter).await.map_err(|e| e.to_string())?;
        if left.is_empty() {
            return delete_team_tag(client, &backend_tag(backend))
                .await
                .map_err(|e| e.to_string());
        }
        for machine in &left {
            let _ = client.delete(&machine.id).await;
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
    let left = client.list(&filter).await;
    Err(format!("still there after 5 minutes: {left:?}"))
}

/// Delete the team tag `name` if it exists.
async fn delete_team_tag(client: &NorthflankClient, name: &str) -> Result<(), CloudError> {
    let tags: Vec<TeamTag> = client
        .list_all(Call::new("list-tags"), "/tags", Some("tags"))
        .await?;
    for tag in tags.iter().filter(|t| t.name == name) {
        client
            .request(
                Call::new("delete-tag"),
                Method::DELETE,
                &format!("/tags/{}", tag.id),
                &[],
                None,
            )
            .await?;
    }
    Ok(())
}

fn shell(script: &str) -> ProcessSpec {
    let mut spec = ProcessSpec::new("/bin/sh");
    spec.args = vec!["-c".into(), script.into()];
    spec
}

#[tokio::test]
async fn live_northflank_round_trip() {
    let token = std::env::var(TOKEN_ENV)
        .ok()
        .or_else(|| {
            let path = std::env::var(TOKEN_FILE_ENV).ok()?;
            std::fs::read_to_string(path).ok()
        })
        .map(|token| token.trim().to_owned())
        .filter(|v| !v.is_empty());
    let project = std::env::var(PROJECT_ENV).ok().filter(|v| !v.is_empty());
    let (Some(token), Some(project)) = (token, project) else {
        eprintln!(
            "skipped: set {TOKEN_ENV} (or {TOKEN_FILE_ENV}) and {PROJECT_ENV} to run the live Northflank test"
        );
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
    let backend = format!("live-{run}");
    let tags = BTreeMap::from([
        ("intentBackendId".to_string(), backend.clone()),
        ("intentLiveTest".to_string(), run.clone()),
    ]);
    let created = client
        .create(&CreateInstance {
            image,
            tags: tags.clone(),
            plan: Some(plan),
            disk_gib: Some(10),
            ..CreateInstance::default()
        })
        .await;
    let instance = match created {
        Ok(instance) => instance,
        Err(error) => {
            let cleaned = clean_up(&client, &backend).await;
            panic!("create failed: {error} (clean-up: {cleaned:?})");
        }
    };
    let id = instance.id.clone();
    eprintln!("created {id} with volume {:?}", instance.volume_id);

    let result = AssertUnwindSafe(async {
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
        let by_backend = client
            .list(&ListInstances {
                tags: BTreeMap::from([("intentBackendId".to_string(), backend.clone())]),
                states: vec![],
            })
            .await?;
        assert_eq!(by_backend.len(), 1, "{by_backend:?}");
        assert!(
            std::env::var(FAIL_ENV).is_err(),
            "failing on purpose ({FAIL_ENV} is set)"
        );

        client.pause(&id).await?;
        let connected = client.connect(&id, 600).await?;
        assert!(connected.resumed);
        let read = client
            .run_command(&connected.instance, &shell("cat /home/user/live-test"))
            .await?;
        assert_eq!(read.stdout, b"kept\n");
        Ok::<_, CloudError>(())
    })
    .catch_unwind()
    .await;

    let deleted = client.delete(&id).await;
    let cleaned = clean_up(&client, &backend).await;
    let deleted_again = client.delete(&id).await;
    eprintln!("cleaned up {id}: deleted {deleted:?}, then {cleaned:?}");
    match result {
        Err(panic) => std::panic::resume_unwind(panic),
        Ok(result) => result.unwrap(),
    }
    assert!(deleted.unwrap());
    cleaned.unwrap();
    assert!(!deleted_again.unwrap());
}
