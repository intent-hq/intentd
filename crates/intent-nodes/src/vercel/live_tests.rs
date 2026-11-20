//! Opt-in round trip against the real Vercel Sandbox API, through the
//! adapter: check the token and image, create a sandbox from the published
//! image, start intentd with its tunnel, renew, stop, resume (a new
//! session: the disk kept, intentd started again from it), and delete. It
//! runs only with `VERCEL_LIVE=1`, a token (`VERCEL_TOKEN` or
//! `VERCEL_TOKEN_FILE`), `VERCEL_TEAM_ID` and `VERCEL_PROJECT_ID`; no CI
//! job sets them. It deletes every sandbox it made, with its snapshots,
//! whatever happens, and checks nothing is left.
//!
//! ```text
//! VERCEL_LIVE=1 VERCEL_TOKEN_FILE=~/.config/vercel-sandbox/token \
//!   VERCEL_TEAM_ID=team_... VERCEL_PROJECT_ID=prj_... \
//!   cargo test -p intent-nodes --lib -- --exact --nocapture \
//!   vercel::live_tests::a_live_round_trip_leaves_nothing_behind
//! ```

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::time::Instant;

use futures_util::FutureExt as _;

use super::capture::{live_scope, live_token, Live, DEFAULT_IMAGE};
use super::*;
use crate::cloud::{CloudProvider, CreateInstance, InstanceState, ListInstances, ProcessSpec};

const LIVE_ENV: &str = "VERCEL_LIVE";
const LIVE_BACKEND: &str = "intent-live-test";

/// Start intentd with its listener and tunnel, then print its status once
/// it has a tunnel address.
const START: &str = r#"set -eu
export INTENTD_DATA_DIR="$HOME/.intent"
mkdir -p "$INTENTD_DATA_DIR"
printf '[server.wsApi]\nenabled = true\n\n[server.tunnel]\nenabled = true\n' > "$INTENTD_DATA_DIR/config.toml"
setsid nohup intentd serve >"$INTENTD_DATA_DIR/serve.log" 2>&1 </dev/null &
i=0
until intentd call system.status 2>/dev/null | grep -q '"tcAddress"'; do
  i=$((i + 1)); [ "$i" -ge 60 ] && { tail -n 5 "$INTENTD_DATA_DIR/serve.log" >&2; exit 3; }
  sleep 1
done
echo tunnel-up
"#;

fn sh(script: &str) -> ProcessSpec {
    let mut spec = ProcessSpec::new("/bin/sh");
    spec.args = vec!["-c".into(), script.into()];
    spec
}

async fn round_trip(client: &VercelClient, image: &str) {
    let started = Instant::now();
    let lap = |what: &str| {
        eprintln!(
            "[vercel] {what} at {:.1} s",
            started.elapsed().as_secs_f64()
        );
    };
    client.check_key().await.unwrap();
    assert!(client.image_ready(image).await.unwrap(), "no image {image}");
    lap("token and image checked");
    let tags = BTreeMap::from([
        (BACKEND_ID_TAG.to_owned(), LIVE_BACKEND.to_owned()),
        ("agentId".to_owned(), "live".to_owned()),
    ]);
    let instance = client
        .create(&CreateInstance {
            image: image.to_owned(),
            timeout_secs: Some(600),
            tags: tags.clone(),
            vcpu: Some(2),
            memory_gib: Some(4),
            ..CreateInstance::default()
        })
        .await
        .unwrap();
    lap("created");

    let who = client
        .run_command(&instance, &ProcessSpec::new("id"))
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&who.stdout).contains("(user)"),
        "{who:?}"
    );
    let up = client.run_command(&instance, &sh(START)).await.unwrap();
    assert!(up.success(), "{}", String::from_utf8_lossy(&up.stderr));
    lap("intentd and its tunnel up");
    client
        .run_command(&instance, &sh("echo kept > \"$HOME/marker\""))
        .await
        .unwrap();

    client.keep_alive(&instance.id, 1800).await.unwrap();
    lap("renewed");
    client.pause(&instance.id).await.unwrap();
    lap("stopped");
    let listed = client
        .list(&ListInstances {
            tags: tags.clone(),
            states: vec![],
        })
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].state, InstanceState::Paused);
    assert!(client
        .keep_alive(&instance.id, 1800)
        .await
        .unwrap_err()
        .is_not_found());

    let connected = client.connect(&instance.id, 1800).await.unwrap();
    assert!(connected.resumed);
    lap("resumed");
    let after = client
        .run_command(
            &instance,
            &sh("cat \"$HOME/marker\"; pgrep -x intentd || echo no-intentd"),
        )
        .await
        .unwrap();
    let after = String::from_utf8_lossy(&after.stdout);
    assert!(after.contains("kept"), "the disk is kept: {after}");
    assert!(after.contains("no-intentd"), "memory is not kept: {after}");
    let up = client.run_command(&instance, &sh(START)).await.unwrap();
    assert!(up.success(), "{}", String::from_utf8_lossy(&up.stderr));
    lap("intentd up again from the disk");

    assert!(client.delete(&instance.id).await.unwrap());
    assert!(!client.delete(&instance.id).await.unwrap());
    lap("deleted");
}

#[tokio::test]
async fn a_live_round_trip_leaves_nothing_behind() {
    if std::env::var(LIVE_ENV).as_deref() != Ok("1") {
        eprintln!("skipped: set {LIVE_ENV}=1, a Vercel token, team and project");
        return;
    }
    let token = live_token().expect("set VERCEL_TOKEN or VERCEL_TOKEN_FILE");
    let (team, project) = live_scope();
    let image = std::env::var("VERCEL_LIVE_IMAGE").unwrap_or_else(|_| DEFAULT_IMAGE.to_owned());
    let live = Live::new(token.clone(), team.clone(), project.clone());
    let left = live.clean(LIVE_BACKEND).await;
    assert!(left.is_empty(), "an earlier run left {left:?}");
    let client = VercelClient::new(
        SecretString::new(token),
        VercelConfig {
            team_id: team,
            project_id: project,
            ..VercelConfig::default()
        },
    )
    .unwrap();
    let outcome = AssertUnwindSafe(round_trip(&client, &image))
        .catch_unwind()
        .await;
    let left = live.clean(LIVE_BACKEND).await;
    assert!(left.is_empty(), "the live test could not delete {left:?}");
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
