//! Opt-in round trip against the real Agent37 API, through the adapter:
//! for a `default` instance in `us` and a `vm` instance in `eu`, import the
//! template, create, start intentd with its tunnel, renew, sleep, wake (the
//! daemon still answers), and delete. It runs only with `AGENT37_LIVE=1`
//! and a key (`AGENT37_API_KEY` or `AGENT37_API_KEY_FILE`); no CI job sets
//! them. It deletes every instance and the template it made, whatever
//! happens, and checks nothing is left.
//!
//! ```text
//! AGENT37_LIVE=1 AGENT37_API_KEY_FILE=~/.config/agent37/api_key \
//!   cargo test -p intent-nodes --lib -- --exact --nocapture \
//!   agent37::live_tests::a_live_round_trip_leaves_nothing_behind
//! ```

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::time::Instant;

use futures_util::FutureExt as _;

use super::capture::{live_key, Live, DEFAULT_IMAGE};
use super::*;
use crate::cloud::{CloudProvider, CreateInstance, InstanceState, ListInstances, ProcessSpec};

const LIVE_ENV: &str = "AGENT37_LIVE";
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

const PID: &str = "pgrep -o -x intentd";

fn sh(script: &str) -> ProcessSpec {
    let mut spec = ProcessSpec::new("/bin/sh");
    spec.args = vec!["-c".into(), script.into()];
    spec
}

async fn round_trip(client: &Agent37Client, image: &str, instance_type: &str, region: &str) {
    let started = Instant::now();
    let lap = |what: &str| {
        eprintln!(
            "[{instance_type}/{region}] {what} at {:.1} s",
            started.elapsed().as_secs_f64()
        );
    };
    assert!(
        client.image_ready(image).await.unwrap(),
        "the template import failed"
    );
    lap("template ready");
    let tags = BTreeMap::from([
        (BACKEND_ID_TAG.to_owned(), LIVE_BACKEND.to_owned()),
        ("agentId".to_owned(), format!("live-{instance_type}")),
    ]);
    let instance = client
        .create(&CreateInstance {
            image: image.to_owned(),
            tags: tags.clone(),
            disk_gib: Some(if instance_type == "vm" { 20 } else { 12 }),
            machine_type: Some(instance_type.to_owned()),
            region: Some(region.to_owned()),
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
    let pid = client
        .run_command(&instance, &ProcessSpec::new("/bin/sh").tap_args(PID))
        .await
        .unwrap();
    let pid = String::from_utf8_lossy(&pid.stdout).trim().to_owned();
    assert!(!pid.is_empty());

    client.keep_alive(&instance.id, 1800).await.unwrap();
    client.pause(&instance.id).await.unwrap();
    lap("asleep");
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
    lap("awake");
    let again = client
        .run_command(&instance, &ProcessSpec::new("/bin/sh").tap_args(PID))
        .await
        .unwrap();
    let again = String::from_utf8_lossy(&again.stdout).trim().to_owned();
    eprintln!("[{instance_type}/{region}] intentd pid {pid} before the sleep, {again} after");
    let status = client
        .run_command(
            &instance,
            &sh("INTENTD_DATA_DIR=$HOME/.intent intentd call system.status"),
        )
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&status.stdout).contains("tcAddress"));

    assert!(client.delete(&instance.id).await.unwrap());
    assert!(!client.delete(&instance.id).await.unwrap());
    lap("deleted");
}

trait TapArgs {
    fn tap_args(self, script: &str) -> Self;
}

impl TapArgs for ProcessSpec {
    fn tap_args(mut self, script: &str) -> Self {
        self.args = vec!["-c".into(), script.into()];
        self
    }
}

#[tokio::test]
async fn a_live_round_trip_leaves_nothing_behind() {
    if std::env::var(LIVE_ENV).as_deref() != Ok("1") {
        eprintln!("skipped: set {LIVE_ENV}=1 and an Agent37 key");
        return;
    }
    let key = live_key().expect("set AGENT37_API_KEY or AGENT37_API_KEY_FILE");
    let image = std::env::var("AGENT37_LIVE_IMAGE").unwrap_or_else(|_| DEFAULT_IMAGE.to_owned());
    let template = template_name(&image);
    let live = Live::new(key.clone());
    let left = live.clean(LIVE_BACKEND, None).await;
    assert!(left.is_empty(), "an earlier run left {left:?}");
    let client = Agent37Client::new(SecretString::new(key), Agent37Config::default()).unwrap();
    let outcome = AssertUnwindSafe(async {
        round_trip(&client, &image, "default", "us").await;
        round_trip(&client, &image, "vm", "eu").await;
    })
    .catch_unwind()
    .await;
    let left = live.clean(LIVE_BACKEND, Some(&template)).await;
    assert!(left.is_empty(), "the live test could not delete {left:?}");
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
