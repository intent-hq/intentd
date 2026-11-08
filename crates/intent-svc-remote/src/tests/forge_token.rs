//! The backend's GitHub token on its machines (nodeProtocol 15): sent right
//! after every bind, before any launch on it; sent again on each change and
//! as `null` once there is none; never sent twice for the same value.

use std::sync::atomic::{AtomicU64, Ordering};

use intent_core::node_link::FORGE_HOST_GITHUB;
use zeroize::Zeroizing;

use super::*;
use crate::forge_token::ForgeTokens;

/// Tokens resolved from `value`, as the daemon's credential would be.
fn tokens(value: &Arc<Mutex<Option<String>>>) -> Arc<ForgeTokens> {
    let value = value.clone();
    Arc::new(ForgeTokens::new(Arc::new(move || {
        let token = value.lock().unwrap().clone().map(Zeroizing::new);
        Box::pin(async move { token })
    })))
}

fn set(value: &Arc<Mutex<Option<String>>>, token: Option<&str>) {
    *value.lock().unwrap() = token.map(str::to_owned);
}

/// The token of the next `forgeToken` frame; a launch before it fails.
async fn next_token(link: &mut NodeEnd) -> Option<String> {
    link.expect(|frame| match frame {
        LinkFrame::ForgeToken(forge) => {
            assert_eq!(forge.host, FORGE_HOST_GITHUB);
            Some(forge.token.map(|token| token.expose().to_owned()))
        }
        LinkFrame::Launch(_) => panic!("a launch went out before the token"),
        _ => None,
    })
    .await
}

#[tokio::test]
async fn a_refresh_reports_only_a_changed_value() {
    let value = Arc::new(Mutex::new(None));
    let tokens = tokens(&value);
    assert!(!tokens.refresh().await, "nothing to nothing");
    set(&value, Some("t-1"));
    assert!(tokens.refresh().await);
    assert!(!tokens.refresh().await, "the same value again");
    set(&value, Some("t-2"));
    assert!(tokens.refresh().await);
    set(&value, None);
    assert!(tokens.refresh().await, "removal");
    assert!(!tokens.refresh().await);
    assert!(!format!("{:?}", *tokens.subscribe().borrow()).contains("t-"));
}

#[tokio::test]
async fn the_token_goes_out_after_every_bind_before_a_launch_and_on_each_change() {
    let backend = Backend::new().await;
    let mut machine = backend.machine().await;
    backend.expose(&mut machine).await.shutdown();
    let value = Arc::new(Mutex::new(Some("t-1".to_owned())));
    let forge = tokens(&value);
    let node = RemoteNode::start(
        backend.store().clone(),
        backend.target(&machine).await,
        tuning(),
        None,
        Some(forge.clone()),
    )
    .await
    .unwrap();
    let agent = AgentId::new();
    let launching = {
        let node = node.clone();
        let command = stop_acks::launch_of(&agent);
        tokio::spawn(async move { node.launch(command).await.map(|_| ()) })
    };
    let mut link = machine.accept().await;
    assert_eq!(next_token(&mut link).await.as_deref(), Some("t-1"));
    link.expect(|frame| matches!(frame, LinkFrame::Launch(_)).then_some(()))
        .await;
    launching.await.unwrap().unwrap();

    // A change goes out at once; the same value never twice; a removal as
    // `null`.
    set(&value, Some("t-2"));
    assert!(forge.refresh().await);
    assert!(!forge.refresh().await);
    assert_eq!(next_token(&mut link).await.as_deref(), Some("t-2"));
    set(&value, None);
    assert!(forge.refresh().await);
    assert_eq!(next_token(&mut link).await, None);

    // A new bind resolves afresh and sends the value first.
    set(&value, Some("t-3"));
    machine.stop().await;
    machine.restart().await;
    node.connect().await.unwrap();
    let mut again = machine.accept().await;
    assert!(again.hello.binding.generation > link.hello.binding.generation);
    assert_eq!(next_token(&mut again).await.as_deref(), Some("t-3"));
    node.shutdown();
}

/// The watcher resolves again on a change of its state (a GitHub connect,
/// revoke or gate change) and on its period, only while a machine is
/// linked.
#[tokio::test]
async fn the_watcher_follows_credential_changes_while_a_machine_is_linked() {
    let value = Arc::new(Mutex::new(Some("t-1".to_owned())));
    let forge = tokens(&value);
    let generation = Arc::new(AtomicU64::new(0));
    let state = {
        let generation = generation.clone();
        move || generation.load(Ordering::SeqCst)
    };
    forge.watch_with(state, Duration::from_secs(3600), Duration::from_millis(10));
    let mut linked = forge.subscribe();
    // Bump the state until the watcher resolves again (its first look may
    // already have seen a bump).
    let bump_until_changed = |rx: &tokio::sync::watch::Receiver<crate::forge_token::Held>| {
        let mut rx = rx.clone();
        let generation = generation.clone();
        async move {
            tokio::time::timeout(wait(), async {
                loop {
                    generation.fetch_add(1, Ordering::SeqCst);
                    if tokio::time::timeout(Duration::from_millis(200), rx.changed())
                        .await
                        .is_ok()
                    {
                        return;
                    }
                }
            })
            .await
            .expect("the watcher resolved again");
        }
    };
    bump_until_changed(&linked).await;
    linked.borrow_and_update();

    // An unchanged state leaves an external change for the period.
    set(&value, Some("t-2"));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!linked.has_changed().unwrap());
    bump_until_changed(&linked).await;
}
