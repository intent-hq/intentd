//! Sub-agents in their parent's checkout (nodeProtocol 13): a launch with
//! `checkoutOf` works in the owner's checkout, pushes go to the owner's
//! refs, and the owner's checkout outlives every agent that shares it.

use intent_core::node_link::{FinalPush, StopOutcome};

use super::*;

fn sharing(id: &str, owner: &str) -> LaunchCommand {
    LaunchCommand {
        is_sub_agent: true,
        checkout_of: Some(AgentId::from(owner)),
        ..launch(id)
    }
}

/// Launch `owner`, then `child` in its checkout; both started.
async fn owner_and_child(h: &Harness, owner: &str, child: &str) {
    h.runner.launch(launch(owner)).await;
    wait_for(&h.runner, "owner started", started(owner)).await;
    h.runner.launch(sharing(child, owner)).await;
    wait_for(&h.runner, "child started", started(child)).await;
}

/// The child sees the owner's uncommitted file, writes into the same
/// checkout, and its turn end pushes the owner's refs, never its own.
#[tokio::test]
async fn a_sharing_agent_works_in_the_owners_checkout_and_pushes_the_owners_refs() {
    let Some(h) = harness(2, &[]) else { return };
    owner_and_child(&h, "o1", "c1").await;
    let checkout = h
        .storage
        .existing_agent_dir(&AgentId::from("o1"))
        .expect("the owner's checkout");
    std::fs::write(checkout.join("parent.txt"), "from parent").unwrap();

    h.runner
        .prompt(
            &AgentId::from("c1"),
            "t1".into(),
            text(r#"wsapi const p = await ws.file.read("parent.txt"); await ws.file.write("child.txt", "from child"); const i = await ws.workspace.info(); return JSON.stringify({ p, path: i.path });"#),
        )
        .await;
    let all = wait_for(&h.runner, "child turn end", turn_ended("c1", "t1")).await;
    let line = wsapi_line(&all, "t1");
    assert!(line.contains("from parent"), "{line}");
    assert!(
        line.contains(&*checkout.to_string_lossy()),
        "the child's workspace is the owner's checkout: {line}"
    );
    assert_eq!(
        std::fs::read_to_string(checkout.join("child.txt")).unwrap(),
        "from child"
    );
    assert_eq!(wait_pushes(&h, 1).await, vec![AgentId::from("o1")]);
    assert!(h.storage.existing_agent_dir(&AgentId::from("c1")).is_none());
}

/// Without the owner's checkout on the machine the launch fails as a
/// checkout failure, naming the owner, and starts nothing.
#[tokio::test]
async fn a_sharing_launch_without_the_owners_checkout_fails() {
    let Some(h) = harness(1, &[]) else { return };
    h.runner.launch(sharing("c1", "gone")).await;
    let all = wait_for(&h.runner, "exit", exited("c1")).await;
    let error = exit_error(find(&all, exited("c1"))).unwrap();
    assert!(error.starts_with(codes::CHECKOUT), "{error}");
    assert!(
        error.contains("the checkout of agent gone is not on this machine"),
        "{error}"
    );
    assert!(!all.iter().any(started("c1")), "{all:#?}");
    assert!(h.runner.running().is_empty());
}

/// A sharing agent's final stop pushes the owner's refs and leaves the
/// owner's checkout; the owner's final stop stops the agents sharing it
/// first, then removes the checkout.
#[tokio::test]
async fn final_stops_keep_a_shared_checkout_until_its_owner_ends() {
    let Some(h) = harness(3, &[]) else { return };
    owner_and_child(&h, "o1", "c1").await;
    let owner = AgentId::from("o1");
    let checkout = h.storage.existing_agent_dir(&owner).unwrap();

    assert_eq!(
        h.runner.final_stop(&AgentId::from("c1")).await,
        Some((StopOutcome::Stopped, FinalPush::Pushed))
    );
    assert_eq!(h.checkouts.pushes.lock().unwrap().as_slice(), [owner.clone()]);
    assert!(checkout.is_dir(), "the owner's checkout stays");

    h.runner.launch(sharing("c2", "o1")).await;
    wait_for(&h.runner, "second child started", started("c2")).await;
    assert_eq!(
        h.runner.final_stop(&owner).await,
        Some((StopOutcome::Stopped, FinalPush::Pushed))
    );
    let all = records(&h.runner).await;
    let child_exit = all.iter().position(exited("c2")).expect("c2 stopped");
    let owner_exit = all.iter().position(exited("o1")).expect("o1 stopped");
    assert!(child_exit < owner_exit, "{all:#?}");
    assert!(!checkout.exists());
    assert!(h.runner.running().is_empty());

    h.runner.launch(sharing("c3", "o1")).await;
    let all = wait_for(&h.runner, "late child exit", exited("c3")).await;
    let error = exit_error(find(&all, exited("c3"))).unwrap();
    assert!(error.starts_with(codes::CHECKOUT), "{error}");
}

/// The bind sweep keeps a stopped owner's checkout while an agent sharing
/// it runs, or is on the lease.
#[tokio::test]
async fn the_sweep_keeps_a_checkout_its_sharers_use() {
    let Some(h) = harness(2, &[]) else { return };
    owner_and_child(&h, "o1", "c1").await;
    let owner = AgentId::from("o1");
    let child = AgentId::from("c1");
    h.runner.stop(&owner).wait().await.unwrap();

    assert!(h.runner.sweep(&[]).await.is_empty());
    h.runner.stop(&child).wait().await.unwrap();
    assert!(h.runner.sweep(&[child.clone()]).await.is_empty());
    assert!(h.storage.existing_agent_dir(&owner).is_some());
    assert_eq!(h.runner.sweep(&[]).await, vec![owner.clone()]);
    assert!(h.storage.existing_agent_dir(&owner).is_none());
}

/// `pushWip`: the owner's checkout is pushed now, also through a sharing
/// agent's id; an agent with no checkout here is skipped.
#[tokio::test]
async fn a_push_request_pushes_the_owners_checkout_now() {
    let Some(h) = harness(2, &[]) else { return };
    owner_and_child(&h, "o1", "c1").await;
    let owner = AgentId::from("o1");
    assert_eq!(h.runner.push_now(&owner).await, (FinalPush::Pushed, None));
    assert_eq!(
        h.runner.push_now(&AgentId::from("c1")).await,
        (FinalPush::Pushed, None)
    );
    assert_eq!(
        h.checkouts.pushes.lock().unwrap().as_slice(),
        [owner.clone(), owner.clone()]
    );
    assert_eq!(
        h.runner.push_now(&AgentId::from("nobody")).await,
        (FinalPush::Skipped, None)
    );
    h.checkouts
        .fail_pushes
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (outcome, error) = h.runner.push_now(&owner).await;
    assert_eq!(outcome, FinalPush::Failed);
    assert!(
        error.as_deref().unwrap_or_default().contains("head refused the push"),
        "{error:?}"
    );
}
