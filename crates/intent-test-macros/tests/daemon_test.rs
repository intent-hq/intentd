//! Compile-and-run proof of the `#[daemon_test]` expansion. A proc-macro
//! crate cannot use its own macro from `src/`, so the contract lives here.

use intent_core::{current_caller, with_caller, Caller, PrincipalId};

fn wire(principal: &str) -> Caller {
    Caller::Wire {
        principal_id: PrincipalId(principal.into()),
        is_administrator: false,
    }
}

#[intent_test_macros::daemon_test]
async fn body_runs_with_the_daemon_caller_bound() {
    assert_eq!(current_caller(), Some(Caller::Daemon));
}

#[intent_test_macros::daemon_test]
async fn binding_survives_awaits_inside_the_body() {
    tokio::task::yield_now().await;
    assert_eq!(current_caller(), Some(Caller::Daemon));
}

#[intent_test_macros::daemon_test]
async fn a_non_daemon_caller_is_still_an_explicit_with_caller_away() {
    let seen = with_caller(wire("p-1"), async { current_caller() }).await;
    assert_eq!(seen, Some(wire("p-1")));
    assert_eq!(current_caller(), Some(Caller::Daemon));
}

#[intent_test_macros::daemon_test]
async fn a_declared_return_type_is_preserved() -> Result<(), String> {
    let caller = current_caller().ok_or("unbound")?;
    assert_eq!(caller, Caller::Daemon);
    Ok(())
}

#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 2)]
async fn attribute_arguments_are_forwarded_to_tokio_test() {
    assert_eq!(current_caller(), Some(Caller::Daemon));
}

#[intent_test_macros::daemon_test]
#[should_panic(expected = "kept")]
async fn other_attributes_on_the_function_are_kept() {
    assert_eq!(current_caller(), Some(Caller::Daemon));
    panic!("kept");
}

#[tokio::test]
async fn a_plain_tokio_test_stays_unbound() {
    assert_eq!(current_caller(), None);
}
