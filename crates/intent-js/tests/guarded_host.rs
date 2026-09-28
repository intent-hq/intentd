//! Real `QuickJS` tests of neutral transfer mechanics. Positive admissions below
//! are private fixtures, not repository authority, eligibility, or native reads.

use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_js::{
    eval, eval_guarded, BoxFuture, EvalOptions, GuardedHostFn, HostAdmissionOutcome, HostCallId,
    HostFn, HostReply, HostReplyAdmission, JsError, PreparedHostTransfer,
};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Barrier, Semaphore};

type Admit =
    Box<dyn FnOnce(PreparedHostTransfer) -> BoxFuture<'static, HostAdmissionOutcome> + Send>;
struct Admission(Admit);

impl HostReplyAdmission for Admission {
    fn admit(
        self: Box<Self>,
        transfer: PreparedHostTransfer,
    ) -> BoxFuture<'static, HostAdmissionOutcome> {
        (self.0)(transfer)
    }
}

fn admission(
    f: impl FnOnce(PreparedHostTransfer) -> BoxFuture<'static, HostAdmissionOutcome> + Send + 'static,
) -> Box<dyn HostReplyAdmission> {
    Box::new(Admission(Box::new(f)))
}

fn allow(id: HostCallId) -> Box<dyn HostReplyAdmission> {
    admission(move |packet| Box::pin(async move { packet.transfer(&id) }))
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn run(code: &str, host: GuardedHostFn) -> Result<Value, JsError> {
    tokio::time::timeout(
        Duration::from_secs(10),
        eval_guarded(code, &EvalOptions::default(), Some(host)),
    )
    .await
    .expect("test watchdog: engine must settle")
}

async fn next<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), rx.recv())
        .await
        .expect("test watchdog: expected transition")
        .expect("fixture sender exists")
}

const CATCH: &str = "try { return await host(null); } catch (e) { return e.message; }";
const REFUSED: &str = "host reply admission refused";

#[tokio::test]
async fn captures_original_context_synchronously_before_host_future_poll() {
    let stage = Arc::new(AtomicUsize::new(0));
    let captured = stage.clone();
    let host: GuardedHostFn = Arc::new(move |arg, id| {
        assert_eq!(captured.fetch_add(1, Ordering::SeqCst), 0);
        let stage = captured.clone();
        Box::pin(async move {
            assert_eq!(stage.fetch_add(1, Ordering::SeqCst), 1);
            tokio::task::yield_now().await;
            HostReply::Guarded {
                outcome: Ok(arg),
                admission: allow(id),
            }
        })
    });
    assert_eq!(
        run("return await host({n: 7});", host).await.unwrap(),
        json!({"n":7})
    );
    assert_eq!(stage.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn transferred_bytes_are_unobserved_until_admission_returns() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let gate = Arc::new(Semaphore::new(0));
    let gate_for_host = gate.clone();
    let observed = Arc::new(AtomicUsize::new(0));
    let observations = observed.clone();
    let host: GuardedHostFn = Arc::new(move |arg, id| {
        if arg == "observed" {
            observations.fetch_add(1, Ordering::SeqCst);
            return Box::pin(async { HostReply::Ordinary(Ok(Value::Null)) });
        }
        let tx = tx.clone();
        let gate = gate_for_host.clone();
        Box::pin(async move {
            HostReply::Guarded {
                outcome: Ok(json!("private")),
                admission: admission(move |packet| {
                    Box::pin(async move {
                        let receipt = packet.transfer(&id);
                        assert!(matches!(receipt, HostAdmissionOutcome::Transferred(_)));
                        tx.send(()).unwrap();
                        let _guard = gate.acquire().await.unwrap();
                        receipt
                    })
                }),
            }
        })
    });
    let task = tokio::spawn(run(
        "const x = await host(null); await host('observed'); return x;",
        host,
    ));
    next(&mut rx).await;
    assert_eq!(observed.load(Ordering::SeqCst), 0);
    assert!(!task.is_finished());
    gate.add_permits(1);
    assert_eq!(task.await.unwrap().unwrap(), "private");
    assert_eq!(observed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retirement_before_transfer_refuses_without_retry_or_private_diagnostics() {
    for outcome in [
        Ok(json!({"ok": false, "isError": true, "error": "SECRET"})),
        Err("SECRET".into()),
    ] {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let alive = Arc::new(AtomicBool::new(true));
        let alive_for_host = alive.clone();
        let gate = Arc::new(Semaphore::new(0));
        let gate_for_host = gate.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let call_count = calls.clone();
        let host: GuardedHostFn = Arc::new(move |_, id| {
            call_count.fetch_add(1, Ordering::SeqCst);
            let outcome = outcome.clone();
            let (alive, gate, tx) = (alive_for_host.clone(), gate_for_host.clone(), tx.clone());
            Box::pin(async move {
                HostReply::Guarded {
                    outcome,
                    admission: admission(move |packet| {
                        Box::pin(async move {
                            tx.send(()).unwrap();
                            let _permit = gate.acquire().await.unwrap();
                            if alive.load(Ordering::SeqCst) {
                                packet.transfer(&id)
                            } else {
                                drop(packet);
                                HostAdmissionOutcome::Refused
                            }
                        })
                    }),
                }
            })
        });
        let task = tokio::spawn(run(CATCH, host));
        next(&mut rx).await;
        alive.store(false, Ordering::SeqCst);
        gate.add_permits(1);
        assert_eq!(task.await.unwrap().unwrap(), REFUSED);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn retirement_after_transfer_does_not_recall_the_admitted_effect() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let alive = Arc::new(AtomicBool::new(true));
    let gate = Arc::new(Semaphore::new(0));
    let alive_for_host = alive.clone();
    let gate_for_host = gate.clone();
    let transfers = Arc::new(AtomicUsize::new(0));
    let transferred = transfers.clone();
    let host: GuardedHostFn = Arc::new(move |_, id| {
        let (alive, gate, tx, transferred) = (
            alive_for_host.clone(),
            gate_for_host.clone(),
            tx.clone(),
            transferred.clone(),
        );
        Box::pin(async move {
            HostReply::Guarded {
                outcome: Ok(json!(42)),
                admission: admission(move |packet| {
                    Box::pin(async move {
                        assert!(alive.load(Ordering::SeqCst));
                        let receipt = packet.transfer(&id);
                        assert!(matches!(receipt, HostAdmissionOutcome::Transferred(_)));
                        transferred.fetch_add(1, Ordering::SeqCst);
                        tx.send(()).unwrap();
                        let _permit = gate.acquire().await.unwrap();
                        receipt
                    })
                }),
            }
        })
    });
    let task = tokio::spawn(run("return await host(null);", host));
    next(&mut rx).await;
    alive.store(false, Ordering::SeqCst);
    gate.add_permits(1);
    assert_eq!(task.await.unwrap().unwrap(), 42);
    assert_eq!(transfers.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parallel_equal_arguments_keep_original_slots_when_completion_order_reverses() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let gates = [Arc::new(Semaphore::new(0)), Arc::new(Semaphore::new(0))];
    let host_gates = gates.clone();
    let call_count = calls.clone();
    let host: GuardedHostFn = Arc::new(move |arg, id| {
        assert_eq!(arg, json!({"same": true}));
        let index = call_count.fetch_add(1, Ordering::SeqCst);
        let gate = host_gates[index].clone();
        let tx = tx.clone();
        Box::pin(async move {
            tx.send((false, index)).unwrap();
            let _permit = gate.acquire().await.unwrap();
            HostReply::Guarded {
                outcome: Ok(json!(index)),
                admission: admission(move |packet| {
                    Box::pin(async move {
                        let receipt = packet.transfer(&id);
                        assert!(matches!(receipt, HostAdmissionOutcome::Transferred(_)));
                        tx.send((true, index)).unwrap();
                        receipt
                    })
                }),
            }
        })
    });
    let task = tokio::spawn(run(
        "return await Promise.all([host({same:true}), host({same:true})]);",
        host,
    ));
    assert!(!next(&mut rx).await.0);
    assert!(!next(&mut rx).await.0);
    gates[1].add_permits(1);
    assert_eq!(next(&mut rx).await, (true, 1));
    gates[0].add_permits(1);
    assert_eq!(next(&mut rx).await, (true, 0));
    assert_eq!(task.await.unwrap().unwrap(), json!([0, 1]));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn foreign_call_identity_cannot_redirect_a_packet() {
    let ids = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(2));
    let host: GuardedHostFn = Arc::new(move |_, id| {
        let index = {
            let mut ids = ids.lock().unwrap();
            let index = ids.len();
            ids.push(Some(id));
            index
        };
        let (ids, barrier) = (ids.clone(), barrier.clone());
        Box::pin(async move {
            HostReply::Guarded {
                outcome: Ok(json!("SECRET")),
                admission: admission(move |packet| {
                    Box::pin(async move {
                        barrier.wait().await;
                        let other = ids.lock().unwrap()[1 - index].take().unwrap();
                        let result = packet.transfer(&other);
                        assert!(matches!(result, HostAdmissionOutcome::ForeignCall));
                        result
                    })
                }),
            }
        })
    });
    let result = run("return await Promise.all([host(null).catch(e=>e.message),host(null).catch(e=>e.message)]);", host).await.unwrap();
    assert_eq!(result, json!([REFUSED, REFUSED]));
}

#[tokio::test]
async fn receipt_from_another_invocation_is_not_an_admission() {
    let (tx0, rx0) = oneshot::channel();
    let (tx1, rx1) = oneshot::channel();
    let exchanges = Arc::new(Mutex::new(vec![Some((tx0, rx1)), Some((tx1, rx0))]));
    let calls = Arc::new(AtomicUsize::new(0));
    let host: GuardedHostFn = Arc::new(move |_, id| {
        let index = calls.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = exchanges.lock().unwrap()[index].take().unwrap();
        Box::pin(async move {
            HostReply::Guarded {
                outcome: Ok(json!("SECRET")),
                admission: admission(move |packet| {
                    Box::pin(async move {
                        let own = packet.transfer(&id);
                        assert!(matches!(own, HostAdmissionOutcome::Transferred(_)));
                        assert!(tx.send(own).is_ok());
                        rx.await.unwrap()
                    })
                }),
            }
        })
    });
    let (a, b) = tokio::join!(run(CATCH, host.clone()), run(CATCH, host));
    assert_eq!(a.unwrap(), REFUSED);
    assert_eq!(b.unwrap(), REFUSED);
}

#[tokio::test]
async fn ordinary_wrapper_and_typed_ordinary_reply_preserve_success_and_error_shapes() {
    for outcome in [
        Ok(json!({"ok":false,"error":"ordinary","nested":[1,null]})),
        Err("ordinary failure".into()),
    ] {
        let original = outcome.clone();
        let host: HostFn = Arc::new(move |_| {
            let outcome = original.clone();
            Box::pin(async move { outcome })
        });
        let ordinary: GuardedHostFn = Arc::new(move |_, _| {
            let outcome = outcome.clone();
            Box::pin(async move { HostReply::Ordinary(outcome) })
        });
        let old = eval(CATCH, &EvalOptions::default(), Some(host))
            .await
            .unwrap();
        assert_eq!(run(CATCH, ordinary).await.unwrap(), old);
    }
}

#[tokio::test]
async fn admitted_error_preserves_original_thrown_message() {
    let host: GuardedHostFn = Arc::new(|_, id| {
        Box::pin(async move {
            HostReply::Guarded {
                outcome: Err("actual provider error".into()),
                admission: allow(id),
            }
        })
    });
    assert_eq!(run(CATCH, host).await.unwrap(), "actual provider error");
}

#[tokio::test]
async fn dropping_unpolled_eval_never_invokes_host() {
    let calls = Arc::new(AtomicUsize::new(0));
    let call_count = calls.clone();
    let host: GuardedHostFn = Arc::new(move |_, id| {
        call_count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            HostReply::Guarded {
                outcome: Ok(json!(1)),
                admission: allow(id),
            }
        })
    });
    let opts = EvalOptions::default();
    let future = eval_guarded("return await host(null);", &opts, Some(host));
    drop(future);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dropping_unpolled_admission_packet_refuses_without_fallback() {
    let drops = Arc::new(AtomicUsize::new(0));
    let admission_drops = drops.clone();
    let host: GuardedHostFn = Arc::new(move |_, id| {
        let guard = Dropped(admission_drops.clone());
        Box::pin(async move {
            HostReply::Guarded {
                outcome: Ok(json!("SECRET")),
                admission: admission(move |packet| {
                    let unpolled = async move {
                        let _guard = guard;
                        packet.transfer(&id)
                    };
                    drop(unpolled);
                    Box::pin(async { HostAdmissionOutcome::Refused })
                }),
            }
        })
    });
    assert_eq!(run(CATCH, host).await.unwrap(), REFUSED);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_closes_original_consumer_and_drops_pending_admission() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let drops = Arc::new(AtomicUsize::new(0));
    let admission_drops = drops.clone();
    let host: GuardedHostFn = Arc::new(move |_, id| {
        let (tx, guard) = (tx.clone(), Dropped(admission_drops.clone()));
        Box::pin(async move {
            HostReply::Guarded {
                outcome: Ok(json!("SECRET")),
                admission: admission(move |packet| {
                    Box::pin(async move {
                        let _guard = guard;
                        // Deliberately retain the opaque packet outside the cancelled
                        // fixture to try a late transfer; production must not detach it.
                        assert!(tx.send((packet, id)).is_ok());
                        pending().await
                    })
                }),
            }
        })
    });
    let task = tokio::spawn(run("return await host(null);", host));
    let (packet, id) = next(&mut rx).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        packet.transfer(&id),
        HostAdmissionOutcome::ConsumerClosed
    ));
}

#[tokio::test]
async fn timeout_drops_pending_host_and_admission_without_retry() {
    for in_admission in [false, true] {
        let drops = Arc::new(AtomicUsize::new(0));
        let future_drops = drops.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let call_count = calls.clone();
        let host: GuardedHostFn = Arc::new(move |_, id| {
            call_count.fetch_add(1, Ordering::SeqCst);
            let guard = Dropped(future_drops.clone());
            Box::pin(async move {
                if in_admission {
                    HostReply::Guarded {
                        outcome: Ok(json!("SECRET")),
                        admission: admission(move |packet| {
                            Box::pin(async move {
                                let _held = (guard, packet, id);
                                pending().await
                            })
                        }),
                    }
                } else {
                    let _held = (guard, id);
                    pending().await
                }
            })
        });
        let opts = EvalOptions {
            timeout: Duration::from_millis(10),
            ..EvalOptions::default()
        };
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            eval_guarded("return await host(null);", &opts, Some(host)),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(JsError::Timeout { .. })));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn cancellation_after_admission_does_not_retry_or_undo_transfer() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let transferred = Arc::new(AtomicUsize::new(0));
    let transfers = transferred.clone();
    let host: GuardedHostFn = Arc::new(move |arg, id| {
        if arg == "observed" {
            tx.send(()).unwrap();
            return Box::pin(pending());
        }
        let transfers = transfers.clone();
        Box::pin(async move {
            HostReply::Guarded {
                outcome: Ok(json!("private")),
                admission: admission(move |packet| {
                    Box::pin(async move {
                        let receipt = packet.transfer(&id);
                        assert!(matches!(receipt, HostAdmissionOutcome::Transferred(_)));
                        transfers.fetch_add(1, Ordering::SeqCst);
                        receipt
                    })
                }),
            }
        })
    });
    let task = tokio::spawn(run(
        "await host(null); return await host('observed');",
        host,
    ));
    next(&mut rx).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(transferred.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn admission_panic_drops_original_packet_and_pending_guard() {
    let drops = Arc::new(AtomicUsize::new(0));
    let admission_drops = drops.clone();
    let host: GuardedHostFn = Arc::new(move |_, id| {
        let guard = Dropped(admission_drops.clone());
        Box::pin(async move {
            HostReply::Guarded {
                outcome: Ok(json!("SECRET")),
                admission: admission(move |packet| {
                    Box::pin(async move {
                        let _held = (guard, packet, id);
                        panic!("fixture admission panic");
                    })
                }),
            }
        })
    });
    let result = tokio::spawn(run("return await host(null);", host)).await;
    assert!(result.unwrap_err().is_panic());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn abort_after_transfer_before_admission_returns_preserves_only_that_effect() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let transfers = Arc::new(AtomicUsize::new(0));
    let transferred = transfers.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let admission_drops = drops.clone();
    let host: GuardedHostFn = Arc::new(move |_, id| {
        let (tx, transferred, guard) = (
            tx.clone(),
            transferred.clone(),
            Dropped(admission_drops.clone()),
        );
        Box::pin(async move {
            HostReply::Guarded {
                outcome: Ok(json!("SECRET")),
                admission: admission(move |packet| {
                    Box::pin(async move {
                        let held = (guard, packet.transfer(&id));
                        assert!(matches!(held.1, HostAdmissionOutcome::Transferred(_)));
                        transferred.fetch_add(1, Ordering::SeqCst);
                        tx.send(()).unwrap();
                        pending().await
                    })
                }),
            }
        })
    });
    let task = tokio::spawn(run("return await host(null);", host));
    next(&mut rx).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(transfers.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn host_future_panic_drops_captured_context_without_admission_or_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let call_count = calls.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let context_drops = drops.clone();
    let host: GuardedHostFn = Arc::new(move |_, id| {
        call_count.fetch_add(1, Ordering::SeqCst);
        let guard = Dropped(context_drops.clone());
        Box::pin(async move {
            let _captured = (guard, id);
            panic!("fixture host panic");
        })
    });
    assert!(tokio::spawn(run(CATCH, host)).await.unwrap_err().is_panic());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
