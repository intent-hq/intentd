use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

tokio::task_local! { static REQUEST: usize; }

struct Context(AtomicUsize);
struct Scope(usize);

impl McpRequestContext for Context {
    fn capture(&self) -> Arc<dyn McpRequestScope> {
        Arc::new(Scope(self.0.fetch_add(1, Ordering::SeqCst)))
    }
}

impl McpRequestScope for Scope {
    fn scope<'a>(&'a self, request: McpContextFuture<'a>) -> McpContextFuture<'a> {
        Box::pin(REQUEST.scope(self.0, request))
    }
}

#[tokio::test]
async fn original_caller_and_scope_survive_task_hop_and_repeated_entry() {
    let source = Context(AtomicUsize::new(7));
    let caller = Caller::Agent {
        agent_id: "original-agent".into(),
    };
    let captured = CapturedRequestContext::capture(Some(caller.clone()), Some(&source));
    assert_eq!(source.0.load(Ordering::SeqCst), 8);
    let result = tokio::spawn(async move {
        intent_core::with_caller(Caller::Daemon, async {
            for _ in 0..2 {
                captured
                    .run(async {
                        assert_eq!(REQUEST.get(), 7);
                        assert_eq!(intent_core::current_caller(), Some(caller.clone()));
                        tokio::task::yield_now().await;
                        assert_eq!(REQUEST.get(), 7);
                    })
                    .await;
                assert!(REQUEST.try_with(|_| ()).is_err());
                assert_eq!(intent_core::current_caller(), Some(Caller::Daemon));
            }
            captured.run(async { vec!["original", "result"] }).await
        })
        .await
    })
    .await
    .unwrap();
    assert_eq!(result, ["original", "result"]);
    assert_eq!(source.0.load(Ordering::SeqCst), 8);
}

#[tokio::test]
async fn missing_context_preserves_result_without_creating_a_scope() {
    let captured = CapturedRequestContext::capture(None, None);
    assert_eq!(
        captured
            .run(async {
                assert!(REQUEST.try_with(|_| ()).is_err());
                Err::<(), _>("original error")
            })
            .await,
        Err("original error")
    );
}

struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancellation_drops_the_scoped_body_without_detaching_it() {
    let source = Context(AtomicUsize::new(1));
    let captured = CapturedRequestContext::capture(None, Some(&source));
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = Dropped(dropped.clone());
    let (entered, started) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        captured
            .run(async move {
                let _guard = guard;
                entered.send(()).unwrap();
                std::future::pending::<()>().await;
            })
            .await;
    });
    started.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(dropped.load(Ordering::SeqCst));
}
