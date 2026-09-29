//! Optional admission at the actual HTTP-attempt boundary, including retries
//! and redirects. Ordinary on-demand traffic has no admission callback.
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

pub type Admission = Arc<dyn Fn() -> bool + Send + Sync>;
tokio::task_local! { static ADMISSION: Admission; static TERMINAL_DENIAL: Arc<AtomicBool>; }

pub(crate) fn current() -> Option<Admission> {
    ADMISSION.try_with(Clone::clone).ok()
}

pub fn with_admission<T>(
    admission: Admission,
    future: impl Future<Output = T>,
) -> impl Future<Output = T> {
    ADMISSION.scope(admission, Box::pin(future))
}

/// Observe a terminal local refusal independently of the returned API error.
/// Refusing a retry while returning an already received forge response is not
/// a terminal denial. The transport captures this signal across its buffer task.
pub async fn with_admission_tracking<T>(
    admission: Admission,
    future: impl Future<Output = T>,
) -> (T, bool) {
    let denied = Arc::new(AtomicBool::new(false));
    let result = TERMINAL_DENIAL
        .scope(denied.clone(), with_admission(admission, future))
        .await;
    (result, denied.load(Ordering::SeqCst))
}

pub(crate) fn terminal_denial() -> Option<Arc<AtomicBool>> {
    TERMINAL_DENIAL.try_with(Clone::clone).ok()
}
