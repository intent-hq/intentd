//! Optional admission at the actual HTTP-attempt boundary, including retries
//! and redirects. Ordinary on-demand traffic has no admission callback.
use std::{future::Future, sync::Arc};

pub type Admission = Arc<dyn Fn() -> bool + Send + Sync>;
tokio::task_local! { static ADMISSION: Admission; }

pub(crate) fn current() -> Option<Admission> {
    ADMISSION.try_with(Clone::clone).ok()
}

pub fn with_admission<T>(
    admission: Admission,
    future: impl Future<Output = T>,
) -> impl Future<Output = T> {
    ADMISSION.scope(admission, Box::pin(future))
}
