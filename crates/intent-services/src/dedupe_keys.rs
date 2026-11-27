//! Test seams for the dedupe and idempotency gates that use a unique key in
//! the store (`with_idempotency`, keyed attachment placement, the
//! questions-dismissed notice claim).
//!
//! Tests hold an operation at a named step ([`steps`]) with
//! [`park_dedupe_step_for_test`] (test builds and `test-hooks` only), to
//! race a same-key call from another process against it. The parks share
//! the lifecycle park registry (`crate::lifecycle_fence`).

/// The steps a test can hold a keyed operation at.
pub mod steps {
    /// A keyed attachment placement, right after it reserved its key in the
    /// store and before it looks the key's binding up (key: the client's
    /// idempotency key).
    pub const ATTACHMENT_RESERVED: &str = "attachment-place/reserved";
}

/// Holds the operation at `step` of `key` while a test parks it there; a
/// no-op otherwise.
pub(crate) async fn park(step: &'static str, key: &str) {
    crate::lifecycle_fence::park(step, key).await;
}

/// Test seam: the next run of `step` ([`steps`]) for `key` signals
/// `entered` and waits for `release`. One-shot; parks are per process, so a
/// test uses keys of its own.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
#[must_use]
pub fn park_dedupe_step_for_test(
    step: &'static str,
    key: &str,
) -> std::sync::Arc<crate::lifecycle_fence::LifecyclePark> {
    crate::lifecycle_fence::park_lifecycle_step_for_test(step, key)
}

/// Waits until callers in this process found the store key
/// (`workspace_id`, `key`) reserved by another process and waited at least
/// `n` times. The key is the store's: a keyed attachment placement's is
/// namespaced (see [`attachment_reservation_key_for_test`]).
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub async fn reservation_waits_for_test(workspace_id: &str, key: &str, n: u64) {
    let mut seen = waits(workspace_id, key).subscribe();
    let _ = seen.wait_for(|count| *count >= n).await;
}

/// A keyed placement of `bytes` as `file_name` in `workspace_id` with
/// `key`, the way an agent's image attachment is placed (no archive fence:
/// the `file.placeAttachment` RPC also holds the workspace's archive-fence
/// lease, which already orders same-workspace placements across processes).
///
/// # Errors
///
/// As the placement.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub async fn place_keyed_attachment_for_test(
    services: &crate::Services,
    workspace_id: intent_core::WorkspaceId,
    file_name: &str,
    bytes: &[u8],
    key: &str,
) -> intent_core::Result<serde_json::Value> {
    use crate::attachment_upload::{attachment_fingerprint, sha256_hex, KeyedPlacement};
    let digest = sha256_hex(bytes);
    let fingerprint = attachment_fingerprint(file_name, bytes.len() as u64, Some(&digest));
    services
        .place_attachment_registered(
            workspace_id,
            file_name,
            &crate::file_ops::AttachmentSource::Bytes(bytes),
            None,
            Some(KeyedPlacement {
                key: key.to_owned(),
                lookup_fingerprint: Some(fingerprint),
                sha256: Some(digest),
            }),
        )
        .await
}

/// The store reservation key of a keyed attachment placement with `key`.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
#[must_use]
pub fn attachment_reservation_key_for_test(key: &str) -> String {
    crate::attachment_upload::attachment_reservation_key(key)
}

#[cfg(any(test, feature = "test-hooks"))]
type Waits = std::sync::Mutex<
    std::collections::HashMap<(String, String), std::sync::Arc<tokio::sync::watch::Sender<u64>>>,
>;

#[cfg(any(test, feature = "test-hooks"))]
fn waits(workspace_id: &str, key: &str) -> std::sync::Arc<tokio::sync::watch::Sender<u64>> {
    static WAITS: std::sync::OnceLock<Waits> = std::sync::OnceLock::new();
    WAITS
        .get_or_init(Waits::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry((workspace_id.to_owned(), key.to_owned()))
        .or_insert_with(|| std::sync::Arc::new(tokio::sync::watch::Sender::new(0)))
        .clone()
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn count_wait(workspace_id: &str, key: &str) {
    waits(workspace_id, key).send_modify(|count| *count += 1);
}
