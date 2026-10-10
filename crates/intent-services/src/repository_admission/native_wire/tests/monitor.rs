use super::*;
use intent_core::review_monitor::{
    ReviewMonitorOutput, ReviewMonitorRecipientSupport, ReviewMonitoringCapabilities,
};

async fn capture_receipt(socket: &Socket) -> Option<Arc<super::super::monitor::Receipt>> {
    socket
        .request(async { Ok(NATIVE_REQUEST.with(|request| request.monitor_receipt.clone())) })
        .await
        .unwrap()
}

fn supported(receipt: Option<&Arc<super::super::monitor::Receipt>>) -> bool {
    receipt
        .and_then(|receipt| receipt.support())
        .and_then(|support| {
            ReviewMonitorOutput::Qualified(()).project(
                &ReviewMonitoringCapabilities {
                    review_monitoring: Some(1),
                },
                support,
            )
        })
        .is_some()
}

#[intent_test_macros::daemon_test]
async fn native_monitor_receipt_requires_ack_and_never_survives_rehello_or_socket_replacement() {
    let fixture = Fixture::new().await;
    let socket = fixture.socket().await;
    assert!(!supported(capture_receipt(&socket).await.as_ref()));
    let capable = ReviewMonitorRecipientSupport::from_capabilities(&json!({"reviewMonitoring":1}));
    let first = socket
        .entered(async { socket.owner.review_monitoring_hello(capable).unwrap() })
        .await;
    let before_ack = capture_receipt(&socket).await;
    assert!(
        before_ack.is_none(),
        "an unacknowledged hello is not support"
    );
    socket.entered(async { first.acknowledge() }).await;
    let original = capture_receipt(&socket).await.unwrap();
    assert!(
        before_ack.is_none(),
        "a queued request cannot acquire a later receipt"
    );
    assert!(supported(Some(&original)));

    let legacy = socket
        .entered(async {
            socket
                .owner
                .review_monitoring_hello(ReviewMonitorRecipientSupport::default())
                .unwrap()
        })
        .await;
    assert!(
        !supported(Some(&original)),
        "replacement retires immediately"
    );
    socket.entered(async { first.acknowledge() }).await;
    assert!(
        !supported(Some(&original)),
        "late acknowledgement cannot resurrect A"
    );
    socket.entered(async { legacy.acknowledge() }).await;
    assert!(!supported(capture_receipt(&socket).await.as_ref()));

    let fresh = socket
        .entered(async { socket.owner.review_monitoring_hello(capable).unwrap() })
        .await;
    socket.entered(async { fresh.acknowledge() }).await;
    let replacement = capture_receipt(&socket).await.unwrap();
    assert!(supported(Some(&replacement)));
    assert!(
        !supported(Some(&original)),
        "A to B to A never revives A's receipt"
    );
    let other = fixture.socket().await;
    assert!(!supported(capture_receipt(&other).await.as_ref()));
    socket.owner.retire();
    assert!(!supported(Some(&replacement)), "physical retirement wins");
    socket.entered(async { fresh.acknowledge() }).await;
    assert!(!supported(Some(&replacement)));
}
