//! Actual hello-handler sequencing; fixture receipts confer no read authority.
use std::sync::atomic::{AtomicBool, Ordering};

use intent_core::repository_request::ReviewMonitorHello;
use intent_core::review_monitor::{
    ReviewMonitorOutput, ReviewMonitorRecipientSupport, ReviewMonitoringCapabilities,
};

use super::*;

pub(super) struct Hello {
    support: ReviewMonitorRecipientSupport,
    acknowledged: AtomicBool,
}

impl Hello {
    pub(super) fn new(support: ReviewMonitorRecipientSupport) -> Self {
        Self {
            support,
            acknowledged: AtomicBool::new(false),
        }
    }

    fn acknowledged(&self) -> bool {
        self.acknowledged.load(Ordering::Acquire)
    }

    fn capable(&self) -> bool {
        ReviewMonitorOutput::Qualified(())
            .project(
                &ReviewMonitoringCapabilities {
                    review_monitoring: Some(1),
                },
                self.support,
            )
            .is_some()
    }
}

impl ReviewMonitorHello for Hello {
    fn retire(&self) {
        self.acknowledged.store(false, Ordering::Release);
    }

    fn acknowledge(&self) {
        self.acknowledged.store(true, Ordering::Release);
    }
}

fn hello(id: Option<Value>, client: Value, capability: Value) -> String {
    let mut value = json!({
        "jsonrpc":"2.0", "method":"client.hello",
        "params":{"capabilities":{}}
    });
    value["params"]["clientId"] = client;
    value["params"]["capabilities"]["reviewMonitoring"] = capability;
    if let Some(id) = id {
        value["id"] = id;
    }
    value.to_string()
}

fn last(h: &Harness) -> Arc<Hello> {
    h.original()
        .monitor_hellos
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn monitor_hello_only_acknowledges_successful_response_on_original_connection() {
    let mut h = Harness::new(FixtureApi::default(), HostRole::Owner).await;
    assert!(
        h.dispatch(&hello(Some(json!(1)), json!("same-client"), json!(1)))
            .await
    );
    let response = h.response().await;
    assert!(response.get("result").is_some(), "{response}");
    assert!(last(&h).acknowledged());
    assert!(
        !last(&h).capable(),
        "ordinary pooled sockets never gain monitor support"
    );
    assert!(
        response["result"]["server"]["capabilities"]
            .get("reviewMonitoring")
            .is_none(),
        "production advertisement stays disabled"
    );

    let mut other = Harness::new(FixtureApi::default(), HostRole::Owner).await;
    assert!(
        other
            .dispatch(&hello(Some(json!(2)), json!("same-client"), Value::Null))
            .await
    );
    assert!(other.response().await.get("result").is_some());
    assert!(last(&other).acknowledged());
    assert!(!last(&other).capable());
    assert_eq!(h.original().monitor_hellos.lock().unwrap().len(), 1);

    assert!(
        h.dispatch(&hello(Some(json!(3)), json!(42), json!(1)))
            .await
    );
    assert!(h.response().await.get("error").is_some());
    assert!(
        !last(&h).acknowledged(),
        "failed hello cannot acknowledge support"
    );
    assert!(
        h.dispatch(&hello(None, json!("same-client"), json!(1)))
            .await
    );
    assert!(
        !last(&h).acknowledged(),
        "notification has no successful response"
    );
    assert!(h.rx.priority.try_recv().is_err());

    for malformed in [
        Value::Null,
        json!(false),
        json!("1"),
        json!(-1),
        json!(0),
        json!(1.5),
    ] {
        assert!(
            h.dispatch(&hello(Some(json!(4)), json!("same-client"), malformed))
                .await
        );
        assert!(h.response().await.get("result").is_some());
        assert!(last(&h).acknowledged());
        assert!(!last(&h).capable());
    }
}

#[tokio::test]
async fn monitor_hello_acknowledgement_waits_for_response_queue_and_rejects_lost_writer() {
    let mut h = Harness::new(FixtureApi::default(), HostRole::Owner).await;
    for _ in 0..PRIORITY_CAPACITY {
        h.tx.priority.send("occupied".into()).await.unwrap();
    }
    let call = h.dispatch(&hello(Some(json!(1)), json!("queue-client"), json!(1)));
    tokio::pin!(call);
    tokio::select! {
        biased;
        result = &mut call => panic!("full queue did not wait: {result}"),
        () = tokio::task::yield_now() => {}
    }
    let receipt = last(&h);
    assert!(!receipt.acknowledged());
    assert_eq!(h.rx.priority.recv().await.as_deref(), Some("occupied"));
    assert!(call.await);
    assert!(receipt.acknowledged());
    for _ in 1..PRIORITY_CAPACITY {
        assert_eq!(h.rx.priority.recv().await.as_deref(), Some("occupied"));
    }
    assert!(h.response().await.get("result").is_some());

    h.rx.priority.close();
    assert!(
        !h.dispatch(&hello(Some(json!(2)), json!("queue-client"), json!(1)))
            .await
    );
    assert!(
        !last(&h).acknowledged(),
        "lost writer cannot acknowledge negotiation"
    );
}
