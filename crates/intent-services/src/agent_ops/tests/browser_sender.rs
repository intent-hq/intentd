use super::{create_agent, setup};
use intent_core::{
    browser_message::{with_browser_message_ingress, BrowserMessageIngress},
    with_caller, Caller, ClientHostInfo, ClientId, HostRole, WorkspaceApi,
};
use serde_json::json;

#[intent_test_macros::daemon_test]
async fn browser_sender_queue_acceptance_is_durable_ordered_and_retry_safe() {
    let (tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "sender").await;
    let other = create_agent(&svc, &ws, "other").await;
    let phone = ClientId::from("phone");
    let desktop = ClientId::from("desktop");
    for client in [&phone, &desktop] {
        svc.store
            .upsert_client(client, None, None, &ClientHostInfo::default())
            .await
            .unwrap();
    }
    let principal = svc.store.get_primary_principal().await.unwrap();
    with_caller(
        Caller::Wire {
            principal_id: principal.id,
            host_role: HostRole::Owner,
        },
        async {
            let old = BrowserMessageIngress::new(Some(desktop.clone()));
            let new = BrowserMessageIngress::new(Some(phone.clone()));
            assert!(old.received_order < new.received_order);
            // Accept newer request first; delayed older request cannot reverse it.
            for (receipt, id) in [(new, "new"), (old, "old")] {
                with_browser_message_ingress(
                    Some(receipt),
                    svc.agent_queue_submission(
                        agent.clone(),
                        Some(id.into()),
                        id.into(),
                        None,
                        None,
                        None,
                    ),
                )
                .await
                .unwrap();
            }
            assert_eq!(
                svc.store.latest_browser_sender(&agent).await.unwrap(),
                Some(phone.clone())
            );
            assert_eq!(svc.store.latest_browser_sender(&other).await.unwrap(), None);
            // A retransmitted id from another connection must not become a new send.
            with_browser_message_ingress(
                Some(BrowserMessageIngress::new(Some(desktop.clone()))),
                svc.agent_queue_submission(
                    agent.clone(),
                    Some("new".into()),
                    "new".into(),
                    None,
                    None,
                    None,
                ),
            )
            .await
            .unwrap();
            assert_eq!(
                svc.store.latest_browser_sender(&agent).await.unwrap(),
                Some(phone.clone())
            );
            // Daemon/agent calls cannot use even an accidentally propagated receipt.
            with_browser_message_ingress(
                Some(BrowserMessageIngress::new(Some(desktop.clone()))),
                with_caller(
                    Caller::Daemon,
                    svc.agent_queue_submission(
                        agent.clone(),
                        Some("automatic".into()),
                        "wake".into(),
                        None,
                        None,
                        Some(json!({"source":"system"})),
                    ),
                ),
            )
            .await
            .unwrap();
            assert_eq!(
                svc.store.latest_browser_sender(&agent).await.unwrap(),
                Some(phone.clone())
            );
            // Delivery of an older queued message is not a new user submission.
            with_caller(
                Caller::Daemon,
                svc.agent_send_queued_message_now_op(agent.clone(), "new".into()),
            )
            .await
            .unwrap();
            assert_eq!(
                svc.store.latest_browser_sender(&agent).await.unwrap(),
                Some(phone.clone())
            );
            // Direct transcript acceptance shares the same persistence seam.
            with_browser_message_ingress(
                Some(BrowserMessageIngress::new(Some(desktop.clone()))),
                svc.agent_send_message_op(
                    other.clone(),
                    "direct".into(),
                    Some("direct".into()),
                    None,
                    None,
                    None,
                ),
            )
            .await
            .unwrap();
            assert_eq!(
                svc.store.latest_browser_sender(&other).await.unwrap(),
                Some(desktop)
            );
            let reopened = intent_store::Store::open(&tmp.path).await.unwrap();
            assert_eq!(
                reopened.latest_browser_sender(&agent).await.unwrap(),
                Some(phone)
            );
            // A human connection without a hello is latest, but has no preferred host.
            with_browser_message_ingress(
                Some(BrowserMessageIngress::new(None)),
                svc.agent_queue_submission(
                    agent.clone(),
                    Some("unknown".into()),
                    "unknown".into(),
                    None,
                    None,
                    None,
                ),
            )
            .await
            .unwrap();
            assert_eq!(svc.store.latest_browser_sender(&agent).await.unwrap(), None);
        },
    )
    .await;
}
