//! The startup check in [`intent_app::compose`]: every feature port core
//! declares is registered, and a missing registration fails at boot.

use intent_services::Services;
use intent_store::Store;

async fn bare_services(dir: &tempfile::TempDir) -> Services {
    Services::new(
        Store::open(&dir.path().join("svc.db"))
            .await
            .expect("open store"),
    )
}

#[tokio::test]
async fn compose_registers_every_feature_port() {
    let dir = tempfile::tempdir().expect("tempdir");
    let services = intent_app::compose(bare_services(&dir).await);
    assert!(
        services.unregistered_feature_ports().is_empty(),
        "unregistered: {:?}",
        services.unregistered_feature_ports()
    );
    services
        .ensure_feature_ports_registered()
        .expect("startup check passes after compose");
    intent_app::assert_feature_ports_registered(&services);
}

#[tokio::test]
#[should_panic(expected = "intent_app::compose: feature ports not registered: transfer, runtime")]
async fn an_unregistered_production_port_fails_the_startup_check() {
    let dir = tempfile::tempdir().expect("tempdir");
    let services = bare_services(&dir).await;
    intent_app::assert_feature_ports_registered(&services);
}
