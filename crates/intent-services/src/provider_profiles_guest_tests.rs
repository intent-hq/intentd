//! `providers.catalog` for workspace guests: the host's profile names and
//! renamed Default configurations are host settings, so a guest's read omits
//! them (§5.54 "Catalog, models and status"). Owners and host members keep
//! the full catalog.

use intent_core::{with_caller, Caller, HostRole, PrincipalId};

use super::*;

/// A non-primary principal; a host member when `member`, otherwise a guest.
async fn principal(fx: &Fixture, member: bool) -> PrincipalId {
    let mut person = fx.svc.store.get_primary_principal().await.unwrap();
    person.id = PrincipalId::new();
    person.is_primary = false;
    fx.svc.store.upsert_principal(&person).await.unwrap();
    if member {
        sqlx::query("INSERT INTO host_member (principal_id,added_at) VALUES (?,?)")
            .bind(person.id.as_str())
            .bind(intent_core::now_iso())
            .execute(fx.svc.store.write_pool())
            .await
            .unwrap();
    }
    person.id
}

async fn catalog_as(fx: &Fixture, principal_id: &PrincipalId, host_role: HostRole) -> Value {
    let caller = Caller::Wire {
        principal_id: principal_id.clone(),
        host_role,
    };
    with_caller(caller, fx.svc.providers_catalog())
        .await
        .unwrap()
}

async fn named_fixture() -> Fixture {
    let fx = Fixture::new().await;
    fx.create(
        "codex",
        "Work account",
        json!([{ "key": "OPENAI_API_KEY", "kind": "set", "value": SECRET }]),
    )
    .await;
    fx.svc
        .provider_profiles_update_default_impl(
            json!({ "baseProviderId": "claude-code", "name": "Team plan" }),
        )
        .await
        .unwrap();
    fx
}

fn assert_hides_host_names(catalog: &Value) {
    assert_eq!(catalog["profiles"], json!([]), "{catalog}");
    assert!(catalog.get("defaultNames").is_none(), "{catalog}");
    let text = catalog.to_string();
    for name in ["Work account", "Team plan"] {
        assert!(!text.contains(name), "{name} leaked: {text}");
    }
    assert!(
        catalog["providers"]
            .as_array()
            .is_some_and(|rows| !rows.is_empty()),
        "provider rows stay: {catalog}"
    );
}

fn assert_shows_host_names(catalog: &Value) {
    assert_eq!(catalog["profiles"][0]["name"], "Work account", "{catalog}");
    assert_eq!(
        catalog["defaultNames"],
        json!({ "claude-code": "Team plan" })
    );
}

#[intent_test_macros::daemon_test]
async fn a_guest_catalog_omits_the_host_profile_and_default_names() {
    let fx = named_fixture().await;
    let guest = principal(&fx, false).await;
    assert_hides_host_names(&catalog_as(&fx, &guest, HostRole::Guest).await);
}

#[intent_test_macros::daemon_test]
async fn owners_and_host_members_keep_the_full_catalog() {
    let fx = named_fixture().await;
    assert_shows_host_names(&fx.svc.providers_catalog().await.unwrap());
    let owner = fx.svc.store.get_primary_principal().await.unwrap().id;
    assert_shows_host_names(&catalog_as(&fx, &owner, HostRole::Owner).await);
    let member = principal(&fx, true).await;
    assert_shows_host_names(&catalog_as(&fx, &member, HostRole::Member).await);
}

/// The durable role decides, not the role cached when the connection was
/// admitted: a removed member is a guest from the next read, and a guest
/// promoted to member sees the names without reconnecting.
#[intent_test_macros::daemon_test]
async fn the_durable_host_role_decides_not_the_admitted_one() {
    let fx = named_fixture().await;
    let removed = principal(&fx, false).await;
    assert_hides_host_names(&catalog_as(&fx, &removed, HostRole::Member).await);
    let promoted = principal(&fx, true).await;
    assert_shows_host_names(&catalog_as(&fx, &promoted, HostRole::Guest).await);
}

/// Fails closed: a wire caller whose principal the store does not know is
/// treated as a guest, whatever role its connection was admitted with.
#[intent_test_macros::daemon_test]
async fn an_unknown_principal_reads_the_guest_catalog() {
    let fx = named_fixture().await;
    let unknown = PrincipalId::new();
    for role in [HostRole::Guest, HostRole::Member] {
        assert_hides_host_names(&catalog_as(&fx, &unknown, role).await);
    }
}

/// Agents act on host authority, so an agent's read keeps the names.
#[intent_test_macros::daemon_test]
async fn an_agent_reads_the_full_catalog() {
    let fx = named_fixture().await;
    let caller = Caller::Agent {
        agent_id: intent_core::AgentId::new(),
    };
    let catalog = with_caller(caller, fx.svc.providers_catalog())
        .await
        .unwrap();
    assert_shows_host_names(&catalog);
}
