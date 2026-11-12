//! `remoteMachines.order` / `setOrder` (monorepo
//! `docs/protocol/methods/nodes.md`, "Ordered machine list"): the user's
//! order of the places this backend runs agents, derived from the saved
//! keys and the live machines and entries (see
//! [`intent_core::machine_order`]).

use serde_json::{json, Value};

use intent_core::machine_order::{
    names_exactly, ordered, parse_saved, MachineOrderKey, MachineOrderRow, MachineOrderRows,
    MACHINE_ORDER_STATE_KEY, ORDER_STALE_CODE,
};
use intent_core::{Error, Result};
use intent_services::feature_api::Services;

/// Every change of the list takes this lock: `setOrder` reads, checks,
/// saves and announces as one step, and an add or remove announces the list
/// read after its own change, so the last event a client receives is never
/// an older list.
static ORDER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The ordered list now.
///
/// # Errors
///
/// When the store cannot be read.
pub async fn current(svc: &Services) -> Result<Vec<MachineOrderRow>> {
    let saved = parse_saved(
        svc.store()
            .get_setting(MACHINE_ORDER_STATE_KEY)
            .await?
            .as_deref(),
    );
    Ok(ordered(&saved, &rows(svc).await?))
}

async fn rows(svc: &Services) -> Result<MachineOrderRows> {
    let local = if svc.may_run_agents_locally() {
        Some(crate::methods::backend_name(svc).await)
    } else {
        None
    };
    let machines = svc
        .store()
        .list_exposed_machines()
        .await?
        .into_iter()
        .map(|row| (row.id, row.name))
        .collect();
    let entries = crate::cloud::entry_names(svc).await?;
    Ok(MachineOrderRows {
        local,
        machines,
        entries,
    })
}

/// `remoteMachines.order`.
pub(crate) async fn order(svc: &Services) -> Result<Value> {
    Ok(json!({ "order": current(svc).await? }))
}

/// `remoteMachines.setOrder`: the whole list, naming exactly the current
/// rows, else `order-stale` with the current list.
pub(crate) async fn set_order(svc: &Services, params: &Value) -> Result<Value> {
    let raw = params
        .get("order")
        .filter(|order| order.is_array())
        .ok_or_else(|| Error::InvalidParams("order is required and must be an array".into()))?;
    let order: Vec<MachineOrderKey> = serde_json::from_value(raw.clone())
        .map_err(|error| Error::InvalidParams(format!("order: {error}")))?;
    let _turn = ORDER_LOCK.lock().await;
    let rows = rows(svc).await?;
    let now = ordered(
        &parse_saved(
            svc.store()
                .get_setting(MACHINE_ORDER_STATE_KEY)
                .await?
                .as_deref(),
        ),
        &rows,
    );
    if !names_exactly(&order, &now) {
        return Err(Error::remote_machine(
            false,
            "The machine list changed; reload it and reorder again.",
            json!({ "code": ORDER_STALE_CODE, "order": now }),
        ));
    }
    save(svc, &order).await?;
    let list = current(svc).await?;
    publish_list(svc, &list).await;
    Ok(json!({ "order": list }))
}

async fn save(svc: &Services, order: &[MachineOrderKey]) -> Result<()> {
    let encoded = serde_json::to_string(order)
        .map_err(|error| Error::Internal(format!("machine order: {error}")))?;
    svc.store()
        .set_setting(MACHINE_ORDER_STATE_KEY, &encoded)
        .await
}

/// Drop a removed machine or entry from the saved order, so a later add of
/// the same id is appended at the end, then announce the list.
pub(crate) async fn forget(svc: &Services, removed: &MachineOrderKey) {
    let _turn = ORDER_LOCK.lock().await;
    let pruned = svc
        .store()
        .get_setting(MACHINE_ORDER_STATE_KEY)
        .await
        .map(|raw| parse_saved(raw.as_deref()));
    match pruned {
        Ok(saved) if saved.contains(removed) => {
            if let Err(error) =
                save(svc, &intent_core::machine_order::without(&saved, removed)).await
            {
                tracing::warn!(%error, "machine order: could not drop a removed row");
            }
        }
        Ok(_) => {}
        Err(error) => tracing::warn!(%error, "machine order: could not read the saved order"),
    }
    announce(svc).await;
}

/// Announce the list after a machine or entry was added or renamed.
pub(crate) async fn publish(svc: &Services) {
    let _turn = ORDER_LOCK.lock().await;
    announce(svc).await;
}

async fn announce(svc: &Services) {
    match current(svc).await {
        Ok(list) => publish_list(svc, &list).await,
        Err(error) => {
            tracing::warn!(%error, "machine order: could not read the list to announce it");
        }
    }
}

async fn publish_list(svc: &Services, list: &[MachineOrderRow]) {
    svc.publish_remote_machine_changed(json!({ "order": list }))
        .await;
}
