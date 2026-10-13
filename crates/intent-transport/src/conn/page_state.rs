//! One persisted tuple per subscription. Backpressure retains a dirty bit,
//! never a queue of decoded notes, comment collections or changed ranges.
use super::{
    events, json, send_fast_path_error, spawn_forwarder, subscriptions, Arc, Channel, ConnSubs,
    EventBus, NoteId, OutboundSender, SubscriptionFilter, Value, WorkspaceApi, WorkspaceId,
    NOTE_CREATED, NOTE_DELETED, NOTE_UPDATED, WORKSPACE_UPDATED,
};
use crate::annotation_subscription::{build_page_state_push, PageStateSubscription};
use intent_core::events::{
    COMMENT_ADDED, COMMENT_DELETED, COMMENT_RESOLVED, LINE_ATTRIBUTION_UPDATED, WORKSPACE_DELETED,
};
use intent_services::InvalidationSubscription;

pub(super) fn valid_id(id: Option<&Value>) -> bool {
    match id {
        None => true,
        Some(Value::String(id)) => id.len() <= 64,
        Some(Value::Number(id)) => id
            .as_i64()
            .is_some_and(|n| n.unsigned_abs() <= 9_007_199_254_740_991),
        _ => false,
    }
}

pub(super) async fn subscribe(
    id: events::IdInfo,
    channel: Channel,
    request: PageStateSubscription,
    api: &Arc<dyn WorkspaceApi>,
    bus: &EventBus,
    outbound: &OutboundSender,
    registry: &mut ConnSubs,
) -> bool {
    let timer = subscriptions::SnapshotTimer::start(channel, &request.workspace_id);
    let subscription = bus.subscribe_invalidation(SubscriptionFilter {
        event_types: [
            NOTE_CREATED,
            NOTE_UPDATED,
            NOTE_DELETED,
            COMMENT_ADDED,
            COMMENT_DELETED,
            COMMENT_RESOLVED,
            LINE_ATTRIBUTION_UPDATED,
            WORKSPACE_UPDATED,
            WORKSPACE_DELETED,
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        workspace_id: Some(request.workspace_id.clone()),
        batch_window: None,
        collaborator_only: crate::context::is_non_administrator_caller(),
        ..Default::default()
    });
    // Authorization gets its own payload-free signal so revocation is checked
    // while the bulk lane is blocked, without keeping membership Event clones.
    let membership = bus.subscribe_invalidation(SubscriptionFilter {
        event_types: vec![WORKSPACE_UPDATED.into(), WORKSPACE_DELETED.into()],
        workspace_id: Some(request.workspace_id.clone()),
        ..Default::default()
    });
    let workspace = WorkspaceId::from(request.workspace_id);
    let note = NoteId::from(request.note_id);
    // Admission errors are RPC failures, never successful empty subscriptions.
    let state = match api
        .get_note_page_state(workspace.clone(), note.clone(), None)
        .await
    {
        Ok(state) => state,
        Err(error) => return respond_error(id, error, outbound).await,
    };
    let subscription_id = events::next_subscription_id();
    if state["scope"]["workspaceId"] != workspace.as_str()
        || state["scope"]["noteId"] != note.as_str()
        || build_page_state_push(&subscription_id, 0, &state).is_err()
    {
        return send_fast_path_error(id, "Invalid pageState subscription state", outbound).await;
    }
    if let Some(group) = request.replace_group.as_deref() {
        registry.remove_group(api, group).await;
    }
    if id.present
        && outbound
            .send_priority(events::success_frame(
                &id.echo,
                &json!({"subscriptionId":subscription_id}),
            ))
            .await
            .is_err()
    {
        return false;
    }
    let handle = spawn_forwarder(forward(
        api.clone(),
        workspace,
        note,
        state["scope"].clone(),
        subscription,
        membership,
        subscription_id.clone(),
        outbound.clone(),
        timer,
    ));
    registry.insert(subscription_id, handle, request.replace_group, None);
    true
}

async fn respond_error(
    id: events::IdInfo,
    error: intent_core::Error,
    outbound: &OutboundSender,
) -> bool {
    let message = match error {
        intent_core::Error::NotFound(_) => "Note page state not found",
        intent_core::Error::Forbidden(_) => "Note page state access denied",
        _ => "Note page state unavailable",
    };
    if id.present {
        return outbound
            .send_priority(events::error_frame(&id.echo, error.code(), message))
            .await
            .is_ok();
    }
    true
}

/// Lag is a refresh signal, not permission to use a legacy collection snapshot.
/// Membership invalidation is observed even while the bulk lane is full.
async fn changed(
    subscription: &mut InvalidationSubscription,
    membership: &mut InvalidationSubscription,
    api: &Arc<dyn WorkspaceApi>,
    workspace: &WorkspaceId,
    note: &NoteId,
    scope: &Value,
) -> bool {
    tokio::select! {
        biased;
        dirty = membership.changed() => {
            if !dirty { return false; }
            let Some(incarnation) = scope["noteInstanceId"].as_str() else { return false; };
            // A signal carries no authority. Recheck the subscriber's current
            // rights even if there is still no output capacity. Fail closed.
            api.get_note_page_state(workspace.clone(), note.clone(), Some(incarnation.into()))
                .await.is_ok_and(|state| state["scope"] == *scope)
        }
        dirty = subscription.changed() => dirty,
    }
}

#[expect(clippy::too_many_arguments)]
async fn forward(
    api: Arc<dyn WorkspaceApi>,
    workspace: WorkspaceId,
    note: NoteId,
    scope: Value,
    mut subscription: InvalidationSubscription,
    mut membership: InvalidationSubscription,
    subscription_id: String,
    outbound: OutboundSender,
    timer: subscriptions::SnapshotTimer,
) {
    let Some(incarnation) = scope["noteInstanceId"].as_str() else {
        return;
    };
    let sender = outbound.bulk_sender();
    let mut timer = Some(timer);
    let mut last: Option<Value> = None;
    let mut seq = 0;
    let mut pending = true;
    loop {
        if !pending {
            if !changed(
                &mut subscription,
                &mut membership,
                &api,
                &workspace,
                &note,
                &scope,
            )
            .await
            {
                return;
            }
            pending = true;
        }
        // Only one dirty bit is retained while consumers are slow. Reserve
        // before the authorized read so obsolete states are not buffered here.
        let permit = tokio::select! {
            permit = sender.reserve() => match permit { Ok(permit) => permit, Err(_) => return },
            visible = changed(&mut subscription, &mut membership, &api, &workspace, &note, &scope) => {
                if !visible { return; }
                continue;
            }
        };
        let state = match api
            .get_note_page_state(workspace.clone(), note.clone(), Some(incarnation.into()))
            .await
        {
            Ok(state) => state,
            Err(intent_core::Error::NotFound(_) | intent_core::Error::Forbidden(_)) => return,
            Err(_) => {
                drop(permit);
                // Keep the refresh owed after a transient read failure, without
                // publishing a fake empty snapshot or extending a stored lease.
                tokio::select! {
                    visible = changed(&mut subscription, &mut membership, &api, &workspace, &note, &scope) => { if !visible { return; } },
                    () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
                    () = sender.closed() => return,
                }
                continue;
            }
        };
        if state["scope"] != scope {
            return;
        }
        let Ok(frame) = build_page_state_push(&subscription_id, seq, &state) else {
            return;
        };
        if let Some(previous) = &last {
            let generation = |value: &Value| {
                value["stateGeneration"]
                    .as_str()
                    .and_then(|s| s.parse::<u64>().ok())
            };
            match (generation(previous), generation(&state)) {
                (Some(old), Some(new)) if new < old || (new == old && state != *previous) => return,
                (Some(old), Some(new)) if new == old => {
                    pending = false;
                    continue;
                }
                (Some(_), Some(_)) => {}
                _ => return,
            }
        }
        let deleted = state["deleted"] == true;
        permit.send(frame);
        if let Some(timer) = timer.take() {
            timer.snapshot_emitted();
        }
        if deleted {
            return;
        }
        last = Some(state);
        seq += 1;
        pending = false;
    }
}

#[cfg(test)]
mod tests;
