//! Change metrics and usage counters on both backends.
//!
//! Decoy rule: values carry the tenant (its name's length), the decoy keeps
//! every row the subject deletes, and it owns decoy-only rows behind the ids
//! the subject probes as absent. Ids, models and providers mix case so their
//! order (and the bucket range filters) depend on `COLLATE "C"`.

use intent_core::{Error, Workspace, WorkspaceId};
use intent_store::{LocalStamp, Store, UsageRateDelta, UsageStatsDelta};

use crate::harness::{exec, parity_test};

const T0: &str = "2026-01-01T00:00:00.000Z";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

async fn add_workspace(store: &Store, id: &str) {
    store
        .insert_workspace(&Workspace {
            id: wid(id),
            title: format!("title {id}"),
            branch: "main".into(),
            created_at: T0.into(),
            updated_at: T0.into(),
            ..Default::default()
        })
        .await
        .unwrap();
}

fn weight(tenant: &str) -> i64 {
    i64::try_from(tenant.len()).unwrap()
}

parity_test!(workspace_metrics_round_trip, |store, tenant| {
    let k = weight(tenant);
    for id in ["ws-a", "ws-B", "ws-c"] {
        add_workspace(&store, id).await;
    }
    assert_eq!(
        store.get_workspace_metrics(&wid("ws-a")).await.unwrap(),
        None
    );

    store
        .upsert_workspace_metrics(&wid("ws-a"), 1, 2, 3)
        .await
        .unwrap();
    store
        .upsert_workspace_metrics(&wid("ws-a"), 10 * k, 20 * k, 30 * k)
        .await
        .unwrap();
    let row = store
        .get_workspace_metrics(&wid("ws-a"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            row.workspace_id.0.as_str(),
            row.additions,
            row.deletions,
            row.files_changed
        ),
        ("ws-a", 10 * k, 20 * k, 30 * k)
    );
    assert!(!row.updated_at.is_empty());

    store
        .upsert_workspace_metrics(&wid("ws-B"), k, 0, 1)
        .await
        .unwrap();
    store
        .upsert_workspace_metrics(&wid("ws-c"), k, k, 2)
        .await
        .unwrap();
    let ids = |rows: Vec<intent_store::WorkspaceMetricsRow>| -> Vec<(String, i64)> {
        rows.into_iter()
            .map(|r| (r.workspace_id.0, r.additions))
            .collect()
    };
    let all = vec![
        ("ws-B".to_owned(), k),
        ("ws-a".to_owned(), 10 * k),
        ("ws-c".to_owned(), k),
    ];
    assert_eq!(ids(store.list_workspace_metrics().await.unwrap()), all);

    let err = store
        .upsert_workspace_metrics(&wid("ws-missing"), 1, 1, 1)
        .await;
    assert!(matches!(err, Err(Error::Internal(_))), "{err:?}");

    if tenant == "decoy" {
        add_workspace(&store, "ws-gone").await;
        store
            .upsert_workspace_metrics(&wid("ws-gone"), 9, 9, 9)
            .await
            .unwrap();
        return;
    }
    assert_eq!(
        store.get_workspace_metrics(&wid("ws-gone")).await.unwrap(),
        None
    );
    store
        .delete_workspace_metrics(&wid("ws-gone"))
        .await
        .unwrap();
    store.delete_workspace_metrics(&wid("ws-c")).await.unwrap();
    assert_eq!(
        store.get_workspace_metrics(&wid("ws-c")).await.unwrap(),
        None
    );
    assert_eq!(
        ids(store.list_workspace_metrics().await.unwrap()),
        all[..2].to_vec()
    );
});

parity_test!(agent_metrics_round_trip, |store, tenant| {
    let k = weight(tenant);
    for id in ["ws-a", "ws-B", "ws-c"] {
        add_workspace(&store, id).await;
    }
    store
        .upsert_agent_metrics(&wid("ws-a"), "agent-a", 1, 1, 1)
        .await
        .unwrap();
    store
        .upsert_agent_metrics(&wid("ws-a"), "agent-a", 10 * k, 20 * k, 30 * k)
        .await
        .unwrap();
    store
        .upsert_agent_metrics(&wid("ws-a"), "agent-B", k, 0, 1)
        .await
        .unwrap();
    store
        .upsert_agent_metrics(&wid("ws-B"), "agent-a", 2 * k, 0, 1)
        .await
        .unwrap();
    for ws in ["ws-a", "ws-B"] {
        store
            .upsert_agent_metrics(&wid(ws), "agent-x", k, k, k)
            .await
            .unwrap();
    }
    for agent in ["agent-a", "agent-B"] {
        store
            .upsert_agent_metrics(&wid("ws-c"), agent, k, k, k)
            .await
            .unwrap();
    }
    let err = store
        .upsert_agent_metrics(&wid("ws-missing"), "agent-a", 1, 1, 1)
        .await;
    assert!(matches!(err, Err(Error::Internal(_))), "{err:?}");

    let pairs = |rows: Vec<intent_store::AgentMetricsRow>| -> Vec<(String, String, i64)> {
        rows.into_iter()
            .map(|r| (r.workspace_id.0, r.agent_id, r.additions))
            .collect()
    };
    let row = |ws: &str, agent: &str, additions: i64| (ws.to_owned(), agent.to_owned(), additions);
    let ws_a = vec![
        row("ws-a", "agent-B", k),
        row("ws-a", "agent-a", 10 * k),
        row("ws-a", "agent-x", k),
    ];
    assert_eq!(
        pairs(
            store
                .list_agent_metrics_for_workspace(&wid("ws-a"))
                .await
                .unwrap()
        ),
        ws_a
    );
    let full = store
        .list_agent_metrics_for_workspace(&wid("ws-a"))
        .await
        .unwrap();
    assert_eq!((full[1].deletions, full[1].files_changed), (20 * k, 30 * k));
    assert!(!full[1].updated_at.is_empty());
    assert_eq!(
        pairs(store.list_agent_metrics("agent-a").await.unwrap()),
        vec![
            row("ws-B", "agent-a", 2 * k),
            row("ws-a", "agent-a", 10 * k),
            row("ws-c", "agent-a", k),
        ]
    );

    if tenant == "decoy" {
        add_workspace(&store, "ws-gone").await;
        store
            .upsert_agent_metrics(&wid("ws-gone"), "agent-gone", 9, 9, 9)
            .await
            .unwrap();
        return;
    }
    assert!(store
        .list_agent_metrics("agent-gone")
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .list_agent_metrics_for_workspace(&wid("ws-gone"))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.delete_agent_metrics("agent-gone").await.unwrap(), 0);
    store
        .delete_agent_metrics_for_workspace(&wid("ws-gone"))
        .await
        .unwrap();

    assert_eq!(store.delete_agent_metrics("agent-x").await.unwrap(), 2);
    assert!(store
        .list_agent_metrics("agent-x")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(store.delete_agent_metrics("agent-x").await.unwrap(), 0);

    store
        .delete_agent_metrics_for_workspace(&wid("ws-c"))
        .await
        .unwrap();
    assert!(store
        .list_agent_metrics_for_workspace(&wid("ws-c"))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        pairs(
            store
                .list_agent_metrics_for_workspace(&wid("ws-a"))
                .await
                .unwrap()
        ),
        ws_a[..2].to_vec()
    );
});

/// A lowercase `t` sorts after every `T` bucket in byte order, but between
/// them under a natural-language collation.
const LOWER: &str = "2026-07-30t14:07:00Z";

parity_test!(usage_rate_round_trip, |store, tenant| {
    let k = weight(tenant).cast_unsigned();
    let tokens = |n: u64| UsageRateDelta {
        input_tokens: n,
        ..Default::default()
    };
    store
        .add_usage_rate(
            "2026-07-30T14:07:00Z",
            &UsageRateDelta {
                input_tokens: 100,
                output_tokens: 40,
                cache_read_tokens: 20,
                cache_creation_tokens: 10,
                thought_tokens: 5,
            },
        )
        .await
        .unwrap();
    store
        .add_usage_rate(
            "2026-07-30T14:07:00Z",
            &UsageRateDelta {
                input_tokens: k,
                output_tokens: 10,
                thought_tokens: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    store
        .add_usage_rate("2026-07-30T14:09:00Z", &tokens(3 * k))
        .await
        .unwrap();
    store.add_usage_rate(LOWER, &tokens(4 * k)).await.unwrap();
    store
        .add_usage_rate("2026-07-30T14:08:00Z", &tokens(2 * k))
        .await
        .unwrap();

    let rows = store.list_usage_rate_since("").await.unwrap();
    let keys: Vec<(&str, u64)> = rows
        .iter()
        .map(|r| (r.bucket_utc.as_str(), r.input_tokens))
        .collect();
    assert_eq!(
        keys,
        vec![
            ("2026-07-30T14:07:00Z", 100 + k),
            ("2026-07-30T14:08:00Z", 2 * k),
            ("2026-07-30T14:09:00Z", 3 * k),
            (LOWER, 4 * k),
        ]
    );
    let first = &rows[0];
    assert_eq!(
        (
            first.output_tokens,
            first.cache_read_tokens,
            first.cache_creation_tokens,
            first.thought_tokens
        ),
        (50, 20, 10, 8)
    );
    assert_eq!(rows[1].thought_tokens, 0);

    let since: Vec<String> = store
        .list_usage_rate_since("2026-07-30T14:08:00Z")
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.bucket_utc)
        .collect();
    assert_eq!(
        since,
        ["2026-07-30T14:08:00Z", "2026-07-30T14:09:00Z", LOWER]
    );

    if tenant == "decoy" {
        store
            .add_usage_rate("2026-08-01T00:00:00Z", &tokens(9))
            .await
            .unwrap();
        return;
    }
    assert!(store
        .list_usage_rate_since("2026-07-31T00:00:00Z")
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .delete_usage_rate_before("2026-07-30T14:08:00Z")
            .await
            .unwrap(),
        2,
        "the cutoff bucket goes too; the lowercase one sorts after it"
    );
    assert_eq!(
        store
            .delete_usage_rate_before("2026-07-30T14:08:00Z")
            .await
            .unwrap(),
        0
    );
    let left: Vec<String> = store
        .list_usage_rate_since("")
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.bucket_utc)
        .collect();
    assert_eq!(left, ["2026-07-30T14:09:00Z", LOWER]);
});

fn stamp(date: &str, hour: u8) -> LocalStamp {
    LocalStamp {
        date: date.to_owned(),
        hour,
    }
}

parity_test!(usage_stats_round_trip, |store, tenant, db| {
    let k = weight(tenant).cast_unsigned();
    let bucket = "2026-07-25T14:00:00Z";
    let local = stamp("2026-07-25", 7);
    store
        .add_usage_stats(
            bucket,
            "Opus 4.8",
            "claude-code",
            Some(&local),
            &UsageStatsDelta {
                input_tokens: 100,
                output_tokens: 40,
                cache_read_tokens: 20,
                cache_creation_tokens: 10,
                thought_tokens: 5,
                runs: 1,
                sessions_started: 1,
                longest_run_ms: 5_000,
                lines_added: 3,
                lines_deleted: 1,
            },
        )
        .await
        .unwrap();
    // A shorter run keeps the MAX; the later stamp does not replace the first.
    store
        .add_usage_stats(
            bucket,
            "Opus 4.8",
            "claude-code",
            Some(&stamp("2026-07-25", 6)),
            &UsageStatsDelta {
                input_tokens: k,
                output_tokens: 10,
                thought_tokens: 2,
                runs: 1,
                longest_run_ms: 2_000,
                lines_added: 4,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let one = |n: u64| UsageStatsDelta {
        input_tokens: n,
        runs: 1,
        ..Default::default()
    };
    for (model, provider) in [
        ("Sonnet 5", "claude-code"),
        ("opus 5", "claude-code"),
        ("Opus 4.8", "auggie"),
        ("Opus 4.8", "Codex"),
    ] {
        store
            .add_usage_stats(bucket, model, provider, Some(&local), &one(k))
            .await
            .unwrap();
    }
    store
        .add_usage_stats(
            "2026-07-25T15:00:00Z",
            "Opus 4.8",
            "claude-code",
            None,
            &one(2 * k),
        )
        .await
        .unwrap();
    if let Some(db) = db {
        exec(
            &db.admin,
            format!(
                "INSERT INTO usage_stats_hourly (tenant_id, bucket_utc, model, local_date, local_hour) \
                 VALUES ('{tenant}', '2026-07-25T16:00:00Z', 'Opus 4.8', '2026-07-25', 99)"
            ),
        )
        .await;
    } else {
        sqlx::raw_sql(
            "INSERT INTO usage_stats_hourly (bucket_utc, model, local_date, local_hour) \
             VALUES ('2026-07-25T16:00:00Z', 'Opus 4.8', '2026-07-25', 99)",
        )
        .execute(store.write_pool())
        .await
        .unwrap();
    }

    let mut rows = store.list_usage_stats_hourly().await.unwrap();
    let keys: Vec<(&str, &str, &str, u64)> = rows
        .iter()
        .map(|r| {
            (
                r.bucket_utc.as_str(),
                r.model.as_str(),
                r.provider.as_str(),
                r.input_tokens,
            )
        })
        .collect();
    assert_eq!(
        keys,
        vec![
            (bucket, "Opus 4.8", "Codex", k),
            (bucket, "Opus 4.8", "auggie", k),
            (bucket, "Opus 4.8", "claude-code", 100 + k),
            (bucket, "Sonnet 5", "claude-code", k),
            (bucket, "opus 5", "claude-code", k),
            ("2026-07-25T15:00:00Z", "Opus 4.8", "claude-code", 2 * k),
            ("2026-07-25T16:00:00Z", "Opus 4.8", "unknown", 0),
        ]
    );
    let folded = &rows[2];
    assert_eq!(
        (
            folded.output_tokens,
            folded.cache_read_tokens,
            folded.cache_creation_tokens,
            folded.thought_tokens,
            folded.runs,
            folded.sessions_started,
            folded.longest_run_ms,
            folded.lines_added,
            folded.lines_deleted,
        ),
        (50, 20, 10, 7, 2, 1, 5_000, 7, 1)
    );
    assert_eq!(
        (folded.local_date.as_deref(), folded.local_hour),
        (Some("2026-07-25"), Some(7))
    );
    assert_eq!(
        (rows[5].local_date.as_deref(), rows[5].local_hour),
        (None, None)
    );
    assert_eq!(
        (rows[6].local_date.as_deref(), rows[6].local_hour),
        (Some("2026-07-25"), None),
        "hour 99 must not surface"
    );

    store
        .add_usage_stats(
            bucket,
            "Opus 4.8",
            "claude-code",
            None,
            &UsageStatsDelta {
                longest_run_ms: 9_000,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    rows = store.list_usage_stats_hourly().await.unwrap();
    assert_eq!(rows[2].longest_run_ms, 9_000);
    assert_eq!(rows[2].local_hour, Some(7));
});
