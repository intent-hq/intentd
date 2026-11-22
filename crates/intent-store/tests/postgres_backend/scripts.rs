//! Script store methods on both backends: definitions, run admission and
//! settlement, first-use bootstrap and the summary list page.
//!
//! Decoy rule: every read runs while the decoy owns rows the subject's query
//! would return without its tenant filter (names and values carry the tenant,
//! so a leaked row never looks right), and every write runs while the decoy's
//! matching rows are in their pre-write state, so the harness's decoy
//! snapshot sees a write without its filter.

use std::collections::BTreeMap;

use intent_core::script_lists::{self, ScriptListOptions};
use intent_core::{
    AgentId, Error, Script, ScriptLastRun, ScriptMode, ScriptPurpose, ScriptRemoteAuthor,
    ScriptRunOutcome, Workspace, WorkspaceId,
};
use intent_store::Store;
use serde_json::json;

use crate::harness::parity_test;
use ScriptMode::{Command, Service};
use ScriptPurpose::{OneOff, Saved};

const T0: &str = "2026-01-01T00:00:00.000Z";
const T1: &str = "2026-01-02T00:00:00.000Z";
const T2: &str = "2026-01-03T00:00:00.000Z";
const T3: &str = "2026-01-04T00:00:00.000Z";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn script(id: &str, ws: &str, created_at: &str, tenant: &str) -> Script {
    Script {
        purpose: ScriptPurpose::Saved,
        archived_at: None,
        last_run: None,
        id: id.into(),
        workspace_id: ws.into(),
        name: format!("{tenant} {id}"),
        command: format!("echo {tenant}"),
        cwd: Some("/srv".into()),
        env: Some(BTreeMap::from([("TENANT".to_owned(), tenant.to_owned())])),
        mode: ScriptMode::Service,
        category: Some("build".into()),
        source: "user".into(),
        auto_start: Some(true),
        created_at: created_at.into(),
        updated_at: Some(created_at.into()),
        remote_author: None,
    }
}

fn run_script(id: &str, mode: ScriptMode, purpose: ScriptPurpose, tenant: &str) -> Script {
    Script {
        mode,
        purpose,
        ..script(id, "ws-a", T0, tenant)
    }
}

fn workspace(id: &str) -> Workspace {
    Workspace {
        id: wid(id),
        title: format!("title {id}"),
        branch: "main".into(),
        created_at: T0.into(),
        updated_at: T0.into(),
        ..Default::default()
    }
}

fn not_found<T: std::fmt::Debug>(r: &intent_core::Result<T>) -> bool {
    matches!(r, Err(Error::NotFound(_)))
}

async fn all(store: &Store) -> Vec<Script> {
    store.list_all_scripts().await.unwrap()
}

async fn active(store: &Store) -> Vec<Script> {
    store.list_unarchived_scripts().await.unwrap()
}

parity_test!(script_definitions_round_trip, |store, tenant| {
    let (wa, wb) = (wid("ws-a"), wid("ws-b"));
    assert!(all(&store).await.is_empty());
    assert!(active(&store).await.is_empty());
    for id in ["s-a", "s-gone", "missing"] {
        assert_eq!(store.script_workspace(id).await.unwrap(), None);
    }
    assert_eq!(
        store.get_script_in_workspace(&wa, "s-a").await.unwrap(),
        None
    );
    assert_eq!(store.get_active_script(&wa, "s-a").await.unwrap(), None);

    let a = script("s-a", "ws-a", T0, tenant);
    store.upsert_script(&a).await.unwrap();
    let b = script("s-b", "ws-a", T1, tenant);
    let mut c = script("s-c", "ws-b", T2, tenant);
    c.purpose = ScriptPurpose::OneOff;
    c.archived_at = Some(T2.into());
    c.remote_author = Some(ScriptRemoteAuthor {
        agent_id: AgentId(format!("{tenant}-agent")),
        node_id: "node-1".into(),
    });
    c.last_run = Some(ScriptLastRun {
        run_id: Some("run-c".into()),
        outcome: ScriptRunOutcome::Succeeded,
        exit_code: Some(0),
        started_at: Some(T1.into()),
        stopped_at: T2.into(),
        error: None,
        started_by: None,
        exit_reason: None,
    });
    store.upsert_scripts(&[b.clone(), c.clone()]).await.unwrap();
    assert_eq!(all(&store).await, [a.clone(), b.clone(), c.clone()]);
    assert_eq!(active(&store).await, [a.clone(), b.clone()]);
    assert_eq!(
        store.get_script_in_workspace(&wa, "s-a").await.unwrap(),
        Some(a.clone())
    );
    assert_eq!(
        store.get_script_in_workspace(&wa, "missing").await.unwrap(),
        None
    );
    assert_eq!(
        store.get_active_script(&wa, "s-b").await.unwrap(),
        Some(b.clone())
    );
    assert_eq!(store.get_active_script(&wb, "s-c").await.unwrap(), None);

    // A replace swaps the whole definition; a repeated id keeps the last one.
    let mut a2 = a.clone();
    a2.command = format!("make {tenant}");
    a2.env = None;
    store.upsert_script(&a2).await.unwrap();
    let mut b2 = b.clone();
    b2.name = format!("{tenant} b2");
    store
        .upsert_scripts(&[b.clone(), b2.clone()])
        .await
        .unwrap();
    assert_eq!(all(&store).await, [a2.clone(), b2.clone(), c.clone()]);

    let mut moved = b2.clone();
    moved.workspace_id = "ws-b".into();
    assert!(not_found(&store.upsert_script_in_workspace(&moved).await));
    let new = script("s-new", "ws-a", T3, tenant);
    store.upsert_script_in_workspace(&new).await.unwrap();
    assert_eq!(
        store.get_script_in_workspace(&wa, "s-new").await.unwrap(),
        Some(new.clone())
    );

    store
        .set_script_archived_at(&wa, "s-b", Some(T3))
        .await
        .unwrap();
    assert_eq!(store.get_active_script(&wa, "s-b").await.unwrap(), None);
    assert!(not_found(
        &store.set_script_archived_at(&wa, "missing", Some(T3)).await
    ));

    store
        .upsert_script(&script("s-gone", "ws-a", T0, tenant))
        .await
        .unwrap();
    assert!(store.remove_script("s-gone").await.unwrap());
    assert!(!store.remove_script("s-gone").await.unwrap());
    assert_eq!(store.script_workspace("s-gone").await.unwrap(), None);
    store
        .upsert_script(&script("s-rm", "ws-a", T0, tenant))
        .await
        .unwrap();
    store.remove_script_in_workspace(&wa, "s-rm").await.unwrap();
    assert!(not_found(
        &store.remove_script_in_workspace(&wa, "s-rm").await
    ));

    let mut b3 = b2.clone();
    b3.archived_at = Some(T3.into());
    assert_eq!(all(&store).await, [a2.clone(), b3, c, new.clone()]);
    assert_eq!(active(&store).await, [a2, new]);

    // Each tenant's s-t lives in its own workspace, so the answer is per tenant.
    let ws_t = format!("ws-{tenant}");
    store
        .upsert_script(&script("s-t", &ws_t, T3, tenant))
        .await
        .unwrap();
    assert_eq!(
        store.script_workspace("s-t").await.unwrap(),
        Some(wid(&ws_t))
    );
    // s-ws is in ws-a here and in ws-b for the decoy.
    store
        .upsert_script(&script("s-ws", "ws-a", T3, tenant))
        .await
        .unwrap();
    assert_eq!(
        store.get_script_in_workspace(&wb, "s-ws").await.unwrap(),
        None
    );
    assert!(not_found(
        &store.set_script_archived_at(&wb, "s-ws", None).await
    ));
    assert!(not_found(
        &store.remove_script_in_workspace(&wb, "s-ws").await
    ));

    if tenant == "decoy" {
        // Pre-write states for the subject's writes, and rows its absent probes would see.
        store
            .set_script_archived_at(&wa, "s-b", None)
            .await
            .unwrap();
        store
            .set_script_archived_at(&wb, "s-c", None)
            .await
            .unwrap();
        for (id, ws) in [
            ("s-gone", "ws-a"),
            ("s-rm", "ws-a"),
            ("s-new", "ws-b"),
            ("s-ws", "ws-b"),
            ("missing", "ws-a"),
        ] {
            store
                .upsert_script(&script(id, ws, T0, tenant))
                .await
                .unwrap();
        }
    }
});

parity_test!(script_runs_admit_start_and_settle, |store, tenant| {
    let wa = wid("ws-a");
    let tok = format!("{tenant}-run");
    let tok = tok.as_str();
    assert!(store.pending_script_runs().await.unwrap().is_empty());
    assert!(store
        .list_was_running_script_ids()
        .await
        .unwrap()
        .is_empty());
    for id in ["r-start", "r-settle", "r-reset"] {
        assert_eq!(store.latest_script_run(&wa, id).await.unwrap(), None);
    }

    for (id, mode, purpose) in [
        ("r-admit", Command, Saved),
        ("r-start", Service, Saved),
        ("r-abandon", Service, Saved),
        ("r-settle", Command, OneOff),
        ("r-recover", Command, Saved),
        ("r-mark", Service, Saved),
        ("r-unmark", Service, Saved),
        ("r-reset", Command, Saved),
    ] {
        store
            .upsert_script(&run_script(id, mode, purpose, tenant))
            .await
            .unwrap();
    }

    if tenant == "decoy" {
        // Leave each row in the state the subject's write starts from. Rows
        // behind token-keyed writes hold the subject's token and a T0 start,
        // so a leaky admit (clears the start), start (sets T1), abandon or
        // settle still changes them.
        store
            .upsert_script(&run_script("r-missing", Command, Saved, tenant))
            .await
            .unwrap();
        for id in ["r-start", "r-abandon", "r-settle", "r-recover"] {
            store
                .admit_script_run(&wa, id, "subject-run")
                .await
                .unwrap();
            store
                .start_script_run(&wa, id, "subject-run", T0)
                .await
                .unwrap();
        }
        store.admit_script_run(&wa, "r-reset", tok).await.unwrap();
        store
            .set_script_was_running("ws-a", "r-unmark", true)
            .await
            .unwrap();
    } else {
        store.admit_script_run(&wa, "r-admit", tok).await.unwrap();
        assert!(not_found(
            &store.admit_script_run(&wa, "r-missing", tok).await
        ));
        assert_eq!(
            store.latest_script_run(&wa, "r-admit").await.unwrap(),
            Some((tok.to_owned(), None))
        );

        store.admit_script_run(&wa, "r-start", tok).await.unwrap();
        store
            .start_script_run(&wa, "r-start", tok, T1)
            .await
            .unwrap();
        store
            .start_script_run(&wa, "r-start", "stale", T2)
            .await
            .unwrap();

        store.admit_script_run(&wa, "r-abandon", tok).await.unwrap();
        store
            .start_script_run(&wa, "r-abandon", tok, T1)
            .await
            .unwrap();
        store
            .abandon_script_run(&wa, "r-abandon", tok, false)
            .await
            .unwrap();

        let result = ScriptLastRun {
            run_id: Some(tok.into()),
            outcome: ScriptRunOutcome::Failed,
            exit_code: Some(2),
            started_at: Some(T1.into()),
            stopped_at: T2.into(),
            error: Some(format!("{tenant} failed")),
            started_by: None,
            exit_reason: None,
        };
        store.admit_script_run(&wa, "r-settle", tok).await.unwrap();
        assert!(store
            .settle_script_run(&wa, "r-settle", tok, &result, false)
            .await
            .unwrap());
        assert!(!store
            .settle_script_run(&wa, "r-settle", tok, &result, false)
            .await
            .unwrap());
        assert_eq!(
            store.latest_script_run(&wa, "r-settle").await.unwrap(),
            Some((tok.to_owned(), Some(result.clone())))
        );
        let settled = store
            .get_script_in_workspace(&wa, "r-settle")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(settled.last_run, Some(result.clone()));
        assert_eq!(settled.archived_at.as_deref(), Some(T2));

        store.admit_script_run(&wa, "r-recover", tok).await.unwrap();
        assert!(store
            .settle_script_run(&wa, "r-recover", tok, &result, true)
            .await
            .unwrap());

        store
            .set_script_was_running("ws-a", "r-mark", true)
            .await
            .unwrap();
        store
            .set_script_was_running("ws-a", "r-unmark", true)
            .await
            .unwrap();
        store
            .set_script_was_running("ws-a", "r-unmark", false)
            .await
            .unwrap();

        // A replace clears the admission and the latest run.
        store.admit_script_run(&wa, "r-reset", tok).await.unwrap();
        store
            .upsert_script(&run_script("r-reset", Command, Saved, tenant))
            .await
            .unwrap();
        assert_eq!(store.latest_script_run(&wa, "r-reset").await.unwrap(), None);

        let mut pending = store.pending_script_runs().await.unwrap();
        pending.sort();
        assert_eq!(
            pending,
            [
                (wa.clone(), "r-admit".into(), tok.into(), None),
                (wa.clone(), "r-start".into(), tok.into(), Some(T1.into())),
            ]
        );
        let mut running = store.list_was_running_script_ids().await.unwrap();
        running.sort();
        let expected: Vec<(String, String)> = ["r-admit", "r-mark", "r-recover", "r-start"]
            .iter()
            .map(|id| ("ws-a".to_owned(), (*id).to_owned()))
            .collect();
        assert_eq!(running, expected);
    }
});

parity_test!(script_bootstrap_claims_a_workspace_once, |store, tenant| {
    let (wa, wb) = (wid("ws-a"), wid("ws-b"));
    for id in ["ws-a", "ws-b"] {
        store.insert_workspace(&workspace(id)).await.unwrap();
    }
    assert!(!store.workspace_scripts_initialized(&wa).await.unwrap());
    assert!(!store
        .workspace_scripts_initialized(&wid("ws-none"))
        .await
        .unwrap());
    assert!(all(&store).await.is_empty());

    assert!(!store.bootstrap_scripts(&wa, &[]).await.unwrap());
    assert!(matches!(
        store
            .bootstrap_scripts(&wa, &[script("d-x", "ws-b", T0, tenant)])
            .await,
        Err(Error::InvalidParams(_))
    ));
    assert!(!store.workspace_scripts_initialized(&wa).await.unwrap());

    let defaults = [
        script("d-1", "ws-a", T0, tenant),
        script("d-2", "ws-a", T1, tenant),
    ];
    assert!(store.bootstrap_scripts(&wa, &defaults).await.unwrap());
    assert!(store.workspace_scripts_initialized(&wa).await.unwrap());
    assert_eq!(all(&store).await, defaults);
    let mut again = defaults.clone();
    again[0].name = "again".into();
    assert!(!store.bootstrap_scripts(&wa, &again).await.unwrap());
    assert_eq!(all(&store).await, defaults);

    // Purging every script does not reopen the first-use claim.
    for d in &defaults {
        assert!(store.remove_script(&d.id).await.unwrap());
    }
    assert!(store.workspace_scripts_initialized(&wa).await.unwrap());
    assert!(!store.bootstrap_scripts(&wa, &defaults).await.unwrap());
    assert!(all(&store).await.is_empty());

    if tenant == "decoy" {
        // ws-b stays unclaimed for the subject's claim; ws-none is initialized.
        // The ids the subject writes and removes live in ws-none, so placing
        // them does not claim ws-a or ws-b.
        store.insert_workspace(&workspace("ws-none")).await.unwrap();
        for id in ["d-none", "d-1", "d-2", "d-b"] {
            store
                .upsert_script(&script(id, "ws-none", T0, tenant))
                .await
                .unwrap();
        }
    } else {
        assert!(!store.workspace_scripts_initialized(&wb).await.unwrap());
        let b = [script("d-b", "ws-b", T0, tenant)];
        assert!(store.bootstrap_scripts(&wb, &b).await.unwrap());
        assert!(store.workspace_scripts_initialized(&wb).await.unwrap());
        assert_eq!(all(&store).await, b);
    }
});

parity_test!(script_summary_pages_bound_and_page_rows, |store, tenant| {
    let wa = wid("ws-a");
    let opts = |limit: usize, cursor: Option<String>| ScriptListOptions {
        summary: true,
        limit,
        cursor,
    };
    let empty = json!({"view": "summary", "scripts": []});
    assert_eq!(
        store
            .script_summary_page(&wa, None, false, &opts(2, None))
            .await
            .unwrap(),
        empty
    );
    assert_eq!(
        store
            .saved_script_summary_page(&wa, &opts(10, None))
            .await
            .unwrap(),
        empty
    );

    let mut long = script("p-long", "ws-a", T0, tenant);
    long.command = format!("{tenant}:{}", "é".repeat(100));
    long.category = None;
    long.updated_at = None;
    long.auto_start = Some(false);
    let error = format!("{}{tenant}", "x".repeat(300));
    long.last_run = Some(ScriptLastRun {
        run_id: None,
        outcome: ScriptRunOutcome::Failed,
        exit_code: None,
        started_at: None,
        stopped_at: T1.into(),
        error: Some(error.clone()),
        started_by: None,
        exit_reason: None,
    });
    long.remote_author = Some(ScriptRemoteAuthor {
        agent_id: AgentId(format!("{tenant}-agent")),
        node_id: "node-1".into(),
    });
    let saved = script("p-saved", "ws-a", T1, tenant);
    let mut one_off = script("p-one", "ws-a", T2, tenant);
    one_off.purpose = ScriptPurpose::OneOff;
    let mut archived = script("p-arch", "ws-a", T3, tenant);
    archived.archived_at = Some(T3.into());
    let other = script("p-other", "ws-b", T3, tenant);
    store
        .upsert_scripts(&[long.clone(), saved, one_off, archived, other])
        .await
        .unwrap();

    let names = |page: &serde_json::Value| -> Vec<String> {
        page["scripts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["name"].as_str().unwrap().to_owned())
            .collect()
    };
    let first = store
        .script_summary_page(&wa, None, false, &opts(2, None))
        .await
        .unwrap();
    assert_eq!(
        names(&first),
        [format!("{tenant} p-one"), format!("{tenant} p-saved")]
    );
    assert_eq!(
        first["scripts"][0],
        json!({
            "id": "p-one", "workspaceId": "ws-a", "name": format!("{tenant} p-one"),
            "mode": "service", "source": "user", "createdAt": T2, "updatedAt": T2,
            "category": "build", "autoStart": true, "purpose": "oneOff",
            "commandPreview": format!("echo {tenant}"), "commandTruncated": false,
        })
    );
    let cursor = first["nextCursor"].as_str().unwrap().to_owned();
    let second = store
        .script_summary_page(&wa, None, false, &opts(2, Some(cursor)))
        .await
        .unwrap();
    assert!(second.get("nextCursor").is_none());
    let (command_preview, _) = script_lists::prefix(&long.command, 160);
    let (error_preview, _) = script_lists::prefix(&error, 256);
    assert_eq!(
        second["scripts"],
        json!([{
            "id": "p-long", "workspaceId": "ws-a", "name": format!("{tenant} p-long"),
            "mode": "service", "source": "user", "createdAt": T0, "autoStart": false,
            "purpose": "saved", "commandPreview": command_preview, "commandTruncated": true,
            "lastRun": {
                "outcome": "failed", "stoppedAt": T1,
                "error": error_preview, "errorTruncated": true,
            },
            "remoteAuthor": {"agentId": format!("{tenant}-agent"), "nodeId": "node-1"},
        }])
    );

    let saved_page = store
        .saved_script_summary_page(&wa, &opts(10, None))
        .await
        .unwrap();
    assert_eq!(
        names(&saved_page),
        [format!("{tenant} p-saved"), format!("{tenant} p-long")]
    );
    // An owner-scoped script.list selects no scripts.
    assert_eq!(
        store
            .script_summary_page(&wa, Some(&AgentId("ag-1".into())), false, &opts(10, None))
            .await
            .unwrap(),
        empty
    );
    // Known gap: no tenant can hold script_monitor rows yet (the registration
    // fence needs a live agent_session, not ported), so the monitor branch
    // is only checked empty. Leak-test it when the monitor domain is ported.
    assert_eq!(
        store
            .script_summary_page(&wa, None, true, &opts(10, None))
            .await
            .unwrap(),
        json!({"view": "summary", "monitors": []})
    );
    assert!(matches!(
        store
            .script_summary_page(&wa, None, false, &opts(2, Some("bad".into())))
            .await,
        Err(Error::InvalidParams(_))
    ));
});
