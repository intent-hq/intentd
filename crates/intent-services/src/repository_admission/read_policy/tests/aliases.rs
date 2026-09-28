//! Alias composition through original services, storage, Git, credentials and ACP delivery.
//! HTTP replies and physical completion are the unchanged parent's fixtures.
//! The scheduling wrapper delegates every admission to the real original policy.

use super::*;
use crate::repository_admission_source_tests::fixtures::Fixture as GitFixture;
use crate::source_control_auth_ops::repository_owner::secret_reader::tests::Fixture as AuthFixture;
use intent_core::{Caller, WorkspaceApi};
use serde_json::{json, Value};

const MR: &str = "/api/v4/projects/group%2Fproject/merge_requests/4";

fn expression(namespace: &str, raw: bool) -> String {
    if raw {
        format!("host({{method:'{namespace}.snapshot',args:{{prNumber:4}}}})")
    } else {
        format!("ws.{namespace}.snapshot(4)")
    }
}

fn json_result(reply: &Value) -> Value {
    assert_eq!(reply["result"]["isError"], false, "{reply}");
    let text = reply["result"]["content"][0]["text"].as_str().unwrap();
    // Explicit JSON.stringify avoids making alias parity depend on TOON settings.
    let encoded: String = serde_json::from_str(text).unwrap();
    serde_json::from_str(&encoded).unwrap()
}

fn no_spill(f: &ActualRead) {
    f.auth
        .registry
        .apply(&[("workspaceApi.maxOutputChars".into(), json!(0))])
        .unwrap();
}

fn delivery_refused(reply: &Value) {
    assert_eq!(
        reply["result"],
        json!({"content":[{"type":"text","text":"Private result delivery refused"}],"isError":true}),
        "{reply}"
    );
}

#[intent_test_macros::daemon_test]
async fn public_and_raw_aliases_share_original_qualified_values_cache_and_request() {
    for first in ["pr", "mr"] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        no_spill(&f);
        let control = Arc::new(Control::default());
        let gate = Hold::new();
        *control.hold.lock().unwrap() = Some((Boundary::HostPromise, true, gate.clone()));
        let endpoint = controlled(&f, control.clone());
        let code = format!(
            "const first=await ws.{first}.snapshot(4); return JSON.stringify({{same:ws.pr===ws.mr, values:[first, await ws.pr.snapshot(4), await host({{method:'mr.snapshot',args:{{prNumber:4}}}}), await host({{method:'pr.snapshot',args:{{prNumber:4}}}})]}});"
        );
        let task = tokio::spawn(async move { run(&endpoint, &code).await });
        gate.reached().await;
        let requests_after_first = http.count();
        assert!(requests_after_first > 0);
        gate.release.add_permits(1);
        let value = json_result(&task.await.unwrap());
        assert_eq!(value["same"], true);
        let values = value["values"].as_array().unwrap();
        assert_eq!(values.len(), 4);
        assert!(values.windows(2).all(|pair| pair[0] == pair[1]));
        let snapshot = &values[0];
        assert_eq!(snapshot["repo"], "group/project");
        assert_eq!(snapshot["prNumber"], 4);
        assert_eq!(snapshot["title"], "actual review");
        assert_eq!(snapshot["resource"]["repository"]["provider"], "gitlab");
        assert_eq!(
            snapshot["resource"]["repository"]["instanceBaseUrl"],
            "https://gitlab.test/forge"
        );
        assert_eq!(snapshot["details"]["resource"], snapshot["resource"]);
        assert_eq!(snapshot["details"]["source"]["projectId"], "42");
        assert_eq!(snapshot["availability"]["checks"], "available");
        assert!(snapshot["requirements"].is_object());
        assert_eq!(
            http.count(),
            requests_after_first,
            "all later reads were cache hits"
        );
        assert_eq!(f.auth.service.pr_cache.lock().unwrap().len(), 1);
        let records = control.records.lock().unwrap();
        assert_eq!(
            records.len(),
            5,
            "primary plus lazy read and three cache-hit obligations"
        );
        assert!(records
            .iter()
            .all(|record| Arc::ptr_eq(&record.request, &records[0].request)));
        assert!(control
            .events
            .lock()
            .unwrap()
            .contains(&(Boundary::DirectResponse, 5)));
    }
}

#[intent_test_macros::daemon_test]
async fn ordinary_github_aliases_need_neither_gitlab_readiness_nor_qualified_policy() {
    for unsettled in [false, true] {
        let git = GitFixture::new().await;
        git.git(
            &git.path,
            &["remote", "add", "origin", "https://github.com/o/r.git"],
        );
        let http = ReadServer::new().await;
        let auth = AuthFixture::unadopted(&http.fixture).await;
        let service = if unsettled {
            auth.service
                .store
                .insert_workspace(&git.workspace)
                .await
                .unwrap();
            auth.service.as_ref().clone()
        } else {
            crate::Services::new(git.store.clone())
        }
        .with_source_control(Arc::new(crate::tests::pr::StubForge::default()));
        assert!(service.gitlab_repository_settled_connection().is_err());
        let service = Arc::new(service);
        let endpoint = WorkspaceMcpServer::new(service.clone(), git.workspace.id.clone());
        for repo in [None, Some("explicit/repository")] {
            let expected = intent_core::with_caller(
                Caller::Daemon,
                service.pr_state(git.workspace.id.clone(), 42, repo.map(str::to_owned)),
            )
            .await
            .unwrap();
            for namespace in ["pr", "mr"] {
                let code = format!(
                    "return JSON.stringify(await ws.{namespace}.snapshot(42,{{repo:{}}}));",
                    json!(repo)
                );
                let value = json_result(&run(&endpoint, &code).await);
                assert_eq!(value, expected);
                assert!(value.get("resource").is_none());
                assert!(value.get("details").is_none());
                assert!(value.get("availability").is_none());
            }
        }
        let mut errors = Vec::new();
        for namespace in ["pr", "mr"] {
            errors.push(json_result(&run(&endpoint, &format!(
                "try {{await ws.{namespace}.snapshot(42,{{repo:'malformed'}});}} catch(e) {{return JSON.stringify(e.message);}}"
            )).await));
        }
        assert_eq!(errors[0], errors[1]);
        assert!(errors[0].as_str().unwrap().contains("owner/name"));
        assert_eq!(http.count(), 0);
    }
}

#[intent_test_macros::daemon_test]
async fn unresolved_or_unanchored_aliases_refuse_before_any_qualified_acquisition() {
    for mode in ["ambiguous", "unmapped", "missing-anchor", "missing-request"] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        let control = Arc::new(Control::default());
        match mode {
            "ambiguous" => {
                f.git.git(
                    &f.git.path,
                    &[
                        "remote",
                        "add",
                        "other",
                        "https://github.com/private/other.git",
                    ],
                );
            }
            "unmapped" => {
                f.git.git(
                    &f.git.path,
                    &[
                        "remote",
                        "set-url",
                        "origin",
                        "git@alias:private/hidden.git",
                    ],
                );
            }
            _ => {}
        }
        let endpoint = match mode {
            "missing-anchor" => WorkspaceMcpServer::new(f.api(), f.git.workspace.id.clone())
                .with_caller_agent_id(Some(f.agent.clone()))
                .with_request_context(Arc::new(f.owner.callback())),
            "missing-request" => WorkspaceMcpServer::new(f.api(), f.git.workspace.id.clone())
                .with_caller_agent_id(Some(f.agent.clone())),
            _ => controlled(&f, control.clone()),
        };
        let auth_requests = http.fixture.control.requests.lock().unwrap().len();
        let mut errors = Vec::new();
        for namespace in ["pr", "mr"] {
            for raw in [false, true] {
                let call = expression(namespace, raw);
                let reply = run(
                    &endpoint,
                    &format!(
                        "try {{await {call};}} catch(e) {{return JSON.stringify(e.message);}}"
                    ),
                )
                .await;
                let error = json_result(&reply);
                assert!(error
                    .as_str()
                    .unwrap()
                    .contains(crate::repository_read_source::REFUSAL));
                assert!(!reply.to_string().contains("private/hidden"));
                assert!(!reply.to_string().contains(f.git.path.to_str().unwrap()));
                assert!(!reply.to_string().contains("stored-pat"));
                errors.push(error);
            }
        }
        assert!(errors.windows(2).all(|pair| pair[0] == pair[1]));
        assert!(control.events.lock().unwrap().is_empty());
        assert!(f.auth.service.pr_cache.lock().unwrap().is_empty());
        assert_eq!(http.count(), 0);
        assert_eq!(
            http.fixture.control.requests.lock().unwrap().len(),
            auth_requests
        );
        // Frozen read_source.rs reserves/binds before obtaining the original
        // secret reader or reaching the qualified cache. No successful policy
        // record or replacement provider path is supplied by this test.
    }
}

#[intent_test_macros::daemon_test]
async fn both_aliases_keep_host_and_direct_transfer_retirement_order() {
    for namespace in ["pr", "mr"] {
        for (boundary, after) in [
            (Boundary::HostPromise, false),
            (Boundary::HostPromise, true),
            (Boundary::DirectResponse, false),
            (Boundary::DirectResponse, true),
        ] {
            let http = ReadServer::new().await;
            let f = ActualRead::new(&http).await;
            let control = Arc::new(Control::default());
            let gate = Hold::new();
            *control.hold.lock().unwrap() = Some((boundary, after, gate.clone()));
            let endpoint = controlled(&f, control.clone());
            let code = format!(
                "try {{ await ws.{namespace}.snapshot(4); }} catch(e) {{}} return 'after-read';"
            );
            let task = tokio::spawn(async move { run(&endpoint, &code).await });
            gate.reached().await;
            let count = http.count();
            f.owner.interrupt_requests();
            gate.release.add_permits(1);
            let reply = task.await.unwrap();
            if boundary == Boundary::DirectResponse && after {
                assert!(
                    reply.to_string().contains("after-read"),
                    "admitted output is not recalled: {reply}"
                );
            } else {
                delivery_refused(&reply);
            }
            assert_eq!(http.count(), count, "no retry after retirement");
            assert!(control
                .events
                .lock()
                .unwrap()
                .iter()
                .all(|(_, count)| *count == 2));
        }
    }
}

#[intent_test_macros::daemon_test]
async fn aliases_cannot_discard_credential_or_source_obligations_at_direct_output() {
    use intent_sourcecontrol::gitlab_token::SECRET_ACCOUNT;
    for (change, suffix) in [
        ("source", "return 'constant';"),
        (
            "credential",
            "try { throw Error('caught'); } catch(e) {} return 'constant';",
        ),
    ] {
        for namespace in ["pr", "mr"] {
            let http = ReadServer::new().await;
            let f = ActualRead::new(&http).await;
            let control = Arc::new(Control::default());
            let gate = Hold::new();
            *control.hold.lock().unwrap() = Some((Boundary::DirectResponse, false, gate.clone()));
            let endpoint = controlled(&f, control.clone());
            let code = format!("await ws.workspace.setStatusMessage('completed ordinary effect'); await ws.{namespace}.snapshot(4); {suffix}");
            let task = tokio::spawn(async move { run(&endpoint, &code).await });
            gate.reached().await;
            let count = http.count();
            if change == "source" {
                f.git.git(
                    &f.git.path,
                    &[
                        "remote",
                        "set-url",
                        "origin",
                        "https://gitlab.test/forge/other/project.git",
                    ],
                );
            } else {
                intent_core::with_caller(
                    Caller::Daemon,
                    f.auth
                        .service
                        .settings_update(json!([{"path":SECRET_ACCOUNT,"value":"pat-second"}])),
                )
                .await
                .unwrap();
            }
            gate.release.add_permits(1);
            delivery_refused(&task.await.unwrap());
            assert_eq!(http.count(), count);
            let workspace = f
                .auth
                .service
                .store
                .get_workspace(&f.git.workspace.id)
                .await
                .unwrap();
            assert_eq!(
                workspace.status_message.as_deref(),
                Some("completed ordinary effect")
            );
            assert!(control
                .events
                .lock()
                .unwrap()
                .contains(&(Boundary::DirectResponse, 2)));
        }
    }
}

#[intent_test_macros::daemon_test]
async fn both_aliases_use_original_tcp_response_admission_and_preserve_completed_effects() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    for namespace in ["pr", "mr"] {
        for after in [false, true] {
            let http = ReadServer::new().await;
            let f = ActualRead::new(&http).await;
            let control = Arc::new(Control::default());
            let gate = Hold::new();
            *control.hold.lock().unwrap() = Some((Boundary::TcpResponse, after, gate.clone()));
            let bridge = intent_acp::mcp_bridge::serve_workspace_mcp_tcp(Arc::new(controlled(
                &f,
                control.clone(),
            )))
            .await
            .unwrap();
            let mut socket = tokio::net::TcpStream::connect(bridge.addr()).await.unwrap();
            let code = format!("await ws.workspace.setStatusMessage('completed ordinary effect'); await ws.{namespace}.snapshot(4); return 'constant';");
            let request = crate::repository_read_source::tests::call(&code);
            socket
                .write_all(format!("{request}\n").as_bytes())
                .await
                .unwrap();
            gate.reached().await;
            let count = http.count();
            f.owner.interrupt_requests();
            gate.release.add_permits(1);
            let mut line = String::new();
            tokio::time::timeout(
                Duration::from_secs(10),
                BufReader::new(socket).read_line(&mut line),
            )
            .await
            .unwrap()
            .unwrap();
            let reply: Value = serde_json::from_str(&line).unwrap();
            if after {
                assert!(
                    line.contains("constant"),
                    "already queued output is not recalled"
                );
            } else {
                delivery_refused(&reply);
            }
            assert_eq!(http.count(), count);
            assert_eq!(
                f.auth
                    .service
                    .store
                    .get_workspace(&f.git.workspace.id)
                    .await
                    .unwrap()
                    .status_message
                    .as_deref(),
                Some("completed ordinary effect")
            );
            assert_eq!(
                control
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(kind, _)| *kind == Boundary::TcpResponse)
                    .count(),
                1
            );
            drop(bridge);
        }
    }
}

#[intent_test_macros::daemon_test]
async fn mixed_names_and_raw_dispatch_have_one_sixty_four_record_acquisition_budget() {
    for count in [63, 64] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        let control = Arc::new(Control::default());
        let endpoint = controlled(&f, control.clone());
        let code = format!(
            "for(let i=0;i<{count};i++) {{
                try {{
                    if(i%2) await ws.mr.snapshot(4);
                    else await host({{method:'pr.snapshot',args:{{prNumber:4}}}});
                }} catch(e) {{
                    try {{await ws.pr.snapshot(4);}} catch(e) {{}}
                }}
            }}
            return 'bounded success';"
        );
        let reply = run(&endpoint, &code).await;
        if count == 63 {
            assert!(reply.to_string().contains("bounded success"), "{reply}");
            let records = control.records.lock().unwrap();
            assert_eq!(records.len(), 64);
            assert!(records
                .iter()
                .all(|r| Arc::ptr_eq(&r.request, &records[0].request)));
            assert!(control
                .events
                .lock()
                .unwrap()
                .contains(&(Boundary::DirectResponse, 64)));
        } else {
            delivery_refused(&reply);
        }
        assert_eq!(
            http.count(),
            4,
            "one full read and cache hits; rejected acquisitions never fetch"
        );
        assert_eq!(f.auth.service.pr_cache.lock().unwrap().len(), 1);
    }
}

#[intent_test_macros::daemon_test]
async fn partial_quota_is_identical_for_both_aliases_without_legacy_pause_or_fallback() {
    let mut results = Vec::new();
    for namespace in ["pr", "mr"] {
        let http = ReadServer::new().await;
        let f = ActualRead::new(&http).await;
        no_spill(&f);
        http.status(&format!("{MR}/approvals"), 429);
        let control = Arc::new(Control::default());
        let gate = Hold::new();
        *control.hold.lock().unwrap() = Some((Boundary::HostPromise, true, gate.clone()));
        let endpoint = controlled(&f, control.clone());
        let code = format!(
            "const first=await ws.{namespace}.snapshot(4);
            const errors=[];
            for(const name of ['pr','mr']) {{
                try {{ await ws[name].snapshot(4); }} catch(e) {{ errors.push(e.message); }}
            }}
            return JSON.stringify({{first,errors}});"
        );
        let task = tokio::spawn(async move { run(&endpoint, &code).await });
        gate.reached().await;
        let requests_after_first = http.count();
        // The MR, project policy and approvals request run; the quota observation
        // prevents the later discussions request from reaching HTTP.
        assert_eq!(requests_after_first, 3);
        gate.release.add_permits(1);
        let reply = task.await.unwrap();
        let value = json_result(&reply);
        let first = &value["first"];
        assert_eq!(first["title"], "actual review");
        assert_eq!(first["availability"]["approvals"], "rate-limited");
        assert_eq!(first["reviews"]["approvals"], Value::Null);
        assert_eq!(first["availability"]["checks"], "available");
        assert_eq!(first["availability"]["discussions"], "rate-limited");
        assert!(first.get("pausedUntil").is_none());
        assert!(f.auth.service.sweep_rate_limit_paused_until().is_none());
        // Reusing a partial observation is not authorized by spelling. Both
        // retries retain the actual existing backoff result without another HTTP read.
        let errors = value["errors"].as_array().unwrap();
        assert_eq!(errors.len(), 2);
        assert_eq!(errors[0], errors[1]);
        assert!(errors[0].as_str().unwrap().contains("Backoff"));
        assert_eq!(http.count(), requests_after_first);
        // A partial response does not install a complete cache payload. Its
        // quota remains enforced by the original credential owner; the public
        // snapshot exposes availability, not the internal quota receipt.
        assert_eq!(f.auth.service.pr_cache.lock().unwrap().len(), 1);
        let records = control.records.lock().unwrap();
        assert_eq!(
            records.len(),
            6,
            "each uncached call retains its lazy subread"
        );
        assert!(records
            .iter()
            .all(|r| Arc::ptr_eq(&r.request, &records[0].request)));
        assert!(!reply.to_string().contains("stored-pat"));
        results.push(value);
    }
    assert_eq!(results[0], results[1]);
}
