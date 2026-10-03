//! Reproduce the renderer capability probe against real connection-bound events.
use super::*;
use tokio::io::AsyncBufReadExt;

#[allow(clippy::large_enum_variant)]
enum ProbeSocket {
    Wss(Socket),
    #[cfg(unix)]
    Uds(tokio::io::BufReader<tokio::net::UnixStream>),
}
impl ProbeSocket {
    async fn send(&mut self, value: Value) {
        match self {
            Self::Wss(socket) => socket
                .send(Message::Text(value.to_string().into()))
                .await
                .unwrap(),
            #[cfg(unix)]
            Self::Uds(socket) => {
                socket
                    .get_mut()
                    .write_all(format!("{value}\n").as_bytes())
                    .await
                    .unwrap();
            }
        }
    }
    async fn recv(&mut self) -> Value {
        match self {
            Self::Wss(socket) => loop {
                match socket.next().await.unwrap().unwrap() {
                    Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                    Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await.unwrap(),
                    other => panic!("unexpected frame {other:?}"),
                }
            },
            #[cfg(unix)]
            Self::Uds(socket) => {
                let mut line = String::new();
                assert_ne!(socket.read_line(&mut line).await.unwrap(), 0);
                serde_json::from_str(&line).unwrap()
            }
        }
    }
    async fn observe(&mut self, frame: Value, observed: &mut Vec<Value>) {
        if frame["method"] == "desktop.control" {
            let params = &frame["params"];
            let result = match params["operation"].as_str().unwrap() {
                "prepare" => {
                    json!({"computerId":"probe-computer","computerName":"Probe desktop","platform":"macos"})
                }
                "endControl" => json!({"sessionId":params["sessionId"],"ended":true}),
                other => panic!("unexpected native operation {other}"),
            };
            self.send(json!({"jsonrpc":"2.0","id":frame["id"],"result":result}))
                .await;
        }
        observed.push(frame);
    }
    async fn rpc(&mut self, method: &str, params: Value, observed: &mut Vec<Value>) -> Value {
        self.send(json!({"jsonrpc":"2.0","id":9123,"method":method,"params":params}))
            .await;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let frame = self.recv().await;
                if frame["id"] == 9123 {
                    assert!(frame.get("error").is_none(), "{method}: {frame}");
                    return frame["result"].clone();
                }
                self.observe(frame, observed).await;
            }
        })
        .await
        .unwrap()
    }
    async fn agent<F: Future>(&mut self, future: F, observed: &mut Vec<Value>) -> F::Output {
        tokio::pin!(future);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    result = &mut future => return result,
                    frame = self.recv() => self.observe(frame, observed).await,
                }
            }
        })
        .await
        .unwrap()
    }
    async fn barrier(&mut self, srv: &Server, ws: &WorkspaceId, observed: &mut Vec<Value>) {
        // Same subscription FIFO, but no epoch restriction: receiving this proves
        // the preceding permission event has reached the forwarding decision.
        let principal = srv.store.get_primary_principal().await.unwrap();
        let marker = intent_core::AgentId::new().to_string();
        srv.bus
            .publish(&intent_store::NewEvent {
                workspace_id: ws.clone(),
                timestamp: intent_core::now_iso(),
                event_type: "desktop:session-changed".into(),
                actor: Default::default(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: Some(
                    json!({"desktopPrincipalId":principal.id,"desktopConnectionEpoch":null}),
                ),
                data: json!({"marker":marker}),
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let frame = self.recv().await;
                if frame["params"]["event"]["data"]["marker"] == marker {
                    break;
                }
                self.observe(frame, observed).await;
            }
        })
        .await
        .expect("subscription barrier must arrive");
    }
}

async fn reproduce(uds: bool) {
    let (srv, services) = super::super::authenticated_devices::start_roster().await;
    let ws = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws))
        .await
        .unwrap();
    srv.registry
        .apply(&[
            ("model.defaultProvider".into(), json!("auggie")),
            ("providers.paths".into(), json!({"auggie":"/bin/sh"})),
        ])
        .unwrap();
    let principal = srv.store.get_primary_principal().await.unwrap();
    let created = intent_core::with_caller(
        Caller::Wire {
            principal_id: principal.id,
            host_role: HostRole::Owner,
        },
        services.agent_create(
            ws.clone(),
            Some("Probe agent".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        ),
    )
    .await
    .unwrap();
    let agent = AgentId::from(created["agent"]["id"].as_str().unwrap());
    let mut uds_task = None;
    let mut socket = if uds {
        #[cfg(unix)]
        {
            let path = srv.dir.path().join("desktop-probe.sock");
            let api = srv.api.clone();
            let bus = srv.bus.clone();
            let registry = srv.reverse_registry.clone();
            let socket_path = path.clone();
            uds_task = Some(tokio::spawn(async move {
                intent_transport::serve_uds_with_reverse(
                    api,
                    bus,
                    &socket_path,
                    None,
                    None,
                    registry,
                    intent_transport::RpcLimiter::unlimited(),
                    std::future::pending::<()>(),
                )
                .await
                .unwrap();
            }));
            let stream = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Ok(stream) = tokio::net::UnixStream::connect(&path).await {
                        break stream;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            ProbeSocket::Uds(tokio::io::BufReader::new(stream))
        }
        #[cfg(not(unix))]
        panic!("UDS unavailable");
    } else {
        let url = format!("wss://localhost:{}/ws?token={TOKEN}", srv.port);
        ProbeSocket::Wss(common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await)
    };
    let caller = Caller::Agent {
        agent_id: agent.clone(),
    };
    let hello =
        json!({"clientId":"probe-client","capabilities":{"browserExec":true,"desktopControl":1}});
    let scope = json!({"workspaceId":ws,"agentId":agent});
    let mut observed = Vec::new();
    socket
        .rpc("client.hello", hello.clone(), &mut observed)
        .await;
    socket
        .rpc(
            "events.subscribe",
            json!({"workspaceId":ws,"eventTypes":["desktop:*"]}),
            &mut observed,
        )
        .await;
    // Direct getState is the positive control: it must not renegotiate identity.
    socket
        .rpc("desktop.getState", scope.clone(), &mut observed)
        .await;
    let before = socket
        .agent(
            intent_core::with_caller(
                caller.clone(),
                services.desktop_agent_call(ws.clone(), "startControl".into(), json!({})),
            ),
            &mut observed,
        )
        .await
        .unwrap();
    socket.barrier(&srv, &ws, &mut observed).await;
    let delivered = |frames: &[Value], request: &Value| {
        frames.iter().any(|f| {
            f["params"]["event"]["type"] == "desktop:permission-requested"
                && f["params"]["event"]["data"]["requestId"] == *request
        })
    };
    assert!(
        delivered(&observed, &before["requestId"]),
        "positive-control prompt"
    );
    assert_eq!(
        socket
            .rpc("desktop.getState", scope.clone(), &mut observed)
            .await["state"]["requestId"],
        before["requestId"]
    );
    socket
        .agent(
            intent_core::with_caller(
                caller.clone(),
                services.desktop_agent_call(ws.clone(), "endControl".into(), json!({})),
            ),
            &mut observed,
        )
        .await
        .unwrap();
    // Actual FE capability-probe order: subscription A, then hello B + getState.
    socket
        .rpc("client.hello", hello.clone(), &mut observed)
        .await;
    socket
        .rpc("desktop.getState", scope.clone(), &mut observed)
        .await;
    let pending = socket
        .agent(
            intent_core::with_caller(
                caller.clone(),
                services.desktop_agent_call(ws.clone(), "startControl".into(), json!({})),
            ),
            &mut observed,
        )
        .await
        .unwrap();
    assert_eq!(pending["status"], "pending_permission");
    socket.barrier(&srv, &ws, &mut observed).await;
    let prompt_delivered = delivered(&observed, &pending["requestId"]);
    // A later state read repeats the probe and invalidates the pending B request.
    socket.rpc("client.hello", hello, &mut observed).await;
    let after = socket.rpc("desktop.getState", scope, &mut observed).await;
    eprintln!("transport={} baseline_prompt=true probe_prompt={prompt_delivered} pending_request={} state_after_second_probe={}",if uds {"UDS"}else{"WSS"},pending["requestId"],after["state"]);
    if let Some(task) = uds_task {
        task.abort();
    }
    srv.ws.stop().await;
    assert!(prompt_delivered,"capability probe lost the consent event on the existing subscription; after second probe: {}",after["state"]);
    assert_eq!(
        after["state"]["requestId"], pending["requestId"],
        "capability probe invalidated pending consent"
    );
}
#[tokio::test]
async fn desktop_capability_probe_consent_wss() {
    reproduce(false).await;
}
#[cfg(unix)]
#[tokio::test]
async fn desktop_capability_probe_consent_uds() {
    reproduce(true).await;
}
