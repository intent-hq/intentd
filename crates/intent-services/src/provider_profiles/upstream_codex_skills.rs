use super::*;
use tokio::io::{AsyncReadExt, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout};

struct ModelFixture {
    url: String,
    requests: tokio::sync::mpsc::UnboundedReceiver<Value>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for ModelFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl ModelFixture {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let (tx, requests) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let mut turn = 0;
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let mut socket = BufReader::new(socket);
                let mut line = String::new();
                socket.read_line(&mut line).await.unwrap();
                assert!(line.starts_with("POST /v1/responses "), "{line}");
                let mut length = None;
                loop {
                    line.clear();
                    socket.read_line(&mut line).await.unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':') {
                        if name.eq_ignore_ascii_case("content-length") {
                            length = Some(value.trim().parse::<usize>().unwrap());
                        }
                    }
                }
                let length = length.expect("Responses request needs a bounded body");
                assert!(length < 4 * 1024 * 1024);
                let mut body = vec![0; length];
                socket.read_exact(&mut body).await.unwrap();
                tx.send(serde_json::from_slice(&body).unwrap()).unwrap();
                turn += 1;
                let message = json!({"id":format!("message_{turn}"),"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"fixture-history-sentinel","annotations":[]}]});
                let events = [
                    json!({"type":"response.created","response":{"id":format!("response_{turn}"),"status":"in_progress","output":[]}}),
                    json!({"type":"response.output_item.added","output_index":0,"item":{"id":format!("message_{turn}"),"type":"message","role":"assistant","content":[]}}),
                    json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"fixture-history-sentinel"}),
                    json!({"type":"response.output_item.done","output_index":0,"item":message.clone()}),
                    json!({"type":"response.completed","response":{"id":format!("response_{turn}"),"status":"completed","output":[message],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}),
                ];
                let mut body = String::new();
                for event in events {
                    use std::fmt::Write;
                    writeln!(
                        body,
                        "event: {}\ndata: {event}\n",
                        event["type"].as_str().unwrap()
                    )
                    .unwrap();
                }
                socket
                    .write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes())
                    .await
                    .unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        Self {
            url,
            requests,
            server,
        }
    }

    async fn capture(&mut self) -> Value {
        tokio::time::timeout(std::time::Duration::from_secs(15), self.requests.recv())
            .await
            .expect("Codex did not call the local model endpoint")
            .unwrap()
    }
}

struct AdapterFixture {
    child: Child,
    _group: ProcessGroup,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
    next_id: u64,
}

impl AdapterFixture {
    async fn start(profile: &ProviderLaunchProfile, home: &Path, cwd: &Path) -> Self {
        let mut command = tokio::process::Command::new("node");
        command
            .arg(
                std::env::var("INTENTD_PROFILE_CODEX_ADAPTER_JS")
                    .expect("set installed codex-acp dist/index.js"),
            )
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("HOME", home)
            .env(
                "CODEX_PATH",
                std::env::var("INTENTD_PROFILE_CODEX_BIN").expect("set installed Codex binary"),
            )
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .process_group(0);
        profile.apply_to_command(&mut command);
        let mut child = command.spawn().unwrap();
        let group = ProcessGroup(i32::try_from(child.id().unwrap()).unwrap());
        let stdin = child.stdin.take().unwrap();
        let lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut fixture = Self {
            child,
            _group: group,
            stdin,
            lines,
            next_id: 0,
        };
        fixture.call("initialize", json!({"protocolVersion":1,"clientInfo":{"name":"intent-skills-test","version":"1"},"clientCapabilities":{}})).await;
        fixture
    }

    async fn call(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.stdin
            .write_all(
                format!(
                    "{}\n",
                    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                let line = self
                    .lines
                    .next_line()
                    .await
                    .unwrap()
                    .expect("adapter exited");
                let value: Value = serde_json::from_str(&line).unwrap();
                if value["id"] == id {
                    assert!(value.get("error").is_none(), "{method}: {value}");
                    return value["result"].clone();
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{method} timed out"))
    }

    async fn prompt(&mut self, session: &str, text: String) {
        let result = self
            .call(
                "session/prompt",
                json!({"sessionId":session,"prompt":[{"type":"text","text":text}]}),
            )
            .await;
        assert_eq!(result["stopReason"], "end_turn");
    }

    async fn stop(mut self) {
        self.child.kill().await.unwrap();
        self.child.wait().await.unwrap();
    }
}

fn assert_catalog_request(request: &Value, skills: &[crate::skills::SkillMetadata]) {
    let input = request["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|item| item["content"].as_array())
        .flatten()
        .filter_map(|content| content["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(input.contains(&crate::skills::build_skills_catalog(skills)));
    for skill in skills {
        assert!(Path::new(&skill.location).is_file());
    }
    assert!(
        input.contains("skill-creator"),
        "bundled system skills missing"
    );
    assert!(
        input.contains("skill-installer"),
        "bundled system skills missing"
    );
}

#[tokio::test]
#[ignore = "requires INTENTD_PROFILE_CODEX_ADAPTER_JS and INTENTD_PROFILE_CODEX_BIN; isolated local model endpoint"]
async fn upstream_codex_personal_skills_reach_fresh_and_persisted_context() {
    let dir = crate::test_support::test_tempdir("profile-codex-skill-context");
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    let assistant = dir.path().join("assistant");
    std::fs::create_dir_all(&assistant).unwrap();
    for name in ["find-skills", "ios-device-build"] {
        write(&home.join(format!(".agents/skills/{name}/SKILL.md")), &format!("---\nname: {name}\ndescription: Personal {name} fixture\n---\nUse this fixture skill.\n"));
    }
    write(&project.join(".agents/skills/repo-fixture/SKILL.md"), "---\nname: repo-fixture\ndescription: Repository fixture skill\n---\nUse this fixture skill.\n");
    let mut model = ModelFixture::start().await;
    write(&home.join(".codex/config.toml"), &format!("model_provider='fixture'\nmodel='gpt-5.1-codex'\n[model_providers.fixture]\nname='Local recording fixture'\nbase_url='{}'\nwire_api='responses'\nrequires_openai_auth=false\n", model.url));
    let mcp = NormalizedMcpServers::new();
    let personal_home = std::env::var_os("INTENTD_PROFILE_PERSONAL_SKILLS_HOME").map(PathBuf::from);
    let routing_home = home.join(".codex");
    let mut cases = vec![
        ("assistant", &assistant, None, &home),
        ("project", &project, Some(project.as_path()), &home),
    ];
    if let Some(personal_home) = personal_home.as_ref() {
        cases.extend([
            ("personal-assistant", &assistant, None, personal_home),
            (
                "personal-project",
                &project,
                Some(project.as_path()),
                personal_home,
            ),
        ]);
    }
    for (identity, cwd, workspace, skill_home) in cases {
        let skills = crate::skills::discover_skills_sync(workspace, Some(skill_home.clone()));
        for name in ["find-skills", "ios-device-build"] {
            assert!(skills.iter().any(|s| s.name == name && s.scope == "user"));
        }
        assert_eq!(
            skills.iter().any(|s| s.name == "repo-fixture"),
            workspace.is_some()
        );
        let originals: Vec<_> = skills
            .iter()
            .map(|skill| (&skill.location, std::fs::read(&skill.location).unwrap()))
            .collect();
        let catalog = crate::skills::build_skills_catalog(&skills);
        let mut request = req(dir.path(), skill_home, cwd, "codex", &mcp);
        request.provider_home = Some(&routing_home);
        request.purpose = LaunchPurpose::Persistent;
        request.persistent_identity = Some(identity);
        let profile = prepare_provider_profile(request).unwrap();
        let mut adapter = AdapterFixture::start(&profile, skill_home, cwd).await;
        let session = adapter
            .call("session/new", json!({"cwd":cwd,"mcpServers":[]}))
            .await["sessionId"]
            .as_str()
            .unwrap()
            .to_owned();
        let initial = crate::harness::latest().first_turn_prepend_block(&catalog);
        adapter
            .prompt(&session, format!("{initial}\n\nFirst fixture turn."))
            .await;
        assert_catalog_request(&model.capture().await, &skills);
        adapter.stop().await;

        if skill_home == &home {
            write(&home.join(".agents/skills/find-skills/SKILL.md"), &format!("---\nname: find-skills\ndescription: Refreshed personal skill for {identity}\n---\nUse this fixture skill.\n"));
        }
        let refreshed = crate::skills::discover_skills_sync(workspace, Some(skill_home.clone()));
        let catalog = crate::skills::build_skills_catalog(&refreshed);

        let mut request = req(dir.path(), skill_home, cwd, "codex", &mcp);
        request.provider_home = Some(&routing_home);
        request.purpose = LaunchPurpose::Persistent;
        request.persistent_identity = Some(identity);
        request.resume = true;
        let profile = prepare_provider_profile(request).unwrap();
        let mut adapter = AdapterFixture::start(&profile, skill_home, cwd).await;
        adapter
            .call(
                "session/load",
                json!({"sessionId":session,"cwd":cwd,"mcpServers":[]}),
            )
            .await;
        let update = crate::rules::skill_catalog_update(&catalog);
        adapter
            .prompt(&session, format!("{update}\n\nResumed fixture turn."))
            .await;
        let captured = model.capture().await;
        assert_catalog_request(&captured, &refreshed);
        assert!(
            captured["input"]
                .to_string()
                .contains("fixture-history-sentinel"),
            "persisted assistant history missing"
        );
        adapter.stop().await;
        if skill_home != &home {
            for (path, bytes) in originals {
                assert_eq!(
                    std::fs::read(path).unwrap(),
                    bytes,
                    "user skill changed: {path}"
                );
            }
        }
    }
}
