use super::*;

const GIB: u64 = 1024 * 1024 * 1024;

fn load(repo: &str, gib: u64, busy: bool, age_secs: u64) -> Load {
    Load {
        repo_id: repo.to_string(),
        bytes: gib * GIB,
        busy,
        evictable: true,
        last_used: Instant::now()
            .checked_sub(Duration::from_secs(age_secs))
            .unwrap(),
    }
}

fn evicted(plan: &[(String, bool)]) -> Vec<&str> {
    plan.iter().map(|(repo, _)| repo.as_str()).collect()
}

#[test]
fn a_model_that_fits_evicts_nothing() {
    let loads = [load("x", 10, false, 5), load("y", 10, true, 1)];
    assert!(plan_evictions(Some(40 * GIB), Some(15 * GIB), &loads).is_empty());
}

#[test]
fn idle_servers_go_first_then_least_recently_used_busy_ones() {
    let loads = [
        load("busy-old", 10, true, 50),
        load("idle-new", 10, false, 1),
        load("idle-old", 10, false, 30),
        load("busy-new", 10, true, 2),
    ];
    // 40 used + 15 needed against 30: free 25 GiB, i.e. three servers.
    let plan = plan_evictions(Some(30 * GIB), Some(15 * GIB), &loads);
    assert_eq!(evicted(&plan), ["idle-old", "idle-new", "busy-old"]);
    assert_eq!(plan[2], ("busy-old".to_string(), true));
}

#[test]
fn a_server_the_daemon_may_not_stop_still_uses_memory() {
    let mut adopted = load("adopted", 20, true, 50);
    adopted.evictable = false;
    let loads = [adopted, load("idle", 5, false, 1)];
    // 25 used + 10 needed against 30: only the idle server can go, and that
    // is not enough, so it goes and the adopted one stays.
    assert_eq!(
        evicted(&plan_evictions(Some(30 * GIB), Some(10 * GIB), &loads)),
        ["idle"]
    );
    assert_eq!(evicted(&plan_evictions(None, None, &loads)), ["idle"]);
}

#[test]
fn unknown_budget_or_size_falls_back_to_one_server() {
    let loads = [load("x", 1, false, 5), load("y", 1, true, 1)];
    assert_eq!(
        evicted(&plan_evictions(None, Some(GIB), &loads)),
        ["x", "y"]
    );
    assert_eq!(
        evicted(&plan_evictions(Some(100 * GIB), None, &loads)),
        ["x", "y"]
    );
    assert!(plan_evictions(None, None, &[]).is_empty());
}

#[test]
fn only_a_shutdown_abort_from_a_pool_stop_reads_as_stopped() {
    let abort = shutting_down_error();
    // The teardown path appends the server output tail to the error.
    let abort_with_tail = intent_core::Error::Internal(format!("{abort}\n--- tail ---\nbye"));
    let crash = intent_core::Error::Internal("unsloth server exited before ready".into());
    let missing = intent_core::Error::InvalidInput("unsloth CLI not found".into());

    assert!(stopped_by_pool(&abort, true, false));
    assert!(stopped_by_pool(&abort_with_tail, true, false));
    assert!(
        !stopped_by_pool(&abort, true, true),
        "a daemon shutdown keeps its message"
    );
    assert!(!stopped_by_pool(&abort, false, false));
    assert!(
        !stopped_by_pool(&crash, true, false),
        "a real failure keeps its error"
    );
    assert!(!stopped_by_pool(&missing, true, false));
}

#[cfg(unix)]
mod lifecycle {
    use super::*;
    use std::path::{Path, PathBuf};

    const A: &str = "unsloth/Qwen3.5-9B-GGUF";
    const B: &str = "unsloth/Qwen3.8-27B-GGUF";
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const KEY: &str = "sk-unsloth-pool-key";

    /// Loopback stand-in for one managed server: 200 with the key, 401 without.
    async fn spawn_stub_http() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    let resp = if req.contains(&format!("Bearer {KEY}")) {
                        "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n{}"
                    } else {
                        "HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n"
                    };
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        port
    }

    /// Stub `unsloth`: `run` logs its argv and stays up; `start opencode`
    /// writes the generated config for the server `UNSLOTH_STUDIO_URL` names,
    /// as the real CLI does.
    fn write_stub_binary(dir: &Path) -> PathBuf {
        let log = dir.join("stub.log");
        let cfg = dir.join(".unsloth/studio/auth/agents/opencode");
        let script = format!(
            r#"#!/bin/sh
echo "$@" >> '{log}'
case "$1" in
  run) exec sleep 300 ;;
  start)
    model=""
    while [ $# -gt 0 ]; do [ "$1" = "--model" ] && model="$2"; shift; done
    mkdir -p '{cfg}'
    cat > '{cfg}/opencode.json' <<EOF
{{"provider":{{"unsloth-studio":{{"options":{{"baseURL":"$UNSLOTH_STUDIO_URL/v1","apiKey":"{KEY}"}},"models":{{"$model":{{"name":"m","limit":{{"context":8192,"output":1024}}}}}}}}}}}}
EOF
  ;;
esac
"#,
            log = log.display(),
            cfg = cfg.display(),
        );
        let path = dir.join("unsloth");
        let mut f = std::fs::File::create(&path).expect("create stub");
        f.write_all(script.as_bytes()).expect("write stub");
        f.set_permissions(std::fs::Permissions::from_mode(0o755))
            .expect("chmod stub");
        path
    }

    struct Fixture {
        pool: UnslothServerPool,
        ports: Vec<u16>,
        _dir: tempfile::TempDir,
    }

    /// A pool whose first server gets the configured port and later ones
    /// the next stub port, with 32 GiB of RAM (a ~21 GiB budget).
    async fn fixture(servers: usize) -> Fixture {
        let dir = crate::test_support::test_tempdir("unsloth-pool-");
        let binary = write_stub_binary(dir.path());
        let mut ports = Vec::new();
        for _ in 0..servers {
            ports.push(spawn_stub_http().await);
        }
        let spare = Arc::new(Mutex::new(ports[1..].to_vec()));
        let config = UnslothConfig {
            resolve_binary: Box::new(move |_| Some(binary.clone())),
            home_dir: Some(dir.path().to_path_buf()),
            port: ports[0],
            server_up_timeout: Duration::from_secs(60),
            model_ready_timeout: Duration::from_secs(60),
            probe_interval: Duration::from_millis(50),
            mint_timeout: Duration::from_secs(60),
            adopt_mint_timeout: Duration::from_secs(60),
            hf_api_base: "http://127.0.0.1:1".to_string(),
            hf_files_timeout: Duration::from_secs(2),
            total_memory_bytes: Box::new(|| Some(32 * GIB)),
            port_is_free: Box::new(|_| true),
            pick_free_port: Box::new(move || {
                let mut spare = spare.lock().unwrap();
                (!spare.is_empty()).then(|| spare.remove(0))
            }),
            mint_lock: Arc::new(TokioMutex::new(())),
        };
        Fixture {
            pool: UnslothServerPool::with_config(config),
            ports,
            _dir: dir,
        }
    }

    fn none() -> HashMap<String, usize> {
        HashMap::new()
    }

    async fn running(pool: &UnslothServerPool) -> Vec<(String, u16)> {
        let mut servers: Vec<_> = pool
            .statuses()
            .await
            .into_iter()
            .map(|s| (s.repo_id, s.port))
            .collect();
        servers.sort();
        servers
    }

    #[tokio::test]
    async fn two_models_run_side_by_side_on_their_own_ports() {
        let fx = fixture(2).await;
        fx.pool.seed_footprint_for_test(A, 4 * GIB);
        fx.pool.seed_footprint_for_test(B, 4 * GIB);

        let a = fx
            .pool
            .ensure_endpoint(A, None, &none(), &|_, _| {})
            .await
            .expect("A starts");
        let b = fx
            .pool
            .ensure_endpoint(B, None, &none(), &|_, _| {})
            .await
            .expect("B starts");

        assert_eq!(a.base_url, format!("http://127.0.0.1:{}/v1", fx.ports[0]));
        assert_eq!(b.base_url, format!("http://127.0.0.1:{}/v1", fx.ports[1]));
        assert_eq!(
            running(&fx.pool).await,
            [(A.to_string(), fx.ports[0]), (B.to_string(), fx.ports[1])]
        );
        let again = fx
            .pool
            .ensure_endpoint(A, None, &none(), &|_, _| {})
            .await
            .expect("A reused");
        assert_eq!(again, a);
        fx.pool.shutdown().await;
        assert!(fx.pool.statuses().await.is_empty());
    }

    #[tokio::test]
    async fn a_model_that_does_not_fit_stops_an_idle_server_first() {
        let fx = fixture(2).await;
        // 12 GiB + 12 GiB footprints against a ~21 GiB budget.
        fx.pool.seed_footprint_for_test(A, 12 * GIB);
        fx.pool.seed_footprint_for_test(B, 12 * GIB);
        fx.pool
            .ensure_endpoint(A, None, &none(), &|_, _| {})
            .await
            .expect("A starts");

        let messages = Arc::new(Mutex::new(Vec::new()));
        let seen = messages.clone();
        fx.pool
            .ensure_endpoint(B, None, &none(), &move |level, m| {
                seen.lock().unwrap().push((level, m));
            })
            .await
            .expect("B starts");

        assert_eq!(running(&fx.pool).await, [(B.to_string(), fx.ports[0])]);
        assert!(
            messages
                .lock()
                .unwrap()
                .iter()
                .all(|(level, _)| *level == StatusLevel::Info),
            "stopping an idle server warns no one"
        );
        fx.pool.shutdown().await;
    }

    #[tokio::test]
    async fn stopping_a_busy_server_warns_before_it_happens() {
        let fx = fixture(2).await;
        fx.pool.seed_footprint_for_test(A, 12 * GIB);
        fx.pool.seed_footprint_for_test(B, 12 * GIB);
        fx.pool
            .ensure_endpoint(A, None, &none(), &|_, _| {})
            .await
            .expect("A starts");
        let attached = HashMap::from([(A.to_string(), 2)]);

        assert_eq!(fx.pool.plan_for(B, &attached).await, [A.to_string()]);
        assert_eq!(running(&fx.pool).await.len(), 1, "planning stops nothing");

        let warnings = Arc::new(Mutex::new(Vec::new()));
        let seen = warnings.clone();
        fx.pool
            .ensure_endpoint(B, None, &attached, &move |level, m| {
                if level == StatusLevel::Warning {
                    seen.lock().unwrap().push(m);
                }
            })
            .await
            .expect("B starts");
        let warnings = warnings.lock().unwrap().clone();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains(A) && warnings[0].contains("2 live agent sessions"));
        fx.pool.shutdown().await;
    }

    #[tokio::test]
    async fn a_running_model_or_a_model_that_fits_plans_no_stop() {
        let fx = fixture(2).await;
        fx.pool.seed_footprint_for_test(A, 4 * GIB);
        fx.pool.seed_footprint_for_test(B, 4 * GIB);
        fx.pool
            .ensure_endpoint(A, None, &none(), &|_, _| {})
            .await
            .expect("A starts");
        let attached = HashMap::from([(A.to_string(), 1)]);
        assert!(fx.pool.plan_for(A, &attached).await.is_empty());
        assert!(fx.pool.plan_for(B, &attached).await.is_empty());
        fx.pool.shutdown().await;
    }

    #[tokio::test]
    async fn a_server_still_starting_keeps_its_entry_and_port() {
        let fx = fixture(2).await;
        fx.pool.seed_footprint_for_test(A, 4 * GIB);
        fx.pool.seed_footprint_for_test(B, 4 * GIB);
        let starting_port = fx.pool.insert_starting_for_test(A);

        // A send-time check must not drop the entry before its server spawns.
        assert!(fx.pool.plan_for(B, &none()).await.is_empty());
        assert!(fx.pool.tracks_for_test(A));

        let b = fx
            .pool
            .ensure_endpoint(B, None, &none(), &|_, _| {})
            .await
            .expect("B starts");
        assert_ne!(b.base_url, format!("http://127.0.0.1:{starting_port}/v1"));
        assert!(fx.pool.tracks_for_test(A), "A is still starting");
        fx.pool.shutdown().await;
    }

    #[tokio::test]
    async fn a_crashed_server_restarts_through_the_memory_check() {
        let fx = fixture(2).await;
        fx.pool.seed_footprint_for_test(A, 4 * GIB);
        fx.pool.seed_footprint_for_test(B, 4 * GIB);
        for repo in [A, B] {
            fx.pool
                .ensure_endpoint(repo, None, &none(), &|_, _| {})
                .await
                .expect("starts");
        }
        let pid = fx
            .pool
            .statuses()
            .await
            .into_iter()
            .find(|status| status.repo_id == A)
            .and_then(|status| status.pid)
            .expect("A runs");
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid.cast_signed()),
            nix::sys::signal::Signal::SIGKILL,
        )
        .expect("kill A");

        // Now A no longer fits beside B: its restart must make room.
        fx.pool.seed_footprint_for_test(A, 12 * GIB);
        fx.pool.seed_footprint_for_test(B, 12 * GIB);
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(30)),
            async {
                while fx.pool.server_is_live_for_test(A) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            },
        )
        .await
        .expect("A's death is observed");

        // Two agents retry A at once: both must go through the memory check.
        let attached = none();
        let (first, second) = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(30)),
            async {
                tokio::join!(
                    fx.pool.ensure_endpoint(A, None, &attached, &|_, _| {}),
                    fx.pool.ensure_endpoint(A, None, &attached, &|_, _| {}),
                )
            },
        )
        .await
        .expect("both restarts finish");
        assert_eq!(first.expect("A restarts"), second.expect("A restarts"));
        assert_eq!(
            running(&fx.pool)
                .await
                .into_iter()
                .map(|(repo, _)| repo)
                .collect::<Vec<_>>(),
            [A.to_string()],
            "B stopped to make room for A, and A runs once"
        );
        assert_eq!(running(&fx.pool).await.len(), 1);
        fx.pool.shutdown().await;
    }

    #[tokio::test]
    async fn stopping_does_not_wait_behind_another_servers_mint() {
        let fx = fixture(1).await;
        fx.pool.seed_footprint_for_test(A, 4 * GIB);
        // Another server's mint holds the lock (e.g. a long first download).
        let held = fx.pool.base.mint_lock.clone().lock_owned().await;
        let pool = Arc::new(fx.pool);
        let starting = pool.clone();
        let start =
            tokio::spawn(
                async move { starting.ensure_endpoint(A, None, &none(), &|_, _| {}).await },
            );
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(30)),
            async {
                while pool.statuses().await.is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            },
        )
        .await
        .expect("A's server spawned and waits to mint");

        let stopped = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(30)),
            pool.stop_all(),
        )
        .await
        .expect("stop_all returns while the mint lock is still held");
        assert!(stopped);
        assert!(start.await.unwrap().is_err(), "the interrupted start fails");
        drop(held);
    }

    #[tokio::test]
    async fn stop_all_stops_every_server() {
        let fx = fixture(2).await;
        fx.pool.seed_footprint_for_test(A, 4 * GIB);
        fx.pool.seed_footprint_for_test(B, 4 * GIB);
        for repo in [A, B] {
            fx.pool
                .ensure_endpoint(repo, None, &none(), &|_, _| {})
                .await
                .expect("starts");
        }
        assert!(fx.pool.stop_all().await);
        assert!(fx.pool.statuses().await.is_empty());
        assert!(!fx.pool.stop_all().await, "nothing left to stop");
    }

    #[tokio::test]
    async fn stopping_one_model_leaves_the_others_running() {
        let fx = fixture(2).await;
        fx.pool.seed_footprint_for_test(A, 4 * GIB);
        fx.pool.seed_footprint_for_test(B, 4 * GIB);
        for repo in [A, B] {
            fx.pool
                .ensure_endpoint(repo, None, &none(), &|_, _| {})
                .await
                .expect("starts");
        }

        assert!(fx.pool.stop_model(A).await);
        assert_eq!(running(&fx.pool).await, [(B.to_string(), fx.ports[1])]);
        assert!(!fx.pool.stop_model(A).await, "A is already stopped");
        assert!(
            !fx.pool.stop_model("unsloth/Unknown-GGUF").await,
            "a model with no server is a no-op"
        );

        fx.pool
            .ensure_endpoint(A, None, &none(), &|_, _| {})
            .await
            .expect("A starts again on demand");
        assert_eq!(running(&fx.pool).await.len(), 2);
        fx.pool.shutdown().await;
    }

    #[tokio::test]
    async fn a_start_evicted_to_make_room_says_its_server_stopped() {
        let fx = fixture(2).await;
        // 12 GiB + 12 GiB footprints against a ~21 GiB budget.
        fx.pool.seed_footprint_for_test(A, 12 * GIB);
        fx.pool.seed_footprint_for_test(B, 12 * GIB);
        let held = fx.pool.base.mint_lock.clone().lock_owned().await;
        let pool = Arc::new(fx.pool);
        let starting = pool.clone();
        let start_a =
            tokio::spawn(
                async move { starting.ensure_endpoint(A, None, &none(), &|_, _| {}).await },
            );
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(30)),
            async {
                while pool.statuses().await.is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            },
        )
        .await
        .expect("A's server spawned and waits to mint");

        let starting = pool.clone();
        let start_b =
            tokio::spawn(
                async move { starting.ensure_endpoint(B, None, &none(), &|_, _| {}).await },
            );
        let err = start_a
            .await
            .unwrap()
            .expect_err("B's start stops A to make room");
        assert!(err.to_string().contains("stopped"), "got: {err}");
        assert!(!err.to_string().contains("shutting down"), "got: {err}");

        drop(held);
        start_b.await.unwrap().expect("B starts");
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn stopping_a_model_that_is_still_starting_says_its_server_stopped() {
        let fx = fixture(2).await;
        fx.pool.seed_footprint_for_test(A, 4 * GIB);
        fx.pool.seed_footprint_for_test(B, 4 * GIB);
        fx.pool
            .ensure_endpoint(B, None, &none(), &|_, _| {})
            .await
            .expect("B starts");
        // A waits to mint behind the lock, like a long first download.
        let held = fx.pool.base.mint_lock.clone().lock_owned().await;
        let pool = Arc::new(fx.pool);
        let starting = pool.clone();
        let start =
            tokio::spawn(
                async move { starting.ensure_endpoint(A, None, &none(), &|_, _| {}).await },
            );
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(30)),
            async {
                while pool.statuses().await.len() < 2 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            },
        )
        .await
        .expect("A's server spawned and waits to mint");

        assert!(
            pool.stop_model(A).await,
            "a start in flight counts as stopped"
        );
        let err = start
            .await
            .unwrap()
            .expect_err("the interrupted start fails");
        let message = err.to_string();
        assert!(!message.contains("shutting down"), "got: {message}");
        assert!(message.contains("stopped"), "got: {message}");
        assert_eq!(running(&pool).await, [(B.to_string(), fx.ports[0])]);

        drop(held);
        pool.ensure_endpoint(A, None, &none(), &|_, _| {})
            .await
            .expect("A starts again");
        pool.shutdown().await;
    }
}
