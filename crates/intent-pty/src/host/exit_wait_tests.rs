//! [`PtyHost::wait_exit`]: the exit is returned only with the output that
//! came before it, and no copy of the terminal can hold the exit forever.

use super::*;
use std::time::Instant;

fn scaled(d: Duration) -> Duration {
    d.mul_f64(intentd_test_support::test_timeout_multiplier())
}

fn tail_bytes(backlog: &OutputChunk, tail: &ExitTail) -> Vec<u8> {
    let mut out = backlog.bytes.clone();
    let mut end = backlog.end_offset;
    for item in &tail.output {
        if let TailOutput::Chunk(chunk) = item {
            assert_eq!(chunk.start_offset, end, "tail chunks are contiguous");
            end = chunk.end_offset;
            out.extend_from_slice(chunk);
        }
    }
    out
}

/// Many fast children at once, so the reap races the reader: every byte
/// must be in the attach backlog plus the tail, and in the sink, when
/// `wait_exit` returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wait_exit_returns_every_byte_before_the_exit() {
    let host = Arc::new(PtyHost::new());
    let mut tasks = Vec::new();
    for i in 0..32 {
        let host = Arc::clone(&host);
        tasks.push(tokio::spawn(async move {
            let marker = format!("wait-exit-last-{i}");
            let mut spec = SpawnSpec::new("s", "sh");
            spec.args = vec!["-c".into(), format!("seq 1 2000; printf {marker}")];
            let seen = Arc::new(Mutex::new(Vec::<u8>::new()));
            let sink = Arc::clone(&seen);
            spec.output_sink = Some(OutputSink::new(move |bytes| {
                sink.lock().unwrap().extend_from_slice(bytes);
            }));
            let id = host.spawn(spec).unwrap();
            let Attachment { backlog, mut live } = host.attach(id).unwrap();

            let tail = host.wait_exit(id, ExitWait::SCRIPT, Some(&mut live)).await;

            let exit = tail.exit.clone().expect("exit status");
            assert!(exit.success, "{marker}: {exit:?}");
            assert!(
                host.output_eof(id).unwrap(),
                "{marker}: returned before EOF"
            );
            let streamed = tail_bytes(&backlog, &tail);
            assert!(streamed.ends_with(marker.as_bytes()), "{marker}: tail lost");
            assert!(
                seen.lock().unwrap().ends_with(marker.as_bytes()),
                "{marker}: sink lost"
            );
            host.kill(id).await;
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
}

/// A background job (immune to the hangup at the shell's exit) keeps its
/// copy of the terminal: the terminal preset
/// leaves it running and returns once the bound has passed.
#[tokio::test]
async fn terminal_wait_is_bounded_by_a_job_holding_the_terminal() {
    let host = PtyHost::new();
    let mut spec = SpawnSpec::new("s", "sh");
    spec.args = vec![
        "-c".into(),
        "trap '' HUP; sleep 120 & echo $!; echo done".into(),
    ];
    let id = host.spawn(spec).unwrap();
    let Attachment { backlog, mut live } = host.attach(id).unwrap();

    let tail = tokio::time::timeout(
        scaled(Duration::from_secs(20)),
        host.wait_exit(id, ExitWait::TERMINAL, Some(&mut live)),
    )
    .await
    .expect("the wait does not last as long as the job");

    assert!(
        !host.output_eof(id).unwrap(),
        "returned at the bound, not at EOF"
    );
    assert!(tail.exit.as_ref().expect("exit status").success);
    let text = String::from_utf8(tail_bytes(&backlog, &tail)).unwrap();
    assert!(text.contains("done"), "{text:?}");
    let job: u32 = text.lines().next().unwrap().trim().parse().unwrap();
    assert!(process_exists(job), "the terminal preset ends no job");
    host.kill(id).await;
}

/// The script preset ends the group's stragglers first, so the output
/// reaches EOF and no process outlives the run.
#[tokio::test]
async fn script_wait_ends_the_group_then_reaches_eof() {
    let host = PtyHost::new();
    let mut spec = SpawnSpec::new("s", "sh");
    spec.args = vec!["-c".into(), "trap '' HUP; sleep 30 & echo $!".into()];
    let id = host.spawn(spec).unwrap();
    let Attachment { backlog, mut live } = host.attach(id).unwrap();

    let tail = host.wait_exit(id, ExitWait::SCRIPT, Some(&mut live)).await;

    assert!(tail.exit.is_some());
    assert!(host.output_eof(id).unwrap());
    let text = String::from_utf8(tail_bytes(&backlog, &tail)).unwrap();
    let job: u32 = text.trim().parse().unwrap();
    assert!(process_gone(job).await, "straggler {job} survived the wait");
    host.kill(id).await;
}

/// A kill while the wait is under way ends it with the latched exit; an id
/// the host never had ends it at once with no exit.
#[tokio::test]
async fn wait_exit_ends_on_kill_and_on_an_unknown_id() {
    let host = Arc::new(PtyHost::new());
    let mut spec = SpawnSpec::new("s", "sleep");
    spec.args = vec!["30".into()];
    let id = host.spawn(spec).unwrap();
    let waiter = {
        let host = Arc::clone(&host);
        tokio::spawn(async move { host.wait_exit(id, ExitWait::SCRIPT, None).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    host.kill(id).await;
    let tail = tokio::time::timeout(scaled(Duration::from_secs(10)), waiter)
        .await
        .expect("wait_exit ends after a kill")
        .unwrap();
    assert!(tail.exit.is_some_and(|exit| !exit.success));

    let gone = host.wait_exit(id, ExitWait::SCRIPT, None).await;
    assert!(gone.exit.is_none() && gone.output.is_empty());
}

/// A reader that panics still ends the output, so even an unbounded wait
/// ends with it.
#[tokio::test]
async fn a_panicking_reader_still_ends_the_output() {
    let host = PtyHost::new();
    let mut spec = SpawnSpec::new("s", "echo");
    spec.args = vec!["boom".into()];
    spec.output_sink = Some(OutputSink::new(|_| panic!("test: the sink panicked")));
    let id = host.spawn(spec).unwrap();
    let until_eof = ExitWait {
        output_eof: None,
        reap_group: true,
    };
    let tail = tokio::time::timeout(
        scaled(Duration::from_secs(10)),
        host.wait_exit(id, until_eof, None),
    )
    .await
    .expect("the wait ends after a reader panic");
    assert!(tail.exit.is_some());
    assert!(host.output_eof(id).unwrap());
    host.kill(id).await;
}

fn process_exists(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    !matches!(
        kill(Pid::from_raw(pid.cast_signed()), None),
        Err(nix::errno::Errno::ESRCH)
    )
}

/// The orphaned job is reaped by init, not by this process: poll for it.
async fn process_gone(pid: u32) -> bool {
    let deadline = Instant::now() + scaled(Duration::from_secs(5));
    while process_exists(pid) {
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    true
}
