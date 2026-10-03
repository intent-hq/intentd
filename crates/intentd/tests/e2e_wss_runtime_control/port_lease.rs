//! Coordinate only scenarios that release and reclaim production-selected ports.
//! The lock is shared by nextest processes and worktrees, not by installations
//! inside one scenario. Never unlink it: waiters must keep using the same inode.

use super::*;
use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Write};
use std::net::TcpListener;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::net::UnixStream as BlockingUnixStream;
use tokio::net::UnixListener;

fn open() -> File {
    // SAFETY: geteuid has no preconditions and does not mutate process state.
    let uid = unsafe { libc::geteuid() };
    // Do not use TMPDIR/HOME: individual fixtures and worktrees override them.
    let path = format!("/tmp/intentd-wss-default-port-{uid}.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .expect("open per-user default-port fixture lease");
    let metadata = file.metadata().unwrap();
    assert!(metadata.is_file());
    assert_eq!(metadata.uid(), uid, "lease must belong to this user");
    file
}

pub(super) fn acquire() -> Flock<File> {
    acquire_bounded(open())
}

fn acquire_bounded(mut file: File) -> Flock<File> {
    let deadline = std::time::Instant::now() + common::daemon_startup_timeout();
    loop {
        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lease) => return lease,
            Err((returned, Errno::EWOULDBLOCK)) => {
                file = returned;
                assert!(
                    std::time::Instant::now() < deadline,
                    "fixture lease acquisition timed out"
                );
                // Never retry a bind or a failed scenario assertion.
                // timing-guard: wait for another complete cooperating scenario.
                std::thread::sleep(Duration::from_millis(10));
            }
            Err((_, error)) => panic!("acquire default-port fixture lease: {error}"),
        }
    }
}

pub(super) struct Contender {
    child: GuardedChild,
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: tokio::net::unix::OwnedWriteHalf,
    _dir: tempfile::TempDir,
}

impl Contender {
    pub(super) async fn start(port: u16) -> Self {
        let dir = temp_data_dir();
        let socket = dir.path().join("contender.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "port_lease::contender_process",
                "--nocapture",
            ])
            .env("INTENTD_PORT_CONTENDER_SOCKET", socket)
            .env("INTENTD_PORT_CONTENDER_PORT", port.to_string());
        let child = GuardedChild::spawn(&mut command).unwrap();
        let (stream, _) = timeout(common::daemon_startup_timeout(), listener.accept())
            .await
            .expect("contender connects before existing startup deadline")
            .unwrap();
        let (reader, writer) = stream.into_split();
        let mut contender = Self {
            child,
            reader: BufReader::new(reader),
            writer,
            _dir: dir,
        };
        let acknowledgement = contender.read().await;
        assert!(
            matches!(acknowledgement.as_str(), "blocked" | "bound"),
            "contender must acknowledge exclusion or an actual bind: {acknowledgement}"
        );
        eprintln!(
            "port contender pid={} port={port} initial={acknowledgement}",
            contender.child.id()
        );
        contender
    }

    async fn read(&mut self) -> String {
        let mut line = String::new();
        let count = timeout(
            common::daemon_startup_timeout(),
            self.reader.read_line(&mut line),
        )
        .await
        .expect("contender acknowledgement before existing startup deadline")
        .unwrap();
        assert_ne!(count, 0, "contender exited before acknowledging ownership");
        line.trim().to_owned()
    }

    pub(super) async fn finish(mut self, port: u16) {
        self.writer.write_all(b"recover\n").await.unwrap();
        assert_eq!(self.read().await, "bound-after-release");
        eprintln!(
            "port contender pid={} port={port} bound-after-release",
            self.child.id()
        );
        self.writer.write_all(b"exit\n").await.unwrap();
        assert!(self
            .child
            .wait_with_timeout(common::daemon_startup_timeout())
            .unwrap()
            .expect("contender exits while still holding its lease and port")
            .success());

        // process::exit in the child skips destructors. Both kernel-owned
        // resources must nevertheless be released, not stranded by a failed test.
        let _lease = acquire();
        let _listener = TcpListener::bind(("127.0.0.1", port))
            .expect("process exit releases the actual contender socket");
        eprintln!("port={port} lease-and-socket-released-on-process-exit");
    }
}

#[test]
#[ignore = "owned subprocess driven by the real first-enable regression"]
fn contender_process() {
    let socket = std::env::var_os("INTENTD_PORT_CONTENDER_SOCKET").unwrap();
    let port: u16 = std::env::var("INTENTD_PORT_CONTENDER_PORT")
        .unwrap()
        .parse()
        .unwrap();
    let mut stream = BlockingUnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(common::daemon_startup_timeout()))
        .unwrap();
    stream
        .set_write_timeout(Some(common::daemon_startup_timeout()))
        .unwrap();
    let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
    let initial = match Flock::lock(open(), FlockArg::LockExclusiveNonblock) {
        Ok(lease) => {
            let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
            stream.write_all(b"bound\n").unwrap();
            Ok((lease, listener))
        }
        Err((file, Errno::EWOULDBLOCK)) => {
            stream.write_all(b"blocked\n").unwrap();
            Err(file)
        }
        Err((_, error)) => panic!("contender lease failed: {error}"),
    };
    let mut request = String::new();
    reader.read_line(&mut request).unwrap();
    assert_eq!(request, "recover\n");
    // The parent sends this only after its daemon and lease have been dropped.
    let _resources = match initial {
        Ok(resources) => resources,
        Err(file) => {
            let lease = acquire_bounded(file);
            let listener =
                TcpListener::bind(("127.0.0.1", port)).expect("parent released saved port");
            (lease, listener)
        }
    };
    stream.write_all(b"bound-after-release\n").unwrap();
    request.clear();
    reader.read_line(&mut request).unwrap();
    assert_eq!(request, "exit\n");
    std::process::exit(0);
}
