//! The `git-remote-intent` remote helper. Git runs it for `intent://`
//! remotes with the remote name and URL; it speaks git's `connect` helper
//! protocol on stdin/stdout and carries the session through the node's
//! local git socket (named by [`intent_git::node_git::SOCKET_ENV`]), which
//! forwards it to head over the link.

use std::path::Path;

use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};

use intent_core::{Error, Result};
use intent_git::node_git::{AgentRemote, GitService};

/// Longest request line on the helper socket and on the helper's stdin.
pub(super) const MAX_REQUEST: u64 = 1024;

fn helper_error(detail: &str) -> Error {
    Error::Internal(format!("git-remote-intent: {detail}"))
}

/// Run the helper for `url`, reaching the node through `socket`.
///
/// # Errors
///
/// For a URL that is not an agent remote, an unknown command or service,
/// an unreachable node socket, or head's refusal (its error code).
pub async fn run_git_remote_helper(url: &str, socket: &Path) -> Result<()> {
    let remote = AgentRemote::parse(url)?;
    let mut input = BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    loop {
        let mut command = Vec::new();
        (&mut input)
            .take(MAX_REQUEST + 1)
            .read_until(b'\n', &mut command)
            .await
            .map_err(|_| helper_error("stdin closed"))?;
        if command.len() as u64 > MAX_REQUEST {
            return Err(helper_error("command too long"));
        }
        match command.as_slice() {
            b"capabilities\n" => {
                output
                    .write_all(b"connect\n\n")
                    .await
                    .map_err(|_| helper_error("stdout closed"))?;
                output
                    .flush()
                    .await
                    .map_err(|_| helper_error("stdout closed"))?;
            }
            b"\n" | b"" => return Ok(()),
            line => {
                let service = std::str::from_utf8(line)
                    .ok()
                    .and_then(|line| line.strip_prefix("connect "))
                    .and_then(|line| line.strip_suffix('\n'))
                    .ok_or_else(|| helper_error("unsupported command"))?;
                let service = GitService::parse(service)?;
                return connect(service, &remote, socket, input, output).await;
            }
        }
    }
}

async fn connect(
    service: GitService,
    remote: &AgentRemote,
    socket: &Path,
    mut input: BufReader<tokio::io::Stdin>,
    mut output: tokio::io::Stdout,
) -> Result<()> {
    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .map_err(|_| helper_error("the node git socket is unreachable"))?;
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    write
        .write_all(format!("{} {}\n", service.as_str(), remote.url()).as_bytes())
        .await
        .map_err(|_| helper_error("the node git socket closed"))?;
    let status = read
        .read_u8()
        .await
        .map_err(|_| helper_error("the node git socket closed"))?;
    if status != 0 {
        let mut code = Vec::new();
        let _ = (&mut read)
            .take(MAX_REQUEST)
            .read_until(b'\n', &mut code)
            .await;
        let code = String::from_utf8_lossy(&code);
        let code: String = code
            .trim()
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
            .collect();
        return Err(helper_error(&format!("head refused the session ({code})")));
    }
    output
        .write_all(b"\n")
        .await
        .map_err(|_| helper_error("stdout closed"))?;
    output
        .flush()
        .await
        .map_err(|_| helper_error("stdout closed"))?;
    let upload = async {
        tokio::io::copy(&mut input, &mut write)
            .await
            .map_err(|_| helper_error("stdin closed"))?;
        let _ = write.shutdown().await;
        std::future::pending::<Result<()>>().await
    };
    let download = async {
        tokio::io::copy(&mut read, &mut output)
            .await
            .map_err(|_| helper_error("the node git socket closed"))?;
        output
            .flush()
            .await
            .map_err(|_| helper_error("stdout closed"))
    };
    tokio::select! {
        result = upload => result,
        result = download => result,
    }
}
