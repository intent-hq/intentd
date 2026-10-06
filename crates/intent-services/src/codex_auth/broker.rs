//! The daemon owns refresh work; adapter trees only own cancellable RPC clients.
use super::*;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};

/// Closing this lease stops admission, never an already accepted native read.
/// No Debug implementation: the lease is part of the private auth transport.
pub struct OwnerLease {
    _input: ChildStdin,
    directory: tempfile::TempDir,
}

impl OwnerLease {
    #[must_use]
    pub fn socket(&self) -> PathBuf {
        self.directory.path().join("auth.sock")
    }
}

/// Start from the daemon, before the adapter exists. The child is consequently
/// outside both its process group and its descendant-sweep ownership tree.
///
/// # Errors
/// Returns a credential-free error if private transport or process setup fails.
pub fn start_owner(
    runtime: &Path,
    native: &Path,
    user_home: &Path,
    helper: &Path,
    context: &Command,
) -> std::result::Result<OwnerLease, String> {
    let handle = tokio::runtime::Handle::try_current().map_err(|_| CONTRACT_ERROR.to_owned())?;
    let directory = tempfile::Builder::new()
        .prefix("intent-codex-authority-")
        .tempdir()
        .map_err(|_| CONTRACT_ERROR.to_owned())?;
    let mut command = Command::new(helper);
    command.args([
        "provider",
        "codex-auth-owner",
        "--runtime",
        &runtime.to_string_lossy(),
        "--native-home",
        &native.to_string_lossy(),
        "--user-home",
        &user_home.to_string_lossy(),
        "--socket",
        &directory.path().join("auth.sock").to_string_lossy(),
    ]);
    for (key, value) in context.as_std().get_envs() {
        if let Some(value) = value {
            command.env(key, value);
        } else {
            command.env_remove(key);
        }
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0)
        // Daemon runtime shutdown must close the lease, not kill a refresh.
        .kill_on_drop(false)
        .spawn()
        .map_err(|_| CONTRACT_ERROR.to_owned())?;
    let input = child.stdin.take().ok_or(CONTRACT_ERROR.to_owned())?;
    handle.spawn(intent_core::with_caller(
        intent_core::Caller::Daemon,
        async move {
            let _ = child.wait().await;
        },
    ));
    Ok(OwnerLease {
        _input: input,
        directory,
    })
}

async fn serve(authority: Authority, stream: UnixStream) {
    let (read, mut write) = stream.into_split();
    let mut frames = Frames::new(BufReader::new(read));
    frames.limit = Some(AUTH_FRAME_LIMIT);
    // No credentials can be consumed until a complete request was admitted.
    let Ok(Ok(Some(request))) = tokio::time::timeout(TIMEOUT, frames.next()).await else {
        return;
    };
    let previous = request["previous"].as_str();
    // Once admitted, client EOF, lease closure and caller deadlines do not
    // cancel native I/O. Only native completion/error releases its OS lock.
    let result = authority.read_inner(previous).await;
    let response = match result {
        Ok(Some(credentials)) => json!({"status": {
            "requiresOpenaiAuth": true, "authMethod": credentials.mode,
            "authToken": credentials.token,
        }}),
        Ok(None) => json!({"status": {"requiresOpenaiAuth": false}}),
        Err(error) => json!({"error": error}),
    };
    // A disconnected caller is harmless now: native persistence has finished.
    let _ = tokio::time::timeout(TIMEOUT, write_frame(&mut write, &response)).await;
}

/// Serve native reads until the daemon lease closes, then drain accepted work.
///
/// # Errors
/// Returns a credential-free error if the private listener cannot be opened.
pub async fn run_owner(
    runtime: PathBuf,
    native_home: PathBuf,
    user_home: PathBuf,
    socket: PathBuf,
) -> std::result::Result<(), String> {
    run_owner_with_lease(runtime, native_home, user_home, socket, tokio::io::stdin()).await
}

pub(super) async fn run_owner_with_lease(
    runtime: PathBuf,
    native_home: PathBuf,
    user_home: PathBuf,
    socket: PathBuf,
    mut lease: impl tokio::io::AsyncRead + Unpin,
) -> std::result::Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let listener = UnixListener::bind(&socket).map_err(|_| CONTRACT_ERROR.to_owned())?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|_| CONTRACT_ERROR.to_owned())?;
    let authority = Authority {
        runtime,
        home: native_home,
        user_home,
        socket: None,
    };
    let mut byte = [0u8];
    let mut requests = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = lease.read(&mut byte) => break,
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { break };
                requests.spawn(serve(authority.clone(), stream));
            }
            _ = requests.join_next(), if !requests.is_empty() => {}
        }
    }
    drop(listener);
    while requests.join_next().await.is_some() {}
    let _ = std::fs::remove_file(socket);
    Ok(())
}

pub(super) async fn read(socket: &Path, previous: Option<&str>) -> Result<Option<Credentials>> {
    let result = async {
        // The daemon launches the broker before the adapter, but it may still
        // be binding its private socket when the first initialize arrives.
        let stream = loop {
            match UnixStream::connect(socket).await {
                Ok(stream) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => return Err(CONTRACT_ERROR),
            }
        };
        let (read, mut write) = stream.into_split();
        write_frame(&mut write, &json!({"previous": previous})).await?;
        let mut frames = Frames::new(BufReader::new(read));
        frames.limit = Some(AUTH_FRAME_LIMIT);
        let response = frames.next().await?.ok_or(CONTRACT_ERROR)?;
        if let Some(error) = response["error"].as_str() {
            return Err(match error {
                AUTH_ERROR => AUTH_ERROR,
                BUSY_ERROR => BUSY_ERROR,
                POLICY_ERROR => POLICY_ERROR,
                ACCOUNT_ERROR => ACCOUNT_ERROR,
                _ => CONTRACT_ERROR,
            });
        }
        Credentials::from_status(&response["status"])
    };
    tokio::time::timeout(TIMEOUT, result)
        .await
        .map_err(|_| BUSY_ERROR)?
}
