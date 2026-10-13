//! Bound local endpoint retained across the restricted-to-ready handoff.
#[cfg(windows)]
use super::pipe_name_for_socket_path;
#[cfg(any(unix, windows))]
use super::{
    handle_connection, read_line_bounded, sleep_unless_shutdown, AcceptBackoff, AcceptFailure,
    BoundedLine,
};
use crate::startup::StartupDispatch;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{future::Future, path::Path};
#[cfg(any(unix, windows))]
use tokio::io::{AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::UnixListener;

/// Owns the original endpoint through startup and normal service.
pub struct StartupLocalListener {
    #[cfg(unix)]
    listener: UnixListener,
    #[cfg(windows)]
    listener: tokio::net::windows::named_pipe::NamedPipeServer,
    path: std::path::PathBuf,
}

impl StartupLocalListener {
    /// Bind the configured local endpoint with the ordinary permissions.
    ///
    /// # Errors
    /// Returns bind or permission errors. Caller must hold instance ownership.
    pub fn bind(path: &Path) -> std::io::Result<Self> {
        #[cfg(unix)]
        {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            let listener = UnixListener::bind(path)?;
            let result = Self {
                listener,
                path: path.to_owned(),
            };
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
            Ok(result)
        }
        #[cfg(windows)]
        {
            let listener = tokio::net::windows::named_pipe::ServerOptions::new()
                .first_pipe_instance(true)
                .create(pipe_name_for_socket_path(path)?)?;
            Ok(Self {
                listener,
                path: path.to_owned(),
            })
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = path;
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "local transport unavailable",
            ))
        }
    }

    /// Serve both restricted and fresh ready connections on this bound endpoint.
    ///
    /// # Errors
    /// Returns fatal local transport errors.
    #[cfg(any(unix, windows))]
    pub async fn serve<F>(mut self, dispatch: StartupDispatch, shutdown: F) -> std::io::Result<()>
    where
        F: Future<Output = ()>,
    {
        let read_listener = crate::context::ReadListenerGuard::default();
        tokio::pin!(shutdown);
        let mut backoff = AcceptBackoff::default();
        let mut bootstrap = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                () = &mut shutdown => break,
                Some(_) = bootstrap.join_next(), if !bootstrap.is_empty() => {},
                accepted = self.accept() => match accepted {
                    Ok(stream) => {
                        backoff.on_success();
                        let ready = dispatch.ready().cloned();
                        let dispatch = dispatch.clone();
                        let owners = read_listener.0.clone();
                        if let Some(ready) = ready {
                            tokio::spawn(async move {
                                let (r,w) = tokio::io::split(stream);
                                let _ = handle_connection(r,w,ready.api,ready.bus,ready.control,ready.server_pairing_info,ready.reverse_registry,ready.rpc_limiter,owners).await;
                            });
                        } else {
                            bootstrap.spawn(async move {
                                let _ = bootstrap_connection(stream, dispatch).await;
                            });
                        }
                    }
                    Err(e) => match backoff.on_error(&e) {
                        AcceptFailure::Backoff { delay, .. } => if sleep_unless_shutdown(delay, &mut shutdown).await { break; },
                        AcceptFailure::Other => return Err(e),
                    }
                }
            }
        }
        bootstrap.abort_all();
        while bootstrap.join_next().await.is_some() {}
        Ok(())
    }

    /// Unsupported targets have no local transport.
    ///
    /// # Errors
    /// Always returns an unsupported error.
    #[cfg(not(any(unix, windows)))]
    pub async fn serve<F>(self, _dispatch: StartupDispatch, _shutdown: F) -> std::io::Result<()>
    where
        F: Future<Output = ()>,
    {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "local transport unavailable",
        ))
    }

    #[cfg(unix)]
    async fn accept(&mut self) -> std::io::Result<tokio::net::UnixStream> {
        self.listener.accept().await.map(|(stream, _)| stream)
    }

    #[cfg(windows)]
    async fn accept(
        &mut self,
    ) -> std::io::Result<tokio::net::windows::named_pipe::NamedPipeServer> {
        self.listener.connect().await?;
        let next = tokio::net::windows::named_pipe::ServerOptions::new()
            .create(pipe_name_for_socket_path(&self.path)?)?;
        Ok(std::mem::replace(&mut self.listener, next))
    }
}

impl Drop for StartupLocalListener {
    fn drop(&mut self) {
        #[cfg(unix)]
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(any(unix, windows))]
async fn bootstrap_connection<S>(stream: S, dispatch: StartupDispatch) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (r, mut w) = tokio::io::split(stream);
    let mut reader = BufReader::new(r);
    let mut line = Vec::new();
    let serving = async {
        loop {
            line.clear();
            match read_line_bounded(&mut reader, &mut line, crate::MAX_INBOUND_MESSAGE_BYTES)
                .await?
            {
                BoundedLine::Line => {
                    let raw = std::str::from_utf8(&line)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                    if let Some(frame) = crate::startup::response(dispatch.status(), raw) {
                        if frame.len() > crate::MAX_OUTBOUND_MESSAGE_BYTES {
                            return Ok(());
                        }
                        w.write_all(frame.as_bytes()).await?;
                        w.write_all(b"\n").await?;
                        w.flush().await?;
                    }
                }
                BoundedLine::Eof | BoundedLine::TooLong => return Ok(()),
            }
        }
    };
    tokio::select! {
        biased;
        () = dispatch.retired() => Ok(()),
        result = serving => result,
    }
}
