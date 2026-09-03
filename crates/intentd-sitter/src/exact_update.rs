//! Private, versioned daemon-to-sitter handoff. Never encoded as SIGUSR1:
//! old sitters cannot accidentally reinterpret an exact request as latest.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::mpsc;

use crate::paths::SitterPaths;
use crate::updater::{UpdateLock, Updater};

/// Set only for children of a sitter with an active v1 control listener.
pub const EXACT_UPDATE_ENV: &str = "INTENTD_SITTER_EXACT_UPDATE_PID";
/// A temporary stabilization period, not a persistent version/channel pin.
pub const STABILIZATION: Duration = Duration::from_secs(60);

pub(crate) struct ExactRequest {
    pub version: String,
    pub lock: UpdateLock,
}

pub(crate) struct ExactServer {
    pub requests: mpsc::Receiver<ExactRequest>,
    task: tokio::task::JoinHandle<()>,
    path: PathBuf,
}

impl Drop for ExactServer {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    protocol: u8,
    version: String,
}

pub(crate) fn start(paths: &SitterPaths, updater: Arc<Updater>) -> io::Result<ExactServer> {
    use std::os::unix::fs::PermissionsExt;
    let path = paths
        .sitter_dir
        .join(format!("exact-update-{}.sock", std::process::id()));
    let listener = UnixListener::bind(&path)?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    let (tx, requests) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut line = String::new();
            let read = tokio::time::timeout(Duration::from_secs(2), async {
                BufReader::new((&mut stream).take(1024))
                    .read_line(&mut line)
                    .await
            })
            .await;
            let request = if matches!(read, Ok(Ok(_))) && line.ends_with('\n') {
                serde_json::from_str::<Request>(&line)
                    .ok()
                    .filter(|r| r.protocol == 1)
            } else {
                None
            };
            let reservation = match request {
                Some(request) => updater
                    .reserve_exact(&request.version)
                    .map(|lock| ExactRequest {
                        version: request.version,
                        lock,
                    })
                    .map_err(|e| e.to_string()),
                None => Err("invalid exact-update v1 request".to_string()),
            };
            let response = match &reservation {
                Ok(request) => {
                    serde_json::json!({"ok":true,"accepted":true,"version":request.version})
                }
                Err(error) => serde_json::json!({"error":error}),
            };
            let response = format!("{response}\n");
            if tokio::time::timeout(
                Duration::from_secs(2),
                stream.write_all(response.as_bytes()),
            )
            .await
            .is_ok_and(|r| r.is_ok())
            {
                if let Ok(request) = reservation {
                    if tx.send(request).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    Ok(ExactServer {
        requests,
        task,
        path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Channel;
    use crate::updater::UpdateError;

    #[test]
    fn exact_reservation_excludes_all_installers_and_is_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let paths = SitterPaths::from_data_dir(dir.path());
        let first = Updater::with_base_url(paths.clone(), "http://127.0.0.1:1").unwrap();
        let second = Updater::with_base_url(paths, "http://127.0.0.1:1").unwrap();
        let reservation = first.reserve_exact("1.2.3").unwrap();
        assert!(matches!(
            second.install_exact("1.2.4"),
            Err(UpdateError::Busy)
        ));
        assert!(matches!(
            second.check_and_install(Channel::Alpha),
            Err(UpdateError::Busy)
        ));
        assert!(matches!(
            second.force_install(Channel::Stable),
            Err(UpdateError::Busy)
        ));
        drop(reservation);
        let automatic = second.lock().unwrap();
        assert!(matches!(
            first.reserve_exact("1.2.3"),
            Err(UpdateError::Busy)
        ));
        drop(automatic);
        assert!(first.reserve_exact("1.2.3").is_ok());
    }
}
