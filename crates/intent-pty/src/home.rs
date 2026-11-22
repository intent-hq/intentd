//! What the backend opens on a workspace's home machine (nodeProtocol 19):
//! terminals, `host.execStream` processes and saved scripts' home runs. The
//! node-connection service relays them; the head's domain starts them
//! (`RemoteLaunchPort` in intent-services re-exports these types).

/// What `RemoteLaunchPort::open_home_terminal` starts on the home.
#[derive(Debug, Clone)]
pub struct HomeTerminalOpen {
    pub mode: intent_core::node_link::PtyMode,
    pub cols: u16,
    pub rows: u16,
    /// Relative to the home checkout.
    pub cwd: Option<String>,
    /// The directory `terminal.list` shows (the orchestrator's mirror path).
    pub display_cwd: Option<String>,
    pub command: Option<String>,
    pub env: Vec<(String, String)>,
}

/// What `RemoteLaunchPort::open_home_exec` starts on the home: one
/// `host.execStream` process, as an exec-mode `ptyOpen`.
#[derive(Debug, Clone)]
pub struct HomeExecOpen {
    /// Relative to the home checkout.
    pub cwd: Option<String>,
    /// The shell command line the machine runs.
    pub command: String,
    pub env: Vec<(String, String)>,
}

/// What a home exec process reports, in order: its output, then its end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HomeExecEvent {
    Output {
        stderr: bool,
        bytes: Vec<u8>,
    },
    /// The process ended; `error` without `exit_code` when it never started
    /// or the home was lost.
    Exited {
        exit_code: Option<i32>,
        error: Option<String>,
        /// Why it ended, when the machine said (intent-hq/intent#9809).
        exit_reason: Option<intent_core::ExitReason>,
    },
}

/// Input and kill of one home exec process, sent to its machine in order.
pub trait HomeExecControl: Send + Sync {
    /// Send `data` to the process's stdin.
    ///
    /// # Errors
    ///
    /// When the home's machine is not connected or the process ended.
    fn write(&self, data: &[u8]) -> std::io::Result<()>;
    /// End the process on the machine; its [`HomeExecEvent::Exited`] follows.
    fn kill(&self);
}

/// A home exec process started by `RemoteLaunchPort::open_home_exec`.
pub struct HomeExec {
    pub events: tokio::sync::mpsc::UnboundedReceiver<HomeExecEvent>,
    pub control: std::sync::Arc<dyn HomeExecControl>,
}

/// What `RemoteLaunchPort::open_home_script` starts on the home: one
/// process of a saved script's home run.
#[derive(Debug, Clone)]
pub struct HomeScriptOpen {
    pub script_id: String,
    pub command: String,
    pub mode: intent_core::ScriptMode,
    /// Relative to the home checkout.
    pub cwd: Option<String>,
    /// The saved environment, in saved order.
    pub env: Vec<(String, String)>,
    pub scrollback_bytes: usize,
    /// Receives the run's output, for its log.
    pub output_sink: Option<crate::OutputSink>,
}
