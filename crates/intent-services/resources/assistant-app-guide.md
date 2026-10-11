# Intent app guide for the Assistant

This bundled guide describes the app shipped with the daemon build supplying it. Use this copy instead of
earlier copies in the conversation for UI facts; preserve the user's customized
Assistant behavior. It describes supported paths, not the user's current settings,
connection, permissions, or installed frontend version. Read live tools when those
facts matter, and explain any mismatch with the user's actual UI.

## Features ready for users

Keep ordinary answers and recommendations focused on supported features ready for
users. Do not proactively mention or recommend unfinished, experimental, Labs-only,
internal, or unreleased features, or suggest enabling hidden flags to solve ordinary
tasks. Code, tool, route, or capability availability alone does not prove readiness.
If readiness is unclear, use a supported path and state what you cannot confirm.

When the user explicitly asks about an experiment, or is developing or testing it,
answer honestly, label its experimental status, and explain known limits. Do not deny
that it exists or present it as ready for general use. Ordinary setup or permission
requirements do not, by themselves, make a supported feature experimental.

Multiplayer, Collaboration, and personal device pairing are currently experimental.
Do not introduce them in routine answers about settings, devices, or mobile pairing.

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/settings/MobileSettings.svelte; src/features/devices/{PersonalDevicesPanel.svelte,personal-devices-selectors.ts}; src/store/renderer/slices/principal/principal-selectors.ts; src/store/renderer/slices/websocket-api/sagas/websocket-api-saga.ts; messages/en.json. -->

Only when explicitly asked about personal device pairing: it pairs another device
with the signed-in person's current access. It is separate from the local Remote
Access QR, which grants access to that computer. Do not offer the local QR as a
substitute or unrelated alternative. If the user asks where to test personal
pairing, use **Settings → Mobile → Intent Mobile → Show QR Code** on the intended
connection. Older instructions may call it **Pair another device as me**; that is
not the current button label. Personal pairing requires a current signed-in identity
and the connection's personal-pairing capability. The restricted-access panel also
requires Multiplayer and a refreshed identity; the remote-owner panel needs Remote
Access enabled. Connected-device list support is independent. Do not invent a
readiness date or advise enabling the experiment for ordinary mobile setup.

## Finding things

Answer location questions with the destination and action first. Read settings only
when current state affects the answer or the user reports a problem; finding a
control is not a request to change configuration.

The paths below identify the documented UI destinations. `ws.app.ui.targets()` can
omit known destinations: an omission does not mean the feature is missing. Use a
returned target for a navigation link when available; otherwise give the documented
click-by-click directions in prose.
Do not invent target IDs, anchors, settings tabs, or current setting values. A backend
settings category is not necessarily a visible Settings section. If navigation
cannot open a documented destination, explain how to reach it manually and check
the current connection, access, and version rather than guessing another URL.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/settings/SettingsSidebarNav.svelte; src/shared/app-ui-targets.ts; messages/en.json. -->

## Find a setting

Open Settings and use these sidebar labels (English labels shown):

| Section | What it contains | Canonical link |
| --- | --- | --- |
| Appearance | Theme, colors, language, and fonts | `/settings?tab=display#theme` |
| General | Notifications, updates, external apps, GitHub link behavior | `/settings?tab=app-behavior#notifications` |
| Input and shortcuts | Voice dictation and keyboard shortcuts | `/settings?tab=input#keyboard-shortcuts` |
| Agent defaults | Global instructions and agent features | `/settings?tab=agent-behavior#global-instructions` |
| Providers | AI coding CLIs and default model | `/settings?tab=providers#providers` |
| Connections | Integrations and MCP servers | `/settings?tab=connections#integrations` |
| Devices | Local machine and saved remote connections | `/settings?tab=devices#devices` |
| Mobile | Intent Mobile pairing and Remote Access | `/settings?tab=mobile#mobile` |
| Workspace setup | Git, shell, and workspace defaults | `/settings?tab=setup#git-workspace` |
| Advanced | Agent backend, connection, tool output/retention, data, reset | `/settings?tab=advanced#workspace-api` |

<!-- Source: packages/intentd/crates/intent-services/src/mcp_servers.rs. -->

For MCP configuration, open **Connections → MCP servers**; its reference route is `/settings?tab=connections#mcp-servers`. When MCP is enabled, saving edits to an enabled server reconnects the daemon using the saved settings, including a changed URL. Disabled servers stay disabled; an unreachable URL shows a connection error.

## Choose a model or change agent behavior

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/initializer/InitialAgentPicker.svelte; packages/cloudlands-fe/src/lib/components/settings/SettingsSidebarNav.svelte; packages/cloudlands-fe/src/shared/app-ui-targets.ts. -->

For the default model, open **Settings → Providers → Default model**
(`/settings?tab=providers#utility-default-model`). For new work, choose a model in
the workspace creation form where offered. Available choices depend on configured
providers; diagnose a missing provider in **Providers**. Do not promise a default
change will switch an existing agent. Specialist entries appear
in the Settings sidebar's Agents group. Create one at
`/settings?tab=specialists&view=create-specialist#create-specialist`; select an
existing specialist to edit its behavior. Global defaults and a specialist's
instructions are different scopes. Some agent feature changes apply only to newly
created sessions; do not promise they alter running conversations.

Settings sections and controls can depend on platform, daemon capabilities, or
access. A supported route does not grant permissions or prove that a control is
available on the current connection.

<!-- Sources: packages/cloudlands-fe/src/features/settings/MobileSettings.svelte; packages/cloudlands-fe/src/lib/components/settings/{DevicesSettings,DeviceRow,WebSocketApiSettings}.svelte; packages/cloudlands-fe/src/store/renderer/slices/websocket-api/sagas/websocket-api-saga.ts. -->

## Find the mobile pairing QR code

Open **Settings → Mobile → Intent Mobile → Show QR Code**. Use the mobile app's
pairing scanner. The Remote Access reference route is
`/settings?tab=mobile#websocket-api`. There is no need to expand a device's Edit panel.

For ordinary local-machine pairing, select that machine as the active connection.
If connected elsewhere, use **Settings → Devices**, then the local machine's
**Connect** action, and return to **Mobile**. The QR controls remain visible but
disabled when **Enable Remote Access** is off, data is loading, or no running listener
port is available. Explain the required enablement; do not silently change it.
For a generation error, inspect the displayed connection/listener error. TLS and
tunnel switches are not visibility requirements: do not suggest toggling them to
reveal the controls. Local pairing grants access to that machine; do not offer it
as a substitute for someone else's restricted access.

Pairing links and QR codes contain credentials; keep them private. A visible QR does not prove that the
phone can reach the host. Diagnose address/network reachability separately from
finding the QR control.

## Find work on another device

Devices lists the local machine and saved remote connections. **Add device** opens
the connection form; a remote device's actions include **Edit** and **Connect**,
with **Test connection** inside its editing panel. Workspaces and agents belong to a
host, so identify the intended device before explaining missing work or changing
host settings. **Mobile** follows the active connection and its permissions; do not
describe a remote connection's QR as belonging to the local desktop. To pair the
local machine, connect to it first. Enabling access, changing listening addresses,
and configuring a tunnel are separate choices from displaying pairing information.
Read current state and respect the user's requested scope.

## Start or continue work

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/{CompactWorkspaceInitializer,MultiSelectTabbedSidebar,CreateAgentSection}.svelte; messages/en.json. -->

Open **New workspace** (`/workspace/new`), select a repository, describe the work,
and choose **Create workspace**. The workspace groups working files, notes, and
agent conversations. Resolve any displayed repository, Git, or provider setup
error before retrying. If creation succeeded but sending the first message failed,
use the form's retry instruction instead of creating a second workspace.

<!-- Sources: packages/cloudlands-fe/src/lib/components/chat/MonitoredPrsRow.svelte; packages/intentd/crates/intent-services/src/pr_monitor.rs. -->

When an agent monitors a GitHub pull request, its conversation shows the monitored
PR and its latest status. Required checks that are still running or failing can
block merging. If monitoring is paused or information is unavailable, wait for a
fresh status before treating the PR as ready.

<!-- Sources: packages/intentd/crates/intent-services/src/{provider_images,agent_session,agent_ops}.rs. -->

Images sent to agents may be resized or re-encoded for provider delivery; original
attachments and their displayed previews are unchanged. If an image cannot be read
or the images together exceed the delivery budget, the turn fails rather than
silently omitting an image. Re-export an unreadable image as PNG/JPEG, or send fewer
images cropped to the relevant detail. A provider request-size error can also come
from images retained in earlier turns: start a new agent conversation with only
the images needed if sending fewer images still fails.

<!-- Sources: packages/intentd/crates/intent-services/src/lib.rs (SETUP_TERMINAL_NAME); packages/intentd/crates/intent-pty/src/host.rs. -->

Workspace setup runs in the **Setup Script** terminal and can finish without that
terminal being open. If setup stays running, inspect its last output before
retrying. Include that output, the Intent version, and the operating system when
reporting the problem.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/terminal/QuakeTerminalOverlay.svelte; src/features/layout/tab-types/TerminalTabType.svelte; src/features/scripts/confirm-script-deletion.ts; src/store/renderer/slices/scripts/scripts-selectors.ts; messages/en.json. -->

To delete a saved script, use **Delete script** beside its play/edit controls in
the bottom bar, or open the script panel's **…** menu and choose **Delete script**
immediately after **Show in bottom bar**. Both actions ask **Delete “[name]”?**;
confirm with **Delete script**, or cancel to keep it. Stop an active script first
and wait until it is idle or has exited. Deletion stays disabled while the script
is starting, running, restarting, its state is unknown, or changes are pending.

To continue work, open its existing card or sidebar entry, then the existing agent's
conversation. Read its status and last response before sending a follow-up. Use
**Create new agent** in that workspace only when a separate conversation is needed;
choose a specialist and model where offered. A specialist defines reusable behavior;
an agent is a particular conversation doing work.

<!-- Sources: packages/cloudlands-fe/src/lib/components/chat/questions/QuestionWizard.svelte. -->

When answering agent questions above the composer, paste images into **Or type your own answer…** to attach removable previews and send them with your answers, with or without text.

On desktop, recent conversation messages appear progressively, newest first, while
older messages in the initial window load. The conversation stays bottom-aligned
unless you scroll away; older-history loading becomes available when that initial
window finishes. Older clients may show the initial messages together.

<!-- Sources: packages/cloudlands-fe/src/lib/components/chat/ChatPanel.svelte; packages/intentd/crates/intent-transport/src/{conn,subscriptions}.rs. -->

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/HomeAssistantThreads.svelte; src/lib/components/layout/sidebar-nav/cards/ChiefCard.svelte; src/lib/components/chat/AssistantThreadTitle.svelte. -->

In **Home → Assistant**, select a thread in the sidebar. To rename it, click its
title in the conversation header. The title field uses the available header width.
Press **Enter** or click away to save; press **Escape** to cancel. Sidebar titles
select conversations and cannot be edited there.

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomeIntegrations,HomePullSummary,HomePullCode}.svelte; messages/en.json. -->

In **Home → Pull requests**, select a PR to open its preview. **Summary** shows
reviews and check status counts; expand the overview to inspect individual checks in
open status groups. Collapse a group to hide its checks. **Code** lists changed
files with their directory paths. Search by filename or path, then click a file to
expand its diff in place. Multiple files
can stay open, and the preview scrolls through them together. If a patch is
unavailable, use **Open file on GitHub** when offered or **Review on GitHub**.

## Workspace numbers for Micro keys

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomePage,HomeWorkspaceBoard}.svelte; src/lib/components/layout/WorkspaceTabStrip.svelte; src/features/hardware-console/device/{connection-status,supported-devices}.ts; src/features/hardware-console/assignment/{key-assignment,key-pin-persistence-service,workspace-key-menu}.ts; src/features/hardware-console/components/WorkspaceMicroKeySlot.svelte; messages/en.json. -->

With a supported **Creator Micro 2** or **Codex Micro** connected to Intent,
**Home → Workspaces** shows colored numbered squares on assigned workspaces in
both list and board views. Matching smaller squares appear at the left of top
workspace tabs, including pinned tabs. Numbers **1–6** identify the workspace's
Micro key assignment. An unassigned workspace has no number.

Right-click a workspace's list row, board card, or top tab and choose **Assign to
Micro Key**, then a key number. This also works for workspaces without a number;
the workspace does not need to be selected. Archived, deleted, and Assistant
workspaces cannot receive Micro assignments.
An occupied key's menu label names the workspace it will replace. Choose
**Unassign** to remove the current assignment. Changes are shared across these
views and saved.

The numbers and assignment actions appear only while Intent's Micro integration
is connected; a device merely being plugged in or detected is not enough.
Disconnecting hides them without deleting saved assignments. If they are missing,
check the device's connection to Intent before trying to change assignments.

## Inspect a failed response and recover

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/chat/{StreamingStatus,TurnFailureNotice,FailureDetails,QueuedMessageList,ChatPanel}.svelte; messages/en.json. -->

In the existing conversation, **Couldn't complete this response** summarizes a
stopped response. Open **Details** to inspect the technical error, then **Copy
details** to copy it. **Needs attention** means action is needed; choose **Retry**
when offered. **Queued** means a message is in the queue, not that a retry is
scheduled. Inspect that message and use its available controls before sending
another copy. Active work keeps its normal **Thinking** or activity status; do not
infer a retry from that status alone. Controls depend on the conversation's state
and your access.

Earlier failures may appear as **1 recorded failure** or **N recorded failures**.
Expand that label to inspect the saved errors and timestamps and use **Copy
details**. These are counts of recorded notices, not total attempts or proof that
all failures belong to the same request. A historical notice alone does not mean
the agent is still failing; check the current status and latest response.

<!-- Sources: packages/intentd/crates/intent-services/src/agent_ops.rs (agent_resolve_blocker_op); packages/cloudlands-fe/src/shared/utils/agent-attention.ts. -->

A current blocker warning means an agent reported a problem that prevents work.
After confirming recovery, the agent can clear its warning while a release or
scheduled check is still pending. A recorded blocker notice can remain in the
conversation after the current warning clears.

Follow specific recovery guidance when shown: **Retry with [model]** uses the
offered available model; **Retry on [provider]** switches away from a provider
whose usage limit was reached. For sign-in errors, run the displayed CLI login
command; signing in to the Claude desktop app does not sign in its CLI. For
**Agent session corrupted**, retry starts a fresh session and carries over the
conversation history. Do not promise a model or provider switch unless the UI
offers it; inspect **Settings → Providers** for setup problems.

<!-- Sources: packages/intentd/crates/intent-services/src/{pi_cli.rs,agent_manager.rs,pi_mcp_wrapper.cmd}; packages/intentd/crates/intent-providers/src/config.rs. -->

Pi requires the installed Pi CLI version shown in **Providers**. Intent supplies
its workspace tools automatically and keeps user-installed Pi extensions enabled.
If Pi works in a terminal but fails in Intent, copy the conversation's failure
details and check the CLI found by **Providers**. A Windows error saying Pi MCP
delivery requires a Unix host comes from an older Intent build; changing the
workspace setup script does not resolve it.

## Restore workspace browser use

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/workspace/{DrivingClientIndicator,SetPrimaryClientConfirmDialog}.svelte; src/lib/components/workspace/sidebar/WorkspaceProgressCard.svelte; src/store/renderer/slices/browser-clients/browser-clients-selectors.ts; src/store/renderer/slices/browser-clients/sagas/browser-clients-saga.ts; messages/en.json. -->

The workspace sidebar warns when its primary browser client is offline, even if
the workspace has no browser tabs yet. Hover over the warning for recovery help:
agent browser tabs and tunnels fail until that client reconnects or another
connected, browser-capable client is set as primary.

To switch, open the workspace in the client you want to use, open the workspace
sidebar's menu, choose **Set Current Client as Primary**, and confirm **Set as
Primary**. This moves agent-owned tabs to that client without preserving page
state; tabs you opened yourself stay where they are. The action depends on the
current client's browser capability and your access. After reconnecting, wait
for the current client and workspace browser state to refresh. A checked action
means this client is already explicitly primary; an unavailable action can also
mean the connection, browser capability, or access is not ready. Do not promise
automatic failover or change the primary client without the user's instruction.

## Add context, run a task, and find results

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/MultiSelectTabbedSidebar.svelte; sidebar/{AddContextSection,ContextPanel,NotesPanel}.svelte; NoteMetadataBar.svelte (sidebar/ and NoteMetadataBar paths relative to the same workspace component directory); messages/en.json. -->

In the workspace sidebar, open **Context** to find the **Spec** and other notes.
Use its **Add context** action to create a note. In layouts showing the Notes-panel
button, its visible label is **Attach more context**; **New note** is the tooltip.
Notes hold shared context, decisions, and deliverables for that workspace.
When a supported note deletion offers **Undo**, it cancels a pending deletion during
the displayed grace period (normally 15 seconds). It does not restore an already
deleted note. If the result is uncertain, check the note’s current status before trying again. If the
connected backend does not support cancellable deletion, the note is kept.

Open a task note and inspect its status and assigned agent first. Select the
assigned agent to continue existing work. To start an agent for the task, use the
play button labeled **Run agent** when available. This creates an agent and sends
the task's initial message; do not use it to duplicate work already underway.
It is a task-note action, not an action on the Spec or every ordinary note. Access
can hide it; explain the observed limitation instead of promising it is always there.

Find results in the agent's conversation and relevant task note; follow the file
or artifact links the agent supplied. A status label alone is not proof that work
succeeded. Use live workspace, note, and agent tools for actual IDs, progress, and
available actions. Never construct links from guessed IDs or claim completion from
this static guide.

Workspace file links open a file panel. Excel workbooks and other binary files
without a preview show a binary-file notice; use **Download file** in that panel
to save the original file on your computer, including from a remote workspace.
Binary files are not editable in the file panel or Files view. Delete is unavailable
when Undo cannot preserve the file's contents; the file stays unchanged.
A missing-file or access error is different: check the path and access rather than
assuming the file cannot be previewed.

<!-- Sources: packages/cloudlands-fe/src/features/layout/tab-types/FileTabType.svelte; packages/cloudlands-fe/src/lib/components/file-explorer/file-explorer-layout.svelte; packages/cloudlands-fe/src/store/renderer/slices/files/sagas/files-write-saga.ts; packages/cloudlands-fe/src/lib/client/live/live-files-client.ts; packages/cloudlands-fe/src/features/file/services/download-workspace-file.ts. -->

## Daemon lifecycle from the terminal

For a separately installed `intentd` launcher on Windows, macOS, or Linux, use
`intentd start` to run in the background, `intentd status` to inspect live status,
`intentd stop` to confirm shutdown (including a supervisor still starting or recovering;
already stopped succeeds), and `intentd restart` to replace the supervised daemon
or start it if stopped. These commands act on the
host where they run; stopping the daemon disconnects its clients.

`start` waits for readiness (60-second default budget); starting a healthy daemon
again succeeds without restarting it. Startup errors return nonzero and point to
`<data-dir>/sitter/start.log`. `restart` from stopped uses that same startup path.
For a running supervisor, restart retains its launch options; Windows waits for
replacement readiness, while macOS/Linux return after signaling, so check `status`.
This also works with supervisors launched by services or Windows Scheduled Tasks.
Background start/restart does not install or enable boot/login services or tasks;
stop does not disable an existing one.

Use the same `INTENTD_DATA_DIR` and configuration environment for each command.
`start` accepts serve options (`--mode`, `--insecure` for development only,
`--resume-all`, `--specialists-dir`); restart takes no launch options. Stop then
start to change options. `intentd --help` and `intentd start --help` are safe even
before daemon installation. `serve` stays in the foreground. Direct bare daemon
builds and the desktop-bundled daemon are distinct from this installed launcher;
start/restart are launcher commands. Check the installed launcher's help when its
version differs from this guide; do not infer launcher support from daemon version.

<!-- Sources: packages/intentd/README.md (Start, status, stop, and restart); packages/intentd/crates/intentd-sitter/src/{cli.rs,main.rs,startup.rs,supervisor.rs,paths.rs,readiness.rs}; packages/intentd/crates/intentd/src/main.rs (Serve, Status, Stop). -->
