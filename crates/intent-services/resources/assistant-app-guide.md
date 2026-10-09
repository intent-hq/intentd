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

<!-- Sources: packages/intentd/crates/intent-services/src/lib.rs (SETUP_TERMINAL_NAME); packages/intentd/crates/intent-pty/src/host.rs. -->

Workspace setup runs in the **Setup Script** terminal and can finish without that
terminal being open. If setup stays running, inspect its last output before
retrying. Include that output, the Intent version, and the operating system when
reporting the problem.

To continue work, open its existing card or sidebar entry, then the existing agent's
conversation. Read its status and last response before sending a follow-up. Use
**Create new agent** in that workspace only when a separate conversation is needed;
choose a specialist and model where offered. A specialist defines reusable behavior;
an agent is a particular conversation doing work.

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

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/workspace/{DrivingClientIndicator,SetPrimaryClientConfirmDialog}.svelte; src/lib/components/workspace/sidebar/WorkspaceProgressCard.svelte; src/store/renderer/slices/browser-clients/browser-clients-selectors.ts; messages/en.json. -->

The workspace sidebar warns when its primary browser client is offline, even if
the workspace has no browser tabs yet. Hover over the warning for recovery help:
agent browser tabs and tunnels fail until that client reconnects or another
connected, browser-capable client is set as primary.

To switch, open the workspace in the client you want to use, open the workspace
sidebar's menu, choose **Set Current Client as Primary**, and confirm **Set as
Primary**. This moves agent-owned tabs to that client without preserving page
state; tabs you opened yourself stay where they are. The action depends on the
current client's browser capability and your access. Do not promise automatic
failover or change the primary client without the user's instruction.

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
