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

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/settings/{ConnectionsSettings,GitLabAuthConnection,GitLabChildGitSetting}.svelte; messages/en.json. -->

When explicitly asked about experimental GitLab terminal access, open **Connections
→ Accounts → GitLab → Use GitLab account for terminal Git**. Only the connected
host's owner can change this choice, and the daemon must support it. It defaults
off for a new account or instance. Wait for the saved acknowledgement; an unavailable
status does not mean off. Use **Refresh** to check the current choice after an error.
Intent's own authorized Git operations work with this off. Enabling it lets terminal
and agent Git commands request credentials for that account and full instance; it
does not sign in `glab`. The GitLab section can remain visible for a configured
account even when Labs is off; do not infer permission or readiness from visibility.

<!-- Sources: packages/intentd/crates/intent-services/src/mcp_servers.rs; packages/intentd/crates/intent-acp/src/mcp_server/bindings/app/mcp.rs; packages/cloudlands-fe/src/features/mcp/main/mcp-oauth.ts. -->

For MCP configuration, open **Connections → MCP servers**; its reference route is `/settings?tab=connections#mcp-servers`. Users can also ask the Assistant to set up or update a server. Use `ws.app.mcp` to read existing configurations, avoid duplicates, and apply the requested change app-wide. These configuration tools are available only in Assistant chat; ordinary workspace agents retain access to enabled servers' tools.

Check the provider's official setup instructions before choosing a URL or local command. Creation connects by default; `enabled: false` saves a disabled server. For edits, read the full configuration first and preserve redacted credential placeholders; omitted keys are removed. When MCP is enabled, saving edits to an enabled server reconnects with the saved settings. Preserve its enabled value unless asked to change it. Check the returned status: saved does not mean connected. For `auth_required`, direct the user to the existing sign-in flow in **Connections → MCP servers**; keep tokens out of chat. Disabled MCP and connection errors must be reported as such.

A failed local server start or restart reports `error` with `lastError`. Fix its configuration and retry; the failure remains visible until the server is stopped or another start replaces it.

## Mac update availability

Mac update availability can differ by architecture. Intel Macs stay on their last
compatible available release in the selected update channel when a newer release
has no Intel build. Older Apple Silicon apps may also remain on an earlier
compatible release; manually installing a current Apple Silicon build lets them
receive newer releases. Do not
suggest an Apple Silicon build for an Intel Mac.

<!-- Sources: packages/cloudlands-fe/src/features/auto-update/main/{auto-update.service.ts,mac-update-compatibility.ts}; packages/cloudlands-fe/scripts/mac-channel-feeds.mjs; packages/intentd/scripts/optional-intel-release.py; packages/intentd/crates/intentd-sitter/src/updater.rs. -->

## Manage Assistant chats

Users can ask the Assistant to find, read, rename, or delete another Assistant chat.
Use `ws.app.chats.list` to identify it and read only the relevant conversation slice.
Ask which chat they mean if several match. Apply a rename or deletion only when
requested. These tools are exclusive to Assistant chat and cannot target workspace
agents. A chat cannot delete itself through these tools.

Deletion uses the app's 15-second undo window. Report the deletion as scheduled;
`ws.app.chats.cancelDelete` can undo it before the returned deadline. It cannot
recover a chat after deletion has completed.

<!-- Sources: packages/intentd/crates/intent-acp/src/mcp_server/bindings/app/chats.rs; packages/cloudlands-fe/src/lib/components/chat/assistant-thread-rename.svelte.ts; packages/cloudlands-fe/src/lib/components/layout/sidebar-nav/cards/ChiefCard.svelte; packages/cloudlands-fe/src/features/agent/utils/pending-agent-deletions.ts. -->

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

<!-- Sources: packages/cloudlands-fe/src/features/daemon-status/StartupProgress.svelte; packages/cloudlands-fe/src/shared/{startup-status,startup-monitor}.ts; messages/en.json; packages/ios/Intent/Services/{ConnectionManagerStartup,WebSocketAdmission}.swift; packages/ios/Intent/Views/Connection/{DaemonStartupProgressView,ConnectingView,ConnectionLossPresenter}.swift; packages/ios/Intent/App/RootView.swift. -->

When the app and connected daemon support startup status, the connection screen
shows **Starting intentd** and **Finishing startup** while services initialize.
Keep the app open; it reconnects when ready. Unopened notes stay lazy: startup does
not rebuild their indexes or report indexing counts or an ETA. Preparing a note
on demand after startup does not make the daemon unready again.

If a connected daemon reports an indexing phase, **Rebuilding note indexes** shows
its measured counts and approximate indexing-only estimate; an unchanged count
alone does not mean failure. **Startup status unavailable** means fresh status
could not be received; do not present cached progress as current. For **Daemon
startup failed**, check the daemon logs.

With matching mobile and daemon support, iPhone and iPad show **Starting Desktop**
and **Finishing Startup** during initial connection or reconnection over saved
direct or Tailcat routes. **Indexing Notes** appears only when the daemon reports
that phase. **Startup status unavailable. Reconnecting securely…** means current
status is unavailable. For **Startup Failed**, use **Retry Connection** after
checking the desktop daemon. **Back to Home** keeps saved credentials.

## Start or continue work

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomePage,HomeWorkspaceBoard,HomeWorkspaceDetail}.svelte; src/features/home/home.css. -->

In **Home → Workspaces**, list rows and board cards show organization/repository
labels in views that include multiple repositories. When an organization is selected,
labels show only the repository name. Narrow lists hide the row label; the selected
workspace's preview header shows its repository. Local repositories show their name
or path. Long labels truncate; hover to read the full name. Repository-specific
views identify the repository in the page heading.

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/{CompactWorkspaceInitializer,MultiSelectTabbedSidebar,CreateAgentSection}.svelte; messages/en.json. -->

Open **New workspace** (`/workspace/new`), select a repository, describe the work,
and choose **Create workspace**. The workspace groups working files, notes, and
agent conversations. Resolve any displayed repository, Git, or provider setup
error before retrying. If creation succeeded but sending the first message failed,
use the form's retry instruction instead of creating a second workspace.

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/workspace/utils/workspace-tab-navigation.ts; src/lib/utils/shortcut-bindings.ts; messages/en.json. -->

Use **Next Workspace Tab** (default **Ctrl+Tab**) or **Previous Workspace Tab**
(default **Ctrl+Shift+Tab**) to cycle through open workspace tabs in displayed
order. From **Home**, next opens the first workspace tab and previous opens the
last, regardless of which workspace was selected before Home. With no open
workspace tabs, these shortcuts do nothing. Customize them in **Settings → Input
and shortcuts → Keyboard shortcuts**.

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

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/terminal/QuakeTerminalOverlay.svelte; src/features/layout/tab-types/TerminalTabType.svelte; src/features/scripts/confirm-script-deletion.ts; src/store/renderer/slices/scripts/scripts-selectors.ts; src/lib/components/chat/EventSubscriptionsCard.svelte; messages/en.json. Backend source: packages/intentd/crates/intent-services/src/script_monitor.rs. -->

Script lists show unarchived scripts; use **Load more** when offered to see the
next page. Open a script to view available output; use **Edit command** to inspect
or edit its full definition. Output is temporary and may no longer be available after a restart. Script watches show
only active monitors and disappear when they finish, trigger, expire, or are
cancelled; completion notices remain in the agent conversation.

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

In an empty composer, press **Up Arrow** to recall a previous prompt. Recalled
prompts use the visible message text, without hidden sender or delivery metadata.
At the start of the text, **Up Arrow** moves to an older prompt; at the end,
**Down Arrow** moves forward and restores your draft. If an editable queued
message is present, **Up Arrow** in an empty composer edits that message first.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/chat/ChatPanel.svelte; src/lib/components/chat/input/TipTapEditor.svelte; src/lib/utils/user-message-presentation.ts. -->

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

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomeIntegrations,HomePullSummary,HomePullCode,HomeWorkspacePullBadge}.svelte; messages/en.json. -->

In **Home → Workspaces**, PR badges update as Intent observes changes.
**Queued** means the PR is in the merge queue, including while queue checks
are running. You do not need to open the workspace or hover over the PR to
receive these updates.

In **Home → Pull requests**, select a PR to open its preview. **Summary** shows
reviews and check status counts; expand the overview to inspect individual checks in
open status groups. Collapse a group to hide its checks. **Code** lists changed
files with their directory paths. Search by filename or path, then click a file to
expand its diff in place. Multiple files
can stay open, and the preview scrolls through them together. If a patch is
unavailable, use **Open file on GitHub** when offered or **Review on GitHub**.

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomePage,HomeIntegrations,HomeRepositorySummary,HomeReviewConversation}.svelte; src/features/home/{home-repository-discovery-saga,home-integrations-saga,home-review-threads}.ts; messages/en.json. -->

For the experimental GitLab review flow, open **Home → Pull requests**. The inner
list is labeled **Merge requests** for GitLab alone or **Pull and merge requests**
with both forges. It reads selected, known repositories through the current
connection. **Closed / merged** includes merged and unmerged-closed MRs. Use
**Load more** when offered, including after an empty filtered page. **Summary**
shows the qualified project, description, available checks and approvals, merge
requirements, and discussions with replies. Restricted, unknown, partial or
unsupported information does not establish merge readiness. **Review head** and
**Note diff head** distinguish the current review from a note's code position.
The GitLab preview has no **Code** tab. Use **Review on GitLab** for provider-side
actions. **Start workspace** opens the existing creation form with the qualified
item; it does not automatically check out the MR source head. The separate
**Linear issues** section is unchanged.

## Home Workspaces

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomePage,HomeWorkspaceSettings,HomeWorkspaceBoard}.svelte; src/features/home/{home-workspace-status-groups,home-workspaces-persistence}.ts. -->

In **Home → Workspaces**, open **View options → Group by** in either list or board
view. **Activity** keeps the broad **Needs you**, **Running**, and **Done & idle**
groups. In list view, **Status** groups workspace statuses in this order:
**Needs attention** (failed, blocked, or needing attention), **Not started**,
**In progress**, **PR open**, **PR mergeable**, **PR queued**, **PR merged**, then
**Done & idle** (idle or complete).
**Repository** groups by repository; **None** removes grouping.

The board uses **Activity** while the list is grouped by **Status**. Switching back
to the list restores its detailed groups.

The grouping choice is remembered with Home's settings for the current signed-in
user and connected backend. **Activity** remains the default; existing saved
grouping choices keep their behavior.

## Home workspace status icons

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/home/{HomeWorkspaceStatus.svelte,home-attention.ts,home-model.ts}; src/lib/components/workspace/utils/workspace-status-presentation.ts; messages/en.json. Backend source: packages/intentd/crates/intent-services/src/workspace_status.rs (workspace_is_waiting). -->

In **Home → Workspaces**, the status icon in list and board views describes the
workspace's current status. Hover over it for an explanation:

- **PR Open** (pull request icon): a pull request is open.
- **PR Mergeable** (pull request icon): a pull request is reported ready to merge.
- **PR Queued** (hourglass): a pull request is in the merge queue; it has not merged yet.

Colors follow the current theme: Open and Queued use its informational color;
Mergeable uses its success color. Use the label and tooltip to identify the state.

Questions, review requests, failures, and blockers can take precedence over PR
icons. **PR Mergeable** does not grant permission to merge: agents still need the
user's explicit authorization and must check the current merge requirements.

A muted hourglass indicates waiting on external events or delegated work in the
workspace; it does not mean every agent is waiting. It replaces an otherwise idle
dot or appears beside a PR, merged, or complete status. **PR Queued** keeps one
hourglass, with additional waiting explained in its tooltip. Waiting is also
included in accessible status text. The waiting indicator is hidden when the main
status shows active work, unread updates, a question, a review request, a failure,
or a blocker.

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

<!-- Sources (paths relative to packages/cloudlands-fe): src/features/agent/agent-attention-toast-service.ts; src/store/renderer/slices/agent-session/sagas/agent-failure-toast-saga.ts. -->

**Switch To** notifications for agent failures, attention requests, and automatic
workspace unarchiving are skipped when that workspace is already selected, even
if another conversation, file, or note is open. Notifications for other workspaces
can still appear. The workspace's agent status and conversation notices remain
available.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/chat/{StreamingStatus,TurnFailureNotice,FailureDetails,QueuedMessageList,QueuedMessageActions,ChatPanel}.svelte; messages/en.json. -->

In the existing conversation, **Couldn't complete this response** summarizes a
stopped response. Open **Details** to inspect the technical error, then **Copy
details** to copy it. **Needs attention** means action is needed; choose **Retry**
when offered. **Queued** means a message is in the queue, not that a retry is
scheduled. Inspect that message and use its available controls before sending
another copy. Active work keeps its normal **Thinking** or activity status; do not
infer a retry from that status alone. Controls depend on the conversation's state
and your access.

For one queued message, use **Edit**, **Send now**, or **Remove** in the queue
header. While editing, **Save** and **Cancel** stay beside the message. With
several queued messages, each message has its own controls; the header offers bulk actions.

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

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/workspace/{DrivingClientIndicator,SetPrimaryClientConfirmDialog}.svelte; src/lib/components/workspace/sidebar/WorkspaceProgressCard.svelte; src/features/events/daemon-events-bridge.client.ts; src/store/renderer/slices/browser-clients/{browser-clients-selectors,browser-clients-slice}.ts; src/store/renderer/slices/browser-clients/sagas/browser-clients-saga.ts; messages/en.json. -->

The workspace sidebar warns when its primary browser client is offline, even if
the workspace has no browser tabs yet. Hover over the warning for recovery help:
agent browser tabs and tunnels fail until that client reconnects or another
connected, browser-capable client is set as primary.

To switch, open the workspace in the client you want to use, open the workspace
sidebar's menu, choose **Set Current Client as Primary**, and confirm **Set as
Primary**. This moves agent-owned tabs to that client without preserving page
state; tabs you opened yourself stay where they are. The action depends on the
current client's connection, browser capability and your access. A checkmark
means the saved primary selection matches the confirmed current client; it does
not guarantee browser availability. The selection updates when the primary
changes; after reconnecting, wait for the current client and workspace browser
state to refresh. When the saved selection is known and this client is not
selected, the action is available once its connection, browser capability and
your access are ready. Do not promise automatic failover or change the primary
client without the user's instruction.

## Add context, run a task, and find results

<!-- Sources: packages/cloudlands-fe/src/lib/components/workspace/MultiSelectTabbedSidebar.svelte; sidebar/{AddContextSection,ContextPanel,NotesPanel}.svelte; NoteMetadataBar.svelte (sidebar/ and NoteMetadataBar paths relative to the same workspace component directory); messages/en.json. -->

In the workspace sidebar, open **Context** to find the **Spec** and other notes.
Use its **Add context** action to create a note. In layouts showing the Notes-panel
button, its visible label is **Attach more context**; **New note** is the tooltip.
Notes hold shared context, decisions, and deliverables for that workspace.

<!-- Sources (paths relative to packages/cloudlands-fe): src/lib/components/workspace/initializer/{IssueSuggestions,GitLabContextSuggestions}.svelte; src/lib/components/workspace/sidebar/context-picker/ContextPickerModal.svelte. -->

When testing experimental GitLab context suggestions, choose **GL Issues** or
**GL MRs** in workspace creation or the workspace context picker. Search the
selected projects; **Use link** accepts an explicit project URL where offered.
**Refresh** and **Load more** operate on that selection. Unsupported filters and
partial results remain explicit. Selecting an item attaches its qualified context;
it does not automatically check out the MR's source branch. Availability depends
on the current connection, repository access and supported daemon.

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

Workspace file links open a file panel. Markdown files open in a read-only preview
that renders Mermaid diagrams. Use the diagram's **View source** action to inspect
its code, or turn off **Markdown Preview** in the panel menu to edit the file's source.
Excel workbooks and other binary files
without a preview show a binary-file notice; use **Download file** in that panel
to save the original file on your computer, including from a remote workspace.
Binary files are not editable in the file panel or Files view. Delete is unavailable
when Undo cannot preserve the file's contents; the file stays unchanged.
A missing-file or access error is different: check the path and access rather than
assuming the file cannot be previewed.

<!-- Sources: packages/cloudlands-fe/src/features/layout/tab-types/FileTabType.svelte; packages/cloudlands-fe/src/lib/components/markdown/{MarkdownViewer,MarkdownDiagram,MermaidRenderer}.svelte; packages/cloudlands-fe/src/lib/components/file-explorer/file-explorer-layout.svelte; packages/cloudlands-fe/src/store/renderer/slices/files/sagas/files-write-saga.ts; packages/cloudlands-fe/src/lib/client/live/live-files-client.ts; packages/cloudlands-fe/src/features/file/services/download-workspace-file.ts. -->

## Daemon lifecycle from the terminal

For a separately installed `intentd` launcher on Windows, macOS, or Linux, use
`intentd start` to run in the background, `intentd status` to inspect live status,
`intentd stop` to confirm shutdown (including a supervisor still starting or recovering;
already stopped succeeds), and `intentd restart` to replace the supervised daemon
or start it if stopped. These commands act on the
host where they run; stopping the daemon disconnects its clients.

Startup-aware launchers keep waiting while fresh startup status arrives, with a
60-second budget for silence. Older launchers can return once the daemon is alive
but still preparing; check `intentd status` before using it. Starting a healthy daemon
again succeeds without restarting it. Startup errors return nonzero and point to
`<data-dir>/sitter/start.log`. `restart` from stopped uses that same startup path.
For a running supervisor, restart retains its launch options; Windows waits for
replacement readiness, while macOS/Linux return after signaling, so check `status`.
During startup, `intentd status` reports not ready. The scripting command
`intentd call system.status` can return successful liveness JSON with `ready: false`;
that is not application readiness. This preserves live startup with older launchers.
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
