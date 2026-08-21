# Browser API Overview

The browser API gives you programmatic access to embedded browser tabs via Chrome DevTools Protocol.
Actions are executed as a sequence of declarative operations.

## Reuse Tabs by URL

Use one tab per URL by default. Before `openTab`, call `listTabs`. If a tab you own
already has the target URL or that target's rewritten/redirected `finalUrl`, call
`focusTab` with its `tabId` and operate on that tab instead of opening another. For
example, a target using `daemon.localhost` may appear in `listTabs` as `127.0.0.1` or
the remote daemon host. An unowned (user) tab at the target URL can be reused only
after you claim it with `claimTab` (see Tab Ownership & Sizing below); tabs owned by
other agents are never reused. `openTab` dedupe is likewise per-agent: re-opening the
same `requestedUrl` reuses your own existing tab (hidden or visible — visibility does
not affect matching), never another agent's tab or an unowned one — two agents opening
the same URL get two tabs. A dedupe hit never changes the reused tab's visibility:
even with `visible: true`, a hidden tab stays hidden (and a visible tab stays
visible) — revealing an existing tab is `showTab`-only. Use `openTab` only when no
reusable tab matches the target or its `finalUrl`, or when the user explicitly asks for
another tab, side-by-side view, or second instance of the page.

A different URL may open in a new tab. Do not navigate an existing tab away from its
current URL merely to avoid opening a tab for a different URL. Do not close user-opened
tabs; leave extra tabs the user created in place.

## Tab Ownership & Sizing

Tabs are agent-scoped. Every tab carries a nullable `ownerAgentId`: user-opened tabs
start unowned; tabs you open with `openTab` are owned by you from creation. You may
only manipulate (navigate / close / evaluate / screenshot / ...) tabs you own — other
agents' tabs are visible in `listTabs` but not manipulable. Ownership failures surface
as structured action-result errors inside the per-action `{ action, success: false,
error }` envelope (never as a top-level failure): `not-owner` (an op on a tab you do
not own) and `already-claimed` (a claim lost to an earlier claim). Each names the
owning agent when the tab has one; a `not-owner` on an unowned tab carries no owner
info — the remedy is `claimTab`.

- `{ action: "claimTab", tabId, width, height? }` - Claim an unowned (user) tab for
  yourself. `width` is required (a claim without it is a validation error); omitted
  `height` defaults to 800. Claims are atomic, first-claim-wins: a successful claim
  transfers ownership and enables viewport emulation at the given size in one step.
  There is no stealing — a claim on an already-owned tab fails with `already-claimed`.
- `{ action: "resizeTab", tabId, width, height? }` - Change an owned tab's emulated
  size; omitted `height` keeps the tab's current emulated height. Owner-only: on a tab
  you do not own it returns `not-owner`. There is no size op for unowned (user) tabs
  and no reset-to-native form.
- `{ action: "listTabs", scope? }` - `scope: "mine" | "unclaimed" | "all"` (default
  `all`). Every returned tab carries `ownerAgentId` (`null` when unowned) plus owner
  display info, and sizing info: `mode: "native" | "emulated"` and, when emulated, the
  current `width` / `height` — so you can see a tab's size before deciding to claim or
  resize.

Sizing invariant: unowned (user) tabs are always native; agent-owned tabs are always
emulated — the size is applied as viewport emulation, so owned tabs render
deterministically offscreen without disturbing the user's panel layout. There is no
opt-in and no clear/reset op. Everywhere they appear (`claimTab` / `openTab` /
`resizeTab`), `width` and `height` are positive integers (CSS px); zero, negative,
fractional, or non-finite values are validation errors. Agent-issued `openTab` accepts
optional `width` / `height` and the tab is emulated from creation; omitted `width`
defaults to 1280 and omitted `height` to 800.

Lifecycle: ownership is persisted alongside the tab and survives app restarts — an
owned tab never silently reverts to unowned on relaunch — and persists when the owning
agent completes. Agent deletion destroys all the agent's tabs, self-opened and claimed
alike (there is no release-to-unowned path, so no tab ever transitions
emulated→native). A user "close" of an agent-owned tab is a UI-level hide, not a
destroy: the tab returns to hidden (`visibility: "hidden"`), stays alive, continues to
appear in `listTabs`, and can be revealed again with `showTab`.

## Tab Visibility

Agent-opened tabs start hidden: `openTab` without `visible: true` creates the tab
hidden — alive, owned by you, emulated (the sizing invariant above is unchanged),
returned by `listTabs` with `visibility: "hidden"`, and rendering offscreen — with no
panel mount and no focus or active-tab change. Pass `visible: true` to open directly
into the user's panel layout. Per-agent `openTab` dedupe matches regardless of
visibility — a same-URL reopen reuses your tab whether it is hidden or visible — and a
dedupe hit never changes the reused tab's visibility: a hidden tab stays hidden even
when the `openTab` carried `visible: true` (and a visible tab stays visible).
Revealing an existing tab is `showTab`-only.

- `{ action: "showTab", tabId, focus? }` - Reveal a hidden tab by mounting it into a
  panel. Owner-only: on a tab you do not own it returns the structured `not-owner`
  error. `focus` defaults to false: the tab is mounted without being activated and
  without moving panel focus; `focus: true` reveals AND activates — the tab becomes
  the panel's active tab and the panel takes focus. Idempotent on an already-visible
  tab: with `focus: false` it is a no-op success; with `focus: true` it still
  activates the tab and focuses its panel. An unknown `tabId` fails as an
  action-result error naming the unknown id.
- `listTabs` results carry `visibility: "visible" | "hidden"` on every tab.
- `focusTab` is unchanged for visible tabs (activate + focus the panel). On a hidden
  tab it fails with an action-result error directing you to `showTab` — there is no
  focusTab overload that reveals a hidden tab.

Hidden tabs are fully usable for background work — screenshots, evaluation, and
navigation are deterministic offscreen thanks to viewport emulation. Reveal a tab
only when the user should see it, and prefer the default `focus: false` so you never
steal focus.

Tab operations do not require the workspace to be currently open/visible in the app:
every action (`openTab` hidden or visible, `closeTab`, `showTab`, `evaluate`,
`screenshot`, …) works regardless of workspace visibility — webviews spin up in the
background as needed. Visibility/activation effects apply to the persisted layout
state: `showTab` mounts (and with `focus: true`, activates) the tab in the
workspace's layout so it is correct when the user next opens the workspace. When the
workspace is not currently visible in the UI, no actual UI focus/activation is
attempted: `showTab { focus: true }`, `focusTab`, and `openTab { visible: true }`
succeed, apply their state effects, skip the UI focus attempt, and the action result
carries an additive `warning` string stating that the workspace is not visible so no
UI focus was attempted (the field is absent when the workspace is visible).

## Basic Actions
- `{ action: "listTabs", scope? }` - List browser tabs (`scope: "mine" | "unclaimed" | "all"`, default `all`) with ownership, sizing, and visibility info
- `{ action: "getAccessibilityTree", tabId? }` - Get page structure as YAML
- `{ action: "screenshot", tabId? }` - Take a screenshot
- `{ action: "evaluate", expression, tabId? }` - Run JavaScript in the page
- `{ action: "focusTab", tabId? }` - Focus/remount a visible tab (fails on a hidden tab — use showTab)

## Capture Actions (for debugging)
- `{ action: "snapshot", workspaceId, name?, reload?, waitFor? }` - Quick point-in-time capture
- `{ action: "startSession", workspaceId, name? }` / `{ action: "endSession", sessionId }` - Multi-step capture
- `{ action: "startCapture", sessionId }` / `{ action: "endCapture", sessionId }` - Event capture
- `{ action: "captureStep", sessionId, stepName, reload?, waitFor? }` - Capture a named step
- `{ action: "startTrace", sessionId, traceName }` / `{ action: "stopTrace", sessionId, traceName }` - Performance traces

## Navigation
- `{ action: "navigate", url, tabId? }` - Navigate an existing tab to a new URL

## Recovery
- `{ action: "resetTab", tabId? }` - Force reset CDP connection (use if you get "already attached" errors)

## UI Control
- `{ action: "openTab", url, position?, visible?, width?, height? }` - Open a new browser tab, owned by you; hidden by default (see Tab Visibility)
  - visible: true opens directly into the UI; omitted/false creates the tab hidden
  - position: 'adjacent' (default), 'replace', or 'same'
  - width/height: emulated viewport size in CSS px; omitted width defaults to 1280, omitted height to 800
- `{ action: "showTab", tabId, focus? }` - Reveal a hidden owned tab; focus: true also activates it (see Tab Visibility)
- `{ action: "claimTab", tabId, width, height? }` - Claim an unowned (user) tab (see Tab Ownership & Sizing)
- `{ action: "resizeTab", tabId, width, height? }` - Change an owned tab's emulated size
- `{ action: "closeTab", tabId }` - Close a browser tab
  - tabId is REQUIRED; it does not fall back to the sequence-level default tabId

## Supported Protocols
The embedded browser supports http://, https://, and file:// URLs.
You can open local HTML files directly: `{ action: "openTab", url: "file:///path/to/index.html" }`

## Loopback URLs: daemon vs client machine

The embedded browser runs on the client (user's) machine, while servers you start (via
`ws.host.exec`, scripts, terminals) run on the daemon machine. In a remote-daemon setup
these are different machines, so `navigate`/`openTab` URLs support reserved hostnames:

- `daemon.localhost` — the machine where the daemon runs (where your servers run).
  Local setup: rewritten to `127.0.0.1`. Remote setup: rewritten to the daemon host.
- `client.localhost` — the user's machine (where the embedded browser runs). Always
  rewritten to `127.0.0.1`.
- Bare `127.0.0.1` / `localhost` / `[::1]` — ambiguous; assumed to mean the daemon
  machine (your frame of reference). Unchanged in a local setup; in a remote setup it is
  rewritten to the daemon host and the result carries a `warning` suggesting the explicit
  aliases.

Only the hostname is rewritten — scheme, port, path, query, and hash are preserved. Every
rewritten action's result echoes `{ requestedUrl, finalUrl, rewritten: true, reason, tunneled?, warning? }`
so you can see what was opened. Prefer `daemon.localhost` for servers you started.

Remote-daemon caveat: the client's browser reaches daemon-side servers over the network,
so prefer binding `0.0.0.0` and keeping the port reachable from the client machine. URLs
rewritten to a remote daemon host are reachability-probed before navigating. If the origin
cannot be reached, the Electron desktop client automatically forwards the port over the
daemon connection and opens the tunneled URL instead — the result carries `tunneled: true`
and the `reason` describes the forward — so servers bound to `127.0.0.1` on the daemon
still work. If the tunnel forward itself fails, or on web-browser clients (which cannot
open a local tunnel listener), the action fails with an explanatory error (pointing at
`127.0.0.1`-only binding or a firewall) instead of opening a broken page.

## Port Tunnels

Explicit port forwards from the client machine to daemon-loopback ports — the tab-free
counterpart to the implicit `openTab`/`navigate` auto-tunnel above. Use them when you
need the forwarded port itself rather than a browser tab. On the Electron desktop client
the same three actions work uniformly on every transport: remote daemons forward over
the daemon connection (`backend: "tunnel"`), local daemons use a loopback relay
(`backend: "direct"`). Never branch on the backend — it is diagnostic only. On
web-browser clients (which cannot open a local listener) no tunnel backend exists:
`openTunnel`/`closeTunnel` fail with an explanatory error and `listTunnels` returns
`{ tunnels: [] }`.

- `{ action: "openTunnel", remotePort }` → `{ remotePort, localPort, backend: "tunnel"|"direct", reused }`
  — forward daemon-side `remotePort`; reach it at `127.0.0.1:<localPort>` on the client.
  Reuses a live forward for the same `remotePort` (`reused: true`) or creates one.
  `reused` is best-effort/diagnostic — like `backend`, don't branch on it.
- `{ action: "listTunnels" }` → `{ tunnels: [{ remotePort, localPort, backend }] }`
- `{ action: "closeTunnel", remotePort }` → `{ remotePort, closed: true }` — errors when
  no active forward exists for that port.

### Lifecycle & recovery

Forwards are persistent client-side state: once opened, a forward keeps its `localPort`
for the lifetime of the Electron app. Transient drops of the underlying daemon
connection do not kill it — the forward stays registered on the same `localPort` and
lazily reconnects on the next inbound connection.

- A forward is closed only by: an explicit `closeTunnel`, a daemon backend switch,
  quitting the app, or all of its owning workspaces being archived/deleted. Forwards
  not tied to any workspace live for the app lifetime.
- One exception: a definitively refused connect to the daemon-side port (server gone)
  drops the forward immediately — re-open with `openTunnel { remotePort }` to re-mint
  it.
- `openTunnel` is cheap and idempotent per `remotePort`: it returns the existing
  forward (`reused: true`) when one is already registered, so calling it again is
  always safe and returns the current `localPort`.
- Older Electron clients may still exhibit the previous ephemeral behavior (forwards
  dropping on transport changes or after idling); if a previously returned `localPort`
  refuses connections there, re-open with `openTunnel { remotePort }`.

Use `browser_docs` with topic="capture" or topic="examples" for detailed usage.