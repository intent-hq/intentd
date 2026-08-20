# Browser API Examples

## Reusing a Tab Already at the Target URL

List tabs before opening one. If a tab you own already has the target URL or that
target's rewritten/redirected `finalUrl`, focus it by `tabId` and continue working in
that tab. An unowned (user) tab at the target URL must be claimed first (see "Claiming
and Resizing Tabs" below); tabs owned by other agents are never reused. For example,
when targeting `http://daemon.localhost:8000`:

```json
{
  "actions": [
    { "action": "listTabs" }
  ]
}
// A matching result may show the rewritten finalUrl instead of the requested alias.
// Each tab carries its owner and sizing info:
// { tabId: "tab-abc123", url: "http://127.0.0.1:8000/", ownerAgentId: "<your-agent-id>", mode: "emulated", width: 1280, height: 800, ... }

{
  "actions": [
    { "action": "focusTab", "tabId": "tab-abc123" },
    { "action": "getAccessibilityTree", "tabId": "tab-abc123" }
  ]
}
```

Use `openTab` only after `listTabs` shows no reusable tab with the target URL or its
rewritten/redirected `finalUrl`, or when the user explicitly asks for another tab,
side-by-side view, or second instance. A different URL may open in a new tab; do not
navigate an existing tab away from its current URL merely to avoid opening a tab for a
different URL. Leave user-opened extra tabs in place.

## Claiming and Resizing Tabs

Tabs are agent-owned: you can only manipulate tabs you own. To work in an unowned
(user) tab, claim it first — `width` is required, omitted `height` defaults to 800:

```json
// Find unowned tabs
{
  "actions": [
    { "action": "listTabs", "scope": "unclaimed" }
  ]
}
// → tabs with ownerAgentId: null and native sizing:
// { tabId: "tab-user1", url: "http://localhost:5173/", ownerAgentId: null, mode: "native", ... }

// Claim it — atomic, first-claim-wins; ownership transfer and viewport emulation
// at the given size happen in one step
{
  "actions": [
    { "action": "claimTab", "tabId": "tab-user1", "width": 1280, "height": 800 }
  ]
}

// Later, change the owned tab's emulated size (omitted height keeps the current one)
{
  "actions": [
    { "action": "resizeTab", "tabId": "tab-user1", "width": 375, "height": 667 }
  ]
}
```

Ownership failures come back as structured action-result errors (inside the
per-action `{ action, success: false, error }` envelope, never a top-level failure):

```json
// Claim lost to an earlier claim — no stealing; the error names the owner
// { action: "claimTab", success: false, errorCode: "already-claimed", ownerAgentId: "<other-agent-id>", error: "Tab ... is owned by agent ..." }

// Op on a tab you do not own — a not-owner on an unowned tab carries no owner
// to name (ownerAgentId: null); the remedy is claimTab
// { action: "resizeTab", success: false, errorCode: "not-owner", ownerAgentId: null, error: "Tab ... is not owned by you ..." }
```

Agent-issued `openTab` also accepts optional `width` / `height` (emulated from
creation; defaults 1280×800 when omitted):

```json
{
  "actions": [
    { "action": "openTab", "url": "http://localhost:5173", "width": 1440, "height": 900 }
  ]
}
```

## Opening Local HTML Files

After confirming no tab already has the target URL, you can open local files directly
using file:// URLs:

```json
// No existing tab has this URL, so open it
{
  "actions": [
    { "action": "openTab", "url": "file:///Users/me/project/index.html" }
  ]
}
```

## Opening a Server Started on the Daemon

Servers you start (via `ws.host.exec`, scripts, terminals) run on the daemon machine,
which is not the client's machine in a remote-daemon setup. Use the `daemon.localhost`
alias to target it explicitly — and bind `0.0.0.0` so a remote client can reach the port:

```json
// 1. Start a server on the daemon machine. Use a service-mode script (ws.script.*) or a
//    terminal for long-running processes — ws.host.exec is one-shot and would block:
//    ws.script.create("http-server", "python3 -m http.server 8000 --bind 0.0.0.0", "service")

// 2. After listTabs confirms no tab has this URL, open it using the daemon.localhost alias
{
  "actions": [
    { "action": "openTab", "url": "http://daemon.localhost:8000" }
  ]
}
// The result echoes the rewrite, e.g.:
// { requestedUrl: "http://daemon.localhost:8000", finalUrl: "http://10.0.0.5:8000/", rewritten: true, reason: "..." }

// To target an app on the user's machine instead, use client.localhost. After the same
// listTabs check, open a new tab if no tab already has this different URL.
{
  "actions": [
    { "action": "openTab", "url": "http://client.localhost:5173" }
  ]
}
```

Bare loopback URLs (`http://localhost:3000`, `http://127.0.0.1:3000`) are assumed
daemon-local: unchanged in a local setup, rewritten to the daemon host (with a `warning`
in the result) in a remote setup. See topic="overview" for the full convention.

## Forwarding a Daemon Port (Tunnels)

When you need the port itself (curl, WebSockets, API clients) rather than a browser tab,
forward it explicitly. On the Electron desktop client this works uniformly on every
transport — do not branch on `backend`. (Web-browser clients cannot open a local
listener, so there `openTunnel` fails with an explanatory error.)

```json
// Forward daemon-side port 8000 to a client-local port
{
  "actions": [
    { "action": "openTunnel", "remotePort": 8000 }
  ]
}
// → { remotePort: 8000, localPort: 52341, backend: "tunnel", reused: false }
// Reach the daemon-side server at http://127.0.0.1:52341 (the returned localPort).

// Inspect and clean up
{
  "actions": [
    { "action": "listTunnels" }
  ]
}
// → { tunnels: [{ remotePort: 8000, localPort: 52341, backend: "tunnel" }] }

{
  "actions": [
    { "action": "closeTunnel", "remotePort": 8000 }
  ]
}
// → { remotePort: 8000, closed: true }
```

Lifecycle: forwards are persistent — a forward keeps its `localPort` for the Electron
app's lifetime and survives transient daemon-connection drops (it lazily reconnects on
the next inbound connection). It is closed only by an explicit `closeTunnel`, a daemon
backend switch, app quit, or all of its owning workspaces being archived/deleted
(forwards not tied to any workspace live for the app lifetime). One exception: a
definitively refused connect to the daemon-side port (server gone) drops the forward
immediately — re-open with the same `remotePort` to re-mint it. `openTunnel` stays
idempotent per `remotePort` (`reused: true` when already registered). Older Electron
clients may still exhibit the previous ephemeral behavior; if a `localPort` refuses
connections there, re-open with the same `remotePort`.

## Cleaning Up Tabs

Start by listing tabs and reusing a matching one. Close a tab when you are done only if
you opened it for testing or automation; do not close a user-opened tab that you reused.
`closeTab` requires an explicit `tabId` — it never falls back to the sequence-level
default `tabId`.

```json
// First look for a tab already at the target URL
{
  "actions": [
    { "action": "listTabs" }
  ]
}
// If a matching tab exists, focus and reuse it. Leave it open if the user opened it.

// If no matching tab exists, open one for this work
{
  "actions": [
    { "action": "openTab", "url": "http://localhost:5173" }
  ]
}

// Discover the explicit id of the tab you opened
{
  "actions": [
    { "action": "listTabs" }
  ]
}
// Returns tabs with their ids, e.g. { tabId: "tab-new456", url: "http://localhost:5173", ... }

{
  "actions": [
    { "action": "getAccessibilityTree", "tabId": "tab-new456" },
    { "action": "closeTab", "tabId": "tab-new456" }
  ]
}
```

## Debugging a Page Load

```json
// Capture everything from page load
{
  "actions": [
    {
      "action": "snapshot",
      "workspaceId": "my-workspace-id",
      "reload": true,
      "waitFor": { "networkIdle": 2000 }
    }
  ]
}
// Returns: { dir, a11y, screenshot, console?, network?, metadata }

// Then check summary for quick triage:
{
  "actions": [
    { "action": "getSummary", "captureDir": "/path/from/snapshot/result" }
  ]
}
```

## Finding and Clicking Elements

```json
// Get accessibility tree to understand page structure
{
  "actions": [
    { "action": "getAccessibilityTree" }
  ]
}

// Find button by accessible name and click it
{
  "actions": [
    {
      "action": "evaluate",
      "expression": "const buttons = document.querySelectorAll('button'); for (const btn of buttons) { if (btn.textContent.includes('Submit')) { btn.click(); break; } }"
    },
    { "action": "getAccessibilityTree" }
  ]
}
```

## Capturing a Login Flow

```json
// 1. Start session
{
  "actions": [
    { "action": "startSession", "workspaceId": "my-workspace-id", "name": "login-flow" }
  ]
}
// Returns: { id: "session-id", ... }

// 2. Capture initial state
{
  "actions": [
    { "action": "captureStep", "sessionId": "session-id", "stepName": "1-initial", "reload": true }
  ]
}

// 3. Fill credentials and capture
{
  "actions": [
    { "action": "startCapture", "sessionId": "session-id" },
    { "action": "evaluate", "expression": "document.querySelector('#email').value = 'test@example.com'; document.querySelector('#password').value = 'password123';" },
    { "action": "captureStep", "sessionId": "session-id", "stepName": "2-filled" },
    { "action": "evaluate", "expression": "document.querySelector('#login-form').submit()" },
    { "action": "captureStep", "sessionId": "session-id", "stepName": "3-after-submit", "waitFor": { "networkIdle": 2000 } },
    { "action": "endCapture", "sessionId": "session-id" },
    { "action": "endSession", "sessionId": "session-id" }
  ]
}
```

## Measuring Scroll Performance

```json
// Start session and trace
{
  "actions": [
    { "action": "startSession", "workspaceId": "my-workspace-id", "name": "scroll-perf" }
  ]
}

// Run trace (note: for loops need multiple calls or use evaluate)
{
  "actions": [
    { "action": "startTrace", "sessionId": "session-id", "traceName": "scroll" },
    { "action": "evaluate", "expression": "window.scrollBy(0, 2500)" },
    { "action": "stopTrace", "sessionId": "session-id", "traceName": "scroll" },
    { "action": "endSession", "sessionId": "session-id" }
  ]
}
// Trace file saved to {session}/scroll.json - open in Chrome DevTools Performance tab
```

## Debugging Network Requests

```json
{
  "actions": [
    { "action": "startSession", "workspaceId": "my-workspace-id", "name": "api-debug" }
  ]
}

{
  "actions": [
    { "action": "startCapture", "sessionId": "session-id" },
    { "action": "evaluate", "expression": "fetch('/api/data').then(r => r.json())" },
    { "action": "captureStep", "sessionId": "session-id", "stepName": "after-fetch", "waitFor": { "networkIdle": 2000 } },
    { "action": "endCapture", "sessionId": "session-id" },
    { "action": "endSession", "sessionId": "session-id" }
  ]
}
// Network requests are in result.network (JSONL file)
```

## Comparing Before/After States

```json
{
  "actions": [
    { "action": "startSession", "workspaceId": "my-workspace-id", "name": "comparison" }
  ]
}

{
  "actions": [
    { "action": "captureStep", "sessionId": "session-id", "stepName": "before" },
    { "action": "evaluate", "expression": "document.body.style.fontSize = '20px'; document.querySelector('.sidebar').remove();" },
    { "action": "captureStep", "sessionId": "session-id", "stepName": "after" },
    { "action": "endSession", "sessionId": "session-id" }
  ]
}
// Compare the a11y trees in before/ and after/ directories
```