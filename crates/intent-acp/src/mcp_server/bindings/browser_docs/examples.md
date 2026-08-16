# Browser API Examples

## Opening Local HTML Files

You can open local files directly using file:// URLs:

```json
// Open a local HTML file
{
  "actions": [
    { "action": "openTab", "url": "file:///Users/me/project/index.html" }
  ]
}

// Navigate an existing tab to a different local file
{
  "actions": [
    { "action": "navigate", "url": "file:///Users/me/project/other.html" }
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

// 2. Open it using the daemon.localhost alias
{
  "actions": [
    { "action": "openTab", "url": "http://daemon.localhost:8000" }
  ]
}
// The result echoes the rewrite, e.g.:
// { requestedUrl: "http://daemon.localhost:8000", finalUrl: "http://10.0.0.5:8000/", rewritten: true, reason: "..." }

// To target an app on the user's machine instead, use client.localhost
{
  "actions": [
    { "action": "navigate", "url": "http://client.localhost:5173" }
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
(forwards not tied to any workspace live for the app lifetime). `openTunnel` stays
idempotent per `remotePort` (`reused: true` when already registered). Older Electron
clients may still exhibit the previous ephemeral behavior; if a `localPort` refuses
connections there, re-open with the same `remotePort`.

## Cleaning Up Tabs

Close tabs you opened for testing/automation when you are done with them. `closeTab`
requires an explicit `tabId` (discover it via `listTabs`) — it never falls back to the
sequence-level default `tabId`.

```json
// Open a tab, find its id, work with it, then close it
{
  "actions": [
    { "action": "openTab", "url": "http://localhost:5173" }
  ]
}

{
  "actions": [
    { "action": "listTabs" }
  ]
}
// Returns tabs with their ids, e.g. { tabId: "tab-abc123", url: "http://localhost:5173", ... }

{
  "actions": [
    { "action": "getAccessibilityTree", "tabId": "tab-abc123" },
    { "action": "closeTab", "tabId": "tab-abc123" }
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