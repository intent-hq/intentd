# Browser Capture API

Capture is **intentional** - no background listeners run by default. You explicitly instrument when debugging.

## Simple Snapshot

For quick debugging, use the snapshot action:

```json
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

// Then read the summary for quick triage:
{
  "actions": [
    { "action": "getSummary", "captureDir": "/path/to/snapshot" }
  ]
}
```

## Multi-Step Session

For capturing a flow (login, checkout, etc.):

```json
// 1. Start session
{
  "actions": [
    { "action": "startSession", "workspaceId": "my-workspace-id", "name": "login-flow" }
  ]
}
// Returns: { id: "session-id", ... }

// 2. Capture initial state (reload to get console/network from page load)
{
  "actions": [
    { "action": "captureStep", "sessionId": "session-id", "stepName": "initial", "reload": true }
  ]
}

// 3. Start capturing events, do interaction, capture result
{
  "actions": [
    { "action": "startCapture", "sessionId": "session-id" },
    { "action": "evaluate", "expression": "document.querySelector('#login-btn').click()" },
    { "action": "captureStep", "sessionId": "session-id", "stepName": "after-login" },
    { "action": "endCapture", "sessionId": "session-id" }
  ]
}

// 4. End session
{
  "actions": [
    { "action": "endSession", "sessionId": "session-id" }
  ]
}
// Returns: { dir, steps, console, network, traces, metadata }
```

## Performance Traces

Named traces work like setTimeout/clearTimeout:

```json
{
  "actions": [
    { "action": "startSession", "workspaceId": "my-workspace-id", "name": "scroll-perf" }
  ]
}
// Get session.id from result

{
  "actions": [
    { "action": "startTrace", "sessionId": "session-id", "traceName": "scroll" },
    { "action": "evaluate", "expression": "window.scrollBy(0, 1000)" },
    { "action": "stopTrace", "sessionId": "session-id", "traceName": "scroll" },
    { "action": "endSession", "sessionId": "session-id" }
  ]
}
```

## Wait Conditions

Both snapshot and captureStep support wait conditions:

```json
"waitFor": {
  "console": "App ready",      // Wait for console message
  "networkIdle": 2000,         // Wait for no network activity for N ms
  "selector": "#loaded",       // Wait for element to appear
  "timeout": 30000             // Max wait time (default 30s)
}
```