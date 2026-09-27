// Slow-Node-startup repro for intent-hq/intent#5649.
//
// Stands in for host-injected Node instrumentation (e.g. a Datadog
// `NODE_OPTIONS=--require .../dd-trace/init.js`) that adds ~500 ms to every
// Node process start. Preloading it busy-waits synchronously before the main
// module runs, so the stall plus Node's own startup consumes most or all of a
// mock ACP agent's 500 ms handshake budget. Usage, from `packages/intentd`:
//
//   NODE_OPTIONS="--require $PWD/crates/intentd/tests/fixtures/slow-node-startup.cjs" \
//     cargo nextest run -p intentd --test e2e_wss_agent_spawn_retry
//
// `SLOW_NODE_STARTUP_MS` overrides the default 450 ms stall. The default leaves
// Node's own startup to push the total over budget; on a host where that margin
// is not enough to reproduce the timeout, set it above the budget (e.g. 600).
'use strict';

const stallMs = Number.parseInt(process.env.SLOW_NODE_STARTUP_MS ?? '450', 10);
const deadline = Date.now() + (Number.isFinite(stallMs) ? stallMs : 450);
while (Date.now() < deadline) {
  // Busy-wait: a timer would let the event loop start serving stdin early.
}
