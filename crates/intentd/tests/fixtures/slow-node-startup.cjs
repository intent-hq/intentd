// Slow-Node-startup repro for intent-hq/intent#5649.
//
// Stands in for host-injected Node instrumentation (e.g. a Datadog
// `NODE_OPTIONS=--require .../dd-trace/init.js`) that adds ~500 ms to every
// Node process start. Preloading it busy-waits synchronously before the main
// module runs, so a mock ACP agent launched under it cannot answer the daemon's
// handshake inside a 500 ms budget. Usage, from `packages/intentd`:
//
//   NODE_OPTIONS="--require $PWD/crates/intentd/tests/fixtures/slow-node-startup.cjs" \
//     cargo nextest run -p intentd --test e2e_wss_agent_spawn_retry
//
// `SLOW_NODE_STARTUP_MS` overrides the default 450 ms stall.
'use strict';

const stallMs = Number.parseInt(process.env.SLOW_NODE_STARTUP_MS ?? '450', 10);
const deadline = Date.now() + (Number.isFinite(stallMs) ? stallMs : 450);
while (Date.now() < deadline) {
  // Busy-wait: a timer would let the event loop start serving stdin early.
}
