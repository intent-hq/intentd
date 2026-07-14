## Problem

The process_cap_events_queued_resumed_evicted test was failing ~50% of the time with:
- assertion failed: events[0].event_type == AGENT_PROCESS_RESUMED
- left: agent:process:evicted
- right: agent:process:resumed

## Root Cause

When a queued spawn was woken from the wait queue (via mark_idle or deregister), the acquire method would loop back and re-evaluate the registry state. This created a race:

1. Process B marks idle, wakes queued process D
2. mark_idle spawns RESUMED event emission (async)
3. D's acquire task wakes up and loops
4. D finds B is idle and evicts it
5. acquire spawns EVICTED event emission (async)
6. Either RESUMED or EVICTED arrives first → test flake

## Fix

When woken from the queue, return immediately rather than looping. A slot is guaranteed to be available (either a process deregistered or became idle), and re-evaluation races with concurrent state changes.

## Verification

- ✅ 30x single-threaded loop: 30/30 passes (was 15/30 on main)
- ✅ Full test suite passes
- ✅ All gates green (fmt, clippy, test)

Fixes the flake identified in PR #162 review.
