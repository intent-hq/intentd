Fixes the process_cap_events_queued_resumed_evicted test failure on CI.

## Problem

The test consistently fails on CI (passes locally) with assertion failure:
resume path emits agent:process:resumed but got agent:process:evicted

## Root Cause

When a queued process is woken (resumed), the acquire method was looping to re-evaluate slot availability. This raced with the idle-marking process and could evict the very process that freed the slot, emitting a spurious eviction event instead of the expected resumed event.

## Fix

Return immediately after waking from the queue instead of looping. This ensures deterministic event ordering: queued → resumed (not queued → evicted).

## Testing

- Test passes locally before and after fix
- Fix eliminates the race condition that caused CI failures  
- All local gates pass: cargo fmt, cargo clippy, cargo test

Unblocks all intentd merges currently blocked on main being red.
