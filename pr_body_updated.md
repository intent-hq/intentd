## Summary

Implements three observability features to make the next unexplained daemon death diagnosable from disk (addresses gaps identified in STAB-15 investigation):

1. **File logging with rotation**
2. **Panic hook with backtrace logging**  
3. **DB health section in intentd doctor**

## Changes

### File Logging
- Uses tracing-appender::rolling with **daily rotation** (time-based, not size-based)
- Logs to INTENTD_DATA_DIR/intentd.log
- Keeps ~5 files (max_log_files setting)
- INFO default, RUST_LOG override respected
- Dual output: stderr (interactive) + file (diagnostics)
- Degrades gracefully: if file appender fails, continues with stderr-only logging

### Panic Hook
- Installed via std::panic::set_hook
- Captures full backtrace using std::backtrace::Backtrace::force_capture()
- Logs to tracing (which writes to file) with location, message, and backtrace
- Also writes to stderr for immediate visibility
- Chains default panic hook to preserve standard Rust panic formatting (thread name, etc.)
- Process panics/unwinds/aborts according to Rust's standard behavior after both hooks run

### DB Health Section
- Added report_db_health() function called from cmd_doctor()
- Runs PRAGMA integrity_check (reports all issues, not just the first)
- Runs PRAGMA wal_checkpoint(PASSIVE) and reports frame counts
  - Marks as [WARN] when busy != 0 (checkpoint incomplete) or checkpointed < log (partial checkpoint)
- Reports connection pool stats (size, idle connections)
- All checks informational, never fail doctor

## Verification

```
make check && make test
```

All tests passing locally. No existing tests broken.
