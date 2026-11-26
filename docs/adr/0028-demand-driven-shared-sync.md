# ADR-0028: Demand-driven sync over a shared local cache

**Status:** Accepted for the fork

## Context

Short-lived stdio clients discover tools even when no recipe tool will be used. Startup authentication and a full sync put intermittent cloud stalls and rate limits on that discovery path. Separate processes hydrate independent stores and recipe hash indexes from the same cache; avoiding duplicate cloud calls alone does not prevent a stale process from publishing an obsolete index.

## Decision

Add opt-in lazy mode, leaving eager behavior as the default. Connection and discovery perform no cloud work. Actual tool invocation acquires a local mutex and a heartbeat-renewed filesystem lease, reloads existing store/cache objects, checks the last fully successful sync time, refreshes when stale, then holds the lease through the tool operation and its cache commits. Resources take the snapshot lease without triggering bulk sync.

A private atomic marker separates successful-sync freshness from attempts and write propagation. Every reference, core and additive reconcile and final cache flush must succeed to publish freshness. A failed cycle invalidates freshness before modifying files, preserves any landed partial progress, and causes explicit tool errors with a short retry backoff. A completed local write delays reconciliation for the pending-write window across all processes; it does not change the last-success timestamp. A pre-write dirty checkpoint makes a crash or error-returning operation require later reconciliation instead of silently trusting potentially partial state.

Reload is explicit per domain, preserving objects captured by registered handlers and rebuilding the full recipe hash map, including removals. Lazy external flush is disabled because all legitimate commits already flush while leased. All users of a shared directory must use lazy mode; mixing uncoordinated eager processes is unsupported. The optional derived vector index is excluded by validation until it has an equivalent cross-process reload lifecycle.

Per-attempt cloud deadlines complement demand-driven scheduling. The request-timeout implementation is adapted from upstream [PR #604](https://github.com/bojanrajkovic/mcp-paprika/pull/604); its background startup sync is deliberately not adopted. Total tool duration remains subject to retries and client/scheduler budgets.

## Alternatives

- **Bound startup and sync in the background:** preserves availability but still sends cloud traffic for unused connections and can duplicate it across processes.
- **Lock only the sync cycle:** permits a stale tool writer or empty recipe-cache flush to overwrite another process's index. The lease must include writes and reloads.
- **One persistent HTTP server:** removes most duplicate process state but changes deployment/authentication and adds a long-lived service. A useful alternative for a dedicated deployment, not required for short-lived stdio clients.
- **Persist per-entity pending-write journals:** preserves finer-grained reconciliation but adds a schema and recovery lifecycle across every store. A conservative shared propagation window is smaller and sufficient when all operations are serialized and sync is demand-driven.

## Consequences

Tools on one cache root execute serially, including slow writes. A stale/cold first call may still exceed a short trigger budget; lazy initialization is not a substitute for an adequate execution budget. Fresh reads incur disk reload I/O. Local-filesystem lease semantics and clocks are assumed; this is not distributed multi-host synchronization. Tests cover discovery without cloud work, first-use freshness, cache deletion/hash refresh, cross-process operation exclusion, failure state and write propagation.
