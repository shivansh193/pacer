
## Progress Update

### What’s done

The core algorithm from the explainer is now fully implemented — pretty much a one-to-one translation:

* **`resample_spi`**
  Each thread maintains local read/write counters, which are used to compute `SPI` and `delta_SPI`. These get published via relaxed atomics, and the rolling window resets after each sample.

* **Parameter computation (`compute_batch_k`, `compute_epoch_stride`, `compute_retry_wait`)**
  All three are implemented using continuous formulas — no discrete modes or branching.
  There’s also a small optimization where `batch_k` gets reduced early if `delta_SPI` crosses a threshold (trend-based correction).

* **`Scc::take_snapshot`**
  This is the CAS-based snapshot coordinator.

  * Fast path: returns the current window timestamp in ~3 atomic loads.
  * Slow path: tries to open a new window and computes window size using `compute_W` (based on hit rate and latency headroom).

* **`PacerSystem::reclaim`**
  The unified reclamation loop is in place. It:

  * Reads `SPI` / `delta_SPI`
  * Derives all pacing parameters
  * Calls `LimboBag::drain`
  * Advances epochs based on stride

* **Write path split (`write_op_prepare` / `post_write`)**
  The write path is now split cleanly so a version-chain CAS can be inserted between the two phases without coupling to the underlying data structure.

---

### What’s next

Here’s the current roadmap, in order of priority:

1. **Version-chain data structure**
   Right now, `PacerSystem` is generic — it doesn’t know what it’s protecting.
   Need to plug in a real lock-free structure (likely a hashmap or skiplist) that uses:

   * `write_op_prepare`
   * custom CAS on version chains
   * `post_write`

   This is also where the `AtomicU64` pointer on `VersionNode` gets connected to actual structure heads.

2. **Snapshot hazard-pointer table**
   `safe_reclaim_ts` is currently just a placeholder (`epoch - 2`).
   The real implementation needs:

   * Each thread publishing its active snapshot timestamp
   * A shared table
   * GC computing the global minimum across threads

   This is the most correctness-critical piece.

3. **Thread lifecycle management**
   Threads need to:

   * Register with `PacerSystem` on spawn
   * Drain their limbo bags on exit

   Otherwise, retired nodes will leak.
   Likely solution: a `ThreadHandle` RAII wrapper.

4. **Wall-clock integration**
   `Scc::take_snapshot` currently takes `now_ns` externally.
   Need to:

   * Replace with `quanta` or `std::time::Instant`
   * Benchmark clock overhead

   On some systems, raw TSC reads might dominate fast-path cost.

5. **Benchmarking**
   Add `criterion` and cover three workloads:

   * Write-heavy (SPI ≈ 0)
   * Snapshot-heavy (SPI ≈ 1)
   * Mixed

   This will help tune:

   * `MAX_BATCH`
   * `ALPHA`
   * SCC window bounds

6. **Concurrency testing (loom / shuttle)**
   The tricky parts:

   * SCC CAS loop
   * Epoch-advance CAS

   Need exhaustive testing here.
   `loom` (Tokio) should help — it explores interleavings that normal tests won’t catch.

