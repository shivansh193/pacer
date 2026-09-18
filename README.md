# PACER — adaptive memory reclamation for lock-free data structures

An investigation into whether epoch-based reclamation can adapt its freeing
policy to workload pressure (extending `token4` from Kim et al., PPoPP 2024).
Implemented in C++ (a drop-in reclaimer for Brown's
[setbench](https://gitlab.com/trbot86/setbench)) and Rust (a from-scratch core
with model-checked concurrency tests), then stress-tested and benchmarked on
Linux until the idea either held up or didn't.

**Short version:** the original 10–22% speedup was a bug, not a win. Stress
testing caught a use-after-free in my own reclaimer; after fixing it, the
adaptive policy ties the baseline on every workload I could run. I kept the
receipts: scripts, raw CSVs, and a sweep showing *why* there's no headroom.

## What I found

- **A use-after-free in my own design.** The C++ reclaimer freed the epoch bag
  still being written to. It aborted with heap corruption in **6 of 10** runs
  at 16 threads (baselines: **0 of 13**). Root-caused by diffing against
  `token4`'s rotation; after the fix, **20 of 20** clean runs.
- **The headline result was an artifact.** With the bug fixed, PACER vs
  `token4` is 0.93–1.03× (a tie within noise) on write-only, 80%-read and
  50%-read workloads. The buggy build was 1.07–1.14× "faster" at 2–8 threads
  and crashed while doing it.
- **Two concurrency bugs in the Rust core**, found by tests and a
  [loom](https://github.com/tokio-rs/loom) model checker: a livelock in the
  snapshot coordinator (a window flag that was never reset) and a torn read
  (an advanced generation visible before its data). Both fixed and covered by
  regression tests.
- **An unbounded memory leak** in the Rust reclaim cadence (peak 1.5M retired
  nodes vs 255 for DEBRA), found by tracking backlog instead of just
  throughput. Fixed with a per-op / per-window split.
- **No headroom for an adaptive free count on this hardware.** A sweep of the
  per-op free count K ∈ {1,2,4,8,16,64} across write-only, 80%-read and
  range-query workloads found K=1 best everywhere; larger K cost up to 35%
  throughput and worsened p99.9 by 2–3×. Backlog did not shrink with larger K —
  it is set by how long nodes wait for the epoch to turn, not by the free rate.

## Results

Ubuntu on WSL2, 16 logical cores, single socket, glibc malloc, 20M-key
lock-free ABtree, 5 s runs, 5 repeats per cell (mean ± sd in `results/`).

| Workload (16 threads) | PACER vs `token4` | Best fixed K | Larger K (K=64) |
|---|---|---|---|
| write-only | 0.93× (tie) | K=1 | 0.65× throughput, p99.9 3× worse |
| 80% reads | 1.00× (tie) | K=1 | 0.93–0.99× (mostly noise) |
| range queries (long readers) | 0.96× (tie) | K=1 | 0.68× throughput |

Measured backlog is small in absolute terms (~1K–20K retired-but-unfreed nodes,
roughly 0.2–4 MB against a 4.4 GB tree).

## What this does *not* show

The regime the approach was designed for — 4-socket machines with jemalloc
thread-cache flushes (Kim et al.'s "remote batch free") — is **untested**; I
don't have that hardware, and my simulated contention model did not reproduce
the collapse, so it is a harness, not evidence. Runs are short (5 s), single
socket, and one data structure. A next step would be an emulated or real
multi-socket run, or a bounded-memory framing with a longer-duration backlog
study.

## Layout

- `pacer/reclaimer_pacer.h` — the C++ reclaimer (setbench drop-in), with the
  bag-rotation fix and hysteresis on tier selection.
- `pacer/pacer_instr.h` — measurement hooks: backlog counter and a
  quarter-octave `rdtsc` latency histogram, one atomic report line per thread.
- `src/lib.rs` — Rust core: SPI sampling, tier selection with hysteresis,
  snapshot coalescing coordinator (seqlock-style publish), announcement table,
  synthetic benchmark, 16 unit tests + 3 loom tests. Pure `std`; builds and
  tests on Windows and Linux.
- `examples/sweep.rs` — mixed read/write sweep with repeats and stddev.
- `scripts/` — build, run, K-sweep, instrumented sweep, and summary scripts
  (assume setbench at `/root/setbench`).
- `results/` — raw CSVs behind every number above.
- `src/abtree_ffi.rs`, `src/bench.rs`, `cpp/` — an FFI testbed that is
  **unfinished** (needs setbench headers that aren't vendored); the C++ +
  setbench route above is the one that has actually been run.

## Reproduce

```bash
git clone https://gitlab.com/trbot86/setbench.git && cd setbench
git submodule update --init --recursive
# build binaries and sweep — see pacer/README.md and scripts/
python3 scripts/summarize.py results/res_write_only.csv
cargo test --lib                                   # Rust core
RUSTFLAGS="--cfg loom" cargo test --lib loom_tests # model-checked tests
```

More detail, including every experiment and its caveats: `pacer/README.md`.
See `Progress.md` for what remains open.
