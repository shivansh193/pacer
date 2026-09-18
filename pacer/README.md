# PACER — Reproduction Instructions

## Prerequisites
- Linux (or WSL2 on Windows)
- g++ with C++17 support
- git

## Setup (one-time)

```bash
git clone https://gitlab.com/trbot86/setbench.git
cd setbench
git submodule update --init --recursive
cp reclaimer_pacer.h common/recordmgr/reclaimer_pacer.h
```

## Build

```bash
cd microbench

# PACER (our algorithm)
g++ ./main.cpp -o bin/brown_ext_abtree_lf.pacer \
  -I../ds/brown_ext_abtree_lf -I../common -I../common/recordmgr \
  -I../common/rq -I../common/papi -I../common/descriptors \
  -DDS_TYPENAME=brown_ext_abtree_lf -DRECLAIM_TYPE=pacer \
  -DMAX_THREADS_POW2=512 -DCPU_FREQ_GHZ=2.1 \
  "-DMEMORY_STATS=if(1)" "-DMEMORY_STATS2=if(0)" \
  -DNDEBUG -DDEBRA_ORIGINAL_FREE -DUSE_TREE_STATS \
  -std=c++17 -O3 -mcx16 -fopenmp -gdwarf -fno-omit-frame-pointer \
  -L../lib -lpthread

# token4 baseline (Kim et al. 2024)
g++ ./main.cpp -o bin/brown_ext_abtree_lf.token4 \
  -I../ds/brown_ext_abtree_lf -I../common -I../common/recordmgr \
  -I../common/rq -I../common/papi -I../common/descriptors \
  -DDS_TYPENAME=brown_ext_abtree_lf -DRECLAIM_TYPE=token1 -Dvtoken4 \
  -DMAX_THREADS_POW2=512 -DCPU_FREQ_GHZ=2.1 \
  "-DMEMORY_STATS=if(1)" "-DMEMORY_STATS2=if(0)" \
  -DNDEBUG -DUSE_TREE_STATS \
  -std=c++17 -O3 -mcx16 -fopenmp -gdwarf -fno-omit-frame-pointer \
  -L../lib -lpthread

# DEBRA baseline
g++ ./main.cpp -o bin/brown_ext_abtree_lf.debra \
  -I../ds/brown_ext_abtree_lf -I../common -I../common/recordmgr \
  -I../common/rq -I../common/papi -I../common/descriptors \
  -DDS_TYPENAME=brown_ext_abtree_lf -DRECLAIM_TYPE=debra \
  -DMAX_THREADS_POW2=512 -DCPU_FREQ_GHZ=2.1 \
  "-DMEMORY_STATS=if(1)" "-DMEMORY_STATS2=if(0)" \
  -DNDEBUG -DDEBRA_ORIGINAL_FREE -DUSE_TREE_STATS \
  -std=c++17 -O3 -mcx16 -fopenmp -gdwarf -fno-omit-frame-pointer \
  -L../lib -lpthread
```

## Run

```bash
for threads in 1 2 4 8 16; do
  for binary in debra token4 pacer; do
    echo -n "=== $binary T=$threads === "
    ./bin/brown_ext_abtree_lf.$binary \
      -nwork $threads -insdel 50 50 -k 20000000 -t 5000 \
      2>/dev/null | grep "total_ops="
    echo ""
  done
done
```

## What to expect (measured, not claimed)

**The paper's "PACER beats token4 by 10–22%" result does not reproduce.** Rerun
on Ubuntu (WSL2, 16 logical cores, single socket, no jemalloc), 5 repeats per
cell, 5 s runs, 20M-key ABtree — raw data in `results/`, scripts in `scripts/`:

- The original `reclaimer_pacer.h` had a memory-safety bug: `rotateEpochBags`
  freed the bag being retired into *this* epoch instead of the safe old one
  (`token4` frees `last`). It aborted with `double free or corruption` in
  6 of 10 runs at 16 threads (also at 2 and 8 threads); `token4` and DEBRA
  crashed 0 of 13. The paper's numbers are from runs that happened to survive.
  Fixed to match `token4`'s rotation; 20/20 clean runs at 16 threads after.
- With the bug fixed, PACER vs `token4` is a tie within noise (0.95–1.03×,
  run-to-run sd 3–10%) on the write-only workload at every thread count. The
  original buggy build was 1.07–1.14× faster at 2–8T, consistent with (not
  proof of) the "gain" having come from freeing nodes too early.
- On 80%-read and 50%-read mixes PACER vs `token4` is also a tie (0.98–1.04×).
  I did not verify that `ConservativeHold` actually engaged in those runs
  (no per-tier counters in the C++ reclaimer), so "the adaptive tiers don't
  help" is not established — only that the end-to-end effect is unmeasurable
  here. Note that in the C++ reclaimer `AggressiveEbr` and `AmortisedFree`
  both do K=1 per op, so only `ConservativeHold` behaves differently at all.
- Not tested: 4-socket/192-thread hardware and jemalloc, i.e. the regime the
  approach is motivated by.

### Is there headroom for an adaptive K? (`results/k_sweep.csv`)

To test whether *any* per-op free count could beat K=1, `token4` was patched to
free K nodes per op (`scripts/build_k.sh`) and swept over K ∈ {1,2,4,8,16,64}
on write-only, 80%-read and range-query (long readers, `brown_ext_abtree_rq_lf`)
workloads at 8 and 16 threads, 5 repeats each. **K=1 was best in every
workload.** Larger K cost up to 35% on write-only and range-query mixes
(K=64 ≈ 0.65×) and made no difference on the 80%-read mix (0.95–0.99×, inside
noise). So on this hardware a K-adaptive policy has no throughput to win.
Peak RSS was useless as a memory metric here (the 20M-key tree is ~4.4 GB and
K changed it by <1.5%); a limbo-size counter would be needed. The regime where
Kim et al. found K matters (4 sockets, jemalloc tcache flushes) is untested.

### Does K=1's garbage cost anything? (`results/instr_sweep.csv`)

`pacer_instr.h` adds a retired-minus-freed backlog counter (sampled every 256
ops) and a per-`startOp` reclamation-latency histogram (rdtsc, calibrated to
ns). 5 repeats, 16 threads (8 for `write_8`), token4 with K ∈ {1,4,16,64} and
PACER:

- **Backlog is not reduced by larger K.** Average backlog was equal or higher
  for K>1 (e.g. write_8: 962 nodes at K=1 vs 1,109–1,406). Backlog is
  dominated by nodes waiting for the epoch/token to turn, not by the per-op
  free rate, so a K-driven controller cannot bound it. In absolute terms it is
  small: ~1K–20K nodes ≈ 0.2–4 MB against a 4.4 GB tree.
- **Larger K worsens tails.** write_16 p99.9: 67 µs (K=1) vs 143–211 µs
  (K>1); p99 up to ~20× worse at K=4/16. Throughput 0.65–0.73× (as before).
- **PACER ≈ token4 K=1** on every metric (throughput 0.93–1.02×, backlog and
  tails within noise).
- **No clear growth over time:** K=1 average backlog at 5 s ≈ 17K vs 20–35K at
  20 s (n=3, very noisy); a 60 s+ run would be needed to rule out slow drift.
- Not measured: multi-socket/jemalloc, and backlog variance across repeats is
  not reported in the table (raw values are in the CSV).

To reproduce: `scripts/build_variant.sh` builds a binary, `scripts/run_setbench.sh`
sweeps threads × algorithms, `scripts/summarize.py` prints mean ± sd and crash
counts. (Scripts assume setbench cloned at `/root/setbench`.)

These are the numbers reported in the paper (`PACER_IEEE.pdf`), produced by
`reclaimer_pacer.h` dropped into setbench as above. The Rust `bench` binary
at the repo root (`cargo build --release --bin bench`, also Linux-only —
see the top-level README) exercises a *different, more elaborate*
reimplementation of the same idea (batch draining + epoch striding instead
of setbench's token-passing K=1 scheme) against the same ABtree via a C FFI
shim in `cpp/`. Its numbers are not directly comparable to Table II in the
paper; treat it as a second, independent testbed for the SPI concept rather
than a reproduction of the published results.