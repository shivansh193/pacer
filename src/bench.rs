// src/bench.rs — PACER benchmark using Brown lock-free ABtree via C FFI
//
// Workload:   50% insert + 50% delete, uniform random [0, 2x10^7)
// Duration:   5s timed + 1s warmup (matches Kim et al. PPoPP 2024)
// Data str:   Brown's lock-free ABtree (brown_ext_abtree_lf_impl.h)
// Strategies: Batch-EBR | token_af | PACER-Discrete
//
// HOW TO RUN:
//   1. Download ABtree header (see README.md — one wget command)
//   2. cargo build --release --bin bench
//   3. ./target/release/bench | tee results.txt

mod abtree_ffi;
use abtree_ffi::AbtreeFFI;
use pacer::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::*};
use std::time::{Instant, Duration};

const KEY_RANGE:   u64 = 20_000_000;
const INSERT_PCT:  f64 = 0.50;
const RUN_SECS:    f64 = 5.0;
const WARMUP_SECS: f64 = 1.0;

fn lcg(r: &mut u64) -> u64 {
    *r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407); *r
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Strat { BatchEbr, TokenAf, PacerDiscrete }

struct RunResult { mops: f64, peak_limbo: usize }

fn run_once(strat: Strat, nthreads: usize) -> RunResult {
    let tree     = Arc::new(AbtreeFFI::new(nthreads + 1));
    let sys      = Arc::new(PacerSystem::new());
    let ebr_epoch = Arc::new(AtomicU64::new(1u64));

    // Pre-populate with KEY_RANGE/2 keys so deletes find something
    {
        let pre_tid = sys.register_thread();
        let mut t: ThreadLocalState<u64> = ThreadLocalState::new(pre_tid);
        tree.register_thread(&mut t);
        let mut rng = 0xfeed_beef_cafe_u64;
        for _ in 0..KEY_RANGE / 2 {
            let k = lcg(&mut rng) % KEY_RANGE;
            tree.insert(k, &sys, &mut t);
        }
        t.limbo_bag.drain(u64::MAX, usize::MAX);
    }

    let total = Arc::new(AtomicU64::new(0));
    let peak  = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let warmup_end = start + Duration::from_secs_f64(WARMUP_SECS);
    let run_end    = start + Duration::from_secs_f64(WARMUP_SECS + RUN_SECS);

    std::thread::scope(|s| {
        for tidx in 0..nthreads {
            let tree   = Arc::clone(&tree);
            let sys    = Arc::clone(&sys);
            let ebr_ep = Arc::clone(&ebr_epoch);
            let tot    = Arc::clone(&total);
            let pk     = Arc::clone(&peak);
            s.spawn(move || {
                let tid = sys.register_thread();
                let mut t: ThreadLocalState<u64> = ThreadLocalState::new(tid);
                tree.register_thread(&mut t);
                let mut rng     = (tidx as u64).wrapping_mul(2654435761) ^ 0xdeadbeef_00000000;
                let mut local   = 0u64;
                let mut ebr_ops = 0u64;

                while Instant::now() < run_end {
                    let key       = lcg(&mut rng) % KEY_RANGE;
                    let is_insert = (lcg(&mut rng) & 0xFF) < (INSERT_PCT * 256.0) as u64;
                    let counting  = Instant::now() >= warmup_end;

                    match strat {
                        Strat::PacerDiscrete => {
                            // PACER: AbtreeFFI calls resample+reclaim every WINDOW_SIZE
                            if is_insert { tree.insert(key, &sys, &mut t); }
                            else         { tree.remove(key, &sys, &mut t); }
                        }
                        Strat::BatchEbr => {
                            // Flush entire limbo bag every WINDOW_SIZE writes
                            let ts = ebr_ep.fetch_add(1, SeqCst);
                            unsafe { abtree_ffi::set_epoch_for_thread(ts); }
                            if is_insert { tree.insert_no_gc(key, &mut t); }
                            else         { tree.remove_no_gc(key, &mut t); }
                            ebr_ops += 1;
                            if ebr_ops % WINDOW_SIZE == 0 {
                                let safe = ebr_ep.load(SeqCst).saturating_sub(2);
                                t.limbo_bag.drain(safe, usize::MAX);
                                let e = ebr_ep.load(SeqCst);
                                ebr_ep.compare_exchange(e, e+1, SeqCst, Relaxed).ok();
                            }
                        }
                        Strat::TokenAf => {
                            // K=1 per write op (Kim et al.)
                            let ts = ebr_ep.fetch_add(1, SeqCst);
                            unsafe { abtree_ffi::set_epoch_for_thread(ts); }
                            if is_insert { tree.insert_no_gc(key, &mut t); }
                            else         { tree.remove_no_gc(key, &mut t); }
                            let safe = ebr_ep.load(SeqCst).saturating_sub(2);
                            t.limbo_bag.drain(safe, 1);
                            ebr_ops += 1;
                            if ebr_ops % WINDOW_SIZE == 0 {
                                let e = ebr_ep.load(SeqCst);
                                ebr_ep.compare_exchange(e, e+1, SeqCst, Relaxed).ok();
                            }
                        }
                    }

                    if counting { local += 1; }
                    let l = t.limbo_bag.len() as u64;
                    if l > pk.load(Relaxed) { pk.fetch_max(l, Relaxed); }
                }

                t.limbo_bag.drain(u64::MAX, usize::MAX);
                tot.fetch_add(local, Relaxed);
            });
        }
    });

    let ops  = total.load(Relaxed);
    let mops = ops as f64 / RUN_SECS / 1_000_000.0;
    RunResult { mops, peak_limbo: peak.load(Relaxed) as usize }
}

fn main() {
    let ncores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let mut tcs: Vec<usize> = (0u32..).map(|i| 1usize << i).take_while(|&n| n <= ncores).collect();
    if tcs.last().copied().unwrap_or(0) != ncores { tcs.push(ncores); }

    println!("\n{}", "=".repeat(90));
    println!("  PACER vs token_af vs Batch-EBR  |  Brown ABtree FFI  |  [0, {})", KEY_RANGE);
    println!("  Warmup {}s  |  Timed {}s  |  Threads {:?}  |  This machine: {} cores", WARMUP_SECS, RUN_SECS, tcs, ncores);
    println!("{}\n", "=".repeat(90));

    let strats = [
        (Strat::BatchEbr,      "Batch-EBR (DEBRA)"),
        (Strat::TokenAf,       "token_af (Kim 2024)"),
        (Strat::PacerDiscrete, "PACER-Discrete"),
    ];

    println!("{:<26} {:>5}  {:>10}  {:>12}  {:>9}", "Strategy", "T", "Mops/s", "PeakLimbo", "vs-EBR");
    println!("{}", "-".repeat(68));

    let mut summary: Vec<(String, usize, f64, usize, f64)> = Vec::new();

    for &tc in &tcs {
        let ebr_r = run_once(Strat::BatchEbr,     tc);
        let af_r  = run_once(Strat::TokenAf,       tc);
        let pac_r = run_once(Strat::PacerDiscrete, tc);
        let ebr_m = ebr_r.mops;

        for (r, name) in [(&ebr_r,"Batch-EBR (DEBRA)"),(&af_r,"token_af (Kim 2024)"),(&pac_r,"PACER-Discrete")] {
            let vs = r.mops / ebr_m.max(1e-9);
            println!("{:<26} {:>5}  {:>10.3}  {:>12}  {:>8.2}x", name, tc, r.mops, r.peak_limbo, vs);
            summary.push((name.to_string(), tc, r.mops, r.peak_limbo, vs));
        }
        println!();
    }

    println!("\n{}", "=".repeat(90));
    println!("  Kim et al. PPoPP 2024 — Published (192T, 4-socket Xeon, JEmalloc 5.0.1, ABtree)");
    println!("{}", "=".repeat(90));
    println!("{:<26}  {:>10}  {:>8}  {}", "Strategy", "Mops/s", "vs-EBR", "Notes");
    println!("{}", "-".repeat(70));
    println!("{:<26}  {:>10}  {:>8}  {}", "Batch-EBR (DEBRA)", "43.4",  "1.00x", "60% time in free() at 192T");
    println!("{:<26}  {:>10}  {:>8}  {}", "token_af",          "111.3", "2.57x", "K=1 Amortised Free");
    println!("{:<26}  {:>10}  {:>8}  {}", "token_af (best K)", "123.7", "2.85x", "Optimal K per workload");

    println!("\n── PACER mode selection ─────────────────────────────────────────");
    println!("{:<8} {:<22} {:>8} {:>13}", "SPI", "Mode", "batch_k", "epoch_stride");
    println!("{}", "-".repeat(55));
    for pct in [0u64,15,30,31,60,61,100] {
        let spi    = pct as f64 / 100.0;
        let mode   = select_gc_mode(spi);
        let bk     = if spi <= 0.30 { 64usize } else if spi <= 0.60 { 1 } else { 0 };
        let stride = (BASE_STRIDE as f64 * (1.0 + ALPHA * spi)).round() as u64;
        println!("{:<8.2} {:<22?} {:>8} {:>13}", spi, mode, bk, stride);
    }
    println!("\nDone. Paste output to Claude to update paper with real numbers.");
}