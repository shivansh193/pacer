// examples/sweep.rs — mixed read/write sweep with repeats + stddev.
//
// Fixes two gaps in the paper's evaluation (see PACER_IEEE.pdf §IV, §VI):
//   1. The ABtree benchmark is 0% reads, so SPI never leaves AggressiveEbr —
//      AmortisedFree and ConservativeHold are never exercised. This sweeps
//      snap_fraction so all three tiers actually get exercised.
//   2. "Each configuration is run twice; we report the average" — no
//      variance. This runs N repeats per configuration and reports
//      mean ± stddev for throughput, peak limbo size, simulated %time-in-free
//      (see AllocatorPressure), and snapshot-latency proxy.
//
// Pure Rust, no C++ FFI — builds and runs on any platform:
//   cargo run --release --example sweep
//
// This is a synthetic, self-contained harness (see run_bench_synthetic in
// src/lib.rs), not the setbench/ABtree benchmark the paper's Table II comes
// from. Treat its numbers as evidence about the SPI/hysteresis/contention
// mechanisms, not as a reproduction of the paper's published numbers.

use pacer::*;
use std::time::Duration;

const REPEATS: usize = 5;
const RUN_SECS: f64 = 0.25;
const THREAD_COUNTS: &[usize] = &[1, 4, 8];
const SNAP_FRACTIONS: &[f64] = &[0.0, 0.1, 0.3, 0.6, 0.9];

fn mean_stddev(xs: &[f64]) -> (f64, f64) {
    let n = xs.len() as f64;
    let mean = xs.iter().sum::<f64>() / n;
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
    (mean, var.sqrt())
}

fn main() {
    let strategies = [
        (Strategy::BatchEbr, "Batch-EBR (DEBRA)"),
        (Strategy::TokenAf, "token_af"),
        (Strategy::PacerDiscrete, "PACER-Discrete"),
        (Strategy::PacerContinuous, "PACER-Continuous"),
    ];

    println!(
        "Sweep: {} repeats x {:.2}s per (strategy, threads, snap_fraction); threads={:?} snap_fraction={:?}",
        REPEATS, RUN_SECS, THREAD_COUNTS, SNAP_FRACTIONS
    );
    println!(
        "{:<20} {:>3} {:>5}  {:>16}  {:>14}  {:>12}  {:>14}",
        "Strategy", "T", "snap", "Mops/s (mean±sd)", "PeakLimbo", "%time-free", "snap-lat(us)"
    );
    println!("{}", "-".repeat(96));

    for &threads in THREAD_COUNTS {
        for &snap in SNAP_FRACTIONS {
            for (strategy, name) in strategies {
                let mut mops = Vec::with_capacity(REPEATS);
                let mut peak = Vec::with_capacity(REPEATS);
                let mut pct_free = Vec::with_capacity(REPEATS);
                let mut snap_lat = Vec::with_capacity(REPEATS);

                for _ in 0..REPEATS {
                    let r = run_bench_synthetic(strategy, threads, RUN_SECS, snap);
                    mops.push(r.throughput_mops);
                    peak.push(r.peak_limbo_len as f64);
                    pct_free.push(r.pct_time_in_free);
                    snap_lat.push(r.p99_snapshot_ns as f64 / 1000.0);
                    // Let contention/latency state settle between repeats
                    // rather than carrying a residual EMA into the next run.
                    std::thread::sleep(Duration::from_millis(5));
                }

                let (mops_m, mops_sd) = mean_stddev(&mops);
                let (peak_m, _) = mean_stddev(&peak);
                let (free_m, _) = mean_stddev(&pct_free);
                let (lat_m, _) = mean_stddev(&snap_lat);

                println!(
                    "{:<20} {:>3} {:>5.2}  {:>8.3} ± {:<5.3}  {:>14.0}  {:>11.2}%  {:>14.2}",
                    name, threads, snap, mops_m, mops_sd, peak_m, free_m, lat_m
                );
            }
            println!();
        }
    }

    println!("Done. snap=0.0 rows reproduce the paper's write-only regime;");
    println!("snap>0.0 rows are the mixed-workload evaluation §V.A of the paper called for");
    println!("but never ran. %time-free is a simulated-contention proxy, not real jemalloc");
    println!("instrumentation — see AllocatorPressure in src/lib.rs.");
}
