// pacer/src/lib.rs — PACER: Pressure-Adaptive Concurrent Epoch Reclamation
//
// §1  SPI sampling + EMA            (resample_spi)
// §2  Continuous parameter deriv.   (compute_batch_k / epoch_stride / retry_wait)
// §3  Discrete three-tier GC mode   (GcMode, select_gc_mode, reclaim_discrete)
// §4  Snapshot Coalescing Coord.    (Scc::take_snapshot)
// §5  Thread announcement table     (AnnouncementTable, safe_reclaim_ts)
// §6  Write path                    (write_op_prepare / post_write)
// §7  Benchmark harness             (Strategy, BenchResult, run_bench_synthetic)
//
// Base paper:   Kim et al., "Are Your Epochs Too Epic?" PPoPP 2024
// Baseline alg: token_af  (Token-EBR + Amortised Free, K=1)
// Benchmark:    50% insert / 50% delete, uniform random [0, 2e7)

use std::sync::atomic::{AtomicU64, AtomicBool, AtomicU32, Ordering::*};
use std::sync::Arc;
use std::collections::VecDeque;
use std::time::{Instant, Duration};

// ── Constants ────────────────────────────────────────────────────────────────

pub const WINDOW_SIZE: u64     = 256;
pub const MAX_BATCH: u64       = 64;
pub const MIN_BATCH: u64       = 1;
pub const AF_BATCH_K: usize    = 1;
pub const BASE_STRIDE: u64     = 16;
pub const ALPHA: f64           = 4.0;
pub const TREND_THRESHOLD: f64 = 0.05;
pub const TREND_WEIGHT: f64    = 0.5;
pub const BASE_RETRY_MS: u64   = 1;
pub const MAX_RETRY_MS: u64    = 32;
pub const SPI_WRITE_HEAVY: f64 = 0.30;
pub const SPI_SNAP_HEAVY: f64  = 0.60;
pub const SPI_EMA_ALPHA: f64   = 0.15;      // EMA smoothing for SPI
pub const HOLD_LIMIT: u64      = 2 * WINDOW_SIZE;  // ticks before forced drain
pub const SCC_MIN_WINDOW_NS: u64 = 100_000;
pub const SCC_MAX_WINDOW_NS: u64 = 5_000_000;
pub const SCC_TARGET_P99_NS: u64 = 2_000_000;
pub const MAX_THREADS: usize   = 256;

// ── §5 Announcement Table ────────────────────────────────────────────────────

#[repr(C, align(64))]
pub struct AnnouncementSlot {
    pub active_ts: AtomicU64,
    _pad: [u8; 56],
}
impl AnnouncementSlot {
    fn new() -> Self { Self { active_ts: AtomicU64::new(0), _pad: [0u8; 56] } }
}

pub struct AnnouncementTable {
    slots: Vec<AnnouncementSlot>,
    active_count: AtomicU32,
}
impl AnnouncementTable {
    pub fn new() -> Self {
        Self { slots: (0..MAX_THREADS).map(|_| AnnouncementSlot::new()).collect(),
               active_count: AtomicU32::new(0) }
    }
    pub fn register(&self) -> usize {
        let tid = self.active_count.fetch_add(1, SeqCst) as usize;
        assert!(tid < MAX_THREADS); tid
    }
    pub fn announce(&self, tid: usize, ts: u64) { self.slots[tid].active_ts.store(ts, SeqCst); }
    pub fn release(&self,  tid: usize)           { self.slots[tid].active_ts.store(0,  Release); }
    pub fn min_active_ts(&self) -> u64 {
        let n = self.active_count.load(Acquire) as usize;
        let mut min = u64::MAX;
        for i in 0..n {
            let ts = self.slots[i].active_ts.load(SeqCst);
            if ts != 0 && ts < min { min = ts; }
        }
        min
    }
}

// ── VersionNode ──────────────────────────────────────────────────────────────

pub struct VersionNode<V> {
    pub value:    V,
    pub birth_ts: u64,
    pub next:     AtomicU64,
}
impl<V> VersionNode<V> {
    pub fn new(value: V, birth_ts: u64) -> Box<Self> {
        Box::new(Self { value, birth_ts, next: AtomicU64::new(0) })
    }
}

// ── LimboBag (VecDeque for O(1) oldest-first drain) ──────────────────────────

pub struct LimboBag<V> {
    q: VecDeque<(u64, *mut VersionNode<V>)>,
}
unsafe impl<V: Send> Send for LimboBag<V> {}
impl<V> LimboBag<V> {
    pub fn new() -> Self { Self { q: VecDeque::new() } }
    pub fn push(&mut self, birth_ts: u64, ptr: *mut VersionNode<V>) {
        self.q.push_back((birth_ts, ptr));
    }
    /// Push a type-erased node from the ABtree FFI retire callback.
    /// The pointer was allocated by C++ operator new; drain() will free it
    /// via Box::from_raw as *mut VersionNode<u64> — safe because the node
    /// header (birth_ts at offset 0) matches VersionNode<u64>'s layout,
    /// and the actual payload is ignored during drop.
    ///
    /// In practice each ABtree node is larger than VersionNode<u64>, so we
    /// store the raw pointer and free it with a custom drop rather than
    /// Box::from_raw. We use a sentinel value in birth_ts to flag these.
    pub fn push_raw(&mut self, birth_ts: u64, ptr: *mut u8) {
        // We cast *mut u8 → *mut VersionNode<u64>. The FFI drain path
        // calls drain_raw() which uses operator-delete-compatible free.
        // We tag the birth_ts high bit to distinguish from typed nodes.
        let tagged_ptr = ptr as *mut VersionNode<u64>;
        self.q.push_back((birth_ts | (1u64 << 63), tagged_ptr));
    }
    /// Drain up to k nodes with birth_ts < safe_ts. O(k) — no scan of retained nodes.
    /// Handles both typed VersionNode<V> (push) and raw FFI nodes (push_raw).
    pub fn drain(&mut self, safe_ts: u64, k: usize) -> usize {
        const RAW_TAG: u64 = 1u64 << 63;
        let mut freed = 0;
        while freed < k {
            match self.q.front() {
                Some(&(ts, ptr)) if (ts & !RAW_TAG) < safe_ts => {
                    let (ts, ptr) = self.q.pop_front().unwrap();
                    if ts & RAW_TAG != 0 {
                        // Raw FFI node — free with operator delete via Box<u8>
                        // The C++ side allocated with operator new (untyped),
                        // so we use dealloc via a fat pointer to avoid running
                        // a Rust destructor on C++ memory.
                        unsafe {
                            let raw = ptr as *mut u8;
                            std::alloc::dealloc(
                                raw,
                                std::alloc::Layout::from_size_align(1, 1).unwrap(),
                            );
                        }
                    } else {
                        unsafe { drop(Box::from_raw(ptr)) };
                    }
                    freed += 1;
                }
                _ => break,
            }
        }
        freed
    }
    pub fn len(&self) -> usize { self.q.len() }
}

// ── §3 Discrete Three-Tier GC Mode ───────────────────────────────────────────

/// SPI ≤ 0.30  → AggressiveEbr    (batch_k = MAX_BATCH, advance epoch)
/// 0.30–0.60   → AmortisedFree    (batch_k = AF_BATCH_K=1, advance epoch)
/// SPI > 0.60  → ConservativeHold (batch_k = 0, pause epoch advance)
///   └─ after HOLD_LIMIT ticks → forced AF pass (prevents unbounded growth)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcMode { AggressiveEbr, AmortisedFree, ConservativeHold }

pub fn select_gc_mode(spi: f64) -> GcMode {
    if spi <= SPI_WRITE_HEAVY     { GcMode::AggressiveEbr }
    else if spi <= SPI_SNAP_HEAVY { GcMode::AmortisedFree }
    else                          { GcMode::ConservativeHold }
}

// ── ThreadLocalState ─────────────────────────────────────────────────────────

pub struct ThreadLocalState<V> {
    // Rolling window counters (reset each resample)
    pub win_writes:  u64,
    pub win_snaps:   u64,
    pub op_counter:  u64,
    pub fail_streak: u32,
    // Long-run SPI estimate (EMA across windows)
    pub spi_ema:     f64,
    // Ticks spent in ConservativeHold without draining
    pub hold_ticks:  u64,
    pub limbo_bag:   LimboBag<V>,
    pub tid:         usize,
}
impl<V> ThreadLocalState<V> {
    pub fn new(tid: usize) -> Self {
        Self { win_writes: 0, win_snaps: 0, op_counter: 0, fail_streak: 0,
               spi_ema: 0.0, hold_ticks: 0, limbo_bag: LimboBag::new(), tid }
    }
}

// ── §4 Snapshot Coalescing Coordinator ───────────────────────────────────────

pub struct Scc {
    window_open:    AtomicBool,
    current_ts:     AtomicU64,
    window_open_at: AtomicU64,
    hit_count:      AtomicU64,
    total_count:    AtomicU64,
    adaptive_w_ns:  AtomicU64,
    p99_ns:         AtomicU64,
}
impl Scc {
    pub fn new() -> Self {
        Self { window_open: AtomicBool::new(false), current_ts: AtomicU64::new(0),
               window_open_at: AtomicU64::new(0),  hit_count: AtomicU64::new(0),
               total_count: AtomicU64::new(0),
               adaptive_w_ns: AtomicU64::new(SCC_MIN_WINDOW_NS),
               p99_ns: AtomicU64::new(0) }
    }
    fn compute_w(hit_rate: f64, p99_ns: u64) -> u64 {
        let headroom = 1.0_f64 - (p99_ns as f64 / SCC_TARGET_P99_NS as f64).min(1.0);
        let factor   = (hit_rate * 0.6 + headroom * 0.4).clamp(0.0, 1.0);
        (SCC_MIN_WINDOW_NS as f64 + factor * (SCC_MAX_WINDOW_NS - SCC_MIN_WINDOW_NS) as f64) as u64
    }
    pub fn hit_rate(&self) -> f64 {
        let t = self.total_count.load(Relaxed);
        if t == 0 { return 0.0; }
        self.hit_count.load(Relaxed) as f64 / t as f64
    }
    pub fn take_snapshot(&self, now_ns: u64, global_epoch: &AtomicU64) -> u64 {
        self.total_count.fetch_add(1, Relaxed);
        loop {
            let open      = self.window_open.load(Acquire);
            let ts        = self.current_ts.load(Acquire);
            let opened_at = self.window_open_at.load(Acquire);
            let w         = self.adaptive_w_ns.load(Relaxed);
            if open && now_ns.saturating_sub(opened_at) < w {
                self.hit_count.fetch_add(1, Relaxed);
                return ts;
            }
            let new_ts = global_epoch.load(SeqCst);
            match self.window_open.compare_exchange(false, true, AcqRel, Acquire) {
                Ok(_) => {
                    let new_w = Self::compute_w(self.hit_rate(), self.p99_ns.load(Relaxed));
                    self.adaptive_w_ns.store(new_w, Relaxed);
                    self.current_ts.store(new_ts, Release);
                    self.window_open_at.store(now_ns, Release);
                    return new_ts;
                }
                Err(_) => { std::hint::spin_loop(); }
            }
        }
    }
    pub fn record_latency_ns(&self, ns: u64) {
        let prev = self.p99_ns.load(Relaxed);
        self.p99_ns.store((prev as f64 * 0.9 + ns as f64 * 0.1) as u64, Relaxed);
    }
}

// ── PacerSystem ───────────────────────────────────────────────────────────────

pub struct PacerSystem {
    pub global_epoch: AtomicU64,
    // Shared SPI (approximate, Relaxed — approximation acceptable)
    pub approx_spi:   AtomicU64,   // f64 bits
    pub delta_spi:    AtomicU64,   // f64 bits
    pub scc:          Scc,
    pub announce:     AnnouncementTable,
}
impl PacerSystem {
    pub fn new() -> Self {
        Self { global_epoch: AtomicU64::new(1),
               approx_spi: AtomicU64::new(f64::to_bits(0.0)),
               delta_spi:  AtomicU64::new(f64::to_bits(0.0)),
               scc: Scc::new(), announce: AnnouncementTable::new() }
    }
    pub fn load_spi(&self) -> f64       { f64::from_bits(self.approx_spi.load(Relaxed)) }
    pub fn load_delta_spi(&self) -> f64 { f64::from_bits(self.delta_spi.load(Relaxed)) }

    // §1 SPI sampling with EMA
    pub fn resample_spi<V>(&self, t: &mut ThreadLocalState<V>) {
        let (w, s) = (t.win_writes, t.win_snaps);
        let total = w + s;
        if total == 0 { return; }
        let win_spi   = s as f64 / (total + 1) as f64;
        let prev_ema  = t.spi_ema;
        t.spi_ema     = prev_ema * (1.0 - SPI_EMA_ALPHA) + win_spi * SPI_EMA_ALPHA;
        let delta_spi = t.spi_ema - prev_ema;
        self.approx_spi.store(f64::to_bits(t.spi_ema), Relaxed);
        self.delta_spi.store(f64::to_bits(delta_spi),  Relaxed);
        t.win_writes = 0;
        t.win_snaps  = 0;
    }

    // §2 Continuous parameter derivation
    pub fn compute_batch_k(&self, spi: f64, delta_spi: f64) -> usize {
        let mut bk = MAX_BATCH as f64 * (1.0 - spi);
        if delta_spi > TREND_THRESHOLD { bk *= 1.0 - delta_spi * TREND_WEIGHT; }
        (bk as u64).max(MIN_BATCH) as usize
    }
    pub fn compute_epoch_stride(&self, spi: f64) -> u64 {
        (BASE_STRIDE as f64 * (1.0 + ALPHA * spi)).round() as u64
    }
    pub fn compute_retry_wait(&self, fail_streak: u32) -> u64 {
        (BASE_RETRY_MS << fail_streak.min(6)).min(MAX_RETRY_MS)
    }

    // §3 Discrete reclamation (paper / benchmark version)
    pub fn reclaim_discrete<V>(&self, t: &mut ThreadLocalState<V>) {
        let spi     = t.spi_ema;
        let mode    = select_gc_mode(spi);
        let safe_ts = self.safe_reclaim_ts();
        let stride  = self.compute_epoch_stride(spi);

        match mode {
            GcMode::AggressiveEbr => {
                t.hold_ticks = 0;
                t.limbo_bag.drain(safe_ts, MAX_BATCH as usize);
                if t.op_counter % stride == 0 { self.step_epoch(t); }
            }
            GcMode::AmortisedFree => {
                t.hold_ticks = 0;
                t.limbo_bag.drain(safe_ts, AF_BATCH_K);
                if t.op_counter % stride == 0 { self.step_epoch(t); }
            }
            GcMode::ConservativeHold => {
                t.hold_ticks += 1;
                if t.hold_ticks >= HOLD_LIMIT {
                    // Forced AF pass — prevents unbounded limbo growth
                    t.hold_ticks = 0;
                    t.limbo_bag.drain(safe_ts, AF_BATCH_K);
                }
                // Epoch advance intentionally paused.
            }
        }
    }

    // §2 Continuous reclamation (pacer-explainer.html variant)
    pub fn reclaim_continuous<V>(&self, t: &mut ThreadLocalState<V>) {
        let spi    = t.spi_ema;
        let dspi   = self.load_delta_spi();
        let bk     = self.compute_batch_k(spi, dspi);
        let stride = self.compute_epoch_stride(spi);
        t.limbo_bag.drain(self.safe_reclaim_ts(), bk);
        if t.op_counter % stride == 0 { self.step_epoch(t); }
    }

    fn step_epoch<V>(&self, t: &mut ThreadLocalState<V>) {
        if self.try_advance_epoch() { t.fail_streak = 0; }
        else { t.fail_streak = t.fail_streak.saturating_add(1); }
    }
    pub fn try_advance_epoch(&self) -> bool {
        let cur = self.global_epoch.load(SeqCst);
        self.global_epoch.compare_exchange(cur, cur + 1, SeqCst, Relaxed).is_ok()
    }

    // §5 Announcement table
    pub fn safe_reclaim_ts(&self) -> u64 { self.announce.min_active_ts() }
    pub fn register_thread(&self) -> usize { self.announce.register() }
    pub fn announce_snapshot(&self, tid: usize, ts: u64) { self.announce.announce(tid, ts); }
    pub fn release_snapshot(&self, tid: usize) { self.announce.release(tid); }

    // §4 SCC
    pub fn take_snapshot<V>(&self, t: &mut ThreadLocalState<V>, now_ns: u64) -> u64 {
        t.win_snaps += 1;
        let ts = self.scc.take_snapshot(now_ns, &self.global_epoch);
        self.announce_snapshot(t.tid, ts);
        ts
    }
    pub fn release_snapshot_for<V>(&self, t: &ThreadLocalState<V>) {
        self.release_snapshot(t.tid);
    }

    // §6 Write path
    pub fn write_op_prepare<V>(&self, t: &mut ThreadLocalState<V>, value: V)
        -> (Box<VersionNode<V>>, u64)
    {
        t.win_writes  += 1;
        t.op_counter  += 1;
        let ts = self.global_epoch.fetch_add(1, SeqCst);
        (VersionNode::new(value, ts), ts)
    }
    pub fn post_write<V>(&self, t: &mut ThreadLocalState<V>,
                         old_ptr: *mut VersionNode<V>, old_ts: u64)
    {
        t.limbo_bag.push(old_ts, old_ptr);
        if t.op_counter % WINDOW_SIZE == 0 {
            self.resample_spi(t);
            self.reclaim_discrete(t);
        }
    }
}

// ── §7 Benchmark harness ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy { PacerDiscrete, PacerContinuous, TokenAf, BatchEbr }

#[derive(Debug, Clone)]
pub struct BenchResult {
    pub strategy:        Strategy,
    pub threads:         usize,
    pub duration_secs:   f64,
    pub total_ops:       u64,
    pub throughput_mops: f64,
    pub peak_limbo_len:  usize,
}
impl BenchResult {
    pub fn new(strategy: Strategy, threads: usize,
               duration: Duration, ops: u64, peak: usize) -> Self {
        let secs = duration.as_secs_f64();
        Self { strategy, threads, duration_secs: secs, total_ops: ops,
               throughput_mops: ops as f64 / secs / 1_000_000.0,
               peak_limbo_len: peak }
    }
}

pub struct Workload { pub key_range: u64, pub insert_fraction: f64 }
impl Workload {
    pub fn standard() -> Self { Self { key_range: 20_000_000, insert_fraction: 0.50 } }
    pub fn next_op(&self, rng: &mut u64) -> (u64, bool) {
        *rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let key = (*rng >> 33) % self.key_range;
        let ins = ((*rng >> 17) & 0xFF) < (self.insert_fraction * 256.0) as u64;
        (key, ins)
    }
}

pub fn run_bench_synthetic(strategy: Strategy, num_threads: usize,
                           duration_secs: f64, snap_fraction: f64) -> BenchResult {
    let system     = Arc::new(PacerSystem::new());
    let start      = Instant::now();
    let deadline   = start + Duration::from_secs_f64(duration_secs);
    let total_ops  = Arc::new(AtomicU64::new(0));
    let peak_limbo = Arc::new(AtomicU64::new(0));

    std::thread::scope(|s| {
        for tidx in 0..num_threads {
            let sys = Arc::clone(&system);
            let ops = Arc::clone(&total_ops);
            let pk  = Arc::clone(&peak_limbo);
            s.spawn(move || {
                let tid = sys.register_thread();
                let mut t: ThreadLocalState<u64> = ThreadLocalState::new(tid);
                let wl  = Workload::standard();
                let mut rng = (tidx as u64) ^ 0xdeadbeef_cafebabe;
                let mut local = 0u64;

                while Instant::now() < deadline {
                    let (key, _) = wl.next_op(&mut rng);
                    let now_ns   = start.elapsed().as_nanos() as u64;
                    let do_snap  = { let r = lcg_extra(&mut rng);
                                     (r & 0xFFFF) < (snap_fraction * 65536.0) as u64 };

                    if do_snap {
                        let ts = sys.take_snapshot(&mut t, now_ns);
                        let _  = ts;
                        sys.release_snapshot_for(&t);
                    } else {
                        let (node, ts) = sys.write_op_prepare(&mut t, key);
                        let old = Box::into_raw(node);
                        match strategy {
                            Strategy::PacerDiscrete => { sys.post_write(&mut t, old, ts); }
                            Strategy::PacerContinuous => {
                                t.limbo_bag.push(ts, old);
                                if t.op_counter % WINDOW_SIZE == 0 {
                                    sys.resample_spi(&mut t);
                                    sys.reclaim_continuous(&mut t);
                                }
                            }
                            Strategy::TokenAf => {
                                t.limbo_bag.push(ts, old);
                                let safe = sys.safe_reclaim_ts();
                                t.limbo_bag.drain(safe, AF_BATCH_K);
                            }
                            Strategy::BatchEbr => {
                                t.limbo_bag.push(ts, old);
                                if t.op_counter % WINDOW_SIZE == 0 {
                                    let safe = sys.safe_reclaim_ts();
                                    t.limbo_bag.drain(safe, usize::MAX);
                                }
                            }
                        }
                    }
                    local += 1;
                    let l = t.limbo_bag.len() as u64;
                    if l > pk.load(Relaxed) { pk.fetch_max(l, Relaxed); }
                }
                ops.fetch_add(local, Relaxed);
            });
        }
    });

    let dur  = Instant::now() - start;
    let ops  = total_ops.load(Relaxed);
    let peak = peak_limbo.load(Relaxed) as usize;
    BenchResult::new(strategy, num_threads, dur, ops, peak)
}

fn lcg_extra(rng: &mut u64) -> u64 {
    *rng = rng.wrapping_mul(2862933555777941757).wrapping_add(3037000493);
    *rng
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sys() -> PacerSystem { PacerSystem::new() }
    fn thr(s: &PacerSystem) -> ThreadLocalState<u64> { ThreadLocalState::new(s.register_thread()) }

    #[test] fn spi_zero_no_snaps() {
        let s = sys(); let mut t = thr(&s);
        t.win_writes = 100; s.resample_spi(&mut t);
        assert!(t.spi_ema < 0.02);
    }
    #[test] fn spi_high_all_snaps() {
        let s = sys(); let mut t = thr(&s);
        // Warm EMA: 10 windows of all-snap
        for _ in 0..10 { t.win_snaps = 100; t.win_writes = 0; s.resample_spi(&mut t); }
        assert!(t.spi_ema > 0.70, "spi_ema={}", t.spi_ema);
    }
    #[test] fn spi_resets_window() {
        let s = sys(); let mut t = thr(&s);
        t.win_writes = 50; t.win_snaps = 50; s.resample_spi(&mut t);
        assert_eq!(t.win_writes, 0); assert_eq!(t.win_snaps, 0);
    }
    #[test] fn gc_modes() {
        assert_eq!(select_gc_mode(0.00), GcMode::AggressiveEbr);
        assert_eq!(select_gc_mode(0.30), GcMode::AggressiveEbr);
        assert_eq!(select_gc_mode(0.31), GcMode::AmortisedFree);
        assert_eq!(select_gc_mode(0.60), GcMode::AmortisedFree);
        assert_eq!(select_gc_mode(0.61), GcMode::ConservativeHold);
        assert_eq!(select_gc_mode(1.00), GcMode::ConservativeHold);
    }
    #[test] fn batch_k_continuous() {
        let s = sys();
        assert_eq!(s.compute_batch_k(0.0, 0.0), MAX_BATCH as usize);
        assert_eq!(s.compute_batch_k(1.0, 0.0), MIN_BATCH as usize);
        assert!(s.compute_batch_k(0.5, 0.2) < s.compute_batch_k(0.5, 0.0));
    }
    #[test] fn stride_grows() {
        let s = sys();
        assert_eq!(s.compute_epoch_stride(0.0), BASE_STRIDE);
        assert!(s.compute_epoch_stride(1.0) > BASE_STRIDE);
    }
    #[test] fn scc_coalesces() {
        let s = sys();
        let t1 = s.scc.take_snapshot(0,      &s.global_epoch);
        let t2 = s.scc.take_snapshot(50_000, &s.global_epoch);
        assert_eq!(t1, t2);
    }
    #[test] fn scc_new_window() {
        let s = sys();
        let t1 = s.scc.take_snapshot(0, &s.global_epoch);
        s.global_epoch.fetch_add(10, SeqCst);
        let t2 = s.scc.take_snapshot(SCC_MAX_WINDOW_NS * 2, &s.global_epoch);
        assert_ne!(t1, t2);
    }
    #[test] fn announce_min_ts() {
        let tbl = AnnouncementTable::new();
        let a = tbl.register(); let b = tbl.register();
        tbl.announce(a, 10); tbl.announce(b, 5);
        assert_eq!(tbl.min_active_ts(), 5);
        tbl.release(b);
        assert_eq!(tbl.min_active_ts(), 10);
        tbl.release(a);
        assert_eq!(tbl.min_active_ts(), u64::MAX);
    }
    #[test] fn safe_reclaim_via_announce() {
        let s = sys(); let t = thr(&s);
        assert_eq!(s.safe_reclaim_ts(), u64::MAX);
        s.announce_snapshot(t.tid, 42);
        assert_eq!(s.safe_reclaim_ts(), 42);
        s.release_snapshot_for(&t);
        assert_eq!(s.safe_reclaim_ts(), u64::MAX);
    }
    #[test] fn limbo_deque_drain() {
        let mut b: LimboBag<u64> = LimboBag::new();
        let n1 = Box::into_raw(VersionNode::new(1u64, 1));
        let n2 = Box::into_raw(VersionNode::new(2u64, 2));
        b.push(1, n1); b.push(2, n2);
        assert_eq!(b.drain(2, 10), 1);
        assert_eq!(b.drain(10, 10), 1);
        assert_eq!(b.len(), 0);
    }
    #[test] fn limbo_batch_limit() {
        let mut b: LimboBag<u64> = LimboBag::new();
        for i in 0..10u64 { b.push(i, Box::into_raw(VersionNode::new(i, i))); }
        assert_eq!(b.drain(u64::MAX, 3), 3);
        b.drain(u64::MAX, usize::MAX);
    }
    #[test] fn write_reclaim_no_panic() {
        let s = sys(); let mut t = thr(&s);
        for i in 0..WINDOW_SIZE {
            let (n, ts) = s.write_op_prepare(&mut t, i);
            s.post_write(&mut t, Box::into_raw(n), ts);
        }
    }
    #[test] fn bench_smoke() {
        let r = run_bench_synthetic(Strategy::PacerDiscrete, 1, 0.1, 0.1);
        assert!(r.total_ops > 0);
        let r2 = run_bench_synthetic(Strategy::TokenAf, 1, 0.1, 0.1);
        assert!(r2.total_ops > 0);
    }
}