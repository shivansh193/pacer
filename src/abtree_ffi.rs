// src/abtree_ffi.rs — Rust FFI wrapper for Brown's lock-free ABtree
//
// The C++ glue (cpp/abtree_glue.cpp) exposes a plain C ABI.
// This file provides a safe Rust struct (AbtreeFFI) with the same
// insert/remove/contains API as our old ConcurrentBst.

use crate::{PacerSystem, ThreadLocalState, WINDOW_SIZE};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

// ── Raw C declarations ────────────────────────────────────────────────────────
#[repr(C)]
pub struct RawAbtree { _private: [u8; 0] }

pub type RetireFn = unsafe extern "C" fn(birth_ts: u64, ptr: *mut u8, ctx: *mut u8);

extern "C" {
    fn abtree_create(num_threads: i32) -> *mut RawAbtree;
    fn abtree_destroy(tree: *mut RawAbtree);
    fn abtree_register_thread(retire_fn: RetireFn, retire_ctx: *mut u8, epoch_ref: *mut u64);
    fn abtree_set_epoch(epoch: u64);
    fn abtree_insert(tree: *mut RawAbtree, tid: i32, key: i64) -> i32;
    fn abtree_remove(tree: *mut RawAbtree, tid: i32, key: i64) -> i32;
    fn abtree_contains(tree: *mut RawAbtree, tid: i32, key: i64) -> i32;
}

// ── Public helper (called from bench.rs for EBR/token_af strategies) ─────────
/// Update the C++ thread-local epoch (call before each non-PACER insert/remove).
pub unsafe fn set_epoch_for_thread(epoch: u64) {
    abtree_set_epoch(epoch);
}

// ── Retire callback (called from C++ PacerReclaimer::retire) ─────────────────
/// Pushes the retired node into the Rust limbo bag.
/// birth_ts is the epoch value stamped by PacerAllocator at allocation time.
/// ptr points to a C++ heap object (operator new) — freed via dealloc in drain().
unsafe extern "C" fn pacer_retire_cb(birth_ts: u64, ptr: *mut u8, ctx: *mut u8) {
    let t = &mut *(ctx as *mut ThreadLocalState<u64>);
    t.limbo_bag.push_raw(birth_ts, ptr);
}

// ── Safe wrapper ──────────────────────────────────────────────────────────────
pub struct AbtreeFFI {
    raw:   *mut RawAbtree,
    pub epoch: AtomicU64,
}
unsafe impl Send for AbtreeFFI {}
unsafe impl Sync for AbtreeFFI {}

impl AbtreeFFI {
    pub fn new(num_threads: usize) -> Self {
        let raw = unsafe { abtree_create(num_threads as i32) };
        assert!(!raw.is_null(), "abtree_create returned null — is the C++ compiled?");
        Self { raw, epoch: AtomicU64::new(1) }
    }

    /// Register the calling thread's retire callback. Call once per thread.
    pub fn register_thread<V>(&self, t: &mut ThreadLocalState<V>) {
        let mut epoch_val = self.epoch.load(SeqCst);
        unsafe {
            abtree_register_thread(
                pacer_retire_cb,
                t as *mut ThreadLocalState<V> as *mut u8,
                &mut epoch_val as *mut u64,
            );
        }
    }

    // ── PACER strategy: insert + GC maintenance ───────────────────────────────
    pub fn insert<V>(&self, key: u64, sys: &PacerSystem, t: &mut ThreadLocalState<V>) -> bool {
        let ts = self.epoch.fetch_add(1, SeqCst);
        unsafe { abtree_set_epoch(ts); }
        t.win_writes += 1;
        t.op_counter += 1;
        let r = unsafe { abtree_insert(self.raw, t.tid as i32, key as i64) };
        if t.op_counter % WINDOW_SIZE == 0 {
            sys.resample_spi(t);
            sys.reclaim_discrete(t);
        }
        r != 0
    }

    pub fn remove<V>(&self, key: u64, sys: &PacerSystem, t: &mut ThreadLocalState<V>) -> bool {
        let ts = self.epoch.fetch_add(1, SeqCst);
        unsafe { abtree_set_epoch(ts); }
        t.win_writes += 1;
        t.op_counter += 1;
        let r = unsafe { abtree_remove(self.raw, t.tid as i32, key as i64) };
        if t.op_counter % WINDOW_SIZE == 0 {
            sys.resample_spi(t);
            sys.reclaim_discrete(t);
        }
        r != 0
    }

    // ── EBR/token_af strategies: insert/remove without PACER GC ──────────────
    // The caller handles its own epoch and drain logic.
    pub fn insert_no_gc<V>(&self, key: u64, t: &mut ThreadLocalState<V>) -> bool {
        t.op_counter += 1;
        let r = unsafe { abtree_insert(self.raw, t.tid as i32, key as i64) };
        r != 0
    }

    pub fn remove_no_gc<V>(&self, key: u64, t: &mut ThreadLocalState<V>) -> bool {
        t.op_counter += 1;
        let r = unsafe { abtree_remove(self.raw, t.tid as i32, key as i64) };
        r != 0
    }

    pub fn contains(&self, key: u64, tid: usize) -> bool {
        unsafe { abtree_contains(self.raw, tid as i32, key as i64) != 0 }
    }

    /// No-op — live nodes are freed by abtree_destroy in Drop.
    pub fn drain(&self) {}
}

impl Drop for AbtreeFFI {
    fn drop(&mut self) {
        unsafe { abtree_destroy(self.raw); }
    }
}