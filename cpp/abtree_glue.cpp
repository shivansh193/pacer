 // cpp/abtree_glue.cpp
//
// Plain-C ABI wrapper around Brown's lock-free ABtree.
// Rust calls these functions via extern "C" in abtree_ffi.rs.
//
// Brown's ABtree interface (from brown_ext_abtree_lf_impl.h):
//   abtree<K, V, Reclaimer, Allocator, Pool>
//   .insert(tid, key, value)  → bool
//   .erase (tid, key)         → bool
//   .find  (tid, key, &val)   → bool
//
// We fix K=long, V=long, and use the "debra2" reclaimer that Kim et al.
// used as their DEBRA baseline, then replace it with our PACER hooks.
//
// PACER hook design:
//   Brown's ABtree calls its reclaimer's retire(node_ptr) when it
//   removes a node from the tree.  We intercept this by supplying a
//   custom "pacer_reclaimer" that, instead of freeing immediately,
//   calls back into Rust via the retire_cb function pointer that was
//   registered for this thread.
//
// The retire callback is:
//   extern "C" fn retire_cb(birth_ts: u64, ptr: *mut u8, ctx: *mut u8)
// where ctx is the opaque ThreadState pointer Rust passes in.

#include "brown_ext_abtree_lf_impl.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>

// ── Retire callback type (matches abtree_ffi.rs) ─────────────────────────────

// Called by the PacerReclaimer when the ABtree logically retires a node.
// birth_ts : the epoch stamp we attached when the node was allocated
// ptr      : raw pointer to the retired node (do NOT free here — PACER does it)
// ctx      : opaque pointer to the Rust ThreadState for this thread
typedef void (*RetireFn)(uint64_t birth_ts, void* ptr, void* ctx);

// Per-thread registration: Rust sets this before calling insert/remove.
// We use thread_local so each OS thread has its own slot.
thread_local static RetireFn  tl_retire_fn  = nullptr;
thread_local static void*     tl_retire_ctx = nullptr;
thread_local static uint64_t  tl_epoch_ptr  = 0;   // current epoch, set by Rust

// ── Node wrapper ──────────────────────────────────────────────────────────────
// We prefix every ABtree node with a birth_ts so the retire callback can
// pass it to PACER.  The ABtree sees a node of size sizeof(NodeWrapper)
// but only uses the [key, value] portion; the birth_ts sits at the front.

struct NodeWrapper {
    uint64_t birth_ts;
    // The ABtree's internal node data follows here; we don't need to
    // name its type because the ABtree allocates/owns it internally.
    // birth_ts is stored at offset 0, which we recover in the retire hook.
};

// ── PACER reclaimer (drop-in replacement for debra2) ─────────────────────────
//
// Brown's ABtree template parameter "Reclaimer" must provide:
//   void retire(T* ptr, void(*deleter)(T*))
//   void quiescent_state()
//   void begin_op()
//   void end_op()
//
// We only implement retire(); the rest are no-ops because PACER handles
// epoch tracking on the Rust side via the announcement table.

struct PacerReclaimer {
    // Called by the ABtree whenever it logically removes a node.
    template <typename T>
    static void retire(T* ptr, void(*)(T*)) {
        if (!tl_retire_fn) {
            // No Rust callback registered — fall back to immediate free.
            // This should only happen during warm-up before threads register.
            ::operator delete(ptr);
            return;
        }
        // Recover birth_ts from the NodeWrapper prefix.
        // The ABtree allocates nodes via its Allocator; we hook the allocator
        // below to embed birth_ts at offset 0 of every node.
        uint64_t birth_ts = *reinterpret_cast<uint64_t*>(ptr);
        tl_retire_fn(birth_ts, reinterpret_cast<void*>(ptr), tl_retire_ctx);
    }

    static void quiescent_state() {}
    static void begin_op()        {}
    static void end_op()          {}
    static void init()            {}
    static void deinit()          {}
};

// ── PACER allocator ───────────────────────────────────────────────────────────
//
// Brown's Allocator template parameter must provide:
//   T* allocate(int tid, ...)
//   void deallocate(T* ptr)
//
// We intercept allocate() to stamp birth_ts at offset 0 of the node.
// The epoch value is read from tl_epoch_ptr (Rust updates this each op).

struct PacerAllocator {
    template <typename T>
    static T* allocate(int /*tid*/) {
        void* raw = ::operator new(sizeof(T));
        // Stamp birth_ts at the first 8 bytes.
        *reinterpret_cast<uint64_t*>(raw) = tl_epoch_ptr;
        return reinterpret_cast<T*>(raw);
    }

    template <typename T>
    static void deallocate(T* ptr) {
        ::operator delete(ptr);
    }
};

// ── ABtree instantiation ──────────────────────────────────────────────────────
//
// Brown's header uses:
//   abtree<K, V, Reclaimer, Allocator, Pool>
//
// K = long (64-bit key), V = long (64-bit value — we store key as value too)
// We use the default Pool (none) since PACER manages memory.

using Tree = abtree<long, long, PacerReclaimer, PacerAllocator>;

// ── C ABI exposed to Rust ─────────────────────────────────────────────────────

extern "C" {

// Create a new ABtree instance on the heap.
// num_threads: maximum number of concurrent threads (for internal sizing).
Tree* abtree_create(int num_threads) {
    return new Tree(num_threads);
}

// Destroy the tree and free all remaining nodes immediately.
// Call only after all threads have finished — no concurrent access.
void abtree_destroy(Tree* tree) {
    delete tree;
}

// Register the retire callback and epoch source for the calling thread.
// Must be called once per thread before any insert/remove on that thread.
//   retire_fn  : Rust function that receives (birth_ts, ptr, ctx)
//   retire_ctx : opaque pointer passed back as ctx (Rust ThreadState ptr)
//   epoch_ref  : pointer to the u64 epoch that Rust increments each write
void abtree_register_thread(RetireFn retire_fn, void* retire_ctx,
                             uint64_t* epoch_ref) {
    tl_retire_fn  = retire_fn;
    tl_retire_ctx = retire_ctx;
    tl_epoch_ptr  = (epoch_ref ? *epoch_ref : 0);
}

// Update the thread-local epoch snapshot (call before each write op).
void abtree_set_epoch(uint64_t epoch) {
    tl_epoch_ptr = epoch;
}

// Insert key->key into the tree. Returns 1 if inserted, 0 if already present.
int abtree_insert(Tree* tree, int tid, long key) {
    return tree->insert(tid, key, key) ? 1 : 0;
}

// Remove key from the tree. Returns 1 if removed, 0 if not found.
// The removed node's retire callback fires inside this call.
int abtree_remove(Tree* tree, int tid, long key) {
    return tree->erase(tid, key) ? 1 : 0;
}

// Returns 1 if key is present, 0 otherwise. Read-only — no retire.
int abtree_contains(Tree* tree, int tid, long key) {
    long val = 0;
    return tree->find(tid, key, &val) ? 1 : 0;
}

// Frees a node retired via the RetireFn callback. Nodes are allocated in
// PacerAllocator::allocate() with ::operator new, so they must be released
// with ::operator delete here — not with Rust's global allocator, which
// knows nothing about this heap. Called from LimboBag::drain on the Rust
// side once safe_reclaim_ts has passed the node's birth_ts.
void abtree_free_node(void* ptr) {
    ::operator delete(ptr);
}

} // extern "C"