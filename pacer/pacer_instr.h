// pacer_instr.h — measurement hooks for reclaimers (define PACER_INSTR to enable).
//
// Measures what throughput alone can't show:
//   * backlog: nodes retired but not yet returned to the allocator
//     (retired - freed), sampled every 256 startOps -> peak and time-average
//   * reclamation latency: cycles spent inside startOp (rotation + frees),
//     as a quarter-octave log histogram, so tail stalls (batch frees) show up
// Each thread prints one "INSTR ..." line to stderr from deinitThread.
// Prefill threads also print; consumers should take the LAST (work+rq) lines.
#pragma once
#include <cstdio>
#include <cstdint>
#include <x86intrin.h>
#include <unistd.h>

namespace pinstr {

struct ThreadStats {
    uint64_t retired = 0, freed = 0, ops = 0, peak = 0, sum = 0, samples = 0, maxcyc = 0;
    uint64_t hist[256];
    ThreadStats() { for (auto& h : hist) h = 0; }
};
inline thread_local ThreadStats ts;

// quarter-octave buckets: d<4 -> d; else (msb-2)*4 + 4 + next-two-bits
inline int bucket(uint64_t d) {
    if (d < 4) return (int)d;
    int msb = 63 - __builtin_clzll(d);
    return (msb - 2) * 4 + 4 + (int)((d >> (msb - 2)) & 3);
}

struct Timer {
    uint64_t t0;
    Timer() : t0(__rdtsc()) {}
    ~Timer() {
        uint64_t d = __rdtsc() - t0;
        ThreadStats& s = ts;
        s.hist[bucket(d)]++;
        if (d > s.maxcyc) s.maxcyc = d;
        if ((++s.ops & 255) == 0) {
            uint64_t b = s.retired > s.freed ? s.retired - s.freed : 0;
            if (b > s.peak) s.peak = b;
            s.sum += b;
            s.samples++;
        }
    }
};

inline void on_retire()              { ++ts.retired; }
inline void on_free(uint64_t n = 1)  { ts.freed += n; }

inline void report(int tid) {
    ThreadStats& s = ts;
    // One buffer, one write(2): threads deinit concurrently, and separate
    // fprintf calls interleave their pieces into unparseable lines.
    static thread_local char buf[8192];
    int n = snprintf(buf, sizeof buf,
            "INSTR tid=%d ops=%llu retired=%llu freed=%llu peak=%llu avg=%.1f maxcyc=%llu hist=",
            tid, (unsigned long long)s.ops, (unsigned long long)s.retired, (unsigned long long)s.freed,
            (unsigned long long)s.peak, s.samples ? (double)s.sum / s.samples : 0.0,
            (unsigned long long)s.maxcyc);
    for (int i = 0; i < 256 && n < (int)sizeof buf - 40; ++i)
        if (s.hist[i]) n += snprintf(buf + n, sizeof buf - n, "%d:%llu,", i, (unsigned long long)s.hist[i]);
    buf[n++] = '\n';
    if (write(2, buf, n) < 0) { /* best effort */ }
}

} // namespace pinstr

#ifdef PACER_INSTR
#  define PI_TIMER        ::pinstr::Timer _pi_timer;
#  define PI_RETIRE       ::pinstr::on_retire();
#  define PI_FREE         ::pinstr::on_free();
#  define PI_REPORT(tid)  ::pinstr::report(tid);
#else
#  define PI_TIMER
#  define PI_RETIRE
#  define PI_FREE
#  define PI_REPORT(tid)
#endif
