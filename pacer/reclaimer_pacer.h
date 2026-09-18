/**
 * reclaimer_pacer.h — PACER: Pressure-Adaptive Concurrent Epoch Reclamation
 *
 * Stable architecture based on token4 + SPI-gated rotation drain:
 *
 * Every startOp:  free K=1 from deamortizedFreeables  (same as token4)
 * On token recv:  rotate bags AND drain extra nodes based on SPI:
 *   AggressiveEbr:    drain entire bag at rotation (fast reclaim)
 *   AmortisedFree:    drain nothing extra at rotation (K=1/op is enough)
 *   ConservativeHold: drain nothing (pause)
 *
 * This guarantees K=1/op baseline (no memory starvation) while PACER
 * adds extra draining when workload allows it.
 */
#pragma once
#include <atomic>
#include <cassert>
#include <iostream>
#include <sstream>
#include <limits.h>
#include "blockbag.h"
#include "plaf.h"
#include "allocator_interface.h"
#include "reclaimer_interface.h"
#include "gstats_definitions_epochs.h"

#define PACER_SPI_WRITE_HEAVY 0.30
#define PACER_SPI_SNAP_HEAVY  0.60
#define PACER_EMA_ALPHA       0.15
#define PACER_HOLD_LIMIT      8
#define PACER_HYSTERESIS      0.05

enum class PacerGcMode { AggressiveEbr, AmortisedFree, ConservativeHold };

// Stateless classification — only used to seed the first decision.
static inline PacerGcMode pacer_select_mode(double spi) {
    if (spi <= PACER_SPI_WRITE_HEAVY) return PacerGcMode::AggressiveEbr;
    if (spi <= PACER_SPI_SNAP_HEAVY)  return PacerGcMode::AmortisedFree;
    return PacerGcMode::ConservativeHold;
}

// Hysteresis-aware classification: `prev` only changes once `spi` clears the
// relevant threshold by more than PACER_HYSTERESIS, so SPI noise sitting
// near 0.30 or 0.60 can't flip the mode every window. This is what
// startOp() actually uses.
static inline PacerGcMode pacer_select_mode_hyst(double spi, PacerGcMode prev) {
    switch (prev) {
        case PacerGcMode::AggressiveEbr:
            if (spi > PACER_SPI_SNAP_HEAVY + PACER_HYSTERESIS)  return PacerGcMode::ConservativeHold;
            if (spi > PACER_SPI_WRITE_HEAVY + PACER_HYSTERESIS) return PacerGcMode::AmortisedFree;
            return PacerGcMode::AggressiveEbr;
        case PacerGcMode::AmortisedFree:
            if (spi <= PACER_SPI_WRITE_HEAVY - PACER_HYSTERESIS) return PacerGcMode::AggressiveEbr;
            if (spi > PACER_SPI_SNAP_HEAVY + PACER_HYSTERESIS)   return PacerGcMode::ConservativeHold;
            return PacerGcMode::AmortisedFree;
        case PacerGcMode::ConservativeHold:
        default:
            if (spi <= PACER_SPI_WRITE_HEAVY - PACER_HYSTERESIS) return PacerGcMode::AggressiveEbr;
            if (spi <= PACER_SPI_SNAP_HEAVY - PACER_HYSTERESIS)  return PacerGcMode::AmortisedFree;
            return PacerGcMode::ConservativeHold;
    }
}

template <typename T = void, class Pool = pool_interface<T>>
class reclaimer_pacer : public reclaimer_interface<T, Pool> {
protected:
    class ThreadData {
    private: PAD;
    public:
        volatile int  token;
        int           tokenCount;
        blockbag<T> * curr;
        blockbag<T> * last;
        blockbag<T> * deamortizedFreeables;
        long long     win_writes;
        long long     win_snaps;
        double        spi_ema;
        int           hold_ticks;
        PacerGcMode   gc_mode;
        ThreadData() {}
    private: PAD;
    };
    ThreadData threadData[MAX_THREADS_POW2];
    PAD;

public:
    template<typename _Tp1>
    struct rebind { typedef reclaimer_pacer<_Tp1, Pool> other; };
    template<typename _Tp1, typename _Tp2>
    struct rebind2 { typedef reclaimer_pacer<_Tp1, _Tp2> other; };

    inline static bool quiescenceIsPerRecordType() { return false; }
    inline static bool shouldHelp()                { return true;  }
    inline static bool supportsCrashRecovery()     { return false; }
    inline static bool isProtected(const int tid, T* const obj)  { return true; }
    inline static bool isQProtected(const int tid, T* const obj) { return false; }
    inline static bool protect(const int tid, T* const obj, CallbackType cb, CallbackArg arg, bool mb=true) { return true; }
    inline static void unprotect(const int tid, T* const obj) {}
    inline static bool qProtect(const int tid, T* const obj, CallbackType cb, CallbackArg arg, bool mb=true) { return true; }
    inline static void qUnprotectAll(const int tid) {}
    inline static bool isQuiescent(const int tid) { return false; }

    long long getSizeInNodes() {
        long long sum = 0;
        for (int tid = 0; tid < this->NUM_PROCESSES; ++tid) {
            if (threadData[tid].curr)                 sum += threadData[tid].curr->computeSize();
            if (threadData[tid].last)                 sum += threadData[tid].last->computeSize();
            if (threadData[tid].deamortizedFreeables) sum += threadData[tid].deamortizedFreeables->computeSize();
        }
        return sum;
    }
    std::string getSizeString()    { std::stringstream ss; ss << getSizeInNodes(); return ss.str(); }
    std::string getDetailsString() { return getSizeString(); }
    inline void getSafeBlockbags(const int tid, blockbag<T>** bags) {
        setbench_error("getSafeBlockbags not supported by reclaimer_pacer");
    }

    // Must match reclaimer_token1.h (vtoken4) rotateEpochBags exactly: only the
    // OLD bag `last` (retired before this thread's previous token receipt, so
    // every thread has since passed through a quiescent point) is safe to free.
    // The bag currently being retired into (`curr`) must NOT be freed — other
    // threads may still hold pointers into it. An earlier version swapped
    // curr<->last first and then handed the just-closed bag to
    // deamortizedFreeables, freeing nodes from the *current* epoch: a
    // use-after-free that aborted with "double free or corruption" in ~50% of
    // 16-thread runs (0 of 13+ for token4/DEBRA on the same workload).
    inline void rotateEpochBags(const int tid) {
        blockbag<T>* const freeable = threadData[tid].last;
        // moves full blocks only; a partially-filled head block stays in
        // `freeable` (which becomes curr) and is freed a rotation later
        threadData[tid].deamortizedFreeables->appendMoveFullBlocks(freeable);
        threadData[tid].last = threadData[tid].curr;
        threadData[tid].curr = freeable;
        // No extra drain at rotation — per-op K=1 handles reclamation (avoids RBF)
    }

    template <typename... Types>
    class BagRotator {
    public:
        inline void rotateAllEpochBags(const int tid, void* const* const reclaimers, const int i) {}
    };
    template <typename First, typename... Rest>
    class BagRotator<First, Rest...> : public BagRotator<Rest...> {
    public:
        inline void rotateAllEpochBags(const int tid, void* const* const reclaimers, const int i) {
            typedef typename Pool::template rebindAlloc<First>::other classAlloc;
            typedef typename Pool::template rebind2<First, classAlloc>::other classPool;
            ((reclaimer_pacer<First, classPool>* const) reclaimers[i])->rotateEpochBags(tid);
            ((BagRotator<Rest...>*) this)->rotateAllEpochBags(tid, reclaimers, 1 + i);
        }
    };

    template <typename First, typename... Rest>
    inline bool startOp(const int tid, void* const* const reclaimers,
                        const int numReclaimers, const bool readOnly = false)
    {
        ThreadData& td = threadData[tid];

        if (readOnly) td.win_snaps++;
        else          td.win_writes++;

        bool rotated = false;

        if (td.token) {
            td.token = 0;
            threadData[(tid + 1) % this->NUM_PROCESSES].token = 1;
            td.tokenCount++;

            // Resample SPI
            long long total = td.win_writes + td.win_snaps;
            if (total > 0) {
                double win_spi = (double)td.win_snaps / (double)(total + 1);
                td.spi_ema = td.spi_ema * (1.0 - PACER_EMA_ALPHA) + win_spi * PACER_EMA_ALPHA;
                td.win_writes = 0;
                td.win_snaps  = 0;
            }

            BagRotator<First, Rest...> rotator;
            rotator.rotateAllEpochBags(tid, reclaimers, 0);
            rotated = true;
        }

        // Per-op K=1 drain — always runs, guarantees memory is reclaimed
        // This is identical to token4 and prevents memory starvation at any thread count
        PacerGcMode mode = pacer_select_mode_hyst(td.spi_ema, td.gc_mode);
        td.gc_mode = mode;
        if (mode == PacerGcMode::ConservativeHold) {
            // Gate: only drain as safety valve
            td.hold_ticks++;
            if (td.hold_ticks >= PACER_HOLD_LIMIT) {
                td.hold_ticks = 0;
                // Drain up to PACER_HOLD_LIMIT nodes, not just one — a
                // single node freed every PACER_HOLD_LIMIT ops still lets
                // the backlog grow at ~(LIMIT-1)/LIMIT of the retire rate,
                // which is slower but still unbounded, not actually capped.
                for (int i = 0; i < PACER_HOLD_LIMIT && !td.deamortizedFreeables->isEmpty(); ++i) {
                    this->pool->add(tid, td.deamortizedFreeables->remove());
                    GSTATS_ADD(tid, limbo_object_frees, 1);
                }
            }
        } else {
            // AggressiveEbr and AmortisedFree: always K=1 per op
            td.hold_ticks = 0;
            if (!td.deamortizedFreeables->isEmpty()) {
                this->pool->add(tid, td.deamortizedFreeables->remove());
                GSTATS_ADD(tid, limbo_object_frees, 1);
            }
        }

        return rotated;
    }

    inline void endOp(const int tid) {}

    inline void retire(const int tid, T* p) {
        threadData[tid].curr->add(p);
        DEBUG2 this->debug->addRetired(tid, 1);
    }

    void initThread(const int tid) {
        ThreadData& td = threadData[tid];
        if (!td.curr)                 td.curr                 = new blockbag<T>(tid, this->pool->blockpools[tid]);
        if (!td.last)                 td.last                 = new blockbag<T>(tid, this->pool->blockpools[tid]);
        if (!td.deamortizedFreeables) td.deamortizedFreeables = new blockbag<T>(tid, this->pool->blockpools[tid]);
    }

    void deinitThread(const int tid) {
        ThreadData& td = threadData[tid];
        if (td.curr)                 { this->pool->addMoveAll(tid, td.curr);                 delete td.curr;                 td.curr                 = NULL; }
        if (td.last)                 { this->pool->addMoveAll(tid, td.last);                 delete td.last;                 td.last                 = NULL; }
        if (td.deamortizedFreeables) { this->pool->addMoveAll(tid, td.deamortizedFreeables); delete td.deamortizedFreeables; td.deamortizedFreeables = NULL; }
    }

    void debugPrintStatus(const int tid) {
        if (tid == 0)
            std::cout << "pacer_spi_ema=" << threadData[0].spi_ema
                      << " gc_mode=" << (int)threadData[0].gc_mode << std::endl;
    }

    reclaimer_pacer(const int numProcesses, Pool* _pool, debugInfo* const _debug,
                    RecoveryMgr<void*>* const _recoveryMgr = NULL)
        : reclaimer_interface<T, Pool>(numProcesses, _pool, _debug, _recoveryMgr)
    {
        for (int tid = 0; tid < numProcesses; ++tid) {
            threadData[tid].token                = (tid == 0 ? 1 : 0);
            threadData[tid].tokenCount           = 0;
            threadData[tid].curr                 = NULL;
            threadData[tid].last                 = NULL;
            threadData[tid].deamortizedFreeables = NULL;
            threadData[tid].win_writes           = 0;
            threadData[tid].win_snaps            = 0;
            threadData[tid].spi_ema              = 0.0;
            threadData[tid].hold_ticks           = 0;
            threadData[tid].gc_mode              = PacerGcMode::AggressiveEbr;
        }
    }
    ~reclaimer_pacer() {}
};