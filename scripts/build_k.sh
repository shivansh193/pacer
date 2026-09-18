#!/bin/bash
set -e
cd /root/setbench/common/recordmgr
[ -f reclaimer_token1.h.orig ] || cp reclaimer_token1.h reclaimer_token1.h.orig
python3 - <<'EOF'
import re
src = open('reclaimer_token1.h.orig').read()
old = """    if (!threadData[tid].deamortizedFreeables->isEmpty()) {
        this->pool->add(tid, threadData[tid].deamortizedFreeables->remove());
        GSTATS_ADD(tid, limbo_object_frees, 1);
    }
"""
new = """    for (int kk = 0; kk < KFREE && !threadData[tid].deamortizedFreeables->isEmpty(); ++kk) {
        this->pool->add(tid, threadData[tid].deamortizedFreeables->remove());
        GSTATS_ADD(tid, limbo_object_frees, 1);
    }
"""
assert src.count(old) == 1, src.count(old)
out = "#ifndef KFREE\n#define KFREE 1\n#endif\n" + src.replace(old, new)
open('reclaimer_token1.h', 'w').write(out)
print("patched")
EOF
cd /root/setbench/microbench
COMMON=(-I../ds/brown_ext_abtree_lf -I../common -I../common/recordmgr -I../common/rq
        -I../common/papi -I../common/descriptors
        -DDS_TYPENAME=brown_ext_abtree_lf -DMAX_THREADS_POW2=512 -DCPU_FREQ_GHZ=2.1
        "-DMEMORY_STATS=if(1)" "-DMEMORY_STATS2=if(0)" -DNDEBUG -DUSE_TREE_STATS
        -std=c++17 -O3 -mcx16 -fopenmp -gdwarf -fno-omit-frame-pointer -L../lib -lpthread)
for K in 1 2 4 8 16 64; do
  ( g++ ./main.cpp -o bin/brown_ext_abtree_lf.k$K -DRECLAIM_TYPE=token1 -Dvtoken4 -DKFREE=$K "${COMMON[@]}" 2>&1 | grep -E "error" | head -3 ) &
done
wait
ls bin | grep '\.k'
