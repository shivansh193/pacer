#!/bin/bash
# Builds instrumented token4 (K variants) and PACER binaries: bin/i.k<K>, bin/i.pacer,
# and RQ-tree versions bin/r.k<K>, bin/r.pacer (if they compile).
set -e
REPO=/mnt/c/Development/tinkeringwrust/pacer
RM=/root/setbench/common/recordmgr
cp $REPO/pacer/pacer_instr.h $RM/pacer_instr.h
cd $RM
[ -f reclaimer_token1.h.orig ] || cp reclaimer_token1.h reclaimer_token1.h.orig

python3 - <<'EOF'
import re
RM='/root/setbench/common/recordmgr'
REPO='/mnt/c/Development/tinkeringwrust/pacer'

def patch_common(src, is_token):
    src = src.replace('GSTATS_ADD(tid, limbo_object_frees, 1);',
                      'GSTATS_ADD(tid, limbo_object_frees, 1); PI_FREE')
    assert 'PI_FREE' in src
    n = src.count('threadData[tid].curr->add(p);\n        DEBUG2 this->debug->addRetired(tid, 1);')
    assert n == 1, n
    src = src.replace('threadData[tid].curr->add(p);\n        DEBUG2 this->debug->addRetired(tid, 1);',
                      'threadData[tid].curr->add(p);\n        PI_RETIRE\n        DEBUG2 this->debug->addRetired(tid, 1);')
    assert src.count('void deinitThread(const int tid) {') == 1
    src = src.replace('void deinitThread(const int tid) {', 'void deinitThread(const int tid) {\n        PI_REPORT(tid)')
    # startOp: insert timer as first statement of body
    m = re.search(r'inline bool startOp\([^{]*?\)\s*\{', src, re.S)
    assert m, 'startOp not found'
    src = src[:m.end()] + '\n        PI_TIMER' + src[m.end():]
    return '#include "pacer_instr.h"\n' + src

# --- token4 with K free per op ---
src = open(RM + '/reclaimer_token1.h.orig').read()
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
assert src.count(old) == 1
src = "#ifndef KFREE\n#define KFREE 1\n#endif\n" + src.replace(old, new)
open(RM + '/reclaimer_token1.h', 'w').write(patch_common(src, True))

# --- PACER (repo version) ---
p = open(REPO + '/pacer/reclaimer_pacer.h').read()
open(RM + '/reclaimer_pacer.h', 'w').write(patch_common(p, False))
print('patched token1 + pacer')
EOF

cd /root/setbench/microbench
mkdir -p bin
mk() { # dsdir dstype outname extra...
  local ds=$1 ty=$2 out=$3; shift 3
  g++ ./main.cpp -o bin/$out "$@" -DPACER_INSTR \
    -I../ds/$ds -I../common -I../common/recordmgr -I../common/rq -I../common/papi -I../common/descriptors \
    -DDS_TYPENAME=$ty -DMAX_THREADS_POW2=512 -DCPU_FREQ_GHZ=2.1 "-DMEMORY_STATS=if(1)" "-DMEMORY_STATS2=if(0)" \
    -DNDEBUG -DUSE_TREE_STATS -std=c++17 -O3 -mcx16 -fopenmp -gdwarf -fno-omit-frame-pointer -L../lib -lpthread 2>&1 \
    | grep -E "error" | head -3 || true
}
for K in 1 4 16 64; do
  mk brown_ext_abtree_lf    brown_ext_abtree_lf    i.k$K -DRECLAIM_TYPE=token1 -Dvtoken4 -DKFREE=$K &
  mk brown_ext_abtree_rq_lf brown_ext_abtree_rq_lf r.k$K -DRECLAIM_TYPE=token1 -Dvtoken4 -DKFREE=$K &
done
mk brown_ext_abtree_lf    brown_ext_abtree_lf    i.pacer -DRECLAIM_TYPE=pacer -DDEBRA_ORIGINAL_FREE &
mk brown_ext_abtree_rq_lf brown_ext_abtree_rq_lf r.pacer -DRECLAIM_TYPE=pacer -DDEBRA_ORIGINAL_FREE &
wait
ls bin | grep -E '^(i|r)\.'
