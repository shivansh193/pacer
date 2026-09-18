#!/bin/bash
# Usage: build_variant.sh <header_path> <binary_suffix> [extra -D flags...]
# Builds setbench microbench with the given header installed as reclaimer_pacer.h.
set -e
HDR=$1; NAME=$2; shift 2
cd /root/setbench
cp "$HDR" common/recordmgr/reclaimer_pacer.h
cd microbench
g++ ./main.cpp -o bin/brown_ext_abtree_lf.$NAME "$@" \
  -I../ds/brown_ext_abtree_lf -I../common -I../common/recordmgr -I../common/rq \
  -I../common/papi -I../common/descriptors \
  -DDS_TYPENAME=brown_ext_abtree_lf -DRECLAIM_TYPE=pacer -DDEBRA_ORIGINAL_FREE \
  -DMAX_THREADS_POW2=512 -DCPU_FREQ_GHZ=2.1 "-DMEMORY_STATS=if(1)" "-DMEMORY_STATS2=if(0)" \
  -DNDEBUG -DUSE_TREE_STATS -std=c++17 -O3 -mcx16 -fopenmp -gdwarf -fno-omit-frame-pointer \
  -L../lib -lpthread 2>&1 | grep -E "error" | head -5 || true
ls -la bin/brown_ext_abtree_lf.$NAME
