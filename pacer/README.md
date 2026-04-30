cat > /mnt/c/Users/shivansh193/Desktop/README.md << 'EOF'
# PACER — Reproduction Instructions

## Prerequisites
- Linux (or WSL2 on Windows)
- g++ with C++17 support
- git

## Setup (one-time)

```bash
git clone https://gitlab.com/trbot86/setbench.git
cd setbench
git submodule update --init --recursive
cp reclaimer_pacer.h common/recordmgr/reclaimer_pacer.h
```

## Build

```bash
cd microbench

# PACER (our algorithm)
g++ ./main.cpp -o bin/brown_ext_abtree_lf.pacer \
  -I../ds/brown_ext_abtree_lf -I../common -I../common/recordmgr \
  -I../common/rq -I../common/papi -I../common/descriptors \
  -DDS_TYPENAME=brown_ext_abtree_lf -DRECLAIM_TYPE=pacer \
  -DMAX_THREADS_POW2=512 -DCPU_FREQ_GHZ=2.1 \
  "-DMEMORY_STATS=if(1)" "-DMEMORY_STATS2=if(0)" \
  -DNDEBUG -DDEBRA_ORIGINAL_FREE -DUSE_TREE_STATS \
  -std=c++17 -O3 -mcx16 -fopenmp -gdwarf -fno-omit-frame-pointer \
  -L../lib -lpthread

# token4 baseline (Kim et al. 2024)
g++ ./main.cpp -o bin/brown_ext_abtree_lf.token4 \
  -I../ds/brown_ext_abtree_lf -I../common -I../common/recordmgr \
  -I../common/rq -I../common/papi -I../common/descriptors \
  -DDS_TYPENAME=brown_ext_abtree_lf -DRECLAIM_TYPE=token1 -Dvtoken4 \
  -DMAX_THREADS_POW2=512 -DCPU_FREQ_GHZ=2.1 \
  "-DMEMORY_STATS=if(1)" "-DMEMORY_STATS2=if(0)" \
  -DNDEBUG -DUSE_TREE_STATS \
  -std=c++17 -O3 -mcx16 -fopenmp -gdwarf -fno-omit-frame-pointer \
  -L../lib -lpthread

# DEBRA baseline
g++ ./main.cpp -o bin/brown_ext_abtree_lf.debra \
  -I../ds/brown_ext_abtree_lf -I../common -I../common/recordmgr \
  -I../common/rq -I../common/papi -I../common/descriptors \
  -DDS_TYPENAME=brown_ext_abtree_lf -DRECLAIM_TYPE=debra \
  -DMAX_THREADS_POW2=512 -DCPU_FREQ_GHZ=2.1 \
  "-DMEMORY_STATS=if(1)" "-DMEMORY_STATS2=if(0)" \
  -DNDEBUG -DDEBRA_ORIGINAL_FREE -DUSE_TREE_STATS \
  -std=c++17 -O3 -mcx16 -fopenmp -gdwarf -fno-omit-frame-pointer \
  -L../lib -lpthread
```

## Run

```bash
for threads in 1 2 4 8 16; do
  for binary in debra token4 pacer; do
    echo -n "=== $binary T=$threads === "
    ./bin/brown_ext_abtree_lf.$binary \
      -nwork $threads -insdel 50 50 -k 20000000 -t 5000 \
      2>/dev/null | grep "total_ops="
    echo ""
  done
done
```

## What to expect
PACER should outperform token4 by 10-22% at all thread counts.
EOF