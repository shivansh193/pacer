#!/bin/bash
# Usage: ALGOS="debra token4 pacer" run_setbench.sh <outfile> <repeats> <ins> <del>
OUT=${1:-/root/results.csv}
REPS=${2:-5}
INS=${3:-50}
DEL=${4:-50}
ALGOS=${ALGOS:-"debra token4 pacer"}
cd /root/setbench/microbench
echo "algo,threads,rep,total_ops,mops,rc" > "$OUT"
for rep in $(seq 1 $REPS); do
  for t in 1 2 4 8 16; do
    for algo in $ALGOS; do
      res=$(./bin/brown_ext_abtree_lf.$algo -nwork $t -insdel $INS $DEL -k 20000000 -t 5000 2>/dev/null)
      rc=$?
      ops=$(echo "$res" | grep -oP 'total_ops=\K[0-9]+' | head -1)
      if [ -n "$ops" ]; then mops=$(python3 -c "print(round($ops/5/1e6,4))"); else mops=""; fi
      echo "$algo,$t,$rep,$ops,$mops,$rc" >> "$OUT"
    done
  done
  echo "rep $rep done" >&2
done
echo DONE >&2
