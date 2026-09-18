import csv, statistics as s, collections, sys
rows = collections.defaultdict(list)
for r in csv.DictReader(open(sys.argv[1])):
    if r['mops'] == '':
        rows[(r['cfg'], r['algo'])].append(None); continue
    rows[(r['cfg'], r['algo'])].append({k: float(v) for k, v in r.items() if k not in ('cfg', 'algo', 'rep', 'rc')})
cfgs = list(dict.fromkeys(k[0] for k in rows)); algos = list(dict.fromkeys(k[1] for k in rows))
def m(vals, key):
    v = [x[key] for x in vals if x]; return s.mean(v) if v else float('nan')
def sd(vals, key):
    v = [x[key] for x in vals if x]; return s.stdev(v) if len(v) > 1 else 0
for cfg in cfgs:
    base = m(rows[(cfg, algos[0])], 'mops')
    print(f'== {cfg}')
    print(f'  {"algo":6} {"Mops/s":>13} {"vs K1":>6} {"avgBacklog":>10} {"maxThrPeak":>10} {"sumPeaks":>9} '
          f'{"p50ns":>6} {"p99ns":>7} {"p99.9ns":>9} {"p99.99ns":>10} {"maxns(ms)":>10} crashes')
    for a in algos:
        v = rows[(cfg, a)]; bad = sum(1 for x in v if x is None)
        print(f'  {a:6} {m(v,"mops"):6.2f}±{sd(v,"mops"):4.2f} {m(v,"mops")/base:5.2f}x '
              f'{m(v,"avg_backlog"):10.0f} {m(v,"max_thread_peak"):10.0f} {m(v,"sum_peaks"):9.0f} '
              f'{m(v,"p50_ns"):6.0f} {m(v,"p99_ns"):7.0f} {m(v,"p999_ns"):9.0f} {m(v,"p9999_ns"):10.0f} '
              f'{m(v,"max_ns")/1e6:10.1f} {bad}')
    print()
