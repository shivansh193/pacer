import csv, statistics as s, collections, sys
d = collections.defaultdict(lambda: collections.defaultdict(list))
fails = collections.Counter()
for r in csv.DictReader(open(sys.argv[1])):
    key = (r['cfg'], int(r['K']))
    if r['rc'] != '0' or not r['total_ops']:
        fails[key] += 1; continue
    secs = 5.0
    d[key]['tot'].append(int(r['total_ops']) / secs / 1e6)
    d[key]['upd'].append(int(r['updates']) / secs / 1e6)
    d[key]['rss'].append(float(r['peak_rss_mb']))
cfgs = []
for k in d:
    if k[0] not in cfgs: cfgs.append(k[0])
Ks = sorted({k[1] for k in d})
for cfg in cfgs:
    print(f'== {cfg}  (Mops/s total | updates-only ; peak RSS MB)')
    base = s.mean(d[(cfg, 1)]['tot'])
    best = max(Ks, key=lambda k: s.mean(d[(cfg, k)]['tot']))
    for K in Ks:
        t = d[(cfg, K)]['tot']; u = d[(cfg, K)]['upd']; m = d[(cfg, K)]['rss']
        sd = s.stdev(t) if len(t) > 1 else 0
        flag = ' <- best' if K == best else ''
        print(f'  K={K:<3} {s.mean(t):6.2f} ± {sd:4.2f} ({s.mean(t)/base:4.2f}x)  upd {s.mean(u):6.2f}  rss {s.mean(m):6.0f}  n={len(t)}{flag}')
    print()
