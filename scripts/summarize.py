import csv, statistics as s, collections, sys
path = sys.argv[1]
d = collections.defaultdict(list)
fail = collections.Counter()
tot = collections.Counter()
algos = []
for r in csv.DictReader(open(path)):
    k = (int(r['threads']), r['algo'])
    if r['algo'] not in algos: algos.append(r['algo'])
    tot[k] += 1
    if r['mops'] == '':
        fail[k] += 1
    else:
        d[k].append(float(r['mops']))
base = 'token4'
print('T   algo         mean ± sd     ok/n   vs-debra  vs-token4')
for t in sorted({k[0] for k in tot}):
    m = {a: s.mean(d[(t, a)]) for a in algos if d[(t, a)]}
    for a in algos:
        v = d[(t, a)]
        if not v:
            print(f'{t:<3} {a:<11} ALL {tot[(t,a)]} RUNS CRASHED'); continue
        sd = s.stdev(v) if len(v) > 1 else 0
        print(f'{t:<3} {a:<11} {m[a]:6.3f} ± {sd:5.3f}  {len(v)}/{tot[(t,a)]}   {m[a]/m["debra"]:.2f}x     {m[a]/m[base]:.2f}x')
    print()
