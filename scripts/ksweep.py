import os, re, subprocess, sys, csv, time
os.chdir('/root/setbench/microbench')
out = sys.argv[1]
reps = int(sys.argv[2])
Ks = [int(x) for x in sys.argv[3].split(',')]
only = sys.argv[4].split(',') if len(sys.argv) > 4 else None
CONFIGS = {
    'write_8':   ['-nwork', '8',  '-insdel', '50', '50'],
    'write_16':  ['-nwork', '16', '-insdel', '50', '50'],
    'read80_8':  ['-nwork', '8',  '-insdel', '10', '10'],
    'read80_16': ['-nwork', '16', '-insdel', '10', '10'],
    'rq_8':      ['-nwork', '6',  '-nrq', '2', '-rqsize', '10000', '-insdel', '50', '50'],
    'rq_16':     ['-nwork', '12', '-nrq', '4', '-rqsize', '10000', '-insdel', '50', '50'],
}
def num(pat, s):
    m = re.search(pat + r'=(\d+)', s)
    return int(m.group(1)) if m else None
def run(k, cfg):
    binary = f'./bin/rq.k{k}' if cfg.startswith('rq') else f'./bin/brown_ext_abtree_lf.k{k}'
    cmd = [binary] + CONFIGS[cfg] + ['-k', '20000000', '-t', '5000']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    stdout = p.stdout.read()
    _, status, ru = os.wait4(p.pid, 0)
    rc = os.waitstatus_to_exitcode(status)
    return rc, stdout, ru.ru_maxrss / 1024
w = csv.writer(open(out, 'w'), lineterminator='\n')
w.writerow(['cfg', 'K', 'rep', 'rc', 'total_ops', 'updates', 'rqs', 'searches', 'peak_rss_mb'])
for rep in range(1, reps + 1):
    for cfg in CONFIGS:
        if only and cfg not in only: continue
        for k in Ks:
            rc, so, rss = run(k, cfg)
            w.writerow([cfg, k, rep, rc, num('total_ops', so),
                        (num('sum_num_inserts_total', so) or 0) + (num('sum_num_deletes_total', so) or 0),
                        num('sum_num_rq_total', so), num('sum_num_searches_total', so), round(rss)])
    print('rep', rep, 'done', flush=True)
print('DONE', flush=True)
