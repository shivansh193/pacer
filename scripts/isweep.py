import os, re, subprocess, sys, csv, time, ctypes

os.chdir('/root/setbench/microbench')
out, reps, algos = sys.argv[1], int(sys.argv[2]), sys.argv[3].split(',')
cfgs_sel = sys.argv[4].split(',') if len(sys.argv) > 4 else None

CONFIGS = {  # name: (args, n_timed_threads, uses_rq_tree)
    'write_8':   (['-nwork', '8',  '-insdel', '50', '50'], 8, False),
    'write_16':  (['-nwork', '16', '-insdel', '50', '50'], 16, False),
    'read80_16': (['-nwork', '16', '-insdel', '10', '10'], 16, False),
    'rq_16':     (['-nwork', '12', '-nrq', '4', '-rqsize', '10000', '-insdel', '50', '50'], 16, True),
}

# --- calibrate TSC cycles per ns ---
open('/tmp/tsc.c', 'w').write('''#include <stdio.h>
#include <time.h>
#include <x86intrin.h>
int main(){struct timespec a,b;clock_gettime(CLOCK_MONOTONIC,&a);unsigned long long t0=__rdtsc();
do{clock_gettime(CLOCK_MONOTONIC,&b);}while((b.tv_sec-a.tv_sec)*1e9+(b.tv_nsec-a.tv_nsec)<3e8);
unsigned long long t1=__rdtsc();printf("%f\\n",(double)(t1-t0)/((b.tv_sec-a.tv_sec)*1e9+(b.tv_nsec-a.tv_nsec)));}''')
subprocess.run(['gcc', '-O2', '/tmp/tsc.c', '-o', '/tmp/tsc'], check=True)
CYC_PER_NS = float(subprocess.check_output(['/tmp/tsc']).decode())
print('TSC cycles/ns =', CYC_PER_NS, flush=True)

def bucket_lo(i):
    if i < 4: return i
    k, m = (i - 4) // 4, (i - 4) % 4
    return (4 + m) << k

def pct(hist, p):
    tot = sum(hist.values()); need = tot * p; c = 0
    for i in sorted(hist):
        c += hist[i]
        if c >= need: return bucket_lo(i)
    return 0

def num(pat, s):
    m = re.search(pat + r'=(\d+)', s); return int(m.group(1)) if m else None

w = csv.writer(open(out, 'w'), lineterminator='\n')
w.writerow(['cfg', 'algo', 'rep', 'rc', 'mops', 'avg_backlog', 'max_thread_peak', 'sum_peaks',
            'retired', 'freed', 'p50_ns', 'p99_ns', 'p999_ns', 'p9999_ns', 'max_ns'])
for rep in range(1, reps + 1):
    for cfg, (args, nt, rq) in CONFIGS.items():
        if cfgs_sel and cfg not in cfgs_sel: continue
        for a in algos:
            binary = f'./bin/{"r" if rq else "i"}.{a}'
            p = subprocess.run([binary] + args + ['-k', '20000000', '-t', '5000'],
                               capture_output=True, text=True)
            so, se = p.stdout, p.stderr
            tot = num('total_ops', so)
            lines = [l for l in se.splitlines() if l.startswith('INSTR')]
            if p.returncode != 0 or tot is None or len(lines) < nt:
                w.writerow([cfg, a, rep, p.returncode] + [''] * 11); continue
            lines = lines[-nt:]
            hist, avg, peaks, ret, fr, mx = {}, 0.0, [], 0, 0, 0
            for l in lines:
                avg += float(re.search(r'avg=([\d.]+)', l).group(1))
                peaks.append(num('peak', l)); ret += num('retired', l); fr += num('freed', l)
                mx = max(mx, num('maxcyc', l))
                for kv in re.search(r'hist=(.*)$', l).group(1).split(','):
                    if kv:
                        i, c = kv.split(':'); hist[int(i)] = hist.get(int(i), 0) + int(c)
            f = lambda cyc: round(cyc / CYC_PER_NS)
            w.writerow([cfg, a, rep, p.returncode, round(tot / 5e6, 3), round(avg), max(peaks), sum(peaks),
                        ret, fr, f(pct(hist, .5)), f(pct(hist, .99)), f(pct(hist, .999)),
                        f(pct(hist, .9999)), f(mx)])
    print('rep', rep, 'done', flush=True)
print('DONE', flush=True)
