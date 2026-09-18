import os, re, subprocess
os.chdir('/root/setbench/microbench')
def go(algo, ms):
    p = subprocess.run([f'./bin/i.{algo}', '-nwork', '16', '-insdel', '50', '50', '-k', '20000000', '-t', str(ms)],
                       capture_output=True, text=True)
    ls = [l for l in p.stderr.splitlines() if l.startswith('INSTR')][-16:]
    g = lambda k, l: float(re.search(k + r'=([\d.]+)', l).group(1))
    avg = sum(g('avg', l) for l in ls); peak = max(g('peak', l) for l in ls)
    left = sum(g('retired', l) - g('freed', l) for l in ls)
    ops = int(re.search(r'total_ops=(\d+)', p.stdout).group(1))
    return ops / (ms / 1000) / 1e6, avg, peak, left
print(f'{"algo":6} {"secs":>4} {"Mops/s":>7} {"avgBacklog":>10} {"maxThrPeak":>10} {"unfreed@end":>11}')
for algo in ['k1', 'k64']:
    for ms in [5000, 20000]:
        for _ in range(3):
            t, a, pk, left = go(algo, ms)
            print(f'{algo:6} {ms//1000:>4} {t:7.2f} {a:10.0f} {pk:10.0f} {left:11.0f}', flush=True)
