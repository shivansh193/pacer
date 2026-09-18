import subprocess, resource, re, sys, os
threads = sys.argv[1]
reps = int(sys.argv[2])
algos = sys.argv[3:]
os.chdir('/root/setbench/microbench')
for i in range(reps):
    for a in algos:
        before = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        p = subprocess.run(
            [f'./bin/brown_ext_abtree_lf.{a}', '-nwork', threads, '-insdel', '50', '50',
             '-k', '20000000', '-t', '5000'],
            capture_output=True, text=True)
        m = re.search(r'total_ops=(\d+)', p.stdout)
        peak_mb = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024
        print(f'rep{i+1} {a:7} rc={p.returncode:<4} ops={m.group(1) if m else "NONE":>10} '
              f'peakRSS(max so far)={peak_mb:.0f}MB', flush=True)
        if p.returncode != 0 or not m:
            print('   stderr tail:', p.stderr.strip().splitlines()[-3:], flush=True)
            print('   stdout tail:', p.stdout.strip().splitlines()[-3:], flush=True)
