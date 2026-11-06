# Fault tests for lost workers

Do lost workers and services end a run promptly, without touching healthy long tasks, and with the right answer when
the run goes on? `run_fault_tests.py` runs each case below on the `LinearTransform` problem, whose answer is known.

```sh
stack build
salloc --ntasks=1 --cpus-per-task=4 --mem=8G --time=1:00:00   # or any allocation of about 4 CPUs
srun --overlap python3 test/fault/run_fault_tests.py \
  --exe "$(stack path --local-install-root)/bin/hyperion-scheduler-test" --out /some/disposable/folder --json results.json
```

All cases take about 40 minutes, and `--arms a,b` picks some. It needs no second machine: stand-ins in `bin/` for `sbatch`,
`ssh` and `scontrol` give one machine two pretend nodes, the master's own and `fakeb`, so hyperion takes its remote launch
path over real TCP connections. Both nodes share one node-local folder, so these tests can't show a problem with moving
files between real disks. A test exits 1 if a case didn't come out as wanted; each line of output is one case in JSON.

**Faults.** `inject.sh` is the worker command (`HYPERION_SCHEDULER_TEST_WORKER_WRAPPER`). It starts the worker, or for a
chosen launch simulates its loss before it connects:
`exit1`, `vanish` (an empty log, then exit 0), `hang` (alive, never connects), `badport` (the real worker's messages go to
a channel that doesn't exist, so it logs `Couldn't connect to master` and exits). In a run, the test sends SIGKILL to a
worker. It kills only processes of this test, in its own Slurm allocation.

**Controls.** `long-task` runs a healthy native task (`HYPERION_SCHEDULER_TEST_SLOW_ELEMENT`, an external `sleep`) for three
times the connect limit: it must finish with the right answer.

**What a case checks.** How the run ended (`success` needs the exact expected vector in both passes; `fail` needs hyperion's
hold line; `hung` is neither within the wait), the error text naming what was lost, the seconds from fault to end, and that no
worker is left 40 s after the run ended. Native processes left are reported only: a native outlives a worker that is lost. `task-kill-retry` also uses hyperion's retry on the held job, and needs
the right answer. `ready-kill`, `reusable-kill` and `launcher-kill` want only that the run ends with no wrong answer: either way is correct,
since whether the lost helper is needed again depends on where tasks land. `sleep 86407` left behind is `inject.sh`'s `hang`,
a simulated worker; a real one gives up after about 53 s.
