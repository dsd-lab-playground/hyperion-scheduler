# hyperion-scheduler

A task scheduler built on [hyperion](https://github.com/davidsd/hyperion). Given a
DAG of tasks with memory, CPU and disk-space estimates, it distributes them over
the nodes of a job, moves intermediate files between node-local storage, and
cleans them up when they are no longer needed.

## Design notes

- [Stat keys and input summaries](docs/stat-key-inputs-design.md): how tasks
  are estimated, and how recorded statistics correct the estimates.

## Worker launch

Each task runs on a new worker process. A supervisor on each node launches
these workers, keeping one ready per CPU, so the master sends one message per
task. The supervisor runs on 2 capabilities and each worker on 1, whatever
the RTS options of the program and the CPUs of the task (in a threaded RTS).
A task that wants parallel Haskell can call `setNumCapabilities` itself.

The ready workers use memory that task estimates do not count: 20–40 MB each
on Expanse, i.e. 3–5 GB per node at 128 CPUs. Leave room for it when you set
`nodeMemory`.

With `HYPERION_SCHEDULER_NODE_SUPERVISORS=0`, read by the master, the master
launches each worker itself.

## Building

```sh
stack build
```

## Tests

### Unit tests

Tests for task maps (validation, replacement, placeholders, input sizes),
statistics and estimates, and input summaries. No cluster; only the `TaskFiles`
test touches the filesystem, in a temporary directory:

```sh
stack test
```

### LinearTransform

An end-to-end scheduler test that computes a chain of linear transformations
`x_{i+1} = A_i . x_i`, one task per product `A_ik x_k`. `A_i` is a cyclic shift
by one position, so the answer is known in advance and is checked at the end.
The point is to generate many small tasks with many dependencies:
`--shift` sets the depth of the task graph and `--dim` its width
(`shift * dim^2` multiplication tasks in total).

#### Locally, without SLURM

```sh
stack exec -- hyperion-scheduler-test local \
  --shift 5 --dim 20 --base-dir /tmp/lt-test
```

The scheduler and its node supervisor run in this process, and each task runs
on a new worker process (or on a thread of this process, with
`HYPERION_SCHEDULER_NODE_SUPERVISORS=0`). A single node is reported to the
scheduler with `--cpus` CPUs (all cores by default). `--base-dir` defaults to `tmp/hyperion-scheduler-linear-transform-test`
in the current directory and holds the results, the logs, and
`node_local_storage/`, which stands in for a compute node's local scratch so the
file-service and cleanup logic is exercised too.

#### On a SLURM cluster

```sh
export HYPERION_SCHEDULER_TEST_SITE=expanse
stack exec -- hyperion-scheduler-test master \
  --shift 10 --dim 100 --nodes 2 --ntasks-per-node 32 \
  --time 30 --mem 64G
```

Per-site settings (partition, account, scratch directory, hostname strategy)
live in `Hyperion.Scheduler.Test.Config`; add a `Site` constructor to support a
new cluster. Anything passed on the command line overrides the site defaults.

The site comes from `HYPERION_SCHEDULER_TEST_SITE`, not a flag. Workers are
launched by hyperion without our options, and master and workers must agree on
`HyperionStaticConfig`; the environment is inherited through `sbatch`, a flag
would not be. Unset, it defaults to `expanse`.

Results go to `<base-dir>/nodes_<N>_ntasks_<M>/`, so runs with different
allocations can be compared. `--base-dir` defaults to the site's scratch
directory.

#### Output

Both modes run the problem twice, in `from_model/` and `from_stats/`. The
second pass is scheduled from the statistics the first one recorded, and the
test fails if a task with a stat key did not use them. Next to the two
directories:

- `task_records.json` — one record per task of the first pass (start time,
  runtime, memory, node, CPUs, output file sizes, the estimates and their
  sources, the stat key and the input summaries)
- `task_stats.json` — the same, aggregated by stat key and input summary
- `task_records_from_stats.json` — the records of the second pass
