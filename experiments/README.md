# Experiments: daisy 1.x versus 2.0, verified by running

Every claim in the migration guide's behaviour tables points at a directory
here. Each directory holds a `run.py` that works with either daisy 1.x or
daisy 2.0 installed, and a `README.md` with the outputs we captured on both,
so the result can be read without running anything.

```
python experiments/representative/modes.py A        # a 3-D task in each worker mode
python experiments/<table>/<experiment>/run.py      # one claim, one script
python experiments/speed/throughput_by_mode.py      # the throughput benchmark
```

The scripts never trust daisy for the ground truth: block bodies append a
line to a file for every attempt, and the script prints
`EVIDENCE blocks attempted a/n, succeeded b/n, total attempts c` next to
daisy's own summary.

| Directory | What it holds |
| --- | --- |
| [`representative/`](representative/README.md) | the same 512-block 3-D sleep task in modes A, B, C and S on both lines, with the script and the outputs |
| [`same_behavior/`](same_behavior/) | cases where both lines do the same thing |
| [`v1_hangs_v2_finishes/`](v1_hangs_v2_finishes/) | cases where 1.x waits forever and 2.0 finishes or refuses |
| [`v1_finishes_v2_abandons/`](v1_finishes_v2_abandons/) | cases where 1.x finishes with failed blocks and 2.0 abandons the task |
| [`v1_silent_v2_loud/`](v1_silent_v2_loud/) | cases where 1.x reports success or says nothing and 2.0 returns False or raises |
| [`different_by_design/`](different_by_design/) | behaviour that changed on purpose |
| [`speed/`](speed/README.md) | throughput by mode and worker count, resume cost, scheduler cost, with plots |

The tables that index these experiments are in [`MIGRATION.md`](../MIGRATION.md#verified-behaviour-differences).

## Worker modes

| Mode | What you pass to `Task(process_function=...)` | What runs where |
| --- | --- | --- |
| A | a function taking one `block` | daisy starts the workers (forked on 1.x, subprocesses on 2.0) and runs the acquire/process/release loop for you |
| B | a function taking no arguments | daisy starts one process per worker and calls your function once; it creates `daisy.Client()` and loops itself |
| C | a function taking no arguments that launches something external (`subprocess.run`, `srun`, `sbatch --wait`) and blocks until it exits | the external process reads `DAISY_CONTEXT` from its environment, creates `daisy.Client()` and connects over TCP |
| S | `run_blockwise(..., multiprocessing=False)` with a one-`block` function | everything runs in the calling process, no server, no workers |

## Regenerating the READMEs

```
python experiments/_tools/capture.py --v1 /path/to/v1-env/bin/python --v2 /path/to/v2-env/bin/python
python experiments/_tools/capture_representative.py --v1 ... --v2 ...
python experiments/speed/throughput_by_mode.py --csv experiments/speed/throughput.csv   # once per interpreter
python experiments/speed/scheduler_drain.py >> experiments/speed/scheduler.csv           # once per interpreter
python experiments/speed/plots.py
```

The captured outputs in this tree come from daisy 1.2.3 (master @ 49ebecf)
and daisy 2.0.0 (the `v2.0` branch) on Python 3.12, one 96-core machine,
local ext4 storage.
