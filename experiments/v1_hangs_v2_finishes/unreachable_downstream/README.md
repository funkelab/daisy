# unreachable downstream

**Claim.** A downstream task reaching beyond its upstream hangs 1.x after the reachable blocks; 2.0 refuses the pipeline.

**Table.** v1 hangs v2 finishes

**Run it** (works with daisy 1.x or 2.0 installed):

```
python experiments/v1_hangs_v2_finishes/unreachable_downstream/run.py
```

## What the script does

A downstream task whose total_roi extends 10 blocks beyond the upstream
task's write region. Those 10 blocks have no upstream block to wait on, so
1.x waits forever after the rest completes; 2.0 refuses the pipeline before
it starts.

## Output on daisy 1.x (1.2.3)

Status: killed after 45 s (hang), 45.0 s.

```text
===========================================
downstream task reaches beyond its upstream
===========================================
daisy 1.2.3 (1.x), python 3.12.0
```

stderr: 91 lines, 91 progress-bar refreshes.

## Output on daisy 2.0 (2.0.0)

Status: exit 0, 0.1 s.

```text
===========================================
downstream task reaches beyond its upstream
===========================================
daisy 2.0.0 (2.0), python 3.12.0


RAISED RuntimeError: invalid configuration: task "down" has blocks (total_roi Roi((0), (300))) that read nothing their upstream task "up" writes (total write roi Roi((0), (200))) al  (0.0 s)
EVIDENCE blocks attempted 0/50, succeeded 0/50, total attempts 0
```

stderr: 0 lines.
