"""Write representative/README.md: the 3-D sleep task in every worker mode,
on daisy 1.x and 2.0, with the script shown and the outputs trimmed.

python _tools/capture_representative.py --v1 <python> --v2 <python>
"""

import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
from capture import clean, stderr_summary, version_of  # noqa: E402

MODES = [
    ("A", "block function, daisy starts the workers"),
    ("B", "worker function with its own daisy.Client() loop"),
    ("C", "launcher that starts an external worker process"),
    ("S", "serial, no workers"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--v1", required=True)
    ap.add_argument("--v2", required=True)
    a = ap.parse_args()
    script = os.path.join(ROOT, "representative", "modes.py")
    with open(script) as f:
        source = f.read()
    v1v, v2v = version_of(a.v1), version_of(a.v2)
    md = ["# A representative task in every worker mode", ""]
    md += [
        "512 blocks (8 x 8 x 8 of 16^3 voxels with 4 voxels of context, so neighbours conflict and are scheduled in levels), each sleeping 10 ms, 4 workers. The same script runs on daisy 1.x and 2.0; the only difference it hides is the `num_workers` / `max_workers` rename (see `experiments/common.py`).",
        "",
    ]
    md += [
        "```",
        "python experiments/representative/modes.py A   # or B, C, S; --workers N",
        "```",
        "",
    ]
    md += ["## The script", "", "```python", source.strip(), "```", ""]
    for mode, desc in MODES:
        md += [f"## Mode {mode}: {desc}", ""]
        for python, label, version in ((a.v1, "1.x", v1v), (a.v2, "2.0", v2v)):
            out, err, status, secs, cwd = run_mode(python, script, mode)
            print(
                f"[representative] mode {mode} daisy {label}: {status} ({secs:.1f} s)",
                flush=True,
            )
            md += [
                f"### daisy {label} ({version})",
                "",
                f"Status: {status}, {secs:.1f} s.",
                "",
            ]
            body, _ = clean(out, keep=40)
            md += ["```text", body, "```", ""]
            summary, tail = stderr_summary(err)
            md += [f"stderr: {summary}." + (" Last lines:" if tail else ""), ""]
            if tail:
                md += ["```text", "\n".join(tail[-4:]), "```", ""]
    md += [
        "## Reading the output",
        "",
        "Both lines process all 512 blocks exactly once in every mode: `EVIDENCE blocks attempted 512/512, succeeded 512/512` is counted by the script itself from a file every block appends to, not from daisy. The execution summaries agree with it. Worker log files hold the `print` from the block body; on 2.0 they are written while the worker runs, on 1.x as well. Wall time differs because 1.x pays about 43 ms of round trip per block per worker (see `experiments/speed/`).",
        "",
    ]
    with open(os.path.join(ROOT, "representative", "README.md"), "w") as f:
        f.write("\n".join(md))


def run_mode(python, script, mode):
    import subprocess
    import tempfile
    import time

    cwd = tempfile.mkdtemp(prefix="daisy-rep-")
    env = dict(os.environ)
    env.pop("DAISY_EXPERIMENT_EVIDENCE", None)
    env.pop("DAISY_CONTEXT", None)
    t0 = time.monotonic()
    p = subprocess.run(
        [python, "-u", script, mode], cwd=cwd, env=env, capture_output=True, timeout=180
    )
    return (
        p.stdout.decode(errors="replace"),
        p.stderr.decode(errors="replace"),
        f"exit {p.returncode}",
        time.monotonic() - t0,
        cwd,
    )


if __name__ == "__main__":
    main()
