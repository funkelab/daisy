"""Run every experiment under a daisy 1.x and a daisy 2.0 interpreter and
write each experiment's README.md from the captured output.

python _tools/capture.py --v1 /path/to/v1/python --v2 /path/to/v2/python [name ...]
"""

import argparse
import os
import re
import signal
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
from manifest import EXPERIMENTS  # noqa: E402

TQDM = re.compile(r"\d+%\|")


def run_one(python, script, timeout):
    """Run a script in a fresh temp cwd; return (stdout, stderr, status, seconds)."""
    cwd = tempfile.mkdtemp(prefix="daisy-exp-")
    env = dict(os.environ)
    env.pop("DAISY_EXPERIMENT_EVIDENCE", None)
    env.pop("DAISY_CONTEXT", None)
    env["PYTHONUNBUFFERED"] = "1"
    t0 = time.monotonic()
    proc = subprocess.Popen(
        [python, "-u", script],
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        out, err = proc.communicate(timeout=timeout)
        status = f"exit {proc.returncode}"
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        out, err = proc.communicate()
        status = f"killed after {timeout} s (hang)"
    secs = time.monotonic() - t0
    return out.decode(errors="replace"), err.decode(errors="replace"), status, secs, cwd


def version_of(python):
    return subprocess.run(
        [python, "-c", "import importlib.metadata as m; print(m.version('daisy'))"],
        capture_output=True,
        text=True,
    ).stdout.strip()


SCRUB = [
    (re.compile(r"/[^ \"']*?/site-packages/"), ".../site-packages/"),
    (re.compile(r"/[^ \"']*?/lib/python3\.\d+/"), ".../lib/python3/"),
    (re.compile(r"/[^ \"']*?/experiments/"), "experiments/"),
    (re.compile(r"/tmp/daisy-[a-z]+-[A-Za-z0-9_]+"), "/tmp/daisy-tmp"),
    (re.compile(r"/tmp/pytest-of-[^ /]+/[^ ]*"), "/tmp/..."),
]


def scrub(text):
    for pat, rep in SCRUB:
        text = pat.sub(rep, text)
    return text


def clean(text, keep=60):
    """Drop progress-bar refreshes, then keep the head and tail with a count
    of what was omitted."""
    text = scrub(text)
    lines = []
    for raw in text.replace("\r", "\n").splitlines():
        if TQDM.search(raw) or "block/s" in raw or "blocks/s" in raw:
            continue
        lines.append(raw.rstrip())
    while lines and not lines[-1]:
        lines.pop()
    if len(lines) <= keep:
        return "\n".join(lines), 0
    head, tail = lines[: keep // 2], lines[-keep // 2 :]
    return "\n".join(
        head + [f"... ({len(lines) - keep} lines omitted) ..."] + tail
    ), len(lines) - keep


def stderr_summary(text):
    text = scrub(text)
    lines = [line for line in text.replace("\r", "\n").splitlines() if line.strip()]
    bars = sum(1 for line in lines if TQDM.search(line))
    other = [line for line in lines if not TQDM.search(line)]
    parts = [f"{len(lines)} lines"]
    if bars:
        parts.append(f"{bars} progress-bar refreshes")
    if other:
        parts.append(f"{len(other)} other lines")
    return ", ".join(parts), other[-8:]


def readme(group, name, claim, doc, runs):
    md = [
        f"# {name.replace('_', ' ')}",
        "",
        f"**Claim.** {claim}",
        "",
        f"**Table.** {group.replace('_', ' ')}",
        "",
    ]
    md += [
        "**Run it** (works with daisy 1.x or 2.0 installed):",
        "",
        "```",
        f"python experiments/{group}/{name}/run.py",
        "```",
        "",
    ]
    md += ["## What the script does", "", doc.strip(), ""]
    for version, label, (out, err, status, secs, _cwd) in runs:
        md += [
            f"## Output on daisy {label} ({version})",
            "",
            f"Status: {status}, {secs:.1f} s.",
            "",
        ]
        body, omitted = clean(out)
        md += ["```text", body or "(no stdout)", "```", ""]
        summary, tail = stderr_summary(err)
        md += [f"stderr: {summary}." + (" Last lines:" if tail else ""), ""]
        if tail:
            md += ["```text", "\n".join(tail), "```", ""]
    return "\n".join(md)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--v1", required=True)
    ap.add_argument("--v2", required=True)
    ap.add_argument("names", nargs="*")
    a = ap.parse_args()
    v1_version, v2_version = version_of(a.v1), version_of(a.v2)
    for group, name, claim, timeout in EXPERIMENTS:
        if a.names and name not in a.names:
            continue
        script = os.path.join(ROOT, group, name, "run.py")
        with open(script) as f:
            doc = f.read().split('"""')[1]
        runs = []
        for python, label, version in (
            (a.v1, "1.x", v1_version),
            (a.v2, "2.0", v2_version),
        ):
            res = run_one(python, script, timeout)
            runs.append((version, label, res))
            print(
                f"[{group}/{name}] daisy {label}: {res[2]} ({res[3]:.1f} s)", flush=True
            )
        with open(os.path.join(ROOT, group, name, "README.md"), "w") as f:
            f.write(readme(group, name, claim, doc, runs))


if __name__ == "__main__":
    main()
