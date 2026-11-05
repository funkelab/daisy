window.BENCHMARK_DATA = {
  "lastUpdate": 1793850039494,
  "repoUrl": "https://github.com/funkelab/daisy",
  "entries": {
    "daisy": [
      {
        "commit": {
          "author": {
            "name": "William Patton",
            "username": "pattonw",
            "email": "pattonw@hhmi.org"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "4e1ba81d07c07c62144d507443d7b4a8570c1f34",
          "message": "Run coverage from pytest addopts; make the ty job check the tests (#98)\n\n* Run coverage from pytest addopts so a bare `uv run pytest` is the CI command\n\n`[tool.pytest.ini_options] addopts` now carries `--cov --cov-report=term-missing`,\nso the README's `uv run pytest` prints the coverage table and enforces\n`[tool.coverage.report] fail_under` the same way CI does. The CI step drops the\nflags it no longer needs. A single-file run can skip the floor with `--no-cov`.\n\nCo-Authored-By: Claude Mythos 5.1 <noreply@anthropic.com>\n\n* Type-check the tests for real: build the package for ty, fill the stub gaps\n\nThe ty job ran with only the lint group installed, so neither `daisy` nor\npytest nor funlib.geometry resolved and, with unresolved-import ignored, every\nname in tests/ was Unknown and no diagnostic could fire. The job now builds\nthe extension and installs the dev group (`uv run --group lint ty check`),\nthe unresolved-import override is gone, and rich sits in the lint group so\nthe optional import in daisy/logging.py resolves too. `--error-on-warning`\nin CI and the README, since nothing is excluded, overridden or ignored.\n\nIn a synced environment that command reported 34 diagnostics, all in tests/.\nFive were stub gaps: `_daisy.pyi` now declares `Context.from_env_string` and\nthe `return_states`, `host` and `summary` keywords `Pipeline.run_blockwise`\nforwards, with overloads for the bool / states-dict return. The rest were\ntest code: submodules accessed as attributes after a bare `import daisy`\n(now imported explicitly), `Optional` results indexed without a guard (now\nasserted), the dynamic `daisy_states` attribute on KeyboardInterrupt (read\nwith getattr), and deliberately wrong argument types (passed through `Any`).\n\nCo-Authored-By: Claude Mythos 5.1 <noreply@anthropic.com>\n\n* Track import time and test duration across commits\n\nThe tests job times its own `uv run pytest`, measures the cold import of\nthe package's headline modules (benchmarks/run.py), and uploads the two\nnumbers as an artifact from one matrix cell. The benchmark workflow runs\nwhen a tests run on main completes, downloads that artifact, creates the\ngh-pages branch if it is missing, and publishes with github-action-benchmark\nto gh-pages:dev/bench, with a comparison table in the job summary and a\ncommit comment when a metric gets 1.5x worse. The docs gain a Benchmarks\npage that charts every metric from that data, one line per metric with the\ncommit on hover and click; the docs workflow now runs after each benchmark\nrun and copies dev/bench from gh-pages into the deployed site, so the\ncharts carry the newest point and the action's own dashboard keeps its URL.\nNothing runs the test suite twice.\n\nCo-Authored-By: Claude Mythos 5.1 <noreply@anthropic.com>\n\n* benchmarks: format run.py\n\n---------\n\nCo-authored-by: Claude Mythos 5.1 <noreply@anthropic.com>",
          "timestamp": "2026-11-05T03:34:59Z",
          "url": "https://github.com/funkelab/daisy/commit/4e1ba81d07c07c62144d507443d7b4a8570c1f34"
        },
        "date": 1793850037923,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "import daisy (cold)",
            "value": 58.632,
            "range": "± 4.303",
            "unit": "ms",
            "extra": "interpreter startup 10.9 ms subtracted; n=10"
          },
          {
            "name": "pytest wall time",
            "value": 59.938,
            "unit": "s",
            "extra": "`uv run pytest` in the tests job"
          }
        ]
      }
    ]
  }
}