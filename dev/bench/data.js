window.BENCHMARK_DATA = {
  "lastUpdate": 1794594022350,
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
      },
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
          "id": "b8b405455ad51c6e90ae56c28c81426c26bb6376",
          "message": "Deploy docs on version tags and by hand only (#100)\n\n* Deploy docs on version tags and by hand only\n\nThe docs workflow now runs on a v* tag push and on workflow_dispatch, and\nnot on every push or pull request commit. A tag or a manual run on main\ndeploys; a manual run on any other branch builds without deploying, which\nis how a pull request's docs are checked once it is ready to merge.\n\nCo-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>\n\n* Run the lowest-direct test leg against the declared floors\n\nThe resolution mode was passed to `uv sync` only. The next step, a plain\n`uv run pytest`, saw a lockfile written in another mode, re-resolved to the\nnewest versions and replaced the environment, so the leg named\nlowest-direct tested unlocked newest versions instead. The mode is now set\nonce as UV_RESOLUTION for the whole job.\n\nCo-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>\n\n* Raise the pytest-cov floor to 7\n\nWith branch coverage on, pytest-cov 6.x fails when it combines the data\nfiles written by daisy's worker processes: 'Can't combine statement\ncoverage data with branch data'. The lowest-direct test legs install\npytest-cov 6.0.0 and failed after all tests passed. 6.0 to 6.3 all fail;\n7.0.0 passes.\n\nCo-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>\n\n* Build the macOS arm64 wheel on macos-15\n\nGitHub now rejects the macos-14 runner ('The configuration\nmacos-14-arm64-us-default is not supported'), so the aarch64 wheel job\nwas cancelled before it started.\n\nCo-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>\n\n* Raise the cloudpickle floor to 3.1.1\n\ncloudpickle 3.0.0 and 3.1.0 raise RecursionError when pickling a worker\nfunction on Python 3.14, so daisy cannot start a worker subprocess. The\npy3.14 lowest-direct test leg installs 3.0.0 and failed 25 tests. 3.1.1\npasses. The floor moves in both the runtime dependency and the dev group.\n\nCo-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>\n\n---------\n\nCo-authored-by: Claude Fable 5.2 <noreply@anthropic.com>",
          "timestamp": "2026-11-11T23:37:56Z",
          "url": "https://github.com/funkelab/daisy/commit/b8b405455ad51c6e90ae56c28c81426c26bb6376"
        },
        "date": 1794440624402,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "import daisy (cold)",
            "value": 38.052,
            "range": "± 1.765",
            "unit": "ms",
            "extra": "interpreter startup 7.6 ms subtracted; n=10"
          },
          {
            "name": "pytest wall time",
            "value": 50.219,
            "unit": "s",
            "extra": "`uv run pytest` in the tests job"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Will",
            "username": "pattonw",
            "email": "wllmpttn24@gmail.com"
          },
          "committer": {
            "name": "Will",
            "username": "pattonw",
            "email": "wllmpttn24@gmail.com"
          },
          "id": "d54de4612dd17f41ce59263874f626a5ba7848a3",
          "message": "Bump version to 2.0.2\n\nCo-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>",
          "timestamp": "2026-11-12T16:30:07Z",
          "url": "https://github.com/funkelab/daisy/commit/d54de4612dd17f41ce59263874f626a5ba7848a3"
        },
        "date": 1794501291809,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "import daisy (cold)",
            "value": 53.785,
            "range": "± 0.931",
            "unit": "ms",
            "extra": "interpreter startup 9.6 ms subtracted; n=10"
          },
          {
            "name": "pytest wall time",
            "value": 58.236,
            "unit": "s",
            "extra": "`uv run pytest` in the tests job"
          }
        ]
      },
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
          "id": "7a67fff6395b1719aaf61412187abfdc23648d2b",
          "message": "Make Sphinx warnings fatal in the docs build (#101)\n\nAdd -W to sphinx-build in docs.yml and to the two build commands in the\nREADME, so a broken cross-reference or autodoc import fails the docs job\ninstead of publishing silently. The build is already warning-free.\n\nCo-authored-by: Claude Fable 5.2 <noreply@anthropic.com>",
          "timestamp": "2026-11-13T18:12:02Z",
          "url": "https://github.com/funkelab/daisy/commit/7a67fff6395b1719aaf61412187abfdc23648d2b"
        },
        "date": 1794593822675,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "import daisy (cold)",
            "value": 43.636,
            "range": "± 0.596",
            "unit": "ms",
            "extra": "interpreter startup 8.5 ms subtracted; n=10"
          },
          {
            "name": "pytest wall time",
            "value": 54.158,
            "unit": "s",
            "extra": "`uv run pytest` in the tests job"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "dependabot[bot]",
            "username": "dependabot[bot]",
            "email": "49699333+dependabot[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "9c589ea9b4de499b4433a411b77ec02deac3ee60",
          "message": "Bump soupsieve from 2.10 to 4.0 (#102)\n\nBumps [soupsieve](https://github.com/facelessuser/soupsieve) from 2.10 to 4.0.\n- [Release notes](https://github.com/facelessuser/soupsieve/releases)\n- [Commits](https://github.com/facelessuser/soupsieve/compare/2.10...4.0)\n\n---\nupdated-dependencies:\n- dependency-name: soupsieve\n  dependency-version: '4.0'\n  dependency-type: indirect\n...\n\nSigned-off-by: dependabot[bot] <support@github.com>\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2026-11-13T18:12:30Z",
          "url": "https://github.com/funkelab/daisy/commit/9c589ea9b4de499b4433a411b77ec02deac3ee60"
        },
        "date": 1794594020828,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "import daisy (cold)",
            "value": 56.398,
            "range": "± 1.275",
            "unit": "ms",
            "extra": "interpreter startup 11.1 ms subtracted; n=10"
          },
          {
            "name": "pytest wall time",
            "value": 58.722,
            "unit": "s",
            "extra": "`uv run pytest` in the tests job"
          }
        ]
      }
    ]
  }
}