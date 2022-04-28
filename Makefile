default:
	pip install .

install-dev:
	pip install -r requirements_dev.txt
	pip install -e .[full]

.PHONY: tests
tests:
	PY_MAJOR_VERSION=py`python -c 'import sys; print(sys.version_info[0])'` pytest -v --benchmark-skip --cov=daisy --cov-config=.coveragerc tests
	flake8 daisy
benchmark:
	PY_MAJOR_VERSION=py`python -c 'import sys; print(sys.version_info[0])'` pytest -v --benchmark-only --benchmark-group-by=param:size,param:test benchmarks

