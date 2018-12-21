default:
	pip install .

.PHONY: install-dev
install-dev:
	pip install -e .[full]

.PHONY: tests
tests:
	python -m tests -v
