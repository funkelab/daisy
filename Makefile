default:
	pip install .

.PHONY: install-dev
install-dev:
	pip install -e .[full]

.PHONY: test
test:
	python -m tests -v
