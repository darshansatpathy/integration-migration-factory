.PHONY: install check lint test regenerate regress

install:
	pip install -e ".[dev]"

lint:
	ruff check .
	ruff format --check .
	mypy
	imf generate --check
	imf samples --check

# Set IMF_BOOTSTRAP to run the Kafka tests too.
test:
	pytest --cov --cov-report=term-missing

check: lint test

# After changing a flow, a layout, a template or the sample generators.
regenerate:
	imf samples
	imf generate
	imf inventory --out docs/inventory.md --csv docs/inventory.csv

regress:
	imf regress --bus kafka --bootstrap $${IMF_BOOTSTRAP:-localhost:9092} --report docs/regression-report.md
