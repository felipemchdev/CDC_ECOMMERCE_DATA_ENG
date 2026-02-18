PYTHON ?= python
DATE ?= 2026-01-01
START ?= 2026-01-01
END ?= 2026-01-07

install:
	$(PYTHON) -m pip install -e ".[dev]"

run:
	$(PYTHON) -m cdc_ecommerce run --date $(DATE)

backfill:
	$(PYTHON) -m cdc_ecommerce backfill --start $(START) --end $(END)

test:
	$(PYTHON) -m pytest
