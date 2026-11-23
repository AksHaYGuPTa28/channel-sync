PY ?= python

.PHONY: db test lint fmt chaos ui serve

db:
	docker compose up -d --wait

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

fmt:
	$(PY) -m ruff check --fix .
	$(PY) -m ruff format .

chaos:
	$(PY) -m sim.run_chaos --actions 3000 --seed 7

ui:
	cd dashboard && npm ci && npm run build

serve: ui
	$(PY) -m sim.server
