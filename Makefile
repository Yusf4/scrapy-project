DAGSTER_HOME := $(CURDIR)/dagster_home

.PHONY: up dev test check

up:
	docker compose up -d

dev:
	DAGSTER_HOME=$(DAGSTER_HOME) .venv/bin/dagster dev

test:
	.venv/bin/python -m pytest -q

check:
	.venv/bin/python scripts/check_env.py
