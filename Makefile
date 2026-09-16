.PHONY: help setup up down down-clean status logs reset \
        synth load produce build-lambdas build-flink \
        test test-unit test-integration test-e2e test-floci \
        lint lint-fix typecheck \
        tf-init tf-plan tf-apply tf-destroy tf-fmt \
        superset-setup validate diag

PYTHON := uv run python
SHELL := /bin/bash

help: ## Show this help message
	@echo "Healthcare Realtime Vitals Lakehouse - Targets:"
	@echo "=================================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup: ## Initial setup: .env, uv sync, .venv
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env from .env.example"; fi
	uv sync --extra dev

# --- compose -------------------------------------------------------------
up: ## Start the full local stack (Floci, Trino, Superset, MySQL, Nessie)
	docker compose up -d --wait
	@$(MAKE) status

up-floci: ## Start only Floci (for producer/integration dev)
	docker compose up -d floci --wait

down: ## Stop all services
	docker compose down

down-clean: ## Stop services and remove volumes + local data
	docker compose down -v --remove-orphans
	rm -rf data/ synthea-output/ superset_home/ mysql_data/

status: ## Show service status
	docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

logs: ## View logs (make logs SERVICE=trino)
	@if [ -n "$(SERVICE)" ]; then docker compose logs -f $(SERVICE); else docker compose logs -f; fi

reset: ## Wipe Floci emulated AWS state (buckets, streams, lambdas) and re-apply
	docker compose restart floci
	@echo "Floci restarted with a clean store. Run 'make tf-apply' to re-provision."

# --- data ------------------------------------------------------------------
synth: ## Run Synthea once -> synthea-output/fhir (requires java)
	./scripts/generate_cohort.sh

load: ## Load Synthea FHIR cohort into Iceberg via Trino (patient_data loader)
	$(PYTHON) -m patient_data.synthea_loader

produce: ## Stream vitals from the simulator to Kinesis (Ctrl-C to stop)
	$(PYTHON) -m producer.producer

seed: ## Seed DDB read-model fixtures (L1/L3 + dashboards pre-Flink, idempotent)
	$(PYTHON) scripts/seed_fixtures.py

smoke: ## Post-deploy smoke (Kinesis, DDB, SNS, packaged Lambdas)
	./scripts/smoke_deploy.sh

# --- build ----------------------------------------------------------------
build-lambdas: ## Zip L1-L4 Lambda packages into lambdas/_build/ (+ hashes)
	$(PYTHON) scripts/build_lambdas.py

build-flink: ## Build the Flink SQL job JAR (Maven, into flink/target/)
	@mkdir -p flink/src/main/resources
	@cp flink/sql/job.sql flink/src/main/resources/job.sql
	@if command -v mvn >/dev/null 2>&1; then mvn -f flink/pom.xml -q package; \
	else echo "mvn not found - install Maven or run from CI."; exit 1; fi

# --- tests -----------------------------------------------------------------
test: ## All unit + floci-integration tests (no e2e)
	$(PYTHON) -m pytest tests/ -v

test-unit: ## Unit tests only (no infra)
	$(PYTHON) -m pytest tests/ -v -m unit -n auto

test-floci: ## Floci-backed integration tests via testcontainers
	$(PYTHON) -m pytest tests/ -v -m floci

test-integration: ## Integration tests against a running compose stack
	$(PYTHON) -m pytest tests/integration tests/e2e -v -m "floci or e2e"

test-e2e: ## Full pipeline smoke against a running compose stack
	$(PYTHON) -m pytest tests/e2e -v -m e2e

coverage: ## Unit coverage with 90% threshold on core modules
	$(PYTHON) -m pytest tests/unit -m unit --cov=lambdas --cov=producer --cov=patient_data --cov-fail-under=90

# --- quality ---------------------------------------------------------------
lint: ## ruff lint
	ruff check .

lint-fix: ## ruff check --fix + format
	ruff check --fix .
	ruff format .

typecheck: ## mypy strict
	mypy .

# --- infra (Terraform / OpenToFu -> Floci) ---------------------------------
tf-init: ## Init providers (tofu preferred, terraform fallback)
	@if command -v tofu >/dev/null 2>&1; then tofu -chdir=infra init; elif command -v terraform >/dev/null 2>&1; then terraform -chdir=infra init; else echo "Install OpenToFu or Terraform"; exit 1; fi

tf-plan: ## Plan against Floci
	$(MAKE) tf-init
	@if command -v tofu >/dev/null 2>&1; then tofu -chdir=infra plan; else terraform -chdir=infra plan; fi

tf-apply: ## Apply infra (Lambdas, DDB, streams, GW, rules) against Floci
	$(MAKE) tf-init
	@if command -v tofu >/dev/null 2>&1; then tofu -chdir=infra apply -auto-approve; else terraform -chdir=infra apply -auto-approve; fi

tf-destroy: ## Tear down emulated resources
	@if command -v tofu >/dev/null 2>&1; then tofu -chdir=infra destroy -auto-approve; else terraform -chdir=infra destroy -auto-approve; fi

tf-fmt: ## Format + validate Terraform
	@if command -v tofu >/dev/null 2>&1; then tofu -chdir=infra fmt -recursive; tofu -chdir=infra validate; else terraform fmt -recursive infra/; terraform -chdir=infra validate; fi

# --- superset ----------------------------------------------------------------
superset-setup: ## Provision Superset DB/datasets/charts/dashboards (REST API)
	$(PYTHON) scripts/provision_superset.py

# --- validation / diagnostics -----------------------------------------------
validate: ## Run validation queries against Trino
	docker exec -i trino trino < sql/00_validation.sql 2>/dev/null || echo "run: bring up compose first"

workshop-run: ## Run the time-series SQL workshop (bootstrap + 01-07) on the live Trino (iceberg.healthcare)
	@docker exec -i vitals-trino trino --catalog iceberg --schema healthcare < sql/workshop/00_bootstrap_patients.sql
	@for q in sql/workshop/0[1-7]_*.sql; do \
		printf '\n==== %s ====\n' "$$q"; \
		docker exec -i vitals-trino trino --catalog iceberg --schema healthcare < "$$q"; \
	done

diag: ## Collect diagnostics into diagnostics/
	./scripts/collect_diagnostics.sh