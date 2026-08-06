# Testing guide & workflow

This document details local unit testing, integration testing, and automated CI/CD pipeline execution for the Zero-Trust Banking Pipeline across both `dev` and `team` (production) environments.

---

## Test structure

```plaintext
tests/
├── bronze/
│   └── test_bronze.py                      # Tests for Bronze layer ingestion & metadata enrichment
├── silver/
│   ├── test_card_transformation.py         # Card domain schema & logic transformation tests
│   ├── test_card_validation.py             # Card domain row-level data quality validation tests
│   ├── test_customer_transformation.py     # Customer domain transformation tests
│   ├── test_customer_validation.py         # Customer domain quality validation tests
│   ├── test_fincrime_transformation.py     # Financial Crime domain transformation tests
│   ├── test_fincrime_validation.py         # Financial Crime domain quality validation tests
│   ├── test_nab_tdm_masking.py             # PII masking & TDM security tests
│   ├── test_quality_rules.py               # Rule registry & validation engine unit tests
│   ├── test_transaction_transformation.py # Transaction domain transformation tests
│   └── test_transaction_validation.py      # Transaction domain quality validation tests
├── gold/
│   ├── test_ai_aml_investigation_context.py # AI AML investigation views & context tests
│   ├── test_customer_360_context.py        # Customer 360 aggregation & view tests
│   └── test_fraud_transaction_context.py   # Fraud transaction aggregation tests
├── conftest.py                             # Pytest fixtures & shared test configurations
├── run_unit_tests.py                       # Unit test execution script for Databricks compute / local
└── README.md                               # Testing documentation & guide

```

---

## Running tests

### Option 1: Local testing (Development)

Run tests locally during development before committing code. Use `-m "not integration"` to exclude tests that depend on a deployed pipeline/data state:

```bash
# Recommended: Run unit tests excluding pipeline-dependent integration tests
pytest -v -m "not integration" tests/

# Run with coverage report
pytest -v -m "not integration" --cov=src tests/ --cov-report=term-missing

# Using unittest (from project root)
PYTHONPATH=src python -m unittest discover -s tests -p 'test_*.py'

# Run a specific test file
pytest tests/silver/test_quality_rules.py

# Run tests for a specific medallion layer
pytest tests/bronze/
pytest tests/silver/
pytest tests/gold/

```

---

### Option 2: Integration testing via bundle jobs (Recommended)

Run integration tests through the bundle-deployed job on Databricks. You can target either the **Development (`dev`)** environment or the **Production (`team`)** environment:

#### Development environment (`dev`)

```bash
# Deploy changes to dev target
databricks bundle deploy -t dev

# Run integration tests in dev
databricks bundle run run_integration_tests -t dev

```

#### Team / Production environment (`team`)

```bash
# Deploy changes to team target
databricks bundle deploy -t team

# Run integration tests in team
databricks bundle run run_integration_tests -t team

```

**Job execution steps:**

* Executes test suites on Databricks compute nodes via `run_unit_tests.py`.
* Validates quality rules, data contracts, and transformation logic across Bronze, Silver, and Gold layers.
* Ensures schema enforcement and data contract alignment across target schemas.

---

## Test coverage

The test suite covers:

* **Bronze layer:** Metadata enrichment, landing zone schema verification, and Auto Loader ingest validation.
* **Silver layer:** Row-level quality rule validation, domain-specific transformations (Customer, Card, Transaction, FinCrime), PII masking (`nab_tdm_masking`), and quarantine tagging.
* **Gold layer:** Pre-aggregated views, Customer 360 contexts, Fraud transaction context, and AI AML investigation contexts.
* **Infrastructure & Fixtures:** Shared test configurations and PySpark session fixtures defined in `conftest.py`.

---

## Adding new tests

1. Identify the target layer (`bronze`, `silver`, or `gold`) and create a new test file under that folder (e.g., `tests/silver/test_new_feature.py`).
2. Import required modules from `src/` and shared fixtures from `conftest.py`.
3. If the test requires full pipeline state/Databricks workspace tables, annotate it with `@pytest.mark.integration`.
4. Implement test cases using `pytest` or `unittest`.
5. Ensure local unit tests pass via `pytest -m "not integration" tests/`.

---

## Integration with CI/CD

Tests are fully integrated into the GitHub Actions CI/CD pipeline (`.github/workflows/ci-cd.yml`):

### CI/CD workflow stages

```text
Push / PR to main/dev ──► Static analysis & linting ──► Unit tests (Py 3.10, 3.11, 3.12)
                                                                 │
                                                                 ▼
                                                       Bundle syntax validation
                                                                 │
                                                                 ▼
                                                       Deploy target (dev/team)
                                                                 │
                                                                 ▼
                                                       Databricks integration tests

```

1. **Pre-commit / Local testing:**
```bash
pytest -v -m "not integration" tests/

```


2. **Pull request / Commit stage:**
* Runs `flake8`, `black`, and `isort` code formatting checks.
* Executes unit tests across Python 3.10, 3.11, and 3.12 runners.
* Validates Databricks Asset Bundle definitions (`databricks bundle validate -t dev`).


3. **Deployment & integration test stage:**
* Deploys bundle to target environment (`dev` for dev triggers, `team` for `main` release).
* Executes `run_integration_tests` job remotely on Databricks using `run_unit_tests.py`.



---

## Environment commands summary

| Task | Dev Environment (`-t dev`) | Team Environment (`-t team`) |
| --- | --- | --- |
| **Validate bundle** | `databricks bundle validate -t dev` | `databricks bundle validate -t team` |
| **Deploy bundle** | `databricks bundle deploy -t dev` | `databricks bundle deploy -t team` |
| **Run integration tests** | `databricks bundle run run_integration_tests -t dev` | `databricks bundle run run_integration_tests -t team` |
| **Run full pipeline** | `databricks bundle run banking_investigation_pipeline_orchestration -t dev` | `databricks bundle run banking_investigation_pipeline_orchestration -t team` |

---

## Troubleshooting

* **Import errors locally:** Verify `PYTHONPATH=src` is set before executing `pytest` or `python -m unittest`.
* **Missing dependencies:** Run `pip install -r requirements.txt`.
* **Databricks job execution failure:**
* Ensure bundle is deployed to the target environment (`databricks bundle deploy -t dev` or `-t team`).
* Check workspace authentication (`databricks auth login`).
* Verify catalog and schema permissions on Unity Catalog.



```
