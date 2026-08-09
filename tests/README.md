# Testing Guide & Workflow

This document details local unit testing, code formatting, integration testing, and automated CI/CD pipeline execution for the Zero-Trust Banking Pipeline across both `dev` and `team` (production) environments.

---

## Test Structure

```plaintext
tests/
├── bronze/
│   └── test_bronze.py                      # Tests for Bronze layer ingestion & metadata enrichment
├── silver/
│   ├── test_card_transformation.py         # Card domain schema & logic transformation tests
│   ├── test_customer_transformation.py     # Customer domain transformation tests
│   ├── test_fincrime_transformation.py     # Financial Crime domain transformation tests
│   ├── test_quality_rules.py               # Rule registry, native expectations & shared quarantine tests
│   └── test_transaction_transformation.py  # Transaction domain transformation tests
├── gold/
│   ├── test_ai_aml_investigation_context.py # AI AML investigation views & context tests
│   ├── test_customer_360_context.py        # Customer 360 aggregation & view tests
│   └── test_fraud_transaction_context.py   # Fraud transaction aggregation tests
├── pipeline/
│   └── test_production_orchestration.py   # Production orchestration & bundle contract tests
├── conftest.py                             # Pytest fixtures & shared test configurations
├── run_unit_tests.py                       # Unit test execution script for Databricks compute / local
└── README.md                               # Testing documentation & guide

```

---

## Developer Workflow & Code Formatting

Before committing code or submitting a Pull Request, developers must format their code to comply with PEP8 standards and organize import statements:

```bash
# 1. Format code automatically using Black
black src/ tests/

# 2. Sort import statements using isort
isort --profile black src/ tests/

```

---

## Running Tests

### Option 1: Local Testing (Development)

Run unit tests locally during development before pushing code. You can exclude integration tests that require a live Databricks pipeline state using `-m "not integration"`:

```bash
# Run unit tests locally
python -m pytest tests/

# Run unit tests excluding pipeline-dependent integration tests
python -m pytest -v -m "not integration" tests/

# Run unit tests with coverage report
python -m pytest -v -m "not integration" --cov=src tests/ --cov-report=term-missing

# Using unittest
PYTHONPATH=src python -m unittest discover -s tests -p 'test_*.py'

# Run via test runner script
python tests/run_unit_tests.py

# Run a specific test file
python -m pytest tests/silver/test_quality_rules.py

# Run tests for a specific medallion layer
python -m pytest tests/bronze/
python -m pytest tests/silver/
python -m pytest tests/gold/

```

---

### Option 2: Integration Testing via Bundle Jobs (Recommended)

Run integration tests remotely through the bundle-deployed job on Databricks. You can target either the **Development (`dev`)** or **Team/Production (`team`)** environment:

#### Development Environment (`dev`)

```bash
# Validate bundle
databricks bundle validate -t dev

# Deploy changes to dev target
databricks bundle deploy -t dev --auto-approve

# Run integration tests remotely on Databricks
databricks bundle run run_integration_tests -t dev

```

#### Team / Production Environment (`team`)

```bash
# Validate bundle
databricks bundle validate -t team

# Deploy changes to team target
databricks bundle deploy -t team --auto-approve

# Run integration tests remotely on Databricks
databricks bundle run run_integration_tests -t team

```

**Job Execution Steps:**

* Executes test suites on Databricks compute nodes via `run_unit_tests.py`.
* Validates quality rules, data contracts, and transformation logic across Bronze, Silver, and Gold layers.
* Ensures schema enforcement and data contract alignment across target schemas.

---

## Test Coverage

The test suite covers:

* **Bronze Layer:** Metadata enrichment, landing zone schema verification, Auto Loader ingest validation, and source contracts.
* **Silver Layer:** Row-level quality rule validation, domain-specific transformations (Customer, Card, Transaction, FinCrime), and quarantine tagging.
* **Gold Layer:** Pre-aggregated views, Customer 360 contexts, Fraud transaction context, and AI AML investigation contexts.
* **Pipeline Orchestration:** Bundle contract validation and production workflow orchestration.
* **Infrastructure & Fixtures:** Shared test configurations and PySpark session fixtures defined in `conftest.py`.

---

## Adding New Tests

1. Identify the target layer (`bronze`, `silver`, `gold`, or `pipeline`) and create a new test file under that folder (e.g., `tests/silver/test_new_feature.py`).
2. Import required modules from `src/` and shared fixtures from `conftest.py`.
3. If the test requires a full pipeline state or live workspace tables, annotate it with `@pytest.mark.integration`.
4. Implement test cases using `pytest` or `unittest`.
5. Format the new test file using `black` and `isort`.
6. Ensure local unit tests pass via `python -m pytest tests/`.

---

## Integration with CI/CD

Tests and formatting checks are fully integrated into the GitHub Actions CI/CD pipeline (`.github/workflows/ci-cd.yml`):

### CI/CD Workflow Stages

```text
Push / PR to dev/main ──► Code formatting & linting ──► Unit tests (Py 3.10, 3.11, 3.12)
                          (black, isort, flake8)                  │
                                                                   ▼
                                                         Bundle syntax validation
                                                         (databricks bundle validate)
                                                                   │
                                                                   ▼
                                                         Deploy target (dev/team)
                                                         (databricks bundle deploy --auto-approve)
                                                                   │
                                                                   ▼
                                                         Databricks integration tests
                                                         (databricks bundle run run_integration_tests)

```

1. **Pre-commit / Local Developer Cycle:**
* Run `black src/ tests/` and `isort --profile black src/ tests/`.
* Execute local tests via `python -m pytest tests/`.
* Validate bundle using `databricks bundle validate -t dev`.


2. **Pull Request / Commit Stage:**
* CI runner executes `black --check`, `isort --check`, and `flake8` checks.
* Runs unit tests across Python 3.10, 3.11, and 3.12 environments.
* Validates Databricks Asset Bundle syntax (`databricks bundle validate -t dev`).


3. **Deployment & Integration Testing Stage:**
* Deploys bundle to target environment (`dev` for feature/dev branch PRs, `team` for releases).
* Executes `run_integration_tests` job remotely on Databricks using `run_unit_tests.py`.



---

## Environment Commands Summary

| Task | Dev Environment (`-t dev`) | Team Environment (`-t team`) |
| --- | --- | --- |
| **Validate bundle** | `databricks bundle validate -t dev` | `databricks bundle validate -t team` |
| **Deploy bundle** | `databricks bundle deploy -t dev --auto-approve` | `databricks bundle deploy -t team --auto-approve` |
| **Run integration tests** | `databricks bundle run run_integration_tests -t dev` | `databricks bundle run run_integration_tests -t team` |
| **Run full pipeline** | `databricks bundle run banking_investigation_pipeline_orchestration -t dev` | `databricks bundle run banking_investigation_pipeline_orchestration -t team` |

---

## Troubleshooting

* **Import errors locally:** Verify the Python virtual environment is activated (`source .venv/bin/activate` or `.venv\Scripts\activate`).
* **Databricks job execution failure:**
* Ensure bundle is deployed to the target environment (`databricks bundle deploy -t dev --auto-approve` or `-t team`).
* Check workspace authentication (`databricks auth login`).
* Verify catalog and schema permissions in Unity Catalog.