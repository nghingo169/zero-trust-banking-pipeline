# Zero-Trust Banking Pipeline

A production-grade Databricks lakehouse pipeline for ingesting banking source snapshots with comprehensive data quality validation, SCD Type 2 history tracking, and automated quarantine management.

The project uses Databricks Declarative Automation Bundles (DABs) so the same codebase can be deployed across engineer workspaces and target environments (`dev`, `staging`, and team/production). The `main` branch is reserved for production-ready releases; ongoing integration work belongs on the `dev` branch.

---

## Overview

This project implements a zero-trust data pipeline that:
* Processes 41 banking source tables across Customer, Card, Transaction, and Financial Crime domains.
* Implements SCD Type 2 for historical snapshot entities and SCD Type 1 for immutable events.
* Enforces row-level data quality rules with automatic quarantine routing.
* Publishes native SDP event-log metrics through dashboard-ready monitoring views.
* Uses Databricks Declarative Automation Bundles for multi-environment deployment.
* Includes an automated CI/CD pipeline with parallel testing and deployment triggers.

---

## Architecture & Medallion Flow

### Pipeline Flow

```text
                    Source Snapshot Files (.csv/.parquet)
                                      ↓
                    Source Landing Volume (Unity Catalog)
                                      ↓
                    ================================
                         BRONZE LAYER (Raw)
                    ================================
                    SCD Type 2: Customer, Account, Card
                    SCD Type 1: Transactions, Events
                    + Source lineage & metadata
                                      ↓
                    Validation & Normalization Engine
                                      ↓
              ┌──────────────────────┴──────────────────────┐
              ↓                                             ↓
    ================================               Governance Quarantine
        SILVER LAYER (Validated)                     (Failed Records)
    ================================                        ↓
    Clean, normalized records                       Audit & Monitoring
    Row-level quality validated
    Business rules applied
              ↓
    ================================
        GOLD LAYER (Aggregated)
    ================================
    Customer 360 views
    Pre-aggregated metrics
    Business-ready datasets
              ↓
    Dashboards, Reports, ML Models

```

### Medallion Architecture Layers

#### Bronze Layer — Raw Data Ingestion

* **Purpose:** Immutable landing zone preserving full source history.
* **SCD Type 2 for snapshot entities (Customer, Account, Card):** Tracks historical changes with effective dates (`valid_from`, `valid_to`) to maintain a complete audit trail of state changes.
* **SCD Type 1 for status-event tables (Transactions, Status Changes):** Treats immutable events using their real composite business keys. No history tracking needed as events are point-in-time facts.
* **Metadata enrichment:** `business_date`, `load_timestamp`, `source_file`.
* **Schema:** Minimal transformations, preserves raw source structure.

#### Silver Layer — Validated & Normalized

* **Purpose:** Clean, conformed data ready for analytics and downstream ML feature engineering.
* **Data quality validation:** Row-level quality rule enforcement.
* **Schema standardization:** Consistent data types, naming conventions, and deduplication.
* **Quarantine routing:** Failed records are automatically routed to Governance Quarantine with context.

#### Gold Layer — Business Aggregates & Analytics

* **Purpose:** Pre-aggregated, business-level datasets optimized for reporting and ML.
* **Customer 360 Views:** Unified customer profiles across all 4 domains.
* **Aggregated metrics:** Denormalized models for executive analytics and dashboards.

#### Governance & Infrastructure

Cross-layer capabilities for data quality, lineage, and compliance:

* **Quarantine table:** Centralized tracking for failed validation rules. Stores one record per failed rule, preserving the original Bronze payload for root cause analysis and remediation.
* **Monitoring views:** Pipeline status, row counts, duration, dropped rows, and rule failures derived from the native SDP event log and quarantine records.
* **Infrastructure lifecycle:** Unity Catalog schemas and governance state tables represent persistent infrastructure. They are preloaded once via SQL scripts, while application bundles deploy and own pipelines, jobs, and source code updates without destroying existing catalogs.

---

## Repository Structure

```plaintext
zero-trust-banking-pipeline/
├── .databricks/                # Databricks bundle local state & overrides
│   └── bundle/
│       ├── dev/
│       └── team/
├── .github/
│   └── workflows/
│       └── ci-cd.yml           # GitHub Actions CI/CD workflow
├── .vscode/                    # VS Code editor workspace settings
├── configs/                    # Bundle environment overrides template
│   └── environment.variable-overrides.example.json
├── data/                       # Local/sample dataset guidelines
├── data_contract/              # YAML data contract definitions
│   ├── Card_domain_datacontract.yaml
│   ├── Customer_domain_datacontract.yaml
│   ├── Customer_domain_datacontract_ver2.yaml
│   ├── FinCrime_domain_datacontract.yaml
│   └── Transaction_domain_datacontract.yaml
├── docs/                       # Architecture & runbook documentation
│   ├── banking_daily_change_catalog.md
│   ├── banking_error_injection_catalog.md
│   ├── Banking_Silver_Atomic_Warehouse.dbml
│   ├── customer_360_silver_guide.md
│   ├── error_injection_rule_mapping.md
│   ├── IMPLEMENTATION_SUMMARY.md
│   ├── NAB_TDM_MASKING_GUIDE.md
│   └── Team_Workspace_Pipeline_Technical_Runbook.md
├── resources/                  # Databricks Bundle Job & Pipeline resources
│   ├── apply_and_verify_pii_tags.job.yml
│   ├── banking_investigation.job.yml
│   ├── banking_investigation.pipeline.yml
│   ├── banking_investigation_bootstrap.job.yml
│   └── run_integration_tests.yml
├── scripts/                    # Helper utility scripts
│   └── source_landing/
│       └── load_local_snapshots.sh
├── sql/                        # SQL scripts for Infrastructure & Customer 360
│   ├── customer_360/           # 18 Customer 360 analytics SQL views/queries
│   └── infrastructure/         # Catalog setup SQL scripts
│       └── 01_create_catalog_and_delegate.sql
├── src/                        # Pipeline source code
│   ├── data_contracts/         # Normalization, schema, monitoring & rules
│   │   ├── audit/              # Audit log writers
│   │   ├── monitoring/         # Monitoring views
│   │   ├── quality_rules/      # Domain-specific quality rules
│   │   ├── schemas/            # Domain schema contracts
│   │   ├── normalization.py
│   │   └── table_catalog.py
│   ├── legacy/                 # Legacy transformations and notebooks
│   └── pipeline/               # Core pipeline transformation scripts
│       ├── bronze/             # Source to Bronze ingestion
│       ├── silver/             # Bronze to Silver transformations
│       ├── gold/               # Gold contexts (Customer 360, Fraud, AML)
│       ├── governance/         # ABAC, PII tagging, and catalog setup
│       ├── monitoring/         # Pipeline setup and finalization
│       └── run_context.py      # Pipeline run context helper
├── tests/                      # Unit & integration test suite
│   ├── bronze/                 # Bronze ingestion unit tests
│   ├── gold/                   # Gold layer context tests
│   ├── pipeline/               # Production orchestration tests
│   ├── silver/                 # Silver transformation & quality tests
│   ├── conftest.py             # Pytest Spark session configuration
│   └── run_unit_tests.py       # Automated test runner script
├── databricks.yml              # Databricks Asset Bundle (DAB) configuration
├── README.md                   # Root documentation
└── test_results.txt            # Test execution output logs

```

---

## Quick Start & Setup

### Prerequisites

* Python 3.10+ and Git
* Databricks CLI with Bundle support
* A Unity Catalog workspace with serverless SDP, governed tags, and ABAC support
* A serverless SQL warehouse for the one-time catalog setup
* Permission to create or assign the required account groups and service principals

Use `staging` for the current shared workspace. Other environment owners define their own Bundle target and choose their own CLI profile, workspace, catalog, and teammate memberships. Follow the [team workspace runbook](https://www.google.com/search?q=docs/Team_Workspace_Pipeline_Technical_Runbook.md) for the complete identity, catalog, S3-secret, permission, and acceptance-test procedure.

### Initial Setup

1. **Clone the repository and set up environment:**

```bash
git clone <repository-url>
cd zero-trust-banking-pipeline
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

```

2. **Authenticate to the selected workspace:**

```bash
databricks auth login \
  --host <workspace-url> \
  --profile <cli-profile>

databricks current-user me --profile <cli-profile>

```

3. **Prepare the workspace once:**

* Create or reuse the two runtime service principals and the `governance-admins`, `data-engineers`, and `pii-dq-operator` account groups.
* Create the isolated catalog and delegate catalog-level bootstrap authority using `sql/infrastructure/01_create_catalog_and_delegate.sql`. The bootstrap Job creates the schemas and intentionally cannot create arbitrary catalogs.
* Configure the `banking-s3-ingestion` secret scope through interactive CLI prompts and grant it only to the pipeline service principal.
* Grant the runtime service principals access to the deployed Bundle files path.

4. **Create the gitignored local Bundle override:**

```bash
mkdir -p .databricks/bundle/<bundle-target>
cp configs/environment.variable-overrides.example.json \
  .databricks/bundle/<bundle-target>/variable-overrides.json

git check-ignore .databricks/bundle/<bundle-target>/variable-overrides.json

```

Set the chosen catalog, service-principal application IDs, and group names in that local file. Do not commit workspace URLs, personal emails, IDs, or secrets.

5. **Test, validate, review, and deploy:**

```bash
python -m pytest -q tests/pipeline/test_production_orchestration.py

databricks bundle validate --target <bundle-target> --profile <cli-profile>
databricks bundle plan --target <bundle-target> --profile <cli-profile>
databricks bundle deploy --target <bundle-target> --profile <cli-profile> \
  --fail-on-active-runs

```

6. **Run bootstrap once:**

```bash
databricks bundle run banking_investigation_bootstrap \
  --target <bundle-target> \
  --profile <cli-profile>

```

Run bootstrap again only after an approved governance, tag, masking, group, or catalog-grant change.

7. **Run the recurring Source-to-Gold Job:**

```bash
databricks bundle run banking_investigation_pipeline_orchestration \
  --target <bundle-target> \
  --profile <cli-profile> \
  --params business_date=2026-07-10

```

For new source data or an ordinary retry, rerun only `banking_investigation_pipeline_orchestration`. Authorized Data Engineers may start it, but it always executes as the pipeline service principal.

---

## Environment Commands Summary

Use `staging` for the current shared deployment, or replace `<bundle-target>` with the target defined for another workspace.

| Task | Command |
| --- | --- |
| **Validate bundle** | `databricks bundle validate -t <bundle-target> -p <cli-profile>` |
| **Deploy bundle** | `databricks bundle deploy -t <bundle-target> -p <cli-profile> --fail-on-active-runs` |
| **Run integration tests** | `databricks bundle run run_integration_tests -t <bundle-target> -p <cli-profile>` |
| **Run full pipeline** | `databricks bundle run banking_investigation_pipeline_orchestration -t <bundle-target> -p <cli-profile>` |

---

## Developer Local & CI/CD Workflow

### Branching Strategy

* `main`: Production-ready code only. Direct commits are strictly restricted.
* `dev`: Integration branch for ongoing development. Feature branches target `dev` first.
* `feature/*`: Feature-specific branches created from `dev`.

### Local Development & CI/CD Lifecycle

1. **Branch Out:** Create a new feature branch from `dev`:
```bash
git checkout dev
git pull origin dev
git checkout -b feature/your-feature-name

```


2. **Modify & Auto-Format Code Locally:** After modifying the codebase locally, you must run the following formatting commands to enforce PEP8 standards and organize imports before committing:
```bash
# 1. Format code automatically using Black
black src/ tests/

# 2. Sort import statements using isort
isort --profile black src/ tests/

```


3. **Validate & Test Locally:** Verify bundle configuration and unit tests before pushing:
```bash
python -m pytest tests/
databricks bundle validate --target dev

```


4. **Create Pull Request (PR):** Push your branch to remote and open a Pull Request targeting `dev` (or from `dev` to `main`).
5. **GitHub Actions CI Validation Check:** The CI workflow (`.github/workflows/ci-cd.yml`) triggers automatically to check code quality, linters (`flake8`, `black`, `isort`), unit tests, and executes bundle validation:
```bash
databricks bundle validate -t dev

```


6. **Merge PR & Automated Deployment:** Once validation checks pass and the PR is reviewed and approved:
* Merge the Pull Request into the target branch.
* GitHub Actions triggers the deployment pipeline automatically:
```bash
# 1. Deploy bundle resources to Databricks Workspace
databricks bundle deploy -t dev --auto-approve

# 2. Trigger automated Integration Tests remotely
databricks bundle run run_integration_tests -t dev

```





---

## Testing & Validation

Testing occurs across multiple levels:

### 1. Local Unit Tests

```bash
# Using pytest
python -m pytest tests/ -v --cov=src --cov-report=term-missing

# Using the test runner script
python tests/run_unit_tests.py

```

### 2. Static Code Analysis

```bash
black src/ tests/
isort --profile black src/ tests/
flake8 src/ tests/ --max-line-length=127

```

### 3. Integration & End-to-End Testing (Databricks)

```bash
# Validate and deploy bundle in dev target
databricks bundle validate -t dev -p <your-profile>
databricks bundle deploy -t dev -p <your-profile> --auto-approve

# Run integration tests remotely (dev environment)
databricks bundle run run_integration_tests -t dev -p <your-profile>

```

---

## Security & Configuration

* **Zero hardcoded credentials:** No passwords, tokens, or workspace URLs are committed to source control.
* **Local authentication:** Handled via standard Databricks CLI configuration in `~/.databrickscfg`.
* **Variable overrides:** Stored locally in `.databricks/bundle/<target>/variable-overrides.json` (gitignored).
* **Cloud storage access:** Unity Catalog Storage Credentials and External Locations govern S3 access. *(Note: Free Edition uses temporary secret scopes as documented in the runbook).*
* **Gitignore safety:** `.databricks/`, `__pycache__/`, `.pytest_cache/`, `.coverage`, and environment files are strictly excluded.

---

## Troubleshooting

| Issue | Cause | Solution |
| --- | --- | --- |
| **Bundle validation fails** | YAML syntax error or missing variable | Run `databricks bundle validate --target dev` and inspect missing resource key/variable definitions. |
| **Local unit tests fail** | Missing `PYTHONPATH` or environment setup | Ensure virtual environment is active and set path: `export PYTHONPATH=src`. |
| **CLI auth failure** | Expired token or invalid host | Re-authenticate using `databricks auth login --host <workspace-url>`. |
| **Job run failures** | Schema mismatch or missing Volume | Check job execution logs in Databricks UI, verify UC volume path, and consult the Quarantine table for validation rule errors. |
| **Notebook import error** | Missing workspace deployment | Ensure `databricks bundle deploy` has been executed on the target environment. |

---

## Documentation

### Available guides

* **[Pipeline runbook](docs/Team_Workspace_Pipeline_Technical_Runbook.md)** - Complete setup and operations guide
* **[Testing guide](tests/README.md)** - Unit and integration testing
* **[Banking silver warehouse](https://www.google.com/search?q=docs/Banking_Silver_Atomic_Warehouse.dbml)** - Data model DBML
* **[Daily change catalog](docs/banking_daily_change_catalog.md)** - Change detection patterns
* **[Error injection catalog](docs/banking_error_injection_catalog.md)** - Quality rule catalog
* **[Customer 360 guide](docs/customer_360_silver_guide.md)** - Customer analytics queries

---

## Tech Stack & Dependencies

* **Language:** Python 3.10+
* **Engine:** PySpark 3.4+, Databricks Runtime
* **Governance:** Databricks Unity Catalog
* **Deployment:** Databricks Declarative Automation Bundles (DABs)
* **Testing:** pytest, unittest
* **CI/CD:** GitHub Actions