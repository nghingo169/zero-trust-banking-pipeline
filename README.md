# Zero-trust banking pipeline

A production-grade Databricks lakehouse pipeline for ingesting banking source snapshots with comprehensive data quality validation, SCD Type 2 history tracking, and automated quarantine management.

The project uses Databricks Declarative Automation Bundles (DABs) so the same codebase can be deployed across engineer workspaces and target environments (`dev`, portable `demo`, and team/production). The `main` branch is reserved for production-ready releases; ongoing integration work belongs on the `dev` branch.

---

## Overview

This project implements a zero-trust data pipeline that:
* Processes 41 banking source tables across Customer, Card, Transaction, and Financial Crime domains.
* Implements SCD Type 2 for historical snapshot entities and SCD Type 1 for immutable events.
* Enforces row-level data quality rules with automatic quarantine routing.
* Provides full audit logging and quality metrics tracking.
* Uses Databricks Declarative Automation Bundles for multi-environment deployment.
* Includes an automated CI/CD pipeline with parallel testing and deployment triggers.

---

## Architecture & medallion flow

### Pipeline flow

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

### Medallion architecture layers

#### Bronze Layer — Raw Data Ingestion

* **Purpose:** Immutable landing zone preserving full source history.
* **SCD Type 2 for snapshot entities (customer, account, card):** Tracks historical changes with effective dates (`valid_from`, `valid_to`) to maintain a complete audit trail of state changes.
* **SCD Type 1 for status-event tables (transactions, status changes):** Treats immutable events using their real composite business keys. No history tracking needed as events are point-in-time facts.
* **Metadata enrichment:** `business_date`, `load_timestamp`, `source_file`.
* **Schema:** Minimal transformations, preserves raw source structure.

#### Silver layer — Validated & normalized

* **Purpose:** Clean, conformed data ready for analytics and downstream ML feature engineering.
* **Data quality validation:** Row-level quality rule enforcement.
* **Schema standardization:** Consistent data types, naming conventions, and deduplication.
* **Quarantine routing:** Failed records are automatically routed to Governance Quarantine with context.

#### Gold layer — Business aggregates & analytics

* **Purpose:** Pre-aggregated, business-level datasets optimized for reporting and ML.
* **Customer 360 Views:** Unified customer profiles across all 4 domains.
* **Aggregated metrics:** Denormalized models for executive analytics and dashboards.

#### Governance & Infrastructure

Cross-layer capabilities for data quality, lineage, and compliance:

* **Quarantine table:** Centralized tracking for failed validation rules. Stores one record per failed rule, preserving the original Bronze payload for root cause analysis and remediation.
* **Audit logs:** Full lineage and execution metadata, tracking performance, metrics, and compliance logs across all pipeline stages.
* **Infrastructure lifecycle:** Unity Catalog schemas and governance state tables represent persistent infrastructure. They are preloaded once via SQL scripts, while application bundles deploy and own pipelines, jobs, and source code updates without destroying existing catalogs.

---

## Repository structure

```plaintext
zero-trust-banking-pipeline/
├── .github/
│   └── workflows/
│       └── ci-cd.yml           # GitHub Actions CI/CD pipeline
├── resources/                  # Databricks Bundle resources
│   ├── banking_investigation.pipeline.yml
│   │                            # One Source-to-Gold SDP pipeline
│   ├── banking_investigation_bootstrap.job.yml
│   │                            # One-time governance/bootstrap Job
│   ├── banking_investigation.job.yml
│   │                            # Recurring pipeline orchestration Job
│   ├── apply_and_verify_pii_tags.job.yml
│   │                            # Governance-owned internal tagging Job
│   └── run_integration_tests.yml
├── src/
│   ├── pipeline/
│   │   ├── bronze/             # Source-to-Bronze ingestion
│   │   ├── silver/             # Validation, quarantine, and atomic Silver
│   │   ├── gold/               # Gold investigation contexts
│   │   ├── governance/         # Schema, UDF, ABAC, grants, and PII tags
│   │   ├── monitoring/         # Layer audits and run finalizers
│   │   ├── source_landing/     # Source landing guidance
│   │   ├── run_context.py      # Canonical fail-closed run identity
│   │   └── README.md           # Pipeline source overview
│   └── data_contracts/         # Authoritative schemas and quality rules
│       ├── schemas/            # Table schema definitions
│       ├── quality_rules/      # Data quality rule registry
│       ├── audit/              # Audit logging modules
│       ├── table_catalog.py    # Table metadata and keys
│       └── normalization.py    # Data normalization functions
├── tests/
│   ├── bronze/                 # Bronze ingestion and metadata tests
│   ├── silver/                 # Transformation, validation, and masking tests
│   ├── gold/                   # Gold context tests
│   └── pipeline/               # Production orchestration contract tests
├── configs/
│   └── demo.variable-overrides.example.json
│                                # Non-secret portable target template
├── docs/                       # Runbook, data models, and architecture documentation
│   └── Team_Workspace_Pipeline_Technical_Runbook.md
├── sql/                        # Analytics and exploration queries
├── scripts/                    # Source and deployment utilities
├── databricks.yml              # Bundle variables and deployment targets
├── requirements.txt            # Python dependencies
├── .gitignore
└── README.md                   # Root documentation

```

---

## Quick start & setup

### Prerequisites

- Python 3.10+ and Git
- Databricks CLI with Bundle support
- A Unity Catalog workspace with serverless SDP, governed tags, and ABAC support
- A serverless SQL warehouse for the one-time catalog setup
- Permission to create or assign the required account groups and service principals

The portable Bundle target is `demo`. Each demo owner chooses their own CLI
profile, workspace, catalog, and teammate memberships. Follow the
[team workspace runbook](docs/Team_Workspace_Pipeline_Technical_Runbook.md)
for the complete identity, catalog, S3-secret, permission, and acceptance-test
procedure.

### Initial setup

1. **Clone the repository and install dependencies:**

```bash
git clone <repository-url>
cd zero-trust-banking-pipeline
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

2. **Authenticate to the selected workspace:**

```bash
databricks auth login \
  --host <demo-workspace-url> \
  --profile <demo-profile>

databricks current-user me --profile <demo-profile>
```

3. **Prepare the workspace once:**

- Create or reuse the two runtime service principals and the
  `governance-admins`, `data-engineers`, and `pii-dq-operator` account groups.
- Create the isolated catalog and delegate catalog-level bootstrap authority
  using `sql/infrastructure/01_create_catalog_and_delegate.sql`. The bootstrap
  Job creates the schemas and intentionally cannot create arbitrary catalogs.
- Configure the `banking-s3-ingestion` secret scope through interactive CLI
  prompts and grant it only to the pipeline service principal.
- Grant the runtime service principals access to the deployed Bundle files path.

4. **Create the gitignored local Bundle override:**

```bash
mkdir -p .databricks/bundle/demo
cp configs/demo.variable-overrides.example.json \
  .databricks/bundle/demo/variable-overrides.json

git check-ignore .databricks/bundle/demo/variable-overrides.json
```

Set the chosen catalog, service-principal application IDs, and group names in
that local file. Do not commit workspace URLs, personal emails, IDs, or secrets.

5. **Test, validate, review, and deploy:**

```bash
.venv/bin/pytest -q tests/pipeline/test_production_orchestration.py

databricks bundle validate --target demo --profile <demo-profile>
databricks bundle plan --target demo --profile <demo-profile>
databricks bundle deploy --target demo --profile <demo-profile>
```

6. **Run bootstrap once:**

```bash
databricks bundle run banking_investigation_bootstrap \
  --target demo \
  --profile <demo-profile>
```

Run bootstrap again only after an approved governance, tag, masking, group, or
catalog-grant change.

7. **Run the recurring Source-to-Gold Job:**

```bash
databricks bundle run banking_investigation_pipeline_orchestration \
  --target demo \
  --profile <demo-profile> \
  --params audit_business_date=2026-07-10
```

For new source data or an ordinary retry, rerun only
`banking_investigation_pipeline_orchestration`. Authorized Data Engineers may
start it, but it always executes as the pipeline service principal.

---

## Environment Commands Summary

| Task | Dev Environment (`-t dev`) | Team Environment (`-t team`) (recommended) |
| --- | --- | --- |
| **Validate bundle** | `databricks bundle validate -t dev -p <your-profile>` | `databricks bundle validate -t team -p <your-profile>` |
| **Deploy bundle** | `databricks bundle deploy -t dev -p <your-profile>` | `databricks bundle deploy -t team -p <your-profile>` |
| **Run integration tests** | `databricks bundle run run_integration_tests -t dev -p <your-profile>` | `databricks bundle run run_integration_tests -t team -p <your-profile>` |
| **Run full pipeline** | `databricks bundle run banking_investigation_pipeline_orchestration -t dev -p <your-profile>` | `databricks bundle run banking_investigation_pipeline_orchestration -t team -p <your-profile>` |

---

## Engineering workflow

### Branch strategy

* `main`: Production-ready code only. Direct commits are restricted; changes require PR approvals. Merges trigger production deployment.
* `dev`: Integration branch for ongoing development. Feature branches target `dev` first.
* `feature/*`: Feature-specific branches created from `dev`.

### Developer cycle

1. **Branch out:**
```bash
git checkout dev && git pull origin dev && git checkout -b feature/your-feature

```


2. **Develop & test locally:**
```bash
# Unit testing
pytest tests/ -v --cov=src

# Formatting & Linting
black src/ tests/
isort src/ tests/
flake8 src/ tests/

```


3. **Validate bundle configuration:**
```bash
databricks bundle validate --target dev --profile <your-profile>

```


4. **Deploy & integration test on Databricks:**
```bash
databricks bundle deploy --target dev
databricks bundle run run_integration_tests -t dev

```


5. **PR & merge:** Open PR to `dev`. Upon review and approval, merge to `dev`, and eventually promote from `dev` to `main`.

---

## Testing & validation

Testing occurs across multiple levels:

### 1. Local unit tests

```bash
# Using pytest
pytest tests/ -v --cov=src --cov-report=term-missing

# Using unittest
PYTHONPATH=src python -m unittest discover -s tests -p 'test_*.py'

```

### 2. Static code analysis

```bash
black src/ tests/
isort src/ tests/
flake8 src/ tests/ --max-line-length=127

```

### 3. Integration & end-to-end testing (Databricks)

```bash
# Validate and deploy bundle in dev target
databricks bundle validate -t dev -p <your-profile>
databricks bundle deploy -t dev -p <your-profile>

# Run integration tests remotely (dev environment)
databricks bundle run run_integration_tests -t dev -p <your-profile>

# Or team environment (recommended)
databricks bundle validate -t team -p <your-profile>
databricks bundle deploy -t team -p <your-profile>
databricks bundle run run_integration_tests -t team -p <your-profile>

```

### 4. CI/CD automation (GitHub actions)

Located at `.github/workflows/ci-cd.yml`:

* **On push/PR to `main`:** Runs code quality checks (`flake8`, `black`, `isort`), parallel unit tests on Python 3.10/3.11/3.12, and bundle syntax validation.
* **On merge to `main`:** Automatically deploys to Dev, executes integration tests on Databricks, and promotes/deploys to the production/team workspace.
* **Secret configuration:** Ensure `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are configured in GitHub Repository Secrets.

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
| **Local unit tests fail** | Missing `PYTHONPATH` or packages | Run `pip install -r requirements.txt` and set path: `export PYTHONPATH=src`. |
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
