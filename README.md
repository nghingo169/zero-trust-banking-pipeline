# Zero-Trust Banking Pipeline

A production-grade Databricks lakehouse pipeline for ingesting banking source snapshots with comprehensive data quality validation, SCD Type 2 history tracking, and automated quarantine management.

The project uses Databricks Declarative Automation Bundles (DABs) so the same codebase can be deployed seamlessly across engineer workspaces and target environments (`dev`, `staging`, `prod`/`team`). The `main` branch is reserved for production-ready releases; ongoing integration work belongs on the `dev` branch.

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
              ┌──────────────────┴──────────────────┐
              ↓                                      ↓
    ================================      Governance Quarantine
       SILVER LAYER (Validated)             (Failed Records)
    ================================               ↓
    Clean, normalized records              Audit & Monitoring
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
* **SCD Type 2 for Snapshot Entities (Customer, Account, Card):** Tracks historical changes with effective dates (`valid_from`, `valid_to`) to maintain a complete audit trail of state changes.
* **SCD Type 1 for Status-Event Tables (Transactions, Status Changes):** Treats immutable events using their real composite business keys. No history tracking needed as events are point-in-time facts.
* **Metadata Enrichment:** `business_date`, `load_timestamp`, `source_file`.
* **Schema:** Minimal transformations, preserves raw source structure.

#### Silver Layer — Validated & Normalized
* **Purpose:** Clean, conformed data ready for analytics and downstream ML feature engineering.
* **Data Quality Validation:** Row-level quality rule enforcement.
* **Schema Standardization:** Consistent data types, naming conventions, and deduplication.
* **Quarantine Routing:** Failed records are automatically routed to Governance Quarantine with context.

#### Gold Layer — Business Aggregates & Analytics
* **Purpose:** Pre-aggregated, business-level datasets optimized for reporting and ML.
* **Customer 360 Views:** Unified customer profiles across all 4 domains.
* **Aggregated Metrics:** Star schema / denormalized models for executive analytics and dashboards.

#### Governance & Infrastructure
Cross-layer capabilities for data quality, lineage, and compliance:
* **Quarantine Table:** Centralized tracking for failed validation rules. Stores one record per failed rule, preserving the original Bronze payload for root cause analysis and remediation.
* **Audit Logs:** Full lineage and execution metadata, tracking performance, metrics, and compliance logs across all pipeline stages.
* **Infrastructure Lifecycle:** Unity Catalog schemas and governance state tables represent persistent infrastructure. They are preloaded once via SQL scripts, while application bundles deploy and own pipelines, jobs, and source code updates without destroying existing catalogs.

---

## Repository Structure

```plaintext
zero-trust-banking-pipeline/
├── .github/
│   └── workflows/
│       └── ci-cd.yml           # GitHub Actions CI/CD pipeline
├── src/
│   ├── pipeline/              # Core pipeline modules
│   │   ├── source_landing/    # Auto Loader ingestion
│   │   ├── bronze/            # SCD Type 2/1 bronze layer logic
│   │   ├── silver/            # Validation and normalization
│   │   ├── monitoring/        # Audit and metrics
│   │   └── README.md          # Detailed pipeline runbook
│   └── data_contracts/        # Schemas and rules
│       ├── schemas/           # Table schema definitions
│       ├── quality_rules/     # Data quality rule registry
│       ├── audit/             # Audit logging modules
│       ├── table_catalog.py   # Table metadata and keys
│       └── normalization.py   # Data normalization functions
├── tests/
│   ├── test_quality_rules.py  # Unit tests for quality rules
│   ├── run_unit_tests.ipynb   # Notebook-based test runner
│   └── README.md              # Testing guide
├── resources/                 # Databricks Bundle resources
│   ├── pipelines.yml          # DLT pipeline definitions
│   ├── jobs.yml               # Job definitions
│   └── volumes.yml            # UC Volume definitions
├── deliverables/              # Implementation evidence and runbooks
│   └── Team_Workspace_Pipeline_Technical_Runbook.md
├── docs/                      # Architecture documentation
│   ├── Banking_Silver_Atomic_Warehouse.dbml
│   ├── banking_daily_change_catalog.md
│   ├── banking_error_injection_catalog.md
│   ├── customer_360_silver_guide.md
│   └── error_injection_rule_mapping.md
├── sql/                       # SQL queries and exploration
│   └── customer_360/          # Customer 360 exploration SQL
├── scripts/                   # Utility scripts
│   └── source_landing/        # Data upload helpers
├── notebooks/                 # Ad-hoc analysis notebooks
├── configs/                   # Configuration files
├── data/                      # Sample/test data
├── databricks.yml             # Bundle configuration
├── requirements.txt           # Python dependencies
├── .gitignore
└── README.md                  # Root documentation
```

---

## Quick Start & Setup

### Prerequisites
* Python 3.10+ installed locally
* Databricks CLI installed and configured
* Unity Catalog enabled workspace with appropriate schema/volume permissions
* Git for version control

### Initial Setup (5 minutes)

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd zero-trust-banking-pipeline
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Databricks CLI:**
   ```bash
   databricks auth login --host <workspace-url>
   # Or configure profile via: databricks configure
   ```

4. **Preload Unity Catalog infrastructure:**
   Execute the preload SQL script to set up schemas, volumes, and governance tables. Refer to the Pipeline runbook for full SQL statements.

5. **Validate and deploy bundle:**
   ```bash
   databricks bundle validate --target dev
   databricks bundle deploy --target dev
   ```

6. **Run the pipeline:**
   ```bash
   # Run silver validation job
   databricks bundle run full_source_to_validated_silver -t dev

   # Or run end-to-end to Gold
   databricks bundle run full_source_to_gold -t dev
   ```

---

## Engineering Workflow

### Branch Strategy
* `main`: Production-ready code only. Direct commits are restricted; changes require PR approvals. Merges trigger production deployment.
* `dev`: Integration branch for ongoing development. Feature branches target `dev` first.
* `feature/*`: Feature-specific branches created from `dev`.

### Developer Cycle
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

## Testing & Validation

Testing occurs across multiple levels:

### 1. Local Unit Tests
```bash
# Using pytest
pytest tests/ -v --cov=src --cov-report=term-missing

# Using unittest
PYTHONPATH=src python -m unittest discover -s tests -p 'test_*.py'
```

### 2. Static Code Analysis
```bash
black src/ tests/
isort src/ tests/
flake8 src/ tests/ --max-line-length=127
```

### 3. Integration & End-to-End Testing (Databricks)
```bash
# Validate bundle schema and variables
databricks bundle validate --target dev

# Run integration tests remotely
databricks bundle run run_integration_tests -t dev

# Run full pipeline execution
databricks bundle run full_source_to_validated_silver -t dev
```

### 4. CI/CD Automation (GitHub Actions)
Located at `.github/workflows/ci-cd.yml`:
* **On push/PR to `main` or `dev`:** Runs code quality checks (`flake8`, `black`, `isort`), parallel unit tests on Python 3.10/3.11/3.12, and bundle syntax validation.
* **On merge to `main`:** Automatically deploys to Dev, executes integration tests on Databricks, and promotes/deploys to the production/team workspace.
* **Secret Configuration:** Ensure `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are configured in GitHub Repository Secrets.

---

## Security & Configuration

* **Zero Hardcoded Credentials:** No passwords, tokens, or workspace URLs are committed to source control.
* **Local Authentication:** Handled via standard Databricks CLI configuration in `~/.databrickscfg`.
* **Variable Overrides:** Stored locally in `.databricks/bundle/<target>/variable-overrides.json` (gitignored).
* **Cloud Storage Access:** Unity Catalog Storage Credentials and External Locations govern S3 access. *(Note: Free Edition uses temporary secret scopes as documented in the runbook).*
* **Gitignore Safety:** `.databricks/`, `__pycache__/`, `.pytest_cache/`, `.coverage`, and environment files are strictly excluded.

---

## Troubleshooting

| Issue | Cause | Solution |
| :--- | :--- | :--- |
| **Bundle validation fails** | YAML syntax error or missing variable | Run `databricks bundle validate --target dev` and inspect missing resource key/variable definitions. |
| **Local unit tests fail** | Missing `PYTHONPATH` or packages | Run `pip install -r requirements.txt` and set path: `export PYTHONPATH=src`. |
| **CLI auth failure** | Expired token or invalid host | Re-authenticate using `databricks auth login --host <workspace-url>`. |
| **Job run failures** | Schema mismatch or missing Volume | Check job execution logs in Databricks UI, verify UC volume path, and consult the Quarantine table for validation rule errors. |
| **Notebook import error** | Missing workspace deployment | Ensure `databricks bundle deploy` has been executed on the target environment. |

---

## Documentation & References

* **Pipeline Runbook** — Detailed setup, operational runbook, and deployment guide.
* **Testing Guide** — Guide to local unit and integration testing workflows.
* **Banking Silver Warehouse (DBML)** — Full data model and relational diagrams.
* **Daily Change Catalog** — SCD change detection and handling specs.
* **Error Injection Catalog** — Quality rule taxonomy and error handling definitions.
* **Customer 360 Guide** — Guide to querying Customer 360 Silver views.

---

## Tech Stack & Dependencies

* **Language:** Python 3.10+
* **Engine:** PySpark 3.4+, Databricks Runtime
* **Governance:** Databricks Unity Catalog
* **Deployment:** Databricks Declarative Automation Bundles (DABs)
* **Testing:** pytest, unittest
* **CI/CD:** GitHub Actions
