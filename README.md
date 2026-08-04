# Zero-trust banking pipeline

[![CI/CD Pipeline](https://github.com/your-org/zero-trust-banking-pipeline/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/your-org/zero-trust-banking-pipeline/actions)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Databricks](https://img.shields.io/badge/databricks-runtime-orange.svg)](https://docs.databricks.com/)

A production-grade Databricks lakehouse pipeline for ingesting banking source snapshots with comprehensive data quality validation, SCD Type 2 history tracking, and automated quarantine management.

## Overview

This project implements a zero-trust data pipeline that:
* Processes **41 banking source tables** across Customer, Card, Transaction, and Financial Crime domains
* Implements **SCD Type 2** for historical snapshot entities and **SCD Type 1** for immutable events
* Enforces **row-level data quality rules** with automatic quarantine routing
* Provides **full audit logging** and quality metrics tracking
* Uses **Databricks Declarative Automation Bundles** for multi-environment deployment
* Includes **automated CI/CD pipeline** with parallel testing and validation

### Architecture flow

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
    🥇 GOLD LAYER (Aggregated)
    ================================
    Customer 360 views
    Pre-aggregated metrics
    Business-ready datasets
              ↓
    Dashboards, Reports, ML Models
```

## Architecture components

### Medallion architecture layers

The pipeline implements the **Medallion architecture** pattern with three progressive data quality layers:

#### Bronze layer - Raw data ingestion
* **Purpose**: Immutable landing zone preserving full source history
* **SCD Type 2** for snapshot entities (customer, account, card)
  * Tracks historical changes with effective dates (valid_from, valid_to)
  * Maintains complete audit trail of all state changes
* **SCD Type 1** for immutable events (transactions, status changes)
  * Events with composite business keys and timestamps
  * No history tracking needed (events are point-in-time facts)
* **Metadata enrichment**: business_date, load_timestamp, source_file
* **Schema**: Minimal transformations, preserves raw source structure

#### Silver layer - Validated & normalized
* **Purpose**: Clean, conformed data ready for analytics
* **Data quality validation**: Row-level quality rules enforcement
* **Schema standardization**: Consistent data types, naming conventions
* **Business rule application**: Normalization, enrichment, deduplication
* **Quarantine routing**: Failed records automatically routed to governance quarantine
* **Status**: Production-ready for reporting and ML feature engineering

#### Gold layer - Business aggregates
* **Purpose**: Pre-aggregated, business-level datasets
* **Customer 360 views**: Unified customer profiles across domains
* **Aggregated metrics**: Pre-calculated KPIs and business metrics
* **Analytics-optimized**: Star schema, denormalized for performance
* **Use cases**: Dashboards, reports, executive analytics

### Governance I=infrastructure

Cross-layer capabilities for data quality and compliance:

* **Quarantine table** - Centralized quality failure tracking
  * One record per failed validation rule with full context
  * Original Bronze payload preserved for root cause analysis
  * Enables data quality monitoring and remediation workflows

* **Audit logs** - Comprehensive lineage and compliance tracking
  * Pipeline execution metadata and performance metrics
  * Data lineage from source to gold
  * Regulatory compliance audit trail

## Quick start

### Prerequisites

* **Python 3.10 or higher** installed locally
* **Databricks CLI** installed and configured
* **Unity Catalog** enabled workspace with appropriate permissions
* **Git** for version control

### Initial Setup (5 minutes)

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd zero-trust-banking-pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Databricks CLI**
   ```bash
   databricks configure
   # Or use: databricks auth login --host <workspace-url>
   ```

4. **Set up Unity Catalog infrastructure**
   ```bash
   # Create required schemas and volumes
   # See src/pipeline/README.md for detailed setup
   ```

5. **Validate and deploy bundle**
   ```bash
   databricks bundle validate --target dev
   databricks bundle deploy --target dev (commment)
   databricks bundle deploy --target team
   ```

6. **Run the pipeline**
   ```bash
   databricks bundle run full_source_to_validated_silver -t dev
   ```

### For New Engineers

Read the comprehensive guide:

📖 **[Pipeline Runbook](src/pipeline/README.md)** - Complete setup, deployment, and troubleshooting guide

## 📚 Repository Structure

```text
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
├── docs/                      # Architecture documentation
│   ├── Banking_Silver_Atomic_Warehouse.dbml
│   ├── banking_daily_change_catalog.md
│   ├── banking_error_injection_catalog.md
│   ├── customer_360_silver_guide.md
│   └── error_injection_rule_mapping.md
├── sql/                       # SQL queries and exploration
├── scripts/                   # Utility scripts
│   └── source_landing/        # Data upload helpers
├── notebooks/                 # Ad-hoc analysis notebooks
├── configs/                   # Configuration files
├── data/                      # Sample/test data
├── databricks.yml             # Bundle configuration
├── requirements.txt           # Python dependencies
├── .gitignore
└── README.md                  # This file
```

## Configuration and security

The committed Bundle configuration contains no workspace URL, personal source
path, token, AWS access key, or secret. Each engineer uses:

- a local Databricks CLI profile in `~/.databrickscfg` for workspace access;
- an ignored `.databricks/bundle/<target>/variable-overrides.json` file for
  local Bundle values; and
- Unity Catalog storage credentials/external locations for production S3
  access.

Never commit credentials, access tokens, or personal workspace paths.

## Engineering workflow

### Development cycle

1. **Create feature branch**
   ```bash
   git checkout dev
   git pull origin dev
   git checkout -b feature/your-feature-name
   ```

2. **Make changes and test locally**
   ```bash
   # Run unit tests
   pytest tests/ -v
   
   # Check code quality
   black src/ tests/
   isort src/ tests/
   flake8 src/ tests/
   ```

3. **Validate bundle**
   ```bash
   databricks bundle validate --target dev
   ```

4. **Test deployment locally**
   ```bash
   databricks bundle deploy --target dev
   databricks bundle run run_integration_tests -t dev
   ```

5. **Commit and push**
   ```bash
   git add .
   git commit -m "feat: your descriptive commit message"
   git push origin feature/your-feature-name
   ```

6. **Create pull request**
   * Open PR from `feature/your-feature-name` → `dev`
   * CI/CD pipeline automatically runs quality checks and tests
   * Address any feedback from reviewers

7. **Merge and promote**
   * After approval, merge to `dev` for integration testing
   * When ready for production, create PR from `dev` → `main`
   * Merging to `main` triggers full CI/CD pipeline with production deployment

### Branch strategy

* **`main`** - Production-ready releases only
  * Protected branch requiring PR reviews
  * Merges trigger full CI/CD with dev and production deployment
  * Always deployable to production

* **`dev`** - Integration branch for ongoing development
  * Feature branches merge here first
  * Integration testing happens here
  * Staging ground for production releases

* **`feature/*`** - Individual feature development
  * Created from `dev`
  * Merged back to `dev` after review
  * Deleted after merge

## Testing & validation

Comprehensive testing at multiple levels ensures pipeline reliability and data quality.

### 1. Local unit tests (Pre-commit)

Run unit tests before every commit:

```bash
# Using pytest (recommended)
pytest tests/ -v --cov=src --cov-report=term-missing

# Or using unittest
PYTHONPATH=src python -m unittest discover -s tests -p 'test_*.py'
```

**What's tested:**
* Quality rule validation logic
* Data normalization functions
* Schema validation and enforcement
* Error handling and edge cases

### 2. Code quality checks

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Lint code
flake8 src/ tests/ --max-line-length=127
```

### 3. Bundle validation

Validate bundle configuration before deployment:

```bash
databricks bundle validate --target dev
```

This checks:
* YAML syntax and structure
* Resource dependencies
* Variable substitution
* Permission requirements

### 4. Integration tests (Databricks)

Run integration tests on Databricks compute:

```bash
# Deploy and run integration tests
databricks bundle deploy --target dev
databricks bundle run run_integration_tests -t dev
```

**Integration test coverage:**
* End-to-end pipeline execution
* Data quality rule enforcement
* Quarantine routing logic
* Audit log generation
* Bronze-to-Silver transformations

### 5. End-to-end pipeline validation

Run the full pipeline to validate entire data flow:

```bash
databricks bundle run full_source_to_validated_silver -t dev
```

This validates:
* Source ingestion from Volume/S3
* Bronze layer SCD logic
* Data validation and normalization
* Silver layer creation
* Quarantine management
* Audit logging

### CI/CD Automation

The project includes a GitHub Actions workflow (`.github/workflows/ci-cd.yml`) that automatically:

**On every push/PR to `main`:**
1. **Code quality checks** - Linting (flake8), formatting (black), import sorting (isort)
2. **Parallel unit tests** - Run tests across Python 3.10, 3.11, 3.12
3. **Bundle validation** - Verify bundle configuration

**On merge to `main` branch:**
1. **Deploy to Dev** - Automatic deployment to dev environment
2. **Integration Tests** - Run `run_integration_tests` job on Databricks
3. **Deploy to Team (Production)** - Automatic deployment to team environment

**Setup:**
- Configure GitHub Secrets: `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
- Pipeline runs automatically on push/PR - no manual intervention needed
- View workflow results in GitHub **Actions** tab

## Security & configuration

### Credential management

The project follows security best practices:

* **No hardcoded credentials** - All configs are parameterized
* **Local CLI profiles** - Use `~/.databrickscfg` for workspace authentication
* **Variable overrides** - `.databricks/bundle/<target>/variable-overrides.json` for local values (gitignored)
* **UC Storage Credentials** - Production S3 access via Unity Catalog external locations
* **GitHub Secrets** - CI/CD uses encrypted secrets for DATABRICKS_HOST and DATABRICKS_TOKEN

### What's Ignored (.gitignore)

```text
.databricks/         # Local bundle state and variable overrides
__pycache__/         # Python bytecode
*.pyc
.pytest_cache/       # Test cache
.coverage            # Coverage reports
*.egg-info/          # Package metadata
.DS_Store            # macOS files
```

**Never commit:**
* Workspace URLs or personal paths
* Access tokens or API keys
* AWS credentials or secrets
* Personal data or customer information

## Troubleshooting

### Common issues

**Bundle validation fails**
```bash
# Check YAML syntax
databricks bundle validate --target dev

# Review error message for specific issues
# Common causes: missing variables, invalid resource refs
```

**Tests fail locally**
```bash
# Ensure dependencies are installed
pip install -r requirements.txt

# Set PYTHONPATH
export PYTHONPATH=src
pytest tests/ -v
```

**CLI authentication issues**
```bash
# Reconfigure CLI
databricks auth login --host <workspace-url>

# Or use configure
databricks configure
```

**Job run failures**
* Check job run logs in Databricks UI
* Verify compute resources are available
* Confirm Unity Catalog permissions
* Review data quality for source files
* Check quarantine table for validation failures

**Import errors in notebooks**
* Ensure bundle is deployed to correct target
* Verify notebook is using correct cluster
* Check that src/ modules are in workspace

### Getting help

1. Check [Pipeline Runbook](src/pipeline/README.md) for detailed guidance
2. Review [docs/](docs/) for architecture documentation
3. Search existing GitHub issues
4. Ask in team Slack channel
5. Create new GitHub issue with details

## Documentation

### Available guides

* **[Pipeline Runbook](src/pipeline/README.md)** - Complete setup and operations guide
* **[Testing Guide](tests/README.md)** - Unit and integration testing
* **[Banking Silver Warehouse](docs/Banking_Silver_Atomic_Warehouse.dbml)** - Data model DBML
* **[Daily Change Catalog](docs/banking_daily_change_catalog.md)** - Change detection patterns
* **[Error Injection Catalog](docs/banking_error_injection_catalog.md)** - Quality rule catalog
* **[Customer 360 Guide](docs/customer_360_silver_guide.md)** - Customer analytics queries

## Dependencies

Key technologies:

* **Python 3.10+** - Core language
* **PySpark 3.4+** - Data processing
* **pytest** - Unit testing framework
* **Databricks Runtime** - Execution environment
* **Unity Catalog** - Data governance
* **Declarative Automation Bundles** - Deployment framework

See [requirements.txt](requirements.txt) for complete dependency list.

---

**Built with ❤️ by the Data Engineering Team**
