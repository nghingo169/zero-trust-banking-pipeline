# Zero-Trust Banking Pipeline

A Databricks lakehouse pipeline for ingesting banking source snapshots, keeping
history where it matters, validating data quality, and routing failed records
to a centralized quarantine table.

The project uses Databricks Declarative Automation Bundles (Bundles) so the
same source code can be deployed from each engineer's local machine to their
own Databricks workspace. `main` is reserved for production-ready releases;
ongoing integration work belongs on the remote `dev` branch.

## What the pipeline does

```text
Source snapshot files
  -> source landing Volume
  -> Bronze
  -> normalize and validate
  -> validated Silver or governance quarantine
  -> audit logs and quality metrics
```

The pipeline processes 41 banking source tables across Customer, Card,
Customer Transaction, and Financial Crime domains.

- **Bronze** retains source lineage and manages historical snapshot entities as
  SCD Type 2.
- **Status-event tables** are immutable events and use SCD Type 1, with their
  real composite business key.
- **Validated Silver** contains records that pass normalization and row-level
  quality rules.
- **Governance quarantine** contains one record per failed rule, including the
  original Bronze payload for investigation.

## Start here

Read the full local setup, deployment, run, and troubleshooting guide:

**[Pipeline runbook](deliverables/Team_Workspace_Pipeline_Technical_Runbook.md)**

For a new engineer, the normal workflow is:

1. Authenticate the Databricks CLI to your own workspace.
2. Create the required Unity Catalog schemas and source landing Volume.
3. Upload source snapshots to the Volume.
4. Validate and deploy the Bundle.
5. Run the `full_source_to_validated_silver` job.
6. Review job results, validated Silver tables, quarantine records, and audit
   logs.

## Repository structure

```text
.github/workflows/          CI/CD pipeline configuration
databricks.yml              Bundle settings and portable variables
resources/                  Pipeline, job, and Volume resource definitions
src/pipeline/               Active ingestion, validation, and audit code
src/data_contracts/         Schemas, table keys, normalization, and quality rules
docs/                       Architecture and data-model documentation
sql/customer_360/           Customer 360 exploration and validation SQL
scripts/source_landing/     Local snapshot upload helper
tests/                      Unit tests for quality-rule behaviour
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

1. Create a feature branch from `dev`.
2. Make and test the change locally.
3. Run `databricks bundle validate --target dev --profile <your-profile>`.
4. Open a pull request into `dev`.
5. **CI/CD pipeline automatically runs** (on push/PR to `main`):
   - Code quality checks (flake8, black, isort)
   - Unit tests across Python 3.10, 3.11, 3.12
   - Bundle validation
   - Deploy to dev environment (on merge to `main`)
   - Integration tests on Databricks
   - Deploy to production (team environment)
6. Promote reviewed, production-ready changes from `dev` to `main`.

## Verification

The repository includes unit tests for the shared quality-rule registry.

### Local Testing

Run unit tests locally before committing:

```bash
PYTHONPATH=src python -m unittest discover -s tests -p 'test_*.py'
```

### Automated Testing via Bundle Jobs

The project includes automated test jobs that can be executed via bundle commands:

**Run all integration tests:**
```bash
databricks bundle run run_integration_tests -t team --profile <your-profile>
```

This job executes all tests in the `tests/` folder, providing comprehensive validation of pipeline components including quality rules, data contracts, and transformation logic.

**Run the full pipeline end-to-end:**
```bash
databricks bundle run full_source_to_validated_silver -t team --profile <your-profile>
```

This job runs the complete pipeline from source landing through validated Silver tables, serving as an executable validation of the entire data flow.

### Bundle Validation

Before deployment, also run Bundle validation as described in the
[pipeline runbook](deliverables/Team_Workspace_Pipeline_Technical_Runbook.md):

```bash
databricks bundle validate --target dev --profile <your-profile>
```

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
