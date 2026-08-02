# Unit Tests

This directory contains comprehensive unit and integration tests for the Zero-Trust Banking Pipeline.

## Test Structure

```text
tests/
  test_quality_rules.py       Unit tests for quality rule validation logic
  test_*.py                   Additional test modules (add as needed)
```

## Running Tests

### Option 1: Local Testing (Development)

Run tests locally during development before committing:

```bash
# From the project root directory
PYTHONPATH=src python -m unittest discover -s tests -p 'test_*.py'
```

For pytest (recommended):

```bash
# Run all tests
pytest tests/

# Run with verbose output
pytest -v tests/

# Run with coverage report
pytest --cov=src tests/

# Run specific test file
pytest tests/test_quality_rules.py
```

### Option 2: Automated Testing via Bundle Job

Run all integration tests through the bundle-deployed job:

```bash
databricks bundle run run_integration_tests -t team --profile <your-profile>
```

This job:
* Executes all tests in the `tests/` folder on Databricks compute
* Provides comprehensive validation of pipeline components
* Validates quality rules, data contracts, and transformation logic
* Useful for CI/CD and pre-deployment verification

### Option 3: End-to-End Pipeline Validation

Run the complete pipeline as an executable validation:

```bash
databricks bundle run full_source_to_validated_silver -t team --profile <your-profile>
```

This validates the entire data flow from source to validated Silver tables.

## Test Coverage

The test suite currently covers:

* **Quality Rules**: Validation logic for data quality checks
* **Data Contracts**: Schema validation and enforcement
* **Transformation Logic**: Bronze to Silver transformations
* **Normalization**: Data normalization rules and functions

## Adding New Tests

1. Create a new test file following the naming convention `test_<module_name>.py`
2. Import the module or functions you want to test from `src/`
3. Write test cases using `unittest.TestCase` or `pytest` syntax
4. Ensure tests can run both locally and on Databricks compute

### Example Test Structure

```python
import unittest
from src.data_contracts.quality_rules import validate_rule

class TestQualityRules(unittest.TestCase):
    def test_validation_logic(self):
        # Arrange
        test_data = {...}
        
        # Act
        result = validate_rule(test_data)
        
        # Assert
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
```

## Best Practices

* **Isolation**: Each test should be independent and not rely on external state
* **Naming**: Use descriptive test names that explain what is being tested
* **Assertions**: Include clear assertion messages for debugging
* **Coverage**: Aim for high test coverage of critical pipeline logic
* **Speed**: Keep unit tests fast; use mocks for external dependencies
* **Documentation**: Add docstrings to test classes and methods

## Integration with CI/CD

These tests are fully integrated into the GitHub Actions CI/CD pipeline (`.github/workflows/ci-cd.yml`):

### Automated Test Execution

**1. Local Development (Pre-commit)**
```bash
# Run before committing
pytest tests/
```

**2. GitHub Actions Pipeline (Automatic)**

On every push or pull request to `main`:

* **Code Quality Stage**
  - Runs flake8 (linting)
  - Checks black formatting
  - Validates import sorting (isort)

* **Unit Tests Stage** (Parallel)
  - Runs all tests in `tests/` folder
  - Tests across Python 3.10, 3.11, 3.12 simultaneously
  - Generates code coverage reports
  - **Fails pipeline if any test fails**

* **Integration Tests Stage** (After Deploy to Dev)
  - Triggers `run_integration_tests` job on Databricks
  - Runs all tests on actual Databricks compute
  - Validates end-to-end pipeline functionality
  - **Blocks production deployment if tests fail**

**3. Manual Bundle Job Execution**
```bash
# Run integration tests via bundle
databricks bundle run run_integration_tests -t team --profile <your-profile>
```

### CI/CD Test Flow

```text
Push/PR to main → Code Quality → Unit Tests (3.10, 3.11, 3.12) → Bundle Validate
                      ✓              ✓                              ✓
                                                                         ↓
                                                    Merge to main → Deploy Dev
                                                                         ↓
                                                              Integration Tests
                                                                         ✓
                                                                         ↓
                                                            Deploy Team (Production)
```

### Viewing Test Results

* **Local**: Terminal output from `pytest` command
* **GitHub Actions**: Navigate to repo → **Actions** tab → Select workflow run
* **Databricks**: Job runs page for `run_integration_tests` job

## Troubleshooting

**Import errors**: Ensure `PYTHONPATH=src` is set when running locally

**Test failures**: Check that:
* Required dependencies are installed (`pip install -r requirements.txt`)
* Test data fixtures are properly configured
* Environment variables are set if needed

**Bundle job failures**: Verify:
* Bundle is deployed (`databricks bundle deploy -t team`)
* Compute resources are available
* Job permissions are properly configured