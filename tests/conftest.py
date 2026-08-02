
"""
Pytest configuration and fixtures for pipeline tests.
"""
import pytest
from pyspark.sql import SparkSession
import os
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("pipeline-tests")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

@pytest.fixture(scope="session")
def test_catalog():
    """Test catalog name."""
    return "test_catalog"

@pytest.fixture(scope="session")
def test_schema():
    """Test schema name."""
    return "test_schema"

@pytest.fixture
def sample_dataframe(spark):
    """Create a sample DataFrame for testing."""
    data = [
        ("CB-001", "John Doe", "123456789"),
        ("CRM-002", "Jane Smith", "987654321"),
    ]
    return spark.createDataFrame(data, ["cust_no", "full_name", "national_id"])