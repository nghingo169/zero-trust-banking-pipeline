"""
Pytest configuration and fixtures for pipeline tests.
"""
import os
import sys
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

venv_python = sys.executable
os.environ["PYSPARK_PYTHON"] = venv_python
os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python

project_root = Path(__file__).parent.parent
src_dir = project_root / "src"

paths_to_inject = [
    str(project_root),
    str(src_dir),
    str(src_dir / "data_contracts"),
    str(src_dir / "pipeline"),
    str(src_dir / "pipeline" / "silver"),
    str(src_dir / "pipeline" / "bronze"),
    str(src_dir / "pipeline" / "gold"),
]
for p in paths_to_inject:
    if p not in sys.path:
        sys.path.insert(0, p)

# tests/conftest.py

@pytest.fixture(scope="session")
def spark():
    """Khởi tạo Spark Session Local In-Memory nhẹ nhàng."""
    try:
        active = SparkSession.getActiveSession()
        if active:
            active.conf.set("spark.sql.stackTracesInDataFrameContext", "1")
            return active
    except Exception:
        pass

    session = (
        SparkSession.builder.master("local[2]")
        .appName("pipeline-tests")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.stackTracesInDataFrameContext", "1")
        .getOrCreate()
    )
    session.conf.set("spark.sql.stackTracesInDataFrameContext", "1")
    return session

@pytest.fixture(scope="session")
def test_catalog():
    return "workspace"


@pytest.fixture(scope="session")
def test_schema():
    return "test_schema"


@pytest.fixture
def sample_dataframe(spark):
    data = [
        ("CB-001", "John Doe", "123456789"),
        ("CRM-002", "Jane Smith", "987654321"),
    ]
    return spark.createDataFrame(data, ["cust_no", "full_name", "national_id"])