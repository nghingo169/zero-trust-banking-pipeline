"""
Unit tests for pipeline.silver.fincrime_validation module.

Tests:
- TABLES configuration structure mapping.
- Data normalization and rule evaluation via assess().
- Proper attachment of _failed_rule_names column for valid, invalid, and empty rule scenarios.
"""

from pathlib import Path
import os
import sys
import builtins
from types import ModuleType
from unittest.mock import MagicMock, patch
import pytest

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION FOR 'src' AND 'pipeline' PACKAGES
# ------------------------------------------------------------------------------
current_file = Path(__file__).resolve()

project_root = None
for parent in [current_file] + list(current_file.parents):
    if (parent / "pipeline").exists() or (parent / "src" / "pipeline").exists():
        project_root = parent
        break

if project_root:
    if (project_root / "src").exists():
        src_path = str(project_root / "src")
        if src_path not in sys.path:
            sys.path.insert(0, src_path)
    root_path = str(project_root)
    if root_path not in sys.path:
        sys.path.insert(0, root_path)

silver_dir = str(current_file.parent)
if silver_dir not in sys.path:
    sys.path.insert(0, silver_dir)


# ------------------------------------------------------------------------------
# 2. COMPLETE PACKAGE & DLT MOCKS FOR PYTEST EXECUTION
# ------------------------------------------------------------------------------
# Root mock cho data_contracts
if "data_contracts" not in sys.modules:
    try:
        import data_contracts
    except ImportError:
        dc_pkg = ModuleType("data_contracts")
        dc_pkg.__path__ = []
        sys.modules["data_contracts"] = dc_pkg

if "data_contracts.normalization" not in sys.modules:
    try:
        import data_contracts.normalization
    except ImportError:
        norm_mock = ModuleType("data_contracts.normalization")
        # Giả lập normalize giữ nguyên DataFrame hoặc thêm logic chuẩn hóa cơ bản
        norm_mock.normalize = lambda df: df
        sys.modules["data_contracts.normalization"] = norm_mock

if "data_contracts.quality_rules" not in sys.modules:
    try:
        import data_contracts.quality_rules
    except ImportError:
        qr_pkg = ModuleType("data_contracts.quality_rules")
        qr_pkg.__path__ = []
        sys.modules["data_contracts.quality_rules"] = qr_pkg

if "data_contracts.quality_rules.registry" not in sys.modules:
    try:
        import data_contracts.quality_rules.registry
    except ImportError:
        reg_mock = ModuleType("data_contracts.quality_rules.registry")
        # Đăng ký một số quy tắc giả lập cho fincrime
        reg_mock.RULES_BY_TABLE = {
            "aml_alert": [
                {"name": "alert_id_not_null", "constraint": "alert_id IS NOT NULL"},
                {"name": "score_positive", "constraint": "score > 0"}
            ]
        }
        sys.modules["data_contracts.quality_rules.registry"] = reg_mock

if "data_contracts.table_catalog" not in sys.modules:
    try:
        import data_contracts.table_catalog
    except ImportError:
        mock_catalog = ModuleType("data_contracts.table_catalog")
        mock_catalog.DOMAINS = {
            "fincrime": {
                "scd2": {"aml_case": "case_id"},
                "append": {"aml_alert": "alert_id"}
            }
        }
        sys.modules["data_contracts.table_catalog"] = mock_catalog


# ------------------------------------------------------------------------------
# 3. SPARK SESSION FIXTURE
# ------------------------------------------------------------------------------
try:
    active_spark = spark  # type: ignore # noqa: F821
except NameError:
    active_spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("FincrimeValidation-UnitTest")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    builtins.spark = active_spark
    globals()["spark"] = active_spark


@pytest.fixture(scope="module")
def test_spark():
    return active_spark


# ------------------------------------------------------------------------------
# 4. SAFE IMPORT OF TARGET MODULE
# ------------------------------------------------------------------------------
try:
    from pipeline.silver import fincrime_validation
except ImportError:
    import fincrime_validation


# ==============================================================================
# UNIT TEST CASES
# ==============================================================================

def test_fincrime_tables_mapping():
    """Verify TABLES dict properly combines scd2 and append tables for fincrime domain."""
    assert "aml_case" in fincrime_validation.TABLES
    assert "aml_alert" in fincrime_validation.TABLES
    assert fincrime_validation.TABLES["aml_case"] == "case_id"
    assert fincrime_validation.TABLES["aml_alert"] == "alert_id"


def test_assess_attaches_failed_rule_names(test_spark):
    """Verify assess identifies rule failures and appends failed rule names into _failed_rule_names."""
    schema = StructType([
        StructField("alert_id", StringType(), True),
        StructField("score", DoubleType(), True)
    ])

    data = [
        ("ALT_01", 85.5),   # Valid record -> Should pass all rules
        ("ALT_02", -5.0),   # Score <= 0 -> Fails 'score_positive'
        (None, 50.0)        # Null ID -> Fails 'alert_id_not_null'
    ]
    df_input = test_spark.createDataFrame(data, schema)

    mock_rules = {
        "aml_alert": [
            {"name": "alert_id_not_null", "constraint": "alert_id IS NOT NULL"},
            {"name": "score_positive", "constraint": "score > 0"}
        ]
    }

    with patch("data_contracts.quality_rules.registry.RULES_BY_TABLE", mock_rules):
        res_df = fincrime_validation.assess(df_input, "aml_alert")
        rows = {r.alert_id: r for r in res_df.collect()}

        # 1. Valid Record
        assert len(rows["ALT_01"]._failed_rule_names) == 0

        # 2. Score <= 0 Failed Record
        assert "score_positive" in rows["ALT_02"]._failed_rule_names
        assert len(rows["ALT_02"]._failed_rule_names) == 1

        # 3. Null alert_id Record
        null_row = next(r for r in res_df.collect() if r.alert_id is None)
        assert "alert_id_not_null" in null_row._failed_rule_names
        assert len(null_row._failed_rule_names) == 1


def test_assess_table_with_no_rules_configured(test_spark):
    """Verify assess handles tables without any configured quality rules gracefully."""
    schema = StructType([
        StructField("case_id", StringType(), True),
        StructField("status", StringType(), True)
    ])
    df_input = test_spark.createDataFrame([("CASE_100", "OPEN")], schema)

    with patch("data_contracts.quality_rules.registry.RULES_BY_TABLE", {}):
        res_df = fincrime_validation.assess(df_input, "unregistered_table")
        row = res_df.first()

        assert "_failed_rule_names" in res_df.columns
        assert len(row._failed_rule_names) == 0


if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])