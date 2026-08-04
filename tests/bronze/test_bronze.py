# Databricks notebook source
from pathlib import Path
import os
import sys
import builtins
import datetime
import tempfile
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock
import pytest

import pyspark
from pyspark.errors import AnalysisException
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import DecimalType, LongType, StringType

# ------------------------------------------------------------------------------
# 1. DYNAMIC PATH RESOLUTION
# ------------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_SRC = str(PROJECT_ROOT / "src")
BRONZE_SRC = str(PROJECT_ROOT / "src" / "pipeline" / "bronze")

for path_str in [str(PROJECT_ROOT), PROJECT_SRC, BRONZE_SRC]:
    if os.path.exists(path_str) and path_str not in sys.path:
        sys.path.insert(0, path_str)

try:
    test_spark_session = spark  # Databricks Runtime Context
except NameError:
    test_spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("Pipeline-UnitTest-Bronze")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

builtins.spark = test_spark_session
# source_to_bronze_ingestion calls dbutils.fs.ls(...) inside
builtins.dbutils = MagicMock(name="dbutils")

# Local temp directory standing in for the Bronze snapshot source root - no
# S3 / Volume needed. Created once at import time (module-level, same as the
# Spark session above), since the module under test reads
# pipeline.source_root at import time too.
_SOURCE_ROOT = f"file:{tempfile.mkdtemp(prefix='bronze_source_root_')}"

# Set default pipeline configs to prevent AnalysisException on Databricks
# Connect - same try/except-swallow pattern as test_customer_transformation.py.
_PIPELINE_CONF_DEFAULTS = {
    "pipeline.source_mode": "volume",
    "pipeline.source_root": _SOURCE_ROOT,
    "pipeline.target_catalog": "workspace",
    "pipeline.target_schema": "bronze",
}
for k, v in _PIPELINE_CONF_DEFAULTS.items():
    try:
        builtins.spark.conf.set(k, v)
    except Exception:
        pass


_conf_cls = type(builtins.spark.conf)
if not hasattr(_conf_cls, "_bronze_test_get_patched"):
    _original_conf_get = _conf_cls.get

    def _patched_conf_get(self, key, default=None):
        try:
            return _original_conf_get(self, key, default)
        except Exception:
            return default

    _conf_cls.get = _patched_conf_get
    _conf_cls._bronze_test_get_patched = True


pipelines_mock = ModuleType("pyspark.pipelines")
pipelines_mock.create_streaming_table = MagicMock(name="create_streaming_table")
pipelines_mock.create_auto_cdc_from_snapshot_flow = MagicMock(name="create_auto_cdc_from_snapshot_flow")
pyspark.pipelines = pipelines_mock
sys.modules["pyspark.pipelines"] = pipelines_mock

# ------------------------------------------------------------------------------
# 2. LOCAL / DATABRICKS SPARK SESSION FIXTURE
# ------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def test_spark():
    return test_spark_session


@pytest.fixture(scope="module")
def fake_dp():
    """The mocked pyspark.pipelines module that source_to_bronze_ingestion imports as `dp`."""
    return pipelines_mock


# ------------------------------------------------------------------------------
# 3. IMPORT TARGET MODULE
# ------------------------------------------------------------------------------
import source_to_bronze_ingestion


@pytest.fixture(autouse=True)
def _reset_business_date_cache():
    """get_available_business_dates() caches its result - reset between tests."""
    source_to_bronze_ingestion.CACHED_BUSINESS_DATES = None
    yield
    source_to_bronze_ingestion.CACHED_BUSINESS_DATES = None


# ==============================================================================
# SECTION 1: SCHEMA VALIDATION + DATA TRANSFORMATION
# apply_schema_hints(df, hints)
# ==============================================================================

def test_schema_hints_cast_columns_to_correct_types_and_values(test_spark):
    df = test_spark.createDataFrame([Row(account_id="123", amount="45.50", label="x")])

    out = source_to_bronze_ingestion.apply_schema_hints(df, "account_id BIGINT, amount DECIMAL(12,2)")
    schema = {f.name: f.dataType for f in out.schema.fields}
    row = out.collect()[0]

    assert isinstance(schema["account_id"], LongType)
    assert isinstance(schema["amount"], DecimalType)
    assert isinstance(schema["label"], StringType)  # not named in hints -> untouched
    assert row["account_id"] == 123
    assert row["amount"] == 45.50


def test_schema_hints_skip_columns_not_present_on_the_df(test_spark):
    """A hint naming a column the snapshot doesn't have must not raise."""
    df = test_spark.createDataFrame([Row(only_col="value")])

    out = source_to_bronze_ingestion.apply_schema_hints(df, "account_id BIGINT, open_date DATE")

    assert out.columns == ["only_col"]


def test_schema_hints_empty_string_is_a_no_op(test_spark):
    df = test_spark.createDataFrame([Row(a="1")])

    out = source_to_bronze_ingestion.apply_schema_hints(df, "")

    assert out.schema == df.schema


def test_schema_hints_malformed_value_casts_to_null_rather_than_raising(test_spark):
    """Data-quality edge case: a non-numeric string cast to DECIMAL -> NULL, not a crash."""
    df = test_spark.createDataFrame([Row(amount="not-a-number")])

    out = source_to_bronze_ingestion.apply_schema_hints(df, "amount DECIMAL(12,2)")

    assert out.collect()[0]["amount"] is None


# ==============================================================================
# SECTION 2: DATA TRANSFORMATION
# add_derived_event_keys(df, table_name)
# ==============================================================================

_EVENT_KEY_SCHEMA = "account_txn_id BIGINT, card_txn_id BIGINT"


def test_derived_event_keys_only_apply_to_payment_gateway_status_event(test_spark):
    df = test_spark.createDataFrame(
        [Row(account_txn_id=1, card_txn_id=None)], _EVENT_KEY_SCHEMA
    )

    out = source_to_bronze_ingestion.add_derived_event_keys(df, "account_transaction")

    assert "event_parent_ref" not in out.columns


@pytest.mark.parametrize(
    "account_txn_id, card_txn_id, expected",
    [
        (42, None, "ACCOUNT:42"),          # account id present -> account ref
        (None, 99, "CARD:99"),             # only card id present -> card ref
        (42, 99, "ACCOUNT:42"),            # both present -> account takes priority
        (None, None, "<MISSING_PARENT>"),  # neither present -> explicit sentinel, not NULL
    ],
)
def test_derived_event_keys_resolve_the_parent_reference_correctly(
    test_spark, account_txn_id, card_txn_id, expected
):
    df = test_spark.createDataFrame(
        [Row(account_txn_id=account_txn_id, card_txn_id=card_txn_id)], _EVENT_KEY_SCHEMA
    )

    out = source_to_bronze_ingestion.add_derived_event_keys(df, "payment_gateway_status_event")

    assert out.collect()[0]["event_parent_ref"] == expected


# ==============================================================================
# SECTION 3: DATA QUALITY
# remove_confirmed_snapshot_replays(df, table_name, keys)
# ==============================================================================

def test_dedup_only_applies_to_account_transaction_status_event(test_spark):
    df = test_spark.createDataFrame(
        [
            Row(status_event_id=1, account_txn_id=100),
            Row(status_event_id=1, account_txn_id=100),  # exact replay
            Row(status_event_id=2, account_txn_id=101),
        ]
    )

    out = source_to_bronze_ingestion.remove_confirmed_snapshot_replays(
        df, "account_transaction_status_event", ["status_event_id", "account_txn_id"]
    )

    assert out.count() == 2


def test_dedup_leaves_every_other_table_untouched(test_spark):
    """
    By design: an unexpected duplicate key on any other table should fail
    visibly downstream rather than be silently dropped here.
    """
    df = test_spark.createDataFrame(
        [Row(account_txn_id=1, amount=10.0), Row(account_txn_id=1, amount=10.0)]
    )

    out = source_to_bronze_ingestion.remove_confirmed_snapshot_replays(
        df, "account_transaction", ["account_txn_id"]
    )

    assert out.count() == 2


# ==============================================================================
# SECTION 4: METADATA
# add_operational_metadata(df, domain, business_date)
# ==============================================================================
WORKSPACE_TMP_DIR = PROJECT_ROOT / ".tmp_pytest"
WORKSPACE_TMP_DIR.mkdir(parents=True, exist_ok=True)

def _read_back(spark, tmp_path, rows, subdir="snapshot"):
    # Generate unique test path inside project workspace directory
    target_dir = WORKSPACE_TMP_DIR / tmp_path.name / subdir
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    target_path = str(target_dir)
    spark.createDataFrame(rows).write.mode("overwrite").parquet(target_path)
    return spark.read.parquet(target_path)


def test_metadata_adds_all_technical_columns_and_drops_layout_only_ones(test_spark, tmp_path):
    raw = _read_back(
        test_spark, tmp_path, [Row(cust_no="CB-1", simulation_id="sim-1", snapshot_type="FULL")]
    )

    out = source_to_bronze_ingestion.add_operational_metadata(
        raw, domain="customer_master", business_date="2026-07-15"
    )

    for col in source_to_bronze_ingestion.TECHNICAL_METADATA_COLUMNS:
        assert col in out.columns, f"missing metadata column: {col}"
    assert "simulation_id" not in out.columns
    assert "snapshot_type" not in out.columns
    assert "cust_no" in out.columns


def test_metadata_business_date_domain_and_timestamps_are_correct(test_spark, tmp_path):
    raw = _read_back(test_spark, tmp_path, [Row(a=1, simulation_id="s", snapshot_type="FULL")])

    out = source_to_bronze_ingestion.add_operational_metadata(
        raw, domain="card", business_date="2026-01-31"
    )
    row = out.collect()[0]

    assert row["domain"] == "card"
    assert row["business_date"] == datetime.date(2026, 1, 31)
    assert row["LOAD_DTTM"] == row["EXTRACT_DTTM"]
    assert row["EXTRACT_DTE"] == row["LOAD_DTTM"].date()


def test_metadata_requires_a_file_backed_dataframe(test_spark):
    """
    An in-memory DataFrame has no `_metadata` column.
    Accessing `out.schema` triggers the Spark Analyzer on the Driver to raise
    the exception without submitting a failed Spark Job to the cluster UI.
    """
    df = test_spark.createDataFrame([Row(a=1, simulation_id="s", snapshot_type="FULL")])

    with pytest.raises(Exception):
        out = source_to_bronze_ingestion.add_operational_metadata(
            df, domain="card", business_date="2026-01-31"
        )
        _ = out.schema 

# ==============================================================================
# SECTION 5: INCREMENTAL LOGIC
# get_available_business_dates(), build_snapshot_flow()'s watermark closure
# ==============================================================================

def _dir(path: str) -> SimpleNamespace:
    """Minimal stand-in for the FileInfo objects dbutils.fs.ls() returns."""
    return SimpleNamespace(path=path)


def test_business_dates_are_parsed_deduped_and_sorted(monkeypatch):
    listing = [
        _dir("dbfs:/root/business_date=2026-03-01/"),
        _dir("dbfs:/root/business_date=2026-01-15/"),
        _dir("dbfs:/root/business_date=2026-01-15/customer_master/"),  # duplicate date
        _dir("dbfs:/root/_delta_log/"),  # doesn't match the pattern -> ignored
    ]
    monkeypatch.setattr(builtins.dbutils.fs, "ls", lambda root: listing)

    assert source_to_bronze_ingestion.get_available_business_dates() == [20260115, 20260301]


def test_business_dates_result_is_cached(monkeypatch):
    calls = {"n": 0}

    def fake_ls(root):
        calls["n"] += 1
        return [_dir("dbfs:/root/business_date=2026-01-15/")]

    monkeypatch.setattr(builtins.dbutils.fs, "ls", fake_ls)

    source_to_bronze_ingestion.get_available_business_dates()
    source_to_bronze_ingestion.get_available_business_dates()

    assert calls["n"] == 1


def test_business_dates_listing_failure_raises_a_helpful_error(monkeypatch):
    monkeypatch.setattr(
        builtins.dbutils.fs, "ls", lambda root: (_ for _ in ()).throw(Exception("boom"))
    )

    with pytest.raises(RuntimeError, match="Unable to list Bronze source snapshots"):
        source_to_bronze_ingestion.get_available_business_dates()


@pytest.fixture
def watermark_source(fake_dp, test_spark, tmp_path_factory, monkeypatch, request):
    domain, table = "wm_domain", f"wm_table_{request.node.name}"
    
    # Store temporary test snapshots under project root workspace folder
    root = WORKSPACE_TMP_DIR / tmp_path_factory.mktemp("watermark_root").name
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(source_to_bronze_ingestion, "SOURCE_ROOT", str(root))

    def write_snapshot(business_date: str, row_id: int):
        target_dir = root / f"business_date={business_date}" / domain / table
        target_path = str(target_dir)
        test_spark.createDataFrame([Row(id=row_id)]).write.mode("overwrite").parquet(
            target_path
        )

    source_to_bronze_ingestion.build_snapshot_flow(
        table_name=table, domain=domain, keys=["id"], stored_as_scd_type="2", schema_hints=""
    )
    source_fn = fake_dp.create_auto_cdc_from_snapshot_flow.call_args.kwargs["source"]
    return source_fn, write_snapshot


def test_watermark_first_run_returns_the_earliest_snapshot(watermark_source, monkeypatch):
    source_fn, write_snapshot = watermark_source
    write_snapshot("2026-01-01", row_id=20260101)
    write_snapshot("2026-02-01", row_id=20260201)
    monkeypatch.setattr(
        source_to_bronze_ingestion, "get_available_business_dates", lambda: [20260101, 20260201]
    )

    df, version = source_fn(None)  # no watermark yet -> first run

    assert version == 20260101
    assert df.collect()[0]["id"] == 20260101


def test_watermark_only_advances_to_snapshots_newer_than_the_current_one(watermark_source, monkeypatch):
    source_fn, write_snapshot = watermark_source
    write_snapshot("2026-01-01", row_id=20260101)
    write_snapshot("2026-02-01", row_id=20260201)
    monkeypatch.setattr(
        source_to_bronze_ingestion, "get_available_business_dates", lambda: [20260101, 20260201]
    )

    df, version = source_fn(20260101)  # already processed 2026-01-01

    assert version == 20260201
    assert df.collect()[0]["id"] == 20260201


def test_watermark_returns_none_when_fully_caught_up(watermark_source, monkeypatch):
    """Edge case: nothing new to ingest since the last run."""
    source_fn, write_snapshot = watermark_source
    write_snapshot("2026-01-01", row_id=20260101)
    monkeypatch.setattr(source_to_bronze_ingestion, "get_available_business_dates", lambda: [20260101])

    assert source_fn(20260101) is None


# Direct execution entrypoint
if __name__ == "__main__":
    pytest.main(["-v", "-s", __file__])