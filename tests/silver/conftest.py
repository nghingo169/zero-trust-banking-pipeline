# Databricks notebook source
import os
import sys
from types import ModuleType
from pathlib import Path
from unittest.mock import MagicMock
import pytest

# ------------------------------------------------------------------------------
# 1. SETUP SYS.PATH FOR LOCAL MODULES
# ------------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
SILVER_DIR = SRC_DIR / "pipeline" / "silver"

for p in [str(SRC_DIR), str(SILVER_DIR)]:
    if os.path.exists(p) and p not in sys.path:
        sys.path.insert(0, p)

# ------------------------------------------------------------------------------
# 2. MOCK DATA CONTRACTS DOMAINS STRUCTURE (CHO VỪA KHÍCH CODE SẢN XUẤT)
# ------------------------------------------------------------------------------
# Mock DOMAINS dạng Dict tương thích với cả list lẫn dict unpack
mock_domains = {
    "card": {"scd2": ["card_master"], "append": ["card_transaction"]},
    "customer": {"scd2": ["core_banking_customer"], "append": ["customer_event"]},
    "fincrime": {"scd2": ["sanction_watchlist"], "append": ["aml_alert"]},
    "financial_crime": {"scd2": ["sanction_watchlist"], "append": ["aml_alert"]},
    "transaction": {"scd2": ["account_transaction"], "append": ["payment_event"]},
    "payments": {"scd2": ["account_transaction"], "append": ["payment_event"]}
}

if "data_contracts" not in sys.modules:
    data_contracts_mock = ModuleType("data_contracts")
    sys.modules["data_contracts"] = data_contracts_mock

if "data_contracts.table_catalog" not in sys.modules:
    table_catalog_mock = ModuleType("data_contracts.table_catalog")
    table_catalog_mock.DOMAINS = mock_domains
    table_catalog_mock.tables = lambda domain: {
        "core_banking_customer": "customer_id",
        "card_master": "card_id",
        "sanction_watchlist": "watchlist_id",
        "account_transaction": "account_txn_id"
    }
    sys.modules["data_contracts.table_catalog"] = table_catalog_mock

if "data_contracts.normalization" not in sys.modules:
    norm_mock = ModuleType("data_contracts.normalization")
    norm_mock.normalize = lambda df: df
    sys.modules["data_contracts.normalization"] = norm_mock

if "data_contracts.quality_rules.registry" not in sys.modules:
    reg_mock = ModuleType("data_contracts.quality_rules.registry")
    reg_mock.get_rules = lambda table_name: {}
    reg_mock.RULES_BY_TABLE = {}
    sys.modules["data_contracts.quality_rules.registry"] = reg_mock

# ------------------------------------------------------------------------------
# 3. MOCK DLT, PYSPARK & SPARK SESSION
# ------------------------------------------------------------------------------
if "dlt" not in sys.modules:
    dlt_mock = ModuleType("dlt")
    dlt_mock.table = lambda *args, **kwargs: (lambda func: func)
    dlt_mock.view = lambda *args, **kwargs: (lambda func: func)
    sys.modules["dlt"] = dlt_mock

if "pyspark.pipelines" not in sys.modules:
    pipelines_mock = ModuleType("pyspark.pipelines")
    pipelines_mock.table = lambda *args, **kwargs: (lambda func: func)
    pipelines_mock.view = lambda *args, **kwargs: (lambda func: func)
    sys.modules["pyspark.pipelines"] = pipelines_mock

from unittest.mock import MagicMock
mock_spark = MagicMock()
mock_spark.conf.get.side_effect = lambda key, default=None: default or "workspace"

import builtins
builtins.spark = mock_spark

@pytest.fixture(scope="session", autouse=True)
def test_spark():
    return mock_spark