# Databricks notebook source
"""Unit Test Orchestrator Runner.

Location: src/pipeline/silver/unit_tests.py
Executes all 10 unit test suites located in the tests/ directory.
"""
from __future__ import annotations

import sys
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F


RULE_PATH = spark.conf.get("pipeline.quality_rules_path")
if RULE_PATH not in sys.path:
    sys.path.insert(0, RULE_PATH)

from data_contracts.table_catalog import DOMAINS, tables
from pipeline.

# 1. TẠO / LẤY SPARK SESSION & ĐỌC PARAMETERS TỪ WIDGETS
spark = SparkSession.builder.getOrCreate()

try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    
    # Đọc tham số từ Databricks Job Widgets (fallback về giá trị mặc định nếu thiếu)
    catalog = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.help()] else "workspace"
    silver_schema = dbutils.widgets.get("silver_schema") if "silver_schema" in [w.name for w in dbutils.widgets.help()] else "silver"
    bronze_schema = dbutils.widgets.get("bronze_schema") if "bronze_schema" in [w.name for w in dbutils.widgets.help()] else "bronze"
    
    # Nạp trực tiếp vào Spark Config (KHÔNG DÙNG OS.ENVIRON)
    spark.conf.set("pipeline.catalog", catalog)
    spark.conf.set("pipeline.validated_schema", silver_schema)
    spark.conf.set("pipeline.bronze_schema", bronze_schema)
    print(f"📌 Spark Config Loaded -> Catalog: {catalog} | Silver: {silver_schema} | Bronze: {bronze_schema}")
except Exception as e:
    print(f"⚠️ Running in local or widgets not available: {e}")

# 2. XÁC ĐỊNH ĐƯỜNG DẪN TỚI THƯ MỤC TESTS
# File nằm ở src/pipeline/silver/unit_tests.py (sâu 3 cấp từ root)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
tests_dir = os.path.join(project_root, "tests")

src_dir = os.path.join(project_root, "src")
for path in [project_root, src_dir]:
    if path not in sys.path:
        sys.path.insert(0, path)

print(f"🚀 Project Root Path : {project_root}")
print(f"🧪 Target Test Directory: {tests_dir}")

# 3. CHẠY PYTEST (VỚI CỜ TẮT CACHE VÀ TB SHORT)
retcode = pytest.main([
    tests_dir,
    "-v",
    "--tb=short",
    "-p", "no:cacheprovider"   # Tắt cacheprovider để không sinh file .pytest_cache
])

# 4. BÁO LỖI NẾU CÓ TEST FAIL
if retcode != 0:
    raise RuntimeError(f"❌ PyTest suite failed with exit code: {retcode}")
else:
    print("✅ All 10 Unit Test files passed successfully!")