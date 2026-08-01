import pytest
import sys
import os

# --- 1. ĐỌC BASE_PARAMETERS TỪ DATABRICKS WIDGETS ---
try:
    # Lấy dbutils từ môi trường Databricks
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    dbutils = spark.conf.get("spark.databricks.workspaceUrl", None) # check runtime
    import dbutils as dbutils_lib # hoặc dùng dbutils mặc định của notebook
    
    # Hứng các tham số truyền từ YAML
    catalog = dbutils.widgets.get("catalog")
    business_date = dbutils.widgets.get("business_date")
    silver_schema = dbutils.widgets.get("silver_schema")
    
    # Đưa vào os.environ để các file test_*.py có thể dùng os.getenv(...)
    os.environ["TEST_CATALOG"] = catalog
    os.environ["TEST_BUSINESS_DATE"] = business_date
    os.environ["TEST_SILVER_SCHEMA"] = silver_schema
    print(f"Loaded Parameters -> Catalog: {catalog}, Date: {business_date}")
except Exception as e:
    print(f"Running in local or widget not found: {e}")

# --- 2. CẤU HÌNH THƯ MỤC CHẠY PYTEST ---
test_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(test_dir, "../../"))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Running PyTest suite in: {test_dir}")

# --- 3. KÍCH HOẠT PYTEST ---
retcode = pytest.main([
    test_dir,
    "-v",
    "--tb=short"
])

if retcode != 0:
    raise RuntimeError(f"PyTest failed with exit code: {retcode}")
else:
    print("All Unit Tests passed successfully!")