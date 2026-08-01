import os
import sys
import pytest

# 1. Định vị đường dẫn Workspace
try:
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    CURRENT_DIR = os.getcwd()

PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
SILVER_TESTS_DIR = os.path.join(PROJECT_ROOT, "tests", "silver")
SILVER_SRC_DIR = os.path.join(PROJECT_ROOT, "src", "pipeline", "silver")

# 2. Thêm thư mục nguồn vào sys.path
if SILVER_SRC_DIR not in sys.path:
    sys.path.insert(0, SILVER_SRC_DIR)
if SILVER_TESTS_DIR not in sys.path:
    sys.path.insert(0, SILVER_TESTS_DIR)

# 3. Chạy PyTest ngắt bỏ cơ chế cache và auto-import notebook của Databricks
if __name__ == "__main__":
    exit_code = pytest.main([
        SILVER_TESTS_DIR,
        "-v",
        "-p", "no:cacheprovider",
        "--import-mode=importlib"  # Ép PyTest dùng importlib chuẩn thay vì workspace machinery của Databricks
    ])
    
    if exit_code != 0:
        raise RuntimeError(f"PyTest failed with exit code: {exit_code}")