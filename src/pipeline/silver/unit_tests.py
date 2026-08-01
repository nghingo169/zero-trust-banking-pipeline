# Databricks notebook source
"""Unit Test Orchestrator Runner in src/

Executes all 10 unit test suites located in the tests/ directory.
"""

import os
import sys
import pytest

# 1. TÍNH TOÁN ĐƯỜNG DẪN TƯƠNG ĐỐI
# File runner nằm tại: src/pipeline/testing/run_unit_tests.py (sâu 3 cấp từ root)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))

# Đường dẫn trỏ tới thư mục chứa 10 file test_*.py
tests_dir = os.path.join(project_root, "tests")

# Thêm project_root và src/ vào sys.path để các file test import được code nghiệp vụ
src_dir = os.path.join(project_root, "src")
for path in [project_root, src_dir]:
    if path not in sys.path:
        sys.path.insert(0, path)

print(f"Project Root Path : {project_root}")
print(f"Target Test Directory: {tests_dir}")

# 2. KÍCH HOẠT PYTEST QUÉT QUA CẢ 10 FILE TEST
retcode = pytest.main([
    tests_dir,
    "-v",
    "--tb=short"
])

# 3. TRẢ VỀ KẾT QUẢ CHO DATABRICKS JOB
if retcode != 0:
    raise RuntimeError(f"PyTest suite failed with exit code: {retcode}")
else:
    print("All Unit Test files passed successfully!")