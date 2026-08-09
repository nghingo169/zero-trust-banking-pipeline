# Databricks notebook source
"""
Automated Pipeline Unit & Integration Test Runner.

Executes PyTest without requiring __init__.py files in project structure.
This script runs on Databricks Job Cluster / Single-Node Cluster.
"""

import os
import subprocess
import sys

# Tự động cài đặt pytest nếu môi trường chưa có sẵn
try:
    import pytest
except ImportError:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "pytest==8.3.5", "-q"]
    )
    import pytest

import builtins
import logging
from pathlib import Path
from types import ModuleType
from typing import List, Tuple

# Prevent Python from creating __pycache__ directories in Workspace filesystem
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
sys.dont_write_bytecode = True

import pyspark

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("UnitTestRunner")


class TestRunner:
    """Encapsulates test environment setup and PyTest execution."""

    def __init__(self) -> None:
        self.tests_dir, self.src_dir, self.project_root = self._resolve_paths()
        self._inject_spark_and_mocks()
        self._configure_python_path()

    def _resolve_paths(self) -> Tuple[Path, Path, Path]:
        base_path = (
            Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
        )
        tests_dir = base_path if base_path.name == "tests" else base_path / "tests"
        project_root = tests_dir.parent.resolve()
        src_dir = project_root / "src"
        return tests_dir, src_dir, project_root

    def _inject_spark_and_mocks(self) -> None:
        """Injects Spark context, safely patches spark.conf.get, and mocks ONLY DLT runtime modules."""
        # 1. Inject Spark
        try:
            if "spark" in globals():
                builtins.spark = globals()["spark"]
            elif "spark" in builtins.__dict__:
                pass

            if hasattr(builtins, "spark") and hasattr(builtins.spark, "conf"):
                _orig_conf_get = builtins.spark.conf.get

                def _safe_conf_get(key: str, default=None):
                    try:
                        val = _orig_conf_get(key, default)
                        return val if val is not None else default
                    except Exception:
                        defaults_map = {
                            "pipeline.catalog": "workspace",
                            "pipeline.bronze_schema": "bronze",
                            "pipeline.silver_validated": "silver_validated",
                            "pipeline.silver_validated_schema": "silver_validated",
                            "pipeline.silver_schema": "silver",
                            "pipeline.gold_schema": "gold",
                            "pipeline.quality_rules_path": ".",
                            "spark.sql.stackTracesInDataFrameContext": "1",
                        }
                        res = defaults_map.get(key, default)
                        return (
                            "1"
                            if key == "spark.sql.stackTracesInDataFrameContext"
                            and res is None
                            else res
                        )

                builtins.spark.conf.get = _safe_conf_get

            logger.info(
                "Databricks SparkSession injected and config getter safely patched."
            )
        except Exception as e:
            logger.warning(f"Could not inject Spark session: {e}")

        # 2. Mock pyspark.pipelines và dlt
        if not hasattr(pyspark, "pipelines"):
            pipelines_mock = ModuleType("pyspark.pipelines")
            pipelines_mock.table = lambda *args, **kwargs: (lambda func: func)
            pipelines_mock.temporary_view = lambda *args, **kwargs: (lambda func: func)
            pyspark.pipelines = pipelines_mock
            sys.modules["pyspark.pipelines"] = pipelines_mock

        # Bổ sung vào _inject_spark_and_mocks() trong tests/run_unit_tests.py
        if "dlt" not in sys.modules:
            dlt_mock = ModuleType("dlt")
            noop_decorator = lambda *args, **kwargs: (lambda func: func)

            dlt_mock.table = noop_decorator
            dlt_mock.temporary_view = noop_decorator

            # Mock DLT Expectations & Data Quality
            dlt_mock.expect = noop_decorator
            dlt_mock.expect_or_drop = noop_decorator
            dlt_mock.expect_or_fail = noop_decorator
            dlt_mock.expect_all = noop_decorator
            dlt_mock.expect_all_or_drop = noop_decorator
            dlt_mock.expect_all_or_fail = noop_decorator

            # Mock DLT Readers
            dlt_mock.read = lambda *args, **kwargs: None
            dlt_mock.read_stream = lambda *args, **kwargs: None

            sys.modules["dlt"] = dlt_mock

    def _configure_python_path(self) -> None:
        """Injects source subdirectories directly into sys.path."""
        paths_to_inject = [
            self.project_root,
            self.src_dir,
            self.src_dir / "data_contracts",
            self.src_dir / "pipeline",
            self.src_dir / "pipeline" / "silver",
            self.src_dir / "pipeline" / "bronze",
            self.src_dir / "pipeline" / "gold",
            self.tests_dir,
            self.tests_dir / "silver",
        ]

        for path in paths_to_inject:
            path_str = str(path)
            if path_str not in sys.path and path.exists():
                sys.path.insert(0, path_str)

    def discover_target_paths(self) -> List[Path]:
        subfolders = [
            item
            for item in self.tests_dir.iterdir()
            if item.is_dir() and not item.name.startswith((".", "__"))
        ]
        return subfolders if subfolders else [self.tests_dir]

    def run(self) -> None:
        targets = self.discover_target_paths()

        logger.info("=" * 65)
        logger.info("AUTOMATED PIPELINE UNIT & INTEGRATION TEST SUITE")
        logger.info(f"Project Root : {self.project_root}")
        logger.info(f"Source directory   : {self.src_dir}")
        logger.info(f"Target layer : {[t.name for t in targets]}")
        logger.info("=" * 65)

        pytest_args = [
            *[str(t) for t in targets],
            "-v",
            "--tb=short",
            "--import-mode=importlib",
            "-o",
            "python_files=test_*.py *_test.py",
            "-p",
            "no:cacheprovider",
        ]

        exit_code = pytest.main(pytest_args)

        if exit_code != 0:
            logger.error(f"Test suite execution failed with exit code: {exit_code}")
            raise RuntimeError(f"PyTest suite failed with exit code {exit_code}.")

        logger.info("All Pipeline Unit Tests Passed Successfully!")


def main() -> None:
    runner = TestRunner()
    runner.run()


if __name__ == "__main__":
    main()
