"""Unit tests for six-day Card development replay sequencing invariants."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from replay_validation.card import REPLAY_DATES, expected_staged_dates, validate_staged_prefix


class CardDevelopmentReplayTests(unittest.TestCase):
    def test_all_six_dates_are_whitelisted_in_order(self) -> None:
        self.assertEqual(REPLAY_DATES[-1], "2026-07-10")
        self.assertEqual(len(REPLAY_DATES), 6)

    def test_expected_prefix_before_each_date(self) -> None:
        self.assertEqual(expected_staged_dates("2026-07-05"), ())
        self.assertEqual(expected_staged_dates("2026-07-08"), REPLAY_DATES[:3])
        self.assertEqual(expected_staged_dates("2026-07-10"), REPLAY_DATES[:5])

    def test_rejects_repeated_skipped_and_future_staging(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly"):
            validate_staged_prefix("2026-07-06", ())
        with self.assertRaisesRegex(ValueError, "exactly"):
            validate_staged_prefix("2026-07-07", ("2026-07-05", "2026-07-07"))
        with self.assertRaisesRegex(ValueError, "exactly"):
            validate_staged_prefix("2026-07-06", ("2026-07-05", "2026-07-06"))

    def test_allows_exact_prior_prefix(self) -> None:
        validate_staged_prefix("2026-07-09", REPLAY_DATES[:4])

    def test_rejects_unknown_date(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            expected_staged_dates("2026-07-11")

    def test_bronze_normalization_casts_only_limit_amount(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "src/pipeline/bronze/card_ingestion.py"
        ).read_text()
        self.assertIn('df.withColumn("limit_amount", F.col("limit_amount").cast("decimal(12,2)"))', source)
        self.assertNotIn('select("history_id"', source)

    def test_july_9_history_assertion_uses_snapshot_version_type(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "src/pipeline/monitoring/card_quality_audit.py"
        ).read_text()
        self.assertIn('__START_AT < 20260709', source)
        self.assertNotIn("__START_AT < DATE '2026-07-09'", source)


if __name__ == "__main__":
    unittest.main()
