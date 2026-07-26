"""Tests for the direct Databricks-style Card rule module."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_contracts.quality_rules.registry import (
    get_domain_rules,
    get_quarantine_condition,
    get_rules,
    get_rules_as_list_of_dict,
)


class QualityRulesTests(unittest.TestCase):
    def test_card_rules_are_lakeflow_expectation_dictionary(self) -> None:
        self.assertEqual(
            get_rules("card")["card__card_id__not_null"],
            "(card_id IS NOT NULL)",
        )
        self.assertEqual(
            get_rules("card")["card__card_number__not_invalid_pan"],
            "(card_number <> 'INVALID-PAN')",
        )

    def test_catalog_sentinel_rules_match_card_error_injection(self) -> None:
        expected_rules = {
            "card": {
                "card__account_id__resolved": "(account_id <> -1)",
                "card__card_number__not_invalid_pan": "(card_number <> 'INVALID-PAN')",
            },
            "card_transaction": {
                "card_transaction__card_id__resolved": "(card_id <> 'UNKNOWN-CARD')",
            },
            "card_fraud_flag": {
                "card_fraud_flag__card_txn_id__resolved": "(card_txn_id <> -1)",
            },
            "card_limit_history": {
                "card_limit_history__card_id__resolved": "(card_id <> 'UNKNOWN-CARD')",
            },
            "card_transaction_status_event": {
                "card_transaction_status_event__card_txn_id__resolved": "(card_txn_id <> -1)",
            },
        }

        for table, expected in expected_rules.items():
            rules = get_rules(table)
            for name, constraint in expected.items():
                self.assertEqual(rules[name], constraint)

    def test_all_required_card_columns_have_not_null_rules(self) -> None:
        expected_not_null_counts = {
            "card": 7,
            "card_transaction": 7,
            "card_fraud_flag": 5,
            "card_limit_history": 4,
            "card_transaction_status_event": 6,
        }

        for table, expected_count in expected_not_null_counts.items():
            rules = get_rules(table)
            self.assertEqual(
                len([name for name in rules if name.endswith("__not_null")]),
                expected_count,
            )

    def test_quarantine_condition_matches_databricks_pattern(self) -> None:
        rules = get_rules("card")
        self.assertEqual(
            get_quarantine_condition("card"),
            "NOT({0})".format(" AND ".join(rules.values())),
        )

    def test_unknown_table_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "No data-quality rules"):
            get_rules("unknown")

    def test_card_rules_are_isolated_in_the_card_domain_module(self) -> None:
        rules = get_rules_as_list_of_dict()
        self.assertEqual(len(get_domain_rules("card")), 36)
        self.assertGreater(len(rules), 36)
        self.assertTrue(all(set(rule) == {"name", "constraint", "table"} for rule in rules))

    def test_customer_transaction_and_fincrime_injections_have_quarantine_rules(self) -> None:
        expected_rules = {
            "core_banking_customer": {
                "core_banking_customer__phone__not_placeholder": "(phone <> '0000000000')",
            },
            "account_transaction": {
                "account_transaction__account_id__resolved": "(account_id <> -1)",
                "account_transaction__credit_amount__non_negative": "(NOT (direction = 'CREDIT' AND amount < 0))",
            },
            "fraud_alert": {
                "fraud_alert__alert_score__in_range": "(alert_score >= 0 AND alert_score <= 100)",
            },
            "investigation_case_sanction_screening": {
                "investigation_case_sanction_screening__screening_id__resolved": "(screening_id <> 'UNKNOWN-SCREENING')",
            },
        }
        for table, expected in expected_rules.items():
            for name, constraint in expected.items():
                self.assertEqual(get_rules(table)[name], constraint)

    def test_every_catalog_quarantine_table_is_covered_by_its_domain_registry(self) -> None:
        # These are the tables whose injected defects are explicitly marked
        # QUARANTINE_FIELD or QUARANTINE_RECORD in the error catalog.
        expected_tables = {
            "customer": {
                "core_banking_customer", "crm_customer", "customer_kyc",
                "customer_employment", "customer_request", "account",
                "customer_account",
            },
            "transaction": {
                "account_transaction", "account_transaction_status_event",
                "merchant_store", "log_atm", "atm_transaction_status_event",
                "payment_gateway_log", "payment_gateway_status_event",
                "balance_snapshot",
            },
            "fincrime": {
                "account_transaction_risk_score", "fraud_alert",
                "transaction_monitoring_alert",
                "transaction_monitoring_alert_account_transaction",
                "transaction_monitoring_alert_card_transaction",
                "investigation_case_transaction_monitoring_alert", "aml_case",
                "sanction_screening", "suspicious_activity_report", "chargeback",
                "investigation_case_account_transaction",
                "investigation_case_card_transaction", "investigation_case_fraud_alert",
                "investigation_case_sanction_screening",
            },
        }
        for domain, tables in expected_tables.items():
            actual_tables = {rule["table"] for rule in get_domain_rules(domain)}
            self.assertTrue(tables <= actual_tables)

    def test_non_quarantine_catalog_handling_is_not_a_row_quarantine_rule(self) -> None:
        # Status reconciliation, replay deduplication, and stale-reference
        # flagging require history or monitoring, not a single-row predicate.
        self.assertNotIn("transaction_monitoring_alert__alert_status", get_rules("transaction_monitoring_alert"))
        self.assertNotIn("watchlist", [rule["table"] for rule in get_domain_rules("fincrime")])

    def test_silver_validation_imports_the_shared_rule_module(self) -> None:
        pipeline_source = (
            Path(__file__).resolve().parents[1]
            / "src/pipeline/silver/card_validation.py"
        ).read_text()
        self.assertIn('spark.conf.get("pipeline.quality_rules_path")', pipeline_source)
        self.assertIn("from data_contracts.quality_rules.registry import get_rules", pipeline_source)
        self.assertNotIn("RULES_BY_TABLE =", pipeline_source)

    def test_silver_validation_quarantines_null_rule_evaluations(self) -> None:
        pipeline_source = (
            Path(__file__).resolve().parents[1]
            / "src/pipeline/silver/card_validation.py"
        ).read_text()
        self.assertIn("~F.coalesce(F.expr(constraint), F.lit(False))", pipeline_source)

    def test_transaction_ingestion_separates_snapshot_scd2_and_event_history(self) -> None:
        pipeline_source = (
            Path(__file__).resolve().parents[1]
            / "src/pipeline/bronze/transaction_ingestion.py"
        ).read_text()
        self.assertIn("EVENT_TABLES", pipeline_source)
        self.assertIn("SNAPSHOT_TABLES", pipeline_source)
        self.assertIn("@dp.append_flow", pipeline_source)
        self.assertIn(".dropDuplicates([k])", pipeline_source)
        self.assertNotIn('F.col("business_date"), F.col(key)', pipeline_source)

    def test_card_ingestion_classifies_scd2_and_immutable_history(self) -> None:
        source = (Path(__file__).resolve().parents[1] / "src/pipeline/bronze/card_ingestion.py").read_text()
        self.assertIn("SCD2_SNAPSHOT_TABLES", source)
        self.assertIn('DOMAINS["card"]["scd2"]', source)
        self.assertIn("APPEND_ONLY_TABLES", source)
        self.assertIn('DOMAINS["card"]["append"]', source)
        self.assertIn("@dp.append_flow", source)

    def test_customer_and_fincrime_are_all_scd2_snapshots(self) -> None:
        customer = (Path(__file__).resolve().parents[1] / "src/pipeline/bronze/customer_ingestion.py").read_text()
        fincrime = (Path(__file__).resolve().parents[1] / "src/pipeline/bronze/fincrime_ingestion.py").read_text()
        self.assertNotIn('"stream": True', customer)
        self.assertIn("stored_as_scd_type=\"2\"", customer)
        self.assertIn("SCD2_SNAPSHOT_TABLES", fincrime)
        self.assertNotIn("create_auto_cdc_flow", fincrime)

    def test_quality_pipelines_use_cdf_for_quarantine_history(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/pipeline/silver"
        for filename in (
            "transaction_validation.py",
            "customer_validation.py",
            "fincrime_validation.py",
        ):
            self.assertIn('option("readChangeFeed", "true")', (root / filename).read_text())

    def test_silver_validation_retains_scd2_history(self) -> None:
        root = Path(__file__).resolve().parents[1] / "src/pipeline/silver"
        self.assertIn("with_validation_metadata(all_versions(name), name)", (root / "customer_validation.py").read_text())
        self.assertIn("with_validation_metadata(all_rows(n), n, rules)", (root / "transaction_validation.py").read_text())
        self.assertIn("with_validation_metadata(all_versions(n), n, r)", (root / "fincrime_validation.py").read_text())
        self.assertIn("_with_validation_metadata(_silver_rows(table_name), table_name)", (root / "card_validation.py").read_text())


if __name__ == "__main__":
    unittest.main()
