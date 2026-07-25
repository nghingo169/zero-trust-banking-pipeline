# Pipeline source code

## Domain data quality: the Databricks quarantine pattern

Rules are split into [`card_quality_rules.py`](card_quality_rules.py),
[`customer_quality_rules.py`](customer_quality_rules.py),
[`transaction_quality_rules.py`](transaction_quality_rules.py), and
[`fincrime_quality_rules.py`](fincrime_quality_rules.py). The lightweight
[`quality_rules.py`](quality_rules.py) facade routes a table to its domain, so
existing pipeline imports remain valid. None of these modules parse
data-contract YAML at runtime.

[`pipelines/card_quarantine.py`](pipelines/card_quarantine.py) applies those
rules exactly as in the Databricks quarantine example:

```python
rules = get_rules("card")
quarantine_rules = get_quarantine_condition("card")

@dp.table(temporary=True, partition_cols=["is_quarantined"])
@dp.expect_all(rules)
def card_data_quarantine():
    return (
        spark.readStream.table("raw_card_data")
        .withColumn("is_quarantined", expr(quarantine_rules))
    )
```

`silver_card` reads rows where `is_quarantined=false`. `card_quarantine` keeps
the invalid rows where `is_quarantined=true` for investigation and replay.

Some catalog actions are intentionally outside the row-level quarantine
predicate: duplicate detection, status reconciliation, arrival-order handling,
and stale-reference flagging need aggregate or cross-record logic. Primary-key
uniqueness and invalid-rate thresholds likewise belong in separate monitoring
tables.
