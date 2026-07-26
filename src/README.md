# Pipeline source code

## Authoritative data definitions

[`data_contracts/`](data_contracts/) is the single source of truth for YAML
schemas, table identity, business keys, ingestion semantics, and row-quality
rules. Pipeline code and replay helpers live under [`pipeline/`](pipeline/) and
[`replay_validation/`](replay_validation/) respectively.

The previous standalone quarantine prototype is preserved at
[`legacy/pipeline_definitions/card_quarantine.py`](legacy/pipeline_definitions/card_quarantine.py).
It is not part of the deployed pipeline path.

## Quarantine pattern

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

Some data-quality actions are intentionally outside the row-level quarantine
predicate: duplicate detection, status reconciliation, arrival-order handling,
and stale-reference flagging need aggregate or cross-record logic. Primary-key
uniqueness and invalid-rate thresholds likewise belong in separate monitoring
tables.
