-- Replace <catalog>, <quarantine_schema>, and the UNION branches for the
-- domain being reported. One failed row is counted once for every failed rule.
WITH quarantine_rules AS (
  SELECT source_table AS table_name, validation_business_date AS business_date,
         EXPLODE(SPLIT(failed_rules, ',')) AS failed_rule
  FROM <catalog>.<quarantine_schema>.<table_name>
)
SELECT business_date, table_name, failed_rule, COUNT(*) AS quarantined_rows
FROM quarantine_rules
GROUP BY business_date, table_name, failed_rule
ORDER BY business_date, table_name, failed_rule;

-- Centralized daily replay metrics: one row per audited table and run.
SELECT
  m.business_date,
  m.domain,
  m.table_name,
  m.landing_rows,
  m.bronze_change_rows,
  m.clean_current_rows,
  m.quarantined_rows,
  m.recorded_at
FROM <catalog>.dev_quality_audit.table_quality_metrics AS m
ORDER BY m.business_date, m.domain, m.table_name, m.recorded_at;

-- Centralized rule/check outcomes.  This is separate from the quarantine
-- report above, which remains the source for record-level failed_rules.
SELECT
  business_date,
  domain,
  target_table_name,
  rule_name,
  records_checked,
  records_failed,
  evaluated_at
FROM <catalog>.dev_quality_audit.data_quality_audit_log
ORDER BY business_date, domain, target_table_name, rule_name, evaluated_at;
