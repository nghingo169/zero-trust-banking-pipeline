"""Field-specific canonicalization applied before Silver quality validation."""

from pyspark.sql import DataFrame, functions as F

UPPERCASE_IDENTIFIER_COLUMNS = {
    "national_id", "cust_no", "party_id", "customer_ref", "cif_number",
    "id_number", "source_system",
}


def normalize(df: DataFrame) -> DataFrame:
    """Normalize identifiers and contact fields without changing free text."""
    result = df
    string_columns = {
        field.name for field in df.schema.fields if field.dataType.simpleString() == "string"
    }
    for column_name in UPPERCASE_IDENTIFIER_COLUMNS & string_columns:
        result = result.withColumn(column_name, F.upper(F.trim(F.col(column_name))))
    if "email" in string_columns:
        result = result.withColumn("email", F.lower(F.trim(F.col("email"))))
    if "phone" in string_columns:
        result = result.withColumn("phone", F.regexp_replace(F.trim(F.col("phone")), r"[^0-9]", ""))
    return result
