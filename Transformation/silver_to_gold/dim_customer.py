import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="dim_customer",
    comment="Gold dimension — current active customers from Salesforce"
)
def dim_customer():
    """
    Gold dim_customer.
    Grain: one row per active customer.
    Filters to current SCD2 records only (is_active = true).
    Source: retail_silver.account
    """
    return (
        spark.read.table("retail_q.retail_silver.account")
        .filter(
            (F.col("is_active") == True) &
            (F.col("is_deleted") == False)
        )
        .select(
            F.col("id").alias("customer_id"),
            F.col("customer_name"),
            F.col("customer_type"),
            F.col("billing_city").alias("city"),
            F.col("billing_state").alias("state"),
            F.col("billing_country").alias("country"),
            F.col("billing_state_code"),
            F.col("billing_country_code"),
            F.col("industry"),
            F.col("annual_revenue"),
            F.col("number_of_employees"),
            F.col("description"),
            F.col("phone"),
            F.col("website"),
            F.col("created_date"),
            F.current_timestamp().alias("gold_processed_at")
        )
    )