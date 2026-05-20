import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="fact_sales",
    comment="Gold fact table — retail transactions enriched with customer dimension"
)
def fact_sales():
    """
    Gold layer fact_sales.

    Grain: one row per transaction.

    Key join:
    transactions.opportunity_name → opportunity.name → opportunity.account_id
    This two-hop resolves customer_id from Salesforce CRM into the fact table.

    Filters:
    - Left join preserves all transactions even without CRM match
    """
    transactions_df = spark.read.table("retail_q.retail_silver.transactions")
    opportunity_df  = spark.read.table("retail_q.retail_silver.opportunity")

    return (
        transactions_df.alias("t")
        .join(
            opportunity_df.alias("o"),
            F.upper(F.trim(F.col("t.opportunity_name"))) ==
            F.upper(F.trim(F.col("o.name"))),
            how="left"
        )
        .select(
            # Transaction keys
            F.col("t.transaction_id"),
            F.col("t.product_id"),
            F.col("t.store_id"),

            # Customer FK — resolved via opportunity join
            F.col("o.account_id").alias("customer_id"),

            # Calendar FK — integer YYYYMMDD for dim_calendar join
            F.date_format(F.col("t.transaction_date"), "yyyyMMdd")
             .cast("int")
             .alias("date_key"),

            # CRM context
            F.col("t.opportunity_name"),

            # Transaction metrics
            F.col("t.quantity"),
            F.col("t.selling_price"),
            F.col("t.discount_amount"),
            F.col("t.gross_amount"),
            F.col("t.net_sales_amount"),

            # Timestamps
            F.col("t.transaction_timestamp"),
            F.col("t.transaction_date"),

            # Categoricals
            F.col("t.payment_mode"),
            F.col("t.sales_channel")
        )
    )