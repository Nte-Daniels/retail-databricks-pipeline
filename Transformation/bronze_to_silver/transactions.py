import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="transactions",
    comment="Silver transactions — cleaned, type-cast, DQ validated, derived metrics"
)
@dlt.expect_or_drop("non_null_transaction_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("non_null_product_id",     "product_id IS NOT NULL")
@dlt.expect("valid_quantity",                  "quantity > 0")
@dlt.expect("valid_selling_price",             "selling_price >= 0")
@dlt.expect("valid_discount_amount",           "discount_amount >= 0")
@dlt.expect("valid_payment_mode",
    "payment_mode IN ('UPI','Card','Cash','Net Banking')")
def transactions():
    """
    Silver layer transactions.

    Transformations:
    - Cast quantity to IntegerType
    - Cast selling_price and discount_amount to decimal(10,2)
    - Parse transaction_timestamp from non-standard format
    - Derive transaction_date for calendar join
    - Derive gross_amount and net_sales_amount
    - Drop _rescued_data and Bronze audit columns

    DQ Rules:
    - DROP row  : NULL transaction_id, NULL product_id
    - WARN only : negative quantity, negative price,
                  negative discount, unknown payment mode
    """
    return (
        spark.read.table("retail_q.blob_bronze.transaction")
        .select(
            # Keys
            F.col("transaction_id"),
            F.col("opportunity_name"),
            F.col("product_id"),
            F.col("store_id"),

            # Numeric fields — correct types
            F.col("quantity").cast("int").alias("quantity"),
            F.col("selling_price").cast("decimal(10,2)").alias("selling_price"),
            F.col("discount_amount").cast("decimal(10,2)").alias("discount_amount"),

            # Derived financial metrics
            (F.col("quantity").cast("int") *
             F.col("selling_price").cast("decimal(10,2)"))
            .cast("decimal(12,2)")
            .alias("gross_amount"),

            (
                (F.col("quantity").cast("int") *
                 F.col("selling_price").cast("decimal(10,2)")) -
                F.col("discount_amount").cast("decimal(10,2)")
            )
            .cast("decimal(12,2)")
            .alias("net_sales_amount"),

            # Timestamp — parse non-standard format
            F.to_timestamp(
                F.col("transaction_timestamp"),
                "dd-MMM-yyyy hh.mm.ss a"
            ).alias("transaction_timestamp"),

            # Date key — required for dim_calendar join in Gold
            F.to_date(
                F.to_timestamp(
                    F.col("transaction_timestamp"),
                    "dd-MMM-yyyy hh.mm.ss a"
                )
            ).alias("transaction_date"),

            # Categorical
            F.col("payment_mode"),
            F.col("sales_channel"),

            # Audit
            F.current_timestamp().alias("processed_at")
        )
    )