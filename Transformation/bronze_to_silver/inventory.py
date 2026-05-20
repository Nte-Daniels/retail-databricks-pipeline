import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="inventory",
    comment="Silver inventory — cleaned, validated, stock status derived"
)
@dlt.expect_or_drop("non_null_inventory_id", "inventory_id IS NOT NULL")
@dlt.expect_or_drop("non_null_product_id",   "product_id IS NOT NULL")
@dlt.expect_or_drop("non_null_store_id",     "store_id IS NOT NULL")
@dlt.expect("valid_stock_quantity",          "stock_quantity >= 0")
@dlt.expect("valid_reorder_level",           "reorder_level > 0")
def inventory():
    """
    Silver layer inventory.

    Transformations:
    - Trim warehouse_location
    - Derive three-state inventory_status
    - Add processed_at audit column

    DQ Rules:
    - DROP row  : NULL inventory_id, product_id, store_id
    - WARN only : negative stock, zero reorder level
    """
    return (
        spark.read.table("retail_q.postgres_bronze.inventory")
        .select(
            F.col("inventory_id"),
            F.col("product_id"),
            F.col("store_id"),
            F.col("stock_quantity"),
            F.col("reorder_level"),

            # Three-state inventory status
            F.when(F.col("stock_quantity") == 0,
                   F.lit("OUT_OF_STOCK"))
             .when(F.col("stock_quantity") < F.col("reorder_level"),
                   F.lit("LOW_STOCK"))
             .otherwise(F.lit("HEALTHY"))
             .alias("inventory_status"),

            # Standardise warehouse location
            F.when(F.trim(F.col("warehouse_location")) == "", None)
             .otherwise(F.trim(F.col("warehouse_location")))
             .alias("warehouse_location"),

            F.col("last_stock_update"),

            # Audit
            F.current_timestamp().alias("processed_at")
        )
    )