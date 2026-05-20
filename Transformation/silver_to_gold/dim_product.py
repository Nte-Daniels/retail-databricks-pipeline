import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="dim_product",
    comment="Gold dimension — current active products from PostgreSQL"
)
def dim_product():
    """
    Gold dim_product.
    Grain: one row per active product.
    Filters to current SCD2 records only (is_active = true).
    Source: retail_silver.product_catalog
    """
    return (
        spark.read.table("retail_q.retail_silver.product_catalog")
        .filter(F.col("is_active") == True)
        .select(
            F.col("product_id"),
            F.col("product_name"),
            F.col("category"),
            F.col("subcategory"),
            F.col("brand"),
            F.col("product_segment"),
            F.col("unit_price"),
            F.col("supplier_name"),
            F.col("launch_date"),
            F.col("updated_at"),
            F.current_timestamp().alias("gold_processed_at")
        )
    )