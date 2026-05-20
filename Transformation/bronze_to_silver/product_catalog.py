import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="product_catalog",
    comment="Silver layer product catalog — standardized, DQ validated, SCD2 preserved",
    cluster_by=["category", "product_segment"]
)
@dlt.expect_or_drop("valid_product_id",
    "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
@dlt.expect_or_drop("valid_product_name",
    "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0")
@dlt.expect_or_drop("valid_unit_price",   "unit_price > 0")
@dlt.expect_or_drop("valid_launch_date",  "launch_date IS NOT NULL")
@dlt.expect("valid_category",             "category IS NOT NULL")
@dlt.expect("valid_supplier",             "supplier_name IS NOT NULL")
def product_catalog():
    """
    Silver layer product catalog.

    Transformations:
    - Trim and title case all string fields
    - NULL for missing optional fields (not Unknown)
    - product_segment derived from price tier
    - is_active re-derived from SCD2 __END_AT
    - SCD2 columns renamed and preserved
    - processed_at audit column added
    """
    return (
        spark.read.table("retail_q.postgres_bronze.product_catalog")
        .select(
            # Keys — uppercase and trimmed
            F.upper(F.trim(F.col("product_id"))).alias("product_id"),

            # Descriptive fields — title case, NULL for empty
            F.when(F.trim(F.col("product_name")) == "", None)
             .otherwise(F.initcap(F.trim(F.col("product_name"))))
             .alias("product_name"),

            F.when(F.trim(F.col("category")) == "", None)
             .otherwise(F.initcap(F.trim(F.col("category"))))
             .alias("category"),

            F.when(F.trim(F.col("subcategory")) == "", None)
             .otherwise(F.initcap(F.trim(F.col("subcategory"))))
             .alias("subcategory"),

            F.when(F.trim(F.col("brand")) == "", None)
             .otherwise(F.initcap(F.trim(F.col("brand"))))
             .alias("brand"),

            # Price — rounded to 2dp
            F.round(F.col("unit_price"), 2).cast("decimal(10,2)").alias("unit_price"),

            F.when(F.trim(F.col("supplier_name")) == "", None)
             .otherwise(F.initcap(F.trim(F.col("supplier_name"))))
             .alias("supplier_name"),

            # Dates
            F.col("launch_date"),
            F.col("updated_at"),

            # Price-based segment — business classification
            F.when(F.col("unit_price") > 50000, F.lit("PREMIUM"))
             .when(F.col("unit_price") > 10000, F.lit("MID_RANGE"))
             .otherwise(F.lit("BUDGET"))
             .alias("product_segment"),

            # SCD2 — renamed, clean convention
            F.col("__START_AT").alias("start_at"),
            F.col("__END_AT").alias("end_at"),

            # is_active re-derived from SCD2 logic
            F.when(F.col("__END_AT").isNull(), F.lit(True))
             .otherwise(F.lit(False))
             .alias("is_active"),

            # Audit
            F.current_timestamp().alias("processed_at")
        )
    )