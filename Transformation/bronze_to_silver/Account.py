import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="account",
    comment="Silver Salesforce accounts — standardised, DQ validated, SCD2 preserved"
)
@dlt.expect_or_drop("non_null_id",            "id IS NOT NULL")
@dlt.expect("non_null_customer_name",         "customer_name IS NOT NULL")
@dlt.expect("non_deleted_account",            "is_deleted = false")
def account():
    """
    Silver layer Salesforce accounts.

    Transformations:
    - PascalCase → snake_case column renaming
    - Trim and title case name fields
    - NULL for missing optional fields
    - State/country codes preserved for geo analytics
    - Customer description preserved for segmentation
    - is_active re-derived from SCD2 __END_AT
    - SCD2 history columns preserved
    - processed_at audit column added
    """
    return (
        spark.read.table("retail_q.salesforce_bronze.account")
        .select(
            # Key
            F.col("Id").alias("id"),

            # Status
            F.col("IsDeleted").alias("is_deleted"),

            # Name — title case, trim
            F.when(F.trim(F.col("Name")) == "", None)
             .otherwise(F.initcap(F.trim(F.col("Name"))))
             .alias("customer_name"),

            # Account metadata
            F.col("Type").alias("customer_type"),
            F.col("ParentId").alias("parent_id"),
            F.col("Industry").alias("industry"),
            F.col("AnnualRevenue").alias("annual_revenue"),
            F.col("NumberOfEmployees").alias("number_of_employees"),
            F.col("Description").alias("description"),

            # Billing address — full + ISO codes
            F.col("BillingStreet").alias("billing_street"),
            F.col("BillingCity").alias("billing_city"),
            F.col("BillingState").alias("billing_state"),
            F.col("BillingStateCode").alias("billing_state_code"),
            F.col("BillingPostalCode").alias("billing_postal_code"),
            F.col("BillingCountry").alias("billing_country"),
            F.col("BillingCountryCode").alias("billing_country_code"),

            # Shipping address — full + ISO codes
            F.col("ShippingStreet").alias("shipping_street"),
            F.col("ShippingCity").alias("shipping_city"),
            F.col("ShippingState").alias("shipping_state"),
            F.col("ShippingStateCode").alias("shipping_state_code"),
            F.col("ShippingPostalCode").alias("shipping_postal_code"),
            F.col("ShippingCountry").alias("shipping_country"),
            F.col("ShippingCountryCode").alias("shipping_country_code"),

            # Contact
            F.col("Phone").alias("phone"),
            F.col("Website").alias("website"),

            # Dates
            F.col("CreatedDate").alias("created_date"),
            F.col("LastModifiedDate").alias("last_modified_date"),

            # SCD2 history
            F.col("__START_AT").alias("start_at"),
            F.col("__END_AT").alias("end_at"),

            # is_active re-derived from SCD2
            F.when(F.col("__END_AT").isNull(), F.lit(True))
             .otherwise(F.lit(False))
             .alias("is_active"),

            # Audit
            F.current_timestamp().alias("processed_at")
        )
    )