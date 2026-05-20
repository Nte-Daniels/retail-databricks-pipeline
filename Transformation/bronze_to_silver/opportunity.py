import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="opportunity",
    comment="Silver Salesforce opportunities — standardised, DQ validated"
)
@dlt.expect_or_drop("non_null_id",         "id IS NOT NULL")
@dlt.expect("non_null_name",               "name IS NOT NULL")
@dlt.expect("non_deleted_opportunity",     "is_deleted = false")
@dlt.expect("valid_amount",                "amount IS NULL OR amount >= 0")
@dlt.expect("valid_probability",           "probability IS NULL OR (probability >= 0 AND probability <= 100)")
@dlt.expect("valid_stage",                 
    "stage_name IN ('Prospecting','Qualification','Needs Analysis',"
    "'Negotiation/Review','Proposal/Price Quote','Closed Won','Closed Lost')")
def opportunity():
    """
    Silver layer Salesforce opportunities.

    Transformations:
    - PascalCase → snake_case column renaming
    - deal_size derived from amount — NULL-safe
    - All internal Salesforce system columns dropped
    - processed_at audit column added

    DQ Rules:
    - DROP row  : NULL id
    - WARN only : NULL name, deleted, negative amount,
                  invalid probability, unknown stage
    """
    return (
        spark.read.table("retail_q.salesforce_bronze.opportunity")
        .select(
            # Keys
            F.col("Id").alias("id"),
            F.col("AccountId").alias("account_id"),

            # Status flags
            F.col("IsDeleted").alias("is_deleted"),
            F.col("IsClosed").alias("is_closed"),
            F.col("IsWon").alias("is_won"),

            # Core fields
            F.col("Name").alias("name"),
            F.col("StageName").alias("stage_name"),
            F.col("ForecastCategory").alias("forecast_category"),
            F.col("Type").alias("opportunity_type"),
            F.col("LeadSource").alias("lead_source"),

            # Financials
            F.col("Amount").alias("amount"),
            F.col("Probability").alias("probability"),

            # Deal size — NULL-safe
            F.when(F.col("Amount").isNull(),    None)
             .when(F.col("Amount") > 100000,    F.lit("ENTERPRISE"))
             .when(F.col("Amount") > 25000,     F.lit("MID_MARKET"))
             .otherwise(F.lit("SMALL"))
             .alias("deal_size"),

            # Dates
            F.col("CloseDate").alias("close_date"),
            F.col("CreatedDate").alias("created_date"),
            F.col("LastModifiedDate").alias("last_modified_date"),

            # Audit
            F.current_timestamp().alias("processed_at")
        )
    )