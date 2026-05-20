import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="dim_calendar",
    comment="Gold calendar dimension — date range covering all transaction dates"
)
def dim_calendar():
    """
    Gold dim_calendar.
    Grain: one row per day.
    Generated from sequence — no source table needed.
    Covers 2026-01-01 to 2026-12-31 to span all transaction dates.
    date_key is integer YYYYMMDD — used as FK in fact_sales.
    """
    return spark.sql("""
        WITH date_range AS (
            SELECT explode(sequence(
                to_date('2026-01-01'),
                to_date('2026-12-31'),
                interval 1 day
            )) AS full_date
        )
        SELECT
            CAST(date_format(full_date, 'yyyyMMdd') AS INT)  AS date_key,
            full_date,
            dayofmonth(full_date)                            AS day,
            date_format(full_date, 'EEEE')                   AS day_name,
            weekofyear(full_date)                            AS week_number,
            month(full_date)                                 AS month,
            date_format(full_date, 'MMMM')                  AS month_name,
            quarter(full_date)                               AS quarter,
            year(full_date)                                  AS year,
            CASE
                WHEN dayofweek(full_date) IN (1, 7) THEN true
                ELSE false
            END                                              AS is_weekend,
            CASE
                WHEN dayofweek(full_date) NOT IN (1, 7) THEN true
                ELSE false
            END                                              AS is_weekday,
            concat(year(full_date), '-Q', quarter(full_date)) AS year_quarter,
            date_format(full_date, 'yyyy-MM')                AS year_month
        FROM date_range
        ORDER BY full_date
    """)