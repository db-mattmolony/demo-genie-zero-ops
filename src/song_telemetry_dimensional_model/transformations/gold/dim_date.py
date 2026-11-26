from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="dim_date", comment="Calendar dimension (store-local dates) 2026-2027, incl. Australian fiscal year (Jul-Jun).")
def dim_date():
    dates = spark.sql(
        "SELECT explode(sequence(DATE'2026-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS date"
    )
    return dates.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day_of_month"),
        F.dayofweek("date").alias("day_of_week"),  # 1 = Sunday
        F.date_format("date", "EEEE").alias("day_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        (F.year("date") + F.when(F.month("date") >= 7, 1).otherwise(0)).alias("au_fiscal_year"),
    )
