from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="dim_time", comment="Time-of-day dimension at minute grain (store-local). time_key = HHMM.")
def dim_time():
    minutes = spark.range(0, 1440).select(
        (F.col("id") / 60).cast("int").alias("hour"),
        (F.col("id") % 60).cast("int").alias("minute"),
    )
    return minutes.select(
        (F.col("hour") * 100 + F.col("minute")).alias("time_key"),
        "hour",
        "minute",
        F.format_string("%02d:%02d", "hour", "minute").alias("time_label"),
        F.when(F.col("hour") < 11, "morning")
         .when(F.col("hour") < 14, "midday")
         .when(F.col("hour") < 17, "afternoon")
         .otherwise("evening").alias("daypart"),
    )
