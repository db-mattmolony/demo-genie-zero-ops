from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="dim_zone", comment="In-store audio zone dimension; each zone belongs to one store.")
def dim_zone():
    return (
        spark.read.table("silver_play_events")
        .groupBy("zone_id")
        .agg(
            F.max_by("zone_name", "event_ts_utc").alias("zone_name"),
            F.max_by("store_id", "event_ts_utc").alias("store_id"),
        )
    )
