from pyspark import pipelines as dp
from pyspark.sql import functions as F

STORE_ATTRS = [
    "store_name", "retailer_id", "retailer_name", "retailer_category", "store_format",
    "city", "state", "country", "store_timezone", "latitude", "longitude",
]


@dp.materialized_view(name="dim_store", comment="Store dimension (SCD Type 1, latest observed attributes).")
def dim_store():
    return (
        spark.read.table("silver_play_events")
        .groupBy("store_id")
        .agg(
            *[F.max_by(c, "event_ts_utc").alias(c) for c in STORE_ATTRS],
            F.min("event_date_local").alias("first_seen_date"),
            F.max("event_date_local").alias("last_seen_date"),
        )
    )
