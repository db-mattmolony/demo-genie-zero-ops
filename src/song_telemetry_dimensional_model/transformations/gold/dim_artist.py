from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="dim_artist", comment="Artist dimension (music content only).")
def dim_artist():
    return (
        spark.read.table("silver_play_events")
        .where("artist_id IS NOT NULL")
        .groupBy("artist_id")
        .agg(
            F.max_by("artist_name", "event_ts_utc").alias("artist_name"),
            F.max_by("label", "event_ts_utc").alias("label"),
        )
    )
