from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="dim_playlist", comment="Playlist dimension with its scheduled daypart.")
def dim_playlist():
    return (
        spark.read.table("silver_play_events")
        .groupBy("playlist_id")
        .agg(
            F.max_by("playlist_name", "event_ts_utc").alias("playlist_name"),
            F.max_by("daypart", "event_ts_utc").alias("scheduled_daypart"),
        )
    )
