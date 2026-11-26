from pyspark import pipelines as dp
from pyspark.sql import functions as F

CONTENT_ATTRS = [
    "content_type", "title", "isrc", "artist_id", "album_name", "label", "genre",
    "release_year", "bpm", "energy", "explicit_flag", "content_duration_ms",
]


@dp.materialized_view(
    name="dim_content",
    comment="Content dimension: music tracks, retailer ads and announcements. Music-only attributes are NULL for ads/announcements.",
)
def dim_content():
    return (
        spark.read.table("silver_play_events")
        .groupBy("content_id")
        .agg(*[F.max_by(c, "event_ts_utc").alias(c) for c in CONTENT_ATTRS])
    )
