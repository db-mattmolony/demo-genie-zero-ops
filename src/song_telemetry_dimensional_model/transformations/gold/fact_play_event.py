from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="fact_play_event",
    comment="Play event fact at one row per play. Joins to dim_date (date_key), dim_time (time_key), dim_store, "
            "dim_zone, dim_content, dim_artist, dim_playlist, and the dim_device version valid at play time.",
    cluster_by=["date_key", "store_id"],
)
def fact_play_event():
    e = spark.read.table("silver_play_events")
    d = spark.read.table("dim_device").select(
        F.col("device_id").alias("dim_device_id"),
        F.col("__START_AT").alias("device_valid_from"),
        F.col("__END_AT").alias("device_valid_to"),
    )
    joined = e.join(
        d,
        (e["device_id"] == d["dim_device_id"])
        & (e["event_ts_utc"] >= d["device_valid_from"])
        & (d["device_valid_to"].isNull() | (e["event_ts_utc"] < d["device_valid_to"])),
        "left",
    )
    return joined.select(
        "event_id",
        # dimension keys
        "date_key", "time_key", "store_id", "zone_id", "device_id", "device_valid_from",
        "content_id", "artist_id", "playlist_id",
        # degenerate attributes
        "event_ts_utc", "ingest_ts_utc", "event_date_local", "daypart", "content_type", "end_reason",
        # measures
        "played_duration_ms", "content_duration_ms", "completion_pct", "volume_level", "buffer_events",
        "played_from_cache", "royalty_reportable",
        (F.col("end_reason") == "completed").alias("is_completed"),
        (F.col("end_reason") == "skipped").alias("is_skipped"),
        (F.unix_timestamp("ingest_ts_utc") - F.unix_timestamp("event_ts_utc")).alias("ingest_latency_s"),
    )
