from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view(
    name="play_events_clean",
    comment="Typed and validated play events, before de-duplication.",
)
@dp.expect_all_or_drop({
    "event_id_not_null": "event_id IS NOT NULL",
    "event_ts_not_null": "event_ts_utc IS NOT NULL",
    "store_id_not_null": "store_id IS NOT NULL",
    "device_id_not_null": "device_id IS NOT NULL",
    "content_id_not_null": "content_id IS NOT NULL",
})
@dp.expect_all({
    "played_within_duration": "played_duration_ms <= content_duration_ms",
    "volume_in_range": "volume_level BETWEEN 0 AND 100",
    "valid_content_type": "content_type IN ('music', 'ad', 'announcement')",
})
def play_events_clean():
    return (
        spark.readStream.table("bronze_play_events")
        .withColumn("event_ts_utc", F.to_timestamp("event_ts_utc"))
        .withColumn("ingest_ts_utc", F.to_timestamp("ingest_ts_utc"))
        # Store-local wall clock parsed from the string so it is independent of session timezone
        .withColumn("event_date_local", F.to_date(F.substring("event_ts_local", 1, 10)))
        .withColumn("local_hour", F.substring("event_ts_local", 12, 2).cast("int"))
        .withColumn("local_minute", F.substring("event_ts_local", 15, 2).cast("int"))
        .withColumn("date_key", F.date_format("event_date_local", "yyyyMMdd").cast("int"))
        .withColumn("time_key", F.col("local_hour") * 100 + F.col("local_minute"))
        .drop("_rescued_data")
    )


# De-duplicate on event_id (devices can re-upload cached plays); latest ingest wins
dp.create_streaming_table(
    name="silver_play_events",
    comment="Cleansed, de-duplicated play events: one row per event_id.",
)

dp.create_auto_cdc_flow(
    target="silver_play_events",
    source="play_events_clean",
    keys=["event_id"],
    sequence_by="ingest_ts_utc",
    stored_as_scd_type=1,
)
