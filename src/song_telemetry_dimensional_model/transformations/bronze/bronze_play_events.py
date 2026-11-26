from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_PATH = "/Volumes/cricket_australia_sponsor/song_telemetry/events/landing/"


@dp.table(
    name="bronze_play_events",
    comment="Raw in-store play events ingested incrementally from the landing volume with Auto Loader.",
)
def bronze_play_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        # Keep timestamps as raw strings in bronze; they are typed in silver
        .option(
            "cloudFiles.schemaHints",
            "event_ts_utc STRING, event_ts_local STRING, ingest_ts_utc STRING, "
            "latitude DOUBLE, longitude DOUBLE, energy DOUBLE, completion_pct DOUBLE",
        )
        .option("pathGlobFilter", "*.json")
        .load(LANDING_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("source_file"),
            F.current_timestamp().alias("bronze_loaded_at"),
        )
    )
