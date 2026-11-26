from pyspark import pipelines as dp


@dp.temporary_view(name="device_observations", comment="Device attributes observed on each play event.")
def device_observations():
    return spark.readStream.table("play_events_clean").select(
        "device_id", "player_model", "firmware_version", "network_type", "store_id", "zone_id", "event_ts_utc"
    )


dp.create_streaming_table(
    name="dim_device",
    comment="Player device dimension, SCD Type 2 (new version when firmware, network, model or location changes). "
            "Validity in __START_AT / __END_AT (UTC event time).",
)

dp.create_auto_cdc_flow(
    target="dim_device",
    source="device_observations",
    keys=["device_id"],
    sequence_by="event_ts_utc",
    except_column_list=["event_ts_utc"],
    stored_as_scd_type=2,
    track_history_column_list=["firmware_version", "network_type", "player_model", "store_id", "zone_id"],
)
