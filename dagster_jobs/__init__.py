from dagster import Definitions
from .ingest_silver_job import ingest_silver_job, s3_new_objects_sensor_silver
from .organize_parquet import organize_parquet, hourly_compaction

defs = Definitions(
    jobs=[ingest_silver_job, organize_parquet],
    schedules=[hourly_compaction],
    sensors=[s3_new_objects_sensor_silver],
)
