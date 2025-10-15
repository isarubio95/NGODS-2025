import os
from datetime import timezone
from collections import defaultdict
from urllib.parse import urljoin

import boto3
from dagster import (
    Definitions, job, op, In, Out, get_dagster_logger, ScheduleDefinition, Config, Nothing
)

# ---------- Configuración ----------
# Origen y destino en S3 (MinIO). Usa "file://" si quieres local.
BUCKET = os.getenv("S3_BUCKET", "ngods")
SOURCE_PREFIX = os.getenv("PARQUET_SOURCE_PREFIX", "landing/parquet/")
TARGET_PREFIX = os.getenv("PARQUET_TARGET_PREFIX", "silver/parquet_by_hour/")
# nº de archivos resultantes por partición tras compactar (1 = máximo compactado)
FILES_PER_PARTITION = int(os.getenv("COMPACT_FILES_PER_PARTITION", "1"))

def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )

# ---------- Ops ----------
class ScanConfig(Config):
    # límite de archivos por ejecución para evitar picos
    max_files: int = 500

@op(out=Out(list), config_schema=ScanConfig.to_config_schema())
def scan_loose_parquets(context) -> list[dict]:
    # Lista objetos .parquet en el SOURCE_PREFIX.
    log = get_dagster_logger()
    s3 = make_s3()
    paginator = s3.get_paginator("list_objects_v2")
    found = []
    count = 0
    for page in paginator.paginate(Bucket=BUCKET, Prefix=SOURCE_PREFIX):
        for obj in page.get("Contents", []):
            if not obj["Key"].lower().endswith(".parquet"):
                continue
            found.append({"key": obj["Key"], "last_modified": obj["LastModified"]})
            count += 1
            if count >= context.op_config.get("max_files", 500):
                break
        if count >= context.op_config.get("max_files", 500):
            break
    log.info(f"Encontrados {len(found)} parquet(s) en {BUCKET}/{SOURCE_PREFIX}")
    return found

@op(out=Out(list))
def move_into_hour_partitions(parquets: list[dict]) -> list[dict]:
    """
    Copia cada parquet a ruta tipo:
    silver/parquet_by_hour/year=YYYY/month=MM/day=DD/hour=HH/<archivo>
    y elimina el original. Devuelve la lista de particiones tocadas.
    """
    log = get_dagster_logger()
    s3 = make_s3()
    touched = []
    by_partition = defaultdict(list)

    for item in parquets:
        key = item["key"]
        ts = item["last_modified"].astimezone(timezone.utc)
        year = ts.year
        month = f"{ts.month:02d}"
        day = f"{ts.day:02d}"
        hour = f"{ts.hour:02d}"

        folder = f"{TARGET_PREFIX}year={year}/month={month}/day={day}/hour={hour}/"
        filename = key.rsplit("/", 1)[-1]
        dst_key = folder + filename

        # copy + delete para "mover"
        s3.copy_object(
            Bucket=BUCKET,
            CopySource={"Bucket": BUCKET, "Key": key},
            Key=dst_key,
        )
        s3.delete_object(Bucket=BUCKET, Key=key)
        by_partition[(year, month, day, hour)].append(dst_key)

    for (y, m, d, h), keys in by_partition.items():
        touched.append({"year": int(y), "month": m, "day": d, "hour": h, "keys": keys})

    log.info(f"Reubicados {sum(len(v) for v in by_partition.values())} archivos en {len(touched)} particiones.")
    return touched

@op
def compact_touched_partitions(touched: list[dict]) -> None:
    """
    Compacta cada partición tocada con Spark: lee todos los parquet de la partición
    y reescribe coalesce(FILES_PER_PARTITION) en la misma ruta.
    """
    import pyspark
    from pyspark.sql import SparkSession

    log = get_dagster_logger()

    # Spark para MinIO S3A
    spark = (
        SparkSession.builder.appName("compact-parquet-by-hour")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    try:
        for part in touched:
            y, m, d, h = part["year"], part["month"], part["day"], part["hour"]
            partition_path = f"s3a://{BUCKET}/{TARGET_PREFIX}year={y}/month={m}/day={d}/hour={h}/"

            # leer todo lo que haya en esa partición
            df = spark.read.parquet(partition_path)

            # coalesce a N archivos grandes (1 por defecto)
            nfiles = int(os.getenv("COMPACT_FILES_PER_PARTITION", str(FILES_PER_PARTITION)))
            df2 = df.coalesce(max(1, nfiles))

            # Escribimos "in-place" sobre la propia partición
            # Para evitar lectores ver a medias: escribimos a _tmp y luego renombramos.
            tmp_path = partition_path.rstrip("/") + "_tmp/"
            df2.write.mode("overwrite").parquet(tmp_path)

            # mover tmp -> definitivo (via Hadoop FS)
            hadoop = spark._jvm.org.apache.hadoop
            conf = spark._jsc.hadoopConfiguration()
            Path = hadoop.fs.Path
            fs = Path(partition_path).getFileSystem(conf)

            # borrar destino actual y renombrar tmp
            fs.delete(Path(partition_path), True)
            fs.rename(Path(tmp_path), Path(partition_path))

            log.info(f"Compaction done: {partition_path}")
    finally:
        spark.stop()

# ---------- Job & Schedule ----------
@job
def organize_parquet():
    files = scan_loose_parquets()
    partitions = move_into_hour_partitions(files)
    compact_touched_partitions(partitions)

# Corre cada hora al minuto 5 (ajústalo a tu gusto)
hourly_compaction = ScheduleDefinition(
    job=organize_parquet,
    cron_schedule="5 * * * *",
    execution_timezone=os.getenv("TZ", "Europe/Madrid"),
    run_config={
        "ops": {
            "scan_loose_parquets": {"config": {"max_files": 1000}}
        }
    },
)
