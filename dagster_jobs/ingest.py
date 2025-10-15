import os
from datetime import datetime, timezone
from io import BytesIO
from zoneinfo import ZoneInfo
from typing import Iterable

import boto3
import pandas as pd
from dagster import (
    Definitions, sensor, RunRequest, job, op, get_dagster_logger, OpExecutionContext, In
)
from dagster_dbt import DbtCliResource


# ---------- Config ----------
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/work/dbt")
BUCKET        = os.getenv("S3_BUCKET", "ngods")
INGEST_PREFIX = os.getenv("S3_PREFIX",  "ingest/")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver/")
TZ_EUROPE_MAD = ZoneInfo("Europe/Madrid")

dbt = DbtCliResource(project_dir="/work/dbt", profiles_dir=DBT_PROFILES_DIR)


# ---------- Helpers ----------
def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )

def _local_parts(ts_utc: datetime) -> dict:
    ts_local = ts_utc.astimezone(TZ_EUROPE_MAD)
    return {
        "y":  ts_local.year,
        "m":  f"{ts_local.month:02d}",
        "d":  f"{ts_local.day:02d}",
        "hh": f"{ts_local.hour:02d}",
        "mm": f"{ts_local.minute:02d}",
        "ts": ts_local,  # por si se quiere loggear
    }

def _list_objects(prefix: str) -> Iterable[dict]:
    s3 = make_s3()
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    for obj in resp.get("Contents", []):
        if not obj["Key"].endswith("/"):
            yield obj


# ---------- Ops ----------
@op
def download_from_minio(key: str) -> bytes:
    s3 = make_s3()
    buf = BytesIO()
    s3.download_fileobj(Bucket=BUCKET, Key=key, Fileobj=buf)
    buf.seek(0)
    get_dagster_logger().info(f"Descargado {key} ({buf.getbuffer().nbytes} bytes)")
    return buf.getvalue()

@op
def validate_is_excel(payload: bytes, key: str) -> str:
    k = key.lower()
    if k.endswith(".xlsx"): return "xlsx"
    if k.endswith(".xls"):  return "xls"
    from dagster import Failure
    raise Failure(f"El archivo {key} no es Excel (.xlsx/.xls)")

@op
def parse_excel_to_dataframe(payload: bytes, ext: str) -> pd.DataFrame:
    # pandas elegirá el motor adecuado; usa openpyxl para xlsx si está instalado
    df = pd.read_excel(BytesIO(payload))
    df.columns = [str(c).strip() for c in df.columns]
    get_dagster_logger().info(f"Excel leído: {len(df)} filas × {len(df.columns)} columnas")
    return df

@op(ins={"key": In(default_value=None, description="S3 key fuente (opcional, se coge de tags si no se pasa)")})
def upload_dataframe_as_parquet(context: OpExecutionContext, df: pd.DataFrame, key: str | None) -> str:
    key = key or context.run.tags.get("s3_key")
    lm_iso = context.run.tags.get("s3_last_modified")
    ts_utc = datetime.fromisoformat(lm_iso) if lm_iso else datetime.now(timezone.utc)
    parts = _local_parts(ts_utc)

    df = df.copy()
    df["year"], df["month"], df["day"], df["hour"], df["minute"] = (
        parts["y"], int(parts["m"]), int(parts["d"]), int(parts["hh"]), int(parts["mm"])
    )

    base = os.path.basename(key)
    stem = os.path.splitext(base)[0]
    out_key = (
        f"{SILVER_PREFIX.rstrip('/')}/"
        f"year={parts['y']}/month={parts['m']}/day={parts['d']}/hour={parts['hh']}/minute={parts['mm']}/"
        f"{stem}.parquet"
    )

    import pyarrow as pa
    import pyarrow.parquet as pq
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3 = make_s3()
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue())
    get_dagster_logger().info(
        f"Subido: s3://{BUCKET}/{out_key} (partición {parts['y']}-{parts['m']}-{parts['d']} {parts['hh']}:{parts['mm']} Europe/Madrid)"
    )
    return out_key

@op
def run_dbt_build() -> str:
    res = dbt.cli(["build"])
    res.wait()
    return "dbt build OK"


# ---------- Job ----------
@job
def ingest_job():
    payload = download_from_minio()
    ext     = validate_is_excel(payload)
    df      = parse_excel_to_dataframe(payload, ext)
    _out    = upload_dataframe_as_parquet(df)
    run_dbt_build()


# ---------- Sensor ----------
@sensor(job=ingest_job, minimum_interval_seconds=3)
def s3_new_objects_sensor(context):
    last_seen = context.cursor
    now_iso   = datetime.now(timezone.utc).isoformat()

    items = []
    for obj in _list_objects(INGEST_PREFIX):
        lm_iso = obj["LastModified"].astimezone(timezone.utc).isoformat()
        if last_seen is None or lm_iso > last_seen:
            items.append((obj["Key"], lm_iso))

    if not items:
        if last_seen is None:
            context.update_cursor(now_iso)
        return

    items.sort(key=lambda x: x[1])
    for key, lm_iso in items:
        yield RunRequest(
            run_key=lm_iso,
            run_config={
                "ops": {
                    "download_from_minio": {"inputs": {"key": {"value": key}}},
                    "validate_is_excel":   {"inputs": {"key": {"value": key}}},
                }
            },
            tags={"s3_key": key, "s3_last_modified": lm_iso},
        )

    context.update_cursor(items[-1][1])


# ---------- Definitions ----------
defs = Definitions(
    jobs=[ingest_job],
    sensors=[s3_new_objects_sensor],
    resources={"dbt": dbt},
)
