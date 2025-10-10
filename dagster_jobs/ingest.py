# /work/dagster_jobs/ingest.py
import os
from datetime import datetime, timezone
from io import BytesIO

import boto3
import pandas as pd
from dagster import (
    Definitions, sensor, RunRequest, job, op, get_dagster_logger, OpExecutionContext, In
)
from dagster_dbt import DbtCliResource

# ---------- Config / helpers ----------

def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )

DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/work/dbt")
dbt = DbtCliResource(project_dir="/work/dbt", profiles_dir=DBT_PROFILES_DIR)

BUCKET = os.getenv("S3_BUCKET", "ngods")
INGEST_PREFIX = os.getenv("S3_PREFIX", "ingest/")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver/")  # opcional

# ---------- Ops ----------

@op
def download_from_minio(key: str) -> bytes:
    """Descarga el objeto del bucket/prefijo indicado y devuelve los bytes."""
    s3 = make_s3()
    buf = BytesIO()
    s3.download_fileobj(Bucket=BUCKET, Key=key, Fileobj=buf)
    buf.seek(0)
    get_dagster_logger().info(f"Descargado {key} ({buf.getbuffer().nbytes} bytes)")
    return buf.getvalue()

@op
def validate_is_excel(payload: bytes, key: str) -> str:
    """Valida extensión .xlsx/.xls y devuelve la extensión normalizada."""
    if key.lower().endswith(".xlsx"):
        return "xlsx"
    if key.lower().endswith(".xls"):
        return "xls"
    # si quieres ignorar otros, lanza Failure para cortar el run
    from dagster import Failure
    raise Failure(f"El archivo {key} no es Excel (.xlsx/.xls)")

@op
def parse_excel_to_dataframe(payload: bytes, ext: str) -> pd.DataFrame:
    """Lee el Excel a DataFrame (primera hoja). Puedes ampliar para varias hojas."""
    engine = "openpyxl" if ext == "xlsx" else None  # xlrd ya no soporta xls nuevos
    df = pd.read_excel(BytesIO(payload), engine=engine)
    # Limpieza básica opcional
    df.columns = [str(c).strip() for c in df.columns]
    get_dagster_logger().info(f"Leído Excel -> DataFrame con {len(df)} filas y {len(df.columns)} columnas")
    return df

@op(ins={"key": In(default_value=None, description="S3 key del objeto fuente (opcional)")})
def upload_dataframe_as_parquet(context: OpExecutionContext, df: pd.DataFrame, key: str | None) -> str:
    if not key:
        key = context.run.tags.get("s3_key")
    # ruta destino: silver/<basename_sin_ext>.parquet
    base = os.path.basename(key)
    stem = os.path.splitext(base)[0]
    out_key = f"{SILVER_PREFIX.rstrip('/')}/{stem}.parquet"

    # a parquet en memoria
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    s3 = make_s3()
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue())
    get_dagster_logger().info(f"Subido a MinIO: s3://{BUCKET}/{out_key}")
    return out_key

@op
def run_dbt_build() -> str:
    res = dbt.cli(["build"])
    res.wait()
    return "dbt build OK"

# ---------- Job ----------

@job
def ingest_job():
    # El sensor inyecta "key" vía run_config
    payload = download_from_minio()
    ext = validate_is_excel(payload, )  # usa implicit param mapping (payload, key)
    df = parse_excel_to_dataframe(payload, ext)
    _out_key = upload_dataframe_as_parquet(df, )
    run_dbt_build()

# ---------- Sensor ----------

@sensor(job=ingest_job, minimum_interval_seconds=3)
def s3_new_objects_sensor(context):
    s3 = make_s3()
    last_seen = context.cursor
    now = datetime.now(timezone.utc)

    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=INGEST_PREFIX)
    contents = resp.get("Contents", [])
    if not contents:
        if last_seen is None:
            context.update_cursor(now.isoformat())
        return

    new_items = []
    for obj in contents:
        key = obj["Key"]
        if key.endswith("/"):
            continue
        lm_iso = obj["LastModified"].astimezone(timezone.utc).isoformat()
        if last_seen is None or lm_iso > last_seen:
            new_items.append((key, lm_iso))

    if not new_items:
        if last_seen is None:
            context.update_cursor(now.isoformat())
        return

    new_items.sort(key=lambda t: t[1])
    for key, lm_iso in new_items:
        # Solo Excel: si quieres disparar solo para .xlsx/.xls, descomenta:
        # if not (key.lower().endswith(".xlsx") or key.lower().endswith(".xls")):
        #     continue

        yield RunRequest(
            run_key=lm_iso,
            run_config={
                "ops": {
                    "download_from_minio":   {"inputs": {"key": {"value": key}}},
                    "validate_is_excel":     {"inputs": {"key": {"value": key}}},
                    # "upload_dataframe_as_parquet":  # <- ya no hace falta
                }
            },
            tags={"s3_key": key},
        )


    context.update_cursor(new_items[-1][1])

# ---------- Definitions ----------
defs = Definitions(
    jobs=[ingest_job],
    sensors=[s3_new_objects_sensor],
    resources={"dbt": dbt},
)
