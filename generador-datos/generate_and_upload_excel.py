"""
Generate a batch of Excel files and upload them directly to MinIO (S3-compatible).
Requires: pandas, openpyxl, boto3

Usage (PowerShell):
  $env:MINIO_ENDPOINT="http://localhost:9000"
  $env:MINIO_ACCESS_KEY="minio"
  $env:MINIO_SECRET_KEY="MinioPass_2025!"
  $env:S3_BUCKET="ngods"
  $env:S3_PREFIX="raw/excel_samples"

  python generate_and_upload_excel.py --num-files 10 --rows 100

You can also override with CLI flags:
  --endpoint, --access-key, --secret-key, --bucket, --prefix
"""

import os
import io
import argparse
import random
from datetime import datetime, timedelta

import pandas as pd
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

def rand_date(start, end):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def ensure_bucket(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        # Create bucket if it doesn't exist
        # MinIO ignores region mostly, but we call create_bucket without LocationConstraint
        try:
            s3.create_bucket(Bucket=bucket)
        except ClientError as ce:
            # If bucket already owned by you or exists, ignore
            code = ce.response.get("Error", {}).get("Code", "")
            if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise

def upload_bytes(s3, bucket, key, data: bytes, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"):
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-files", type=int, default=30, help="How many Excel files to generate")
    parser.add_argument("--rows", type=int, default=200, help="Rows per file")
    parser.add_argument("--sheet", type=str, default="datos", help="Excel sheet name")
    parser.add_argument("--start-date", type=str, default="2024-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2025-10-01", help="End date (YYYY-MM-DD)")
    parser.add_argument("--endpoint", type=str, default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--access-key", type=str, default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--secret-key", type=str, default=os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    parser.add_argument("--bucket", type=str, default=os.getenv("S3_BUCKET", "ngods"))
    parser.add_argument("--prefix", type=str, default=os.getenv("S3_PREFIX", "raw/excel_samples"))
    parser.add_argument("--dry-run", action="store_true", help="Generate in-memory but don't upload")
    args = parser.parse_args()

    session = boto3.session.Session()
    s3 = session.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )

    ensure_bucket(s3, args.bucket)

    start_date = datetime.fromisoformat(args.start_date)
    end_date = datetime.fromisoformat(args.end_date)

    for i in range(1, args.num_files + 1):
        ids = list(range((i-1)*args.rows + 1, i*args.rows + 1))
        fechas = [rand_date(start_date, end_date).strftime("%Y-%m-%d") for _ in range(args.rows)]
        valores = [round(random.uniform(0, 1000), 2) for _ in range(args.rows)]
        df = pd.DataFrame({"id": ids, "fecha": fechas, "valor": valores})

        # Write to BytesIO to avoid local disk writes
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=args.sheet)
        bio.seek(0)

        key = f"{args.prefix}/lote_{i:03d}.xlsx"
        if args.dry_run:
            print(f"[DRY] Would upload s3://{args.bucket}/{key} ({len(bio.getvalue())} bytes)")
        else:
            upload_bytes(s3, args.bucket, key, bio.getvalue())
            print(f"[OK] Uploaded s3://{args.bucket}/{key}")

    print("Done.")

if __name__ == "__main__":
    main()
