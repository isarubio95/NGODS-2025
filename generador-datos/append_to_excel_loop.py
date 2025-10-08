"""
Continuously appends new rows to a single Excel file in MinIO in a loop.
If the file doesn't exist, it's created with a predefined 10-column schema.

Requires: pandas, openpyxl, boto3

Usage (PowerShell):
  $env:MINIO_ENDPOINT="http://localhost:9000"
  $env:MINIO_ACCESS_KEY="minio"
  $env:MINIO_SECRET_KEY="MinioPass_2025!"
  $env:S3_BUCKET="ngods"
  $env:S3_KEY="ingest/datos_centralizados.xlsx" # The single file to manage

  # Run with all defaults: adds 30 rows every 30 seconds
  python append_to_excel_loop.py

  # Override defaults
  python append_to_excel_loop.py --add-rows 5 --interval 10
"""

import os
import io
import argparse
import random
import uuid
import time
from datetime import datetime, timedelta

import pandas as pd
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# --- Funciones de Ayuda (sin cambios) ---
def rand_date(start, end):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def upload_bytes(s3, bucket, key, data: bytes):
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# --- Lógica Principal ---
def main():
    parser = argparse.ArgumentParser(description="Continuously appends rows to an Excel file in MinIO.")
    
    # ### CAMBIO: Se establece el valor por defecto de 'add-rows' a 30 ###
    parser.add_argument("--add-rows", type=int, default=30, help="Number of new rows to add in each batch (default: 30).")
    
    parser.add_argument("--interval", type=int, default=30, help="Seconds to wait between each batch (default: 30).")
    parser.add_argument("--sheet", type=str, default="registros", help="Excel sheet name.")
    parser.add_argument("--endpoint", type=str, default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--access-key", type=str, default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--secret-key", type=str, default=os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    parser.add_argument("--bucket", type=str, default=os.getenv("S3_BUCKET", "ngods"))
    parser.add_argument("--key", type=str, default=os.getenv("S3_KEY", "ingest/datos_centralizados.xlsx"))
    args = parser.parse_args()

    # Conexión a S3 (MinIO)
    session = boto3.session.Session()
    s3 = session.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )

    column_names = [
        "id_transaccion", "id_producto", "id_cliente", "fecha_evento",
        "cantidad", "precio_unitario", "descuento", "coste_envio",
        "region", "metodo_pago"
    ]
    
    while True:
        try:
            df_existente = pd.DataFrame(columns=column_names)

            # 1. Comprobar si el archivo ya existe y descargarlo
            try:
                print(f"Buscando archivo existente en s3://{args.bucket}/{args.key}...")
                response = s3.get_object(Bucket=args.bucket, Key=args.key)
                
                in_memory_file = io.BytesIO(response['Body'].read())
                df_existente = pd.read_excel(in_memory_file, sheet_name=args.sheet)
                print(f"Archivo encontrado. Contiene {len(df_existente)} filas.")
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print("El archivo no existe. Se creará uno nuevo.")
                else:
                    raise

            # 2. Generar las nuevas filas
            print(f"Generando {args.add_rows} filas nuevas...")
            nuevas_filas = []
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2025, 10, 1)
            
            for _ in range(args.add_rows):
                fila = {
                    "id_transaccion": str(uuid.uuid4()),
                    "id_producto": random.randint(1000, 2000),
                    "id_cliente": random.randint(1, 500),
                    "fecha_evento": rand_date(start_date, end_date),
                    "cantidad": random.randint(1, 10),
                    "precio_unitario": round(random.uniform(5.0, 500.0), 2),
                    "descuento": round(random.uniform(0.0, 0.25), 2),
                    "coste_envio": round(random.uniform(2.0, 20.0), 2),
                    "region": random.choice(["Norte", "Sur", "Este", "Oeste", "Centro"]),
                    "metodo_pago": random.choice(["Tarjeta", "PayPal", "Transferencia"])
                }
                nuevas_filas.append(fila)
            
            df_nuevas = pd.DataFrame(nuevas_filas)

            # 3. Añadir las nuevas filas al DataFrame existente
            df_final = pd.concat([df_existente, df_nuevas], ignore_index=True)
            print(f"Se han generado {len(df_final)} filas.")

            # 4. Escribir el DataFrame final en memoria y subirlo
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df_final.to_excel(writer, index=False, sheet_name=args.sheet)
            bio.seek(0)

            upload_bytes(s3, args.bucket, args.key, bio.getvalue())
            print(f"[OK] Archivo s3://{args.bucket}/{args.key} actualizado correctamente.")

        except Exception as e:
            print(f"Ha ocurrido un error: {e}")

        # Pausa antes de la siguiente iteración
        print(f"--- Esperando {args.interval} segundos para el siguiente lote ---")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()