"""
Job de ETL para refinar datos de BRONZE a SILVER.
Lee datos crudos de una tabla particionada por hora, los enriquece, 
los re-particiona y los compacta por minuto en una nueva tabla refinada.

Uso:
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /work/generador-datos/compaction_job_by_hour.py \
  --tabla-origen eventos_crudos_por_hora \
  --tabla-destino eventos_refinados_por_minuto \
  --fecha 2025-10-09 \
  --hora 14
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, minute

# --- CONFIGURACIÓN ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "MinioPass_2025!"
S3_BUCKET = "ngods"
WAREHOUSE_PATH = f"s3a://{S3_BUCKET}/warehouse"
HIVE_METASTORE_URIS = "thrift://metastore:9083"
DATABASE_NAME = "default"

def main():
    parser = argparse.ArgumentParser(description="Refina datos de una partición de HORA y los compacta por MINUTO en una nueva tabla.")
    
    parser.add_argument("--tabla-origen", default="eventos_crudos_por_hora", help="Tabla BRONZE de origen con datos por hora.")
    parser.add_argument("--tabla-destino", default="eventos_refinados_por_minuto", help="Tabla SILVER de destino a crear/sobrescribir con datos por minuto.") 
    parser.add_argument("--fecha", required=True, help="La fecha a procesar en formato YYYY-MM-DD.")
    parser.add_argument("--hora", required=True, type=int, help="La hora a procesar (0-23).")
    args = parser.parse_args()

    # --- INICIALIZACIÓN DE SPARK ---
    spark = SparkSession.builder \
        .appName(f"RefineAndCompact-{args.tabla_origen}-{args.fecha}-{args.hora}") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir", WAREHOUSE_PATH) \
        .config("hive.metastore.uris", HIVE_METASTORE_URIS) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()

    year = int(args.fecha.split('-')[0])
    month = int(args.fecha.split('-')[1])
    day = int(args.fecha.split('-')[2])
    hour_to_process = args.hora
    
    full_source_table = f"{DATABASE_NAME}.{args.tabla_origen}"
    full_dest_table = f"{DATABASE_NAME}.{args.tabla_destino}"

    try:
        # 1. Leer los datos de la tabla origen (BRONZE), filtrando por la hora
        print(f"Leyendo datos de '{full_source_table}' para la hora {hour_to_process} del {args.fecha}...")
        df = spark.read.table(full_source_table).where(
            (col("year") == year) & (col("month") == month) & (col("day") == day) & (col("hour") == hour_to_process)
        )
        
        # 2. CREAR la nueva columna de partición 'minute' a partir del timestamp
        df_with_minute = df.withColumn("minute", minute(col("timestamp_ingesta")))
        
        # Contar filas para verificación
        count = df_with_minute.count()
        if count == 0:
            print("No se encontraron datos en la tabla origen para la hora especificada. No se hará nada.")
            return

        print(f"Datos leídos ({count} filas). Procediendo a reparticionar por minuto...")

        # 3. Agrupar los datos en memoria por cada minuto. Cada grupo se convertirá en un archivo.
        partition_cols = ["year", "month", "day", "hour", "minute"]
        df_compacted = df_with_minute.repartition(*partition_cols)

        # 4. Escribir en la tabla de destino (SILVER). 
        #    ¡Esta operación CREARÁ la estructura de carpetas por minuto!
        print(f"Escribiendo en la tabla de destino '{full_dest_table}'...")
        df_compacted.write \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .format("parquet") \
            .saveAsTable(full_dest_table)
            
        print("¡Proceso completado con éxito!")

    except Exception as e:
        print(f"Error durante el proceso: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

