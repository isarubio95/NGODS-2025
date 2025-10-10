"""
Job de ETL para REPARAR una tabla existente en el data lake.
Lee los datos de una partición de HORA de una tabla, los RE-PARTICIONA por MINUTO
y reemplaza las particiones de la tabla original con la nueva estructura de forma segura.

Este es un script de MANTENIMIENTO para arreglar un data lake ya en funcionamiento.

Uso:
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /work/generador-datos/repair_partition_job.py \
  --tabla eventos_en_tiempo_real \
  --fecha 2025-10-09 \
  --hora 8
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
    parser = argparse.ArgumentParser(description="Repara una tabla re-particionando los datos de una hora por minuto.")
    parser.add_argument("--tabla", required=True, help="La tabla a reparar (origen y destino).")
    parser.add_argument("--fecha", required=True, help="La fecha a procesar en formato YYYY-MM-DD.")
    parser.add_argument("--hora", required=True, type=int, help="La hora a procesar (0-23).")
    args = parser.parse_args()

    # --- INICIALIZACIÓN DE SPARK ---
    spark = SparkSession.builder \
        .appName(f"RepairJob-{args.tabla}-{args.fecha}-{args.hora}") \
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
    
    full_table_name = f"{DATABASE_NAME}.{args.tabla}"
    temp_table_name = f"{full_table_name}_temp_migration"
    backup_table_name = f"{full_table_name}_backup"

    try:
        # 1. Leer los datos de la tabla original, filtrando por la hora
        print(f"Leyendo datos de '{full_table_name}' para la hora {hour_to_process} del {args.fecha}...")
        df = spark.read.table(full_table_name).where(
            (col("year") == year) & (col("month") == month) & (col("day") == day) & (col("hour") == hour_to_process)
        )
        
        count = df.count()
        if count == 0:
            print("No se encontraron datos para la hora especificada. No se necesita hacer nada.")
            spark.stop()
            return
            
        print(f"Datos leídos ({count} filas). Procediendo a re-particionar y escribir en tabla temporal...")

        # 2. CREAR la nueva columna de partición 'minute'
        df_with_minute = df.withColumn("minute", minute(col("timestamp_ingesta")))
        
        # 3. Agrupar los datos en memoria por cada minuto
        partition_cols = ["year", "month", "day", "hour", "minute"]
        df_compacted = df_with_minute.repartition(*partition_cols)

        # 4. Escribir en una tabla TEMPORAL con la nueva estructura
        spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")
        df_compacted.write \
            .mode("overwrite") \
            .partitionBy(*partition_cols) \
            .saveAsTable(temp_table_name)
        
        print(f"Datos escritos correctamente en la tabla temporal '{temp_table_name}'.")

        # 5. Realizar el intercambio de tablas (SWAP) de forma segura
        print("Realizando el intercambio de tablas...")
        spark.sql(f"DROP TABLE IF EXISTS {backup_table_name}")
        spark.sql(f"ALTER TABLE {full_table_name} RENAME TO {backup_table_name}")
        spark.sql(f"ALTER TABLE {temp_table_name} RENAME TO {full_table_name}")
        spark.sql(f"DROP TABLE {backup_table_name}")
        
        print("¡Proceso de reparación completado con éxito!")

    except Exception as e:
        print(f"Error durante el proceso: {e}")
        # Lógica de Rollback en caso de error durante el SWAP
        print("Intentando restaurar el estado original...")
        try:
            # Si el backup existe, lo restauramos
            spark.sql(f"ALTER TABLE {backup_table_name} RENAME TO {full_table_name}")
            print("Rollback exitoso. La tabla original ha sido restaurada.")
        except Exception as rollback_e:
            print(f"¡FALLO CRÍTICO EN EL ROLLBACK! Revisión manual requerida. La tabla original puede estar en '{backup_table_name}'. Error: {rollback_e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

