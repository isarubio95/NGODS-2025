"""
Job de mantenimiento para compactar los datos de una HORA en un único archivo.
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# --- CONFIGURACIÓN ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "MinioPass_2025!"
S3_BUCKET = "ngods"
WAREHOUSE_PATH = f"s3a://{S3_BUCKET}/warehouse"
HIVE_METASTORE_URIS = "thrift://metastore:9083"
DATABASE_NAME = "default"

def main():
    parser = argparse.ArgumentParser(description="Compacta una partición de hora de una tabla de Spark.")
    parser.add_argument("--tabla", required=True, help="El nombre de la tabla a compactar.")
    parser.add_argument("--fecha", required=True, help="La fecha de la partición en formato YYYY-MM-DD.")
    parser.add_argument("--hora", required=True, type=int, help="La hora de la partición (0-23).")
    args = parser.parse_args()

    table_name = args.tabla
    partition_date_str = args.fecha
    hour_to_compact = args.hora
    
    try:
        partition_date = datetime.strptime(partition_date_str, "%Y-%m-%d")
    except ValueError:
        print("Error: El formato de la fecha debe ser YYYY-MM-DD.")
        return

    # --- INICIALIZACIÓN DE SPARK ---
    spark = SparkSession.builder \
        .appName(f"CompactionJobByHour-{table_name}-{partition_date_str}-{hour_to_compact}") \
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

    full_table_name = f"{DATABASE_NAME}.{table_name}"
    year, month, day = partition_date.year, partition_date.month, partition_date.day

    print(f"Iniciando compactación para la tabla '{full_table_name}', partición de HORA: {year}-{month:02d}-{day:02d} HORA {hour_to_compact:02d}")

    try:
        partition_path = f"{WAREHOUSE_PATH}/{table_name}/year={year}/month={month}/day={day}/hour={hour_to_compact}"
        
        print(f"Leyendo archivos directamente desde: {partition_path}")
        df = spark.read.parquet(partition_path)

        df.cache()
        num_files_before = df.rdd.getNumPartitions()

        if num_files_before <= 1:
            print(f"No se necesita compactación. La partición ya tiene {num_files_before} archivo(s).")
            spark.stop()
            return
        
        print(f"La partición contiene aproximadamente {num_files_before} archivos pequeños. Procediendo a compactar...")

        # ### CAMBIO FUNDAMENTAL: "Rehidratar" el DataFrame ###
        # Volvemos a añadir las columnas de partición con los valores que ya conocemos.
        df_with_partitions = df.withColumn("year", lit(year)) \
                               .withColumn("month", lit(month)) \
                               .withColumn("day", lit(day)) \
                               .withColumn("hour", lit(hour_to_compact))
        
        # Agrupamos todo en una sola partición en memoria
        df_compacted = df_with_partitions.repartition(1)
        
        # Ahora el DataFrame que escribimos SÍ tiene las columnas de partición
        df_compacted.write \
            .mode("overwrite") \
            .insertInto(full_table_name)
            
        print("¡Compactación completada con éxito!")

    except Exception as e:
        print(f"Error durante la compactación: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()