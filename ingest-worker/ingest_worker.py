import os
import time
from minio import Minio
from pyspark.sql import SparkSession
# Se importan las funciones de tiempo necesarias
from pyspark.sql.functions import year, month, dayofmonth, hour
import pandas as pd

# --- CONFIGURACIÓN ---
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")
S3_BUCKET = os.environ.get("S3_BUCKET")

APP_NAME = "IngestWorker"

def main():
    print("Inicializando Ingest-Worker...")

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir", f"s3a://{S3_BUCKET}/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris", "thrift://metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()

    print("Ingest-Worker inicializado. Esperando ficheros...")

    while True:
        try:
            objects = minio_client.list_objects(S3_BUCKET, prefix="ingest/", recursive=True)
            for obj in objects:
                if obj.object_name.endswith(('.xlsx', '.xls')):
                    start_time = time.time()
                    print(f"Procesando fichero: {obj.object_name}")

                    local_path = f"/tmp/{os.path.basename(obj.object_name)}"
                    minio_client.fget_object(S3_BUCKET, obj.object_name, local_path)

                    pd_df = pd.read_excel(local_path)
                    
                    # Se añade una columna con la fecha y hora de la ingesta
                    pd_df['timestamp_ingesta'] = pd.to_datetime(time.time(), unit='s')
                    
                    spark_df = spark.createDataFrame(pd_df)

                    # Se añaden las columnas de partición
                    spark_df_partitioned = spark_df.withColumn("year", year(spark_df.timestamp_ingesta)) \
                                                   .withColumn("month", month(spark_df.timestamp_ingesta)) \
                                                   .withColumn("day", dayofmonth(spark_df.timestamp_ingesta)) \
                                                   .withColumn("hour", hour(spark_df.timestamp_ingesta))

                    # El nombre de la tabla donde se ingieren los datos crudos
                    table_name = "eventos_crudos_por_hora"
                    
                    # Se especifica que se particionará solo hasta la hora
                    partition_cols = ["year", "month", "day", "hour"]

                    # Se agrupa en un solo Parquet y se escribe en la partición correcta
                    spark_df_partitioned.coalesce(1).write \
                        .mode("append") \
                        .partitionBy(*partition_cols) \
                        .saveAsTable(table_name)
                    
                    print(f"Fichero {obj.object_name} procesado y guardado en la tabla {table_name}")

                    minio_client.remove_object(S3_BUCKET, obj.object_name)
                    
                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    print(f"Tiempo de procesamiento: {elapsed_time:.2f} segundos")

        except Exception as e:
            print(f"Error procesando ficheros: {e}")

        # Pausa antes de la siguiente iteración
        print("--- Esperando 1 segundo para el siguiente lote ---")
        time.sleep(1)

if __name__ == "__main__":
    main()
