import os
import time
from minio import Minio
from pyspark.sql import SparkSession
import pandas as pd

# Configuración de MinIO
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")
S3_BUCKET = os.environ.get("S3_BUCKET")

# Configuración de Spark
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
            print("Leyendo ficheros de MinIO...")
            objects = minio_client.list_objects(S3_BUCKET, prefix="ingest/", recursive=True)
            for obj in objects:
                if obj.object_name.endswith(('.xlsx', '.xls')):
                    
                    # --- INICIO DEL CONTADOR ---
                    start_time = time.time()
                    
                    print(f"Procesando fichero: {obj.object_name}")

                    # 1. Descargar el fichero a una ruta temporal
                    local_path = f"/tmp/{os.path.basename(obj.object_name)}"
                    minio_client.fget_object(S3_BUCKET, obj.object_name, local_path)

                    # 2. Leer el fichero Excel con pandas y convertir a DataFrame de Spark
                    pd_df = pd.read_excel(local_path)
                    spark_df = spark.createDataFrame(pd_df)

                    # 3. Guardar el DataFrame como tabla en Hive
                    table_name = os.path.splitext(os.path.basename(obj.object_name))[0]
                    table_name = "".join(filter(str.isalnum, table_name)).lower()
                    spark_df.write.mode("append").saveAsTable(table_name)      
                    print(f"Fichero {obj.object_name} procesado y guardado como tabla {table_name}")
                    
                    # 4. Borrar el fichero procesado de MinIO
                    minio_client.remove_object(S3_BUCKET, obj.object_name)

                    # --- FIN DEL CONTADOR ---
                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    print(f"----- Tiempo de procesamiento: {elapsed_time:.2f} segundos -----")

        except Exception as e:
            print(f"Error procesando ficheros: {e}")

        print("Ejecutando de nuevo en 1 segundo...")
        time.sleep(1)

if __name__ == "__main__":
    main()