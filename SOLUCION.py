from pyspark.sql.types import IntegerType, DoubleType, BooleanType, StringType, TimestampType
from pyspark.sql.types import StructType, StructField
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
# ### CAMBIO: Se añaden las funciones de Spark SQL necesarias para el particionamiento
from pyspark.sql.functions import year, month, dayofmonth, col, lit, to_timestamp, current_timestamp
from dagster import job, op, ScheduleDefinition, repository
from dagster import sensor, RunRequest, SkipReason, DefaultSensorStatus
from dagster import get_dagster_logger
import boto3
import socket
import os
from datetime import date
import pandas as pd
import re
import time


# Get Ip address
hostname = socket.gethostname()
IPAddr = socket.gethostbyname(hostname)

database = 'uploads'
bucket_name = 'bbtwins'

logger = get_dagster_logger()

# Sin cambios en los esquemas
dic_schemas = {
    'gemelo_matanza.csv': StructType([
        StructField("LoteRecepID", IntegerType(), True),
        StructField("CarcassID", IntegerType(), True),
        StructField("Number", IntegerType(), True),
        StructField("Weight", DoubleType(), True),
        StructField("Fat", IntegerType(), True),
        StructField("NumRejectsHams", IntegerType(), True),
        StructField("NumRejectsShoulders", IntegerType(), True),
        StructField("CarcassType", IntegerType(), True),
        StructField("DescriptionCarcassType", StringType(), True),
        StructField("RejectsHamType", IntegerType(), True),
        StructField("RejectsHam", StringType(), True),
        StructField("RejectsShoulderType", IntegerType(), True),
        StructField("RejectsShoulder", StringType(), True),
        StructField("ConfiscationType", IntegerType(), True),
        StructField("ConfiscationDescription", StringType(), True),
        StructField("TypeClasifID", StringType(), True),
        StructField("DescripClasif", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("AptaIGP", IntegerType(), True),
        StructField("SUBTIPO_LOTE", StringType(), True),
        StructField("FechaRecep", TimestampType(), True),
        StructField("Fecha_subida", TimestampType(), True),
        StructField("IdRail", IntegerType(), True)
    ]),
    'gemelo_pedidos.csv': StructType([
        StructField("date", TimestampType(), True),
        StructField("descrip", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("erp_code", StringType(), True),
        StructField("product", StringType(), True),
        StructField("kilos_order", StringType(), True),
        StructField("pieces_ordered", DoubleType(), True),
        StructField("boxes_ordered", IntegerType(), True),
        StructField("kilos_served", DoubleType(), True),
        StructField("pieces_served", DoubleType(), True),
        StructField("boxes_served", IntegerType(), True),
        StructField("kilos_dif", DoubleType(), True),
        StructField("diff_pieces", DoubleType(), True),
        StructField("diff_boxes", IntegerType(), True),
        StructField("fecha_entrega", StringType(), True),
        StructField("fecha_subida", TimestampType(), True)
    ]),
    'gemelo_partidas.csv': StructType([
        StructField("loterecepid", IntegerType(), True),
        StructField("codigo", StringType(), True),
        StructField("fecharecep", TimestampType(), True),
        StructField("descripgranja", StringType(), True),
        StructField("granjaorigen", StringType(), True),
        StructField("codigogranja", StringType(), True),
        StructField("codigodo", StringType(), True),
        StructField("animalesvivos", IntegerType(), True),
        StructField("animalesmuertos", IntegerType(), True),
        StructField("tipopartida", StringType(), True),
        StructField("numguiasanitaria", StringType(), True),
        StructField("pesototal", DoubleType(), True),
        StructField("numintegracion", IntegerType(), True),
        StructField("descripintegracion", StringType(), True),
        StructField("fechasacrif", TimestampType(), True),
        StructField("descripproveedor", StringType(), True),
        StructField("descrippropietario", StringType(), True),
        StructField("descripmatadero", StringType(), True),
        StructField("numpartidafact", IntegerType(), True),
        StructField("genetica", StringType(), True),
        StructField("numalbaran", StringType(), True),
        StructField("lotesalida", IntegerType(), True),
        StructField("fecha_subida", TimestampType(), True)
    ]),
    'in_propuesta_estimacion_semanal_weekly_plan.csv': StructType([
        StructField("slaughter", StringType(), True),
        StructField("cutting", StringType(), True),
        StructField("supplier", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("do", IntegerType(), True),
        StructField("quantity", IntegerType(), True)
    ]),
}

# ### CAMBIO: Se crea un diccionario para mapear qué columna de fecha usar para particionar cada tabla.
# Esto hace el código más limpio y mantenible.
# Para tablas sin fecha, usaremos 'fecha_proceso', que se añadirá con la fecha actual.
date_column_mapping = {
    'gemelo_matanza': 'FechaRecep',
    'gemelo_pedidos': 'date',
    'gemelo_partidas': 'fecharecep',
    'in_propuesta_estimacion_semanal_weekly_plan': 'fecha_proceso' # Tabla sin fecha, usará la fecha de ingesta
}

def spark_setup():
    # Load spark session
    spark = SparkSession.builder.master(f"spark://{IPAddr}:7077").getOrCreate()
    return spark


# Load s3 client
s3 = boto3.client('s3', region_name=os.getenv("AWS_REGION"))


# La función add_table_columns se mantiene por si se necesita para evolución de esquema,
# pero ya no es parte crítica del flujo de escritura.
def add_table_columns(spark, df, table_name):
    for col in df.columns:
        try:
            if col not in spark.table(table_name).columns:
                data_type = [item for item in df.dtypes if item[0]==col][0][1]
                query = f"ALTER TABLE {table_name} ADD COLUMNS (`{col}` {data_type})"
                spark.sql(query)
        except AnalysisException:
            pass


# ### CAMBIO: La función `write_data` se ha simplificado enormemente.
# Ahora solo escribe, sin leer ni comparar. Es mucho más rápida y escalable.
def write_data(name, warehouse, df, spark):
    logger.info(f"Iniciando escritura para la tabla: {name}")
    table_name = f"{warehouse}.{database}.`{name}`"

    # Se crea la base de datos si no existe
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {warehouse}.{database}")
    
    # Define las columnas por las que siempre se va a particionar
    partition_cols = ["year", "month", "day"]

    try:
        # La operación de escritura ahora es un simple 'append' particionado.
        # Es la forma más eficiente de añadir datos nuevos a un data lake.
        df.write \
          .mode("append") \
          .partitionBy(partition_cols) \
          .saveAsTable(table_name)
        
        logger.info(f"Escritura completada exitosamente para {table_name}")

    except Exception as e:
        logger.error(f"Error durante la escritura para la tabla {table_name}: {e}")
        # Opcional: añadir lógica para manejar el error, como mover el archivo a una carpeta de cuarentena.


def create_table(warehouse,spark):
    # Sin cambios
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {warehouse}.{database}.raw_data (
    path string,
    filename string,
    extension string,
    date date);
    """)


def insert_to_table(warehouse,spark,path,file_name,extension=""):
    # Sin cambios
    create_table(warehouse,spark)
    now = date.today().strftime('%Y-%m-%d')
    spark.sql(f"""
    INSERT INTO {warehouse}.{database}.raw_data
    VALUES ('{path}', '{file_name}', '{extension}', date '{now}')
    """)


def convert_excel_to_csv(bucket_name, key, downloaded_file,spark,warehouse):
    # Sin cambios funcionales, se mantiene la lógica de conversión.
    s3.download_file(bucket_name, key, downloaded_file)
    excel_file = pd.ExcelFile(downloaded_file)
    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name, keep_default_na=False, converters={'producer_id': lambda x: f'{x}', 'variety_id': lambda x: f'{x}'})
        if not df.columns.empty:
            for col in df.columns:
                df.loc[df[col]=="NA", col] = ""
                if col == "Date" or col == "datetime":
                    df[col] = pd.to_datetime(df[col], infer_datetime_format=True, dayfirst=True)
            try:
                df = df.drop(columns=["Unnamed: 3"])
            except KeyError:
                pass
            try:
                df = df.drop(columns=["Unnamed: 5"])
            except KeyError:
                pass

            sheet_name = re.sub(r'[-—–\s]', '_', sheet_name.lower())
        if 'gemelo_pedidos' in downloaded_file:
            csv_file_name = re.sub(r'[-—–\s]', '_', downloaded_file.split(".")[0].lower()) + ".csv"
        else:
            csv_file_name = re.sub(r'[-—–\s]', '_', downloaded_file.split(".")[0].lower()) + "_" + sheet_name + ".csv"
        df.to_csv(csv_file_name, index=False, encoding="utf-8", escapechar='\\')
        print("Create file   --> ",  csv_file_name)
        s3.upload_file(csv_file_name, bucket_name, key.rsplit('/', 1)[0] + "/" + csv_file_name.split("/")[-1])
        os.remove(csv_file_name)
    os.remove(downloaded_file)
    # ### CAMBIO: Se elimina la llamada recursiva a process_data().
    # La lógica ahora es lineal: el op principal convierte y luego procesa.
    # Esta línea se elimina para evitar comportamiento impredecible.
    # s3.delete_object(Bucket=bucket_name, Key=key) # Opcional: borrar el excel original
    # moved_file = move_file_to_raw_data(spark,warehouse,key) # Mover el excel original a procesados.


@op
def process_data():
    spark = spark_setup()
    data_path = 'Upload_Data'
    result = s3.list_objects(Bucket=bucket_name, Prefix=data_path)
    
    if "Contents" not in result:
        logger.info("No se encontraron archivos en la carpeta de subida.")
        return
        
    files_list = result.get("Contents", []).copy()

    for content in files_list:
        key = content.get("Key")
        if key.endswith("/"):
            continue

        logger.info(f"Procesando archivo: {key}")
        file = key.split("/")[-1]
        warehouse = key.split("/")[1].lower()
        downloaded_file = f"/var/lib/ngods/dagster/{file}"
        downloaded_file_ext = downloaded_file.split(".")[-1].lower()

        # Lógica de conversión de Excel a CSV
        if downloaded_file_ext in ["xls", "xlsx"]:
            logger.info(f"Convirtiendo archivo Excel a CSV: {key}")
            convert_excel_to_csv(bucket_name, key, downloaded_file, spark, warehouse)
            # Después de convertir, el sensor se reactivará y procesará los CSV generados.
            # Opcional: se podría borrar el Excel aquí. s3.delete_object(Bucket=bucket_name, Key=key)
            continue # Pasa al siguiente archivo, el CSV será procesado en la siguiente ejecución del sensor.
        
        # Lógica de procesamiento de CSV
        elif downloaded_file_ext == "csv":
            try:
                s3.download_file(bucket_name, key, downloaded_file)
                
                file_name_no_ext = file.rsplit(".", 1)[0]
                schema = dic_schemas.get(file)

                if schema:
                    logger.info(f"Usando esquema predefinido para {file}")
                    df = spark.read.options(header=True, encoding='utf-8').schema(schema).csv(downloaded_file)
                else:
                    logger.info(f"Infiriendo esquema para {file}")
                    df = spark.read.options(inferSchema=True, header=True, encoding='utf-8').csv(downloaded_file)

                if df.count() == 0:
                    logger.warning(f"El archivo {key} está vacío. Saltando...")
                    move_file_to_raw_data(spark, warehouse, key) # Mover archivo vacío
                    os.remove(downloaded_file)
                    continue
                
                # ### CAMBIO: Se añade la lógica de particionamiento
                date_col = date_column_mapping.get(file_name_no_ext)
                if not date_col:
                    logger.warning(f"No hay una columna de fecha definida para {file_name_no_ext}. Usando fecha de proceso.")
                    date_col = 'fecha_proceso'
                
                if date_col not in df.columns:
                     df = df.withColumn(date_col, current_timestamp())

                # (ahora) year/month/day desde el momento de escritura (ingesta)
                from pyspark.sql.functions import (
                    year, month, dayofmonth, hour, minute,
                    current_timestamp, to_timestamp, col
                )

                ing_ts = current_timestamp()
                df_with_partitions = df.withColumn("year",  year(ing_ts)) \
                       .withColumn("month", month(ing_ts)) \
                       .withColumn("day",  dayofmonth(ing_ts))

                # Llamar a la función de escritura simplificada
                write_data(file_name_no_ext, warehouse, df_with_partitions, spark)

                # Mover el archivo procesado
                move_file_to_raw_data(spark, warehouse, key)
                os.remove(downloaded_file)

            except Exception as e:
                logger.error(f"Error procesando el archivo CSV {key}: {e}")
                # Opcional: mover archivo con error a una carpeta de cuarentena
                os.remove(downloaded_file)


def move_file_to_raw_data(spark, warehouse,key):
    # Sin cambios
    print("Moviendo archivo ------> ", key)
    new_path = 'Data/'
    now = date.today()
    file = key.split("/")[-1]
    save_path = f"{new_path}{warehouse}Files/{now.year}/{now.month}/{now.day}/{file}"
    file_name = key.split("/")[-1].split(".")[0]
    file_extension = file.split(".")[-1]
    s3.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": key}, Key=save_path)
    s3.delete_object(Bucket=bucket_name, Key=key)
    insert_to_table(warehouse,spark,f"{bucket_name}/{save_path}",file_name,file_extension)


@job
def load_data_to_spark():
    process_data()


# do_it_all_schedule = ScheduleDefinition(job=load_data_to_spark, cron_schedule="0 */6 * * *") #execute task every 6 hours

@sensor(job=load_data_to_spark, minimum_interval_seconds=1800, default_status=DefaultSensorStatus.RUNNING,)
def s3_sensor(context):
    # Sin cambios
    result = s3.list_objects(Bucket=bucket_name, Prefix='Upload_Data')
    
    if "Contents" not in result:
        yield SkipReason("No files found in Upload_Data folder.")
        return

    run_key = f"files-{int(time.time())}"
    yield RunRequest(run_key=run_key)


@repository
def workspace():
    return [
        load_data_to_spark,
        s3_sensor
    ]