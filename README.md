<h1>🚀 NGODS-2025: Plataforma de Data Lakehouse Local</h1>
Este proyecto despliega una completa plataforma de data lakehouse en tu máquina local utilizando Docker. Permite ingerir, almacenar, procesar y consultar datos de manera eficiente, simulando un entorno de Big Data profesional.

✨ Servicios Incluidos
La plataforma se compone de los siguientes servicios orquestados a través de docker-compose:

## 🧰 Servicios del Data Lake

| Servicio             | Descripción                                                      | Puerto (Local)               |
|-----------------------|-------------------------------------------------------------------|-------------------------------|
| 🚢 **MinIO**           | Almacenamiento de objetos compatible con S3 (Data Lake).          | `9000` (API), `9001` (Consola Web) |
| 🐝 **Hive Metastore**  | Catálogo central de metadatos para las tablas.                    | `9083`                        |
| 🐬 **MariaDB**         | Base de datos que da soporte al Hive Metastore.                   | `3307`                        |
| ✨ **Spark Thrift**    | Servidor para ejecutar consultas SQL sobre Spark.                 | `10000` (JDBC), `4040` (UI)   |
| 🚀 **Trino**           | Motor de consultas SQL federado de alto rendimiento.              | `8081` (UI & API)             |
| 🔧 **dbt-runner**      | Entorno para ejecutar transformaciones de datos con dbt.          | -                             |
| 👷 **ingest-worker**   | Servicio que ingiere automáticamente archivos de MinIO.          | -                             |


<h2>1. Instalación</h2>
1. Crear la red Docker
```powershell
docker network create ngodsnet
```

2. Iniciar todos los servicios
```powershell
docker compose up -d
```

<h2>2. Generar y Subir Datos de Prueba</h2>
El proyecto incluye un script para generar archivos Excel de prueba y subirlos directamente a MinIO para que el ingest-worker los procese.

1. Activa el entorno virtual
```powershell
generador-datos\.venv\Scripts\Activate.ps1
```

2. Configura las variables de entorno para conectar con MinIO
```powershell
$env:MINIO_ENDPOINT="http://localhost:9000"
$env:MINIO_ACCESS_KEY="minio"
$env:MINIO_SECRET_KEY="MinioPass_2025!"
$env:S3_BUCKET="ngods"
$env:S3_PREFIX="ingest" # Carpeta que el ingest-worker está vigilando
```

3. Ejecuta el script para generar 100 archivos con 50 filas cada uno
```powershell
python generador-datos/generate_and_upload_excel.py --num-files 100 --rows 50
```

Una vez que los archivos se suban a la carpeta ingest/ en MinIO, el servicio ingest-worker los detectará automáticamente y comenzará el proceso de ingesta hacia la capa Bronze del Data Lake.


<h2>3. Flujo de Datos: De la Subida a Silver</h2>
El viaje de un archivo desde que se sube hasta que está listo para el análisis es el siguiente:

Subida -> (ingest-worker) -> BRONZE -> (compaction_job) -> SILVER

Paso 1: Subida del Archivo

Un archivo Excel (ej: datos.xlsx) se sube a la carpeta ingest/ del bucket ngods en MinIO.

Paso 2: Ingesta (Capa Bronze)

El servicio ingest-worker detecta el archivo, añade un timestamp de ingesta y lo guarda en formato Parquet en la tabla eventos_crudos_por_hora, particionando los datos por hora.

Paso 3: Refinado y Compactación (Capa Silver)

Un job de Spark (compaction_job_by_hour.py) lee los datos de la capa Bronze, añade una partición más granular por minuto, y los guarda en una nueva tabla, optimizada para consultas analíticas rápidas.






