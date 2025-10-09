NGODS-2025: Plataforma de Data Lakehouse Local 🚀
Este proyecto despliega una completa plataforma de data lakehouse en tu máquina local utilizando Docker. Permite ingerir, almacenar, procesar y consultar datos de manera eficiente, simulando un entorno de Big Data profesional.

✨ Servicios Incluidos
La plataforma se compone de los siguientes servicios orquestados a través de docker-compose:

Servicio	Descripción	Puerto (Local)
🚢 MinIO	Almacenamiento de objetos compatible con S3 (Data Lake).	9000 (API), 9001 (Consola Web)
🐝 Hive Metastore	Catálogo central de metadatos para las tablas.	9083
🐬 MariaDB	Base de datos que da soporte al Hive Metastore.	3307
✨ Spark Thrift	Servidor para ejecutar consultas SQL sobre Spark.	10000 (JDBC), 4040 (UI)
🚀 Trino	Motor de consultas SQL federado de alto rendimiento.	8081 (UI & API)
🔧 dbt-runner	Entorno para ejecutar transformaciones de datos con dbt.	-
👷 ingest-worker	Servicio que ingiere automáticamente archivos de MinIO.	-

Exportar a Hojas de cálculo
⚙️ Primeros Pasos
Prerrequisitos
Asegúrate de tener instalado:

Docker

Docker Compose

1. Instalación
Primero, crea la red de Docker que usarán los contenedores y luego levanta todos los servicios en segundo plano.

Bash

# 1. Crear la red Docker
docker network create ngodsnet

# 2. Iniciar todos los servicios
docker compose up -d
Puedes verificar que todos los contenedores se están ejecutando con docker compose ps.

2. Generar y Subir Datos de Prueba
El proyecto incluye un script para generar archivos Excel de prueba y subirlos directamente a MinIO para que el ingest-worker los procese.

Para Windows (PowerShell):

PowerShell

# Activa tu entorno virtual (si usas uno)
.venv\Scripts\Activate.ps1

# Configura las variables de entorno para conectar con MinIO
$env:MINIO_ENDPOINT="http://localhost:9000"
$env:MINIO_ACCESS_KEY="minio"
$env:MINIO_SECRET_KEY="MinioPass_2025!"
$env:S3_BUCKET="ngods"
$env:S3_PREFIX="ingest" # Carpeta que el ingest-worker está vigilando

# Ejecuta el script para generar 100 archivos con 50 filas cada uno
python generador-datos/generate_and_upload_excel.py --num-files 100 --rows 50
Para Linux/macOS:

Bash

# Configura las variables de entorno
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="minio"
export MINIO_SECRET_KEY="MinioPass_2025!"
export S3_BUCKET="ngods"
export S3_PREFIX="ingest"

# Ejecuta el script
python generador-datos/generate_and_upload_excel.py --num-files 100 --rows 50
Una vez que los archivos se suban a la carpeta ingest/ en MinIO, el servicio ingest-worker los detectará automáticamente y comenzará el proceso de ingesta hacia la capa Bronze del Data Lake.
