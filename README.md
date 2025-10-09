<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Documentación del Proyecto NGODS-2025</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans", Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji";
            line-height: 1.6;
            color: #24292e;
            background-color: #ffffff;
            margin: 0;
            padding: 0;
        }
        .container {
            max-width: 900px;
            margin: 40px auto;
            padding: 20px;
            border: 1px solid #d1d5da;
            border-radius: 8px;
        }
        h1, h2, h3 {
            border-bottom: 1px solid #eaecef;
            padding-bottom: 0.3em;
            margin-top: 24px;
            margin-bottom: 16px;
            font-weight: 600;
        }
        h1 { font-size: 2em; }
        h2 { font-size: 1.5em; }
        h3 { font-size: 1.25em; }
        p { margin-bottom: 16px; }
        code {
            font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
            background-color: rgba(27, 31, 35, 0.05);
            padding: 0.2em 0.4em;
            margin: 0;
            font-size: 85%;
            border-radius: 3px;
        }
        pre {
            background-color: #f6f8fa;
            border: 1px solid #d1d5da;
            border-radius: 6px;
            padding: 16px;
            overflow: auto;
            line-height: 1.45;
        }
        pre code {
            background-color: transparent;
            padding: 0;
            margin: 0;
            font-size: 100%;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 16px;
        }
        th, td {
            border: 1px solid #d1d5da;
            padding: 8px 12px;
            text-align: left;
        }
        th {
            background-color: #f6f8fa;
            font-weight: 600;
        }
        tr:nth-child(even) {
            background-color: #f6f8fa;
        }
        .text-center {
            text-align: center;
        }
        .info-box {
            background-color: #f6f8fa;
            border: 1px solid #d1d5da;
            border-radius: 6px;
            padding: 16px;
            margin-bottom: 16px;
        }
        details {
            margin-bottom: 10px;
        }
        summary {
            font-weight: 600;
            cursor: pointer;
        }
    </style>
</head>
<body>

<div class="container">

    <div class="text-center">
      <h1>🚀 NGODS-2025: Plataforma de Data Lakehouse Local</h1>
    </div>

    <p class="text-center">
      Este proyecto despliega una completa plataforma de <em>data lakehouse</em> en tu máquina local utilizando Docker. Permite ingerir, almacenar, procesar y consultar datos de manera eficiente, simulando un entorno de Big Data profesional.
    </p>

    <hr>

    <h2>✨ Servicios Incluidos</h2>
    <p>La plataforma se compone de los siguientes servicios orquestados a través de <code>docker-compose</code>:</p>
    <table>
      <thead>
        <tr>
          <th>Servicio</th>
          <th>Descripción</th>
          <th class="text-center">Puerto (Local)</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>🚢 <strong>MinIO</strong></td>
          <td>Almacenamiento de objetos compatible con S3 (Data Lake).</td>
          <td class="text-center"><code>9000</code> (API), <code>9001</code> (Consola Web)</td>
        </tr>
        <tr>
          <td>🐝 <strong>Hive Metastore</strong></td>
          <td>Catálogo central de metadatos para las tablas.</td>
          <td class="text-center"><code>9083</code></td>
        </tr>
        <tr>
          <td>🐬 <strong>MariaDB</strong></td>
          <td>Base de datos que da soporte al Hive Metastore.</td>
          <td class="text-center"><code>3307</code></td>
        </tr>
        <tr>
          <td>✨ <strong>Spark Thrift</strong></td>
          <td>Servidor para ejecutar consultas SQL sobre Spark.</td>
          <td class="text-center"><code>10000</code> (JDBC), <code>4040</code> (UI)</td>
        </tr>
        <tr>
          <td>🚀 <strong>Trino</strong></td>
          <td>Motor de consultas SQL federado de alto rendimiento.</td>
          <td class="text-center"><code>8081</code> (UI & API)</td>
        </tr>
        <tr>
          <td>🔧 <strong>dbt-runner</strong></td>
          <td>Entorno para ejecutar transformaciones de datos con dbt.</td>
          <td class="text-center">-</td>
        </tr>
        <tr>
          <td>👷 <strong>ingest-worker</strong></td>
          <td>Servicio que ingiere automáticamente archivos de MinIO.</td>
          <td class="text-center">-</td>
        </tr>
      </tbody>
    </table>

    <hr>

    <h2>⚙️ Primeros Pasos</h2>
    <h3>Prerrequisitos</h3>
    <p>Asegúrate de tener instalado:</p>
    <ul>
      <li><strong>Docker</strong></li>
      <li><strong>Docker Compose</strong></li>
    </ul>

    <h3>1. Instalación</h3>
    <div class="info-box">
        <p>Primero, crea la red de Docker que usarán los contenedores y luego levanta todos los servicios en segundo plano.</p>
        <pre><code># 1. Crear la red Docker
docker network create ngodsnet

# 2. Iniciar todos los servicios
docker compose up -d</code></pre>
        <p>Puedes verificar que todos los contenedores se están ejecutando con <code>docker compose ps</code>.</p>
    </div>

    <h3>2. Generar y Subir Datos de Prueba</h3>
    <div class="info-box">
        <p>El proyecto incluye un script para generar archivos Excel de prueba y subirlos directamente a MinIO para que el <code>ingest-worker</code> los procese.</p>
        <details>
            <summary><strong>Para Windows (PowerShell)</strong></summary>
            <pre><code># Activa tu entorno virtual (si usas uno)
.venv\Scripts\Activate.ps1

# Configura las variables de entorno para conectar con MinIO
$env:MINIO_ENDPOINT="http://localhost:9000"
$env:MINIO_ACCESS_KEY="minio"
$env:MINIO_SECRET_KEY="MinioPass_2025!"
$env:S3_BUCKET="ngods"
$env:S3_PREFIX="ingest" # Carpeta que el ingest-worker está vigilando

# Ejecuta el script para generar 100 archivos con 50 filas cada uno
python generador-datos/generate_and_upload_excel.py --num-files 100 --rows 50</code></pre>
        </details>
        <details>
            <summary><strong>Para Linux/macOS</strong></summary>
            <pre><code># Configura las variables de entorno
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="minio"
export MINIO_SECRET_KEY="MinioPass_2025!"
export S3_BUCKET="ngods"
export S3_PREFIX="ingest"

# Ejecuta el script
python generador-datos/generate_and_upload_excel.py --num-files 100 --rows 50</code></pre>
        </details>
        <p>Una vez que los archivos se suban a la carpeta <code>ingest/</code> en MinIO, el servicio <code>ingest-worker</code> los detectará automáticamente y comenzará el proceso de ingesta hacia la capa Bronze del Data Lake.</p>
    </div>
    
    <hr>

    <h2>🏛️ Visión General de la Arquitectura</h2>
    <p>Este sistema implementa una arquitectura de <em>data lakehouse</em> local utilizando Docker. Está diseñado para ingerir archivos Excel, procesarlos y almacenarlos en un formato optimizado para consultas analíticas.</p>

    <h3>Esquema de Componentes</h3>
    <p>A continuación se detalla la función de cada contenedor:</p>
    
    <ul>
        <li><strong><code>minio</code> 🗄️</strong>: Es el corazón del data lake. Almacena todos los datos en <em>buckets</em>, simulando el comportamiento de Amazon S3.</li>
        <li><strong><code>mariadb</code> 🐬</strong>: Base de datos SQL que almacena todos los metadatos del Hive Metastore.</li>
        <li><strong><code>metastore</code> 🐝</strong>: Servicio central que gestiona el catálogo de los datos, permitiendo a Spark y Trino saber qué tablas existen.</li>
        <li><strong><code>ingest-worker</code> 👷‍♂️</strong>: Vigila la carpeta <code>ingest/</code> en MinIO y procesa los nuevos archivos Excel a formato Parquet en la capa <strong>Bronze</strong>.</li>
        <li><strong><code>spark-thrift</code> ✨</strong>: Expone la funcionalidad de Spark SQL a través de una interfaz JDBC.</li>
        <li><strong><code>trino</code> 🚀</strong>: Motor de consulta SQL optimizado para analíticas rápidas sobre grandes volúmenes de datos.</li>
        <li><strong><code>dbt-runner</code> 🔧</strong>: Contenedor con `dbt` para ejecutar transformaciones de datos.</li>
    </ul>

    <hr>
    
    <h2>🗺️ Mapa de Dependencias de Contenedores</h2>
    <p>El orden de arranque y las dependencias están diseñados para asegurar que los servicios base estén listos antes de que los servicios que los consumen se inicien.</p>

    <div class="text-center">
    <pre><code>
                 ┌───────────┐      ┌─────────┐
                 │  mariadb  │      │  minio  │
                 └─────┬─────┘      └────┬────┘
                       │                  │
                       ▼                  ▼
                 ┌───────────┐      ┌─────────────┐
                 │ metastore │      │ minio-setup │
                 └─────┬─────┘      └──────┬──────┘
         ┌─────────────┼──────────────────┼──────────────┐
         │             │                  │              │
         ▼             ▼                  ▼              ▼
┌────────────────┐ ┌───────┐      ┌───────────────┐
│ spark-thrift   │ │ trino │      │ ingest-worker │
└────────┬───────┘ └────┬──┘      └───────────────┘
         │              │
         └───────┬──────┘
                 │
                 ▼
         ┌────────────┐
         │ dbt-runner │
         └────────────┘
    </code></pre>
    </div>
    
    <p><strong>En resumen:</strong></p>
    <ul>
        <li><strong><code>mariadb</code></strong> y <strong><code>minio</code></strong> son la base.</li>
        <li><strong><code>metastore</code></strong> necesita a <code>mariadb</code> para guardar los metadatos.</li>
        <li><strong><code>minio-setup</code></strong> configura a <code>minio</code>.</li>
        <li><strong><code>spark-thrift</code></strong>, <strong><code>trino</code></strong> e <strong><code>ingest-worker</code></strong> necesitan que <code>metastore</code> y <code>minio</code> estén listos.</li>
        <li><strong><code>dbt-runner</code></strong> se conecta a <code>spark-thrift</code> y <code>trino</code> para ejecutar transformaciones.</li>
    </ul>

    <hr>
    
    <h2>🌊 Flujo de Datos: De la Subida a Silver</h2>
    <p>El siguiente diagrama ilustra el viaje de un archivo desde que se sube hasta que está listo para el análisis.</p>
    <div class="text-center"><strong><code>Subida -> (ingest-worker) -> BRONZE -> (compaction_job) -> SILVER</code></strong></div>
    <ol>
        <li><strong>Paso 1: Subida del Archivo</strong><br>
            Un usuario sube un archivo Excel a la carpeta <code>ingest/</code> del bucket <code>ngods</code> en MinIO.
        </li>
        <li><strong>Paso 2: Ingesta (Capa Bronze)</strong><br>
            El servicio <strong><code>ingest-worker</code></strong> detecta el archivo, añade un timestamp de ingesta, y lo guarda en formato Parquet en la tabla <code>eventos_crudos_por_hora</code>, particionando los datos por hora.
        </li>
        <li><strong>Paso 3: Refinado y Compactación (Capa Silver)</strong><br>
            Un job de Spark (<code>compaction_job_by_hour.py</code>) lee los datos de la capa Bronze, añade una partición más granular por minuto, y los guarda en una nueva tabla <code>eventos_refinados_por_minuto</code>, optimizada para consultas.
        </li>
    </ol>
    
    <hr>
    
    <h2>🔌 Configuración y Endpoints</h2>
    <p>Resumen de los puntos de conexión y variables clave:</p>
    <table>
        <thead>
            <tr>
                <th>Servicio</th>
                <th>Endpoint</th>
                <th>Usuario</th>
                <th>Contraseña</th>
                <th>Notas</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><strong>MinIO (S3)</strong></td>
                <td><code>http://localhost:9000</code></td>
                <td><code>minio</code></td>
                <td><code>MinioPass_2025!</code></td>
                <td>Consola web en <code>http://localhost:9001</code></td>
            </tr>
            <tr>
                <td><strong>MariaDB</strong></td>
                <td><code>localhost:3307</code></td>
                <td><code>root</code></td>
                <td><code>admin123</code></td>
                <td>Para el metastore: <code>hive</code>/<code>hivepass</code></td>
            </tr>
            <tr>
                <td><strong>Trino</strong></td>
                <td><code>http://localhost:8081</code></td>
                <td><code>dbt</code></td>
                <td>(ninguna)</td>
                <td>Conexión JDBC/BI</td>
            </tr>
            <tr>
                <td><strong>Spark Thrift</strong></td>
                <td><code>localhost:10000</code></td>
                <td><code>dbt</code></td>
                <td>(ninguna)</td>
                <td>Conexión JDBC para Spark SQL</td>
            </tr>
            <tr>
                <td><strong>Hive Metastore</strong></td>
                <td><code>thrift://localhost:9083</code></td>
                <td>-</td>
                <td>-</td>
                <td>Usado internamente por Spark y Trino</td>
            </tr>
        </tbody>
    </table>
</div>

</body>
</html>
