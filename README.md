<h1>NGODS-2025</h1>

<div style="background-color: #f0f0f0; padding: 10px; border-radius: 5px;">
  <h2>Instalación</h2>
  <p>docker network create ngodsnet</p>
  <p>docker compose up -d</p>
</div>

<div style="background-color: #f0f0f0; padding: 10px; border-radius: 5px;">
  <h2>Subir archivos de prueba a MinIO</h2>
  <p>.venv\Scripts\Activate.ps1</p>
  <p>
    $env:MINIO_ENDPOINT="http://localhost:9000"<br>
    $env:MINIO_ACCESS_KEY="minio"<br>
    $env:MINIO_SECRET_KEY="MinioPass_2025!"<br>
    $env:S3_BUCKET="ngods"<br>
    $env:S3_PREFIX="ingest"
  </p>
  <p>python generate_and_upload_excel.py --num-files 100 --rows 50</p>
</div>




 




