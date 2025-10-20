#!/bin/sh
set -eu

MINIO_URL="http://minio:9000"

echo "Esperando a que MinIO esté listo en ${MINIO_URL} ..."
i=0
until mc alias set local "${MINIO_URL}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
  i=$((i+1))
  if [ "$i" -gt 90 ]; then
    echo "ERROR: MinIO no responde tras 90 intentos (~180s)."
    exit 1
  fi
  sleep 2
done

echo "MinIO OK. Asegurando bucket ${S3_BUCKET} ..."
mc mb "local/${S3_BUCKET}" --ignore-existing >/dev/null 2>&1 || true

# mc anonymous set download "local/${S3_BUCKET}" >/dev/null 2>&1 || true

echo "MinIO init OK"
