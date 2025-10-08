# Versión "bullseye", de las más estables
FROM python:3.9-slim-bullseye

# Instalar la versión de Java correcta (OpenJDK 17) y procps
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Asegurarse de que JAVA_HOME apunta a la versión 17
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest_worker.py .

CMD ["python", "-u", "ingest_worker.py"]