# Reproducible Spark + Delta dev environment.
# Python 3.11 and Java 17 are the versions Spark 3.5 is tested against.
# Pinned to bookworm (Debian 12): trixie dropped the openjdk-17 package.
FROM python:3.11-slim-bookworm

# Spark needs a JRE. eclipse-temurin's JDK 17 works; the slim image has apt.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

# jre-headless registers `java` on PATH via update-alternatives, so we let
# PySpark discover it there rather than hardcoding an arch-specific JAVA_HOME.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default to a no-op; docker-compose / make supplies the real command.
CMD ["python", "-c", "import pyspark, delta; print('Spark', pyspark.__version__, 'ready')"]
