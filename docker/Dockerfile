# One image for every Python component: Spark jobs, ingestion, API and dashboard.
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless libgomp1 procps curl \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    FEAST_USAGE=False

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Resolve the Spark Kafka connector at build time so jobs start without a download.
RUN python -c "import pyspark; from pyspark.sql import SparkSession; \
SparkSession.builder.master('local[1]').config('spark.jars.packages', \
f'org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}').getOrCreate().stop()"

COPY pyproject.toml .
COPY src ./src
COPY feature_repo ./feature_repo
RUN pip install --no-cache-dir --no-deps -e .
