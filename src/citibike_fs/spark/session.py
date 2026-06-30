from __future__ import annotations

import os

import pyspark
from pyspark.sql import SparkSession

from citibike_fs.config import get_settings

# On EMR Serverless the connector is supplied with --packages at submit time.
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"


def build_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        # Microsecond timestamps are read natively by DuckDB and Athena alike.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    )
    if not get_settings().is_aws:
        builder = (
            builder.master(os.environ.get("SPARK_MASTER", "local[2]"))
            .config("spark.jars.packages", KAFKA_PACKAGE)
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
