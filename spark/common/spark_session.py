from __future__ import annotations

import os

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "customer360-lakehouse",
) -> SparkSession:
    """
    Create a reusable SparkSession configured for:

    - Spark standalone execution
    - Delta Lake
    - MinIO through the S3A connector
    - UTC timestamp handling
    """

    minio_endpoint = os.getenv(
        "MINIO_ENDPOINT",
        "http://minio:9000",
    )
    minio_access_key = os.getenv(
        "MINIO_ACCESS_KEY",
        "customer360_admin",
    )
    minio_secret_key = os.getenv(
        "MINIO_SECRET_KEY",
        "customer360_password",
    )

    spark = (
        SparkSession.builder
        .appName(app_name)

        # Delta Lake SQL support
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        # MinIO/S3A connection
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            minio_endpoint,
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            minio_access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            minio_secret_key,
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

        # Consistent timestamps and partition behaviour
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "8",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark