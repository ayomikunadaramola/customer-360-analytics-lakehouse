from __future__ import annotations

import os

from pyspark.sql import SparkSession


def create_spark():

    return (
        SparkSession.builder
        .appName("verify-gold")

        # Delta Lake
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )

        # MinIO
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("MINIO_ENDPOINT"),
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("MINIO_ACCESS_KEY"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("MINIO_SECRET_KEY"),
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .getOrCreate()
    )


spark = create_spark()

bucket = "customer360"

tables = [
    "customer_360",
    "sales_summary",
    "product_performance",
    "marketing_summary",
    "support_summary",
    "web_engagement_summary"
]

print()

print("=" * 80)
print("VERIFYING GOLD TABLES")
print("=" * 80)

for table in tables:
    path = f"s3a://{bucket}/gold/{table}"

    df = (
        spark.read
        .format("delta")
        .load(path)
    )

    print()
    print(table)
    print("-" * 60)
    print("Rows:", df.count())
    df.printSchema()

spark.stop()