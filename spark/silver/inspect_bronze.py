from __future__ import annotations

import os

from pyspark.sql import SparkSession


DATASETS = [
    "crm_customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "marketing_events",
    "support_tickets",
    "web_events",
]


def create_spark_session() -> SparkSession:
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required."
        )

    return (
        SparkSession.builder
        .appName("customer360-inspect-bronze")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main() -> None:
    bucket = os.getenv("MINIO_BUCKET", "customer360")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        for dataset in DATASETS:
            path = f"s3a://{bucket}/bronze/{dataset}"
            df = spark.read.format("delta").load(path)

            print()
            print("=" * 100)
            print(f"DATASET: {dataset}")
            print(f"ROWS: {df.count():,}")
            print("=" * 100)

            df.printSchema()
            df.show(3, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()