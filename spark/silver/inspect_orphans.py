from __future__ import annotations

import os

from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required."
        )

    return (
        SparkSession.builder
        .appName("customer360-orphan-inspection")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
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
        customers = (
            spark.read.format("delta")
            .load(f"s3a://{bucket}/silver/customers")
        )

        orders = (
            spark.read.format("delta")
            .load(f"s3a://{bucket}/silver/orders")
        )

        order_items = (
            spark.read.format("delta")
            .load(f"s3a://{bucket}/silver/order_items")
        )

        payments = (
            spark.read.format("delta")
            .load(f"s3a://{bucket}/silver/payments")
        )

        marketing_events = (
            spark.read.format("delta")
            .load(f"s3a://{bucket}/silver/marketing_events")
        )

        orphan_order_items = order_items.join(
            orders.select("order_id").dropDuplicates(),
            on="order_id",
            how="left_anti",
        )

        orphan_payments = payments.join(
            orders.select("order_id").dropDuplicates(),
            on="order_id",
            how="left_anti",
        )

        orphan_marketing = marketing_events.join(
            customers.selectExpr(
                "email AS customer_email"
            ).dropDuplicates(),
            on="customer_email",
            how="left_anti",
        )

        print()
        print("=" * 90)
        print("ORPHAN ORDER ITEMS")
        print("=" * 90)
        print(f"Count: {orphan_order_items.count():,}")
        orphan_order_items.select(
            "order_item_id",
            "order_id",
            "product_id",
        ).show(20, truncate=False)

        print()
        print("=" * 90)
        print("ORPHAN PAYMENTS")
        print("=" * 90)
        print(f"Count: {orphan_payments.count():,}")
        orphan_payments.select(
            "payment_id",
            "order_id",
            "customer_id",
        ).show(20, truncate=False)

        print()
        print("=" * 90)
        print("ORPHAN MARKETING EVENTS")
        print("=" * 90)
        print(f"Count: {orphan_marketing.count():,}")
        orphan_marketing.select(
            "event_id",
            "customer_email",
            "campaign_id",
            "event_type",
        ).show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()