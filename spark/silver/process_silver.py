from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from datasets import DATASETS, get_dataset_config
from transforms import (
    apply_common_transformations,
    apply_dataset_transformations,
    deduplicate_records,
)
from validators import (
    assert_columns_exist,
    split_valid_and_invalid,
    validate_non_negative_columns,
)


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
        .appName("customer360-silver-processing")
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
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic",
        )
        .getOrCreate()
    )


def apply_numeric_validation(
    df: DataFrame,
    dataset_name: str,
) -> DataFrame:
    rules = {
        "products": [
            "price",
            "unit_price",
            "cost",
        ],
        "orders": [
            "order_total",
            "total_amount",
            "discount_amount",
            "shipping_amount",
        ],
        "order_items": [
            "quantity",
            "unit_price",
            "line_amount",
        ],
        "payments": [
            "amount",
        ],
    }

    return validate_non_negative_columns(
        df,
        rules.get(dataset_name, []),
    )


def process_dataset(
    spark: SparkSession,
    bucket: str,
    dataset_name: str,
) -> dict:
    config = get_dataset_config(dataset_name)

    silver_name = config["silver_name"]
    primary_key = config["primary_key"]
    required_columns = config["required_columns"]
    timestamp_candidates = config["timestamp_candidates"]

    bronze_path = (
        f"s3a://{bucket}/bronze/{dataset_name}"
    )
    silver_path = (
        f"s3a://{bucket}/silver/{silver_name}"
    )
    quarantine_path = (
        f"s3a://{bucket}/quarantine/{silver_name}"
    )

    print()
    print("=" * 100)
    print(f"PROCESSING SILVER DATASET: {dataset_name}")
    print(f"Bronze source   : {bronze_path}")
    print(f"Silver target   : {silver_path}")
    print(f"Quarantine path : {quarantine_path}")
    print("=" * 100)

    bronze_df = (
        spark.read
        .format("delta")
        .load(bronze_path)
    )

    bronze_count = bronze_df.count()

    transformed_df = apply_common_transformations(
        bronze_df,
        source_dataset=dataset_name,
    )

    assert_columns_exist(
        transformed_df,
        required_columns,
    )

    transformed_df = apply_dataset_transformations(
        transformed_df,
        dataset_name,
    )

    transformed_df = apply_numeric_validation(
        transformed_df,
        dataset_name,
    )

    valid_df, invalid_df = split_valid_and_invalid(
        transformed_df,
        required_columns,
    )

    valid_before_deduplication = valid_df.count()
    invalid_count = invalid_df.count()

    if config.get("deduplicate", True):
        valid_df = deduplicate_records(
            valid_df,
            primary_key=primary_key,
            timestamp_candidates=timestamp_candidates,
        )

    silver_count = valid_df.count()
    duplicates_removed = (
        valid_before_deduplication - silver_count
    )

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    if invalid_count > 0:
        (
            invalid_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(quarantine_path)
        )

    readback_count = (
        spark.read
        .format("delta")
        .load(silver_path)
        .count()
    )

    if silver_count != readback_count:
        raise RuntimeError(
            f"Silver write validation failed for {dataset_name}. "
            f"Expected {silver_count:,}, read back "
            f"{readback_count:,}."
        )

    result = {
        "dataset": dataset_name,
        "silver_name": silver_name,
        "bronze_count": bronze_count,
        "valid_before_deduplication": (
            valid_before_deduplication
        ),
        "duplicates_removed": duplicates_removed,
        "quarantined": invalid_count,
        "silver_count": silver_count,
    }

    print(
        f"SUCCESS: {dataset_name} "
        f"bronze={bronze_count:,}, "
        f"duplicates_removed={duplicates_removed:,}, "
        f"quarantined={invalid_count:,}, "
        f"silver={silver_count:,}"
    )

    return result


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process Customer 360 Silver datasets."
    )

    parser.add_argument(
        "--dataset",
        default="all",
        help=(
            "Dataset to process or 'all'. "
            f"Valid datasets: {', '.join(DATASETS)}"
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    if args.dataset == "all":
        selected_datasets: List[str] = list(DATASETS)
    else:
        if args.dataset not in DATASETS:
            print(
                f"Invalid dataset: {args.dataset}",
                file=sys.stderr,
            )
            sys.exit(2)

        selected_datasets = [args.dataset]

    bucket = os.getenv("MINIO_BUCKET", "customer360")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    results = []

    print("Customer 360 Silver processing started")
    print(
        "Started:",
        datetime.now(timezone.utc).isoformat(),
    )

    try:
        for dataset_name in selected_datasets:
            result = process_dataset(
                spark,
                bucket=bucket,
                dataset_name=dataset_name,
            )

            results.append(result)

        print()
        print("=" * 105)
        print("SILVER PROCESSING SUMMARY")
        print("=" * 105)
        print(
            f"{'Dataset':<22}"
            f"{'Bronze':>12}"
            f"{'Duplicates':>14}"
            f"{'Quarantined':>14}"
            f"{'Silver':>12}"
        )
        print("-" * 105)

        for result in results:
            print(
                f"{result['dataset']:<22}"
                f"{result['bronze_count']:>12,}"
                f"{result['duplicates_removed']:>14,}"
                f"{result['quarantined']:>14,}"
                f"{result['silver_count']:>12,}"
            )

        print("-" * 105)
        print(
            f"{'TOTAL':<22}"
            f"{sum(r['bronze_count'] for r in results):>12,}"
            f"{sum(r['duplicates_removed'] for r in results):>14,}"
            f"{sum(r['quarantined'] for r in results):>14,}"
            f"{sum(r['silver_count'] for r in results):>12,}"
        )
        print("=" * 105)
        print("Silver processing completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()