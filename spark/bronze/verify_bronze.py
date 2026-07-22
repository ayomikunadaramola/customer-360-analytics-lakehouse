from __future__ import annotations

import os
import sys
from typing import Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct


EXPECTED_COUNTS: Dict[str, int] = {
    "crm_customers": 2063,
    "products": 500,
    "orders": 10000,
    "order_items": 24956,
    "payments": 10000,
    "marketing_events": 16504,
    "support_tickets": 4000,
    "web_events": 30000,
}

PRIMARY_KEYS: Dict[str, str] = {
    "crm_customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "marketing_events": "event_id",
    "support_tickets": "ticket_id",
    "web_events": "event_id",
}


def create_spark_session() -> SparkSession:
    """
    Create a Spark session configured for Delta Lake and MinIO.
    """

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")

    if not minio_access_key or not minio_secret_key:
        raise ValueError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be available "
            "inside the Spark container."
        )

    spark = (
        SparkSession.builder
        .appName("customer360-verify-bronze")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
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

    spark.sparkContext.setLogLevel("WARN")

    return spark


def determine_status(
    total_rows: int,
    expected_count: int,
    null_keys: int,
    duplicate_keys: int,
) -> str:
    """
    Determine the Bronze verification status.

    Bronze tables are expected to preserve source data as received.
    Duplicate keys are therefore reported as warnings rather than failures.

    A dataset fails only when:
    - its row count does not match the expected source count; or
    - its primary key contains null values.
    """

    row_count_matches = total_rows == expected_count
    has_no_null_keys = null_keys == 0

    if not row_count_matches or not has_no_null_keys:
        return "FAILED"

    if duplicate_keys > 0:
        return (
            f"PASSED_WITH_WARNINGS: "
            f"{duplicate_keys} duplicate key(s)"
        )

    return "PASSED"


def verify_dataset(
    spark: SparkSession,
    bucket: str,
    dataset: str,
    expected_count: int,
    primary_key: str,
) -> dict:
    """
    Verify one Bronze Delta dataset.

    Checks performed:
    - Delta table can be read;
    - total row count matches the expected source count;
    - primary-key column exists;
    - primary-key values are not null;
    - duplicate keys are measured and reported as warnings.
    """

    path = f"s3a://{bucket}/bronze/{dataset}"

    df = spark.read.format("delta").load(path)

    total_rows = df.count()

    if primary_key not in df.columns:
        return {
            "dataset": dataset,
            "rows": total_rows,
            "expected": expected_count,
            "null_keys": None,
            "duplicate_keys": None,
            "status": f"FAILED: missing column '{primary_key}'",
        }

    key_metrics = (
        df.agg(
            count(col(primary_key)).alias("non_null_keys"),
            countDistinct(col(primary_key)).alias("distinct_keys"),
        )
        .first()
    )

    non_null_keys = int(key_metrics["non_null_keys"])
    distinct_keys = int(key_metrics["distinct_keys"])

    null_keys = total_rows - non_null_keys
    duplicate_keys = non_null_keys - distinct_keys

    status = determine_status(
        total_rows=total_rows,
        expected_count=expected_count,
        null_keys=null_keys,
        duplicate_keys=duplicate_keys,
    )

    return {
        "dataset": dataset,
        "rows": total_rows,
        "expected": expected_count,
        "null_keys": null_keys,
        "duplicate_keys": duplicate_keys,
        "status": status,
    }


def is_failed_status(status: str) -> bool:
    """
    Return True only when a dataset genuinely failed verification.
    """

    return status == "FAILED" or status.startswith("FAILED:")


def format_optional_number(value: Optional[int]) -> str:
    """
    Format integers for terminal output while safely handling None.
    """

    if value is None:
        return "N/A"

    return f"{value:,}"


def main() -> None:
    """
    Verify every Bronze Delta table and print a consolidated summary.
    """

    bucket = os.getenv("MINIO_BUCKET", "customer360")
    expected_total = sum(EXPECTED_COUNTS.values())

    spark = create_spark_session()
    results = []

    print()
    print("=" * 118)
    print("CUSTOMER 360 BRONZE VERIFICATION")
    print("=" * 118)
    print(
        f"{'Dataset':<22}"
        f"{'Rows':>12}"
        f"{'Expected':>12}"
        f"{'Null Keys':>12}"
        f"{'Duplicates':>14}"
        f"  Status"
    )
    print("-" * 118)

    try:
        for dataset, expected_count in EXPECTED_COUNTS.items():
            primary_key = PRIMARY_KEYS[dataset]

            try:
                result = verify_dataset(
                    spark=spark,
                    bucket=bucket,
                    dataset=dataset,
                    expected_count=expected_count,
                    primary_key=primary_key,
                )
            except Exception as exc:
                result = {
                    "dataset": dataset,
                    "rows": None,
                    "expected": expected_count,
                    "null_keys": None,
                    "duplicate_keys": None,
                    "status": f"FAILED: {exc}",
                }

            results.append(result)

            print(
                f"{result['dataset']:<22}"
                f"{format_optional_number(result['rows']):>12}"
                f"{format_optional_number(result['expected']):>12}"
                f"{format_optional_number(result['null_keys']):>12}"
                f"{format_optional_number(result['duplicate_keys']):>14}"
                f"  {result['status']}"
            )

        failed_results = [
            result
            for result in results
            if is_failed_status(result["status"])
        ]

        warning_results = [
            result
            for result in results
            if result["status"].startswith("PASSED_WITH_WARNINGS")
        ]

        verified_rows = sum(
            result["rows"]
            for result in results
            if isinstance(result["rows"], int)
        )

        passed_tables = len(results) - len(failed_results)

        print("-" * 118)
        print(f"Verified rows : {verified_rows:,}")
        print(f"Expected rows : {expected_total:,}")
        print(f"Passed tables : {passed_tables}/{len(results)}")
        print(f"Warnings      : {len(warning_results)}")
        print(f"Failures      : {len(failed_results)}")
        print("=" * 118)

        if warning_results:
            print()
            print("BRONZE DATA QUALITY WARNINGS")
            print("-" * 118)

            for result in warning_results:
                print(
                    f"- {result['dataset']}: "
                    f"{result['duplicate_keys']:,} duplicate key(s) detected. "
                    "These records should be deduplicated or quarantined "
                    "during Silver processing."
                )

        if failed_results:
            print()
            print("BRONZE VERIFICATION FAILURES")
            print("-" * 118)

            for result in failed_results:
                print(
                    f"- {result['dataset']}: {result['status']}"
                )

            sys.exit(1)

        print()
        print("Bronze verification completed successfully.")

        if warning_results:
            print(
                "The Bronze layer preserved all source records, including "
                "known duplicate records that will be handled in Phase 11."
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()