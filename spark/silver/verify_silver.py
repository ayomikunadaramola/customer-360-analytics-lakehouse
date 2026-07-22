from __future__ import annotations

import os
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SILVER_TABLES: Dict[str, Dict[str, object]] = {
    "customers": {
        "primary_key": "customer_id",
        "required_columns": [
            "customer_id",
            "email",
        ],
    },
    "products": {
        "primary_key": "product_id",
        "required_columns": [
            "product_id",
        ],
    },
    "orders": {
        "primary_key": "order_id",
        "required_columns": [
            "order_id",
            "customer_id",
        ],
    },
    "order_items": {
        "primary_key": "order_item_id",
        "required_columns": [
            "order_item_id",
            "order_id",
            "product_id",
        ],
    },
    "payments": {
        "primary_key": "payment_id",
        "required_columns": [
            "payment_id",
            "order_id",
        ],
    },
    "marketing_events": {
        "primary_key": "event_id",
        "required_columns": [
            "event_id",
            "customer_email",
        ],
    },
    "support_tickets": {
        "primary_key": "ticket_id",
        "required_columns": [
            "ticket_id",
            "customer_email",
        ],
    },
    "web_events": {
        "primary_key": "event_id",
        "required_columns": [
            "event_id",
        ],
    },
}


def create_spark_session() -> SparkSession:
    endpoint = os.getenv(
        "MINIO_ENDPOINT",
        "http://minio:9000",
    )
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required."
        )

    return (
        SparkSession.builder
        .appName("customer360-silver-verification")
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
            endpoint,
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            secret_key,
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


def read_silver_table(
    spark: SparkSession,
    bucket: str,
    table_name: str,
) -> DataFrame:
    path = f"s3a://{bucket}/silver/{table_name}"

    return (
        spark.read
        .format("delta")
        .load(path)
    )


def missing_columns(
    df: DataFrame,
    required_columns: List[str],
) -> List[str]:
    return [
        column_name
        for column_name in required_columns
        if column_name not in df.columns
    ]


def count_null_primary_keys(
    df: DataFrame,
    primary_key: str,
) -> int:
    return (
        df.filter(
            F.col(primary_key).isNull()
            | (
                F.trim(
                    F.col(primary_key).cast("string")
                )
                == ""
            )
        )
        .count()
    )


def count_duplicate_primary_keys(
    df: DataFrame,
    primary_key: str,
) -> int:
    return (
        df.groupBy(primary_key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def count_orphan_records(
    child_df: DataFrame,
    parent_df: DataFrame,
    child_column: str,
    parent_column: str,
    allow_null_child: bool = False,
) -> int:
    child_keys = child_df.select(
        F.col(child_column).alias("relationship_key")
    )

    if allow_null_child:
        child_keys = child_keys.filter(
            F.col("relationship_key").isNotNull()
        )

    parent_keys = (
        parent_df.select(
            F.col(parent_column).alias(
                "relationship_key"
            )
        )
        .filter(
            F.col("relationship_key").isNotNull()
        )
        .dropDuplicates()
    )

    return (
        child_keys.join(
            parent_keys,
            on="relationship_key",
            how="left_anti",
        )
        .count()
    )


def verify_table(
    spark: SparkSession,
    bucket: str,
    table_name: str,
    config: Dict[str, object],
) -> Tuple[DataFrame, Dict[str, object]]:
    df = read_silver_table(
        spark,
        bucket,
        table_name,
    )

    primary_key = str(config["primary_key"])
    required_columns = list(
        config["required_columns"]
    )

    absent_columns = missing_columns(
        df,
        required_columns,
    )

    if absent_columns:
        return df, {
            "table": table_name,
            "row_count": df.count(),
            "null_primary_keys": None,
            "duplicate_primary_keys": None,
            "missing_columns": absent_columns,
            "status": "FAILED",
        }

    row_count = df.count()
    null_primary_keys = count_null_primary_keys(
        df,
        primary_key,
    )
    duplicate_primary_keys = (
        count_duplicate_primary_keys(
            df,
            primary_key,
        )
    )

    status = "PASSED"

    if (
        row_count == 0
        or null_primary_keys > 0
        or duplicate_primary_keys > 0
    ):
        status = "FAILED"

    return df, {
        "table": table_name,
        "row_count": row_count,
        "null_primary_keys": null_primary_keys,
        "duplicate_primary_keys": (
            duplicate_primary_keys
        ),
        "missing_columns": absent_columns,
        "status": status,
    }


def verify_relationships(
    tables: Dict[str, DataFrame],
) -> List[Dict[str, object]]:
    checks: List[Dict[str, object]] = []

    relationships = [
        (
            "orders.customer_id",
            "customers.customer_id",
            "orders",
            "customers",
            "customer_id",
            "customer_id",
            False,
        ),
        (
            "order_items.order_id",
            "orders.order_id",
            "order_items",
            "orders",
            "order_id",
            "order_id",
            False,
        ),
        (
            "order_items.product_id",
            "products.product_id",
            "order_items",
            "products",
            "product_id",
            "product_id",
            False,
        ),
        (
            "payments.order_id",
            "orders.order_id",
            "payments",
            "orders",
            "order_id",
            "order_id",
            False,
        ),
        (
            "marketing_events.customer_email",
            "customers.email",
            "marketing_events",
            "customers",
            "customer_email",
            "email",
            False,
        ),
        (
            "support_tickets.customer_email",
            "customers.email",
            "support_tickets",
            "customers",
            "customer_email",
            "email",
            False,
        ),
        (
            "web_events.customer_id",
            "customers.customer_id",
            "web_events",
            "customers",
            "customer_id",
            "customer_id",
            True,
        ),
    ]

    for (
        child_label,
        parent_label,
        child_table,
        parent_table,
        child_column,
        parent_column,
        allow_null_child,
    ) in relationships:
        child_df = tables[child_table]
        parent_df = tables[parent_table]

        if child_column not in child_df.columns:
            checks.append(
                {
                    "relationship": (
                        f"{child_label} -> {parent_label}"
                    ),
                    "orphan_count": None,
                    "status": "SKIPPED",
                    "reason": (
                        f"Missing child column: "
                        f"{child_column}"
                    ),
                }
            )
            continue

        if parent_column not in parent_df.columns:
            checks.append(
                {
                    "relationship": (
                        f"{child_label} -> {parent_label}"
                    ),
                    "orphan_count": None,
                    "status": "SKIPPED",
                    "reason": (
                        f"Missing parent column: "
                        f"{parent_column}"
                    ),
                }
            )
            continue

        orphan_count = count_orphan_records(
            child_df,
            parent_df,
            child_column,
            parent_column,
            allow_null_child=allow_null_child,
        )

        checks.append(
            {
                "relationship": (
                    f"{child_label} -> {parent_label}"
                ),
                "orphan_count": orphan_count,
                "status": (
                    "PASSED"
                    if orphan_count == 0
                    else "FAILED"
                ),
                "reason": "",
            }
        )

    return checks


def print_table_results(
    results: List[Dict[str, object]],
) -> None:
    print()
    print("=" * 105)
    print("SILVER TABLE VERIFICATION")
    print("=" * 105)
    print(
        f"{'Table':<22}"
        f"{'Rows':>12}"
        f"{'Null PKs':>14}"
        f"{'Duplicate PKs':>16}"
        f"{'Status':>14}"
    )
    print("-" * 105)

    for result in results:
        null_value = result["null_primary_keys"]
        duplicate_value = result[
            "duplicate_primary_keys"
        ]

        null_display = (
            "-"
            if null_value is None
            else f"{null_value:,}"
        )
        duplicate_display = (
            "-"
            if duplicate_value is None
            else f"{duplicate_value:,}"
        )

        print(
            f"{result['table']:<22}"
            f"{result['row_count']:>12,}"
            f"{null_display:>14}"
            f"{duplicate_display:>16}"
            f"{result['status']:>14}"
        )

        missing = result["missing_columns"]

        if missing:
            print(
                "  Missing columns: "
                + ", ".join(missing)
            )

    print("=" * 105)


def print_relationship_results(
    checks: List[Dict[str, object]],
) -> None:
    print()
    print("=" * 105)
    print("REFERENTIAL-INTEGRITY VERIFICATION")
    print("=" * 105)
    print(
        f"{'Relationship':<72}"
        f"{'Orphans':>12}"
        f"{'Status':>14}"
    )
    print("-" * 105)

    for check in checks:
        orphan_count = check["orphan_count"]

        orphan_display = (
            "-"
            if orphan_count is None
            else f"{orphan_count:,}"
        )

        print(
            f"{check['relationship']:<72}"
            f"{orphan_display:>12}"
            f"{check['status']:>14}"
        )

        if check["reason"]:
            print(
                f"  Reason: {check['reason']}"
            )

    print("=" * 105)


def main() -> None:
    print("Customer 360 Silver verification started")

    bucket = os.getenv(
        "MINIO_BUCKET",
        "customer360",
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    tables: Dict[str, DataFrame] = {}
    table_results: List[Dict[str, object]] = []

    try:
        for table_name, config in SILVER_TABLES.items():
            print(
                f"Verifying Silver table: {table_name}"
            )

            df, result = verify_table(
                spark,
                bucket,
                table_name,
                config,
            )

            tables[table_name] = df
            table_results.append(result)

        relationship_results = (
            verify_relationships(tables)
        )

        print_table_results(table_results)
        print_relationship_results(
            relationship_results
        )

        failed_tables = [
            result
            for result in table_results
            if result["status"] == "FAILED"
        ]

        failed_relationships = [
            result
            for result in relationship_results
            if result["status"] == "FAILED"
        ]

        if failed_tables or failed_relationships:
            raise RuntimeError(
                "Silver verification failed. "
                f"Failed tables: {len(failed_tables)}; "
                "failed relationships: "
                f"{len(failed_relationships)}."
            )

        print()
        print(
            "All Silver tables and relationships "
            "passed verification."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()