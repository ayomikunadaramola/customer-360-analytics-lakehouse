from __future__ import annotations

import re
from typing import Iterable, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


TECHNICAL_COLUMNS = {
    "_source_file",
    "_source_system",
    "_batch_id",
    "_load_date",
    "_ingested_at",
}


def to_snake_case(value: str) -> str:
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)

    return value.strip("_").lower()


def standardize_column_names(df: DataFrame) -> DataFrame:
    result = df

    for old_name in df.columns:
        new_name = to_snake_case(old_name)

        if old_name != new_name:
            result = result.withColumnRenamed(old_name, new_name)

    return result


def trim_string_columns(df: DataFrame) -> DataFrame:
    result = df

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            result = result.withColumn(
                field.name,
                F.when(
                    F.col(field.name).isNull(),
                    F.lit(None),
                ).otherwise(F.trim(F.col(field.name))),
            )

    return result


def convert_blank_strings_to_null(df: DataFrame) -> DataFrame:
    result = df

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            result = result.withColumn(
                field.name,
                F.when(
                    F.trim(F.col(field.name)) == "",
                    F.lit(None),
                ).otherwise(F.col(field.name)),
            )

    return result


def normalize_email_columns(df: DataFrame) -> DataFrame:
    result = df

    for column_name in df.columns:
        if "email" in column_name.lower():
            result = result.withColumn(
                column_name,
                F.lower(F.trim(F.col(column_name))),
            )

    return result


def normalize_phone_columns(df: DataFrame) -> DataFrame:
    result = df

    for column_name in df.columns:
        if "phone" in column_name.lower():
            result = result.withColumn(
                column_name,
                F.regexp_replace(
                    F.col(column_name),
                    r"[^0-9+]",
                    "",
                ),
            )

    return result


def normalize_boolean_columns(df: DataFrame) -> DataFrame:
    result = df

    boolean_like_columns = [
        column_name
        for column_name in df.columns
        if column_name.startswith("is_")
        or column_name.startswith("has_")
        or column_name.endswith("_flag")
    ]

    for column_name in boolean_like_columns:
        result = result.withColumn(
            column_name,
            F.when(
                F.lower(F.col(column_name).cast("string")).isin(
                    "true",
                    "1",
                    "yes",
                    "y",
                ),
                F.lit(True),
            )
            .when(
                F.lower(F.col(column_name).cast("string")).isin(
                    "false",
                    "0",
                    "no",
                    "n",
                ),
                F.lit(False),
            )
            .otherwise(
                F.col(column_name).cast("boolean")
            ),
        )

    return result


def cast_timestamp_columns(df: DataFrame) -> DataFrame:
    result = df

    timestamp_patterns = (
        "_at",
        "_date",
        "_time",
        "_timestamp",
    )

    for column_name in df.columns:
        lower_name = column_name.lower()

        if lower_name == "_load_date":
            result = result.withColumn(
                column_name,
                F.to_date(F.col(column_name)),
            )
        elif lower_name.endswith(timestamp_patterns):
            result = result.withColumn(
                column_name,
                F.to_timestamp(F.col(column_name)),
            )

    return result


def add_processing_metadata(
    df: DataFrame,
    source_dataset: str,
) -> DataFrame:
    return (
        df.withColumn(
            "_silver_processed_at",
            F.current_timestamp(),
        )
        .withColumn(
            "_silver_source_dataset",
            F.lit(source_dataset),
        )
        .withColumn(
            "_silver_record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(
                            F.col(column_name).cast("string"),
                            F.lit("<NULL>"),
                        )
                        for column_name in sorted(df.columns)
                    ],
                ),
                256,
            ),
        )
    )


def choose_ordering_column(
    df: DataFrame,
    timestamp_candidates: Iterable[str],
) -> Optional[str]:
    for candidate in timestamp_candidates:
        if candidate in df.columns:
            return candidate

    return None


def deduplicate_records(
    df: DataFrame,
    primary_key: str,
    timestamp_candidates: Iterable[str],
) -> DataFrame:
    if primary_key not in df.columns:
        raise ValueError(
            f"Cannot deduplicate because primary key "
            f"'{primary_key}' does not exist."
        )

    ordering_column = choose_ordering_column(
        df,
        timestamp_candidates,
    )

    if ordering_column:
        ordering_expression = F.col(
            ordering_column
        ).desc_nulls_last()
    elif "_ingested_at" in df.columns:
        ordering_expression = F.col(
            "_ingested_at"
        ).desc_nulls_last()
    else:
        ordering_expression = F.monotonically_increasing_id().desc()

    window = Window.partitionBy(primary_key).orderBy(
        ordering_expression
    )

    return (
        df.withColumn(
            "_deduplication_rank",
            F.row_number().over(window),
        )
        .filter(
            F.col("_deduplication_rank") == 1
        )
        .drop("_deduplication_rank")
    )


def apply_common_transformations(
    df: DataFrame,
    source_dataset: str,
) -> DataFrame:
    result = standardize_column_names(df)
    result = trim_string_columns(result)
    result = convert_blank_strings_to_null(result)
    result = normalize_email_columns(result)
    result = normalize_phone_columns(result)
    result = normalize_boolean_columns(result)
    result = cast_timestamp_columns(result)
    result = add_processing_metadata(
        result,
        source_dataset=source_dataset,
    )

    return result

def transform_customers(df: DataFrame) -> DataFrame:
    result = df

    if "first_name" in result.columns:
        result = result.withColumn(
            "first_name",
            F.initcap(F.col("first_name")),
        )

    if "last_name" in result.columns:
        result = result.withColumn(
            "last_name",
            F.initcap(F.col("last_name")),
        )

    if "full_name" in result.columns:
        result = result.withColumn(
            "full_name",
            F.initcap(F.col("full_name")),
        )

    if "country" in result.columns:
        result = result.withColumn(
            "country",
            F.upper(F.col("country")),
        )

    if "status" in result.columns:
        result = result.withColumn(
            "status",
            F.lower(F.col("status")),
        )

    return result


def transform_products(df: DataFrame) -> DataFrame:
    result = df

    if "product_name" in result.columns:
        result = result.withColumn(
            "product_name",
            F.initcap(F.col("product_name")),
        )

    if "category" in result.columns:
        result = result.withColumn(
            "category",
            F.lower(F.col("category")),
        )

    for column_name in ["price", "unit_price", "cost"]:
        if column_name in result.columns:
            result = result.withColumn(
                column_name,
                F.round(
                    F.col(column_name).cast("decimal(18,2)"),
                    2,
                ),
            )

    return result


def transform_orders(df: DataFrame) -> DataFrame:
    result = df

    if "order_status" in result.columns:
        result = result.withColumn(
            "order_status",
            F.lower(F.col("order_status")),
        )

    if "status" in result.columns:
        result = result.withColumn(
            "status",
            F.lower(F.col("status")),
        )

    for column_name in [
        "order_total",
        "total_amount",
        "discount_amount",
        "shipping_amount",
    ]:
        if column_name in result.columns:
            result = result.withColumn(
                column_name,
                F.round(
                    F.col(column_name).cast("decimal(18,2)"),
                    2,
                ),
            )

    return result


def transform_order_items(df: DataFrame) -> DataFrame:
    result = df

    if "quantity" in result.columns:
        result = result.withColumn(
            "quantity",
            F.col("quantity").cast("integer"),
        )

    for column_name in [
        "unit_price",
        "line_amount",
        "discount_amount",
    ]:
        if column_name in result.columns:
            result = result.withColumn(
                column_name,
                F.round(
                    F.col(column_name).cast("decimal(18,2)"),
                    2,
                ),
            )

    if (
        "line_amount" not in result.columns
        and "quantity" in result.columns
        and "unit_price" in result.columns
    ):
        result = result.withColumn(
            "line_amount",
            F.round(
                F.col("quantity") * F.col("unit_price"),
                2,
            ),
        )

    return result


def transform_payments(df: DataFrame) -> DataFrame:
    result = df

    if "payment_status" in result.columns:
        result = result.withColumn(
            "payment_status",
            F.lower(F.col("payment_status")),
        )

    if "payment_method" in result.columns:
        result = result.withColumn(
            "payment_method",
            F.lower(F.col("payment_method")),
        )

    if "amount" in result.columns:
        result = result.withColumn(
            "amount",
            F.round(
                F.col("amount").cast("decimal(18,2)"),
                2,
            ),
        )

    return result


def transform_marketing_events(df: DataFrame) -> DataFrame:
    result = df

    for column_name in [
        "channel",
        "event_type",
        "campaign_name",
    ]:
        if column_name in result.columns:
            result = result.withColumn(
                column_name,
                F.lower(F.col(column_name)),
            )

    return result


def transform_support_tickets(df: DataFrame) -> DataFrame:
    result = df

    for column_name in [
        "status",
        "priority",
        "category",
        "channel",
    ]:
        if column_name in result.columns:
            result = result.withColumn(
                column_name,
                F.lower(F.col(column_name)),
            )

    return result


def transform_web_events(df: DataFrame) -> DataFrame:
    result = df

    for column_name in [
        "event_type",
        "device_type",
        "browser",
        "traffic_source",
    ]:
        if column_name in result.columns:
            result = result.withColumn(
                column_name,
                F.lower(F.col(column_name)),
            )

    return result


DATASET_TRANSFORMERS = {
    "crm_customers": transform_customers,
    "products": transform_products,
    "orders": transform_orders,
    "order_items": transform_order_items,
    "payments": transform_payments,
    "marketing_events": transform_marketing_events,
    "support_tickets": transform_support_tickets,
    "web_events": transform_web_events,
}


def apply_dataset_transformations(
    df: DataFrame,
    dataset_name: str,
) -> DataFrame:
    transformer = DATASET_TRANSFORMERS.get(dataset_name)

    if transformer is None:
        return df

    return transformer(df)