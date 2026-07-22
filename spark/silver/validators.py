from __future__ import annotations

from typing import Iterable, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_columns_exist(
    df: DataFrame,
    required_columns: Iterable[str],
) -> None:
    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing required columns: "
            + ", ".join(missing_columns)
        )


def add_validation_reason(
    df: DataFrame,
    required_columns: Iterable[str],
) -> DataFrame:
    required_columns = list(required_columns)

    validation_conditions = []

    for column_name in required_columns:
        validation_conditions.append(
            F.when(
                F.col(column_name).isNull(),
                F.lit(f"{column_name}_is_null"),
            )
        )

    if not validation_conditions:
        return df.withColumn(
            "_validation_error",
            F.lit(None).cast("string"),
        )

    error_array = F.array(
        *validation_conditions
    )

    return df.withColumn(
        "_validation_error",
        F.concat_ws(
            ";",
            F.array_compact(error_array),
        ),
    )


def split_valid_and_invalid(
    df: DataFrame,
    required_columns: Iterable[str],
) -> Tuple[DataFrame, DataFrame]:
    validated_df = add_validation_reason(
        df,
        required_columns,
    )

    valid_df = (
        validated_df.filter(
            F.col("_validation_error") == ""
        )
        .drop("_validation_error")
    )

    invalid_df = (
        validated_df.filter(
            F.col("_validation_error") != ""
        )
        .withColumn(
            "_quarantined_at",
            F.current_timestamp(),
        )
    )

    return valid_df, invalid_df


def validate_non_negative_columns(
    df: DataFrame,
    column_names: Iterable[str],
) -> DataFrame:
    result = df

    for column_name in column_names:
        if column_name in result.columns:
            result = result.filter(
                F.col(column_name).isNull()
                | (F.col(column_name) >= 0)
            )

    return result