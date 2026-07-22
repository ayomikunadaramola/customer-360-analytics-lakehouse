from __future__ import annotations

from typing import Iterable, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def first_existing_column(
    df: DataFrame,
    candidates: Iterable[str],
) -> Optional[str]:
    """
    Return the first candidate column that exists in the DataFrame.
    """

    for column_name in candidates:
        if column_name in df.columns:
            return column_name

    return None


def require_column(
    df: DataFrame,
    candidates: Iterable[str],
    purpose: str,
) -> str:
    """
    Find a usable column or raise a descriptive error.
    """

    column_name = first_existing_column(
        df,
        candidates,
    )

    if column_name is None:
        raise ValueError(
            f"Could not identify the column required for {purpose}. "
            f"Expected one of: {', '.join(candidates)}. "
            f"Available columns: {', '.join(df.columns)}"
        )

    return column_name


def safe_divide(
    numerator: Column,
    denominator: Column,
    default_value: float = 0.0,
) -> Column:
    """
    Divide two Spark expressions safely without returning division errors.
    """

    return F.when(
        denominator.isNull() | (denominator == 0),
        F.lit(default_value),
    ).otherwise(
        numerator.cast("double")
        / denominator.cast("double")
    )


def monetary_value(
    column_name: str,
) -> Column:
    """
    Convert a monetary column to a consistent decimal-compatible double.
    """

    return F.coalesce(
        F.col(column_name).cast("double"),
        F.lit(0.0),
    )


def normalised_text(
    column_name: str,
) -> Column:
    """
    Normalise text for comparisons and joins.
    """

    return F.lower(
        F.trim(
            F.col(column_name).cast("string")
        )
    )


def calculate_average_order_value(
    revenue_column: Column,
    order_count_column: Column,
) -> Column:
    """
    Calculate average order value.
    """

    return F.round(
        safe_divide(
            revenue_column,
            order_count_column,
        ),
        2,
    )


def calculate_conversion_rate(
    conversions_column: Column,
    opportunities_column: Column,
) -> Column:
    """
    Calculate conversion rate as a percentage.
    """

    return F.round(
        safe_divide(
            conversions_column,
            opportunities_column,
        )
        * F.lit(100.0),
        2,
    )


def calculate_resolution_rate(
    resolved_column: Column,
    ticket_count_column: Column,
) -> Column:
    """
    Calculate support-ticket resolution rate as a percentage.
    """

    return F.round(
        safe_divide(
            resolved_column,
            ticket_count_column,
        )
        * F.lit(100.0),
        2,
    )


def classify_customer_status(
    total_orders_column: Column,
    total_revenue_column: Column,
) -> Column:
    """
    Assign a simple business status to each customer.
    """

    return (
        F.when(
            (total_orders_column >= 10)
            | (total_revenue_column >= 5000),
            F.lit("VIP"),
        )
        .when(
            total_orders_column >= 5,
            F.lit("Loyal"),
        )
        .when(
            total_orders_column >= 1,
            F.lit("Active"),
        )
        .otherwise(
            F.lit("Prospect")
        )
    )


def standardise_email(
    df: DataFrame,
    source_column: str,
    output_column: str = "customer_email",
) -> DataFrame:
    """
    Create a normalised email field used for cross-system matching.
    """

    return df.withColumn(
        output_column,
        normalised_text(source_column),
    )