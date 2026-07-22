from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from datasets import (
    SILVER_TABLES,
    get_gold_config,
)
from metrics import (
    calculate_average_order_value,
    calculate_conversion_rate,
    calculate_resolution_rate,
    classify_customer_status,
    first_existing_column,
    monetary_value,
    normalised_text,
    require_column,
)

# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    """
    Create a Spark session configured for:

    - Apache Spark standalone cluster
    - MinIO through the S3A connector
    - Delta Lake tables
    """

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
        .appName("customer360-gold-processing")
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
        .config(
            "spark.sql.shuffle.partitions",
            "4",
        )
        .config(
            "spark.default.parallelism",
            "4",
        )
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# SILVER TABLE READER
# ---------------------------------------------------------------------------

def read_silver_tables(
    spark: SparkSession,
    bucket: str,
) -> Dict[str, DataFrame]:
    """
    Read all configured Silver Delta tables from MinIO.
    """

    tables: Dict[str, DataFrame] = {}

    print()
    print("=" * 100)
    print("READING SILVER DELTA TABLES")
    print("=" * 100)

    for logical_name, silver_name in SILVER_TABLES.items():
        path = f"s3a://{bucket}/silver/{silver_name}"

        print(
            f"Reading {logical_name:<20} "
            f"from {path}"
        )

        tables[logical_name] = (
            spark.read
            .format("delta")
            .load(path)
        )

    print("Silver tables loaded successfully.")

    return tables


# ---------------------------------------------------------------------------
# REUSABLE COLUMN HELPERS
# ---------------------------------------------------------------------------

def get_customer_email_column(
    customers: DataFrame,
) -> str:
    """
    Find the customer email column.
    """

    return require_column(
        customers,
        [
            "customer_email",
            "email",
            "email_address",
        ],
        purpose="customer email",
    )


def get_order_amount_column(
    orders: DataFrame,
) -> str:
    """
    Find the column containing the order monetary value.
    """

    return require_column(
        orders,
        [
            "order_total",
            "total_amount",
            "total",
            "order_amount",
        ],
        purpose="order monetary value",
    )


def get_order_date_column(
    orders: DataFrame,
) -> str:
    """
    Find the order date or timestamp column.
    """

    return require_column(
        orders,
        [
            "order_date",
            "created_at",
            "ordered_at",
            "purchase_date",
        ],
        purpose="order date",
    )


# ---------------------------------------------------------------------------
# GOLD MODEL 1: CUSTOMER 360
# ---------------------------------------------------------------------------

def build_customer_360(
    tables: Dict[str, DataFrame],
) -> DataFrame:
    """
    Build one consolidated record per customer.

    Sources:

    - customers
    - orders
    - payments
    - marketing_events
    - support_tickets
    - web_events
    """

    print()
    print("=" * 100)
    print("BUILDING GOLD TABLE: customer_360")
    print("=" * 100)

    customers = tables["customers"]
    orders = tables["orders"]
    payments = tables["payments"]
    marketing = tables["marketing_events"]
    support = tables["support_tickets"]
    web = tables["web_events"]

    customer_email_column = get_customer_email_column(
        customers
    )

    order_amount_column = get_order_amount_column(
        orders
    )

    order_date_column = get_order_date_column(
        orders
    )

    # -----------------------------------------------------------------------
    # Customer base
    # -----------------------------------------------------------------------

    customer_base = customers.withColumn(
        "customer_email",
        normalised_text(customer_email_column),
    )

    name_column = first_existing_column(
        customer_base,
        [
            "customer_name",
            "full_name",
            "name",
        ],
    )

    if name_column:
        customer_base = customer_base.withColumn(
            "customer_name",
            F.col(name_column),
        )

    else:
        first_name_column = first_existing_column(
            customer_base,
            ["first_name"],
        )

        last_name_column = first_existing_column(
            customer_base,
            [
                "last_name",
                "surname",
            ],
        )

        if first_name_column and last_name_column:
            customer_base = customer_base.withColumn(
                "customer_name",
                F.concat_ws(
                    " ",
                    F.col(first_name_column),
                    F.col(last_name_column),
                ),
            )

        else:
            customer_base = customer_base.withColumn(
                "customer_name",
                F.lit(None).cast("string"),
            )

    # -----------------------------------------------------------------------
    # Order metrics
    # -----------------------------------------------------------------------

    order_metrics = (
        orders
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias(
                "total_orders"
            ),
            F.round(
                F.sum(
                    monetary_value(
                        order_amount_column
                    )
                ),
                2,
            ).alias(
                "total_revenue"
            ),
            F.min(
                F.to_timestamp(
                    F.col(order_date_column)
                )
            ).alias(
                "first_order_at"
            ),
            F.max(
                F.to_timestamp(
                    F.col(order_date_column)
                )
            ).alias(
                "last_order_at"
            ),
        )
        .withColumn(
            "average_order_value",
            calculate_average_order_value(
                F.col("total_revenue"),
                F.col("total_orders"),
            ),
        )
    )

    # -----------------------------------------------------------------------
    # Payment metrics
    # -----------------------------------------------------------------------

    payment_amount_column = first_existing_column(
        payments,
        [
            "amount",
            "payment_amount",
        ],
    )

    payment_status_column = first_existing_column(
        payments,
        [
            "payment_status",
            "status",
        ],
    )

    payment_aggregations = [
        F.countDistinct("payment_id").alias(
            "total_payments"
        ),
    ]

    if payment_amount_column:
        payment_aggregations.append(
            F.round(
                F.sum(
                    monetary_value(
                        payment_amount_column
                    )
                ),
                2,
            ).alias(
                "total_payment_amount"
            )
        )

    if payment_status_column:
        payment_aggregations.append(
            F.sum(
                F.when(
                    F.lower(
                        F.trim(
                            F.col(payment_status_column)
                        )
                    ).isin(
                        "successful",
                        "success",
                        "paid",
                        "completed",
                    ),
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "successful_payments"
            )
        )

    payment_metrics = (
        payments
        .groupBy("customer_id")
        .agg(*payment_aggregations)
    )

    # -----------------------------------------------------------------------
    # Marketing metrics
    # -----------------------------------------------------------------------

    marketing_metrics = (
        marketing
        .withColumn(
            "customer_email",
            normalised_text("customer_email"),
        )
        .groupBy("customer_email")
        .agg(
            F.countDistinct("event_id").alias(
                "marketing_events"
            ),
            F.countDistinct("campaign_id").alias(
                "campaigns_engaged"
            ),
            F.max(
                F.to_timestamp(
                    F.col("event_timestamp")
                )
            ).alias(
                "last_marketing_event_at"
            ),
        )
    )

    # -----------------------------------------------------------------------
    # Support metrics
    # -----------------------------------------------------------------------

    support_metrics = (
        support
        .withColumn(
            "customer_email",
            normalised_text("customer_email"),
        )
        .groupBy("customer_email")
        .agg(
            F.countDistinct("ticket_id").alias(
                "support_tickets"
            ),
            F.round(
                F.avg(
                    F.col(
                        "satisfaction_score"
                    ).cast("double")
                ),
                2,
            ).alias(
                "average_satisfaction_score"
            ),
            F.max(
                F.to_timestamp(
                    F.col("created_at")
                )
            ).alias(
                "last_support_ticket_at"
            ),
        )
    )

    # -----------------------------------------------------------------------
    # Web metrics
    # -----------------------------------------------------------------------

    if "customer_id" in web.columns:
        web_metrics = (
            web
            .filter(
                F.col("customer_id").isNotNull()
            )
            .groupBy("customer_id")
            .agg(
                F.countDistinct("event_id").alias(
                    "web_events"
                ),
                F.countDistinct("session_id").alias(
                    "website_sessions"
                ),
            )
        )

    else:
        web_metrics = (
            customer_base
            .select("customer_id")
            .limit(0)
            .withColumn(
                "web_events",
                F.lit(0).cast("long"),
            )
            .withColumn(
                "website_sessions",
                F.lit(0).cast("long"),
            )
        )

    # -----------------------------------------------------------------------
    # Join all customer metrics
    # -----------------------------------------------------------------------

    result = (
        customer_base
        .join(
            order_metrics,
            on="customer_id",
            how="left",
        )
        .join(
            payment_metrics,
            on="customer_id",
            how="left",
        )
        .join(
            marketing_metrics,
            on="customer_email",
            how="left",
        )
        .join(
            support_metrics,
            on="customer_email",
            how="left",
        )
        .join(
            web_metrics,
            on="customer_id",
            how="left",
        )
    )

    numeric_defaults = {
        "total_orders": 0,
        "total_revenue": 0.0,
        "average_order_value": 0.0,
        "total_payments": 0,
        "total_payment_amount": 0.0,
        "successful_payments": 0,
        "marketing_events": 0,
        "campaigns_engaged": 0,
        "support_tickets": 0,
        "average_satisfaction_score": 0.0,
        "web_events": 0,
        "website_sessions": 0,
    }

    for column_name, default_value in (
        numeric_defaults.items()
    ):
        if column_name not in result.columns:
            result = result.withColumn(
                column_name,
                F.lit(default_value),
            )

        else:
            result = result.withColumn(
                column_name,
                F.coalesce(
                    F.col(column_name),
                    F.lit(default_value),
                ),
            )

    result = (
        result
        .withColumn(
            "customer_status",
            classify_customer_status(
                F.col("total_orders"),
                F.col("total_revenue"),
            ),
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp(),
        )
    )

    preferred_columns = [
        "customer_id",
        "customer_name",
        "customer_email",
        "total_orders",
        "total_revenue",
        "average_order_value",
        "total_payments",
        "total_payment_amount",
        "successful_payments",
        "marketing_events",
        "campaigns_engaged",
        "support_tickets",
        "average_satisfaction_score",
        "web_events",
        "website_sessions",
        "first_order_at",
        "last_order_at",
        "last_marketing_event_at",
        "last_support_ticket_at",
        "customer_status",
        "gold_processed_at",
    ]

    existing_preferred_columns = [
        column_name
        for column_name in preferred_columns
        if column_name in result.columns
    ]

    remaining_customer_columns = [
        column_name
        for column_name in customer_base.columns
        if column_name
        not in existing_preferred_columns
    ]

    result = result.select(
        *existing_preferred_columns,
        *remaining_customer_columns,
    )

    print("customer_360 model created.")

    return result


# ---------------------------------------------------------------------------
# GOLD MODEL 2: SALES SUMMARY
# ---------------------------------------------------------------------------

def build_sales_summary(
    tables: Dict[str, DataFrame],
) -> DataFrame:
    """
    Build daily sales and payment performance metrics.
    """

    print()
    print("=" * 100)
    print("BUILDING GOLD TABLE: sales_summary")
    print("=" * 100)

    orders = tables["orders"]
    payments = tables["payments"]

    order_amount_column = get_order_amount_column(
        orders
    )

    order_date_column = get_order_date_column(
        orders
    )

    order_status_column = first_existing_column(
        orders,
        [
            "order_status",
            "status",
        ],
    )

    discount_column = first_existing_column(
        orders,
        [
            "discount_amount",
            "discount",
        ],
    )

    shipping_column = first_existing_column(
        orders,
        [
            "shipping_amount",
            "shipping_cost",
        ],
    )

    daily_orders = orders.withColumn(
        "sales_date",
        F.to_date(
            F.col(order_date_column)
        ),
    )

    order_aggregations = [
        F.countDistinct("order_id").alias(
            "total_orders"
        ),
        F.countDistinct("customer_id").alias(
            "unique_customers"
        ),
        F.round(
            F.sum(
                monetary_value(
                    order_amount_column
                )
            ),
            2,
        ).alias(
            "gross_order_value"
        ),
    ]

    if discount_column:
        order_aggregations.append(
            F.round(
                F.sum(
                    monetary_value(
                        discount_column
                    )
                ),
                2,
            ).alias(
                "total_discount_amount"
            )
        )

    if shipping_column:
        order_aggregations.append(
            F.round(
                F.sum(
                    monetary_value(
                        shipping_column
                    )
                ),
                2,
            ).alias(
                "total_shipping_amount"
            )
        )

    if order_status_column:
        order_aggregations.append(
            F.sum(
                F.when(
                    F.lower(
                        F.trim(
                            F.col(order_status_column)
                        )
                    ).isin(
                        "cancelled",
                        "canceled",
                    ),
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            ).alias(
                "cancelled_orders"
            )
        )

    sales_summary = (
        daily_orders
        .groupBy("sales_date")
        .agg(*order_aggregations)
        .withColumn(
            "average_order_value",
            calculate_average_order_value(
                F.col("gross_order_value"),
                F.col("total_orders"),
            ),
        )
    )

    # -----------------------------------------------------------------------
    # Payment summary
    # -----------------------------------------------------------------------

    payment_date_column = first_existing_column(
        payments,
        [
            "payment_date",
            "created_at",
            "paid_at",
        ],
    )

    payment_amount_column = first_existing_column(
        payments,
        [
            "amount",
            "payment_amount",
        ],
    )

    payment_status_column = first_existing_column(
        payments,
        [
            "payment_status",
            "status",
        ],
    )

    if payment_date_column:
        daily_payments = payments.withColumn(
            "sales_date",
            F.to_date(
                F.col(payment_date_column)
            ),
        )

        payment_aggregations = [
            F.countDistinct("payment_id").alias(
                "total_payment_attempts"
            ),
        ]

        if payment_status_column:
            successful_condition = (
                F.lower(
                    F.trim(
                        F.col(payment_status_column)
                    )
                ).isin(
                    "successful",
                    "success",
                    "paid",
                    "completed",
                )
            )

            payment_aggregations.append(
                F.sum(
                    F.when(
                        successful_condition,
                        F.lit(1),
                    ).otherwise(
                        F.lit(0)
                    )
                ).alias(
                    "successful_payments"
                )
            )

            if payment_amount_column:
                payment_aggregations.append(
                    F.round(
                        F.sum(
                            F.when(
                                successful_condition,
                                monetary_value(
                                    payment_amount_column
                                ),
                            ).otherwise(
                                F.lit(0.0)
                            )
                        ),
                        2,
                    ).alias(
                        "successful_payment_amount"
                    )
                )

        else:
            payment_aggregations.append(
                F.countDistinct(
                    "payment_id"
                ).alias(
                    "successful_payments"
                )
            )

            if payment_amount_column:
                payment_aggregations.append(
                    F.round(
                        F.sum(
                            monetary_value(
                                payment_amount_column
                            )
                        ),
                        2,
                    ).alias(
                        "successful_payment_amount"
                    )
                )

        payment_summary = (
            daily_payments
            .groupBy("sales_date")
            .agg(*payment_aggregations)
        )

        sales_summary = sales_summary.join(
            payment_summary,
            on="sales_date",
            how="left",
        )

    default_columns = {
        "total_discount_amount": 0.0,
        "total_shipping_amount": 0.0,
        "cancelled_orders": 0,
        "total_payment_attempts": 0,
        "successful_payments": 0,
        "successful_payment_amount": 0.0,
    }

    for column_name, default_value in (
        default_columns.items()
    ):
        if column_name not in sales_summary.columns:
            sales_summary = (
                sales_summary.withColumn(
                    column_name,
                    F.lit(default_value),
                )
            )

        else:
            sales_summary = (
                sales_summary.withColumn(
                    column_name,
                    F.coalesce(
                        F.col(column_name),
                        F.lit(default_value),
                    ),
                )
            )

    sales_summary = (
        sales_summary
        .withColumn(
            "payment_success_rate",
            F.round(
                F.when(
                    F.col(
                        "total_payment_attempts"
                    ) == 0,
                    F.lit(0.0),
                ).otherwise(
                    F.col(
                        "successful_payments"
                    ).cast("double")
                    / F.col(
                        "total_payment_attempts"
                    ).cast("double")
                    * F.lit(100.0)
                ),
                2,
            ),
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp(),
        )
        .orderBy("sales_date")
    )

    print("sales_summary model created.")

    return sales_summary


# ---------------------------------------------------------------------------
# GOLD MODEL 3: PRODUCT PERFORMANCE
# ---------------------------------------------------------------------------

def build_product_performance(
    tables: Dict[str, DataFrame],
) -> DataFrame:
    """
    Build one record per product containing:

    - units sold;
    - distinct orders;
    - product revenue;
    - average selling price;
    - first sale date;
    - last sale date;
    - revenue rank.
    """

    print()
    print("=" * 100)
    print("BUILDING GOLD TABLE: product_performance")
    print("=" * 100)

    products = tables["products"]
    order_items = tables["order_items"]
    orders = tables["orders"]

    # -----------------------------------------------------------------------
    # Identify the required order-item columns
    # -----------------------------------------------------------------------

    quantity_column = require_column(
        order_items,
        [
            "quantity",
            "qty",
        ],
        purpose="order-item quantity",
    )

    unit_price_column = require_column(
        order_items,
        [
            "unit_price",
            "price",
        ],
        purpose="order-item unit price",
    )

    line_amount_column = first_existing_column(
        order_items,
        [
            "line_amount",
            "line_total",
            "total_amount",
        ],
    )

    order_date_column = get_order_date_column(
        orders
    )

    # -----------------------------------------------------------------------
    # Calculate revenue for each order item
    # -----------------------------------------------------------------------

    if line_amount_column:
        item_base = order_items.withColumn(
            "_product_revenue",
            monetary_value(line_amount_column),
        )

    else:
        item_base = order_items.withColumn(
            "_product_revenue",
            F.coalesce(
                F.col(
                    quantity_column
                ).cast("double"),
                F.lit(0.0),
            )
            * monetary_value(unit_price_column),
        )

    # -----------------------------------------------------------------------
    # Get each order's timestamp
    # -----------------------------------------------------------------------

    order_dates = orders.select(
        "order_id",
        F.to_timestamp(
            F.col(order_date_column)
        ).alias(
            "order_timestamp"
        ),
    )

    # Join the order-item records to their order dates.
    sales = item_base.join(
        order_dates,
        on="order_id",
        how="left",
    )

    # -----------------------------------------------------------------------
    # Aggregate product sales metrics
    # -----------------------------------------------------------------------

    product_metrics = (
        sales
        .groupBy("product_id")
        .agg(
            F.sum(
                F.coalesce(
                    F.col(
                        quantity_column
                    ).cast("long"),
                    F.lit(0),
                )
            ).alias(
                "units_sold"
            ),
            F.countDistinct("order_id").alias(
                "total_orders"
            ),
            F.round(
                F.sum(
                    F.col("_product_revenue")
                ),
                2,
            ).alias(
                "product_revenue"
            ),
            F.round(
                F.avg(
                    F.col(
                        unit_price_column
                    ).cast("double")
                ),
                2,
            ).alias(
                "average_selling_price"
            ),
            F.min("order_timestamp").alias(
                "first_sale_at"
            ),
            F.max("order_timestamp").alias(
                "last_sale_at"
            ),
        )
    )

    # Preserve products that have never been purchased by using a left join.
    result = products.join(
        product_metrics,
        on="product_id",
        how="left",
    )

    default_values = {
        "units_sold": 0,
        "total_orders": 0,
        "product_revenue": 0.0,
        "average_selling_price": 0.0,
    }

    for column_name, default_value in (
        default_values.items()
    ):
        result = result.withColumn(
            column_name,
            F.coalesce(
                F.col(column_name),
                F.lit(default_value),
            ),
        )

    # -----------------------------------------------------------------------
    # Product ranking and business classification
    # -----------------------------------------------------------------------

    revenue_window = Window.orderBy(
        F.col("product_revenue").desc(),
        F.col("product_id").asc(),
    )

    result = (
        result
        .withColumn(
            "revenue_rank",
            F.dense_rank().over(
                revenue_window
            ),
        )
        .withColumn(
            "product_sales_status",
            F.when(
                F.col("units_sold") == 0,
                F.lit("No Sales"),
            )
            .when(
                F.col("units_sold") >= 1000,
                F.lit("Best Seller"),
            )
            .when(
                F.col("units_sold") >= 500,
                F.lit("High Performing"),
            )
            .when(
                F.col("units_sold") >= 100,
                F.lit("Moderate"),
            )
            .otherwise(
                F.lit("Low Performing")
            ),
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp(),
        )
    )

    preferred_columns = [
        "product_id",
        "product_name",
        "category",
        "brand",
        "price",
        "unit_price",
        "cost",
        "units_sold",
        "total_orders",
        "product_revenue",
        "average_selling_price",
        "first_sale_at",
        "last_sale_at",
        "revenue_rank",
        "product_sales_status",
        "gold_processed_at",
    ]

    selected_columns = [
        column_name
        for column_name in preferred_columns
        if column_name in result.columns
    ]

    remaining_columns = [
        column_name
        for column_name in result.columns
        if column_name not in selected_columns
    ]

    result = result.select(
        *selected_columns,
        *remaining_columns,
    )

    print("product_performance model created.")

    return result

def build_marketing_summary(
    tables: Dict[str, DataFrame],
) -> DataFrame:
    """
    Build campaign-level marketing performance metrics.
    """

    print()
    print("=" * 100)
    print("BUILDING GOLD TABLE: marketing_summary")
    print("=" * 100)

    marketing = tables["marketing_events"]

    required_columns = [
        "campaign_id",
        "event_id",
        "event_type",
    ]

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in marketing.columns
    ]

    if missing_columns:
        raise ValueError(
            "marketing_events is missing required columns: "
            + ", ".join(missing_columns)
        )

    base = marketing.withColumn(
        "_event_type",
        F.lower(
            F.trim(
                F.col("event_type")
            )
        ),
    )

    grouping_columns = ["campaign_id"]

    for optional_column in [
        "campaign_name",
        "channel",
    ]:
        if optional_column in base.columns:
            grouping_columns.append(optional_column)

    aggregations = [
        F.countDistinct("event_id").alias(
            "total_events"
        ),
        F.sum(
            F.when(
                F.col("_event_type") == "impression",
                F.lit(1),
            ).otherwise(
                F.lit(0)
            )
        ).alias(
            "impressions"
        ),
        F.sum(
            F.when(
                F.col("_event_type") == "click",
                F.lit(1),
            ).otherwise(
                F.lit(0)
            )
        ).alias(
            "clicks"
        ),
        F.sum(
            F.when(
                F.col("_event_type").isin(
                    "conversion",
                    "converted",
                    "purchase",
                ),
                F.lit(1),
            ).otherwise(
                F.lit(0)
            )
        ).alias(
            "conversions"
        ),
        F.sum(
            F.when(
                F.col("_event_type") == "unsubscribe",
                F.lit(1),
            ).otherwise(
                F.lit(0)
            )
        ).alias(
            "unsubscribes"
        ),
    ]

    if "customer_email" in base.columns:
        aggregations.append(
            F.countDistinct("customer_email").alias(
                "unique_contacts"
            )
        )

    if "campaign_cost" in base.columns:
        aggregations.append(
            F.round(
                F.sum(
                    F.coalesce(
                        F.col("campaign_cost").cast("double"),
                        F.lit(0.0),
                    )
                ),
                2,
            ).alias(
                "campaign_cost"
            )
        )

    if "event_timestamp" in base.columns:
        aggregations.append(
            F.max(
                F.to_timestamp(
                    F.col("event_timestamp")
                )
            ).alias(
                "last_event_at"
            )
        )

    result = (
        base
        .groupBy(*grouping_columns)
        .agg(*aggregations)
    )

    default_columns = {
        "unique_contacts": 0,
        "campaign_cost": 0.0,
    }

    for column_name, default_value in default_columns.items():
        if column_name not in result.columns:
            result = result.withColumn(
                column_name,
                F.lit(default_value),
            )
        else:
            result = result.withColumn(
                column_name,
                F.coalesce(
                    F.col(column_name),
                    F.lit(default_value),
                ),
            )

    result = (
        result
        .withColumn(
            "click_through_rate",
            calculate_conversion_rate(
                F.col("clicks"),
                F.col("impressions"),
            ),
        )
        .withColumn(
            "conversion_rate",
            calculate_conversion_rate(
                F.col("conversions"),
                F.col("clicks"),
            ),
        )
        .withColumn(
            "cost_per_conversion",
            F.round(
                F.when(
                    F.col("conversions") == 0,
                    F.lit(0.0),
                ).otherwise(
                    F.col("campaign_cost")
                    / F.col("conversions")
                ),
                2,
            ),
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp(),
        )
    )

    print("marketing_summary model created.")

    return result

# ---------------------------------------------------------------------------
# GOLD MODEL 5: SUPPORT SUMMARY
# ---------------------------------------------------------------------------

def build_support_summary(
    tables: Dict[str, DataFrame],
) -> DataFrame:
    """
    Build support-ticket performance metrics grouped by
    ticket priority and category.
    """

    print()
    print("=" * 100)
    print("BUILDING GOLD TABLE: support_summary")
    print("=" * 100)

    support = tables["support_tickets"]

    # -----------------------------------------------------------------------
    # Validate required source columns
    # -----------------------------------------------------------------------

    required_source_columns = [
        "ticket_id",
        "customer_email",
    ]

    missing_columns = [
        column_name
        for column_name in required_source_columns
        if column_name not in support.columns
    ]

    if missing_columns:
        raise ValueError(
            "support_tickets is missing required columns: "
            + ", ".join(missing_columns)
        )

    status_column = require_column(
        support,
        [
            "status",
            "ticket_status",
        ],
        purpose="support-ticket status",
    )

    created_at_column = first_existing_column(
        support,
        [
            "created_at",
            "opened_at",
            "ticket_created_at",
        ],
    )

    resolved_at_column = first_existing_column(
        support,
        [
            "resolved_at",
            "closed_at",
            "completed_at",
        ],
    )

    satisfaction_column = first_existing_column(
        support,
        [
            "satisfaction_score",
            "csat_score",
            "customer_satisfaction",
        ],
    )

    # -----------------------------------------------------------------------
    # Standardise the source values
    # -----------------------------------------------------------------------

    base = (
        support
        .withColumn(
            "_status",
            F.lower(
                F.trim(
                    F.col(status_column).cast("string")
                )
            ),
        )
        .withColumn(
            "customer_email",
            normalised_text("customer_email"),
        )
    )

    if "priority" not in base.columns:
        base = base.withColumn(
            "priority",
            F.lit("unknown"),
        )
    else:
        base = base.withColumn(
            "priority",
            F.coalesce(
                F.lower(
                    F.trim(
                        F.col("priority").cast("string")
                    )
                ),
                F.lit("unknown"),
            ),
        )

    if "category" not in base.columns:
        base = base.withColumn(
            "category",
            F.lit("unknown"),
        )
    else:
        base = base.withColumn(
            "category",
            F.coalesce(
                F.lower(
                    F.trim(
                        F.col("category").cast("string")
                    )
                ),
                F.lit("unknown"),
            ),
        )

    # -----------------------------------------------------------------------
    # Create timestamp and resolution-time fields
    # -----------------------------------------------------------------------

    if created_at_column:
        base = base.withColumn(
            "_created_timestamp",
            F.to_timestamp(
                F.col(created_at_column)
            ),
        )
    else:
        base = base.withColumn(
            "_created_timestamp",
            F.lit(None).cast("timestamp"),
        )

    if resolved_at_column:
        base = base.withColumn(
            "_resolved_timestamp",
            F.to_timestamp(
                F.col(resolved_at_column)
            ),
        )
    else:
        base = base.withColumn(
            "_resolved_timestamp",
            F.lit(None).cast("timestamp"),
        )

    base = base.withColumn(
        "_resolution_hours",
        F.when(
            F.col("_created_timestamp").isNotNull()
            & F.col("_resolved_timestamp").isNotNull()
            & (
                F.col("_resolved_timestamp")
                >= F.col("_created_timestamp")
            ),
            (
                F.col("_resolved_timestamp").cast("long")
                - F.col("_created_timestamp").cast("long")
            )
            / F.lit(3600.0),
        ).otherwise(
            F.lit(None).cast("double")
        ),
    )

    # -----------------------------------------------------------------------
    # Define ticket-status conditions
    # -----------------------------------------------------------------------

    resolved_condition = F.col("_status").isin(
        "resolved",
        "closed",
        "completed",
    )

    open_condition = F.col("_status").isin(
        "open",
        "pending",
        "in progress",
        "in_progress",
        "new",
        "assigned",
    )

    # -----------------------------------------------------------------------
    # Build aggregation expressions
    # -----------------------------------------------------------------------

    aggregations = [
        F.countDistinct("ticket_id").alias(
            "total_tickets"
        ),
        F.sum(
            F.when(
                resolved_condition,
                F.lit(1),
            ).otherwise(
                F.lit(0)
            )
        ).alias(
            "resolved_tickets"
        ),
        F.sum(
            F.when(
                open_condition,
                F.lit(1),
            ).otherwise(
                F.lit(0)
            )
        ).alias(
            "open_tickets"
        ),
        F.countDistinct("customer_email").alias(
            "unique_customers"
        ),
        F.round(
            F.avg("_resolution_hours"),
            2,
        ).alias(
            "average_resolution_hours"
        ),
        F.max("_created_timestamp").alias(
            "latest_ticket_at"
        ),
    ]

    if satisfaction_column:
        aggregations.append(
            F.round(
                F.avg(
                    F.col(
                        satisfaction_column
                    ).cast("double")
                ),
                2,
            ).alias(
                "average_satisfaction"
            )
        )

    # -----------------------------------------------------------------------
    # Aggregate by priority and category
    # -----------------------------------------------------------------------

    result = (
        base
        .groupBy(
            "priority",
            "category",
        )
        .agg(*aggregations)
    )

    if "average_satisfaction" not in result.columns:
        result = result.withColumn(
            "average_satisfaction",
            F.lit(0.0),
        )
    else:
        result = result.withColumn(
            "average_satisfaction",
            F.coalesce(
                F.col("average_satisfaction"),
                F.lit(0.0),
            ),
        )

    result = (
        result
        .withColumn(
            "support_group",
            F.concat_ws(
                " | ",
                F.col("priority"),
                F.col("category"),
            ),
        )
        .withColumn(
            "resolution_rate",
            calculate_resolution_rate(
                F.col("resolved_tickets"),
                F.col("total_tickets"),
            ),
        )
        .withColumn(
            "unresolved_tickets",
            F.greatest(
                F.col("total_tickets")
                - F.col("resolved_tickets"),
                F.lit(0),
            ),
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp(),
        )
    )

    preferred_columns = [
        "support_group",
        "priority",
        "category",
        "total_tickets",
        "resolved_tickets",
        "open_tickets",
        "unresolved_tickets",
        "resolution_rate",
        "average_resolution_hours",
        "average_satisfaction",
        "unique_customers",
        "latest_ticket_at",
        "gold_processed_at",
    ]

    result = result.select(
        *[
            column_name
            for column_name in preferred_columns
            if column_name in result.columns
        ]
    )

    print("support_summary model created.")

    return result

# ---------------------------------------------------------------------------
# GOLD MODEL 6: WEB ENGAGEMENT SUMMARY
# ---------------------------------------------------------------------------

def build_web_engagement_summary(
    tables: Dict[str, DataFrame],
) -> DataFrame:
    """
    Build daily web engagement and conversion-funnel metrics.

    The model produces one row per engagement date.
    """

    print()
    print("=" * 100)
    print("BUILDING GOLD TABLE: web_engagement_summary")
    print("=" * 100)

    web = tables["web_events"]

    # -----------------------------------------------------------------------
    # Validate required source columns
    # -----------------------------------------------------------------------

    required_source_columns = [
        "event_id",
        "session_id",
        "event_type",
    ]

    missing_columns = [
        column_name
        for column_name in required_source_columns
        if column_name not in web.columns
    ]

    if missing_columns:
        raise ValueError(
            "web_events is missing required columns: "
            + ", ".join(missing_columns)
        )

    event_timestamp_column = require_column(
        web,
        [
            "event_timestamp",
            "event_time",
            "created_at",
            "occurred_at",
        ],
        purpose="web-event timestamp",
    )

    # -----------------------------------------------------------------------
    # Standardise event types and timestamps
    # -----------------------------------------------------------------------

    base = (
        web
        .withColumn(
            "_event_type",
            F.lower(
                F.trim(
                    F.col("event_type").cast("string")
                )
            ),
        )
        .withColumn(
            "_event_timestamp",
            F.to_timestamp(
                F.col(event_timestamp_column)
            ),
        )
        .withColumn(
            "engagement_date",
            F.to_date(
                F.col("_event_timestamp")
            ),
        )
        .filter(
            F.col("engagement_date").isNotNull()
        )
    )

    # -----------------------------------------------------------------------
    # Create a reusable visitor identifier
    # -----------------------------------------------------------------------

    visitor_identifier_columns = []

    if "customer_id" in base.columns:
        visitor_identifier_columns.append(
            F.col("customer_id").cast("string")
        )

    if "anonymous_id" in base.columns:
        visitor_identifier_columns.append(
            F.col("anonymous_id").cast("string")
        )

    visitor_identifier_columns.append(
        F.col("session_id").cast("string")
    )

    base = base.withColumn(
        "_visitor_id",
        F.coalesce(
            *visitor_identifier_columns
        ),
    )

    # -----------------------------------------------------------------------
    # Define normalized funnel conditions
    # -----------------------------------------------------------------------

    page_view_condition = F.col("_event_type").isin(
        "page_view",
        "page view",
        "pageview",
        "view_page",
    )

    product_view_condition = F.col("_event_type").isin(
        "product_view",
        "product view",
        "view_product",
    )

    add_to_cart_condition = F.col("_event_type").isin(
        "add_to_cart",
        "add to cart",
        "cart_add",
        "added_to_cart",
    )

    checkout_condition = F.col("_event_type").isin(
        "checkout",
        "begin_checkout",
        "checkout_started",
        "start_checkout",
    )

    purchase_condition = F.col("_event_type").isin(
        "purchase",
        "purchased",
        "conversion",
        "order_completed",
    )

    # -----------------------------------------------------------------------
    # Aggregate daily web metrics
    # -----------------------------------------------------------------------

    result = (
        base
        .groupBy("engagement_date")
        .agg(
            F.countDistinct("event_id").alias(
                "total_events"
            ),
            F.countDistinct("session_id").alias(
                "unique_sessions"
            ),
            F.countDistinct("_visitor_id").alias(
                "unique_visitors"
            ),
            F.sum(
                F.when(
                    page_view_condition,
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias(
                "page_views"
            ),
            F.sum(
                F.when(
                    product_view_condition,
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias(
                "product_views"
            ),
            F.sum(
                F.when(
                    add_to_cart_condition,
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias(
                "add_to_cart_events"
            ),
            F.sum(
                F.when(
                    checkout_condition,
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias(
                "checkout_events"
            ),
            F.sum(
                F.when(
                    purchase_condition,
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias(
                "purchase_events"
            ),
            F.countDistinct(
                F.when(
                    purchase_condition,
                    F.col("session_id"),
                )
            ).alias(
                "converted_sessions"
            ),
            F.max("_event_timestamp").alias(
                "latest_event_at"
            ),
        )
    )

    # -----------------------------------------------------------------------
    # Calculate funnel rates
    # -----------------------------------------------------------------------

    result = (
        result
        .withColumn(
            "product_view_rate",
            calculate_conversion_rate(
                F.col("product_views"),
                F.col("page_views"),
            ),
        )
        .withColumn(
            "cart_rate",
            calculate_conversion_rate(
                F.col("add_to_cart_events"),
                F.col("product_views"),
            ),
        )
        .withColumn(
            "checkout_rate",
            calculate_conversion_rate(
                F.col("checkout_events"),
                F.col("add_to_cart_events"),
            ),
        )
        .withColumn(
            "purchase_rate",
            calculate_conversion_rate(
                F.col("purchase_events"),
                F.col("checkout_events"),
            ),
        )
        .withColumn(
            "session_conversion_rate",
            calculate_conversion_rate(
                F.col("converted_sessions"),
                F.col("unique_sessions"),
            ),
        )
        .withColumn(
            "events_per_session",
            F.round(
                F.when(
                    F.col("unique_sessions") == 0,
                    F.lit(0.0),
                ).otherwise(
                    F.col("total_events").cast("double")
                    / F.col("unique_sessions").cast("double")
                ),
                2,
            ),
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp(),
        )
        .orderBy("engagement_date")
    )

    preferred_columns = [
        "engagement_date",
        "total_events",
        "unique_sessions",
        "unique_visitors",
        "page_views",
        "product_views",
        "add_to_cart_events",
        "checkout_events",
        "purchase_events",
        "converted_sessions",
        "product_view_rate",
        "cart_rate",
        "checkout_rate",
        "purchase_rate",
        "session_conversion_rate",
        "events_per_session",
        "latest_event_at",
        "gold_processed_at",
    ]

    result = result.select(
        *[
            column_name
            for column_name in preferred_columns
            if column_name in result.columns
        ]
    )

    print("web_engagement_summary model created.")

    return result

# ---------------------------------------------------------------------------
# GOLD DELTA WRITER
# ---------------------------------------------------------------------------

def write_gold_table(
    df: DataFrame,
    bucket: str,
    dataset_name: str,
) -> int:
    """
    Write a Gold DataFrame as a Delta table and validate its readback count.
    """

    config = get_gold_config(dataset_name)

    gold_name = str(
        config["gold_name"]
    )

    output_path = (
        f"s3a://{bucket}/gold/{gold_name}"
    )

    print()
    print(
        f"Writing {dataset_name} to {output_path}"
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .save(output_path)
    )

    readback_df = (
        df.sparkSession.read
        .format("delta")
        .load(output_path)
    )

    row_count = readback_df.count()

    if row_count == 0:
        raise RuntimeError(
            f"Gold write validation failed for "
            f"{dataset_name}: output table has zero rows."
        )

    print(
        f"SUCCESS: {dataset_name} written "
        f"with {row_count:,} rows."
    )

    return row_count


# ---------------------------------------------------------------------------
# COMMAND-LINE ARGUMENTS
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    """
    Parse the requested Gold dataset.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Build Customer 360 Gold datasets."
        )
    )

    parser.add_argument(
        "--dataset",
        default="all",
        choices=[
            "all",
            "customer_360",
            "sales_summary",
            "product_performance",
            "marketing_summary",
            "support_summary",
            "web_engagement_summary",       
        ],
        help=(
            "Gold dataset to build. "
            "Use 'all' to build every currently "
            "implemented Gold dataset."
        ),
    )

    return parser.parse_args()


# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Execute the selected Gold-layer models.
    """

    args = parse_arguments()

    bucket = os.getenv(
        "MINIO_BUCKET",
        "customer360",
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    results: List[Dict[str, object]] = []

    print()
    print("=" * 100)
    print("CUSTOMER 360 GOLD PROCESSING STARTED")
    print("=" * 100)
    print(
        "Started:",
        datetime.now(timezone.utc).isoformat(),
    )
    print(
        "Selected dataset:",
        args.dataset,
    )

    try:
        tables = read_silver_tables(
            spark,
            bucket,
        )

        if args.dataset == "all":
            selected_datasets = [
                "customer_360",
                "sales_summary",
                "product_performance",
                "marketing_summary",
                "support_summary",
                "web_engagement_summary",
            ]

        else:
            selected_datasets = [
                args.dataset
            ]

        for dataset_name in selected_datasets:
            print()
            print(
                f"Starting Gold dataset: "
                f"{dataset_name}"
            )

            if dataset_name == "customer_360":
                gold_df = build_customer_360(
                    tables
                )

            elif dataset_name == "sales_summary":
                gold_df = build_sales_summary(
                    tables
                )

            elif dataset_name == "product_performance":
                gold_df = build_product_performance(
                    tables
                )
                
            elif dataset_name == "marketing_summary":
                gold_df = build_marketing_summary(
                    tables
                )    
                
            elif dataset_name == "support_summary":
                gold_df = build_support_summary(
                    tables
                )

            elif dataset_name == "web_engagement_summary":
                gold_df = build_web_engagement_summary(
                    tables
                )

            else:
                print(
                    f"Unsupported Gold dataset: "
                    f"{dataset_name}",
                    file=sys.stderr,
                )
                sys.exit(2)

            row_count = write_gold_table(
                gold_df,
                bucket,
                dataset_name,
            )

            results.append(
                {
                    "dataset": dataset_name,
                    "row_count": row_count,
                }
            )

        print()
        print("=" * 75)
        print("GOLD PROCESSING SUMMARY")
        print("=" * 75)
        print(
            f"{'Dataset':<35}"
            f"{'Rows':>15}"
            f"{'Status':>20}"
        )
        print("-" * 75)

        for result in results:
            print(
                f"{result['dataset']:<35}"
                f"{result['row_count']:>15,}"
                f"{'SUCCESS':>20}"
            )

        print("-" * 75)
        print(
            f"{'TOTAL':<35}"
            f"{sum(r['row_count'] for r in results):>15,}"
            f"{'SUCCESS':>20}"
        )
        print("=" * 75)
        print(
            "Gold processing completed successfully."
        )

    except Exception as exc:
        print()
        print("=" * 100)
        print("GOLD PROCESSING FAILED")
        print("=" * 100)
        print(
            f"Error type: {type(exc).__name__}"
        )
        print(
            f"Error message: {exc}"
        )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()