from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType


def string_schema(columns: list[str]) -> StructType:
    """
    Bronze stores source values as strings to preserve source fidelity.

    Data typing and business-standard parsing will happen in Silver.
    """
    return StructType(
        [
            StructField(column, StringType(), True)
            for column in columns
        ]
    )


CUSTOMERS_SCHEMA = string_schema(
    [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "date_of_birth",
        "city",
        "country",
        "signup_date",
        "account_status",
        "updated_at",
    ]
)

PRODUCTS_SCHEMA = string_schema(
    [
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "currency",
        "is_active",
    ]
)

ORDERS_SCHEMA = string_schema(
    [
        "order_id",
        "customer_id",
        "order_date",
        "order_status",
        "sales_channel",
        "currency",
        "discount_amount",
        "shipping_amount",
        "order_total",
        "updated_at",
    ]
)

ORDER_ITEMS_SCHEMA = string_schema(
    [
        "order_item_id",
        "order_id",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        "line_amount",
    ]
)

PAYMENTS_SCHEMA = string_schema(
    [
        "payment_id",
        "order_id",
        "customer_id",
        "payment_date",
        "payment_method",
        "amount",
        "payment_status",
        "gateway",
        "failure_reason",
    ]
)

MARKETING_EVENTS_SCHEMA = string_schema(
    [
        "event_id",
        "customer_email",
        "campaign_id",
        "campaign_name",
        "channel",
        "event_type",
        "event_timestamp",
        "campaign_cost",
    ]
)

SUPPORT_TICKETS_SCHEMA = string_schema(
    [
        "ticket_id",
        "customer_email",
        "customer_phone",
        "created_at",
        "resolved_at",
        "priority",
        "category",
        "status",
        "satisfaction_score",
    ]
)

WEB_EVENTS_SCHEMA = string_schema(
    [
        "event_id",
        "anonymous_id",
        "customer_id",
        "session_id",
        "event_type",
        "page_url",
        "product_id",
        "event_timestamp",
        "device_type",
        "traffic_source",
    ]
)