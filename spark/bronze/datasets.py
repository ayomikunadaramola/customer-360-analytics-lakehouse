from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import StructType

from spark.bronze.schemas import (
    CUSTOMERS_SCHEMA,
    MARKETING_EVENTS_SCHEMA,
    ORDERS_SCHEMA,
    ORDER_ITEMS_SCHEMA,
    PAYMENTS_SCHEMA,
    PRODUCTS_SCHEMA,
    SUPPORT_TICKETS_SCHEMA,
    WEB_EVENTS_SCHEMA,
)


@dataclass(frozen=True)
class BronzeDataset:
    name: str
    source_system: str
    source_path_template: str
    target_path: str
    schema: StructType


DATASETS: dict[str, BronzeDataset] = {
    "crm_customers": BronzeDataset(
        name="crm_customers",
        source_system="crm",
        source_path_template=(
            "landing/crm/load_date={load_date}/customers.csv"
        ),
        target_path="bronze/crm_customers",
        schema=CUSTOMERS_SCHEMA,
    ),
    "products": BronzeDataset(
        name="products",
        source_system="commerce",
        source_path_template=(
            "landing/commerce/load_date={load_date}/products.csv"
        ),
        target_path="bronze/products",
        schema=PRODUCTS_SCHEMA,
    ),
    "orders": BronzeDataset(
        name="orders",
        source_system="commerce",
        source_path_template=(
            "landing/commerce/load_date={load_date}/orders.csv"
        ),
        target_path="bronze/orders",
        schema=ORDERS_SCHEMA,
    ),
    "order_items": BronzeDataset(
        name="order_items",
        source_system="commerce",
        source_path_template=(
            "landing/commerce/load_date={load_date}/order_items.csv"
        ),
        target_path="bronze/order_items",
        schema=ORDER_ITEMS_SCHEMA,
    ),
    "payments": BronzeDataset(
        name="payments",
        source_system="payments",
        source_path_template=(
            "landing/commerce/load_date={load_date}/payments.csv"
        ),
        target_path="bronze/payments",
        schema=PAYMENTS_SCHEMA,
    ),
    "marketing_events": BronzeDataset(
        name="marketing_events",
        source_system="marketing",
        source_path_template=(
            "landing/marketing/load_date={load_date}/"
            "marketing_events.csv"
        ),
        target_path="bronze/marketing_events",
        schema=MARKETING_EVENTS_SCHEMA,
    ),
    "support_tickets": BronzeDataset(
        name="support_tickets",
        source_system="support",
        source_path_template=(
            "landing/support/load_date={load_date}/"
            "support_tickets.csv"
        ),
        target_path="bronze/support_tickets",
        schema=SUPPORT_TICKETS_SCHEMA,
    ),
    "web_events": BronzeDataset(
        name="web_events",
        source_system="web",
        source_path_template=(
            "landing/web/load_date={load_date}/web_events.csv"
        ),
        target_path="bronze/web_events",
        schema=WEB_EVENTS_SCHEMA,
    ),
}