from __future__ import annotations

from typing import Dict, List


SILVER_TABLES: Dict[str, str] = {
    "customers": "customers",
    "products": "products",
    "orders": "orders",
    "order_items": "order_items",
    "payments": "payments",
    "marketing_events": "marketing_events",
    "support_tickets": "support_tickets",
    "web_events": "web_events",
}


GOLD_DATASETS: Dict[str, Dict[str, object]] = {
    "customer_360": {
        "gold_name": "customer_360",
        "description": (
            "One consolidated record per customer containing "
            "commerce, payment, marketing, support, and web metrics."
        ),
        "primary_key": "customer_id",
        "required_columns": [
            "customer_id",
            "customer_email",
            "total_orders",
            "total_revenue",
            "average_order_value",
            "total_payments",
            "marketing_events",
            "support_tickets",
            "web_events",
        ],
        "partition_columns": [],
    },

    "sales_summary": {
        "gold_name": "sales_summary",
        "description": (
            "Daily sales performance metrics generated from "
            "customer orders and payment activity."
        ),
        "primary_key": "sales_date",
        "required_columns": [
            "sales_date",
            "total_orders",
            "unique_customers",
            "gross_order_value",
            "average_order_value",
            "successful_payments",
            "successful_payment_amount",
        ],
        "partition_columns": [],
    },

    "product_performance": {
        "gold_name": "product_performance",
        "description": (
            "Product-level sales, quantity, order, "
            "and revenue metrics."
        ),
        "primary_key": "product_id",
        "required_columns": [
            "product_id",
            "units_sold",
            "total_orders",
            "product_revenue",
            "average_selling_price",
            "revenue_rank",
            "product_sales_status",
        ],
        "partition_columns": [],
    },

    "marketing_summary": {
        "gold_name": "marketing_summary",
        "description": (
            "Campaign and channel performance metrics."
        ),
        "primary_key": "campaign_id",
        "required_columns": [
            "campaign_id",
            "total_events",
            "impressions",
            "clicks",
            "conversions",
            "unsubscribes",
            "click_through_rate",
            "conversion_rate",
            "cost_per_conversion",
        ],
        "partition_columns": [],
    },

    "support_summary": {
        "gold_name": "support_summary",
        "description": (
            "Support-ticket performance metrics grouped "
            "by priority and category."
        ),
        "primary_key": "support_group",
        "required_columns": [
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
        ],
        "partition_columns": [],
    }, 
    
    "web_engagement_summary": {
        "gold_name": "web_engagement_summary",
        "description": (
            "Daily web engagement and conversion-funnel metrics "
            "generated from customer web events."
        ),
        "primary_key": "engagement_date",
        "required_columns": [
            "engagement_date",
            "total_events",
            "unique_sessions",
            "unique_visitors",
            "page_views",
            "product_views",
            "add_to_cart_events",
            "checkout_events",
            "purchase_events",
            "session_conversion_rate",
        ],
        "partition_columns": [],
    },
    
}


GOLD_DATASET_NAMES: List[str] = list(
    GOLD_DATASETS.keys()
)


def get_gold_config(
    dataset_name: str,
) -> Dict[str, object]:
    """
    Return the configuration for a Gold dataset.

    Raises:
        ValueError: If the requested dataset is not configured.
    """

    if dataset_name not in GOLD_DATASETS:
        valid_names = ", ".join(
            GOLD_DATASET_NAMES
        )

        raise ValueError(
            f"Unknown Gold dataset: {dataset_name}. "
            f"Valid datasets: {valid_names}"
        )

    return GOLD_DATASETS[dataset_name]