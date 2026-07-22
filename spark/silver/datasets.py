from __future__ import annotations

from typing import Any, Dict


DATASETS: Dict[str, Dict[str, Any]] = {
    "crm_customers": {
        "silver_name": "customers",
        "primary_key": "customer_id",
        "deduplicate": True,
        "timestamp_candidates": [
            "updated_at",
            "created_at",
            "registration_date",
            "_ingested_at",
        ],
        "required_columns": [
            "customer_id",
        ],
    },
    "products": {
        "silver_name": "products",
        "primary_key": "product_id",
        "deduplicate": True,
        "timestamp_candidates": [
            "updated_at",
            "created_at",
            "_ingested_at",
        ],
        "required_columns": [
            "product_id",
        ],
    },
    "orders": {
        "silver_name": "orders",
        "primary_key": "order_id",
        "deduplicate": True,
        "timestamp_candidates": [
            "updated_at",
            "order_date",
            "created_at",
            "_ingested_at",
        ],
        "required_columns": [
            "order_id",
            "customer_id",
        ],
    },
    "order_items": {
        "silver_name": "order_items",
        "primary_key": "order_item_id",
        "deduplicate": True,
        "timestamp_candidates": [
            "updated_at",
            "created_at",
            "_ingested_at",
        ],
        "required_columns": [
            "order_item_id",
            "order_id",
            "product_id",
        ],
    },
    "payments": {
        "silver_name": "payments",
        "primary_key": "payment_id",
        "deduplicate": True,
        "timestamp_candidates": [
            "payment_date",
            "updated_at",
            "created_at",
            "_ingested_at",
        ],
        "required_columns": [
            "payment_id",
            "order_id",
        ],
    },
    "marketing_events": {
        "silver_name": "marketing_events",
        "primary_key": "event_id",
        "deduplicate": True,
        "timestamp_candidates": [
            "event_timestamp",
            "event_time",
            "created_at",
            "_ingested_at",
        ],
        "required_columns": [
            "event_id",
            "customer_email",
        ],
    },
    "support_tickets": {
        "silver_name": "support_tickets",
        "primary_key": "ticket_id",
        "deduplicate": True,
        "timestamp_candidates": [
            "created_at",
            "resolved_at",
            "_ingested_at",
        ],
        "required_columns": [
            "ticket_id",
            "customer_email",
        ],
    },
    "web_events": {
        "silver_name": "web_events",
        "primary_key": "event_id",
        "deduplicate": True,
        "timestamp_candidates": [
            "event_timestamp",
            "event_time",
            "created_at",
            "_ingested_at",
        ],
        "required_columns": [
            "event_id",
        ],
    },
}


def get_dataset_config(dataset_name: str) -> Dict[str, Any]:
    if dataset_name not in DATASETS:
        valid_names = ", ".join(DATASETS)
        raise ValueError(
            f"Unknown dataset '{dataset_name}'. "
            f"Valid datasets: {valid_names}"
        )

    return DATASETS[dataset_name]