from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pandas as pd
from faker import Faker


fake = Faker()
random.seed(42)
Faker.seed(42)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LANDING_ROOT = PROJECT_ROOT / "lakehouse" / "landing"

CITIES = [
    "Lagos",
    "Abuja",
    "Ibadan",
    "Port Harcourt",
    "Benin City",
    "Enugu",
]

PRODUCT_CATEGORIES = [
    "Electronics",
    "Fashion",
    "Home",
    "Beauty",
    "Sports",
    "Books",
]

SALES_CHANNELS = ["Web", "Mobile App", "Retail Store"]
MARKETING_CHANNELS = ["Email", "Social", "Search", "Display", "Affiliate"]
WEB_EVENT_TYPES = [
    "Page View",
    "Product View",
    "Add to Cart",
    "Checkout",
    "Purchase",
]


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic Customer 360 source datasets."
    )

    parser.add_argument(
        "--customers",
        type=int,
        default=2_000,
        help="Number of customers to generate.",
    )

    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of historical days covered by generated activity.",
    )

    parser.add_argument(
        "--load-date",
        default=datetime.now().date().isoformat(),
        help="Logical load date in YYYY-MM-DD format.",
    )

    return parser.parse_args()


def create_output_directories(load_date: str) -> dict[str, Path]:
    directories = {
        "crm": LANDING_ROOT / "crm" / f"load_date={load_date}",
        "commerce": LANDING_ROOT / "commerce" / f"load_date={load_date}",
        "support": LANDING_ROOT / "support" / f"load_date={load_date}",
        "marketing": LANDING_ROOT / "marketing" / f"load_date={load_date}",
        "web": LANDING_ROOT / "web" / f"load_date={load_date}",
    }

    for directory in directories.values():
        directory.mkdir(parents=True, exist_ok=True)

    return directories


def random_timestamp(days: int) -> datetime:
    now = datetime.now()

    return now - timedelta(
        days=random.randint(0, days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )


def generate_customers(
    customer_count: int,
    days: int,
) -> pd.DataFrame:
    records: list[dict] = []

    for number in range(1, customer_count + 1):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email().lower()

        customer = {
            "customer_id": f"CRM-{number:07d}",
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": fake.phone_number() if random.random() > 0.08 else None,
            "date_of_birth": fake.date_of_birth(
                minimum_age=18,
                maximum_age=80,
            ),
            "city": random.choice(CITIES),
            "country": "Nigeria",
            "signup_date": fake.date_between(
                start_date="-3y",
                end_date="today",
            ),
            "account_status": random.choices(
                ["Active", "Suspended", "Closed"],
                weights=[0.88, 0.07, 0.05],
                k=1,
            )[0],
            "updated_at": random_timestamp(days),
        }

        records.append(customer)

        # Add some intentionally inconsistent duplicate records.
        if random.random() < 0.03:
            duplicate = customer.copy()
            duplicate["first_name"] = first_name.upper()
            duplicate["email"] = email.upper()
            duplicate["updated_at"] = random_timestamp(days)
            records.append(duplicate)

    return pd.DataFrame(records)


def generate_products(product_count: int = 500) -> pd.DataFrame:
    records: list[dict] = []

    for number in range(1, product_count + 1):
        category = random.choice(PRODUCT_CATEGORIES)
        unit_price = round(random.uniform(2_000, 500_000), 2)

        records.append(
            {
                "product_id": f"PRD-{number:06d}",
                "product_name": f"{fake.word().title()} {category} Product",
                "category": category,
                "unit_price": unit_price,
                "currency": "NGN",
                "is_active": random.random() > 0.05,
            }
        )

    return pd.DataFrame(records)


def generate_commerce(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    customer_ids = customers["customer_id"].drop_duplicates().tolist()
    product_records = products.to_dict("records")

    order_count = len(customer_ids) * 5

    orders: list[dict] = []
    order_items: list[dict] = []
    payments: list[dict] = []

    for order_number in range(1, order_count + 1):
        order_id = f"ORD-{order_number:09d}"
        customer_id = random.choice(customer_ids)
        order_date = random_timestamp(days)
        status = random.choices(
            [
                "Created",
                "Confirmed",
                "Shipped",
                "Delivered",
                "Cancelled",
                "Returned",
            ],
            weights=[0.05, 0.10, 0.15, 0.60, 0.07, 0.03],
            k=1,
        )[0]

        selected_products = random.sample(
            product_records,
            k=random.randint(1, 4),
        )

        subtotal = 0.0

        for item_number, product in enumerate(selected_products, start=1):
            quantity = random.randint(1, 4)
            unit_price = float(product["unit_price"])
            line_amount = round(quantity * unit_price, 2)
            subtotal += line_amount

            order_items.append(
                {
                    "order_item_id": (
                        f"ITEM-{order_number:09d}-{item_number:02d}"
                    ),
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "product_name": product["product_name"],
                    "category": product["category"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_amount": line_amount,
                }
            )

        discount_amount = round(
            subtotal * random.choice([0, 0, 0.05, 0.10, 0.15]),
            2,
        )
        shipping_amount = round(random.uniform(0, 15_000), 2)
        order_total = round(
            subtotal - discount_amount + shipping_amount,
            2,
        )

        # Deliberately introduce a small number of invalid values.
        if random.random() < 0.005:
            order_total = -order_total

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date,
                "order_status": status,
                "sales_channel": random.choice(SALES_CHANNELS),
                "currency": "NGN",
                "discount_amount": discount_amount,
                "shipping_amount": shipping_amount,
                "order_total": order_total,
                "updated_at": order_date + timedelta(
                    hours=random.randint(1, 72)
                ),
            }
        )

        payment_status = random.choices(
            ["Successful", "Failed", "Pending", "Refunded"],
            weights=[0.80, 0.10, 0.06, 0.04],
            k=1,
        )[0]

        payments.append(
            {
                "payment_id": f"PAY-{order_number:09d}",
                "order_id": order_id,
                "customer_id": customer_id,
                "payment_date": order_date + timedelta(
                    minutes=random.randint(1, 180)
                ),
                "payment_method": random.choice(
                    ["Card", "Bank Transfer", "Wallet", "USSD"]
                ),
                "amount": abs(order_total),
                "payment_status": payment_status,
                "gateway": random.choice(
                    ["Paystack", "Flutterwave", "Interswitch"]
                ),
                "failure_reason": (
                    random.choice(
                        [
                            "Insufficient funds",
                            "Gateway timeout",
                            "Card declined",
                        ]
                    )
                    if payment_status == "Failed"
                    else None
                ),
            }
        )

    return (
        pd.DataFrame(orders),
        pd.DataFrame(order_items),
        pd.DataFrame(payments),
    )


def generate_support(
    customers: pd.DataFrame,
    days: int,
) -> pd.DataFrame:
    base_customers = customers.drop_duplicates("customer_id")
    customer_records = base_customers.to_dict("records")

    ticket_count = max(500, len(base_customers) * 2)
    records: list[dict] = []

    for number in range(1, ticket_count + 1):
        customer = random.choice(customer_records)
        created_at = random_timestamp(days)

        status = random.choice(
            ["Open", "In Progress", "Resolved", "Closed"]
        )

        records.append(
            {
                "ticket_id": f"TKT-{number:08d}",
                "customer_email": (
                    customer["email"].upper()
                    if random.random() < 0.08
                    else customer["email"]
                ),
                "customer_phone": customer["phone"],
                "created_at": created_at,
                "resolved_at": (
                    created_at
                    + timedelta(hours=random.randint(1, 240))
                    if status in {"Resolved", "Closed"}
                    else None
                ),
                "priority": random.choice(
                    ["Low", "Medium", "High", "Critical"]
                ),
                "category": random.choice(
                    [
                        "Payment",
                        "Delivery",
                        "Account",
                        "Refund",
                        "Product",
                    ]
                ),
                "status": status,
                "satisfaction_score": (
                    random.randint(1, 5)
                    if status in {"Resolved", "Closed"}
                    else None
                ),
            }
        )

    return pd.DataFrame(records)


def generate_marketing(
    customers: pd.DataFrame,
    days: int,
) -> pd.DataFrame:
    emails = customers["email"].drop_duplicates().tolist()
    event_count = max(5_000, len(emails) * 8)

    records: list[dict] = []

    for number in range(1, event_count + 1):
        email = random.choice(emails)

        if random.random() < 0.05:
            email = email.upper()

        if random.random() < 0.02:
            email = fake.email().lower()

        channel = random.choice(MARKETING_CHANNELS)
        event_type = random.choices(
            ["Impression", "Click", "Conversion", "Unsubscribe"],
            weights=[0.65, 0.25, 0.08, 0.02],
            k=1,
        )[0]

        records.append(
            {
                "event_id": str(uuid4()),
                "customer_email": email,
                "campaign_id": f"CMP-{random.randint(1, 50):04d}",
                "campaign_name": (
                    f"{channel} Campaign {random.randint(1, 50)}"
                ),
                "channel": channel,
                "event_type": event_type,
                "event_timestamp": random_timestamp(days),
                "campaign_cost": round(random.uniform(50, 5_000), 2),
            }
        )

    return pd.DataFrame(records)


def generate_web_events(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    days: int,
) -> pd.DataFrame:
    customer_ids = customers["customer_id"].drop_duplicates().tolist()
    product_ids = products["product_id"].tolist()

    event_count = max(10_000, len(customer_ids) * 15)

    records: list[dict] = []

    for _ in range(event_count):
        logged_in = random.random() > 0.30
        customer_id = (
            random.choice(customer_ids) if logged_in else None
        )

        event_type = random.choice(WEB_EVENT_TYPES)

        records.append(
            {
                "event_id": str(uuid4()),
                "anonymous_id": str(uuid4()),
                "customer_id": customer_id,
                "session_id": str(uuid4()),
                "event_type": event_type,
                "page_url": random.choice(
                    [
                        "/",
                        "/products",
                        "/cart",
                        "/checkout",
                        "/account",
                    ]
                ),
                "product_id": (
                    random.choice(product_ids)
                    if event_type
                    in {"Product View", "Add to Cart", "Purchase"}
                    else None
                ),
                "event_timestamp": random_timestamp(days),
                "device_type": random.choice(
                    ["Mobile", "Desktop", "Tablet"]
                ),
                "traffic_source": random.choice(
                    [
                        "Direct",
                        "Organic Search",
                        "Paid Search",
                        "Social",
                        "Email",
                    ]
                ),
            }
        )

    return pd.DataFrame(records)


def save_dataframe(
    dataframe: pd.DataFrame,
    path: Path,
) -> None:
    dataframe.to_csv(path, index=False)
    print(f"Created {path.name}: {len(dataframe):,} rows")
    print(f"Location: {path}")


def main() -> None:
    args = parse_arguments()
    output_dirs = create_output_directories(args.load_date)

    print("Starting Customer 360 data generation...")
    print(f"Load date: {args.load_date}")

    customers = generate_customers(
        customer_count=args.customers,
        days=args.days,
    )

    products = generate_products()

    orders, order_items, payments = generate_commerce(
        customers=customers,
        products=products,
        days=args.days,
    )

    support = generate_support(customers, args.days)
    marketing = generate_marketing(customers, args.days)

    web_events = generate_web_events(
        customers=customers,
        products=products,
        days=args.days,
    )

    save_dataframe(
        customers,
        output_dirs["crm"] / "customers.csv",
    )

    save_dataframe(
        products,
        output_dirs["commerce"] / "products.csv",
    )

    save_dataframe(
        orders,
        output_dirs["commerce"] / "orders.csv",
    )

    save_dataframe(
        order_items,
        output_dirs["commerce"] / "order_items.csv",
    )

    save_dataframe(
        payments,
        output_dirs["commerce"] / "payments.csv",
    )

    save_dataframe(
        support,
        output_dirs["support"] / "support_tickets.csv",
    )

    save_dataframe(
        marketing,
        output_dirs["marketing"] / "marketing_events.csv",
    )

    save_dataframe(
        web_events,
        output_dirs["web"] / "web_events.csv",
    )

    print("\nCustomer 360 data generation completed successfully.")


if __name__ == "__main__":
    main()