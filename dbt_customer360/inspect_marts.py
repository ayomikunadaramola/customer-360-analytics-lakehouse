from __future__ import annotations

import duckdb


MARTS = [
    "dim_customer_segments",
    "fct_daily_sales",
    "fct_product_performance",
    "fct_marketing_performance",
    "fct_support_performance",
    "fct_web_engagement",
]


def main() -> None:
    connection = duckdb.connect("customer360.duckdb", read_only=True)

    try:
        print("=" * 72)
        print("CUSTOMER 360 MART VALIDATION")
        print("=" * 72)

        for table_name in MARTS:
            row_count = connection.execute(
                f"""
                SELECT COUNT(*)
                FROM analytics.{table_name}
                """
            ).fetchone()[0]

            print(f"{table_name:<35} {row_count:>10,} rows")

        print("\nTop 10 customers by revenue:")
        rows = connection.execute(
            """
            SELECT
                customer_id,
                customer_email,
                customer_value_segment,
                purchase_frequency_segment,
                total_revenue
            FROM analytics.dim_customer_segments
            ORDER BY total_revenue DESC
            LIMIT 10
            """
        ).fetchall()

        for row in rows:
            print(row)

    finally:
        connection.close()


if __name__ == "__main__":
    main()