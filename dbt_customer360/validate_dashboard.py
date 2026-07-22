import duckdb

connection = duckdb.connect(
    "customer360.duckdb",
    read_only=True,
)

queries = {
    "Total Revenue": """
        SELECT SUM(gross_order_value)
        FROM analytics.fct_daily_sales
    """,
    "Total Orders": """
        SELECT SUM(total_orders)
        FROM analytics.fct_daily_sales
    """,
    "Total Customers": """
        SELECT COUNT(DISTINCT customer_id)
        FROM analytics.dim_customer_segments
    """,
    "Total Product Revenue": """
        SELECT SUM(product_revenue)
        FROM analytics.fct_product_performance
    """,
    "Total Tickets": """
        SELECT SUM(total_tickets)
        FROM analytics.fct_support_performance
    """,
    "Total Web Events": """
        SELECT SUM(total_events)
        FROM analytics.fct_web_engagement
    """,
}

try:
    for label, query in queries.items():
        value = connection.execute(query).fetchone()[0]
        print(f"{label:<25} {value}")
finally:
    connection.close()