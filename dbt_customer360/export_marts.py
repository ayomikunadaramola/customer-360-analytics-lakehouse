from __future__ import annotations

from pathlib import Path

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
    output_directory = Path("/usr/app/dbt/dashboard_exports")
    output_directory.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect("customer360.duckdb", read_only=True)

    try:
        for table_name in MARTS:
            csv_path = output_directory / f"{table_name}.csv"
            parquet_path = output_directory / f"{table_name}.parquet"

            connection.execute(
                f"""
                COPY (
                    SELECT *
                    FROM analytics.{table_name}
                )
                TO '{csv_path.as_posix()}'
                (
                    HEADER,
                    DELIMITER ','
                )
                """
            )

            connection.execute(
                f"""
                COPY (
                    SELECT *
                    FROM analytics.{table_name}
                )
                TO '{parquet_path.as_posix()}'
                (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD
                )
                """
            )

            count = connection.execute(
                f"""
                SELECT COUNT(*)
                FROM analytics.{table_name}
                """
            ).fetchone()[0]

            print(
                f"Exported {table_name}: "
                f"{count:,} rows to CSV and Parquet"
            )

    finally:
        connection.close()


if __name__ == "__main__":
    main()