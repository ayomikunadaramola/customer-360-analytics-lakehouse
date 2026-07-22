from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.bronze.datasets import DATASETS, BronzeDataset
from spark.common.spark_session import create_spark_session


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest Customer 360 landing CSV files "
            "into Bronze Delta tables."
        )
    )

    parser.add_argument(
        "--dataset",
        default="all",
        choices=["all", *DATASETS.keys()],
        help="Dataset to ingest, or 'all'.",
    )

    parser.add_argument(
        "--load-date",
        required=True,
        help="Source load date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--batch-id",
        default=None,
        help="Optional externally supplied pipeline batch ID.",
    )

    return parser.parse_args()


def validate_load_date(load_date: str) -> None:
    try:
        datetime.strptime(load_date, "%Y-%m-%d")
    except ValueError as error:
        raise ValueError(
            "--load-date must use YYYY-MM-DD format."
        ) from error


def add_bronze_metadata(
    dataframe: DataFrame,
    dataset: BronzeDataset,
    batch_id: str,
    load_date: str,
) -> DataFrame:
    source_columns = dataframe.columns

    hash_columns = [
        F.coalesce(
            F.col(column).cast("string"),
            F.lit(""),
        )
        for column in source_columns
    ]

    return (
        dataframe
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "_source_file",
            F.input_file_name(),
        )
        .withColumn(
            "_source_system",
            F.lit(dataset.source_system),
        )
        .withColumn(
            "_dataset_name",
            F.lit(dataset.name),
        )
        .withColumn(
            "_batch_id",
            F.lit(batch_id),
        )
        .withColumn(
            "_load_date",
            F.to_date(F.lit(load_date)),
        )
        .withColumn(
            "_record_hash",
            F.sha2(
                F.concat_ws("||", *hash_columns),
                256,
            ),
        )
    )


def ingest_dataset(
    spark: SparkSession,
    dataset: BronzeDataset,
    bucket: str,
    batch_id: str,
    load_date: str,
) -> dict[str, object]:
    source_key = dataset.source_path_template.format(
        load_date=load_date
    )

    source_path = f"s3a://{bucket}/{source_key}"
    target_path = f"s3a://{bucket}/{dataset.target_path}"

    print("\n" + "=" * 72)
    print(f"Dataset: {dataset.name}")
    print(f"Source:  {source_path}")
    print(f"Target:  {target_path}")
    print("=" * 72)

    raw_dataframe = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .schema(dataset.schema)
        .load(source_path)
    )

    source_count = raw_dataframe.count()

    if source_count == 0:
        raise ValueError(
            f"No source records found for {dataset.name} "
            f"at {source_path}"
        )

    bronze_dataframe = add_bronze_metadata(
        dataframe=raw_dataframe,
        dataset=dataset,
        batch_id=batch_id,
        load_date=load_date,
    )

    # Rerunning a load date replaces only that date's partition.
    (
        bronze_dataframe.write
        .format("delta")
        .mode("overwrite")
        .option(
            "replaceWhere",
            f"_load_date = DATE '{load_date}'",
        )
        .partitionBy("_load_date")
        .save(target_path)
    )

    written_count = (
        spark.read
        .format("delta")
        .load(target_path)
        .filter(F.col("_load_date") == F.to_date(F.lit(load_date)))
        .count()
    )

    if written_count != source_count:
        raise RuntimeError(
            f"Row-count validation failed for {dataset.name}: "
            f"source={source_count:,}, written={written_count:,}"
        )

    print(
        f"SUCCESS: {dataset.name} "
        f"source={source_count:,}, written={written_count:,}"
    )

    return {
        "dataset": dataset.name,
        "source_count": source_count,
        "written_count": written_count,
        "target_path": target_path,
    }


def main() -> None:
    args = parse_arguments()
    validate_load_date(args.load_date)

    batch_id = args.batch_id or str(uuid4())

    spark = create_spark_session(
        app_name=(
            f"customer360-bronze-{args.dataset}-"
            f"{args.load_date}"
        )
    )

    bucket = (
        spark.sparkContext.getConf()
        .get("spark.customer360.bucket", "")
    )

    if not bucket:
        import os
        bucket = os.getenv("MINIO_BUCKET", "customer360")

    selected_datasets = (
        DATASETS.values()
        if args.dataset == "all"
        else [DATASETS[args.dataset]]
    )

    started_at = datetime.now(timezone.utc)
    results: list[dict[str, object]] = []

    try:
        print("Customer 360 Bronze ingestion started")
        print(f"Batch ID:  {batch_id}")
        print(f"Load date: {args.load_date}")
        print(f"Started:   {started_at.isoformat()}")

        for dataset in selected_datasets:
            result = ingest_dataset(
                spark=spark,
                dataset=dataset,
                bucket=bucket,
                batch_id=batch_id,
                load_date=args.load_date,
            )
            results.append(result)

        print("\n" + "=" * 72)
        print("BRONZE INGESTION SUMMARY")
        print("=" * 72)

        total_source = 0
        total_written = 0

        for result in results:
            source_count = int(result["source_count"])
            written_count = int(result["written_count"])

            total_source += source_count
            total_written += written_count

            print(
                f"{result['dataset']:<24} "
                f"source={source_count:>10,} "
                f"written={written_count:>10,}"
            )

        print("-" * 72)
        print(
            f"{'TOTAL':<24} "
            f"source={total_source:>10,} "
            f"written={total_written:>10,}"
        )
        print("=" * 72)
        print("Bronze ingestion completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()