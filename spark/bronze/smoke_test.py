from __future__ import annotations

import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from spark.common.spark_session import create_spark_session


def main() -> None:
    spark = create_spark_session(
        "customer360-delta-minio-smoke-test"
    )

    bucket = os.getenv("MINIO_BUCKET", "customer360")
    target = f"s3a://{bucket}/bronze/_smoke_test"

    try:
        source = spark.range(1, 11)

        (
            source.write
            .format("delta")
            .mode("overwrite")
            .save(target)
        )

        result = (
            spark.read
            .format("delta")
            .load(target)
        )

        count = result.count()

        if count != 10:
            raise RuntimeError(
                f"Smoke test expected 10 rows but found {count}."
            )

        result.orderBy("id").show(10, truncate=False)

        print("Spark → MinIO → Delta smoke test passed.")
        print(f"Target: {target}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()