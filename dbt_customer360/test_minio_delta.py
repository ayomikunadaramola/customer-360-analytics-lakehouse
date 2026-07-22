import duckdb
import os

con = duckdb.connect("customer360.duckdb")

con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

con.execute("INSTALL delta")
con.execute("LOAD delta")

con.execute("""
DROP SECRET IF EXISTS minio_secret
""")

con.execute(f"""
CREATE SECRET minio_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID '{os.environ["MINIO_ACCESS_KEY"]}',
    SECRET '{os.environ["MINIO_SECRET_KEY"]}',
    REGION 'us-east-1',
    ENDPOINT 'minio:9000',
    URL_STYLE 'path',
    USE_SSL false,
    SCOPE 's3://{os.environ["MINIO_BUCKET"]}'
)
""")

print("Listing Delta log...")

print(
    con.execute("""
    SELECT file
    FROM glob(
        's3://customer360/gold/customer_360/_delta_log/*'
    )
    LIMIT 5
    """).fetchall()
)

print("Reading Delta table...")

print(
    con.execute("""
    SELECT COUNT(*)
    FROM delta_scan(
        's3://customer360/gold/customer_360'
    )
    """).fetchone()
)

con.close()