FROM python:3.12-slim

WORKDIR /usr/app/dbt

RUN pip install --no-cache-dir \
    dbt-core==1.11.12 \
    dbt-duckdb==1.10.1

CMD ["sleep", "infinity"]