"""
AWS Lambda: Flatten + explode nested order JSON (array) into 2 Parquet datasets in S3

Fix included:
- Drops partition columns (order_year, order_month) from the Parquet *file schema*
  because they are already present as Hive-style partitions in the S3 folder path.
  This prevents Athena/Glue "duplicate columns" / HIVE_INVALID_METADATA errors.

INPUT (raw):  S3 object containing a JSON ARRAY of orders.

OUTPUT (processed, Parquet datasets partitioned by order_year/order_month):
- s3://<bucket>/processed/store_sales/fact_orders/
- s3://<bucket>/processed/store_sales/fact_order_items/

Requires:
- pandas + pyarrow available in Lambda (layer or container).
"""

import json
import os
import tempfile
import urllib.parse
from datetime import datetime

import boto3
import pandas as pd

s3 = boto3.client("s3")

PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed/store_sales").rstrip("/")
PROCESSED_BUCKET_ENV = os.environ.get("PROCESSED_BUCKET")  # optional

# IMPORTANT: Partition columns must NOT be written inside Parquet files
# if you're using Hive-style partition folders like order_year=2025/order_month=1/
PARTITION_COLS = ["order_year", "order_month"]


def _parse_s3_event(event: dict) -> tuple[str, str]:
    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
    return bucket, key


def _safe_to_datetime(series: pd.Series) -> pd.Series:
    """
    Robust parser for mixed timestamp formats in a single column:
    - ISO8601 strings: "2025-01-01T12:00:00Z"
    - epoch in nanoseconds: 1735735513000000000
    - epoch in milliseconds: 1735735513000
    Returns UTC-aware datetime64[ns, UTC].
    """
    s = series.copy()

    # Work in string space so mixed object types don't break detection
    s_str = s.astype("string")

    # numeric-only values (digits)
    is_num = s_str.str.fullmatch(r"\d+").fillna(False)

    # parse numeric candidates to integers
    num = pd.to_numeric(s_str.where(is_num), errors="coerce")

    # ms if < 1e15, ns if >= 1e15 (2025 ns ~ 1e18)
    dt_ms = pd.to_datetime(num.where(num < 1e15), unit="ms", errors="coerce", utc=True)
    dt_ns = pd.to_datetime(num.where(num >= 1e15), unit="ns", errors="coerce", utc=True)
    dt_num = dt_ms.fillna(dt_ns)

    # parse non-numeric values as ISO/general strings
    dt_str = pd.to_datetime(s_str.where(~is_num), errors="coerce", utc=True)

    # combine
    return dt_str.fillna(dt_num)



def _write_parquet_and_upload(df: pd.DataFrame, bucket: str, key: str) -> None:
    """
    Writes df to a local parquet file in /tmp then uploads it to s3://bucket/key
    """
    if df is None or df.empty:
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, "data.parquet")
        df.to_parquet(local_path, index=False, compression="snappy")  # requires pyarrow
        s3.upload_file(local_path, bucket, key)


def lambda_handler(event, context):
    in_bucket, in_key = _parse_s3_event(event)
    out_bucket = PROCESSED_BUCKET_ENV or in_bucket

    # --- Read JSON array from S3 ---
    obj = s3.get_object(Bucket=in_bucket, Key=in_key)
    raw_bytes = obj["Body"].read()

    try:
        orders = json.loads(raw_bytes)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Input is not valid JSON array: s3://{in_bucket}/{in_key}") from e

    if not isinstance(orders, list) or len(orders) == 0:
        return {"status": "ok", "message": "No orders found", "input": f"s3://{in_bucket}/{in_key}"}

    # --- Normalize orders into DataFrame (top-level) ---
    df_orders_raw = pd.DataFrame(orders)

    # Required fields check (light)
    if "order_id" not in df_orders_raw.columns or "order_timestamp" not in df_orders_raw.columns:
        raise RuntimeError("Missing required fields: order_id and/or order_timestamp")

    # Parse timestamp and derive partition fields
    df_orders_raw["order_timestamp"] = _safe_to_datetime(df_orders_raw["order_timestamp"])
    df_orders_raw = df_orders_raw.dropna(subset=["order_timestamp"])

    df_orders_raw["order_date"] = df_orders_raw["order_timestamp"].dt.date.astype(str)  # YYYY-MM-DD
    df_orders_raw["order_year"] = df_orders_raw["order_timestamp"].dt.year.astype("int64")
    df_orders_raw["order_month"] = df_orders_raw["order_timestamp"].dt.month.astype("int64")

    # --- Flatten nested objects: customer.*, payment.* ---
    customer_df = (
        pd.json_normalize(df_orders_raw["customer"]).add_prefix("customer_")
        if "customer" in df_orders_raw.columns
        else pd.DataFrame()
    )

    payment_df = (
        pd.json_normalize(df_orders_raw["payment"]).add_prefix("payment_")
        if "payment" in df_orders_raw.columns
        else pd.DataFrame()
    )

    # Build FACT_ORDERS (one row per order)
    base_cols = [c for c in df_orders_raw.columns if c not in ("customer", "payment", "items")]
    df_fact_orders = pd.concat(
        [
            df_orders_raw[base_cols].reset_index(drop=True),
            customer_df.reset_index(drop=True),
            payment_df.reset_index(drop=True),
        ],
        axis=1,
    )

    if "order_total" in df_fact_orders.columns:
        df_fact_orders["order_total"] = pd.to_numeric(df_fact_orders["order_total"], errors="coerce")

    # --- Explode items[] into FACT_ORDER_ITEMS ---
    if "items" in df_orders_raw.columns:
        ctx_cols = ["order_id", "order_timestamp", "order_date", "order_year", "order_month"]
        df_items_ctx = df_orders_raw[ctx_cols + ["items"]].copy()

        df_items_ctx = df_items_ctx.explode("items", ignore_index=True)

        items_df = pd.json_normalize(df_items_ctx["items"])
        df_fact_order_items = pd.concat(
            [df_items_ctx.drop(columns=["items"]).reset_index(drop=True), items_df.reset_index(drop=True)],
            axis=1,
        )
    else:
        df_fact_order_items = pd.DataFrame()

    if not df_fact_order_items.empty:
        if "quantity" in df_fact_order_items.columns:
            df_fact_order_items["quantity"] = pd.to_numeric(df_fact_order_items["quantity"], errors="coerce")
        if "unit_price" in df_fact_order_items.columns:
            df_fact_order_items["unit_price"] = pd.to_numeric(df_fact_order_items["unit_price"], errors="coerce")
        if "quantity" in df_fact_order_items.columns and "unit_price" in df_fact_order_items.columns:
            df_fact_order_items["line_total"] = (
                df_fact_order_items["quantity"] * df_fact_order_items["unit_price"]
            ).round(2)

    # --- Write Parquet outputs partitioned by order_year/order_month ---
    partitions = df_fact_orders[PARTITION_COLS].drop_duplicates().to_dict("records")

    for p in partitions:
        y = int(p["order_year"])
        m = int(p["order_month"])

        orders_part = df_fact_orders[(df_fact_orders["order_year"] == y) & (df_fact_orders["order_month"] == m)].copy()
        items_part = (
            df_fact_order_items[(df_fact_order_items["order_year"] == y) & (df_fact_order_items["order_month"] == m)].copy()
            if not df_fact_order_items.empty
            else pd.DataFrame()
        )

        # DROP partition cols from file schema to avoid Glue/Athena duplicate columns
        orders_to_write = orders_part.drop(columns=PARTITION_COLS, errors="ignore")
        items_to_write = items_part.drop(columns=PARTITION_COLS, errors="ignore")

        run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + (context.aws_request_id if context else "local")

        orders_key = (
            f"{PROCESSED_PREFIX}/fact_orders/"
            f"order_year={y}/order_month={m}/"
            f"part-{run_id}.parquet"
        )
        items_key = (
            f"{PROCESSED_PREFIX}/fact_order_items/"
            f"order_year={y}/order_month={m}/"
            f"part-{run_id}.parquet"
        )

        _write_parquet_and_upload(orders_to_write, out_bucket, orders_key)
        _write_parquet_and_upload(items_to_write, out_bucket, items_key)

    return {
        "status": "ok",
        "input": f"s3://{in_bucket}/{in_key}",
        "output_bucket": out_bucket,
        "outputs": {
            "fact_orders_prefix": f"s3://{out_bucket}/{PROCESSED_PREFIX}/fact_orders/",
            "fact_order_items_prefix": f"s3://{out_bucket}/{PROCESSED_PREFIX}/fact_order_items/",
        },
        "rows": {
            "fact_orders": int(len(df_fact_orders)),
            "fact_order_items": int(len(df_fact_order_items)) if not df_fact_order_items.empty else 0,
        },
    }
