import os
import ssl
import urllib.parse

import awswrangler as wr
import pandas as pd
import pg8000.dbapi

# ––– DB Config –––
DB = dict(
    host=os.environ["DB_HOST"],
    port=int(os.environ.get("DB_PORT", "5432")),
    database=os.environ.get("DB_NAME", "postgres"),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
)

TABLE = "customer_aggregates"
COLUMNS = "(customer_id, country, n_orders, n_items, total_spend, first_purchase, last_purchase)"
ROW = "(%s, %s, %s, %s, %s, %s::timestamp, %s::timestamp)"
ON_CONFLICT = """
ON CONFLICT (customer_id) DO UPDATE SET
    country        = EXCLUDED.country,
    n_orders       = EXCLUDED.n_orders,
    n_items        = EXCLUDED.n_items,
    total_spend    = EXCLUDED.total_spend,
    first_purchase = EXCLUDED.first_purchase,
    last_purchase  = EXCLUDED.last_purchase,
    loaded_at      = now()
"""
BATCH_SIZE = 1000

# ––– Supabase connection via Session Pooler –––
# Relaxed verification for demo purposes
def connect():
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    return pg8000.dbapi.connect(ssl_context=ssl_ctx, **DB)

# –––  Transform dtype and aggregate transactions to one row per customer –––
def aggregate_customers(df):
    # awswrangler returns nullable dtypes; move to numpy and keep dates as ISO
    # strings so Postgres casts them (pd.to_datetime is unstable on this runtime).
    cols = ["InvoiceNo", "Quantity", "UnitPrice", "CustomerID", "Country", "InvoiceDate"]
    work = pd.DataFrame({c: df[c].to_numpy() for c in cols})
    for c in ["InvoiceNo", "Country", "InvoiceDate"]:
        work[c] = work[c].astype(object)
    work["Quantity"] = work["Quantity"].astype("int64")
    work["UnitPrice"] = work["UnitPrice"].astype("float64")
    work["CustomerID"] = work["CustomerID"].astype("float64")
    work["line_total"] = work["Quantity"] * work["UnitPrice"]

    return work.groupby("CustomerID").agg(
        country=("Country", "first"),
        n_orders=("InvoiceNo", "nunique"),
        n_items=("Quantity", "sum"),
        total_spend=("line_total", "sum"),
        first_purchase=("InvoiceDate", "min"),
        last_purchase=("InvoiceDate", "max"),
    ).reset_index()

# ––– Convert aggregated data to list of tuples –––
def to_rows(agg):
    return [
        (
            int(r.CustomerID),
            None if pd.isna(r.country) else str(r.country),
            int(r.n_orders),
            int(r.n_items),
            float(round(r.total_spend, 2)),
            str(r.first_purchase),
            str(r.last_purchase),
        )
        for r in agg.itertuples(index=False)
    ]

# ––– Upsert rows into Postgres in batches to avoid timeout –––
def upsert(conn, rows):
    cur = conn.cursor()
    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]
        values = ",".join([ROW] * len(batch))
        params = [value for row in batch for value in row]
        cur.execute(f"INSERT INTO {TABLE} {COLUMNS} VALUES {values} {ON_CONFLICT}", params)
    conn.commit()

# ––– Lambda handler (main function) –––
def lambda_handler(event, context):
    # 1. Resolve the parquet that triggered this run
    record = event["Records"][0]["s3"]
    bucket = record["bucket"]["name"]
    key = urllib.parse.unquote_plus(record["object"]["key"])

    # 2. Read and aggregate to one row per customer
    df = wr.s3.read_parquet(f"s3://{bucket}/{key}")
    rows = to_rows(aggregate_customers(df))

    # 3. Upsert into Postgres in batches
    conn = connect()
    try:
        upsert(conn, rows)
    finally:
        conn.close()

    print(f"rows_read={len(df)} customers_loaded={len(rows)}")
    return {"rows_read": len(df), "customers_loaded": len(rows)}
