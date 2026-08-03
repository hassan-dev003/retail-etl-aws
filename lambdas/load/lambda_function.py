import os
import io
import csv
import ssl
import datetime as dt
import urllib.parse

import awswrangler as wr
import pandas as pd
import pg8000.dbapi

# ––– CONSTANTS –––
DB = dict(
    host=os.environ["DB_HOST"],
    port=int(os.environ.get("DB_PORT", "5432")),
    database=os.environ.get("DB_NAME", "postgres"),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
)

BUCKET = os.environ["BUCKET"]
PROCESSED_KEY = os.environ.get("PROCESSED_KEY", "processed/online_retail_clean.parquet")

DIM_BATCH = 1000


# ––– Parse S3 event to determine source parquet –––
def resolve_source(event):
    # S3 trigger carries object; a scheduled event does not,
    # so we default to the latest processed file.
    records = event.get("Records") if isinstance(event, dict) else None
    if records:
        s3 = records[0]["s3"]
        return s3["bucket"]["name"], urllib.parse.unquote_plus(s3["object"]["key"])
    return BUCKET, PROCESSED_KEY


# ––– Supabase connection via Session Pooler –––
# Relaxed verification for demo purposes
def connect():
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    return pg8000.dbapi.connect(ssl_context=ssl_ctx, **DB)


# –––  Read & Normalize dtypes –––
def read_and_normalize(bucket, key):
    # awswrangler returns nullable dtypes; move to numpy and keep dates as ISO
    # strings so Postgres casts them (pd.to_datetime is unstable on this runtime).
    df = wr.s3.read_parquet(f"s3://{bucket}/{key}")
    cols = [
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "UnitPrice", "CustomerID", "Country", "InvoiceDate"
    ]
    work = pd.DataFrame({c: df[c].to_numpy() for c in cols})
    for c in ["InvoiceNo", "StockCode", "Description", "Country", "InvoiceDate"]:
        work[c] = work[c].astype(object)
    work["Quantity"] = work["Quantity"].astype("int64")
    work["UnitPrice"] = work["UnitPrice"].astype("float64")
    work["CustomerID"] = work["CustomerID"].astype("float64")
    work["line_total"] = work["Quantity"] * work["UnitPrice"]
    work["date_str"] = work["InvoiceDate"].str.slice(0, 10)
    work["date_key"] = work["date_str"].str.replace("-", "", regex=False).astype("int64")

    return work


# ––– Aggregate line items to customer level –––
def insert_batched(cur, table, columns, placeholder, rows, batch_size, suffix=""):
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        values = ",".join([placeholder] * len(batch))
        params = [value for row in batch for value in row]
        cur.execute(f"INSERT INTO {table} {columns} VALUES {values} {suffix}", params)


# ––– Upsert dimensions into Postgres –––
def upsert_dimensions(cur, work):
    customers = work.groupby("CustomerID")["Country"].first().reset_index()
    customer_rows = [
        (int(cid), None if pd.isna(c) else str(c))
        for cid, c in customers.itertuples(index=False)
    ]
    insert_batched(
        cur, "dim_customer", "(customer_id, country)", "(%s,%s)",
        customer_rows, DIM_BATCH, "ON CONFLICT (customer_id) DO NOTHING"
    )

    products = work.groupby("StockCode")["Description"].first().reset_index()
    product_rows = [
        (str(sc), None if pd.isna(d) else str(d))
        for sc, d in products.itertuples(index=False)
    ]
    insert_batched(
        cur, "dim_product", "(stock_code, description)", "(%s, %s)",
        product_rows, DIM_BATCH, "ON CONFLICT (stock_code) DO NOTHING"
    )

    dates = work[["date_key", "date_str"]].drop_duplicates()
    date_rows = []
    for date_key, date_str in dates.itertuples(index=False):
        d = dt.date.fromisoformat(date_str)
        date_rows.append((int(date_key), date_str, d.year, d.month, d.day, d.weekday()))
    insert_batched(
        cur, "dim_date", "(date_key, full_date, year, month, day, day_of_week)", "(%s, %s::date, %s, %s, %s, %s)",
        date_rows, DIM_BATCH, "ON CONFLICT (date_key) DO NOTHING"
    )

    return len(customer_rows), len(product_rows), len(date_rows)


# --- Mapping keys ---
def key_maps(cur):
    cur.execute("SELECT customer_id, customer_key FROM dim_customer")
    customer_map = {int(cid): int(k) for cid, k in cur.fetchall()}
    cur.execute("SELECT stock_code, product_key FROM dim_product")
    product_map = {str(sc): int(k) for sc, k in cur.fetchall()}
    return customer_map, product_map


# --- Building Facts ---
def build_facts(work, customer_map, product_map):
    return [
        (
            str(inv),
            customer_map[int(cid)],
            product_map[str(sc)],
            int(dk),
            int(q),
            float(round(up, 2)),
            float(round(lt, 2)),
        )
        for inv, cid, sc, dk, q, up, lt in zip(
            work["InvoiceNo"], work["CustomerID"], work["StockCode"], work["date_key"],
            work["Quantity"], work["UnitPrice"], work["line_total"],
        )
    ]


# ––– Load facts into Postgres –––
def copy_facts(cur, facts):
    # Bulk-load the fact table w COPY (one streamed operation)
    # instead of thousands of parameterized inserts
    text = io.StringIO()
    csv.writer(text).writerows(facts)
    stream = io.BytesIO(text.getvalue().encode("utf-8"))
    cur.execute(
        "COPY fact_sales "
        "(invoice_no, customer_key, product_key, date_key, quantity, unit_price, line_total) "
        "FROM STDIN WITH (FORMAT csv)",
        stream=stream
    )


# --- Main Process ---
def lambda_handler(event, context):
    # 1. Resolve the source (S3 upload or scheduled refresh)
    bucket, key = resolve_source(event)
    work = read_and_normalize(bucket, key)

    conn = connect()
    try:
        cur = conn.cursor()

        # 2. Upsert dimensions, then resolve natural keys -> surrogate key
        n_cust, n_prod, n_date = upsert_dimensions(cur, work)
        customer_map, product_map = key_maps(cur)
        facts = build_facts(work, customer_map, product_map)

        # 3. Full refresh of the fact table via COPY
        cur.execute("TRUNCATE fact_sales")
        copy_facts(cur, facts)
        conn.commit()
    finally:
        conn.close()

    print(f"facts_loaded={len(facts)} customers={n_cust} products={n_prod} date={n_date}")
    return {"facts_loaded": len(facts), "customers": n_cust, "products": n_prod, "dates": n_date}
    
