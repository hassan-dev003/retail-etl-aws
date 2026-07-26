import awswrangler as wr
import urllib.parse

def lambda_handler(event, context):
    # 1. What file triggered us?
    s3 = event["Records"][0]["s3"]
    bucket = s3["bucket"]["name"]
    key = urllib.parse.unquote_plus(s3["object"]["key"])
    print(f"Triggered by s3://{bucket}/{key}")

    # 2. Read the raw CSV from S3.
    # UCI Online Retail is Latin-1, NOT utf-8 – without this, it crashes.
    df = wr.s3.read_csv(f"s3://{bucket}/{key}", encoding="latin-1")
    rows_in = len(df)

    # 3. Cleaning logic
    df = df.dropna(subset=["CustomerID"])
    df = df[df["Quantity"] > 0]
    df = df[df["UnitPrice"] > 0]
    df = df[~df["InvoiceNo"].astype(str).str.startswith("C")] # drop cancellations
    rows_out = len(df)

    # 4. Write cleaned Parquet to processed/
    out_path = f"s3://{bucket}/processed/online_retail_clean.parquet"
    wr.s3.to_parquet(df=df, path=out_path, index=False)

    # 5. Monitoring
    rejected = rows_in - rows_out
    print(f"rows_in={rows_in} rows_out={rows_out} rows_rejected={rejected}")
    return {"rows_in": rows_in, "rows_out": rows_out, "rows_rejected": rejected}
