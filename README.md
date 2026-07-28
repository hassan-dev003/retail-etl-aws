# retail-etl-aws

An event-driven ETL pipeline on AWS that ingests and cleans the UCI Online
Retail dataset (541,909 rows) and loads customer-level aggregates into Postgres.
Rebuilt from an original pandas/university version to run on real cloud
infrastructure — the point of the project was to learn what changes when you
move a laptop pipeline onto AWS.

## Architecture

```
raw CSV
   │ upload
   ▼
S3 raw/ ──event──▶ Lambda: clean ──▶ S3 processed/ ──event──┐
                                                            ▼
                        EventBridge (daily cron) ─────▶ Lambda: load ──▶ Postgres (Supabase)
```

The load Lambda has two triggers: a new Parquet in `processed/` (event-driven,
loads fresh data) and a daily EventBridge schedule (refreshes from the latest
processed file). It handles both event shapes.

## Pipeline stages

### 1. Ingestion — S3, event-driven
A CSV uploaded to the `raw/` prefix emits an S3 object-created event that
triggers the cleaning Lambda. The trigger is scoped to the `raw/` prefix and
`.csv` suffix so the Lambda never re-triggers on the output it writes to
`processed/` (which would otherwise loop forever).

### 2. Clean — Lambda
`lambdas/clean/lambda_function.py` reads the raw CSV, drops rows with a missing
CustomerID, removes negative quantities and prices, and drops cancelled orders
(InvoiceNo beginning with "C"), then writes a cleaned Parquet file to
`processed/`. Runtime: Python 3.12 with the AWS-managed AWSSDKPandas layer
(pandas + pyarrow). Every run logs row counts.

Latest run: **541,909 in → 397,884 out; 144,025 rejected.**

### 3. Load — Postgres
`lambdas/load/lambda_function.py` reads the cleaned Parquet, aggregates the
transactions to one row per customer (order count, item count, total spend,
first/last purchase), and upserts the ~4,338 rows into a `customer_aggregates`
table in Supabase. Writes are batched into multi-row inserts and use
`ON CONFLICT` so re-runs are idempotent. Connects with pg8000 (bundled in the
AWSSDKPandas layer) over the Supabase session pooler for IPv4 reachability from
Lambda.

Implementation note: the runtime's pandas returns nullable dtypes (`Int64`,
`string`), and `pd.to_datetime` crashes on them here, so dates are kept as ISO
strings and cast to `timestamp` server-side (`%s::timestamp`).

### 4. Scheduling & monitoring
A daily EventBridge rule invokes the load Lambda to refresh the aggregates,
alongside the event-driven trigger. Each run logs `rows_read` / `customers_loaded`
(load) and `rows_in` / `rows_out` / `rows_rejected` (clean) to CloudWatch, and
the driver script surfaces the clean counts after a run.

## Running it

```bash
# Run the pipeline
./scripts/run_etl.sh data/Online_Retail.csv

# Show the plan only
./scripts/run_etl.sh --dry-run data/Online_Retail.csv
```

The driver uploads the file, polls `processed/` until a *new* cleaned Parquet
appears, and prints the run's row counts (pulled from CloudWatch). Requires the
AWS CLI configured with an IAM user that has S3 access.

## What happens when a stage fails

- **Wrong file type in `raw/`** — the trigger's `.csv` suffix filter ignores it;
  nothing runs.
- **Malformed CSV** — the clean Lambda errors and logs to CloudWatch, and
  `processed/` is not updated. The driver polls for a *new* processed object
  (by timestamp), so this surfaces as a timeout rather than a false success.
- **Load failure (DB connection or SQL error)** — the load Lambda raises and
  logs the traceback to CloudWatch. The upsert commits once at the end, so a
  mid-load failure leaves `customer_aggregates` unchanged (no partial writes),
  and `ON CONFLICT` makes a retry safe.

## Cost

Runs within AWS's always-free monthly limits — a few MB in S3 and a handful of
Lambda invocations, well under the 1M free-request tier. The database is
Supabase (free tier). Batching the load into multi-row inserts cut a
cross-region write (Lambda in `ap-southeast-1`, Supabase pooler in
`ap-northeast-1`) from ~5 min to ~2 s.

## Planned (Day 3)

Deferred, not yet built: a fact/dimension star schema (`sql/schema.sql`), an
SQS queue decoupling the clean and load stages, and an architecture diagram
under `docs/`.

## Data

The raw dataset is not committed (~48 MB). Download the UCI Online Retail
dataset and place it at `data/Online_Retail.csv`.
