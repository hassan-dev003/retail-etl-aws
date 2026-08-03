# retail-etl-aws

An event-driven ETL pipeline on AWS that ingests and cleans the UCI Online
Retail dataset (541,909 rows) and loads it into a star-schema warehouse in
Postgres. Rebuilt from an original pandas/university version to run on real
cloud infrastructure — the point of the project was to learn what changes when
you move a laptop pipeline onto AWS.

## Architecture

```
raw CSV ─▶ S3 raw/ ─event▶ Lambda: clean ─▶ S3 processed/ ─event▶ SQS ─▶ Lambda: load ─▶ Postgres star schema
                                                                          ▲
                                                EventBridge (daily) ──────┘
```

See [`docs/architecture.md`](docs/architecture.md) for the rendered diagram.
The load Lambda has two triggers — the SQS queue (event-driven) and a daily
EventBridge schedule — and handles both event shapes.

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

### 3. Queue — SQS (decoupling)
The Parquet write to `processed/` emits an event to an SQS queue rather than
invoking the load Lambda directly. The queue decouples the two stages: it
buffers events if the consumer is busy, retries a failed delivery, and (with a
dead-letter queue attached) can isolate messages that keep failing. The load
Lambda polls the queue.

### 4. Load — star schema (Postgres)
`lambdas/load/lambda_function.py` reads the cleaned Parquet, upserts the three
dimensions (`dim_customer`, `dim_product`, `dim_date`), resolves each line's
natural keys to surrogate keys, and bulk-loads ~397,884 rows into `fact_sales`
with `COPY`. The fact table is truncate-and-reloaded, so re-runs are idempotent.
`customer_aggregates` is exposed as a view over the star schema. Connects with
pg8000 (bundled in the AWSSDKPandas layer) over the Supabase session pooler for
IPv4 reachability from Lambda. See [`sql/schema.sql`](sql/schema.sql).

Implementation note: the runtime's pandas returns nullable dtypes (`Int64`,
`string`), and `pd.to_datetime` crashes on them here, so dates are kept as ISO
strings and `date_key` is derived as `YYYYMMDD` without datetime parsing.

### 5. Scheduling & monitoring
A daily EventBridge rule also invokes the load Lambda to refresh the warehouse
from the latest processed file. Each run logs counts to CloudWatch — clean:
`rows_in` / `rows_out` / `rows_rejected`; load: `facts_loaded` / `customers` /
`products` / `dates` — and the driver script surfaces the clean counts after a run.

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
  logs the traceback. `TRUNCATE` + `COPY` run in one transaction, so a failure
  rolls back to the previous load with no partial state. The SQS message becomes
  visible again and is retried; a dead-letter queue can isolate poison messages.

## Cost

Runs within AWS's always-free monthly limits — a few MB in S3 and a handful of
Lambda invocations, well under the 1M free-request tier. The database is
Supabase (free tier). The fact table is bulk-loaded with `COPY` (one streamed
operation); most of a run's wall-clock time is building the rows in Python, not
the database write. Note the cross-region hop: Lambda in `ap-southeast-1`,
Supabase pooler in `ap-northeast-1`.

## Data

The raw dataset is not committed (~48 MB). Download the UCI Online Retail
dataset and place it at `data/Online_Retail.csv`.
