# retail-etl-aws

An event-driven ETL pipeline on AWS that ingests and cleans the UCI Online
Retail dataset (541,909 rows). Rebuilt from an original pandas/university
version to run on real cloud infrastructure — the point of the project was to
learn what changes when you move a laptop pipeline onto AWS.

## Architecture

```
raw CSV ──▶ S3 (raw/) ──event──▶ Lambda: clean ──▶ S3 (processed/, Parquet)
                                                          │
                                                          ▼
                                              [ Lambda: load ] ──▶ Postgres
```

<!-- TODO: replace the ASCII sketch above with a real diagram in docs/ -->

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

Latest run: **541,909 in → 397,884 ou;→ 144,025 rejected.**

### 3. Load — Postgres  *(in progress)*
<!-- TODO: a second Lambda aggregates transactions to customer level and loads
     into Postgres against a fact/dimension schema (see sql/schema.sql). -->

### 4. Scheduling & monitoring  *(in progress)*
<!-- TODO: EventBridge cron schedule; per-run monitoring via CloudWatch logs
     (rows in / out / rejected). -->

## Running it

```bash
# Run the pipeline
./scripts/run_etl.sh data/Online_Retail.csv

# Show the plan only
./scripts/run_etl.sh --dry-run data/Online_Retail.csv
```

The driver uploads the file, then polls `processed/` until a *new* cleaned
Parquet appears and prints a summary. Requires the AWS CLI configured with an
IAM user that has S3 access.

## What happens when a stage fails
<!-- TODO: per stage — e.g. a malformed CSV, a Lambda timeout, a DB connection
     failure — describe how it surfaces and what recovery looks like. -->

## Cost
<!-- TODO: note the pipeline runs within AWS's always-free monthly limits for
     S3 and Lambda; the load stage's database is Supabase (free tier). -->

## Data
The raw dataset is not committed (~48 MB). Download the UCI Online Retail
dataset and place it at `data/Online_Retail.csv`.
```
