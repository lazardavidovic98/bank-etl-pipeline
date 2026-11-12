# Documentation: bank-database-pipeline

This document describes the pipeline phase by phase: what each step does, which files are involved, and what the original version of the project did differently. For installation and a quick overview see [README.md](README.md). The database design is explained in [docs/database-schema.md](docs/database-schema.md).

## Original assignment

1. The whole process must be automated and repeatable.
2. Download the dataset from `http://65.52.154.25` using a Bearer token.
3. Track and log errors while downloading and parsing.
4. Store the data in a local SQL database, preferably Microsoft SQL Server.
5. Normalize the relational model (1NF, 2NF, 3NF).
6. Create Data Mart tables with aggregated data.
7. Make it possible to analyze the distribution of the data by the `City` column.
8. Connect all scripts into one pipeline that runs automatically every 24 hours.

| Requirement | Where it is implemented |
|---|---|
| 1. Automated, repeatable | `scripts/run_pipeline.py` runs all steps. Re-runs are idempotent (`load_core.sql`) |
| 2. Download with Bearer token | `src/bank_pipeline/extract/api_client.py` |
| 3. Error logging | `utils/logger.py`, `logs/`, `etl.pipeline_run`, `etl.rejected_record` |
| 4. SQL Server | `database/`, `load/`, `sql/` (tested on SQL Server 2022 Express) |
| 5. 1NF/2NF/3NF | `sql/schema/04_core_tables.sql`, [docs/database-schema.md](docs/database-schema.md) |
| 6. Data Mart | `sql/schema/05_datamart_tables.sql`, `sql/datamart/refresh_datamart.sql` |
| 7. Analysis by City | `mart.agg_city_summary`, `mart.v_city_distribution`, `sql/analysis/city_distribution.sql` |
| 8. Every 24 hours | `scheduler/windows/` (Task Scheduler), `scheduler/linux/` (systemd timer / cron) |

## What changed compared to the original version

The first version (2025) was a Jupyter notebook exported to one `main.py` script. It implemented phases I–III (download, flat load, splitting `City`). The normalization and the Data Mart were described in the documentation, but **the code for them did not exist**. Other problems found during the review:

| Problem in the original | Now |
|---|---|
| The token and server name were stored in `config/config.json`, which was committed | Settings in `.env` (git-ignored), with `.env.example` as a template. The token is masked in logs |
| After an error the script continued: the log showed `name 'response' is not defined` after the HTTP error | Every step stops cleanly on an error. The run is marked `failed`, and an exit code tells the scheduler what happened |
| The server name was hard-coded in the code, even though it was also in the config | All connection settings come from configuration |
| No timeout or retries on the HTTP request | Timeout, retries with exponential backoff, no retry on 401/403, size limit |
| The "chunked" load unpacked the `json` column only from the **first** row of each chunk | The envelope is decoded correctly once, and then the rows are processed in chunks |
| Phase III loaded the whole table into memory (`pd.read_sql("SELECT *")`) with a raw pyodbc connection | No full-table reads in Python. Core and mart are filled with set-based SQL inside SQL Server |
| Tables were dropped/truncated and fully reloaded on every run | Staging is truncated, while core is updated with insert/update by `source_row_id` (idempotent, history kept) |
| No normalization or Data Mart code | 3NF schema with keys and constraints, star-schema Data Mart, aggregates, views |
| No scheduler | Windows Task Scheduler and Linux systemd/cron, with protection against parallel runs |
| No tests | 38 pytest tests (unit + SQL Server integration) |
| `sql/SQLQuery.sql` contained `DROP TABLE transactions` | Replaced with read-only analysis queries |

## Prerequisites

- **Python 3.10+**
- **Microsoft SQL Server** 2019/2022 (Express or Developer edition is enough), running and reachable. The default configuration uses the instance `localhost\SQLEXPRESS` with Windows authentication.
- **Microsoft ODBC Driver 18 for SQL Server** (`DB_DRIVER` in `.env`; Driver 17 also works).
- Optional: SQL Server Management Studio, Azure Data Studio or the *SQL Server (mssql)* extension for VS Code to browse the tables and run `sql/analysis/city_distribution.sql`.
- With Windows authentication no user name or password is needed. For SQL authentication set `DB_TRUSTED_CONNECTION=false`, `DB_USER` and `DB_PASSWORD`.

## Configuration and files

| File | Purpose |
|---|---|
| `.env` (from `.env.example`) | All settings: source, token, SQL Server connection, chunk/batch size, logging |
| `data/sample/transactions_snapshot.json` | Last real response from the API (was `data/dataset.json`), used in `file` mode and as the fallback |
| `data/raw/`, `data/processed/`, `data/rejected/` | Per-run files: raw download, parsed CSV, rejected rows. The last `KEEP_RUNS` runs are kept |
| `logs/pipeline.log`, `logs/errors.log`, `logs/scheduler.log` | Logs (rotated at 5 MB, 10 files kept) |
| `sql/schema/*.sql` | Tables and views. Every script checks whether the object exists first |
| `sql/transformations/load_core.sql` | Staging → normalized tables |
| `sql/datamart/refresh_datamart.sql` | Normalized tables → Data Mart |
| `sql/analysis/city_distribution.sql` | Example report queries |

## Phase I: Dataset retrieval (Extract)

Files: `extract/__init__.py`, `extract/api_client.py`

- The source is chosen with `SOURCE_MODE`:
  - `api`: only the HTTP API. If it fails, the run fails.
  - `file`: only `LOCAL_DATA_FILE` (or `--file`).
  - `auto`: try the API first. If it is not available, log a warning and use the local file. The run is saved with `used_fallback = 1` and `source = 'file'`, so a fallback is never reported as a successful download.
- The request sends `Authorization: Bearer <SOURCE_TOKEN>` with a timeout of `HTTP_TIMEOUT` seconds.
- **Retries:** connection errors, timeouts and HTTP 429/500/502/503/504 are retried up to `HTTP_RETRIES` times. The wait between attempts doubles each time: `HTTP_BACKOFF_SECONDS`, then 2×, then 4×.
- **No retry** on 401/403 (wrong token) or other 4xx errors, because trying again would not help.
- The response is **streamed to disk** in 64 KB blocks. If it grows beyond `MAX_DOWNLOAD_MB`, the download is stopped and the partial file deleted.
- An empty response counts as an error. The time and duration of the download are in the log.
- `python scripts/run_pipeline.py --check-source` sends one request and reports whether the API responds.

**Availability check (2026-11-12):** `http://65.52.154.25` did not respond. Connections on port 80 and 443 timed out (`curl`: connection timed out; `Test-NetConnection`: false). The original project's log already showed the same error on 2025-05-31. The token from the old `config.json` could therefore not be checked. It is in the public Git history and must be considered compromised.

## Phase II: Parsing

File: `transform/parser.py`

The API response has this format (a *string* in the `json` field, created by pandas `to_json(orient="columns")`):

```json
{"status": "200", "message": "ok",
 "json": "{\"index\":{\"0\":0,\"1\":1},\"City\":{\"0\":\"Delhi, India\",\"1\":\"Greater Mumbai, India\"},\"Date\":{\"0\":\"29-Oct-14\",...},...}"}
```

- The envelope is checked: valid JSON, `json` field present, `status` = 200.
- The inner JSON is decoded and the columns are mapped to internal names (`index → source_row_id`, `Card Type → card_type`, ...). Column names are compared without regard to case, spaces or `_`. A missing required column stops the run with a clear message.
- The rows are written **one at a time** (generator + `csv.DictWriter`) to `data/processed/<run>.csv`, and the decoded JSON is then released.
- Instead of the API format, a CSV file with the same columns can be used (`--file data.csv`). Nested structures are no longer a problem because the CSV is flat.

## Phase III: Validation and cleaning (Transform)

File: `transform/cleaner.py`

The CSV is read with `pd.read_csv(chunksize=CHUNK_SIZE, dtype=str, keep_default_na=False)`: everything is read as text and converted explicitly, and only empty cells count as missing. Each chunk goes through these steps:

| Check | Reason code |
|---|---|
| Required value is empty | `missing_<column>` |
| `source_row_id` is not a whole number ≥ 0 | `invalid_id` |
| `City` is not in the form `City, Country` | `city_without_country` |
| Date is not `D-Mon-YY` or does not exist (e.g. `31-Feb-15`) | `invalid_date` |
| Date is after today | `future_date` |
| Gender is not F/M (`Female`/`Male` are accepted) | `invalid_gender` |
| Amount is not a number or ≤ 0 | `invalid_amount` |
| The same `source_row_id` was already accepted (first valid row wins) | `duplicate_id` |

Normalization of valid rows: spaces trimmed and collapsed, `City` split into `city_name` and `country_name`, card and expense types in Title Case, gender as `F`/`M`, dates as real dates, amounts rounded to 2 decimals.

Months are parsed with a fixed table (`jan`…`dec`), not with `strptime`, so the result does not depend on the language settings of the computer.

Invalid rows keep their original values and get a `reasons` column (for example `missing_city;invalid_amount`). They are saved to `etl.rejected_record` (as JSON) and to `data/rejected/<run>.csv`.

## Phase IV: Loading into SQL Server

Files: `load/staging.py`, `load/core.py`, `database/`, `sql/transformations/load_core.sql`

**Connection:** SQLAlchemy engine with `mssql+pyodbc`, a connection pool (`pool_size=5`, `pool_pre_ping=True`) and `fast_executemany=True`, so pyodbc sends a whole batch of rows in one round trip.

**Staging:**

1. `TRUNCATE TABLE stg.transactions`. Choosing between TRUNCATE and DELETE:
   - The whole table needs to be emptied, and TRUNCATE does this without logging each row, so it is much faster.
   - It is safe here: no foreign key points to the staging table (TRUNCATE would fail otherwise), there is no IDENTITY column to reset, and the table keeps no history (the permanent data is in `core`).
2. Each valid chunk is inserted in batches of `BATCH_SIZE` rows with a parameterized `INSERT ... VALUES (?, ?, ...)`. No values are ever pasted into SQL text.

**Core load** (`load_core.sql`, run by `load_core()` inside **one transaction** via `engine.begin()`):

1. Insert new countries, cities, card types and expense types (only values that do not exist yet).
2. Update transactions that already exist (same `source_row_id`) but have different values.
3. Insert transactions that do not exist yet.

If any statement fails, everything is rolled back, so the core tables are never left half-loaded. Running the same data again inserts 0 and updates 0 rows. Transactions are never deleted.

Before the core load, the run is stopped if there are no valid rows or if the share of invalid rows is above `MAX_REJECT_RATIO`. In that case the core tables are not touched.

## Phase V: Normalization

Implemented in `sql/schema/04_core_tables.sql`. Summary:

- **1NF:** one row per transaction, atomic values (`City` split into city and country), every row identified by `source_row_id`.
- **2NF:** all tables have a single-column primary key, so no attribute can depend on part of a key.
- **3NF:** the transitive dependency `transaction → city → country` is removed (`core.country` ← `core.city` ← `core.transactions`). Gender names, card types and expense types are separate lookup tables.

The full explanation, with functional dependencies and keys, is in [docs/database-schema.md](docs/database-schema.md).

## Phase VI: Data Mart

Files: `sql/schema/05_datamart_tables.sql`, `06_datamart_views.sql`, `sql/datamart/refresh_datamart.sql`

The original version calculated the number of transactions and total/average spending per city in pandas (`datamart_po_gradu`). This is now a star schema in the `mart` schema:

- Fact table `mart.fact_transactions` (one row per transaction) with dimensions `dim_date`, `dim_city`, `dim_card_type`, `dim_expense_type`, `dim_gender`.
- `mart.agg_city_summary`: per city, the number of transactions, % of all transactions, rank, total/average/min/max amount, % of total amount, and rank by amount. This includes everything the old `datamart_po_gradu` table had.
- `mart.agg_city_expense`: per city and expense type.
- Views `v_city_distribution` (with cumulative %), `v_overview`, `v_city_expense`.

The refresh runs automatically after every successful core load, in one transaction. Dimensions are updated with INSERT/UPDATE because the fact table references them with foreign keys (TRUNCATE is not allowed on such tables). The fact and aggregate tables are rebuilt with TRUNCATE + INSERT. Before the commit, Python checks that the number of rows in core, in the fact table and the sum of all city counts are equal. Otherwise the refresh is rolled back.

## Phase VII: Logging and monitoring

Files: `utils/logger.py`, `load/run_log.py`, `pipeline.py`

- Start and end of the pipeline and of each step, with duration.
- Counts: extracted, valid, rejected, inserted, updated, number of cities.
- HTTP errors (each attempt), parsing errors, SQL errors, and unexpected errors with a full traceback.
- Levels: `DEBUG` (row counts per SQL step and per chunk), `INFO` (normal progress), `WARNING` (retry, fallback, invalid rows), `ERROR` (failed step), `CRITICAL` (failed run).
- `pipeline.log` (everything) and `errors.log` (warnings and errors), both rotated. Secrets are replaced with `***`.
- `etl.pipeline_run` keeps the history of all runs, so you can see which run failed and why without opening log files:

```sql
SELECT TOP (10) * FROM etl.pipeline_run ORDER BY run_id DESC;
```

## Phase VIII: Automation

Files: `scheduler/windows/*`, `scheduler/linux/*`, `utils/lock.py`

- **Windows:** `register_task.ps1` creates the task *BankDatabasePipeline* in Task Scheduler (daily at 02:00, no parallel instance, catch-up after the PC was off, 2 h time limit, 2 retries). The task runs `run_pipeline.ps1`, which uses `.venv` and writes to `logs/scheduler.log`.
- **Linux:** `bank-pipeline.timer` + `bank-pipeline.service` (systemd), or one line in crontab. Both use `run_pipeline.sh`, which also uses `flock`.
- **Protection against parallel runs:** `run_pipeline.py` takes an OS file lock (`logs/pipeline.lock`). A second run started at the same time exits immediately with code 3. The lock is released automatically when the process ends, even after a crash.
- `.env` is read by the pipeline itself, so the scheduler does not need to set any environment variables.

Step-by-step instructions are in the README, section *Automatic scheduling*.

## Phase IX: Tests

Run with `pytest -v`. Unit tests use fake HTTP responses and temporary files. Database tests use their own `bank_test` database and are skipped if SQL Server is not available. The list of tests is in the README, section *Testing*.
