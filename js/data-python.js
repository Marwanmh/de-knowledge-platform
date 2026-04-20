// ---- PYTHON FOR DATA ENGINEERING ----
const PYTHON_MODULES = [
  {
    id: 'py-setup',
    module: 1,
    title: 'Project Setup & Virtual Environments',
    subtitle: 'Every DE project starts here — no exceptions',
    summary: 'Virtual environments isolate dependencies per project. Secrets stay in .env files, never in code.',
    points: [
      {
        label: 'Why virtual environments',
        text: 'Project A needs pandas 1.5, Project B needs pandas 2.0. Without venv, installing one breaks the other. venv creates an isolated Python environment per project — its own packages, its own pip.'
      },
      {
        label: 'Creating and using venv',
        text: 'python -m venv venv → creates the env. source venv/bin/activate (Mac/Linux) or venv\\Scripts\\activate (Windows) → activates it. pip install packages only go into THIS env. deactivate → exits. Always add venv/ to .gitignore.'
      },
      {
        label: 'requirements.txt',
        text: 'pip freeze > requirements.txt captures all installed packages + versions. Other developers run pip install -r requirements.txt to get the exact same environment. Commit this file. Do not commit the venv/ folder.'
      },
      {
        label: '.env files and secrets',
        text: 'NEVER hardcode passwords, API keys, or connection strings in code. Store them in a .env file (DB_PASSWORD=secret123). Load with python-dotenv: from dotenv import load_dotenv; load_dotenv(); os.environ["DB_PASSWORD"]. Add .env to .gitignore — if you commit it, rotate your credentials immediately.'
      }
    ],
    code: `# Terminal commands to set up a DE project
# Create project
mkdir my_pipeline && cd my_pipeline
python -m venv venv
source venv/bin/activate      # Mac/Linux
# venv\\Scripts\\activate       # Windows

# Install common DE packages
pip install pandas psycopg2-binary python-dotenv requests sqlalchemy pyarrow

# Save dependencies
pip freeze > requirements.txt

# Project structure
# my_pipeline/
#   venv/              (gitignored)
#   src/
#     extract.py
#     transform.py
#     load.py
#   dags/
#     pipeline_dag.py
#   .env               (gitignored)
#   .gitignore
#   requirements.txt
#   README.md

# .env file contents (never commit this)
# DB_HOST=localhost
# DB_PORT=5432
# DB_NAME=mydb
# DB_USER=myuser
# DB_PASSWORD=secret123
# API_KEY=abc123xyz

# Loading secrets in Python
import os
from dotenv import load_dotenv

load_dotenv()  # reads .env file into environment

DB_HOST = os.environ['DB_HOST']
DB_PASS = os.environ['DB_PASSWORD']
API_KEY = os.getenv('API_KEY', 'default_if_missing')`
  },
  {
    id: 'py-files',
    module: 2,
    title: 'File I/O — CSV, JSON, Parquet',
    subtitle: 'Reading and writing the file formats you will encounter daily',
    summary: 'DE pipelines constantly read from files and write to files. pandas handles all common formats with a consistent API.',
    points: [
      {
        label: 'CSV — most common source format',
        text: 'pd.read_csv() with key params: sep (delimiter), encoding (utf-8 vs latin-1 for Arabic/special chars), dtype (force column types), parse_dates (convert date columns), nrows (read only first N rows for testing). Always specify encoding — never assume utf-8.'
      },
      {
        label: 'JSON — API responses and semi-structured data',
        text: 'pd.read_json() for flat JSON. For nested JSON (list of dicts inside each row), use pd.json_normalize() to flatten. Most API responses need json_normalize before they become a clean DataFrame.'
      },
      {
        label: 'Parquet — the analytics standard',
        text: 'pd.read_parquet() and df.to_parquet() require pyarrow or fastparquet as the engine. Always use Parquet for intermediate and output files in a pipeline — smaller, faster, typed. Supports reading specific columns only: pd.read_parquet(path, columns=["col1","col2"]).'
      },
      {
        label: 'Chunked reading for large files',
        text: 'For files too large for memory: pd.read_csv(path, chunksize=100000) returns an iterator. Process each chunk and write to DB incrementally. This is how you handle files larger than your available RAM.'
      }
    ],
    code: `import pandas as pd
import json

# ── CSV ──────────────────────────────────────────────
df = pd.read_csv(
    'orders.csv',
    sep=',',
    encoding='utf-8',
    dtype={'order_id': str, 'customer_id': str},
    parse_dates=['order_date'],
    nrows=1000  # read first 1000 rows for testing
)

# Write CSV
df.to_csv('output.csv', index=False, encoding='utf-8')

# ── JSON ─────────────────────────────────────────────
# Flat JSON
df = pd.read_json('data.json')

# Nested JSON (e.g. API response)
with open('response.json') as f:
    data = json.load(f)

# Flatten nested structure
df = pd.json_normalize(
    data['results'],
    record_path=['items'],          # nested list to expand
    meta=['user_id', 'created_at']  # parent fields to keep
)

# ── Parquet ───────────────────────────────────────────
# Read all columns
df = pd.read_parquet('data.parquet')

# Read only needed columns (faster — columnar benefit)
df = pd.read_parquet('data.parquet', columns=['id', 'amount', 'date'])

# Write Parquet
df.to_parquet('output.parquet', index=False, compression='snappy')

# ── Large files: chunked processing ──────────────────
chunk_iter = pd.read_csv('huge_file.csv', chunksize=100_000)
total_rows = 0

for chunk in chunk_iter:
    chunk_cleaned = chunk.dropna(subset=['order_id'])
    # write each chunk to DB or append to output
    total_rows += len(chunk_cleaned)
    print(f"Processed {total_rows} rows so far...")`
  },
  {
    id: 'py-database',
    module: 3,
    title: 'Database Connectivity — psycopg2 & SQLAlchemy',
    subtitle: 'Connecting Python to PostgreSQL — the core of every pipeline',
    summary: 'psycopg2 is the low-level PostgreSQL driver. SQLAlchemy is the higher-level ORM that integrates with pandas.',
    points: [
      {
        label: 'psycopg2 — direct database operations',
        text: 'Use for: executing DML (INSERT, UPDATE, DELETE), running DDL (CREATE TABLE), and procedural pipeline logic. Key pattern: connect → cursor → execute → commit → close. Always use parameterized queries (%s placeholders) — NEVER string formatting with user input (SQL injection risk).'
      },
      {
        label: 'SQLAlchemy engine + pandas',
        text: 'Use for: reading query results into DataFrames (pd.read_sql), writing DataFrames to tables (df.to_sql). create_engine() takes a connection string. to_sql() if_exists options: "replace" (drop/recreate), "append" (add rows), "fail" (error if exists).'
      },
      {
        label: 'Connection strings',
        text: 'Format: dialect://user:password@host:port/database. PostgreSQL: postgresql://myuser:mypassword@localhost:5432/mydb. Always build from environment variables, never hardcode.'
      },
      {
        label: 'Transaction management',
        text: 'Always commit after writes (conn.commit()) or use a context manager (with conn: ...). If an exception occurs before commit, the transaction is automatically rolled back — partial writes don\'t happen.'
      }
    ],
    code: `import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ── Connection string from environment variables ──────
conn_str = (
    f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
    f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
)

# ── psycopg2: execute SQL directly ───────────────────
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

# Read data
cur.execute("SELECT * FROM orders WHERE order_date > %s", ('2024-01-01',))
rows = cur.fetchall()
columns = [desc[0] for desc in cur.description]
df = pd.DataFrame(rows, columns=columns)

# Write data (parameterized — safe from SQL injection)
cur.execute("""
    INSERT INTO pipeline_metadata (pipeline_name, last_run_at, row_count)
    VALUES (%s, NOW(), %s)
    ON CONFLICT (pipeline_name)
    DO UPDATE SET last_run_at = NOW(), row_count = EXCLUDED.row_count
""", ('orders_pipeline', len(df)))

conn.commit()
cur.close()
conn.close()

# ── SQLAlchemy: read/write DataFrames ─────────────────
engine = create_engine(conn_str)

# Read query result into DataFrame
df = pd.read_sql("SELECT * FROM dim_customer WHERE country = 'EG'", engine)

# Write DataFrame to table
df_clean.to_sql(
    name='staging_orders',
    con=engine,
    if_exists='replace',   # 'append', 'fail', or 'replace'
    index=False,
    chunksize=10_000       # insert in batches for large DataFrames
)

# ── Context manager pattern (auto-commits or rolls back) ──
with psycopg2.connect(conn_str) as conn:
    with conn.cursor() as cur:
        cur.execute("UPDATE orders SET status = %s WHERE order_id = %s",
                    ('processed', 'ORD-001'))
    # auto-commits here if no exception, auto-rollbacks if exception`
  },
  {
    id: 'py-apis',
    module: 4,
    title: 'REST API Ingestion',
    subtitle: 'Pulling data from external APIs — how most real pipelines get their data',
    summary: 'Most modern data sources expose REST APIs. You need to handle pagination, authentication, and rate limits.',
    points: [
      {
        label: 'Basic requests pattern',
        text: 'import requests. GET for fetching data. POST for sending data. Response has .status_code (200=OK, 404=not found, 429=rate limited, 500=server error), .json() (parse response body), .raise_for_status() (throws exception on 4xx/5xx — always call this).'
      },
      {
        label: 'Authentication methods',
        text: 'API Key: usually in headers (headers={"Authorization": "Bearer YOUR_KEY"}) or query params (?api_key=...). Basic auth: requests.get(url, auth=(user, password)). OAuth: more complex, token refresh needed. Always store credentials in environment variables.'
      },
      {
        label: 'Pagination',
        text: 'Most APIs don\'t return all records in one call. Offset-based: ?page=1&per_page=100, increment page until results < per_page. Cursor-based: response includes a "next_cursor" or "next_url" — use it until it\'s None. Always check the API docs for pagination style.'
      },
      {
        label: 'Rate limiting and retries',
        text: '429 Too Many Requests means you hit the rate limit. Add time.sleep() between calls. For robustness, use exponential backoff: retry after 1s, then 2s, then 4s. The requests-retry adapter automates this.'
      }
    ],
    code: `import requests
import time
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ['WEATHER_API_KEY']
BASE_URL = 'https://api.example.com'

# ── Basic GET request ─────────────────────────────────
def get_data(endpoint, params=None):
    headers = {'Authorization': f'Bearer {API_KEY}'}
    response = requests.get(
        f'{BASE_URL}/{endpoint}',
        headers=headers,
        params=params,
        timeout=30  # don't hang forever
    )
    response.raise_for_status()  # raises exception on 4xx/5xx
    return response.json()

# ── Offset-based pagination ───────────────────────────
def get_all_orders(start_date):
    all_records = []
    page = 1
    per_page = 100

    while True:
        data = get_data('orders', params={
            'start_date': start_date,
            'page': page,
            'per_page': per_page
        })
        records = data.get('results', [])
        all_records.extend(records)

        print(f"Page {page}: fetched {len(records)} records")

        if len(records) < per_page:
            break  # last page — fewer results than requested
        page += 1
        time.sleep(0.2)  # be respectful to the API

    return pd.DataFrame(all_records)

# ── Cursor-based pagination ───────────────────────────
def get_all_events():
    all_records = []
    next_cursor = None

    while True:
        params = {'limit': 100}
        if next_cursor:
            params['cursor'] = next_cursor

        data = get_data('events', params=params)
        all_records.extend(data['data'])

        next_cursor = data.get('next_cursor')
        if not next_cursor:
            break  # no more pages

    return pd.DataFrame(all_records)

# ── Retry with exponential backoff ───────────────────
def get_with_retry(url, headers, max_retries=3):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt  # 1s, 2s, 4s
                print(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
    return None`
  },
  {
    id: 'py-errors',
    module: 5,
    title: 'Error Handling & Logging',
    subtitle: 'Production pipelines fail gracefully and leave traces',
    summary: 'Silent failures are worse than loud ones. Pipelines must log what they do, handle errors explicitly, and alert on failure.',
    points: [
      {
        label: 'try/except/finally pattern',
        text: 'try: the code that might fail. except ExceptionType as e: handle specific errors. finally: always runs (close connections, cleanup). Catch specific exceptions (psycopg2.Error, requests.HTTPError) not bare except — bare except catches SystemExit and KeyboardInterrupt which you never want to swallow.'
      },
      {
        label: 'Python logging module',
        text: 'Never use print() in production pipelines. logging module writes to file and stdout with timestamps and levels. Levels: DEBUG (verbose), INFO (normal progress), WARNING (unexpected but handled), ERROR (failure, pipeline may continue), CRITICAL (pipeline must stop). Airflow captures log output automatically.'
      },
      {
        label: 'Pipeline metadata tracking',
        text: 'Write pipeline run status to a metadata table: pipeline_name, started_at, finished_at, rows_processed, status (success/failed), error_message. This is your audit trail and makes debugging much faster.'
      },
      {
        label: 'When to raise vs when to continue',
        text: 'Raise (let it fail): data quality checks failed, DB connection lost, authentication failed. Continue with warning: one record has a bad format (log it, skip it, continue). The rule: if the overall result would be wrong, fail loudly. If one bad record shouldn\'t stop 1M good records, log and skip.'
      }
    ],
    code: `import logging
import psycopg2
from datetime import datetime

# ── Configure logging ─────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()  # also print to console
    ]
)
logger = logging.getLogger(__name__)

# ── Pipeline with full error handling ────────────────
def run_pipeline(pipeline_name: str, run_date: str):
    start_time = datetime.now()
    rows_processed = 0
    status = 'success'
    error_msg = None

    try:
        logger.info(f"Pipeline '{pipeline_name}' started for {run_date}")

        # Extract
        logger.info("Extracting data from source...")
        df = extract_from_api(run_date)
        logger.info(f"Extracted {len(df)} rows")

        # Validate
        if len(df) == 0:
            raise ValueError(f"No data returned for {run_date} — possible source issue")

        # Transform
        logger.info("Transforming data...")
        df_clean = transform(df)

        # Load
        logger.info("Loading to target table...")
        load_to_db(df_clean)
        rows_processed = len(df_clean)
        logger.info(f"Loaded {rows_processed} rows successfully")

    except ValueError as e:
        status = 'failed'
        error_msg = str(e)
        logger.error(f"Data validation failed: {e}")
        raise  # re-raise so Airflow marks task as failed

    except psycopg2.Error as e:
        status = 'failed'
        error_msg = str(e)
        logger.error(f"Database error: {e}")
        raise

    except Exception as e:
        status = 'failed'
        error_msg = str(e)
        logger.critical(f"Unexpected error: {e}", exc_info=True)
        raise

    finally:
        # Always log the run result — even if it failed
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Pipeline '{pipeline_name}' finished | "
            f"Status: {status} | Rows: {rows_processed} | "
            f"Duration: {duration:.1f}s"
        )
        write_metadata(pipeline_name, start_time, rows_processed, status, error_msg)


def write_metadata(name, started_at, rows, status, error_msg):
    # Write to pipeline_metadata table for audit trail
    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_metadata
                      (pipeline_name, started_at, finished_at, rows_processed, status, error_message)
                    VALUES (%s, %s, NOW(), %s, %s, %s)
                """, (name, started_at, rows, status, error_msg))
    except Exception as e:
        logger.warning(f"Could not write metadata: {e}")`
  },
  {
    id: 'py-pandas',
    module: 6,
    title: 'Pandas for DE — Transformations',
    subtitle: 'The Python transformation layer before loading to a database',
    summary: 'pandas is your in-Python SQL. Learn the DE-specific patterns: cleaning, deduplication, merging, type casting.',
    points: [
      {
        label: 'Essential cleaning operations',
        text: 'dropna(subset=["critical_col"]) — drop rows missing required fields. fillna({"col": default}) — fill NULLs with defaults. astype({"date_col": "datetime64"}) — cast column types. str.strip().str.lower() — normalize string fields. drop_duplicates(subset=["id"]) — remove duplicate rows.'
      },
      {
        label: 'merge() — the SQL JOIN equivalent',
        text: 'pd.merge(left, right, on="key", how="left/inner/right/outer"). how= maps directly to SQL join types. Always specify on= explicitly. suffixes=("_left","_right") handles duplicate column names after merge.'
      },
      {
        label: 'groupby() + agg() — aggregation',
        text: 'df.groupby("col").agg({"revenue": "sum", "orders": "count", "avg_val": ("amount","mean")}). Returns one row per group. reset_index() converts the group keys back to regular columns.'
      },
      {
        label: 'Memory optimization',
        text: 'Large DataFrames OOM (out of memory) if dtypes are wrong. int64 uses 8 bytes per value; int32 uses 4. category dtype for low-cardinality string columns (status, country) reduces memory by 90%. Check with df.memory_usage(deep=True).'
      }
    ],
    code: `import pandas as pd
import numpy as np

# ── Load raw data ─────────────────────────────────────
df = pd.read_csv('raw_orders.csv', parse_dates=['order_date'])
print(f"Shape: {df.shape}")
print(df.dtypes)
print(df.isnull().sum())  # check NULLs per column

# ── Cleaning ─────────────────────────────────────────
df = (df
    .dropna(subset=['order_id', 'customer_id'])    # drop rows missing required fields
    .drop_duplicates(subset=['order_id'])            # remove duplicate orders
    .assign(
        status=df['status'].str.strip().str.lower(), # normalize: '  Completed ' -> 'completed'
        customer_id=df['customer_id'].astype(str),   # ensure string type
        order_date=pd.to_datetime(df['order_date']), # ensure datetime
        total_amount=df['total_amount'].fillna(0)    # fill NULLs with 0
    )
    .rename(columns={'cust_id': 'customer_id',       # rename columns
                     'amt': 'total_amount'})
)

# ── Merging (JOINs) ──────────────────────────────────
customers = pd.read_csv('customers.csv')

# LEFT JOIN: keep all orders, add customer info
df_merged = pd.merge(
    df,
    customers[['customer_id', 'name', 'country']],
    on='customer_id',
    how='left'
)

# Check for failed joins (customers not found)
missing = df_merged[df_merged['name'].isnull()]
if len(missing) > 0:
    print(f"WARNING: {len(missing)} orders have unknown customer_id")

# ── Aggregation ──────────────────────────────────────
summary = (df_merged
    .groupby(['country', 'status'])
    .agg(
        order_count=('order_id', 'count'),
        total_revenue=('total_amount', 'sum'),
        avg_order=('total_amount', 'mean')
    )
    .reset_index()
    .sort_values('total_revenue', ascending=False)
)

# ── Memory optimization ───────────────────────────────
# Before optimization
print(df.memory_usage(deep=True).sum() / 1024**2, "MB")

df['status'] = df['status'].astype('category')   # 'completed','failed','pending' → category
df['country'] = df['country'].astype('category')
df['total_amount'] = df['total_amount'].astype('float32')  # float64 → float32

# After optimization
print(df.memory_usage(deep=True).sum() / 1024**2, "MB")`
  }
];
