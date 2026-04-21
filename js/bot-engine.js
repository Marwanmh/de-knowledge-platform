// ============================================================
// DE EXPERT BOT — OFFLINE KNOWLEDGE BASE + SEARCH ENGINE
// All knowledge baked in. No API. No internet required.
// ============================================================

const BOT_KB = [

  // ============================================================
  // DATA ENGINEERING — FOUNDATIONS
  // ============================================================
  {
    id: 'what-is-de',
    keywords: ['what','data engineering','data engineer','de','role','job','career','become'],
    title: 'What is Data Engineering?',
    answer: `Data Engineering is the job of building and maintaining the systems that move, store, and transform data so analysts and data scientists can use it.

Think of it like plumbing. Analysts need clean water (data) at the tap. Data engineers build the pipes, filters, and pumps that get it there.

**What a data engineer actually does:**
- Builds pipelines that extract data from sources (APIs, databases, files)
- Transforms messy raw data into clean, structured formats
- Loads data into warehouses (BigQuery, Snowflake, PostgreSQL)
- Schedules and monitors those pipelines (usually with Airflow)
- Makes sure data is accurate, complete, and available on time

**Key tools you need:** SQL (most important), Python, Airflow, Spark, dbt, a cloud data warehouse.

Start with SQL — it's used in literally every data engineering task.`
  },
  {
    id: 'oltp-olap',
    keywords: ['oltp','olap','difference','transactional','analytical','database type'],
    title: 'OLTP vs OLAP',
    answer: `OLTP and OLAP are two completely different types of databases built for different jobs.

**OLTP (Online Transaction Processing):**
Used by your actual application. Handles thousands of small reads/writes per second. Your app's PostgreSQL database is OLTP.
- Optimized for: INSERT, UPDATE, DELETE
- Example: recording a new order, updating a user's email
- Row-based storage. Fast for single-row lookups.

**OLAP (Online Analytical Processing):**
Used for analysis and reporting. Handles complex queries across millions of rows.
- Optimized for: SELECT with aggregations (SUM, COUNT, AVG across millions of rows)
- Example: "What was total revenue by country last quarter?"
- Column-based storage. Fast for aggregations, slow for single-row lookups.

**Why separate them?**
Running heavy analytics on your OLTP database slows down your actual app. A single badly written analytical query can lock tables and bring your production system down.

The data engineer's job is to move data from OLTP → OLAP (the data warehouse) so analysts can query freely without touching production.`
  },
  {
    id: 'etl-elt',
    keywords: ['etl','elt','extract','transform','load','difference','pipeline'],
    title: 'ETL vs ELT',
    answer: `ETL and ELT describe the order in which you move and transform data.

**ETL (Extract → Transform → Load):**
Transform data BEFORE loading it into the warehouse. Used when the warehouse is expensive or limited (old approach — Oracle, Teradata).
- Extract raw data from source
- Transform it in a separate compute layer (Spark, pandas, DataStage)
- Load only clean data into the warehouse

**ELT (Extract → Load → Transform):**
Load raw data into the warehouse FIRST, then transform it there using SQL. Modern approach enabled by cheap cloud compute (BigQuery, Snowflake, Redshift).
- Extract raw data
- Load it straight into a raw/staging layer
- Transform using SQL/dbt inside the warehouse

**Why ELT is winning:**
- Cloud warehouses have nearly unlimited compute — transformation is cheap
- Raw data is preserved — if business logic changes, you can re-transform without re-ingesting
- dbt is built entirely around ELT — all transformations are SQL SELECT statements

**Simple rule:** If you're using a cloud data warehouse + dbt → you're doing ELT. If you're using Airflow + pandas/Spark to clean before loading → you're doing ETL.`
  },
  {
    id: 'data-warehouse',
    keywords: ['data warehouse','warehouse','dwh','dw','snowflake','bigquery','redshift','what is warehouse'],
    title: 'What is a Data Warehouse?',
    answer: `A data warehouse is a central database optimized for analytical queries — it stores historical data from all your business systems in one place.

**Why it exists:**
Your production database (OLTP) only keeps current state. The warehouse keeps history. Analysts need to ask "how did revenue trend over the past 2 years?" — the warehouse answers that.

**How data gets there:**
Data engineers build pipelines that extract data from production databases, APIs, and files → clean and transform it → load it into the warehouse on a schedule.

**Key characteristics:**
- Column-based storage (fast for aggregations)
- Designed for reads, not writes
- Stores months or years of history
- Structured in star or snowflake schema

**Popular warehouses:**
- **BigQuery** (Google) — pay per query scanned, serverless
- **Snowflake** — separate compute and storage, time-travel feature
- **Redshift** (AWS) — cluster-based, tight AWS integration
- **PostgreSQL** — not a warehouse, but used as one for small-scale DE projects

**Data Lake vs Warehouse:**
Warehouse = structured, clean, SQL-queryable. Lake = raw files in any format (Parquet, JSON, CSV) stored in object storage (S3, GCS). Lakehouse = hybrid (Delta Lake, Iceberg).`
  },
  {
    id: 'pipeline-phases',
    keywords: ['pipeline','stage','staging','transform','core','medallion','bronze','silver','gold','phases','layers'],
    title: 'Pipeline Phases / Medallion Architecture',
    answer: `Data pipelines are organized in layers. Each layer has a specific purpose. The most common pattern is called Medallion Architecture (Bronze → Silver → Gold) or Stage → Transform → Core.

**Layer 1 — Stage / Bronze (Raw):**
Exact copy of source data. No transformations. This is your safety net — if anything goes wrong downstream, you can always re-process from here.
- Table name example: \`stg_orders\`, \`raw.orders\`
- Contains: raw columns, nulls, duplicates, bad data — everything as-is

**Layer 2 — Transform / Silver (Cleaned):**
Data is cleaned, deduplicated, typed correctly, and standardized. Still close to source structure.
- Nulls handled, dates parsed, strings trimmed
- Table name example: \`cleaned_orders\`, \`int_orders\`

**Layer 3 — Core / Gold (Business-ready):**
Final tables structured for business use. Star schema with fact and dimension tables. What analysts actually query.
- Table name example: \`fact_orders\`, \`dim_customer\`
- Aggregated, joined, ready for dashboards

**Why this matters in interviews:**
Interviewers ask "how would you design a pipeline?" — always describe these 3 layers. Shows you understand data quality, reprocessability, and separation of concerns.`
  },
  {
    id: 'incremental-loading',
    keywords: ['incremental','loading','watermark','delta','full load','incremental load','updated_at','last run'],
    title: 'Incremental Loading',
    answer: `Incremental loading means only loading new or changed records since the last pipeline run — instead of re-loading everything every time.

**Why full load is bad:**
If your orders table has 50 million rows and you reload all of it every hour, that's 50M rows of network transfer, compute, and storage cost — every hour. Wasteful and slow.

**The watermark pattern (most common):**
\`\`\`sql
-- Load only rows newer than last run
SELECT * FROM orders
WHERE updated_at > '2024-01-15 06:00:00'  -- last_run_timestamp
\`\`\`

**How it works step by step:**
1. Read \`last_run_timestamp\` from a metadata table
2. Query source: \`WHERE updated_at > last_run_timestamp\`
3. Load those rows into staging
4. On success, write new \`last_run_timestamp\` back to metadata table
5. Next run uses the new timestamp

**The metadata table:**
\`\`\`sql
CREATE TABLE pipeline_metadata (
  pipeline_name VARCHAR(100),
  last_run_timestamp TIMESTAMP,
  status VARCHAR(20)
);
\`\`\`

**Watch out for:**
- **Late-arriving data:** Records with old timestamps that arrive late — you'll miss them
- **Soft deletes:** Deleted records don't appear as "new" — use a deleted_at flag or SCD Type 2
- **No updated_at column:** You'll have to use full load or ID range filtering`
  },
  {
    id: 'idempotency',
    keywords: ['idempotent','idempotency','safe to rerun','rerun','duplicate','run twice'],
    title: 'Idempotency',
    answer: `An idempotent pipeline produces the same result no matter how many times you run it. Running it once or running it 10 times gives identical output.

**Why it matters:**
Pipelines fail. Networks drop, databases timeout, servers crash. When a pipeline fails halfway, you need to rerun it. If it's not idempotent, you get duplicate data, partial loads, or inconsistent state.

**Non-idempotent example (bad):**
\`\`\`sql
INSERT INTO fact_orders SELECT * FROM stg_orders;
-- Run twice = duplicate rows. Disaster.
\`\`\`

**Idempotent pattern (good) — TRUNCATE + INSERT:**
\`\`\`sql
TRUNCATE TABLE fact_orders;
INSERT INTO fact_orders SELECT * FROM stg_orders;
-- Run 10 times = same result every time.
\`\`\`

**Idempotent pattern — MERGE/UPSERT:**
\`\`\`sql
INSERT INTO fact_orders (order_id, amount)
VALUES (:order_id, :amount)
ON CONFLICT (order_id) DO UPDATE SET amount = EXCLUDED.amount;
-- Inserts new rows, updates existing ones. Safe to rerun.
\`\`\`

**How to make pipelines idempotent:**
- Use MERGE/UPSERT instead of plain INSERT
- TRUNCATE staging tables before loading
- Use DELETE WHERE date = run_date then INSERT (partition replacement)
- Store watermarks in metadata tables (not in the pipeline code)

**Interview answer:** "I always design pipelines to be idempotent — using MERGE patterns and partition replacement — so any failure can be safely retried without data corruption."`
  },
  {
    id: 'acid',
    keywords: ['acid','atomicity','consistency','isolation','durability','transaction','commit','rollback'],
    title: 'ACID Transactions',
    answer: `ACID is a set of 4 guarantees that a database transaction must satisfy to be reliable. Every production database (PostgreSQL, MySQL, Oracle) supports ACID.

**A — Atomicity:**
A transaction is all or nothing. If you transfer $100 from Account A to Account B, both the debit AND credit happen — or neither does. No partial transactions.

**C — Consistency:**
After a transaction, the database is in a valid state. All constraints (foreign keys, NOT NULL, UNIQUE) are still satisfied.

**I — Isolation:**
Concurrent transactions don't see each other's intermediate states. If two people book the last seat on a plane simultaneously, isolation prevents double-booking.

**D — Durability:**
Once a transaction is committed, it survives crashes. Data is written to disk, not just memory.

**Why data engineers care:**
\`\`\`sql
BEGIN;
  TRUNCATE staging.orders;
  INSERT INTO staging.orders SELECT * FROM raw.orders;
  UPDATE pipeline_metadata SET last_run = NOW();
COMMIT;
-- If INSERT fails, the TRUNCATE is also rolled back. Safe.
\`\`\`

Wrapping pipeline steps in transactions prevents partial loads — either the whole batch succeeds, or nothing changes.

**Interview tip:** ACID guarantees are why we trust relational databases for critical data. NoSQL databases often sacrifice some ACID properties for speed and scale.`
  },
  {
    id: 'star-schema',
    keywords: ['star schema','snowflake schema','fact table','dimension table','dimensional modeling','star','schema design'],
    title: 'Star Schema & Dimensional Modeling',
    answer: `Star schema is the standard way to structure a data warehouse. It's called "star" because the diagram looks like a star — one fact table in the middle, dimension tables around it.

**Fact Table:**
Stores measurable events — sales, clicks, transactions. Has many rows. Contains foreign keys to dimensions + numeric measures.
\`\`\`sql
fact_sales: sale_id, date_sk, customer_sk, product_sk, quantity, revenue
\`\`\`

**Dimension Tables:**
Store descriptive context — who, what, where, when. Fewer rows. Contains attributes used for filtering and grouping.
\`\`\`sql
dim_customer: customer_sk, name, city, country, segment
dim_product:  product_sk, name, category, brand, price
dim_date:     date_sk, date, year, month, quarter, day_of_week
\`\`\`

**Why star schema?**
Simple joins, fast queries. Analysts can query with intuitive SQL. BI tools (Tableau, Power BI) work perfectly with it.

**Grain:**
The grain is what each row in the fact table represents. "One row per order line item" is the grain. Always define grain before designing a fact table.

**Star vs Snowflake:**
- Star: dimension tables are denormalized (all attributes in one table). Simpler queries.
- Snowflake: dimensions are normalized (split into sub-tables). Less storage but more joins.
- In practice: use star schema. The join complexity of snowflake rarely pays off.

**Surrogate key:**
Use a system-generated integer (surrogate key) as the primary key for dimension tables — not the natural key from the source system. Natural keys can change, be null, or be non-unique across systems.`
  },
  {
    id: 'scd',
    keywords: ['scd','slowly changing dimension','type 1','type 2','type 3','history','dimension changes'],
    title: 'Slowly Changing Dimensions (SCD)',
    answer: `SCD describes how you handle changes to dimension data over time. When a customer moves cities, do you overwrite the old city or keep both versions?

**SCD Type 1 — Overwrite:**
Simply update the existing row. History is lost.
\`\`\`sql
UPDATE dim_customer SET city = 'Alexandria' WHERE customer_id = 101;
\`\`\`
Use when: history doesn't matter (e.g., fixing a typo)

**SCD Type 2 — Add new row (most common in DE):**
Keep the old row, add a new row with the change. Use effective_from/effective_to dates + is_current flag.
\`\`\`sql
-- Old row
(101, 'Ahmed', 'Cairo',      '2023-01-01', '2024-06-01', FALSE)
-- New row added
(101, 'Ahmed', 'Alexandria', '2024-06-01', '9999-12-31', TRUE)
\`\`\`
Use when: you need full history (sales analysis by customer's location at time of purchase)

**SCD Type 3 — Add column:**
Add a "previous_value" column. Only keeps one level of history.
\`\`\`sql
dim_customer: customer_id, current_city, previous_city
\`\`\`
Use when: you only care about one prior value (rarely used in practice)

**Interview tip:** SCD Type 2 is by far the most asked about. Know it cold — the is_current flag, effective dates, and why surrogate keys are essential for it to work.`
  },
  {
    id: 'data-lake',
    keywords: ['data lake','lakehouse','delta lake','iceberg','s3','gcs','object storage','parquet','lake'],
    title: 'Data Lake vs Lakehouse',
    answer: `**Data Lake:**
Store ANY data — structured, semi-structured, unstructured — as raw files in cheap object storage (S3, GCS, Azure Blob). No schema enforced upfront.
- Format: Parquet, JSON, CSV, images, logs — anything
- Cheap storage, scalable to petabytes
- Problem: becomes a "data swamp" — no governance, no ACID, hard to query reliably

**Data Warehouse:**
Structured, cleaned, SQL-queryable. Enforces schema. Fast for analytics but expensive and rigid.

**Lakehouse (best of both):**
Combines lake storage cost with warehouse query performance + ACID guarantees. Achieved through table formats like Delta Lake (Databricks), Apache Iceberg, Apache Hudi.
- Store Parquet files on S3/GCS
- Table format adds ACID transactions, schema enforcement, time travel
- Query with Spark, Trino, or directly via SQL

**Why this matters:**
Most modern DE stacks are moving toward Lakehouse. Databricks, Snowflake, and BigQuery all support lakehouse patterns. Knowing Parquet + Delta/Iceberg is increasingly important.

**Quick summary:**
- Lake = cheap, flexible, messy
- Warehouse = expensive, structured, clean
- Lakehouse = cheap storage + warehouse features`
  },

  // ============================================================
  // CAREER / LEARNING
  // ============================================================
  {
    id: 'how-to-start',
    keywords: ['how to start','where to start','beginning','beginner','learn','roadmap','path','first step'],
    title: 'How to Start Learning Data Engineering',
    answer: `Learn in this exact order — each skill builds on the previous one.

**Phase 1 — SQL (3 weeks):**
Most critical skill. Every pipeline, transformation, and data quality check uses SQL. Don't move on until you can write complex JOINs, window functions, and CTEs without Googling.

**Phase 2 — Python (3 weeks):**
Focus only on what DEs use: file I/O (CSV/JSON/Parquet), pandas, psycopg2 (connect to PostgreSQL), requests (call APIs), and error handling. You don't need to be a software engineer.

**Phase 3 — Set up your local environment:**
Install PostgreSQL, Docker, Airflow (via docker-compose), VS Code. Build things locally before learning cloud tools.

**Phase 4 — DE Concepts:**
Now that you can write code, learn the vocabulary: OLTP/OLAP, star schema, ETL/ELT, incremental loading, idempotency. These concepts make sense once you've built something.

**Phase 5 — Airflow:**
Write a DAG from scratch. Schedule a pipeline. Learn about operators, XComs, and connections.

**Phase 6 — Spark/PySpark:**
Distributed processing for large-scale data. Learn DataFrames, lazy evaluation, partitioning.

**Phase 7 — dbt + Cloud:**
dbt is in ~60% of DE job postings. BigQuery/Snowflake are in ~90% of cloud roles.

**Phase 8 — Interview prep.**

Don't jump ahead. SQL mastery opens more doors than knowing 10 tools shallowly.`
  },
  {
    id: 'interview-tips',
    keywords: ['interview','tips','prepare','preparation','questions','junior','job','hired'],
    title: 'Data Engineer Interview Tips',
    answer: `Junior DE interviews test 3 things: SQL, system design basics, and whether you understand the "why" behind concepts.

**SQL (most tested):**
Practice writing queries from scratch — no autocomplete. You will get a problem and a whiteboard/editor. Common asks:
- Write a query using window functions (ROW_NUMBER, LAG)
- Get the latest record per group
- Find duplicates
- Calculate running totals

**Concept questions:**
They don't want definitions — they want you to explain WHY. Not "ETL means Extract Transform Load" but "I use ELT for cloud warehouses because transforming inside the warehouse is cheaper and preserves raw data for reprocessing."

**Design questions:**
"Design a pipeline that ingests 10M rows of orders daily." Walk through: source → staging layer → transformation → core tables → scheduling → monitoring. Mention idempotency, error handling, and incremental loading.

**Behavioral questions:**
Prepare 3 stories: a time you debugged a data problem, a time you improved a process, a project you built end-to-end.

**What to say when you don't know:**
"I haven't used that specific tool, but I understand the concept — it's similar to X which I've worked with. I pick up new tools quickly." Never say "I don't know" and stop there.

**Biggest mistake juniors make:**
Memorizing definitions without understanding when and why to use something. Interviewers see through it immediately.`
  },

  // ============================================================
  // SQL
  // ============================================================
  {
    id: 'sql-joins',
    keywords: ['join','inner join','left join','right join','full outer','cross join','joins','joining tables'],
    title: 'SQL JOINs Explained',
    answer: `JOINs combine rows from two tables based on a related column. They are the most tested SQL concept in DE interviews.

**INNER JOIN — only matching rows:**
\`\`\`sql
SELECT o.order_id, c.name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id;
-- Returns only orders that have a matching customer
\`\`\`

**LEFT JOIN — all left rows, matching right rows (NULLs if no match):**
\`\`\`sql
SELECT c.name, o.order_id
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id;
-- Returns ALL customers, even those with no orders (order_id = NULL)
\`\`\`

**RIGHT JOIN — opposite of LEFT JOIN** (rarely used — just flip the table order and use LEFT JOIN)

**FULL OUTER JOIN — all rows from both tables:**
Returns everything — matching rows combined, plus non-matching rows from both sides with NULLs.

**CROSS JOIN — every combination:**
\`\`\`sql
SELECT a.color, b.size FROM colors a CROSS JOIN sizes b;
-- If colors has 3 rows and sizes has 4 rows → 12 rows returned
\`\`\`

**The most common interview question:**
"What's the difference between LEFT JOIN and INNER JOIN?"
Answer: INNER JOIN drops rows with no match. LEFT JOIN keeps all rows from the left table and fills right-side columns with NULL when there's no match.`
  },
  {
    id: 'window-functions',
    keywords: ['window function','row_number','rank','dense_rank','lag','lead','partition by','over','running total','window'],
    title: 'Window Functions',
    answer: `Window functions perform calculations across a set of related rows without collapsing them — unlike GROUP BY which reduces multiple rows to one.

**Syntax pattern:**
\`\`\`sql
FUNCTION() OVER (PARTITION BY col ORDER BY col)
\`\`\`

**ROW_NUMBER — unique row number per partition:**
\`\`\`sql
-- Get the latest order per customer (most common DE pattern)
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY customer_id ORDER BY order_date DESC
  ) AS rn
  FROM orders
) t WHERE rn = 1;
\`\`\`

**RANK vs DENSE_RANK (tie handling):**
\`\`\`sql
-- If two rows tie for 2nd place:
-- RANK gives: 1, 2, 2, 4  (skips 3)
-- DENSE_RANK gives: 1, 2, 2, 3  (no gap)
\`\`\`

**LAG / LEAD — access previous or next row:**
\`\`\`sql
SELECT month, revenue,
  LAG(revenue, 1) OVER (ORDER BY month) AS prev_month_revenue,
  revenue - LAG(revenue, 1) OVER (ORDER BY month) AS mom_change
FROM monthly_revenue;
\`\`\`

**Running total:**
\`\`\`sql
SUM(revenue) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
\`\`\`

**Key insight:** PARTITION BY is like GROUP BY but it doesn't collapse rows. Think of it as "reset the calculation for each group."`
  },
  {
    id: 'ctes',
    keywords: ['cte','with clause','common table expression','with','named query','chained cte'],
    title: 'CTEs — WITH Clause',
    answer: `A CTE (Common Table Expression) is a named temporary result set defined at the top of a query using WITH. Think of it as giving a subquery a name so you can reference it cleanly.

**Basic CTE:**
\`\`\`sql
WITH monthly_revenue AS (
  SELECT DATE_TRUNC('month', order_date) AS month,
         SUM(total_amount) AS revenue
  FROM orders
  GROUP BY 1
)
SELECT month, revenue
FROM monthly_revenue
WHERE revenue > 10000;
\`\`\`

**Chained CTEs (dbt model structure):**
\`\`\`sql
WITH raw AS (
  SELECT * FROM orders WHERE status != 'cancelled'
),
aggregated AS (
  SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS total
  FROM raw
  GROUP BY customer_id
),
flagged AS (
  SELECT *, CASE WHEN total > 1000 THEN 'VIP' ELSE 'Standard' END AS tier
  FROM aggregated
)
SELECT * FROM flagged WHERE tier = 'VIP';
\`\`\`

**CTEs vs Subqueries:**
CTEs are readable and reusable within the same query. Deeply nested subqueries become unreadable fast. In dbt, every model is structured as a chain of CTEs.

**Important:** CTEs are NOT materialized by default — the database re-runs them each time they're referenced. For performance-critical queries, use a temp table or materialized view instead.`
  },
  {
    id: 'group-by-having',
    keywords: ['group by','having','aggregate','count','sum','avg','min','max','grouping','difference having where'],
    title: 'GROUP BY and HAVING',
    answer: `GROUP BY collapses multiple rows into one summary row per group. HAVING filters those groups AFTER aggregation.

**GROUP BY basics:**
\`\`\`sql
SELECT customer_id,
       COUNT(*) AS order_count,
       SUM(total_amount) AS total_spent
FROM orders
GROUP BY customer_id;
-- One row per customer
\`\`\`

**The most common mistake:** Selecting a column that's not in GROUP BY and not in an aggregate function.
\`\`\`sql
-- WRONG — order_date not in GROUP BY or aggregate
SELECT customer_id, order_date, COUNT(*)
FROM orders
GROUP BY customer_id;
\`\`\`

**WHERE vs HAVING — the #1 interview question:**
\`\`\`sql
SELECT customer_id, COUNT(*) AS order_count
FROM orders
WHERE total_amount > 100      -- WHERE runs BEFORE GROUP BY (filters rows)
GROUP BY customer_id
HAVING COUNT(*) > 5;          -- HAVING runs AFTER GROUP BY (filters groups)
\`\`\`

Simple rule:
- WHERE = filter individual ROWS (before grouping)
- HAVING = filter GROUPS (after aggregation)
- You CANNOT use aggregate functions in WHERE

**Conditional aggregation (advanced):**
\`\`\`sql
SELECT
  COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed,
  COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failed
FROM orders;
\`\`\``
  },
  {
    id: 'null-handling',
    keywords: ['null','is null','coalesce','nullif','null handling','missing','empty'],
    title: 'NULL Handling in SQL',
    answer: `NULL means "unknown" or "missing" — not zero, not empty string. NULLs behave unexpectedly if you don't know the rules.

**The #1 NULL rule:** You CANNOT compare NULL with =
\`\`\`sql
-- WRONG — this never returns rows even if value IS NULL
WHERE email = NULL;

-- CORRECT
WHERE email IS NULL;
WHERE email IS NOT NULL;
\`\`\`

**COALESCE — return first non-null value:**
\`\`\`sql
SELECT COALESCE(phone, 'No phone') AS phone FROM customers;
-- If phone is NULL, returns 'No phone'

-- Multiple fallbacks
SELECT COALESCE(mobile, home_phone, work_phone, 'No contact') AS contact;
\`\`\`

**NULLIF — return NULL if two values match (prevents divide-by-zero):**
\`\`\`sql
SELECT revenue / NULLIF(units_sold, 0) AS avg_price;
-- If units_sold = 0, NULLIF returns NULL, avoiding division by zero error
\`\`\`

**NULLs in aggregates:**
\`\`\`sql
SELECT
  COUNT(*) AS total_rows,         -- includes NULLs
  COUNT(email) AS rows_with_email -- excludes NULLs
FROM customers;
\`\`\`

**NULLs in ORDER BY:** NULLs appear first with ORDER BY ASC by default in most databases. Control with NULLS FIRST / NULLS LAST.`
  },
  {
    id: 'indexes',
    keywords: ['index','indexes','b-tree','explain','seq scan','query plan','slow query','performance','optimize'],
    title: 'Indexes and Query Performance',
    answer: `An index lets the database find rows without scanning the entire table. Like a book's index — jump straight to the page instead of reading every page.

**Without index — Seq Scan (slow):**
\`\`\`sql
EXPLAIN SELECT * FROM orders WHERE customer_id = 'C123';
-- Seq Scan on orders (cost=0..48000 rows=50000...)
-- Database reads EVERY row
\`\`\`

**With index — Index Scan (fast):**
\`\`\`sql
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
-- Now: Index Scan (cost=0..8 rows=3...)
\`\`\`

**When to add an index:**
- Columns in WHERE clauses
- Columns in JOIN ON conditions
- Columns in ORDER BY (if you're filtering first)
- Foreign key columns

**When NOT to add indexes:**
- Small tables (full scan is faster)
- Columns with very few unique values (e.g., status = 'active'/'inactive')
- Tables with heavy write load (indexes slow down INSERT/UPDATE/DELETE)

**EXPLAIN ANALYZE — see what's actually happening:**
\`\`\`sql
EXPLAIN ANALYZE
SELECT * FROM orders WHERE customer_id = 'C123' AND status = 'completed';
\`\`\`
Look for: Seq Scan on large tables (bad), high actual rows vs estimated rows (means stale stats — run ANALYZE).

**Composite index column order matters:**
Index on (year, month) helps queries filtering by year OR year+month. It does NOT help queries filtering by month alone.`
  },
  {
    id: 'subqueries',
    keywords: ['subquery','nested query','correlated subquery','inline view','exists','in subquery'],
    title: 'Subqueries',
    answer: `A subquery is a SELECT statement inside another query. Three types matter:

**1. Scalar subquery — returns one value:**
\`\`\`sql
-- Orders above average
SELECT order_id, total_amount
FROM orders
WHERE total_amount > (SELECT AVG(total_amount) FROM orders);
\`\`\`

**2. Table subquery (inline view) — returns a result set:**
\`\`\`sql
SELECT customer_id, total_spent
FROM (
  SELECT customer_id, SUM(total_amount) AS total_spent
  FROM orders GROUP BY customer_id
) AS customer_totals
WHERE total_spent > 1000;
-- Better written as a CTE
\`\`\`

**3. Correlated subquery — references the outer query (runs once per row — SLOW on large tables):**
\`\`\`sql
SELECT o.order_id, o.customer_id
FROM orders o
WHERE o.order_date = (
  SELECT MAX(o2.order_date) FROM orders o2
  WHERE o2.customer_id = o.customer_id  -- references outer query
);
-- Use ROW_NUMBER() window function instead for large tables
\`\`\`

**EXISTS vs IN:**
\`\`\`sql
-- EXISTS stops at first match — faster for large subquery results
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)

-- IN loads all subquery results into memory first
WHERE customer_id IN (SELECT customer_id FROM orders)
\`\`\`

**Practical rule:** When you catch yourself writing nested subqueries, rewrite as CTEs. Much more readable and maintainable.`
  },
  {
    id: 'views-materialized',
    keywords: ['view','views','materialized view','create view','refresh','virtual table'],
    title: 'Views and Materialized Views',
    answer: `A view is a saved SELECT query that you can query like a table. No data is stored — it runs the underlying query every time.

**Regular View:**
\`\`\`sql
CREATE OR REPLACE VIEW vw_active_customers AS
SELECT customer_id, name, email
FROM customers
WHERE status = 'active';

-- Query it like a table
SELECT * FROM vw_active_customers WHERE city = 'Cairo';
\`\`\`

**Use views for:**
- Simplifying complex joins that analysts run repeatedly
- Hiding sensitive columns (create view without salary column)
- Creating a stable interface — underlying table can change, view stays the same

**Materialized View — stores the result physically:**
\`\`\`sql
CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT DATE_TRUNC('day', order_date) AS day,
       SUM(total_amount) AS revenue
FROM orders GROUP BY 1;

-- Must refresh to update data
REFRESH MATERIALIZED VIEW mv_daily_revenue;

-- Add index for faster queries
CREATE INDEX ON mv_daily_revenue(day);
\`\`\`

**Regular View vs Materialized View:**
| | Regular View | Materialized View |
|---|---|---|
| Data stored | No | Yes |
| Query speed | Depends on base table | Fast (pre-computed) |
| Data freshness | Always current | Stale until refreshed |
| Use when | Simple queries | Heavy aggregations queried often |

Materialized views are the database-native equivalent of dbt incremental models.`
  },
  {
    id: 'merge-upsert',
    keywords: ['merge','upsert','insert on conflict','on duplicate key','upsert pattern','update or insert'],
    title: 'MERGE / UPSERT Pattern',
    answer: `MERGE (also called UPSERT) inserts a row if it doesn't exist, or updates it if it does. This is the foundation of idempotent pipelines.

**PostgreSQL — INSERT ... ON CONFLICT:**
\`\`\`sql
INSERT INTO dim_customer (customer_id, name, city, updated_at)
VALUES ('C123', 'Ahmed', 'Cairo', NOW())
ON CONFLICT (customer_id)
DO UPDATE SET
  name = EXCLUDED.name,
  city = EXCLUDED.city,
  updated_at = EXCLUDED.updated_at;
-- EXCLUDED refers to the row that was attempted to be inserted
\`\`\`

**SQL Standard MERGE (works in Snowflake, BigQuery, SQL Server):**
\`\`\`sql
MERGE INTO dim_customer AS target
USING staging_customer AS source
  ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET name = source.name, city = source.city
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, city) VALUES (source.customer_id, source.name, source.city);
\`\`\`

**Why MERGE makes pipelines idempotent:**
Run the same MERGE 10 times with the same source data → same result every time. No duplicates, no missing rows.

**Pattern for incremental loads:**
1. Load new/changed records into a staging table
2. MERGE staging into the target table
3. Safe to rerun if anything fails`
  },
  {
    id: 'stored-procedures',
    keywords: ['stored procedure','procedure','function','plpgsql','create function','call','pl/sql'],
    title: 'Stored Procedures and Functions',
    answer: `A function returns a value and can be used in SELECT. A procedure executes logic and is called with CALL.

**Function — returns a value:**
\`\`\`sql
CREATE OR REPLACE FUNCTION classify_customer(p_total NUMERIC)
RETURNS VARCHAR AS $$
BEGIN
  IF p_total > 10000 THEN RETURN 'Gold';
  ELSIF p_total > 5000 THEN RETURN 'Silver';
  ELSE RETURN 'Bronze';
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Use in a query
SELECT customer_id, classify_customer(total_spent) AS tier FROM customer_totals;
\`\`\`

**Procedure — executes multi-step logic:**
\`\`\`sql
CREATE OR REPLACE PROCEDURE refresh_daily_stats()
LANGUAGE plpgsql AS $$
BEGIN
  TRUNCATE TABLE daily_stats;
  INSERT INTO daily_stats
    SELECT DATE_TRUNC('day', order_date), SUM(amount), COUNT(*)
    FROM orders WHERE order_date = CURRENT_DATE - 1
    GROUP BY 1;
  INSERT INTO pipeline_log(run_date, status) VALUES(CURRENT_DATE, 'success');
  COMMIT;
END;
$$;

CALL refresh_daily_stats();
\`\`\`

**When to use them in DE:**
- Procedures: encapsulate multi-step pipeline logic (truncate + load + log)
- Functions: create reusable business logic (tier classification, custom transformations)
- Both make SQL pipelines testable and reusable`
  },

  // ============================================================
  // PYTHON FOR DATA ENGINEERING
  // ============================================================
  {
    id: 'python-setup',
    keywords: ['venv','virtual environment','pip','requirements','setup','install','python setup','env'],
    title: 'Python Setup for DE Projects',
    answer: `Always use a virtual environment. Never install packages globally — different projects need different versions.

**Create and activate:**
\`\`\`bash
python3 -m venv venv
source venv/bin/activate        # Mac/Linux
venv\\Scripts\\activate           # Windows

pip install pandas psycopg2-binary sqlalchemy requests python-dotenv
pip freeze > requirements.txt   # save exact versions
\`\`\`

**Project structure:**
\`\`\`
my_pipeline/
  venv/              # never commit this
  src/
    extract.py
    transform.py
    load.py
  dags/              # Airflow DAGs
  .env               # secrets — never commit
  .gitignore         # includes venv/ and .env
  requirements.txt
\`\`\`

**.env file for secrets:**
\`\`\`
DB_HOST=localhost
DB_USER=postgres
DB_PASSWORD=mysecret
DB_NAME=mydb
\`\`\`

**Load in Python:**
\`\`\`python
from dotenv import load_dotenv
import os

load_dotenv()
db_host = os.getenv('DB_HOST')
\`\`\`

**Golden rule:** Never hardcode credentials in Python files. Always use environment variables or a secrets manager. If you accidentally commit a password to GitHub, rotate it immediately.`
  },
  {
    id: 'python-pandas',
    keywords: ['pandas','dataframe','read_csv','merge','groupby','fillna','dropna','pandas tutorial','df'],
    title: 'Pandas for Data Engineering',
    answer: `Pandas is Python's data manipulation library. For DE, you use it to clean, transform, and move data.

**Load data:**
\`\`\`python
import pandas as pd

df = pd.read_csv('orders.csv')
df = pd.read_json('data.json')
df = pd.read_parquet('data.parquet')
\`\`\`

**Inspect:**
\`\`\`python
df.head()           # first 5 rows
df.shape            # (rows, columns)
df.dtypes           # column types
df.info()           # types + null counts
df.isnull().sum()   # count nulls per column
\`\`\`

**Clean:**
\`\`\`python
df = df.dropna(subset=['customer_id'])       # drop rows where customer_id is null
df['city'] = df['city'].fillna('Unknown')    # fill nulls
df = df.drop_duplicates(subset=['order_id']) # remove duplicates
df['order_date'] = pd.to_datetime(df['order_date'])
df['name'] = df['name'].str.strip().str.upper()
\`\`\`

**Transform:**
\`\`\`python
# Group and aggregate
summary = df.groupby('customer_id').agg(
  order_count=('order_id', 'count'),
  total_spent=('amount', 'sum')
).reset_index()

# Merge (like SQL JOIN)
merged = pd.merge(orders, customers, on='customer_id', how='left')
\`\`\`

**Write to database:**
\`\`\`python
from sqlalchemy import create_engine
engine = create_engine('postgresql://user:pass@localhost/db')
df.to_sql('staging_orders', engine, if_exists='replace', index=False)
\`\`\`

**Memory tip:** For large files, use chunked reading:
\`\`\`python
for chunk in pd.read_csv('huge.csv', chunksize=10000):
    process(chunk)
\`\`\``
  },
  {
    id: 'python-postgres',
    keywords: ['psycopg2','postgresql','postgres','connect','database','python database','sqlalchemy','connection string'],
    title: 'Connecting Python to PostgreSQL',
    answer: `Two libraries: psycopg2 (raw SQL control) and SQLAlchemy (ORM + pandas integration).

**psycopg2 — direct SQL execution:**
\`\`\`python
import psycopg2
import os

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
cursor = conn.cursor()

# Execute query
cursor.execute("SELECT * FROM orders WHERE status = %s", ('completed',))
rows = cursor.fetchall()

# Insert data
cursor.execute(
    "INSERT INTO logs (run_date, status) VALUES (%s, %s)",
    ('2024-01-15', 'success')
)
conn.commit()   # Must commit to persist changes
cursor.close()
conn.close()
\`\`\`

**ALWAYS use %s placeholders** — never f-strings or .format() for SQL values (SQL injection risk).

**SQLAlchemy — works with pandas + higher level:**
\`\`\`python
from sqlalchemy import create_engine

engine = create_engine(
    'postgresql://user:password@localhost:5432/mydb'
)

# With pandas
df.to_sql('staging_orders', engine, if_exists='replace', index=False)
df = pd.read_sql('SELECT * FROM orders', engine)
\`\`\`

**Connection string format:**
\`\`\`
postgresql://username:password@host:port/database
\`\`\`

**Best practice — context manager:**
\`\`\`python
with psycopg2.connect(**conn_params) as conn:
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
# Connection auto-closes even if error occurs
\`\`\``
  },
  {
    id: 'python-apis',
    keywords: ['requests','api','rest','http','get','post','pagination','api call','json','endpoint'],
    title: 'Calling REST APIs in Python',
    answer: `The requests library is the standard for API calls in Python.

**Basic GET request:**
\`\`\`python
import requests

response = requests.get(
    'https://api.example.com/orders',
    headers={'Authorization': 'Bearer YOUR_TOKEN'},
    params={'status': 'completed', 'limit': 100}
)
response.raise_for_status()  # raises exception if 4xx/5xx
data = response.json()
\`\`\`

**Handling pagination (offset-based):**
\`\`\`python
all_records = []
page = 1

while True:
    response = requests.get(url, params={'page': page, 'per_page': 100})
    data = response.json()

    if not data['results']:
        break

    all_records.extend(data['results'])
    page += 1

df = pd.DataFrame(all_records)
\`\`\`

**Retry on failure:**
\`\`\`python
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
session.mount('https://', HTTPAdapter(max_retries=retry))
\`\`\`

**Error handling:**
\`\`\`python
try:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()
except requests.exceptions.Timeout:
    logging.error("API timed out")
    raise
except requests.exceptions.HTTPError as e:
    logging.error(f"HTTP error: {e.response.status_code}")
    raise
\`\`\``
  },
  {
    id: 'python-error-handling',
    keywords: ['error handling','try except','exception','logging','try','except','finally','errors'],
    title: 'Error Handling and Logging in Python',
    answer: `Production pipelines must catch errors gracefully and log everything. Silent failures are the worst kind — your pipeline "ran" but loaded garbage data.

**Try/except/finally:**
\`\`\`python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()  # also print to console
    ]
)

def extract_data(source):
    try:
        logging.info(f"Starting extraction from {source}")
        data = fetch_from_api(source)
        logging.info(f"Extracted {len(data)} records")
        return data
    except requests.exceptions.Timeout:
        logging.error(f"Timeout connecting to {source}")
        raise                      # re-raise so caller knows it failed
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        raise
    finally:
        logging.info("Extraction step complete")
        # finally always runs — use for cleanup (close connections, etc.)
\`\`\`

**Log levels:**
- DEBUG: detailed diagnostic info (dev only)
- INFO: normal operations ("Loaded 5000 rows")
- WARNING: something unexpected but recoverable
- ERROR: something failed (pipeline continues if possible)
- CRITICAL: fatal error (pipeline should stop)

**What to always log:**
- Start and end of each pipeline stage
- Number of rows extracted/loaded
- Any errors with full stack trace (exc_info=True)
- Duration of slow operations`
  },
  {
    id: 'parquet',
    keywords: ['parquet','columnar','file format','compression','snappy','row format','parquet vs csv'],
    title: 'Parquet File Format',
    answer: `Parquet is a columnar file format designed for analytics. It's the standard format for data lakes and Spark pipelines.

**Row format (CSV) vs Columnar format (Parquet):**
- CSV stores data row by row: [row1_col1, row1_col2, row1_col3], [row2_col1...]
- Parquet stores data column by column: [all_col1_values], [all_col2_values]...

**Why columnar is better for analytics:**
If you query SELECT SUM(revenue) FROM orders, you only need the revenue column. Parquet reads just that column. CSV reads every column on every row — wasteful.

**Parquet advantages:**
- 5-10x smaller than CSV (same data, much better compression)
- Faster analytics queries (column pruning)
- Schema embedded in file (no guessing data types)
- Supported natively by Spark, BigQuery, Snowflake, Pandas

**Write and read with Python:**
\`\`\`python
import pandas as pd

# Write
df.to_parquet('orders.parquet', compression='snappy', index=False)

# Read
df = pd.read_parquet('orders.parquet')

# Read only specific columns (massive performance gain)
df = pd.read_parquet('orders.parquet', columns=['order_id', 'revenue'])
\`\`\`

**Partitioned Parquet (for large datasets):**
\`\`\`python
# PySpark — partition by date so queries filter efficiently
df.write.partitionBy('year', 'month').parquet('s3://bucket/orders/')
# Creates: orders/year=2024/month=01/part-000.parquet
\`\`\`

**Interview answer:** "I use Parquet for all intermediate storage in pipelines — it's 10x smaller than CSV and Spark reads it 10x faster because it only loads the columns the query needs."`
  },

  // ============================================================
  // AIRFLOW
  // ============================================================
  {
    id: 'airflow-basics',
    keywords: ['airflow','dag','operator','task','schedule','orchestration','workflow','airflow basics'],
    title: 'Apache Airflow Basics',
    answer: `Airflow is a workflow orchestration tool. You write Python files called DAGs that define tasks and their dependencies. Airflow schedules and monitors them.

**DAG = Directed Acyclic Graph.** Tasks are nodes, dependencies are edges. "Acyclic" means no circular dependencies.

**Minimal DAG:**
\`\`\`python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'marwan',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}

with DAG(
    dag_id='daily_orders_pipeline',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # 6am daily
    start_date=datetime(2024, 1, 1),
    catchup=False,                   # IMPORTANT: don't backfill
) as dag:

    extract = PythonOperator(task_id='extract', python_callable=run_extract)
    transform = PythonOperator(task_id='transform', python_callable=run_transform)
    load = PythonOperator(task_id='load', python_callable=run_load)

    extract >> transform >> load    # dependency chain
\`\`\`

**catchup=False is critical.** Without it, if your DAG's start_date is 6 months ago, Airflow runs 180 missed executions the moment you turn it on.

**Key operators:** PythonOperator (run Python), BashOperator (run shell), PostgresOperator (run SQL), EmailOperator (send email).

**Connections:** Store database credentials in Airflow UI → Admin → Connections. Reference by conn_id in operators. Never hardcode credentials in DAG files.`
  },
  {
    id: 'airflow-xcoms',
    keywords: ['xcom','xcoms','push','pull','pass data','task communication','airflow xcom'],
    title: 'Airflow XComs — Passing Data Between Tasks',
    answer: `XComs (Cross-Communications) let Airflow tasks share small values between each other.

**Push a value from a task:**
\`\`\`python
def extract(**context):
    records = fetch_data()
    row_count = len(records)
    # Push to XCom
    context['ti'].xcom_push(key='row_count', value=row_count)
    return records  # return value is automatically pushed as 'return_value'
\`\`\`

**Pull in a downstream task:**
\`\`\`python
def validate(**context):
    row_count = context['ti'].xcom_pull(
        task_ids='extract',
        key='row_count'
    )
    if row_count == 0:
        raise ValueError("No data extracted!")
\`\`\`

**Important limitations:**
- XComs are stored in Airflow's metadata database
- NOT designed for large data — use for small values only (counts, filenames, status flags)
- For large datasets, pass file paths or S3 keys between tasks, not the actual data

**execution_date gotcha:**
\`\`\`python
# execution_date is the START of the scheduled interval, not when the task ran
# For a DAG scheduled at 6am on Jan 15, execution_date = Jan 14 (previous interval)
# Use data_interval_start/end in Airflow 2.x for clarity
\`\`\``
  },

  // ============================================================
  // SPARK
  // ============================================================
  {
    id: 'spark-basics',
    keywords: ['spark','pyspark','distributed','rdd','dataframe','spark session','spark basics'],
    title: 'Apache Spark / PySpark Basics',
    answer: `Spark is a distributed computing framework for processing large datasets that don't fit on a single machine. PySpark is the Python API for Spark.

**When to use Spark instead of pandas:**
- Dataset > a few GB (pandas loads everything into RAM)
- Need distributed processing across a cluster
- Reading from data lake (Parquet on S3/GCS)

**SparkSession — entry point:**
\`\`\`python
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("OrdersPipeline") \\
    .config("spark.sql.shuffle.partitions", "200") \\
    .getOrCreate()
\`\`\`

**Read and inspect:**
\`\`\`python
df = spark.read.parquet("s3://bucket/orders/")
df = spark.read.csv("orders.csv", header=True, inferSchema=True)

df.printSchema()
df.show(5)
df.count()
\`\`\`

**Common transformations:**
\`\`\`python
from pyspark.sql import functions as F

df = df.filter(F.col("status") == "completed")
df = df.select("order_id", "customer_id", "amount")
df = df.withColumn("year", F.year(F.col("order_date")))
df = df.groupBy("customer_id").agg(F.sum("amount").alias("total_spent"))
\`\`\`

**Write output:**
\`\`\`python
df.write.mode("overwrite").partitionBy("year", "month").parquet("output/")
\`\`\``
  },
  {
    id: 'spark-lazy',
    keywords: ['lazy evaluation','transformation','action','lazy','dag spark','spark execution','explain'],
    title: 'Spark Lazy Evaluation',
    answer: `Spark is lazy — transformations don't execute immediately. They build a logical plan. Execution only happens when you call an action.

**Transformations (lazy — build the plan):**
filter(), select(), withColumn(), groupBy(), join(), map()

**Actions (trigger execution):**
count(), show(), collect(), write(), first(), take()

\`\`\`python
# These 3 lines do NOTHING yet — just build a plan
df = spark.read.parquet("orders.parquet")
df = df.filter(df.status == "completed")
df = df.groupBy("customer_id").agg({"amount": "sum"})

# THIS triggers execution — Spark runs the entire plan
df.count()
\`\`\`

**Why this matters:**
Spark optimizes the entire plan before executing. It can reorder operations, push filters down to the data source, and skip reading columns it doesn't need.

**See the plan:**
\`\`\`python
df.explain(True)
# Shows: Parsed Plan → Analyzed Plan → Optimized Plan → Physical Plan
\`\`\`

**Shuffle (the expensive part):**
When Spark needs to redistribute data across nodes (for groupBy, join, distinct), it's called a shuffle. Shuffles read/write to disk and network — they're expensive. Minimize them.

**spark.sql.shuffle.partitions:**
\`\`\`python
# Default is 200 — too many for small datasets, too few for large
# Rule of thumb: 2-4 partitions per CPU core
spark.conf.set("spark.sql.shuffle.partitions", "50")
\`\`\``
  },
  {
    id: 'spark-partitions',
    keywords: ['partition','repartition','coalesce','partitionby','data skew','skew','broadcast join','shuffle partitions'],
    title: 'Spark Partitions and Data Skew',
    answer: `Partitions are chunks of data that Spark processes in parallel. More partitions = more parallelism. But too many small partitions = overhead.

**Check and change partitions:**
\`\`\`python
df.rdd.getNumPartitions()        # how many partitions now

df = df.repartition(100)         # increase partitions (causes shuffle)
df = df.coalesce(10)             # decrease partitions (no shuffle — just merge)
\`\`\`

**Rule:** Use repartition() to increase. Use coalesce() to decrease before writing output.

**Data Skew (the silent killer):**
Skew happens when some partitions have far more data than others. One partition takes 10 minutes while 199 others finish in 30 seconds. Your job is as slow as the slowest partition.

**Diagnosing skew:** In the Spark UI, look at task duration on the Stages tab — if one task takes 100x longer than others, you have skew.

**Fix 1 — Broadcast join (for small tables):**
\`\`\`python
from pyspark.sql.functions import broadcast

# If customers table is small (< 100MB), broadcast it
result = orders.join(broadcast(customers), "customer_id")
# Sends customers to every executor — eliminates shuffle entirely
\`\`\`

**Fix 2 — Salting (for large skewed tables):**
\`\`\`python
# Add random salt to distribute skewed keys
from pyspark.sql.functions import concat, col, lit, rand, floor

# Add salt column to break up hot key
df = df.withColumn("salt", (floor(rand() * 10)).cast("int"))
df = df.withColumn("salted_key", concat(col("customer_id"), lit("_"), col("salt")))
\`\`\``
  },

  // ============================================================
  // DBT
  // ============================================================
  {
    id: 'dbt-basics',
    keywords: ['dbt','data build tool','dbt model','ref','materialization','dbt run','dbt test','transform sql'],
    title: 'dbt (data build tool) Basics',
    answer: `dbt is a transformation tool that turns SQL SELECT statements into production-ready tables and views in your warehouse. It's in ~60% of junior DE job postings.

**Core concept:** Write SELECT statements. dbt handles the CREATE TABLE / INSERT / DROP logic.

**dbt model — just a SELECT file:**
\`\`\`sql
-- models/staging/stg_orders.sql
SELECT
  order_id,
  customer_id,
  amount,
  CAST(order_date AS DATE) AS order_date,
  UPPER(status) AS status
FROM {{ source('raw', 'orders') }}
WHERE status != 'cancelled'
\`\`\`

**ref() — creates dependency between models:**
\`\`\`sql
-- models/core/fact_orders.sql
SELECT o.order_id, c.customer_sk, o.amount
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('dim_customer') }} c USING (customer_id)
-- dbt knows stg_orders must run before fact_orders
\`\`\`

**Materializations:**
- \`view\` (default): CREATE VIEW — runs query every time
- \`table\`: DROP + CREATE TABLE — faster queries
- \`incremental\`: only process new rows — efficient for large tables
- \`ephemeral\`: just a CTE inline, not persisted

**Commands:**
\`\`\`bash
dbt run           # run all models
dbt test          # run all data tests
dbt run -s stg_orders  # run one model
dbt docs generate && dbt docs serve  # lineage graph
\`\`\`

**Tests:**
\`\`\`yaml
# schema.yml
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests: [not_null, unique]
      - name: status
        tests:
          - accepted_values:
              values: ['COMPLETED', 'PENDING', 'FAILED']
\`\`\``
  },

  // ============================================================
  // DOCKER
  // ============================================================
  {
    id: 'docker-basics',
    keywords: ['docker','container','dockerfile','docker-compose','image','compose','containerize'],
    title: 'Docker for Data Engineers',
    answer: `Docker packages your application and all its dependencies into a container — it runs the same way on your laptop, your colleague's machine, and the cloud server.

**Why DEs use Docker:**
- Run Airflow locally without complex installation
- Package Python pipelines so they run identically in production
- Spin up a test PostgreSQL database in seconds

**Key concepts:**
- **Image**: Blueprint/template (like a class in OOP)
- **Container**: Running instance of an image (like an object)
- **Dockerfile**: Instructions to build an image

**Most useful commands:**
\`\`\`bash
docker ps                           # running containers
docker images                       # local images
docker run postgres:15              # run a container
docker stop container_id            # stop it
docker exec -it container_id bash   # get a shell inside
docker logs container_id            # see output
\`\`\`

**docker-compose — run multiple containers together:**
\`\`\`yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_PASSWORD: password
      POSTGRES_DB: mydb
    ports:
      - "5432:5432"

  airflow:
    image: apache/airflow:2.7.0
    depends_on: [postgres]
    ports:
      - "8080:8080"
\`\`\`

\`\`\`bash
docker-compose up -d    # start everything in background
docker-compose down     # stop and remove containers
\`\`\`

**How Airflow local setup works:** The official Airflow docker-compose.yaml starts Airflow Webserver, Scheduler, Worker, and a PostgreSQL metadata database — all with one command.`
  }
];

// ============================================================
// SEARCH ENGINE
// ============================================================

function botTokenize(text) {
  return text.toLowerCase()
    .replace(/[^\w\s]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length > 1);
}

function scoreKB(entry, tokens) {
  let score = 0;
  const titleTokens = botTokenize(entry.title);
  tokens.forEach(t => {
    if (entry.keywords.some(k => k === t || k.includes(t) || t.includes(k))) score += 4;
    if (titleTokens.some(k => k === t || k.includes(t))) score += 3;
    if (entry.answer.toLowerCase().includes(t)) score += 1;
  });
  return score;
}

function searchPlatformData(tokens) {
  const results = [];

  // Search KNOWLEDGE topics
  if (typeof KNOWLEDGE !== 'undefined') {
    KNOWLEDGE.forEach(k => {
      let score = 0;
      const haystack = (k.title + ' ' + k.subtitle + ' ' + JSON.stringify(k.points)).toLowerCase();
      tokens.forEach(t => { if (haystack.includes(t)) score += 2; });
      if (score > 0) results.push({ score, type: 'knowledge', data: k });
    });
  }

  // Search SQL_TOPICS
  if (typeof SQL_TOPICS !== 'undefined') {
    SQL_TOPICS.forEach(t => {
      let score = 0;
      const haystack = (t.title + ' ' + t.summary + ' ' + t.explanation).toLowerCase();
      tokens.forEach(tok => { if (haystack.includes(tok)) score += 2; });
      if (score > 0) results.push({ score, type: 'sql', data: t });
    });
  }

  // Search PYTHON_MODULES
  if (typeof PYTHON_MODULES !== 'undefined') {
    PYTHON_MODULES.forEach(m => {
      let score = 0;
      const haystack = (m.title + ' ' + m.subtitle + ' ' + m.summary + ' ' + m.points.join(' ')).toLowerCase();
      tokens.forEach(tok => { if (haystack.includes(tok)) score += 2; });
      if (score > 0) results.push({ score, type: 'python', data: m });
    });
  }

  // Search GAPS
  if (typeof GAPS !== 'undefined') {
    GAPS.forEach(g => {
      let score = 0;
      const haystack = (g.title + ' ' + g.subtitle + ' ' + g.why).toLowerCase();
      tokens.forEach(tok => { if (haystack.includes(tok)) score += 1; });
      if (score > 0) results.push({ score, type: 'gap', data: g });
    });
  }

  // Search INTERVIEW_QA from data-extra.js
  if (typeof INTERVIEW_QA !== 'undefined') {
    INTERVIEW_QA.forEach(topic => {
      topic.questions.forEach(q => {
        let score = 0;
        const haystack = (q.q + ' ' + q.a).toLowerCase();
        tokens.forEach(tok => { if (haystack.includes(tok)) score += 1; });
        if (score > 2) results.push({ score, type: 'iq', data: q, topic: topic.topic });
      });
    });
  }

  results.sort((a, b) => b.score - a.score);
  return results.slice(0, 3);
}

function formatPlatformResult(result) {
  const { type, data } = result;
  if (type === 'knowledge') {
    let text = `**${data.title}**\n${data.subtitle}\n\n`;
    data.points.slice(0, 4).forEach(p => { text += `**${p.label}:** ${p.text}\n\n`; });
    return text;
  }
  if (type === 'sql') {
    return `**${data.title}** (${data.tierLabel})\n\n${data.explanation}\n\n\`\`\`sql\n${data.example}\n\`\`\``;
  }
  if (type === 'python') {
    return `**${data.title}**\n${data.subtitle}\n\n${data.summary}\n\n- ${data.points.slice(0, 5).join('\n- ')}\n\n\`\`\`python\n${data.code.slice(0, 600)}${data.code.length > 600 ? '\n// ...' : ''}\n\`\`\``;
  }
  if (type === 'gap') {
    return `**${data.title}** — Gap to fill\n\n${data.why}\n\n${data.lesson ? data.lesson.intro : ''}`;
  }
  if (type === 'iq') {
    return `**Interview Q:** ${data.q}\n\n**Answer:** ${data.a}`;
  }
  return '';
}

const BOT_GREETINGS = ['hi','hello','hey','good morning','good evening','howdy','yo','sup','whats up'];
const BOT_THANKS    = ['thanks','thank you','thx','ty','thank'];
const BOT_HELP      = ['help','what can you do','commands','topics','list'];

function botRespond(userInput) {
  const raw   = userInput.trim();
  const lower = raw.toLowerCase();
  const tokens = botTokenize(raw);

  // Greeting
  if (BOT_GREETINGS.some(g => lower.includes(g)) && tokens.length <= 4) {
    return `Hey! I'm your DE Expert. Ask me anything about Data Engineering, SQL, Python, Airflow, Spark, dbt, or how to structure pipelines.\n\nSome examples:\n- "What is idempotency?"\n- "Explain window functions"\n- "How do I connect Python to PostgreSQL?"\n- "What's the difference between ETL and ELT?"`;
  }

  // Thanks
  if (BOT_THANKS.some(g => lower.includes(g)) && tokens.length <= 4) {
    return `You're welcome! Ask me anything else — SQL, Python, Airflow, Spark, pipeline design, or interview prep. I'm here.`;
  }

  // Help
  if (BOT_HELP.some(g => lower.includes(g))) {
    return `I can explain any Data Engineering topic. My knowledge covers:\n\n**Concepts:** OLTP/OLAP, ETL/ELT, Data Warehouse, Star Schema, SCD, Idempotency, ACID, Medallion Architecture, Incremental Loading, Parquet\n\n**SQL:** JOINs, Window Functions, CTEs, GROUP BY/HAVING, Subqueries, NULL handling, Indexes, Views, MERGE/UPSERT, Stored Procedures\n\n**Python:** pandas, psycopg2, SQLAlchemy, APIs, error handling, logging, virtual environments\n\n**Tools:** Airflow (DAGs, operators, XComs), PySpark (lazy eval, partitions, skew, broadcast), dbt (models, ref, materializations, tests), Docker\n\n**Career:** How to start, interview tips\n\nJust ask naturally — "explain X" or "what is X" or "how do I X".`;
  }

  // Search hand-crafted knowledge base
  const kbScores = BOT_KB.map(entry => ({ entry, score: scoreKB(entry, tokens) }))
    .filter(x => x.score > 0)
    .sort((a, b) => b.score - a.score);

  if (kbScores.length > 0 && kbScores[0].score >= 4) {
    return kbScores[0].entry.answer;
  }

  // Search platform data files
  const platformResults = searchPlatformData(tokens);
  if (platformResults.length > 0 && platformResults[0].score >= 4) {
    return formatPlatformResult(platformResults[0]);
  }

  // Weak match — try KB with lower threshold
  if (kbScores.length > 0 && kbScores[0].score >= 2) {
    return kbScores[0].entry.answer;
  }

  // Weak platform match
  if (platformResults.length > 0 && platformResults[0].score >= 2) {
    return formatPlatformResult(platformResults[0]);
  }

  // Fallback
  return `I don't have a specific answer for "${raw}" yet.\n\nTry rephrasing — for example:\n- "What is [topic]?"\n- "Explain [concept]"\n- "How does [tool] work?"\n\nOr type **help** to see all topics I know.`;
}
