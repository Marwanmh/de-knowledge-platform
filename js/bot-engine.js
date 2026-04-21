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
  },

  // ── STEP 2 ENTRIES ──────────────────────────────────────────

  {
    id: 'data-quality',
    title: 'Data Quality',
    keywords: ['data quality','data validation','data testing','great expectations','dq checks','data quality rules','bad data','dirty data','null check','uniqueness','completeness','accuracy'],
    related: ['dbt-basics','airflow-basics','data-pipeline'],
    answer: `**Data quality** means your data is accurate, complete, consistent, and timely. Bad data breaks dashboards, models, and business decisions.

**The 6 dimensions of data quality:**
| Dimension | Meaning | Example check |
|-----------|---------|---------------|
| Completeness | No missing values | \`COUNT(*) WHERE col IS NULL = 0\` |
| Uniqueness | No duplicates | \`COUNT(DISTINCT id) = COUNT(*)\` |
| Validity | Values in expected range | \`age BETWEEN 0 AND 120\` |
| Consistency | Same value across systems | Customer name matches in CRM and DWH |
| Timeliness | Data arrives on schedule | Latest record < 2 hours old |
| Accuracy | Reflects real-world truth | Revenue matches finance report |

**Implementing checks — three approaches:**

1. **SQL assertions (simplest):**
\`\`\`sql
-- Fail pipeline if any nulls in required column
SELECT COUNT(*) FROM orders WHERE customer_id IS NULL;
-- If result > 0 → raise alert
\`\`\`

2. **dbt tests (recommended for transform layer):**
\`\`\`yaml
models:
  - name: orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: status
        tests:
          - accepted_values:
              values: ['pending','completed','cancelled']
\`\`\`

3. **Great Expectations (Python, full framework):**
\`\`\`python
import great_expectations as ge
df = ge.read_csv("orders.csv")
df.expect_column_values_to_not_be_null("customer_id")
df.expect_column_values_to_be_unique("order_id")
results = df.validate()
print(results["success"])  # True / False
\`\`\`

**Where to add checks in a pipeline:**
- **Source** — validate raw data before loading (fail fast)
- **Transform** — dbt tests after each model
- **Output** — assert final table row counts, freshness

**Golden rule:** if a pipeline doesn't fail on bad data, bad data will silently corrupt your warehouse.`
  },

  {
    id: 'data-modeling',
    title: 'Data Modeling',
    keywords: ['data modeling','data model','star schema','snowflake schema','dimension table','fact table','dim','fct','slowly changing dimension','scd','third normal form','3nf','denormalization','normalization','er diagram','erd'],
    related: ['dbt-basics','data-warehouse','sql-basics'],
    answer: `**Data modeling** is designing how data is structured in a database or warehouse for storage, querying, and analysis.

**Two main approaches:**

**1. Normalized (3NF) — for OLTP (transactional)**
- Eliminate redundancy, split into many small tables
- Every table has one purpose, foreign keys link them
- Good for write-heavy systems (CRMs, order systems)
- Bad for analytics: many JOINs = slow queries

**2. Dimensional Modeling — for OLAP (analytics)**
Built around **fact tables** and **dimension tables**:

\`\`\`
Fact Table (fct_orders)          Dimension Tables
─────────────────────           ─────────────────
order_id (PK)                   dim_customers: customer_id, name, city
customer_id (FK) ───────────►   dim_products:  product_id, name, category
product_id  (FK) ───────────►   dim_dates:     date_id, year, month, day
date_id     (FK) ───────────►
amount
quantity
\`\`\`

**Star Schema** = fact table directly joined to flat dimension tables (fast, simple, slight redundancy)
**Snowflake Schema** = dimensions further normalized into sub-dimensions (less redundancy, more joins)

**Slowly Changing Dimensions (SCD):**
| Type | Behavior | Use case |
|------|----------|----------|
| SCD 0 | Never change (overwrite) | Product SKU |
| SCD 1 | Overwrite old value | Fix typos |
| SCD 2 | Add new row, flag old | Customer address history |
| SCD 3 | Add new column | Track only last 2 values |

**SCD Type 2 example:**
\`\`\`sql
-- Old row gets end date + is_current = false
-- New row gets new values + is_current = true
INSERT INTO dim_customers VALUES
  (customer_id, 'Old City', '2024-01-01', '2025-03-15', FALSE),
  (customer_id, 'New City', '2025-03-15', NULL,          TRUE);
\`\`\`

**In dbt** you'd use the \`dbt_utils\` or \`snapshot\` feature to handle SCD 2 automatically.`
  },

  {
    id: 'data-warehouse',
    title: 'Data Warehouse Concepts',
    keywords: ['data warehouse','dwh','olap','redshift','bigquery','snowflake','databricks','data lakehouse','data lake','warehouse vs lake','columnar storage','row vs column','analytical database'],
    related: ['data-modeling','etl-elt','spark-basics'],
    answer: `**Data Warehouse (DWH):** central repository for structured, historical data optimized for analytical queries.

**Row vs Columnar storage:**
| | Row Store | Columnar Store |
|-|-----------|----------------|
| **Reads** | Full row at once | Only requested columns |
| **Best for** | OLTP (INSERT/UPDATE) | OLAP (SELECT + aggregations) |
| **Example** | PostgreSQL, MySQL | BigQuery, Redshift, Snowflake |

Why columnar is faster for analytics: \`SELECT SUM(revenue) FROM orders\` reads only the revenue column — skips all other columns entirely.

**Major cloud DWHs compared:**
| | BigQuery | Snowflake | Redshift | Databricks |
|-|----------|-----------|----------|------------|
| **Compute** | Serverless | Virtual warehouses | Node clusters | Spark clusters |
| **Storage** | Separated | Separated | Coupled | Separated |
| **Best for** | GCP users, ad-hoc | Multi-cloud, BI | AWS-heavy orgs | ML + data eng |
| **Pricing** | Per query TB | Per compute-second | Per node-hour | DBU |

**Data Lake vs Data Warehouse vs Lakehouse:**
- **Data Lake:** raw files (Parquet, JSON, CSV) in S3/GCS — cheap, flexible, schema-on-read
- **Data Warehouse:** structured, curated, schema-on-write — fast queries, governed
- **Lakehouse:** combines both — open format (Delta/Iceberg) + warehouse-style ACID + SQL engine

**Layers in a modern DWH (medallion architecture):**
\`\`\`
Bronze (raw)  →  Silver (cleaned)  →  Gold (aggregated/business-ready)
    S3/GCS           dbt models            BI tools / dashboards
\`\`\`

**Partitioning in cloud DWHs:**
\`\`\`sql
-- BigQuery: partition by date column
CREATE TABLE dataset.orders
PARTITION BY DATE(created_at)
AS SELECT * FROM ...;
-- Queries filtering on created_at skip entire partitions → 10-100x cheaper
\`\`\``
  },

  {
    id: 'etl-elt',
    title: 'ETL vs ELT',
    keywords: ['etl','elt','extract transform load','extract load transform','etl vs elt','pipeline','data pipeline','transformation','loading data','ingestion'],
    related: ['airflow-basics','dbt-basics','data-warehouse'],
    answer: `**ETL** = Extract → Transform → Load (transform *before* loading into warehouse)
**ELT** = Extract → Load → Transform (load raw first, transform *inside* the warehouse)

**Why ELT won for modern stacks:**
| | ETL | ELT |
|-|-----|-----|
| **Where transform?** | Separate server | Inside DWH (BigQuery/Snowflake) |
| **Storage cost** | Lower (only clean data) | Higher (keep raw too) |
| **Flexibility** | Rigid — re-run if logic changes | Re-transform anytime, raw still there |
| **Tools** | Informatica, SSIS, custom scripts | dbt + Fivetran/Airbyte |
| **Best for** | Legacy systems, compliance | Modern cloud DWH |

**ETL pipeline in Python (simple example):**
\`\`\`python
# Extract
df = pd.read_csv("s3://bucket/raw/orders.csv")

# Transform
df = df.dropna(subset=["order_id"])
df["revenue"] = df["quantity"] * df["unit_price"]
df["created_at"] = pd.to_datetime(df["created_at"])

# Load
df.to_sql("orders_clean", engine, if_exists="replace", index=False)
\`\`\`

**ELT pipeline (dbt does the T):**
\`\`\`
Fivetran/Airbyte → loads raw to BigQuery → dbt transforms inside BigQuery
\`\`\`

**Pipeline stages you should always have:**
1. **Ingestion** — get data from source to raw storage
2. **Validation** — check data quality before transform
3. **Transform** — clean, join, aggregate
4. **Load / Publish** — write to final table for consumption
5. **Monitoring** — alert on failure, row count drops, freshness`
  },

  {
    id: 'debugging-pipelines',
    title: 'Debugging Data Pipelines',
    keywords: ['debug','debugging','pipeline failed','airflow error','pipeline error','troubleshoot','fix pipeline','task failed','dag failed','error handling','retry','backfill','idempotent'],
    related: ['airflow-basics','airflow-xcoms','python-error-handling'],
    answer: `**Debugging data pipelines** is a core DE skill. Most failures fall into a few categories.

**Step 1 — Read the actual error message:**
\`\`\`bash
# Airflow: check task logs
airflow tasks logs <dag_id> <task_id> <execution_date>

# Docker: check container logs
docker logs <container_name> --tail 100

# Python: always log with context
import logging
logger = logging.getLogger(__name__)
logger.error("Failed to load orders: %s", str(e), exc_info=True)
\`\`\`

**Common failure categories:**

| Category | Symptom | Fix |
|----------|---------|-----|
| Connection | "Connection refused" / timeout | Check host/port/credentials, network firewall |
| Data | Null constraint, type mismatch | Add validation before load, check source schema changes |
| Memory | OOM kill, Spark executor lost | Increase executor memory, reduce partition size |
| Permissions | "Access denied" / 403 | Check IAM role, service account, DB grants |
| Logic | Wrong row counts, bad aggregations | Unit test transforms, compare with source |

**Idempotency — the most important pipeline property:**
An idempotent pipeline produces the same result no matter how many times it runs.
\`\`\`python
# BAD — appends duplicates on retry
df.to_sql("orders", engine, if_exists="append")

# GOOD — safe to re-run
df.to_sql("orders", engine, if_exists="replace")

# GOOD for incremental — upsert
INSERT INTO orders ... ON CONFLICT (order_id) DO UPDATE SET ...
\`\`\`

**Backfilling in Airflow:**
\`\`\`bash
# Re-run a date range after fixing a bug
airflow dags backfill my_dag --start-date 2025-01-01 --end-date 2025-01-31
\`\`\`

**Debugging checklist:**
1. Read full error (not just last line)
2. Check if data changed at source (schema drift)
3. Reproduce locally with a small sample
4. Add logging at each step to narrow it down
5. Fix root cause — not just the symptom`
  },

  {
    id: 'incremental-loading',
    title: 'Incremental Loading',
    keywords: ['incremental','incremental load','incremental pipeline','watermark','change data capture','cdc','full load','delta load','last modified','updated at','upsert','merge','batch vs streaming'],
    related: ['etl-elt','airflow-basics','data-warehouse'],
    answer: `**Incremental loading** means only processing new or changed data since the last run — instead of re-loading everything.

**Full load vs Incremental:**
| | Full Load | Incremental |
|-|-----------|-------------|
| **How** | Truncate + reload all rows | Only new/changed rows |
| **Cost** | High (scales with table size) | Low (scales with change volume) |
| **Complexity** | Simple | Moderate (need watermark) |
| **When** | Small tables, no change tracking | Large tables, has updated_at |

**Watermark pattern (most common):**
\`\`\`python
import pandas as pd
from datetime import datetime

def get_last_watermark(conn):
    result = conn.execute("SELECT MAX(updated_at) FROM orders").fetchone()
    return result[0] or datetime(2000, 1, 1)

def load_incremental(conn):
    watermark = get_last_watermark(conn)
    query = f"SELECT * FROM source_orders WHERE updated_at > '{watermark}'"
    df = pd.read_sql(query, source_conn)
    if len(df) > 0:
        df.to_sql("orders", conn, if_exists="append", index=False)
        print(f"Loaded {len(df)} new rows")
\`\`\`

**Upsert (INSERT or UPDATE) for changed records:**
\`\`\`sql
-- PostgreSQL
INSERT INTO orders (order_id, status, updated_at)
VALUES (%(order_id)s, %(status)s, %(updated_at)s)
ON CONFLICT (order_id) DO UPDATE SET
  status     = EXCLUDED.status,
  updated_at = EXCLUDED.updated_at;
\`\`\`

**Change Data Capture (CDC):**
- Reads database transaction log (WAL in Postgres) to capture every INSERT/UPDATE/DELETE
- Tools: **Debezium**, **AWS DMS**, **Fivetran**
- Sends changes to Kafka → consumed by downstream pipeline

**In Airflow — incremental DAG pattern:**
\`\`\`python
# Use execution_date for time-partitioned loads
def extract(**context):
    run_date = context["ds"]  # "2025-03-15"
    query = f"SELECT * FROM orders WHERE DATE(created_at) = '{run_date}'"
\`\`\``
  },

  {
    id: 'normalization',
    title: 'Database Normalization',
    keywords: ['normalization','normal form','1nf','2nf','3nf','denormalization','database design','redundancy','anomaly','update anomaly','functional dependency'],
    related: ['data-modeling','sql-basics','indexes'],
    answer: `**Normalization** eliminates data redundancy and prevents update anomalies by organizing tables correctly.

**The 3 main normal forms:**

**1NF — First Normal Form:**
- Each column holds atomic (single) values
- No repeating groups

\`\`\`
BAD (not 1NF):
orders: order_id | products = "Phone, Laptop, Cable"

GOOD (1NF):
order_items: order_id | product_name
             1        | Phone
             1        | Laptop
\`\`\`

**2NF — Second Normal Form (needs 1NF first):**
- Every non-key column depends on the *whole* primary key (no partial dependencies)
- Applies when PK is composite

\`\`\`
BAD (2NF violation):
order_items: (order_id, product_id) PK | quantity | product_name
-- product_name depends only on product_id, not the full PK

GOOD (2NF):
order_items: (order_id, product_id) | quantity
products: product_id | product_name
\`\`\`

**3NF — Third Normal Form (needs 2NF first):**
- No transitive dependencies: non-key column must not depend on another non-key column

\`\`\`
BAD (3NF violation):
employees: emp_id | dept_id | dept_name
-- dept_name depends on dept_id, not emp_id

GOOD (3NF):
employees: emp_id | dept_id
departments: dept_id | dept_name
\`\`\`

**Denormalization** — intentionally breaking normal forms for query performance:
- Flatten tables to avoid expensive JOINs
- Used in data warehouses, analytical tables (star schema)
- Trade: faster reads, but harder to maintain consistency

**Rule of thumb:**
- OLTP (apps): normalize to 3NF
- OLAP (analytics): denormalize for speed`
  },

  {
    id: 'window-functions',
    title: 'SQL Window Functions',
    keywords: ['window function','window functions','over','partition by','order by','rank','row_number','dense_rank','lag','lead','running total','cumulative','moving average','ntile','first_value','last_value'],
    related: ['sql-basics','group-by-having','subqueries'],
    answer: `**Window functions** perform calculations across a set of related rows without collapsing them into one row (unlike GROUP BY).

**Syntax:**
\`\`\`sql
function_name() OVER (
  PARTITION BY column   -- divide into groups
  ORDER BY column       -- define row order within group
  ROWS/RANGE frame      -- optional: limit which rows count
)
\`\`\`

**Ranking functions:**
\`\`\`sql
SELECT
  employee_id,
  department,
  salary,
  ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num,
  RANK()       OVER (PARTITION BY department ORDER BY salary DESC) AS rank,
  DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank
FROM employees;

-- salary: 100, 90, 90, 80
-- ROW_NUMBER:  1, 2, 3, 4  (always unique)
-- RANK:        1, 2, 2, 4  (gaps after ties)
-- DENSE_RANK:  1, 2, 2, 3  (no gaps)
\`\`\`

**LAG / LEAD — access previous/next row:**
\`\`\`sql
SELECT
  order_date,
  revenue,
  LAG(revenue, 1)  OVER (ORDER BY order_date) AS prev_revenue,
  LEAD(revenue, 1) OVER (ORDER BY order_date) AS next_revenue,
  revenue - LAG(revenue, 1) OVER (ORDER BY order_date) AS day_over_day_change
FROM daily_revenue;
\`\`\`

**Running total / cumulative sum:**
\`\`\`sql
SELECT
  order_date,
  revenue,
  SUM(revenue) OVER (ORDER BY order_date) AS cumulative_revenue,
  AVG(revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_avg
FROM daily_revenue;
\`\`\`

**Top N per group (classic interview question):**
\`\`\`sql
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn
  FROM employees
) t
WHERE rn <= 3;  -- top 3 earners per department
\`\`\``
  },

  {
    id: 'ctes-advanced',
    title: 'CTEs and Recursive Queries',
    keywords: ['cte','common table expression','with clause','recursive cte','recursive query','hierarchical data','tree','graph','with recursive','organizational chart','parent child'],
    related: ['sql-basics','subqueries','window-functions'],
    answer: `**CTE (Common Table Expression)** = named temporary result set defined with \`WITH\`, used to simplify complex queries.

**Basic CTE:**
\`\`\`sql
WITH monthly_revenue AS (
  SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(amount) AS revenue
  FROM orders
  GROUP BY 1
),
ranked AS (
  SELECT *,
    RANK() OVER (ORDER BY revenue DESC) AS rank
  FROM monthly_revenue
)
SELECT * FROM ranked WHERE rank <= 3;
\`\`\`

**Why use CTEs over subqueries:**
- Readable — name each step
- Reusable — reference same CTE multiple times
- Debuggable — test each CTE in isolation

**Recursive CTE — for hierarchical/tree data:**
\`\`\`sql
-- Employee → Manager hierarchy
WITH RECURSIVE org_chart AS (
  -- Base case: start with top-level employee (no manager)
  SELECT emp_id, name, manager_id, 1 AS level
  FROM employees
  WHERE manager_id IS NULL

  UNION ALL

  -- Recursive case: join each employee to their manager
  SELECT e.emp_id, e.name, e.manager_id, oc.level + 1
  FROM employees e
  JOIN org_chart oc ON e.manager_id = oc.emp_id
)
SELECT * FROM org_chart ORDER BY level, name;
\`\`\`

**Generating a date series (useful for gap-filling):**
\`\`\`sql
WITH RECURSIVE date_series AS (
  SELECT '2025-01-01'::date AS d
  UNION ALL
  SELECT d + 1 FROM date_series WHERE d < '2025-01-31'
)
SELECT ds.d, COALESCE(r.revenue, 0)
FROM date_series ds
LEFT JOIN daily_revenue r ON r.order_date = ds.d;
-- Fills missing days with 0 instead of gaps
\`\`\``
  },

  {
    id: 'python-generators',
    title: 'Python Generators & Iterators',
    keywords: ['generator','generators','yield','iterator','lazy evaluation','memory efficient','large files','chunking','streaming data','__iter__','__next__','list vs generator'],
    related: ['python-setup','python-pandas','parquet'],
    answer: `**Generators** produce values one at a time with \`yield\`, never loading everything into memory — critical for large data files.

**Generator vs list:**
\`\`\`python
# List: loads ALL 1M rows into memory at once
rows = [process(r) for r in huge_file]  # 2GB RAM

# Generator: processes one row at a time
def process_rows(file):
    for row in file:
        yield process(row)

rows = process_rows(huge_file)  # near-zero RAM
for row in rows:                # pull one at a time
    save_to_db(row)
\`\`\`

**Generator function:**
\`\`\`python
def read_csv_chunks(filepath, chunk_size=10000):
    """Read large CSV in chunks, yield DataFrames."""
    for chunk in pd.read_csv(filepath, chunksize=chunk_size):
        yield chunk

for chunk in read_csv_chunks("orders_100M.csv"):
    chunk["revenue"] = chunk["qty"] * chunk["price"]
    chunk.to_sql("orders", engine, if_exists="append", index=False)
\`\`\`

**Generator expression (like list comprehension but lazy):**
\`\`\`python
# List comprehension — all in memory
squares = [x**2 for x in range(1_000_000)]

# Generator expression — lazy
squares = (x**2 for x in range(1_000_000))
total = sum(squares)  # computed one by one
\`\`\`

**When to use generators in DE:**
- Reading large files line by line
- Streaming API responses (paginated)
- Building custom iterators for DB cursors
- Chunked inserts to avoid OOM

\`\`\`python
# DB cursor as generator
def fetch_in_batches(cursor, query, batch_size=5000):
    cursor.execute(query)
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows
\`\`\``
  },

  {
    id: 'python-decorators',
    title: 'Python Decorators',
    keywords: ['decorator','decorators','@','wrapper','functools','timing','retry','logging decorator','wraps','higher order function','closure'],
    related: ['python-setup','python-error-handling','airflow-basics'],
    answer: `**Decorators** wrap a function to add behavior before/after it runs — used heavily in Airflow, FastAPI, and DE tooling.

**What a decorator is:**
\`\`\`python
def my_decorator(func):
    def wrapper(*args, **kwargs):
        print("Before function")
        result = func(*args, **kwargs)       # call original
        print("After function")
        return result
    return wrapper

@my_decorator          # same as: load_data = my_decorator(load_data)
def load_data():
    print("Loading...")

load_data()
# Before function
# Loading...
# After function
\`\`\`

**Practical DE decorators:**

**1. Timing decorator:**
\`\`\`python
import time, functools

def timer(func):
    @functools.wraps(func)  # preserve function name/docstring
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        print(f"{func.__name__} took {time.time()-start:.2f}s")
        return result
    return wrapper

@timer
def load_million_rows(): ...
\`\`\`

**2. Retry decorator:**
\`\`\`python
def retry(max_attempts=3, delay=2):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    time.sleep(delay)
                    print(f"Retry {attempt+1}/{max_attempts}: {e}")
        return wrapper
    return decorator

@retry(max_attempts=3, delay=5)
def call_flaky_api(): ...
\`\`\`

**In Airflow:** \`@dag\` and \`@task\` are decorators that register functions as DAGs/operators without subclassing.`
  },

  {
    id: 'python-context-managers',
    title: 'Python Context Managers',
    keywords: ['context manager','with statement','with open','__enter__','__exit__','contextlib','resource management','connection management','file handling','cleanup'],
    related: ['python-setup','python-postgres','python-error-handling'],
    answer: `**Context managers** handle setup and teardown automatically — guaranteed cleanup even if exceptions occur.

**The \`with\` statement:**
\`\`\`python
# Without context manager — risky
f = open("data.csv")
data = f.read()   # if this raises, file stays open
f.close()

# With context manager — always closes
with open("data.csv") as f:
    data = f.read()
# f.close() called automatically, even on exception
\`\`\`

**Database connections — most important DE use case:**
\`\`\`python
from contextlib import contextmanager
from sqlalchemy import create_engine

engine = create_engine("postgresql://user:pass@host/db")

@contextmanager
def get_connection():
    conn = engine.connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()  # ALWAYS runs

with get_connection() as conn:
    conn.execute("INSERT INTO logs VALUES (...)")
# committed + closed, or rolled back on error
\`\`\`

**Writing your own context manager (class-based):**
\`\`\`python
class Timer:
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed = time.time() - self.start
        print(f"Elapsed: {self.elapsed:.2f}s")
        return False  # don't suppress exceptions

with Timer() as t:
    run_heavy_query()
print(f"Query took {t.elapsed:.2f}s")
\`\`\``
  },

  {
    id: 'kafka-basics',
    title: 'Apache Kafka Basics',
    keywords: ['kafka','apache kafka','message queue','event streaming','producer','consumer','topic','partition','offset','broker','streaming','real-time','pubsub','pub sub','event driven'],
    related: ['spark-basics','airflow-basics','incremental-loading'],
    answer: `**Apache Kafka** is a distributed event streaming platform — a high-throughput, fault-tolerant message queue used for real-time data pipelines.

**Core concepts:**
| Term | What it is |
|------|-----------|
| **Topic** | Named category/feed of messages (like a table) |
| **Producer** | App that writes messages to a topic |
| **Consumer** | App that reads messages from a topic |
| **Broker** | Kafka server storing messages |
| **Partition** | Topic split into parts for parallelism |
| **Offset** | Position of a message within a partition |
| **Consumer Group** | Multiple consumers sharing topic load |

**Architecture:**
\`\`\`
App servers ──► Producer ──► [Topic: orders]  ──► Consumer Group A → DWH load
                              Partition 0           Consumer Group B → ML model
                              Partition 1           Consumer Group C → alerts
                              Partition 2
\`\`\`

**Python producer/consumer (kafka-python):**
\`\`\`python
from kafka import KafkaProducer, KafkaConsumer
import json

# Producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode()
)
producer.send("orders", {"order_id": 123, "amount": 99.99})

# Consumer
consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=["localhost:9092"],
    group_id="dwh-loader",
    value_deserializer=lambda v: json.loads(v.decode()),
    auto_offset_reset="earliest"
)
for msg in consumer:
    process_order(msg.value)
\`\`\`

**Kafka vs traditional queue:**
- Kafka **retains** messages (configurable retention — days/weeks)
- Multiple consumer groups each get ALL messages independently
- Messages can be replayed by resetting offsets

**When to use Kafka:** high-volume real-time events (clicks, transactions, IoT), decoupling services, CDC pipelines.`
  },

  {
    id: 'git-for-de',
    title: 'Git for Data Engineers',
    keywords: ['git','version control','github','branching','pull request','pr','merge','commit','gitignore','git workflow','code review','ci cd','github actions'],
    related: ['dbt-basics','docker-basics','debugging-pipelines'],
    answer: `**Git** is version control for code — essential for DE work with dbt, Airflow DAGs, Python pipelines.

**Core daily workflow:**
\`\`\`bash
git status                          # see what changed
git add src/transform/orders.py     # stage specific file
git commit -m "fix: null handling in orders transform"
git push origin feature/orders-fix  # push to remote branch
\`\`\`

**Branching strategy for DE projects:**
\`\`\`
main ──────────────────────────────────► production
  └── develop ────────────────────────► staging
        └── feature/add-orders-model ──► PR → develop → main
\`\`\`

**Useful commands for DE:**
\`\`\`bash
# See history of a specific file
git log --oneline -- dags/my_dag.py

# Who changed this line? (blame)
git blame dags/my_dag.py

# Undo last commit (keep changes)
git reset --soft HEAD~1

# Stash work-in-progress before switching branches
git stash
git checkout main
git stash pop

# See difference between branches
git diff main..feature/my-feature
\`\`\`

**gitignore for DE projects:**
\`\`\`gitignore
# Secrets and credentials
.env
*.key
credentials.json
secrets/

# Python
__pycache__/
*.pyc
venv/
.venv/

# dbt
target/
dbt_packages/
logs/

# Jupyter
.ipynb_checkpoints/
\`\`\`

**Commit message convention:**
\`\`\`
feat: add incremental load for orders table
fix: handle null customer_id in transform
refactor: simplify revenue calculation CTE
docs: add README for pipeline setup
\`\`\`

**In dbt:** every model change goes through a PR. CI runs \`dbt test\` on the PR before merge.`
  },

  {
    id: 'career-de',
    title: 'Data Engineering Career Guide',
    keywords: ['career','job','interview','data engineer job','junior de','portfolio','resume','skills needed','what to learn','roadmap','hiring','salary','data engineer skills','job search'],
    related: ['data-pipeline','airflow-basics','dbt-basics'],
    answer: `**Breaking into Data Engineering** — what actually matters for junior roles.

**Core skills hiring managers look for:**
| Skill | Why | Minimum bar |
|-------|-----|-------------|
| SQL | Every DE role uses it daily | Window functions, CTEs, query optimization |
| Python | Scripts, pipelines, testing | Pandas, psycopg2, error handling |
| A pipeline tool | Airflow most common | Know DAGs, tasks, scheduling |
| Cloud basics | AWS/GCP/Azure | S3/GCS, basic IAM, one managed service |
| Git | Collaboration standard | Branch, PR, commit history |

**What to build for a portfolio:**
1. **End-to-end pipeline** — ingest from public API → transform → load to Postgres/BigQuery
2. **dbt project** — models with tests, staging/mart layers
3. **Airflow DAG** — scheduled, idempotent, with error handling
4. **Data quality framework** — add validation to any of the above

**Good public datasets for projects:**
- NYC TLC taxi data (large, real-world)
- OpenWeatherMap API (time series, API ingestion)
- GitHub Archive (events, JSON, large scale)
- World Bank / Our World in Data (analytical)

**Interview preparation — common questions:**
1. "Design a pipeline to ingest 10M orders/day" → talk about partitioning, incremental load, idempotency
2. "How do you handle late-arriving data?" → watermarks, grace periods, reprocessing
3. "SQL: find customers who ordered in Jan but not Feb" → NOT IN / LEFT JOIN / EXCEPT
4. "What's the difference between ETL and ELT?" → see ETL vs ELT entry
5. "How would you debug a failing pipeline?" → see Debugging entry

**Resume tips:**
- Quantify: "reduced pipeline runtime by 40%" beats "improved pipeline"
- List tools: Airflow, dbt, Spark, BigQuery — ATS scans for these
- Show impact: "enabled daily reporting for 3 business teams"
- GitHub link with real code — most candidates don't have this`
  },

  // ── STEP 4 ENTRIES ──────────────────────────────────────────

  {
    id: 'schema-evolution',
    title: 'Schema Evolution & Schema Drift',
    keywords: ['schema evolution','schema drift','schema change','column added','column removed','breaking change','backward compatible','forward compatible','avro schema','schema registry','source change'],
    related: ['data-quality','etl-elt','kafka-basics'],
    answer: `**Schema drift** happens when the source data structure changes unexpectedly — a column is added, renamed, removed, or its type changes. It's one of the most common pipeline failures in production.

**Types of schema changes:**
| Change | Backward compatible? | Risk |
|--------|---------------------|------|
| Add nullable column | Yes | Low — old consumers ignore new field |
| Add NOT NULL column | No | High — old data has no value for it |
| Rename column | No | High — breaks all downstream queries |
| Change column type | Usually no | High — INT→STRING breaks aggregations |
| Remove column | No | High — breaks anything referencing it |

**How to detect schema drift:**
\`\`\`python
import pandas as pd

EXPECTED_SCHEMA = {'order_id': 'int64', 'amount': 'float64', 'status': 'object'}

def validate_schema(df, expected):
    actual = dict(df.dtypes.astype(str))
    missing = set(expected) - set(actual)
    extra   = set(actual) - set(expected)
    wrong   = {c: (expected[c], actual[c]) for c in expected if c in actual and expected[c] != actual[c]}
    if missing or extra or wrong:
        raise ValueError(f"Schema drift! Missing: {missing}, Extra: {extra}, Wrong types: {wrong}")

df = pd.read_csv("orders.csv")
validate_schema(df, EXPECTED_SCHEMA)
\`\`\`

**Strategies for handling schema evolution:**

1. **Additive-only policy** — only allow adding nullable columns, never rename or remove
2. **Versioned schemas** — keep old table + create new table with suffix \`_v2\`
3. **Schema registry** (Kafka/Avro) — centralized contract for every topic's schema
4. **dbt source freshness + schema tests** — auto-alert when source columns change
5. **SELECT explicit columns** — never \`SELECT *\` in production — add new columns deliberately

\`\`\`sql
-- BAD: breaks if source adds unexpected columns
SELECT * FROM raw.orders

-- GOOD: explicit, controlled
SELECT order_id, customer_id, amount, status, created_at
FROM raw.orders
\`\`\`

**In dbt:** add \`sources.yml\` with column definitions → \`dbt source freshness\` catches late/missing data.`
  },

  {
    id: 'data-contracts',
    title: 'Data Contracts',
    keywords: ['data contract','data contracts','sla','data sla','data agreement','producer consumer','data ownership','data quality sla','data mesh','data governance'],
    related: ['data-quality','schema-evolution','data-modeling'],
    answer: `**Data contracts** are formal agreements between data producers (engineers who build pipelines) and data consumers (analysts, data scientists) that define what data will look like and when it will arrive.

**What a data contract specifies:**
\`\`\`yaml
# Example data contract (YAML format)
contract:
  name: orders_daily
  owner: data-engineering-team
  consumers: [analytics, ml-team, finance]

  schema:
    - name: order_id
      type: integer
      nullable: false
      unique: true
    - name: amount
      type: float
      nullable: false
      constraints: "> 0"

  sla:
    freshness: "data available by 06:00 UTC daily"
    completeness: "99.9% of source orders present"
    latency_max: "2 hours from source to warehouse"

  change_policy:
    breaking_changes: "30 days notice required"
    additive_changes: "7 days notice"
    notification_channel: "#data-platform-changes"
\`\`\`

**Why they matter:**
- Analysts build dashboards on your tables — silent schema changes break their work
- Without contracts, DE team gets blamed for every downstream breakage
- Contracts shift ownership: producer owns schema stability, consumer owns usage

**Tools for data contracts:**
- **Great Expectations** — codify expectations as executable contracts
- **dbt** — schema tests + docs = partial contract enforcement
- **Soda Core** — dedicated data quality framework
- **OpenDataContract** — open spec for contract-as-code

**Real-world pattern:**
\`\`\`python
# On pipeline success — publish contract metadata
contract = {
    "table": "fct_orders",
    "row_count": len(df),
    "max_updated_at": df["updated_at"].max().isoformat(),
    "schema_hash": hash(str(df.dtypes.to_dict())),
    "run_timestamp": datetime.utcnow().isoformat()
}
# Log to metadata table → dashboards show freshness + completeness
\`\`\``
  },

  {
    id: 'observability',
    title: 'Data Observability & Lineage',
    keywords: ['observability','data observability','data lineage','lineage','data monitoring','data freshness','data anomaly','monte carlo','data catalog','column lineage','upstream downstream','data discovery'],
    related: ['data-quality','data-contracts','debugging-pipelines'],
    answer: `**Data observability** = knowing the health of your data at all times. It's like application monitoring (Datadog, Grafana) but for data.

**The 5 pillars of data observability:**
| Pillar | What it means | How to measure |
|--------|---------------|----------------|
| **Freshness** | Is data up to date? | \`MAX(updated_at) < NOW() - INTERVAL '2 hours'\` |
| **Volume** | Expected row count? | Row count drops/spikes alert |
| **Schema** | Structure unchanged? | Column count, types match |
| **Distribution** | Values in normal range? | Null rate, min/max, unique count |
| **Lineage** | Where did this data come from? | Table → table dependency graph |

**Basic observability with SQL:**
\`\`\`sql
-- Freshness check
SELECT
  table_name,
  MAX(updated_at) AS last_update,
  EXTRACT(EPOCH FROM (NOW() - MAX(updated_at)))/3600 AS hours_stale
FROM fct_orders
GROUP BY 1
HAVING hours_stale > 2;

-- Volume anomaly: flag if row count drops >20% vs yesterday
WITH today AS (SELECT COUNT(*) AS cnt FROM fct_orders WHERE DATE(created_at) = CURRENT_DATE),
     yesterday AS (SELECT COUNT(*) AS cnt FROM fct_orders WHERE DATE(created_at) = CURRENT_DATE - 1)
SELECT
  today.cnt, yesterday.cnt,
  ROUND(100.0 * (today.cnt - yesterday.cnt) / NULLIF(yesterday.cnt, 0), 1) AS pct_change
FROM today, yesterday;
\`\`\`

**Data Lineage** = tracking how data flows from source to destination:
\`\`\`
PostgreSQL (orders table)
  → Airflow extracts → S3 raw/orders/
    → Spark transforms → S3 clean/orders/
      → dbt models → BigQuery fct_orders
        → Looker dashboard "Revenue Report"
\`\`\`

If revenue report shows wrong numbers, lineage tells you exactly which step broke.

**Tools:**
- **dbt docs** — auto-generates lineage graph for all models
- **OpenLineage / Marquez** — open standard for pipeline lineage
- **Monte Carlo** — commercial observability platform
- **Atlan, DataHub** — data catalog + lineage

**Minimum viable observability (build yourself):**
1. Log row counts + runtime to a \`pipeline_runs\` table after every job
2. Query that table in a daily health dashboard
3. Alert if freshness > threshold or row count drops > 20%`
  },

  {
    id: 'cloud-storage',
    title: 'Cloud Storage for DE (S3 & GCS)',
    keywords: ['s3','gcs','cloud storage','aws s3','google cloud storage','object storage','bucket','prefix','boto3','gsutil','storage class','data lake storage','partitioned path'],
    related: ['parquet','etl-elt','data-lake'],
    answer: `**S3 (AWS) and GCS (Google Cloud)** are the object storage backbone of modern data engineering. Most data lakes live here.

**Core concepts:**
- **Bucket** — top-level container (like a drive)
- **Object / Blob** — a file stored at a key (path)
- **Prefix** — path-like string used as a folder (not a real folder)
- **Partitioned path** — embed date/category in path for query efficiency

**Best practice path structure:**
\`\`\`
s3://my-data-lake/
  raw/
    orders/year=2025/month=03/day=15/orders_20250315.parquet
    customers/year=2025/month=03/day=15/
  clean/
    orders/year=2025/month=03/
  gold/
    fct_orders/year=2025/month=03/
\`\`\`
This Hive-style partitioning lets Spark/Athena/BigQuery skip entire folders.

**Python with S3 (boto3):**
\`\`\`python
import boto3, pandas as pd
from io import BytesIO

s3 = boto3.client("s3")

# Upload DataFrame as Parquet
def upload_df(df, bucket, key):
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

# Read Parquet from S3
def read_df(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))

# List objects under prefix
def list_files(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]
\`\`\`

**Python with GCS (google-cloud-storage):**
\`\`\`python
from google.cloud import storage

client = storage.Client()
bucket = client.bucket("my-data-lake")

# Upload
blob = bucket.blob("raw/orders/2025-03-15.parquet")
blob.upload_from_filename("local_orders.parquet")

# Download
blob.download_to_filename("local_orders.parquet")
\`\`\`

**Storage classes (cost optimization):**
| Class | Access | Cost | Use case |
|-------|--------|------|----------|
| Standard | Instant | High | Hot data, current month |
| Infrequent Access | Instant | Medium | Data older than 30 days |
| Glacier / Archive | Hours | Very low | Cold data, compliance retention |

**Lifecycle policies** — automatically move old files to cheaper storage after N days.`
  },

  {
    id: 'delta-iceberg',
    title: 'Delta Lake & Apache Iceberg',
    keywords: ['delta lake','apache iceberg','table format','open table format','lakehouse','acid transactions','time travel','delta','iceberg','hudi','data lakehouse','versioning','merge into'],
    related: ['parquet','data-warehouse','spark-basics'],
    answer: `**Delta Lake** and **Apache Iceberg** are open table formats that add ACID transactions, versioning, and schema evolution to Parquet files in S3/GCS — turning a data lake into a **Lakehouse**.

**The problem they solve:**
Plain Parquet files in S3 have no transactions. Two writers = corrupted files. No rollback if something goes wrong. No efficient updates/deletes.

**What they add on top of Parquet:**
| Feature | Plain Parquet | Delta/Iceberg |
|---------|--------------|---------------|
| ACID transactions | No | Yes |
| UPDATE/DELETE rows | No | Yes |
| Time travel (query old data) | No | Yes |
| Schema evolution | Manual | Built-in |
| Concurrent writers | Unsafe | Safe |

**Delta Lake with PySpark:**
\`\`\`python
from delta import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .getOrCreate()

# Write as Delta
df.write.format("delta").mode("overwrite").save("s3://lake/orders/")

# MERGE (upsert) — key Delta feature
delta_table = DeltaTable.forPath(spark, "s3://lake/orders/")
delta_table.alias("target").merge(
    updates_df.alias("updates"),
    "target.order_id = updates.order_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Time travel — query data as of yesterday
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2025-03-14") \
    .load("s3://lake/orders/")
\`\`\`

**Delta vs Iceberg vs Hudi:**
| | Delta Lake | Apache Iceberg | Apache Hudi |
|-|-----------|----------------|-------------|
| **Origin** | Databricks | Netflix | Uber |
| **Best with** | Databricks, Spark | Any engine (multi-engine) | Streaming + batch |
| **Time travel** | Yes | Yes | Yes |
| **Multi-engine** | Improving | Excellent | Good |

**When to use:**
- Databricks shop → Delta Lake
- Multi-engine (Spark + Trino + Flink) → Iceberg
- Need row-level streaming upserts → Hudi`
  },

  {
    id: 'dbt-advanced',
    title: 'dbt Advanced — Macros, Snapshots & Hooks',
    keywords: ['dbt macro','dbt snapshot','dbt hook','dbt test','dbt source','dbt seed','dbt incremental','dbt vars','jinja','dbt advanced','dbt package','dbt utils','dbt generic test'],
    related: ['dbt-basics','ctes-advanced','data-quality'],
    answer: `**dbt advanced patterns** that separate junior from senior DE work.

**Macros — reusable SQL logic (Jinja templating):**
\`\`\`sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name) %}
  ({{ column_name }} / 100.0)::numeric(10,2)
{% endmacro %}

-- models/fct_orders.sql
SELECT
  order_id,
  {{ cents_to_dollars('amount_cents') }} AS amount_dollars
FROM {{ ref('stg_orders') }}
\`\`\`

**Snapshots — SCD Type 2 automatically:**
\`\`\`sql
-- snapshots/customer_snapshot.sql
{% snapshot customer_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at',
  )
}}
SELECT * FROM {{ source('app_db', 'customers') }}
{% endsnapshot %}
\`\`\`
dbt tracks old values automatically, adds \`dbt_valid_from\`, \`dbt_valid_to\`, \`dbt_is_current\`.

**Incremental models — only process new rows:**
\`\`\`sql
{{ config(materialized='incremental', unique_key='order_id') }}

SELECT order_id, customer_id, amount, created_at
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
  WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
\`\`\`

**Hooks — run SQL before/after model:**
\`\`\`yaml
# dbt_project.yml
models:
  my_project:
    +post-hook:
      - "GRANT SELECT ON {{ this }} TO ROLE reporter"
      - "ANALYZE {{ this }}"
\`\`\`

**Custom generic tests:**
\`\`\`sql
-- tests/generic/assert_positive.sql
{% test assert_positive(model, column_name) %}
SELECT {{ column_name }} FROM {{ model }}
WHERE {{ column_name }} <= 0
{% endtest %}
\`\`\`
\`\`\`yaml
# usage in schema.yml
columns:
  - name: amount
    tests:
      - assert_positive
\`\`\`

**dbt_utils package** — install with \`packages.yml\`, gives you:
- \`dbt_utils.surrogate_key()\` — hash-based PK from multiple columns
- \`dbt_utils.pivot()\` — SQL pivot macro
- \`dbt_utils.date_spine()\` — generate a date series`
  },

  {
    id: 'cost-optimization',
    title: 'Data Warehouse Cost Optimization',
    keywords: ['cost optimization','cost','warehouse cost','bigquery cost','snowflake cost','query cost','partition pruning','clustering','materialized view','caching','cost control','expensive query','reduce cost'],
    related: ['data-warehouse','indexes','views-materialized'],
    answer: `Cloud DWH costs scale with compute and data scanned — unoptimized queries can cost thousands per month.

**BigQuery cost optimization:**
\`\`\`sql
-- BAD: scans entire table every time
SELECT * FROM dataset.orders WHERE DATE(created_at) = '2025-03-15'

-- GOOD: use partitioned table (only scans one day's data)
CREATE TABLE dataset.orders
PARTITION BY DATE(created_at);
-- Same query now scans 1/365 of the data → ~99% cheaper

-- GOOD: clustering reduces scans within a partition
CREATE TABLE dataset.orders
PARTITION BY DATE(created_at)
CLUSTER BY customer_id, status;
\`\`\`

**Snowflake cost optimization:**
\`\`\`sql
-- Auto-suspend warehouse when idle (don't pay for idle compute)
ALTER WAREHOUSE my_wh SET AUTO_SUSPEND = 60;  -- suspend after 60s

-- Result cache: Snowflake caches query results 24h — exact same query = free
-- Don't add NOW() or CURRENT_TIMESTAMP() to queries unnecessarily

-- Use SEARCH OPTIMIZATION for point lookups on large tables
ALTER TABLE orders ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id);
\`\`\`

**Universal optimizations:**

1. **SELECT only needed columns** — columnar DBs scan only requested columns
\`\`\`sql
-- BAD (scans all columns)
SELECT * FROM fct_orders

-- GOOD (scans 2 columns only)
SELECT order_id, amount FROM fct_orders
\`\`\`

2. **Filter early, join late** — push WHERE clauses into CTEs/subqueries
3. **Materialize expensive CTEs** — if referenced 3+ times, make it a table
4. **Partition your tables** — always partition large tables by date
5. **Drop unused tables** — data graveyards cost storage money
6. **Scheduled queries** — run heavy reports once, cache result, don't re-run per user

**EXPLAIN / query profiling:**
\`\`\`sql
-- See query execution plan and cost
EXPLAIN SELECT * FROM orders WHERE customer_id = 123;
-- Look for: Seq Scan (bad on large tables) vs Index Scan (good)
\`\`\``
  },

  {
    id: 'testing-pipelines',
    title: 'Testing Data Pipelines',
    keywords: ['test pipeline','unit test','integration test','pytest','testing','test data','mock','fixture','pipeline test','data test','test driven','tdd','test airflow','test transformation'],
    related: ['data-quality','debugging-pipelines','dbt-basics'],
    answer: `**Testing data pipelines** ensures transforms are correct and catches regressions before they reach production.

**Three levels of testing:**

**1. Unit tests — test individual transform functions:**
\`\`\`python
import pytest
import pandas as pd
from transforms import clean_orders  # your transform function

def test_clean_orders_removes_nulls():
    raw = pd.DataFrame({
        "order_id": [1, 2, None],
        "amount":   [10.0, 20.0, 30.0]
    })
    result = clean_orders(raw)
    assert result["order_id"].isna().sum() == 0

def test_clean_orders_calculates_revenue():
    raw = pd.DataFrame({"quantity": [2, 3], "unit_price": [10.0, 5.0]})
    result = clean_orders(raw)
    assert list(result["revenue"]) == [20.0, 15.0]
\`\`\`

**2. Integration tests — test full pipeline with real (small) data:**
\`\`\`python
# Use a test database, not production
@pytest.fixture
def test_db(tmp_path):
    from sqlalchemy import create_engine
    engine = create_engine(f"sqlite:///{tmp_path}/test.db")
    yield engine
    engine.dispose()

def test_load_orders_end_to_end(test_db):
    sample_df = pd.DataFrame({
        "order_id": [1, 2, 3],
        "amount": [10.0, 20.0, 30.0],
        "status": ["completed", "pending", "completed"]
    })
    load_orders(sample_df, test_db)  # your load function
    result = pd.read_sql("SELECT COUNT(*) as cnt FROM orders", test_db)
    assert result["cnt"][0] == 3
\`\`\`

**3. Data quality tests — assert properties of output data:**
\`\`\`python
def assert_no_duplicates(df, key_col):
    dups = df[key_col].duplicated().sum()
    assert dups == 0, f"Found {dups} duplicate {key_col} values"

def assert_referential_integrity(orders_df, customers_df):
    orphan_orders = ~orders_df["customer_id"].isin(customers_df["customer_id"])
    assert orphan_orders.sum() == 0, f"{orphan_orders.sum()} orders with missing customer"
\`\`\`

**Testing Airflow DAGs:**
\`\`\`python
from airflow.models import DagBag

def test_dag_loads_without_errors():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

def test_dag_has_expected_tasks():
    dagbag = DagBag(dag_folder="dags/")
    dag = dagbag.get_dag("orders_pipeline")
    assert set(dag.task_ids) == {"extract", "validate", "transform", "load"}
\`\`\``
  },

  {
    id: 'cdc-debezium',
    title: 'CDC with Debezium',
    keywords: ['cdc','change data capture','debezium','wal','write ahead log','logical replication','binlog','mysql binlog','postgres wal','real time sync','event streaming','debezium connector'],
    related: ['kafka-basics','incremental-loading','data-quality'],
    answer: `**CDC (Change Data Capture)** reads the database transaction log to capture every INSERT, UPDATE, DELETE in real time — without polling the source.

**How Debezium works:**
\`\`\`
PostgreSQL WAL (Write-Ahead Log)
    ↓  (Debezium connector reads log)
Kafka Topic: dbserver.public.orders
    ↓  (consumers read events)
DWH Loader → BigQuery/Snowflake
ML Pipeline → Feature Store
Cache Invalidation → Redis
\`\`\`

**WAL / binlog explained:**
Every database keeps a transaction log for crash recovery. CDC tools like Debezium read that log rather than querying the table — so they catch every change including DELETEs (which no updated_at watermark can catch).

**Debezium event format (Kafka message):**
\`\`\`json
{
  "op": "u",
  "before": { "order_id": 1, "status": "pending" },
  "after":  { "order_id": 1, "status": "completed" },
  "source": {
    "table": "orders",
    "ts_ms": 1710000000000,
    "lsn": 12345678
  }
}
\`\`\`
- \`op\`: "c" = create, "u" = update, "d" = delete, "r" = snapshot (initial load)

**PostgreSQL setup for Debezium:**
\`\`\`sql
-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;

-- Create replication slot
SELECT pg_create_logical_replication_slot('debezium', 'pgoutput');

-- Grant permissions to Debezium user
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;
GRANT REPLICATION ON DATABASE mydb TO debezium_user;
\`\`\`

**Processing CDC events in Python:**
\`\`\`python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer("dbserver.public.orders", ...)
for msg in consumer:
    event = json.loads(msg.value)
    op = event["op"]
    if op in ("c", "u"):
        upsert_to_dwh(event["after"])
    elif op == "d":
        soft_delete_in_dwh(event["before"]["order_id"])
\`\`\`

**CDC vs Polling:**
| | CDC | Polling (watermark) |
|-|-----|---------------------|
| Catches DELETEs | Yes | No |
| Source load | Near zero | Queries source on schedule |
| Latency | Sub-second | Batch interval (minutes/hours) |
| Complexity | High (needs WAL access) | Low |`
  },

  {
    id: 'python-typing',
    title: 'Python Type Hints & Dataclasses',
    keywords: ['type hints','type hint','typing','dataclass','dataclasses','mypy','pydantic','type annotation','typed python','static typing','optional','union','list type','dict type','model'],
    related: ['python-setup','python-error-handling','python-decorators'],
    answer: `**Type hints** make Python code self-documenting, catch bugs at development time, and are standard in modern DE codebases.

**Basic type hints:**
\`\`\`python
from typing import Optional, List, Dict, Tuple

def load_orders(
    source_table: str,
    batch_size: int = 1000,
    filters: Optional[Dict[str, str]] = None
) -> List[Dict]:
    ...

def get_row_count(table: str) -> int:
    ...

# Python 3.9+ — use built-in types directly (no need for typing module)
def process(data: list[dict]) -> dict[str, int]:
    ...
\`\`\`

**Dataclasses — structured config objects:**
\`\`\`python
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class PipelineConfig:
    source_table: str
    target_table: str
    batch_size: int = 1000
    incremental: bool = True
    partition_col: str = "created_at"
    tags: list[str] = field(default_factory=list)
    watermark: Optional[str] = None

# Usage
config = PipelineConfig(
    source_table="raw.orders",
    target_table="fct_orders",
    batch_size=5000
)
print(config.batch_size)  # 5000 — autocomplete works in IDEs
\`\`\`

**Pydantic — dataclasses with validation (used in FastAPI, Airflow):**
\`\`\`python
from pydantic import BaseModel, validator
from typing import Optional
from datetime import datetime

class OrderEvent(BaseModel):
    order_id: int
    customer_id: int
    amount: float
    status: str
    created_at: datetime

    @validator("amount")
    def amount_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("amount must be positive")
        return v

    @validator("status")
    def status_must_be_valid(cls, v):
        valid = {"pending", "completed", "cancelled"}
        if v not in valid:
            raise ValueError(f"status must be one of {valid}")
        return v

# Parses AND validates
event = OrderEvent(**raw_dict)  # raises ValidationError if invalid
\`\`\`

**Why use them in DE:**
- Airflow operators with typed params are self-documenting
- Pipeline configs as dataclasses = no more mystery dicts
- Pydantic for API ingestion = validate at the boundary, not deep in the pipeline`
  },

  {
    id: 'api-pagination',
    title: 'REST API Pagination & Rate Limiting',
    keywords: ['api pagination','pagination','page','cursor','offset','rate limit','throttle','retry','backoff','rest api','requests','api ingestion','next page','paginate','rate limiting'],
    related: ['python-apis','python-error-handling','incremental-loading'],
    answer: `**API pagination** is how APIs serve large datasets in chunks. Every production API has it — your ingestion code must handle it.

**Three pagination styles:**

**1. Offset/Page pagination:**
\`\`\`python
import requests, time

def fetch_all_orders(base_url, api_key):
    page = 1
    all_records = []
    while True:
        resp = requests.get(
            f"{base_url}/orders",
            params={"page": page, "per_page": 100},
            headers={"Authorization": f"Bearer {api_key}"}
        )
        resp.raise_for_status()
        data = resp.json()
        records = data.get("data", [])
        if not records:
            break
        all_records.extend(records)
        page += 1
        if not data.get("has_more", False):  # or check total_pages
            break
    return all_records
\`\`\`

**2. Cursor pagination (most reliable for large datasets):**
\`\`\`python
def fetch_with_cursor(base_url, api_key):
    cursor = None
    while True:
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = requests.get(f"{base_url}/events", params=params,
                            headers={"Authorization": f"Bearer {api_key}"})
        data = resp.json()
        yield from data["items"]
        cursor = data.get("next_cursor")
        if not cursor:
            break
\`\`\`

**3. Link header pagination (GitHub-style):**
\`\`\`python
url = "https://api.github.com/repos/org/repo/commits"
while url:
    resp = requests.get(url, headers={"Authorization": f"token {TOKEN}"})
    yield from resp.json()
    url = resp.links.get("next", {}).get("url")  # None when last page
\`\`\`

**Rate limiting + exponential backoff:**
\`\`\`python
import time, random

def api_get_with_retry(url, headers, max_retries=5):
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers)
        if resp.status_code == 429:  # Too Many Requests
            retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
            jitter = random.uniform(0, 1)
            time.sleep(retry_after + jitter)
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception(f"API failed after {max_retries} retries")
\`\`\`

**Incremental API ingestion (checkpoint pattern):**
\`\`\`python
# Save last fetched timestamp, only pull new records
last_ts = get_last_watermark()
for page in fetch_with_cursor(since=last_ts):
    process_and_load(page)
save_watermark(datetime.utcnow())
\`\`\``
  },

  {
    id: 'query-performance',
    title: 'SQL Query Performance & EXPLAIN',
    keywords: ['query performance','slow query','explain','explain analyze','query plan','seq scan','index scan','vacuum','analyze','query optimization','execution plan','cost estimate','nested loop','hash join','full table scan'],
    related: ['indexes','normalization','cost-optimization'],
    answer: `**EXPLAIN** shows the database's execution plan — how it will run your query, what indexes it uses, and estimated cost.

**Reading EXPLAIN output:**
\`\`\`sql
EXPLAIN ANALYZE SELECT * FROM orders WHERE customer_id = 123;

-- Output:
-- Index Scan using orders_customer_id_idx on orders  (cost=0.43..8.45 rows=1 width=120)
--                                                     actual time=0.028..0.031 rows=1 loops=1
--   Index Cond: (customer_id = 123)
-- Planning Time: 0.1 ms
-- Execution Time: 0.05 ms
\`\`\`

**What to look for:**
| Node type | Meaning | Good or bad? |
|-----------|---------|--------------|
| **Index Scan** | Uses index, reads matching rows | Good for selective queries |
| **Seq Scan** | Reads entire table | Bad on large tables |
| **Hash Join** | Builds hash table for join | Good for large joins |
| **Nested Loop** | Row-by-row join | Good if inner set is small |
| **Sort** | Explicit sort (no index) | Expensive — add index |
| **Bitmap Heap Scan** | Multiple index scans merged | Decent |

**Common performance fixes:**

**1. Add missing index:**
\`\`\`sql
-- Slow: seq scan on 10M row table
SELECT * FROM orders WHERE customer_id = 123;

-- Fix:
CREATE INDEX CONCURRENTLY orders_customer_id ON orders(customer_id);
-- CONCURRENTLY = no table lock, safe in production
\`\`\`

**2. Fix N+1 query (loop with query inside loop):**
\`\`\`python
# BAD: 1000 queries for 1000 customers
for customer_id in customer_ids:
    orders = db.execute(f"SELECT * FROM orders WHERE customer_id = {customer_id}")

# GOOD: 1 query with IN clause
customer_ids_str = ",".join(str(i) for i in customer_ids)
orders = db.execute(f"SELECT * FROM orders WHERE customer_id IN ({customer_ids_str})")
\`\`\`

**3. VACUUM and ANALYZE (PostgreSQL maintenance):**
\`\`\`sql
-- After large DELETE/UPDATE — reclaim dead rows
VACUUM ANALYZE orders;

-- Auto-vacuum runs automatically, but after bulk loads run manually
-- ANALYZE updates statistics so query planner makes better decisions
\`\`\`

**4. Avoid functions on indexed columns in WHERE:**
\`\`\`sql
-- BAD: function on column = index not used
WHERE DATE(created_at) = '2025-03-15'

-- GOOD: range filter = index used
WHERE created_at >= '2025-03-15' AND created_at < '2025-03-16'
\`\`\``
  }
];

// ============================================================
// SEARCH ENGINE v2 — Intent-aware, synonym-expanded, context-memory
// ============================================================

// --- Synonym groups: any word maps to the canonical form ---
const BOT_SYNONYMS = {
  // verbs
  'explain':'what is','show':'what is','describe':'what is','define':'what is','tell me about':'what is',
  'how to':'howto','how do i':'howto','how can i':'howto','how do you':'howto','how does':'howto',
  'difference between':'vs','compare':'vs','vs':'vs','versus':'vs','better':'vs',
  'give me an example':'example','show me an example':'example','example of':'example','code for':'example',
  'why':'why','why is':'why','why do':'why','why does':'why','when to use':'why','when should':'why',
  'debug':'debug','fix':'debug','error':'debug','problem':'debug','issue':'debug','not working':'debug',
  'best practice':'howto','best way':'howto',
  // DE terms
  'data eng':'data engineering','de ':'data engineering','data pipeline':'pipeline',
  'incremental':'incremental loading','watermark':'incremental loading',
  'idempotent':'idempotency','safe to rerun':'idempotency',
  'star':'star schema','fact':'star schema','dimension':'star schema',
  'scd':'slowly changing dimension','type 1':'scd','type 2':'scd',
  'row number':'window function','rank':'window function','lag':'window function','lead':'window function',
  'partition by':'window function','over clause':'window function',
  'with clause':'cte','named query':'cte','common table':'cte',
  'upsert':'merge','on conflict':'merge','insert or update':'merge',
  'postgre':'postgresql','postgres':'postgresql','psql':'postgresql',
  'pyspark':'spark','apache spark':'spark',
  'dbt':'dbt','data build tool':'dbt',
  'dag':'airflow','operator':'airflow','airflow dag':'airflow',
  'docker compose':'docker','containerize':'docker','container':'docker',
  'parquet':'parquet','columnar':'parquet','file format':'parquet',
  'olap':'oltp olap','oltp':'oltp olap',
  'bronze':'pipeline phases','silver':'pipeline phases','gold':'pipeline phases','medallion':'pipeline phases',
  'staging':'pipeline phases','stage layer':'pipeline phases',
  'full load':'incremental loading','batch load':'incremental loading',
  'schema drift':'schema evolution','column change':'schema evolution','breaking change':'schema evolution',
  'data contract':'data contracts','sla':'data contracts',
  'data lineage':'observability','lineage':'observability','data monitoring':'observability',
  's3':'cloud storage','gcs':'cloud storage','object storage':'cloud storage','boto3':'cloud storage',
  'delta lake':'delta iceberg','iceberg':'delta iceberg','lakehouse':'delta iceberg','hudi':'delta iceberg',
  'dbt macro':'dbt advanced','dbt snapshot':'dbt advanced','dbt incremental':'dbt advanced',
  'slow query':'query performance','explain analyze':'query performance','seq scan':'query performance','vacuum':'query performance',
  'change data capture':'cdc debezium','debezium':'cdc debezium','wal':'cdc debezium','binlog':'cdc debezium',
  'type hint':'python typing','dataclass':'python typing','pydantic':'python typing','mypy':'python typing',
  'pagination':'api pagination','rate limit':'api pagination','cursor pagination':'api pagination',
  'warehouse cost':'cost optimization','partition pruning':'cost optimization','query cost':'cost optimization',
  'test pipeline':'testing pipelines','pytest':'testing pipelines','unit test':'testing pipelines',
};

// --- Stop words to ignore during scoring ---
const BOT_STOP = new Set(['the','a','an','is','are','was','be','to','of','and','or','in',
  'on','at','for','with','this','that','what','how','why','when','can','do','does',
  'did','will','would','could','should','please','me','my','i','you','it','its','im']);

// --- Intent patterns ---
const BOT_INTENTS = [
  { type: 'greeting',    re: /^(hi|hey|hello|good (morning|evening|night|day)|howdy|yo|sup|whats up|what's up)\b/i },
  { type: 'thanks',      re: /^(thanks|thank you|thx|ty|cheers|great|awesome|perfect|nice)\b/i },
  { type: 'help',        re: /^(help|what can you|what do you know|topics|commands|list topics)\b/i },
  { type: 'followup',    re: /^(more|more detail|elaborate|expand|go on|continue|tell me more|and|also|what about|but why|but how)\b/i },
  { type: 'example',     re: /\b(example|code|snippet|show me|demonstrate|sample)\b/i },
  { type: 'vs',          re: /\bvs\.?\b|\bversus\b|\bdifference between\b|\bcompare\b|\bwhich is better\b/i },
  { type: 'howto',       re: /\bhow (to|do|can|does|did|would|should)\b|\bbest (way|practice)\b|\bsteps\b/i },
  { type: 'why',         re: /\bwhy\b|\bwhen (to|should|would)\b|\bpurpose\b|\breason\b|\bbenefit\b/i },
  { type: 'define',      re: /\b(what is|what are|what's|what does|define|explain|describe|tell me about)\b/i },
  { type: 'quiz',        re: /\b(quiz|test me|practice|question|ask me|challenge me|drill)\b/i },
];

// --- Conversation memory ---
let BOT_LAST_TOPIC = null;   // last matched KB entry id
let BOT_LAST_ENTRY = null;   // last matched KB entry object
const BOT_HISTORY  = [];     // last 5 matched entries (most recent first)

function pushHistory(entry) {
  if (!entry) return;
  // Remove if already in history, then push to front
  const idx = BOT_HISTORY.findIndex(e => e.id === entry.id);
  if (idx !== -1) BOT_HISTORY.splice(idx, 1);
  BOT_HISTORY.unshift(entry);
  if (BOT_HISTORY.length > 5) BOT_HISTORY.pop();
}

function detectIntent(text) {
  const lower = text.toLowerCase().trim();
  for (const { type, re } of BOT_INTENTS) {
    if (re.test(lower)) return type;
  }
  return 'general';
}

function expandSynonyms(text) {
  let t = text.toLowerCase();
  Object.entries(BOT_SYNONYMS).forEach(([from, to]) => {
    t = t.replace(new RegExp(from.replace(/[.*+?^${}()|[\]\\]/g,'\\$&'), 'gi'), ' ' + to + ' ');
  });
  return t;
}

// Simple edit-distance for typo tolerance (handles 1-2 char differences)
function editDistance(a, b) {
  if (Math.abs(a.length - b.length) > 3) return 99;
  const m = a.length, n = b.length;
  const dp = Array.from({length: m+1}, (_, i) => Array.from({length: n+1}, (_, j) => i || j));
  for (let i = 1; i <= m; i++)
    for (let j = 1; j <= n; j++)
      dp[i][j] = a[i-1] === b[j-1] ? dp[i-1][j-1] : 1 + Math.min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1]);
  return dp[m][n];
}

// Returns true if token fuzzy-matches a keyword (typo-tolerant)
function fuzzyMatch(token, keyword) {
  if (token.length < 4) return token === keyword;
  if (keyword.includes(token) || token.includes(keyword)) return true;
  const threshold = token.length <= 6 ? 1 : 2;
  return editDistance(token, keyword) <= threshold;
}

function botTokenize(text) {
  const expanded = expandSynonyms(text);
  return expanded
    .replace(/[^\w\s]/g, ' ')
    .split(/\s+/)
    .map(w => w.trim())
    .filter(w => w.length > 1 && !BOT_STOP.has(w));
}

// Extract the core subject from a question
// e.g. "what is idempotency?" → "idempotency"
// e.g. "how do I connect python to postgres?" → "connect python postgres"
function extractSubject(text) {
  return text.toLowerCase()
    .replace(/^(what is|what are|explain|describe|tell me about|how to|how do i|how does|why is|why do|what's|define)\s+/i, '')
    .replace(/[?.!]$/, '')
    .trim();
}

function scoreKB(entry, tokens, subject, intent) {
  let score = 0;
  const titleLower = entry.title.toLowerCase();
  const answerLower = entry.answer.toLowerCase();

  // Exact title match (highest weight)
  if (subject && titleLower.includes(subject)) score += 20;

  // Keyword phrase match (exact + fuzzy)
  entry.keywords.forEach(kw => {
    if (subject && subject.includes(kw)) score += 12;
    tokens.forEach(t => {
      if (kw === t) score += 6;
      else if (kw.includes(t) || t.includes(kw)) score += 3;
      else if (t.length >= 4 && fuzzyMatch(t, kw)) score += 2;  // typo tolerance
    });
  });

  // Title token match (exact + fuzzy)
  const titleTokens = botTokenize(entry.title);
  tokens.forEach(t => {
    if (titleTokens.some(tt => tt === t || tt.includes(t) || t.includes(tt))) score += 4;
    else if (t.length >= 4 && titleTokens.some(tt => fuzzyMatch(t, tt))) score += 2;
  });

  // Answer body match (lower weight, just relevance signal)
  tokens.forEach(t => { if (answerLower.includes(t)) score += 0.5; });

  // Intent bonus: if asking for comparison and entry is about comparison topic
  if (intent === 'vs' && (entry.id.includes('vs') || entry.keywords.some(k => k.includes('vs') || k.includes('difference')))) score += 8;
  if (intent === 'example' && (entry.answer.includes('```') || entry.answer.includes('example'))) score += 4;

  return score;
}

function searchPlatformData(tokens, subject) {
  const results = [];

  function hayScore(hay, toks, subj) {
    let s = 0;
    if (subj && hay.includes(subj)) s += 10;
    toks.forEach(t => { if (hay.includes(t)) s += 2; });
    return s;
  }

  if (typeof KNOWLEDGE !== 'undefined') {
    KNOWLEDGE.forEach(k => {
      const hay = (k.title + ' ' + k.subtitle + ' ' + JSON.stringify(k.points)).toLowerCase();
      const s = hayScore(hay, tokens, subject);
      if (s > 0) results.push({ score: s, type: 'knowledge', data: k });
    });
  }
  if (typeof SQL_TOPICS !== 'undefined') {
    SQL_TOPICS.forEach(t => {
      const hay = (t.title + ' ' + t.summary + ' ' + t.explanation).toLowerCase();
      const s = hayScore(hay, tokens, subject);
      if (s > 0) results.push({ score: s, type: 'sql', data: t });
    });
  }
  if (typeof PYTHON_MODULES !== 'undefined') {
    PYTHON_MODULES.forEach(m => {
      const hay = (m.title + ' ' + m.subtitle + ' ' + m.summary + ' ' + m.points.join(' ')).toLowerCase();
      const s = hayScore(hay, tokens, subject);
      if (s > 0) results.push({ score: s, type: 'python', data: m });
    });
  }
  if (typeof GAPS !== 'undefined') {
    GAPS.forEach(g => {
      const hay = (g.title + ' ' + g.subtitle + ' ' + (g.why||'')).toLowerCase();
      const s = hayScore(hay, tokens, subject);
      if (s > 0) results.push({ score: s, type: 'gap', data: g });
    });
  }
  if (typeof INTERVIEW_QA !== 'undefined') {
    INTERVIEW_QA.forEach(topic => {
      topic.questions.forEach(q => {
        const hay = (q.q + ' ' + q.a).toLowerCase();
        const s = hayScore(hay, tokens, subject);
        if (s > 3) results.push({ score: s, type: 'iq', data: q, topicName: topic.topic });
      });
    });
  }

  results.sort((a, b) => b.score - a.score);
  return results.slice(0, 3);
}

function relatedTopics(entryId) {
  const rel = {
    // DE concepts
    'what-is-de':          ['etl-elt','pipeline-phases','how-to-start'],
    'oltp-olap':           ['data-warehouse','star-schema','etl-elt'],
    'pipeline-phases':     ['etl-elt','incremental-loading','data-quality'],
    'idempotency':         ['acid','merge-upsert','incremental-loading'],
    'acid':                ['idempotency','python-postgres','merge-upsert'],
    'star-schema':         ['scd','data-modeling','data-warehouse'],
    'scd':                 ['star-schema','merge-upsert','data-modeling'],
    'data-lake':           ['data-warehouse','parquet','etl-elt'],
    'how-to-start':        ['career-de','what-is-de','git-for-de'],
    'interview-tips':      ['career-de','sql-joins','window-functions'],
    'etl-elt':             ['pipeline-phases','incremental-loading','dbt-basics'],
    'incremental-loading': ['idempotency','merge-upsert','debugging-pipelines'],
    'data-quality':        ['dbt-basics','airflow-basics','debugging-pipelines'],
    'data-modeling':       ['star-schema','scd','dbt-basics'],
    'data-warehouse':      ['data-modeling','etl-elt','spark-basics'],
    'debugging-pipelines': ['airflow-basics','python-error-handling','idempotency'],
    'normalization':       ['data-modeling','sql-joins','indexes'],
    'kafka-basics':        ['spark-basics','airflow-basics','incremental-loading'],
    'git-for-de':          ['dbt-basics','docker-basics','career-de'],
    'career-de':           ['how-to-start','interview-tips','git-for-de'],
    // SQL
    'sql-joins':           ['subqueries','window-functions','null-handling'],
    'window-functions':    ['ctes-advanced','group-by-having','sql-joins'],
    'ctes':                ['window-functions','subqueries','dbt-basics'],
    'ctes-advanced':       ['window-functions','subqueries','sql-joins'],
    'group-by-having':     ['window-functions','subqueries','indexes'],
    'null-handling':       ['sql-joins','data-quality','merge-upsert'],
    'indexes':             ['views-materialized','stored-procedures','normalization'],
    'subqueries':          ['ctes-advanced','sql-joins','window-functions'],
    'views-materialized':  ['indexes','stored-procedures','dbt-basics'],
    'merge-upsert':        ['idempotency','incremental-loading','stored-procedures'],
    'stored-procedures':   ['views-materialized','merge-upsert','python-postgres'],
    // Python
    'python-setup':        ['python-pandas','python-postgres','git-for-de'],
    'python-pandas':       ['python-postgres','python-apis','parquet'],
    'python-postgres':     ['python-error-handling','merge-upsert','acid'],
    'python-apis':         ['python-error-handling','python-pandas','incremental-loading'],
    'python-error-handling':['python-context-managers','debugging-pipelines','airflow-basics'],
    'python-generators':   ['python-pandas','parquet','spark-basics'],
    'python-decorators':   ['python-error-handling','airflow-basics','python-context-managers'],
    'python-context-managers':['python-postgres','python-error-handling','python-decorators'],
    // Tools
    'parquet':             ['spark-basics','data-warehouse','python-pandas'],
    'airflow-basics':      ['airflow-xcoms','docker-basics','etl-elt'],
    'airflow-xcoms':       ['airflow-basics','python-error-handling','debugging-pipelines'],
    'spark-basics':        ['spark-lazy','spark-partitions','parquet'],
    'spark-lazy':          ['spark-basics','spark-partitions','python-generators'],
    'spark-partitions':    ['spark-basics','spark-lazy','parquet'],
    'dbt-basics':          ['ctes-advanced','etl-elt','data-quality'],
    'docker-basics':       ['airflow-basics','python-setup','git-for-de'],
    // Step 4 entries
    'schema-evolution':    ['data-quality','data-contracts','etl-elt'],
    'data-contracts':      ['data-quality','schema-evolution','observability'],
    'observability':       ['data-quality','data-contracts','debugging-pipelines'],
    'cloud-storage':       ['parquet','delta-iceberg','etl-elt'],
    'delta-iceberg':       ['parquet','spark-basics','data-warehouse'],
    'dbt-advanced':        ['dbt-basics','ctes-advanced','data-quality'],
    'cost-optimization':   ['data-warehouse','indexes','views-materialized'],
    'testing-pipelines':   ['data-quality','debugging-pipelines','dbt-basics'],
    'cdc-debezium':        ['kafka-basics','incremental-loading','schema-evolution'],
    'python-typing':       ['python-setup','python-error-handling','python-decorators'],
    'api-pagination':      ['python-apis','python-error-handling','incremental-loading'],
    'query-performance':   ['indexes','normalization','cost-optimization'],
  };
  const ids = rel[entryId] || [];
  const names = ids.map(id => {
    const e = BOT_KB.find(k => k.id === id);
    return e ? e.title : null;
  }).filter(Boolean);
  return names.length ? `\n\n---\n**Related topics you can ask about:** ${names.join(' · ')}` : '';
}

function formatPlatformResult(result, intent) {
  const { type, data } = result;
  let text = '';
  if (type === 'knowledge') {
    text = `**${data.title}**\n*${data.subtitle}*\n\n`;
    const pts = intent === 'example' ? data.points : data.points.slice(0, 4);
    pts.forEach(p => { text += `**${p.label}:** ${p.text}\n\n`; });
  } else if (type === 'sql') {
    text = `**${data.title}** *(SQL — ${data.tierLabel})*\n\n${data.explanation}\n\n\`\`\`sql\n${data.example}\n\`\`\``;
    if (intent !== 'example') text += `\n\n**Practice:** ${data.practice}`;
  } else if (type === 'python') {
    text = `**${data.title}**\n*${data.subtitle}*\n\n${data.summary}\n\n${data.points.slice(0, 5).map(p=>`- ${p}`).join('\n')}\n\n\`\`\`python\n${data.code.slice(0, 700)}${data.code.length > 700 ? '\n# ...' : ''}\n\`\`\``;
  } else if (type === 'gap') {
    text = `**${data.title}** — this is a gap worth filling\n\n${data.why || ''}\n\n${data.lesson ? data.lesson.intro : ''}`;
  } else if (type === 'iq') {
    text = `**Interview Question:** ${data.q}\n\n**Model Answer:** ${data.a}${data.tip ? `\n\n**Pro tip:** ${data.tip}` : ''}`;
  }
  return text;
}

// Intent-aware intro phrases
function intentIntro(intent, title) {
  const intros = {
    define:  [`**${title}** — here's what you need to know:\n\n`, `Let me break down **${title}** for you:\n\n`, `**${title}** explained from the ground up:\n\n`],
    howto:   [`Here's how to work with **${title}**, step by step:\n\n`, `Let's walk through **${title}**:\n\n`, `Here's the practical approach to **${title}**:\n\n`],
    why:     [`Here's why **${title}** matters in Data Engineering:\n\n`, `Great question — **${title}** is important because:\n\n`],
    example: [`Here's a concrete example of **${title}** in action:\n\n`, `Let me show you **${title}** with a real example:\n\n`],
    vs:      [`**${title}** — let's compare them clearly:\n\n`, `Here's the difference — **${title}**:\n\n`],
  };
  const opts = intros[intent];
  if (!opts) return '';
  return opts[Math.floor(Math.random() * opts.length)];
}

// ── Quiz mode ──
const BOT_QUIZ_QUESTIONS = [
  { q: "What does ACID stand for in databases?", a: "**A**tomicity, **C**onsistency, **I**solation, **D**urability — the four guarantees of a reliable database transaction.", tag: "acid" },
  { q: "What is the difference between a fact table and a dimension table?", a: "**Fact table** stores measurable events (orders, transactions) with foreign keys and numeric measures. **Dimension table** stores descriptive attributes (customer name, product category) used to filter and group facts.", tag: "star-schema" },
  { q: "What does idempotency mean for a data pipeline?", a: "An idempotent pipeline produces the same result no matter how many times it runs. Running it 1x or 10x = identical output. Achieved with TRUNCATE+INSERT or MERGE/UPSERT patterns.", tag: "idempotency" },
  { q: "What is the difference between ETL and ELT?", a: "**ETL**: transform data before loading (old approach, separate compute layer). **ELT**: load raw data first, transform inside the warehouse using SQL/dbt (modern cloud approach).", tag: "etl-elt" },
  { q: "What SQL clause would you use to filter aggregated results?", a: "**HAVING** — it filters after GROUP BY aggregation. WHERE filters rows before aggregation. Example: `HAVING COUNT(*) > 5`", tag: "group-by-having" },
  { q: "What is window function PARTITION BY equivalent to conceptually?", a: "PARTITION BY is like GROUP BY but **doesn't collapse rows**. It resets the window calculation for each group while keeping all rows in the output.", tag: "window-functions" },
  { q: "What is the difference between RANK() and DENSE_RANK()?", a: "**RANK()**: leaves gaps after ties (1,2,2,4). **DENSE_RANK()**: no gaps (1,2,2,3). ROW_NUMBER() always gives unique numbers regardless of ties.", tag: "window-functions" },
  { q: "What is a CTE and when would you use one over a subquery?", a: "**CTE (Common Table Expression)** = named temporary result defined with WITH. Use over subquery when: (1) you need to reference it multiple times, (2) query is complex and readability matters, (3) you want to debug each step.", tag: "ctes-advanced" },
  { q: "What does Spark's lazy evaluation mean?", a: "Spark doesn't execute transformations immediately. It builds an execution plan (DAG) and only runs it when an **action** is called (collect, write, count). This allows Catalyst optimizer to reorder and optimize operations.", tag: "spark-lazy" },
  { q: "What is the Bronze/Silver/Gold (Medallion) architecture?", a: "**Bronze**: raw data as-is from source. **Silver**: cleaned, validated, typed. **Gold**: aggregated, business-ready tables for dashboards/analytics. Each layer adds quality and removes detail.", tag: "pipeline-phases" },
  { q: "How does incremental loading work? What is a watermark?", a: "Incremental loading only processes new/changed records since last run. A **watermark** is the timestamp of the last successful run — used in `WHERE updated_at > last_watermark` to fetch only new rows.", tag: "incremental-loading" },
  { q: "What is schema drift and how do you handle it?", a: "Schema drift = unexpected source schema change (column added/removed/renamed). Handle by: (1) validating schema before loading, (2) using SELECT explicit columns (not SELECT *), (3) alerting on column count/type changes.", tag: "schema-evolution" },
  { q: "What is the difference between a data lake and a data warehouse?", a: "**Data lake**: raw files in object storage (S3/GCS), any format, schema-on-read, cheap. **Data warehouse**: structured, curated, schema-on-write, SQL-queryable, fast analytics. Lakehouse (Delta/Iceberg) combines both.", tag: "data-warehouse" },
  { q: "What SQL keyword prevents duplicate rows when inserting existing data?", a: "**ON CONFLICT DO UPDATE** (PostgreSQL UPSERT) or **MERGE** statement. Example: `INSERT INTO ... ON CONFLICT (id) DO UPDATE SET col = EXCLUDED.col`", tag: "merge-upsert" },
  { q: "What does dbt's ref() function do?", a: "`{{ ref('model_name') }}` references another dbt model and automatically handles dependencies. dbt builds models in the correct order based on ref() calls and replaces it with the actual table name at runtime.", tag: "dbt-basics" },
  { q: "What are the 6 dimensions of data quality?", a: "**Completeness** (no nulls), **Uniqueness** (no duplicates), **Validity** (values in range), **Consistency** (same across systems), **Timeliness** (data arrives on schedule), **Accuracy** (reflects reality).", tag: "data-quality" },
  { q: "What is the difference between a Seq Scan and Index Scan in EXPLAIN?", a: "**Seq Scan**: reads entire table row by row (bad for large tables). **Index Scan**: uses an index to jump directly to matching rows (good for selective queries). See Seq Scan on large tables → add an index.", tag: "query-performance" },
  { q: "What does CDC stand for and how is it different from polling?", a: "**Change Data Capture** — reads the database transaction log (WAL) to capture every change in real time. Unlike polling (watermark queries), CDC: catches DELETEs, has sub-second latency, puts near-zero load on source.", tag: "cdc-debezium" },
];

let BOT_QUIZ_IDX = null; // current active quiz question index
let BOT_QUIZ_PENDING = null; // question awaiting answer check

function handleQuiz(subject, tokens) {
  // Filter questions by topic if subject given
  let pool = BOT_QUIZ_QUESTIONS;
  if (subject && subject.length > 2) {
    const filtered = pool.filter(q =>
      q.q.toLowerCase().includes(subject) ||
      q.tag.includes(subject) ||
      tokens.some(t => q.tag.includes(t) || q.q.toLowerCase().includes(t))
    );
    if (filtered.length > 0) pool = filtered;
  }
  // Pick random question not recently asked
  const q = pool[Math.floor(Math.random() * pool.length)];
  BOT_QUIZ_PENDING = q;
  return `**Quiz time! 🎯**\n\n${q.q}\n\n*Think about it, then type "answer" or "show answer" to see the model answer.*\n*Type "next" for another question, or ask about "${q.tag}" for a full explanation.*`;
}

// Build follow-up prompt suggestions based on the last entry
function followUpSuggestions(entry) {
  if (!entry) return '';
  return `\n\n---\n*Want to go deeper? Try asking:*\n- "Give me an example of ${entry.title}"\n- "Why is ${entry.title} important?"\n- "How does ${entry.title} work in practice?"`;
}

function botRespond(userInput) {
  const raw    = userInput.trim();
  const lower  = raw.toLowerCase();
  const intent = detectIntent(raw);
  const tokens = botTokenize(raw);
  const subject = extractSubject(raw);

  // ── Greeting ──
  if (intent === 'greeting') {
    const greetings = [
      `Hey! Ready to talk Data Engineering.\n\nAsk me anything — I know SQL, Python, Airflow, Spark, dbt, pipeline design, data warehousing, data quality, Kafka, and more.\n\nSome examples to get started:\n- "What is incremental loading?"\n- "How do window functions work?"\n- "Difference between ETL and ELT?"\n- "How do I connect Python to PostgreSQL?"\n- "How do I debug a failing pipeline?"\n\nType **help** to see everything I can explain.`,
      `Hi! I'm your offline DE Expert — I know this whole platform inside out.\n\nTry asking:\n- "Explain data modeling"\n- "How do I use CTEs?"\n- "What's the difference between Spark and pandas?"\n- "How does Kafka work?"\n\nType **help** for the full topic map.`
    ];
    return greetings[Math.floor(Math.random() * greetings.length)];
  }

  // ── Quiz answer reveal ──
  if (/^(answer|show answer|give answer|reveal|i give up|solution)$/i.test(lower) && BOT_QUIZ_PENDING) {
    const q = BOT_QUIZ_PENDING;
    BOT_QUIZ_PENDING = null;
    return `**Answer:**\n\n${q.a}\n\n---\nType **"quiz ${q.tag}"** for more questions on this topic, or ask me to explain **"${q.tag}"** in detail.`;
  }

  // ── Quiz next question ──
  if (/^(next|next question|another|another question|more questions?)$/i.test(lower)) {
    return handleQuiz('', []);
  }

  // ── Thanks ──
  if (intent === 'thanks') {
    const base = BOT_LAST_ENTRY
      ? `Glad that helped! Here are some natural follow-ups on **${BOT_LAST_ENTRY.title}**:\n- "Give me an example of ${BOT_LAST_ENTRY.title}"\n- "Why does ${BOT_LAST_ENTRY.title} matter?"\n- "How is ${BOT_LAST_ENTRY.title} used in a real pipeline?"`
      : `Glad I could help! Ask me anything else — SQL, Python, pipelines, tools, or career advice.`;
    return base;
  }

  // ── Help ──
  if (intent === 'help') {
    return `Here's everything I can explain — ask naturally ("what is X", "how to X", "explain X", "difference between X and Y"):\n\n**Core DE Concepts:**\nETL vs ELT · Data Pipeline Phases · Incremental Loading · Idempotency · ACID · Data Quality · Data Modeling · Star/Snowflake Schema · SCD Types · Medallion Architecture · Normalization · Parquet · Data Warehouse · Kafka\n\n**SQL:**\nJOINs · Window Functions (RANK, LAG, LEAD, running totals) · CTEs · Recursive CTEs · GROUP BY/HAVING · Subqueries · NULL Handling · Indexes · EXPLAIN · Views · Materialized Views · MERGE/UPSERT · Stored Procedures\n\n**Python for DE:**\nEnvironment setup · pandas · psycopg2/SQLAlchemy · REST APIs · Error Handling · Generators · Decorators · Context Managers · File I/O\n\n**Tools & Frameworks:**\nAirflow (DAGs · XComs · Scheduling · Backfill) · PySpark (Lazy eval · Partitions · Skew) · dbt (Models · Tests · ref()) · Docker · Git\n\n**Career:**\nJob search strategy · Portfolio projects · Interview prep · Skills roadmap\n\nWhat would you like to dive into?`;
  }

  // ── Follow-up: use history context ──
  if (intent === 'followup') {
    if (BOT_LAST_ENTRY) {
      return `Here's more on **${BOT_LAST_ENTRY.title}**:\n\n${BOT_LAST_ENTRY.answer}${relatedTopics(BOT_LAST_ENTRY.id)}`;
    }
    if (BOT_HISTORY.length > 0) {
      const recent = BOT_HISTORY[0];
      return `Continuing from **${recent.title}**:\n\n${recent.answer}${relatedTopics(recent.id)}`;
    }
  }

  // ── Quiz intent ──
  if (intent === 'quiz') {
    return handleQuiz(subject, tokens);
  }

  // ── Main search: hand-crafted KB ──
  const kbScores = BOT_KB
    .map(entry => ({ entry, score: scoreKB(entry, tokens, subject, intent) }))
    .filter(x => x.score > 0)
    .sort((a, b) => b.score - a.score);

  if (kbScores.length > 0 && kbScores[0].score >= 6) {
    const best = kbScores[0].entry;
    BOT_LAST_TOPIC = best.id;
    BOT_LAST_ENTRY = best;
    pushHistory(best);
    const intro = intentIntro(intent, best.title);
    return (intro || '') + best.answer + relatedTopics(best.id);
  }

  // ── Platform data search ──
  const platformResults = searchPlatformData(tokens, subject);
  if (platformResults.length > 0 && platformResults[0].score >= 6) {
    BOT_LAST_ENTRY = null;
    return formatPlatformResult(platformResults[0], intent);
  }

  // ── Weak KB match (score 3-5) ──
  if (kbScores.length > 0 && kbScores[0].score >= 3) {
    const best = kbScores[0].entry;
    BOT_LAST_TOPIC = best.id;
    BOT_LAST_ENTRY = best;
    pushHistory(best);
    const intro = intentIntro(intent, best.title);
    return (intro || '') + best.answer + relatedTopics(best.id);
  }

  // ── Weak platform match ──
  if (platformResults.length > 0 && platformResults[0].score >= 3) {
    return formatPlatformResult(platformResults[0], intent);
  }

  // ── Smart fallback: suggest closest matching topics ──
  const topMatches = BOT_KB
    .map(entry => ({ entry, score: scoreKB(entry, tokens, subject, 'define') }))
    .sort((a, b) => b.score - a.score)
    .slice(0, 4)
    .filter(x => x.score > 0);

  if (topMatches.length > 0) {
    const suggestions = topMatches.map(x => `- "${x.entry.title}"`).join('\n');
    return `I'm not 100% sure what you're asking — but based on your question, you might mean one of these:\n\n${suggestions}\n\nTry asking: "explain [topic]" or "how does [topic] work?". Type **help** for the full topic map.`;
  }

  return `I didn't quite catch that — could you rephrase?\n\nTry:\n- "What is [concept]?"\n- "How does [tool] work?"\n- "Difference between X and Y?"\n- "Give me an example of [topic]"\n\nType **help** to see my full topic map.`;
}
