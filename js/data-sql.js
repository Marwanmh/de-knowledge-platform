// ---- SQL FUNDAMENTALS ----
const SQL_TOPICS = [

  // ============================================================
  // TIER 1 — BASICS
  // ============================================================
  {
    id: 'sql-select',
    tier: 1,
    tierLabel: 'Basics',
    title: 'SELECT, FROM, WHERE, ORDER BY',
    summary: 'The foundation of every SQL query.',
    explanation: 'Every SQL query starts with SELECT (what columns), FROM (which table), optionally WHERE (filter rows), and ORDER BY (sort results). LIMIT caps how many rows come back — always use it when exploring large tables.',
    example: `-- Get all columns from a table
SELECT * FROM orders;

-- Get specific columns
SELECT order_id, customer_id, total_amount FROM orders;

-- Filter rows
SELECT order_id, total_amount
FROM orders
WHERE total_amount > 100
  AND status = 'completed';

-- Sort results (DESC = highest first)
SELECT order_id, total_amount
FROM orders
ORDER BY total_amount DESC
LIMIT 10;`,
    practice: 'Write a query that returns the order_id and total_amount for all orders where status is "shipped", sorted by total_amount from highest to lowest, showing only the top 5.',
    answer: `SELECT order_id, total_amount
FROM orders
WHERE status = 'shipped'
ORDER BY total_amount DESC
LIMIT 5;`
  },
  {
    id: 'sql-null',
    tier: 1,
    tierLabel: 'Basics',
    title: 'NULL Handling',
    summary: 'NULLs are not zero, not empty string — they are the absence of a value.',
    explanation: 'NULL comparisons with = always return NULL (not TRUE or FALSE). You must use IS NULL or IS NOT NULL. COALESCE returns the first non-null value. NULLIF returns NULL if two values are equal. NULLs are excluded from COUNT(col) but included in COUNT(*).',
    example: `-- Wrong — this never returns rows even if email IS NULL
SELECT * FROM customers WHERE email = NULL;

-- Correct
SELECT * FROM customers WHERE email IS NULL;
SELECT * FROM customers WHERE email IS NOT NULL;

-- COALESCE: substitute a default if NULL
SELECT customer_id,
       COALESCE(phone, 'No phone provided') AS phone
FROM customers;

-- NULLIF: returns NULL if values match (avoids divide-by-zero)
SELECT revenue / NULLIF(units_sold, 0) AS avg_price
FROM products;

-- COUNT difference
SELECT COUNT(*) AS total_rows,         -- includes NULLs
       COUNT(email) AS rows_with_email  -- excludes NULLs
FROM customers;`,
    practice: 'Write a query returning all customers where the city is NULL. For those customers, show their name and substitute "Unknown" for the city using COALESCE.',
    answer: `SELECT name,
       COALESCE(city, 'Unknown') AS city
FROM customers
WHERE city IS NULL;`
  },
  {
    id: 'sql-case',
    tier: 1,
    tierLabel: 'Basics',
    title: 'CASE WHEN',
    summary: 'SQL\'s if/else — creates new columns based on conditions.',
    explanation: 'CASE WHEN evaluates conditions top to bottom and returns the value of the first matching WHEN. ELSE catches anything that didn\'t match. Without ELSE, unmatched rows return NULL. Used to create category buckets, flag rows, or pivot data.',
    example: `-- Create a category column based on value
SELECT order_id,
       total_amount,
       CASE
         WHEN total_amount >= 500 THEN 'High'
         WHEN total_amount >= 100 THEN 'Medium'
         ELSE 'Low'
       END AS order_tier
FROM orders;

-- Flag rows matching a condition
SELECT customer_id,
       CASE WHEN country = 'EG' THEN 1 ELSE 0 END AS is_egypt
FROM customers;

-- Use CASE inside aggregate (conditional count)
SELECT
  COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed,
  COUNT(CASE WHEN status = 'failed'    THEN 1 END) AS failed
FROM orders;`,
    practice: 'Write a query that returns each product\'s name and a "stock_status" column: "Out of Stock" if quantity=0, "Low Stock" if quantity between 1 and 10, "In Stock" otherwise.',
    answer: `SELECT name,
       CASE
         WHEN quantity = 0          THEN 'Out of Stock'
         WHEN quantity BETWEEN 1 AND 10 THEN 'Low Stock'
         ELSE 'In Stock'
       END AS stock_status
FROM products;`
  },
  {
    id: 'sql-groupby',
    tier: 1,
    tierLabel: 'Basics',
    title: 'GROUP BY + Aggregate Functions',
    summary: 'Collapse many rows into one summary row per group.',
    explanation: 'GROUP BY groups rows with the same value in the specified column(s). Aggregate functions (COUNT, SUM, AVG, MIN, MAX) compute one value per group. Every column in SELECT must either be in GROUP BY or inside an aggregate function — this is the most common SQL beginner mistake.',
    example: `-- Count orders per customer
SELECT customer_id,
       COUNT(*) AS order_count,
       SUM(total_amount) AS total_spent,
       AVG(total_amount) AS avg_order_value
FROM orders
GROUP BY customer_id;

-- Group by multiple columns
SELECT customer_id, status, COUNT(*) AS count
FROM orders
GROUP BY customer_id, status;

-- Common mistake: selecting a column not in GROUP BY
-- This fails:
SELECT customer_id, order_date, COUNT(*)  -- order_date not in GROUP BY
FROM orders
GROUP BY customer_id;`,
    practice: 'Write a query that shows each product category with the total revenue (sum of price * quantity) and the number of distinct products in that category.',
    answer: `SELECT category,
       SUM(price * quantity) AS total_revenue,
       COUNT(DISTINCT product_id) AS product_count
FROM products
GROUP BY category;`
  },
  {
    id: 'sql-having',
    tier: 1,
    tierLabel: 'Basics',
    title: 'HAVING vs WHERE',
    summary: 'WHERE filters rows before grouping. HAVING filters groups after aggregation.',
    explanation: 'This is one of the most tested SQL concepts at junior level. WHERE runs before GROUP BY — it filters individual rows. HAVING runs after GROUP BY — it filters the aggregated groups. You cannot use aggregate functions in WHERE. You can only use aggregate functions in HAVING.',
    example: `-- WHERE: filters rows before they are grouped
-- Get orders > 100, then count per customer
SELECT customer_id, COUNT(*) AS order_count
FROM orders
WHERE total_amount > 100        -- filters ROWS first
GROUP BY customer_id;

-- HAVING: filters groups after aggregation
-- Get customers who placed MORE THAN 5 orders
SELECT customer_id, COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 5;            -- filters GROUPS

-- Combined: WHERE first, then HAVING
SELECT customer_id, COUNT(*) AS large_orders
FROM orders
WHERE total_amount > 100        -- 1st: filter rows
GROUP BY customer_id
HAVING COUNT(*) > 3;            -- 2nd: filter groups`,
    practice: 'Find all customers who have placed more than 3 orders AND whose average order value is above 200. Return customer_id, order_count, and avg_order_value.',
    answer: `SELECT customer_id,
       COUNT(*) AS order_count,
       AVG(total_amount) AS avg_order_value
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 3
   AND AVG(total_amount) > 200;`
  },
  {
    id: 'sql-dates',
    tier: 1,
    tierLabel: 'Basics',
    title: 'Date & String Functions',
    summary: 'Essential functions for cleaning and transforming data.',
    explanation: 'Date and string manipulation is core to DE work — nearly every pipeline deals with date filtering, truncation, and string standardization. Syntax varies slightly between databases (PostgreSQL shown here).',
    example: `-- DATE functions
SELECT
  CURRENT_DATE,                              -- today
  NOW(),                                     -- current timestamp
  DATE_TRUNC('month', order_date),           -- first day of month
  EXTRACT(YEAR FROM order_date) AS yr,       -- extract part
  order_date + INTERVAL '7 days',            -- add time
  order_date::DATE,                          -- cast to date

-- Filtering by date range
WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE)  -- this month
  AND order_date < NOW() - INTERVAL '1 day';            -- before yesterday

-- STRING functions
SELECT
  TRIM('  hello  '),          -- 'hello'
  UPPER('marwan'),             -- 'MARWAN'
  LOWER('CAIRO'),              -- 'cairo'
  LENGTH('data'),              -- 4
  SUBSTRING('data_eng', 1, 4), -- 'data'
  CONCAT(first_name, ' ', last_name),
  REPLACE('hello world', 'world', 'DE'),
  -- Pattern matching
  name LIKE '%Engineer%',     -- contains
  name LIKE 'Data%',          -- starts with
  name ILIKE '%engineer%';    -- case-insensitive (PostgreSQL)`,
    practice: 'Write a query returning all orders from the current month. Show order_id, a formatted date as "YYYY-MM" using DATE_TRUNC, and the customer\'s full name in UPPERCASE using CONCAT.',
    answer: `SELECT order_id,
       TO_CHAR(DATE_TRUNC('month', order_date), 'YYYY-MM') AS order_month,
       UPPER(CONCAT(first_name, ' ', last_name)) AS customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE);`
  },

  // ============================================================
  // TIER 2 — INTERMEDIATE
  // ============================================================
  {
    id: 'sql-subqueries',
    tier: 2,
    tierLabel: 'Intermediate',
    title: 'Subqueries',
    summary: 'A query nested inside another query — powerful but use CTEs when possible.',
    explanation: 'A subquery is a SELECT inside another query. Scalar subquery returns one value. Table subquery (in FROM) returns a result set. Correlated subquery references the outer query and re-runs for each row (slow on large tables). CTEs (WITH clause) are usually clearer than deeply nested subqueries.',
    example: `-- Scalar subquery in WHERE: orders above average
SELECT order_id, total_amount
FROM orders
WHERE total_amount > (SELECT AVG(total_amount) FROM orders);

-- Table subquery in FROM (inline view)
SELECT customer_id, total_spent
FROM (
  SELECT customer_id, SUM(total_amount) AS total_spent
  FROM orders
  GROUP BY customer_id
) AS customer_totals
WHERE total_spent > 1000;

-- Correlated subquery: get latest order per customer
SELECT o.order_id, o.customer_id, o.order_date
FROM orders o
WHERE o.order_date = (
  SELECT MAX(o2.order_date)
  FROM orders o2
  WHERE o2.customer_id = o.customer_id  -- references outer query
);
-- ^ Works but slow. Use ROW_NUMBER() window function instead for large tables.`,
    practice: 'Using a subquery, find all products whose price is above the average price in their own category. Return product_name, category, and price.',
    answer: `SELECT p.product_name, p.category, p.price
FROM products p
WHERE p.price > (
  SELECT AVG(p2.price)
  FROM products p2
  WHERE p2.category = p.category
);`
  },
  {
    id: 'sql-ctes',
    tier: 2,
    tierLabel: 'Intermediate',
    title: 'CTEs — WITH Clause',
    summary: 'Named temporary result sets that make complex queries readable.',
    explanation: 'A CTE (Common Table Expression) is a named subquery defined at the top of a query using WITH. You can chain multiple CTEs. Each can reference previous ones. dbt models are built entirely from CTEs — every transformation layer is a CTE. CTEs do not improve performance (they\'re not materialized), but they make queries maintainable.',
    example: `-- Single CTE
WITH monthly_revenue AS (
  SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(total_amount) AS revenue
  FROM orders
  GROUP BY 1
)
SELECT month, revenue,
       revenue - LAG(revenue) OVER (ORDER BY month) AS mom_change
FROM monthly_revenue;

-- Multiple CTEs (dbt model structure)
WITH raw_orders AS (
  SELECT * FROM orders WHERE status != 'cancelled'
),
customer_totals AS (
  SELECT customer_id,
         COUNT(*) AS order_count,
         SUM(total_amount) AS lifetime_value
  FROM raw_orders
  GROUP BY customer_id
),
flagged AS (
  SELECT *,
         CASE WHEN lifetime_value > 1000 THEN 'VIP' ELSE 'Standard' END AS tier
  FROM customer_totals
)
SELECT * FROM flagged WHERE tier = 'VIP';`,
    practice: 'Using CTEs, write a query that: (1) calculates total revenue per product, (2) ranks products by revenue, (3) returns only the top 3 products with their rank.',
    answer: `WITH product_revenue AS (
  SELECT product_id, SUM(price * quantity) AS total_revenue
  FROM order_items
  GROUP BY product_id
),
ranked AS (
  SELECT product_id, total_revenue,
         RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
  FROM product_revenue
)
SELECT product_id, total_revenue, revenue_rank
FROM ranked
WHERE revenue_rank <= 3;`
  },
  {
    id: 'sql-ddl',
    tier: 2,
    tierLabel: 'Intermediate',
    title: 'DDL — Creating & Modifying Tables',
    summary: 'How to define database structure — the write side of SQL.',
    explanation: 'DDL (Data Definition Language) is how you create and modify tables. Every DE writes CREATE TABLE statements for staging and target tables. Constraints enforce data integrity at the database level. ALTER TABLE changes existing table structure.',
    example: `-- Create a dimension table
CREATE TABLE dim_customer (
  customer_sk   SERIAL PRIMARY KEY,         -- auto-increment surrogate key
  customer_id   VARCHAR(50) NOT NULL,       -- natural key
  name          VARCHAR(200),
  email         VARCHAR(200) UNIQUE,        -- no duplicates
  city          VARCHAR(100),
  country       VARCHAR(100) DEFAULT 'EG',  -- default value
  created_at    TIMESTAMP DEFAULT NOW(),
  CONSTRAINT uq_customer_id UNIQUE (customer_id)
);

-- Create a fact table with foreign keys
CREATE TABLE fact_orders (
  order_sk      SERIAL PRIMARY KEY,
  order_id      VARCHAR(50) NOT NULL,
  customer_sk   INT REFERENCES dim_customer(customer_sk),
  order_date    DATE NOT NULL,
  total_amount  NUMERIC(12,2),
  loaded_at     TIMESTAMP DEFAULT NOW()
);

-- Modify existing table
ALTER TABLE dim_customer ADD COLUMN phone VARCHAR(20);
ALTER TABLE dim_customer DROP COLUMN phone;
ALTER TABLE dim_customer ALTER COLUMN name TYPE TEXT;

-- Create an index for faster lookups
CREATE INDEX idx_orders_customer ON fact_orders(customer_sk);
CREATE INDEX idx_orders_date ON fact_orders(order_date);`,
    practice: 'Write CREATE TABLE statements for a dim_product (product_sk, product_id, name, category, price) and a fact_sales (sale_sk, product_sk FK, quantity, sale_date, revenue) table with proper primary and foreign keys.',
    answer: `CREATE TABLE dim_product (
  product_sk  SERIAL PRIMARY KEY,
  product_id  VARCHAR(50) NOT NULL UNIQUE,
  name        VARCHAR(200) NOT NULL,
  category    VARCHAR(100),
  price       NUMERIC(10,2)
);

CREATE TABLE fact_sales (
  sale_sk     SERIAL PRIMARY KEY,
  product_sk  INT REFERENCES dim_product(product_sk),
  quantity    INT NOT NULL,
  sale_date   DATE NOT NULL,
  revenue     NUMERIC(12,2)
);`
  },
  {
    id: 'sql-indexes',
    tier: 2,
    tierLabel: 'Intermediate',
    title: 'Indexes & EXPLAIN',
    summary: 'How databases find rows fast — and how to see what\'s happening.',
    explanation: 'An index is a data structure that allows the database to find rows without scanning the entire table. B-tree indexes work for equality and range queries. Indexes speed up reads but slow down writes (they must be updated on every INSERT/UPDATE/DELETE). EXPLAIN shows the query execution plan without running it. EXPLAIN ANALYZE runs it and shows actual timings.',
    example: `-- Without index: Seq Scan (reads every row)
EXPLAIN SELECT * FROM orders WHERE customer_id = 'C123';
-- Output: Seq Scan on orders (cost=0..1200 rows=50000...)

-- Create index on the filtered column
CREATE INDEX idx_orders_customer_id ON orders(customer_id);

-- Now: Index Scan (fast, reads only matching rows)
EXPLAIN SELECT * FROM orders WHERE customer_id = 'C123';
-- Output: Index Scan using idx_orders_customer_id (cost=0..8 rows=3...)

-- Composite index: column ORDER matters
-- Good for: WHERE year = 2024 AND month = 1
-- Also good for: WHERE year = 2024  (uses first column)
-- NOT useful for: WHERE month = 1   (skips first column)
CREATE INDEX idx_orders_year_month ON orders(year, month);

-- EXPLAIN ANALYZE: actually runs the query, shows real timings
EXPLAIN ANALYZE
SELECT customer_id, COUNT(*)
FROM orders
WHERE order_date > '2024-01-01'
GROUP BY customer_id;`,
    practice: 'You have a query: SELECT * FROM events WHERE user_id = 123 AND event_type = \'click\' ORDER BY created_at DESC. The table has 50M rows. Write the CREATE INDEX statement that would best optimize this query and explain your column order choice.',
    answer: `-- Index on (user_id, event_type) filters rows first (equality conditions)
-- created_at not in index since ORDER BY runs after filtering
CREATE INDEX idx_events_user_type ON events(user_id, event_type);

-- If you also frequently filter by event_type alone, add separately:
CREATE INDEX idx_events_type ON events(event_type);

-- Column order: user_id first because it has higher cardinality
-- (more unique values = better filtering power as the leading column)`
  },

  {
    id: 'sql-views',
    tier: 2,
    tierLabel: 'Intermediate',
    title: 'Views & Materialized Views',
    summary: 'Save a query as a named object — query it like a table.',
    explanation: 'A VIEW is a stored SELECT statement. It runs the underlying query every time you query the view — no data is stored. Use views to simplify complex joins, enforce column-level security, or create a stable interface that hides table changes. A MATERIALIZED VIEW physically stores the result set on disk. Much faster to query but data is stale until you REFRESH it. Used in warehouses for pre-aggregated reporting tables.',
    example: `-- Create a regular view
CREATE VIEW vw_active_customers AS
SELECT customer_id, name, email, city
FROM customers
WHERE status = 'active';

-- Query it like a table
SELECT * FROM vw_active_customers WHERE city = 'Cairo';

-- Update the view definition
CREATE OR REPLACE VIEW vw_active_customers AS
SELECT customer_id, name, email, city, created_at
FROM customers
WHERE status = 'active';

-- Drop a view
DROP VIEW IF EXISTS vw_active_customers;

-- Materialized view: stores result on disk (faster reads, stale data)
CREATE MATERIALIZED VIEW mv_daily_revenue AS
SELECT
  DATE_TRUNC('day', order_date) AS day,
  SUM(total_amount) AS daily_revenue,
  COUNT(*) AS order_count
FROM orders
GROUP BY 1;

-- Must refresh manually to update data
REFRESH MATERIALIZED VIEW mv_daily_revenue;

-- Add index on materialized view for even faster lookups
CREATE INDEX ON mv_daily_revenue(day);`,
    practice: 'Create a view called vw_high_value_orders that returns order_id, customer_id, total_amount, and a computed column "tier" (CASE WHEN total_amount > 500 THEN \'Premium\' ELSE \'Standard\' END) for all non-cancelled orders.',
    answer: `CREATE OR REPLACE VIEW vw_high_value_orders AS
SELECT
  order_id,
  customer_id,
  total_amount,
  CASE
    WHEN total_amount > 500 THEN 'Premium'
    ELSE 'Standard'
  END AS tier
FROM orders
WHERE status != 'cancelled';`
  },
  {
    id: 'sql-procedures',
    tier: 2,
    tierLabel: 'Intermediate',
    title: 'Stored Procedures & Functions',
    summary: 'Reusable SQL logic stored in the database — called by name.',
    explanation: 'A FUNCTION returns a value and can be used in a SELECT. A PROCEDURE executes logic and returns nothing (or uses OUT parameters). Procedures are used for repetitive operations: truncating staging tables, running multi-step transforms, logging pipeline runs. Know the difference: functions are called in queries; procedures are called with CALL. PostgreSQL uses CREATE FUNCTION for both (it added PROCEDURE in v11).',
    example: `-- Function: returns a value, usable in SELECT
CREATE OR REPLACE FUNCTION get_customer_tier(p_customer_id INT)
RETURNS VARCHAR AS $$
DECLARE
  v_total NUMERIC;
BEGIN
  SELECT SUM(total_amount)
  INTO v_total
  FROM orders
  WHERE customer_id = p_customer_id;

  IF v_total > 10000 THEN RETURN 'Gold';
  ELSIF v_total > 5000 THEN RETURN 'Silver';
  ELSE RETURN 'Bronze';
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Call function inside a query
SELECT customer_id, get_customer_tier(customer_id) AS tier
FROM customers;

-- Procedure: executes logic, no return value
CREATE OR REPLACE PROCEDURE load_daily_snapshot()
LANGUAGE plpgsql AS $$
BEGIN
  -- Truncate staging
  TRUNCATE TABLE stg_orders;

  -- Insert today's orders
  INSERT INTO stg_orders
  SELECT * FROM raw_orders
  WHERE order_date = CURRENT_DATE;

  -- Log the run
  INSERT INTO pipeline_log(run_date, status)
  VALUES (CURRENT_DATE, 'success');

  COMMIT;
END;
$$;

-- Call a procedure
CALL load_daily_snapshot();`,
    practice: 'Write a function called classify_product(p_price NUMERIC) that returns "Budget" if price < 50, "Mid-range" if 50–200, "Premium" if above 200. Then write a SELECT that uses it to classify every product.',
    answer: `CREATE OR REPLACE FUNCTION classify_product(p_price NUMERIC)
RETURNS VARCHAR AS $$
BEGIN
  IF p_price < 50 THEN RETURN 'Budget';
  ELSIF p_price <= 200 THEN RETURN 'Mid-range';
  ELSE RETURN 'Premium';
  END IF;
END;
$$ LANGUAGE plpgsql;

SELECT product_id, name, price,
       classify_product(price) AS price_tier
FROM products;`
  },
  {
    id: 'sql-jobs',
    tier: 2,
    tierLabel: 'Intermediate',
    title: 'Scheduled Jobs',
    summary: 'Run SQL automatically on a schedule — the database-side scheduler.',
    explanation: 'In production DE, pipelines run on schedules. Within the database itself, you can schedule SQL using pg_cron (PostgreSQL extension), DBMS_SCHEDULER (Oracle), or SQL Server Agent. In data warehouses: BigQuery Scheduled Queries, Snowflake Tasks. The key concept: a job is a cron expression + a SQL statement. In modern stacks, Airflow or dbt handle scheduling at the orchestration layer — but knowing database-native jobs is tested in interviews because legacy systems use them heavily.',
    example: `-- PostgreSQL: enable pg_cron extension (run as superuser)
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Schedule a job: refresh materialized view every hour
SELECT cron.schedule(
  'refresh-daily-revenue',     -- job name
  '0 * * * *',                 -- cron: every hour at :00
  'REFRESH MATERIALIZED VIEW mv_daily_revenue'
);

-- Schedule a job: truncate staging table at midnight
SELECT cron.schedule(
  'truncate-staging',
  '0 0 * * *',                 -- daily at 00:00
  'TRUNCATE TABLE stg_orders'
);

-- List all scheduled jobs
SELECT * FROM cron.job;

-- Remove a job
SELECT cron.unschedule('refresh-daily-revenue');

-- Snowflake equivalent: TASK
CREATE OR REPLACE TASK refresh_summary_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 6 * * * UTC'  -- 6am UTC daily
AS
  INSERT INTO summary_table
  SELECT DATE_TRUNC('day', order_date), SUM(total_amount)
  FROM orders
  WHERE order_date = CURRENT_DATE - 1
  GROUP BY 1;

ALTER TASK refresh_summary_task RESUME;  -- tasks start suspended

-- Cron cheat sheet:
-- "0 * * * *"   = every hour
-- "0 0 * * *"   = daily at midnight
-- "0 6 * * 1"   = every Monday at 6am
-- "*/15 * * * *" = every 15 minutes`,
    practice: 'Write a pg_cron job called "daily-customer-stats" that runs at 2am every day and calls the stored procedure CALL load_daily_snapshot(). Then write the query to verify it was registered.',
    answer: `-- Schedule the job
SELECT cron.schedule(
  'daily-customer-stats',
  '0 2 * * *',
  'CALL load_daily_snapshot()'
);

-- Verify it was registered
SELECT jobid, jobname, schedule, command, active
FROM cron.job
WHERE jobname = 'daily-customer-stats';`
  },

  // ============================================================
  // TIER 3 — ADVANCED
  // ============================================================
  {
    id: 'sql-window',
    tier: 3,
    tierLabel: 'Advanced',
    title: 'Window Functions',
    summary: 'Perform calculations across related rows without collapsing them.',
    explanation: 'Window functions compute a value for each row based on a "window" of related rows — unlike GROUP BY which collapses rows. PARTITION BY divides rows into groups (like GROUP BY but keeps all rows). ORDER BY within the window determines calculation order. The frame clause (ROWS BETWEEN) controls which rows are included.',
    example: `-- ROW_NUMBER: rank rows within each customer (latest order = 1)
SELECT order_id, customer_id, order_date,
       ROW_NUMBER() OVER (
         PARTITION BY customer_id
         ORDER BY order_date DESC
       ) AS rn
FROM orders;
-- Use to get latest order: WHERE rn = 1 in a CTE/subquery

-- RANK vs DENSE_RANK (tie handling)
-- RANK: 1,2,2,4 (skips 3 after tie)
-- DENSE_RANK: 1,2,2,3 (no gap after tie)
SELECT product_id, revenue,
       RANK() OVER (ORDER BY revenue DESC) AS rnk,
       DENSE_RANK() OVER (ORDER BY revenue DESC) AS dense_rnk
FROM product_revenue;

-- LAG/LEAD: access previous/next row
SELECT month, revenue,
       LAG(revenue, 1) OVER (ORDER BY month) AS prev_month,
       revenue - LAG(revenue, 1) OVER (ORDER BY month) AS mom_change
FROM monthly_revenue;

-- Running total + moving average
SELECT order_date, daily_revenue,
       SUM(daily_revenue) OVER (ORDER BY order_date
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
       AVG(daily_revenue) OVER (ORDER BY order_date
         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_avg
FROM daily_sales;`,
    practice: 'Write a query that returns each order with: the customer\'s rank by total_amount within their country (highest = rank 1), and the difference between this order\'s amount and that customer\'s average order amount.',
    answer: `WITH customer_avg AS (
  SELECT customer_id,
         AVG(total_amount) AS avg_amount
  FROM orders
  GROUP BY customer_id
)
SELECT o.order_id,
       o.customer_id,
       c.country,
       o.total_amount,
       o.total_amount - ca.avg_amount AS diff_from_avg,
       RANK() OVER (
         PARTITION BY c.country
         ORDER BY o.total_amount DESC
       ) AS country_rank
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN customer_avg ca ON o.customer_id = ca.customer_id;`
  },
  {
    id: 'sql-explain-plan',
    tier: 3,
    tierLabel: 'Advanced',
    title: 'Reading Query Plans',
    summary: 'Diagnose slow queries — the skill that separates juniors from seniors.',
    explanation: 'EXPLAIN ANALYZE shows exactly how the database executed your query: which scan type (Seq Scan vs Index Scan), join strategy (Hash Join vs Nested Loop vs Merge Join), estimated vs actual rows, and time per operation. High cost operations are often missing indexes, bad statistics, or unselective filters.',
    example: `EXPLAIN ANALYZE
SELECT c.name, COUNT(o.order_id) AS order_count
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE c.country = 'EG'
GROUP BY c.name;

/* Example output breakdown:
HashAggregate (cost=2850..2900 rows=500) (actual time=45..48 ms rows=320)
  -> Hash Left Join (cost=120..2600 rows=5000) (actual time=5..40 ms rows=5000)
       Hash Cond: (o.customer_id = c.customer_id)
       -> Seq Scan on orders (cost=0..800 rows=50000) (actual time=0..15 ms)
       -> Hash (cost=80..80 rows=3200) (actual time=4..4 ms rows=3200)
            -> Index Scan on customers using idx_country
                 (cost=0..80 rows=3200) (actual time=0..3 ms rows=3200)
                 Index Cond: (country = 'EG')

Key things to look for:
- Seq Scan on a large table = missing index
- Nested Loop with large row estimates = potential performance problem
- Actual rows >> Estimated rows = stale statistics (run ANALYZE table_name)
- High cost Hash operations = shuffle/sort happening (data is being rearranged)
*/`,
    practice: 'You see "Seq Scan on orders (cost=0..48000)" in your EXPLAIN output. The query filters by order_date. What is the likely cause and what is your fix?',
    answer: `-- Cause: no index on order_date column
-- The DB is scanning every row in the orders table (Seq Scan)
-- instead of jumping directly to matching rows

-- Fix: create an index
CREATE INDEX idx_orders_order_date ON orders(order_date);

-- After creating the index, re-run EXPLAIN ANALYZE
-- You should see "Index Scan" instead of "Seq Scan"
-- Cost drops dramatically for selective date range queries

-- Also run ANALYZE if statistics are stale:
ANALYZE orders;`
  }
];
