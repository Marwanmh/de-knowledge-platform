const MARWAN = {
  name: "Marwan Mohamed",
  role: "Data Engineer",
  location: "Giza, Egypt",
  experience: [
    { company: "Valify Solutions", role: "Data Engineer", period: "Jan 2026 – Present" },
    { company: "Etisalat Egypt", role: "Data Warehouse Support", period: "Jan 2025 – Dec 2025" }
  ],
  skills: ["Python", "SQL", "Apache Airflow", "Apache Spark", "PostgreSQL", "PySpark", "Pandas", "Docker"]
};

const KNOWLEDGE = [
  {
    id: "oltp-olap",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "OLTP vs OLAP",
    subtitle: "Transactional vs Analytical databases",
    tags: ["foundations", "databases"],
    summary: "Two fundamentally different database paradigms serving different masters.",
    points: [
      { label: "OLTP", text: "Transactional database powering production apps. Optimized for fast reads/writes of individual rows. Think banking transactions, e-commerce orders." },
      { label: "OLAP", text: "Analytical database built for complex queries across millions of rows. Optimized for aggregations, not row-level operations." },
      { label: "Why separate them", text: "Analytical workloads are read-heavy, resource-intensive, and slow. Running them on production OLTP systems would degrade app performance and risk data integrity." },
      { label: "Real-world example", text: "Telecom billing systems use Teradata or Snowflake for analytics (OLAP) while transactional systems run on PostgreSQL or Oracle (OLTP). Classic separation in enterprise data stacks." }
    ],
    relatedIds: ["pipeline-phases", "etl-elt", "dw-lake-lakehouse"]
  },
  {
    id: "fact-dimension",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "Fact vs Dimension Tables",
    subtitle: "The foundation of dimensional modeling",
    tags: ["dimensional-modeling", "schema-design"],
    summary: "Facts store numbers. Dimensions store context. Together they answer every business question.",
    points: [
      { label: "Fact Table", text: "Holds measurable, numeric business events. Examples: quantity sold, revenue, clicks, duration. Each row = one business event." },
      { label: "Dimension Table", text: "Holds descriptive context about facts. Examples: customer name, product category, store location, date details. Answers the WHO/WHAT/WHERE/WHEN." },
      { label: "Surrogate Key", text: "A system-generated integer primary key on dimension tables (not from source data). Used to join facts to dimensions. More stable than natural/business keys which can change." },
      { label: "Build order", text: "Always build dimensions first — fact tables reference dimension surrogate keys via foreign keys. Can't build the fact without the keys it references." },
      { label: "Your project", text: "Your Data Warehouse End-to-End project applied this exactly: surrogate-keyed dimensions + central fact table capturing quantity, price, total amount." }
    ],
    relatedIds: ["star-snowflake", "scd-types", "surrogate-scd"]
  },
  {
    id: "star-snowflake",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "Star Schema vs Snowflake Schema",
    subtitle: "~90% of production warehouses use Star Schema",
    tags: ["dimensional-modeling", "schema-design"],
    summary: "Star = simple, fast, widely used. Snowflake = normalized, complex, rarely preferred.",
    points: [
      { label: "Star Schema", text: "One central fact table surrounded by denormalized dimension tables. All dimensions connect directly to fact. Simple joins, fast queries, easy for BI tools. Used ~90% of the time." },
      { label: "Snowflake Schema", text: "Dimensions are normalized — split into sub-dimensions not directly connected to fact. Example: City → State → Country chain instead of one Location dim. Saves storage, hurts query performance." },
      { label: "Why Star wins", text: "Query performance, simplicity, BI tool compatibility. Storage is cheap. Query time is expensive. Denormalization is a deliberate tradeoff." },
      { label: "Your work", text: "Your DW project used star schema correctly with surrogate keys establishing the hub-and-spoke fact→dim relationships." },
      { label: "Grain", text: "The grain of a fact table = what one row represents. Must be defined FIRST before designing the table. Examples: 'one row per order line item', 'one row per daily product-store combination', 'one row per website click'. Mixing grains in one fact table causes incorrect aggregations." }
    ],
    relatedIds: ["fact-dimension", "scd-types", "dbt-tool"]
  },
  {
    id: "pipeline-phases",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "Pipeline Phases: Stage → Transform → Core",
    subtitle: "Three-layer architecture for clean, reliable data flow",
    tags: ["pipeline", "architecture"],
    summary: "Raw in, clean out — with a clear boundary at each layer.",
    points: [
      { label: "Stage", text: "Raw data lands here unchanged. Direct copy from source. No transformations. Acts as a safety net — if something breaks downstream, raw data is preserved." },
      { label: "Transform", text: "Raw data gets cleaned, standardized, validated, joined. Business rules applied here. Bad data caught here before polluting production." },
      { label: "Core", text: "Clean, analytics-ready data. What BI tools and analysts query. Structured as star schema (fact + dimension tables). Trusted source of truth." },
      { label: "Connection to your work", text: "Your DW project implemented this exactly: staging layer → SQL transformation views → core warehouse with fact and dimension tables." }
    ],
    relatedIds: ["etl-elt", "incremental-loading", "airflow", "medallion-arch"]
  },
  {
    id: "incremental-loading",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "Incremental Loading",
    subtitle: "Load only new/changed data — never reload everything",
    tags: ["pipeline", "data-ingestion"],
    summary: "First load = full. Every load after = delta only. Prevents duplication, saves resources.",
    points: [
      { label: "Problem it solves", text: "Re-loading all data every run is expensive and causes duplication. Incremental loading loads only records newer than the last successful run." },
      { label: "Common conditions", text: "Date watermark (WHERE updated_at > last_run_timestamp), ID range, hash comparison. The condition is your guard against duplicates." },
      { label: "Real-world example", text: "A common pattern: Python pipeline reads WHERE updated_at > last_run_timestamp, stages records, then writes the new high-watermark back to a metadata table. Used in nearly every production DE pipeline." },
      { label: "Watch out for", text: "Late-arriving data (records with old timestamps that arrive late), soft deletes (deleted records don't appear as new — you miss them without SCD or tombstone patterns)." },
      { label: "Watermark table pattern", text: "Store last_run_timestamp in a metadata table (e.g. pipeline_metadata). On each run: read last_run from table, load WHERE updated_at > last_run, on success write new timestamp back. Survives restarts and provides auditability." }
    ],
    relatedIds: ["pipeline-phases", "scd-types", "airflow", "idempotency"]
  },
  {
    id: "etl-elt",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "ETL vs ELT",
    subtitle: "When to transform: before or after loading?",
    tags: ["pipeline", "architecture"],
    summary: "ETL = transform before load (traditional). ELT = load raw then transform in-warehouse (modern).",
    points: [
      { label: "ETL", text: "Extract → Transform → Load. Data is cleaned/transformed before entering the warehouse. Common with legacy tools (DataStage, Informatica). Transform happens in external compute." },
      { label: "ELT", text: "Extract → Load → Transform. Raw data lands in warehouse first, then transformed using warehouse compute (SQL/dbt). Modern approach enabled by powerful cloud DWs (BigQuery, Snowflake, Redshift)." },
      { label: "Why ELT is trending", text: "Cloud warehouses have near-unlimited compute. Storing raw data first enables reprocessing if business logic changes. Simpler pipelines — just move data, transform in SQL." },
      { label: "Tool comparison", text: "IBM DataStage, Informatica, SSIS = classic ETL tools (transform before load). Airflow + dbt = modern ELT approach (load raw, transform in warehouse). Most new companies build ELT stacks." },
      { label: "Reverse ETL", text: "Sending data FROM the warehouse BACK to operational systems (CRM, Salesforce, marketing tools). Warehouse becomes the source of truth; operational tools consume it. Tools: Census, Hightouch. Increasingly common in modern data stacks." }
    ],
    relatedIds: ["pipeline-phases", "dbt-tool", "dw-lake-lakehouse"]
  },
  {
    id: "dw-lake-lakehouse",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "Data Warehouse vs Data Lake vs Lakehouse",
    subtitle: "Three storage paradigms, each with a different purpose",
    tags: ["architecture", "storage"],
    summary: "Warehouse = structured. Lake = anything. Lakehouse = both, structured on top of flexible.",
    points: [
      { label: "Data Warehouse", text: "Stores structured, processed data. Schema enforced on write. SQL-queryable. High performance for analytics. Examples: PostgreSQL, Teradata, BigQuery, Snowflake, Redshift." },
      { label: "Data Lake", text: "Stores raw data in any format (structured, semi-structured, unstructured: JSON, CSV, images, logs, Parquet). Schema on read. Cheap storage (S3, ADLS, GCS). No inherent query performance." },
      { label: "Lakehouse", text: "Data Lake storage (cheap, flexible) + Data Warehouse features (ACID transactions, schema enforcement, fast SQL queries). Enabled by table formats: Delta Lake, Apache Iceberg, Apache Hudi." },
      { label: "Trend", text: "Most modern data stacks move toward Lakehouse. You store everything in object storage, use table formats for reliability, query with engines like Spark or Trino." }
    ],
    relatedIds: ["oltp-olap", "etl-elt", "spark-architecture", "delta-iceberg"]
  },
  {
    id: "airflow",
    section: "Orchestration — Apache Airflow",
    sectionIcon: "🌊",
    title: "Apache Airflow",
    subtitle: "Orchestration engine for managing pipelines end-to-end",
    tags: ["orchestration", "tools"],
    summary: "Airflow doesn't move data — it manages WHEN and HOW your pipeline runs.",
    points: [
      { label: "What it does", text: "Schedules, triggers, monitors, and retries pipeline tasks. Defines dependencies between tasks. Sends alerts on failure. Your pipeline's traffic controller." },
      { label: "DAGs", text: "Directed Acyclic Graph. The Python file defining your pipeline. 'Directed' = tasks have direction/dependencies. 'Acyclic' = no cycles (no loop back). NOT required to be linear — can have parallel branches and diamonds." },
      { label: "Tasks & Operators", text: "Each node in a DAG is a Task. Operators define WHAT the task does: PythonOperator (run Python), BashOperator (run shell), PostgresOperator (run SQL), S3ToRedshiftOperator, etc." },
      { label: "XComs", text: "Cross-communication mechanism for tasks to pass data to each other. Task A pushes a value (xcom_push), Task B pulls it (xcom_pull). Used for small values like record counts, file paths, or status flags — NOT large datasets." },
      { label: "default_args & catchup", text: "default_args dict sets DAG-level defaults: owner, retries, retry_delay, email_on_failure. catchup=False prevents Airflow from backfilling all missed runs since start_date — critical setting to get right or you'll trigger hundreds of runs at once." },
      { label: "Sensors", text: "Special operators that wait for a condition before proceeding. FileSensor (wait for file to appear), ExternalTaskSensor (wait for another DAG's task to succeed), HttpSensor (wait for API to return 200). Use poke_interval and timeout to avoid infinite waits." },
      { label: "Real-world example", text: "Production Airflow DAG: PythonOperator extracts from PostgreSQL, Pandas transforms and validates data, PostgresOperator loads into staging tables, EmailOperator sends failure alerts on any task failure." }
    ],
    relatedIds: ["pipeline-phases", "incremental-loading", "spark-architecture", "docker-de"]
  },
  {
    id: "spark-architecture",
    section: "Big Data — Apache Spark",
    sectionIcon: "⚡",
    title: "Apache Spark Architecture",
    subtitle: "Master-slave distributed processing engine",
    tags: ["spark", "big-data", "distributed-systems"],
    summary: "Cluster Manager = brain. Driver = coordinator. Executors = workers. Data = partitions.",
    points: [
      { label: "Cluster Manager", text: "Manages cluster resources. Allocates CPU/memory to applications. Can be YARN, Mesos, Kubernetes, or Spark Standalone. The infrastructure layer." },
      { label: "Driver", text: "The main process running your application code. Converts your code into a DAG of tasks, optimizes it (via Catalyst optimizer), and distributes tasks to executors." },
      { label: "Executors", text: "Worker processes that run tasks and store data. Each executor has multiple cores (task slots) and memory. They send results back to driver." },
      { label: "Worker Nodes", text: "Physical/virtual machines that host executors. Multiple executors can run on one worker. Not the same as executor — worker is the machine, executor is the process." },
      { label: "spark.sql.shuffle.partitions", text: "Default number of partitions after a shuffle = 200. For small datasets this creates 200 tiny partitions (overhead). For very large datasets, 200 may be too few (each partition too large). Tune to 2–4× number of executor cores. Set with spark.conf.set('spark.sql.shuffle.partitions', 50)." }
    ],
    relatedIds: ["spark-rdd-partitions", "spark-shuffle", "spark-lazy-eval"]
  },
  {
    id: "spark-rdd-partitions",
    section: "Big Data — Apache Spark",
    sectionIcon: "⚡",
    title: "Partitions & RDDs",
    subtitle: "How Spark distributes and represents data",
    tags: ["spark", "big-data"],
    summary: "RDD = distributed collection. Partitions = units of parallel work. Immutability = safety.",
    points: [
      { label: "RDD", text: "Resilient Distributed Dataset. The fundamental data structure in Spark. A distributed collection of objects split across the cluster. Fault-tolerant (can recompute lost partitions from lineage)." },
      { label: "Partitions", text: "An RDD is divided into partitions. Each partition lives on one executor. Tasks process one partition at a time. More partitions = more parallelism (up to number of cores)." },
      { label: "Immutability", text: "RDDs cannot be modified. Transformations create new RDDs from existing ones. The lineage graph tracks this chain. This enables fault recovery — recompute any lost RDD from its parent." },
      { label: "DataFrames vs RDDs", text: "Modern Spark uses DataFrames/Datasets (structured API) built on top of RDDs. Higher level, SQL-like, benefits from Catalyst optimizer. RDD API is lower-level but gives full control." }
    ],
    relatedIds: ["spark-architecture", "spark-cache-persist", "spark-lazy-eval", "spark-shuffle"]
  },
  {
    id: "spark-shuffle",
    section: "Big Data — Apache Spark",
    sectionIcon: "⚡",
    title: "Shuffling",
    subtitle: "The most expensive operation in Spark",
    tags: ["spark", "big-data", "performance"],
    summary: "Shuffle = moving data across executors over network. Triggered by wide transformations.",
    points: [
      { label: "What it is", text: "Redistribution of data across partitions — potentially moving data between executors over the network. Required when a transformation needs data from multiple partitions." },
      { label: "What triggers it", text: "Wide transformations: groupBy, join, distinct, repartition, orderBy, reduceByKey. Narrow transformations (map, filter, select) do NOT shuffle — they process partition-locally." },
      { label: "Why it's expensive", text: "Network I/O + disk I/O (data written to disk between stages). Shuffles are the #1 cause of Spark slowness. Minimize them in critical paths." },
      { label: "Optimization", text: "Broadcast joins (small table sent to all executors — no shuffle), partition pruning, avoiding groupBy on high-cardinality columns, pre-partitioning data by join key." }
    ],
    relatedIds: ["spark-rdd-partitions", "spark-cache-persist", "wide-narrow-transforms"]
  },
  {
    id: "spark-cache-persist",
    section: "Big Data — Apache Spark",
    sectionIcon: "⚡",
    title: "Cache & Persist",
    subtitle: "Avoid recomputing frequently used DataFrames",
    tags: ["spark", "big-data", "performance"],
    summary: "cache() = shorthand for persist(). Stores computed results to skip recomputation.",
    points: [
      { label: "What it solves", text: "Spark recomputes an RDD/DataFrame from scratch on every action (lazy eval). If you use the same DataFrame in multiple places, persist it to avoid redundant computation." },
      { label: "cache()", text: "Shorthand for persist() with default storage level. For DataFrames/Datasets: defaults to MEMORY_AND_DISK. For RDDs: defaults to MEMORY_ONLY." },
      { label: "MEMORY_AND_DISK", text: "Store in memory. If memory full, overflow to disk. Good balance — fast if fits in memory, no data loss if it doesn't. Default for DataFrames." },
      { label: "MEMORY_ONLY", text: "Store only in memory. If full, data is NOT written to disk — it's simply recomputed when needed. Fastest when data fits, but can be slow if it doesn't." },
      { label: "DISK_ONLY", text: "Store only on disk. Slowest option. Use only when data is too large for memory and recomputation is very expensive." },
      { label: "unpersist()", text: "Always call unpersist() when done with a cached DataFrame. Spark won't automatically free the memory until eviction, which can cause OOM errors." },
      { label: "When NOT to cache", text: "Don't cache DataFrames used only once — you pay the serialization/memory cost for zero benefit. Don't cache very large DataFrames that exceed executor memory (spills to disk, may be slower than recompute). Cache only when a DataFrame is reused 2+ times in the same job." }
    ],
    relatedIds: ["spark-lazy-eval", "spark-rdd-partitions", "spark-shuffle"]
  },
  {
    id: "spark-lazy-eval",
    section: "Big Data — Apache Spark",
    sectionIcon: "⚡",
    title: "Lazy Evaluation",
    subtitle: "Spark's most powerful optimization mechanism",
    tags: ["spark", "big-data", "performance"],
    summary: "Transformations don't execute immediately — they build a plan. Actions trigger execution + optimization.",
    points: [
      { label: "How it works", text: "When you call filter(), groupBy(), select() etc., nothing runs. Spark records the transformation in a DAG (execution plan). Only when an action is called (collect(), count(), write(), show()) does Spark execute." },
      { label: "Actions vs Transformations", text: "Transformations: filter, map, select, groupBy, join, withColumn — lazy, return new DataFrame. Actions: collect(), count(), show(), write(), take() — trigger execution." },
      { label: "Why it's powerful", text: "At execution time, Catalyst Optimizer sees your ENTIRE chain of transformations and can: reorder operations, push filters down, merge steps, eliminate unnecessary work. You can't do this if you execute eagerly." },
      { label: "Real example", text: "filter().groupBy().join() — Spark may push the filter before the join, dramatically reducing data shuffled. You wrote the code in one order; Spark executes it in the optimal order." },
      { label: "Analogy", text: "Like SQL query planning — you write SELECT * FROM a JOIN b WHERE a.x > 5, but the optimizer executes filter first, then join on smaller dataset." },
      { label: "Inspect the plan", text: "df.explain() prints Spark's execution plan. df.explain(True) shows all plan stages: Parsed → Analyzed → Optimized → Physical. Use it to verify filter pushdown happened and joins are optimal." }
    ],
    relatedIds: ["spark-cache-persist", "spark-rdd-partitions", "spark-shuffle"]
  },
  {
    id: "sql-joins",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "SQL Joins",
    subtitle: "How to combine tables — the most-tested SQL skill",
    tags: ["sql", "foundations"],
    summary: "Joins combine rows from two tables based on a related column. Every interview tests this.",
    points: [
      { label: "INNER JOIN", text: "Returns only rows that have a match in BOTH tables. Most common join. If a customer has no orders, they don't appear in the result. Think: intersection." },
      { label: "LEFT JOIN", text: "Returns ALL rows from the left table, plus matched rows from the right. If no match, right-side columns are NULL. Classic use: get all customers, show their orders (or NULL if none)." },
      { label: "RIGHT JOIN", text: "Mirror of LEFT JOIN — all rows from right table, matched from left. Rarely used; you can always rewrite as LEFT JOIN by swapping table order." },
      { label: "FULL OUTER JOIN", text: "Returns all rows from both tables. NULLs fill in where no match exists on either side. Use when you want everything from both sides regardless of match." },
      { label: "CROSS JOIN", text: "Cartesian product — every row from left combined with every row from right. 100 rows × 50 rows = 5,000 rows. Rarely useful; watch out for accidental cross joins (missing join condition)." },
      { label: "Common interview trap", text: "Filtering on a LEFT JOIN result column in WHERE instead of ON clause converts it to an INNER JOIN. WHERE b.id IS NOT NULL after a LEFT JOIN eliminates the NULLs — you lose the 'all rows from left' guarantee." }
    ],
    relatedIds: ["oltp-olap", "fact-dimension", "advanced-sql"]
  },
  {
    id: "parquet-format",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "Parquet File Format",
    subtitle: "Why analytics uses Parquet instead of CSV",
    tags: ["storage", "big-data", "foundations"],
    summary: "Columnar storage format. Reads only needed columns. Compresses efficiently. Industry default for analytics.",
    points: [
      { label: "Row vs Columnar storage", text: "CSV/JSON store data row-by-row (all columns for row 1, then row 2...). Parquet stores column-by-column (all values for col1, then col2...). For analytics querying 3 columns out of 100, Parquet reads 3% of the data. CSV reads 100%." },
      { label: "Compression", text: "Columnar storage compresses much better — values in the same column have similar types and patterns. A 10GB CSV typically becomes 1–2GB Parquet. Less I/O = faster reads." },
      { label: "Schema embedded", text: "Parquet files carry their schema (column names, types) inside the file itself. No separate schema file needed. Readers know exactly what they're getting." },
      { label: "Predicate pushdown", text: "Parquet stores min/max statistics per column per row group. Query engines can skip entire row groups that can't match your WHERE filter — without reading the data." },
      { label: "Why it's the standard", text: "Spark writes Parquet by default. Delta Lake and Iceberg are built on Parquet. dbt output is Parquet in cloud DWs. S3/GCS data lakes store Parquet. If you're doing analytics data engineering, your files are Parquet." },
      { label: "vs CSV and JSON", text: "CSV: human-readable, no types, no compression, slow for analytics. JSON: flexible (nested), very large, slow. Parquet: binary, typed, compressed, fast for analytics. Use CSV/JSON for source ingestion, convert to Parquet for storage." }
    ],
    relatedIds: ["dw-lake-lakehouse", "spark-architecture", "delta-iceberg"]
  },
  {
    id: "idempotency",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "Idempotency in Pipelines",
    subtitle: "Running a pipeline twice must produce the same result as running it once",
    tags: ["pipeline", "architecture", "reliability"],
    summary: "Idempotent pipelines can safely be re-run after failure without double-counting or corrupting data.",
    points: [
      { label: "What it means", text: "f(f(x)) = f(x). Running your pipeline N times produces the same result as running it once. Critical for production pipelines where retries, backfills, and re-runs are normal." },
      { label: "Why it matters", text: "Airflow retries failed tasks automatically. On-call engineers re-run failed pipelines. If your pipeline isn't idempotent, every retry double-inserts data or corrupts aggregates." },
      { label: "Truncate-insert pattern", text: "DELETE all rows for the target partition/date before inserting. Run twice: deletes then inserts, deletes then inserts — same final state both times. Simple and reliable for batch pipelines." },
      { label: "MERGE / UPSERT pattern", text: "INSERT or UPDATE based on whether a key already exists. Naturally idempotent — re-running matches existing rows and updates them, doesn't insert duplicates." },
      { label: "Watermark guard", text: "Incremental pipelines use a metadata table storing last_run_timestamp. Even if re-run, they process the same window and the MERGE handles dedup. The guard prevents processing future data." },
      { label: "Interview framing", text: "Always say: 'My pipelines are designed to be idempotent — they use a truncate-insert or MERGE pattern so re-running after a failure produces the same result without side effects.'" }
    ],
    relatedIds: ["incremental-loading", "airflow", "pipeline-phases"]
  },
  {
    id: "acid-transactions",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "ACID Transactions",
    subtitle: "The four guarantees that make database writes safe",
    tags: ["databases", "reliability", "foundations"],
    summary: "Atomicity, Consistency, Isolation, Durability — the contract a database makes about your writes.",
    points: [
      { label: "Atomicity", text: "A transaction either fully succeeds or fully fails — no partial states. If you INSERT 1000 rows and it crashes on row 600, none of the 1000 are committed. All or nothing." },
      { label: "Consistency", text: "A transaction brings the database from one valid state to another. Constraints (foreign keys, NOT NULL, UNIQUE) are always enforced. You can't leave the DB in an invalid state." },
      { label: "Isolation", text: "Concurrent transactions don't interfere with each other. Transaction A doesn't see Transaction B's uncommitted changes. Different isolation levels trade performance for strictness." },
      { label: "Durability", text: "Once a transaction is committed, it's permanent — survives crashes, power failures, restarts. Achieved via write-ahead logs (WAL) written to disk before commit returns." },
      { label: "Why data lakes lack ACID", text: "Plain files on S3 (CSV, Parquet) have no transaction coordinator. Two writers can overwrite each other. A crashed write leaves a partial file. Readers can see partial data mid-write. This is WHY Delta Lake and Iceberg exist." },
      { label: "ACID in DE pipelines", text: "OLTP databases (PostgreSQL, MySQL) are fully ACID. Traditional data lakes are NOT. Lakehouse table formats (Delta Lake, Iceberg) ADD ACID to object storage — enabling safe concurrent writes, rollbacks, and time travel." }
    ],
    relatedIds: ["dw-lake-lakehouse", "delta-iceberg", "oltp-olap"]
  },
  {
    id: "storage-partitioning",
    section: "Big Data — Apache Spark",
    sectionIcon: "⚡",
    title: "Storage Partitioning",
    subtitle: "How data is physically organized on disk for fast reads",
    tags: ["spark", "big-data", "performance", "storage"],
    summary: "Hive-style folder partitioning lets query engines skip entire directories. Different from Spark runtime partitions.",
    points: [
      { label: "What it is", text: "Physically splitting data files into folders based on column values. Example: year=2024/month=01/day=15/file.parquet. Standard pattern in all data lakes and lakehouses." },
      { label: "Partition pruning", text: "When you query WHERE year=2024 AND month=01, Spark only opens the matching folders — skips everything else. Scanning 1 day out of 3 years = reading ~0.1% of data instead of 100%." },
      { label: "Different from Spark partitions", text: "Spark runtime partitions = data split across executor memory during processing. Storage partitions = folder structure on disk. They're related (Spark uses storage partitions to determine task boundaries) but NOT the same thing." },
      { label: "Partition column choice", text: "Choose columns you frequently filter on. Date (year/month/day) is universal. Event_type, region, source_system are common. Avoid high-cardinality columns (user_id) — creates millions of tiny files (small file problem)." },
      { label: "Small file problem", text: "Over-partitioning creates thousands of tiny files. Each file = overhead for the metadata layer. Spark has to open/close thousands of files instead of a few large ones. Periodically compact small files with repartition().write()." },
      { label: "How to write partitioned data", text: "df.write.partitionBy('year', 'month', 'day').parquet('s3://bucket/table/'). Spark creates the folder structure automatically. Reading back respects partition pruning automatically." }
    ],
    relatedIds: ["spark-rdd-partitions", "parquet-format", "spark-shuffle"]
  },
  {
    id: "docker-de",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "Docker for Data Engineering",
    subtitle: "Reproducible environments for pipelines and tools",
    tags: ["tools", "infrastructure"],
    summary: "Containers package your code + dependencies together. Same behavior everywhere — local, CI, production.",
    points: [
      { label: "Container vs VM", text: "VM = full OS + your app. Container = just your app + its dependencies, sharing the host OS kernel. Containers start in seconds, use far less memory, and are portable across machines." },
      { label: "Why DEs use Docker", text: "Airflow locally via docker-compose (one command spins up webserver + scheduler + workers + Postgres). Reproducible pipeline environments (Python 3.11 + exact package versions). Consistent behavior from your laptop to production." },
      { label: "Dockerfile", text: "Recipe to build a Docker image. FROM base_image, RUN install_commands, COPY your_code, CMD start_command. Building creates an image — a snapshot of the environment." },
      { label: "docker-compose", text: "Defines multi-container applications in a YAML file. Airflow needs webserver + scheduler + worker + metadata DB + redis — docker-compose starts all of them together with one command: docker-compose up." },
      { label: "Image vs Container", text: "Image = blueprint (static, like a class). Container = running instance of that image (like an object). You can run 5 containers from the same image simultaneously." },
      { label: "Your Airflow setup", text: "The official Airflow docker-compose.yaml is the standard way to run Airflow locally. Mount your dags/ folder as a volume — changes appear instantly without rebuilding the image." }
    ],
    relatedIds: ["airflow", "pipeline-phases"]
  },
  {
    id: "data-quality",
    section: "Data Warehouse Fundamentals",
    sectionIcon: "🏛️",
    title: "Data Quality",
    subtitle: "How to define and enforce quality in your pipelines",
    tags: ["pipeline", "reliability", "foundations"],
    summary: "Bad data in = bad decisions out. Quality is enforced at ingestion, not discovered by analysts.",
    points: [
      { label: "Completeness", text: "Are all expected records present? Are required fields populated? Example: 10,000 orders expected today, only 8,000 arrived — pipeline may have failed mid-run. NULL rate on non-nullable fields is a completeness metric." },
      { label: "Accuracy", text: "Do values reflect reality? Is revenue in the correct currency? Are dates valid (no 2099-01-01 from bad defaults)? Accuracy issues often come from source system bugs." },
      { label: "Consistency", text: "Same entity represented the same way across sources. Customer 'Marwan Mohamed' in CRM vs 'M. Mohamed' in ERP — same person, inconsistent representation. Requires deduplication and standardization." },
      { label: "Uniqueness", text: "No duplicate records for the same business event. A transaction_id should appear exactly once in a fact table. Duplicate detection uses GROUP BY + HAVING COUNT(*) > 1." },
      { label: "Timeliness", text: "Data available when it's needed. SLA: 'orders table must be ready by 8am for the morning dashboard.' Airflow SLA miss alerts enforce this. Late data may be excluded from reports." },
      { label: "Enforcement tools", text: "dbt tests (not_null, unique, accepted_values, relationships) run after every transformation. Great Expectations for custom expectations. Pipeline-level checks: assert row_count > threshold before proceeding." }
    ],
    relatedIds: ["pipeline-phases", "incremental-loading", "dbt-tool"]
  }
];

const CORRECTIONS = [
  {
    id: "cache-correction",
    severity: "medium",
    title: "cache() default storage level depends on API",
    yourStatement: "cache is a certain type of persist used for MEMORY_AND_DISK storage",
    correction: "cache() behavior differs between RDD API and DataFrame API. For DataFrames/Datasets: cache() = MEMORY_AND_DISK (your statement is correct here). For RDDs: cache() = MEMORY_ONLY — data that doesn't fit in memory gets recomputed, NOT spilled to disk. You need persist(StorageLevel.MEMORY_AND_DISK) explicitly on RDDs to get disk fallback.",
    rule: "DataFrame cache() → MEMORY_AND_DISK | RDD cache() → MEMORY_ONLY",
    connectedTo: "spark-cache-persist"
  },
  {
    id: "dag-linearity",
    severity: "low",
    title: "DAGs don't have to be linear",
    yourStatement: "DAG must be linear not iterative",
    correction: "The constraint is ACYCLIC (no cycles, no loops back) — not linear. DAGs can and do have parallel branches, merge points, and diamond shapes. 'Linear' would mean only one path A→B→C→D. In practice most DAGs have parallel task groups that fan out then fan in. The 'A' in DAG means no loops, not no branches.",
    rule: "DAG rule = no cycles (acyclic), NOT no branches",
    connectedTo: "airflow"
  }
];

const GAPS = [
  {
    id: "scd-types",
    priority: "critical",
    title: "Slowly Changing Dimensions (SCD Types)",
    subtitle: "The missing piece of your dimensional modeling knowledge",
    connectsTo: ["fact-dimension", "star-snowflake", "incremental-loading"],
    connectionExplain: "You know star schema and surrogate keys deeply. SCD is the NEXT layer — it answers: what happens when a customer moves cities? Do you overwrite? Keep history? This is why surrogate keys exist in the first place.",
    whyItMatters: "Almost guaranteed interview question if you mention star schema. Every production DW handles dimension changes. You have 80% of the knowledge — SCD is the 20% that completes it.",
    lesson: {
      intro: "Dimensions change. A customer changes their address. A product gets recategorized. How you handle this defines whether your DW preserves history or loses it.",
      types: [
        {
          name: "SCD Type 1 — Overwrite",
          desc: "Simply update the dimension record. Old value is lost. Use when history doesn't matter (correcting a typo, fixing a data error).",
          example: "Customer name typo: 'Marwn' → 'Marwan'. Just update. No need to keep 'Marwn'."
        },
        {
          name: "SCD Type 2 — Add New Row (Most Important)",
          desc: "Keep the old record, insert a new one with the change. Mark old record inactive. Add effective_from and effective_to date columns. THIS is why surrogate keys exist — same natural key, multiple rows, different surrogate keys.",
          example: "Customer moves from Giza to Cairo. Old row: surrogate=1, customer_id=100, city=Giza, active=false, end_date=2025-01-01. New row: surrogate=2, customer_id=100, city=Cairo, active=true, end_date=9999-12-31. Old orders point to surrogate=1 (Giza), new orders point to surrogate=2 (Cairo). History preserved. Tip: end_date=9999-12-31 is the industry sentinel meaning 'currently active' — lets you filter current rows with WHERE end_date = '9999-12-31' instead of a boolean flag."
        },
        {
          name: "SCD Type 3 — Add Column",
          desc: "Add a 'previous_value' column. Keeps only one level of history. Rarely used — only when exactly one historical value matters.",
          example: "Add 'previous_city' column: city=Cairo, previous_city=Giza. Can't track if they moved 3 times."
        }
      ]
    }
  },
  {
    id: "dbt-tool",
    priority: "critical",
    title: "dbt (data build tool)",
    subtitle: "Industry-standard transformation layer — almost every modern DE job uses it",
    connectsTo: ["etl-elt", "pipeline-phases", "star-snowflake"],
    connectionExplain: "You know ELT and pipeline transformation phases. dbt IS the tool that implements the transform step in ELT. It's what replaced raw SQL scripts and DataStage jobs for most companies.",
    whyItMatters: "Listed in ~60% of data engineer job postings. If you use Snowflake, BigQuery, Redshift, or dbt Cloud in an interview — you need to know dbt. It's the transformation standard.",
    lesson: {
      intro: "dbt handles the T in ELT. You write SELECT statements. dbt handles running them in the right order, testing them, documenting them, and deploying them.",
      types: [
        {
          name: "Models",
          desc: "A model is just a SELECT statement in a .sql file. dbt compiles it into a CREATE TABLE or CREATE VIEW. You define transformations as SELECT — dbt handles the DDL."
        },
        {
          name: "DAG (lineage)",
          desc: "dbt builds a dependency DAG from your models using ref() function. ref('customers') in model B means B depends on A. dbt ensures correct execution order."
        },
        {
          name: "Tests",
          desc: "Built-in data quality tests: not_null, unique, accepted_values, relationships (foreign key integrity). Run after every build. Catches bad data before it reaches analysts."
        },
        {
          name: "Materializations",
          desc: "How your model is stored: table (rebuild every run), view (just a SQL view), incremental (only process new rows — connects to your incremental loading knowledge), ephemeral (CTE, never stored)."
        }
      ]
    }
  },
  {
    id: "advanced-sql",
    priority: "high",
    title: "Advanced SQL — Window Functions & CTEs",
    subtitle: "Interview staple — will definitely be tested",
    connectsTo: ["oltp-olap", "fact-dimension"],
    connectionExplain: "You use SQL for data retrieval and transformations. Window functions are how you answer analytical questions on top of your fact/dimension tables without losing row-level detail.",
    whyItMatters: "Window functions appear in nearly every senior SQL interview. CTEs are expected in any data transformation code. You likely use these without knowing the names.",
    lesson: {
      intro: "Window functions perform calculations across a set of rows related to the current row — without collapsing them into one (unlike GROUP BY).",
      types: [
        {
          name: "ROW_NUMBER() / RANK() / DENSE_RANK()",
          desc: "Assign row numbers within a partition. Classic use: get the latest record per customer.",
          example: "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) — ranks rows per customer newest-first. WHERE row_num = 1 gets latest."
        },
        {
          name: "LAG() / LEAD()",
          desc: "Access previous or next row's value without a self-join. Perfect for comparing period-over-period.",
          example: "LAG(revenue, 1) OVER (ORDER BY month) — gets previous month's revenue. revenue - LAG(revenue,1) = month-over-month change."
        },
        {
          name: "SUM() / AVG() OVER ()",
          desc: "Running totals and moving averages — the most common window aggregate.",
          example: "SUM(revenue) OVER (PARTITION BY product ORDER BY date ROWS UNBOUNDED PRECEDING) = running total per product."
        },
        {
          name: "CTEs (WITH clause)",
          desc: "Named temporary result sets that make complex queries readable. Also enables recursive queries. dbt models use CTEs as their standard structure.",
          example: "WITH cleaned AS (SELECT ... FROM raw WHERE ...), aggregated AS (SELECT ... FROM cleaned GROUP BY ...) SELECT * FROM aggregated;"
        }
      ]
    }
  },
  {
    id: "cloud-platforms",
    priority: "high",
    title: "Cloud Data Warehouses",
    subtitle: "BigQuery / Snowflake / Redshift — most jobs require at least one",
    connectsTo: ["dw-lake-lakehouse", "etl-elt", "oltp-olap"],
    connectionExplain: "You know what a data warehouse is conceptually and have used Teradata (on-prem). Cloud DWs are the same concept but fully managed, serverless, and with near-infinite scale.",
    whyItMatters: "90% of new DE jobs are cloud-first. Even if your current role is on-prem, interviewers will ask. BigQuery (GCP) and Snowflake are the most commonly mentioned in job postings.",
    lesson: {
      intro: "Cloud DWs are managed OLAP databases. No server management. Pay per query or per storage. Scale automatically. The main three:",
      types: [
        {
          name: "Google BigQuery",
          desc: "Serverless. Pay per TB scanned. Columnar storage. Separates compute from storage. Great for ELT — raw data in GCS, transform with SQL in BigQuery. Native integration with GCP ecosystem."
        },
        {
          name: "Snowflake",
          desc: "Separate storage (S3/GCS/Azure) and compute (virtual warehouses). Multi-cloud. Industry favorite for its simplicity. Strong dbt integration. Time Travel feature (query data as it was at any point in past 90 days)."
        },
        {
          name: "Amazon Redshift",
          desc: "Columnar MPP warehouse on AWS. Strong ecosystem integration (S3, Glue, Lambda). Redshift Spectrum queries S3 directly — lakehouse pattern."
        },
        {
          name: "Key concepts across all",
          desc: "Columnar storage (reads only needed columns — faster aggregations), MPP (massively parallel processing), separation of storage and compute, auto-scaling, SQL interface."
        }
      ]
    }
  },
  {
    id: "wide-narrow-transforms",
    priority: "medium",
    title: "Wide vs Narrow Transformations in Spark",
    subtitle: "The foundation of understanding Spark performance",
    connectsTo: ["spark-shuffle", "spark-rdd-partitions", "spark-lazy-eval"],
    connectionExplain: "You know shuffling happens. This fills in WHY — some transformations shuffle and some don't, and this distinction determines your job's performance profile.",
    whyItMatters: "Understanding this distinction lets you write faster Spark code and explain performance decisions in interviews.",
    lesson: {
      intro: "Every Spark transformation is either narrow (no shuffle) or wide (causes shuffle). This determines whether data stays on its executor or moves across the network.",
      types: [
        {
          name: "Narrow Transformations",
          desc: "Each output partition depends on exactly one input partition. No data movement between executors. Fast — processed in-place.",
          example: "map(), filter(), select(), withColumn(), flatMap(), union() — all narrow."
        },
        {
          name: "Wide Transformations",
          desc: "Output partitions may depend on data from multiple input partitions. Requires shuffle — data moves across network. Marks a stage boundary in the execution plan.",
          example: "groupBy(), join(), distinct(), repartition(), orderBy(), reduceByKey() — all wide. Each causes a new stage."
        },
        {
          name: "Stages",
          desc: "Spark splits execution into stages at shuffle boundaries. Each stage is a set of narrow transformations that can run without data movement. A job with 3 wide transformations has at least 4 stages."
        }
      ]
    }
  },
  {
    id: "medallion-arch",
    priority: "medium",
    title: "Medallion Architecture (Bronze / Silver / Gold)",
    subtitle: "Modern naming for your Stage → Transform → Core mental model",
    connectsTo: ["pipeline-phases", "dw-lake-lakehouse", "etl-elt"],
    connectionExplain: "You already know Stage → Transform → Core. Medallion is the same concept with different names, applied to Lakehouses. Learning this lets you communicate your existing knowledge in modern terminology.",
    whyItMatters: "Delta Lake, Databricks, and most modern data stack documentation uses Bronze/Silver/Gold. Interviewers use this vocabulary. You already know the concept — just learn the naming.",
    lesson: {
      intro: "Medallion is a data lakehouse design pattern with three layers — a direct mapping to what you already know.",
      types: [
        {
          name: "Bronze = Stage",
          desc: "Raw data exactly as it arrived from source. No transformations. Append-only. Preserves full history. All formats welcome (JSON, CSV, Avro, Parquet)."
        },
        {
          name: "Silver = Transform",
          desc: "Cleaned, filtered, validated data. Deduplicated. Standardized schemas. Joined across domains. Business rules applied. Still fairly raw — analysts don't query here."
        },
        {
          name: "Gold = Core",
          desc: "Business-ready, aggregated, analytics-optimized tables. Star schema lives here. This is what BI tools and analysts query. High quality, trusted data."
        },
        {
          name: "Why it matters in Lakehouses",
          desc: "In a lakehouse (Delta Lake/Iceberg), all three layers are stored in object storage (S3/GCS) as Parquet files with ACID guarantees. No separate databases — just different folders/tables with different quality levels."
        }
      ]
    }
  },
  {
    id: "delta-iceberg",
    priority: "medium",
    title: "Delta Lake & Apache Iceberg",
    subtitle: "Table formats that make data lakes behave like databases",
    connectsTo: ["dw-lake-lakehouse", "spark-architecture", "medallion-arch"],
    connectionExplain: "You know what a lakehouse is. Delta Lake and Iceberg are the actual technology that MAKES a data lake into a lakehouse — they add ACID, schema evolution, and time travel on top of Parquet files.",
    whyItMatters: "Every modern data stack uses one of these. Databricks uses Delta Lake (they created it). Snowflake external tables, AWS Glue, and many others support Iceberg. Coming up fast in DE interviews.",
    lesson: {
      intro: "Plain Parquet files on S3 don't support ACID transactions, updates, or deletes. Delta Lake and Iceberg add a transaction log on top of Parquet to enable all of this.",
      types: [
        {
          name: "Delta Lake",
          desc: "Open-source table format by Databricks. Adds transaction log (_delta_log) to Parquet. Supports ACID, updates/deletes (MERGE), time travel (query data at any past version), schema evolution."
        },
        {
          name: "Apache Iceberg",
          desc: "Table format by Netflix. Similar capabilities to Delta Lake. More open/vendor-neutral. Hidden partitioning (add/change partition strategy without rewriting data). Strong community adoption."
        },
        {
          name: "Time Travel",
          desc: "Query data as it was at any previous point: SELECT * FROM table VERSION AS OF 10 or TIMESTAMP AS OF '2024-01-01'. Critical for debugging pipeline issues and auditing."
        }
      ]
    }
  },
  {
    id: "streaming-kafka",
    priority: "low",
    title: "Streaming Data — Kafka Basics",
    subtitle: "Real-time data pipelines — the step beyond batch",
    connectsTo: ["pipeline-phases", "spark-architecture"],
    connectionExplain: "Everything you know is batch (process data at scheduled intervals). Streaming is continuous processing. Spark Streaming and Kafka are how batch DE skills extend to real-time.",
    whyItMatters: "Not required for most junior roles, but knowing the concept puts you ahead. Senior DE roles increasingly require streaming knowledge.",
    lesson: {
      intro: "Kafka is a distributed message streaming platform. Think of it as a super-fast, durable, scalable message queue that retains messages so consumers can replay them.",
      types: [
        {
          name: "Core concepts",
          desc: "Topics (named streams of messages), Producers (publish to topics), Consumers (read from topics), Consumer Groups (parallel consumption), Partitions (parallelism unit), Offsets (position tracking)."
        },
        {
          name: "Spark Structured Streaming",
          desc: "Spark's streaming API — treats a real-time stream as an infinite DataFrame. Same transformations you know, just on continuously arriving data. Reads from Kafka topics."
        },
        {
          name: "Batch vs Stream",
          desc: "Batch: run pipeline every hour, process last hour's data. Stream: process each event within seconds of arrival. Latency vs complexity tradeoff."
        }
      ]
    }
  },
  {
    id: "merge-upsert",
    priority: "critical",
    title: "MERGE / UPSERT — SQL Incremental Pattern",
    subtitle: "The SQL statement that powers every incremental pipeline",
    connectsTo: ["incremental-loading", "scd-types", "idempotency"],
    connectionExplain: "You know incremental loading needs to handle existing records. MERGE is HOW you implement it in SQL — match on key, update if exists, insert if not. It's also the SQL behind SCD Type 1 and the idempotent insert pattern.",
    whyItMatters: "Asked in nearly every DE interview that touches SQL. 'How do you handle records that already exist in your target table?' The answer is MERGE. Also called UPSERT. PostgreSQL uses INSERT ... ON CONFLICT.",
    lesson: {
      intro: "MERGE (or UPSERT) solves the 'what if the row already exists?' problem in one atomic SQL statement.",
      types: [
        {
          name: "Standard MERGE syntax",
          desc: "MERGE INTO target USING source ON (target.id = source.id) WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT (...) VALUES (...). Supported by: SQL Server, Oracle, BigQuery, Snowflake, Delta Lake.",
          example: "MERGE INTO customers t USING staging_customers s ON t.customer_id = s.customer_id WHEN MATCHED THEN UPDATE SET t.city = s.city, t.updated_at = s.updated_at WHEN NOT MATCHED THEN INSERT (customer_id, name, city) VALUES (s.customer_id, s.name, s.city);"
        },
        {
          name: "PostgreSQL: INSERT ... ON CONFLICT",
          desc: "PostgreSQL uses different syntax. ON CONFLICT (key_column) DO UPDATE SET col = EXCLUDED.col. EXCLUDED refers to the row that was rejected due to conflict.",
          example: "INSERT INTO customers (customer_id, name, city) VALUES (%s, %s, %s) ON CONFLICT (customer_id) DO UPDATE SET city = EXCLUDED.city, updated_at = NOW();"
        },
        {
          name: "WHEN NOT MATCHED BY SOURCE — DELETE",
          desc: "Optional third clause: handle rows in target that no longer exist in source. Used for hard deletes — when a record disappears from source, remove it from target too."
        },
        {
          name: "Why it's idempotent",
          desc: "Run MERGE twice with same source data: first run inserts/updates correctly. Second run: MATCHED rows get re-updated with same values (no change), NOT MATCHED rows already exist so they match. Result = identical table state."
        }
      ]
    }
  },
  {
    id: "data-skew",
    priority: "high",
    title: "Data Skew in Spark",
    subtitle: "When one partition ruins your entire job's performance",
    connectsTo: ["spark-shuffle", "spark-rdd-partitions", "wide-narrow-transforms"],
    connectionExplain: "You know shuffle redistributes data. Skew is what happens when that redistribution is uneven — one executor gets 10× more data than others. It's the most common cause of slow Spark jobs after shuffle.",
    whyItMatters: "'Your Spark job has 200 tasks. 199 finish in 2 minutes but one takes 20 minutes — what's wrong?' is a classic interview question. Answer: data skew. Knowing the causes and fixes is what separates theory from practice.",
    lesson: {
      intro: "Data skew = uneven data distribution across partitions. One partition has millions of rows, others have thousands. The executor handling the large partition becomes a bottleneck (straggler task).",
      types: [
        {
          name: "Cause: skewed join key",
          desc: "Joining on a column where one value dominates (e.g. customer_id where 80% of orders belong to one B2B customer). All rows with that key go to the same partition → one executor does 80% of the work.",
          example: "If customer_id = 'AMAZON_WHOLESALE' represents 5M of your 6M rows, the partition for that key will have 5M rows while others average 5,000."
        },
        {
          name: "Fix: Broadcast join",
          desc: "If one table is small (< a few hundred MB), broadcast it to all executors. No shuffle needed — each executor has the full small table locally. No skew possible. from pyspark.sql.functions import broadcast — df_large.join(broadcast(df_small), 'key')."
        },
        {
          name: "Fix: Salting",
          desc: "Add a random 'salt' value (0–N) to the join key to distribute skewed values across N partitions. Distribute the small table N times (one copy per salt value). Join on original_key + salt. Remove salt after join.",
          example: "Skewed key 'AMAZON': becomes 'AMAZON_0', 'AMAZON_1', ..., 'AMAZON_9' across 10 partitions instead of all in one."
        },
        {
          name: "Fix: repartition()",
          desc: "df.repartition(n, 'join_key') forces even redistribution before the join. Costs a shuffle upfront but may be worth it if the subsequent join would be severely skewed."
        }
      ]
    }
  },
  {
    id: "normalization",
    priority: "medium",
    title: "Database Normalization",
    subtitle: "Why OLTP is normalized and OLAP is deliberately NOT",
    connectsTo: ["oltp-olap", "star-snowflake", "fact-dimension"],
    connectionExplain: "Normalization is the theory behind OLTP design. Star schema deliberately violates it for performance. Understanding the rule you're breaking (and why) is what distinguishes a DE who knows dimensional modeling from one who just memorized the shapes.",
    whyItMatters: "Interviewers occasionally ask 'what is 3NF?' or 'why don't you normalize your warehouse?' Knowing normalization forms gives you a complete answer instead of just 'star schema is faster'.",
    lesson: {
      intro: "Normalization is the process of organizing a database to reduce redundancy and dependency. The goal: each fact stored once, in one place.",
      types: [
        {
          name: "1NF — First Normal Form",
          desc: "Each column holds atomic (indivisible) values. No repeating groups or arrays in a cell. Each row is unique (has a primary key).",
          example: "Bad: phone_numbers column = '01012345, 01098765'. Good: separate rows for each phone number."
        },
        {
          name: "2NF — Second Normal Form",
          desc: "1NF + no partial dependencies. Every non-key column depends on the WHOLE primary key (not just part of it). Applies when composite primary key exists.",
          example: "Order_Items table with PK=(order_id, product_id). customer_name depends only on order_id — partial dependency. Move customer_name to Orders table."
        },
        {
          name: "3NF — Third Normal Form",
          desc: "2NF + no transitive dependencies. Non-key columns must depend on the key directly, not on other non-key columns.",
          example: "If employee table has: emp_id, dept_id, dept_name — dept_name depends on dept_id (not emp_id). Move dept_name to Departments table."
        },
        {
          name: "Why warehouses denormalize",
          desc: "Joins are expensive at scale. Star schema deliberately violates 3NF by keeping city, region, country in one dimension table (instead of separate normalized tables). The tradeoff: faster queries at the cost of some redundancy. Storage is cheap; query time is not."
        }
      ]
    }
  },
  {
    id: "airflow-scheduling",
    priority: "high",
    title: "Airflow Scheduling & Backfill",
    subtitle: "How Airflow triggers DAGs and handles historical runs",
    connectsTo: ["airflow", "incremental-loading", "idempotency"],
    connectionExplain: "You know Airflow orchestrates pipelines. This covers HOW it decides when to run them, how execution dates work (often confusing), and how to safely re-run historical data — which requires idempotent pipelines.",
    whyItMatters: "Practical Airflow questions like 'how do you schedule a daily pipeline?' or 'how would you re-process last month's data?' come up in almost every interview where Airflow is listed.",
    lesson: {
      intro: "Airflow scheduling has some unintuitive behavior around execution dates. Understanding it prevents bugs in production pipelines.",
      types: [
        {
          name: "Cron expressions",
          desc: "Schedule uses cron: '0 8 * * *' = 8am daily. '@daily' = midnight daily. '@hourly' = every hour. '@weekly' = Sunday midnight. Five fields: minute hour day_of_month month day_of_week.",
          example: "'30 6 * * 1-5' = 6:30am Monday–Friday only. '0 */4 * * *' = every 4 hours."
        },
        {
          name: "execution_date (data_interval_start)",
          desc: "Confusing Airflow behavior: a DAG with schedule='@daily' and start_date=2024-01-01 has its FIRST run at 2024-01-02, with execution_date=2024-01-01. Airflow runs AFTER the interval closes. execution_date = start of the data interval being processed, not the time of actual execution.",
          example: "Daily DAG triggered at midnight Jan 2 processes Jan 1's data. execution_date = Jan 1. This is intentional — you process yesterday's complete data."
        },
        {
          name: "catchup=False",
          desc: "If your DAG's start_date is 6 months ago and catchup=True (default), Airflow immediately queues 180+ runs. Set catchup=False to only run for current period. Critical setting to configure before deploying a new DAG."
        },
        {
          name: "Backfill",
          desc: "Deliberately re-process historical data: airflow dags backfill -s 2024-01-01 -e 2024-01-31 my_dag. Creates DAG runs for each day in the range. Requires idempotent pipelines — otherwise re-running doubles your data.",
          example: "Business logic changed: re-process all of January with new logic. Run backfill for Jan 1–31. Each run processes that day's data freshly."
        }
      ]
    }
  }
];

const CONNECTIONS = [
  {
    from: "incremental-loading",
    to: "scd-types",
    fromLabel: "Incremental Loading",
    toLabel: "SCD Type 2",
    relation: "Incremental loading prevents DUPLICATE rows. SCD Type 2 deliberately ADDS new rows when data changes. Together they define exactly how a dimension table grows over time: add new rows on change (SCD2), deduplicate on re-run (incremental guard)."
  },
  {
    from: "spark-rdd-partitions",
    to: "spark-lazy-eval",
    fromLabel: "RDD Immutability",
    toLabel: "Lazy Evaluation",
    relation: "Immutability ENABLES lazy evaluation. Because an RDD can't be modified, Spark can safely defer execution — the source data will be the same when execution finally happens. Mutable data + lazy eval = undefined behavior."
  },
  {
    from: "star-snowflake",
    to: "fact-dimension",
    fromLabel: "Star Schema",
    toLabel: "Fact & Dimension Tables",
    relation: "Star schema IS the physical implementation of the fact/dimension conceptual model. You can't understand star schema without fact/dimension. You can't apply fact/dimension without choosing a schema. They're the same idea at different levels of abstraction."
  },
  {
    from: "pipeline-phases",
    to: "etl-elt",
    fromLabel: "Stage → Transform → Core",
    toLabel: "ETL / ELT",
    relation: "Pipeline phases describe WHAT layers exist. ETL/ELT describes WHERE transformation happens. In ETL: transform before hitting Stage. In ELT (modern): Stage = raw load, Transform layer = in-warehouse SQL, Core = output. Your Stage→Transform→Core IS ELT architecture."
  },
  {
    from: "spark-shuffle",
    to: "wide-narrow-transforms",
    fromLabel: "Shuffling",
    toLabel: "Wide Transformations",
    relation: "Shuffle is not a random event — it's caused specifically by wide transformations. groupBy, join, distinct trigger shuffle because output partitions need data from multiple input partitions. Narrow transforms (filter, select) never shuffle. This is the root cause/effect relationship."
  },
  {
    from: "airflow",
    to: "pipeline-phases",
    fromLabel: "Apache Airflow DAG",
    toLabel: "Pipeline Phases",
    relation: "Your pipeline phases (Stage→Transform→Core) define the logical data flow. An Airflow DAG defines the execution schedule and dependency order of those phases as tasks. The DAG IS the operational wrapper around your pipeline architecture."
  },
  {
    from: "fact-dimension",
    to: "scd-types",
    fromLabel: "Surrogate Keys",
    toLabel: "SCD Type 2",
    relation: "Surrogate keys exist primarily FOR SCD Type 2. When a dimension changes (customer moves city), you add a new row with a new surrogate key. Old fact rows keep the old surrogate key (pointing to old city). New fact rows get the new surrogate key. Without surrogate keys, you can't track multiple versions of the same natural entity."
  },
  {
    from: "dw-lake-lakehouse",
    to: "medallion-arch",
    fromLabel: "Lakehouse",
    toLabel: "Medallion Architecture",
    relation: "Medallion Architecture is the design pattern INSIDE a Lakehouse. The Lakehouse is the storage paradigm (cheap lake storage + DW features). Medallion defines how you organize data within it: Bronze (raw) → Silver (clean) → Gold (analytics-ready) — your Stage→Transform→Core in lakehouse vocabulary."
  },
  {
    from: "spark-lazy-eval",
    to: "oltp-olap",
    fromLabel: "Lazy Evaluation + Catalyst",
    toLabel: "OLAP Query Optimizer",
    relation: "Both Spark's Catalyst optimizer (triggered by lazy eval) and OLAP database query planners do the same thing: receive your logical query, build an optimized physical execution plan before running it. SQL EXPLAIN PLAN = Spark DAG visualization. Same idea, different engines."
  },
  {
    from: "etl-elt",
    to: "dbt-tool",
    fromLabel: "ELT Pattern",
    toLabel: "dbt",
    relation: "dbt is the tool that implements the T in ELT. You load raw data into your warehouse (EL), then use dbt SELECT models to transform it in-place (T). dbt made ELT practical and scalable by adding dependency management, testing, and documentation to what was previously raw SQL scripts."
  },
  {
    from: "parquet-format",
    to: "spark-shuffle",
    fromLabel: "Parquet Columnar Storage",
    toLabel: "Spark Shuffle",
    relation: "Parquet's columnar format and Spark's shuffle both exist to minimize I/O. Parquet reduces reads by skipping irrelevant columns. Shuffle minimization reduces network I/O. When Spark writes shuffle output, it writes Parquet files. Understanding both means understanding WHY your Spark job is fast or slow at the storage layer."
  },
  {
    from: "idempotency",
    to: "incremental-loading",
    fromLabel: "Idempotency",
    toLabel: "Incremental Loading",
    relation: "Incremental loading without idempotency = double-counting on re-runs. Idempotency is the property that MAKES incremental pipelines safe to retry. Every robust incremental pipeline is idempotent by design — either via MERGE (match existing records) or truncate-insert (delete then re-insert the exact same window)."
  },
  {
    from: "normalization",
    to: "star-snowflake",
    fromLabel: "3NF Normalization",
    toLabel: "Star Schema (Denormalized)",
    relation: "Star schema is a deliberate violation of 3NF. Normalization says: no transitive dependencies — split city/region/country into separate tables. Star schema says: keep them all in one dimension for query performance. Knowing the rule you're breaking (and why) is what separates architects from implementers."
  },
  {
    from: "data-quality",
    to: "pipeline-phases",
    fromLabel: "Data Quality Checks",
    toLabel: "Pipeline Phases",
    relation: "Quality checks map to pipeline phases: Bronze/Stage = completeness (did all records arrive?). Silver/Transform = accuracy and consistency (are values valid? duplicates removed?). Gold/Core = uniqueness and timeliness (no duplicate facts, SLA met). Quality enforcement belongs at each layer, not just at the end."
  }
];

const INTERVIEW_READINESS = {
  overall: 72,
  level: "Junior-Ready (Solid Foundation)",
  summary: "Strong conceptual foundation across core DE topics. Real project portfolio backs up the theory. Main gaps are tooling (dbt, cloud platforms) that most junior roles require. Not yet interview-ready for cloud-native roles but strong for on-prem/hybrid positions.",
  categories: [
    { name: "Data Warehouse Concepts", score: 87, status: "strong", note: "OLTP/OLAP, DW/Lake/Lakehouse — solid. Missing SCD types to complete dimensional modeling." },
    { name: "Dimensional Modeling", score: 78, status: "good", note: "Star schema, fact/dim, surrogate keys — good. SCD types (esp. Type 2) is the gap." },
    { name: "Pipeline Design", score: 80, status: "good", note: "Phases, incremental loading, ETL/ELT — well understood and backed by real project." },
    { name: "Apache Airflow", score: 68, status: "decent", note: "Orchestration concept, DAGs, basics — solid. Gaps: XComs, Sensors, templating, SubDAGs, backfill strategies." },
    { name: "Apache Spark", score: 74, status: "good", note: "Architecture, RDDs, lazy eval, cache/persist — good. Missing: wide/narrow transforms, broadcast joins, Spark UI." },
    { name: "SQL (Intermediate+)", score: 63, status: "needs-work", note: "Clearly used in projects. Window functions, CTEs, and query optimization likely undertested in interview context." },
    { name: "Cloud Platforms", score: 18, status: "critical-gap", note: "No cloud DW experience. BigQuery/Snowflake/Redshift appear in majority of job postings. Biggest blocker." },
    { name: "dbt", score: 10, status: "critical-gap", note: "Not mentioned anywhere. Industry-standard transformation tool. Most modern roles require it." },
    { name: "Streaming / Real-time", score: 15, status: "gap", note: "Not required for most junior roles but basic Kafka knowledge helps in interviews." },
    { name: "Data Quality & Testing", score: 32, status: "gap", note: "Basic pipeline hygiene from projects. No formal testing framework knowledge (Great Expectations, dbt tests)." }
  ],
  weakPoints: [
    { rank: 1, topic: "Cloud Platforms", impact: "High", detail: "Appears in 70%+ of DE job postings. BigQuery or Snowflake experience is often a hard requirement. Even basic free-tier hands-on projects would significantly improve your candidacy." },
    { rank: 2, topic: "dbt", impact: "High", detail: "The standard transformation tool in modern data stacks. Interviewers at companies using dbt will test it. 1 week of dbt fundamentals fills this gap." },
    { rank: 3, topic: "SCD Types", impact: "Medium-High", detail: "If an interviewer asks 'you know star schema — how do you handle dimension changes?', you need SCD. It's the natural next question after any dimensional modeling discussion." },
    { rank: 4, topic: "Advanced SQL", impact: "Medium", detail: "Window functions (ROW_NUMBER, LAG, RANK, running totals) are standard in DE SQL interviews. You likely have the underlying SQL ability — just practice these patterns." },
    { rank: 5, topic: "Spark Internals Depth", impact: "Medium", detail: "You know the concepts but wide vs narrow transformations, broadcast joins, and reading the Spark UI are expected at most companies using Spark." }
  ]
};

// globals available to app.js
