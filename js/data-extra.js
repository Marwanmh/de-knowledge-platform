// ---- ELI5 ----
const ELI5 = {
  'oltp-olap': 'OLTP is like a restaurant waiter — handles one order at a time, super fast. OLAP is like the manager reviewing all orders from last month to see which dish sold best. One is built for speed per transaction, the other for seeing the big picture.',
  'fact-dimension': 'Facts are the numbers on your receipt (amount paid, quantity). Dimensions are the descriptions (what you bought, where, from which store). Surrogate key is like a receipt number that ties everything together.',
  'star-snowflake': 'Star schema: imagine a wheel. Fact table is the hub, dimensions are the spokes. Everything connects directly to the center. Snowflake schema: same wheel but the spokes have mini-spokes attached. More organized on paper, slower in practice.',
  'pipeline-phases': 'Stage = take a photo of the raw data. Transform = edit that photo (crop, color-correct, fix mistakes). Core = the final polished photo, ready to hang on the wall for everyone to see.',
  'incremental-loading': 'First time you move into a house, you bring all your furniture (full load). Every day after, you only bring new packages that arrived (incremental). You never re-move ALL furniture every single day.',
  'etl-elt': 'ETL: Take data → clean it → put it in warehouse. ELT: Take data → dump it in warehouse raw → clean it there. Same steps, different order. Modern cloud warehouses are powerful enough to do the cleaning themselves.',
  'dw-lake-lakehouse': 'Warehouse = organized filing cabinet (only clean, labeled folders). Lake = entire floor covered in papers, photos, videos (everything, messy). Lakehouse = organized filing cabinet that also has a system to find the messy pile — best of both.',
  'airflow': 'Airflow is like a smart alarm clock for your data pipeline. It wakes up your tasks at the right time, in the right order, retries if something fails, and sends you a message if it can\'t fix itself.',
  'spark-architecture': 'Think of a company: Cluster Manager = CEO (controls all resources). Driver = Project Manager (plans and distributes the work). Executors = Workers on the floor (actually process the data). Each worker handles their own piece.',
  'spark-rdd-partitions': 'Split a 1 million row dataset into 100 pieces (partitions). Give each piece to a different worker. All 100 work at the same time instead of one worker doing everything. Immutability means if a worker loses their piece, you recreate it from the instructions, not from nothing.',
  'spark-shuffle': 'Imagine 10 workers each sorting their own pile of letters by name. Shuffle is when you ask them to re-sort by city — now they all have to swap letters with each other. Expensive because data travels across the network.',
  'spark-cache-persist': 'You squeeze 1kg of oranges into juice (expensive computation). Cache = put the juice in the fridge. Next time you want juice, just grab it — don\'t squeeze again. Without cache, Spark squeezes fresh oranges every single time.',
  'spark-lazy-eval': 'You tell a chef: peel potatoes, dice them, then fry. Lazy eval means the chef doesn\'t start immediately — they think: "can I peel AND dice at the same time? Can I skip the fry if you only wanted raw?" Then they execute optimally. No lazy eval = start peeling immediately, no optimization possible.',
  'sql-joins': 'Tables are like two guest lists. INNER JOIN = people on BOTH lists. LEFT JOIN = everyone on list A, plus matches from list B (blanks if no match). FULL OUTER = everyone from both lists regardless. CROSS JOIN = every person from list A paired with every person from list B — usually an accident.',
  'parquet-format': 'Imagine a spreadsheet. CSV reads left to right row by row. Parquet reads top to bottom column by column. If you only need 3 columns out of 100, Parquet skips the other 97 entirely. Like reading only the "Name" column of a phonebook instead of every line.',
  'idempotency': 'A light switch is idempotent. Flipping it ON twice = same result as flipping it ON once. A non-idempotent pipeline is like a cash register that rings up your order again every time you press "confirm" — run it twice and you paid double.',
  'acid-transactions': 'ACID is a promise your database makes. Atomic: your transaction either fully happens or nothing happens — no half-finished orders. Consistent: database rules always enforced. Isolated: two people booking the last seat at the same time get a fair result. Durable: once confirmed, it\'s saved forever even if power cuts out.',
  'storage-partitioning': 'Imagine a library with 10 years of newspapers. If they\'re all in one pile, finding January 2024 = search everything. If organized in folders by Year/Month/Day, you go straight to the right shelf. Storage partitioning is the folder organization for your data files.',
  'docker-de': 'Docker is like a lunch box. You pack everything needed for a meal — food, utensils, napkin. The meal tastes identical whether eaten at home, office, or on a train. Docker packs your code + exact software versions so it behaves identically on your laptop and on the production server.',
  'data-quality': 'Data quality is like food safety. Completeness = did all the ingredients arrive? Accuracy = is the chicken actually chicken, not expired? Consistency = is it measured in grams everywhere, not grams in some places and ounces in others? Uniqueness = only one chicken, not five identical chickens accidentally ordered. You check quality before cooking, not after serving.'
};

// ---- FLASHCARDS ----
const FLASHCARDS = [
  {
    topicId: 'oltp-olap',
    cards: [
      { front: 'What does OLTP stand for and what is it used for?', back: 'Online Transaction Processing. Powers production apps (banking, e-commerce). Optimized for fast single-row reads and writes.' },
      { front: 'What does OLAP stand for and what is it used for?', back: 'Online Analytical Processing. Powers analytics and reporting. Optimized for complex queries scanning millions of rows.' },
      { front: 'Why must OLTP and OLAP systems always be separated?', back: 'OLAP queries are resource-heavy and slow. Running them on OLTP would degrade production app performance and risk data integrity for live users.' }
    ]
  },
  {
    topicId: 'fact-dimension',
    cards: [
      { front: 'What type of data does a fact table hold?', back: 'Numeric, measurable business events. Examples: quantity sold, revenue, number of clicks, duration. Each row = one business event.' },
      { front: 'What is a surrogate key?', back: 'A system-generated integer primary key on dimension tables (not from source data). More stable than natural keys which can change. Used to join fact tables to dimensions.' },
      { front: 'Why build dimension tables before fact tables?', back: 'Fact tables reference dimension surrogate keys as foreign keys. You need the keys to exist before you can reference them in the fact table.' },
      { front: 'What does a dimension table contain?', back: 'Descriptive context about facts — WHO, WHAT, WHERE, WHEN. Examples: customer name, product category, store city, date details.' }
    ]
  },
  {
    topicId: 'star-snowflake',
    cards: [
      { front: 'Describe the structure of a star schema.', back: 'One central fact table surrounded by denormalized dimension tables. All dimensions connect directly to the fact table via surrogate keys. Looks like a star.' },
      { front: 'What makes snowflake schema different from star schema?', back: 'Dimensions are normalized — split into sub-dimensions not directly connected to the fact. Creates chains of joins instead of single hops.' },
      { front: 'Why is star schema preferred (~90% of use cases)?', back: 'Fewer joins = faster queries. Simpler structure. Better BI tool compatibility. Storage is cheap — denormalization tradeoff is worth it.' }
    ]
  },
  {
    topicId: 'pipeline-phases',
    cards: [
      { front: 'Name the three pipeline layers and their purpose.', back: 'Stage: raw data, unchanged. Transform: clean, standardize, apply business rules. Core: analytics-ready, trusted data for BI tools and analysts.' },
      { front: 'Why keep raw data in staging unchanged?', back: 'Safety net. If downstream transformations break or logic changes, you still have the original data and can reprocess without re-extracting from source.' },
      { front: 'What happens in the transformation layer?', back: 'Data is cleaned, deduplicated, validated, standardized, and joined. Business rules applied. Bad data caught here before polluting the core layer.' }
    ]
  },
  {
    topicId: 'incremental-loading',
    cards: [
      { front: 'What is incremental loading?', back: 'Loading only new or changed records since the last successful run. Prevents duplication and avoids re-loading all data every time.' },
      { front: 'What is a common condition used in incremental loading?', back: 'Date watermark: WHERE updated_at > last_run_timestamp. Only records newer than the last run get loaded.' },
      { front: 'What problem does incremental loading prevent?', back: 'Duplicate rows from re-loading the same data, wasted compute from reprocessing unchanged records, and long pipeline runtimes.' }
    ]
  },
  {
    topicId: 'etl-elt',
    cards: [
      { front: 'What do the letters in ETL stand for?', back: 'Extract (pull from source), Transform (clean and reshape), Load (put into warehouse). Transform happens BEFORE loading.' },
      { front: 'How does ELT differ from ETL?', back: 'ELT = Extract, Load, Transform. Raw data lands in warehouse first, then transformed using warehouse compute. Transform happens AFTER loading.' },
      { front: 'When is ELT preferred over ETL?', back: 'When using powerful cloud data warehouses (BigQuery, Snowflake, Redshift) that have enough compute to transform data in-place. More flexible — raw data preserved for reprocessing.' }
    ]
  },
  {
    topicId: 'dw-lake-lakehouse',
    cards: [
      { front: 'What type of data does a data warehouse store?', back: 'Structured, processed data only. Schema enforced on write. SQL-queryable. High performance for analytics.' },
      { front: 'What type of data does a data lake store?', back: 'Anything — structured, semi-structured, unstructured (JSON, CSV, images, logs, video). No schema required. Cheap object storage.' },
      { front: 'What is a data lakehouse?', back: 'Combines data lake storage (cheap, flexible) with data warehouse features (ACID transactions, schema enforcement, fast SQL). Built on table formats like Delta Lake or Apache Iceberg.' }
    ]
  },
  {
    topicId: 'airflow',
    cards: [
      { front: 'What does Apache Airflow do?', back: 'Orchestrates pipelines — schedules when to run, defines task dependencies, retries on failure, sends alerts. Does not move data itself.' },
      { front: 'What is a DAG in Airflow?', back: 'Directed Acyclic Graph. A Python file defining the pipeline. Directed = tasks have dependency direction. Acyclic = no loops. Can have parallel branches — just no cycles.' },
      { front: 'What is an Operator in Airflow?', back: 'Defines what a task does. PythonOperator runs Python code. BashOperator runs shell commands. PostgresOperator runs SQL. Each task uses one operator.' }
    ]
  },
  {
    topicId: 'spark-architecture',
    cards: [
      { front: 'Name the three main components of Spark architecture.', back: 'Cluster Manager (resource allocation), Driver (plans and distributes tasks), Executors (run tasks on partitions). Worker nodes host the executors.' },
      { front: 'What does the Driver do in Spark?', back: 'Runs your application code, converts it to a DAG of tasks, optimizes the execution plan via Catalyst, distributes tasks to executors, collects results.' },
      { front: 'What is the difference between a Worker Node and an Executor?', back: 'Worker Node = physical/virtual machine. Executor = process running ON the worker. Multiple executors can run on one worker. Executor is what actually processes data.' }
    ]
  },
  {
    topicId: 'spark-rdd-partitions',
    cards: [
      { front: 'What does RDD stand for and what makes it resilient?', back: 'Resilient Distributed Dataset. Resilient because Spark tracks the lineage (how it was created) — if a partition is lost, it can be recomputed from parent RDDs.' },
      { front: 'Why are RDDs immutable?', back: 'Immutability enables fault recovery and safe lazy execution. Transformations always create new RDDs — the original is never changed so it can always serve as the source for recomputation.' },
      { front: 'How do partitions relate to parallelism?', back: 'Each partition is processed by one task on one executor core. More partitions = more parallel tasks (up to available cores). Too few partitions = underutilized cluster.' }
    ]
  },
  {
    topicId: 'spark-shuffle',
    cards: [
      { front: 'What is shuffling in Spark?', back: 'Redistribution of data across partitions — may involve moving data between executors over the network. Triggered by wide transformations.' },
      { front: 'Name 3 operations that trigger a shuffle.', back: 'groupBy(), join(), distinct(), repartition(), orderBy() — any wide transformation that requires data from multiple partitions to produce one output partition.' },
      { front: 'Why is shuffling the most expensive Spark operation?', back: 'Network I/O (data moves between executors), disk I/O (data spilled to disk between stages), and serialization/deserialization overhead. Minimize shuffles in hot paths.' }
    ]
  },
  {
    topicId: 'spark-cache-persist',
    cards: [
      { front: 'When should you use cache() or persist() in Spark?', back: 'When the same DataFrame/RDD is used multiple times in your code. Without it, Spark recomputes from scratch each time an action is called.' },
      { front: 'What is the default storage level of cache() for DataFrames?', back: 'MEMORY_AND_DISK — stores in memory, spills to disk if memory is full. For RDDs, cache() defaults to MEMORY_ONLY.' },
      { front: 'What happens with MEMORY_ONLY if data does not fit?', back: 'The data is NOT written to disk. Partitions that don\'t fit are simply recomputed from scratch when needed. No disk fallback.' }
    ]
  },
  {
    topicId: 'spark-lazy-eval',
    cards: [
      { front: 'What is lazy evaluation in Spark?', back: 'Transformations (filter, groupBy, select) are not executed immediately — Spark records them in an execution plan. Only when an action (collect, count, write) is called does execution happen.' },
      { front: 'Name 3 Spark actions that trigger execution.', back: 'collect(), count(), show(), write(), take(), first() — any operation that produces a result or side effect outside Spark.' },
      { front: 'What optimization advantage does lazy evaluation provide?', back: 'Catalyst Optimizer sees the entire transformation chain before executing — can reorder operations, push filters down, merge steps, eliminate redundant work. Not possible with eager execution.' }
    ]
  },
  {
    topicId: 'sql-joins',
    cards: [
      { front: 'What does an INNER JOIN return?', back: 'Only rows that have a matching value in BOTH tables. Rows with no match on either side are excluded.' },
      { front: 'What does a LEFT JOIN return?', back: 'ALL rows from the left table + matching rows from the right. Where no right-side match exists, right-side columns are NULL.' },
      { front: 'What is the common LEFT JOIN gotcha in WHERE clauses?', back: 'Filtering on a right-table column in WHERE (e.g. WHERE b.id IS NOT NULL) converts the LEFT JOIN into an INNER JOIN — you lose the "all left rows" guarantee. Filter condition should be in ON clause.' },
      { front: 'When would you use a FULL OUTER JOIN?', back: 'When you need all rows from both tables regardless of match. Unmatched rows from either side get NULLs on the opposite side. Useful for reconciliation between two sources.' }
    ]
  },
  {
    topicId: 'parquet-format',
    cards: [
      { front: 'Why is Parquet faster than CSV for analytical queries?', back: 'Columnar storage — reads only the columns needed. If querying 3 of 100 columns, Parquet reads ~3% of the data. CSV reads every column in every row regardless.' },
      { front: 'What is predicate pushdown in Parquet?', back: 'Parquet stores min/max statistics per column per row group. Query engines use these to skip entire row groups that cannot satisfy the WHERE filter — without reading the actual data.' },
      { front: 'Name 3 advantages of Parquet over CSV.', back: '1. Columnar reads (only needed columns). 2. Better compression (similar values in same column). 3. Schema embedded in file. 4. Predicate pushdown via statistics.' }
    ]
  },
  {
    topicId: 'idempotency',
    cards: [
      { front: 'What does idempotent mean in the context of a data pipeline?', back: 'Running the pipeline N times produces the same result as running it once. Safe to retry on failure without duplicating or corrupting data.' },
      { front: 'Name two patterns to make a pipeline idempotent.', back: '1. Truncate-insert: DELETE target rows for the processed window, then INSERT. 2. MERGE/UPSERT: match on key, update if exists, insert if not. Both are safe to re-run.' },
      { front: 'Why is idempotency critical in Airflow pipelines?', back: 'Airflow automatically retries failed tasks. If the pipeline isn\'t idempotent, each retry inserts duplicate rows. Production pipelines must be retry-safe.' }
    ]
  },
  {
    topicId: 'acid-transactions',
    cards: [
      { front: 'What does ACID stand for?', back: 'Atomicity (all-or-nothing), Consistency (constraints always enforced), Isolation (concurrent transactions don\'t interfere), Durability (committed data survives crashes).' },
      { front: 'Why do plain Parquet files on S3 lack ACID?', back: 'No transaction coordinator. Two writers can overwrite each other. A crash mid-write leaves partial files. Readers can see incomplete data. Delta Lake and Iceberg add ACID on top of Parquet.' },
      { front: 'What does Atomicity mean for a pipeline?', back: 'Either ALL records in a transaction are written or NONE are. If a pipeline crashes after inserting 600 of 1000 rows, the transaction rolls back — no partial state in the target table.' }
    ]
  },
  {
    topicId: 'storage-partitioning',
    cards: [
      { front: 'What is Hive-style storage partitioning?', back: 'Physically organizing data files into subfolders based on column values. Pattern: table/year=2024/month=01/day=15/file.parquet. Standard across all data lakes and lakehouses.' },
      { front: 'How does storage partitioning improve query performance?', back: 'Partition pruning — when you filter WHERE year=2024 AND month=01, Spark only reads the matching folders. Skips all other partitions entirely. Can reduce data scanned from 100% to <1%.' },
      { front: 'What is the small file problem in storage partitioning?', back: 'Over-partitioning creates thousands of tiny files. Each file carries metadata overhead. Spark must open/close thousands of files instead of a few large ones. Avoid high-cardinality partition columns (e.g. user_id).' }
    ]
  },
  {
    topicId: 'docker-de',
    cards: [
      { front: 'What is the difference between a Docker image and a container?', back: 'Image = blueprint (static, immutable, like a class). Container = running instance of an image (like an object). Multiple containers can run from the same image simultaneously.' },
      { front: 'Why do data engineers use Docker?', back: 'Reproducible environments — same Python version, same library versions everywhere. Run Airflow locally with docker-compose. Code that works in your container works identically in production.' },
      { front: 'What does docker-compose do for Airflow?', back: 'Starts all Airflow components (webserver, scheduler, worker, metadata DB, redis) with one command. Defines the multi-container setup in a YAML file. Official Airflow docker-compose.yaml is the standard local setup.' }
    ]
  },
  {
    topicId: 'data-quality',
    cards: [
      { front: 'Name the 5 dimensions of data quality.', back: 'Completeness (all records present, no NULLs), Accuracy (values reflect reality), Consistency (same entity represented same way), Uniqueness (no duplicates), Timeliness (data available when needed).' },
      { front: 'How do you enforce data quality in a dbt pipeline?', back: 'dbt built-in tests: not_null (no NULLs), unique (no duplicates), accepted_values (value in allowed list), relationships (foreign key integrity). Run automatically after every build.' },
      { front: 'What is a data quality check in a pipeline context?', back: 'Assertion that must pass before data proceeds to the next layer. Examples: row_count > 0 (completeness), null_rate < 1% on required fields (accuracy), max date within expected range (timeliness).' }
    ]
  }
];

// ---- INTERVIEW QUESTIONS ----
const INTERVIEW_QUESTIONS = [
  {
    topicId: 'oltp-olap',
    questions: [
      {
        q: 'Can you explain the difference between OLTP and OLAP?',
        a: 'OLTP (Online Transaction Processing) is designed for production applications — it handles fast, row-level reads and writes like processing a payment or updating a user record. OLAP (Online Analytical Processing) is designed for analytics — it handles complex queries that scan large amounts of data to produce aggregated insights. OLTP is optimized for write speed and data integrity; OLAP is optimized for read performance on large datasets.',
        difficulty: 'easy',
        tip: 'Always mention a real example for each. "OLTP is like a bank transaction system; OLAP is like the reporting system that shows the CFO how much was transacted last month."'
      },
      {
        q: 'Why should you never run heavy analytics directly on a production database?',
        a: 'Analytical queries are read-heavy and scan large amounts of data, consuming significant CPU and memory. Running them on a production OLTP database would slow down or block the transactions that real users depend on — like placing orders or making payments. It also risks data inconsistency if a long-running analytics query reads data mid-transaction.',
        difficulty: 'easy',
        tip: 'Mention the impact on end-users. Shows you think about the full system, not just the data side.'
      },
      {
        q: 'Give a real-world example of an OLTP system and an OLAP system.',
        a: 'OLTP: an e-commerce platform\'s orders database — handles thousands of INSERT/UPDATE operations per second as customers place orders. OLAP: a data warehouse used by the analytics team to run monthly revenue reports, customer segmentation queries, and product performance analysis.',
        difficulty: 'easy',
        tip: 'Use examples from your actual experience — Etisalat\'s Teradata system was OLAP, production application databases are OLTP.'
      }
    ]
  },
  {
    topicId: 'fact-dimension',
    questions: [
      {
        q: 'What is a fact table and what does it contain?',
        a: 'A fact table contains measurable, numeric business events. Each row represents one business event — a sale, a click, a payment. It holds the metrics you want to analyze (quantity, revenue, duration) and foreign keys pointing to dimension tables that provide context.',
        difficulty: 'easy',
        tip: 'Always clarify "each row = one event" — this shows you understand granularity, which interviewers love.'
      },
      {
        q: 'What is a surrogate key and why use it instead of a natural/business key?',
        a: 'A surrogate key is a system-generated integer (auto-increment) used as the primary key in a dimension table. Unlike natural keys (like customer_id from source systems), surrogate keys are stable — they don\'t change if the business key changes. They\'re also required for SCD Type 2, where multiple rows represent the same entity at different points in time, each with a different surrogate key.',
        difficulty: 'medium',
        tip: 'Mentioning SCD Type 2 here shows depth. Even if the interviewer hasn\'t asked about it yet, it signals you understand WHY surrogate keys exist.'
      },
      {
        q: 'Why do you create dimension tables before fact tables?',
        a: 'Fact tables reference dimension surrogate keys as foreign keys. You need those keys to exist in the dimension tables before you can insert rows into the fact table that reference them. Building dimensions first ensures referential integrity.',
        difficulty: 'easy',
        tip: 'Short, clear answer. This is usually a quick filter question — get it right fast and move on.'
      }
    ]
  },
  {
    topicId: 'star-snowflake',
    questions: [
      {
        q: 'What is a star schema? Can you describe its structure?',
        a: 'A star schema has one central fact table surrounded by multiple dimension tables, all directly connected to the fact table via surrogate keys. It looks like a star when drawn — fact in the center, dimensions as the points. Dimensions are denormalized (redundant data is acceptable) to minimize the number of joins needed in queries.',
        difficulty: 'easy',
        tip: 'Sketch it if you\'re on a whiteboard. Visual aids in data modeling questions demonstrate confidence.'
      },
      {
        q: 'What is the difference between star schema and snowflake schema?',
        a: 'In a star schema, all dimensions connect directly to the fact table. In a snowflake schema, dimensions are normalized — split into hierarchies of sub-dimensions. For example, a Location dimension in star schema has city, state, and country in one table. In snowflake, City, State, and Country are separate tables chained together. Snowflake saves some storage but requires more joins, making queries slower.',
        difficulty: 'easy',
        tip: 'Follow up with why star is preferred — shows you have an opinion, not just definitions.'
      },
      {
        q: 'Why is star schema preferred in production data warehouses?',
        a: 'Query performance. Denormalized dimensions mean fewer joins — analysts and BI tools can get answers faster. Storage is cheap and predictable, so the duplication from denormalization is an acceptable tradeoff. Star schema is also simpler to understand and maintain, and most BI tools are optimized for it.',
        difficulty: 'easy',
        tip: '"Storage is cheap, query time is expensive" — this phrase shows engineering judgment.'
      }
    ]
  },
  {
    topicId: 'pipeline-phases',
    questions: [
      {
        q: 'Walk me through how you would structure a data pipeline.',
        a: 'I would use a three-layer architecture: Stage, Transform, and Core. In the Stage layer, raw data lands exactly as received from the source — no modifications. This preserves the original data and acts as a safety net. In the Transform layer, I apply business rules, clean the data, handle deduplication, and standardize formats. Finally, in the Core layer, I load the clean, analytics-ready data modeled as a star schema — fact and dimension tables that BI tools can query. This separation keeps raw data recoverable and transformations replayable.',
        difficulty: 'easy',
        tip: 'This is your chance to shine — you\'ve built this. Reference your Data Warehouse End-to-End project directly.'
      },
      {
        q: 'Why do you keep raw data in the staging layer without any transformation?',
        a: 'The staging layer is your safety net. If a transformation has a bug, business logic changes, or you need to reprocess historical data, you always have the original source data to fall back on without re-extracting from the source system. Source systems are not always available for historical re-extraction — they may have retention limits or performance constraints.',
        difficulty: 'easy',
        tip: 'Mention the "source systems aren\'t always available" angle — shows you\'ve thought about real operational constraints.'
      }
    ]
  },
  {
    topicId: 'incremental-loading',
    questions: [
      {
        q: 'What is incremental loading and why is it important?',
        a: 'Incremental loading means loading only new or changed records since the last successful pipeline run, instead of reloading all data every time. It\'s important for performance (less data processed), cost efficiency, and speed. A full reload of millions of records every hour is neither practical nor necessary when only hundreds of records changed.',
        difficulty: 'easy',
        tip: 'Mention the alternative (full load) and why it\'s problematic. Contrast makes the explanation clearer.'
      },
      {
        q: 'How do you prevent duplicate data in an incremental load pipeline?',
        a: 'You track a watermark — usually the maximum value of a timestamp column (like updated_at or created_at) from the last successful run. On each run, you only load records where updated_at > last_watermark. After a successful load, you update the watermark. You can store this watermark in a control table or a config file.',
        difficulty: 'medium',
        tip: 'Mention the control table pattern for storing watermarks — it shows you think about operationalizing pipelines, not just the logic.'
      }
    ]
  },
  {
    topicId: 'etl-elt',
    questions: [
      {
        q: 'What is ETL and can you walk me through each step?',
        a: 'ETL stands for Extract, Transform, Load. Extract: pull data from source systems (databases, APIs, files). Transform: clean, reshape, and apply business rules to the data — this happens outside the target database, in a processing engine. Load: write the transformed, clean data into the data warehouse. The transformation happens before the data reaches its final destination.',
        difficulty: 'easy',
        tip: 'Keep it structured — name each letter, explain each step. Clear and systematic answers signal engineering maturity.'
      },
      {
        q: 'What is the difference between ETL and ELT, and when would you use each?',
        a: 'In ETL, transformation happens before loading — data is processed in an external tool then loaded clean into the warehouse. In ELT, raw data is loaded first, then transformed inside the warehouse using its own compute. ELT is preferred with modern cloud data warehouses like BigQuery or Snowflake that have massive compute power — they can run SQL transformations efficiently at scale. ETL was more common when warehouses were expensive and external compute was cheaper.',
        difficulty: 'medium',
        tip: 'Mention dbt if you know it — dbt is the standard tool for the Transform step in ELT. Shows awareness of modern tooling.'
      }
    ]
  },
  {
    topicId: 'dw-lake-lakehouse',
    questions: [
      {
        q: 'What is the difference between a data warehouse and a data lake?',
        a: 'A data warehouse stores structured, processed data with a defined schema. It\'s optimized for SQL analytics — fast, reliable, and consistent. A data lake stores raw data in any format — structured, semi-structured, or unstructured — in cheap object storage like S3. There\'s no schema enforced. The lake is flexible and stores everything, but querying it is slower and less structured.',
        difficulty: 'easy',
        tip: '"Schema on write" (warehouse) vs "schema on read" (lake) is a great technical phrase to drop here.'
      },
      {
        q: 'What is a data lakehouse and what problem does it solve?',
        a: 'A lakehouse combines the flexibility and low cost of a data lake with the reliability and performance of a data warehouse. It stores data in open formats like Parquet on object storage but adds a metadata layer (via table formats like Delta Lake or Apache Iceberg) that provides ACID transactions, schema enforcement, and fast SQL queries. Solves the problem of maintaining two separate systems — a lake for raw data and a warehouse for clean data.',
        difficulty: 'medium',
        tip: 'Mention Delta Lake or Apache Iceberg by name — shows you know the actual technology behind the concept.'
      }
    ]
  },
  {
    topicId: 'airflow',
    questions: [
      {
        q: 'What is Apache Airflow and what problem does it solve?',
        a: 'Apache Airflow is a workflow orchestration tool. It schedules and monitors data pipelines, manages task dependencies (making sure task B only runs after task A succeeds), retries failed tasks, and sends alerts on failure. Without orchestration, you\'d manually run scripts and have no visibility into which tasks ran, which failed, or why.',
        difficulty: 'easy',
        tip: 'Emphasize that Airflow doesn\'t move data — it manages WHEN things run. A common misconception is that it\'s an ETL tool.'
      },
      {
        q: 'What is a DAG in Airflow and what rules must it follow?',
        a: 'A DAG is a Directed Acyclic Graph — a Python file defining the pipeline\'s tasks and their dependencies. "Directed" means tasks have a defined execution order. "Acyclic" means there are no cycles — a task cannot depend on itself or create a loop. DAGs can have parallel branches and merge points; the only restriction is no loops.',
        difficulty: 'easy',
        tip: 'Clarify that "acyclic" means no loops, not no branches. A common mistake is thinking DAGs must be linear — they don\'t.'
      },
      {
        q: 'What are operators in Airflow and can you name a few?',
        a: 'Operators define what a task does. PythonOperator executes a Python function. BashOperator runs a shell command. PostgresOperator runs a SQL query against a PostgreSQL database. EmailOperator sends an email. Each task in a DAG is an instance of an operator.',
        difficulty: 'easy',
        tip: 'Naming 3+ operators shows hands-on experience, not just theoretical knowledge.'
      }
    ]
  },
  {
    topicId: 'spark-architecture',
    questions: [
      {
        q: 'Can you explain Apache Spark\'s architecture?',
        a: 'Spark follows a master-slave architecture. The Cluster Manager allocates CPU and memory resources across the cluster — it can be YARN, Kubernetes, or Spark Standalone. The Driver is the main process that runs your application code, builds an execution plan, and distributes tasks. Executors are worker processes on the cluster nodes that actually run the tasks and store data in memory or disk. Each executor has multiple cores (slots) for parallel task execution.',
        difficulty: 'medium',
        tip: 'Draw the hierarchy: Cluster Manager → Worker Nodes → Executors → Tasks. A clear mental model shows solid understanding.'
      },
      {
        q: 'What is the role of the Driver in Spark?',
        a: 'The Driver runs your Spark application code, converts your transformations into a logical plan, optimizes it using the Catalyst optimizer, breaks it into stages and tasks, distributes those tasks to executors, and collects the final results. It\'s the coordinator — it doesn\'t process data itself, it directs who does.',
        difficulty: 'easy',
        tip: 'Key phrase: "coordinator, not processor." The driver plans and coordinates; executors do the actual work.'
      }
    ]
  },
  {
    topicId: 'spark-rdd-partitions',
    questions: [
      {
        q: 'What is an RDD in Spark?',
        a: 'RDD stands for Resilient Distributed Dataset. It\'s Spark\'s fundamental data structure — a distributed, immutable collection of objects spread across the cluster. "Resilient" means fault-tolerant: Spark tracks the lineage of each RDD, so if a partition is lost, it can be recomputed from its parent. Modern Spark uses DataFrames built on top of RDDs, but understanding RDDs explains how Spark works under the hood.',
        difficulty: 'easy',
        tip: 'Mention that modern code uses DataFrames not raw RDDs — shows you know both the theory AND current practice.'
      },
      {
        q: 'What are partitions and why do they matter for performance?',
        a: 'A partition is a chunk of an RDD or DataFrame processed by one task on one executor core. The number of partitions determines the degree of parallelism — more partitions means more tasks can run in parallel. Too few partitions means you\'re not using the full cluster. Too many means overhead from task scheduling outweighs the benefit. The right partition count is typically 2-4x the number of executor cores.',
        difficulty: 'medium',
        tip: 'The "2-4x cores" rule shows practical knowledge. Interviewers appreciate hearing rules of thumb, not just definitions.'
      }
    ]
  },
  {
    topicId: 'spark-shuffle',
    questions: [
      {
        q: 'What is shuffling in Spark and why should you minimize it?',
        a: 'Shuffling is the process of redistributing data across partitions — potentially moving data between executors over the network. It\'s triggered by wide transformations like groupBy, join, and distinct. Shuffles are expensive because they involve network I/O, disk I/O (data is written to disk between stages), and serialization overhead. They create stage boundaries and are typically the #1 cause of slow Spark jobs.',
        difficulty: 'easy',
        tip: 'Say "stage boundary" — it shows you understand how Spark breaks execution into stages around shuffles.'
      },
      {
        q: 'How can you reduce or avoid shuffling in Spark?',
        a: 'For joins involving a small table, use a broadcast join — the small table is sent to all executors eliminating the shuffle entirely. Pre-partition your data on the join key before joining. Use reduceByKey instead of groupByKey when possible. Avoid sorting unless necessary. Cache DataFrames used multiple times so they\'re not recomputed across shuffles.',
        difficulty: 'medium',
        tip: 'Broadcast join is the classic shuffle-avoidance answer. Always mention it first.'
      }
    ]
  },
  {
    topicId: 'spark-cache-persist',
    questions: [
      {
        q: 'When would you use cache() in Spark and what does it do?',
        a: 'I use cache() when the same DataFrame is referenced multiple times in the same job. Without caching, Spark recomputes the DataFrame from scratch each time an action triggers it. cache() stores the computed result in memory (and disk if memory fills) so subsequent uses are fast. A good example: if I filter a large dataset and then run multiple aggregations on it, I\'d cache the filtered DataFrame.',
        difficulty: 'easy',
        tip: 'Give a concrete use case. "I\'d cache it when..." shows practical judgment, not just definition recall.'
      },
      {
        q: 'What is the difference between cache() and persist() in Spark?',
        a: 'cache() is a shorthand for persist() with the default storage level. persist() gives you control over the storage level: MEMORY_AND_DISK (default for DataFrames — stores in memory, spills to disk), MEMORY_ONLY (RDD default — recomputes if doesn\'t fit), DISK_ONLY (slowest, only on disk). Use persist() when you need explicit control — for example, when memory is tight and you want DISK_ONLY to avoid OOM errors.',
        difficulty: 'medium',
        tip: 'The RDD vs DataFrame cache() distinction is a classic trick question — cache() on an RDD is MEMORY_ONLY, not MEMORY_AND_DISK.'
      }
    ]
  },
  {
    topicId: 'spark-lazy-eval',
    questions: [
      {
        q: 'What is lazy evaluation in Spark and why does it matter?',
        a: 'Lazy evaluation means transformations (filter, groupBy, select, join) are not executed immediately when called — Spark records them in a logical execution plan. Only when an action is called (collect, count, write, show) does Spark execute. This matters because it allows the Catalyst optimizer to see the entire transformation chain and optimize it before running — reordering operations, pushing filters down, merging steps — which would be impossible if each transformation ran immediately.',
        difficulty: 'easy',
        tip: 'End with the optimization angle. "It enables Catalyst to see the whole picture" is the key insight they\'re testing.'
      },
      {
        q: 'What is the difference between a transformation and an action in Spark?',
        a: 'Transformations (filter, map, groupBy, select, join, withColumn) are lazy — they return a new DataFrame/RDD and do not execute. Actions (collect, count, show, write, take, first) trigger actual execution and either return a result to the driver or write data to an external system. A Spark job doesn\'t start until an action is called.',
        difficulty: 'easy',
        tip: 'Be ready to categorize any Spark function as transformation or action — this is a common follow-up question.'
      }
    ]
  },
  {
    topicId: 'sql-joins',
    questions: [
      {
        q: 'What is the difference between INNER JOIN and LEFT JOIN?',
        a: 'INNER JOIN returns only rows with a match in both tables — rows with no match are excluded entirely. LEFT JOIN returns all rows from the left table plus matched rows from the right; where no match exists, right-side columns are NULL. Use INNER when you only want confirmed matches. Use LEFT when you need all records from one side regardless of whether the other side has data.',
        difficulty: 'easy',
        tip: 'Draw a Venn diagram mentally — INNER is the intersection, LEFT is the full left circle plus the intersection. Interviewers love asking "when would you use LEFT instead of INNER?"'
      },
      {
        q: 'You write a LEFT JOIN but get the same result as an INNER JOIN. What went wrong?',
        a: 'Almost certainly a WHERE clause filtering on the right table\'s column. Example: LEFT JOIN orders b ON a.id = b.customer_id WHERE b.status = "completed" — the WHERE b.status filters out NULL rows (customers with no completed orders), converting the LEFT JOIN to an INNER JOIN. Solution: move the condition to the ON clause: ON a.id = b.customer_id AND b.status = "completed".',
        difficulty: 'medium',
        tip: 'This is a classic gotcha question. The fix is always: move the right-table filter from WHERE to ON. Shows you understand NULL behavior in outer joins.'
      }
    ]
  },
  {
    topicId: 'parquet-format',
    questions: [
      {
        q: 'Why does Parquet perform better than CSV for analytical queries?',
        a: 'Parquet uses columnar storage — data is organized column-by-column instead of row-by-row. For a query selecting 3 columns out of 100, Parquet reads only those 3 columns (~3% of data). CSV reads all 100 columns for every row. Parquet also compresses better (similar data types in same column), embeds the schema, and supports predicate pushdown via per-column min/max statistics to skip irrelevant data blocks entirely.',
        difficulty: 'easy',
        tip: 'Say "columnar storage" first, then explain the column pruning benefit with a concrete example. Mention predicate pushdown for extra credit.'
      }
    ]
  },
  {
    topicId: 'idempotency',
    questions: [
      {
        q: 'What does it mean for a pipeline to be idempotent and why does it matter?',
        a: 'An idempotent pipeline produces the same result whether it runs once or ten times. It matters because Airflow retries failed tasks automatically, on-call engineers re-run failed pipelines, and backfills process historical periods. If a pipeline isn\'t idempotent, every retry inserts duplicate rows. Common patterns: truncate-insert (delete the target window before inserting) or MERGE/UPSERT (update if exists, insert if not).',
        difficulty: 'medium',
        tip: 'Start with the definition, then immediately give a real consequence ("double-counting revenue if retried"), then give the solution pattern. Shows you understand the production impact, not just the theory.'
      }
    ]
  },
  {
    topicId: 'acid-transactions',
    questions: [
      {
        q: 'What does ACID mean and why is it important for data engineering?',
        a: 'ACID = Atomicity (all-or-nothing writes), Consistency (constraints always enforced), Isolation (concurrent transactions don\'t interfere), Durability (committed data survives crashes). It\'s important because pipelines write data that analysts and systems depend on — partial writes, race conditions, or lost commits corrupt reports and break downstream systems. OLTP databases are fully ACID. Plain data lakes (Parquet on S3) are not — this is why Delta Lake and Iceberg exist: to bring ACID guarantees to object storage.',
        difficulty: 'medium',
        tip: 'Don\'t just define the acronym — explain the DE consequence. "Without atomicity, a failed pipeline write leaves partial data in the target table." That\'s the answer interviewers want to hear.'
      }
    ]
  },
  {
    topicId: 'data-quality',
    questions: [
      {
        q: 'How do you ensure data quality in your pipelines?',
        a: 'I apply quality checks at each pipeline layer. At ingestion: assert row count is within expected range and required fields are non-null before proceeding. At transformation: check for duplicates (GROUP BY key HAVING COUNT > 1), validate referential integrity (all foreign keys exist), and verify value ranges. At the output layer: use dbt tests (not_null, unique, accepted_values, relationships) that run automatically after every build. If a check fails, the pipeline stops and alerts — bad data never reaches analysts.',
        difficulty: 'medium',
        tip: 'Structure your answer by pipeline layer (ingest → transform → output). Mentioning dbt tests shows tool awareness. End with "pipeline fails loudly rather than silently propagating bad data" — that\'s the mindset interviewers want.'
      },
      {
        q: 'What are the dimensions of data quality?',
        a: 'Completeness (all expected records present, required fields populated), Accuracy (values reflect reality — no impossible dates, wrong currencies), Consistency (same entity represented the same way across sources), Uniqueness (no duplicate records for the same business event), Timeliness (data available within the agreed SLA window). Each dimension maps to different checks: completeness = row count assertions and NULL checks; uniqueness = deduplication; timeliness = pipeline SLA monitoring.',
        difficulty: 'easy',
        tip: 'You don\'t need to memorize all five perfectly. Naming 3-4 with real examples shows practical understanding. Interviewers care that you think about quality proactively, not reactively.'
      }
    ]
  },
  {
    topicId: 'docker-de',
    questions: [
      {
        q: 'What is Docker and why do data engineers use it?',
        a: 'Docker packages an application with all its dependencies into a container — an isolated, portable runtime environment. For data engineers: local Airflow setup via docker-compose (one command spins up webserver, scheduler, workers, metadata database); reproducible pipeline environments (exact Python version and library versions); consistent behavior between development and production. Without Docker, "it works on my machine" is a constant problem — with Docker, the machine is the container, and it\'s identical everywhere.',
        difficulty: 'easy',
        tip: 'Mention your Airflow docker-compose experience specifically — it\'s a concrete, relatable answer. Don\'t just give a theoretical "containers are isolated environments" answer.'
      }
    ]
  },
  {
    topicId: 'storage-partitioning',
    questions: [
      {
        q: 'What is partition pruning and how does storage partitioning enable it?',
        a: 'Partition pruning is when a query engine skips entire data directories that cannot contain matching rows. Storage partitioning organizes files into folders by column value (e.g. year=2024/month=01/day=15/). When you run WHERE year=2024 AND month=01, Spark only opens those specific folders — all other year/month directories are skipped without reading a single byte. On a 3-year dataset querying one month, this reduces I/O by ~97%.',
        difficulty: 'medium',
        tip: 'Give a concrete percentage or example. "Reduces data scanned from 100% to ~3%" makes the benefit tangible. Follow up by mentioning the small file problem if they ask about partitioning pitfalls.'
      }
    ]
  }
];

// ---- CODE SNIPPETS ----
const CODE_SNIPPETS = [
  {
    topicId: 'pipeline-phases',
    snippets: [
      {
        title: 'Three-Layer SQL View Pattern',
        language: 'sql',
        code: `-- STAGE layer: raw data (exact copy from source)
CREATE TABLE stage.orders AS
SELECT * FROM source_db.orders;

-- TRANSFORM layer: clean SQL view on top of stage
CREATE VIEW transform.orders_clean AS
SELECT
    order_id,
    UPPER(TRIM(customer_name))     AS customer_name,
    CAST(order_date AS DATE)       AS order_date,
    quantity,
    unit_price,
    quantity * unit_price          AS total_amount
FROM stage.orders
WHERE order_id IS NOT NULL
  AND quantity > 0;

-- CORE layer: final analytics-ready table
CREATE TABLE core.fact_orders AS
SELECT * FROM transform.orders_clean;`,
        explanation: 'Your actual DW project used this exact pattern — stage raw, transform with SQL views, load to core.'
      }
    ]
  },
  {
    topicId: 'incremental-loading',
    snippets: [
      {
        title: 'Incremental Load with Watermark (Python + SQL)',
        language: 'python',
        code: `import psycopg2
from datetime import datetime

def get_last_watermark(conn):
    cur = conn.cursor()
    cur.execute("SELECT MAX(loaded_at) FROM control.pipeline_runs WHERE status = 'success'")
    result = cur.fetchone()[0]
    return result or datetime(2000, 1, 1)  # default for first run

def incremental_load(conn, last_watermark):
    cur = conn.cursor()
    # Only load records newer than last successful run
    cur.execute("""
        INSERT INTO stage.orders
        SELECT * FROM source.orders
        WHERE updated_at > %s
          AND updated_at <= NOW()
    """, (last_watermark,))

    rows_loaded = cur.rowcount
    # Update watermark after successful load
    cur.execute("""
        INSERT INTO control.pipeline_runs (loaded_at, rows_loaded, status)
        VALUES (NOW(), %s, 'success')
    """, (rows_loaded,))
    conn.commit()
    return rows_loaded`,
        explanation: 'The WHERE updated_at > last_watermark is the guard against duplicates. Store watermark in a control table for reliability.'
      }
    ]
  },
  {
    topicId: 'airflow',
    snippets: [
      {
        title: 'Basic DAG with 3 Tasks',
        language: 'python',
        code: `from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def extract():
    print("Extracting data from source...")

def transform():
    print("Transforming data...")

def load():
    print("Loading to warehouse...")

with DAG(
    dag_id='etl_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',        # runs every day
    catchup=False,                     # don't backfill missed runs
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True,
    }
) as dag:

    t1 = PythonOperator(task_id='extract', python_callable=extract)
    t2 = PythonOperator(task_id='transform', python_callable=transform)
    t3 = PythonOperator(task_id='load', python_callable=load)

    # Define dependency order: t1 → t2 → t3
    t1 >> t2 >> t3`,
        explanation: 'The >> operator sets task dependencies. t1 must succeed before t2 runs. This is the pattern from your ETL Pipeline project.'
      },
      {
        title: 'Parallel Tasks in a DAG',
        language: 'python',
        code: `# Parallel tasks that fan out then merge
# extract → [transform_sales, transform_customers] → load

t_extract = PythonOperator(task_id='extract', ...)
t_sales   = PythonOperator(task_id='transform_sales', ...)
t_customers = PythonOperator(task_id='transform_customers', ...)
t_load    = PythonOperator(task_id='load', ...)

# Fan out: extract triggers both transforms in parallel
t_extract >> [t_sales, t_customers]

# Fan in: load waits for BOTH transforms to complete
[t_sales, t_customers] >> t_load`,
        explanation: 'This is why DAGs aren\'t "linear" — they can have parallel branches. Both transform tasks run simultaneously after extract.'
      }
    ]
  },
  {
    topicId: 'spark-rdd-partitions',
    snippets: [
      {
        title: 'Partitions and Parallelism',
        language: 'python',
        code: `from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitionsDemo").getOrCreate()

df = spark.read.csv("large_file.csv", header=True)

# Check current partition count
print(f"Partitions: {df.rdd.getNumPartitions()}")

# Increase partitions for more parallelism (repartition = causes shuffle)
df_more = df.repartition(200)

# Reduce partitions efficiently (coalesce = no shuffle, combines existing)
df_less = df.coalesce(10)

# Partition by a column — useful before joins or writes
df_partitioned = df.repartition(50, "customer_id")

print(f"After repartition: {df_more.rdd.getNumPartitions()}")`,
        explanation: 'repartition() causes a shuffle (full redistribution). coalesce() only merges existing partitions — use it to reduce count cheaply.'
      }
    ]
  },
  {
    topicId: 'spark-shuffle',
    snippets: [
      {
        title: 'Shuffle vs Broadcast Join',
        language: 'python',
        code: `from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("JoinsDemo").getOrCreate()

large_df = spark.read.parquet("orders/")        # millions of rows
small_df = spark.read.csv("products.csv")       # few thousand rows

# BAD: regular join — causes shuffle on both DataFrames
result_slow = large_df.join(small_df, "product_id")

# GOOD: broadcast join — small_df sent to all executors, NO shuffle
result_fast = large_df.join(broadcast(small_df), "product_id")

# Rule of thumb: broadcast if table < 10MB (configurable via spark.sql.autoBroadcastJoinThreshold)`,
        explanation: 'Broadcast join eliminates the shuffle entirely for the small table. Classic optimization for joining a large fact table with small dimension tables.'
      }
    ]
  },
  {
    topicId: 'spark-cache-persist',
    snippets: [
      {
        title: 'Cache vs Persist Storage Levels',
        language: 'python',
        code: `from pyspark.sql import SparkSession
from pyspark import StorageLevel

spark = SparkSession.builder.appName("CacheDemo").getOrCreate()

df = spark.read.parquet("large_dataset/")

# Filter once — reuse many times
filtered = df.filter(df.status == "active").filter(df.amount > 1000)

# cache() = persist(MEMORY_AND_DISK) for DataFrames
filtered.cache()

# Equivalent explicit persist:
# filtered.persist(StorageLevel.MEMORY_AND_DISK)

# Now multiple actions reuse the cached result
count     = filtered.count()           # triggers execution + caching
avg_amt   = filtered.agg({"amount": "avg"}).collect()   # uses cache
top_10    = filtered.orderBy("amount", ascending=False).limit(10).collect()

# Always unpersist when done to free memory
filtered.unpersist()`,
        explanation: 'Without cache(), each action recomputes the full filter chain from disk. With cache(), only the first action pays the cost.'
      }
    ]
  },
  {
    topicId: 'spark-lazy-eval',
    snippets: [
      {
        title: 'Lazy Evaluation — Transformation Chain',
        language: 'python',
        code: `from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("LazyDemo").getOrCreate()

# All of these lines DO NOTHING yet — no execution
df = spark.read.parquet("orders/")                   # lazy
filtered = df.filter(col("status") == "completed")  # lazy
grouped  = filtered.groupBy("product_id")            # lazy
result   = grouped.agg(_sum("amount").alias("total_revenue"))  # lazy

# Only NOW does Spark execute — and it optimizes the entire chain first
# Catalyst may: push the filter before the groupBy, prune unused columns
output = result.collect()   # <-- ACTION: triggers execution

# Check the optimized plan Spark actually runs (not what you wrote):
result.explain(extended=True)`,
        explanation: 'explain() shows you what Catalyst actually runs vs what you wrote. The "Optimized Logical Plan" is often reordered significantly.'
      }
    ]
  },
  {
    topicId: 'star-snowflake',
    snippets: [
      {
        title: 'Star Schema — DDL Pattern',
        language: 'sql',
        code: `-- Dimension table: always has surrogate key
CREATE TABLE dim_customer (
    customer_sk   SERIAL PRIMARY KEY,       -- surrogate key
    customer_id   VARCHAR(50),              -- natural/business key
    customer_name VARCHAR(100),
    city          VARCHAR(100),
    country       VARCHAR(100),
    -- SCD Type 2 columns
    effective_from DATE    DEFAULT CURRENT_DATE,
    effective_to   DATE    DEFAULT '9999-12-31',
    is_current     BOOLEAN DEFAULT TRUE
);

CREATE TABLE dim_product (
    product_sk   SERIAL PRIMARY KEY,
    product_id   VARCHAR(50),
    product_name VARCHAR(200),
    category     VARCHAR(100),
    unit_price   DECIMAL(10, 2)
);

-- Fact table: references surrogate keys, stores metrics
CREATE TABLE fact_orders (
    order_sk        SERIAL PRIMARY KEY,
    customer_sk     INT REFERENCES dim_customer(customer_sk),
    product_sk      INT REFERENCES dim_product(product_sk),
    order_date      DATE,
    quantity        INT,
    unit_price      DECIMAL(10, 2),
    total_amount    DECIMAL(10, 2)
);`,
        explanation: 'Notice: fact table has NO natural keys from source — only surrogate keys + metrics. SCD columns already in dim_customer for when you learn SCD Type 2.'
      }
    ]
  },
  {
    topicId: 'etl-elt',
    snippets: [
      {
        title: 'ELT Pattern — Load Raw, Transform with SQL',
        language: 'sql',
        code: `-- ELT Step 1: LOAD raw data as-is (no transformation)
CREATE TABLE raw.orders AS
SELECT * FROM external_source.orders;

-- ELT Step 2: TRANSFORM using SQL in the warehouse
CREATE TABLE clean.orders AS
SELECT
    order_id::INTEGER                          AS order_id,
    UPPER(TRIM(customer_name))                 AS customer_name,
    TO_DATE(order_date, 'DD/MM/YYYY')          AS order_date,
    COALESCE(quantity, 0)                      AS quantity,
    COALESCE(unit_price, 0.0)                  AS unit_price,
    COALESCE(quantity, 0) * COALESCE(unit_price, 0.0) AS total_amount
FROM raw.orders
WHERE order_id IS NOT NULL;

-- In a modern stack with dbt, the transform step above
-- is just a .sql model file — dbt handles the CREATE TABLE part`,
        explanation: 'ELT keeps raw data intact. The transform SQL runs inside the warehouse. dbt automates exactly this pattern with version control, tests, and documentation.'
      }
    ]
  },
  {
    topicId: 'dw-lake-lakehouse',
    snippets: [
      {
        title: 'Reading Delta Lake Table with Spark',
        language: 'python',
        code: `from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LakehouseDemo") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Write to Delta Lake (adds ACID transaction log on top of Parquet)
df.write.format("delta").mode("overwrite").save("/data/bronze/orders")

# Read it back
df_bronze = spark.read.format("delta").load("/data/bronze/orders")

# Time travel — query data as it was yesterday
df_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2025-01-01") \
    .load("/data/bronze/orders")

# Or by version number
df_v1 = spark.read.format("delta").option("versionAsOf", 1).load("/data/bronze/orders")`,
        explanation: 'Delta Lake turns plain Parquet files into a lakehouse table. Time travel is free — Spark just reads an older version of the transaction log.'
      }
    ]
  },
  {
    topicId: 'idempotency',
    snippets: [
      {
        title: 'MERGE / UPSERT — Idempotent Incremental Load',
        language: 'sql',
        code: `-- PostgreSQL: INSERT ... ON CONFLICT (UPSERT)
-- Idempotent: run twice → same result

INSERT INTO dim_customer (customer_id, name, city, updated_at)
SELECT customer_id, name, city, updated_at
FROM staging_customers
ON CONFLICT (customer_id)
DO UPDATE SET
    name       = EXCLUDED.name,
    city       = EXCLUDED.city,
    updated_at = EXCLUDED.updated_at;

-- -------------------------------------------------
-- Standard MERGE syntax (BigQuery, Snowflake, Delta)
-- -------------------------------------------------
MERGE INTO dim_customer AS target
USING staging_customers AS source
ON target.customer_id = source.customer_id

WHEN MATCHED THEN
    UPDATE SET
        target.name       = source.name,
        target.city       = source.city,
        target.updated_at = source.updated_at

WHEN NOT MATCHED THEN
    INSERT (customer_id, name, city, updated_at)
    VALUES (source.customer_id, source.name, source.city, source.updated_at);

-- -------------------------------------------------
-- Truncate-insert pattern (simpler, also idempotent)
-- Best when you're reprocessing a full date partition
-- -------------------------------------------------
BEGIN;
  DELETE FROM fact_orders WHERE order_date = '2024-01-15';
  INSERT INTO fact_orders
  SELECT * FROM staging_orders WHERE order_date = '2024-01-15';
COMMIT;`,
        explanation: 'Three idempotent write patterns. UPSERT/MERGE: match on natural key, update if exists, insert if not. Safe to re-run — re-running updates same rows with same values. Truncate-insert: delete the partition then re-insert — clean slate for that window. Both patterns make pipeline retries safe.'
      }
    ]
  },
  {
    topicId: 'storage-partitioning',
    snippets: [
      {
        title: 'Hive-style Partitioned Write in PySpark',
        language: 'python',
        code: `from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth

spark = SparkSession.builder.appName("PartitionedWrite").getOrCreate()

df = spark.read.parquet("s3://bucket/raw/orders/")

# Add partition columns if not already present
df_partitioned = df.withColumn("year",  year("order_date")) \
                   .withColumn("month", month("order_date")) \
                   .withColumn("day",   dayofmonth("order_date"))

# Write with Hive-style partitions
# Creates: s3://bucket/orders/year=2024/month=1/day=15/part-*.parquet
df_partitioned.write \\
    .mode("overwrite") \\
    .partitionBy("year", "month", "day") \\
    .parquet("s3://bucket/orders/")

# Reading back — Spark automatically applies partition pruning
df_jan = spark.read.parquet("s3://bucket/orders/") \\
    .filter("year = 2024 AND month = 1")
# ^ Only reads year=2024/month=1/ folders — skips everything else

# Check how many partitions Spark created after a shuffle
print(f"Shuffle partitions: {df_jan.rdd.getNumPartitions()}")

# Tune if needed (default 200 is often wrong)
spark.conf.set("spark.sql.shuffle.partitions", "50")`,
        explanation: 'partitionBy() creates the year=/month=/day= folder structure automatically. Spark\'s query planner applies partition pruning when reading with matching filters. The key insight: storage partitions (folders) and Spark runtime partitions (memory) are different things — partitionBy() controls both here.'
      }
    ]
  }
];

// globals available to app.js
