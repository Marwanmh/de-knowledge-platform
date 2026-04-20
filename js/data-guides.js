// ---- SETUP GUIDES ----
const SETUP_GUIDES = [
  {
    id: 'setup-python',
    icon: '🐍',
    title: 'Python Environment Setup',
    subtitle: 'Install Python and set up your first DE project',
    steps: [
      {
        title: 'Install Python 3.11+',
        commands: [
          '# Mac — using Homebrew (recommended)',
          'brew install python@3.11',
          '',
          '# Linux (Ubuntu/Debian)',
          'sudo apt update && sudo apt install python3.11 python3.11-venv python3-pip',
          '',
          '# Windows — download installer from python.org',
          '# https://www.python.org/downloads/',
          '# IMPORTANT: check "Add Python to PATH" during installation',
          '',
          '# Verify installation',
          'python3 --version   # should print Python 3.11.x'
        ],
        note: 'On Mac, use python3 not python. On Windows, usually python.'
      },
      {
        title: 'Create your first project with virtual environment',
        commands: [
          'mkdir my_de_project && cd my_de_project',
          'python3 -m venv venv',
          '',
          '# Activate (Mac/Linux)',
          'source venv/bin/activate',
          '',
          '# Activate (Windows)',
          'venv\\Scripts\\activate',
          '',
          '# Your prompt should now show (venv)',
          '# Install packages',
          'pip install pandas psycopg2-binary python-dotenv requests sqlalchemy pyarrow',
          '',
          '# Save requirements',
          'pip freeze > requirements.txt'
        ],
        note: 'Always activate venv before working on a project. You should see (venv) in your terminal prompt.'
      },
      {
        title: 'Create .gitignore and .env files',
        commands: [
          '# Create .gitignore',
          'cat > .gitignore << EOF',
          'venv/',
          '.env',
          '__pycache__/',
          '*.pyc',
          '.DS_Store',
          '*.parquet',
          'EOF',
          '',
          '# Create .env file with your secrets',
          'cat > .env << EOF',
          'DB_HOST=localhost',
          'DB_PORT=5432',
          'DB_NAME=mydb',
          'DB_USER=myuser',
          'DB_PASSWORD=changeme',
          'API_KEY=your_api_key_here',
          'EOF'
        ],
        note: 'NEVER commit .env to git. The .gitignore above protects you.'
      }
    ]
  },
  {
    id: 'setup-postgres',
    icon: '🐘',
    title: 'PostgreSQL Local Setup',
    subtitle: 'Install and connect to your local database',
    steps: [
      {
        title: 'Install PostgreSQL',
        commands: [
          '# Mac',
          'brew install postgresql@16',
          'brew services start postgresql@16',
          '',
          '# Linux (Ubuntu/Debian)',
          'sudo apt install postgresql postgresql-contrib',
          'sudo systemctl start postgresql',
          'sudo systemctl enable postgresql',
          '',
          '# Windows',
          '# Download installer from: https://www.postgresql.org/download/windows/',
          '# Use default settings. Remember the password you set for postgres user.'
        ],
        note: 'On Mac with Homebrew, PostgreSQL starts automatically. On Linux, use systemctl.'
      },
      {
        title: 'Create database and user',
        commands: [
          '# Connect as postgres superuser',
          '# Mac/Linux:',
          'psql postgres',
          '',
          '# Windows:',
          '# Open "SQL Shell (psql)" from Start Menu',
          '',
          '-- Run these SQL commands inside psql:',
          'CREATE USER myuser WITH PASSWORD \'mypassword\';',
          'CREATE DATABASE mydb OWNER myuser;',
          'GRANT ALL PRIVILEGES ON DATABASE mydb TO myuser;',
          '\\q',
          '',
          '# Connect with your new user',
          'psql -h localhost -U myuser -d mydb',
          '',
          '# Test it works',
          'SELECT version();',
          '\\q'
        ],
        note: 'Write down your user/password — you need them in your .env file.'
      },
      {
        title: 'Connect from Python',
        commands: [
          '# Test your Python → PostgreSQL connection',
          'python3 << EOF',
          'import psycopg2',
          'import os',
          'from dotenv import load_dotenv',
          'load_dotenv()',
          '',
          'conn = psycopg2.connect(',
          '    host=os.environ["DB_HOST"],',
          '    port=os.environ["DB_PORT"],',
          '    dbname=os.environ["DB_NAME"],',
          '    user=os.environ["DB_USER"],',
          '    password=os.environ["DB_PASSWORD"]',
          ')',
          'cur = conn.cursor()',
          'cur.execute("SELECT version();")',
          'print("Connected:", cur.fetchone()[0])',
          'conn.close()',
          'print("Connection test PASSED")',
          'EOF'
        ],
        note: 'If you see "Connection test PASSED" — you are ready. Install DBeaver (https://dbeaver.io) for a visual interface.'
      }
    ]
  },
  {
    id: 'setup-airflow',
    icon: '🌊',
    title: 'Apache Airflow with Docker',
    subtitle: 'Run Airflow locally in minutes using the official docker-compose setup',
    steps: [
      {
        title: 'Install Docker Desktop',
        commands: [
          '# Download Docker Desktop from:',
          '# https://www.docker.com/products/docker-desktop/',
          '',
          '# Verify installation',
          'docker --version',
          'docker-compose --version',
          '',
          '# Make sure Docker Desktop is running (you should see the whale icon)'
        ],
        note: 'Docker Desktop includes docker-compose. Give it at least 4GB RAM in Settings → Resources.'
      },
      {
        title: 'Download and start Airflow',
        commands: [
          'mkdir airflow-local && cd airflow-local',
          '',
          '# Download the official Airflow docker-compose',
          'curl -LfO \'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml\'',
          '',
          '# Create required directories',
          'mkdir -p ./dags ./logs ./plugins ./config',
          '',
          '# Set the Airflow UID (Linux/Mac only)',
          'echo -e "AIRFLOW_UID=$(id -u)" > .env',
          '',
          '# Initialize the database (first time only — takes 2-3 minutes)',
          'docker-compose up airflow-init',
          '',
          '# Start all services',
          'docker-compose up -d',
          '',
          '# Check all containers are running',
          'docker-compose ps'
        ],
        note: 'First startup takes 3-5 minutes. Wait until all containers show "healthy" status.'
      },
      {
        title: 'Access Airflow and create your first connection',
        commands: [
          '# Open Airflow in browser:',
          '# http://localhost:8080',
          '# Username: airflow',
          '# Password: airflow',
          '',
          '# Add a PostgreSQL connection:',
          '# 1. Go to Admin → Connections',
          '# 2. Click + (Add connection)',
          '# 3. Fill in:',
          '#    Connection Id: postgres_default',
          '#    Connection Type: Postgres',
          '#    Host: host.docker.internal  (NOT localhost — Docker networking)',
          '#    Database: mydb',
          '#    Login: myuser',
          '#    Password: mypassword',
          '#    Port: 5432',
          '# 4. Test → Save',
          '',
          '# Your DAG files go in: ./dags/',
          '# Airflow auto-detects new .py files in that folder (every 30s)'
        ],
        note: 'On Mac/Linux use "host.docker.internal" to reach your local PostgreSQL from inside Docker. On Linux this may need extra docker-compose config — see the troubleshooting note in the official docs.'
      },
      {
        title: 'Stop and restart Airflow',
        commands: [
          '# Stop all Airflow containers (keeps data)',
          'docker-compose down',
          '',
          '# Start again',
          'docker-compose up -d',
          '',
          '# View logs for a specific service',
          'docker-compose logs -f airflow-scheduler',
          '',
          '# Restart just the scheduler (if a DAG change isn\'t showing)',
          'docker-compose restart airflow-scheduler'
        ],
        note: 'Your DAGs and connections persist between restarts. Logs are in the ./logs directory.'
      }
    ]
  },
  {
    id: 'setup-spark',
    icon: '⚡',
    title: 'PySpark Local Setup',
    subtitle: 'Run Spark on your laptop — no cluster needed',
    steps: [
      {
        title: 'Install Java (required by Spark)',
        commands: [
          '# Mac',
          'brew install openjdk@11',
          'echo \'export JAVA_HOME=$(brew --prefix openjdk@11)\' >> ~/.zshrc',
          'source ~/.zshrc',
          '',
          '# Linux (Ubuntu/Debian)',
          'sudo apt install openjdk-11-jdk',
          'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64',
          '',
          '# Windows',
          '# Download JDK 11 from: https://adoptium.net/',
          '# Set JAVA_HOME environment variable to installation directory',
          '',
          '# Verify',
          'java -version  # should show openjdk 11.x'
        ],
        note: 'Spark requires Java. Without it, PySpark imports will fail with a cryptic error.'
      },
      {
        title: 'Install PySpark',
        commands: [
          '# Inside your activated virtual environment',
          'pip install pyspark',
          '',
          '# Test the installation',
          'python3 << EOF',
          'from pyspark.sql import SparkSession',
          '',
          'spark = SparkSession.builder \\',
          '    .appName("test") \\',
          '    .master("local[2]") \\',
          '    .getOrCreate()',
          '',
          'spark.sparkContext.setLogLevel("ERROR")',
          '',
          'data = [("Alice", 1), ("Bob", 2), ("Carol", 3)]',
          'df = spark.createDataFrame(data, ["name", "value"])',
          'df.show()',
          '',
          'print("PySpark working!")',
          'spark.stop()',
          'EOF'
        ],
        note: 'First run downloads Spark binaries (~200MB) and takes 30-60 seconds. Subsequent runs are faster.'
      },
      {
        title: 'Configure for local development',
        commands: [
          '# Create a reusable SparkSession helper',
          'cat > spark_utils.py << EOF',
          'from pyspark.sql import SparkSession',
          '',
          'def get_spark(app_name="local_dev"):',
          '    return (SparkSession.builder',
          '        .appName(app_name)',
          '        .master("local[*]")   # use all CPU cores',
          '        .config("spark.sql.shuffle.partitions", "4")  # override 200 default for local',
          '        .config("spark.driver.memory", "2g")',
          '        .getOrCreate())',
          'EOF',
          '',
          '# Open the Spark UI (while a job is running): http://localhost:4040'
        ],
        note: 'local[*] uses all available CPU cores. local[2] uses exactly 2 cores. Always set shuffle.partitions to a small number locally (4-8) — 200 is way too many for local work.'
      }
    ]
  },
  {
    id: 'setup-dbt',
    icon: '🔷',
    title: 'dbt Core Setup',
    subtitle: 'Set up dbt against your local PostgreSQL in 10 minutes',
    steps: [
      {
        title: 'Install dbt-postgres',
        commands: [
          '# Inside your virtual environment',
          'pip install dbt-core dbt-postgres',
          '',
          '# Verify',
          'dbt --version'
        ],
        note: 'dbt-postgres installs dbt-core + the PostgreSQL adapter. For BigQuery use dbt-bigquery, for Snowflake use dbt-snowflake.'
      },
      {
        title: 'Initialize a dbt project',
        commands: [
          'dbt init my_dbt_project',
          '# Answer the prompts:',
          '# Which database? → postgres (option 1)',
          '# Host: localhost',
          '# Port: 5432',
          '# User: myuser',
          '# Pass: mypassword',
          '# Database: mydb',
          '# Schema: dbt_dev    (dbt creates this schema)',
          '# Threads: 4',
          '',
          'cd my_dbt_project',
          '',
          '# Test connection',
          'dbt debug  # should show all green checks'
        ],
        note: 'dbt creates a profiles.yml file in ~/.dbt/. This is where connection details live.'
      },
      {
        title: 'Write your first model and run it',
        commands: [
          '# Create your first model',
          'cat > models/staging/stg_orders.sql << EOF',
          'SELECT',
          '    order_id,',
          '    customer_id,',
          '    order_date::DATE AS order_date,',
          '    total_amount,',
          '    UPPER(status) AS status',
          'FROM {{ source(\'raw\', \'orders\') }}',
          'WHERE order_id IS NOT NULL',
          'EOF',
          '',
          '# Add a schema.yml for sources and tests',
          '# (see dbt docs for full syntax)',
          '',
          '# Run all models',
          'dbt run',
          '',
          '# Run tests',
          'dbt test',
          '',
          '# View lineage in browser',
          'dbt docs generate && dbt docs serve',
          '# Opens: http://localhost:8080'
        ],
        note: 'dbt creates a VIEW or TABLE in your database for each model. Check your mydb database after running — you should see the dbt_dev schema with your model as a view.'
      }
    ]
  },
  {
    id: 'setup-vscode',
    icon: '💻',
    title: 'VS Code for Data Engineering',
    subtitle: 'Configure VS Code for productive DE development',
    steps: [
      {
        title: 'Install VS Code and extensions',
        commands: [
          '# Download VS Code: https://code.visualstudio.com/',
          '',
          '# Install these extensions (Ctrl+Shift+X → search name):',
          '# 1. Python (Microsoft) — core Python support, debugger',
          '# 2. Pylance — fast type checking and autocomplete',
          '# 3. SQLTools — run SQL queries from VS Code',
          '# 4. SQLTools PostgreSQL Driver — connect to PostgreSQL',
          '# 5. Docker — view/manage containers',
          '# 6. GitLens — enhanced git visualization',
          '# 7. YAML (Red Hat) — Airflow docker-compose.yaml support',
          '# 8. Rainbow CSV — makes CSV files readable',
          '# 9. indent-rainbow — visualize Python indentation'
        ],
        note: 'Extensions are installed once and apply to all projects.'
      },
      {
        title: 'Configure Python interpreter and settings',
        commands: [
          '# Open your project folder in VS Code',
          '# Ctrl+Shift+P → "Python: Select Interpreter"',
          '# Choose: ./venv/bin/python (your project virtualenv)',
          '',
          '# Create settings.json for project-specific config',
          'mkdir -p .vscode',
          'cat > .vscode/settings.json << EOF',
          '{',
          '  "python.defaultInterpreterPath": "${workspaceFolder}/venv/bin/python",',
          '  "editor.formatOnSave": true,',
          '  "python.formatting.provider": "black",',
          '  "[python]": {',
          '    "editor.rulers": [88]',
          '  },',
          '  "files.exclude": {',
          '    "**/__pycache__": true,',
          '    "**/*.pyc": true',
          '  }',
          '}',
          'EOF',
          '',
          '# Install Black formatter',
          'pip install black'
        ],
        note: 'formatOnSave automatically formats your Python code on every save — keeps code clean without thinking about it.'
      },
      {
        title: 'Connect SQLTools to PostgreSQL',
        commands: [
          '# In VS Code:',
          '# 1. Click the SQLTools icon in sidebar (cylinder shape)',
          '# 2. Click "Add New Connection"',
          '# 3. Select PostgreSQL',
          '# 4. Fill in:',
          '#    Connection Name: local_postgres',
          '#    Server: localhost',
          '#    Port: 5432',
          '#    Database: mydb',
          '#    Username: myuser',
          '#    Password: mypassword',
          '# 5. Test Connection → Save',
          '',
          '# Now you can run SQL directly from VS Code:',
          '# Open any .sql file',
          '# Ctrl+Shift+P → "SQLTools: Run Selected Query"',
          '# Or highlight SQL and press Ctrl+E Ctrl+E'
        ],
        note: 'SQLTools lets you run SQL and browse tables without leaving VS Code — much faster than switching to DBeaver for quick checks.'
      }
    ]
  }
];

// ---- GUIDED PROJECTS ----
const GUIDED_PROJECTS = [
  {
    id: 'project-weather',
    number: 1,
    emoji: '🌤️',
    title: 'Weather Data Pipeline',
    subtitle: 'API → PostgreSQL → Airflow → SQL transforms',
    difficulty: 'Beginner',
    time: '4–6 hours',
    prerequisites: ['Python basics', 'PostgreSQL setup', 'Airflow running locally'],
    teaches: ['REST API ingestion', 'Incremental loading', 'Idempotency', 'Airflow DAG', 'SQL transformations', 'Error handling'],
    description: 'Build a complete pipeline that pulls weather data from a free API, loads it to PostgreSQL, transforms it with SQL, and schedules everything with Airflow. This is your first production-grade pipeline.',
    architecture: [
      'OpenWeatherMap API (free) → Python extract script',
      '→ staging.weather_raw (PostgreSQL, raw JSON stored)',
      '→ Airflow DAG (schedules daily)',
      '→ SQL transform → core.weather_facts (clean table)',
      '→ Analytical query: hottest days per city'
    ],
    steps: [
      {
        title: 'Step 1: Get a free API key',
        detail: 'Sign up at openweathermap.org → My API Keys → generate key. It activates in ~10 minutes. Free tier allows 60 calls/minute, more than enough.',
        code: `# Test your key works
import requests

API_KEY = 'your_key_here'
city = 'Cairo'

resp = requests.get(
    'https://api.openweathermap.org/data/2.5/weather',
    params={'q': city, 'appid': API_KEY, 'units': 'metric'}
)
print(resp.json())`
      },
      {
        title: 'Step 2: Create the database tables',
        detail: 'Run this SQL in your PostgreSQL database to create the staging and core tables.',
        code: `-- Run in psql or DBeaver
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;

-- Raw staging table (stores everything from API)
CREATE TABLE staging.weather_raw (
    id            SERIAL PRIMARY KEY,
    city          VARCHAR(100),
    fetched_at    TIMESTAMP DEFAULT NOW(),
    raw_data      JSONB,          -- store full API response
    temperature   NUMERIC(5,2),
    humidity      INT,
    description   VARCHAR(200),
    wind_speed    NUMERIC(5,2),
    weather_date  DATE
);

-- Core facts table (clean, analytics-ready)
CREATE TABLE core.weather_facts (
    id            SERIAL PRIMARY KEY,
    city          VARCHAR(100),
    weather_date  DATE,
    avg_temp      NUMERIC(5,2),
    max_temp      NUMERIC(5,2),
    min_temp      NUMERIC(5,2),
    avg_humidity  INT,
    description   VARCHAR(200),
    loaded_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE(city, weather_date)   -- idempotency: one row per city per day
);

-- Metadata table for tracking pipeline runs
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    pipeline_name  VARCHAR(100) PRIMARY KEY,
    last_run_at    TIMESTAMP,
    last_run_rows  INT,
    status         VARCHAR(20)
);`
      },
      {
        title: 'Step 3: Write the extract and load script',
        detail: 'This script fetches current weather for a list of cities and loads to staging. It is idempotent — running it twice for the same day does not duplicate rows.',
        code: `# src/extract_weather.py
import requests
import psycopg2
import os
import json
import logging
from datetime import date
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

CITIES = ['Cairo', 'Alexandria', 'Giza', 'London', 'New York']
API_KEY = os.environ['WEATHER_API_KEY']
BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'

def fetch_weather(city: str) -> dict:
    resp = requests.get(
        BASE_URL,
        params={'q': city, 'appid': API_KEY, 'units': 'metric'},
        timeout=10
    )
    resp.raise_for_status()
    return resp.json()

def load_to_staging(city: str, data: dict, conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO staging.weather_raw
                (city, raw_data, temperature, humidity, description, wind_speed, weather_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            city,
            json.dumps(data),
            data['main']['temp'],
            data['main']['humidity'],
            data['weather'][0]['description'],
            data['wind']['speed'],
            date.today()
        ))
    conn.commit()

def run_extract():
    conn_str = (f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
                f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}")
    conn = psycopg2.connect(conn_str)
    loaded = 0

    for city in CITIES:
        try:
            data = fetch_weather(city)
            load_to_staging(city, data, conn)
            loaded += 1
            logger.info(f"Loaded weather for {city}: {data['main']['temp']}°C")
        except Exception as e:
            logger.error(f"Failed to fetch {city}: {e}")

    conn.close()
    logger.info(f"Extract complete. Loaded {loaded}/{len(CITIES)} cities.")
    return loaded

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s | %(levelname)s | %(message)s')
    run_extract()`
      },
      {
        title: 'Step 4: Write the SQL transform',
        detail: 'Transform raw staging data into clean core facts. The MERGE ensures idempotency — re-running updates existing rows instead of duplicating.',
        code: `-- sql/transform_weather.sql
INSERT INTO core.weather_facts
    (city, weather_date, avg_temp, max_temp, min_temp, avg_humidity, description)
SELECT
    city,
    weather_date,
    ROUND(AVG(temperature)::NUMERIC, 2) AS avg_temp,
    MAX(temperature)                    AS max_temp,
    MIN(temperature)                    AS min_temp,
    ROUND(AVG(humidity))::INT           AS avg_humidity,
    MODE() WITHIN GROUP (ORDER BY description) AS description
FROM staging.weather_raw
WHERE weather_date = CURRENT_DATE
GROUP BY city, weather_date
ON CONFLICT (city, weather_date)
DO UPDATE SET
    avg_temp    = EXCLUDED.avg_temp,
    max_temp    = EXCLUDED.max_temp,
    min_temp    = EXCLUDED.min_temp,
    avg_humidity = EXCLUDED.avg_humidity,
    loaded_at   = NOW();

-- Analytical query: top 3 hottest days per city this month
SELECT city, weather_date, avg_temp,
       RANK() OVER (PARTITION BY city ORDER BY avg_temp DESC) AS temp_rank
FROM core.weather_facts
WHERE weather_date >= DATE_TRUNC('month', CURRENT_DATE)
QUALIFY temp_rank <= 3;  -- Use CTE + WHERE if your DB doesn't support QUALIFY`
      },
      {
        title: 'Step 5: Create the Airflow DAG',
        detail: 'Put this file in your Airflow dags/ folder. It runs daily, is idempotent, and marks itself failed if no data is loaded.',
        code: `# dags/weather_pipeline_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
sys.path.insert(0, '/opt/airflow/src')

from extract_weather import run_extract

default_args = {
    'owner': 'de_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    schedule_interval='0 7 * * *',   # 7am daily
    start_date=days_ago(1),
    catchup=False,
    tags=['weather', 'daily']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_weather',
        python_callable=run_extract,
    )

    transform_task = PostgresOperator(
        task_id='transform_to_core',
        postgres_conn_id='postgres_default',
        sql='sql/transform_weather.sql',
    )

    # Task dependency: extract must succeed before transform
    extract_task >> transform_task`
      }
    ]
  },
  {
    id: 'project-ecommerce',
    number: 2,
    emoji: '🛒',
    title: 'E-commerce Data Warehouse',
    subtitle: 'CSV files → Star Schema → Window Function Analysis',
    difficulty: 'Beginner–Intermediate',
    time: '3–5 hours',
    prerequisites: ['SQL Fundamentals complete', 'PostgreSQL setup'],
    teaches: ['Star schema hands-on', 'Surrogate keys', 'Dimension/fact build order', 'Window functions in practice', 'Data cleaning with SQL'],
    description: 'Build a dimensional data warehouse from scratch using 3 CSV files. Design the star schema, build the dimensions and fact table in the correct order, then answer business questions with window function queries.',
    architecture: [
      'customers.csv + products.csv + orders.csv (raw files)',
      '→ staging schema (raw CSV data loaded)',
      '→ dim_customer, dim_product, dim_date (dimension tables)',
      '→ fact_sales (fact table with surrogate key FKs)',
      '→ Analytical queries using window functions'
    ],
    steps: [
      {
        title: 'Step 1: Create sample CSV data',
        detail: 'Create these 3 CSV files to use as your source data.',
        code: `# create_sample_data.py
import pandas as pd
import numpy as np
from datetime import date, timedelta
import random

random.seed(42)

# customers.csv
customers = pd.DataFrame({
    'customer_id': [f'C{i:04d}' for i in range(1, 101)],
    'name': [f'Customer {i}' for i in range(1, 101)],
    'email': [f'customer{i}@email.com' for i in range(1, 101)],
    'city': random.choices(['Cairo', 'Alexandria', 'Giza', 'Luxor'], k=100),
    'country': 'EG',
    'created_at': [date(2023, 1, 1) + timedelta(days=random.randint(0, 365)) for _ in range(100)]
})
customers.to_csv('customers.csv', index=False)

# products.csv
products = pd.DataFrame({
    'product_id': [f'P{i:04d}' for i in range(1, 51)],
    'name': [f'Product {i}' for i in range(1, 51)],
    'category': random.choices(['Electronics', 'Clothing', 'Food', 'Books'], k=50),
    'price': [round(random.uniform(5, 500), 2) for _ in range(50)]
})
products.to_csv('products.csv', index=False)

# orders.csv
orders = []
for i in range(1, 1001):
    orders.append({
        'order_id': f'ORD{i:05d}',
        'customer_id': random.choice(customers['customer_id'].tolist()),
        'product_id': random.choice(products['product_id'].tolist()),
        'quantity': random.randint(1, 10),
        'order_date': date(2024, 1, 1) + timedelta(days=random.randint(0, 364)),
        'status': random.choices(['completed', 'cancelled', 'pending'], weights=[70, 15, 15])[0]
    })
pd.DataFrame(orders).to_csv('orders.csv', index=False)
print("Sample data created: customers.csv, products.csv, orders.csv")`
      },
      {
        title: 'Step 2: Load CSVs to staging + build dimensions',
        detail: 'Load raw CSVs to staging, then build dimension tables with surrogate keys.',
        code: `-- Load staging tables (do this with Python psycopg2 + pandas to_sql, or use COPY)

-- Build dimension tables (surrogate keys added by SERIAL)
CREATE TABLE dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) UNIQUE NOT NULL,
    name        VARCHAR(200),
    city        VARCHAR(100),
    country     VARCHAR(10),
    created_at  DATE
);

CREATE TABLE dim_product (
    product_sk  SERIAL PRIMARY KEY,
    product_id  VARCHAR(20) UNIQUE NOT NULL,
    name        VARCHAR(200),
    category    VARCHAR(100),
    price       NUMERIC(10,2)
);

CREATE TABLE dim_date (
    date_sk     INT PRIMARY KEY,   -- YYYYMMDD as integer: 20240115
    full_date   DATE,
    year        INT,
    quarter     INT,
    month       INT,
    month_name  VARCHAR(20),
    week        INT,
    day_of_week VARCHAR(20)
);

-- Populate dim_customer from staging
INSERT INTO dim_customer (customer_id, name, city, country, created_at)
SELECT customer_id, name, city, country, created_at::DATE
FROM staging.customers;

-- Populate dim_date (generate all dates for 2024)
INSERT INTO dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_sk,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INT,
    EXTRACT(QUARTER FROM d)::INT,
    EXTRACT(MONTH FROM d)::INT,
    TO_CHAR(d, 'Month'),
    EXTRACT(WEEK FROM d)::INT,
    TO_CHAR(d, 'Day')
FROM generate_series('2024-01-01'::DATE, '2024-12-31'::DATE, '1 day') AS d;`
      },
      {
        title: 'Step 3: Build the fact table',
        detail: 'Build fact_sales referencing dimension surrogate keys. This is the core of the star schema.',
        code: `-- Build fact_sales AFTER dimensions exist
CREATE TABLE fact_sales (
    sale_sk      SERIAL PRIMARY KEY,
    order_id     VARCHAR(20),
    customer_sk  INT REFERENCES dim_customer(customer_sk),
    product_sk   INT REFERENCES dim_product(product_sk),
    date_sk      INT REFERENCES dim_date(date_sk),
    quantity     INT,
    unit_price   NUMERIC(10,2),
    revenue      NUMERIC(12,2),
    status       VARCHAR(20)
);

-- Load fact by joining staging to dimensions to get surrogate keys
INSERT INTO fact_sales
    (order_id, customer_sk, product_sk, date_sk, quantity, unit_price, revenue, status)
SELECT
    o.order_id,
    dc.customer_sk,
    dp.product_sk,
    TO_CHAR(o.order_date::DATE, 'YYYYMMDD')::INT AS date_sk,
    o.quantity,
    dp.price AS unit_price,
    (o.quantity * dp.price) AS revenue,
    o.status
FROM staging.orders o
JOIN dim_customer dc ON o.customer_id = dc.customer_id
JOIN dim_product  dp ON o.product_id  = dp.product_id
WHERE o.status = 'completed';  -- only completed orders in fact

-- Verify
SELECT COUNT(*) FROM fact_sales;
SELECT SUM(revenue) AS total_revenue FROM fact_sales;`
      },
      {
        title: 'Step 4: Answer business questions with window functions',
        detail: 'These are the types of queries your star schema enables. Practice writing each one.',
        code: `-- Q1: Top 3 customers by revenue per city
WITH customer_revenue AS (
    SELECT
        dc.city,
        dc.name,
        SUM(f.revenue) AS total_revenue
    FROM fact_sales f
    JOIN dim_customer dc ON f.customer_sk = dc.customer_sk
    GROUP BY dc.city, dc.name
),
ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY city ORDER BY total_revenue DESC) AS rnk
    FROM customer_revenue
)
SELECT city, name, total_revenue FROM ranked WHERE rnk <= 3;

-- Q2: Monthly revenue with month-over-month growth
WITH monthly AS (
    SELECT
        dd.year,
        dd.month,
        SUM(f.revenue) AS revenue
    FROM fact_sales f
    JOIN dim_date dd ON f.date_sk = dd.date_sk
    GROUP BY dd.year, dd.month
)
SELECT
    year, month, revenue,
    LAG(revenue) OVER (ORDER BY year, month) AS prev_month,
    ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY year, month))
          / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0), 1) AS pct_growth
FROM monthly;

-- Q3: Best-selling product per category
WITH prod_revenue AS (
    SELECT dp.category, dp.name,
           SUM(f.revenue) AS total_revenue
    FROM fact_sales f JOIN dim_product dp ON f.product_sk = dp.product_sk
    GROUP BY dp.category, dp.name
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rnk
    FROM prod_revenue
)
SELECT category, name, total_revenue FROM ranked WHERE rnk = 1;`
      }
    ]
  },
  {
    id: 'project-spark',
    number: 3,
    emoji: '⚡',
    title: 'PySpark Data Transformation',
    subtitle: 'Large-scale processing: partitioned Parquet → SQL analysis',
    difficulty: 'Intermediate',
    time: '3–4 hours',
    prerequisites: ['PySpark installed', 'Python for DE complete', 'Spark Knowledge Map reviewed'],
    teaches: ['PySpark DataFrame API', 'Storage partitioning', 'Broadcast joins', 'Cache and persist', 'df.explain()', 'Performance tuning'],
    description: 'Process a large dataset with PySpark: read CSV, apply transformations, write partitioned Parquet, run analytical queries, and inspect the execution plan. You will see firsthand how lazy evaluation and partitioning affect performance.',
    architecture: [
      'Large CSV file (1M+ rows generated)',
      '→ PySpark read with schema',
      '→ Filter, join with broadcast, groupBy',
      '→ Cache reused DataFrame',
      '→ Write partitioned Parquet (year/month)',
      '→ Read back with partition pruning',
      '→ df.explain(True) inspection'
    ],
    steps: [
      {
        title: 'Step 1: Generate a large dataset',
        detail: 'Generate a 1M-row dataset to work with. This takes ~30 seconds.',
        code: `# generate_data.py
import pandas as pd
import numpy as np
import os

np.random.seed(42)
N = 1_000_000  # 1 million rows

df = pd.DataFrame({
    'event_id':    range(1, N + 1),
    'user_id':     np.random.randint(1, 10001, N),
    'product_id':  np.random.randint(1, 501, N),
    'event_type':  np.random.choice(['view', 'add_to_cart', 'purchase'], N, p=[0.7, 0.2, 0.1]),
    'amount':      np.where(np.random.choice(['purchase', 'other'], N, p=[0.1, 0.9]) == 'purchase',
                            np.random.uniform(10, 500, N), 0).round(2),
    'year':        np.random.choice([2023, 2024], N),
    'month':       np.random.randint(1, 13, N),
    'day':         np.random.randint(1, 29, N),
})

os.makedirs('data', exist_ok=True)
df.to_csv('data/events.csv', index=False)
print(f"Generated {N:,} rows → data/events.csv ({os.path.getsize('data/events.csv')//1024//1024}MB)")`
      },
      {
        title: 'Step 2: Read, filter, and inspect',
        detail: 'Read with an explicit schema (never infer on large files — it reads the whole file twice). Apply filters and inspect the plan.',
        code: `# spark_pipeline.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, broadcast, sum as spark_sum, count, avg, lit

spark = (SparkSession.builder
    .appName("DE_Project_3")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")  # small dataset, 8 is enough
    .config("spark.driver.memory", "2g")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# Define schema explicitly — faster than inferSchema=True
schema = StructType([
    StructField("event_id",   IntegerType(), False),
    StructField("user_id",    IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("event_type", StringType(),  False),
    StructField("amount",     DoubleType(),  True),
    StructField("year",       IntegerType(), False),
    StructField("month",      IntegerType(), False),
    StructField("day",        IntegerType(), False),
])

# Read CSV
events = spark.read.csv("data/events.csv", schema=schema, header=True)

print(f"Total rows: {events.count():,}")
events.printSchema()

# Filter to purchases only — narrow transformation (no shuffle)
purchases = events.filter(col("event_type") == "purchase")
print(f"Purchases: {purchases.count():,}")

# Inspect the execution plan — see filter pushdown
purchases.explain(True)`
      },
      {
        title: 'Step 3: Broadcast join + cache',
        detail: 'Join with a small lookup table using broadcast (no shuffle). Cache the result since it is used twice.',
        code: `# Create a small product lookup (simulates a dimension table)
product_data = [(i, f"Product {i}", ["Electronics","Clothing","Food","Books"][i % 4])
                for i in range(1, 501)]
products_df = spark.createDataFrame(product_data, ["product_id", "product_name", "category"])

print(f"Products table size: {products_df.count()} rows — small enough to broadcast")

# Broadcast join — products_df sent to all executors, no shuffle
enriched = purchases.join(
    broadcast(products_df),
    on="product_id",
    how="left"
)

# Cache because we use enriched twice (revenue summary + category summary)
enriched.cache()
enriched.count()  # trigger caching (first action materializes it)

print("DataFrame cached. Next operations will read from memory.")

# Check the plan — you should see BroadcastHashJoin (not SortMergeJoin)
enriched.explain()

# Use enriched twice — reads from cache both times (no recomputation)
# 1. Revenue by user
user_revenue = (enriched
    .groupBy("user_id")
    .agg(
        spark_sum("amount").alias("total_revenue"),
        count("*").alias("purchase_count"),
        avg("amount").alias("avg_order_value")
    )
    .orderBy(col("total_revenue").desc())
)

# 2. Revenue by category
category_revenue = (enriched
    .groupBy("category")
    .agg(spark_sum("amount").alias("revenue"))
    .orderBy(col("revenue").desc())
)

print("\\nTop 10 users by revenue:")
user_revenue.show(10)

print("\\nRevenue by category:")
category_revenue.show()`
      },
      {
        title: 'Step 4: Write partitioned Parquet and read with pruning',
        detail: 'Write output as Hive-style partitioned Parquet. Read back with a filter to see partition pruning in action.',
        code: `# Write partitioned by year/month
(enriched
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet("data/output/purchases/")
)

print("Written partitioned Parquet to data/output/purchases/")
print("Check the folder structure — you should see year=2023/, year=2024/ subfolders")

# Release cache
enriched.unpersist()

# Read back with filter — Spark reads ONLY matching partitions
purchases_2024 = spark.read.parquet("data/output/purchases/").filter(
    (col("year") == 2024) & (col("month").isin([1, 2, 3]))
)

print(f"\\nQ1 2024 purchases: {purchases_2024.count():,}")

# Explain shows FileScan only reading year=2024/month=1,2,3 partitions
purchases_2024.explain()

# Final cleanup
spark.stop()
print("\\nProject 3 complete!")`
      }
    ]
  }
];
