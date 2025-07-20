## 🔄 Pipeline Overview
This animation demonstrates how recent rows from multiple local Postgres databases are dynamically mapped and synced to Neon using Python and Airflow.

![Alt text](https://github.com/RenzieCoding/sql_portfolio_projects/blob/main/Music%20Database(Chinook)/Img_folder/Pipeline.gif?raw=true)

### 📊 Sync Features and Highlights

- Dynamic source-to-target mapping using parameterized SQL
- Incremental loads across multiple tables
- Centralized error handling for maintainability
- Fully automated with Airflow DAGs
In here, 1 new row synced to artist table in Neon.
We can check the Logs inside airflow like a terminal in VS Code. 

![Alt text](https://github.com/RenzieCoding/sql_portfolio_projects/blob/main/Music%20Database(Chinook)/Img_folder/Airflow_Logs_png?raw=true)

### 💾 Neon Tech
- Stores data synced from our local Postgres setup, acting as a lightweight cloud app backend.
  
![Alt text](https://github.com/RenzieCoding/sql_portfolio_projects/blob/main/Music%20Database(Chinook)/Img_folder/Neon_app_img?raw=true)

### :program: Python Code
<pre><code>```python import psycopg2 import os from datetime import datetime

TABLES = ["track", "album", "artist"] TIME_INTERVAL = "30 minutes"

LOCAL_DB = { "dbname": os.getenv("LOCAL_DB_NAME", "Chinook"), "user": os.getenv("LOCAL_DB_USER", "postgres"), "password": os.getenv("LOCAL_DB_PASSWORD", "your_local_password"), "host": os.getenv("LOCAL_DB_HOST", "host.docker.internal"), "port": os.getenv("LOCAL_DB_PORT", "5432") }

NEON_DB = { "dbname": os.getenv("NEON_DB_NAME", "neondb"), "user": os.getenv("NEON_DB_USER", "neondb_owner"), "password": os.getenv("NEON_DB_PASSWORD", "your_neon_password"), "host": os.getenv("NEON_DB_HOST", "your-neon-host-url"), "port": os.getenv("NEON_DB_PORT", "5432") }

local_conn = psycopg2.connect(LOCAL_DB) local_cursor = local_conn.cursor() neon_conn = psycopg2.connect(NEON_DB) neon_cursor = neon_conn.cursor()

def sync_table(table_name): try: local_cursor.execute(f""" SELECT * FROM {table_name} WHERE created_at > now() - interval '{TIME_INTERVAL}'; """) rows = local_cursor.fetchall() if not rows: print(f"⏩ No new rows for {table_name}") return

columns = [desc[0] for desc in local_cursor.description] col_list = ", ".join(columns) placeholders = ", ".join(["%s"] * len(columns))

insert_sql = f""" INSERT INTO {table_name} ({col_list}) VALUES ({placeholders}); """ for row in rows: neon_cursor.execute(insert_sql, row)

print(f"✅ {len(rows)} new rows synced to {table_name}") except Exception as e: print(f"❌ Error syncing {table_name}: {e}")

def run_pipeline(): print("🚀 Starting sync...") for table in TABLES: sync_table(table) neon_conn.commit() print("🎉 Sync complete.")

if name == "main": run_pipeline() local_cursor.close() local_conn.close() neon_cursor.close() neon_conn.close()
