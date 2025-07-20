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

### 🐍 Python Code
```python 
import psycopg2
from datetime import datetime

# --- CONFIG ---
TABLES = [
    "artist",
    "album",
    "media_type",
    "genre",
    "track",
    "customer",
    "invoice",
    "invoice_line",
    "employee",
    "playlist",
    "playlist_track"
]

TIME_INTERVAL = "30 minutes"  # Adjust as needed

LOCAL_DB = {
    "dbname": "Chinook",
    "user": "postgres",
    "password": "postgres",
    "host": "host.docker.internal",
    "port": "5432"
}

NEON_DB = {
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_PlqJ2NQc1Xty",
    "host": "ep-cool-tree-a1ulzi0i-pooler.ap-southeast-1.aws.neon.tech",
    "port": "5432"
}

# --- DB CONNECTIONS ---
local_conn = psycopg2.connect(**LOCAL_DB)
local_cursor = local_conn.cursor()

neon_conn = psycopg2.connect(**NEON_DB)
neon_cursor = neon_conn.cursor()


def sync_table(table_name):
    """Fetch new rows from local DB and insert them into Neon DB."""
    try:
        local_cursor.execute(f"""
            SELECT * FROM {table_name}
            WHERE created_at > now() - interval '{TIME_INTERVAL}';
        """)
        rows = local_cursor.fetchall()
        if not rows:
            print(f"⏩ No new rows for {table_name}")
            return

        # Dynamically build columns & placeholders
        columns = [desc[0] for desc in local_cursor.description]
        col_list = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        insert_sql = f"""
            INSERT INTO {table_name} ({col_list})
            VALUES ({placeholders});
        """

        for row in rows:
            neon_cursor.execute(insert_sql, row)

        print(f"✅ {len(rows)} new rows synced to {table_name}")

    except Exception as e:
        print(f"❌ Error syncing {table_name}: {e}")


def run_pipeline():
    print("🚀 Starting Chinook -> Neon sync pipeline...\n")
    for table in TABLES:
        sync_table(table)

    neon_conn.commit()
    print("\n🎉 All tables synced successfully!")


# --- RUN ---
if __name__ == "__main__":
    run_pipeline()

    # Close connections
    local_cursor.close()
    local_conn.close()
    neon_cursor.close()
    neon_conn.close()
```
### 🧱 Neon to Databricks
- After staging the data to Neon, we can move the tables to databricks and perform future transformations
  ![Alt text](https://github.com/RenzieCoding/sql_portfolio_projects/blob/main/Music%20Database(Chinook)/Img_folder/neon_to_databricks?raw=true)

### 🧱 📊 Databricks connection to power BI desktop
- Navigate to SQL warehouses > Connection details (use this in power BI desktop)
  ![Alt text](https://github.com/RenzieCoding/sql_portfolio_projects/blob/main/Music%20Database(Chinook)/Img_folder/databricks_to_pbi?raw=true)

### 📊 Successful connection in Power BI Deskto
- Navigate to SQL warehouses > Connection details (use this in power BI desktop)
  ![Alt text](https://github.com/RenzieCoding/sql_portfolio_projects/blob/main/Music%20Database(Chinook)/Img_folder/pbi_connection?raw=true)
  
  
