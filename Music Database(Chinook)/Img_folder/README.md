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
