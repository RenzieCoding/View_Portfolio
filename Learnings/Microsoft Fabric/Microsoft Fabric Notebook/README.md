
![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20Notebooks/asset_checking_dim_date.png?raw=true)

## ✅ Module 1 - Create a dimension table in power query

<details>
<summary>Click to expand notes</summary>
  
### ✨Created a dim_date table in my dataflow before using notebook. 


This will be used in joining the fact_table for testing purposes in the notebook.
Overview

``` sql

  let

  Source = fact_table,

  MinDate = List.Min(Source[lpepPickup]),

  MaxDate = List.Max(Source[lpepPickup]),

  DateList = List.Dates(MinDate, Duration.Days(MaxDate - MinDate) + 1, #duration(1,0,0,0)),

  DateTable = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"} ),

  #"Changed column type" = Table.TransformColumnTypes(DateTable, {{"Date", type date}}),

  #"Inserted year" = Table.AddColumn(#"Changed column type", "Year", each Date.Year([Date]), type nullable number),

  #"Inserted month" = Table.AddColumn(#"Inserted year", "Month", each Date.Month([Date]), type nullable number),

  #"Inserted quarter" = Table.AddColumn(#"Inserted month", "Quarter", each Date.QuarterOfYear([Date]), type nullable number),

  #"Added custom" = Table.AddColumn(#"Inserted quarter", "MonthYearOrder", each [Year]* 100 + [Month]),

  #"Inserted day" = Table.AddColumn(#"Added custom", "Day", each Date.Day([Date]), type nullable number),

  #"Inserted day of week" = Table.AddColumn(#"Inserted day", "Day of week", each Date.DayOfWeek([Date]), type nullable number)

in

  #"Inserted day of week"
```
</details>

## 📁 Module 2 Using Notebook

<details>
<summary>Click to expand notes</summary>

### ✨ Checking dim_date table


```python

nyc_dim_date_df = spark.sql("""SELECT * FROM mylakehouse.dim_date""")

display(nyc_dim_date_df)
```
![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20Notebooks/asset_checking_dim_date.png?raw=true)

## Using python to future proof if ever there are table name changes


``` python
nyc_taxi_table = "mylakehouse.nyc_taxi_merged_with_discounts_source"

date_table ="mylakehouse.dim_date"

  

query = f"""

SELECT * FROM {nyc_taxi_table} AS nyc_taxi_fact

LEFT JOIN {date_table} dim_date ON dim_date.Date = nyc_taxi_fact.lpepPickup

LIMIT 1000"""

  

nyc_merged_df = spark.sql(query)

display(nyc_merged_df)
```

![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20Notebooks/using_python_to_future_proof.png?raw=true)

## Transforming the time columns into int

 ```python
 #transforming

#Define keywords to match

keywords = ["Year","Year2","Month","Quarter","Day","Week"]

  

#Find matching columns

target_cols =[c for c in nyc_merged_df.columns if any (k in c for k in keywords)]

  

#ReStart with original Dataframe

nyc_merged_df_cleaned = nyc_merged_df

  

#Cast all matching columns to int

for c in target_cols:

    nyc_merged_df_cleaned = nyc_merged_df_cleaned.withColumn(c, col(c).cast("int"))

  

display(nyc_merged_df_cleaned)
```

![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20Notebooks/transformed_columns_into_int.png?raw=true)

## # Cleaning Column Name because Delta Lake (used by Microsoft Fabric) does not allow by default

``` python

def sanitized_column_names(nyc_merged_df_cleaned):

    for col_name in nyc_merged_df_cleaned.columns:

        clean_name = col_name.replace(" ","_").replace("(","").replace(")")
```

## Function to save the DataFrame to mylakehouse

```python
def sanitize_column_names(df):

    for col_name in df.columns:

        clean_name = col_name.replace(" ", "_").replace("(", "").replace(")", "")

        df = df.withColumnRenamed(col_name, clean_name)

    return df

  

nyc_cleaned_sanitized = sanitize_column_names(nyc_merged_df_cleaned)

  

nyc_cleaned_sanitized.write.format("delta").mode("overwrite").saveAsTable("nyc_taxi_transformed")
```

## Checking if the joined and cleaned table is ready

```
df = spark.sql("SELECT * FROM mylakehouse.nyc_taxi_transformed LIMIT 1000")

display(df)
```

![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20Notebooks/checking_joined_and_cleaned_table_ready.png?raw=true)
