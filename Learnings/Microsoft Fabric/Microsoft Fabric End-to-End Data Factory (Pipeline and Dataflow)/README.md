# Microsoft Fabric End-to-End Data Factory (Pipeline and Dataflow)

Learning Reference: [Module 2 - Transform data with a dataflow in Data Factory - Microsoft Fabric | Microsoft Learn](https://learn.microsoft.com/en-us/fabric/data-factory/tutorial-end-to-end-dataflow)

Overview

I recently explored Microsoft Fabric's data capabilities through a three-module learning path. This hands-on experience helped me get familiar with the interface and key components like pipelines, dataflows, and Lakehouse.

Coming from a Power BI development background, I was able to apply my knowledge of Power Query - especially in areas like:

-  Dynamic column transformation
-  Appending columns without breaking the schema
-  Avoiding duplicate columns that can disrupt the query logic

One thing I always strive for is making scenarios as close to real business use cases as possible, and this learning path supported that mindset. If you're aiming to bridge the gap between data engineering and BI, this module is definitely worth exploring.

## 📁 Module 1 – Creating Data Pipeline

<details>
<summary>Click to expand notes</summary>

### ✨ Source tab and Destination tab

- Built a Data Pipeline using NYC Taxi dataset 
- Ingested and organized data in preparation for transformation

In the source tab, we can select the source. I've chosen the NYC taxi trip.

![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20End-to-End%20Date%20Factory%20(Pipeline%20and%20Dataflow)/asset_creating_pipeline.png?raw=true)

In the destination tab, we can establish connection to "mylakehouse" (Lakehouse) and we name this as Bronze.

![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20End-to-End%20Date%20Factory%20(Pipeline%20and%20Dataflow)/asset_pipeline_destination_mapping.png?raw=true)

```sql
SELECT * FROM my_table;
```

</details>

## 📁 Module 2 – Transform data with a dataflow

<details>
<summary>Click to expand notes</summary>

### ✨ Transforming the data using Dataflow Gen2
-  Created a Dataflow to clean and optimize raw data
- Key steps:
    - Added  lpepPickupDate and lpepDropoff
    - Used check logics
    - Filtered "storeAndFwdFlag" = Y for discounts
    - Filtered "lpepPickUp" = (2015,1,1) to (2015,1,31)
    - Merged the discount table to Bronze table
    - Created a Discount Calculation logic using a conditional column:
	    - Added "TotalAmountAfterDiscount" column
      - Applied a rounding function with RoundingMode
    - Adding Data from Neon Console
