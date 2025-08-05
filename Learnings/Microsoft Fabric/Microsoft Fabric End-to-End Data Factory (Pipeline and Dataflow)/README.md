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

This code acts as CREATE OR REPLACE for the 2 columns  "lpepPickup" and "lpepDropoff"
```sql
	 ListZip = List.Zip({ListofAutomaticallySelectedDateColumns, TransformDateColumntoExtractJustDatewithprefix}),

        InsertedDateColumns = List.Accumulate(

                                  ListZip,

                                  ReinstateSource,

                                  (state, pair) =>

                                  //checks and avoids duplicate column names inside list.accumulate. note: if another column the satifies the requirement of the name datetime logic then it wwwwill not cause error.\

                                    let

                                       cleanState = if List.Contains(Table.ColumnNames(state), pair{1}) //this line checks

                                                    then Table.Removecolumns(state, {pair{1}})

                                                    else state

                                    in

                                    Table.AddColumn(

                                      cleanState,

                                      pair{1},

                                      each Date.From(Record.Field(_, pair{0})),

                                      type date

                                      )

                                      ),

  #"Filtered rows" = Table.SelectRows(InsertedDateColumns, each ([storeAndFwdFlag] = "Y")),
```

This the code from the Advanced Editor that transformed the Bronze Table.

![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20End-to-End%20Date%20Factory%20(Pipeline%20and%20Dataflow)/asset_AdvancedEditor.png?raw=true)

Using Power Query UI, Get New Data > Text/CSV

Reference: https://raw.githubusercontent.com/ekote/azure-architect/master/Generated-NYC-Taxi-Green-Discounts.csv

![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20End-to-End%20Date%20Factory%20(Pipeline%20and%20Dataflow)/asset_Discounttablepowerquery.png?raw=true)

Merging the Bronze table with Generated-NYC-Taxi-Green-Discount
    -  I selected vendor ID and lpepPickup from the Bronze table and VendorID and Date from the discount table to make the connection.
![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20End-to-End%20Date%20Factory%20(Pipeline%20and%20Dataflow)/asset_mergedbronzedtableanddiscounttable.png?raw=true)

Creating the Discount in the merged as new query (fact_table)
![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20End-to-End%20Date%20Factory%20(Pipeline%20and%20Dataflow)/asset_fact_table_flow.png?raw=true)

Added some Rounding function in TotaAmountAfterDiscount column

![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20End-to-End%20Date%20Factory%20(Pipeline%20and%20Dataflow)/aasset_conditionalcolumnfordiscount.png?raw=true)

Connecting the fact_table to its data destination 
    - My traget data destination here is the Lakehouse and I've set the default to mylakehouse just to differentiate but they are just the same. 
    
![Alt text](https://github.com/RenzieCoding/View_Portfolio/blob/main/Images/Microsoft%20Fabric/Microsoft%20Fabric%20End-to-End%20Date%20Factory%20(Pipeline%20and%20Dataflow)/asset_connectingtothelakehousedestination.png?raw=true)



