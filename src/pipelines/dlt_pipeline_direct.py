import dlt
from pyspark.sql.functions import col, when, to_date, udf
from pyspark.sql.types import StringType

@dlt.table(name="opportunity_raw_batch")
def opportunity_raw_batch():
    csv_path = "dbfs:/FileStore/tables/sales_data.csv"
    
    df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
    
    for c in ["Closed_Opportunity", "Active_Opportunity", "Latest_Status_Entry"]:
        df = df.withColumn(c, when(col(c) == "TRUE", True).otherwise(False))
    
    df = df.withColumn("Date", to_date(col("Date"), "M/d/yyyy"))
    df = df.withColumn("Target_Close", to_date(col("Target_Close"), "M/d/yyyy"))
    
    return df

def mask_text(text):
    return str(text)[0] + "***" if text else text

mask_udf = udf(mask_text, StringType())

@dlt.table(name="opportunity_enriched_batch")
def opportunity_enriched_batch():
    df = dlt.read("opportunity_raw_batch")
    df = df.withColumn("Big_Deal", col("Forecasted_Monthly_Revenue") > 100000)
    return df

@dlt.table(name="opportunity_masked_batch")
def opportunity_masked_batch():
    df = dlt.read("opportunity_enriched_batch")
    df = df.withColumn("Salesperson", mask_udf(col("Salesperson")))
    df = df.withColumn("Lead_Name", mask_udf(col("Lead_Name")))
    return df

@dlt.table(name="opportunity_final_batch", partition_cols=["Region"])
def opportunity_final_batch():
    return dlt.read("opportunity_masked_batch")
