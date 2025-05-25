import dlt
from pyspark.sql.functions import col, udf, when, to_date
from pyspark.sql.types import StringType

### -- BATCH PIPELINE (đọc từ Delta Table staging) -- ###

@dlt.table(name="opportunity_raw_batch")
def opportunity_raw_batch():
    return spark.read.table("staging_opportunity_data")

### -- STREAMING PIPELINE (Auto Loader với cloudFiles) -- ###

@dlt.table(name="opportunity_raw_streaming")
def opportunity_raw_streaming():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("inferSchema", True)
        .load("/tmp/j4_databrick_streaming")
    )

### -- ENRICH (Streaming) -- ###
@dlt.table(name="opportunity_enriched_streaming")
def opportunity_enriched_streaming():
    df = dlt.read_stream("opportunity_raw_streaming")
    for c in ["Closed_Opportunity", "Active_Opportunity", "Latest_Status_Entry"]:
        df = df.withColumn(c, when(col(c) == "TRUE", True).otherwise(False))
    df = df.withColumn("Date", to_date(col("Date"), "M/d/yyyy"))
    df = df.withColumn("Target_Close", to_date(col("Target_Close"), "M/d/yyyy"))
    df = df.withColumn("Big_Deal", col("Forecasted_Monthly_Revenue") > 100000)
    return df

### -- ENRICH (Batch) -- ###
@dlt.table(name="opportunity_enriched_batch")
def opportunity_enriched_batch():
    df = dlt.read("opportunity_raw_batch")
    df = df.withColumn("Big_Deal", col("Forecasted_Monthly_Revenue") > 100000)
    return df

### -- PRIVACY MASK UDF -- ###
def mask_text(text):
    return str(text)[0] + "***" if text else text

mask_udf = udf(mask_text, StringType())

### -- MASKING (Streaming) -- ###
@dlt.table(name="opportunity_masked_streaming")
def opportunity_masked_streaming():
    df = dlt.read_stream("opportunity_enriched_streaming")
    df = df.withColumn("Salesperson", mask_udf(col("Salesperson")))
    df = df.withColumn("Lead_Name", mask_udf(col("Lead_Name")))
    return df

### -- MASKING (Batch) -- ###
@dlt.table(name="opportunity_masked_batch")
def opportunity_masked_batch():
    df = dlt.read("opportunity_enriched_batch")
    df = df.withColumn("Salesperson", mask_udf(col("Salesperson")))
    df = df.withColumn("Lead_Name", mask_udf(col("Lead_Name")))
    return df

### -- FINAL TABLES (Partition for performance) -- ###

@dlt.table(name="opportunity_final_streaming", partition_cols=["Region"])
def opportunity_final_streaming():
    return dlt.read_stream("opportunity_masked_streaming")

@dlt.table(name="opportunity_final_batch", partition_cols=["Region"])
def opportunity_final_batch():
    return dlt.read("opportunity_masked_batch")
