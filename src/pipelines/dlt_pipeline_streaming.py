import dlt
from pyspark.sql.functions import col, when, to_date, udf, current_timestamp
from pyspark.sql.types import StringType

REVENUE_THRESHOLD = 100000
# SCHEMA_CHECKPOINT_PATH = "dbfs:/tmp/j4_schema_checkpoint"
SCHEMA_CHECKPOINT_PATH = dbutils.secrets.get(scope="j4-secrets", key="schema_checkpoint_path")
# STREAMING_SOURCE_PATH = "dbfs:/tmp/j4_databrick_streaming/"
STREAMING_SOURCE_PATH = dbutils.secrets.get(scope="j4-secrets", key="streaming_source_path")

def mask_text(text):
    """Mask PII data by keeping first character and replacing rest with asterisks"""
    return str(text)[0] + "***" if text else text

mask_udf = udf(mask_text, StringType())

def convert_boolean_columns(df):
    """Convert string boolean values to actual boolean type"""
    boolean_columns = ["Closed_Opportunity", "Active_Opportunity", "Latest_Status_Entry"]
    for column in boolean_columns:
        df = df.withColumn(column, when(col(column) == "TRUE", True).otherwise(False))
    return df

def convert_date_columns(df):
    """Convert string dates to date type"""
    date_columns = {
        "Date": "M/d/yyyy",
        "Target_Close": "M/d/yyyy"
    }
    for column, format in date_columns.items():
        df = df.withColumn(column, to_date(col(column), format))
    return df

# 1. Streaming source table
@dlt.table(name="opportunity_raw_streaming")
@dlt.expect_or_drop("valid_lead", "Lead_Name IS NOT NULL")
def opportunity_raw_streaming():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_CHECKPOINT_PATH)
        .option("header", "true")
        .load(STREAMING_SOURCE_PATH)
        .withColumn("processing_time", current_timestamp())
        .transform(convert_boolean_columns)
        .transform(convert_date_columns)
    )

# 2. Enriched streaming table
@dlt.table(name="opportunity_enriched_streaming")
def opportunity_enriched_streaming():
    return (
        dlt.read_stream("opportunity_raw_streaming")
        .withColumn("Big_Deal", col("Forecasted_Monthly_Revenue") > REVENUE_THRESHOLD)
    )

# 3. Masked streaming table
@dlt.table(name="opportunity_masked_streaming")
def opportunity_masked_streaming():
    return (
        dlt.read_stream("opportunity_enriched_streaming")
        .withColumn("Salesperson", mask_udf(col("Salesperson")))
        .withColumn("Lead_Name", mask_udf(col("Lead_Name")))
    )

# 4. Final streaming table with partitioning
@dlt.table(name="opportunity_final_streaming", partition_cols=["Region"])
def opportunity_final_streaming():
    return dlt.read_stream("opportunity_masked_streaming")
