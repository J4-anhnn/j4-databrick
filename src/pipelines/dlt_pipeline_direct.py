import dlt
from pyspark.sql.functions import col, when, to_date, udf
from pyspark.sql.types import StringType

# Constants
CSV_PATH = "dbfs:/FileStore/tables/sales_data.csv"
REVENUE_THRESHOLD = 100000

# Helper functions
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

@dlt.table(name="opportunity_raw_batch")
def opportunity_raw_batch():
    df = spark.read.option("header", True).option("inferSchema", True).csv(CSV_PATH)
    df = convert_boolean_columns(df)
    df = convert_date_columns(df)
    return df

@dlt.table(name="opportunity_enriched_batch")
def opportunity_enriched_batch():
    return (
        dlt.read("opportunity_raw_batch")
        .withColumn("Big_Deal", col("Forecasted_Monthly_Revenue") > REVENUE_THRESHOLD)
    )

@dlt.table(name="opportunity_masked_batch")
def opportunity_masked_batch():
    return (
        dlt.read("opportunity_enriched_batch")
        .withColumn("Salesperson", mask_udf(col("Salesperson")))
        .withColumn("Lead_Name", mask_udf(col("Lead_Name")))
    )

@dlt.table(name="opportunity_final_batch", partition_cols=["Region"])
def opportunity_final_batch():
    return dlt.read("opportunity_masked_batch")
