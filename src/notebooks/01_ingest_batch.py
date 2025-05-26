from pyspark.sql.functions import col, when, to_date

# Constants
CSV_PATH = "dbfs:/FileStore/tables/sales_data-1.csv"
TARGET_TABLE = "staging_opportunity_data"

# Helper functions
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

def main():
    # Read CSV file
    df = spark.read.option("header", True).option("inferSchema", True).csv(CSV_PATH)
    
    # Transform data
    df = convert_boolean_columns(df)
    df = convert_date_columns(df)
    
    # Display sample for verification
    df.show(5)
    
    # Save to staging table
    df.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    
    # Verify table creation
    display(spark.sql("SHOW TABLES"))

# Execute main function
main()
