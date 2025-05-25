from pyspark.sql.functions import col, when, to_date

csv_path = "../../sales_data.csv"  # chỉnh lại nếu notebook/as code di chuyển chỗ khác

df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

for c in ["Closed_Opportunity", "Active_Opportunity", "Latest_Status_Entry"]:
    df = df.withColumn(c, when(col(c) == "TRUE", True).otherwise(False))
df = df.withColumn("Date", to_date(col("Date"), "M/d/yyyy"))
df = df.withColumn("Target_Close", to_date(col("Target_Close"), "M/d/yyyy"))

df.write.format("delta").mode("overwrite").saveAsTable("staging_opportunity_data")
