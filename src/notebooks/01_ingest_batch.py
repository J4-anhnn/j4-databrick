from pyspark.sql.functions import col, when, to_date

# Đường dẫn file
csv_path = "dbfs:/FileStore/tables/sales_data-1.csv"  

# Đọc file CSV
df = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)

# Chuyển đổi boolean
for c in ["Closed_Opportunity", "Active_Opportunity", "Latest_Status_Entry"]:
    df = df.withColumn(c, when(col(c) == "TRUE", True).otherwise(False))

# Chuyển đổi date  
df = df.withColumn("Date", to_date(col("Date"), "M/d/yyyy"))
df = df.withColumn("Target_Close", to_date(col("Target_Close"), "M/d/yyyy"))

# Hiển thị 5 dòng kiểm tra
df.show(5)

# Lưu bảng staging
df.write.format("delta").mode("overwrite").saveAsTable("staging_opportunity_data")

# Kiểm tra bảng đã tạo
display(spark.sql("SHOW TABLES"))