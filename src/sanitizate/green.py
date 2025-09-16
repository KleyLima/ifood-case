from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("app").getOrCreate()

green_df = spark.table("raw.green") \
    .select("VendorID", "passenger_count", "total_amount", 
            "lpep_pickup_datetime", "lpep_dropoff_datetime")

only_valid_vendors = green_df.filter(col("VendorID").isNotNull())

valid_passenger_cnt = only_valid_vendors.filter(col("passenger_count") > 0)

spark.sql("CREATE SCHEMA IF NOT EXISTS trusted")

valid_passenger_cnt.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("trusted.green")
