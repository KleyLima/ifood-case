from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

spark = SparkSession.builder.appName("app").getOrCreate()

green_df = spark.table("raw.green") \
    .select("VendorID", "passenger_count", "total_amount", 
            "lpep_pickup_datetime", "lpep_dropoff_datetime")

only_valid_vendors = green_df.filter(col("VendorID").isNotNull())

valid_passenger_cnt = only_valid_vendors.filter(col("passenger_count") > 0)

desired_range_only = valid_passenger_cnt.filter((year("lpep_pickup_datetime") == 2023) & (month("lpep_pickup_datetime") < 6))

spark.sql("CREATE SCHEMA IF NOT EXISTS trusted")

valid_passenger_cnt.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("trusted.green")
