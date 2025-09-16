from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

spark = SparkSession.builder.appName("app").getOrCreate()

yellow_df = spark.table("raw.yellow") \
    .select("VendorID", "passenger_count", "total_amount", 
            "tpep_pickup_datetime", "tpep_dropoff_datetime")

only_valid_vendors = yellow_df.filter(col("VendorID").isNotNull())

valid_passenger_cnt = only_valid_vendors.filter(col("passenger_count") > 0)

desired_range_only = valid_passenger_cnt.filter((year("tpep_pickup_datetime") == 2023) & (month("tpep_pickup_datetime") < 6))

spark.sql("CREATE SCHEMA IF NOT EXISTS trusted")

valid_passenger_cnt.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable("trusted.yellow")
