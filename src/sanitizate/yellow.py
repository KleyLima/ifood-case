from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

spark = SparkSession.builder.appName("app").getOrCreate()

yellow_df = spark.table("raw.yellow").select(
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
)

only_valid_vendors = yellow_df.filter(
    col("VendorID").isNotNull() & col("VendorID").isin([1, 2, 6, 7])
)
print("Removed nullable and invalid vendor IDS")

valid_passenger_cnt = only_valid_vendors.filter(col("passenger_count") > 0)
print("Removing invalid passenger count")

desired_range_only = valid_passenger_cnt.filter(
    (year("tpep_pickup_datetime") == 2023) & (month("tpep_pickup_datetime") < 6)
)
print("Keeping data on desired range (January to May of 2023)")

only_valid_datatime = desired_range_only.filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
print("Validating departure and arriving order")

spark.sql("CREATE SCHEMA IF NOT EXISTS trusted")
print("Schema created")

only_valid_datatime.write.mode("overwrite").format("delta").saveAsTable(
    "trusted.yellow"
)
print("Table created")
