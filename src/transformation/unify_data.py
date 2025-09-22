from pyspark.sql.functions import lit, month
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("app").getOrCreate()


green_data = spark.table("trusted.green")
green_data_label = green_data.withColumn("cab_type", lit("green"))
green_data_renamed = green_data_label.withColumnRenamed(
    "lpep_pickup_datetime", "pickup_datetime"
).withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")


yellow_data = spark.table("trusted.yellow")
yellow_data_label = yellow_data.withColumn("cab_type", lit("yellow"))
yellow_data_renamed = yellow_data_label.withColumnRenamed(
    "tpep_pickup_datetime", "pickup_datetime"
).withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

taxi_data_unified = yellow_data_renamed.unionByName(green_data_renamed)

taxi_data_unified = taxi_data_unified.withColumn("month", month("pickup_datetime"))

taxi_data_unified.write.mode("overwrite").format("delta").saveAsTable(
    "trusted.taxi_data_unified"
)
