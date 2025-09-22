from pyspark.sql.functions import hour, year, month, avg, round
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("app").getOrCreate()

df = spark.table("trusted.taxi_data_unified")


result_df = (
    df.groupBy(hour("pickup_datetime").alias("hour"))
    .agg(round(avg("passenger_count"), 2).alias("average_count"))
    .orderBy("hour")
)

result_df.write.mode("overwrite").format("delta").saveAsTable(
    "trusted.passenger_by_hour"
)
