from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from databricks.sdk.runtime import dbutils

spark = SparkSession.builder.appName("app").getOrCreate()


def merge_schemas_and_create_table(table_name, numeric_columns):
    """Due to parquet files with different schema that break when trying to create a table with all files.
    So this function unify it and create the table without this problem

    Args:
        table_name (str): Table to create, must be the same name of the directory created in S3 during ingestion
        numeric_columns (list): List of numeric fields that need normalization
    """

    files = [
        f.path
        for f in dbutils.fs.ls(f"s3://taxis-raw-data/{table_name}/")
        if f.name.endswith(".parquet")
    ]

    dfs = []
    for file_path in files:
        print(f"Reading {file_path}")
        temp_df = spark.read.parquet(file_path)
        if temp_df is not None:
            dfs.append(temp_df)
        else:
            print(f"Skipping problematic file: {file_path}")

    if dfs:
        df = dfs[0]

        for temp_df in dfs[1:]:
            df = df.union(temp_df)

        numeric_columns = [
            "VendorID",
            "passenger_count",
            "RatecodeID",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
        ]

        for column in numeric_columns:
            if column in df.columns:
                df = df.withColumn(column, col(column).cast("double"))

        df.write.format("parquet").mode("overwrite").option(
            "path", f"s3://taxis-raw-data/{table_name}_table_data/"
        ).saveAsTable(f"raw.{table_name}")
        print(f"Table raw.{table_name} created")
    else:
        print("No files were successfully read")
