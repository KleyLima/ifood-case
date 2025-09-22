from shared.create_table import merge_schemas_and_create_table

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

merge_schemas_and_create_table("yellow", numeric_columns)
