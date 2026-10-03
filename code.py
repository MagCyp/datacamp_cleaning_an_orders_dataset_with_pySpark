from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_state(address_col):
    last_part = F.element_at(F.split(address_col, ","), -1)

    return F.substring(F.trim(last_part), 1, 2)


spark = SparkSession.builder.appName(
    "cleaning_orders_dataset_with_pyspark"
).getOrCreate()


orders_data = spark.read.parquet("orders_data.parquet")


cleaned_data = (
    orders_data
    # Remove orders from 12am through 5am inclusive
    .filter(
        ~(
            (F.date_format("order_date", "HH:mm:ss") >= "00:00:00")
            & (F.date_format("order_date", "HH:mm:ss") <= "05:00:00")
        )
    )
    # Create time_of_day before removing time from order_date
    .withColumn(
        "time_of_day",
        F.when(F.hour("order_date") < 12, "morning")
        .when(F.hour("order_date") < 18, "afternoon")
        .otherwise("evening"),
    )
    # Convert timestamp to date
    .withColumn("order_date", F.to_date("order_date"))
    # Remove TV products
    .filter(~F.lower(F.col("product")).contains("tv"))
    # Lowercase product
    .withColumn("product", F.lower(F.col("product")))
    # Lowercase category
    .withColumn("category", F.lower(F.col("category")))
    # Extract state from purchase_address
    .withColumn("purchase_state", get_state(F.col("purchase_address")))
)


cleaned_data.write.mode("overwrite").parquet("orders_data_clean.parquet")
