import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, trim, lower

CONFIG = yaml.safe_load(open("/quick-commerece-pipeline/config/app_config.yaml"))
paths = CONFIG["paths"]
warehouse = paths["warehouse_dir"]
raw = paths["raw_dir"]

spark = (
    SparkSession.builder
    .appName(CONFIG["spark"]["app_name"] + "_batch")
    .getOrCreate()
)

def main():
    orders = spark.read.option("header", True).csv(os.path.join(raw, "orders.csv"))
    inventory = spark.read.json(os.path.join(raw, "inventory.json"))

    # Cast types
    orders = (orders
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("order_ts", col("order_ts").cast("timestamp"))
        .withColumn("status", lower(trim(col("status"))))
    )

    # Derived metrics
    orders = orders.withColumn("order_value", col("quantity") * col("price"))

    # Normalize status
    orders = orders.withColumn(
        "status_norm",
        when(col("status").isin("delivered","dispatched","cancelled","pending"), col("status"))
        .otherwise(expr("'pending'"))
    )
    inventory = inventory.select("item_id","name","category","warehouse","stock_level","last_update")
    joined = (orders.join(inventory, on="item_id", how="left")
              .withColumn("stock_level", col("stock_level").cast("int")))

    # Write fact table
    out = os.path.join(warehouse, "fact_orders")
    joined.write.mode("overwrite").parquet(out)
    print(f"Wrote fact_orders → {out}")

if __name__ == "__main__":
    main()
    spark.stop()
