import yaml, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

CONFIG = yaml.safe_load(open("/quick-commerece-pipeline/config/app_config.yaml"))
BOOTSTRAP = CONFIG["spark"]["kafka_bootstrap"]
paths = CONFIG["paths"]
warehouse = paths["warehouse_dir"]

spark = (
    SparkSession.builder
    .appName(CONFIG["spark"]["app_name"] + "_streaming")
    .getOrCreate()
)

gps_schema = StructType([
    StructField("courier_id", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("ts", StringType()),
])

status_schema = StructType([
    StructField("order_id", StringType()),
    StructField("courier_id", StringType()),
    StructField("status", StringType()),
    StructField("ts", StringType()),
])

def main():
    gps_raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", CONFIG["kafka"]["gps_topic"])
        .option("startingOffsets", "latest")
        .load())

    gps = (gps_raw
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), gps_schema).alias("d"))
        .select("d.*"))

    status_raw = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", CONFIG["kafka"]["status_topic"])
        .option("startingOffsets", "latest")
        .load())

    status = (status_raw
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), status_schema).alias("d"))
        .select("d.*"))

    # Example aggregation: Count status per courier
    status_counts = (status
        .groupBy("courier_id", "status")
        .count())

    query = (status_counts
        .writeStream
        .outputMode("complete")
        .format("parquet")
        .option("path", os.path.join(warehouse, "status_counts"))
        .option("checkpointLocation", os.path.join(warehouse, "_chk_status_counts"))
        .start())

    query.awaitTermination()

if __name__ == "__main__":
    main()
    spark.stop()
