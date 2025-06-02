from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

IDENTIFIER = "serhii_mishovych"

# Spark Session
spark = SparkSession.builder \
    .appName("IoT Streaming Alerts") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema for JSON
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType())
])

# Read from Kafka
sensor_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", f"building_sensors_{IDENTIFIER}") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON
sensor_df = sensor_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select(
        col("data.sensor_id"),
        col("data.timestamp").cast(TimestampType()).alias("timestamp"),
        col("data.temperature"),
        col("data.humidity")
    )

# Watermark and window
windowed_avg = sensor_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("sensor_id")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )

# Load alert thresholds
alerts_df = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)

# Rename sensor_id in alerts_df to avoid ambiguity
alerts_df = alerts_df.withColumnRenamed("sensor_id", "alert_sensor_id")

# Join on sensor_id
joined = windowed_avg.join(alerts_df, windowed_avg.sensor_id == alerts_df.alert_sensor_id)

# Filter alerts
filtered_alerts = joined.filter(
    (
        ((col("avg_temperature") > col("max_temperature")) & (col("max_temperature") != -999)) |
        ((col("avg_temperature") < col("min_temperature")) & (col("min_temperature") != -999)) |
        ((col("avg_humidity") > col("max_humidity")) & (col("max_humidity") != -999)) |
        ((col("avg_humidity") < col("min_humidity")) & (col("min_humidity") != -999))
    )
).select(
    windowed_avg.sensor_id.alias("sensor_id"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("avg_temperature"),
    col("avg_humidity"),
    lit("THRESHOLD").alias("alert_code"),
    lit("Threshold exceeded").alias("alert_message")
)


# Write alerts to Kafka
query = filtered_alerts.selectExpr(
    "to_json(named_struct(" +
    "'sensor_id', sensor_id, " +
    "'window_start', window_start, " +
    "'window_end', window_end, " +
    "'avg_temperature', avg_temperature, " +
    "'avg_humidity', avg_humidity, " +
    "'alert_code', alert_code, " +
    "'alert_message', alert_message)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", f"alerts_{IDENTIFIER}") \
    .option("checkpointLocation", "./checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()
