import os
import sys
from dotenv import load_dotenv

# Read configuration values (Kafka server, topic names, paths, etc.)
# from the .env file so they can be changed without editing the code.
load_dotenv()

# Unset SPARK_HOME to ensure we use the pyspark bundled version
# This must be done BEFORE importing pyspark
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# Spark requires Java 17; please ensure your JAVA_HOME environment
# variable points to a Java 17 installation before running this script.
# (Most systems already have this set, so no programmatic check is needed.)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, min, max as spark_max
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
except ImportError:
    print("Error: PySpark not found. Please ensure it is installed in your environment.")
    sys.exit(1)

# 1. Define the Schema based on OpenWeatherMap's JSON structure
schema = StructType([
    StructField("name", StringType()),
    StructField("dt", IntegerType()),  # Unix timestamp from API
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType())
    ])),
    StructField("wind", StructType([
        StructField("speed", DoubleType())
    ]))
])


def create_spark_session():
    """Initialize and return a Spark Session.

    SparkSession is the entry point to using Spark. We also add our own
    JAR files (Kafka connector, etc.) by pointing to the `librairies` folder.
    """
    print("Initializing Spark Session...")
    try:
        # Get absolute path to librairies folder
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        libs_path = os.path.join(project_root, "librairies")
        
        # List all jars
        jars = [os.path.join(libs_path, f) for f in os.listdir(libs_path) if f.endswith('.jar')]
        jars_str = ",".join(jars)

        spark = SparkSession.builder \
            .appName("WeatherStreamingApp") \
            .config("spark.jars", jars_str) \
            .config("spark.driver.extraClassPath", jars_str) \
            .config("spark.executor.extraClassPath", jars_str) \
            .config("spark.ui.prometheus.enabled", "true") \
            .config("spark.executor.processTreeMetrics.enabled", "true") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print("Spark Session Initialized.")
        return spark
    except Exception as e:
        print(f"Critical Error: Failed to create Spark Session: {e}")
        sys.exit(1)


def read_stream(spark):
    """Read data from Kafka.

    The raw topic is assumed to be `raw_api_events`. Offsets are tracked by
    checkpointing (see `CHECKPOINT_BASE` environment variable) so that the job
    can be restarted without data loss.
    """
    # This function returns a streaming DataFrame that continuously
    # pulls raw bytes from the Kafka topic. Spark handles connecting
    # and buffering for us.
    print("Setting up Kafka Read Stream...")
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_RAW_TOPIC", "raw_api_events")
    starting = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting) \
        .load()


def transform_raw(raw_df):
    """Convert the raw Kafka DataFrame into structured weather records.

    This constitutes the **silver** layer in our lakehouse: JSON is parsed,
    timestamps are computed, and minimal quality checks (drop nulls) are
    applied. The resulting DataFrame is used by both bronze storage and
    aggregation jobs.
    """
    # raw_df has binary messages; convert to string and parse JSON
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("dt")))

    parsed_df = parsed_df.dropna(subset=["main.temp"])
    return parsed_df


def aggregate(parsed_df):
    """Perform windowed aggregations (gold layer) on weather data.

    The resulting DataFrame flattens the window struct into `window_start` and
    `window_end` columns so downstream consumers (like the dashboard) can access
    them directly.
    """
    window_duration = os.getenv("SPARK_WINDOW_DURATION", "5 minutes")
    slide_duration = os.getenv("SPARK_SLIDE_DURATION", "1 minute")

    print(f"Aggregating data with window={window_duration}, slide={slide_duration}")

    aggregated_df = parsed_df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), window_duration, slide_duration),
            col("name")
        ) \
        .agg(
            avg("main.temp").alias("avg_temp"),
            spark_max("wind.speed").alias("max_wind_speed"),
            # include min and max temperature for dashboard metrics
            min("main.temp").alias("min_temp"),
            spark_max("main.temp").alias("max_temp")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "name",
            "avg_temp",
            "max_wind_speed",
            "min_temp",
            "max_temp"
        )

    return aggregated_df


def write_aggregated_to_kafka(aggregated_df):
    """Send aggregated (gold) records to Kafka topic.

    The checkpoint directory also stores window state so that rolling averages
    survive restarts. This corresponds to **Kafka Offsets** and **Window State**
    entries in the lifecycle table; both live under the same base checkpoint
    path.
    """
    print("Starting Streaming Query to Kafka (gold)...")

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_AGG_TOPIC", "weather_aggregates")
    checkpoint_base = os.getenv("CHECKPOINT_BASE", "checkpoints")
    checkpoint_location = os.path.join(checkpoint_base, "weather_agg")

    os.makedirs(checkpoint_location, exist_ok=True)

    kafka_df = aggregated_df.selectExpr(
        "CAST(name AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )

    return kafka_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("update") \
        .start()


def describe_lifecycle():
    """Print the lifecycle table for the different data types."""
    checkpoint_base = os.getenv("CHECKPOINT_BASE", "checkpoints")
    bronze_path = os.getenv("BRONZE_PATH", "data/bronze_weather")

    print("\nData lifecycle:\n")
    print(f"- Kafka Offsets & Window State: {checkpoint_base}/ (used for restartability and KPI continuity; retained after run)")
    print(f"- Raw Events: {bronze_path}/ (archived in parquet for lakehouse proofs; retained)")
    print("- Topic Messages: stored in Kafka via Docker volumes (auto-managed)")
    print("")


def ensure_directories():
    """Create directories for checkpoints and raw data if they do not exist."""
    checkpoint_base = os.getenv("CHECKPOINT_BASE", "checkpoints")
    bronze_path = os.getenv("BRONZE_PATH", "data/bronze_weather")

    os.makedirs(checkpoint_base, exist_ok=True)
    os.makedirs(bronze_path, exist_ok=True)
    print(f"Ensured directories: {checkpoint_base}, {bronze_path}")


def write_bronze(parsed_df):
    """Persist raw parsed events to disk (bronze layer)."""
    bronze_path = os.getenv("BRONZE_PATH", "data/bronze_weather")
    checkpoint_base = os.getenv("CHECKPOINT_BASE", "checkpoints")
    bronze_checkpoint = os.path.join(checkpoint_base, "bronze_storage")

    os.makedirs(bronze_checkpoint, exist_ok=True)

    return parsed_df.writeStream \
        .format("parquet") \
        .option("path", bronze_path) \
        .option("checkpointLocation", bronze_checkpoint) \
        .trigger(processingTime="1 minute") \
        .start()


def main():
    # show user where data will live and how lifecycle is handled
    describe_lifecycle()
    # prepare filesystem layout described in lifecycle table
    ensure_directories()
    spark = create_spark_session()

    try:
        raw_df = read_stream(spark)
        parsed_df = transform_raw(raw_df)

        # raw events archive
        bronze_q = write_bronze(parsed_df)

        # aggregated gold stream
        agg_df = aggregate(parsed_df)
        kafka_q = write_aggregated_to_kafka(agg_df)

        print("Queries started, awaiting termination...")
        spark.streams.awaitAnyTermination()
        print("All queries terminated.")

    except Exception as e:
        print(f"Stream processing error: {e}")
        import traceback
        traceback.print_exc()
    except KeyboardInterrupt:
        print("Stopping application...")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
