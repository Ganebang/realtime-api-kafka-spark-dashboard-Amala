import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Unset SPARK_HOME to ensure we use the pyspark bundled version
# This must be done BEFORE importing pyspark
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# Force Java 17 to avoid incompatibility with Java 25 and Spark/Hadoop
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, max, min
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
    """Initialize and return a Spark Session."""
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
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print("Spark Session Initialized.")
        return spark
    except Exception as e:
        print(f"Critical Error: Failed to create Spark Session: {e}")
        sys.exit(1)

def read_stream(spark):
    """Read data from Kafka."""
    print("Setting up Kafka Read Stream...")
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "raw_api_events") \
        .option("startingOffsets", "latest") \
        .load()

def process_data(raw_df):
    """Parse JSON and aggregate data."""
    # window settings
    window_duration = os.getenv("SPARK_WINDOW_DURATION", "5 minutes")
    slide_duration = os.getenv("SPARK_SLIDE_DURATION", "1 minute")
    
    print(f"Processing data with window={window_duration}, slide={slide_duration}")

    # Parse JSON
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("dt")))

    # Apply Window Operations
    aggregated_df = parsed_df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), window_duration, slide_duration),
            col("name") # Business dimension: City
        ) \
        .agg(
            avg("main.temp").alias("avg_temp"),
            min("main.temp").alias("min_temp"),
            max("main.temp").alias("max_temp"),
            max("wind.speed").alias("max_wind_speed")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "name",
            "avg_temp",
            "min_temp",
            "max_temp",
            "max_wind_speed"
        )
    return aggregated_df

def write_stream(aggregated_df):
    """Output stream to Kafka."""
    print("Starting Streaming Query to Kafka...")
    
    # Prepare DataFrame for Kafka (key, value)
    kafka_df = aggregated_df.selectExpr(
        "CAST(name AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )
    
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "weather_aggregates"
    checkpoint_location = "/tmp/checkpoint_weather_agg"

    return kafka_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("update") \
        .start()

def main():
    spark = create_spark_session()
    
    try:
        raw_df = read_stream(spark)
        aggregated_df = process_data(raw_df)
        query = write_stream(aggregated_df)
        
        print("Query Started. Awaiting Termination...")
        query.awaitTermination()
        print("Query Terminated.")
        
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