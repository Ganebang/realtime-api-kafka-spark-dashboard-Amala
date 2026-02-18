import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

load_dotenv()

# Force Java 17 to avoid incompatibility with Java 25 and Spark/Hadoop
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# 1. Define the Schema based on OpenWeatherMap's JSON structure
# This matches the "Explicit StructType schema" requirement in section 6.2
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

def main():
    try:
        print("Initializing Spark Session...")
        import pyspark
        print(f"PySpark Version: {pyspark.__version__}")
        print(f"PySpark Location: {pyspark.__file__}")
        print(f"SPARK_HOME: {os.environ.get('SPARK_HOME')}")

        # Initialize Spark Session with Kafka connector
        spark = SparkSession.builder \
            .appName("WeatherStreamingApp") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("Spark Session Initialized.")

        # 2. Read from Kafka (Requirement 6.2: readStream)
        print("Setting up Kafka Read Stream...")
        raw_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")) \
            .option("subscribe", "raw_api_events") \
            .option("startingOffsets", "latest") \
            .load()
        print("Kafka Read Stream setup.")

        # 3. Parse JSON and Convert Timestamp
        # We cast the binary value to string, then parse the JSON
        parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("event_time", to_timestamp(col("dt")))

        # 4. Apply Window Operations (Requirement 5.3 & 6.2)
        # - 5-minute sliding window, sliding every 1 minute
        # - 2-minute watermark to handle late data
        aggregated_df = parsed_df \
            .withWatermark("event_time", "2 minutes") \
            .groupBy(
                window(col("event_time"), "5 minutes", "1 minute"),
                col("name") # Business dimension: City
            ) \
            .agg(
                avg("main.temp").alias("avg_temp"),
                max("wind.speed").alias("max_wind_speed")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "name",
                "avg_temp",
                "max_wind_speed"
            )

        # 5. Output to Console for Validation (Step 7.4 requirement)
        # We use 'complete' mode to see the updated aggregation table
        print("Starting Streaming Query...")
        query = aggregated_df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .start()
        
        print("Query Started. Awaiting Termination...")
        query.awaitTermination()
        print("Query Terminated.")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()