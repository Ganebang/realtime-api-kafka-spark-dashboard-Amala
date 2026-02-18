import time
import json
import os
import datetime
import signal
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError
from api_client import OpenWeatherClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main():
    # 1. Configuration
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_name = "raw_api_events"
    cities_str = os.getenv("CITIES", "Paris,London,New York,Tokyo")
    cities = cities_str.split(",") if cities_str else []
    
    try:
        poll_interval = int(os.getenv("POLL_INTERVAL", "30"))
    except ValueError:
        print("Invalid POLL_INTERVAL, defaulting to 30s")
        poll_interval = 30

    if not cities:
        print("Error: No CITIES defined in environment variables.")
        return

    print(f"Configuration: Bootstrap Servers={bootstrap_servers}, Topic={topic_name}, Poll Interval={poll_interval}s")

    # 2. Initialize Kafka Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: str(v).encode('utf-8'),
            retries=5
        )
        print("Kafka Producer initialized successfully.")
    except Exception as e:
        print(f"Critical Error: Failed to initialize Kafka Producer: {e}")
        return

    # 3. Initialize API Client (will raise ValueError if key missing)
    try:
        client = OpenWeatherClient()
    except ValueError as e:
        print(f"Configuration Error: {e}")
        print("Producer shutting down. Please set OPENWEATHER_API_KEY in your environment.")
        return
    print(f"Starting API Producer. Polling {len(cities)} cities every {poll_interval}s...")

    # Graceful shutdown handler
    running = True
    def signal_handler(sig, frame):
        nonlocal running
        print("Shutdown signal received. Stopping producer...")
        running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while running:
            start_time = time.time()
            
            for city in cities:
                city = city.strip()
                if not city:
                    continue
                    
                try:
                    data = client.fetch_weather(city)
                    
                    if data:
                        # Use City Name as Kafka Key for partitioning
                        city_key = data.get("name")
                        
                        # Add ingestion timestamp (using timezone-aware UTC)
                        data['ingestion_timestamp'] = datetime.datetime.now(datetime.UTC).isoformat()
                        
                        producer.send(topic_name, key=city_key, value=data)
                        print(f"Sent data for {city_key} to Kafka. Timestamp: {data['ingestion_timestamp']}")
                    else:
                        print(f"Warning: No data received for {city}")
                        
                except Exception as e:
                    print(f"Error processing city {city}: {e}")
            
            try:
                producer.flush() # Ensure messages are sent
            except KafkaError as e:
                print(f"Error: Failed to flush messages to Kafka: {e}")

            # Sleep for the remainder of the interval
            elapsed = time.time() - start_time
            sleep_time = max(0, poll_interval - elapsed)
            if running and sleep_time > 0:
                time.sleep(sleep_time)
            
    except Exception as e:
        print(f"Critical Error in main loop: {e}")
    finally:
        print("Closing Kafka Producer...")
        producer.close()
        print("Producer closed. Bye!")

if __name__ == "__main__":
    main()