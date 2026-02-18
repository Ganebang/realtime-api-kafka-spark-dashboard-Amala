import time
import json
import os
from kafka import KafkaProducer
from api_client import OpenWeatherClient
from dotenv import load_dotenv

load_dotenv()

def main():
    # 1. Configuration
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_name = "raw_api_events"
    cities = os.getenv("CITIES", "Paris,London,New York,Tokyo").split(",")
    poll_interval = 30 # Seconds between API calls

    # 2. Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: str(v).encode('utf-8')
    )

    client = OpenWeatherClient()
    print(f"Starting API Producer. Polling {len(cities)} cities every {poll_interval}s...")

    try:
        while True:
            for city in cities:
                data = client.fetch_weather(city.strip())
                
                if data:
                    # Use City Name as Kafka Key for partitioning
                    city_key = data.get("name")
                    producer.send(topic_name, key=city_key, value=data)
                    print(f"Sent data for {city_key} to Kafka.")
            
            producer.flush() # Ensure messages are sent
            time.sleep(poll_interval)
            
    except KeyboardInterrupt:
        print("Stopping Producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()