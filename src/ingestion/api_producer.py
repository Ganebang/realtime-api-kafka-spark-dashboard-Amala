import time
import json
import os
import datetime
import signal
import sys
import logging
from typing import List
from kafka import KafkaProducer
from kafka.errors import KafkaError

from ingestion.api_client import OpenWeatherClient
from dotenv import load_dotenv

# Load configuration (API key, Kafka servers, etc.) from the .env file
load_dotenv()

# Configure logging for easier debugging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_config() -> dict:
    """Load and validate configuration from environment."""
    # default configuration values; environment variables override them
    cfg = {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "topic_name": os.getenv("KAFKA_RAW_TOPIC", "raw_api_events"),
        "cities": [],
        "poll_interval": 30,
    }

    cities_str = os.getenv("CITIES", "Paris,London,New York,Tokyo")
    cfg["cities"] = [c.strip() for c in cities_str.split(",") if c.strip()]

    try:
        cfg["poll_interval"] = int(os.getenv("POLL_INTERVAL", "30"))
    except ValueError:
        logger.warning("Invalid POLL_INTERVAL, defaulting to 30s")

    if not cfg["cities"]:
        logger.error("No CITIES defined in environment variables.")

    return cfg


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Initialize KafkaProducer with JSON serialization."""
    # KafkaProducer will connect to the broker and send bytes. We wrap
    # the data as JSON strings so that other programs (Spark, Streamlit) can
    # read it easily.
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda v: str(v).encode("utf-8"),
            retries=5,
        )
        logger.info("Kafka Producer initialized successfully.")
        return producer
    except Exception as e:
        logger.critical(f"Failed to initialize Kafka Producer: {e}")
        raise


def create_client() -> OpenWeatherClient:
    """Create an OpenWeatherClient, raising if the API key is missing."""
    try:
        client = OpenWeatherClient()
        return client
    except ValueError as e:
        logger.critical(e)
        raise


def publish_weather(
    producer: KafkaProducer, topic: str, data: dict
) -> None:
    """Send a single weather record to Kafka, adding ingestion metadata."""
    # use the city name as the message key so that Kafka partitions
    # by city (not required, but makes debugging easier)
    city_key = data.get("name")
    # add a timestamp showing when we pulled the data
    data["ingestion_timestamp"] = datetime.datetime.now(datetime.UTC).isoformat()
    producer.send(topic, key=city_key, value=data)
    logger.debug(f"Sent data for {city_key} to Kafka.")


def main_loop(cfg: dict, producer: KafkaProducer, client: OpenWeatherClient) -> None:
    """Main polling loop that gathers weather data and publishes it."""
    running = True

    def _signal_handler(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received. Stopping producer...")
        running = False

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        while running:
            start_time = time.time()
            for city in cfg["cities"]:
                try:
                    data = client.fetch_weather(city)
                except Exception as e:
                    logger.error(f"Error fetching {city}: {e}")
                    continue

                if data:
                    publish_weather(producer, cfg["topic_name"], data)
                else:
                    logger.warning(f"No data received for {city}")

            try:
                producer.flush()
            except KafkaError as e:
                logger.error(f"Failed to flush messages to Kafka: {e}")

            elapsed = time.time() - start_time
            sleep_time = max(0, cfg["poll_interval"] - elapsed)
            if running and sleep_time > 0:
                time.sleep(sleep_time)
    except Exception as e:
        logger.critical(f"Critical error in main loop: {e}")
        raise
    finally:
        logger.info("Closing Kafka Producer...")
        producer.close()
        logger.info("Producer closed. Bye!")


def main():
    cfg = load_config()
    if not cfg["cities"]:
        sys.exit(1)

    try:
        producer = create_producer(cfg["bootstrap_servers"])
        client = create_client()
    except Exception:
        sys.exit(1)

    logger.info(
        f"Starting API Producer. Polling {len(cfg['cities'])} cities every {cfg['poll_interval']}s..."
    )

    main_loop(cfg, producer, client)


if __name__ == "__main__":
    main()
