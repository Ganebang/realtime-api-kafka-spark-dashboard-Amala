#!/bin/bash

# Exit on error
set -e

# Load environment variables
if [ -f .env ]; then
    source .env
fi

echo "Starting Real-Time Dashboard Environment..."

# optional cleanup of previous state to reset checkpoints and storage
echo "Cleaning up previous state..."
# stop and remove docker containers/volumes if any
docker compose -f docker/docker-compose.yml down -v || true
# remove any existing checkpoint or bronze data to start fresh
rm -rf checkpoints/* data/bronze_weather/* || true


# 1. Start Docker Containers
echo "Step 1: Starting Docker Containers..."
docker compose -f docker/docker-compose.yml up -d

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 20  # Simple wait, could be improved with healthchecks

# ensure required topics exist (auto.create.topics may be disabled)
echo "Creating Kafka topics if they do not exist..."
docker exec kafka kafka-topics --create --topic raw_api_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true
docker exec kafka kafka-topics --create --topic weather_aggregates --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true

# 2. Check if virtual environment is active, if not activate it
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# 3. Start Producer in Background
echo "Step 2: Starting API Producer (Background)..."
# ensure python can import the package modules
export PYTHONPATH="${PYTHONPATH:-}:/$(pwd)/src"
# run as module to respect package layout
python -m ingestion.api_producer > producer.log 2>&1 &
PRODUCER_PID=$!
echo "Producer running with PID $PRODUCER_PID. Logs in producer.log"

# 4. Start Spark Streaming App
# (also needs PYTHONPATH to import modules correctly)
echo "Step 3: Starting Spark Streaming App (Background)..."
export PYTHONPATH="${PYTHONPATH:-}:/$(pwd)/src"
python src/processing/streaming_app.py > spark_app.log 2>&1 &
SPARK_PID=$!
echo "Spark App running with PID $SPARK_PID. Logs in spark_app.log"

# 5. Start Dashboard
echo "Step 4: Starting Dashboard..."
echo "Press Ctrl+C to stop all services."

trap "kill $PRODUCER_PID $SPARK_PID; exit" INT TERM

streamlit run src/dashboard/app.py --server.headless=true
