#!/bin/bash

# Exit on error
set -e

# Load environment variables
if [ -f .env ]; then
    source .env
fi

echo "Starting Real-Time Dashboard Environment..."

# 1. Start Docker Containers
echo "Step 1: Starting Docker Containers..."
docker compose -f docker/docker-compose.yml up -d

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 20  # Simple wait, could be improved with healthchecks

# 2. Check if virtual environment is active, if not activate it
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# 3. Start Producer in Background
echo "Step 2: Starting API Producer (Background)..."
python src/ingestion/api_producer.py > producer.log 2>&1 &
PRODUCER_PID=$!
echo "Producer running with PID $PRODUCER_PID. Logs in producer.log"

# 4. Start Spark Streaming App
echo "Step 3: Starting Spark Streaming App (Background)..."
python src/processing/streaming_app.py > spark_app.log 2>&1 &
SPARK_PID=$!
echo "Spark App running with PID $SPARK_PID. Logs in spark_app.log"

# 5. Start Dashboard
echo "Step 4: Starting Dashboard..."
echo "Press Ctrl+C to stop all services."

trap "kill $PRODUCER_PID $SPARK_PID; exit" INT TERM

streamlit run src/dashboard/app.py --server.headless=true
