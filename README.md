# Real-Time Weather Analytics Dashboard

A complete end-to-end data engineering project that ingests real-time weather data from OpenWeatherMap, processes it with Spark Streaming, and visualizes it on a live Streamlit dashboard.

## 🏗️ Architecture

```mermaid
graph TD
    API[OpenWeatherMap API] -->|JSON| Producer[Python Producer]
    Producer -->|Topic: raw_api_events| Kafka[Kafka Broker]
    
    subgraph Processing Layer
        Kafka -->|Read Stream| Spark[Spark Streaming (PySpark)]
        Spark -->|Aggregation (3m Window)| Spark
        Spark -->|Topic: weather_aggregates| Kafka
    end
    
    subgraph Visualization Layer
        Kafka -->|Consume Stream| Dashboard[Streamlit Dashboard]
        Dashboard -->|Live Charts & Metrics| User[End User]
    end
```

## 🚀 Key Features

*   **Real-Time Ingestion**: Polls weather data for multiple cities every 15 seconds.
*   **Stream Processing**: Uses Spark Structured Streaming to calculate rolling averages, min/max temperatures, and wind speeds.
*   **Low Latency**: Optimized for near real-time updates (15s latency).
*   **Premium Dashboard**:
    *   **Dark Mode UI** with custom card styling.
    *   **Live Metrics**: Current Temperature, Min/Max (with deltas).
    *   **Interactive Charts**: Temperature trends and High Wind alerts using Plotly.
    *   **City Filter**: Sidebar control to focus on specific locations.
    *   **Auto-Refresh**: Automatically updates as new data arrives.

## 🛠️ Tech Stack

*   **Language**: Python 3.12
*   **Ingestion**: `kafka-python`, OpenWeatherMap API
*   **Message Broker**: Apache Kafka, Zookeeper (via Docker)
*   **Processing**: Apache Spark (PySpark)
*   **Visualization**: Streamlit, Plotly
*   **Infrastructure**: Docker, Docker Compose

## 📋 Prerequisites

*   Docker & Docker Compose
*   Python 3.12+
*   Java 17 (Required for Spark compatibility)

## ⚡ Quick Start

The project includes a helper script to automate the entire startup process.

1.  **Clone the repository** (if you haven't already).
2.  **Sourcing the environment** (optional but recommended):
    ```bash
    source .venv/bin/activate
    ```
3.  **Run the application**:
    ```bash
    ./scripts/run_all.sh
    ```
    This script will:
    *   Start Docker containers (Kafka, Zookeeper, Kafka UI).
    *   Start the API Producer in the background.
    *   Start the Spark Streaming App in the background.
    *   Launch the Streamlit Dashboard.

4.  **Access the Dashboard**:
    Open your browser at **[http://localhost:8501](http://localhost:8501)**.

## 🔧 Manual Setup

If you prefer to run components individually, use separate terminals:

1.  **Start Infrastructure**:
    ```bash
    docker compose -f docker/docker-compose.yml up -d
    ```

2.  **Start Producer**:
    ```bash
    source .venv/bin/activate
    python src/ingestion/api_producer.py
    ```

3.  **Start Spark Processor**:
    ```bash
    source .venv/bin/activate
    python src/processing/streaming_app.py
    ```

4.  **Start Dashboard**:
    ```bash
    source .venv/bin/activate
    streamlit run src/dashboard/app.py
    ```

## 📂 Project Structure

```
├── docker/
│   └── docker-compose.yml   # Kafka & Zookeeper configuration
├── scripts/
│   └── run_all.sh           # Automation script
├── src/
│   ├── dashboard/
│   │   └── app.py           # Streamlit Dashboard
│   ├── ingestion/
│   │   ├── api_client.py    # OpenWeatherMap Client
│   │   └── api_producer.py  # Kafka Producer
│   └── processing/
│       └── streaming_app.py # Spark Streaming Job
├── librairies/              # JAR dependencies (Spark/Kafka)
├── .env                     # Configuration (API Keys, Settings)
└── requirements.txt         # Python Dependencies

## 🔒 Security

This project uses environment variables to manage sensitive information like API keys.

1.  **Do not commit your `.env` file**. It is included in `.gitignore` by default.
2.  **Use the template**: A `.env.example` file is provided.
    ```bash
    cp .env.example .env
    ```
    Then, edit `.env` and add your actual API keys.
```
