# Real-Time Weather Analytics Dashboard

A complete end-to-end data engineering project that ingests real-time weather data from OpenWeatherMap, processes it with Spark Streaming, and visualizes it on a live Streamlit dashboard.

## 🏗️ Architecture

![Dashboard screenshot](screenshots/project_img.png)


```mermaid
graph TD
    API[OpenWeatherMap API] -->|JSON| Producer[Python Producer]
    Producer -->|raw_api_events| Kafka[Kafka Broker]

    subgraph Processing Layer
        Kafka -->|read| Spark1[Streaming]
        Spark1 -->|aggregate 5m| Spark2[Aggregator]
        Spark2 -->|weather_aggregates| Kafka
    end

    subgraph Visualization Layer
        Kafka -->|consume| Dashboard[Streamlit Dashboard]
        Dashboard -->|charts| User[End User]
    end
```

## 🚀 Key Features

*   **Real-Time Ingestion**: Polls weather data for multiple cities every 15 seconds (configurable via `.env`).
*   **Stream Processing**: Uses Spark Structured Streaming to calculate rolling averages, min/max temperatures, and wind speeds.
*   **Low Latency**: Optimized for near real-time updates (15s latency).
*   **Data Persistence**: Raw messages are archived under `data/`, and processing state is kept in `checkpoints/` for reliable restarts.
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
*   **Visualization**: Streamlit, Plotly (dashboard now shows average, min, and max temperatures plus wind speed)
*   **Infrastructure**: Docker, Docker Compose

## 📋 Prerequisites

*   Docker & Docker Compose
*   Python 3.12+
*   Java 17 (Required for Spark compatibility)

## ⚡ Quick Start

The project includes a helper script to automate the entire startup process.

1.  **Clone the repository** (if you haven't already).
2.  **Create & populate your `.env`**
    ```bash
    cp .env.example .env
    # then open .env and set your personal OpenWeather API key
    # e.g. OPENWEATHER_API_KEY=your_api_key_here
    ```
    The producer will fail with a clear error if the key is missing.
3.  **Activate the virtual environment** (optional but recommended):
    ```bash
    source .venv/bin/activate
    ```
4.  **Run the application**:
    ```bash
    ./scripts/run_all.sh
    ```
    This script will automatically:
    *   Start Docker containers (Kafka, Zookeeper, Kafka UI).
    *   Start the API Producer in the background.
    *   Start the Spark Streaming App in the background.
    *   Launch the Streamlit Dashboard. Metrics include the current average temperature along with minimum and maximum readings for each city.

5.  **Access the Dashboard**:
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
├── data/                    # Bronze parquet storage (raw events)
├── checkpoints/             # Spark & Kafka offset state
├── screenshots/             # Example images and documentation
├── .env                     # Configuration (API Keys, Settings)
├── .env.example             # Template for environment variables
└── requirements.txt         # Python Dependencies

## 🔒 Security

This project uses environment variables to manage sensitive information like API keys.

1.  **Do not commit your `.env` file**. It is included in `.gitignore` by default.
2.  **Use the template**: A `.env.example` file is provided.
    ```bash
    cp .env.example .env
    ```
    Then, edit `.env` and add your actual API keys. At minimum you must populate `OPENWEATHER_API_KEY`. If this value is empty the producer will log 401 errors (see `producer.log`) and the dashboard will not receive any data.
3.  **Restart services** after modifying the `.env` file so that the producer picks up the new values.
```
