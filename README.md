# 🌦️ Weather Data Pipeline

> An automated, end-to-end data engineering pipeline that ingests real-time weather data from the OpenWeatherMap API, processes it through AWS S3, and loads it into a PostgreSQL database — orchestrated with Apache Airflow on Astronomer.

---

## 📐 Architecture Overview

```
OpenWeatherMap API
        │
        ▼
┌───────────────────┐
│  fetch_and_store  │  ← Concurrent city fetching (ThreadPoolExecutor)
│   (Airflow Task)  │    Stores raw JSON to S3 (partitioned by date)
└────────┬──────────┘
         │
         ▼
┌────────────────────────┐
│ transform_weather_data │  ← Reads raw JSON from S3
│     (Airflow Task)     │    Flattens & transforms into tabular CSV
└────────┬───────────────┘    Writes processed CSV back to S3
         │
         ▼
┌──────────────────┐
│ load_to_postgres │  ← Reads processed CSV from S3
│  (Airflow Task)  │    Appends to PostgreSQL `weather` table
└──────────────────┘
```

The DAG runs **hourly**, collecting weather data for 8 cities across 4 continents.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Orchestration** | Apache Airflow 3.x (Astronomer Runtime) |
| **Containerization** | Docker |
| **Cloud Storage** | AWS S3 |
| **Database** | PostgreSQL |
| **Language** | Python 3.12 |
| **Key Libraries** | `pandas`, `requests`, `apache-airflow-providers-amazon`, `apache-airflow-providers-postgres` |
| **Testing** | `pytest` |

---

## 📁 Project Structure

```
WeatherDataPipeline/
├── dags/
│   └── weather_data_pipeline.py   # Airflow DAG definition (TaskFlow API)
├── include/
│   ├── config.py                  # Centralized config: connections, bucket names, cities
│   └── longtasks.py               # Core logic: API fetching & data transformation
├── tests/
│   └── dags/
│       └── test_weather_data_pipeline.py  # DAG integrity + unit tests
├── Dockerfile                     # Astronomer Runtime base image
├── requirements.txt               # Python dependencies
└── packages.txt                   # OS-level dependencies
```

---

## ⚙️ Pipeline Tasks

### 1. `fetch_and_store`
- Uses the **OpenWeatherMap Geocoding API** to resolve city names to coordinates.
- Fetches city coordinates concurrently via `ThreadPoolExecutor` (5 workers) for efficiency.
- Calls the **Current Weather API** for each city.
- Serializes the response as JSON and uploads it to S3 under a **Hive-style partitioned path**:
  ```
  raw/year=YYYY/month=MM/day=DD/weather_YYYYMMDD_HHMMSS.json
  ```
- Returns the S3 file key for the next task.

### 2. `transform_weather_data`
- Reads the raw JSON file from S3.
- Flattens the nested weather response into a clean, tabular pandas DataFrame.
- Converts UNIX timestamps to UTC-aware datetime objects.
- Uploads the processed data as CSV to S3 under:
  ```
  processed/year=YYYY/month=MM/day=DD/weather_YYYYMMDD_HHMMSS.csv
  ```
- Returns the processed CSV key for the next task.

### 3. `load_to_postgres`
- Reads the processed CSV from S3 into a pandas DataFrame.
- Appends all records to the `weather` table in PostgreSQL using SQLAlchemy.

---

## 🏙️ Monitored Cities

| City | Region |
|---|---|
| London | Europe |
| Madrid | Europe |
| Paris | Europe |
| Milan | Europe |
| Bogotá | South America |
| Buenos Aires | South America |
| Lima | South America |
| New York | North America |

---

## 🚀 Getting Started

### Prerequisites

- [Docker](https://www.docker.com/)
- [Astronomer CLI](https://www.astronomer.io/docs/astro/cli/install-cli) (`astro`)
- An [OpenWeatherMap API key](https://openweathermap.org/api) (free tier works)
- AWS credentials with S3 read/write access
- A PostgreSQL instance

### 1. Clone the Repository

```bash
git clone https://github.com/SantiOrtizQ/WeatherDataPipeline.git
cd WeatherDataPipeline
```

### 2. Start Local Airflow

```bash
astro dev start
```

This spins up 5 Docker containers (Postgres metadata DB, Scheduler, DAG Processor, API Server, Triggerer). The Airflow UI will be available at [http://localhost:8080](http://localhost:8080).

### 3. Configure Airflow Connections & Variables

In the Airflow UI, add the following:

**Connections:**
| Conn ID | Type | Description |
|---|---|---|
| `aws_s3` | Amazon Web Services | AWS credentials for S3 access |
| `postgres_default` | Postgres | Target PostgreSQL database |

**Variables:**
| Key | Value |
|---|---|
| `OPENWEATHER_API_KEY` | Your OpenWeatherMap API key |

### 4. Configure the S3 Bucket

Update `include/config.py` with your own S3 bucket name:

```python
BUCKET_NAME = "your-s3-bucket-name"
```

### 5. Trigger the DAG

In the Airflow UI, enable and trigger the `weather_data_pipeline` DAG. It is scheduled to run **@hourly**.

---

## 🧪 Running Tests

```bash
astro dev pytest
```

The test suite covers:

- **DAG integrity**: Validates that all DAGs load without import errors.
- **`transform_data` unit test**: Asserts the transformation function produces a non-empty DataFrame with the expected columns, given a known input payload.

---

## 🔧 Configuration Reference

All configurable values are centralized in `include/config.py`:

```python
CONN_ID           = "aws_s3"
POSTGRES_CONN_ID  = "postgres_default"
RAW_DATA_NAME     = "raw"
PROCESSED_FILE_NAME = "processed"
BUCKET_NAME       = "weather-data-pipeline-bucket1"
CITIES            = ["London", "Bogota", "New York", "Buenos Aires",
                     "Paris", "Madrid", "Milan", "Lima"]
API_key           = Variable.get("OPENWEATHER_API_KEY", default_var=None)
```

---

## 🔄 Data Flow Summary

```
Raw API Response (JSON)
    └── city → { weather: { temp, humidity, pressure, temp_max,
                             temp_min, sea_level, feels_like,
                             grnd_level, timestamp } }

Transformed Output (CSV columns)
    └── city | temp | humidity | pressure | temp_max | temp_min |
             sea_level | feels_like | grnd_level | timestamp (UTC)
```

---

## 📦 Dependencies

```
apache-airflow-providers-amazon==9.21.0
apache-airflow-providers-postgres==6.6.0
```

Base image: `astrocrpublic.azurecr.io/runtime:3.1-13`

---

## Author

Santiago Ortiz
Engineering Physicist | Data Analyst | Prospective Data Engineer | Python · SQL · Power BI
