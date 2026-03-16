from datetime import datetime

today=datetime.today()

CONN_ID="aws_s3"
POSTGRES_CONN_ID="postgres_default"
RAW_DATA_NAME=f"raw/raw_data_{today}.json"
CSV_FILE_NAME=f"processed/processed_data_{today}.csv"
BUCKET_NAME="weather-data-pipeline-bucket1"
CITIES=["London", "Bogota", "New York", "Buenos Aires"]
API_key="dc4d36eaf2e989621f13e835fbc190a4"