from datetime import datetime

today=datetime.today()

CONN_ID="aws_s3"
RAW_DATA_NAME=f"raw/raw_data_{today}.json"
BUCKET_NAME="weather-data-pipeline-bucket1"
CITIES=["London", "Bogota", "New York", "Buenos Aires"]
API_key="dc4d36eaf2e989621f13e835fbc190a4"