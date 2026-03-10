from datetime import datetime

today=datetime.today()

CONN_ID="aws_s3"
RAW_DATA_NAME=f"raw/raw_data_{today}.json"
BUCKET_NAME="weather-data-pipeline-bucket1"