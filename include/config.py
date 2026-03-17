from airflow.models import Variable
from datetime import datetime

today=datetime.today()

CONN_ID="aws_s3"
POSTGRES_CONN_ID="postgres_default"
RAW_DATA_NAME="raw"
PROCESSED_FILE_NAME="processed"
BUCKET_NAME="weather-data-pipeline-bucket1"
CITIES=["London", "Bogota", "New York", "Buenos Aires", "Paris", "Madrid", "Milan", "Lima"]
API_key=Variable.get("OPENWEATHER_API_KEY")