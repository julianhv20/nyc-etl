import logging
from pyspark.sql import SparkSession

def get_spark_session(app_name="NYC Taxi ETL"):
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger()