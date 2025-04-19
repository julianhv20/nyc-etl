import logging
from pyspark.sql import DataFrame
from utils import get_spark_session

def read_taxi_data(spark, source_path, years):
    try:
        logger = logging.getLogger(__name__)
        paths = [f"{source_path}year={year}/" for year in years]
        logger.info(f"Constructed paths for years {years}: {paths}")

        # Log the number of files/partitions found
        logger.info("Starting to read data from the specified paths...")
        df = spark.read.parquet(*paths)
        logger.info(f"Data read successfully. Number of partitions: {df.rdd.getNumPartitions()}")

        return df
    except Exception as e:
        logger.error(f"Error reading data: {e}", exc_info=True)
        raise RuntimeError(f"Error reading data: {e}")