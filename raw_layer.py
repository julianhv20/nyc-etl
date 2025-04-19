from data_reader import read_taxi_data
import logging

def process_raw_layer(spark, source_path, output_path, years):
    logger = logging.getLogger()
    try:
        logger.info("Starting Raw Layer processing...")
        df = read_taxi_data(spark, source_path, years)
        logger.info(f"Records read: {df.count()}")
        df.write.parquet(output_path, mode="overwrite", partitionBy=["year", "month"])
        logger.info(f"Records written: {df.count()}")  # Log the count of records written
        logger.info(f"Raw data written to {output_path}")
        return output_path  # Return the path to the raw data location
    except Exception as e:
        logger.error(f"Error in Raw Layer: {e}")
        raise