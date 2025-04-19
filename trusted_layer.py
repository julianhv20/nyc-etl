from pyspark.sql.functions import col
import logging

def process_trusted_layer(spark, raw_data_path, output_path, lookup_csv_path):
    logger = logging.getLogger()
    try:
        logger.info("Starting Trusted Layer processing...")
        df = spark.read.parquet(raw_data_path)
        logger.info(f"Records read from Raw Layer: {df.count()}")

        # Validaciones
        df = df.filter(col("tpep_pickup_datetime") < col("tpep_dropoff_datetime"))
        df = df.filter((col("trip_distance") > 0) & (col("fare_amount") > 0))
        df = df.filter((col("trip_distance") < 1000) & (col("fare_amount") < 5000))  # Outlier thresholds

        # Manejo de valores nulos
        critical_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "PULocationID", "DOLocationID"]
        initial_count = df.count()
        df = df.na.drop(subset=critical_columns)
        dropped_count = initial_count - df.count()
        logger.info(f"Rows dropped due to nulls in critical columns: {dropped_count}")

        # Estandarización
        df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        df = df.withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

        # Enriquecimiento
        lookup_df = spark.read.csv(lookup_csv_path, header=True, inferSchema=True)
        df = df.join(lookup_df.withColumnRenamed("LocationID", "PULocationID")
                          .withColumnRenamed("Zone", "PUZone")
                          .withColumnRenamed("Borough", "PUBorough"),
                      on="PULocationID", how="left")
        df = df.join(lookup_df.withColumnRenamed("LocationID", "DOLocationID")
                          .withColumnRenamed("Zone", "DOZone")
                          .withColumnRenamed("Borough", "DOBorough"),
                      on="DOLocationID", how="left")

        enriched_count = df.count()
        logger.info(f"Records after enrichment: {enriched_count}")

        df.write.parquet(output_path, mode="overwrite")
        logger.info(f"Records written to Trusted Layer: {df.count()}")
        return output_path  # Return the path to the trusted data location
    except Exception as e:
        logger.error(f"Error in Trusted Layer: {e}")
        raise