from pyspark.sql.functions import count, avg, hour, dayofweek, dayofmonth, month, year, col, sum
import logging

def process_refined_layer(spark, trusted_data_path, output_path):
    logger = logging.getLogger()
    try:
        logger.info("Starting Refined Layer processing...")
        df = spark.read.parquet(trusted_data_path)

        # Log the number of records read from Trusted
        logger.info(f"Records read from Trusted Layer: {df.count()}")

        # Calculate trip duration and handle negative durations
        df = df.withColumn("duration_seconds", 
                          (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")))
        df = df.filter(col("duration_seconds") >= 0)

        # Extract time features
        df = df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
        df = df.withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime"))
        df = df.withColumn("pickup_day_of_month", dayofmonth("tpep_pickup_datetime"))
        df = df.withColumn("pickup_month", month("tpep_pickup_datetime"))
        df = df.withColumn("pickup_year", year("tpep_pickup_datetime"))

        # KPI: Demand and Peak Times
        demand_kpi = df.groupBy("pickup_hour", "pickup_day_of_week") \
                       .agg(count("*").alias("trip_count"),
                            avg("duration_seconds").alias("avg_duration_seconds"),
                            avg("fare_amount").alias("avg_fare_amount"))
        demand_kpi.write.parquet(f"{output_path}/demand_kpi", mode="overwrite")

        # KPI: Efficiency by Zone/Borough
        efficiency_kpi = df.filter(col("trip_distance") > 0).filter(col("duration_seconds") > 0) \
                           .groupBy("PUBorough", "PUZone") \
                           .agg((sum("fare_amount") / sum("trip_distance")).alias("avg_fare_per_mile"),
                                (sum("trip_distance") / sum("duration_seconds") * (3600/1609.34)).alias("avg_speed_mph"))
        efficiency_kpi.write.parquet(f"{output_path}/efficiency_kpi", mode="overwrite")

        # KPI: Total Revenue
        total_revenue = df.agg(sum("fare_amount").alias("total_revenue")).collect()[0]["total_revenue"]
        logger.info(f"Total Revenue: {total_revenue}")

        # Return paths and scalar KPIs
        return {
            "demand_kpi_path": f"{output_path}/demand_kpi",
            "efficiency_kpi_path": f"{output_path}/efficiency_kpi",
            "total_revenue": total_revenue
        }
    except Exception as e:
        logger.error(f"Error in Refined Layer: {e}")
        raise