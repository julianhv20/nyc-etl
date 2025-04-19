import time
import json
from utils import get_spark_session, setup_logging
from raw_layer import process_raw_layer
from trusted_layer import process_trusted_layer
from refined_layer import process_refined_layer
import config

if __name__ == "__main__":
    logger = setup_logging()
    spark = get_spark_session()

    # Record start time
    start_time = time.time()
    report = {
        "start_timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)),
        "stages": {},
    }

    try:
        logger.info("Starting ETL Pipeline...")

        # Process Raw Layer
        stage_start = time.time()
        raw_output_path, raw_record_count = process_raw_layer(spark, config.SOURCE_PATH, config.RAW_LAYER_PATH, config.YEARS_TO_PROCESS)
        stage_end = time.time()
        report["stages"]["raw"] = {
            "execution_time": stage_end - stage_start,
            "records_read": raw_record_count,
        }

        # Process Trusted Layer
        stage_start = time.time()
        trusted_output_path, trusted_processed_count, trusted_discarded_count = process_trusted_layer(
            spark, raw_output_path, config.TRUSTED_LAYER_PATH, config.LOOKUP_CSV_PATH
        )
        stage_end = time.time()
        report["stages"]["trusted"] = {
            "execution_time": stage_end - stage_start,
            "records_processed": trusted_processed_count,
            "records_discarded": trusted_discarded_count,
        }

        # Process Refined Layer
        stage_start = time.time()
        refined_results, refined_processed_count = process_refined_layer(
            spark, trusted_output_path, config.REFINED_LAYER_PATH
        )
        stage_end = time.time()
        report["stages"]["refined"] = {
            "execution_time": stage_end - stage_start,
            "records_processed": refined_processed_count,
            "kpi_values": refined_results,
        }

        # Record end time and total execution time
        end_time = time.time()
        report["end_timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
        report["total_execution_time"] = end_time - start_time

        # Save report to JSON
        report_path = f"run_report_{int(end_time)}.json"
        with open(report_path, "w") as report_file:
            json.dump(report, report_file, indent=4)

        logger.info(f"ETL Pipeline completed successfully. Report saved to {report_path}")
    except Exception as e:
        logger.critical(f"Pipeline failed: {e}", exc_info=True)
    finally:
        spark.stop()