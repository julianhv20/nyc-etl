# NYC Taxi ETL Pipeline

## Project Description
This project implements an ETL pipeline for processing NYC Yellow Taxi data using the Medallion Architecture. The pipeline extracts raw data, cleans and enriches it, and calculates KPIs for analysis.

## Architecture Overview
The pipeline follows the Medallion Architecture:
- **Raw Layer**: Stores raw data as-is from the source.
- **Trusted Layer**: Cleans, validates, and enriches the data.
- **Refined Layer**: Aggregates data and calculates KPIs for business insights.

## Setup Instructions
### Prerequisites
- Python 3.8+
- Java (for Apache Spark)
- Apache Spark

### Installation
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration
Edit `config.py` to set the following (if neccesary):
- `SOURCE_PATH`: Path to the source data (e.g., S3 bucket).
- `RAW_LAYER_PATH`, `TRUSTED_LAYER_PATH`, `REFINED_LAYER_PATH`: Paths for storing processed data.
- `YEARS_TO_PROCESS`: List of years to process.
- `LOOKUP_CSV_PATH`: Path to the Taxi Zone Lookup CSV.

## Execution Instructions
Run the pipeline:
```bash
python main.py
```

## Output Description
- Processed data is stored in the `data/` directory under `raw/`, `trusted/`, and `refined/`.
- A JSON report summarizing the execution is generated in the root directory (e.g., `run_report_<timestamp>.json`).

## Additional Documentation
- [Architecture](docs/architecture.md)
- [Transformations](docs/transformations.md)
- [Observability](docs/observability.md)