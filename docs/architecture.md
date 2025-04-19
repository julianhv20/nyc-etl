# Architecture Overview

## Medallion Architecture
The ETL pipeline is structured into three layers:

### Raw Layer
- **Purpose**: Stores raw data as-is from the source.
- **Input**: NYC Yellow Taxi data in Parquet format.
- **Output**: Data stored in the `data/raw/` directory.

### Trusted Layer
- **Purpose**: Cleans, validates, and enriches the data.
- **Input**: Data from the Raw Layer.
- **Output**: Cleaned and enriched data stored in the `data/trusted/` directory.

### Refined Layer
- **Purpose**: Aggregates data and calculates KPIs for business insights.
- **Input**: Data from the Trusted Layer.
- **Output**: Aggregated data and KPIs stored in the `data/refined/` directory.

## Data Flow
1. **Raw Layer**: Extracts data from the source and stores it in its original format.
2. **Trusted Layer**: Applies cleaning rules, validation checks, and joins with the Taxi Zone Lookup table.
3. **Refined Layer**: Performs aggregations and calculates KPIs for analysis.