# Transformations

## Cleaning Rules
- Ensure `tpep_pickup_datetime` is earlier than `tpep_dropoff_datetime`.
- Filter out records where `trip_distance` or `fare_amount` is less than or equal to zero.
- Remove outliers based on thresholds:
  - `trip_distance` < 1000
  - `fare_amount` < 5000

## Validation Checks
- Drop rows with null values in critical columns:
  - `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `trip_distance`, `fare_amount`, `PULocationID`, `DOLocationID`.

## Outlier Handling Strategy
- Define thresholds for valid ranges of `trip_distance` and `fare_amount`.
- Filter out records that exceed these thresholds.

## Join Logic
- Enrich data by joining with the Taxi Zone Lookup table:
  - Join on `PULocationID` and `DOLocationID` to add zone and borough information.

## KPI Calculations
### Demand and Peak Times
- Group by hour and day of the week to calculate:
  - Total trips.
  - Average trip duration.
  - Average fare amount.

### Efficiency by Zone/Borough
- Calculate:
  - Average fare per mile.
  - Average speed (miles per hour).

### Data Quality Impact
- Measure:
  - Percentage of records discarded or corrected.
  - Total revenue before and after cleaning.