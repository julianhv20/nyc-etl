# Observability

## Logging Strategy
- **INFO**: Log key events such as the start and end of each pipeline stage, and the number of records processed.
- **WARNING**: Log potential issues such as missing or invalid data.
- **ERROR**: Log critical errors that prevent the pipeline from continuing.

## Error Handling Approach
- Use `try...except` blocks around critical operations such as:
  - Reading data from the source.
  - Writing data to the output.
  - Performing transformations.
- Log errors with stack traces for debugging.

## Execution Report
- Generate a JSON report summarizing:
  - Total records processed.
  - Records discarded or corrected.
  - Execution time for each stage (Raw, Trusted, Refined).
  - Calculated KPIs.
- Save the report to the root directory (e.g., `run_report_<timestamp>.json`).