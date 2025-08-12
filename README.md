Bronze → Silver ETL
This process cleans and standardizes raw market data from the Bronze layer into the Silver layer, making it analytics-ready.
Purpose
- Bronze: Raw ingested data from external sources (CSVs, APIs).
- Silver: Clean, deduplicated, and type-correct data ready for downstream analysis.
File Structure
sql/
│
├── 01_create_bronze_objects.sql       # Create Bronze tables and stages
├── 02_create_silver_prices.sql        # Create Silver PRICES table
├── 03_create_bronze_v_prices_raw.sql  # Bronze view for raw prices
├── 04_merge_bronze_to_silver.sql      # Merge logic from Bronze to Silver
└── setup.sql                          # My environment setup

Transformations in Silver
- Type Casting: Ensures correct data types for numeric and date columns.
- Null Filtering: Drops rows where DT or CLOSE is NULL.
- Deduplication: Keeps only the latest row per (TICKER, DT) using ROW_NUMBER().
- Upsert Logic: Uses MERGE to update existing records and insert new ones.

Steps to Run:
1. Create Bronze objects 
2. Load data into Bronze
3. Create Bronze View
4. Create Silver table
5. Merge from Bronze to Silver# financial-analytics
