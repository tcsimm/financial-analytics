# Financial Analytics – Bronze to Silver ETL (Snowflake)

This project demonstrates an ETL pipeline on Snowflake for processing financial market price data.  
It uses the Bronze → Silver architecture pattern:
- Bronze layer: raw ingestion of financial price data.
- Silver layer: cleaned, deduplicated, and type-cast tables ready for analytics.

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
5. Merge from Bronze to Silver
