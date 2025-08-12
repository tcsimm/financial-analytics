USE ROLE FINLAB_ROLE;
USE WAREHOUSE FINLAB_WH;
USE DATABASE FINLAB;

CREATE OR REPLACE VIEW BRONZE.V_PRICES_RAW AS
SELECT
  TICKER,
  TO_DATE(DATA:date::string)  AS DT,
  DATA:open::float            AS OPEN,
  DATA:high::float            AS HIGH,
  DATA:low::float             AS LOW,
  DATA:close::float           AS CLOSE,
  DATA:adj_close::float       AS ADJ_CLOSE,
  DATA:volume::number         AS VOLUME,
  LOAD_TS
FROM BRONZE.PRICES_RAW
WHERE DATA:date IS NOT NULL;
