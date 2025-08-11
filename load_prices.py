# Imports
import os, json
import yfinance as yf
import snowflake.connector
from dotenv import load_dotenv

# Read .env
load_dotenv()  

# Connect to database
conn = snowflake.connector.connect(
    account=os.getenv("SNOW_ACCOUNT"),
    user=os.getenv("SNOW_USER"),
    password=os.getenv("SNOW_PASSWORD"),
    role=os.getenv("SNOW_ROLE", "FINLAB_ROLE"),
    warehouse=os.getenv("SNOW_WAREHOUSE", "FINLAB_WH"),
    database=os.getenv("SNOW_DATABASE", "FINLAB"),
    schema=os.getenv("SNOW_SCHEMA", "BRONZE"),
)

# Cursor
cur = conn.cursor()

def load_ticker(ticker: str, period="1mo", interval="1d"):
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False)
    df = df.reset_index()  
    rows = []
    for _, r in df.iterrows():
        payload = {
            "date": r["Date"].strftime("%Y-%m-%d"),
            "open": None if r["Open"] != r["Open"] else float(r["Open"]),
            "high": None if r["High"] != r["High"] else float(r["High"]),
            "low":  None if r["Low"]  != r["Low"]  else float(r["Low"]),
            "close":None if r["Close"]!= r["Close"] else float(r["Close"]),
            "adj_close": None if r.get("Adj Close") != r.get("Adj Close") else float(r["Adj Close"]),
            "volume": None if r["Volume"] != r["Volume"] else int(r["Volume"]),
        }
        rows.append((ticker, json.dumps(payload)))
    if rows:
        cur.executemany(
            """
            INSERT INTO FINLAB.BRONZE.PRICES_RAW (TICKER, DATA)
            SELECT %s, PARSE_JSON(%s)
            """,
            rows,
        )
        conn.commit()

# examples
for t in ["AAPL", "MSFT", "SPY"]:
    load_ticker(t, period="3mo", interval="1d")

# Close function
cur.close()
conn.close()
print("Loaded!")
