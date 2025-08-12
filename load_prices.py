import os, json
from typing import Iterable, Tuple, List

import pandas as pd
import yfinance as yf
import snowflake.connector
from dotenv import load_dotenv

# ---------- helpers ----------
def require_env(var: str) -> str:
    v = os.getenv(var)
    if not v:
        raise RuntimeError(f"Missing required env var: {var}")
    return v

def connect_snowflake():
    load_dotenv()  # read .env on each run
    return snowflake.connector.connect(
        account=require_env("SNOW_ACCOUNT"),   
        user=require_env("SNOW_USER"),
        password=require_env("SNOW_PASSWORD"),
        role=os.getenv("SNOW_ROLE", "FINLAB_ROLE"),
        warehouse=os.getenv("SNOW_WAREHOUSE", "FINLAB_WH"),
        database=os.getenv("SNOW_DATABASE", "FINLAB"),
        schema=os.getenv("SNOW_SCHEMA", "BRONZE"),
    )

def _flatten_columns(cols) -> List[str]:
    """Flatten plain or MultiIndex columns to snake_case strings."""
    if isinstance(cols, pd.MultiIndex):
        out = []
        for tup in cols:
            parts = [str(x) for x in tup if x not in (None, "", " ")]
            out.append("_".join(parts))
        cols = out
    else:
        cols = [str(c) for c in cols]
    return [c.strip().lower().replace(" ", "_") for c in cols]

def yf_download(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """
    Download from yfinance and return a normalized DataFrame with a 'date_str' column.
    """
    df = yf.download(
        ticker,
        period=period,
        interval=interval,
        auto_adjust=False,
        group_by="column",   # avoid ('AAPL','Open') style columns
        threads=True,
        progress=False,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index()
    df.columns = _flatten_columns(df.columns)

    # Determine the datetime column
    dt_col = "date" if "date" in df.columns else ("datetime" if "datetime" in df.columns else None)
    if not dt_col:
        return pd.DataFrame()

    # Standardize date string
    df["date_str"] = pd.to_datetime(df[dt_col], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date_str"])

    # Ensure expected numeric columns exist
    for col in ["open", "high", "low", "close", "adj_close", "volume"]:
        if col not in df.columns:
            df[col] = pd.NA

    return df

def df_to_rows(ticker: str, df: pd.DataFrame) -> List[Tuple[str, str]]:
    """Transform normalized DF into list of (ticker, json_string) rows."""
    if df.empty:
        return []
    recs = df[["date_str", "open", "high", "low", "close", "adj_close", "volume"]].to_dict(orient="records")

    rows: List[Tuple[str, str]] = []
    for r in recs:
        payload = {
            "date": r["date_str"],
            "open": None if pd.isna(r["open"]) else float(r["open"]),
            "high": None if pd.isna(r["high"]) else float(r["high"]),
            "low": None if pd.isna(r["low"]) else float(r["low"]),
            "close": None if pd.isna(r["close"]) else float(r["close"]),
            "adj_close": None if pd.isna(r["adj_close"]) else float(r["adj_close"]),
            "volume": None if pd.isna(r["volume"]) else int(r["volume"]),
        }
        rows.append((ticker, json.dumps(payload)))
    return rows

def insert_rows(conn, rows: Iterable[Tuple[str, str]], table_fqn: str = "FINLAB.BRONZE.PRICES_RAW") -> int:
    rows = list(rows)
    if not rows:
        return 0

    # Use FROM VALUES and parse JSON from column2
    sql = f"""
        INSERT INTO {table_fqn} (TICKER, DATA)
        SELECT column1, PARSE_JSON(column2)
        FROM VALUES (%s, %s)
    """
    cur = conn.cursor()
    try:
        cur.executemany(sql, rows)
        conn.commit()
        return len(rows)
    finally:
        cur.close()

def load_ticker(conn, ticker: str, period: str = "1mo", interval: str = "1d", debug=False) -> int:
    df = yf_download(ticker, period, interval)
    if df.empty:
        print(f"{ticker}: nothing to load")
        return 0
    if debug:
        print(f"{ticker} columns -> {list(df.columns)[:12]}")
        print(df.head(3).to_string(index=False))
    rows = df_to_rows(ticker, df)
    inserted = insert_rows(conn, rows)
    print(f"{ticker}: inserted {inserted} rows")
    return inserted

def main():
    conn = connect_snowflake()
    try:
        cur = conn.cursor()
        who = cur.execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
        ).fetchone()
        cur.close()
        print(f"Connected as={who[0]} role={who[1]} wh={who[2]} db={who[3]} schema={who[4]}")

        tickers = ["AAPL", "MSFT", "SPY"]
        total = 0
        for i, t in enumerate(tickers):
            total += load_ticker(conn, t, period="3mo", interval="1d", debug=(i == 0))
        print(f"Done. Total rows inserted: {total}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
