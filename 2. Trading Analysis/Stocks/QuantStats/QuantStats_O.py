import os
import webbrowser
import yfinance as yf
import pandas as pd
import quantstats as qs

# ======================================================
# 📊 Data Fetch Function
# ======================================================
def fetch_yf_data(symbol, period="1y", interval="1d"):
    """
    Fetch historical OHLCV data from yfinance
    with clean column handling.
    """
    df = yf.download(
        symbol,
        period=period,
        interval=interval,
        auto_adjust=False,
        progress=False
    )

    df = df.reset_index()

    # Flatten MultiIndex columns if any
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            col[0] if col[1] == symbol else col[1]
            for col in df.columns
        ]

    # Ensure Date column
    if "Date" not in df.columns:
        df.rename(columns={df.columns[0]: "Date"}, inplace=True)

    df = df.sort_values("Date").reset_index(drop=True)
    return df


# ======================================================
# 1️⃣ Single Stock QuantStats Report
# ======================================================
def single_stock_report(symbol="KOTAKBANK.NS", period="5y", interval="1d"):
    folder = "Single Stock"
    os.makedirs(folder, exist_ok=True)

    # Fetch Data
    df_stock = fetch_yf_data(symbol, period, interval)

    # Returns
    returns = df_stock.set_index("Date")["Close"].pct_change().dropna()
    returns.name = symbol

    # Metrics (Terminal)
    print("\n==============================")
    print(f"📊 {symbol} – QuantStats Metrics")
    print("==============================")
    print(qs.reports.metrics(returns))

    # HTML Output
    output_file = os.path.join(folder, f"{symbol}_quantstats_report.html")

    qs.reports.html(
        returns,
        title=f"{symbol} Performance Report",
        output=output_file
    )

    # Absolute path for clickable link
    abs_path = os.path.abspath(output_file)
    file_url = f"file:///{abs_path.replace(os.sep, '/')}"

    # Auto-open in browser
    webbrowser.open(file_url)

    # Clickable link in terminal
    print("\n✅ HTML report generated successfully")
    print(f"🔗 Click to open report:\n{file_url}\n")


# ======================================================
# 🚀 Main Execution
# ======================================================
if __name__ == "__main__":
    
    single_stock_report("FIRSTCRY.NS", "max", "1d")
