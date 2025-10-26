import os
import yfinance as yf
import pandas as pd
import quantstats as qs

# ======================================================
# 📊 Data Fetch Function
# ======================================================
def fetch_yf_data(symbol, period="1y", interval="1d"):
    """
    Fetch historical OHLCV data from yfinance with clean column handling.
    """
    df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False)
    df = df.reset_index()

    # Flatten MultiIndex columns if any
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if col[1] == symbol else col[1] for col in df.columns]

    # Ensure 'Date' column exists
    if 'Date' not in df.columns:
        df.rename(columns={df.columns[0]: 'Date'}, inplace=True)

    df = df.sort_values('Date').reset_index(drop=True)
    return df


# ======================================================
# 1️⃣ Single Stock Report
# ======================================================
def single_stock_report(symbol="TCS.NS", period="1y", interval="1d"):
    folder = "Single Stock"
    os.makedirs(folder, exist_ok=True)

    df_stock = fetch_yf_data(symbol, period, interval)
    returns_stock = df_stock.set_index('Date')['Close'].pct_change().dropna()
    returns_stock.name = symbol

    print("=== Single Stock Metrics ===")
    print(qs.reports.metrics(returns_stock))

    output_file = os.path.join(folder, f"{symbol}_quantstats_report.html")
    qs.reports.html(
        returns_stock,
        title=f"{symbol} Performance Report",
        output=output_file
    )
    print(f"✅ HTML report generated: {output_file}\n")


# ======================================================
# 2️⃣ Stock vs Benchmark Report
# ======================================================
def stock_vs_benchmark_report(symbol="TCS.NS", benchmark_symbol="^NSEI", period="1y", interval="1d"):
    folder = "Stock vs Benchmark"
    os.makedirs(folder, exist_ok=True)

    df_stock = fetch_yf_data(symbol, period, interval)
    returns_stock = df_stock.set_index('Date')['Close'].pct_change().dropna()
    returns_stock.name = symbol

    df_bench = fetch_yf_data(benchmark_symbol, period, interval)
    returns_bench = df_bench.set_index('Date')['Close'].pct_change().dropna()
    returns_bench.name = "NIFTY"

    print("=== Stock vs Benchmark Metrics ===")
    print(qs.reports.metrics(returns_stock, benchmark=returns_bench))

    output_file = os.path.join(folder, f"{symbol}_vs_NIFTY_report.html")
    qs.reports.html(
        returns_stock,
        benchmark=returns_bench,
        title=f"{symbol} vs NIFTY Performance Report",
        output=output_file
    )
    print(f"✅ HTML report generated: {output_file}\n")


# ======================================================
# 3️⃣ Multi-Stock Portfolio Report
# ======================================================
def multi_stock_portfolio_report(symbols=None, period="1y", interval="1d"):
    if symbols is None:
        symbols = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"]

    folder = "Multi-Stock Portfolio"
    os.makedirs(folder, exist_ok=True)

    dfs = [fetch_yf_data(sym, period, interval) for sym in symbols]
    df_all = pd.concat([df.set_index('Date')['Close'].rename(sym) for df, sym in zip(dfs, symbols)], axis=1)
    df_all = df_all.dropna()

    portfolio_returns = df_all.pct_change().mean(axis=1).dropna()
    portfolio_returns.name = "Portfolio"
    portfolio_returns.index = pd.to_datetime(portfolio_returns.index)

    output_file = os.path.join(folder, "portfolio_quantstats_report.html")
    qs.reports.html(
        portfolio_returns,
        title="Equal-Weighted Portfolio Performance Report",
        output=output_file
    )
    print(f"✅ Portfolio report generated: {output_file}\n")


# ======================================================
# 🚀 Run All Reports
# ======================================================
if __name__ == "__main__":

    single_stock_report("NIFTYBEES.NS", "max", "1d")

    # stock_vs_benchmark_report("GODFRYPHLP.NS", "^NSEI", "max", "1d")
    
    # multi_stock_portfolio_report(["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"], "max", "1d")
