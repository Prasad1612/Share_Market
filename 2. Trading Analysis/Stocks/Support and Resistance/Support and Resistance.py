"""
Fractal-based Support & Resistance Detector (Professional Version)
──────────────────────────────────────────────────────────────────
✓ Supports both yfinance and local CSV input.
✓ Generates one consolidated All_Levels.csv + Summary.csv
✓ Optional plotting for visual validation.
✓ Designed for professional trading analysis & automation.

"""

import os
import time
from pathlib import Path
import pandas as pd
import numpy as np
import yfinance as yf
from tqdm import tqdm
import matplotlib.pyplot as plt
import matplotlib.dates as mpl_dates
from mplfinance.original_flavor import candlestick_ohlc

plt.rcParams['figure.figsize'] = [12, 7]
plt.rc('font', size=12)

# -------------------------------------------------
# Fetch Historical OHLCV Data with Retry Logic
# -------------------------------------------------
def fetch_yf_data(symbol, period="6mo", interval="1d", retries=3, pause=1.0):
    for attempt in range(retries):
        try:
            df = yf.download(symbol, period=period, interval=interval, auto_adjust=True, progress=False)
            df = df.reset_index()

            # Flatten MultiIndex if any
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] if col[1] == symbol else col[1] for col in df.columns]

            if 'Date' not in df.columns:
                df.rename(columns={df.columns[0]: 'Date'}, inplace=True)

            # Ensure numeric values
            for col in ['Open', 'High', 'Low', 'Close']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)

            df = df.sort_values('Date').reset_index(drop=True)
            return df
        except Exception as e:
            if attempt == retries - 1:
                raise e
        time.sleep(pause * (2 ** attempt))
    return pd.DataFrame()

# -------------------------------------------------
# Fractal Detection Logic
# -------------------------------------------------
def is_support(df, i):
    return (df['Low'][i] < df['Low'][i-1] and df['Low'][i] < df['Low'][i+1] and
            df['Low'][i+1] < df['Low'][i+2] and df['Low'][i-1] < df['Low'][i-2])

def is_resistance(df, i):
    return (df['High'][i] > df['High'][i-1] and df['High'][i] > df['High'][i+1] and
            df['High'][i+1] > df['High'][i+2] and df['High'][i-1] > df['High'][i-2])

def identify_levels(df):
    levels = []
    for i in range(2, df.shape[0] - 2):
        if is_support(df, i):
            levels.append((i, df['Low'][i], 'Support'))
        elif is_resistance(df, i):
            levels.append((i, df['High'][i], 'Resistance'))
    return levels

# -------------------------------------------------
# Level Filtering & Strength Calculation
# -------------------------------------------------
def filter_levels(df, levels, sensitivity=1.0):
    mean_range = np.mean(df['High'] - df['Low'])
    filtered = []
    for i, level, typ in levels:
        if all(abs(level - lv[1]) > mean_range * sensitivity for lv in filtered):
            filtered.append((i, level, typ))
    return filtered

def compute_strength(df, levels, tolerance=0.005):
    result = []
    for i, level, typ in levels:
        if typ == 'Support':
            touches = ((df['Low'] >= level * (1 - tolerance)) & (df['Low'] <= level * (1 + tolerance))).sum()
        else:
            touches = ((df['High'] >= level * (1 - tolerance)) & (df['High'] <= level * (1 + tolerance))).sum()
        result.append((i, level, typ, touches))
    return result

def nearest_two_levels(df, level_strengths):
    ltp = df['Close'].iloc[-1]
    supports = sorted([lvl for lvl in level_strengths if lvl[1] <= ltp], key=lambda x: -x[1])[:2]
    resistances = sorted([lvl for lvl in level_strengths if lvl[1] >= ltp], key=lambda x: x[1])[:2]

    def dist(p): return ((p - ltp) / ltp) * 100
    supports = [(i, p, t, s, dist(p)) for i, p, t, s in supports]
    resistances = [(i, p, t, s, dist(p)) for i, p, t, s in resistances]

    return ltp, supports, resistances

# -------------------------------------------------
# Plotting Function (Optional)
# -------------------------------------------------
def plot_levels(df, level_strengths, symbol, nearest_support, nearest_resistance):
    df_plot = df.copy()
    df_plot['Date'] = df_plot['Date'].map(mpl_dates.date2num)
    ohlc = df_plot[['Date', 'Open', 'High', 'Low', 'Close']].values

    fig, ax = plt.subplots()
    candlestick_ohlc(ax, ohlc, width=0.6, colorup='green', colordown='red', alpha=0.8)

    # Plot all levels
    for i, level, level_type, strength in level_strengths:
        color = 'green' if level_type == 'Support' else 'red'
        plt.hlines(level, xmin=df_plot['Date'][i], xmax=df_plot['Date'].iloc[-1],
                   colors=color, linewidth=1.2, alpha=0.7)
        plt.text(df_plot['Date'][i], level,
                 f"{level:.2f}\n({level_type[0]})[{strength}]",
                 color=color, fontsize=8)

    # Highlight nearest zones
    if nearest_support:
        plt.axhline(nearest_support[1], color='green', linestyle='--', linewidth=1.5, label='Nearest Support')
    if nearest_resistance:
        plt.axhline(nearest_resistance[1], color='red', linestyle='--', linewidth=1.5, label='Nearest Resistance')

    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mpl_dates.DateFormatter('%d-%b-%y'))
    plt.title(f"Support & Resistance Levels for {symbol}", fontsize=16, fontweight='bold')
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.show()


# -------------------------------------------------
# Main Runner – Single Consolidated CSV Output
# -------------------------------------------------
from math import ceil

def run_fractal_sr(tickers, period='6mo', interval='1d',
                   use_local=False, local_dir=None, out_dir='./SR_Outputs',
                   sensitivity=1.0, tolerance=0.005, plot=False,
                   batch_size=10, delay_per_batch=2.0):
    """
    Fetches support/resistance levels for tickers in batches.

    Parameters:
        tickers: list of stock symbols
        batch_size: number of tickers per batch (default 10)
        delay_per_batch: seconds to wait after each batch
    """
    tickers = list(set(tickers))  # remove duplicates
    out_dir = Path(out_dir)
    out_dir.mkdir(exist_ok=True, parents=True)
    summary = []
    all_levels = []

    total_batches = ceil(len(tickers) / batch_size)

    for batch_idx in range(total_batches):
        batch = tickers[batch_idx * batch_size : (batch_idx + 1) * batch_size]

        for ticker in tqdm(batch, desc=f'Analyzing batch {batch_idx + 1}/{total_batches}'):
            try:
                # Adjust ticker name for local CSVs
                csv_ticker = ticker
                if use_local and local_dir:
                    csv_ticker = ticker.replace('.NS', '')  # strip .NS for local files

                # Load data
                if use_local and local_dir:
                    path = Path(local_dir) / f"{csv_ticker}.csv"
                    if path.exists():
                        df = pd.read_csv(path, parse_dates=['Date'])
                    else:
                        summary.append({'SYMBOL': ticker, 'ERROR': 'Local CSV not found'})
                        continue  # skip this ticker
                else:
                    df = fetch_yf_data(ticker, period, interval)

                if df.empty or len(df) < 10:
                    summary.append({'SYMBOL': ticker, 'ERROR': 'Insufficient data'})
                    continue

                # Core logic
                levels = identify_levels(df)
                if not levels:
                    summary.append({'SYMBOL': ticker, 'ERROR': 'No fractal levels detected'})
                    continue

                flt = filter_levels(df, levels, sensitivity)
                strength = compute_strength(df, flt, tolerance)
                ltp, sup, res = nearest_two_levels(df, strength)

                # Add each level to combined list
                for lv in strength:
                    all_levels.append({
                        'SYMBOL': ticker,
                        'CURRENT_PRICE': round(ltp, 2),
                        'LEVEL': round(lv[1], 2),
                        'TYPE': lv[2],
                        'STRENGTH': lv[3],
                        'DIST_%': round(((lv[1] - ltp) / ltp) * 100, 2)
                    })

                # Add summary data
                summary.append({
                    'SYMBOL': ticker,
                    'LTP': round(ltp, 2),
                    'SUPPORTS': ' | '.join([f"{p[1]:.2f} ({p[4]:+.2f}%)" for p in sup]),
                    'RESISTANCES': ' | '.join([f"{p[1]:.2f} ({p[4]:+.2f}%)" for p in res])
                })

                # Plotting
                if plot:
                    plot_levels(df, strength, ticker, sup[0] if sup else None, res[0] if res else None)

            except Exception as e:
                summary.append({'SYMBOL': ticker, 'ERROR': str(e)})

        # Wait after each batch
        if batch_idx < total_batches - 1:
            print(f"\n⏳ Waiting {delay_per_batch} seconds before next batch...\n")
            time.sleep(delay_per_batch)

    # Save all levels to a single CSV
    df_levels = pd.DataFrame(all_levels)
    df_levels.to_csv(out_dir / 'All_Levels.csv', index=False)

    # Save summary
    df_sum = pd.DataFrame(summary)
    df_sum.to_csv(out_dir / 'Summary.csv', index=False)

    return df_sum, df_levels


# -------------------------------------------------
# Example Execution
# -------------------------------------------------
if __name__ == '__main__':
    tickers = ['RELIANCE.NS', 'TCS.NS', 'INFY.NS']

    summary, all_levels = run_fractal_sr(
        tickers,
        period      = '6mo',
        interval    = '1d',
        use_local   = False,                                    # True = use local csv
        local_dir   = None,      #r"D:\user\Trading\Stocks",    # CSV path csv file name like (RELIANCE, TCS, INFY -  file format must be .csv)
        plot        = True                                      # True = show chart
    )

    print("\n📊 Summary:")
    print(summary)

    print("\n📈 Combined All_Levels:")
    print(all_levels.head())
