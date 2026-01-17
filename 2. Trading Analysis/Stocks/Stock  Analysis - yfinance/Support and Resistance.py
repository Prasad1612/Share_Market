"""
Fractal-based Support & Resistance Detector (Professional Version)
──────────────────────────────────────────────────────────────────
✓ Supports both yfinance and local CSV input.
✓ Generates one consolidated All_Levels.csv + Summary.csv
✓ Automatically saves charts for levels within ±DIST_% of LTP.
✓ Designed for professional trading analysis & automation.

"""

import os
import time
import shutil
from pathlib import Path
import pandas as pd
import numpy as np
import yfinance as yf
from tqdm import tqdm
import matplotlib
matplotlib.use("Agg")  # Use non-GUI backend to avoid Tkinter errors
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
            df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False)
            df = df.reset_index()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] if col[1] == symbol else col[1] for col in df.columns]
            if 'Date' not in df.columns:
                df.rename(columns={df.columns[0]: 'Date'}, inplace=True)
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
# Plotting & Auto-Save Function
# -------------------------------------------------
def plot_and_save_levels(df, level_strengths, symbol, nearest_support, nearest_resistance, out_dir):
    df_plot = df.copy()
    df_plot['Date_num'] = df_plot['Date'].map(mpl_dates.date2num)
    ohlc = df_plot[['Date_num', 'Open', 'High', 'Low', 'Close']].values

    # Create subplots with 2 rows, share same x-axis
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, gridspec_kw={'height_ratios': [3, 1]}, figsize=(12, 9))
    
    # 1. Candlestick Chart (Top Subplot)
    candlestick_ohlc(ax1, ohlc, width=0.6, colorup='green', colordown='red', alpha=0.8)

    for i, level, level_type, strength in level_strengths:
        color = 'green' if level_type == 'Support' else 'red'
        ax1.hlines(level, xmin=df_plot['Date_num'][i], xmax=df_plot['Date_num'].iloc[-1],
                   colors=color, linewidth=1.2, alpha=0.7)
        ax1.text(df_plot['Date_num'][i], level,
                 f"{level:.2f}\n({level_type[0]})[{strength}]",
                 color=color, fontsize=8)

    if nearest_support:
        ax1.axhline(nearest_support[1], color='green', linestyle='--', linewidth=1.5)
    if nearest_resistance:
        ax1.axhline(nearest_resistance[1], color='red', linestyle='--', linewidth=1.5)

    ax1.set_title(f"Support & Resistance Levels for {symbol}", fontsize=16, fontweight='bold')
    ax1.set_ylabel("Price")
    ax1.grid(True, linestyle='--', alpha=0.4)

    # 2. Volume Chart (Bottom Subplot)
    # Filter for green/red volume bars
    colors = ['green' if df_plot.iloc[i]['Close'] >= df_plot.iloc[i]['Open'] else 'red' for i in range(len(df_plot))]
    ax2.bar(df_plot['Date_num'], df_plot['Volume'], color=colors, alpha=0.8, width=0.6)
    ax2.set_ylabel("Volume")
    ax2.grid(True, linestyle='--', alpha=0.4)

    # Date formatting for shared x-axis
    ax2.xaxis_date()
    ax2.xaxis.set_major_formatter(mpl_dates.DateFormatter('%d-%b-%y'))
    plt.xticks(rotation=20)
    
    plt.xlabel("Date")
    plt.tight_layout()

    chart_dir = Path(out_dir) / "Charts"
    chart_dir.mkdir(exist_ok=True, parents=True)
    plt.savefig(chart_dir / f"{symbol.replace('.NS','')}.png", dpi=150)
    plt.close()

# -------------------------------------------------
# Main Runner – Single Consolidated CSV Output
# -------------------------------------------------
# -------------------------------------------------
# Main Runner – Single Consolidated CSV Output
# -------------------------------------------------
def run_fractal_sr(tickers, period='6mo', interval='1d',
                   use_local=False, local_dir=None, out_dir='outputs/support_resistance/',
                   sensitivity=1.0, tolerance=0.005, plot=False,
                   save_charts=False, dist_range=0.3,
                   batch_size=10, delay_per_batch=2.0, data_dict=None):
    tickers = list(set(tickers))
    out_dir = Path(out_dir)
    out_dir.mkdir(exist_ok=True, parents=True)
    summary = []
    all_levels = []

    # Delete existing Charts folder for a fresh start
    chart_dir = out_dir / "Charts"
    if chart_dir.exists() and chart_dir.is_dir():
        shutil.rmtree(chart_dir)

    # Single tqdm progress bar for all tickers
    with tqdm(total=len(tickers), desc="Processing tickers") as pbar:
        for batch_start in range(0, len(tickers), batch_size):
            batch = tickers[batch_start:batch_start + batch_size]

            for ticker in batch:
                try:
                    df = pd.DataFrame()
                    if data_dict and ticker in data_dict:
                        df = data_dict[ticker].copy()
                        # Ensure standard format for SR logic
                        if 'Date' not in df.columns and isinstance(df.index, pd.DatetimeIndex):
                            df = df.reset_index()
                        
                        # Use only requested period size roughly (approx 125 days for 6mo, 252 for 1y)
                        # Logic: "6mo". If data has 1y, take last 6mo.
                        # For simplicity, we use available data unless it's way too much (slow).
                        # But typically more data is fine for fractal levels.
                        
                        if 'Date' not in df.columns:
                            # If date is still missing
                            df.rename(columns={df.columns[0]: 'Date'}, inplace=True)

                    else:
                        csv_ticker = ticker.replace('.NS','') if use_local else ticker
                        if use_local and local_dir:
                            path = Path(local_dir) / f"{csv_ticker}.csv"
                            if path.exists():
                                df = pd.read_csv(path, parse_dates=['Date'])
                            else:
                                summary.append({'SYMBOL': ticker, 'ERROR': 'Local CSV not found'})
                                pbar.update(1)
                                continue
                        else:
                            df = fetch_yf_data(ticker, period, interval)

                    if df.empty or len(df) < 10:
                        summary.append({'SYMBOL': ticker, 'ERROR': 'Insufficient data'})
                        pbar.update(1)
                        continue
                    
                    # Ensure numeric columns
                    for col in ['Open', 'High', 'Low', 'Close']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    df.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)

                    levels = identify_levels(df)
                    if not levels:
                        summary.append({'SYMBOL': ticker, 'ERROR': 'No fractal levels detected'})
                        pbar.update(1)
                        continue

                    flt = filter_levels(df, levels, sensitivity)
                    strength = compute_strength(df, flt, tolerance)
                    ltp, sup, res = nearest_two_levels(df, strength)

                    for lv in strength:
                        dist_pct = ((lv[1]-ltp)/ltp)*100
                        all_levels.append({
                            'SYMBOL': ticker,
                            'CURRENT_PRICE': round(ltp, 2),
                            'LEVEL': round(lv[1], 2),
                            'TYPE': lv[2],
                            'STRENGTH': lv[3],
                            'DIST_%': round(dist_pct, 2)
                        })

                    summary.append({
                        'SYMBOL': ticker,
                        'LTP': round(ltp,2),
                        'SUPPORTS': ' | '.join([f"{p[1]:.2f} ({p[4]:+.2f}%)" for p in sup]),
                        'RESISTANCES': ' | '.join([f"{p[1]:.2f} ({p[4]:+.2f}%)" for p in res])
                    })

                    if plot:
                        plot_and_save_levels(df, strength, ticker, sup[0] if sup else None, res[0] if res else None, out_dir)

                    # Auto-save charts for levels within ±dist_range%
                    if save_charts:
                        filtered_levels = [lv for lv in strength if abs((lv[1]-ltp)/ltp*100) <= dist_range]
                        if filtered_levels:
                            plot_and_save_levels(df, strength, ticker, sup[0] if sup else None, res[0] if res else None, out_dir)

                except Exception as e:
                    summary.append({'SYMBOL': ticker, 'ERROR': str(e)})

                pbar.update(1)  # update progress for each ticker

            # Delay after each batch (only if fetching from net, but harmless if using local dict)
            # if not data_dict and not use_local:
            if batch_start + batch_size < len(tickers):
                 # Reduced sleep if using dict
                 sleep_time = 0.01 if data_dict else delay_per_batch
                 time.sleep(sleep_time)

    # Save CSVs
    df_levels = pd.DataFrame(all_levels)
    df_levels.to_csv(out_dir / 'All_Levels.csv', index=False)
    df_sum = pd.DataFrame(summary)
    df_sum.to_csv(out_dir / 'Summary.csv', index=False)

    return df_sum, df_levels


# -------------------------------------------------
# Example Execution
# -------------------------------------------------
# -------------------------------------------------
# Example Execution
# -------------------------------------------------
if __name__ == '__main__':
    try:
        import stock_data_manager
        tickers = stock_data_manager.get_combined_ticker_list()
        
        # NOTE: SR script uses a huge list. The user mentioned "3 file yfinance use for data I want one time...".
        # So we should use the centralized data fetcher.
        
        # Example limit for testing if running standalone
        # tickers = tickers[:5] 
        # But we will fetch all if requested.
    
        period          = "1y"              #   Using 1y as fetched
        interval        = "1d"      

        print("Checking local cache or fetching data...")
        data_map = stock_data_manager.get_data(tickers, period=period, interval=interval)
        
        summary, all_levels = run_fractal_sr(
            tickers,
    
            # period          = "6mo"   ,              #   Using 1y as fetched
            # interval        = "1d"  ,                
    
            use_local       = False,
            local_dir       = r"D:\Prasad\Trading\01. Py\0. Main\3. SMC Analysis & Screener\Stocks",
            plot            = False,         
            save_charts     = True,          
            dist_range      = 0.3,           
            batch_size      = 50,           # Can increase batch size as we have data locally in RAM
            delay_per_batch = 0.1,          # Minimal delay
            data_dict       = data_map      # Pass the data
        )
    
        print("\n📊 Summary:")
        print(summary)
        print("\n📈 Combined All_Levels:")
        print(all_levels.head())

    except ImportError:
        print("⚠️ stock_data_manager or other dependencies not found.")
        # Fallback to old behavior if needed, or just fail
