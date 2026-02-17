
import os
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pickle
import shutil
from datetime import datetime, timedelta
from tqdm import tqdm

pd.set_option('future.no_silent_downcasting', True)

# Settings
DATA_CACHE_DIR = "data_cache"
CACHE_FILE = os.path.join(DATA_CACHE_DIR, "stock_data_max.pkl")
OUTPUT_DIR = "outputs/Consolidation_Box"

def get_data(symbol, cache, interval="1d", period="6mo"):
    """Fetch data using pre-loaded cache: downloads 'max' if missing and slices by period."""
    needs_download = False
    if symbol not in cache:
        needs_download = True
    else:
        last_date = cache[symbol].index[-1]
        if datetime.now() - last_date > timedelta(days=1):
            needs_download = True

    if needs_download:
        df = yf.download(symbol, interval=interval, period="max", auto_adjust=False, progress=False, multi_level_index=False)
        if not df.empty:
            cache[symbol] = df
            if not os.path.exists(DATA_CACHE_DIR): os.makedirs(DATA_CACHE_DIR)
            with open(CACHE_FILE, 'wb') as f:
                pickle.dump(cache, f)
        else:
            return pd.DataFrame()

    df = cache[symbol]
    
    period_map = {
        "1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "2y": 730, "5y": 1825, "10y": 3650, "max": 99999
    }
    
    if period in period_map:
        days = period_map[period]
        start_date = df.index[-1] - timedelta(days=days)
        df = df[df.index >= start_date]
    
    return df

def calculate_atr(df, length=14):
    high = df['High']
    low = df['Low']
    close = df['Close']
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(length).mean()

def detect_consolidation_boxes(symbol, df, range_len=10, cooldown_bars=13, atr_len=14, max_atr_mult=3.0, retest_bars=20, retest_gap_pct=1.0):
    """Refactored Darvas Box logic with retest tracking and full history."""
    df = df.copy()
    df['ATR'] = calculate_atr(df, atr_len)
    
    historical_boxes = []
    current_signal = "Neutral"
    latest_active_box = None

    phase = 0
    box_high = box_low = start_idx = cooldown_start = None
    breakout_type = is_retested = None
    
    for i in range(len(df)):
        row = df.iloc[i]
        
        # PHASE 0: Look for new range
        if phase == 0 and i >= range_len:
            window = df.iloc[i - range_len:i]
            hh, ll = window['High'].max(), window['Low'].min()
            
            if pd.notna(row['ATR']) and (hh - ll) <= row['ATR'] * max_atr_mult:
                box_high, box_low, start_idx = hh, ll, i - range_len
                phase = 2
                
        # PHASE 2: Monitor for breakout or extension
        elif phase == 2:
            close = row['Close']
            if close > box_high or close < box_low:
                breakout_type = "UP" if close > box_high else "DOWN"
                is_retested = False
                cooldown_start = i
                
                historical_boxes.append({
                    "symbol": symbol,
                    "start_date": df.index[start_idx].strftime('%d-%m-%y %H:%M'),
                    "end_date": df.index[i].strftime('%d-%m-%y %H:%M'),
                    "high": round(box_high, 2),
                    "low": round(box_low, 2),
                    "breakout": breakout_type,
                    "retested": False
                })
                
                if i == len(df) - 1: current_signal = f"Breakout {breakout_type}"
                phase = 3
            else:
                if row['High'] > box_high: box_high = row['High']
                if row['Low'] < box_low: box_low = row['Low']
                latest_active_box = {'start': df.index[start_idx], 'end': df.index[i], 'high': box_high, 'low': box_low}

        # PHASE 3: Cooldown + Retest Check
        elif phase == 3:
            lookback = i - cooldown_start
            if not is_retested and lookback > 0:
                safe_lb = min(retest_bars, lookback)
                recent = df.iloc[i - safe_lb:i]
                breakout_price = box_high if breakout_type == "UP" else box_low
                gap = breakout_price * retest_gap_pct / 100
                
                if breakout_type == "UP":
                    if (breakout_price - gap) <= recent['Low'].min() < breakout_price:
                        is_retested = True
                        historical_boxes[-1]["retested"] = True
                else:
                    if breakout_price < recent['High'].max() <= (breakout_price + gap):
                        is_retested = True
                        historical_boxes[-1]["retested"] = True
            
            if lookback >= cooldown_bars:
                phase = 0

    return historical_boxes, latest_active_box, current_signal

def plot_consolidation(symbol, df, boxes, current_box, signal, save=True, show=False):
    """Plot candles and shaded boxes using Plotly."""
    df.index = pd.to_datetime(df.index).tz_localize(None)
    date_strs = df.index.strftime('%d-%b-%Y')

    fig = go.Figure()
    fig.add_trace(go.Candlestick(x=date_strs, open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'], name='Price'))

    for box in boxes:
        # Convert date string back for x-axis matching
        b_start = datetime.strptime(box['start_date'], '%d-%m-%y %H:%M').strftime('%d-%b-%Y')
        b_end = datetime.strptime(box['end_date'], '%d-%m-%y %H:%M').strftime('%d-%b-%Y')
        
        color = "rgba(0, 255, 0, 0.2)" if box['breakout'] == 'UP' else "rgba(255, 0, 0, 0.2)"
        fig.add_vrect(x0=b_start, x1=b_end, fillcolor=color, layer="below", line_width=0)
        fig.add_shape(type="line", x0=b_start, x1=b_end, y0=box['high'], y1=box['high'], line=dict(color="gray", width=1, dash="dash"))
        fig.add_shape(type="line", x0=b_start, x1=b_end, y0=box['low'], y1=box['low'], line=dict(color="gray", width=1, dash="dash"))

    if current_box:
        c_start, c_end = current_box['start'].strftime('%d-%b-%Y'), current_box['end'].strftime('%d-%b-%Y')
        fig.add_vrect(x0=c_start, x1=c_end, fillcolor="rgba(255, 255, 255, 0.1)", layer="below", line_width=0)

    fig.update_layout(title=f'Consolidation Box Breakout: {symbol} ({signal})', xaxis_rangeslider_visible=False, template='plotly_dark', xaxis_type='category', margin=dict(l=50, r=50, t=100, b=50))
    fig.update_xaxes(nticks=10)

    if save:
        filename = os.path.join(OUTPUT_DIR, f"{symbol.replace('^', '')}_consolidation.png")
        fig.write_image(filename, width=1600, height=800, scale=2)

if __name__ == "__main__":
    if os.path.exists(OUTPUT_DIR): shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    manual_symbols              = ["^NSEI", "RELIANCE.NS", "TCS.NS"]
    PROCESS_ALL_CACHED          = True
    SHOW_ALL_MANUAL_PLOTS       = False # Set to True to show plots for all manual_symbols regardless of signal
    interval, period            = "1d", "1y"

    cache = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'rb') as f: cache = pickle.load(f)

    symbols = list(cache.keys()) if PROCESS_ALL_CACHED and cache else manual_symbols
    print(f"Scanning {len(symbols)} symbols for Consolidation Breakouts...")

    summary_results = []
    all_historical_boxes = []
    plotting_queue = []

    for symbol in tqdm(symbols, desc="Scanning for Breakouts", unit="stock"):
        data = get_data(symbol, cache, interval, period)
        if not data.empty:
            history, active_box, signal = detect_consolidation_boxes(symbol, data)
            
            all_historical_boxes.extend(history)
            summary_results.append({"Symbol": symbol, "Price": round(data['Close'].iloc[-1], 2), "Signal": signal})

            if signal != "Neutral" or (symbol in manual_symbols and SHOW_ALL_MANUAL_PLOTS):
                plotting_queue.append((symbol, data, history, active_box, signal))

    # Save CSVs first (Complete report)
    hist_csv = os.path.join(OUTPUT_DIR, "Historical_Consolidation_Boxes.csv")
    res_csv = os.path.join(OUTPUT_DIR, "Consolidation_Results.csv")
    pd.DataFrame(all_historical_boxes).to_csv(hist_csv, index=False)
    pd.DataFrame(summary_results).to_csv(res_csv, index=False)
    print(f"\nScanning complete. Reports saved to {OUTPUT_DIR}")

    # Plot batch second
    if plotting_queue:
        print(f"\nGenerating {len(plotting_queue)} high-quality charts...")
        for symbol, data, history, active_box, signal in tqdm(plotting_queue, desc="Plotting", unit="chart"):
            plot_consolidation(symbol, data, history, active_box, signal)

    print(f"\nProcess complete. All results saved to {OUTPUT_DIR}")
