import os
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pickle
from datetime import datetime, timedelta
from tqdm import tqdm
import shutil

pd.set_option('future.no_silent_downcasting', True)

# Settings
DATA_CACHE_DIR = "data_cache"
CACHE_FILE = os.path.join(DATA_CACHE_DIR, "stock_data_max.pkl")

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
            # Save cache after each download to avoid losing progress
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

def momentum_squeeze_vectorized(df):
    """Vectorized Momentum Squeeze calculation with Practical Interpretation."""
    if df.empty: return pd.DataFrame()

    length = 20
    multKC = 1.5
    
    high = df['High']
    low = df['Low']
    close = df['Close']

    # Bollinger Bands (Matching original code: mult=1.5)
    basis = close.rolling(length).mean()
    dev = multKC * close.rolling(length).std() 
    upperBB = basis + dev
    lowerBB = basis - dev

    # Keltner Channels (Matching original code)
    ma = close.rolling(length).mean()
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    
    rangema = tr.rolling(length).mean()
    upperKC = ma + (rangema * multKC)
    lowerKC = ma - (rangema * multKC)

    # Squeeze Conditions (Matching original code)
    sqzOn = (lowerBB > lowerKC) & (upperBB < upperKC)
    sqzOff = (lowerBB < lowerKC) & (upperBB > upperKC)
    noSqz = (~sqzOn) & (~sqzOff)
    
    # Momentum (Linear Regression)
    highest_high = high.rolling(length).max()
    lowest_low = low.rolling(length).min()
    mid1 = (highest_high + lowest_low) / 2
    mid2 = (mid1 + close.rolling(length).mean()) / 2
    momentum_source = close - mid2

    n = length
    x = np.arange(n)
    x_sum = x.sum()
    x2_sum = (x**2).sum()
    denominator = n * x2_sum - x_sum**2

    def get_linreg_val(y):
        y_sum = y.sum()
        xy_sum = (x * y).sum()
        m = (n * xy_sum - x_sum * y_sum) / denominator
        c = (y_sum - m * x_sum) / n
        return m * (n - 1) + c

    val = momentum_source.rolling(length).apply(get_linreg_val, raw=True)
    val_prev = val.shift(1)

    # Colors and Interpretation (Matching original levels)
    hist_color = np.where(val > 0, 
                          np.where(val > val_prev, "lime", "green"),
                          np.where(val < val_prev, "red", "maroon"))
    
    dot_color = np.empty(len(df), dtype=object)
    interpretation = np.empty(len(df), dtype=object)
    signal = np.empty(len(df), dtype=object)

    sqzOn_prev = sqzOn.shift(1).fillna(False)

    for i in range(len(df)):
        if noSqz.iloc[i]:
            dot_color[i] = "blue"
            interpretation[i] = "No Squeeze"
            signal[i] = "Neutral"
        elif sqzOn.iloc[i]:
            dot_color[i] = "black"
            interpretation[i] = "Phase 1: Squeeze On (Market coiling - No trade)"
            signal[i] = "Neutral"
        elif sqzOff.iloc[i]:
            dot_color[i] = "gray"
            if sqzOn_prev.iloc[i]:
                if hist_color[i] == "lime":
                    interpretation[i] = "Phase 2 & 3: Squeeze Released + Bullish Expansion"
                    signal[i] = "Bullish"
                elif hist_color[i] == "red":
                    interpretation[i] = "Phase 2 & 3: Squeeze Released + Bearish Expansion"
                    signal[i] = "Bearish"
                else:
                    interpretation[i] = "Phase 2: Squeeze Released (Watch closely)"
                    signal[i] = "Neutral"
            else:
                interpretation[i] = "In Motion (Expansion continues)"
                signal[i] = "Neutral"
        else:
            dot_color[i] = "blue"
            interpretation[i] = "No Squeeze"
            signal[i] = "Neutral"

    result = df.copy()
    result['Momentum'] = val
    result['HistColor'] = hist_color
    result['DotColor'] = dot_color
    result['Interpretation'] = interpretation
    result['Signal'] = signal
    result.dropna(subset=['Momentum'], inplace=True)
    
    return result

def plot_squeeze(symbol, df, show_plot=True, save_plot=False):
    """Create interactive Plotly chart and save as PNG if requested."""
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.05, 
                        row_heights=[0.7, 0.3])

    # Remove timezone and convert index to string format for consistent static export
    df.index = pd.to_datetime(df.index).tz_localize(None)
    date_strs = df.index.strftime('%d-%b-%Y')

    fig.add_trace(go.Candlestick(x=date_strs,
                open=df['Open'], high=df['High'],
                low=df['Low'], close=df['Close'],
                name='Price'), row=1, col=1)

    fig.add_trace(go.Bar(x=date_strs, y=df['Momentum'],
                        marker_color=df['HistColor'],
                        name='Momentum'), row=2, col=1)

    # Add Zero Line for Momentum
    fig.add_hline(y=0, line_dash="solid", line_color="gray", line_width=1, row=2, col=1)

    fig.add_trace(go.Scatter(x=date_strs, y=[0]*len(df),
                            mode='markers',
                            marker=dict(color=df['DotColor'], size=8),
                            name='Squeeze Status'), row=2, col=1)

    fig.update_layout(title=f'Momentum Squeeze: {symbol}<br><sup>{df["Interpretation"].iloc[-1]}</sup>',
                      xaxis_rangeslider_visible=False,
                      template='plotly_dark',
                      showlegend=True,
                      margin=dict(l=50, r=50, t=100, b=50))
    
    # Category type for perfect static alignment
    fig.update_xaxes(type='category', nticks=10)
    
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Momentum", row=2, col=1)

    if save_plot:
        output_dir = "outputs/Squeeze_Result"
        if not os.path.exists(output_dir): os.makedirs(output_dir)
        filename = os.path.join(output_dir, f"{symbol.replace('^', '')}_squeeze.png")
        # Enforce rectangular dimensions for PNG export specifically
        fig.write_image(filename, width=1600, height=800, scale=2)

    if show_plot:
        # Default web view is now responsive
        fig.show()

def save_to_csv(data_list):
    """Save latest indicator details to CSV."""
    output_dir = "outputs/Squeeze_Result"
    if not os.path.exists(output_dir): os.makedirs(output_dir)
    df_results = pd.DataFrame(data_list)
    filename = os.path.join(output_dir, "Squeeze_Results.csv")
    df_results.to_csv(filename, index=False)
    print(f"\nCurrent details saved to {filename}")

if __name__ == "__main__":
    
    # Clear output folder before run
    output_dir = "outputs/Squeeze_Result"
    if os.path.exists(output_dir):
        try:
            shutil.rmtree(output_dir)
            # print(f"Cleared existing {output_dir} folder.")
        except Exception as e:
            print(f"Warning: Could not clear {output_dir}: {e}")
    os.makedirs(output_dir, exist_ok=True)

    # Configuration
    manual_symbols          = ["FINPIPE.NS", "^NSEI", "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS"]
    PROCESS_ALL_CACHED      = True
    SHOW_ALL_MANUAL_PLOTS   = False # Set to True to show plots for all manual_symbols regardless of signal
    
    interval                = "1d"
    period                  = "6mo"

    show_plot               = False
    save_plot               = True

    # Pre-load cache once
    cache = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'rb') as f:
                cache = pickle.load(f)
        except Exception as e:
            print(f"Error loading cache: {e}")

    if PROCESS_ALL_CACHED and cache:
        symbols = list(cache.keys())
        print(f"Found {len(symbols)} symbols in cache. Processing all with tqdm...")
    else:
        symbols = manual_symbols

    all_details = []

    # Process with tqdm progress bar
    for symbol in tqdm(symbols, desc="Processing Stocks", unit="stock"):
        data = get_data(symbol, cache, interval, period)
        if not data.empty:
            result = momentum_squeeze_vectorized(data)
            latest = result.iloc[-1]
            
            detail = {
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Symbol": symbol,
                "Price": latest['Close'],
                "Momentum": round(latest['Momentum'], 2),
                "Status": latest['DotColor'],
                "Signal": latest['Signal'],
                "Interpretation": latest['Interpretation']
            }
            all_details.append(detail)
            
            # Logic: Show plot if it's a breakout signal OR if it's a manual symbol and the toggle is ON
            is_breakout = latest['Signal'] in ["Bullish", "Bearish"]
            is_forced_manual = (symbol in manual_symbols) and SHOW_ALL_MANUAL_PLOTS
            
            if is_breakout or is_forced_manual:
                msg_prefix = "[SIGNAL]" if is_breakout else "[MANUAL]"
                tqdm.write(f"\n{msg_prefix} {symbol}: {latest['Signal']} | {latest['Interpretation']}")
                plot_squeeze(symbol, result, show_plot=show_plot, save_plot=save_plot)
    
    if all_details:
        save_to_csv(all_details)
