# ============================================================
# 📊 Fibonacci Retracement Auto Plotter 
# ============================================================

import yfinance as yf
import pandas as pd
import mplfinance as mpf
import seaborn as sns
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="tkinter")

# -----------------------------------
# 1️⃣ Fetch Historical Data
# -----------------------------------
def get_data(symbol: str, period="6mo", interval="1d"):
    df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False)
    df = df.reset_index()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if col[1] == symbol else col[1] for col in df.columns]

    if 'Date' not in df.columns:
        df.rename(columns={df.columns[0]: 'Date'}, inplace=True)

    df = df.sort_values('Date').reset_index(drop=True)

    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


# -----------------------------------
# 2️⃣ Detect Recent Swing High & Low
# -----------------------------------
def find_recent_swing(df, lookback=30):
    recent_data = df.tail(lookback)

    swing_high_val = recent_data['High'].max()
    swing_low_val = recent_data['Low'].min()

    swing_high_date = pd.to_datetime(
        recent_data.loc[recent_data['High'] == swing_high_val, 'Date'].values[-1]
    )
    swing_low_date = pd.to_datetime(
        recent_data.loc[recent_data['Low'] == swing_low_val, 'Date'].values[-1]
    )

    return float(swing_high_val), float(swing_low_val), swing_high_date, swing_low_date


# -----------------------------------
# 3️⃣ Compute Fibonacci Levels
# -----------------------------------
def fibonacci_levels(swing_high, swing_low, trend="auto"):
    if trend == "auto":
        trend = "up" if swing_high > swing_low else "down"

    diff = abs(swing_high - swing_low)

    if trend == "up":
        return {
            '0.0% (High)': swing_high,
            '23.6%': swing_high - 0.236 * diff,
            '38.2%': swing_high - 0.382 * diff,
            '50.0%': swing_high - 0.5 * diff,
            '61.8%': swing_high - 0.618 * diff,
            '78.6%': swing_high - 0.786 * diff,
            '100.0% (Low)': swing_low
        }
    else:
        return {
            '0.0% (Low)': swing_low,
            '23.6%': swing_low + 0.236 * diff,
            '38.2%': swing_low + 0.382 * diff,
            '50.0%': swing_low + 0.5 * diff,
            '61.8%': swing_low + 0.618 * diff,
            '78.6%': swing_low + 0.786 * diff,
            '100.0% (High)': swing_high
        }


# -----------------------------------
# 4️⃣ Plot Candlestick + Fibonacci
# -----------------------------------
def plot_fibonacci(df, swing_high, swing_low, swing_low_date, levels, symbol):
    sns.set_theme(style="whitegrid")

    # Soft pastel color scheme for up/down candles
    mc = mpf.make_marketcolors(
        up="#2E8B57",       # green shade (bullish)
        down="#E74C3C",     # red shade (bearish)
        edge='black',
        wick='black',
        volume='dimgray'
    )

    s = mpf.make_mpf_style(
        marketcolors=mc,
        gridcolor='#D3D3D3',
        gridstyle='--',
        facecolor='white',
        figcolor='white',
        rc={
            'axes.labelcolor': 'black',
            'axes.edgecolor': 'black',
            'axes.titleweight': 'bold',
            'xtick.color': 'black',
            'ytick.color': 'black'
        }
    )

    df['Date'] = pd.to_datetime(df['Date'])
    plot_data = df.tail(90).set_index('Date')

    fib_lines = []
    colors = sns.color_palette("Set2", n_colors=len(levels))
    for (name, level), color in zip(levels.items(), colors):
        mask = plot_data.index >= swing_low_date
        line = pd.Series(index=plot_data.index, dtype='float64')
        line[mask] = level
        fib_lines.append(mpf.make_addplot(line, color=color, linestyle='--', width=1.2))

    # Plot chart
    fig, axlist = mpf.plot(
        plot_data,
        type='candle',
        style=s,
        title=f"\n📊 {symbol} - Fibonacci Retracement (Recent Swing)",
        ylabel="Price",
        addplot=fib_lines,
        figratio=(16, 8),
        figscale=1.25,
        volume=True,
        tight_layout=True,
        returnfig=True
    )

    ax = axlist[0]
    # Annotate Fibonacci levels
    for (name, price), color in zip(levels.items(), colors):
        ax.text(plot_data.index[-1], price, f"  {name}",
                va='center', fontsize=9, color=color,
                bbox=dict(facecolor='white', edgecolor=color, boxstyle='round,pad=0.3'))

    plt.show()

    # Console output
    print(f"\n🔹 Fibonacci Levels for {symbol}")
    for name, price in levels.items():
        print(f"{name:>12}: {price:.2f}")


# -----------------------------------
# 5️⃣ Example Run
# -----------------------------------
if __name__ == "__main__":
    symbol = "RELIANCE.NS"
    df = get_data(symbol, "6mo", "1d")

    swing_high, swing_low, high_date, low_date = find_recent_swing(df, lookback=30)

    print(f"\n📈 Symbol: {symbol}")
    print(f"Recent Swing High: {swing_high:.2f} on {high_date.date()}")
    print(f"Recent Swing Low : {swing_low:.2f} on {low_date.date()}")

    levels = fibonacci_levels(swing_high, swing_low, trend="auto")
    plot_fibonacci(df, swing_high, swing_low, low_date, levels, symbol)
