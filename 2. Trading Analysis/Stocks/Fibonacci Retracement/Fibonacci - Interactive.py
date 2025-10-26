import yfinance as yf
import pandas as pd
import plotly.graph_objects as go

# -------------------------
# Fetch data
# -------------------------
symbol = "RELIANCE.NS"
period = "6mo"
interval = "1d"

df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False)
df = df.reset_index()

if isinstance(df.columns, pd.MultiIndex):
    df.columns = [col[0] if col[1] == symbol else col[1] for col in df.columns]

if 'Date' not in df.columns:
    df.rename(columns={df.columns[0]: 'Date'}, inplace=True)

df = df.sort_values('Date').reset_index(drop=True)

# -------------------------
# Detect swing high & low safely
# -------------------------
lookback = 30
recent = df.tail(lookback)
swing_high = float(recent['High'].max())
swing_low = float(recent['Low'].min())

swing_high_idx = recent['High'].idxmax()
swing_low_idx  = recent['Low'].idxmin()

swing_high_date = recent.loc[swing_high_idx, 'Date']
swing_low_date  = recent.loc[swing_low_idx, 'Date']

# -------------------------
# Fibonacci levels
# -------------------------
diff = swing_high - swing_low
fib_levels = {
    '0.0% (High)': swing_high,
    '23.6%': swing_high - 0.236*diff,
    '38.2%': swing_high - 0.382*diff,
    '50.0%': swing_high - 0.5*diff,
    '61.8%': swing_high - 0.618*diff,
    '78.6%': swing_high - 0.786*diff,
    '100.0% (Low)': swing_low
}

colors = ['gold', 'orange', 'tomato', 'limegreen', 'cyan', 'magenta', 'blue']

# -------------------------
# Plot candlestick + volume
# -------------------------
fig = go.Figure()

# Candlestick
fig.add_trace(go.Candlestick(
    x=df['Date'],
    open=df['Open'],
    high=df['High'],
    low=df['Low'],
    close=df['Close'],
    name='Price'
))

# Volume as bar chart
fig.add_trace(go.Bar(
    x=df['Date'],
    y=df['Volume'],
    name='Volume',
    marker_color='lightgrey',
    yaxis='y2'
))

# Fibonacci lines (start from swing_low_date)
for (level_name, price), color in zip(fib_levels.items(), colors):
    # Only dates after swing_low_date
    fib_dates = df[df['Date'] >= swing_low_date]['Date']
    fig.add_trace(go.Scatter(
        x=fib_dates,
        y=[float(price)]*len(fib_dates),
        mode='lines',
        line=dict(color=color, dash='dash', width=1.5),
        name=f"{level_name}: {float(price):.2f}"
    ))

# -------------------------
# Layout
# -------------------------
fig.update_layout(
    title=f"{symbol} - Interactive Fibonacci Retracement",
    xaxis_title="Date",
    yaxis_title="Price",
    template="plotly_white",
    legend=dict(x=1, y=1, traceorder='normal', font=dict(size=12)),
    hovermode="x unified",
    yaxis=dict(domain=[0.2, 1]),   # Candlestick above
    yaxis2=dict(domain=[0, 0.2], title='Volume', showgrid=False)  # Volume below
)

fig.show()
