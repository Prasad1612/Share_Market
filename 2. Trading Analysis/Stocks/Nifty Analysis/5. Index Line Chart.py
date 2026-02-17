# ==========================================
# Stocks – % Performance Comparison (1Y)
# Top / Bottom Ranked | Hide / Show Controls
# ==========================================

import yfinance as yf
import pandas as pd
import time
from tqdm import tqdm
import plotly.graph_objects as go

# =========================
# CONFIG
# =========================
INDEX_NAME = ["^NSEI","^NSEBANK","^CNXIT","^CNXFMCG","^CNXAUTO","^CNXMETAL","^CNXPHARMA","^CNXMEDIA","^CNXPSUBANK","^CNXREALTY","NIFTY_HEALTHCARE.NS","NIFTY_CHEMICALS.NS"]

PERIOD      = "1y"                  # "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"
INTERVAL    = '1d'                  # "1d", "5d", "1wk", "1mo"

BATCH_SIZE  = 10
SLEEP_SEC   = 2
TOP_N       = 3   # top & bottom performers to show

# Map tickers to readable names
INDEX_NAMES_READABLE = {
    "^NSEI": "Nifty 50",
    "^NSEBANK": "Nifty Bank",
    "^CNXIT": "Nifty IT",
    "^CNXFMCG": "Nifty FMCG",
    "^CNXAUTO": "Nifty Auto",
    "^CNXMETAL": "Nifty Metal",
    "^CNXPHARMA": "Nifty Pharma",
    "^CNXMEDIA": "Nifty Media",
    "^CNXPSUBANK": "Nifty PSU Bank",
    "^CNXREALTY": "Nifty Realty",
    "NIFTY_HEALTHCARE.NS": "Nifty Healthcare",
    "NIFTY_CHEMICALS.NS": "Nifty Chemicals"
}

# =========================
# FETCH DATA & % RETURNS
# =========================
pct_df = pd.DataFrame()

for idx, ticker in enumerate(tqdm(INDEX_NAME, desc="Downloading Indices")):
    df = yf.download(
        ticker,
        period=PERIOD,
        interval=INTERVAL,
        progress=False,
        auto_adjust=False
    )

    if not df.empty:
        pct_df[ticker] = (df["Close"] / df["Close"].iloc[0] - 1) * 100

    # Batch sleep
    if (idx + 0) % BATCH_SIZE == 0:
        time.sleep(SLEEP_SEC)

pct_df.dropna(how="all", inplace=True)

# =========================
# RANK STOCKS (LATEST %)
# =========================
latest_returns = pct_df.iloc[-1].sort_values(ascending=False)

top_stocks    = latest_returns.head(TOP_N).index.tolist()
bottom_stocks = latest_returns.tail(TOP_N).index.tolist()

# =========================
# PLOTLY CHART
# =========================
fig = go.Figure()

for stock in latest_returns.index:

    visible_state = True if stock in top_stocks + bottom_stocks else "legendonly"
    display_name = INDEX_NAMES_READABLE.get(stock, stock)

    fig.add_trace(
        go.Scatter(
            x=pct_df.index,
            y=pct_df[stock],
            mode="lines",
            name=f"{display_name} ({latest_returns[stock]:.2f}%)",
            visible=visible_state,
            hovertemplate=
            "<b>%{fullData.name}</b><br>" +
            "Return: %{y:.2f}%<br>" +
            "Date: %{x|%d-%b-%Y}<extra></extra>"
        )
    )

# =========================
# BUTTON CONTROLS
# =========================
total_traces = len(latest_returns)

fig.update_layout(
    updatemenus=[dict(
        type="buttons",
        direction="right",
        x=0.99,
        y=1.18,
        xanchor="right",
        yanchor="top",
        showactive=True,
        buttons=[
            dict(label="Show All", method="update", args=[{"visible": [True]*total_traces}]),
            dict(label="Hide All", method="update", args=[{"visible": ["legendonly"]*total_traces}]),
            dict(
                label="Top Performers",
                method="update",
                args=[{"visible": [True if s in top_stocks else "legendonly" for s in latest_returns.index]}]
            ),
            dict(
                label="Bottom Performers",
                method="update",
                args=[{"visible": [True if s in bottom_stocks else "legendonly" for s in latest_returns.index]}]
            ),
        ],
    )]
)

# =========================
# FINAL LAYOUT
# =========================
fig.add_hline(y=0, line_dash="dash", line_color="gray")

fig.update_layout(
    title="Indices – % Performance Comparison (1Y)",
    xaxis_title="Date",
    yaxis_title="Return (%)",
    template="plotly_white",
    height=650,
    hovermode="closest",
    legend=dict(
        title="Indices (Ranked High → Low)",
        font=dict(size=10),
        itemsizing="constant"
    ),
    yaxis=dict(
        tickformat=".2f",
        ticksuffix="%"
    )
)

fig.show()
