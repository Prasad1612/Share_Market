import yfinance as yf
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import NseKit
import time
from tqdm.auto import tqdm

# ===============================
# NSE INDEX CONFIG
# ===============================
get = NseKit.Nse()

INDEX_NAME = "NIFTY IT"

stocks = get.index_live_indices_stocks_data(INDEX_NAME, list_only=True)

# Add .NS for yfinance (exclude index name itself)
stocks_ns = {f"{s}.NS" for s in stocks if s != INDEX_NAME}

# ===============================
# GENERAL CONFIG
# ===============================
MONTHS_BACK = 5        # Data buffer
MIN_WEEKS   = 6        # Minimum data required
BATCH_SIZE  = 10       # yfinance safety
SLEEP_SEC   = 2        # Pause between batches

Stocks = sorted(stocks_ns)

# ===============================
# DATE RANGE
# ===============================
end_date   = pd.Timestamp.today().normalize()
start_date = end_date - pd.DateOffset(months=MONTHS_BACK)

# ===============================
# DOWNLOAD & PROCESS DATA
# ===============================
weekly_series = []
counter = 0

with tqdm(total=len(Stocks), desc="Downloading yfinance data") as pbar:
    for ticker in Stocks:
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False,
            threads=False
        )

        if not df.empty and "Close" in df.columns:
            weekly_close = df["Close"].resample("W-FRI").last()
            weekly_ret   = weekly_close.pct_change().dropna() * 100

            if len(weekly_ret) >= MIN_WEEKS:
                weekly_ret.name = ticker
                weekly_series.append(weekly_ret)

        counter += 1
        pbar.update(1)

        # Rate-limit protection
        if counter % BATCH_SIZE == 0:
            time.sleep(SLEEP_SEC)

# ===============================
# SAFETY CHECK
# ===============================
if not weekly_series:
    raise ValueError("No valid stock data available for heatmap")

# ===============================
# BUILD HEATMAP MATRIX
# ===============================
heatmap_df = pd.concat(weekly_series, axis=1).T

# Keep last ~4 months (≈16 weeks)
heatmap_df = heatmap_df.iloc[:, -16:]

# Format week labels
heatmap_df.columns = heatmap_df.columns.strftime("%d-%b")

# Sort by latest week performance
heatmap_df = heatmap_df.sort_values(
    by=heatmap_df.columns[-1],
    ascending=False
)

# Remove .NS only for display
heatmap_df.index = heatmap_df.index.str.replace(".NS", "", regex=False)

# ===============================
# AUTO FIGURE SIZE (STOCK-BASED)
# ===============================
n_stocks = heatmap_df.shape[0]
n_weeks  = heatmap_df.shape[1]

fig_height = max(6, n_stocks * 0.35)   # height scales with stocks
fig_width  = max(10, n_weeks * 1.1)    # width scales with weeks

# ===============================
# PLOT HEATMAP
# ===============================
plt.figure(figsize=(fig_width, fig_height))

sns.heatmap(
    heatmap_df,
    annot=True,
    fmt=".2f",
    cmap="RdYlGn",
    center=0,
    linewidths=0.4,
    cbar_kws={"label": "Weekly Return (%)"}
)

plt.title(
    f"{INDEX_NAME} – Weekly Returns Heatmap (Last 4 Months)",
    fontsize=14
)
plt.xlabel("Week")
plt.ylabel("Stock")

plt.tight_layout()
plt.show()


#-------------------------------------------------------------------------------------------------------

import plotly.express as px
import numpy as np

# Assume heatmap_df is already defined
max_abs = np.max(np.abs(heatmap_df.values))

fig = px.imshow(
    heatmap_df,
    color_continuous_scale="RdYlGn",
    origin="lower",
    aspect="auto",
    labels=dict(
        x="Week",
        y="Stock",
        color="Weekly Return (%)"
    ),
    zmin=-max_abs,
    zmax=max_abs
)

fig.update_layout(
    title="Weekly Returns Heatmap (Last 4 Months)",
    height=700,
    width=1600,
    # xaxis=dict(tickangle=-45),
    yaxis=dict(autorange="reversed"),
    coloraxis_colorbar=dict(title="Weekly Return (%)")
)

fig.update_traces(
    hovertemplate="<b>%{y}</b><br>Week: %{x}<br>Return: %{z:.2f}%<extra></extra>"
)

fig.show()