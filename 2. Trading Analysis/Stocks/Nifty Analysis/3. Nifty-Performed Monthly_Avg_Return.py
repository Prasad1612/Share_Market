import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go

#------------------------------------------------------------------------------------------

symbol      = '^NSEI'               # '^NSEI' 'TCS.NS'
period      = 'max'                 #   "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y","5y", "10y", "ytd", "max"
interval    = '1d'                  #   "1m", "2m", "5m", "15m", "30m", "60m"/"1h", "90m",      "1d", "5d", "1wk", "1mo", "3mo"

nifty = yf.download(symbol, period=period, interval=interval, progress=False, auto_adjust=True)
nifty.columns = nifty.columns.get_level_values(0)

monthly_close = nifty['Close'].resample('ME').last()

monthly_returns = monthly_close.pct_change() * 100
# display(monthly_returns)

# Create dataframe with Year-Month
monthly_data = pd.DataFrame({
    'Year': monthly_returns.index.year,
    'Month': monthly_returns.index.month_name(),
    'Monthly Return (%)': monthly_returns.values
})

month_summary = monthly_data.groupby('Month')['Monthly Return (%)'].agg(
    Positive_Count=lambda x: (x > 0).sum(),
    Negative_Count=lambda x: (x < 0).sum(),
    Avg_Return=lambda x: round(x.mean(), 2)
).reset_index()

month_order = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]

month_summary['Month'] = pd.Categorical(month_summary['Month'], categories=month_order, ordered=True)
month_summary = month_summary.sort_values('Positive_Count')

# display(month_summary)

#------------------------------------------------------------------------------------------

from matplotlib import gridspec
import seaborn as sns  # Import seaborn

fig = plt.figure(figsize=(10, 5))
gs = gridspec.GridSpec(1, 3, width_ratios=[1, 1, 1])

cols = ['Positive_Count', 'Negative_Count', 'Avg_Return']
cmaps = ['Greens', 'Reds', 'RdYlGn']

# Set 'Month' as index for heatmap
month_summary_indexed = month_summary.set_index('Month')

for i, (col, cmap) in enumerate(zip(cols, cmaps)):
    ax = plt.subplot(gs[i])
    # Use month_summary_indexed instead of month_summary
    sns.heatmap(
        month_summary_indexed[[col]], annot=True, fmt=".2f",
        cmap=cmap, center=0 if col == 'Avg_Return' else None,
        linewidths=.5, cbar=False, ax=ax, yticklabels=True  # Ensure yticklabels are shown
    )
    ax.set_title(col)
    ax.set_ylabel("")
    ax.set_xlabel("")
    # ax.yaxis.set_tick_params(labelleft=(i == 0))  # Remove this line as we want labels on all heatmaps for clarity

plt.suptitle(f"{symbol} Monthly Performance Heatmap (Last {period})", fontsize=14, y=1.05)
plt.tight_layout()
plt.show()