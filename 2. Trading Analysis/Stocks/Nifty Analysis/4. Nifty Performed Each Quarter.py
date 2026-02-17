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

quarterly_close = nifty['Close'].resample('QE').last()
quarterly_returns = quarterly_close.pct_change() * 100

quarterly_data = pd.DataFrame({
    'Quarter_End': quarterly_returns.index,
    'Quarterly Return (%)': quarterly_returns.values
})

quarterly_data['Year'] = quarterly_data['Quarter_End'].dt.year
quarterly_data['Quarter'] = quarterly_data['Quarter_End'].dt.quarter

# display(quarterly_data)

quarterly_summary = quarterly_data.groupby('Quarter')['Quarterly Return (%)'].agg(
    Avg_Return=lambda x: round(x.mean(), 2),
    Positive_Count=lambda x: (x > 0).sum(),
    Negative_Count=lambda x: (x < 0).sum(),
    Best_Return=lambda x: round(x.max(), 2),
    Worst_Return=lambda x: round(x.min(), 2)
).reset_index()

quarter_map = {1: "Q1 (Jan-Mar)", 2: "Q2 (Apr-Jun)", 3: "Q3 (Jul-Sep)", 4: "Q4 (Oct-Dec)"}
quarterly_summary['Quarter_Name'] = quarterly_summary['Quarter'].map(quarter_map)

quarterly_summary = quarterly_summary.sort_values('Quarter').reset_index(drop=True)

print(f"\n✅ Quarterly {symbol} Summary (Last {period}):")
print(quarterly_summary)

#------------------------------------------------------------------------------------------

import matplotlib.pyplot as plt
import seaborn as sns

# Set style
sns.set(style="whitegrid", font_scale=1.2)

# Colors
colors = ["#16C784" if val > 0 else "#FF4B4B" for val in quarterly_summary['Avg_Return']]

plt.figure(figsize=(8, 5))
bars = plt.bar(quarterly_summary['Quarter_Name'], quarterly_summary['Avg_Return'], color=colors, width=0.6)

# Adjust annotation position dynamically
for i, val in enumerate(quarterly_summary['Avg_Return']):
    if val >= 0:
        plt.text(i, val + 0.25, f"{val:.2f}%", ha='center', va='bottom', fontsize=11, fontweight='bold')
    else:
        plt.text(i, val - 0.4, f"{val:.2f}%", ha='center', va='top', fontsize=11, fontweight='bold')

# Titles & labels
plt.title(f"{symbol} – Average Quarterly Returns (Last {period})", fontsize=15, fontweight='bold', pad=15)
plt.xlabel("Quarter", fontsize=12)
plt.ylabel("Average Return (%)", fontsize=12)

# Adjust Y-axis limits for better spacing above bars
y_min = min(quarterly_summary['Avg_Return']) - 2
y_max = max(quarterly_summary['Avg_Return']) + 2
plt.ylim(y_min, y_max)

plt.tight_layout()
plt.show()