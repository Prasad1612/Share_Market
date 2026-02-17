import yfinance as yf
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

symbol      = '^NSEI'               # '^NSEI' 'TCS.NS'
period      = 'max'                 #   "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y","5y", "10y", "ytd", "max"
interval    = '1d'                  #   "1m", "2m", "5m", "15m", "30m", "60m"/"1h", "90m",      "1d", "5d", "1wk", "1mo", "3mo"

# # Download the historical data for Nifty Index
data = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False, multi_level_index=None, rounding=True)

# Resample the data on a quarterly basis
data_quarterly = data.resample('QE').last()

# Calculate the quarterly returns
quarterly_returns = data_quarterly['Close'].pct_change()

# Convert quarterly returns to a pandas DataFrame
quarterly_returns_df = pd.DataFrame(quarterly_returns)

# Pivot the DataFrame to create a matrix of quarterly returns by year and quarter
quarterly_returns_matrix = quarterly_returns_df.pivot_table(values='Close', index=quarterly_returns_df.index.year, columns=quarterly_returns_df.index.quarter)

# Set the column names to the quarter names
quarterly_returns_matrix.columns = ['Q1', 'Q2', 'Q3', 'Q4']

# Year-end closing prices
yearly_close = data.resample('YE').last()['Close']

# Year-on-Year returns
yearly_returns = yearly_close.pct_change()

# Align index with heatmap
yearly_returns.index = yearly_returns.index.year

# Add the yearly returns to the matrix as a new column
quarterly_returns_matrix['Yearly'] = yearly_returns

# Set the font scale
sns.set(font_scale=0.9)

# Plot the heatmap using seaborn
plt.figure(figsize=(10, 8))
sns.heatmap(quarterly_returns_matrix, annot=True, cmap='RdYlGn', center=0, fmt='.2%', cbar=False, annot_kws={"size": 10})

# Title & labels
plt.title('Quarterly and Yearly Returns by Year and Quarter', fontsize=16)
plt.xlabel('Quarter', fontsize=12)
plt.ylabel('Year', fontsize=12)

# Tick label sizes
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)

plt.tight_layout()
plt.show()