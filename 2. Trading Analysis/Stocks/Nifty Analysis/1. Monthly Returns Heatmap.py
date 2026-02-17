import yfinance as yf
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# ticker      = "^NSEI"
# start_date  = "2000-01-01"
# end_date    = "2025-12-27"

# # Download the historical data for Nifty Index
# data = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False, progress=False, multi_level_index=None, rounding=True)

#------------------------------------------------------------------------------------------

symbol      = '^NSEI'               # '^NSEI' 'TCS.NS'
period      = 'max'                 #   "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y","5y", "10y", "ytd", "max"
interval    = '1d'                  #   "1m", "2m", "5m", "15m", "30m", "60m"/"1h", "90m",      "1d", "5d", "1wk", "1mo", "3mo"

# # Download the historical data for Nifty Index
data = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False, multi_level_index=None, rounding=True)

# Resample the data on a monthly basis
data_monthly = data.resample('ME').last()

# Calculate the monthly returns
monthly_returns = data_monthly['Close'].pct_change()

# Convert monthly returns to a pandas DataFrame
monthly_returns_df = pd.DataFrame(monthly_returns, columns=['Close'])

# Pivot the DataFrame to create a matrix of monthly returns by year and month
monthly_returns_matrix = monthly_returns_df.pivot_table(values='Close', index=monthly_returns_df.index.year, columns=monthly_returns_df.index.month)

# Set the column names to the month names
monthly_returns_matrix.columns = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

# Year-end closing prices
yearly_close = data.resample('YE').last()['Close']

# Year-on-Year returns
yearly_returns = yearly_close.pct_change()

# Align index with heatmap
yearly_returns.index = yearly_returns.index.year

# Add to matrix
monthly_returns_matrix['Yearly'] = yearly_returns

# Reduce global font scale
sns.set(font_scale=0.9)

plt.figure(figsize=(12, 10))

sns.heatmap(monthly_returns_matrix, annot=True, cmap='RdYlGn', center=0, fmt='.2%', cbar=False, annot_kws={"size": 10})

# Title & labels
plt.title('Monthly and Yearly Returns by Year and Month', fontsize=16)
plt.xlabel('Month', fontsize=12)
plt.ylabel('Year', fontsize=12)

# Tick label sizes
plt.xticks(fontsize=10)
plt.yticks(fontsize=10)

plt.tight_layout()
plt.show()