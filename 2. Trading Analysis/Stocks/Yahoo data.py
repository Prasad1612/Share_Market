import yfinance as yf

symbol = 'TCS'

df = yf.download(symbol+".NS",period="1y", interval="1d", auto_adjust=False, progress=False, multi_level_index=None, rounding=True, threads=True)

if df.empty:
    raise ValueError(f"No data found for {symbol}")

# Reset index to keep Date as a column
df.reset_index(inplace=True)
df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
df.set_index("Date", inplace=True)

print(df.head())