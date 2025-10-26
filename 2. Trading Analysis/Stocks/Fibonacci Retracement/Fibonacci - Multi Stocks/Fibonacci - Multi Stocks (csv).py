import yfinance as yf
import pandas as pd
from tqdm import tqdm

# -------------------------
# Stocks list
# -------------------------
stocks              = ["HDFCBANK.NS", "ICICIBANK.NS", "RELIANCE.NS", "INFY.NS", "TCS.NS"]

lookback            = 60    # last 60 days to detect trend
distance_threshold  = 2.0   # % distance from LTP to include in output

period              = "6mo"
interval            = "1d"

records = []

for symbol in tqdm(stocks, desc="Processing Stocks", unit="stock"):
    df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False)
    
    if df.empty or len(df) < 30:
        print(f"Skipping {symbol}: Not enough data")
        continue

    df = df.reset_index().sort_values('Date').reset_index(drop=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if col[1] == symbol else col[1] for col in df.columns]

    recent = df.tail(lookback).copy()
    recent[['Close', 'High', 'Low']] = recent[['Close', 'High', 'Low']].astype(float)
    
    # Determine trend
    trend = 'Uptrend' if recent['Close'].iloc[-1] > recent['Close'].iloc[0] else 'Downtrend'
    
    # Swings
    last_high, last_low = recent['High'].max(), recent['Low'].min()
    prev_high, prev_low = recent['High'].iloc[:-1].max(), recent['Low'].iloc[:-1].min()
    
    swing_high, swing_low = last_high, last_low
    used_swing = "Current Swing"
    
    if trend == 'Uptrend':
        if recent['High'].iloc[-1] >= last_high or recent['Low'].iloc[-1] <= last_low:
            swing_high, swing_low = prev_high, prev_low
            used_swing = "Prev Swing"
    else:
        if recent['Low'].iloc[-1] <= last_low or recent['High'].iloc[-1] >= last_high:
            swing_high, swing_low = prev_high, prev_low
            used_swing = "Prev Swing"
    
    # Fibonacci levels
    diff = swing_high - swing_low
    fib_levels = {
        '23.60%': swing_high - 0.236 * diff,
        '38.20%': swing_high - 0.382 * diff,
        '50.00%': swing_high - 0.5 * diff,
        '61.80%': swing_high - 0.618 * diff,
        '78.60%': swing_high - 0.786 * diff,
        '100.00%': swing_low
    }

    current_price = float(df['Close'].iloc[-1])

    # Calculate distance & filter
    for fib_label, fib_price in fib_levels.items():
        distance_pct = (fib_price - current_price) / current_price * 100
        if abs(distance_pct) <= distance_threshold:
            records.append({
                "Stock": symbol.replace('.NS',''),
                "Trend": trend,
                "Used_Swing": used_swing,
                "Current_Price": round(current_price, 2),
                "Current_Swing_High": round(last_high, 2),
                "Current_Swing_Low": round(last_low, 2),
                "Prev_Swing_High": round(prev_high, 2),
                "Prev_Swing_Low": round(prev_low, 2),
                "Fib_Level": fib_label,
                "Level_Price": round(fib_price, 2),
                "Distance_from_LTP": f"{distance_pct:+.2f}%"
            })

# Convert to DataFrame
final_df = pd.DataFrame(records)

# Save to CSV
final_df.to_csv("fib_levels_detailed.csv", index=False)

print("\n--- Fibonacci Levels Near Current Price ---\n")
print(final_df)


# ----------------------------------------------------------------#########--------------------------------------------------------------------#

# import yfinance as yf
# import pandas as pd
# from tqdm import tqdm

# # -------------------------
# # Stocks list
# # -------------------------
# stocks = ["HDFCBANK.NS", "ICICIBANK.NS", "RELIANCE.NS", "INFY.NS", "TCS.NS"]

# lookback = 60  # last 60 days to detect trend
# distance_threshold = 2.0  # show all levels

# period = "6mo"
# interval = "1d"

# output = []

# for symbol in tqdm(stocks, desc="Processing Stocks", unit="stock"):
#     df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False)
    
    
#     if df.empty or len(df) < 30:
#         print(f"Skipping {symbol}: Not enough data")
#         continue
    
#     df = df.reset_index().sort_values('Date').reset_index(drop=True)
#     if isinstance(df.columns, pd.MultiIndex):
#         df.columns = [col[0] if col[1] == symbol else col[1] for col in df.columns]

#     recent = df.tail(lookback).copy()
#     recent[['Close', 'High', 'Low']] = recent[['Close', 'High', 'Low']].astype(float)
    
#     # Determine trend
#     trend = 'uptrend' if recent['Close'].iloc[-1] > recent['Close'].iloc[0] else 'downtrend'
    
#     # Detect last & previous swings
#     last_swing_high = recent['High'].max()
#     last_swing_low = recent['Low'].min()
    
#     prev_swing_high = recent['High'].iloc[:-1].max()
#     prev_swing_low = recent['Low'].iloc[:-1].min()
    
#     # Default to current swing
#     swing_high = last_swing_high
#     swing_low = last_swing_low
#     used_swing = "Current Swing"
    
#     # Adjust if last candle equals last swing → use previous swing
#     if trend == 'uptrend':
#         if recent['High'].iloc[-1] >= last_swing_high:
#             swing_high = prev_swing_high
#             used_swing = "Prev Swing"
#         if recent['Low'].iloc[-1] <= last_swing_low:
#             swing_low = prev_swing_low
#             used_swing = "Prev Swing"
#     else:  # downtrend
#         if recent['Low'].iloc[-1] <= last_swing_low:
#             swing_low = prev_swing_low
#             used_swing = "Prev Swing"
#         if recent['High'].iloc[-1] >= last_swing_high:
#             swing_high = prev_swing_high
#             used_swing = "Prev Swing"
    
#     # Fibonacci levels
#     diff = swing_high - swing_low
#     fib_levels = {
#         '23.60%': swing_high - 0.236*diff,
#         '38.20%': swing_high - 0.382*diff,
#         '50.00%': swing_high - 0.5*diff,
#         '61.80%': swing_high - 0.618*diff,
#         '78.60%': swing_high - 0.786*diff,
#         '100.00%': swing_low
#     }
    
#     current_price = float(df['Close'].iloc[-1])

#     # Distance % from Fib levels
#     fib_distances = {}
#     for k, v in fib_levels.items():
#         distance_pct = (v - current_price) / current_price * 100
#         if abs(distance_pct) <= distance_threshold:
#             sign = "+" if distance_pct > 0 else "-"
#             fib_distances[k] = f"{v:.2f} ({sign}{abs(distance_pct):.2f}%)"
#         else:
#             fib_distances[k] = ""
    
#     # Append row
#     row = {
#         "Stock_Code": symbol.replace('.NS',''),
#         "Current_Price": round(current_price, 2),
#         "Used_Swing": used_swing,  # shows which swing was used
#         "Current_Swing": f"H:{last_swing_high:.2f} / L:{last_swing_low:.2f}",
#         "Prev_Swing": f"H:{prev_swing_high:.2f} / L:{prev_swing_low:.2f}",
#         **fib_distances
#     }
#     output.append(row)

# # Convert to DataFrame & save CSV
# output_df = pd.DataFrame(output)
# output_df = output_df[['Stock_Code', 'Current_Price', 'Used_Swing', 'Current_Swing', 'Prev_Swing',
#                        '23.60%', '38.20%', '50.00%', '61.80%', '78.60%', '100.00%']]
# output_df.to_csv("fib_levels_near_price.csv", index=False)

# print("\n--- Fibonacci Levels Near Current Price ---\n")
# print(output_df)
