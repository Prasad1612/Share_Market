import yfinance as yf
import pandas as pd
import numpy as np

# ===============================
# CONFIG
# ===============================
START = "2018-01-01"
END   = None

SECTORS = {
    "AUTO": "^CNXAUTO",
    "BANK": "^NSEBANK",
    "FMCG": "^CNXFMCG",
    "IT": "^CNXIT",
    "METAL": "^CNXMETAL",
    "PHARMA": "^CNXPHARMA",
    "PSU_BANK": "^CNXPSUBANK",
    "REALTY": "^CNXREALTY",
    "ENERGY": "^CNXENERGY",
    "MEDIA": "^CNXMEDIA",
    "HEALTHCARE": "NIFTY_HEALTHCARE.NS",
    "CHEMICALS": "NIFTY_CHEMICALS.NS",
    "Nifty 50": "^NSEI"
}

# ===============================
# INDICATORS
# ===============================
def RSI(close, period=14):
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def ATR(df, period=14):
    tr = pd.concat([
        df['High'] - df['Low'],
        (df['High'] - df['Close'].shift()).abs(),
        (df['Low'] - df['Close'].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

# ===============================
# ANALYSIS
# ===============================
rows = []

for sector, ticker in SECTORS.items():
    df = yf.download(
        ticker,
        start=START,
        end=END,
        auto_adjust=False,
        progress=False
    )

    if df.empty or len(df) < 200:
        continue

    df = df.dropna()

    df['EMA50']  = df['Close'].ewm(span=50).mean()
    df['EMA100'] = df['Close'].ewm(span=100).mean()
    df['RSI']    = RSI(df['Close'])
    df['ATR']    = ATR(df)

    weekly_close  = df['Close'].resample('W-FRI').last()
    monthly_close = df['Close'].resample('ME').last()

    weekly_ret  = weekly_close.pct_change().iloc[-1].item() * 100
    monthly_ret = monthly_close.pct_change().iloc[-1].item() * 100

    trend = "Bullish" if df['EMA50'].iloc[-1] > df['EMA100'].iloc[-1] else "Bearish"

    rows.append({
        "Sector": sector,
        "Weekly %": round(weekly_ret, 2),
        "Monthly %": round(monthly_ret, 2),
        "Trend": trend,
        "RSI": round(float(df['RSI'].iloc[-1]), 1),
        "ATR": round(float(df['ATR'].iloc[-1]), 2)
    })

# ===============================
# FINAL OUTPUT
# ===============================
sector_df = pd.DataFrame(rows)

sector_df = (
    sector_df
    .dropna()
    .sort_values(by="Monthly %", ascending=False)
    .reset_index(drop=True)
)

print("\n📊 NIFTY SECTORAL WEEKLY & MONTHLY ANALYSIS\n")
print(sector_df.to_string(index=False))


#---------------------------------------------------------------------------------------------------------------------------

import matplotlib.pyplot as plt

# ===============================
# SORT DATA (High → Low)
# ===============================
weekly_sorted  = sector_df.sort_values("Weekly %", ascending=False)
monthly_sorted = sector_df.sort_values("Monthly %", ascending=False)

weekly_vals  = weekly_sorted["Weekly %"]
monthly_vals = monthly_sorted["Monthly %"]

weekly_colors  = ['green' if x >= 0 else 'red' for x in weekly_vals]
monthly_colors = ['green' if x >= 0 else 'red' for x in monthly_vals]

# ===============================
# SPLIT VIEW PLOT
# ===============================
fig, axes = plt.subplots(1, 2, figsize=(18, 6), sharey=False)

# ---------- WEEKLY ----------
bars_w = axes[0].bar(
    weekly_sorted["Sector"],
    weekly_vals,
    color=weekly_colors
)

axes[0].axhline(0)
axes[0].set_title("Weekly Returns (%) – High to Low", fontsize=14)
axes[0].set_xlabel("Sector")
axes[0].set_ylabel("Return (%)")
axes[0].tick_params(axis='x', rotation=45)

axes[0].bar_label(
    bars_w,
    labels=[f"{v:.2f}%" for v in weekly_vals],
    padding=3,
    fontsize=9
)

# ---------- MONTHLY ----------
bars_m = axes[1].bar(
    monthly_sorted["Sector"],
    monthly_vals,
    color=monthly_colors
)

axes[1].axhline(0)
axes[1].set_title("Monthly Returns (%) – High to Low", fontsize=14)
axes[1].set_xlabel("Sector")
axes[1].tick_params(axis='x', rotation=45)

axes[1].bar_label(
    bars_m,
    labels=[f"{v:.2f}%" for v in monthly_vals],
    padding=3,
    fontsize=9
)

plt.suptitle("NIFTY Sectoral Performance – Weekly vs Monthly", fontsize=16)
plt.tight_layout()
plt.show()

#---------------------------------------------------------------------------------------------------------------------------

import seaborn as sns
import matplotlib.pyplot as plt

# -------------------------------
# Prepare heatmap data
# -------------------------------
heatmap_df = (
    sector_df
    .set_index("Sector")[["Weekly %", "Monthly %"]]
)

# -------------------------------
# Plot heatmap
# -------------------------------
plt.figure(figsize=(8, 6))

sns.heatmap(
    heatmap_df,
    annot=True,
    fmt=".2f",
    cmap="RdYlGn",
    center=0,
    linewidths=0.5,
    cbar_kws={"label": "Return (%)"}
)

plt.title("NIFTY Sectoral Returns – Weekly & Monthly Heatmap", fontsize=14)
plt.xlabel("Timeframe")
plt.ylabel("Sector")

plt.tight_layout()
plt.show()