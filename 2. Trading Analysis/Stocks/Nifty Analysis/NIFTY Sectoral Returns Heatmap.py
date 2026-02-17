import yfinance as yf
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# ===============================
# CONFIG
# ===============================
MONTHS_BACK = 5   # buffer for weekly calc
MIN_WEEKS   = 6   # minimum weeks required

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
# DATE RANGE
# ===============================
end_date   = pd.Timestamp.today().normalize()
start_date = end_date - pd.DateOffset(months=MONTHS_BACK)

# ===============================
# BUILD WEEKLY RETURN SERIES
# ===============================
weekly_series = []

for sector, ticker in SECTORS.items():
    df = yf.download(
        ticker,
        start=start_date,
        end=end_date,
        auto_adjust=False,
        progress=False
    )

    if df.empty:
        print(f"⚠️ No data for {sector}")
        continue

    weekly_close = df['Close'].resample('W-FRI').last()
    weekly_ret   = weekly_close.pct_change().dropna() * 100

    if len(weekly_ret) < MIN_WEEKS:
        print(f"⚠️ Not enough weekly data for {sector}")
        continue

    weekly_ret.name = sector
    weekly_series.append(weekly_ret)

# ===============================
# SAFETY CHECK
# ===============================
if not weekly_series:
    raise ValueError("No valid sector data available for heatmap")

# ===============================
# CREATE MONTHLY-STYLE MATRIX
# ===============================
heatmap_df = pd.concat(weekly_series, axis=1).T

# Keep last ~16 weeks (≈ 4 months)
heatmap_df = heatmap_df.iloc[:, -16:]

# Format week labels (monthly-style)
heatmap_df.columns = heatmap_df.columns.strftime('%d-%b')

# Sort sectors by latest week
heatmap_df = heatmap_df.sort_values(
    by=heatmap_df.columns[-1],
    ascending=False
)

# ===============================
# PLOT HEATMAP
# ===============================
plt.figure(figsize=(18, 7))

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
    "NIFTY Sectoral Weekly Returns Heatmap (Monthly Style – Last 4 Months)",
    fontsize=14
)
plt.xlabel("Week")
plt.ylabel("Sector")

plt.tight_layout()
plt.show()