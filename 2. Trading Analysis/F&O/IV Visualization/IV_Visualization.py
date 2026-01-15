# ================================================================
# NIFTY / Stock Option Chain IV Visualization (Single Expiry JSON)
# ================================================================

import NseKit
import math
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import plotly.graph_objects as go

# ------------------------------------------------
# NSE Instance
# ------------------------------------------------
nse = NseKit.Nse()

# ------------------------------------------------
# CONFIG
# ------------------------------------------------
symbol      = "HDFCBANK"                  #   "NIFTY" "BANKNIFTY" "FINNIFTY" "MIDCPNIFTY" "NIFTYNXT50" or Stocks "RELIANCE" "HDFCBANK" "TCS" etc
expiry      = "27-Jan-2026"               #   "27-Jan-2026"

csv_save    = False                       #   set False to skip CSV     True Save CSV
html_save   = False                       #   set False to skip HTML    True Save HTML

# ------------------------------------------------
# 1️⃣ Fetch Option Chain (single expiry)
# ------------------------------------------------
data = nse.fno_live_option_chain_raw(symbol, expiry_date=expiry)

# ✅ Updated: handle single-expiry JSON (no 'records' key)
if "data" not in data:
    raise ValueError("Invalid NSE option chain structure")

option_rows = data["data"]
underlying  = data.get("underlyingValue", np.nan)

# ------------------------------------------------
# 2️⃣ Extract IVs (CE & PE) safely
# ------------------------------------------------
rows = []
for rec in option_rows:
    strike = rec.get("strikePrice")
    if strike is None:
        continue

    def safe_iv(opt):
        try:
            iv = float(opt.get("impliedVolatility", np.nan))
            return iv if iv > 0 else np.nan
        except:
            return np.nan

    ce_iv = safe_iv(rec.get("CE", {}))
    pe_iv = safe_iv(rec.get("PE", {}))

    rows.append({
        "Strike": float(strike),
        "CE_IV": ce_iv,
        "PE_IV": pe_iv
    })

df = pd.DataFrame(rows).drop_duplicates("Strike").sort_values("Strike").reset_index(drop=True)
if df.empty:
    raise ValueError("No valid option data found")

# ------------------------------------------------
# 3️⃣ Compute ATM & DTE
# ------------------------------------------------
try:
    underlying = float(underlying)
except:
    underlying = np.nan

if not math.isnan(underlying):
    atm_idx = (df["Strike"] - underlying).abs().idxmin()
else:
    atm_idx = len(df) // 2

atm_strike = int(df.loc[atm_idx, "Strike"])

expiry_str = option_rows[0].get("expiryDates")  # first record expiry
expiry_dt = pd.to_datetime(expiry_str, format="%d-%m-%Y", errors="coerce")
today = pd.Timestamp(datetime.now(timezone.utc).date())
dte = max((expiry_dt.normalize() - today).days if not pd.isna(expiry_dt) else 0, 0)

# ------------------------------------------------
# 4️⃣ Interpolate IVs
# ------------------------------------------------
pivot = df.set_index("Strike")[["CE_IV", "PE_IV"]].interpolate("linear", limit_direction="both").ffill().bfill().reset_index()

strikes = pivot["Strike"].values
ce_iv   = pivot["CE_IV"].values
pe_iv   = pivot["PE_IV"].values

# ------------------------------------------------
# 5️⃣ 3D IV Surface
# ------------------------------------------------
title_suffix = f"{symbol} - {expiry}| ATM: {atm_strike} | DTE: {dte}d"
fig = go.Figure()

fig.add_trace(go.Surface(
    x=np.array([strikes, strikes]),
    y=np.array([[0]*len(strikes), [1]*len(strikes)]),
    z=np.array([pe_iv, ce_iv]),
    colorscale="Viridis",
    opacity=0.45,
    colorbar=dict(title="IV (%)", len=0.5, thickness=10, x=1.05),
    name="IV Surface"
))

fig.add_trace(go.Scatter3d(
    x=strikes, y=[1]*len(strikes), z=ce_iv,
    mode="lines+markers",
    name="Call (CE) IV",
    line=dict(color="limegreen", width=5),
    marker=dict(color="limegreen", size=4)
))

fig.add_trace(go.Scatter3d(
    x=strikes, y=[0]*len(strikes), z=pe_iv,
    mode="lines+markers",
    name="Put (PE) IV",
    line=dict(color="red", width=5),
    marker=dict(color="red", size=4)
))

atm_iv = np.nanmean([ce_iv[atm_idx], pe_iv[atm_idx]])
fig.add_trace(go.Scatter3d(
    x=[atm_strike], y=[0.5], z=[atm_iv],
    mode="markers+text",
    marker=dict(color="yellow", size=7, symbol="diamond"),
    text=[f"ATM {atm_strike}"],
    textposition="top center",
    name="ATM Strike"
))

fig.update_layout(
    title=f"{symbol} IV Surface (3D) — {title_suffix}",
    scene=dict(
        xaxis_title="Strike Price",
        yaxis=dict(title="Option Type (0=PE, 1=CE)", tickvals=[0,1], ticktext=["PE","CE"]),
        zaxis_title="Implied Volatility (%)",
        aspectratio=dict(x=2, y=0.7, z=0.7)
    ),
    template="plotly_dark",
    margin=dict(l=0, r=0, b=0, t=50)
)

# ------------------------------------------------
# 6️⃣ 2D IV Skew
# ------------------------------------------------
fig2 = go.Figure()
fig2.add_trace(go.Scatter(x=strikes, y=ce_iv, mode="lines+markers", name="CE IV", line=dict(color="limegreen", width=2)))
fig2.add_trace(go.Scatter(x=strikes, y=pe_iv, mode="lines+markers", name="PE IV", line=dict(color="red", width=2)))
fig2.add_trace(go.Scatter(x=strikes, y=ce_iv - pe_iv, mode="lines", name="IV Skew (CE-PE)", line=dict(color="yellow", dash="dot", width=1)))

fig2.add_vline(x=atm_strike, line=dict(color="yellow", dash="dash", width=1),
               annotation_text="ATM", annotation_position="top right")

fig2.update_layout(
    title=f"{symbol} IV Skew (2D) — {title_suffix}",
    xaxis_title="Strike Price",
    yaxis_title="Implied Volatility (%)",
    template="plotly_dark",
    xaxis=dict(rangeslider=dict(visible=True)),
    legend=dict(x=0.8, y=0.95),
    margin=dict(t=60)
)

# ------------------------------------------------
# 7️⃣ Save HTML (optional)
# ------------------------------------------------
if html_save:
    fig.write_html(f"{symbol}_IV_3D.html", include_plotlyjs="cdn")
    fig2.write_html(f"{symbol}_IV_2D.html", include_plotlyjs="cdn")
    print(f"\nSaved interactive plots to: *_3D.html and *_2D.html\n")

# ------------------------------------------------
# 8️⃣ Show Charts
# ------------------------------------------------
fig.show()
fig2.show()
