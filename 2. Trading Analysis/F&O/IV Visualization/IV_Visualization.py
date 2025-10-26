# ================================================================
# Enhanced NIFTY Option Chain IV Visualization (3D surface + 2D skew)
# ================================================================

# pip install NseKit pandas numpy plotly scipy

import NseKit
import math
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from scipy.interpolate import griddata
import plotly.graph_objects as go
import os

# Create a NSE instance of NseKit
get = NseKit.Nse()

# ----------------------------
# 1️⃣ Fetch & basic parse
# ----------------------------
symbol      = "NIFTY"                   #   "NIFTY" "BANKNIFTY" "FINNIFTY" "MIDCPNIFTY" "NIFTYNXT50" or Stocks "RELIANCE" "HDFCBANK" "TCS" etc
expiry_date = None                      #   "28-10-2025"              None

csv_save    = True                     #   set False to skip CSV     True Save CSV
html_save   = True                     #   set False to skip HTML    True Save HTML    

data = get.fno_live_option_chain_raw(symbol,expiry_date=expiry_date)

# Validate
if 'records' not in data or 'data' not in data['records']:
    raise ValueError("Unexpected NSE JSON structure. Inspect `data` manually.")

available_expiries = data['records'].get('expiryDates', [])
print(f"\nAvailable expiries ({len(available_expiries)}) : {available_expiries}")
expiry_to_filter = available_expiries[0]  # choose first by default
print("\nUsing expiry:", expiry_to_filter)

# ----------------------------
# 2️⃣ Extract IV data robustly (handles missing keys)
# ----------------------------
rows = []
for rec in data['records']['data']:
    # Some records may contain lists or nested structures; guard access
    strike = rec.get('strikePrice')
    expiry = rec.get('expiryDate')
    # skip if strike/expiry missing
    if strike is None or expiry is None:
        continue
    if expiry != expiry_to_filter:
        continue

    # CE
    ce = rec.get('CE')
    if ce and isinstance(ce, dict):
        iv_ce = ce.get('impliedVolatility')
        # store numeric only
        try:
            iv_ce = float(iv_ce) if iv_ce is not None else np.nan
        except Exception:
            iv_ce = np.nan
    else:
        iv_ce = np.nan

    # PE
    pe = rec.get('PE')
    if pe and isinstance(pe, dict):
        iv_pe = pe.get('impliedVolatility')
        try:
            iv_pe = float(iv_pe) if iv_pe is not None else np.nan
        except Exception:
            iv_pe = np.nan
    else:
        iv_pe = np.nan

    rows.append({'Strike': float(strike), 'CE_IV': iv_ce, 'PE_IV': iv_pe, 'Expiry': expiry})

df = pd.DataFrame(rows)
if df.empty:
    raise ValueError("No option rows for chosen expiry. Verify expiry selection and data payload.")

# Sort and remove duplicates
df = df.sort_values('Strike').drop_duplicates(subset=['Strike']).reset_index(drop=True)

# ----------------------------
# Save CSV snapshot (if enabled)
# ----------------------------
if csv_save:
    out_csv = f"{symbol}_OptionChain_IV_{expiry_to_filter.replace('/', '-')}.csv"
    df.to_csv(out_csv, index=False)
    print(f"\nSaved {len(df)} rows to {out_csv}")

# ----------------------------
# 3️⃣ Compute ATM and DTE (Days to Expiry)
# ----------------------------
underlying = data['records'].get('underlyingValue', np.nan)
try:
    underlying = float(underlying)
except Exception:
    underlying = np.nan

# ATM defined as strike with minimal abs diff
if not math.isnan(underlying):
    atm_idx = (df['Strike'] - underlying).abs().idxmin()
    atm_strike = int(df.loc[atm_idx, 'Strike'])
else:
    atm_strike = int(df['Strike'].iloc[len(df)//2])  # fallback

# Compute DTE using expiry string -> pandas datetime
expiry_dt = pd.to_datetime(expiry_to_filter, dayfirst=False, errors='coerce')
today = pd.Timestamp(datetime.now(timezone.utc).date())  # ✅ timezone-aware
if pd.isna(expiry_dt):
    dte = None
else:
    dte = (expiry_dt.normalize() - today).days
    dte = max(dte, 0)

print(f"\nUnderlying: {underlying} | ATM Strike: {atm_strike} | DTE: {dte} days")

# ----------------------------
# 4️⃣ Prepare pivot and fill/interpolate IVs for both sides
# ----------------------------
pivot = df[['Strike', 'CE_IV', 'PE_IV']].set_index('Strike').sort_index()
# Forward/backfill then linear interpolation to fill small gaps
pivot = pivot.interpolate(method='linear', limit_direction='both')
pivot = pivot.ffill().bfill()
pivot = pivot.reset_index()

# Guarantee numeric arrays for plotting
strikes = pivot['Strike'].values
ce_iv = np.nan_to_num(pivot['CE_IV'].values, nan=np.nanmean(pivot['CE_IV'].values))
pe_iv = np.nan_to_num(pivot['PE_IV'].values, nan=np.nanmean(pivot['PE_IV'].values))

# ----------------------------
# 5️⃣  Plot 3D surface + CE/PE lines + ATM marker
# ----------------------------
title_suffix = f"{symbol} {expiry_to_filter} | ATM: {atm_strike} | DTE: {dte}d"
fig = go.Figure()

# Surface between CE & PE (smaller color scale)
fig.add_trace(go.Surface(
    x=np.array([strikes, strikes]),
    y=np.array([[0]*len(strikes), [1]*len(strikes)]),
    z=np.array([pe_iv, ce_iv]),
    colorscale='Viridis',
    opacity=0.45,
    colorbar=dict(title='IV (%)', len=0.5, thickness=10, x=1.05),  # smaller bar, right side
    name='IV Surface'
))


# CE 3D line (y=1)
fig.add_trace(go.Scatter3d(
    x=strikes, y=[1.0]*len(strikes), z=ce_iv,
    mode='lines+markers', name='Call (CE) IV',
    line=dict(color='limegreen', width=5),
    marker=dict(color='limegreen', size=4),
    hovertemplate='Strike: %{x}<br>CE IV: %{z:.2f}%<extra></extra>'
    
))

# PE 3D line (y=0)
fig.add_trace(go.Scatter3d(
    x=strikes, y=[0.0]*len(strikes), z=pe_iv,
    mode='lines+markers', name='Put (PE) IV',
    line=dict(color='red', width=5),
    marker=dict(color='red', size=4),
    hovertemplate='Strike: %{x}<br>PE IV: %{z:.2f}%<extra></extra>'
    
))

# ATM marker
# Average IV at ATM (CE/PE mean if present)
atm_mean_iv = np.nan
row_atm = df.loc[df['Strike'] == float(atm_strike)]
if not row_atm.empty:
    atm_mean_iv = np.nanmean([row_atm['CE_IV'].values[0], row_atm['PE_IV'].values[0]])
else:
    # fallback: interpolate from pivot
    atm_mean_iv = np.interp(atm_strike, strikes, (ce_iv + pe_iv) / 2.0)

fig.add_trace(go.Scatter3d(
    x=[atm_strike], y=[0.5], z=[float(atm_mean_iv)],
    mode='markers+text',
    name='ATM Strike',
    marker=dict(color='yellow', size=7, symbol='diamond'),
    text=[f'ATM {atm_strike}'],
    textposition='top center',
    hovertemplate='ATM Strike: %{x}<br>IV: %{z:.2f}%<extra></extra>'
))

fig.update_layout(
    title=f'{symbol} IV Surface (3D) — {title_suffix}',
    scene=dict(
        xaxis_title='Strike Price',
        yaxis=dict(title='Option Type (0=PE, 1=CE)', tickvals=[0,1], ticktext=['PE','CE']),
        zaxis_title='Implied Volatility (%)',
        aspectratio=dict(x=2, y=0.7, z=0.7)
    ),
    template='plotly_dark',
    margin=dict(l=0, r=0, b=0, t=50),
    font=dict(size=12)
)

# ----------------------------
# 6️⃣ 2D IV Skew & difference chart
# ----------------------------
fig2 = go.Figure()
fig2.add_trace(go.Scatter(
    x=strikes, y=ce_iv,
    mode='lines+markers', name='CE IV',
    line=dict(color='limegreen', width=2),
    hovertemplate='Strike: %{x}<br>CE IV: %{y:.2f}%<extra></extra>'
))
fig2.add_trace(go.Scatter(
    x=strikes, y=pe_iv,
    mode='lines+markers', name='PE IV',
    line=dict(color='red', width=2),
    hovertemplate='Strike: %{x}<br>PE IV: %{y:.2f}%<extra></extra>'
))
skew = ce_iv - pe_iv
fig2.add_trace(go.Scatter(
    x=strikes, y=skew,
    mode='lines', name='IV Skew (CE - PE)', line=dict(color='yellow', dash='dot', width=1),
    hovertemplate='Strike: %{x}<br>Skew: %{y:.2f}%<extra></extra>'
))

# Visual ATM line
fig2.add_vline(x=atm_strike, line=dict(color='yellow', dash='dash', width=1), annotation_text='ATM', annotation_position="top right")
fig2.update_layout(
    title=f'{symbol} IV Skew (2D) — {title_suffix}',
    xaxis_title='Strike Price',
    yaxis_title='Implied Volatility (%)',
    template='plotly_dark',
    xaxis=dict(rangeslider=dict(visible=True)),
    legend=dict(x=0.8, y=0.95),
    margin=dict(t=60)
)

# ----------------------------
# 7️⃣ Save interactive html (so you can share / open later)
# ----------------------------
html_out = f"{symbol}_IV_Surface_{expiry_to_filter.replace('/', '-')}.html"
if html_save:
    fig.write_html(html_out.replace('.html', '_3D.html'), include_plotlyjs='cdn')
    fig2.write_html(html_out.replace('.html', '_2D.html'), include_plotlyjs='cdn')
    print(f"\nSaved interactive plots to: *_3D.html and *_2D.html\n")

# ----------------------------
# 8️⃣ Show (in Jupyter/Colab this will render)
# ----------------------------
fig.show()
fig2.show()
