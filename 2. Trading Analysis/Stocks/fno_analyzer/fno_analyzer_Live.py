import NseKit
import pandas as pd
import numpy as np
import datetime
import os
import time
import shutil
import glob
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def get_option_history(get, symbol, strike, opt_type, trade_date, expiry, instrument="Stock Options"):
    """Fetches historical option data with safety and formatting."""
    print(f"   Fetching history for {symbol} {strike}{opt_type} ({instrument})...")
    try:
        # Using TRADE_DATE as requested.
        df_hist = get.option_price_volume_data(symbol, instrument, str(strike), opt_type, trade_date, expiry=expiry)
        if df_hist is not None and not df_hist.empty:
            # Clean and ensure numeric
            needed_cols = ['FH_OPEN_INT', 'FH_CHANGE_IN_OI', 'FH_TIMESTAMP', 'FH_UNDERLYING_VALUE']
            for col in needed_cols:
                if col in df_hist.columns and col != 'FH_TIMESTAMP':
                    df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce')
            return df_hist
    except Exception as e:
        print(f"      Error fetching history: {e}")
    return None

def plot_option_charts(df_hist, symbol, strike, opt_type, plot_flag, save_flag, ranking_metric, s1=None, s2=None, r1=None, r2=None, ltp_val=None, ltp_date=None):
    """Plots dual-subplot interactive charts with horizontal S/R dotted lines and conditional coloring."""
    if df_hist is None or df_hist.empty: return

    # Sort and ensure timestamp is correct
    df_hist['FH_TIMESTAMP'] = pd.to_datetime(df_hist['FH_TIMESTAMP'], format='mixed')
    df_hist = df_hist.sort_values('FH_TIMESTAMP')
    # Explicitly format date strings for X-axis to fix scientifique notation issue
    date_labels = df_hist['FH_TIMESTAMP'].dt.strftime('%d-%b-%y')

    # Create figure with vertical subplots
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=(f"OI vs Price", f"OI Change vs Price"),
        specs=[[{"secondary_y": True}], [{"secondary_y": True}]]
    )

    # Subplot 1: OI vs Underlying
    fig.add_trace(
        go.Bar(x=date_labels, y=df_hist['FH_OPEN_INT'], name="Open Int", marker_color='rgba(54, 162, 235, 0.6)'),
        row=1, col=1, secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=date_labels, y=df_hist['FH_UNDERLYING_VALUE'], name="Price", line=dict(color='red', width=2)),
        row=1, col=1, secondary_y=True
    )

    # Subplot 2: OI Change vs Underlying (Conditional Coloring)
    # Light Green for positive, Light Red for negative
    bar_colors = ['rgba(144, 238, 144, 0.7)' if val >= 0 else 'rgba(255, 182, 193, 0.7)' for val in df_hist['FH_CHANGE_IN_OI']]
    
    fig.add_trace(
        go.Bar(x=date_labels, y=df_hist['FH_CHANGE_IN_OI'], name="OI Change", marker_color=bar_colors),
        row=2, col=1, secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=date_labels, y=df_hist['FH_UNDERLYING_VALUE'], name="Price (Sub)", line=dict(color='red', width=2), showlegend=False),
        row=2, col=1, secondary_y=True
    )

    # CMP / LTP Horizontal Line removed as per user request (continuous red line preferred)

    # --- S/R Reference Lines (Interactive Hover) ---
    if opt_type == 'PE' and (s1 or s2):
        # Support Analysis: Plot S1 and S2 in Green
        for lvl, name, color in [(s1, "S1", "green"), (s2, "S2", "limegreen")]:
            if lvl:
                # Add to both subplots with secondary_y=True
                for r in [1, 2]:
                    fig.add_trace(
                        go.Scatter(
                            x=date_labels, y=[lvl]*len(date_labels),
                            name=f"{name} Level",
                            line=dict(color=color, width=1, dash='dot'),
                            showlegend=(r == 1), # Only show in legend once
                            hovertemplate=f"{name}: %{{y:,.2f}}<extra></extra>"
                        ),
                        row=r, col=1, secondary_y=True
                    )
    elif opt_type == 'CE' and (r1 or r2):
        # Resistance Analysis: Plot R1 and R2 in Red
        for lvl, name, color in [(r1, "R1", "darkred"), (r2, "R2", "red")]:
            if lvl:
                # Add to both subplots with secondary_y=True
                for r in [1, 2]:
                    fig.add_trace(
                        go.Scatter(
                            x=date_labels, y=[lvl]*len(date_labels),
                            name=f"{name} Level",
                            line=dict(color=color, width=1, dash='dot'),
                            showlegend=(r == 1), # Only show in legend once
                            hovertemplate=f"{name}: %{{y:,.2f}}<extra></extra>"
                        ),
                        row=r, col=1, secondary_y=True
                    )

    if ltp_date is not None:
        # vertical Dotted Line for Date (No Label)
        try:
            # Format date to match x-axis (DD-Mon-YY, e.g., 13-Jan-26)
            d_obj = datetime.datetime.strptime(ltp_date, "%d-%m-%Y")
            date_label_fmt = d_obj.strftime('%d-%b-%y')
            
            # Check if date exists in x-axis data
            if date_label_fmt in date_labels.values:
                for r in [1, 2]:
                    # Plot Vertical Line (Standard add_vline)
                    fig.add_vline(
                        x=date_label_fmt, 
                        line_width=2, line_dash="dot", line_color="black",
                        row=r, col=1
                    )
            else:
                print(f"      Note: LTP Date {date_label_fmt} not found in chart history.")
        except Exception as e:
            print(f"      Warning: Could not plot vertical date line for {ltp_date}: {e}")

    # Formatting
    fig.update_layout(
        title_text=f"{symbol} {strike}{opt_type} - Historical Analysis ({ranking_metric})",
        hovermode="x unified",
        template="plotly_white",
        height=1000, # Increased height for vertical stacking
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig.update_xaxes(title_text="Date", tickangle=45)
    fig.update_yaxes(title_text="Quantity", secondary_y=False)
    fig.update_yaxes(title_text="Underlying Value", secondary_y=True)

    if save_flag:
        # Save in metric-specific subfolder
        metric_folder = os.path.join("charts", ranking_metric)
        if not os.path.exists(metric_folder): os.makedirs(metric_folder)
        
        # Try-except for kaleido dependency
        try:
            filename = f"{symbol}_{strike}{opt_type}_history.png"
            filepath = os.path.join(metric_folder, filename)
            fig.write_image(filepath, width=1200, height=1000)
            print(f"      Plotly chart saved to {filepath}")
        except Exception as e:
            print(f"      Warning: Could not save image (requires kaleido): {e}")

    if plot_flag:
        fig.show()


def archive_old_reports():
    """Moves existing fno_*.csv files to an archive folder with a timestamp."""
    if not os.path.exists("archive"):
        os.makedirs("archive")
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_files = glob.glob("fno_*.csv")
    
    if csv_files:
        print(f"Archiving {len(csv_files)} old report(s) to 'archive' folder...")
        for f in csv_files:
            filename = os.path.basename(f)
            name, ext = os.path.splitext(filename)
            new_name = f"{name}_{timestamp}{ext}"
            try:
                shutil.move(f, os.path.join("archive", new_name))
            except Exception as e:
                print(f"   Error archiving {f}: {e}")

def clear_charts_folder():
    """Wipes the entire charts folder and its sub-directories for fresh results."""
    if os.path.exists("charts"):
        print("Clearing 'charts' folder (refreshing all subfolders)...")
        try:
            shutil.rmtree("charts")
        except Exception as e:
            print(f"   Warning: Could not fully clear charts folder: {e}")
    os.makedirs("charts", exist_ok=True)

def fetch_and_clean_data(trade_date, use_live=True, ltp_date=None):
    get = NseKit.Nse()
    print(f"Fetching F&O Bhavcopy for {trade_date}...")
    try:
        df_bhav = get.fno_eod_bhav_copy(trade_date)
    except Exception as e:
        print(f"Error fetching bhavcopy: {e}")
        return None, None

    if df_bhav is None or df_bhav.empty:
        print(f"Bhavcopy for {trade_date} is empty or None. Please ensure it's a valid trading date.")
        return None, None

    # Clean Bhavcopy
    df_bhav.columns = [c.strip() for c in df_bhav.columns]
    mapping = {
        'TckrSymb': 'ticker',
        'FinInstrmTp': 'instrm_type',
        'XpryDt': 'xpry_dt',
        'StrkPric': 'strk_prc',
        'OptnTp': 'optn_tp',
        'HghPric': 'high_prc',
        'OpnIntrst': 'opn_intrst',
        'ChngInOpnIntrst': 'chng_in_oi',
        'TtlTrfVal': 'ttl_val',
        'ClsPric': 'clse_prc'
    }
    df_bhav.rename(columns=mapping, inplace=True)
    
    numeric_cols = ['strk_prc', 'high_prc', 'opn_intrst', 'chng_in_oi', 'ttl_val']
    for col in numeric_cols:
        if col in df_bhav.columns:
            # ONLY use .abs() if necessary, but keep sign for ChngInOpnIntrst
            val = pd.to_numeric(df_bhav[col], errors='coerce')
            if col == 'chng_in_oi':
                df_bhav[col] = val
            else:
                df_bhav[col] = val.abs()
    
    if 'instrm_type' in df_bhav.columns:
        df_bhav = df_bhav[df_bhav['instrm_type'].isin(['IDO', 'STO'])]
        print(f"After IDO/STO filter: {df_bhav['ticker'].nunique()} tickers")
    
    if use_live:
        print("Fetching live F&O stock data...")
        try:
            df_live = get.index_live_indices_stocks_data("SECURITIES IN F&O")
            
            # Fetch Index Data
            index_map = {
                "NIFTY 50": "NIFTY",
                "NIFTY BANK": "BANKNIFTY",
                "NIFTY FIN SERVICE": "FINNIFTY",
                "NIFTY MID SELECT": "MIDCPNIFTY",
                "NIFTY NEXT 50": "NIFTYNXT50"
            }
            index_list = []
            for cat, symbol in index_map.items():
                try:
                    df_idx = get.index_live_indices_stocks_data(cat)
                    if df_idx is not None and not df_idx.empty:
                        # Extract the index row
                        if symbol in df_idx.index:
                            row = df_idx.loc[[symbol]].copy()
                        else:
                            row = df_idx.head(1).copy()
                        
                        row['symbol'] = symbol
                        index_list.append(row)
                except Exception as e:
                    print(f"      Warning: Could not fetch {cat}: {e}")
            
            if index_list:
                df_indices = pd.concat(index_list)
                # Ensure symbol is set as index for both
                if 'symbol' in df_live.columns: df_live.set_index('symbol', inplace=True)
                if 'symbol' in df_indices.columns: df_indices.set_index('symbol', inplace=True)
                
                # Combine
                df_live = pd.concat([df_live, df_indices])
                # Remove duplicates if any
                df_live = df_live[~df_live.index.duplicated(keep='first')]
                print(f"Merged live data. Total tickers: {len(df_live)}")
                
        except Exception as e:
            print(f"Error fetching live data: {e}")
            df_live = None
    else:
        print(f"Fetching historical equity bhavcopy for LTP as of {ltp_date}...")
        try:
            df_hist_ltp = get.cm_eod_equity_bhavcopy(ltp_date)
            if df_hist_ltp is not None and not df_hist_ltp.empty:
                # Filter series 'EQ' and rename columns for consistency
                df_hist_ltp = df_hist_ltp[df_hist_ltp['SctySrs'] == 'EQ'].copy()
                df_hist_ltp.rename(columns={'TckrSymb': 'symbol', 'SttlmPric': 'lastPrice'}, inplace=True)
                
                # Filter for only tickers present in F&O bhavcopy
                fno_tickers = df_bhav['ticker'].unique()
                df_hist_ltp = df_hist_ltp[df_hist_ltp['symbol'].isin(fno_tickers)]
                
                # Set index to symbol for consistent lookup
                df_hist_ltp.set_index('symbol', inplace=True)
                df_live = df_hist_ltp
            else:
                print(f"Historical equity bhavcopy for {ltp_date} is empty/None.")
                df_live = None
        except Exception as e:
            print(f"Error fetching historical LTP data: {e}")
            df_live = None

    return df_bhav, df_live

def fno_analysis(ranking_metric, df_bhav, df_live, df_lots, target_expiry, trade_date, plot_charts=False, save_charts=False, ltp_date=None, is_live_oi_mode=False):
    if df_bhav is None or df_live is None:
        print(f"Skipping {ranking_metric} run due to missing data.")
        return

    get = NseKit.Nse()

    metric_map = {
        'OI': 'opn_intrst',
        'OI_CHNG': 'chng_in_oi',
        'VALUE': 'ttl_val'
    }
    sort_column = metric_map.get(ranking_metric, 'opn_intrst')
    
    print(f"\n--- Processing Metric: {ranking_metric} ({sort_column}) ---")
    
    # Filter for Expiry
    if target_expiry not in df_bhav['xpry_dt'].unique():
        print(f"Expiry {target_expiry} not found. Available: {sorted(df_bhav['xpry_dt'].dropna().unique())}")
        return
    
    df_filtered = df_bhav[df_bhav['xpry_dt'] == target_expiry].copy()

    # 3. Calculate Support/Resistance
    sr_levels = {}
    symbols = df_filtered['ticker'].unique()
    
    for symbol in symbols:
        ticker_df = df_filtered[df_filtered['ticker'] == symbol]
        
        # CE -> Resistance (Rank by metric, then pick top 2)
        ce_df = ticker_df[ticker_df['optn_tp'] == 'CE'].sort_values(by=sort_column, ascending=False).head(2)
        r_list = []
        for _, r in ce_df.iterrows():
            r_list.append({'strike': r['strk_prc'], 'val': r['strk_prc'] + r['high_prc'], 'm': r[sort_column]})
            
        # PE -> Support (Rank by metric, then pick top 2)
        pe_df = ticker_df[ticker_df['optn_tp'] == 'PE'].sort_values(by=sort_column, ascending=False).head(2)
        s_list = []
        for _, r in pe_df.iterrows():
            s_list.append({'strike': r['strk_prc'], 'val': r['strk_prc'] - r['high_prc'], 'm': r[sort_column]})
            
        if r_list or s_list:
            # Assign strictly by Rank (as requested: First High is R1/S1)
            sr_levels[symbol] = {
                'R1': r_list[0]['val'] if len(r_list) > 0 else None,
                'R2': r_list[1]['val'] if len(r_list) > 1 else None,
                'S1': s_list[0]['val'] if len(s_list) > 0 else None,
                'S2': s_list[1]['val'] if len(s_list) > 1 else None,
                'R1_Metric': r_list[0]['m'] if len(r_list) > 0 else 0,
                'S1_Metric': s_list[0]['m'] if len(s_list) > 0 else 0,
                'R2_Metric': r_list[1]['m'] if len(r_list) > 1 else 0,
                'S2_Metric': s_list[1]['m'] if len(s_list) > 1 else 0,
                'R1_Strike': r_list[0]['strike'] if len(r_list) > 0 else None,
                'R2_Strike': r_list[1]['strike'] if len(r_list) > 1 else None,
                'S1_Strike': s_list[0]['strike'] if len(s_list) > 0 else None,
                'S2_Strike': s_list[1]['strike'] if len(s_list) > 1 else None,
                'instrm_type': ticker_df['instrm_type'].iloc[0]
            }

    # 5. Analyze Relationship
    results = []
    trend_results = []
    match_count = 0
    
    for ticker, sr in sr_levels.items():
        if ticker not in df_live.index: continue
        try:
            ltp = float(df_live.loc[ticker, 'lastPrice'])
        except: continue
        match_count += 1

        s1, s2 = sr.get('S1'), sr.get('S2')
        r1, r2 = sr.get('R1'), sr.get('R2')
        
        # Calculate zone range (since R1 may be > R2 or vice versa based on rank)
        s_min, s_max = (min(s1, s2), max(s1, s2)) if s1 and s2 else (None, None)
        r_min, r_max = (min(r1, r2), max(r1, r2)) if r1 and r2 else (None, None)

        status = "Range Bound"
        is_opp = False
        
        # Zone check (Correctly handle range regardless of which rank had higher price)
        if s_min and s_max and s_min <= ltp <= s_max:
            status, is_opp = "Inside Support Zone (S1-S2)", True
        elif r_min and r_max and r_min <= ltp <= r_max:
            status, is_opp = "Inside Resistance Zone (R1-R2)", True
        elif s1 and abs(ltp - s1) / s1 <= 0.005: status, is_opp = "Near Support (S1)", True
        elif r1 and abs(ltp - r1) / r1 <= 0.005: status, is_opp = "Near Resistance (R1)", True
        elif r1 and ltp > r1: status = "Above Resistance"
        elif s1 and ltp < s1: status = "Below Support"

        # Chart Generation for Near S1/R1
        if (plot_charts or save_charts) and status in ["Near Support (S1)", "Near Resistance (R1)"]:
            s_type = "PE" if "Support" in status else "CE"
            s_strike = sr.get('S1_Strike') if "Support" in status else sr.get('R1_Strike')
            # Expiry format for history: DD-MM-YYYY (Match target_expiry if it's already in that format)
            # target_expiry is YYYY-MM-DD from config, let's fix it
            exp_obj = datetime.datetime.strptime(target_expiry, '%Y-%m-%d')
            exp_formatted = exp_obj.strftime('%d-%m-%Y')
            
            # Determine instrument type
            inst_type = "Index Options" if sr.get('instrm_type') == 'IDO' else "Stock Options"
            
            df_hist = get_option_history(get, ticker, s_strike, s_type, trade_date, exp_formatted, instrument=inst_type)
            if df_hist is not None:
                # If using live data, append the current LTP as the latest point for the chart
                plot_ltp_date = ltp_date
                if df_live is not None and ticker in df_live.index:
                    # Decide on the timestamp for this data point
                    if ltp_date:
                        # Historical LTP mode
                        target_date_obj = datetime.datetime.strptime(ltp_date, "%d-%m-%Y")
                        is_live_mode = False
                    else:
                        # Live mode
                        target_date_obj = datetime.datetime.now()
                        is_live_mode = True
                    
                    target_str = target_date_obj.strftime("%d-%b-%Y")
                    plot_ltp_date = ltp_date if ltp_date else target_date_obj.strftime("%d-%m-%Y")
                    
                    # --- Fetch Live OI/OI Change ONLY if is_live_oi_mode is True ---
                    live_oi, live_oi_chng = None, None
                    if is_live_mode: # This is the local is_live_mode for determining if we should even consider live (False if ltp_date passed)
                        # 1. Weekend Check (Sat=5, Sun=6)
                        if target_date_obj.weekday() >= 5:
                            is_fetch_allowed = False
                        else:
                            is_fetch_allowed = is_live_oi_mode # Use the global control passed to the function

                        if is_fetch_allowed:
                            try:
                                # Prepare expiry format (e.g., 24-Feb-2026)
                                target_dt = datetime.datetime.strptime(target_expiry, '%Y-%m-%d')
                                oc_exp_match = target_dt.strftime('%d-%b-%Y')
                                
                                # Use fno_live_active_contracts as requested
                                active_contracts = get.fno_live_active_contracts(ticker, expiry_date=oc_exp_match)
                                if active_contracts and isinstance(active_contracts, list):
                                    # Identify Lot Size for scaling (ONLY in live mode)
                                    lot_size = 1
                                    if df_lots is not None and ticker in df_lots['SYMBOL'].values:
                                        try:
                                            lot_col = target_dt.strftime('%b-%y').upper()
                                            if lot_col in df_lots.columns:
                                                lot_size = pd.to_numeric(df_lots.loc[df_lots['SYMBOL'] == ticker, lot_col].iloc[0], errors='coerce') or 1
                                            else:
                                                print(f"      Warning: Lot size column '{lot_col}' not found for {ticker}. Using default lot_size=1.")
                                        except Exception as e:
                                            print(f"      Warning: Could not determine lot size for {ticker}: {e}. Using default lot_size=1.")
                                            lot_size = 1

                                    for contract in active_contracts:
                                        try:
                                            c_strike = float(contract.get('Strike Price', 0))
                                            c_type = contract.get('Option Type')
                                            if c_type == s_type and abs(c_strike - float(s_strike)) < 0.1:
                                                raw_oi = pd.to_numeric(contract.get('OI', 0), errors='coerce') or 0
                                                raw_oi_chng = pd.to_numeric(contract.get('Chng in OI', 0), errors='coerce') or 0
                                                
                                                # Scale by Lot Size ONLY if requested and in live mode
                                                live_oi = raw_oi * lot_size
                                                live_oi_chng = raw_oi_chng * lot_size
                                                break
                                        except: continue
                            except Exception as e:
                                print(f"      Warning: Could not fetch live OI for {ticker}: {e}")

                    # Create a new row to append to historical data
                    new_row = pd.DataFrame([{
                        'FH_TIMESTAMP': target_str,
                        'FH_UNDERLYING_VALUE': ltp,
                        'FH_OPEN_INT': live_oi if live_oi is not None else np.nan,
                        'FH_CHANGE_IN_OI': live_oi_chng if live_oi_chng is not None else np.nan
                    }])
                    
                    # Correctly update or append to df_hist
                    hist_ts = pd.to_datetime(df_hist['FH_TIMESTAMP'], format='mixed')
                    target_date = target_date_obj.date()
                    
                    # Search for existing row with this date
                    match_idx = df_hist.index[hist_ts.dt.date == target_date]
                    
                    if not match_idx.empty:
                        # Update the existing row (especially the LTP)
                        idx = match_idx[0]
                        df_hist.at[idx, 'FH_UNDERLYING_VALUE'] = ltp
                        # Update OI only if we successfully fetched live scaled values
                        if live_oi is not None:
                            df_hist.at[idx, 'FH_OPEN_INT'] = live_oi
                            df_hist.at[idx, 'FH_CHANGE_IN_OI'] = live_oi_chng
                    else:
                        # No match, safe to append
                        df_hist = pd.concat([df_hist, new_row], ignore_index=True)

                plot_option_charts(
                    df_hist, ticker, s_strike, s_type, plot_charts, save_charts, ranking_metric,
                    s1=sr.get('S1'), s2=sr.get('S2'), r1=sr.get('R1'), r2=sr.get('R2'),
                    ltp_val=ltp, ltp_date=plot_ltp_date
                )
                
                # --- NEW TREND LOGIC ---
                # Ensure sorted by date (Plotly already does this, but we need it here)
                df_hist_sorted = df_hist.sort_values('FH_TIMESTAMP')
                last_3 = df_hist_sorted.tail(3)['FH_CHANGE_IN_OI'].tolist()
                
                trend_results.append({
                    'Symbol': ticker, 'Level': status, 'Strike': s_strike,
                    'OI_Chng_Latest': last_3[-1] if len(last_3) >= 1 else 0,
                    'OI_Chng_Prev1': last_3[-2] if len(last_3) >= 2 else 0,
                    'OI_Chng_Prev2': last_3[-3] if len(last_3) >= 3 else 0,
                })
                # -----------------------
                
                time.sleep(1) # Rate limit delay
            
        results.append({
            'Symbol': ticker, 'LTP': ltp, 
            'S2_Strike': sr.get('S2_Strike'), 'S2': s2, 
            'S1_Strike': sr.get('S1_Strike'), 'S1': s1, 
            'R1_Strike': sr.get('R1_Strike'), 'R1': r1, 
            'R2_Strike': sr.get('R2_Strike'), 'R2': r2,
            'Relation': status, 'Opportunity': "YES" if is_opp else "NO",
            f'S1_{ranking_metric}': sr.get('S1_Metric'), f'R1_{ranking_metric}': sr.get('R1_Metric'),
            f'S2_{ranking_metric}': sr.get('S2_Metric'), f'R2_{ranking_metric}': sr.get('R2_Metric')
        })

    full_df = pd.DataFrame(results)
    
    if ranking_metric == "OI":
        try:
            full_df.to_csv("charts/fno_full_analysis.csv", index=False)
            print("Full analysis (OI based) saved to fno_full_analysis.csv")
        except Exception as e:
            print(f"Error saving fno_full_analysis.csv: {e}")

    opp_df = full_df[full_df['Opportunity'] == "YES"].copy()
    if not opp_df.empty:
        opp_df.drop(columns=['Opportunity'], inplace=True)
    
    output_file = f"charts/fno_sr_analysis_{ranking_metric}.csv"
    try:
        opp_df.to_csv(output_file, index=False)
        print(f"Analysis ({ranking_metric}) saved to {output_file}")
        if not opp_df.empty:
            # Show strikes in console too
            cols_to_show = ['Symbol', 'LTP', 'S2_Strike', 'S1_Strike', 'S1', 'R1_Strike', 'R1', 'Relation']
            print(opp_df[cols_to_show].head(15))
    except Exception as e:
        print(f"Error saving {output_file}: {e}")
    
    if opp_df.empty:
        print(f"No stocks currently inside or near {ranking_metric}-based S/R zones.")

    if trend_results:
        trend_file = f"charts/fno_3day_trend_analysis_{ranking_metric}.csv"
        try:
            pd.DataFrame(trend_results).to_csv(trend_file, index=False)
            print(f"3-Day Trend Analysis saved to {trend_file}")
        except Exception as e:
            print(f"Error saving {trend_file}: {e}")

if __name__ == "__main__":
    # --- HARDCORE CONFIG ---
    TRADE_DATE  = "27-01-2026" 
    EXPIRY_DATE = "2026-02-24"

    LTP_HIST_DATE = "05-02-2026" # Date for historical LTP (if USE_LIVE_DATA is False)

    METRICS       = ['OI', 'OI_CHNG', 'VALUE']                 #  ['OI']   or   ['OI', 'OI_CHNG', 'VALUE']

    # LTP Configuration
    USE_LIVE_DATA = True   # Set to False to use historical LTP from Equity Bhavcopy
    IS_LIVE_MODE  = True  # Set to True to fetch and scale real-time OI (False = Historical Only)

    PLOT_CHARTS   = False  # Set to True to see interactive plot windows
    SAVE_CHARTS   = True   # Set to True to save PNG images
    
    # 1. Archive & Clean-up old results
    # archive_old_reports()
    clear_charts_folder()
    
    # 2. Fetch Data ONCE
    bhav_data, live_data = fetch_and_clean_data(TRADE_DATE, USE_LIVE_DATA, LTP_HIST_DATE)
    
    # Fetch Lot Sizes ONCE
    df_lots = None
    try:
        get_obj = NseKit.Nse()
        df_lots = get_obj.fno_eom_lot_size()
        if df_lots is not None and not df_lots.empty:
            df_lots.columns = [c.strip() for c in df_lots.columns]
            if 'SYMBOL' in df_lots.columns:
                df_lots['SYMBOL'] = df_lots['SYMBOL'].str.strip()
    except Exception as e:
        if USE_LIVE_DATA: # Only warn if it's critical for live scaling
            print(f"Warning: Could not fetch lot sizes: {e}")

    # Process each metric using shared data
    if bhav_data is not None and live_data is not None:
        for m in METRICS:
            # Pass LTP_HIST_DATE if using historical data
            hist_date = LTP_HIST_DATE if not USE_LIVE_DATA else None
            fno_analysis(m, bhav_data, live_data, df_lots, EXPIRY_DATE, TRADE_DATE, PLOT_CHARTS, SAVE_CHARTS, ltp_date=hist_date, is_live_oi_mode=IS_LIVE_MODE)
