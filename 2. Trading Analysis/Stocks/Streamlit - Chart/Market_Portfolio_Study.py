'''
streamlit run Market_Portfolio_Study.py
'''
import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import quantstats as qs
import plotly.graph_objects as go
import plotly.express as px
from pypfopt import EfficientFrontier, risk_models, expected_returns
from datetime import datetime, timedelta

# =============================
# PAGE CONFIG & STYLING
# =============================
st.set_page_config(layout="wide", page_title="Market Portfolio Study", page_icon="💹")
qs.extend_pandas()

st.title("💹 Market Portfolio Study")

col_setup, col_metrics_table = st.columns([1, 2.5])

with col_setup:
    st.subheader("Configuration")
    with st.expander("⚙️ Portfolio Settings", expanded=True):
        years = st.slider("Analysis Duration (Years)", 1, 20, 10)
        asset_map = {
            "Nifty 50": "^NSEI",
            "Gold": "GC=F",
            "Silver": "SI=F",
            "SBI 10Yr Gilt (Bonds)": "SETF10GILT.NS"
        }
        selected_base = st.multiselect(
            "Select Assets for Portfolio",
            options=list(asset_map.keys()),
            default=list(asset_map.keys())
        )
        custom_input = st.text_input(
            "Add Custom Tickers (comma separated, e.g. INFY.NS,TCS.NS)",
            "TCS.NS,RELIANCE.NS"
        )
        capital = st.number_input("Total Capital (INR)", value=100000.0, step=1000.0)
        risk_profile = st.selectbox("Risk Profile", ["Conservative", "Moderate", "Aggressive"])
        analyze = st.button("Analyze & Optimize Portfolio", type="primary", use_container_width=True)

# =============================
# ANALYSIS LOGIC
# =============================
if analyze or 'analyzed' in st.session_state:
    st.session_state.analyzed = True
    
    all_tickers = [asset_map[a] for a in selected_base]
    if custom_input:
        all_tickers.extend([t.strip().upper() for t in custom_input.split(",") if t.strip()])

    @st.cache_data(ttl=3600)
    def get_market_data(tickers, years):
        end = datetime.today()
        start = end - timedelta(days=years * 365 + 30)
        data = yf.download(tickers, start=start, end=end, progress=False)
        if data.empty: return pd.DataFrame()
        # Handle single vs multi-ticker download
        if len(tickers) == 1:
            df = data['Adj Close'].to_frame(name=tickers[0]) if 'Adj Close' in data.columns else data['Close'].to_frame(name=tickers[0])
        else:
            df = data['Adj Close'] if 'Adj Close' in data.columns.get_level_values(0) else data['Close']
        return df.dropna()

    prices = get_market_data(all_tickers, years)
    if prices.empty:
        st.error("No data fetched. Check tickers or internet connection.")
        st.stop()

    returns = prices.pct_change().dropna()

    # Optimize Portfolio
    mu = expected_returns.mean_historical_return(prices)
    S = risk_models.sample_cov(prices)
    ef = EfficientFrontier(mu, S)
    if risk_profile == "Conservative":
        ef.min_volatility()
    elif risk_profile == "Moderate":
        ef.efficient_risk(target_volatility=0.15)
    else:
        ef.max_sharpe()
    raw_weights = ef.clean_weights()
    weights = {k: v for k, v in raw_weights.items() if v > 0.01}  # Filter tiny weights

    # Normalize weights to 100% if filtered
    total_w = sum(weights.values())
    weights = {k: v / total_w for k, v in weights.items()}

    # Human-readable asset names (reverse map + custom)
    reverse_map = {v: k for k, v in asset_map.items()}
    display_names = {}
    for tick in prices.columns:
        if tick in reverse_map:
            display_names[tick] = reverse_map[tick]
        else:
            display_names[tick] = tick.replace('.NS', '').replace('.BO', '')

    # Metrics Calculation
    asset_metrics = pd.DataFrame(index=prices.columns)
    for col in prices.columns:
        asset_metrics.loc[col, "Sharpe Ratio"] = qs.stats.sharpe(returns[col])
        asset_metrics.loc[col, "Sortino Ratio"] = qs.stats.sortino(returns[col])
        asset_metrics.loc[col, "Calmar Ratio"] = qs.stats.calmar(returns[col])
        asset_metrics.loc[col, "Max Drawdown (%)"] = qs.stats.max_drawdown(returns[col]) * 100
        asset_metrics.loc[col, "Total Return (%)"] = ((1 + returns[col]).prod() - 1) * 100
        asset_metrics.loc[col, "CAGR (%)"] = qs.stats.cagr(returns[col]) * 100

    # Rename index to display names
    asset_metrics.index = [display_names.get(idx, idx) for idx in asset_metrics.index]

    # =============================
    # ASSET PERFORMANCE METRICS TABLE
    # =============================
    with col_metrics_table:

        # =============================
        # PORTFOLIO TOTALS (CALCULATE FIRST)
        # =============================
        total_projected = 0
        alloc_rows = []

        for tick, w in weights.items():
            asset_name = display_names.get(tick, tick)
            allocation = capital * w

            total_ret_pct = asset_metrics.loc[asset_name, "Total Return (%)"] / 100
            projected = allocation * (1 + total_ret_pct)
            profit = allocation * total_ret_pct

            total_projected += projected

            alloc_rows.append({
                "Asset Name": asset_name,
                "Allocation": f"₹{allocation:,.2f}",
                "Percentage": f"{w*100:.1f}%",
                "CAGR (%)": f"{asset_metrics.loc[asset_name, 'CAGR (%)']:.2f}%",
                "Abs. Return (%)": f"{asset_metrics.loc[asset_name, 'Total Return (%)']:.2f}%",
                "Projected Value": f"₹{projected:,.2f}",
                "Profit": f"₹{profit:,.2f}"
            })

        total_growth_pct = (total_projected / capital - 1) * 100
        portfolio_cagr = ((total_projected / capital) ** (1 / years) - 1) * 100

        # =============================
        # TOTAL PROJECTED VALUE (SHOW FIRST)
        # =============================
        st.markdown(
            f"""
            <div style='text-align:center; padding:4px 6px;
                        background-color:#1e1e1e; border-radius:4px;
                        margin-bottom:6px; line-height:0.5;'>
                <h2 style='margin:0; font-size:16px; color:#00ffaa;'>
                    Total Projected Value
                </h2>
                <h2 style='margin:2px 0; font-size:20px; color:#00ffaa;'>
                    ₹{total_projected:,.2f}
                </h2>
                <p style='margin:0; font-size:12px; color:#aaaaaa;'>
                    ₹{total_projected - capital:,.2f}
                    ({total_growth_pct:+.2f}%),
                    CAGR {portfolio_cagr:.2f}%
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )

        # =============================
        # PIE + SHARPE SUMMARY
        # =============================
        c1, c2 = st.columns(2)

        with c1:
            st.subheader("Recommended Allocation")
            pie_names = [display_names.get(t, t) for t in weights.keys()]
            fig_pie = px.pie(
                names=pie_names,
                values=list(weights.values()),
                hole=0.4,
                color_discrete_sequence=px.colors.sequential.Plasma
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            fig_pie.update_layout(template="plotly_dark", height=330)
            st.plotly_chart(fig_pie, use_container_width=True)

        with c2:
            st.subheader("Asset Efficiency (Sharpe Ratio)")
            sharpe_df = asset_metrics["Sharpe Ratio"].reset_index()
            sharpe_df.columns = ["Asset", "Sharpe Ratio"]
            fig_sharpe = px.bar(
                sharpe_df,
                x="Asset",
                y="Sharpe Ratio",
                color="Sharpe Ratio",
                color_continuous_scale="RdYlGn"
            )
            fig_sharpe.update_layout(
                template="plotly_dark",
                height=300,
                showlegend=False
            )
            st.plotly_chart(fig_sharpe, use_container_width=True)

        st.caption(
            f"Analysis Period: {prices.index[0].date()} "
            f"to {prices.index[-1].date()} (≈ {years:.2f} years)"
        )

        # =============================
        # OPTIMIZED ALLOCATION TABLE
        # =============================
        st.subheader(f"Optimized Allocation ({risk_profile})")
        alloc_df = pd.DataFrame(alloc_rows)
        st.dataframe(
            alloc_df.style.hide(axis="index"),
            use_container_width=True
        )

        # =============================
        # ASSET PERFORMANCE METRICS
        # =============================
        st.subheader("Asset Performance Metrics")

        styled_metrics = asset_metrics.style \
            .background_gradient(
                cmap='RdYlGn',
                subset=[
                    "Sharpe Ratio",
                    "Sortino Ratio",
                    "Calmar Ratio",
                    "Total Return (%)",
                    "CAGR (%)"
                ]
            ) \
            .background_gradient(
                cmap='RdYlGn_r',
                subset=["Max Drawdown (%)"]
            ) \
            .format({
                "Sharpe Ratio": "{:.6f}",
                "Sortino Ratio": "{:.6f}",
                "Calmar Ratio": "{:.6f}",
                "Max Drawdown (%)": "{:.2f}",
                "Total Return (%)": "{:.3f}",
                "CAGR (%)": "{:.3f}"
            })

        st.dataframe(styled_metrics, use_container_width=True)

    st.divider()

    # =============================
    # VISUALIZATIONS SECTION
    # ============================

    # Correlation Matrix
    st.subheader("Correlation Matrix (Daily Returns)")
    fig_corr = px.imshow(
        returns.corr(),
        text_auto=".2f",
        color_continuous_scale="RdBu_r",
        aspect="auto"
    )
    fig_corr.update_layout(template="plotly_dark", height=500)
    st.plotly_chart(fig_corr, use_container_width=True)

    st.header("Charts")

    # Rebased Performance
    st.subheader("Portfolio Components Performance (Rebased to 100)")
    rebased = (prices / prices.iloc[0]) * 100
    rebased.columns = [display_names.get(c, c) for c in rebased.columns]
    fig_line = px.line(rebased)
    fig_line.update_layout(template="plotly_dark", height=500, yaxis_title="Normalized Value (Start = 100)")
    st.plotly_chart(fig_line, use_container_width=True)

    # Drawdown
    st.subheader("Historical Drawdown (%)")
    fig_dd = go.Figure()
    for col in returns.columns:
        cum_ret = (1 + returns[col]).cumprod()
        dd = (cum_ret / cum_ret.cummax() - 1) * 100
        name = display_names.get(col, col)
        fig_dd.add_trace(go.Scatter(x=returns.index, y=dd, name=name, fill='tozeroy'))
    fig_dd.update_layout(template="plotly_dark", height=500, yaxis_title="Drawdown (%)")
    st.plotly_chart(fig_dd, use_container_width=True)

    # Daily Returns Volatility
    st.subheader("Daily Returns (%) - Volatility View")
    fig_vol = go.Figure()
    for col in returns.columns:
        name = display_names.get(col, col)
        fig_vol.add_trace(go.Scatter(x=returns.index, y=returns[col]*100, name=name, mode='lines'))
    fig_vol.update_layout(template="plotly_dark", height=500, yaxis_title="Daily Return (%)")
    st.plotly_chart(fig_vol, use_container_width=True)