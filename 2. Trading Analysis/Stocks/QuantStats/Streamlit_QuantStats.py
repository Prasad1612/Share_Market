'''
streamlit run Streamlit_QuantStats.py
'''

import streamlit as st
import quantstats as qs
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Set page configuration
st.set_page_config(
    page_title="QuantStats Interactive Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for premium look
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif;
        background-color: #0e1117;
        color: #fafafa;
    }
    
    .stApp {
        background: radial-gradient(circle at top right, #1a1c24, #0e1117);
    }
    
    /* Headings */
    h1, h2, h3 {
        font-weight: 700 !important;
        letter-spacing: -0.5px;
    }
    
    /* Metric Card Styling */
    div[data-testid="stMetric"] {
        background: rgba(26, 28, 36, 0.7);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        padding: 25px;
        border-radius: 20px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-8px);
        background: rgba(30, 32, 42, 0.9);
        border: 1px solid rgba(0, 180, 255, 0.3);
        box-shadow: 0 12px 40px rgba(0, 180, 255, 0.1);
    }
    [data-testid="stMetricLabel"] p {
        color: #94a3b8 !important;
        font-weight: 600 !important;
        font-size: 0.85rem !important;
        text-transform: uppercase;
        letter-spacing: 1.5px;
    }
    [data-testid="stMetricValue"] div {
        color: #00b4ff !important;
        font-weight: 800 !important;
        font-size: 2.2rem !important;
        text-shadow: 0 0 20px rgba(0, 180, 255, 0.3);
    }
    
    /* Plot Container Styling */
    .stPlotlyChart {
        background: rgba(26, 28, 36, 0.6) !important;
        backdrop-filter: blur(10px);
        border-radius: 24px !important;
        border: 1px solid rgba(255, 255, 255, 0.05) !important;
        padding: 15px !important;
        margin-bottom: 30px !important;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2) !important;
    }
    
    /* Tabs Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 30px;
        background-color: transparent;
        border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    }
    .stTabs [data-baseweb="tab"] {
        height: 60px;
        background-color: transparent;
        border: none;
        color: #94a3b8;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    .stTabs [aria-selected="true"] {
        color: #00b4ff !important;
        font-weight: 700 !important;
        background: linear-gradient(to top, rgba(0, 180, 255, 0.1), transparent);
        border-bottom: 3px solid #00b4ff !important;
    }
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background-color: #11141c;
        border-right: 1px solid rgba(255, 255, 255, 0.05);
    }
    [data-testid="stSidebarNav"] {
        background-color: transparent;
    }
    
    /* Custom button styling */
    .stButton>button {
        background: linear-gradient(135deg, #00b4ff, #007bff);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 10px 25px;
        font-weight: 600;
        transition: all 0.3s ease;
        width: 100%;
    }
    .stButton>button:hover {
        transform: scale(1.02);
        box-shadow: 0 0 25px rgba(0, 180, 255, 0.5);
    }
    </style>
    """, unsafe_allow_html=True)

# --- Sidebar Controls ---
st.sidebar.header("📊 Configuration")

ticker = st.sidebar.text_input("Stock Ticker", value="TCS.NS")
benchmark = st.sidebar.text_input("Benchmark Ticker", value="^NSEI")

periods = ["1y", "2y", "5y", "10y", "max", "ytd"]
selected_period = st.sidebar.selectbox("Analysis Period", options=periods, index=0)

rf_rate = st.sidebar.number_input("Risk-Free Rate (%)", value=0.0, step=0.1) / 100

st.sidebar.markdown("---")
show_full_metrics = st.sidebar.checkbox("Show Detailed Metrics Table", value=False)
generate_report = st.sidebar.button("Generate Full HTML Report")

# --- Data Fetching ---
@st.cache_data(ttl=3600)
def get_data(ticker, benchmark, period):
    try:
        # Fetch returns
        stock_data = qs.utils.download_returns(ticker, period=period)
        bench_data = qs.utils.download_returns(benchmark, period=period)
        
        # Ensure they are aligned
        combined = pd.concat([stock_data, bench_data], axis=1).dropna()
        combined.columns = [ticker, benchmark]
        
        return combined[ticker], combined[benchmark]
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None, None

# --- Native Plotly Charts (Unified Dark Style) ---
import plotly.graph_objects as go
import plotly.express as px
import numpy as np

QS_COLORS = {
    'strategy': '#00b4ff',
    'benchmark': '#94a3b8',
    'positive': '#00ffa3',
    'negative': '#ff4d4d',
    'drawdown': 'rgba(255, 77, 77, 0.2)',
    'bg': '#1a1c24',
    'grid': '#2d333b'
}

PLOTLY_DARK_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family="Outfit, sans-serif", color="#fafafa"),
    xaxis=dict(gridcolor="#2d333b", zerolinecolor="#2d333b"),
    yaxis=dict(gridcolor="#2d333b", zerolinecolor="#2d333b"),
    margin=dict(l=20, r=20, t=60, b=20)
)

def plot_returns_plotly(returns, benchmark=None, title="Cumulative Returns"):
    fig = go.Figure()
    
    # Calculate cumulative returns
    cum_returns = (1 + returns).cumprod() - 1
    fig.add_trace(go.Scatter(x=cum_returns.index, y=cum_returns, mode='lines', 
                             name='Strategy', line=dict(color=QS_COLORS['strategy'], width=3)))
    
    if benchmark is not None:
        cum_bench = (1 + benchmark).cumprod() - 1
        fig.add_trace(go.Scatter(x=cum_bench.index, y=cum_bench, mode='lines', 
                                 name='Benchmark', line=dict(color=QS_COLORS['benchmark'], width=1.5, dash='dash')))
        
    fig.update_layout(
        title=dict(text=title.upper(), font=dict(size=18, weight='bold')),
        xaxis_title="DATE",
        yaxis_title="CUMULATIVE RETURN",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, bgcolor="rgba(0,0,0,0)"),
        **PLOTLY_DARK_LAYOUT
    )
    fig.update_yaxes(tickformat=".0%")
    return fig

def plot_drawdown_plotly(returns, title="Drawdown (Underwater)"):
    dd = qs.stats.to_drawdown_series(returns)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=dd.index, y=dd, fill='tozeroy', 
                             name='Drawdown', line=dict(color=QS_COLORS['negative'], width=1),
                             fillcolor=QS_COLORS['drawdown']))
    fig.update_layout(
        title=dict(text=title, font=dict(size=20)),
        xaxis_title="Date",
        yaxis_title="Drawdown",
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_yaxes(tickformat=".0%")
    return fig

def plot_daily_returns_plotly(returns, benchmark=None, title="Daily Returns"):
    fig = go.Figure()
    
    colors = [QS_COLORS['positive'] if x >= 0 else QS_COLORS['negative'] for x in returns]
    fig.add_trace(go.Bar(x=returns.index, y=returns, name='Strategy', marker_color=colors))
    
    if benchmark is not None:
        fig.add_trace(go.Scatter(x=benchmark.index, y=benchmark, mode='lines', 
                                 name='Benchmark', line=dict(color=QS_COLORS['benchmark'], width=1), opacity=0.5))
        
    fig.update_layout(
        title=dict(text=title, font=dict(size=20)),
        xaxis_title="Date",
        yaxis_title="Return",
        template="plotly_white",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_yaxes(tickformat=".1%")
    return fig

def plot_yearly_returns_plotly(returns, benchmark=None, title="EOY Returns"):
    # Calculate yearly returns manually
    yearly = (1 + returns).groupby(returns.index.year).prod() - 1
    fig = go.Figure()
    
    colors = [QS_COLORS['positive'] if x >= 0 else QS_COLORS['negative'] for x in yearly]
    fig.add_trace(go.Bar(x=yearly.index.astype(str), y=yearly, name='Strategy', marker_color=colors, text=yearly.apply(lambda x: f"{x:.1%}"), textposition='auto'))
    
    if benchmark is not None:
        y_bench = (1 + benchmark).groupby(benchmark.index.year).prod() - 1
        fig.add_trace(go.Bar(x=y_bench.index.astype(str), y=y_bench, name='Benchmark', marker_color=QS_COLORS['benchmark'], opacity=0.6))

    fig.update_layout(
        title=dict(text=title, font=dict(size=20)),
        xaxis_title="Year",
        yaxis_title="Return",
        template="plotly_white",
        barmode='group',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_yaxes(tickformat=".0%")
    return fig

def plot_monthly_heatmap_plotly(returns, title="Monthly Returns (%)"):
    monthly = qs.stats.monthly_returns(returns) * 100
    if 'EOY' in monthly.columns:
        monthly = monthly.drop(columns=['EOY'])
    # Reshape for heatmap
    z = monthly.values
    x = monthly.columns.tolist()
    y = monthly.index.tolist()
    
    fig = go.Figure(data=go.Heatmap(
        z=z, x=x, y=y,
        colorscale='RdYlGn',
        reversescale=False,
        text=np.around(z, 2),
        texttemplate="%{text}%",
        showscale=True
    ))
    
    fig.update_layout(
        title=dict(text=title, font=dict(size=20)),
        xaxis_title="Month",
        yaxis_title="Year",
        template="plotly_white",
        margin=dict(l=20, r=20, t=60, b=20)
    )
    return fig

def plot_rolling_plotly(series, title="Rolling Metric", benchmark=None):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=series.index, y=series, mode='lines', name='Strategy', line=dict(color=QS_COLORS['strategy'], width=2)))
    if benchmark is not None:
         fig.add_trace(go.Scatter(x=benchmark.index, y=benchmark, mode='lines', name='Benchmark', line=dict(color=QS_COLORS['benchmark'], width=1.5, dash='dash')))
    
    fig.update_layout(
        title=dict(text=title, font=dict(size=20)),
        xaxis_title="Date",
        template="plotly_white",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=20, t=60, b=20)
    )
    return fig

def plot_histogram_plotly(returns, title="Daily Returns Distribution"):
    fig = go.Figure()
    fig.add_trace(go.Histogram(x=returns, nbinsx=50, name='Returns', marker_color=QS_COLORS['strategy'], opacity=0.7))
    fig.update_layout(
        title=dict(text=title, font=dict(size=20)),
        xaxis_title="Return",
        yaxis_title="Frequency",
        template="plotly_white",
        margin=dict(l=20, r=20, t=60, b=20)
    )
    fig.update_xaxes(tickformat=".1%")
    return fig

# --- Helper Functions ---
def display_plot(fig, use_container_width=True):
    """Display a Plotly figure in Streamlit."""
    if fig is not None:
        st.plotly_chart(fig, use_container_width=use_container_width)

# --- Main Dashboard ---
st.title("📈 QuantStats Interactive Portfolio Analytics")
st.markdown(f"**Ticker:** `{ticker}` | **Benchmark:** `{benchmark}` | **Period:** `{selected_period}`")

with st.spinner("Fetching data and calculating metrics..."):
    returns, bench_returns = get_data(ticker, benchmark, selected_period)

if returns is not None:
    # --- Metrics Grid ---
    st.columns(1) # Spacing
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        cagr = qs.stats.cagr(returns)
        st.metric("CAGR", f"{cagr:.2%}")
    with col2:
        sharpe = qs.stats.sharpe(returns, rf=rf_rate)
        st.metric("Sharpe Ratio", f"{sharpe:.2f}")
    with col3:
        sortino = qs.stats.sortino(returns, rf=rf_rate)
        st.metric("Sortino Ratio", f"{sortino:.2f}")
    with col4:
        max_drawdown = qs.stats.max_drawdown(returns)
        st.metric("Max Drawdown", f"{max_drawdown:.2%}")
    with col5:
        vol = qs.stats.volatility(returns)
        st.metric("Volatility", f"{vol:.2%}")

    # --- Tabs for Analysis ---
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🚀 Overview", 
        "📈 Performance", 
        "📉 Risk & Drawdown", 
        "🔄 Rolling Metrics",
        "📅 Monthly/Dist",
        "📄 Detailed Metrics"
    ])

    with tab1:
        st.subheader("Performance Snapshot")
        # Snapshot is hard to reproduce in plotly, keep as static image for now but centered
        fig_snap = qs.plots.snapshot(returns, benchmark=bench_returns, show=False)
        st.pyplot(fig_snap)
        plt.close(fig_snap)
            
        st.subheader("Cumulative Returns vs Benchmark")
        display_plot(plot_returns_plotly(returns, bench_returns))

    with tab2:
        col_p1, col_p2 = st.columns(2)
        with col_p1:
            st.subheader("Earnings (Cumulative)")
            # Cumulative earnings is just cumprod returns
            display_plot(plot_returns_plotly(returns, title="Cumulative Earnings"))
        with col_p2:
            st.subheader("Daily Returns")
            display_plot(plot_daily_returns_plotly(returns, benchmark=bench_returns))
            
        st.subheader("EOY Returns")
        display_plot(plot_yearly_returns_plotly(returns, benchmark=bench_returns))

    with tab3:
        st.subheader("Drawdown Analysis")
        display_plot(plot_drawdown_plotly(returns))
        
        st.subheader("Underwater Plot")
        # Drawdown is the underwater plot
        display_plot(plot_drawdown_plotly(returns, title="Underwater Plot"))
        
        col_r1, col_r2 = st.columns(2)
        with col_r1:
            st.subheader("Longest Drawdowns")
            # Drawdown table
            dd_table = qs.stats.to_drawdown_series(returns)
            st.dataframe(qs.stats.drawdown_details(dd_table).head(10), use_container_width=True)
        with col_r2:
            st.subheader("Monthly Returns (%)")
            st.dataframe(qs.stats.monthly_returns(returns) * 100, use_container_width=True)

    with tab4:
        st.subheader("Rolling Beta (vs Benchmark)")
        beta_df = qs.stats.rolling_greeks(returns, bench_returns, periods=126)
        beta_series = beta_df['beta']
        display_plot(plot_rolling_plotly(beta_series, "Rolling Beta (6M)"))
        
        st.subheader("Rolling Sharpe (6-Months)")
        sharpe_series = qs.stats.rolling_sharpe(returns, rolling_period=126)
        display_plot(plot_rolling_plotly(sharpe_series, "Rolling Sharpe (6M)"))
        
        st.subheader("Rolling Volatility (6-Months)")
        vol_series = qs.stats.rolling_volatility(returns, rolling_period=126)
        display_plot(plot_rolling_plotly(vol_series, "Rolling Volatility (6M)"))

    with tab5:
        st.subheader("Monthly Returns Heatmap")
        display_plot(plot_monthly_heatmap_plotly(returns))
            
        st.subheader("Daily Returns Distribution")
        display_plot(plot_histogram_plotly(returns))

    with tab6:
        st.subheader("Complete Performance Metrics")
        metrics_df = qs.reports.metrics(returns, benchmark=bench_returns, rf=rf_rate, display=False)
        st.dataframe(metrics_df, use_container_width=True, height=800)

    # --- Report Generation and Preview ---
    if generate_report:
        with st.spinner("Generating Interactive HTML Report..."):
            report_name = f"report_{ticker}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            # Note: qs.reports.html can be a bit heavy
            qs.reports.html(returns, benchmark=bench_returns, output=report_name, title=f"{ticker} vs {benchmark}")
            
            with open(report_name, 'r', encoding='utf-8') as f:
                html_content = f.read()
                
            st.markdown("### 📄 Report Preview")
            st.components.v1.html(html_content, height=800, scrolling=True)
            
            st.download_button(
                label="📥 Download Full HTML Report",
                data=html_content,
                file_name=report_name,
                mime="text/html"
            )

else:
    st.info("Please enter valid ticker symbols in the sidebar to start analysis.")

st.markdown("---")
st.caption("Powered by QuantStats and Streamlit. Data provided by Yahoo Finance.")
