# pip install yfinance pandas numpy plotly

import yfinance as yf
import pandas as pd
import plotly.graph_objects as go


def seasonal_analysis(
    symbol="^NSEI",
    start="2005-01-01",
    years_to_plot=5
):
    # --------------------------------------------------
    # Download historical data
    # --------------------------------------------------
    df = yf.download(
        symbol,
        start=start,
        auto_adjust=False,
        progress=False
    )

    df = df[['Close']].dropna()
    df['ret'] = df['Close'].pct_change()
    df['Year'] = df.index.year

    # --------------------------------------------------
    # Select years
    # --------------------------------------------------
    all_years = sorted(df['Year'].unique())

    if isinstance(years_to_plot, int):
        years = all_years[-years_to_plot:]
    else:
        years = years_to_plot

    # --------------------------------------------------
    # Build seasonality (Trading-day aligned)
    # --------------------------------------------------
    seasonal = {}
    month_ticks = {}

    for y in years:
        g = df[df['Year'] == y].dropna().copy()
        g['TD'] = range(1, len(g) + 1)

        cum = (1 + g['ret']).cumprod() - 1
        seasonal[y] = pd.Series(cum.values, index=g['TD'])

        # Capture month start trading days (once)
        if not month_ticks:
            g['Month'] = g.index.month
            month_ticks = (
                g.groupby('Month')['TD']
                .first()
                .to_dict()
            )

    seasonal_df = pd.DataFrame(seasonal)

    # --------------------------------------------------
    # Month labels
    # --------------------------------------------------
    month_labels = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
        5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
        9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }

    tick_vals = list(month_ticks.values())
    tick_text = [month_labels[m] for m in month_ticks.keys()]

    # --------------------------------------------------
    # Plotly interactive chart
    # --------------------------------------------------
    fig = go.Figure()

    for y in years:
        fig.add_trace(
            go.Scatter(
                x=seasonal_df.index,
                y=seasonal_df[y] * 100,
                mode='lines',
                line=dict(width=1),
                name=str(y),
                hovertemplate="%{y:.2f}%"
            )
        )

    # Zero line
    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color="gray",
        opacity=0.6
    )

    fig.update_layout(
        title=f"{symbol} – Seasonality (Cumulative %)",
        xaxis=dict(
            title="Month",
            tickmode="array",
            tickvals=tick_vals,
            ticktext=tick_text
        ),
        yaxis_title="Cumulative Return (%)",
        hovermode="x unified",
        template="plotly_dark",
        height=900,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    fig.show()

# # --------------------------------------------------
# # RUN
# # --------------------------------------------------

# # Last 5 years automatically
seasonal_analysis("TCS.NS", years_to_plot=5)

# # Custom years example
# # seasonal_analysis("^NSEI", years_to_plot=[2021, 2022, 2023, 2024, 2025])
