import yfinance as yf
import pandas as pd
import numpy as np
import plotly.express as px
from tqdm import tqdm
import time

# ======================
# Parameters
# ======================
stocks    = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS"]         #   ['^NSEI']         ["RELIANCE.NS", "TCS.NS", "INFY.NS"]
period    = "5d"                                                        #   "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y","5y", "10y", "ytd", "max"
interval  = "1d"                                                        #   "1m", "2m", "5m", "15m", "30m", "60m"/"1h", "90m",      "1d", "5d", "1wk", "1mo", "3mo"

# start_date = "2023-01-01"
# end_date = "2025-11-07"

decimal_places = 2
output_excel = "Gap_Analysis_Dashboard.xlsx"

month_order = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
weekday_order = ['Mon','Tue','Wed','Thu','Fri']

# ============================
# Functions (High, Low based)
# ============================
def analyze_gaps(df):
    df = df.copy()
    df['Prev_Close'] = df['Close'].shift(1)
    df['Prev_High'] = df['High'].shift(1)
    df['Prev_Low'] = df['Low'].shift(1)

    # Identify Gap Type based on previous day's high/low
    df['Gap_Type'] = np.where(
        df['Open'] > df['Prev_High'], 'Gap Up',
        np.where(df['Open'] < df['Prev_Low'], 'Gap Down', 'Inside Range')
    )

    # Gap Value (magnitude of gap)
    df['Gap'] = np.where(
        df['Gap_Type'] == 'Gap Up', df['Open'] - df['Prev_High'],
        np.where(df['Gap_Type'] == 'Gap Down', df['Prev_Low'] - df['Open'], 0)
    )

    # Sustained definition — confirm continuation in the direction of the gap
    df['Sustained'] = np.where(
        ((df['Gap_Type'] == 'Gap Up') & (df['Close'] > df['Open'])) |
        ((df['Gap_Type'] == 'Gap Down') & (df['Close'] < df['Open'])),
        1, 0
    )

    # Round numeric values
    numeric_cols = df.select_dtypes(include=np.number).columns
    df[numeric_cols] = df[numeric_cols].round(decimal_places)

    return df

def summarize_gaps(df, stock_name):
    df['Month'] = df.index.month_name().str[:3]
    df['Weekday'] = df.index.day_name().str[:3]

    # Month-wise summary
    month_summary = df.groupby(['Month','Gap_Type']).agg(
        Count=('Gap_Type','count'),
        Sustained=('Sustained','sum')
    ).reset_index()
    month_summary['Month'] = pd.Categorical(month_summary['Month'], categories=month_order, ordered=True)
    month_summary = month_summary.sort_values(['Month','Gap_Type'])
    month_summary['%Sustained'] = ((month_summary['Sustained']/month_summary['Count']*100).round(2).astype(str)+'%')
    month_summary['Stock'] = stock_name

    # Weekday-wise summary
    weekday_summary = df.groupby(['Weekday','Gap_Type']).agg(
        Count=('Gap_Type','count'),
        Sustained=('Sustained','sum')
    ).reset_index()
    weekday_summary['Weekday'] = pd.Categorical(weekday_summary['Weekday'], categories=weekday_order, ordered=True)
    weekday_summary = weekday_summary.sort_values(['Weekday','Gap_Type'])
    weekday_summary['%Sustained'] = ((weekday_summary['Sustained']/weekday_summary['Count']*100).round(2).astype(str)+'%')
    weekday_summary['Stock'] = stock_name

    # Overall summary
    overall_summary = df['Gap_Type'].value_counts().to_dict()
    overall_summary['Stock'] = stock_name
    overall_summary['Total_Days'] = len(df)

    return month_summary, weekday_summary, overall_summary

# ======================
# Main Processing
# ======================
all_month = []
all_weekday = []
overall_summary_list = []
daily_data_dict = {}

for stock in tqdm(stocks, desc="Processing all stocks"):
    df = yf.download(stock, period=period, interval=interval, auto_adjust=False, progress=False, multi_level_index=None, rounding=True)
    # df = yf.download(stock, start=start_date, end=end_date, auto_adjust=False, progress=False, multi_level_index=None, rounding=True)
    df = df[['Open','High','Low','Close','Volume']]
    df.index = pd.to_datetime(df.index)
    df = analyze_gaps(df)
    daily_data_dict[stock] = df

    month_summary, weekday_summary, overall_summary = summarize_gaps(df, stock)
    all_month.append(month_summary)
    all_weekday.append(weekday_summary)
    overall_summary_list.append(overall_summary)

    # Pause every 10 stocks to avoid API throttling
    if (stocks.index(stock)+1) % 10 == 0:
        time.sleep(2)

# Combine summaries
month_df = pd.concat(all_month, ignore_index=True)
weekday_df = pd.concat(all_weekday, ignore_index=True)
overall_df = pd.DataFrame(overall_summary_list).fillna(0)

# ======================
# Save to Excel
# ======================
with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
    # Daily sheets
    for stock, df in daily_data_dict.items():
        df.to_excel(writer, sheet_name=f"{stock}_Daily")
    # Month summary
    month_df.to_excel(writer, sheet_name="Month_Summary", index=False)
    # Weekday summary
    weekday_df.to_excel(writer, sheet_name="Weekday_Summary", index=False)
    # Overall summary
    overall_df.to_excel(writer, sheet_name="Overall_Summary", index=False)

print(f"✅ Excel dashboard saved: {output_excel}")

# ============================
# Interactive charts as HTML
# ============================

# Custom color mapping (light shades)
gap_colors = {
    'Gap Up': '#90EE90',     # Light green
    'Gap Down': '#FF7F7F',   # Light red
    'Inside Range': '#BEBEBE'  # Light grey
}

# Month-wise bar chart
fig_month = px.bar(
    month_df,
    x='Month', y='Count',
    color='Gap_Type',
    color_discrete_map=gap_colors,
    facet_col='Stock',
    text='%Sustained',
    title='Month-wise Gap Analysis by Stock'
)
fig_month.update_traces(textposition='outside')
fig_month.update_layout(bargap=0.2)
fig_month.write_html("Month_Gap_Analysis.html")
fig_month.show()

# Weekday-wise bar chart
fig_week = px.bar(
    weekday_df,
    x='Weekday', y='Count',
    color='Gap_Type',
    color_discrete_map=gap_colors,
    facet_col='Stock',
    text='%Sustained',
    title='Weekday-wise Gap Analysis by Stock'
)
fig_week.update_traces(textposition='outside')
fig_week.update_layout(bargap=0.2)
fig_week.write_html("Weekday_Gap_Analysis.html")
fig_week.show()

# Heatmap for Month-wise %Sustained
month_heat = month_df.copy()
month_heat['%Sustained'] = month_heat['%Sustained'].str.rstrip('%').astype(float)

fig_heat = px.density_heatmap(
    month_heat,
    x='Month',
    y='Stock',
    z='%Sustained',
    color_continuous_scale='Viridis',
    text_auto=True,
    title='Month-wise Gap %Sustained Heatmap'
)
fig_heat.update_layout(coloraxis_colorbar_title='% Sustained')
fig_heat.write_html("Month_Heatmap.html")
fig_heat.show()