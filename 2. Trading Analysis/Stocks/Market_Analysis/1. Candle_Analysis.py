# pip install NseKit pandas gspread google-auth pytz schedule

from NseKit import Nse
import pandas as pd
from datetime import datetime, timedelta
import pytz
import time as sys_time
import schedule
import gspread
from google.oauth2.service_account import Credentials

nse = Nse()
IST = pytz.timezone("Asia/Kolkata")

INDEX                   = "NIFTY 500"       # "SECURITIES IN F&O"   "NIFTY 50" 
SCHEDULING_ENABLED      = True              # Set to True to enable scheduling, False to disable
SCHEDULING_INTERVAL     = 3                 # Minutes

# =====================================================
# GOOGLE SHEET CONFIG
# =====================================================
SERVICE_ACCOUNT_FILE    = 'Credentials/credentials.json'
SPREADSHEET_NAME        = "Market_Analysis"
WORKSHEET_NAME          = "Candle_Data"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# =====================================================
# MAIN TASK
# =====================================================
def run_tasks():

    # =====================================================
    # TRADING DAY LOGIC
    # =====================================================
    holiday_list = nse.nse_trading_holidays(list_only=True)
    holidays = set(pd.to_datetime(holiday_list, format='%d-%b-%Y', errors='coerce').date)

    def get_prev_trading_day(d):
        d -= timedelta(days=1)
        while d.weekday() >= 5 or d in holidays:
            d -= timedelta(days=1)
        return d

    today = datetime.now(IST).date()
    while today.weekday() >= 5 or today in holidays:
        today -= timedelta(days=1)

    pdate = get_prev_trading_day(today)

    date_str  = today.strftime('%d-%m-%Y')
    pdate_str = pdate.strftime('%d-%m-%Y')

    print("Current Trading Day :", today.strftime('%d-%b-%Y'))
    print("Previous Trading Day:", pdate.strftime('%d-%b-%Y'))
    print("-" * 60)

    # =====================================================
    # STEP 1: LIVE NIFTY 50 DATA (CURRENT DAY)
    # =====================================================
    live_df = nse.index_live_indices_stocks_data(INDEX)
    live_df['SYMBOL'] = live_df['symbol'].str.upper().str.strip()

    live_df = live_df.rename(columns={
        'previousClose': 'PREV_CLOSE_CURR',
        'open': 'OPEN_CURR',
        'dayHigh': 'HIGH_CURR',
        'dayLow': 'LOW_CURR',
        'lastPrice': 'CLOSE_CURR',
        'totalTradedVolume': 'VOLUME_CURR'
    })

    # =====================================================
    # CLEAN LIVE NUMERIC COLUMNS (CRITICAL)
    # =====================================================

    live_num_cols = [
        'PREV_CLOSE_CURR',
        'OPEN_CURR',
        'HIGH_CURR',
        'LOW_CURR',
        'CLOSE_CURR',
        'VOLUME_CURR'
    ]

    for col in live_num_cols:
        live_df[col] = (
            live_df[col]
            .astype(str)
            .str.replace(',', '', regex=False)
            .str.strip()
        )
        live_df[col] = pd.to_numeric(live_df[col], errors='coerce')

    # =====================================================
    # STEP 2: BHAVCOPY (T-1)
    # =====================================================
    prev_df = nse.cm_eod_bhavcopy_with_delivery(pdate_str)
    prev_df = prev_df[prev_df['SERIES'] == 'EQ'].copy()
    prev_df['SYMBOL'] = prev_df['SYMBOL'].str.upper().str.strip()

    num_cols = [
        'OPEN_PRICE','HIGH_PRICE','LOW_PRICE','CLOSE_PRICE',
        'DELIV_PER','NO_OF_TRADES'
    ]
    for c in num_cols:
        prev_df[c] = pd.to_numeric(prev_df[c], errors='coerce')

    # =====================================================
    # STEP 3: PREVIOUS CANDLE TYPE
    # =====================================================
    prev_df['Prev_Candle'] = prev_df.apply(
        lambda x: 'Bullish' if x['CLOSE_PRICE'] > x['OPEN_PRICE']
        else 'Bearish' if x['CLOSE_PRICE'] < x['OPEN_PRICE']
        else 'Neutral',
        axis=1
    )

    # =====================================================
    # STEP 4: MERGE (ONLY NIFTY 50)
    # =====================================================
    df = pd.merge(
        prev_df,
        live_df,
        on='SYMBOL',
        how='inner'
    )

    # =====================================================
    # STEP 5: OPEN POSITION LOGIC
    # =====================================================
    df['Prev_Range'] = df['HIGH_PRICE'] - df['LOW_PRICE']
    df['Prev_Mid']   = df['LOW_PRICE'] + (df['Prev_Range'] / 2)

    def open_position(r):
        if pd.isna(r['OPEN_CURR']) or pd.isna(r['HIGH_PRICE']) or pd.isna(r['LOW_PRICE']):
            return 'Data Error'

        if r['OPEN_CURR'] > r['HIGH_PRICE']:
            return 'Gap Up'
        elif r['OPEN_CURR'] < r['LOW_PRICE']:
            return 'Gap Down'
        elif r['Prev_Candle'] == 'Bullish':
            return 'Premium Open' if r['OPEN_CURR'] >= r['Prev_Mid'] else 'Discount Open'
        elif r['Prev_Candle'] == 'Bearish':
            return 'Discount Open' if r['OPEN_CURR'] >= r['Prev_Mid'] else 'Premium Open'
        return 'Neutral Open'

    df['Open_Position'] = df.apply(open_position, axis=1)

    # =====================================================
    # STEP 6: CANDLE ANALYSIS (LIVE DAY)
    # =====================================================
    def candle_analysis(r):
        rng = r['HIGH_CURR'] - r['LOW_CURR']
        if rng == 0:
            return pd.Series(['Neutral/Doji',0,0,0,'Doji/No Range',0])

        body = abs(r['CLOSE_CURR'] - r['OPEN_CURR'])
        uw = r['HIGH_CURR'] - max(r['OPEN_CURR'], r['CLOSE_CURR'])
        lw = min(r['OPEN_CURR'], r['CLOSE_CURR']) - r['LOW_CURR']

        body_pct = body / rng * 100
        uw_pct   = uw / rng * 100
        lw_pct   = lw / rng * 100

        if r['OPEN_CURR'] == r['LOW_CURR']:
            candle = 'Strong Bullish (Open=Low)'
        elif r['OPEN_CURR'] == r['HIGH_CURR']:
            candle = 'Strong Bearish (Open=High)'
        elif r['CLOSE_CURR'] > r['OPEN_CURR']:
            candle = 'Bullish'
        elif r['CLOSE_CURR'] < r['OPEN_CURR']:
            candle = 'Bearish'
        else:
            candle = 'Neutral/Doji'

        if body_pct > 65 and r['CLOSE_CURR'] > r['OPEN_CURR']:
            sentiment, score = 'Very Bullish', 3
        elif body_pct > 65 and r['CLOSE_CURR'] < r['OPEN_CURR']:
            sentiment, score = 'Very Bearish', -3
        elif lw_pct > 60:
            sentiment, score = 'Buyers Come', 1
        elif uw_pct > 60:
            sentiment, score = 'Sellers Come', -1
        else:
            sentiment, score = 'Neutral / Sideways Day', 0

        return pd.Series([candle, body_pct, uw_pct, lw_pct, sentiment, score])

    df[['Curr_Candle','Body_Ratio','Upper_Wick_Ratio',
        'Lower_Wick_Ratio','Day_Sentiment','Sentiment_Score']] = df.apply(candle_analysis, axis=1)

    # =====================================================
    # STEP 6A: SINGLE CURRENT POSITION (INSTITUTIONAL LOGIC)
    # =====================================================

    def current_position(r):
        close = r['CLOSE_CURR']
        high  = r['HIGH_CURR']
        low   = r['LOW_CURR']

        # -------------------------
        # 1️⃣ PREVIOUS DAY BREAK
        # -------------------------
        if close > r['HIGH_PRICE']:
            return 'Above Prev Day High (Strength)'
        if close < r['LOW_PRICE']:
            return 'Below Prev Day Low (Weakness)'

        # -------------------------
        # 2️⃣ TODAY RANGE POSITION
        # -------------------------
        day_range = high - low
        if day_range > 0:
            if (high - close) / day_range <= 0.20:
                return 'Near Day High (Buyers Control)'
            if (close - low) / day_range <= 0.20:
                return 'Near Day Low (Sellers Control)'

        # -------------------------
        # 3️⃣ PREMIUM / DISCOUNT ZONE
        # -------------------------
        if r['Prev_Candle'] == 'Bullish':
            # Green candle logic
            if r['OPEN_PRICE'] <= close <= r['HIGH_PRICE']:
                return 'Premium Zone (Prev Bullish)'
            if r['LOW_PRICE'] <= close < r['OPEN_PRICE']:
                return 'Discount Zone (Prev Bullish)'
        else:
            # Red candle logic
            if r['CLOSE_PRICE'] <= close <= r['HIGH_PRICE']:
                return 'Premium Zone (Prev Bearish)'
            if r['LOW_PRICE'] <= close < r['CLOSE_PRICE']:
                return 'Discount Zone (Prev Bearish)'

        # -------------------------
        # 4️⃣ FAIR VALUE
        # -------------------------
        return 'Fair Value / Balanced Area'

    df['Current_Position'] = df.apply(current_position, axis=1)

    # =====================================================
    # STEP 7: DELIVERY + INTRADAY CLASSIFICATION
    # =====================================================
    df['Intraday_Activity_Level'] = df['DELIV_PER'].apply(
        lambda x: 'Positional Bias' if x > 70 else
                'Intraday Heavy' if x < 30 else
                'Balanced Activity'
    )

    df['Intraday_Trade_Level'] = pd.qcut(
        df['NO_OF_TRADES'].rank(method='first'),
        3,
        labels=['Low Trades','Medium Trades','High Trades']
    )

    df['Delivery_Sentiment'] = df.apply(
        lambda r: 'More Buyers' if r['DELIV_PER'] > 70 and r['CLOSE_CURR'] > r['OPEN_CURR']
        else 'More Sellers' if r['DELIV_PER'] > 70
        else 'Avg Delivery',
        axis=1
    )

    # =====================================================
    # FINAL OUTPUT
    # =====================================================

    final_cols = [
        'SYMBOL', 'Prev_Candle', 'Open_Position', 'Current_Position', 'Curr_Candle',
        'Day_Sentiment', 'Sentiment_Score', 'DELIV_PER', 'Delivery_Sentiment',
        'Intraday_Activity_Level', 'Intraday_Trade_Level', 'Body_Ratio',
        'Upper_Wick_Ratio', 'Lower_Wick_Ratio'
    ]

    result = df[final_cols].round(2)

    from rich.console import Console
    from rich.table import Table

    # --------------------------
    # FILTER EXTREME CANDLES
    # --------------------------
    extreme_df = result[
        (result['Body_Ratio'] > 95) &
        (result['Day_Sentiment'].isin(['Very Bullish', 'Very Bearish']))
    ].copy()

    # --------------------------
    # SELECT COLUMNS TO SHOW
    # --------------------------
    display_cols = [
        'SYMBOL', 'Prev_Candle', 'Open_Position', 'Curr_Candle',
        'Day_Sentiment', 'DELIV_PER', 'Body_Ratio', 'Upper_Wick_Ratio', 'Lower_Wick_Ratio'
    ]

    extreme_df = extreme_df[display_cols]

    # --------------------------
    # DISPLAY IN TERMINAL WITH COLORS
    # --------------------------
    console = Console()
    table = Table(show_header=True, header_style="bold magenta")

    for col in display_cols:
        table.add_column(col)

    for _, row in extreme_df.iterrows():
        day_sentiment = row['Day_Sentiment']
        if day_sentiment == 'Very Bullish':
            style = "green"
        elif day_sentiment == 'Very Bearish':
            style = "red"
        else:
            style = None

        table.add_row(*[f"[{style}]{row[c]}[/{style}]" if c == 'Day_Sentiment' else str(row[c])
                        for c in display_cols])

    console.print(table)

    # ================= GOOGLE SHEETS =================
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    sh = gc.open(SPREADSHEET_NAME)

    try:
        ws = sh.worksheet(WORKSHEET_NAME)
        ws.clear()
    except:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows="2000", cols="30")

    # Write main table
    ws.update(values=[result.columns.tolist()] + result.values.tolist())

    # Write last updated timestamp in O1
    now_ist = datetime.now(IST)
    ws.update(range_name="O1", values=[[now_ist.strftime('%d-%b-%Y %I:%M %p')]])

    print("✅ Google Sheet updated successfully")
    print(f"\n✅✅ Last updated: {now_ist.strftime('%d-%b-%Y %I:%M %p')} ✅✅\n")

# =====================================================
# SCHEDULER CONTROL
# =====================================================

if __name__ == "__main__":
    if SCHEDULING_ENABLED:
        run_tasks()
        schedule.every(SCHEDULING_INTERVAL).minutes.do(run_tasks)
        while True:
            schedule.run_pending()
            sys_time.sleep(1)
    else:
        run_tasks()



#---------------------------------------------------------------------------------------------------------

# {
#   "Indices Eligible In Derivatives": [
#     "NIFTY 50",
#     "NIFTY BANK",
#     "NIFTY FINANCIAL SERVICES",
#     "NIFTY MIDCAP SELECT",
#     "NIFTY NEXT 50"
#   ],
#   "Broad Market Indices": [
#     "NIFTY 100",
#     "NIFTY 200",
#     "NIFTY 500",
#     "NIFTY INDIA FPI 150",
#     "NIFTY LARGEMIDCAP 250",
#     "NIFTY MICROCAP 250",
#     "NIFTY MIDCAP 100",
#     "NIFTY MIDCAP 150",
#     "NIFTY MIDCAP 50",
#     "NIFTY MIDSMALLCAP 400",
#     "NIFTY SMALLCAP 100",
#     "NIFTY SMALLCAP 250",
#     "NIFTY SMALLCAP 50",
#     "NIFTY TOTAL MARKET",
#     "NIFTY500 LARGEMIDSMALL EQUAL-CAP WEIGHTED",
#     "NIFTY500 MULTICAP 50:25:25"
#   ],
#   "Sectoral Market Indices": [
#     "NIFTY AUTO",
#     "NIFTY CHEMICALS",
#     "NIFTY CONSUMER DURABLES",
#     "NIFTY FINANCIAL SERVICES EX-BANK",
#     "NIFTY FINANCIAL SERVICES 25/50",
#     "NIFTY FMCG",
#     "NIFTY HEALTHCARE INDEX",
#     "NIFTY IT",
#     "NIFTY MEDIA",
#     "NIFTY METAL",
#     "NIFTY MIDSMALL HEALTHCARE",
#     "NIFTY MIDSMALL FINANCIAL SERVICES",
#     "NIFTY MIDSMALL IT & TELECOM",
#     "NIFTY OIL & GAS",
#     "NIFTY PHARMA",
#     "NIFTY PSU BANK",
#     "NIFTY PRIVATE BANK",
#     "NIFTY REALTY",
#     "NIFTY500 HEALTHCARE"
#   ],
#   "Thematic Market Indices": [
#     "NIFTY CAPITAL MARKETS",
#     "NIFTY COMMODITIES",
#     "NIFTY INDIA CONSUMPTION",
#     "NIFTY CORE HOUSING",
#     "NIFTY INDIA SELECT 5 CORPORATE GROUPS (MAATR)",
#     "NIFTY CPSE",
#     "NIFTY ENERGY",
#     "NIFTY EV & NEW AGE AUTOMOTIVE",
#     "NIFTY HOUSING",
#     "NIFTY INDIA DEFENCE",
#     "NIFTY INDIA DIGITAL",
#     "NIFTY INDIA TOURISM",
#     "NIFTY INDIA MANUFACTURING",
#     "NIFTY INFRASTRUCTURE",
#     "NIFTY INDIA INFRASTRUCTURE & LOGISTICS",
#     "NIFTY INDIA INTERNET",
#     "NIFTY IPO",
#     "NIFTY MIDCAP LIQUID 15",
#     "NIFTY MNC",
#     "NIFTY MOBILITY",
#     "NIFTY MIDSMALL INDIA CONSUMPTION",
#     "NIFTY500 MULTICAP INFRASTRUCTURE 50:30:20",
#     "NIFTY500 MULTICAP INDIA MANUFACTURING 50:30:20",
#     "NIFTY INDIA NEW AGE CONSUMPTION",
#     "NIFTY NON-CYCLICAL CONSUMER",
#     "NIFTY PSE",
#     "NIFTY RURAL",
#     "NIFTY SERVICES SECTOR",
#     "NIFTY SHARIAH 25",
#     "NIFTY SME EMERGE",
#     "NIFTY INDIA CORPORATE GROUP INDEX - TATA GROUP 25% CAP",
#     "NIFTY TRANSPORTATION & LOGISTICS",
#     "NIFTY WAVES",
#     "NIFTY100 ENHANCED ESG",
#     "NIFTY100 ESG",
#     "NIFTY100 LIQUID 15",
#     "NIFTY50 SHARIAH",
#     "NIFTY500 SHARIAH"
#   ],
#   "Strategy Market Indices": [
#     "NIFTY ALPHA 50",
#     "NIFTY ALPHA LOW-VOLATILITY 30",
#     "NIFTY ALPHA QUALITY LOW-VOLATILITY 30",
#     "NIFTY ALPHA QUALITY VALUE LOW-VOLATILITY 30",
#     "NIFTY DIVIDEND OPPORTUNITIES 50",
#     "NIFTY GROWTH SECTORS 15",
#     "NIFTY HIGH BETA 50",
#     "NIFTY LOW VOLATILITY 50",
#     "NIFTY MIDCAP150 QUALITY 50",
#     "NIFTY500 MULTICAP MOMENTUM QUALITY 50",
#     "NIFTY QUALITY LOW-VOLATILITY 30",
#     "NIFTY SMALLCAP250 QUALITY 50",
#     "NIFTY TOTAL MARKET MOMENTUM QUALITY 50",
#     "NIFTY TOP 10 EQUAL WEIGHT",
#     "NIFTY TOP 15 EQUAL WEIGHT",
#     "NIFTY TOP 20 EQUAL WEIGHT",
#     "NIFTY100 ALPHA 30",
#     "NIFTY100 EQUAL WEIGHT",
#     "NIFTY100 LOW VOLATILITY 30",
#     "NIFTY100 QUALITY 30",
#     "NIFTY200 ALPHA 30",
#     "NIFTY200 QUALITY 30",
#     "NIFTY200 VALUE 30",
#     "NIFTY200 MOMENTUM 30",
#     "NIFTY50 EQUAL WEIGHT",
#     "NIFTY50 VALUE 20",
#     "NIFTY500 EQUAL WEIGHT",
#     "NIFTY500 FLEXICAP QUALITY 30",
#     "NIFTY500 LOW VOLATILITY 50",
#     "NIFTY500 MULTIFACTOR MQVLV 50",
#     "NIFTY500 QUALITY 50",
#     "NIFTY500 VALUE 50",
#     "NIFTY500 MOMENTUM 50",
#     "NIFTY MIDCAP150 MOMENTUM 50",
#     "NIFTY MIDSMALLCAP400 MOMENTUM QUALITY 100",
#     "NIFTY SMALLCAP250 MOMENTUM QUALITY 100"
#   ],
#   "Others": [
#     "PERMITTED TO TRADE",
#     "SECURITIES IN F&O"
#   ]
# }