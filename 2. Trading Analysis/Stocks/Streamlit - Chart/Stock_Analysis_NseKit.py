'''
streamlit run Stock_Analysis_NseKit.py

pip install NseKit pandas plotly kaleido pandas_market_calendars --quiet
pip install --upgrade NseKit pandas --quiet

'''

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import NseKit
import streamlit as st
import warnings
import os
import time
import re
import pathlib

RUN_WITH_CODE = False  # 🔀 Switch: True → Run directly, False → Run via streamlit

# Create NSE instance
get = NseKit.Nse()

# Suppress FutureWarnings
warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------- Helper Functions ---------------- #

def update_default_dates(new_from, new_to):
    """Update the default dates in this .py file."""
    file_path = pathlib.Path(__file__)
    code = file_path.read_text(encoding="utf-8")
    matches = list(re.finditer(r'pd\.to_datetime\("20\d{2}-\d{2}-\d{2}"\)', code))
    if len(matches) >= 2:
        old_from = matches[0].group(0)
        old_to = matches[1].group(0)
        new_from_expr = f'pd.to_datetime("{new_from}")'
        new_to_expr = f'pd.to_datetime("{new_to}")'
        if new_from_expr != old_from:
            code = code[:matches[0].start()] + new_from_expr + code[matches[0].end():]
        matches = list(re.finditer(r'pd\.to_datetime\("20\d{2}-\d{2}-\d{2}"\)', code))
        if new_to_expr != old_to:
            code = code[:matches[1].start()] + new_to_expr + code[matches[1].end():]
    file_path.write_text(code, encoding="utf-8")

def format_indian_number(num):
    if pd.isna(num):
        return "0"
    num = float(num)
    if num >= 10000000:
        return f"{(num / 10000000):.2f} Cr"
    elif num >= 100000:
        return f"{(num / 100000):.2f} L"
    elif num >= 1000:
        return f"{(num / 1000):.2f} K"
    else:
        return f"{num:.0f}"

# ---------------- Streamlit UI ---------------- #
st.set_page_config(page_title="Stock Price & Volume Dashboard", layout="wide")
st.title(f"📈 Stock - Price, Volume & Deliverables")

# Sidebar inputs
st.sidebar.header("⚙️ Settings")

# Example NSE stock list (trimmed for demo)
stock_list = ["Enter Manually", "360ONE", "3MINDIA", "ABB", "ACC", "ACMESOLAR", "AIAENG", "APLAPOLLO", "AUBANK", "AWL", "AADHARHFC", "AARTIIND", "AAVAS", "ABBOTINDIA", "ACE", "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS", "ADANIPOWER", "ATGL", "ABCAPITAL", "ABFRL", "ABLBL", "ABREL", "ABSLAMC", "AEGISLOG", "AEGISVOPAK", "AFCONS", "AFFLE", "AJANTPHARM", "AKUMS", "AKZOINDIA", "APLLTD", "ALKEM", "ALKYLAMINE", "ALOKINDS", "ARE&M", "AMBER", "AMBUJACEM", "ANANDRATHI", "ANANTRAJ", "ANGELONE", "APARINDS", "APOLLOHOSP", "APOLLOTYRE", "APTUS", "ASAHIINDIA", "ASHOKLEY", "ASIANPAINT", "ASTERDM", "ASTRAZEN", "ASTRAL", "ATHERENERG", "ATUL", "AUROPHARMA", "AIIL", "DMART", "AXISBANK", "BASF", "BEML", "BLS", "BSE", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BAJAJHLDNG", "BAJAJHFL", "BALKRISIND", "BALRAMCHIN", "BANDHANBNK", "BANKBARODA", "BANKINDIA", "MAHABANK", "BATAINDIA", "BAYERCROP", "BERGEPAINT", "BDL", "BEL", "BHARATFORG", "BHEL", "BPCL", "BHARTIARTL", "BHARTIHEXA", "BIKAJI", "BIOCON", "BSOFT", "BLUEDART", "BLUEJET", "BLUESTARCO", "BBTC", "BOSCHLTD", "FIRSTCRY", "BRIGADE", "BRITANNIA", "MAPMYINDIA", "CCL", "CESC", "CGPOWER", "CRISIL", "CAMPUS", "CANFINHOME", "CANBK", "CAPLIPOINT", "CGCL", "CARBORUNIV", "CASTROLIND", "CEATLTD", "CENTRALBK", "CDSL", "CENTURYPLY", "CERA", "CHALET", "CHAMBLFERT", "CHENNPETRO", "CHOICEIN", "CHOLAHLDNG", "CHOLAFIN", "CIPLA", "CUB", "CLEAN", "COALINDIA", "COCHINSHIP", "COFORGE", "COHANCE", "COLPAL", "CAMS", "CONCORDBIO", "CONCOR", "COROMANDEL", "CRAFTSMAN", "CREDITACC", "CROMPTON", "CUMMINSIND", "CYIENT", "DCMSHRIRAM", "DLF", "DOMS", "DABUR", "DALBHARAT", "DATAPATTNS", "DEEPAKFERT", "DEEPAKNTR", "DELHIVERY", "DEVYANI", "DIVISLAB", "DIXON", "AGARWALEYE", "LALPATHLAB", "DRREDDY", "EIDPARRY", "EIHOTEL", "EICHERMOT", "ELECON", "ELGIEQUIP", "EMAMILTD", "EMCURE", "ENDURANCE", "ENGINERSIN", "ERIS", "ESCORTS", "ETERNAL", "EXIDEIND", "NYKAA", "FEDERALBNK", "FACT", "FINCABLES", "FINPIPE", "FSL", "FIVESTAR", "FORCEMOT", "FORTIS", "GAIL", "GVT&D", "GMRAIRPORT", "GRSE", "GICRE", "GILLETTE", "GLAND", "GLAXO", "GLENMARK", "MEDANTA", "GODIGIT", "GPIL", "GODFRYPHLP", "GODREJAGRO", "GODREJCP", "GODREJIND", "GODREJPROP", "GRANULES", "GRAPHITE", "GRASIM", "GRAVITA", "GESHIP", "FLUOROCHEM", "GUJGASLTD", "GMDCLTD", "GSPL", "HEG", "HBLENGINE", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HFCL", "HAPPSTMNDS", "HAVELLS", "HEROMOTOCO", "HEXT", "HSCL", "HINDALCO", "HAL", "HINDCOPPER", "HINDPETRO", "HINDUNILVR", "HINDZINC", "POWERINDIA", "HOMEFIRST", "HONASA", "HONAUT", "HUDCO", "HYUNDAI", "ICICIBANK", "ICICIGI", "ICICIPRULI", "IDBI", "IDFCFIRSTB", "IFCI", "IIFL", "INOXINDIA", "IRB", "IRCON", "ITCHOTELS", "ITC", "ITI", "INDGN", "INDIACEM", "INDIAMART", "INDIANB", "IEX", "INDHOTEL", "IOC", "IOB", "IRCTC", "IRFC", "IREDA", "IGL", "INDUSTOWER", "INDUSINDBK", "NAUKRI", "INFY", "INOXWIND", "INTELLECT", "INDIGO", "IGIL", "IKS", "IPCALAB", "JBCHEPHARM", "JKCEMENT", "JBMA", "JKTYRE", "JMFINANCIL", "JSWENERGY", "JSWINFRA", "JSWSTEEL", "JPPOWER", "J&KBANK", "JINDALSAW", "JSL", "JINDALSTEL", "JIOFIN", "JUBLFOOD", "JUBLINGREA", "JUBLPHARMA", "JWL", "JYOTHYLAB", "JYOTICNC", "KPRMILL", "KEI", "KPITTECH", "KSB", "KAJARIACER", "KPIL", "KALYANKJIL", "KARURVYSYA", "KAYNES", "KEC", "KFINTECH", "KIRLOSBROS", "KIRLOSENG", "KOTAKBANK", "KIMS", "LTF", "LTTS", "LICHSGFIN", "LTFOODS", "LTIM", "LT", "LATENTVIEW", "LAURUSLABS", "LEMONTREE", "LICI", "LINDEINDIA", "LLOYDSME", "LODHA", "LUPIN", "MMTC", "MRF", "MGL", "MAHSCOOTER", "MAHSEAMLES", "M&MFIN", "M&M", "MANAPPURAM", "MRPL", "MANKIND", "MARICO", "MARUTI", "MFSL", "MAXHEALTH", "MAZDOCK", "METROPOLIS", "MINDACORP", "MSUMI", "MOTILALOFS", "MPHASIS", "MCX", "MUTHOOTFIN", "NATCOPHARM", "NBCC", "NCC", "NHPC", "NLCINDIA", "NMDC", "NSLNISP", "NTPCGREEN", "NTPC", "NH", "NATIONALUM", "NAVA", "NAVINFLUOR", "NESTLEIND", "NETWEB", "NEULANDLAB", "NEWGEN", "NAM-INDIA", "NIVABUPA", "NUVAMA", "NUVOCO", "OBEROIRLTY", "ONGC", "OIL", "OLAELEC", "OLECTRA", "PAYTM", "ONESOURCE", "OFSS", "POLICYBZR", "PCBL", "PGEL", "PIIND", "PNBHOUSING", "PTCIL", "PVRINOX", "PAGEIND", "PATANJALI", "PERSISTENT", "PETRONET", "PFIZER", "PHOENIXLTD", "PIDILITIND", "PPLPHARMA", "POLYMED", "POLYCAB", "POONAWALLA", "PFC", "POWERGRID", "PRAJIND", "PREMIERENE", "PRESTIGE", "PGHH", "PNB", "RRKABEL", "RBLBANK", "RECLTD", "RHIM", "RITES", "RADICO", "RVNL", "RAILTEL", "RAINBOW", "RKFORGE", "RCF", "REDINGTON", "RELIANCE", "RELINFRA", "RPOWER", "SBFC", "SBICARD", "SBILIFE", "SJVN", "SKFINDIA", "SRF", "SAGILITY", "SAILIFE", "SAMMAANCAP", "MOTHERSON", "SAPPHIRE", "SARDAEN", "SAREGAMA", "SCHAEFFLER", "THELEELA", "SCHNEIDER", "SCI", "SHREECEM", "SHRIRAMFIN", "SHYAMMETL", "ENRIN", "SIEMENS", "SIGNATURE", "SOBHA", "SOLARINDS", "SONACOMS", "SONATSOFTW", "STARHEALTH", "SBIN", "SAIL", "SUMICHEM", "SUNPHARMA", "SUNTV", "SUNDARMFIN", "SUNDRMFAST", "SUPREMEIND", "SUZLON", "SWANCORP", "SWIGGY", "SYNGENE", "SYRMA", "TBOTEK", "TVSMOTOR", "TATACHEM", "TATACOMM", "TCS", "TATACONSUM", "TATAELXSI", "TATAINVEST", "TATAMOTORS", "TATAPOWER", "TATASTEEL", "TATATECH", "TTML", "TECHM", "TECHNOE", "TEJASNET", "NIACL", "RAMCOCEM", "THERMAX", "TIMKEN", "TITAGARH", "TITAN", "TORNTPHARM", "TORNTPOWER", "TARIL", "TRENT", "TRIDENT", "TRIVENI", "TRITURBINE", "TIINDIA", "UCOBANK", "UNOMINDA", "UPL", "UTIAMC", "ULTRACEMCO", "UNIONBANK", "UBL", "UNITDSPR", "USHAMART", "VGUARD", "DBREALTY", "VTL", "VBL", "MANYAVAR", "VEDL", "VENTIVE", "VIJAYA", "VMM", "IDEA", "VOLTAS", "WAAREEENER", "WELCORP", "WELSPUNLIV", "WHIRLPOOL", "WIPRO", "WOCKPHARMA", "YESBANK", "ZFCVINDIA", "ZEEL", "ZENTEC", "ZENSARTECH", "ZYDUSLIFE", "ECLERX"]

dropdown_choice = st.sidebar.selectbox("Select Stock Symbol (NSE)", stock_list, index=0)
symbol = st.sidebar.text_input("Enter Stock Symbol", "").strip().upper() if dropdown_choice=="Enter Manually" else dropdown_choice

# Quick Date Range Slider
range_labels = ["Manual", "1W", "1M", "6M", "1Y"]
date_range_option = st.sidebar.select_slider("Select Day Range", options=range_labels, value="Manual")

# Manual date pickers
from_date = st.sidebar.date_input("From Date", pd.to_datetime("2025-01-01"))
to_date = st.sidebar.date_input("To Date", pd.to_datetime("2025-12-06"))

# Apply quick ranges
today = pd.to_datetime("today").normalize()
if date_range_option == "1W":
    from_date, to_date = today - pd.Timedelta(weeks=1), today
elif date_range_option == "1M":
    from_date, to_date = today - pd.DateOffset(months=1), today
elif date_range_option == "6M":
    from_date, to_date = today - pd.DateOffset(months=6), today
elif date_range_option == "1Y":
    from_date, to_date = today - pd.DateOffset(years=1), today

# Automatically update defaults if Manual
if date_range_option == "Manual":
    update_default_dates(from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))

show_data = st.sidebar.toggle("Show Raw Data", value=False)

# ---------------- Delete CSV ---------------- #
delete_csv = st.sidebar.toggle("Delete file.csv", value=True)


# Buttons
col1, col2 = st.sidebar.columns(2)
with col1: clear_btn = st.button("Clear Cache")
with col2: fetch_btn = st.button("Fetch Chart")

if clear_btn:
    st.cache_data.clear()
    st.success("✅ Cache cleared! Please fetch again.")

# Status message placeholder in sidebar
status_placeholder_api = st.sidebar.empty()
status_placeholder = st.sidebar.empty()

# Initialize session state
if 'api_request_count' not in st.session_state:
    st.session_state.api_request_count = 0
if 'last_fetch_source' not in st.session_state:
    st.session_state.last_fetch_source = ""

# ---------------- Fetch Data with Cache ---------------- #
@st.cache_data(ttl=60*30)  # cache for 30 mins
def fetch_nse_data(symbol, from_date, to_date):
    """Fetch NSE data and return DataFrame."""
    time.sleep(2)  # simulate rate limit
    fdate = from_date.strftime("%d-%m-%Y")
    tdate = to_date.strftime("%d-%m-%Y")
    df = pd.DataFrame(
        get.cm_hist_security_wise_data(
            symbol=symbol, from_date=fdate, to_date=tdate
        )
    )
    df = df[df["Series"]=="EQ"].copy()
    numeric_cols = ["Prev Close","Open Price","High Price","Low Price",
                    "Last Price","Close Price","Total Traded Quantity",
                    "Deliverable Qty","No. of Trades","% Dly Qt to Traded Qty"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",",""), errors="coerce")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.sort_values("Date").reset_index(drop=True)
    df["%PriceChange"] = df["Close Price"].pct_change().fillna(0) * 100
    df["% Dly Qt to Traded Qty"] = df["% Dly Qt to Traded Qty"].astype(str).replace('-', pd.NA).str.rstrip('%')
    df["% Dly Qt to Traded Qty"] = pd.to_numeric(df["% Dly Qt to Traded Qty"], errors='coerce').ffill().fillna(0).clip(0,100)
    return df

# Wrapper to detect cache vs fresh
def fetch_with_status(symbol, from_date, to_date):
    # Before calling cached function, record current time
    t1 = pd.Timestamp.now()
    df = fetch_nse_data(symbol, from_date, to_date)
    t2 = pd.Timestamp.now()

    # If execution took more than ~1 second, assume fresh fetch; otherwise cached
    if (t2 - t1).total_seconds() > 1.0:
        st.session_state.api_request_count += 1
        st.session_state.last_fetch_source = "✅ Data from NSE"
    else:
        st.session_state.last_fetch_source = "ℹ️ Data from cache"
    return df

# Usage
if fetch_btn and symbol:
    df = fetch_with_status(symbol, from_date, to_date)
    status_placeholder_api.info(f"📡 NSE API calls: {st.session_state.api_request_count}")
    status_placeholder.info(f"{st.session_state.last_fetch_source}")
    
    # ---------------- Chart ---------------- #
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        vertical_spacing=0.02, row_heights=[0.5,0.25,0.25],
        specs=[[{}],[{}],[{"secondary_y":True}]]
    )

    # Candlestick
    candlestick_text = [
        f"Date: {row['Date'].strftime('%d-%b-%Y')}<br>"
        f"Open: ₹{row['Open Price']:.2f}<br>"
        f"High: ₹{row['High Price']:.2f}<br>"
        f"Low: ₹{row['Low Price']:.2f}<br>"
        f"Close: ₹{row['Close Price']:.2f}<br>"
        f"% Change: {row['%PriceChange']:+.2f}%"
        for _, row in df.iterrows()
    ]
    fig.add_trace(
        go.Candlestick(
            x=df["Date"], open=df["Open Price"], high=df["High Price"],
            low=df["Low Price"], close=df["Close Price"],
            name="Price", hoverinfo="text", text=candlestick_text
        ), row=1, col=1
    )

    # Deliverable vs Intraday
    fig.add_trace(
        go.Bar(
            x=df["Date"], y=df["Deliverable Qty"], name="Deliverable Qty",
            marker_color="green",
            customdata=[format_indian_number(q) for q in df["Deliverable Qty"]],
            hovertemplate="Delivery: %{customdata}<br>Date: %{x|%d-%b-%Y}<extra></extra>"
        ), row=2, col=1
    )
    fig.add_trace(
        go.Bar(
            x=df["Date"], y=df["Total Traded Quantity"]-df["Deliverable Qty"], name="Intraday Qty",
            marker_color="red",
            customdata=[format_indian_number(q) for q in df["Total Traded Quantity"]-df["Deliverable Qty"]],
            hovertemplate="Intraday: %{customdata}<br>Date: %{x|%d-%b-%Y}<extra></extra>"
        ), row=2, col=1
    )

    # Trades & % Deliverable
    fig.add_trace(
        go.Bar(
            x=df["Date"], y=df["No. of Trades"], name="No. of Trades",
            marker_color="blue", opacity=0.5,
            customdata=[format_indian_number(t) for t in df["No. of Trades"]],
            hovertemplate="Trades: %{customdata}<br>Date: %{x|%d-%b-%Y}<extra></extra>"
        ), row=3, col=1, secondary_y=False
    )
    fig.add_trace(
        go.Scatter(
            x=df["Date"], y=df["% Dly Qt to Traded Qty"], mode="lines",
            name="% Deliverable", line=dict(color="red", width=2),
            hovertemplate="% Deliverable: %{y:.2f}%<br>Date: %{x|%d-%b-%Y}<extra></extra>"
        ), row=3, col=1, secondary_y=True
    )

    fig.update_layout(
        title=f"{symbol} - Price, Volume & Deliverables",
        xaxis_rangeslider_visible=False,
        barmode="stack",
        template="plotly_white",
        height=800,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.update_yaxes(title_text="No. of Trades", row=3, col=1, secondary_y=False, range=[0, max(df["No. of Trades"])*1.2])
    fig.update_yaxes(title_text="% Deliverable", row=3, col=1, secondary_y=True, range=[0,100], side="right")

    st.plotly_chart(fig, use_container_width=True)

    if show_data:
        st.subheader("📊 Raw Data")
        st.dataframe(df)

if delete_csv and os.path.exists("file.csv"):
    os.remove("file.csv")
    # st.warning("⚠️ file.csv deleted from disk.")

if __name__ == "__main__":
    import sys, os, subprocess

    # RUN_WITH_CODE = False  # 🔀 Switch: True → Run directly, False → Run via streamlit

    if RUN_WITH_CODE :
        # Prevent infinite loop if already inside Streamlit
        if os.environ.get("STREAMLIT_RUNNING") != "true":
            os.environ["STREAMLIT_RUNNING"] = "true"
            script_path = os.path.abspath(__file__)
            subprocess.run([sys.executable, "-m", "streamlit", "run", script_path])
            sys.exit(0)  # ✅ Exit original process
    else:
        print("\n")
        print("⚡ Open Terminal & Run: streamlit run Stock_Analysis.py")
        print("\n")