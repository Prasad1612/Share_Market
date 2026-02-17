import os
import json
import time
from datetime import datetime, timedelta
import pandas as pd
import NseKit

# ==================================================
# GLOBALS
# ==================================================
get = NseKit.Nse()

TODAY_DT = datetime.now()
TOMORROW_DT = TODAY_DT + timedelta(days=1)

TODAY = TODAY_DT.strftime("%Y-%m-%d")
NOW_TS = TODAY_DT.isoformat(timespec="seconds")

BASE_DIR = "universe"
universe_path = f"{BASE_DIR}/universe.json"
live_path = f"{BASE_DIR}/live_hits.json"
fno_path = f"{BASE_DIR}/fno.json"
equity_path = f"{BASE_DIR}/equity_once.json"
final_path = f"{BASE_DIR}/final_stock_list.json"

FILE_PATH = f"{BASE_DIR}/final_stock_list.json"

API_SLEEP = 0.3

PERIODICAL_PRIORITY = [
    "Top 10 Nifty 50",
    "NIFTY 50",
    "SECURITIES IN F&O",
    "NIFTY 500"
]

# ==================================================
# DATE NORMALIZER
# ==================================================
def normalize_date(val):
    if val is None:
        return None

    s = str(val).strip()

    for fmt in (
        "%Y-%m-%d",
        "%d-%b-%Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S"
    ):
        try:
            return datetime.strptime(s[:19], fmt).date()
        except Exception:
            continue
    return None

# ==================================================
# GENERIC DAILY CACHE LOADER
# ==================================================
def load_or_run_daily(path, builder_fn, label):
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                payload = json.load(f)
            if payload.get("date") == TODAY:
                print(f"🟢 {label} loaded from cache")
                return payload["data"]
        except Exception:
            pass

    print(f"🔵 Building {label} (ONE TIME API HIT)")
    data = builder_fn()

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump({
            "date": TODAY,
            "last_updated": NOW_TS,
            "data": data
        }, f, indent=2)

    return data

# ==================================================
# UNIVERSE (ONCE PER DAY)
# ==================================================
def build_universe():

    AUTO_DATE           = True                                                  # True = auto (today), False = manual
    
    holiday_list = get.nse_trading_holidays(list_only=True)                         # NSE trading holidays (as datetime.date objects)
    holidays = set(pd.to_datetime(holiday_list, format='%d-%b-%Y', errors='coerce').date)

    def get_next_trading_day(start_date, direction="forward"):                  # Find next valid NSE trading day, skipping weekends & holidays
        step = 1 if direction == "forward" else -1
        d = start_date + timedelta(days=step)
        while d.weekday() >= 5 or d in holidays:                                # Sat=5, Sun=6
            d += timedelta(days=step)
        return d

    def get_current_trading_day(start_date):                                    # Return last valid trading day if today is weekend/holiday
        d = start_date
        while d.weekday() >= 5 or d in holidays:                                # adjust back to last trading day
            d -= timedelta(days=1)
        return d

    if AUTO_DATE:
        today_dt = datetime.now().date()
        current_trading_day = get_current_trading_day(today_dt)                  # Adjust today's date to last valid trading day (T)
        # date  = current_trading_day.strftime('%d-%m-%Y')                         # T (last valid trading day)
        pdate = get_next_trading_day(current_trading_day, "backward").strftime('%d-%m-%Y')  # T-1
        # ndate = get_next_trading_day(current_trading_day, "forward").strftime('%d-%m-%Y')   # T+1
        

    else:
        # ndate   = '05-01-2026'    # Mrng Time, "Today Date", Eve Time, "Next Trade date"
        # date    = '02-01-2026'
        pdate   = '01-01-2026'
        
    # Short date formats
    # sndate = datetime.strptime(ndate, '%d-%m-%Y').strftime('%d-%m-%y')  # T+1 short date
    # sdate  = datetime.strptime(date, '%d-%m-%Y').strftime('%d-%m-%y')   # T short date
    spdate = datetime.strptime(pdate, '%d-%m-%Y').strftime('%d-%m-%y')  # T-1 short date

    time.sleep(API_SLEEP)
    top10 = get.nse_eod_top10_nifty50(spdate)

    time.sleep(API_SLEEP)
    nifty50 = get.index_live_indices_stocks_data("NIFTY 50", list_only=True)

    time.sleep(API_SLEEP)
    fno = get.index_live_indices_stocks_data("SECURITIES IN F&O", list_only=True)

    time.sleep(API_SLEEP)
    nifty500 = get.index_live_indices_stocks_data("NIFTY 500", list_only=True)

    return {
        "Top 10 Nifty 50": top10["SYMBOL"].tolist(),
        "NIFTY 50": nifty50,
        "SECURITIES IN F&O": fno,
        "NIFTY 500": nifty500
    }

universe = load_or_run_daily(universe_path, build_universe, "Universe")

# ==================================================
# HELPERS
# ==================================================
def normalize_symbol(row):
    for k in ("symbol", "SYMBOL", "bm_symbol", "Symbol"):
        if k in row and row[k]:
            return str(row[k]).strip().upper()
    return None

def get_best_periodical(symbol):
    for p in PERIODICAL_PRIORITY:
        if symbol in universe.get(p, []):
            return p
    return None

# ==================================================
# MERGE HELPERS
# ==================================================
def merge_hits(raw):
    merged = {}

    for h in raw:
        sym = h["symbol"]
        if sym not in merged:
            merged[sym] = {
                "symbol": sym,
                "periodicals": h.get("periodicals"),
                "source": set(),
                "timestamp": h["timestamp"]
            }
        merged[sym]["source"].add(h["source"])

    return [
        {**v, "source": sorted(v["source"])}
        for v in merged.values()
    ]

def merge_fno_hits(raw):
    merged = {}

    for h in raw:
        sym = h["symbol"]
        if sym not in merged:
            merged[sym] = {
                "symbol": sym,
                "periodicals": h.get("periodicals"),
                "source": set(),
                "timestamp": h["timestamp"]
            }
        merged[sym]["source"].add(h["source"])

    return [
        {**v, "source": sorted(v["source"])}
        for v in merged.values()
    ]

def merge_live_and_equity(live, equity, fno):
    combined = {}

    for block in (live, equity):
        for i in block:
            sym = i["symbol"]
            combined.setdefault(sym, {
                "symbol": sym,
                "periodicals": i.get("periodicals"),
                "source": set(),
                "timestamp": i["timestamp"]
            })
            combined[sym]["source"].update(i["source"])

    for i in fno:
        sym = i["symbol"]
        combined.setdefault(sym, {
            "symbol": sym,
            "periodicals": i.get("periodicals"),
            "source": set(),
            "timestamp": i["timestamp"]
        })
        combined[sym]["source"].update(i["source"])

    return [
        {**v, "source": sorted(v["source"])}
        for v in combined.values()
    ]

# ==================================================
# LIVE HITS
# ==================================================
def build_live_hits():
    feeds = {
        "Most Active Equities": get.cm_live_most_active_equity_by_value,
        "Volume Spurts": get.cm_live_volume_spurts,
        "52W High": get.cm_live_52week_high,
        "52W Low": get.cm_live_52week_low,
        "Insider Trading": get.cm_live_hist_insider_trading,
        # "Corporate Announcements": get.cm_live_hist_corporate_announcement
    }

    raw = []

    for name, fn in feeds.items():
        try:
            time.sleep(API_SLEEP)
            df = fn()
            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                sym = normalize_symbol(row)
                periodical = get_best_periodical(sym)
                if sym and periodical:
                    raw.append({
                        "symbol": sym,
                        "source": name,
                        "periodicals": periodical,
                        "timestamp": NOW_TS
                    })

            print(f"[OK] {name}: {len(df)}")
        except Exception as e:
            print(f"[ERROR] {name}: {e}")

    return merge_hits(raw)

# ==================================================
# FNO LIVE
# ==================================================

EXCLUDE = {'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50'}

def build_fno_live():
    raw = []

    try:
        time.sleep(API_SLEEP)
        for df, label in [
            (get.fno_live_top_20_derivatives_contracts("Stock Futures"), "Top Futures"),
            (get.fno_live_top_20_derivatives_contracts("Stock Options"), "Top Options")
        ]:
            if df is not None and not df.empty:
                for sym in df["Symbol"].astype(str).str.upper().unique():
                    if sym in EXCLUDE:
                        continue

                    periodical = get_best_periodical(sym)
                    if not periodical:
                        continue

                    raw.append({
                        "symbol": sym,
                        "source": label,
                        "periodicals": periodical,
                        "timestamp": NOW_TS
                    })
        print("[OK] Top FNO Contracts")
    except Exception as e:
        print("[ERROR] FNO Contracts:", e)

    try:
        time.sleep(API_SLEEP)
        df = get.fno_live_most_active_underlying()
        if df is not None and not df.empty:
            value_columns = {
                "High Fut Value": ["Fut Val (₹ Lakhs)", "FUT VALUE"],
                "High Opt Value": ["Opt Val (₹ Lakhs)", "OPT VALUE"],
                "High Total Value": ["Total Val (₹ Lakhs)", "TOTAL VALUE"]
            }

            for label, cols in value_columns.items():
                col = next((c for c in cols if c in df.columns), None)
                if not col:
                    continue

                for sym in df.sort_values(col, ascending=False).head(5)["Symbol"]:
                    sym = str(sym).upper()
                    if sym in EXCLUDE:
                        continue

                    periodical = get_best_periodical(sym)
                    if not periodical:
                        continue

                    raw.append({
                        "symbol": sym,
                        "source": label,
                        "periodicals": periodical,
                        "timestamp": NOW_TS
                    })

            print("[OK] Most Active Underlying")
    except Exception as e:
        print("[ERROR] Most Active Underlying:", e)

    try:
        time.sleep(API_SLEEP)
        df = get.fno_live_change_in_oi()
        if df is not None and not df.empty:
            for sym in df.head(5)["Symbol"]:
                sym = str(sym).upper()
                if sym in EXCLUDE:
                    continue

                periodical = get_best_periodical(sym)
                if periodical:
                    raw.append({
                        "symbol": sym,
                        "source": "OI Increase",
                        "periodicals": periodical,
                        "timestamp": NOW_TS
                    })

            for sym in df.tail(5)["Symbol"]:
                sym = str(sym).upper()
                if sym in EXCLUDE:
                    continue

                periodical = get_best_periodical(sym)
                if periodical:
                    raw.append({
                        "symbol": sym,
                        "source": "OI Decrease",
                        "periodicals": periodical,
                        "timestamp": NOW_TS
                    })

            print("[OK] Change in OI")
    except Exception as e:
        print("[ERROR] Change in OI:", e)

    return merge_fno_hits(raw)

# ==================================================
# EQUITY EVENTS (ONCE PER DAY)
# ==================================================
def build_equity_once_normalized():
    feeds = [
        ("Corporate Actions", get.cm_live_hist_corporate_action, "EX-DATE", TODAY_DT.date()),
        ("Today Events", get.cm_live_today_event_calendar, "date", TODAY_DT.date()),
        ("Upcoming Events", get.cm_live_upcoming_event_calendar, "date", TOMORROW_DT.date()),
        ("Board Meetings", get.cm_live_hist_board_meetings, "bm_timestamp", TODAY_DT.date()),
        ("Shareholder Meetings", get.cm_live_hist_Shareholder_meetings, "date", TODAY_DT.date()),
    ]

    raw = []

    for name, fn, date_col, match_date in feeds:
        try:
            time.sleep(API_SLEEP)
            df = fn()
            if df is None or df.empty or date_col not in df.columns:
                continue

            df["_norm"] = df[date_col].apply(normalize_date)
            df = df[df["_norm"] == match_date]

            for _, row in df.iterrows():
                sym = normalize_symbol(row)
                periodical = get_best_periodical(sym)
                if sym and periodical:
                    raw.append({
                        "symbol": sym,
                        "source": name,
                        "periodicals": periodical,
                        "timestamp": NOW_TS
                    })

            print(f"[OK] {name}: {len(df)}")
        except Exception as e:
            print(f"[ERROR] {name}: {e}")

    return merge_hits(raw)

def sort_final_hits(hits):
    return sorted(
        hits,
        key=lambda x: (
            -len(x.get("source", [])),                       # more sources first
            PERIODICAL_PRIORITY.index(x["periodicals"])      # higher index priority
            if x.get("periodicals") in PERIODICAL_PRIORITY else 99,
            x["symbol"]                                      # stable alphabetical
        )
    )

# ==================================================
# EXECUTION
# ==================================================
os.makedirs(BASE_DIR, exist_ok=True)

live_hits = build_live_hits()
fno_hits = build_fno_live()
equity_hits = load_or_run_daily(equity_path, build_equity_once_normalized, "Equity Events")

final_hits = merge_live_and_equity(live_hits, equity_hits, fno_hits)
final_hits = sort_final_hits(final_hits)

with open(live_path, "w") as f:
    json.dump(live_hits, f, indent=2)

with open(fno_path, "w") as f:
    json.dump(fno_hits, f, indent=2)

with open(final_path, "w") as f:
    json.dump(final_hits, f, indent=2)

print("✅ Live hits:", len(live_hits))
print("✅ FNO symbols:", len(fno_hits))
print("✅ Equity hits:", len(equity_hits))
print("✅ Final merged list:", len(final_hits))
print("📁 Saved to:", final_path)


# --------------------------------------------------
# LOAD & EXTRACT SYMBOLS
# --------------------------------------------------
EXCLUDE = {'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50'}
symbols = []

if os.path.exists(FILE_PATH):
    with open(FILE_PATH, "r") as f:
        data = json.load(f)

    seen = set()
    for item in data:
        if isinstance(item, dict) and "symbol" in item:
            sym = item["symbol"].strip().upper()
            if sym not in seen and sym not in EXCLUDE:
                seen.add(sym)
                symbols.append(sym)

    # # keep only first 3 (order preserved)
    # symbols = symbols[:3]                                                                         # For limited Stocks (5, 10, 20,)

else:
    raise FileNotFoundError(f"{FILE_PATH} not found")

# --------------------------------------------------
# EXTRACT FNO SYMBOLS FROM FINAL SYMBOL LIST
# --------------------------------------------------

fno_symbols = []

# Build FNO universe set once (fast lookup)
fno_universe = set(universe.get("SECURITIES IN F&O", []))

for sym in symbols:
    if sym in fno_universe:
        fno_symbols.append(sym)

# --------------------------------------------------
# RESULT
# --------------------------------------------------
print(f"\nTotal Symbols: {len(symbols)}")
# print("All Symbols:", symbols)

print(f"Total F&O Symbols: {len(fno_symbols)}\n")
# print("F&O Symbols:", fno_symbols)

# =====================================================
# NSE INDEX DATA FETCH + CSV / GOOGLE SHEET SAVE (FULL)
# =====================================================

import NseKit
from tqdm import tqdm
import time
import csv
import os
from datetime import datetime
import pytz
import gspread
from google.oauth2.service_account import Credentials

# =====================================================
# CONFIG
# =====================================================
# INDEX_NAME          = "SECURITIES IN F&O"    # "SECURITIES IN F&O"
RESULT_SAVE_MODE    = "GSHEET"      # SAVE MODE → "CSV" | "GSHEET" | "BOTH"

DELAY               = 0.3
MAX_RETRIES         = 3
RETRY_DELAY         = 1.0
RESULT_FOLDER       = "universe"

if RESULT_SAVE_MODE in ("CSV", "BOTH"):
    os.makedirs(RESULT_FOLDER, exist_ok=True)

RAW_CSV_FILE = os.path.join(RESULT_FOLDER, "stocks_data.csv")
PERCENT_CSV_FILE = os.path.join(RESULT_FOLDER, "stocks_data_percent.csv")

# =====================================================
# GOOGLE SHEET CONFIG
# =====================================================
SERVICE_ACCOUNT_FILE = "Credentials/credentials.json"
SPREADSHEET_NAME = "Market_Analysis"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

IST = pytz.timezone("Asia/Kolkata")

# =====================================================
# INIT NSE
# =====================================================
get = NseKit.Nse()

# =====================================================
# HELPERS
# =====================================================
def format_large_number(num):
    try:
        num = float(num)
    except (ValueError, TypeError):
        return num

    if num >= 1e7:
        return f"{num/1e7:.2f} Cr"
    elif num >= 1e5:
        return f"{num/1e5:.2f} L"
    else:
        return f"{num:.0f}"


def percent_of_total(num, total):
    try:
        return f"{(float(num) / float(total) * 100):.3f}" if total else "0.000"
    except (ValueError, TypeError):
        return "0.000"


# # =====================================================
# # GET STOCK LIST
# # =====================================================
# stocks = get.index_live_indices_stocks_data(
#     INDEX_NAME, list_only=True
# )
# stocks = [s for s in stocks if s != INDEX_NAME]

# print(f"\nTotal Stocks: {len(stocks)}")

# =====================================================
# STORAGE
# =====================================================
raw_data = []
screen_rows = []
failed_stocks = []

# =====================================================
# FETCH DATA
# =====================================================
for symbol in tqdm(symbols, desc="Fetching NSE Equity Data"):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = get.cm_live_equity_full_info(symbol)
            raw_data.append(data)

            total_issued = data.get("TotalIssuedShares", 0)

            screen_rows.append({
                "Symbol": symbol,
                "LTP": data.get("LastTradedPrice"),
                "CHG": data.get("Change"),
                "%CHG": data.get("PercentChange"),
                "VWAP": data.get("VWAP"),
                "VOLUME": format_large_number(data.get("TotalTradedVolume")),
                "VALUE": format_large_number(data.get("TotalTradedValue")),
                "DEL%": data.get("DeliveryPercent"),
                "BUYQ": format_large_number(data.get("TotalBuyQuantity")),
                "SELLQ": format_large_number(data.get("TotalSellQuantity")),
                "BUYQ%": data.get("BuyQuantity%"),
                "SELLQ%": data.get("SellQuantity%"),
                "TVOL%": percent_of_total(data.get("TotalTradedVolume"), total_issued),
                "TDEL_Q%": percent_of_total(data.get("DeliveryQty"), total_issued),
                "TBUYQ%": percent_of_total(data.get("TotalBuyQuantity"), total_issued),
                "TSELLQ%": percent_of_total(data.get("TotalSellQuantity"), total_issued),
                "Industry": data.get("BasicIndustry")
            })

            time.sleep(DELAY)
            break

        except Exception as e:
            if attempt == MAX_RETRIES:
                failed_stocks.append(symbol)
                print(f"FAILED {symbol} → {e}")
            time.sleep(RETRY_DELAY)

# =====================================================
# TERMINAL OUTPUT
# =====================================================
print(f"\nDATA FETCH COMPLETED\n")

# Load JSON data
with open(FILE_PATH, "r") as f:
    stock_info = json.load(f)

# Build a lookup dict for fast access by symbol
info_lookup = {item["symbol"].upper(): item for item in stock_info}

# Print header (optional)
print(f"{'Symbol':<10}{'LTP':>10}{'CHG':>10}{'%CHG':>10}{'VWAP':>10}"
      f"{'VOLUME':>12}{'VALUE':>15}{'DEL%':>10}{'BUYQ':>10}{'SELLQ':>10}"
      f"{'BUYQ%':>10}{'SELLQ%':>10}{'TVOL%':>8}{'TDEL_Q%':>8}{'TBUYQ%':>8}"
      f"{'TSELLQ%':>8}{'Industry':>35}{'Periodicals':>20}{'Source':>50}")

for r in screen_rows:
    sym = r['Symbol'].upper()
    # Get extra info if available
    extra = info_lookup.get(sym, {})
    r["Periodicals"] = extra.get("periodicals", "")
    r["Source"] = ", ".join(extra.get("source", []))
    
    print(
        f"{r['Symbol']:<10}{r['LTP']:>10}{r['CHG']:>10}"
        f"{r['%CHG']:>10}{r['VWAP']:>10}"
        f"{r['VOLUME']:>12}{r['VALUE']:>15}"
        f"{r['DEL%']:>10}{r['BUYQ']:>10}"
        f"{r['SELLQ']:>10}{r['BUYQ%']:>10}"
        f"{r['SELLQ%']:>10}{r['TVOL%']:>8}"
        f"{r['TDEL_Q%']:>8}{r['TBUYQ%']:>8}"
        f"{r['TSELLQ%']:>8}{r['Industry']:>35}"
        f"{r.get('Periodicals',''):>20}"
        # f"{r.get('Source',''):>50}"
    )

# =====================================================
# SAVE FUNCTIONS
# =====================================================
def save_to_csv():
    if raw_data:
        with open(RAW_CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=raw_data[0].keys())
            writer.writeheader()
            writer.writerows(raw_data)

    if screen_rows:
        with open(PERCENT_CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=screen_rows[0].keys())
            writer.writeheader()
            writer.writerows(screen_rows)

    print("📁 CSV files saved successfully")


def save_to_google_sheet():
    # ================= AUTH =================
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    sh = gc.open(SPREADSHEET_NAME)

    CLEAR_RANGES = {
        "D_Stocks": "A1:BP",
        "D_Screen_Stocks": "A1:S",
    }

    # ================= HELPERS =================
    def clean_value(v):
        if isinstance(v, str):
            v = v.strip()
            if v.replace(".", "", 1).isdigit():
                return float(v) if "." in v else int(v)
        return v

    def sanitize_rows(rows):
        return [r for r in rows if isinstance(r, dict)]

    def write_sheet(sheet_name, data):
        if not data:
            return

        data = sanitize_rows(data)
        if not data:
            return

        ws = sh.worksheet(sheet_name)

        # Clear old data
        ws.batch_clear([CLEAR_RANGES.get(sheet_name, "A1:Z3000")])

        headers = list(data[0].keys())

        values = [
            [clean_value(row.get(h, "")) for h in headers]
            for row in data
        ]

        ts = datetime.now(IST).strftime("%d-%b-%Y %I:%M %p")

        # ✅ FIXED update calls (named arguments)
        ws.update(
            range_name="A1",
            values=[[f"Last Updated: {ts}"]],
            value_input_option="USER_ENTERED"
        )

        ws.update(
            range_name="A2",
            values=[headers],
            value_input_option="USER_ENTERED"
        )

        if values:
            ws.update(
                range_name="A3",
                values=values,
                value_input_option="USER_ENTERED"
            )

    # ================= WRITE =================
    write_sheet("D_Stocks", raw_data)
    write_sheet("D_Screen_Stocks", screen_rows)

    print("📊 Google Sheet updated successfully")



# =====================================================
# SAVE CONTROLLER
# =====================================================
print("\nSaving Results...")

if RESULT_SAVE_MODE == "CSV":
    save_to_csv()

elif RESULT_SAVE_MODE == "GSHEET":
    save_to_google_sheet()

elif RESULT_SAVE_MODE == "BOTH":
    save_to_csv()
    save_to_google_sheet()

else:
    print("Invalid RESULT_SAVE_MODE")

# =====================================================
# FAILED REPORT
# =====================================================
if failed_stocks:
    print("\nFAILED STOCKS:", ", ".join(failed_stocks))
else:
    print("\nAll stocks fetched successfully ✅")