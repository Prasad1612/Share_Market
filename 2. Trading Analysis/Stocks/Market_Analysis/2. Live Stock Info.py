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
INDEX_NAME          = "SECURITIES IN F&O"    # "SECURITIES IN F&O"
RESULT_SAVE_MODE    = "BOTH"      # SAVE MODE → "CSV" | "GSHEET" | "BOTH"

DELAY               = 0.3
MAX_RETRIES         = 3
RETRY_DELAY         = 1.0
RESULT_FOLDER       = "Result"

if RESULT_SAVE_MODE in ("CSV", "BOTH"):
    os.makedirs(RESULT_FOLDER, exist_ok=True)

RAW_CSV_FILE = os.path.join(RESULT_FOLDER, f"{INDEX_NAME.lower()}_stocks_data.csv")
PERCENT_CSV_FILE = os.path.join(RESULT_FOLDER, f"{INDEX_NAME.lower()}_stocks_data_percent.csv")

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


# =====================================================
# GET STOCK LIST
# =====================================================
stocks = get.index_live_indices_stocks_data(
    INDEX_NAME, list_only=True
)
stocks = [s for s in stocks if s != INDEX_NAME]

print(f"\nTotal Stocks: {len(stocks)}")

# =====================================================
# STORAGE
# =====================================================
raw_data = []
screen_rows = []
failed_stocks = []

# =====================================================
# FETCH DATA
# =====================================================
for symbol in tqdm(stocks, desc="Fetching NSE Equity Data"):
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
print(f"\n{INDEX_NAME} - DATA FETCH COMPLETED\n")

header = (
    f"{'SYMBOL':<10}{'LTP':>10}{'CHG':>10}{'%CHG':>10}"
    f"{'VWAP':>10}{'VOLUME':>12}{'VALUE':>15}"
    f"{'DEL%':>10}{'BUYQ':>10}{'SELLQ':>10}"
    f"{'BUYQ%':>10}{'SELLQ%':>10}"
    f"{'TVOL%':>8}{'TDEL_Q%':>8}"
    f"{'TBUYQ%':>8}{'TSELLQ%':>8}"
    f"{'Industry':>35}"
)

print(header)
print("-" * len(header))

for r in screen_rows:
    print(
        f"{r['Symbol']:<10}{r['LTP']:>10}{r['CHG']:>10}"
        f"{r['%CHG']:>10}{r['VWAP']:>10}"
        f"{r['VOLUME']:>12}{r['VALUE']:>15}"
        f"{r['DEL%']:>10}{r['BUYQ']:>10}"
        f"{r['SELLQ']:>10}{r['BUYQ%']:>10}"
        f"{r['SELLQ%']:>10}{r['TVOL%']:>8}"
        f"{r['TDEL_Q%']:>8}{r['TBUYQ%']:>8}"
        f"{r['TSELLQ%']:>8}{r['Industry']:>35}"
    )

# =====================================================
# SAVE FUNCTIONS
# =====================================================
# def save_to_csv():
#     if raw_data:
#         with open(RAW_CSV_FILE, "w", newline="", encoding="utf-8") as f:
#             writer = csv.DictWriter(f, fieldnames=raw_data[0].keys())
#             writer.writeheader()
#             writer.writerows(raw_data)

#     if screen_rows:
#         with open(PERCENT_CSV_FILE, "w", newline="", encoding="utf-8") as f:
#             writer = csv.DictWriter(f, fieldnames=screen_rows[0].keys())
#             writer.writeheader()
#             writer.writerows(screen_rows)

#     print("📁 CSV files saved successfully")

def save_to_csv():
    clean_raw = [r for r in raw_data if isinstance(r, dict)]
    clean_screen = [r for r in screen_rows if isinstance(r, dict)]

    if clean_raw:
        with open(RAW_CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=clean_raw[0].keys())
            writer.writeheader()
            writer.writerows(clean_raw)

    if clean_screen:
        with open(PERCENT_CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=clean_screen[0].keys())
            writer.writeheader()
            writer.writerows(clean_screen)

    print("📁 CSV files saved successfully")

def save_to_google_sheet():
    # ================= AUTH =================
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    sh = gc.open(SPREADSHEET_NAME)

    CLEAR_RANGES = {
        "Stocks_Info": "A1:BP",
        "Screen_Stocks_Info": "A1:Q",
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

    # Write data to both sheets
    write_sheet("Stocks_Info", raw_data)
    write_sheet("Screen_Stocks_Info", screen_rows)

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
