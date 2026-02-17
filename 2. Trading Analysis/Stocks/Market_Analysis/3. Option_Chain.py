import NseKit
import pandas as pd
import numpy as np
import time
import os
from tqdm import tqdm
from datetime import datetime
import pytz
# import random

# Google Sheet imports
import gspread
from google.oauth2.service_account import Credentials

# =====================================================
# CONFIG
# =====================================================
INDEX_NAME       = "SECURITIES IN F&O"          #"NIFTY IT"  "SECURITIES IN F&O"
EXPIRY_DATE      = "24-Feb-2026"

RESULT_SAVE_MODE = "GSHEET"      # "CSV" | "GSHEET" | "BOTH"
RESULT_FOLDER    = "Result"

DELAY_BASE   = 0.4
MAX_RETRIES  = 3
RETRY_DELAY  = 1.2

IST = pytz.timezone("Asia/Kolkata")

# =====================================================
# FILE PATHS
# =====================================================
if RESULT_SAVE_MODE in ("CSV", "BOTH"):
    os.makedirs(RESULT_FOLDER, exist_ok=True)
CSV_FILE = os.path.join(
    RESULT_FOLDER, f"{INDEX_NAME.lower().replace(' ', '_')}_option_analytics.csv"
)

# =====================================================
# GOOGLE SHEET CONFIG
# =====================================================
SERVICE_ACCOUNT_FILE = "Credentials/credentials.json"
SPREADSHEET_NAME     = "Market_Analysis"
GSHEET_TAB_NAME      = "Option_Chain"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# =====================================================
# EXPIRY DATE VALIDATION
# =====================================================
from datetime import datetime

def validate_expiry_date(expiry_str: str):
    try:
        expiry = datetime.strptime(expiry_str, "%d-%b-%Y").date()
    except ValueError:
        print(f"❌ Invalid EXPIRY_DATE format → {expiry_str}")
        print("✅ Use format: DD-Mon-YYYY (e.g. 24-Feb-2026)")
        exit(0)

    today = datetime.now(IST).date()

    if expiry < today:
        print("❌ EXPIRY DATE OVER")
        print(f"📅 Expiry : {expiry.strftime('%d-%b-%Y')}")
        print(f"📅 Today  : {today.strftime('%d-%b-%Y')}")
        print("🚫 Option chain fetch aborted")
        exit(0)

    print(f"✅ Expiry Valid → {expiry.strftime('%d-%b-%Y')}")

# Run validation
validate_expiry_date(EXPIRY_DATE)


# =====================================================
# INIT NSE
# =====================================================
nse = NseKit.Nse()

# =====================================================
# GET SYMBOL LIST
# =====================================================
# Fetch F&O stock universe
FnO_stocks = nse.index_live_indices_stocks_data("SECURITIES IN F&O", list_only=True)
FnO_stocks = [s for s in FnO_stocks if s != INDEX_NAME]

# Fetch index stocks
stocks = nse.index_live_indices_stocks_data(INDEX_NAME, list_only=True)
stocks = [s for s in stocks if s != INDEX_NAME]

# Classification
fno_index_stocks = [s for s in stocks if s in FnO_stocks]
non_fno_index_stocks = [s for s in stocks if s not in FnO_stocks]

print(f"\n{INDEX_NAME} | Total: {len(stocks)} | F&O: {len(fno_index_stocks)} | Non-F&O: {len(non_fno_index_stocks)}\n")

# ---------------------------------
# OPTION ANALYSIS FUNCTION
# ---------------------------------
def analyze_option_chain(symbol, oc):
    data = oc.get("data", [])
    spot = oc.get("underlyingValue", 0)

    if not data:
        return None

    rows = []
    for r in data:
        strike = r.get("strikePrice")
        ce = r.get("CE", {})
        pe = r.get("PE", {})

        rows.append({
            "Strike": strike,

            "CE_OI": ce.get("openInterest", 0),
            "CE_ChgOI": ce.get("changeinOpenInterest", 0),
            "CE_LTP": ce.get("lastPrice", 0),
            "CE_Chg": ce.get("change", 0),
            "CE_IV": ce.get("impliedVolatility", 0),

            "PE_OI": pe.get("openInterest", 0),
            "PE_ChgOI": pe.get("changeinOpenInterest", 0),
            "PE_LTP": pe.get("lastPrice", 0),
            "PE_Chg": pe.get("change", 0),
            "PE_IV": pe.get("impliedVolatility", 0),
        })

    df = pd.DataFrame(rows)

    # ---------------- TOTALS ----------------
    ce_oi  = df["CE_OI"].sum()
    pe_oi  = df["PE_OI"].sum()
    ce_chg = df["CE_ChgOI"].sum()
    pe_chg = df["PE_ChgOI"].sum()

    ce_chg_p = (ce_chg / ce_oi * 100) if ce_oi != 0 else 0
    pe_chg_p = (pe_chg / pe_oi * 100) if pe_oi != 0 else 0   

    oi_pcr  = round(pe_oi / ce_oi, 3) if ce_oi else 0
    chg_pcr = round(pe_chg / ce_chg, 3) if ce_chg else 0

    # ---------------- SORTING ----------------
    ce_oi_s  = df.sort_values("CE_OI", ascending=False)
    pe_oi_s  = df.sort_values("PE_OI", ascending=False)
    ce_chg_s = df.sort_values("CE_ChgOI", ascending=False)
    pe_chg_s = df.sort_values("PE_ChgOI", ascending=False)

    ce_exit = df.sort_values("CE_ChgOI")
    pe_exit = df.sort_values("PE_ChgOI")

    # ---------------- ATM ----------------
    df["ATM_DIFF"] = abs(df["Strike"] - spot)
    atm = df.loc[df["ATM_DIFF"].idxmin()]

    # ---------------- MAX PAIN ----------------
    pain = []
    for s in df["Strike"]:
        ce_loss = ((s - df[df["Strike"] < s]["Strike"]) *
                   df[df["Strike"] < s]["CE_OI"]).sum()
        pe_loss = ((df[df["Strike"] > s]["Strike"] - s) *
                   df[df["Strike"] > s]["PE_OI"]).sum()
        pain.append(ce_loss + pe_loss)

    df["Pain"] = pain
    max_pain = df.loc[df["Pain"].idxmin(), "Strike"]

    # ---------------- FINAL ROW ----------------
    return {
        "Symbol": symbol,
        "Spot": spot,

        "Total_CE_OI": ce_oi,
        "Total_PE_OI": pe_oi,
        "Total_CE_ChgOI": ce_chg,
        "Total_PE_ChgOI": pe_chg,
        "Call OI % Change": ce_chg_p,
        "Put OI % Change": pe_chg_p,
        "OI_PCR": oi_pcr,
        "Chg_OI_PCR": chg_pcr,

        "Max_PE_Strike": pe_oi_s.iloc[0]["Strike"],
        "Max_CE_Strike": ce_oi_s.iloc[0]["Strike"],
        "Max_PE_OI": pe_oi_s.iloc[0]["PE_OI"],
        "Max_CE_OI": ce_oi_s.iloc[0]["CE_OI"],

        "Max_PE_Chg_Strike": pe_chg_s.iloc[0]["Strike"],
        "Max_CE_Chg_Strike": ce_chg_s.iloc[0]["Strike"], 
        "Max_PE_ChgOI": pe_chg_s.iloc[0]["PE_ChgOI"],       
        "Max_CE_ChgOI": ce_chg_s.iloc[0]["CE_ChgOI"],

        "Max_PE_Exit_Strike": pe_exit.iloc[0]["Strike"],
        "Max_CE_Exit_Strike": ce_exit.iloc[0]["Strike"],
        "Max_PE_Exit": pe_exit.iloc[0]["PE_ChgOI"],
        "Max_CE_Exit": ce_exit.iloc[0]["CE_ChgOI"],

        "Max_Pain": max_pain,

        "ATM_Strike": atm["Strike"],
        "ATM_Call_Price": atm["CE_LTP"],
        "ATM_Put_Price": atm["PE_LTP"],
        "ATM_Call_Chg": atm["CE_Chg"],
        "ATM_Put_Chg": atm["PE_Chg"],
        "ATM_Call_IV": atm["CE_IV"],
        "ATM_Put_IV": atm["PE_IV"],
    }
# ---------------------------------
# FETCH WITH RETRY + PROGRESS BAR
# ---------------------------------
final_rows = []

for symbol in tqdm(fno_index_stocks, desc="Fetching Option Chain"):
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            oc = nse.fno_live_option_chain_raw(
                symbol, expiry_date=EXPIRY_DATE
            )
            row = analyze_option_chain(symbol, oc)
            if row:
                final_rows.append(row)
            break

        except Exception as e:
            attempt += 1
            if attempt == MAX_RETRIES:
                tqdm.write(f"❌ {symbol} skipped after {MAX_RETRIES} retries")
            else:
                time.sleep(RETRY_DELAY)

    time.sleep(DELAY_BASE)
    # time.sleep(DELAY_BASE + random.uniform(0, 0.5))

final_df = pd.DataFrame(final_rows)

# =====================================================
# SAVE FUNCTIONS
# =====================================================
def save_to_csv(df):
    df.to_csv(CSV_FILE, index=False)
    print(f"📁 CSV saved → {CSV_FILE}")

def save_to_google_sheet(df):
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    sh = gc.open(SPREADSHEET_NAME)

    try:
        ws = sh.worksheet(GSHEET_TAB_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(
            title=GSHEET_TAB_NAME,
            rows="3000",
            cols="50"
        )

    # ✅ Clear only required range (NOT whole sheet)
    ws.batch_clear(["A1:AZ3000"])

    ts = datetime.now(IST).strftime("%d-%b-%Y %I:%M %p")

    ws.update(
        range_name="A1",
        values=[[f"Last Updated: {ts}"]],
        value_input_option="USER_ENTERED"
    )

    ws.update(
        range_name="A2",
        values=[df.columns.tolist()],
        value_input_option="USER_ENTERED"
    )

    ws.update(
        range_name="A3",
        values=df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print("📊 Google Sheet updated successfully")


# =====================================================
# SAVE CONTROLLER
# =====================================================
print("\nSaving Results...")

if RESULT_SAVE_MODE == "CSV":
    save_to_csv(final_df)

elif RESULT_SAVE_MODE == "GSHEET":
    save_to_google_sheet(final_df)

elif RESULT_SAVE_MODE == "BOTH":
    save_to_csv(final_df)
    save_to_google_sheet(final_df)

else:
    print("❌ Invalid RESULT_SAVE_MODE")

print("\nOption Chain Analysis Completed Successfully ✅")
