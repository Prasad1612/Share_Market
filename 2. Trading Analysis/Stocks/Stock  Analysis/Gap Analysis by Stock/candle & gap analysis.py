#!/usr/bin/env python3

"""
Hard-coded multi-stock candle & gap analysis using yfinance.
Outputs:
 - green/red/doji counts & %
 - gap-up/gap-down based on previous close
 - gap above prev high / below prev low
 - high > open, low < open counts & %
 - sustained gap-up/gap-down %
 - terminal report
 - CSV save
 - visualization save
"""

import os
from datetime import datetime
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
from tqdm import tqdm
import time
import warnings
warnings.filterwarnings("ignore")

# -------------------------------------------------------------
# HARD-CODE SETTINGS (EDIT HERE)
# -------------------------------------------------------------

# Nifty 500
TICKERS       = ["360ONE.NS", "3MINDIA.NS", "ABB.NS", "ACC.NS", "ACMESOLAR.NS", "AIAENG.NS", "APLAPOLLO.NS", "AUBANK.NS", "AWL.NS", "AADHARHFC.NS", "AARTIIND.NS", "AAVAS.NS", "ABBOTINDIA.NS", "ACE.NS", "ADANIENSOL.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ADANIPOWER.NS", "ATGL.NS", "ABCAPITAL.NS", "ABFRL.NS", "ABLBL.NS", "ABREL.NS", "ABSLAMC.NS", "AEGISLOG.NS", "AEGISVOPAK.NS", "AFCONS.NS", "AFFLE.NS", "AJANTPHARM.NS", "AKUMS.NS", "AKZOINDIA.NS", "APLLTD.NS", "ALKEM.NS", "ALKYLAMINE.NS", "ALOKINDS.NS", "ARE&M.NS", "AMBER.NS", "AMBUJACEM.NS", "ANANDRATHI.NS", "ANANTRAJ.NS", "ANGELONE.NS", "APARINDS.NS", "APOLLOHOSP.NS", "APOLLOTYRE.NS", "APTUS.NS", "ASAHIINDIA.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTERDM.NS", "ASTRAZEN.NS", "ASTRAL.NS", "ATHERENERG.NS", "ATUL.NS", "AUROPHARMA.NS", "AIIL.NS", "DMART.NS", "AXISBANK.NS", "BASF.NS", "BEML.NS", "BLS.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BAJAJHLDNG.NS", "BAJAJHFL.NS", "BALKRISIND.NS", "BALRAMCHIN.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS", "MAHABANK.NS", "BATAINDIA.NS", "BAYERCROP.NS", "BERGEPAINT.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHEL.NS", "BPCL.NS", "BHARTIARTL.NS", "BHARTIHEXA.NS", "BIKAJI.NS", "BIOCON.NS", "BSOFT.NS", "BLUEDART.NS", "BLUEJET.NS", "BLUESTARCO.NS", "BBTC.NS", "BOSCHLTD.NS", "FIRSTCRY.NS", "BRIGADE.NS", "BRITANNIA.NS", "MAPMYINDIA.NS", "CCL.NS", "CESC.NS", "CGPOWER.NS", "CRISIL.NS", "CAMPUS.NS", "CANFINHOME.NS", "CANBK.NS", "CAPLIPOINT.NS", "CGCL.NS", "CARBORUNIV.NS", "CASTROLIND.NS", "CEATLTD.NS", "CENTRALBK.NS", "CDSL.NS", "CENTURYPLY.NS", "CERA.NS", "CHALET.NS", "CHAMBLFERT.NS", "CHENNPETRO.NS", "CHOICEIN.NS", "CHOLAHLDNG.NS", "CHOLAFIN.NS", "CIPLA.NS", "CUB.NS", "CLEAN.NS", "COALINDIA.NS", "COCHINSHIP.NS", "COFORGE.NS", "COHANCE.NS", "COLPAL.NS", "CAMS.NS", "CONCORDBIO.NS", "CONCOR.NS", "COROMANDEL.NS", "CRAFTSMAN.NS", "CREDITACC.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DCMSHRIRAM.NS", "DLF.NS", "DOMS.NS", "DABUR.NS", "DALBHARAT.NS", "DATAPATTNS.NS", "DEEPAKFERT.NS", "DEEPAKNTR.NS", "DELHIVERY.NS", "DEVYANI.NS", "DIVISLAB.NS", "DIXON.NS", "AGARWALEYE.NS", "LALPATHLAB.NS", "DRREDDY.NS", "EIDPARRY.NS", "EIHOTEL.NS", "EICHERMOT.NS", "ELECON.NS", "ELGIEQUIP.NS", "EMAMILTD.NS", "EMCURE.NS", "ENDURANCE.NS", "ENGINERSIN.NS", "ERIS.NS", "ESCORTS.NS", "ETERNAL.NS", "EXIDEIND.NS", "NYKAA.NS", "FEDERALBNK.NS", "FACT.NS", "FINCABLES.NS", "FINPIPE.NS", "FSL.NS", "FIVESTAR.NS", "FORCEMOT.NS", "FORTIS.NS", "GAIL.NS", "GVT&D.NS", "GMRAIRPORT.NS", "GRSE.NS", "GICRE.NS", "GILLETTE.NS", "GLAND.NS", "GLAXO.NS", "GLENMARK.NS", "MEDANTA.NS", "GODIGIT.NS", "GPIL.NS", "GODFRYPHLP.NS", "GODREJAGRO.NS", "GODREJCP.NS", "GODREJIND.NS", "GODREJPROP.NS", "GRANULES.NS", "GRAPHITE.NS", "GRASIM.NS", "GRAVITA.NS", "GESHIP.NS", "FLUOROCHEM.NS", "GUJGASLTD.NS", "GMDCLTD.NS", "GSPL.NS", "HEG.NS", "HBLENGINE.NS", "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HFCL.NS", "HAPPSTMNDS.NS", "HAVELLS.NS", "HEROMOTOCO.NS", "HEXT.NS", "HSCL.NS", "HINDALCO.NS", "HAL.NS", "HINDCOPPER.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "POWERINDIA.NS", "HOMEFIRST.NS", "HONASA.NS", "HONAUT.NS", "HUDCO.NS", "HYUNDAI.NS", "ICICIBANK.NS", "ICICIGI.NS", "ICICIPRULI.NS", "IDBI.NS", "IDFCFIRSTB.NS", "IFCI.NS", "IIFL.NS", "INOXINDIA.NS", "IRB.NS", "IRCON.NS", "ITCHOTELS.NS", "ITC.NS", "ITI.NS", "INDGN.NS", "INDIACEM.NS", "INDIAMART.NS", "INDIANB.NS", "IEX.NS", "INDHOTEL.NS", "IOC.NS", "IOB.NS", "IRCTC.NS", "IRFC.NS", "IREDA.NS", "IGL.NS", "INDUSTOWER.NS", "INDUSINDBK.NS", "NAUKRI.NS", "INFY.NS", "INOXWIND.NS", "INTELLECT.NS", "INDIGO.NS", "IGIL.NS", "IKS.NS", "IPCALAB.NS", "JBCHEPHARM.NS", "JKCEMENT.NS", "JBMA.NS", "JKTYRE.NS", "JMFINANCIL.NS", "JSWENERGY.NS", "JSWINFRA.NS", "JSWSTEEL.NS", "JPPOWER.NS", "J&KBANK.NS", "JINDALSAW.NS", "JSL.NS", "JINDALSTEL.NS", "JIOFIN.NS", "JUBLFOOD.NS", "JUBLINGREA.NS", "JUBLPHARMA.NS", "JWL.NS", "JYOTHYLAB.NS", "JYOTICNC.NS", "KPRMILL.NS", "KEI.NS", "KPITTECH.NS", "KSB.NS", "KAJARIACER.NS", "KPIL.NS", "KALYANKJIL.NS", "KARURVYSYA.NS", "KAYNES.NS", "KEC.NS", "KFINTECH.NS", "KIRLOSBROS.NS", "KIRLOSENG.NS", "KOTAKBANK.NS", "KIMS.NS", "LTF.NS", "LTTS.NS", "LICHSGFIN.NS", "LTFOODS.NS", "LTIM.NS", "LT.NS", "LATENTVIEW.NS", "LAURUSLABS.NS", "THELEELA.NS", "LEMONTREE.NS", "LICI.NS", "LINDEINDIA.NS", "LLOYDSME.NS", "LODHA.NS", "LUPIN.NS", "MMTC.NS", "MRF.NS", "MGL.NS", "MAHSCOOTER.NS", "MAHSEAMLES.NS", "M&MFIN.NS", "M&M.NS", "MANAPPURAM.NS", "MRPL.NS", "MANKIND.NS", "MARICO.NS", "MARUTI.NS", "MFSL.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "METROPOLIS.NS", "MINDACORP.NS", "MSUMI.NS", "MOTILALOFS.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NATCOPHARM.NS", "NBCC.NS", "NCC.NS", "NHPC.NS", "NLCINDIA.NS", "NMDC.NS", "NSLNISP.NS", "NTPCGREEN.NS", "NTPC.NS", "NH.NS", "NATIONALUM.NS", "NAVA.NS", "NAVINFLUOR.NS", "NESTLEIND.NS", "NETWEB.NS", "NEULANDLAB.NS", "NEWGEN.NS", "NAM-INDIA.NS", "NIVABUPA.NS", "NUVAMA.NS", "NUVOCO.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "OLAELEC.NS", "OLECTRA.NS", "PAYTM.NS", "ONESOURCE.NS", "OFSS.NS", "POLICYBZR.NS", "PCBL.NS", "PGEL.NS", "PIIND.NS", "PNBHOUSING.NS", "PTCIL.NS", "PVRINOX.NS", "PAGEIND.NS", "PATANJALI.NS", "PERSISTENT.NS", "PETRONET.NS", "PFIZER.NS", "PHOENIXLTD.NS", "PIDILITIND.NS", "PPLPHARMA.NS", "POLYMED.NS", "POLYCAB.NS", "POONAWALLA.NS", "PFC.NS", "POWERGRID.NS", "PRAJIND.NS", "PREMIERENE.NS", "PRESTIGE.NS", "PGHH.NS", "PNB.NS", "RRKABEL.NS", "RBLBANK.NS", "RECLTD.NS", "RHIM.NS", "RITES.NS", "RADICO.NS", "RVNL.NS", "RAILTEL.NS", "RAINBOW.NS", "RKFORGE.NS", "RCF.NS", "REDINGTON.NS", "RELIANCE.NS", "RELINFRA.NS", "RPOWER.NS", "SBFC.NS", "SBICARD.NS", "SBILIFE.NS", "SJVN.NS", "SKFINDIA.NS", "SRF.NS", "SAGILITY.NS", "SAILIFE.NS", "SAMMAANCAP.NS", "MOTHERSON.NS", "SAPPHIRE.NS", "SARDAEN.NS", "SAREGAMA.NS", "SCHAEFFLER.NS", "SCHNEIDER.NS", "SCI.NS", "SHREECEM.NS", "SHRIRAMFIN.NS", "SHYAMMETL.NS", "ENRIN.NS", "SIEMENS.NS", "SIGNATURE.NS", "SOBHA.NS", "SOLARINDS.NS", "SONACOMS.NS", "SONATSOFTW.NS", "STARHEALTH.NS", "SBIN.NS", "SAIL.NS", "SUMICHEM.NS", "SUNPHARMA.NS", "SUNTV.NS", "SUNDARMFIN.NS", "SUNDRMFAST.NS", "SUPREMEIND.NS", "SUZLON.NS", "SWANCORP.NS", "SWIGGY.NS", "SYNGENE.NS", "SYRMA.NS", "TBOTEK.NS", "TVSMOTOR.NS", "TATACHEM.NS", "TATACOMM.NS", "TCS.NS", "TATACONSUM.NS", "TATAELXSI.NS", "TATAINVEST.NS", "TMPV.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TATATECH.NS", "TTML.NS", "TECHM.NS", "TECHNOE.NS", "TEJASNET.NS", "NIACL.NS", "RAMCOCEM.NS", "THERMAX.NS", "TIMKEN.NS", "TITAGARH.NS", "TITAN.NS", "TORNTPHARM.NS", "TORNTPOWER.NS", "TARIL.NS", "TRENT.NS", "TRIDENT.NS", "TRIVENI.NS", "TRITURBINE.NS", "TIINDIA.NS", "UCOBANK.NS", "UNOMINDA.NS", "UPL.NS", "UTIAMC.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UBL.NS", "UNITDSPR.NS", "USHAMART.NS", "VGUARD.NS", "DBREALTY.NS", "VTL.NS", "VBL.NS", "MANYAVAR.NS", "VEDL.NS", "VENTIVE.NS", "VIJAYA.NS", "VMM.NS", "IDEA.NS", "VOLTAS.NS", "WAAREEENER.NS", "WELCORP.NS", "WELSPUNLIV.NS", "WHIRLPOOL.NS", "WIPRO.NS", "WOCKPHARMA.NS", "YESBANK.NS", "ZFCVINDIA.NS", "ZEEL.NS", "ZENTEC.NS", "ZENSARTECH.NS", "ZYDUSLIFE.NS", "ECLERX.NS"]
    
# Nifty 50
# TICKERS       = ["ADANIENT.NS", "ADANIPORTS.NS", "APOLLOHOSP.NS", "ASIANPAINT.NS", "AXISBANK.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BEL.NS", "BHARTIARTL.NS", "CIPLA.NS", "COALINDIA.NS", "DRREDDY.NS", "EICHERMOT.NS", "ETERNAL.NS", "GRASIM.NS", "HCLTECH.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HINDALCO.NS", "HINDUNILVR.NS", "ICICIBANK.NS", "ITC.NS", "INFY.NS", "INDIGO.NS", "JSWSTEEL.NS", "JIOFIN.NS", "KOTAKBANK.NS", "LT.NS", "M&M.NS", "MARUTI.NS", "MAXHEALTH.NS", "NTPC.NS", "NESTLEIND.NS", "ONGC.NS", "POWERGRID.NS", "RELIANCE.NS", "SBILIFE.NS", "SHRIRAMFIN.NS", "SBIN.NS", "SUNPHARMA.NS", "TCS.NS", "TATACONSUM.NS", "TATAMOTORS.NS", "TATASTEEL.NS", "TECHM.NS", "TITAN.NS", "TRENT.NS", "ULTRACEMCO.NS", "WIPRO.NS"]

PERIOD        = "1mo"                #   "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y","5y", "10y", "ytd", "max"
INTERVAL      = "1d"                 #   "1m", "2m", "5m", "15m", "30m", "60m"/"1h", "90m",      "1d", "5d", "1wk", "1mo", "3mo"

OUTDIR        = "analysis_results"
SAVE_RAW      = False                # True False
SAVE_PLOTS    = True                 # True False

PRINT_REPORT  = False                # True False
# -------------------------------------------------------------

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def fetch_data(tickers):
    data = {}
    print("\nDownloading price data...\n")

    for i, t in enumerate(tqdm(tickers, desc="Fetching", ncols=150)):
        df = yf.download(t, period=PERIOD, interval=INTERVAL, auto_adjust=False, progress=False, multi_level_index=None, rounding=True)
        if df.empty:
            print(f"⚠ No data for {t}")
        data[t] = df

        if (i + 0) % 10 == 0:
            # print("⏳ Pause 2 seconds (API cooldown)...")
            time.sleep(2)
    return data

def analyze_df(df):
    if df.empty:
        return df
    
    df = df.copy()

    df["Prev_Close"] = df["Close"].shift(1)
    df["Prev_High"] = df["High"].shift(1)
    df["Prev_Low"] = df["Low"].shift(1)

    # Candle categories
    df["is_green"] = df["Close"] > df["Open"]
    df["is_red"] = df["Close"] < df["Open"]
    df["is_doji"] = df["Close"] == df["Open"]

    # Basic gaps
    df["gap_up_prev_close"] = (df["Open"] > df["Prev_Close"]) & (df["Open"] <= df["Prev_High"])
    df["gap_down_prev_close"] = (df["Open"] < df["Prev_Close"]) & (df["Open"] >= df["Prev_Low"])

    df["gap_above_prev_high"] = df["Open"] > df["Prev_High"]
    df["gap_below_prev_low"] = df["Open"] < df["Prev_Low"]

    # Open = high/low
    df["open_equal_high"] = df["High"] == df["Open"]
    df["open_equal_low"]  = df["Low"]  == df["Open"]

    # Sustained
    df["sustained_gap_up"] = df["gap_up_prev_close"] & (df["Close"] > df["Open"])
    df["sustained_gap_down"] = df["gap_down_prev_close"] & (df["Close"] < df["Open"])

    df = df.dropna(subset=["Prev_Close"])
    return df


def stats(df):
    if df.empty:
        return {}

    total = len(df)
    pct = lambda x: round((x / total) * 100, 2)

    S = {
        "days": total,
        "green": df["is_green"].sum(),
        "red": df["is_red"].sum(),
        "doji": df["is_doji"].sum(),
    }

    S["green_pct"] = pct(S["green"])
    S["red_pct"] = pct(S["red"])
    S["doji_pct"] = pct(S["doji"])

    # Gaps
    S["gap_up"] = df["gap_up_prev_close"].sum()
    S["gap_down"] = df["gap_down_prev_close"].sum()
    S["gap_up_pct"] = pct(S["gap_up"])
    S["gap_down_pct"] = pct(S["gap_down"])

    S["gap_above_prev_high"] = df["gap_above_prev_high"].sum()
    S["gap_below_prev_low"] = df["gap_below_prev_low"].sum()
    S["gap_above_prev_high_pct"] = pct(S["gap_above_prev_high"])
    S["gap_below_prev_low_pct"] = pct(S["gap_below_prev_low"])

    S["open_equal_high"] = df["open_equal_high"].sum()
    S["open_equal_low"] = df["open_equal_low"].sum()
    S["open_equal_high_pct"] = pct(S["open_equal_high"])
    S["open_equal_low_pct"] = pct(S["open_equal_low"])

    S["sustained_gap_up"] = df["sustained_gap_up"].sum()
    S["sustained_gap_down"] = df["sustained_gap_down"].sum()

    S["sustained_gap_up_pct"] = round((S["sustained_gap_up"] / (S["gap_up"] or 1)) * 100, 2)
    S["sustained_gap_down_pct"] = round((S["sustained_gap_down"] / (S["gap_down"] or 1)) * 100, 2)

    return S


# -------------------------------------------------------------
# REPORT PRINTING (NOW OPTIONAL)
# -------------------------------------------------------------
def print_report(ticker, s):
    if not PRINT_REPORT:
        return  # <-- Skip report

    if not s:
        print(f"No data for {ticker}")
        return

    print("\n" + "=" * 60)
    print(f"REPORT → {ticker}")
    print("-" * 60)

    rows = [
        ("Trading days", s["days"]),
        ("Green candles", f'{s["green"]} ({s["green_pct"]}%)'),
        ("Red candles", f'{s["red"]} ({s["red_pct"]}%)'),
        ("Doji", f'{s["doji"]} ({s["doji_pct"]}%)'),

        ("Gap Up (Prev Close)", f'{s["gap_up"]} ({s["gap_up_pct"]}%)'),
        ("Gap Down (Prev Close)", f'{s["gap_down"]} ({s["gap_down_pct"]}%)'),

        ("Gap Above Prev High", f'{s["gap_above_prev_high"]} ({s["gap_above_prev_high_pct"]}%)'),
        ("Gap Below Prev Low", f'{s["gap_below_prev_low"]} ({s["gap_below_prev_low_pct"]}%)'),

        ("Open = High", f'{s["open_equal_high"]} ({s["open_equal_high_pct"]}%)'),
        ("Open = Low", f'{s["open_equal_low"]} ({s["open_equal_low_pct"]}%)'),

        ("Sustained Gap Up", f'{s["sustained_gap_up"]} ({s["sustained_gap_up_pct"]}%)'),
        ("Sustained Gap Down", f'{s["sustained_gap_down"]} ({s["sustained_gap_down_pct"]}%)')
    ]

    print(tabulate(rows, headers=["Metric", "Value"], tablefmt="simple"))
    print("=" * 60)


def save_plot(ticker, s, outpath):
    labels = [
        "Green %", "Red %",
        "Gap Up %", "Gap Above High %",
        "Open = High %", "Open = Low %"
    ]

    values = [
        s["green_pct"], s["red_pct"],
        s["gap_up_pct"], s["gap_above_prev_high_pct"],
        s["open_equal_high_pct"], s["open_equal_low_pct"]
    ]

    plt.figure(figsize=(8, 4))
    bars = plt.bar(labels, values)

    for bar, val in zip(bars, values):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{val}%",
                 ha="center", va="bottom", fontsize=7)

    plt.title(f"{ticker} — Summary Metrics", fontsize=9)
    plt.xticks(rotation=20, fontsize=7)
    plt.yticks(fontsize=7)
    plt.ylim(0, 100)

    plt.tight_layout()
    plt.savefig(outpath, dpi=150)
    plt.close()


def save_summary_csv(all_stats, outpath):
    df = pd.DataFrame(all_stats)
    df.to_csv(outpath, index=False)
    print(f"Saved summary: {outpath}")


def run():
    ensure_dir(OUTDIR)
    timestamp = datetime.now().strftime("%Y-%m-%d")

    data = fetch_data(TICKERS)
    summary_rows = []

    for t in TICKERS:
        df = data[t]
        if df.empty:
            continue

        analyzed = analyze_df(df)
        stat = stats(analyzed)

        print_report(t, stat)     # PRINT_REPORT option applied

        # row = {"ticker": t}
        clean_ticker = t.replace(".NS", "")
        row = {"ticker": clean_ticker}
        row.update(stat)
        summary_rows.append(row)

        if SAVE_RAW:
            raw_path = os.path.join(OUTDIR, f"{t.replace('.', '_')}_raw.csv")
            if os.path.exists(raw_path):
                os.remove(raw_path)
            analyzed.to_csv(raw_path)

        if SAVE_PLOTS:
            plot_path = os.path.join(OUTDIR, f"{t.replace('.', '_')}_plot.png")
            if os.path.exists(plot_path):
                os.remove(plot_path)
            save_plot(t, stat, plot_path)

    summary_path = os.path.join(OUTDIR, f"aa_summary_candle_gap_{timestamp}.csv")
    save_summary_csv(summary_rows, summary_path)

    print("\n✔ Analysis Complete.")

if __name__ == "__main__":
    run()
