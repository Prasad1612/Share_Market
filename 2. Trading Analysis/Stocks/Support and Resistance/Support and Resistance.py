"""
Fractal-based Support & Resistance Detector (Professional Version)
──────────────────────────────────────────────────────────────────
✓ Supports both yfinance and local CSV input.
✓ Generates one consolidated All_Levels.csv + Summary.csv
✓ Automatically saves charts for levels within ±DIST_% of LTP.
✓ Designed for professional trading analysis & automation.

"""

import os
import time
import shutil
from pathlib import Path
import pandas as pd
import numpy as np
import yfinance as yf
from tqdm import tqdm
import matplotlib
matplotlib.use("Agg")  # Use non-GUI backend to avoid Tkinter errors
import matplotlib.pyplot as plt
import matplotlib.dates as mpl_dates
from mplfinance.original_flavor import candlestick_ohlc

plt.rcParams['figure.figsize'] = [12, 7]
plt.rc('font', size=12)

# -------------------------------------------------
# Fetch Historical OHLCV Data with Retry Logic
# -------------------------------------------------
def fetch_yf_data(symbol, period="6mo", interval="1d", retries=3, pause=1.0):
    for attempt in range(retries):
        try:
            df = yf.download(symbol, period=period, interval=interval, auto_adjust=False, progress=False)
            df = df.reset_index()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] if col[1] == symbol else col[1] for col in df.columns]
            if 'Date' not in df.columns:
                df.rename(columns={df.columns[0]: 'Date'}, inplace=True)
            for col in ['Open', 'High', 'Low', 'Close']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)
            df = df.sort_values('Date').reset_index(drop=True)
            return df
        except Exception as e:
            if attempt == retries - 1:
                raise e
        time.sleep(pause * (2 ** attempt))
    return pd.DataFrame()

# -------------------------------------------------
# Fractal Detection Logic
# -------------------------------------------------
def is_support(df, i):
    return (df['Low'][i] < df['Low'][i-1] and df['Low'][i] < df['Low'][i+1] and
            df['Low'][i+1] < df['Low'][i+2] and df['Low'][i-1] < df['Low'][i-2])

def is_resistance(df, i):
    return (df['High'][i] > df['High'][i-1] and df['High'][i] > df['High'][i+1] and
            df['High'][i+1] > df['High'][i+2] and df['High'][i-1] > df['High'][i-2])

def identify_levels(df):
    levels = []
    for i in range(2, df.shape[0] - 2):
        if is_support(df, i):
            levels.append((i, df['Low'][i], 'Support'))
        elif is_resistance(df, i):
            levels.append((i, df['High'][i], 'Resistance'))
    return levels

# -------------------------------------------------
# Level Filtering & Strength Calculation
# -------------------------------------------------
def filter_levels(df, levels, sensitivity=1.0):
    mean_range = np.mean(df['High'] - df['Low'])
    filtered = []
    for i, level, typ in levels:
        if all(abs(level - lv[1]) > mean_range * sensitivity for lv in filtered):
            filtered.append((i, level, typ))
    return filtered

def compute_strength(df, levels, tolerance=0.005):
    result = []
    for i, level, typ in levels:
        if typ == 'Support':
            touches = ((df['Low'] >= level * (1 - tolerance)) & (df['Low'] <= level * (1 + tolerance))).sum()
        else:
            touches = ((df['High'] >= level * (1 - tolerance)) & (df['High'] <= level * (1 + tolerance))).sum()
        result.append((i, level, typ, touches))
    return result

def nearest_two_levels(df, level_strengths):
    ltp = df['Close'].iloc[-1]
    supports = sorted([lvl for lvl in level_strengths if lvl[1] <= ltp], key=lambda x: -x[1])[:2]
    resistances = sorted([lvl for lvl in level_strengths if lvl[1] >= ltp], key=lambda x: x[1])[:2]

    def dist(p): return ((p - ltp) / ltp) * 100
    supports = [(i, p, t, s, dist(p)) for i, p, t, s in supports]
    resistances = [(i, p, t, s, dist(p)) for i, p, t, s in resistances]

    return ltp, supports, resistances

# -------------------------------------------------
# Plotting & Auto-Save Function
# -------------------------------------------------
def plot_and_save_levels(df, level_strengths, symbol, nearest_support, nearest_resistance, out_dir):
    df_plot = df.copy()
    df_plot['Date'] = df_plot['Date'].map(mpl_dates.date2num)
    ohlc = df_plot[['Date', 'Open', 'High', 'Low', 'Close']].values

    fig, ax = plt.subplots()
    candlestick_ohlc(ax, ohlc, width=0.6, colorup='green', colordown='red', alpha=0.8)

    for i, level, level_type, strength in level_strengths:
        color = 'green' if level_type == 'Support' else 'red'
        plt.hlines(level, xmin=df_plot['Date'][i], xmax=df_plot['Date'].iloc[-1],
                   colors=color, linewidth=1.2, alpha=0.7)
        plt.text(df_plot['Date'][i], level,
                 f"{level:.2f}\n({level_type[0]})[{strength}]",
                 color=color, fontsize=8)

    if nearest_support:
        plt.axhline(nearest_support[1], color='green', linestyle='--', linewidth=1.5)
    if nearest_resistance:
        plt.axhline(nearest_resistance[1], color='red', linestyle='--', linewidth=1.5)

    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mpl_dates.DateFormatter('%d-%b-%y'))
    plt.title(f"Support & Resistance Levels for {symbol}", fontsize=16, fontweight='bold')
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()

    chart_dir = Path(out_dir) / "Charts"
    chart_dir.mkdir(exist_ok=True, parents=True)
    plt.savefig(chart_dir / f"{symbol.replace('.NS','')}.png", dpi=150)
    plt.close()

# -------------------------------------------------
# Main Runner – Single Consolidated CSV Output
# -------------------------------------------------
def run_fractal_sr(tickers, period='6mo', interval='1d',
                   use_local=False, local_dir=None, out_dir='./SR_Outputs',
                   sensitivity=1.0, tolerance=0.005, plot=False,
                   save_charts=False, dist_range=0.3,
                   batch_size=10, delay_per_batch=2.0):
    tickers = list(set(tickers))
    out_dir = Path(out_dir)
    out_dir.mkdir(exist_ok=True, parents=True)
    summary = []
    all_levels = []

    # Delete existing Charts folder for a fresh start
    chart_dir = out_dir / "Charts"
    if chart_dir.exists() and chart_dir.is_dir():
        shutil.rmtree(chart_dir)

    # Single tqdm progress bar for all tickers
    with tqdm(total=len(tickers), desc="Processing tickers") as pbar:
        for batch_start in range(0, len(tickers), batch_size):
            batch = tickers[batch_start:batch_start + batch_size]

            for ticker in batch:
                try:
                    csv_ticker = ticker.replace('.NS','') if use_local else ticker
                    if use_local and local_dir:
                        path = Path(local_dir) / f"{csv_ticker}.csv"
                        if path.exists():
                            df = pd.read_csv(path, parse_dates=['Date'])
                        else:
                            summary.append({'SYMBOL': ticker, 'ERROR': 'Local CSV not found'})
                            pbar.update(1)
                            continue
                    else:
                        df = fetch_yf_data(ticker, period, interval)

                    if df.empty or len(df) < 10:
                        summary.append({'SYMBOL': ticker, 'ERROR': 'Insufficient data'})
                        pbar.update(1)
                        continue

                    levels = identify_levels(df)
                    if not levels:
                        summary.append({'SYMBOL': ticker, 'ERROR': 'No fractal levels detected'})
                        pbar.update(1)
                        continue

                    flt = filter_levels(df, levels, sensitivity)
                    strength = compute_strength(df, flt, tolerance)
                    ltp, sup, res = nearest_two_levels(df, strength)

                    for lv in strength:
                        dist_pct = ((lv[1]-ltp)/ltp)*100
                        all_levels.append({
                            'SYMBOL': ticker,
                            'CURRENT_PRICE': round(ltp, 2),
                            'LEVEL': round(lv[1], 2),
                            'TYPE': lv[2],
                            'STRENGTH': lv[3],
                            'DIST_%': round(dist_pct, 2)
                        })

                    summary.append({
                        'SYMBOL': ticker,
                        'LTP': round(ltp,2),
                        'SUPPORTS': ' | '.join([f"{p[1]:.2f} ({p[4]:+.2f}%)" for p in sup]),
                        'RESISTANCES': ' | '.join([f"{p[1]:.2f} ({p[4]:+.2f}%)" for p in res])
                    })

                    if plot:
                        plot_and_save_levels(df, strength, ticker, sup[0] if sup else None, res[0] if res else None, out_dir)

                    # Auto-save charts for levels within ±dist_range%
                    if save_charts:
                        filtered_levels = [lv for lv in strength if abs((lv[1]-ltp)/ltp*100) <= dist_range]
                        if filtered_levels:
                            plot_and_save_levels(df, strength, ticker, sup[0] if sup else None, res[0] if res else None, out_dir)

                except Exception as e:
                    summary.append({'SYMBOL': ticker, 'ERROR': str(e)})

                pbar.update(1)  # update progress for each ticker

            # Delay after each batch
            if batch_start + batch_size < len(tickers):
                time.sleep(delay_per_batch)

    # Save CSVs
    df_levels = pd.DataFrame(all_levels)
    df_levels.to_csv(out_dir / 'All_Levels.csv', index=False)
    df_sum = pd.DataFrame(summary)
    df_sum.to_csv(out_dir / 'Summary.csv', index=False)

    return df_sum, df_levels


# -------------------------------------------------
# Example Execution
# -------------------------------------------------
if __name__ == '__main__':

    tickers = ["360ONE.NS", "3MINDIA.NS", "ABB.NS", "ACC.NS", "ACMESOLAR.NS", "AIAENG.NS", "APLAPOLLO.NS", "AUBANK.NS", "AWL.NS", "AADHARHFC.NS", "AARTIIND.NS", "AAVAS.NS", "ABBOTINDIA.NS", "ACE.NS", "ADANIENSOL.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ADANIPOWER.NS", "ATGL.NS", "ABCAPITAL.NS", "ABFRL.NS", "ABLBL.NS", "ABREL.NS", "ABSLAMC.NS", "AEGISLOG.NS", "AEGISVOPAK.NS", "AFCONS.NS", "AFFLE.NS", "AJANTPHARM.NS", "AKUMS.NS", "AKZOINDIA.NS", "APLLTD.NS", "ALKEM.NS", "ALKYLAMINE.NS", "ALOKINDS.NS", "ARE&M.NS", "AMBER.NS", "AMBUJACEM.NS", "ANANDRATHI.NS", "ANANTRAJ.NS", "ANGELONE.NS", "APARINDS.NS", "APOLLOHOSP.NS", "APOLLOTYRE.NS", "APTUS.NS", "ASAHIINDIA.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTERDM.NS", "ASTRAZEN.NS", "ASTRAL.NS", "ATHERENERG.NS", "ATUL.NS", "AUROPHARMA.NS", "AIIL.NS", "DMART.NS", "AXISBANK.NS", "BASF.NS", "BEML.NS", "BLS.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BAJAJHLDNG.NS", "BAJAJHFL.NS", "BALKRISIND.NS", "BALRAMCHIN.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS", "MAHABANK.NS", "BATAINDIA.NS", "BAYERCROP.NS", "BERGEPAINT.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHEL.NS", "BPCL.NS", "BHARTIARTL.NS", "BHARTIHEXA.NS", "BIKAJI.NS", "BIOCON.NS", "BSOFT.NS", "BLUEDART.NS", "BLUEJET.NS", "BLUESTARCO.NS", "BBTC.NS", "BOSCHLTD.NS", "FIRSTCRY.NS", "BRIGADE.NS", "BRITANNIA.NS", "MAPMYINDIA.NS", "CCL.NS", "CESC.NS", "CGPOWER.NS", "CRISIL.NS", "CAMPUS.NS", "CANFINHOME.NS", "CANBK.NS", "CAPLIPOINT.NS", "CGCL.NS", "CARBORUNIV.NS", "CASTROLIND.NS", "CEATLTD.NS", "CENTRALBK.NS", "CDSL.NS", "CENTURYPLY.NS", "CERA.NS", "CHALET.NS", "CHAMBLFERT.NS", "CHENNPETRO.NS", "CHOICEIN.NS", "CHOLAHLDNG.NS", "CHOLAFIN.NS", "CIPLA.NS", "CUB.NS", "CLEAN.NS", "COALINDIA.NS", "COCHINSHIP.NS", "COFORGE.NS", "COHANCE.NS", "COLPAL.NS", "CAMS.NS", "CONCORDBIO.NS", "CONCOR.NS", "COROMANDEL.NS", "CRAFTSMAN.NS", "CREDITACC.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DCMSHRIRAM.NS", "DLF.NS", "DOMS.NS", "DABUR.NS", "DALBHARAT.NS", "DATAPATTNS.NS", "DEEPAKFERT.NS", "DEEPAKNTR.NS", "DELHIVERY.NS", "DEVYANI.NS", "DIVISLAB.NS", "DIXON.NS", "AGARWALEYE.NS", "LALPATHLAB.NS", "DRREDDY.NS", "EIDPARRY.NS", "EIHOTEL.NS", "EICHERMOT.NS", "ELECON.NS", "ELGIEQUIP.NS", "EMAMILTD.NS", "EMCURE.NS", "ENDURANCE.NS", "ENGINERSIN.NS", "ERIS.NS", "ESCORTS.NS", "ETERNAL.NS", "EXIDEIND.NS", "NYKAA.NS", "FEDERALBNK.NS", "FACT.NS", "FINCABLES.NS", "FINPIPE.NS", "FSL.NS", "FIVESTAR.NS", "FORCEMOT.NS", "FORTIS.NS", "GAIL.NS", "GVT&D.NS", "GMRAIRPORT.NS", "GRSE.NS", "GICRE.NS", "GILLETTE.NS", "GLAND.NS", "GLAXO.NS", "GLENMARK.NS", "MEDANTA.NS", "GODIGIT.NS", "GPIL.NS", "GODFRYPHLP.NS", "GODREJAGRO.NS", "GODREJCP.NS", "GODREJIND.NS", "GODREJPROP.NS", "GRANULES.NS", "GRAPHITE.NS", "GRASIM.NS", "GRAVITA.NS", "GESHIP.NS", "FLUOROCHEM.NS", "GUJGASLTD.NS", "GMDCLTD.NS", "GSPL.NS", "HEG.NS", "HBLENGINE.NS", "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HFCL.NS", "HAPPSTMNDS.NS", "HAVELLS.NS", "HEROMOTOCO.NS", "HEXT.NS", "HSCL.NS", "HINDALCO.NS", "HAL.NS", "HINDCOPPER.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "POWERINDIA.NS", "HOMEFIRST.NS", "HONASA.NS", "HONAUT.NS", "HUDCO.NS", "HYUNDAI.NS", "ICICIBANK.NS", "ICICIGI.NS", "ICICIPRULI.NS", "IDBI.NS", "IDFCFIRSTB.NS", "IFCI.NS", "IIFL.NS", "INOXINDIA.NS", "IRB.NS", "IRCON.NS", "ITCHOTELS.NS", "ITC.NS", "ITI.NS", "INDGN.NS", "INDIACEM.NS", "INDIAMART.NS", "INDIANB.NS", "IEX.NS", "INDHOTEL.NS", "IOC.NS", "IOB.NS", "IRCTC.NS", "IRFC.NS", "IREDA.NS", "IGL.NS", "INDUSTOWER.NS", "INDUSINDBK.NS", "NAUKRI.NS", "INFY.NS", "INOXWIND.NS", "INTELLECT.NS", "INDIGO.NS", "IGIL.NS", "IKS.NS", "IPCALAB.NS", "JBCHEPHARM.NS", "JKCEMENT.NS", "JBMA.NS", "JKTYRE.NS", "JMFINANCIL.NS", "JSWENERGY.NS", "JSWINFRA.NS", "JSWSTEEL.NS", "JPPOWER.NS", "J&KBANK.NS", "JINDALSAW.NS", "JSL.NS", "JINDALSTEL.NS", "JIOFIN.NS", "JUBLFOOD.NS", "JUBLINGREA.NS", "JUBLPHARMA.NS", "JWL.NS", "JYOTHYLAB.NS", "JYOTICNC.NS", "KPRMILL.NS", "KEI.NS", "KPITTECH.NS", "KSB.NS", "KAJARIACER.NS", "KPIL.NS", "KALYANKJIL.NS", "KARURVYSYA.NS", "KAYNES.NS", "KEC.NS", "KFINTECH.NS", "KIRLOSBROS.NS", "KIRLOSENG.NS", "KOTAKBANK.NS", "KIMS.NS", "LTF.NS", "LTTS.NS", "LICHSGFIN.NS", "LTFOODS.NS", "LTIM.NS", "LT.NS", "LATENTVIEW.NS", "LAURUSLABS.NS", "THELEELA.NS", "LEMONTREE.NS", "LICI.NS", "LINDEINDIA.NS", "LLOYDSME.NS", "LODHA.NS", "LUPIN.NS", "MMTC.NS", "MRF.NS", "MGL.NS", "MAHSCOOTER.NS", "MAHSEAMLES.NS", "M&MFIN.NS", "M&M.NS", "MANAPPURAM.NS", "MRPL.NS", "MANKIND.NS", "MARICO.NS", "MARUTI.NS", "MFSL.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "METROPOLIS.NS", "MINDACORP.NS", "MSUMI.NS", "MOTILALOFS.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NATCOPHARM.NS", "NBCC.NS", "NCC.NS", "NHPC.NS", "NLCINDIA.NS", "NMDC.NS", "NSLNISP.NS", "NTPCGREEN.NS", "NTPC.NS", "NH.NS", "NATIONALUM.NS", "NAVA.NS", "NAVINFLUOR.NS", "NESTLEIND.NS", "NETWEB.NS", "NEULANDLAB.NS", "NEWGEN.NS", "NAM-INDIA.NS", "NIVABUPA.NS", "NUVAMA.NS", "NUVOCO.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "OLAELEC.NS", "OLECTRA.NS", "PAYTM.NS", "ONESOURCE.NS", "OFSS.NS", "POLICYBZR.NS", "PCBL.NS", "PGEL.NS", "PIIND.NS", "PNBHOUSING.NS", "PTCIL.NS", "PVRINOX.NS", "PAGEIND.NS", "PATANJALI.NS", "PERSISTENT.NS", "PETRONET.NS", "PFIZER.NS", "PHOENIXLTD.NS", "PIDILITIND.NS", "PPLPHARMA.NS", "POLYMED.NS", "POLYCAB.NS", "POONAWALLA.NS", "PFC.NS", "POWERGRID.NS", "PRAJIND.NS", "PREMIERENE.NS", "PRESTIGE.NS", "PGHH.NS", "PNB.NS", "RRKABEL.NS", "RBLBANK.NS", "RECLTD.NS", "RHIM.NS", "RITES.NS", "RADICO.NS", "RVNL.NS", "RAILTEL.NS", "RAINBOW.NS", "RKFORGE.NS", "RCF.NS", "REDINGTON.NS", "RELIANCE.NS", "RELINFRA.NS", "RPOWER.NS", "SBFC.NS", "SBICARD.NS", "SBILIFE.NS", "SJVN.NS", "SKFINDIA.NS", "SRF.NS", "SAGILITY.NS", "SAILIFE.NS", "SAMMAANCAP.NS", "MOTHERSON.NS", "SAPPHIRE.NS", "SARDAEN.NS", "SAREGAMA.NS", "SCHAEFFLER.NS", "SCHNEIDER.NS", "SCI.NS", "SHREECEM.NS", "SHRIRAMFIN.NS", "SHYAMMETL.NS", "ENRIN.NS", "SIEMENS.NS", "SIGNATURE.NS", "SOBHA.NS", "SOLARINDS.NS", "SONACOMS.NS", "SONATSOFTW.NS", "STARHEALTH.NS", "SBIN.NS", "SAIL.NS", "SUMICHEM.NS", "SUNPHARMA.NS", "SUNTV.NS", "SUNDARMFIN.NS", "SUNDRMFAST.NS", "SUPREMEIND.NS", "SUZLON.NS", "SWANCORP.NS", "SWIGGY.NS", "SYNGENE.NS", "SYRMA.NS", "TBOTEK.NS", "TVSMOTOR.NS", "TATACHEM.NS", "TATACOMM.NS", "TCS.NS", "TATACONSUM.NS", "TATAELXSI.NS", "TATAINVEST.NS", "TMPV.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TATATECH.NS", "TTML.NS", "TECHM.NS", "TECHNOE.NS", "TEJASNET.NS", "NIACL.NS", "RAMCOCEM.NS", "THERMAX.NS", "TIMKEN.NS", "TITAGARH.NS", "TITAN.NS", "TORNTPHARM.NS", "TORNTPOWER.NS", "TARIL.NS", "TRENT.NS", "TRIDENT.NS", "TRIVENI.NS", "TRITURBINE.NS", "TIINDIA.NS", "UCOBANK.NS", "UNOMINDA.NS", "UPL.NS", "UTIAMC.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UBL.NS", "UNITDSPR.NS", "USHAMART.NS", "VGUARD.NS", "DBREALTY.NS", "VTL.NS", "VBL.NS", "MANYAVAR.NS", "VEDL.NS", "VENTIVE.NS", "VIJAYA.NS", "VMM.NS", "IDEA.NS", "VOLTAS.NS", "WAAREEENER.NS", "WELCORP.NS", "WELSPUNLIV.NS", "WHIRLPOOL.NS", "WIPRO.NS", "WOCKPHARMA.NS", "YESBANK.NS", "ZFCVINDIA.NS", "ZEEL.NS", "ZENTEC.NS", "ZENSARTECH.NS", "ZYDUSLIFE.NS", "ECLERX.NS"]
    
    # tickers = ["POWERINDIA.NS", "TCS.NS", "RELIANCE.NS"]  # Example tickers

    summary, all_levels = run_fractal_sr(
        tickers,
        period          = '6mo',
        interval        = '1d',
        use_local       = False,                                    # True = use local csv
        local_dir       = None,      #r"D:\user\Trading\Stocks",    # CSV path csv file name like (RELIANCE, TCS, INFY -  file format must be .csv)
        plot            = False,                                    # Set True to show charts interactively
        save_charts     = True,                                     # Set True to save charts automatically
        dist_range      = 0.3,                                      # DIST_% ±0.3%
        batch_size      = 10,
        delay_per_batch = 2.0
    )

    print("\n📊 Summary:")
    print(summary)
    print("\n📈 Combined All_Levels:")
    print(all_levels.head())
