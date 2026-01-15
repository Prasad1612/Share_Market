import yfinance as yf
import pandas as pd
import numpy as np
import time
from prettytable import PrettyTable
from tqdm import tqdm

# ======================================================
# 🗂 STOCK GROUPS
# ======================================================
indices         = ["^NSEI", "^NSEBANK", "^CNXIT", "^CNXAUTO", "^CNXFMCG", "^CNXMETAL", "^CNXPHARMA", "^CNXREALTY", "^CNXENERGY", "^CNXMEDIA"]

nifty_top_10    = ["HDFCBANK.NS", "ICICIBANK.NS", "RELIANCE.NS", "INFY.NS", "BHARTIARTL.NS", "LT.NS", "ITC.NS", "SBIN.NS", "AXISBANK.NS", "KOTAKBANK.NS"]

nifty_50        = ["ADANIENT.NS", "ADANIPORTS.NS", "APOLLOHOSP.NS", "ASIANPAINT.NS", "AXISBANK.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BEL.NS", "BHARTIARTL.NS", "CIPLA.NS", "COALINDIA.NS", "DRREDDY.NS", "EICHERMOT.NS", "ETERNAL.NS", "GRASIM.NS", "HCLTECH.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HINDALCO.NS", "HINDUNILVR.NS", "ICICIBANK.NS", "ITC.NS", "INFY.NS", "INDIGO.NS", "JSWSTEEL.NS", "JIOFIN.NS", "KOTAKBANK.NS", "LT.NS", "M&M.NS", "MARUTI.NS", "MAXHEALTH.NS", "NTPC.NS", "NESTLEIND.NS", "ONGC.NS", "POWERGRID.NS", "RELIANCE.NS", "SBILIFE.NS", "SHRIRAMFIN.NS", "SBIN.NS", "SUNPHARMA.NS", "TCS.NS", "TATACONSUM.NS", "TATAMOTORS.NS", "TATASTEEL.NS", "TECHM.NS", "TITAN.NS", "TRENT.NS", "ULTRACEMCO.NS", "WIPRO.NS"]

fn_o_stocks     = ["360ONE.NS", "ABB.NS", "APLAPOLLO.NS", "AUBANK.NS", "ADANIENSOL.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ABCAPITAL.NS", "ALKEM.NS", "AMBER.NS", "AMBUJACEM.NS", "ANGELONE.NS", "APOLLOHOSP.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTRAL.NS", "AUROPHARMA.NS", "DMART.NS", "AXISBANK.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHEL.NS", "BPCL.NS", "BHARTIARTL.NS", "BIOCON.NS", "BLUESTARCO.NS", "BOSCHLTD.NS", "BRITANNIA.NS", "CGPOWER.NS", "CANBK.NS", "CDSL.NS", "CHOLAFIN.NS", "CIPLA.NS", "COALINDIA.NS", "COFORGE.NS", "COLPAL.NS", "CAMS.NS", "CONCOR.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DLF.NS", "DABUR.NS", "DALBHARAT.NS", "DELHIVERY.NS", "DIVISLAB.NS", "DIXON.NS", "DRREDDY.NS", "ETERNAL.NS", "EICHERMOT.NS", "EXIDEIND.NS", "NYKAA.NS", "FORTIS.NS", "GAIL.NS", "GMRAIRPORT.NS", "GLENMARK.NS", "GODREJCP.NS", "GODREJPROP.NS", "GRASIM.NS", "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HFCL.NS", "HAVELLS.NS", "HEROMOTOCO.NS", "HINDALCO.NS", "HAL.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "POWERINDIA.NS", "HUDCO.NS", "ICICIBANK.NS", "ICICIGI.NS", "ICICIPRULI.NS", "IDFCFIRSTB.NS", "IIFL.NS", "ITC.NS", "INDIANB.NS", "IEX.NS", "IOC.NS", "IRCTC.NS", "IRFC.NS", "IREDA.NS", "IGL.NS", "INDUSTOWER.NS", "INDUSINDBK.NS", "NAUKRI.NS", "INFY.NS", "INOXWIND.NS", "INDIGO.NS", "JINDALSTEL.NS", "JSWENERGY.NS", "JSWSTEEL.NS", "JIOFIN.NS", "JUBLFOOD.NS", "KEI.NS", "KPITTECH.NS", "KALYANKJIL.NS", "KAYNES.NS", "KFINTECH.NS", "KOTAKBANK.NS", "LTF.NS", "LICHSGFIN.NS", "LTIM.NS", "LT.NS", "LAURUSLABS.NS", "LICI.NS", "LODHA.NS", "LUPIN.NS", "M&M.NS", "MANAPPURAM.NS", "MANKIND.NS", "MARICO.NS", "MARUTI.NS", "MFSL.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NBCC.NS", "NCC.NS", "NHPC.NS", "NMDC.NS", "NTPC.NS", "NATIONALUM.NS", "NESTLEIND.NS", "NUVAMA.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "PAYTM.NS", "OFSS.NS", "POLICYBZR.NS", "PGEL.NS", "PIIND.NS", "PNBHOUSING.NS", "PAGEIND.NS", "PATANJALI.NS", "PERSISTENT.NS", "PETRONET.NS", "PIDILITIND.NS", "PPLPHARMA.NS", "POLYCAB.NS", "PFC.NS", "POWERGRID.NS", "PRESTIGE.NS", "PNB.NS", "RBLBANK.NS", "RECLTD.NS", "RVNL.NS", "RELIANCE.NS", "SBICARD.NS", "SBILIFE.NS", "SHREECEM.NS", "SRF.NS", "SAMMAANCAP.NS", "MOTHERSON.NS", "SHRIRAMFIN.NS", "SIEMENS.NS", "SOLARINDS.NS", "SONACOMS.NS", "SBIN.NS", "SAIL.NS", "SUNPHARMA.NS", "SUPREMEIND.NS", "SUZLON.NS", "SYNGENE.NS", "TATACONSUM.NS", "TITAGARH.NS", "TVSMOTOR.NS", "TCS.NS", "TATAELXSI.NS", "TATAMOTORS.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TATATECH.NS", "TECHM.NS", "FEDERALBNK.NS", "INDHOTEL.NS", "PHOENIXLTD.NS", "TITAN.NS", "TORNTPHARM.NS", "TORNTPOWER.NS", "TRENT.NS", "TIINDIA.NS", "UNOMINDA.NS", "UPL.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UNITDSPR.NS", "VBL.NS", "VEDL.NS", "IDEA.NS", "VOLTAS.NS", "WIPRO.NS", "YESBANK.NS", "ZYDUSLIFE.NS"]

nifty_500       = ["360ONE.NS", "3MINDIA.NS", "ABB.NS", "ACC.NS", "ACMESOLAR.NS", "AIAENG.NS", "APLAPOLLO.NS", "AUBANK.NS", "AWL.NS", "AADHARHFC.NS", "AARTIIND.NS", "AAVAS.NS", "ABBOTINDIA.NS", "ACE.NS", "ADANIENSOL.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ADANIPOWER.NS", "ATGL.NS", "ABCAPITAL.NS", "ABFRL.NS", "ABLBL.NS", "ABREL.NS", "ABSLAMC.NS", "AEGISLOG.NS", "AEGISVOPAK.NS", "AFCONS.NS", "AFFLE.NS", "AJANTPHARM.NS", "AKUMS.NS", "AKZOINDIA.NS", "APLLTD.NS", "ALKEM.NS", "ALKYLAMINE.NS", "ALOKINDS.NS", "ARE&M.NS", "AMBER.NS", "AMBUJACEM.NS", "ANANDRATHI.NS", "ANANTRAJ.NS", "ANGELONE.NS", "APARINDS.NS", "APOLLOHOSP.NS", "APOLLOTYRE.NS", "APTUS.NS", "ASAHIINDIA.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTERDM.NS", "ASTRAZEN.NS", "ASTRAL.NS", "ATHERENERG.NS", "ATUL.NS", "AUROPHARMA.NS", "AIIL.NS", "DMART.NS", "AXISBANK.NS", "BASF.NS", "BEML.NS", "BLS.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BAJAJHLDNG.NS", "BAJAJHFL.NS", "BALKRISIND.NS", "BALRAMCHIN.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS", "MAHABANK.NS", "BATAINDIA.NS", "BAYERCROP.NS", "BERGEPAINT.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHEL.NS", "BPCL.NS", "BHARTIARTL.NS", "BHARTIHEXA.NS", "BIKAJI.NS", "BIOCON.NS", "BSOFT.NS", "BLUEDART.NS", "BLUEJET.NS", "BLUESTARCO.NS", "BBTC.NS", "BOSCHLTD.NS", "FIRSTCRY.NS", "BRIGADE.NS", "BRITANNIA.NS", "MAPMYINDIA.NS", "CCL.NS", "CESC.NS", "CGPOWER.NS", "CRISIL.NS", "CAMPUS.NS", "CANFINHOME.NS", "CANBK.NS", "CAPLIPOINT.NS", "CGCL.NS", "CARBORUNIV.NS", "CASTROLIND.NS", "CEATLTD.NS", "CENTRALBK.NS", "CDSL.NS", "CENTURYPLY.NS", "CERA.NS", "CHALET.NS", "CHAMBLFERT.NS", "CHENNPETRO.NS", "CHOICEIN.NS", "CHOLAHLDNG.NS", "CHOLAFIN.NS", "CIPLA.NS", "CUB.NS", "CLEAN.NS", "COALINDIA.NS", "COCHINSHIP.NS", "COFORGE.NS", "COHANCE.NS", "COLPAL.NS", "CAMS.NS", "CONCORDBIO.NS", "CONCOR.NS", "COROMANDEL.NS", "CRAFTSMAN.NS", "CREDITACC.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DCMSHRIRAM.NS", "DLF.NS", "DOMS.NS", "DABUR.NS", "DALBHARAT.NS", "DATAPATTNS.NS", "DEEPAKFERT.NS", "DEEPAKNTR.NS", "DELHIVERY.NS", "DEVYANI.NS", "DIVISLAB.NS", "DIXON.NS", "AGARWALEYE.NS", "LALPATHLAB.NS", "DRREDDY.NS", "EIDPARRY.NS", "EIHOTEL.NS", "EICHERMOT.NS", "ELECON.NS", "ELGIEQUIP.NS", "EMAMILTD.NS", "EMCURE.NS", "ENDURANCE.NS", "ENGINERSIN.NS", "ERIS.NS", "ESCORTS.NS", "ETERNAL.NS", "EXIDEIND.NS", "NYKAA.NS", "FEDERALBNK.NS", "FACT.NS", "FINCABLES.NS", "FINPIPE.NS", "FSL.NS", "FIVESTAR.NS", "FORCEMOT.NS", "FORTIS.NS", "GAIL.NS", "GVT&D.NS", "GMRAIRPORT.NS", "GRSE.NS", "GICRE.NS", "GILLETTE.NS", "GLAND.NS", "GLAXO.NS", "GLENMARK.NS", "MEDANTA.NS", "GODIGIT.NS", "GPIL.NS", "GODFRYPHLP.NS", "GODREJAGRO.NS", "GODREJCP.NS", "GODREJIND.NS", "GODREJPROP.NS", "GRANULES.NS", "GRAPHITE.NS", "GRASIM.NS", "GRAVITA.NS", "GESHIP.NS", "FLUOROCHEM.NS", "GUJGASLTD.NS", "GMDCLTD.NS", "GSPL.NS", "HEG.NS", "HBLENGINE.NS", "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HFCL.NS", "HAPPSTMNDS.NS", "HAVELLS.NS", "HEROMOTOCO.NS", "HEXT.NS", "HSCL.NS", "HINDALCO.NS", "HAL.NS", "HINDCOPPER.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "POWERINDIA.NS", "HOMEFIRST.NS", "HONASA.NS", "HONAUT.NS", "HUDCO.NS", "HYUNDAI.NS", "ICICIBANK.NS", "ICICIGI.NS", "ICICIPRULI.NS", "IDBI.NS", "IDFCFIRSTB.NS", "IFCI.NS", "IIFL.NS", "INOXINDIA.NS", "IRB.NS", "IRCON.NS", "ITCHOTELS.NS", "ITC.NS", "ITI.NS", "INDGN.NS", "INDIACEM.NS", "INDIAMART.NS", "INDIANB.NS", "IEX.NS", "INDHOTEL.NS", "IOC.NS", "IOB.NS", "IRCTC.NS", "IRFC.NS", "IREDA.NS", "IGL.NS", "INDUSTOWER.NS", "INDUSINDBK.NS", "NAUKRI.NS", "INFY.NS", "INOXWIND.NS", "INTELLECT.NS", "INDIGO.NS", "IGIL.NS", "IKS.NS", "IPCALAB.NS", "JBCHEPHARM.NS", "JKCEMENT.NS", "JBMA.NS", "JKTYRE.NS", "JMFINANCIL.NS", "JSWCEMENT.NS", "JSWENERGY.NS", "JSWINFRA.NS", "JSWSTEEL.NS", "JPPOWER.NS", "J&KBANK.NS", "JINDALSAW.NS", "JSL.NS", "JINDALSTEL.NS", "JIOFIN.NS", "JUBLFOOD.NS", "JUBLINGREA.NS", "JUBLPHARMA.NS", "JWL.NS", "JYOTHYLAB.NS", "JYOTICNC.NS", "KPRMILL.NS", "KEI.NS", "KPITTECH.NS", "KSB.NS", "KAJARIACER.NS", "KPIL.NS", "KALYANKJIL.NS", "KARURVYSYA.NS", "KAYNES.NS", "KEC.NS", "KFINTECH.NS", "KIRLOSBROS.NS", "KIRLOSENG.NS", "KOTAKBANK.NS", "KIMS.NS", "LTF.NS", "LTTS.NS", "LICHSGFIN.NS", "LTFOODS.NS", "LTIM.NS", "LT.NS", "LATENTVIEW.NS", "LAURUSLABS.NS", "THELEELA.NS", "LEMONTREE.NS", "LICI.NS", "LINDEINDIA.NS", "LLOYDSME.NS", "LODHA.NS", "LUPIN.NS", "MMTC.NS", "MRF.NS", "MGL.NS", "MAHSCOOTER.NS", "MAHSEAMLES.NS", "M&MFIN.NS", "M&M.NS", "MANAPPURAM.NS", "MRPL.NS", "MANKIND.NS", "MARICO.NS", "MARUTI.NS", "MFSL.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "METROPOLIS.NS", "MINDACORP.NS", "MSUMI.NS", "MOTILALOFS.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NATCOPHARM.NS", "NBCC.NS", "NCC.NS", "NHPC.NS", "NLCINDIA.NS", "NMDC.NS", "NSLNISP.NS", "NTPCGREEN.NS", "NTPC.NS", "NH.NS", "NATIONALUM.NS", "NAVA.NS", "NAVINFLUOR.NS", "NESTLEIND.NS", "NETWEB.NS", "NEULANDLAB.NS", "NEWGEN.NS", "NAM-INDIA.NS", "NIVABUPA.NS", "NUVAMA.NS", "NUVOCO.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "OLAELEC.NS", "OLECTRA.NS", "PAYTM.NS", "ONESOURCE.NS", "OFSS.NS", "POLICYBZR.NS", "PCBL.NS", "PGEL.NS", "PIIND.NS", "PNBHOUSING.NS", "PTCIL.NS", "PVRINOX.NS", "PAGEIND.NS", "PATANJALI.NS", "PERSISTENT.NS", "PETRONET.NS", "PFIZER.NS", "PHOENIXLTD.NS", "PIDILITIND.NS", "PPLPHARMA.NS", "POLYMED.NS", "POLYCAB.NS", "POONAWALLA.NS", "PFC.NS", "POWERGRID.NS", "PRAJIND.NS", "PREMIERENE.NS", "PRESTIGE.NS", "PGHH.NS", "PNB.NS", "RRKABEL.NS", "RBLBANK.NS", "RECLTD.NS", "RHIM.NS", "RITES.NS", "RADICO.NS", "RVNL.NS", "RAILTEL.NS", "RAINBOW.NS", "RKFORGE.NS", "RCF.NS", "REDINGTON.NS", "RELIANCE.NS", "RELINFRA.NS", "RPOWER.NS", "SBFC.NS", "SBICARD.NS", "SBILIFE.NS", "SJVN.NS", "SRF.NS", "SAGILITY.NS", "SAILIFE.NS", "SAMMAANCAP.NS", "MOTHERSON.NS", "SAPPHIRE.NS", "SARDAEN.NS", "SAREGAMA.NS", "SCHAEFFLER.NS", "SCHNEIDER.NS", "SCI.NS", "SHREECEM.NS", "SHRIRAMFIN.NS", "SHYAMMETL.NS", "ENRIN.NS", "SIEMENS.NS", "SIGNATURE.NS", "SOBHA.NS", "SOLARINDS.NS", "SONACOMS.NS", "SONATSOFTW.NS", "STARHEALTH.NS", "SBIN.NS", "SAIL.NS", "SUMICHEM.NS", "SUNPHARMA.NS", "SUNTV.NS", "SUNDARMFIN.NS", "SUNDRMFAST.NS", "SUPREMEIND.NS", "SUZLON.NS", "SWANCORP.NS", "SWIGGY.NS", "SYNGENE.NS", "SYRMA.NS", "TBOTEK.NS", "TVSMOTOR.NS", "TATACHEM.NS", "TATACOMM.NS", "TCS.NS", "TATACONSUM.NS", "TATAELXSI.NS", "TATAINVEST.NS", "TMPV.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TATATECH.NS", "TTML.NS", "TECHM.NS", "TECHNOE.NS", "TEJASNET.NS", "NIACL.NS", "RAMCOCEM.NS", "THERMAX.NS", "TIMKEN.NS", "TITAGARH.NS", "TITAN.NS", "TORNTPHARM.NS", "TORNTPOWER.NS", "TARIL.NS", "TRENT.NS", "TRIDENT.NS", "TRIVENI.NS", "TRITURBINE.NS", "TIINDIA.NS", "UCOBANK.NS", "UNOMINDA.NS", "UPL.NS", "UTIAMC.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UBL.NS", "UNITDSPR.NS", "USHAMART.NS", "VGUARD.NS", "DBREALTY.NS", "VTL.NS", "VBL.NS", "MANYAVAR.NS", "VEDL.NS", "VENTIVE.NS", "VIJAYA.NS", "VMM.NS", "IDEA.NS", "VOLTAS.NS", "WAAREEENER.NS", "WELCORP.NS", "WELSPUNLIV.NS", "WHIRLPOOL.NS", "WIPRO.NS", "WOCKPHARMA.NS", "YESBANK.NS", "ZFCVINDIA.NS", "ZEEL.NS", "ZENTEC.NS", "ZENSARTECH.NS", "ZYDUSLIFE.NS", "ECLERX.NS"]

groups = { "indices": indices, "nifty_top_10": nifty_top_10, "nifty_50": nifty_50, "fn_o_stocks": fn_o_stocks, "nifty_500": nifty_500}

# ======================================================
# 🧭 CONFIGURATION VARIABLES
# ======================================================
period              = "1y"     # Data period        "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y","5y", "10y", "ytd", "max"
interval            = "1d"     # Candle interval    "1m", "2m", "5m", "15m", "30m", "60m"/"1h", "90m",      "1d", "5d", "1wk", "1mo", "3mo"
auto_adjust         = False    # False → Raw prices as traded historically          True → Adjusts for splits & dividends.
near_tolerance      = 0.03     # 0.03 ±3% near price zone
min_gap_percent     = 1.0      # 1.0 Only consider gaps ≥ 1% in size
sleep_after         = 10       # After how many stocks to sleep
sleep_time          = 1        # Sleep time (seconds) after every batch
only_near           = True     # True = only gaps near current price; False = include all

# ======================================================
# 🗳 USER SELECTION FOR GROUP (NICE LOOK)
# ======================================================
print("\n📊  Select Stock Group to Scan:\n")

menu_items = [
    ("🏦", "indices"),
    ("📈", "nifty_top_10"),
    ("📈", "nifty_50"),
    ("💹", "fn_o_stocks"),
    ("📈", "nifty_500")
]

# Calculate padding for alignment
max_name_length = max(len(name) for _, name in menu_items)

for i, (emoji, name) in enumerate(menu_items, start=1):
    stock_count = len(groups[name])
    print(f"{i}. {emoji} {name.ljust(max_name_length)} ({stock_count} stocks)")

# Get user input
while True:
    try:
        choice = int(input("\n➡️  Enter choice number: "))
        if 1 <= choice <= len(menu_items):
            selected_group = menu_items[choice-1][1]
            tickers = groups[selected_group]
            print(f"\n✅ Selected group: {selected_group} ({len(tickers)} stocks) 🔹\n")
            break
        else:
            print(f"⚠️  Enter a number between 1 and {len(menu_items)}")
    except ValueError:
        print("⚠️  Invalid input, please enter a number")


# ======================================================
# 🔍 Detect Gaps Function
# ======================================================
def detect_gaps(ticker_symbols):
    results = {}

    for index, ticker in enumerate(tqdm(ticker_symbols, desc="🔍 Scanning Stocks", unit="stock")):
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period=period, interval=interval, auto_adjust=auto_adjust)
            if df.empty:
                tqdm.write(f"⚠️ No data for {ticker}")
                continue

            df.reset_index(inplace=True)
            gaps = []
            current_price = df['Close'].iloc[-1]

            for i in range(1, len(df)):
                prev_low = df['Low'].iloc[i-1]
                prev_high = df['High'].iloc[i-1]
                prev_close = df['Close'].iloc[i-1]
                curr_high = df['High'].iloc[i]
                curr_low = df['Low'].iloc[i]

                # ---- Bearish Gap ----
                if curr_high < prev_low:
                    raw_gap_range = (prev_low, curr_high) # (Top, Bottom)
                    
                    # Check for fills
                    post_data = df.iloc[i+1:]
                    
                    # Check if FULLY filled (High goes above the gap top)
                    filled = any(post_data['High'] >= prev_low)
                    if filled:
                        continue
                        
                    # Calculate Invading High (highest price reached inside the gap after formation)
                    if not post_data.empty:
                        invading_high = post_data['High'].max()
                    else:
                        invading_high = curr_high # No post data, so effective bottom is just the gap bottom

                    # Effective Bottom moves UP if price invaded the gap
                    effective_bottom = max(curr_high, invading_high)
                    
                    # Recalculate Gap Size based on remaining unfilled portion
                    gap_size = prev_low - effective_bottom
                    gap_range = (prev_low, effective_bottom)
                    
                    # Re-check min gap percent on the remaining gap
                    gap_size_percent = (gap_size / prev_close) * 100
                    if gap_size_percent < min_gap_percent:
                        continue

                    near_price = (
                        abs(current_price - gap_range[0]) / current_price <= near_tolerance or
                        abs(current_price - gap_range[1]) / current_price <= near_tolerance
                    )
                    if only_near and not near_price:
                        continue
                    
                    # SWAP for Bearish Gap
                    gap_distance_start_percent = ((current_price - gap_range[1]) / current_price) * 100  # upper -> start
                    gap_distance_end_percent   = ((current_price - gap_range[0]) / current_price) * 100  # lower -> end

                    gaps.append({
                        "date": df['Date'].iloc[i].date(),
                        "gap_type": "Bearish Gap",
                        "gap_size": gap_size,
                        "gap_range": gap_range,
                        "near_price": near_price,
                        "gap_size_percent": gap_size_percent,
                        "gap_distance_start_percent": gap_distance_start_percent,
                        "gap_distance_end_percent": gap_distance_end_percent
                    })


                # ---- Bullish Gap ----
                if curr_low > prev_high:
                    raw_gap_range = (prev_high, curr_low) # (Bottom, Top)
                    
                    # Check for fills
                    post_data = df.iloc[i+1:]
                    
                    # Check if FULLY filled (Low goes below the gap bottom)
                    filled = any(post_data['Low'] <= prev_high)
                    if filled:
                        continue

                    # Calculate Invading Low (lowest price reached inside the gap after formation)
                    if not post_data.empty:
                        invading_low = post_data['Low'].min()
                    else:
                        invading_low = curr_low
                        
                    # Effective Top moves DOWN if price invaded the gap
                    effective_top = min(curr_low, invading_low)
                    
                    # Recalculate Gap Size based on remaining unfilled portion
                    gap_size = effective_top - prev_high
                    gap_range = (prev_high, effective_top)

                    # Re-check min gap percent on the remaining gap
                    gap_size_percent = (gap_size / prev_close) * 100
                    if gap_size_percent < min_gap_percent:
                        continue

                    near_price = (
                        abs(current_price - gap_range[0]) / current_price <= near_tolerance or
                        abs(current_price - gap_range[1]) / current_price <= near_tolerance
                    )
                    if only_near and not near_price:
                        continue
                    gap_distance_start_percent = ((current_price - gap_range[1]) / current_price) * 100
                    gap_distance_end_percent = ((current_price - gap_range[0]) / current_price) * 100
                    gaps.append({
                        "date": df['Date'].iloc[i].date(),
                        "gap_type": "Bullish Gap",
                        "gap_size": gap_size,
                        "gap_range": gap_range,
                        "near_price": near_price,
                        "gap_size_percent": gap_size_percent,
                        "gap_distance_start_percent": gap_distance_start_percent,
                        "gap_distance_end_percent": gap_distance_end_percent
                    })

            if gaps:
                results[ticker] = {"gaps": gaps, "current_price": current_price}

        except Exception as e:
            tqdm.write(f"⚠️ Error fetching {ticker}: {e}")

        if (index + 0) % sleep_after == 0:
            time.sleep(sleep_time)

    return results

# ======================================================
# 💾 Save to CSV
# ======================================================
import os

def save_to_csv(results, filename="gaps_results.csv"):
    # Create folder if it doesn't exist
    folder = "Gaps Result"
    os.makedirs(folder, exist_ok=True)
    
    # Full file path inside folder
    filepath = os.path.join(folder, filename)

    rows = []
    for ticker, data in results.items():
        clean_ticker = ticker.replace(".NS", "")
        for gap in data['gaps']:
            rows.append({
                "Stock": clean_ticker,
                "Current Price": f"{data['current_price']:.2f}",
                "Date": gap['date'].strftime("%d-%m-%y"),
                "Gap Type": gap['gap_type'],
                "Gap Size": f"{gap['gap_size']:.2f}",
                "Gap Range": f"({gap['gap_range'][0]:.2f}, {gap['gap_range'][1]:.2f})",
                "Near Price": str(gap['near_price']).upper(),
                "Gap Size %": f"{gap['gap_size_percent']:.2f}",
                "Gap Dist Start %": f"{gap['gap_distance_start_percent']:.2f}",
                "Gap Dist End %": f"{gap['gap_distance_end_percent']:.2f}"
            })

    df = pd.DataFrame(rows, columns=[
        "Stock", "Current Price", "Date", "Gap Type", "Gap Size", "Gap Range",
        "Near Price", "Gap Size %", "Gap Dist Start %", "Gap Dist End %"
    ])

    # Save inside folder
    df.to_csv(filepath, index=False)
    print(f"✅ CSV saved: {filepath}")

# ======================================================
# 📋 Pretty Table Output
# ======================================================
def print_pretty_results(results):
    for ticker, data in results.items():
        print(f"\nStock: {ticker}, Current Price: {data['current_price']:.2f}")
        table = PrettyTable()
        table.field_names = [
            "Date", "Gap Type", "Gap Size", "Gap Range", "Near Price",
            "Gap Size %", "Gap Dist Start %", "Gap Dist End %"
        ]
        for gap in data['gaps']:
            table.add_row([
                gap['date'], gap['gap_type'], f"{gap['gap_size']:.2f}",
                f"({gap['gap_range'][0]:.2f}, {gap['gap_range'][1]:.2f})",
                gap['near_price'],
                f"{gap['gap_size_percent']:.2f}",
                f"{gap['gap_distance_start_percent']:.2f}",
                f"{gap['gap_distance_end_percent']:.2f}"
            ])
        print(table)

# ======================================================
# 🔹 MAIN EXECUTION
# ======================================================
if __name__ == "__main__":
    gaps = detect_gaps(tickers)
    save_to_csv(gaps, filename=f"gaps_{selected_group}.csv")
    # print_pretty_results(gaps)
