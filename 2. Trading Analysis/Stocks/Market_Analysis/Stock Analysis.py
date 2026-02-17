from datetime import datetime

from market_analyzer import stock_data_manager, ltp_near_gaps, support_resistance, candle_gap_analysis, fibonacci_levels, chart_patterns

# ======================================================
# ⚙️ CENTRAL PROJECT CONFIGURATION (EDIT HERE)
# ======================================================

# 🗂 STOCK GROUPS
INDICES         = ["^NSEI", "^NSEBANK", "^CNXIT", "^CNXAUTO", "^CNXFMCG", "^CNXMETAL", "^CNXPHARMA", "^CNXREALTY", "^CNXENERGY", "^CNXMEDIA"]

NIFTY_TOP_10    = ["HDFCBANK.NS", "ICICIBANK.NS", "RELIANCE.NS", "INFY.NS", "BHARTIARTL.NS", "LT.NS", "SBIN.NS", "AXISBANK.NS", "TCS.NS", "M&M.NS"]

NIFTY_50        = ["ADANIENT.NS", "ADANIPORTS.NS", "APOLLOHOSP.NS", "ASIANPAINT.NS", "AXISBANK.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BEL.NS", "BHARTIARTL.NS", "CIPLA.NS", "COALINDIA.NS", "DRREDDY.NS", "EICHERMOT.NS", "ETERNAL.NS", "GRASIM.NS", "HCLTECH.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HINDALCO.NS", "HINDUNILVR.NS", "ICICIBANK.NS", "ITC.NS", "INFY.NS", "INDIGO.NS", "JSWSTEEL.NS", "JIOFIN.NS", "KOTAKBANK.NS", "LT.NS", "M&M.NS", "MARUTI.NS", "MAXHEALTH.NS", "NTPC.NS", "NESTLEIND.NS", "ONGC.NS", "POWERGRID.NS", "RELIANCE.NS", "SBILIFE.NS", "SHRIRAMFIN.NS", "SBIN.NS", "SUNPHARMA.NS", "TCS.NS", "TATACONSUM.NS", "TMPV.NS", "TATASTEEL.NS", "TECHM.NS", "TITAN.NS", "TRENT.NS", "ULTRACEMCO.NS", "WIPRO.NS"]

FN_O_STOCKS     = ["360ONE.NS", "ABB.NS", "APLAPOLLO.NS", "AUBANK.NS", "ADANIENSOL.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ABCAPITAL.NS", "ALKEM.NS", "AMBER.NS", "AMBUJACEM.NS", "ANGELONE.NS", "APOLLOHOSP.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTRAL.NS", "AUROPHARMA.NS", "DMART.NS", "AXISBANK.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHEL.NS", "BPCL.NS", "BHARTIARTL.NS", "BIOCON.NS", "BLUESTARCO.NS", "BOSCHLTD.NS", "BRITANNIA.NS", "CGPOWER.NS", "CANBK.NS", "CDSL.NS", "CHOLAFIN.NS", "CIPLA.NS", "COALINDIA.NS", "COFORGE.NS", "COLPAL.NS", "CAMS.NS", "CONCOR.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DLF.NS", "DABUR.NS", "DALBHARAT.NS", "DELHIVERY.NS", "DIVISLAB.NS", "DIXON.NS", "DRREDDY.NS", "ETERNAL.NS", "EICHERMOT.NS", "EXIDEIND.NS", "NYKAA.NS", "FORTIS.NS", "GAIL.NS", "GMRAIRPORT.NS", "GLENMARK.NS", "GODREJCP.NS", "GODREJPROP.NS", "GRASIM.NS", "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HFCL.NS", "HAVELLS.NS", "HEROMOTOCO.NS", "HINDALCO.NS", "HAL.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "POWERINDIA.NS", "HUDCO.NS", "ICICIBANK.NS", "ICICIGI.NS", "ICICIPRULI.NS", "IDFCFIRSTB.NS", "IIFL.NS", "ITC.NS", "INDIANB.NS", "IEX.NS", "IOC.NS", "IRCTC.NS", "IRFC.NS", "IREDA.NS", "IGL.NS", "INDUSTOWER.NS", "INDUSINDBK.NS", "NAUKRI.NS", "INFY.NS", "INOXWIND.NS", "INDIGO.NS", "JINDALSTEL.NS", "JSWENERGY.NS", "JSWSTEEL.NS", "JIOFIN.NS", "JUBLFOOD.NS", "KEI.NS", "KPITTECH.NS", "KALYANKJIL.NS", "KAYNES.NS", "KFINTECH.NS", "KOTAKBANK.NS", "LTF.NS", "LICHSGFIN.NS", "LTIM.NS", "LT.NS", "LAURUSLABS.NS", "LICI.NS", "LODHA.NS", "LUPIN.NS", "M&M.NS", "MANAPPURAM.NS", "MANKIND.NS", "MARICO.NS", "MARUTI.NS", "MFSL.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NBCC.NS", "NCC.NS", "NHPC.NS", "NMDC.NS", "NTPC.NS", "NATIONALUM.NS", "NESTLEIND.NS", "NUVAMA.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "PAYTM.NS", "OFSS.NS", "POLICYBZR.NS", "PGEL.NS", "PIIND.NS", "PNBHOUSING.NS", "PAGEIND.NS", "PATANJALI.NS", "PERSISTENT.NS", "PETRONET.NS", "PIDILITIND.NS", "PPLPHARMA.NS", "POLYCAB.NS", "PFC.NS", "POWERGRID.NS", "PRESTIGE.NS", "PNB.NS", "RBLBANK.NS", "RECLTD.NS", "RVNL.NS", "RELIANCE.NS", "SBICARD.NS", "SBILIFE.NS", "SHREECEM.NS", "SRF.NS", "SAMMAANCAP.NS", "MOTHERSON.NS", "SHRIRAMFIN.NS", "SIEMENS.NS", "SOLARINDS.NS", "SONACOMS.NS", "SBIN.NS", "SAIL.NS", "SUNPHARMA.NS", "SUPREMEIND.NS", "SUZLON.NS", "SYNGENE.NS", "TATACONSUM.NS", "TITAGARH.NS", "TVSMOTOR.NS", "TCS.NS", "TATAELXSI.NS", "TMPV.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TATATECH.NS", "TECHM.NS", "FEDERALBNK.NS", "INDHOTEL.NS", "PHOENIXLTD.NS", "TITAN.NS", "TORNTPHARM.NS", "TORNTPOWER.NS", "TRENT.NS", "TIINDIA.NS", "UNOMINDA.NS", "UPL.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UNITDSPR.NS", "VBL.NS", "VEDL.NS", "IDEA.NS", "VOLTAS.NS", "WIPRO.NS", "YESBANK.NS", "ZYDUSLIFE.NS"]

NIFTY_500       = ["360ONE.NS", "3MINDIA.NS", "ABB.NS", "ACC.NS", "ACMESOLAR.NS", "AIAENG.NS", "APLAPOLLO.NS", "AUBANK.NS", "AWL.NS", "AADHARHFC.NS", "AARTIIND.NS", "AAVAS.NS", "ABBOTINDIA.NS", "ACE.NS", "ADANIENSOL.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ADANIPOWER.NS", "ATGL.NS", "ABCAPITAL.NS", "ABFRL.NS", "ABLBL.NS", "ABREL.NS", "ABSLAMC.NS", "AEGISLOG.NS", "AEGISVOPAK.NS", "AFCONS.NS", "AFFLE.NS", "AJANTPHARM.NS", "AKUMS.NS", "AKZOINDIA.NS", "APLLTD.NS", "ALKEM.NS", "ALKYLAMINE.NS", "ALOKINDS.NS", "ARE&M.NS", "AMBER.NS", "AMBUJACEM.NS", "ANANDRATHI.NS", "ANANTRAJ.NS", "ANGELONE.NS", "APARINDS.NS", "APOLLOHOSP.NS", "APOLLOTYRE.NS", "APTUS.NS", "ASAHIINDIA.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTERDM.NS", "ASTRAZEN.NS", "ASTRAL.NS", "ATHERENERG.NS", "ATUL.NS", "AUROPHARMA.NS", "AIIL.NS", "DMART.NS", "AXISBANK.NS", "BASF.NS", "BEML.NS", "BLS.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BAJAJHLDNG.NS", "BAJAJHFL.NS", "BALKRISIND.NS", "BALRAMCHIN.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS", "MAHABANK.NS", "BATAINDIA.NS", "BAYERCROP.NS", "BERGEPAINT.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHEL.NS", "BPCL.NS", "BHARTIARTL.NS", "BHARTIHEXA.NS", "BIKAJI.NS", "BIOCON.NS", "BSOFT.NS", "BLUEDART.NS", "BLUEJET.NS", "BLUESTARCO.NS", "BBTC.NS", "BOSCHLTD.NS", "FIRSTCRY.NS", "BRIGADE.NS", "BRITANNIA.NS", "MAPMYINDIA.NS", "CCL.NS", "CESC.NS", "CGPOWER.NS", "CRISIL.NS", "CAMPUS.NS", "CANFINHOME.NS", "CANBK.NS", "CAPLIPOINT.NS", "CGCL.NS", "CARBORUNIV.NS", "CASTROLIND.NS", "CEATLTD.NS", "CENTRALBK.NS", "CDSL.NS", "CENTURYPLY.NS", "CERA.NS", "CHALET.NS", "CHAMBLFERT.NS", "CHENNPETRO.NS", "CHOICEIN.NS", "CHOLAHLDNG.NS", "CHOLAFIN.NS", "CIPLA.NS", "CUB.NS", "CLEAN.NS", "COALINDIA.NS", "COCHINSHIP.NS", "COFORGE.NS", "COHANCE.NS", "COLPAL.NS", "CAMS.NS", "CONCORDBIO.NS", "CONCOR.NS", "COROMANDEL.NS", "CRAFTSMAN.NS", "CREDITACC.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DCMSHRIRAM.NS", "DLF.NS", "DOMS.NS", "DABUR.NS", "DALBHARAT.NS", "DATAPATTNS.NS", "DEEPAKFERT.NS", "DEEPAKNTR.NS", "DELHIVERY.NS", "DEVYANI.NS", "DIVISLAB.NS", "DIXON.NS", "AGARWALEYE.NS", "LALPATHLAB.NS", "DRREDDY.NS", "EIDPARRY.NS", "EIHOTEL.NS", "EICHERMOT.NS", "ELECON.NS", "ELGIEQUIP.NS", "EMAMILTD.NS", "EMCURE.NS", "ENDURANCE.NS", "ENGINERSIN.NS", "ERIS.NS", "ESCORTS.NS", "ETERNAL.NS", "EXIDEIND.NS", "NYKAA.NS", "FEDERALBNK.NS", "FACT.NS", "FINCABLES.NS", "FINPIPE.NS", "FSL.NS", "FIVESTAR.NS", "FORCEMOT.NS", "FORTIS.NS", "GAIL.NS", "GVT&D.NS", "GMRAIRPORT.NS", "GRSE.NS", "GICRE.NS", "GILLETTE.NS", "GLAND.NS", "GLAXO.NS", "GLENMARK.NS", "MEDANTA.NS", "GODIGIT.NS", "GPIL.NS", "GODFRYPHLP.NS", "GODREJAGRO.NS", "GODREJCP.NS", "GODREJIND.NS", "GODREJPROP.NS", "GRANULES.NS", "GRAPHITE.NS", "GRASIM.NS", "GRAVITA.NS", "GESHIP.NS", "FLUOROCHEM.NS", "GUJGASLTD.NS", "GMDCLTD.NS", "GSPL.NS", "HEG.NS", "HBLENGINE.NS", "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HFCL.NS", "HAPPSTMNDS.NS", "HAVELLS.NS", "HEROMOTOCO.NS", "HEXT.NS", "HSCL.NS", "HINDALCO.NS", "HAL.NS", "HINDCOPPER.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "POWERINDIA.NS", "HOMEFIRST.NS", "HONASA.NS", "HONAUT.NS", "HUDCO.NS", "HYUNDAI.NS", "ICICIBANK.NS", "ICICIGI.NS", "ICICIPRULI.NS", "IDBI.NS", "IDFCFIRSTB.NS", "IFCI.NS", "IIFL.NS", "INOXINDIA.NS", "IRB.NS", "IRCON.NS", "ITCHOTELS.NS", "ITC.NS", "ITI.NS", "INDGN.NS", "INDIACEM.NS", "INDIAMART.NS", "INDIANB.NS", "IEX.NS", "INDHOTEL.NS", "IOC.NS", "IOB.NS", "IRCTC.NS", "IRFC.NS", "IREDA.NS", "IGL.NS", "INDUSTOWER.NS", "INDUSINDBK.NS", "NAUKRI.NS", "INFY.NS", "INOXWIND.NS", "INTELLECT.NS", "INDIGO.NS", "IGIL.NS", "IKS.NS", "IPCALAB.NS", "JBCHEPHARM.NS", "JKCEMENT.NS", "JBMA.NS", "JKTYRE.NS", "JMFINANCIL.NS", "JSWCEMENT.NS", "JSWENERGY.NS", "JSWINFRA.NS", "JSWSTEEL.NS", "JPPOWER.NS", "J&KBANK.NS", "JINDALSAW.NS", "JSL.NS", "JINDALSTEL.NS", "JIOFIN.NS", "JUBLFOOD.NS", "JUBLINGREA.NS", "JUBLPHARMA.NS", "JWL.NS", "JYOTHYLAB.NS", "JYOTICNC.NS", "KPRMILL.NS", "KEI.NS", "KPITTECH.NS", "KSB.NS", "KAJARIACER.NS", "KPIL.NS", "KALYANKJIL.NS", "KARURVYSYA.NS", "KAYNES.NS", "KEC.NS", "KFINTECH.NS", "KIRLOSBROS.NS", "KIRLOSENG.NS", "KOTAKBANK.NS", "KIMS.NS", "LTF.NS", "LTTS.NS", "LICHSGFIN.NS", "LTFOODS.NS", "LTIM.NS", "LT.NS", "LATENTVIEW.NS", "LAURUSLABS.NS", "THELEELA.NS", "LEMONTREE.NS", "LICI.NS", "LINDEINDIA.NS", "LLOYDSME.NS", "LODHA.NS", "LUPIN.NS", "MMTC.NS", "MRF.NS", "MGL.NS", "MAHSCOOTER.NS", "MAHSEAMLES.NS", "M&MFIN.NS", "M&M.NS", "MANAPPURAM.NS", "MRPL.NS", "MANKIND.NS", "MARICO.NS", "MARUTI.NS", "MFSL.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "METROPOLIS.NS", "MINDACORP.NS", "MSUMI.NS", "MOTILALOFS.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NATCOPHARM.NS", "NBCC.NS", "NCC.NS", "NHPC.NS", "NLCINDIA.NS", "NMDC.NS", "NSLNISP.NS", "NTPCGREEN.NS", "NTPC.NS", "NH.NS", "NATIONALUM.NS", "NAVA.NS", "NAVINFLUOR.NS", "NESTLEIND.NS", "NETWEB.NS", "NEULANDLAB.NS", "NEWGEN.NS", "NAM-INDIA.NS", "NIVABUPA.NS", "NUVAMA.NS", "NUVOCO.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "OLAELEC.NS", "OLECTRA.NS", "PAYTM.NS", "ONESOURCE.NS", "OFSS.NS", "POLICYBZR.NS", "PCBL.NS", "PGEL.NS", "PIIND.NS", "PNBHOUSING.NS", "PTCIL.NS", "PVRINOX.NS", "PAGEIND.NS", "PATANJALI.NS", "PERSISTENT.NS", "PETRONET.NS", "PFIZER.NS", "PHOENIXLTD.NS", "PIDILITIND.NS", "PPLPHARMA.NS", "POLYMED.NS", "POLYCAB.NS", "POONAWALLA.NS", "PFC.NS", "POWERGRID.NS", "PRAJIND.NS", "PREMIERENE.NS", "PRESTIGE.NS", "PGHH.NS", "PNB.NS", "RRKABEL.NS", "RBLBANK.NS", "RECLTD.NS", "RHIM.NS", "RITES.NS", "RADICO.NS", "RVNL.NS", "RAILTEL.NS", "RAINBOW.NS", "RKFORGE.NS", "RCF.NS", "REDINGTON.NS", "RELIANCE.NS", "RELINFRA.NS", "RPOWER.NS", "SBFC.NS", "SBICARD.NS", "SBILIFE.NS", "SJVN.NS", "SRF.NS", "SAGILITY.NS", "SAILIFE.NS", "SAMMAANCAP.NS", "MOTHERSON.NS", "SAPPHIRE.NS", "SARDAEN.NS", "SAREGAMA.NS", "SCHAEFFLER.NS", "SCHNEIDER.NS", "SCI.NS", "SHREECEM.NS", "SHRIRAMFIN.NS", "SHYAMMETL.NS", "ENRIN.NS", "SIEMENS.NS", "SIGNATURE.NS", "SOBHA.NS", "SOLARINDS.NS", "SONACOMS.NS", "SONATSOFTW.NS", "STARHEALTH.NS", "SBIN.NS", "SAIL.NS", "SUMICHEM.NS", "SUNPHARMA.NS", "SUNTV.NS", "SUNDARMFIN.NS", "SUNDRMFAST.NS", "SUPREMEIND.NS", "SUZLON.NS", "SWANCORP.NS", "SWIGGY.NS", "SYNGENE.NS", "SYRMA.NS", "TBOTEK.NS", "TVSMOTOR.NS", "TATACHEM.NS", "TATACOMM.NS", "TCS.NS", "TATACONSUM.NS", "TATAELXSI.NS", "TATAINVEST.NS", "TMPV.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TATATECH.NS", "TTML.NS", "TECHM.NS", "TECHNOE.NS", "TEJASNET.NS", "NIACL.NS", "RAMCOCEM.NS", "THERMAX.NS", "TIMKEN.NS", "TITAGARH.NS", "TITAN.NS", "TORNTPHARM.NS", "TORNTPOWER.NS", "TARIL.NS", "TRENT.NS", "TRIDENT.NS", "TRIVENI.NS", "TRITURBINE.NS", "TIINDIA.NS", "UCOBANK.NS", "UNOMINDA.NS", "UPL.NS", "UTIAMC.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UBL.NS", "UNITDSPR.NS", "USHAMART.NS", "VGUARD.NS", "DBREALTY.NS", "VTL.NS", "VBL.NS", "MANYAVAR.NS", "VEDL.NS", "VENTIVE.NS", "VIJAYA.NS", "VMM.NS", "IDEA.NS", "VOLTAS.NS", "WAAREEENER.NS", "WELCORP.NS", "WELSPUNLIV.NS", "WHIRLPOOL.NS", "WIPRO.NS", "WOCKPHARMA.NS", "YESBANK.NS", "ZFCVINDIA.NS", "ZEEL.NS", "ZENTEC.NS", "ZENSARTECH.NS", "ZYDUSLIFE.NS", "ECLERX.NS"]

GROUPS = {
    "indices"       : INDICES,
    "nifty_top_10"  : NIFTY_TOP_10,
    "nifty_50"      : NIFTY_50,
    "fn_o_stocks"   : FN_O_STOCKS,
    "nifty_500"     : NIFTY_500
}

# 🛠 GLOBAL RUN SETTINGS
DEFAULT_TICKERS       = NIFTY_500           # Ticker list to use by default
PERIOD_LONG           = "1y"                # Period for SR, Fib, Patterns # Data period        "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y","5y", "10y", "ytd", "max"
PERIOD_SHORT          = "1mo"               # Period for Candle Analysis

INTERVAL              = "1d"                # Candle interval               # Candle interval    "1m", "2m", "5m", "15m", "30m", "60m"/"1h", "90m",      "1d", "5d", "1wk", "1mo", "3mo"

# 💾 DATA CACHE SETTINGS
CACHE_EXPIRY_HOURS    = 4
BATCH_SIZE            = 50
BATCH_DELAY           = 1.2

# 📊 MODULE SPECIFIC SETTINGS

# Candle & Gap Analysis
CANDLE_SAVE_RAW       = False
CANDLE_SAVE_PLOTS     = False
CANDLE_PRINT_REPORT   = False

# LTP Near Gaps
GAPS_NEAR_TOLERANCE   = 0.03          # 0.03 ±3% near price zone
GAPS_MIN_GAP_PERCENT  = 1.0           # 1.0 Only consider gaps ≥ 1% in size
GAPS_ONLY_NEAR        = True          # True = only gaps near current price; False = include all

# Support & Resistance
SR_SENSITIVITY        = 1.0
SR_TOLERANCE          = 0.005
SR_SAVE_CHARTS        = True          # Set True to save charts automatically
SR_DIST_RANGE         = 0.3           # DIST_% ±0.3%

# Fibonacci Levels
FIB_NEAR_TOLERANCE    = 0.01                                        # 1% distance from a Fibonacci level
FIB_LEVELS            = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]   # Fibonacci Levels (Edit here)
FIB_CHART_LEVELS      = [0.5, 0.618]                                # Charts will ONLY be generated for these levels
FIB_PRINT_LEVELS      = ["50.0%", "61.8%"]                          # Print ONLY for these levels

# Chart Patterns
PATTERNS_TOLERANCE_PCT    = 0.012       # Stricter 1.2% tolerance for pattern similarity
PATTERNS_NEAR_TOLERANCE   = 0.02        # 2% distance from entry for "Opportunity"
PATTERNS_PIVOT_WINDOW     = 10          # Increased for "perfect" large patterns
PATTERNS_ONLY_OPPS        = True        # True - for "OPPORTUNITY"chart only save, False - for All stocks charts save 

def main():
    print(f"🚀 Starting Combined Analysis at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
    
    # Use current settings
    tickers = DEFAULT_TICKERS
    
    # 1. Fetch Data (Checks cache first)
    print("\n1️⃣  Fetching Stock Data (Local Cache or Fresh Download)...")
    
    # Update cache settings dynamically
    stock_data_manager.CACHE_EXPIRY_HOURS = CACHE_EXPIRY_HOURS
    stock_data_manager.BATCH_SIZE = BATCH_SIZE
    stock_data_manager.BATCH_DELAY = BATCH_DELAY

    # Fetching long period data (1y)
    data_long = stock_data_manager.get_data(tickers, period=PERIOD_LONG, interval=INTERVAL)
    # Fetching short period data (1mo)
    data_short = stock_data_manager.get_data(tickers, period=PERIOD_SHORT, interval=INTERVAL)

    if not data_long:
        print("❌ No data fetched. Exiting.")
        return

    # 2. Run LTP Near Gaps
    print("\n2️⃣  Running LTP Near Gaps Analysis...")
    try:
        ltp_near_gaps.run(
            data_dict=data_long, 
            near_tolerance=GAPS_NEAR_TOLERANCE, 
            min_gap_percent=GAPS_MIN_GAP_PERCENT, 
            only_near=GAPS_ONLY_NEAR,
            out_filename="gaps_detailed_results.csv"
        )
    except Exception as e:
        print(f"❌ Error running LTP Near Gaps: {e}")

    # 3. Run Support and Resistance
    print("\n3️⃣  Running Support & Resistance Analysis...")
    try:
        support_resistance.run_fractal_sr(
            tickers, 
            data_dict=data_long,
            sensitivity=SR_SENSITIVITY,
            tolerance=SR_TOLERANCE,
            save_charts=SR_SAVE_CHARTS,
            dist_range=SR_DIST_RANGE,
            out_dir='outputs/Support_Resistance'
        )
    except Exception as e:
        print(f"❌ Error running Support and Resistance: {e}")

    # 4. Candle & Gap Analysis
    print("\n4️⃣  Running Candle & Gap Analysis...")
    try:
        candle_gap_analysis.run(
            data_dict=data_short,
            save_raw=CANDLE_SAVE_RAW,
            save_plots=CANDLE_SAVE_PLOTS,
            print_enabled=CANDLE_PRINT_REPORT
        )
    except Exception as e:
        print(f"❌ Error running Candle & Gap Analysis: {e}")

    # 5. Fibonacci Levels Analysis
    print("\n5️⃣  Running Fibonacci Levels Analysis...")
    try:
        fibonacci_levels.run_analysis(
            tickers, 
            data_dict=data_long,
            near_tolerance=FIB_NEAR_TOLERANCE,
            fib_levels=FIB_LEVELS,
            chart_levels=FIB_CHART_LEVELS,
            print_levels=FIB_PRINT_LEVELS,
            out_dir="outputs/Fibonacci"
        )
    except Exception as e:
        print(f"❌ Error running Fibonacci Analysis: {e}")

    # 6. Chart Pattern Detection
    print("\n6️⃣  Running Chart Pattern Detection...")
    try:
        chart_patterns.run_analysis(
            tickers, 
            data_dict=data_long,
            tolerance_pct=PATTERNS_TOLERANCE_PCT,
            near_price_tolerance=PATTERNS_NEAR_TOLERANCE,
            pivot_window=PATTERNS_PIVOT_WINDOW,
            plot_only_opps=PATTERNS_ONLY_OPPS,
            out_dir="outputs/Chart_Patterns"
        )
    except Exception as e:
        print(f"❌ Error running Chart Pattern Detection: {e}")

    print("\n✅ All Analyses Completed Successfully!")

if __name__ == "__main__":
    main()
