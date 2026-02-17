import asyncio
import os
from market_analyzer.smc_analysis import main as main_analysis
from market_analyzer.smc_screener import main as main_screener

async def run_full_pipeline():
    # Create analysis directory
    os.makedirs("analysis", exist_ok=True)

    # Define stock code lists for each option
    # 🗂 STOCK GROUPS
    INDICES         = ["^NSEI", "^NSEBANK", "^CNXIT", "^CNXAUTO", "^CNXFMCG", "^CNXMETAL", "^CNXPHARMA", "^CNXREALTY", "^CNXENERGY", "^CNXMEDIA"]

    NIFTY_TOP_10    = ["HDFCBANK.NS", "ICICIBANK.NS", "RELIANCE.NS", "INFY.NS", "BHARTIARTL.NS", "LT.NS", "SBIN.NS", "AXISBANK.NS", "TCS.NS", "M&M.NS"]

    NIFTY_50        = ["ADANIENT.NS", "ADANIPORTS.NS", "APOLLOHOSP.NS", "ASIANPAINT.NS", "AXISBANK.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BEL.NS", "BHARTIARTL.NS", "CIPLA.NS", "COALINDIA.NS", "DRREDDY.NS", "EICHERMOT.NS", "ETERNAL.NS", "GRASIM.NS", "HCLTECH.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HINDALCO.NS", "HINDUNILVR.NS", "ICICIBANK.NS", "ITC.NS", "INFY.NS", "INDIGO.NS", "JSWSTEEL.NS", "JIOFIN.NS", "KOTAKBANK.NS", "LT.NS", "M&M.NS", "MARUTI.NS", "MAXHEALTH.NS", "NTPC.NS", "NESTLEIND.NS", "ONGC.NS", "POWERGRID.NS", "RELIANCE.NS", "SBILIFE.NS", "SHRIRAMFIN.NS", "SBIN.NS", "SUNPHARMA.NS", "TCS.NS", "TATACONSUM.NS", "TMPV.NS", "TATASTEEL.NS", "TECHM.NS", "TITAN.NS", "TRENT.NS", "ULTRACEMCO.NS", "WIPRO.NS"]

    FN_O_STOCKS     = ["360ONE.NS", "ABB.NS", "APLAPOLLO.NS", "AUBANK.NS", "ADANIENSOL.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ABCAPITAL.NS", "ALKEM.NS", "AMBER.NS", "AMBUJACEM.NS", "ANGELONE.NS", "APOLLOHOSP.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTRAL.NS", "AUROPHARMA.NS", "DMART.NS", "AXISBANK.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHEL.NS", "BPCL.NS", "BHARTIARTL.NS", "BIOCON.NS", "BLUESTARCO.NS", "BOSCHLTD.NS", "BRITANNIA.NS", "CGPOWER.NS", "CANBK.NS", "CDSL.NS", "CHOLAFIN.NS", "CIPLA.NS", "COALINDIA.NS", "COFORGE.NS", "COLPAL.NS", "CAMS.NS", "CONCOR.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DLF.NS", "DABUR.NS", "DALBHARAT.NS", "DELHIVERY.NS", "DIVISLAB.NS", "DIXON.NS", "DRREDDY.NS", "ETERNAL.NS", "EICHERMOT.NS", "EXIDEIND.NS", "NYKAA.NS", "FORTIS.NS", "GAIL.NS", "GMRAIRPORT.NS", "GLENMARK.NS", "GODREJCP.NS", "GODREJPROP.NS", "GRASIM.NS", "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HFCL.NS", "HAVELLS.NS", "HEROMOTOCO.NS", "HINDALCO.NS", "HAL.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "POWERINDIA.NS", "HUDCO.NS", "ICICIBANK.NS", "ICICIGI.NS", "ICICIPRULI.NS", "IDFCFIRSTB.NS", "IIFL.NS", "ITC.NS", "INDIANB.NS", "IEX.NS", "IOC.NS", "IRCTC.NS", "IRFC.NS", "IREDA.NS", "IGL.NS", "INDUSTOWER.NS", "INDUSINDBK.NS", "NAUKRI.NS", "INFY.NS", "INOXWIND.NS", "INDIGO.NS", "JINDALSTEL.NS", "JSWENERGY.NS", "JSWSTEEL.NS", "JIOFIN.NS", "JUBLFOOD.NS", "KEI.NS", "KPITTECH.NS", "KALYANKJIL.NS", "KAYNES.NS", "KFINTECH.NS", "KOTAKBANK.NS", "LTF.NS", "LICHSGFIN.NS", "LTIM.NS", "LT.NS", "LAURUSLABS.NS", "LICI.NS", "LODHA.NS", "LUPIN.NS", "M&M.NS", "MANAPPURAM.NS", "MANKIND.NS", "MARICO.NS", "MARUTI.NS", "MFSL.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NBCC.NS", "NCC.NS", "NHPC.NS", "NMDC.NS", "NTPC.NS", "NATIONALUM.NS", "NESTLEIND.NS", "NUVAMA.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "PAYTM.NS", "OFSS.NS", "POLICYBZR.NS", "PGEL.NS", "PIIND.NS", "PNBHOUSING.NS", "PAGEIND.NS", "PATANJALI.NS", "PERSISTENT.NS", "PETRONET.NS", "PIDILITIND.NS", "PPLPHARMA.NS", "POLYCAB.NS", "PFC.NS", "POWERGRID.NS", "PRESTIGE.NS", "PNB.NS", "RBLBANK.NS", "RECLTD.NS", "RVNL.NS", "RELIANCE.NS", "SBICARD.NS", "SBILIFE.NS", "SHREECEM.NS", "SRF.NS", "SAMMAANCAP.NS", "MOTHERSON.NS", "SHRIRAMFIN.NS", "SIEMENS.NS", "SOLARINDS.NS", "SONACOMS.NS", "SBIN.NS", "SAIL.NS", "SUNPHARMA.NS", "SUPREMEIND.NS", "SUZLON.NS", "SYNGENE.NS", "TATACONSUM.NS", "TITAGARH.NS", "TVSMOTOR.NS", "TCS.NS", "TATAELXSI.NS", "TMPV.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TATATECH.NS", "TECHM.NS", "FEDERALBNK.NS", "INDHOTEL.NS", "PHOENIXLTD.NS", "TITAN.NS", "TORNTPHARM.NS", "TORNTPOWER.NS", "TRENT.NS", "TIINDIA.NS", "UNOMINDA.NS", "UPL.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UNITDSPR.NS", "VBL.NS", "VEDL.NS", "IDEA.NS", "VOLTAS.NS", "WIPRO.NS", "YESBANK.NS", "ZYDUSLIFE.NS"]

    NIFTY_500       = ["360ONE.NS", "3MINDIA.NS", "ABB.NS", "ACC.NS", "ACMESOLAR.NS", "AIAENG.NS", "APLAPOLLO.NS", "AUBANK.NS", "AWL.NS", "AADHARHFC.NS", "AARTIIND.NS", "AAVAS.NS", "ABBOTINDIA.NS", "ACE.NS", "ADANIENSOL.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ADANIPOWER.NS", "ATGL.NS", "ABCAPITAL.NS", "ABFRL.NS", "ABLBL.NS", "ABREL.NS", "ABSLAMC.NS", "AEGISLOG.NS", "AEGISVOPAK.NS", "AFCONS.NS", "AFFLE.NS", "AJANTPHARM.NS", "AKUMS.NS", "AKZOINDIA.NS", "APLLTD.NS", "ALKEM.NS", "ALKYLAMINE.NS", "ALOKINDS.NS", "ARE&M.NS", "AMBER.NS", "AMBUJACEM.NS", "ANANDRATHI.NS", "ANANTRAJ.NS", "ANGELONE.NS", "APARINDS.NS", "APOLLOHOSP.NS", "APOLLOTYRE.NS", "APTUS.NS", "ASAHIINDIA.NS", "ASHOKLEY.NS", "ASIANPAINT.NS", "ASTERDM.NS", "ASTRAZEN.NS", "ASTRAL.NS", "ATHERENERG.NS", "ATUL.NS", "AUROPHARMA.NS", "AIIL.NS", "DMART.NS", "AXISBANK.NS", "BASF.NS", "BEML.NS", "BLS.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJFINANCE.NS", "BAJAJFINSV.NS", "BAJAJHLDNG.NS", "BAJAJHFL.NS", "BALKRISIND.NS", "BALRAMCHIN.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS", "MAHABANK.NS", "BATAINDIA.NS", "BAYERCROP.NS", "BERGEPAINT.NS", "BDL.NS", "BEL.NS", "BHARATFORG.NS", "BHEL.NS", "BPCL.NS", "BHARTIARTL.NS", "BHARTIHEXA.NS", "BIKAJI.NS", "BIOCON.NS", "BSOFT.NS", "BLUEDART.NS", "BLUEJET.NS", "BLUESTARCO.NS", "BBTC.NS", "BOSCHLTD.NS", "FIRSTCRY.NS", "BRIGADE.NS", "BRITANNIA.NS", "MAPMYINDIA.NS", "CCL.NS", "CESC.NS", "CGPOWER.NS", "CRISIL.NS", "CAMPUS.NS", "CANFINHOME.NS", "CANBK.NS", "CAPLIPOINT.NS", "CGCL.NS", "CARBORUNIV.NS", "CASTROLIND.NS", "CEATLTD.NS", "CENTRALBK.NS", "CDSL.NS", "CENTURYPLY.NS", "CERA.NS", "CHALET.NS", "CHAMBLFERT.NS", "CHENNPETRO.NS", "CHOICEIN.NS", "CHOLAHLDNG.NS", "CHOLAFIN.NS", "CIPLA.NS", "CUB.NS", "CLEAN.NS", "COALINDIA.NS", "COCHINSHIP.NS", "COFORGE.NS", "COHANCE.NS", "COLPAL.NS", "CAMS.NS", "CONCORDBIO.NS", "CONCOR.NS", "COROMANDEL.NS", "CRAFTSMAN.NS", "CREDITACC.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DCMSHRIRAM.NS", "DLF.NS", "DOMS.NS", "DABUR.NS", "DALBHARAT.NS", "DATAPATTNS.NS", "DEEPAKFERT.NS", "DEEPAKNTR.NS", "DELHIVERY.NS", "DEVYANI.NS", "DIVISLAB.NS", "DIXON.NS", "AGARWALEYE.NS", "LALPATHLAB.NS", "DRREDDY.NS", "EIDPARRY.NS", "EIHOTEL.NS", "EICHERMOT.NS", "ELECON.NS", "ELGIEQUIP.NS", "EMAMILTD.NS", "EMCURE.NS", "ENDURANCE.NS", "ENGINERSIN.NS", "ERIS.NS", "ESCORTS.NS", "ETERNAL.NS", "EXIDEIND.NS", "NYKAA.NS", "FEDERALBNK.NS", "FACT.NS", "FINCABLES.NS", "FINPIPE.NS", "FSL.NS", "FIVESTAR.NS", "FORCEMOT.NS", "FORTIS.NS", "GAIL.NS", "GVT&D.NS", "GMRAIRPORT.NS", "GRSE.NS", "GICRE.NS", "GILLETTE.NS", "GLAND.NS", "GLAXO.NS", "GLENMARK.NS", "MEDANTA.NS", "GODIGIT.NS", "GPIL.NS", "GODFRYPHLP.NS", "GODREJAGRO.NS", "GODREJCP.NS", "GODREJIND.NS", "GODREJPROP.NS", "GRANULES.NS", "GRAPHITE.NS", "GRASIM.NS", "GRAVITA.NS", "GESHIP.NS", "FLUOROCHEM.NS", "GUJGASLTD.NS", "GMDCLTD.NS", "GSPL.NS", "HEG.NS", "HBLENGINE.NS", "HCLTECH.NS", "HDFCAMC.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HFCL.NS", "HAPPSTMNDS.NS", "HAVELLS.NS", "HEROMOTOCO.NS", "HEXT.NS", "HSCL.NS", "HINDALCO.NS", "HAL.NS", "HINDCOPPER.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "POWERINDIA.NS", "HOMEFIRST.NS", "HONASA.NS", "HONAUT.NS", "HUDCO.NS", "HYUNDAI.NS", "ICICIBANK.NS", "ICICIGI.NS", "ICICIPRULI.NS", "IDBI.NS", "IDFCFIRSTB.NS", "IFCI.NS", "IIFL.NS", "INOXINDIA.NS", "IRB.NS", "IRCON.NS", "ITCHOTELS.NS", "ITC.NS", "ITI.NS", "INDGN.NS", "INDIACEM.NS", "INDIAMART.NS", "INDIANB.NS", "IEX.NS", "INDHOTEL.NS", "IOC.NS", "IOB.NS", "IRCTC.NS", "IRFC.NS", "IREDA.NS", "IGL.NS", "INDUSTOWER.NS", "INDUSINDBK.NS", "NAUKRI.NS", "INFY.NS", "INOXWIND.NS", "INTELLECT.NS", "INDIGO.NS", "IGIL.NS", "IKS.NS", "IPCALAB.NS", "JBCHEPHARM.NS", "JKCEMENT.NS", "JBMA.NS", "JKTYRE.NS", "JMFINANCIL.NS", "JSWCEMENT.NS", "JSWENERGY.NS", "JSWINFRA.NS", "JSWSTEEL.NS", "JPPOWER.NS", "J&KBANK.NS", "JINDALSAW.NS", "JSL.NS", "JINDALSTEL.NS", "JIOFIN.NS", "JUBLFOOD.NS", "JUBLINGREA.NS", "JUBLPHARMA.NS", "JWL.NS", "JYOTHYLAB.NS", "JYOTICNC.NS", "KPRMILL.NS", "KEI.NS", "KPITTECH.NS", "KSB.NS", "KAJARIACER.NS", "KPIL.NS", "KALYANKJIL.NS", "KARURVYSYA.NS", "KAYNES.NS", "KEC.NS", "KFINTECH.NS", "KIRLOSBROS.NS", "KIRLOSENG.NS", "KOTAKBANK.NS", "KIMS.NS", "LTF.NS", "LTTS.NS", "LICHSGFIN.NS", "LTFOODS.NS", "LTIM.NS", "LT.NS", "LATENTVIEW.NS", "LAURUSLABS.NS", "THELEELA.NS", "LEMONTREE.NS", "LICI.NS", "LINDEINDIA.NS", "LLOYDSME.NS", "LODHA.NS", "LUPIN.NS", "MMTC.NS", "MRF.NS", "MGL.NS", "MAHSCOOTER.NS", "MAHSEAMLES.NS", "M&MFIN.NS", "M&M.NS", "MANAPPURAM.NS", "MRPL.NS", "MANKIND.NS", "MARICO.NS", "MARUTI.NS", "MFSL.NS", "MAXHEALTH.NS", "MAZDOCK.NS", "METROPOLIS.NS", "MINDACORP.NS", "MSUMI.NS", "MOTILALOFS.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NATCOPHARM.NS", "NBCC.NS", "NCC.NS", "NHPC.NS", "NLCINDIA.NS", "NMDC.NS", "NSLNISP.NS", "NTPCGREEN.NS", "NTPC.NS", "NH.NS", "NATIONALUM.NS", "NAVA.NS", "NAVINFLUOR.NS", "NESTLEIND.NS", "NETWEB.NS", "NEULANDLAB.NS", "NEWGEN.NS", "NAM-INDIA.NS", "NIVABUPA.NS", "NUVAMA.NS", "NUVOCO.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "OLAELEC.NS", "OLECTRA.NS", "PAYTM.NS", "ONESOURCE.NS", "OFSS.NS", "POLICYBZR.NS", "PCBL.NS", "PGEL.NS", "PIIND.NS", "PNBHOUSING.NS", "PTCIL.NS", "PVRINOX.NS", "PAGEIND.NS", "PATANJALI.NS", "PERSISTENT.NS", "PETRONET.NS", "PFIZER.NS", "PHOENIXLTD.NS", "PIDILITIND.NS", "PPLPHARMA.NS", "POLYMED.NS", "POLYCAB.NS", "POONAWALLA.NS", "PFC.NS", "POWERGRID.NS", "PRAJIND.NS", "PREMIERENE.NS", "PRESTIGE.NS", "PGHH.NS", "PNB.NS", "RRKABEL.NS", "RBLBANK.NS", "RECLTD.NS", "RHIM.NS", "RITES.NS", "RADICO.NS", "RVNL.NS", "RAILTEL.NS", "RAINBOW.NS", "RKFORGE.NS", "RCF.NS", "REDINGTON.NS", "RELIANCE.NS", "RELINFRA.NS", "RPOWER.NS", "SBFC.NS", "SBICARD.NS", "SBILIFE.NS", "SJVN.NS", "SRF.NS", "SAGILITY.NS", "SAILIFE.NS", "SAMMAANCAP.NS", "MOTHERSON.NS", "SAPPHIRE.NS", "SARDAEN.NS", "SAREGAMA.NS", "SCHAEFFLER.NS", "SCHNEIDER.NS", "SCI.NS", "SHREECEM.NS", "SHRIRAMFIN.NS", "SHYAMMETL.NS", "ENRIN.NS", "SIEMENS.NS", "SIGNATURE.NS", "SOBHA.NS", "SOLARINDS.NS", "SONACOMS.NS", "SONATSOFTW.NS", "STARHEALTH.NS", "SBIN.NS", "SAIL.NS", "SUMICHEM.NS", "SUNPHARMA.NS", "SUNTV.NS", "SUNDARMFIN.NS", "SUNDRMFAST.NS", "SUPREMEIND.NS", "SUZLON.NS", "SWANCORP.NS", "SWIGGY.NS", "SYNGENE.NS", "SYRMA.NS", "TBOTEK.NS", "TVSMOTOR.NS", "TATACHEM.NS", "TATACOMM.NS", "TCS.NS", "TATACONSUM.NS", "TATAELXSI.NS", "TATAINVEST.NS", "TMPV.NS", "TATAPOWER.NS", "TATASTEEL.NS", "TATATECH.NS", "TTML.NS", "TECHM.NS", "TECHNOE.NS", "TEJASNET.NS", "NIACL.NS", "RAMCOCEM.NS", "THERMAX.NS", "TIMKEN.NS", "TITAGARH.NS", "TITAN.NS", "TORNTPHARM.NS", "TORNTPOWER.NS", "TARIL.NS", "TRENT.NS", "TRIDENT.NS", "TRIVENI.NS", "TRITURBINE.NS", "TIINDIA.NS", "UCOBANK.NS", "UNOMINDA.NS", "UPL.NS", "UTIAMC.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UBL.NS", "UNITDSPR.NS", "USHAMART.NS", "VGUARD.NS", "DBREALTY.NS", "VTL.NS", "VBL.NS", "MANYAVAR.NS", "VEDL.NS", "VENTIVE.NS", "VIJAYA.NS", "VMM.NS", "IDEA.NS", "VOLTAS.NS", "WAAREEENER.NS", "WELCORP.NS", "WELSPUNLIV.NS", "WHIRLPOOL.NS", "WIPRO.NS", "WOCKPHARMA.NS", "YESBANK.NS", "ZFCVINDIA.NS", "ZEEL.NS", "ZENTEC.NS", "ZENSARTECH.NS", "ZYDUSLIFE.NS", "ECLERX.NS"]

    csv_column_mapping = {
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Date': 'Date'
    }

    spreadsheet_id  = "XXXXXXXXXXXXXXXXX"  # Replace with your Sheet ID

    csv_directory   = "Stocks"
    fetch_csv_data  = False

    period          = "max"                 #   "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y","5y", "10y", "ytd", "max"
    interval        = "1d"                  #   "1m", "2m", "5m", "15m", "30m", "60m"/"1h", "90m",      "1d", "5d", "1wk", "1mo", "3mo"

    force_refresh_data = False                # If True - Fresh yfinance data fetch , False - used cache data
    
    auto_adjust     = False                 # Raw prices — unadjusted for splits/dividends.
    visualize       = False                 # If True, plots SMC charts; False skips to save time.
    print_details   = False                 # If True, logs detailed analysis/screener info; False for minimal output.
    output_format   = "both"                 # Save to csv, google_sheets, or both for analysis and screener results.
    clear           = True                  # Clears existing CSVs and Google Sheets before saving new data.

    use_colab       = False                 # if goole colab use True otherwise False

    distance_percentage = 2.0                       # % threshold for screener to find stocks near SMC levels.
    output_csv          = "screener_results.csv"    # File path for screener results CSV.

    while True:
        print("\n📌 Select the task to run:")
        print("   1️⃣  SMC Analysis")
        print("   2️⃣  SMC Screener")
        print("   3️⃣  Both (Analysis + Screener)")
        print("   0️⃣  Exit")
        try:
            task_choice = input("\n👉 Enter your choice (1, 2, 3, or 0 to exit): ").strip().lower()
            if task_choice in ["1", "2", "3"]:
                break
            elif task_choice == "0" or task_choice == "exit":
                print("\n🛑 Exiting program.")
                return
            print("❌ Invalid choice. Please enter 1, 2, 3, or 0.")
        except KeyboardInterrupt:
            print("\n🛑 Exiting program.")
            return

    # Map task
    task = {"1": "analysis", "2": "screener", "3": "both"}[task_choice]

    stock_codes, stock_list_name = None, None

    # Only ask stock list if Analysis or Both is selected
    if task in ["analysis", "both"]:
        while True:
            print("\n📊 Select the stock list to process:")
            print("   1️⃣  Nifty Top 10")
            print("   2️⃣  Nifty 50")
            print("   3️⃣  F&O")
            print("   4️⃣  Nifty 500")
            print("   5️⃣  Indices")
            print("   0️⃣  Back to task selection")
            try:
                stock_choice = input("\n👉 Enter your choice (1, 2, 3, 4, 5, or 0 to go back): ").strip().lower()
                if stock_choice in ["1", "2", "3", "4", "5"]:
                    break
                elif stock_choice == "0" or stock_choice == "exit":
                    print("\n🔙 Returning to task selection.")
                    return await run_full_pipeline()  # Go back to task selection
                print("❌ Invalid choice. Please enter 1, 2, 3, 4, 5, or 0.")
            except KeyboardInterrupt:
                print("\n🛑 Exiting program.")
                return

        # Map stock list
        stock_codes = {
            "1": NIFTY_TOP_10,
            "2": NIFTY_50,
            "3": FN_O_STOCKS,
            "4": NIFTY_500,
            "5": INDICES
        }[stock_choice]
        stock_list_name = {
            "1": "Nifty Top 10",
            "2": "Nifty 50",
            "3": "F&O",
            "4": "Nifty 500",
            "5": "Indices"
        }[stock_choice]

        print(f"\n✅ Selected Stocks or Indices list: *{stock_list_name}* ({len(stock_codes)} No. of Stocks or Indices)")

    # Execute selected task(s)
    if task in ["analysis", "both"]:
        print("\n🚀 Running SMC Analysis...")
        failed_symbols = await main_analysis(
            stock_codes         = stock_codes,
            csv_directory       = csv_directory,
            fetch_csv_data      = fetch_csv_data,
            csv_column_mapping  = csv_column_mapping,
            spreadsheet_id      = spreadsheet_id,
            period              = period,
            interval            = interval,
            auto_adjust         = auto_adjust,
            visualize           = visualize,
            print_details       = print_details,
            clear               = clear,
            output_format       = output_format,
            force_refresh_data  = force_refresh_data,
            use_colab           = use_colab
        )
        print("📈 Analysis completed successfully!")

    if task in ["screener", "both"]:
        print("\n🔎 Running SMC Screener...")
        main_screener(
            proximity_percentage    = distance_percentage,
            output_csv              = output_csv,
            spreadsheet_id          = spreadsheet_id,
            output_format           = output_format,
            clear                   = clear,
            print_details           = print_details,
            use_colab               = use_colab
        )
        print("📊 Screener completed successfully!")

    # Final message
    if stock_list_name:
        print(f"\n🎯 Task *{task.upper()}* completed for *{stock_list_name}*! 🎉")
    else:
        print(f"\n🎯 Task *{task.upper()}* completed successfully! 🎉")

    # Report failed symbols
    if 'failed_symbols' in locals() and failed_symbols:
        print("\n⚠️ Failed to fetch data for the following symbols:")
        for symbol in failed_symbols:
            print(f"   - {symbol}")

if __name__ == "__main__":
    asyncio.run(run_full_pipeline())

# # For Google Colab:
# if __name__ == "__main__":
#     await(run_full_pipeline())
