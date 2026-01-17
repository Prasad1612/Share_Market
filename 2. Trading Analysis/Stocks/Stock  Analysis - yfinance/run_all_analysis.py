import importlib.util
import os
import sys
import stock_data_manager
import pandas as pd
from tqdm import tqdm

def load_module_from_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def main():
    print("🚀 Starting Combined Analysis...")
    
    # 1. Fetch Data (Checks cache first)
    print("\n1️⃣  Fetching Stock Data (Local Cache or Fresh Download)...")
    tickers = stock_data_manager.get_combined_ticker_list()
    # For testing, you can limit tickers here. Comment out for production.
    # tickers = tickers[:5] 
    
    # get_data handles cache checking and smart slicing automatically
    LTP_Near_Gaps       = stock_data_manager.get_data(tickers, period="1y", interval="1d")
    Support_Resistance  = stock_data_manager.get_data(tickers, period="1y", interval="1d")
    Candle_Analysis     = stock_data_manager.get_data(tickers, period="1mo", interval="1d")

    if not LTP_Near_Gaps:
        print("❌ No data fetched. Exiting.")
        return

    # 2. Run LTP Near Gaps
    print("\n2️⃣  Running LTP Near Gaps Analysis...")
    try:
        ltp_module = load_module_from_path("ltp_gaps", "LTP Near Gaps.py")
        
        # Run for each group defined in the module
        groups = ltp_module.groups
        for group_name, group_tickers in groups.items():
            print(f"   > Processing Group: {group_name}...")
            # Filter tickers that are in data_map
            valid_tickers = [t for t in group_tickers if t in LTP_Near_Gaps]
            
            if valid_tickers:
                gaps = ltp_module.detect_gaps(LTP_Near_Gaps, valid_tickers)
                ltp_module.save_to_csv(gaps, filename=f"gaps_{group_name}.csv")
            else:
                print(f"     No valid data for group {group_name}")
                
    except Exception as e:
        print(f"❌ Error running LTP Near Gaps: {e}")

    # 3. Run Support and Resistance
    print("\n3️⃣  Running Support & Resistance Analysis...")
    try:
        sr_module = load_module_from_path("support_resistance", "Support and Resistance.py")
        
        # Pass all tickers or a specific list. Passing all unique available tickers.
        all_available_tickers = list(Support_Resistance.keys())
        
        sr_module.run_fractal_sr(
            all_available_tickers, 
            period="1y", 
            interval="1d", 
            data_dict=Support_Resistance,
            save_charts=True,
            plot=False,
            out_dir='outputs/support_resistance/' # separate folder to distinguish
        )
        
    except Exception as e:
        print(f"❌ Error running Support and Resistance: {e}")

    # 4. Candle & Gap Analysis
    print("\n4️⃣  Running Candle & Gap Analysis...")
    try:
        candle_module = load_module_from_path("candle_analysis", "candle & gap analysis.py")
        candle_module.run(data_dict=Candle_Analysis)
        
    except Exception as e:
        print(f"❌ Error running Candle & Gap Analysis: {e}")

    # 5. Fibonacci Levels Analysis
    print("\n5️⃣  Running Fibonacci Levels Analysis...")
    try:
        fib_module = load_module_from_path("fibonacci_analysis", "Fibonacci Levels.py")
        
        # Use a group for analysis (e.g., nifty_500 as default or whatever is in stock_data_manager)
        all_tickers = stock_data_manager.get_combined_ticker_list()
        
        fib_module.run_analysis(all_tickers, data_dict=Support_Resistance) # Re-using Support_Resistance cache (1y data)
        
    except Exception as e:
        print(f"❌ Error running Fibonacci Analysis: {e}")

    print("\n✅ All Analyses Completed Successfully!")

if __name__ == "__main__":
    main()
