# 🧠 Fractal-based Support & Resistance Detector

## 📘 Overview
**Fractal Support & Resistance Detector** automates identification of market structure levels using fractal analysis.  
It’s designed for **traders, analysts, and quant professionals** who require precision-level price action insights with support for **both Yahoo Finance and local CSV data**.

---

## 🚀 Key Features
- ✅ **Dual Input Source** — Works with both `yfinance` (live data) and `local CSV` files.
- ✅ **Batch Processing** — Analyze hundreds of symbols with automatic batching and delay control.
- ✅ **Smart Fractal Detection** — Identifies multi-level support & resistance zones with adaptive filters.
- ✅ **Strength Calculation** — Calculates how many times price touched each level (validating importance).
- ✅ **Consolidated Output** — Generates:
  - `All_Levels.csv` — All detected levels for all stocks.
  - `Summary.csv` — Per-symbol overview with nearest supports/resistances.
- ✅ **Professional Visualization** — Optional candlestick plotting with marked fractal levels.

---

## 🧩 Core Logic Summary

| Function | Purpose |
|-----------|----------|
| `fetch_yf_data()` | Fetches OHLCV data from Yahoo Finance with retry logic |
| `is_support()` / `is_resistance()` | Detects local minima/maxima using 5-bar fractal logic |
| `identify_levels()` | Extracts potential levels from detected fractals |
| `filter_levels()` | Removes nearby redundant levels based on mean range |
| `compute_strength()` | Calculates how many times each level was retested |
| `nearest_two_levels()` | Finds two nearest support/resistance levels around LTP |
| `plot_levels()` | Visualizes price candles with all detected levels |
| `run_fractal_sr()` | Batch executor with CSV output and optional charting |

---

## ⚙️ Example Usage

```python
if __name__ == '__main__':
    tickers = ['RELIANCE.NS', 'TCS.NS', 'INFY.NS']

    summary, all_levels = run_fractal_sr(
        tickers,
        period      = '6mo',
        interval    = '1d',
        use_local   = False,                                    # True = use local csv
        local_dir   = None,      #r"D:\user\Trading\Stocks",    # CSV path csv file name like (RELIANCE, TCS, INFY -  file format must be .csv)
        plot        = True                                      # True = show chart
    )
```

### 📂 Output Files
| File | Description |
|------|--------------|
| `All_Levels.csv` | All S/R levels with type, strength, and distance % |
| `Summary.csv` | Summary with LTP, nearest supports, resistances, and errors |

---

## 📊 Output Example

**Summary.csv**
| SYMBOL | LTP | SUPPORTS | RESISTANCES |
|--------|-----|-----------|--------------|
| RELIANCE.NS | 2934.25 | 2900.5 (-1.15%) | 2982.0 (+1.63%) |
| TCS.NS | 3781.0 | 3720.0 (-1.61%) | 3845.5 (+1.70%) |

**All_Levels.csv**
| SYMBOL | CURRENT_PRICE | LEVEL | TYPE | STRENGTH | DIST_% |
|--------|----------------|--------|------|-----------|---------|
| RELIANCE.NS | 2934.25 | 2900.5 | Support | 3 | -1.15 |
| RELIANCE.NS | 2934.25 | 2982.0 | Resistance | 2 | +1.63 |

---

## 🧠 Technical Notes
- Uses **5-candle fractal pattern** for identification.
- Includes **mean-range based filtering** to reduce noise.
- Automatically handles **date parsing, numeric conversions, and missing data**.
- Built for **Python 3.9+** and tested with **Pandas, Numpy, Matplotlib, yfinance, tqdm**.

---

## 🏗️ Directory Output
```
📂 SR_Outputs
 ├── All_Levels.csv
 └── Summary.csv
```

---

## 🧭 Ideal Use Cases
- Swing & Intraday support/resistance mapping  
- Quantitative event zone analysis  
- Auto-detection of retest zones and key reaction levels  
- Integration into trading bots or dashboards  

---

## 🧩 Dependencies
```bash
pip install pandas numpy yfinance matplotlib tqdm
```

---

**Expertise:** Intraday Trading | Risk Management | Financial Analysis | Business Strategy

---

## 📜 License
MIT License — Free to use and modify for personal or professional purposes.

---

---

> ⚠️ **Disclaimer:**  
> This tool is built for educational and research purposes only.  
> It does not constitute investment advice. Use data and insights responsibly.
