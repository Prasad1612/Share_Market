# 📊 Fibonacci Retracement Analysis (Multi-Stock, Interactive & Non-Interactive)

## 🧠 Overview
A comprehensive **Python-based Fibonacci Retracement Analysis Toolkit** designed for **Intraday Traders and Investors**.  
These scripts automatically identify **recent swing highs/lows**, compute **Fibonacci retracement levels**, and visualize or export actionable zones.

The toolkit includes three modules:
1. **Fibonacci – Multi Stocks (CSV)**           → Bulk stock scanning & CSV output  
2. **Fibonacci – Interactive (Plotly)**         → Web-style interactive Fibonacci visualization  
3. **Fibonacci – Non-Interactive (MPLFinance)** → Elegant static Fibonacci plots for reporting  

---

## 🚀 Features
- Works on **multiple stocks simultaneously**.
- Fetches **live data via `yfinance`**.
- Automatically detects **recent swing high & swing low** using lookback windows.
- Calculates key **Fibonacci retracement levels (23.6%, 38.2%, 50%, 61.8%, 78.6%)**.
- Exports **filtered levels** near current market price.
- Optional **interactive charting** for technical analysis presentations.
- Generates **volume overlay** and **trend detection** for each instrument.

---

## 📁 Project Structure
```
Fibonacci/
│
├── Fibonacci - Multi Stocks (CSV).py
├── Fibonacci - Interactive.py
├── Fibonacci - Non Interactive.py
└── README.md
```

---

## 📈 Module 1: Fibonacci – Multi Stocks (CSV)
### Purpose
Scans multiple stocks, identifies the **trend**, detects **recent swing highs/lows**, and exports **nearby Fibonacci levels** (within user-defined % distance).

### Key Parameters
| Parameter | Description | Default |
|------------|--------------|----------|
| `lookback` | No. of days for swing detection | 60 |
| `distance_threshold` | Max % deviation from LTP | 2.0 |
| `period` | Historical data range | 6mo |
| `interval` | Candle interval | 1d |

### Output Example (`fib_levels_detailed.csv`)
| Stock | Trend | Fib_Level | Level_Price | Distance_from_LTP |
|:------|:-------|:-----------|:--------------|:------------------|
| RELIANCE | Uptrend | 38.2% | 2805.50 | +1.85% |
| HDFCBANK | Downtrend | 61.8% | 1580.20 | -1.23% |

---

## 🌐 Module 2: Fibonacci – Interactive (Plotly)
### Purpose
Generates **interactive candlestick charts** with **Fibonacci retracement overlays**. Ideal for dashboards, research visuals, and trading presentations.

### Features
- Dynamic chart zoom, hover, and pan.
- Volume overlay on secondary axis.
- Auto-calculates swing points and plots levels post swing low date.

### Output
An interactive **HTML plot** showing retracement levels colored by strength.

---

## 🖼️ Module 3: Fibonacci – Non-Interactive (MPLFinance)
### Purpose
Produces **high-quality static Fibonacci retracement plots** with annotations, ideal for reports or batch analysis.

### Features
- Detects recent swing using statistical highs/lows.
- Uses Seaborn and MPLFinance styling for publication-ready visuals.
- Annotates each Fibonacci level on the chart.

---

## 🧮 Fibonacci Levels Formula
For a swing from **High (H)** to **Low (L)**:

| Level | Formula |
|:-------|:---------|
| 23.6% | `H - 0.236 × (H - L)` |
| 38.2% | `H - 0.382 × (H - L)` |
| 50.0% | `H - 0.5 × (H - L)` |
| 61.8% | `H - 0.618 × (H - L)` |
| 78.6% | `H - 0.786 × (H - L)` |
| 100% | `L` |

---

## 🧰 Installation
```bash
pip install yfinance pandas matplotlib mplfinance seaborn plotly tqdm
```

---

## ⚙️ Example Run
```python
# Multi-Stock Mode
python "Fibonacci - Multi Stocks (CSV).py"

# Interactive Plot
python "Fibonacci - Interactive.py"

# Non-Interactive Plot
python "Fibonacci - Non Interactive.py"
```

---

## 🧾 Interpretation Guide
- **Uptrend**: Price is rising; Fibonacci retracement acts as potential **support zones**.
- **Downtrend**: Price is falling; Fibonacci retracement acts as potential **resistance zones**.
- **Confluence** between multiple levels (Fibs, S/R, EMAs) strengthens trade reliability.

---

## 🛡️ Risk Management Notes
- Combine Fibonacci levels with **volume breakout**, **RSI**, or **moving average zones**.
- Avoid relying on Fibonacci alone; validate with **price action confirmation**.
- Maintain strict **stop-loss discipline** below swing lows/highs.

---


## 📜 License
MIT License — Free to use and modify for personal or professional purposes.

---

---

> ⚠️ **Disclaimer:**  
> This tool is built for educational and research purposes only.  
> It does not constitute investment advice. Use data and insights responsibly.