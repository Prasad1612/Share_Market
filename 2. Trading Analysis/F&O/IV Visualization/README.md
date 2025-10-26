# 📈 Enhanced NIFTY Option Chain IV Visualization (3D Surface + 2D Skew)
 
**Profession:** Intraday Trader & Investor | Financial Analyst | Risk Management Specialist

---

## 🗂️ Project Overview

This Python script visualizes **Implied Volatility (IV)** across **NIFTY, BANKNIFTY, FINNIFTY, and other indices/stocks** using **live NSE Option Chain data** fetched via the [`NseKit`](https://pypi.org/project/NseKit/) library.

The tool provides two rich, interactive visualizations:
- **3D IV Surface Plot:** Displays CE (Call) and PE (Put) IVs in a 3D view, helping visualize volatility skew and symmetry.
- **2D IV Skew Chart:** Shows strike-wise IV distribution and CE-PE IV differential (Skew), enabling traders to identify volatility imbalance and market sentiment shifts.

---

## ⚙️ Key Features

✅ **Live Data Fetching** — Automatically fetches real-time F&O option chain data from NSE India using `NseKit`.

✅ **Expiry Auto-Detection** — Automatically lists and selects the nearest expiry date.

✅ **Robust IV Extraction** — Handles missing or malformed data gracefully, ensuring stable performance.

✅ **ATM Strike Detection** — Automatically detects the strike closest to the current underlying value.

✅ **Days-to-Expiry (DTE)** — Calculates number of days left to expiry for better context.

✅ **3D Interactive Visualization** — Creates a high-quality surface plot of CE and PE IV using Plotly.

✅ **2D IV Skew Chart** — Displays CE-IV vs PE-IV along with IV skew (difference curve).

✅ **Data Export Options** — Optionally saves extracted data to CSV and interactive HTML reports.

---

## 🧩 Requirements

Install dependencies using:

```bash
pip install NseKit pandas numpy plotly scipy
```

---

## 🧠 How It Works

### 1️⃣ Fetch Live Option Chain
The script connects to NSE and retrieves **raw JSON data** for a specified symbol (e.g., `NIFTY`, `BANKNIFTY`, `RELIANCE`, etc.).

### 2️⃣ Parse & Clean Data
Extracts **strike prices, CE IVs, and PE IVs** for the nearest expiry. Missing data is interpolated and forward/backfilled.

### 3️⃣ Compute Key Metrics
- **Underlying Spot Value**
- **ATM Strike Price**
- **Days to Expiry (DTE)**

### 4️⃣ Generate Visuals
- **3D IV Surface:** Displays CE and PE IVs as a smooth surface using `plotly.graph_objects.Surface`.
- **2D IV Skew:** Plots CE-IV, PE-IV, and IV Skew (CE–PE) with ATM marker.

---

## 🖼️ Output Examples

### 🧊 3D IV Surface
Visualizes how CE and PE IVs vary across strike prices.
- Axis X → Strike Price  
- Axis Y → Option Type (0=PE, 1=CE)  
- Axis Z → Implied Volatility (%)

### 📉 2D IV Skew Chart
Displays:
- CE IV (green)
- PE IV (red)
- IV Skew (yellow dotted line)
- ATM Strike marker

---

## 📁 Optional Outputs

| Output Type | Description | Toggle |
|--------------|-------------|---------|
| **CSV File** | Saves IV data snapshot (`NIFTY_OptionChain_IV_<expiry>.csv`) | `csv_save = True` |
| **HTML File** | Saves interactive charts for sharing | `html_save = True` |

---

## 🔧 Configuration

You can easily modify parameters in the script:

| Variable | Description | Example |
|-----------|--------------|----------|
| `symbol` | F&O Index or Stock Symbol | `"NIFTY"` |
| `expiry_date` | Specific expiry or `None` for nearest | `"28-10-2025"` |
| `csv_save` | Save IV data to CSV | `True / False` |
| `html_save` | Save 3D & 2D plots as HTML | `True / False` |

---

## 🧭 Example Usage

```python
symbol = "NIFTY"
expiry_date = None
csv_save = True
html_save = True
```

Run the script:
```bash
python IV_Visualization.py
```

---

## 📊 Sample Console Output

```
Available expiries (4): ['31-Oct-2025', '07-Nov-2025', '28-Nov-2025', '26-Dec-2025']
Using expiry: 31-Oct-2025

Underlying: 22450.75 | ATM Strike: 22450 | DTE: 5 days
Saved 156 rows to NIFTY_OptionChain_IV_31-Oct-2025.csv
Saved interactive plots to: NIFTY_IV_Surface_3D.html and NIFTY_IV_Surface_2D.html
```

---

## 🧮 Interpretation Guide

| Metric | Insight |
|--------|----------|
| **High IV** | Indicates high expected volatility → expensive options |
| **Low IV** | Indicates stable outlook → cheaper options |
| **IV Skew (CE > PE)** | Bullish bias (Calls demand more) |
| **IV Skew (PE > CE)** | Bearish bias (Puts demand more) |

---

## 📘 Financial Utility

This visualization helps **professional traders** and **analysts** to:
- Identify **volatility smile/skew**
- Detect **event premium** buildup pre-expiry
- Spot **unusual IV behavior** across strikes
- Support **volatility-based trading strategies**

---

## 🧱 Project Structure

```
📦 IV_Visualization/
 ┣ 📜 IV_Visualization.py
 ┣ 📊 NIFTY_OptionChain_IV_<expiry>.csv
 ┣ 📈 NIFTY_IV_Surface_3D.html
 ┣ 📉 NIFTY_IV_Surface_2D.html
 ┗ 📘 README.md
```

---

## 📜 License

MIT License © 2025 Prasad  
You are free to use, modify, and distribute this tool with proper credit.

---


> ⚠️ **Disclaimer:**  
> This tool is built for educational and research purposes only.  
> It does not constitute investment advice. Use data and insights responsibly.
