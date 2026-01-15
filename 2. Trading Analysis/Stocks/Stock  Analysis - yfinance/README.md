# 📊 Optimized Stock Analysis System

A high‑performance, production‑ready stock analysis pipeline designed for **large universes (500+ stocks)** with **API safety, smart caching, and full automation**.

This system is optimized for **professional trading & analysis workflows**, ensuring speed, reliability, and repeatability.

---

## 🚀 Key Enhancements

### 1️⃣ Smart Disk Caching (4‑Hour Window)
**What it does**
- Downloads 1‑year historical data once and saves it locally:
  ```
  data_cache/stock_data_1y.pkl
  ```

**Why it matters**
- Prevents repeated API calls
- Avoids yfinance rate‑limit blocks
- Subsequent runs load instantly from disk

⏱️ **Cache Validity:** 4 hours (auto‑refresh after expiry)

---

### 2️⃣ API Safety & Controlled Downloads

**Batching Strategy**
- Stocks downloaded in batches of **50 symbols**
- **2‑second pause** between batches

**Benefits**
- API‑safe execution
- Stable long‑running jobs
- Suitable for daily or intraday reruns

---

### 3️⃣ Live Progress Tracking

- Integrated **tqdm progress bars**
- Real‑time visibility of:
  - Download progress
  - Analysis execution
  - Completion status

No more blind waits 🚦

---

### 4️⃣ Single‑Command Execution

Run the **entire analysis pipeline** with one command:

```bash
python run_all_analysis.py
```

This makes the system:
- Easy to automate
- Cron / Task‑Scheduler friendly
- Suitable for daily market routines

---

## 📂 Project Structure

```
.
├── run_all_analysis.py          # Master orchestration script
├── stock_data_manager.py        # Data fetch, caching & batching logic
├── LTP Near Gaps.py             # Gap proximity analysis
├── Support and Resistance.py    # S/R levels with volume charts
├── candle & gap analysis.py     # Candle patterns & gap detection
├── data_cache/
│   └── stock_data_1y.pkl        # Cached historical data
├── outputs/
│   ├── gaps/
│   ├── support_resistance/
│   └── candle_analysis/
└── README.md
```

---

## 🔄 Execution Flow (What Happens Internally)

1. **Stock Discovery**
   - Collects all unique stocks from all strategy lists

2. **Data Management**
   - Checks cache availability
   - Loads from disk if valid
   - Downloads only if cache expired

3. **Analysis Modules Executed Sequentially**
   - LTP Near Gaps
   - Support & Resistance (with Volume)
   - Candle & Gap Analysis

4. **Output Generation**
   - CSV reports
   - Charts
   - Strategy‑specific folders

---

## ✅ Verification & Performance

**Universe Size:** 511 stocks

| Stage | Time |
|------|------|
| Initial Fetch | ~5 minutes |
| Secondary Runs | Instant (cache) |
| Analysis Completion | 100% success |

✔️ All CSVs and charts generated correctly
✔️ No API throttling or failures
✔️ Fully repeatable execution

---

## 🧠 Designed For

- Intraday traders
- Swing traders
- Quant & systematic analysis
- Large‑scale NSE / global equity scans
- Professional market workflows

---

## 🔧 Best Practices

- Run once per day for fresh cache
- Avoid deleting `data_cache/` unless needed
- Schedule execution before market open
- Extend modules without touching core data logic

---

## 📌 Future‑Ready

This architecture easily supports:
- ML prediction layers
- Accuracy tracking
- Backtesting engines
- Auto‑alerts & dashboards

---

### ⚡ Built for speed. Designed for scale. Safe for APIs.

Happy Trading 📈

