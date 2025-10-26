
# 📊 Indian Stock Gap Detector

A Python-based tool to detect **bullish and bearish gaps** in Indian stocks and indices using historical price data from Yahoo Finance (`yfinance`). Supports multiple stock groups, custom filters, and exports results to CSV.

---

## 🔹 Features

- Scan multiple stock groups:
  - NSE Indices
  - Nifty Top 10
  - Nifty 50
  - Futures & Options Stocks (FnO)
  - Nifty 500
- Detect **Bullish Gaps** (price jumps above previous day high)
- Detect **Bearish Gaps** (price drops below previous day low)
- Filter gaps by:
  - Near current price (customizable tolerance)
  - Minimum gap size (%)
- Export results to CSV (`Gaps Result/`)
- Optional pretty table output in terminal
- Adjustable scan period (`1y`, `6mo`, `3mo`, etc.) and candle interval (`1d`, `1wk`, etc.)
- Automatic batch sleeping to avoid API throttling

---

## ⚙️ Usage

```bash
python Unfilled Gaps Near LTP.py
```

1. Select a stock group by entering the number.
2. The script scans selected stocks and detects gaps based on your configuration.
3. CSV results are saved in `Gaps Result/gaps_<group_name>.csv`.

---

## 🧭 Configuration

Set these variables in `Unfilled Gaps Near LTP.py`:

| Variable           | Description                                     | Default  |
|-------------------|-------------------------------------------------|----------|
| period             | Data period (`1d`, `1mo`, `6mo`, `1y`, etc.)  | `"1y"`   |
| interval           | Candle interval (`1d`, `1wk`, `1mo`)           | `"1d"`   |
| near_tolerance     | How close gap is to current price (fraction)   | 0.03     |
| min_gap_percent    | Minimum gap size (%)                            | 1.0      |
| sleep_after        | Sleep after how many stocks                     | 10       |
| sleep_time         | Sleep duration (seconds)                        | 1        |
| only_near          | Only include gaps near current price           | True     |

---

## 📂 Output

CSV saved in `Gaps Result/`

Columns:

| Stock | Current Price | Date | Gap Type | Gap Size | Gap Range | Near Price | Gap Size % | Gap Dist Start % | Gap Dist End % |

Optional terminal output with PrettyTable for quick review.

---

## 💡 Example

```text
➡️  Select Stock Group to Scan:
1. 🏦 indices (10 stocks)
2. 📈 nifty_top_10 (10 stocks)
3. 📈 nifty_50 (50 stocks)
4. 💹 fn_o_stocks (250+ stocks)
5. 📈 nifty_500 (500+ stocks)

Enter choice number: 2

✅ Selected group: nifty_top_10 (10 stocks) 🔹
Scanning stocks...
CSV saved: Gaps Result/gaps_nifty_top_10.csv
```

---

## 🔧 Customization

- Adjust `near_tolerance`, `min_gap_percent`, and `interval` to fit your trading strategy.
- Set `only_near=False` to include all gaps.
- Can integrate into automated trading dashboards or backtesting frameworks.

---

## ⚠️ Notes

- Relies on Yahoo Finance API; some stocks may have missing data.
- Batch sleep prevents rate-limiting issues.
- Tested with Indian NSE tickers (suffix `.NS` required).

---


## 📜 License
MIT License — Free to use and modify for personal or professional purposes.

---

> ⚠️ **Disclaimer:**  
> This tool is built for educational and research purposes only.  
> It does not constitute investment advice. Use data and insights responsibly.