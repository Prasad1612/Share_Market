# QuantStats Stock Analysis Reports

This project provides Python scripts to generate detailed performance reports for single stocks, stocks versus benchmarks, and multi-stock portfolios using `yfinance` and `QuantStats`.

---

## Features

1. **Single Stock Report**
   - Generates an HTML report with performance metrics for any stock.
   - Includes returns, risk measures, Sharpe ratio, drawdowns, and more.

2. **Stock vs Benchmark Report**
   - Compare any stock against a benchmark index (e.g., NIFTY).
   - Generates metrics and interactive HTML report highlighting relative performance.

3. **Multi-Stock Portfolio Report**
   - Create an equal-weighted portfolio from multiple stocks.
   - Provides consolidated portfolio returns and performance metrics.

---

## Dependencies

- Python 3.10+
- `yfinance`
- `pandas`
- `quantstats`
- `os` (standard library)

Install dependencies via pip:

```bash
pip install yfinance pandas quantstats
```

---

## Usage

### 1️⃣ Single Stock Report

```python
single_stock_report("TCS.NS", period="1y", interval="1d")
```
- Generates `Single Stock/TCS.NS_quantstats_report.html`
- Displays key performance metrics in console.

### 2️⃣ Stock vs Benchmark Report

```python
stock_vs_benchmark_report("TCS.NS", benchmark_symbol="^NSEI", period="1y", interval="1d")
```
- Generates `Stock vs Benchmark/TCS.NS_vs_NIFTY_report.html`
- Console output includes metrics against the benchmark.

### 3️⃣ Multi-Stock Portfolio Report

```python
multi_stock_portfolio_report(["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"], period="1y", interval="1d")
```
- Generates `Multi-Stock Portfolio/portfolio_quantstats_report.html`
- Calculates equal-weighted portfolio returns and performance.

---

## Notes

- Ensure internet connectivity to fetch stock data from Yahoo Finance.
- `period` and `interval` parameters follow `yfinance` standards.
- HTML reports are interactive and can be opened in any modern browser.

---

## Example Run

```python
if __name__ == "__main__":
    # Single stock
    single_stock_report("NIFTYBEES.NS", "max", "1d")

    # Stock vs benchmark
    stock_vs_benchmark_report("GODFRYPHLP.NS", "^NSEI", "max", "1d")

    # Multi-stock portfolio
    multi_stock_portfolio_report(["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS"], "max", "1d")
```

---


## 📜 License
MIT License — Free to use and modify for personal or professional purposes.

---

> ⚠️ **Disclaimer:**  
> This tool is built for educational and research purposes only.  
> It does not constitute investment advice. Use data and insights responsibly.