# NseKit

A Python package to fetch data from the National Stock Exchange (NSE) of India, including market data, IPOs, indices, and more.

## Installation

```bash
pip install NseKit
```

## Usage

```python
from NseKit import Nse

get = Nse()

# Fetch trading holidays
holidays = get.nse_trading_holidays()
print(holidays)

# Fetch live market turnover
turnover = get.nse_live_market_turnover()
print(turnover)

# Fetch historical index data
nifty_data = get.index_historical_data("NIFTY 50", period="1M")
print(nifty_data)
```

## Requirements

- Python 3.7+


## 📜 License
MIT License — Free to use and modify for personal or professional purposes.


> ⚠️ **Disclaimer:**  
> This tool is built for educational and research purposes only.  
> It does not constitute investment advice. Use data and insights responsibly.