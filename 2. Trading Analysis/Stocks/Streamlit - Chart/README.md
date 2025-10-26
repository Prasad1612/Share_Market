# 📈 Stock Price, Volume & Deliverables Dashboard  

An interactive **Streamlit dashboard** to analyze **NSE stock price, volume, deliverables, and trades** using the [nselib](https://pypi.org/project/nselib/) library.  
The app provides candlestick charts, deliverable vs intraday volumes, number of trades, and % deliverable trends — all with **quick date range filters** and **raw data view**.  

---

## 🚀 Features
- **Stock Symbol Selector** – Drop-down from NSE 500-listed stocks or Enter Manually.  
- **Quick Date Filters** – `1W`, `1M`, `6M`, `1Y`, or Manual date range.  
- **Candlestick Chart** – Price movement with % change hover info.  
- **Deliverable vs Intraday Volumes** – Compare delivery vs intraday Volumes.  
- **Trades & % Deliverables** – Dual-axis bar & line chart.  
- **Raw Data Viewer** – Toggle to inspect clean historical data.  
- **Cache Management** – One-click clear cache button.  


---

## ▶️ Usage  

Run the Streamlit app:  

```bash
streamlit run Stock_Analysis.py
```

The dashboard will open in your browser (default: `http://localhost:8501`).  

---

## 📊 Example Output  

- **Candlestick chart** showing price movements.  
- **Stacked bar chart** for Deliverable vs Intraday volumes.  
- **Number of trades & % Deliverable** on dual axes.  

---

## ⚡ Notes
- Data is fetched from **NSE via nselib**. Availability depends on NSE API stability.  
- Only **EQ (Equity)** series data is used.  
- Numbers are formatted in **Indian units (K, L, Cr)** for readability.  

---


## 📜 License
MIT License — Free to use and modify for personal or professional purposes.

---

> ⚠️ **Disclaimer:**  
> This tool is built for educational and research purposes only.  
> It does not constitute investment advice. Use data and insights responsibly.
