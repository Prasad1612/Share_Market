# -*- coding: utf-8 -*-
# +----------------------------------------------------------------------------------+
# |                  NseKitAsync v6  -  Full Potential Usage Guide                   |
# |              148 Methods  |  Async/Await  |  All Calling Patterns               |
# +----------------------------------------------------------------------------------+
#
# +---------------------------------------------------------------------------------+
# |                     IMPORTS & CLIENT CREATION                                   |
# +---------------------------------------------------------------------------------+
#
import asyncio
from NseKitAsync import (
    AsyncNse,              # Main async client
    AsyncNseSession,       # Async context-manager factory
    AsyncNseQueryBuilder,  # Fluent async query builder
    CachePolicy,           # IntFlag: NONE | READ | WRITE | READWRITE
    Period,                # StrEnum: D1 W1 M1 M3 M6 Y1 Y2 Y5 Y10 YTD MAX
    nse_api,               # Decorator -- rate-limit + cache on custom methods
    sync_fetch,            # Run async code from sync context
)
# from rich.console import Console
#
# -- 1. Standard async context manager --------------------------------------------
async def main():
    async with AsyncNse() as nse:
        df = await nse.index_live_all_indices_data()
asyncio.run(main())
#
# -- 2. Custom rate limits + verbose debug logging ---------------------------------
# async with AsyncNse(max_per_second=3, max_per_minute=60, min_gap=0.3, verbose=True) as nse:
#     ...
#
# -- 3. AsyncNseSession factory -- auto cache-clear + cookie vault reset on exit ---
# async with AsyncNseSession(max_per_second=5, max_per_minute=180, cache_ttl=30,
#                             cache_size=1024) as nse:
#     df = await nse.index_live_all_indices_data()
#
# -- 4. sync_fetch -- call from non-async code / Jupyter ---------------------------
# async def _run():
#     async with AsyncNse() as nse:
#         return await nse.nse_market_status()
# result = sync_fetch(_run)
#
# ----------------------------------------------------------------------------------
# + v6 ENGINE -- RATE LIMITER, CACHE, COOKIE VAULT                                   +
# ----------------------------------------------------------------------------------

# -- Rate Limiter ------------------------------------------------------------------
# async with AsyncNse() as nse:
#     nse.rate_limiter.stats()
#     # -> {"total_calls": 42, "total_429s": 0, "total_waited_secs": 1.2,
#     #    "current_backoff": 0.0, "max_per_second": 5, "max_per_minute": 180}
#
#     nse.rate_limiter_stats()              # same as above, convenience alias
#     nse.rate_limiter.configure(max_per_second=2, max_per_minute=60, min_gap=0.5)

# -- Cache -------------------------------------------------------------------------
# async with AsyncNse() as nse:
#     nse.cache_stats()                     # -> {"size": 14, "max_size": 512}
#     await nse.clear_cache()               # wipe everything
#
#     async with nse.no_cache():            # bypass cache for this block only
#         df = await nse.nse_market_status()

# -- CookieVault (singleton -- shared across ALL AsyncNse instances) -----------------
# from NseKitAsync import _AsyncCookieVault
# vault = _AsyncCookieVault()
# await vault.invalidate()              # force refresh on next call
# await vault.inject({"nsit": "abc"})   # manually seed cookies

# -- Period enum (type-safe, case-insensitive) -------------------------------------
# Period.M3             # "3M"
# Period("ytd")         # Period.YTD  <- _missing_ handles lowercase
# Period.MAX            # "MAX"


# ----------------------------------------------------------------------------------
# + CONCURRENT BATCH -- THE ASYNC ADVANTAGE                                         +
# ----------------------------------------------------------------------------------

# -- asyncio.gather -- all fire simultaneously --------------------------------------
# async with AsyncNse() as nse:
#     market, turnover, indices, vix, preopen = await asyncio.gather(
#         nse.nse_market_status("Market Status"),
#         nse.nse_live_market_turnover(),
#         nse.index_live_all_indices_data(),
#         nse.india_vix_historical_data("5D"),
#         nse.pre_market_info("All"),
#     )

# -- fetch_many -- variadic list of callables ----------------------------------------
# async with AsyncNse() as nse:
#     results = await nse.fetch_many([
#         nse.nse_market_status,
#         nse.nse_live_market_turnover,
#         nse.index_live_all_indices_data,
#     ])

# -- Built-in morning dashboard (5 concurrent calls) ------------------------------
# async with AsyncNse() as nse:
#     data = await nse.morning_dashboard()
#     # -> {"market": df, "turnover": df, "indices": df, "vix": df, "preopen": df}

# -- Built-in watchlist snapshot ---------------------------------------------------
# async with AsyncNse() as nse:
#     snap = await nse.watchlist_snapshot(["RELIANCE", "TCS", "HDFCBANK", "INFY"])
#     # -> {"RELIANCE": {...}, "TCS": {...}, ...}


# ----------------------------------------------------------------------------------
# + FLUENT QUERY BUILDER -- AsyncNseQueryBuilder                                    +
# ----------------------------------------------------------------------------------

# -- Full chain --------------------------------------------------------------------
# async with AsyncNse() as nse:
#     df = await (AsyncNseQueryBuilder(nse)
#                 .symbol("RELIANCE")
#                 .period("3M")
#                 .cache_policy(CachePolicy.READWRITE)
#                 .fetch(nse.cm_live_hist_insider_trading))

# -- Date range + bypass cache -----------------------------------------------------
# async with AsyncNse() as nse:
#     df = await (AsyncNseQueryBuilder(nse)
#                 .symbol("INFY")
#                 .date_range("01-01-2025", "31-03-2025")
#                 .cache_policy(CachePolicy.NONE)
#                 .fetch(nse.cm_live_hist_board_meetings))

# -- Filter + Period ---------------------------------------------------------------
# async with AsyncNse() as nse:
#     df = await (AsyncNseQueryBuilder(nse)
#                 .period("1M")
#                 .filter("Dividend")
#                 .fetch(nse.cm_live_hist_corporate_action))

# -- Cached property shorthand -----------------------------------------------------
# async with AsyncNse() as nse:
#     df = await nse.query.symbol("TCS").period("6M").fetch(nse.cm_live_hist_insider_trading)


# ----------------------------------------------------------------------------------
# + EXTEND NseKitAsync WITH YOUR OWN METHODS                                       +
# ----------------------------------------------------------------------------------

# -- Custom method with @nse_api -- gets rate-limit + cache for free ----------------
# from NseKitAsync import nse_api, CachePolicy
#
# class MyNse(AsyncNse):
#     @nse_api(ttl=60.0, cache=CachePolicy.READWRITE)
#     async def live_pe_band(self, symbol: str) -> dict | None:
#         info = await self.cm_live_equity_info(symbol)
#         if not info:
#             return None
#         return {
#             "symbol": symbol,
#             "ltp":    info["LastTradedPrice"],
#             "note":   "Custom endpoint with @nse_api caching"
#         }
#
# async def run():
#     async with MyNse() as nse:
#         print(await nse.live_pe_band("TCS"))   # cached for 60 s
#         print(nse.cache_stats())
# asyncio.run(run())


# ----------------------------------------------------------------------------------
# + NSE -- MARKET STATUS & HOLIDAYS                                                +
# ----------------------------------------------------------------------------------

# -- Market Status -----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_market_status())                           # Default: "Market Status" DataFrame
#     print(await nse.nse_market_status("Market Status"))            # Market state for all segments
#     print(await nse.nse_market_status("Mcap"))                     # Market capitalisation row
#     print(await nse.nse_market_status("Nifty50"))                  # Indicative Nifty 50 value
#     print(await nse.nse_market_status("Gift Nifty"))               # GIFT Nifty live data
#     print(await nse.nse_market_status("All"))                      # dict of all 4 DataFrames

# -- Market Open Check (Rich coloured output) --------------------------------------
# from rich.console import Console
# rich = Console()
# async with AsyncNse() as nse:
#     rich.print(await nse.nse_is_market_open())                     # "Capital Market" default
#     rich.print(await nse.nse_is_market_open("Capital Market"))
#     rich.print(await nse.nse_is_market_open("Currency"))
#     rich.print(await nse.nse_is_market_open("Commodity"))
#     rich.print(await nse.nse_is_market_open("Debt"))
#     rich.print(await nse.nse_is_market_open("currencyfuture"))

# -- Trading Holidays --------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_trading_holidays())                        # Full DataFrame
#     print(await nse.nse_trading_holidays(list_only=True))          # ['02-Oct-2025', '24-Oct-2025', ...]

# -- Clearing Holidays -------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_clearing_holidays())
#     print(await nse.nse_clearing_holidays(list_only=True))

# -- Holiday Checks ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.is_nse_trading_holiday())                      # Is today a trading holiday?
#     print(await nse.is_nse_trading_holiday("21-Oct-2025"))         # True / False
#     print(await nse.is_nse_clearing_holiday())
#     print(await nse.is_nse_clearing_holiday("22-Oct-2025"))


# ----------------------------------------------------------------------------------
# + NSE -- MARKET ACTIVITY                                                         +
# ----------------------------------------------------------------------------------

# -- Live Market Turnover ----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_live_market_turnover())
#     # Segment-wise: Equities | Derivatives | Currency | Commodity
#     # Columns: Product, Vol, Value Rs.Cr, OI, Orders, Trades, AvgTradeValue

# -- NSE Circulars -----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_live_hist_circulars())                          # Yesterday -> today
#     print(await nse.nse_live_hist_circulars("18-07-2025", "18-10-2025"))
#     print(await nse.nse_live_hist_circulars(filter="NSE Listing"))
#     print(await nse.nse_live_hist_circulars(filter="Surveillance"))
#     print(await nse.nse_live_hist_circulars(filter="NSE Clearing"))
#     # Dept keywords: Corporate Communications | Investor Services Cell |
#     #                Member Compliance | NSE Clearing | NSE Indices |
#     #                NSE Listing | Surveillance

# -- NSE Press Releases ------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_live_hist_press_releases())
#     print(await nse.nse_live_hist_press_releases("18-07-2025", "18-10-2025"))
#     print(await nse.nse_live_hist_press_releases("01-10-2025", "04-10-2025", "NSE Listing"))

# -- Reference / Currency Spot Rates ----------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_reference_rates())
#     # Columns: currency, unit, value, prevDayValue   (USD/INR, EUR/INR, GBP/INR ...)

# -- Top 10 Nifty 50 EOD -----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_eod_top10_nifty50("17-10-25"))              # Date: DD-MM-YY


# ----------------------------------------------------------------------------------
# + SYMBOL & INDEX LISTS                                                          +
# ----------------------------------------------------------------------------------

# -- Nifty 50 ----------------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_6m_nifty_50())                              # DataFrame: Company, Industry, Symbol, Series, ISIN
#     print(await nse.nse_6m_nifty_50(list_only=True))                # ['ADANIENT', 'ADANIPORTS', ...]

# -- Nifty 500 ---------------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_6m_nifty_500())
#     print(await nse.nse_6m_nifty_500(list_only=True))

# -- All NSE Equities --------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_eod_equity_full_list())                     # ~2400+ symbols
#     print(await nse.nse_eod_equity_full_list(list_only=True))

# -- F&O Underlyings ---------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.nse_eom_fno_full_list())                        # Default: stocks
#     print(await nse.nse_eom_fno_full_list(list_only=True))          # Symbol list
#     print(await nse.nse_eom_fno_full_list("index"))                 # Index underlyings
#     print(await nse.nse_eom_fno_full_list("index", list_only=True))

# -- Index Categories --------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.list_of_indices())                              # Raw dict {category: [indices...]}
#     print(await nse.list_of_indices(as_dataframe=True))             # Long-form DataFrame
#     # -> columns: indexCategory | index

# -- State-wise Registered Investors ----------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.state_wise_registered_investors())              # JSON dict


# ----------------------------------------------------------------------------------
# + IPO                                                                           +
# ----------------------------------------------------------------------------------

# async with AsyncNse() as nse:
#     print(await nse.ipo_current())                                  # Open issues: symbol, dates, size, price
#     print(await nse.ipo_preopen())                                  # Pre-open session (newly listed today)
#     print(await nse.ipo_tracker_summary())                          # All YTD: listing gain, current return
#     print(await nse.ipo_tracker_summary("SME"))                     # SME IPOs only
#     print(await nse.ipo_tracker_summary("Mainboard"))               # Mainboard only


# ----------------------------------------------------------------------------------
# + PRE-OPEN MARKET                                                               +
# ----------------------------------------------------------------------------------

# -- Pre-Open Index Summary --------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.pre_market_nifty_info())                        # "All" default
#     print(await nse.pre_market_nifty_info("NIFTY 50"))
#     print(await nse.pre_market_nifty_info("Nifty Bank"))
#     print(await nse.pre_market_nifty_info("Emerge"))
#     print(await nse.pre_market_nifty_info("Securities in F&O"))
#     print(await nse.pre_market_nifty_info("Others"))

# -- NSE-wide Pre-Open Advance / Decline ------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.pre_market_all_nse_adv_dec_info())

# -- Pre-Open Stocks ---------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.pre_market_info("All"))
#     print(await nse.pre_market_info("NIFTY 50"))
#     print(await nse.pre_market_info("Nifty Bank"))
#     print(await nse.pre_market_info("Emerge"))
#     print(await nse.pre_market_info("Securities in F&O"))
#     print(await nse.pre_market_info("Others"))

# -- Pre-Open Derivatives ----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.pre_market_derivatives_info("Index Futures"))
#     print(await nse.pre_market_derivatives_info("Stock Futures"))


# ----------------------------------------------------------------------------------
# + INDEX -- LIVE                                                                  +
# ----------------------------------------------------------------------------------

# -- All Indices Live --------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.index_live_all_indices_data())
#     # Columns: key, index, indexSymbol, last, variation, percentChange, open, high,
#     #          low, previousClose, yearHigh, yearLow, pe, pb, dy, declines, advances,
#     #          unchanged, perChange30d, perChange365d

# -- Index Constituent Stocks ------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.index_live_indices_stocks_data("NIFTY 50"))
#     print(await nse.index_live_indices_stocks_data("NIFTY BANK"))
#     print(await nse.index_live_indices_stocks_data("NIFTY IT"))
#     print(await nse.index_live_indices_stocks_data("NIFTY MIDCAP 50"))
#     print(await nse.index_live_indices_stocks_data("NIFTY 50", list_only=True))   # Symbols only

# -- Nifty 50 Returns Summary ------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.index_live_nifty_50_returns())                  # 1W / 1M / 3M / 6M / 1Y / 3Y / 5Y

# -- Index Contribution ------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.index_live_contribution())                      # NIFTY 50, top-5 contributors
#     print(await nse.index_live_contribution("Full"))                # NIFTY 50, all stocks
#     print(await nse.index_live_contribution("NIFTY BANK"))          # Different index
#     print(await nse.index_live_contribution("NIFTY IT", "Full"))
#     print(await nse.index_live_contribution(Index="NIFTY MIDCAP 50", Mode="Full"))


# ----------------------------------------------------------------------------------
# + INDEX -- EOD & HISTORICAL                                                      +
# ----------------------------------------------------------------------------------

# -- Index EOD Bhavcopy ------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.index_eod_bhav_copy("17-10-2025"))              # All indices OHLC for that date

# -- Historical OHLC + Turnover ----------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.index_historical_data("NIFTY 50", "01-10-2025", "17-10-2025"))
#     print(await nse.index_historical_data("NIFTY 50", "01-01-2025"))           # From date -> today auto
#     print(await nse.index_historical_data("NIFTY BANK", "1W"))                 # Last 1 week
#     print(await nse.index_historical_data("NIFTY 50", "1Y"))
#     print(await nse.index_historical_data("NIFTY 50", "5Y"))
#     print(await nse.index_historical_data("NIFTY 50", "YTD"))
#     print(await nse.index_historical_data("NIFTY 50", "MAX"))                  # Full history from 2008
#     print(await nse.index_historical_data("NIFTY50 USD", period=Period.Y2))    # Period enum
#     print(await nse.index_historical_data("NIFTY IT", period=Period.YTD))
#     # Periods: 1D | 1W | 1M | 3M | 6M | 1Y | 2Y | 5Y | 10Y | YTD | MAX

# -- Historical P/E, P/B, Dividend Yield ------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.index_pe_pb_div_historical_data("NIFTY 50", "01-01-2025", "17-10-2025"))
#     print(await nse.index_pe_pb_div_historical_data("NIFTY 50", "01-01-2025"))
#     print(await nse.index_pe_pb_div_historical_data("NIFTY BANK", "1Y"))
#     print(await nse.index_pe_pb_div_historical_data("NIFTY 50", period=Period.Y5))

# -- India VIX Historical ----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.india_vix_historical_data("01-08-2025", "17-10-2025"))
#     print(await nse.india_vix_historical_data("01-01-2025"))                   # From date -> today
#     print(await nse.india_vix_historical_data("1M"))
#     print(await nse.india_vix_historical_data("1Y"))
#     print(await nse.india_vix_historical_data("YTD"))
#     print(await nse.india_vix_historical_data("MAX"))
#     print(await nse.india_vix_historical_data(period=Period.MAX))


# ----------------------------------------------------------------------------------
# + LIVE CHARTS                                                                   +
# ----------------------------------------------------------------------------------

# -- Index Chart -------------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.index_chart("NIFTY 50", "1D"))                  # Returns raw dict (chartjs-ready)
#     print(await nse.index_chart("NIFTY BANK", "1M"))
#     print(await nse.index_chart("NIFTY IT", "3M"))
#     # Periods: "1D" | "1M" | "3M" | "6M" | "1Y"

# -- Stock Chart -------------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.stock_chart("RELIANCE", "1D"))
#     print(await nse.stock_chart("TCS", "3M"))

# -- F&O Contract Chart ------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_chart("TCS", "1M"))
#     print(await nse.fno_chart("NIFTY", "3M"))

# -- India VIX Chart ---------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.india_vix_chart())                              # Default: 1M
#     print(await nse.india_vix_chart("3M"))
#     print(await nse.india_vix_chart("1Y"))


# ----------------------------------------------------------------------------------
# + EQUITY -- LIVE                                                                 +
# ----------------------------------------------------------------------------------

# -- Gift Nifty + USD/INR ----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_gifty_nifty())
#     # Columns: symbol, lastprice, daychange, perchange, contractstraded, timestmp,
#     #          expirydate, usdInr_symbol, usdInr_ltp, usdInr_updated_time

# -- Capital Market Statistics -----------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_market_statistics())
#     # Columns: Total, Advances, Declines, Unchanged, 52W High, 52W Low,
#     #          Upper Circuit, Lower Circuit, Market Cap Rs. Lac Crs, Market Cap Tn $,
#     #          Registered Investors, Registered Investors (Cr), Date

# -- Equity Quote -- Concise --------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_equity_info("RELIANCE"))
#     print(await nse.cm_live_equity_info("TCS"))
#     print(await nse.cm_live_equity_info("HDFCBANK"))
#     # Returns dict: Symbol, PreviousClose, LastTradedPrice, Change, PercentChange,
#     #               Open, Close, High, Low, VWAP, UpperCircuit, LowerCircuit

# -- Equity Quote -- Full with Order Book ------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_equity_price_info("RELIANCE"))
#     # + Bid Price/Qty 1-5, Ask Price/Qty 1-5, deliveryToTradedQuantity,
#     #   totalBuyQuantity, totalSellQuantity

# -- Equity Full Info (New NSE API) ------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_equity_full_info("RELIANCE"))
#     print(await nse.cm_live_equity_full_info("NIFTY 50"))           # Also works for indices

# -- Most Active Stocks ------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_most_active_equity_by_value())          # Top stocks by traded Rs. value
#     print(await nse.cm_live_most_active_equity_by_vol())            # Top stocks by traded volume

# -- Volume Spurts ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_volume_spurts())

# -- 52-Week High / Low ------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_52week_high())
#     # Columns: symbol, series, ltp, pChange, new52WHL, prev52WHL, prevHLDate
#     print(await nse.cm_live_52week_low())

# -- Block Deals (intraday) --------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_block_deal())
#     # Columns: session, symbol, series, open, dayHigh, dayLow, lastPrice,
#     #          previousClose, pchange, totalTradedVolume, totalTradedValue


# ----------------------------------------------------------------------------------
# + CORPORATE FILINGS -- LIVE & HISTORICAL                                         +
# ----------------------------------------------------------------------------------

# -- Insider Trading (PIT Disclosures) ---------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_hist_insider_trading())                 # Today's filings
#     print(await nse.cm_live_hist_insider_trading("1D"))
#     print(await nse.cm_live_hist_insider_trading("1W"))
#     print(await nse.cm_live_hist_insider_trading("1M"))
#     print(await nse.cm_live_hist_insider_trading("3M"))
#     print(await nse.cm_live_hist_insider_trading("6M"))
#     print(await nse.cm_live_hist_insider_trading("1Y"))
#     print(await nse.cm_live_hist_insider_trading("01-01-2025", "15-10-2025"))   # Date range
#     print(await nse.cm_live_hist_insider_trading("RELIANCE"))
#     print(await nse.cm_live_hist_insider_trading("RELIANCE", "1M"))
#     print(await nse.cm_live_hist_insider_trading("RELIANCE", "01-01-2025", "15-10-2025"))
#     # Fluent:
#     df = await nse.query.symbol("RELIANCE").period("3M").fetch(nse.cm_live_hist_insider_trading)

# -- Corporate Announcements -------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_hist_corporate_announcement())
#     print(await nse.cm_live_hist_corporate_announcement("12-10-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_corporate_announcement("RELIANCE"))
#     print(await nse.cm_live_hist_corporate_announcement("RELIANCE", "01-01-2025", "15-10-2025"))

# -- Corporate Actions (Dividend / Bonus / Split / Rights ...) ----------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_hist_corporate_action())                # Default: next 90 days
#     print(await nse.cm_live_hist_corporate_action("1D"))
#     print(await nse.cm_live_hist_corporate_action("1W"))
#     print(await nse.cm_live_hist_corporate_action("1M"))
#     print(await nse.cm_live_hist_corporate_action("3M"))
#     print(await nse.cm_live_hist_corporate_action("6M"))
#     print(await nse.cm_live_hist_corporate_action("1Y"))
#     print(await nse.cm_live_hist_corporate_action("01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_corporate_action("LAURUSLABS"))
#     print(await nse.cm_live_hist_corporate_action("LAURUSLABS", "1Y"))
#     print(await nse.cm_live_hist_corporate_action("RELIANCE", "01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_corporate_action("01-01-2025", "15-03-2025", filter="Dividend"))
#     print(await nse.cm_live_hist_corporate_action("01-01-2025", "15-03-2025", filter="Bonus"))
#     print(await nse.cm_live_hist_corporate_action(filter="Split"))

# -- Event Calendar ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_today_event_calendar())                 # Today's scheduled events
#     print(await nse.cm_live_today_event_calendar("01-01-2025", "01-01-2025"))
#     print(await nse.cm_live_today_event_calendar("15-10-2025", "20-10-2025"))
#     print(await nse.cm_live_upcoming_event_calendar())              # All upcoming events

# -- Board Meetings ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_hist_board_meetings())                  # Today (no dates)
#     print(await nse.cm_live_hist_board_meetings("01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_board_meetings("RELIANCE"))        # Symbol-only
#     print(await nse.cm_live_hist_board_meetings("TCS"))
#     print(await nse.cm_live_hist_board_meetings("RELIANCE", "01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_board_meetings(period="1M"))
#     print(await nse.cm_live_hist_board_meetings(period="3M"))
#     print(await nse.cm_live_hist_board_meetings("RELIANCE", period="6M"))

# -- Shareholder Meetings (AGM / EGM / Postal Ballot) -----------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_hist_Shareholder_meetings())
#     print(await nse.cm_live_hist_Shareholder_meetings("01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_Shareholder_meetings("RELIANCE"))
#     print(await nse.cm_live_hist_Shareholder_meetings("RELIANCE", "01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_Shareholder_meetings(period="3M"))
#     print(await nse.cm_live_hist_Shareholder_meetings("INFY", period="1Y"))

# -- Qualified Institutional Placement (QIP) --------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_hist_qualified_institutional_placement("In-Principle"))
#     print(await nse.cm_live_hist_qualified_institutional_placement("Listing Stage"))
#     print(await nse.cm_live_hist_qualified_institutional_placement("In-Principle", "1Y"))
#     print(await nse.cm_live_hist_qualified_institutional_placement("Listing Stage", "1Y"))
#     print(await nse.cm_live_hist_qualified_institutional_placement("In-Principle",  "01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_qualified_institutional_placement("Listing Stage", "01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_qualified_institutional_placement("RELIANCE"))
#     print(await nse.cm_live_hist_qualified_institutional_placement("In-Principle",  "RELIANCE", "01-01-2025"))
#     print(await nse.cm_live_hist_qualified_institutional_placement("In-Principle",  "RELIANCE", "01-01-2025", "15-10-2025"))
#     # Fluent:
#     df = await (AsyncNseQueryBuilder(nse)
#                 .symbol("RELIANCE")
#                 .date_range("01-01-2025","15-10-2025")
#                 .fetch(nse.cm_live_hist_qualified_institutional_placement))

# -- Preferential Issue ------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_hist_preferential_issue("In-Principle"))
#     print(await nse.cm_live_hist_preferential_issue("Listing Stage"))
#     print(await nse.cm_live_hist_preferential_issue("In-Principle", "1Y"))
#     print(await nse.cm_live_hist_preferential_issue("Listing Stage", "1Y"))
#     print(await nse.cm_live_hist_preferential_issue("In-Principle",  "01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_preferential_issue("Listing Stage", "01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_preferential_issue("RELIANCE"))
#     print(await nse.cm_live_hist_preferential_issue("In-Principle",  "RELIANCE", "01-01-2025"))
#     print(await nse.cm_live_hist_preferential_issue("In-Principle",  "RELIANCE", "01-01-2025", "15-10-2025"))

# -- Rights Issue ------------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_hist_right_issue("In-Principle"))
#     print(await nse.cm_live_hist_right_issue("Listing Stage"))
#     print(await nse.cm_live_hist_right_issue("In-Principle", "1Y"))
#     print(await nse.cm_live_hist_right_issue("Listing Stage", "1Y"))
#     print(await nse.cm_live_hist_right_issue("In-Principle",  "01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_right_issue("Listing Stage", "01-01-2025", "15-10-2025"))
#     print(await nse.cm_live_hist_right_issue("RELIANCE"))
#     print(await nse.cm_live_hist_right_issue("In-Principle",  "RELIANCE", "01-01-2025"))
#     print(await nse.cm_live_hist_right_issue("In-Principle",  "RELIANCE", "01-01-2025", "15-10-2025"))

# -- Voting Results ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_voting_results())
#     # Columns: vrSymbol, vrCompanyName, vrMeetingType, vrTimestamp, vrResolution,
#     #          vrTotPercFor, vrTotPercAgainst, vrbroadcastDt ...

# -- Quarterly Shareholding Patterns ----------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_qtly_shareholding_patterns())
#     # Columns: symbol, name, pr_and_prgrp, public_val, employeeTrusts, date ...

# -- Annual Reports (RSS) ----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.recent_annual_reports())
#     # Columns: symbol, companyName, fyFrom, fyTo, link, submissionDate, SME

# -- BRSR (Business Responsibility & Sustainability Report) -----------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_hist_br_sr("RELIANCE"))
#     print(await nse.cm_live_hist_br_sr("TCS"))
#     print(await nse.cm_live_hist_br_sr("HDFCBANK"))

# -- Quarterly Financial Results  (JSON dict) --------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.quarterly_financial_results("TCS"))
#     print(await nse.quarterly_financial_results("RELIANCE"))
#     # Returns: last 3 quarters -- Income, PBT, Net Profit, EPS (Consolidated + Standalone)

# -- iXBRL HTML Tables ------------------------------------------------------------
# async with AsyncNse() as nse:
#     url = "https://nsearchives.nseindia.com/corporate/ixbrl/INTEGRATED_FILING_INDAS_139754_02022026201126_iXBRL_WEB.html"
#     print(await nse.html_tables(url))                               # JSON (default)
#     print(await nse.html_tables(url, show_tables=True))
#     print(await nse.html_tables(url, output="dataframe"))


# ----------------------------------------------------------------------------------
# + EQUITY -- HISTORICAL TRADE DATA                                                +
# ----------------------------------------------------------------------------------

# -- Security-wise Historical OHLCV -----------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_hist_security_wise_data("RELIANCE"))                     # Default: 1Y
#     print(await nse.cm_hist_security_wise_data("RELIANCE", "2Y"))
#     print(await nse.cm_hist_security_wise_data("TCS", "6M"))
#     print(await nse.cm_hist_security_wise_data("RELIANCE", "01-10-2025", "17-10-2025"))
#     print(await nse.cm_hist_security_wise_data("TCS", "01-01-2025"))            # From -> today auto
#     # No-cache (always fresh):
#     async with nse.no_cache():
#         df = await nse.cm_hist_security_wise_data("RELIANCE", "1Y")

# -- Historical Bulk Deals ---------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_hist_bulk_deals())                                       # Today, all symbols
#     print(await nse.cm_hist_bulk_deals("1D"))
#     print(await nse.cm_hist_bulk_deals("1W"))
#     print(await nse.cm_hist_bulk_deals("1M"))
#     print(await nse.cm_hist_bulk_deals("3M"))
#     print(await nse.cm_hist_bulk_deals("6M"))
#     print(await nse.cm_hist_bulk_deals("1Y"))
#     print(await nse.cm_hist_bulk_deals("01-10-2025"))                           # From -> today auto
#     print(await nse.cm_hist_bulk_deals("15-10-2025", "17-10-2025"))
#     print(await nse.cm_hist_bulk_deals("RELIANCE"))
#     print(await nse.cm_hist_bulk_deals("DSSL", "1Y"))
#     print(await nse.cm_hist_bulk_deals("DSSL", "01-10-2025"))
#     print(await nse.cm_hist_bulk_deals("DSSL", "01-10-2025", "17-10-2025"))

# -- Historical Block Deals --------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_hist_block_deals())
#     print(await nse.cm_hist_block_deals("1W"))
#     print(await nse.cm_hist_block_deals("1M"))
#     print(await nse.cm_hist_block_deals("01-10-2025"))
#     print(await nse.cm_hist_block_deals("15-10-2025", "17-10-2025"))
#     print(await nse.cm_hist_block_deals("RELIANCE"))
#     print(await nse.cm_hist_block_deals("DSSL", "1Y"))
#     print(await nse.cm_hist_block_deals("DSSL", "01-10-2025"))
#     print(await nse.cm_hist_block_deals("DSSL", "01-10-2025", "17-10-2025"))

# -- Historical Short Selling ------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_hist_short_selling())
#     print(await nse.cm_hist_short_selling("1W"))
#     print(await nse.cm_hist_short_selling("1M"))
#     print(await nse.cm_hist_short_selling("01-10-2025"))
#     print(await nse.cm_hist_short_selling("15-10-2025", "17-10-2025"))
#     print(await nse.cm_hist_short_selling("RELIANCE"))
#     print(await nse.cm_hist_short_selling("DSSL", "1Y"))
#     print(await nse.cm_hist_short_selling("DSSL", "01-10-2025"))
#     print(await nse.cm_hist_short_selling("DSSL", "01-10-2025", "17-10-2025"))

# -- Historical Equity Price Band --------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_hist_eq_price_band())
#     print(await nse.cm_hist_eq_price_band("1W"))
#     print(await nse.cm_hist_eq_price_band("1M"))
#     print(await nse.cm_hist_eq_price_band("01-10-2025"))
#     print(await nse.cm_hist_eq_price_band("15-10-2025", "17-10-2025"))
#     print(await nse.cm_hist_eq_price_band("WEWIN"))
#     print(await nse.cm_hist_eq_price_band("WEWIN", "1Y"))
#     print(await nse.cm_hist_eq_price_band("DSSL", "01-10-2025"))
#     print(await nse.cm_hist_eq_price_band("DSSL", "01-10-2025", "17-10-2025"))


# ----------------------------------------------------------------------------------
# + EQUITY -- EOD DATA                                                             +
# ----------------------------------------------------------------------------------

# -- FII / DII Activity ------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_fii_dii_activity())                      # Latest FII + DII net buy/sell

# -- Market Activity Report --------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_market_activity_report())                # Daily market summary

# -- Bhavcopy with Delivery -------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_bhavcopy_with_delivery("17-10-2025"))    # Full bhavcopy + delivery %

# -- Equity Bhavcopy ---------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_equity_bhavcopy("17-10-2025"))

# -- 52-Week High / Low (EOD archive) ---------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_52_week_high_low("17-10-2025"))

# -- Bulk Deals (EOD archive) -----------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_bulk_deal())
#     print(await nse.cm_eod_bulk_deal("17-10-2025"))

# -- Block Deals (EOD archive) ----------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_block_deal())
#     print(await nse.cm_eod_block_deal("17-10-2025"))

# -- Short Selling (EOD) -----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_shortselling("17-10-2025"))

# -- Surveillance Indicator --------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_surveillance_indicator("17-10-25"))      # Date: DD-MM-YY

# -- Series Changes ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_series_change("17-10-2025"))

# -- Equity Band Changes -----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_eq_band_changes("17-10-2025"))

# -- Equity Price Band (EOD) -------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_eq_price_band("17-10-2025"))

# -- PE Ratio (EOD) ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_pe_ratio("17-10-25"))                    # Date: DD-MM-YY

# -- Market Cap (EOD) --------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_mcap("17-10-25"))

# -- Name Changes -----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_eq_name_change())

# -- Symbol Changes ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_eod_eq_symbol_change())


# ----------------------------------------------------------------------------------
# + CAPITAL MARKET -- BUSINESS GROWTH & STATS                                      +
# ----------------------------------------------------------------------------------

# -- Daily / Monthly / Yearly Business Growth -------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_dmy_biz_growth())                            # Current month daily (default)
#     print(await nse.cm_dmy_biz_growth("daily"))
#     print(await nse.cm_dmy_biz_growth("monthly"))                   # Current FY monthly
#     print(await nse.cm_dmy_biz_growth("yearly"))                    # All years
#     print(await nse.cm_dmy_biz_growth("daily",   "OCT", 2025))      # Oct 2025 daily
#     print(await nse.cm_dmy_biz_growth("daily",   "JAN", 2024))
#     print(await nse.cm_dmy_biz_growth("monthly", 2025))             # FY 2025 monthly
#     print(await nse.cm_dmy_biz_growth("monthly", 2024))

# -- Monthly Settlement Report -----------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_monthly_settlement_report())                 # Current FY
#     print(await nse.cm_monthly_settlement_report("1Y"))
#     print(await nse.cm_monthly_settlement_report("2Y"))
#     print(await nse.cm_monthly_settlement_report("3Y"))
#     print(await nse.cm_monthly_settlement_report("2024", 2026))     # FY 2024->2026 range
#     print(await nse.cm_monthly_settlement_report(from_year=2023, to_year=2025))

# -- Monthly Most Active Equity ----------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_monthly_most_active_equity())

# -- Historical Advances / Declines ------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.historical_advances_decline())                  # Previous month, month-wise
#     print(await nse.historical_advances_decline("2025"))            # Full year month-wise
#     print(await nse.historical_advances_decline("2024"))
#     print(await nse.historical_advances_decline("Day_wise",   "OCT", 2025))
#     print(await nse.historical_advances_decline("Day_wise",   "JAN", 2024))
#     print(await nse.historical_advances_decline("Month_wise", 2024))
#     print(await nse.historical_advances_decline(mode="Day_wise", month=10, year=2025))


# ----------------------------------------------------------------------------------
# + F&O -- LIVE                                                                    +
# ----------------------------------------------------------------------------------

# -- Full Symbol F&O Data  (JSON) --------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.symbol_full_fno_live_data("TCS"))               # All strikes, all expiries
#     print(await nse.symbol_full_fno_live_data("NIFTY"))

# -- Most Active Calls / Puts / Contracts by OI ------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI("TCS",   "CE"))
#     print(await nse.symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI("TCS",   "PE"))
#     print(await nse.symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI("NIFTY", "CE"))

# -- F&O Contract Intraday Chart  (JSON) -------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.identifier_based_fno_contracts_live_chart_data("OPTSTKTCS30-12-2025CE3300.00"))
#     print(await nse.identifier_based_fno_contracts_live_chart_data("FUTIDXNIFTY26-06-2025XX00000.00"))

# -- Futures Live Data -------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_futures_data("RELIANCE"))
#     print(await nse.fno_live_futures_data("TCS"))
#     print(await nse.fno_live_futures_data("NIFTY"))
#     print(await nse.fno_live_futures_data("BANKNIFTY"))

# -- Top 20 Contracts --------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_top_20_derivatives_contracts("NIFTY"))
#     print(await nse.fno_live_top_20_derivatives_contracts("BANKNIFTY"))

# -- Most Active Futures by Volume ------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_most_active_futures_contracts())

# -- Most Active Options  (mode x type x metric) ----------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_most_active("Index", "Call", "Volume"))
#     print(await nse.fno_live_most_active("Index", "Call", "Value"))
#     print(await nse.fno_live_most_active("Index", "Put",  "Volume"))
#     print(await nse.fno_live_most_active("Index", "Put",  "Value"))
#     print(await nse.fno_live_most_active("Stock", "Call", "Volume"))
#     print(await nse.fno_live_most_active("Stock", "Call", "Value"))
#     print(await nse.fno_live_most_active("Stock", "Put",  "Volume"))
#     print(await nse.fno_live_most_active("Stock", "Put",  "Value"))

# -- Most Active Contracts ---------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_most_active_contracts_by_oi())
#     print(await nse.fno_live_most_active_contracts_by_volume())
#     print(await nse.fno_live_most_active_options_contracts_by_volume())

# -- Most Active Underlying --------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_most_active_underlying())

# -- Change in Open Interest -------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_change_in_oi())
#     # Shows: Rise OI+Price  |  Rise OI+Slide Price  |  Slide OI+Rise Price  |  Slide OI+Price

# -- Price vs OI -------------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_oi_vs_price())

# -- Expiry Dates (Raw JSON) -------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_expiry_dates_raw())                         # NIFTY all expiries dict
#     print(await nse.fno_expiry_dates_raw("TCS"))

# -- Expiry Dates (Clean) ----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_expiry_dates())                             # NIFTY -- all expiries DataFrame
#     print(await nse.fno_expiry_dates("TCS"))
#     print(await nse.fno_expiry_dates("NIFTY",     "Current"))       # -> "28-10-2025"
#     print(await nse.fno_expiry_dates("NIFTY",     "Next Week"))     # -> "04-11-2025"
#     print(await nse.fno_expiry_dates("NIFTY",     "Month"))         # -> "25-11-2025"
#     print(await nse.fno_expiry_dates("NIFTY",     "All"))           # -> ["28-10-2025", "04-11-2025", ...]
#     print(await nse.fno_expiry_dates("TCS",       "Current"))
#     print(await nse.fno_expiry_dates("TCS",       "Month"))
#     print(await nse.fno_expiry_dates("BANKNIFTY", "Current"))

# -- Option Chain ------------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_option_chain("NIFTY"))                 # Default: nearest expiry, full mode
#     print(await nse.fno_live_option_chain("BANKNIFTY"))
#     print(await nse.fno_live_option_chain("RELIANCE"))
#     print(await nse.fno_live_option_chain("TCS"))
#     print(await nse.fno_live_option_chain("NIFTY",    expiry_date="27-Jan-2026"))
#     print(await nse.fno_live_option_chain("RELIANCE", expiry_date="27-Jan-2026"))
#     print(await nse.fno_live_option_chain("NIFTY",    oi_mode="compact"))         # Compact: CE+PE summary
#     print(await nse.fno_live_option_chain("RELIANCE", oi_mode="compact"))
#     # No-cache -- real-time chain:
#     async with nse.no_cache():
#         df = await nse.fno_live_option_chain("NIFTY")

# -- Option Chain Raw  (JSON) ------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_option_chain_raw("NIFTY"))
#     print(await nse.fno_live_option_chain_raw("TCS"))

# -- Active Contracts --------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_active_contracts("NIFTY"))
#     print(await nse.fno_live_active_contracts("NIFTY",    expiry_date="27-Jan-2026"))
#     print(await nse.fno_live_active_contracts("RELIANCE"))
#     print(await nse.fno_live_active_contracts("RELIANCE", expiry_date="27-Jan-2026"))
#     print(await nse.fno_live_active_contracts("TCS"))

# -- F&O Lot Sizes -----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_eom_lot_size())                             # All symbols
#     print(await nse.fno_eom_lot_size("NIFTY"))


# ----------------------------------------------------------------------------------
# + F&O -- EOD DATA                                                                +
# ----------------------------------------------------------------------------------

# -- F&O Bhavcopy -----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_eod_bhav_copy("17-10-2025"))                # Full F&O bhavcopy CSV

# -- FII Statistics ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_eod_fii_stats("17-10-2025"))

# -- Top 10 Futures ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_eod_top10_fut("17-10-2025"))                # Returns list of rows

# -- Top 20 Options ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_eod_top20_opt("31-12-2025"))

# -- Securities in Ban ------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_eod_sec_ban("17-10-2025"))

# -- MWPL (Market Wide Position Limit) --------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_eod_mwpl_3("17-10-2025"))

# -- Combined Open Interest --------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_eod_combine_oi("17-10-2025"))

# -- Participant-wise OI -----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_eod_participant_wise_oi("17-10-2025"))

# -- Participant-wise Volume -------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_eod_participant_wise_vol("17-10-2025"))


# ----------------------------------------------------------------------------------
# + F&O -- HISTORICAL PRICE & VOLUME                                               +
# ----------------------------------------------------------------------------------

# -- Futures Historical ------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.future_price_volume_data("NIFTY",     "Index Futures",  "OCT-25", "01-10-2025", "17-10-2025"))
#     print(await nse.future_price_volume_data("BANKNIFTY", "Index Futures",  "OCT-25"))
#     print(await nse.future_price_volume_data("ITC",        "Stock Futures", "OCT-25", "04-10-2025"))
#     print(await nse.future_price_volume_data("BANKNIFTY", "Index Futures",  "3M"))
#     print(await nse.future_price_volume_data("BANKNIFTY", "Index Futures",  "6M"))
#     print(await nse.future_price_volume_data("NIFTY",     "Index Futures",  "NOV-24"))
#     print(await nse.future_price_volume_data("TCS",        "Stock Futures", "1M"))
#     # Expiry format: MON-YY e.g. "OCT-25", "NOV-24"

# -- Options Historical ------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.option_price_volume_data("NIFTY",     "Index Options", "01-10-2025",  "17-10-2025", expiry="20-10-2025"))
#     print(await nse.option_price_volume_data("TCS",        "Stock Options","3000", "CE",  "01-02-2026", "06-02-2026", expiry="24-02-2026"))
#     print(await nse.option_price_volume_data("BANKNIFTY", "Index Options", "47000",       "01-10-2025", "17-10-2025", expiry="28-10-2025"))
#     print(await nse.option_price_volume_data("ITC",        "Stock Options","04-10-2025",  expiry="28-10-2025"))
#     print(await nse.option_price_volume_data("BANKNIFTY", "Index Options", "3M"))
#     print(await nse.option_price_volume_data("BANKNIFTY", "Index Options", "6M"))
#     print(await nse.option_price_volume_data("NIFTY",     "Index Options", "PE",          "01-10-2025", expiry="28-10-2025"))
#     print(await nse.option_price_volume_data("NIFTY",     "Index Options", "CE",          "01-10-2025", expiry="28-10-2025"))


# ----------------------------------------------------------------------------------
# + F&O -- BUSINESS GROWTH & SETTLEMENT                                           +
# ----------------------------------------------------------------------------------

# -- F&O Business Growth -----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_dmy_biz_growth())                           # Monthly, current year
#     print(await nse.fno_dmy_biz_growth("monthly"))
#     print(await nse.fno_dmy_biz_growth("yearly"))
#     print(await nse.fno_dmy_biz_growth("daily",   month="OCT", year=2025))
#     print(await nse.fno_dmy_biz_growth("daily",   month="JAN", year=2024))
#     print(await nse.fno_dmy_biz_growth("monthly", year=2024))

# -- F&O Monthly Settlement Report ------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_monthly_settlement_report())                # Current FY
#     print(await nse.fno_monthly_settlement_report(from_year=2024, to_year=2025))
#     print(await nse.fno_monthly_settlement_report(from_year=2023, to_year=2026))


# ----------------------------------------------------------------------------------
# + SEBI                                                                          +
# ----------------------------------------------------------------------------------

# -- SEBI Circulars ----------------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.sebi_circulars())                               # Default: last 1W
#     print(await nse.sebi_circulars("1W"))
#     print(await nse.sebi_circulars("2W"))
#     print(await nse.sebi_circulars("1M"))
#     print(await nse.sebi_circulars("3M"))
#     print(await nse.sebi_circulars("6M"))
#     print(await nse.sebi_circulars("1Y"))
#     print(await nse.sebi_circulars("2Y"))
#     print(await nse.sebi_circulars("01-10-2025", "10-10-2025"))     # Date range (DD-MM-YYYY)
#     print(await nse.sebi_circulars("01-10-2025"))                   # From date -> today

# -- SEBI Paged Circulars ----------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.sebi_data())                                    # Page 1 (latest)
#     print(await nse.sebi_data(pages=3))                             # Fetch 3 pages
#     print(await nse.sebi_data(pages=5))


# ----------------------------------------------------------------------------------
# + ASYNC-ONLY ADDITIONS                                                          +
# ----------------------------------------------------------------------------------

# -- Live Equity Market by Index ---------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_equity_market())                        # NIFTY 50 default
#     print(await nse.cm_live_equity_market("NIFTY BANK"))
#     print(await nse.cm_live_equity_market("NIFTY IT"))

# -- Single Index Snapshot ---------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.index_live_specific_data("NIFTY 50"))
#     print(await nse.index_live_specific_data("NIFTY BANK"))

# -- F&O Derivatives Snapshot ------------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_live_derivatives_snapshot("NIFTY"))

# -- Live Bulk Deals (JSON API) ----------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_bulk_deals("1M"))
#     print(await nse.cm_live_bulk_deals("01-01-2025", "31-03-2025"))

# -- Live Block Deals (JSON API) ---------------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_block_deals("1M"))

# -- FII / DII Trades (Historical JSON) -------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.fno_eod_fii_dii("1M"))
#     print(await nse.fno_eod_fii_dii("01-01-2025", "31-03-2025"))

# -- Annual Reports (Corporate Filing) --------------------------------------------
# async with AsyncNse() as nse:
#     print(await nse.cm_live_hist_annual_reports("TCS", "3M"))
#     print(await nse.cm_live_hist_annual_reports("RELIANCE", "1Y"))


# ----------------------------------------------------------------------------------
# + POWER PATTERNS -- ADVANCED ASYNC RECIPES                                       +
# ----------------------------------------------------------------------------------

# -- Pattern 1: Morning dashboard -- all concurrent ---------------------------------
# async with AsyncNseSession(max_per_second=5, cache_ttl=60) as nse:
#     market, turnover, stats, gift, preopen, vix = await asyncio.gather(
#         nse.nse_market_status("Market Status"),
#         nse.nse_live_market_turnover(),
#         nse.cm_live_market_statistics(),
#         nse.cm_live_gifty_nifty(),
#         nse.pre_market_info("NIFTY 50"),
#         nse.india_vix_historical_data("5D"),
#     )

# -- Pattern 2: Build a watchlist with order books ---------------------------------
# async with AsyncNse() as nse:
#     watchlist = ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK"]
#     data = await nse.watchlist_snapshot(watchlist)

# -- Pattern 3: Option chain scanner ----------------------------------------------
# async with AsyncNse() as nse:
#     async with nse.no_cache():                          # Always live
#         chain = await nse.fno_live_option_chain("NIFTY", oi_mode="full")
#     # Filter high OI strikes
#     high_oi = chain[chain["CALLS_OI"] > chain["CALLS_OI"].quantile(0.9)]

# -- Pattern 4: Bulk historical download -- all concurrent -------------------------
# async with AsyncNse(max_per_second=3, max_per_minute=60) as nse:
#     symbols = (await nse.nse_eom_fno_full_list(list_only=True))[:10]
#     results = await asyncio.gather(
#         *[nse.cm_hist_security_wise_data(sym, "1Y") for sym in symbols],
#         return_exceptions=True,
#     )
#     for sym, df in zip(symbols, results):
#         if isinstance(df, Exception): continue
#         df.to_csv(f"{sym}_1Y.csv", index=False)

# -- Pattern 5: Save historical index to CSV ---------------------------------------
# async with AsyncNse() as nse:
#     data = await nse.index_historical_data("NIFTY 50", "2Y")
#     print(data.tail())
#     data.to_csv("NIFTY50_2Y.csv", index=False)
#     print("Saved.")

# -- Pattern 6: Multi-index PE/PB comparison --------------------------------------
# import pandas as pd
# async with AsyncNse() as nse:
#     indices  = ["NIFTY 50", "NIFTY BANK", "NIFTY IT", "NIFTY MIDCAP 50"]
#     dfs = await asyncio.gather(
#         *[nse.index_pe_pb_div_historical_data(idx, "1Y") for idx in indices]
#     )
#     combined = pd.concat(
#         [df.assign(Index=idx) for df, idx in zip(dfs, indices) if df is not None],
#         ignore_index=True,
#     )
#     print(combined.groupby("Index")[["P/E","P/B","Div Yield%"]].tail(1))

# -- Pattern 7: Custom subclass with @nse_api on your own endpoints ----------------
# from NseKitAsync import nse_api, CachePolicy
# import pandas as pd
#
# class ExtendedNse(AsyncNse):
#     @nse_api(ttl=30.0, cache=CachePolicy.READWRITE)
#     async def live_pe_band(self, symbol: str) -> dict | None:
#         info = await self.cm_live_equity_info(symbol)
#         if not info:
#             return None
#         return {
#             "symbol": symbol,
#             "ltp":    info["LastTradedPrice"],
#             "note":   "Custom endpoint with @nse_api caching"
#         }
#
# async with ExtendedNse() as nse:
#     print(await nse.live_pe_band("TCS"))               # Cached for 30s
#     print(nse.cache_stats())

# -- Pattern 8: Rate-aware batch download -----------------------------------------
# async with AsyncNse(max_per_second=2, max_per_minute=40, min_gap=0.5) as nse:
#     symbols = (await nse.nse_eom_fno_full_list(list_only=True))[:10]
#     results = await asyncio.gather(
#         *[nse.cm_hist_security_wise_data(sym, "1Y") for sym in symbols],
#         return_exceptions=True,
#     )
#     for sym, df in zip(symbols, results):
#         if isinstance(df, Exception): continue
#         df.to_csv(f"{sym}_1Y.csv", index=False)
#     print(nse.rate_limiter.stats())

# -- Pattern 9: Corporate action calendar filter pipeline --------------------------
# async with AsyncNse() as nse:
#     actions   = await nse.cm_live_hist_corporate_action("3M")
#     dividends = actions[actions["PURPOSE"].str.contains("Dividend", case=False, na=False)]
#     upcoming  = dividends.sort_values("EX-DATE")
#     print(upcoming[["SYMBOL","COMPANY NAME","PURPOSE","EX-DATE","RECORD DATE"]].head(20))

# -- Pattern 10: VIX + Index correlation snapshot ---------------------------------
# import pandas as pd
# async with AsyncNse() as nse:
#     vix, nifty = await asyncio.gather(
#         nse.india_vix_historical_data("1M"),
#         nse.index_historical_data("NIFTY 50", "1M"),
#     )
#     merged = pd.merge(
#         vix[["Date","Close Price"]].rename(columns={"Close Price":"VIX"}),
#         nifty[["Date","Close"]],
#         on="Date",
#     )
#     print(merged.corr())
