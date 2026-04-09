# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║           NseKitAsync v6.0  ─  Complete Usage Guide (131 methods)           ║
# ║     True async I/O · Concurrent fetching · 100% original API coverage       ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

import asyncio
from NseKitAsync import (
    AsyncNse,
    AsyncNseSession,
    AsyncNseQueryBuilder,
    sync_fetch,
    Period,
    CachePolicy,
)


# ══════════════════════════════════════════════════════════════════════════════
# ▌ CLIENT CREATION                                                            ▌
# ══════════════════════════════════════════════════════════════════════════════

# 1. Standard async context manager (recommended)
# async with AsyncNse() as nse:
#     df = await nse.index_live_all_indices_data()

# 2. Custom rate limits + verbose debug logging
# async with AsyncNse(max_per_second=3, max_per_minute=60, min_gap=0.3, verbose=True) as nse:
#     ...

# 3. AsyncNseSession factory (explicit teardown guarantee)
# async with AsyncNseSession(max_per_second=5, cache_ttl=30, cache_size=1024) as nse:
#     df = await nse.nse_market_status()

# 4. sync wrapper — call from non-async code / Jupyter
# async def _run():
#     async with AsyncNse() as nse:
#         return await nse.index_live_all_indices_data()
# df = sync_fetch(_run)


# ══════════════════════════════════════════════════════════════════════════════
# ▌ CACHE · RATE LIMITER · NO-CACHE BLOCK                                     ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     nse.rate_limiter_stats()          # {"total_calls":N, "total_waited_secs":..., ...}
#     nse.cache_stats()                 # {"size": N, "max_size": 512}
#     await nse.clear_cache()           # wipe all cached responses
#
#     async with nse.no_cache():        # bypass cache for this block
#         df = await nse.nse_market_status()


# ══════════════════════════════════════════════════════════════════════════════
# ▌ FLUENT QUERY BUILDER                                                       ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     df = await (AsyncNseQueryBuilder(nse)
#                 .symbol("RELIANCE")
#                 .period("3M")
#                 .cache_policy(CachePolicy.READWRITE)
#                 .fetch(nse.cm_live_hist_insider_trading))
#
#     df = await (nse.query
#                 .symbol("INFY")
#                 .date_range("01-01-2025", "31-03-2025")
#                 .cache_policy(CachePolicy.NONE)
#                 .fetch(nse.cm_live_hist_board_meetings))
#
#     df = await (nse.query
#                 .period("1M")
#                 .filter("Dividend")
#                 .fetch(nse.cm_live_hist_corporate_action))


# ══════════════════════════════════════════════════════════════════════════════
# ▌ CONCURRENT BATCH  — THE KEY ASYNC ADVANTAGE                               ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     # All 5 fire at the same time — not one by one
#     market, turnover, indices, vix, preopen = await asyncio.gather(
#         nse.nse_market_status("Market Status"),
#         nse.nse_live_market_turnover(),
#         nse.index_live_all_indices_data(),
#         nse.india_vix_historical_data(period="5D"),
#         nse.pre_market_info("All"),
#     )
#
#     # fetch_many() convenience helper
#     results = await nse.fetch_many([
#         nse.nse_market_status,
#         nse.nse_live_market_turnover,
#         nse.index_live_all_indices_data,
#     ])
#
#     # Built-in parallel morning dashboard
#     data = await nse.morning_dashboard()
#     # keys: "market", "turnover", "indices", "vix", "preopen"
#
#     # Concurrent watchlist
#     snap = await nse.watchlist_snapshot(["RELIANCE","TCS","INFY","HDFCBANK"])


# ══════════════════════════════════════════════════════════════════════════════
# ▌ NSE — MARKET STATUS & HOLIDAYS                                            ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.nse_market_status()
#     await nse.nse_market_status("Market Status")
#     await nse.nse_market_status("Mcap")
#     await nse.nse_market_status("Nifty50")
#     await nse.nse_market_status("Gift Nifty")
#     await nse.nse_market_status("All")
#
#     await nse.nse_is_market_open()
#     await nse.nse_is_market_open("Capital Market")
#     await nse.nse_is_market_open("Currency")
#     await nse.nse_is_market_open("Commodity")
#     await nse.nse_is_market_open("Debt")
#
#     await nse.nse_trading_holidays()
#     await nse.nse_trading_holidays(list_only=True)
#     await nse.nse_clearing_holidays()
#     await nse.nse_clearing_holidays(list_only=True)
#     await nse.is_nse_trading_holiday()
#     await nse.is_nse_trading_holiday("25-Dec-2025")
#     await nse.is_nse_clearing_holiday("25-Dec-2025")
#
#     await nse.nse_live_market_turnover()
#     await nse.nse_reference_rates()
#     await nse.nse_live_hist_circulars()
#     await nse.nse_live_hist_circulars("01-01-2025", "31-01-2025")
#     await nse.nse_live_hist_circulars(filter="Trading")
#     await nse.nse_live_hist_press_releases()
#     await nse.nse_live_hist_press_releases("01-01-2025", "31-01-2025", filter="Equity")
#     await nse.nse_eod_top10_nifty50("25-10-25")


# ══════════════════════════════════════════════════════════════════════════════
# ▌ NSE — LISTS & REGISTRIES                                                  ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.nse_6m_nifty_50()
#     await nse.nse_6m_nifty_50(list_only=True)
#     await nse.nse_6m_nifty_500()
#     await nse.nse_6m_nifty_500(list_only=True)
#     await nse.nse_eod_equity_full_list()
#     await nse.nse_eod_equity_full_list(list_only=True)
#     await nse.nse_eom_fno_full_list()
#     await nse.nse_eom_fno_full_list(mode="index")
#     await nse.nse_eom_fno_full_list(list_only=True)
#     await nse.list_of_indices()
#     await nse.list_of_indices(as_dataframe=True)
#     await nse.state_wise_registered_investors()


# ══════════════════════════════════════════════════════════════════════════════
# ▌ IPO                                                                       ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.ipo_current()
#     await nse.ipo_preopen()
#     await nse.ipo_tracker_summary()
#     await nse.ipo_tracker_summary(filter="MAINBOARD")
#     await nse.ipo_tracker_summary(filter="SME")


# ══════════════════════════════════════════════════════════════════════════════
# ▌ PRE-OPEN MARKET                                                           ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.pre_market_nifty_info()
#     await nse.pre_market_nifty_info("NIFTY 50")
#     await nse.pre_market_nifty_info("Nifty Bank")
#     await nse.pre_market_all_nse_adv_dec_info()
#     await nse.pre_market_all_nse_adv_dec_info("All")
#     await nse.pre_market_info()
#     await nse.pre_market_info("NIFTY 50")
#     await nse.pre_market_info("Securities in F&O")
#     await nse.pre_market_derivatives_info()
#     await nse.pre_market_derivatives_info("Stock Futures")


# ══════════════════════════════════════════════════════════════════════════════
# ▌ INDEX — LIVE                                                              ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.index_live_all_indices_data()
#     await nse.index_live_specific_data("NIFTY 50")
#     await nse.index_live_indices_stocks_data()
#     await nse.index_live_indices_stocks_data("NIFTY BANK")
#     await nse.index_live_indices_stocks_data("NIFTY 50", list_only=True)
#     await nse.index_live_nifty_50_returns()
#     await nse.index_live_contribution()
#     await nse.index_live_contribution("NIFTY BANK", "Full")
#     await nse.index_live_contribution("First Five")
#     await nse.cm_live_market_statistics()
#     await nse.cm_live_gifty_nifty()


# ══════════════════════════════════════════════════════════════════════════════
# ▌ INDEX — HISTORICAL                                                        ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.index_historical_data("NIFTY 50", "2Y")
#     await nse.index_historical_data("NIFTY BANK", "01-01-2024", "31-12-2024")
#     await nse.india_vix_historical_data("6M")
#     await nse.india_vix_historical_data("01-01-2025", "31-03-2025")
#     await nse.index_pe_pb_div_historical_data("NIFTY 50", "1Y")
#     await nse.index_eod_bhav_copy("25-10-2024")


# ══════════════════════════════════════════════════════════════════════════════
# ▌ CHARTS                                                                    ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.index_chart("NIFTY 50")
#     await nse.index_chart("NIFTY BANK", "3M")
#     await nse.stock_chart("RELIANCE")
#     await nse.stock_chart("TCS", "6M")
#     await nse.fno_chart("NIFTY")
#     await nse.fno_chart("BANKNIFTY", "1M")
#     await nse.india_vix_chart()
#     await nse.india_vix_chart("3M")
#     await nse.identifier_based_fno_contracts_live_chart_data("NIFTY25JAN24FUT")


# ══════════════════════════════════════════════════════════════════════════════
# ▌ EQUITY — LIVE                                                             ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.cm_live_equity_info("RELIANCE")
#     await nse.cm_live_equity_price_info("TCS")
#     await nse.cm_live_equity_full_info("INFY")
#     await nse.cm_live_equity_market()
#     await nse.cm_live_equity_market("NIFTY BANK")
#     await nse.cm_live_52week_high()
#     await nse.cm_live_52week_low()
#     await nse.cm_live_block_deal()
#     await nse.cm_live_volume_spurts()
#     await nse.cm_live_most_active_equity_by_value()
#     await nse.cm_live_most_active_equity_by_vol()
#     await nse.cm_live_market_statistics()


# ══════════════════════════════════════════════════════════════════════════════
# ▌ EQUITY — HISTORICAL                                                       ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.cm_hist_security_wise_data("RELIANCE", "1Y")
#     await nse.cm_hist_security_wise_data("TCS", "01-01-2024", "31-12-2024")
#     await nse.cm_hist_bulk_deals("1M")
#     await nse.cm_hist_bulk_deals("01-01-2025", "31-01-2025")
#     await nse.cm_hist_bulk_deals(symbol="RELIANCE", period="3M")
#     await nse.cm_hist_block_deals("1M")
#     await nse.cm_hist_short_selling("1M")
#     await nse.cm_hist_eq_price_band("1M")
#     await nse.cm_hist_eq_price_band(symbol="RELIANCE", period="6M")


# ══════════════════════════════════════════════════════════════════════════════
# ▌ CORPORATE FILINGS                                                         ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.cm_live_hist_insider_trading("RELIANCE", "3M")
#     await nse.cm_live_hist_insider_trading("01-01-2025", "31-03-2025")
#     await nse.cm_live_hist_board_meetings("TCS", "3M")
#     await nse.cm_live_hist_board_meetings()
#     await nse.cm_live_hist_corporate_action("3M")
#     await nse.cm_live_hist_corporate_action("3M", filter="Dividend")
#     await nse.cm_live_hist_corporate_action(symbol="INFY", period="1Y")
#     await nse.cm_live_hist_corporate_announcement("RELIANCE", "1M")
#     await nse.cm_live_today_event_calendar()
#     await nse.cm_live_today_event_calendar("01-01-2025", "31-01-2025")
#     await nse.cm_live_upcoming_event_calendar()
#     await nse.cm_live_hist_Shareholder_meetings("RELIANCE", "3M")
#     await nse.cm_live_hist_qualified_institutional_placement("3M")
#     await nse.cm_live_hist_qualified_institutional_placement("In-Principle", "6M")
#     await nse.cm_live_hist_qualified_institutional_placement("Listing Stage")
#     await nse.cm_live_hist_preferential_issue("3M")
#     await nse.cm_live_hist_right_issue("6M")
#     await nse.cm_live_voting_results()
#     await nse.cm_live_qtly_shareholding_patterns()
#     await nse.cm_live_hist_br_sr("RELIANCE")
#     await nse.cm_live_hist_annual_reports("TCS", "3M")
#     await nse.recent_annual_reports()
#     await nse.quarterly_financial_results("INFY")


# ══════════════════════════════════════════════════════════════════════════════
# ▌ CM EOD DATA                                                               ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.cm_eod_fii_dii_activity()
#     await nse.cm_eod_market_activity_report()
#     await nse.cm_eod_equity_bhavcopy("25-10-2024")
#     await nse.cm_eod_bhavcopy_with_delivery("25-10-2024")
#     await nse.cm_eod_52_week_high_low("25-10-2024")
#     await nse.cm_eod_bulk_deal()
#     await nse.cm_eod_block_deal()
#     await nse.cm_eod_shortselling("25-10-2024")
#     await nse.cm_eod_surveillance_indicator("25-10-2024")
#     await nse.cm_eod_series_change("25-10-2024")
#     await nse.cm_eod_eq_band_changes("25-10-2024")
#     await nse.cm_eod_eq_price_band("25-10-2024")
#     await nse.cm_eod_pe_ratio("25-10-24")
#     await nse.cm_eod_mcap("25-10-24")
#     await nse.cm_eod_eq_name_change()
#     await nse.cm_eod_eq_symbol_change()


# ══════════════════════════════════════════════════════════════════════════════
# ▌ CM BUSINESS GROWTH & STATS                                                ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.cm_dmy_biz_growth()
#     await nse.cm_dmy_biz_growth("monthly")
#     await nse.cm_dmy_biz_growth("yearly")
#     await nse.cm_dmy_biz_growth("daily", month="OCT", year=2024)
#     await nse.cm_monthly_settlement_report()
#     await nse.cm_monthly_settlement_report(from_year=2022, to_year=2024)
#     await nse.cm_monthly_most_active_equity()
#     await nse.historical_advances_decline()
#     await nse.historical_advances_decline("Month_wise", year=2024)
#     await nse.historical_advances_decline("Day_wise", "OCT", 2024)


# ══════════════════════════════════════════════════════════════════════════════
# ▌ F&O — LIVE                                                                ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.fno_live_option_chain("NIFTY")
#     await nse.fno_live_option_chain("BANKNIFTY", oi_mode="compact")
#     await nse.fno_live_option_chain("TCS")
#     await nse.fno_live_option_chain_raw("NIFTY")
#     await nse.fno_live_futures_data("NIFTY")
#     await nse.fno_live_futures_data("BANKNIFTY")
#     await nse.symbol_full_fno_live_data("NIFTY")
#     await nse.fno_live_derivatives_snapshot("NIFTY")
#     await nse.fno_live_active_contracts("NIFTY")
#     await nse.fno_live_active_contracts("RELIANCE", expiry_date="27-Feb-2025")
#     await nse.fno_live_top_20_derivatives_contracts("NIFTY")
#     await nse.fno_live_most_active()
#     await nse.fno_live_most_active("Index", "Call", "Volume")
#     await nse.fno_live_most_active("stocks", "Put", "Value")
#     await nse.fno_live_most_active_futures_contracts()
#     await nse.fno_live_most_active_contracts_by_oi()
#     await nse.fno_live_most_active_contracts_by_volume()
#     await nse.fno_live_most_active_options_contracts_by_volume()
#     await nse.fno_live_most_active_underlying()
#     await nse.symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI("NIFTY", "CE")
#     await nse.symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI("BANKNIFTY", "PE")
#     await nse.fno_live_change_in_oi()
#     await nse.fno_live_oi_vs_price()
#     await nse.fno_expiry_dates("NIFTY")
#     await nse.fno_expiry_dates("NIFTY", label_filter="Current")
#     await nse.fno_expiry_dates("NIFTY", label_filter="All")
#     await nse.fno_expiry_dates("NIFTY", label_filter="Month")
#     await nse.fno_expiry_dates_raw("NIFTY")


# ══════════════════════════════════════════════════════════════════════════════
# ▌ F&O — HISTORICAL                                                          ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.future_price_volume_data("NIFTY",     "Index Futures",  "3M")
#     await nse.future_price_volume_data("BANKNIFTY", "Index Futures",  "6M")
#     await nse.future_price_volume_data("TCS",        "Stock Futures", "1M")
#     await nse.option_price_volume_data("NIFTY",     "Index Options",  "3M")
#     await nse.option_price_volume_data("BANKNIFTY", "Index Options",  "6M")
#     await nse.option_price_volume_data("TCS", "Stock Options", "3000", "CE",
#                                         "01-02-2025", "28-02-2025", expiry="27-02-2025")
#     await nse.fno_eod_participant_wise_oi("17-10-2024")
#     await nse.fno_eod_participant_wise_vol("17-10-2024")
#     await nse.fno_dmy_biz_growth()
#     await nse.fno_dmy_biz_growth("monthly")
#     await nse.fno_dmy_biz_growth("yearly")
#     await nse.fno_dmy_biz_growth("daily", month="OCT", year=2024)
#     await nse.fno_monthly_settlement_report()
#     await nse.fno_monthly_settlement_report(from_year=2023, to_year=2025)


# ══════════════════════════════════════════════════════════════════════════════
# ▌ F&O — EOD DATA                                                            ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.fno_eod_bhav_copy("25-10-2024")
#     await nse.fno_eod_fii_stats("25-10-2024")
#     await nse.fno_eod_top10_fut("25-10-2024")
#     await nse.fno_eod_top20_opt("25-10-2024")
#     await nse.fno_eod_sec_ban("25-10-2024")
#     await nse.fno_eod_mwpl_3("25-10-2024")
#     await nse.fno_eod_combine_oi("25-10-2024")
#     await nse.fno_eod_participant_wise_oi("25-10-2024")
#     await nse.fno_eod_participant_wise_vol("25-10-2024")
#     await nse.fno_eom_lot_size()
#     await nse.fno_eom_lot_size("NIFTY")
#
#     # Static helpers (no await needed)
#     AsyncNse.clean_mwpl_data(df)
#     AsyncNse.detect_excel_format(BytesIO(raw_bytes))
#     AsyncNse._extract_csv_from_zip(zip_bytes)


# ══════════════════════════════════════════════════════════════════════════════
# ▌ SEBI                                                                      ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.sebi_circulars()
#     await nse.sebi_circulars("1M")
#     await nse.sebi_circulars("3M")
#     await nse.sebi_circulars("01-10-2024", "31-10-2024")
#     await nse.sebi_data()
#     await nse.sebi_data(pages=3)


# ══════════════════════════════════════════════════════════════════════════════
# ▌ MISC                                                                      ▌
# ══════════════════════════════════════════════════════════════════════════════

# async with AsyncNse() as nse:
#     await nse.html_tables("https://www.nseindia.com/some-page")
#     await nse.html_tables("https://...", show_tables=True, output="df")


# ══════════════════════════════════════════════════════════════════════════════
# ▌ POWER PATTERNS                                                            ▌
# ══════════════════════════════════════════════════════════════════════════════

# ── Pattern 1: Morning dashboard (5 calls concurrent) ────────────────────────
# async def morning():
#     async with AsyncNse() as nse:
#         data = await nse.morning_dashboard()
#         print("Market:\n",  data["market"])
#         print("Turnover:\n", data["turnover"])
# asyncio.run(morning())


# ── Pattern 2: Option chain scanner ─────────────────────────────────────────
# async def scan_oi():
#     async with AsyncNse() as nse:
#         async with nse.no_cache():
#             chain = await nse.fno_live_option_chain("NIFTY")
#         high_oi = chain[chain["OI"] > chain["OI"].quantile(0.9)]
#         print(high_oi[["StrikePrice","OptionType","OI","IV","LTP"]])
# asyncio.run(scan_oi())


# ── Pattern 3: Bulk historical download (all concurrent) ────────────────────
# async def bulk():
#     async with AsyncNse(max_per_second=3, max_per_minute=60) as nse:
#         symbols = (await nse.nse_eom_fno_full_list(list_only=True))[:20]
#         results = await asyncio.gather(
#             *[nse.cm_hist_security_wise_data(s, "1Y") for s in symbols],
#             return_exceptions=True,
#         )
#         for sym, df in zip(symbols, results):
#             if df is not None and not isinstance(df, Exception):
#                 df.to_csv(f"{sym}_1Y.csv", index=False)
# asyncio.run(bulk())


# ── Pattern 4: Dividend calendar ────────────────────────────────────────────
# async def dividends():
#     async with AsyncNse() as nse:
#         df = await nse.cm_live_hist_corporate_action("3M", filter="Dividend")
#         print(df.sort_values("EX-DATE")[
#             ["SYMBOL","COMPANY NAME","PURPOSE","EX-DATE","RECORD DATE"]
#         ].head(20))
# asyncio.run(dividends())


# ── Pattern 5: VIX + Nifty correlation ──────────────────────────────────────
# async def vix_nifty():
#     import pandas as pd
#     async with AsyncNse() as nse:
#         vix, nifty = await asyncio.gather(
#             nse.india_vix_historical_data("1M"),
#             nse.index_historical_data("NIFTY 50", "1M"),
#         )
#     merged = pd.merge(
#         vix[["Date","Close Price"]].rename(columns={"Close Price":"VIX"}),
#         nifty[["Date","Close"]], on="Date",
#     )
#     print(merged.corr())
# asyncio.run(vix_nifty())


# ── Pattern 6: Custom subclass with @nse_api ─────────────────────────────────
# from NseKitAsync import nse_api, CachePolicy
# class MyNse(AsyncNse):
#     @nse_api(ttl=60.0, cache=CachePolicy.READWRITE)
#     async def live_pe_band(self, symbol: str) -> dict | None:
#         info = await self.cm_live_equity_info(symbol)
#         if not info: return None
#         return {"symbol": symbol, "ltp": info["LastTradedPrice"]}
#
# async def run():
#     async with MyNse() as nse:
#         print(await nse.live_pe_band("TCS"))
# asyncio.run(run())


# ── Pattern 7: sync wrapper (Jupyter / script) ───────────────────────────────
# async def get_data():
#     async with AsyncNse() as nse:
#         return await nse.index_live_all_indices_data()
# df = sync_fetch(get_data)
# print(df)


# ══════════════════════════════════════════════════════════════════════════════
# ▌ MIGRATION CHEAT SHEET: sync NseKit → async NseKitAsync                   ▌
# ══════════════════════════════════════════════════════════════════════════════
#
#  Sync (old)                              Async (new)
#  ─────────────────────────────────────── ───────────────────────────────────
#  nse = Nse()                             async with AsyncNse() as nse:
#  df = nse.method(args)                       df = await nse.method(args)
#
#  with NseSession() as nse:               async with AsyncNseSession() as nse:
#
#  for sym in syms:                        results = await asyncio.gather(
#      nse.cm_live_equity_info(sym)            *[nse.cm_live_equity_info(s) for s in syms]
#  # sequential, slow                     )  # concurrent, N× faster
#
#  nse.rate_limiter.stats()                nse.rate_limiter_stats()
#  nse.clear_cache()                       await nse.clear_cache()
#  with nse.no_cache(): ...                async with nse.no_cache(): ...
#  nse.query.symbol("X").fetch(fn)         await nse.query.symbol("X").fetch(fn)
#  Period.M3, CachePolicy.READWRITE        unchanged
# ══════════════════════════════════════════════════════════════════════════════
