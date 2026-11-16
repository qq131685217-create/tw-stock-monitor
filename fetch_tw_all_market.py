import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import requests

# 設定台灣時區 (UTC+8)
TW_TZ = timezone(timedelta(hours=8))

# 證交所 (TWSE) 與 櫃買中心 (TPEx) 官方開放資料 API（用於取得全市場 1,867 家名單、估值與盤後法人）
TWSE_DAY_ALL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TWSE_BWIBBU_URL = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
TWSE_T86_URL = "https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALLBUT0999"

TPEX_QUOTES_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
TPEX_PERATIO_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"
TPEX_3INST_URL = "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"

# 當下即時股價雙通道（Yahoo Finance 批量即時成交價 ＋ 證交所 MIS 即時行情專線）
TWSE_MIS_HOME_URL = "https://mis.twse.com.tw/stock/fibest.jsp"
TWSE_MIS_REALTIME_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
YAHOO_QUOTE_BATCH_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
YAHOO_CHART_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

OUTPUT_JSON = "tw_market_data.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Referer": "https://mis.twse.com.tw/stock/fibest.jsp",
    "Accept": "application/json, text/javascript, */*; q=0.01"
}


def safe_float(val, default=0.0):
    try:
        if val is None:
            return default
        s = str(val).replace(",", "").replace("+", "").replace("%", "").strip()
        if s in ("", "-", "--", "N/A", "除息", "除權"):
            return default
        return float(s)
    except Exception:
        return default


def safe_int(val, default=0):
    try:
        if val is None:
            return default
        s = str(val).replace(",", "").replace("+", "").strip()
        if s in ("", "-", "--"):
            return default
        return int(float(s))
    except Exception:
        return default


def fetch_json(url, timeout=25):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[警告] 抓取失敗 {url}: {e}")
        return None


def load_previous_data():
    """讀取既有的 tw_market_data.json 以保留全市場 1,867 檔 50 日 K 線歷史與法人籌碼"""
    if not os.path.exists(OUTPUT_JSON):
        return {}
    try:
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {item["code"]: item for item in data.get("stocks", [])}
    except Exception as e:
        print(f"[警告] 無法讀取舊檔 {OUTPUT_JSON}: {e}")
        return {}


def is_intraday_session(now_tw):
    """判斷是否為台股盤中交易時段 (週一至週五 09:00 ~ 13:35)"""
    if now_tw.weekday() >= 5:
        return False
    hhmm = now_tw.hour * 100 + now_tw.minute
    return 900 <= hhmm <= 1335


def fetch_all_1867_current_prices(stocks_map, now_tw):
    """
    核心模組：以「當下的股價為準」抓取全市場 1,867 檔最新即時成交價
    通道一：Yahoo Finance v7 批量即時報價（一次 150 檔，秒抓全市場 1,867 檔 regularMarketPrice）
    通道二：證交所 MIS 即時行情專線（mis.twse.com.tw）補齊剩餘標的
    通道三：Yahoo Finance v8 Chart 單檔即時現價補漏
    """
    symbols = []
    sym_to_code = {}
    for code, st in stocks_map.items():
        suffix = ".TW" if st["market"] == "TWSE" else ".TWO"
        sym = f"{code}{suffix}"
        symbols.append(sym)
        sym_to_code[sym] = code

    today_mmdd = now_tw.strftime("%m/%d")
    y_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }

    # --- 通道一：Yahoo Finance v7 批量即時成交價 ---
    yahoo_updated = 0
    batch_size = 150
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        url = f"{YAHOO_QUOTE_BATCH_URL}?symbols={','.join(batch)}"
        try:
            resp = requests.get(url, headers=y_headers, timeout=15)
            if resp.status_code == 200:
                results = resp.json().get("quoteResponse", {}).get("result", [])
                for item in results:
                    sym = str(item.get("symbol", "")).strip()
                    code = sym_to_code.get(sym)
                    if not code or code not in stocks_map:
                        continue
                    st = stocks_map[code]
                    live_p = safe_float(item.get("regularMarketPrice"))
                    prev_c = safe_float(item.get("regularMarketPreviousClose"), st["price"])
                    if live_p <= 0:
                        continue
                    if prev_c > 0 and not (prev_c * 0.895 <= live_p <= prev_c * 1.105):
                        continue

                    open_p = safe_float(item.get("regularMarketOpen"), live_p)
                    high_p = safe_float(item.get("regularMarketDayHigh"), live_p)
                    low_p = safe_float(item.get("regularMarketDayLow"), live_p)
                    vol_lots = max(1, round(safe_int(item.get("regularMarketVolume")) / 1000))
                    pct = round(((live_p - prev_c) / prev_c) * 100, 2) if prev_c > 0 else st["pct"]

                    st["price"] = round(live_p, 2)
                    st["pct"] = pct
                    st["_todayBar"] = {
                        "d": today_mmdd,
                        "o": round(open_p if open_p > 0 else live_p, 2),
                        "h": round(max(high_p, live_p), 2),
                        "l": round(min(low_p, live_p) if low_p > 0 else live_p, 2),
                        "c": round(live_p, 2),
                        "v": vol_lots
                    }
                    st["_liveUpdated"] = True
                    yahoo_updated += 1
        except Exception as e:
            print(f"[警告] Yahoo v7 批量即時報價批次 {i} 略過: {e}")
        time.sleep(0.15)

    # --- 通道二：證交所 MIS 即時行情專線補齊未更新標的 ---
    mis_updated = 0
    remaining_keys = []
    for code, st in stocks_map.items():
        if not st.get("_liveUpdated"):
            prefix = "tse" if st["market"] == "TWSE" else "otc"
            remaining_keys.append(f"{prefix}_{code}.tw")

    if remaining_keys:
        session = requests.Session()
        session.headers.update(HEADERS)
        try:
            session.get(TWSE_MIS_HOME_URL, timeout=12)
            time.sleep(0.3)
        except Exception:
            pass

        for i in range(0, len(remaining_keys), 60):
            batch = remaining_keys[i:i + 60]
            url = f"{TWSE_MIS_REALTIME_URL}?ex_ch={'|'.join(batch)}&json=1&delay=0&_={int(time.time() * 1000)}"
            try:
                resp = session.get(url, timeout=15)
                msg_arr = resp.json().get("msgArray") or []
                for item in msg_arr:
                    code = str(item.get("c", "")).strip()
                    if code not in stocks_map:
                        continue
                    st = stocks_map[code]
                    yesterday_close = safe_float(item.get("y"), st["price"])
                    last_trade = safe_float(item.get("z"))
                    if last_trade <= 0:
                        bid_str = str(item.get("b", "")).split("_")[0]
                        last_trade = safe_float(bid_str)
                    if last_trade <= 0:
                        continue
                    if yesterday_close > 0 and not (yesterday_close * 0.895 <= last_trade <= yesterday_close * 1.105):
                        continue

                    open_p = safe_float(item.get("o"), last_trade)
                    high_p = safe_float(item.get("h"), last_trade)
                    low_p = safe_float(item.get("l"), last_trade)
                    vol_lots = safe_int(item.get("v"))
                    pct = round(((last_trade - yesterday_close) / yesterday_close) * 100, 2) if yesterday_close > 0 else st["pct"]

                    st["price"] = round(last_trade, 2)
                    st["pct"] = pct
                    st["_todayBar"] = {
                        "d": today_mmdd,
                        "o": round(open_p if open_p > 0 else last_trade, 2),
                        "h": round(max(high_p, last_trade), 2),
                        "l": round(min(low_p, last_trade) if low_p > 0 else last_trade, 2),
                        "c": round(last_trade, 2),
                        "v": vol_lots
                    }
                    st["_liveUpdated"] = True
                    mis_updated += 1
            except Exception:
                pass
            time.sleep(0.2)

    # --- 通道三：Yahoo Finance v8 Chart 並發即時現價補漏（防 v7 Crumb 401 驗證阻擋） ---
    chart_updated = 0
    still_missing = [code for code, st in stocks_map.items() if not st.get("_liveUpdated")]
    if still_missing:
        def fetch_single_v8_chart(code):
            st = stocks_map[code]
            suffix = ".TW" if st["market"] == "TWSE" else ".TWO"
            url = f"{YAHOO_CHART_BASE_URL}/{code}{suffix}?interval=1d&range=2d"
            try:
                r = requests.get(url, headers=y_headers, timeout=10)
                if r.status_code != 200:
                    return None
                res_list = r.json().get("chart", {}).get("result") or []
                if not res_list:
                    return None
                meta = res_list[0].get("meta", {})
                live_p = safe_float(meta.get("regularMarketPrice"))
                prev_c = safe_float(meta.get("chartPreviousClose"), st["price"])
                if live_p <= 0:
                    return None
                if prev_c > 0 and not (prev_c * 0.895 <= live_p <= prev_c * 1.105):
                    return None
                open_p = safe_float(meta.get("regularMarketOpen"), live_p)
                high_p = safe_float(meta.get("regularMarketDayHigh"), live_p)
                low_p = safe_float(meta.get("regularMarketDayLow"), live_p)
                vol_lots = max(1, round(safe_int(meta.get("regularMarketVolume")) / 1000))
                pct = round(((live_p - prev_c) / prev_c) * 100, 2) if prev_c > 0 else st["pct"]
                return code, live_p, pct, open_p, high_p, low_p, vol_lots
            except Exception:
                return None

        with ThreadPoolExecutor(max_workers=12) as ex:
            for res in ex.map(fetch_single_v8_chart, still_missing):
                if res:
                    code, live_p, pct, open_p, high_p, low_p, vol_lots = res
                    st = stocks_map[code]
                    st["price"] = round(live_p, 2)
                    st["pct"] = pct
                    st["_todayBar"] = {
                        "d": today_mmdd,
                        "o": round(open_p if open_p > 0 else live_p, 2),
                        "h": round(max(high_p, live_p), 2),
                        "l": round(min(low_p, live_p) if low_p > 0 else live_p, 2),
                        "c": round(live_p, 2),
                        "v": vol_lots
                    }
                    st["_liveUpdated"] = True
                    chart_updated += 1

    print(f"=== 全市場 {len(stocks_map)} 檔當下即時股價同步完成：Yahoo-v7 {yahoo_updated} ＋ 證交所MIS {mis_updated} ＋ Yahoo-v8 {chart_updated} 檔 ===")
    return yahoo_updated + mis_updated + chart_updated


def backfill_all_market_50d_by_date(stocks_map, now_tw):
    """透過 FinMind 全市場按日批量查詢，一次補齊全市場 1,867 檔近 50 個交易日真實 OHLCV K 線"""
    per_stock_bars = {code: [] for code in stocks_map}
    finmind_url = "https://api.finmindtrade.com/api/v4/data"
    trading_dates = []
    for offset in range(78, -1, -1):
        d = now_tw - timedelta(days=offset)
        if d.weekday() < 5:
            trading_dates.append(d.strftime("%Y-%m-%d"))

    def fetch_one_day(date_str):
        try:
            r = requests.get(
                finmind_url,
                params={"dataset": "TaiwanStockPrice", "date": date_str},
                headers={"User-Agent": HEADERS["User-Agent"]},
                timeout=18
            )
            if r.status_code == 200:
                return date_str, r.json().get("data") or []
        except Exception:
            pass
        return date_str, []

    day_results = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for res in ex.map(fetch_one_day, trading_dates):
            if res[1]:
                day_results.append(res)

    day_results.sort(key=lambda x: x[0])
    for date_str, rows in day_results:
        mmdd = date_str[5:].replace("-", "/")
        for row in rows:
            code = str(row.get("stock_id", "")).strip()
            if code not in per_stock_bars:
                continue
            c = safe_float(row.get("close"))
            if c <= 0:
                continue
            o = safe_float(row.get("open"), c)
            h = safe_float(row.get("max"), max(o, c))
            l = safe_float(row.get("min"), min(o, c))
            v = max(1, round(safe_int(row.get("Trading_Volume")) / 1000))
            bars_list = per_stock_bars[code]
            if bars_list:
                prev_c = bars_list[-1]["c"]
                if prev_c > 0 and not (prev_c * 0.85 <= c <= prev_c * 1.15):
                    bars_list.clear()
            bars_list.append({
                "d": mmdd,
                "o": round(o, 2),
                "h": round(max(h, o, c), 2),
                "l": round(min(l, o, c), 2),
                "c": round(c, 2),
                "v": v
            })

    filled_stocks = 0
    for code, bars in per_stock_bars.items():
        if len(bars) >= 5:
            per_stock_bars[code] = bars[-50:]
            filled_stocks += 1
        else:
            per_stock_bars[code] = []

    print(f"=== 全市場 50 日真實 K 線批量回補完成：共 {filled_stocks} / {len(stocks_map)} 檔具備完整真實日線 ===")
    return per_stock_bars


def main():
    now_tw = datetime.now(TW_TZ)
    today_label = now_tw.strftime("%Y/%m/%d")
    today_mmdd = now_tw.strftime("%m/%d")
    intraday_mode = is_intraday_session(now_tw)
    print(f"=== 開始執行台股全市場 1,867 家當下即時股價更新 ({today_label} {now_tw.strftime('%H:%M:%S')} TST) ===")

    prev_map = load_previous_data()
    stocks_map = {}

    # 1. 取得上市 (TWSE) 全市場掛牌名單與基本估值（作為名單與昨收基準）
    twse_day = fetch_json(TWSE_DAY_ALL_URL) or []
    twse_val = fetch_json(TWSE_BWIBBU_URL) or []
    val_map_twse = {str(item.get("Code", "")).strip(): item for item in twse_val}

    for row in twse_day:
        code = str(row.get("Code", "")).strip()
        name = str(row.get("Name", "")).strip()
        if len(code) != 4 or not code.isdigit() or code.startswith("00"):
            continue

        ref_close = safe_float(row.get("ClosingPrice"))
        if ref_close <= 0:
            continue

        open_p = safe_float(row.get("OpeningPrice"), ref_close)
        high_p = safe_float(row.get("HighestPrice"), ref_close)
        low_p = safe_float(row.get("LowestPrice"), ref_close)
        change_p = safe_float(row.get("Change"))
        prev_close = ref_close - change_p if (ref_close - change_p) > 0 else ref_close
        pct = round((change_p / prev_close) * 100, 2) if prev_close > 0 else 0.0
        vol_lots = round(safe_int(row.get("TradeVolume")) / 1000)

        v_info = val_map_twse.get(code, {})
        pe = safe_float(v_info.get("PEratio"), prev_map.get(code, {}).get("pe", 18.0))
        yld = safe_float(v_info.get("DividendYield"), prev_map.get(code, {}).get("yield", 2.5))

        stocks_map[code] = {
            "code": code,
            "name": name,
            "market": "TWSE",
            "sector": prev_map.get(code, {}).get("sector", "上市企業"),
            "price": ref_close,
            "pct": pct,
            "pe": pe if pe > 0 else 25.0,
            "yield": yld,
            "roe": prev_map.get(code, {}).get("roe", 14.5),
            "revYoy": prev_map.get(code, {}).get("revYoy", 10.0),
            "inst1d": prev_map.get(code, {}).get("inst1d", 0),
            "inst5d": prev_map.get(code, {}).get("inst5d", 0),
            "detail": prev_map.get(code, {}).get("detail", "外資0 / 投信0 / 自營0"),
            "_todayBar": {"d": today_mmdd, "o": open_p, "h": high_p, "l": low_p, "c": ref_close, "v": vol_lots}
        }

    # 非盤中時段才更新上市盤後三大法人買賣超
    if not intraday_mode:
        time.sleep(0.8)
        twse_t86 = fetch_json(TWSE_T86_URL)
        if twse_t86 and isinstance(twse_t86.get("data"), list) and len(twse_t86["data"]) > 0:
            for r in twse_t86["data"]:
                code = str(r[0]).strip()
                if code in stocks_map:
                    foreign_lots = round(safe_int(r[4]) / 1000)
                    trust_lots = round(safe_int(r[10]) / 1000)
                    dealer_lots = round(safe_int(r[11]) / 1000)
                    total_lots = round(safe_int(r[-1]) / 1000)
                    stocks_map[code]["inst1d"] = total_lots
                    stocks_map[code]["inst5d"] = prev_map.get(code, {}).get("inst5d", 0) + total_lots
                    stocks_map[code]["detail"] = (
                        f"外資{foreign_lots:+} / 投信{trust_lots:+} / 自營{dealer_lots:+}"
                    )

    # 2. 取得上櫃 (TPEx) 全市場掛牌名單與基本估值
    time.sleep(0.8)
    tpex_quotes = fetch_json(TPEX_QUOTES_URL) or []
    tpex_pe = fetch_json(TPEX_PERATIO_URL) or []
    tpex_inst = [] if intraday_mode else (fetch_json(TPEX_3INST_URL) or [])

    pe_map_tpex = {str(item.get("SecuritiesCompanyCode", "")).strip(): item for item in tpex_pe}
    inst_map_tpex = {str(item.get("SecuritiesCompanyCode", "")).strip(): item for item in tpex_inst}

    for row in tpex_quotes:
        code = str(row.get("SecuritiesCompanyCode", "")).strip()
        name = str(row.get("CompanyName", "")).strip()
        if len(code) != 4 or not code.isdigit() or code.startswith("00"):
            continue

        ref_close = safe_float(row.get("Close"))
        if ref_close <= 0:
            continue

        open_p = safe_float(row.get("Open"), ref_close)
        high_p = safe_float(row.get("High"), ref_close)
        low_p = safe_float(row.get("Low"), ref_close)
        change_p = safe_float(row.get("Change"))
        prev_close = ref_close - change_p if (ref_close - change_p) > 0 else ref_close
        pct = round((change_p / prev_close) * 100, 2) if prev_close > 0 else 0.0
        vol_lots = round(safe_int(row.get("TradingShares")) / 1000)

        p_info = pe_map_tpex.get(code, {})
        pe = safe_float(p_info.get("PEratio"), prev_map.get(code, {}).get("pe", 19.0))
        yld = safe_float(p_info.get("DividendYield"), prev_map.get(code, {}).get("yield", 2.4))

        i_info = inst_map_tpex.get(code, {})
        if i_info and not intraday_mode:
            foreign_lots = round(safe_int(i_info.get("ForeignInvestorsDifference")) / 1000)
            trust_lots = round(safe_int(i_info.get("InvestmentTrustDifference")) / 1000)
            dealer_lots = round(safe_int(i_info.get("DealerDifference")) / 1000)
            total_lots = round(safe_int(i_info.get("TotalDifference")) / 1000)
            detail_str = f"外資{foreign_lots:+} / 投信{trust_lots:+} / 自營{dealer_lots:+}"
            inst5d_val = prev_map.get(code, {}).get("inst5d", 0) + total_lots
        else:
            total_lots = prev_map.get(code, {}).get("inst1d", 0)
            inst5d_val = prev_map.get(code, {}).get("inst5d", 0)
            detail_str = prev_map.get(code, {}).get("detail", "外資0 / 投信0 / 自營0")

        stocks_map[code] = {
            "code": code,
            "name": name,
            "market": "TPEx",
            "sector": prev_map.get(code, {}).get("sector", "上櫃企業"),
            "price": ref_close,
            "pct": pct,
            "pe": pe if pe > 0 else 25.0,
            "yield": yld,
            "roe": prev_map.get(code, {}).get("roe", 14.0),
            "revYoy": prev_map.get(code, {}).get("revYoy", 10.0),
            "inst1d": total_lots,
            "inst5d": inst5d_val,
            "detail": detail_str,
            "_todayBar": {"d": today_mmdd, "o": open_p, "h": high_p, "l": low_p, "c": ref_close, "v": vol_lots}
        }

    # 若舊檔有額外標的但今日 API 暫缺，補回以確保 1,867 檔完整收錄
    for code, old_st in prev_map.items():
        if code not in stocks_map:
            stocks_map[code] = {
                **old_st,
                "_todayBar": (old_st.get("klines50d") or [{"d": today_mmdd, "o": old_st["price"], "h": old_st["price"], "l": old_st["price"], "c": old_st["price"], "v": 100}])[-1]
            }

    # 3. 無論盤中或盤後，一律以「當下的最新即時成交價」覆寫全市場 1,867 檔股價與今日 K 棒
    live_updated_total = 0
    if len(stocks_map) > 500:
        live_updated_total = fetch_all_1867_current_prices(stocks_map, now_tw)

    # 4. 全市場 1,867 檔滾動更新 50 日真實 K 線並輸出 JSON
    stocks_needing_backfill = sum(
        1 for code in stocks_map
        if len(prev_map.get(code, {}).get("klines50d") or []) < 20
    )
    bulk_backfill_map = {}
    if stocks_needing_backfill > 100 and len(stocks_map) > 500:
        bulk_backfill_map = backfill_all_market_50d_by_date(stocks_map, now_tw)

    final_list = []
    kline_ready_count = 0
    for code, st in sorted(stocks_map.items()):
        old_bars = list(prev_map.get(code, {}).get("klines50d") or [])
        today_bar = st.pop("_todayBar")
        st.pop("_liveUpdated", None)

        if len(old_bars) < 20 and bulk_backfill_map.get(code):
            old_bars = bulk_backfill_map[code]

        if old_bars and old_bars[-1].get("d") == today_bar["d"]:
            old_bars[-1] = today_bar
        else:
            old_bars.append(today_bar)

        # 確保股價嚴格等於最新一根 K 棒的當下收盤/成交價
        if old_bars:
            st["price"] = old_bars[-1]["c"]

        st["klines50d"] = old_bars[-50:]
        if len(st["klines50d"]) >= 5:
            kline_ready_count += 1
        final_list.append(st)

    if len(final_list) > 500:
        time_hhmm = now_tw.strftime("%H:%M")
        mode_tag = f"當下即時股價 {time_hhmm}" if intraday_mode else f"當下最新股價 {time_hhmm}"
        payload = {
            "updated_at": now_tw.strftime("%Y/%m/%d %H:%M"),
            "trading_date": f"{today_label} ({mode_tag})",
            "is_intraday": intraday_mode,
            "live_updated_count": live_updated_total,
            "total_count": len(final_list),
            "kline_ready_count": kline_ready_count,
            "twse_count": sum(1 for s in final_list if s["market"] == "TWSE"),
            "tpex_count": sum(1 for s in final_list if s["market"] == "TPEx"),
            "stocks": final_list
        }
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        print(f"=== 成功收錄 {len(final_list)} 檔上市櫃當下即時股價（即時更新 {live_updated_total} 檔、50日K線 {kline_ready_count} 檔）至 {OUTPUT_JSON} ===")
    else:
        print("[提示] 今日 API 暫無資料，保留既有資料庫不覆蓋。")


if __name__ == "__main__":
    main()
