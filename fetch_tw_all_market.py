import json
import os
import time
from datetime import datetime, timezone, timedelta
import requests

# 設定台灣時區 (UTC+8)
TW_TZ = timezone(timedelta(hours=8))

# 證交所 (TWSE) 與 櫃買中心 (TPEx) 官方盤後開放資料 API
TWSE_DAY_ALL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TWSE_BWIBBU_URL = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
TWSE_T86_URL = "https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALLBUT0999"

TPEX_QUOTES_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
TPEX_PERATIO_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"
TPEX_3INST_URL = "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"

# 證交所即時行情專線 API (盤中 09:00 ~ 13:35 即時成交價)
TWSE_MIS_REALTIME_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"

OUTPUT_JSON = "tw_market_data.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Referer": "https://mis.twse.com.tw/stock/fibest.jsp"
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


def parse_roc_date_to_mmdd(roc_str, fallback_mmdd):
    """將證交所/櫃買中心民國日期 (如 1151116 或 115/11/16) 轉為 MM/DD"""
    try:
        s = str(roc_str or "").replace("/", "").replace("-", "").strip()
        if len(s) == 7 and s.isdigit():
            return f"{s[3:5]}/{s[5:7]}"
        if len(s) == 8 and s.isdigit():
            return f"{s[4:6]}/{s[6:8]}"
    except Exception:
        pass
    return fallback_mmdd


def load_previous_data():
    """讀取既有的 tw_market_data.json 以保留 50 日 K 線歷史與法人籌碼"""
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


def fetch_realtime_mis_prices(stocks_map, now_tw):
    """向證交所 MIS 即時專線批量抓取全市場當下真實盤中成交價"""
    all_keys = []
    for code, st in stocks_map.items():
        prefix = "tse" if st["market"] == "TWSE" else "otc"
        all_keys.append(f"{prefix}_{code}.tw")

    updated_cnt = 0
    batch_size = 80
    session = requests.Session()
    session.headers.update(HEADERS)
    today_mmdd = now_tw.strftime("%m/%d")

    for i in range(0, len(all_keys), batch_size):
        batch = all_keys[i:i + batch_size]
        ex_ch = "|".join(batch)
        url = f"{TWSE_MIS_REALTIME_URL}?ex_ch={ex_ch}&json=1&delay=0&_={int(time.time() * 1000)}"
        try:
            resp = session.get(url, timeout=15)
            data = resp.json()
            msg_arr = data.get("msgArray") or []
            for item in msg_arr:
                code = str(item.get("c", "")).strip()
                if code not in stocks_map:
                    continue
                # z 為最近一筆真實成交價；若尚未撮合則取買進/賣出揭露價或昨收 y
                last_trade = safe_float(item.get("z"))
                yesterday_close = safe_float(item.get("y"))
                if last_trade <= 0:
                    bid_first = safe_float(str(item.get("b", "")).split("_")[0])
                    ask_first = safe_float(str(item.get("a", "")).split("_")[0])
                    last_trade = bid_first if bid_first > 0 else (ask_first if ask_first > 0 else yesterday_close)

                if last_trade > 0:
                    st = stocks_map[code]
                    open_p = safe_float(item.get("o"), last_trade)
                    high_p = safe_float(item.get("h"), last_trade)
                    low_p = safe_float(item.get("l"), last_trade)
                    vol_lots = safe_int(item.get("v"))
                    pct = round(((last_trade - yesterday_close) / yesterday_close) * 100, 2) if yesterday_close > 0 else st["pct"]

                    st["price"] = last_trade
                    st["pct"] = pct
                    st["_todayBar"] = {
                        "d": today_mmdd,
                        "o": open_p if open_p > 0 else last_trade,
                        "h": max(high_p, last_trade),
                        "l": min(low_p, last_trade) if low_p > 0 else last_trade,
                        "c": last_trade,
                        "v": vol_lots
                    }
                    updated_cnt += 1
        except Exception as e:
            print(f"[警告] 盤中即時批次 {i} 讀取略過: {e}")
        time.sleep(0.35)

    print(f"=== 證交所 MIS 盤中即時報價同步完成：共更新 {updated_cnt} 檔當下真實成交價 ===")
    return updated_cnt


def backfill_50d_finmind(code, now_tw):
    """首次建立資料庫時，自動從 FinMind 開放庫補齊核心標的近 50 個交易日真實 K 線"""
    try:
        start_dt = (now_tw - timedelta(days=95)).strftime("%Y-%m-%d")
        url = f"https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockPrice&data_id={code}&start_date={start_dt}"
        data = fetch_json(url, timeout=15)
        if not data or not isinstance(data.get("data"), list):
            return []
        bars = []
        for r in data["data"][-50:]:
            c = safe_float(r.get("close"))
            if c <= 0:
                continue
            d_str = str(r.get("date", ""))[5:].replace("-", "/")
            bars.append({
                "d": d_str,
                "o": safe_float(r.get("open"), c),
                "h": safe_float(r.get("max"), c),
                "l": safe_float(r.get("min"), c),
                "c": c,
                "v": round(safe_int(r.get("Trading_Volume")) / 1000)
            })
        return bars
    except Exception:
        return []


def main():
    now_tw = datetime.now(TW_TZ)
    today_label = now_tw.strftime("%Y/%m/%d")
    fallback_mmdd = now_tw.strftime("%m/%d")
    intraday_mode = is_intraday_session(now_tw)
    print(f"=== 開始執行台股全市場更新 ({today_label} {now_tw.strftime('%H:%M:%S')} TST | {'盤中即時模式' if intraday_mode else '盤後收盤模式'}) ===")

    prev_map = load_previous_data()
    stocks_map = {}

    # ---------------------------------------------------------
    # 1. 抓取上市 (TWSE) 全市場名單、基本估值與最近盤後基準
    # ---------------------------------------------------------
    twse_day = fetch_json(TWSE_DAY_ALL_URL) or []
    twse_val = fetch_json(TWSE_BWIBBU_URL) or []
    val_map_twse = {str(item.get("Code", "")).strip(): item for item in twse_val}

    for row in twse_day:
        code = str(row.get("Code", "")).strip()
        name = str(row.get("Name", "")).strip()
        if len(code) != 4 or not code.isdigit() or code.startswith("00"):
            continue

        close_p = safe_float(row.get("ClosingPrice"))
        if close_p <= 0:
            continue

        bar_date = parse_roc_date_to_mmdd(row.get("Date"), fallback_mmdd)
        open_p = safe_float(row.get("OpeningPrice"), close_p)
        high_p = safe_float(row.get("HighestPrice"), close_p)
        low_p = safe_float(row.get("LowestPrice"), close_p)
        change_p = safe_float(row.get("Change"))
        prev_close = close_p - change_p if (close_p - change_p) > 0 else close_p
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
            "price": close_p,
            "pct": pct,
            "pe": pe if pe > 0 else 25.0,
            "yield": yld,
            "roe": prev_map.get(code, {}).get("roe", 14.5),
            "revYoy": prev_map.get(code, {}).get("revYoy", 10.0),
            "inst1d": prev_map.get(code, {}).get("inst1d", 0),
            "inst5d": prev_map.get(code, {}).get("inst5d", 0),
            "detail": prev_map.get(code, {}).get("detail", "外資0 / 投信0 / 自營0"),
            "_todayBar": {"d": bar_date, "o": open_p, "h": high_p, "l": low_p, "c": close_p, "v": vol_lots}
        }

    # 非盤中時段才更新上市盤後三大法人買賣超
    if not intraday_mode:
        time.sleep(1)
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

    # ---------------------------------------------------------
    # 2. 抓取上櫃 (TPEx) 全市場名單、估值與最近盤後基準
    # ---------------------------------------------------------
    time.sleep(1)
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

        close_p = safe_float(row.get("Close"))
        if close_p <= 0:
            continue

        bar_date = parse_roc_date_to_mmdd(row.get("Date"), fallback_mmdd)
        open_p = safe_float(row.get("Open"), close_p)
        high_p = safe_float(row.get("High"), close_p)
        low_p = safe_float(row.get("Low"), close_p)
        change_p = safe_float(row.get("Change"))
        prev_close = close_p - change_p if (close_p - change_p) > 0 else close_p
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
            "price": close_p,
            "pct": pct,
            "pe": pe if pe > 0 else 25.0,
            "yield": yld,
            "roe": prev_map.get(code, {}).get("roe", 14.0),
            "revYoy": prev_map.get(code, {}).get("revYoy", 10.0),
            "inst1d": total_lots,
            "inst5d": inst5d_val,
            "detail": detail_str,
            "_todayBar": {"d": bar_date, "o": open_p, "h": high_p, "l": low_p, "c": close_p, "v": vol_lots}
        }

    # ---------------------------------------------------------
    # 3. 盤中時段 (09:00~13:35) 覆寫為證交所 MIS 當下即時成交價
    # ---------------------------------------------------------
    if intraday_mode and len(stocks_map) > 500:
        fetch_realtime_mis_prices(stocks_map, now_tw)

    # ---------------------------------------------------------
    # 4. 滾動更新 50 日 K 線並輸出 JSON
    # ---------------------------------------------------------
    priority_codes = set([
        "2330", "2454", "2317", "2382", "3017", "2891", "2303",
        "2881", "2885", "2610", "2618", "1815", "6147", "3264", "2355"
    ])
    by_inst = sorted(stocks_map.values(), key=lambda x: abs(x["inst1d"]), reverse=True)[:35]
    for item in by_inst:
        priority_codes.add(item["code"])

    final_list = []
    for code, st in sorted(stocks_map.items()):
        old_bars = list(prev_map.get(code, {}).get("klines50d") or [])
        today_bar = st.pop("_todayBar")

        if len(old_bars) < 15 and code in priority_codes:
            backfilled = backfill_50d_finmind(code, now_tw)
            if backfilled:
                old_bars = backfilled
            time.sleep(0.15)

        if old_bars and old_bars[-1].get("d") == today_bar["d"]:
            old_bars[-1] = today_bar
        else:
            old_bars.append(today_bar)

        st["klines50d"] = old_bars[-50:]
        final_list.append(st)

    if len(final_list) > 500:
        sample_date = final_list[0]["klines50d"][-1]["d"] if final_list[0].get("klines50d") else fallback_mmdd
        mode_tag = "盤中即時成交" if intraday_mode else "盤後官方收盤"
        payload = {
            "updated_at": now_tw.strftime("%Y/%m/%d %H:%M"),
            "trading_date": f"{now_tw.year}/{sample_date} ({mode_tag})",
            "is_intraday": intraday_mode,
            "total_count": len(final_list),
            "twse_count": sum(1 for s in final_list if s["market"] == "TWSE"),
            "tpex_count": sum(1 for s in final_list if s["market"] == "TPEx"),
            "stocks": final_list
        }
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        print(f"=== 成功更新 {len(final_list)} 檔上市櫃{mode_tag}資料至 {OUTPUT_JSON} ===")
    else:
        print("[提示] 今日可能為休市日或 API 暫無資料，保留既有資料庫不覆蓋。")


if __name__ == "__main__":
    main()
