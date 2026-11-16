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

OUTPUT_JSON = "tw_market_data.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
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
    """讀取前一日的 tw_market_data.json 以保留 50 日 K 線歷史與產業分類"""
    if not os.path.exists(OUTPUT_JSON):
        return {}
    try:
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {item["code"]: item for item in data.get("stocks", [])}
    except Exception as e:
        print(f"[警告] 無法讀取舊檔 {OUTPUT_JSON}: {e}")
        return {}


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
    short_date = now_tw.strftime("%m/%d")
    print(f"=== 開始執行台股全市場盤後更新 ({today_label} {now_tw.strftime('%H:%M:%S')} TST) ===")

    prev_map = load_previous_data()
    stocks_map = {}

    # ---------------------------------------------------------
    # 1. 抓取上市 (TWSE) 全市場當日成交與估值
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
            "inst1d": 0,
            "inst5d": prev_map.get(code, {}).get("inst5d", 0),
            "detail": "外資0 / 投信0 / 自營0",
            "_todayBar": {"d": short_date, "o": open_p, "h": high_p, "l": low_p, "c": close_p, "v": vol_lots}
        }

    # 抓取上市 (TWSE) 三大法人買賣超
    time.sleep(1)
    twse_t86 = fetch_json(TWSE_T86_URL)
    if twse_t86 and isinstance(twse_t86.get("data"), list):
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
    # 2. 抓取上櫃 (TPEx) 全市場當日成交、估值與三大法人
    # ---------------------------------------------------------
    time.sleep(1)
    tpex_quotes = fetch_json(TPEX_QUOTES_URL) or []
    tpex_pe = fetch_json(TPEX_PERATIO_URL) or []
    tpex_inst = fetch_json(TPEX_3INST_URL) or []

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
        foreign_lots = round(safe_int(i_info.get("ForeignInvestorsDifference")) / 1000)
        trust_lots = round(safe_int(i_info.get("InvestmentTrustDifference")) / 1000)
        dealer_lots = round(safe_int(i_info.get("DealerDifference")) / 1000)
        total_lots = round(safe_int(i_info.get("TotalDifference")) / 1000)

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
            "inst5d": prev_map.get(code, {}).get("inst5d", 0) + total_lots,
            "detail": f"外資{foreign_lots:+} / 投信{trust_lots:+} / 自營{dealer_lots:+}",
            "_todayBar": {"d": short_date, "o": open_p, "h": high_p, "l": low_p, "c": close_p, "v": vol_lots}
        }

    # ---------------------------------------------------------
    # 3. 滾動更新 50 日 K 線並輸出 JSON
    # ---------------------------------------------------------
    # 針對法人買賣超前 40 名與核心權值股，若尚無 50 日歷史 K 線則自動補齊
    priority_codes = set(["2330", "2454", "2317", "2382", "3017", "2891", "2303", "2881", "2885", "2610", "2618", "1815", "6147", "3264"])
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

        if old_bars and old_bars[-1].get("d") == short_date:
            old_bars[-1] = today_bar
        else:
            old_bars.append(today_bar)

        st["klines50d"] = old_bars[-50:]
        final_list.append(st)

    if len(final_list) > 500:
        payload = {
            "updated_at": now_tw.strftime("%Y/%m/%d %H:%M"),
            "trading_date": today_label,
            "total_count": len(final_list),
            "twse_count": sum(1 for s in final_list if s["market"] == "TWSE"),
            "tpex_count": sum(1 for s in final_list if s["market"] == "TPEx"),
            "stocks": final_list
        }
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        print(f"=== 成功更新 {len(final_list)} 檔上市櫃資料至 {OUTPUT_JSON} ===")
    else:
        print("[提示] 今日可能為休市日或 API 尚未產出盤後資料，保留既有資料庫不覆蓋。")


if __name__ == "__main__":
    main()
