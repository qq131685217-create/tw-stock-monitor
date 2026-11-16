import json
import os
import time
import asyncio
import httpx
from datetime import datetime, timezone, timedelta

TW_TZ = timezone(timedelta(hours=8))

TWSE_DAY_ALL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TPEX_QUOTES_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"

YAHOO_CHART_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
FINMIND_DATA_URL = "https://api.finmindtrade.com/api/v4/data"

OUTPUT_JSON = "tw_market_data.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*"
}

def safe_float(val, default=0.0):
    try:
        if val is None: return default
        s = str(val).replace(",", "").replace("+", "").replace("%", "").strip()
        if s in ("", "-", "--", "N/A", "除息", "除權"): return default
        return float(s)
    except Exception: return default

def safe_int(val, default=0):
    try:
        if val is None: return default
        s = str(val).replace(",", "").replace("+", "").strip()
        if s in ("", "-", "--"): return default
        return int(float(s))
    except Exception: return default

def load_previous_data():
    if not os.path.exists(OUTPUT_JSON): return {}
    try:
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {item["code"]: item for item in data.get("stocks", [])}
    except Exception: return {}

def is_intraday_session(now_tw):
    if now_tw.weekday() >= 5: return False
    hhmm = now_tw.hour * 100 + now_tw.minute
    return 900 <= hhmm <= 1335

async def fetch_single_chart_async(client, semaphore, code, market, today_mmdd):
    suffix = ".TW" if market == "TWSE" else ".TWO"
    url = f"{YAHOO_CHART_BASE_URL}/{code}{suffix}?interval=1d&range=2d"
    async with semaphore:
        try:
            resp = await client.get(url, headers=HEADERS, timeout=3.0)
            if resp.status_code != 200: return None
            res_list = resp.json().get("chart", {}).get("result") or []
            if not res_list: return None
            meta = res_list[0].get("meta", {})
            live_p = safe_float(meta.get("regularMarketPrice"))
            if live_p <= 0: return None
            prev_c = safe_float(meta.get("chartPreviousClose"), safe_float(meta.get("previousClose"), live_p))
            open_p = safe_float(meta.get("regularMarketOpen"), live_p)
            high_p = safe_float(meta.get("regularMarketDayHigh"), live_p)
            low_p = safe_float(meta.get("regularMarketDayLow"), live_p)
            vol_lots = max(1, round(safe_int(meta.get("regularMarketVolume")) / 1000))
            pct = round(((live_p - prev_c) / prev_c) * 100, 2) if prev_c > 0 else 0.0
            return code, live_p, pct, open_p, high_p, low_p, vol_lots
        except Exception:
            return None

async def fetch_all_1867_current_prices_async(stocks_map, now_tw):
    today_str = now_tw.strftime("%Y-%m-%d")
    today_mmdd = now_tw.strftime("%m/%d")
    v8_updated = 0
    semaphore = asyncio.Semaphore(15) # 嚴格控制同時最高 15 個並發，避免觸發 Yahoo 封鎖限流

    async with httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=50, max_connections=100)) as client:
        # 1. 優先透過 FinMind 批次取得當日快照
        try:
            r = await client.get(FINMIND_DATA_URL, params={"dataset": "TaiwanStockPrice", "date": today_str}, headers=HEADERS, timeout=6.0)
            if r.status_code == 200:
                rows = r.json().get("data") or []
                for row in rows:
                    code = str(row.get("stock_id", "")).strip()
                    if code not in stocks_map: continue
                    st = stocks_map[code]
                    live_p = safe_float(row.get("close"))
                    if live_p <= 0: continue
                    spread = safe_float(row.get("spread"))
                    ref_prev = (live_p - spread) if (live_p - spread) > 0 else st["price"]
                    open_p = safe_float(row.get("open"), live_p)
                    high_p = safe_float(row.get("max"), live_p)
                    low_p = safe_float(row.get("min"), live_p)
                    vol_lots = max(1, round(safe_int(row.get("Trading_Volume")) / 1000))
                    pct = round(((live_p - ref_prev) / ref_prev) * 100, 2) if ref_prev > 0 else st["pct"]
                    st["price"] = round(live_p, 2)
                    st["pct"] = pct
                    st["_todayBar"] = {"d": today_mmdd, "o": round(open_p or live_p, 2), "h": round(max(high_p, live_p), 2), "l": round(min(low_p, live_p) or live_p, 2), "c": round(live_p, 2), "v": vol_lots}
                    st["_liveUpdated"] = True
                    v8_updated += 1
        except Exception:
            pass

        # 2. 針對未更新標的以安全號誌限速非同步補齊
        tasks = [
            fetch_single_chart_async(client, semaphore, code, st["market"], today_mmdd)
            for code, st in stocks_map.items() if not st.get("_liveUpdated")
        ]
        results = await asyncio.gather(*tasks)
        for res in results:
            if res:
                code, live_p, pct, open_p, high_p, low_p, vol_lots = res
                st = stocks_map[code]
                st["price"] = round(live_p, 2)
                st["pct"] = pct
                st["_todayBar"] = {"d": today_mmdd, "o": round(open_p or live_p, 2), "h": round(max(high_p, live_p), 2), "l": round(min(low_p, live_p) or live_p, 2), "c": round(live_p, 2), "v": vol_lots}
                v8_updated += 1

    print(f"=== [限速非同步版] 全市場即時成交價更新完成：共同步 {v8_updated} 檔 ===")
    return v8_updated

def main():
    now_tw = datetime.now(TW_TZ)
    today_label = now_tw.strftime("%Y/%m/%d")
    today_mmdd = now_tw.strftime("%m/%d")
    intraday_mode = is_intraday_session(now_tw)
    print(f"=== [極速啟動] 台股全市場 1,867 家限速非同步更新 ({today_label} {now_tw.strftime('%H:%M:%S')}) ===")

    prev_map = load_previous_data()
    stocks_map = {}

    try:
        r_twse = requests.get(TWSE_DAY_ALL_URL, headers=HEADERS, timeout=8)
        if r_twse.status_code == 200:
            for row in r_twse.json():
                code = str(row.get("Code", "")).strip()
                name = str(row.get("Name", "")).strip()
                if len(code) != 4 or not code.isdigit() or code.startswith("00"): continue
                ref_close = safe_float(row.get("ClosingPrice"))
                if ref_close <= 0: continue
                stocks_map[code] = {
                    "code": code, "name": name, "market": "TWSE",
                    "sector": prev_map.get(code, {}).get("sector", "上市企業"),
                    "price": ref_close, "pct": 0.0, "pe": 20.0, "yield": 3.0,
                    "roe": 14.0, "revYoy": 10.0, "inst1d": 0, "inst5d": 0,
                    "detail": "外資0 / 投信0 / 自營0",
                    "_todayBar": {"d": today_mmdd, "o": ref_close, "h": ref_close, "l": ref_close, "c": ref_close, "v": 1000}
                }
    except Exception:
        pass

    try:
        r_tpex = requests.get(TPEX_QUOTES_URL, headers=HEADERS, timeout=8)
        if r_tpex.status_code == 200:
            for row in r_tpex.json():
                code = str(row.get("SecuritiesCompanyCode", "")).strip()
                name = str(row.get("CompanyName", "")).strip()
                if len(code) != 4 or not code.isdigit() or code.startswith("00"): continue
                ref_close = safe_float(row.get("Close"))
                if ref_close <= 0: continue
                stocks_map[code] = {
                    "code": code, "name": name, "market": "TPEx",
                    "sector": prev_map.get(code, {}).get("sector", "上櫃企業"),
                    "price": ref_close, "pct": 0.0, "pe": 20.0, "yield": 3.0,
                    "roe": 14.0, "revYoy": 10.0, "inst1d": 0, "inst5d": 0,
                    "detail": "外資0 / 投信0 / 自營0",
                    "_todayBar": {"d": today_mmdd, "o": ref_close, "h": ref_close, "l": ref_close, "c": ref_close, "v": 1000}
                }
    except Exception:
        pass

    for code, old_st in prev_map.items():
        if code not in stocks_map:
            stocks_map[code] = old_st

    live_updated_total = asyncio.run(fetch_all_1867_current_prices_async(stocks_map, now_tw))

    final_list = []
    for code, st in sorted(stocks_map.items()):
        old_bars = list(prev_map.get(code, {}).get("klines50d") or [])
        today_bar = st.pop("_todayBar", None)
        st.pop("_liveUpdated", None)

        if today_bar:
            if old_bars and old_bars[-1].get("d") == today_bar["d"]:
                old_bars[-1] = today_bar
            else:
                old_bars.append(today_bar)

        if old_bars:
            st["price"] = old_bars[-1]["c"]

        st["klines50d"] = old_bars[-50:]
        final_list.append(st)

    if len(final_list) > 500:
        payload = {
            "updated_at": now_tw.strftime("%Y/%m/%d %H:%M"),
            "trading_date": f"{today_label} (限速非同步)",
            "is_intraday": intraday_mode,
            "live_updated_count": live_updated_total,
            "total_count": len(final_list),
            "stocks": final_list
        }
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        print(f"=== [完成] 成功儲存 {len(final_list)} 檔資料至 {OUTPUT_JSON} ===")

if __name__ == "__main__":
    main()
