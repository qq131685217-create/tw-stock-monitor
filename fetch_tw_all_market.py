import json
import os
import asyncio
import httpx
import requests
from datetime import datetime, timezone, timedelta

TW_TZ = timezone(timedelta(hours=8))

TWSE_DAY_ALL_URL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TPEX_QUOTES_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"

YAHOO_CHART_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
FINMIND_DATA_URL = "https://api.finmindtrade.com/api/v4/data"

OUTPUT_JSON = "tw_market_data.json"
TWSE_TOP10_URL = "https://www.twse.com.tw/rwd/zh/fund/T86"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*"
}


def safe_float(val, default=0.0):
    try:
        if val is None:
            return default
        s = str(val).replace(",", "").replace("+", "").replace("%", "").strip()
        if s in ("", "-", "--", "N/A", "除息", "除權", "null"):
            return default
        return float(s)
    except Exception:
        return default


def safe_int(val, default=0):
    try:
        if val is None:
            return default
        s = str(val).replace(",", "").replace("+", "").strip()
        if s in ("", "-", "--", "N/A", "null"):
            return default
        return int(float(s))
    except Exception:
        return default
def fetch_top10_institutional_data():
    try:
        payload = {
            "response": "json",
            "date": datetime.now(TW_TZ).strftime("%Y%m%d"),
            "selectType": "ALL"
        }
        resp = requests.get(TWSE_TOP10_URL, params=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("data", []) if isinstance(data, dict) else []
        result = []

        for row in rows[:10]:
            if not row or len(row) < 6:
                continue

            code = str(row[0]).strip()
            name = str(row[1]).strip()
            foreign = safe_int(row[2], 0)
            trust = safe_int(row[3], 0)
            dealer = safe_int(row[4], 0)
            total = safe_int(row[5], 0)

            if not code:
                continue

            result.append({
                "code": code,
                "name": name,
                "market": "TWSE",
                "foreign": foreign,
                "trust": trust,
                "dealer": dealer,
                "total": total
            })

        return {
            "updated_at": datetime.now(TW_TZ).strftime("%Y/%m/%d %H:%M"),
            "top10": result
        }
    except Exception:
        return {
            "updated_at": datetime.now(TW_TZ).strftime("%Y/%m/%d %H:%M"),
            "top10": []
        }



def load_previous_data():
    if not os.path.exists(OUTPUT_JSON):
        return {}

    try:
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            stocks = data.get("stocks") or []
            return {item["code"]: item for item in stocks if isinstance(item, dict) and "code" in item}
    except Exception:
        return {}


def is_intraday_session(now_tw):
    if now_tw.weekday() >= 5:
        return False
    hhmm = now_tw.hour * 100 + now_tw.minute
    return 900 <= hhmm <= 1335


def create_stock_record(code, name, market, prev_stock, price, open_p, high_p, low_p, volume_lots, today_mmdd):
    old = prev_stock or {}
    return {
        "code": code,
        "name": name,
        "market": market,
        "sector": old.get("sector", "上市企業" if market == "TWSE" else "上櫃企業"),
        "price": price,
        "pct": old.get("pct", 0.0),
        "pe": old.get("pe"),
        "yield": old.get("yield"),
        "roe": old.get("roe"),
        "revYoy": old.get("revYoy"),
        "inst1d": old.get("inst1d"),
        "inst5d": old.get("inst5d"),
        "detail": old.get("detail", ""),
        "_todayBar": {
            "d": today_mmdd,
            "o": round(open_p or price, 2),
            "h": round(max(high_p, price), 2),
            "l": round(min(low_p, price), 2),
            "c": round(price, 2),
            "v": max(0, volume_lots),
        },
    }


async def fetch_single_chart_async(client, semaphore, code, market):
    suffix = ".TW" if market == "TWSE" else ".TWO"
    url = f"{YAHOO_CHART_BASE_URL}/{code}{suffix}?interval=1d&range=2d"

    async with semaphore:
        try:
            resp = await client.get(url, headers=HEADERS, timeout=3.0)
            if resp.status_code != 200:
                return None

            payload = resp.json()
            result = (payload.get("chart") or {}).get("result") or []
            if not result:
                return None

            meta = result[0].get("meta", {})
            live_p = safe_float(meta.get("regularMarketPrice"))
            if live_p <= 0:
                return None

            prev_close = safe_float(meta.get("chartPreviousClose"), safe_float(meta.get("previousClose"), live_p))
            open_p = safe_float(meta.get("regularMarketOpen"), live_p)
            high_p = safe_float(meta.get("regularMarketDayHigh"), live_p)
            low_p = safe_float(meta.get("regularMarketDayLow"), live_p)
            vol_lots = max(0, round(safe_int(meta.get("regularMarketVolume")) / 1000))
            pct = round(((live_p - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0.0

            return code, live_p, pct, open_p, high_p, low_p, vol_lots
        except Exception:
            return None


async def fetch_all_1867_current_prices_async(stocks_map, now_tw):
    today_str = now_tw.strftime("%Y-%m-%d")
    today_mmdd = now_tw.strftime("%m/%d")
    updated_count = 0
    semaphore = asyncio.Semaphore(15)

    async with httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=50, max_connections=100)
    ) as client:
        try:
            r = await client.get(
                FINMIND_DATA_URL,
                params={"dataset": "TaiwanStockPrice", "date": today_str},
                headers=HEADERS,
                timeout=6.0
            )

            if r.status_code == 200:
                rows = r.json().get("data") or []
                for row in rows:
                    code = str(row.get("stock_id", "")).strip()
                    if code not in stocks_map:
                        continue

                    st = stocks_map[code]
                    live_p = safe_float(row.get("close"))
                    if live_p <= 0:
                        continue

                    spread = safe_float(row.get("spread"))
                    ref_prev = (live_p - spread) if (live_p - spread) > 0 else st["price"]
                    open_p = safe_float(row.get("open"), live_p)
                    high_p = safe_float(row.get("max"), live_p)
                    low_p = safe_float(row.get("min"), live_p)
                    vol_lots = max(0, round(safe_int(row.get("Trading_Volume")) / 1000))
                    pct = round(((live_p - ref_prev) / ref_prev) * 100, 2) if ref_prev > 0 else st["pct"]

                    st["price"] = round(live_p, 2)
                    st["pct"] = pct
                    st["_todayBar"] = {
                        "d": today_mmdd,
                        "o": round(open_p or live_p, 2),
                        "h": round(max(high_p, live_p), 2),
                        "l": round(min(low_p, live_p) or live_p, 2),
                        "c": round(live_p, 2),
                        "v": vol_lots,
                    }
                    st["_liveUpdated"] = True
                    updated_count += 1
        except Exception as exc:
            print(f"[FinMind] 資料更新失敗：{exc}")

        tasks = [
            fetch_single_chart_async(client, semaphore, code, st["market"])
            for code, st in stocks_map.items()
            if not st.get("_liveUpdated")
        ]

        if tasks:
            results = await asyncio.gather(*tasks)
            for res in results:
                if not res:
                    continue
                code, live_p, pct, open_p, high_p, low_p, vol_lots = res
                st = stocks_map.get(code)
                if not st:
                    continue

                st["price"] = round(live_p, 2)
                st["pct"] = pct
                st["_todayBar"] = {
                    "d": today_mmdd,
                    "o": round(open_p or live_p, 2),
                    "h": round(max(high_p, live_p), 2),
                    "l": round(min(low_p, live_p) or live_p, 2),
                    "c": round(live_p, 2),
                    "v": vol_lots,
                }
                st["_liveUpdated"] = True
                updated_count += 1

    print(f"=== [限速非同步版] 全市場即時成交價更新完成：實際更新 {updated_count} 檔 ===")
    return updated_count


def fetch_twse_market_data(prev_map, today_mmdd):
    stocks_map = {}
    response = requests.get(TWSE_DAY_ALL_URL, headers=HEADERS, timeout=8)
    response.raise_for_status()
    rows = response.json()

    if not isinstance(rows, list):
        raise ValueError("TWSE 回傳格式錯誤：不是列表")

    for row in rows:
        code = str(row.get("Code", "")).strip()
        name = str(row.get("Name", "")).strip()

        if len(code) != 4 or not code.isdigit() or code.startswith("00"):
            continue

        price = safe_float(row.get("ClosingPrice"))
        if price <= 0:
            continue

        open_price = safe_float(row.get("OpeningPrice"), price)
        high_price = safe_float(row.get("HighestPrice"), price)
        low_price = safe_float(row.get("LowestPrice"), price)
        volume_lots = max(0, safe_int(row.get("TradeVolume")) // 1000)

        prev_stock = prev_map.get(code, {})
        stocks_map[code] = create_stock_record(
            code=code,
            name=name,
            market="TWSE",
            prev_stock=prev_stock,
            price=price,
            open_p=open_price,
            high_p=high_price,
            low_p=low_price,
            volume_lots=volume_lots,
            today_mmdd=today_mmdd,
        )

    return stocks_map


def fetch_tpex_market_data(prev_map, today_mmdd):
    stocks_map = {}
    response = requests.get(TPEX_QUOTES_URL, headers=HEADERS, timeout=8)
    response.raise_for_status()
    rows = response.json()

    if not isinstance(rows, list):
        raise ValueError("TPEx 回傳格式錯誤：不是列表")

    for row in rows:
        code = str(row.get("SecuritiesCompanyCode", "")).strip()
        name = str(row.get("CompanyName", "")).strip()

        if len(code) != 4 or not code.isdigit() or code.startswith("00"):
            continue

        price = safe_float(row.get("Close"))
        if price <= 0:
            continue

        prev_stock = prev_map.get(code, {})
        stocks_map[code] = create_stock_record(
            code=code,
            name=name,
            market="TPEx",
            prev_stock=prev_stock,
            price=price,
            open_p=price,
            high_p=price,
            low_p=price,
            volume_lots=0,
            today_mmdd=today_mmdd,
        )

    return stocks_map


def main():
    now_tw = datetime.now(TW_TZ)
    today_label = now_tw.strftime("%Y/%m/%d")
    today_mmdd = now_tw.strftime("%m/%d")
    intraday_mode = is_intraday_session(now_tw)

    print(f"=== [極速啟動] 台股全市場 1,867 家限速非同步更新 ({today_label} {now_tw.strftime('%H:%M:%S')}) ===")

    prev_map = load_previous_data()
    stocks_map = {}

    try:
        stocks_map.update(fetch_twse_market_data(prev_map, today_mmdd))
    except Exception as exc:
        print(f"[TWSE] 讀取失敗：{exc}")

    try:
        stocks_map.update(fetch_tpex_market_data(prev_map, today_mmdd))
    except Exception as exc:
        print(f"[TPEx] 讀取失敗：{exc}")

    # 若 TWSE/TPEx 都沒任何資料，就不寫入檔案，避免偽造資料
    if not stocks_map:
        raise RuntimeError("TWSE / TPEx 兩個來源均無有效資料，停止寫檔。")

    # 補回舊資料（但不覆蓋新抓到的資料）
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

    payload = {
        "updated_at": now_tw.strftime("%Y/%m/%d %H:%M"),
        "trading_date": f"{today_label} (限速非同步)",
        "is_intraday": intraday_mode,
        "live_updated_count": live_updated_total,
        "total_count": len(final_list),
        "stocks": final_list,
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    print(f"=== [完成] 成功儲存 {len(final_list)} 檔資料至 {OUTPUT_JSON} ===")

    top10_payload = fetch_top10_institutional_data()
    with open("tw_top10_data.json", "w", encoding="utf-8") as f:
        json.dump(top10_payload, f, ensure_ascii=False, separators=(",", ":"))

    print(f"=== [完成] 成功儲存法人買賣超前十大資料至 tw_top10_data.json ===")

if __name__ == "__main__":
    main()
