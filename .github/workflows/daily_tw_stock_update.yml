name: Daily TWSE & TPEx Market Data Auto-Update

on:
  # 週一至週五台灣時間 15:35（UTC 07:35）自動執行盤後資料抓取
  schedule:
    - cron: '35 7 * * 1-5'
  # 允許在 GitHub Actions 網頁上手動點擊 "Run workflow" 立即測試更新
  workflow_dispatch:

permissions:
  contents: write

jobs:
  update-market-data:
    runs-on: ubuntu-latest
    steps:
      - name: 1. 簽出儲存庫程式碼
        uses: actions/checkout@v4

      - name: 2. 設定 Python 3.11 環境
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: 3. 安裝網路請求套件
        run: |
          python -m pip install --upgrade pip
          pip install requests

      - name: 4. 執行台股全市場（上市＋上櫃）盤後數據爬蟲
        run: python fetch_tw_all_market.py

      - name: 5. 自動 Commit 並推播最新 tw_market_data.json
        run: |
          git config --local user.email "github-actions[bot]@users.noreply.github.com"
          git config --local user.name "github-actions[bot]"
          git add tw_market_data.json
          if git diff --staged --quiet; then
            echo "今日資料無變動（可能為休市日），無需提交。"
          else
            git commit -m "chore: 自動更新台股全市場盤後行情與三大法人數據 ($(TZ='Asia/Taipei' date +'%Y-%m-%d'))"
            git push
          fi
