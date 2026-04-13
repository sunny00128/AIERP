# Gemio ERP 自然語言查詢

以繁體中文描述需求，系統自動產生 SQL 並查詢 Gemio ERP 資料庫。

## 功能
- 自然語言 → SQL（Gemini AI）
- 資料表 Schema 自動注入
- 查詢結果顯示（數字欄位自動加總）
- 匯出 Excel

## 本地啟動

```bash
pip install -r requirements.txt
uvicorn main:app --reload
```

開啟瀏覽器：http://localhost:8000

## 環境變數

```
GEMINI_API_KEY=...
DB_SERVER=163.17.141.61,8000
DB_NAME=gemio
DB_USER=drcas
DB_PASSWORD=...
```
