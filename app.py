import os
import io
import pandas as pd
import pytds
import anthropic
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Gemio ERP 自然語言查詢",
    page_icon="🔍",
    layout="wide",
)


def _secret(key: str, default: str = "") -> str:
    try:
        return st.secrets[key]
    except Exception:
        return os.getenv(key, default)


def get_conn():
    server = _secret("DB_SERVER", "163.17.141.61,8000").replace(",", ":")
    host, port = server.rsplit(":", 1)
    return pytds.connect(
        dsn=host,
        port=int(port),
        user=_secret("DB_USER"),
        password=_secret("DB_PASSWORD", ""),
        database=_secret("DB_NAME"),
        timeout=10,
    )


@st.cache_data(ttl=300)
def load_schema() -> dict:
    schema = {}
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT o.name, o.type_desc, c.name, t.name, c.is_nullable
        FROM sys.objects o
        JOIN sys.columns c ON o.object_id = c.object_id
        JOIN sys.types   t ON c.user_type_id = t.user_type_id
        WHERE o.type IN ('U','V') AND o.is_ms_shipped = 0
        ORDER BY o.name, c.column_id
    """)
    for tbl, tdesc, col, dtype, _ in cur.fetchall():
        if tbl not in schema:
            schema[tbl] = {"type": tdesc, "columns": []}
        schema[tbl]["columns"].append({"name": col, "type": dtype})
    conn.close()
    return schema


def execute_sql(sql: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = [list(r) for r in cur.fetchall()]
    conn.close()
    return cols, rows


def nl_to_sql(question: str, schema: dict) -> str:
    schema_text = "\n".join(
        f"  {tbl}: " + ", ".join(f"{c['name']} ({c['type']})" for c in info["columns"])
        for tbl, info in schema.items()
    )
    prompt = f"""你是一位 SQL Server 專家。請根據以下資料庫 schema，將使用者的問題轉換成正確的 T-SQL 查詢語句。

資料庫: gemio
可用的資料表/檢視表:
{schema_text}

規則:
1. 只回傳 SQL 語句，不要任何解釋或 markdown 標記
2. 使用繁體中文欄位名稱（schema 中有的）
3. 盡量使用檢視表（VIEW），避免直接查原始資料表
4. 不要使用 DROP、DELETE、UPDATE、INSERT 等修改指令
5. 若無法回答，回傳: SELECT '無法轉換此查詢' AS 訊息

使用者問題: {question}

SQL:"""

    client = anthropic.Anthropic(api_key=_secret("ANTHROPIC_API_KEY"))
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    sql = resp.content[0].text.strip()
    if sql.startswith("```"):
        sql = "\n".join(l for l in sql.split("\n") if not l.startswith("```"))
    return sql.strip()


def to_excel(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="查詢結果")
    return buf.getvalue()


# ─── 主介面（啟動時不連 DB）────────────────────────────────────────────────────

st.title("🔍 Gemio ERP 自然語言查詢")
st.caption("Powered by Claude AI — 用中文問問題，自動轉換為 SQL 查詢")

question = st.text_input(
    "輸入你的問題",
    placeholder="例如：列出本月採購金額最高的前十筆",
)

if st.button("🔍 查詢", type="primary", disabled=not question):

    # 步驟 1：載入 schema
    with st.spinner("載入資料庫結構…"):
        try:
            schema = load_schema()
        except Exception as e:
            st.error(f"資料庫連線失敗：{e}")
            st.stop()

    with st.expander(f"📋 可查詢的資料表／檢視表（共 {len(schema)} 個）"):
        st.write("、".join(schema.keys()))

    # 步驟 2：AI 轉 SQL
    with st.spinner("AI 分析中…"):
        try:
            sql = nl_to_sql(question, schema)
        except Exception as e:
            st.error(f"AI 轉換失敗：{e}")
            st.stop()

    st.subheader("產生的 SQL")
    st.code(sql, language="sql")

    # 步驟 3：執行 SQL
    with st.spinner("執行查詢…"):
        try:
            cols, rows = execute_sql(sql)
        except Exception as e:
            st.error(f"SQL 執行失敗：{e}")
            st.stop()

    if rows:
        df = pd.DataFrame(
            [[str(v) if v is not None else "" for v in row] for row in rows],
            columns=cols,
        )
        st.success(f"共 {len(df)} 筆資料")
        st.dataframe(df, use_container_width=True)
        st.download_button(
            label="📥 下載 Excel",
            data=to_excel(df),
            file_name="query_result.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.info("查詢結果為空")
