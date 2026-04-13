import os
import json
import io
import pandas as pd
import pyodbc
import google.generativeai as genai
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Gemio ERP 自然語言查詢")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Gemini setup
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel("gemini-1.5-flash")

# DB connection string
DB_CONN = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')};"
    f"TrustServerCertificate=yes;"
)

# Schema cache
_schema_cache: dict = {}


def get_db_connection():
    return pyodbc.connect(DB_CONN, timeout=10)


def load_schema() -> dict:
    """Load all table/view schemas from SQL Server and cache them."""
    global _schema_cache
    if _schema_cache:
        return _schema_cache

    schema = {}
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all user tables and views with column info
        cursor.execute("""
            SELECT
                o.name AS table_name,
                o.type_desc,
                c.name AS column_name,
                t.name AS data_type,
                c.max_length,
                c.is_nullable
            FROM sys.objects o
            JOIN sys.columns c ON o.object_id = c.object_id
            JOIN sys.types t ON c.user_type_id = t.user_type_id
            WHERE o.type IN ('U', 'V')
              AND o.is_ms_shipped = 0
            ORDER BY o.name, c.column_id
        """)
        rows = cursor.fetchall()
        for row in rows:
            tbl = row.table_name
            if tbl not in schema:
                schema[tbl] = {"type": row.type_desc, "columns": []}
            schema[tbl]["columns"].append({
                "name": row.column_name,
                "type": row.data_type,
                "nullable": row.is_nullable
            })
        conn.close()
    except Exception as e:
        schema["_error"] = str(e)

    _schema_cache = schema
    return schema


def schema_to_prompt(schema: dict) -> str:
    """Convert schema dict to a compact text for the AI prompt."""
    lines = []
    for tbl, info in schema.items():
        if tbl.startswith("_"):
            continue
        cols = ", ".join(
            f"{c['name']} ({c['type']})" for c in info["columns"]
        )
        lines.append(f"  {tbl}: {cols}")
    return "\n".join(lines)


def nl_to_sql(question: str, schema: dict) -> str:
    """Use Gemini to convert natural language to SQL."""
    schema_text = schema_to_prompt(schema)
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

    response = model.generate_content(prompt)
    sql = response.text.strip()
    # Clean up markdown code blocks if present
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(
            l for l in lines
            if not l.startswith("```")
        )
    return sql.strip()


def execute_sql(sql: str) -> tuple[list[str], list[list]]:
    """Execute SQL and return (columns, rows)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    rows = [list(row) for row in cursor.fetchall()]
    conn.close()
    return columns, rows


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    schema = load_schema()
    table_names = [k for k in schema.keys() if not k.startswith("_")]
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "tables": table_names}
    )


@app.post("/query", response_class=HTMLResponse)
async def query(request: Request, question: str = Form(...)):
    schema = load_schema()
    sql = ""
    columns = []
    rows = []
    error = ""
    numeric_cols = []

    try:
        sql = nl_to_sql(question, schema)
        columns, rows = execute_sql(sql)

        # Detect numeric columns for summing
        if rows:
            for i, col in enumerate(columns):
                try:
                    vals = [r[i] for r in rows if r[i] is not None]
                    if vals and all(isinstance(v, (int, float)) for v in vals):
                        numeric_cols.append(i)
                except Exception:
                    pass
    except Exception as e:
        error = str(e)

    # Convert rows to JSON-serializable
    rows_clean = []
    for row in rows:
        rows_clean.append([
            str(v) if v is not None else "" for v in row
        ])

    return templates.TemplateResponse(
        "result.html",
        {
            "request": request,
            "question": question,
            "sql": sql,
            "columns": columns,
            "rows": rows_clean,
            "error": error,
            "numeric_cols": numeric_cols,
        }
    )


@app.post("/export")
async def export(
    columns: str = Form(...),
    rows: str = Form(...),
):
    """Export result to Excel."""
    cols = json.loads(columns)
    data = json.loads(rows)
    df = pd.DataFrame(data, columns=cols)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="查詢結果")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=query_result.xlsx"}
    )


@app.get("/schema")
async def get_schema():
    """Reload and return schema (for debugging)."""
    global _schema_cache
    _schema_cache = {}
    schema = load_schema()
    return JSONResponse(schema)
