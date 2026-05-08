# ========================= IMPORTS =========================
import streamlit as st
import pandas as pd
import re
import json
import os
import time
from sqlalchemy import create_engine
from difflib import get_close_matches
from io import BytesIO
import google.generativeai as genai

st.set_page_config(layout="wide")

DASHBOARD_FILE = "dashboards.json"

# ========================= SESSION STATE =========================
if "widgets" not in st.session_state:
    st.session_state.widgets = []

if "metrics" not in st.session_state:
    st.session_state.metrics = {
        "total": 0,
        "success": 0,
        "fail": 0,
        "time": 0
    }

# ========================= GEMINI CONFIG =========================
st.sidebar.header("⚙️ AI Settings")

gemini_key = st.sidebar.text_input(
    "Gemini API Key",
    type="password"
)

if gemini_key:
    genai.configure(api_key=gemini_key)
    st.sidebar.success("Gemini Connected ✅")
else:
    st.sidebar.warning("No API Key ❌")

# ========================= HELPERS =========================
def safe_col(c):
    return f"`{c}`"

def clean_value(v):

    if isinstance(v, str):
        return v.strip().strip("'").strip('"')

    return v

def match_col(name, cols):

    if not name:
        return None

    name = str(name).strip()

    # exact match
    for c in cols:
        if c.lower() == name.lower():
            return c

    # fuzzy match
    matches = get_close_matches(
        name,
        cols,
        n=1,
        cutoff=0.5
    )

    return matches[0] if matches else None

# ========================= DATA CLEANER =========================
def clean_dataframe(df):

    cleaned = df.copy()

    for col in cleaned.columns:

        if cleaned[col].dtype == "object":

            temp = cleaned[col].astype(str)

            temp = temp.str.replace(",", "", regex=False)
            temp = temp.str.replace("$", "", regex=False)
            temp = temp.str.replace("₹", "", regex=False)
            temp = temp.str.strip()

            numeric = pd.to_numeric(
                temp,
                errors="coerce"
            )

            # convert if majority numeric
            if numeric.notna().sum() > len(df) * 0.6:
                cleaned[col] = numeric

    return cleaned

# ========================= GEMINI → INTENT =========================
def gemini_to_intent(query, df):

    if not gemini_key:
        return None

    models = [
        "gemini-3.1-flash-lite-preview",
        "gemini-2.5-flash-latest",
        "gemini-1.5-flash-latest"
    ]

    prompt = f"""
Return ONLY valid JSON.

Dataset Columns:
{df.columns.tolist()}

JSON FORMAT:
{{
    "agg":"sum/avg/count/max/min/none",
    "column":"column name",
    "group_by":"column name or none",
    "condition":"SQL style condition or none",
    "limit":"number",
    "sort":"asc/desc/none"
}}

Rules:
- ONLY JSON
- NO explanation
- Use exact dataset columns
- If no aggregate -> "none"
- If no group_by -> "none"
- If no condition -> "none"

User Query:
{query}
"""

    for m in models:

        try:

            model = genai.GenerativeModel(m)

            res = model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0
                }
            )

            text = res.text.strip()

            match = re.search(
                r"\{[\s\S]*\}",
                text
            )

            if match:

                parsed = json.loads(
                    match.group()
                )

                st.success(f"Using Model: {m} ✅")

                return parsed

        except Exception:
            continue

    st.warning("Gemini Parsing Failed")
    return None

# ========================= CONDITION PARSER =========================
def build_where_clause(condition, df):

    if not condition:
        return ""

    if str(condition).lower() == "none":
        return ""

    operators = [
        "<=",
        ">=",
        "!=",
        "=",
        "<",
        ">"
    ]

    found_op = None

    for op in operators:
        if op in condition:
            found_op = op
            break

    if not found_op:
        return ""

    try:

        left, right = condition.split(
            found_op,
            1
        )

        left = clean_value(left)
        right = clean_value(right)

        matched_col = match_col(
            left,
            df.columns
        )

        if not matched_col:
            return ""

        dtype = str(df[matched_col].dtype)

        # STRING COLUMN
        if (
            "object" in dtype
            or "str" in dtype
        ):

            return f"""
            WHERE LOWER(TRIM({safe_col(matched_col)}))
            {found_op}
            LOWER(TRIM('{right}'))
            """

        # NUMERIC COLUMN
        else:

            try:

                num = float(right)

                return f"""
                WHERE {safe_col(matched_col)}
                {found_op}
                {num}
                """

            except:
                return ""

    except:
        return ""

# ========================= SQL BUILDER =========================
def build_sql(intent, df):

    if not intent:
        return "SELECT * FROM data LIMIT 10"

    agg = str(
        intent.get("agg", "none")
    ).lower()

    col = match_col(
        intent.get("column"),
        df.columns
    )

    grp = match_col(
        intent.get("group_by"),
        df.columns
    )

    cond = intent.get(
        "condition",
        "none"
    )

    limit = intent.get("limit", 10)

    sort = str(
        intent.get("sort", "none")
    ).lower()

    sql = ""

    # ================= SELECT =================
    if agg != "none" and col:

        agg_map = {
            "sum": "SUM",
            "avg": "AVG",
            "count": "COUNT",
            "max": "MAX",
            "min": "MIN"
        }

        agg_sql = agg_map.get(agg, "SUM")

        if grp and grp != "none":

            sql = f"""
            SELECT
                {safe_col(grp)},
                {agg_sql}({safe_col(col)}) AS value
            FROM data
            """

        else:

            sql = f"""
            SELECT
                {agg_sql}({safe_col(col)}) AS value
            FROM data
            """

    else:

        sql = "SELECT * FROM data"

    # ================= WHERE =================
    where_clause = build_where_clause(
        cond,
        df
    )

    sql += f" {where_clause}"

    # ================= GROUP BY =================
    if grp and grp != "none":

        sql += f"""
        GROUP BY {safe_col(grp)}
        """

    # ================= ORDER BY =================
    if sort in ["asc", "desc"]:

        order_col = grp if grp else col

        if order_col:

            sql += f"""
            ORDER BY {safe_col(order_col)}
            {sort.upper()}
            """

    # ================= LIMIT =================
    try:
        limit = int(limit)
    except:
        limit = 10

    sql += f" LIMIT {limit}"

    return sql

# ========================= STORAGE =========================
def load_dashboards():

    if not os.path.exists(DASHBOARD_FILE):
        return {}

    return json.load(open(DASHBOARD_FILE))

def save_dashboards(data):

    json.dump(
        data,
        open(DASHBOARD_FILE, "w"),
        indent=2
    )

# ========================= NORMALIZE RESULT =========================
def normalize_result(result):

    if result is None or result.empty:
        return result

    result.columns = [
        str(c).strip()
        for c in result.columns
    ]

    return result

# ========================= CHART PREP =========================
def prepare_chart_data(result, x, y):

    df = result.loc[
        :,
        ~result.columns.duplicated()
    ].copy()

    if x not in df.columns or y not in df.columns:
        return None

    y_data = pd.to_numeric(
        df[y],
        errors="coerce"
    )

    plot_df = pd.DataFrame({
        x: df[x],
        y: y_data
    }).dropna()

    return plot_df if not plot_df.empty else None

# ========================= UI =========================
st.title("🚀 AI Data Analysis System")

file = st.file_uploader("Upload CSV")

if file:

    try:

        df = pd.read_csv(file)

        # CLEAN DATA
        df = clean_dataframe(df)

    except Exception as e:

        st.error(f"CSV Error: {e}")

        st.stop()

    # ================= FILTERS =================
    st.sidebar.header("🔎 Filters")

    filtered_df = df.copy()

    for col in df.columns:

        vals = (
            df[col]
            .astype(str)
            .unique()
        )

        sel = st.sidebar.multiselect(
            col,
            vals,
            default=vals
        )

        filtered_df = filtered_df[
            filtered_df[col]
            .astype(str)
            .isin(sel)
        ]

    if filtered_df.empty:

        st.warning("No data after filtering")

        st.stop()

    # ================= SQLITE =================
    engine = create_engine(
        "sqlite:///:memory:"
    )

    # ensure numeric types
    for c in filtered_df.columns:

        if pd.api.types.is_numeric_dtype(
            filtered_df[c]
        ):

            filtered_df[c] = pd.to_numeric(
                filtered_df[c],
                errors="coerce"
            )

    filtered_df.to_sql(
        "data",
        engine,
        index=False,
        if_exists="replace"
    )

    st.dataframe(filtered_df)

    # ================= QUERY =================
    query = st.text_input(
        "Ask your question"
    )

    if query:

        start = time.time()

        st.session_state.metrics[
            "total"
        ] += 1

        # ================= INTENT =================
        intent = gemini_to_intent(
            query,
            filtered_df
        )

        st.subheader("🧠 Parsed Intent")
        st.json(intent)

        # ================= SQL =================
        sql = build_sql(
            intent,
            filtered_df
        )

        st.subheader("🧾 SQL")
        st.code(sql)

        # ================= EXECUTE =================
        try:

            result = pd.read_sql(
                sql,
                engine
            )

            result = normalize_result(
                result
            )

            st.session_state[
                "last_result"
            ] = result

            st.session_state.metrics[
                "success"
            ] += 1

        except Exception as e:

            st.error(f"SQL Error: {e}")

            result = filtered_df.head(10)

            st.session_state.metrics[
                "fail"
            ] += 1

        st.session_state.metrics[
            "time"
        ] += (time.time() - start)

        # ================= RESULT =================
        st.subheader("📊 Result")

        if result.empty:

            st.warning(
                "No matching rows found"
            )

        else:

            st.dataframe(result)

        # ================= INSIGHTS =================
        st.subheader("📈 Insights")

        num_cols = result.select_dtypes(
            include=["number"]
        ).columns.tolist()

        if len(num_cols):

            metric_col = num_cols[0]

            c1, c2, c3, c4, c5 = st.columns(5)

            c1.metric(
                "SUM",
                round(
                    result[metric_col].sum(),
                    2
                )
            )

            c2.metric(
                "AVG",
                round(
                    result[metric_col].mean(),
                    2
                )
            )

            c3.metric(
                "MAX",
                round(
                    result[metric_col].max(),
                    2
                )
            )

            c4.metric(
                "MIN",
                round(
                    result[metric_col].min(),
                    2
                )
            )

            c5.metric(
                "COUNT",
                result[metric_col].count()
            )

        # ================= PERFORMANCE =================
        st.subheader(
            "📊 Query Performance"
        )

        m = st.session_state.metrics

        total = m["total"]

        c1, c2, c3, c4 = st.columns(4)

        c1.metric(
            "Total Queries",
            total
        )

        c2.metric(
            "Success",
            m["success"]
        )

        c3.metric(
            "Failed",
            m["fail"]
        )

        avg_time = round(
            m["time"] / max(total, 1),
            3
        )

        c4.metric(
            "Avg Time (s)",
            avg_time
        )

        # ================= EXPORT =================
        st.subheader("📤 Export")

        fmt = st.selectbox(
            "Format",
            ["CSV", "Excel"]
        )

        if fmt == "CSV":

            st.download_button(
                "Download CSV",
                result.to_csv(index=False),
                "data.csv"
            )

        else:

            buf = BytesIO()

            result.to_excel(
                buf,
                index=False
            )

            st.download_button(
                "Download Excel",
                buf.getvalue(),
                "data.xlsx"
            )

# ========================= RESULT =========================
result = st.session_state.get(
    "last_result",
    None
)

# ========================= CHART BUILDER =========================
st.subheader("📊 Chart Builder")

if (
    result is not None
    and not result.empty
):

    if len(result.columns) >= 2:

        cols = list(result.columns)

        chart_type = st.selectbox(
            "Chart Type",
            ["Bar", "Line", "Scatter"]
        )

        x = st.selectbox(
            "X Axis",
            cols
        )

        y = st.selectbox(
            "Y Axis",
            cols
        )

        if x == y:

            st.warning(
                "X and Y cannot be same"
            )

        else:

            try:

                plot_df = prepare_chart_data(
                    result,
                    x,
                    y
                )

                if plot_df is not None:

                    if chart_type == "Bar":

                        st.bar_chart(
                            plot_df
                            .set_index(x)[y]
                        )

                    elif chart_type == "Line":

                        st.line_chart(
                            plot_df
                            .set_index(x)[y]
                        )

                    elif chart_type == "Scatter":

                        st.scatter_chart(
                            plot_df[[x, y]]
                        )

            except Exception as e:

                st.error(
                    f"Chart failed: {e}"
                )

        # ================= ADD CHART =================
        if st.button("Add Chart"):

            st.session_state.widgets.append({
                "x": x,
                "y": y,
                "type": chart_type
            })

# ========================= DASHBOARD =========================
st.subheader("🧩 Dashboard")

if (
    result is not None
    and not result.empty
    and st.session_state.widgets
):

    for i, w in enumerate(
        st.session_state.widgets
    ):

        try:

            plot_df = prepare_chart_data(
                result,
                w["x"],
                w["y"]
            )

            if plot_df is not None:

                if w["type"] == "Bar":

                    st.bar_chart(
                        plot_df
                        .set_index(w["x"])[w["y"]]
                    )

                elif w["type"] == "Line":

                    st.line_chart(
                        plot_df
                        .set_index(w["x"])[w["y"]]
                    )

                elif w["type"] == "Scatter":

                    st.scatter_chart(
                        plot_df[
                            [w["x"], w["y"]]
                        ]
                    )

        except Exception as e:

            st.warning(
                f"Chart {i} failed: {e}"
            )

        if st.button(
            f"Remove {i}",
            key=f"remove_{i}"
        ):

            st.session_state.widgets.pop(i)

            st.rerun()

# ========================= SAVE REPORT =========================
st.subheader("💾 Save Report")

name = st.text_input(
    "Report Name"
)

if st.button("Save"):

    db = load_dashboards()

    db[name] = st.session_state.widgets

    save_dashboards(db)

    st.success("Saved")

# ========================= LOAD REPORT =========================
st.subheader("📂 Load Report")

db = load_dashboards()

if db:

    selected = st.selectbox(
        "Select Report",
        list(db.keys())
    )

    if st.button("Load"):

        st.session_state.widgets = db[
            selected
        ]

        st.success(
            f"Loaded: {selected}"
        )

        st.rerun()
