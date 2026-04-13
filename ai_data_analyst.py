# ========================= IMPORTS =========================
import streamlit as st
import pandas as pd
import re
import time
import json
import os
from sqlalchemy import create_engine
from difflib import get_close_matches
from io import BytesIO  # ✅ FIXED (export bug)

# CREWAI + GPT4All
try:
    from crewai import Agent, Task, Crew
    from crewai_tools import NL2SQLTool
    from gpt4all import GPT4All
    CREWAI_AVAILABLE = True
except ImportError:
    CREWAI_AVAILABLE = False
    st.warning("pip install crewai crewai-tools gpt4all==2.8.2")

st.set_page_config(layout="wide", page_title="AI SQL Dashboard")

# ========================= CONFIG =========================
DASHBOARD_FILE = "dashboards.json"

# ========================= STATE =========================
if "metrics" not in st.session_state:
    st.session_state.metrics = {"q":0,"success":0,"fail":0,"time":0,"gpt4all_success":0}

if "widgets" not in st.session_state:
    st.session_state.widgets = []

if "gpt4all_model" not in st.session_state:
    st.session_state.gpt4all_model = None

# ========================= GPT4ALL =========================
@st.cache_resource
def gpt4all_llm():
    if st.session_state.gpt4all_model:
        return st.session_state.gpt4all_model

    model = GPT4All(
        "Meta-Llama-3-8B-Instruct.Q4_0.gguf",
        device="cpu",
        model_path="./models",
        verbose=False
    )
    st.session_state.gpt4all_model = model
    return model

# ========================= SCHEMA =========================
def describe_df(df):
    desc = f"Table: data | Rows: {len(df)} | Columns: {len(df.columns)}\n"
    for col in df.columns:
        desc += f"- {col} ({df[col].dtype})\n"
    return desc

# ========================= CREWAI =========================
def crewai_nl2sql(query, engine, df):
    if not CREWAI_AVAILABLE:
        return None

    try:
        llm = gpt4all_llm()
        schema = describe_df(df)

        agent = Agent(
            role="SQL Expert",
            goal="Convert NL to SQL",
            backstory="Expert in SQL",
            llm=llm,
            verbose=False
        )

        task = Task(
            description=f"Schema:\n{schema}\nQuery:{query}\nReturn only SQL",
            agent=agent
        )

        crew = Crew(agents=[agent], tasks=[task])
        result = crew.kickoff()

        # ✅ FIXED REGEX
        match = re.search(r"(SELECT.*?)(?:LIMIT\s+\d+|;|$)", result, re.IGNORECASE | re.DOTALL)

        if match:
            return match.group(1).strip()

        return None

    except Exception as e:
        st.error(f"LLM Error: {str(e)[:80]}")
        return None

# ========================= RULE ENGINE =========================
def map_columns(query, cols):
    return [c for c in cols if c.lower() in query.lower()]

def detect_agg(query, df):
    q = query.lower()
    agg_map = {"sum":"SUM","avg":"AVG","count":"COUNT","max":"MAX","min":"MIN"}

    agg = next((agg_map[k] for k in agg_map if k in q), None)
    val = next((c for c in df.columns if c.lower() in q), None)
    grp = next((c for c in df.columns if f"by {c.lower()}" in q), None)

    return agg, val, grp

def rule_sql(query, df):
    agg,val,grp = detect_agg(query, df)

    if agg and val:
        if grp:
            return f"SELECT `{grp}`, {agg}(`{val}`) FROM data GROUP BY `{grp}`"
        return f"SELECT {agg}(`{val}`) FROM data"

    cols = map_columns(query, df.columns)
    if cols:
        return f"SELECT {', '.join(cols)} FROM data LIMIT 10"

    return "SELECT * FROM data LIMIT 10"

# ========================= HYBRID =========================
def generate_sql(query, df, engine):
    start = time.time()

    sql = crewai_nl2sql(query, engine, df)
    method = "GPT4All"

    success = False

    if sql:
        try:
            pd.read_sql(sql, engine)
            success = True
        except:
            success = False

    if not success:
        sql = rule_sql(query, df)
        method = "Rules"
        success = True

    elapsed = time.time() - start

    st.session_state.metrics["q"] += 1
    st.session_state.metrics["success"] += int(success)
    st.session_state.metrics["fail"] += int(not success)
    st.session_state.metrics["time"] += elapsed

    return sql, method, success, elapsed

# ========================= STORAGE =========================
def load_dashboards():
    if not os.path.exists(DASHBOARD_FILE):
        return {}
    return json.load(open(DASHBOARD_FILE))

def save_dashboards(data):
    json.dump(data, open(DASHBOARD_FILE,"w"), indent=2)

# ========================= UI =========================
st.title("🚀 AI Data Analysis Dashboard")

file = st.file_uploader("Upload CSV")

if file:
    df = pd.read_csv(file)

    # FILTER
    st.sidebar.header("Filters")
    filtered_df = df.copy()

    for col in df.columns:
        vals = sorted(df[col].dropna().astype(str).unique())
        selected = st.sidebar.multiselect(col, vals, default=vals)  # ✅ FIXED

        if selected:
            filtered_df = filtered_df[filtered_df[col].astype(str).isin(selected)]

    engine = create_engine("sqlite:///:memory:")
    filtered_df.to_sql("data", engine, index=False)

    st.dataframe(filtered_df)

    query = st.text_input("Ask")

    if query:
        sql,method,success,elapsed = generate_sql(query, filtered_df, engine)

        st.success(f"{method} | {elapsed:.2f}s")

        # ✅ SAFE SQL DISPLAY
        if sql:
            st.subheader("Generated SQL")
            st.code(sql, language="sql")
        else:
            st.warning("No SQL generated")

        try:
            result = pd.read_sql(sql, engine)
            st.dataframe(result)
        except Exception as e:
            st.error(str(e))
            result = filtered_df.head(10)

        # EXPORT
        if not result.empty:
            csv = result.to_csv(index=False).encode("utf-8")
            st.download_button("CSV", csv)

            buf = BytesIO()
            result.to_excel(buf, index=False)
            st.download_button("Excel", buf.getvalue())

        # INSIGHTS
        nums = result.select_dtypes(include=['number']).columns

        if len(nums) > 0:  # ✅ FIXED
            col = nums[0]
            st.metric("SUM", result[col].sum())
            st.metric("AVG", result[col].mean())
            st.metric("MAX", result[col].max())
            st.metric("COUNT", len(result))

        # CHART
        if len(result.columns) >= 2:
            st.bar_chart(result.set_index(result.columns[0])[result.columns[1]])
