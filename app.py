import streamlit as st
import psycopg2
import pandas as pd

# --- Streamlit page settings ---
st.set_page_config(page_title="Live News Feed", page_icon="📰", layout="wide")

st.title("📰 Live News Feed from Airflow ETL")

# --- Connect to PostgreSQL database ---
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="airflow",
        user="airflow",
        password="airflow",
        port="5432"
    )

# --- Fetch latest news ---
def fetch_news():
    conn = get_connection()
    query = """
        SELECT title, description, published_at
        FROM news_articles
        ORDER BY published_at DESC
        LIMIT 10;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# --- Main App ---
with st.spinner('Fetching latest news... 🛰️'):
    df_news = fetch_news()

# --- Display how many news articles were fetched ---
st.success(f"Fetched {len(df_news)} news articles!")

# --- Display the news ---
for idx, row in df_news.iterrows():
    st.subheader(row['title'])
    st.write(row['description'])
    st.caption(f"🗓️ Published at: {row['published_at']}")

# --- Manual Refresh Button ---
if st.button('🔄 Refresh News'):
    st.rerun()
