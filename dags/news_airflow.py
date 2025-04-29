from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from datetime import datetime
import os 

# DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Define your News API Key
NEWS_API_KEY = '8e111a2ff1a24c96a25e3d8e1606082a'  # <- replace this

# DAG
with DAG(
    dag_id='news_etl_to_postgres',
    default_args=default_args,
    schedule_interval='@daily',  # Run hr
    catchup=False
) as dag:

    def extract_news():
        url = f'https://newsapi.org/v2/top-headlines?country=us&apiKey={NEWS_API_KEY}'
        response = requests.get(url)
        data = response.json()
        articles = data.get('articles', [])
        return articles

    def transform_news(**kwargs):
        ti = kwargs['ti']
        articles = ti.xcom_pull(task_ids='extract_task')
        cleaned_articles = []

        for article in articles:
            cleaned_articles.append({
                'title': article.get('title', '').replace("'", ""),  # remove single quotes
                'description': article.get('description', '').replace("'", ""),
                'published_at': article.get('publishedAt', '')
            })
        
        return cleaned_articles

    def load_news(**kwargs):
        ti = kwargs['ti']
        articles = ti.xcom_pull(task_ids='transform_task')

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')  # or your connection ID
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS news_articles (
                id SERIAL PRIMARY KEY,
                title TEXT,
                description TEXT,
                published_at TIMESTAMP
            );
        """)
        conn.commit()

        # Insert articles
        for article in articles:
            cursor.execute("""
                INSERT INTO news_articles (title, description, published_at)
                VALUES (%s, %s, %s)
            """, (article['title'], article['description'], article['published_at']))
        
        conn.commit()
        cursor.close()
        conn.close()

    # Define tasks
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_news
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_news,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_news,
        provide_context=True
    )

    # Set task order
    extract_task >> transform_task >> load_task
