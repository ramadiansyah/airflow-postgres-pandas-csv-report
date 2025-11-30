from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from services.capstone2.p1.sql_loader import load_sql
from services.capstone2.p1.db_conn import get_engine
from datetime import datetime, timedelta
from utils.notifier import create_failure_callback, create_success_callback
import pandas as pd

WEBHOOK_URL = "https://discord.com/api/webhooks/URL" #put your URL here

default_args = {
    'owner': 'airflow',
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "on_failure_callback": create_failure_callback(WEBHOOK_URL),
}

import pandas as pd
from datetime import datetime
import os
import logging
from sqlalchemy.exc import SQLAlchemyError, DBAPIError

logger = logging.getLogger(__name__)

def export_to_csv(sql_filename, output_prefix):
    def _run():
        try:
            # ─── Load SQL Query ───────────────────────────────────────
            query = load_sql(sql_filename).strip()
            if not query:
                raise ValueError(f"SQL file '{sql_filename}' is empty or invalid.")

            # ─── Connect to DB ────────────────────────────────────────
            try:
                engine = get_engine()
                raw_conn = engine.raw_connection()
            except (SQLAlchemyError, DBAPIError) as db_err:
                logger.exception("❌ Failed to connect to the database.")
                raise RuntimeError(f"Database connection failed: {db_err}")

            # ─── Execute Query ────────────────────────────────────────
            try:
                df = pd.read_sql_query(query, con=raw_conn)
            except Exception as e:
                logger.exception("❌ Failed to execute SQL query.")
                raise RuntimeError(f"Query execution failed: {e}")
            finally:
                raw_conn.close()

            # ─── Save to CSV ──────────────────────────────────────────
            filename = f"data/c2p1/{output_prefix}_{datetime.today().strftime('%Y%m%d')}.csv"
            try:
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                df.to_csv(filename, index=False)
                logger.info(f"✅ Exported: {filename}")
            except Exception as e:
                logger.exception("❌ Failed to save CSV.")
                raise RuntimeError(f"CSV export failed: {e}")

        except Exception as e:
            logger.exception("🚨 Unhandled error in export_to_csv.")
            raise  # Re-raise to ensure Airflow marks task as FAILED

    return _run


with DAG(
    dag_id='capstone2_sql',
    default_args=default_args,
    description='Capstone 2 Project 1 - Export Metrics to CSV',
    schedule_interval='@weekly',
    start_date=days_ago(1),
    catchup=False,
    tags=['capstone2', 'sql'],
) as dag:

    task_a = PythonOperator(
        task_id='export_total_transactions',
        python_callable=export_to_csv('a_total_transactions.sql', 'capstone2_total_transactions'),
    )

    task_b = PythonOperator(
        task_id='export_total_order_amount',
        python_callable=export_to_csv('b_total_order_amount.sql', 'capstone2_total_order_amount'),
    )

    task_c = PythonOperator(
        task_id='export_top_discounted_products',
        python_callable=export_to_csv('c_top_discounted_products.sql', 'capstone2_top_discounted_products'),
    )

    task_d = PythonOperator(
        task_id='export_sales_by_category',
        python_callable=export_to_csv('d_sales_by_category.sql', 'capstone2_sales_by_category'),
    )

    task_e = PythonOperator(
        task_id='export_sales_high_rating',
        python_callable=export_to_csv('e_sales_high_rating.sql', 'capstone2_sales_high_rating'),
    )

    task_f = PythonOperator(
        task_id='export_doohickey_low_reviews',
        python_callable=export_to_csv('f_doohickey_low_reviews.sql', 'capstone2_doohickey_low_reviews'),
    )

    task_g1 = PythonOperator(
        task_id='export_unique_sources_count',
        python_callable=export_to_csv('g1_unique_sources.sql', 'capstone2_unique_sources_count'),
    )

    task_g2 = PythonOperator(
        task_id='export_list_sources',
        python_callable=export_to_csv('g2_list_sources.sql', 'capstone2_list_sources'),
    )

    task_h = PythonOperator(
        task_id='export_gmail_users_count',
        python_callable=export_to_csv('h_gmail_users_count.sql', 'capstone2_gmail_users_count'),
    )

    task_i = PythonOperator(
        task_id='export_products_price_range',
        python_callable=export_to_csv('i_products_price_range.sql', 'capstone2_products_price_range'),
    )

    task_j = PythonOperator(
        task_id='export_users_born_after_1997',
        python_callable=export_to_csv('j_users_born_after_1997.sql', 'capstone2_users_born_after_1997'),
    )

    task_k = PythonOperator(
        task_id='export_duplicate_products',
        python_callable=export_to_csv('k_duplicate_products.sql', 'capstone2_duplicate_products'),
    )

    # Optional chaining (sequential execution)
    task_a >> task_b >> task_c >> task_d >> task_e >> task_f >> task_g1 >> task_g2 >> task_h >> task_i >> task_j >> task_k
