"""
DAG: Migration - Add DateKey to Fact_Sales
==========================================

One-time migration DAG to add DateKey column to existing Fact_Sales table.
This allows integration with Dim_Date and Dim_Macro_Economic WITHOUT
having to re-run the entire historical ETL pipeline.

Run this ONCE after upgrading to the new schema.

Author: Data Engineering Team
Created: December 2025
"""

from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Add migrations folder to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from migrations.add_datekey_to_fact_sales import migrate_add_datekey


default_args = {
    'owner': 'anhth',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id='migration_add_datekey',
    default_args=default_args,
    description='One-time migration: Add DateKey column to Fact_Sales for macro data integration',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['migration', 'one-time', 'fact_sales', 'datekey'],
    doc_md=__doc__,
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    add_datekey = PythonOperator(
        task_id='add_datekey_to_fact_sales',
        python_callable=migrate_add_datekey,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> add_datekey >> end
