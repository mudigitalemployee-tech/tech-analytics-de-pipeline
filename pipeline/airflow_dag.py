"""Airflow DAG — Tech Analytics Pipeline | Schedule: daily 07:00 UTC"""
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
default_args = {"owner":"ved-de","retries":3,"retry_delay":timedelta(minutes=5)}
with DAG("tech_analytics_pipeline",default_args=default_args,
         schedule_interval="0 7 * * *",start_date=datetime(2024,1,1),
         catchup=False,max_active_runs=1,tags=["saas","tech-analytics","medallion"]) as dag:
    extract   = PythonOperator(task_id="extract_all_sources", python_callable=extract_fn)
    pii_mask  = PythonOperator(task_id="mask_pii",            python_callable=mask_pii_fn)
    transform = PythonOperator(task_id="transform_silver",    python_callable=transform_fn)
    dq_gate   = BranchPythonOperator(task_id="dq_gate",       python_callable=dq_check_fn)
    gold      = PythonOperator(task_id="load_gold",           python_callable=load_gold_fn)
    notify_ok = DummyOperator(task_id="notify_success")
    notify_fail=DummyOperator(task_id="alert_dq_failure")
    extract >> pii_mask >> transform >> dq_gate >> [gold, notify_fail]
    gold >> notify_ok
