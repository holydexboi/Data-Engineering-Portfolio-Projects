from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Zulikif',
    'start_date': days_ago(0),
    'email': ['dhulqev@gmail.com'],
    'email_on_failure':  True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    "process_web_log",
    default_args=default_args,
    description="web log process",
    schedule_interval=timedelta(days=1)
)

extract = BashOperator(
    task_id="extract_data",
    bash_command="cut -c -12 accesslog.txt > /transformed_data.txt",
    dag=dag,
)

transform = BashOperator(
    task_id="transform_data",
    bash_command="grep -v 198.46.149.143 /extracted_data.txt > /transformed_data.txt",
    dag=dag,
)

load = BashOperator(
    task_id="load_data",
    bash_command="tar -cvf /weblog.tar /transformed_data.txt",
    dag=dag,
)

extract >> transform >> load