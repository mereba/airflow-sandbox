from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def hello():
    return 'Hey yo!'

default_args = {
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'owner': 'team_x'
}
dag = DAG(
    'hello_world',
    description='saying hello to the world',
    start_date=datetime.fromisoformat('2020-03-23'),
    schedule_interval="0 */4 * * *",
    default_args=default_args
)

start = DummyOperator(task_id='start', dag=dag)
hello = PythonOperator(task_id='hello', python_callable=hello, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> hello >> end
