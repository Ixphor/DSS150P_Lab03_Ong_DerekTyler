from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Task B: Failure handling callback
def on_failure_callback(context):
    print(f"ALERT: Task Failed!")
    print(f"DAG: {context['dag'].dag_id} | Task: {context['task_instance'].task_id}")
    print(f"Run ID: {context['run_id']}")

default_args = {
    'owner': 'derek',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,                                  
    'retry_delay': timedelta(minutes=1),           
    'execution_timeout': timedelta(minutes=10),    
    'on_failure_callback': on_failure_callback
}

with DAG(
    'sales_pipeline',
    default_args=default_args,
    description='Modular ETL pipeline for sales data',
    schedule_interval='0 2 * * *',                 
    catchup=False,                                
    tags=['sales', 'etl'],
    params={                                       
        "run_mode": "full", 
        "year": 2026,
        "month": 1
    }
) as dag:
    
    run_id_arg = "--run-id {{ run_id }}"

    extract = BashOperator(
        task_id='extract',
        bash_command=f"python -m src.cli extract {run_id_arg}"
    )

    transform = BashOperator(
        task_id='transform',
        bash_command=f"python -m src.cli transform {run_id_arg}"
    )

    load_cmd = """
    {% if params.run_mode == 'partition' %}
        python -m src.cli load-partition --year {{ params.year }} --month {{ params.month }} --run-id {{ run_id }}
    {% else %}
        python -m src.cli load --run-id {{ run_id }}
    {% endif %}
    """

    load = BashOperator(
        task_id='load',
        bash_command=load_cmd
    )

    validate = BashOperator(
        task_id='validate',
        bash_command=f"python -m src.cli validate {run_id_arg}"
    )

    extract >> transform >> load >> validate