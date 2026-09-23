import os
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
CALLBACK_LOG = "/opt/airflow/project/logs/failure_callbacks.log"

logger = logging.getLogger("airflow.task")


def failure_callback(context):
    ti = context.get("task_instance")
    dag_run = context.get("dag_run")
    exc = context.get("exception")
    now = datetime.now(timezone.utc).isoformat()

    msg = (
        f"[{now}] TASK FAILED\n"
        f"  dag_id     = {getattr(ti, 'dag_id', '?')}\n"
        f"  task_id    = {getattr(ti, 'task_id', '?')}\n"
        f"  run_id     = {getattr(dag_run, 'run_id', '?')}\n"
        f"  try_number = {getattr(ti, 'try_number', '?')}\n"
        f"  max_tries  = {getattr(ti, 'max_tries', '?')}\n"
        f"  state      = {getattr(ti, 'state', '?')}\n"
        f"  exception  = {exc!r}\n"
    )

    logger.error(msg)

    try:
        os.makedirs(os.path.dirname(CALLBACK_LOG), exist_ok=True)
        with open(CALLBACK_LOG, "a") as f:
            f.write(msg + "\n")
    except Exception as e:
        logger.error("Failed to write failure callback log: %s", e)


DEFAULT_ARGS = {
    "owner": "dss150p",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": failure_callback,
}


with DAG(
    dag_id="dss150p_sales_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "run_mode": Param("full", enum=["full", "partition"]),
        "year": Param(2026, type="integer"),
        "month": Param(1, type="integer", minimum=1, maximum=12),
    },
    tags=["DSS150P"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli extract',
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli transform',
    )

    load = BashOperator(
        task_id="load",
        bash_command=(
            f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" '
            "{% if params.run_mode == 'partition' %}"
            "python -m src.cli load-partition --year {{ params.year }} --month {{ params.month }}"
            "{% else %}"
            "python -m src.cli load"
            "{% endif %}"
        ),
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli validate',
    )

    extract >> transform >> load >> validate