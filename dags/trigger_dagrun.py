from airflow.sdk import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="trigger_dagrun",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="30 9 * * *",
    catchup=False,
    tags=["example", "dagrun"],
) as dag:
    start_task = BashOperator(
        task_id="start_task",
        bash_command="echo \"start!\"",
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id="trigger_dag_task",
        trigger_dag_id="python_operator",
        trigger_run_id=None,
        logical_date="{{data_interval_start}}",
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=None
        )

    start_task >> trigger_dag_task