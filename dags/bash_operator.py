from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "bash"],
) as dag:
    bash_task1 = BashOperator(
        task_id="bash_task1",
        bash_command="echo whoami",
    )

    bash_task2 = BashOperator(
        task_id="bash_task2",
        bash_command="echo $HOSTNAME",
    )

    bash_task1 >> bash_task2