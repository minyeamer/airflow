from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable
import pendulum

with DAG(
    dag_id="bash_variable",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    var = Variable.get("sample_key")

    bash_var_task1 = BashOperator(
        task_id="bash_var_task1",
        bash_command=f"echo variable: \"{var}\"",
    )

    bash_var_task2 = BashOperator(
        task_id="bash_var_task2",
        bash_command="echo variable: \"{{ var.value.sample_key }}\"",
    )