from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
import pendulum

with DAG(
    dag_id="branch_python_operator",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 0 * * *",
    catchup=False,
    tags=["example", "branch"],
) as dag:
    def select_random():
        import random

        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return "task_a"
        else:
            return ["task_b","task_c"]

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=select_random
    )

    def print_selected(**kwargs):
        print(kwargs["selected"])

    task_a = PythonOperator(
        task_id="task_a",
        python_callable=print_selected,
        op_kwargs={"selected":'A'}
    )

    task_b = PythonOperator(
        task_id="task_b",
        python_callable=print_selected,
        op_kwargs={"selected":'B'}
    )

    task_c = PythonOperator(
        task_id="task_c",
        python_callable=print_selected,
        op_kwargs={"selected":'C'}
    )

    branch_task >> [task_a, task_b, task_c]