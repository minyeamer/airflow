from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="trigger_rule2",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 0 * * *",
    catchup=False,
    tags=["example", "branch"],
) as dag:
    @task.branch(task_id="branching")
    def random_branch():
        import random

        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return "upstream_task_a"
        elif selected_item == 'B':
            return "upstream_task_b"
        elif selected_item == 'C':
            return "upstream_task_c"

    upstream_task_a = BashOperator(
        task_id="upstream_task_a",
        bash_command="echo upstream1"
    )

    @task(task_id="upstream_task_b")
    def upstream_task_b():
        print("정상 처리")

    @task(task_id="upstream_task_c")
    def upstream_task_c():
        print("정상 처리")

    @task(task_id="downstream_task", trigger_rule="none_skipped")
    def downstream_task():
        print("정상 처리")

    random_branch() >> [upstream_task_a, upstream_task_b(), upstream_task_c()] >> downstream_task()