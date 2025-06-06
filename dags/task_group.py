from airflow.sdk import DAG, task, task_group, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id="task_group",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 0 * * *",
    catchup=False,
    tags=["example", "branch"],
) as dag:
    def common_function(**kwargs):
        msg = kwargs.get("msg") or str() 
        print(msg)

    @task_group(group_id="first_group")
    def first_group():
        """ 첫 번째 TaskGroup 에 대한 Tooltip 입니다. """

        @task(task_id="inner_function1")
        def inner_function1(**kwargs):
            print("첫 번째 TaskGroup 내 첫 번째 Task 입니다.")

        inner_function2 = PythonOperator(
            task_id="inner_function2",
            python_callable=common_function,
            op_kwargs={"msg":"첫 번째 TaskGroup 내 두 번째 Task 입니다."}
        )

        inner_function1() >> inner_function2

    with TaskGroup(group_id="second_group", tooltip="두 번째 TaskGroup 에 대한 Tooltip 입니다.") as second_group:
        """ tooltip 파라미터의 내용이 우선적으로 표시됩니다. """
        @task(task_id="inner_function1")
        def inner_function1(**kwargs):
            print("두 번째 TaskGroup 내 첫 번째 Task 입니다.")

        inner_function2 = PythonOperator(
            task_id="inner_function2",
            python_callable=common_function,
            op_kwargs={"msg": "두 번째 TaskGroup 내 두 번째 Task 입니다."}
        )

        inner_function1() >> inner_function2

    first_group() >> second_group