from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from common.common_func import regist
import pendulum

with DAG(
    dag_id="python_parameter",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    regist_task = PythonOperator(
        task_id="regist_task",
        python_callable=regist,
        op_args=["김철수", 20, "서울", "대한민국"],
        op_kwargs={"이메일":"su@example.com", "전화번호":"010-1234-5678"},
    )

    regist_task