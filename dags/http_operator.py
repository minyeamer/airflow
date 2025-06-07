from airflow.sdk import DAG, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.models.taskinstance import TaskInstance
import pendulum

with DAG(
    dag_id="http_operator",
    start_date=pendulum.datetime(2025, 6, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["example", "http"],
) as dag:
    def parse(response):
        return response.json()["items"][0]

    nshopping_search_task = HttpOperator(
        task_id="nshopping_search_task",
        http_conn_id="openapi.naver.com",
        endpoint="/v1/search/shop.json",
        method="GET",
        data={
            "query": "노트북",
            "display": 10,
            "start": 1,
            "sort": "sim"
        },
        headers={
            "Content-Type": "application/json",
            "X-Naver-Client-Id": "{{var.value.client_id_openapi_naver_com}}",
            "X-Naver-Client-Secret": "{{var.value.client_secret_openapi_naver_com}}"
        },
        response_filter=parse
    )

    @task(task_id="print_product_task")
    def print_product_task(ti: TaskInstance, **kwargs):
        result = ti.xcom_pull(task_ids="nshopping_search_task")
        from pprint import pprint
        pprint(result)

    nshopping_search_task >> print_product_task()