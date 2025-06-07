from airflow.sdk import DAG
from operators.nshopping import NaverSearchToCsvOperator
import pendulum

with DAG(
    dag_id="nsearch_operator",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "http"],
) as dag:
    api_keys = dict(
        client_id="{{var.value.client_id_openapi_naver_com}}",
        client_secret="{{var.value.client_secret_openapi_naver_com}}")
    common_params = dict(keyword="노트북", display=1000, start=1, sort="sim")

    """네이버 쇼핑 검색"""
    search_shopping_task = NaverSearchToCsvOperator(
        task_id="search_shopping_task",
        search_type="shop",
        file_path="naverSearch/{{data_interval_end.in_timezone(\"Asia/Seoul\") | ds_nodash }}/shop.csv",
        **api_keys,
        **common_params
    )

    """네이버 블로그 검색"""
    search_blog_task = NaverSearchToCsvOperator(
        task_id="search_blog_task",
        search_type="blog",
        file_path="naverSearch/{{data_interval_end.in_timezone(\"Asia/Seoul\") | ds_nodash }}/blog.csv",
        **api_keys,
        **common_params
    )

    search_shopping_task >> search_blog_task