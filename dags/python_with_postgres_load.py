from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id="python_with_postgres_load",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "hook"],
) as dag:
    def bulk_load_postgres(postgres_conn_id: str, table_name: str, file_path: str, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(table_name, file_path)

    postgres_task = PythonOperator(
        task_id="postgres_task",
        python_callable=bulk_load_postgres,
        op_kwargs={"postgres_conn_id":"conn-db-postgres-custom",
                    "table_name":"nshopping.search",
                    "file_path":"/opt/airflow/files/naverSearch/{{data_interval_end.in_timezone(\"Asia/Seoul\") | ds_nodash }}/shop_with_tab.csv"}
    )