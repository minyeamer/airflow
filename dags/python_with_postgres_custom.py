from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from hooks.postgres import CustomPostgresHook
import pendulum

with DAG(
        dag_id="python_with_postgres_custom",
        schedule=None,
        start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=["example", "hook"],
) as dag:
    def bulk_load_postgres(postgres_conn_id: str, table: str, filename: str, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table=table, filename=filename, if_exists="replace", sep=",", with_header=True)

    bulk_load_postgres = PythonOperator(
        task_id="bulk_load_postgres",
        python_callable=bulk_load_postgres,
        op_kwargs={"postgres_conn_id": "conn-db-postgres-custom",
                    "table":"nshopping.search2",
                    "filename":"/opt/airflow/files/naverSearch/{{data_interval_end.in_timezone(\"Asia/Seoul\") | ds_nodash }}/shop.csv"}
    )