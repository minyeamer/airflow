from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id="python_with_postgres_hook",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "hook"],
) as dag:
    def insert_into_postgres(postgres_conn_id: str, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing
        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get("ti").dag_id
                task_id = kwargs.get("ti").task_id
                run_id = kwargs.get("ti").run_id
                msg = "INSERT INTO 수행"
                sql = "INSERT INTO dag_run VALUES (%s,%s,%s,%s);"
                cursor.execute(sql,(dag_id,task_id,run_id,msg))
                conn.commit()

    postgres_task = PythonOperator(
        task_id="postgres_task",
        python_callable=insert_into_postgres,
        op_kwargs={"postgres_conn_id":"conn-db-postgres-custom"}
    )