from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id="python_with_postgres",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "hook"],
) as dag:
    def insert_into_postgres(ip: str, port: str, dbname: str, user: str, passwd: str, **kwargs):
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
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
        op_args=["172.28.0.3", "5432", "minyeamer", "minyeamer", "minyeamer"]
    )