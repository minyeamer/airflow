from airflow.sdk import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator
import pendulum

with DAG(
    dag_id="email_operator",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 9 1 * *",
    catchup=False,
    tags=["example", "email"],
) as dag:
    send_email_task = EmailOperator(
        task_id="send_email_task",
        conn_id="gmail",
        to="example@gmail.com",
        subject="Airflow 테스트",
        html_content="Airflow 작업이 완료되었습니다."
    )