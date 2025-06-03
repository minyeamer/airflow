from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.timetables.interval import CronDataIntervalTimetable
import pendulum

with DAG(
    dag_id="bash_macros1",
    schedule=CronDataIntervalTimetable("0 0 L * *", timezone="Asia/Seoul"),
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    bash_task1 = BashOperator(
        task_id="bash_task1",
        env={
            "START_DATE": "{{ (data_interval_start.in_timezone(\"Asia/Seoul\") + macros.dateutil.relativedelta.relativedelta(days=1)) | ds }}",
            "END_DATE": "{{ data_interval_end.in_timezone(\"Asia/Seoul\") | ds }}",
        },
        bash_command="echo \"Start date is $START_DATE \" && echo \"End date is $END_DATE\"",
    )

    bash_task1