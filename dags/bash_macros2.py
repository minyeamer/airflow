from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable
import datetime as dt
import pendulum

with DAG(
    dag_id="bash_macros2",
    schedule=CronTriggerTimetable(
        "0 0 * * 6",
        timezone="Asia/Seoul",
        interval=dt.timedelta(weeks=1),
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    bash_task2 = BashOperator(
        task_id="bash_task2",
        env={
            "START_DATE": "{{ data_interval_start.in_timezone(\"Asia/Seoul\") | ds }}",
            # "START_DATE": "{{ (data_interval_start.in_timezone(\"Asia/Seoul\") + macros.dateutil.relativedelta.relativedelta(day=1)) | ds }}",
            "END_DATE": "{{ data_interval_end.in_timezone(\"Asia/Seoul\") | ds }}",
            # "END_DATE": "{{ (data_interval_end.in_timezone(\"Asia/Seoul\") + macros.dateutil.relativedelta.relativedelta(day=1)) | ds }}",
        },
        bash_command="echo \"Start date is $START_DATE \" && echo \"End date is $END_DATE\"",
    )

    bash_task2