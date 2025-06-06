from airflow.sdk import DAG, Label
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum

with DAG(
    dag_id="edge_label",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 0 * * *",
    catchup=False,
    tags=["example", "branch"],
) as dag:
    empty_1 = EmptyOperator(
        task_id="empty_1"
    )

    empty_2 = EmptyOperator(
        task_id="empty_2"
    )

    empty_1 >> Label("라벨") >> empty_2

    empty_3 = EmptyOperator(
        task_id="empty_3"
    )

    empty_4 = EmptyOperator(
        task_id="empty_4"
    )

    empty_5 = EmptyOperator(
        task_id="empty_5"
    )

    empty_6 = EmptyOperator(
        task_id="empty_6"
    )

    empty_2 >> Label("브랜치 시작") >> [empty_3,empty_4,empty_5] >> Label("브랜치 종료") >> empty_6