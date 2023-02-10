from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator
import datetime as dt

dag = DAG(
    dag_id="niels-capstone-scheduler",
    description="Schedules for the capstone project",
    default_args={"owner": "Airflow"},
    schedule_interval="@once",
    start_date=dt.datetime(2021, 1, 1),
)
batch_job_name = "Niels_" + dt.datetime.now().strftime("%Y%m%d%H%M%s")

submit_batch_job = BatchOperator(
    task_id="submit_batch_job",
    job_name=batch_job_name,
    job_queue="academy-capstone-winter-2023-job-queue",
    job_definition="Niels-capstone",
    dag=dag,
    overrides="")