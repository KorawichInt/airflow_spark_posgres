import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
# Spark connection
spark_conn = os.environ.get("spark_default", "spark_default")
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/assets/jars/postgresql-42.2.6.jar"
# postgres_driver_jar = "/src/spark/assets/jars/postgresql-42.2.6.jar"

# Paths for CSV and Spark jobs
prompts_file = "/usr/local/spark/assets/data/prompts.csv"
# prompts_file ="src/spark/assets/data/prompts.csv"
postgres_db = "jdbc:postgresql://postgres:5432/airflow"
postgres_user = "airflow"
postgres_pwd = "airflow"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="spark-postgres_prompts",
    description="DAG to load CSV into PostgreSQL and read back 10 rows from Postgres.",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

# Start task
start = DummyOperator(task_id="start", dag=dag)

# Spark job to load CSV into Postgres
spark_job_load_postgres = SparkSubmitOperator(
    task_id="spark_job_load_postgres",
    application="/usr/local/spark/applications/load-postgres.py",
    # application="/src/spark/applications/load-postgres.py",
    name="load-postgres",
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[prompts_file, postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag
)

# Spark job to read 10 rows from Postgres and log the result
spark_job_read_postgres = SparkSubmitOperator(
    task_id="spark_job_read_postgres",
    application="/usr/local/spark/applications/read-postgres.py",
    # application="/src/spark/applications/read-postgres.py",
    name="read-postgres",
    conn_id=spark_conn,
    verbose=1,
    conf={"spark.master": spark_master},
    application_args=[postgres_db, postgres_user, postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag
)

# End task
end = DummyOperator(task_id="end", dag=dag)

# Set task dependencies
start >> spark_job_load_postgres >> spark_job_read_postgres >> end
