# clone directory
git clone https://github.com/andrejnevesjr/airflow-spark-minio-postgres.git

# cd to airflow-spark-minio-postgres directory
cd airflow-spark-minio-postgres

# compose up
docker-compose -f docker-compose.yml up -d

# run airflow-webserver and create user (change email <airflow@example.com>) (only 1st time)
docker-compose run airflow-webserver airflow users create --role Admin --username airflow
--email 64xxxxxxxx@xxx.com --firstname airflow --lastname airflow --password airflow

# open browser at http://localhost:8085
User : airflow
Password : airflow

# go to <Admin> tap, select Connections
# edit <spark_conn>
# Spark

Connection Id -> spark_default
Connection Type -> Spark
Host -> spark://spark
Port -> 7077
Extra -> {"queue: "root.default"}

# Edit hello_spark.py
change all "spark_conn" into "spark_default"

# Edit spark-postgres.py
change all "spark_conn" into "spark_default"