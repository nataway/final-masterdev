from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


dag = DAG('SparkToPostgres', description='Hello World DAG',
          schedule_interval='*/20 * * * *',
          start_date=datetime(2022, 8, 13), catchup=False)

spark_submit_local = SparkSubmitOperator(
		application ='/home/chibm/spark_gtvt/streaming.py' ,
		conn_id= 'spark_local', 
		task_id='spark_submit_task', 
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.postgresql:postgresql:42.2.18",
		dag=dag
		)
spark_submit_local
