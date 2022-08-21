from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

sshHook = SSHHook(ssh_conn_id=None,
                  remote_host='172.17.80.21',
                  username='hadoop',
                  password='',
                  key_file=None,
                  port=22,
                  conn_timeout=10)

dag = DAG('SparkStreaming_Postgres', description='spark submit',
          schedule_interval=timedelta(minutes=20),
          start_date=datetime(2022, 8, 13),
          catchup=False)

t1 = SSHOperator(
    task_id="spark-submit-chibm",
    command="export HADOOP_HOME=/usr/local/hadoop; \
		export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop; \
		export SPARK_HOME=/usr/local/spark; \
		export PATH=$PATH:$SPARK_HOME/bin; \
		export PATH=$PATH:$SPARK_HOME/sbin; \
		export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH; \
		spark-submit \
		--master yarn \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.postgresql:postgresql:42.2.18 \
		--deploy-mode client \
		--class Streaming /home/hadoop/chibm/demolast-1.0-SNAPSHOT.jar",
    ssh_hook=sshHook,
    dag=dag,
)
t1
