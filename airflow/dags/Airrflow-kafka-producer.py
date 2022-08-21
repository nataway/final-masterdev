from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import csv
from kafka import KafkaProducer,KafkaConsumer

def my_producer():
    my_producer = KafkaProducer(
    bootstrap_servers = ['192.168.193.93 : 9092'],
    value_serializer = lambda x : bytearray(x, 'utf-8')
    )
    topic = 'test_final1'

    file_r = open('/home/chibm/data/checkpoin.txt', 'r')
    checkpoin = int(file_r.read().strip()) + 50
    file_r.close()
    file_w = open('/home/chibm/data/checkpoin.txt', 'w')
    file_w.write(str(checkpoin))
    file_w.close()
    count = 0

    with open('/home/chibm/data/data-test.csv', newline='') as csvfile:
        datas = csv.reader(csvfile, delimiter='\n')
        for row in datas:
            value = str(row[0])
            values = list(value.split(","))
            if checkpoin > int(values[0]) > (checkpoin - 20):
                my_producer.send(topic, value=value)
                print(row[0])
                count += 1
            if count == 50:
                break



def my_consumers():
    my_consumer = KafkaConsumer(
        'test_final1',
        bootstrap_servers=['192.168.193.93 : 9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x : str(x, 'utf-8')
        )

    for i in my_consumer:
        message = i.value
        print(message)
        break

dag = DAG('csvToKafka', description='Hello World DAG',
          schedule_interval='*/1 * * * *',
          start_date=datetime(2022, 8, 13), catchup=False)


Kafka_Producer = PythonOperator(
    task_id='kafka_producer', 
    python_callable=my_producer, 
    dag=dag)


# Kafka_Consumer = PythonOperator(
#     task_id='Kafka_Consumer', 
#     python_callable=my_consumers, 
#     dag=dag)

#Spark_streaming = BashOperator(
#    task_id='spark_streaming',
#    bash_command=' bash /home/chibm/streaming_run.sh',
#    dag=dag
#)

Kafka_Producer
