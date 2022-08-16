from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import csv
from kafka import KafkaProducer,KafkaConsumer

def my_producer():
    producer = KafkaProducer(
    bootstrap_servers = ['172.17.80.23 : 9092'],
    value_serializer = lambda x : bytearray(x, 'utf-8')
    )
    topic = 'test_final1'

    with open('/home/chibm/data/data1.csv', newline='') as csvfile:
        datas = csv.reader(csvfile, delimiter='\n')
        for row in datas:
            producer.send(topic, value=str(row[0]))
def my_consumers():
    my_consumer = KafkaConsumer(
        'test_final1',
        bootstrap_servers=['172.17.80.23 : 9092'],
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
          schedule_interval='*/12 * * * *',
          start_date=datetime(2022, 8, 13), catchup=False)

Kafka_Producer = PythonOperator(
    task_id='kafka_producer', 
    python_callable=my_producer, 
    dag=dag)

Kafka_Consumer = PythonOperator(
    task_id='Kafka_Consumer', 
    python_callable=my_consumers, 
    dag=dag)
Kafka_Producer