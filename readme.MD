# Chức năng hệ thống

- khảo sát thông tin và phân tích về bệnh tim mạch qua các năm

# mô hình hoạt động 

<img width="821" alt="Screen Shot 2022-08-13 at 16 06 09" src="https://user-images.githubusercontent.com/43698423/184611087-d0c4b70a-384a-4107-9764-76dbfee232f0.png">

1. Kafka: stream dữ liệu đổ về

2. Spark Streaming: consum data vào lưu trữ data stream vào postgres

3. superset : Phân tích dữ liệu

# airflow

```
http://172.17.80.23:8081
```
- user:passs airflow:airflow
- dag : csvToKafka

  - đẩy data từ file csv -> kafka. mỗi phút đọc 50 bản ghi

- dag : SparkToPostgres

  - lập lịch chạy Spark Streaming lưu data đọc từ kafka xuống postgres
  
  - 20p đọc 1 lần. 
# superset

```
http://172.17.80.24:8089
```
- user:pass admin:1
- dashboard : chibm

<img width="1440" alt="Screen Shot 2022-08-22 at 09 58 27" src="https://user-images.githubusercontent.com/43698423/186805454-cf8dc8ba-e478-42ab-a222-2d0cfafd528c.png">

<img width="1440" alt="Screen Shot 2022-08-22 at 09 58 42" src="https://user-images.githubusercontent.com/43698423/186805610-4b2af837-88eb-45a6-ba71-2a39659efda9.png">

<img width="1440" alt="Screen Shot 2022-08-22 at 09 58 56" src="https://user-images.githubusercontent.com/43698423/186805558-c8f4dc42-4baa-46de-bcaa-8a1818e5080b.png">


